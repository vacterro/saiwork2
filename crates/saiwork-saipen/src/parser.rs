//! Strict parsers for the canonical SAIPEN text format (TASK 14 §19–§21).
//!
//! Verified against the `donors/saipen` baseline (v7.224.3, schema_version 3):
//! STATE.md is YAML frontmatter (`--- … ---`) with single-line scalars,
//! optional list values (`requires:`), CRLF or LF, optional UTF-8 BOM.
//! BOARD.md uses `## DOING/TODO/DONE/BLOCKED` sections; ticket status comes
//! from the section, never the checkbox alone.
//!
//! Strictness (TASK 14 §174): required structure and schema version are
//! strict; duplicate keys are surfaced as errors (never last-write-wins —
//! donor `saipen/state.ts` lesson); harmless formatting/whitespace and
//! unknown optional fields are tolerated (reader never writes back, so
//! preservation is not needed, §173).

use std::collections::BTreeMap;

use crate::model::{BoardSummary, FRONTMATTER_DELIM};

/// A parsed scalar map plus raw list values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateDoc {
    pub scalars: BTreeMap<String, String>,
    pub lists: BTreeMap<String, Vec<String>>,
}

/// Parse STATE.md canonical frontmatter.
///
/// Rules:
/// - optional UTF-8 BOM is stripped;
/// - the document must begin with a `---` delimiter line;
/// - content runs until the next `---` line (first match);
/// - `key: value` single-line scalars; value is trimmed and surrounding
///   matching quotes are stripped (`""` / `''`);
/// - `key:` with no value followed by indented `- item` lines collects a list
///   (canonical `requires:` shape);
/// - duplicate key → `Err` (surface, never silently resolved);
/// - unknown keys are preserved (they may be harmless forward-compatible
///   metadata; the reader is read-only, §173).
pub fn parse_state(raw: &str) -> Result<StateDoc, String> {
    let text = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let mut lines = text.lines();
    let first = lines.next().ok_or("empty STATE.md")?;
    if first.trim() != FRONTMATTER_DELIM {
        return Err(format!(
            "STATE.md must start with a `{FRONTMATTER_DELIM}` frontmatter delimiter, got {first:?}"
        ));
    }
    let mut scalars: BTreeMap<String, String> = BTreeMap::new();
    let mut lists: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut closed = false;
    let mut pending_list_key: Option<String> = None;
    for line in lines {
        let trimmed = line.trim();
        if trimmed == FRONTMATTER_DELIM {
            closed = true;
            break;
        }
        if trimmed.is_empty() {
            pending_list_key = None;
            continue;
        }
        // Continuation of a list value.
        if trimmed.starts_with('-') && pending_list_key.is_some() {
            if let Some(key) = pending_list_key.clone() {
                let item = trim_quotes(trimmed[1..].trim());
                lists.entry(key).or_default().push(item);
            }
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            // Not a key:value line and not a list item — a structural problem
            // inside frontmatter. Surface, do not guess.
            return Err(format!("unexpected line in frontmatter: {trimmed:?}"));
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(format!("empty key in frontmatter line: {trimmed:?}"));
        }
        let value = value.trim();
        if value.is_empty() {
            pending_list_key = Some(key.to_string());
            continue;
        }
        pending_list_key = None;
        if scalars.contains_key(key) || lists.contains_key(key) {
            return Err(format!("duplicated key in STATE.md frontmatter: {key}"));
        }
        scalars.insert(key.to_string(), trim_quotes(value));
    }
    if !closed {
        return Err(format!(
            "STATE.md frontmatter is not closed (missing `{FRONTMATTER_DELIM}`)"
        ));
    }
    Ok(StateDoc { scalars, lists })
}

/// Strip one layer of surrounding matching quotes.
fn trim_quotes(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 {
        let bytes = s.as_bytes();
        if (bytes[0] == b'"' && bytes[s.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[s.len() - 1] == b'\'')
        {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

/// Parse BOARD.md into section → ticket ids (§23). Section headers are
/// `## DOING` / `## TODO` / `## DONE` / `## BLOCKED` (canonical names; other
/// `##` headers are tolerated and skipped). Ticket lines start with
/// `- [ ]` or `- [x]`; the id is the first `T-\d+` token. Status derives
/// from the enclosing section, never the checkbox.
pub fn parse_board(raw: &str) -> Result<BoardSummary, String> {
    let text = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let mut sections: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(header) = trimmed.strip_prefix("## ") {
            let name = header.trim().to_uppercase();
            if matches!(name.as_str(), "DOING" | "TODO" | "DONE" | "BLOCKED") {
                current = Some(name.clone());
                sections.entry(name.clone()).or_default();
            } else {
                current = None;
            }
            continue;
        }
        let Some(section) = &current else { continue };
        if let Some(rest) = trimmed.strip_prefix("- [") {
            if let Some(id) = extract_ticket_id(rest) {
                sections.entry(section.clone()).or_default().push(id);
            }
        }
    }
    let counts = sections.iter().map(|(k, v)| (k.clone(), v.len())).collect();
    Ok(BoardSummary { sections, counts })
}

fn extract_ticket_id(rest: &str) -> Option<String> {
    // rest starts after "- [" — find the closing ']' then scan for T-\d+.
    let after = rest.find(']').map(|i| &rest[i + 1..]).unwrap_or(rest);
    let mut it = after.split_whitespace();
    let token = it.next()?;
    let upper = token.to_uppercase();
    if let Some(stripped) = upper.strip_prefix('T') {
        // Canonical ticket ids are T-<digits>; tolerate both T-101 and T101.
        let digits: String = stripped
            .trim_start_matches('-')
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !digits.is_empty() {
            return Some(format!("T-{digits}"));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const REAL_STATE: &str = "---\r\nphase: DONE\r\ntask: none\r\nnext_action: \"saipen continue\"\r\nblocker: \"\"\r\ntransition_from: SHIP\r\nsaipen_version: 7\r\nschema_version: 3\r\nlast_event: 3426\r\nstyle_contract: ded-4ae736e4\r\nagent: claude\r\nrequires:\r\n  - filesystem\r\n  - git\r\n  - python\r\nmode: full\r\nexecution_intent: goal\r\ngoal_waves: 1\r\ngoal_tickets: 13\r\nupdated: \"2026-08-14T09:27:13Z\"\r\n---\r\n";

    #[test]
    fn parses_real_canonical_state_crlf() {
        let doc = parse_state(REAL_STATE).expect("real STATE must parse");
        assert_eq!(doc.scalars.get("phase").map(String::as_str), Some("DONE"));
        assert_eq!(doc.scalars.get("task").map(String::as_str), Some("none"));
        assert_eq!(
            doc.scalars.get("next_action").map(String::as_str),
            Some("saipen continue")
        );
        assert_eq!(doc.scalars.get("blocker").map(String::as_str), Some(""));
        assert_eq!(
            doc.scalars.get("schema_version").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            doc.scalars.get("saipen_version").map(String::as_str),
            Some("7")
        );
        assert_eq!(
            doc.lists.get("requires"),
            Some(&vec!["filesystem".into(), "git".into(), "python".into()])
        );
        // Unknown optional keys are preserved, not rejected.
        assert_eq!(
            doc.scalars.get("goal_tickets").map(String::as_str),
            Some("13")
        );
    }

    #[test]
    fn parses_lf_and_bom() {
        let raw = "\u{feff}---\nphase: BUILD\n---\n";
        let doc = parse_state(raw).expect("BOM + LF must parse");
        assert_eq!(doc.scalars.get("phase").map(String::as_str), Some("BUILD"));
    }

    #[test]
    fn duplicate_key_is_an_error_never_last_wins() {
        let raw = "---\nphase: A\nphase: B\n---\n";
        let err = parse_state(raw).unwrap_err();
        assert!(err.contains("duplicated"), "got: {err}");
    }

    #[test]
    fn missing_delimiter_or_closing_is_an_error() {
        assert!(parse_state("phase: A\n").is_err());
        assert!(parse_state("---\nphase: A\n").is_err());
    }

    #[test]
    fn unquoted_and_single_quoted_scalars() {
        let raw = "---\nphase: 'BUILD'\nblocker: none\n---\n";
        let doc = parse_state(raw).unwrap();
        assert_eq!(doc.scalars.get("phase").map(String::as_str), Some("BUILD"));
        assert_eq!(doc.scalars.get("blocker").map(String::as_str), Some("none"));
    }

    #[test]
    fn parses_real_board_sections() {
        let board = "# Board\n## DOING\n\n## TODO\n- [ ] T-101 [P2] something | verify: x\n## DONE\n- [x] T-100 [P3] done thing\n## BLOCKED\n- [ ] T-102 [P1] blocked thing | blocker: y\n";
        let summary = parse_board(board).unwrap();
        assert_eq!(summary.counts.get("TODO"), Some(&1));
        assert_eq!(summary.counts.get("DONE"), Some(&1));
        assert_eq!(summary.counts.get("BLOCKED"), Some(&1));
        assert_eq!(summary.counts.get("DOING"), Some(&0));
        assert_eq!(summary.sections.get("DONE"), Some(&vec!["T-100".into()]));
        // Status comes from section, not checkbox.
        assert_eq!(summary.sections.get("TODO"), Some(&vec!["T-101".into()]));
        assert_eq!(summary.sections.get("BLOCKED"), Some(&vec!["T-102".into()]));
    }

    #[test]
    fn non_ticket_list_lines_are_skipped() {
        let board = "## TODO\n\n_(pruned note)_\n- [ ] T-5 pick me\n";
        let summary = parse_board(board).unwrap();
        assert_eq!(summary.counts.get("TODO"), Some(&1));
    }
}
