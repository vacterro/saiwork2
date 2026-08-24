//! Read path (TASK 14 §17, §25–§28, §45–§48).
//!
//! Side-effect free: opens files read-only, never modifies mtime/content,
//! never creates lock files (§121–§123). Reads are size-bounded and strict
//! UTF-8. Snapshot consistency across STATE + BOARD uses the canonical
//! writer's atomic-replace behavior: read both files, then re-check their
//! (size, mtime) markers; if they moved during the read, retry boundedly.
//! This is the "read marker before/after" strategy (§26) with the
//! filesystem's own metadata as the marker — SAIPEN has no in-file
//! generation counter in this schema version.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::{
    Discovery, SaipenDescriptor, SaipenError, SaipenRoot, SaipenSnapshot, SaipenSummary, WatchStatus,
    BOARD_FILE, SAIPEN_DIR, STATE_FILE,
};
use crate::parser::{parse_board, parse_state};
use crate::paths;

/// Generous but bounded canonical file limits (§46–§47). STATE.md is a few
/// KB; a 1 MiB cap is far beyond any sane canonical file while still
/// bounding a corrupted 10 GB file.
pub const MAX_STATE_BYTES: u64 = 1024 * 1024;
pub const MAX_BOARD_BYTES: u64 = 8 * 1024 * 1024;

/// Consistency retry budget for transient atomic-replace races (§28).
pub const MAX_CONSISTENCY_RETRIES: u32 = 2;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>, SaipenError> {
    let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => SaipenError::NotPresent,
        std::io::ErrorKind::PermissionDenied => SaipenError::PermissionDenied {
            path: path.to_path_buf(),
        },
        _ => SaipenError::Io {
            path: path.to_path_buf(),
            source: e,
        },
    })?;
    if meta.len() > max {
        return Err(SaipenError::TooLarge(format!(
            "{} ({} bytes > limit {max})",
            path.display(),
            meta.len()
        )));
    }
    std::fs::read(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => SaipenError::NotPresent,
        std::io::ErrorKind::PermissionDenied => SaipenError::PermissionDenied {
            path: path.to_path_buf(),
        },
        _ => SaipenError::Io {
            path: path.to_path_buf(),
            source: e,
        },
    })
}

fn read_text(path: &Path, max: u64, label: &str) -> Result<String, SaipenError> {
    let bytes = read_bounded(path, max)?;
    String::from_utf8(bytes)
        .map_err(|_| SaipenError::Encoding(format!("{label} at {}", path.display())))
}

/// (size, mtime_ns) marker for consistency checking.
fn marker(path: &Path) -> Option<(u64, i128)> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime = meta.modified().ok()?;
    let ns = mtime
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i128)
        .unwrap_or(0);
    Some((meta.len(), ns))
}

/// Discover and validate SAIPEN in a workspace (§8–§16).
pub fn discover(workspace_root: &Path) -> Result<Discovery, SaipenError> {
    let Some(root) = paths::validate_root(workspace_root)? else {
        return Ok(Discovery::NotPresent);
    };
    let state_path = match paths::validate_file_in_root(&root, STATE_FILE) {
        Ok(p) => p,
        Err(SaipenError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Discovery::Invalid {
                reason: format!("{} missing", root.dir.join(STATE_FILE).display()),
            });
        }
        Err(e) => return Err(e),
    };
    let raw = match read_bounded(&state_path, MAX_STATE_BYTES) {
        Ok(b) => String::from_utf8(b).map_err(|_| SaipenError::Encoding(STATE_FILE.to_string()))?,
        Err(SaipenError::NotPresent) => {
            return Ok(Discovery::Invalid {
                reason: format!("{} missing", root.dir.join(STATE_FILE).display()),
            });
        }
        Err(e) => return Err(e),
    };
    let doc = match parse_state(&raw) {
        Ok(d) => d,
        Err(detail) => {
            return Ok(Discovery::Invalid {
                reason: format!("cannot parse {STATE_FILE}: {detail}"),
            });
        }
    };
    let schema_version = doc.scalars.get("schema_version").cloned();
    let protocol_version = doc.scalars.get("saipen_version").cloned();
    // Supported schema versions (verified contract): 3 (current).
    const SUPPORTED: &[&str] = &["3"];
    if let Some(sv) = &schema_version {
        if !SUPPORTED.contains(&sv.as_str()) {
            return Ok(Discovery::Unsupported {
                schema_version: schema_version.clone(),
                protocol_version,
            });
        }
    }
    Ok(Discovery::Present(SaipenDescriptor {
        root,
        schema_version,
        protocol_version,
        project_name: doc.scalars.get("project").cloned(),
    }))
}

/// Read a full normalized snapshot. Consistency: read STATE + BOARD, then
/// verify their markers did not move during the read; on movement, retry
/// boundedly (§25–§28). **Retry exhaustion with markers still moving returns
/// a typed `InconsistentSnapshot`** — never a hybrid STATE+BOARD read that
/// the consistency logic exists to prevent.
pub fn read_snapshot(root: &SaipenRoot, generation: u64) -> Result<SaipenSnapshot, SaipenError> {
    #[cfg(feature = "failpoints")]
    crate::test_hooks::maybe_slow_down(root);
    read_snapshot_inner(root, generation, &mut |p| marker(p))
}

/// Core read with an injectable marker reader (test hook): the marker pair
/// before/after each attempt must be identical to accept the read.
fn read_snapshot_inner(
    root: &SaipenRoot,
    generation: u64,
    markers: &mut dyn FnMut(&Path) -> Option<(u64, i128)>,
) -> Result<SaipenSnapshot, SaipenError> {
    let mut attempt = 0u32;
    loop {
        let before = (
            markers(&root.dir.join(STATE_FILE)),
            markers(&root.dir.join(BOARD_FILE)),
        );
        let state_path = paths::validate_file_in_root(root, STATE_FILE)?;
        let board_path = paths::validate_file_in_root(root, BOARD_FILE)?;
        let state_raw = read_text(&state_path, MAX_STATE_BYTES, STATE_FILE)?;
        let board_raw = read_text(&board_path, MAX_BOARD_BYTES, BOARD_FILE)?;
        let after = (
            markers(&root.dir.join(STATE_FILE)),
            markers(&root.dir.join(BOARD_FILE)),
        );
        if before == after {
            let doc = parse_state(&state_raw).map_err(|detail| SaipenError::Parse {
                file: STATE_FILE.to_string(),
                detail,
            })?;
            let board = parse_board(&board_raw).map_err(|detail| SaipenError::Parse {
                file: BOARD_FILE.to_string(),
                detail,
            })?;
            return Ok(SaipenSnapshot {
                generation,
                read_at_ms: now_ms(),
                root: Some(root.dir.clone()),
                schema_version: doc.scalars.get("schema_version").cloned(),
                saipen_version: doc.scalars.get("saipen_version").cloned(),
                project: doc.scalars.get("project").cloned(),
                phase: doc.scalars.get("phase").cloned(),
                task: doc.scalars.get("task").cloned(),
                next_action: doc.scalars.get("next_action").cloned(),
                blocker: doc.scalars.get("blocker").cloned(),
                mode: doc.scalars.get("mode").cloned(),
                execution_intent: doc.scalars.get("execution_intent").cloned(),
                agent: doc.scalars.get("agent").cloned(),
                updated: doc.scalars.get("updated").cloned(),
                last_event: doc.scalars.get("last_event").cloned(),
                board,
                watch_status: WatchStatus::NotWatching,
                last_error: None,
                stale: false,
            });
        }
        attempt += 1;
        if attempt > MAX_CONSISTENCY_RETRIES {
            return Err(SaipenError::InconsistentSnapshot(format!(
                "STATE/BOARD markers kept moving across {} attempts (limit {} retries)",
                attempt, MAX_CONSISTENCY_RETRIES
            )));
        }
    }
}

/// Convenience: full pipeline for one shot (discovery + snapshot). Used by
/// the Tauri command and tests; the service keeps the cached projection.
pub fn snapshot_for_workspace(
    workspace_root: &Path,
    generation: u64,
) -> Result<Option<SaipenSnapshot>, SaipenError> {
    match discover(workspace_root)? {
        Discovery::NotPresent => Ok(None),
        Discovery::Present(desc) => read_snapshot(&desc.root, generation).map(Some),
        Discovery::Invalid { reason } => Err(SaipenError::Parse {
            file: format!("{SAIPEN_DIR}/"),
            detail: reason,
        }),
        Discovery::Unsupported {
            schema_version,
            protocol_version,
        } => Err(SaipenError::UnsupportedVersion(format!(
            "schema {schema_version:?} protocol {protocol_version:?}"
        ))),
        Discovery::PermissionDenied { path } => Err(SaipenError::PermissionDenied { path }),
    }
}

/// Cheap sidebar/list summary (TASK 24 perf): STATE discovery only — reads
/// and parses STATE scalars, never BOARD, and never runs the consistency
/// pipeline. Presence is `Some`; `Invalid`/`Unsupported`/`PermissionDenied`
/// mirror `snapshot_for_workspace`'s error mapping so badge behavior is
/// unchanged (callers that previously swallowed errors still see `None`).
pub fn summarize(workspace_root: &Path) -> Result<Option<SaipenSummary>, SaipenError> {
    match discover(workspace_root)? {
        Discovery::NotPresent => Ok(None),
        Discovery::Present(desc) => Ok(Some(SaipenSummary {
            schema_version: desc.schema_version,
            saipen_version: desc.protocol_version,
            project: desc.project_name,
        })),
        Discovery::Invalid { reason } => Err(SaipenError::Parse {
            file: format!("{SAIPEN_DIR}/"),
            detail: reason,
        }),
        Discovery::Unsupported {
            schema_version,
            protocol_version,
        } => Err(SaipenError::UnsupportedVersion(format!(
            "schema {schema_version:?} protocol {protocol_version:?}"
        ))),
        Discovery::PermissionDenied { path } => Err(SaipenError::PermissionDenied { path }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(dir: &Path, name: &str, content: &str) {
        fs::write(dir.join(name), content).unwrap();
    }

    fn fixture() -> (tempfile::TempDir, SaipenRoot) {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        write(
            dir.path(),
            ".saipen/STATE.md",
            "---\nphase: BUILD\ntask: T-100\nnext_action: continue\nschema_version: 3\nsaipen_version: 7\nblocker: \"\"\n---\n",
        );
        write(
            dir.path(),
            ".saipen/BOARD.md",
            "## DOING\n- [ ] T-100 [P1] x\n## TODO\n- [ ] T-101 [P2] y\n## DONE\n- [x] T-99 [P3] z\n## BLOCKED\n",
        );
        let root = paths::validate_root(dir.path()).unwrap().unwrap();
        (dir, root)
    }

    #[test]
    fn discovers_present() {
        let (dir, _) = fixture();
        let d = discover(dir.path()).unwrap();
        match d {
            Discovery::Present(desc) => {
                assert_eq!(desc.schema_version.as_deref(), Some("3"));
            }
            other => panic!("expected Present, got {other:?}"),
        }
    }

    #[test]
    fn discovers_not_present() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(discover(dir.path()).unwrap(), Discovery::NotPresent);
    }

    #[test]
    fn summarize_never_reads_board() {
        // TASK 24 perf: the sidebar/list summary must be STATE discovery
        // only. A workspace whose BOARD is BROKEN (unparsable garbage) still
        // summarizes fine (zero BOARD reads), while the full snapshot — which
        // reads and parses BOARD — fails. This is the discriminating proof
        // that listing N workspaces performs no BOARD I/O.
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        write(
            dir.path(),
            ".saipen/STATE.md",
            "---\nphase: BUILD\ntask: T-100\nnext_action: continue\nschema_version: 3\nsaipen_version: 7\nblocker: \"\"\n---\n",
        );
        // An oversized BOARD: read_snapshot rejects it as a bounded read
        // error, while summarize never touches BOARD at all.
        write(
            dir.path(),
            ".saipen/BOARD.md",
            &"x".repeat(9 * 1024 * 1024),
        );

        let summary = summarize(dir.path()).expect("summary must not need BOARD");
        let summary = summary.expect("SAIPEN present from STATE alone");
        assert_eq!(summary.schema_version.as_deref(), Some("3"));
        assert_eq!(summary.saipen_version.as_deref(), Some("7"));

        // The full pipeline WOULD fail on this workspace (oversized BOARD):
        // if listing used the old full read, the badge would disappear.
        assert!(
            snapshot_for_workspace(dir.path(), 0).is_err(),
            "full snapshot requires BOARD and must fail on the oversized board"
        );
    }

    #[test]
    fn summarize_matches_old_presence_truth() {
        let (dir, _) = fixture();
        let s = summarize(dir.path()).unwrap().unwrap();
        assert_eq!(s.project.as_deref(), None);
        // Empty workspace: absent in both cheap and full paths.
        let plain = tempfile::tempdir().unwrap();
        assert_eq!(summarize(plain.path()).unwrap(), None);
        assert_eq!(snapshot_for_workspace(plain.path(), 0).unwrap(), None);
    }

    #[test]
    fn unsupported_schema_is_rejected_not_parsed() {
        let (dir, _) = fixture();
        write(
            dir.path(),
            ".saipen/STATE.md",
            "---\nschema_version: 99\nphase: BUILD\n---\n",
        );
        match discover(dir.path()).unwrap() {
            Discovery::Unsupported { schema_version, .. } => {
                assert_eq!(schema_version.as_deref(), Some("99"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn missing_state_file_is_invalid_not_not_present() {
        let (dir, _) = fixture();
        fs::remove_file(dir.path().join(".saipen/STATE.md")).unwrap();
        match discover(dir.path()).unwrap() {
            Discovery::Invalid { .. } => {}
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn reads_full_snapshot() {
        let (_dir, root) = fixture();
        let snap = read_snapshot(&root, 7).unwrap();
        assert_eq!(snap.generation, 7);
        assert_eq!(snap.phase.as_deref(), Some("BUILD"));
        assert_eq!(snap.task.as_deref(), Some("T-100"));
        assert_eq!(snap.next_action.as_deref(), Some("continue"));
        assert_eq!(snap.blocker.as_deref(), Some(""));
        assert_eq!(snap.board.counts.get("DONE"), Some(&1));
        assert_eq!(
            snap.board.sections.get("DOING"),
            Some(&vec!["T-100".into()])
        );
    }

    #[test]
    fn oversized_state_is_bounded_error() {
        let (dir, root) = fixture();
        let big = "phase: X\n".repeat(200_000); // ~1.8 MB
        write(dir.path(), ".saipen/STATE.md", &format!("---\n{big}---\n"));
        assert!(matches!(
            read_snapshot(&root, 1),
            Err(SaipenError::TooLarge(_))
        ));
    }

    #[test]
    fn invalid_utf8_is_encoding_error_not_panic() {
        let (dir, root) = fixture();
        fs::write(
            dir.path().join(".saipen/STATE.md"),
            b"---\n\xff\xfe\x00---\n",
        )
        .unwrap();
        assert!(matches!(
            read_snapshot(&root, 1),
            Err(SaipenError::Encoding(_))
        ));
    }

    #[test]
    fn retry_exhaustion_with_moving_markers_is_inconsistent_snapshot() {
        // A deterministic "writer": markers change on every marker() call, so
        // every attempt sees before != after and the consistency budget
        // exhausts. The reader must return a typed InconsistentSnapshot — it
        // must NEVER accept the hybrid STATE+BOARD read this logic protects
        // against (§25–§28).
        let (_dir, root) = fixture();
        let mut calls = 0u64;
        let mut moving = |_p: &Path| {
            calls += 1;
            Some((calls, calls as i128))
        };
        let err = read_snapshot_inner(&root, 1, &mut moving).unwrap_err();
        assert!(
            matches!(err, SaipenError::InconsistentSnapshot(_)),
            "expected InconsistentSnapshot, got {err:?}"
        );
        // Budget: MAX_CONSISTENCY_RETRIES + 1 attempts, 4 marker calls each
        // (before STATE, before BOARD, after STATE, after BOARD).
        assert_eq!(
            calls,
            u64::from(MAX_CONSISTENCY_RETRIES + 1) * 4,
            "consistency budget must be exactly MAX_CONSISTENCY_RETRIES + 1 attempts"
        );
    }

    #[test]
    fn reading_does_not_modify_files() {
        let (dir, root) = fixture();
        let before_state = fs::read(dir.path().join(".saipen/STATE.md")).unwrap();
        let before_board = fs::read(dir.path().join(".saipen/BOARD.md")).unwrap();
        read_snapshot(&root, 1).unwrap();
        assert_eq!(
            fs::read(dir.path().join(".saipen/STATE.md")).unwrap(),
            before_state
        );
        assert_eq!(
            fs::read(dir.path().join(".saipen/BOARD.md")).unwrap(),
            before_board
        );
        // No lock/residue files created by the reader.
        let names: Vec<_> = fs::read_dir(dir.path().join(".saipen"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            names.iter().all(|n| n == "STATE.md" || n == "BOARD.md"),
            "got {names:?}"
        );
    }
}
