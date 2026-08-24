//! Runtime diagnostics (bounded) and secret redaction (SECURITY.md).
//!
//! Redaction happens at the log/diagnostics boundary — never after the fact.
//! The recent-error ring is bounded (law 13).

use std::collections::VecDeque;
use std::sync::Mutex;

use regex::Regex;

/// Hard cap for the in-memory recent-error ring. Diagnostics metadata in the
/// DB has its own retention policy (STORAGE.md).
pub const MAX_RECENT_ERRORS: usize = 64;

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct ErrorRecord {
    pub code: String,
    pub message: String, // already redacted
    pub ts_ms: i64,
}

#[derive(Debug, Default)]
pub struct Diagnostics {
    recent_errors: Mutex<VecDeque<ErrorRecord>>,
}

impl Diagnostics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a runtime error (bounded ring; oldest dropped at cap).
    pub fn record_error(&self, code: impl Into<String>, message: impl Into<String>) {
        let mut ring = self
            .recent_errors
            .lock()
            .expect("diagnostics mutex poisoned");
        if ring.len() >= MAX_RECENT_ERRORS {
            ring.pop_front();
        }
        ring.push_back(ErrorRecord {
            code: code.into(),
            message: redact(&message.into()),
            ts_ms: now_ms(),
        });
    }

    pub fn recent_errors(&self) -> Vec<ErrorRecord> {
        let ring = self
            .recent_errors
            .lock()
            .expect("diagnostics mutex poisoned");
        ring.iter().cloned().collect()
    }

    pub fn clear_errors(&self) {
        self.recent_errors
            .lock()
            .expect("diagnostics mutex poisoned")
            .clear();
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Redact secrets from a string destined for logs/diagnostics.
///
/// Covers the shapes SAIWORK2 legitimately sees: bearer tokens, authorization
/// headers, api keys, refresh tokens, passwords. Exact-match secrets are
/// handled by callers that know the secret; this is the shape-level net.
pub fn redact(input: &str) -> String {
    let mut out = input.to_string();
    for re in REDACTION_PATTERNS.iter() {
        out = re
            .replace_all(&out, |caps: &regex::Captures<'_>| {
                let m = caps.get(0).expect("whole match captured");
                // Boundaries are evaluated against the EXACT haystack whose
                // match offsets `m.start()`/`m.end()` describe — `out`, not
                // `input`. An earlier length-changing replacement makes `out`
                // longer/shorter than `input`, so indexing `input` with `out`'s
                // offsets could panic or read the wrong byte and let a later
                // standalone secret slip past the guard (T-051).
                let before = m.start().checked_sub(1).map(|i| out.as_bytes()[i]);
                let after = out.as_bytes().get(m.end());
                // Keep tokens that look like part of a path/key name instead
                // of a standalone secret.
                let boundary_ok = before.is_none_or(|b| !b.is_ascii_alphanumeric())
                    && after.is_none_or(|b| !b.is_ascii_alphanumeric());
                if boundary_ok {
                    "[REDACTED]".to_string()
                } else {
                    m.as_str().to_string()
                }
            })
            .into_owned();
    }
    out
}

// Lazy static compiled once. `Regex::new` is expensive; these are global.
static REDACTION_PATTERNS: std::sync::LazyLock<Vec<Regex>> = std::sync::LazyLock::new(|| {
    let raw = [
        // Authorization: Bearer <token> / Basic <base64> / <raw token>
        r"(?i)\bauthorization\s*[:=]\s*(bearer\s+|basic\s+)?[A-Za-z0-9._~+/=-]+",
        // Common key/value secret names.
        r"(?i)\b(api[_-]?key|apikey|refresh[_-]?token|access[_-]?token|client[_-]?secret|secret|password)\b\s*[:=]\s*\S+",
        // Bearer token anywhere.
        r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{12,}",
        // Long random-looking tokens (base64/hex, >= 24 chars) not part of a
        // path or already-redacted marker. This is the last-resort net. The
        // boundary guard (not surrounded by alphanumerics) is applied in the
        // replacer closure: the `regex` crate does not support look-around.
        r"(?i)[a-z0-9+/]{24,}={0,2}",
    ];
    raw.iter()
        .map(|p| Regex::new(p).expect("static redaction regex must compile"))
        .collect()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_bearer_token() {
        let out = redact("Authorization: Bearer abc.def.ghi-1234567890abcdef");
        assert!(!out.contains("abc.def.ghi-1234567890abcdef"));
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn redacts_key_value_secrets() {
        let out = redact("api_key=sk-abcdefghijklmnopqrstuvwxyz012345");
        assert!(!out.contains("sk-abcdefghijklmnopqrstuvwxyz012345"));
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn redacts_long_random_tokens() {
        let out = redact("session leaked dGhpcyBpcyBhIHNlY3JldCB0b2tlbiB2YWx1ZQ==");
        assert!(!out.contains("dGhpcyBpcyBhIHNlY3JldCB0b2tlbiB2YWx1ZQ=="));
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn keeps_plain_text_intact() {
        let out = redact("engine fake started ok");
        assert_eq!(out, "engine fake started ok");
    }

    #[test]
    fn does_not_redact_paths() {
        let out = redact(r"C:\Users\alice\projects\my-project-2026\src\lib.rs");
        assert_eq!(out, r"C:\Users\alice\projects\my-project-2026\src\lib.rs");
    }

    #[test]
    fn mixed_authorization_then_long_token_redacts_both_without_panic() {
        // T-051 regression: the first replacement changes `out` length; a later
        // standalone long token must still be redacted against the new length
        // (no panic, no leak).
        let input = "Authorization: Bearer abc.def.ghi-1234567890abcdef then session leaked dGhpcyBpcyBhIHNlY3JldCB0b2tlbiB2YWx1ZQ== end";
        let out = redact(input);
        assert!(!out.contains("abc.def.ghi-1234567890abcdef"), "bearer must redact");
        assert!(
            !out.contains("dGhpcyBpcyBhIHNlY3JldCB0b2tlbiB2YWx1ZQ=="),
            "later long token must redact"
        );
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn many_prior_short_secrets_then_long_token_does_not_panic() {
        let mut input = String::new();
        for i in 0..50 {
            input.push_str(&format!("api_key=short{i} "));
        }
        input.push_str("leak dGhpcyBpcyBhIHNlY3JldCB0b2tlbiB2YWx1ZQ==");
        let out = redact(&input);
        assert!(!out.contains("dGhpcyBpcyBhIHNlY3JldCB0b2tlbiB2YWx1ZQ=="));
    }

    #[test]
    fn diagnostics_ring_is_bounded() {
        let d = Diagnostics::new();
        for i in 0..(MAX_RECENT_ERRORS + 10) {
            d.record_error(format!("code{i}"), format!("msg{i}"));
        }
        assert_eq!(d.recent_errors().len(), MAX_RECENT_ERRORS);
    }
}
