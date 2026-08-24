//! Per-runtime random server secret (TASK 10 §21–§24).
//!
//! The secret is generated fresh per runtime (never persisted — §84), is
//! random (128 bits from uuid v4, which is backed by the OS CSPRNG), is
//! transported via environment variable (never argv — §23, since argv is
//! visible to local process inspection), and its `Debug` is redacted so it
//! can never leak through a panic report or diagnostic dump.

use uuid::Uuid;

/// A per-runtime local-server password. Never persisted; never logged.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// Fresh random secret: 128 bits of CSPRNG output, hex-encoded.
    pub fn generate() -> Self {
        Self(format!("{:x}", Uuid::new_v4()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Replace every occurrence of the secret value in a diagnostic string
    /// with `***` (§84). A misbehaving server (or an error body that echoes
    /// the Authorization header) can never push the runtime secret into a
    /// log or surfaced error.
    pub fn redact(&self, text: &str) -> String {
        if self.0.is_empty() {
            return text.to_string();
        }
        text.replace(self.as_str(), "***")
    }
}

/// `Debug` never reveals the value (§24, §74).
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_is_random_and_debug_is_redacted() {
        let a = Secret::generate();
        let b = Secret::generate();
        assert_ne!(a.as_str(), b.as_str());
        assert!(a.as_str().len() >= 32);
        // The value must never appear in Debug output.
        let debug = format!("{a:?}");
        assert!(!debug.contains(a.as_str()));
        assert_eq!(debug, "Secret(***)");
    }
}
