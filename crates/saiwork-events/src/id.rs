//! Typed domain identifiers (EVENTS.md, TASK 04 §4–5).
//!
//! `WorkspaceId != SessionId != EngineId` at the type level: passing the
//! wrong kind of ID into a function or event is a compile error, not a
//! runtime surprise. Each ID is an opaque `Arc<str>` newtype — cheap to
//! clone, `Send + Sync`, hashable, comparable, and serde-transparent (it
//! serializes as a plain string, so the Rust↔TS wire shape is unchanged).
//!
//! IDs are created by core authorities only (never generated in the UI) and
//! their textual form is stable per concept. No UUID framework is used: the
//! values are opaque by construction and their concrete representation is
//! an implementation detail of whoever allocates them.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone)]
        pub struct $name(Arc<str>);

        impl $name {
            /// Wrap an identifier. The caller (core authority) owns the
            /// concrete textual form.
            pub fn new(value: impl Into<String>) -> Self {
                Self(Arc::from(value.into().as_str()))
            }

            /// The opaque textual form (for storage, logs, display).
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Accessor to the inner string for legacy call sites.
            pub fn to_string_owned(&self) -> String {
                self.0.to_string()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0.as_ref()).finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for $name {}

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = String::deserialize(deserializer)?;
                Ok(Self::new(s))
            }
        }
    };
}

id_type! {
    /// Stable identity of an opened workspace (derived from its normalized
    /// location, not a display name — WORKSPACE.md).
    WorkspaceId
}

id_type! {
    /// Identity of a registered engine adapter (e.g. `fake`, later `opencode`).
    EngineId
}

id_type! {
    /// Identity of a SAIWORK2 session (metadata; content stays with the engine).
    SessionId
}

id_type! {
    /// Identity of a single run (one prompt execution) inside a session.
    RunId
}

id_type! {
    /// Identity of a streamed message part (deltas of one message share it).
    MessageId
}

id_type! {
    /// Identity of a durable queue item.
    QueueItemId
}

id_type! {
    /// Identity of a permission request/response pair.
    RequestId
}

id_type! {
    /// Identity of a supervised OS child process (distinct from the OS PID:
    /// PIDs may be reused, ProcessId never is within one run).
    ProcessId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_distinct_types_with_stable_text() {
        let w = WorkspaceId::new("ws-1");
        let e = EngineId::new("fake");
        let s = SessionId::new("sess-1");
        assert_eq!(w.as_str(), "ws-1");
        assert_eq!(e.as_str(), "fake");
        assert_eq!(s.as_str(), "sess-1");
        // Compile-time guard: assigning/equating different ID kinds is a type
        // error (cannot be tested at runtime; the types are structurally
        // distinct by construction).
        assert_ne!(format!("{w:?}"), format!("{e:?}"));
    }

    #[test]
    fn ids_clone_hash_and_compare() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(WorkspaceId::new("a"));
        set.insert(WorkspaceId::new("a"));
        set.insert(WorkspaceId::new("b"));
        assert_eq!(set.len(), 2);
        assert!(WorkspaceId::new("a") == WorkspaceId::new("a"));
        assert!(WorkspaceId::new("a") < WorkspaceId::new("b"));
    }

    #[test]
    fn ids_serialize_as_plain_strings() {
        let json = serde_json::to_value(EngineId::new("opencode")).unwrap();
        assert_eq!(json, serde_json::json!("opencode"));
        let back: EngineId = serde_json::from_value(json).unwrap();
        assert_eq!(back.as_str(), "opencode");
    }
}
