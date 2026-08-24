//! Adapter-local session registry (TASK 21 §7, §11–§13).
//!
//! Harness ACP sessions are **fresh + connection-owned**: the authoritative
//! live session set for a runtime is what this adapter created on this
//! connection (DEEPSEEK_HARNESS.md §8). SAIWORK2 persists only session
//! metadata (SessionManager); the upstream Harness session id stays opaque and
//! Harness-owned. This registry is the only place that maps the SAIWORK2
//! session id (the generic surface) to the opaque Harness session id (the
//! `session/new` result), so generic identity never collides across engines
//! (§7: Harness session "abc" ≠ OpenCode session "abc").
//!
//! Sessions do not survive a runtime restart: a fresh runtime is a fresh
//! connection with fresh sessions (`session_resume = false`). After a
//! restart, sends to a stale session id fail with `SessionNotFound` — honest,
//! never a fabricated reconstruction (§10, §75).

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub(crate) struct HarnessSession {
    /// SAIWORK2 session id (the generic surface).
    pub saiwork_id: String,
    /// Upstream Harness/ACP session id (opaque).
    pub harness_id: String,
    pub display_name: String,
}

#[derive(Default)]
pub(crate) struct SessionRegistry {
    by_saiwork: Mutex<HashMap<String, HarnessSession>>,
    by_harness: Mutex<HashMap<String, String>>,
}

impl SessionRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&self, session: HarnessSession) {
        self.by_harness
            .lock()
            .expect("session registry mutex poisoned")
            .insert(session.harness_id.clone(), session.saiwork_id.clone());
        self.by_saiwork
            .lock()
            .expect("session registry mutex poisoned")
            .insert(session.saiwork_id.clone(), session);
    }

    pub(crate) fn get(&self, saiwork_id: &str) -> Option<HarnessSession> {
        self.by_saiwork
            .lock()
            .expect("session registry mutex poisoned")
            .get(saiwork_id)
            .cloned()
    }

    pub(crate) fn list(&self) -> Vec<HarnessSession> {
        self.by_saiwork
            .lock()
            .expect("session registry mutex poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn remove(&self, saiwork_id: &str) -> Option<HarnessSession> {
        let session = self
            .by_saiwork
            .lock()
            .expect("session registry mutex poisoned")
            .remove(saiwork_id);
        if let Some(s) = &session {
            self.by_harness
                .lock()
                .expect("session registry mutex poisoned")
                .remove(&s.harness_id);
        }
        session
    }

    /// Drop every session. Sessions are connection-owned: when the runtime is
    /// torn down (stop/kill/crash) the registry must not retain stale ids
    /// (§75 — a fresh runtime is a fresh connection with no sessions).
    pub(crate) fn clear(&self) {
        self.by_saiwork
            .lock()
            .expect("session registry mutex poisoned")
            .clear();
        self.by_harness
            .lock()
            .expect("session registry mutex poisoned")
            .clear();
    }
}
