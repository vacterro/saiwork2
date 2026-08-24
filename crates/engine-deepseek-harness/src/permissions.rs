//! Pending permission registry (TASK 21 §55–§62).
//!
//! A Harness `session/request_permission` server request is routed to the
//! permission handler task, which publishes the generic `permission.requested`
//! and waits on a decision channel. `resolve_permission` (the generic surface)
//! takes the pending entry and sends the user decision; the handler answers
//! the upstream request and publishes the authoritative `permission.resolved`.
//!
//! Fail-closed (§57): if the decision never arrives — run terminal, engine
//! stop/crash, transport death, or the sender dropping — the handler responds
//! `reject` (deny). Never default Allow. Duplicate/stale resolutions are a
//! no-op (`take` returns None → no second protocol command, §58–§60).

use std::collections::HashMap;
use std::sync::Mutex;

use saiwork_core::engine::PendingPermissionInfo;
use tokio::sync::oneshot;

pub(crate) struct PendingPermission {
    /// Session that owns this request (W2-002): the generic permission surface
    /// advertises session-scoped mutation, so the owner correlation must be
    /// retained. `resolve_permission` verifies this before consuming.
    pub session_id: String,
    /// Run that owns this request (stale-generation guard, §59).
    pub run_id: String,
    /// SAIWORK2 request id (authoritative identity for the UI card).
    pub request_id: String,
    /// Bounded, safe permission detail (§62) for reconciliation after a
    /// bounded-bus lag (W2-004): a missed `permission.requested` can be
    /// reconstructed from this snapshot.
    pub detail: String,
    /// Decision channel: `resolve_permission` sends the user decision; a drop
    /// (run terminal / teardown) resolves as reject.
    pub decision_tx: oneshot::Sender<bool>,
}

#[derive(Default)]
pub(crate) struct PermissionRegistry {
    by_request: Mutex<HashMap<String, PendingPermission>>,
}

impl PermissionRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&self, request_id: String, pending: PendingPermission) {
        // Bounded by live permission requests; every entry is released on
        // resolution, run terminal, or teardown — never accumulated (§170).
        self.by_request
            .lock()
            .expect("permission registry mutex poisoned")
            .insert(request_id, pending);
    }

    /// Take a pending request by its SAIWORK2 request id. Returns None for an
    /// unknown/already-resolved/stale request (idempotent no-op, §58–§60).
    pub(crate) fn take(&self, request_id: &str) -> Option<PendingPermission> {
        self.by_request
            .lock()
            .expect("permission registry mutex poisoned")
            .remove(request_id)
    }

    /// Take every pending entry (teardown / engine stop). Dropping the
    /// returned senders releases every awaiting handler, which settles
    /// fail-closed (§73).
    pub(crate) fn clear(&self) -> Vec<PendingPermission> {
        std::mem::take(
            &mut *self
                .by_request
                .lock()
                .expect("permission registry mutex poisoned"),
        )
        .into_values()
        .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.by_request
            .lock()
            .expect("permission registry mutex poisoned")
            .len()
    }

    /// W2-002: peek whether the pending entry for `request_id` belongs to
    /// `session_id` (owner correlation) without consuming it. Used by
    /// `resolve_permission` to reject a mismatched `(session_id, request_id)`
    /// call without consuming another session's entry.
    pub(crate) fn session_matches(&self, request_id: &str, session_id: &str) -> bool {
        self.by_request
            .lock()
            .expect("permission registry mutex poisoned")
            .get(request_id)
            .is_some_and(|p| p.session_id == session_id)
    }

    /// Authoritative snapshot of every open permission request (W2-004): the
    /// exact session/run/request ownership reconciliation rebuilds the UI
    /// permission cards from after a bounded-bus lag. Bounded by live requests;
    /// every entry is released on resolution, run terminal, or teardown.
    pub(crate) fn snapshot(&self) -> Vec<PendingPermissionInfo> {
        self.by_request
            .lock()
            .expect("permission registry mutex poisoned")
            .values()
            .map(|p| PendingPermissionInfo {
                session_id: p.session_id.clone(),
                run_id: p.run_id.clone(),
                request_id: p.request_id.clone(),
                detail: p.detail.clone(),
            })
            .collect()
    }
}
