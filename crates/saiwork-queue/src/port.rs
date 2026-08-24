//! EnginePort — the queue's narrow, typed window into engine execution.
//!
//! The queue must dispatch through the engine but must not know engine
//! internals (§116). The `EnginePort` is implemented by the orchestration
//! layer (`saiwork-core` bridges its registry + session manager) so
//! `saiwork-queue` depends on nothing above its own crate. Error categories
//! are typed (`PortError`) — raw engine DTOs never cross this boundary.

use async_trait::async_trait;

use crate::model::PortError;

/// The queue's view of engine availability (derived, minimal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    Ready,
    NotReady,
    Failed,
}

/// Authoritative dispatch receipt: the queue may commit DISPATCHED **only**
/// on `Accepted` — never from a locally allocated RunId or MessageStarted
/// (TASK 24 §9). `OutcomeUnknown` (transport loss / engine death across the
/// acceptance boundary) must never auto-redispatch; the item becomes UNKNOWN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchReceipt {
    /// The engine authoritatively accepted the prompt; the run is live.
    Accepted { run_id: String },
    /// The engine definitively rejected the prompt before executing it.
    /// `run_id` identifies the run that carries the FAILED terminal.
    DefinitelyRejected {
        run_id: String,
        code: String,
        message: String,
    },
    /// The send crossed the boundary but acceptance cannot be proven.
    /// `run_id` lets the caller correlate the eventual authoritative
    /// terminal (`message.outcome_unknown` / `message.failed`).
    OutcomeUnknown { run_id: String, message: String },
}

/// Authoritative session-creation outcome: `Created` only when the engine
/// proved creation. `CreationUnknown` must never loop-create orphan sessions
/// (TASK 24 §9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionCreateOutcome {
    /// The engine authoritatively created the session.
    Created { session_id: String },
    /// Definite rejection before any creation happened.
    DefinitelyNotCreated { code: String, message: String },
    /// The create crossed the boundary; creation may have happened.
    CreationUnknown { message: String },
}

/// One dispatch-capable engine surface.
#[async_trait]
pub trait EnginePort: Send + Sync {
    fn engine_state(&self, engine_id: &str) -> EngineState;

    /// Engine availability for a specific workspace. A healthy engine bound
    /// to a DIFFERENT workspace is not ready for this item: the queue must
    /// Wait (Notify on explicit restart/rebind), never FAILED and never
    /// auto-rebind (TASK 24 §9). Default = plain `engine_state` for ports
    /// without binding knowledge.
    fn engine_state_for_workspace(&self, engine_id: &str, _workspace_id: &str) -> EngineState {
        self.engine_state(engine_id)
    }

    /// True when the SAIWORK2 session is known to this process.
    fn session_exists(&self, session_id: &str) -> bool;

    /// True when the session has an active run (arbitration with direct
    /// sends, §31–§32, §210–§211). Queued dispatch never enters a busy
    /// session.
    fn session_busy(&self, session_id: &str) -> bool;

    /// Validate an `existing`-mode enqueue target BEFORE durable persistence
    /// (TASK 24 §9): the session must belong to the item's engine AND the
    /// item's workspace, otherwise queued execution could route through a
    /// different engine than the row claims. The default accepts; the core
    /// bridge validates against the authoritative session map.
    fn validate_enqueue(
        &self,
        _session_id: &str,
        _engine_id: &str,
        _workspace_id: &str,
    ) -> Result<(), PortError> {
        Ok(())
    }

    /// Hydrate an existing session (e.g. after restart, when the in-memory
    /// map is empty). Errors `SessionNotFound` if the session is gone.
    async fn ensure_session(&self, session_id: &str) -> Result<(), PortError>;

    /// Create a fresh engine session. `Created` is the only outcome that
    /// proves an external session exists; `CreationUnknown` must never be
    /// auto-retried (no orphan-session loops).
    async fn create_session(
        &self,
        engine_id: &str,
        workspace_id: Option<&str>,
        model: Option<&str>,
    ) -> Result<SessionCreateOutcome, PortError>;

    /// Dispatch a prompt; returns the authoritative dispatch receipt. The
    /// queue commits DISPATCHED only on `Accepted` (TASK 24 §9).
    async fn send(
        &self,
        session_id: &str,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<DispatchReceipt, PortError>;

    /// Cross-authority durability compensation (TASK 24 §9): delete a
    /// session the queue created but failed to persist into the queue row.
    /// `Ok` = authoritative upstream deletion proven (or engine-tolerated,
    /// e.g. a connection-owned session that dies with its runtime); `Err` =
    /// cleanup failed/unsupported — the caller must fail closed (UNKNOWN),
    /// never retry as a clean NewSession.
    async fn delete_session(&self, session_id: &str) -> Result<(), PortError>;

    /// Cancel a run. Idempotent for unknown/completed runs (engine contract).
    async fn cancel(&self, session_id: &str, run_id: &str) -> Result<(), PortError>;
}
