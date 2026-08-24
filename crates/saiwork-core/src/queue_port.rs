//! `EnginePort` bridge (TASK 13): the durable queue's typed window into
//! engine execution. Implemented over `EngineRegistry` + `SessionManager`, so
//! `saiwork-queue` stays free of core/engine internals (dependency direction
//! UI → core → queue → events/storage, ENGINE_CONTRACT.md §116).

use std::sync::Arc;

use async_trait::async_trait;
use saiwork_queue::{DispatchReceipt, EnginePort, EngineState, PortError, SessionCreateOutcome};

use crate::engine::{EngineError, EngineHealth, EngineRegistry};
use crate::error::CoreError;
use crate::sessions::SessionManager;

pub struct QueueEnginePort {
    engines: Arc<EngineRegistry>,
    sessions: Arc<SessionManager>,
}

impl QueueEnginePort {
    pub fn new(engines: Arc<EngineRegistry>, sessions: Arc<SessionManager>) -> Self {
        Self { engines, sessions }
    }
}

fn map_engine_error(e: EngineError) -> PortError {
    match e {
        EngineError::SessionNotFound { session_id } => {
            PortError::SessionNotFound(format!("session '{session_id}' not found"))
        }
        EngineError::SessionBusy { session_id } => {
            PortError::SessionBusy(format!("session '{session_id}' is busy"))
        }
        EngineError::NotStarted { .. }
        | EngineError::NotReady { .. }
        | EngineError::Crashed { .. } => PortError::EngineUnavailable(e.to_string()),
        EngineError::Auth(_) => PortError::Auth(e.to_string()),
        EngineError::Network(_) => PortError::Network(e.to_string()),
        EngineError::Protocol(_) => PortError::Protocol(e.to_string()),
        EngineError::UnsupportedCapability { .. } => PortError::Invalid(e.to_string()),
        EngineError::Canceled => PortError::Internal(e.to_string()),
        other => PortError::Provider(other.to_string()),
    }
}

fn map_error(e: CoreError) -> PortError {
    match e {
        CoreError::Engine(engine) => map_engine_error(engine),
        CoreError::SessionNotFound(sid) => PortError::SessionNotFound(sid),
        CoreError::SessionNotResumable(sid) => PortError::Invalid(format!(
            "session '{sid}' has no trustworthy upstream session id and is not resumable"
        )),
        CoreError::WorkspaceBusy { workspace_id, .. } => {
            // The queue should never reach send() with a busy workspace
            // (session_busy gates it); this is the conservative mapping for
            // the narrow race where a direct send slipped in between the
            // pre-dispatch busy check and the engine call.
            PortError::SessionBusy(format!("workspace '{workspace_id}' has an active run"))
        }
        CoreError::WorkspaceMismatch {
            engine_id,
            expected_workspace_id,
            requested_workspace_id,
        } => PortError::Invalid(format!(
            "engine '{engine_id}' is bound to workspace '{expected_workspace_id}' but the session workspace is '{requested_workspace_id}'; restart the engine for that workspace"
        )),
        CoreError::SessionBusy { session_id } => PortError::SessionBusy(session_id),
        CoreError::NotReady => PortError::EngineUnavailable("application not ready".into()),
        CoreError::ShuttingDown => {
            PortError::EngineUnavailable("application is shutting down".into())
        }
        other => PortError::Internal(other.to_string()),
    }
}

#[async_trait]
impl EnginePort for QueueEnginePort {
    fn engine_state(&self, engine_id: &str) -> EngineState {
        match self.engines.get(engine_id) {
            None => EngineState::NotReady,
            Some(engine) => match engine.health() {
                EngineHealth::Ready => EngineState::Ready,
                EngineHealth::Failed { .. } => EngineState::Failed,
                _ => EngineState::NotReady,
            },
        }
    }

    /// Workspace-aware readiness (TASK 24 §9): a healthy engine bound to a
    /// different workspace is NotReady for this item — the queue Waits and
    /// never converts the binding mismatch into FAILED. An explicit
    /// stop/start for the target workspace wakes the dispatcher.
    fn engine_state_for_workspace(&self, engine_id: &str, workspace_id: &str) -> EngineState {
        match self.engines.bound_workspace(engine_id) {
            // Not started, or started without a workspace binding: the
            // engine's own state decides.
            None | Some(None) => self.engine_state(engine_id),
            Some(Some(bound)) => {
                if bound != workspace_id {
                    EngineState::NotReady
                } else {
                    self.engine_state(engine_id)
                }
            }
        }
    }

    fn session_exists(&self, session_id: &str) -> bool {
        self.sessions.get(session_id).is_some()
    }

    fn session_busy(&self, session_id: &str) -> bool {
        // Same-session busy OR same-workspace busy (TASK 18 §21): the queue
        // never dispatches a second agent run into a workspace that already
        // has one active (returns Wait in `resolve_session`).
        self.sessions.busy_for_dispatch(session_id)
    }

    /// Enqueue-time target validation (TASK 24 §9): an `existing`-mode item
    /// must reference a session that belongs to the item's engine AND the
    /// item's workspace — otherwise durable execution could route through a
    /// different engine than the row claims. Rejected BEFORE persistence.
    fn validate_enqueue(
        &self,
        session_id: &str,
        engine_id: &str,
        workspace_id: &str,
    ) -> Result<(), PortError> {
        let session = self
            .sessions
            .ensure_loaded(session_id)
            .map_err(|e| match e {
                CoreError::SessionNotFound(sid) => PortError::SessionNotFound(sid),
                CoreError::SessionNotResumable(sid) => PortError::Invalid(format!(
                    "session '{sid}' has no trustworthy upstream session id and is not resumable"
                )),
                // AUDIT-W2-001: a live connection-owned session whose engine
                // generation died is unusable history — same typed rule as
                // direct send.
                CoreError::SessionNotUsableNow { session_id: sid } => PortError::Invalid(format!(
                    "session '{sid}' is not usable with the current engine runtime generation"
                )),
                other => PortError::Internal(other.to_string()),
            })?;
        if session.engine_id != engine_id {
            return Err(PortError::Invalid(format!(
                "session '{session_id}' belongs to engine '{}' but the queue item targets engine '{engine_id}'",
                session.engine_id
            )));
        }
        if session.workspace_id.as_deref() != Some(workspace_id) {
            return Err(PortError::Invalid(format!(
                "session '{session_id}' belongs to workspace '{:?}' but the queue item targets workspace '{workspace_id}'",
                session.workspace_id
            )));
        }
        Ok(())
    }

    async fn ensure_session(&self, session_id: &str) -> Result<(), PortError> {
        self.sessions
            .ensure_loaded(session_id)
            .map(|_| ())
            .map_err(map_error)
    }

    async fn create_session(
        &self,
        engine_id: &str,
        workspace_id: Option<&str>,
        model: Option<&str>,
    ) -> Result<SessionCreateOutcome, PortError> {
        match self.sessions.create(engine_id, workspace_id, model).await {
            Ok(s) => Ok(SessionCreateOutcome::Created {
                session_id: s.id,
            }),
            Err(CoreError::Engine(EngineError::OutcomeUnknown(message))) => {
                // The create request may have reached the engine: never
                // auto-retry into an orphan-session loop (TASK 24 §9).
                Ok(SessionCreateOutcome::CreationUnknown { message })
            }
            Err(e) => Err(map_error(e)),
        }
    }

    async fn send(
        &self,
        session_id: &str,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<DispatchReceipt, PortError> {
        self.sessions
            .send_for_dispatch(session_id, prompt, model)
            .await
            .map_err(map_error)
    }

    async fn cancel(&self, session_id: &str, run_id: &str) -> Result<(), PortError> {
        self.sessions
            .cancel(session_id, run_id)
            .await
            .map_err(map_error)
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), PortError> {
        self.sessions
            .delete_session(session_id)
            .await
            .map_err(map_error)
    }
}
