//! Core error domains (ENGINE_CONTRACT.md error model).

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("workspace '{path}' is not a directory")]
    NotADirectory { path: PathBuf },

    #[error("workspace '{path}' cannot be canonicalized: {source}")]
    Canonicalize {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("workspace not found: {0}")]
    WorkspaceNotFound(String),

    #[error("storage error: {0}")]
    Storage(#[from] saiwork_storage::StorageError),

    #[error("engine error: {0}")]
    Engine(#[from] crate::engine::EngineError),

    #[error("process error: {0}")]
    Process(#[from] saiwork_process::ProcessError),

    #[error("queue error: {0}")]
    Queue(#[from] saiwork_queue::QueueError),

    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Legacy metadata row whose upstream session id was never recorded
    /// (NULL/empty, migration v4): historical display only — never usable for
    /// engine calls (TASK 24 §9).
    #[error("session '{0}' has no trustworthy upstream session id and is not resumable")]
    SessionNotResumable(String),

    /// The session belongs to a PREVIOUS engine runtime generation: usable
    /// only while the connection-owned runtime that owns it is alive, or the
    /// engine is not currently READY (TASK 24 §9). Distinct from
    /// `SessionNotResumable` (no upstream id at all).
    #[error("session '{session_id}' is not usable with the current engine runtime — restart the engine (or create a new session)")]
    SessionNotUsableNow { session_id: String },

    #[error("session '{session_id}' is busy with an active run")]
    SessionBusy { session_id: String },

    #[error("session '{session_id}' is still in use: {reason}")]
    SessionInUse { session_id: String, reason: String },

    /// TASK 24 §9/§120: the generic session-id namespace is the adapter's own
    /// id (engine events re-emit it verbatim), so a second engine returning
    /// the same session id must fail closed rather than silently overwrite the
    /// first engine's session in the generic map/DB.
    #[error("session id '{session_id}' already belongs to engine '{existing_engine_id}' (cannot be reused by engine '{engine_id}')")]
    SessionIdConflict {
        session_id: String,
        engine_id: String,
        existing_engine_id: String,
    },

    /// TASK 18 §21–§22: one mutating agent run per physical workspace (no
    /// worktrees). Another session in the same workspace has an active run.
    #[error("workspace '{workspace_id}' has an active run in session '{active_session_id}' (one agent run per workspace)")]
    WorkspaceBusy {
        workspace_id: String,
        active_session_id: String,
        attempted_session_id: String,
    },

    /// The engine runtime is bound to one workspace (persisted at start);
    /// the requested session workspace differs (TASK 24 §9). The UI must
    /// restart the engine for the target workspace.
    #[error("engine '{engine_id}' is bound to workspace '{expected_workspace_id}' but the session workspace is '{requested_workspace_id}'; restart the engine for that workspace")]
    WorkspaceMismatch {
        engine_id: String,
        expected_workspace_id: String,
        requested_workspace_id: String,
    },

    /// The durable Queue holds an UNKNOWN item in this workspace (TASK 24
    /// §9): the external run may still be live even after a restart, so a
    /// direct send into the same workspace is rejected BEFORE any reservation
    /// or engine call. Only an explicit risk-confirmed resolution of the
    /// UNKNOWN item (or its authoritative terminal) clears the gate.
    #[error("workspace '{workspace_id}' has an UNKNOWN queue run whose outcome may still be live; resolve or abandon it explicitly before sending here")]
    WorkspaceOutcomeUnknown { workspace_id: String },

    /// Forget (TASK 24 §9): the workspace cannot be deleted while live
    /// services still require it — an active run, a bound engine runtime, or
    /// nonterminal durable queue work. The row is retained until the caller
    /// resolves those references.
    #[error("workspace '{workspace_id}' is still in use: {reason}")]
    WorkspaceInUse { workspace_id: String, reason: String },

    /// The direct-send boundary (TASK 24 §9): the UI sends a prompt with the
    /// workspace/engine it *believes* is active. When that context does not
    /// match the session's own metadata, the send is rejected BEFORE any
    /// reservation or external call — a stale UI can never execute the wrong
    /// session's engine/workspace.
    #[error("session '{session_id}' does not match the active UI context (expected engine '{expected_engine_id}' / workspace '{expected_workspace_id:?}', but the session belongs to engine '{actual_engine_id}' / workspace '{actual_workspace_id:?}'); switch to the session's project/engine")]
    SessionContextMismatch {
        session_id: String,
        expected_engine_id: String,
        expected_workspace_id: Option<String>,
        actual_engine_id: String,
        actual_workspace_id: Option<String>,
    },

    #[error("run '{run_id}' does not match the active run in session '{session_id}'")]
    SessionRunMismatch {
        session_id: String,
        run_id: String,
    },

    #[error("operation canceled")]
    Canceled,

    #[error("application is not ready yet")]
    NotReady,

    #[error("app is shutting down")]
    ShuttingDown,

    #[error("preset import error: {0}")]
    PresetImport(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for CoreError {
    fn from(source: std::io::Error) -> Self {
        CoreError::Internal(format!("io: {source}"))
    }
}
