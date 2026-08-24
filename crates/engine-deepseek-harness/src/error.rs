//! Typed error taxonomy (TASK 20 §71–§72). Layered: the adapter maps these
//! into the generic `EngineError::Engine { .. }` at the `EngineAdapter`
//! boundary — user-safe messages, never raw protocol payloads.

use std::time::Duration;

/// Adapter-local error categories. Messages are user-safe (no raw protocol
/// bodies, no credentials, no prompts).
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("deepseek-harness executable not found (set SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE)")]
    HarnessNotFound,

    #[error("deepseek-harness probe failed: {0}")]
    ProbeFailed(String),

    #[error("deepseek-harness version/protocol unsupported: {0}")]
    UnsupportedVersion(String),

    #[error("deepseek-harness configuration invalid: {0}")]
    ConfigurationInvalid(String),

    #[error("deepseek-harness spawn failed: {0}")]
    SpawnFailed(String),

    #[error("harness exited during startup: {0}")]
    ExitedDuringStartup(String),

    #[error("harness protocol transport closed: {0}")]
    TransportClosed(String),

    #[error("harness handshake timed out after {0:?}")]
    HandshakeTimeout(Duration),

    #[error("harness handshake rejected: {0}")]
    HandshakeRejected(String),

    #[error("harness protocol version mismatch: {0}")]
    ProtocolVersionMismatch(String),

    #[error("malformed protocol frame: {0}")]
    MalformedFrame(String),

    #[error("protocol frame exceeds the {0} byte cap")]
    MessageTooLarge(usize),

    #[error("protocol decode error: {0}")]
    ProtocolDecode(String),

    #[error("request '{method}' timed out after {timeout:?}")]
    RequestTimeout { method: String, timeout: Duration },

    #[error("request '{method}' rejected: {message} (code {code})")]
    RequestRejected {
        method: String,
        code: i64,
        message: String,
    },

    #[error("harness runtime lost: {0}")]
    RuntimeLost(String),

    #[error("operation canceled")]
    Canceled,

    #[error(
        "harness startup failed: {startup}; cleanup failed and process termination is unproven: {cleanup}"
    )]
    StartupCleanupFailed { startup: String, cleanup: String },

    #[error(
        "previous harness process {pid} termination is unproven; stop or kill it before restarting"
    )]
    PreviousRuntimeTerminationUnproven { pid: u32 },

    #[error("harness session '{session_id}' not found (sessions are connection-owned and do not survive a runtime restart)")]
    SessionNotFound { session_id: String },

    #[error("harness session '{session_id}' is busy with an active turn")]
    SessionBusy { session_id: String },

    #[error("harness rejected the turn: {0}")]
    TurnRejected(String),

    #[error("turn outcome unknown: {0}")]
    TurnOutcomeUnknown(String),

    #[error("unsupported by this adapter: {0}")]
    Unsupported(String),

    #[error("internal adapter error: {0}")]
    Internal(String),
}

impl HarnessError {
    /// Map into the generic engine boundary error (adapter firewall).
    /// Cancellation maps to the canonical `EngineError::Canceled` (matching
    /// the FakeEngine contract for stop-during-start, ENGINE_CONTRACT.md);
    /// session-not-found and session-busy map to the canonical generic
    /// variants so the UI/queue classify them correctly (ENGINE_CONTRACT.md).
    pub fn engine(self) -> saiwork_core::engine::EngineError {
        match self {
            HarnessError::Canceled => saiwork_core::engine::EngineError::Canceled,
            HarnessError::SessionNotFound { session_id } => {
                saiwork_core::engine::EngineError::SessionNotFound { session_id }
            }
            HarnessError::SessionBusy { session_id } => {
                saiwork_core::engine::EngineError::SessionBusy { session_id }
            }
            other => saiwork_core::engine::EngineError::engine(super::ENGINE_ID, other.to_string()),
        }
    }
}
