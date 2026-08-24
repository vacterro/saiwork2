//! Typed OpenCode adapter errors (TASK 10 §33, TASK 11 §56–§58).
//!
//! Startup failures are classified — never a bare "opencode failed to start"
//! — and TASK 11 adds the session/protocol layer categories: HTTP status
//! mapping, protocol failures, provider errors, and run-level outcomes.
//! Each variant maps onto the canonical `EngineError` at the adapter
//! boundary, so consumers get one stable surface while diagnostics keep the
//! specific cause.

use std::path::PathBuf;
use std::time::Duration;

use saiwork_core::engine::EngineError;

#[derive(Debug, thiserror::Error)]
pub enum OpenCodeError {
    #[error("OpenCode executable not found (searched: {searched:?})")]
    ExecutableNotFound { searched: Vec<String> },

    #[error("explicit OpenCode executable is not usable: {path} — {reason}")]
    ExplicitExecutableInvalid { path: PathBuf, reason: String },

    #[error("discovered launcher cannot be executed directly: {path}")]
    UnsupportedLauncher { path: PathBuf },

    #[error("OpenCode probe failed ({executable}): {detail}")]
    ProbeFailed { executable: String, detail: String },

    #[error("workspace is not a usable directory: {path}")]
    InvalidWorkspace { path: PathBuf },

    #[error("port unavailable (after {attempts} attempts)")]
    PortUnavailable { attempts: u32 },

    #[error("failed to spawn OpenCode process: {detail}")]
    SpawnFailed { detail: String },

    #[error("OpenCode did not become ready within {timeout:?} (endpoint {endpoint}; {detail})")]
    ReadinessTimeout {
        endpoint: String,
        timeout: Duration,
        detail: String,
    },

    #[error("OpenCode process exited during startup (code {code:?}){tail}")]
    ExitedDuringStartup {
        code: Option<i32>,
        /// Safe tail of captured output (bounded, redacted).
        tail: String,
    },

    #[error("OpenCode endpoint answered but is not an OpenCode server: {detail}")]
    ProtocolUnexpected { detail: String },

    #[error("OpenCode server authentication configuration failed: {detail}")]
    AuthConfigurationFailed { detail: String },

    #[error("OpenCode startup was canceled")]
    Cancelled,

    #[error(
        "OpenCode startup failed: {startup}; cleanup failed and process termination is unproven: {cleanup}"
    )]
    StartupCleanupFailed { startup: String, cleanup: String },

    #[error(
        "previous OpenCode process {pid} termination is unproven; stop or kill it before restarting"
    )]
    PreviousRuntimeTerminationUnproven { pid: u32 },

    // ---- TASK 11: session/protocol layer (engine READY, request failed) ----
    #[error("OpenCode engine is not ready (phase {phase:?})")]
    NotReady { phase: &'static str },

    #[error("OpenCode session '{session_id}' not found")]
    SessionNotFound { session_id: String },

    #[error(
        "OpenCode session '{session_id}' has an active run; one run per session (TASK 11 §70–§72)"
    )]
    SessionBusy { session_id: String },

    #[error("OpenCode session '{session_id}' already has a run with id '{run_id}'")]
    DuplicateRun { session_id: String, run_id: String },

    #[error("OpenCode request failed: {detail}")]
    RequestFailed { detail: String },

    #[error("OpenCode returned HTTP {status} for {operation}: {detail}")]
    Http {
        status: u16,
        operation: &'static str,
        detail: String,
    },

    #[error("OpenCode protocol violation: {detail}")]
    Protocol { detail: String },

    #[error("OpenCode stream disconnected before a terminal outcome: {detail}")]
    Disconnected { detail: String },

    #[error("OpenCode run was cancelled")]
    RunCancelled,

    #[error("OpenCode engine process is gone (run cannot continue)")]
    EngineUnavailable,

    #[error("OpenCode model '{model_id}' is not available: {detail}")]
    ModelUnavailable { model_id: String, detail: String },

    #[error("OpenCode prompt is too large ({bytes} bytes; limit {limit})")]
    PromptTooLarge { bytes: usize, limit: usize },

    /// A metadata/session response arrived after its runtime generation was
    /// replaced (engine restart mid-request). The response is discarded so a
    /// stale runtime can never become current authority (§32).
    #[error("OpenCode runtime changed while the request was in flight; stale response discarded")]
    StaleRuntime,
}

/// `?`-friendly conversion at the adapter boundary.
impl From<OpenCodeError> for EngineError {
    fn from(e: OpenCodeError) -> Self {
        e.into_engine()
    }
}

impl OpenCodeError {
    /// Map onto the canonical engine error surface (ENGINE_CONTRACT.md).
    pub fn into_engine(self) -> EngineError {
        match self {
            OpenCodeError::ExecutableNotFound { .. }
            | OpenCodeError::ExplicitExecutableInvalid { .. }
            | OpenCodeError::UnsupportedLauncher { .. }
            | OpenCodeError::ProbeFailed { .. }
            | OpenCodeError::InvalidWorkspace { .. }
            | OpenCodeError::PortUnavailable { .. }
            | OpenCodeError::SpawnFailed { .. }
            | OpenCodeError::ReadinessTimeout { .. }
            | OpenCodeError::ExitedDuringStartup { .. }
            | OpenCodeError::ProtocolUnexpected { .. }
            | OpenCodeError::AuthConfigurationFailed { .. }
            | OpenCodeError::StartupCleanupFailed { .. }
            | OpenCodeError::PreviousRuntimeTerminationUnproven { .. }
            | OpenCodeError::RequestFailed { .. }
            | OpenCodeError::Http { .. }
            | OpenCodeError::Protocol { .. }
            | OpenCodeError::Disconnected { .. }
            | OpenCodeError::ModelUnavailable { .. }
            | OpenCodeError::PromptTooLarge { .. }
            | OpenCodeError::StaleRuntime => EngineError::engine("opencode", self.to_string()),
            OpenCodeError::Cancelled | OpenCodeError::RunCancelled => EngineError::Canceled,
            OpenCodeError::NotReady { .. } => EngineError::NotReady {
                engine_id: "opencode".into(),
            },
            OpenCodeError::SessionNotFound { session_id } => {
                EngineError::SessionNotFound { session_id }
            }
            OpenCodeError::SessionBusy { session_id } => EngineError::SessionBusy { session_id },
            OpenCodeError::DuplicateRun { .. } => EngineError::engine("opencode", self.to_string()),
            OpenCodeError::EngineUnavailable => EngineError::Crashed {
                engine_id: "opencode".into(),
                message: "engine process is gone".into(),
            },
        }
    }

    /// True when this failure is safe (and sensible) to retry with a fresh
    /// port/process: only an actual port collision (§17, §90). A process
    /// that exited during startup with an EADDRINUSE-style message is
    /// classified as a collision; everything else (missing/wrong executable,
    /// invalid workspace, auth config) is a configuration problem that
    /// retrying cannot fix.
    pub fn is_port_retryable(&self) -> bool {
        match self {
            OpenCodeError::PortUnavailable { .. } => true,
            OpenCodeError::ExitedDuringStartup { tail, .. } => {
                let lower = tail.to_lowercase();
                lower.contains("address in use")
                    || lower.contains("eaddrinuse")
                    || lower.contains("port already in use")
            }
            _ => false,
        }
    }
}
