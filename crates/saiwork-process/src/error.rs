//! Typed process errors (domain PROCESS, ENGINE_CONTRACT.md error model).

use std::path::PathBuf;

use saiwork_events::ProcessId;

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("invalid process spec: {0}")]
    InvalidSpec(String),

    #[error("a process with id {id} is already registered")]
    DuplicateId { id: ProcessId },

    #[error("command '{command}' not found: {source}")]
    CommandNotFound {
        command: String,
        #[source]
        source: std::io::Error,
    },

    #[error("working directory {path} does not exist or is not a directory")]
    BadCwd { path: PathBuf },

    #[error("process {id} spawn failed: {source}")]
    Spawn {
        id: ProcessId,
        #[source]
        source: std::io::Error,
    },

    #[error("process {id} platform operation '{op}' failed: {source}")]
    Platform {
        id: ProcessId,
        op: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("process {id} is not running")]
    NotRunning { id: ProcessId },

    #[error("process {id} did not terminate within the bounded wait")]
    TerminationTimeout { id: ProcessId },

    #[error("cannot spawn: supervisor is shutting down")]
    ShuttingDown,
}
