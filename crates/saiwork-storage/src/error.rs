//! Typed storage errors (ENGINE_CONTRACT.md error model, domain STORAGE).

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("cannot open database at {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },

    #[error("cannot prepare database location at {path}: {source}")]
    PrepareLocation {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("database integrity check failed at {path}: {detail}")]
    Integrity { path: PathBuf, detail: String },

    #[error("database at {path} is not a valid SQLite database: {detail}")]
    Corrupt { path: PathBuf, detail: String },

    #[error("migration {version} failed: {source}")]
    Migration {
        version: i64,
        #[source]
        source: rusqlite::Error,
    },

    #[error("unsupported database version {found} (app supports up to {supported})")]
    UnsupportedVersion { found: i64, supported: i64 },

    #[error("database is busy: another writer holds the lock (bounded wait exhausted): {0}")]
    Busy(String),

    /// AUDIT-W2-003: a workspace deletion was rejected because nonterminal
    /// durable work (queue rows) still references it — checked INSIDE the
    /// deletion transaction so an enqueue can never slip between the
    /// preflight and the delete.
    #[error("workspace '{workspace_id}' still has nonterminal durable references")]
    WorkspaceReferenced { workspace_id: String },

    #[error("database operation failed: {0}")]
    Query(rusqlite::Error),

    #[error("data in database is invalid: {0}")]
    InvalidData(String),
}

/// Map rusqlite failures onto the typed model. `SQLITE_BUSY`/`SQLITE_LOCKED`
/// surface as `Busy` so callers can distinguish an exhausted bounded wait from
/// a plain query error (busy policy: 5s `busy_timeout`, then a typed error).
impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        match &e {
            rusqlite::Error::SqliteFailure(err, msg) => {
                if err.code == rusqlite::ErrorCode::DatabaseBusy
                    || err.code == rusqlite::ErrorCode::DatabaseLocked
                {
                    StorageError::Busy(msg.clone().unwrap_or_default())
                } else {
                    StorageError::Query(e)
                }
            }
            _ => StorageError::Query(e),
        }
    }
}
