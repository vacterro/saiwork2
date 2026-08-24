//! Typed read model for the canonical SAIPEN integration (TASK 14).
//!
//! SAIPEN is the authority; SAIWORK2 is a reader/projection. Unknown values
//! stay `None` (UI renders UNKNOWN) — nothing is ever fabricated from
//! optimism (law 59, TASK 14 §18).

use std::path::PathBuf;

use serde::Serialize;

/// Canonical SAIPEN layout facts (verified against `donors/saipen` baseline
/// v7.224.3, schema_version 3 — TASK 14 §4).
pub const SAIPEN_DIR: &str = ".saipen";
pub const STATE_FILE: &str = "STATE.md";
pub const BOARD_FILE: &str = "BOARD.md";
pub const LOG_FILE: &str = "LOG.md";
/// STATE.md frontmatter delimiter (canonical format).
pub const FRONTMATTER_DELIM: &str = "---";

/// Discovery result — typed, never `Option<PathBuf>` (§14). Absence of SAIPEN
/// is a normal state, not an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Discovery {
    /// No canonical SAIPEN in this workspace (normal state).
    NotPresent,
    /// Canonical SAIPEN detected and schema understood.
    Present(SaipenDescriptor),
    /// `.saipen` exists but is structurally broken (e.g. STATE.md missing).
    Invalid { reason: String },
    /// Present but schema/protocol version newer than this reader supports.
    Unsupported {
        schema_version: Option<String>,
        protocol_version: Option<String>,
    },
    /// Present but unreadable (permission). Never reported as NotPresent.
    PermissionDenied { path: PathBuf },
}

/// Validated canonical SAIPEN root (typed, workspace-bound — §10–§11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaipenRoot {
    /// Canonicalized `.saipen` directory (boundary-validated).
    pub dir: PathBuf,
    /// Canonicalized workspace root this SAIPEN belongs to.
    pub workspace_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaipenDescriptor {
    pub root: SaipenRoot,
    pub schema_version: Option<String>,
    pub protocol_version: Option<String>,
    /// Project identity from canonical source when available (STATE `project`
    /// field) — never the folder basename masquerading as canonical (§80).
    pub project_name: Option<String>,
}

/// Cheap SAIPEN presence/version summary for sidebar/list rows (TASK 24
/// perf): produced from STATE discovery ONLY — no BOARD read, no
/// consistency pipeline, no full snapshot. The full authoritative projection
/// belongs to `SaipenService::attach` / `get_saipen`; this is a badge, never
/// a replacement for the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SaipenSummary {
    pub schema_version: Option<String>,
    pub saipen_version: Option<String>,
    pub project: Option<String>,
}

/// Live watch status — surfaced, never silently frozen (§37, §61).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WatchStatus {
    #[default]
    NotWatching,
    Live,
    Failed(String),
}

/// Normalized board summary (§23). Ticket status comes from the section
/// (DOING/TODO/DONE/BLOCKED), never the checkbox alone — canonical rule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct BoardSummary {
    /// section → ticket ids (in board order, first N per section).
    pub sections: std::collections::BTreeMap<String, Vec<String>>,
    pub counts: std::collections::BTreeMap<String, usize>,
}

/// Read-only normalized projection of canonical SAIPEN state (§17). Every
/// canonical field is `Option` — `None` renders as UNKNOWN. This is a
/// projection cache, never an authority (§166).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SaipenSnapshot {
    /// SAIWORK2 semantic snapshot revision — monotonic per service instance
    /// (§50). Advances ONLY on meaningful change (content or stale-marking),
    /// never on no-op rereads; validation staleness compares against this
    /// (§87–§88). Not a SAIPEN protocol version and not the watch epoch.
    pub generation: u64,
    pub read_at_ms: i64,
    pub root: Option<PathBuf>,
    pub schema_version: Option<String>,
    pub saipen_version: Option<String>,
    pub project: Option<String>,
    pub phase: Option<String>,
    pub task: Option<String>,
    pub next_action: Option<String>,
    pub blocker: Option<String>,
    pub mode: Option<String>,
    pub execution_intent: Option<String>,
    pub agent: Option<String>,
    pub updated: Option<String>,
    pub last_event: Option<String>,
    pub board: BoardSummary,
    pub watch_status: WatchStatus,
    pub last_error: Option<String>,
    /// True when this snapshot came from a read that failed and we kept the
    /// previous good one (marked STALE — §49). Never shown as current truth.
    pub stale: bool,
}

impl SaipenSnapshot {
    /// Semantic equality for change suppression (§54, §167): ignores
    /// read timing and generation, compares only canonical content.
    pub fn semantically_eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.saipen_version == other.saipen_version
            && self.project == other.project
            && self.phase == other.phase
            && self.task == other.task
            && self.next_action == other.next_action
            && self.blocker == other.blocker
            && self.mode == other.mode
            && self.execution_intent == other.execution_intent
            && self.agent == other.agent
            && self.updated == other.updated
            && self.last_event == other.last_event
            && self.board == other.board
            && self.stale == other.stale
    }
}

/// Typed read errors (§97–§99). `PermissionDenied` and `NotPresent` are
/// distinct — the UI must never collapse them.
#[derive(Debug, thiserror::Error)]
pub enum SaipenError {
    #[error("no canonical SAIPEN in workspace")]
    NotPresent,
    #[error("SAIPEN schema version {0} is not supported by this reader")]
    UnsupportedVersion(String),
    #[error("permission denied reading {path}")]
    PermissionDenied { path: PathBuf },
    #[error("path escape rejected: {0}")]
    PathEscape(String),
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("canonical file too large: {0}")]
    TooLarge(String),
    #[error("canonical file is not valid UTF-8: {0}")]
    Encoding(String),
    #[error("cannot parse {file}: {detail}")]
    Parse { file: String, detail: String },
    #[error("inconsistent snapshot across canonical files: {0}")]
    InconsistentSnapshot(String),
    #[error("SAIPEN watcher failed: {0}")]
    WatchFailed(String),
    #[error("canonical validation is not run in read integration: {0}")]
    CanonicalValidationUnavailable(String),
    #[error("internal: {0}")]
    Internal(String),
}
