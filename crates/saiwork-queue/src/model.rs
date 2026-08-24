//! Durable queue domain model (KNOWLEDGE/QUEUE.md, TASK 13).
//!
//! States are exact facts, not progress labels:
//!
//! ```text
//! QUEUED      durable work exists and is eligible for future claim
//! LEASED      one dispatcher exclusively owns this item temporarily; the
//!             engine handoff has not yet been authoritatively accepted
//! DISPATCHED  the engine accepted the send and a RunId is associated
//! DONE        authoritative run completed successfully
//! FAILED      terminal queue outcome under the retry policy
//! CANCELLED   user/system intentionally removed the item from execution
//! UNKNOWN     execution outcome cannot be proven (crash during handoff, or
//!             a dispatched run whose authority was lost at restart). Never
//!             auto-dispatched; blocks further mutating dispatch in its
//!             workspace; resolved only by explicit user action (retry as a
//!             new attempt, cancel, or an authoritative terminal found
//!             externally). TASK 23.
//! ```
//!
//! LEASED carries a `dispatch_phase` that discriminates the crash window:
//! `prepare` means no external side effect exists yet (recoverable to QUEUED);
//! `sending` means the engine may have accepted the send (ambiguous — never
//! blindly redispatch).

use serde::Serialize;

/// Maximum prompt payload, byte-bounded (law 13, QUEUE.md).
pub const PAYLOAD_MAX_BYTES: usize = 64 * 1024;
/// Bounded safe failure detail stored on the item (never a raw HTTP body).
pub const LAST_ERROR_MAX_CHARS: usize = 500;
/// UI snapshot payload preview cap (TASK 24 perf): a queue of thousands of
/// up-to-64 KiB prompts must not serialize/mount tens of MiB through IPC —
/// the snapshot carries at most this many payload bytes per item; the full
/// payload is fetched on demand only when editing/inspecting the item.
pub const PAYLOAD_PREVIEW_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueState {
    Queued,
    Leased,
    Dispatched,
    Done,
    Failed,
    Cancelled,
    /// Execution outcome cannot be proven. Distinct from `Failed` (which
    /// asserts the attempt failed): this is a blocked, user-resolvable state
    /// that never auto-dispatches (TASK 23 §17–§21, §50).
    Unknown,
}

impl QueueState {
    pub fn as_str(self) -> &'static str {
        match self {
            QueueState::Queued => "queued",
            QueueState::Leased => "leased",
            QueueState::Dispatched => "dispatched",
            QueueState::Done => "done",
            QueueState::Failed => "failed",
            QueueState::Cancelled => "cancelled",
            QueueState::Unknown => "unknown",
        }
    }

    /// Parse a stored state string. Domain parser (not `std::str::FromStr`):
    /// returns `Option` because an unknown stored value is recoverable data,
    /// not a caller error.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "queued" => QueueState::Queued,
            "leased" => QueueState::Leased,
            "dispatched" => QueueState::Dispatched,
            "done" => QueueState::Done,
            "failed" => QueueState::Failed,
            "cancelled" => QueueState::Cancelled,
            "unknown" => QueueState::Unknown,
            _ => return None,
        })
    }

    /// Terminal outcomes are authoritative facts (`Done`/`Failed`/`Cancelled`).
    /// `Unknown` is a stable blocked state, not a terminal fact: it is
    /// resolved by explicit user action, never by automatic dispatch.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            QueueState::Done | QueueState::Failed | QueueState::Cancelled
        )
    }
}

/// Explicit session targeting (§58): never overload an empty string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMode {
    /// Create a fresh engine session at dispatch time.
    New,
    /// Dispatch into an existing SAIWORK2 session (stored `session_id`).
    Existing,
}

impl SessionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionMode::New => "new",
            SessionMode::Existing => "existing",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "new" => Some(SessionMode::New),
            "existing" => Some(SessionMode::Existing),
            _ => None,
        }
    }
}

/// Crash-window discriminator inside the LEASED state (§84–§86).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchPhase {
    /// Claimed but no external side effect exists yet: a crash here recovers
    /// the item to QUEUED without loss.
    Prepare,
    /// The engine may have accepted the send (session created / send in
    /// flight): a crash here is ambiguous — never blindly redispatch.
    Sending,
}

impl DispatchPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            DispatchPhase::Prepare => "prepare",
            DispatchPhase::Sending => "sending",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "prepare" => Some(DispatchPhase::Prepare),
            "sending" => Some(DispatchPhase::Sending),
            _ => None,
        }
    }
}

/// Maximum candidate rows materialized by one dispatcher page. A fixed
/// product bound keeps repeated queue drains O(N) instead of rebuilding the
/// entire remaining queue for every claimed item.
pub const DISPATCH_CANDIDATE_PAGE_SIZE: usize = 128;

/// Lightweight eligibility candidate (PERFORMANCE.md): the subset of a
/// queue row the dispatcher needs to evaluate eligibility — never the
/// payload/error strings. Loaded in bounded keyset pages; the full
/// `QueueItem` is materialized only for the single selected+claimed candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchCandidate {
    pub id: String,
    pub revision: i64,
    pub engine_id: String,
    pub workspace_id: String,
    pub session_mode: SessionMode,
    pub session_id: Option<String>,
    pub model: Option<String>,
    /// Durable ordering tuple retained only as the next-page cursor.
    pub order_key: i64,
    pub created_at: i64,
}

/// One durable queue row. Every field has a consumer (schema §7).
#[derive(Debug, Clone, Serialize)]
pub struct QueueItem {
    pub id: String,
    pub workspace_id: String,
    pub engine_id: String,
    /// Target SAIWORK2 session (set when `session_mode` is `existing`, or
    /// when a `new`-mode item has created its session).
    pub session_id: Option<String>,
    pub session_mode: SessionMode,
    /// Canonical model id, or `None` = explicit `UseEngineDefault`
    /// (resolved at dispatch, §184–§185).
    pub model: Option<String>,
    pub payload: String,
    /// `true` when this payload is a bounded SNAPSHOT preview (the full
    /// durable body is fetched via `get` before editing). Always `false` on
    /// rows read through `get`/full decode (§13: the UI renders "…" on this
    /// flag — the durable row itself is never mutated).
    #[serde(default)]
    pub payload_truncated: bool,
    pub state: QueueState,
    /// Deterministic ordering key (insertion order; renumbered on reorder).
    pub order_key: i64,
    /// Monotonic CAS counter for edit/reorder/delete/retry.
    pub revision: i64,
    pub lease_id: Option<String>,
    pub leased_at: Option<i64>,
    pub attempt_count: i64,
    /// Engine-accepted run identity (dispatch correlation, §24).
    pub run_id: Option<String>,
    pub last_error: Option<String>,
    pub last_error_code: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Manager health (derived, minimal — §97).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    #[default]
    Ready,
    Paused,
    /// Fail-closed: dispatch disabled until restart (durability failure).
    Failed,
    ShuttingDown,
    Stopped,
}

/// Authoritative snapshot for the UI (one truth, §69–§74). `payload_preview`
/// is `true` when `items[].payload` is a bounded preview (TASK 24 perf): the
/// full payload is fetched via the dedicated get-item operation before
/// editing/inspecting. Durable queue contents are never changed by this.
#[derive(Debug, Clone, Serialize)]
pub struct QueueSnapshot {
    pub status: QueueStatus,
    pub paused: bool,
    pub items: Vec<QueueItem>,
    #[serde(default)]
    pub payload_preview: bool,
}

/// Bounded diagnostics (no prompt bodies, §154).
#[derive(Debug, Clone, Serialize, Default)]
pub struct QueueDiagnostics {
    pub status: QueueStatus,
    pub paused: bool,
    pub queued: usize,
    pub leased: usize,
    pub dispatched: usize,
    pub done: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub unknown: usize,
    pub current_item: Option<String>,
    pub worker_alive: bool,
    pub last_dispatch_error_code: Option<String>,
}

/// Enqueue request.
#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    pub workspace_id: String,
    pub engine_id: String,
    pub session_id: Option<String>,
    pub session_mode: SessionMode,
    pub model: Option<String>,
    pub payload: String,
}

/// Typed queue error taxonomy (§115).
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue is not ready: {0}")]
    NotReady(String),

    #[error("queue is paused")]
    Paused,

    #[error("queue item not found: {0}")]
    NotFound(String),

    #[error("revision conflict: item {item_id} is at revision {current}, expected {expected}")]
    Conflict {
        item_id: String,
        current: i64,
        expected: i64,
    },

    #[error("invalid state for item {item_id}: {detail}")]
    InvalidState { item_id: String, detail: String },

    #[error("storage unavailable: {0}")]
    StorageUnavailable(String),

    #[error("lease lost for item {0}")]
    LeaseLost(String),

    #[error("session busy: {0}")]
    SessionBusy(String),

    #[error("engine unavailable: {0}")]
    EngineUnavailable(String),

    #[error("dispatch rejected: {0}")]
    DispatchRejected(String),

    #[error("dispatch outcome is ambiguous (external acceptance cannot be proven): {0}")]
    DispatchAmbiguous(String),

    #[error("retry policy exhausted for item {0}")]
    RetryExhausted(String),

    #[error("payload too large: {bytes} bytes (max {max})")]
    PayloadTooLarge { bytes: usize, max: usize },

    #[error("queue payload must not be empty")]
    EmptyPayload,

    /// Strict enum decode of a persisted row failed (corrupted / partially
    /// migrated / future-schema row). The queue fails closed and dispatch is
    /// disabled — never a silent business-state substitution (TASK 24 §9).
    #[error("invalid persisted queue row {row_id}: unknown {field} value {value:?}")]
    InvalidPersistedRow {
        row_id: String,
        field: &'static str,
        value: String,
    },

    #[error("operation cancelled")]
    Cancelled,

    #[error("port error: {0}")]
    Port(#[from] PortError),

    #[error("application is shutting down")]
    ShuttingDown,

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<saiwork_storage::StorageError> for QueueError {
    fn from(e: saiwork_storage::StorageError) -> Self {
        QueueError::StorageUnavailable(e.to_string())
    }
}

/// Engine-facing failure categories (mapped by the `EnginePort` bridge from
/// the engine's typed errors — never raw DTOs through the queue boundary,
/// §116).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortError {
    EngineUnavailable(String),
    SessionNotFound(String),
    SessionBusy(String),
    ModelUnavailable(String),
    RateLimited(String),
    Auth(String),
    Provider(String),
    Protocol(String),
    Network(String),
    Invalid(String),
    Internal(String),
}

impl PortError {
    /// Stable bounded category stored on the queue row (`last_error_code`).
    pub fn code(&self) -> &'static str {
        match self {
            PortError::EngineUnavailable(_) => "engine_unavailable",
            PortError::SessionNotFound(_) => "session_not_found",
            PortError::SessionBusy(_) => "session_busy",
            PortError::ModelUnavailable(_) => "model_unavailable",
            PortError::RateLimited(_) => "rate_limited",
            PortError::Auth(_) => "auth",
            PortError::Provider(_) => "provider",
            PortError::Protocol(_) => "protocol",
            PortError::Network(_) => "network",
            PortError::Invalid(_) => "invalid",
            PortError::Internal(_) => "internal",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            PortError::EngineUnavailable(m)
            | PortError::SessionNotFound(m)
            | PortError::SessionBusy(m)
            | PortError::ModelUnavailable(m)
            | PortError::RateLimited(m)
            | PortError::Auth(m)
            | PortError::Provider(m)
            | PortError::Protocol(m)
            | PortError::Network(m)
            | PortError::Invalid(m)
            | PortError::Internal(m) => m,
        }
    }

    /// Whether this error is item-specific (mark FAILED) vs environmental
    /// (release the lease and wait — the queue does not burn attempts on
    /// infrastructure).
    pub fn is_environmental(&self) -> bool {
        matches!(
            self,
            PortError::EngineUnavailable(_) | PortError::Network(_) | PortError::SessionBusy(_)
        )
    }
}

impl std::fmt::Display for PortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for PortError {}
