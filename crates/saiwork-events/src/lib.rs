//! Canonical application events (EVENTS.md).
//!
//! Every event crossing any boundary uses `Envelope { seq, ts, type, payload }`.
//! `type` is the canonical dot-name from the taxonomy; payload fields are the
//! normalized event payload. Raw provider payloads never ride here except as
//! `engine.raw_event` (debug-only, bounded).

pub mod bus;
pub mod coalescing;
pub mod id;

pub use bus::{EventBus, SubscribeError, Subscription};
pub use id::{
    EngineId, MessageId, ProcessId, QueueItemId, RequestId, RunId, SessionId, WorkspaceId,
};

use serde::Serialize;

/// Monotonic sequence number for ordering and gap detection. Reset per app run.
pub type Seq = u64;
/// Wall-clock milliseconds (display purposes).
pub type Timestamp = u64;

/// Semantic event class (EVENTS.md §18, TASK 22 §31). Used to differentiate
/// delivery and buffering policy where needed:
/// - `State` — a **live fact** about authoritative state (e.g. `engine.ready`,
///   `message.completed`, `queue.changed`); consumers reconcile from state
///   snapshots, never from replay. The fact itself is terminal/durable where
///   its domain authority restores it (EVENTS.md reconstruction table); the
///   EventBus never persists it (§30).
/// - `Stream` — **stream deltas** (e.g. `message.delta`, `tool.output`); the UI
///   bridge may batch/coalesce render updates without changing order, and a
///   lagging consumer may drop them (reconstructable from the engine session
///   where supported, never from EventBus replay).
/// - `Diagnostic` — warnings/errors; must never recurse (subscriber failure
///   is reported once, not re-published as another `runtime.error`).
///
/// The EventBus is runtime fact distribution, never a database: durable
/// authority stays with Storage/Queue/SAIPEN/engine sessions (EVENTS.md
/// "Semantic classification" table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventClass {
    State,
    Stream,
    Diagnostic,
}

/// Canonical event taxonomy. Variant names map to `app.started`, `message.delta`
/// etc. via explicit serde renames — do not rename without updating EVENTS.md.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    #[serde(rename = "app.started")]
    AppStarted { version: String },
    #[serde(rename = "app.stopping")]
    AppStopping { reason: String },

    #[serde(rename = "workspace.opened")]
    WorkspaceOpened {
        workspace_id: WorkspaceId,
        path: String,
    },
    #[serde(rename = "workspace.closed")]
    WorkspaceClosed { workspace_id: WorkspaceId },
    #[serde(rename = "workspace.changed")]
    WorkspaceChanged { workspace_id: WorkspaceId },

    #[serde(rename = "engine.starting")]
    EngineStarting { engine_id: EngineId },
    #[serde(rename = "engine.ready")]
    EngineReady { engine_id: EngineId },
    #[serde(rename = "engine.stopping")]
    EngineStopping { engine_id: EngineId },
    #[serde(rename = "engine.stopped")]
    EngineStopped { engine_id: EngineId },
    #[serde(rename = "engine.failed")]
    EngineFailed { engine_id: EngineId, error: String },
    #[serde(rename = "engine.health_changed")]
    EngineHealthChanged { engine_id: EngineId, healthy: bool },

    /// OS process lifecycle facts (ProcessSupervisor). `process alive` is NOT
    /// `engine ready` — readiness lives at the engine layer (PROCESS_LIFECYCLE).
    #[serde(rename = "process.started")]
    ProcessStarted { process_id: ProcessId, pid: u32 },
    #[serde(rename = "process.exited")]
    ProcessExited {
        process_id: ProcessId,
        pid: u32,
        code: Option<i32>,
        signaled: bool,
    },
    #[serde(rename = "process.failed")]
    ProcessFailed {
        process_id: ProcessId,
        error: String,
    },
    #[serde(rename = "session.created")]
    SessionCreated {
        session_id: SessionId,
        engine_id: EngineId,
        /// The full authoritative session DTO (TASK 24 §9): the frontend
        /// reducer builds its row from these fields — it never fabricates
        /// workspace/upstream-id/display-name from local UI state.
        workspace_id: Option<WorkspaceId>,
        engine_session_id: String,
        display_name: String,
        created_at: i64,
        /// Authoritative normalized state (TASK 24 §9) — the reducer must
        /// NEVER fabricate either from local UI state. `resumable` = survives
        /// runtime/app restart; `usable_now` = usable with the CURRENT engine
        /// runtime generation (a fresh connection-owned Harness/Generic
        /// session is usable now even though it is not restart-resumable).
        resumable: bool,
        usable_now: bool,
    },
    #[serde(rename = "session.loaded")]
    SessionLoaded { session_id: SessionId },
    #[serde(rename = "session.changed")]
    SessionChanged { session_id: SessionId },
    #[serde(rename = "session.closed")]
    SessionClosed { session_id: SessionId },

    #[serde(rename = "message.started")]
    MessageStarted {
        session_id: SessionId,
        run_id: RunId,
    },
    #[serde(rename = "message.delta")]
    MessageDelta {
        session_id: SessionId,
        run_id: RunId,
        delta: String,
    },
    #[serde(rename = "message.completed")]
    MessageCompleted {
        session_id: SessionId,
        run_id: RunId,
    },
    #[serde(rename = "message.failed")]
    MessageFailed {
        session_id: SessionId,
        run_id: RunId,
        error: String,
    },
    /// Distinct terminal fact: the run was canceled. Exactly one terminal
    /// outcome per run: completed | failed | cancelled.
    #[serde(rename = "message.cancelled")]
    MessageCancelled {
        session_id: SessionId,
        run_id: RunId,
    },
    /// The engine accepted the run but its terminal outcome cannot be proven
    /// (transport lost the response, runtime died mid-run). Distinct from
    /// `failed` (which asserts the attempt failed): the external side effect
    /// may still be live. Callers must never treat this as a plain failure
    /// (TASK 24 §9).
    #[serde(rename = "message.outcome_unknown")]
    MessageOutcomeUnknown {
        session_id: SessionId,
        run_id: RunId,
        error: String,
    },

    /// Tool lifecycle facts. `run_id` scopes the tool to the run that
    /// invoked it; `tool_call_id` is the stable upstream identity of ONE tool
    /// invocation (two same-named tools in one run never merge — TASK 24 §9).
    /// Adapters map upstream call ids (OpenCode `callID`, Harness
    /// `tool_call_id`); when an upstream has no call id, the adapter emits a
    /// stable run-scoped id instead of reusing the tool name.
    #[serde(rename = "tool.started")]
    ToolStarted {
        session_id: SessionId,
        run_id: RunId,
        tool_call_id: String,
        tool: String,
    },
    #[serde(rename = "tool.output")]
    ToolOutput {
        session_id: SessionId,
        run_id: RunId,
        tool_call_id: String,
        tool: String,
        output: String,
    },
    #[serde(rename = "tool.completed")]
    ToolCompleted {
        session_id: SessionId,
        run_id: RunId,
        tool_call_id: String,
        tool: String,
    },
    #[serde(rename = "tool.failed")]
    ToolFailed {
        session_id: SessionId,
        run_id: RunId,
        tool_call_id: String,
        tool: String,
        error: String,
    },

    #[serde(rename = "permission.requested")]
    PermissionRequested {
        session_id: SessionId,
        /// Owning run (mandatory routing identity, TASK 24 §9): a delayed
        /// permission event must be attached to the run that actually owns
        /// it — never to whatever run is active later. `request_id` is the
        /// permission identity; `run_id` supplies routing.
        run_id: RunId,
        request_id: RequestId,
        detail: String,
    },
    #[serde(rename = "permission.resolved")]
    PermissionResolved {
        session_id: SessionId,
        run_id: RunId,
        request_id: RequestId,
        allowed: bool,
    },

    /// AUDIT-CORE-002: the engine asked the user a structured question
    /// (OpenCode `question.asked` / `question.v2.asked`). `detail` carries a
    /// bounded, redacted JSON rendering of the questions; the typed answer
    /// path is `resolve_question`, never boolean permission semantics.
    #[serde(rename = "question.asked")]
    QuestionAsked {
        session_id: SessionId,
        run_id: RunId,
        request_id: RequestId,
        detail: String,
    },
    /// AUDIT-CORE-002: an authoritative question resolution landed
    /// (`question.replied` / `question.rejected` and v2 twins, or the local
    /// reply succeeded). Tears down the UI question card.
    #[serde(rename = "question.resolved")]
    QuestionResolved {
        session_id: SessionId,
        run_id: RunId,
        request_id: RequestId,
    },

    #[serde(rename = "queue.changed")]
    QueueChanged { item_id: QueueItemId, state: String },
    #[serde(rename = "queue.dispatch_started")]
    QueueDispatchStarted { item_id: QueueItemId },
    #[serde(rename = "queue.dispatch_completed")]
    QueueDispatchCompleted { item_id: QueueItemId },
    #[serde(rename = "queue.dispatch_failed")]
    QueueDispatchFailed { item_id: QueueItemId, error: String },

    #[serde(rename = "saipen.detected")]
    SaipenDetected { workspace_id: WorkspaceId },
    #[serde(rename = "saipen.changed")]
    SaipenChanged { workspace_id: WorkspaceId },
    #[serde(rename = "saipen.validation_changed")]
    SaipenValidationChanged {
        workspace_id: WorkspaceId,
        valid: bool,
    },
    #[serde(rename = "saipen.action_started")]
    SaipenActionStarted {
        workspace_id: WorkspaceId,
        action_id: String,
        kind: String,
    },
    #[serde(rename = "saipen.action_completed")]
    SaipenActionCompleted {
        workspace_id: WorkspaceId,
        action_id: String,
        kind: String,
        result: String,
    },
    #[serde(rename = "saipen.action_failed")]
    SaipenActionFailed {
        workspace_id: WorkspaceId,
        action_id: String,
        kind: String,
        error: String,
    },
    #[serde(rename = "saipen.action_cancelled")]
    SaipenActionCancelled {
        workspace_id: WorkspaceId,
        action_id: String,
        kind: String,
    },

    #[serde(rename = "git.changed")]
    GitChanged { workspace_id: WorkspaceId },

    #[serde(rename = "runtime.warning")]
    RuntimeWarning { code: String, message: String },
    #[serde(rename = "runtime.error")]
    RuntimeError { code: String, message: String },

    /// Debug-only, bounded, redacted. Never user-facing as primary content.
    #[serde(rename = "engine.raw_event")]
    EngineRawEvent {
        engine_id: EngineId,
        kind: String,
        payload: String,
    },
}

impl Event {
    /// Semantic class of this event (EVENTS.md §18).
    ///
    /// INVARIANT: this match is EXHAUSTIVE on purpose — no `_` arm. A new
    /// event variant must be classified explicitly at compile time; a
    /// catch-all would silently route future Stream/Diagnostic variants into
    /// the State-only channel and break its isolation guarantee
    /// (PERFORMANCE.md): high-rate content would wake and lag the
    /// correctness-critical state consumers.
    pub fn class(&self) -> EventClass {
        match self {
            // Stream: droppable high-rate content (EVENTS.md delivery table).
            Event::MessageDelta { .. }
            | Event::ToolOutput { .. }
            | Event::EngineRawEvent { .. } => EventClass::Stream,
            // Diagnostic: never recurses.
            Event::RuntimeWarning { .. } | Event::RuntimeError { .. } => EventClass::Diagnostic,
            // Everything else is an authoritative state fact.
            Event::AppStarted { .. }
            | Event::AppStopping { .. }
            | Event::WorkspaceOpened { .. }
            | Event::WorkspaceClosed { .. }
            | Event::WorkspaceChanged { .. }
            | Event::EngineStarting { .. }
            | Event::EngineReady { .. }
            | Event::EngineStopping { .. }
            | Event::EngineStopped { .. }
            | Event::EngineFailed { .. }
            | Event::EngineHealthChanged { .. }
            | Event::ProcessStarted { .. }
            | Event::ProcessExited { .. }
            | Event::ProcessFailed { .. }
            | Event::SessionCreated { .. }
            | Event::SessionLoaded { .. }
            | Event::SessionChanged { .. }
            | Event::SessionClosed { .. }
            | Event::MessageStarted { .. }
            | Event::MessageCompleted { .. }
            | Event::MessageFailed { .. }
            | Event::MessageCancelled { .. }
            | Event::MessageOutcomeUnknown { .. }
            | Event::ToolStarted { .. }
            | Event::ToolCompleted { .. }
            | Event::ToolFailed { .. }
            | Event::PermissionRequested { .. }
            | Event::PermissionResolved { .. }
            | Event::QuestionAsked { .. }
            | Event::QuestionResolved { .. }
            | Event::QueueChanged { .. }
            | Event::QueueDispatchStarted { .. }
            | Event::QueueDispatchCompleted { .. }
            | Event::QueueDispatchFailed { .. }
            | Event::SaipenDetected { .. }
            | Event::SaipenChanged { .. }
            | Event::SaipenValidationChanged { .. }
            | Event::SaipenActionStarted { .. }
            | Event::SaipenActionCompleted { .. }
            | Event::SaipenActionFailed { .. }
            | Event::SaipenActionCancelled { .. }
            | Event::GitChanged { .. } => EventClass::State,
        }
    }

    /// Canonical dot-name of the event (for routing and logging).
    pub fn name(&self) -> &'static str {
        match self {
            Event::AppStarted { .. } => "app.started",
            Event::AppStopping { .. } => "app.stopping",
            Event::WorkspaceOpened { .. } => "workspace.opened",
            Event::WorkspaceClosed { .. } => "workspace.closed",
            Event::WorkspaceChanged { .. } => "workspace.changed",
            Event::EngineStarting { .. } => "engine.starting",
            Event::EngineReady { .. } => "engine.ready",
            Event::EngineStopping { .. } => "engine.stopping",
            Event::EngineStopped { .. } => "engine.stopped",
            Event::EngineFailed { .. } => "engine.failed",
            Event::EngineHealthChanged { .. } => "engine.health_changed",
            Event::ProcessStarted { .. } => "process.started",
            Event::ProcessExited { .. } => "process.exited",
            Event::ProcessFailed { .. } => "process.failed",
            Event::SessionCreated { .. } => "session.created",
            Event::SessionLoaded { .. } => "session.loaded",
            Event::SessionChanged { .. } => "session.changed",
            Event::SessionClosed { .. } => "session.closed",
            Event::MessageStarted { .. } => "message.started",
            Event::MessageDelta { .. } => "message.delta",
            Event::MessageCompleted { .. } => "message.completed",
            Event::MessageFailed { .. } => "message.failed",
            Event::MessageCancelled { .. } => "message.cancelled",
            Event::MessageOutcomeUnknown { .. } => "message.outcome_unknown",
            Event::ToolStarted { .. } => "tool.started",
            Event::ToolOutput { .. } => "tool.output",
            Event::ToolCompleted { .. } => "tool.completed",
            Event::ToolFailed { .. } => "tool.failed",
            Event::PermissionRequested { .. } => "permission.requested",
            Event::PermissionResolved { .. } => "permission.resolved",
            Event::QuestionAsked { .. } => "question.asked",
            Event::QuestionResolved { .. } => "question.resolved",
            Event::QueueChanged { .. } => "queue.changed",
            Event::QueueDispatchStarted { .. } => "queue.dispatch_started",
            Event::QueueDispatchCompleted { .. } => "queue.dispatch_completed",
            Event::QueueDispatchFailed { .. } => "queue.dispatch_failed",
            Event::SaipenDetected { .. } => "saipen.detected",
            Event::SaipenChanged { .. } => "saipen.changed",
            Event::SaipenValidationChanged { .. } => "saipen.validation_changed",
            Event::SaipenActionStarted { .. } => "saipen.action_started",
            Event::SaipenActionCompleted { .. } => "saipen.action_completed",
            Event::SaipenActionFailed { .. } => "saipen.action_failed",
            Event::SaipenActionCancelled { .. } => "saipen.action_cancelled",
            Event::GitChanged { .. } => "git.changed",
            Event::RuntimeWarning { .. } => "runtime.warning",
            Event::RuntimeError { .. } => "runtime.error",
            Event::EngineRawEvent { .. } => "engine.raw_event",
        }
    }

    /// True for events that carry user content and therefore must never be
    /// logged verbatim by default.
    pub fn carries_user_content(&self) -> bool {
        matches!(
            self,
            Event::MessageDelta { .. }
                | Event::ToolOutput { .. }
                | Event::PermissionRequested { .. }
                | Event::QuestionAsked { .. }
                | Event::EngineRawEvent { .. }
        )
    }
}

/// One normalized event on the wire: `{ seq, ts, type, ...payload }`.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Envelope {
    pub seq: Seq,
    pub ts: Timestamp,
    #[serde(flatten)]
    pub event: Event,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_names_match_taxonomy() {
        assert_eq!(
            Event::AppStarted {
                version: "x".into()
            }
            .name(),
            "app.started"
        );
        assert_eq!(
            Event::MessageDelta {
                session_id: "s".into(),
                run_id: "r".into(),
                delta: "d".into()
            }
            .name(),
            "message.delta"
        );
        assert_eq!(
            Event::MessageCancelled {
                session_id: "s".into(),
                run_id: "r".into()
            }
            .name(),
            "message.cancelled"
        );
        assert_eq!(
            Event::EngineRawEvent {
                engine_id: "e".into(),
                kind: "k".into(),
                payload: "p".into()
            }
            .name(),
            "engine.raw_event"
        );
        assert_eq!(
            Event::ProcessExited {
                process_id: "p1".into(),
                pid: 123,
                code: Some(0),
                signaled: false
            }
            .name(),
            "process.exited"
        );
        assert_eq!(
            Event::ProcessFailed {
                process_id: "p1".into(),
                error: "boom".into()
            }
            .class(),
            EventClass::State
        );
    }

    #[test]
    fn envelope_serializes_with_dotted_type_tag() {
        let env = Envelope {
            seq: 1,
            ts: 2,
            event: Event::MessageDelta {
                session_id: "s".into(),
                run_id: "r".into(),
                delta: "hi".into(),
            },
        };
        let json = serde_json::to_value(&env).unwrap();
        assert_eq!(json["type"], "message.delta");
        assert_eq!(json["seq"], 1);
        assert_eq!(json["delta"], "hi");
    }
}
