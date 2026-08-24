//! OpenCode event normalization (TASK 11 §28–§41, §172).
//!
//! The pipeline is: HTTP/SSE → `SseParser` → raw `data:` string → JSON
//! `ServerEvent` → protocol validation → adapter state + canonical
//! `EventBus` events. The generic EventBus never sees raw OpenCode JSON.
//!
//! Policy:
//! - `message.part.delta` (field `text`) → `message.delta` (canonical).
//! - `message.part.updated` with a `tool` part → `tool.*` lifecycle.
//! - `session.status busy` / `message.updated` / any part event → the
//!   acceptance evidence that gates `message.started` (§22).
//! - `session.error` → recorded on the run; the POST task turns it into a
//!   FAILED terminal (provider error ≠ engine failure, §57–§59).
//! - `permission.request` → `permission.requested` (defensive shape; real
//!   smoke was not reproducible with auto-allow config, §99).
//! - unknown event types → debug-level ignore (§29). Unknown-but-valid JSON
//!   never crashes; malformed JSON in the stream is a diagnostic, because
//!   the POST response — not the stream — is the run's terminal authority
//!   (§30): a broken stream can never fabricate completion.
//! - duplicate event ids are skipped (bounded dedup window, §33).

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saiwork_core::engine::{PendingPermissionInfo, PendingQuestionInfo};
use saiwork_events::{Event, EventBus};
use tracing::debug;

use crate::models::{ServerEvent, ToolState};
use crate::runs::{RunRecord, RunRegistry};

/// Bounded tool output forwarded to `tool.output` (§39).
const MAX_TOOL_OUTPUT: usize = 32 * 1024;
/// Dedup window: recent event ids per stream (ring).
const DEDUP_WINDOW: usize = 256;

/// Bounded pending-permission authority (W2-002): the exact session/run/request
/// ownership a missed `permission.requested` event can be reconstructed from
/// after a bounded-bus lag. Inserted BEFORE the `PermissionRequested` event is
/// published; removed on resolution, run terminal, or runtime teardown. Keyed
/// by `request_id`; bounded by `MAX_PENDING_PERMISSIONS` (FIFO eviction) so it
/// can never grow unbounded (no unbounded anything, §ARCHITECTURE).
pub(crate) struct PendingPermissions {
    inner: Mutex<VecDeque<PendingPermissionInfo>>,
}

impl PendingPermissions {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(VecDeque::with_capacity(MAX_PENDING_PERMISSIONS)),
        })
    }

    /// Insert/update a pending permission. Update-in-place preserves ordering
    /// when the same request_id is re-observed (idempotent reprocessing).
    pub(crate) fn insert(&self, info: PendingPermissionInfo) {
        let mut q = self.inner.lock().expect("pending permissions mutex poisoned");
        if let Some(slot) = q.iter_mut().find(|p| p.request_id == info.request_id) {
            *slot = info;
            return;
        }
        q.push_back(info);
        while q.len() > MAX_PENDING_PERMISSIONS {
            q.pop_front();
        }
    }

    /// Remove a single resolved permission by request id.
    pub(crate) fn remove(&self, request_id: &str) {
        let mut q = self.inner.lock().expect("pending permissions mutex poisoned");
        q.retain(|p| p.request_id != request_id);
    }

    /// Remove every pending permission for a run (run terminal / teardown).
    pub(crate) fn remove_for_run(&self, run_id: &str) {
        let mut q = self.inner.lock().expect("pending permissions mutex poisoned");
        q.retain(|p| p.run_id != run_id);
    }

    /// Drop all pending permissions (runtime teardown).
    pub(crate) fn clear(&self) {
        self.inner.lock().expect("pending permissions mutex poisoned").clear();
    }

    /// Authoritative snapshot for `EngineAdapter::pending_permissions()`.
    pub(crate) fn snapshot(&self) -> Vec<PendingPermissionInfo> {
        self.inner
            .lock()
            .expect("pending permissions mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }
}

const MAX_PENDING_PERMISSIONS: usize = 256;
/// AUDIT-CORE-002: bounded pending-question authority (same contract and
/// bound as the permission store).
const MAX_PENDING_QUESTIONS: usize = 256;

/// AUDIT-CORE-002: bounded pending-question authority — the exact
/// session/run/request ownership a missed `question.asked` state event can
/// be reconstructed from after a bounded-bus lag. Inserted BEFORE the
/// `QuestionAsked` event is published; removed on authoritative reply/
/// reject, run terminal, or runtime teardown.
pub(crate) struct PendingQuestions {
    inner: Mutex<VecDeque<PendingQuestionInfo>>,
}

impl PendingQuestions {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(VecDeque::with_capacity(MAX_PENDING_QUESTIONS)),
        })
    }

    /// Insert/update a pending question (update-in-place on the same id).
    pub(crate) fn insert(&self, info: PendingQuestionInfo) {
        let mut q = self.inner.lock().expect("pending questions mutex poisoned");
        if let Some(slot) = q.iter_mut().find(|p| p.request_id == info.request_id) {
            *slot = info;
            return;
        }
        q.push_back(info);
        while q.len() > MAX_PENDING_QUESTIONS {
            q.pop_front();
        }
    }

    pub(crate) fn remove(&self, request_id: &str) {
        let mut q = self.inner.lock().expect("pending questions mutex poisoned");
        q.retain(|p| p.request_id != request_id);
    }

    pub(crate) fn remove_for_run(&self, run_id: &str) {
        let mut q = self.inner.lock().expect("pending questions mutex poisoned");
        q.retain(|p| p.run_id != run_id);
    }

    pub(crate) fn clear(&self) {
        self.inner.lock().expect("pending questions mutex poisoned").clear();
    }

    pub(crate) fn snapshot(&self) -> Vec<PendingQuestionInfo> {
        self.inner
            .lock()
            .expect("pending questions mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }
}

pub(crate) struct EventRouter {
    bus: EventBus,
    registry: Arc<RunRegistry>,
    /// Bounded dedup window (PERF-003): the `VecDeque` preserves eviction
    /// order (oldest first) and the `HashSet` gives O(1) membership, kept in
    /// lockstep so eviction stays correct and lookups never scan the window.
    seen_ids: Mutex<(VecDeque<String>, HashSet<String>)>,
    /// Bounded pending-permission authority (W2-002): shared with the adapter
    /// so `pending_permissions()` can rebuild the UI permission cards after a
    /// bounded-bus lag.
    pending: Arc<PendingPermissions>,
    /// AUDIT-CORE-002: bounded pending-question authority, same ownership as
    /// the permission store above.
    pending_questions: Arc<PendingQuestions>,
    /// Runtime secret, used to redact echoed credentials from surfaced
    /// errors (§84).
    secret: crate::secret::Secret,
}

impl EventRouter {
    pub(crate) fn new(
        bus: EventBus,
        registry: Arc<RunRegistry>,
        pending: Arc<PendingPermissions>,
        pending_questions: Arc<PendingQuestions>,
        secret: crate::secret::Secret,
    ) -> Self {
        Self {
            bus,
            registry,
            seen_ids: Mutex::new((
                VecDeque::with_capacity(DEDUP_WINDOW),
                HashSet::with_capacity(DEDUP_WINDOW),
            )),
            pending,
            pending_questions,
            secret,
        }
    }

    /// Process one raw SSE `data:` payload. Never panics.
    pub(crate) fn on_data(&self, data: &str) {
        let Ok(event) = serde_json::from_str::<ServerEvent>(data) else {
            // §30/§82: malformed stream data is a diagnostic, never a crash;
            // the raw payload is NOT logged (it may contain user content or
            // secrets echoed by the upstream). The POST response remains the
            // terminal authority.
            debug!(
                "opencode stream: malformed event JSON (ignored; POST is the terminal authority)"
            );
            return;
        };
        let id = event.id.as_str();
        if !id.is_empty() && self.seen_or_insert(id) {
            debug!(id, "opencode stream: duplicate event id skipped");
            return;
        }
        // AUDIT-CORE-002: resolution events are handled BEFORE session
        // routing. The wire may deliver them without a sessionID (legacy
        // reply payloads carry only the request id), and a reply landing
        // after its run went terminal must STILL clear the pending card —
        // neither gate may swallow an authoritative resolution.
        match event.r#type.as_str() {
            "permission.replied" | "permission.v2.replied" | "permission.resolved" => {
                if let Some(request_id) = Self::interaction_request_id(&event.properties) {
                    self.pending.remove(request_id);
                }
                return;
            }
            "question.replied"
            | "question.v2.replied"
            | "question.rejected"
            | "question.v2.rejected" => {
                if let Some(request_id) = Self::interaction_request_id(&event.properties) {
                    self.pending_questions.remove(request_id);
                }
                return;
            }
            _ => {}
        }
        let Some(engine_session_id) = event.session_id().map(str::to_string) else {
            // Events without a session (server.connected etc.) are liveness.
            return;
        };
        let Some(run) = self.registry.active_for_session(&engine_session_id) else {
            // Event for a session this adapter is not running — another
            // session/workspace; never route across (§116–§117).
            return;
        };
        // Canonical events always carry the GENERIC session id (TASK 24 §9);
        // the wire id above is only used for routing.
        let session_id = run.session_id.clone();
        // §21/§166: an event that arrives after the run reached its terminal
        // state (but before the registry removal lands) must never reopen or
        // mutate the terminal projection. Drop it with a diagnostic; the
        // terminal stays terminal.
        if run.is_terminal() {
            debug!(
                kind = event.r#type,
                run = %run.run_id,
                "opencode stream: event after terminal discarded (terminal stays terminal)"
            );
            return;
        }
        // TASK 24 perf: every matched event is liveness evidence — the POST
        // task's idle-grace wait starts from HERE, so a settled stream pays
        // no unconditional 250 ms terminal tax.
        run.note_stream_activity();
        match event.r#type.as_str() {
            "session.status" => {
                // busy/idle is acceptance + liveness evidence.
                run.mark_started(&self.bus);
            }
            "message.updated" => {
                run.mark_started(&self.bus);
                if let Some(id) = event
                    .properties
                    .get("info")
                    .and_then(|i| i.get("id"))
                    .and_then(|v| v.as_str())
                {
                    run.note_message_id(id, &self.bus);
                }
            }
            "message.part.delta" => {
                run.mark_started(&self.bus);
                let props = &event.properties;
                let field = props.get("field").and_then(|v| v.as_str()).unwrap_or("");
                if field == "text" {
                    let delta = props.get("delta").and_then(|v| v.as_str()).unwrap_or("");
                    if !delta.is_empty() {
                        if let Some(id) = props.get("messageID").and_then(|v| v.as_str()) {
                            run.note_message_id(id, &self.bus);
                        }
                        self.bus.publish(Event::MessageDelta {
                            session_id: session_id.clone().into(),
                            run_id: run.run_id.clone(),
                            delta: delta.to_string(),
                        });
                    }
                }
            }
            "message.part.updated" => {
                run.mark_started(&self.bus);
                self.on_part_updated(&session_id, &run, &event);
            }
            "session.error" => {
                let message = event
                    .properties
                    .get("error")
                    .and_then(|e| e.get("data"))
                    .and_then(|d| d.get("message"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| "opencode reported a session error".into());
                // §84: a provider error body may echo credentials; redact the
                // runtime secret before the message can reach a log or the UI.
                run.record_session_error(self.secret.redact(&message));
            }
            // AUDIT-CORE-002: the canonical OpenCode interaction families.
            // `permission.request` stays only as a defensive legacy alias;
            // the real stream emits asked/updated/replied (+ v2 twins).
            "permission.asked" | "permission.v2.asked" | "permission.request" => {
                run.mark_started(&self.bus);
                self.on_permission_request(&run, &session_id, &event);
            }
            "permission.updated" => {
                // Update-in-place: same request id re-observed with fresh
                // detail (e.g. permission kind/metadata changed upstream).
                run.mark_started(&self.bus);
                self.on_permission_request(&run, &session_id, &event);
            }
            "permission.replied" | "permission.v2.replied" | "permission.resolved" => {
                // Handled before session routing (see on_data head).
            }
            "question.asked" | "question.v2.asked" => {
                run.mark_started(&self.bus);
                self.on_question_asked(&run, &session_id, &event);
            }
            "question.replied"
            | "question.v2.replied"
            | "question.rejected"
            | "question.v2.rejected" => {
                // Handled before session routing (see on_data head).
            }
            other => {
                // §29: unknown-but-valid event → diagnostic, ignore. Protocol
                // state never depends on event types we do not recognize.
                debug!(
                    kind = other,
                    "opencode stream: unknown event type (ignored)"
                );
            }
        }
    }

    fn on_part_updated(&self, session_id: &str, run: &Arc<RunRecord>, event: &ServerEvent) {
        let Some(part) = event.properties.get("part") else {
            return;
        };
        let Some(part_type) = part.get("type").and_then(|v| v.as_str()) else {
            return;
        };
        if part_type != "tool" {
            return;
        }
        let tool = part
            .get("tool")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        // Stable identity of ONE tool invocation (TASK 24 §9): the upstream
        // `callID` when present, else the part id; never the bare tool name,
        // so two same-named tools in one run stay independent. Last resort:
        // a run-scoped synthetic id.
        let tool_call_id = part
            .get("callID")
            .and_then(|v| v.as_str())
            .or_else(|| part.get("id").and_then(|v| v.as_str()))
            .map(str::to_string)
            .unwrap_or_else(|| format!("tool-{}", run.run_id));
        let state: Option<ToolState> = part
            .get("state")
            .and_then(|s| serde_json::from_value(s.clone()).ok());
        let Some(state) = state else {
            debug!(
                tool,
                "opencode stream: tool part without parseable state (ignored)"
            );
            return;
        };
        let run_id = run.run_id.clone();
        match state.status.as_str() {
            "running" => {
                self.bus.publish(Event::ToolStarted {
                    session_id: session_id.into(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool: tool.into(),
                });
            }
            "completed" => {
                if let Some(output) = state.output.as_deref() {
                    if !output.is_empty() {
                        self.bus.publish(Event::ToolOutput {
                            session_id: session_id.into(),
                            run_id: run_id.clone(),
                            tool_call_id: tool_call_id.clone(),
                            tool: tool.into(),
                            output: truncate(output, MAX_TOOL_OUTPUT),
                        });
                    }
                } else if let Some(meta) = state.metadata.as_ref() {
                    if let Some(output) = meta.get("output").and_then(|v| v.as_str()) {
                        self.bus.publish(Event::ToolOutput {
                            session_id: session_id.into(),
                            run_id: run_id.clone(),
                            tool_call_id: tool_call_id.clone(),
                            tool: tool.into(),
                            output: truncate(output, MAX_TOOL_OUTPUT),
                        });
                    }
                }
                self.bus.publish(Event::ToolCompleted {
                    session_id: session_id.into(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool: tool.into(),
                });
            }
            "error" => {
                let message = state
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("error"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| "tool failed".into());
                self.bus.publish(Event::ToolFailed {
                    session_id: session_id.into(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool: tool.into(),
                    error: truncate(&message, 2000),
                });
            }
            "cancelled" => {
                // §49: a tool aborted by cancellation maps to tool.failed.
                self.bus.publish(Event::ToolFailed {
                    session_id: session_id.into(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool: tool.into(),
                    error: "cancelled".into(),
                });
            }
            _ => {
                debug!(
                    status = state.status,
                    tool, "opencode stream: unknown tool state (ignored)"
                );
            }
        }
    }

    fn on_permission_request(&self, run: &Arc<RunRecord>, session_id: &str, event: &ServerEvent) {
        let permission = event.properties.get("permission");
        // AUDIT-CORE-002: id aliases across the protocol families — nested
        // `permission.id` plus the legacy/v2 `requestID/requestId/
        // permissionID/permissionId` spellings, then a bare top-level id.
        let request_id = permission
            .and_then(|p| p.get("id"))
            .or_else(|| permission.and_then(|p| p.get("requestID")))
            .or_else(|| permission.and_then(|p| p.get("requestId")))
            .or_else(|| permission.and_then(|p| p.get("permissionID")))
            .or_else(|| permission.and_then(|p| p.get("permissionId")))
            .or_else(|| event.properties.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("perm-{}", run.run_id));
        let detail = permission
            .and_then(|p| p.get("request"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| {
                // Prefer the legacy `permission` kind field, then render the
                // whole request object so v2 shapes (action/resources/metadata)
                // stay visible on the card.
                permission
                    .and_then(|p| p.get("permission"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .or_else(|| {
                        permission
                            .and_then(|p| serde_json::to_string(p).ok())
                            .filter(|s| s != "null")
                    })
                    .unwrap_or_else(|| "permission requested".into())
            });
        let detail = self.secret.redact(&detail);
        // W2-002: register the pending permission BEFORE publishing the
        // canonical event, so a lagged-bus reconciliation that reads
        // `pending_permissions()` after a missed `permission.requested` can
        // reconstruct the exact session/run/request ownership. The entry is
        // removed on resolution, run terminal, or runtime teardown.
        self.pending.insert(PendingPermissionInfo {
            session_id: session_id.to_string(),
            run_id: run.run_id.to_string(),
            request_id: request_id.clone(),
            detail: truncate(&detail, 2000),
        });
        self.bus.publish(Event::PermissionRequested {
            session_id: session_id.into(),
            run_id: run.run_id.clone(),
            request_id: request_id.into(),
            detail: truncate(&detail, 2000),
        });
    }

    /// AUDIT-CORE-002: normalize a user-question request. The wire shape is
    /// `properties` = the question request itself (`{id, sessionID,
    /// questions:[...]}`; a nested `question` object is tolerated). The
    /// bounded redacted JSON rendering becomes the card detail; structured
    /// answering goes through `resolve_question`, never boolean permission
    /// semantics.
    fn on_question_asked(&self, run: &Arc<RunRecord>, session_id: &str, event: &ServerEvent) {
        let question = event.properties.get("question").unwrap_or(&event.properties);
        let request_id = question
            .get("id")
            .or_else(|| question.get("requestID"))
            .or_else(|| question.get("requestId"))
            .or_else(|| event.properties.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("q-{}", run.run_id));
        // Preserve valid structured JSON while bounding every collection and
        // user-controlled string. Truncating the serialized blob produced
        // invalid JSON and made the UI hide the actual prompt/options.
        let detail = Self::bounded_question_detail(question, &self.secret);
        self.pending_questions.insert(PendingQuestionInfo {
            session_id: session_id.to_string(),
            run_id: run.run_id.to_string(),
            request_id: request_id.clone(),
            detail: detail.clone(),
        });
        self.bus.publish(Event::QuestionAsked {
            session_id: session_id.into(),
            run_id: run.run_id.clone(),
            request_id: request_id.into(),
            detail,
        });
    }

    /// AUDIT-CORE-002: pull the request id out of a permission/question
/// resolution event across every legacy/v2 alias the protocol has used.
fn interaction_request_id(properties: &serde_json::Value) -> Option<&str> {
    for key in ["requestID", "requestId", "permissionID", "permissionId", "id"] {
        if let Some(v) = properties.get(key).and_then(|v| v.as_str()) {
            return Some(v);
        }
    }
    // Nested `permission.id` (defensive legacy shape).
    properties
        .get("permission")
        .and_then(|p| p.get("id"))
        .and_then(|v| v.as_str())
}

fn bounded_question_detail(question: &serde_json::Value, secret: &crate::secret::Secret) -> String {
    const MAX_QUESTIONS: usize = 8;
    const MAX_OPTIONS: usize = 16;
    const MAX_TEXT: usize = 512;
    let Some(questions) = question.get("questions").and_then(|value| value.as_array()) else {
        return "question requested".into();
    };
    let normalized: Vec<serde_json::Value> = questions
        .iter()
        .take(MAX_QUESTIONS)
        .map(|raw| {
            let bounded = |key: &str| {
                raw.get(key)
                    .and_then(|value| value.as_str())
                    .map(|value| truncate(&secret.redact(value), MAX_TEXT))
                    .unwrap_or_default()
            };
            let options: Vec<serde_json::Value> = raw
                .get("options")
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
                .take(MAX_OPTIONS)
                .map(|option| serde_json::json!({
                    "label": option.get("label").and_then(|value| value.as_str())
                        .map(|value| truncate(&secret.redact(value), MAX_TEXT)).unwrap_or_default(),
                    "description": option.get("description").and_then(|value| value.as_str())
                        .map(|value| truncate(&secret.redact(value), MAX_TEXT)).unwrap_or_default(),
                }))
                .collect();
            serde_json::json!({
                "header": bounded("header"),
                "question": bounded("question"),
                "options": options,
                "multiple": raw.get("multiple").and_then(|value| value.as_bool()).unwrap_or(false),
                "custom": raw.get("custom").and_then(|value| value.as_bool()).unwrap_or(true),
            })
        })
        .collect();
    serde_json::json!({ "questions": normalized }).to_string()
}

/// O(1) dedup check + insert (PERF-003). Returns `true` if `id` was ALREADY
    /// present (caller treats the event as a duplicate and drops it). Inserts
    /// new ids and, beyond `DEDUP_WINDOW`, evicts the oldest — the `VecDeque`
    /// (eviction order) and `HashSet` (membership) are mutated together so they
    /// never diverge.
    fn seen_or_insert(&self, id: &str) -> bool {
        let mut seen = self.seen_ids.lock().expect("dedup mutex poisoned");
        if seen.1.contains(id) {
            return true;
        }
        if seen.0.len() >= DEDUP_WINDOW {
            if let Some(old) = seen.0.pop_front() {
                seen.1.remove(&old);
            }
        }
        seen.0.push_back(id.to_string());
        seen.1.insert(id.to_string());
        false
    }
}

/// The single terminal outcome of a run (§24): completed | failed | cancelled
/// | outcome_unknown (the engine accepted the run but the terminal cannot be
/// proven — distinct from a definite failure, TASK 24 §9).
#[derive(Debug, Clone)]
pub(crate) enum TerminalOutcome {
    Completed,
    Failed(String),
    Cancelled,
    Unknown(String),
}

/// Emit the terminal event for a run. The `terminal_emitted` CAS is the one
/// gate that guarantees exactly one terminal per run even when the POST task
/// and the engine-crash watcher race (§24, §48).
pub(crate) fn emit_terminal(bus: &EventBus, record: &Arc<RunRecord>, outcome: TerminalOutcome) {
    if record
        .terminal_emitted
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    *record.state.lock().expect("run state mutex poisoned") = match &outcome {
        TerminalOutcome::Completed => crate::runs::RunState::Completed,
        TerminalOutcome::Failed(_) | TerminalOutcome::Unknown(_) => crate::runs::RunState::Failed,
        TerminalOutcome::Cancelled => crate::runs::RunState::Cancelled,
    };
    let session_id = record.session_id.clone().into();
    let run_id = record.run_id.clone();
    let _ = match outcome {
        TerminalOutcome::Completed => bus.publish(Event::MessageCompleted { session_id, run_id }),
        TerminalOutcome::Failed(message) => bus.publish(Event::MessageFailed {
            session_id,
            run_id,
            error: message,
        }),
        TerminalOutcome::Cancelled => bus.publish(Event::MessageCancelled { session_id, run_id }),
        TerminalOutcome::Unknown(error) => bus.publish(Event::MessageOutcomeUnknown {
            session_id,
            run_id,
            error,
        }),
    };
}

impl RunRecord {
    /// Publish `message.started` exactly once, on the first authoritative
    /// evidence that OpenCode accepted the request (§22).
    pub(crate) fn mark_started(&self, bus: &EventBus) {
        if self
            .started_emitted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            bus.publish(Event::MessageStarted {
                session_id: self.session_id.clone().into(),
                run_id: self.run_id.clone(),
            });
        }
    }

    pub(crate) fn note_message_id(&self, id: &str, _bus: &EventBus) {
        let mut slot = self.message_id.lock().expect("message id mutex poisoned");
        if slot.is_none() {
            *slot = Some(id.to_string());
        }
    }

    pub(crate) fn record_session_error(&self, message: String) {
        let mut slot = self.session_error.lock().expect("run error mutex poisoned");
        if slot.is_none() {
            *slot = Some(message);
        }
        // PERF-008: wake the POST task's idle-grace settle so it need not poll
        // `session_error` every 10 ms.
        self.session_notify.notify_one();
    }
}

pub(crate) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…(truncated)", &s[..end])
    }
}

/// Idle timeout used when closing the global event stream after the last
/// run ends — a short grace avoids reconnect churn on back-to-back sends
/// while still closing the connection when nothing is running (§171).
pub(crate) const STREAM_IDLE_GRACE: Duration = Duration::from_millis(250);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runs::{RunRecord, RunRegistry, RunState};
    use saiwork_events::RunId;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    fn sample_record(session_id: &str) -> Arc<RunRecord> {
        Arc::new(RunRecord {
            run_id: RunId::new(String::from("run-test")),
            session_id: session_id.into(),
            engine_session_id: session_id.into(),
            generation: 0,
            cancel_requested: AtomicBool::new(false),
            abort_delivered: AtomicBool::new(false),
            engine_lost: AtomicBool::new(false),
            started_emitted: AtomicBool::new(false),
            message_id: Mutex::new(None),
            session_error: Mutex::new(None),
            last_stream_activity: Mutex::new(None),
            state: Mutex::new(RunState::Running),
            terminal_emitted: AtomicBool::new(false),
            post_task: Mutex::new(None),
            engine_lost_notify: Notify::new(),
            session_notify: Notify::new(),
        })
    }

    fn delta_payload(session_id: &str) -> String {
        format!(
            r#"{{"id":"evt_late","type":"message.part.delta","properties":{{"sessionID":"{session_id}","messageID":"m","partID":"p","field":"text","delta":"late"}}}}"#
        )
    }

    /// §21: a semantic event arriving after the run reached its terminal
    /// state must be discarded — never published after the terminal, never
    /// reopening the run.
    #[tokio::test]
    async fn event_after_terminal_is_discarded() {
        let bus = EventBus::new();
        let mut observer = bus.subscribe();
        let registry = Arc::new(RunRegistry::new());
        let record = sample_record("ses_1");
        registry.insert(record.clone()).unwrap();
        let router = EventRouter::new(
            bus.clone(),
            registry.clone(),
            PendingPermissions::new(),
            PendingQuestions::new(),
            crate::secret::Secret::generate(),
        );

        // Terminal first.
        emit_terminal(&bus, &record, TerminalOutcome::Completed);
        let terminal = observer.try_recv().unwrap().expect("terminal event");
        assert!(matches!(terminal.event, Event::MessageCompleted { .. }));

        // A late delta arrives before the registry removal (terminal runs are
        // removed right after the terminal is emitted).
        router.on_data(&delta_payload("ses_1"));

        // No further events may appear.
        assert!(
            observer.try_recv().unwrap().is_none(),
            "late delta must not be published after the terminal"
        );
        // The run state stays terminal.
        assert!(record.is_terminal());
    }

    /// §21: a late delta for an unknown (already-removed) run is dropped too.
    #[tokio::test]
    async fn event_for_removed_run_is_dropped() {
        let bus = EventBus::new();
        let mut observer = bus.subscribe();
        let registry = Arc::new(RunRegistry::new());
        let record = sample_record("ses_2");
        registry.insert(record.clone()).unwrap();
        let router = EventRouter::new(
            bus.clone(),
            registry.clone(),
            PendingPermissions::new(),
            PendingQuestions::new(),
            crate::secret::Secret::generate(),
        );

        registry.remove("run-test");
        router.on_data(&delta_payload("ses_2"));
        assert!(observer.try_recv().unwrap().is_none());
    }

    /// W2-002: a `permission.request` is registered in the bounded pending
    /// authority BEFORE the canonical `PermissionRequested` event is published,
    /// so a lagged-bus reconciliation can rebuild the UI card from
    /// `pending_permissions()`. A later `permission.resolved` clears it.
    #[tokio::test]
    async fn permission_request_is_registered_before_publish_and_cleared_on_resolve() {
        let bus = EventBus::new();
        let mut observer = bus.subscribe();
        let registry = Arc::new(RunRegistry::new());
        let record = sample_record("ses_perm");
        registry.insert(record).unwrap();
        let pending = PendingPermissions::new();
        let router = EventRouter::new(
            bus.clone(),
            registry.clone(),
            pending.clone(),
            PendingQuestions::new(),
            crate::secret::Secret::generate(),
        );

        let req = r#"{"id":"evt_perm","type":"permission.request","properties":{"sessionID":"ses_perm","permission":{"id":"req-1","request":"may I sudo?"}}}"#;
        router.on_data(req);

        // The pending authority MUST contain the entry now (registered before
        // the event is published).
        let snap = pending.snapshot();
        assert_eq!(snap.len(), 1, "permission must be registered before publish");
        assert_eq!(snap[0].request_id, "req-1");
        assert_eq!(snap[0].session_id, "ses_perm");
        assert_eq!(snap[0].run_id, "run-test");
        assert!(snap[0].detail.contains("sudo"));

        // The canonical event was published too. `mark_started` (acceptance +
        // liveness evidence) may emit its own event BEFORE the permission
        // event, so drain the bus until we see `PermissionRequested`.
        let published = loop {
            let e = observer.try_recv().unwrap().expect("event");
            if matches!(e.event, Event::PermissionRequested { .. }) {
                break e;
            }
        };
        assert!(matches!(published.event, Event::PermissionRequested { .. }));

        // Resolution over the wire clears the pending entry.
        let res = r#"{"id":"evt_perm_res","type":"permission.resolved","properties":{"sessionID":"ses_perm","permission":{"id":"req-1"}}}"#;
        router.on_data(res);
        assert!(
            pending.snapshot().is_empty(),
            "resolution must clear the pending permission"
        );
    }

    /// AUDIT-CORE-002: the canonical asked/replied families (legacy + v2)
    /// register/clear pending permissions, including every id alias
    /// (`requestID`/`requestId`/`permissionID`/`permissionId`) and the
    /// update-in-place `permission.updated`.
    #[tokio::test]
    async fn permission_asked_updated_replied_families_are_normalized() {
        let bus = EventBus::new();
        let mut observer = bus.subscribe();
        let registry = Arc::new(RunRegistry::new());
        let record = sample_record("ses_perm2");
        registry.insert(record).unwrap();
        let pending = PendingPermissions::new();
        let router = EventRouter::new(
            bus.clone(),
            registry.clone(),
            pending.clone(),
            PendingQuestions::new(),
            crate::secret::Secret::generate(),
        );

        // Legacy asked with a bare requestID alias (no nested permission.id).
        router.on_data(
            r#"{"id":"e1","type":"permission.asked","properties":{"sessionID":"ses_perm2","permission":{"requestID":"perm-legacy-1","permission":"bash","request":"run install?"}}}"#,
        );
        let snap = pending.snapshot();
        assert_eq!(snap.len(), 1, "legacy asked must register");
        assert_eq!(snap[0].request_id, "perm-legacy-1");
        loop {
            let e = observer.try_recv().unwrap().expect("event");
            if matches!(e.event, Event::PermissionRequested { .. }) {
                break;
            }
        }

        // v2 updated re-observed: update-in-place, still exactly one entry.
        router.on_data(
            r#"{"id":"e2","type":"permission.updated","properties":{"sessionID":"ses_perm2","permission":{"permissionId":"perm-legacy-1","action":"edit","resources":["src/main.rs"]}}}"#,
        );
        let snap = pending.snapshot();
        assert_eq!(snap.len(), 1, "updated must not duplicate the entry");
        assert_eq!(snap[0].request_id, "perm-legacy-1");
        assert!(snap[0].detail.contains("main.rs"), "detail refreshes in place");

        // v2 replied clears by the permissionId alias.
        router.on_data(
            r#"{"id":"e3","type":"permission.v2.replied","properties":{"sessionID":"ses_perm2","permissionId":"perm-legacy-1"}}"#,
        );
        assert!(
            pending.snapshot().is_empty(),
            "v2 replied (permissionId alias) must clear the pending permission"
        );

        // v2 asked + legacy requestId resolution.
        router.on_data(
            r#"{"id":"e4","type":"permission.v2.asked","properties":{"sessionID":"ses_perm2","permission":{"id":"perm-v2-1","action":"write","resources":["a.txt"]}}}"#,
        );
        assert_eq!(pending.snapshot()[0].request_id, "perm-v2-1");
        loop {
            let e = observer.try_recv().unwrap().expect("event");
            if matches!(e.event, Event::PermissionRequested { .. }) {
                break;
            }
        }
        router.on_data(
            r#"{"id":"e5","type":"permission.replied","properties":{"sessionID":"ses_perm2","requestId":"perm-v2-1"}}"#,
        );
        assert!(pending.snapshot().is_empty());
    }

    fn drain_until<E>(
        observer: &mut tokio::sync::mpsc::UnboundedReceiver<saiwork_events::Envelope>,
        pred: impl Fn(&E) -> bool,
    ) {
    }

    /// AUDIT-CORE-002: a question.asked registers the bounded pending-question
    /// authority BEFORE the canonical `QuestionAsked` event is published; the
    /// replied/rejected families (legacy + v2) clear it by `requestID`.
    #[tokio::test]
    async fn question_asked_registers_and_reply_reject_clears() {
        let bus = EventBus::new();
        let mut observer = bus.subscribe();
        let registry = Arc::new(RunRegistry::new());
        let record = sample_record("ses_q");
        registry.insert(record).unwrap();
        let questions = PendingQuestions::new();
        let router = EventRouter::new(
            bus.clone(),
            registry.clone(),
            PendingPermissions::new(),
            questions.clone(),
            crate::secret::Secret::generate(),
        );

        // Legacy asked: properties IS the question request.
        router.on_data(
            r#"{"id":"q1","type":"question.asked","properties":{"sessionID":"ses_q","id":"q-req-1","questions":[{"question":"Proceed with plan?","options":[{"label":"Yes"},{"label":"No"}]}]}}"#,
        );
        let snap = questions.snapshot();
        assert_eq!(snap.len(), 1, "question must be registered before publish");
        assert_eq!(snap[0].request_id, "q-req-1");
        assert_eq!(snap[0].session_id, "ses_q");
        assert_eq!(snap[0].run_id, "run-test");
        assert!(snap[0].detail.contains("Proceed with plan?"));
        let structured: serde_json::Value = serde_json::from_str(&snap[0].detail)
            .expect("bounded question detail must remain valid JSON");
        assert_eq!(structured["questions"][0]["options"][0]["label"], "Yes");
        let published = loop {
            let e = observer.try_recv().unwrap().expect("event");
            if matches!(e.event, Event::QuestionAsked { .. }) {
                break e;
            }
        };
        match published.event {
            Event::QuestionAsked { session_id, run_id, request_id, detail } => {
                assert_eq!(session_id.as_str(), "ses_q");
                assert_eq!(run_id.as_str(), "run-test");
                assert_eq!(request_id.as_str(), "q-req-1");
                assert!(detail.contains("options"));
            }
            _ => unreachable!(),
        }

        // v2 rejected clears by requestID.
        router.on_data(
            r#"{"id":"q2","type":"question.v2.rejected","properties":{"requestID":"q-req-1"}}"#,
        );
        assert!(questions.snapshot().is_empty());

        // v2 asked then legacy replied clears too.
        router.on_data(
            r#"{"id":"q3","type":"question.v2.asked","properties":{"sessionID":"ses_q","id":"q-req-2","questions":[{"question":"Pick one","options":[{"label":"A"}]}]}}"#,
        );
        assert_eq!(questions.snapshot()[0].request_id, "q-req-2");
        loop {
            let e = observer.try_recv().unwrap().expect("event");
            if matches!(e.event, Event::QuestionAsked { .. }) {
                break;
            }
        }
        router.on_data(
            r#"{"id":"q4","type":"question.replied","properties":{"requestID":"q-req-2"}}"#,
        );
        assert!(
            questions.snapshot().is_empty(),
            "legacy question.replied must clear the pending question"
        );
    }

    /// W2-002: the authority is bounded — exceeding the cap evicts the oldest
    /// entry and never grows unbounded (no unbounded anything, §ARCHITECTURE).
    #[test]
    fn pending_authority_is_bounded() {
        let pending = PendingPermissions::new();
        for i in 0..(super::MAX_PENDING_PERMISSIONS + 1) {
            pending.insert(PendingPermissionInfo {
                session_id: "s".into(),
                run_id: format!("r{i}"),
                request_id: format!("req-{i}"),
                detail: format!("d{i}"),
            });
        }
        let snap = pending.snapshot();
        assert_eq!(
            snap.len(),
            super::MAX_PENDING_PERMISSIONS,
            "must stay bounded at the cap"
        );
        assert!(
            !snap.iter().any(|p| p.request_id == "req-0"),
            "oldest entry must be evicted"
        );
    }

    #[test]
    fn truncate_multibyte_unicode_does_not_panic() {
        let kanji = "日本語".repeat(10); // 3 bytes per char
        let res = truncate(&kanji, 4); // splits second 3-byte char
        assert_eq!(res, "日…(truncated)");
    }
}
