//! Harness session-event normalization (TASK 21 §32–§41, §48–§62, §97).
//!
//! Pipeline: protocol frame → JSON-RPC envelope → typed `session/update`
//! notification (transport) → this router → run/session state → normalized
//! `EventBus` events. No raw JSON leaks to the generic bus (§32).
//!
//! Policy (DEEPSEEK_HARNESS.md §39–§40 classification):
//! - `agent_message_chunk` (text) is the **live** committed-chunk stream →
//!   `message.delta` (incremental, §35), one canonical MessageId per upstream
//!   message (§31), never one message per chunk.
//! - `tool_call` updates → `tool.*` lifecycle, keyed by the generic tool name
//!   (ToolCallId stays adapter-internal for the exactly-one-terminal rule, §52).
//! - `session/request_permission` is handled by `permission_handler` (§55–§62).
//! - The **durable** session-log facts (turn/step/session-log) are NOT on the
//!   ACP wire; the adapter never fabricates them and never mirrors them (§6).
//! - Unknown update kinds are ignored (debug) — not every Harness internal
//!   fact becomes a public event (§97).
//!
//! Routing is by the stable upstream session id (§33–§34), never by the
//! currently selected session/tab. Events for sessions without an active run
//! (external activity) are ignored (§122–§124).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use saiwork_events::{Event, EventBus};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use uuid::Uuid;

use crate::permissions::{PendingPermission, PermissionRegistry};
use crate::protocol::{
    ContentBlockIn, RequestPermissionParams, RequestPermissionResult, ServerRequest, SessionUpdate,
    SessionUpdateNotification, ToolCallUpdate, METHOD_SESSION_REQUEST_PERMISSION,
};
use crate::runs::{RunRecord, RunRegistry, RunState};
use crate::transport::Transport;

/// Bounded tool output forwarded to `tool.output` (§51).
const MAX_TOOL_OUTPUT: usize = 32 * 1024;
/// Bounded safe representation of a tool's raw input shown to the user (§50,
/// §62 — never a raw giant JSON, never a secret-bearing env dump).
const MAX_TOOL_INPUT_SUMMARY: usize = 500;

pub(crate) struct EventRouter {
    pub bus: EventBus,
    pub registry: Arc<RunRegistry>,
}

impl EventRouter {
    /// Process one routed `session/update` notification. Never panics.
    pub(crate) fn on_session_update(&self, notification: &SessionUpdateNotification) {
        let Some(run) = self.registry.active_for_harness(&notification.session_id) else {
            // Event for a session this adapter is not actively running — an
            // external session or an idle session; never route across (§122).
            debug!(
                session = %notification.session_id,
                "harness session/update for a session with no active run (ignored)"
            );
            return;
        };
        // §21/§121: an event after the run reached terminal must never reopen
        // or mutate the terminal projection.
        if run.is_terminal() {
            debug!(
                run = %run.run_id,
                "harness session/update after terminal discarded (terminal stays terminal)"
            );
            return;
        }
        // First routed event for this run = authoritative acceptance evidence
        // (§25, §30 — the run has started; the prompt task also marks start at
        // dispatch, so whichever comes first wins via the CAS).
        run.mark_started(&self.bus);
        match &notification.update {
            SessionUpdate::AgentMessageChunk {
                message_id,
                content,
            } => {
                if let Some(id) = message_id {
                    run.note_message_id(id);
                }
                if let ContentBlockIn::Text { text } = content {
                    if !text.is_empty() {
                        self.bus.publish(Event::MessageDelta {
                            session_id: run.session_id.clone().into(),
                            run_id: run.run_id.clone(),
                            delta: text.clone(),
                        });
                    }
                }
            }
            SessionUpdate::UserMessageChunk { .. } | SessionUpdate::AgentThoughtChunk { .. } => {
                // User-message echo and reasoning thoughts are not assistant
                // output; not normalized into message.delta (§97).
            }
            SessionUpdate::ToolCall { tool_call } => {
                self.on_tool_call(&run, tool_call);
            }
            SessionUpdate::Unknown => {
                debug!(
                    run = %run.run_id,
                    "harness session/update with an unknown kind (ignored)"
                );
            }
        }
    }

    fn on_tool_call(&self, run: &Arc<RunRecord>, tc: &ToolCallUpdate) {
        let tool = tc
            .name
            .clone()
            .or_else(|| tc.title.clone())
            .or_else(|| tc.kind.clone())
            .unwrap_or_else(|| "unknown".into());
        let status = tc.status.as_deref().unwrap_or("in_progress");
        // Stable identity of ONE tool invocation (TASK 24 §9): the upstream
        // `tool_call_id` is carried through every lifecycle event so two
        // same-named tools in one run stay independent cards in the UI.
        let tool_call_id = if tc.tool_call_id.is_empty() {
            format!("tool-{}", run.run_id)
        } else {
            tc.tool_call_id.clone()
        };
        let run_id = run.run_id.clone();
        match status {
            "pending" | "in_progress" | "running" => {
                self.bus.publish(Event::ToolStarted {
                    session_id: run.session_id.clone().into(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool: tool.clone(),
                });
            }
            "completed" | "failed" | "error" | "cancelled" => {
                // Exactly one terminal per ToolCallId (§52): late updates for
                // an already-terminal tool are ignored.
                let mut terminal_tools = run
                    .terminal_tools
                    .lock()
                    .expect("terminal tools mutex poisoned");
                if !terminal_tools.insert(tc.tool_call_id.clone()) {
                    return;
                }
                drop(terminal_tools);
                let terminal = match status {
                    "completed" => {
                        if let Some(output) = bounded_tool_output(tc) {
                            if !output.is_empty() {
                                self.bus.publish(Event::ToolOutput {
                                    session_id: run.session_id.clone().into(),
                                    run_id: run_id.clone(),
                                    tool_call_id: tool_call_id.clone(),
                                    tool: tool.clone(),
                                    output,
                                });
                            }
                        }
                        Event::ToolCompleted {
                            session_id: run.session_id.clone().into(),
                            run_id: run_id.clone(),
                            tool_call_id: tool_call_id.clone(),
                            tool: tool.clone(),
                        }
                    }
                    "cancelled" => Event::ToolFailed {
                        session_id: run.session_id.clone().into(),
                        run_id: run_id.clone(),
                        tool_call_id: tool_call_id.clone(),
                        tool,
                        error: "cancelled".into(),
                    },
                    _ => Event::ToolFailed {
                        session_id: run.session_id.clone().into(),
                        run_id: run_id.clone(),
                        tool_call_id: tool_call_id.clone(),
                        tool,
                        error: "tool failed".into(),
                    },
                };
                self.bus.publish(terminal);
            }
            other => {
                debug!(
                    run = %run.run_id,
                    status = other, "harness tool_call with an unknown status (ignored)"
                );
            }
        }
    }
}

/// Bounded output for a completed tool call: prefer text content blocks, else
/// A char-boundary-safe prefix of at most `max` bytes (never panics on a
/// hostile multi-byte boundary).
fn safe_prefix(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Append up to `cap` bytes of `text`, stopping as soon as the cap is
/// filled — a multi-megabyte payload is never materialized just to be
/// truncated (TASK 24 perf). Returns `true` when the cap was hit.
fn append_capped(out: &mut String, text: &str, cap: usize) -> bool {
    let remaining = cap.saturating_sub(out.len());
    if text.len() <= remaining {
        out.push_str(text);
        false
    } else {
        out.push_str(safe_prefix(text, remaining));
        true
    }
}

/// A capped `io::Write` that ABORTS serialization as soon as the cap is
/// reached: `serde_json::to_writer` propagates the writer error and stops
/// traversing the Value, so a giant raw JSON never pays full serialization
/// (TASK 24 perf). Small payloads are byte-identical to `to_string`.
struct CappedWriter {
    buf: String,
    cap: usize,
    over: bool,
}

impl std::io::Write for CappedWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.over {
            return Err(std::io::Error::other("cap reached"));
        }
        let text = std::str::from_utf8(b).map_err(std::io::Error::other)?;
        let remaining = self.cap.saturating_sub(self.buf.len());
        if text.len() <= remaining {
            self.buf.push_str(text);
            Ok(b.len())
        } else {
            self.buf.push_str(safe_prefix(text, remaining));
            self.over = true;
            Err(std::io::Error::other("cap reached"))
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serialize `value` to JSON retaining at most `cap` bytes (aborts early on
/// overflow; appends the existing truncation marker).
fn json_capped(value: &serde_json::Value, cap: usize) -> String {
    let mut w = CappedWriter {
        buf: String::new(),
        cap,
        over: false,
    };
    let result = serde_json::to_writer(&mut w, value);
    if w.over || result.is_err() {
        w.buf.push_str("…(truncated)");
    }
    w.buf
}

/// a bounded `rawOutput` JSON (§51 — high-volume tool output must not flood
/// the bus or the UI).
fn bounded_tool_output(tc: &ToolCallUpdate) -> Option<String> {
    if let Some(content) = &tc.content {
        let mut out = String::new();
        let mut truncated = false;
        for block in content {
            if let Some(text) = block
                .get("content")
                .and_then(|c| c.get("text"))
                .and_then(|v| v.as_str())
            {
                truncated |= append_capped(&mut out, text, MAX_TOOL_OUTPUT);
                if truncated {
                    break;
                }
            }
        }
        if !out.is_empty() {
            if truncated {
                out.push_str("…(truncated)");
            }
            return Some(out);
        }
    }
    tc.raw_output
        .as_ref()
        .map(|v| json_capped(v, MAX_TOOL_OUTPUT))
}

/// Bounded, safe permission detail (§62): tool name + a short raw-input
/// summary. Never a raw giant JSON payload.
fn permission_detail(params: &RequestPermissionParams) -> String {
    let tool = params
        .tool_call
        .name
        .clone()
        .or_else(|| params.tool_call.title.clone())
        .or_else(|| params.tool_call.kind.clone())
        .unwrap_or_else(|| "unknown tool".into());
    let input = params
        .tool_call
        .raw_input
        .as_ref()
        .map(|v| json_capped(v, MAX_TOOL_INPUT_SUMMARY))
        .unwrap_or_default();
    format!("{tool}: {}", input)
}

/// Pick the permission option that matches the decision (allow→allow_once/
/// allow_always; reject→reject_once/reject_always). Falls back to any option
/// of the same decision family; `None` when the agent offered no matching
/// option (§56 — only actual supported decisions are used, never invented).
fn pick_option(params: &RequestPermissionParams, allowed: bool) -> Option<String> {
    let preferred: &[&str] = if allowed {
        &["allow_once", "allow_always"]
    } else {
        &["reject_once", "reject_always"]
    };
    for kind in preferred {
        if let Some(opt) = params.options.iter().find(|o| o.kind == *kind) {
            return Some(opt.option_id.clone());
        }
    }
    params
        .options
        .iter()
        .find(|o| {
            if allowed {
                o.kind.starts_with("allow")
            } else {
                o.kind.starts_with("reject")
            }
        })
        .map(|o| o.option_id.clone())
}

/// The permission handler task (one per runtime): consumes routed
/// `session/request_permission` server requests, publishes the generic
/// `permission.requested`, waits for the user decision (or the run's terminal
/// — fail-closed), answers the upstream request, and publishes the
/// authoritative `permission.resolved`. Never blocks the transport reader
/// (it reads from a bounded channel, §101).
pub(crate) async fn permission_handler(
    bus: EventBus,
    registry: Arc<RunRegistry>,
    permissions: Arc<PermissionRegistry>,
    transport: Transport,
    mut rx: mpsc::Receiver<ServerRequest>,
) {
    while let Some(request) = rx.recv().await {
        if request.method != METHOD_SESSION_REQUEST_PERMISSION {
            continue; // the transport only routes this method; defensive
        }
        let Ok(params) = serde_json::from_value::<RequestPermissionParams>(request.params) else {
            let _ = transport
                .respond_error(request.id, -32602, "invalid permission request params")
                .await;
            continue;
        };
        let Some(run) = registry.active_for_harness(&params.session_id) else {
            // Unknown/external session: fail closed, no UI orphan (§57).
            let _ = transport
                .respond_error(request.id, -32601, "no active run for session")
                .await;
            continue;
        };
        if run.is_terminal() {
            let _ = transport
                .respond_error(request.id, -32601, "run already terminal")
                .await;
            continue;
        }
        let (tx, rx_decision) = oneshot::channel();
        let request_id = format!("perm-{}", Uuid::new_v4());
        permissions.insert(
            request_id.clone(),
            PendingPermission {
                session_id: run.session_id.clone(),
                run_id: run.run_id.to_string(),
                request_id: request_id.clone(),
                detail: permission_detail(&params),
                decision_tx: tx,
            },
        );
        bus.publish(Event::PermissionRequested {
            session_id: run.session_id.clone().into(),
            run_id: run.run_id.clone(),
            request_id: request_id.clone().into(),
            detail: permission_detail(&params),
        });

        // Wait for the user decision or the run's terminal (fail-closed, §57).
        let mut terminal_rx = run.terminal_rx.clone();
        let already_terminal = *terminal_rx.borrow();
        let decision = if already_terminal {
            None
        } else {
            tokio::select! {
                d = rx_decision => d.ok(), // sender dropped → None → reject
                _ = terminal_rx.changed() => None, // run terminal → reject
            }
        };
        // Idempotent cleanup: resolve_permission may already have taken it.
        let _ = permissions.take(&request_id);

        let allowed = decision.unwrap_or(false); // fail-closed: never default allow
        let result = RequestPermissionResult {
            decision: if allowed { "allow" } else { "reject" }.into(),
            option_id: pick_option(&params, allowed),
        };
        let _ = transport
            .respond(
                request.id,
                serde_json::to_value(&result).unwrap_or(serde_json::Value::Null),
            )
            .await;
        if decision.is_some() {
            bus.publish(Event::PermissionResolved {
                session_id: run.session_id.clone().into(),
                run_id: run.run_id.clone(),
                request_id: request_id.clone().into(),
                allowed,
            });
        }
    }
}

/// The single terminal outcome of a run (§67): completed | failed | cancelled
/// | unknown (the engine accepted the run but the terminal cannot be proven).
#[derive(Debug, Clone)]
pub(crate) enum TerminalOutcome {
    Completed,
    Failed(String),
    Cancelled,
    Unknown(String),
}

/// Emit the terminal event for a run. The `terminal_emitted` CAS is the one
/// gate that guarantees exactly one terminal per run even when the prompt task
/// and the engine-crash watcher race (§24, §67). Also signals the terminal
/// watch so pending permission handlers settle fail-closed (§70).
pub(crate) fn emit_terminal(bus: &EventBus, record: &Arc<RunRecord>, outcome: TerminalOutcome) {
    if record
        .terminal_emitted
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    *record.state.lock().expect("run state mutex poisoned") = match &outcome {
        TerminalOutcome::Completed => RunState::Completed,
        TerminalOutcome::Failed(_) | TerminalOutcome::Unknown(_) => RunState::Failed,
        TerminalOutcome::Cancelled => RunState::Cancelled,
    };
    let _ = record.terminal_tx.send(true);
    let session_id = record.session_id.clone().into();
    let run_id = record.run_id.clone();
    match outcome {
        TerminalOutcome::Completed => {
            let _ = bus.publish(Event::MessageCompleted { session_id, run_id });
        }
        TerminalOutcome::Failed(message) => {
            let _ = bus.publish(Event::MessageFailed {
                session_id,
                run_id,
                error: message,
            });
        }
        TerminalOutcome::Cancelled => {
            let _ = bus.publish(Event::MessageCancelled { session_id, run_id });
        }
        TerminalOutcome::Unknown(error) => {
            let _ = bus.publish(Event::MessageOutcomeUnknown {
                session_id,
                run_id,
                error,
            });
        }
    }
}

/// Map an ACP stop reason to a terminal outcome (§67, §121 — the stop reason
/// is the authoritative turn result). `end_turn` → completed; `cancelled` /
/// `discarded` → cancelled; anything else → failed with a safe message.
pub(crate) fn outcome_from_stop_reason(stop_reason: &str) -> TerminalOutcome {
    match stop_reason {
        crate::protocol::STOP_REASON_END_TURN => TerminalOutcome::Completed,
        crate::protocol::STOP_REASON_CANCELLED | crate::protocol::STOP_REASON_DISCARDED => {
            TerminalOutcome::Cancelled
        }
        other => TerminalOutcome::Failed(format!("harness turn ended with stop reason '{other}'")),
    }
}

/// Idle grace for the session-event dispatcher after the last run ends —
/// avoids a race where the terminal is emitted while a final delta is still
/// in the route channel (§37: authoritative final content reconciles the live
/// projection; a dropped tail delta would otherwise be lost).
pub(crate) const ROUTE_DRAIN_GRACE: Duration = Duration::from_millis(100);

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deepseek_truncate_multibyte_safe() {
        let kanji = "深層探求テスト".repeat(10);
        let t = truncate(&kanji, 5);
        assert_eq!(t, "深…(truncated)");
    }

    #[test]
    fn json_capped_small_is_byte_identical_to_to_string() {
        let small = json!({"cmd": "ls", "cwd": "/tmp", "args": []});
        assert_eq!(
            json_capped(&small, MAX_TOOL_INPUT_SUMMARY),
            serde_json::to_string(&small).unwrap()
        );
        // No truncation marker for in-cap payloads.
        assert!(!json_capped(&small, MAX_TOOL_INPUT_SUMMARY).contains("truncated"));
    }

    #[test]
    fn json_capped_aborts_well_below_full_serialization() {
        // A multi-megabyte nested Value — serializing it fully would take
        // ~16 MiB of output. The capped writer must stop at the cap.
        let giant_text = "x".repeat(8 * 1024 * 1024);
        let huge = json!({"a": {"b": {"c": [giant_text.clone(), giant_text, {"n": 1}]}}});
        let capped = json_capped(&huge, MAX_TOOL_INPUT_SUMMARY);
        assert!(capped.len() <= MAX_TOOL_INPUT_SUMMARY + "…(truncated)".len());
        assert!(capped.ends_with("…(truncated)"));
        assert!(capped.starts_with("{"));
    }

    #[test]
    fn bounded_tool_output_text_blocks_stop_at_cap() {
        // Two giant text blocks: accumulation must stop once the cap fills.
        let block = |n: usize| {
            json!({"content": {"text": "y".repeat(n)}})
        };
        let tc = ToolCallUpdate {
            tool_call_id: "t1".into(),
            name: Some("bash".into()),
            title: None,
            kind: None,
            status: Some("completed".into()),
            content: Some(vec![block(MAX_TOOL_OUTPUT * 2), block(MAX_TOOL_OUTPUT * 2)]),
            raw_input: None,
            raw_output: None,
        };
        let out = bounded_tool_output(&tc).expect("text path");
        assert!(out.len() <= MAX_TOOL_OUTPUT + "…(truncated)".len());
        assert!(out.ends_with("…(truncated)"));
        assert!(out.starts_with("y"));
        // The cap stopped mid-first-block; the second block was never touched.
        assert_eq!(out.matches('y').count(), MAX_TOOL_OUTPUT);
    }

    #[test]
    fn bounded_tool_output_small_text_is_untouched() {
        let tc = ToolCallUpdate {
            tool_call_id: "t1".into(),
            name: Some("bash".into()),
            title: None,
            kind: None,
            status: Some("completed".into()),
            content: Some(vec![json!({"content": {"text": "hello"}})]),
            raw_input: None,
            raw_output: None,
        };
        assert_eq!(bounded_tool_output(&tc).as_deref(), Some("hello"));
    }
}
