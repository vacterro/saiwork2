//! TASK 21 vertical slice tests (DEEPSEEK_HARNESS.md §22, TASK 21 §104–§133).
//!
//! Every test runs the deterministic fake ACP server (`fake-harness`) as a
//! **real stdio process** through the ProcessSupervisor, driving a full agent
//! turn over the wire: `session/new` → `session/prompt` → `session/update`
//! notifications (+ `session/request_permission` round-trips) → prompt
//! response, with `session/cancel` honored mid-turn. This is a
//! process/protocol test, not an in-process fake.
//!
//! Covers the hostile run matrix: normal turn, multi-step, tools, tool
//! failure, permission allow/deny/no-response (fail-closed), cancel before
//! first chunk / mid-chunk / race, provider failure, runtime crash, transport
//! loss, duplicate chunks, wrong-session events, accepted-then-response-lost,
//! session busy, restart, engine stop settles runs, and the generic
//! SessionManager flow (cross-engine contract proof).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use engine_deepseek_harness::{HarnessAdapter, HarnessConfig};
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineError, EngineHealth, EngineStartContext, RunHandle,
    SendAcceptance, SendRequest, SessionInfo,
};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{bus::Subscription, Event, EventBus};
use saiwork_process::ProcessSupervisor;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const FIXTURE: &str = env!("CARGO_BIN_EXE_fake-harness");

struct Harness {
    bus: EventBus,
    adapter: Arc<HarnessAdapter>,
    supervisor: Arc<ProcessSupervisor>,
    _tmp: TempDir,
}

fn start_ctx(bus: EventBus, supervisor: Arc<ProcessSupervisor>, workspace_path: Option<PathBuf>) -> EngineStartContext {
    let bus2 = bus.clone();
    EngineStartContext {
        workspace_id: None,
        workspace_path,
        bus: bus.clone(),
        diagnostics: Arc::new(Diagnostics::new()),
        supervisor: supervisor.clone(),
        report_failure: Arc::new(move |engine_id: &str, message: &str| {
            bus2.publish(Event::EngineFailed {
                engine_id: engine_id.into(),
                error: message.into(),
            });
        }),
    }
}

async fn new_harness(scenario: &str) -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = HarnessConfig {
        executable: Some(PathBuf::from(FIXTURE)),
        // The runtime cwd is the `session/new` primary cwd (TASK 21 §8).
        cwd: Some(tmp.path().to_path_buf()),
        handshake_timeout: Duration::from_secs(3),
        stop_grace: Duration::from_secs(1),
        stop_force: Duration::from_secs(1),
        prompt_timeout: Duration::from_secs(30),
        args: vec![scenario.into()],
        ..HarnessConfig::default()
    };
    // The vertical suite deliberately floods 10k committed chunks in one run
    // (large_stream_is_bounded_and_completes). Under parallel load a
    // 1024-cap bus can drop deltas a tight test loop cannot keep up with; the
    // bounded-bus Lagged contract is proven in saiwork-events, so this suite
    // gives its deliberate flood headroom instead of flaking.
    let bus = EventBus::with_capacity(65_536);
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let adapter = Arc::new(HarnessAdapter::new(cfg));
    Harness {
        bus,
        adapter,
        supervisor,
        _tmp: tmp,
    }
}

async fn start(h: &Harness) {
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone(), Some(h._tmp.path().to_path_buf())))
        .await
        .expect("start");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
}

async fn create_session(h: &Harness) -> SessionInfo {
    let generic = fresh_session_id();
    match h
        .adapter
        .create_session(&CreateSessionRequest {
            session_id: generic.clone(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect("create session")
    {
        saiwork_core::engine::SessionCreation::Created {
            engine_session_id,
            display_name,
        } => SessionInfo {
            id: generic,
            engine_session_id,
            display_name,
        },
        other => panic!("harness create must be Created: {other:?}"),
    }
}

/// The harness fixture always accepts a prompt; unwrap the authoritative
/// receipt to the run handle for assertions.
fn accepted(acc: SendAcceptance) -> RunHandle {
    match acc {
        SendAcceptance::Accepted { run_id } => RunHandle { run_id },
        other => panic!("expected Accepted, got {other:?}"),
    }
}

/// Unique generic session id per call (the manager mints these in the real
/// app; tests need distinct ids so no two sessions alias).
fn fresh_session_id() -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "ses-{}",
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    )
}

/// Wait for the next event; bounded so a missing event fails fast.
async fn next_event(sub: &mut Subscription) -> Event {
    timeout(Duration::from_secs(15), sub.recv())
        .await
        .expect("timed out waiting for an event")
        .expect("subscription ended")
        .event
}

/// Collect events until a terminal for `run_id`; returns all events seen.
/// Bounded by `max_events` so a missing terminal fails fast.
async fn collect_until_terminal_n(
    sub: &mut Subscription,
    run_id: &str,
    max_events: usize,
) -> Vec<Event> {
    let mut events = Vec::new();
    for _ in 0..max_events {
        let event = next_event(sub).await;
        let terminal = matches!(
            &event,
            Event::MessageCompleted { run_id: r, .. }
                | Event::MessageFailed { run_id: r, .. }
                | Event::MessageCancelled { run_id: r, .. }
                | Event::MessageOutcomeUnknown { run_id: r, .. }
                if r.to_string() == run_id
        );
        events.push(event);
        if terminal {
            return events;
        }
    }
    panic!("run {run_id} never reached a terminal");
}

async fn collect_until_terminal(sub: &mut Subscription, run_id: &str) -> Vec<Event> {
    collect_until_terminal_n(sub, run_id, 2000).await
}

fn event_run_id(e: &Event) -> Option<String> {
    match e {
        Event::MessageStarted { run_id, .. }
        | Event::MessageDelta { run_id, .. }
        | Event::MessageCompleted { run_id, .. }
        | Event::MessageFailed { run_id, .. }
        | Event::MessageCancelled { run_id, .. } => Some(run_id.to_string()),
        _ => None,
    }
}

fn deltas(events: &[Event]) -> String {
    events
        .iter()
        .filter_map(|e| match e {
            Event::MessageDelta { delta, .. } => Some(delta.clone()),
            _ => None,
        })
        .collect()
}

fn terminal_of(events: &[Event]) -> &Event {
    events
        .iter()
        .rev()
        .find(|e| {
            matches!(
                e,
                Event::MessageCompleted { .. }
                    | Event::MessageFailed { .. }
                    | Event::MessageCancelled { .. }
                    | Event::MessageOutcomeUnknown { .. }
            )
        })
        .expect("terminal event present")
}

async fn await_supervisor_empty(h: &Harness) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        if h.supervisor.count() == 0 {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("supervisor did not return to zero processes");
}

async fn assert_clean(h: &Harness) {
    await_supervisor_empty(h).await;
    assert_eq!(h.adapter.task_count(), 0, "no runtime tasks after teardown");
    assert_eq!(
        h.adapter.pending_requests(),
        0,
        "no pending protocol requests"
    );
    assert_eq!(h.adapter.active_runs(), 0, "no active runs");
    assert_eq!(h.adapter.pending_permissions(), 0, "no pending permissions");
    assert!(h.adapter.running_generation().is_none());
}

// -------------------------------------------------------------------------
// Normal turn / streaming
// -------------------------------------------------------------------------

#[tokio::test]
async fn normal_turn_streams_and_completes_exactly_once() {
    let h = new_harness("agent-normal").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;

    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "hello".into(),
            model: None,
        })
        .await
        .expect("send"));
    let run_id = handle.run_id.clone();
    let events = collect_until_terminal(&mut sub, &run_id).await;

    // Started exactly once; deltas in order; exactly one terminal (completed).
    let started = events
        .iter()
        .filter(|e| matches!(e, Event::MessageStarted { .. }))
        .count();
    assert_eq!(started, 1, "message.started exactly once");
    assert_eq!(deltas(&events), "Hello world!");
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    let terminals = events
        .iter()
        .filter(|e| matches!(e, Event::MessageCompleted { .. }))
        .count();
    assert_eq!(terminals, 1, "exactly one terminal");
    // Every event belongs to this run (no cross-routing).
    for e in &events {
        if let Some(rid) = event_run_id(e) {
            assert_eq!(rid, run_id);
        }
    }
    // No events after the terminal.
    let terminal_idx = events
        .iter()
        .position(|e| matches!(e, Event::MessageCompleted { .. }))
        .unwrap();
    assert_eq!(terminal_idx, events.len() - 1, "terminal is last");

    assert_eq!(h.adapter.active_runs(), 0);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn second_turn_in_same_session_works_after_terminal() {
    let h = new_harness("agent-normal").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;

    let first = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "one".into(),
            model: None,
        })
        .await
        .unwrap());
    collect_until_terminal(&mut sub, &first.run_id).await;

    let second = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "two".into(),
            model: None,
        })
        .await
        .expect("second turn in the same session after terminal"));
    assert_ne!(first.run_id, second.run_id);
    let events = collect_until_terminal(&mut sub, &second.run_id).await;
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn session_busy_rejects_second_concurrent_send() {
    let h = new_harness("agent-cancel").await;
    start(&h).await;
    let session = create_session(&h).await;

    let first = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "first".into(),
            model: None,
        })
        .await
        .unwrap());
    let err = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "second".into(),
            model: None,
        })
        .await
        .expect_err("second concurrent send must be rejected");
    assert!(
        matches!(err, EngineError::SessionBusy { .. }),
        "typed SessionBusy, got {err:?}"
    );
    // Settle the first run so cleanup is clean.
    h.adapter.cancel(&first.run_id).await.unwrap();
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn empty_prompt_and_oversized_prompt_rejected() {
    let h = new_harness("agent-normal").await;
    start(&h).await;
    let session = create_session(&h).await;
    let err = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "   ".into(),
            model: None,
        })
        .await
        .expect_err("empty prompt rejected");
    assert!(matches!(err, EngineError::Engine { .. }));
    let big = "x".repeat(2 * 1024 * 1024);
    let err = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: big,
            model: None,
        })
        .await
        .expect_err("oversized prompt rejected");
    assert!(matches!(err, EngineError::Engine { .. }));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn explicit_model_is_unsupported_not_silently_ignored() {
    let h = new_harness("agent-normal").await;
    start(&h).await;
    let session = create_session(&h).await;
    let err = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "hi".into(),
            model: Some("some-model".into()),
        })
        .await
        .expect_err("explicit model is unsupported on the ACP baseline (§84)");
    assert!(matches!(err, EngineError::UnsupportedCapability { .. }));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

// -------------------------------------------------------------------------
// Multi-step / tools
// -------------------------------------------------------------------------

#[tokio::test]
async fn multi_step_turn_maps_to_one_run_with_isolated_tools() {
    // Two tool cycles inside one turn: generic SAIWORK2 still sees one RunId
    // (§16, §106); tool correlation survives step transitions.
    let h = new_harness("agent-multi-step").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "do it".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;

    let tools: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e, Event::ToolStarted { .. }))
        .collect();
    assert_eq!(tools.len(), 2, "two tool cycles observed");
    let completed: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e, Event::ToolCompleted { .. }))
        .collect();
    assert_eq!(completed.len(), 2);
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    assert_eq!(deltas(&events), "Step one: Step two: Finished.");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn tool_lifecycle_normalized() {
    let h = new_harness("agent-tool").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "list".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;

    let mut saw_started = false;
    let mut saw_output = false;
    let mut saw_completed = false;
    let mut tool_name = String::new();
    for e in &events {
        match e {
            Event::ToolStarted { tool, .. } => {
                saw_started = true;
                tool_name = tool.clone();
            }
            Event::ToolOutput { tool, output, .. } => {
                assert_eq!(tool, &tool_name);
                assert!(output.contains("file.txt"), "tool output surfaced");
                saw_output = true;
            }
            Event::ToolCompleted { tool, .. } => {
                assert_eq!(tool, &tool_name);
                saw_completed = true;
            }
            _ => {}
        }
    }
    assert!(
        saw_started && saw_output && saw_completed,
        "full tool lifecycle"
    );
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

/// TASK 24 §9: a flood of stream chunks overflows the drop-with-counter
/// stream lane, yet the tool completed terminal fact must STILL arrive
/// exactly once via the NON-DROPPABLE state lane. A completed run must never
/// retain a permanently-running tool card solely because of route overflow.
#[tokio::test]
async fn tool_terminal_survives_stream_lane_overflow() {
    let h = new_harness("agent-tool-burst").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "burst".into(),
            model: None,
        })
        .await
        .unwrap());
    // Flooded deltas + tool facts + terminal exceed the default 2000 collect
    // bound; use a bound above the flood size.
    let events = collect_until_terminal_n(&mut sub, &handle.run_id, 200_000).await;

    // The tool lifecycle terminal arrived exactly once under a heavy stream.
    // (The overflow-routing invariant itself is proven deterministically at
    // the transport level: `transport::tests::tool_terminal_survives_full_
    // stream_lane` fills the stream lane and asserts the tool frames still
    // route through the non-droppable state lane.)
    let started: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e, Event::ToolStarted { .. }))
        .collect();
    let completed: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e, Event::ToolCompleted { .. }))
        .collect();
    assert_eq!(started.len(), 1, "tool started exactly once");
    assert_eq!(completed.len(), 1, "tool completed exactly once — never dropped");
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn tool_failure_does_not_fail_the_run() {
    // §53: a tool failure is not a runtime failure — the run follows the
    // upstream turn result (end_turn → completed).
    let h = new_harness("agent-tool-fail").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "fail".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        events.iter().any(|e| matches!(e, Event::ToolFailed { .. })),
        "tool.failed emitted"
    );
    assert!(
        matches!(terminal_of(&events), Event::MessageCompleted { .. }),
        "run completes despite tool failure (§53)"
    );
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

// -------------------------------------------------------------------------
// Permissions
// -------------------------------------------------------------------------

#[tokio::test]
async fn permission_allow_round_trip() {
    let h = new_harness("agent-permission-allow").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "remove".into(),
            model: None,
        })
        .await
        .unwrap());

    // Wait for permission.requested.
    let mut request_id = None;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if let Event::PermissionRequested {
            request_id: rid,
            detail,
            ..
        } = &e
        {
            assert!(detail.contains("bash"), "safe detail surfaced");
            request_id = Some(rid.to_string());
            break;
        }
    }
    let request_id = request_id.expect("permission.requested arrived");
    assert_eq!(h.adapter.pending_permissions(), 1);

    h.adapter
        .resolve_permission(&session.id, &request_id, true)
        .await
        .expect("allow");

    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::PermissionResolved { allowed: true, .. })),
        "permission.resolved(allow) published"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ToolCompleted { .. })),
        "tool completed after allow"
    );
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    assert_eq!(h.adapter.pending_permissions(), 0);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn permission_deny_resolves_and_fails_run() {
    let h = new_harness("agent-permission-deny").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "remove".into(),
            model: None,
        })
        .await
        .unwrap());

    let mut request_id = None;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if let Event::PermissionRequested {
            request_id: rid, ..
        } = &e
        {
            request_id = Some(rid.to_string());
            break;
        }
    }
    let request_id = request_id.expect("permission.requested arrived");
    h.adapter
        .resolve_permission(&session.id, &request_id, false)
        .await
        .expect("deny");

    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::PermissionResolved { allowed: false, .. })),
        "permission.resolved(deny) published"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::ToolFailed { .. })),
        "tool failed after deny"
    );
    assert!(
        matches!(terminal_of(&events), Event::MessageFailed { .. }),
        "denied turn fails"
    );
    // The engine stays healthy — a denied turn is not an engine failure (§82).
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn permission_no_response_is_fail_closed_on_cancel() {
    // §57/§113: the run is cancelled while the permission is pending — the
    // pending permission settles (no UI orphan), the run reaches a terminal.
    let h = new_harness("agent-permission-no-response").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "remove".into(),
            model: None,
        })
        .await
        .unwrap());

    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if matches!(e, Event::PermissionRequested { .. }) {
            break;
        }
    }
    assert_eq!(h.adapter.pending_permissions(), 1);
    h.adapter.cancel(&handle.run_id).await.unwrap();
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        matches!(terminal_of(&events), Event::MessageCancelled { .. }),
        "cancelled during permission"
    );
    assert_eq!(h.adapter.pending_permissions(), 0, "no orphaned permission");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn resolve_permission_unknown_is_idempotent_noop() {
    let h = new_harness("agent-normal").await;
    start(&h).await;
    let session = create_session(&h).await;
    // Unknown / already-resolved request: no-op Ok, no protocol command (§60).
    h.adapter
        .resolve_permission(&session.id, "perm-does-not-exist", true)
        .await
        .expect("unknown permission resolution is a no-op");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

// -------------------------------------------------------------------------
// Cancellation
// -------------------------------------------------------------------------

#[tokio::test]
async fn cancel_mid_chunk_produces_one_terminal_no_deltas_after() {
    let h = new_harness("agent-cancel").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "stream".into(),
            model: None,
        })
        .await
        .unwrap());

    // Wait for the first delta (acceptance + streaming began).
    let mut saw_delta = false;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if matches!(e, Event::MessageDelta { .. }) {
            saw_delta = true;
            break;
        }
    }
    assert!(saw_delta, "streaming began before cancel");
    h.adapter.cancel(&handle.run_id).await.unwrap();

    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCancelled { .. }
    ));
    // No semantic events after the terminal.
    let terminal_idx = events
        .iter()
        .position(|e| matches!(e, Event::MessageCancelled { .. }))
        .unwrap();
    assert_eq!(terminal_idx, events.len() - 1, "no events after terminal");
    assert_eq!(h.adapter.active_runs(), 0);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn cancel_race_authoritative_finish_wins() {
    // §67: the runtime still reports end_turn after a cancel raced in — the
    // authoritative stop reason wins (exactly one terminal: completed).
    let h = new_harness("agent-cancel-race").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "race".into(),
            model: None,
        })
        .await
        .unwrap());
    // Wait until the turn is provably dispatched and streaming (a delta
    // arrived) — only then race a cancel against the in-flight turn (§67).
    let mut saw_delta = false;
    for _ in 0..200 {
        if matches!(&next_event(&mut sub).await, Event::MessageDelta { .. }) {
            saw_delta = true;
            break;
        }
    }
    assert!(saw_delta, "turn dispatched before the cancel race");
    h.adapter.cancel(&handle.run_id).await.unwrap();
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        matches!(terminal_of(&events), Event::MessageCompleted { .. }),
        "authoritative finish wins over a racing cancel"
    );
    let terminals = events
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::MessageCompleted { .. }
                    | Event::MessageFailed { .. }
                    | Event::MessageCancelled { .. }
            )
        })
        .count();
    assert_eq!(terminals, 1, "exactly one terminal");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn cancel_twice_and_cancel_after_complete_are_noops() {
    let h = new_harness("agent-cancel").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .unwrap());
    h.adapter.cancel(&handle.run_id).await.unwrap();
    h.adapter.cancel(&handle.run_id).await.unwrap(); // idempotent (§65)
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCancelled { .. }
    ));
    // Cancel after terminal: no-op, no new terminal event.
    h.adapter.cancel(&handle.run_id).await.unwrap();
    sleep(Duration::from_millis(150)).await;
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn cancel_unknown_run_is_noop() {
    let h = new_harness("agent-normal").await;
    start(&h).await;
    h.adapter.cancel("run-does-not-exist").await.expect("no-op");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn cancel_before_first_chunk_settles_cancelled() {
    // §114: cancel right after send (before any chunk) → one cancelled
    // terminal; never a stuck run.
    let h = new_harness("agent-cancel").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .unwrap());
    h.adapter.cancel(&handle.run_id).await.unwrap();
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCancelled { .. }
    ));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

// -------------------------------------------------------------------------
// Failure / crash / transport
// -------------------------------------------------------------------------

#[tokio::test]
async fn provider_failure_fails_run_engine_stays_ready() {
    let h = new_harness("agent-provider-fail").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "call".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        matches!(terminal_of(&events), Event::MessageFailed { .. }),
        "provider error fails the run"
    );
    assert_eq!(
        h.adapter.health(),
        EngineHealth::Ready,
        "engine stays ready (§82)"
    );
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn runtime_crash_fails_run_and_engine() {
    let h = new_harness("agent-crash").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "crash".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        matches!(
            terminal_of(&events),
            Event::MessageFailed { .. } | Event::MessageOutcomeUnknown { .. }
        ),
        "crash settles the active run (§71) — no eternal RUNNING; outcome is honest (unknown when unproven)"
    );
    // The engine reports the crash (never silent).
    let mut saw_engine_failed = false;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if matches!(e, Event::EngineFailed { .. }) {
            saw_engine_failed = true;
            break;
        }
    }
    assert!(saw_engine_failed, "engine.failed emitted");
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    assert_eq!(h.adapter.active_runs(), 0, "no orphaned run");
    assert_clean(&h).await;
}

#[tokio::test]
async fn transport_loss_fails_run_and_engine() {
    let h = new_harness("agent-transport-loss").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "lose".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        matches!(
            terminal_of(&events),
            Event::MessageFailed { .. } | Event::MessageOutcomeUnknown { .. }
        ),
        "transport loss settles the run (§126); outcome is honest (unknown when unproven)"
    );
    let mut saw_engine_failed = false;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if matches!(e, Event::EngineFailed { .. }) {
            saw_engine_failed = true;
            break;
        }
    }
    assert!(saw_engine_failed);
    assert_clean(&h).await;
}

#[tokio::test]
async fn accepted_then_response_lost_is_outcome_unknown_no_retry() {
    // §128–§129: the prompt was accepted (a committed chunk arrived) but the
    // response was lost. ACP has no client correlation to recover the turn, so
    // the adapter fails the run honestly (outcome unknown) and NEVER replays
    // the prompt (no mutation retry, §26).
    let h = new_harness("agent-accepted-response-lost").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "lose".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        matches!(
            terminal_of(&events),
            Event::MessageFailed { .. } | Event::MessageOutcomeUnknown { .. }
        ),
        "ambiguous outcome is an honest unknown, never a fake completion"
    );
    assert_eq!(h.adapter.active_runs(), 0);
    // No auto-retry: the run is terminal and no second prompt was dispatched.
    sleep(Duration::from_millis(150)).await;
    assert_eq!(h.adapter.active_runs(), 0);
    assert_clean(&h).await;
}

/// TASK 24 §9 P0: a written prompt frame is NOT acceptance. The runtime can
/// answer with an explicit rejection AFTER the write — the receipt must be
/// DefinitelyRejected (never Accepted) and no execution evidence may be
/// published for the rejected turn.
#[tokio::test]
async fn prompt_reject_after_write_is_definitely_rejected_not_accepted() {
    let h = new_harness("agent-prompt-reject").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let receipt = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "reject me".into(),
            model: None,
        })
        .await
        .expect("send");
    assert!(
        matches!(
            &receipt,
            SendAcceptance::DefinitelyRejected { run_id, .. } if !run_id.is_empty()
        ),
        "post-write rejection must be DefinitelyRejected, got {receipt:?}"
    );
    // No execution evidence: no started, no deltas — only the failed
    // terminal, then nothing.
    let mut saw_terminal = false;
    for _ in 0..100 {
        let e = next_event(&mut sub).await;
        match e {
            Event::MessageStarted { .. } | Event::MessageDelta { .. } => {
                panic!("execution evidence after a definite rejection: {e:?}")
            }
            Event::MessageFailed { .. } => {
                saw_terminal = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_terminal, "definite rejection emits the failed terminal");
    assert_eq!(h.adapter.active_runs(), 0);
    // The runtime is still alive (a reject is not a crash): engine stays
    // Ready and can serve the next turn.
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

/// TASK 24 §9 P0: transport loss after the frame write but BEFORE any
/// execution evidence is OutcomeUnknown — the runtime may hold the prompt.
/// Never Accepted, never a definite Failed, never a replay.
#[tokio::test]
async fn transport_loss_before_evidence_is_outcome_unknown() {
    let h = new_harness("agent-loss-before-evidence").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let receipt = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "lose before evidence".into(),
            model: None,
        })
        .await
        .expect("send");
    assert!(
        matches!(
            &receipt,
            SendAcceptance::OutcomeUnknown { run_id, .. } if !run_id.is_empty()
        ),
        "pre-evidence transport loss must be OutcomeUnknown, got {receipt:?}"
    );
    let mut saw_unknown = false;
    let mut saw_failed = false;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        match e {
            Event::MessageOutcomeUnknown { .. } => {
                saw_unknown = true;
                break;
            }
            Event::MessageFailed { .. } => {
                saw_failed = true;
                break;
            }
            _ => {}
        }
    }
    assert!(!saw_failed, "unprovable outcome must never emit MessageFailed");
    assert!(
        saw_unknown,
        "unprovable outcome emits MessageOutcomeUnknown exactly once"
    );
    // No replay: exactly one run, now terminal, nothing re-dispatched.
    assert_eq!(h.adapter.active_runs(), 0);
    sleep(Duration::from_millis(150)).await;
    assert_eq!(h.adapter.active_runs(), 0);
    assert_clean(&h).await;
}

// -------------------------------------------------------------------------
// Session / event routing
// -------------------------------------------------------------------------

#[tokio::test]
async fn duplicate_chunk_does_not_corrupt_or_crash() {
    // §119: ACP v1 has no per-chunk identity (messageId identifies the
    // message, not the chunk), so text-based dedup is forbidden (§37). The
    // adapter must not crash or corrupt on a repeated chunk.
    let h = new_harness("agent-duplicate-chunk").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "dup".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn wrong_session_event_never_mutates_the_active_run() {
    // §122: a session/update for another session must not affect the active
    // run (no cross-routing).
    let h = new_harness("agent-wrong-session").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(
        !deltas(&events).contains("intruder"),
        "external session content must not leak into the active run"
    );
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn sessions_are_connection_owned_and_do_not_survive_restart() {
    // §8/§75: ACP sessions are fresh + connection-owned. After a runtime
    // restart the session registry is empty and the old session id is
    // NotFound — honest, never a fabricated reconstruction.
    let h = new_harness("agent-normal").await;
    start(&h).await;
    let session = create_session(&h).await;
    assert_eq!(h.adapter.list_sessions().await.unwrap().len(), 1);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;

    start(&h).await;
    assert_eq!(
        h.adapter.list_sessions().await.unwrap().len(),
        0,
        "fresh runtime = fresh connection = no sessions"
    );
    let err = h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect_err("stale session after restart is NotFound");
    assert!(matches!(err, EngineError::SessionNotFound { .. }));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn engine_stop_settles_active_runs() {
    // §78–§79: stopping the engine fails active runs (no eternal RUNNING) and
    // releases pending permissions.
    let h = new_harness("agent-cancel").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .unwrap());
    // Let the run start streaming, then stop the engine mid-run.
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if matches!(e, Event::MessageDelta { .. }) {
            break;
        }
    }
    h.adapter.stop().await.unwrap();
    // The run must reach a failed terminal (engine stopping).
    let mut terminal = None;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if matches!(
            &e,
            Event::MessageCompleted { run_id: r, .. }
                | Event::MessageFailed { run_id: r, .. }
                | Event::MessageCancelled { run_id: r, .. }
                | Event::MessageOutcomeUnknown { run_id: r, .. }
                if r.to_string() == handle.run_id
        ) {
            terminal = Some(e);
            break;
        }
    }
    assert!(
        matches!(
            terminal,
            Some(Event::MessageFailed { .. }) | Some(Event::MessageOutcomeUnknown { .. })
        ),
        "engine stop settles the active run (honest unknown when outcome unproven)"
    );
    assert_eq!(h.adapter.active_runs(), 0);
    assert_eq!(h.adapter.pending_permissions(), 0);
    assert_clean(&h).await;
}

#[tokio::test]
async fn large_stream_is_bounded_and_completes() {
    // §98: 10k committed chunks through the real bridge stay bounded and the
    // run completes with exactly one terminal.
    let h = new_harness("agent-large-stream").await;
    start(&h).await;
    let mut sub = h.bus.subscribe();
    let session = create_session(&h).await;
    let handle = accepted(h
        .adapter
        .send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "flood".into(),
            model: None,
        })
        .await
        .unwrap());
    let events = collect_until_terminal_n(&mut sub, &handle.run_id, 10_500).await;
    let delta_count = events
        .iter()
        .filter(|e| matches!(e, Event::MessageDelta { .. }))
        .count();
    assert_eq!(delta_count, 10_000, "all committed chunks delivered");
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    assert_eq!(h.adapter.active_runs(), 0);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

// -------------------------------------------------------------------------
// Generic SessionManager flow (cross-engine contract proof, §147–§149)
// -------------------------------------------------------------------------

#[tokio::test]
async fn generic_session_manager_flow_produces_canonical_events() {
    // The full generic workflow through saiwork-core's SessionManager — the
    // same path OpenCode/FakeEngine use: create session → send → canonical
    // events → terminal. No engine-specific branching.
    use saiwork_core::{App, AppConfig};

    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        data_root: dir.path().join("data"),
        portable: true,
    };
    let core = App::bootstrap_with(config).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cfg = HarnessConfig {
        executable: Some(PathBuf::from(FIXTURE)),
        cwd: Some(tmp.path().to_path_buf()),
        handshake_timeout: Duration::from_secs(3),
        stop_grace: Duration::from_secs(1),
        stop_force: Duration::from_secs(1),
        args: vec!["agent-normal".into()],
        ..HarnessConfig::default()
    };
    let adapter = Arc::new(HarnessAdapter::new(cfg));
    core.engines.register(adapter.clone());
    core.engines
        .start("deepseek-harness", &core.engines.start_context(None, Some(tmp.path().to_path_buf())))
        .await
        .unwrap();
    let mut sub = core.bus.subscribe();

    let session = core
        .sessions
        .create("deepseek-harness", None, None)
        .await
        .expect("SessionManager.create");
    assert_eq!(session.engine_id, "deepseek-harness");
    assert!(!session.engine_session_id.is_empty());

    // session.created is published by SessionManager (the single owner).
    let mut saw_created = false;
    for _ in 0..100 {
        let e = next_event(&mut sub).await;
        if let Event::SessionCreated { engine_id, .. } = &e {
            assert_eq!(engine_id.to_string(), "deepseek-harness");
            saw_created = true;
            break;
        }
    }
    assert!(saw_created);

    let handle = core
        .sessions
        .send(&session.id, "hello via SessionManager", None)
        .await
        .expect("SessionManager.send");
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert_eq!(deltas(&events), "Hello world!");
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));

    core.engines.stop("deepseek-harness").await.unwrap();
    let _ = core.shutdown("test").await;
}

#[tokio::test]
async fn generic_session_manager_resolve_permission_round_trip() {
    // The UI path: SessionManager.resolve_permission → adapter → upstream.
    use saiwork_core::{App, AppConfig};

    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        data_root: dir.path().join("data"),
        portable: true,
    };
    let core = App::bootstrap_with(config).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cfg = HarnessConfig {
        executable: Some(PathBuf::from(FIXTURE)),
        cwd: Some(tmp.path().to_path_buf()),
        handshake_timeout: Duration::from_secs(3),
        stop_grace: Duration::from_secs(1),
        stop_force: Duration::from_secs(1),
        args: vec!["agent-permission-allow".into()],
        ..HarnessConfig::default()
    };
    let adapter = Arc::new(HarnessAdapter::new(cfg));
    core.engines.register(adapter.clone());
    core.engines
        .start("deepseek-harness", &core.engines.start_context(None, Some(tmp.path().to_path_buf())))
        .await
        .unwrap();
    let mut sub = core.bus.subscribe();

    let session = core
        .sessions
        .create("deepseek-harness", None, None)
        .await
        .unwrap();
    let handle = core
        .sessions
        .send(&session.id, "remove", None)
        .await
        .unwrap();

    let mut request_id = None;
    for _ in 0..200 {
        let e = next_event(&mut sub).await;
        if let Event::PermissionRequested {
            request_id: rid, ..
        } = &e
        {
            request_id = Some(rid.to_string());
            break;
        }
    }
    let request_id = request_id.expect("permission requested");
    core.sessions
        .resolve_permission(&session.id, &request_id, true)
        .await
        .expect("SessionManager.resolve_permission");
    let events = collect_until_terminal(&mut sub, &handle.run_id).await;
    assert!(matches!(
        terminal_of(&events),
        Event::MessageCompleted { .. }
    ));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::PermissionResolved { allowed: true, .. })));
    core.engines.stop("deepseek-harness").await.unwrap();
    let _ = core.shutdown("test").await;
}
