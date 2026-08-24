//! FakeEngine integration suite (TASK 07 §100 gate).
//!
//! Deterministic: no random sleeps, every async wait is a bounded
//! predicate/event wait, every hostile test has an overall timeout.

use std::sync::Arc;
use std::time::Duration;

use engine_fake::{FakeEngine, FakeScenario, PermissionStep, RawFrame, StartupMode};
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineError, EngineHealth, EngineStartContext,
    SendRequest, SessionInfo,
};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{bus::Subscription, Envelope, Event, EventBus};
use saiwork_process::ProcessSupervisor;
use tokio::time::{sleep, timeout};

fn start_ctx(bus: EventBus) -> EngineStartContext {
    let bus2 = bus.clone();
    EngineStartContext {
        workspace_id: None,
        workspace_path: None,
        bus: bus.clone(),
        diagnostics: Arc::new(Diagnostics::new()),
        // FakeEngine never spawns; a fresh supervisor is inert here.
        supervisor: Arc::new(ProcessSupervisor::new(bus)),
        report_failure: Arc::new(move |engine_id: &str, message: &str| {
            bus2.publish(Event::EngineFailed {
                engine_id: engine_id.into(),
                error: message.into(),
            });
        }),
    }
}

/// One harness per test: a fresh bus, a fresh engine, one long-lived
/// subscription positioned before any run (so nothing is missed).
struct Harness {
    bus: EventBus,
    engine: FakeEngine,
    session: SessionInfo,
    sub: Subscription,
}

impl Harness {
    async fn new() -> Self {
        Self::with_startup(StartupMode::Immediate).await
    }

    async fn with_startup(mode: StartupMode) -> Self {
        let bus = EventBus::new();
        let sub = bus.subscribe();
        let engine = FakeEngine::with_startup(mode);
        engine.start(&start_ctx(bus.clone())).await.expect("start");
        let session = match engine
            .create_session(&CreateSessionRequest {
                session_id: "test-session".into(),
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
                id: "test-session".into(),
                engine_session_id,
                display_name,
            },
            other => panic!("fake create must be Created: {other:?}"),
        };
        Harness {
            bus,
            engine,
            session,
            sub,
        }
    }

    async fn next(&mut self) -> Envelope {
        timeout(Duration::from_secs(15), async {
            loop {
                match self.sub.recv().await {
                    Ok(env) => return env,
                    // The bounded bus may lag under burst floods — that is
                    // correct backpressure, not a failure; skip and continue.
                    Err(saiwork_events::SubscribeError::Lagged(_)) => continue,
                    Err(e) => panic!("subscription error: {e}"),
                }
            }
        })
        .await
        .expect("timed out waiting for an event")
    }

    async fn wait_until(&mut self, name: &str) -> Envelope {
        timeout(Duration::from_secs(15), async {
            loop {
                let env = self.next().await;
                if env.event.name() == name {
                    return env;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("never saw {name}"))
    }

    /// Collect this session's message/tool/permission events until a
    /// terminal message event; returns them in order.
    async fn run_to_terminal(&mut self) -> Vec<Envelope> {
        let session_id = self.session.id.clone();
        timeout(Duration::from_secs(20), async {
            let mut seen = Vec::new();
            loop {
                let env = self.next().await;
                if !belongs(&env, &session_id) {
                    continue;
                }
                let name = env.event.name().to_string();
                let terminal = matches!(
                    name.as_str(),
                    "message.completed" | "message.failed" | "message.cancelled"
                );
                seen.push(env);
                if terminal {
                    return seen;
                }
            }
        })
        .await
        .expect("run never reached a terminal event")
    }

    async fn send(&self, prompt: &str) -> saiwork_core::engine::RunHandle {
        accepted(
            self.engine
                .send(&SendRequest {
                    session_id: self.session.id.clone(),
                    engine_session_id: self.session.engine_session_id.clone(),
                    prompt: prompt.into(),
                    model: None,
                })
                .await
                .expect("send"),
        )
    }

    async fn send_scenario(&self, scenario: FakeScenario) -> saiwork_core::engine::RunHandle {
        self.engine
            .send_scenario(
                &SendRequest {
                    session_id: self.session.id.clone(),
                    engine_session_id: self.session.engine_session_id.clone(),
                    prompt: "scenario".into(),
                    model: None,
                },
                scenario,
            )
            .await
            .expect("send_scenario")
    }
}

fn belongs(env: &Envelope, session_id: &str) -> bool {
    match &env.event {
        Event::MessageStarted { session_id: s, .. }
        | Event::MessageDelta { session_id: s, .. }
        | Event::MessageCompleted { session_id: s, .. }
        | Event::MessageFailed { session_id: s, .. }
        | Event::MessageCancelled { session_id: s, .. }
        | Event::ToolStarted { session_id: s, .. }
        | Event::ToolOutput { session_id: s, .. }
        | Event::ToolCompleted { session_id: s, .. }
        | Event::ToolFailed { session_id: s, .. }
        | Event::PermissionRequested { session_id: s, .. }
        | Event::PermissionResolved { session_id: s, .. } => s.as_str() == session_id,
        Event::EngineFailed { .. }
        | Event::RuntimeWarning { .. }
        | Event::EngineRawEvent { .. } => true,
        _ => false,
    }
}

/// Exactly one terminal outcome per run.
fn assert_single_terminal(events: &[Envelope]) {
    let count = events
        .iter()
        .filter(|e| {
            matches!(
                e.event.name(),
                "message.completed" | "message.failed" | "message.cancelled"
            )
        })
        .count();
    assert_eq!(count, 1, "expected exactly one terminal event: {events:?}");
}

/// No semantic content (deltas/tool/permission) after the terminal event.
fn assert_no_events_after_terminal(events: &[Envelope]) {
    let terminal_pos = events
        .iter()
        .position(|e| {
            matches!(
                e.event.name(),
                "message.completed" | "message.failed" | "message.cancelled"
            )
        })
        .expect("no terminal event");
    for e in &events[terminal_pos + 1..] {
        assert!(
            !matches!(
                e.event.name(),
                "message.delta"
                    | "tool.started"
                    | "tool.output"
                    | "tool.failed"
                    | "tool.completed"
                    | "permission.requested"
            ),
            "semantic event after terminal: {e:?}"
        );
    }
}

fn count_named(events: &[Envelope], name: &str) -> usize {
    events.iter().filter(|e| e.event.name() == name).count()
}

/// The fake engine's in-memory send is immediately authoritative; unwrap the
/// receipt to the run handle for assertions.
fn accepted(acc: saiwork_core::engine::SendAcceptance) -> saiwork_core::engine::RunHandle {
    match acc {
        saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
            saiwork_core::engine::RunHandle { run_id }
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
}

/// Rebuild the authoritative SessionInfo from a Created receipt (the generic
/// id is minted by the caller in these tests).
fn created_info(
    generic_id: String,
    acc: saiwork_core::engine::SessionCreation,
) -> saiwork_core::engine::SessionInfo {
    match acc {
        saiwork_core::engine::SessionCreation::Created {
            engine_session_id,
            display_name,
        } => saiwork_core::engine::SessionInfo {
            id: generic_id,
            engine_session_id,
            display_name,
        },
        other => panic!("expected Created, got {other:?}"),
    }
}

// ---- lifecycle ------------------------------------------------------------

#[tokio::test]
async fn start_stop_restart_keeps_sessions() {
    let bus = EventBus::new();
    let engine = FakeEngine::new();
    engine.start(&start_ctx(bus.clone())).await.unwrap();
    assert_eq!(engine.health(), EngineHealth::Ready);
    let session = created_info(
        "restart-session".into(),
        engine
            .create_session(&CreateSessionRequest {
                session_id: "restart-session".into(),
                workspace_id: None,
                model: None,
                title: None,
            })
            .await
            .unwrap(),
    );

    engine.stop().await.unwrap();
    assert_eq!(engine.health(), EngineHealth::Stopped);
    engine.stop().await.unwrap(); // stop twice: safe, deterministic

    // Restart: sessions survive within the adapter object (documented).
    engine.start(&start_ctx(bus)).await.unwrap();
    let listed = engine.list_sessions().await.unwrap();
    assert!(
        listed.iter().any(|s| s.id == session.id),
        "sessions must survive engine restart"
    );
}

#[tokio::test]
async fn start_twice_is_already_started() {
    let engine = FakeEngine::new();
    engine.start(&start_ctx(EventBus::new())).await.unwrap();
    let err = engine.start(&start_ctx(EventBus::new())).await.unwrap_err();
    assert!(matches!(err, EngineError::AlreadyStarted { .. }));
}

#[tokio::test]
async fn startup_failure_is_typed_and_engine_stays_failed() {
    // StartupMode::Fail is deterministic: every start() fails (restart from
    // FAILED is exercised by the crash test on an Immediate engine).
    let engine = FakeEngine::with_startup(StartupMode::Fail);
    let err = engine.start(&start_ctx(EventBus::new())).await.unwrap_err();
    assert!(matches!(
        err,
        EngineError::Engine { ref message, .. } if message.contains("startup failure")
    ));
    assert!(matches!(engine.health(), EngineHealth::Failed { .. }));
    // Sends are rejected while the engine is failed.
    let err = engine
        .send(&SendRequest {
            session_id: "x".into(),
            engine_session_id: "x".into(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Crashed { .. }));
}

#[tokio::test]
async fn stop_during_delayed_start_cancels_and_never_becomes_ready() {
    let engine = Arc::new(FakeEngine::with_startup(StartupMode::DelayedMs(30_000)));
    let ctx = start_ctx(EventBus::new());
    let engine2 = engine.clone();
    let start_task = tokio::spawn(async move { engine2.start(&ctx).await });
    sleep(Duration::from_millis(150)).await;
    engine.stop().await.unwrap();
    let err = start_task.await.unwrap().unwrap_err();
    assert!(matches!(err, EngineError::Canceled));
    assert!(!matches!(engine.health(), EngineHealth::Ready));
}

#[tokio::test]
async fn startup_hang_is_cancelled_by_stop() {
    let engine = Arc::new(FakeEngine::with_startup(StartupMode::Hang));
    let ctx = start_ctx(EventBus::new());
    let engine2 = engine.clone();
    let start_task = tokio::spawn(async move { engine2.start(&ctx).await });
    sleep(Duration::from_millis(150)).await;
    engine.stop().await.unwrap();
    let err = start_task.await.unwrap().unwrap_err();
    assert!(matches!(err, EngineError::Canceled));
}

#[tokio::test]
async fn send_before_start_and_after_stop_fails_cleanly() {
    let engine = FakeEngine::new();
    // Before start.
    let err = engine
        .send(&SendRequest {
            session_id: "x".into(),
            engine_session_id: "x".into(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::NotStarted { .. }));

    engine.start(&start_ctx(EventBus::new())).await.unwrap();
    engine.stop().await.unwrap();
    let err = engine
        .send(&SendRequest {
            session_id: "x".into(),
            engine_session_id: "x".into(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::NotStarted { .. }));
}

// ---- sessions ---------------------------------------------------------------

#[tokio::test]
async fn session_ops_and_unknown_session_errors() {
    let h = Harness::new().await;
    let sessions = h.engine.list_sessions().await.unwrap();
    assert_eq!(sessions.len(), 1);

    // Unknown session send → typed error.
    let err = h
        .engine
        .send(&SendRequest {
            session_id: "no-such-session".into(),
            engine_session_id: "no-such-engine-session".into(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::SessionNotFound { .. }));

    // Unknown resume → typed error.
    let err = h
        .engine
        .resume_session("no-such-engine-session")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::SessionNotFound { .. }));

    // Delete works; resume after delete fails.
    h.engine
        .delete_session(&h.session.engine_session_id)
        .await
        .unwrap();
    let err = h
        .engine
        .resume_session(&h.session.engine_session_id)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::SessionNotFound { .. }));
}

// ---- streaming -------------------------------------------------------------

#[tokio::test]
async fn normal_run_streams_in_order_and_terminates_once() {
    let mut h = Harness::new().await;
    let _handle = h.send("/sim:normal").await;
    let events = h.run_to_terminal().await;
    assert_single_terminal(&events);
    assert_no_events_after_terminal(&events);
    assert_eq!(events.first().unwrap().event.name(), "message.started");
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
    assert!(count_named(&events, "message.delta") >= 2);
    // Deltas arrive in emission order (global seq increases).
    let delta_seqs: Vec<u64> = events
        .iter()
        .filter(|e| e.event.name() == "message.delta")
        .map(|e| e.seq)
        .collect();
    assert!(delta_seqs.windows(2).all(|w| w[0] < w[1]));
}

#[tokio::test]
async fn empty_response_has_zero_deltas() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::empty_response()).await;
    let events = h.run_to_terminal().await;
    assert_eq!(count_named(&events, "message.delta"), 0);
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
    assert_single_terminal(&events);
}

#[tokio::test]
async fn single_and_large_deltas() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::single_delta()).await;
    let events = h.run_to_terminal().await;
    assert_eq!(count_named(&events, "message.delta"), 1);

    h.send_scenario(FakeScenario::large_delta()).await;
    let events = h.run_to_terminal().await;
    let delta = events
        .iter()
        .find(|e| matches!(e.event, Event::MessageDelta { .. }))
        .map(|e| match &e.event {
            Event::MessageDelta { delta, .. } => delta.clone(),
            _ => unreachable!(),
        })
        .unwrap();
    assert_eq!(delta.len(), 128 * 1024, "large single delta preserved");
}

#[tokio::test]
async fn burst_emits_all_deltas() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::burst(500)).await;
    let events = h.run_to_terminal().await;
    assert_eq!(count_named(&events, "message.delta"), 500);
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
}

#[tokio::test]
async fn large_stream_completes_without_deadlock() {
    let mut h = Harness::new().await;
    let handle = h.send_scenario(FakeScenario::large_stream(10_000)).await;
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
    assert_single_terminal(&events);
    // Producer emitted every delta (engine-side count; the bus may lag a
    // slow consumer, never the producer).
    assert_eq!(h.engine.emitted_deltas(&handle.run_id), 10_000);
}

// ---- cancellation ----------------------------------------------------------

#[tokio::test]
async fn cancel_mid_stream_emits_cancelled_and_stops() {
    let mut h = Harness::new().await;
    let handle = h.send("/sim:slow").await;
    let started = h.wait_until("message.started").await;
    assert_eq!(started.event.name(), "message.started");
    // Give it a couple of deltas, then cancel.
    h.wait_until("message.delta").await;
    h.engine.cancel(&handle.run_id).await.unwrap();
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.cancelled");
    assert_single_terminal(&events);
    assert_no_events_after_terminal(&events);
    assert_eq!(count_named(&events, "message.completed"), 0);
}

#[tokio::test]
async fn double_cancel_and_cancel_after_complete_are_noops() {
    let mut h = Harness::new().await;
    let handle = h.send("/sim:slow").await;
    h.engine.cancel(&handle.run_id).await.unwrap();
    h.engine.cancel(&handle.run_id).await.unwrap(); // double cancel: no-op
    let events = h.run_to_terminal().await;
    assert_single_terminal(&events);
    assert_eq!(count_named(&events, "message.cancelled"), 1);

    // Cancel after complete: no second terminal, no error.
    let handle = h.send("/sim:normal").await;
    h.run_to_terminal().await;
    h.engine.cancel(&handle.run_id).await.unwrap();
    h.engine.cancel("no-such-run").await.unwrap(); // unknown: no-op
    assert_eq!(h.engine.active_runs(), 0);
}

#[tokio::test]
async fn cancel_under_event_pressure_stops_producer() {
    let mut h = Harness::new().await;
    // Large zero-delay burst: completion is impossible before the cancel
    // lands (send returns before the run task starts publishing), so the
    // outcome is deterministic: the producer stops, the run cancels.
    let total = 200_000;
    let handle = h.send_scenario(FakeScenario::burst(total)).await;
    h.engine.cancel(&handle.run_id).await.unwrap();
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.cancelled");
    assert_eq!(count_named(&events, "message.completed"), 0);
    // Producer stopped: the emitted counter settles well below the total.
    for _ in 0..100 {
        if h.engine.active_runs() == 0 {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(h.engine.active_runs(), 0);
    let emitted = h.engine.emitted_deltas(&handle.run_id);
    assert!(
        emitted < total,
        "producer must stop emitting after cancel (emitted {emitted})"
    );
}

#[tokio::test]
async fn cancel_vs_completion_race_has_exactly_one_terminal() {
    let mut h = Harness::new().await;
    for i in 0..10 {
        let handle = h.send_scenario(FakeScenario::single_delta()).await;
        // Cancel immediately: the final delta races with the cancellation.
        h.engine.cancel(&handle.run_id).await.unwrap();
        let events = h.run_to_terminal().await;
        assert_single_terminal(&events);
        let name = events.last().unwrap().event.name();
        assert!(
            name == "message.completed" || name == "message.cancelled",
            "iteration {i}: ambiguous outcome {name}"
        );
    }
}

// ---- failure / hang --------------------------------------------------------

#[tokio::test]
async fn mid_stream_failure_emits_failed_and_no_completed() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::mid_stream_failure(3)).await;
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.failed");
    assert_eq!(count_named(&events, "message.completed"), 0);
    assert_eq!(count_named(&events, "message.delta"), 3);
    assert_no_events_after_terminal(&events);
}

#[tokio::test]
async fn hang_run_terminates_on_cancel() {
    let mut h = Harness::new().await;
    let handle = h.send_scenario(FakeScenario::hang()).await;
    h.wait_until("message.started").await;
    h.engine.cancel(&handle.run_id).await.unwrap();
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.cancelled");
}

#[tokio::test]
async fn engine_crash_fails_run_and_rejects_new_sends() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::engine_crash()).await;
    let events = h.run_to_terminal().await;
    // The run reaches a terminal state AND the engine failure is reported.
    assert_eq!(events.last().unwrap().event.name(), "message.failed");
    assert!(count_named(&events, "engine.failed") >= 1);
    assert!(matches!(h.engine.health(), EngineHealth::Failed { .. }));

    // New sends are rejected until restart.
    let err = h
        .engine
        .send(&SendRequest {
            session_id: h.session.id.clone(),
            engine_session_id: h.session.engine_session_id.clone(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Crashed { .. }));

    // Restart heals the engine.
    h.engine.start(&start_ctx(h.bus.clone())).await.unwrap();
    assert_eq!(h.engine.health(), EngineHealth::Ready);
}

#[tokio::test]
async fn engine_crash_terminates_other_active_runs() {
    let mut h = Harness::new().await;
    let victim = h.send_scenario(FakeScenario::engine_crash()).await;
    // A second, hanging run must also reach a terminal state when the engine
    // crashes (§30/§75).
    let other = h.send_scenario(FakeScenario::hang()).await;
    let mut terminals = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let env = timeout(Duration::from_secs(1), h.next())
            .await
            .expect("no event while waiting for both runs to die");
        if matches!(
            env.event.name(),
            "message.completed" | "message.failed" | "message.cancelled"
        ) {
            terminals += 1;
            if terminals >= 2 {
                break;
            }
        }
    }
    assert_eq!(terminals, 2, "both runs must terminate after engine crash");
    assert!(h.engine.active_runs() == 0);
    let _ = victim;
    let _ = other;
}

#[tokio::test]
async fn connection_loss_fails_run_but_not_engine() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::connection_loss()).await;
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.failed");
    assert!(matches!(h.engine.health(), EngineHealth::Ready));
}

// ---- tools -----------------------------------------------------------------

#[tokio::test]
async fn tool_and_text_interleave_in_order() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::tool_and_text()).await;
    let events = h.run_to_terminal().await;
    assert_single_terminal(&events);
    assert_no_events_after_terminal(&events);
    // started → delta* → tool.started → tool.output → tool.completed → delta* → completed
    let names: Vec<&str> = events.iter().map(|e| e.event.name()).collect();
    let t_started = names.iter().position(|n| *n == "tool.started").unwrap();
    let t_completed = names.iter().position(|n| *n == "tool.completed").unwrap();
    assert_eq!(
        &names[t_started..=t_completed],
        &["tool.started", "tool.output", "tool.completed"]
    );
    assert_eq!(names.last(), Some(&"message.completed"));
    assert!(names[..t_started].contains(&"message.delta"));
    assert!(names[t_completed + 1..].contains(&"message.delta"));
}

#[tokio::test]
async fn tool_failure_fails_run_but_engine_stays_healthy() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::tool_failure()).await;
    let events = h.run_to_terminal().await;
    assert!(count_named(&events, "tool.failed") == 1);
    assert_eq!(events.last().unwrap().event.name(), "message.failed");
    assert!(matches!(h.engine.health(), EngineHealth::Ready));
}

// ---- permissions -----------------------------------------------------------

/// Hostile scheduler (multi-thread, TASK 24 §9): the consumer resolves the
/// permission IMMEDIATELY on receipt. The pending slot must already exist
/// before `permission.requested` is published, so this can never no-op into an
/// eternal Await — the run proceeds exactly once and the pending map drains.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permission_allow_flow() {
    let mut h = Harness::new().await;
    let handle = h
        .send_scenario(FakeScenario::permission(PermissionStep::Await))
        .await;
    let requested = h.wait_until("permission.requested").await;
    let request_id = match &requested.event {
        Event::PermissionRequested { request_id, .. } => request_id.to_string(),
        _ => unreachable!(),
    };
    assert_eq!(h.engine.pending_permissions(), 1);
    h.engine
        .resolve_permission(&h.session.id, &request_id, true)
        .await
        .unwrap();
    let events = h.run_to_terminal().await;
    assert!(count_named(&events, "permission.resolved") == 1);
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
    assert_eq!(h.engine.pending_permissions(), 0);
    // Resolve again: idempotent no-op.
    h.engine
        .resolve_permission(&h.session.id, &request_id, false)
        .await
        .unwrap();
    let _ = handle;
}

#[tokio::test]
async fn permission_deny_fails_run() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::permission(PermissionStep::Await))
        .await;
    let requested = h.wait_until("permission.requested").await;
    let request_id = match &requested.event {
        Event::PermissionRequested { request_id, .. } => request_id.to_string(),
        _ => unreachable!(),
    };
    h.engine
        .resolve_permission(&h.session.id, &request_id, false)
        .await
        .unwrap();
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.failed");
    assert!(count_named(&events, "tool.failed") == 1);
}

#[tokio::test]
async fn cancel_while_permission_pending_releases_it() {
    let mut h = Harness::new().await;
    let handle = h
        .send_scenario(FakeScenario::permission(PermissionStep::Await))
        .await;
    h.wait_until("permission.requested").await;
    assert_eq!(h.engine.pending_permissions(), 1);
    h.engine.cancel(&handle.run_id).await.unwrap();
    let events = h.run_to_terminal().await;
    assert_eq!(events.last().unwrap().event.name(), "message.cancelled");
    assert_eq!(h.engine.pending_permissions(), 0);
}

#[tokio::test]
async fn engine_stop_while_permission_pending_releases_all() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::permission(PermissionStep::Await))
        .await;
    h.wait_until("permission.requested").await;
    assert_eq!(h.engine.pending_permissions(), 1);
    h.engine.stop().await.unwrap();
    // The pending wait resolves (cancelled); no eternal suspension (§26).
    let events = h.run_to_terminal().await;
    assert!(matches!(
        events.last().unwrap().event.name(),
        "message.cancelled" | "message.failed"
    ));
    assert_eq!(h.engine.pending_permissions(), 0);
    assert_eq!(h.engine.active_runs(), 0);
}

// ---- hostile raw boundary --------------------------------------------------

#[tokio::test]
async fn duplicate_raw_frame_is_dropped_with_diagnostic() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::duplicate_frame()).await;
    let events = h.run_to_terminal().await;
    assert_eq!(
        count_named(&events, "message.delta"),
        1,
        "duplicate must not re-enter the stream"
    );
    assert!(count_named(&events, "engine.raw_event") >= 1);
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
}

#[tokio::test]
async fn malformed_raw_frame_is_contained_and_stream_continues() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::malformed_frame()).await;
    let events = h.run_to_terminal().await;
    assert!(count_named(&events, "runtime.warning") >= 1);
    assert_eq!(count_named(&events, "message.delta"), 1);
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
}

#[tokio::test]
async fn unknown_raw_frame_is_ignored_with_diagnostic() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::unknown_frame()).await;
    let events = h.run_to_terminal().await;
    assert!(count_named(&events, "runtime.warning") >= 1);
    assert_eq!(count_named(&events, "message.delta"), 0);
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
}

#[tokio::test]
async fn out_of_order_raw_frame_is_rejected_not_reordered() {
    let mut h = Harness::new().await;
    h.send_scenario(FakeScenario::out_of_order_frame()).await;
    let events = h.run_to_terminal().await;
    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|e| match &e.event {
            Event::MessageDelta { delta, .. } => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        deltas,
        vec!["later", "next"],
        "accepted order preserved; out-of-order frame dropped"
    );
    assert_eq!(events.last().unwrap().event.name(), "message.completed");
}

#[tokio::test]
async fn live_push_raw_uses_the_same_boundary() {
    let h = Harness::new().await;
    let session_id: saiwork_events::SessionId = h.session.id.clone().into();
    let run_id: saiwork_events::RunId = "r1".into();
    let first = h.engine.push_raw(
        &session_id,
        &run_id,
        RawFrame {
            seq: 1,
            kind: "delta",
            payload: Some("one".into()),
        },
    );
    assert!(matches!(
        first,
        engine_fake::NormalizedFrame::Event(Event::MessageDelta { .. })
    ));
    let dup = h.engine.push_raw(
        &session_id,
        &run_id,
        RawFrame {
            seq: 1,
            kind: "delta",
            payload: Some("one".into()),
        },
    );
    assert!(
        matches!(dup, engine_fake::NormalizedFrame::ProtocolNote { .. }),
        "duplicate must be caught at the boundary"
    );
}

// ---- isolation / concurrency -----------------------------------------------

#[tokio::test]
async fn multiple_sessions_are_isolated() {
    let bus = EventBus::new();
    let engine = FakeEngine::new();
    engine.start(&start_ctx(bus.clone())).await.unwrap();
    let sa = created_info(
        "sa".into(),
        engine
            .create_session(&CreateSessionRequest {
                session_id: "sa".into(),
                workspace_id: None,
                model: None,
                title: Some("A".into()),
            })
            .await
            .unwrap(),
    );
    let sb = created_info(
        "sb".into(),
        engine
            .create_session(&CreateSessionRequest {
                session_id: "sb".into(),
                workspace_id: None,
                model: None,
                title: Some("B".into()),
            })
            .await
            .unwrap(),
    );
    let mut sub = bus.subscribe();

    let ha = accepted(
        engine
            .send(&SendRequest {
                session_id: sa.id.clone(),
                engine_session_id: sa.engine_session_id.clone(),
                prompt: "/sim:slow".into(),
                model: None,
            })
            .await
            .unwrap(),
    );
    engine
        .send(&SendRequest {
            session_id: sb.id.clone(),
            engine_session_id: sb.engine_session_id.clone(),
            prompt: "/sim:normal".into(),
            model: None,
        })
        .await
        .unwrap();

    // Cancel A's run; B must be unaffected.
    engine.cancel(&ha.run_id).await.unwrap();

    let mut a_terminal = false;
    let mut b_terminal = false;
    let mut b_deltas = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline && !(a_terminal && b_terminal) {
        let env = timeout(Duration::from_secs(1), sub.recv())
            .await
            .expect("timeout")
            .expect("recv");
        match &env.event {
            Event::MessageDelta { session_id: s, .. } if s.as_str() == sb.id => b_deltas += 1,
            Event::MessageCancelled { session_id: s, .. } if s.as_str() == sa.id => {
                a_terminal = true
            }
            Event::MessageCompleted { session_id: s, .. } if s.as_str() == sb.id => {
                b_terminal = true
            }
            _ => {}
        }
    }
    assert!(a_terminal, "A must reach a terminal state");
    assert!(b_terminal, "B must complete unaffected by A's cancel");
    assert!(b_deltas > 0);
    assert_eq!(engine.active_runs(), 0);
}

#[tokio::test]
async fn parallel_runs_in_same_session_are_isolated() {
    let mut h = Harness::new().await;
    // Same-session concurrency policy: allowed; each run has its own RunId
    // and events carry it, so consumers can filter.
    let ha = h.send_scenario(FakeScenario::single_delta()).await;
    let hb = h.send_scenario(FakeScenario::single_delta()).await;
    let mut a_terminal = 0;
    let mut b_terminal = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline && (a_terminal == 0 || b_terminal == 0) {
        let env = timeout(Duration::from_secs(1), h.next())
            .await
            .expect("timeout");
        let run = match &env.event {
            Event::MessageCompleted { run_id, .. }
            | Event::MessageFailed { run_id, .. }
            | Event::MessageCancelled { run_id, .. } => run_id.to_string(),
            _ => continue,
        };
        if run == ha.run_id {
            a_terminal += 1;
        } else if run == hb.run_id {
            b_terminal += 1;
        }
    }
    assert_eq!(a_terminal, 1);
    assert_eq!(b_terminal, 1);
    assert_eq!(h.engine.active_runs(), 0);
}

#[tokio::test]
async fn subscriber_removal_does_not_affect_run_or_other_subscribers() {
    let bus = EventBus::new();
    let mut sub1 = bus.subscribe();
    let mut sub2 = bus.subscribe();
    let engine = FakeEngine::new();
    engine.start(&start_ctx(bus.clone())).await.unwrap();
    let session = created_info(
        "sub-session".into(),
        engine
            .create_session(&CreateSessionRequest {
                session_id: "sub-session".into(),
                workspace_id: None,
                model: None,
                title: None,
            })
            .await
            .unwrap(),
    );
    let handle = accepted(
        engine
            .send(&SendRequest {
                session_id: session.id.clone(),
                engine_session_id: session.engine_session_id.clone(),
                prompt: "/sim:slow".into(),
                model: None,
            })
            .await
            .unwrap(),
    );

    // Both subscribers see the start.
    let mut s1_saw_start = false;
    let mut s2_saw_start = false;
    for _ in 0..50 {
        let e1 = timeout(Duration::from_millis(300), sub1.recv())
            .await
            .unwrap()
            .unwrap();
        if e1.event.name() == "message.started" {
            s1_saw_start = true;
        }
        let e2 = timeout(Duration::from_millis(300), sub2.recv())
            .await
            .unwrap()
            .unwrap();
        if e2.event.name() == "message.started" {
            s2_saw_start = true;
        }
        if s1_saw_start && s2_saw_start {
            break;
        }
    }
    assert!(s1_saw_start && s2_saw_start);

    // Drop subscriber 2 mid-run; the run and subscriber 1 continue.
    drop(sub2);
    engine.cancel(&handle.run_id).await.unwrap();
    let mut saw_terminal = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !saw_terminal {
        let env = timeout(Duration::from_secs(1), sub1.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(
            env.event.name(),
            "message.cancelled" | "message.completed" | "message.failed"
        ) {
            saw_terminal = true;
        }
    }
    assert!(
        saw_terminal,
        "remaining subscriber must see the terminal event"
    );
    assert_eq!(engine.active_runs(), 0);
}

#[tokio::test]
async fn slow_consumer_never_blocks_producer() {
    let bus = EventBus::new();
    // This subscriber never reads: it will lag the bounded bus — which must
    // not block or deadlock the producer (§78).
    let _slow = bus.subscribe();
    let engine = FakeEngine::new();
    engine.start(&start_ctx(bus.clone())).await.unwrap();
    let session = created_info(
        "slow-session".into(),
        engine
            .create_session(&CreateSessionRequest {
                session_id: "slow-session".into(),
                workspace_id: None,
                model: None,
                title: None,
            })
            .await
            .unwrap(),
    );
    let handle = engine
        .send_scenario(
            &SendRequest {
                session_id: session.id.clone(),
                engine_session_id: session.engine_session_id.clone(),
                prompt: "x".into(),
                model: None,
            },
            FakeScenario::burst(3_000),
        )
        .await
        .unwrap();
    // Producer completes despite the slow consumer.
    for _ in 0..200 {
        if engine.active_runs() == 0 {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(engine.active_runs(), 0, "producer must not deadlock");
    assert_eq!(engine.emitted_deltas(&handle.run_id), 3_000);
}

// ---- cleanup ---------------------------------------------------------------

#[tokio::test]
async fn dispose_releases_runs_permissions_and_tasks() {
    let bus = EventBus::new();
    let engine = FakeEngine::new();
    engine.start(&start_ctx(bus.clone())).await.unwrap();
    let session = created_info(
        "dispose-session".into(),
        engine
            .create_session(&CreateSessionRequest {
                session_id: "dispose-session".into(),
                workspace_id: None,
                model: None,
                title: None,
            })
            .await
            .unwrap(),
    );
    let req = SendRequest {
        session_id: session.id.clone(),
        engine_session_id: session.engine_session_id.clone(),
        prompt: "x".into(),
        model: None,
    };
    // One hanging run + one pending-permission run.
    engine
        .send_scenario(&req, FakeScenario::hang())
        .await
        .unwrap();
    engine
        .send_scenario(&req, FakeScenario::permission(PermissionStep::Await))
        .await
        .unwrap();
    // Wait until the permission is pending, then dispose.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && engine.pending_permissions() == 0 {
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(engine.pending_permissions(), 1);
    assert_eq!(engine.active_runs(), 2);

    engine.dispose();
    engine.dispose(); // idempotent

    // Everything is released (bounded wait for task workers to wind down).
    for _ in 0..100 {
        if engine.active_runs() == 0 && engine.pending_permissions() == 0 {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(engine.active_runs(), 0);
    assert_eq!(engine.pending_permissions(), 0);
    assert_eq!(engine.task_count(), 0, "no background tasks after dispose");
    assert!(matches!(engine.health(), EngineHealth::Stopped));

    let err = engine.send(&req).await.unwrap_err();
    assert!(matches!(err, EngineError::NotStarted { .. }));
}

#[tokio::test]
async fn command_history_records_lifecycle() {
    let mut h = Harness::new().await;
    let handle = h.send("/sim:single").await;
    h.engine.cancel(&handle.run_id).await.unwrap();
    let _ = h.run_to_terminal().await;
    h.engine.stop().await.unwrap();
    let commands = h.engine.received_commands();
    assert!(commands.iter().any(|c| c == "start"));
    assert!(commands.iter().any(|c| c == "create_session"));
    assert!(commands.iter().any(|c| c == "send:single"));
    assert!(commands.iter().any(|c| c == "cancel"));
    assert!(commands.iter().any(|c| c == "stop"));
}

#[tokio::test]
async fn invalid_scenario_rejected_before_run() {
    let h = Harness::new().await;
    let mut bad = FakeScenario::hang();
    bad.fail_after_delta = Some(2); // contradictory: hang + failure terminal
    let err = h
        .engine
        .send_scenario(
            &SendRequest {
                session_id: h.session.id.clone(),
                engine_session_id: h.session.engine_session_id.clone(),
                prompt: "x".into(),
                model: None,
            },
            bad,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Engine { .. }));
    assert_eq!(h.engine.active_runs(), 0);
}
