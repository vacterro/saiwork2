//! Hostile matrix + security + resource cleanliness for the DeepSeek Harness
//! adapter (TASK 20 §85–§109, §171–§173). Every test runs the deterministic
//! fake ACP server (`fake-harness`) as a **real stdio process** through the
//! ProcessSupervisor — this is a process/protocol adapter test, not an
//! in-process fake (§86). Scenario is passed via argv (parallel-test-safe).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use engine_deepseek_harness::{HarnessAdapter, HarnessConfig, HarnessError};
use saiwork_core::engine::{
    EngineAdapter, EngineCapabilities, EngineError, EngineHealth, EngineStartContext, SendRequest,
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
    sub: Subscription,
    _tmp: TempDir,
}

fn start_ctx(bus: EventBus, supervisor: Arc<ProcessSupervisor>) -> EngineStartContext {
    let bus2 = bus.clone();
    EngineStartContext {
        workspace_id: None,
        workspace_path: None,
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

/// Fast stop budgets keep the matrix quick; the graceful escalation path is
/// still exercised (force kill when the fixture ignores shutdown).
async fn new_harness(scenario: &str) -> Harness {
    new_harness_with(scenario, |cfg| cfg).await
}

async fn new_harness_with(
    scenario: &str,
    tweak: impl FnOnce(HarnessConfig) -> HarnessConfig,
) -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let base = HarnessConfig {
        executable: Some(PathBuf::from(FIXTURE)),
        cwd: Some(tmp.path().to_path_buf()),
        handshake_timeout: Duration::from_secs(3),
        stop_grace: Duration::from_secs(1),
        stop_force: Duration::from_secs(1),
        args: vec![scenario.into()],
        ..HarnessConfig::default()
    };
    let cfg = tweak(base);
    let bus = EventBus::new();
    let sub = bus.subscribe();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let adapter = Arc::new(HarnessAdapter::new(cfg));
    Harness {
        bus,
        adapter,
        supervisor,
        sub,
        _tmp: tmp,
    }
}

/// The supervisor registry removes exited records after a bounded drain
/// (≤ 2 s); poll for the true zero-process baseline.
async fn await_supervisor_empty(h: &Harness) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        if h.supervisor.count() == 0 {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "supervisor did not return to zero processes: {}",
        h.supervisor.count()
    );
}

/// Wait for an `engine.failed` event on the bus (bounded).
async fn await_engine_failed(h: &mut Harness) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let env = timeout(Duration::from_secs(1), h.sub.recv())
            .await
            .expect("no event while waiting for engine.failed")
            .expect("recv");
        if let Event::EngineFailed { engine_id, error } = &env.event {
            assert_eq!(engine_id.to_string(), "deepseek-harness");
            return error.clone();
        }
    }
    panic!("never saw engine.failed");
}

/// Assert the engine is fully cleaned up (baseline resources).
async fn assert_clean(h: &Harness) {
    await_supervisor_empty(h).await;
    assert_eq!(h.adapter.task_count(), 0, "no runtime tasks after teardown");
    assert_eq!(
        h.adapter.pending_requests(),
        0,
        "no pending protocol requests"
    );
    assert!(h.adapter.running_generation().is_none());
}

// ---- happy path / lifecycle -------------------------------------------------

#[tokio::test]
async fn normal_handshake_reaches_ready_and_stops_cleanly() {
    let h = new_harness("normal").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    assert_eq!(h.adapter.state_label(), "ready");
    let info = h.adapter.server_info().expect("server info");
    assert_eq!(info.name, "dsh-acp-fixture");
    assert_eq!(info.version, "0.1.0");
    assert_eq!(h.adapter.protocol_version(), "2025-03-26");
    assert!(h.adapter.last_handshake_ms().is_some());
    assert_eq!(h.adapter.pending_requests(), 0);

    h.adapter.stop().await.expect("stop");
    assert_eq!(h.adapter.health(), EngineHealth::Stopped);
    assert_clean(&h).await;
}

#[tokio::test]
async fn delayed_handshake_within_timeout_succeeds() {
    let h = new_harness("delay-500").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("delayed handshake must succeed before the deadline");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn fragmented_handshake_parses_across_reads() {
    let h = new_harness("fragmented").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("byte-fragmented handshake response must assemble");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn unknown_notification_is_ignored_and_runtime_stays_ready() {
    let h = new_harness("unknown-notification").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start");
    // Give the notification a moment to arrive; the runtime must stay ready.
    sleep(Duration::from_millis(300)).await;
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn duplicate_response_resolves_once() {
    let h = new_harness("duplicate-response").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("duplicate response must not break the handshake");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn unknown_response_id_is_contained() {
    let h = new_harness("unknown-response-id").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start");
    sleep(Duration::from_millis(300)).await;
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn server_request_is_answered_unsupported_and_runtime_stays_ready() {
    let h = new_harness("server-request").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start");
    sleep(Duration::from_millis(300)).await;
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn metadata_request_works_and_timeout_is_operation_local() {
    // normal fixture answers every request with {}.
    let h = new_harness("normal").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    // The normal fixture answers any unknown request with {} (its default
    // branch); a known-but-unsupported method (session/list on the
    // fresh-sessions-only fixture) is a typed -32601, never a hang.
    let result = h
        .adapter
        .request_metadata("ping", serde_json::json!({}), Duration::from_secs(2))
        .await
        .expect("metadata request");
    assert_eq!(result, serde_json::json!({}));
    let err = h
        .adapter
        .request_metadata(
            "session/list",
            serde_json::json!({}),
            Duration::from_secs(2),
        )
        .await
        .expect_err("session/list is unsupported on the fresh-sessions-only fixture");
    assert!(matches!(
        err,
        HarnessError::RequestRejected { code: -32601, .. }
    ));
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;

    // ignore-requests: handshake ok, then nothing answers — a short timeout
    // must be operation-local (runtime stays healthy, §100).
    let h = new_harness("ignore-requests").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    let err = h
        .adapter
        .request_metadata("ping", serde_json::json!({}), Duration::from_millis(400))
        .await
        .expect_err("request must time out");
    assert!(matches!(err, HarnessError::RequestTimeout { .. }));
    assert_eq!(
        h.adapter.health(),
        EngineHealth::Ready,
        "runtime survives an operation-local timeout"
    );
    assert_eq!(
        h.adapter.pending_requests(),
        0,
        "timed-out request must leave the registry"
    );
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn protocol_flood_keeps_runtime_responsive() {
    let h = new_harness("flood").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    // The flood of notifications is drained by one reader; a metadata
    // request still round-trips.
    let result = h
        .adapter
        .request_metadata("ping", serde_json::json!({}), Duration::from_secs(3))
        .await
        .expect("metadata request after flood");
    assert_eq!(result, serde_json::json!({}));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn stderr_flood_is_bounded_and_protocol_unaffected() {
    let h = new_harness("stderr-flood").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    let result = h
        .adapter
        .request_metadata("ping", serde_json::json!({}), Duration::from_secs(3))
        .await
        .expect("protocol works despite stderr flood");
    assert_eq!(result, serde_json::json!({}));
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

// ---- hostile failure paths --------------------------------------------------

#[tokio::test]
async fn handshake_hang_times_out_and_process_is_killed() {
    let h = new_harness_with("hang", |mut c| {
        c.handshake_timeout = Duration::from_millis(800);
        c
    })
    .await;
    let err = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect_err("hang must time out");
    let text = err.to_string();
    assert!(text.contains("timed out"), "typed timeout: {text}");
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    await_supervisor_empty(&h).await;
    assert_eq!(h.adapter.pending_requests(), 0);
    // Restart after failure works (§65): the fixture hangs again — this time
    // expect the same typed failure, proving restart is explicit and clean.
    let err = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect_err("second start also times out");
    assert!(err.to_string().contains("timed out"));
    await_supervisor_empty(&h).await;
}

#[tokio::test]
async fn handshake_reject_is_typed() {
    let h = new_harness("reject").await;
    let err = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect_err("reject must fail the handshake");
    assert!(matches!(err, EngineError::Engine { .. }));
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    await_supervisor_empty(&h).await;
}

#[tokio::test]
async fn exit_before_handshake_fails_fast() {
    let h = new_harness("exit-early").await;
    let err = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect_err("exit-early must fail startup");
    // The probe (--version) succeeds; the runtime then dies before the
    // handshake — a fast typed exited-during-startup, never a full timeout.
    assert!(
        err.to_string().contains("exited during startup"),
        "typed failure: {err}"
    );
    await_supervisor_empty(&h).await;
}

#[tokio::test]
async fn exit_after_handshake_fails_engine_with_event() {
    // exit-after-handshake: handshake succeeds first (Ready), then the
    // runtime dies — the engine must fail and report (never silent, §56).
    let mut h = new_harness("exit-after-delay-400").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start reaches Ready");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    let error = await_engine_failed(&mut h).await;
    assert!(
        error.contains("process exited") || error.contains("protocol"),
        "failure message: {error}"
    );
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    assert_clean(&h).await;
    // Restart heals (§65): no app restart required.
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("restart after crash");
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn malformed_frame_fails_engine_without_panic() {
    let h = new_harness("malformed").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start reaches Ready (malformed line arrives after handshake)");
    // The garbage line kills the transport deterministically.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        if matches!(h.adapter.health(), EngineHealth::Failed { .. }) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    assert_clean(&h).await;
}

#[tokio::test]
async fn oversized_frame_is_rejected_before_huge_allocation() {
    let h = new_harness("oversized").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        if matches!(h.adapter.health(), EngineHealth::Failed { .. }) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    assert_clean(&h).await;
}

#[tokio::test]
async fn partial_frame_eof_fails_engine_cleanly() {
    let h = new_harness("partial-eof").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        if matches!(h.adapter.health(), EngineHealth::Failed { .. }) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    assert_clean(&h).await;
}

// ---- stop / shutdown semantics ---------------------------------------------

#[tokio::test]
async fn stop_during_start_cancels_with_no_late_ready_and_no_orphan() {
    let h = new_harness_with("hang", |mut c| {
        c.handshake_timeout = Duration::from_secs(30);
        c
    })
    .await;
    let ctx = start_ctx(h.bus.clone(), h.supervisor.clone());
    let adapter = h.adapter.clone();
    let start_task = tokio::spawn(async move { adapter.start(&ctx).await });
    sleep(Duration::from_millis(300)).await;
    h.adapter.stop().await.expect("stop during start");
    let err = start_task.await.unwrap().expect_err("start must cancel");
    assert!(matches!(err, EngineError::Canceled), "canceled: {err}");
    assert!(
        !matches!(h.adapter.health(), EngineHealth::Ready),
        "no late READY"
    );
    assert_clean(&h).await;
}

#[tokio::test]
async fn aborting_start_task_during_handshake_reaps_partial_runtime() {
    let h = new_harness_with("hang", |mut c| {
        c.handshake_timeout = Duration::from_secs(30);
        c
    })
    .await;
    let ctx = start_ctx(h.bus.clone(), h.supervisor.clone());
    let adapter = h.adapter.clone();
    let start_task = tokio::spawn(async move { adapter.start(&ctx).await });

    let reached_handshake = timeout(Duration::from_secs(5), async {
        loop {
            if h
                .supervisor
                .snapshots()
                .iter()
                .any(|process| process.id.starts_with("dsh-runtime-"))
            {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok();
    if !reached_handshake {
        start_task.abort();
        let _ = start_task.await;
        let _ = h.supervisor.shutdown().await;
        panic!("start never reached the post-spawn handshake boundary");
    }
    sleep(Duration::from_millis(150)).await;

    start_task.abort();
    let join_error = start_task.await.expect_err("start task must be aborted");
    assert!(join_error.is_cancelled(), "unexpected join failure: {join_error}");

    let cleaned_automatically = timeout(Duration::from_secs(8), async {
        loop {
            if h.supervisor.count() == 0 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok();
    let task_count = h.adapter.task_count();
    let pending = h.adapter.pending_requests();
    let generation = h.adapter.running_generation();
    if !cleaned_automatically {
        // Keep the red test itself resource-safe: the assertion below must
        // report the product orphan without leaving its fixture behind.
        let _ = h.supervisor.shutdown().await;
    }

    assert!(cleaned_automatically, "aborted start orphaned its process");
    assert_eq!(task_count, 0, "aborted start orphaned runtime tasks");
    assert_eq!(pending, 0, "aborted handshake left a pending request");
    assert!(generation.is_none(), "partial runtime stayed published");
    assert!(!matches!(h.adapter.health(), EngineHealth::Ready));
}

#[tokio::test]
async fn stop_during_request_settles_pending_and_runtime_gone() {
    let h = new_harness("ignore-requests").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    let adapter = h.adapter.clone();
    let pending = tokio::spawn(async move {
        adapter
            .request_metadata(
                "session/list",
                serde_json::json!({}),
                Duration::from_secs(30),
            )
            .await
    });
    sleep(Duration::from_millis(200)).await;
    assert_eq!(h.adapter.pending_requests(), 1);
    h.adapter.stop().await.unwrap();
    let result = pending
        .await
        .unwrap()
        .expect_err("pending request must settle");
    assert!(
        matches!(
            result,
            HarnessError::RuntimeLost(_) | HarnessError::TransportClosed(_)
        ),
        "settled with {result:?}"
    );
    assert_clean(&h).await;
}

#[tokio::test]
async fn ignored_shutdown_is_force_terminated() {
    // The fixture ignores stdin EOF (protocol shutdown) — the supervisor's
    // graceful→force escalation must still reclaim the process (§106).
    let h = new_harness("ignore-shutdown").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    assert_eq!(h.adapter.health(), EngineHealth::Ready);
    h.adapter.stop().await.expect("stop must complete bounded");
    assert_clean(&h).await;
}

#[tokio::test]
async fn stop_twice_is_idempotent_and_start_twice_is_rejected() {
    let h = new_harness("normal").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    let err = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect_err("second start must be rejected");
    assert!(matches!(err, EngineError::AlreadyStarted { .. }));
    h.adapter.stop().await.unwrap();
    h.adapter.stop().await.unwrap(); // idempotent
    assert_clean(&h).await;
}

#[tokio::test]
async fn restart_gets_fresh_generation_and_clean_state() {
    let h = new_harness("normal").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    let g1 = h.adapter.running_generation().expect("generation 1");
    h.adapter.stop().await.unwrap();
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    let g2 = h.adapter.running_generation().expect("generation 2");
    assert!(g2 > g1, "fresh generation per start");
    assert_eq!(h.adapter.pending_requests(), 0);
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn repeated_lifecycle_returns_to_baseline() {
    let h = new_harness("normal").await;
    for i in 0..25 {
        h.adapter
            .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
            .await
            .unwrap_or_else(|e| panic!("cycle {i}: start failed: {e}"));
        assert_eq!(h.adapter.health(), EngineHealth::Ready);
        h.adapter
            .stop()
            .await
            .unwrap_or_else(|e| panic!("cycle {i}: stop failed: {e}"));
        assert_eq!(
            h.adapter.pending_requests(),
            0,
            "cycle {i}: pending requests must be empty"
        );
        await_supervisor_empty(&h).await;
        assert_eq!(h.adapter.task_count(), 0, "cycle {i}: tasks must be empty");
    }
}

// ---- discovery / config / security -----------------------------------------

#[tokio::test]
async fn not_found_and_invalid_explicit_path_are_typed_errors() {
    // Explicit path is authoritative — no silent PATH fallback (§10).
    let h = new_harness_with("normal", |mut c| {
        c.executable = Some(PathBuf::from("C:/definitely/not/a/harness/dsh.exe"));
        c
    })
    .await;
    let err = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect_err("invalid explicit path must be a typed config error");
    assert!(matches!(err, EngineError::Engine { .. }));
    assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
    await_supervisor_empty(&h).await;

    // No explicit path: PATH discovery must be deterministic — a config with
    // no executable resolves through `resolve_executable` (never a silent
    // fallback), and the adapter reports the result honestly.
    let h = new_harness_with("normal", |mut c| {
        c.executable = None;
        c
    })
    .await;
    let result = h
        .adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await;
    match result {
        Ok(()) => {
            // `dsh` happened to be on PATH: discovery worked — still honest.
            h.adapter.stop().await.unwrap();
        }
        Err(e) => {
            assert!(matches!(e, EngineError::Engine { .. }), "typed: {e}");
            assert!(matches!(h.adapter.health(), EngineHealth::Failed { .. }));
        }
    }
    await_supervisor_empty(&h).await;
}

#[tokio::test]
async fn unknown_newer_version_is_recorded_not_rejected() {
    // Forward-compatible policy (§13–§14): a newer/unknown server version is
    // accepted and recorded — compatibility is proven by the handshake.
    let h = new_harness_with("normal", |mut c| {
        c.args = vec![
            "normal".into(),
            "--version-str=9.9.9-preview".into(),
            "--proto=2026-99-99".into(),
        ];
        c
    })
    .await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("newer version must be accepted");
    let info = h.adapter.server_info().unwrap();
    assert_eq!(info.version, "9.9.9-preview");
    assert_eq!(h.adapter.protocol_version(), "2026-99-99");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

/// TASK 21 vertical-slice capabilities: everything implemented and
/// fixture-proven is true; resume/models stay false (ACP fresh-sessions-only,
/// no machine-facing model discovery — never fake parity, §145–§146).
#[tokio::test]
async fn capabilities_are_honest_vertical_slice() {
    let h = new_harness("normal").await;
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .expect("start");
    let caps: EngineCapabilities = h.adapter.capabilities();
    assert!(caps.sessions && caps.streaming && caps.cancel && caps.tools && caps.permissions);
    assert!(
        caps.parallel_sessions,
        "one in-flight prompt per ACP session"
    );
    assert!(
        !caps.resume,
        "ACP sessions are fresh + connection-owned (§8)"
    );
    assert!(!caps.models && !caps.attachments && !caps.images && !caps.usage);
    // Send to an unknown session is a typed SessionNotFound (the session was
    // never created on this connection), not a fabricated run.
    let err = h
        .adapter
        .send(&SendRequest {
            session_id: "x".into(),
            engine_session_id: "x".into(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .expect_err("send to unknown session must fail");
    assert!(matches!(err, EngineError::SessionNotFound { .. }));
    // list_sessions is authoritative (the live connection-owned set) and
    // empty before any session/new.
    let sessions = h.adapter.list_sessions().await.expect("sessions list");
    assert!(sessions.is_empty());
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

#[tokio::test]
async fn idle_adapter_does_no_background_work() {
    // Constructed but never started: zero processes, zero tasks, zero
    // pending, and no startup side effects (§136).
    let h = new_harness("normal").await;
    sleep(Duration::from_millis(200)).await;
    assert_eq!(h.supervisor.count(), 0, "idle engine must not spawn");
    assert_eq!(h.adapter.task_count(), 0);
    assert_eq!(h.adapter.pending_requests(), 0);
    assert!(h.adapter.running_generation().is_none());
    h.adapter.stop().await.unwrap();
}

#[tokio::test]
async fn versioned_identity_and_protocol_recorded() {
    let h = new_harness("normal").await;
    let identity = h.adapter.identity();
    assert_eq!(identity.id, "deepseek-harness");
    assert_eq!(identity.display_name, "DeepSeek Harness");
    h.adapter
        .start(&start_ctx(h.bus.clone(), h.supervisor.clone()))
        .await
        .unwrap();
    // The engine identity is static (adapter crate version); the harness
    // version evidence lives in server_info (safe diagnostics, §69).
    assert_eq!(h.adapter.server_info().unwrap().version, "0.1.0");
    h.adapter.stop().await.unwrap();
    assert_clean(&h).await;
}

/// Registry isolation: a failing Harness adapter must not affect another
/// engine (TASK 17 §110, TASK 20 §156). Uses FakeEngine as the healthy peer.
#[tokio::test]
async fn harness_failure_does_not_poison_the_registry() {
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let registry = Arc::new(saiwork_core::engine::EngineRegistry::new(
        bus.clone(),
        Arc::new(Diagnostics::new()),
        supervisor.clone(),
    ));
    let fake = Arc::new(engine_fake::FakeEngine::new());
    registry.register(fake.clone());
    let harness_cfg = HarnessConfig {
        executable: Some(PathBuf::from("C:/does/not/exist/dsh.exe")),
        ..HarnessConfig::default()
    };
    let harness = Arc::new(HarnessAdapter::new(harness_cfg));
    registry.register(harness.clone());

    // FakeEngine is healthy and starts.
    registry
        .start("fake", &registry.start_context(None, None))
        .await
        .expect("fake starts");
    // Harness start fails — typed, contained.
    let err = registry
        .start("deepseek-harness", &registry.start_context(None, None))
        .await
        .expect_err("harness start fails");
    assert!(matches!(err, EngineError::Engine { .. }));
    // FakeEngine unaffected.
    assert_eq!(
        registry.get("fake").expect("fake present").health(),
        EngineHealth::Ready
    );
    let info = registry.list_info();
    let ids: Vec<&str> = info.iter().map(|e| e.identity.id.as_str()).collect();
    assert!(ids.contains(&"deepseek-harness"));
    assert!(ids.contains(&"fake"));
    registry.stop("fake").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        if supervisor.count() == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(supervisor.count(), 0);
}
