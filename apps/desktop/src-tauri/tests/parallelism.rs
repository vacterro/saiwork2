//! Parallelism + workspace-safety integration tests (TASK 18 §12–§13,
//! §21–§22, §190–§199, §37–§39).
//!
//! Decision under test: different-workspace runs may run concurrently;
//! same-workspace mutating runs are **serialized** (one agent run per
//! physical workspace — no worktrees). Same-session concurrency stays the
//! engine's own REJECT contract. The durable queue keeps concurrency = 1.
//!
//! Uses the real `saiwork-core::App` + `engine-fake` in isolated temporary
//! data roots (never the developer's real data). FakeEngine is deterministic
//! (fixed delays, no randomness).

use std::sync::Arc;

use engine_fake::FakeEngine;
use saiwork_core::engine::{EngineAdapter, RunHandle};
use saiwork_core::queue_port::QueueEnginePort;
use saiwork_core::{App, AppConfig, CoreError};
use saiwork_events::{bus::Subscription, Event};
use saiwork_queue::EnginePort;
use tokio::time::{sleep, timeout, Duration};

fn temp_config() -> (tempfile::TempDir, AppConfig) {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = AppConfig {
        data_root: dir.path().join("data"),
        portable: true,
    };
    (dir, config)
}

struct Harness {
    _dir: tempfile::TempDir,
    core: Arc<App>,
    engine: Arc<FakeEngine>,
    engine_id: String,
    w1: String,
    w2: String,
}

async fn harness() -> Harness {
    let (dir, config) = temp_config();
    let core = App::bootstrap_with(config).unwrap();
    let w1_path = dir.path().join("w1");
    let w2_path = dir.path().join("w2");
    std::fs::create_dir_all(&w1_path).unwrap();
    std::fs::create_dir_all(&w2_path).unwrap();
    let w1 = core.workspaces.open(&w1_path).await.unwrap().id;
    let w2 = core.workspaces.open(&w2_path).await.unwrap().id;
    let engine = Arc::new(FakeEngine::new());
    let engine_id = engine.identity().id.clone();
    core.engines.register(engine.clone());
    core.engines
        .start(&engine_id, &core.engines.start_context(None, None))
        .await
        .unwrap();
    Harness {
        _dir: dir,
        core,
        engine,
        engine_id,
        w1,
        w2,
    }
}

async fn wait_for(sub: &mut Subscription, pred: impl Fn(&Event) -> bool) -> Event {
    for _ in 0..400 {
        match timeout(Duration::from_millis(250), sub.recv()).await {
            Ok(Ok(env)) if pred(&env.event) => return env.event,
            Ok(Ok(_)) => continue,
            Ok(Err(_)) => panic!("subscription ended"),
            Err(_) => panic!("timed out waiting for event"),
        }
    }
    panic!("event never arrived")
}

fn run_id(e: &Event) -> Option<String> {
    match e {
        Event::MessageStarted { run_id, .. }
        | Event::MessageDelta { run_id, .. }
        | Event::MessageCompleted { run_id, .. }
        | Event::MessageFailed { run_id, .. }
        | Event::MessageCancelled { run_id, .. } => Some(run_id.to_string()),
        _ => None,
    }
}

/// Different workspaces run concurrently and stay fully isolated (§13):
/// B is accepted while A is active, B completes, A is untouched, cancelling
/// A affects only A.
#[tokio::test]
async fn different_workspaces_run_concurrently_and_isolated() {
    let h = harness().await;
    let mut sub = h.core.bus.subscribe();

    let a = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();
    let b = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w2), None)
        .await
        .unwrap();

    // A never completes on its own (hang); B completes normally.
    let ha = h
        .core
        .sessions
        .send(&a.id, "/sim:hang", None)
        .await
        .unwrap();
    let hb: RunHandle = h
        .core
        .sessions
        .send(&b.id, "/sim:normal", None)
        .await
        .expect("different-workspace send must be accepted");

    assert_ne!(ha.run_id, hb.run_id, "distinct RunIds");

    // B completes while A is still active — two runs coexisted (the whole
    // point: different-workspace concurrency is real).
    let done_b = wait_for(
        &mut sub,
        |e| matches!(e, Event::MessageCompleted { run_id, .. } if run_id.to_string() == hb.run_id),
    )
    .await;
    assert_eq!(run_id(&done_b).as_deref(), Some(hb.run_id.as_str()));
    assert_eq!(
        h.engine.active_runs(),
        1,
        "A (hang) still active after B completed"
    );

    // Cancel A: only A reaches Cancelled; B stays completed (one terminal each).
    h.core.sessions.cancel(&a.id, &ha.run_id).await.unwrap();
    let cancelled_a = wait_for(
        &mut sub,
        |e| matches!(e, Event::MessageCancelled { run_id, .. } if run_id.to_string() == ha.run_id),
    )
    .await;
    assert_eq!(run_id(&cancelled_a).as_deref(), Some(ha.run_id.as_str()));

    // No stray cancelled event for B.
    let mut b_cancelled = false;
    for _ in 0..50 {
        if let Ok(Ok(env)) = timeout(Duration::from_millis(100), sub.recv()).await {
            if matches!(env.event, Event::MessageCancelled { run_id, .. } if run_id.to_string() == hb.run_id)
            {
                b_cancelled = true;
                break;
            }
        } else {
            break;
        }
    }
    assert!(!b_cancelled, "cancel A must never touch B");
}

/// Same-workspace serialization (§21–§22): a second send into the same
/// workspace is rejected with the typed WorkspaceBusy error, not queued or
/// silently run.
#[tokio::test]
async fn same_workspace_second_send_rejected_typed() {
    let h = harness().await;
    let a = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();
    let b = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();

    h.core
        .sessions
        .send(&a.id, "/sim:hang", None)
        .await
        .unwrap();
    let err = h
        .core
        .sessions
        .send(&b.id, "/sim:normal", None)
        .await
        .expect_err("same-workspace send must be rejected");
    assert!(
        matches!(err, CoreError::WorkspaceBusy { .. }),
        "typed WorkspaceBusy, got {err:?}"
    );
}

/// After the active same-workspace run reaches terminal, the next send is
/// accepted (no permanent lock).
#[tokio::test]
async fn same_workspace_serializes_then_allows_next() {
    let h = harness().await;
    let mut sub = h.core.bus.subscribe();
    let a = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();
    let b = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();

    let ha = h
        .core
        .sessions
        .send(&a.id, "/sim:normal", None)
        .await
        .unwrap();
    wait_for(
        &mut sub,
        |e| matches!(e, Event::MessageCompleted { run_id, .. } if run_id.to_string() == ha.run_id),
    )
    .await;

    let hb = h
        .core
        .sessions
        .send(&b.id, "/sim:normal", None)
        .await
        .expect("send allowed after workspace run finished");
    assert_ne!(ha.run_id, hb.run_id);
    wait_for(
        &mut sub,
        |e| matches!(e, Event::MessageCompleted { run_id, .. } if run_id.to_string() == hb.run_id),
    )
    .await;
}

/// The queue-facing gate (`session_busy` through the EnginePort) honors the
/// same-workspace rule: the queue waits when another session in the same
/// workspace is running, and proceeds for a different workspace.
#[tokio::test]
async fn queue_port_busy_respects_workspace_boundary() {
    let h = harness().await;
    let port = QueueEnginePort::new(h.core.engines.clone(), h.core.sessions.clone());

    let a = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();
    let b_same = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w1), None)
        .await
        .unwrap();
    let c_other = h
        .core
        .sessions
        .create(&h.engine_id, Some(&h.w2), None)
        .await
        .unwrap();

    // A active in w1.
    let ha = h
        .core
        .sessions
        .send(&a.id, "/sim:hang", None)
        .await
        .unwrap();

    // Same workspace → busy (queue waits, never claims).
    assert!(port.session_busy(&b_same.id), "same-workspace session busy");
    // Different workspace → free.
    assert!(
        !port.session_busy(&c_other.id),
        "other-workspace session free"
    );
    // The running session itself → busy.
    assert!(port.session_busy(&a.id), "running session busy");

    h.core.sessions.cancel(&a.id, &ha.run_id).await.unwrap();
    // Give the cancel terminal a moment to settle.
    sleep(Duration::from_millis(150)).await;
    assert!(
        !port.session_busy(&b_same.id),
        "workspace released after run ends"
    );
}
