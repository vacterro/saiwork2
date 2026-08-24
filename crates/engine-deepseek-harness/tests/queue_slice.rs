//! TASK 23 — DeepSeek Harness as a durable QueueManager target (TASK 23 §43–§44,
//! §191–§192). Uses the REAL production wiring — `EngineRegistry` +
//! `SessionManager` + `QueueEnginePort` + `QueueManager` — over the real
//! `HarnessAdapter` (deterministic fake ACP server as a stdio process). The
//! queue never knows ACP/Harness protocol details: it dispatches through the
//! generic `EnginePort` and consumes generic `message.*` terminal events.
//!
//! Proves: enqueue → claim → durable sending phase → Harness `send` → run →
//! `message.completed` → durable DONE; cancellation via Harness run cancel →
//! CANCELLED; provider failure → FAILED with the engine still READY; engine
//! crash → FAILED with no auto-requeue. No direct queue DB write from the
//! adapter, no ACP call from the queue.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use engine_deepseek_harness::{HarnessAdapter, HarnessConfig};
use saiwork_core::engine::{EngineAdapter, EngineHealth, EngineRegistry, EngineStartContext};
use saiwork_core::queue_port::QueueEnginePort;
use saiwork_core::sessions::SessionManager;
use saiwork_diagnostics::Diagnostics;
use saiwork_events::EventBus;
use saiwork_process::ProcessSupervisor;
use saiwork_queue::model::{EnqueueRequest, QueueState, SessionMode};
use saiwork_queue::{QueueManager, QueueRepo};
use saiwork_storage::Db;
use tempfile::TempDir;

const FIXTURE: &str = env!("CARGO_BIN_EXE_fake-harness");
const ENGINE: &str = "deepseek-harness";

struct QueueSlice {
    queue: Arc<QueueManager>,
    repo: QueueRepo,
    adapter: Arc<HarnessAdapter>,
    sessions: Arc<SessionManager>,
    _tmp: TempDir,
}

async fn new_slice(scenario: &str) -> QueueSlice {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = HarnessConfig {
        executable: Some(PathBuf::from(FIXTURE)),
        cwd: Some(tmp.path().to_path_buf()),
        handshake_timeout: Duration::from_secs(3),
        stop_grace: Duration::from_secs(1),
        stop_force: Duration::from_secs(1),
        prompt_timeout: Duration::from_secs(30),
        args: vec![scenario.into()],
        ..HarnessConfig::default()
    };
    let bus = EventBus::new();
    let db = Db::open_in_memory().unwrap();
    // AUDIT-W2-003: enqueue verifies workspace existence in-tx.
    db.with_conn(|conn| {
        conn.execute(
            "INSERT OR IGNORE INTO workspaces (id, path, name, last_opened_at, created_at, updated_at)
             VALUES ('w1', 'file:///w1', 'w1', 0, 0, 0)",
            [],
        )
        .map(|_| ())
        .map_err(saiwork_storage::StorageError::Query)
    })
    .unwrap();
    let diagnostics = Arc::new(Diagnostics::new());
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let engines = Arc::new(EngineRegistry::new(
        bus.clone(),
        diagnostics.clone(),
        supervisor.clone(),
    ));
    let adapter = Arc::new(HarnessAdapter::new(cfg));
    engines.register(adapter.clone());
    let bus2 = bus.clone();
    let ctx = EngineStartContext {
        workspace_id: None,
        workspace_path: Some(tmp.path().to_path_buf()),
        bus: bus.clone(),
        diagnostics,
        supervisor,
        report_failure: Arc::new(move |engine_id: &str, message: &str| {
            bus2.publish(saiwork_events::Event::EngineFailed {
                engine_id: engine_id.into(),
                error: message.into(),
            });
        }),
    };
    adapter.start(&ctx).await.expect("harness start");
    assert_eq!(adapter.health(), EngineHealth::Ready);

    let sessions = Arc::new(SessionManager::new(
        db.clone(),
        bus.clone(),
        engines.clone(),
    ));
    let port = Arc::new(QueueEnginePort::new(engines.clone(), sessions.clone()));
    let queue = QueueManager::new(db.clone(), bus.clone(), port.clone());
    queue.init().unwrap();
    let repo = QueueRepo::new(db.clone());
    QueueSlice {
        queue,
        repo,
        adapter,
        sessions,
        _tmp: tmp,
    }
}

fn state(s: &QueueSlice, id: &str) -> QueueState {
    s.repo.get(id).unwrap().unwrap().state
}

async fn wait_state(s: &QueueSlice, id: &str, target: QueueState, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if state(s, id) == target {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {id} to reach {target:?}; current {:?} (error {:?})",
            state(s, id),
            s.repo.get(id).unwrap().unwrap().last_error
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn enqueue(s: &QueueSlice, payload: &str) -> saiwork_queue::QueueItem {
    s.queue
        .enqueue(EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: payload.into(),
        })
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_queued_item_dispatches_once_and_done() {
    // TASK 23 §209: Harness is a real QueueManager target through the generic
    // EngineAdapter; the run completes and the item reaches durable DONE.
    let s = new_slice("agent-normal").await;
    let item = enqueue(&s, "hello harness");
    wait_state(&s, &item.id, QueueState::Done, Duration::from_secs(15)).await;
    let done = s.repo.get(&item.id).unwrap().unwrap();
    assert_eq!(done.attempt_count, 1);
    assert!(done.run_id.is_some(), "run correlation persisted");
    assert!(
        done.session_id.is_some(),
        "harness session correlation persisted"
    );
    s.queue.shutdown_barrier();
    s.queue.finish_shutdown().await;
    s.adapter.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_existing_live_session_is_queue_targetable_despite_non_resumable() {
    // AUDIT-W2-001: a LIVE connection-owned (resume=false) Harness session is
    // a valid EXISTING queue target while its creating runtime generation is
    // alive — the same usability direct send accepts. The old unconditional
    // resumability gate rejected the enqueue before persistence with a
    // misleading non-resumable error.
    let s = new_slice("agent-normal").await;
    let session = s.sessions.create(ENGINE, Some("w1"), None).await.unwrap();
    assert!(!session.resumable, "harness sessions are connection-owned");

    let item = s
        .queue
        .enqueue(EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: Some(session.id.clone()),
            session_mode: SessionMode::Existing,
            model: None,
            payload: "existing-session prompt".into(),
        })
        .expect("live non-resumable session must be enqueueable");
    wait_state(&s, &item.id, QueueState::Done, Duration::from_secs(15)).await;
    let done = s.repo.get(&item.id).unwrap().unwrap();
    assert_eq!(done.session_id.as_deref(), Some(session.id.as_str()));
    s.queue.shutdown_barrier();
    s.queue.finish_shutdown().await;
    s.adapter.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_queued_item_cancel_marks_cancelled() {
    // TASK 23 §39–§40: queue cancel maps to Harness run cancellation (never
    // an engine kill) and the item reaches durable CANCELLED.
    let s = new_slice("agent-cancel").await;
    let item = enqueue(&s, "cancel me");
    wait_state(
        &s,
        &item.id,
        QueueState::Dispatched,
        Duration::from_secs(15),
    )
    .await;
    s.queue.cancel(&item.id).await.unwrap();
    wait_state(&s, &item.id, QueueState::Cancelled, Duration::from_secs(15)).await;
    assert!(
        matches!(s.adapter.health(), EngineHealth::Ready),
        "cancel must not kill the Harness runtime"
    );
    s.queue.shutdown_barrier();
    s.queue.finish_shutdown().await;
    s.adapter.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_provider_failure_fails_item_engine_stays_ready() {
    // TASK 23 §36: a provider/model failure fails the queue item per the run
    // result; the Harness runtime stays READY (no engine restart by default).
    let s = new_slice("agent-provider-fail").await;
    let item = enqueue(&s, "provider fail");
    wait_state(&s, &item.id, QueueState::Failed, Duration::from_secs(15)).await;
    let failed = s.repo.get(&item.id).unwrap().unwrap();
    assert!(matches!(
        failed.last_error_code.as_deref(),
        Some("run_failed")
    ));
    assert_eq!(s.adapter.health(), EngineHealth::Ready);
    s.queue.shutdown_barrier();
    s.queue.finish_shutdown().await;
    s.adapter.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_crash_fails_item_no_requeue() {
    // TASK 23 §52 / T-008 (TASK 24 §9): a Harness crash during an accepted
    // queued run cannot prove the run's outcome, so the item becomes UNKNOWN
    // (non-releasing, `engine_lost`) with its run correlation retained — the
    // workspace stays blocked until a matching terminal or proven death. It
    // must NEVER auto-requeue and must NEVER fabricate a definitive FAILED
    // that would release the workspace while the external agent may still
    // mutate files.
    let s = new_slice("agent-crash").await;
    let item = enqueue(&s, "crash me");
    wait_state(&s, &item.id, QueueState::Unknown, Duration::from_secs(15)).await;
    let failed = s.repo.get(&item.id).unwrap().unwrap();
    assert!(
        matches!(
            failed.last_error_code.as_deref(),
            Some("outcome_unknown") | Some("engine_lost")
        ),
        "crash settles the item as unknown (outcome unprovable), never a definitive failure; got {:?}",
        failed.last_error_code
    );
    assert!(
        failed.run_id.is_some(),
        "run correlation must be retained for later reconciliation"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        s.repo.get(&item.id).unwrap().unwrap().state,
        QueueState::Unknown,
        "unknown item must never auto-requeue"
    );
    assert_eq!(s.adapter.active_runs(), 0, "no eternal run after crash");
    s.queue.shutdown_barrier();
    s.queue.finish_shutdown().await;
}
