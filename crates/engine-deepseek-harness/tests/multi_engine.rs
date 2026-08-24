//! TASK 24 — POST-V1 multi-engine hostile matrix (TASK 24 §9, §14, §16–§18,
//! §20–§21, §35, §120). Uses the REAL production wiring — `EngineRegistry` +
//! `SessionManager` + `QueueEnginePort` + `QueueManager` — with **two engines
//! registered simultaneously**: the in-process `FakeEngine` and the
//! `HarnessAdapter` (deterministic fake ACP server as a stdio process).
//!
//! Proves: cross-engine session/run isolation; cross-engine queue routing to
//! the exact stored EngineId; queue target immutability (engine selection
//! changes never retarget queued work); one-engine failure isolation; the
//! same-workspace cross-engine serialization law; and the fail-closed
//! session-id collision guard (§9/§120) via hostile adapters returning the
//! same session id.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use engine_deepseek_harness::{HarnessAdapter, HarnessConfig};
use engine_fake::FakeEngine;
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineCapabilities, EngineError, EngineHealth,
    EngineIdentity, EngineRegistry, EngineStartContext, ModelInfo, SendAcceptance, SendRequest,
    SessionCreation, SessionInfo,
};
use saiwork_core::error::CoreError;
use saiwork_core::queue_port::QueueEnginePort;
use saiwork_core::sessions::SessionManager;
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{bus::Subscription, Event, EventBus};
use saiwork_process::ProcessSupervisor;
use saiwork_queue::model::{EnqueueRequest, QueueState, SessionMode};
use saiwork_queue::{QueueManager, QueueRepo};
use saiwork_storage::Db;
use tempfile::TempDir;

const FIXTURE: &str = env!("CARGO_BIN_EXE_fake-harness");
const ENGINE: &str = "deepseek-harness";
const FAKE: &str = "fake";

struct Multi {
    sub: Subscription,
    queue: Arc<QueueManager>,
    repo: QueueRepo,
    fake: Arc<FakeEngine>,
    harness: Arc<HarnessAdapter>,
    sessions: Arc<SessionManager>,
    _tmp: TempDir,
}

async fn new_multi() -> Multi {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = HarnessConfig {
        executable: Some(PathBuf::from(FIXTURE)),
        cwd: Some(tmp.path().to_path_buf()),
        handshake_timeout: Duration::from_secs(3),
        stop_grace: Duration::from_secs(1),
        stop_force: Duration::from_secs(1),
        prompt_timeout: Duration::from_secs(30),
        args: vec!["agent-normal".into()],
        ..HarnessConfig::default()
    };
    let bus = EventBus::new();
    let sub = bus.subscribe();
    let db = Db::open_in_memory().unwrap();
    // AUDIT-W2-003: enqueue verifies workspace existence in-tx; seed every
    // workspace id the tests enqueue against.
    for wid in ["w1", "w2", "w3"] {
        let path = format!("file:///{wid}");
        db.with_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO workspaces (id, path, name, last_opened_at, created_at, updated_at)
                 VALUES (?1, ?2, ?2, 0, 0, 0)",
                [&wid, &path.as_str()],
            )
            .map(|_| ())
            .map_err(saiwork_storage::StorageError::Query)
        })
        .unwrap();
    }
    let diagnostics = Arc::new(Diagnostics::new());
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let engines = Arc::new(EngineRegistry::new(
        bus.clone(),
        diagnostics.clone(),
        supervisor.clone(),
    ));
    let fake = Arc::new(FakeEngine::new());
    let harness = Arc::new(HarnessAdapter::new(cfg));
    engines.register(fake.clone());
    engines.register(harness.clone());

    let bus2 = bus.clone();
    let ctx = EngineStartContext {
        workspace_id: None,
        workspace_path: Some(tmp.path().to_path_buf()),
        bus: bus.clone(),
        diagnostics,
        supervisor,
        report_failure: Arc::new(move |engine_id: &str, message: &str| {
            bus2.publish(Event::EngineFailed {
                engine_id: engine_id.into(),
                error: message.into(),
            });
        }),
    };
    fake.start(&ctx).await.expect("fake start");
    harness.start(&ctx).await.expect("harness start");
    assert_eq!(fake.health(), EngineHealth::Ready);
    assert_eq!(harness.health(), EngineHealth::Ready);

    let sessions = Arc::new(SessionManager::new(
        db.clone(),
        bus.clone(),
        engines.clone(),
    ));
    let port = Arc::new(QueueEnginePort::new(engines.clone(), sessions.clone()));
    let queue = QueueManager::new(db.clone(), bus.clone(), port.clone());
    queue.init().unwrap();
    let repo = QueueRepo::new(db.clone());
    Multi {
        sub,
        queue,
        repo,
        fake,
        harness,
        sessions,
        _tmp: tmp,
    }
}

fn state(m: &Multi, id: &str) -> QueueState {
    m.repo.get(id).unwrap().unwrap().state
}

async fn wait_state(m: &Multi, id: &str, target: QueueState, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if state(m, id) == target {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {id} to reach {target:?}; current {:?} (error {:?})",
            state(m, id),
            m.repo.get(id).unwrap().unwrap().last_error
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn enqueue(m: &Multi, engine_id: &str, workspace: &str, payload: &str) -> saiwork_queue::QueueItem {
    m.queue
        .enqueue(EnqueueRequest {
            workspace_id: workspace.into(),
            engine_id: engine_id.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: payload.into(),
        })
        .unwrap()
}

async fn wait_completed(m: &mut Multi, session_id: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(env) = tokio::time::timeout(Duration::from_millis(200), m.sub.recv()).await {
            if let Event::MessageCompleted { session_id: s, .. } = env.unwrap().event {
                if s.as_str() == session_id {
                    return;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for MessageCompleted on {session_id}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cross-engine session + run isolation (TASK 24 §9, §12, §14)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_engine_session_and_run_isolation() {
    let mut m = new_multi().await;

    let fake_sess = m.sessions.create(FAKE, Some("w1"), None).await.unwrap();
    let harness_sess = m.sessions.create(ENGINE, Some("w2"), None).await.unwrap();
    assert_ne!(fake_sess.engine_id, harness_sess.engine_id);

    // Send to the FakeEngine session; it must complete on its own session id.
    let fake_handle = m
        .sessions
        .send(&fake_sess.id, "/sim:normal hello", None)
        .await
        .unwrap();
    wait_completed(&mut m, &fake_sess.id, Duration::from_secs(15)).await;

    // Now the Harness session in a *different* workspace (w2) — cross-engine
    // runs are independent; the workspace gate only serializes same-workspace.
    let harness_handle = m
        .sessions
        .send(&harness_sess.id, "hello harness", None)
        .await
        .unwrap();
    wait_completed(&mut m, &harness_sess.id, Duration::from_secs(15)).await;

    assert_ne!(fake_handle.run_id, harness_handle.run_id);
    // Each engine's run is tracked under its own run id.
    assert!(m.fake.active_runs() == 0, "fake run finished");
    assert!(
        matches!(m.harness.health(), EngineHealth::Ready),
        "harness still ready"
    );

    m.queue.shutdown_barrier();
    m.queue.finish_shutdown().await;
    m.fake.stop().await.unwrap();
    m.harness.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// Cross-engine queue routing: each item dispatches to its exact stored engine
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_routes_mixed_targets_exactly() {
    let m = new_multi().await;

    // Different workspaces: same-workspace serialization is covered by its
    // own test; here the two items must both reach DONE via exact routing.
    let fake_item = enqueue(&m, FAKE, "w1", "/sim:normal fake task");
    let harness_item = enqueue(&m, ENGINE, "w2", "harness task");

    wait_state(&m, &fake_item.id, QueueState::Done, Duration::from_secs(15)).await;
    wait_state(
        &m,
        &harness_item.id,
        QueueState::Done,
        Duration::from_secs(15),
    )
    .await;

    let fake_done = m.repo.get(&fake_item.id).unwrap().unwrap();
    let harness_done = m.repo.get(&harness_item.id).unwrap().unwrap();
    // Exact target retention: engine_id frozen per item, never rewritten.
    assert_eq!(fake_done.engine_id, FAKE);
    assert_eq!(harness_done.engine_id, ENGINE);
    // Each item's session belongs to its engine (no cross-engine session reuse).
    let fake_sid = fake_done.session_id.clone().unwrap();
    let harness_sid = harness_done.session_id.clone().unwrap();
    let fake_sess = m.sessions.get(&fake_sid).unwrap();
    let harness_sess = m.sessions.get(&harness_sid).unwrap();
    assert_eq!(fake_sess.engine_id, FAKE);
    assert_eq!(harness_sess.engine_id, ENGINE);
    assert_ne!(fake_sid, harness_sid);

    m.queue.shutdown_barrier();
    m.queue.finish_shutdown().await;
    m.fake.stop().await.unwrap();
    m.harness.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// Queue target immutability: changing the "selected" engine cannot retarget
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_target_immutable_after_selection_change() {
    let m = new_multi().await;

    // Enqueue a Harness item, then "select" the FakeEngine (create a session
    // there + dispatch a fake run) — the queued Harness item must still route
    // to Harness, exactly as stored (§21).
    let harness_item = enqueue(&m, ENGINE, "w1", "harness task");
    let fake_item = enqueue(&m, FAKE, "w2", "/sim:normal fake task");

    wait_state(&m, &fake_item.id, QueueState::Done, Duration::from_secs(15)).await;
    wait_state(
        &m,
        &harness_item.id,
        QueueState::Done,
        Duration::from_secs(15),
    )
    .await;

    let harness_done = m.repo.get(&harness_item.id).unwrap().unwrap();
    assert_eq!(
        harness_done.engine_id, ENGINE,
        "selection change must not retarget"
    );
    let harness_sess = m.sessions.get(&harness_done.session_id.unwrap()).unwrap();
    assert_eq!(harness_sess.engine_id, ENGINE);

    m.queue.shutdown_barrier();
    m.queue.finish_shutdown().await;
    m.fake.stop().await.unwrap();
    m.harness.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// One-engine failure isolation (TASK 24 §35): Harness crash must not break an
// active FakeEngine run, and vice-versa the fake keeps running.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_crash_isolated_from_other_engine() {
    let m = new_multi().await;

    // Start a long (hang) FakeEngine run in w1.
    let fake_sess = m.sessions.create(FAKE, Some("w1"), None).await.unwrap();
    let _fake_handle = m
        .sessions
        .send(&fake_sess.id, "/sim:hang", None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(m.fake.active_runs(), 1);

    // Take the Harness runtime down while the fake run is live. (kill() is an
    // app-initiated teardown → Stopped; a spontaneous crash would report
    // Failed. Either way the engine is gone and must not affect the fake.)
    m.harness.kill().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        m.harness.health() != EngineHealth::Ready,
        "harness must be down"
    );

    // The FakeEngine run is untouched and still completes.
    assert_eq!(
        m.fake.active_runs(),
        1,
        "fake run unaffected by harness loss"
    );
    m.sessions
        .cancel(&fake_sess.id, &_fake_handle.run_id)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(m.fake.active_runs(), 0, "fake run settled after cancel");

    // A queued Harness item must NOT fall back to FakeEngine (§8): it stays
    // pending (engine unavailable), never dispatched, never auto-failed.
    let harness_item = enqueue(&m, ENGINE, "w1", "harness task");
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let h = m.repo.get(&harness_item.id).unwrap().unwrap();
    assert!(
        !matches!(
            h.state,
            QueueState::Done | QueueState::Failed | QueueState::Cancelled
        ),
        "harness item must not dispatch through another engine (no fallback)"
    );
    assert_eq!(h.session_id, None, "harness item never dispatched");

    // FakeEngine queued work in another workspace still dispatches fine.
    let fake_item = enqueue(&m, FAKE, "w3", "/sim:normal fake task");
    wait_state(&m, &fake_item.id, QueueState::Done, Duration::from_secs(15)).await;

    m.queue.shutdown_barrier();
    m.queue.finish_shutdown().await;
    m.fake.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// Same-workspace cross-engine serialization (TASK 24 §18): an active FakeEngine
// run in workspace w1 must block a Harness run in the SAME workspace, even
// though the engines differ — filesystem side effects are not engine-scoped.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_workspace_cross_engine_serialized() {
    let mut m = new_multi().await;

    let fake_sess = m.sessions.create(FAKE, Some("w1"), None).await.unwrap();
    let _fake_handle = m
        .sessions
        .send(&fake_sess.id, "/sim:hang", None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A Harness session in the SAME workspace must be rejected while the fake
    // run is active (WorkspaceBusy) — engine difference does not matter.
    let harness_sess = m.sessions.create(ENGINE, Some("w1"), None).await.unwrap();
    let err = m
        .sessions
        .send(&harness_sess.id, "hello harness", None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CoreError::WorkspaceBusy { .. }),
        "cross-engine same-workspace send must be serialized, got {err:?}"
    );

    // A DIFFERENT workspace (w2) is not blocked.
    let harness_sess2 = m.sessions.create(ENGINE, Some("w2"), None).await.unwrap();
    let _h2 = m
        .sessions
        .send(&harness_sess2.id, "hello harness", None)
        .await
        .unwrap();
    wait_completed(&mut m, &harness_sess2.id, Duration::from_secs(15)).await;

    m.sessions
        .cancel(&fake_sess.id, &_fake_handle.run_id)
        .await
        .unwrap();
    m.queue.shutdown_barrier();
    m.queue.finish_shutdown().await;
    m.fake.stop().await.unwrap();
    m.harness.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// Engine-independent session ids (TASK 24 §9): two engines may return the
// SAME upstream session id; generic SAIWORK2 ids are minted by the manager,
// so both sessions survive independently — even across a simulated restart —
// and the generic namespace can never collide.
// ---------------------------------------------------------------------------

/// Minimal hostile adapter that returns a fixed upstream session id on create.
struct HostileAdapter {
    id: String,
    engine_id: String,
}

impl HostileAdapter {
    fn new(engine_id: &str, session_id: &str) -> Self {
        Self {
            id: session_id.into(),
            engine_id: engine_id.into(),
        }
    }
}

#[async_trait]
impl EngineAdapter for HostileAdapter {
    fn identity(&self) -> EngineIdentity {
        EngineIdentity {
            id: self.engine_id.clone(),
            display_name: self.engine_id.clone(),
            version: "0".into(),
            experimental: false,
        }
    }
    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            sessions: true,
            ..Default::default()
        }
    }
    async fn start(&self, _ctx: &EngineStartContext) -> Result<(), EngineError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), EngineError> {
        Ok(())
    }
    async fn kill(&self) -> Result<(), EngineError> {
        Ok(())
    }
    fn health(&self) -> EngineHealth {
        EngineHealth::Ready
    }
    async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
        Ok(Vec::new())
    }
    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
        Ok(Vec::new())
    }
    async fn create_session(
        &self,
        _req: &CreateSessionRequest,
    ) -> Result<SessionCreation, EngineError> {
        // Echo the generic id; keep the hostile fixed id as the upstream
        // engine session id.
        Ok(SessionCreation::Created {
            engine_session_id: self.id.clone(),
            display_name: "hostile".into(),
        })
    }
    async fn resume_session(&self, _id: &str) -> Result<SessionInfo, EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: "hostile".into(),
            capability: "resume",
        })
    }
    async fn delete_session(&self, _id: &str) -> Result<(), EngineError> {
        Ok(())
    }
    async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: "hostile".into(),
            capability: "send",
        })
    }
    async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_upstream_ids_survive_as_independent_generic_sessions() {
    let bus = EventBus::new();
    let db = Db::open_in_memory().unwrap();
    let diagnostics = Arc::new(Diagnostics::new());
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let engines = Arc::new(EngineRegistry::new(bus.clone(), diagnostics, supervisor));
    let a = Arc::new(HostileAdapter::new("engine-a", "1"));
    let b = Arc::new(HostileAdapter::new("engine-b", "1"));
    engines.register(a.clone());
    engines.register(b.clone());

    let sessions = Arc::new(SessionManager::new(
        db.clone(),
        bus.clone(),
        engines.clone(),
    ));

    // Both engines return the same raw upstream id "1"; the generic ids
    // must differ and both sessions must survive.
    let first = sessions.create("engine-a", None, None).await.unwrap();
    let second = sessions.create("engine-b", None, None).await.unwrap();
    assert_ne!(first.id, second.id, "generic ids are engine-independent");
    assert_eq!(first.engine_session_id, "1");
    assert_eq!(second.engine_session_id, "1");
    assert_eq!(sessions.list(None).unwrap().len(), 2);

    // Simulated restart: a fresh manager over the same durable DB restores
    // both sessions with their engine/upstream association intact.
    let restored = SessionManager::new(db.clone(), bus.clone(), engines.clone());
    let listed = restored.list(None).unwrap();
    assert_eq!(listed.len(), 2, "persisted sessions_meta survive restart");
    let a_row = listed.iter().find(|s| s.engine_id == "engine-a").unwrap();
    let b_row = listed.iter().find(|s| s.engine_id == "engine-b").unwrap();
    assert_eq!(a_row.engine_session_id, "1");
    assert_eq!(b_row.engine_session_id, "1");
    assert_ne!(a_row.id, b_row.id);
    // Routing facts are exact: each generic session maps to exactly its own
    // engine + upstream id.
    let loaded_a = restored.get(&a_row.id).unwrap();
    assert_eq!(loaded_a.engine_id, "engine-a");
    assert_eq!(loaded_a.engine_session_id, "1");
    let loaded_b = restored.get(&b_row.id).unwrap();
    assert_eq!(loaded_b.engine_id, "engine-b");
    assert_eq!(loaded_b.engine_session_id, "1");
}
