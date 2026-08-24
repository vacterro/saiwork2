//! QueueManager integration tests with FakeEngine (TASK 13 dispatch gate,
//! crash matrix, concurrency matrix). Feature-gated: the failpoint hooks used
//! here are unreachable in production builds (§85, §230).
#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use engine_fake::FakeEngine;
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineHealth, EngineStartContext, SendRequest,
};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::EventBus;
use saiwork_process::ProcessSupervisor;
use saiwork_queue::manager::DispatchHooks;
use saiwork_queue::model::{EnqueueRequest, QueueError, QueueState, QueueStatus, SessionMode};
use saiwork_queue::port::{DispatchReceipt, SessionCreateOutcome};
use saiwork_queue::repo::RepoFailpoints;
use saiwork_queue::{
    EnginePort, EngineState, PortError, QueueItem, QueueManager, DISPATCH_CANDIDATE_PAGE_SIZE,
};
use saiwork_storage::Db;

const ENGINE: &str = "fake";

// ---- test engine port over FakeEngine ----

struct FakePort {
    engine: Arc<FakeEngine>,
    sessions: Mutex<HashMap<String, String>>,
    sends: AtomicUsize,
    sent_payloads: Mutex<Vec<String>>,
    busy: AtomicUsize,
    /// Remaining simulated transient `create_session` failures (Network).
    /// `usize::MAX` = fail forever. The session is NOT created on a failure.
    create_failures: AtomicUsize,
    /// Total `create_session` attempts (spin vs bounded-backoff assertion).
    create_attempts: AtomicUsize,
    /// Remaining simulated `delete_session` (compensation) failures.
    delete_failures: AtomicUsize,
    /// Total `delete_session` calls (compensation assertions).
    delete_count: AtomicUsize,
    /// Total `cancel` calls (TASK 24 §9 cancel fail-closed assertions).
    cancel_count: AtomicUsize,
    /// Simulated runtime workspace binding (TASK 24 §9): when `Some(bound)`,
    /// items targeting a different workspace are NotReady/Wait — the engine
    /// cannot serve that project until explicitly restarted for it.
    bound_workspace: Mutex<Option<String>>,
}

impl FakePort {
    fn new(engine: Arc<FakeEngine>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            sessions: Mutex::new(HashMap::new()),
            sends: AtomicUsize::new(0),
            sent_payloads: Mutex::new(Vec::new()),
            busy: AtomicUsize::new(0),
            create_failures: AtomicUsize::new(0),
            create_attempts: AtomicUsize::new(0),
            delete_failures: AtomicUsize::new(0),
            delete_count: AtomicUsize::new(0),
            cancel_count: AtomicUsize::new(0),
            bound_workspace: Mutex::new(None),
        })
    }

    fn set_bound_workspace(&self, bound: Option<String>) {
        *self.bound_workspace.lock().unwrap() = bound;
    }

    fn send_count(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }

    fn sent_payloads(&self) -> Vec<String> {
        self.sent_payloads.lock().unwrap().clone()
    }

    fn set_busy(&self, busy: bool) {
        self.busy.store(busy as usize, Ordering::SeqCst);
    }

    fn set_create_failures(&self, n: usize) {
        self.create_failures.store(n, Ordering::SeqCst);
    }

    fn create_attempt_count(&self) -> usize {
        self.create_attempts.load(Ordering::SeqCst)
    }

    fn set_delete_failures(&self, n: usize) {
        self.delete_failures.store(n, Ordering::SeqCst);
    }

    fn delete_count(&self) -> usize {
        self.delete_count.load(Ordering::SeqCst)
    }

    fn cancel_count(&self) -> usize {
        self.cancel_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EnginePort for FakePort {
    fn engine_state(&self, engine_id: &str) -> EngineState {
        if engine_id != ENGINE {
            return EngineState::NotReady;
        }
        match self.engine.health() {
            EngineHealth::Ready => EngineState::Ready,
            EngineHealth::Failed { .. } => EngineState::Failed,
            _ => EngineState::NotReady,
        }
    }

    fn engine_state_for_workspace(&self, engine_id: &str, workspace_id: &str) -> EngineState {
        // A healthy engine bound to a DIFFERENT workspace is NotReady for this
        // item (Wait, never FAILED/auto-rebind — TASK 24 §9).
        if let Some(bound) = self.bound_workspace.lock().unwrap().clone() {
            if bound != workspace_id {
                return EngineState::NotReady;
            }
        }
        self.engine_state(engine_id)
    }

    fn session_exists(&self, session_id: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(session_id)
    }

    fn session_busy(&self, _session_id: &str) -> bool {
        self.busy.load(Ordering::SeqCst) == 1
    }

    async fn ensure_session(&self, session_id: &str) -> Result<(), PortError> {
        if self.sessions.lock().unwrap().contains_key(session_id) {
            Ok(())
        } else {
            Err(PortError::SessionNotFound(session_id.into()))
        }
    }

    async fn create_session(
        &self,
        _engine_id: &str,
        workspace_id: Option<&str>,
        model: Option<&str>,
    ) -> Result<SessionCreateOutcome, PortError> {
        self.create_attempts.fetch_add(1, Ordering::SeqCst);
        if self.create_failures.load(Ordering::SeqCst) > 0 {
            // Transient dependency failure: nothing is created, nothing is
            // mutated (the queue must back off, not spin).
            self.create_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(PortError::Network("simulated network failure".into()));
        }
        let generic_id = uuid::Uuid::new_v4().to_string();
        let info = match self
            .engine
            .create_session(&CreateSessionRequest {
                session_id: generic_id.clone(),
                workspace_id: workspace_id.map(String::from),
                model: model.map(String::from),
                title: None,
            })
            .await
            .map_err(|e| PortError::Provider(e.to_string()))?
        {
            saiwork_core::engine::SessionCreation::Created {
                engine_session_id,
                ..
            } => (engine_session_id, generic_id.clone()),
            other => panic!("test fake create must be Created, got {other:?}"),
        };
        self.sessions
            .lock()
            .unwrap()
            .insert(info.1.clone(), info.0);
        Ok(SessionCreateOutcome::Created {
            session_id: info.1,
        })
    }

    async fn send(
        &self,
        session_id: &str,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<DispatchReceipt, PortError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.sent_payloads.lock().unwrap().push(prompt.to_string());
        // Generic → upstream id split (TASK 24 §9): the adapter call uses the
        // upstream engine session id registered at creation.
        let engine_session_id = self
            .sessions
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
            .unwrap_or_else(|| session_id.to_string());
        match self
            .engine
            .send(&SendRequest {
                session_id: session_id.into(),
                engine_session_id: engine_session_id.into(),
                prompt: prompt.into(),
                model: model.map(String::from),
            })
            .await
            .map_err(|e| PortError::Provider(e.to_string()))?
        {
            saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
                Ok(DispatchReceipt::Accepted { run_id })
            }
            other => panic!("test fake send must be Accepted, got {other:?}"),
        }
    }

    async fn cancel(&self, session_id: &str, run_id: &str) -> Result<(), PortError> {
        let _ = session_id;
        self.cancel_count.fetch_add(1, Ordering::SeqCst);
        self.engine
            .cancel(run_id)
            .await
            .map_err(|e| PortError::Provider(e.to_string()))
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), PortError> {
        self.delete_count.fetch_add(1, Ordering::SeqCst);
        if self.delete_failures.load(Ordering::SeqCst) > 0 {
            self.delete_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(PortError::Internal("simulated cleanup failure".into()));
        }
        // Mirrors the core bridge: authoritative upstream delete + local map
        // removal. Engine errors (incl. unsupported) map to PortError.
        let engine_session_id = self
            .sessions
            .lock()
            .unwrap()
            .remove(session_id)
            .unwrap_or_else(|| session_id.to_string());
        self.engine
            .delete_session(&engine_session_id)
            .await
            .map_err(|e| PortError::Provider(e.to_string()))
    }
}

// ---- harness ----

struct Harness {
    bus: EventBus,
    db: Db,
    engine: Arc<FakeEngine>,
    port: Arc<FakePort>,
}

impl Harness {
    async fn new() -> Self {
        let bus = EventBus::new();
        let db = Db::open_in_memory().unwrap();
        // AUDIT-W2-003: enqueue now requires the referenced workspace row
        // (checked inside the insert transaction). Hostile cases below use
        // w2 and A/B to prove cross-workspace isolation/binding.
        db.with_conn(|conn| {
            for (id, path) in [
                ("w1", "file:///w1"),
                ("w2", "file:///w2"),
                ("A", "file:///A"),
                ("B", "file:///B"),
            ] {
                conn.execute(
                    "INSERT OR IGNORE INTO workspaces \
                     (id, path, name, last_opened_at, created_at, updated_at) \
                     VALUES (?1, ?2, ?1, 0, 0, 0)",
                    rusqlite::params![id, path],
                )
                .map_err(saiwork_storage::StorageError::Query)?;
            }
            Ok(())
        })
        .unwrap();
        let engine = Arc::new(FakeEngine::new());
        start_engine(&bus, &engine).await;
        let port = FakePort::new(engine.clone());
        Self {
            bus,
            db,
            engine,
            port,
        }
    }

    /// Fresh engine (restart simulation): same bus, engine NOT started.
    fn restarted_engine(&self) -> Arc<FakeEngine> {
        Arc::new(FakeEngine::new())
    }

    fn manager(
        &self,
        engine: Option<Arc<FakeEngine>>,
        hooks: Option<DispatchHooks>,
    ) -> (Arc<QueueManager>, Arc<FakePort>) {
        let engine = engine.unwrap_or_else(|| self.engine.clone());
        let port = if Arc::ptr_eq(&engine, &self.engine) {
            self.port.clone()
        } else {
            FakePort::new(engine.clone())
        };
        let m = QueueManager::new(self.db.clone(), self.bus.clone(), port.clone());
        if let Some(h) = hooks {
            m.set_dispatch_hooks_for_test(h);
        }
        m.init().unwrap();
        (m, port)
    }

    fn enqueue(&self, m: &QueueManager, payload: &str, model: Option<&str>) -> QueueItem {
        m.enqueue(EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: model.map(String::from),
            payload: payload.into(),
        })
        .unwrap()
    }

    fn get(&self, id: &str) -> QueueItem {
        self.db_waiter().get(id).unwrap().unwrap()
    }

    fn db_waiter(&self) -> saiwork_queue::QueueRepo {
        saiwork_queue::QueueRepo::new(self.db.clone())
    }

    fn state(&self, id: &str) -> QueueState {
        self.get(id).state
    }
}

async fn start_engine(bus: &EventBus, engine: &FakeEngine) {
    let diagnostics = Arc::new(Diagnostics::new());
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let ctx = EngineStartContext {
        workspace_id: None,
        workspace_path: None,
        bus: bus.clone(),
        diagnostics,
        supervisor,
        report_failure: Arc::new(|_, _| {}),
    };
    engine.start(&ctx).await.unwrap();
}

/// Bounded test wait for a queue state (20ms ticks; test-only polling).
async fn wait_state(h: &Harness, id: &str, state: QueueState, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if h.state(id) == state {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {id} to reach {state:?}; current {:?} (error {:?})",
            h.state(id),
            h.get(id).last_error
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---- dispatch gate ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_item_dispatches_once_and_completes_done() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let item = h.enqueue(&m, "hello", None);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 1);
    assert_eq!(h.port.sent_payloads(), vec!["hello"]);
    let done = h.get(&item.id);
    assert_eq!(done.attempt_count, 1);
    assert!(done.run_id.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_items_dispatch_in_deterministic_order() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let a = h.enqueue(&m, "A", None);
    let b = h.enqueue(&m, "B", None);
    wait_state(&h, &a.id, QueueState::Done, Duration::from_secs(15)).await;
    wait_state(&h, &b.id, QueueState::Done, Duration::from_secs(15)).await;
    assert_eq!(
        h.port.sent_payloads(),
        vec!["A", "B"],
        "strict order, concurrency=1"
    );
    assert_eq!(h.port.send_count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_run_marks_item_failed_and_engine_stays_ready() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let item = h.enqueue(&m, "will fail", Some("fake:fail"));
    wait_state(&h, &item.id, QueueState::Failed, Duration::from_secs(10)).await;
    assert_eq!(
        h.get(&item.id).last_error_code.as_deref(),
        Some("run_failed")
    );
    assert_eq!(h.port.send_count(), 1);
    assert_eq!(
        h.engine.health(),
        EngineHealth::Ready,
        "run failure ≠ engine failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hang_blocks_next_item_until_cancelled() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let a = h.enqueue(&m, "hang me", Some("fake:hang"));
    let b = h.enqueue(&m, "after", None);
    wait_state(&h, &a.id, QueueState::Dispatched, Duration::from_secs(10)).await;
    // B must NOT dispatch while A hangs (concurrency = 1).
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.state(&b.id),
        QueueState::Queued,
        "hang blocks the next item"
    );
    assert_eq!(h.port.send_count(), 1);
    // Cancel A → terminal CANCELLED → B proceeds.
    m.cancel(&a.id).await.unwrap();
    wait_state(&h, &a.id, QueueState::Cancelled, Duration::from_secs(10)).await;
    wait_state(&h, &b.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_dispatched_durability_failure_is_fail_closed_and_never_invokes_adapter() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    let item = h.enqueue(&m, "hello", None);
    wait_state(&h, &item.id, QueueState::Dispatched, Duration::from_secs(10)).await;

    // A durability failure of the cancel intent must fail closed AND must NOT
    // reach the external adapter cancel: a persistence error can never justify
    // a side-effecting external cancel whose durable intent did not survive
    // (TASK 24 §9).
    m.set_repo_failpoints_for_test(RepoFailpoints {
        cancel_dispatched_error: Some(Arc::new(|_| true)),
        ..Default::default()
    });
    let err = m.cancel(&item.id).await.unwrap_err();
    assert!(
        matches!(err, QueueError::StorageUnavailable(_)),
        "durability failure must fail closed, got {err:?}"
    );
    assert_eq!(
        port.cancel_count(),
        0,
        "adapter cancel must NOT run when the durable cancel intent failed"
    );

    // The run still completes normally; the authoring terminal reconciles it.
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(port.cancel_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_queued_durability_failure_returns_error_without_retrying() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    m.pause().unwrap();
    let item = h.enqueue(&m, "keep durable", None);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_hook = calls.clone();
    m.set_repo_failpoints_for_test(RepoFailpoints {
        cancel_queued_error: Some(Arc::new(move |_| {
            (calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0).then(|| {
                QueueError::StorageUnavailable(
                    "injected queued-cancel durability failure (test)".into(),
                )
            })
        })),
        ..Default::default()
    });

    let err = m.cancel(&item.id).await.unwrap_err();
    assert!(
        matches!(err, QueueError::StorageUnavailable(_)),
        "durability failure must surface, got {err:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a storage error is terminal for this mutation, never a retry signal"
    );
    assert_eq!(h.state(&item.id), QueueState::Queued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_queued_cas_conflict_rereads_and_routes_current_state() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    m.pause().unwrap();
    let item = h.enqueue(&m, "cancel after race", None);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_hook = calls.clone();
    let item_id = item.id.clone();
    m.set_repo_failpoints_for_test(RepoFailpoints {
        cancel_queued_error: Some(Arc::new(move |_| {
            (calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0).then(|| {
                QueueError::Conflict {
                    item_id: item_id.clone(),
                    current: 2,
                    expected: 1,
                }
            })
        })),
        ..Default::default()
    });

    m.cancel(&item.id).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.state(&item.id), QueueState::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_of_already_terminal_does_not_invoke_adapter() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    let item = h.enqueue(&m, "hello", None);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;

    // Cancelling a stale/already-terminal run must reroute (false CAS) and must
    // never invoke the external adapter cancel for a run that is no longer
    // dispatched (TASK 24 §9).
    let err = m.cancel(&item.id).await.unwrap_err();
    assert!(
        matches!(err, QueueError::InvalidState { .. }),
        "cancel of a terminal item must reroute, got {err:?}"
    );
    assert_eq!(
        port.cancel_count(),
        0,
        "adapter cancel must NOT run for a stale/terminal run"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_queued_item_never_dispatches() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    // Deterministic no-send: pause blocks claims, so the item is QUEUED when
    // cancelled and can never reach the engine. (Cancel racing an in-flight
    // claim is exercised by `cancel_during_handoff…` and the hang test.)
    m.pause().unwrap();
    let item = h.enqueue(&m, "do not send", None);
    m.cancel(&item.id).await.unwrap();
    wait_state(&h, &item.id, QueueState::Cancelled, Duration::from_secs(5)).await;
    m.resume().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.port.send_count(), 0, "cancelled item must never dispatch");
    assert_eq!(
        h.state(&item.id),
        QueueState::Cancelled,
        "terminal stays terminal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_handoff_intent_is_honored_without_send() {
    let h = Harness::new().await;
    // Block the worker inside the before-send window: LEASED + sending,
    // session created, send not yet called.
    let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release2 = release.clone();
    let hooks = DispatchHooks {
        before_send: Some(Arc::new(move || {
            let release2 = release2.clone();
            Box::pin(async move {
                let (lock, cvar) = &*release2;
                let mut flag = lock.lock().unwrap();
                while !*flag {
                    flag = cvar.wait(flag).unwrap();
                }
            })
        })),
        after_send: None,
    };
    let (m, _) = h.manager(None, Some(hooks));
    let item = h.enqueue(&m, "cancel me", None);
    // Wait until the item is LEASED+sending (session created, worker parked).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if h.state(&item.id) == QueueState::Leased {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "worker never reached the lease window"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    m.cancel(&item.id).await.unwrap();
    {
        let (lock, cvar) = &*release;
        *lock.lock().unwrap() = true;
        cvar.notify_one();
    }
    wait_state(&h, &item.id, QueueState::Cancelled, Duration::from_secs(10)).await;
    assert_eq!(
        h.port.send_count(),
        0,
        "cancel intent must prevent the send"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_unavailable_waits_then_dispatches_on_ready() {
    let h = Harness::new().await;
    // A manager over a NOT-started engine.
    let fresh = h.restarted_engine();
    let (m, port) = h.manager(Some(fresh.clone()), None);
    let item = h.enqueue(&m, "wait for engine", None);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.state(&item.id),
        QueueState::Queued,
        "item must wait, not fail, while the engine is unavailable"
    );
    assert_eq!(port.send_count(), 0);
    // Start the engine and publish engine.ready (the registry does this in
    // production); the dispatcher wakes and proceeds.
    start_engine(&h.bus, &fresh).await;
    h.bus.publish(saiwork_events::Event::EngineReady {
        engine_id: ENGINE.into(),
    });
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(port.send_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eligible_item_beyond_first_candidate_page_is_dispatched() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let mut blocked = Vec::new();
    for index in 0..DISPATCH_CANDIDATE_PAGE_SIZE {
        blocked.push(
            m.enqueue(EnqueueRequest {
                workspace_id: "w1".into(),
                engine_id: "unavailable".into(),
                session_id: None,
                session_mode: SessionMode::New,
                model: None,
                payload: format!("blocked-{index}"),
            })
            .unwrap(),
        );
    }
    let ready = h.enqueue(&m, "ready-after-page", None);

    wait_state(&h, &ready.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.sent_payloads(), vec!["ready-after-page"]);
    assert_eq!(
        h.state(&blocked[0].id),
        QueueState::Queued,
        "blocked head must stay durable while a later page dispatches"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_blocks_new_claims_and_resume_dispatches() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    m.pause().unwrap();
    let item = h.enqueue(&m, "paused item", None);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.state(&item.id),
        QueueState::Queued,
        "paused queue must not claim"
    );
    assert_eq!(h.port.send_count(), 0);
    m.resume().unwrap();
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_wakeup_enqueue_at_idle_is_never_missed() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    // Let the dispatcher reach its idle wait.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let item = h.enqueue(&m, "wake me", None);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_dispatcher_ignores_stream_flood_and_does_zero_scans() {
    // PERFORMANCE.md: the dispatcher is event-driven (Notify only). A
    // high-rate stream flood must not wake it, and an idle queue must
    // perform zero eligibility scans over a timed idle sample. The wake
    // matrix (enqueue) stays intact afterwards.
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    // Let the dispatcher complete its initial truth scan and reach idle.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let baseline = m.dispatch_scan_count();
    for i in 0..10_000u32 {
        h.bus.publish(saiwork_events::Event::MessageDelta {
            session_id: format!("s{i}").into(),
            run_id: format!("r{i}").into(),
            delta: "x".into(),
        });
    }
    // Give any (wrong) wake/scan the chance to surface.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        m.dispatch_scan_count(),
        baseline,
        "stream deltas must not trigger eligibility scans"
    );
    // Wake matrix intact: enqueue still dispatches and completes.
    let item = h.enqueue(&m, "still wakes", None);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 1);
}

// ---- double dispatch / concurrency ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_cannot_double_dispatch_one_item() {
    let h = Harness::new().await;
    // Two managers over the same DB (forbidden in production by
    // single-instance; the atomic claim must still hold).
    let (m1, _) = h.manager(None, None);
    let (m2, _) = h.manager(None, None);
    let item = h.enqueue(&m1, "exactly once", None);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(
        h.port.send_count(),
        1,
        "two dispatchers must produce exactly one engine send"
    );
    let _ = &m2;
}

// ---- crash matrix (failpoints) ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_claim_before_session_recovers_to_queued() {
    let h = Harness::new().await;
    // Simulate a crash exactly after QUEUED → LEASED (phase=prepare): no
    // external side effect exists. Recovery must restore it to QUEUED.
    let item = h
        .db_waiter()
        .enqueue(&saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "recover me".into(),
        })
        .unwrap();
    assert!(h.db_waiter().claim(&item.id).unwrap());
    assert_eq!(
        h.db_waiter().get(&item.id).unwrap().unwrap().state,
        QueueState::Leased
    );
    // New app lifetime.
    let (m, _) = h.manager(None, None);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(
        h.port.send_count(),
        1,
        "recovered item dispatches exactly once"
    );
    assert_eq!(h.port.sent_payloads(), vec!["recover me"]);
    let _ = &m;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_send_is_ambiguous_and_never_resent() {
    let h = Harness::new().await;
    // Panic inside the before-send window: LEASED+sending is committed, the
    // send never ran, and the dispatcher dies (simulated crash).
    let hooks = DispatchHooks {
        before_send: Some(Arc::new(|| {
            Box::pin(async { panic!("simulated crash before send") })
        })),
        after_send: None,
    };
    let (m1, _) = h.manager(None, Some(hooks));
    let item = h.enqueue(&m1, "ambiguous", None);
    // Let the dispatcher panic and the row settle in LEASED+sending.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.db_waiter().get(&item.id).unwrap().unwrap().state,
        QueueState::Leased,
        "crash leaves the item LEASED+sending (no run recorded)"
    );
    assert_eq!(h.port.send_count(), 0, "the send never ran");
    drop(m1);
    // New app lifetime: recovery must NOT blindly redispatch — the item is
    // UNKNOWN (execution outcome cannot be proven), never auto-retried.
    let (m2, _) = h.manager(None, None);
    let item2 = h.db_waiter().get(&item.id).unwrap().unwrap();
    assert_eq!(item2.state, QueueState::Unknown);
    assert_eq!(item2.last_error_code.as_deref(), Some("dispatch_unknown"));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.port.send_count(),
        0,
        "unknown item must never auto-redispatch"
    );
    let _ = &m2;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_external_acceptance_is_never_resent() {
    let h = Harness::new().await;
    // The after_send hook fires once the engine accepted the send (run_id
    // exists upstream) but before the durable DISPATCHED commit.
    let hooks = DispatchHooks {
        before_send: None,
        after_send: Some(Arc::new(|| {
            Box::pin(async { panic!("simulated crash after acceptance") })
        })),
    };
    let (m1, _) = h.manager(None, Some(hooks));
    let item = h.enqueue(&m1, "accepted once", None);
    // Let the dispatcher panic after the accepted send.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        h.port.send_count(),
        1,
        "the engine accepted exactly one send before the crash"
    );
    assert_eq!(
        h.db_waiter().get(&item.id).unwrap().unwrap().state,
        QueueState::Leased,
        "crash before the durable DISPATCHED commit leaves LEASED+sending"
    );
    drop(m1);
    let (m2, _) = h.manager(None, None);
    let item2 = h.db_waiter().get(&item.id).unwrap().unwrap();
    assert_eq!(item2.state, QueueState::Unknown);
    assert_eq!(item2.last_error_code.as_deref(), Some("dispatch_unknown"));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.port.send_count(),
        1,
        "recovery must never duplicate an accepted send"
    );
    let _ = &m2;
}

// ---- TASK 23: OUTCOME_UNKNOWN state + workspace blocking ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatched_at_restart_marks_unknown_and_never_redispatch() {
    // TASK 23 §28–§29: a DISPATCHED item at restart has no reconcilable
    // engine authority in this process (run registries are in-memory, Harness
    // ACP sessions are connection-owned) → UNKNOWN, never resend, never
    // presented as a live run.
    let h = Harness::new().await;
    let item = h
        .db_waiter()
        .enqueue(&saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "was running".into(),
        })
        .unwrap();
    h.db_waiter().claim(&item.id).unwrap();
    h.db_waiter().begin_send(&item.id, "sess-1").unwrap();
    h.db_waiter().mark_dispatched(&item.id, "run-1").unwrap();
    assert_eq!(h.state(&item.id), QueueState::Dispatched);
    // New app lifetime.
    let (m, _) = h.manager(None, None);
    let item2 = h.db_waiter().get(&item.id).unwrap().unwrap();
    assert_eq!(item2.state, QueueState::Unknown);
    assert_eq!(item2.last_error_code.as_deref(), Some("dispatch_unknown"));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.port.send_count(),
        0,
        "dispatched-at-restart item must never auto-redispatch"
    );
    let _ = &m;
}

/// P1 (TASK 24 §9): UNKNOWN deliberately carries its run_id so a LATER
/// authoritative terminal can reconcile it — the exact persisted id, never a
/// guess. `Completed(run-1)` reconciles UNKNOWN(run-1) → DONE; an unrelated
/// terminal does nothing; restart retains the correlation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_run_id_correlates_and_reconciles_on_matching_terminal() {
    let h = Harness::new().await;
    // Restart path: DISPATCHED run-1 is persisted, then a NEW app lifetime
    // recovers it to UNKNOWN (keeping run_id) and re-correlates it.
    let item = h
        .db_waiter()
        .enqueue(&saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "was dispatched".into(),
        })
        .unwrap();
    h.db_waiter().claim(&item.id).unwrap();
    h.db_waiter().begin_send(&item.id, "sess-1").unwrap();
    h.db_waiter().mark_dispatched(&item.id, "run-1").unwrap();
    let (m, port) = h.manager(None, None);
    assert_eq!(h.state(&item.id), QueueState::Unknown);
    assert_eq!(
        h.get(&item.id).run_id.as_deref(),
        Some("run-1"),
        "UNKNOWN must persist its exact run_id"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(port.send_count(), 0, "never auto-redispatch");

    // An UNRELATED terminal must NOT reconcile the UNKNOWN row.
    h.bus.publish(saiwork_events::Event::MessageCompleted {
        session_id: "sess-1".into(),
        run_id: "run-other".into(),
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        h.state(&item.id),
        QueueState::Unknown,
        "unrelated run terminal must be ignored"
    );

    // The MATCHING authoritative terminal reconciles UNKNOWN → DONE exactly
    // once (restart correlation: the run_index was rebuilt from the
    // persisted run_id).
    h.bus.publish(saiwork_events::Event::MessageCompleted {
        session_id: "sess-1".into(),
        run_id: "run-1".into(),
    });
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(5)).await;
    assert_eq!(port.send_count(), 0, "reconciliation never redispatchs");
}

/// P1 (TASK 24 §9): an in-process DISPATCHED → UNKNOWN transition (engine
/// published message.outcome_unknown) KEEPS the run correlation, so a later
/// authoritative terminal still reconciles the item; a FAILED/CANCELLED
/// terminal works the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_process_unknown_keeps_correlation_for_later_terminal() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    let item = h.enqueue(&m, "hello", None);
    wait_state(&h, &item.id, QueueState::Dispatched, Duration::from_secs(10)).await;
    let run_id = h.get(&item.id).run_id.clone().expect("dispatched run_id");
    assert_eq!(port.send_count(), 1);

    // The engine loses the outcome: DISPATCHED → UNKNOWN (keeps run_id).
    h.bus.publish(saiwork_events::Event::MessageOutcomeUnknown {
        session_id: "sess-1".into(),
        run_id: run_id.clone().into(),
        error: "transport lost".into(),
    });
    wait_state(&h, &item.id, QueueState::Unknown, Duration::from_secs(5)).await;
    assert_eq!(
        h.get(&item.id).run_id.as_deref(),
        Some(run_id.as_str()),
        "UNKNOWN keeps its run_id"
    );

    // A later definitive terminal for the SAME run reconciles UNKNOWN → DONE.
    h.bus.publish(saiwork_events::Event::MessageFailed {
        session_id: "sess-1".into(),
        run_id: run_id.into(),
        error: "late failure surfacing".into(),
    });
    wait_state(&h, &item.id, QueueState::Failed, Duration::from_secs(5)).await;
    assert_eq!(port.send_count(), 1, "no redispatch during reconciliation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_item_blocks_same_workspace_but_not_others() {
    // TASK 23 §50–§51: an UNKNOWN item in workspace A blocks further queued
    // mutating dispatch in A (the unknown old run may have mutated files);
    // workspace B proceeds independently.
    let h = Harness::new().await;
    // Seed an UNKNOWN item in w1 directly (as recovery would).
    let unknown = h
        .db_waiter()
        .enqueue(&saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "ambiguous old run".into(),
        })
        .unwrap();
    h.db_waiter().claim(&unknown.id).unwrap();
    h.db_waiter().begin_send(&unknown.id, "sess-1").unwrap();
    let (m, _) = h.manager(None, None);
    assert_eq!(h.state(&unknown.id), QueueState::Unknown);
    // New item in the SAME workspace must wait.
    let a = m
        .enqueue(saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "blocked by ambiguity".into(),
        })
        .unwrap();
    // New item in a DIFFERENT workspace dispatches normally.
    let b = m
        .enqueue(saiwork_queue::model::EnqueueRequest {
            workspace_id: "w2".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "other workspace".into(),
        })
        .unwrap();
    wait_state(&h, &b.id, QueueState::Done, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.state(&a.id),
        QueueState::Queued,
        "unknown in the workspace blocks further dispatch there"
    );
    assert_eq!(h.port.send_count(), 1, "only workspace B dispatched");
    // Resolve the ambiguity (explicit risk-confirmed abandon, TASK 24 §9)
    // → workspace A unblocks.
    m.resolve_unknown(&unknown.id, h.get(&unknown.id).revision)
        .unwrap();
    wait_state(
        &h,
        &unknown.id,
        QueueState::Cancelled,
        Duration::from_secs(5),
    )
    .await;
    wait_state(&h, &a.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_unknown_dispatches_new_attempt_with_explicit_risk() {
    // TASK 23 §20, §107–§109: retrying UNKNOWN is an explicit user act; a new
    // attempt dispatches while the old ambiguous evidence stays visible, and
    // a late old-attempt terminal can never mutate the new attempt.
    let h = Harness::new().await;
    let item = h
        .db_waiter()
        .enqueue(&saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "retry unknown".into(),
        })
        .unwrap();
    h.db_waiter().claim(&item.id).unwrap();
    h.db_waiter().begin_send(&item.id, "sess-1").unwrap();
    h.db_waiter().mark_dispatched(&item.id, "old-run").unwrap();
    let (m, _) = h.manager(None, None);
    assert_eq!(h.state(&item.id), QueueState::Unknown);
    let unknown = h.get(&item.id);
    assert!(unknown.last_error_code.as_deref() == Some("dispatch_unknown"));
    m.retry(&item.id, unknown.revision).unwrap();
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    let done = h.get(&item.id);
    assert_eq!(done.attempt_count, 2, "new attempt after explicit retry");
    assert_eq!(h.port.send_count(), 1, "exactly one new send");
    // A late terminal for the OLD run id cannot mutate the new attempt.
    h.bus.publish(saiwork_events::Event::MessageCompleted {
        session_id: "s".into(),
        run_id: "old-run".into(),
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        h.state(&item.id),
        QueueState::Done,
        "old terminal is ignored"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_cancel_cannot_resolve_unknown_but_explicit_abandon_can() {
    // TASK 24 §9: UNKNOWN means external work may still be mutating the
    // workspace. A generic Cancel must NOT fabricate cancellation (that would
    // unblock the workspace without stopping the external run); only the
    // explicit risk-confirmed abandon (`resolve_unknown`) may transition it,
    // with the ambiguity evidence retained.
    let h = Harness::new().await;
    let item = h
        .db_waiter()
        .enqueue(&saiwork_queue::model::EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "abandon".into(),
        })
        .unwrap();
    h.db_waiter().claim(&item.id).unwrap();
    h.db_waiter().begin_send(&item.id, "sess-1").unwrap();
    let (m, _) = h.manager(None, None);
    assert_eq!(h.state(&item.id), QueueState::Unknown);
    let unknown = h.get(&item.id);
    assert!(unknown.last_error_code.as_deref() == Some("dispatch_unknown"));

    // Plain Cancel is rejected — the workspace block stays.
    let err = m.cancel(&item.id).await.unwrap_err();
    assert!(matches!(err, QueueError::InvalidState { .. }));
    assert_eq!(h.state(&item.id), QueueState::Unknown);
    assert!(h.db_waiter().workspace_has_unknown("w1").unwrap());

    // Explicit abandon (risk-confirmed) resolves it; evidence is retained.
    m.resolve_unknown(&item.id, unknown.revision).unwrap();
    assert_eq!(h.state(&item.id), QueueState::Cancelled);
    let cancelled = h.get(&item.id);
    assert_eq!(
        cancelled.last_error_code.as_deref(),
        Some("dispatch_unknown"),
        "prior ambiguity evidence is retained after abandon"
    );
    assert!(!h.db_waiter().workspace_has_unknown("w1").unwrap());
    assert_eq!(h.port.send_count(), 0, "resolving unknown never sends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_session_busy_waits_then_dispatches() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    let sid = match port.create_session(ENGINE, Some("w1"), None).await.unwrap() {
        SessionCreateOutcome::Created { session_id } => session_id,
        o => panic!("expected Created, got {o:?}"),
    };
    // The session is busy (e.g. a direct run in flight): the queued item
    // must wait, never claim/lease/fail repeatedly (§32, §210–§211).
    port.set_busy(true);
    let item = m
        .enqueue(EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: Some(sid.clone()),
            session_mode: SessionMode::Existing,
            model: None,
            payload: "after busy".into(),
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        h.state(&item.id),
        QueueState::Queued,
        "busy session blocks claim"
    );
    assert_eq!(port.send_count(), 0);
    // Session frees → wake → dispatch.
    port.set_busy(false);
    h.bus.publish(saiwork_events::Event::SessionChanged {
        session_id: sid.into(),
    });
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(port.send_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_session_gone_fails_item_clearly() {
    let h = Harness::new().await;
    let (m, _port) = h.manager(None, None);
    let item = m
        .enqueue(EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: Some("ghost-session".into()),
            session_mode: SessionMode::Existing,
            model: None,
            payload: "to nowhere".into(),
        })
        .unwrap();
    wait_state(&h, &item.id, QueueState::Failed, Duration::from_secs(10)).await;
    assert_eq!(
        h.get(&item.id).last_error_code.as_deref(),
        Some("session_not_found"),
        "a disappeared target session must fail clearly, never auto-create"
    );
    assert_eq!(h.port.send_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_failed_item_dispatches_again() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let item = h.enqueue(&m, "flaky", Some("fake:fail"));
    wait_state(&h, &item.id, QueueState::Failed, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 1);
    // Manual retry: FAILED → QUEUED → edit the failing model off → the item
    // re-dispatches with a fresh run and succeeds. Pause keeps the worker
    // from claiming between the retry and the edit (deterministic CAS).
    let failed = h.get(&item.id);
    m.pause().unwrap();
    m.retry(&item.id, failed.revision).unwrap();
    let queued = h.get(&item.id);
    assert_eq!(queued.state, QueueState::Queued);
    m.edit(&item.id, queued.revision, &queued.payload, None)
        .unwrap();
    m.resume().unwrap();
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    let done = h.get(&item.id);
    assert_eq!(
        done.attempt_count, 2,
        "attempt metadata retained across retry"
    );
    assert_eq!(h.port.send_count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_crash_fails_dispatched_run_without_requeue() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let item = h.enqueue(&m, "crash me", Some("fake:crash"));
    wait_state(&h, &item.id, QueueState::Failed, Duration::from_secs(10)).await;
    let failed = h.get(&item.id);
    assert!(matches!(
        failed.last_error_code.as_deref(),
        Some("run_failed") | Some("engine_lost")
    ));
    assert_eq!(h.port.send_count(), 1, "crash must not auto-resend");
    assert!(
        matches!(h.engine.health(), EngineHealth::Failed { .. }),
        "engine is failed after crash"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_failed_without_terminal_marks_unknown_not_failed() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    // A hang run: dispatched, tracked, but no authoritative terminal will ever
    // arrive on its own.
    let item = h.enqueue(&m, "hang me", Some("fake:hang"));
    wait_state(&h, &item.id, QueueState::Dispatched, Duration::from_secs(10)).await;

    // The engine reports failure but no run terminal was observed. The
    // external run's outcome is UNPROVEN, so the workspace ownership must NOT
    // be released — the item becomes UNKNOWN (non-releasing, correlation
    // retained), never a definitive FAILED (TASK 24 §9).
    h.bus.publish(saiwork_events::Event::EngineFailed {
        engine_id: ENGINE.into(),
        error: "engine lost (test)".into(),
    });
    wait_state(&h, &item.id, QueueState::Unknown, Duration::from_secs(10)).await;
    let row = h.get(&item.id);
    assert_eq!(
        row.last_error_code.as_deref(),
        Some("engine_lost"),
        "must stay ambiguous (engine_lost), not a definitive failure"
    );
    assert!(
        row.run_id.is_some(),
        "run correlation must be retained for later reconciliation"
    );
    assert_eq!(port.send_count(), 1, "no redispatch after engine failure");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus_lag_reconciles_tracked_run_to_unknown_never_stuck_dispatched() {
    let h = Harness::new().await;
    let (m, _port) = h.manager(None, None);
    let item = h.enqueue(&m, "hang me", Some("fake:hang"));
    wait_state(&h, &item.id, QueueState::Dispatched, Duration::from_secs(10)).await;

    // Flood the STATE bus so the coordinator lags and cannot reconstruct the
    // (would-be) terminal for the tracked run.
    let bus = h.bus.clone();
    let producer = tokio::spawn(async move {
        for i in 0..5000u64 {
            bus.publish(saiwork_events::Event::EngineReady {
                engine_id: format!("flood-{i}").into(),
            });
        }
    });
    producer.await.unwrap();

    // A missed terminal must never leave the row indefinitely DISPATCHED: the
    // lagged coordinator reconciles every tracked run to UNKNOWN (non-releasing)
    // with correlation retained (TASK 24 §9).
    wait_state(&h, &item.id, QueueState::Unknown, Duration::from_secs(10)).await;
}

// ---- shutdown ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_keeps_queued_items_durable_and_stops_claims() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    let a = h.enqueue(&m, "A", None);
    let b = h.enqueue(&m, "B", None);
    // App shutdown with items still queued: nothing is lost, no new claims.
    m.shutdown_barrier();
    m.finish_shutdown().await;
    assert_eq!(h.state(&a.id), QueueState::Queued);
    assert_eq!(h.state(&b.id), QueueState::Queued);
    // A new app lifetime picks them up in order.
    let (m2, _) = h.manager(None, None);
    wait_state(&h, &a.id, QueueState::Done, Duration::from_secs(10)).await;
    wait_state(&h, &b.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.sent_payloads(), vec!["A", "B"]);
    let _ = &m2;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_after_shutdown_is_rejected() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    m.shutdown_barrier();
    m.finish_shutdown().await;
    let err = m.enqueue(EnqueueRequest {
        workspace_id: "w1".into(),
        engine_id: ENGINE.into(),
        session_id: None,
        session_mode: SessionMode::New,
        model: None,
        payload: "too late".into(),
    });
    assert!(
        matches!(err, Err(saiwork_queue::QueueError::ShuttingDown)),
        "no new work accepted after shutdown: {err:?}"
    );
}

/// TASK 24 §9: a transient `create_session` dependency failure while the
/// engine health stays Ready must NOT spin the dispatcher into a
/// claim→fail→rescan storm. Each failure schedules one bounded cancellable
/// backoff; when the dependency recovers, the SAME item dispatches exactly
/// once; shutdown cancels the backoff promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_create_failures_backoff_never_spins() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    // Engine READY, but create_session keeps failing with a transient
    // Network error.
    port.set_create_failures(usize::MAX);
    let item = h.enqueue(&m, "hi", None);
    // Give a spinning dispatcher plenty of time to spin.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let attempts = port.create_attempt_count();
    // 1.5 s with a 500 ms backoff → ~4 attempts; a spin would be hundreds.
    assert!(
        attempts <= 6,
        "create_session attempts must be bounded by the backoff, got {attempts}"
    );
    assert!(attempts >= 2, "bounded retry still retries, got {attempts}");
    // The item is still being retried (never FAILED, never dispatched).
    assert!(
        matches!(h.state(&item.id), QueueState::Queued | QueueState::Leased),
        "item stays queued across transient failures, got {:?}",
        h.state(&item.id)
    );
    assert_eq!(h.port.send_count(), 0, "no external send before creation");

    // Dependency recovers: the next bounded retry dispatches exactly once.
    port.set_create_failures(0);
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(h.port.send_count(), 1, "exactly one external send after recovery");
    assert_eq!(port.create_attempt_count(), attempts + 1);

    // Shutdown cancels the backoff promptly: no attempts after stop.
    let before = port.create_attempt_count();
    port.set_create_failures(usize::MAX);
    let blocked = h.enqueue(&m, "blocked", None);
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mid = port.create_attempt_count();
    assert!(mid > before, "retries continue while the queue runs");
    m.shutdown_barrier();
    m.finish_shutdown().await;
    let after = port.create_attempt_count();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        port.create_attempt_count(),
        after,
        "shutdown cancels the backoff: no attempts after stop"
    );
    assert_eq!(h.state(&blocked.id), QueueState::Queued);
}

/// TASK 24 §9 cross-authority durability: persisting the created SessionId
/// into the queue row fails AFTER the upstream session was authoritatively
/// created. With a successful authoritative cleanup, the item fails safely
/// (exactly one create + one delete — no orphan, no duplicate).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persist_created_failure_with_cleanup_fails_safely_no_duplicate() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    m.set_repo_failpoints_for_test(RepoFailpoints {
        persist_created_error: Some(Arc::new(|_| true)),
        ..Default::default()
    });
    let item = h.enqueue(&m, "hi", None);
    wait_state(&h, &item.id, QueueState::Failed, Duration::from_secs(10)).await;
    // Exactly one external create and one compensating delete: the orphan is
    // gone, so a retry can never create a duplicate.
    assert_eq!(port.create_attempt_count(), 1, "exactly one create");
    assert_eq!(port.delete_count(), 1, "cleanup deletes the orphaned session");
    assert_eq!(h.port.send_count(), 0, "no send before persistence");
    let row = h.get(&item.id);
    assert_eq!(
        row.last_error_code.as_deref(),
        Some("persist_session_failed"),
        "fail-safe code recorded"
    );
}

/// TASK 24 §9 cross-authority durability: persisting the created SessionId
/// fails AND authoritative cleanup is unsupported/fails — the item becomes
/// UNKNOWN (fail-closed), and a restart cannot issue a second create for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persist_created_failure_with_failed_cleanup_is_ambiguous_no_recreate() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    port.set_delete_failures(1);
    m.set_repo_failpoints_for_test(RepoFailpoints {
        persist_created_error: Some(Arc::new(|_| true)),
        ..Default::default()
    });
    let item = h.enqueue(&m, "hi", None);
    wait_state(&h, &item.id, QueueState::Unknown, Duration::from_secs(10)).await;
    assert_eq!(port.create_attempt_count(), 1, "exactly one create");
    assert_eq!(port.delete_count(), 1, "cleanup was attempted");
    assert_eq!(h.port.send_count(), 0);
    // Restart (fresh manager, same DB): the UNKNOWN row is not a candidate —
    // no second create, no dispatch.
    m.shutdown_barrier();
    m.finish_shutdown().await;
    let (m2, _) = h.manager(None, None);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        port.create_attempt_count(),
        1,
        "restart must not issue a second create"
    );
    assert_eq!(h.state(&item.id), QueueState::Unknown);
    m2.shutdown_barrier();
    m2.finish_shutdown().await;
}

// ---- strict persisted-enum decoding (fail closed, TASK 24 §9) ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_persisted_state_fails_closed_and_never_dispatches() {
    // A corrupted/future-schema row must fail closed: no silent substitution
    // to FAILED/NEW, no dispatch, and the exact invalid value surfaced.
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    // Seed a row with an unknown state via raw SQL (tests only).
    h.db.with_conn(|c| {
        c.execute(
            "INSERT INTO queue_items
               (id, workspace_id, engine_id, session_id, session_mode, model, payload,
                state, order_key, revision, attempt_count, dispatch_phase, created_at, updated_at)
             VALUES ('bad-state', 'w1', 'fake', NULL, 'new', NULL, 'x', 'future_state',
                     1, 1, 0, 'prepare', 0, 0)",
            [],
        )
        .map(|_| ())
        .map_err(saiwork_storage::StorageError::from)
    })
    .unwrap();
    // Schema validation surfaces the typed error with the exact value.
    let err = h.db_waiter().validate_schema_integrity().unwrap_err();
    assert!(
        err.to_string().contains("future_state")
            && err.to_string().contains("bad-state"),
        "invalid value identified: {err}"
    );
    assert!(!matches!(h.port.send_count() > 0, true), "no dispatch");
    // QueueManager init over the corrupt row fails closed.
    let m2 = saiwork_queue::QueueManager::new(h.db.clone(), h.bus.clone(), h.port.clone());
    assert!(
        m2.init().is_err(),
        "bootstrap must fail closed on invalid persisted enum"
    );
    let _ = m;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_persisted_session_mode_fails_closed() {
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    h.db.with_conn(|c| {
        c.execute(
            "INSERT INTO queue_items
               (id, workspace_id, engine_id, session_id, session_mode, model, payload,
                state, order_key, revision, attempt_count, dispatch_phase, created_at, updated_at)
             VALUES ('bad-mode', 'w1', 'fake', NULL, 'existng', NULL, 'x', 'queued',
                     1, 1, 0, 'prepare', 0, 0)",
            [],
        )
        .map(|_| ())
        .map_err(saiwork_storage::StorageError::from)
    })
    .unwrap();
    let err = h.db_waiter().validate_schema_integrity().unwrap_err();
    assert!(
        err.to_string().contains("existng") && err.to_string().contains("bad-mode"),
        "invalid session_mode identified: {err}"
    );
    let m2 = saiwork_queue::QueueManager::new(h.db.clone(), h.bus.clone(), h.port.clone());
    assert!(
        m2.init().is_err(),
        "bootstrap must fail closed on invalid persisted session_mode"
    );
    let _ = m;
}

// ---- durability failures always fail closed (TASK 24 audit) ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatched_without_run_id_fails_closed_invariantly() {
    // A corrupted/partial DISPATCHED row without a run_id has an unknown live
    // external run that cancel/reconcile could never correlate: bootstrap
    // validation must fail closed, and cancel must NEVER fabricate a
    // Cancelled terminal for it (TASK 24 §9).
    let h = Harness::new().await;
    let (m, _) = h.manager(None, None);
    h.db.with_conn(|c| {
        c.execute(
            "INSERT INTO queue_items
               (id, workspace_id, engine_id, session_id, session_mode, model, payload,
                state, order_key, revision, attempt_count, dispatch_phase, created_at, updated_at, run_id)
             VALUES ('bad-dispatch', 'w1', 'fake', 's1', 'existing', NULL, 'x', 'dispatched',
                     1, 1, 0, 'sending', 0, 0, '')",
            [],
        )
        .map(|_| ())
        .map_err(saiwork_storage::StorageError::from)
    })
    .unwrap();
    // Schema validation rejects the row with the typed invariant error.
    let err = h.db_waiter().validate_schema_integrity().unwrap_err();
    assert!(
        err.to_string().contains("run_id") && err.to_string().contains("bad-dispatch"),
        "missing-run_id DISPATCHED identified: {err}"
    );
    // QueueManager init over the corrupt row fails closed.
    let m2 = saiwork_queue::QueueManager::new(h.db.clone(), h.bus.clone(), h.port.clone());
    assert!(
        m2.init().is_err(),
        "bootstrap must fail closed on DISPATCHED without run_id"
    );
    // Cancel on such a row is a typed invariant error, never a fabricated
    // Cancelled (and no Cancelled event is published).
    let mut sub = h.bus.subscribe();
    let err = m.cancel("bad-dispatch").await.unwrap_err();
    assert!(
        err.to_string().contains("run_id"),
        "cancel must surface the invariant, got {err:?}"
    );
    // No Cancelled state event for the row (bus is drained with no match).
    let mut saw_cancelled = false;
    let drain = async {
        loop {
            let env = sub.recv().await.unwrap();
            match env.event {
                saiwork_events::Event::QueueChanged { item_id, state } => {
                    if item_id.as_str() == "bad-dispatch"
                        && matches!(state.as_str(), "cancelled")
                    {
                        saw_cancelled = true;
                    }
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_millis(400), drain)
        .await
        .map(|_| ())
        .map_err(|_| ())
        .unwrap_or(());
    assert!(!saw_cancelled, "no Cancelled may be fabricated for the invariant row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatched_row_read_failure_fails_closed_and_never_fabricates_terminal() {
    use std::sync::atomic::AtomicUsize;

    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    // Fail `get` on the SECOND read of the dispatched item: the first read
    // (in dispatch_claimed) succeeds, the item is sent + marked DISPATCHED,
    // and the very next read (in wait_for_terminal) hits the injected
    // durability failure. The target id is set after enqueue (generated).
    let target = Arc::new(Mutex::new(None::<String>));
    let target2 = target.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = calls.clone();
    m.set_repo_failpoints_for_test(RepoFailpoints {
        get_error: Some(Arc::new(move |id: &str| {
            if target2.lock().unwrap().as_deref() == Some(id) {
                calls2.fetch_add(1, Ordering::SeqCst) == 1
            } else {
                false
            }
        })),
        ..Default::default()
    });
    let a = m
        .enqueue(EnqueueRequest {
            workspace_id: "w1".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "failme-item".into(),
        })
        .unwrap();
    *target.lock().unwrap() = Some(a.id.clone());
    // A second, DIFFERENT-workspace item: normally eligible while A runs.
    // It must NEVER dispatch once the queue fails closed.
    let b = m
        .enqueue(EnqueueRequest {
            workspace_id: "w2".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "B".into(),
        })
        .unwrap();

    // The injected read failure must fail the queue closed: no fabricated
    // Cancelled/terminal, no next-item dispatch while A's run may be live.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while m.status() != QueueStatus::Failed {
        assert!(
            std::time::Instant::now() < deadline,
            "queue must fail closed after the injected read failure; status {:?}",
            m.status()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !m.diagnostics().unwrap().worker_alive,
        "worker must stop after fail-closed"
    );
    // A was sent exactly once and its row was NOT rewritten (still
    // DISPATCHED — the failure path never touches the row).
    assert_eq!(port.send_count(), 1);
    assert_eq!(h.state(&a.id), QueueState::Dispatched);
    // B never dispatched.
    assert_eq!(h.state(&b.id), QueueState::Queued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_retry_clears_cancel_requested_and_starts_a_fresh_attempt() {
    // A previously cancel-requested UNKNOWN/FAILED item, retried manually,
    // must execute its new attempt instead of being claimed and immediately
    // cancelled (TASK 24 audit): cancel_requested resets to 0 atomically and
    // exactly one new send reaches the adapter.
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    for (id, state, key) in [("unk", "unknown", 1), ("fl", "failed", 2)] {
        h.db
            .with_conn(|c| {
                c.execute(
                    "INSERT INTO queue_items
                       (id, workspace_id, engine_id, session_id, session_mode, model, payload,
                        state, order_key, revision, attempt_count, dispatch_phase,
                        cancel_requested, run_id, created_at, updated_at)
                     VALUES (?1, 'w1', 'fake', NULL, 'new', NULL, 'retry-me', ?2,
                             ?3, 1, 1, 'sending', 1, 'old-run', 0, 0)",
                    rusqlite::params![id, state, key],
                )
                .map(|_| ())
                .map_err(saiwork_storage::StorageError::from)
            })
            .unwrap();
        // Manual retry: the new attempt is genuinely fresh.
        m.retry(id, 1).unwrap();
        let row = h.get(id);
        assert_eq!(row.state, QueueState::Queued);
        assert!(row.run_id.is_none(), "stale run association cleared");
        let flag: i64 = h
            .db
            .with_conn(|c| {
                c.query_row(
                    "SELECT cancel_requested FROM queue_items WHERE id = ?1",
                    rusqlite::params![id],
                    |r| r.get(0),
                )
                .map_err(saiwork_storage::StorageError::from)
            })
            .unwrap();
        assert_eq!(
            flag, 0,
            "{id}: the retried attempt must NOT inherit the old cancel intent"
        );
        wait_state(&h, id, QueueState::Done, Duration::from_secs(10)).await;
    }
    assert_eq!(
        port.send_count(),
        2,
        "exactly one new send per retried item"
    );
    assert_eq!(port.sent_payloads(), vec!["retry-me"; 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_durable_pause_fails_closed_and_canonical_values_survive_restart() {
    let h = Harness::new().await;
    // Seed a ready queued item + a garbage durable pause value.
    h.db
        .with_conn(|c| {
            c.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ('queue.paused', 'garbage', 1)",
                [],
            )
            .map(|_| ())
            .map_err(saiwork_storage::StorageError::from)
        })
        .unwrap();
    h.db
        .with_conn(|c| {
            c.execute(
                "INSERT INTO queue_items
                   (id, workspace_id, engine_id, payload, state, order_key, revision,
                    session_mode, dispatch_phase, created_at, updated_at)
                 VALUES ('ready-item', 'w1', 'fake', 'x', 'queued', 1, 1, 'new', 'prepare', 0, 0)",
                [],
            )
            .map(|_| ())
            .map_err(saiwork_storage::StorageError::from)
        })
        .unwrap();
    // A garbage pause value must fail init closed — never silently unpause
    // and dispatch work at startup.
    let m = QueueManager::new(h.db.clone(), h.bus.clone(), h.port.clone());
    let err = m.init().expect_err("garbage pause must fail closed");
    assert!(
        err.to_string().contains("queue.paused"),
        "typed storage error naming the bad setting: {err}"
    );
    assert_eq!(h.port.send_count(), 0, "no dispatch with a corrupt pause value");

    // Canonical spellings still work across restart.
    for (value, expected_paused) in [("1", true), ("0", false)] {
        h.db
            .with_conn(|c| {
                c.execute(
                    "UPDATE app_settings SET value = ?1 WHERE key = 'queue.paused'",
                    rusqlite::params![value],
                )
                .map(|_| ())
                .map_err(saiwork_storage::StorageError::from)
            })
            .unwrap();
        let m2 = QueueManager::new(h.db.clone(), h.bus.clone(), h.port.clone());
        m2.init().unwrap();
        assert_eq!(m2.is_paused(), expected_paused, "value {value}");
        m2.shutdown_barrier();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finish_shutdown_aborts_workers_that_exceed_the_join_timeout() {
    let h = Harness::new().await;
    // Park the dispatcher inside the before_send failpoint AT AN AWAIT POINT
    // (tokio Notify) so `JoinHandle::abort()` can actually cancel it — abort
    // cannot interrupt a task blocked in synchronous code, and finish_shutdown
    // must not block forever waiting for a worker that can never join.
    let gate = Arc::new(tokio::sync::Notify::new());
    let gate2 = gate.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered2 = entered.clone();
    let hooks = DispatchHooks {
        before_send: Some(Arc::new(move || {
            entered2.notify_one();
            // Park on an await point; abort() below cancels this future.
            let gate = gate2.clone();
            Box::pin(async move { gate.notified().await })
        })),
        after_send: None,
    };
    let (m, port) = h.manager(None, Some(hooks));
    let _item = h.enqueue(&m, "blocked", None);
    entered.notified().await; // dispatcher parked in before_send
    tokio::time::sleep(Duration::from_millis(50)).await;
    let scans_before = m.dispatch_scan_count();

    m.shutdown_barrier();
    m.finish_shutdown().await;
    // The join exceeded STOP_JOIN_TIMEOUT: the worker must have been ABORTED
    // (not merely detached) and awaited to termination.
    assert_eq!(m.status(), QueueStatus::Stopped);
    assert!(!m.diagnostics().unwrap().worker_alive);

    // Release the parked hook (harmless after the task was cancelled);
    // afterwards there must be no further repo/event activity and no late send.
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        m.dispatch_scan_count(),
        scans_before,
        "no repo activity after finish_shutdown"
    );
    assert_eq!(port.send_count(), 0, "the blocked send must never fire");
}

/// W2-002: a runtime `AppStopping` must NOT terminate the queue coordinator
/// prematurely. The coordinator must stay alive (draining) to observe the
/// terminals of active runs while engines stop — otherwise the tracked-run
/// correlation is dropped and rows are left stuck DISPATCHED / force-failed.
/// Here the active run completes AFTER AppStopping; the item must still reach
/// Done, proving the coordinator survived the event (and the drain completed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_stopping_does_not_kill_coordinator_before_active_run_terminal() {
    let h = Harness::new().await;
    let (m, _port) = h.manager(None, None);
    let item = h.enqueue(&m, "hello", None);
    wait_state(&h, &item.id, QueueState::Dispatched, Duration::from_secs(10)).await;
    let run_id = h.get(&item.id).run_id.clone().expect("dispatched run_id");

    // Shutdown begins (mirrors App::run_shutdown_sequence): publish AppStopping
    // then raise the barrier.
    h.bus.publish(saiwork_events::Event::AppStopping { reason: "test".into() });
    m.shutdown_barrier();

    // The active run completes only AFTER shutdown started.
    h.bus.publish(saiwork_events::Event::MessageCompleted {
        session_id: "sess-1".into(),
        run_id: run_id.clone().into(),
    });

    // The terminal MUST still be correlated → Done. If the coordinator had died
    // on AppStopping, this would hang/timeout and the item would stay DISPATCHED.
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(5)).await;
}

// ---- workspace-bound engine readiness (TASK 24 §9) ----

/// A healthy engine bound to workspace A must NOT serve an item targeting
/// workspace B: the item waits QUEUED (never FAILED, never sent, never
/// auto-rebound). Explicit rebind (simulated restart for B) wakes the
/// dispatcher and sends exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_workspace_binding_waits_not_fails() {
    let h = Harness::new().await;
    let (m, port) = h.manager(None, None);
    // Engine is READY but its runtime is bound to workspace "A".
    port.set_bound_workspace(Some("A".into()));

    let item = m
        .enqueue(EnqueueRequest {
            workspace_id: "B".into(),
            engine_id: ENGINE.into(),
            session_id: None,
            session_mode: SessionMode::New,
            model: None,
            payload: "hi from B".into(),
        })
        .unwrap();

    // Must stay QUEUED while bound to A: no create/send, no FAILED.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        h.state(&item.id),
        QueueState::Queued,
        "wrong-workspace binding must Wait, never FAILED"
    );
    assert_eq!(port.create_attempt_count(), 0, "no session creation for B");
    assert_eq!(port.send_count(), 0, "no send for B");

    // Explicit rebind to B (user restarts the engine for project B) wakes
    // the dispatcher through the Notify path (`resume()` notifies; the engine
    // event bus would do the same in production).
    port.set_bound_workspace(Some("B".into()));
    m.resume().unwrap();
    wait_state(&h, &item.id, QueueState::Done, Duration::from_secs(10)).await;
    assert_eq!(port.send_count(), 1, "exactly one send after rebind");
    assert_eq!(h.get(&item.id).workspace_id, "B");
}
