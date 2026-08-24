//! QueueManager — the single durable queue authority (law 7, TASK 13).
//!
//! - **SQLite is the durable truth**; this manager owns the state machine and
//!   the dispatch worker. The UI is a projection (law 5, 23).
//! - **One worker** (concurrency = 1, §56–§57): the dispatcher claims and
//!   dispatches one item at a time and waits for its terminal before the
//!   next. No parallel agent scheduler.
//! - **Event-driven**: the worker sleeps on a `Notify`; mutations and the
//!   run-coordinator wake it. A bounded backstop re-scan guards against any
//!   missed event (ADR-008 backstop, not polling).
//! - **Fail-closed**: a durability failure stops new dispatch until restart
//!   (§99–§101).
//!
//! Dispatch boundary (the heart of the queue, §23–§27):
//!
//! ```text
//! QUEUED ──claim──▶ LEASED(prepare) ──session+phase=sending──▶ LEASED(sending)
//!   ──send──▶ DISPATCHED(run_id, attempt++) ──terminal──▶ DONE|FAILED|CANCELLED
//! ```
//!
//! - A crash while `prepare` has no external side effect → recover to QUEUED.
//! - A crash while `sending` may have accepted the send → FAILED(ambiguous),
//!   never blindly redisplayed (exactly-once external effect is NOT
//!   guaranteed across the crash boundary — §25).

use std::collections::HashMap;
#[cfg(feature = "failpoints")]
use std::future::Future;
#[cfg(feature = "failpoints")]
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use saiwork_events::{Event, EventBus, QueueItemId, SubscribeError};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::model::{
    EnqueueRequest, PortError, QueueDiagnostics, QueueError, QueueItem, QueueSnapshot, QueueState,
    QueueStatus, SessionMode, DISPATCH_CANDIDATE_PAGE_SIZE,
};
use crate::port::{DispatchReceipt, EnginePort, EngineState, SessionCreateOutcome};
use crate::repo::QueueRepo;

/// Bounded join for dispatcher/coordinator tasks on shutdown.
const STOP_JOIN_TIMEOUT: Duration = Duration::from_secs(3);
/// Bounded drain: how long the coordinator waits for tracked runs to reach a
/// terminal after shutdown began before force-failing them.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Bounded wait for run terminals after an engine failure event.
const ENGINE_FAIL_DRAIN: Duration = Duration::from_secs(2);
/// Bounded wait for a cancelled run's terminal before reconciling directly.
const CANCEL_TERMINAL_WAIT: Duration = Duration::from_secs(3);
/// Bounded retry backoff for TRANSIENT pre-mutation dependency errors
/// (e.g. `create_session` network failures while engine health stays Ready).
/// The dispatcher never spins on these: one bounded cancellable sleep per
/// failure, then a re-scan. State conditions (engine not-ready, session busy)
/// instead park on the event Notify — an unrelated event must never bypass
/// the bounded backoff into a spin (TASK 24 §9).
const TRANSIENT_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Test-only dispatch failpoints (crash-window tests). Feature-gated: not
/// reachable in production builds (§85, §230).
#[cfg(feature = "failpoints")]
#[derive(Default)]
pub struct DispatchHooks {
    /// Fires after `begin_send` committed and immediately before `send()` —
    /// the "crash before engine call" and "crash during engine call" windows.
    /// Async so a parked worker sits at an await point that `abort()` can
    /// interrupt (a task blocked in synchronous code can never be cancelled).
    pub before_send: Option<Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>>,
    /// Fires after `send()` returned Ok but before `mark_dispatched` — the
    /// "crash after external acceptance" window.
    pub after_send: Option<Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>>,
}

/// No-op hooks in production builds.
#[cfg(not(feature = "failpoints"))]
#[derive(Default)]
pub struct DispatchHooks {}

enum DispatchOutcome {
    /// Item fully processed (terminal reached or failed before send).
    Done,
    /// Lease released; the worker should re-scan (wait condition).
    Wait,
    /// Lease released after a TRANSIENT pre-mutation dependency error: the
    /// worker must sleep one bounded cancellable backoff before re-scanning
    /// (never spin, TASK 24 §9).
    Backoff(Duration),
}

pub struct QueueManager {
    bus: EventBus,
    port: Arc<dyn EnginePort>,
    repo: QueueRepo,
    /// Wake the dispatch worker (permit semantics — safe against lost wakeups).
    wake: Arc<Notify>,
    /// Bounded drain timeout observed by the coordinator.
    stop_signal: Arc<Notify>,
    /// Fired whenever a tracked run reaches its terminal (cancel waits).
    terminal_notify: Arc<Notify>,
    /// run_id → (item_id, engine_id). Coordinator correlation; bounded by the
    /// single active dispatch (§120–§121).
    run_index: RwLock<HashMap<String, (String, String)>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    stopping: AtomicBool,
    stopped: AtomicBool,
    paused: AtomicBool,
    failed: RwLock<Option<String>>,
    worker_alive: AtomicBool,
    last_dispatch_error: Mutex<Option<String>>,
    #[cfg(feature = "failpoints")]
    hooks: Mutex<DispatchHooks>,
    stopping_since: Mutex<Option<Instant>>,
}

impl QueueManager {
    pub fn new(db: saiwork_storage::Db, bus: EventBus, port: Arc<dyn EnginePort>) -> Arc<Self> {
        Arc::new(Self {
            repo: QueueRepo::new(db),
            bus,
            port,
            wake: Arc::new(Notify::new()),
            stop_signal: Arc::new(Notify::new()),
            terminal_notify: Arc::new(Notify::new()),
            run_index: RwLock::new(HashMap::new()),
            tasks: Mutex::new(Vec::new()),
            stopping: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            failed: RwLock::new(None),
            worker_alive: AtomicBool::new(false),
            last_dispatch_error: Mutex::new(None),
            #[cfg(feature = "failpoints")]
            hooks: Mutex::new(DispatchHooks::default()),
            stopping_since: Mutex::new(None),
        })
    }

    /// Number of eligibility scans performed by this manager's repo
    /// (diagnostics/test gate: an idle queue must perform zero — the
    /// dispatcher is event-driven, PERFORMANCE.md).
    pub fn dispatch_scan_count(&self) -> u64 {
        self.repo.dispatch_scan_count()
    }

    /// Recovery + worker startup. Must complete before READY (§75–§77):
    /// stale leases are recovered and only then dispatch is enabled.
    pub fn init(self: &Arc<Self>) -> Result<(), QueueError> {
        // Fail closed on any invalid persisted enum (TASK 24 §9): a
        // corrupted/future row disables dispatch before any worker starts.
        self.repo.validate_schema_integrity()?;
        let paused = self.repo.is_paused()?;
        self.paused.store(paused, Ordering::SeqCst);
        let report = self.repo.recover()?;
        if report.recovered_to_queued > 0 {
            info!(
                count = report.recovered_to_queued,
                "queue recovery: restored stale leases to queued"
            );
        }
        let unknown = report.marked_unknown + report.marked_unknown_dispatched;
        if unknown > 0 {
            self.bus.publish(Event::RuntimeWarning {
                code: "QUEUE_OUTCOME_UNKNOWN".into(),
                message: format!(
                    "{unknown} queue item(s) have an unknown execution outcome (crash during handoff or dispatch at shutdown): automatic retry is disabled to avoid duplicate work and their workspace is blocked until resolved",
                ),
            });
        }
        // Restart correlation (TASK 24 §9): UNKNOWN rows that still carry
        // their persisted run_id are re-correlated so a later authoritative
        // terminal from a resumed engine can reconcile them — the exact
        // persisted id, never a guess. Rows without a run_id (session-create
        // ambiguity) stay uncorrelated.
        {
            let correlated = self.repo.unknown_runs_with_ids()?;
            if !correlated.is_empty() {
                let mut index = self.run_index.write().expect("queue run index mutex poisoned");
                for (item_id, run_id, engine_id) in correlated {
                    index.insert(run_id, (item_id, engine_id));
                }
            }
        }
        self.spawn_worker();
        info!("queue manager ready");
        Ok(())
    }

    fn spawn_worker(self: &Arc<Self>) {
        let dispatcher = self.clone();
        let coordinator = self.clone();
        let h1 = tokio::spawn(async move { dispatcher.dispatcher_loop().await });
        let h2 = tokio::spawn(async move { coordinator.coordinator_loop().await });
        self.worker_alive.store(true, Ordering::SeqCst);
        *self.tasks.lock().expect("queue tasks mutex poisoned") = vec![h1, h2];
    }

    // ---- status / snapshots ----

    pub fn status(&self) -> QueueStatus {
        if self.stopped.load(Ordering::SeqCst) {
            QueueStatus::Stopped
        } else if self.stopping.load(Ordering::SeqCst) {
            QueueStatus::ShuttingDown
        } else if self
            .failed
            .read()
            .expect("queue failed mutex poisoned")
            .is_some()
        {
            QueueStatus::Failed
        } else if self.paused.load(Ordering::SeqCst) {
            QueueStatus::Paused
        } else {
            QueueStatus::Ready
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn snapshot(&self) -> Result<QueueSnapshot, QueueError> {
        let items = self.repo.list_snapshot(50)?;
        Ok(QueueSnapshot {
            status: self.status(),
            paused: self.is_paused(),
            items,
            payload_preview: true,
        })
    }

    /// Full durable item (exact payload) for editing/inspecting one row
    /// (TASK 24 perf — the snapshot carries only bounded payload previews).
    pub fn get_item(&self, id: &str) -> Result<QueueItem, QueueError> {
        self.repo.get(id)?.ok_or_else(|| QueueError::NotFound(id.to_string()))
    }

    pub fn diagnostics(&self) -> Result<QueueDiagnostics, QueueError> {
        let counts = self.repo.counts()?;
        let mut d = QueueDiagnostics {
            status: self.status(),
            paused: self.is_paused(),
            ..Default::default()
        };
        for (state, n) in counts {
            match state {
                QueueState::Queued => d.queued = n,
                QueueState::Leased => d.leased = n,
                QueueState::Dispatched => d.dispatched = n,
                QueueState::Done => d.done = n,
                QueueState::Failed => d.failed = n,
                QueueState::Cancelled => d.cancelled = n,
                QueueState::Unknown => d.unknown = n,
            }
        }
        d.worker_alive = self.worker_alive.load(Ordering::SeqCst);
        let current = self
            .run_index
            .read()
            .expect("queue run index mutex poisoned")
            .values()
            .next()
            .cloned();
        d.current_item = current.map(|(item_id, _)| item_id);
        d.last_dispatch_error_code = self
            .last_dispatch_error
            .lock()
            .expect("queue last error mutex poisoned")
            .clone();
        Ok(d)
    }

    // ---- commands (all go through QueueManager — one dispatch authority) ----

    pub fn enqueue(&self, req: EnqueueRequest) -> Result<QueueItem, QueueError> {
        self.require_usable()?;
        // Target validation before durable persistence (TASK 24 §9): an
        // existing session must belong to the item's engine AND workspace,
        // or the row would be a lie about where the work will execute.
        if req.session_mode == SessionMode::Existing {
            let Some(session_id) = req.session_id.clone() else {
                return Err(QueueError::InvalidState {
                    item_id: "<new>".into(),
                    detail: "existing-session mode requires a session_id".into(),
                });
            };
            if let Err(e) = self
                .port
                .validate_enqueue(&session_id, &req.engine_id, &req.workspace_id)
            {
                return Err(QueueError::DispatchRejected(e.message().into()));
            }
        }
        let item = self.repo.enqueue(&req)?;
        self.publish_changed(&item.id, item.state);
        self.wake.notify_one();
        Ok(item)
    }

    pub fn edit(
        &self,
        id: &str,
        expected_revision: i64,
        payload: &str,
        model: Option<&str>,
    ) -> Result<QueueItem, QueueError> {
        self.require_usable()?;
        let item = self.repo.edit(id, expected_revision, payload, model)?;
        self.publish_changed(&item.id, item.state);
        self.wake.notify_one();
        Ok(item)
    }

    pub fn reorder(
        &self,
        id: &str,
        expected_revision: i64,
        new_index: usize,
    ) -> Result<(), QueueError> {
        self.require_usable()?;
        self.repo.reorder(id, expected_revision, new_index)?;
        self.publish_changed(id, QueueState::Queued);
        self.wake.notify_one();
        Ok(())
    }

    /// Cancel by state (§45–§47, §63): QUEUED → CANCELLED (CAS); LEASED →
    /// durable cancel intent honored by the worker; DISPATCHED → engine
    /// cancel, then bounded wait for the authoritative terminal.
    pub async fn cancel(&self, id: &str) -> Result<(), QueueError> {
        self.require_usable()?;
        let current_id = id.to_string();
        loop {
            let item = self
                .repo
                .get(&current_id)?
                .ok_or_else(|| QueueError::NotFound(current_id.clone()))?;
            match item.state {
                QueueState::Queued => {
                    // The dispatcher may claim the item between our read and
                    // the CAS: then the intent path below owns it. A clean
                    // CAS miss re-reads and re-routes; storage failure is not
                    // a retry signal and must surface immediately.
                    match self.repo.cancel_queued(&current_id, item.revision) {
                        Ok(true) => {
                            self.publish_changed(&current_id, QueueState::Cancelled);
                            self.wake.notify_one();
                            return Ok(());
                        }
                        Ok(false) => continue,
                        Err(QueueError::Conflict { .. } | QueueError::InvalidState { .. }) => {
                            continue
                        }
                        Err(error) => return Err(error),
                    }
                }
                QueueState::Leased => {
                    if self.repo.request_cancel_leased(&current_id)? {
                        // The worker honors the intent at its next step
                        // (before any external side effect, or by cancelling
                        // the run). One cancellation owner (§63).
                        self.wake.notify_one();
                        return Ok(());
                    }
                    // Transitioned under us — re-read and route.
                    continue;
                }
                QueueState::Unknown => {
                    // UNKNOWN means external work may still be mutating the
                    // workspace. A generic Cancel must NOT fabricate
                    // cancellation — that would unblock the workspace while
                    // the external run may still run. Only the explicit
                    // `resolve_unknown` abandonment (risk-confirmed, UI) may
                    // transition it (TASK 24 §9).
                    return Err(QueueError::InvalidState {
                        item_id: current_id,
                        detail: "item outcome is unknown; use the explicit abandon/resolve action (this does NOT stop external work)".into(),
                    });
                }
                QueueState::Dispatched => {
                    let Some((run_id, _lease_id)) = self.repo.current_run(&current_id)? else {
                        // DISPATCHED without a run association is invariant
                        // corruption: bootstrap validation rejects such rows
                        // (TASK 24 §9) and at runtime this is never a clean
                        // state. Fail closed with a typed error — NEVER
                        // fabricate a Cancelled terminal (the durable row
                        // would stay DISPATCHED while we claimed success and
                        // an unknown live external run may still exist).
                        return Err(QueueError::InvalidPersistedRow {
                            row_id: current_id,
                            field: "run_id",
                            value: "<missing> (DISPATCHED requires a run_id)".into(),
                        });
                    };
                    if run_id.trim().is_empty() {
                        // Same invariant for an empty-string association.
                        return Err(QueueError::InvalidPersistedRow {
                            row_id: current_id,
                            field: "run_id",
                            value: "<empty> (DISPATCHED requires a run_id)".into(),
                        });
                    }
                    // Durable cancel intent FIRST — and it MUST succeed before
                    // any external side effect (TASK 24 §9): a persistence
                    // failure is a durability failure that fail-closes (no
                    // external adapter cancel); a false CAS means the row
                    // changed under us (terminal/cancelled elsewhere) so we
                    // re-read and reroute by its CURRENT state, never invoking
                    // the adapter cancel for a stale run.
                    match self.repo.request_cancel_dispatched(&current_id) {
                        Ok(true) => {}
                        Ok(false) => continue, // false CAS → re-read and reroute
                        Err(e) => return Err(e), // durability failure → fail closed
                    }
                    let session_id = item.session_id.clone().unwrap_or_default();
                    self.port
                        .cancel(&session_id, &run_id)
                        .await
                        .map_err(QueueError::Port)?;
                    // Bounded wait for the authoritative terminal (the
                    // coordinator transitions the row). If no terminal
                    // arrives, the item STAYS dispatched — never a fabricated
                    // CANCELLED. The cancel intent persists, and the run's
                    // real terminal (whenever it arrives) completes it.
                    let notify = self.terminal_notify.clone();
                    tokio::time::timeout(CANCEL_TERMINAL_WAIT, notify.notified())
                        .await
                        .ok();
                    self.wake.notify_one();
                    return Ok(());
                }
                _ => {
                    return Err(QueueError::InvalidState {
                        item_id: current_id,
                        detail: format!("cannot cancel a terminal item ({})", item.state.as_str()),
                    });
                }
            }
        }
    }

    /// Manual retry: FAILED → QUEUED or UNKNOWN → QUEUED (§112–§114, TASK 23
    /// §20). Retrying an UNKNOWN item is an explicit user act that
    /// acknowledges possible duplication risk — the UI surfaces it; it is
    /// never automatic.
    pub fn retry(&self, id: &str, expected_revision: i64) -> Result<(), QueueError> {
        self.require_usable()?;
        self.repo.retry(id, expected_revision)?;
        // The manual retry starts a GENUINELY fresh attempt: drop any stale
        // run correlation for this item so a late terminal of the old run
        // can never touch the new attempt (TASK 24 §9).
        self.drop_run_correlation_for_item(id);
        self.publish_changed(id, QueueState::Queued);
        self.wake.notify_one();
        Ok(())
    }

    /// Explicitly abandon an UNKNOWN item (TASK 24 §9). This is NOT a
    /// cancellation of the external run: the run may still be mutating the
    /// workspace, and the UI must state that risk before calling. The prior
    /// ambiguity evidence (`last_error`/`last_error_code`) is retained so the
    /// blocked workspace is only unblocked after the user explicitly accepts
    /// the risk. Revision CAS.
    pub fn resolve_unknown(&self, id: &str, expected_revision: i64) -> Result<(), QueueError> {
        self.require_usable()?;
        self.repo.resolve_unknown(id, expected_revision)?;
        // Explicit risk-confirmed abandonment: the row is terminal CANCELLED;
        // drop the run correlation so a late terminal can never re-touch it.
        self.drop_run_correlation_for_item(id);
        self.publish_changed(id, QueueState::Cancelled);
        self.wake.notify_one();
        Ok(())
    }

    /// Drop every run-index entry pointing at `item_id` (used when a manual
    /// user act replaces the attempt: retry / explicit abandonment).
    fn drop_run_correlation_for_item(&self, item_id: &str) {
        let mut index = self.run_index.write().expect("queue run index mutex poisoned");
        index.retain(|_, (iid, _)| iid != item_id);
    }

    /// Read-only durable ambiguity gate (TASK 24 §9): true when the workspace
    /// has an UNKNOWN item whose external run may still be live. The direct
    /// (non-queue) send boundary consults this BEFORE any reservation or
    /// engine call — a restart must never let a direct send bypass durable
    /// ambiguity. Storage failures propagate (fail-closed: the caller must
    /// not send when it cannot prove the workspace is clean).
    pub fn workspace_has_unknown(&self, workspace_id: &str) -> Result<bool, QueueError> {
        self.repo.workspace_has_unknown(workspace_id)
    }

    /// Safe-Forget gate (TASK 24 §9): true when any active/nonterminal item
    /// (QUEUED/LEASED/DISPATCHED/UNKNOWN) references the workspace. Deleting
    /// the workspace identity while such work exists would strand durable
    /// references; terminal history rows do not block.
    pub fn workspace_has_nonterminal(&self, workspace_id: &str) -> Result<bool, QueueError> {
        self.repo.workspace_has_nonterminal(workspace_id)
    }

    pub fn session_has_nonterminal(&self, session_id: &str) -> Result<bool, QueueError> {
        self.repo.session_has_nonterminal(session_id)
    }

    pub fn pause(&self) -> Result<(), QueueError> {
        self.repo.set_paused(true)?;
        self.paused.store(true, Ordering::SeqCst);
        self.bus.publish(Event::RuntimeWarning {
            code: "QUEUE_PAUSED".into(),
            message: "queue paused: no new items will be claimed".into(),
        });
        self.wake.notify_one();
        Ok(())
    }

    pub fn resume(&self) -> Result<(), QueueError> {
        self.repo.set_paused(false)?;
        self.paused.store(false, Ordering::SeqCst);
        self.wake.notify_one();
        Ok(())
    }

    // ---- shutdown ----

    /// Phase 1: stop claiming new items and release safe leases. Active runs
    /// keep streaming; the coordinator stays alive to observe their terminals
    /// while the app stops engines.
    pub fn shutdown_barrier(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        *self.stopping_since.lock().expect("stopping mutex poisoned") = Some(Instant::now());
        self.wake.notify_one();
        self.terminal_notify.notify_one();
        self.stop_signal.notify_one();
    }

    /// Phase 2: bounded join of the worker tasks (after engines stopped), then
    /// release safe leases. Any LEASED `sending` item is left for restart
    /// recovery (ambiguous by design, §81).
    pub async fn finish_shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.wake.notify_one();
        self.terminal_notify.notify_one();
        self.stop_signal.notify_one();
        let handles = self
            .tasks
            .lock()
            .expect("queue tasks mutex poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for mut h in handles {
            // Await by reference (JoinHandle's Future impl consumes the
            // handle); on timeout ABORT the task and await its termination —
            // dropping a JoinHandle only detaches, and the worker must be
            // provably terminal before storage closes (TASK 24 §9). A
            // completed handle is never polled again.
            let pinned = std::pin::Pin::new(&mut h); // JoinHandle is Unpin
            if tokio::time::timeout(STOP_JOIN_TIMEOUT, pinned).await.is_err() {
                warn!("queue worker join timed out; aborting worker task");
                h.abort();
                let _ = h.await;
            }
        }
        self.worker_alive.store(false, Ordering::SeqCst);
        match self.repo.release_prepare_leases_on_shutdown() {
            Ok(n) if n > 0 => info!(count = n, "queue shutdown: restored prepare-phase leases"),
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "queue shutdown: lease release failed (startup recovery will handle)")
            }
        }
        self.stopped.store(true, Ordering::SeqCst);
    }

    // ---- internals ----

    fn require_usable(&self) -> Result<(), QueueError> {
        if self.stopped.load(Ordering::SeqCst) || self.stopping.load(Ordering::SeqCst) {
            return Err(QueueError::ShuttingDown);
        }
        if self
            .failed
            .read()
            .expect("queue failed mutex poisoned")
            .is_some()
        {
            return Err(QueueError::NotReady(
                "queue is fail-closed after a durability failure; restart the application".into(),
            ));
        }
        Ok(())
    }

    fn fail_closed(&self, e: QueueError) {
        let reason = e.to_string();
        *self.failed.write().expect("queue failed mutex poisoned") = Some(reason.clone());
        self.worker_alive.store(false, Ordering::SeqCst);
        self.bus.publish(Event::RuntimeError {
            code: "QUEUE_DISPATCH_DISABLED".into(),
            message: format!("queue dispatch disabled (fail-closed): {reason}"),
        });
        self.wake.notify_one();
        warn!(error = %reason, "queue fail-closed: dispatch disabled");
    }

    fn fail_closed_str(&self, reason: String) {
        *self.failed.write().expect("queue failed mutex poisoned") = Some(reason.clone());
        self.worker_alive.store(false, Ordering::SeqCst);
        self.bus.publish(Event::RuntimeError {
            code: "QUEUE_DISPATCH_DISABLED".into(),
            message: format!("queue dispatch disabled (fail-closed): {reason}"),
        });
        self.wake.notify_one();
        warn!(error = %reason, "queue fail-closed: dispatch disabled");
    }

    fn publish_changed(&self, item_id: &str, state: QueueState) {
        self.bus.publish(Event::QueueChanged {
            item_id: QueueItemId::new(item_id),
            state: state.as_str().into(),
        });
    }

    fn record_dispatch_error(&self, code: &str) {
        *self
            .last_dispatch_error
            .lock()
            .expect("queue last error mutex poisoned") = Some(code.to_string());
    }

    // ---- dispatcher (one worker, concurrency = 1) ----

    async fn dispatcher_loop(self: Arc<Self>) {
        // Event-driven only: enqueue/edit/reorder/retry/resume/cancel and the
        // coordinator (EngineReady / SessionChanged / run terminals) all call
        // `wake.notify_one()`, and tokio::sync::Notify stores a permit when no
        // waiter is parked, so a notification between a state check and
        // `notified()` is never lost. No periodic DB polling: the dispatcher
        // performs a truth scan only when something it cares about happened
        // (PERFORMANCE.md — idle queue performs zero periodic DB reads).
        'worker: loop {
            if self.stopping.load(Ordering::SeqCst) || self.stopped.load(Ordering::SeqCst) {
                break;
            }
            if self
                .failed
                .read()
                .expect("queue failed mutex poisoned")
                .is_some()
            {
                break;
            }
            if self.paused.load(Ordering::SeqCst) {
                self.wait_for_wake().await;
                continue;
            }
            // Lightweight eligibility scan (PERFORMANCE.md): one fixed-size
            // keyset page is resident at a time — no payload/error fields and
            // no full remaining-queue materialization on every drain step.
            // The UNKNOWN workspace set is fetched ONCE for the whole paged
            // scan; the full item is loaded only after one candidate wins.
            let unknown_workspaces = match self.repo.unknown_workspaces() {
                Ok(u) => u,
                Err(e) => {
                    self.fail_closed(e);
                    break;
                }
            };
            let mut target: Option<String> = None;
            // A transient pre-mutation dependency error was seen during this
            // scan and nothing else was dispatchable: sleep one bounded
            // backoff instead of re-scanning immediately (no spin).
            let mut saw_transient = false;
            let mut after = None;
            loop {
                let candidates = match self.repo.list_candidate_page(after.as_ref()) {
                    Ok(candidates) => candidates,
                    Err(error) => {
                        self.fail_closed(error);
                        break 'worker;
                    }
                };
                if candidates.is_empty() {
                    break;
                }
                for item in &candidates {
                    // Eligibility: engine ready FOR THIS WORKSPACE (a healthy
                    // engine bound elsewhere waits for explicit restart —
                    // never FAILED/auto-rebind, TASK 24 §9); workspace not
                    // ambiguity-blocked; existing session resolvable and not
                    // busy. First eligible wins (§54–§55).
                    if !matches!(
                        self.port
                            .engine_state_for_workspace(&item.engine_id, &item.workspace_id),
                        EngineState::Ready
                    ) {
                        continue;
                    }
                    // TASK 23 §50: an UNKNOWN item in this workspace blocks
                    // further mutating queued dispatch. Other workspaces
                    // continue through later keyset pages (§51).
                    if unknown_workspaces.contains(&item.workspace_id) {
                        continue;
                    }
                    if item.session_mode == SessionMode::Existing {
                        let Some(sid) = item.session_id.clone() else {
                            continue;
                        };
                        match self.port.ensure_session(&sid).await {
                            Ok(()) => {}
                            Err(PortError::SessionNotFound(_)) => {
                                match self.repo.mark_failed_queued(
                                    &item.id,
                                    "session_not_found",
                                    "target session no longer exists",
                                ) {
                                    Ok(true) => {
                                        self.publish_changed(&item.id, QueueState::Failed);
                                        self.record_dispatch_error("session_not_found");
                                    }
                                    Ok(false) => {}
                                    Err(e) => {
                                        self.fail_closed(e);
                                        return;
                                    }
                                }
                                continue;
                            }
                            Err(_) => {
                                // Transient dependency error: do not spin on
                                // the next scan — one bounded backoff below.
                                saw_transient = true;
                                continue;
                            }
                        }
                        if self.port.session_busy(&sid) {
                            continue;
                        }
                    }
                    target = Some(item.id.clone());
                    break;
                }
                if target.is_some() || candidates.len() < DISPATCH_CANDIDATE_PAGE_SIZE {
                    break;
                }
                after = candidates.last().cloned();
            }
            let Some(item_id) = target else {
                if saw_transient {
                    if !self.backoff_sleep(TRANSIENT_RETRY_BACKOFF).await {
                        break; // shutdown during backoff
                    }
                } else {
                    self.wait_for_wake().await;
                }
                continue;
            };
            // Pause barrier right before the claim (§92): the claim itself is
            // also atomically gated on the pause flag, so a pause commit and
            // a claim racing each other resolve deterministically — a claim
            // may only begin before the pause commit.
            if self.paused.load(Ordering::SeqCst) {
                self.wait_for_wake().await;
                continue;
            }
            // Atomic claim (§15–§16): exactly one owner.
            match self.repo.claim(&item_id) {
                Ok(true) => {}
                Ok(false) => continue, // claimed by another worker in the meantime
                Err(e) => {
                    self.fail_closed(e);
                    break;
                }
            }
            match self.dispatch_claimed(&item_id).await {
                Ok(DispatchOutcome::Done) => {}
                Ok(DispatchOutcome::Wait) => {}
                Ok(DispatchOutcome::Backoff(delay)) => {
                    if !self.backoff_sleep(delay).await {
                        break; // shutdown during backoff
                    }
                }
                Err(e) => {
                    self.fail_closed_str(e);
                    break;
                }
            }
        }
        self.worker_alive.store(false, Ordering::SeqCst);
    }

    /// Dispatch one leased item through the handoff boundary.
    async fn dispatch_claimed(&self, item_id: &str) -> Result<DispatchOutcome, String> {
        let Some(item) = self.repo.get(item_id).map_err(|e| e.to_string())? else {
            return Ok(DispatchOutcome::Done); // deleted under us
        };
        if item.state != QueueState::Leased {
            return Ok(DispatchOutcome::Done); // concurrently cancelled/removed
        }
        let Some(lease_id) = item.lease_id.clone() else {
            return Err("leased item without lease token (corrupt row)".into());
        };
        // Honored cancel intent from before the claim took effect.
        if self
            .repo
            .is_cancel_requested(item_id)
            .map_err(|e| e.to_string())?
        {
            self.repo
                .cancel_from_intent(item_id, &lease_id)
                .map_err(|e| e.to_string())?;
            self.publish_changed(item_id, QueueState::Cancelled);
            self.wake.notify_one();
            return Ok(DispatchOutcome::Done);
        }
        // Re-check eligibility under the lease (engine may have died or been
        // rebound to another workspace between the scan and the claim). A
        // healthy engine bound elsewhere is NotReady for this item → Wait
        // (explicit restart/rebind required, never FAILED).
        if !matches!(
            self.port
                .engine_state_for_workspace(&item.engine_id, &item.workspace_id),
            EngineState::Ready
        ) {
            self.repo
                .release_lease(item_id, &lease_id)
                .map_err(|e| e.to_string())?;
            return Ok(DispatchOutcome::Wait);
        }
        // Session resolution.
        let session_id = match self.resolve_session(&item).await {
            Ok(sid) => sid,
            Err(SessionStep::Wait) => {
                self.repo
                    .release_lease(item_id, &lease_id)
                    .map_err(|e| e.to_string())?;
                return Ok(DispatchOutcome::Wait);
            }
            Err(SessionStep::Backoff(msg)) => {
                // Transient pre-mutation dependency error: release the lease
                // and schedule one bounded cancellable backoff — never a
                // claim→fail→rescan spin.
                self.repo
                    .release_lease(item_id, &lease_id)
                    .map_err(|e| e.to_string())?;
                warn!(
                    item_id,
                    error = %msg,
                    "queue transient dependency error; bounded retry backoff"
                );
                return Ok(DispatchOutcome::Backoff(TRANSIENT_RETRY_BACKOFF));
            }
            Err(SessionStep::Fail(code, msg)) => {
                self.repo
                    .mark_failed_leased(item_id, &code, &msg)
                    .map_err(|e| e.to_string())?;
                self.record_dispatch_error(&code);
                self.publish_changed(item_id, QueueState::Failed);
                self.bus.publish(Event::QueueDispatchFailed {
                    item_id: QueueItemId::new(item_id),
                    error: code.clone(),
                });
                self.wake.notify_one();
                return Ok(DispatchOutcome::Done);
            }
            // The session-create boundary crossed but creation cannot be
            // proven: the item must NOT auto-wait/retry (that would loop-create
            // orphan upstream sessions). It becomes UNKNOWN — blocked,
            // manual/recovery state (TASK 24 §9). No run exists yet, so no
            // run_id is persisted.
            Err(SessionStep::Ambiguous(msg)) => {
                self.repo
                    .mark_unknown_leased(item_id, None, "create_unknown", &msg)
                    .map_err(|e| e.to_string())?;
                self.record_dispatch_error("create_unknown");
                self.publish_changed(item_id, QueueState::Unknown);
                self.bus.publish(Event::RuntimeWarning {
                    code: "QUEUE_OUTCOME_UNKNOWN".into(),
                    message: format!(
                        "queue item {item_id}: session creation outcome unknown (the engine may have created the session); automatic retry disabled to avoid duplicate sessions"
                    ),
                });
                self.wake.notify_one();
                return Ok(DispatchOutcome::Done);
            }
        };
        // Durably enter the sending phase BEFORE the send: from here the
        // crash window is ambiguous (§27).
        if !self
            .repo
            .begin_send(item_id, &session_id)
            .map_err(|e| e.to_string())?
        {
            return Ok(DispatchOutcome::Done); // state changed concurrently
        }
        if self.stopping.load(Ordering::SeqCst) {
            // In-process shutdown before the send: safely release keeping the
            // session reference (no external side effect happened).
            self.repo
                .release_after_interrupt(item_id, &lease_id)
                .map_err(|e| e.to_string())?;
            return Ok(DispatchOutcome::Done);
        }
        if self
            .repo
            .is_cancel_requested(item_id)
            .map_err(|e| e.to_string())?
        {
            self.repo
                .cancel_from_intent(item_id, &lease_id)
                .map_err(|e| e.to_string())?;
            self.publish_changed(item_id, QueueState::Cancelled);
            self.wake.notify_one();
            return Ok(DispatchOutcome::Done);
        }
        // Failpoint: crash between the durable sending-phase commit and the
        // engine call (LEASED + sending + no run_id on restart → ambiguous).
        #[cfg(feature = "failpoints")]
        {
            let hook = self
                .hooks
                .lock()
                .expect("queue hooks mutex poisoned")
                .before_send
                .clone();
            if let Some(hook) = hook {
                hook().await;
            }
        }
        // Re-check the cancel intent immediately before the send: a cancel
        // issued while the handoff window was open must still be honored
        // without ever reaching the engine (§46, §63).
        if self
            .repo
            .is_cancel_requested(item_id)
            .map_err(|e| e.to_string())?
        {
            self.repo
                .cancel_from_intent(item_id, &lease_id)
                .map_err(|e| e.to_string())?;
            self.publish_changed(item_id, QueueState::Cancelled);
            self.wake.notify_one();
            return Ok(DispatchOutcome::Done);
        }
        // The send is the authoritative acceptance evidence (§24): DISPATCHED
        // is committed ONLY on `Accepted` — never from a local RunId or
        // MessageStarted (TASK 24 §9).
        let receipt = match self
            .port
            .send(&session_id, &item.payload, item.model.as_deref())
            .await
        {
            Ok(receipt) => receipt,
            Err(e) => {
                // CORE-009: proven pre-accept environmental failures can be safely requeued
                // without burning attempts or causing ambiguous session state.
                if matches!(e, PortError::SessionBusy(_) | PortError::EngineUnavailable(_)) {
                    self.repo
                        .release_after_interrupt(item_id, &lease_id)
                        .map_err(|e| e.to_string())?;
                    self.publish_changed(item_id, QueueState::Queued);
                    self.wake.notify_one();
                    return Ok(DispatchOutcome::Done);
                }

                // A definite error (engine not ready / rejected before the
                // boundary) that is NOT a transient environmental state:
                // terminal for this attempt, never auto-requeue (§147–§149).
                let code = e.code();
                self.repo
                    .mark_failed_leased(item_id, code, e.message())
                    .map_err(|e| e.to_string())?;
                self.record_dispatch_error(code);
                self.publish_changed(item_id, QueueState::Failed);
                self.bus.publish(Event::QueueDispatchFailed {
                    item_id: QueueItemId::new(item_id),
                    error: code.into(),
                });
                self.wake.notify_one();
                return Ok(DispatchOutcome::Done);
            }
        };
        let run_id = match receipt {
            DispatchReceipt::Accepted { run_id } => run_id,
            DispatchReceipt::DefinitelyRejected { run_id: _, code, message } => {
                self.repo
                    .mark_failed_leased(item_id, &code, &message)
                    .map_err(|e| e.to_string())?;
                self.record_dispatch_error(&code);
                self.publish_changed(item_id, QueueState::Failed);
                self.bus.publish(Event::QueueDispatchFailed {
                    item_id: QueueItemId::new(item_id),
                    error: code,
                });
                self.wake.notify_one();
                return Ok(DispatchOutcome::Done);
            }
            // The send crossed the boundary but acceptance cannot be proven
            // (transport loss / engine death): UNKNOWN — never auto-redispatch,
            // workspace stays blocked until explicit user resolution. The
            // known run_id is PERSISTED and correlated in the run index so a
            // later authoritative terminal can reconcile this item (TASK 24
            // §9) — correlation is never guessed.
            DispatchReceipt::OutcomeUnknown { run_id, message } => {
                self.repo
                    .mark_unknown_leased(item_id, Some(&run_id), "dispatch_unknown", &message)
                    .map_err(|e| e.to_string())?;
                let engine_id = item.engine_id.clone();
                self.run_index
                    .write()
                    .expect("queue run index mutex poisoned")
                    .insert(run_id.clone(), (item_id.to_string(), engine_id));
                self.record_dispatch_error("dispatch_unknown");
                self.publish_changed(item_id, QueueState::Unknown);
                self.bus.publish(Event::RuntimeWarning {
                    code: "QUEUE_OUTCOME_UNKNOWN".into(),
                    message: format!(
                        "queue item {item_id}: send outcome unknown (external acceptance cannot be proven); automatic retry disabled to avoid duplicate work"
                    ),
                });
                self.wake.notify_one();
                return Ok(DispatchOutcome::Done);
            }
        };
        // Failpoint: crash after engine acceptance before the durable
        // DISPATCHED commit (LEASED + sending + run may exist upstream).
        #[cfg(feature = "failpoints")]
        {
            let hook = self
                .hooks
                .lock()
                .expect("queue hooks mutex poisoned")
                .after_send
                .clone();
            if let Some(hook) = hook {
                hook().await;
            }
        }
        match self
            .repo
            .mark_dispatched(item_id, &run_id)
            .map_err(|e| e.to_string())?
        {
            true => {
                let engine_id = item.engine_id.clone();
                self.run_index
                    .write()
                    .expect("queue run index mutex poisoned")
                    .insert(run_id.clone(), (item_id.to_string(), engine_id));
                self.publish_changed(item_id, QueueState::Dispatched);
                self.bus.publish(Event::QueueDispatchStarted {
                    item_id: QueueItemId::new(item_id),
                });
                // Cancel intent that landed while the send was in flight: the
                // run is live and tracked — cancel it; the terminal event
                // transitions the item (no contradictory state, §46).
                if self
                    .repo
                    .is_cancel_requested(item_id)
                    .map_err(|e| e.to_string())?
                {
                    if let Err(error) = self.port.cancel(&session_id, &run_id).await {
                        // The user's durable intent is still authoritative and
                        // the tracked row stays DISPATCHED. Surface delivery
                        // failure so cancellation can be retried; never claim
                        // a terminal state the engine did not prove.
                        self.bus.publish(Event::RuntimeWarning {
                            code: "QUEUE_CANCEL_DELIVERY_FAILED".into(),
                            message: format!(
                                "queue item {item_id}: handoff cancellation was not delivered ({}); the run remains active and cancellation may be retried",
                                error.code()
                            ),
                        });
                    }
                }
            }
            false => {
                // Row changed under us (cancel intent honored / removed). The
                // run is live but untracked: cancel it best-effort so the
                // user's intent is honored and nothing dangles.
                self.bus.publish(Event::RuntimeWarning {
                    code: "QUEUE_UNTRACKED_RUN".into(),
                    message: "run accepted but queue row changed during handoff; cancellation recovery is required".into(),
                });
                if self
                    .repo
                    .is_cancel_requested(item_id)
                    .map_err(|e| e.to_string())?
                {
                    self.port.cancel(&session_id, &run_id).await.map_err(|error| {
                        format!(
                            "accepted untracked run {run_id} for queue item {item_id}: cancellation delivery failed ({}); dispatch disabled with the durable row left non-terminal",
                            error.code()
                        )
                    })?;
                    match self.repo.cancel_from_intent(item_id, &lease_id) {
                        Ok(true) => {
                            self.publish_changed(item_id, QueueState::Cancelled);
                            self.wake.notify_one();
                        }
                        Ok(false) => {
                            return Err(format!(
                                "accepted untracked run {run_id} for queue item {item_id}: cancellation was delivered but the durable terminal transition lost its state/lease guard; dispatch disabled"
                            ));
                        }
                        Err(error) => {
                            return Err(format!(
                                "accepted untracked run {run_id} for queue item {item_id}: cancellation was delivered but the durable terminal transition failed ({error}); dispatch disabled"
                            ));
                        }
                    }
                }
            }
        }
        // Concurrency = 1: wait for THIS run's terminal before the next item
        // (§56–§57, §206). Bounded backstop re-checks; cancel wakes us.
        self.wait_for_terminal(item_id).await;
        Ok(DispatchOutcome::Done)
    }

    async fn resolve_session(&self, item: &QueueItem) -> Result<String, SessionStep> {
        match item.session_mode {
            SessionMode::Existing => {
                let Some(sid) = item.session_id.clone() else {
                    return Err(SessionStep::Fail(
                        "invalid".into(),
                        "existing-session item without session_id".into(),
                    ));
                };
                match self.port.ensure_session(&sid).await {
                    Ok(()) => {}
                    Err(PortError::SessionNotFound(_)) => {
                        return Err(SessionStep::Fail(
                            "session_not_found".into(),
                            "target session no longer exists".into(),
                        ));
                    }
                    // State conditions park on the event Notify (EngineReady /
                    // session-terminal events wake the worker).
                    Err(PortError::SessionBusy(_)) | Err(PortError::EngineUnavailable(_)) => {
                        return Err(SessionStep::Wait)
                    }
                    // Transient dependency errors get one bounded backoff,
                    // never a spin (TASK 24 §9).
                    Err(e) => return Err(SessionStep::Backoff(e.message().into())),
                }
                if self.port.session_busy(&sid) {
                    return Err(SessionStep::Wait);
                }
                Ok(sid)
            }
            SessionMode::New => {
                // Reuse a session reference from a previous interrupted
                // dispatch when it still exists (§59–§60); otherwise create.
                if let Some(existing) = &item.session_id {
                    match self.port.ensure_session(existing).await {
                        Ok(()) => {
                            if self.port.session_busy(existing) {
                                return Err(SessionStep::Wait);
                            }
                            return Ok(existing.clone());
                        }
                        Err(PortError::SessionNotFound(_)) => { /* stale: create fresh */ }
                        Err(PortError::SessionBusy(_)) | Err(PortError::EngineUnavailable(_)) => {
                            return Err(SessionStep::Wait)
                        }
                        Err(e) => return Err(SessionStep::Backoff(e.message().into())),
                    }
                }
                let outcome = match self
                    .port
                    .create_session(
                        &item.engine_id,
                        Some(&item.workspace_id),
                        item.model.as_deref(),
                    )
                    .await
                {
                    Ok(o) => o,
                    // State conditions park on the event Notify.
                    Err(PortError::EngineUnavailable(_)) | Err(PortError::SessionBusy(_)) => {
                        return Err(SessionStep::Wait)
                    }
                    // Transient dependency errors get one bounded backoff,
                    // never a spin (TASK 24 §9).
                    Err(PortError::Network(_)) => {
                        return Err(SessionStep::Backoff("network error creating session".into()))
                    }
                    Err(e) => {
                        return Err(SessionStep::Fail(e.code().into(), e.message().into()))
                    }
                };
                let sid = match outcome {
                    // Authoritative creation only. An ambiguous create may
                    // have produced an upstream session — the item must NOT
                    // auto-wait/retry into an orphan-session loop; it becomes
                    // UNKNOWN (TASK 24 §9).
                    SessionCreateOutcome::Created { session_id } => session_id,
                    SessionCreateOutcome::DefinitelyNotCreated { code, message } => {
                        return Err(SessionStep::Fail(code, message))
                    }
                    SessionCreateOutcome::CreationUnknown { message } => {
                        return Err(SessionStep::Ambiguous(format!(
                            "session creation outcome unknown: {message}"
                        )))
                    }
                };
                // Persist the created session identity IMMEDIATELY (durable
                // side effect exists): the next wake reuses it instead of
                // creating another external session, and a crash in this
                // window can never classify the creation as side-effect-free
                // (TASK 24 §9).
                match self.repo.persist_session_created(&item.id, &sid) {
                    Ok(true) => {
                        if self.port.session_busy(&sid) {
                            return Err(SessionStep::Wait);
                        }
                        Ok(sid)
                    }
                    Ok(false) => {
                        // Row changed under us (cancelled/removed) — the
                        // external session was created but no row references
                        // it: an orphan. Compensate best-effort (the row is
                        // gone; the scan drops the item either way), then
                        // Wait so the worker re-scans rather than dispatching
                        // into a dead row.
                        let _ = self.port.delete_session(&sid).await;
                        Err(SessionStep::Wait)
                    }
                    Err(e) => {
                        // CROSS-AUTHORITY DURABILITY FAILURE (TASK 24 §9):
                        // the upstream session is authoritatively created but
                        // the durable queue row could not record its
                        // identity. Ordinary Fail would let a manual/restart
                        // retry create ANOTHER external session. Compensate
                        // with an authoritative delete first.
                        match self.port.delete_session(&sid).await {
                            Ok(()) => {
                                // Cleanup proven: safe to fail the item (no
                                // orphan remains upstream).
                                Err(SessionStep::Fail(
                                    "persist_session_failed".into(),
                                    format!(
                                        "session was created upstream but could not be persisted ({e}); authoritative cleanup succeeded"
                                    ),
                                ))
                            }
                            Err(cleanup_err) => {
                                // Cleanup unsupported/failed: the upstream
                                // session may still exist. Fail closed as
                                // UNKNOWN — never auto/manual retry as a
                                // clean NewSession until explicitly resolved
                                // (TASK 24 §9).
                                Err(SessionStep::Ambiguous(format!(
                                    "session '{sid}' was created upstream but could not be persisted ({e}); authoritative cleanup also failed ({})\u{2014} automatic retry disabled to avoid duplicate sessions",
                                    cleanup_err.message()
                                )))
                            }
                        }
                    }
                }
            }
        }
    }

    /// Wait until the just-dispatched run reaches a terminal state (or the
    /// worker is stopping). The coordinator performs the DB transition and
    /// signals `terminal_notify`; cancel and shutdown paths signal it too
    /// (PERFORMANCE.md — no periodic DB polling). Permit semantics make the
    /// check-then-wait loop wakeup-safe.
    ///
    /// Fail-closed (TASK 24 §9): a missing DISPATCHED row is an invariant
    /// violation and a storage error is a durability failure — neither may
    /// fabricate a terminal state, or the next queue item could start while
    /// the previous external run is still active.
    async fn wait_for_terminal(&self, item_id: &str) {
        loop {
            if self.stopping.load(Ordering::SeqCst) || self.stopped.load(Ordering::SeqCst) {
                return;
            }
            match self.repo.get(item_id) {
                Ok(Some(item)) => {
                    if item.state != QueueState::Dispatched {
                        return; // authoritative terminal reached
                    }
                }
                Ok(None) => {
                    // The item we just dispatched vanished from the durable
                    // queue. Do NOT pretend it was cancelled: the external
                    // run may still be live. Fail closed instead.
                    self.fail_closed_str(format!(
                        "dispatched item {item_id} vanished from the durable queue (invariant violation); dispatch disabled"
                    ));
                    return;
                }
                Err(e) => {
                    self.fail_closed(e);
                    return;
                }
            }
            let notify = self.terminal_notify.clone();
            notify.notified().await;
        }
    }

    async fn wait_for_wake(&self) {
        // Permit-semantics Notify: a notify_one that races this await is
        // stored and the next notified() returns immediately — the
        // check-then-wait pattern is wakeup-safe (tokio::sync::Notify).
        self.wake.notified().await;
    }

    /// Bounded cancellable backoff: sleeps `delay`, polling the shutdown
    /// flags on a short tick so pause/shutdown cancels the retry promptly
    /// (TASK 24 §9 — a transient dependency error never spins and never
    /// outlives shutdown). Returns false when the worker should stop.
    async fn backoff_sleep(&self, delay: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + delay;
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.stopping.load(Ordering::SeqCst) || self.stopped.load(Ordering::SeqCst) {
                return false;
            }
            if tokio::time::Instant::now() >= deadline {
                return true;
            }
            tick.tick().await;
        }
    }

    // ---- coordinator (run terminal correlation, §120–§121) ----

    async fn coordinator_loop(self: Arc<Self>) {
        // State-only subscription: the coordinator correlates run terminals
        // and engine/session state; stream deltas can neither wake it nor
        // lag its bounded buffer (PERFORMANCE.md).
        let mut events = self.bus.subscribe_state();
        loop {
            if self.stopping.load(Ordering::SeqCst) {
                let empty = self
                    .run_index
                    .read()
                    .expect("queue run index mutex poisoned")
                    .is_empty();
                if empty {
                    break;
                }
                let since = self.stopping_since.lock().expect("stopping mutex poisoned");
                if since.map(|s| s.elapsed() > DRAIN_TIMEOUT).unwrap_or(false) {
                    drop(since);
                    self.force_fail_all_tracked();
                    break;
                }
            }
            // The bounded bus reports `Lagged` for a slow consumer. Lagged is
            // NOT terminal for this correctness-critical consumer: on Lagged
            // the coordinator re-scans authoritative state (the DB is truth;
            // events are hints) and continues. Only `Closed` ends the loop
            // (TASK 24 §9).
            let env = if self.stopping.load(Ordering::SeqCst) {
                match tokio::time::timeout(Duration::from_millis(200), events.recv()).await {
                    Ok(Ok(env)) => Some(env),
                    Ok(Err(SubscribeError::Lagged(skipped))) => {
                        // Lagged is NOT terminal: reconcile every still-tracked
                        // run from authoritative state. The missed terminals
                        // cannot be reconstructed, so each becomes UNKNOWN
                        // (non-releasing) with its run correlation retained —
                        // a missed terminal must never leave a row indefinitely
                        // DISPATCHED (TASK 24 §9).
                        tracing::warn!(skipped, "queue coordinator lagged; reconciling tracked runs");
                        self.reconcile_lagged_tracked_runs();
                        None
                    }
                    Ok(Err(SubscribeError::Closed)) => break, // bus closed
                    Err(_) => None,                            // bounded drain tick
                }
            } else {
                match events.recv().await {
                    Ok(env) => Some(env),
                    Err(SubscribeError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "queue coordinator lagged; reconciling tracked runs");
                        self.reconcile_lagged_tracked_runs();
                        None
                    }
                    Err(SubscribeError::Closed) => break,
                }
            };
            if let Some(env) = env {
                if !self.handle_event(&mut events, env).await {
                    break;
                }
            }
        }
    }

    /// Returns false when the loop should exit. `EngineFailed` runs a bounded
    /// inline drain (processing incoming events without recursion) before
    /// force-failing any tracked run of that engine that never reached a
    /// terminal state.
    async fn handle_event(
        &self,
        events: &mut saiwork_events::Subscription,
        env: saiwork_events::Envelope,
    ) -> bool {
        match env.event {
            Event::EngineFailed { engine_id, .. } => {
                let engine = engine_id.as_str().to_string();
                if !self.stopping.load(Ordering::SeqCst) {
                    let tracked: Vec<(String, String)> = self
                        .run_index
                        .read()
                        .expect("queue run index mutex poisoned")
                        .iter()
                        .filter(|(_, (_, e))| e.as_str() == engine)
                        .map(|(rid, (iid, _))| (rid.clone(), iid.clone()))
                        .collect();
                    if !tracked.is_empty() {
                        let deadline = Instant::now() + ENGINE_FAIL_DRAIN;
                        loop {
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            let still = self
                                .run_index
                                .read()
                                .expect("queue run index mutex poisoned")
                                .iter()
                                .filter(|(rid, _)| {
                                    tracked
                                        .iter()
                                        .any(|(trid, _)| trid.as_str() == rid.as_str())
                                })
                                .count();
                            if still == 0 || remaining.is_zero() {
                                break;
                            }
                            match tokio::time::timeout(remaining, events.recv()).await {
                                Ok(Ok(ev)) => {
                                    if !self.process_event(ev.event) {
                                        return false;
                                    }
                                }
                                // Lagged during the drain: missed terminals
                                // for tracked runs are conservatively
                                // force-failed below (engine already failed).
                                Ok(Err(_)) => break,
                                Err(_) => break,
                            }
                        }
                        for (rid, _iid) in &tracked {
                            if self
                                .run_index
                                .read()
                                .expect("queue run index mutex poisoned")
                                .contains_key(rid.as_str())
                            {
                                // The engine failed but the external run's
                                // outcome is UNPROVEN: it may still be mutating
                                // the workspace, so ownership is NOT released.
                                // Transition to UNKNOWN (non-releasing) and
                                // retain the exact run correlation so a later
                                // authoritative terminal (or proven death) can
                                // reconcile — never a definitive FAILED (TASK
                                // 24 §9).
                                self.on_terminal(
                                    rid.as_str(),
                                    QueueState::Unknown,
                                    Some("engine_lost"),
                                    Some("engine failed before the run reached a terminal state (outcome unknown)"),
                                );
                            }
                        }
                    }
                }
                self.wake.notify_one();
                true
            }
            other => self.process_event(other),
        }
    }

    /// Synchronous event processing (no recursion): run terminals, wake hints.
    fn process_event(&self, event: Event) -> bool {
        match event {
            Event::AppStopping { .. } => {
                // W2-002: the coordinator must STAY ALIVE through shutdown to
                // observe the terminals of active runs while engines stop —
                // `shutdown_barrier` and the drain loop in `coordinator_loop`
                // own the decision to exit. Returning false here would kill the
                // coordinator on the very first AppStopping and drop tracked-run
                // correlation, leaving rows stuck DISPATCHED (or force-failed by
                // the drain timeout). Mirror `shutdown_barrier` so the bounded
                // drain begins immediately, then keep the loop alive.
                self.stopping.store(true, Ordering::SeqCst);
                let mut since = self
                    .stopping_since
                    .lock()
                    .expect("stopping mutex poisoned");
                if since.is_none() {
                    *since = Some(Instant::now());
                }
                drop(since);
                self.wake.notify_one();
                self.terminal_notify.notify_one();
                self.stop_signal.notify_one();
                true
            }
            Event::MessageCompleted { run_id, .. } => {
                self.on_terminal(run_id.as_str(), QueueState::Done, None, None);
                true
            }
            Event::MessageFailed { run_id, error, .. } => {
                self.on_terminal(
                    run_id.as_str(),
                    QueueState::Failed,
                    Some("run_failed"),
                    Some(error.as_str()),
                );
                true
            }
            // The engine accepted the run but its terminal cannot be proven
            // (transport loss / runtime death): UNKNOWN, never a plain FAILED
            // — the external work may still be live (TASK 24 §9).
            Event::MessageOutcomeUnknown {
                run_id, error, ..
            } => {
                self.on_terminal(
                    run_id.as_str(),
                    QueueState::Unknown,
                    Some("outcome_unknown"),
                    Some(error.as_str()),
                );
                true
            }
            Event::MessageCancelled { run_id, .. } => {
                self.on_terminal(
                    run_id.as_str(),
                    QueueState::Cancelled,
                    Some("cancelled"),
                    None,
                );
                true
            }
            Event::SessionChanged { .. } | Event::EngineReady { .. } => {
                self.wake.notify_one();
                true
            }
            _ => true,
        }
    }

    /// Transition a tracked run to its terminal. Guarded on DISPATCHED +
    /// run_id for the ordinary flow; an UNKNOWN row with a MATCHING run_id
    /// may transition only on a definitive terminal (completed/failed/
    /// cancelled) — unrelated/stale terminals are ignored, and the exact
    /// persisted run correlation is retained across restart (TASK 24 §9).
    fn on_terminal(&self, run_id: &str, state: QueueState, code: Option<&str>, msg: Option<&str>) {
        let entry = self
            .run_index
            .read()
            .expect("queue run index mutex poisoned")
            .get(run_id)
            .cloned();
        let Some((item_id, _)) = entry else {
            return; // unknown / untracked run (direct send, external activity)
        };
        // First the DISPATCHED guard (normal flow), then the UNKNOWN guard
        // (a row that already became UNKNOWN can still be reconciled by its
        // exact run). An `Unknown` target state never transitions an UNKNOWN
        // row (no-op — the row is already unknown; there is nothing to prove).
        let result = match self.repo.mark_terminal(&item_id, run_id, state, code, msg) {
            Ok(true) => Ok(true),
            Ok(false) => self.repo.mark_terminal_unknown(&item_id, run_id, state, code, msg),
            Err(e) => Err(e),
        };
        match result {
            Ok(true) => {
                // A definitive terminal removes the correlation. An UNKNOWN
                // transition (row DISPATCHED → UNKNOWN) KEEPS it so a later
                // authoritative terminal can still reconcile the ambiguous
                // run (TASK 24 §9).
                if !matches!(state, QueueState::Unknown) {
                    self.run_index
                        .write()
                        .expect("queue run index mutex poisoned")
                        .remove(run_id);
                }
                self.publish_changed(&item_id, state);
                match state {
                    QueueState::Done => {
                        self.bus.publish(Event::QueueDispatchCompleted {
                            item_id: QueueItemId::new(&item_id),
                        });
                    }
                    QueueState::Failed => {
                        self.bus.publish(Event::QueueDispatchFailed {
                            item_id: QueueItemId::new(&item_id),
                            error: code.unwrap_or("run_failed").into(),
                        });
                    }
                    _ => {}
                }
                self.terminal_notify.notify_one();
                self.wake.notify_one();
            }
            Ok(false) => {
                // Stale/duplicate terminal (retried item with a new run, a
                // different run on an UNKNOWN row, or a duplicate event):
                // drop the association, keep the row truth.
                self.run_index
                    .write()
                    .expect("queue run index mutex poisoned")
                    .remove(run_id);
            }
            Err(e) => self.fail_closed(e),
        }
    }

    /// On EventBus lag the missed terminals cannot be reconstructed, so every
    /// still-tracked (DISPATCHED) run is conservatively reconciled to UNKNOWN:
    /// the run may still be mutating the workspace, so its ownership is NOT
    /// released (non-releasing) and the exact run correlation is retained for a
    /// later authoritative terminal. A missed terminal must never leave a row
    /// indefinitely DISPATCHED (TASK 24 §9).
    fn reconcile_lagged_tracked_runs(&self) {
        // Snapshot the tracked run ids first: `on_terminal` mutates `run_index`
        // (it keeps the correlation for an UNKNOWN transition), so we must not
        // hold the read guard across the calls.
        let tracked: Vec<String> = self
            .run_index
            .read()
            .expect("queue run index mutex poisoned")
            .keys()
            .cloned()
            .collect();
        for rid in tracked {
            self.on_terminal(
                &rid,
                QueueState::Unknown,
                Some("bus_lagged"),
                Some("event bus lagged; run terminal unprovable; workspace blocked until reconciliation"),
            );
        }
    }

    fn force_fail_all_tracked(&self) {
        let tracked: Vec<String> = self
            .run_index
            .read()
            .expect("queue run index mutex poisoned")
            .keys()
            .cloned()
            .collect();
        for rid in tracked {
            // Shutdown drain ended without an authoritative terminal: the
            // external run's outcome is UNPROVEN, so its workspace ownership
            // must NOT be released. Transition to UNKNOWN (non-releasing) and
            // retain the exact run correlation for later reconciliation — never
            // a definitive FAILED (TASK 24 §9).
            self.on_terminal(
                &rid,
                QueueState::Unknown,
                Some("engine_lost"),
                Some("shutdown ended before the run reached a terminal state (outcome unknown)"),
            );
        }
    }
}

enum SessionStep {
    /// State condition (engine not-ready / session busy): park on the event
    /// Notify, do not sleep.
    Wait,
    /// Transient pre-mutation dependency error (network hiccup, provider
    /// blip): one bounded cancellable backoff before re-scanning — never a
    /// spin.
    Backoff(String),
    Fail(String, String),
    /// The create boundary crossed but creation cannot be proven: never
    /// auto-retry (no orphan-session loops) — the item becomes UNKNOWN.
    Ambiguous(String),
}

#[cfg(feature = "failpoints")]
impl QueueManager {
    /// Test-only injection of dispatch failpoints (§85, §230). Feature-gated.
    #[doc(hidden)]
    pub fn set_dispatch_hooks_for_test(&self, hooks: DispatchHooks) {
        *self.hooks.lock().expect("queue hooks mutex poisoned") = hooks;
    }

    /// Test-only injection of repo failpoints (durability-failure tests).
    /// Feature-gated.
    #[doc(hidden)]
    pub fn set_repo_failpoints_for_test(&self, failpoints: crate::repo::RepoFailpoints) {
        self.repo.set_failpoints_for_test(failpoints);
    }
}
