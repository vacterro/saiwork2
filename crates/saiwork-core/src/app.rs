//! The application lifecycle authority (TASK 08, ARCHITECTURE.md).
//!
//! `App` is the single owner of application-level services: EventBus,
//! Storage, ProcessSupervisor, engine registry, diagnostics. It owns the
//! startup order, the shutdown sequence, and the application state machine.
//! It is a **coordinator**, not a god object: queue/SAIPEN/OpenCode logic
//! never lives here.
//!
//! Application states (valid transitions):
//!
//! ```text
//! BOOTING → READY            (bootstrap completed)
//! BOOTING → FAILED           (required init failed)
//! BOOTING → SHUTTING_DOWN    (shutdown requested during boot)
//! READY   → SHUTTING_DOWN
//! FAILED  → SHUTTING_DOWN
//! SHUTTING_DOWN → STOPPED    (cleanup complete)
//! ```
//!
//! Restart = a new OS process; `STOPPED → READY` is impossible.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use saiwork_events::{Event, EventBus, SubscribeError};
use saiwork_process::ProcessSupervisor;
use saiwork_storage::Db;
use serde::Serialize;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::engine::{EngineRegistry, PendingPermissionInfo};
use crate::error::CoreError;
use crate::queue_port::QueueEnginePort;
use crate::sessions::SessionManager;
use crate::workspace::{Workspace, WorkspaceManager};

/// Application lifecycle state (TASK 08 §5–§6). Serialized as the canonical
/// snake_case name; the frontend projects it read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AppState {
    /// Required services are being initialized.
    Booting,
    /// Required foundation services initialized; normal commands allowed.
    Ready,
    /// Shutdown requested; new work rejected; cleanup in progress.
    ShuttingDown,
    /// Cleanup complete; the process should exit.
    Stopped,
    /// Required init failed; the app never entered normal mode.
    Failed,
}

impl AppState {
    fn code(self) -> u8 {
        match self {
            AppState::Booting => 0,
            AppState::Ready => 1,
            AppState::ShuttingDown => 2,
            AppState::Stopped => 3,
            AppState::Failed => 4,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            0 => AppState::Booting,
            1 => AppState::Ready,
            2 => AppState::ShuttingDown,
            3 => AppState::Stopped,
            _ => AppState::Failed,
        }
    }

    /// True when the app accepts new work (READY only).
    pub fn accepts_work(self) -> bool {
        matches!(self, AppState::Ready)
    }
}

/// Valid transitions of the application state machine.
fn transition_allowed(from: AppState, to: AppState) -> bool {
    matches!(
        (from, to),
        (AppState::Booting, AppState::Ready)
            | (AppState::Booting, AppState::Failed)
            | (AppState::Booting, AppState::ShuttingDown)
            | (AppState::Ready, AppState::ShuttingDown)
            | (AppState::Failed, AppState::ShuttingDown)
            | (AppState::ShuttingDown, AppState::Stopped)
    )
}

/// Startup stage timings (TASK 08 §50). No SLA — baseline facts for the
/// TASK 09 audit and future performance tracking.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StartupTimings {
    /// Data root resolution + layout creation.
    pub data_root_ms: u64,
    /// DB open + migration + integrity preflight.
    pub storage_ms: u64,
    /// Service graph construction (bus, supervisor, registries, managers).
    pub services_ms: u64,
    /// Total bootstrap, from resolve to READY.
    pub total_ms: u64,
}

/// Outcome of one shutdown sequence (TASK 08 §26, §86).
#[derive(Debug, Clone, Serialize)]
pub struct ShutdownReport {
    /// Clean — no warnings AND no process required force termination;
    /// `CompletedWithWarnings` — cleanup succeeded with recorded warnings
    /// (which now also covers processes that required force termination, so a
    /// non-empty `forced_processes` can never co-exist with `outcome:
    /// "clean"`); `Failed` — something could not be cleaned.
    pub outcome: &'static str,
    /// Process ids whose exit remained unproven after the final force pass.
    /// Their live records stay owned by ProcessSupervisor for teardown retry.
    /// A non-empty list is surfaced as a warning, so it makes the outcome at
    /// least `completed_with_warnings` (W2-008/W2-005).
    pub forced_processes: Vec<String>,
    /// Bounded list of non-fatal cleanup warnings.
    pub warnings: Vec<String>,
    /// Duration of the shutdown sequence.
    pub shutdown_ms: u64,
}

impl ShutdownReport {
    fn clean(shutdown_ms: u64) -> Self {
        Self {
            outcome: "clean",
            forced_processes: Vec::new(),
            warnings: Vec::new(),
            shutdown_ms,
        }
    }

    fn finish(mut self, forced: &[String], mut warnings: Vec<String>) -> Self {
        self.forced_processes = forced.to_vec();
        warnings.retain(|w| !w.is_empty());
        // A non-empty forced list means the final force pass could not prove
        // exit — even with no storage warnings the shutdown is NOT clean,
        // otherwise `outcome` and `forced_processes` would contradict each
        // other. The supervisor retains each live record for teardown retry.
        if !forced.is_empty() {
            warnings.push(format!(
                "process exit remained unproven after final force pass: {}",
                forced.join(", ")
            ));
        }
        self.outcome = if warnings.is_empty() {
            "clean"
        } else {
            "completed_with_warnings"
        };
        self.warnings = warnings;
        self
    }
}

/// A snapshot of app state for the diagnostics panel (bounded by design).
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticsSnapshot {
    pub version: String,
    pub data_root: String,
    pub portable: bool,
    pub lifecycle: AppState,
    pub startup_ms: Option<StartupTimings>,
    pub last_shutdown_ms: Option<u64>,
    pub db_integrity: String,
    pub db_schema_version: i64,
    pub storage_status: String,
    /// Per-engine snapshot: identity + runtime health + capabilities
    /// (TASK 17 §115 — never secrets, never raw protocol state).
    pub engines: Vec<crate::engine::EngineInfo>,
    pub engine_count: usize,
    pub supervisor_active: usize,
    pub processes: Vec<saiwork_process::ProcessSnapshot>,
    pub workspaces: usize,
    pub sessions: usize,
    pub recent_errors: Vec<saiwork_diagnostics::ErrorRecord>,
    pub event_subscribers: usize,
    pub log_dir: Option<String>,
    pub log_fallback: bool,
    pub platform: String,
    pub architecture: String,
    pub timestamp_ms: i64,
}

/// Shared shutdown completion channel (TASK 08 §25, TASK 24 §9): the winning
/// shutdown caller publishes the final `ShutdownReport`; every concurrent
/// caller awaiting SHUTTING_DOWN observes the SAME report instead of
/// returning early — so a window-close can never `exit(0)` while the first
/// shutdown is still stopping queues/engines/processes/DB.
struct ShutdownHandle {
    tx: tokio::sync::watch::Sender<Option<ShutdownReport>>,
    rx: tokio::sync::watch::Receiver<Option<ShutdownReport>>,
}

pub struct App {
    pub config: AppConfig,
    pub bus: EventBus,
    pub db: Db,
    /// Shared owner of every child process. Arc so engines can reach it
    /// through `EngineStartContext` (TASK 10 §4: adapters spawn only here).
    pub supervisor: Arc<ProcessSupervisor>,
    pub diagnostics: Arc<saiwork_diagnostics::Diagnostics>,
    pub engines: Arc<EngineRegistry>,
    pub workspaces: WorkspaceManager,
    pub sessions: Arc<SessionManager>,
    /// The one durable queue authority (TASK 13, law 7).
    pub queue: Arc<saiwork_queue::QueueManager>,
    /// Existing-session enqueue and session deletion cross two authorities:
    /// the durable queue and SessionManager. App owns their shared critical
    /// section so an enqueue can neither slip past delete preflight nor target
    /// a session after its metadata was removed.
    session_queue_lock: tokio::sync::Mutex<()>,
    /// Read-only SAIPEN projection service (TASK 14): one watcher per
    /// attached workspace; SAIPEN remains the canonical authority.
    pub saipen: Arc<saiwork_saipen::SaipenService>,
    /// SAIPEN canonical action manager (TASK 15): invokes the canonical
    /// tool through the supervisor; never writes canonical files itself.
    pub saipen_actions: Arc<saiwork_saipen::ActionManager>,
    /// Application lifecycle state (authoritative, single source).
    state: AtomicU8,
    /// Monotonic generation for the active-workspace commit (CORE-001). Every
    /// `commit_active_workspace` carries the caller's selection epoch; an older
    /// epoch is ignored so a superseded selection can never persist its id
    /// after a newer one already committed (latest-wins across async IPC).
    active_selection_gen: AtomicU64,
    /// Serializes all active-workspace transitions (select, clear, close-of-active,
    /// forget-of-active). The generation check, durable pointer write, and watcher
    /// detach/attach must run as one atomic unit so concurrent generations cannot
    /// interleave side effects (CORE-002).
    active_workspace_lock: Mutex<()>,
    startup: RwLock<Option<StartupTimings>>,
    last_shutdown_ms: RwLock<Option<u64>>,
    logging: RwLock<crate::logging::LoggingInfo>,
    /// One shared shutdown completion per process lifetime (created lazily by
    /// the first caller). Every SHUTTING_DOWN caller awaits the same report.
    shutdown_handle: Mutex<Option<Arc<ShutdownHandle>>>,
    /// AUDIT-W2-002: exactly-once guard for the App-owned cleanup task. The
    /// sequence is spawned ONCE (the CAS winner merely arms it); aborting or
    /// panicking any individual CALLER can no longer strand canonical
    /// cleanup with a live never-publishing Sender.
    shutdown_task_started: Mutex<bool>,
    /// AUDIT-W2-002: weak back-pointer set by `construct_booting` so the
    /// cleanup task can own the App without keeping it alive forever.
    self_weak: std::sync::OnceLock<std::sync::Weak<App>>,
}

impl App {
    /// Bootstrap in the deterministic startup order (TASK 08 §8):
    ///
    /// ```text
    /// 1. resolve executable/application identity (AppConfig)
    /// 2. (single-instance authority — desktop shell, process-level)
    /// 3. resolve data root
    /// 4. (logging/diagnostics bootstrap — desktop shell, before bootstrap)
    /// 5. open Storage + migrations/preflight
    /// 6. EventBus
    /// 7. ProcessSupervisor (before any production spawn)
    /// 8. other foundation services
    /// 9. publish app.started
    /// 10. state → READY
    /// ```
    ///
    /// `bootstrap()` resolves the data root itself and is equivalent to
    /// `bootstrap_with(AppConfig::resolve()?)`. Use `bootstrap_with` when the
    /// shell already resolved the root (e.g. to init logging in between).
    pub fn bootstrap() -> Result<Arc<Self>, CoreError> {
        Self::bootstrap_with(AppConfig::resolve()?)
    }

    /// Bootstrap against an already-resolved config (startup order §8:
    /// resolve → logging → `bootstrap_with`).
    pub fn bootstrap_with(config: AppConfig) -> Result<Arc<Self>, CoreError> {
        let started = Instant::now();

        let t0 = Instant::now();
        config.ensure_layout()?;
        let data_root_ms = ms(t0);

        let t0 = Instant::now();
        let db = Db::open(&config.database_path())?;
        let storage_ms = ms(t0);
        info!(root = %config.data_root.display(), schema = db.version().unwrap_or(-1), "data root + storage ready");

        let t0 = Instant::now();
        let app = Self::construct_booting(config, db);
        let services_ms = ms(t0);

        // Queue recovery + worker startup: stale leases are recovered and
        // only then dispatch is enabled, before the app reaches READY
        // (TASK 13 §75–§77). A recovery failure is fail-closed.
        //
        // ALL fallible pre-READY initialization runs BEFORE any long-lived
        // task is spawned: a `queue.init()` failure must return Err with
        // zero surviving background ownership (no EventBus → SessionManager
        // → DB/EngineRegistry/ProcessSupervisor reference chains left alive
        // by a detached tracker task, TASK 24 §9).
        app.queue.init()?;

        // Session running-state is derived from the message stream, not set
        // by the UI (law 23). The task ends when it observes `app.stopping`
        // (canonical shutdown) or when the bus closes (process exit).
        // Spawned only after init succeeded — the last fallible step of
        // bootstrap, so a rollback never leaves it behind.
        //
        // The bounded bus reports `Lagged` for a slow consumer. Lagged is NOT
        // terminal: a fast delta flood must never kill the workspace-running
        // truth. On Lagged we reconcile every running flag against the
        // authoritative engine liveness and continue; only `Closed` ends the
        // task (TASK 24 §9).
        let running_tracker = app.sessions.clone();
        let tracker_bus = app.bus.clone();
        tokio::spawn(async move {
            // State-only subscription: the tracker needs run/engine state, not
            // stream deltas — a 10k-delta flood can neither wake it nor lag
            // its bounded buffer (PERFORMANCE.md).
            let mut sub = tracker_bus.subscribe_state();
            loop {
                let env = match sub.recv().await {
                    Ok(env) => env,
                    Err(SubscribeError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "running tracker lagged; reconciling from engines");
                        running_tracker.reconcile_running_from_engines();
                        continue;
                    }
                    Err(SubscribeError::Closed) => break,
                };
                if matches!(env.event, Event::AppStopping { .. }) {
                    break; // shutdown started: this subscription ends here
                }
                match &env.event {
                    Event::MessageStarted { session_id, run_id, .. } => {
                        // CORE-012: The event tracker MUST respect the run_id from MessageStarted
                        // to bind the reservation state to a specific run, preventing scheduler races.
                        running_tracker.note_started(session_id.as_str(), run_id.as_str());
                    }
                    // Authoritative terminals release the ordinary reservation
                    // and — ONLY when the run_id matches — any unknown
                    // reservation (TASK 24 §9). An unrelated terminal can
                    // never clear an ambiguous run.
                    Event::MessageCompleted {
                        session_id, run_id, ..
                    }
                    | Event::MessageFailed {
                        session_id, run_id, ..
                    }
                    | Event::MessageCancelled {
                        session_id, run_id, ..
                    } => {
                        running_tracker.note_terminal(session_id.as_str(), run_id.as_str());
                    }
                    // UNKNOWN is a NON-releasing reservation: the external run
                    // may still be live. Never clear it here — only a matching
                    // terminal, proven engine/process death, or an explicit
                    // risk-confirmed resolution may (TASK 24 §9).
                    Event::MessageOutcomeUnknown {
                        session_id, run_id, ..
                    } => {
                        running_tracker.note_outcome_unknown(session_id.as_str(), run_id.as_str());
                    }
                    _ => {}
                }
            }
        });

        let total_ms = ms(started);
        app.record_startup(StartupTimings {
            data_root_ms,
            storage_ms,
            services_ms,
            total_ms,
        });

        // Transition to READY only now — app.started is the record of
        // reaching READY (TASK 08 §49), not "main() ran".
        app.transition(AppState::Booting, AppState::Ready)
            .expect("bootstrap transitions BOOTING → READY by construction");
        app.bus.publish(Event::AppStarted {
            version: crate::config::APP_VERSION.into(),
        });
        info!(
            total_ms,
            data_root_ms, storage_ms, services_ms, "application ready"
        );
        // AUDIT-W2-002: record the weak self handle the App-owned shutdown
        // task uses to own cleanup independently of any single caller.
        let _ = app.self_weak.set(Arc::downgrade(&app));
        Ok(app)
    }

    /// Build the service graph in state BOOTING (no tracker, no `app.started`,
    /// no READY). Used by `bootstrap_with` and by lifecycle tests to exercise
    /// shutdown-from-boot. Not part of the stable API.
    #[doc(hidden)]
    pub fn construct_booting(config: AppConfig, db: Db) -> Arc<Self> {
        let bus = EventBus::new();
        let diagnostics = Arc::new(saiwork_diagnostics::Diagnostics::new());
        let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
        let engines = Arc::new(EngineRegistry::new(
            bus.clone(),
            diagnostics.clone(),
            supervisor.clone(),
        ));
        let workspaces = WorkspaceManager::new(db.clone(), bus.clone());
        let sessions = Arc::new(SessionManager::new(
            db.clone(),
            bus.clone(),
            engines.clone(),
        ));
        let queue = saiwork_queue::QueueManager::new(
            db.clone(),
            bus.clone(),
            Arc::new(QueueEnginePort::new(engines.clone(), sessions.clone())),
        );
        let saipen = saiwork_saipen::SaipenService::new(bus.clone());
        let saipen_actions = saiwork_saipen::ActionManager::new(bus.clone(), supervisor.clone());

        Arc::new(Self {
            config,
            bus,
            db,
            supervisor,
            diagnostics,
            engines,
            workspaces,
            sessions,
            queue,
            session_queue_lock: tokio::sync::Mutex::new(()),
            saipen,
            saipen_actions,
            state: AtomicU8::new(AppState::Booting.code()),
            active_selection_gen: AtomicU64::new(0),
            active_workspace_lock: Mutex::new(()),
            startup: RwLock::new(None),
            last_shutdown_ms: RwLock::new(None),
            logging: RwLock::new(crate::logging::LoggingInfo {
                log_dir: None,
                fallback: false,
            }),
            shutdown_handle: Mutex::new(None),
            shutdown_task_started: Mutex::new(false),
            self_weak: std::sync::OnceLock::new(),
        })
    }

    /// CAS on the application state with transition validation. Terminal
    /// states never leave; impossible transitions are a programmer error.
    pub fn transition(&self, from: AppState, to: AppState) -> Result<(), CoreError> {
        if !transition_allowed(from, to) {
            return Err(CoreError::Internal(format!(
                "invalid application state transition {from:?} → {to:?}"
            )));
        }
        let prev = self
            .state
            .compare_exchange(from.code(), to.code(), Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                CoreError::Internal(format!(
                    "application state changed concurrently: expected {from:?}"
                ))
            })?;
        info!(from = ?from, to = ?to, "application state");
        let _ = prev;
        Ok(())
    }

    /// Current application lifecycle state.
    pub fn state(&self) -> AppState {
        AppState::from_code(self.state.load(Ordering::SeqCst))
    }

    /// Guard: domain commands call this to reject work before READY or after
    /// shutdown began (TASK 08 §32, §64–§65). Never silently queued.
    pub fn require_ready(&self) -> Result<(), CoreError> {
        match self.state() {
            AppState::Ready => Ok(()),
            AppState::ShuttingDown | AppState::Stopped => Err(CoreError::ShuttingDown),
            AppState::Booting => Err(CoreError::NotReady),
            AppState::Failed => Err(CoreError::NotReady),
        }
    }

    /// Open a workspace and attach the SAIPEN read service to it. The
    /// SaipenService owns the `saipen.detected` transition (TASK 14 §52);
    /// the returned `Workspace.saipen` is the one-shot sidebar summary.
    pub async fn open_workspace(&self, path: &Path) -> Result<Workspace, CoreError> {
        let workspace = self.workspaces.open(path).await?;
        // CORE-001: opening/probing a workspace only registers it in recent
        // history — it MUST NOT transfer the current SAIPEN watcher. The watcher
        // (detach previous + attach new) is moved to `commit_active_workspace`,
        // which the frontend calls only AFTER the scoped session/SAIPEN reads
        // succeed. This keeps a failed selection from leaving the backend with B
        // watched while the durable/frontend selection is still A.
        Ok(workspace)
    }

    /// Begin a new active workspace selection epoch (W2-004).
    pub fn begin_active_workspace_selection(&self) -> u64 {
        self.active_selection_gen.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Commit a workspace as the exact current/active one (CORE-001). This is
    /// the SINGLE ownership boundary for active-workspace identity, watcher
    /// ownership and durable restore: it validates the id, detaches the
    /// previous current watcher, attaches the new one, and persists the exact
    /// active id — all as one ordered transition. Callers (the frontend
    /// `selectWorkspace`) invoke this only after the scoped reads succeed, so a
    /// failed selection never transfers the watcher.
    ///
    /// `gen` is the caller's selection epoch; a stale (older) epoch is ignored
    /// so a superseded selection cannot persist its id after a newer commit
    /// landed across async IPC (latest-wins). `id = None` clears the current
    /// workspace: detach the watcher and clear the durable pointer together.
    pub fn commit_active_workspace(
        &self,
        id: Option<&str>,
        gen: Option<u64>,
    ) -> Result<(), CoreError> {
        // CORE-006: reject active-workspace mutation after shutdown began.
        // The Tauri command layer also enforces this (require_ready), but the
        // App authority is canonical — a late IPC that bypasses the frontend
        // guard is still rejected here.
        self.require_ready()?;
        // CORE-002: all active-workspace transitions are serialized under a
        // single lock so concurrent generations cannot interleave the durable
        // pointer write with watcher detach/attach. The generation check/update
        // runs inside the lock so the latest-wins invariant holds across the
        // entire transition, not just the atomic load/store pair.
        let _guard = self
            .active_workspace_lock
            .lock()
            .expect("active workspace lock poisoned");
        // Latest-wins: ignore a superseded selection epoch.
        if let Some(g) = gen {
            let cur = self.active_selection_gen.load(Ordering::SeqCst);
            if g < cur {
                return Ok(());
            }
            self.active_selection_gen.store(g, Ordering::SeqCst);
        }
        match id {
            None => {
                // Clear: durable pointer + watcher detach under the lock
                // (CORE-007 — no recursive locking; shares the helper with
                // close_workspace).
                self.clear_active_workspace_locked()?;
            }
            Some(id) => {
                // Validate the id names a real workspace before touching anything.
                let path = self.workspaces.path_of(id)?;
                // Read the current active pointer (propagating errors).
                let prev = self.workspaces.get_active_workspace()?;
                // Commit the durable pointer FIRST; a storage failure preserves
                // the old watcher and pointer.
                self.workspaces.set_active_workspace(Some(id))?;
                // Only after the durable write succeeds, detach the old watcher
                // and attach the new one.
                if let Some(ref p) = prev {
                    if p != id {
                        self.saipen.detach(p);
                    }
                }
                self.saipen.attach(id, &path);
            }
        }
        Ok(())
    }

    /// The app-level direct-send boundary (TASK 24 §9): durable Queue
    /// UNKNOWN ambiguity in the session's own workspace blocks a direct send
    /// BEFORE any reservation or engine call. An UNKNOWN queue run may still
    /// be live even after a restart — the direct path must never bypass
    /// durable ambiguity; only explicit risk-confirmed resolution (or the
    /// item's authoritative terminal) re-opens the workspace. Other
    /// workspaces are unaffected.
    pub async fn send_scoped_receipt(
        &self,
        session_id: &str,
        expected_workspace_id: Option<&str>,
        expected_engine_id: Option<&str>,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<crate::engine::SendAcceptance, CoreError> {
        // Resolve the session's authoritative workspace (metadata, not the
        // caller's UI context). A storage failure here fails closed — the
        // send is blocked because we cannot prove the workspace is clean.
        if let Some(ws) = self.sessions.get(session_id).and_then(|s| s.workspace_id) {
            if self.queue.workspace_has_unknown(&ws)? {
                return Err(CoreError::WorkspaceOutcomeUnknown { workspace_id: ws });
            }
        }
        self.sessions
            .send_scoped_receipt(
                session_id,
                expected_workspace_id,
                expected_engine_id,
                prompt,
                model,
            )
            .await
    }

    /// Clear the durable active-workspace pointer and detach its SAIPEN watcher.
    ///
    /// Caller MUST already hold `active_workspace_lock` (CORE-007): this method
    /// performs NO locking of its own, so it is shared by `commit_active_workspace`
    /// (already under the lock) and `close_workspace` (also under the lock)
    /// without risking a recursive-lock deadlock. `WorkspaceClosed` for the
    /// cleared id is published by `WorkspaceManager::close`, not here.
    fn clear_active_workspace_locked(&self) -> Result<(), CoreError> {
        // Read the current active pointer (propagating errors), then commit the
        // durable clear BEFORE detaching the watcher. A storage failure preserves
        // the existing watcher and pointer.
        let prev = self.workspaces.get_active_workspace()?;
        self.workspaces.set_active_workspace(None)?;
        if let Some(p) = prev {
            self.saipen.detach(&p);
        }
        Ok(())
    }

    /// Close a workspace and detach its SAIPEN watcher (TASK 14 §62). Late
    /// watcher events are discarded by the generation guard (§65).
    ///
    /// CORE-007: the entire authoritative read/decision/close transition runs
    /// under `active_workspace_lock`, so a concurrent selection cannot commit a
    /// newer active id between this read and the clear. A storage read failure
    /// is propagated (never swallowed by `matches!`), so we never publish
    /// `WorkspaceClosed` while leaving a dangling active pointer/watcher. A
    /// newer selection committed before we took the lock is reflected in the
    /// active-pointer read, so an older close can never erase a newer
    /// selection. `WorkspaceClosed` is published only after the durable
    /// pointer/watcher state is coherent.
    pub fn close_workspace(&self, id: &str) -> Result<(), CoreError> {
        let _guard = self
            .active_workspace_lock
            .lock()
            .expect("active workspace lock poisoned");
        // Read the active pointer UNDER the lock and propagate any error.
        let active = self.workspaces.get_active_workspace()?;
        if active.as_deref() == Some(id) {
            // This exact workspace is the active one: clear durable pointer +
            // watcher under the SAME lock (no recursive lock). A newer selection
            // is reflected in `active`, so we cannot erase it.
            self.clear_active_workspace_locked()?;
        }
        self.workspaces.close(id)?;
        Ok(())
    }

    /// App-owned Forget (TASK 24 §9): the workspace identity is removed ONLY
    /// after every live service that requires it has been released. Rejected
    /// with a typed `WorkspaceInUse` while an engine runtime is bound to the
    /// workspace, any session has an active/unknown run, or the durable queue
    /// holds active/nonterminal items referencing it. On success the SAIPEN
    /// watcher/cache is detached, workspace session metadata is wiped (no
    /// durable reference to a missing identity), and the row is deleted.
    pub async fn forget_workspace(&self, id: &str) -> Result<(), CoreError> {
        // W2-001: hold the binding-stability READ lease for every registered
        // engine across the entire check+delete. Lifecycle transitions
        // (start/stop) take the WRITE lease around their binding write, so this
        // fully sequences `forget` against any rebind — a concurrent
        // start/stop cannot flip a binding between our `bound_workspace()`
        // check and the row deletion, which would otherwise let us delete a
        // workspace an engine just (re)bound to. Acquired BEFORE the
        // active-workspace lock so the std Mutex is never held across the
        // `.await`.
        let mut _binding_leases: Vec<_> = Vec::new();
        for eid in self.engines.list() {
            _binding_leases.push(self.engines.acquire_binding_read_lease(&eid.id).await);
        }
        // CORE-002: serialize under the active transition lock so selection
        // cannot race deletion — the durable transaction now also clears the
        // matching active pointer atomically.
        let _guard = self
            .active_workspace_lock
            .lock()
            .expect("active workspace lock poisoned");
        // Existence + canonical path validation: unknown id fails before any
        // mutation.
        self.workspaces.path_of(id)?;
        // A started engine runtime bound to this workspace requires it.
        for eid in self.engines.list() {
            if self.engines.bound_workspace(&eid.id) == Some(Some(id.to_string())) {
                return Err(CoreError::WorkspaceInUse {
                    workspace_id: id.to_string(),
                    reason: format!("engine '{}' is bound to this workspace — stop it (or restart it for another project) first", eid.id),
                });
            }
        }
        // In-memory active/unknown runs (also hydrates durable rows so the
        // check covers restart state; storage failure fails closed).
        for s in self.sessions.list(Some(id))? {
            if s.running || s.unknown_run.is_some() {
                return Err(CoreError::WorkspaceInUse {
                    workspace_id: id.to_string(),
                    reason: format!("session '{}' has an active or unknown run", s.id),
                });
            }
        }
        // Nonterminal durable queue work referencing the workspace.
        if self.queue.workspace_has_nonterminal(id)? {
            return Err(CoreError::WorkspaceInUse {
                workspace_id: id.to_string(),
                reason: "the queue holds active/nonterminal work for this workspace".to_string(),
            });
        }
        // Safe deletion path: the durable session-row + workspace-row + matching
        // active-pointer deletion runs in ONE storage transaction (TASK 24 §9,
        // CORE-002) — a failure in any step rolls back the whole operation, so
        // no partially-forgotten workspace or dangling active id is left behind.
        // ONLY AFTER that commits do we mutate the live in-memory projections
        // and detach the SAIPEN watcher/cache. AUDIT-W2-003: a
        // WorkspaceReferenced failure from inside the transaction means an
        // enqueue committed between our preflight and the delete — surface
        // it as the same typed in-use rejection.
        self.db.forget_workspace_with_sessions(id).map_err(|e| match e {
            saiwork_storage::StorageError::WorkspaceReferenced { workspace_id } => {
                CoreError::WorkspaceInUse {
                    workspace_id,
                    reason: "the queue holds active/nonterminal work for this workspace".to_string(),
                }
            }
            other => CoreError::Storage(other),
        })?;
        self.sessions.drop_workspace_sessions(id);
        self.saipen.detach(id);
        Ok(())
    }

    /// User-facing session deletion. Queue references are checked by the App,
    /// which is the only layer that can see both durable queue and session
    /// authorities; SessionManager then performs upstream-first cleanup.
    pub async fn delete_session(&self, session_id: &str) -> Result<(), CoreError> {
        let _guard = self.session_queue_lock.lock().await;
        if self.queue.session_has_nonterminal(session_id)? {
            return Err(CoreError::SessionInUse {
                session_id: session_id.into(),
                reason: "the queue holds active/nonterminal work for this session".into(),
            });
        }
        self.sessions.delete_session(session_id).await
    }

    /// Enqueue through the App boundary whenever an existing session is the
    /// target. QueueManager performs the authoritative target validation
    /// while the same lock used by deletion is held; therefore either the
    /// queue row commits first and deletion rejects it, or deletion commits
    /// first and validation rejects the stale session id.
    pub async fn enqueue_prompt(
        &self,
        req: saiwork_queue::EnqueueRequest,
    ) -> Result<saiwork_queue::QueueItem, CoreError> {
        if req.session_mode == saiwork_queue::SessionMode::Existing {
            let _guard = self.session_queue_lock.lock().await;
            return self.queue.enqueue(req).map_err(CoreError::Queue);
        }
        self.queue.enqueue(req).map_err(CoreError::Queue)
    }

    // ---- model favorites (durable UI preference) ----
    //
    // The app is the authority for UI preferences that must survive
    // restarts; the UI never writes the DB (law 5). Favorites are model ids
    // (`<provider>/<raw-key>`, globally namespaced by the adapter), so the
    // set is engine-independent. Bounded and deduplicated: a hostile or
    // corrupt stored value can never grow without limit (law 13) or
    // duplicate entries.
    pub const SETTING_MODEL_FAVORITES: &str = "ui.models.favorites";
    pub const MAX_MODEL_FAVORITES: usize = 50;

    pub fn model_favorites(&self) -> Result<Vec<String>, CoreError> {
        match self.db.get_setting(Self::SETTING_MODEL_FAVORITES)? {
            None => Ok(Vec::new()),
            Some(raw) => parse_favorites(&raw),
        }
    }

    pub fn set_model_favorites(&self, favorites: &[String]) -> Result<(), CoreError> {
        let capped = normalize_favorites(favorites);
        self.db
            .set_setting(Self::SETTING_MODEL_FAVORITES, &serialize_favorites(&capped))?;
        Ok(())
    }

    // ---- settings preset import (T-078) ----

    /// Upper bound on an imported preset file. A preset is a tiny durable-UI
    /// bundle (favorites + a few settings); anything larger is not a preset.
    pub const MAX_PRESET_BYTES: usize = 1 * 1024 * 1024;

    /// Setting keys a preset file may write. Everything else is rejected
    /// (fail-closed): the generic `set_setting` surface enforces the same
    /// whitelist, and a preset must never bypass it (T-052 / law 5).
    pub const PRESET_SETTING_KEYS: &[&str] = &["ui.layout.v1", "ui.engine.v1"];

    /// Apply a durable-UI settings preset read from a user-picked file.
    ///
    /// Defect this closes (T-078): a preset file was handed to the UI as raw
    /// bytes and `JSON.parse`d blindly. A ZIP archive starts with the ASCII
    /// magic `PK\x03\x04`, so parsing it as JSON produced a misleading
    /// "Unexpected token P" failure — or worse, a wrong-typed value. The
    /// preset import MUST detect the ZIP magic up front, reject it with an
    /// actionable error, and never feed a ZIP into the JSON parser. JSON
    /// presets are still the supported import format.
    ///
    /// Returns the counts actually applied (settings + favorites), so the UI
    /// can report what landed rather than a bare success.
    pub fn import_preset(&self, bytes: &[u8]) -> Result<PresetImportSummary, CoreError> {
        if bytes.len() > Self::MAX_PRESET_BYTES {
            return Err(CoreError::PresetImport(format!(
                "preset file exceeds the {} byte limit",
                Self::MAX_PRESET_BYTES
            )));
        }
        if is_zip_magic(bytes) {
            return Err(CoreError::PresetImport(
                "the selected file is a ZIP archive (PK magic), not a JSON preset — \
                 extract the preset.json from the archive and import that"
                    .into(),
            ));
        }
        let preset: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
            CoreError::PresetImport(format!("preset is not valid JSON: {e}"))
        })?;
        let obj = preset.as_object().ok_or_else(|| {
            CoreError::PresetImport("preset must be a JSON object".into())
        })?;

        let mut settings_applied = 0;
        if let Some(settings) = obj.get("settings") {
            let map = settings.as_object().ok_or_else(|| {
                CoreError::PresetImport("preset 'settings' must be an object".into())
            })?;
            for (key, value) in map {
                if !Self::PRESET_SETTING_KEYS.contains(&key.as_str()) {
                    return Err(CoreError::PresetImport(format!(
                        "preset setting '{key}' is not an allowed preference"
                    )));
                }
                let text = value.as_str().ok_or_else(|| {
                    CoreError::PresetImport(format!(
                        "preset setting '{key}' must be a string"
                    ))
                })?;
                self.db.set_setting(key, text)?;
                settings_applied += 1;
            }
        }

        let mut favorites_applied = 0;
        if let Some(favorites) = obj.get("favorites") {
            let list = favorites.as_array().ok_or_else(|| {
                CoreError::PresetImport("preset 'favorites' must be an array of model ids".into())
            })?;
            let mut ids = Vec::with_capacity(list.len());
            for entry in list {
                let id = entry.as_str().ok_or_else(|| {
                    CoreError::PresetImport("preset 'favorites' must contain only strings".into())
                })?;
                ids.push(id.to_string());
            }
            self.set_model_favorites(&ids)?;
            favorites_applied = normalize_favorites(&ids).len();
        }

        Ok(PresetImportSummary {
            settings_applied,
            favorites_applied,
        })
    }

    /// Request shutdown from any pre-STOPPED state (Booting/Ready/Failed).
    /// Idempotent: a concurrent or repeated request observes the same
    /// terminal outcome; exactly one sequence runs (TASK 08 §25, TASK 24 §9).
    /// A caller that loses the CAS does NOT return early — it awaits the one
    /// shared completion, so window-close can never `exit(0)` while the
    /// winning sequence is still stopping queues/engines/processes/DB.
    ///
    /// Phases (TASK 08 §23):
    /// ```text
    /// 1. → SHUTTING_DOWN (reject new work)
    /// 2. publish app.stopping
    /// 3. supervisor rejects new spawns
    /// 4. stop/dispose engines (cancels runs, releases permissions)
    /// 5. ProcessSupervisor.shutdown() (graceful → force → clear)
    /// 6. storage checkpoint (flush durable state) + final integrity
    /// 7. → STOPPED
    /// ```
    ///
    /// The EventBus stays open until the last sender drops (App teardown at
    /// process exit): `app.stopping` is published before cleanup so consumers
    /// observe the shutdown, and the bus is never closed first (TASK 08 §30).
    /// Authoritative pending-permission snapshot (W2-004): every open
    /// permission request across all engines, keyed by exact session/run/
    /// request ownership. Reconciliation rebuilds the UI permission cards from
    /// this after a bounded-bus lag, so a missed `permission.requested` state
    /// event is recoverable.
    pub fn pending_permissions(&self) -> Vec<PendingPermissionInfo> {
        self.engines.pending_permissions()
    }

    /// AUDIT-CORE-002: authoritative pending-question snapshot — every open
    /// user question across all engines, keyed by exact session/run/request
    /// ownership. Same reconciliation contract as `pending_permissions`.
    pub fn pending_questions(&self) -> Vec<crate::engine::PendingQuestionInfo> {
        self.engines.pending_questions()
    }

    pub async fn shutdown(&self, reason: &str) -> ShutdownReport {
        let started = Instant::now();

        // Phase 1: barrier. Whichever caller wins the CAS ARMS the sequence;
        // every caller — including the initiator — then awaits the same
        // App-owned completion.
        let prev = self.state();
        if self.transition(prev, AppState::ShuttingDown).is_ok() {
            let handle = self.shutdown_handle();
            // AUDIT-W2-002: arm the internally owned cleanup exactly once.
            // The old code ran the sequence INLINE in the winner's future:
            // an aborted/panicking winner abandoned `run_shutdown_sequence`
            // mid-flight, never published, and every later caller blocked
            // forever in `rx.changed()` because the Sender outlives callers
            // (App holds the handle). The spawned owner below ALWAYS
            // publishes a terminal report; a panic inside the sequence is
            // converted into a deterministic failed terminal outcome.
            let first = {
                let mut guard = self
                    .shutdown_task_started
                    .lock()
                    .expect("shutdown task mutex poisoned");
                if *guard {
                    false
                } else {
                    *guard = true;
                    true
                }
            };
            if first {
                match self.self_weak.get().and_then(|w| w.upgrade()) {
                    Some(app) => {
                        let reason = reason.to_string();
                        tokio::spawn(async move {
                            let worker_app = app.clone();
                            let worker_reason = reason.clone();
                            let worker = tokio::spawn(async move {
                                worker_app
                                    .run_shutdown_sequence(&worker_reason, started)
                                    .await
                            });
                            // Join the inner task so a panic becomes a typed
                            // failed terminal instead of a wedged state.
                            let report = match worker.await {
                                Ok(report) => report,
                                Err(join_err) => {
                                    warn!(error = %join_err, "shutdown sequence panicked");
                                    let _ = app.transition(
                                        AppState::ShuttingDown,
                                        AppState::Stopped,
                                    );
                                    ShutdownReport {
                                        outcome: "failed",
                                        forced_processes: Vec::new(),
                                        warnings: vec![format!(
                                            "shutdown sequence panicked: {join_err}"
                                        )],
                                        shutdown_ms: 0,
                                    }
                                }
                            };
                            let handle = app.shutdown_handle();
                            let _ = handle.tx.send(Some(report));
                        });
                    }
                    None => {
                        // No Arc handle recorded (direct construction): run
                        // inline and publish synchronously (legacy path).
                        let report = self.run_shutdown_sequence(reason, started).await;
                        let _ = handle.tx.send(Some(report.clone()));
                        return report;
                    }
                }
            }
            // Every caller awaits the shared completion from here.
            let mut rx = handle.rx.clone();
            loop {
                if let Some(report) = rx.borrow().as_ref() {
                    return report.clone();
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }

        // Lost the CAS. If a sequence is still running, await ITS report —
        // never return early while cleanup may still be stopping engines /
        // checkpointing the DB.
        if self.state() == AppState::ShuttingDown {
            let handle = self.shutdown_handle();
            let mut rx = handle.rx.clone();
            loop {
                if let Some(report) = rx.borrow().as_ref() {
                    return report.clone();
                }
                if rx.changed().await.is_err() {
                    // Sender dropped without publishing: fall through to the
                    // terminal fallback.
                    break;
                }
            }
        }

        // Terminal fallback: the sequence already finished (STOPPED) or never
        // ran (an earlier failed/boot path). Report the shared outcome when
        // recorded, else the historical outcome.
        if let Some(report) = self
            .shutdown_handle
            .lock()
            .expect("shutdown handle mutex poisoned")
            .as_ref()
            .and_then(|h| h.rx.borrow().clone())
        {
            return report;
        }
        let already = self
            .last_shutdown_ms
            .read()
            .expect("shutdown report mutex poisoned")
            .unwrap_or(0);
        ShutdownReport {
            outcome: if self.state() == AppState::Stopped {
                "already_stopped"
            } else {
                "already_shutting_down"
            },
            forced_processes: Vec::new(),
            warnings: Vec::new(),
            shutdown_ms: already,
        }
    }

    /// The one shutdown sequence, run only by the caller that won the CAS.
    async fn run_shutdown_sequence(&self, reason: &str, started: Instant) -> ShutdownReport {
        let mut warnings = Vec::new();

        // Phase 2: announce shutdown before anything else dies (EVENTS.md).
        self.bus.publish(Event::AppStopping {
            reason: reason.into(),
        });

        // Phase 3: no new child may start mid-shutdown.
        self.supervisor.mark_shutting_down();

        // Phase 3.5: SAIPEN actions first — reject new actions, request safe
        // cancellation of active ones (bounded; supervisor sweep is the
        // fallback) (TASK 15 §67, §145). Then stop SAIPEN watchers
        // (TASK 14 §138): stop refreshes, watchers, debounce/read tasks,
        // drop projections.
        self.saipen_actions.shutdown();
        self.saipen.shutdown();

        // Phase 4: queue barrier — stop claiming new items and release safe
        // leases; active runs keep streaming so the queue can observe their
        // authoritative terminals while engines stop (TASK 13 §78–§81).
        self.queue.shutdown_barrier();

        // Phase 5: engines stop (cancel runs, release pending permissions).
        // The queue coordinator drains the resulting run terminals.
        self.engines.stop_all().await;

        // Phase 6: join the queue worker, then release any remaining safe
        // leases (LEASED-prepare) before storage closes.
        self.queue.finish_shutdown().await;

        // Phase 7: supervisor shutdown — graceful → bounded wait → force.
        let forced = self.supervisor.shutdown().await;
        if !forced.is_empty() {
            warn!(processes = ?forced, "some processes required force kill");
        }

        // Phase 8: flush durable state (WAL checkpoint) + final integrity.
        if let Err(e) = self.db.checkpoint() {
            warnings.push(format!("storage checkpoint failed: {e}"));
        }
        // The promised final integrity check must re-run the PRAGMA after
        // checkpoint — the startup cache is stale by definition (runtime
        // writes happened since), and only an exact `ok` verdict passes
        // (TASK 24 §9).
        if let Err(e) = self.db.deep_integrity() {
            self.diagnostics
                .record_error("DB_FINAL_CHECK", e.to_string());
            warnings.push(format!("final integrity check failed: {e}"));
        }

        // Phase 9: terminal.
        let shutdown_ms = ms(started);
        let report = ShutdownReport::clean(shutdown_ms).finish(&forced, warnings);
        *self
            .last_shutdown_ms
            .write()
            .expect("shutdown report mutex poisoned") = Some(shutdown_ms);
        let _ = self.transition(AppState::ShuttingDown, AppState::Stopped);
        info!(
            outcome = report.outcome,
            shutdown_ms,
            forced = report.forced_processes.len(),
            reason,
            "shutdown complete"
        );
        report
    }

    /// Get or create the one shared shutdown completion channel (lazily — a
    /// process that never shuts down never allocates it).
    fn shutdown_handle(&self) -> Arc<ShutdownHandle> {
        let mut guard = self
            .shutdown_handle
            .lock()
            .expect("shutdown handle mutex poisoned");
        if let Some(h) = guard.as_ref() {
            return h.clone();
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        let handle = Arc::new(ShutdownHandle { tx, rx });
        *guard = Some(handle.clone());
        handle
    }

    pub fn is_shutting_down(&self) -> bool {
        matches!(
            self.state(),
            AppState::ShuttingDown | AppState::Stopped | AppState::Failed
        )
    }

    fn record_startup(&self, timings: StartupTimings) {
        *self
            .startup
            .write()
            .expect("startup timings mutex poisoned") = Some(timings);
    }

    /// Diagnostics snapshot (TASK 08 §36–§39): reads subsystem status from
    /// the owners, never a shadow state tree. No secrets, no env dumps.
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        let logging = self.logging.read().expect("logging info mutex poisoned");
        DiagnosticsSnapshot {
            version: crate::config::APP_VERSION.into(),
            data_root: self.config.data_root.display().to_string(),
            portable: self.config.portable,
            lifecycle: self.state(),
            startup_ms: self
                .startup
                .read()
                .expect("startup timings mutex poisoned")
                .clone(),
            last_shutdown_ms: *self
                .last_shutdown_ms
                .read()
                .expect("shutdown report mutex poisoned"),
            // Cached integrity (checked once at DB open; never re-run here),
            // cheap counts only — no workspace enumeration/FS/SAIPEN work in
            // a normal snapshot (PERFORMANCE.md).
            db_integrity: self
                .db
                .integrity()
                .unwrap_or_else(|e| format!("ERROR: {e}")),
            db_schema_version: self.db.version().unwrap_or(-1),
            storage_status: self
                .db
                .integrity()
                .map(|_| "ok".into())
                .unwrap_or_else(|e| {
                    self.diagnostics
                        .record_error("STORAGE_STATUS", e.to_string());
                    "error".into()
                }),
            engines: self.engines.list_info(),
            engine_count: self.engines.count(),
            supervisor_active: self.supervisor.count(),
            processes: self.supervisor.snapshots(),
            workspaces: self.db.workspace_count().unwrap_or(0),
            sessions: self.sessions.list(None).map(|v| v.len()).unwrap_or(0),
            recent_errors: self.diagnostics.recent_errors(),
            event_subscribers: self.bus.subscriber_count(),
            log_dir: logging.log_dir.as_ref().map(|p| p.display().to_string()),
            log_fallback: logging.fallback,
            platform: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            timestamp_ms: now_ms(),
        }
    }

    /// Record where the shell put the logs (diagnostics only; not an
    /// authority — the shell is the only writer, TASK 08 §37). Called by the
    /// desktop shell after `logging::init`.
    pub fn set_logging_info(&self, info: crate::logging::LoggingInfo) {
        *self.logging.write().expect("logging info mutex poisoned") = info;
    }

    /// Data root, for the frontend.
    pub fn data_root(&self) -> &PathBuf {
        &self.config.data_root
    }
}

fn ms(instant: Instant) -> u64 {
    instant.elapsed().as_millis() as u64
}

// ---- model favorites helpers ----

/// Result of a settings-preset import (T-078): what the preset actually
/// applied, so the caller can report real numbers instead of a bare OK.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PresetImportSummary {
    pub settings_applied: usize,
    pub favorites_applied: usize,
}

/// ZIP archives begin with the ASCII bytes `PK` (0x50 0x4B) followed by a
/// version byte; the empty-archive (`PK\x05\x06`) and spanned (`PK\x07\x08`)
/// variants carry the same two-letter prefix. A JSON preset never starts with
/// `PK`. This is the gate that stops a ZIP from ever reaching the JSON parser
/// (T-078): no `PK` prefix, no JSON.parse of archive bytes.
fn is_zip_magic(bytes: &[u8]) -> bool {
    bytes.starts_with(b"PK\x03\x04")
        || bytes.starts_with(b"PK\x05\x06")
        || bytes.starts_with(b"PK\x07\x08")
}

/// Parse the stored favorites JSON. A corrupt value FAILS CLOSED (the UI
/// learns the preference could not be read) instead of silently wiping a
/// possibly-larger old set.
fn parse_favorites(raw: &str) -> Result<Vec<String>, CoreError> {
    let parsed: Vec<String> = serde_json::from_str(raw).map_err(|e| {
        CoreError::Internal(format!(
            "corrupt {} setting: {e}",
            App::SETTING_MODEL_FAVORITES
        ))
    })?;
    Ok(normalize_favorites(&parsed))
}

/// Deduplicate, drop empty ids, and bound the set (law 13): a hostile
/// caller can never persist an unbounded favorites list.
fn normalize_favorites(favorites: &[String]) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut out = Vec::new();
    for id in favorites {
        if !id.is_empty() && seen.insert(id.as_str()) {
            out.push(id.clone());
        }
        if out.len() >= App::MAX_MODEL_FAVORITES {
            break;
        }
    }
    out
}

fn serialize_favorites(favorites: &[String]) -> String {
    serde_json::to_string(favorites).unwrap_or_else(|_| "[]".to_string())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::SendAcceptance;
    use async_trait::async_trait;

    #[test]
    fn valid_transitions_are_allowed() {
        assert!(transition_allowed(AppState::Booting, AppState::Ready));
        assert!(transition_allowed(AppState::Booting, AppState::Failed));
        assert!(transition_allowed(
            AppState::Booting,
            AppState::ShuttingDown
        ));
        assert!(transition_allowed(AppState::Ready, AppState::ShuttingDown));
        assert!(transition_allowed(AppState::Failed, AppState::ShuttingDown));
        assert!(transition_allowed(
            AppState::ShuttingDown,
            AppState::Stopped
        ));
    }

    #[test]
    fn impossible_transitions_are_rejected() {
        // No resurrection within one process lifetime (TASK 08 §6).
        assert!(!transition_allowed(AppState::Stopped, AppState::Ready));
        assert!(!transition_allowed(AppState::Failed, AppState::Ready));
        assert!(!transition_allowed(AppState::ShuttingDown, AppState::Ready));
        assert!(!transition_allowed(AppState::Ready, AppState::Ready));
        assert!(!transition_allowed(AppState::Stopped, AppState::Failed));
    }

    // ---- model favorites ----

    #[test]
    fn favorites_normalize_dedupe_and_bound() {
        // Dedupe + empty drop (law 13: nothing unbounded, nothing duplicated).
        let raw = vec![
            "a".to_string(),
            "a".to_string(),
            "".to_string(),
            "b".to_string(),
        ];
        let norm = normalize_favorites(&raw);
        assert_eq!(norm, vec!["a".to_string(), "b".to_string()]);

        // Cap: 50 max, first N win (insertion order preserved).
        let many: Vec<String> = (0..200).map(|i| format!("m{i}")).collect();
        let capped = normalize_favorites(&many);
        assert_eq!(capped.len(), App::MAX_MODEL_FAVORITES);
        assert_eq!(capped[0], "m0");
        assert_eq!(capped[49], "m49");
    }

    #[test]
    fn favorites_roundtrip_through_storage() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);

        // Absent setting reads as empty — never an error.
        assert_eq!(app.model_favorites().unwrap(), Vec::<String>::new());

        let favs = vec![
            "anthropic/claude-3.5".to_string(),
            "openai/gpt-4o".to_string(),
        ];
        app.set_model_favorites(&favs).unwrap();
        assert_eq!(app.model_favorites().unwrap(), favs);

        // The setting survives a full App reconstruction (durable, not
        // in-memory state).
        let config2 = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let db2 = Db::open(&config2.database_path()).unwrap();
        let app2 = App::construct_booting(config2, db2);
        assert_eq!(app2.model_favorites().unwrap(), favs);
    }

    #[test]
    fn corrupt_favorites_fail_closed() {
        // A corrupt stored value must surface as an error — never silently
        // wipe a larger old set by treating the value as empty.
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        db.set_setting(App::SETTING_MODEL_FAVORITES, "{not-json")
            .unwrap();
        let app = App::construct_booting(config, db);
        assert!(app.model_favorites().is_err());
    }

    #[test]
    fn favorites_set_is_capped_before_persist() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);
        let many: Vec<String> = (0..300).map(|i| format!("m{i}")).collect();
        app.set_model_favorites(&many).unwrap();
        assert_eq!(
            app.model_favorites().unwrap().len(),
            App::MAX_MODEL_FAVORITES
        );
    }

    // ---- settings preset import (T-078) ----

    #[test]
    fn zip_magic_is_detected_before_any_json_parse() {
        // A ZIP never reaches the JSON parser: all three magic variants are
        // refused up front with an actionable error (T-078).
        for magic in [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"] {
            assert!(is_zip_magic(magic));
        }
        assert!(!is_zip_magic(b"{"));
        assert!(!is_zip_magic(b"PK"));
        assert!(!is_zip_magic(b"PKX"));

        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);

        // Real ZIP local-file-header bytes.
        let zip = [0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00];
        let err = app.import_preset(&zip).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ZIP archive"), "got: {msg}");
    }

    #[test]
    fn preset_import_applies_settings_and_favorites() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);

        let preset = br#"{
            "settings": { "ui.layout.v1": "{\"tabs\":[\"queue\"]}" },
            "favorites": ["a/provider", "b/model", "a/provider"]
        }"#;
        let summary = app.import_preset(preset).unwrap();
        assert_eq!(summary.settings_applied, 1);
        assert_eq!(summary.favorites_applied, 2); // deduped by the authority

        assert_eq!(
            app.db.get_setting("ui.layout.v1").unwrap(),
            Some("{\"tabs\":[\"queue\"]}".into())
        );
        assert_eq!(
            app.model_favorites().unwrap(),
            vec!["a/provider".to_string(), "b/model".to_string()]
        );
    }

    #[test]
    fn preset_import_rejects_unknown_settings_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);

        // An unknown key must reject the WHOLE preset — no partial apply that
        // could slip a non-whitelisted preference past the T-052 gate.
        let preset = br#"{
            "settings": { "ui.models.favorites": "[\"x\"]" }
        }"#;
        let err = app.import_preset(preset).unwrap_err();
        assert!(err.to_string().contains("ui.models.favorites"));
        assert!(app.model_favorites().unwrap().is_empty());
    }

    #[test]
    fn preset_import_rejects_non_object_and_oversized() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);

        assert!(app.import_preset(b"[1,2,3]").is_err());
        assert!(app.import_preset(b"not json").is_err());

        let huge = vec![b' '; App::MAX_PRESET_BYTES + 1];
        let err = app.import_preset(&huge).unwrap_err();
        assert!(err.to_string().contains("byte limit"));
    }

    #[tokio::test]
    async fn shutdown_from_booting_is_supported_and_cleans() {
        // The window cannot exist during the (synchronous, millisecond)
        // bootstrap, so a close-during-boot cannot happen in the real app;
        // the state machine still supports BOOTING → SHUTTING_DOWN → STOPPED
        // defensively (TASK 08 §61) and cleanup is complete.
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);
        assert_eq!(app.state(), AppState::Booting);

        let report = app.shutdown("during boot").await;
        assert_eq!(app.state(), AppState::Stopped);
        assert!(app.supervisor.count() == 0);
        assert!(report.forced_processes.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_then_shutdown_reaches_stopped_and_records_timings() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        assert_eq!(app.state(), AppState::Ready);
        assert!(app
            .startup
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|t| t.total_ms > 0));

        let report = app.shutdown("test").await;
        assert_eq!(app.state(), AppState::Stopped);
        assert!(report.shutdown_ms > 0 || report.outcome == "clean");
    }

    /// AUDIT-W2-002 test engine: `stop` parks on a Notify gate so the test
    /// can hold the shutdown sequence mid-flight deterministically.
    struct BlockingStopEngine {
        id: String,
        stopped: std::sync::atomic::AtomicBool,
        entered: std::sync::atomic::AtomicBool,
        stop_calls: std::sync::atomic::AtomicUsize,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl crate::engine::EngineAdapter for BlockingStopEngine {
        fn identity(&self) -> crate::engine::EngineIdentity {
            crate::engine::EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }
        fn capabilities(&self) -> crate::engine::EngineCapabilities {
            crate::engine::EngineCapabilities::default()
        }
        async fn start(
            &self,
            _ctx: &crate::engine::EngineStartContext,
        ) -> Result<(), crate::engine::EngineError> {
            self.stopped
                .store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn stop(&self) -> Result<(), crate::engine::EngineError> {
            self.stop_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.release.notified().await;
            self.stopped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn kill(&self) -> Result<(), crate::engine::EngineError> {
            Ok(())
        }
        fn health(&self) -> crate::engine::EngineHealth {
            if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
                crate::engine::EngineHealth::Stopped
            } else {
                crate::engine::EngineHealth::Ready
            }
        }
        async fn list_models(
            &self,
        ) -> Result<Vec<crate::engine::ModelInfo>, crate::engine::EngineError> {
            Err(crate::engine::EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }
        async fn list_sessions(
            &self,
        ) -> Result<Vec<crate::engine::SessionInfo>, crate::engine::EngineError> {
            Ok(Vec::new())
        }
        async fn create_session(
            &self,
            _req: &crate::engine::CreateSessionRequest,
        ) -> Result<crate::engine::SessionCreation, crate::engine::EngineError> {
            Err(crate::engine::EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "sessions",
            })
        }
        async fn resume_session(
            &self,
            _id: &str,
        ) -> Result<crate::engine::SessionInfo, crate::engine::EngineError> {
            Err(crate::engine::EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "resume",
            })
        }
        async fn delete_session(&self, _id: &str) -> Result<(), crate::engine::EngineError> {
            Ok(())
        }
        async fn send(
            &self,
            _req: &crate::engine::SendRequest,
        ) -> Result<crate::engine::SendAcceptance, crate::engine::EngineError> {
            Err(crate::engine::EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "send",
            })
        }
        async fn cancel(&self, _run_id: &str) -> Result<(), crate::engine::EngineError> {
            Ok(())
        }
    }

    /// AUDIT-W2-002: aborting the shutdown CALLER that armed the sequence
    /// must not wedge canonical cleanup. The sequence is owned by an
    /// App-owned task once armed; a later caller completes normally and
    /// cleanup ran exactly once.
    #[tokio::test]
    async fn aborted_shutdown_winner_cannot_wedge_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let blocker = Arc::new(BlockingStopEngine {
            id: "blocker".into(),
            stopped: std::sync::atomic::AtomicBool::new(true),
            entered: std::sync::atomic::AtomicBool::new(false),
            stop_calls: std::sync::atomic::AtomicUsize::new(0),
            release: release.clone(),
        });
        // Start the runtime so stop_all actually calls into it.
        let ctx = crate::engine::EngineStartContext {
            workspace_id: None,
            workspace_path: Some(dir.path().to_path_buf()),
            bus: app.bus.clone(),
            diagnostics: app.diagnostics.clone(),
            supervisor: app.supervisor.clone(),
            report_failure: Arc::new(|_, _| {}),
        };
        app.engines.register(blocker.clone());
        app.engines.start("blocker", &ctx).await.unwrap();

        // Caller A arms + runs the sequence; it parks inside the blocked
        // engine stop.
        let app_a = app.clone();
        let task_a = tokio::spawn(async move { app_a.shutdown("a").await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !blocker.entered.load(std::sync::atomic::Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "shutdown sequence never reached the blocked engine stop"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // ABORT the arming caller while it awaits the shared completion.
        task_a.abort();

        // Caller B must still complete within bounds; cleanup runs exactly
        // once and reaches STOPPED with the shared report.
        let release_gate = release.clone();
        tokio::time::timeout(std::time::Duration::from_millis(200), async move {
            // Give A's abort a moment to land before unblocking.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            release_gate.notify_waiters();
        })
        .await
        .unwrap();
        let report_b = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            app.shutdown("b"),
        )
        .await
        .expect("caller B must not wait forever after A was aborted");
        assert_eq!(app.state(), AppState::Stopped);
        assert_eq!(report_b.outcome, "clean");
        assert_eq!(
            blocker
                .stop_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "cleanup must have run exactly once"
        );
    }

    /// Bootstrap rollback leaves zero background ownership (TASK 24 §9): the
    /// running-tracker task is spawned only AFTER the fallible queue init,
    /// so a corrupt queue persistence failure returns Err with no surviving
    /// tasks/subscribers/service references — the DB file is immediately
    /// deletable and reopenable.
    #[tokio::test]
    async fn bootstrap_failure_after_queue_init_leaves_no_background_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db_path = config.database_path();
        // Seed a corrupt queue row so `queue.init()` fails closed (invalid
        // persisted state → storage error) AFTER services are constructed.
        {
            let db = Db::open(&db_path).unwrap();
            db.with_conn(|conn| {
                conn.execute(
                    "INSERT INTO queue_items (id, workspace_id, engine_id, payload, state, order_key, created_at, updated_at) \
                     VALUES ('q_bad', 'w1', 'fake', 'x', 'bogus-state', 1, 1, 1)",
                    [],
                )
                .unwrap();
                Ok::<(), saiwork_storage::StorageError>(())
            })
            .unwrap();
        }

        let err = match App::bootstrap_with(config) {
            Ok(_) => panic!("corrupt queue persistence must fail bootstrap"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("invalid persisted queue row"),
            "typed storage error, got {err:?}"
        );

        // Zero surviving ownership: the DB file must be deletable and
        // reopenable immediately (a lingering tracker task would hold the
        // handle open on Windows).
        std::fs::remove_file(&db_path)
            .expect("no task may hold the DB open after failed bootstrap");
        let reopened = Db::open(&db_path).expect("DB must be reopenable after rollback");
        drop(reopened);
    }

    #[tokio::test]
    async fn double_shutdown_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        let first = app.shutdown("first").await;
        let second = app.shutdown("second").await;
        assert_eq!(app.state(), AppState::Stopped);
        assert_eq!(first.outcome, "clean");
        // Both callers converge on the SAME shared completion report — the
        // second caller awaits the first's result instead of returning early
        // (TASK 24 §9: a window-close during shutdown must never exit before
        // cleanup finished).
        assert_eq!(second.outcome, "clean");
        assert_eq!(first.shutdown_ms, second.shutdown_ms);
    }

    #[tokio::test]
    async fn require_ready_guards_boot_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        config.ensure_layout().unwrap();
        let db = Db::open(&config.database_path()).unwrap();
        let app = App::construct_booting(config, db);
        assert!(matches!(app.require_ready(), Err(CoreError::NotReady)));

        // Simulate reaching READY (bootstrap path normally does this).
        app.transition(AppState::Booting, AppState::Ready).unwrap();
        assert!(app.require_ready().is_ok());

        let _ = app.shutdown("guard test").await;
        assert!(matches!(app.require_ready(), Err(CoreError::ShuttingDown)));
    }

    /// Concurrent shutdown race (TASK 09 §39, TASK 24 §9): many callers
    /// request shutdown at once. Exactly one sequence runs; every concurrent
    /// caller awaits the SAME shared completion report (none returns early
    /// with `already_shutting_down` while cleanup is still in progress); the
    /// app ends STOPPED with durable state checkpointed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_shutdown_requests_run_exactly_one_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config.clone()).unwrap();
        app.db.set_setting("durable", "yes").unwrap();

        let mut handles = Vec::new();
        for i in 0..8 {
            let app = app.clone();
            handles.push(tokio::spawn(async move {
                app.shutdown(&format!("caller {i}")).await
            }));
        }
        let reports: Vec<ShutdownReport> = {
            let mut out = Vec::new();
            for h in handles {
                out.push(h.await.expect("shutdown task must not panic"));
            }
            out
        };

        assert_eq!(app.state(), AppState::Stopped);
        // Every caller converged on the one shared report: no caller returned
        // early while the winner was mid-sequence, so all outcomes are the
        // same "clean" terminal.
        assert!(
            reports.iter().all(|r| r.outcome == "clean"),
            "all callers must await the shared completion: {reports:?}"
        );
        assert!(
            reports
                .iter()
                .all(|r| r.shutdown_ms == reports[0].shutdown_ms),
            "all callers observe the identical report: {reports:?}"
        );

        // The winning sequence completed durable cleanup: state reopened from
        // the same data root sees the pre-shutdown write.
        let reopened = App::bootstrap_with(config).unwrap();
        assert_eq!(
            reopened.db.get_setting("durable").unwrap().as_deref(),
            Some("yes"),
            "shutdown checkpoint must have flushed durable state"
        );
        let _ = reopened.shutdown("cleanup").await;
    }

    /// Event ordering at the boundary (TASK 09 §34): after `app.stopping` is
    /// published, no further lifecycle events are emitted by the app — the
    /// terminal app state is announced once, then only bus teardown silence.
    #[tokio::test]
    async fn no_lifecycle_events_after_app_stopping() {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        let mut observer = app.bus.subscribe();
        // Drain the bootstrap events so we only observe the shutdown window.
        while observer.try_recv().unwrap().is_some() {}

        let report = app.shutdown("order test").await;
        assert_eq!(app.state(), AppState::Stopped);
        assert_eq!(report.outcome, "clean");

        // Exactly one lifecycle event is observed during shutdown: app.stopping.
        let mut saw_stopping = false;
        let mut trailing = Vec::new();
        loop {
            match observer.try_recv() {
                Ok(Some(env)) => {
                    if matches!(env.event, Event::AppStopping { .. }) {
                        saw_stopping = true;
                    } else {
                        trailing.push(env.event.name().to_string());
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("unexpected drain error: {e:?}"),
            }
        }
        assert!(saw_stopping, "app.stopping must be published exactly once");
        assert!(
            trailing.is_empty(),
            "no events after app.stopping: {trailing:?}"
        );
    }

    /// Fake adapter whose send always returns OutcomeUnknown (the run may be
    /// live) and whose create succeeds. Counts external send calls.
    struct UnknownRunEngine {
        id: String,
        calls: std::sync::atomic::AtomicUsize,
        /// Lifecycle state: Stopped until `start`, Ready while running — the
        /// registry's start precheck requires a Stopped/Unknown health.
        stopped: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl crate::engine::EngineAdapter for UnknownRunEngine {
        fn identity(&self) -> crate::engine::EngineIdentity {
            crate::engine::EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }

        fn capabilities(&self) -> crate::engine::EngineCapabilities {
            crate::engine::EngineCapabilities {
                sessions: true,
                ..Default::default()
            }
        }

        async fn start(
            &self,
            _ctx: &crate::engine::EngineStartContext,
        ) -> Result<(), crate::engine::EngineError> {
            self.stopped
                .store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn stop(&self) -> Result<(), crate::engine::EngineError> {
            self.stopped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn kill(&self) -> Result<(), crate::engine::EngineError> {
            Ok(())
        }
        fn health(&self) -> crate::engine::EngineHealth {
            if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
                crate::engine::EngineHealth::Stopped
            } else {
                crate::engine::EngineHealth::Ready
            }
        }
        async fn list_models(
            &self,
        ) -> Result<Vec<crate::engine::ModelInfo>, crate::engine::EngineError> {
            Err(crate::engine::EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }
        async fn list_sessions(
            &self,
        ) -> Result<Vec<crate::engine::SessionInfo>, crate::engine::EngineError> {
            Ok(Vec::new())
        }
        async fn create_session(
            &self,
            req: &crate::engine::CreateSessionRequest,
        ) -> Result<crate::engine::SessionCreation, crate::engine::EngineError> {
            Ok(crate::engine::SessionCreation::Created {
                engine_session_id: format!("up-{}", req.session_id),
                display_name: "unknown-engine session".into(),
            })
        }
        async fn resume_session(
            &self,
            _id: &str,
        ) -> Result<crate::engine::SessionInfo, crate::engine::EngineError> {
            Err(crate::engine::EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "resume",
            })
        }
        async fn delete_session(&self, _id: &str) -> Result<(), crate::engine::EngineError> {
            Ok(())
        }
        async fn send(
            &self,
            _req: &crate::engine::SendRequest,
        ) -> Result<crate::engine::SendAcceptance, crate::engine::EngineError> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::engine::SendAcceptance::OutcomeUnknown {
                run_id: format!("r{n}"),
                message: "transport lost".into(),
            })
        }
        async fn cancel(&self, _run_id: &str) -> Result<(), crate::engine::EngineError> {
            Ok(())
        }
    }

    async fn unknown_run_app() -> (Arc<App>, Arc<UnknownRunEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        // AUDIT-W2-003: session creation now requires the bound workspace
        // row to exist; seed the ids these tests create against.
        for wid in ["ws-A", "ws-C", "ws-D"] {
            app.db
                .with_conn(|conn| {
                    conn.execute(
                        "INSERT OR IGNORE INTO workspaces (id, path, name, last_opened_at, created_at, updated_at)
                         VALUES (?1, ?2, ?2, 0, 0, 0)",
                        rusqlite::params![wid, wid],
                    )
                    .map(|_| ())
                    .map_err(saiwork_storage::StorageError::Query)
                })
                .unwrap();
        }
        let engine = Arc::new(UnknownRunEngine {
            id: "eng".into(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            stopped: std::sync::atomic::AtomicBool::new(true),
        });
        app.engines.register(engine.clone());
        // Start the runtime so `send_acceptance` reaches the adapter; the
        // session-level generation check requires a live engine. Sessions are
        // created AFTER the start by every caller (generation-consistent).
        let ctx = app.engines.start_context(None, None);
        app.engines.start("eng", &ctx).await.unwrap();
        (app, engine)
    }

    #[tokio::test]
    async fn session_delete_and_existing_enqueue_have_one_app_owned_order() {
        let (app, _engine) = unknown_run_app().await;
        let session = app
            .sessions
            .create("eng", Some("ws-A"), None)
            .await
            .unwrap();
        app.queue.pause().unwrap();

        // Enqueue-first order: the durable reference wins and deletion must
        // retain the session until that work becomes terminal.
        let item = app
            .enqueue_prompt(saiwork_queue::EnqueueRequest {
                workspace_id: "ws-A".into(),
                engine_id: "eng".into(),
                session_id: Some(session.id.clone()),
                session_mode: saiwork_queue::SessionMode::Existing,
                model: None,
                payload: "queued first".into(),
            })
            .await
            .unwrap();
        assert!(matches!(
            app.delete_session(&session.id).await,
            Err(CoreError::SessionInUse { .. })
        ));

        app.queue.cancel(&item.id).await.unwrap();
        app.delete_session(&session.id).await.unwrap();

        // Delete-first order: target validation runs under the same lock and
        // rejects the removed id before a second durable row can be written.
        let err = app
            .enqueue_prompt(saiwork_queue::EnqueueRequest {
                workspace_id: "ws-A".into(),
                engine_id: "eng".into(),
                session_id: Some(session.id.clone()),
                session_mode: saiwork_queue::SessionMode::Existing,
                model: None,
                payload: "deleted first".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, CoreError::Queue(_)));
        assert!(!app.queue.session_has_nonterminal(&session.id).unwrap());

        let _ = app.shutdown("test complete").await;
    }

    /// P0 (TASK 24 §9): an OutcomeUnknown run is a NON-releasing workspace
    /// reservation. Lag reconciliation (which may see the run as no longer
    /// active) must never free the workspace; only a matching authoritative
    /// terminal may. The fixture: accept run → lose outcome → flood
    /// reconciliation → attempt a second same-workspace send: external send
    /// count stays 1 until the authoritative terminal; another workspace
    /// stays usable.
    #[tokio::test]
    async fn unknown_run_never_releases_workspace_until_matching_terminal() {
        let (app, engine) = unknown_run_app().await;
        let s_a = app
            .sessions
            .create("eng", Some("ws-A"), None)
            .await
            .unwrap();
        let s_a2 = app
            .sessions
            .create("eng", Some("ws-A"), None)
            .await
            .unwrap();
        let s_c = app
            .sessions
            .create("eng", Some("ws-C"), None)
            .await
            .unwrap();

        // Send #1: engine accepts but the outcome is lost → OutcomeUnknown
        // receipt (the typed kind, mapped to an error by the command layer).
        let receipt = app
            .send_scoped_receipt(&s_a.id, Some("ws-A"), Some("eng"), "hello", None)
            .await
            .expect("engine must be reached");
        assert!(
            matches!(receipt, SendAcceptance::OutcomeUnknown { ref run_id, .. } if run_id == "r0"),
            "OutcomeUnknown with run r0, got {receipt:?}"
        );
        let sess = app.sessions.get(&s_a.id).unwrap();
        assert_eq!(
            sess.unknown_run.as_deref(),
            Some("r0"),
            "reservation pinned"
        );
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Same session and same-workspace session are both blocked (zero
        // adapter calls): the unknown run may still be live.
        let err = app
            .send_scoped_receipt(&s_a.id, Some("ws-A"), Some("eng"), "again", None)
            .await
            .expect_err("unknown-run session must stay busy");
        assert!(err.to_string().contains("busy"), "{err:?}");
        let err = app
            .send_scoped_receipt(&s_a2.id, Some("ws-A"), Some("eng"), "again", None)
            .await
            .expect_err("same-workspace send must stay blocked");
        assert!(
            matches!(err, CoreError::WorkspaceBusy { .. }),
            "WorkspaceBusy, got {err:?}"
        );
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Another workspace remains usable (this send reaches the engine).
        let receipt = app
            .send_scoped_receipt(&s_c.id, Some("ws-C"), Some("eng"), "other", None)
            .await
            .expect("other workspace must be usable");
        assert!(
            matches!(receipt, SendAcceptance::OutcomeUnknown { .. }),
            "{receipt:?}"
        );
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Lag reconciliation: the engine no longer reports the run as active
        // → `running` is cleared, but the unknown reservation SURVIVES and
        // the workspace stays blocked.
        app.sessions.reconcile_running_from_engines();
        let sess = app.sessions.get(&s_a.id).unwrap();
        assert!(!sess.running, "ordinary liveness cleared by reconcile");
        assert_eq!(
            sess.unknown_run.as_deref(),
            Some("r0"),
            "reconcile must preserve the unknown reservation"
        );
        let err = app
            .send_scoped_receipt(&s_a.id, Some("ws-A"), Some("eng"), "after reconcile", None)
            .await
            .expect_err("still blocked after reconcile");
        assert!(err.to_string().contains("busy"), "{err:?}");
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 2);

        // A DIFFERENT run's terminal must NOT clear the reservation.
        app.bus.publish(Event::MessageCompleted {
            session_id: s_a.id.clone().into(),
            run_id: "other-run".into(),
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            app.sessions.get(&s_a.id).unwrap().unknown_run.as_deref(),
            Some("r0"),
            "unrelated terminal must never release an unknown reservation"
        );

        // The MATCHING authoritative terminal releases it via the real
        // tracker wiring (published on the bus).
        app.bus.publish(Event::MessageCompleted {
            session_id: s_a.id.clone().into(),
            run_id: "r0".into(),
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if app.sessions.get(&s_a.id).unwrap().unknown_run.is_none() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "matching terminal must clear the reservation"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // The workspace is usable again: the next send reaches the engine.
        let receipt = app
            .send_scoped_receipt(&s_a.id, Some("ws-A"), Some("eng"), "after terminal", None)
            .await
            .expect("engine must be reached again");
        assert!(
            matches!(receipt, SendAcceptance::OutcomeUnknown { .. }),
            "{receipt:?}"
        );
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    /// P0 (TASK 24 §9): the durable Queue UNKNOWN gate at the direct-send
    /// boundary. Seeded UNKNOWN in workspace A blocks direct Send A with a
    /// typed WorkspaceOutcomeUnknown and zero adapter calls (even after the
    /// in-memory session state is clean); other workspaces are unaffected;
    /// explicit risk-confirmed resolution re-opens A.
    #[tokio::test]
    async fn direct_send_blocked_by_durable_queue_unknown_until_resolution() {
        let (app, engine) = unknown_run_app().await;
        let s_a = app
            .sessions
            .create("eng", Some("ws-A"), None)
            .await
            .unwrap();
        let s_d = app
            .sessions
            .create("eng", Some("ws-D"), None)
            .await
            .unwrap();

        // Seed a durable UNKNOWN queue item in ws-A (as a restart would
        // leave it) WITH its persisted run_id.
        app.db
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO queue_items \
                     (id, workspace_id, engine_id, payload, state, run_id, order_key, created_at, updated_at) \
                     VALUES ('q_u', 'ws-A', 'eng', 'x', 'unknown', 'r-1', 1, 1, 1)",
                    [],
                )
                .unwrap();
                Ok::<(), saiwork_storage::StorageError>(())
            })
            .unwrap();

        // Direct send into ws-A: blocked BEFORE any reservation or engine
        // call, with the typed error.
        let err = app
            .send_scoped_receipt(&s_a.id, Some("ws-A"), Some("eng"), "hello", None)
            .await
            .expect_err("durable UNKNOWN must block direct send");
        assert!(
            matches!(err, CoreError::WorkspaceOutcomeUnknown { .. }),
            "typed WorkspaceOutcomeUnknown, got {err:?}"
        );
        assert_eq!(
            engine.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "zero adapter calls while the workspace has durable UNKNOWN"
        );

        // Another workspace is unaffected (the send reaches the engine).
        let receipt = app
            .send_scoped_receipt(&s_d.id, Some("ws-D"), Some("eng"), "other", None)
            .await
            .expect("other workspace must be usable");
        assert!(
            matches!(receipt, SendAcceptance::OutcomeUnknown { .. }),
            "{receipt:?}"
        );
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Explicit risk-confirmed resolution re-opens A.
        app.queue.resolve_unknown("q_u", 1).unwrap();
        let receipt = app
            .send_scoped_receipt(&s_a.id, Some("ws-A"), Some("eng"), "after resolve", None)
            .await
            .expect("resolution must re-open the workspace");
        assert!(
            matches!(receipt, SendAcceptance::OutcomeUnknown { .. }),
            "{receipt:?}"
        );
        assert_eq!(engine.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    /// P1 (TASK 24 §9): Forget is App-owned and fail-closed. An active/unknown
    /// run and a bound engine runtime each produce a typed Busy with the row
    /// retained; after the unknown resolves and the engine stops, safe Forget
    /// wipes session metadata and deletes the row — no durable reference to a
    /// missing WorkspaceId remains.
    #[tokio::test]
    async fn forget_workspace_fails_closed_while_busy_then_cleans() {
        let dir = tempfile::tempdir().unwrap();
        let ws_dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        let engine = Arc::new(UnknownRunEngine {
            id: "eng".into(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            stopped: std::sync::atomic::AtomicBool::new(true),
        });
        app.engines.register(engine.clone());

        let ws = app.open_workspace(ws_dir.path()).await.unwrap();

        // Engine runtime bound to this workspace (started BEFORE the session
        // is created — a later start bumps the generation and would make the
        // connection-owned session non-usable-now).
        let ctx = app
            .engines
            .start_context(Some(ws.id.clone()), Some(ws.path.clone()));
        app.engines.start("eng", &ctx).await.unwrap();

        let sid = app
            .sessions
            .create("eng", Some(&ws.id), None)
            .await
            .unwrap()
            .id;

        // Active unknown run in the workspace (reservation pinned to r0).
        let receipt = app
            .send_scoped_receipt(&sid, Some(&ws.id), Some("eng"), "hello", None)
            .await
            .expect("engine must be reached");
        assert!(
            matches!(receipt, SendAcceptance::OutcomeUnknown { ref run_id, .. } if run_id == "r0"),
            "{receipt:?}"
        );

        // Forget is rejected while the run is active: typed Busy, row retained.
        let err = app.forget_workspace(&ws.id).await.expect_err("must be busy");
        assert!(matches!(err, CoreError::WorkspaceInUse { .. }), "{err:?}");
        assert!(
            app.workspaces.path_of(&ws.id).is_ok(),
            "row must be retained while busy"
        );

        // Resolve the unknown (matching authoritative terminal). The engine
        // runtime is still bound → still busy.
        app.sessions.note_terminal(&sid, "r0");
        let err = app
            .forget_workspace(&ws.id)
            .await
            .expect_err("engine still bound");
        assert!(err.to_string().contains("bound"), "{err:?}");

        // Stop the engine → safe Forget succeeds and leaves no durable
        // reference to the missing WorkspaceId.
        app.engines.stop("eng").await.unwrap();
        app.forget_workspace(&ws.id).await.expect("safe forget");
        assert!(
            app.workspaces.path_of(&ws.id).is_err(),
            "row must be deleted after safe forget"
        );
        assert!(
            app.sessions.list(Some(&ws.id)).unwrap().is_empty(),
            "no session metadata may reference a deleted workspace"
        );
    }

    /// CORE-001: forgetting the exact-active workspace must clear the durable
    /// exact-active pointer at the same authority that detaches its watcher —
    /// a dangling exact-active id must never survive a successful forget, or a
    /// later cold bootstrap would re-select a deleted workspace.
    #[tokio::test]
    async fn forget_workspace_clears_a_dangling_exact_active_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let ws_dir = tempfile::tempdir().unwrap();
        let config = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        let app = App::bootstrap_with(config).unwrap();
        let ws = app.open_workspace(ws_dir.path()).await.unwrap();

        // Make this workspace the exact-active one (the durable pointer the
        // frontend bootstraps from).
        app.commit_active_workspace(Some(&ws.id), Some(1)).unwrap();
        assert_eq!(
            app.workspaces.get_active_workspace().unwrap(),
            Some(ws.id.clone()),
            "commit must set the exact-active pointer"
        );

        // Nothing is running — safe Forget must also clear the dangling
        // exact-active pointer so a later cold bootstrap never re-selects a
        // deleted workspace.
        app.forget_workspace(&ws.id).await.expect("safe forget");
        assert_eq!(
            app.workspaces.get_active_workspace().unwrap(),
            None,
            "dangling exact-active id must be cleared after forget"
        );
    }
}
