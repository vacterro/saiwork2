//! `HarnessAdapter` — the DeepSeek Harness `EngineAdapter` (TASK 20 + TASK 21).
//!
//! TASK 20 (foundation, unchanged): discovery, probe, ProcessSupervisor
//! ownership of the top-level runtime, protocol transport, ACP `initialize`
//! handshake, lifecycle with generation protection, typed errors.
//!
//! TASK 21 (vertical slice): the first complete Harness agent workflow through
//! the generic `EngineAdapter` — authoritative `session/new`, in-memory
//! session registry (Harness owns session identity), `session/prompt` runs
//! with `message.*` committed-chunk streaming, `tool_call` → `tool.*`
//! lifecycle, `session/request_permission` → generic permission round-trip
//! (fail-closed), `session/cancel` scoped to a RunId, exactly one terminal per
//! run, and clean teardown that settles every active run. Session-resume stays
//! `false` (ACP sessions are fresh + connection-owned); there is no SQLite
//! transcript mirror; QueueManager still cannot dispatch to this engine
//! (TASK 23 owns that).
//!
//! Lifecycle (adapter-internal, §52): `Unknown → Starting → Ready`; `stop`
//! goes to `Stopped`; unexpected runtime/protocol death goes to `Failed`
//! (reporting through the engine failure hook — never silent). Restart is
//! explicit and yields a fresh ProcessId, generation, transport, and registry;
//! stale runtime events cannot affect a new generation (§55, §107).
//!
//! READY requires (§33): top-level process alive **and** protocol transport
//! established **and** the ACP `initialize` handshake accepted. Process
//! RUNNING never equals engine READY. Protocol loss with the process alive
//! removes READY immediately (§57–§58). No automatic reconnect (§59).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineCapabilities, EngineError, EngineHealth,
    EngineIdentity, EngineStartContext, ModelInfo, SendAcceptance, SendRequest, SessionCreation,
    SessionInfo,
};
use saiwork_events::{Event, EventBus, RunId};
use saiwork_process::ManagedProcess;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::HarnessConfig;
use crate::error::HarnessError;
use crate::events::{
    emit_terminal, outcome_from_stop_reason, permission_handler, EventRouter, TerminalOutcome,
    ROUTE_DRAIN_GRACE,
};
use crate::permissions::PermissionRegistry;
use crate::protocol::{
    CancelParams, ContentBlock, DeleteSessionParams, NewSessionParams, NewSessionResult,
    PromptParams, PromptResult, SessionUpdateNotification, METHOD_SESSION_CANCEL,
    METHOD_SESSION_DELETE, METHOD_SESSION_NEW, METHOD_SESSION_PROMPT,
};
use crate::runs::{RunRecord, RunRegistry, RunState};
use crate::sessions::{HarnessSession, SessionRegistry};
use crate::transport::Transport;

/// Canonical engine id (registered through the existing EngineRegistry — no
/// second registry, TASK 20 §7).
pub const ENGINE_ID: &str = "deepseek-harness";

/// Adapter-internal lifecycle (mapped onto the generic engine health).
#[derive(Debug, Clone)]
enum AdapterState {
    Unknown,
    Starting,
    Ready,
    Stopping,
    Stopped,
    Failed { message: String },
}

/// One live runtime generation. Owned by the adapter slot; every long-lived
/// task belongs here with a cancel path and join at teardown (§79).
struct Runtime {
    generation: u64,
    process: Arc<ManagedProcess>,
    transport: Transport,
    cancel_tx: watch::Sender<bool>,
    tasks: Mutex<tokio::task::JoinSet<()>>,
}

struct Inner {
    config: HarnessConfig,
    state: RwLock<AdapterState>,
    runtime_slot: Mutex<Option<Arc<Runtime>>>,
    ctx: RwLock<Option<EngineStartContext>>,
    /// Serializes start/stop so no two runtimes can ever be spawned (§53,
    /// §64).
    start_lock: tokio::sync::Mutex<()>,
    /// Cancel signal for an in-flight start (stop-during-start, §54).
    cancel_flag: Mutex<Option<watch::Sender<bool>>>,
    generation: AtomicU64,
    last_error: RwLock<Option<String>>,
    server_info: RwLock<Option<crate::protocol::ServerInfo>>,
    protocol_version: RwLock<String>,
    last_handshake_ms: RwLock<Option<u64>>,
    /// The runtime's effective working directory (the `cwd` of `session/new`),
    /// recorded at start: explicit config cwd, else the engine-start
    /// workspace. ACP requires an absolute primary cwd per session (§8).
    runtime_cwd: RwLock<Option<PathBuf>>,
    /// TASK 21: adapter-local registries. One owner each; Harness stays the
    /// authority for session content/log; SAIWORK2 owns the normalized live
    /// projection (§6, §28).
    runs: Arc<RunRegistry>,
    sessions: Arc<SessionRegistry>,
    permissions: Arc<PermissionRegistry>,
}

/// Owns a published-but-not-yet-ready runtime across every cancellation
/// point in `start`. Normal completion disarms it; dropping the start future
/// schedules deterministic teardown while the adapter slot retains authority.
struct StartupOwnershipGuard {
    inner: Arc<Inner>,
    runtime: Option<Arc<Runtime>>,
}

impl StartupOwnershipGuard {
    fn new(inner: Arc<Inner>, runtime: Arc<Runtime>) -> Self {
        Self {
            inner,
            runtime: Some(runtime),
        }
    }

    fn disarm(&mut self) {
        self.runtime = None;
    }
}

impl Drop for StartupOwnershipGuard {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let inner = self.inner.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    inner.cleanup_aborted_start(runtime).await;
                });
            }
            Err(error) => {
                // The runtime remains published in `runtime_slot`; losing the
                // executor must never also lose process authority.
                warn!(engine = ENGINE_ID, %error, "cannot schedule aborted-start cleanup; runtime remains owned");
            }
        }
    }
}

impl Inner {
    fn set_state(&self, state: AdapterState) {
        *self.state.write().expect("state mutex poisoned") = state;
    }

    /// Record a failure: state → Failed, last-error, diagnostics. Used for
    /// startup failures and runtime crashes (never for user cancel).
    fn record_failure(&self, err: &HarnessError, phase: &str) {
        let message = format!("{phase}: {err}");
        *self.last_error.write().expect("last error mutex poisoned") = Some(message.clone());
        let already_failed = matches!(
            *self.state.read().expect("state mutex poisoned"),
            AdapterState::Failed { .. }
        );
        if !already_failed {
            self.set_state(AdapterState::Failed {
                message: message.clone(),
            });
        }
        if let Some(ctx) = self.ctx.read().expect("ctx mutex poisoned").clone() {
            ctx.diagnostics
                .record_error("ENGINE_START_FAILED", format!("{ENGINE_ID}: {message}"));
        }
    }

    /// Settle every active run of a generation to a failed terminal and
    /// release all pending permissions (fail-closed, §71–§73). Called before
    /// the process dies (stop/kill/crash) so no run is ever left eternally
    /// active and no UI permission is orphaned. Terminal emission is gated by
    /// each run's CAS, so a racing prompt task can never double-emit.
    fn fail_runs_of_generation(&self, generation: u64, reason: &str) {
        let ctx = self.ctx.read().expect("ctx mutex poisoned").clone();
        let bus = ctx.as_ref().map(|c| c.bus.clone());
        let Some(bus) = bus else { return };
        for record in self.runs.take_all(generation, reason) {
            // At teardown (stop/kill/fail_runtime) the process exit is NOT yet
            // proven and no authoritative terminal was observed for this run,
            // so its outcome is genuinely UNPROVABLE — emit Unknown
            // (non-releasing), never a definitive FAILED. A definitive FAILED
            // would release the workspace ownership while the external agent
            // may still be mutating files; the run must stay ambiguous and
            // reconcile only on a later authoritative terminal or proven death
            // (TASK 24 §9).
            let outcome = TerminalOutcome::Unknown(format!("{reason} (outcome unprovable at teardown)"));
            emit_terminal(&bus, &record, outcome);
            if let Some(task) = record
                .prompt_task
                .lock()
                .expect("prompt task mutex poisoned")
                .take()
            {
                task.abort();
            }
        }
        // Release pending permission senders → handlers settle reject.
        let _ = self.permissions.clear();
    }

    /// Full deterministic teardown of one runtime: cancel, close transport,
    /// protocol stdin EOF, supervisor stop (graceful → force), join tasks.
    /// Idempotent (tasks are `take`n). Returns an error if process termination is unproven.
    async fn teardown_runtime(&self, runtime: &Arc<Runtime>) -> Result<(), EngineError> {
        let _ = runtime.cancel_tx.send(true);
        runtime.transport.close("stopped").await;
        runtime.process.stdin_close().await;
        let mut stop_res = Ok(());
        let ctx = self.ctx.read().expect("ctx mutex poisoned").clone();
        if let Some(ctx) = ctx {
            match ctx.supervisor.stop(&runtime.process, true).await {
                Ok(_) | Err(saiwork_process::ProcessError::NotRunning { .. }) => {}
                Err(e) => {
                    warn!(
                        engine = ENGINE_ID,
                        generation = runtime.generation,
                        error = %e,
                        "harness graceful stop failed; forcing"
                    );
                    match ctx.supervisor.stop(&runtime.process, false).await {
                        Ok(_) | Err(saiwork_process::ProcessError::NotRunning { .. }) => {}
                        Err(fe) => {
                            stop_res = Err(EngineError::engine(ENGINE_ID, format!("force stop failed: {fe}")));
                        }
                    }
                }
            }
        }
        let mut tasks = std::mem::take(&mut *runtime.tasks.lock().expect("tasks mutex poisoned"));
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        stop_res
    }

    fn clear_runtime_if_generation(&self, generation: u64) -> bool {
        let mut slot = self
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned");
        if slot.as_ref().map(|runtime| runtime.generation) == Some(generation) {
            slot.take();
            true
        } else {
            false
        }
    }

    async fn finish_failed_start(
        &self,
        runtime: &Arc<Runtime>,
        failure: HarnessError,
        phase: &str,
    ) -> EngineError {
        let canceled = matches!(failure, HarnessError::Canceled);
        let failure_text = failure.to_string();
        match self.teardown_runtime(runtime).await {
            Ok(()) => {
                self.clear_runtime_if_generation(runtime.generation);
                if !canceled {
                    self.record_failure(&failure, phase);
                }
                failure.engine()
            }
            Err(cleanup) => {
                // Keep `runtime_slot` intact: process exit is unproven, so a
                // later explicit stop/kill must retain the exact authority.
                let combined = HarnessError::StartupCleanupFailed {
                    startup: failure_text,
                    cleanup: cleanup.to_string(),
                };
                self.record_failure(&combined, phase);
                combined.engine()
            }
        }
    }

    async fn cleanup_aborted_start(&self, runtime: Arc<Runtime>) {
        let cleanup = self.teardown_runtime(&runtime).await;
        if cleanup.is_ok() {
            self.clear_runtime_if_generation(runtime.generation);
            self.cancel_flag
                .lock()
                .expect("cancel flag mutex poisoned")
                .take();
        }

        let message = match cleanup {
            Ok(()) => "startup task was aborted before readiness".to_string(),
            Err(error) => format!(
                "startup task was aborted and cleanup failed; process termination is unproven: {error}"
            ),
        };
        let changed = {
            let mut state = self.state.write().expect("state mutex poisoned");
            if matches!(*state, AdapterState::Starting) {
                *state = AdapterState::Failed {
                    message: message.clone(),
                };
                true
            } else {
                false
            }
        };
        if changed {
            *self.last_error.write().expect("last error mutex poisoned") = Some(message.clone());
            if let Some(ctx) = self.ctx.read().expect("ctx mutex poisoned").clone() {
                ctx.diagnostics.record_error(
                    "ENGINE_START_FAILED",
                    format!("{ENGINE_ID}: {message}"),
                );
                (ctx.report_failure)(ENGINE_ID, &message);
            }
        }
    }

    /// Unexpected runtime/protocol death while Starting/Ready: fail the
    /// engine, settle active runs, report through the failure hook, tear
    /// down. Generation-guarded so a stale runtime cannot affect a newer one.
    async fn fail_runtime(&self, generation: u64, message: String) {
        {
            let state = self.state.read().expect("state mutex poisoned");
            if !matches!(*state, AdapterState::Starting | AdapterState::Ready) {
                return;
            }
        }
        {
            let slot = self
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned");
            if slot.as_ref().map(|r| r.generation) != Some(generation) {
                return; // stale generation
            }
        }
        // Settle active runs first (order explicit: run cleanup before the
        // process teardown, §71–§73).
        self.fail_runs_of_generation(generation, &message);
        if !matches!(
            *self.state.read().expect("state mutex poisoned"),
            AdapterState::Failed { .. }
        ) {
            self.set_state(AdapterState::Failed {
                message: message.clone(),
            });
        }
        *self.last_error.write().expect("last error mutex poisoned") = Some(message.clone());
        let ctx = self.ctx.read().expect("ctx mutex poisoned").clone();
        if let Some(ctx) = ctx {
            (ctx.report_failure)(ENGINE_ID, &message);
            ctx.diagnostics
                .record_error("ENGINE_FAILED", format!("{ENGINE_ID}: {message}"));
        }
        let slot = self
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned")
            .take();
        if let Some(r) = slot {
            if r.generation == generation {
                let _ = self.teardown_runtime(&r).await;
            }
        }
        // Connection-owned sessions die with the runtime (§75).
        self.sessions.clear();
    }
}

pub struct HarnessAdapter {
    inner: Arc<Inner>,
}

impl HarnessAdapter {
    pub fn new(config: HarnessConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                state: RwLock::new(AdapterState::Unknown),
                runtime_slot: Mutex::new(None),
                ctx: RwLock::new(None),
                start_lock: tokio::sync::Mutex::new(()),
                cancel_flag: Mutex::new(None),
                generation: AtomicU64::new(0),
                last_error: RwLock::new(None),
                server_info: RwLock::new(None),
                protocol_version: RwLock::new(String::new()),
                last_handshake_ms: RwLock::new(None),
                runtime_cwd: RwLock::new(None),
                runs: Arc::new(RunRegistry::new()),
                sessions: Arc::new(SessionRegistry::new()),
                permissions: Arc::new(PermissionRegistry::new()),
            }),
        }
    }

    fn identity_static(&self) -> EngineIdentity {
        EngineIdentity {
            id: ENGINE_ID.into(),
            display_name: self.inner.config.label.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            // DeepSeek Harness is Developer Preview — the UI marks it
            // experimental and never hides instability (TASK 21 §88).
            experimental: true,
        }
    }

    // ---- test/diagnostics accessors (adapter-local, no secrets) -----------

    pub fn state_label(&self) -> String {
        match &*self.inner.state.read().expect("state mutex poisoned") {
            AdapterState::Unknown => "unknown".into(),
            AdapterState::Starting => "starting".into(),
            AdapterState::Ready => "ready".into(),
            AdapterState::Stopping => "stopping".into(),
            AdapterState::Stopped => "stopped".into(),
            AdapterState::Failed { message } => format!("failed: {message}"),
        }
    }

    pub fn running_generation(&self) -> Option<u64> {
        self.inner
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned")
            .as_ref()
            .map(|r| r.generation)
    }

    pub fn task_count(&self) -> usize {
        let slot = self
            .inner
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned");
        match slot.as_ref() {
            Some(r) => r.tasks.lock().expect("tasks mutex poisoned").len(),
            None => 0,
        }
    }

    pub fn pending_requests(&self) -> usize {
        let slot = self
            .inner
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned");
        match slot.as_ref() {
            Some(r) => r.transport.pending_count(),
            None => 0,
        }
    }

    pub fn server_info(&self) -> Option<crate::protocol::ServerInfo> {
        self.inner
            .server_info
            .read()
            .expect("server info mutex poisoned")
            .clone()
    }

    pub fn protocol_version(&self) -> String {
        self.inner
            .protocol_version
            .read()
            .expect("protocol version mutex poisoned")
            .clone()
    }

    pub fn last_error(&self) -> Option<String> {
        self.inner
            .last_error
            .read()
            .expect("last error mutex poisoned")
            .clone()
    }

    pub fn last_handshake_ms(&self) -> Option<u64> {
        *self
            .inner
            .last_handshake_ms
            .read()
            .expect("handshake mutex poisoned")
    }

    /// Active runs (TASK 21 resource-cleanliness tests).
    pub fn active_runs(&self) -> usize {
        self.inner.runs.active_count()
    }

    /// Pending permission requests awaiting a decision (TASK 21 §160).
    pub fn pending_permissions(&self) -> usize {
        self.inner.permissions.len()
    }

    /// Bounded optional metadata request over the live protocol (TASK 20
    /// §100): a request timeout is operation-local — the runtime stays
    /// healthy. `Err` when no runtime is active.
    pub async fn request_metadata(
        &self,
        method: &str,
        params: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, HarnessError> {
        let ready = matches!(
            *self.inner.state.read().expect("state mutex poisoned"),
            AdapterState::Ready
        );
        if !ready {
            return Err(HarnessError::Unsupported("no active runtime".into()));
        }
        let runtime = {
            let slot = self
                .inner
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned");
            slot.as_ref().cloned()
        };
        let Some(runtime) = runtime else {
            return Err(HarnessError::Unsupported("no active runtime".into()));
        };
        runtime.transport.request(method, params, timeout).await
    }

    /// Copy the live runtime handle (transport + generation + bus) out of the
    /// slot so no lock guard survives an await. `Err` when not Ready.
    fn live_runtime(&self) -> Result<(Arc<Runtime>, EventBus, u64), HarnessError> {
        let ready = matches!(
            *self.inner.state.read().expect("state mutex poisoned"),
            AdapterState::Ready
        );
        if !ready {
            return Err(HarnessError::Unsupported("engine not ready".into()));
        }
        let runtime = {
            let slot = self
                .inner
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned");
            slot.as_ref().cloned()
        };
        let runtime =
            runtime.ok_or_else(|| HarnessError::Unsupported("no active runtime".into()))?;
        let bus = self
            .inner
            .ctx
            .read()
            .expect("ctx mutex poisoned")
            .as_ref()
            .map(|c| c.bus.clone())
            .ok_or_else(|| HarnessError::Unsupported("no engine context".into()))?;
        let generation = runtime.generation;
        Ok((runtime, bus, generation))
    }

    /// The effective runtime cwd for `session/new` (recorded at start). ACP
    /// requires an absolute primary cwd per session (§8); it is the BOUND
    /// workspace context, never the configured launcher cwd — a mutating
    /// session without a valid workspace context is a typed error (T-054).
    fn session_cwd(&self) -> Result<PathBuf, HarnessError> {
        self.inner
            .runtime_cwd
            .read()
            .expect("runtime cwd mutex poisoned")
            .clone()
            .ok_or_else(|| {
                HarnessError::ConfigurationInvalid(
                    "no workspace context for Harness sessions: open a workspace before starting the engine (the session cwd must be the bound workspace path)".into(),
                )
            })
    }
}

#[async_trait]
impl EngineAdapter for HarnessAdapter {
    fn identity(&self) -> EngineIdentity {
        self.identity_static()
    }

    /// Truthful capability set (TASK 21 §145–§146). Everything implemented
    /// and fixture-proven is true; anything not proven is false. `resume` is
    /// false — ACP sessions are fresh + connection-owned (DEEPSEEK_HARNESS.md
    /// §8). `models` is false — the ACP baseline advertises no machine-facing
    /// provider/model discovery; model selection delegates to the Harness
    /// profile default (`UseEngineDefault`, §23) and the UI never offers a
    /// selector. `parallel_sessions` is true — one in-flight prompt per ACP
    /// session, different sessions independent (§79–§81).
    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            streaming: true,
            sessions: true,
            resume: false,
            cancel: true,
            tools: true,
            permissions: true,
            attachments: false,
            images: false,
            models: false,
            usage: false,
            reasoning: false,
            context_window: None,
            worktrees: false,
            parallel_sessions: true,
            session_revert: false,
            structured_events: true,
        }
    }

    async fn start(&self, ctx: &EngineStartContext) -> Result<(), EngineError> {
        let _guard = self.inner.start_lock.lock().await;
        {
            let state = self.inner.state.read().expect("state mutex poisoned");
            if matches!(
                *state,
                AdapterState::Starting | AdapterState::Ready | AdapterState::Stopping
            ) {
                return Err(EngineError::AlreadyStarted {
                    engine_id: ENGINE_ID.into(),
                });
            }
        }
        let prior_runtime = self
            .inner
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned")
            .clone();
        if let Some(prior) = prior_runtime {
            if !prior.process.has_exited() {
                return Err(HarnessError::PreviousRuntimeTerminationUnproven {
                    pid: prior.process.pid(),
                }
                .engine());
            }
            self.inner.teardown_runtime(&prior).await?;
            self.inner.clear_runtime_if_generation(prior.generation);
        }
        self.inner.set_state(AdapterState::Starting);
        *self.inner.ctx.write().expect("ctx mutex poisoned") = Some(ctx.clone());

        // Cancel signal for stop-during-start (§54): created before any
        // phase so stop() can always reach it.
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        *self
            .inner
            .cancel_flag
            .lock()
            .expect("cancel flag mutex poisoned") = Some(cancel_tx.clone());
        let generation = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let started_at = Instant::now();

        // 1) Discovery + cheap probe (bounded).
        let executable = match self.inner.config.resolve_executable() {
            Ok(exe) => exe,
            Err(e) => {
                self.inner.record_failure(&e, "discovery");
                return Err(e.engine());
            }
        };
        if let Err(e) = self
            .inner
            .config
            .probe(&ctx.supervisor, &executable, generation)
            .await
        {
            self.inner.record_failure(&e, "probe");
            return Err(e.engine());
        }
        if *cancel_rx.borrow_and_update() {
            return Err(HarnessError::Canceled.engine());
        }

        // 2) Record the agent-session cwd — the ACP `session/new.cwd` — from
        // the BOUND engine-start workspace context (T-054). The launcher/process
        // cwd is configured separately (HarnessConfig.cwd) and may differ; the
        // agent must only ever mutate the workspace it is attributed to.
        *self
            .inner
            .runtime_cwd
            .write()
            .expect("runtime cwd mutex poisoned") = ctx.workspace_path.clone();

        // 3) Spawn the top-level runtime through the ProcessSupervisor.
        let spec =
            self.inner
                .config
                .runtime_spec(generation, &executable);
        let process = match ctx.supervisor.spawn(spec).await {
            Ok(p) => p,
            Err(e) => {
                let err = HarnessError::SpawnFailed(e.to_string());
                self.inner.record_failure(&err, "spawn");
                return Err(err.engine());
            }
        };
        if *cancel_rx.borrow_and_update() {
            let _ = ctx.supervisor.stop(&process, true).await;
            return Err(HarnessError::Canceled.engine());
        }

        // 4) Protocol stream + transport (one reader per runtime).
        let protocol_rx = match process.protocol_stream() {
            Some(rx) => rx,
            None => {
                let _ = ctx.supervisor.stop(&process, true).await;
                let err = HarnessError::Internal("protocol stream unavailable".into());
                self.inner.record_failure(&err, "transport");
                return Err(err.engine());
            }
        };
        let (
            transport,
            _monitor_dead,
            session_events_rx,
            tool_events_rx,
            server_requests_rx,
        ) = Transport::new(
            generation,
            process.clone(),
            protocol_rx,
            self.inner.config.frame_cap_bytes,
        );
        let runtime = Arc::new(Runtime {
            generation,
            process: process.clone(),
            transport,
            cancel_tx: cancel_tx.clone(),
            tasks: Mutex::new(tokio::task::JoinSet::new()),
        });
        *self
            .inner
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned") = Some(runtime.clone());
        let mut startup_ownership =
            StartupOwnershipGuard::new(self.inner.clone(), runtime.clone());

        // 5) ACP initialize handshake: bounded + cancel-aware.
        let params = crate::protocol::InitializeParams {
            protocol_version: crate::protocol::ACP_PROTOCOL_VERSION.into(),
            client_info: crate::protocol::ClientInfo {
                name: "saiwork2".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            capabilities: json!({}),
        };
        let params_value = match serde_json::to_value(&params) {
            Ok(v) => v,
            Err(e) => {
                // Partial-initialization rollback (§17/§147): the process and
                // transport reader are already live — tear them down before
                // returning, never leak a spawned runtime on a late failure.
                let err = HarnessError::Internal(format!("initialize params: {e}"));
                let result = self
                    .inner
                    .finish_failed_start(&runtime, err, "handshake")
                    .await;
                startup_ownership.disarm();
                return Err(result);
            }
        };
        let handshake = runtime.transport.request(
            "initialize",
            params_value,
            self.inner.config.handshake_timeout,
        );
        tokio::pin!(handshake);
        let handshake_result = tokio::select! {
            r = &mut handshake => r,
            _ = cancel_rx.changed() => Err(HarnessError::Canceled),
        };

        let init: crate::protocol::InitializeResult = match handshake_result {
            Ok(value) => match serde_json::from_value(value) {
                Ok(init) => init,
                Err(e) => {
                    let err = HarnessError::HandshakeRejected(format!(
                        "malformed initialize result: {e}"
                    ));
                    let result = self
                        .inner
                        .finish_failed_start(&runtime, err, "handshake")
                        .await;
                    startup_ownership.disarm();
                    return Err(result);
                }
            },
            Err(e) => {
                let err = match e {
                    HarnessError::RequestTimeout { .. } => {
                        HarnessError::HandshakeTimeout(self.inner.config.handshake_timeout)
                    }
                    HarnessError::RuntimeLost(reason) => HarnessError::ExitedDuringStartup(reason),
                    other => other,
                };
                let result = self
                    .inner
                    .finish_failed_start(&runtime, err, "handshake")
                    .await;
                startup_ownership.disarm();
                return Err(result);
            }
        };

        // Version/identity evidence (§13–§14, §132): server_info is required;
        // protocol version is recorded; newer/unknown versions are accepted.
        let server_info = match init.server_info {
            Some(info) if !info.name.is_empty() => info,
            Some(_) => {
                let err = HarnessError::HandshakeRejected(
                    "initialize result has an empty server_info.name".into(),
                );
                let result = self
                    .inner
                    .finish_failed_start(&runtime, err, "handshake")
                    .await;
                startup_ownership.disarm();
                return Err(result);
            }
            None => {
                let err =
                    HarnessError::HandshakeRejected("initialize result missing server_info".into());
                let result = self
                    .inner
                    .finish_failed_start(&runtime, err, "handshake")
                    .await;
                startup_ownership.disarm();
                return Err(result);
            }
        };
        *self
            .inner
            .server_info
            .write()
            .expect("server info mutex poisoned") = Some(server_info);
        *self
            .inner
            .protocol_version
            .write()
            .expect("protocol version mutex poisoned") = init.protocol_version.clone();

        // 6) TASK 21: spawn the session-event dispatcher + permission handler
        // (one each per runtime; owned by the runtime's JoinSet, §33/§169).
        let bus = ctx.bus.clone();
        let dispatcher = session_event_dispatcher(
            bus.clone(),
            self.inner.runs.clone(),
            runtime.transport.clone(),
            self.inner.config.prompt_timeout,
            session_events_rx,
        );
        runtime
            .tasks
            .lock()
            .expect("tasks mutex poisoned")
            .spawn(dispatcher);
        // Second dispatcher for the NON-DROPPABLE tool lifecycle lane (TASK
        // 24 §9): a `ToolCall completed/failed` can never be dropped on a
        // full stream lane, or the UI tool would stay permanently
        // started/output with no reconstruction in the final response.
        let tool_dispatcher = tool_event_dispatcher(
            bus.clone(),
            self.inner.runs.clone(),
            tool_events_rx,
        );
        runtime
            .tasks
            .lock()
            .expect("tasks mutex poisoned")
            .spawn(tool_dispatcher);
        let perm_handler = permission_handler(
            bus.clone(),
            self.inner.runs.clone(),
            self.inner.permissions.clone(),
            runtime.transport.clone(),
            server_requests_rx,
        );
        runtime
            .tasks
            .lock()
            .expect("tasks mutex poisoned")
            .spawn(perm_handler);

        // 7) Runtime ownership was published before the first handshake
        // await; now attach its monitor and promote it to READY.
        spawn_monitor(&self.inner, &runtime);
        *self
            .inner
            .last_handshake_ms
            .write()
            .expect("handshake mutex poisoned") = Some(started_at.elapsed().as_millis() as u64);
        self.inner.set_state(AdapterState::Ready);
        startup_ownership.disarm();
        let version = self
            .inner
            .server_info
            .read()
            .expect("server info mutex poisoned")
            .as_ref()
            .map(|s| s.version.clone())
            .unwrap_or_default();
        info!(
            engine = ENGINE_ID,
            generation,
            version = %version,
            protocol_version = %init.protocol_version,
            "deepseek-harness runtime ready"
        );
        Ok(())
    }

    async fn stop(&self) -> Result<(), EngineError> {
        {
            let state = self.inner.state.read().expect("state mutex poisoned");
            if matches!(*state, AdapterState::Unknown | AdapterState::Stopped) {
                return Ok(());
            }
        }
        self.inner.set_state(AdapterState::Stopping);
        if let Some(tx) = self
            .inner
            .cancel_flag
            .lock()
            .expect("cancel flag mutex poisoned")
            .clone()
        {
            let _ = tx.send(true);
        }
        let _guard = self.inner.start_lock.lock().await;
        let runtime = {
            self.inner
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned")
                .clone()
        };
        if let Some(runtime) = runtime {
            // Settle active runs before the process dies (order explicit,
            // §78–§79): no eternal RUNNING, no orphaned permission.
            self.inner
                .fail_runs_of_generation(runtime.generation, "engine stopping");
            match self.inner.teardown_runtime(&runtime).await {
                Ok(()) => {
                    self.inner
                        .runtime_slot
                        .lock()
                        .expect("runtime slot mutex poisoned")
                        .take();
                }
                Err(e) => {
                    self.inner.set_state(AdapterState::Failed {
                        message: format!("stop failed: {e}"),
                    });
                    return Err(e);
                }
            }
        }
        // Sessions are connection-owned: a fresh runtime is a fresh connection
        // with no sessions (§75, §11).
        self.inner.sessions.clear();
        self.inner
            .cancel_flag
            .lock()
            .expect("cancel flag mutex poisoned")
            .take();
        self.inner.set_state(AdapterState::Stopped);
        info!(engine = ENGINE_ID, "deepseek-harness runtime stopped");
        Ok(())
    }

    async fn kill(&self) -> Result<(), EngineError> {
        if let Some(tx) = self
            .inner
            .cancel_flag
            .lock()
            .expect("cancel flag mutex poisoned")
            .clone()
        {
            let _ = tx.send(true);
        }
        let _guard = self.inner.start_lock.lock().await;
        let runtime = {
            self.inner
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned")
                .clone()
        };
        if let Some(runtime) = runtime {
            self.inner
                .fail_runs_of_generation(runtime.generation, "engine killed");
            let _ = runtime.cancel_tx.send(true);
            runtime.transport.close("killed").await;
            runtime.process.stdin_close().await;
            let mut stop_res = Ok(());
            let ctx = self.inner.ctx.read().expect("ctx mutex poisoned").clone();
            if let Some(ctx) = ctx {
                if let Err(e) = ctx.supervisor.stop(&runtime.process, false).await {
                    stop_res = Err(EngineError::engine(ENGINE_ID, format!("kill failed: {e}")));
                }
            }
            let mut tasks =
                std::mem::take(&mut *runtime.tasks.lock().expect("tasks mutex poisoned"));
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            if let Err(e) = stop_res {
                self.inner.set_state(AdapterState::Failed {
                    message: format!("kill failed: {e}"),
                });
                return Err(e);
            }
            self.inner
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned")
                .take();
            self.inner.sessions.clear();
        }
        self.inner.set_state(AdapterState::Stopped);
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        match &*self.inner.state.read().expect("state mutex poisoned") {
            AdapterState::Unknown => EngineHealth::Unknown,
            AdapterState::Starting => EngineHealth::Starting,
            AdapterState::Ready => EngineHealth::Ready,
            AdapterState::Stopping | AdapterState::Stopped => EngineHealth::Stopped,
            AdapterState::Failed { message } => EngineHealth::Failed {
                message: message.clone(),
            },
        }
    }

    /// No machine-facing provider/model discovery on the ACP baseline; model
    /// selection delegates to the Harness profile default (`UseEngineDefault`,
    /// §23). No hardcoded model list (§22).
    async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: ENGINE_ID.into(),
            capability: "models",
        })
    }

    /// In-memory active runs (generic ids) for the core's lag-reconciliation.
    fn active_runs(&self) -> Vec<saiwork_core::engine::ActiveRun> {
        self.inner
            .runs
            .list_active()
            .into_iter()
            .map(|(session_id, run_id)| saiwork_core::engine::ActiveRun {
                session_id,
                run_id,
            })
            .collect()
    }

    /// Authoritative pending-permission snapshot (W2-004): every open
    /// permission request this engine holds, keyed by exact session/run/request
    /// ownership. Reconciliation rebuilds the UI permission cards from this
    /// after a bounded-bus lag, so a missed `permission.requested` state event
    /// is recoverable.
    fn pending_permissions(&self) -> Vec<saiwork_core::engine::PendingPermissionInfo> {
        self.inner.permissions.snapshot()
    }

    /// The authoritative live session set for this connection: the sessions
    /// this adapter created via `session/new` (ACP sessions are fresh +
    /// connection-owned, §9). Never filesystem scanning.
    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
        if !matches!(
            *self.inner.state.read().expect("state mutex poisoned"),
            AdapterState::Ready
        ) {
            return Err(EngineError::NotReady {
                engine_id: ENGINE_ID.into(),
            });
        }
        Ok(self
            .inner
            .sessions
            .list()
            .into_iter()
            .map(|s| SessionInfo {
                id: s.saiwork_id,
                engine_session_id: s.harness_id,
                display_name: s.display_name,
            })
            .collect())
    }

    /// Create a session through the real Harness protocol (`session/new`).
    /// `session.created` is emitted by SessionManager after authoritative
    /// success (§8) — the adapter does not double-publish. The authoritative
    /// creation outcome is returned: only `Created` proves an external
    /// session exists; an ambiguous create must never be loop-retried
    /// (TASK 24 §9).
    async fn create_session(
        &self,
        req: &CreateSessionRequest,
    ) -> Result<SessionCreation, EngineError> {
        let (runtime, _bus, generation) = self.live_runtime().map_err(HarnessError::engine)?;
        let cwd = self.session_cwd().map_err(HarnessError::engine)?;
        let params = NewSessionParams {
            cwd: cwd.to_string_lossy().into_owned(),
            additional_directories: Vec::new(),
            mcp_servers: Vec::new(),
        };
        let params_value = serde_json::to_value(&params)
            .map_err(|e| HarnessError::Internal(format!("session/new params: {e}")))
            .map_err(HarnessError::engine)?;
        let result = match runtime
            .transport
            .request(
                METHOD_SESSION_NEW,
                params_value,
                self.inner.config.prompt_timeout,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return Ok(classify_create_failure(e)),
        };
        let created: NewSessionResult = match serde_json::from_value(result) {
            Ok(v) => v,
            Err(e) => {
                return Ok(SessionCreation::CreationUnknown {
                    message: format!("malformed session/new result: {e}"),
                })
            }
        };
        if created.session_id.is_empty() {
            return Ok(SessionCreation::CreationUnknown {
                message: "session/new returned an empty sessionId".into(),
            });
        }
        // §32: the session was created on the runtime that answered; if that
        // runtime was replaced while the request was in flight, the response
        // must not become current authority.
        if !self.generation_matches(generation) {
            return Ok(SessionCreation::CreationUnknown {
                message: "runtime changed during session creation; response discarded".into(),
            });
        }
        // The generic SAIWORK2 id is minted by SessionManager and echoed
        // verbatim; the Harness session id is upstream-only (TASK 24 §9).
        let saiwork_id = req.session_id.clone();
        let display_name = req.title.clone().unwrap_or_else(|| {
            format!(
                "Harness session {}",
                &created.session_id[..created.session_id.len().min(8)]
            )
        });
        self.inner.sessions.insert(HarnessSession {
            saiwork_id: saiwork_id.clone(),
            harness_id: created.session_id.clone(),
            display_name: display_name.clone(),
        });
        info!(
            engine = ENGINE_ID,
            generation,
            session = %saiwork_id,
            harness_session = %created.session_id,
            "harness session created"
        );
        Ok(SessionCreation::Created {
            engine_session_id: created.session_id,
            display_name,
        })
    }

    /// Resume is unsupported: ACP sessions are fresh + connection-owned
    /// (DEEPSEEK_HARNESS.md §8). Never fabricate a reconstruction (§10).
    async fn resume_session(&self, _engine_session_id: &str) -> Result<SessionInfo, EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: ENGINE_ID.into(),
            capability: "resume",
        })
    }

    /// Delete a session: best-effort upstream `session/delete` (tolerated if
    /// the runtime answers method-not-found — dsh-acp is fresh-sessions-only)
    /// plus adapter-local removal. Not exercised by the app UI.
    async fn delete_session(&self, engine_session_id: &str) -> Result<(), EngineError> {
        let (runtime, _bus, generation) = self.live_runtime().map_err(HarnessError::engine)?;
        let params = DeleteSessionParams {
            session_id: engine_session_id.into(),
        };
        let params_value = serde_json::to_value(&params)
            .map_err(|e| HarnessError::Internal(format!("session/delete params: {e}")))
            .map_err(HarnessError::engine)?;
        let result = runtime
            .transport
            .request(
                METHOD_SESSION_DELETE,
                params_value,
                self.inner.config.prompt_timeout,
            )
            .await;
        // -32601 (method not found) is tolerated: the connection-owned session
        // dies with the runtime anyway. Anything else is a real error.
        if let Err(HarnessError::RequestRejected { code, .. }) = &result {
            if *code == -32601 {
                // tolerated
            } else {
                return Err(match result {
                    Err(e) => e.engine(),
                    _ => unreachable!(),
                });
            }
        }
        if !self.generation_matches(generation) {
            return Err(HarnessError::Unsupported("stale runtime".into()).engine());
        }
        // Adapter-local removal by the upstream id (find the SAIWORK2 id).
        let saiwork_id = self
            .inner
            .sessions
            .list()
            .into_iter()
            .find(|s| s.harness_id == engine_session_id)
            .map(|s| s.saiwork_id);
        if let Some(id) = saiwork_id {
            self.inner.sessions.remove(&id);
        }
        Ok(())
    }

    /// Send a prompt: validate → map SAIWORK2 session → register the run
    /// (same-session REJECT) → spawn the prompt task → **await the
    /// authoritative acceptance receipt**. `send()` returns only when the
    /// `session/prompt` response proves the prompt was accepted, definitely
    /// rejected, or the outcome is unprovable — never a locally allocated
    /// RunId passed off as acceptance (TASK 24 §9). The prompt task emits
    /// exactly one terminal from the authoritative stop reason (§22–§24, §67)
    /// and never auto-retries an ambiguous transport failure (§26, §128–§129).
    async fn send(&self, req: &SendRequest) -> Result<SendAcceptance, EngineError> {
        if !matches!(
            *self.inner.state.read().expect("state mutex poisoned"),
            AdapterState::Ready
        ) {
            return Err(EngineError::NotReady {
                engine_id: ENGINE_ID.into(),
            });
        }
        if req.prompt.trim().is_empty() {
            return Err(HarnessError::TurnRejected("empty prompt".into()).engine());
        }
        if req.prompt.len() > self.inner.config.max_prompt_bytes {
            return Err(HarnessError::TurnRejected(format!(
                "prompt exceeds the {} byte limit",
                self.inner.config.max_prompt_bytes
            ))
            .engine());
        }
        // Model selection is not machine-exposed on the ACP baseline: only the
        // Harness profile default (`UseEngineDefault`, §23) is supported. An
        // explicit model is an honest unsupported error, never a silent
        // fallback (§84).
        if let Some(model) = req.model.as_deref() {
            if !model.trim().is_empty() {
                return Err(EngineError::UnsupportedCapability {
                    engine_id: ENGINE_ID.into(),
                    capability: "models",
                });
            }
        }
        let session = self
            .inner
            .sessions
            .get(&req.session_id)
            .ok_or_else(|| HarnessError::SessionNotFound {
                session_id: req.session_id.clone(),
            })
            .map_err(HarnessError::engine)?;
        let (runtime, bus, generation) = self.live_runtime().map_err(HarnessError::engine)?;

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (terminal_tx, terminal_rx) = watch::channel(false);
        let (started_tx, started_rx) = watch::channel(false);
        let run = Arc::new(RunRecord {
            run_id: RunId::new(format!("run-{}", Uuid::new_v4())),
            session_id: session.saiwork_id.clone(),
            harness_session_id: session.harness_id.clone(),
            generation,
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
            cancel_tx,
            cancel_rx,
            started_emitted: std::sync::atomic::AtomicBool::new(false),
            started_tx,
            started_rx,
            message_id: Mutex::new(None),
            terminal_tools: Mutex::new(std::collections::HashSet::new()),
            terminal_tx,
            terminal_rx,
            state: Mutex::new(RunState::Running),
            terminal_emitted: std::sync::atomic::AtomicBool::new(false),
            prompt_task: Mutex::new(None),
        });
        // Same-session concurrency: REJECT (one in-flight prompt per ACP
        // session, §80–§81).
        self.inner
            .runs
            .insert(run.clone())
            .map_err(HarnessError::engine)?;

        let run_id = run.run_id.clone();
        let registry = self.inner.runs.clone();
        let transport = runtime.transport.clone();
        let prompt = req.prompt.clone();
        let timeout = self.inner.config.prompt_timeout;
        // The prompt task sends the authoritative acceptance receipt the
        // moment the session/prompt request resolves; send() waits for it.
        let (acceptance_tx, acceptance_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_prompt_task(
            bus.clone(),
            registry,
            run.clone(),
            transport,
            prompt,
            timeout,
            acceptance_tx,
        ));
        *run.prompt_task.lock().expect("prompt task mutex poisoned") = Some(task);
        info!(
            engine = ENGINE_ID,
            run = %run_id,
            session = %session.saiwork_id,
            harness_session = %session.harness_id,
            "harness run dispatched"
        );
        match acceptance_rx.await {
            Ok(acc) => Ok(acc),
            Err(_) => Ok(SendAcceptance::OutcomeUnknown {
                run_id: run_id.to_string(),
                message: "engine stopped before the send outcome was confirmed".into(),
            }),
        }
    }

    /// Real cancellation: mark the run cancel-requested (single CAS owner) and
    /// signal the prompt task, which is the single owner of the
    /// `session/cancel` protocol write (§64–§65). The prompt task's terminal
    /// is authoritative — if the runtime still reports a normal finish, that
    /// ordering wins (§67–§68). Never kills the engine process (§63).
    async fn cancel(&self, run_id: &str) -> Result<(), EngineError> {
        let Some(record) = self.inner.runs.request_cancel(run_id) else {
            // Unknown or already terminal: idempotent no-op (§66).
            return Ok(());
        };
        // Signal the prompt task (it performs the protocol write).
        let _ = record.cancel_tx_send();
        Ok(())
    }

    /// Resolve a pending `permission.requested` (Allow/Deny). Idempotent:
    /// an unknown/already-resolved/stale request is a no-op (EVENTS.md,
    /// §58–§60) — the server never receives a second decision.
    ///
    /// W2-002: the generic surface advertises SESSION-SCOPED permission
    /// mutation (`(session_id, request_id)`), but the registry is keyed only
    /// by request id. Owner correlation is verified before consuming: a
    /// mismatched `session_id` does NOT consume the pending entry of another
    /// session on the same engine — it is a typed no-op so a corrupt/stale UI
    /// pair cannot approve or deny a request that does not belong to it.
    async fn resolve_permission(
        &self,
        session_id: &str,
        request_id: &str,
        allowed: bool,
    ) -> Result<(), EngineError> {
        // Peek (no take) and verify owner correlation before consuming.
        let owner_matches = self.inner.permissions.session_matches(request_id, session_id);
        if !owner_matches {
            // Mismatched owner (B's entry must NOT be consumed for A's call)
            // or unknown/already-resolved request: idempotent no-op.
            return Ok(());
        }
        // Ownership verified: consume exactly this entry.
        if let Some(pending) = self.inner.permissions.take(request_id) {
            // Stale guard (§59): only deliver the decision while the owning
            // run is still active. A terminal run's handler has already
            // settled fail-closed (reject); the decision is moot. If the run
            // is active, the handler answers the upstream request and
            // publishes the authoritative permission.resolved.
            let active = self
                .inner
                .runs
                .get(&pending.run_id)
                .is_some_and(|r| !r.is_terminal());
            if active {
                let _ = pending.decision_tx.send(allowed);
            }
        }
        Ok(())
    }

    /// Best-effort synchronous cleanup: settle active runs and release
    /// pending permissions. Processes are owned and stopped by the
    /// ProcessSupervisor, not here.
    fn dispose(&self) {
        if let Some((runtime, _bus, generation)) = {
            let slot = self
                .inner
                .runtime_slot
                .lock()
                .expect("runtime slot mutex poisoned");
            slot.as_ref().map(|r| (r.clone(), (), r.generation))
        } {
            self.inner
                .fail_runs_of_generation(generation, "engine disposed");
            let _ = runtime.cancel_tx.send(true);
        }
    }
}

/// True when `generation` is still the live runtime's generation. Used to
/// discard responses that arrived after a restart, so a stale runtime can
/// never become current authority (§32).
impl HarnessAdapter {
    fn generation_matches(&self, generation: u64) -> bool {
        self.inner
            .runtime_slot
            .lock()
            .expect("runtime slot mutex poisoned")
            .as_ref()
            .is_some_and(|r| r.generation == generation)
    }
}

/// The session-event dispatcher (one per runtime): consumes routed
/// `session/update` notifications and normalizes them onto the generic bus,
/// routed by the stable upstream session id (§33–§34). Coalesced backpressure
/// warning when the transport dropped stream frames (§102). Owned by the
/// runtime's JoinSet; ends when the runtime tears down (channel closes).
async fn session_event_dispatcher(
    bus: EventBus,
    registry: Arc<RunRegistry>,
    transport: Transport,
    _prompt_timeout: Duration,
    mut rx: mpsc::Receiver<SessionUpdateNotification>,
) {
    let router = EventRouter {
        bus: bus.clone(),
        registry,
    };
    let mut last_dropped = transport.dropped_events();
    while let Some(notification) = rx.recv().await {
        let dropped = transport.dropped_events();
        if dropped != last_dropped {
            last_dropped = dropped;
            bus.publish(Event::RuntimeWarning {
                code: "HARNESS_STREAM_OVERFLOW".into(),
                message: format!(
                    "{dropped} harness session/update frame(s) dropped (backpressure)"
                ),
            });
        }
        router.on_session_update(&notification);
    }
}

/// The tool-lifecycle dispatcher (one per runtime): consumes the
/// NON-DROPPABLE state lane (TASK 24 §9) and routes tool updates through the
/// same `EventRouter` (run lookup, terminal guard, `mark_started` evidence,
/// normalization). Tool facts are keyed by ToolCallId and independent of text
/// chunk ordering, so lane separation cannot corrupt the projection.
async fn tool_event_dispatcher(
    bus: EventBus,
    registry: Arc<RunRegistry>,
    mut rx: mpsc::Receiver<SessionUpdateNotification>,
) {
    let router = EventRouter {
        bus,
        registry,
    };
    while let Some(notification) = rx.recv().await {
        router.on_session_update(&notification);
    }
}

/// The prompt task: the terminal authority for a run (§24). It sends the
/// `session/prompt` request, sends the authoritative acceptance receipt the
/// moment the request resolves (TASK 24 §9), observes cancellation (sending
/// `session/cancel` exactly once via its own flag), maps the authoritative
/// stop reason to exactly one terminal, drains a bounded window of final
/// committed chunks, and deregisters the run.
#[allow(clippy::too_many_arguments)]
async fn run_prompt_task(
    bus: EventBus,
    registry: Arc<RunRegistry>,
    run: Arc<RunRecord>,
    transport: Transport,
    prompt: String,
    timeout: Duration,
    acceptance_tx: tokio::sync::oneshot::Sender<SendAcceptance>,
) {
    // Cancel-before-dispatch (§114): if cancel was requested before the
    // prompt was sent, never send it — emit cancelled directly. Nothing
    // crossed the boundary, so the receipt is a definite "no acceptance".
    if run.cancel_requested.load(Ordering::SeqCst) {
        let _ = acceptance_tx.send(SendAcceptance::DefinitelyRejected {
            run_id: run.run_id.to_string(),
            code: "cancelled_before_dispatch".into(),
            message: "cancelled before the prompt was sent".into(),
        });
        emit_terminal(&bus, &run, TerminalOutcome::Cancelled);
        registry.remove(run.run_id.as_str());
        return;
    }

    let params = PromptParams {
        session_id: run.harness_session_id.clone(),
        prompt: vec![ContentBlock {
            kind: "text".into(),
            text: prompt,
        }],
    };
    let params_value = match serde_json::to_value(&params) {
        Ok(v) => v,
        Err(e) => {
            let _ = acceptance_tx.send(SendAcceptance::DefinitelyRejected {
                run_id: run.run_id.to_string(),
                code: "invalid".into(),
                message: format!("prompt serialization failed: {e}"),
            });
            emit_terminal(
                &bus,
                &run,
                TerminalOutcome::Failed(format!("prompt serialization failed: {e}")),
            );
            registry.remove(run.run_id.as_str());
            return;
        }
    };

    // Two-phase send: write the prompt frame FIRST (the acceptance boundary
    // — the runtime received the request and the run is live), then await the
    // response for the terminal. Never a locally allocated RunId as
    // acceptance (TASK 24 §9).
    let request = match transport
        .request_start(METHOD_SESSION_PROMPT, params_value, timeout)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            // The frame was never written: nothing was accepted — a definite
            // rejection (safe to record FAILED).
            let _ = acceptance_tx.send(SendAcceptance::DefinitelyRejected {
                run_id: run.run_id.to_string(),
                code: "prompt_not_sent".into(),
                message: format!("prompt send failed: {e}"),
            });
            emit_terminal(
                &bus,
                &run,
                TerminalOutcome::Failed(format!("prompt send failed: {e}")),
            );
            registry.remove(run.run_id.as_str());
            return;
        }
    };
    // The prompt frame was written — that is NOT acceptance (the runtime may
    // still reject the turn, and the same request can later resolve with
    // `RequestRejected`). Acceptance is proven only by actual execution
    // evidence: the first routed session/update for this run (the
    // dispatcher's `mark_started` fires `started_rx`) or the successful
    // final prompt response itself (a silent request is proven at its
    // response, TASK 24 §9). A `RequestRejected` before that evidence is a
    // definite rejection; transport loss after the write but before proof is
    // OutcomeUnknown — never a fabricated Accepted or Failed.
    let mut started_rx = run.started_rx.clone();
    let mut accepted = false;
    // The receipt is sent at most once (the Option is the single-owner gate;
    // `accepted` additionally records which kind was chosen).
    let mut acceptance = Some(acceptance_tx);
    macro_rules! send_acceptance {
        ($acc:expr) => {{
            if let Some(tx) = acceptance.take() {
                let _ = tx.send($acc);
            }
        }};
    }
    let mut cancel_rx = run.cancel_rx.clone();
    let mut cancel_sent = false;
    let mut response = Box::pin(request.await_response());
    let outcome;
    loop {
        // Evidence may already be present if an update was routed while we
        // were still constructing the await (watch: `borrow()` reflects the
        // latest value; `changed()` alone would then wait for a second bump).
        if !accepted && *started_rx.borrow() {
            accepted = true;
            send_acceptance!(SendAcceptance::Accepted {
                run_id: run.run_id.to_string(),
            });
        }
        tokio::select! {
            biased;
            // First routed session/update for this run: authoritative
            // execution evidence (§9, §30). The dispatcher already published
            // MessageStarted before routing the update. Biased so racing
            // execution evidence wins over a contradictory rejection.
            _ = started_rx.changed() => {
                if !accepted {
                    accepted = true;
                    send_acceptance!(SendAcceptance::Accepted {
                        run_id: run.run_id.to_string(),
                    });
                }
            }
            result = &mut response => {
                outcome = match result {
                    Ok(value) => {
                        // A successful final prompt response proves
                        // acceptance even without an earlier execution event
                        // (§9). The silent run still surfaces started so the
                        // terminal has a message (CAS: no-op if already
                        // emitted).
                        if !accepted {
                            accepted = true;
                            send_acceptance!(SendAcceptance::Accepted {
                                run_id: run.run_id.to_string(),
                            });
                        }
                        run.mark_started(&bus);
                        match serde_json::from_value::<PromptResult>(value) {
                            Ok(pr) => {
                                // Authoritative stop reason (§67): a normal
                                // finish wins over a racing cancel; cancelled
                                // wins too.
                                outcome_from_stop_reason(&pr.stop_reason)
                            }
                            Err(_) => TerminalOutcome::Unknown("malformed session/prompt result".into()),
                        }
                    }
                    Err(HarnessError::RequestRejected { code, message, .. }) => {
                        if !accepted {
                            // The runtime answered with an explicit rejection
                            // before any execution evidence: nothing was
                            // accepted — a definite rejection.
                            accepted = true;
                            send_acceptance!(SendAcceptance::DefinitelyRejected {
                                run_id: run.run_id.to_string(),
                                code: format!("rejected_{code}"),
                                message: format!("harness rejected the turn (code {code}): {message}"),
                            });
                        }
                        TerminalOutcome::Failed(format!(
                            "harness rejected the turn (code {code}): {message}"
                        ))
                    }
                    Err(HarnessError::RequestTimeout { .. }) => {
                        if !accepted {
                            // The request may still be executing on the
                            // runtime: unprovable, never a definite failure.
                            accepted = true;
                            send_acceptance!(SendAcceptance::OutcomeUnknown {
                                run_id: run.run_id.to_string(),
                                message: "prompt timed out before execution evidence".into(),
                            });
                        }
                        // Bounded run timeout: stop the agent best-effort and
                        // settle with an honest unknown outcome (§26, §127).
                        let _ = transport
                            .notify(METHOD_SESSION_CANCEL, cancel_params(&run))
                            .await;
                        TerminalOutcome::Unknown("run timed out (outcome unknown)".into())
                    }
                    Err(HarnessError::RuntimeLost(reason))
                    | Err(HarnessError::TransportClosed(reason)) => {
                        if !accepted {
                            // Transport loss after the write but before any
                            // proof: the runtime may hold the prompt —
                            // unprovable, never a definite failure.
                            accepted = true;
                            send_acceptance!(SendAcceptance::OutcomeUnknown {
                                run_id: run.run_id.to_string(),
                                message: format!("transport lost before execution evidence: {reason}"),
                            });
                        }
                        TerminalOutcome::Unknown(format!("harness runtime lost: {reason}"))
                    }
                    Err(HarnessError::Canceled) => {
                        if !accepted {
                            accepted = true;
                            send_acceptance!(SendAcceptance::DefinitelyRejected {
                                run_id: run.run_id.to_string(),
                                code: "cancelled".into(),
                                message: "cancelled before acceptance evidence".into(),
                            });
                        }
                        TerminalOutcome::Cancelled
                    }
                    Err(other) => {
                        if !accepted {
                            accepted = true;
                            send_acceptance!(SendAcceptance::OutcomeUnknown {
                                run_id: run.run_id.to_string(),
                                message: other.to_string(),
                            });
                        }
                        TerminalOutcome::Unknown(other.to_string())
                    }
                };
                break;
            }
            _ = cancel_rx.changed() => {
                if !cancel_sent && run.cancel_requested.load(Ordering::SeqCst) {
                    cancel_sent = true;
                    let _ = transport.notify(METHOD_SESSION_CANCEL, cancel_params(&run)).await;
                }
            }
        }
    }

    // Drain grace: give the dispatchers a bounded window to process final
    // committed chunks AND tool terminals so the UI never loses the tail
    // (§36–§37, §130; TASK 24 §9 — a tool completed/failed must not be lost
    // to teardown). Waits only while either lane still has pending frames;
    // bounded by ROUTE_DRAIN_GRACE so a slow peer can never delay the
    // terminal.
    let deadline = Instant::now() + ROUTE_DRAIN_GRACE;
    while (transport.route_pending() > 0 || transport.tool_route_pending() > 0)
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // One settle tick so the dispatchers process the last drained frames.
    if Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    emit_terminal(&bus, &run, outcome);
    registry.remove(run.run_id.as_str());
}

/// Classify a `session/new` failure into the authoritative creation outcome
/// (TASK 24 §9). A definitive protocol rejection means nothing was created;
/// anything where the request may have crossed the boundary (timeout,
/// runtime loss, transport close) is `CreationUnknown` — the runtime may hold
/// an orphan session and must never be loop-created.
fn classify_create_failure(e: HarnessError) -> SessionCreation {
    match e {
        HarnessError::RequestRejected { code, message, .. } => SessionCreation::DefinitelyNotCreated {
            code: format!("rejected_{code}"),
            message: format!("harness rejected session/new (code {code}): {message}"),
        },
        other => SessionCreation::CreationUnknown {
            message: other.to_string(),
        },
    }
}

fn cancel_params(run: &RunRecord) -> Value {
    serde_json::to_value(CancelParams {
        session_id: run.harness_session_id.clone(),
    })
    .unwrap_or(Value::Null)
}

/// The runtime monitor: watches protocol death, process exit, and the
/// stop/cancel signal. Owned by the runtime's JoinSet; generation-guarded.
fn spawn_monitor(inner: &Arc<Inner>, runtime: &Arc<Runtime>) {
    let inner = inner.clone();
    let runtime = runtime.clone();
    let generation = runtime.generation;
    let mut dead_rx = runtime.transport.dead();
    let mut exit_rx = runtime.process.exit();
    let mut cancel_rx = runtime.cancel_tx.subscribe();

    runtime
        .tasks
        .lock()
        .expect("tasks mutex poisoned")
        .spawn(async move {
            let initial_dead = dead_rx.borrow_and_update().clone();
            if let Some(reason) = initial_dead {
                if reason != "stopped" && reason != "killed" {
                    inner
                        .fail_runtime(generation, format!("harness protocol lost: {reason}"))
                        .await;
                    return;
                }
            }
            let initial_exit = *exit_rx.borrow_and_update();
            if let Some(info) = initial_exit {
                inner
                    .fail_runtime(
                        generation,
                        format!("harness process exited (code {:?})", info.code),
                    )
                    .await;
                return;
            }
            tokio::select! {
                _ = dead_rx.changed() => {
                    let reason = dead_rx.borrow().clone();
                    if let Some(reason) = reason {
                        if reason != "stopped" && reason != "killed" {
                            inner.fail_runtime(
                                generation,
                                format!("harness protocol lost: {reason}"),
                            ).await;
                        }
                    }
                }
                _ = exit_rx.changed() => {
                    let info = *exit_rx.borrow();
                    if let Some(info) = info {
                        inner.fail_runtime(
                            generation,
                            format!("harness process exited (code {:?})", info.code),
                        ).await;
                    }
                }
                _ = cancel_rx.changed() => {
                    // Normal stop: teardown owns the rest; nothing to do.
                }
            }
        });
}
