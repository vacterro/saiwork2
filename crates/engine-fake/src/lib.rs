//! FakeEngine — the first `EngineAdapter` implementation and permanent test
//! infrastructure (ADR-006/ADR-009, spec §32, TASK 07).
//!
//! Fully in-process, deterministic, network/credential-free. Behavior is
//! selected by typed `FakeScenario` presets (or a `/sim:<name>` prompt
//! directive / `fake:<name>` model for the contract path). It simulates:
//! normal/slow/burst/large streaming, empty/single/large responses,
//! mid-stream failure, hang, tool success/failure with permission gates,
//! raw-frame hostility (duplicate/malformed/unknown/out-of-order/connection
//! loss), engine crash, startup failure/hang, and cancellation — with exactly
//! one terminal outcome per run and no semantic events after it.
//!
//! The engine publishes only canonical events through the bus; it never
//! reaches into storage, the ProcessSupervisor, or the UI.

mod scenario;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineCapabilities, EngineError, EngineHealth,
    EngineIdentity, EngineStartContext, ModelInfo, RunHandle, SendAcceptance, SendRequest,
    SessionCreation, SessionInfo,
};
use saiwork_events::{Event, EventBus, RunId, SessionId};
use tokio::sync::{oneshot, watch};
use tokio::time::sleep;
use tracing::debug;
use uuid::Uuid;

pub use scenario::{
    FakeScenario, HostileMode, NormalizedFrame, PermissionStep, RawFrame, StartupMode, ToolStep,
};

const ENGINE_ID: &str = "fake";
const ENGINE_DISPLAY: &str = "FakeEngine";
const ENGINE_VERSION: &str = "0.1.0";
/// Bounded command history (law 13): tests inspect it, it never grows forever.
const COMMAND_HISTORY_CAP: usize = 256;

struct FakeSession {
    /// Generic SAIWORK2 session id (echoed from the create request).
    id: String,
    /// Upstream engine-owned session id (never used for event correlation).
    engine_session_id: String,
    display_name: String,
}

/// One in-flight run: the cancel signal (the emitted-delta counter lives in
/// `Inner.deltas` so it stays readable after completion).
struct FakeRun {
    /// Generic session id (for the core's active-run reconciliation).
    session_id: String,
    cancel_tx: watch::Sender<bool>,
}

/// Bounded wait for run workers to drain after stop/kill/dispose (§45, §47):
/// "engine stopped" means the engine's work is finished, not merely flagged.
const STOP_WORKER_TIMEOUT: Duration = Duration::from_secs(2);

/// Shared mutable state. `FakeEngine` is a thin shell over this so run tasks
/// can observe health changes (engine crash) and release pending permissions.
struct Inner {
    health: RwLock<EngineHealth>,
    sessions: Mutex<HashMap<String, FakeSession>>,
    runs: Mutex<HashMap<String, Arc<FakeRun>>>,
    /// request_id → resolution sender (dropped on stop/crash/dispose, which
    /// releases every awaiting run — §26).
    pending: Mutex<HashMap<String, oneshot::Sender<bool>>>,
    /// Live run worker handles; drained (bounded) by stop/kill/dispose so the
    /// engine never returns "stopped" while workers are still winding down.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Emitted-delta counter per run (test introspection). Not removed on
    /// completion so tests can read final counts; bounded (law 13).
    deltas: Mutex<HashMap<String, usize>>,
    /// Bounded command history for tests (§51–§52).
    commands: Mutex<VecDeque<String>>,
    active_runs: AtomicUsize,
    task_count: AtomicUsize,
    disposed: AtomicBool,
    start_cancel: AtomicBool,
    /// Monotonic raw-frame sequence (adapter boundary policy).
    last_raw_seq: AtomicU64,
}

pub struct FakeEngine {
    inner: Arc<Inner>,
    identity: EngineIdentity,
    capabilities: EngineCapabilities,
    ctx: RwLock<Option<EngineStartContext>>,
    startup: StartupMode,
}

impl Default for FakeEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeEngine {
    pub fn new() -> Self {
        Self::with_startup(StartupMode::Immediate)
    }

    pub fn with_startup(startup: StartupMode) -> Self {
        Self {
            inner: Arc::new(Inner {
                health: RwLock::new(EngineHealth::Unknown),
                sessions: Mutex::new(HashMap::new()),
                runs: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                tasks: Mutex::new(HashMap::new()),
                deltas: Mutex::new(HashMap::new()),
                commands: Mutex::new(VecDeque::new()),
                active_runs: AtomicUsize::new(0),
                task_count: AtomicUsize::new(0),
                disposed: AtomicBool::new(false),
                start_cancel: AtomicBool::new(false),
                last_raw_seq: AtomicU64::new(0),
            }),
            identity: EngineIdentity {
                id: ENGINE_ID.into(),
                display_name: ENGINE_DISPLAY.into(),
                version: ENGINE_VERSION.into(),
                experimental: false,
            },
            capabilities: EngineCapabilities {
                streaming: true,
                sessions: true,
                resume: true,
                cancel: true,
                tools: true,
                permissions: true,
                models: true,
                parallel_sessions: true,
                session_revert: false,
                structured_events: true,
                ..Default::default()
            },
            ctx: RwLock::new(None),
            startup,
        }
    }

    fn set_health(&self, health: EngineHealth) {
        *self
            .inner
            .health
            .write()
            .expect("fake health mutex poisoned") = health;
    }

    fn health_state(&self) -> EngineHealth {
        self.inner
            .health
            .read()
            .expect("fake health mutex poisoned")
            .clone()
    }

    fn record(&self, command: impl Into<String>) {
        let mut history = self
            .inner
            .commands
            .lock()
            .expect("fake commands mutex poisoned");
        if history.len() >= COMMAND_HISTORY_CAP {
            history.pop_front();
        }
        history.push_back(command.into());
    }

    fn require_started(&self) -> Result<(), EngineError> {
        match self.health_state() {
            EngineHealth::Ready => Ok(()),
            EngineHealth::Stopped | EngineHealth::Unknown => Err(EngineError::NotStarted {
                engine_id: ENGINE_ID.into(),
            }),
            EngineHealth::Failed { message } => Err(EngineError::Crashed {
                engine_id: ENGINE_ID.into(),
                message,
            }),
            EngineHealth::Starting | EngineHealth::Degraded { .. } => Err(EngineError::NotReady {
                engine_id: ENGINE_ID.into(),
            }),
        }
    }

    /// Test introspection: number of currently active fake runs (§47).
    pub fn active_runs(&self) -> usize {
        self.inner.active_runs.load(Ordering::SeqCst)
    }

    /// Test introspection: pending permission requests awaiting resolution.
    pub fn pending_permissions(&self) -> usize {
        self.inner
            .pending
            .lock()
            .expect("fake pending mutex poisoned")
            .len()
    }

    /// Test introspection: live background tasks (run/start workers).
    pub fn task_count(&self) -> usize {
        self.inner.task_count.load(Ordering::SeqCst)
    }

    /// Test introspection: bounded received-command history.
    pub fn received_commands(&self) -> Vec<String> {
        self.inner
            .commands
            .lock()
            .expect("fake commands mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// Test introspection: deltas emitted for a run id (valid while the run
    /// is live AND after it completes).
    pub fn emitted_deltas(&self, run_id: &str) -> usize {
        self.inner
            .deltas
            .lock()
            .expect("fake deltas mutex poisoned")
            .get(run_id)
            .copied()
            .unwrap_or(0)
    }

    /// Live raw-frame injection at the adapter boundary (test API, §31–§34):
    /// feeds a frame through the same `normalize_frame` policy the scripted
    /// hostile scenarios use. Returns the normalized outcome.
    pub fn push_raw(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        frame: RawFrame,
    ) -> NormalizedFrame {
        scenario::normalize_frame(&self.inner.last_raw_seq, session_id, run_id, &frame)
    }

    fn fake_models() -> Vec<ModelInfo> {
        [
            "default",
            "slow",
            "flood",
            "burst",
            "tool",
            "toolfail",
            "permission",
            "permdeny",
            "crash",
            "malformed",
            "duplicate",
            "unknown",
            "outoforder",
            "connloss",
            "hang",
            "fail",
            "empty",
            "single",
            "largedelta",
        ]
        .iter()
        .map(|m| ModelInfo {
            id: format!("fake:{m}"),
            display_name: format!("Fake {m}"),
            provider: Some("fake".into()),
            provider_name: Some("Fake".into()),
        })
        .collect()
    }

    /// Behavior from a `/sim:` directive, else from the `fake:*` model id,
    /// else `normal`.
    fn scenario_for(prompt: &str, model: Option<&str>) -> FakeScenario {
        for line in prompt.lines() {
            if let Some(rest) = line.strip_prefix("/sim:") {
                return FakeScenario::from_directive(rest.trim());
            }
        }
        if let Some(model) = model {
            if let Some(name) = model.strip_prefix("fake:") {
                return FakeScenario::from_directive(name);
            }
        }
        FakeScenario::normal()
    }

    /// Send with an explicit scenario (test/integration API; the trait path
    /// selects a preset via directive/model). Validated before spawning.
    pub async fn send_scenario(
        &self,
        req: &SendRequest,
        scenario: FakeScenario,
    ) -> Result<RunHandle, EngineError> {
        self.require_started()?;
        scenario.validate().map_err(|reason| {
            EngineError::engine(ENGINE_ID, format!("invalid scenario: {reason}"))
        })?;
        // Upstream calls use the engine session id; the generic id is only
        // for event correlation (TASK 24 §9).
        let sessions = self
            .inner
            .sessions
            .lock()
            .expect("fake sessions mutex poisoned");
        if !sessions
            .values()
            .any(|s| s.engine_session_id == req.engine_session_id)
        {
            return Err(EngineError::SessionNotFound {
                session_id: req.engine_session_id.clone(),
            });
        }
        drop(sessions);

        let run_id = Uuid::new_v4().to_string();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let run = Arc::new(FakeRun {
            session_id: req.session_id.clone(),
            cancel_tx,
        });
        self.inner
            .runs
            .lock()
            .expect("fake runs mutex poisoned")
            .insert(run_id.clone(), run.clone());
        {
            let mut deltas = self
                .inner
                .deltas
                .lock()
                .expect("fake deltas mutex poisoned");
            if deltas.len() >= 4096 {
                // Bounded test counter: evict an arbitrary entry (law 13).
                if let Some(oldest) = deltas.keys().next().cloned() {
                    deltas.remove(&oldest);
                }
            }
            deltas.insert(run_id.clone(), 0);
        }
        self.inner.active_runs.fetch_add(1, Ordering::SeqCst);
        self.inner.task_count.fetch_add(1, Ordering::SeqCst);
        self.record(format!("send:{}", scenario.label));

        let Some(ctx) = self
            .ctx
            .read()
            .expect("fake ctx mutex poisoned")
            .as_ref()
            .cloned()
        else {
            return Err(EngineError::NotStarted {
                engine_id: ENGINE_ID.into(),
            });
        };
        let inner = self.inner.clone();
        let session_id: SessionId = req.session_id.clone().into();
        let run_id_typed: RunId = run_id.clone().into();
        let label = scenario.label;
        let task = tokio::spawn(async move {
            run_task(inner, ctx, session_id, run_id_typed, scenario, cancel_rx).await;
        });
        self.inner
            .tasks
            .lock()
            .expect("fake tasks mutex poisoned")
            .insert(run_id.clone(), task);
        debug!(run = %run_id, label, "fake engine run started");
        Ok(RunHandle { run_id })
    }
}

/// Everything the run worker needs; `terminal` guards exactly-one-terminal
/// and no-events-after-terminal (§61–§62).
struct RunCtx {
    inner: Arc<Inner>,
    bus: EventBus,
    diagnostics: Arc<saiwork_diagnostics::Diagnostics>,
    session_id: SessionId,
    run_id: RunId,
    cancel_rx: watch::Receiver<bool>,
    terminal: bool,
}

impl RunCtx {
    fn canceled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    /// Engine health failure observed by this run (crash propagation).
    fn engine_failure(&self) -> Option<String> {
        match &*self
            .inner
            .health
            .read()
            .expect("fake health mutex poisoned")
        {
            EngineHealth::Failed { message } => Some(message.clone()),
            _ => None,
        }
    }

    fn publish(&self, event: Event) {
        self.bus.publish(event);
    }

    /// Publish a delta only while the run is live; counts emissions.
    fn publish_delta(&mut self, text: &str) -> bool {
        if self.terminal || self.canceled() || self.engine_failure().is_some() {
            return false;
        }
        let key = self.run_id.to_string();
        let mut deltas = self
            .inner
            .deltas
            .lock()
            .expect("fake deltas mutex poisoned");
        let entry = deltas.entry(key).or_insert(0);
        *entry += 1;
        drop(deltas);
        self.publish(Event::MessageDelta {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            delta: text.to_string(),
        });
        true
    }

    fn terminal_completed(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.publish(Event::MessageCompleted {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
        });
    }

    fn terminal_failed(&mut self, error: impl Into<String>) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.publish(Event::MessageFailed {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            error: error.into(),
        });
    }

    fn terminal_cancelled(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.publish(Event::MessageCancelled {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
        });
    }

    /// Apply one raw frame through the boundary policy.
    fn apply_raw(&mut self, frame: RawFrame) {
        match scenario::normalize_frame(
            &self.inner.last_raw_seq,
            &self.session_id,
            &self.run_id,
            &frame,
        ) {
            NormalizedFrame::Event(Event::MessageDelta { delta, .. }) => {
                self.publish_delta(&delta);
            }
            NormalizedFrame::Event(other) => {
                if !self.terminal {
                    self.publish(other);
                }
            }
            NormalizedFrame::ProtocolNote { kind, note } => {
                self.publish(Event::EngineRawEvent {
                    engine_id: ENGINE_ID.into(),
                    kind: kind.into(),
                    payload: note.clone(),
                });
                self.publish(Event::RuntimeWarning {
                    code: "PROTOCOL".into(),
                    message: note,
                });
            }
            NormalizedFrame::Unknown { kind } => {
                self.publish(Event::RuntimeWarning {
                    code: "UNKNOWN_EVENT".into(),
                    message: format!("ignored unknown raw frame kind '{kind}'"),
                });
            }
        }
    }
}

/// The run worker. Publishes `message.started`, drives the scenario, then
/// publishes exactly one terminal event and releases all held resources.
async fn run_task(
    inner: Arc<Inner>,
    ctx: EngineStartContext,
    session_id: SessionId,
    run_id: RunId,
    scenario: FakeScenario,
    cancel_rx: watch::Receiver<bool>,
) {
    let mut rc = RunCtx {
        inner: inner.clone(),
        bus: ctx.bus.clone(),
        diagnostics: ctx.diagnostics.clone(),
        session_id: session_id.clone(),
        run_id: run_id.clone(),
        cancel_rx,
        terminal: false,
    };
    rc.publish(Event::MessageStarted {
        session_id: session_id.clone(),
        run_id: run_id.clone(),
    });

    match scenario.hostile {
        HostileMode::None => run_normal_flow(&mut rc, &scenario).await,
        HostileMode::DuplicateFrame => run_duplicate_frame(&mut rc).await,
        HostileMode::MalformedFrame => run_malformed_frame(&mut rc).await,
        HostileMode::UnknownFrame => run_unknown_frame(&mut rc).await,
        HostileMode::OutOfOrderFrame => run_out_of_order(&mut rc).await,
        HostileMode::ConnectionLoss => run_connection_loss(&mut rc).await,
    }

    // Release all resources: drop the run record and counters. (Pending
    // permissions are released by their own flow or by stop/crash/dispose.)
    inner
        .runs
        .lock()
        .expect("fake runs mutex poisoned")
        .remove(run_id.as_str());
    inner
        .tasks
        .lock()
        .expect("fake tasks mutex poisoned")
        .remove(run_id.as_str());
    inner.active_runs.fetch_sub(1, Ordering::SeqCst);
    inner.task_count.fetch_sub(1, Ordering::SeqCst);
}

async fn run_normal_flow(rc: &mut RunCtx, scenario: &FakeScenario) {
    if scenario.empty {
        if rc.canceled() {
            rc.terminal_cancelled();
        } else {
            rc.terminal_completed();
        }
        return;
    }

    // Hang: no terminal until cancel/stop/dispose — or an engine crash. The
    // loop checks both so an engine failure always ends every active run
    // (§30) and no test can hang forever (§29).
    if scenario.hang {
        loop {
            sleep(Duration::from_millis(50)).await;
            if rc.canceled() {
                rc.terminal_cancelled();
                return;
            }
            if let Some(message) = rc.engine_failure() {
                rc.terminal_failed(format!("engine failed: {message}"));
                return;
            }
        }
    }

    for i in 0..scenario.deltas {
        if let Some(crash_at) = scenario.engine_crash_after_delta {
            if i >= crash_at {
                crash_engine(rc, "simulated engine crash");
                return;
            }
        }
        if rc.canceled() {
            rc.terminal_cancelled();
            return;
        }
        if let Some(message) = rc.engine_failure() {
            rc.terminal_failed(format!("engine failed: {message}"));
            return;
        }
        if let Some(after) = scenario.fail_after_delta {
            if i >= after {
                rc.terminal_failed("simulated run failure");
                return;
            }
        }

        let chunk = deterministic_chunk(i, scenario.delta_bytes);
        if !rc.publish_delta(&chunk) {
            return; // canceled or failed mid-iteration
        }

        // Interleave the tool script at the midpoint of the delta stream.
        if let Some(tool) = &scenario.tool {
            if i == scenario.deltas / 2 {
                run_tool(rc, tool).await;
                if rc.terminal {
                    return;
                }
            }
        }

        if scenario.delta_delay > Duration::ZERO {
            sleep(scenario.delta_delay).await;
        }
    }
    rc.terminal_completed();
}

/// The engine itself fails mid-run (§30/§75): mark the engine FAILED, record
/// diagnostics + publish `engine.failed`, release pending permissions, and
/// fail this run. Other active runs observe the FAILED health in their own
/// loops and terminate too.
fn crash_engine(rc: &mut RunCtx, message: &str) {
    *rc.inner.health.write().expect("fake health mutex poisoned") = EngineHealth::Failed {
        message: message.into(),
    };
    rc.diagnostics
        .record_error("FAKE_CRASH", message.to_string());
    rc.inner
        .pending
        .lock()
        .expect("fake pending mutex poisoned")
        .clear();
    rc.publish(Event::EngineFailed {
        engine_id: ENGINE_ID.into(),
        error: message.into(),
    });
    rc.terminal_failed(message.to_string());
}

/// One tool script: started → (permission gate) → output → completed|failed.
/// Tool failure fails the run but leaves the engine healthy (§73).
async fn run_tool(rc: &mut RunCtx, tool: &ToolStep) {
    // Stable per-invocation identity (TASK 24 §9): one tool call per step in
    // the fake scenario, so the call id is run-scoped and derived from the
    // tool name + run id — never the bare name, so the UI keys tools by call.
    let tool_call_id = format!("call-{}-{}", tool.name, rc.run_id);
    let run_id = rc.run_id.clone();
    rc.publish(Event::ToolStarted {
        session_id: rc.session_id.clone(),
        run_id: run_id.clone(),
        tool_call_id: tool_call_id.clone(),
        tool: tool.name.into(),
    });

    if let Some(step) = tool.permission {
        match await_permission(rc, tool.name, step).await {
            PermOutcome::Allowed => {}
            PermOutcome::Denied => {
                rc.publish(Event::ToolFailed {
                    session_id: rc.session_id.clone(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    tool: tool.name.into(),
                    error: "permission denied".into(),
                });
                rc.terminal_failed("permission denied");
                return;
            }
            PermOutcome::Cancelled => {
                rc.terminal_cancelled();
                return;
            }
        }
    }

    rc.publish(Event::ToolOutput {
        session_id: rc.session_id.clone(),
        run_id: run_id.clone(),
        tool_call_id: tool_call_id.clone(),
        tool: tool.name.into(),
        output: tool.output.clone(),
    });
    if tool.fail {
        rc.publish(Event::ToolFailed {
            session_id: rc.session_id.clone(),
            run_id: run_id.clone(),
            tool_call_id: tool_call_id.clone(),
            tool: tool.name.into(),
            error: "simulated tool failure".into(),
        });
        rc.terminal_failed("tool failed");
        return;
    }
    rc.publish(Event::ToolCompleted {
        session_id: rc.session_id.clone(),
        run_id: run_id.clone(),
        tool_call_id: tool_call_id.clone(),
        tool: tool.name.into(),
    });
}

enum PermOutcome {
    Allowed,
    Denied,
    Cancelled,
}

/// Publish `permission.requested` and wait for a resolution. Never blocks
/// forever: engine stop/crash/drop releases every pending sender (§26), and
/// user cancel aborts the wait.
async fn await_permission(rc: &mut RunCtx, tool_name: &str, step: PermissionStep) -> PermOutcome {
    let request_id = Uuid::new_v4().to_string();
    // TASK 24 §9: allocate the channel and insert pending ownership BEFORE
    // publishing PermissionRequested. If publication came first, an immediate
    // consumer (multi-thread runtime) could resolve the just-published
    // request while it is still absent; `resolve_permission` would idempotently
    // no-op, then the sender would be inserted and the Await would wait
    // forever.
    let (tx, rx) = oneshot::channel();
    rc.inner
        .pending
        .lock()
        .expect("fake pending mutex poisoned")
        .insert(request_id.clone(), tx);
    rc.publish(Event::PermissionRequested {
        session_id: rc.session_id.clone(),
        run_id: rc.run_id.clone(),
        request_id: request_id.clone().into(),
        detail: format!("FakeEngine requests permission to run: {tool_name}"),
    });

    let outcome = match step {
        PermissionStep::AutoAllow { auto_after_ms } => {
            sleep(Duration::from_millis(auto_after_ms)).await;
            if rc.canceled() {
                PermOutcome::Cancelled
            } else {
                PermOutcome::Allowed
            }
        }
        PermissionStep::AutoDeny { auto_after_ms } => {
            sleep(Duration::from_millis(auto_after_ms)).await;
            if rc.canceled() {
                PermOutcome::Cancelled
            } else {
                PermOutcome::Denied
            }
        }
        PermissionStep::Await => {
            let mut rx = rx;
            tokio::select! {
                resolved = &mut rx => match resolved {
                    Ok(allowed) => {
                        if allowed { PermOutcome::Allowed } else { PermOutcome::Denied }
                    }
                    Err(_) => PermOutcome::Cancelled, // sender dropped (stop/crash/dispose)
                },
                _ = rc.cancel_rx.changed() => PermOutcome::Cancelled,
            }
        }
    };

    // Release the pending slot (idempotent: resolve_permission already
    // removed it on the Allowed/Denied paths).
    rc.inner
        .pending
        .lock()
        .expect("fake pending mutex poisoned")
        .remove(&request_id);

    match outcome {
        PermOutcome::Allowed => rc.publish(Event::PermissionResolved {
            session_id: rc.session_id.clone(),
            run_id: rc.run_id.clone(),
            request_id: request_id.clone().into(),
            allowed: true,
        }),
        PermOutcome::Denied => rc.publish(Event::PermissionResolved {
            session_id: rc.session_id.clone(),
            run_id: rc.run_id.clone(),
            request_id: request_id.clone().into(),
            allowed: false,
        }),
        PermOutcome::Cancelled => {}
    }
    outcome
}

// ---- raw-boundary hostile flows (shared normalize_frame policy) ----

async fn run_duplicate_frame(rc: &mut RunCtx) {
    let frame = RawFrame {
        seq: 1,
        kind: "delta",
        payload: Some("dup".into()),
    };
    rc.apply_raw(frame.clone());
    rc.apply_raw(frame); // duplicate → protocol note, not a second delta
    rc.terminal_completed();
}

async fn run_malformed_frame(rc: &mut RunCtx) {
    rc.apply_raw(RawFrame {
        seq: 1,
        kind: "delta",
        payload: None, // malformed: no payload
    });
    rc.apply_raw(RawFrame {
        seq: 2,
        kind: "delta",
        payload: Some("ok".into()),
    });
    rc.terminal_completed();
}

async fn run_unknown_frame(rc: &mut RunCtx) {
    rc.apply_raw(RawFrame {
        seq: 1,
        kind: "unknown",
        payload: None,
    });
    rc.terminal_completed();
}

async fn run_out_of_order(rc: &mut RunCtx) {
    rc.apply_raw(RawFrame {
        seq: 2,
        kind: "delta",
        payload: Some("later".into()),
    });
    rc.apply_raw(RawFrame {
        seq: 1,
        kind: "delta",
        payload: Some("earlier".into()), // out of order → dropped with diagnostic
    });
    rc.apply_raw(RawFrame {
        seq: 3,
        kind: "delta",
        payload: Some("next".into()),
    });
    rc.terminal_completed();
}

async fn run_connection_loss(rc: &mut RunCtx) {
    rc.publish_delta("streaming…");
    rc.publish_delta("about to lose the transport…");
    // Logical transport disappears: the run fails, the engine stays healthy.
    rc.terminal_failed("connection lost");
}

/// Deterministic chunk content: a hex label repeated to the byte budget.
fn deterministic_chunk(i: usize, bytes: usize) -> String {
    let unit = format!("{:04x}", i % 0xffff);
    let mut out = String::with_capacity(bytes);
    while out.len() < bytes {
        out.push_str(&unit);
    }
    out.truncate(bytes);
    out
}

#[async_trait::async_trait]
impl EngineAdapter for FakeEngine {
    fn identity(&self) -> EngineIdentity {
        self.identity.clone()
    }

    fn capabilities(&self) -> EngineCapabilities {
        self.capabilities.clone()
    }

    async fn start(&self, ctx: &EngineStartContext) -> Result<(), EngineError> {
        match self.health_state() {
            EngineHealth::Starting | EngineHealth::Ready | EngineHealth::Degraded { .. } => {
                return Err(EngineError::AlreadyStarted {
                    engine_id: ENGINE_ID.into(),
                });
            }
            _ => {}
        }
        self.record("start");
        self.set_health(EngineHealth::Starting);

        match self.startup {
            StartupMode::Immediate => {
                self.set_health(EngineHealth::Ready);
                *self.ctx.write().expect("fake ctx mutex poisoned") = Some(ctx.clone());
                Ok(())
            }
            StartupMode::DelayedMs(ms) => {
                *self.ctx.write().expect("fake ctx mutex poisoned") = Some(ctx.clone());
                let mut elapsed: u64 = 0;
                while elapsed < ms {
                    sleep(Duration::from_millis(50)).await;
                    elapsed += 50;
                    if self.inner.start_cancel.load(Ordering::SeqCst)
                        || self.inner.disposed.load(Ordering::SeqCst)
                    {
                        self.set_health(EngineHealth::Stopped);
                        return Err(EngineError::Canceled);
                    }
                }
                self.set_health(EngineHealth::Ready);
                Ok(())
            }
            StartupMode::Fail => {
                self.set_health(EngineHealth::Failed {
                    message: "simulated startup failure".into(),
                });
                Err(EngineError::engine(ENGINE_ID, "simulated startup failure"))
            }
            StartupMode::Hang => {
                *self.ctx.write().expect("fake ctx mutex poisoned") = Some(ctx.clone());
                loop {
                    sleep(Duration::from_millis(100)).await;
                    if self.inner.start_cancel.load(Ordering::SeqCst)
                        || self.inner.disposed.load(Ordering::SeqCst)
                    {
                        self.set_health(EngineHealth::Stopped);
                        return Err(EngineError::Canceled);
                    }
                }
            }
        }
    }

    async fn stop(&self) -> Result<(), EngineError> {
        self.record("stop");
        self.inner.start_cancel.store(true, Ordering::SeqCst);
        // Cancel every active run; each reaches its terminal event itself.
        let runs: Vec<Arc<FakeRun>> = {
            let map = self.inner.runs.lock().expect("fake runs mutex poisoned");
            map.values().cloned().collect()
        };
        for run in runs {
            let _ = run.cancel_tx.send(true);
        }
        // Release pending permission waits (§26): senders drop → run awaits
        // resolve as cancelled.
        self.inner
            .pending
            .lock()
            .expect("fake pending mutex poisoned")
            .clear();
        // Drain run workers (bounded): after stop returns there are no live
        // fake tasks — "engine stopped" means work finished (§45, §47).
        let workers: Vec<tokio::task::JoinHandle<()>> = {
            let mut tasks = self.inner.tasks.lock().expect("fake tasks mutex poisoned");
            tasks.drain().map(|(_, h)| h).collect()
        };
        for worker in workers {
            let _ = tokio::time::timeout(STOP_WORKER_TIMEOUT, worker).await;
        }
        self.set_health(EngineHealth::Stopped);
        Ok(())
    }

    async fn kill(&self) -> Result<(), EngineError> {
        self.stop().await
    }

    fn health(&self) -> EngineHealth {
        self.health_state()
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
        self.require_started()?;
        Ok(Self::fake_models())
    }

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
        self.require_started()?;
        let sessions = self
            .inner
            .sessions
            .lock()
            .expect("fake sessions mutex poisoned");
        Ok(sessions
            .values()
            .map(|s| SessionInfo {
                id: s.id.clone(),
                engine_session_id: s.engine_session_id.clone(),
                display_name: s.display_name.clone(),
            })
            .collect())
    }

    async fn create_session(
        &self,
        req: &CreateSessionRequest,
    ) -> Result<SessionCreation, EngineError> {
        self.require_started()?;
        self.record("create_session");
        // The generic id is SAIWORK2-owned (echoed verbatim); the upstream
        // engine session id is minted here and must never collide with the
        // generic namespace.
        let id = req.session_id.clone();
        let engine_session_id = format!("fake-engine-{}", Uuid::new_v4());
        let display_name = req
            .title
            .clone()
            .unwrap_or_else(|| format!("Fake session {}", &id[..8]));
        let session = FakeSession {
            id: id.clone(),
            engine_session_id: engine_session_id.clone(),
            display_name: display_name.clone(),
        };
        self.inner
            .sessions
            .lock()
            .expect("fake sessions mutex poisoned")
            .insert(id.clone(), session);
        // Fake creation is synchronous and always authoritative.
        Ok(SessionCreation::Created {
            engine_session_id,
            display_name,
        })
    }

    /// In-memory active runs for the core's lag-reconciliation (generic ids).
    fn active_runs(&self) -> Vec<saiwork_core::engine::ActiveRun> {
        let runs = self.inner.runs.lock().expect("fake runs mutex poisoned");
        runs.iter()
            .map(|(run_id, run)| saiwork_core::engine::ActiveRun {
                session_id: run.session_id.clone(),
                run_id: run_id.clone(),
            })
            .collect()
    }

    async fn resume_session(&self, engine_session_id: &str) -> Result<SessionInfo, EngineError> {
        self.require_started()?;
        self.record("resume_session");
        let sessions = self
            .inner
            .sessions
            .lock()
            .expect("fake sessions mutex poisoned");
        let session = sessions
            .values()
            .find(|s| s.engine_session_id == engine_session_id)
            .ok_or_else(|| EngineError::SessionNotFound {
                session_id: engine_session_id.to_string(),
            })?;
        Ok(SessionInfo {
            id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            display_name: session.display_name.clone(),
        })
    }

    async fn delete_session(&self, engine_session_id: &str) -> Result<(), EngineError> {
        self.require_started()?;
        self.record("delete_session");
        self.inner
            .sessions
            .lock()
            .expect("fake sessions mutex poisoned")
            .retain(|_, s| s.engine_session_id != engine_session_id);
        Ok(())
    }

    async fn send(&self, req: &SendRequest) -> Result<SendAcceptance, EngineError> {
        let scenario = Self::scenario_for(&req.prompt, req.model.as_deref());
        let handle = self.send_scenario(req, scenario).await?;
        // Fake runs are synchronous in-memory simulations: the run is
        // immediately authoritative.
        Ok(SendAcceptance::Accepted {
            run_id: handle.run_id,
        })
    }

    async fn cancel(&self, run_id: &str) -> Result<(), EngineError> {
        self.record("cancel");
        let runs = self.inner.runs.lock().expect("fake runs mutex poisoned");
        if let Some(run) = runs.get(run_id) {
            let _ = run.cancel_tx.send(true);
        }
        // Unknown/already-completed run: no-op (idempotent, §20).
        Ok(())
    }

    async fn resolve_permission(
        &self,
        _session_id: &str,
        request_id: &str,
        allowed: bool,
    ) -> Result<(), EngineError> {
        self.record(if allowed {
            "resolve_permission:allow"
        } else {
            "resolve_permission:deny"
        });
        let sender = self
            .inner
            .pending
            .lock()
            .expect("fake pending mutex poisoned")
            .remove(request_id);
        if let Some(tx) = sender {
            let _ = tx.send(allowed);
        }
        // Unknown/already-resolved request: no-op (EVENTS.md idempotence).
        Ok(())
    }

    fn dispose(&self) {
        if self.inner.disposed.swap(true, Ordering::SeqCst) {
            return; // idempotent
        }
        self.record("dispose");
        self.inner.start_cancel.store(true, Ordering::SeqCst);
        self.set_health(EngineHealth::Stopped);
        let runs: Vec<Arc<FakeRun>> = {
            let map = self.inner.runs.lock().expect("fake runs mutex poisoned");
            map.values().cloned().collect()
        };
        for run in runs {
            let _ = run.cancel_tx.send(true);
        }
        self.inner
            .runs
            .lock()
            .expect("fake runs mutex poisoned")
            .clear();
        self.inner
            .pending
            .lock()
            .expect("fake pending mutex poisoned")
            .clear();
    }
}
