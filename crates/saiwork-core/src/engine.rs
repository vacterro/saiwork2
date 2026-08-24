//! EngineAdapter — the single logical contract (ENGINE_CONTRACT.md).
//!
//! Engine-specific behavior stops at this boundary (law 3). The registry is
//! the only place that knows which engine ids exist. Lifecycle events
//! (`engine.starting/ready/stopping/stopped/failed`) are published by the
//! registry; engines publish `message.*`, `tool.*`, `permission.*` and report
//! runtime health changes through `RuntimeContext`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{Event, EventBus};
use serde::Serialize;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("engine '{engine_id}': {message}")]
    Engine { engine_id: String, message: String },

    #[error("engine '{engine_id}' is not started")]
    NotStarted { engine_id: String },

    #[error("engine '{engine_id}' is not ready")]
    NotReady { engine_id: String },

    #[error("engine '{engine_id}' is already started")]
    AlreadyStarted { engine_id: String },

    #[error("network error: {0}")]
    Network(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("auth error: {0}")]
    Auth(String),

    #[error("session '{session_id}' not found")]
    SessionNotFound { session_id: String },

    /// One active run per session (ENGINE_CONTRACT.md, TASK 11 §70–§72): a
    /// second send to a busy session is rejected, never queued internally.
    #[error("session '{session_id}' is busy with an active run")]
    SessionBusy { session_id: String },

    #[error("run '{run_id}' not found")]
    RunNotFound { run_id: String },

    #[error("operation canceled")]
    Canceled,

    /// The send/create boundary was crossed but the outcome cannot be proven
    /// (transport loss mid-flight, engine died mid-handoff). The external
    /// side effect may exist; callers must never auto-retry.
    #[error("outcome cannot be proven: {0}")]
    OutcomeUnknown(String),

    #[error("engine '{engine_id}' crashed: {message}")]
    Crashed { engine_id: String, message: String },

    #[error("engine '{engine_id}' does not support capability '{capability}'")]
    UnsupportedCapability {
        engine_id: String,
        capability: &'static str,
    },

    /// The engine runtime is bound to one workspace (persisted at start); a
    /// session for a different workspace cannot execute against it. The UI
    /// must restart the engine for the new workspace (ADR-038 workspace
    /// binding, TASK 24 §9).
    #[error("engine '{engine_id}' is bound to workspace '{expected_workspace_id}' but session workspace is '{requested_workspace_id}'; restart the engine for that workspace")]
    WorkspaceMismatch {
        engine_id: String,
        expected_workspace_id: String,
        requested_workspace_id: String,
    },
}

impl EngineError {
    pub fn engine(engine_id: &str, message: impl Into<String>) -> Self {
        EngineError::Engine {
            engine_id: engine_id.into(),
            message: message.into(),
        }
    }
}

/// Static identity of an engine.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EngineIdentity {
    pub id: String,
    pub display_name: String,
    pub version: String,
    /// Engine is experimental (Developer Preview / unstable): the UI should
    /// mark it as such and never hide instability (TASK 21 §88).
    #[serde(default)]
    pub experimental: bool,
}

/// Identity + runtime health + normalized capabilities — the engine surface
/// the UI sees. Never engine-specific fields here (law 3).
#[derive(Debug, Clone, Serialize)]
pub struct EngineInfo {
    #[serde(flatten)]
    pub identity: EngineIdentity,
    pub health: EngineHealth,
    pub capabilities: EngineCapabilities,
    /// Workspace the runtime is currently bound to (`None` = not started or
    /// started without a workspace binding). A READY engine bound to a
    /// different workspace cannot serve that workspace until explicitly
    /// restarted for it — the UI disables create/send and the queue treats
    /// the item as Wait/NotReady, never FAILED (TASK 24 §9).
    #[serde(default)]
    pub bound_workspace_id: Option<String>,
}

/// Normalized capability set (ENGINE_CONTRACT.md). UI builds on these, never
/// on engine identity.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct EngineCapabilities {
    pub streaming: bool,
    pub sessions: bool,
    pub resume: bool,
    pub cancel: bool,
    pub tools: bool,
    pub permissions: bool,
    pub attachments: bool,
    pub images: bool,
    pub models: bool,
    pub usage: bool,
    pub reasoning: bool,
    pub context_window: Option<usize>,
    pub worktrees: bool,
    pub parallel_sessions: bool,
    /// Revert the last visible user turn and restore it (`unrevert`).
    pub session_revert: bool,
    pub structured_events: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ModelInfo {
    pub id: String,
    pub display_name: String,
    /// Provider key (the model's owning provider id, part of the composite
    /// `id`). Nullable for engines without provider concepts.
    pub provider: Option<String>,
    /// Provider display name (wire `Provider.name`), if the engine exposes
    /// one — the UI shows it instead of the raw key when present.
    pub provider_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionInfo {
    /// SAIWORK2 session id.
    pub id: String,
    /// Engine-owned session id.
    pub engine_session_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CreateSessionRequest {
    /// SAIWORK2-assigned generic session id (engine-independent, unique). The
    /// adapter echoes it in `SessionInfo.id` and uses it for every canonical
    /// event it publishes; the upstream engine session is created separately
    /// and returned as `SessionInfo.engine_session_id` (TASK 24 §9).
    pub session_id: String,
    pub workspace_id: Option<String>,
    pub model: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SendRequest {
    /// Generic SAIWORK2 session id — the identity used in canonical events
    /// (`message.*`, `tool.*`, `permission.*`) so the UI correlates runs to
    /// the generic session, never to the engine's own id.
    pub session_id: String,
    /// Upstream engine session id — the identity used for the actual engine
    /// call (send/abort/permission reply). Engines may mint their own ids;
    /// the generic and upstream ids can differ.
    pub engine_session_id: String,
    pub prompt: String,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RunHandle {
    pub run_id: String,
}

/// Authoritative outcome of an engine send at the acceptance boundary — the
/// ONLY evidence a caller may treat as "the engine accepted this prompt"
/// (never a locally allocated RunId, never MessageStarted). Distinguishes
/// real upstream acceptance from a definite pre-execution rejection and from
/// an unprovable outcome.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendAcceptance {
    /// The upstream engine authoritatively accepted the prompt; the run is
    /// live and its terminal arrives via `message.*` events.
    Accepted { run_id: String },
    /// The engine definitively rejected the prompt before executing it (no
    /// upstream mutation happened); safe to record FAILED. `run_id` is the
    /// locally allocated run that carries the FAILED terminal.
    DefinitelyRejected {
        run_id: String,
        code: String,
        message: String,
    },
    /// The send boundary was crossed (or the engine died mid-handoff) and
    /// acceptance cannot be proven; never auto-redispatch. `run_id` lets the
    /// caller correlate the eventual authoritative terminal.
    OutcomeUnknown { run_id: String, message: String },
}

/// Authoritative outcome of an engine session creation — mirrors
/// `SendAcceptance` for the create boundary, so an ambiguous create is never
/// silently retried into an orphan-session loop.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionCreation {
    /// The engine authoritatively created the upstream session.
    Created {
        engine_session_id: String,
        display_name: String,
    },
    /// Definite rejection before any creation happened.
    DefinitelyNotCreated { code: String, message: String },
    /// The create request crossed the boundary; creation may have happened.
    CreationUnknown { message: String },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EngineHealth {
    Unknown,
    Starting,
    Ready,
    Degraded { message: String },
    Stopped,
    Failed { message: String },
}

/// One normalized historical message from an engine's authoritative
/// session-history endpoint (TASK 24 §9). Read-only: never mirrored into
/// SQLite. Overlapping live events deduplicate against `id` (the stable
/// upstream message id). `order` is the engine's own ordering.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionMessage {
    /// Stable upstream message id — the dedup key for overlap with live
    /// `message.*`/`tool.*` events.
    pub id: String,
    /// "user" | "assistant" | "tool".
    pub role: String,
    pub text: String,
    pub tool_call_id: Option<String>,
    pub tool: Option<String>,
    pub order: u64,
    /// Authoritative upstream creation time in epoch milliseconds.
    pub ts: i64,
}

/// One active run as reported by an engine (generic ids only). Used by the
/// core running tracker to reconcile `session.running` state after the
/// bounded EventBus reports Lagged — the engine registry is authoritative
/// for liveness, the event stream is only the delivery hint (EVENTS.md).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRun {
    /// Generic SAIWORK2 session id.
    pub session_id: String,
    pub run_id: String,
}

/// Mid-run engine failure hook (publishes `engine.failed`). Named so the
/// callback type is readable and reusable.
pub type FailureSink = Arc<dyn Fn(&str, &str) + Send + Sync>;

/// Context handed to an engine at start: the workspace it runs in, the
/// normalized event bus, diagnostics, the process owner, and the
/// failure-reporting hook. Engines never spawn outside `supervisor`
/// (TASK 10 §12: ProcessSupervisor owns every engine process).
#[derive(Clone)]
pub struct EngineStartContext {
    /// The SAIWORK2 workspace id this runtime is bound to (None = no
    /// workspace binding). Persisted by the registry at start so a session
    /// for a different workspace is rejected with `WorkspaceMismatch` — a
    /// runtime's cwd is fixed at start and cannot be silently rebound.
    pub workspace_id: Option<String>,
    pub workspace_path: Option<PathBuf>,
    pub bus: EventBus,
    pub diagnostics: Arc<Diagnostics>,
    /// The one process owner; engine adapters build `ProcessSpec`s and spawn
    /// through it. Never `Command::new` directly (law 6, TASK 10 §12).
    pub supervisor: Arc<saiwork_process::ProcessSupervisor>,
    /// Report a mid-run engine failure (publishes `engine.failed`).
    pub report_failure: FailureSink,
}

/// Authoritative pending-permission snapshot (W2-004): the exact
/// session/run/request ownership a missed `permission.requested` event can be
/// reconstructed from after a bounded-bus lag. Never carries the decision
/// channel — it is read-only reconciliation state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingPermissionInfo {
    pub session_id: String,
    pub run_id: String,
    pub request_id: String,
    pub detail: String,
}

/// AUDIT-CORE-002: typed resolution for a pending user question. Questions
/// are NOT boolean permissions — each asked question owns a list of selected
/// labels/custom values (OpenCode wire shape `answers: string[][]`), and the
/// whole request can also be authoritatively rejected.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum QuestionResolution {
    /// One answer list per asked question, in order.
    Answers(Vec<Vec<String>>),
    /// Authoritatively reject the whole request.
    Rejected,
}

/// AUDIT-CORE-002: authoritative pending-question snapshot — the exact
/// session/run/request ownership a missed `question.asked` state event can
/// be reconstructed from after a bounded-bus lag. Read-only reconciliation
/// state; never carries the reply channel.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingQuestionInfo {
    pub session_id: String,
    pub run_id: String,
    pub request_id: String,
    /// Bounded, redacted JSON rendering of the asked questions.
    pub detail: String,
}

/// The one logical contract every engine implements (ENGINE_CONTRACT.md).
#[async_trait]
pub trait EngineAdapter: Send + Sync {
    fn identity(&self) -> EngineIdentity;
    fn capabilities(&self) -> EngineCapabilities;

    async fn start(&self, ctx: &EngineStartContext) -> Result<(), EngineError>;
    async fn stop(&self) -> Result<(), EngineError>;
    async fn kill(&self) -> Result<(), EngineError>;
    fn health(&self) -> EngineHealth;

    async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError>;
    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError>;
    async fn create_session(
        &self,
        req: &CreateSessionRequest,
    ) -> Result<SessionCreation, EngineError>;
    async fn resume_session(&self, engine_session_id: &str) -> Result<SessionInfo, EngineError>;
    async fn delete_session(&self, engine_session_id: &str) -> Result<(), EngineError>;

    /// Revert from a specific upstream user message. Engines without a
    /// verified revert API keep the fail-closed default.
    async fn revert_session(
        &self,
        _engine_session_id: &str,
        _message_id: &str,
    ) -> Result<(), EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: self.identity().id,
            capability: "session_revert",
        })
    }

    /// Clear the upstream revert boundary and restore reverted messages.
    async fn unrevert_session(&self, _engine_session_id: &str) -> Result<(), EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: self.identity().id,
            capability: "session_revert",
        })
    }

    /// Send a prompt. Returns the authoritative acceptance receipt: the
    /// caller must treat only `SendAcceptance::Accepted` as "the engine
    /// accepted the prompt" (TASK 24 §9).
    async fn send(&self, req: &SendRequest) -> Result<SendAcceptance, EngineError>;
    async fn cancel(&self, run_id: &str) -> Result<(), EngineError>;

    /// In-memory active runs (generic session id + run id). Used by the core
    /// running tracker to reconcile running state after the bounded EventBus
    /// reports Lagged (never a terminal for the tracker). Engines that keep a
    /// run registry override this; engines without persistent runs return the
    /// default empty list.
    fn active_runs(&self) -> Vec<ActiveRun> {
        Vec::new()
    }

    /// Authoritative pending-permission snapshot (W2-004): every permission
    /// request this engine currently holds open (awaiting a user decision or a
    /// run terminal). Reconciliation rebuilds the UI permission cards from this
    /// after a bounded-bus Lagged, so a missed `permission.requested` state
    /// event is recoverable. Engines without open permissions return the
    /// default empty list.
    fn pending_permissions(&self) -> Vec<PendingPermissionInfo> {
        Vec::new()
    }

    /// AUDIT-CORE-002: authoritative pending-question snapshot — every user
    /// question this engine currently holds open. Reconciliation rebuilds the
    /// UI question cards from this after a bounded-bus Lagged, so a missed
    /// `question.asked` state event is recoverable. Engines without open
    /// questions return the default empty list.
    fn pending_questions(&self) -> Vec<PendingQuestionInfo> {
        Vec::new()
    }

    /// Read-only authoritative session history (TASK 24 §9): engines that
    /// expose a history endpoint return `Some(normalized messages)`; engines
    /// without one return `Ok(None)` — the UI must then show that the
    /// history is unavailable instead of fabricating a complete empty thread.
    /// Never a SQLite transcript mirror. `session_id` is the engine's own
    /// upstream session id.
    async fn session_history(
        &self,
        _session_id: &str,
    ) -> Result<Option<Vec<SessionMessage>>, EngineError> {
        Ok(None)
    }

    /// Resolve a pending `permission.requested`. Idempotent: resolving an
    /// already-resolved/unknown request is a no-op (EVENTS.md). Engines that
    /// declare `permissions = false` never need to implement this.
    async fn resolve_permission(
        &self,
        _session_id: &str,
        _request_id: &str,
        _allowed: bool,
    ) -> Result<(), EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: "unknown".into(),
            capability: "permissions",
        })
    }

    /// AUDIT-CORE-002: answer or reject a pending user question. Typed (see
    /// `QuestionResolution`) — questions must never be forced through the
    /// boolean permission surface. Idempotent at the engine; resolving an
    /// already-resolved/unknown request is a no-op.
    async fn resolve_question(
        &self,
        _session_id: &str,
        _request_id: &str,
        _resolution: &QuestionResolution,
    ) -> Result<(), EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: "unknown".into(),
            capability: "questions",
        })
    }

    /// Best-effort synchronous cleanup (e.g. dropping watchers). Processes
    /// are owned and stopped by the ProcessSupervisor, not here.
    fn dispose(&self) {}
}

pub struct EngineRegistry {
    engines: RwLock<HashMap<String, Arc<dyn EngineAdapter>>>,
    /// engine_id → workspace id bound at start (None = started without a
    /// workspace). Read by SessionManager to reject create/send for a
    /// different workspace (TASK 24 §9).
    bindings: RwLock<HashMap<String, Option<String>>>,
    /// engine_id → runtime generation: incremented on every successful
    /// `start`. SessionManager uses it to decide `usable_now`: a
    /// connection-owned (resume=false) session is usable only while it was
    /// created/validated in the CURRENT runtime generation — after a
    /// stop/restart the old generation's sessions are unusable history
    /// (TASK 24 §9).
    generations: RwLock<HashMap<String, u64>>,
    bus: EventBus,
    diagnostics: Arc<Diagnostics>,
    supervisor: Arc<saiwork_process::ProcessSupervisor>,
    /// Per-engine lifecycle operation locks (TASK 24 §9): start/stop calls
    /// for the SAME engine serialize here, so concurrent double-start can
    /// never both pass the health precheck and the loser can never publish a
    /// stale `EngineFailed(AlreadyStarted)`; stop-during-start converges to
    /// one deterministic terminal state. Operations on different engines
    /// stay independent. Mutexes are created AT REGISTRATION — the set is
    /// bounded by registered engines and unknown ids allocate nothing (no
    /// `Box::leak`, TASK 24 §9).
    operations: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-engine binding-stability lease (W2-001): a read/write lock guarding
    /// the binding + runtime identity an engine exposes to binding-dependent
    /// session operations. Lifecycle transitions (start/stop/rebind) take the
    /// EXCLUSIVE (write) lease; `create`/`send` take a SHARED (read) lease
    /// spanning binding validation through the adapter's externally-visible
    /// acceptance boundary. This sequences a rebind fully before/after a
    /// session op — so a concurrent `stop`→`start(B)` can never flip the bound
    /// runtime under a `create`/`send` that already validated against the OLD
    /// binding (the "create under B, persist workspace A" defect). Mirrors
    /// `operations`: created at registration, bounded by registered engines,
    /// unknown ids allocate nothing (TASK 24 §9).
    leases: Mutex<HashMap<String, Arc<tokio::sync::RwLock<()>>>>,
}

impl EngineRegistry {
    pub fn new(
        bus: EventBus,
        diagnostics: Arc<Diagnostics>,
        supervisor: Arc<saiwork_process::ProcessSupervisor>,
    ) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            bindings: RwLock::new(HashMap::new()),
            generations: RwLock::new(HashMap::new()),
            bus,
            diagnostics,
            supervisor,
            operations: Mutex::new(HashMap::new()),
            leases: Mutex::new(HashMap::new()),
        }
    }

    /// The per-engine operation mutex (created at registration). Unknown ids
    /// panic via `expect` — callers validate the engine exists first, so an
    /// unknown id never allocates a mutex (TASK 24 §9).
    fn op_mutex(&self, engine_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.operations
            .lock()
            .expect("engine operations mutex poisoned")
            .get(engine_id)
            .cloned()
            .expect("operation mutex exists for every registered engine")
    }

    /// Per-engine lifecycle serialization gate. Held across the whole
    /// start/stop (precheck → adapter call → event publication), so exactly
    /// one lifecycle operation owns each transition; a second concurrent
    /// caller observes the settled state and publishes nothing. The owned
    /// guard is `'static` (the Arc keeps the mutex alive), so it survives the
    /// awaits inside start/stop — and the mutex set is bounded by REGISTERED
    /// engines (unknown ids allocate nothing, TASK 24 §9).
    async fn op_guard(&self, engine_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        self.op_mutex(engine_id).lock_owned().await
    }

    /// The per-engine binding-stability RwLock (created at registration,
    /// bounded by registered engines). Unknown ids panic via `expect` — the
    /// caller validates the engine exists first, so an unknown id never
    /// allocates a lease (TASK 24 §9).
    fn lease(&self, engine_id: &str) -> Arc<tokio::sync::RwLock<()>> {
        self.leases
            .lock()
            .expect("engine leases mutex poisoned")
            .get(engine_id)
            .cloned()
            .expect("binding lease exists for every registered engine")
    }

    /// Shared (read) binding-stability lease for a binding-dependent session
    /// operation (`create`/`send`). Held from binding validation through the
    /// adapter's externally-visible acceptance boundary so the bound runtime
    /// cannot change underneath the operation. The owned guard survives the
    /// adapter `.await`; returns the guard directly (callers validate the
    /// engine exists first, so unknown ids never allocate a lease — TASK 24
    /// §9). W2-001.
    pub async fn acquire_binding_read_lease(
        &self,
        engine_id: &str,
    ) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.lease(engine_id).read_owned().await
    }

    /// Exclusive (write) binding-stability lease for a lifecycle transition
    /// (start/stop/rebind). Held across the binding write so a concurrent
    /// binding-dependent session op is fully sequenced before/after the
    /// rebind — never interleaved. W2-001.
    async fn acquire_binding_write_lease(
        &self,
        engine_id: &str,
    ) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.lease(engine_id).write_owned().await
    }

    pub fn register(&self, engine: Arc<dyn EngineAdapter>) {
        let id = engine.identity().id.clone();
        // The operation mutex is created here (bounded by registered
        // engines); unknown ids never allocate one (TASK 24 §9).
        self.operations
            .lock()
            .expect("engine operations mutex poisoned")
            .entry(id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
        // The binding-stability lease mirrors the operation mutex: created at
        // registration, bounded by registered engines (W2-001).
        self.leases
            .lock()
            .expect("engine leases mutex poisoned")
            .entry(id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(())));
        self.engines
            .write()
            .expect("engine registry mutex poisoned")
            .insert(id, engine);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn EngineAdapter>> {
        self.engines
            .read()
            .expect("engine registry mutex poisoned")
            .get(id)
            .cloned()
    }

    /// Identity + health + capabilities for the UI (list_engines command).
    /// Deterministic ordering: OpenCode (canonical user default) first, then
    /// production adapters, then dev/test engines — never HashMap order, so
    /// the first/default engine is stable across launches (TASK 24 §9).
    pub fn list_info(&self) -> Vec<EngineInfo> {
        let map = self.engines.read().expect("engine registry mutex poisoned");
        let bindings = self
            .bindings
            .read()
            .expect("engine bindings mutex poisoned");
        let mut out: Vec<EngineInfo> = map
            .values()
            .map(|e| {
                // CORE-002: a dead/terminal runtime exposes NO workspace binding.
                // Reading `bindings` blindly (the old bug) kept the last start's
                // workspace glued to a corpse, so SessionManager/queue routed
                // sessions to a dead engine and `forget_workspace` refused to
                // delete the workspace (treated the corpse as a live blocker).
                let bound_workspace_id = if binding_is_live(&e.health()) {
                    bindings.get(&e.identity().id).cloned().flatten()
                } else {
                    None
                };
                EngineInfo {
                    identity: e.identity(),
                    health: e.health(),
                    capabilities: e.capabilities(),
                    bound_workspace_id,
                }
            })
            .collect();
        out.sort_by_key(|e| registry_order(&e.identity.id));
        out
    }

    /// Deterministic identity list (same order as `list_info`).
    pub fn list(&self) -> Vec<EngineIdentity> {
        let map = self.engines.read().expect("engine registry mutex poisoned");
        let mut out: Vec<EngineIdentity> = map.values().map(|e| e.identity()).collect();
        out.sort_by_key(|i| registry_order(&i.id));
        out
    }

    pub fn count(&self) -> usize {
        self.engines
            .read()
            .expect("engine registry mutex poisoned")
            .len()
    }

    /// Aggregate the authoritative pending-permission snapshot (W2-004) across
    /// every registered engine. Bounded by live requests; order follows the
    /// deterministic registry order.
    pub fn pending_permissions(&self) -> Vec<PendingPermissionInfo> {
        let map = self.engines.read().expect("engine registry mutex poisoned");
        let mut out: Vec<PendingPermissionInfo> = map
            .values()
            .flat_map(|e| e.pending_permissions())
            .collect();
        out.sort_by(|a, b| {
            a.session_id
                .cmp(&b.session_id)
                .then(a.run_id.cmp(&b.run_id))
                .then(a.request_id.cmp(&b.request_id))
        });
        out
    }

    /// AUDIT-CORE-002: aggregate the authoritative pending-question snapshot
    /// across every registered engine (same contract as
    /// `pending_permissions`).
    pub fn pending_questions(&self) -> Vec<PendingQuestionInfo> {
        let map = self.engines.read().expect("engine registry mutex poisoned");
        let mut out: Vec<PendingQuestionInfo> =
            map.values().flat_map(|e| e.pending_questions()).collect();
        out.sort_by(|a, b| {
            a.session_id
                .cmp(&b.session_id)
                .then(a.run_id.cmp(&b.run_id))
                .then(a.request_id.cmp(&b.request_id))
        });
        out
    }

    /// The workspace id an engine is currently bound to. `None` = unknown
    /// engine id; `Some(None)` = started without a workspace binding;
    /// `Some(Some(wid))` = bound to workspace `wid`.
    pub fn bound_workspace(&self, engine_id: &str) -> Option<Option<String>> {
        let engines = self.engines.read().expect("engine registry mutex poisoned");
        let engine = match engines.get(engine_id) {
            Some(e) => e,
            None => return None, // unknown engine id
        };
        // CORE-002: a dead/terminal runtime (Failed/Stopped/Unknown) exposes no
        // workspace binding — its last start association is gone. Returning
        // Some(None) lets `forget_workspace` treat it as not-bound (so a corpse
        // can't block deletion) and prevents SessionManager/queue from routing to
        // a dead engine (TASK 24 §9). The map may still carry a stale entry; the
        // death sink `report_failure` also clears it (defense in depth).
        if !binding_is_live(&engine.health()) {
            return Some(None);
        }
        let bindings = self.bindings.read().expect("engine bindings mutex poisoned");
        bindings.get(engine_id).cloned()
    }

    /// Current runtime generation for an engine (0 = never started this
    /// process, or unknown engine). Incremented on every successful start,
    /// so connection-owned sessions die with their generation (TASK 24 §9).
    pub fn generation(&self, engine_id: &str) -> u64 {
        self.generations
            .read()
            .expect("engine generations mutex poisoned")
            .get(engine_id)
            .copied()
            .unwrap_or(0)
    }

    /// Start an engine with lifecycle events. Publishes
    /// `engine.starting` → `engine.ready` | `engine.failed`. Persists the
    /// runtime's workspace binding at start — the runtime cwd is fixed, so
    /// the binding is the authority for workspace-vs-engine validation.
    pub async fn start(&self, id: &str, ctx: &EngineStartContext) -> Result<(), EngineError> {
        // Validate the engine exists BEFORE taking any operation lock: an
        // unknown id must allocate nothing and fail closed (TASK 24 §9).
        if self.get(id).is_none() {
            return Err(EngineError::engine(id, "unknown engine"));
        }
        // Serialize lifecycle operations per engine (TASK 24 §9): a second
        // concurrent start waits, then observes the settled READY/STARTING
        // health and returns AlreadyStarted WITHOUT publishing any event — a
        // healthy runtime can never be flipped into failed UI state by a
        // stale loser.
        let _guard = self.op_guard(id).await;
        // W2-001: the lifecycle transition holds the EXCLUSIVE binding-stability
        // lease across the binding write, so a concurrent binding-dependent
        // `create`/`send` is fully sequenced before/after this rebind — never
        // interleaved with it.
        let _lease = self.acquire_binding_write_lease(id).await;
        let engine = self
            .get(id)
            .ok_or_else(|| EngineError::engine(id, "unknown engine"))?;
        // AlreadyStarted means THIS registry already runs the runtime: the
        // settled loser of a concurrent start sees Ready + a recorded
        // generation. An adapter reporting Ready with no recorded generation
        // was never started here (e.g. attached to an already-live runtime);
        // refusing it would make such runtimes permanently unstartable.
        let ever_started = self
            .generations
            .read()
            .expect("engine generations mutex poisoned")
            .contains_key(id);
        let health = engine.health();
        if matches!(health, EngineHealth::Starting)
            || (matches!(health, EngineHealth::Ready) && ever_started)
        {
            return Err(EngineError::AlreadyStarted {
                engine_id: id.into(),
            });
        }
        self.bus.publish(Event::EngineStarting {
            engine_id: id.into(),
        });
        let result = engine.start(ctx).await;
        match &result {
            Ok(()) => {
                self.bindings
                    .write()
                    .expect("engine bindings mutex poisoned")
                    .insert(id.into(), ctx.workspace_id.clone());
                // New runtime generation: sessions validated under the old
                // generation are no longer usable-now (TASK 24 §9).
                {
                    let mut gens = self
                        .generations
                        .write()
                        .expect("engine generations mutex poisoned");
                    let next = gens.get(id).copied().unwrap_or(0) + 1;
                    gens.insert(id.into(), next);
                }
                self.bus.publish(Event::EngineReady {
                    engine_id: id.into(),
                });
            }
            Err(e) => {
                self.bindings
                    .write()
                    .expect("engine bindings mutex poisoned")
                    .remove(id);
                let message = e.to_string();
                self.diagnostics
                    .record_error("ENGINE_START_FAILED", format!("{id}: {message}"));
                self.bus.publish(Event::EngineFailed {
                    engine_id: id.into(),
                    error: message.clone(),
                });
            }
        }
        result
    }

    /// Stop an engine with lifecycle events: `engine.stopping` → `engine.stopped`.
    /// Releases the workspace binding only on the authoritative `engine.stopped`.
    pub async fn stop(&self, id: &str) -> Result<(), EngineError> {
        // Unknown ids allocate nothing and fail closed (TASK 24 §9).
        if self.get(id).is_none() {
            return Err(EngineError::engine(id, "unknown engine"));
        }
        // Serialize with a concurrent start for the same engine: a
        // stop-during-start waits for the start to settle, then stops the
        // now-READY runtime — one deterministic terminal state, no stale
        // event storm (TASK 24 §9).
        let _guard = self.op_guard(id).await;
        // W2-001: the lifecycle transition holds the EXCLUSIVE binding-stability
        // lease across the binding write, so a concurrent binding-dependent
        // `create`/`send` is fully sequenced before/after this rebind — never
        // interleaved with it.
        let _lease = self.acquire_binding_write_lease(id).await;
        let engine = self
            .get(id)
            .ok_or_else(|| EngineError::engine(id, "unknown engine"))?;
        self.bus.publish(Event::EngineStopping {
            engine_id: id.into(),
        });
        let result = engine.stop().await;
        match &result {
            Ok(()) => {
                self.bindings
                    .write()
                    .expect("engine bindings mutex poisoned")
                    .remove(id);
                self.bus.publish(Event::EngineStopped {
                    engine_id: id.into(),
                });
            }
            Err(e) => {
                self.diagnostics
                    .record_error("ENGINE_STOP_FAILED", format!("{id}: {e}"));
                // A failed stop must still publish ONE deterministic
                // terminal health event — otherwise the UI that saw
                // `engine.stopping` hangs in Stopping forever while the
                // adapter may be Ready/Failed/Stopped (TASK 24 §9). Query
                // the adapter's authoritative health and announce it.
                match engine.health() {
                    EngineHealth::Ready => {
                        self.bus.publish(Event::EngineReady {
                            engine_id: id.into(),
                        });
                    }
                    EngineHealth::Failed { message } => {
                        self.bus.publish(Event::EngineFailed {
                            engine_id: id.into(),
                            error: message,
                        });
                    }
                    EngineHealth::Stopped => {
                        self.bindings
                            .write()
                            .expect("engine bindings mutex poisoned")
                            .remove(id);
                        self.bus.publish(Event::EngineStopped {
                            engine_id: id.into(),
                        });
                    }
                    // Unknown/Starting/Degraded after a failed stop: surface
                    // the stop failure itself as the terminal event, but retain
                    // the binding until process termination is proven.
                    EngineHealth::Unknown
                    | EngineHealth::Starting
                    | EngineHealth::Degraded { .. } => {
                        let message = e.to_string();
                        self.bus.publish(Event::EngineFailed {
                            engine_id: id.into(),
                            error: format!("stop failed: {message}"),
                        });
                    }
                }
            }
        }
        result
    }

    /// Stop every engine CONCURRENTLY (TASK 24 perf): each stop can consume
    /// its full graceful+force budget, so serial stops would make shutdown
    /// latency grow ~N× budgets. Every engine still receives its stop
    /// (attempt-all) and every failure is retained; per-engine lifecycle
    /// ordering is unchanged. App owns the registry as `Arc`, which this
    /// signature requires for concurrent `'static` stop tasks.
    pub async fn stop_all(self: &Arc<Self>) {
        let ids: Vec<String> = self.list().into_iter().map(|i| i.id).collect();
        let mut handles = Vec::with_capacity(ids.len());
        for id in ids {
            let this = self.clone();
            handles.push(tokio::spawn(
                async move { (id.clone(), this.stop(&id).await) },
            ));
        }
        for handle in handles {
            match handle.await {
                Ok((_id, Ok(()))) => {}
                Ok((id, Err(e))) => warn!(engine = %id, error = %e, "engine stop failed"),
                Err(e) => warn!(error = %e, "engine stop task failed to join"),
            }
        }
    }

    /// Build an `EngineStartContext` for an engine. The failure hook is a
    /// single shared closure over bus + diagnostics — no self-referential
    /// cycles, works for any number of engines.
    /// CORE-002: release a dead runtime's workspace binding. Called from the
    /// runtime-death `report_failure` sink (via Weak upgrade, so the closure
    /// never forms a registry→adapter→registry reference cycle) and from
    /// `stop` on terminal health. A failed/stopped engine must expose NO
    /// binding — otherwise its last workspace association survives the death
    /// and blocks `forget_workspace` or routes sessions to a corpse.
    fn clear_binding_on_failure(&self, engine_id: &str) {
        self.bindings
            .write()
            .expect("engine bindings mutex poisoned")
            .remove(engine_id);
    }

    /// Build an `EngineStartContext` for an engine. The failure hook is a
    /// single shared closure over bus + diagnostics — no self-referential
    /// cycles, works for any number of engines. On a runtime death it ALSO
    /// releases the workspace binding (CORE-002): the closure upgrades a Weak
    /// registry handle, so the registry→adapter→registry cycle is avoided.
    pub fn start_context(
        self: &Arc<Self>,
        workspace_id: Option<String>,
        workspace_path: Option<PathBuf>,
    ) -> EngineStartContext {
        let bus = self.bus.clone();
        let diagnostics = self.diagnostics.clone();
        let failure_bus = bus.clone();
        // Weak handle lets the failure sink clear the binding without keeping
        // the registry alive — the adapter may retain the context for its whole
        // lifetime; a strong Arc would leak a cycle (TASK 24 §9).
        let weak = Arc::downgrade(self);
        EngineStartContext {
            workspace_id,
            workspace_path,
            bus,
            diagnostics: diagnostics.clone(),
            supervisor: self.supervisor.clone(),
            report_failure: Arc::new(move |engine_id: &str, message: &str| {
                diagnostics.record_error("ENGINE_FAILED", format!("{engine_id}: {message}"));
                // CORE-002: a mid-run crash must not leave a stale binding that
                // blocks `forget_workspace` or misleads SessionManager/queue.
                if let Some(reg) = weak.upgrade() {
                    reg.clear_binding_on_failure(engine_id);
                }
                failure_bus.publish(Event::EngineFailed {
                    engine_id: engine_id.into(),
                    error: message.into(),
                });
            }),
        }
    }
}

/// A workspace binding is only authoritative while the runtime is actually
/// running. The three "not running" states — Unknown (never started /
/// unknown), Stopped, Failed — have no usable binding. Exposing a
/// previously-bound workspace for a dead runtime is the CORE-002 defect:
/// `forget_workspace` would refuse to delete the workspace (treats the corpse
/// as a live blocker) and SessionManager/queue would route sessions against a
/// dead engine. `Starting`/`Ready`/`Degraded` are live: a degraded-but-running
/// runtime still owns its binding.
fn binding_is_live(health: &EngineHealth) -> bool {
    !matches!(
        health,
        EngineHealth::Failed { .. } | EngineHealth::Stopped | EngineHealth::Unknown
    )
}

/// Deterministic registry order (TASK 24 §9): the canonical user default
/// (OpenCode) first, then production adapters, then experimental/dev-test
/// engines. Never HashMap iteration order — the UI's `engines[0]` default
/// must be stable across launches.
fn registry_order(engine_id: &str) -> (u8, String) {
    match engine_id {
        "opencode" => (0, String::new()),
        "generic-cli" => (1, String::new()),
        "deepseek-harness" => (2, String::new()),
        "fake" => (9, String::new()),
        other => (3, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use saiwork_events::EventBus;

    /// What the adapter's authoritative health reports after a failed stop.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum StopFailureHealth {
        /// The adapter ignored the stop and remains Ready.
        Ready,
        /// The stop failure also failed the adapter.
        Failed,
        /// The adapter actually stopped but still reported an error.
        Stopped,
    }

    struct StopFailingEngine {
        id: String,
        mode: StopFailureHealth,
        health: std::sync::RwLock<EngineHealth>,
    }

    impl StopFailingEngine {
        fn new(id: &str, mode: StopFailureHealth) -> Self {
            Self {
                id: id.into(),
                mode,
                health: std::sync::RwLock::new(EngineHealth::Ready),
            }
        }
    }

    #[async_trait]
    impl EngineAdapter for StopFailingEngine {
        fn identity(&self) -> EngineIdentity {
            EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }

        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities::default()
        }

        async fn start(&self, _ctx: &EngineStartContext) -> Result<(), EngineError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), EngineError> {
            match self.mode {
                StopFailureHealth::Ready => Err(EngineError::engine(
                    &self.id,
                    "stop failed but the adapter is still ready",
                )),
                StopFailureHealth::Failed => {
                    *self.health.write().expect("health mutex") = EngineHealth::Failed {
                        message: "adapter failed during stop".into(),
                    };
                    Err(EngineError::engine(&self.id, "stop failed; adapter failed"))
                }
                StopFailureHealth::Stopped => {
                    *self.health.write().expect("health mutex") = EngineHealth::Stopped;
                    Err(EngineError::engine(
                        &self.id,
                        "stop reported an error but stopped",
                    ))
                }
            }
        }

        async fn kill(&self) -> Result<(), EngineError> {
            Ok(())
        }

        fn health(&self) -> EngineHealth {
            self.health.read().expect("health mutex").clone()
        }

        async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }

        async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "sessions",
            })
        }

        async fn create_session(
            &self,
            _req: &CreateSessionRequest,
        ) -> Result<SessionCreation, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "sessions",
            })
        }

        async fn resume_session(&self, _id: &str) -> Result<SessionInfo, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "resume",
            })
        }

        async fn delete_session(&self, _id: &str) -> Result<(), EngineError> {
            Ok(())
        }

        async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "send",
            })
        }

        async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
            Ok(())
        }
    }

    fn registry() -> (Arc<EngineRegistry>, EventBus) {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        (reg, bus)
    }

    /// A failed stop must publish ONE deterministic terminal health event so
    /// the UI can never hang in Stopping: Ready → engine.ready (usable),
    /// Failed → engine.failed (error state), Stopped → engine.stopped. The
    /// command still returns the stop error and Stop/Restart stays possible
    /// (TASK 24 §9).
    #[tokio::test]
    async fn failed_stop_publishes_terminal_health_never_endless_stopping() {
        for (id, mode, expected) in [
            (
                "stop-ready",
                StopFailureHealth::Ready,
                Event::EngineReady {
                    engine_id: "stop-ready".into(),
                },
            ),
            (
                "stop-failed",
                StopFailureHealth::Failed,
                Event::EngineFailed {
                    engine_id: "stop-failed".into(),
                    error: "adapter failed during stop".into(),
                },
            ),
            (
                "stop-stopped",
                StopFailureHealth::Stopped,
                Event::EngineStopped {
                    engine_id: "stop-stopped".into(),
                },
            ),
        ] {
            let (reg, bus) = registry();
            let mut sub = bus.subscribe();
            reg.register(Arc::new(StopFailingEngine::new(id, mode)));

            let err = reg.stop(id).await.expect_err("stop must fail");
            assert!(err.to_string().contains("stop"), "{err:?}");

            // engine.stopping → one terminal event. Collect until the
            // terminal arrives (bounded).
            let mut saw_stopping = false;
            let mut terminal = None;
            for _ in 0..8 {
                let env = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
                    .await
                    .expect("event timeout")
                    .expect("subscription alive");
                match env.event {
                    Event::EngineStopping { .. } => saw_stopping = true,
                    other if other == expected => {
                        terminal = Some(other);
                        break;
                    }
                    other => panic!("unexpected event for {id}: {other:?}"),
                }
            }
            assert!(saw_stopping, "{id}: engine.stopping must fire first");
            assert!(
                terminal.is_some(),
                "{id}: failed stop must publish a terminal health event, never endless Stopping"
            );

            // The command-level error keeps Stop/Restart possible: health is
            // whatever the adapter reports (no fabricated state), and a
            // retry stop returns the same typed error (not a stuck state).
            let _ = reg.stop(id).await;
        }
    }

    /// Fake adapter with an externally-settable health, so tests can drive the
    /// engine through arbitrary runtime transitions (Ready → Failed) without a
    /// stop. The runtime-death `report_failure` sink is the real contract under
    /// test; the adapter only mirrors the failure into its own health, exactly
    /// like a real engine that observes its process die.
    struct HealthEngine {
        id: String,
        health: std::sync::RwLock<EngineHealth>,
    }

    impl HealthEngine {
        fn new(id: &str) -> Self {
            Self {
                id: id.into(),
                health: std::sync::RwLock::new(EngineHealth::Ready),
            }
        }
        fn set_health(&self, h: EngineHealth) {
            *self.health.write().expect("health mutex") = h;
        }
    }

    #[async_trait]
    impl EngineAdapter for HealthEngine {
        fn identity(&self) -> EngineIdentity {
            EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }
        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities::default()
        }
        async fn start(&self, _ctx: &EngineStartContext) -> Result<(), EngineError> {
            // Mirror the real adapter lifecycle: a successful start owns its
            // health truth again (Ready), exactly like an engine whose process
            // came back up — otherwise restart stays masked as Failed forever.
            self.set_health(EngineHealth::Ready);
            Ok(())
        }
        async fn stop(&self) -> Result<(), EngineError> {
            Ok(())
        }
        async fn kill(&self) -> Result<(), EngineError> {
            Ok(())
        }
        fn health(&self) -> EngineHealth {
            self.health.read().expect("health mutex").clone()
        }
        async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }
        async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "sessions",
            })
        }
        async fn create_session(
            &self,
            _req: &CreateSessionRequest,
        ) -> Result<SessionCreation, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "sessions",
            })
        }
        async fn resume_session(&self, _id: &str) -> Result<SessionInfo, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "resume",
            })
        }
        async fn delete_session(&self, _id: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "send",
            })
        }
        async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
            Ok(())
        }
    }

    /// CORE-002: a failed/stopped engine must expose NO stale workspace binding.
    /// The binding must be masked on read (health-aware `list_info`/
    /// `bound_workspace`) AND cleared by the runtime-death `report_failure`
    /// sink, so `forget_workspace` is never blocked by a corpse and a later
    /// restart for another workspace owns a clean binding.
    #[tokio::test]
    async fn failed_engine_exposes_no_stale_binding_and_allows_rebind() {
        let (reg, _bus) = registry();
        let engine = Arc::new(HealthEngine::new("bind-eng"));
        reg.register(engine.clone());

        // Start bound to workspace A (runtime alive, Ready).
        let ctx_a = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("bind-eng", &ctx_a).await.unwrap();
        assert_eq!(
            reg.bound_workspace("bind-eng"),
            Some(Some("ws-A".to_string())),
            "live engine must report its A binding"
        );
        assert!(
            reg.list_info()
                .iter()
                .find(|e| e.identity.id == "bind-eng")
                .map(|e| e.bound_workspace_id.as_deref())
                == Some(Some("ws-A")),
            "list_info must expose the A binding for a live engine"
        );

        // Runtime dies mid-run: the adapter flips to Failed and reports it.
        engine.set_health(EngineHealth::Failed {
            message: "mid-run crash".into(),
        });
        // The actual runtime-death sink (the same closure the engine holds):
        (ctx_a.report_failure)("bind-eng", "mid-run crash");

        // The dead engine must expose NO binding — neither at the map level
        // (cleared by the sink) nor at the read level (masked by health).
        assert_eq!(
            reg.bound_workspace("bind-eng"),
            Some(None),
            "a failed engine must not expose a stale A binding"
        );
        assert!(
            reg.list_info()
                .iter()
                .find(|e| e.identity.id == "bind-eng")
                .map(|e| e.bound_workspace_id.clone())
                == Some(None),
            "list_info must mask the binding of a failed engine"
        );

        // Restart the SAME runtime for workspace B: the stale failure must not
        // have poisoned B's binding.
        let ctx_b = reg.start_context(Some("ws-B".to_string()), None);
        reg.start("bind-eng", &ctx_b).await.unwrap();
        assert_eq!(
            reg.bound_workspace("bind-eng"),
            Some(Some("ws-B".to_string())),
            "restart for B must own a clean B binding, untainted by the earlier failure"
        );
    }
}
