//! OpenCode engine adapter (TASK 10 process layer + TASK 11 session slice).
//!
//! TASK 10: discover the executable, prove it is OpenCode, start the managed
//! `opencode serve` runtime in a validated workspace, bind loopback, verify
//! readiness through the real API, own the process through the
//! ProcessSupervisor, and stop/kill cleanly.
//!
//! TASK 11: the first production vertical slice on top of that runtime —
//! provider/model discovery, session create/list/resume, prompt send,
//! structured event stream (SSE), normalized `message.*`/`tool.*`/
//! `permission.*` events, real cancellation, and run lifecycle with exactly
//! one terminal outcome. OpenCode remains the authority for session content
//! (§3); the adapter owns the protocol; the EventBus carries normalized
//! facts; nothing engine-specific leaks into generic code (law 3).
//!
//! Process lifecycle (PROCESS_LIFECYCLE.md) is the supervisor's; engine
//! lifecycle (`Starting → Ready → Stopping → Stopped | Failed`) lives here
//! and is published by the registry.

mod client;
mod discovery;
mod endpoint;
mod errors;
mod events;
mod launch;
mod models;
mod probe;
mod readiness;
mod runs;
mod secret;
mod sse;

pub use discovery::{discover, DiscoveredExecutable, LauncherKind};
pub use endpoint::{alloc_free_port, Endpoint, LOOPBACK_HOST};
pub use errors::OpenCodeError;
pub use models::ModelRef;
pub use secret::Secret;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use saiwork_core::engine::{
    EngineAdapter, EngineCapabilities, EngineError, EngineHealth, EngineIdentity,
    EngineStartContext, ModelInfo, QuestionResolution, SendAcceptance, SessionCreation,
    SessionInfo, SessionMessage,
};
use saiwork_events::{EventBus, ProcessId, RunId};
use saiwork_process::ProcessSupervisor;
use tokio::sync::watch;
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::client::ApiClient;
use crate::discovery::discover as discover_impl;
use crate::events::{
    emit_terminal, EventRouter, PendingPermissions, PendingQuestions, TerminalOutcome,
    STREAM_IDLE_GRACE,
};
use crate::models::{AuthProviders, ProviderList};
use crate::probe::ProbeResult;
use crate::readiness::ReadinessConfig;
use crate::runs::{RunRecord, RunRegistry};
use crate::sse::SseParser;

/// Engine id registered in the registry ("opencode").
pub const ENGINE_ID: &str = "opencode";

#[derive(Clone, Debug)]
pub struct OpenCodeConfig {
    /// Explicit executable path (highest precedence, never silently
    /// overridden — §6).
    pub explicit_executable: Option<PathBuf>,
    /// Outer startup deadline: readiness must succeed within this budget
    /// (§30, §89).
    pub startup_timeout: Duration,
    /// Per-request HTTP timeout for readiness probes (§70).
    pub request_timeout: Duration,
    /// Bounded readiness response body (§72) — also the ordinary metadata
    /// response bound (sessions, messages, error bodies).
    pub max_response_bytes: usize,
    /// Dedicated bound for the provider catalog responses (`/provider` and
    /// the `/config/providers` fallback). Deliberately LARGER than
    /// `max_response_bytes`: the real 1.18.18 catalog is ~5 MiB (191
    /// providers / 6615 models measured 2026-08-18) while the ordinary
    /// metadata bound is 4 MiB. Still bounded — never an unbounded read.
    pub provider_catalog_max_bytes: usize,
    /// OpenCode credential file (`auth.json`) to merge into the model list:
    /// providers the server catalog does not expose (custom / `type: api`)
    /// still contribute their declared models. `None` = the standard
    /// per-user path (XDG/HOME, `.local/share/opencode/auth.json`); a
    /// missing file is a silent no-op. ONLY ids and model lists are read —
    /// the API keys inside are never deserialized (models.rs).
    pub auth_json_path: Option<PathBuf>,
    /// Max spawn attempts when the failure is classified as a port collision
    /// (§17, §90–§91).
    pub retry_port_attempts: u32,
    /// Timeout for short metadata requests (providers, sessions, abort,
    /// permission replies) — TASK 11 §9. The message POST has no overall
    /// timeout (its lifetime is the run lifetime).
    pub metadata_timeout: Duration,
    /// Local guard against pathological prompt sizes before IPC/request
    /// serialization (TASK 11 §68).
    pub max_prompt_bytes: usize,
}

impl Default for OpenCodeConfig {
    fn default() -> Self {
        Self {
            explicit_executable: None,
            startup_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(3),
            max_response_bytes: 4 * 1024 * 1024,
            // 16 MiB: ~3.2x the measured 5.04 MiB real catalog — headroom
            // for catalog growth, still a hard bound.
            provider_catalog_max_bytes: 16 * 1024 * 1024,
            // Standard per-user auth.json (auto-detected; absent = no-op).
            auth_json_path: None,
            retry_port_attempts: 3,
            metadata_timeout: Duration::from_secs(15),
            max_prompt_bytes: 1024 * 1024,
        }
    }
}

/// Engine lifecycle phase of the adapter (separate from the supervisor's OS
/// process state, PROCESS_LIFECYCLE.md §26).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Idle = 0,
    Starting = 1,
    Ready = 2,
    Stopping = 3,
    Failed = 4,
    Stopped = 5,
}

impl Phase {
    fn code(self) -> u8 {
        self as u8
    }
    fn from_code(code: u8) -> Self {
        match code {
            0 => Phase::Idle,
            1 => Phase::Starting,
            2 => Phase::Ready,
            3 => Phase::Stopping,
            4 => Phase::Failed,
            _ => Phase::Stopped,
        }
    }
}

impl From<Phase> for EngineHealth {
    fn from(phase: Phase) -> Self {
        match phase {
            Phase::Idle => EngineHealth::Unknown,
            Phase::Starting => EngineHealth::Starting,
            Phase::Ready => EngineHealth::Ready,
            Phase::Stopping => EngineHealth::Stopped,
            Phase::Failed => EngineHealth::Failed {
                message: "opencode runtime failed".into(),
            },
            Phase::Stopped => EngineHealth::Stopped,
        }
    }
}

/// One live OpenCode runtime. Created per start attempt; every attempt gets
/// fresh identity (ProcessId), fresh endpoint/port, a fresh secret, and a
/// fresh API client (§43, §86, §91).
struct Runtime {
    generation: u64,
    process: Arc<saiwork_process::ManagedProcess>,
    endpoint: Mutex<Endpoint>,
    secret: Secret,
    workspace: PathBuf,
    supervisor: Arc<ProcessSupervisor>,
    /// TASK 11: typed API client bound to this runtime's endpoint + secret
    /// (§7–§8, §112: never stale after restart).
    client: ApiClient,
    bus: EventBus,
    /// True once stop/kill/dispose is requested, so the exit watcher treats
    /// the exit as expected (§41).
    stop_requested: Arc<AtomicBool>,
    exit_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
    started_at: Instant,
}

/// The one global event-stream task per runtime (TASK 11 §172, §174): all
/// runs of a runtime share it; events are routed by `sessionID`. It opens
/// lazily on the first send and closes when the last run ends or the runtime
/// stops (§171: no idle SSE connection).
struct StreamHandle {
    generation: u64,
    ready: watch::Sender<bool>,
    close: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

/// Generation-scoped model cache (TASK 24 perf): the sorted UI `Vec` plus an
/// O(1) namespaced-id → exact wire pair lookup, built ONCE per provider
/// fetch. A per-send `resolve_model_ref` never clones/sorts/dedups the
/// model vector again (PERF-001: the cache itself is never cloned — lookups
/// clone only the single matched `ModelRef`).
#[derive(Debug)]
struct ModelCache {
    generation: u64,
    models: Vec<ModelInfo>,
    /// Generic namespaced id (`<provider-id>/<raw-model-key>`) → the exact
    /// wire pair. One entry per exact `(provider_id, raw_model_key)` pair —
    /// no ambiguous-by-model-name lookup (§11): two providers may expose the
    /// same raw key and never collide.
    by_id: HashMap<String, ModelRef>,
}

/// Build the generation-scoped cache from a provider list.
///
/// Model identity rule (provider-bound policy §10–§11): the generic
/// `ModelInfo.id` is `<provider-id>/<raw-model-key>` (globally unambiguous),
/// while the OpenCode wire identity stays `{providerID, modelID}` with
/// `modelID` = the RAW map key verbatim — never a synthesized prefix beyond
/// what the server itself reports. The inner `model.id`/`model.providerID`
/// fields are legacy/redundant and never trusted as authority.
fn build_model_cache(generation: u64, providers: &ProviderList) -> ModelCache {
    let mut models = Vec::new();
    let mut by_id: HashMap<String, ModelRef> = HashMap::new();
    for provider in &providers.all {
        for (key, model) in &provider.models {
            let namespaced = format!("{}/{}", provider.id, key);
            by_id.insert(
                namespaced.clone(),
                ModelRef {
                    providerID: provider.id.clone(),
                    modelID: key.clone(),
                },
            );
            models.push(ModelInfo {
                id: namespaced,
                display_name: if model.name.is_empty() {
                    key.clone()
                } else {
                    model.name.clone()
                },
                provider: Some(provider.id.clone()),
                provider_name: (!provider.name.is_empty()).then(|| provider.name.clone()),
            });
        }
    }
    models.sort_by(|a, b| a.id.cmp(&b.id));
    ModelCache {
        generation,
        models,
        by_id,
    }
}

/// The standard per-user OpenCode credential file location (verified on
/// this machine: `C:\Users\<user>\.local\share\opencode\auth.json`).
/// Resolution mirrors OpenCode: XDG_DATA_HOME → HOME/.local/share →
/// USERPROFILE/.local/share.
fn standard_auth_json_path() -> Option<std::path::PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".local/share"))
        })
        .or_else(|| {
            std::env::var_os("USERPROFILE")
                .map(|h| std::path::PathBuf::from(h).join(".local/share"))
        })?;
    Some(base.join("opencode/auth.json"))
}

/// Merge providers from OpenCode's local `auth.json` into the server
/// catalog (TASK 25 §30): a custom/`type: api` provider that the server's
/// `/provider` does not expose still contributes its declared models.
/// Policy:
/// - the server catalog is ALWAYS the authority — an auth provider id that
///   already exists in `all` is never replaced or duplicated;
/// - a credential-only entry (no `models`) is dropped — a provider with
///   zero models would be an unusable empty shell (e.g. a broken custom
///   provider);
/// - a missing or malformed file is a silent no-op (a credential file must
///   never break model discovery);
/// - only ids and model lists are read — API keys stay on disk.
/// Returns the ids of the providers it ADDED (callers use them to keep
/// auth-backed providers across the connected-only filter — a provider
/// with credentials on disk is connected by definition, even when the
/// server's `/provider` catalog does not list it).
fn augment_auth_providers(list: &mut ProviderList, auth_path: Option<&std::path::Path>) -> Vec<String> {
    let mut added = Vec::new();
    let path = match auth_path {
        Some(p) => p.to_path_buf(),
        None => match standard_auth_json_path() {
            Some(p) => p,
            None => return added,
        },
    };
    let Ok(bytes) = std::fs::read(&path) else {
        return added;
    };
    let auth = AuthProviders::parse(&bytes);
    for provider in auth.providers {
        if provider.models.is_empty() {
            continue;
        }
        if list.all.iter().any(|p| p.id == provider.id) {
            continue;
        }
        added.push(provider.id.clone());
        list.all.push(provider);
    }
    added
}

/// Keep only credential-connected providers in the model catalog (TASK 27):
/// the server's `connected` list is exactly the set of providers the user
/// has usable credentials for — the other ~96% of a default catalog is
/// paywalled noise ("models I did not connect are useless"). Auth.json
/// providers are connected by definition.
///
/// `connected` PRESENCE (not emptiness) is the authority signal (CORE-006):
/// - `None` => the wire had NO connected authority (strict `/config/providers`
///   fallback) — keep the full degraded catalog (never silently show nothing).
/// - `Some(set)` => AUTHORITATIVE: keep only providers named in `set` plus
///   separately proven auth-backed ids. An authoritative `Some([])` therefore
///   drops every ordinary server provider, exactly as the server intends.
/// Per-provider `connected: Option<bool>` is NOT consulted here — the server's
/// `connected` LIST is the single authoritative set.
fn apply_connected_filter(list: &mut ProviderList, auth_ids: &[String]) {
    let Some(connected) = list.connected.as_ref() else {
        // No connected authority on the wire: degraded full catalog.
        return;
    };
    list.all.retain(|p| {
        connected.contains(&p.id) || auth_ids.iter().any(|id| id == &p.id)
    });
}

/// The OpenCode engine adapter. Safe to register in the registry; one
/// instance owns at most one runtime (multi-workspace = multiple instances,
/// §49, §95).
pub struct OpenCodeAdapter {
    config: OpenCodeConfig,
    phase: Arc<AtomicU8>,
    stop_requested: AtomicBool,
    identity: RwLock<EngineIdentity>,
    runtime: Mutex<Option<Arc<Runtime>>>,
    discovered: Mutex<Option<DiscoveredExecutable>>,
    probe_cache: Mutex<Option<ProbeResult>>,
    next_generation: AtomicU64,
    /// TASK 11: active-run registry (one owner — the adapter, §74).
    runs: Arc<RunRegistry>,
    /// Bounded pending-permission authority (W2-002): the exact session/run/
    /// request ownership a missed `permission.requested` event can be
    /// reconstructed from after a bounded-bus lag. Inserted by the EventRouter
    /// before the canonical event is published; read by `pending_permissions()`
    /// and cleared on resolution / run terminal / runtime teardown.
    pending: Arc<PendingPermissions>,
    /// AUDIT-CORE-002: bounded pending-question authority — same lifecycle
    /// as the permission store above (`pending_questions()`, reply/reject,
    /// run terminal, runtime teardown).
    pending_questions: Arc<PendingQuestions>,
    /// The global event stream for the current runtime, if any (§172). The
    /// stream lives for the whole READY runtime: it is closed only on
    /// stop/failure or generation replacement, never per-run idle — keeping
    /// it connected removes reconnect churn between normal human prompts
    /// (TASK 24 perf).
    stream: Mutex<Option<StreamHandle>>,
    /// Per-runtime models cache keyed by generation (§14, §118).
    models_cache: Mutex<Option<ModelCache>>,
    /// PERF-023 / PERF-006: async refresh gate for single-flight provider catalog
    /// fetches. Concurrent cold consumers share one fetch instead of each
    /// independently downloading the multi-megabyte catalog.
    refresh_gate: Arc<tokio::sync::Mutex<Option<Arc<tokio::sync::OnceCell<()>>>>>,
}

impl OpenCodeAdapter {
    pub fn new(config: OpenCodeConfig) -> Self {
        Self {
            config,
            phase: Arc::new(AtomicU8::new(Phase::Idle.code())),
            stop_requested: AtomicBool::new(false),
            identity: RwLock::new(EngineIdentity {
                id: ENGINE_ID.into(),
                display_name: "OpenCode".into(),
                version: "unknown".into(),
                experimental: false,
            }),
            runtime: Mutex::new(None),
            discovered: Mutex::new(None),
            probe_cache: Mutex::new(None),
            next_generation: AtomicU64::new(0),
            runs: Arc::new(RunRegistry::new()),
            pending: PendingPermissions::new(),
            pending_questions: PendingQuestions::new(),
            stream: Mutex::new(None),
            models_cache: Mutex::new(None),
            refresh_gate: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Fetch + normalize providers/models, cached per runtime generation
    /// (§14, §118). `refresh` forces a reload. The sorted UI `Vec` is
    /// returned; the O(1) id→provider lookup rides along in the same cache
    /// so per-send resolution never clones/sorts/dedups the model vector
    /// again (TASK 24 perf).
    async fn models(
        &self,
        client: &ApiClient,
        generation: u64,
        refresh: bool,
    ) -> Result<Vec<ModelInfo>, OpenCodeError> {
        // Fast path: check the cache (no await, short mutex hold).
        if !refresh {
            let cache = self
                .models_cache
                .lock()
                .expect("models cache mutex poisoned");
            if let Some(c) = cache.as_ref() {
                if c.generation == generation {
                    return Ok(c.models.clone());
                }
            }
        }
        // PERF-023 / PERF-006: slow path — single-flight provider catalog fetch.
        // Concurrent cold consumers share one fetch instead of each
        // independently downloading the multi-megabyte catalog.
        let cell = {
            let mut gate = self.refresh_gate.lock().await;
            match gate.as_ref() {
                Some(existing) => existing.clone(),
                None => {
                    let cell = Arc::new(tokio::sync::OnceCell::new());
                    *gate = Some(cell.clone());
                    cell
                }
            }
        };
        cell.get_or_try_init(|| async {
            let providers = client.list_providers().await?;
            let mut providers = providers;
            let auth_ids =
                augment_auth_providers(&mut providers, self.config.auth_json_path.as_deref());
            apply_connected_filter(&mut providers, &auth_ids);
            let cache = build_model_cache(generation, &providers);
            *self
                .models_cache
                .lock()
                .expect("models cache mutex poisoned") = Some(cache);
            Ok::<_, OpenCodeError>(())
        })
        .await?;
        // Clear the gate so the next generation can start fresh.
        {
            let mut gate = self.refresh_gate.lock().await;
            *gate = None;
        }
        let guard = self
            .models_cache
            .lock()
            .expect("models cache mutex poisoned");
        if let Some(c) = guard.as_ref() {
            if c.generation == generation {
                return Ok(c.models.clone());
            }
        }
        Err(OpenCodeError::RequestFailed {
            detail: "model cache missing immediately after fetch".into(),
        })
    }

    /// Map a generic namespaced model id (`<provider-id>/<raw-model-key>`)
    /// to the exact OpenCode `ModelRef` (providerID + raw modelID, verified
    /// required by the message API). No silent fallback (§143–§144): an
    /// unknown id is an error, never "whatever worked". O(1) via the
    /// generation-scoped lookup — no per-send clone/sort/dedup of the model
    /// vector (TASK 24 perf). Never called for `None` (Engine Default).
    ///
    /// PERF-001: the cache is NEVER cloned. The std mutex is held only
    /// briefly to inspect `as_ref()` and clone the single matched
    /// `ModelRef`; it is released before any await (no
    /// `clippy::await_holding_lock`). On a miss/stale generation the catalog
    /// is refreshed once, the guard is reacquired, and again only the result
    /// entry is cloned.
    async fn resolve_model_ref(
        &self,
        client: &ApiClient,
        generation: u64,
        model_id: &str,
    ) -> Result<ModelRef, OpenCodeError> {
        let cached: Option<ModelRef> = {
            let cache = self
                .models_cache
                .lock()
                .expect("models cache mutex poisoned");
            match cache.as_ref() {
                Some(c) if c.generation == generation => c.by_id.get(model_id).cloned(),
                _ => None,
            }
        };
        if let Some(found) = cached {
            return Ok(found);
        }
        // Miss or stale generation: refresh exactly once.
        self.models(client, generation, true).await?;
        let found = {
            let cache = self
                .models_cache
                .lock()
                .expect("models cache mutex poisoned");
            match cache.as_ref() {
                Some(c) if c.generation == generation => c.by_id.get(model_id).cloned(),
                _ => None,
            }
        };
        found.ok_or_else(|| OpenCodeError::ModelUnavailable {
            model_id: model_id.to_string(),
            detail: "not among the (provider, model) pairs OpenCode reports".into(),
        })
    }

    /// Diagnostic-only probe of an installation (no server launch, §46–§47).
    pub async fn probe_installation(
        supervisor: &Arc<ProcessSupervisor>,
        config: &OpenCodeConfig,
    ) -> Result<ProbeResult, OpenCodeError> {
        let discovered = discover_impl(config)?;
        probe::probe(supervisor, &discovered, config.startup_timeout).await
    }

    /// The current endpoint (if a runtime exists), for diagnostics.
    pub fn endpoint(&self) -> Option<Endpoint> {
        let guard = self.runtime.lock().expect("runtime mutex poisoned");
        let runtime = guard.as_ref()?;
        let endpoint = *runtime.endpoint.lock().expect("endpoint mutex poisoned");
        Some(endpoint)
    }

    /// Process id of the live runtime, for diagnostics.
    pub fn process_id(&self) -> Option<ProcessId> {
        let guard = self.runtime.lock().expect("runtime mutex poisoned");
        let runtime = guard.as_ref()?;
        Some(runtime.process.id().clone())
    }

    /// On-demand health check (§109): authenticated readiness probe against
    /// the live endpoint. `false` when there is no runtime or the server no
    /// longer answers as OpenCode. Never polled automatically.
    pub async fn check_ready(&self) -> bool {
        // Guard scoped and dropped before any await (clippy::await_holding_lock).
        let (endpoint, secret) = {
            let guard = self.runtime.lock().expect("runtime mutex poisoned");
            let Some(runtime) = guard.as_ref() else {
                return false;
            };
            let endpoint = *runtime.endpoint.lock().expect("endpoint mutex poisoned");
            let secret = runtime.secret.clone();
            (endpoint, secret)
        };
        let cfg = ReadinessConfig {
            startup_timeout: self.config.startup_timeout,
            request_timeout: self.config.request_timeout,
            max_response_bytes: self.config.max_response_bytes,
        };
        let client = match reqwest::Client::builder()
            .timeout(self.config.request_timeout)
            .connect_timeout(self.config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
        {
            Ok(c) => c,
            Err(_) => return false,
        };
        matches!(
            readiness::probe_once(&client, &endpoint, &secret, &cfg).await,
            Ok(readiness::ProbeOutcome::Ready)
        )
    }

    fn phase(&self) -> Phase {
        Phase::from_code(self.phase.load(Ordering::SeqCst))
    }

    fn set_phase(&self, to: Phase) {
        self.phase.store(to.code(), Ordering::SeqCst);
    }

    /// CAS on the phase; returns false when the current phase differs.
    fn cas_phase(&self, from: Phase, to: Phase) -> bool {
        self.phase
            .compare_exchange(from.code(), to.code(), Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn ensure_discovered(&self) -> Result<DiscoveredExecutable, OpenCodeError> {
        let mut slot = self.discovered.lock().expect("discovery mutex poisoned");
        if slot.is_none() {
            *slot = Some(discover_impl(&self.config)?);
        }
        Ok(slot.clone().expect("just set"))
    }

    async fn ensure_probed(
        &self,
        supervisor: &Arc<ProcessSupervisor>,
    ) -> Result<ProbeResult, OpenCodeError> {
        // Fast path: already probed this adapter lifetime. Never hold the
        // probe-cache lock across the await (§45 lock audit).
        {
            let slot = self.probe_cache.lock().expect("probe mutex poisoned");
            if let Some(result) = slot.as_ref() {
                return Ok(result.clone());
            }
        }
        let discovered = self.ensure_discovered()?;
        let result = probe::probe(supervisor, &discovered, self.config.startup_timeout).await?;
        let mut slot = self.probe_cache.lock().expect("probe mutex poisoned");
        *slot = Some(result.clone());
        Ok(result)
    }

    // -------------------------------------------------------------------
    // TASK 11 helpers
    // -------------------------------------------------------------------

    /// Clone the current runtime's API client + generation, or `NotReady`.
    fn client(&self) -> Result<(ApiClient, u64), OpenCodeError> {
        let guard = self.runtime.lock().expect("runtime mutex poisoned");
        let runtime = guard.as_ref().ok_or(OpenCodeError::NotReady {
            phase: "no runtime",
        })?;
        if self.phase() != Phase::Ready {
            return Err(OpenCodeError::NotReady {
                phase: "engine not ready",
            });
        }
        Ok((runtime.client.clone(), runtime.generation))
    }

    /// Open (or reuse) the runtime's global event stream (§172). Returns the
    /// ready/close handles the POST task needs.
    fn ensure_stream(
        &self,
        runtime: &Arc<Runtime>,
    ) -> (watch::Receiver<bool>, watch::Sender<bool>) {
        let mut slot = self.stream.lock().expect("stream mutex poisoned");
        if let Some(handle) = slot.as_ref() {
            if handle.generation == runtime.generation && !handle.task.is_finished() {
                return (handle.ready.subscribe(), handle.close.clone());
            }
        }
        if let Some(old) = slot.take() {
            let _ = old.close.send(true);
            old.task.abort();
        }
        let (ready_tx, _) = watch::channel(false);
        let (close_tx, close_rx) = watch::channel(false);
        let endpoint = *runtime.endpoint.lock().expect("endpoint mutex poisoned");
        let secret = runtime.secret.clone();
        let generation = runtime.generation;
        let bus = runtime.bus.clone();
        let registry = self.runs.clone();
        let pending = self.pending.clone();
        let pending_questions = self.pending_questions.clone();
        let connect_timeout = self.config.request_timeout;
        let task = tokio::spawn(stream_task(
            endpoint,
            secret,
            bus,
            registry,
            pending,
            pending_questions,
            connect_timeout,
            ready_tx.clone(),
            close_rx,
        ));
        *slot = Some(StreamHandle {
            generation,
            ready: ready_tx.clone(),
            close: close_tx.clone(),
            task,
        });
        (ready_tx.subscribe(), close_tx)
    }

    /// True when `generation` is still the live runtime's generation. Used to
    /// discard metadata/session responses that arrived after a restart, so a
    /// stale runtime can never become current authority (§32).
    fn generation_matches(&self, generation: u64) -> bool {
        self.runtime
            .lock()
            .expect("runtime mutex poisoned")
            .as_ref()
            .is_some_and(|r| r.generation == generation)
    }

    /// Close the runtime's event stream (stop / dispose / generation
    /// replacement). Idempotent. There is intentionally NO idle close: the
    /// stream stays connected for the READY runtime and is reused by every
    /// send, so normal human prompts never pay a reconnect (TASK 24 perf).
    fn close_stream(&self) {
        if let Some(handle) = self.stream.lock().expect("stream mutex poisoned").take() {
            let _ = handle.close.send(true);
            handle.task.abort();
        }
    }

    // -------------------------------------------------------------------
    // TASK 10 internals
    // -------------------------------------------------------------------

    /// The core start sequence (without the phase gate). See `start`.
    async fn start_inner(&self, ctx: &EngineStartContext) -> Result<(), EngineError> {
        // §19–§20: explicit validated workspace. The server's cwd is the
        // workspace; SAIWORK2 never mutates the process CWD (§105).
        let workspace = ctx.workspace_path.clone().ok_or_else(|| {
            OpenCodeError::InvalidWorkspace {
                path: PathBuf::from("<none>"),
            }
            .into_engine()
        })?;
        if !workspace.is_dir() {
            return Err(OpenCodeError::InvalidWorkspace {
                path: workspace.clone(),
            }
            .into_engine());
        }

        let discovered = self.ensure_discovered()?;
        let probe = self.ensure_probed(&ctx.supervisor).await?;
        {
            let mut identity = self.identity.write().expect("identity mutex poisoned");
            identity.version = probe.version.clone();
        }

        // Port-collision retry: bounded, fresh ProcessId + fresh port each
        // attempt, previous attempt fully cleaned (§17, §90–§91).
        let max = self.config.retry_port_attempts.max(1);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self
                .start_attempt(ctx, &discovered, workspace.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) if e.is_port_retryable() => {
                    if attempt < max {
                        warn!(
                            attempt,
                            max, "opencode port unavailable; retrying with a fresh port"
                        );
                        continue;
                    }
                    return Err(OpenCodeError::PortUnavailable { attempts: attempt }.into_engine());
                }
                Err(e) => return Err(e.into_engine()),
            }
        }
    }

    async fn start_attempt(
        &self,
        ctx: &EngineStartContext,
        discovered: &DiscoveredExecutable,
        workspace: PathBuf,
    ) -> Result<(), OpenCodeError> {
        // §34: a stop/shutdown that raced ahead of this attempt.
        if self.stop_requested.load(Ordering::SeqCst) {
            return Err(OpenCodeError::Cancelled);
        }

        let port = endpoint::alloc_free_port().map_err(|e| OpenCodeError::SpawnFailed {
            detail: format!("cannot allocate loopback port: {e}"),
        })?;
        let secret = Secret::generate();
        let process_id = ProcessId::new(format!("opencode-{}", Uuid::new_v4()));
        let spec = launch::server_spec(discovered, &workspace, port, &secret, process_id.clone());
        let process = ctx
            .supervisor
            .spawn(spec)
            .await
            .map_err(|e| OpenCodeError::SpawnFailed {
                detail: format!("{e}"),
            })?;

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let client = ApiClient::new(
            Endpoint::http(endpoint::LOOPBACK_HOST, port),
            secret.clone(),
            self.config.request_timeout,
            self.config.metadata_timeout,
            self.config.max_response_bytes,
            self.config.provider_catalog_max_bytes,
        )?;
        let runtime = Arc::new(Runtime {
            generation,
            process: process.clone(),
            endpoint: Mutex::new(Endpoint::http(endpoint::LOOPBACK_HOST, port)),
            secret,
            workspace,
            supervisor: ctx.supervisor.clone(),
            client,
            bus: ctx.bus.clone(),
            stop_requested: Arc::new(AtomicBool::new(false)),
            exit_task: Mutex::new(None),
            cancel_tx,
            cancel_rx,
            started_at: Instant::now(),
        });

        // Register before readiness: a concurrent stop() must be able to
        // find the runtime and cancel the loop (§34/§76). The stop-flag
        // check happens under the same lock so there is no window where
        // stop() observes "nothing running" while a runtime is about to
        // appear.
        {
            let mut guard = self.runtime.lock().expect("runtime mutex poisoned");
            if self.stop_requested.load(Ordering::SeqCst) {
                drop(guard);
                let _ = ctx.supervisor.stop(&process, true).await;
                return Err(OpenCodeError::Cancelled);
            }
            *guard = Some(runtime.clone());
        }

        self.spawn_exit_watcher(&runtime, ctx);

        let cfg = ReadinessConfig {
            startup_timeout: self.config.startup_timeout,
            request_timeout: self.config.request_timeout,
            max_response_bytes: self.config.max_response_bytes,
        };
        let current = *runtime.endpoint.lock().expect("endpoint mutex poisoned");
        let confirmed = match readiness::wait_ready(
            &current,
            &runtime.secret,
            &process,
            &cfg,
            runtime.cancel_rx.clone(),
        )
        .await
        {
            Ok(ep) => ep,
            Err(startup) => {
                return match self.cleanup_attempt(&runtime).await {
                    Ok(()) => Err(startup),
                    Err(cleanup) => Err(OpenCodeError::StartupCleanupFailed {
                        startup: startup.to_string(),
                        cleanup: cleanup.to_string(),
                    }),
                };
            }
        };
        *runtime.endpoint.lock().expect("endpoint mutex poisoned") = confirmed;

        // Publish-ready gate: if stop() won the race meanwhile, the runtime
        // was already taken and torn down — never a late engine.ready (§34).
        if self.stop_requested.load(Ordering::SeqCst)
            || !self.cas_phase(Phase::Starting, Phase::Ready)
        {
            return Err(OpenCodeError::Cancelled);
        }

        let ms = runtime.started_at.elapsed().as_millis();
        info!(
            engine = ENGINE_ID,
            generation = runtime.generation,
            port = confirmed.port,
            pid = process.pid(),
            workspace = %runtime.workspace.display(),
            startup_ms = ms,
            "opencode ready"
        );
        Ok(())
    }

    /// Teardown after a failed start attempt. Runtime authority is released
    /// only after the supervisor proves exit; a failed stop leaves both the
    /// runtime and its watcher reachable for an explicit retry.
    async fn cleanup_attempt(&self, runtime: &Arc<Runtime>) -> Result<(), OpenCodeError> {
        runtime.stop_requested.store(true, Ordering::SeqCst);
        let _ = runtime.cancel_tx.send(true);
        let res = runtime.supervisor.stop(&runtime.process, true).await;
        match res {
            Ok(_) | Err(saiwork_process::ProcessError::NotRunning { .. }) => {
                let task = runtime
                    .exit_task
                    .lock()
                    .expect("exit task mutex poisoned")
                    .take();
                if let Some(mut task) = task {
                    if tokio::time::timeout(Duration::from_secs(3), &mut task)
                        .await
                        .is_err()
                    {
                        task.abort();
                        let _ = task.await;
                    }
                }
                let mut guard = self.runtime.lock().expect("runtime mutex poisoned");
                if guard
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, runtime))
                {
                    guard.take();
                }
                Ok(())
            }
            Err(e) => Err(OpenCodeError::RequestFailed {
                detail: format!("cleanup failed: {e}"),
            }),
        }
    }

    /// Watch the managed process. On an exit that was neither requested nor
    /// part of a failed start (i.e. the engine was READY), transition to
    /// FAILED and publish `engine.failed` (§40–§41). The registry publishes
    /// start failures itself, so a crash during STARTING is not double-
    /// reported.
    fn spawn_exit_watcher(&self, runtime: &Arc<Runtime>, ctx: &EngineStartContext) {
        let phase = self.phase.clone();
        let process = runtime.process.clone();
        let stop_requested = runtime.stop_requested.clone();
        let report_failure = ctx.report_failure.clone();
        let engine_id = ENGINE_ID.to_string();
        // TASK 11 §80: an unexpected process exit fails every active run of
        // this runtime generation. The registry + bus are owned here, so the
        // watcher needs no `&self` across await.
        let registry = self.runs.clone();
        let generation = runtime.generation;
        let bus = runtime.bus.clone();
        let pending = self.pending.clone();
        let pending_questions = self.pending_questions.clone();
        let task = tokio::spawn(async move {
            let mut rx = process.exit();
            loop {
                if let Some(info) = *rx.borrow() {
                    if !stop_requested.load(Ordering::SeqCst)
                        && phase.load(Ordering::SeqCst) == Phase::Ready.code()
                    {
                        let message = match info.code {
                            Some(code) => {
                                format!("opencode process exited unexpectedly (code {code})")
                            }
                            None => "opencode process exited unexpectedly".into(),
                        };
                        warn!(engine = %engine_id, "{message}");
                        phase.store(Phase::Failed.code(), Ordering::SeqCst);
                        report_failure(&engine_id, &message);
                        // Fail active runs: no eternal RUNNING (§80). Mark
                        // engine-loss BEFORE the terminal so a racing POST
                        // body-read defers to this authoritative terminal
                        // (TASK 24 §9).
                        for record in registry.take_all(generation, &message) {
                            record.mark_engine_lost();
                            emit_terminal(
                                &bus,
                                &record,
                                TerminalOutcome::Failed(
                                    "engine process exited: run interrupted".into(),
                                ),
                            );
                            // W2-002: the run is over — drop its pending
                            // permissions so a crashed runtime never leaves
                            // dangling open requests. AUDIT-CORE-002: same
                            // for pending questions.
                            pending.remove_for_run(record.run_id.as_str());
                            pending_questions.remove_for_run(record.run_id.as_str());
                            if let Some(task) = record
                                .post_task
                                .lock()
                                .expect("post task mutex poisoned")
                                .take()
                            {
                                task.abort();
                            }
                        }
                    }
                    break;
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
        *runtime.exit_task.lock().expect("exit task mutex poisoned") = Some(task);
    }

    /// Shared stop/kill implementation. `graceful=false` forces.
    async fn stop_impl(&self, graceful: bool) -> Result<(), EngineError> {
        // W2-002: the runtime is tearing down — drop every pending permission
        // so a fresh runtime never inherits stale open requests. Same for
        // pending questions (AUDIT-CORE-002).
        self.pending.clear();
        self.pending_questions.clear();
        let runtime = { self.runtime.lock().expect("runtime mutex poisoned").clone() };
        let Some(runtime) = runtime else {
            // Nothing running or starting (or stop already ran): idempotent
            // (§39).
            self.stop_requested.store(true, Ordering::SeqCst);
            return Ok(());
        };
        runtime.stop_requested.store(true, Ordering::SeqCst);
        self.stop_requested.store(true, Ordering::SeqCst);
        let _ = runtime.cancel_tx.send(true);
        self.close_stream();
        // Only a Ready runtime transitions through Stopping; a Starting
        // runtime is left for start() to fail via the cancel path.
        let was_ready = self.cas_phase(Phase::Ready, Phase::Stopping);
        if was_ready {
            info!(engine = ENGINE_ID, "opencode stopping");
        }

        let result = runtime.supervisor.stop(&runtime.process, graceful).await;

        let task = runtime
            .exit_task
            .lock()
            .expect("exit task mutex poisoned")
            .take();
        if let Some(task) = task {
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
        }

        let bus = runtime.bus.clone();
        match result {
            Ok(info) => {
                // Exit proven: release runtime ownership and fail active runs.
                {
                    let _ = self.runtime.lock().expect("runtime mutex poisoned").take();
                }
                for record in self.runs.take_all(runtime.generation, "engine stopping") {
                    record.mark_engine_lost();
                    emit_terminal(
                        &bus,
                        &record,
                        TerminalOutcome::Failed("engine stopping: run interrupted".into()),
                    );
                    if let Some(task) = record
                        .post_task
                        .lock()
                        .expect("post task mutex poisoned")
                        .take()
                    {
                        task.abort();
                    }
                }
                self.set_phase(Phase::Stopped);
                info!(
                    engine = ENGINE_ID,
                    pid = runtime.process.pid(),
                    code = ?info.code,
                    "opencode stopped"
                );
                Ok(())
            }
            Err(saiwork_process::ProcessError::NotRunning { .. }) => {
                // Already dead: release runtime ownership and fail active runs.
                {
                    let _ = self.runtime.lock().expect("runtime mutex poisoned").take();
                }
                for record in self.runs.take_all(runtime.generation, "engine stopping") {
                    record.mark_engine_lost();
                    emit_terminal(
                        &bus,
                        &record,
                        TerminalOutcome::Failed("engine stopping: run interrupted".into()),
                    );
                    if let Some(task) = record
                        .post_task
                        .lock()
                        .expect("post task mutex poisoned")
                        .take()
                    {
                        task.abort();
                    }
                }
                self.set_phase(Phase::Stopped);
                info!(
                    engine = ENGINE_ID,
                    pid = runtime.process.pid(),
                    "opencode was already stopped"
                );
                Ok(())
            }
            Err(e) => {
                warn!(engine = ENGINE_ID, error = %e, "opencode stop reported an error; process termination unproven");
                self.set_phase(Phase::Failed);
                // Exit unproven: do not emit definitive Failed. Transition active runs
                // to OutcomeUnknown so session reservations remain pinned and protect workspace.
                for record in self.runs.take_all(runtime.generation, "engine stop unproven") {
                    record.mark_engine_lost();
                    emit_terminal(
                        &bus,
                        &record,
                        TerminalOutcome::Unknown(format!("engine stop unproven: {e}")),
                    );
                    if let Some(task) = record
                        .post_task
                        .lock()
                        .expect("post task mutex poisoned")
                        .take()
                    {
                        task.abort();
                    }
                }
                Err(EngineError::engine(ENGINE_ID, format!("stop failed: {e}")))
            }
        }
    }
}

#[async_trait]
impl EngineAdapter for OpenCodeAdapter {
    fn identity(&self) -> EngineIdentity {
        self.identity
            .read()
            .expect("identity mutex poisoned")
            .clone()
    }

    /// Truthful capability set (TASK 11 §145). Everything implemented is
    /// true; anything not verified is false. `permissions` is true because
    /// the reply API exists (verified in the OpenAPI surface) and the event
    /// mapping is fixture-tested — real permission traffic was not
    /// reproducible with an auto-allow config (§99) and is documented as
    /// such.
    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            streaming: true,
            sessions: true,
            resume: true,
            cancel: true,
            tools: true,
            permissions: true,
            attachments: false,
            images: false,
            models: true,
            usage: false,
            reasoning: false,
            context_window: None,
            worktrees: false,
            parallel_sessions: true,
            session_revert: true,
            structured_events: true,
        }
    }

    fn health(&self) -> EngineHealth {
        EngineHealth::from(self.phase())
    }

    /// In-memory active runs (generic ids) for the core's lag-reconciliation.
    fn active_runs(&self) -> Vec<saiwork_core::engine::ActiveRun> {
        self.runs
            .list_active()
            .into_iter()
            .map(|(session_id, run_id)| saiwork_core::engine::ActiveRun { session_id, run_id })
            .collect()
    }

    /// Authoritative pending-permission snapshot (W2-002): every permission
    /// request currently held open, keyed by session/run/request. Reconciliation
    /// rebuilds the UI permission cards from this after a bounded-bus Lagged,
    /// so a missed `permission.requested` state event is recoverable. Bounded
    /// by live requests (FIFO eviction in `PendingPermissions`).
    fn pending_permissions(&self) -> Vec<saiwork_core::engine::PendingPermissionInfo> {
        self.pending.snapshot()
    }

    /// AUDIT-CORE-002: authoritative pending-question snapshot — every user
    /// question currently held open, keyed by session/run/request. Same
    /// reconciliation contract as `pending_permissions`; bounded by live
    /// requests (FIFO eviction in `PendingQuestions`).
    fn pending_questions(&self) -> Vec<saiwork_core::engine::PendingQuestionInfo> {
        self.pending_questions.snapshot()
    }

    async fn start(&self, ctx: &EngineStartContext) -> Result<(), EngineError> {
        match self.phase() {
            Phase::Idle | Phase::Stopped | Phase::Failed => {}
            _ => {
                return Err(EngineError::AlreadyStarted {
                    engine_id: ENGINE_ID.into(),
                })
            }
        }
        // A failed runtime may be discarded only after its process exit is
        // observable. Dropping a live one here would orphan adapter authority
        // on the exact path where failed-start teardown could not prove exit.
        {
            let mut guard = self.runtime.lock().expect("runtime mutex poisoned");
            if let Some(runtime) = guard.as_ref() {
                if !runtime.process.has_exited() {
                    return Err(OpenCodeError::PreviousRuntimeTerminationUnproven {
                        pid: runtime.process.pid(),
                    }
                    .into_engine());
                }
                guard.take();
            }
        }
        self.stop_requested.store(false, Ordering::SeqCst);
        self.set_phase(Phase::Starting);

        let result = self.start_inner(ctx).await;
        if result.is_err() {
            // Own the failure transition only if still Starting (a concurrent
            // stop() may already have moved us to Stopped — §34).
            self.cas_phase(Phase::Starting, Phase::Failed);
        }
        result
    }

    async fn stop(&self) -> Result<(), EngineError> {
        self.stop_impl(true).await
    }

    async fn kill(&self) -> Result<(), EngineError> {
        self.stop_impl(false).await
    }

    /// Providers/models discovered on demand and cached per runtime
    /// generation (§13–§14: a metadata failure never fails the engine; the
    /// runtime stays READY).
    async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
        let (client, generation) = self.client()?;
        self.models(&client, generation, false)
            .await
            .map_err(Into::into)
    }

    /// SAIWORK2 session id == engine session id: OpenCode owns session
    /// identity; we do not mint a second unrelated id (§15).
    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
        // The result is read-only display data; if the runtime was replaced
        // mid-request the response is still a truthful snapshot of the
        // server it came from, and the next call re-reads the live runtime.
        let (client, _generation) = self.client()?;
        let sessions = client.list_sessions().await?;
        Ok(sessions
            .into_iter()
            .map(|s| SessionInfo {
                id: s.id.clone(),
                engine_session_id: s.id.clone(),
                display_name: if s.title.is_empty() { s.id } else { s.title },
            })
            .collect())
    }

    /// Create a session on the server; the generic SAIWORK2 session id is
    /// minted by SessionManager and echoed verbatim, while the OpenCode
    /// session id stays upstream-only. `session.created` is published by
    /// SessionManager (the sole normalized `session.*` lifecycle publisher,
    /// TASK 24 §9) — never here.
    async fn create_session(
        &self,
        _req: &saiwork_core::engine::CreateSessionRequest,
    ) -> Result<SessionCreation, EngineError> {
        let (client, generation) = self.client()?;
        let session = match client.create_session().await {
            Ok(s) => s,
            Err(e) => return Ok(classify_create_failure(e)),
        };
        // §32: the session was created on the runtime that answered; if that
        // runtime was replaced while the request was in flight, the response
        // must not become current authority (and must not be announced).
        if !self.generation_matches(generation) {
            return Ok(SessionCreation::CreationUnknown {
                message: OpenCodeError::StaleRuntime.to_string(),
            });
        }
        Ok(SessionCreation::Created {
            engine_session_id: session.id.clone(),
            display_name: if session.title.is_empty() {
                session.id.clone()
            } else {
                session.title.clone()
            },
        })
    }

    /// Resume = re-access an existing OpenCode session by its id (§18–§19).
    /// No phantom replacement session is ever created. The generic id is not
    /// known at this boundary (it lives in SessionManager metadata), so the
    /// returned SessionInfo carries the upstream id in both fields — callers
    /// must map it through their own metadata.
    async fn resume_session(&self, engine_session_id: &str) -> Result<SessionInfo, EngineError> {
        let (client, generation) = self.client()?;
        let session = client.get_session(engine_session_id).await?;
        if !self.generation_matches(generation) {
            return Err(OpenCodeError::StaleRuntime.into());
        }
        Ok(SessionInfo {
            id: session.id.clone(),
            engine_session_id: session.id.clone(),
            display_name: if session.title.is_empty() {
                session.id.clone()
            } else {
                session.title.clone()
            },
        })
    }

    /// Read-only authoritative session history (fixture-verified
    /// `GET /session/{id}/message`): normalizes the raw message array so a
    /// resumed session can restore its exact user/assistant/tool order.
    /// Never a SQLite transcript mirror (TASK 24 §9).
    async fn session_history(
        &self,
        engine_session_id: &str,
    ) -> Result<Option<Vec<saiwork_core::engine::SessionMessage>>, EngineError> {
        let (client, generation) = self.client()?;
        let session = client.get_session(engine_session_id).await?;
        let raw = client.get_session_messages(engine_session_id).await?;
        if !self.generation_matches(generation) {
            return Err(OpenCodeError::StaleRuntime.into());
        }
        let mut history = normalize_session_history(&raw);
        if let Some(revert) = session.revert {
            if let Some(boundary) = history.iter().position(|message| message.id == revert.messageID) {
                history.truncate(boundary);
            }
        }
        Ok(Some(history))
    }

    /// Delete an upstream session; `session.closed` is published by
    /// SessionManager (the sole `session.*` lifecycle publisher), never here.
    async fn delete_session(&self, engine_session_id: &str) -> Result<(), EngineError> {
        let (client, generation) = self.client()?;
        client.delete_session(engine_session_id).await?;
        if !self.generation_matches(generation) {
            return Err(OpenCodeError::StaleRuntime.into());
        }
        Ok(())
    }

    async fn revert_session(
        &self,
        engine_session_id: &str,
        message_id: &str,
    ) -> Result<(), EngineError> {
        let (client, generation) = self.client()?;
        client.revert_session(engine_session_id, message_id).await?;
        if !self.generation_matches(generation) {
            return Err(OpenCodeError::StaleRuntime.into());
        }
        Ok(())
    }

    async fn unrevert_session(&self, engine_session_id: &str) -> Result<(), EngineError> {
        let (client, generation) = self.client()?;
        client.unrevert_session(engine_session_id).await?;
        if !self.generation_matches(generation) {
            return Err(OpenCodeError::StaleRuntime.into());
        }
        Ok(())
    }

    /// Send a prompt: validate → register run (SessionBusy check) → open the
    /// runtime event stream → dispatch the POST task → **await the
    /// authoritative acceptance receipt**. `send()` returns only when the
    /// upstream response proves the prompt was accepted, definitely rejected,
    /// or the outcome is unprovable — never a locally allocated RunId passed
    /// off as acceptance (TASK 24 §9). The POST task emits `message.started`
    /// on the first authoritative evidence and exactly one terminal (§22–§24).
    async fn send(
        &self,
        req: &saiwork_core::engine::SendRequest,
    ) -> Result<saiwork_core::engine::SendAcceptance, EngineError> {
        if self.phase() != Phase::Ready {
            return Err(EngineError::NotReady {
                engine_id: ENGINE_ID.into(),
            });
        }
        if req.prompt.trim().is_empty() {
            return Err(OpenCodeError::RequestFailed {
                detail: "empty prompt".into(),
            }
            .into_engine());
        }
        if req.prompt.len() > self.config.max_prompt_bytes {
            return Err(OpenCodeError::PromptTooLarge {
                bytes: req.prompt.len(),
                limit: self.config.max_prompt_bytes,
            }
            .into_engine());
        }
        let (client, generation) = self.client()?;

        // Model resolution: explicit canonical id → ModelRef, or None to
        // delegate to the OpenCode default (§62–§63, §143–§144).
        let model = match req.model.as_deref() {
            Some(id) if !id.trim().is_empty() => {
                Some(self.resolve_model_ref(&client, generation, id).await?)
            }
            _ => None,
        };

        let session_id = req.session_id.clone();
        let engine_session_id = req.engine_session_id.clone();
        let run = Arc::new(RunRecord {
            run_id: RunId::new(format!("run-{}", Uuid::new_v4())),
            session_id: session_id.clone(),
            engine_session_id: engine_session_id.clone(),
            generation,
            cancel_requested: AtomicBool::new(false),
            abort_delivered: AtomicBool::new(false),
            engine_lost: AtomicBool::new(false),
            started_emitted: AtomicBool::new(false),
            message_id: Mutex::new(None),
            session_error: Mutex::new(None),
            last_stream_activity: Mutex::new(None),
            state: Mutex::new(runs::RunState::Running),
            terminal_emitted: AtomicBool::new(false),
            post_task: Mutex::new(None),
            engine_lost_notify: Notify::new(),
            session_notify: Notify::new(),
        });
        self.runs.insert(run.clone())?; // SessionBusy / DuplicateRun (§70–§72)

        // The runtime event stream must be connected before the POST so the
        // live deltas are captured (§172). Reuses the existing live stream
        // (kept open for the whole READY runtime — no per-run idle close).
        let (ready_rx, close_tx) = {
            let guard = self.runtime.lock().expect("runtime mutex poisoned");
            let runtime = guard.as_ref().expect("client() checked runtime");
            self.ensure_stream(runtime)
        };
        let run_id = run.run_id.clone();
        let bus = self
            .runtime
            .lock()
            .expect("runtime mutex poisoned")
            .as_ref()
            .map(|r| r.bus.clone())
            .expect("runtime checked");
        let registry = self.runs.clone();
        let pending = self.pending.clone();
        let pending_questions = self.pending_questions.clone();
        // The POST task sends the authoritative acceptance receipt the moment
        // the upstream response resolves; send() waits for it.
        let (acceptance_tx, acceptance_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(post_run_task(
            bus.clone(),
            registry.clone(),
            pending.clone(),
            pending_questions.clone(),
            run.clone(),
            client.clone(),
            engine_session_id.clone(),
            req.prompt.clone(),
            model,
            ready_rx,
            close_tx.clone(),
            acceptance_tx,
        ));
        *run.post_task.lock().expect("post task mutex poisoned") = Some(task);
        info!(
            engine = ENGINE_ID,
            run = %run_id,
            session = %session_id,
            model = ?req.model,
            "opencode run dispatched"
        );
        // Await the authoritative acceptance (the POST has no overall timeout
        // — its lifetime is the run's; the receipt resolves on any outcome).
        match acceptance_rx.await {
            Ok(acc) => Ok(acc),
            Err(_) => Ok(saiwork_core::engine::SendAcceptance::OutcomeUnknown {
                run_id: run_id.to_string(),
                message: "engine stopped before the send outcome was confirmed".into(),
            }),
        }
    }

    /// Real cancellation: mark the run cancel-requested and POST the OpenCode
    /// abort API (verified: `POST /session/{id}/abort` → `true`). The POST
    /// task is the terminal authority — if OpenCode still reports a normal
    /// finish, that authoritative ordering wins (§44–§48). Never kills the
    /// engine (§45).
    async fn cancel(&self, run_id: &str) -> Result<(), EngineError> {
        let Some(record) = self.runs.request_cancel(run_id) else {
            // Unknown, already terminal, or already delivered: idempotent no-op (§47).
            return Ok(());
        };
        let client = match self.client() {
            Ok((client, _)) => client,
            Err(_) => return Ok(()), // engine stopping: terminal comes from stop path
        };
        match client.abort(&record.engine_session_id).await {
            Ok(_) => {
                record.abort_delivered.store(true, Ordering::SeqCst);
                Ok(())
            }
            Err(e) => {
                // §139: never fake CANCELLED.
                warn!(
                    engine = ENGINE_ID,
                    run = %record.run_id,
                    error = %e,
                    "opencode abort request failed; run outcome follows server response"
                );
                Ok(())
            }
        }
    }

    /// Resolve a pending permission via the OpenCode reply API (§41–§42).
    /// `session_id` is the generic SAIWORK2 id; the upstream session id is
    /// resolved through the active-run registry (TASK 24 §9).
    async fn resolve_permission(
        &self,
        session_id: &str,
        request_id: &str,
        allowed: bool,
    ) -> Result<(), EngineError> {
        let (client, _generation) = self.client()?;
        let engine_session_id = self
            .runs
            .engine_session_for_generic(session_id)
            .ok_or_else(|| OpenCodeError::SessionNotFound {
                session_id: session_id.into(),
            })?;
        client
            .reply_permission(&engine_session_id, request_id, allowed)
            .await?;
        // W2-002: the reply succeeded — the request is resolved; drop the
        // pending entry so the UI card is torn down (idempotent if the stream
        // already reported `permission.resolved`).
        self.pending.remove(request_id);
        Ok(())
    }

    /// AUDIT-CORE-002: answer/reject a pending user question via the typed
    /// OpenCode question API. `session_id` is the generic SAIWORK2 id; the
    /// upstream session id resolves through the active-run registry.
    async fn resolve_question(
        &self,
        session_id: &str,
        request_id: &str,
        resolution: &QuestionResolution,
    ) -> Result<(), EngineError> {
        let (client, _generation) = self.client()?;
        let engine_session_id = self
            .runs
            .engine_session_for_generic(session_id)
            .ok_or_else(|| OpenCodeError::SessionNotFound {
                session_id: session_id.into(),
            })?;
        match resolution {
            QuestionResolution::Answers(answers) => {
                client
                    .reply_question(&engine_session_id, request_id, answers)
                    .await?;
            }
            QuestionResolution::Rejected => {
                client
                    .reject_question(&engine_session_id, request_id)
                    .await?;
            }
        }
        // The reply succeeded — the request is resolved; drop the pending
        // entry so the UI card is torn down (idempotent if the stream already
        // reported `question.replied|rejected`).
        self.pending_questions.remove(request_id);
        Ok(())
    }

    /// Best-effort synchronous cleanup: mark the runtime as stop-requested so
    /// a later process exit (owned by the supervisor) is not reported as a
    /// crash, cancel any in-flight readiness, and close the event stream.
    /// Processes themselves are owned and stopped by the ProcessSupervisor
    /// at shutdown.
    fn dispose(&self) {
        self.stop_requested.store(true, Ordering::SeqCst);
        // W2-002: runtime is going away — drop every pending permission.
        self.pending.clear();
        // AUDIT-CORE-002: same for pending questions.
        self.pending_questions.clear();
        self.close_stream();
        if let Some(runtime) = self
            .runtime
            .lock()
            .expect("runtime mutex poisoned")
            .as_ref()
        {
            runtime.stop_requested.store(true, Ordering::SeqCst);
            let _ = runtime.cancel_tx.send(true);
        }
    }
}

/// The global event-stream task (one per runtime, §172–§174). Reads the SSE
/// `GET /event` endpoint, feeds `SseParser`, routes every parsed event to the
/// `EventRouter`. Lives exactly as long as the runtime needs it: it is
/// cancelled by stop/dispose and self-terminates on the close signal (idle
/// after the last run, §171) or on connection EOF/error.
#[allow(clippy::too_many_arguments)]
async fn stream_task(
    endpoint: Endpoint,
    secret: Secret,
    bus: EventBus,
    registry: Arc<RunRegistry>,
    pending: Arc<PendingPermissions>,
    pending_questions: Arc<PendingQuestions>,
    connect_timeout: Duration,
    ready: watch::Sender<bool>,
    mut close: watch::Receiver<bool>,
) {
    // The stream is the runtime's CONTROL channel (deltas, tool calls,
    // permission requests, session errors). Loss during a live run must
    // RECONNECT — never silently strand the run or hide a pending permission
    // request (TASK 24 §9). The `close` watch is the ONLY permanent-close
    // signal (stop / failure / generation replacement — no idle close, TASK
    // 24 perf). `ready` is the stream-ready truth: reset to false immediately
    // on loss, true again on successful reconnect, so new sends wait for a
    // live control channel instead of dispatching into the void.
    let router = EventRouter::new(bus, registry, pending, pending_questions, secret.clone());
    let url = format!("{}/event", endpoint.base_url());
    // Bounded, cancellable reconnect backoff. Cap at 5 s; every attempt is
    // cancellable on `close` so stop/shutdown never waits on a reconnect.
    const MAX_BACKOFF: Duration = Duration::from_secs(5);
    let mut backoff = Duration::from_millis(250);

    loop {
        // Permanent close (stop/failure/generation replacement) ends the loop.
        if *close.borrow() {
            break;
        }
        let client = match reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .redirect(reqwest::redirect::Policy::none()) // §71: redirects are suspicious
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                debug!(engine = ENGINE_ID, error = %e, "opencode stream client build failed");
                let _ = ready.send(false);
                backoff = wait_reconnect_backoff(&mut close, backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        let resp = match client
            .get(&url)
            .basic_auth("opencode", Some(secret.as_str()))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!(engine = ENGINE_ID, error = %e, "opencode event stream connect failed");
                let _ = ready.send(false);
                backoff = wait_reconnect_backoff(&mut close, backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        if !resp.status().is_success() {
            debug!(engine = ENGINE_ID, status = %resp.status(), "opencode event stream rejected");
            let _ = ready.send(false);
            backoff = wait_reconnect_backoff(&mut close, backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        // Connected: signal POST tasks they may dispatch (deltas will be seen).
        let _ = ready.send(true);
        backoff = Duration::from_millis(250); // reset on success

        let mut parser = SseParser::new(); // a fresh connection starts fresh
                                           // `Response::chunk()` is an ordinary async fn (no StreamExt needed);
                                           // it returns the next body chunk and `None` at EOF.
        let mut response = resp;
        loop {
            tokio::select! {
                _ = close.changed() => {
                    // Permanent-close signal. Do NOT drop in-flight deltas:
                    // drain with a bounded quiet period so a slow peer's
                    // final events are still processed before the connection
                    // closes (§173).
                    let drain_deadline = Instant::now() + Duration::from_secs(1);
                    loop {
                        tokio::select! {
                            chunk = response.chunk() => {
                                match chunk {
                                    Ok(Some(bytes)) => {
                                        if parser.push(&bytes, &mut |e| router.on_data(&e.data))
                                            == crate::sse::PushResult::LineTooLong
                                        {
                                            break;
                                        }
                                    }
                                    _ => break, // EOF/error ends the drain
                                }
                            }
                            _ = tokio::time::sleep(Duration::from_millis(100)) => break, // quiet
                        }
                        if Instant::now() > drain_deadline {
                            break;
                        }
                    }
                    break;
                }
                chunk = response.chunk() => {
                    match chunk {
                        Ok(Some(bytes)) => {
                            if parser.push(&bytes, &mut |e| router.on_data(&e.data))
                                == crate::sse::PushResult::LineTooLong
                            {
                                debug!(engine = ENGINE_ID, "opencode event stream line overflow");
                                break;
                            }
                        }
                        Ok(None) => {
                            parser.finish(&mut |e| router.on_data(&e.data));
                            debug!(engine = ENGINE_ID, "opencode event stream EOF");
                            break;
                        }
                        Err(e) => {
                            // Connection-level failure: the stream is best-effort
                            // UI deltas; run terminals come from the POST task,
                            // so this never fabricates a failure (§51). But the
                            // control channel IS gone — reconnect below.
                            debug!(engine = ENGINE_ID, error = %e, "opencode event stream error");
                            break;
                        }
                    }
                }
            }
        }
        // The control channel is gone: reset the stream-ready truth so new
        // sends wait for the reconnect instead of dispatching into the void.
        let _ = ready.send(false);
        // A permanent close fired during the drain: exit without reconnecting.
        if *close.borrow() {
            break;
        }
        backoff = wait_reconnect_backoff(&mut close, backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Sleep the current reconnect backoff, cancellable on the permanent-close
/// signal. Returns the next backoff to use (unchanged when cancelled — the
/// caller exits).
async fn wait_reconnect_backoff(close: &mut watch::Receiver<bool>, backoff: Duration) -> Duration {
    tokio::select! {
        _ = close.changed() => {}
        _ = tokio::time::sleep(backoff) => {}
    }
    backoff
}

/// The POST task: the terminal authority for a run (§24, §31–§32). It waits
/// for the runtime event stream to connect (so deltas are captured), then
/// sends the message. The response is the final message; the terminal
/// outcome follows the verified 1.18.18 semantics:
/// - message has `finish` (e.g. `stop`) → COMPLETED (authoritative — wins over a racing abort, §48);
/// - no `finish` + cancel was requested → CANCELLED;
/// - no `finish` + a `session.error` was observed → FAILED (provider error, §57–§59);
/// - no `finish` otherwise → FAILED (run ended without completion evidence).
///
/// The authoritative acceptance receipt is sent through `acceptance_tx` the
/// moment the upstream response resolves — the ONLY evidence a caller may
/// treat as engine acceptance (TASK 24 §9). Exactly one terminal is then
/// emitted (CAS gate); the run is deregistered and the stream is allowed to
/// Remaining safety window before the terminal decision (TASK 24 perf): the
/// full STREAM_IDLE_GRACE when there is no stream evidence at all, the
/// remainder after the most recent matched event, and ZERO when the stream is
/// already settled (> grace) or a `session.error` was already recorded.
fn remaining_idle_grace(idle: Option<Duration>, error_recorded: bool) -> Duration {
    if error_recorded {
        return Duration::ZERO;
    }
    match idle {
        None => STREAM_IDLE_GRACE,
        Some(elapsed) => STREAM_IDLE_GRACE.saturating_sub(elapsed),
    }
}

#[cfg(test)]
mod grace_tests {
    use super::*;

    #[test]
    fn grace_math_is_deterministic() {
        // No stream evidence: full safety cap.
        assert_eq!(remaining_idle_grace(None, false), STREAM_IDLE_GRACE);
        // Stream quiet longer than the grace: decide immediately (zero wait).
        assert_eq!(
            remaining_idle_grace(Some(Duration::from_millis(300)), false),
            Duration::ZERO
        );
        assert_eq!(
            remaining_idle_grace(Some(Duration::from_millis(250)), false),
            Duration::ZERO
        );
        // Last event 200 ms ago: wait only the remaining ~50 ms.
        assert_eq!(
            remaining_idle_grace(Some(Duration::from_millis(200)), false),
            Duration::from_millis(50)
        );
        // A recorded session.error wakes the decision immediately, regardless
        // of how fresh the last event was.
        assert_eq!(
            remaining_idle_grace(Some(Duration::from_millis(0)), true),
            Duration::ZERO
        );
        assert_eq!(remaining_idle_grace(None, true), Duration::ZERO);
    }
}

#[cfg(test)]
mod model_lookup_tests {
    use super::*;
    use crate::models::{Model, Provider};
    use std::collections::HashMap;

    fn provider(id: &str, models: &[(&str, &str)]) -> Provider {
        // Discriminating fixture: the MAP KEY differs from the inner
        // `model.id`/`providerID` so a resolver that trusts the inner fields
        // would resolve to a different identity than the canonical key
        // (TASK 24 §9: the map key is what the message API actually sends).
        let mut map = HashMap::new();
        for (key, inner_id) in models {
            map.insert(
                key.to_string(),
                Model {
                    id: (*inner_id).to_string(),
                    providerID: "legacy-provider".to_string(),
                    name: String::new(),
                    family: None,
                    capabilities: None,
                },
            );
        }
        Provider {
            id: id.to_string(),
            name: id.to_string(),
            models: map,
            connected: None,
        }
    }

    #[test]
    fn lookup_uses_namespaced_key_and_is_unambiguous_across_providers() {
        let providers = ProviderList {
            all: vec![
                provider("p1", &[("canon-a", "inner-x"), ("shared", "inner-y")]),
                provider("p2", &[("canon-b", "inner-z"), ("shared", "inner-w")]),
            ],
            connected: None,
            default: std::collections::HashMap::new(),
        };
        let cache = build_model_cache(7, &providers);
        // Sorted UI list intact: ids are <provider>/<raw-key> namespaced, so
        // the same raw key "shared" appears once per provider as distinct ids
        // — the OLD ambiguity (same canonical id, two providers) is gone.
        let ids: Vec<&str> = cache.models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["p1/canon-a", "p1/shared", "p2/canon-b", "p2/shared"]
        );
        assert_eq!(cache.models[0].provider.as_deref(), Some("p1"));

        // Each namespaced id resolves to the exact wire pair via the O(1) map.
        match cache.by_id.get("p1/canon-a") {
            Some(ModelRef {
                providerID,
                modelID,
            }) => {
                assert_eq!((providerID.as_str(), modelID.as_str()), ("p1", "canon-a"))
            }
            other => panic!("expected a ModelRef, got {other:?}"),
        }
        // Same raw key, distinct namespaces — both resolvable, no ambiguity.
        match cache.by_id.get("p1/shared") {
            Some(ModelRef {
                providerID,
                modelID,
            }) => {
                assert_eq!((providerID.as_str(), modelID.as_str()), ("p1", "shared"))
            }
            other => panic!("expected a ModelRef, got {other:?}"),
        }
        match cache.by_id.get("p2/shared") {
            Some(ModelRef {
                providerID,
                modelID,
            }) => {
                assert_eq!((providerID.as_str(), modelID.as_str()), ("p2", "shared"))
            }
            other => panic!("expected a ModelRef, got {other:?}"),
        }
        // Missing is missing.
        assert!(!cache.by_id.contains_key("p1/nope"));
        assert!(!cache.by_id.contains_key("nope"));
        // The UI list provider attribution never uses the legacy inner
        // providerID of the models.
        assert!(cache
            .models
            .iter()
            .all(|m| m.provider.as_deref() == Some("p1") || m.provider.as_deref() == Some("p2")));
    }

    fn write_auth(tmp: &tempfile::TempDir, contents: &str) -> std::path::PathBuf {
        let path = tmp.path().join("auth.json");
        std::fs::write(&path, contents).expect("write auth.json");
        path
    }

    /// An auth provider with models is appended; the catalog keeps its own
    /// identity for every existing id.
    #[test]
    fn augment_appends_new_provider_and_keeps_catalog_authority() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let auth = write_auth(
            &tmp,
            r#"{
                "fresh-corp": {"type": "api", "key": "sk-1", "models": ["m-new"]},
                "p1": {"type": "api", "key": "sk-2", "models": ["should-not-merge"]}
            }"#,
        );
        let mut list = ProviderList {
            all: vec![
                provider("p1", &[("canon-a", "inner-x")]),
                provider("p2", &[("canon-b", "inner-z")]),
            ],
            connected: None,
            default: HashMap::new(),
        };
        augment_auth_providers(&mut list, Some(&auth));
        assert_eq!(list.all.len(), 3);
        // Catalog id p1 untouched (catalog is the authority — no override).
        assert!(list
            .all
            .iter()
            .any(|p| p.id == "p1" && p.models.contains_key("canon-a")));
        assert!(!list
            .all
            .iter()
            .any(|p| p.id == "p1" && p.models.contains_key("should-not-merge")));
        // Fresh auth provider appended with its declared model.
        let fresh = list
            .all
            .iter()
            .find(|p| p.id == "fresh-corp")
            .expect("fresh-corp");
        assert!(fresh.models.contains_key("m-new"));
    }

    /// Credential-only auth entries (the real sambanova-free shape) are
    /// dropped — never an empty provider shell in the list.
    #[test]
    fn augment_drops_credential_only_providers() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let auth = write_auth(&tmp, r#"{"ghost": {"type": "api", "key": "sk-3"}}"#);
        let mut list = ProviderList {
            all: vec![provider("p1", &[("canon-a", "inner-x")])],
            connected: None,
            default: HashMap::new(),
        };
        augment_auth_providers(&mut list, Some(&auth));
        assert_eq!(list.all.len(), 1);
        assert!(list.all.iter().all(|p| p.id == "p1"));
    }

    /// Missing and malformed files are silent no-ops — a credential file
    /// must never break model discovery.
    #[test]
    fn augment_missing_and_malformed_file_is_noop() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let missing = tmp.path().join("does-not-exist.json");
        let mut list = ProviderList {
            all: vec![provider("p1", &[("canon-a", "inner-x")])],
            connected: None,
            default: HashMap::new(),
        };
        augment_auth_providers(&mut list, Some(&missing));
        assert_eq!(list.all.len(), 1);

        let malformed = write_auth(&tmp, "{{{not json");
        augment_auth_providers(&mut list, Some(&malformed));
        assert_eq!(list.all.len(), 1);
    }

    /// The connected-only filter keeps exactly the credential-connected
    /// providers and drops the paywalled rest.
    #[test]
    fn connected_filter_keeps_only_connected_providers() {
        let mut p1 = provider("p1", &[("canon-a", "inner-x")]); p1.connected = Some(false);
        let mut p2 = provider("p2", &[("canon-b", "inner-z")]); p2.connected = Some(false);
        let mut p3 = provider("p3", &[("canon-c", "inner-y")]); p3.connected = Some(false);
        let mut list = ProviderList {
            all: vec![p1, p2, p3],
            connected: Some(vec!["p2".into()]),
            default: HashMap::new(),
        };
        apply_connected_filter(&mut list, &["auth-only".into()]);
        assert_eq!(list.all.len(), 1);
        assert_eq!(list.all[0].id, "p2");
    }

    /// A provider merged from auth.json survives the filter even when the
    /// server catalog does not list it in `connected` (it has credentials on
    /// disk — usable by definition).
    #[test]
    fn connected_filter_keeps_auth_added_provider() {
        let mut p1 = provider("p1", &[("canon-a", "inner-x")]); p1.connected = Some(false);
        let mut fresh = provider("fresh-corp", &[("m-new", "inner-f")]); fresh.connected = Some(false);
        let mut list = ProviderList {
            all: vec![p1, fresh],
            connected: Some(vec!["p1".into()]),
            default: HashMap::new(),
        };
        apply_connected_filter(&mut list, &["fresh-corp".into()]);
        assert_eq!(list.all.len(), 2);
        assert!(list.all.iter().any(|p| p.id == "fresh-corp"));
    }

    /// Missing `connected` authority (`None`, the strict `/config/providers`
    /// fallback wire) must NEVER collapse the catalog to nothing — degraded
    /// path shows all. This deliberately does NOT conflate an empty vector
    /// with a missing authority (CORE-006).
    #[test]
    fn connected_filter_missing_authority_keeps_everything() {
        let mut list = ProviderList {
            all: vec![
                provider("p1", &[("canon-a", "inner-x")]),
                provider("p2", &[("canon-b", "inner-z")]),
            ],
            connected: None,
            default: HashMap::new(),
        };
        apply_connected_filter(&mut list, &[]);
        assert_eq!(list.all.len(), 2);
    }

    /// AUTHORITATIVE empty `connected` (`Some([])`, a primary `/provider`
    /// response that genuinely reports zero connected providers) must filter
    /// the catalog to nothing but auth-backed providers — NOT be treated as a
    /// missing authority (CORE-006). This is the case the old `is_empty()`-
    /// based logic got wrong.
    #[test]
    fn connected_filter_authoritative_empty_drops_all() {
        let mut list = ProviderList {
            all: vec![
                provider("p1", &[("canon-a", "inner-x")]),
                provider("p2", &[("canon-b", "inner-z")]),
            ],
            connected: Some(vec![]),
            default: HashMap::new(),
        };
        apply_connected_filter(&mut list, &[]);
        assert_eq!(list.all.len(), 0);
    }
}

#[cfg(test)]
mod history_truncation_tests {
    use super::*;

    #[test]
    fn bounded_prefix_never_panics_on_multibyte_cutoffs() {
        // A long Cyrillic string whose code points straddle the 4096-byte
        // tool-output cutoff: raw `s[..4096]` would panic mid-char.
        let cyrillic = "фффф".repeat(3000); // 6000 bytes, 2 bytes/char
        let capped = bounded_prefix(&cyrillic, 4096);
        assert!(capped.is_char_boundary(capped.find('…').unwrap()));
        // 2-byte chars: 4096 is itself a valid boundary — the prefix keeps
        // exactly the cap, never a mid-char slice.
        assert!(capped.starts_with(&cyrillic[..4096]));
        assert!(capped.ends_with("… (truncated)"));
        assert!(std::str::from_utf8(capped.as_bytes()).is_ok());

        // Emoji straddling the 1 MiB message cutoff (4 bytes/char).
        let emoji = "😀".repeat(300_000); // 1.2 MiB
        let big = bounded_prefix(&emoji, 1_048_576);
        assert!(big.is_char_boundary(big.find('…').unwrap()));
        assert!(std::str::from_utf8(big.as_bytes()).is_ok());
        assert!(big.len() < 1_048_576 + 32);
    }

    #[test]
    fn bounded_prefix_ascii_matches_old_slice_exactly() {
        let ascii = "x".repeat(5000);
        let capped = bounded_prefix(&ascii, 4096);
        assert_eq!(capped, format!("{}… (truncated)", &ascii[..4096]));
        // In-cap payloads are untouched.
        assert_eq!(bounded_prefix("hello", 4096), "hello");
        assert_eq!(bounded_prefix(&ascii, 1_048_576), ascii);
    }
}

/// close when idle.
#[allow(clippy::too_many_arguments)]
async fn post_run_task(
    bus: EventBus,
    registry: Arc<RunRegistry>,
    pending: Arc<PendingPermissions>,
    pending_questions: Arc<PendingQuestions>,
    run: Arc<RunRecord>,
    client: ApiClient,
    engine_session_id: String,
    prompt: String,
    model: Option<ModelRef>,
    mut ready: watch::Receiver<bool>,
    _close_tx: watch::Sender<bool>,
    acceptance_tx: tokio::sync::oneshot::Sender<saiwork_core::engine::SendAcceptance>,
) {
    // Bounded wait for the stream connection (§9: connection establishment
    // bounded; the POST itself has no timeout — its lifetime is the run's).
    // A REUSED stream is already connected: `ready.changed()` would wait for
    // a change that never comes (the value is already `true`), so check the
    // current value first and only wait when still connecting (TASK 24 perf:
    // back-to-back sends must never incur a 10 s already-ready wait).
    if !*ready.borrow() {
        let stream_ready = tokio::time::timeout(Duration::from_secs(10), ready.changed()).await;
        if stream_ready.is_err() || !*ready.borrow() {
            debug!(
                engine = ENGINE_ID,
                run = %run.run_id,
                "opencode event stream not connected in time; run still proceeds"
            );
        }
    }

    // Two-phase send (§9): `send_message_start` resolves on HTTP 2xx headers
    // — the run is live upstream — and the receipt is sent THE moment that
    // happens. `PendingMessage::finish` then reads the final message body.
    let outcome = match client
        .send_message_start(&engine_session_id, model.as_ref(), &prompt)
        .await
    {
        Ok(pending) => {
            // Authoritative acceptance: the upstream accepted the POST. This
            // is the ONLY evidence a caller may treat as "accepted" — never
            // the locally allocated RunId.
            let _ = acceptance_tx.send(saiwork_core::engine::SendAcceptance::Accepted {
                run_id: run.run_id.to_string(),
            });
            // §22: started is emitted on the first authoritative evidence;
            // if the stream never delivered any, the accepted response is
            // itself the evidence — ensure started precedes terminal.
            run.mark_started(&bus);

            // Read the final message body. A failure here means the run was
            // accepted but its terminal is unreadable (truncated/ambiguous).
            // Two cases: the ENGINE died (authoritative loss — the run is
            // definitively over; the exit watcher / stop path owns a Failed
            // terminal and marks engine_lost) or the transport died on a
            // live engine (honest Unknown, §9). Give the engine-loss path a
            // bounded window to claim the terminal before settling.
            match pending.finish().await {
                Err(e) if is_connection_loss(&e) => {
                    // PERF-008: park on the engine-loss wake instead of polling
                    // every 10 ms; wake immediately on `mark_engine_lost`, else
                    // resolve at the 500 ms deadline.
                    let deadline = Instant::now() + Duration::from_millis(500);
                    let until = tokio::time::Instant::from_std(deadline);
                    tokio::select! {
                        _ = run.engine_lost_notify.notified() => {}
                        _ = tokio::time::sleep_until(until) => {}
                    }
                    if run.engine_lost.load(Ordering::SeqCst) {
                        TerminalOutcome::Failed("engine process exited: run interrupted".into())
                    } else {
                        TerminalOutcome::Unknown(format!(
                            "opencode accepted the send but its outcome is unknown: {e}"
                        ))
                    }
                }
                Err(e) => TerminalOutcome::Unknown(format!(
                    "opencode accepted the send but its outcome is unknown: {e}"
                )),
                Ok(message) => {
                    run.note_message_id(&message.info.id, &bus);
                    // Facts that are stable as soon as the response arrives.
                    let finish = message.info.finish.is_some();
                    let cancel_requested = run.cancel_requested.load(Ordering::SeqCst);

                    // Bounded settle before reading the rest: a `session.error`
                    // (and the last deltas) travel on the separate /event
                    // connection and can lag the POST response by a few
                    // hundred ms (§166). Wait only the REMAINING grace after
                    // the most recent matched stream event — a settled stream
                    // (quiet > grace) pays NO terminal tax, while a fresh
                    // event keeps the full safety window (TASK 24 perf). A
                    // recorded `session.error` wakes the decision immediately.
                    let idle = run.idle_for();
                    let already_error = run
                        .session_error
                        .lock()
                        .expect("run error mutex poisoned")
                        .is_some();
                    let remaining = remaining_idle_grace(idle, already_error);
                    if !already_error && !remaining.is_zero() {
                        // PERF-008: park on the session-error wake instead of
                        // polling every 10 ms; wake immediately on
                        // `record_session_error`, else resolve at the deadline.
                        let deadline = Instant::now() + remaining;
                        let until = tokio::time::Instant::from_std(deadline);
                        tokio::select! {
                            _ = run.session_notify.notified() => {}
                            _ = tokio::time::sleep_until(until) => {}
                        }
                    }

                    let session_error = run
                        .session_error
                        .lock()
                        .expect("run error mutex poisoned")
                        .clone();
                    let abort_delivered = run.abort_delivered.load(Ordering::SeqCst);
                    if finish && !cancel_requested {
                        TerminalOutcome::Completed
                    } else if abort_delivered && !finish {
                        TerminalOutcome::Cancelled
                    } else if let Some(error) = session_error {
                        TerminalOutcome::Failed(error)
                    } else if !finish {
                        TerminalOutcome::Failed(
                            "run ended without a completion signal (aborted or interrupted)".into(),
                        )
                    } else {
                        // finish && cancel: the abort raced and OpenCode
                        // completed the run — authoritative ordering says
                        // completed (§48).
                        TerminalOutcome::Completed
                    }
                }
            }
        }
        Err(e) => {
            let acceptance = classify_send_failure(run.run_id.as_str(), e);
            let message = match &acceptance {
                saiwork_core::engine::SendAcceptance::DefinitelyRejected {
                    code, message, ..
                } => format!("opencode rejected the send ({code}): {message}"),
                saiwork_core::engine::SendAcceptance::OutcomeUnknown { message, .. } => {
                    format!("opencode send outcome unknown: {message}")
                }
                saiwork_core::engine::SendAcceptance::Accepted { .. } => unreachable!(),
            };
            // A definite rejection means nothing started upstream; an unknown
            // outcome may still be running — surface started so the run is
            // visible until the honest terminal.
            let definitely_rejected = matches!(
                &acceptance,
                saiwork_core::engine::SendAcceptance::DefinitelyRejected { .. }
            );
            let _ = acceptance_tx.send(acceptance);
            if !definitely_rejected {
                run.mark_started(&bus);
            }
            // A definite rejection means nothing started upstream → Failed.
            // An UNKNOWN send outcome may still be live: emit the honest
            // MessageOutcomeUnknown, NEVER a MessageFailed — a possibly-live
            // run must not be asserted as definite failure (TASK 24 §9).
            if definitely_rejected {
                TerminalOutcome::Failed(message)
            } else {
                TerminalOutcome::Unknown(message)
            }
        }
    };

    emit_terminal(&bus, &run, outcome);
    // W2-002: the run is terminal — drop its pending permissions so a finished
    // run never leaves a stranded open request in `pending_permissions()`.
    // AUDIT-CORE-002: same for pending questions.
    pending.remove_for_run(run.run_id.as_str());
    pending_questions.remove_for_run(run.run_id.as_str());
    registry.remove(run.run_id.as_str());
    // NO idle close: the runtime-global event stream stays connected for the
    // READY runtime and is reused by the next send (TASK 24 perf — closing
    // 250 ms after each run forced a reconnect between every human prompt).
    // The stream is closed only on stop/failure/generation replacement via
    // `close_stream` / `ensure_stream`.
}

/// True when the failure is a transport/connection loss rather than a
/// protocol or server verdict — the case where the POST task must decide
/// between engine-loss (authoritative Failed) and honest Unknown.
fn is_connection_loss(e: &OpenCodeError) -> bool {
    matches!(
        e,
        OpenCodeError::Disconnected { .. } | OpenCodeError::StaleRuntime
    )
}

/// Classify a send-message failure into the authoritative acceptance outcome
/// (TASK 24 §9). Definitive server verdicts (HTTP status / session errors)
/// are `DefinitelyRejected` — nothing was accepted. Anything where the
/// request may have crossed the boundary (transport loss, malformed or
/// untrusted response, stale runtime) is `OutcomeUnknown` — never retried
/// blindly. Conservative by design: a wrongly-classified UNKNOWN only blocks
/// the workspace until explicit resolution; a wrongly-classified FAILED
/// could lose an accepted run.
fn classify_send_failure(run_id: &str, e: OpenCodeError) -> SendAcceptance {
    let message = e.to_string();
    let code = match &e {
        OpenCodeError::SessionNotFound { .. } => Some("session_not_found"),
        OpenCodeError::SessionBusy { .. } => Some("session_busy"),
        OpenCodeError::Http { .. } => Some("rejected"),
        _ => None,
    };
    match code {
        Some(code) => SendAcceptance::DefinitelyRejected {
            run_id: run_id.into(),
            code: code.into(),
            message,
        },
        None => SendAcceptance::OutcomeUnknown {
            run_id: run_id.into(),
            message,
        },
    }
}

/// Classify a create-session failure into the authoritative creation outcome.
/// A definitive HTTP verdict means nothing was created; anything where the
/// request may have crossed the boundary is `CreationUnknown` (the engine may
/// hold an orphan session — never loop-create).
fn classify_create_failure(e: OpenCodeError) -> SessionCreation {
    let message = e.to_string();
    match e {
        OpenCodeError::Http { .. } => SessionCreation::DefinitelyNotCreated {
            code: "create_rejected".into(),
            message,
        },
        _ => SessionCreation::CreationUnknown { message },
    }
}

/// Normalize the raw `GET /session/{id}/message` array (TASK 24 §9): each
/// message keeps its stable id, role and (bounded) text; tool parts become
/// separate normalized tool entries keyed by their own part id.
///
/// AUDIT-CORE-004: the parent user/assistant entry is emitted BEFORE the
/// tool entries derived from it — the frontend hydrator attaches a tool to
/// the nearest PRECEDING assistant turn, so emitting tools first fabricated
/// a blank synthetic assistant above the real answer. `order` distinguishes
/// parent and child: the parent keeps `order*2`, its j-th tool part gets
/// `order*2 + 1 + j` (deterministic, strictly increasing across messages).
fn normalize_session_history(raw: &serde_json::Value) -> Vec<SessionMessage> {
    let mut out = Vec::new();
    let arr = match raw.as_array() {
        Some(a) => a,
        None => return out,
    };
    for (order, msg) in arr.iter().enumerate() {
        // OpenCode has shipped both flat `{id,role,parts,time}` history rows
        // and `{info:{id,role,time},parts}` rows. Normalize both through the
        // same boundary; `parts` always remains on the outer message.
        let info = msg.get("info").unwrap_or(msg);
        let ts = info
            .get("time")
            .and_then(|time| time.get("created"))
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let id = info
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let role = info
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let parts = msg.get("parts").and_then(|v| v.as_array());
        let mut text = String::new();
        // AUDIT-CORE-004: tool entries are collected while scanning parts and
        // pushed AFTER the parent entry below (parent-before-child).
        let mut tool_parts: Vec<SessionMessage> = Vec::new();
        if let Some(parts) = parts {
            for part in parts {
                match part.get("type").and_then(|v| v.as_str()) {
                    Some("text") => {
                        if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                            text.push_str(t);
                        }
                    }
                    Some("tool") => {
                        // A tool part becomes its own normalized entry (the
                        // Conversation restores tool cards keyed by their
                        // stable call id).
                        let part_id = part
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let tool = part
                            .get("tool")
                            .and_then(|v| v.as_str())
                            .unwrap_or("tool")
                            .to_string();
                        let out_text = part
                            .get("state")
                            .and_then(|s| s.get("output"))
                            .and_then(|o| o.as_str())
                            .map(|s| bounded_prefix(s, 4096))
                            .unwrap_or_default();
                        let call_id = part_id.clone();
                        let child_order =
                            (order as u64) * 2 + 1 + tool_parts.len() as u64;
                        tool_parts.push(SessionMessage {
                            id: if part_id.is_empty() {
                                format!("{id}-tool-{order}")
                            } else {
                                part_id
                            },
                            role: "tool".into(),
                            text: out_text,
                            tool_call_id: Some(call_id).filter(|s| !s.is_empty()),
                            tool: Some(tool),
                            order: child_order,
                            ts: part
                                .get("state")
                                .and_then(|state| state.get("time"))
                                .and_then(|time| time.get("start"))
                                .and_then(|value| value.as_i64())
                                .unwrap_or(ts),
                        });
                    }
                    _ => {}
                }
            }
        }
        out.push(SessionMessage {
            id: if id.is_empty() {
                format!("msg-{order}")
            } else {
                id
            },
            role,
            text: bounded_prefix(&text, 1_048_576),
            tool_call_id: None,
            tool: None,
            order: (order as u64) * 2,
            ts,
        });
        out.extend(tool_parts);
    }
    out
}

/// UTF-8-safe bounded prefix (TASK 24 §9): `s[..max]` panics when the byte
/// cutoff splits a multi-byte character; this floors the limit to a valid
/// char boundary before appending the existing truncation marker. ASCII
/// truncation stays byte-identical to the old slice.
fn bounded_prefix(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}… (truncated)", &s[..end])
    }
}
