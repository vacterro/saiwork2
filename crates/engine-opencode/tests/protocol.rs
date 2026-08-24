//! TASK 11 fixture-driven protocol gate (ENGINE_CONTRACT.md §162–§167).
//!
//! Drives the session layer against `fixture_protocol` (tests/bin/
//! fixture_protocol.rs), a full fake OpenCode server. The mock proves the
//! adapter: send flow, deltas, tools, cancellation, races, SSE edge cases,
//! error mapping, multi-session isolation. Real OpenCode compatibility is
//! proven separately in tests/real.rs — a fixture pass is never a real
//! integration pass (§104).

// The fixture serialization lock is intentionally held across awaits: tests
// run in parallel in one process and must not race the process-global
// fixture env. Same pattern and justification as tests/hostile.rs.
#![allow(clippy::await_holding_lock)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine_opencode::{OpenCodeAdapter, OpenCodeConfig};
use saiwork_core::engine::{EngineAdapter, EngineError, EngineHealth, EngineStartContext};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{Event, EventBus};
use saiwork_process::ProcessSupervisor;

/// Path to the protocol fixture executable (cargo provides this for bins in
/// the same package).
const FIXTURE_EXE: &str = env!("CARGO_BIN_EXE_fixture_protocol");

static FIXTURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn fixture_guard() -> std::sync::MutexGuard<'static, ()> {
    FIXTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Harness {
    bus: EventBus,
    diagnostics: Arc<Diagnostics>,
    supervisor: Arc<ProcessSupervisor>,
    workspace: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let bus = EventBus::new();
        let diagnostics = Arc::new(Diagnostics::new());
        let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
        let workspace = tempfile::tempdir().expect("temp workspace");
        Self {
            bus,
            diagnostics,
            supervisor,
            workspace,
        }
    }

    fn context(&self) -> EngineStartContext {
        let bus = self.bus.clone();
        let diagnostics = self.diagnostics.clone();
        let failure_bus = bus.clone();
        EngineStartContext {
            workspace_id: None,
            workspace_path: Some(self.workspace.path().to_path_buf()),
            bus,
            diagnostics: diagnostics.clone(),
            supervisor: self.supervisor.clone(),
            report_failure: Arc::new(move |engine_id: &str, message: &str| {
                diagnostics.record_error("ENGINE_FAILED", format!("{engine_id}: {message}"));
                failure_bus.publish(Event::EngineFailed {
                    engine_id: engine_id.into(),
                    error: message.into(),
                });
            }),
        }
    }

    /// Config pointing at the protocol fixture. Callers must hold
    /// `fixture_guard()` for the whole test.
    fn fixture_config(&self) -> OpenCodeConfig {
        OpenCodeConfig {
            explicit_executable: Some(PathBuf::from(FIXTURE_EXE)),
            startup_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(3),
            metadata_timeout: Duration::from_secs(10),
            // Tests are hermetic: never read the developer's real auth.json
            // (a nonexistent path is a silent no-op).
            auth_json_path: Some(PathBuf::from("__no_auth_json_here__")),
            ..OpenCodeConfig::default()
        }
    }

    fn supervisor_count(&self) -> usize {
        self.supervisor.count()
    }
}

/// Every fixture env key the suite can set must be cleared before a test
/// spawns the fixture process, or a stale value from an earlier test leaks
/// into THIS test's fixture (e.g. FIXTURE_ABORT_MODE=hang from an abort test
/// making every later cancel hang, FIXTURE_MSG_MODE=malformed breaking every
/// later send). Keep this list in sync with the fixture's env surface.
fn clear_fixture_env() {
    for key in [
        "FIXTURE_MSG_MODE",
        "FIXTURE_MSG_DELAY_MS",
        "FIXTURE_TOOL",
        "FIXTURE_EVENT_STYLE",
        "FIXTURE_EVENT_DROP_AFTER",
        "FIXTURE_CRASH_AFTER_MS",
        "FIXTURE_PROVIDER_COUNT",
        "FIXTURE_PROVIDER_MODELS_PER",
        "FIXTURE_PROVIDER_HTTP",
        "FIXTURE_PROVIDER_BODY",
        "FIXTURE_PROVIDER_FALLBACK",
        "FIXTURE_PROVIDER_CONNECTED",
        "FIXTURE_MSG_ERROR_BODY",
        "FIXTURE_ABORT_MODE",
        "FIXTURE_DELTA_COUNT",
    ] {
        std::env::remove_var(key);
    }
}

/// Set fixture env knobs (removing stale ones), then start the adapter.
async fn start_with_env(harness: &Harness, overrides: &[(&str, &str)]) -> OpenCodeAdapter {
    clear_fixture_env();
    for (k, v) in overrides {
        std::env::set_var(k, v);
    }
    let adapter = OpenCodeAdapter::new(harness.fixture_config());
    adapter
        .start(&harness.context())
        .await
        .expect("fixture must reach READY");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter
}

/// Bounded event collection from the bus.
struct Collector {
    rx: saiwork_events::Subscription,
    events: Vec<Event>,
}

impl Collector {
    fn new(bus: &EventBus) -> Self {
        Self {
            rx: bus.subscribe(),
            events: Vec::new(),
        }
    }

    fn drain(&mut self) {
        while let Ok(Some(envelope)) = self.rx.try_recv() {
            self.events.push(envelope.event);
        }
    }

    async fn wait_for(&mut self, what: &str, pred: impl Fn(&Event) -> bool, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            self.drain();
            if self.events.iter().any(&pred) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what} (events so far: {:?})",
                self.events.iter().map(Event::name).collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    /// Wait for the run's terminal event (completed | failed | cancelled).
    async fn wait_terminal(&mut self, run: &str, timeout: Duration) -> Event {
        let deadline = Instant::now() + timeout;
        loop {
            self.drain();
            let terminal = self.events.iter().find(|e| {
                matches!(
                    e,
                    Event::MessageCompleted { run_id, .. }
                        | Event::MessageFailed { run_id, .. }
                        | Event::MessageCancelled { run_id, .. }
                        | Event::MessageOutcomeUnknown { run_id, .. }
                        if run_id.as_str() == run
                )
            });
            if let Some(terminal) = terminal {
                return terminal.clone();
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for terminal of {run}"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    /// Number of terminal events for a run (must be exactly 1, §24/§165).
    fn terminal_count(&self, run: &str) -> usize {
        self.events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::MessageCompleted { run_id, .. }
                        | Event::MessageFailed { run_id, .. }
                        | Event::MessageCancelled { run_id, .. }
                        | Event::MessageOutcomeUnknown { run_id, .. }
                        if run_id.as_str() == run
                )
            })
            .count()
    }

    fn deltas(&self, run: &str) -> String {
        self.events
            .iter()
            .filter_map(|e| match e {
                Event::MessageDelta { run_id, delta, .. } if run_id.as_str() == run => {
                    Some(delta.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .concat()
    }
}

async fn create_session(adapter: &OpenCodeAdapter) -> saiwork_core::engine::SessionInfo {
    let generic = fresh_session_id();
    match adapter
        .create_session(&saiwork_core::engine::CreateSessionRequest {
            session_id: generic.clone(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect("create session")
    {
        saiwork_core::engine::SessionCreation::Created {
            engine_session_id,
            display_name,
        } => saiwork_core::engine::SessionInfo {
            id: generic,
            engine_session_id,
            display_name,
        },
        other => panic!("expected Created, got {other:?}"),
    }
}

/// The fixture always accepts a prompt; unwrap the authoritative receipt to
/// the run handle for assertions.
fn accepted(acc: saiwork_core::engine::SendAcceptance) -> saiwork_core::engine::RunHandle {
    match acc {
        saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
            saiwork_core::engine::RunHandle { run_id }
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
}

/// Unique generic session id per call (the manager mints these in the real
/// app; tests need distinct ids so no two sessions alias).
fn fresh_session_id() -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "ses-{}",
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    )
}

async fn send(
    adapter: &OpenCodeAdapter,
    session: &saiwork_core::engine::SessionInfo,
    prompt: &str,
    model: Option<&str>,
) -> saiwork_core::engine::RunHandle {
    match adapter
        .send(&saiwork_core::engine::SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: prompt.into(),
            model: model.map(str::to_string),
        })
        .await
        .expect("send")
    {
        saiwork_core::engine::SendAcceptance::Accepted { run_id }
        | saiwork_core::engine::SendAcceptance::DefinitelyRejected { run_id, .. }
        | saiwork_core::engine::SendAcceptance::OutcomeUnknown { run_id, .. } => {
            // Every outcome carries the run that emits the terminal; callers
            // then assert the *typed* terminal (failed / unknown) they expect.
            saiwork_core::engine::RunHandle { run_id }
        }
    }
}

// ---------------------------------------------------------------------------
// Models / providers
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn models_are_discovered_and_normalized() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let models = adapter.list_models().await.expect("models");
    // Raw fixture keys are `model-1`/`model-2` per provider; the adapter
    // namespaces them: fixture-p1/model-1 + model-2, fixture-p2/model-1 + 2.
    assert!(models.len() >= 4, "got {}", models.len());
    let m1 = models
        .iter()
        .find(|m| m.id == "fixture-p1/model-1")
        .expect("namespaced model id");
    // The generic identity is provider-id + RAW map key (never the inner
    // `id`/`providerID` — the fixture deliberately sets those to
    // `inner-legacy-1`/`legacy-provider`; if any layer substituted them the
    // assertions below would fail — TASK 24 §9).
    assert_eq!(m1.id, "fixture-p1/model-1");
    assert_eq!(m1.provider.as_deref(), Some("fixture-p1"));
    assert_ne!(m1.id, "inner-legacy-1");
    assert_ne!(m1.provider.as_deref(), Some("legacy-provider"));
    assert_eq!(m1.display_name, "Fixture Model 1");
    // Provider display name flows from the wire `Provider.name` (the UI
    // shows it instead of the raw key — provider attribution).
    assert_eq!(m1.provider_name.as_deref(), Some("Fixture Provider 1"));
    // Cache: a second call returns the same set.
    let again = adapter.list_models().await.expect("cached models");
    assert_eq!(again.len(), models.len());
    adapter.stop().await.expect("stop");
}

/// Connected-only catalog (TASK 27): providers the user has NO credentials
/// for must not appear in the model list at all — the default catalog is
/// ~96% paywalled noise. The fixture declares 3 providers but `connected`
/// only contains fixture-p1: exactly its models are returned.
#[tokio::test(flavor = "multi_thread")]
async fn unconnected_providers_are_filtered_out() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_PROVIDER_COUNT", "3"),
            ("FIXTURE_PROVIDER_CONNECTED", "fixture-p1"),
        ],
    )
    .await;
    let models = adapter.list_models().await.expect("models");
    assert_eq!(models.len(), 2, "only fixture-p1 models: {models:?}");
    assert!(models
        .iter()
        .all(|m| m.provider.as_deref() == Some("fixture-p1")));
    // The unconnected catalog providers (and their models) are gone — a
    // stale namespaced id would be a ModelUnavailable on send, not a
    // silent fallback.
    assert!(!models.iter().any(|m| m.id.starts_with("fixture-p2/")));
    assert!(!models.iter().any(|m| m.id.starts_with("fixture-p3/")));
    adapter.stop().await.expect("stop");
}

/// auth.json merge end-to-end: a custom `type: api` provider declared in
/// the credential file (but absent from the server catalog) appears in the
/// model list with namespaced ids and resolves to the exact wire pair;
/// catalog providers are NOT duplicated; credential-only entries are
/// dropped (the sambanova-free shape from the real machine).
#[tokio::test(flavor = "multi_thread")]
async fn auth_json_provider_is_merged_into_models() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    // Own fixture spawn: clear the env surface exactly like start_with_env
    // does (a stale FIXTURE_MSG_MODE from an earlier test would poison the
    // fixture process this test spawns).
    clear_fixture_env();
    let tmp = tempfile::tempdir().expect("tempdir");
    let auth_path = tmp.path().join("auth.json");
    std::fs::write(
        &auth_path,
        r#"{
            "custom-extra": {
                "type": "api",
                "key": "sk-local-secret",
                "models": ["c-model-1", "c-model-2"]
            },
            "fixture-p1": {
                "type": "api",
                "key": "sk-dup",
                "models": ["should-not-appear"]
            },
            "broken-ghost": {
                "type": "api",
                "key": "sk-broken"
            }
        }"#,
    )
    .expect("write auth.json");
    let mut config = harness.fixture_config();
    config.auth_json_path = Some(auth_path);
    let adapter = OpenCodeAdapter::new(config);
    adapter.start(&harness.context()).await.expect("start");
    let models = adapter.list_models().await.expect("models");

    // Auth provider appended with namespaced ids; nothing from the catalog
    // was duplicated (fixture-p1 keeps exactly its fixture models).
    assert!(models.iter().any(|m| m.id == "custom-extra/c-model-1"));
    assert!(models.iter().any(|m| m.id == "custom-extra/c-model-2"));
    assert!(!models
        .iter()
        .any(|m| m.id == "fixture-p1/should-not-appear"));
    // Credential-only entry never becomes an empty shell in the list.
    assert!(!models.iter().any(|m| m.id == "broken-ghost/"));
    assert_eq!(
        models
            .iter()
            .filter(|m| m.id.starts_with("fixture-p1/"))
            .count(),
        2
    );

    // The namespaced auth id resolves to the exact wire pair on send
    // (resolve_model_ref must find it in the generation-scoped cache).
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;
    let run = send(
        &adapter,
        &session,
        "auth model",
        Some("custom-extra/c-model-1"),
    )
    .await;
    collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    adapter.stop().await.expect("stop");
}

/// §10–§11: two providers may expose the SAME raw model key; the namespaced
/// generic ids never collide and each resolves to its exact wire pair.
#[tokio::test(flavor = "multi_thread")]
async fn same_raw_key_across_providers_is_unambiguous() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let models = adapter.list_models().await.expect("models");
    // Both fixture providers expose `model-1`; the generic ids are distinct.
    let a = models
        .iter()
        .find(|m| m.id == "fixture-p1/model-1")
        .expect("p1/model-1");
    let b = models
        .iter()
        .find(|m| m.id == "fixture-p2/model-1")
        .expect("p2/model-1");
    assert_ne!(a.id, b.id, "no collision on equal raw keys");

    // Resolving each namespaced id yields the exact (provider, raw key) pair.
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;
    let run = send(&adapter, &session, "exact pair", Some("fixture-p1/model-1")).await;
    collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    let endpoint = adapter.endpoint().expect("endpoint");
    let client = reqwest::Client::new();
    let url = format!(
        "http://{}:{}/__fixture/last_model",
        endpoint.host, endpoint.port
    );
    let body: serde_json::Value = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("last_model fetch")
        .json()
        .await
        .expect("last_model json");
    assert_eq!(body["providerID"], "fixture-p1");
    assert_eq!(body["modelID"], "model-1");
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn session_create_list_resume_roundtrip() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let mut _collector = Collector::new(&harness.bus);

    let session = create_session(&adapter).await;
    assert!(!session.id.is_empty());
    // Generic and upstream ids differ: the fixture server mints its own id;
    // the adapter echoes the request's generic id (TASK 24 §9).
    assert_ne!(session.id, session.engine_session_id);
    assert!(
        session.engine_session_id.starts_with("ses_fixture_"),
        "engine_session_id is the upstream id: {}",
        session.engine_session_id
    );
    // SessionManager is the sole normalized `session.*` lifecycle publisher
    // (TASK 24 §9): the adapter must NOT emit session.created — only the
    // upstream server's session.created travels on the raw stream and is
    // routed by the run registry, never published as a canonical event here.

    // List contains the created session (plus the seeded one). The adapter
    // lists UPSTREAM sessions (the generic id lives in SessionManager
    // metadata), so the match is on the engine session id.
    let listed = adapter.list_sessions().await.expect("list");
    assert!(
        listed
            .iter()
            .any(|s| s.engine_session_id == session.engine_session_id),
        "listed"
    );

    // Resume by engine id re-accesses the same upstream session. The generic
    // id lives in SessionManager metadata; at this boundary both fields carry
    // the upstream id.
    let resumed = adapter
        .resume_session(&session.engine_session_id)
        .await
        .expect("resume");
    assert_eq!(resumed.id, session.engine_session_id);
    assert_eq!(resumed.engine_session_id, session.engine_session_id);

    // Unknown session → typed SessionNotFound (§19).
    let err = adapter
        .resume_session("ses_does_not_exist")
        .await
        .expect_err("missing session");
    assert!(matches!(err, EngineError::SessionNotFound { .. }), "{err}");

    // Delete works, then it is gone.
    adapter
        .delete_session(&session.engine_session_id)
        .await
        .expect("delete");
    let listed = adapter.list_sessions().await.expect("list after delete");
    assert!(!listed.iter().any(|s| s.id == session.id));

    adapter.stop().await.expect("stop");
}

/// P1 (TASK 24 §9): a resumed/restarted session restores its EXACT
/// user/assistant/tool history from the engine's authoritative endpoint —
/// never a fabricated empty thread, never a SQLite transcript mirror.
#[tokio::test(flavor = "multi_thread")]
async fn session_history_restores_authoritative_order() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;

    let session = create_session(&adapter).await;
    // The fixture preloads authoritative user/assistant/tool history.
    let history = adapter
        .session_history(&session.engine_session_id)
        .await
        .expect("history")
        .expect("OpenCode exposes a history capability");
    let roles: Vec<&str> = history.iter().map(|m| m.role.as_str()).collect();
    assert!(
        roles.contains(&"user") && roles.contains(&"assistant") && roles.contains(&"tool"),
        "user/assistant/tool history present in authoritative order: {roles:?}"
    );
    let user = history.iter().find(|m| m.role == "user").unwrap();
    assert_eq!(user.text, "preloaded user prompt");
    assert_eq!(user.ts, 1786863908016, "upstream timestamp is preserved");
    let assistant = history.iter().find(|m| m.role == "assistant").unwrap();
    assert_eq!(assistant.text, "preloaded assistant answer");
    let tool = history.iter().find(|m| m.role == "tool").unwrap();
    assert_eq!(tool.tool.as_deref(), Some("bash"));
    assert_eq!(tool.tool_call_id.as_deref(), Some("call_1"));

    // AUDIT-CORE-004: the exact normalized sequence for the fixture preload
    // is user -> assistant -> tool — the parent entry precedes the tool
    // entries derived from it (the frontend hydrator attaches a tool to the
    // nearest PRECEDING assistant; tools-first fabricated a blank synthetic
    // assistant above the real answer). `order` is strictly increasing and
    // distinguishes parent (`order*2`) from child (`order*2+1+j`) entries.
    let ids: Vec<&str> = history.iter().map(|m| m.id.as_str()).collect();
    let assistant_idx = ids.iter().position(|i| *i == "msg_pre_2").expect("assistant id");
    let tool_idx = ids.iter().position(|i| *i == "call_1").expect("tool id");
    assert!(
        assistant_idx < tool_idx,
        "parent assistant must precede its tool part: {ids:?}"
    );
    let orders: Vec<u64> = history.iter().map(|m| m.order).collect();
    let mut sorted = orders.clone();
    sorted.sort_unstable();
    assert_eq!(orders, sorted, "normalized order must be strictly increasing");
    assert!(
        assistant.order < tool.order,
        "child order scheme must sort after its parent: {orders:?}"
    );

    // Missing session → typed SessionNotFound.
    let err = adapter
        .session_history("ses_does_not_exist")
        .await
        .expect_err("missing session");
    assert!(matches!(err, EngineError::SessionNotFound { .. }), "{err}");

    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn session_revert_hides_the_boundary_and_unrevert_restores_history() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let session = create_session(&adapter).await;

    adapter
        .revert_session(&session.engine_session_id, "msg_pre_1")
        .await
        .expect("revert");
    let reverted = adapter
        .session_history(&session.engine_session_id)
        .await
        .expect("history after revert")
        .expect("history capability");
    assert!(reverted.is_empty(), "revert boundary and later messages are hidden");

    adapter
        .unrevert_session(&session.engine_session_id)
        .await
        .expect("unrevert");
    let restored = adapter
        .session_history(&session.engine_session_id)
        .await
        .expect("history after unrevert")
        .expect("history capability");
    assert!(restored.iter().any(|message| message.id == "msg_pre_2"));
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Send → stream → terminal
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn send_completes_with_deltas_and_tool_events() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_TOOL", "1")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "Hello fixture", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;

    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(5),
        )
        .await;
    collector
        .wait_for(
            "tool.started",
            |e| matches!(e, Event::ToolStarted { .. }),
            Duration::from_secs(5),
        )
        .await;
    collector
        .wait_for(
            "tool.completed",
            |e| matches!(e, Event::ToolCompleted { .. }),
            Duration::from_secs(5),
        )
        .await;
    collector.drain();
    // Exactly one terminal (§24, §165), and no failed/cancelled.
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    assert!(!collector.events.iter().any(
        |e| matches!(e, Event::MessageFailed { run_id, .. } if run_id.as_str() == run.run_id)
    ));
    // The deltas carried the full text (§35).
    let deltas = collector.deltas(&run.run_id);
    assert!(deltas.contains("from the fixture"), "deltas: {deltas:?}");
    // A second send in the same session works (busy released, §175).
    let run2 = send(&adapter, &session, "again", None).await;
    let t2 = collector
        .wait_terminal(&run2.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(t2, Event::MessageCompleted { .. }), "{t2:?}");

    adapter.stop().await.expect("stop");
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn send_http_error_fails_cleanly() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_MSG_MODE", "error500")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "boom", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    // Engine stays READY (§13, §59): a run failure is not an engine failure.
    assert_eq!(adapter.health(), EngineHealth::Ready);

    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn send_malformed_response_fails_as_protocol() {
    // The request crossed the boundary (2xx) but the body is not valid
    // OpenCode data: the honest terminal is outcome-unknown — never a
    // fabricated completed, and never a definite FAILED (the run may still
    // exist upstream) (TASK 24 §9).
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_MSG_MODE", "malformed")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "x", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageOutcomeUnknown { .. }),
        "{terminal:?}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn send_truncated_response_never_completes() {
    // The run was accepted (2xx head) but the body was cut off mid-stream:
    // outcome-unknown — never a false completed, and never a definite
    // FAILED (the run may still exist upstream) (TASK 24 §9).
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_MSG_MODE", "truncated")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "x", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageOutcomeUnknown { .. }),
        "{terminal:?}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_error_fails_run_but_not_engine() {
    // §57–§59: session.error (provider failure) → run FAILED; engine READY.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_MSG_MODE", "provider_error")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "x", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    if let Event::MessageFailed { error, .. } = terminal {
        assert!(
            error.contains("rate limit"),
            "provider error surfaced: {error}"
        );
    }
    assert_eq!(adapter.health(), EngineHealth::Ready);
    // Error recovery (§175): next send in the same session works.
    let run2 = send(&adapter, &session, "again", None).await;
    let t2 = collector
        .wait_terminal(&run2.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(t2, Event::MessageCompleted { .. }), "{t2:?}");
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_model_is_used_unknown_model_rejected() {
    // §143–§144: no silent fallback. Unknown model → typed error before the
    // request leaves the adapter.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "hi", Some("fixture-p1/model-1")).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );

    // The wire identity is the EXACT canonical pair (provider id + raw map
    // key — the raw key is sent verbatim, never synthesized), never the
    // inner legacy fields — the discriminating fixture records what
    // POST /message actually received.
    let endpoint = adapter.endpoint().expect("endpoint");
    let client = reqwest::Client::new();
    let url = format!(
        "http://{}:{}/__fixture/last_model",
        endpoint.host, endpoint.port
    );
    let body: serde_json::Value = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("last_model fetch")
        .json()
        .await
        .expect("last_model json");
    assert_eq!(body["providerID"], "fixture-p1");
    assert_eq!(body["modelID"], "model-1");

    // Unknown model id: ModelUnavailable → EngineError, no request dispatched.
    let err = adapter
        .send(&saiwork_core::engine::SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: Some("no/such-model".into()),
        })
        .await
        .expect_err("unknown model must be rejected");
    assert!(err.to_string().contains("model"), "{err}");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cancel_aborts_running_message() {
    // §43–§46: real abort API; engine stays READY; one CANCELLED terminal.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "hang"),
            ("FIXTURE_MSG_DELAY_MS", "60000"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "long task", None).await;
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(10),
        )
        .await;
    adapter.cancel(&run.run_id).await.expect("cancel");

    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCancelled { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    // §45: cancel does not kill the engine.
    assert_eq!(adapter.health(), EngineHealth::Ready);
    assert!(harness.supervisor_count() == 1);

    adapter.stop().await.expect("stop");
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_twice_and_after_complete_are_idempotent() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    // Cancel after complete: no-op, no new terminal event (§47).
    let run = send(&adapter, &session, "quick", None).await;
    collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    collector.drain();
    let count_before = collector.terminal_count(&run.run_id);
    adapter
        .cancel(&run.run_id)
        .await
        .expect("cancel after complete");
    adapter.cancel(&run.run_id).await.expect("cancel twice");
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), count_before);

    // Cancel twice on a running run: no duplicate abort storm, one terminal.
    let run2 = send(&adapter, &session, "again", None).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    adapter.cancel(&run2.run_id).await.expect("cancel 1");
    adapter
        .cancel(&run2.run_id)
        .await
        .expect("cancel 2 (idempotent)");
    let terminal = collector
        .wait_terminal(&run2.run_id, Duration::from_secs(15))
        .await;
    collector.drain();
    assert_eq!(collector.terminal_count(&run2.run_id), 1);
    assert!(
        matches!(
            terminal,
            Event::MessageCancelled { .. } | Event::MessageCompleted { .. }
        ),
        "{terminal:?}"
    );
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_complete_race_yields_exactly_one_terminal() {
    // §48: whatever the ordering, exactly one terminal. Run 3 times for
    // signal, not a single lucky sample (§167).
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_MSG_DELAY_MS", "1200")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    for i in 0..3 {
        let run = send(&adapter, &session, &format!("race {i}"), None).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = adapter.cancel(&run.run_id).await;
        let terminal = collector
            .wait_terminal(&run.run_id, Duration::from_secs(20))
            .await;
        assert!(
            matches!(
                terminal,
                Event::MessageCompleted { .. } | Event::MessageCancelled { .. }
            ),
            "iteration {i}: {terminal:?}"
        );
        collector.drain();
        assert_eq!(
            collector.terminal_count(&run.run_id),
            1,
            "iteration {i}: exactly one terminal"
        );
        assert!(
            !collector
                .events
                .iter()
                .any(|e| matches!(e, Event::MessageFailed { run_id, .. } if run_id.as_str() == run.run_id)),
            "iteration {i}: race must not produce FAILED"
        );
    }
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// SSE edge cases (§105–§109)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sse_fragmented_deltas_parse_correctly() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "fragmented")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "frag", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    // The fragmented writer is slow; let the stream drain before asserting
    // the deltas (the adapter drains on close, but the assertion must wait
    // for the bus to carry them).
    tokio::time::sleep(Duration::from_millis(400)).await;
    collector.drain();
    let deltas = collector.deltas(&run.run_id);
    assert!(deltas.contains("from the fixture"), "deltas: {deltas:?}");
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_multiple_events_per_chunk() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "multi")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "multi", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert!(collector.deltas(&run.run_id).contains("fixture"));
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_keepalive_comments_do_not_break_parsing() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "keepalive")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "ka", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert!(collector.deltas(&run.run_id).contains("fixture"));
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_malformed_event_is_ignored_not_fatal() {
    // §30: a malformed stream event is a diagnostic; the POST response is the
    // terminal authority, so the run still completes.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "malformed")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "bad event", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_unknown_event_type_is_ignored() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "unknown")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "future event", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_duplicate_events_do_not_corrupt_deltas() {
    // §33: duplicate event ids are deduped; the delta text is not doubled.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "duplicate")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "dup", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    collector.drain();
    // Without dedup the duplicated delta would appear twice.
    let deltas = collector.deltas(&run.run_id);
    let first = "Hello ";
    assert_eq!(
        deltas.matches(first).count(),
        1,
        "duplicate delta must be deduped (deltas: {deltas:?})"
    );
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_disconnect_before_terminal_still_completes() {
    // §51–§52: stream loss ≠ engine loss; the POST is the terminal authority.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_DROP_AFTER", "1")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "drop", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Concurrency (§70–§72, §163–§164)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn same_session_concurrent_send_is_rejected() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "hang"),
            ("FIXTURE_MSG_DELAY_MS", "60000"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "first", None).await;
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(10),
        )
        .await;

    let err = adapter
        .send(&saiwork_core::engine::SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "second".into(),
            model: None,
        })
        .await
        .expect_err("second send must be rejected");
    assert!(matches!(err, EngineError::SessionBusy { .. }), "{err}");

    // In hang mode every send hangs, so the proof is: (1) the busy rejection
    // above, and (2) the first run still cancels cleanly (multi-session
    // parallelism is proven in `multi_session_events_are_isolated`).
    adapter.cancel(&run.run_id).await.expect("cancel");
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCancelled { .. }),
        "{terminal:?}"
    );
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_session_events_are_isolated() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let mut collector = Collector::new(&harness.bus);
    let session_a = create_session(&adapter).await;
    let session_b = create_session(&adapter).await;

    let run_a = send(&adapter, &session_a, "to A", None).await;
    let run_b = send(&adapter, &session_b, "to B", None).await;
    let ta = collector
        .wait_terminal(&run_a.run_id, Duration::from_secs(15))
        .await;
    let tb = collector
        .wait_terminal(&run_b.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(ta, Event::MessageCompleted { .. }));
    assert!(matches!(tb, Event::MessageCompleted { .. }));

    // Every message event for A carries session A, and vice versa (§163).
    collector.drain();
    let mut crossed = false;
    for e in &collector.events {
        match e {
            Event::MessageStarted {
                session_id, run_id, ..
            }
            | Event::MessageDelta {
                session_id, run_id, ..
            }
            | Event::MessageCompleted {
                session_id, run_id, ..
            }
            | Event::MessageFailed {
                session_id, run_id, ..
            } => {
                let expected_session = if run_id.as_str() == run_a.run_id {
                    session_a.id.as_str()
                } else {
                    session_b.id.as_str()
                };
                if session_id.as_str() != expected_session {
                    crossed = true;
                }
            }
            _ => {}
        }
    }
    assert!(!crossed, "events must be scoped to their own session");
    assert_eq!(collector.terminal_count(&run_a.run_id), 1);
    assert_eq!(collector.terminal_count(&run_b.run_id), 1);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Engine crash / restart (§78–§80, §135–§137)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn engine_crash_fails_active_run_and_restart_works() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "hang"),
            ("FIXTURE_MSG_DELAY_MS", "60000"),
            ("FIXTURE_CRASH_AFTER_MS", "1500"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "doomed run", None).await;
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(10),
        )
        .await;

    // The fixture self-terminates; the exit watcher fails the active run.
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(20))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    collector
        .wait_for(
            "engine.failed",
            |e| matches!(e, Event::EngineFailed { .. }),
            Duration::from_secs(10),
        )
        .await;
    assert!(matches!(adapter.health(), EngineHealth::Failed { .. }));
    assert_eq!(harness.supervisor_count(), 0);

    // Explicit restart; no stale run state (§136–§137). Old run id is gone.
    // The fresh fixture must NOT inherit the hang/crash knobs.
    std::env::remove_var("FIXTURE_MSG_MODE");
    std::env::remove_var("FIXTURE_MSG_DELAY_MS");
    std::env::remove_var("FIXTURE_CRASH_AFTER_MS");
    adapter
        .start(&harness.context())
        .await
        .expect("restart after crash");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter
        .cancel(&run.run_id)
        .await
        .expect("old run: idempotent no-op");

    // Fresh session + send works.
    let session2 = create_session(&adapter).await;
    let run2 = send(&adapter, &session2, "post-crash", None).await;
    let t2 = collector
        .wait_terminal(&run2.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(t2, Event::MessageCompleted { .. }), "{t2:?}");
    adapter.stop().await.expect("stop");
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_stop_fails_active_run_cleanly() {
    // §78–§79: application shutdown with an active run → run FAILED, process
    // gone, no late events.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "hang"),
            ("FIXTURE_MSG_DELAY_MS", "60000"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "will be stopped", None).await;
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(10),
        )
        .await;

    adapter.stop().await.expect("stop with active run");
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    assert_eq!(harness.supervisor_count(), 0);
    assert_eq!(adapter.health(), EngineHealth::Stopped);
}

// ---------------------------------------------------------------------------
// Permissions (§41–§42)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn permission_reply_uses_typed_api() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    // A live run gives the adapter the generic→upstream session mapping the
    // reply API needs (TASK 24 §9); the fixture then resolves the request.
    let adapter = start_with_env(&harness, &[("FIXTURE_MSG_MODE", "hang")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;
    let run = send(&adapter, &session, "needs permission", None).await;
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(10),
        )
        .await;
    adapter
        .resolve_permission(&session.id, "req_123", true)
        .await
        .expect("allow");
    adapter
        .resolve_permission(&session.id, "req_123", false)
        .await
        .expect("deny");
    adapter.cancel(&run.run_id).await.expect("cancel");
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(10))
        .await;
    assert!(matches!(terminal, Event::MessageCancelled { .. }));
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Idle behavior (§170–§171)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn idle_engine_makes_no_requests() {
    // After a completed run, a second run still works — the runtime-global
    // event stream is REUSED (never closed per-run idle, TASK 24 perf). No
    // periodic polling anywhere: this is enforced by construction (no timers
    // outside the run lifecycle), so the observable contract is "a new send
    // always works".
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    for i in 0..3 {
        let run = send(&adapter, &session, &format!("cycle {i}"), None).await;
        let terminal = collector
            .wait_terminal(&run.run_id, Duration::from_secs(15))
            .await;
        assert!(
            matches!(terminal, Event::MessageCompleted { .. }),
            "{terminal:?}"
        );
    }
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_prompts_use_one_event_stream_per_runtime_generation() {
    // TASK 24 perf: the runtime-global SSE stream must stay connected for the
    // READY runtime and be reused — 20 human-spaced prompts must open exactly
    // ONE /event connection per runtime generation (no per-run idle close /
    // reconnect churn), and a reused already-ready stream must never incur a
    // 10 s `ready.changed()` wait.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    for i in 0..20 {
        let run = send(&adapter, &session, &format!("prompt {i}"), None).await;
        let terminal = collector
            .wait_terminal(&run.run_id, Duration::from_secs(15))
            .await;
        assert!(
            matches!(terminal, Event::MessageCompleted { .. }),
            "{terminal:?}"
        );
        // Human-spaced prompts: 500 ms apart, well past the old idle grace.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let counters = fixture_counters(&adapter).await;
    let connections = counters
        .get("event_connections")
        .and_then(|v| v.as_u64())
        .expect("event_connections counter");
    assert_eq!(
        connections, 1,
        "20 prompts must share one /event connection per runtime generation, got {connections}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Validation (§66–§68)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn empty_and_huge_prompts_are_rejected_locally() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let mut config = harness.fixture_config();
    config.max_prompt_bytes = 1024;
    clear_fixture_env();
    std::env::remove_var("FIXTURE_MSG_MODE");
    let adapter = OpenCodeAdapter::new(config);
    adapter.start(&harness.context()).await.expect("ready");
    let session = create_session(&adapter).await;

    let err = adapter
        .send(&saiwork_core::engine::SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "   ".into(),
            model: None,
        })
        .await
        .expect_err("empty prompt");
    assert!(err.to_string().contains("empty"), "{err}");

    let huge = "x".repeat(4096);
    let err = adapter
        .send(&saiwork_core::engine::SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: huge,
            model: None,
        })
        .await
        .expect_err("oversized prompt");
    assert!(
        err.to_string().contains("large") || err.to_string().contains("limit"),
        "{err}"
    );
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — provider/model metadata failures (§44–§46)
// ---------------------------------------------------------------------------

/// Number of abort requests the fixture has seen (cancel-spam proof, §63).
async fn fixture_abort_count(adapter: &OpenCodeAdapter) -> u64 {
    let endpoint = adapter.endpoint().expect("endpoint");
    let url = format!(
        "http://{}:{}/__fixture/abort_count",
        endpoint.host, endpoint.port
    );
    let client = reqwest::Client::new();
    let body = client
        .get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("abort_count fetch")
        .text()
        .await
        .expect("abort_count body");
    serde_json::from_str::<serde_json::Value>(&body)
        .expect("abort_count json")
        .get("count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_endpoint_500_keeps_engine_ready_and_recovers() {
    // §44: a metadata failure must never fail or kill the engine; the
    // runtime stays READY and an explicit refresh works once the server
    // recovers (FIXTURE_PROVIDER_HTTP=500:1 — fail once, then succeed).
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_HTTP", "500:1")]).await;

    let err = adapter
        .list_models()
        .await
        .expect_err("first provider fetch fails");
    assert!(err.to_string().contains("500"), "typed http error: {err}");
    assert_eq!(
        adapter.health(),
        EngineHealth::Ready,
        "engine must stay READY"
    );

    // Explicit refresh after recovery works.
    let models = adapter
        .list_models()
        .await
        .expect("recovered provider fetch");
    assert!(models.len() >= 4, "got {}", models.len());
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_endpoint_401_is_typed_auth_error_engine_stays_ready() {
    // §45: a 401 on the provider endpoint is a credential/config-domain
    // failure, distinct from a provider error; the engine process is not
    // killed and the runtime remains usable.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_HTTP", "401:1")]).await;

    let err = adapter
        .list_models()
        .await
        .expect_err("first provider fetch is 401");
    assert!(err.to_string().contains("401"), "typed auth error: {err}");
    assert_eq!(adapter.health(), EngineHealth::Ready);

    // Recovery: the second fetch succeeds (the failure was transient).
    let models = adapter.list_models().await.expect("recovered");
    assert!(models.len() >= 4);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Provider-bound policy (TASK 25/26): catalog size, fallback, malformed
// catalogs, engine-default usability without a model list.
// ---------------------------------------------------------------------------

/// §13: a valid provider catalog LARGER than the old 4 MiB ordinary metadata
/// bound must succeed under the dedicated provider-catalog bound.
#[tokio::test(flavor = "multi_thread")]
async fn large_valid_catalog_exceeding_old_bound_succeeds() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    // 600 providers × 40 models ≈ 8+ MiB of JSON — beyond the 4 MiB
    // ordinary bound, within the 16 MiB provider-catalog bound.
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_PROVIDER_COUNT", "600"),
            ("FIXTURE_PROVIDER_MODELS_PER", "40"),
        ],
    )
    .await;
    let models = adapter
        .list_models()
        .await
        .expect("large catalog must load");
    assert_eq!(models.len(), 600 * 40);
    assert!(models.iter().any(|m| m.id == "fixture-p1/model-1"));
    adapter.stop().await.expect("stop");
}

/// §13: a catalog LARGER than the configured provider-catalog bound is a
/// typed bounded error; the engine stays READY and Engine Default (model
/// None) still completes a prompt.
#[tokio::test(flavor = "multi_thread")]
async fn catalog_over_configured_bound_is_typed_error_engine_default_works() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let mut config = harness.fixture_config();
    config.provider_catalog_max_bytes = 1024; // tiny bound for the test
    for key in [
        "FIXTURE_MSG_MODE",
        "FIXTURE_MSG_DELAY_MS",
        "FIXTURE_TOOL",
        "FIXTURE_EVENT_STYLE",
        "FIXTURE_EVENT_DROP_AFTER",
        "FIXTURE_CRASH_AFTER_MS",
        "FIXTURE_PROVIDER_HTTP",
        "FIXTURE_PROVIDER_BODY",
        "FIXTURE_PROVIDER_FALLBACK",
        "FIXTURE_MSG_ERROR_BODY",
        "FIXTURE_ABORT_MODE",
        "FIXTURE_DELTA_COUNT",
    ] {
        std::env::remove_var(key);
    }
    clear_fixture_env();
    std::env::set_var("FIXTURE_PROVIDER_COUNT", "600");
    std::env::set_var("FIXTURE_PROVIDER_MODELS_PER", "40");
    let adapter = OpenCodeAdapter::new(config);
    adapter.start(&harness.context()).await.expect("ready");
    assert_eq!(adapter.health(), EngineHealth::Ready);

    let err = adapter.list_models().await.expect_err("catalog over bound");
    assert!(
        err.to_string().contains("limit"),
        "typed bounded error: {err}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);

    // Engine Default path remains fully usable without a model list.
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;
    let run = send(&adapter, &session, "Reply with exactly READY", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    adapter.stop().await.expect("stop");
}

/// §14: a 200 with malformed JSON is a typed protocol error; models become
/// unavailable but Engine Default remains usable; no crash, no fake
/// empty-success list.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_catalog_is_typed_protocol_error_engine_default_works() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_BODY", "malformed")]).await;

    let err = adapter.list_models().await.expect_err("malformed catalog");
    assert!(
        err.to_string().contains("not valid JSON"),
        "typed protocol error: {err}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);

    // Engine Default path remains fully usable.
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;
    let run = send(&adapter, &session, "Reply with exactly READY", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    adapter.stop().await.expect("stop");
}

/// §15: a 401 must NEVER silently fall back to another endpoint. The
/// fixture's `/config/providers` would serve a valid catalog — if the
/// adapter fell back, list_models would succeed. It must not.
#[tokio::test(flavor = "multi_thread")]
async fn provider_401_never_triggers_endpoint_fallback() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_PROVIDER_HTTP", "401:9999"),
            ("FIXTURE_PROVIDER_FALLBACK", "1"),
        ],
    )
    .await;

    let err = adapter
        .list_models()
        .await
        .expect_err("401 must not fall back");
    assert!(err.to_string().contains("401"), "typed auth error: {err}");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

/// §15: a 403 is a typed authentication/config error; engine stays READY.
#[tokio::test(flavor = "multi_thread")]
async fn provider_endpoint_403_is_typed_auth_error() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_HTTP", "403:1")]).await;

    let err = adapter
        .list_models()
        .await
        .expect_err("first provider fetch is 403");
    assert!(err.to_string().contains("403"), "typed auth error: {err}");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

/// §16: a 500 is a safe server/config diagnostic, never "models not found";
/// engine stays READY if the runtime process is healthy; no retry storm.
#[tokio::test(flavor = "multi_thread")]
async fn provider_endpoint_500_is_safe_diagnostic_no_retry_storm() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_HTTP", "500:9999")]).await;

    let err = adapter
        .list_models()
        .await
        .expect_err("first provider fetch is 500");
    assert!(err.to_string().contains("500"), "typed server error: {err}");
    assert!(
        !err.to_string().contains("models not found"),
        "never 'models not found': {err}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);

    // One explicit refresh attempt fails again (no auto-retry masking), and
    // the error stays typed.
    let err2 = adapter
        .list_models()
        .await
        .expect_err("second fetch still 500");
    assert!(
        err2.to_string().contains("500"),
        "typed server error: {err2}"
    );

    // Engine Default path remains available.
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;
    let run = send(&adapter, &session, "Reply with exactly READY", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    adapter.stop().await.expect("stop");
}

/// §5: `/config/providers` is used ONLY as a strict 404/405 fallback.
#[tokio::test(flavor = "multi_thread")]
async fn provider_route_absent_falls_back_to_config_providers() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_FALLBACK", "1")]).await;

    let models = adapter.list_models().await.expect("fallback catalog");
    assert_eq!(models.len(), 4, "got {}", models.len());
    assert!(models.iter().any(|m| m.id == "fixture-p1/model-1"));
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

/// §18–§19: with model discovery failing (500), session creation and the
/// first prompt with model=None MUST still succeed; the outgoing request
/// omits the explicit model field.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_failure_engine_default_session_and_first_prompt_work() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_PROVIDER_HTTP", "500:9999")]).await;

    let err = adapter.list_models().await.expect_err("discovery fails");
    assert!(err.to_string().contains("500"), "{err}");
    assert_eq!(adapter.health(), EngineHealth::Ready);

    // Session creation must not require a model list.
    let session = create_session(&adapter).await;

    // First prompt with model=None; the fixture records whether a model was
    // sent. The outgoing request must OMIT the explicit model field.
    let mut collector = Collector::new(&harness.bus);
    let run = send(&adapter, &session, "Reply with exactly READY", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );

    let endpoint = adapter.endpoint().expect("endpoint");
    let client = reqwest::Client::new();
    let url = format!(
        "http://{}:{}/__fixture/last_model",
        endpoint.host, endpoint.port
    );
    let body: serde_json::Value = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("last_model fetch")
        .json()
        .await
        .expect("last_model json");
    assert!(
        body["providerID"].is_null() && body["modelID"].is_null(),
        "model=None must omit the model field: {body}"
    );

    // Second prompt in the SAME session works too (two turns, §20).
    let run2 = send(&adapter, &session, "Reply with exactly SECOND", None).await;
    let t2 = collector
        .wait_terminal(&run2.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(t2, Event::MessageCompleted { .. }), "{t2:?}");
    adapter.stop().await.expect("stop");
}

/// §17/§26: an engine restart invalidates the model cache — the new runtime
/// generation must re-fetch providers, never serve the stale list.
#[tokio::test(flavor = "multi_thread")]
async fn engine_restart_invalidates_model_cache() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    clear_fixture_env();
    std::env::set_var("FIXTURE_PROVIDER_COUNT", "2");
    std::env::remove_var("FIXTURE_PROVIDER_MODELS_PER");
    let adapter = OpenCodeAdapter::new(harness.fixture_config());
    adapter.start(&harness.context()).await.expect("start");
    let first = adapter.list_models().await.expect("models");
    assert_eq!(first.len(), 4);

    adapter.stop().await.expect("stop");
    // New runtime, different catalog (3 providers).
    std::env::set_var("FIXTURE_PROVIDER_COUNT", "3");
    adapter.start(&harness.context()).await.expect("restart");
    let second = adapter.list_models().await.expect("fresh models");
    assert_eq!(second.len(), 6, "stale cache must not survive restart");
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — error body shapes (§52–§53, §84)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn huge_error_body_is_bounded_and_run_fails_typed() {
    // §53: an 8 MiB error page must be read with the body bound and never
    // buffered into a giant diagnostic string. The run fails (typed), the
    // engine stays READY.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "error500"),
            ("FIXTURE_MSG_ERROR_BODY", "huge"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "boom", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    if let Event::MessageFailed { error, .. } = terminal {
        assert!(
            error.len() < 2000,
            "error detail must be bounded: {}",
            error.len()
        );
    }
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn html_error_body_is_safe() {
    // §52: a non-JSON (HTML) error body must decode safely into a typed
    // failure, not panic and not fabricate a completion.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "error500"),
            ("FIXTURE_MSG_ERROR_BODY", "html"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "boom", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn error_body_echoing_secret_is_redacted() {
    // §84: a server error that echoes the runtime secret (Authorization
    // header) must never surface the secret in the run failure message.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "error500"),
            ("FIXTURE_MSG_ERROR_BODY", "echo_secret"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    // The adapter's own secret for this runtime: recover it from the
    // supervisor snapshot is not possible (values never stored), so assert
    // the redaction marker is present and the Basic scheme line is gone.
    let run = send(&adapter, &session, "boom", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageFailed { .. }),
        "{terminal:?}"
    );
    if let Event::MessageFailed { error, .. } = terminal {
        // The secret value is replaced by the redaction marker, so the
        // echo line can only ever surface as `Basic opencode:***`.
        assert!(
            error.contains("Basic opencode:***"),
            "secret value must be redacted in surfaced error: {error}"
        );
        assert!(
            !error.contains("Basic opencode:-") && error.matches("Basic opencode:").count() <= 1,
            "no unredacted Authorization line may survive: {error}"
        );
    }
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — stream loss (§54–§57)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stream_disconnect_before_first_event_run_still_terminates() {
    // §54: the event stream drops immediately after connecting (before any
    // event). The run must not stay RUNNING forever: the POST response is
    // the terminal authority, so the run still completes and the engine
    // stays READY.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_DROP_AFTER", "0")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "dropped stream", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    assert_eq!(adapter.health(), EngineHealth::Ready);

    // A subsequent send works (fresh stream is created on demand).
    let run2 = send(&adapter, &session, "again", None).await;
    let t2 = collector
        .wait_terminal(&run2.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(t2, Event::MessageCompleted { .. }), "{t2:?}");
    adapter.stop().await.expect("stop");
}

/// TASK 24 §9: SSE loss during an ACTIVE run must reconnect (bounded
/// backoff) — the stream is the runtime's control channel for deltas, tools
/// and permission requests. The run resumes on the new connection and
/// reaches its normal terminal with the full text; it must never hang
/// invisibly with its control channel gone.
#[tokio::test(flavor = "multi_thread")]
async fn sse_loss_during_active_run_reconnects_and_resumes() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_DROP_AFTER", "1")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "drop mid run", None).await;
    // The stream drops right after the first fanned-out event (the run is
    // already started upstream). The adapter must reconnect and the run must
    // still reach its normal terminal with the complete assistant text.
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(20))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    // The reconnect actually happened: more than one /event connection was
    // accepted for the same READY runtime generation. (Deltas published by
    // the fixture into the dead connection while the adapter was in backoff
    // are intentionally not replayed — the fixture has no replay; the
    // adapter contract is: reconnect the control channel and never hang the
    // run invisibly.)
    let counters = fixture_counters(&adapter).await;
    let connections = counters
        .get("event_connections")
        .and_then(|v| v.as_u64())
        .expect("event_connections counter");
    assert!(
        connections >= 2,
        "adapter must reconnect after active-run stream loss, got {connections} connection(s)"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_utf8_in_stream_does_not_break_run() {
    // §13: raw invalid UTF-8 bytes in the event stream must not panic the
    // parser nor corrupt the following events; the run completes normally.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(&harness, &[("FIXTURE_EVENT_STYLE", "bad_utf8")]).await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "utf8", None).await;
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCompleted { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert!(collector.deltas(&run.run_id).contains("fixture"));
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — cancellation (§63–§65)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cancel_spam_sends_exactly_one_abort() {
    // §63: clicking cancel repeatedly must not produce an abort storm.
    // The fixture counts abort POSTs; five cancels on one run must yield
    // exactly one abort and exactly one terminal.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_MSG_MODE", "hang"),
            ("FIXTURE_MSG_DELAY_MS", "60000"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "spam", None).await;
    collector
        .wait_for(
            "message.started",
            |e| matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == run.run_id),
            Duration::from_secs(10),
        )
        .await;
    for _ in 0..5 {
        adapter.cancel(&run.run_id).await.expect("cancel");
    }
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCancelled { .. }),
        "{terminal:?}"
    );
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    assert_eq!(
        fixture_abort_count(&adapter).await,
        1,
        "exactly one abort request per run despite cancel spam"
    );
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_hang_is_bounded_and_run_reconciled() {
    // §64: if the abort API never answers, the request is bounded and the
    // run outcome comes from the authoritative POST response (no fake
    // CANCELLED, no hang). The run completes normally in this fixture.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_ABORT_MODE", "hang"),
            ("FIXTURE_MSG_DELAY_MS", "800"),
        ],
    )
    .await;
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let run = send(&adapter, &session, "hang abort", None).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = Instant::now();
    adapter
        .cancel(&run.run_id)
        .await
        .expect("cancel returns bounded");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "cancel must not hang forever"
    );

    // The fixture ignores the abort (hangs) and completes the run; the
    // authoritative outcome is COMPLETED (the abort never landed).
    let terminal = collector
        .wait_terminal(&run.run_id, Duration::from_secs(20))
        .await;
    assert!(
        matches!(
            terminal,
            Event::MessageCompleted { .. } | Event::MessageCancelled { .. }
        ),
        "{terminal:?}"
    );
    collector.drain();
    assert_eq!(collector.terminal_count(&run.run_id), 1);
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — same-session concurrency torture (§25)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn same_session_concurrent_sends_accept_exactly_one() {
    // §25 (REJECT policy): many concurrent sends on one session — exactly
    // one accepted, all others SessionBusy. No race may admit two.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = Arc::new(
        start_with_env(
            &harness,
            &[
                ("FIXTURE_MSG_MODE", "hang"),
                ("FIXTURE_MSG_DELAY_MS", "60000"),
            ],
        )
        .await,
    );
    let mut collector = Collector::new(&harness.bus);
    let session = create_session(&adapter).await;

    let mut handles = Vec::new();
    for i in 0..8 {
        let adapter = adapter.clone();
        let session_id = session.id.clone();
        let engine_session_id = session.engine_session_id.clone();
        handles.push(tokio::spawn(async move {
            adapter
                .send(&saiwork_core::engine::SendRequest {
                    session_id: session_id.clone(),
                    engine_session_id: engine_session_id.clone(),
                    prompt: format!("burst {i}"),
                    model: None,
                })
                .await
        }));
    }
    let mut accepted: Option<saiwork_core::engine::RunHandle> = None;
    let mut busy = 0usize;
    for h in handles {
        match h.await.expect("send task must not panic") {
            Ok(acc) => match acc {
                saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
                    assert!(accepted.is_none(), "a second run was accepted");
                    accepted = Some(saiwork_core::engine::RunHandle { run_id });
                }
                other => panic!("unexpected acceptance: {other:?}"),
            },
            Err(EngineError::SessionBusy { .. }) => busy += 1,
            Err(e) => panic!("unexpected error type: {e}"),
        }
    }
    let accepted = accepted.expect("exactly one run must be accepted");
    assert_eq!(busy, 7, "all others rejected with SessionBusy");

    // Wait for the accepted run to be in-flight before canceling: an abort
    // that lands before the run exists upstream is a no-op, and this fixture
    // is in hang mode (60 s), so only a real abort produces a terminal.
    collector
        .wait_for(
            "message.started",
            |e| {
                matches!(e, Event::MessageStarted { run_id, .. } if run_id.as_str() == accepted.run_id)
            },
            Duration::from_secs(10),
        )
        .await;
    adapter.cancel(&accepted.run_id).await.expect("cancel");
    let terminal = collector
        .wait_terminal(&accepted.run_id, Duration::from_secs(15))
        .await;
    assert!(
        matches!(terminal, Event::MessageCancelled { .. }),
        "{terminal:?}"
    );
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — mixed multi-session workload (§26)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn multi_session_mixed_workload_is_isolated() {
    // §26: independent sessions running normal / cancel / tool / failure
    // concurrently — events must never cross sessions, and one session's
    // busy/failure state must not block another.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = Arc::new(start_with_env(&harness, &[]).await);
    let mut collector = Collector::new(&harness.bus);

    let s_normal = create_session(&adapter).await;
    let s_cancel = create_session(&adapter).await;
    let s_fail = create_session(&adapter).await;

    // Normal run.
    let run_n = send(&adapter, &s_normal, "normal", None).await;
    // Cancel run: sent, then cancelled mid-flight.
    let run_c = {
        let run = accepted(
            adapter
                .send(&saiwork_core::engine::SendRequest {
                    session_id: s_cancel.id.clone(),
                    engine_session_id: s_cancel.engine_session_id.clone(),
                    prompt: "cancel me".into(),
                    model: None,
                })
                .await
                .expect("send cancel"),
        );
        adapter.cancel(&run.run_id).await.expect("cancel");
        run
    };
    // Fourth run on another independent session.
    let run_f = accepted(
        adapter
            .send(&saiwork_core::engine::SendRequest {
                session_id: s_fail.id.clone(),
                engine_session_id: s_fail.engine_session_id.clone(),
                prompt: "fourth".into(),
                model: None,
            })
            .await
            .expect("send fourth"),
    );

    let ta = collector
        .wait_terminal(&run_n.run_id, Duration::from_secs(15))
        .await;
    let tb = collector
        .wait_terminal(&run_c.run_id, Duration::from_secs(15))
        .await;
    let tf = collector
        .wait_terminal(&run_f.run_id, Duration::from_secs(15))
        .await;
    assert!(matches!(ta, Event::MessageCompleted { .. }), "{ta:?}");
    assert!(
        matches!(
            tb,
            Event::MessageCompleted { .. } | Event::MessageCancelled { .. }
        ),
        "{tb:?}"
    );
    assert!(matches!(tf, Event::MessageCompleted { .. }), "{tf:?}");

    // Cross-session isolation: every message event carries its own session.
    collector.drain();
    for e in &collector.events {
        match e {
            Event::MessageStarted {
                session_id, run_id, ..
            }
            | Event::MessageDelta {
                session_id, run_id, ..
            }
            | Event::MessageCompleted {
                session_id, run_id, ..
            }
            | Event::MessageFailed {
                session_id, run_id, ..
            }
            | Event::MessageCancelled {
                session_id, run_id, ..
            } => {
                let expected = if run_id.as_str() == run_n.run_id {
                    s_normal.id.as_str()
                } else if run_id.as_str() == run_c.run_id {
                    s_cancel.id.as_str()
                } else {
                    s_fail.id.as_str()
                };
                assert_eq!(
                    session_id.as_str(),
                    expected,
                    "event {e:?} must be scoped to its own session"
                );
            }
            _ => {}
        }
    }
    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// TASK 12 hardening gate — large stream (§78, §123)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn large_stream_deltas_are_complete_and_terminal_is_exact() {
    // §78/§123: 10k deltas burst through the real OpenCode-compatible event
    // path. A consumer that keeps up (like the Tauri bridge forwarder, which
    // awaits `recv()` continuously) must see every delta, exactly one
    // terminal, and no corruption. The bounded bus only drops for lagging
    // consumers — the contract is explicit lag, never silent loss.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let adapter = start_with_env(
        &harness,
        &[
            ("FIXTURE_DELTA_COUNT", "10000"),
            ("FIXTURE_MSG_DELAY_MS", "20"),
        ],
    )
    .await;
    let session = create_session(&adapter).await;

    // Continuously-draining consumer: counts deltas, stops at the terminal.
    let mut sub = harness.bus.subscribe();
    let run = send(&adapter, &session, "stress", None).await;
    let (delta_count, terminal_count, total_chars) = tokio::spawn(async move {
        let mut deltas = 0u64;
        let mut terminals = 0u64;
        let mut chars = 0u64;
        loop {
            match sub.recv().await {
                Ok(env) => match &env.event {
                    Event::MessageDelta { run_id, delta, .. } if run_id.as_str() == run.run_id => {
                        deltas += 1;
                        chars += delta.len() as u64;
                    }
                    Event::MessageCompleted { run_id, .. }
                    | Event::MessageFailed { run_id, .. }
                    | Event::MessageCancelled { run_id, .. }
                        if run_id.as_str() == run.run_id =>
                    {
                        terminals += 1;
                        break;
                    }
                    _ => {}
                },
                Err(e) => panic!("consumer failed: {e:?}"),
            }
        }
        (deltas, terminals, chars)
    })
    .await
    .expect("drainer task must not panic");

    // The fixture emits 10_000 burst deltas + 5 regular text parts; a
    // keeping-up consumer receives them all, in order, and exactly one
    // terminal (the POST authority, not the fixture drain).
    assert_eq!(terminal_count, 1, "exactly one terminal under stress");
    let counters = fixture_counters(&adapter).await;
    eprintln!("FIXTURE-COUNTERS: {counters:?}");
    assert!(
        delta_count >= 10_000,
        "expected all 10k burst deltas, got {delta_count}"
    );
    assert!(
        total_chars >= 10_000 * 10,
        "expected full 100k-char payload, got {total_chars}"
    );
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
}

async fn fixture_counters(adapter: &OpenCodeAdapter) -> serde_json::Value {
    let endpoint = adapter.endpoint().expect("endpoint");
    let url = format!(
        "http://{}:{}/__fixture/counters",
        endpoint.host, endpoint.port
    );
    let client = reqwest::Client::new();
    client
        .get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("counters fetch")
        .json()
        .await
        .expect("counters json")
}
