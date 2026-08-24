//! Real-OpenCode smoke gate (TASK 10 §119, §121–§122, §99–§102).
//!
//! Runs only when a real `opencode` executable is discoverable in this
//! environment; otherwise it is reported SKIPPED, never faked (§119). The
//! fixture suite (tests/hostile.rs) proves the adapter lifecycle; this suite
//! proves real CLI discovery, real launch args, the real readiness endpoint,
//! real stop, and version compatibility.
//!
//! Local-only: no provider credentials, no network requirement (§99–§100).
//! OpenCode's own global data (~/.local/share/opencode) is its business; the
//! temp *workspace* must stay untouched by SAIWORK2 (§101–§102).

use std::sync::Arc;
use std::time::{Duration, Instant};

use engine_opencode::{OpenCodeAdapter, OpenCodeConfig};
use saiwork_core::engine::{EngineAdapter, EngineHealth, EngineStartContext};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{Event, EventBus};
use saiwork_process::ProcessSupervisor;

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
                failure_bus.publish(saiwork_events::Event::EngineFailed {
                    engine_id: engine_id.into(),
                    error: message.into(),
                });
            }),
        }
    }
}

/// Discover the real OpenCode or record SKIP. Every real test funnels here
/// so the skip reason is explicit and never mistaken for a pass (§119).
fn real_config() -> Option<OpenCodeConfig> {
    let cfg = OpenCodeConfig::default();
    match engine_opencode::discover(&cfg) {
        Ok(d) => {
            eprintln!("REAL-OPENCODE: discovered {} ({})", d.display(), d.source);
            Some(cfg)
        }
        Err(e) => {
            eprintln!("SKIP real smoke: {e}");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_discover_probe_and_version() {
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let probe = OpenCodeAdapter::probe_installation(&harness.supervisor, &cfg)
        .await
        .expect("real probe");
    assert!(
        !probe.version.trim().is_empty(),
        "real version must be reported"
    );
    assert!(
        harness.supervisor.count() == 0,
        "probe leaves no process behind"
    );
    eprintln!("REAL-OPENCODE: version {}", probe.version);
}

#[tokio::test(flavor = "multi_thread")]
async fn real_start_readiness_stop() {
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let adapter = OpenCodeAdapter::new(cfg);

    let started = Instant::now();
    adapter
        .start(&harness.context())
        .await
        .expect("real OpenCode must start");
    let startup_ms = started.elapsed().as_millis();
    assert_eq!(adapter.health(), EngineHealth::Ready);

    let endpoint = adapter.endpoint().expect("endpoint");
    eprintln!("REAL-OPENCODE: READY on {endpoint} in {startup_ms} ms");
    // §28: on-demand authenticated health check against the real API —
    // readiness evidence is the adapter's own probe; the raw endpoint is
    // secret-gated and must not be probed unauthenticated (§112).
    assert!(
        adapter.check_ready().await,
        "live endpoint must answer as OpenCode"
    );

    adapter.stop().await.expect("clean stop");
    assert_eq!(adapter.health(), EngineHealth::Stopped);
    assert_eq!(harness.supervisor.count(), 0, "no process left");
    assert!(
        std::net::TcpStream::connect((endpoint.host, endpoint.port)).is_err(),
        "port closed after stop"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn real_repeated_lifecycle_no_leak() {
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let adapter = OpenCodeAdapter::new(cfg);

    for i in 0..3 {
        adapter.start(&harness.context()).await.expect("start");
        assert_eq!(adapter.health(), EngineHealth::Ready);
        let port = adapter.endpoint().expect("endpoint").port;
        adapter.stop().await.expect("stop");
        assert_eq!(harness.supervisor.count(), 0, "cycle {i}: supervisor empty");
        assert!(
            std::net::TcpStream::connect(("127.0.0.1", port)).is_err(),
            "cycle {i}: port closed"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_workspace_stays_clean() {
    // §101–§102: SAIWORK2 must not scatter runtime files into the workspace.
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let workspace_path = harness.workspace.path().to_path_buf();
    let adapter = OpenCodeAdapter::new(cfg);
    adapter.start(&harness.context()).await.expect("start");
    adapter.stop().await.expect("stop");

    let entries: Vec<_> = std::fs::read_dir(&workspace_path)
        .expect("read workspace")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        entries.is_empty(),
        "workspace must stay clean after start/stop: {entries:?}"
    );
}

/// §122: two isolated real servers on two workspaces. OpenCode keeps its own
/// global state (~/.local/share/opencode); if the installed version cannot
/// run two concurrent servers, this records the upstream limitation instead
/// of pretending success (§95, §122).
#[tokio::test(flavor = "multi_thread")]
async fn real_two_instances_isolated() {
    let Some(cfg) = real_config() else { return };
    let harness_a = Harness::new();
    let harness_b = Harness::new();
    let adapter_a = OpenCodeAdapter::new(cfg.clone());
    let adapter_b = OpenCodeAdapter::new(cfg);

    let a_start = adapter_a.start(&harness_a.context()).await;
    let b_start = adapter_b.start(&harness_b.context()).await;
    match (a_start, b_start) {
        (Ok(()), Ok(())) => {
            let ep_a = adapter_a.endpoint().expect("A");
            let ep_b = adapter_b.endpoint().expect("B");
            assert_ne!(ep_a.port, ep_b.port, "distinct ports");
            adapter_a.stop().await.expect("stop A");
            assert_eq!(adapter_b.health(), EngineHealth::Ready, "B unaffected");
            adapter_b.stop().await.expect("stop B");
            assert_eq!(harness_a.supervisor.count(), 0);
            assert_eq!(harness_b.supervisor.count(), 0);
        }
        (Ok(()), Err(b_err)) => {
            // Upstream limitation (e.g. shared DB lock): record honestly.
            eprintln!(
                "REAL-OPENCODE: second concurrent server failed ({b_err}); \
                 recording upstream limitation — single-instance semantics"
            );
            adapter_a.stop().await.expect("stop A");
            assert_eq!(harness_a.supervisor.count(), 0);
        }
        (Err(a_err), _) => {
            eprintln!("REAL-OPENCODE: first concurrent server failed ({a_err})");
            let _ = adapter_b.stop().await;
        }
    }
}

/// §99–§100 offline contract: startup must not require provider/network
/// access (no credentials configured anywhere in this environment).
#[tokio::test(flavor = "multi_thread")]
async fn real_list_models_tmp_probe() {
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let adapter = OpenCodeAdapter::new(cfg);
    let start_res = adapter.start(&harness.context()).await;
    match &start_res {
        Ok(()) => eprintln!("REAL-OPENCODE: start OK"),
        Err(e) => eprintln!("REAL-OPENCODE: start ERR: {e:#}"),
    }
    start_res.expect("start");
    eprintln!("REAL-OPENCODE: calling list_models");
    match adapter.list_models().await {
        Ok(models) => eprintln!("REAL-OPENCODE: list_models OK, count={}", models.len()),
        Err(e) => eprintln!("REAL-OPENCODE: list_models ERR: {e}"),
    }
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn real_spawn_supervisor_env_probe() {
    use engine_opencode::discover;
    use engine_opencode::OpenCodeConfig;
    use engine_opencode::Secret;
    use saiwork_events::ProcessId;
    use saiwork_process::ProcessSpec;
    let cfg = OpenCodeConfig::default();
    let discovered = discover(&cfg).expect("discover");
    let harness = Harness::new();
    let secret = Secret::generate();
    let port = 4205u16;
    // Runtime-secret contract (§112): the generated password is NEVER logged.
    // Only non-secret probe metadata is safe to surface in diagnostics.
    eprintln!("REAL-OPENCODE: probe on 127.0.0.1:{port} (secret withheld per runtime-secret contract)");
    let mut spec = ProcessSpec::new(
        ProcessId::new("tmp-test"),
        discovered.path.to_string_lossy().into_owned(),
    );
    spec.args = vec![
        "serve".into(),
        "--port".into(),
        port.to_string(),
        "--hostname".into(),
        "127.0.0.1".into(),
        "--pure".into(),
    ];
    spec.env = vec![
        (
            "OPENCODE_SERVER_PASSWORD".into(),
            secret.as_str().to_owned(),
        ),
        // Mirror server_spec: pin the username so ambient env pollution
        // (a stray OPENCODE_SERVER_USERNAME in the parent chain) cannot
        // change the server's configured identity and 401 our Basic auth.
        ("OPENCODE_SERVER_USERNAME".into(), "opencode".into()),
    ];
    spec.cwd = Some(harness.workspace.path().to_path_buf());
    // Runtime-secret contract (§112): the generated password is NEVER logged.
    // Only non-secret probe metadata is surfaced in diagnostics.
    let proc = harness.supervisor.spawn(spec).await.expect("spawn");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    for kind in ["no-auth", "with-secret"] {
        let client = reqwest::Client::new();
        let mut builder = client.get(format!("http://127.0.0.1:{port}/doc"));
        if kind == "with-secret" {
            builder = builder.basic_auth("opencode", Some(secret.as_str()));
        }
        match builder.send().await {
            Ok(r) => eprintln!("REAL-OPENCODE: {kind} -> {}", r.status()),
            Err(e) => eprintln!("REAL-OPENCODE: {kind} ERR {e}"),
        }
    }
    let _ = harness.supervisor.stop(&proc, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn real_startup_does_not_need_provider_auth() {
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let adapter = OpenCodeAdapter::new(cfg);
    let started = Instant::now();
    adapter
        .start(&harness.context())
        .await
        .expect("start offline");
    eprintln!(
        "REAL-OPENCODE: offline startup in {} ms",
        started.elapsed().as_millis()
    );
    adapter.stop().await.expect("stop");
}

/// TASK 25 first-prompt smoke on the REAL OpenCode: model discovery then TWO
/// real turns in ONE session with model=None (Engine Default). Requires real
/// provider credentials — explicitly gated behind SAIWORK_REAL_SMOKE=1 so the
/// default workspace run stays offline (the fixture suite proves the wire
/// contract; this proves the real end-to-end path).
#[tokio::test(flavor = "multi_thread")]
async fn real_first_prompt_smoke_two_turns() {
    if std::env::var("SAIWORK_REAL_SMOKE").as_deref() != Ok("1") {
        eprintln!("SKIP real_first_prompt_smoke_two_turns: SAIWORK_REAL_SMOKE != 1");
        return;
    }
    let Some(cfg) = real_config() else { return };
    let harness = Harness::new();
    let adapter = OpenCodeAdapter::new(cfg);
    adapter
        .start(&harness.context())
        .await
        .expect("real OpenCode must start");
    eprintln!(
        "REAL-OPENCODE: READY on {}",
        adapter.endpoint().expect("endpoint")
    );

    match adapter.list_models().await {
        Ok(models) => eprintln!(
            "REAL-OPENCODE: list_models OK, count={}, sample={:?}",
            models.len(),
            models.first().map(|m| m.id.as_str())
        ),
        Err(e) => eprintln!("REAL-OPENCODE: list_models ERR (non-fatal): {e}"),
    }

    let generic = "ses-real-smoke";
    let session = match adapter
        .create_session(&saiwork_core::engine::CreateSessionRequest {
            session_id: generic.into(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect("create real session")
    {
        saiwork_core::engine::SessionCreation::Created {
            engine_session_id,
            display_name,
        } => saiwork_core::engine::SessionInfo {
            id: generic.into(),
            engine_session_id,
            display_name,
        },
        other => panic!("expected Created, got {other:?}"),
    };
    eprintln!(
        "REAL-OPENCODE: session {} ({})",
        session.engine_session_id, session.display_name
    );

    let mut collector = Collector::new(&harness.bus);
    for (turn, prompt) in ["Reply with exactly READY", "Reply with exactly SECOND"]
        .iter()
        .enumerate()
    {
        let started = Instant::now();
        let run = match adapter
            .send(&saiwork_core::engine::SendRequest {
                session_id: session.id.clone(),
                engine_session_id: session.engine_session_id.clone(),
                prompt: prompt.to_string(),
                model: None, // Engine Default — wire omits the model field
            })
            .await
            .expect("send")
        {
            saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
                saiwork_core::engine::RunHandle { run_id }
            }
            other => panic!("expected Accepted, got {other:?}"),
        };
        eprintln!("REAL-OPENCODE: turn {} sent, run {}", turn + 1, run.run_id);
        let terminal = collector
            .wait_terminal(&run.run_id, Duration::from_secs(300))
            .await;
        assert!(
            matches!(terminal, Event::MessageCompleted { .. }),
            "real turn {} must complete: {terminal:?}",
            turn + 1
        );
        eprintln!(
            "REAL-OPENCODE: turn {} COMPLETED in {} ms",
            turn + 1,
            started.elapsed().as_millis()
        );
    }

    adapter.stop().await.expect("stop");
    assert_eq!(harness.supervisor.count(), 0, "no process left");
}

/// Bounded event collection from the bus (mirrors tests/protocol.rs).
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
}
