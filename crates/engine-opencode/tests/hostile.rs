//! Fixture-driven hostile gate (TASK 10 §97–§98, §120).
//!
//! Every scenario here drives the *adapter lifecycle* with the reusable
//! `fixture_opencode` executable (tests/bin/fixture_opencode.rs): timeouts,
//! cleanup, auth, malformed readiness, races, retries, crashes, restarts.
//! Real-OpenCode compatibility is proven separately in tests/real.rs.

// The fixture serialization lock (`FIXTURE_LOCK`) is intentionally held across
// awaits: tests run in parallel and must not race the process-global fixture
// env. The guard is `Send` (MutexGuard<()>), acquired exactly once per test,
// and only contended by sibling tests — no deadlock or blocking hazard.
#![allow(clippy::await_holding_lock)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine_opencode::{OpenCodeAdapter, OpenCodeConfig};
use saiwork_core::engine::{EngineAdapter, EngineHealth, EngineStartContext};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{Event, EventBus};
use saiwork_process::{ProcessError, ProcessSupervisor, StopHooks};

/// Path to the compiled fixture executable (cargo provides this for bins in
/// the same package).
const FIXTURE_EXE: &str = env!("CARGO_BIN_EXE_fixture_opencode");

/// The fixture reads its behavior from the process-global `FIXTURE_MODE` env
/// (inherited by children). Tests run in parallel in one binary, so every
/// test that drives the fixture serializes on this lock: one fixture test at
/// a time, so env mutations cannot race (§72 test isolation).
static FIXTURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn fixture_guard() -> std::sync::MutexGuard<'static, ()> {
    FIXTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn fast_config() -> OpenCodeConfig {
    OpenCodeConfig {
        startup_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(1),
        retry_port_attempts: 1,
        ..OpenCodeConfig::default()
    }
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

    /// Build a config pointing at the fixture. Callers must hold
    /// `fixture_guard()` for the whole test (see above).
    fn fixture_config(&self, mode: &str, overrides: OpenCodeConfig) -> OpenCodeConfig {
        // The adapter itself launches the fixture; FIXTURE_MODE is inherited
        // by the child from this process env.
        std::env::set_var("FIXTURE_MODE", mode);
        OpenCodeConfig {
            explicit_executable: Some(PathBuf::from(FIXTURE_EXE)),
            ..overrides
        }
    }

    fn supervisor_count(&self) -> usize {
        self.supervisor.count()
    }
}

/// Poll a condition with a bounded deadline (event-based waits + generous
/// ceiling; TASK 09 §74).
async fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration, what: &str) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn force_kill(pid: u32) {
    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .status();
    }
    #[cfg(unix)]
    {
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
    }
}

fn pid_alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        let out = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output()
            .expect("tasklist");
        String::from_utf8_lossy(&out.stdout).contains(&pid.to_string())
    }
    #[cfg(unix)]
    {
        std::path::Path::new(&format!("/proc/{pid}")).exists()
    }
}

/// Supervisor snapshot of the adapter's managed process (id prefix
/// `opencode-`), if any.
fn adapter_process(sup: &ProcessSupervisor) -> Option<(String, u32)> {
    sup.snapshots()
        .into_iter()
        .find(|p| p.id.starts_with("opencode-"))
        .map(|p| (p.id, p.pid))
}

// ---------------------------------------------------------------------------
// §51–§52, §20 — configuration failures are hard, typed, and leave nothing
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn explicit_missing_executable_is_hard_error() {
    let harness = Harness::new();
    let cfg = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from("Z:\\definitely\\missing\\opencode.exe")),
        ..fast_config()
    };
    let adapter = OpenCodeAdapter::new(cfg);
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("explicit"), "{err}");
    assert_eq!(harness.supervisor_count(), 0);
    assert!(matches!(adapter.health(), EngineHealth::Failed { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_executable_is_rejected_by_probe() {
    let harness = Harness::new();
    let wrong = {
        #[cfg(windows)]
        {
            std::env::var_os("COMSPEC")
                .expect("COMSPEC")
                .to_string_lossy()
                .into_owned()
        }
        #[cfg(unix)]
        {
            "/bin/true".to_string()
        }
    };
    let cfg = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from(wrong)),
        ..fast_config()
    };
    let adapter = OpenCodeAdapter::new(cfg);
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(err.to_string().to_lowercase().contains("probe"), "{err}");
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_workspace_fails_cleanly() {
    // Holds the fixture guard: this test still sets FIXTURE_MODE (via
    // fixture_config) and must not race other fixture tests' env.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let mut ctx = harness.context();
    ctx.workspace_path = Some(harness.workspace.path().join("does-not-exist"));
    let err = adapter.start(&ctx).await.expect_err("must fail");
    assert!(err.to_string().contains("workspace"), "{err}");
    assert_eq!(harness.supervisor_count(), 0);
}

// ---------------------------------------------------------------------------
// §56, §27–§32 — readiness and clean lifecycle
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn real_fixture_reaches_ready_and_stops() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);

    adapter
        .start(&harness.context())
        .await
        .expect("fixture must become ready");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    let endpoint = adapter.endpoint().expect("endpoint");
    assert_eq!(
        endpoint.host,
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert!(harness.supervisor_count() == 1);

    adapter.stop().await.expect("clean stop");
    assert_eq!(adapter.health(), EngineHealth::Stopped);
    assert_eq!(harness.supervisor_count(), 0);
    // §62: old endpoint must no longer accept connections after stop.
    assert!(
        std::net::TcpStream::connect((endpoint.host, endpoint.port)).is_err(),
        "endpoint must be closed after stop"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_timeout_cleans_up() {
    // hang: alive, never binds → probes fail → startup deadline.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("hang", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let started = Instant::now();
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must time out");
    assert!(err.to_string().contains("ready"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(15), "bounded");
    assert_eq!(harness.supervisor_count(), 0, "no orphan after timeout");
    assert!(matches!(adapter.health(), EngineHealth::Failed { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_start_cleanup_failure_is_surfaced_and_runtime_remains_owned() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let fail_once_hook = fail_once.clone();
    harness.supervisor.set_stop_hooks_for_test(StopHooks {
        before_stop: Some(Arc::new(move |id, _graceful| {
            fail_once_hook
                .swap(false, std::sync::atomic::Ordering::SeqCst)
                .then(|| ProcessError::TerminationTimeout { id: id.clone() })
        })),
    });

    let cfg = harness.fixture_config("hang", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let first_error = adapter
        .start(&harness.context())
        .await
        .expect_err("readiness and teardown must fail")
        .to_string();
    assert_eq!(
        harness.supervisor_count(),
        1,
        "unproven process must remain supervised"
    );

    harness
        .supervisor
        .set_stop_hooks_for_test(StopHooks::default());
    let retry_started = Instant::now();
    let retry_error = adapter
        .start(&harness.context())
        .await
        .expect_err("restart must refuse an unproven prior runtime")
        .to_string();
    let retry_elapsed = retry_started.elapsed();
    let adapter_cleanup = adapter.kill().await;
    let count_after_adapter_cleanup = harness.supervisor_count();
    let _ = harness.supervisor.shutdown().await;

    assert!(
        first_error.contains("did not become ready"),
        "{first_error}"
    );
    assert!(first_error.contains("cleanup"), "{first_error}");
    assert!(
        first_error.contains("termination") && first_error.contains("unproven"),
        "{first_error}"
    );
    assert!(
        retry_error.contains("termination") && retry_error.contains("unproven"),
        "{retry_error}"
    );
    assert!(
        retry_elapsed < Duration::from_millis(500),
        "refusal must not spawn or probe again: {retry_elapsed:?}"
    );
    assert!(
        adapter_cleanup.is_ok(),
        "adapter must retain cleanup authority"
    );
    assert_eq!(
        count_after_adapter_cleanup, 0,
        "adapter cleanup must reap it"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn exit_during_startup_is_detected_before_timeout() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("exit_now", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let started = Instant::now();
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("exited"), "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "short-circuit, not timeout"
    );
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn exit_after_bind_is_reported() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("exit_after_bind", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("exited"), "{err}");
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_readiness_is_protocol_unexpected() {
    // HTTP 200 with `{}` — an endpoint answering but not an OpenCode server
    // (§59): classified ProtocolUnexpected, not a bland timeout.
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("wrong_response", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(
        err.to_string().contains("not an OpenCode server") || err.to_string().contains("protocol"),
        "{err}"
    );
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn never_ready_is_protocol_unexpected() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("never_ready", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(
        err.to_string().contains("not an OpenCode server") || err.to_string().contains("protocol"),
        "{err}"
    );
    assert_eq!(harness.supervisor_count(), 0);
}

// ---------------------------------------------------------------------------
// §21, §60 — local server auth
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn auth_required_fixture_succeeds() {
    // FIXTURE_AUTH=1 with no expected password: the fixture accepts any
    // non-empty Basic credential — proves the adapter always sends auth.
    let _guard = fixture_guard();
    std::env::set_var("FIXTURE_AUTH", "1");
    std::env::remove_var("FIXTURE_PASSWORD");
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    adapter
        .start(&harness.context())
        .await
        .expect("auth must succeed");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
    std::env::remove_var("FIXTURE_AUTH");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_auth_fails_fast() {
    // The fixture demands a password the adapter's random secret can never
    // match → 401 → typed auth failure, not a 30s timeout.
    let _guard = fixture_guard();
    std::env::set_var("FIXTURE_AUTH", "1");
    std::env::set_var("FIXTURE_PASSWORD", "impossible-password");
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", fast_config());
    let adapter = OpenCodeAdapter::new(cfg);
    let started = Instant::now();
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("401"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(10), "fail fast");
    assert_eq!(harness.supervisor_count(), 0);
    std::env::remove_var("FIXTURE_AUTH");
    std::env::remove_var("FIXTURE_PASSWORD");
}

// ---------------------------------------------------------------------------
// §34, §76 — stop during STARTING (shutdown race)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stop_during_start_cancels_cleanly() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    // delayed_ready: prints listening, then serves only after 3s — plenty of
    // window for the shutdown race.
    let cfg = harness.fixture_config("delayed_ready", fast_config());
    let adapter = Arc::new(OpenCodeAdapter::new(cfg));
    let ctx = harness.context();

    let start_task = {
        let adapter = adapter.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move { adapter.start(&ctx).await })
    };
    tokio::time::sleep(Duration::from_millis(700)).await;
    adapter.stop().await.expect("stop during start");
    let start_result = start_task.await.expect("start task must not panic");
    assert!(start_result.is_err(), "no late engine.ready after stop");
    assert_eq!(adapter.health(), EngineHealth::Stopped);
    assert_eq!(harness.supervisor_count(), 0);
}

// ---------------------------------------------------------------------------
// §35, §39 — double start / double stop
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn double_start_is_rejected() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    adapter
        .start(&harness.context())
        .await
        .expect("first start");
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("second start");
    assert!(err.to_string().contains("already"), "{err}");
    // Still exactly one managed process, not two (§35).
    assert_eq!(harness.supervisor_count(), 1);
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn double_stop_is_safe() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    adapter.start(&harness.context()).await.expect("start");
    adapter.stop().await.expect("first stop");
    adapter.stop().await.expect("second stop is idempotent");
    assert_eq!(adapter.health(), EngineHealth::Stopped);
    assert_eq!(harness.supervisor_count(), 0);
}

// ---------------------------------------------------------------------------
// §40–§45, §63 — unexpected exit after READY; no auto-restart
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn crash_after_ready_reports_failure_and_dies_cleanly() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    let mut observer = harness.bus.subscribe();

    adapter.start(&harness.context()).await.expect("ready");
    let (_id, pid) = adapter_process(&harness.supervisor).expect("managed process");
    let endpoint = adapter.endpoint().expect("endpoint");

    force_kill(pid);
    wait_until(
        || matches!(adapter.health(), EngineHealth::Failed { .. }),
        Duration::from_secs(10),
        "engine FAILED after crash",
    )
    .await;
    wait_until(
        || {
            let mut saw = false;
            while let Ok(Some(env)) = observer.try_recv() {
                if matches!(env.event, Event::EngineFailed { .. }) {
                    saw = true;
                }
            }
            saw
        },
        Duration::from_secs(5),
        "engine.failed event",
    )
    .await;
    assert!(!pid_alive(pid), "crashed process must be gone");
    assert!(
        std::net::TcpStream::connect((endpoint.host, endpoint.port)).is_err(),
        "endpoint dead after crash"
    );
    // The app itself stays alive: adapter can be restarted explicitly.
    assert_eq!(harness.supervisor_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_after_crash_is_explicit_not_automatic() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    adapter.start(&harness.context()).await.expect("ready");
    let (_old_id, old_pid) = adapter_process(&harness.supervisor).expect("managed process");
    force_kill(old_pid);
    wait_until(
        || matches!(adapter.health(), EngineHealth::Failed { .. }),
        Duration::from_secs(10),
        "FAILED",
    )
    .await;
    // No automatic restart: state stays FAILED until explicitly started.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(matches!(adapter.health(), EngineHealth::Failed { .. }));
    assert_eq!(harness.supervisor_count(), 0);

    adapter
        .start(&harness.context())
        .await
        .expect("explicit restart");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    // Fresh runtime: a new OS pid and a new ProcessId (§43, §91).
    let (_new_id, new_pid) = adapter_process(&harness.supervisor).expect("new process");
    assert_ne!(old_pid, new_pid, "restart must spawn a fresh OS process");
    adapter.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_after_stop_is_clean() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    let mut observer = harness.bus.subscribe();

    for _ in 0..2 {
        adapter.start(&harness.context()).await.expect("start");
        assert_eq!(adapter.health(), EngineHealth::Ready);
        adapter.stop().await.expect("stop");
        assert_eq!(harness.supervisor_count(), 0);
    }
    // A clean stop must never be reported as an engine failure (§41).
    let mut failed = false;
    while let Ok(Some(env)) = observer.try_recv() {
        if matches!(env.event, Event::EngineFailed { .. }) {
            failed = true;
        }
    }
    assert!(!failed, "clean stop must not publish engine.failed");
}

// ---------------------------------------------------------------------------
// §17, §50, §90–§91 — port collision: classified, bounded retry, fresh
// ProcessId, everything cleaned between attempts
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn port_collision_retries_bounded_and_cleans() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config(
        "collision",
        OpenCodeConfig {
            retry_port_attempts: 3,
            ..fast_config()
        },
    );
    let adapter = OpenCodeAdapter::new(cfg);
    let err = adapter
        .start(&harness.context())
        .await
        .expect_err("must fail");
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("port") && msg.contains("3"), "{err}");
    assert_eq!(
        harness.supervisor_count(),
        0,
        "every failed attempt cleaned"
    );
    assert!(matches!(adapter.health(), EngineHealth::Failed { .. }));
}

// ---------------------------------------------------------------------------
// §43, §94 — repeated start/stop: no leaks (processes, listeners, events)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn repeated_start_stop_leaves_nothing_behind() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    let cfg = harness.fixture_config("real", OpenCodeConfig::default());
    let adapter = OpenCodeAdapter::new(cfg);
    let mut observer = harness.bus.subscribe();
    let mut prior_port: Option<u16> = None;

    for i in 0..3 {
        adapter.start(&harness.context()).await.expect("start");
        let endpoint = adapter.endpoint().expect("endpoint");
        if let Some(prior) = prior_port {
            assert_ne!(prior, endpoint.port, "each runtime gets a fresh port (§86)");
        }
        prior_port = Some(endpoint.port);
        assert_eq!(harness.supervisor_count(), 1);
        adapter.stop().await.expect("stop");
        assert_eq!(harness.supervisor_count(), 0, "cycle {i}: registry empty");
        assert!(
            std::net::TcpStream::connect((endpoint.host, endpoint.port)).is_err(),
            "cycle {i}: port closed"
        );
    }
    let mut failed = false;
    while let Ok(Some(env)) = observer.try_recv() {
        if matches!(env.event, Event::EngineFailed { .. }) {
            failed = true;
        }
    }
    assert!(!failed, "no failure events across clean cycles");
}

// ---------------------------------------------------------------------------
// §46–§47 — probe without start
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn probe_installation_works_without_start() {
    let harness = Harness::new();
    let cfg = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from(FIXTURE_EXE)),
        ..OpenCodeConfig::default()
    };
    let result = OpenCodeAdapter::probe_installation(&harness.supervisor, &cfg)
        .await
        .expect("probe");
    assert_eq!(result.version, "1.18.18");
    assert_eq!(
        harness.supervisor_count(),
        0,
        "probe leaves no process behind"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_missing_executable_errors() {
    let harness = Harness::new();
    let cfg = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from("Z:\\nope\\opencode.exe")),
        ..OpenCodeConfig::default()
    };
    let err = OpenCodeAdapter::probe_installation(&harness.supervisor, &cfg)
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("explicit"), "{err}");
    assert_eq!(harness.supervisor_count(), 0);
}

// ---------------------------------------------------------------------------
// §7–§8, §54 — Windows launchers: npm shim resolution + cmd.exe wrapper
// with spaces in the path
// ---------------------------------------------------------------------------

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread")]
async fn npm_style_shim_resolves_to_real_executable() {
    let _guard = fixture_guard();
    let dir = std::env::temp_dir().join(format!("oc-npm-shim-{}", uuid::Uuid::new_v4()));
    let bin = dir.join("node_modules").join("fake-pkg").join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::copy(FIXTURE_EXE, bin.join("opencode.exe")).unwrap();
    let shim = dir.join("opencode.cmd");
    std::fs::write(
        &shim,
        "@ECHO off\r\nGOTO start\r\n:find_dp0\r\nSET dp0=%~dp0\r\nEXIT /b\r\n:start\r\nSETLOCAL\r\nCALL :find_dp0\r\n\"%dp0%\\node_modules\\fake-pkg\\bin\\opencode.exe\"   %*\r\n",
    )
    .unwrap();
    std::env::set_var("FIXTURE_MODE", "real");

    let harness = Harness::new();
    let cfg = OpenCodeConfig {
        explicit_executable: Some(shim),
        ..OpenCodeConfig::default()
    };
    let adapter = OpenCodeAdapter::new(cfg);
    adapter
        .start(&harness.context())
        .await
        .expect("shim-resolved launch");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    adapter.stop().await.expect("stop");
    assert_eq!(harness.supervisor_count(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread")]
async fn cmd_wrapper_launch_with_spaces_in_path() {
    // A shim that does NOT name an .exe directly forces the encapsulated
    // `cmd.exe /D /S /C "<shim>" ...` launch — including a path with spaces
    // (§8, §54). The whole lifecycle (probe + server) runs through cmd.exe,
    let _guard = fixture_guard();
    // and the Job Object must clean up the cmd.exe → launcher.bat → fixture
    // tree.
    let dir = std::env::temp_dir().join(format!("oc wrapper test {}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::copy(FIXTURE_EXE, dir.join("fixture_opencode.exe")).unwrap();
    // The shim must quote its own path references (spaces!). The point of
    // this test is that the ADAPTER quotes the shim path when invoking
    // cmd.exe; the shim itself also quotes before calling the next hop.
    std::fs::write(
        dir.join("opencode.cmd"),
        "@ECHO off\r\ncall \"%~dp0\\launcher.bat\" %*\r\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("launcher.bat"),
        "@\"%~dp0\\fixture_opencode.exe\" %*\r\n",
    )
    .unwrap();
    std::env::set_var("FIXTURE_MODE", "real");

    let harness = Harness::new();
    let cfg = OpenCodeConfig {
        explicit_executable: Some(dir.join("opencode.cmd")),
        ..OpenCodeConfig::default()
    };
    let adapter = OpenCodeAdapter::new(cfg);
    adapter
        .start(&harness.context())
        .await
        .expect("cmd.exe wrapper launch");
    assert_eq!(adapter.health(), EngineHealth::Ready);
    let (_id, pid) = adapter_process(&harness.supervisor).expect("managed process");
    adapter.stop().await.expect("stop");
    assert_eq!(harness.supervisor_count(), 0);
    assert!(
        !pid_alive(pid),
        "wrapper tree must be fully gone after stop"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// §95, §122 — two isolated instances
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn two_instances_are_isolated() {
    let _guard = fixture_guard();
    let harness_a = Harness::new();
    let harness_b = Harness::new();
    std::env::set_var("FIXTURE_MODE", "real");
    let cfg_a = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from(FIXTURE_EXE)),
        ..OpenCodeConfig::default()
    };
    let cfg_b = cfg_a.clone();
    let adapter_a = OpenCodeAdapter::new(cfg_a);
    let adapter_b = OpenCodeAdapter::new(cfg_b);

    adapter_a
        .start(&harness_a.context())
        .await
        .expect("A ready");
    adapter_b
        .start(&harness_b.context())
        .await
        .expect("B ready");
    let ep_a = adapter_a.endpoint().expect("A endpoint");
    let ep_b = adapter_b.endpoint().expect("B endpoint");
    assert_ne!(ep_a.port, ep_b.port, "distinct ports");
    assert_ne!(
        adapter_a.process_id().map(|i| i.to_string()),
        adapter_b.process_id().map(|i| i.to_string()),
        "distinct process identity"
    );

    // Stopping A must not touch B (§95).
    adapter_a.stop().await.expect("stop A");
    assert_eq!(adapter_a.health(), EngineHealth::Stopped);
    assert_eq!(adapter_b.health(), EngineHealth::Ready);
    assert_eq!(harness_b.supervisor_count(), 1);
    adapter_b.stop().await.expect("stop B");
    assert_eq!(harness_b.supervisor_count(), 0);
}

// ---------------------------------------------------------------------------
// §24, §74 — secret hygiene: env names only, values never in diagnostics
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn secret_never_appears_in_process_diagnostics() {
    let _guard = fixture_guard();
    let harness = Harness::new();
    std::env::set_var("FIXTURE_MODE", "real");
    let cfg = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from(FIXTURE_EXE)),
        ..OpenCodeConfig::default()
    };
    let adapter = OpenCodeAdapter::new(cfg);
    adapter.start(&harness.context()).await.expect("ready");

    let snapshots = harness.supervisor.snapshots();
    let ours = snapshots
        .iter()
        .find(|s| s.id.starts_with("opencode-"))
        .expect("snapshot");
    // The env list contains only the variable NAME — the password value is
    // never stored or rendered (supervisor Debug/snapshot contract §24).
    // Both auth vars are pinned (never ambient): password = runtime secret,
    // username = the client's fixed identity (§23).
    assert_eq!(
        ours.env,
        vec![
            "OPENCODE_SERVER_PASSWORD".to_string(),
            "OPENCODE_SERVER_USERNAME".to_string()
        ]
    );
    assert!(!ours.command.contains("OPENCODE_SERVER_PASSWORD"));
    assert!(!ours.command.contains('='), "no env values in command line");

    adapter.stop().await.expect("stop");
}

// ---------------------------------------------------------------------------
// Registry-boundary behavior: session methods require a READY engine
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn session_methods_require_ready_engine() {
    // Holds the fixture lock even though it never spawns a server: it
    // mutates the process-global FIXTURE_MODE env, and any fixture test
    // running concurrently that has set its env but not yet spawned its
    // server child would inherit this value instead of its own. The lock is
    // the serialization point for all FIXTURE_* env mutations.
    let _guard = fixture_guard();
    let _harness = Harness::new();
    std::env::set_var("FIXTURE_MODE", "real");
    let cfg = OpenCodeConfig {
        explicit_executable: Some(PathBuf::from(FIXTURE_EXE)),
        ..OpenCodeConfig::default()
    };
    let adapter = OpenCodeAdapter::new(cfg);
    // TASK 11 turned the capability flags on (this file is the TASK 10
    // process gate); before the engine is started the session API must fail
    // with NotReady — never a fake "no capability".
    assert!(adapter.health() == EngineHealth::Unknown);

    let err = adapter
        .create_session(&saiwork_core::engine::CreateSessionRequest {
            session_id: "ses-not-started".into(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect_err("engine not started");
    assert!(
        err.to_string().contains("not ready") || err.to_string().contains("started"),
        "{err}"
    );

    let err = adapter.list_models().await.expect_err("engine not started");
    assert!(
        err.to_string().contains("not ready") || err.to_string().contains("started"),
        "{err}"
    );
}
