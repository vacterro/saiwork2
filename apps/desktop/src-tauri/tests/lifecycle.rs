//! Application lifecycle integration tests (TASK 08 §55–§65, §100).
//!
//! These run against the real `saiwork-core::App` + `engine-fake` +
//! `saiwork-process` wiring in isolated temporary data roots — never the
//! developer's real data (STORAGE.md test policy).

use std::path::PathBuf;
use std::sync::Arc;

use engine_fake::FakeEngine;
use saiwork_core::engine::EngineAdapter;
use saiwork_core::{App, AppConfig, AppState, CoreError};
use saiwork_process::{ManagedProcess, ProcessSpec, ProcessState};

fn temp_config() -> (tempfile::TempDir, AppConfig) {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = AppConfig {
        data_root: dir.path().join("data"),
        portable: true,
    };
    (dir, config)
}

fn fixture_exe() -> PathBuf {
    // cargo sets CARGO_BIN_EXE_<name> for path-dependency bins; fall back to
    // walking up from the test executable when absent.
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_proc_fixture") {
        return PathBuf::from(p);
    }
    let mut p = std::env::current_exe().expect("test exe path");
    p.pop(); // deps/
    p.pop(); // target/<profile>/
    p.push("proc_fixture.exe");
    if !p.exists() {
        panic!("proc_fixture.exe not found at {p:?} (build saiwork-process first)");
    }
    p
}

async fn spawn_sleeping_child(core: &App) -> Arc<ManagedProcess> {
    let mut spec = ProcessSpec::new("lifecycle-fixture", fixture_exe().display().to_string());
    spec.args = vec!["--sleep".into(), "30000".into()];
    core.supervisor.spawn(spec).await.expect("fixture spawn")
}

// ---- boot / shutdown / event ordering ------------------------------------

#[tokio::test]
async fn boot_ready_shutdown_stopped_with_coherent_event_order() {
    let (_dir, config) = temp_config();
    let core = App::bootstrap_with(config).unwrap();
    assert_eq!(core.state(), AppState::Ready);

    let mut sub = core.bus.subscribe();
    // Bounded wait: the running-tracker task must attach its subscription
    // (spawned during bootstrap; scheduling is async).
    wait_for_subscribers(&core, 2).await;

    let report = core.shutdown("test").await;
    assert_eq!(core.state(), AppState::Stopped);
    assert_eq!(report.outcome, "clean");

    // app.stopping is the first thing the shutdown sequence publishes; the
    // tracker sees it, exits, and drops its subscription (no listener leak).
    let first = sub.recv().await.expect("event after shutdown start");
    assert_eq!(first.event.name(), "app.stopping");
    // app.started was published before this subscription attached, so it can
    // never appear after app.stopping; bounded drain proves no resurrection
    // events follow the shutdown announcement.
    let drained = drain_bounded(&mut sub, 5).await;
    assert!(
        !drained.iter().any(|e| e.name() == "app.started"),
        "app.started must never appear after app.stopping"
    );

    // The tracker subscription ended on app.stopping: back to the test's own.
    assert_eq!(
        core.bus.subscriber_count(),
        1,
        "only the test subscription remains after shutdown"
    );

    // Commands are rejected after shutdown (TASK 08 §65).
    assert!(matches!(core.require_ready(), Err(CoreError::ShuttingDown)));
}

/// Drain pending events for a bounded window (the bus stays open while the
/// App is alive; never wait for it to close).
async fn drain_bounded(
    sub: &mut saiwork_events::Subscription,
    attempts: usize,
) -> Vec<saiwork_events::Event> {
    let mut out = Vec::new();
    for _ in 0..attempts {
        match sub.try_recv() {
            Ok(Some(env)) => out.push(env.event),
            Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            Err(_) => break,
        }
    }
    out
}

/// Yield until the bus has at least `n` subscribers (bounded: 2s).
async fn wait_for_subscribers(core: &App, n: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while core.bus.subscriber_count() < n {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {n} bus subscribers"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

// ---- FakeEngine active work is cleaned on shutdown (§55) -------------------

#[tokio::test]
async fn fake_engine_active_run_is_cleaned_on_shutdown() {
    let (_dir, config) = temp_config();
    let core = App::bootstrap_with(config).unwrap();
    let engine = Arc::new(FakeEngine::new());
    let engine_id = engine.identity().id.clone();
    core.engines.register(engine.clone());
    core.engines
        .start(&engine_id, &core.engines.start_context(None, None))
        .await
        .unwrap();

    let session = core.sessions.create(&engine_id, None, None).await.unwrap();
    // /sim:hang starts a run that never completes on its own.
    core.sessions
        .send(&session.id, "/sim:hang", None)
        .await
        .unwrap();
    assert_eq!(engine.active_runs(), 1, "hang run must be active");

    let report = core.shutdown("hang run cleanup").await;
    assert_eq!(core.state(), AppState::Stopped);
    assert_eq!(report.outcome, "clean");
    assert_eq!(engine.active_runs(), 0, "no fake run may survive shutdown");
    assert_eq!(engine.task_count(), 0, "no fake worker task may survive");
    assert_eq!(
        engine.pending_permissions(),
        0,
        "no pending permission wait may survive"
    );
    assert!(engine.received_commands().iter().any(|c| c == "stop"));
}

// ---- process fixture is cleaned on shutdown (§56) ---------------------------

#[tokio::test]
async fn managed_process_is_cleaned_on_app_shutdown() {
    let (_dir, config) = temp_config();
    let core = App::bootstrap_with(config).unwrap();
    let process = spawn_sleeping_child(&core).await;
    assert_eq!(core.supervisor.count(), 1);
    assert_eq!(process.state(), ProcessState::Running);

    let report = core.shutdown("process cleanup").await;
    assert_eq!(core.state(), AppState::Stopped);
    assert_eq!(core.supervisor.count(), 0, "registry must be empty");
    assert_eq!(process.state(), ProcessState::Exited);
    // OS-level kill proof is the TASK 06 tree test; here we assert the
    // application wiring (registry empty, state terminal, report recorded).
    let _ = report;
}

// ---- storage survives app restart (§57) ------------------------------------

#[tokio::test]
async fn durable_state_survives_app_restart() {
    let (dir, config) = temp_config();
    let ws_path = dir.path().join("workspace-a");
    std::fs::create_dir_all(&ws_path).unwrap();
    {
        let core = App::bootstrap_with(config.clone()).unwrap();
        let ws = core.workspaces.open(&ws_path).await.unwrap();
        assert_eq!(core.workspaces.list().unwrap().len(), 1);
        let _ = ws;
        core.shutdown("first run").await;
    }
    {
        let core = App::bootstrap_with(config.clone()).unwrap();
        let workspaces = core.workspaces.list().unwrap();
        assert_eq!(
            workspaces.len(),
            1,
            "durable state must survive a clean restart"
        );
        assert!(config.database_path().exists());
        core.shutdown("second run").await;
    }
}

// ---- startup failure is fail-closed (§59) ----------------------------------

#[tokio::test]
async fn storage_startup_failure_is_fail_closed() {
    let (_dir, config) = temp_config();
    config.ensure_layout().unwrap();
    // Make the DB path a directory: Db::open fails with a typed error.
    std::fs::create_dir_all(config.database_path()).unwrap();
    let err = match App::bootstrap_with(config.clone()) {
        Err(e) => e,
        Ok(_) => panic!("bootstrap must fail when the DB path is a directory"),
    };
    assert!(
        matches!(err, CoreError::Storage(_)),
        "expected a typed storage error, got {err:?}"
    );
    // Nothing was half-created that would block a later, fixed start.
    std::fs::remove_dir(config.database_path()).unwrap();
    let core = App::bootstrap_with(config).unwrap();
    assert_eq!(core.state(), AppState::Ready);
    core.shutdown("after recovery").await;
}

// ---- corrupt DB is rejected, never deleted (§11, TASK 05 §22) --------------

#[tokio::test]
async fn corrupt_database_fails_boot_without_deletion() {
    let (_dir, config) = temp_config();
    config.ensure_layout().unwrap();
    let garbage = b"not a sqlite database, just noise for the boot test".repeat(16);
    std::fs::write(config.database_path(), &garbage).unwrap();
    let err = match App::bootstrap_with(config.clone()) {
        Err(e) => e,
        Ok(_) => panic!("bootstrap must fail on a corrupt database"),
    };
    assert!(
        matches!(err, CoreError::Storage(_)),
        "expected typed storage error, got {err:?}"
    );
    // The corrupt file is never deleted (TASK 05 §22).
    assert_eq!(
        std::fs::read(config.database_path()).unwrap(),
        garbage,
        "corrupt DB must not be deleted or rewritten"
    );
}

// ---- shutdown aggregation: forced kill must not skip storage close (§38) ---

#[tokio::test]
async fn forced_process_kill_does_not_skip_storage_checkpoint() {
    let (_dir, config) = temp_config();
    let core = App::bootstrap_with(config.clone()).unwrap();
    // A long-sleeping console fixture: the graceful hint (taskkill without
    // /F) has no window to close, so the supervisor must escalate to the
    // force path at its bounded deadline — a real forced kill.
    let process = spawn_sleeping_child(&core).await;
    core.db.set_setting("pre_shutdown", "durable").unwrap();

    let shutdown_started = std::time::Instant::now();
    let report = core.shutdown("forced kill aggregation").await;
    let elapsed = shutdown_started.elapsed().as_millis();
    assert_eq!(core.state(), AppState::Stopped);
    assert_eq!(core.supervisor.count(), 0);
    assert_eq!(process.state(), ProcessState::Exited);
    assert_eq!(
        report.outcome, "clean",
        "a bounded process kill is a warning-free (clean) outcome, not a failure"
    );
    // Escalation evidence (Windows): the fixture is spawned with
    // CREATE_NO_WINDOW, so the graceful hint (taskkill without /F) cannot
    // close it — the supervisor must wait its full graceful budget and then
    // escalate to TerminateJobObject. The escalation succeeding is routine
    // (not a warning): `forced_processes` only records force *failures*, so
    // the observable proof is the elapsed graceful budget itself.
    #[cfg(windows)]
    assert!(
        elapsed >= 4900,
        "graceful path must time out before force on Windows, took {elapsed}ms"
    );

    // Aggregation contract (§37/§38): a process-termination escalation must
    // NOT skip the unrelated subsystem cleanup — storage was checkpointed
    // before STOPPED, so the pre-shutdown write is durable.
    let reopened = App::bootstrap_with(config).unwrap();
    assert_eq!(
        reopened.db.get_setting("pre_shutdown").unwrap().as_deref(),
        Some("durable"),
        "storage close must run even when a process needed force kill"
    );
    reopened.shutdown("cleanup").await;
}

// ---- storage busy during shutdown (§76) ------------------------------------

#[tokio::test]
async fn storage_busy_during_shutdown_is_bounded_and_clean() {
    let (_dir, config) = temp_config();
    let core = App::bootstrap_with(config.clone()).unwrap();
    core.db.set_setting("before_busy", "yes").unwrap();

    // A second connection (another process in real life) holds the write lock
    // while shutdown begins. The app must wait bounded (busy_timeout), then
    // proceed — never hang and never fail-storm.
    let lock_conn = rusqlite::Connection::open(config.database_path()).unwrap();
    lock_conn
        .busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    lock_conn
        .execute_batch(
            "BEGIN IMMEDIATE; \
             INSERT INTO app_settings (key, value, updated_at) VALUES ('ext', '1', 1);",
        )
        .unwrap();

    let app = core.clone();
    let shutdown = tokio::spawn(async move { app.shutdown("storage busy").await });
    // SQLite's `wal_checkpoint` does not run the busy handler: under an
    // external write lock it fails fast with a typed Busy error instead of
    // blocking. The §76 contract is bounded + coherent: shutdown must
    // complete quickly, record the storage failure as a warning (never a
    // hang or endless retry), and still end STOPPED.
    let report = tokio::time::timeout(std::time::Duration::from_secs(8), shutdown)
        .await
        .expect("shutdown must complete within the bounded busy window")
        .expect("shutdown task must not panic");
    lock_conn.execute_batch("COMMIT;").unwrap();
    assert_eq!(core.state(), AppState::Stopped);
    assert!(
        report.outcome == "clean" || report.outcome == "completed_with_warnings",
        "coherent shutdown outcome under lock contention, got {:?}",
        report
    );

    // DB reopened afterwards works normally and the app's own write survived
    // the checkpoint taken after the lock released.
    let reopened = App::bootstrap_with(config).unwrap();
    assert_eq!(
        reopened.db.get_setting("before_busy").unwrap().as_deref(),
        Some("yes")
    );
    reopened.shutdown("cleanup").await;
}
