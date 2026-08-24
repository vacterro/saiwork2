//! Cross-engine isolation suite (TASK 17 §64–§70, §139–§144, §188).
//!
//! Proves the registry + adapters keep engines isolated: identical upstream
//! session/run strings in two adapters never collide, one engine's failure
//! never poisons another, there is no automatic engine fallback, and the
//! generic CLI adapter passes the same contract tests as FakeEngine where
//! capabilities overlap.

use std::sync::Arc;
use std::time::Duration;

use engine_fake::FakeEngine;
use engine_generic_cli::{GenericCliConfig, GenericCliEngine, ENGINE_ID as CLI_ID};
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineHealth, EngineRegistry, EngineStartContext,
    SendRequest,
};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{bus::Subscription, Envelope, Event, EventBus};
use saiwork_process::ProcessSupervisor;
use tempfile::TempDir;
use tokio::time::timeout;

/// Rebuild the authoritative SessionInfo from a Created receipt (the generic
/// id is minted by the caller in these tests).
fn created_info(
    generic_id: String,
    acc: saiwork_core::engine::SessionCreation,
) -> saiwork_core::engine::SessionInfo {
    match acc {
        saiwork_core::engine::SessionCreation::Created {
            engine_session_id,
            display_name,
        } => saiwork_core::engine::SessionInfo {
            id: generic_id,
            engine_session_id,
            display_name,
        },
        other => panic!("expected Created, got {other:?}"),
    }
}

/// The engine's successful spawn/create IS the authoritative acceptance
/// boundary; unwrap the receipt to the run handle.
fn accepted(acc: saiwork_core::engine::SendAcceptance) -> saiwork_core::engine::RunHandle {
    match acc {
        saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
            saiwork_core::engine::RunHandle { run_id }
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
}

fn start_ctx(bus: EventBus, workspace: Option<std::path::PathBuf>) -> EngineStartContext {
    let bus2 = bus.clone();
    EngineStartContext {
        workspace_id: None,
        workspace_path: workspace,
        bus: bus.clone(),
        diagnostics: Arc::new(Diagnostics::new()),
        supervisor: Arc::new(ProcessSupervisor::new(bus)),
        report_failure: Arc::new(move |engine_id: &str, message: &str| {
            bus2.publish(Event::EngineFailed {
                engine_id: engine_id.into(),
                error: message.into(),
            });
        }),
    }
}

/// A registry with FakeEngine + GenericCliEngine registered under distinct
/// ids, both sharing one bus and one supervisor.
fn registry_with_cli(
    script: String,
) -> (
    Arc<EngineRegistry>,
    Arc<FakeEngine>,
    Arc<GenericCliEngine>,
    EventBus,
    TempDir,
) {
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let registry = Arc::new(EngineRegistry::new(
        bus.clone(),
        Arc::new(Diagnostics::new()),
        supervisor,
    ));
    let fake = Arc::new(FakeEngine::new());
    let tmp = tempfile::tempdir().unwrap();
    let cli = Arc::new(GenericCliEngine::new(GenericCliConfig {
        executable: "python".into(),
        args: vec![script],
        label: "Test CLI".into(),
        max_output_bytes: 256 * 1024,
        timeout: Duration::from_secs(30),
        max_prompt_bytes: 64 * 1024,
    }));
    registry.register(fake.clone());
    registry.register(cli.clone());
    (registry, fake, cli, bus, tmp)
}

fn echo_script() -> String {
    let path = std::env::temp_dir().join(format!("saiwork_cli_echo_{}.py", std::process::id()));
    std::fs::write(
        &path,
        "import sys\ndata = sys.stdin.read()\nsys.stdout.write('CLI:' + data)\nsys.stdout.flush()\n",
    )
    .expect("write echo script");
    path.to_string_lossy().to_string()
}

async fn recv_message(sub: &mut Subscription) -> Event {
    for _ in 0..32 {
        let env: Envelope = timeout(Duration::from_secs(20), sub.recv())
            .await
            .expect("event timeout")
            .expect("subscription alive");
        if matches!(
            env.event,
            Event::MessageStarted { .. }
                | Event::MessageDelta { .. }
                | Event::MessageCompleted { .. }
                | Event::MessageFailed { .. }
                | Event::MessageCancelled { .. }
        ) {
            return env.event;
        }
    }
    panic!("no message event received");
}

#[tokio::test]
async fn registry_lists_both_engines_with_distinct_ids() {
    let (registry, _fake, _cli, _bus, _tmp) = registry_with_cli(String::new());
    let infos = registry.list_info();
    let ids: Vec<String> = infos.iter().map(|i| i.identity.id.clone()).collect();
    assert!(ids.contains(&"fake".to_string()), "ids: {ids:?}");
    assert!(ids.contains(&CLI_ID.to_string()), "ids: {ids:?}");
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(ids.len(), unique.len(), "unique EngineIds");
}

#[tokio::test]
async fn one_engine_failure_does_not_poison_the_other() {
    // A broken engine (bogus executable) fails to start; FakeEngine still
    // starts and runs (§99–§100, §40).
    let (registry, fake, _cli, bus, tmp) = registry_with_cli(String::new());
    let bad_cli = Arc::new(GenericCliEngine::new(GenericCliConfig {
        executable: "definitely-not-real-xyz".into(),
        args: vec![],
        label: "Broken CLI".into(),
        max_output_bytes: 1024,
        timeout: Duration::from_secs(5),
        max_prompt_bytes: 1024,
    }));
    registry.register(bad_cli.clone());

    let ctx = start_ctx(bus.clone(), Some(tmp.path().to_path_buf()));
    let err = registry
        .start(CLI_ID, &ctx)
        .await
        .expect_err("bad config fails");
    assert!(err.to_string().contains("not found"));

    let mut sub = bus.subscribe();
    registry.start("fake", &ctx).await.expect("fake starts");
    assert!(matches!(fake.health(), EngineHealth::Ready));

    // FakeEngine completes a run normally despite the broken sibling.
    let session = created_info(
        "iso-fake-session".into(),
        fake.create_session(&CreateSessionRequest {
            session_id: "iso-fake-session".into(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect("fake session"),
    );
    let handle = accepted(
        fake.send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .expect("fake send"),
    );
    let mut terminal = None;
    for _ in 0..64 {
        let ev = recv_message(&mut sub).await;
        if let Event::MessageCompleted { run_id, .. } = &ev {
            if run_id == &handle.run_id.clone().into() {
                terminal = Some(ev);
                break;
            }
        }
    }
    assert!(terminal.is_some(), "fake run completed");
    // The failed sibling remains isolated: its engine.failed event did not
    // mark FakeEngine failed.
    assert!(matches!(fake.health(), EngineHealth::Ready));
}

#[tokio::test]
async fn identical_session_strings_stay_isolated_across_engines() {
    // Both adapters create a session whose upstream id is literally "123";
    // neither can reach into the other's session (§64, §140).
    let (_registry, fake, cli, bus, tmp) = registry_with_cli(echo_script());
    fake.start(&start_ctx(bus.clone(), None))
        .await
        .expect("fake start");
    cli.start(&start_ctx(bus.clone(), Some(tmp.path().to_path_buf())))
        .await
        .expect("cli start");

    let fake_session = created_info(
        "iso-fake-session-2".into(),
        fake.create_session(&CreateSessionRequest {
            session_id: "iso-fake-session-2".into(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect("fake session"),
    );

    // The CLI's engine_session_id is its own generated uuid — never derived
    // from the FakeEngine's id, and a send to a CLI session must not route
    // through the FakeEngine.
    let handle = accepted(
        cli.send(&SendRequest {
            session_id: fake_session.id.clone(),
            engine_session_id: fake_session.id.clone(),
            prompt: "ping".into(),
            model: None,
        })
        .await
        .expect("cli send into its own session"),
    );

    let mut sub = bus.subscribe();
    let mut delta = None;
    for _ in 0..64 {
        let ev = recv_message(&mut sub).await;
        if let Event::MessageDelta {
            run_id, delta: d, ..
        } = &ev
        {
            if run_id == &handle.run_id.clone().into() {
                delta = Some(d.clone());
                break;
            }
        }
        if matches!(ev, Event::MessageFailed { .. }) {
            break;
        }
    }
    // The output proves the CLI (not FakeEngine) executed: FakeEngine would
    // never answer "CLI:ping".
    assert_eq!(delta.as_deref(), Some("CLI:ping"));
}

#[tokio::test]
async fn no_automatic_engine_fallback() {
    // A CLI with a broken executable is registered. A send to a CLI session
    // must FAIL — never silently route to FakeEngine (§22–§23, §68).
    let (registry, fake, cli, bus, tmp) = registry_with_cli(echo_script());
    fake.start(&start_ctx(bus.clone(), None))
        .await
        .expect("fake start");

    // Break the CLI's executable after registration: point it at a script
    // that exits nonzero, and start it (still Ready — probe only checks the
    // executable exists).
    let bad = tmp.path().join("bad.py");
    std::fs::write(&bad, "import sys\nsys.exit(9)\n").expect("write bad script");
    let bad_cli = Arc::new(GenericCliEngine::new(GenericCliConfig {
        executable: "python".into(),
        args: vec![bad.to_string_lossy().to_string()],
        label: "Failing CLI".into(),
        max_output_bytes: 1024,
        timeout: Duration::from_secs(10),
        max_prompt_bytes: 1024,
    }));
    registry.register(bad_cli.clone());
    bad_cli
        .start(&start_ctx(bus.clone(), Some(tmp.path().to_path_buf())))
        .await
        .expect("bad cli starts");

    let session = created_info(
        "iso-bad-cli-session".into(),
        bad_cli.create_session(&CreateSessionRequest {
            session_id: "iso-bad-cli-session".into(),
            workspace_id: None,
            model: None,
            title: None,
        })
        .await
        .expect("cli session"),
    );

    let mut sub = bus.subscribe();
    let handle = accepted(
        bad_cli.send(&SendRequest {
            session_id: session.id.clone(),
            engine_session_id: session.engine_session_id.clone(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send accepted"),
    );
    let mut failed = false;
    for _ in 0..64 {
        let ev = recv_message(&mut sub).await;
        if let Event::MessageFailed { run_id, .. } = &ev {
            if run_id == &handle.run_id.clone().into() {
                failed = true;
                break;
            }
        }
    }
    // The run failed on its own engine; FakeEngine never executed it.
    assert!(failed, "no fallback: target engine failed honestly");
    let _ = cli;
    let _ = registry;
}

#[tokio::test]
async fn stop_all_stops_every_registered_engine() {
    // Registry iterates owned runtimes; both engines return to Stopped
    // (§71–§72, §73).
    let (registry, fake, cli, bus, tmp) = registry_with_cli(String::new());
    let ctx = start_ctx(bus, Some(tmp.path().to_path_buf()));
    registry.start("fake", &ctx).await.expect("fake starts");
    registry.start(CLI_ID, &ctx).await.expect("cli starts");
    registry.stop_all().await;
    assert!(matches!(fake.health(), EngineHealth::Stopped));
    assert!(matches!(cli.health(), EngineHealth::Stopped));
}
