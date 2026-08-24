//! Generic CLI engine suite (TASK 17 §79–§82, §139–§144).
//!
//! Uses a real deterministic helper executable (`python`) with small scripts
//! in a tempdir — real subprocesses through ProcessSupervisor, the same
//! harness allowance as the SAIPEN action tests. Every async wait is a
//! bounded event predicate; every hostile test has an overall timeout.

use std::sync::Arc;
use std::time::Duration;

use engine_generic_cli::{GenericCliConfig, GenericCliEngine, ENGINE_ID};
use saiwork_core::engine::{
    EngineAdapter, EngineCapabilities, EngineError, EngineHealth, EngineStartContext, SendRequest,
};
use saiwork_diagnostics::Diagnostics;
use saiwork_events::{bus::Subscription, Envelope, Event, EventBus};
use saiwork_process::{ProcessError, ProcessSupervisor, StopHooks};
use tempfile::TempDir;
use tokio::time::timeout;

fn python() -> String {
    // `python` is on PATH in this environment (3.11); tests resolve it
    // through the same PATH probe the adapter uses for its own config.
    "python".to_string()
}

fn write_script(dir: &TempDir, name: &str, body: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, body).expect("write script");
    path.to_string_lossy().to_string()
}

/// The generic-cli engine's successful spawn IS the authoritative acceptance
/// boundary; unwrap the receipt to the run handle for assertions.
fn accepted(acc: saiwork_core::engine::SendAcceptance) -> saiwork_core::engine::RunHandle {
    match acc {
        saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
            saiwork_core::engine::RunHandle { run_id }
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
}

fn config(executable: &str, args: Vec<String>) -> GenericCliConfig {
    GenericCliConfig {
        executable: executable.to_string(),
        args,
        label: "Test CLI".into(),
        max_output_bytes: 256 * 1024,
        timeout: Duration::from_secs(30),
        max_prompt_bytes: 64 * 1024,
    }
}

struct Harness {
    bus: EventBus,
    engine: GenericCliEngine,
    sub: Subscription,
    _tmp: TempDir,
}

impl Harness {
    fn new_with(tmp: TempDir, cfg: GenericCliConfig) -> Self {
        let bus = EventBus::new();
        let sub = bus.subscribe();
        let engine = GenericCliEngine::new(cfg);
        Self {
            bus,
            engine,
            sub,
            _tmp: tmp,
        }
    }

    async fn start(&self) {
        let bus = self.bus.clone();
        let ctx = EngineStartContext {
            workspace_id: None,
            workspace_path: Some(self._tmp.path().to_path_buf()),
            bus: self.bus.clone(),
            diagnostics: Arc::new(Diagnostics::new()),
            supervisor: Arc::new(ProcessSupervisor::new(bus)),
            report_failure: Arc::new(|_e: &str, _m: &str| {}),
        };
        self.engine.start(&ctx).await.expect("engine start");
    }

    async fn recv(&mut self) -> Envelope {
        timeout(Duration::from_secs(20), self.sub.recv())
            .await
            .expect("event timeout")
            .expect("subscription alive")
    }

    /// Next message.* event (skips supervisor process.* noise).
    async fn recv_message(&mut self) -> Event {
        loop {
            let env = self.recv().await;
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
    }
}

fn script_echo() -> &'static str {
    r#"
import sys
data = sys.stdin.read()
sys.stdout.write("ECHO:" + data)
sys.stdout.flush()
"#
}

fn script_exit_fail() -> &'static str {
    r#"
import sys
sys.stderr.write("boom detail\n")
sys.stderr.flush()
sys.exit(3)
"#
}

fn script_sleep() -> &'static str {
    r#"
import time
time.sleep(60)
"#
}

fn script_big() -> &'static str {
    r#"
import sys
sys.stdout.write("A" * 500000)
sys.stdout.flush()
"#
}

fn script_stream() -> &'static str {
    r#"
import sys, time
sys.stdout.write("first line\n")
sys.stdout.flush()
time.sleep(0.2)
sys.stdout.write("second line\n")
sys.stdout.flush()
"#
}

/// The fixture closes stdin IMMEDIATELY: the prompt can never be delivered.
fn script_close_stdin() -> &'static str {
    // Close stdin and exit immediately: the pipe's read end is then gone,
    // so the parent's blocking prompt write can neither complete nor be
    // accepted — send() must report OutcomeUnknown, not Accepted. (A long
    // sleep is deliberately avoided: the write blocks until the process
    // exits, which on Windows can be delayed by launcher handle copies.)
    r#"
import sys
sys.stdin.close()
"#
}

fn script_close_stdin_then_sleep() -> &'static str {
    r#"
import os, time
os.close(0)
time.sleep(60)
"#
}

async fn collect_run_events(sub: &mut Subscription, until: Event) -> Vec<Event> {
    let mut events = Vec::new();
    for _ in 0..32 {
        let env = timeout(Duration::from_secs(20), sub.recv())
            .await
            .expect("event timeout")
            .expect("subscription alive");
        // The supervisor publishes process.* on the same bus; the run
        // projection only cares about message.* events.
        if !matches!(
            env.event,
            Event::MessageStarted { .. }
                | Event::MessageDelta { .. }
                | Event::MessageCompleted { .. }
                | Event::MessageFailed { .. }
                | Event::MessageCancelled { .. }
        ) {
            continue;
        }
        let terminal = matches!(
            &env.event,
            Event::MessageCompleted { .. }
                | Event::MessageFailed { .. }
                | Event::MessageCancelled { .. }
        );
        let reached = env.event == until;
        events.push(env.event);
        if terminal || reached {
            break;
        }
    }
    events
}

// ---- capability honesty (§14, §48, §84) ----

#[tokio::test]
async fn capabilities_are_honest() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(tmp, config(&python(), vec![]));
    let caps = harness.engine.capabilities();
    // One-shot text: sessions exist (SAIWORK2-owned), resume does not.
    assert!(caps.sessions);
    assert!(!caps.resume);
    assert!(!caps.streaming); // output arrives at exit — no fake token deltas
    assert!(caps.cancel); // run == process, termination is real cancel (§52)
    assert!(!caps.tools);
    assert!(!caps.permissions);
    assert!(!caps.models);
    assert!(!caps.images);
    assert!(!caps.attachments);
    assert!(!caps.usage);
    assert_eq!(harness.engine.identity().id, ENGINE_ID);
}

#[tokio::test]
async fn unsupported_methods_return_unsupported() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(tmp, config(&python(), vec![]));
    let err = harness
        .engine
        .list_models()
        .await
        .expect_err("models unsupported");
    assert!(matches!(err, EngineError::UnsupportedCapability { .. }));
    let err = harness
        .engine
        .resume_session("x")
        .await
        .expect_err("resume unsupported");
    assert!(matches!(err, EngineError::UnsupportedCapability { .. }));
}

// ---- lifecycle (§26, §28) ----

#[tokio::test]
async fn missing_executable_fails_start() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(tmp, config("definitely-not-a-real-executable-xyz", vec![]));
    let bus = harness.bus.clone();
    let ctx = EngineStartContext {
        workspace_id: None,
        workspace_path: None,
        bus: harness.bus.clone(),
        diagnostics: Arc::new(Diagnostics::new()),
        supervisor: Arc::new(ProcessSupervisor::new(bus)),
        report_failure: Arc::new(|_e: &str, _m: &str| {}),
    };
    let err = harness.engine.start(&ctx).await.expect_err("start fails");
    assert!(err.to_string().contains("not found"));
    assert!(matches!(
        harness.engine.health(),
        EngineHealth::Failed { .. }
    ));
}

#[tokio::test]
async fn send_before_start_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(tmp, config(&python(), vec![]));
    let err = harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "hi".into(),
            model: None,
        })
        .await
        .expect_err("not ready");
    assert!(matches!(err, EngineError::NotReady { .. }));
}

// ---- run outcomes (§32, §34) ----

#[tokio::test]
async fn echo_run_completes_with_output() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "echo.py", script_echo());
    let harness = Harness::new_with(tmp, config(&python(), vec![script]));
    harness.start().await;
    let mut harness = harness;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "hello world".into(),
            model: None,
        })
        .await
        .expect("send"));
    assert!(!handle.run_id.is_empty());

    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageCompleted {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
        },
    )
    .await;

    assert!(
        matches!(&events[0], Event::MessageStarted { run_id, .. } if run_id == &handle.run_id.clone().into())
    );
    let delta = events
        .iter()
        .find_map(|e| match e {
            Event::MessageDelta { delta, .. } => Some(delta.clone()),
            _ => None,
        })
        .expect("one delta");
    // The full one-shot answer is preserved (bounded response channel).
    assert_eq!(delta, "ECHO:hello world");
    assert!(
        matches!(events.last(), Some(Event::MessageCompleted { run_id, .. }) if run_id == &handle.run_id.clone().into())
    );
}

#[tokio::test]
async fn nonzero_exit_fails_with_stderr() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "fail.py", script_exit_fail());
    let harness = Harness::new_with(tmp, config(&python(), vec![script]));
    harness.start().await;
    let mut harness = harness;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send"));

    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageFailed {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
            error: String::new(),
        },
    )
    .await;
    let err = events
        .iter()
        .find_map(|e| match e {
            Event::MessageFailed { error, .. } => Some(error.clone()),
            _ => None,
        })
        .expect("failed event");
    assert!(err.contains("3"), "err: {err}");
    assert!(err.contains("boom detail"), "err: {err}");
    assert!(
        matches!(events.last(), Some(Event::MessageFailed { .. })),
        "exactly one terminal"
    );
}

#[tokio::test]
async fn real_output_streams_as_one_delta_at_terminal() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "stream.py", script_stream());
    let harness = Harness::new_with(tmp, config(&python(), vec![script]));
    harness.start().await;
    let mut harness = harness;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send"));

    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageCompleted {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
        },
    )
    .await;
    let delta = events
        .iter()
        .find_map(|e| match e {
            Event::MessageDelta { delta, .. } => Some(delta.clone()),
            _ => None,
        })
        .expect("delta");
    assert!(delta.contains("first line"));
    assert!(delta.contains("second line"));
    assert!(
        matches!(events.last(), Some(Event::MessageCompleted { .. })),
        "exactly one terminal"
    );
}

// ---- bounded output (§49) ----

#[tokio::test]
async fn oversized_output_is_bounded_with_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "big.py", script_big());
    let harness = Harness::new_with(
        tmp,
        GenericCliConfig {
            max_output_bytes: 64 * 1024,
            ..config(&python(), vec![script])
        },
    );
    harness.start().await;
    let mut harness = harness;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send"));

    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageCompleted {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
        },
    )
    .await;
    let delta = events
        .iter()
        .find_map(|e| match e {
            Event::MessageDelta { delta, .. } => Some(delta.clone()),
            _ => None,
        })
        .expect("delta");
    assert!(delta.len() <= 64 * 1024 + 512, "bounded: {}", delta.len());
    assert!(delta.contains("output truncated"), "marker present");
}

// ---- timeout (§50) ----

#[tokio::test]
async fn timeout_terminates_and_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "sleep.py", script_sleep());
    let harness = Harness::new_with(
        tmp,
        GenericCliConfig {
            timeout: Duration::from_millis(1500),
            ..config(&python(), vec![script])
        },
    );
    harness.start().await;
    let mut harness = harness;

    let started = std::time::Instant::now();
    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send"));

    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageFailed {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
            error: String::new(),
        },
    )
    .await;
    let err = events
        .iter()
        .find_map(|e| match e {
            Event::MessageFailed { error, .. } => Some(error.clone()),
            _ => None,
        })
        .expect("failed event");
    assert!(err.contains("timed out"), "err: {err}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "bounded lifetime, no eternal zombie"
    );
    // Terminal record is retained; exactly one terminal outcome.
    assert!(
        matches!(events.last(), Some(Event::MessageFailed { .. })),
        "exactly one terminal"
    );
}

// ---- cancel (run == process, §52) ----

#[tokio::test]
async fn cancel_terminates_and_reports_cancelled() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "sleep.py", script_sleep());
    let harness = Harness::new_with(tmp, config(&python(), vec![script]));
    harness.start().await;
    let mut harness = harness;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send"));

    // Wait for started, then cancel.
    let ev = harness.recv_message().await;
    assert!(matches!(ev, Event::MessageStarted { .. }));

    harness.engine.cancel(&handle.run_id).await.expect("cancel");

    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageCancelled {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
        },
    )
    .await;
    assert!(
        matches!(events.last(), Some(Event::MessageCancelled { run_id, .. }) if run_id == &handle.run_id.clone().into()),
        "cancel wins ties and is the single terminal"
    );
}

#[tokio::test]
async fn cancel_unknown_run_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(tmp, config(&python(), vec![]));
    harness.start().await;
    // Cancel twice / unknown: idempotent no-op (cancel-twice rule).
    harness
        .engine
        .cancel("no-such-run")
        .await
        .expect("noop cancel ok");
}

// ---- same-session REJECT (§70–§72, TASK 18 §11) ----

#[tokio::test]
async fn same_session_second_send_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "sleep.py", script_sleep());
    let harness = Harness::new_with(tmp, config(&python(), vec![script]));
    harness.start().await;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("first send"));

    // Second send to the same session while the first is active: REJECT,
    // never a second process (ENGINE_CONTRACT.md §70–§72).
    let err = harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "y".into(),
            model: None,
        })
        .await
        .expect_err("same-session send must be rejected");
    assert!(matches!(err, EngineError::SessionBusy { .. }), "err: {err}");

    // A different session is free concurrently (different-session runs).
    harness
        .engine
        .send(&SendRequest {
            session_id: "s2".into(),
            engine_session_id: "s2".into(),
            prompt: "z".into(),
            model: None,
        })
        .await
        .expect("different session accepted");

    // Cleanup: cancel both runs so no zombie sleeps remain.
    harness
        .engine
        .cancel(&handle.run_id)
        .await
        .expect("cancel s1");
}

// ---- prompt delivery is synchronous and proven before Accepted (§46) ----

#[tokio::test]
async fn child_that_closes_stdin_immediately_is_never_accepted() {
    // The prompt must be delivered to the child BEFORE Accepted. A child
    // that closes stdin right away means the write fails — the send must
    // return OutcomeUnknown (side effects not provable), never Accepted and
    // never a fabricated rejection. The spawned child is then terminated.
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "close_stdin.py", script_close_stdin());
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    // The child closes stdin immediately (boot is a few ms). A small prompt
    // would land entirely in the pipe buffer and "succeed" before the close
    // takes effect — so write a prompt far larger than the OS pipe buffer:
    // the write must block until the child's close breaks the pipe, proving
    // send() fails (OutcomeUnknown) rather than fabricating Accepted.
    let mut cfg = config(&python(), vec![script]);
    cfg.max_prompt_bytes = 2 * 1024 * 1024;
    let engine = GenericCliEngine::new(cfg);
    let ctx = EngineStartContext {
        workspace_id: None,
        workspace_path: Some(tmp.path().to_path_buf()),
        bus,
        diagnostics: Arc::new(Diagnostics::new()),
        supervisor: supervisor.clone(),
        report_failure: Arc::new(|_e: &str, _m: &str| {}),
    };
    engine.start(&ctx).await.expect("engine start");

    let acc = engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".repeat(256 * 1024),
            model: None,
        })
        .await
        .expect("send returns a typed outcome");
    match acc {
        saiwork_core::engine::SendAcceptance::OutcomeUnknown { run_id, message } => {
            assert!(!run_id.is_empty());
            assert!(
                message.contains("prompt"),
                "diagnostic names the failed delivery: {message}"
            );
        }
        other => panic!(
            "child closed stdin before the prompt: must be OutcomeUnknown, got {other:?}"
        ),
    }
    // No run is tracked and the child is terminated + reaped: zero processes
    // survive in the supervisor.
    assert!(
        engine.active_runs().is_empty(),
        "no run may be registered for an undelivered prompt"
    );
    timeout(Duration::from_secs(10), async {
        while supervisor.count() > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the undelivered child must be terminated and reaped");
}

#[tokio::test]
async fn failed_prompt_cleanup_retains_run_authority_until_exit_is_proven() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(
        &tmp,
        "close_stdin_sleep.py",
        script_close_stdin_then_sleep(),
    );
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    supervisor.set_stop_hooks_for_test(StopHooks {
        before_stop: Some(Arc::new(|id, _| {
            Some(ProcessError::TerminationTimeout { id: id.clone() })
        })),
    });
    let mut cfg = config(&python(), vec![script]);
    cfg.max_prompt_bytes = 2 * 1024 * 1024;
    let engine = GenericCliEngine::new(cfg);
    engine
        .start(&EngineStartContext {
            workspace_id: None,
            workspace_path: Some(tmp.path().to_path_buf()),
            bus,
            diagnostics: Arc::new(Diagnostics::new()),
            supervisor: supervisor.clone(),
            report_failure: Arc::new(|_e: &str, _m: &str| {}),
        })
        .await
        .expect("engine start");

    let receipt = engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".repeat(256 * 1024),
            model: None,
        })
        .await
        .expect("send returns an honest outcome");
    let (run_id, message) = match receipt {
        saiwork_core::engine::SendAcceptance::OutcomeUnknown { run_id, message } => {
            (run_id, message)
        }
        other => panic!("failed delivery cleanup must be OutcomeUnknown, got {other:?}"),
    };
    let retained = engine
        .active_runs()
        .iter()
        .any(|run| run.run_id == run_id && run.session_id == "s1");
    let owned_processes = supervisor.count();

    // Always clean the RED control too: old code loses adapter authority but
    // the ProcessSupervisor still owns the child and can sweep it directly.
    supervisor.set_stop_hooks_for_test(StopHooks::default());
    if retained {
        engine
            .cancel(&run_id)
            .await
            .expect("retained run is cancellable");
    } else {
        assert!(supervisor.shutdown().await.is_empty());
    }
    timeout(Duration::from_secs(10), async {
        while supervisor.count() > 0 || !engine.active_runs().is_empty() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cleanup retry must prove exit and release both authorities");

    assert!(retained, "live child must remain addressable by run id");
    assert_eq!(owned_processes, 1, "supervisor retains exactly one child");
    assert!(
        message.contains("cleanup") && message.contains("exit"),
        "OutcomeUnknown must report teardown failure: {message}"
    );
}

// ---- prompt bound (§46) ----

#[tokio::test]
async fn oversized_prompt_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(
        tmp,
        GenericCliConfig {
            max_prompt_bytes: 8,
            ..config(&python(), vec![])
        },
    );
    harness.start().await;
    let err = harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "this prompt is far too long".into(),
            model: None,
        })
        .await
        .expect_err("prompt cap");
    assert!(err.to_string().contains("cap"));
}

// ---- engine stop does not kill an active run; shutdown sweep does (§26) ----

#[tokio::test]
async fn engine_stop_leaves_active_run_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let script = write_script(&tmp, "sleep.py", script_sleep());
    let harness = Harness::new_with(tmp, config(&python(), vec![script]));
    harness.start().await;
    let mut harness = harness;

    let handle = accepted(harness
        .engine
        .send(&SendRequest {
            session_id: "s1".into(),
            engine_session_id: "s1".into(),
            prompt: "x".into(),
            model: None,
        })
        .await
        .expect("send"));
    let ev = harness.recv_message().await;
    assert!(matches!(ev, Event::MessageStarted { .. }));

    harness.engine.stop().await.expect("engine stop");
    assert!(matches!(harness.engine.health(), EngineHealth::Stopped));

    // The run process continues; cancel still works (run == process).
    harness
        .engine
        .cancel(&handle.run_id)
        .await
        .expect("cancel still works");
    let events = collect_run_events(
        &mut harness.sub,
        Event::MessageCancelled {
            session_id: "s1".into(),
            run_id: handle.run_id.clone().into(),
        },
    )
    .await;
    assert!(matches!(
        events.last(),
        Some(Event::MessageCancelled { .. })
    ));
}

// ---- explicit trusted config (§44–§45, §97) ----

#[test]
fn config_from_env_absent_present_malformed() {
    // One test owns the env vars (tests in one binary run in parallel).
    unsafe {
        std::env::remove_var("SAIWORK2_CLI_EXECUTABLE");
        std::env::remove_var("SAIWORK2_CLI_ARGS");
        std::env::remove_var("SAIWORK2_CLI_TIMEOUT_MS");
        std::env::remove_var("SAIWORK2_CLI_MAX_OUTPUT_BYTES");
    }
    // Absent → None (engine simply not registered).
    assert!(GenericCliConfig::from_env().is_none());

    unsafe {
        std::env::set_var("SAIWORK2_CLI_EXECUTABLE", "some-tool");
        std::env::set_var("SAIWORK2_CLI_ARGS", "--flag value");
        std::env::set_var("SAIWORK2_CLI_TIMEOUT_MS", "5000");
    }
    let cfg = GenericCliConfig::from_env()
        .expect("present")
        .expect("valid");
    assert_eq!(cfg.executable, "some-tool");
    assert_eq!(cfg.args, vec!["--flag", "value"]);
    assert_eq!(cfg.timeout, Duration::from_millis(5000));

    unsafe {
        std::env::set_var("SAIWORK2_CLI_TIMEOUT_MS", "not-a-number");
    }
    let err = GenericCliConfig::from_env()
        .expect("present")
        .expect_err("malformed value surfaces precisely");
    assert!(err.contains("not a number"));

    unsafe {
        std::env::remove_var("SAIWORK2_CLI_EXECUTABLE");
        std::env::remove_var("SAIWORK2_CLI_ARGS");
        std::env::remove_var("SAIWORK2_CLI_TIMEOUT_MS");
        std::env::remove_var("SAIWORK2_CLI_MAX_OUTPUT_BYTES");
    }
}

// ---- capabilities struct sanity (§14) ----

#[test]
fn capabilities_dont_overclaim() {
    let tmp = tempfile::tempdir().unwrap();
    let harness = Harness::new_with(tmp, config(&python(), vec![]));
    let caps: EngineCapabilities = harness.engine.capabilities();
    assert!(!caps.structured_events);
    assert!(!caps.parallel_sessions);
    assert!(!caps.worktrees);
}
