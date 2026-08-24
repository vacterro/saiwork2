//! ProcessSupervisor integration tests (TASK 06 §48–§67).
//!
//! All tests drive the deterministic `proc_fixture` binary (found via
//! `CARGO_BIN_EXE_proc_fixture`), never random external programs.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use saiwork_events::{Event, EventBus};
use saiwork_process::{
    is_pid_alive, ExitInfo, ManagedProcess, ProcessError, ProcessSpec, ProcessState,
    ProcessSupervisor,
};
use tokio::time::{sleep, timeout};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_proc_fixture"))
}

/// A spec with short, test-friendly termination deadlines.
fn spec(id: &str, args: &[&str]) -> ProcessSpec {
    let mut spec = ProcessSpec::new(id.to_string(), fixture().to_string_lossy().to_string());
    spec.args = args.iter().map(|s| s.to_string()).collect();
    spec.exit_wait_timeout = Duration::from_secs(1);
    spec.kill_timeout = Duration::from_secs(2);
    spec
}

async fn wait_exit(p: &Arc<ManagedProcess>, secs: u64) -> ExitInfo {
    let mut rx = p.exit();
    timeout(Duration::from_secs(secs), async {
        loop {
            if let Some(info) = *rx.borrow() {
                return info;
            }
            if rx.changed().await.is_err() {
                panic!("exit sender dropped");
            }
        }
    })
    .await
    .expect("timed out waiting for process exit")
}

/// Poll the captured stdout for the fixture's `CHILD_PID=<pid>` line.
async fn wait_for_child_pid(p: &Arc<ManagedProcess>) -> u32 {
    timeout(Duration::from_secs(10), async {
        loop {
            for line in p.stdout() {
                if let Some(rest) = line.strip_prefix("CHILD_PID=") {
                    return rest.trim().parse().expect("valid pid");
                }
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("child pid never appeared")
}

// ---- lifecycle -----------------------------------------------------------

#[tokio::test]
async fn spawn_runs_then_exits_with_code_and_ordered_events() {
    let bus = EventBus::new();
    let mut sub = bus.subscribe();
    let sup = ProcessSupervisor::new(bus);

    let p = sup.spawn(spec("exit0", &["--exit", "0"])).await.unwrap();
    assert_eq!(p.state(), ProcessState::Running);
    assert!(p.pid() > 0);
    assert_eq!(sup.count(), 1);

    let info = wait_exit(&p, 10).await;
    assert_eq!(info.code, Some(0));
    assert!(!info.signaled);
    assert_eq!(p.state(), ProcessState::Exited);

    // Terminal event order: process.started → process.exited, seq increasing.
    let mut started_seq = None;
    let mut exited_seq = None;
    while started_seq.is_none() || exited_seq.is_none() {
        let env = sub.recv().await.unwrap();
        match &env.event {
            Event::ProcessStarted { process_id, pid } => {
                assert_eq!(process_id.as_str(), "exit0");
                assert_eq!(*pid, p.pid());
                started_seq = Some(env.seq);
            }
            Event::ProcessExited {
                process_id,
                pid,
                code,
                ..
            } => {
                assert_eq!(process_id.as_str(), "exit0");
                assert_eq!(*pid, p.pid());
                assert_eq!(*code, Some(0));
                exited_seq = Some(env.seq);
            }
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert!(started_seq < exited_seq);

    // Exited records leave the registry (bounded registry).
    assert_eq!(sup.count(), 0);
}

#[tokio::test]
async fn spawn_failure_leaves_no_record_and_publishes_failed() {
    let bus = EventBus::new();
    let mut sub = bus.subscribe();
    let sup = ProcessSupervisor::new(bus);

    let mut spec = spec("missing", &[]);
    spec.command = "definitely-not-a-real-executable-xyz".into();
    let err = sup.spawn(spec).await.unwrap_err();
    assert!(
        matches!(err, ProcessError::CommandNotFound { .. }),
        "{err:?}"
    );
    assert_eq!(sup.count(), 0, "no registry garbage on spawn failure");
    let env = sub.recv().await.unwrap();
    assert!(matches!(
        env.event,
        Event::ProcessFailed { ref process_id, .. } if process_id.as_str() == "missing"
    ));
}

#[tokio::test]
async fn invalid_cwd_rejected_before_spawn() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let mut spec = spec("badcwd", &["--exit", "0"]);
    spec.cwd = Some(PathBuf::from(r"C:\definitely-not-a-real-dir-xyz"));
    let err = sup.spawn(spec).await.unwrap_err();
    assert!(matches!(err, ProcessError::BadCwd { .. }));
    assert_eq!(sup.count(), 0);
}

#[tokio::test]
async fn duplicate_id_rejected() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup.spawn(spec("dup", &["--exit", "0"])).await.unwrap();
    let _ = wait_exit(&p, 10).await;
    // Id is still taken while the record is live; use a second running one.
    let p2 = sup.spawn(spec("dup2", &["--sleep", "30"])).await.unwrap();
    let err = sup
        .spawn(spec("dup2", &["--sleep", "30"]))
        .await
        .unwrap_err();
    assert!(matches!(err, ProcessError::DuplicateId { .. }));
    let _ = sup.stop(&p2, false).await;
}

// ---- output ---------------------------------------------------------------

#[tokio::test]
async fn stdout_and_stderr_captured_separately() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup
        .spawn(spec(
            "out",
            &["--echo-out", "HELLO_OUT", "--echo-err", "HELLO_ERR"],
        ))
        .await
        .unwrap();
    let _ = wait_exit(&p, 10).await;

    assert!(p.stdout().iter().any(|l| l.contains("HELLO_OUT")));
    assert!(!p.stdout().iter().any(|l| l.contains("HELLO_ERR")));
    assert!(p.stderr().iter().any(|l| l.contains("HELLO_ERR")));
    assert!(!p.stderr().iter().any(|l| l.contains("HELLO_OUT")));
}

#[tokio::test]
async fn large_output_stays_bounded() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup
        .spawn(spec("spam", &["--spam-out", "200000"]))
        .await
        .unwrap();
    let _ = wait_exit(&p, 30).await;

    // Memory stays bounded; overflow is deterministic (oldest dropped).
    assert!(
        p.output_bytes() <= saiwork_process::OUTPUT_CAP_BYTES + 8192,
        "output_bytes={}",
        p.output_bytes()
    );
    assert!(p.dropped_lines() > 0, "overflow must drop oldest lines");
}

#[tokio::test]
async fn partial_output_is_preserved_not_lost() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup.spawn(spec("partial", &["--partial"])).await.unwrap();
    let _ = wait_exit(&p, 10).await;
    let joined = p.stdout().join("\n");
    // "abc" (no newline) + "def\n" → one complete line "abcdef": all bytes
    // delivered exactly once, never split or lost (§20/§53).
    assert!(joined.contains("abcdef"), "partial output lost: {joined:?}");
}

#[tokio::test]
async fn invalid_utf8_does_not_panic_and_is_lossy() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup.spawn(spec("raw", &["--raw-bytes"])).await.unwrap();
    let _ = wait_exit(&p, 10).await;
    assert_eq!(p.state(), ProcessState::Exited);
    assert!(
        p.stdout().iter().any(|l| l.contains('\u{FFFD}')),
        "invalid bytes must become U+FFFD, not panic: {:?}",
        p.stdout()
    );
}

#[tokio::test]
async fn non_zero_exit_codes_are_captured_not_errors() {
    let sup = ProcessSupervisor::new(EventBus::new());
    for (id, code) in [("code7", 7), ("code3", 3)] {
        let p = sup
            .spawn(spec(id, &["--exit", &code.to_string()]))
            .await
            .unwrap();
        let info = wait_exit(&p, 10).await;
        assert_eq!(info.code, Some(code), "{id} exit code");
        assert_eq!(p.state(), ProcessState::Exited, "{id} state");
    }
}

// ---- termination ----------------------------------------------------------

#[tokio::test]
async fn graceful_stop_is_bounded() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup
        .spawn(spec("graceful", &["--sleep", "30"]))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let info = sup.stop(&p, true).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(15));
    assert_eq!(p.state(), ProcessState::Exited);
    assert!(p.has_exited());
    let _ = info;
}

#[tokio::test]
async fn force_kill_terminates() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup.spawn(spec("force", &["--sleep", "30"])).await.unwrap();
    let info = sup.stop(&p, false).await.unwrap();
    assert!(p.has_exited());
    assert_eq!(p.state(), ProcessState::Exited);
    let _ = info;
}

#[tokio::test]
async fn double_stop_is_safe_and_second_is_non_destructive() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup.spawn(spec("dbl", &["--sleep", "30"])).await.unwrap();
    sup.stop(&p, false).await.unwrap();
    let err = sup.stop(&p, false).await.unwrap_err();
    assert!(matches!(err, ProcessError::NotRunning { .. }), "{err:?}");
    assert_eq!(p.state(), ProcessState::Exited);
}

#[tokio::test]
async fn stop_after_natural_exit_returns_not_running() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup.spawn(spec("done", &["--exit", "0"])).await.unwrap();
    let _ = wait_exit(&p, 10).await;
    let err = sup.stop(&p, true).await.unwrap_err();
    assert!(matches!(err, ProcessError::NotRunning { .. }));
}

#[tokio::test]
async fn exit_vs_stop_race_resolves_without_false_errors() {
    let sup = ProcessSupervisor::new(EventBus::new());
    for i in 0..15 {
        let id = format!("race{i}");
        let p = sup.spawn(spec(&id, &["--sleep", "0.1"])).await.unwrap();
        let res = sup.stop(&p, true).await;
        match res {
            Ok(_) => {}
            Err(ProcessError::NotRunning { .. }) => {}
            Err(other) => panic!("race {i} produced a false error: {other:?}"),
        }
        assert!(p.has_exited(), "race {i} must end terminated");
    }
}

#[tokio::test]
async fn concurrent_processes_are_isolated() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let a = sup.spawn(spec("a", &["--sleep", "30"])).await.unwrap();
    let b = sup
        .spawn(spec("b", &["--echo-out", "MARKER_B", "--exit", "0"]))
        .await
        .unwrap();
    let c = sup.spawn(spec("c", &["--sleep", "30"])).await.unwrap();

    assert_ne!(a.pid(), b.pid());
    assert_ne!(b.pid(), c.pid());
    assert_eq!(sup.count(), 3);

    // B exits naturally (--exit 0) with its own output; wait for it first so
    // the isolation assertions below are about A and C only.
    let info = wait_exit(&b, 10).await;
    assert_eq!(info.code, Some(0));
    assert!(b.stdout().iter().any(|l| l.contains("MARKER_B")));
    assert!(a.stdout().iter().all(|l| !l.contains("MARKER_B")));

    // Stopping A must not touch C: separate records, separate trees.
    sup.stop(&a, false).await.unwrap();
    assert!(a.has_exited());
    assert!(!c.has_exited(), "stopping A killed unrelated process C");
    assert_eq!(c.state(), ProcessState::Running);

    sup.stop(&c, false).await.unwrap();
    assert!(c.has_exited());
    assert_eq!(sup.count(), 0);
}

// ---- process tree (Windows Job Object / unix group) -----------------------

#[tokio::test]
async fn killing_parent_tree_kills_descendants() {
    let sup = ProcessSupervisor::new(EventBus::new());
    let p = sup
        .spawn(spec("tree", &["--child-sleep", "30"]))
        .await
        .unwrap();
    let child_pid = wait_for_child_pid(&p).await;
    assert!(is_pid_alive(child_pid), "child should be alive");

    sup.stop(&p, false).await.unwrap();
    assert!(p.has_exited(), "parent must be gone");

    // The descendant must die with the parent's tree (poll for the OS to
    // reap; bounded, never forever).
    timeout(Duration::from_secs(10), async {
        while is_pid_alive(child_pid) {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("descendant survived the parent's tree kill — orphan leak");
}

// ---- shutdown --------------------------------------------------------------

#[tokio::test]
async fn shutdown_rejects_new_spawn_and_clears_all() {
    let sup = ProcessSupervisor::new(EventBus::new());
    for i in 0..3 {
        sup.spawn(spec(&format!("s{i}"), &["--sleep", "30"]))
            .await
            .unwrap();
    }
    assert_eq!(sup.count(), 3);

    let forced = sup.shutdown().await;
    assert_eq!(sup.count(), 0, "registry must be empty after shutdown");
    assert!(
        forced.is_empty(),
        "clean shutdown must not report survivors: {forced:?}"
    );

    let err = sup.spawn(spec("late", &["--exit", "0"])).await.unwrap_err();
    assert!(matches!(err, ProcessError::ShuttingDown));
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let sup = ProcessSupervisor::new(EventBus::new());
    sup.spawn(spec("x", &["--sleep", "30"])).await.unwrap();
    let _ = sup.shutdown().await;
    let _ = sup.shutdown().await; // second call is safe, no resurrect
    assert_eq!(sup.count(), 0);
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn shutdown_retains_authority_when_final_force_cannot_prove_exit() {
    use saiwork_process::StopHooks;

    let sup = ProcessSupervisor::new(EventBus::new());
    let process = sup
        .spawn(spec("force-survivor", &["--sleep", "30"]))
        .await
        .unwrap();
    sup.set_stop_hooks_for_test(StopHooks {
        before_stop: Some(Arc::new(|id, _graceful| {
            Some(ProcessError::TerminationTimeout { id: id.clone() })
        })),
    });

    let survivors = sup.shutdown().await;
    let retained = sup.get(process.id()).is_some();
    let registry_count = sup.count();
    let exit_unproven = !process.has_exited();

    // Keep the hostile test itself resource-safe after recording the product
    // outcome: shutdown is idempotent and retries termination of every
    // retained survivor even though new spawn admission remains closed.
    sup.set_stop_hooks_for_test(StopHooks::default());
    let retry_survivors = if exit_unproven {
        sup.shutdown().await
    } else {
        Vec::new()
    };

    assert_eq!(survivors, vec!["force-survivor"]);
    assert!(retained, "shutdown discarded live process authority");
    assert_eq!(registry_count, 1, "the survivor must remain registered");
    assert!(
        exit_unproven,
        "the force failpoint must leave exit unproven"
    );
    assert!(
        retry_survivors.is_empty(),
        "retry must terminate the survivor"
    );
    assert_eq!(sup.count(), 0, "retry must release proven-exited authority");
}

#[tokio::test]
async fn repeated_start_stop_returns_registry_to_zero() {
    let sup = ProcessSupervisor::new(EventBus::new());
    for i in 0..10 {
        let id = format!("cycle{i}");
        let p = sup.spawn(spec(&id, &["--exit", "0"])).await.unwrap();
        let _ = wait_exit(&p, 10).await;
        assert_eq!(
            sup.count(),
            0,
            "registry must return to zero after cycle {i}"
        );
    }
    assert_eq!(sup.count(), 0);
}

// ---- shutdown × spawn admission race (TASK 24 §9) --------------------------

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_parked_between_admission_and_registration_is_never_orphaned() {
    use saiwork_process::SpawnHooks;
    use std::sync::{Condvar, Mutex as StdMutex};

    let bus = EventBus::new();
    let sup = Arc::new(ProcessSupervisor::new(bus));
    // Barrier: the hook parks the spawn in the admission→registration window
    // (child exists, job assigned, not yet registered) until the test opens
    // the gate. Shutdown starts while the spawn is parked.
    let gate = Arc::new((StdMutex::new(false), Condvar::new()));
    let gate2 = gate.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered2 = entered.clone();
    sup.set_spawn_hooks_for_test(SpawnHooks {
        before_register: Some(Arc::new(move || {
            entered2.notify_one();
            let (lock, cv) = &*gate2;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
        })),
    });

    let sup_spawn = sup.clone();
    let spawn_task = tokio::spawn(async move {
        sup_spawn.spawn(spec("race", &["--child-sleep", "30"])).await
    });
    // Wait until the spawn is parked after admission but before registration.
    entered.notified().await;

    // Shutdown begins with the spawn in flight: it closes admission, then
    // waits for in-flight spawns to register or abort before the stop pass.
    let sup_shutdown = sup.clone();
    let shutdown_task = tokio::spawn(async move { sup_shutdown.shutdown().await });
    tokio::time::sleep(Duration::from_millis(100)).await; // let shutdown drain

    // Release the parked spawn. It observes shutdown and must kill/reap its
    // own child (or register and be stopped by the sweep) — never survive.
    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    let spawn_result = timeout(Duration::from_secs(10), spawn_task)
        .await
        .expect("spawn must settle")
        .expect("spawn task panicked");
    let forced = timeout(Duration::from_secs(15), shutdown_task)
        .await
        .expect("shutdown must settle")
        .expect("shutdown task panicked");

    // Either the spawn was rejected (ShuttingDown — child reaped by the
    // re-check, job-close kills descendants) or it registered and shutdown
    // stopped it. Either way the final registry is empty.
    match spawn_result {
        Err(ProcessError::ShuttingDown) => {}
        Ok(p) => {
            assert!(
                p.has_exited(),
                "a spawn registered during shutdown must be stopped by the sweep"
            );
        }
        Err(other) => panic!("unexpected spawn error: {other:?}"),
    }
    assert_eq!(sup.count(), 0, "no process may survive shutdown");
    assert!(
        forced.len() <= 1,
        "force list must only contain real survivors: {forced:?}"
    );

    // Admission is closed afterwards.
    let err = sup.spawn(spec("late", &["--exit", "0"])).await.unwrap_err();
    assert!(matches!(err, ProcessError::ShuttingDown));
}

/// Duplicate ProcessId is reserved ATOMICALLY before any OS spawn (TASK 24
/// §9): a second same-id spawn is rejected with DuplicateId while the first
/// is still in the admission→registration window — it never creates a child.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_id_spawn_is_rejected_before_child_creation() {
    use saiwork_process::SpawnHooks;
    use std::sync::{Condvar, Mutex as StdMutex};

    let bus = EventBus::new();
    let sup = Arc::new(ProcessSupervisor::new(bus));
    // Barrier: the first spawn parks after creating its child but before
    // registration — the exact window where the duplicate check used to be
    // racy. The second same-id spawn must be rejected there.
    let gate = Arc::new((StdMutex::new(false), Condvar::new()));
    let gate2 = gate.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered2 = entered.clone();
    sup.set_spawn_hooks_for_test(SpawnHooks {
        before_register: Some(Arc::new(move || {
            entered2.notify_one();
            let (lock, cv) = &*gate2;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
        })),
    });

    let sup_spawn = sup.clone();
    let first = tokio::spawn(async move {
        sup_spawn.spawn(spec("dup", &["--child-sleep", "30"])).await
    });
    entered.notified().await; // first spawn is parked, child exists

    // Second same-id spawn while the first holds the reservation: rejected
    // with DuplicateId BEFORE any OS spawn (no barrier fires for it).
    let second = sup.spawn(spec("dup", &["--child-sleep", "30"])).await;
    assert!(
        matches!(second, Err(ProcessError::DuplicateId { .. })),
        "second same-id spawn must be DuplicateId, got {second:?}"
    );

    // Release the first spawn; it registers normally.
    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    let first = timeout(Duration::from_secs(10), first)
        .await
        .expect("first spawn must settle")
        .expect("spawn task panicked")
        .expect("first spawn must succeed");
    assert_eq!(sup.count(), 1, "exactly one registry entry for the id");
    let child_pid = wait_for_child_pid(&first).await;

    // Stop the winner and prove zero descendants survive.
    sup.stop(&first, false).await.unwrap();
    assert!(first.has_exited());
    // The Job Object takes the descendant with the parent.
    timeout(Duration::from_secs(5), async {
        loop {
            if !is_pid_alive(child_pid) {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("descendant survived the winner's tree kill — a hidden second child would be an orphan");
    timeout(Duration::from_secs(5), async {
        while sup.count() != 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("proven-exited duplicate winner never left the registry");
}

/// A reader blocked on bounded protocol delivery must be ABORTED + awaited on
/// drain timeout — never detached (a detached task keeps its sender alive and
/// retains process-owned Arcs forever). Observable proof: after the process is
/// removed, the receiver must drain to `None` (all senders dropped). If the
/// reader were merely detached, the sender would stay alive and `recv` would
/// block forever.
#[tokio::test]
async fn output_drain_timeout_aborts_blocked_protocol_reader() {
    let bus = EventBus::new();
    let sup = ProcessSupervisor::new(bus);
    let mut s = spec("drain", &["--spam-out", "100000"]);
    s.stdout_protocol = true;
    // Tiny bounded channel: the reader fills it quickly and blocks on send
    // while the child's stdout backpressures.
    s.protocol_channel_messages = 4;
    s.exit_wait_timeout = Duration::from_secs(1);
    s.kill_timeout = Duration::from_secs(6);
    let p = sup.spawn(s).await.unwrap();
    // Take the protocol stream and NEVER poll it: the reader fills the
    // bounded channel then blocks; the child blocks writing.
    let mut rx = p.protocol_stream().expect("protocol stream");
    sleep(Duration::from_millis(300)).await; // let the pipe/channel fill

    // The child is wedged writing; force-kill it so the monitor sees exit
    // while the reader is still blocked on the un-polled channel.
    sup.stop(&p, false).await.unwrap();
    assert!(p.has_exited());

    // The monitor's OUTPUT_DRAIN_TIMEOUT expires; it must abort + await the
    // reader. The aborted reader drops its sender → recv drains to None.
    timeout(Duration::from_secs(8), async {
        loop {
            match rx.recv().await {
                Some(_) => continue, // queued chunks still draining
                None => break,
            }
        }
    })
    .await
    .expect("reader was detached, not aborted: its sender stays alive forever");

    // The registry entry is gone (bounded registry).
    timeout(Duration::from_secs(5), async {
        loop {
            if sup.count() == 0 {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("registry never drained after the aborted reader");
}

// ---- protocol capture policy (TASK 24 perf) ------------------------------

/// Expected exact stdout bytes of `--spam-out N` (ASCII "line i\n" lines).
fn spam_expected(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * 8);
    for i in 0..n {
        out.extend_from_slice(format!("line {i}\n").as_bytes());
    }
    out
}

/// Drain a protocol stream to its end, concatenating exact raw bytes.
async fn drain_protocol(p: &Arc<ManagedProcess>) -> Vec<u8> {
    let mut rx = p.protocol_stream().expect("protocol stream available");
    let mut all = Vec::new();
    while let Some(chunk) = rx.recv().await {
        all.extend_from_slice(&chunk);
    }
    all
}

/// Protocol mode WITHOUT stdout diagnostics: raw bytes are delivered
/// byte-identical while the lossy line-ring path is skipped entirely (no
/// dual text processing for machine traffic).
#[tokio::test]
async fn protocol_mode_skips_stdout_line_ring() {
    let bus = EventBus::new();
    let sup = ProcessSupervisor::new(bus);
    let mut s = spec("proto", &["--spam-out", "2000"]);
    s.stdout_protocol = true;
    s.protocol_stdout_diagnostics = false;
    s.exit_wait_timeout = Duration::from_secs(2);
    s.kill_timeout = Duration::from_secs(2);

    let p = sup.spawn(s).await.unwrap();
    let raw = drain_protocol(&p).await;
    let info = wait_exit(&p, 10).await;
    assert_eq!(info.code, Some(0));

    // Byte-identical raw delivery.
    assert_eq!(raw, spam_expected(2000));
    // No lossy line-ring capture in protocol mode (explicit diagnostic mode
    // off) — protocol traffic never pays the second text-processing path.
    assert!(p.stdout().is_empty(), "stdout ring must stay empty in protocol mode");
    // stderr diagnostics are always captured.
    assert!(p.stderr().is_empty());
    assert_eq!(sup.count(), 0);
}

/// Explicit diagnostic mode: raw bytes STILL arrive byte-identical AND the
/// lossy line ring is additionally kept for humans.
#[tokio::test]
async fn protocol_mode_with_diagnostics_keeps_ring_and_raw() {
    let bus = EventBus::new();
    let sup = ProcessSupervisor::new(bus);
    let mut s = spec("proto-diag", &["--spam-out", "500"]);
    s.stdout_protocol = true;
    s.protocol_stdout_diagnostics = true;
    s.exit_wait_timeout = Duration::from_secs(2);
    s.kill_timeout = Duration::from_secs(2);

    let p = sup.spawn(s).await.unwrap();
    let raw = drain_protocol(&p).await;
    let info = wait_exit(&p, 10).await;
    assert_eq!(info.code, Some(0));

    assert_eq!(raw, spam_expected(500));
    let lines = p.stdout();
    assert_eq!(lines.len(), 500, "diagnostic ring holds every line");
    assert_eq!(lines[0], "line 0");
    assert_eq!(lines[499], "line 499");
    assert_eq!(sup.count(), 0);
}

/// Independent processes stop CONCURRENTLY (TASK 24 perf): with serial stops,
/// N hung owners each consume their full graceful budget before the next
/// even receives a stop. Concurrent stop_all must approach ONE budget.
#[tokio::test]
async fn stop_all_concurrent_near_one_budget() {
    let sup = ProcessSupervisor::new(EventBus::new());
    // 4 stubborn processes: sleeping far beyond any stop budget, so each
    // graceful attempt burns its full exit_wait_timeout before the force.
    let mut handles = Vec::new();
    for i in 0..4 {
        let mut s = spec(&format!("stub{i}"), &["--sleep", "30"]);
        s.exit_wait_timeout = Duration::from_millis(1000);
        s.kill_timeout = Duration::from_secs(2);
        handles.push(sup.spawn(s).await.unwrap());
    }
    assert_eq!(sup.count(), 4);

    let started = std::time::Instant::now();
    let forced = sup.stop_all().await;
    let elapsed = started.elapsed();

    // Every process was stopped (force succeeded → not in forced).
    assert!(forced.is_empty(), "all stops should have force-succeeded: {forced:?}");
    for p in &handles {
        assert!(p.has_exited(), "process {} must be terminated", p.id());
    }
    // Serial stops would take >= 4 × 1s graceful budgets; concurrent must be
    // far below that — near a single owner budget.
    assert!(
        elapsed < Duration::from_secs(3),
        "stop_all took {elapsed:?} — stops were serialized, not concurrent"
    );
    // Registry drains as the monitors reap the exits.
    timeout(Duration::from_secs(10), async {
        loop {
            if sup.count() == 0 {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("registry never drained after stop_all");
}
