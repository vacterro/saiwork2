//! SaipenService integration tests (TASK 14 §198–§219, §232–§233): real
//! fixture trees, real watcher, event semantics, read-only guarantee.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saiwork_events::{Event, EventBus};
use saiwork_saipen::SaipenService;

fn write(dir: &Path, name: &str, content: &str) {
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    std::fs::write(dir.join(".saipen").join(name), content).unwrap();
}

fn state_doc(phase: &str, task: &str, next: &str) -> String {
    format!(
        "---\nphase: {phase}\ntask: {task}\nnext_action: \"{next}\"\nblocker: \"\"\nschema_version: 3\nsaipen_version: 7\n---\n"
    )
}

fn board_doc(done: &[&str]) -> String {
    let mut s = String::from("## DOING\n\n## TODO\n\n## DONE\n");
    for t in done {
        s.push_str(&format!("- [x] {t} [P3] done\n"));
    }
    s.push_str("## BLOCKED\n");
    s
}

/// Wait for the service snapshot to satisfy `pred` (watcher is async).
async fn wait_for(
    svc: &Arc<SaipenService>,
    ws: &str,
    pred: impl Fn(&saiwork_saipen::SaipenSnapshot) -> bool,
) -> saiwork_saipen::SaipenSnapshot {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(s) = svc.snapshot(ws) {
            if pred(&s) {
                return s;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timeout waiting for snapshot condition"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn count_changed(bus: &EventBus) -> usize {
    let mut n = 0;
    let mut sub = bus.subscribe();
    while let Ok(Some(env)) = sub.try_recv() {
        if matches!(env.event, Event::SaipenChanged { .. }) {
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn attach_reads_snapshot_and_emits_detected() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "continue"),
    );
    write(dir.path(), "BOARD.md", &board_doc(&["T-1"]));
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    let mut sub = bus.subscribe();
    svc.attach("ws-test", dir.path());
    let snap = wait_for(&svc, "ws-test", |_| true).await;
    assert_eq!(snap.phase.as_deref(), Some("BUILD"));
    assert_eq!(snap.next_action.as_deref(), Some("continue"));
    assert_eq!(snap.board.counts.get("DONE"), Some(&1));
    // NotPresent → Present transition must emit saipen.detected once (§52).
    let mut detected = false;
    for _ in 0..16 {
        match sub.try_recv() {
            Ok(Some(env)) => {
                if matches!(env.event, Event::SaipenDetected { .. }) {
                    detected = true;
                }
            }
            _ => break,
        }
    }
    assert!(detected, "saipen.detected must be emitted on first attach");
    svc.detach("ws-test");
    assert_eq!(svc.active_count(), 0);
}

#[tokio::test]
async fn watcher_reflects_external_change_and_suppresses_unchanged_saves() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "continue"),
    );
    write(dir.path(), "BOARD.md", &board_doc(&["T-1"]));
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-test", dir.path());
    let snap = wait_for(&svc, "ws-test", |s| s.phase.as_deref() == Some("BUILD")).await;
    assert_eq!(snap.next_action.as_deref(), Some("continue"));

    // Semantic change: next_action changes.
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "changed!"),
    );
    let snap = wait_for(&svc, "ws-test", |s| {
        s.next_action.as_deref() == Some("changed!")
    })
    .await;
    assert_eq!(snap.phase.as_deref(), Some("BUILD"));

    // Unchanged save: same bytes rewritten → must NOT emit a change event.
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "changed!"),
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    let changed_since = count_changed(&bus);
    assert_eq!(
        changed_since, 0,
        "unchanged save must not emit saipen.changed"
    );

    svc.detach("ws-test");
}

#[tokio::test]
async fn snapshot_revision_advances_on_semantic_change_not_noop_save() {
    // P1: the snapshot `generation` is a SEMANTIC revision, decoupled from
    // the watch epoch. It advances only on meaningful change — so a
    // validation bound to revision N goes stale when STATE moves to N+1,
    // while a no-op atomic save preserves N and keeps the result current
    // (§87–§88).
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "continue"),
    );
    write(dir.path(), "BOARD.md", &board_doc(&["T-1"]));
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-rev", dir.path());
    let snap = wait_for(&svc, "ws-rev", |s| s.generation == 1).await;
    assert_eq!(snap.next_action.as_deref(), Some("continue"));
    let gen1 = snap.generation;

    // Semantic change: next_action changes → revision MUST advance.
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "changed!"),
    );
    let snap = wait_for(&svc, "ws-rev", |s| {
        s.next_action.as_deref() == Some("changed!")
    })
    .await;
    let gen2 = snap.generation;
    assert!(
        gen2 > gen1,
        "semantic change must advance the snapshot revision ({gen1} -> {gen2})"
    );

    // No-op atomic save (same bytes): revision MUST be preserved.
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "changed!"),
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    let snap = svc.snapshot("ws-rev").unwrap();
    assert_eq!(
        snap.generation, gen2,
        "no-op save must NOT advance the snapshot revision"
    );
    svc.detach("ws-rev");
}

#[tokio::test]
async fn detach_stops_updates_and_late_events_are_discarded() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "STATE.md", &state_doc("BUILD", "T-7", "a"));
    write(dir.path(), "BOARD.md", &board_doc(&[]));
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-test", dir.path());
    wait_for(&svc, "ws-test", |_| true).await;

    svc.detach("ws-test");
    assert!(
        svc.snapshot("ws-test").is_none(),
        "projection dropped on detach"
    );

    // Change after detach: no watcher should deliver, no crash, nothing
    // mutates (stale generation guard on top of the drop).
    write(dir.path(), "STATE.md", &state_doc("BUILD", "T-7", "b"));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(svc.snapshot("ws-test").is_none());
}

#[tokio::test]
async fn read_only_guarantee_files_untouched() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "continue"),
    );
    write(dir.path(), "BOARD.md", &board_doc(&["T-1"]));
    let state_before = std::fs::read(dir.path().join(".saipen/STATE.md")).unwrap();
    let board_before = std::fs::read(dir.path().join(".saipen/BOARD.md")).unwrap();
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-test", dir.path());
    wait_for(&svc, "ws-test", |_| true).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    svc.detach("ws-test");
    assert_eq!(
        std::fs::read(dir.path().join(".saipen/STATE.md")).unwrap(),
        state_before,
        "reader must never modify canonical files"
    );
    assert_eq!(
        std::fs::read(dir.path().join(".saipen/BOARD.md")).unwrap(),
        board_before
    );
    // No lock/residue files created by the reader.
    let names: Vec<String> = std::fs::read_dir(dir.path().join(".saipen"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 2, "reader created residue: {names:?}");
}

#[tokio::test]
async fn atomic_root_replacement_rereads_once_without_further_writes() {
    // TASK 24 audit: a root-replacement rebind must trigger one authoritative
    // reread — the replaced .saipen content must become visible even when NO
    // further filesystem write ever happens (the old code only refreshed on
    // the next event).
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "continue"),
    );
    write(dir.path(), "BOARD.md", &board_doc(&["T-1"]));
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-swap", dir.path());
    let snap = wait_for(&svc, "ws-swap", |s| s.phase.as_deref() == Some("BUILD")).await;
    assert_eq!(snap.next_action.as_deref(), Some("continue"));

    // Atomically replace the whole .saipen dir (temp + rename), then perform
    // NO further writes.
    let tmp = dir.path().join(".saipen.new");
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::write(tmp.join("STATE.md"), state_doc("PLAN", "T-9", "design")).unwrap();
    std::fs::write(tmp.join("BOARD.md"), board_doc(&[])).unwrap();
    let old = dir.path().join(".saipen.old");
    std::fs::rename(dir.path().join(".saipen"), &old).unwrap();
    std::fs::rename(&tmp, dir.path().join(".saipen")).unwrap();
    let _ = std::fs::remove_dir_all(&old);

    // The snapshot must update to the replacement content exactly once,
    // with no further writes required.
    let snap = wait_for(&svc, "ws-swap", |s| s.phase.as_deref() == Some("PLAN")).await;
    assert_eq!(snap.next_action.as_deref(), Some("design"));
    // And it must not flap back / keep firing: after a settle window the
    // snapshot is still the replacement (stable, no stale re-read).
    tokio::time::sleep(Duration::from_millis(700)).await;
    let snap = svc.snapshot("ws-swap").unwrap();
    assert_eq!(snap.phase.as_deref(), Some("PLAN"));
    assert_eq!(snap.next_action.as_deref(), Some("design"));
    svc.detach("ws-swap");
}

#[tokio::test]
async fn terminal_watch_failure_reports_failed_and_never_claims_live() {
    // TASK 24 audit: when every rebind attempt fails (the watched root is
    // gone), the watcher must report a terminal Failed signal and the
    // service must surface WatchStatus::Failed — a stale WatchHandle alone
    // must never keep the snapshot claiming Live.
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        &state_doc("BUILD", "T-7", "continue"),
    );
    write(dir.path(), "BOARD.md", &board_doc(&["T-1"]));
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-dead", dir.path());
    let snap = wait_for(&svc, "ws-dead", |s| {
        matches!(s.watch_status, saiwork_saipen::WatchStatus::Live)
    })
    .await;
    assert_eq!(snap.phase.as_deref(), Some("BUILD"));

    // Remove the watched root: every rebind attempt fails (nothing left to
    // watch), the retry budget is exhausted, and the watcher reports the
    // terminal failure.
    std::fs::remove_dir_all(dir.path().join(".saipen")).unwrap();

    let snap = wait_for(&svc, "ws-dead", |s| {
        matches!(s.watch_status, saiwork_saipen::WatchStatus::Failed(_))
    })
    .await;
    // The projection keeps the last good content but never claims Live
    // again — even though the (stale) WatchHandle still exists.
    assert_eq!(snap.phase.as_deref(), Some("BUILD"));
    tokio::time::sleep(Duration::from_millis(700)).await;
    let snap = svc.snapshot("ws-dead").unwrap();
    assert!(
        matches!(snap.watch_status, saiwork_saipen::WatchStatus::Failed(_)),
        "dead watch must stay Failed, never flip back to Live: {:?}",
        snap.watch_status
    );
    svc.detach("ws-dead");
}

#[tokio::test]
async fn absent_workspace_is_normal_state() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-plain", dir.path());
    assert!(svc.snapshot("ws-plain").is_none());
    assert_eq!(svc.active_count(), 0);
}

#[tokio::test]
async fn invalid_state_is_surfaced_not_fabricated() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "STATE.md", "this is not frontmatter at all\n");
    write(dir.path(), "BOARD.md", &board_doc(&[]));
    let bus = EventBus::new();
    let mut sub = bus.subscribe();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-bad", dir.path());
    // Must not fabricate a snapshot; the invalid state is a runtime warning.
    let mut warned = false;
    for _ in 0..16 {
        match sub.try_recv() {
            Ok(Some(env)) => {
                if matches!(env.event, Event::RuntimeWarning { code, .. } if code == "SAIPEN_INVALID")
                {
                    warned = true;
                }
            }
            _ => break,
        }
    }
    assert!(warned, "invalid SAIPEN must surface a typed warning");
    assert!(svc.snapshot("ws-bad").is_none());
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn slow_workspace_read_never_blocks_other_workspaces() {
    // TASK 24 perf: authoritative reads must run with NO service lock held
    // and off the async workers. A slow read of workspace A (failpoint
    // sleep on the blocking pool) must not block attach/snapshot/detach of
    // workspace B.
    let dir_a = tempfile::tempdir().unwrap();
    write(dir_a.path(), "STATE.md", &state_doc("BUILD", "T-A", "a"));
    write(dir_a.path(), "BOARD.md", &board_doc(&[]));
    let dir_b = tempfile::tempdir().unwrap();
    write(dir_b.path(), "STATE.md", &state_doc("PLAN", "T-B", "b"));
    write(dir_b.path(), "BOARD.md", &board_doc(&[]));

    saiwork_saipen::test_hooks::clear();
    // Workspace A reads are genuinely slow (400 ms each).
    saiwork_saipen::test_hooks::set_read_slow(dir_a.path(), Duration::from_millis(400));

    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-a", dir_a.path());
    let snap_a = wait_for(&svc, "ws-a", |_| true).await;
    assert_eq!(snap_a.phase.as_deref(), Some("BUILD"));

    // Trigger a watcher refresh of A: after the debounce window its slow
    // read starts on the blocking pool and stays in flight for 400 ms.
    write(
        dir_a.path(),
        "STATE.md",
        &state_doc("PLAN", "T-A", "a2"),
    );
    tokio::time::sleep(Duration::from_millis(420)).await;

    // While A's refresh read is blocked, B must attach/detach/snapshot
    // immediately — the service lock is NOT held during the read.
    let start = std::time::Instant::now();
    svc.attach("ws-b", dir_b.path());
    let attach_ms = start.elapsed().as_millis();
    assert!(
        attach_ms < 200,
        "attach of B must not wait for A's slow read (took {attach_ms} ms)"
    );
    let snap_b = svc.snapshot("ws-b").unwrap();
    assert_eq!(snap_b.phase.as_deref(), Some("PLAN"));
    svc.detach("ws-b");
    assert!(svc.snapshot("ws-b").is_none());

    // A's blocked read eventually completes and commits the new phase.
    let snap_a = wait_for(&svc, "ws-a", |s| s.phase.as_deref() == Some("PLAN")).await;
    assert_eq!(snap_a.next_action.as_deref(), Some("a2"));
    svc.detach("ws-a");
    saiwork_saipen::test_hooks::clear();
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn change_storm_yields_one_coalesced_reread_and_no_stale_commits() {
    // TASK 24 perf: a 100-event storm must produce ONE coalesced
    // authoritative reread per quiet window (watcher debounce + service
    // single-reader coalescing), and the final projection must be exactly
    // the LAST write — never overwritten by an older read (zero stale
    // commits).
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "STATE.md", &state_doc("BUILD", "T-1", "a"));
    write(dir.path(), "BOARD.md", &board_doc(&[]));
    saiwork_saipen::test_hooks::clear();
    let bus = EventBus::new();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-storm", dir.path());
    wait_for(&svc, "ws-storm", |_| true).await;
    assert_eq!(
        saiwork_saipen::test_hooks::read_count(dir.path()),
        1,
        "attach must perform exactly one full consistency read"
    );

    // 100 rapid writes (a storm). All within one debounce window.
    for i in 0..100 {
        write(
            dir.path(),
            "STATE.md",
            &state_doc("BUILD", "T-1", &format!("v{i}")),
        );
    }
    // The authoritative projection must converge to the LAST write.
    let snap = wait_for(&svc, "ws-storm", |s| {
        s.next_action.as_deref() == Some("v99")
    })
    .await;
    assert_eq!(snap.phase.as_deref(), Some("BUILD"));
    // Settle: no later stale commit may overwrite v99 with an earlier read.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let snap = svc.snapshot("ws-storm").unwrap();
    assert_eq!(snap.next_action.as_deref(), Some("v99"));
    let reads = saiwork_saipen::test_hooks::read_count(dir.path());
    assert!(
        reads <= 4,
        "100-event storm must coalesce to a handful of reads, got {reads}"
    );
    svc.detach("ws-storm");
    saiwork_saipen::test_hooks::clear();
}

#[tokio::test]
async fn unsupported_schema_is_rejected_not_parsed() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "STATE.md",
        "---\nschema_version: 99\nphase: BUILD\n---\n",
    );
    write(dir.path(), "BOARD.md", &board_doc(&[]));
    let bus = EventBus::new();
    let mut sub = bus.subscribe();
    let svc = SaipenService::new(bus.clone());
    svc.attach("ws-future", dir.path());
    let mut warned = false;
    for _ in 0..16 {
        match sub.try_recv() {
            Ok(Some(env)) => {
                if matches!(env.event, Event::RuntimeWarning { code, .. } if code == "SAIPEN_UNSUPPORTED")
                {
                    warned = true;
                }
            }
            _ => break,
        }
    }
    assert!(
        warned,
        "unsupported schema must be surfaced, never parsed as current"
    );
    assert!(svc.snapshot("ws-future").is_none());
}
