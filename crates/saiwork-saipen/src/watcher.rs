//! Filesystem watcher for one SAIPEN root (TASK 14 §30–§38, §42).
//!
//! - **One owner per root** — spawned by the service, never by React (§30).
//! - **Narrow scope**: the `.saipen` directory itself, non-recursive
//!   (§32) — `node_modules` storms never reach SAIPEN status.
//! - **Coalescing, not history**: events set a dirty flag; one semantic
//!   reread fires after a debounce window of quiet (§33–§35, §112–§113).
//! - **Bounded channel**: a full channel is not allowed to grow memory; the
//!   event is dropped and an overflow flag records that a full reread is
//!   required (§36, §112).
//! - **Generation-tagged**: late events from a closed/replaced generation
//!   are discarded by the service (§65–§66).
//! - **Root replacement**: a rename/create/remove of the `.saipen` dir
//!   itself triggers a rebind (§42, §134).
//! - **Bounded restart** on watcher failure, shutdown-aware (§37–§38).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{mpsc, Notify};

use crate::model::{SaipenError, SaipenRoot};

/// Default debounce window (donor SAIPEN.md spec: ~200–300 ms per file).
pub const DEFAULT_DEBOUNCE_MS: u64 = 300;

/// How often the watcher re-checks that its watched root still exists. Only
/// a missing root drives a rebind; a live root costs one stat per tick.
const LIVENESS_CHECK_MS: u64 = 2000;
/// Bounded channel capacity — a filesystem storm cannot grow memory (§112).
pub const CHANNEL_CAPACITY: usize = 64;
/// Bounded watcher restarts on transient backend failure (§38).
pub const MAX_RESTARTS: u32 = 2;

pub struct WatcherConfig {
    pub debounce_ms: u64,
    pub max_restarts: u32,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            debounce_ms: DEFAULT_DEBOUNCE_MS,
            max_restarts: MAX_RESTARTS,
        }
    }
}

/// What the watcher tells its owner. The signal is explicit: a change and a
/// terminal failure are DIFFERENT facts, and the service must never derive
/// `Live` from a stale handle alone (TASK 24 §9).
#[derive(Debug, Clone)]
pub enum WatchSignal {
    /// A change occurred: perform one authoritative reread.
    Change,
    /// The watcher failed terminally (restarts exhausted / rebind broken):
    /// the service must surface a Failed watch state — the watch is dead and
    /// no further events will arrive.
    Failed(String),
}

/// Owned handle: dropping it signals shutdown and aborts the watcher task;
/// `generation` identifies this watch session (stale events are discarded
/// against it).
pub struct WatchHandle {
    pub generation: u64,
    shutdown: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.shutdown.notify_one();
        // Abort so the debounce/refresh task cannot outlive its owner even
        // if the Notify signal races a task that is mid-sleep (§62, §138).
        self.task.abort();
    }
}

/// Spawn one watcher for `root`. `on_signal(generation, signal)` is invoked
/// (from our own async task, never notify's callback thread) after a quiet
/// debounce window with pending changes — the service then performs a full
/// authoritative reread and semantic comparison. A terminal watcher failure
/// is reported as `WatchSignal::Failed` so the service can surface a dead
/// watch instead of deriving `Live` from a stale handle (TASK 24 §9).
pub fn spawn(
    root: SaipenRoot,
    generation: u64,
    on_signal: Arc<dyn Fn(u64, WatchSignal) + Send + Sync>,
    config: WatcherConfig,
) -> Result<WatchHandle, SaipenError> {
    let shutdown = Arc::new(Notify::new());
    let (tx, rx) = mpsc::channel::<Result<Event, notify::Error>>(CHANNEL_CAPACITY);
    let overflow = Arc::new(AtomicBool::new(false));
    let root_dir = root.dir.clone();

    let mut watcher: RecommendedWatcher = notify::recommended_watcher({
        let tx = tx.clone();
        let overflow = overflow.clone();
        move |res: notify::Result<Event>| {
            // Lightweight: signal only. Never heavy work in this callback
            // (§111). On a full channel, drop the event and set the overflow
            // flag — the next refresh is a full authoritative reread.
            if tx.try_send(res).is_err() {
                overflow.store(true, Ordering::SeqCst);
            }
        }
    })
    .map_err(|e| SaipenError::WatchFailed(format!("cannot create watcher: {e}")))?;

    watcher
        .watch(&root_dir, RecursiveMode::NonRecursive)
        .map_err(|e| {
            SaipenError::WatchFailed(format!("cannot watch {}: {e}", root_dir.display()))
        })?;

    // `tokio::spawn` panics when called outside a Tokio runtime (e.g. from a
    // Tauri sync command handler like `set_active_workspace`). The SAIPEN
    // watcher is a best-effort live projection — a missing runtime must degrade
    // to a read-only snapshot, never crash the desktop shell.
    let task = match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.spawn(run_loop(
            watcher,
            rx,
            overflow,
            shutdown.clone(),
            root_dir,
            generation,
            on_signal,
            config,
        )),
        Err(_) => {
            return Err(SaipenError::WatchFailed(
                "no reactor running — watcher degraded to poll-only".into(),
            ))
        }
    };

    Ok(WatchHandle {
        generation,
        shutdown,
        task,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    mut watcher: RecommendedWatcher,
    mut rx: mpsc::Receiver<Result<Event, notify::Error>>,
    overflow: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
    root_dir: PathBuf,
    generation: u64,
    on_signal: Arc<dyn Fn(u64, WatchSignal) + Send + Sync>,
    config: WatcherConfig,
) {
    let mut restarts = 0u32;
    loop {
        let outcome = tokio::select! {
            _ = shutdown.notified() => return,
            // Liveness heartbeat: when the watched root is DELETED, notify
            // may deliver nothing at all (Windows), so a dead watch would
            // otherwise freeze silently while the service still claims Live
            // (TASK 24 §9). Detect the missing root and drive the rebind
            // path, which fails terminally once retries are exhausted.
            _ = tokio::time::sleep(Duration::from_millis(LIVENESS_CHECK_MS)) => {
                if root_dir.exists() {
                    continue;
                }
                WatchLoopOutcome::Rebind
            }
            msg = rx.recv() => {
                match msg {
                    None => return, // sender dropped
                    Some(Ok(ev)) => {
                        if is_root_replacement(&ev, &root_dir) {
                            // The .saipen dir itself was renamed/replaced:
                            // rebind (watch handle may be attached to a
                            // deleted inode, §42). Mark overflow so the next
                            // refresh is a full reread.
                            overflow.store(true, Ordering::SeqCst);
                            WatchLoopOutcome::Rebind
                        } else {
                            WatchLoopOutcome::Dirty
                        }
                    }
                    // A notify error usually means the watch itself broke
                    // (deleted root / invalidated handle): attempt a rebind
                    // rather than pretending a refresh fixes a dead watch.
                    Some(Err(_)) => WatchLoopOutcome::Rebind,
                }
            }
        };
        match outcome {
            WatchLoopOutcome::Dirty => {
                match debounce_and_fire(
                    &mut rx,
                    &shutdown,
                    config.debounce_ms,
                    overflow.load(Ordering::SeqCst),
                    &root_dir,
                    generation,
                    &on_signal,
                )
                .await
                {
                    DebounceOutcome::Shutdown => return,
                    DebounceOutcome::Rebind => {
                        // CORE-022: a root replacement or error observed during
                        // debounce requires a rebind, not a plain refresh.
                        overflow.store(false, Ordering::SeqCst);
                        // Fall through to the rebind path below by re-looping.
                        // We need to break out of the match and hit the rebind
                        // branch. Use a flag to avoid duplicating the rebind logic.
                        // Actually, let's just push the event back and re-enter.
                        // The simplest correct approach: trigger the rebind inline.
                        loop {
                            let rebind = rebind_watcher(
                                &mut watcher,
                                &root_dir,
                                &shutdown,
                                config.debounce_ms,
                            )
                            .await;
                            match rebind {
                                RebindOutcome::Done => {
                                    on_signal(generation, WatchSignal::Change);
                                    break;
                                }
                                RebindOutcome::Shutdown => return,
                                RebindOutcome::Failed(e) => {
                                    restarts += 1;
                                    tracing::warn!(error = %e, restarts, "saipen watcher rebind failed (from debounce promotion)");
                                    if restarts >= config.max_restarts {
                                        on_signal(generation, WatchSignal::Failed(e));
                                        let _ = watcher;
                                        return;
                                    }
                                    if tokio::time::timeout(
                                        Duration::from_millis(500 * u64::from(restarts)),
                                        shutdown.notified(),
                                    )
                                    .await
                                    .is_ok()
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    DebounceOutcome::Dirty => {
                        overflow.store(false, Ordering::SeqCst);
                    }
                }
            }
            WatchLoopOutcome::Rebind => {
                // Retry the rebind actively: after a failed rebind + bounded
                // backoff, the watcher is UNWATCHED — waiting for another fs
                // event that may never come would silently freeze a dead
                // watch while the service still claims Live (TASK 24 §9).
                loop {
                    let rebind = rebind_watcher(
                        &mut watcher,
                        &root_dir,
                        &shutdown,
                        config.debounce_ms,
                    )
                    .await;
                    match rebind {
                        // A successful rebind MUST trigger one authoritative
                        // reread: the replaced .saipen content would
                        // otherwise stay invisible until the next filesystem
                        // event (TASK 24 §9).
                        RebindOutcome::Done => {
                            on_signal(generation, WatchSignal::Change);
                            break;
                        }
                        RebindOutcome::Shutdown => return,
                        RebindOutcome::Failed(e) => {
                            restarts += 1;
                            tracing::warn!(error = %e, restarts, "saipen watcher rebind failed");
                            if restarts >= config.max_restarts {
                                // Terminal failure: report the DEAD watch
                                // explicitly — the service must never claim
                                // Live because a stale handle still exists.
                                on_signal(generation, WatchSignal::Failed(e));
                                let _ = watcher; // dropped
                                return;
                            }
                            // Bounded backoff, then retry.
                            if tokio::time::timeout(
                                Duration::from_millis(500 * u64::from(restarts)),
                                shutdown.notified(),
                            )
                            .await
                            .is_ok()
                            {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

enum WatchLoopOutcome {
    Dirty,
    Rebind,
}

enum RebindOutcome {
    Done,
    Shutdown,
    Failed(String),
}

/// The outcome of a debounce cycle — distinguishes ordinary dirty events
/// from control events (root replacement, notify error) that must be
/// promoted to a rebind (CORE-022).
enum DebounceOutcome {
    /// Ordinary changes coalesced; a single refresh is sufficient.
    Dirty,
    /// A root replacement or notify error was observed during the debounce
    /// window; the watcher handle is invalidated and a rebind is required.
    Rebind,
    /// Shutdown signal received.
    Shutdown,
}

/// Wait for a quiet debounce window with pending changes, then fire exactly
/// one semantic refresh. During the window, every queued event is inspected:
/// root replacements and notify errors are promoted to a rebind so they
/// cannot be silently collapsed into a plain refresh (CORE-022).
///
/// `overflow` indicates the bounded callback channel was full and at least
/// one event was dropped; this is tracked separately from the rebind flag
/// so a root replacement during overflow is never erased by the generic
/// overflow reset (CORE-022).
async fn debounce_and_fire(
    rx: &mut mpsc::Receiver<Result<Event, notify::Error>>,
    shutdown: &Notify,
    debounce_ms: u64,
    overflow: bool,
    root_dir: &std::path::Path,
    generation: u64,
    on_signal: &Arc<dyn Fn(u64, WatchSignal) + Send + Sync>,
) -> DebounceOutcome {
    let mut needs_rebind = false;
    loop {
        tokio::select! {
            _ = shutdown.notified() => return DebounceOutcome::Shutdown,
            _ = tokio::time::sleep(Duration::from_millis(debounce_ms)) => break,
            msg = rx.recv() => {
                match msg {
                    None => return DebounceOutcome::Shutdown,
                    Some(Ok(ev)) => {
                        // CORE-022: inspect every event during debounce — a root
                        // replacement or notify error arriving during the storm must
                        // be promoted, not swallowed.
                        if is_root_replacement(&ev, root_dir) {
                            needs_rebind = true;
                        }
                    }
                    // CORE-022: a notify error during debounce means the watch
                        // handle is invalidated; promote to rebind.
                    Some(Err(_)) => {
                        needs_rebind = true;
                    }
                }
                // re-arm: keep waiting until quiet
            }
        }
    }
    // CORE-022: if the callback channel overflowed AND we observed no root
    // replacement, the overflow itself may have been the dropped root event.
    // Preserve the rebind requirement from overflow unless we already have
    // one from an inspected event.
    if overflow && !needs_rebind {
        needs_rebind = true;
    }
    if needs_rebind {
        DebounceOutcome::Rebind
    } else {
        // One coalesced refresh per storm (§33, §136): N fs events → 1 reread.
        on_signal(generation, WatchSignal::Change);
        DebounceOutcome::Dirty
    }
}

fn is_root_replacement(ev: &Event, root_dir: &std::path::Path) -> bool {
    if !matches!(
        ev.kind,
        EventKind::Create(_)
            | EventKind::Remove(_)
            | EventKind::Modify(notify::event::ModifyKind::Name(_))
    ) {
        return false;
    }
    ev.paths.iter().any(|p| p == root_dir)
}

async fn rebind_watcher(
    watcher: &mut RecommendedWatcher,
    root_dir: &std::path::Path,
    shutdown: &Notify,
    debounce_ms: u64,
) -> RebindOutcome {
    // Unwatch (best effort) and rewatch after the dust settles.
    let _ = watcher.unwatch(root_dir);
    let quiet = tokio::select! {
        _ = shutdown.notified() => RebindOutcome::Shutdown,
        _ = tokio::time::sleep(Duration::from_millis(debounce_ms)) => RebindOutcome::Done,
    };
    if matches!(quiet, RebindOutcome::Shutdown) {
        return quiet;
    }
    match watcher.watch(root_dir, RecursiveMode::NonRecursive) {
        Ok(()) => RebindOutcome::Done,
        Err(e) => RebindOutcome::Failed(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[tokio::test]
    async fn one_storm_yields_one_refresh() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        let root = crate::paths::validate_root(dir.path()).unwrap().unwrap();
        let fires = Arc::new(Mutex::new(0u32));
        let fires2 = fires.clone();
        let handle = spawn(
            root,
            1,
            Arc::new(move |_g, signal| {
                if matches!(signal, WatchSignal::Change) {
                    *fires2.lock().unwrap() += 1;
                }
            }),
            WatcherConfig {
                debounce_ms: 50,
                max_restarts: 0,
            },
        )
        .unwrap();
        // Simulate a save burst: several writes to STATE.md quickly.
        let state = dir.path().join(".saipen/STATE.md");
        for i in 0..10 {
            std::fs::write(&state, format!("---\nphase: BUILD\nlast: {i}\n---\n")).unwrap();
            std::fs::write(dir.path().join(".saipen/BOARD.md"), "## TODO\n").unwrap();
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        drop(handle);
        let fires = *fires.lock().unwrap();
        assert!(
            (1..=3).contains(&fires),
            "storm must coalesce to ~1 refresh, got {fires}"
        );
    }

    #[tokio::test]
    async fn idle_watcher_fires_nothing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        let root = crate::paths::validate_root(dir.path()).unwrap().unwrap();
        let fires = Arc::new(Mutex::new(0u32));
        let fires2 = fires.clone();
        let handle = spawn(
            root,
            1,
            Arc::new(move |_g, signal| {
                if matches!(signal, WatchSignal::Change) {
                    *fires2.lock().unwrap() += 1;
                }
            }),
            WatcherConfig {
                debounce_ms: 40,
                max_restarts: 0,
            },
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(handle);
        assert_eq!(*fires.lock().unwrap(), 0, "idle watcher must not fire");
    }

    #[tokio::test]
    async fn root_replacement_triggers_rebind_path() {
        // The dir-replace event must produce at least one refresh (the
        // rebind path ends in on_change to surface state).
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        let state = dir.path().join(".saipen/STATE.md");
        std::fs::write(&state, "---\nphase: A\n---\n").unwrap();
        let root = crate::paths::validate_root(dir.path()).unwrap().unwrap();
        let fires = Arc::new(Mutex::new(0u32));
        let fires2 = fires.clone();
        let handle = spawn(
            root,
            1,
            Arc::new(move |_g, signal| {
                if matches!(signal, WatchSignal::Change) {
                    *fires2.lock().unwrap() += 1;
                }
            }),
            WatcherConfig {
                debounce_ms: 50,
                max_restarts: 2,
            },
        )
        .unwrap();
        // Atomically replace the whole .saipen dir (temp + rename).
        let tmp = dir.path().join(".saipen.new");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("STATE.md"), "---\nphase: B\n---\n").unwrap();
        let old = dir.path().join(".saipen.old");
        std::fs::rename(dir.path().join(".saipen"), &old).unwrap();
        std::fs::rename(&tmp, dir.path().join(".saipen")).unwrap();
        let _ = std::fs::remove_dir_all(&old);
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(handle);
        assert!(*fires.lock().unwrap() >= 1, "root replacement must refresh");
    }
}
