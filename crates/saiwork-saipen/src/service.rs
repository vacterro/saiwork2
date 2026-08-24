//! SaipenService — the single owner of SAIPEN read state (TASK 14 §30–§31,
//! §58–§68). One instance per application; one watcher per active workspace
//! root. The cached `SaipenSnapshot` is a projection cache, never an
//! authority (§166). Events are semantic facts emitted only after a state
//! determination: `saipen.detected` on NotPresent→Present transitions,
//! `saipen.changed` only when the normalized snapshot meaningfully changed
//! (§51–§54, §167).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use saiwork_events::{Event, EventBus, WorkspaceId};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::model::{Discovery, SaipenError, SaipenRoot, SaipenSnapshot, WatchStatus};
use crate::reader;
use crate::watcher::{self, WatchHandle, WatchSignal, WatcherConfig};

struct Entry {
    root: SaipenRoot,
    watch: Option<WatchHandle>,
    /// Whether the watch is provably LIVE. A stale `WatchHandle` alone must
    /// never claim Live — a terminal watcher failure is reported explicitly
    /// and flips this to false (TASK 24 §9).
    watch_live: bool,
    /// Watch-session epoch (stale-event protection §65): incremented on every
    /// (re)attach, used only to discard late callbacks from a replaced
    /// watcher. Never stamped into snapshots as a semantic revision.
    watch_epoch: u64,
    /// Semantic snapshot revision: incremented ONLY when the projection
    /// meaningfully changed (content or stale-marking), never on no-op
    /// rereads. This is the value stamped into `SaipenSnapshot.generation`
    /// and is what validation staleness compares against (§87–§88).
    revision: u64,
    snapshot: Option<SaipenSnapshot>,
    /// Whether SAIPEN was present at the last determination (detected
    /// transition tracking §52).
    was_present: bool,
    /// True while a refresh read is in flight for this workspace. Change
    /// signals that arrive meanwhile set `refresh_dirty` instead of starting
    /// a parallel reader (TASK 24 perf: one coalesced reread per storm).
    refresh_in_flight: bool,
    refresh_dirty: bool,
}

pub struct SaipenService {
    bus: EventBus,
    generation: std::sync::atomic::AtomicU64,
    shutdown: Arc<Notify>,
    entries: Mutex<HashMap<String, Entry>>,
    /// CORE-023: persistent stopped flag. Once set by `shutdown()`,
    /// `attach()` is permanently rejected for this service lifetime —
    /// a late active-workspace commit after shutdown cannot resurrect
    /// watchers.
    stopped: AtomicBool,
}

impl SaipenService {
    pub fn new(bus: EventBus) -> Arc<Self> {
        Arc::new(Self {
            bus,
            generation: std::sync::atomic::AtomicU64::new(1),
            shutdown: Arc::new(Notify::new()),
            entries: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
        })
    }

    fn next_generation(&self) -> u64 {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Attach a workspace: discovery → spawn watcher (registered first, so
    /// no change can slip between read and watch — §59) → read → emit.
    pub fn attach(self: &Arc<Self>, workspace_id: &str, workspace_root: &Path) {
        // CORE-023: a stopped service must never resurrect watchers.
        if self.stopped.load(Ordering::SeqCst) {
            info!(workspace_id, "saipen: attach rejected (service stopped)");
            return;
        }
        // PERF-005: idempotent re-attach. If this workspace is already attached
        // with a LIVE watcher rooted at the same path, do nothing — re-selecting
        // an already-current workspace must not spawn a new watcher epoch,
        // re-read STATE+BOARD, or re-emit `saipen.detected`. A degraded
        // (non-live) watch is re-attached by the path below (re-discovery).
        {
            let entries = self.entries.lock().expect("saipen entries mutex poisoned");
            if let Some(entry) = entries.get(workspace_id) {
                if entry.watch_live && entry.root.dir == workspace_root {
                    return;
                }
            }
        }
        let discovery = match reader::discover(workspace_root) {
            Ok(d) => d,
            Err(e) => {
                self.record_attach_error(workspace_id, e);
                return;
            }
        };
        let (epoch, root) = {
            let mut entries = self.entries.lock().expect("saipen entries mutex poisoned");
            match discovery {
                Discovery::NotPresent => {
                    entries.remove(workspace_id);
                    info!(workspace_id, "saipen: not present");
                    return;
                }
                Discovery::Present(desc) => {
                    // Watch-session epoch (stale-callback protection only).
                    let epoch = self.next_generation();
                    // W2-005: preserve the previous semantic projection across
                    // a reattach of the SAME workspace. Simply replacing the
                    // entry with `snapshot: None` would discard a last-good
                    // projection; a malformed read during reattach must then
                    // mark it STALE but keep the previous content (the
                    // service's own "keep last good snapshot" recovery
                    // invariant). The watch epoch changes on every (re)attach,
                    // but the semantic `revision` and `was_present` are
                    // carried forward so an unchanged reattach does not bump
                    // the semantic generation, and the previous content is not
                    // thrown away.
                    let (revision, was_present, previous_snapshot) = {
                        let prev = entries.get(workspace_id);
                        (
                            prev.map(|e| e.revision).unwrap_or(0),
                            prev.map(|e| e.was_present).unwrap_or(false),
                            prev.and_then(|e| e.snapshot.clone()),
                        )
                    };
                    let service = self.clone();
                    let wid = workspace_id.to_string();
                    let root = desc.root.clone();
                    let on_signal: Arc<dyn Fn(u64, WatchSignal) + Send + Sync> = Arc::new(move |g, sig| {
                        service.on_watch_signal(&wid, g, sig);
                    });
                    let watch = match watcher::spawn(
                        desc.root.clone(),
                        epoch,
                        on_signal,
                        WatcherConfig::default(),
                    ) {
                        Ok(h) => {
                            info!(workspace_id, "saipen: watch started");
                            Some(h)
                        }
                        Err(e) => {
                            warn!(error = %e, workspace_id, "saipen: watch start failed (degraded read-only view)");
                            None
                        }
                    };
                    let watch_live = watch.is_some();
                    // Register the watcher and a placeholder entry FIRST
                    // (§59: read-after-watch — no lost-update window). The
                    // full STATE+BOARD read runs in phase 2 WITHOUT the
                    // global lock (TASK 24 perf: SAIPEN reads never hold the
                    // entries mutex). The previous snapshot is carried forward
                    // so a failed reattach read marks it STALE but preserves
                    // content instead of fabricating an ERROR projection.
                    entries.insert(
                        workspace_id.to_string(),
                        Entry {
                            root: desc.root,
                            watch,
                            watch_live,
                            watch_epoch: epoch,
                            revision,
                            snapshot: previous_snapshot,
                            was_present: true,
                            refresh_in_flight: false,
                            refresh_dirty: false,
                        },
                    );
                    if !was_present {
                        self.bus.publish(Event::SaipenDetected {
                            workspace_id: WorkspaceId::new(workspace_id),
                        });
                    }
                    (epoch, root)
                }
                Discovery::Invalid { reason } => {
                    self.bus.publish(Event::RuntimeWarning {
                        code: "SAIPEN_INVALID".into(),
                        message: format!(
                            "SAIPEN in workspace {workspace_id} is invalid: {reason}"
                        ),
                    });
                    entries.remove(workspace_id);
                    return;
                }
                Discovery::Unsupported {
                    schema_version,
                    protocol_version,
                } => {
                    self.bus.publish(Event::RuntimeWarning {
                        code: "SAIPEN_UNSUPPORTED".into(),
                        message: format!(
                            "SAIPEN detected in workspace {workspace_id} with unsupported schema {schema_version:?} (protocol {protocol_version:?})"
                        ),
                    });
                    entries.remove(workspace_id);
                    return;
                }
                Discovery::PermissionDenied { path } => {
                    warn!(path = %path.display(), workspace_id, "saipen: permission denied");
                    entries.remove(workspace_id);
                    return;
                }
            }
        };
        // Phase 2: the ONE full STATE+BOARD consistency-read pipeline for
        // this open, run with NO lock held. Any change signals that arrive
        // during the read are coalesced by the refresh in-flight flag.
        let result = reader::read_snapshot(&root, epoch);
        self.commit_refresh(workspace_id, epoch, result);
    }

    fn record_attach_error(&self, workspace_id: &str, e: SaipenError) {
        warn!(error = %e, workspace_id, "saipen: attach failed");
        self.bus.publish(Event::RuntimeWarning {
            code: "SAIPEN_READ_FAILED".into(),
            message: format!("SAIPEN read failed in workspace {workspace_id}: {e}"),
        });
    }

    /// Route one watcher signal (TASK 24 §9): a change triggers the
    /// authoritative reread; a terminal failure flips the entry to a provably
    /// dead watch and surfaces it — never Live from a stale handle.
    fn on_watch_signal(self: &Arc<Self>, workspace_id: &str, epoch: u64, signal: WatchSignal) {
        match signal {
            WatchSignal::Change => self.refresh(workspace_id, epoch),
            WatchSignal::Failed(reason) => self.mark_watch_failed(workspace_id, epoch, reason),
        }
    }

    /// The watcher died terminally: mark the projection's watch status
    /// Failed (never Live), bump the semantic revision, surface once.
    fn mark_watch_failed(&self, workspace_id: &str, epoch: u64, reason: String) {
        let mut entries = self.entries.lock().expect("saipen entries mutex poisoned");
        let Some(entry) = entries.get_mut(workspace_id) else {
            return; // workspace detached; late signal discarded
        };
        if entry.watch_epoch != epoch {
            return; // stale watch session; discard (§65)
        }
        if !entry.watch_live {
            return; // already failed — surface once
        }
        entry.watch_live = false;
        entry.revision += 1;
        if let Some(prev) = &mut entry.snapshot {
            prev.watch_status = WatchStatus::Failed(reason.clone());
            prev.generation = entry.revision;
            prev.stale = true;
            prev.last_error = Some(format!("watch failed: {reason}"));
        }
        drop(entries);
        self.bus.publish(Event::SaipenChanged {
            workspace_id: WorkspaceId::new(workspace_id),
        });
        self.bus.publish(Event::RuntimeWarning {
            code: "SAIPEN_WATCH_FAILED".into(),
            message: format!("SAIPEN watch failed in workspace {workspace_id}: {reason}"),
        });
    }

    /// Refresh after a watcher change signal. Generation-guarded: a late
    /// event from an old watch session cannot mutate the current projection
    /// (§65–§66). Two-phase (TASK 24 perf): the canonical STATE+BOARD read
    /// runs on the blocking pool with NO lock held, so one slow workspace
    /// can never block snapshot/detach/refresh for every other workspace or
    /// occupy an async worker. Change signals that arrive while a read is
    /// in flight are coalesced into at most one follow-up reread.
    fn refresh(self: &Arc<Self>, workspace_id: &str, epoch: u64) {
        // Phase 1 — lock held, cheap: claim the in-flight slot; signals
        // during the read set `refresh_dirty` instead of starting a
        // parallel reader (one coalesced reread per storm).
        let root_clone = {
            let mut entries = self.entries.lock().expect("saipen entries mutex poisoned");
            let Some(entry) = entries.get_mut(workspace_id) else {
                return; // workspace detached; late event discarded
            };
            if entry.watch_epoch != epoch {
                return; // stale watch session; discard (§65)
            }
            if entry.refresh_in_flight {
                entry.refresh_dirty = true;
                return; // coalesce: one reader per workspace
            }
            entry.refresh_in_flight = true;
            entry.root.clone()
        };
        // Phase 2 — NO lock: read on the blocking pool. The watcher task
        // stays responsive for every workspace; a slow read cannot stall
        // snapshot/detach/refresh of any other workspace.
        let service = self.clone();
        let wid = workspace_id.to_string();
        tokio::spawn(async move {
            let result = match tokio::task::spawn_blocking(move || {
                reader::read_snapshot(&root_clone, epoch)
            })
            .await
            {
                Ok(Ok(snap)) => Ok(snap),
                Ok(Err(e)) => Err(e),
                Err(join) => Err(SaipenError::Internal(format!(
                    "SAIPEN refresh task failed: {join}"
                ))),
            };
            let dirty = service.commit_refresh(&wid, epoch, result);
            if dirty {
                // Changes arrived while the read was in flight: exactly one
                // follow-up authoritative reread (coalesced), never a storm.
                service.refresh(&wid, epoch);
            }
        });
    }

    /// Commit a phase-2 read result back under the lock (TASK 24 perf).
    /// Rejects stale epochs and detached workspaces, performs the semantic
    /// comparison + revision bookkeeping, publishes outside the lock, and
    /// returns whether a coalesced follow-up refresh is needed (a `dirty`
    /// flag set by signals that arrived during the read).
    fn commit_refresh(
        &self,
        workspace_id: &str,
        epoch: u64,
        result: Result<SaipenSnapshot, SaipenError>,
    ) -> bool {
        let (publish_change, publish_failure, dirty_follow_up) = {
            let mut entries = self.entries.lock().expect("saipen entries mutex poisoned");
            let Some(entry) = entries.get_mut(workspace_id) else {
                return false; // workspace detached during the read; discard (§65)
            };
            if entry.watch_epoch != epoch {
                return false; // watch replaced during the read; discard late result (§65)
            }
            entry.refresh_in_flight = false;
            let dirty_follow_up = entry.refresh_dirty;
            entry.refresh_dirty = false;
            let mut publish_change = false;
            let mut publish_failure = None;
            match result {
                Ok(mut snap) => {
                    snap.watch_status = if entry.watch_live {
                        WatchStatus::Live
                    } else {
                        WatchStatus::Failed("watch unavailable".into())
                    };
                    let previous = entry.snapshot.clone();
                    let changed = previous
                        .as_ref()
                        .map(|prev| !prev.semantically_eq(&snap))
                        .unwrap_or(true);
                    if changed {
                        // Semantic change → snapshot revision advances. This
                        // is what validation staleness compares against
                        // (§87–§88): a valid result bound to the previous
                        // revision must go STALE the moment the canonical
                        // state moves.
                        entry.revision += 1;
                        snap.generation = entry.revision;
                        entry.snapshot = Some(snap);
                        publish_change = true;
                    } else {
                        // Content unchanged: update timing only; the
                        // semantic revision is PRESERVED — a no-op atomic
                        // save must not invalidate a validation bound to
                        // this snapshot (§54, §167).
                        if let Some(prev) = previous {
                            entry.snapshot = Some(SaipenSnapshot {
                                read_at_ms: snap.read_at_ms,
                                ..prev
                            });
                        }
                    }
                }
                Err(e) => {
                    // Keep last good snapshot but mark STALE; surface once.
                    // The stale-marking is a semantic state change →
                    // revision bumps.
                    if let Some(prev) = &mut entry.snapshot {
                        if !prev.stale {
                            entry.revision += 1;
                            prev.stale = true;
                            prev.generation = entry.revision;
                            prev.last_error = Some(e.to_string());
                            publish_change = true;
                            publish_failure = Some(e);
                        }
                    } else {
                        // First read failed: create an explicitly stale/error snapshot so it is not treated as ABSENT
                        entry.revision += 1;
                        entry.snapshot = Some(SaipenSnapshot {
                            generation: entry.revision,
                            project: Some("ERROR".into()),
                            phase: Some("ERROR".into()),
                            task: Some("ERROR".into()),
                            next_action: Some("ERROR".into()),
                            read_at_ms: reader::now_ms(),
                            stale: true,
                            last_error: Some(e.to_string()),
                            watch_status: WatchStatus::Failed("initial read failed".into()),
                            root: Some(entry.root.dir.clone()),
                            ..Default::default()
                        });
                        publish_change = true;
                        publish_failure = Some(e);
                    }
                }
            }
            (publish_change, publish_failure, dirty_follow_up)
        };
        if publish_change {
            self.bus.publish(Event::SaipenChanged {
                workspace_id: WorkspaceId::new(workspace_id),
            });
        }
        if let Some(e) = publish_failure {
            self.bus.publish(Event::RuntimeWarning {
                code: "SAIPEN_READ_FAILED".into(),
                message: format!(
                    "SAIPEN refresh failed in workspace {workspace_id}: {e}"
                ),
            });
        }
        dirty_follow_up
    }

    /// Current projection (cached), or a fresh read when detached. Returns
    /// `None` for workspaces without SAIPEN (NotPresent is a normal state).
    pub fn snapshot(&self, workspace_id: &str) -> Option<SaipenSnapshot> {
        let entries = self.entries.lock().expect("saipen entries mutex poisoned");
        entries.get(workspace_id).and_then(|e| e.snapshot.clone())
    }

    /// Explicit authoritative refresh after an action (§125): reuses the
    /// same serialized refresh pipeline as the watcher (one refresh owner),
    /// never a parallel reader competing with it. Emits `saipen.changed`
    /// only on a meaningful change.
    pub fn force_refresh(self: &Arc<Self>, workspace_id: &str) {
        let epoch = {
            let entries = self.entries.lock().expect("saipen entries mutex poisoned");
            let Some(entry) = entries.get(workspace_id) else {
                return; // not attached
            };
            entry.watch_epoch
        };
        self.refresh(workspace_id, epoch);
    }

    /// Detach: stop the watcher, cancel its debounce/refresh task, drop the
    /// projection (§62). Late events are discarded by the generation guard.
    pub fn detach(&self, workspace_id: &str) {
        let mut entries = self.entries.lock().expect("saipen entries mutex poisoned");
        if let Some(entry) = entries.remove(workspace_id) {
            drop(entry.watch); // signals shutdown
            info!(workspace_id, "saipen: detached");
        }
    }

    /// App shutdown: stop every watcher deterministically (§138).
    /// CORE-023: set the persistent stopped flag so a late `attach()`
    /// cannot resurrect watchers after shutdown.
    pub fn shutdown(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.shutdown.notify_waiters();
        let mut entries = self.entries.lock().expect("saipen entries mutex poisoned");
        for (_id, entry) in entries.drain() {
            drop(entry.watch);
        }
    }

    pub fn active_count(&self) -> usize {
        self.entries
            .lock()
            .expect("saipen entries mutex poisoned")
            .len()
    }
}
