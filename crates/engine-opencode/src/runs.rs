//! In-memory active-run registry (TASK 11 §73–§76).
//!
//! One owner of run state: the adapter. It tracks RunId ↔ session, the
//! runtime generation (stale-runtime protection, §113–§115), the cancel
//! request, the evidence flags, and the owned POST task. Nothing here is
//! durable — OpenCode remains the authority for session content (§3).
//!
//! Run cleanup policy (§76): terminal runs are removed immediately after the
//! terminal event is emitted; the registry never accumulates history. The
//! adapter keeps only diagnostic counters, never the messages themselves.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use saiwork_events::RunId;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunState {
    Running,
    Completed,
    Failed,
    Cancelled,
}

pub(crate) struct RunRecord {
    pub run_id: RunId,
    /// Generic SAIWORK2 session id — used in every canonical event.
    pub session_id: String,
    /// Upstream OpenCode session id — used for the actual server calls
    /// (message POST, abort, permission reply) and wire event routing
    /// (TASK 24 §9: generic and upstream ids differ).
    pub engine_session_id: String,
    /// Runtime generation this run belongs to (stale-callback guard, §114).
    pub generation: u64,
    /// Set by `cancel()`; read by the POST task at terminal time.
    pub cancel_requested: AtomicBool,
    /// Set by `cancel()` when the abort HTTP request authoritatively succeeds.
    pub abort_delivered: AtomicBool,
    /// Set by the engine-crash watcher / stop path before they claim the
    /// terminal. The POST task defers to this flag when its body read dies:
    /// an engine-loss terminal (Failed) is authoritative — the run is
    /// definitively over — whereas a transport loss on a live engine is an
    /// honest Unknown (TASK 24 §9).
    pub engine_lost: AtomicBool,
    /// `message.started` emitted exactly once (evidence-based, §22).
    pub started_emitted: AtomicBool,
    /// Assistant message id learned from the event stream (correlation §23).
    pub message_id: Mutex<Option<String>>,
    /// Last `session.error` observed for this session during the run (§57).
    pub session_error: Mutex<Option<String>>,
    /// Monotonic wall time of the most recent matched SSE activity for this
    /// run (TASK 24 perf): the POST task waits only the REMAINING
    /// STREAM_IDLE_GRACE after this, not an unconditional 250 ms.
    pub last_stream_activity: Mutex<Option<std::time::Instant>>,
    pub state: Mutex<RunState>,
    /// True once a terminal outcome was emitted (one-terminal gate, §24).
    pub terminal_emitted: AtomicBool,
    /// Owned POST task; joined on cleanup (§77).
    pub post_task: Mutex<Option<JoinHandle<()>>>,
    /// Wake channel for the engine-loss settle (PERF-008): the POST task parks
    /// on this instead of polling `engine_lost` every 10 ms.
    pub engine_lost_notify: Notify,
    /// Wake channel for the idle-grace settle (PERF-008): the POST task parks
    /// on this instead of polling `session_error` every 10 ms.
    pub session_notify: Notify,
}

impl RunRecord {
    pub fn is_terminal(&self) -> bool {
        *self.state.lock().expect("run state mutex poisoned") != RunState::Running
    }

    /// Record matched SSE activity (liveness + idle-grace evidence).
    pub fn note_stream_activity(&self) {
        *self
            .last_stream_activity
            .lock()
            .expect("run stream activity mutex poisoned") = Some(std::time::Instant::now());
    }

    /// Elapsed since the last matched SSE activity, if any.
    pub fn idle_for(&self) -> Option<std::time::Duration> {
        self.last_stream_activity
            .lock()
            .expect("run stream activity mutex poisoned")
            .map(|t| t.elapsed())
    }

    /// Mark the engine lost and wake any POST task parked on `engine_lost_notify`
    /// (PERF-008). Replaces the bare `AtomicBool::store` so the settle wait is
    /// event-driven, not a 10 ms poll.
    pub fn mark_engine_lost(&self) {
        self.engine_lost.store(true, Ordering::SeqCst);
        self.engine_lost_notify.notify_one();
    }
}

#[derive(Default)]
pub(crate) struct RunRegistry {
    by_run: Mutex<HashMap<String, Arc<RunRecord>>>,
    /// Upstream session id → run id (SSE wire routing).
    by_engine_session: Mutex<HashMap<String, String>>,
    /// Generic SAIWORK2 session id → run id (permission resolution, generic
    /// lookups).
    by_generic_session: Mutex<HashMap<String, String>>,
}

impl RunRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register a run. Fails when the run id is already registered (should
    /// never happen — RunIds are fresh UUIDs) or the session already has an
    /// active run (same-session concurrency: REJECT, §70–§72).
    pub(crate) fn insert(
        &self,
        record: Arc<RunRecord>,
    ) -> Result<(), crate::errors::OpenCodeError> {
        let mut by_run = self.by_run.lock().expect("run registry mutex poisoned");
        if by_run.contains_key(record.run_id.as_str()) {
            return Err(crate::errors::OpenCodeError::DuplicateRun {
                session_id: record.session_id.clone(),
                run_id: record.run_id.to_string(),
            });
        }
        // Same-generic-session concurrency: REJECT (§70–§72).
        let mut by_generic = self
            .by_generic_session
            .lock()
            .expect("run registry mutex poisoned");
        let busy = by_generic
            .get(&record.session_id)
            .and_then(|rid| by_run.get(rid.as_str()))
            .is_some_and(|r| !r.is_terminal());
        if busy {
            return Err(crate::errors::OpenCodeError::SessionBusy {
                session_id: record.session_id.clone(),
            });
        }
        // A terminal predecessor is replaced (its in-flight stream events are
        // done by the time it became terminal; stragglers are dropped by the
        // router, which is correct per the no-late-events gate §166).
        by_generic.insert(record.session_id.clone(), record.run_id.to_string());
        self.by_engine_session
            .lock()
            .expect("run registry mutex poisoned")
            .insert(record.engine_session_id.clone(), record.run_id.to_string());
        by_run.insert(record.run_id.to_string(), record);
        Ok(())
    }

    pub(crate) fn get(&self, run_id: &str) -> Option<Arc<RunRecord>> {
        self.by_run
            .lock()
            .expect("run registry mutex poisoned")
            .get(run_id)
            .cloned()
    }

    /// Active run for an upstream (engine) session id — SSE wire routing.
    pub(crate) fn active_for_session(&self, session_id: &str) -> Option<Arc<RunRecord>> {
        let by_run = self.by_run.lock().expect("run registry mutex poisoned");
        let by_engine = self
            .by_engine_session
            .lock()
            .expect("run registry mutex poisoned");
        let run_id = by_engine.get(session_id)?;
        by_run.get(run_id.as_str()).cloned()
    }

    /// The upstream session id for a generic session id with an active run
    /// (permission resolution, TASK 24 §9).
    pub(crate) fn engine_session_for_generic(&self, session_id: &str) -> Option<String> {
        let by_run = self.by_run.lock().expect("run registry mutex poisoned");
        let by_generic = self
            .by_generic_session
            .lock()
            .expect("run registry mutex poisoned");
        let run_id = by_generic.get(session_id)?;
        by_run
            .get(run_id.as_str())
            .map(|r| r.engine_session_id.clone())
    }

    /// Live runs as (generic session id, run id) pairs — the adapter's
    /// contribution to the core's lag-reconciliation (TASK 24 §9).
    pub(crate) fn list_active(&self) -> Vec<(String, String)> {
        self.by_run
            .lock()
            .expect("run registry mutex poisoned")
            .values()
            .filter(|r| !r.is_terminal())
            .map(|r| (r.session_id.clone(), r.run_id.to_string()))
            .collect()
    }

    /// Remove a run (after its terminal event was emitted). Only clears the
    /// session mappings if they still point at this run (a newer run may have
    /// taken over the session in the terminal-grace window).
    pub(crate) fn remove(&self, run_id: &str) -> Option<Arc<RunRecord>> {
        let record = self
            .by_run
            .lock()
            .expect("run registry mutex poisoned")
            .remove(run_id);
        if let Some(record) = &record {
            let mut by_engine = self
                .by_engine_session
                .lock()
                .expect("run registry mutex poisoned");
            if by_engine
                .get(&record.engine_session_id)
                .is_some_and(|rid| rid == run_id)
            {
                by_engine.remove(&record.engine_session_id);
            }
            let mut by_generic = self
                .by_generic_session
                .lock()
                .expect("run registry mutex poisoned");
            if by_generic
                .get(&record.session_id)
                .is_some_and(|rid| rid == run_id)
            {
                by_generic.remove(&record.session_id);
            }
        }
        record
    }

    /// Remove and fail every active run of a generation (process crash /
    /// engine stop, §78–§80). Returns the records so the caller can emit
    /// terminals and terminate their POST tasks. Terminal emission is still
    /// gated by the run's `terminal_emitted` CAS, so a racing POST task can
    /// never double-emit.
    pub(crate) fn take_all(&self, generation: u64, reason: &str) -> Vec<Arc<RunRecord>> {
        let mut by_run = self.by_run.lock().expect("run registry mutex poisoned");
        let mut by_engine = self
            .by_engine_session
            .lock()
            .expect("run registry mutex poisoned");
        let mut by_generic = self
            .by_generic_session
            .lock()
            .expect("run registry mutex poisoned");
        let mut taken = Vec::new();
        by_run.retain(|run_id, record| {
            if record.generation == generation {
                *record.state.lock().expect("run state mutex poisoned") = RunState::Failed;
                *record
                    .session_error
                    .lock()
                    .expect("run error mutex poisoned") = Some(reason.to_string());
                by_engine.remove(&record.engine_session_id);
                by_generic.remove(&record.session_id);
                taken.push(record.clone());
                let _ = run_id;
                false
            } else {
                true
            }
        });
        taken
    }

    /// Cancel-request flag set by `cancel()`. Returns the record if the run
    /// is active and abort delivery has not yet succeeded.
    pub(crate) fn request_cancel(&self, run_id: &str) -> Option<Arc<RunRecord>> {
        let record = self.get(run_id)?;
        if record.is_terminal() {
            return None; // already terminal: no-op (§47)
        }
        if record.abort_delivered.load(Ordering::SeqCst) {
            return None; // already delivered: idempotent no-op (§47, §63)
        }
        record.cancel_requested.store(true, Ordering::SeqCst);
        Some(record)
    }
}
