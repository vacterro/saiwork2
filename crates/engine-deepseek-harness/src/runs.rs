//! Adapter-local active-run registry (TASK 21 §28–§29, §71, §103).
//!
//! One owner of run state: the adapter. A `RunRecord` tracks RunId ↔
//! SAIWORK2 session ↔ Harness session, the runtime generation (stale-event
//! guard), the cancel request (single CAS owner), the exactly-one-terminal
//! gate, the upstream message id (optional in ACP v1), and the owned prompt
//! task. Nothing here is durable — Harness owns the session log; SAIWORK2
//! owns the normalized live projection (§6, §103).
//!
//! Same-session concurrency is REJECT (one in-flight `session/prompt` per ACP
//! session, §80–§81): `insert` refuses a second active run for the same
//! SAIWORK2 session. Different sessions are independent (`parallel_sessions`).
//!
//! Lock ordering invariant: `by_run` → `by_session` → `by_harness` (std
//! Mutexes are never held across an await, but the order is still kept
//! consistent to rule out deadlock). Lookups that need two maps read the id
//! under the first lock, drop it, then read the record.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use saiwork_events::{Event, EventBus, RunId};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::HarnessError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunState {
    Running,
    Completed,
    Failed,
    Cancelled,
}

pub(crate) struct RunRecord {
    pub run_id: RunId,
    /// SAIWORK2 session id (the generic surface; SendRequest.session_id).
    pub session_id: String,
    /// Upstream Harness/ACP session id (opaque; Harness-owned, §7).
    pub harness_session_id: String,
    /// Runtime generation this run belongs to (stale events never cross
    /// generations, §55/§107).
    pub generation: u64,
    /// Set by `cancel()` (CAS); read by the prompt task, which is the single
    /// owner of the `session/cancel` protocol write (§64–§65).
    pub cancel_requested: AtomicBool,
    /// Cancel signal: `cancel()` sends here to wake the prompt task (which
    /// performs the protocol write). The CAS on `cancel_requested` is the
    /// single owner of the transition; the watch is just the wake-up.
    pub cancel_tx: watch::Sender<bool>,
    pub cancel_rx: watch::Receiver<bool>,
    /// `message.started` emitted exactly once (CAS, §30).
    pub started_emitted: AtomicBool,
    /// First-execution-evidence signal (TASK 24 §9): the prompt task sends
    /// `SendAcceptance::Accepted` ONLY on this — the first routed
    /// session/update for the run — or on the successful final prompt
    /// response. A frame write is NOT acceptance (the runtime may still
    /// reject the turn). `mark_started` sets it; the prompt task subscribes.
    pub started_tx: watch::Sender<bool>,
    pub started_rx: watch::Receiver<bool>,
    /// Upstream assistant message id, if the runtime provided one (optional
    /// in ACP v1; adapter-internal, §31 — never one message per chunk).
    pub message_id: Mutex<Option<String>>,
    /// Tool call ids that already reached a terminal state (exactly one
    /// terminal per ToolCallId, §52; late output after terminal is ignored).
    pub terminal_tools: Mutex<std::collections::HashSet<String>>,
    /// Terminal watch: the prompt task signals it when it emits the terminal,
    /// so the permission handler settles fail-closed (§70, §72).
    pub terminal_tx: watch::Sender<bool>,
    pub terminal_rx: watch::Receiver<bool>,
    pub state: Mutex<RunState>,
    /// Exactly-one-terminal gate (§67, §120).
    pub terminal_emitted: AtomicBool,
    /// Owned prompt task; joined/aborted on cleanup (§169).
    pub prompt_task: Mutex<Option<JoinHandle<()>>>,
}

impl RunRecord {
    pub fn is_terminal(&self) -> bool {
        *self.state.lock().expect("run state mutex poisoned") != RunState::Running
    }

    /// Publish `message.started` exactly once, on the first authoritative
    /// evidence that the runtime accepted the request (§30: the run started —
    /// the prompt task emits it at dispatch; the dispatcher also calls this on
    /// the first routed event, so whichever comes first wins).
    pub fn mark_started(&self, bus: &EventBus) {
        if self
            .started_emitted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            bus.publish(Event::MessageStarted {
                session_id: self.session_id.clone().into(),
                run_id: self.run_id.clone(),
            });
        }
        // Signal any waiting prompt task (first execution evidence → the
        // authoritative acceptance boundary, TASK 24 §9).
        let _ = self.started_tx.send(true);
    }

    /// Record the upstream assistant message id (first chunk wins; one
    /// canonical MessageId per upstream message, §31).
    pub fn note_message_id(&self, id: &str) {
        let mut slot = self.message_id.lock().expect("message id mutex poisoned");
        if slot.is_none() {
            *slot = Some(id.to_string());
        }
    }

    /// Wake the prompt task so it performs the `session/cancel` protocol write
    /// (idempotent; the prompt task guards with its own `cancel_sent` flag).
    pub fn cancel_tx_send(&self) -> bool {
        self.cancel_tx.send(true).is_ok()
    }
}

#[derive(Default)]
pub(crate) struct RunRegistry {
    by_run: Mutex<HashMap<String, Arc<RunRecord>>>,
    by_session: Mutex<HashMap<String, String>>,
    by_harness: Mutex<HashMap<String, String>>,
}

impl RunRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register a run. Fails on a duplicate run id (impossible — fresh UUIDs)
    /// or an active run in the same SAIWORK2 session (same-session REJECT).
    pub(crate) fn insert(&self, record: Arc<RunRecord>) -> Result<(), HarnessError> {
        let mut by_run = self.by_run.lock().expect("run registry mutex poisoned");
        if by_run.contains_key(record.run_id.as_str()) {
            return Err(HarnessError::Internal("duplicate run id".into()));
        }
        let busy = self
            .by_session
            .lock()
            .expect("run registry mutex poisoned")
            .get(&record.session_id)
            .and_then(|rid| by_run.get(rid.as_str()))
            .is_some_and(|r| !r.is_terminal());
        if busy {
            return Err(HarnessError::SessionBusy {
                session_id: record.session_id.clone(),
            });
        }
        self.by_session
            .lock()
            .expect("run registry mutex poisoned")
            .insert(record.session_id.clone(), record.run_id.to_string());
        self.by_harness
            .lock()
            .expect("run registry mutex poisoned")
            .insert(record.harness_session_id.clone(), record.run_id.to_string());
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

    /// Active run for a Harness session id (notification routing, §33–§34).
    pub(crate) fn active_for_harness(&self, harness_session_id: &str) -> Option<Arc<RunRecord>> {
        let run_id = self
            .by_harness
            .lock()
            .expect("run registry mutex poisoned")
            .get(harness_session_id)?
            .clone();
        self.get(&run_id)
    }

    /// Remove a run after its terminal was emitted. Only clears the session
    /// mappings if they still point at this run (a newer run may have taken
    /// over the session in the terminal-grace window).
    pub(crate) fn remove(&self, run_id: &str) -> Option<Arc<RunRecord>> {
        let record = self
            .by_run
            .lock()
            .expect("run registry mutex poisoned")
            .remove(run_id);
        if let Some(record) = &record {
            let mut by_session = self.by_session.lock().expect("run registry mutex poisoned");
            if by_session
                .get(&record.session_id)
                .is_some_and(|rid| rid == run_id)
            {
                by_session.remove(&record.session_id);
            }
            let mut by_harness = self.by_harness.lock().expect("run registry mutex poisoned");
            if by_harness
                .get(&record.harness_session_id)
                .is_some_and(|rid| rid == run_id)
            {
                by_harness.remove(&record.harness_session_id);
            }
        }
        record
    }

    /// Active run count (resource-cleanliness tests).
    pub(crate) fn active_count(&self) -> usize {
        self.by_run
            .lock()
            .expect("run registry mutex poisoned")
            .values()
            .filter(|r| !r.is_terminal())
            .count()
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

    /// Fail every active run of a generation (engine crash / stop, §71–§73).
    /// Returns the records so the caller can emit terminals and settle their
    /// prompt tasks. Terminal emission is still gated by each run's
    /// `terminal_emitted` CAS, so a racing prompt task can never double-emit.
    pub(crate) fn take_all(&self, generation: u64, _reason: &str) -> Vec<Arc<RunRecord>> {
        let mut by_run = self.by_run.lock().expect("run registry mutex poisoned");
        let mut by_session = self.by_session.lock().expect("run registry mutex poisoned");
        let mut by_harness = self.by_harness.lock().expect("run registry mutex poisoned");
        let mut taken = Vec::new();
        by_run.retain(|run_id, record| {
            if record.generation == generation {
                *record.state.lock().expect("run state mutex poisoned") = RunState::Failed;
                by_session.remove(&record.session_id);
                by_harness.remove(&record.harness_session_id);
                taken.push(record.clone());
                let _ = run_id;
                false
            } else {
                true
            }
        });
        taken
    }

    /// Cancel-request flag set by `cancel()`. Returns the record only on the
    /// first call: the CAS is the single owner of the cancellation transition,
    /// so cancel spam can never produce a protocol flood (§65). The prompt
    /// task performs the actual `session/cancel` write.
    pub(crate) fn request_cancel(&self, run_id: &str) -> Option<Arc<RunRecord>> {
        let record = self.get(run_id)?;
        if record.is_terminal() {
            return None; // already terminal: no-op (§66)
        }
        if record
            .cancel_requested
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None; // already requested: idempotent no-op (§65)
        }
        Some(record)
    }
}
