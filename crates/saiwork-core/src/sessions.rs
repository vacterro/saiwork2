//! SessionManager (spec §37, TASK 24 §9).
//!
//! SAIWORK2 session metadata never duplicates engine authority: content
//! history stays with the engine. This manager persists metadata (engine id,
//! engine session id, workspace, display meta) and orchestrates the adapter
//! calls.
//!
//! **Session identity is engine-independent**: `Session.id` is a generic
//! SAIWORK2 UUID minted here; the raw engine session id is retained only as
//! `Session.engine_session_id`. Both ids cross the adapter boundary
//! (`CreateSessionRequest.session_id` / `SendRequest.engine_session_id`), so
//! canonical events (`message.*`, `tool.*`, `permission.*`) always carry the
//! generic id while upstream calls use the engine's own id. A second engine
//! can never overwrite the first engine's session after restart — the DB
//! `PRIMARY KEY(id)` is a SAIWORK2 UUID, not an engine-controlled value.
//!
//! `session.*` events are published only here (the sole normalized lifecycle
//! publisher); adapters return upstream facts and never publish them.
//!
//! Same-workspace serialization (TASK 18 §21–§22): one mutating agent run
//! per physical workspace is the correctness boundary. `send` reserves the
//! workspace **atomically** (check + mark under one write lock, before any
//! await), so two concurrent sends to the same workspace can never both
//! reach the engine. The reservation (`running = true`) is released only by
//! the authoritative `message.*` terminal — `cancel` never releases it,
//! because adapter cancellation is only a request and the old agent may
//! still be mutating files.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use saiwork_events::{Event, EventBus};
use saiwork_storage::{Db, SessionMetaRow};
use serde::Serialize;
use uuid::Uuid;

use crate::engine::{
    CreateSessionRequest, EngineError, EngineRegistry, RunHandle, SendAcceptance, SendRequest,
    SessionCreation,
};
use crate::error::CoreError;

/// PERF-003: cap on the number of sessions the frontend `list_sessions` IPC
/// returns in one projection. A workspace with an unbounded session history
/// must never materialize the whole set into a single outgoing `Vec<Session>`
/// (no unbounded anything, §ARCHITECTURE).
pub const RECENT_SESSION_CAP: usize = 256;

#[derive(Debug, Clone, Serialize)]
pub struct Session {
    /// Generic SAIWORK2 session id (engine-independent, unique, durable).
    pub id: String,
    pub workspace_id: Option<String>,
    pub engine_id: String,
    /// Raw engine session id (upstream). Used only for upstream calls, never
    /// for event correlation.
    pub engine_session_id: String,
    pub display_name: String,
    pub created_at: i64,
    /// True while a run is active in this session (the workspace
    /// reservation). Set atomically by `send`; cleared only by the
    /// authoritative `message.*` terminal.
    pub running: bool,
    /// Non-releasing workspace reservation for an `OutcomeUnknown` run: the
    /// external agent may still be live and mutating, so the workspace stays
    /// reserved even though ordinary liveness (`running`) may be cleared by
    /// lag reconciliation. Only a matching authoritative terminal
    /// (`message.completed|failed|cancelled` with the SAME run_id), proven
    /// engine/process death, or an explicit risk-confirmed resolution may
    /// clear it (TASK 24 §9). Distinct from `running` — lag reconciliation
    /// must preserve it.
    pub unknown_run: Option<String>,
    /// The exact active RunId of the CURRENT run (normal or unknown): set on
    /// acceptance, cleared only by the matching authoritative terminal. The
    /// frontend reconciliation snapshot uses it to reconstruct `running`
    /// ownership after reload/lag, so Cancel always targets the real run and
    /// Send gating is exact (TASK 24 §9).
    pub active_run: Option<String>,
    /// AUDIT-CORE-001: true while a send has reserved the workspace
    /// (`running = true`) but its acceptance receipt has not resolved yet —
    /// `active_run` is still None because the RunId only exists after the
    /// engine accepts. This makes the pending-acceptance window an explicit
    /// state instead of overloading `running=true, active_run=None`:
    /// `note_terminal` must NEVER release a pending reservation (a terminal
    /// in that window belongs to some older run, or to this run before its
    /// own acceptance — either way ownership is resolved when the receipt
    /// lands and consumes/observes `terminal_runs`). Purely internal; never
    /// serialized to the frontend DTO (same rule as `terminal_runs`).
    #[serde(skip)]
    pub pending_send: bool,
    /// Short-lived exclusive reservation for session maintenance (delete or
    /// revert). It participates in the same workspace gate as a run and is
    /// released by RAII even when an async command is cancelled.
    #[serde(skip)]
    pub maintenance: bool,
    /// False for legacy metadata whose upstream id was NULL/empty (migration
    /// v4): historical display only — send/queue paths reject it with
    /// `SessionNotResumable` and no engine call ever sees an empty id
    /// (TASK 24 §9). Strictly means “survives runtime/app restart” — it does
    /// NOT mean “usable right now” (TASK 24 §9).
    pub resumable: bool,
    /// Transient usability with the CURRENT engine runtime generation — the
    /// field the UI gates selection/send on. A fresh connection-owned
    /// (resume=false, e.g. Harness/Generic) session is `usable_now=true`
    /// right after creation even though `resumable=false`; after the runtime
    /// stops/restarts its old sessions become `usable_now=false` history.
    /// OpenCode (resume=true) sessions are usable whenever the engine is
    /// READY (revalidated on use). Never fabricated by the frontend — the
    /// backend computes it from the engine's current generation.
    pub usable_now: bool,
    /// Terminal run tombstones (CORE-012 / CORE-003): the set of run_ids that
    /// have authoritatively terminated in this session. A terminal for run X
    /// permanently dominates any later duplicate MessageStarted/Accepted/
    /// OutcomeUnknown for X, and — critically — recording a terminal for run A
    /// NEVER erases the knowledge that run B is terminal. Because the record
    /// is per-run (not a single replaceable scalar), a late duplicate
    /// start/accept of an already-terminal run is always ignored, so a
    /// completed run can never be resurrected by an out-of-order event from a
    /// different run.
    ///
    /// PERF-005: this is PURELY internal tombstone state. `Session` is
    /// serialized directly across the IPC boundary (`create_session` /
    /// `list_sessions`), so the field must be excluded from the DTO: leaking a
    /// monotonically growing `HashSet<run_id>` to the frontend is a needless
    /// payload and a confused-DTO smell. Reconciliation reads it in-memory
    /// only; the frontend never needs it.
    #[serde(skip)]
    pub terminal_runs: HashSet<String>,
}

pub struct SessionManager {
    db: Db,
    bus: EventBus,
    engines: Arc<EngineRegistry>,
    sessions: RwLock<HashMap<String, Session>>,
    /// Generic session ids whose upstream session was authoritatively
    /// validated this process (created here, or re-accessed through the
    /// engine's `resume_session`). Restored rows are validated on first use.
    validated: RwLock<HashSet<String>>,
    /// session_id → engine runtime generation at which it was created or
    /// validated. Drives `usable_now` for connection-owned (resume=false)
    /// sessions: usable only while this equals the CURRENT engine
    /// generation (TASK 24 §9).
    session_generations: RwLock<HashMap<String, u64>>,
}

struct MaintenanceGuard<'a> {
    sessions: &'a RwLock<HashMap<String, Session>>,
    session_id: String,
}

impl Drop for MaintenanceGuard<'_> {
    fn drop(&mut self) {
        if let Some(session) = self
            .sessions
            .write()
            .expect("session map mutex poisoned")
            .get_mut(&self.session_id)
        {
            session.maintenance = false;
        }
    }
}

impl SessionManager {
    pub fn new(db: Db, bus: EventBus, engines: Arc<EngineRegistry>) -> Self {
        Self {
            db,
            bus,
            engines,
            sessions: RwLock::new(HashMap::new()),
            validated: RwLock::new(HashSet::new()),
            session_generations: RwLock::new(HashMap::new()),
        }
    }

    /// Authoritative transient usability (TASK 24 §9): the session must have
    /// a trustworthy upstream id, its engine must be READY, and — for
    /// connection-owned (resume=false) engines — it must have been
    /// created/validated in the CURRENT runtime generation. `resumable` is
    /// NOT consulted here: a fresh non-resumable session is usable now.
    pub fn usable_now(&self, session: &Session) -> bool {
        if session.engine_session_id.is_empty() {
            return false;
        }
        let Some(engine) = self.engines.get(&session.engine_id) else {
            return false;
        };
        if !matches!(engine.health(), crate::engine::EngineHealth::Ready) {
            return false;
        }
        if engine.capabilities().resume {
            // Resumable sessions are usable whenever READY (revalidated on
            // use through the engine's own resume path).
            return true;
        }
        let gen = self.engines.generation(&session.engine_id);
        self.session_generations
            .read()
            .expect("session generations mutex poisoned")
            .get(&session.id)
            == Some(&gen)
    }

    fn note_validated_generation(&self, session_id: &str, engine_id: &str) {
        self.session_generations
            .write()
            .expect("session generations mutex poisoned")
            .insert(session_id.into(), self.engines.generation(engine_id));
    }

    /// Create a session through the engine and persist metadata. The generic
    /// session id is minted here; the engine creates its own upstream session
    /// and returns it as `engine_session_id`.
    pub async fn create(
        &self,
        engine_id: &str,
        workspace_id: Option<&str>,
        model: Option<&str>,
    ) -> Result<Session, CoreError> {
        let engine = self
            .engines
            .get(engine_id)
            .ok_or_else(|| EngineError::engine(engine_id, "unknown engine"))?;
        // W2-001: hold a SHARED binding-stability lease from binding validation
        // through the adapter acceptance boundary. A concurrent rebind
        // (stop→start B) takes the EXCLUSIVE lease and is fully sequenced
        // before/after this create, so the upstream session is always created
        // against the runtime this create validated — never "create under B,
        // persist workspace A". The owned guard survives the adapter `.await`.
        let _lease = self.engines.acquire_binding_read_lease(engine_id).await;
        self.validate_workspace_binding(engine_id, workspace_id)?;

        let session_id = Uuid::new_v4().to_string();
        let info = match engine
            .create_session(&CreateSessionRequest {
                session_id: session_id.clone(),
                workspace_id: workspace_id.map(String::from),
                model: model.map(String::from),
                title: None,
            })
            .await?
        {
            // Authoritative creation only. An ambiguous create may have
            // produced an upstream session — the caller must never retry
            // blindly (no orphan-session loops, TASK 24 §9).
            SessionCreation::Created {
                engine_session_id,
                display_name: _engine_display_name,
            } => engine_session_id,
            SessionCreation::DefinitelyNotCreated { code, message } => {
                return Err(CoreError::Engine(EngineError::engine(
                    engine_id,
                    format!("{code}: {message}"),
                )))
            }
            SessionCreation::CreationUnknown { message } => {
                return Err(CoreError::Engine(EngineError::OutcomeUnknown(format!(
                    "session creation outcome unknown: {message}"
                ))))
            }
        };
        let now = now_ms();
        // T-081: new sessions are named `<projectname>_<DD-MM-YYTHH:MM:SS>`.
        // The engine's generic title (often a session id) is meaningless to
        // the user; the workspace name + local timestamp is readable and
        // collision-free. The durable row and the returned Session both carry
        // it, so ThreadTabs/SessionList show the human name without a rename
        // pass. `workspace_id` is validated above (binding lease), so a
        // workspace name is derivable from the durable row when present.
        let project_name = workspace_id
            .map(|wid| self.db.get_workspace(wid).ok().flatten())
            .flatten()
            .map(|w| w.name)
            .unwrap_or_else(|| engine_id.to_string());
        let display_name = format!("{project_name}_{}", local_timestamp_for_name(now));
        // Resumability is supplied by the ADAPTER, never hardcoded (TASK 24
        // §9): an engine that declares `resume=false` (e.g. Harness) owns
        // connection-scoped sessions that die with its runtime, so the
        // durable row must NOT claim they can be re-accessed after restart.
        // OpenCode (`resume=true`) sessions stay resumable; Generic CLI keeps
        // its explicitly defined stateless behavior.
        let resumable = engine.capabilities().resume;
        // Usable NOW by construction: the engine is READY (the create
        // succeeded against the current runtime), even though a
        // connection-owned engine's session is not restart-resumable.
        let usable_now = true;
        let session = Session {
            id: session_id,
            workspace_id: workspace_id.map(String::from),
            engine_id: engine_id.to_string(),
            engine_session_id: info,
            display_name,
            created_at: now,
            running: false,
            unknown_run: None,
            active_run: None,
            pending_send: false,
            maintenance: false,
            terminal_runs: HashSet::new(),
            resumable,
            usable_now,
        };
        // AUDIT-W2-003: the metadata commit atomically requires the bound
        // workspace to still exist. A Forget that commits while the external
        // create was in flight leaves `persisted = false` — the orphan
        // reference is never written and the upstream session runs through
        // the same authoritative cleanup policy as a storage failure.
        let persisted = self.db.upsert_session_meta_checked(&SessionMetaRow {
            id: session.id.clone(),
            workspace_id: session.workspace_id.clone(),
            engine_id: session.engine_id.clone(),
            engine_session_id: Some(session.engine_session_id.clone()),
            display_name: Some(session.display_name.clone()),
            last_opened_at: Some(now),
            created_at: now,
            updated_at: now,
            resumable,
        });
        if let Err(storage_err) = persisted {
            // The upstream session ALREADY EXISTS (authoritative external
            // mutation) but the local metadata commit failed. A blind retry
            // would create an orphan/duplicate external session. Attempt
            // bounded authoritative cleanup first (TASK 24 §9):
            //  - cleanup succeeds → the caller may safely retry;
            //  - cleanup unsupported/fails → the upstream session may still
            //    exist: surface typed ambiguity and prohibit automatic
            //    retry.
            match engine.delete_session(&session.engine_session_id).await {
                Ok(()) => {
                    return Err(CoreError::Storage(storage_err));
                }
                Err(cleanup_err) => {
                    return Err(CoreError::Engine(EngineError::OutcomeUnknown(format!(
                        "session '{}' was created upstream but the local metadata commit failed ({storage_err}); authoritative cleanup also failed ({cleanup_err}) — retrying may create a duplicate external session",
                        session.engine_session_id
                    ))));
                }
            }
        }
        if !persisted? {
            // AUDIT-W2-003: the workspace disappeared while the upstream
            // create was in flight. The metadata row was NOT written; clean
            // up the now-orphaned upstream session (same policy as above —
            // a typed upstream SessionNotFound means it is already gone,
            // T-012) and fail with the typed missing-workspace error so a
            // retry against a live workspace is safe.
            match engine.delete_session(&session.engine_session_id).await {
                Ok(()) | Err(EngineError::SessionNotFound { .. }) => {
                    return Err(CoreError::WorkspaceNotFound(
                        session
                            .workspace_id
                            .clone()
                            .unwrap_or_else(|| "<gone>".into()),
                    ));
                }
                Err(cleanup_err) => {
                    return Err(CoreError::Engine(EngineError::OutcomeUnknown(format!(
                        "workspace vanished during session creation and upstream session '{}' could not be cleaned up ({cleanup_err}) — retrying may create a duplicate external session",
                        session.engine_session_id
                    ))));
                }
            }
        }

        self.sessions
            .write()
            .expect("session map mutex poisoned")
            .insert(session.id.clone(), session.clone());
        // Freshly created sessions are authoritative by construction.
        self.validated
            .write()
            .expect("validated set mutex poisoned")
            .insert(session.id.clone());
        self.note_validated_generation(&session.id, &session.engine_id);
        // Sole normalized session.* lifecycle publisher. Carries the FULL
        // authoritative DTO so the frontend never fabricates workspace/
        // upstream-id/display-name/resumable/usable_now from local UI state
        // (TASK 24 §9).
        self.bus.publish(Event::SessionCreated {
            session_id: session.id.clone().into(),
            engine_id: session.engine_id.clone().into(),
            workspace_id: session.workspace_id.clone().map(Into::into),
            engine_session_id: session.engine_session_id.clone(),
            display_name: session.display_name.clone(),
            created_at: session.created_at,
            resumable: session.resumable,
            usable_now: session.usable_now,
        });
        Ok(session)
    }

    /// Authoritative session deletion used for cross-authority compensation
    /// (TASK 24 §9): an upstream session that exists but whose local
    /// metadata / durable queue-row persistence failed must be deleted
    /// before any safe retry, or the retry would create an orphan/duplicate
    /// external session. Resolves the session, calls the engine's
    /// authoritative delete, removes the local projection + durable row, and
    /// publishes `session.closed`. `Ok` = upstream deletion proven; `Err` =
    /// cleanup failed/unsupported (the caller must fail closed and never
    /// auto/manual retry as a clean NewSession).
    pub async fn delete_session(&self, session_id: &str) -> Result<(), CoreError> {
        let (session, _maintenance) = self.reserve_maintenance(session_id)?;
        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        // Compensation boundary (TASK 24 §9): a typed upstream
        // `SessionNotFound` means the upstream session was ALREADY deleted
        // (e.g. a prior call deleted it but then failed to persist the local
        // row, and the retry observes a non-authoritative NotFound). Treat it
        // as already-deleted success and continue the durable/local cleanup
        // below — otherwise the retry would die here and leave the surviving
        // local metadata row (and its `session.closed` never published),
        // contradicting the documented retry-recovery path. Every OTHER
        // upstream error is propagated fail-closed.
        match engine.delete_session(&session.engine_session_id).await {
            Ok(()) => {}
            Err(EngineError::SessionNotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        // Durable deletion is part of successful local cleanup (TASK 24 §9):
        // a DB busy/full failure must NOT let the deleted upstream session
        // resurrect after restart, and the operation must never report clean
        // success or publish a definitive `session.closed` while the durable
        // row still exists. Fail closed: the in-memory projection stays (it
        // remains consistent with the durable row), no event is published,
        // and a retry after storage recovers removes the row.
        self.db.delete_session_meta(session_id)?;
        self.sessions
            .write()
            .expect("session map mutex poisoned")
            .remove(session_id);
        self.validated
            .write()
            .expect("validated set mutex poisoned")
            .remove(session_id);
        // PERF-008: reclaim the per-session validity-cache entry. The id is a
        // fresh UUID that can never become useful again after durable deletion,
        // so retaining it (app-lifetime) would monotonically grow a HashMap
        // across create/delete cycles. The removal shares the exact durable/local
        // deletion boundary already used for `sessions`/`validated`.
        self.session_generations
            .write()
            .expect("session generations mutex poisoned")
            .remove(session_id);
        self.bus.publish(Event::SessionClosed {
            session_id: session_id.into(),
        });
        Ok(())
    }

    /// Revert the last currently-visible user turn. The core chooses the
    /// boundary from authoritative engine history, so the UI never guesses
    /// upstream message ids and cannot revert a stale/local-only projection.
    pub async fn revert_last_turn(&self, session_id: &str) -> Result<(), CoreError> {
        let (session, _maintenance) = self.reserve_maintenance(session_id)?;
        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        if !engine.capabilities().session_revert {
            return Err(EngineError::UnsupportedCapability {
                engine_id: session.engine_id,
                capability: "session_revert",
            }
            .into());
        }
        let history = engine
            .session_history(&session.engine_session_id)
            .await?
            .ok_or_else(|| EngineError::UnsupportedCapability {
                engine_id: session.engine_id.clone(),
                capability: "session_history",
            })?;
        let message_id = history
            .iter()
            .rev()
            .find(|message| message.role == "user")
            .map(|message| message.id.clone())
            .ok_or_else(|| EngineError::engine(&session.engine_id, "no user turn to undo"))?;
        engine
            .revert_session(&session.engine_session_id, &message_id)
            .await
            .map_err(CoreError::Engine)
    }

    pub async fn unrevert(&self, session_id: &str) -> Result<(), CoreError> {
        let (session, _maintenance) = self.reserve_maintenance(session_id)?;
        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        if !engine.capabilities().session_revert {
            return Err(EngineError::UnsupportedCapability {
                engine_id: session.engine_id,
                capability: "session_revert",
            }
            .into());
        }
        engine
            .unrevert_session(&session.engine_session_id)
            .await
            .map_err(CoreError::Engine)
    }

    fn reserve_maintenance(
        &self,
        session_id: &str,
    ) -> Result<(Session, MaintenanceGuard<'_>), CoreError> {
        let session = {
            let mut map = self.sessions.write().expect("session map mutex poisoned");
            let target = map
                .get(session_id)
                .cloned()
                .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
            if target.running
                || target.pending_send
                || target.unknown_run.is_some()
                || target.maintenance
            {
                return Err(CoreError::SessionBusy { session_id: session_id.into() });
            }
            if let Some((active_session, workspace)) = workspace_has_active_run_locked(&map, &target) {
                return Err(CoreError::WorkspaceBusy {
                    workspace_id: workspace,
                    active_session_id: active_session,
                    attempted_session_id: target.id.clone(),
                });
            }
            map.get_mut(session_id).expect("session vanished under write lock").maintenance = true;
            target
        };
        Ok((session, MaintenanceGuard { sessions: &self.sessions, session_id: session_id.into() }))
    }

    /// Reject a create/send whose workspace differs from the workspace the
    /// engine runtime is bound to. A runtime's cwd is fixed at start; a
    /// session for a different workspace must never execute against it
    /// (TASK 24 §9). Engines started without a binding accept any workspace.
    fn validate_workspace_binding(
        &self,
        engine_id: &str,
        workspace_id: Option<&str>,
    ) -> Result<(), CoreError> {
        match self.engines.bound_workspace(engine_id) {
            None => Ok(()), // engine not started: the engine call will fail
            Some(None) => Ok(()), // started without a workspace binding
            Some(Some(bound)) => {
                if workspace_id != Some(bound.as_str()) {
                    return Err(CoreError::WorkspaceMismatch {
                        engine_id: engine_id.to_string(),
                        expected_workspace_id: bound,
                        requested_workspace_id: workspace_id.unwrap_or("<none>").to_string(),
                    });
                }
                Ok(())
            }
        }
    }

    /// Send a prompt to a session; returns the run handle. The engine emits
    /// `message.started`/`message.delta`/`message.completed|failed`.
    ///
    /// Same-workspace serialization (TASK 18 §21–§22): the workspace is
    /// **reserved atomically** — the check and the `running = true` mark
    /// happen under one write lock with no await in between, so two
    /// concurrent sends to sessions of the same workspace can never both
    /// observe idle and both reach the engine. The reservation is released
    /// only on a definite pre-accept failure, by the authoritative terminal
    /// event (tracker), or — for an `OutcomeUnknown` receipt — when the
    /// engine's own terminal proves the run is over (the run may still be
    /// live and mutating, so the workspace stays reserved until then).
    /// Different workspaces stay independent.
    pub async fn send(
        &self,
        session_id: &str,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<RunHandle, CoreError> {
        match self.send_acceptance(session_id, prompt, model).await? {
            SendAcceptance::Accepted { run_id } => {
                self.bus.publish(Event::SessionChanged {
                    session_id: session_id.into(),
                });
                Ok(RunHandle { run_id })
            }
            SendAcceptance::DefinitelyRejected { run_id: _, code, message } => {
                self.set_running(session_id, false);
                Err(CoreError::Engine(EngineError::engine(
                    &self
                        .sessions
                        .read()
                        .expect("session map mutex poisoned")
                        .get(session_id)
                        .map(|s| s.engine_id.clone())
                        .unwrap_or_else(|| "?".into()),
                    format!("{code}: {message}"),
                )))
            }
            SendAcceptance::OutcomeUnknown { run_id: _, message } => {
                // The run may still be live upstream: keep the workspace
                // reserved until the engine's own terminal releases it.
                Err(CoreError::Engine(EngineError::OutcomeUnknown(format!(
                    "send outcome unknown: {message}"
                ))))
            }
        }
    }

    /// The shared send core (atomic reserve + engine call). Returns the
    /// authoritative receipt; callers map it to their own surface.
    ///
    /// Reservation safety (TASK 24 §9): every synchronous pre-accept error
    /// (session missing/busy, workspace busy, workspace-vs-engine binding
    /// mismatch, unknown engine) is resolved BEFORE the workspace is reserved,
    /// so a pre-send failure can never leak `running = true`. The actual
    /// check + mark still happens under one write lock with no await in
    /// between, preserving the same-workspace serialization gate.
    async fn send_acceptance(
        &self,
        session_id: &str,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<SendAcceptance, CoreError> {
        // Synchronous preflight (no reservation yet): resolve the session and
        // run every local gate that can fail before we commit to reserving.
        // All of these are non-awaiting, so no async state can change between
        // the checks; the write-lock re-check below still defends against a
        // concurrent send that raced this preflight.
        let (session, engine, _lease) = {
            // `map` (a non-`Send` std RwLock guard) is scoped to this inner
            // block: it is only needed for the synchronous preflight lookup and
            // MUST be fully dropped before the async binding-lease acquisition
            // below, otherwise the `check_send_readiness` future is not `Send`
            // (it is awaited behind `queue_port::send`, which requires `Send`).
            // W2-001 fix.
            let session = {
                let map = self.sessions.read().expect("session map mutex poisoned");
                let s = map
                    .get(session_id)
                    .cloned()
                    .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
                if s.running || s.unknown_run.is_some() || s.maintenance {
                    return Err(CoreError::SessionBusy {
                        session_id: session_id.into(),
                    });
                }
                if let Some((active_session, workspace)) =
                    workspace_has_active_run_locked(&map, &s)
                {
                    return Err(CoreError::WorkspaceBusy {
                        workspace_id: workspace,
                        active_session_id: active_session,
                        attempted_session_id: s.id.clone(),
                    });
                }
                s
            };
            // Rows without a trustworthy upstream id are historical display
            // only — reject before any engine call or reservation (TASK 24
            // §9). Note: this is the EMPTY upstream id gate, NOT `resumable`
            // — a connection-owned (resume=false) session with a real
            // upstream id IS usable while its runtime is alive; only after
            // runtime-generation loss does it become non-resumable history.
            if session.engine_session_id.is_empty() {
                return Err(CoreError::SessionNotResumable(session_id.into()));
            }
            // Binding validation and engine lookup happen BEFORE the
            // reservation: a WorkspaceMismatch / unknown-engine error must
            // leave running=false and never block the workspace (TASK 24 §9).
            // W2-001: the SHARED binding-stability lease is taken here and held
            // through the downstream adapter `send` (returned out of this
            // block) so a concurrent rebind cannot flip the bound runtime under
            // this send (a send for workspace A must never execute against a
            // runtime rebound to B).
            let _lease = self
                .engines
                .acquire_binding_read_lease(&session.engine_id)
                .await;
            self.validate_workspace_binding(&session.engine_id, session.workspace_id.as_deref())?;
            let engine = self
                .engines
                .get(&session.engine_id)
                .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
            // Current-runtime usability gate (TASK 24 §9): distinct from the
            // empty-id gate above. A connection-owned (resume=false) session
            // is usable only while it was created/validated in the CURRENT
            // engine generation — after the runtime restarted, the old
            // session is unusable history and the adapter would reject its
            // dead upstream id anyway; fail closed BEFORE any reservation.
            if !self.usable_now(&session) {
                return Err(CoreError::SessionNotUsableNow {
                    session_id: session.id.clone(),
                });
            }
            (session, engine, _lease)
        };

        // Restored-session validation (TASK 24 §9): a metadata row hydrated
        // from SQLite after restart is NOT yet authoritative. Engines that
        // declare `resume` must re-access the upstream session through the
        // engine's own resume path before the session becomes usable — a
        // deleted upstream session fails closed with SessionNotFound, never
        // an in-memory resurrection. Engines without `resume` have no
        // upstream to re-access (one-shot/connection-owned sessions are
        // validated by the adapter at send time); the row is marked validated
        // so this check runs once per process. All of this happens BEFORE the
        // workspace reservation.
        if !self.is_validated(&session.id) {
            if engine.capabilities().resume {
                match engine.resume_session(&session.engine_session_id).await {
                    Ok(_) => {
                        self.mark_validated(&session.id);
                        self.note_validated_generation(&session.id, &session.engine_id);
                    }
                    Err(EngineError::SessionNotFound { .. }) => {
                        return Err(CoreError::SessionNotFound(session_id.into()));
                    }
                    Err(e) => return Err(CoreError::Engine(e)),
                }
            } else {
                self.mark_validated(&session.id);
                self.note_validated_generation(&session.id, &session.engine_id);
            }
        }

        // Atomic check + reserve: no await while the lock is held. The
        // preflight released the read lock, so re-verify the same gates a
        // concurrent send could have flipped in between.
        {
            let mut map = self.sessions.write().expect("session map mutex poisoned");
            let current = map
                .get(session_id)
                .cloned()
                .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
            if current.running || current.unknown_run.is_some() || current.maintenance {
                return Err(CoreError::SessionBusy {
                    session_id: session_id.into(),
                });
            }
            if let Some((active_session, workspace)) =
                workspace_has_active_run_locked(&map, &current)
            {
                return Err(CoreError::WorkspaceBusy {
                    workspace_id: workspace,
                    active_session_id: active_session,
                    attempted_session_id: current.id.clone(),
                });
            }
            if let Some(s) = map.get_mut(session_id) {
                s.running = true; // reserve the workspace before any await
                // AUDIT-CORE-001: mark the reservation as pending-acceptance.
                // Until the receipt resolves, no terminal may release this
                // gate (a terminal seen now belongs to an older run or races
                // THIS run's own acceptance — both are consumed at the
                // receipt boundary, never by note_terminal).
                s.pending_send = true;
            }
        }

        let result = engine
            .send(&SendRequest {
                session_id: session.id.clone(),
                engine_session_id: session.engine_session_id.clone(),
                prompt: prompt.to_string(),
                model: model.map(String::from),
            })
            .await;
        match result {
            Ok(receipt @ SendAcceptance::OutcomeUnknown { .. }) => {
                // Pin the non-releasing reservation IMMEDIATELY (not only via
                // the bus event, which lag could drop): the run may be live
                // and the workspace must stay reserved (TASK 24 §9).
                if let SendAcceptance::OutcomeUnknown { run_id, .. } = &receipt {
                    self.note_outcome_unknown(session_id, run_id.as_str());
                }
                Ok(receipt)
            }
            Ok(SendAcceptance::Accepted { run_id }) => {
                // Track the exact active RunId so reconciliation can
                // reconstruct Cancel/Send ownership after frontend reload or
                // EventBus lag (TASK 24 §9).
                self.note_started(session_id, &run_id);
                Ok(SendAcceptance::Accepted { run_id })
            }
            // CORE-005: a proven pre-execution rejection means nothing ran
            // upstream, so the workspace reservation must be released
            // synchronously, BEFORE the receipt is returned. This is the single
            // authority for the reservation: the typed UI path
            // (`send_scoped_receipt`) delegates here without its own cleanup,
            // and an immediate retry / concurrent same-workspace send must
            // reach the engine instead of failing SessionBusy/WorkspaceBusy.
            // The legacy `send()` and queue `send_for_dispatch` callers keep
            // their idempotent `set_running(false)` for DefinitelyRejected, but
            // no future receipt-returning surface can forget to release.
            Ok(SendAcceptance::DefinitelyRejected { run_id, code, message }) => {
                self.set_running(session_id, false);
                Ok(SendAcceptance::DefinitelyRejected { run_id, code, message })
            }
            Err(e) => {
                // Definite engine error before acceptance: release the
                // reservation (no run is registered).
                self.set_running(session_id, false);
                Err(e.into())
            }
        }
    }

    /// The scoped direct-send core: context validation + the authoritative
    /// `SendAcceptance` receipt, so the command layer can expose a TYPED
    /// outcome to the UI (accepted / definitely-rejected / outcome-unknown)
    /// instead of a flat command error — the UI must never drop a pending
    /// user turn for an outcome it cannot prove (TASK 24 §9).
    pub async fn send_scoped_receipt(
        &self,
        session_id: &str,
        expected_workspace_id: Option<&str>,
        expected_engine_id: Option<&str>,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<SendAcceptance, CoreError> {
        self.validate_send_context(session_id, expected_workspace_id, expected_engine_id)?;
        self.send_acceptance(session_id, prompt, model).await
    }

    /// Direct-send boundary returning a run handle (internal/queue-free use;
    /// the UI uses `send_scoped_receipt` for the typed outcome).
    pub async fn send_scoped(
        &self,
        session_id: &str,
        expected_workspace_id: Option<&str>,
        expected_engine_id: Option<&str>,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<RunHandle, CoreError> {
        self.validate_send_context(session_id, expected_workspace_id, expected_engine_id)?;
        self.send(session_id, prompt, model).await
    }

    /// Reject a direct send whose UI context does not match the session's own
    /// metadata — BEFORE any reservation or external call (TASK 24 §9).
    fn validate_send_context(
        &self,
        session_id: &str,
        expected_workspace_id: Option<&str>,
        expected_engine_id: Option<&str>,
    ) -> Result<(), CoreError> {
        let session = self
            .sessions
            .read()
            .expect("session map mutex poisoned")
            .get(session_id)
            .cloned()
            .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
        if let Some(expected) = expected_workspace_id {
            if session.workspace_id.as_deref() != Some(expected) {
                return Err(CoreError::SessionContextMismatch {
                    session_id: session_id.to_string(),
                    expected_engine_id: expected_engine_id.unwrap_or("?").to_string(),
                    expected_workspace_id: Some(expected.to_string()),
                    actual_engine_id: session.engine_id.clone(),
                    actual_workspace_id: session.workspace_id.clone(),
                });
            }
        }
        if let Some(expected) = expected_engine_id {
            if session.engine_id != expected {
                return Err(CoreError::SessionContextMismatch {
                    session_id: session_id.to_string(),
                    expected_engine_id: expected.to_string(),
                    expected_workspace_id: expected_workspace_id.map(String::from),
                    actual_engine_id: session.engine_id.clone(),
                    actual_workspace_id: session.workspace_id.clone(),
                });
            }
        }
        Ok(())
    }

    /// Queue-facing send: returns the authoritative `DispatchReceipt` so the
    /// queue commits DISPATCHED only on real engine acceptance. Shares the
    /// atomic workspace reservation with direct sends (TASK 24 §9).
    pub async fn send_for_dispatch(
        &self,
        session_id: &str,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<saiwork_queue::DispatchReceipt, CoreError> {
        Ok(match self.send_acceptance(session_id, prompt, model).await? {
            SendAcceptance::Accepted { run_id } => saiwork_queue::DispatchReceipt::Accepted { run_id },
            SendAcceptance::DefinitelyRejected { run_id, code, message } => {
                self.set_running(session_id, false);
                saiwork_queue::DispatchReceipt::DefinitelyRejected { run_id, code, message }
            }
            SendAcceptance::OutcomeUnknown { run_id, message } => {
                saiwork_queue::DispatchReceipt::OutcomeUnknown { run_id, message }
            }
        })
    }

    /// True when any *other* session in the same workspace already has an
    /// active run (the TASK 18 same-workspace serialization gate). Sessions
    /// without a workspace never block each other. Returns the offending
    /// session id + workspace id for a typed error.
    pub fn workspace_has_active_run(&self, target: &Session) -> Option<(String, String)> {
        let sessions = self.sessions.read().expect("session map mutex poisoned");
        workspace_has_active_run_locked(&sessions, target)
    }

    /// Queue-facing busy check: the session itself is running, OR another
    /// session in the same workspace is running (same-workspace gate). The
    /// queue treats this as Wait — it never claims an item that would race a
    /// concurrent workspace run.
    pub fn busy_for_dispatch(&self, session_id: &str) -> bool {
        let sessions = self.sessions.read().expect("session map mutex poisoned");
        let Some(target) = sessions.get(session_id) else {
            return false;
        };
        if target.running || target.unknown_run.is_some() || target.maintenance {
            return true;
        }
        let Some(ws) = target.workspace_id.as_deref() else {
            return false;
        };
        sessions.values().any(|s| {
            s.id != session_id
                && (s.running || s.unknown_run.is_some() || s.maintenance)
                && s.workspace_id.as_deref() == Some(ws)
        })
    }

    /// Cancel a running run. Adapter cancellation is only a request: the
    /// workspace/session reservation is NOT released here — it stays until
    /// the matching `message.cancelled`/`completed`/`failed` terminal (or an
    /// authoritative engine-loss terminal) arrives via the tracker, so a
    /// same-workspace second send stays blocked while the old agent may
    /// still be mutating files. Safe to call when nothing is running
    /// (returns Ok; repeated cancel is a no-op).
    pub async fn cancel(&self, session_id: &str, run_id: &str) -> Result<(), CoreError> {
        let session = self
            .sessions
            .read()
            .expect("session map mutex poisoned")
            .get(session_id)
            .cloned()
            .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;

        // CORE-013: Verify run ownership before delegating to the engine adapter.
        // If the session has a DIFFERENT active run, it's a caller mismatch error.
        let is_active = session.active_run.as_deref() == Some(run_id);
        let is_unknown = session.unknown_run.as_deref() == Some(run_id);

        if !is_active && !is_unknown {
            if session.active_run.is_some() || session.unknown_run.is_some() {
                // The caller supplied a stale/incorrect run_id while the session
                // is busy with another run. Reject it to protect the current run.
                return Err(CoreError::SessionRunMismatch {
                    session_id: session_id.into(),
                    run_id: run_id.into(),
                });
            } else {
                // The session is idle. This run_id is stale/already terminal.
                // Documented idempotent no-op: do not forward arbitrary run_ids
                // to the global engine registry.
                return Ok(());
            }
        }

        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        engine.cancel(run_id).await?;
        Ok(())
    }

    /// Resolve a pending `permission.requested` for a session's engine
    /// (Allow/Deny). Idempotent at the engine; the UI never fabricates the
    /// decision — the engine publishes the authoritative `permission.resolved`
    /// (TASK 16 §36–§38).
    pub async fn resolve_permission(
        &self,
        session_id: &str,
        request_id: &str,
        allowed: bool,
    ) -> Result<(), CoreError> {
        let session = self
            .sessions
            .read()
            .expect("session map mutex poisoned")
            .get(session_id)
            .cloned()
            .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        engine
            .resolve_permission(session_id, request_id, allowed)
            .await?;
        Ok(())
    }

    /// AUDIT-CORE-002: answer/reject a pending user question through the
    /// owning session's engine. Typed resolution (`QuestionResolution`) —
    /// never boolean permission semantics. The engine publishes the
    /// authoritative resolution; the UI card is torn down by that event.
    pub async fn resolve_question(
        &self,
        session_id: &str,
        request_id: &str,
        resolution: &crate::engine::QuestionResolution,
    ) -> Result<(), CoreError> {
        let session = self
            .sessions
            .read()
            .expect("session map mutex poisoned")
            .get(session_id)
            .cloned()
            .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        engine
            .resolve_question(session_id, request_id, resolution)
            .await?;
        Ok(())
    }

    /// Hydrate a session into the in-memory map from durable metadata when it
    /// is not already present (after restart the map is empty; sessions live
    /// in `sessions_meta`). Used by the queue port to validate/resume an
    /// existing target session (TASK 13 §28, §187).
    ///
    /// The resumable/trustworthy-upstream check (TASK 24 §9) is applied
    /// through ONE canonical path for BOTH in-memory and DB lookups: calling
    /// `list()` first (which hydrates rows) must never bypass the gate.
    pub fn ensure_loaded(&self, session_id: &str) -> Result<Session, CoreError> {
        // AUDIT-W2-001: remember whether this call HYDRATED the row from
        // durable metadata (the map missed) — the restore gate below applies
        // only to that case. An already-live in-memory session is validated
        // against CURRENT runtime usability instead, so a live
        // connection-owned (resume=false) session stays queue-targetable
        // while its creating generation is alive.
        let hydrated;
        let session = if let Some(s) = self.get(session_id) {
            hydrated = false;
            s
        } else {
            // Indexed point lookup (TASK 24 perf): one query on the primary
            // key — never a materialized full-table scan.
            let row = self
                .db
                .get_session_meta(session_id)?
                .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
            let session = self.row_to_session(&row);
            self.sessions
                .write()
                .expect("session map mutex poisoned")
                .insert(session_id.into(), session.clone());
            hydrated = true;
            session
        };
        if session.engine_session_id.is_empty() {
            // Rows without a trustworthy upstream id are historical display
            // only — no engine call may ever see an empty id (TASK 24 §9).
            return Err(CoreError::SessionNotResumable(session_id.into()));
        }
        if !session.resumable {
            if hydrated {
                // Restore rule (TASK 24 §9): a connection-owned row read back
                // from SQLite after restart cannot be re-accessed — the
                // durable queue must never target it. Fail closed with a
                // typed error; the placeholder must never reach an adapter or
                // a reservation.
                return Err(CoreError::SessionNotResumable(session_id.into()));
            }
            // AUDIT-W2-001: a LIVE non-resumable session is exactly as usable
            // as for direct send — valid only in its creation/validated
            // engine generation (`usable_now`). After the runtime restarted,
            // the same in-memory row is unusable history and is rejected
            // here with the SAME typed error direct send produces.
            if !self.usable_now(&session) {
                return Err(CoreError::SessionNotUsableNow {
                    session_id: session.id.clone(),
                });
            }
        }
        Ok(session)
    }

    /// In-memory-only drop of every session of a workspace (TASK 24 §9). The
    /// caller has ALREADY performed the safety checks (no active/unknown run)
    /// and the durable deletion (session rows + workspace row) in one atomic
    /// storage transaction; this only reconciles the live projection AFTER the
    /// durable commit so a failed durable step never strands a live-only entry.
    pub fn drop_workspace_sessions(&self, workspace_id: &str) {
        let mut map = self.sessions.write().expect("session map mutex poisoned");
        let doomed: Vec<String> = map
            .values()
            .filter(|s| s.workspace_id.as_deref() == Some(workspace_id))
            .map(|s| s.id.clone())
            .collect();
        for id in &doomed {
            map.remove(id);
        }
        let mut validated = self.validated.write().expect("validated set mutex poisoned");
        let mut generations = self
            .session_generations
            .write()
            .expect("session generations mutex poisoned");
        for id in &doomed {
            validated.remove(id);
            // PERF-008: reclaim the per-session validity-cache entry for every
            // dropped id. These are fresh UUIDs that can never become useful
            // again, so they must not accumulate across workspace forget cycles.
            generations.remove(id);
        }
    }

    /// List sessions, hydrating any durable rows that are not yet in the
    /// in-memory map (restart restoration: `sessions_meta` survives the
    /// process; the map does not). Filtered by workspace when requested.
    ///
    /// A durable read failure is PROPAGATED (TASK 24 §9): a transient/corrupt
    /// storage error must never look like an authoritative empty list.
    pub fn list(&self, workspace_id: Option<&str>) -> Result<Vec<Session>, CoreError> {
        // Merge durable metadata rows not yet in memory (restart case).
        let rows = self.db.list_session_meta(workspace_id)?;
        {
            let mut map = self.sessions.write().expect("session map mutex poisoned");
            for row in rows {
                if map.contains_key(&row.id) {
                    continue;
                }
                let session = self.row_to_session(&row);
                map.insert(row.id, session);
            }
        }
        let map = self.sessions.read().expect("session map mutex poisoned");
        let mut out: Vec<Session> = map
            .values()
            .filter(|s| workspace_id.is_none_or(|w| s.workspace_id.as_deref() == Some(w)))
            .cloned()
            .collect();
        // Refresh usable_now live: engine restarts must flip connection-owned
        // sessions to unusable in the returned projection (TASK 24 §9).
        for s in &mut out {
            s.usable_now = self.usable_now(s);
        }
        out.sort_by_key(|s| std::cmp::Reverse(s.created_at));
        Ok(out)
    }

    /// Bounded recent-session projection (PERF-003). Identical to `list` but
    /// the returned `Vec<Session>` is capped at `limit` of the most-recent
    /// sessions (the list is already sorted `created_at` desc, so truncation
    /// keeps the newest). Used by the frontend `list_sessions` IPC so a
    /// workspace with an unbounded session history can never materialize the
    /// whole set into one outgoing `Vec` (no unbounded anything,
    /// §ARCHITECTURE). Internal callers that must observe EVERY session — e.g.
    /// `forget_workspace`'s active-run scan — keep using the unbounded `list`.
    pub fn list_recent(
        &self,
        workspace_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Session>, CoreError> {
        let mut out = self.list(workspace_id)?;
        out.truncate(limit);
        Ok(out)
    }

    /// Hydrate an in-memory `Session` from a durable row. Non-resumable rows
    /// keep the empty placeholder ONLY as a display marker — every engine
    /// call path (`send_acceptance`, `ensure_loaded`) rejects them before any
    /// adapter receives an empty upstream id.
    fn row_to_session(&self, row: &SessionMetaRow) -> Session {
        let mut session = Session {
            id: row.id.clone(),
            workspace_id: row.workspace_id.clone(),
            engine_id: row.engine_id.clone(),
            engine_session_id: row.engine_session_id.clone().unwrap_or_default(),
            display_name: row.display_name.clone().unwrap_or_else(|| row.id.clone()),
            created_at: row.created_at,
            running: false,
            unknown_run: None,
            active_run: None,
            pending_send: false,
            maintenance: false,
            terminal_runs: HashSet::new(),
            resumable: row.resumable,
            usable_now: false,
        };
        session.usable_now = self.usable_now(&session);
        session
    }

    pub fn get(&self, session_id: &str) -> Option<Session> {
        // usable_now is derived live from the current engine generation —
        // the stored clone may be stale after an engine restart (TASK 24
        // §9).
        let mut session = self
            .sessions
            .read()
            .expect("session map mutex poisoned")
            .get(session_id)
            .cloned()?;
        session.usable_now = self.usable_now(&session);
        Some(session)
    }

    pub fn set_running(&self, session_id: &str, running: bool) {
        let mut map = self.sessions.write().expect("session map mutex poisoned");
        if let Some(s) = map.get_mut(session_id) {
            let mut changed = false;
            if s.running != running {
                s.running = running;
                changed = true;
            }
            if !running {
                // Releasing the reservation ends any pending-acceptance
                // window too (AUDIT-CORE-001): every release path is a
                // definite resolution of the send attempt.
                if s.pending_send {
                    s.pending_send = false;
                    changed = true;
                }
                if s.unknown_run.is_none() && s.active_run.is_some() {
                    s.active_run = None;
                    changed = true;
                }
            }
            if changed {
                self.bus.publish(Event::SessionChanged {
                    session_id: session_id.into(),
                });
            }
        }
    }

    /// Record a non-releasing reservation for an `OutcomeUnknown` run. The
    /// run may still be live upstream, so `running` is kept true and
    /// `unknown_run` pins the run_id: only a matching authoritative terminal
    /// (or proven death / explicit risk-confirmed resolution) may clear it.
    /// Lag reconciliation preserves it — it only ever touches `running`
    /// (TASK 24 §9). Idempotent for the same run.
    /// Record the exact active RunId for an accepted run (TASK 24 §9): the
    /// frontend reconciliation snapshot needs `session → run_id` to rebuild
    /// Cancel/Send ownership after reload or EventBus lag.
    pub fn note_started(&self, session_id: &str, run_id: &str) {
        let mut map = self.sessions.write().expect("session map mutex poisoned");
        if let Some(s) = map.get_mut(session_id) {
            // CORE-012 / CORE-003: a terminal for this SAME run MUST dominate a
            // late duplicate Accepted/Started. If this run was already marked
            // terminal, do not resurrect it. Terminal knowledge is per-run, so
            // a terminal for some OTHER run can never erase this fact.
            if s.terminal_runs.contains(run_id) {
                // AUDIT-CORE-001: the terminal raced THIS run's acceptance
                // (arrived while the send was still awaiting its receipt).
                // The pending reservation is ours to settle: consume it and
                // release — the run is provably over, the workspace must not
                // stay gated. Without a pending window there is nothing to
                // settle (late duplicate Started for an already-settled
                // state): leave everything untouched.
                if s.pending_send {
                    s.pending_send = false;
                    s.running = false;
                    self.bus.publish(Event::SessionChanged {
                        session_id: session_id.into(),
                    });
                }
                return;
            }
            s.pending_send = false;
            s.running = true;
            s.active_run = Some(run_id.to_string());
            self.bus.publish(Event::SessionChanged {
                session_id: session_id.into(),
            });
        }
    }

    pub fn note_outcome_unknown(&self, session_id: &str, run_id: &str) {
        let mut map = self.sessions.write().expect("session map mutex poisoned");
        if let Some(s) = map.get_mut(session_id) {
            // The receipt resolved the pending window one way or another.
            s.pending_send = false;
            if s.terminal_runs.contains(run_id) {
                // AUDIT-CORE-001: an OutcomeUnknown receipt for a run that
                // ALREADY has an authoritative terminal is proven dead —
                // release instead of pinning a non-releasing reservation
                // nothing would ever clear. (Without this, the old
                // early-return left `running=true` from the pre-await
                // reserve stuck forever.)
                if s.running && s.active_run.is_none() {
                    s.running = false;
                    self.bus.publish(Event::SessionChanged {
                        session_id: session_id.into(),
                    });
                }
                return;
            }
            s.unknown_run = Some(run_id.to_string());
            s.active_run = Some(run_id.to_string());
            s.running = true;
            self.bus.publish(Event::SessionChanged {
                session_id: session_id.into(),
            });
        }
    }

    /// Handle an authoritative `message.*` terminal. Clears the ordinary
    /// `running` flag ONLY when this terminal owns the CURRENT active run
    /// (`active_run` matches `run_id`); clears the unknown reservation ONLY
    /// when the terminal's run_id matches the pinned unknown run. An
    /// unrelated/stale terminal — e.g. a DUPLICATE of an OLD run delivered
    /// after a NEWER run started in the same session — must NEVER release
    /// the newer run's same-workspace serialization gate (TASK 24 §9).
    ///
    /// AUDIT-CORE-001: a terminal arriving while a send is pending-acceptance
    /// (`pending_send`, `active_run == None`) is recorded as a tombstone and
    /// NOTHING else. It can never release the pending reservation: that
    /// window is owned by the in-flight send, and only its own receipt
    /// resolution settles it (Accepted → `note_started`, which consumes a
    /// raced same-run tombstone; Unknown → `note_outcome_unknown`). The old
    /// `active_run.is_none()` release here conflated "terminal-before-start"
    /// with "stale terminal during an unrelated pending send" and let the
    /// stale one admit a concurrent mutating run.
    pub fn note_terminal(&self, session_id: &str, run_id: &str) {
        let mut map = self.sessions.write().expect("session map mutex poisoned");
        if let Some(s) = map.get_mut(session_id) {
            let mut changed = false;

            // CORE-012 / CORE-003: record this run as terminal so late starts
            // are ignored. Insert (not overwrite) — a terminal for run X must
            // never erase terminal knowledge for a different run Y.
            s.terminal_runs.insert(run_id.to_string());

            // Release `running` only for the run that actually owns the
            // reservation now. A stale terminal (run_id != active_run) leaves
            // current liveness untouched so a concurrent newer run stays
            // blocked and same-workspace send stays gated (TASK 24 §9).
            if s.active_run.as_deref() == Some(run_id) {
                if s.running {
                    s.running = false;
                    changed = true;
                }
                s.active_run = None;
            }

            if s.unknown_run.as_deref() == Some(run_id) {
                s.unknown_run = None;
                changed = true;
            }
            if changed {
                self.bus.publish(Event::SessionChanged {
                    session_id: session_id.into(),
                });
            }
        }
    }

    /// Authoritative reconciliation snapshot (TASK 24 §9): exact
    /// `session_id → active RunId` ownership for every session with a live or
    /// unknown run, so the frontend can reconstruct Cancel/Send gating after
    /// a reload or EventBus lag without waiting for incidental events.
    pub fn active_run_ids(&self) -> Vec<(String, String)> {
        let map = self.sessions.read().expect("session map mutex poisoned");
        map.values()
            .filter_map(|s| {
                if s.running || s.unknown_run.is_some() {
                    s.active_run
                        .as_ref()
                        .or(s.unknown_run.as_ref())
                        .map(|rid| (s.id.clone(), rid.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Read-only authoritative session history for the engine that owns this
    /// session (TASK 24 §9): engines without a history capability return
    /// `Ok(None)` — the UI shows the limitation instead of fabricating a
    /// complete empty thread. Never mirrored into SQLite.
    pub async fn session_history(
        &self,
        session_id: &str,
    ) -> Result<Option<Vec<crate::engine::SessionMessage>>, CoreError> {
        let session = self
            .sessions
            .read()
            .expect("session map mutex poisoned")
            .get(session_id)
            .cloned()
            .ok_or_else(|| CoreError::SessionNotFound(session_id.into()))?;
        let engine = self
            .engines
            .get(&session.engine_id)
            .ok_or_else(|| EngineError::engine(&session.engine_id, "unknown engine"))?;
        engine
            .session_history(&session.engine_session_id)
            .await
            .map_err(CoreError::Engine)
    }

    /// True when the session's upstream identity was authoritatively
    /// validated in THIS process (created here, or re-accessed via the
    /// engine's `resume_session`). Restored rows are validated on first use.
    fn is_validated(&self, session_id: &str) -> bool {
        self.validated
            .read()
            .expect("validated set mutex poisoned")
            .contains(session_id)
    }

    fn mark_validated(&self, session_id: &str) {
        self.validated
            .write()
            .expect("validated set mutex poisoned")
            .insert(session_id.to_string());
    }

    /// Reconcile every session's running flag against the authoritative
    /// engine liveness (used after the bounded EventBus reports Lagged: the
    /// event stream may have missed terminals, but the engines know exactly
    /// which runs are still live). Atomically updates running and active_run.
    /// Preserves unknown_run and its non-releasing reservation.
    pub fn reconcile_running_from_engines(&self) {
        let mut live: HashMap<String, String> = HashMap::new();
        for engine in self.engines.list() {
            let Some(adapter) = self.engines.get(&engine.id) else {
                continue;
            };
            for run in adapter.active_runs() {
                live.insert(run.session_id, run.run_id);
            }
        }
        let mut to_publish = Vec::new();
        {
            let mut map = self.sessions.write().expect("session map mutex poisoned");
            for s in map.values_mut() {
                // INVARIANT: this function never mutates unknown_run — the
                // non-releasing reservation survives any reconcile; only the
                // ordinary running/active_run liveness mirrors the engines.
                match live.get(&s.id) {
                    Some(live_run_id) => {
                        let mut changed = false;
                        if !s.running {
                            s.running = true;
                            changed = true;
                        }
                        if s.active_run.as_deref() != Some(live_run_id.as_str()) {
                            s.active_run = Some(live_run_id.clone());
                            changed = true;
                        }
                        if changed {
                            to_publish.push(s.id.clone());
                        }
                    }
                    None => {
                        // AUDIT-CORE-001: a pending-acceptance send is not yet
                        // visible in the adapter's active-run set (the RunId
                        // only exists after acceptance). Reconciliation must
                        // NOT release that reservation mid-flight — only the
                        // receipt resolution owns it.
                        if s.pending_send {
                            continue;
                        }
                        let mut changed = false;
                        if s.running {
                            s.running = false;
                            changed = true;
                        }
                        if s.active_run.is_some() {
                            s.active_run = None;
                            changed = true;
                        }
                        if changed {
                            to_publish.push(s.id.clone());
                        }
                    }
                }
            }
        }
        for session_id in to_publish {
            self.bus.publish(Event::SessionChanged {
                session_id: session_id.into(),
            });
        }
    }
}

/// Locked helper for the same-workspace gate (TASK 18 §21). `target` itself
/// never blocks itself; only sessions with an active run (`running`) block.
fn workspace_has_active_run_locked(
    sessions: &HashMap<String, Session>,
    target: &Session,
) -> Option<(String, String)> {
    let target_ws = target.workspace_id.as_deref()?;
    for s in sessions.values() {
        if s.id == target.id || !(s.running || s.unknown_run.is_some() || s.maintenance) {
            continue;
        }
        if s.workspace_id.as_deref() == Some(target_ws) {
            return Some((s.id.clone(), s.workspace_id.clone().unwrap_or_default()));
        }
    }
    None
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Local timestamp in `DD-MM-YYTHH:MM:SS` format (T-081 session naming).
/// Uses a deterministic local-time breakdown (no `chrono` dependency); the
/// minute/second precision is what makes sibling sessions distinguishable.
fn local_timestamp_for_name(ms: i64) -> String {
    let s = (ms / 1000).max(0) as u64;
    let (year, month, day, hour, min, sec) = local_time_breakdown(s);
    format!("{:02}-{:02}-{:02}T{:02}:{:02}:{:02}", day, month, year % 100, hour, min, sec)
}

/// Minimal local-time breakdown (no `chrono` dependency). Deterministic for
/// timestamps after 1970.
fn local_time_breakdown(unix_secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = unix_secs / 86400;
    let time_secs = unix_secs % 86400;
    let hour = (time_secs / 3600) as u32;
    let min = ((time_secs % 3600) / 60) as u32;
    let sec = (time_secs % 60) as u32;
    let mut y = 1970i64;
    let mut d = days as i64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if d < days_in_year {
            break;
        }
        d -= days_in_year;
        y += 1;
    }
    let month_days = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 0u32;
    for (i, &md) in month_days.iter().enumerate() {
        if d < md as i64 {
            month = i as u32 + 1;
            break;
        }
        d -= md as i64;
    }
    let day = (d + 1) as u32;
    (y as i32, month, day, hour, min, sec)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use saiwork_events::EventBus;
    use tokio::sync::Notify;

    use crate::engine::{
        EngineAdapter, EngineCapabilities, EngineHealth, EngineIdentity, EngineStartContext,
        ModelInfo, SendAcceptance, SessionInfo, SessionMessage,
    };

    /// Minimal connection-owned (resume=false) or resumable (resume=true)
    /// test engine: READY after start, Stopped after stop, session
    /// create/send/resume all succeed.
    struct TestEngine {
        id: String,
        resume: bool,
        health: std::sync::RwLock<EngineHealth>,
        sends: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl TestEngine {
        fn new(id: &str, resume: bool) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
            let sends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            (
                Self {
                    id: id.into(),
                    resume,
                    health: std::sync::RwLock::new(EngineHealth::Stopped),
                    sends: sends.clone(),
                },
                sends,
            )
        }
    }

    #[async_trait]
    impl EngineAdapter for TestEngine {
        fn identity(&self) -> EngineIdentity {
            EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }

        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities {
                streaming: true,
                sessions: true,
                resume: self.resume,
                cancel: true,
                tools: false,
                permissions: false,
                attachments: false,
                images: false,
                models: false,
                usage: false,
                reasoning: false,
                context_window: None,
                worktrees: false,
                parallel_sessions: false,
                session_revert: false,
                structured_events: true,
            }
        }

        async fn start(&self, _ctx: &EngineStartContext) -> Result<(), EngineError> {
            *self.health.write().expect("health mutex") = EngineHealth::Ready;
            Ok(())
        }

        async fn stop(&self) -> Result<(), EngineError> {
            *self.health.write().expect("health mutex") = EngineHealth::Stopped;
            Ok(())
        }

        async fn kill(&self) -> Result<(), EngineError> {
            Ok(())
        }

        fn health(&self) -> EngineHealth {
            self.health.read().expect("health mutex").clone()
        }

        async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }

        async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
            Ok(Vec::new())
        }

        async fn create_session(
            &self,
            req: &CreateSessionRequest,
        ) -> Result<SessionCreation, EngineError> {
            Ok(SessionCreation::Created {
                engine_session_id: format!("upstream-{}", req.session_id),
                display_name: "test session".into(),
            })
        }

        async fn resume_session(&self, engine_session_id: &str) -> Result<SessionInfo, EngineError> {
            Ok(SessionInfo {
                id: engine_session_id.to_string(),
                engine_session_id: engine_session_id.to_string(),
                display_name: "resumed".into(),
            })
        }

        async fn delete_session(&self, _engine_session_id: &str) -> Result<(), EngineError> {
            Ok(())
        }

        async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
            self.sends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(SendAcceptance::Accepted {
                run_id: format!("run-{}", self.sends.load(std::sync::atomic::Ordering::SeqCst)),
            })
        }

        async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
            Ok(())
        }

        async fn session_history(
            &self,
            _session_id: &str,
        ) -> Result<Option<Vec<SessionMessage>>, EngineError> {
            Ok(None)
        }
    }

    fn harness(
        engine_id: &str,
        resume: bool,
    ) -> (SessionManager, Arc<EngineRegistry>, Arc<std::sync::atomic::AtomicUsize>, EventBus) {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        let (engine, sends) = TestEngine::new(engine_id, resume);
        reg.register(Arc::new(engine));
        let db = Db::open_in_memory().unwrap();
        // AUDIT-W2-003: creation now requires the bound workspace row.
        for wid in ["ws-A", "ws-B", "ws-D"] {
            seed_test_workspace(&db, wid);
        }
        (
            SessionManager::new(db, bus.clone(), reg.clone()),
            reg,
            sends,
            bus,
        )
    }

    /// AUDIT-W2-003: insert a workspace row with an exact id (the tests
    /// create sessions bound to these ids).
    fn seed_test_workspace(db: &Db, id: &str) {
        db.with_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO workspaces (id, path, name, last_opened_at, created_at, updated_at)
                 VALUES (?1, ?2, ?2, 0, 0, 0)",
                rusqlite::params![id, format!("test://{id}")],
            )
            .map(|_| ())
            .map_err(saiwork_storage::StorageError::Query)
        })
        .unwrap();
    }

    /// T-081: a new session is named `<projectname>_<DD-MM-YYTHH:MM:SS>` —
    /// readable and collision-free, not the engine's generic title.
    #[tokio::test]
    async fn new_session_named_projectname_timestamp() {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        let (engine, _sends) = TestEngine::new("oc", true);
        reg.register(Arc::new(engine));
        let db = Db::open_in_memory().unwrap();
        let ws = db.upsert_workspace("C:/proj/demo", "demo-app").unwrap();
        let sessions = SessionManager::new(db, bus, reg.clone());
        let ctx = reg.start_context(None, None);
        reg.start("oc", &ctx).await.unwrap();

        let session = sessions.create("oc", Some(&ws.id), None).await.unwrap();
        assert!(
            session.display_name.starts_with("demo-app_"),
            "session name must start with projectname_, got {}",
            session.display_name
        );
        // `_DD-MM-YYTHH:MM:SS` suffix: exactly one `T`, 2-digit groups.
        let rest = session.display_name.strip_prefix("demo-app_").unwrap();
        assert_eq!(rest.len(), 17, "suffix must be DD-MM-YYTHH:MM:SS (17 chars), got {rest}");
        assert_eq!(rest.as_bytes()[8], b'T');
        assert_eq!(rest.as_bytes()[2], b'-');
        assert_eq!(rest.as_bytes()[5], b'-');
        assert_eq!(rest.as_bytes()[11], b':');
        assert_eq!(rest.as_bytes()[14], b':');
    }

    /// No workspace → falls back to the engine id as the project segment
    /// (still timestamp-suffixed, never a raw session id).
    #[tokio::test]
    async fn new_session_without_workspace_names_engineid_timestamp() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(None, None);
        reg.start("oc", &ctx).await.unwrap();
        let session = sessions.create("oc", None, None).await.unwrap();
        assert!(
            session.display_name.starts_with("oc_"),
            "got {}",
            session.display_name
        );
    }

    /// Connection-owned (resume=false) sessions are usable NOW right after
    /// creation even though they are not restart-resumable; after the runtime
    /// restarts they become unusable history and direct send fails closed
    /// (TASK 24 §9).
    #[tokio::test]
    async fn connection_owned_session_usable_now_then_unusable_after_restart() {
        let (sessions, reg, sends, _bus) = harness("conn", false);
        let ctx = reg.start_context(None, None);
        reg.start("conn", &ctx).await.unwrap(); // generation 1

        let session = sessions.create("conn", None, None).await.unwrap();
        assert!(!session.resumable, "connection-owned is not restart-resumable");
        assert!(session.usable_now, "fresh connection-owned session is usable NOW");
        assert!(sessions.get(&session.id).unwrap().usable_now);

        // First prompt works immediately (the finding: UI must not disable it).
        let run = sessions.send(&session.id, "hello", None).await.unwrap();
        assert!(!run.run_id.is_empty());
        sessions.set_running(&session.id, false); // release the reservation

        // Runtime restart → new generation: the old session is unusable.
        reg.stop("conn").await.unwrap();
        reg.start("conn", &ctx).await.unwrap(); // generation 2

        assert!(!sessions.get(&session.id).unwrap().usable_now);
        let listed = sessions.list(None).unwrap();
        assert!(!listed.iter().find(|s| s.id == session.id).unwrap().usable_now);

        // Direct send fails closed BEFORE any reservation or adapter call.
        let err = sessions.send(&session.id, "again", None).await.unwrap_err();
        assert!(
            matches!(err, CoreError::SessionNotUsableNow { .. }),
            "got {err:?}"
        );
        assert!(
            !sessions.get(&session.id).unwrap().running,
            "failed send must not leak a reservation"
        );
        assert_eq!(
            sends.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "restart-disabled send must reach the adapter zero times"
        );
    }

    /// OpenCode-like (resume=true) sessions stay usable across engine
    /// restarts: resumable + READY ⇒ usable, revalidated through the engine's
    /// own resume path (TASK 24 §9).
    #[tokio::test]
    async fn resumable_session_stays_usable_after_restart() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(None, None);
        reg.start("oc", &ctx).await.unwrap();

        let session = sessions.create("oc", None, None).await.unwrap();
        assert!(session.resumable);
        assert!(session.usable_now);
        sessions.set_running(&session.id, false);

        reg.stop("oc").await.unwrap();
        reg.start("oc", &ctx).await.unwrap(); // new generation

        assert!(sessions.get(&session.id).unwrap().usable_now);
        let run = sessions.send(&session.id, "still works", None).await.unwrap();
        assert!(!run.run_id.is_empty());
    }

    /// AUDIT-W2-001: a LIVE connection-owned (resume=false) session is
    /// queue-targetable while its creating runtime generation is alive —
    /// the same usability rule direct send applies. After the engine
    /// restarts (generation loss) the same in-memory row is rejected with
    /// `SessionNotUsableNow`, and a row hydrated from durable metadata by a
    /// fresh SessionManager stays fail-closed `SessionNotResumable`.
    #[tokio::test]
    async fn queue_targeting_usable_now_aware_for_connection_owned_sessions() {
        let (sessions, reg, _sends, _bus) = harness("conn", false);
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("conn", &ctx).await.unwrap(); // generation 1

        let session = sessions
            .create("conn", Some("ws-A"), None)
            .await
            .unwrap();
        assert!(!session.resumable);

        // Live in-memory session: queue targeting must accept it (the old
        // unconditional resumability gate rejected what direct send allows).
        assert!(
            sessions.ensure_loaded(&session.id).is_ok(),
            "a live resume=false session is targetable now"
        );

        // Runtime restart → new generation: the live row becomes history.
        reg.stop("conn").await.unwrap();
        reg.start("conn", &ctx).await.unwrap(); // generation 2
        let err = sessions.ensure_loaded(&session.id).unwrap_err();
        assert!(
            matches!(err, CoreError::SessionNotUsableNow { .. }),
            "dead-generation session must be rejected, got {err:?}"
        );
    }

    #[tokio::test]
    async fn hydrated_non_resumable_row_is_rejected_for_queue_targeting() {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        let (engine, _sends) = TestEngine::new("conn", false);
        reg.register(Arc::new(engine));
        let db = Db::open_in_memory().unwrap();
        let creator = SessionManager::new(db.clone(), bus.clone(), reg.clone());
        let ctx = reg.start_context(None, None);
        reg.start("conn", &ctx).await.unwrap();
        let session = creator.create("conn", None, None).await.unwrap();

        // A fresh manager over the SAME durable metadata (restart
        // simulation): the map misses, the row hydrates, and a
        // connection-owned row must stay rejected for queue targeting.
        let restarted = SessionManager::new(db, bus.clone(), reg.clone());
        let err = restarted.ensure_loaded(&session.id).unwrap_err();
        assert!(
            matches!(err, CoreError::SessionNotResumable(_)),
            "hydrated non-resumable rows stay fail-closed, got {err:?}"
        );
    }

    /// Engine whose `send` blocks inside the adapter call until the test
    /// releases it: models a real engine whose acceptance roundtrip is slow,
    /// exposing the pending-acceptance window (`running=true`,
    /// `active_run=None`, `pending_send=true`).
    struct BlockingEngine {
        id: String,
        health: std::sync::RwLock<EngineHealth>,
        sends: Arc<std::sync::atomic::AtomicUsize>,
        /// Notified once `send` has been entered (the workspace reservation
        /// is already held at that point).
        entered: Arc<tokio::sync::Notify>,
        /// `send` waits for one permit here; tests add one to let the
        /// acceptance resolve.
        release: Arc<tokio::sync::Semaphore>,
    }

    impl BlockingEngine {
        fn new(id: &str) -> Self {
            Self {
                id: id.into(),
                health: std::sync::RwLock::new(EngineHealth::Stopped),
                sends: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                entered: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
            }
        }
    }

    #[async_trait]
    impl EngineAdapter for BlockingEngine {
        fn identity(&self) -> EngineIdentity {
            EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }
        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities {
                streaming: true,
                sessions: true,
                resume: true,
                cancel: true,
                tools: false,
                permissions: false,
                attachments: false,
                images: false,
                models: false,
                usage: false,
                reasoning: false,
                context_window: None,
                worktrees: false,
                parallel_sessions: false,
                session_revert: false,
                structured_events: true,
            }
        }
        async fn start(&self, _ctx: &EngineStartContext) -> Result<(), EngineError> {
            *self.health.write().expect("health mutex") = EngineHealth::Ready;
            Ok(())
        }
        async fn stop(&self) -> Result<(), EngineError> {
            *self.health.write().expect("health mutex") = EngineHealth::Stopped;
            Ok(())
        }
        async fn kill(&self) -> Result<(), EngineError> {
            Ok(())
        }
        fn health(&self) -> EngineHealth {
            self.health.read().expect("health mutex").clone()
        }
        async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }
        async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
            Ok(Vec::new())
        }
        async fn create_session(
            &self,
            req: &CreateSessionRequest,
        ) -> Result<SessionCreation, EngineError> {
            Ok(SessionCreation::Created {
                engine_session_id: format!("upstream-{}", req.session_id),
                display_name: "blocking session".into(),
            })
        }
        async fn resume_session(&self, engine_session_id: &str) -> Result<SessionInfo, EngineError> {
            Ok(SessionInfo {
                id: engine_session_id.to_string(),
                engine_session_id: engine_session_id.to_string(),
                display_name: "resumed".into(),
            })
        }
        async fn delete_session(&self, _engine_session_id: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
            let n = self.sends.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            self.entered.notify_one();
            let _permit = self.release.acquire().await.expect("release semaphore");
            Ok(SendAcceptance::Accepted { run_id: format!("run-{n}") })
        }
        async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn session_history(
            &self,
            _session_id: &str,
        ) -> Result<Option<Vec<SessionMessage>>, EngineError> {
            Ok(None)
        }
    }

    fn blocking_harness(
        engine_id: &str,
    ) -> (
        Arc<SessionManager>,
        Arc<EngineRegistry>,
        Arc<BlockingEngine>,
        EventBus,
    ) {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        let engine = Arc::new(BlockingEngine::new(engine_id));
        reg.register(engine.clone());
        let db = Db::open_in_memory().unwrap();
        for wid in ["ws-A", "ws-B", "ws-D"] {
            seed_test_workspace(&db, wid);
        }
        (
            Arc::new(SessionManager::new(db, bus.clone(), reg.clone())),
            reg,
            engine,
            bus,
        )
    }

    /// AUDIT-CORE-001: while run B is still awaiting acceptance (blocked in
    /// the adapter), a stale terminal for an OLDER run A must not release
    /// B's pending reservation — the same-workspace gate must stay held and
    /// a sibling send must stay rejected until B's own receipt+terminal
    /// resolve. The old code cleared `running` on any terminal when
    /// `active_run == None`, admitting a concurrent mutating send.
    #[tokio::test]
    async fn stale_terminal_during_pending_acceptance_keeps_gate_held() {
        let (sessions, reg, eng, _bus) = blocking_harness("oc");
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();

        let sa = sessions.create("oc", Some("ws-A"), None).await.unwrap();
        let sb = sessions.create("oc", Some("ws-A"), None).await.unwrap();

        // Run A completed earlier in this session.
        sessions.note_started(&sa.id, "A");
        sessions.note_terminal(&sa.id, "A");
        assert!(!sessions.get(&sa.id).unwrap().running);

        // Run B's send reserves the workspace and blocks inside the engine.
        let send_task = tokio::spawn({
            let sessions = sessions.clone();
            let sa_id = sa.id.clone();
            async move { sessions.send(&sa_id, "B prompt", None).await }
        });
        eng.entered.notified().await;

        let pending = sessions.get(&sa.id).unwrap();
        assert!(pending.running, "pending send holds the reservation");
        assert!(pending.pending_send, "reservation is explicitly pending");
        assert_eq!(pending.active_run, None, "no RunId exists yet");

        // Stale/duplicate terminal for OLD run A arrives mid-window.
        sessions.note_terminal(&sa.id, "A");

        let after_stale = sessions.get(&sa.id).unwrap();
        assert!(
            after_stale.running,
            "stale terminal must NOT release the pending reservation"
        );
        assert!(
            after_stale.pending_send,
            "pending ownership survives an unrelated terminal"
        );

        // The sibling same-workspace send stays rejected while B is pending.
        let err = sessions.send(&sb.id, "hi", None).await.unwrap_err();
        assert!(
            matches!(err, CoreError::WorkspaceBusy { .. }),
            "gate must stay held during pending acceptance, got {err:?}"
        );

        // Resolve B's acceptance, then its real terminal.
        eng.release.add_permits(1);
        let handle = send_task.await.unwrap().unwrap();
        let accepted = sessions.get(&sa.id).unwrap();
        assert_eq!(
            accepted.active_run.as_deref(),
            Some(handle.run_id.as_str()),
            "acceptance converts the pending reservation to Active(run)"
        );
        assert!(!accepted.pending_send);
        assert!(accepted.running);

        sessions.note_terminal(&sa.id, &handle.run_id);
        let done = sessions.get(&sa.id).unwrap();
        assert!(!done.running && done.active_run.is_none() && !done.pending_send);

        let ok = sessions.send(&sb.id, "now ok", None).await;
        assert!(ok.is_ok(), "gate releases normally after B resolves");
    }

    /// AUDIT-CORE-001 complementary case: run B's OWN terminal races ahead of
    /// its acceptance receipt (engine emitted the terminal before the send
    /// future observed Accepted). The pending reservation must survive the
    /// early tombstone and be consumed exactly when the acceptance lands,
    /// leaving the session idle — never stuck busy.
    #[tokio::test]
    async fn own_terminal_before_acceptance_releases_at_receipt() {
        let (sessions, reg, eng, _bus) = blocking_harness("oc");
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();

        let sa = sessions.create("oc", Some("ws-A"), None).await.unwrap();
        let sb = sessions.create("oc", Some("ws-A"), None).await.unwrap();

        let send_task = tokio::spawn({
            let sessions = sessions.clone();
            let sa_id = sa.id.clone();
            async move { sessions.send(&sa_id, "B prompt", None).await }
        });
        eng.entered.notified().await;

        // B's own terminal arrives BEFORE its acceptance resolves (the
        // BlockingEngine mints deterministic `run-{n}` ids; first send = 1).
        sessions.note_terminal(&sa.id, "run-1");

        let raced = sessions.get(&sa.id).unwrap();
        assert!(
            raced.running && raced.pending_send,
            "early tombstone must not release the pending window (old bug did)"
        );

        eng.release.add_permits(1);
        send_task.await.unwrap().unwrap();

        let settled = sessions.get(&sa.id).unwrap();
        assert!(
            !settled.running,
            "receipt resolution must consume the raced terminal and release"
        );
        assert_eq!(settled.active_run, None);
        assert!(!settled.pending_send);

        // The session is idle again: sibling send proceeds.
        let ok = sessions.send(&sb.id, "after race", None).await;
        assert!(ok.is_ok());
        let sb_after = sessions.get(&sb.id).unwrap();
        assert!(sb_after.running && !sb_after.pending_send);
        sessions.note_terminal(&sb.id, &ok.unwrap().run_id);
    }

    /// T-011 (TASK 24 §9): a stale/duplicate terminal for an OLD run must NOT
    /// release a NEWER run's same-workspace serialization gate. Sequence: run
    /// A starts and completes, run B starts in the same session, then a
    /// duplicate terminal for A arrives — B must stay running/active and a
    /// same-workspace send must remain blocked until B's own terminal.
    #[tokio::test]
    async fn stale_terminal_must_not_release_newer_run_gate() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();

        let sa = sessions.create("oc", Some("ws-A"), None).await.unwrap();
        let sb = sessions.create("oc", Some("ws-A"), None).await.unwrap();

        // Run A starts, then completes (terminal A).
        sessions.note_started(&sa.id, "A");
        sessions.note_terminal(&sa.id, "A");
        assert!(
            !sessions.get(&sa.id).unwrap().running,
            "run A terminal must clear A's reservation"
        );

        // A new run B starts in the same session (A is done).
        sessions.note_started(&sa.id, "B");
        let sa_live = sessions.get(&sa.id).unwrap();
        assert!(sa_live.running, "run B must reserve the workspace");
        assert_eq!(sa_live.active_run.as_deref(), Some("B"));

        // A STALE/duplicate terminal for run A now arrives.
        sessions.note_terminal(&sa.id, "A");

        // After the fix: B still owns the reservation — the stale terminal for
        // A must never clear B's liveness or active_run.
        let sa_after = sessions.get(&sa.id).unwrap();
        assert!(
            sa_after.running,
            "stale terminal for A must NOT clear B's workspace reservation"
        );
        assert_eq!(
            sa_after.active_run.as_deref(),
            Some("B"),
            "active run must stay B"
        );

        // Same-workspace send to sb stays blocked while B runs in sa.
        let err = sessions.send(&sb.id, "hi", None).await.unwrap_err();
        assert!(
            matches!(err, CoreError::WorkspaceBusy { .. }),
            "same-workspace send must stay blocked until terminal B, got {err:?}"
        );

        // B's real terminal releases the gate.
        sessions.note_terminal(&sa.id, "B");
        let sa_done = sessions.get(&sa.id).unwrap();
        assert!(!sa_done.running);
        assert_eq!(sa_done.active_run, None);
        let ok = sessions.send(&sb.id, "now ok", None).await;
        assert!(ok.is_ok(), "gated send must succeed after B's terminal");
    }

    /// CORE-003 reproduction: terminal(B) -> terminal(A) -> late started/accepted(B)
    /// must leave B idle. The original defect kept a single `last_terminal_run`
    /// scalar that the stale terminal(A) overwrote, erasing the knowledge that B
    /// had terminated, so the late `note_started(B)` resurrected B and re-blocked
    /// the session/workspace forever. With per-run terminal tombstones, B stays
    /// terminal and is never resurrected by an unrelated run's late event.
    #[tokio::test]
    async fn terminal_evidence_cannot_be_erased_by_unrelated_run() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();

        let s = sessions.create("oc", Some("ws-A"), None).await.unwrap();

        // Terminal for B arrives (out of order, before B's own Started/Accepted).
        sessions.note_terminal(&s.id, "B");
        // A stale duplicate terminal for the older run A now arrives and must
        // NOT erase the fact that B is terminal (set semantics, not overwrite).
        sessions.note_terminal(&s.id, "A");

        // The late Started/Accepted for B must be ignored — B is already terminal.
        sessions.note_started(&s.id, "B");
        let after = sessions.get(&s.id).unwrap();
        assert!(
            !after.running,
            "late note_started(B) must NOT resurrect terminal B"
        );
        assert_eq!(
            after.active_run, None,
            "active_run must stay None after terminal B"
        );

        // A fresh, genuinely new run C may still start (distinct run id).
        sessions.note_started(&s.id, "C");
        let c = sessions.get(&s.id).unwrap();
        assert!(c.running, "a new run C must be allowed to start");
        assert_eq!(c.active_run.as_deref(), Some("C"));

        // Same-workspace send to a sibling session stays blocked while C runs.
        let s2 = sessions.create("oc", Some("ws-A"), None).await.unwrap();
        let err = sessions.send(&s2.id, "blocked", None).await.unwrap_err();
        assert!(
            matches!(err, CoreError::WorkspaceBusy { .. }),
            "send must stay gated until C terminates, got {err:?}"
        );
        sessions.note_terminal(&s.id, "C");
        assert!(
            sessions.send(&s2.id, "ok", None).await.is_ok(),
            "send must succeed once C terminates"
        );
    }

    /// CORE-003: an OutcomeUnknown receipt for an already-terminal run must not
    /// resurrect it (mirrors the `note_started` guard for unknown outcomes).
    #[tokio::test]
    async fn outcome_unknown_cannot_resurrect_terminal_run() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();
        let s = sessions.create("oc", Some("ws-A"), None).await.unwrap();

        sessions.note_terminal(&s.id, "B");
        sessions.note_outcome_unknown(&s.id, "B");
        let after = sessions.get(&s.id).unwrap();
        assert!(
            !after.running,
            "OutcomeUnknown for terminal B must not resurrect it"
        );
        assert_eq!(after.active_run, None);
        assert_eq!(after.unknown_run, None);
    }

    /// Engine whose `delete_session` always reports the upstream session as
    /// already gone (typed `SessionNotFound`) — models the retry path where a
    /// prior delete succeeded upstream but the local row survived.
    struct AlreadyDeletedEngine {
        id: String,
        resume: bool,
        health: std::sync::RwLock<EngineHealth>,
    }

    #[async_trait]
    impl EngineAdapter for AlreadyDeletedEngine {
        fn identity(&self) -> EngineIdentity {
            EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }
        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities {
                streaming: true,
                sessions: true,
                resume: self.resume,
                cancel: true,
                tools: false,
                permissions: false,
                attachments: false,
                images: false,
                models: false,
                usage: false,
                reasoning: false,
                context_window: None,
                worktrees: false,
                parallel_sessions: false,
                session_revert: false,
                structured_events: true,
            }
        }
        async fn start(&self, _ctx: &EngineStartContext) -> Result<(), EngineError> {
            *self.health.write().expect("health mutex") = EngineHealth::Ready;
            Ok(())
        }
        async fn stop(&self) -> Result<(), EngineError> {
            *self.health.write().expect("health mutex") = EngineHealth::Stopped;
            Ok(())
        }
        async fn kill(&self) -> Result<(), EngineError> {
            Ok(())
        }
        fn health(&self) -> EngineHealth {
            self.health.read().expect("health mutex").clone()
        }
        async fn create_session(
            &self,
            req: &CreateSessionRequest,
        ) -> Result<SessionCreation, EngineError> {
            Ok(SessionCreation::Created {
                engine_session_id: format!("upstream-{}", req.session_id),
                display_name: "test session".into(),
            })
        }
        async fn resume_session(&self, id: &str) -> Result<SessionInfo, EngineError> {
            Ok(SessionInfo {
                id: id.to_string(),
                engine_session_id: id.to_string(),
                display_name: "resumed".into(),
            })
        }
        async fn delete_session(&self, _engine_session_id: &str) -> Result<(), EngineError> {
            // Upstream session already gone: typed SessionNotFound.
            Err(EngineError::SessionNotFound {
                session_id: _engine_session_id.to_string(),
            })
        }
        async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }
        async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
            Ok(Vec::new())
        }
        async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
            Ok(SendAcceptance::Accepted {
                run_id: "run-1".into(),
            })
        }
        async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
            Ok(())
        }
    }

    /// T-012 (TASK 24 §9): at the cross-authority compensation boundary a typed
    /// upstream `SessionNotFound` must be treated as already-deleted success
    /// and continue the local durable cleanup — never die early and leave the
    /// surviving local metadata row behind.
    #[tokio::test]
    async fn delete_session_treats_upstream_notfound_as_success() {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        let engine = Arc::new(AlreadyDeletedEngine {
            id: "oc".into(),
            resume: true,
            health: std::sync::RwLock::new(EngineHealth::Stopped),
        });
        reg.register(engine);
        let ctx = reg.start_context(Some("ws-D".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();
        let db = Db::open_in_memory().unwrap();
        seed_test_workspace(&db, "ws-D");
        let sessions = SessionManager::new(db, bus.clone(), reg.clone());

        let sid = sessions.create("oc", Some("ws-D"), None).await.unwrap().id;
        assert!(sessions.get(&sid).is_some(), "session exists before delete");

        // Upstream reports NotFound → must still complete local durable cleanup.
        sessions.delete_session(&sid).await.expect("NotFound must be treated as success");
        assert!(
            sessions.get(&sid).is_none(),
            "local session metadata must be removed even when upstream reports NotFound"
        );
        assert!(
            sessions.list(None).unwrap().iter().all(|s| s.id != sid),
            "deleted session must not reappear in list"
        );
    }

    #[tokio::test]
    async fn delete_rejects_a_live_session_without_removing_metadata() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();
        let session = sessions.create("oc", Some("ws-A"), None).await.unwrap();
        sessions.note_started(&session.id, "run-live");

        let error = sessions.delete_session(&session.id).await.expect_err("live delete must fail");
        assert!(matches!(error, CoreError::SessionBusy { .. }));
        assert!(sessions.get(&session.id).is_some(), "metadata remains after rejection");
    }

    #[tokio::test]
    async fn failed_revert_releases_maintenance_reservation() {
        let (sessions, reg, _sends, _bus) = harness("oc", true);
        let ctx = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("oc", &ctx).await.unwrap();
        let session = sessions.create("oc", Some("ws-A"), None).await.unwrap();

        let error = sessions.revert_last_turn(&session.id).await.expect_err("unsupported");
        assert!(matches!(error, CoreError::Engine(EngineError::UnsupportedCapability { .. })));
        sessions.delete_session(&session.id).await.expect("RAII reservation released");
    }

    /// W2-001: a binding-dependent `create` that pauses right after binding
    /// validation (inside the adapter call, holding the SHARED binding-stability
    /// lease) must never be split across a concurrent `stop`→`start(B)`: the
    /// upstream session is created against the runtime the create validated
    /// (`A`), never the rebound `B`, and the persisted metadata carries `A`.
    /// The rebind is fully sequenced AFTER the create by the EXCLUSIVE lease.
    struct LeaseEngine {
        id: String,
        resume: bool,
        health: std::sync::RwLock<EngineHealth>,
        /// Workspace the runtime is currently bound to (written by start/stop).
        bound_workspace: std::sync::RwLock<Option<String>>,
        /// Workspace recorded at each `create_session` entry — the runtime the
        /// upstream session was actually created against (W2-001 invariant).
        created_under: std::sync::Mutex<Vec<Option<String>>>,
        /// Pause barrier: create_session signals entry then waits here.
        entered: Arc<Notify>,
        proceed: Arc<Notify>,
    }

    impl LeaseEngine {
        fn new(id: &str, resume: bool) -> Self {
            Self {
                id: id.into(),
                resume,
                health: std::sync::RwLock::new(EngineHealth::Stopped),
                bound_workspace: std::sync::RwLock::new(None),
                created_under: std::sync::Mutex::new(Vec::new()),
                entered: Arc::new(Notify::new()),
                proceed: Arc::new(Notify::new()),
            }
        }
    }

    #[async_trait]
    impl EngineAdapter for LeaseEngine {
        fn identity(&self) -> EngineIdentity {
            EngineIdentity {
                id: self.id.clone(),
                display_name: self.id.clone(),
                version: "test".into(),
                experimental: false,
            }
        }
        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities {
                streaming: true,
                sessions: true,
                resume: self.resume,
                cancel: true,
                permissions: false,
                tools: false,
                attachments: false,
                images: false,
                models: false,
                usage: false,
                reasoning: false,
                context_window: None,
                worktrees: false,
                parallel_sessions: false,
                session_revert: false,
                structured_events: true,
            }
        }
        async fn start(&self, ctx: &EngineStartContext) -> Result<(), EngineError> {
            *self.bound_workspace.write().expect("bound ws") = ctx.workspace_id.clone();
            *self.health.write().expect("health") = EngineHealth::Ready;
            Ok(())
        }
        async fn stop(&self) -> Result<(), EngineError> {
            *self.bound_workspace.write().expect("bound ws") = None;
            *self.health.write().expect("health") = EngineHealth::Stopped;
            Ok(())
        }
        async fn kill(&self) -> Result<(), EngineError> {
            Ok(())
        }
        fn health(&self) -> EngineHealth {
            self.health.read().expect("health").clone()
        }
        async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
            Err(EngineError::UnsupportedCapability {
                engine_id: self.id.clone(),
                capability: "models",
            })
        }
        async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
            Ok(Vec::new())
        }
        async fn create_session(
            &self,
            req: &CreateSessionRequest,
        ) -> Result<SessionCreation, EngineError> {
            // Record the runtime we are about to create against, then pause so
            // the driver can attempt a concurrent rebind while we hold the
            // SHARED binding-stability lease.
            self.created_under
                .lock()
                .expect("created_under")
                .push(self.bound_workspace.read().expect("bound ws").clone());
            self.entered.notify_one();
            self.proceed.notified().await;
            Ok(SessionCreation::Created {
                engine_session_id: format!("upstream-{}", req.session_id),
                display_name: "lease test".into(),
            })
        }
        async fn resume_session(&self, id: &str) -> Result<SessionInfo, EngineError> {
            Ok(SessionInfo {
                id: id.to_string(),
                engine_session_id: id.to_string(),
                display_name: "resumed".into(),
            })
        }
        async fn delete_session(&self, _id: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn send(&self, _req: &SendRequest) -> Result<SendAcceptance, EngineError> {
            Ok(SendAcceptance::Accepted {
                run_id: "run-1".into(),
            })
        }
        async fn cancel(&self, _run_id: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn session_history(
            &self,
            _session_id: &str,
        ) -> Result<Option<Vec<SessionMessage>>, EngineError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn binding_stability_lease_prevents_create_under_rebound_runtime() {
        let bus = EventBus::new();
        let reg = Arc::new(EngineRegistry::new(
            bus.clone(),
            Arc::new(saiwork_diagnostics::Diagnostics::new()),
            Arc::new(saiwork_process::ProcessSupervisor::new(bus.clone())),
        ));
        let engine = Arc::new(LeaseEngine::new("lease-eng", true));
        reg.register(engine.clone());
        let db = Db::open_in_memory().unwrap();
        seed_test_workspace(&db, "ws-A");
        let sessions = SessionManager::new(db, bus.clone(), reg.clone());

        // Bind the runtime to workspace A.
        let ctx_a = reg.start_context(Some("ws-A".to_string()), None);
        reg.start("lease-eng", &ctx_a).await.unwrap();
        assert_eq!(
            reg.bound_workspace("lease-eng"),
            Some(Some("ws-A".to_string())),
            "engine must be bound to A before the create"
        );

        // Subscribe to the entry signal BEFORE spawning create (no lost wakeup).
        let entered = engine.entered.notified();
        // Spawn the create; it validates A then pauses inside create_session
        // with the SHARED binding-stability lease held.
        let create_task = tokio::spawn({
            async move { sessions.create("lease-eng", Some("ws-A"), None).await }
        });
        entered.await;

        // Attempt a concurrent rebind: stop(A) then start(B). Both must block
        // on the EXCLUSIVE lease until the create releases it.
        let reg_stop = reg.clone();
        let stop_task = tokio::spawn(async move { reg_stop.stop("lease-eng").await });
        let reg_start = reg.clone();
        let ctx_b = reg.start_context(Some("ws-B".to_string()), None);
        let start_task = tokio::spawn(async move { reg_start.start("lease-eng", &ctx_b).await });

        // Release the create: it completes against the STILL-bound-A runtime.
        engine.proceed.notify_one();

        let session = create_task
            .await
            .expect("create task joined")
            .expect("create must succeed against A");

        // The rebind is fully sequenced AFTER the create.
        stop_task.await.expect("stop joined").expect("stop must succeed");
        start_task.await.expect("start joined").expect("start B must succeed");

        // CRITICAL W2-001 invariants:
        // 1. The upstream session was created against the A runtime, never B.
        assert_eq!(
            engine.created_under.lock().expect("created_under").clone(),
            vec![Some("ws-A".to_string())],
            "create must run against the runtime it validated (A), never under the rebound B"
        );
        // 2. The persisted metadata carries workspace A (not B).
        assert_eq!(
            session.workspace_id,
            Some("ws-A".to_string()),
            "persisted session must carry the validated workspace A"
        );
        // 3. The engine ended rebound to B.
        assert_eq!(
            reg.bound_workspace("lease-eng"),
            Some(Some("ws-B".to_string())),
            "the concurrent rebind to B must have completed after the create"
        );
    }
}
