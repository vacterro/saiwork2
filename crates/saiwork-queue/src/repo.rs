//! Queue repository — every durable queue mutation lives here (law 18).
//!
//! All writes go through typed methods; there is no raw `UPDATE queue_items
//! SET state = …` anywhere outside this module (§227–§228). The single
//! `saiwork-storage::Db` connection serializes writers; every mutation is one
//! transaction or one atomic statement.

use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{params, OptionalExtension};
use saiwork_storage::{Db, StorageError};
use uuid::Uuid;

use crate::model::{
    DispatchCandidate, EnqueueRequest, QueueError, QueueItem, QueueState, SessionMode,
    DISPATCH_CANDIDATE_PAGE_SIZE,
};

pub struct QueueRepo {
    db: Db,
    /// Dispatch-read counter (diagnostics/tests): candidate pages and
    /// eligibility-gate queries. The dispatcher must perform zero of these
    /// while idle (PERFORMANCE.md); tests assert it.
    dispatch_scans: AtomicU64,
    #[cfg(feature = "failpoints")]
    failpoints: std::sync::Mutex<RepoFailpoints>,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Strict row decode: unknown persisted enum values fail with a typed
/// storage error (never silent substitution — a corrupted/future row must
/// fail closed, TASK 24 §9). All rusqlite access errors map to
/// `StorageUnavailable`.
fn row_to_item(row: &rusqlite::Row<'_>) -> Result<QueueItem, QueueError> {
    let id: String = row.get(0).map_err(storage_err)?;
    let state: String = row.get(5).map_err(storage_err)?;
    let session_mode: String = row.get(8).map_err(storage_err)?;
    let state = QueueState::from_str(&state).ok_or_else(|| QueueError::InvalidPersistedRow {
        row_id: id.clone(),
        field: "state",
        value: state,
    })?;
    let session_mode = SessionMode::from_str(&session_mode).ok_or_else(|| {
        QueueError::InvalidPersistedRow {
            row_id: id.clone(),
            field: "session_mode",
            value: session_mode,
        }
    })?;
    Ok(QueueItem {
        id,
        workspace_id: row.get(1).map_err(storage_err)?,
        engine_id: row.get(2).map_err(storage_err)?,
        session_id: row.get(3).map_err(storage_err)?,
        payload: row.get(4).map_err(storage_err)?,
        // A full decode is never a preview: `get`/history rows carry the
        // exact durable payload (§13).
        payload_truncated: false,
        state,
        order_key: row.get(6).map_err(storage_err)?,
        revision: row.get(7).map_err(storage_err)?,
        session_mode,
        model: row.get(9).map_err(storage_err)?,
        lease_id: row.get(10).map_err(storage_err)?,
        leased_at: row.get(11).map_err(storage_err)?,
        attempt_count: row.get(12).map_err(storage_err)?,
        run_id: row.get(13).map_err(storage_err)?,
        last_error: row.get(14).map_err(storage_err)?,
        last_error_code: row.get(15).map_err(storage_err)?,
        created_at: row.get(16).map_err(storage_err)?,
        updated_at: row.get(17).map_err(storage_err)?,
    })
}

fn storage_err(e: rusqlite::Error) -> QueueError {
    QueueError::StorageUnavailable(e.to_string())
}

/// Decode a SQL-projected preview blob into a String, trimming a multi-byte
/// character split by the byte cut. The SQL layer projects `substr(CAST(
/// payload AS BLOB), 1, N)` — a clipped TEXT cannot round-trip through
/// rusqlite's UTF-8 `String` decode, so the snapshot decodes the BLOB and
/// this trims to the last valid char boundary. Never panics on a hostile
/// multi-byte payload.
fn utf8_bounded(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => {
            let valid_up_to = e.utf8_error().valid_up_to();
            let bytes = e.into_bytes();
            String::from_utf8_lossy(&bytes[..valid_up_to]).into_owned()
        }
    }
}

/// Run a query returning full item rows, decoding each row strictly.
fn query_items(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
) -> Result<Vec<QueueItem>, QueueError> {
    let mut stmt = conn.prepare(sql).map_err(storage_err)?;
    let mut rows = stmt.query(params).map_err(storage_err)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(storage_err)? {
        out.push(row_to_item(&row)?);
    }
    Ok(out)
}

/// Run a query returning zero-or-one item row, decoding strictly.
fn query_item(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
) -> Result<Option<QueueItem>, QueueError> {
    let mut stmt = conn.prepare(sql).map_err(storage_err)?;
    let mut rows = stmt.query(params).map_err(storage_err)?;
    match rows.next().map_err(storage_err)? {
        Some(row) => Ok(Some(row_to_item(&row)?)),
        None => Ok(None),
    }
}

/// Snapshot-row decode: the SQL projection already truncated `payload` to
/// `PAYLOAD_PREVIEW_BYTES` bytes (BLOB-substr — the full durable body never
/// enters Rust memory) and computed `payload_truncated` with
/// `octet_length` (§13). Columns 0–17 are the full row shape with the
/// preview in the payload slot; column 18 is the projection flag.
fn row_to_item_snapshot(row: &rusqlite::Row<'_>) -> Result<QueueItem, QueueError> {
    let id: String = row.get(0).map_err(storage_err)?;
    let state: String = row.get(5).map_err(storage_err)?;
    let session_mode: String = row.get(8).map_err(storage_err)?;
    let state = QueueState::from_str(&state).ok_or_else(|| QueueError::InvalidPersistedRow {
        row_id: id.clone(),
        field: "state",
        value: state,
    })?;
    let session_mode = SessionMode::from_str(&session_mode).ok_or_else(|| {
        QueueError::InvalidPersistedRow {
            row_id: id.clone(),
            field: "session_mode",
            value: session_mode,
        }
    })?;
    let payload: Vec<u8> = row.get(4).map_err(storage_err)?;
    Ok(QueueItem {
        id,
        workspace_id: row.get(1).map_err(storage_err)?,
        engine_id: row.get(2).map_err(storage_err)?,
        session_id: row.get(3).map_err(storage_err)?,
        payload: utf8_bounded(payload),
        payload_truncated: row.get(18).map_err(storage_err)?,
        state,
        order_key: row.get(6).map_err(storage_err)?,
        revision: row.get(7).map_err(storage_err)?,
        session_mode,
        model: row.get(9).map_err(storage_err)?,
        lease_id: row.get(10).map_err(storage_err)?,
        leased_at: row.get(11).map_err(storage_err)?,
        attempt_count: row.get(12).map_err(storage_err)?,
        run_id: row.get(13).map_err(storage_err)?,
        last_error: row.get(14).map_err(storage_err)?,
        last_error_code: row.get(15).map_err(storage_err)?,
        created_at: row.get(16).map_err(storage_err)?,
        updated_at: row.get(17).map_err(storage_err)?,
    })
}

/// Run a snapshot query (rows 0–18, payload already SQL-projected). Takes
/// generic `rusqlite::Params` so callers can use named bindings — the history
/// query references `:P` twice and `:L` once, and a positional slice is bound
/// by first-appearance order, which silently swapped preview/limit (T-047).
fn query_items_snapshot<P: rusqlite::Params>(
    conn: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<Vec<QueueItem>, QueueError> {
    let mut stmt = conn.prepare(sql).map_err(storage_err)?;
    let mut rows = stmt.query(params).map_err(storage_err)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(storage_err)? {
        out.push(row_to_item_snapshot(&row)?);
    }
    Ok(out)
}

const ITEM_COLUMNS: &str = "id, workspace_id, engine_id, session_id, payload, state, order_key, \
     revision, session_mode, model, lease_id, leased_at, attempt_count, run_id, last_error, \
     last_error_code, created_at, updated_at";

/// Startup recovery report (bounded facts for tests + diagnostics).
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub recovered_to_queued: usize,
    /// Leased items whose send may have crossed the boundary (phase
    /// `sending`) — marked `unknown` (TASK 23 §17–§18).
    pub marked_unknown: usize,
    pub cancelled_from_intent: usize,
    /// Dispatched items at restart whose engine authority is unrecoverable in
    /// this process — marked `unknown` (TASK 23 §28–§29).
    pub marked_unknown_dispatched: usize,
}

/// Test-only repo failpoints (durability-failure tests). Feature-gated: not
/// reachable in production builds.
#[cfg(feature = "failpoints")]
#[derive(Default)]
pub struct RepoFailpoints {
    /// When set, `get(id)` returns a storage error for ids the predicate
    /// matches — simulates a transient durability failure.
    pub get_error: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// When set and the predicate matches, `persist_session_created` returns
    /// a storage error AFTER the external session was authoritatively
    /// created — simulates a cross-authority durability failure (TASK 24 §9).
    pub persist_created_error: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// When set and the predicate matches, `request_cancel_dispatched`
    /// returns a storage error — simulates a durability failure of the cancel
    /// intent (TASK 24 §9). The caller must fail closed and must NOT invoke
    /// the external adapter cancel for a run whose durable intent did not
    /// persist.
    pub cancel_dispatched_error: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
}

#[cfg(feature = "failpoints")]
use std::sync::Arc;

impl QueueRepo {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            dispatch_scans: AtomicU64::new(0),
            #[cfg(feature = "failpoints")]
            failpoints: std::sync::Mutex::new(RepoFailpoints::default()),
        }
    }

    #[cfg(feature = "failpoints")]
    pub fn set_failpoints_for_test(&self, failpoints: RepoFailpoints) {
        *self
            .failpoints
            .lock()
            .expect("repo failpoints mutex poisoned") = failpoints;
    }

    // ---- pause (durable, survives restart) ----

    /// Parse only the canonical durable spellings: `"0"` = running,
    /// `"1"` = paused. Any other persisted value (corrupt/future/partial
    /// setting such as `"true"` or `"garbage"`) is a typed storage error that
    /// keeps the queue fail-closed — a bad setting must never silently
    /// unpause the queue and dispatch work at startup (TASK 24 §9).
    pub fn is_paused(&self) -> Result<bool, QueueError> {
        match self.db.get_setting("queue.paused")? {
            Some(v) => match v.as_str() {
                "0" => Ok(false),
                "1" => Ok(true),
                other => Err(QueueError::InvalidPersistedRow {
                    row_id: "<settings>".into(),
                    field: "queue.paused",
                    value: other.into(),
                }),
            },
            None => Ok(false),
        }
    }

    pub fn set_paused(&self, paused: bool) -> Result<(), QueueError> {
        self.db
            .set_setting("queue.paused", if paused { "1" } else { "0" })?;
        Ok(())
    }

    // ---- enqueue ----

    /// Atomic enqueue: after this returns the item is durably QUEUED (§14).
    pub fn enqueue(&self, req: &EnqueueRequest) -> Result<QueueItem, QueueError> {
        if req.payload.trim().is_empty() {
            return Err(QueueError::EmptyPayload);
        }
        if req.payload.len() > crate::model::PAYLOAD_MAX_BYTES {
            return Err(QueueError::PayloadTooLarge {
                bytes: req.payload.len(),
                max: crate::model::PAYLOAD_MAX_BYTES,
            });
        }
        if req.session_mode == SessionMode::Existing && req.session_id.is_none() {
            return Err(QueueError::InvalidState {
                item_id: "<new>".into(),
                detail: "existing-session mode requires a session_id".into(),
            });
        }
        self.db.transaction_with(|tx| {
            // AUDIT-W2-003: the referenced workspace must exist, checked in
            // THIS transaction with the insert. A Forget that committed
            // first makes the enqueue reject; an enqueue that commits first
            // makes the Forget reject (nonterminal check now inside its
            // deletion transaction). No durable queue row can ever
            // reference a deleted workspace identity.
            let workspace: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM workspaces WHERE id = ?1",
                    rusqlite::params![req.workspace_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(StorageError::from)?;
            if workspace.is_none() {
                return Err(QueueError::InvalidState {
                    item_id: "<new>".into(),
                    detail: format!(
                        "workspace '{}' does not exist — it was forgotten or never opened",
                        req.workspace_id
                    ),
                });
            }
            let next_key: i64 = tx
                .query_row(
                    // PERF-004: force the order_key-leading index so MAX(order_key)
                    // is resolved via the index, not a full scan of the (state,
                    // order_key) composite as history grows.
                    "SELECT COALESCE(MAX(order_key), 0) + 1 FROM queue_items INDEXED BY idx_queue_items_order_key",
                    [],
                    |r| r.get(0),
                )
                .map_err(StorageError::from)?;
            let now = now_ms();
            let id = Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO queue_items
                   (id, workspace_id, engine_id, session_id, session_mode, model, payload,
                    state, order_key, revision, attempt_count, dispatch_phase, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', ?8, 1, 0, 'prepare', ?9, ?9)",
                rusqlite::params![
                    id,
                    req.workspace_id,
                    req.engine_id,
                    req.session_id,
                    req.session_mode.as_str(),
                    req.model,
                    req.payload,
                    next_key,
                    now,
                ],
            )
            .map_err(StorageError::from)?;
            query_item(
                &tx,
                &format!("SELECT {ITEM_COLUMNS} FROM queue_items WHERE id = ?1"),
                rusqlite::params![id],
            )?
            .ok_or_else(|| QueueError::NotFound(id.clone()))
        })
    }

    // ---- reads ----

    pub fn get(&self, id: &str) -> Result<Option<QueueItem>, QueueError> {
        #[cfg(feature = "failpoints")]
        {
            let f = self
                .failpoints
                .lock()
                .expect("repo failpoints mutex poisoned");
            if let Some(pred) = &f.get_error {
                if pred(id) {
                    return Err(QueueError::StorageUnavailable(
                        "injected repo read failure (test)".into(),
                    ));
                }
            }
        }
        Ok(self.db.with_conn(|conn| {
            query_item(
                conn,
                &format!("SELECT {ITEM_COLUMNS} FROM queue_items WHERE id = ?1"),
                rusqlite::params![id],
            )
            .map_err(|e| StorageError::InvalidData(e.to_string()))
        })?)
    }

    /// Total eligibility scans performed (diagnostics/test gate).
    pub fn dispatch_scan_count(&self) -> u64 {
        self.dispatch_scans.load(Ordering::SeqCst)
    }

    /// Fail-closed validation of every persisted row's enum columns (TASK 24
    /// §9): a corrupted / partially migrated / future-schema row must disable
    /// dispatch with a typed error naming the exact invalid value — never a
    /// silent substitution into business state. Runs once at bootstrap
    /// (`QueueManager::init`); the queue then refuses to operate.
    pub fn validate_schema_integrity(&self) -> Result<(), QueueError> {
        self.db.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT id, state, session_mode, session_id, run_id, dispatch_phase, cancel_requested FROM queue_items",
                )
                .map_err(StorageError::from)?;
            let mut rows = stmt.query([]).map_err(StorageError::from)?;
            while let Some(row) = rows.next().map_err(StorageError::from)? {
                let id: String = row.get(0).map_err(StorageError::from)?;
                let state: String = row.get(1).map_err(StorageError::from)?;
                if QueueState::from_str(&state).is_none() {
                    return Err(StorageError::InvalidData(
                        QueueError::InvalidPersistedRow {
                            row_id: id,
                            field: "state",
                            value: state,
                        }
                        .to_string(),
                    ));
                }
                let session_mode: String = row.get(2).map_err(StorageError::from)?;
                if SessionMode::from_str(&session_mode).is_none() {
                    return Err(StorageError::InvalidData(
                        QueueError::InvalidPersistedRow {
                            row_id: id,
                            field: "session_mode",
                            value: session_mode,
                        }
                        .to_string(),
                    ));
                }
                let session_id: Option<String> = row.get(3).map_err(StorageError::from)?;
                if session_mode.eq_ignore_ascii_case("existing") {
                    let empty = match &session_id {
                        None => true,
                        Some(sid) => sid.trim().is_empty(),
                    };
                    if empty {
                        return Err(StorageError::InvalidData(
                            QueueError::InvalidPersistedRow {
                                row_id: id,
                                field: "session_id",
                                value: "<empty> (existing session_mode requires a session_id)".into(),
                            }
                            .to_string(),
                        ));
                    }
                }
                // DISPATCHED requires a nonempty run_id/session correlation
                // (TASK 24 §9): a corrupted/partial row without one is
                // invariant corruption — fail closed at bootstrap.
                if state.eq_ignore_ascii_case("dispatched") {
                    let run_id: Option<String> = row.get(4).map_err(StorageError::from)?;
                    let empty = match &run_id {
                        None => true,
                        Some(r) => r.trim().is_empty(),
                    };
                    if empty {
                        return Err(StorageError::InvalidData(
                            QueueError::InvalidPersistedRow {
                                row_id: id,
                                field: "run_id",
                                value: "<empty> (DISPATCHED requires a run_id)".into(),
                            }
                            .to_string(),
                        ));
                    }
                }
                let dispatch_phase: Option<String> = row.get(5).map_err(StorageError::from)?;
                if let Some(dp) = dispatch_phase {
                    if !dp.is_empty() && crate::model::DispatchPhase::from_str(&dp).is_none() {
                        return Err(StorageError::InvalidData(
                            QueueError::InvalidPersistedRow {
                                row_id: id,
                                field: "dispatch_phase",
                                value: dp,
                            }
                            .to_string(),
                        ));
                    }
                }
                let cancel_requested: Option<i64> = row.get(6).map_err(StorageError::from)?;
                if let Some(cr) = cancel_requested {
                    if cr != 0 && cr != 1 {
                        return Err(StorageError::InvalidData(
                            QueueError::InvalidPersistedRow {
                                row_id: id,
                                field: "cancel_requested",
                                value: cr.to_string(),
                            }
                            .to_string(),
                        ));
                    }
                }
            }
            Ok(())
        })
        .map_err(|e| match e {
            StorageError::InvalidData(msg) => QueueError::Internal(msg),
            other => QueueError::from(other),
        })
    }

    /// One bounded keyset page of lightweight eligibility rows. `after` must
    /// be the final row returned by the previous page; OFFSET is forbidden
    /// because its skipped-row work grows with queue depth. The deterministic
    /// tuple is backed by migration v8's dispatch index.
    pub fn list_candidate_page(
        &self,
        after: Option<&DispatchCandidate>,
    ) -> Result<Vec<DispatchCandidate>, QueueError> {
        self.dispatch_scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.db.with_conn(|conn| {
            let mut stmt = conn.prepare(match after {
                Some(_) => {
                    "SELECT id, revision, engine_id, workspace_id, session_mode, session_id, model, \
                            order_key, created_at \
                     FROM queue_items INDEXED BY idx_queue_items_dispatch_keyset \
                     WHERE state = 'queued' AND (order_key, created_at, id) > (?1, ?2, ?3) \
                     ORDER BY order_key, created_at, id LIMIT ?4"
                }
                None => {
                    "SELECT id, revision, engine_id, workspace_id, session_mode, session_id, model, \
                            order_key, created_at \
                     FROM queue_items INDEXED BY idx_queue_items_dispatch_keyset \
                     WHERE state = 'queued' \
                     ORDER BY order_key, created_at, id LIMIT ?1"
                }
            }).map_err(StorageError::from)?;
            let page_size = DISPATCH_CANDIDATE_PAGE_SIZE as i64;
            let mut rows = match after {
                Some(cursor) => stmt
                    .query(params![cursor.order_key, cursor.created_at, cursor.id, page_size])
                    .map_err(StorageError::from)?,
                None => stmt.query(params![page_size]).map_err(StorageError::from)?,
            };
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(StorageError::from)? {
                let id: String = row.get(0).map_err(StorageError::from)?;
                let session_mode: String = row.get(4).map_err(StorageError::from)?;
                let session_mode =
                    SessionMode::from_str(&session_mode)
                        .ok_or_else(|| {
                            QueueError::InvalidPersistedRow {
                                row_id: id.clone(),
                                field: "session_mode",
                                value: session_mode,
                            }
                        })
                        .map_err(|e| StorageError::InvalidData(e.to_string()))?;
                out.push(DispatchCandidate {
                    id,
                    revision: row.get(1).map_err(StorageError::from)?,
                    engine_id: row.get(2).map_err(StorageError::from)?,
                    workspace_id: row.get(3).map_err(StorageError::from)?,
                    session_mode,
                    session_id: row.get(5).map_err(StorageError::from)?,
                    model: row.get(6).map_err(StorageError::from)?,
                    order_key: row.get(7).map_err(StorageError::from)?,
                    created_at: row.get(8).map_err(StorageError::from)?,
                });
            }
            Ok(out)
        })?)
    }

    /// Workspace ids that currently hold an UNKNOWN item — the TASK 23
    /// ambiguity gate, fetched ONCE per eligibility scan instead of one
    /// COUNT query per candidate (PERFORMANCE.md, N+1 elimination).
    pub fn unknown_workspaces(&self) -> Result<std::collections::HashSet<String>, QueueError> {
        self.dispatch_scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.db.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT DISTINCT workspace_id FROM queue_items WHERE state = 'unknown'")
                .map_err(StorageError::from)?;
            let mut rows = stmt.query([]).map_err(StorageError::from)?;
            let mut out = std::collections::HashSet::new();
            while let Some(row) = rows.next().map_err(StorageError::from)? {
                out.insert(row.get::<_, String>(0).map_err(StorageError::from)?);
            }
            Ok(out)
        })?)
    }

    /// All QUEUED items in deterministic order (full materialization; used
    /// by diagnostics/UI paths that need the complete rows — the dispatcher
    /// uses bounded `list_candidate_page` keyset reads).
    pub fn list_queued(&self) -> Result<Vec<QueueItem>, QueueError> {
        self.dispatch_scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.db.with_conn(|conn| {
            query_items(
                conn,
                &format!(
                    "SELECT {ITEM_COLUMNS} FROM queue_items WHERE state = 'queued' \
                     ORDER BY order_key, created_at, id"
                ),
                &[],
            )
            .map_err(|e| StorageError::InvalidData(e.to_string()))
        })?)
    }

    /// Full snapshot for the UI: active items first, then bounded history.
    /// `unknown` items are active (blocked, user-resolvable — TASK 23 §149).
    /// Payloads are SQL-PROJECTED to `PAYLOAD_PREVIEW_BYTES` bytes (§13,
    /// TASK 24 perf): `substr(CAST(payload AS BLOB), 1, N)` clips in the
    /// database, so a thousand-item queue of up-to-64 KiB prompts never even
    /// serializes/mounts tens of MiB into Rust memory or IPC. The durable
    /// rows are untouched, `payload_truncated` flags the projection, and the
    /// full payload is fetched via `get` only when editing/inspecting.
    pub fn list_snapshot(&self, history_limit: usize) -> Result<Vec<QueueItem>, QueueError> {
        let preview = crate::model::PAYLOAD_PREVIEW_BYTES as i64;
        let projection = "substr(CAST(payload AS BLOB), 1, :P) AS payload, state, order_key, \
             revision, session_mode, model, lease_id, leased_at, attempt_count, run_id, \
             last_error, last_error_code, created_at, updated_at, \
             (octet_length(payload) > :P) AS payload_truncated";
        Ok(self.db.with_conn(|conn| {
            let mut items = query_items_snapshot(
                conn,
                &format!(
                    "SELECT id, workspace_id, engine_id, session_id, {projection} \
                     FROM queue_items \
                     WHERE state IN ('queued','leased','dispatched','unknown') \
                     ORDER BY order_key, created_at, id"
                ),
                rusqlite::named_params! { ":P": preview },
            )
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;
            // PERF-002: the terminal/stationary history is hard-bounded by
            // `LIMIT :L` (history_limit) — a snapshot can never materialize an
            // unbounded slice of completed items. The active-state set above is
            // bounded by nature (only live/queued/dispatched rows).
            let history = query_items_snapshot(
                conn,
                &format!(
                    "SELECT id, workspace_id, engine_id, session_id, {projection} \
                     FROM queue_items INDEXED BY idx_queue_items_terminal_updated \
                     WHERE state IN ('done','failed','cancelled') \
                     ORDER BY updated_at DESC LIMIT :L"
                ),
                rusqlite::named_params! { ":P": preview, ":L": history_limit as i64 },
            )
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;
            items.extend(history);
            Ok(items)
        })?)
    }

    /// Counts per state (diagnostics, §154). Unknown persisted states are
    /// surfaced as typed row errors, never silently omitted (TASK 24 §9).
    pub fn counts(&self) -> Result<[(QueueState, usize); 7], QueueError> {
        Ok(self.db.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT state, COUNT(*) FROM queue_items GROUP BY state")
                .map_err(StorageError::from)?;
            let mut counts = [(QueueState::Queued, 0usize); 7];
            let mut rows = stmt.query([]).map_err(StorageError::from)?;
            while let Some(row) = rows.next().map_err(StorageError::from)? {
                let state: String = row.get(0).map_err(StorageError::from)?;
                let n: i64 = row.get(1).map_err(StorageError::from)?;
                let s = QueueState::from_str(&state).ok_or_else(|| {
                    QueueError::InvalidPersistedRow {
                        row_id: "<aggregate>".into(),
                        field: "state",
                        value: state,
                    }
                })
                .map_err(|e| StorageError::InvalidData(e.to_string()))?;
                counts[s as usize].1 = n as usize;
            }
            Ok(counts)
        })?)
    }

    /// Current dispatch association for an item (run_id + lease), if any.
pub fn current_run(&self, id: &str) -> Result<Option<(String, Option<String>)>, QueueError> {
        Ok(self.db.with_conn(|conn| {
            conn.query_row(
                "SELECT run_id, lease_id FROM queue_items WHERE id = ?1",
                rusqlite::params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(StorageError::from)
        })?)
    }

    // ---- claim / lease (§15–§21) ----

    /// Atomic claim: exactly one caller wins per item (single UPDATE guarded
    /// on `state='queued'`). Returns true if this caller now owns the lease.
    pub fn claim(&self, id: &str) -> Result<bool, QueueError> {
        let lease_id = Uuid::new_v4().to_string();
        let now = now_ms();
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'leased', lease_id = ?2, leased_at = ?3,
                         dispatch_phase = 'prepare', updated_at = ?3
                     WHERE id = ?1 AND state = 'queued'
                       AND NOT EXISTS (
                         SELECT 1 FROM app_settings
                         WHERE key = 'queue.paused' AND value = '1'
                       )",
                    rusqlite::params![id, lease_id, now],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Enter the sending phase: session is durably associated and the send is
    /// (about to be) in flight. Idempotent for the New-mode path where
    /// `persist_session_created` already set phase `sending` right after the
    /// external session creation. A concurrent cancel (which only flips
    /// `cancel_requested`) does not block this; a concurrent state change
    /// does.
    pub fn begin_send(&self, id: &str, session_id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET session_id = ?2, dispatch_phase = 'sending', updated_at = ?3
                     WHERE id = ?1 AND state = 'leased' AND dispatch_phase IN ('prepare','sending')",
                    rusqlite::params![id, session_id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Mark the engine-accepted handoff: DISPATCHED + run_id + attempt++.
    /// This is the authoritative acceptance boundary (§27): only after this
    /// commit is the item safe from blind redispatch.
    pub fn mark_dispatched(&self, id: &str, run_id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'dispatched', run_id = ?2, attempt_count = attempt_count + 1,
                         updated_at = ?3
                     WHERE id = ?1 AND state = 'leased' AND dispatch_phase = 'sending'",
                    rusqlite::params![id, run_id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Release a lease without counting an attempt (engine/session wait,
    /// abort). Only the lease owner transitions (guard on lease_id).
    pub fn release_lease(&self, id: &str, lease_id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'queued', lease_id = NULL, leased_at = NULL,
                         dispatch_phase = 'prepare', updated_at = ?3
                     WHERE id = ?1 AND state = 'leased' AND lease_id = ?2",
                    rusqlite::params![id, lease_id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Terminal transition for a dispatched run. Guarded on
    /// `state='dispatched' AND run_id=?` — a stale or duplicate terminal for
    /// an old attempt can never mutate the current one (§174–§178).
    pub fn mark_terminal(
        &self,
        id: &str,
        run_id: &str,
        state: QueueState,
        error_code: Option<&str>,
        error_msg: Option<&str>,
    ) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = ?3, last_error_code = ?4, last_error = ?5, updated_at = ?6
                     WHERE id = ?1 AND state = 'dispatched' AND run_id = ?2",
                    rusqlite::params![id, run_id, state.as_str(), error_code, error_msg, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Mark a QUEUED item FAILED (validation failure discovered at scan
    /// time, e.g. the target session disappeared, §187).
    pub fn mark_failed_queued(&self, id: &str, code: &str, msg: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'failed', last_error_code = ?2, last_error = ?3,
                         revision = revision + 1, updated_at = ?4
                     WHERE id = ?1 AND state = 'queued'",
                    rusqlite::params![id, code, cap(msg), now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Mark FAILED before/without an accepted send (validation failure or
    /// rejected send). Guarded on LEASED so a terminal race cannot double-apply.
    pub fn mark_failed_leased(&self, id: &str, code: &str, msg: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'failed', last_error_code = ?2, last_error = ?3,
                         lease_id = NULL, leased_at = NULL, updated_at = ?4
                     WHERE id = ?1 AND state = 'leased'",
                    rusqlite::params![id, code, cap(msg), now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Mark LEASED → UNKNOWN: the send/create crossed the boundary but
    /// acceptance cannot be proven (transport loss / engine death). Never
    /// auto-redispatch; the workspace stays blocked until explicit user
    /// resolution (TASK 24 §9). Guarded on LEASED.
    ///
    /// When the run_id is known (an OutcomeUnknown dispatch receipt) it is
    /// persisted so a later authoritative terminal can reconcile this UNKNOWN
    /// item — correlation is never guessed (TASK 24 §9). The session-create
    /// path passes `None` (no run exists yet).
    pub fn mark_unknown_leased(
        &self,
        id: &str,
        run_id: Option<&str>,
        code: &str,
        msg: &str,
    ) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'unknown', run_id = ?2, last_error_code = ?3, last_error = ?4,
                         lease_id = NULL, leased_at = NULL, revision = revision + 1, updated_at = ?5
                     WHERE id = ?1 AND state = 'leased'",
                    rusqlite::params![id, run_id, code, cap(msg), now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Terminal transition for an UNKNOWN row with a MATCHING run_id (TASK
    /// 24 §9): only a later definitive terminal (completed/failed/cancelled)
    /// may reconcile an ambiguous run; unrelated/stale terminals are ignored
    /// (guarded on `state='unknown' AND run_id=?`). Never used to transition
    /// UNKNOWN → UNKNOWN.
    pub fn mark_terminal_unknown(
        &self,
        id: &str,
        run_id: &str,
        state: QueueState,
        error_code: Option<&str>,
        error_msg: Option<&str>,
    ) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = ?3, last_error_code = ?4, last_error = ?5, updated_at = ?6
                     WHERE id = ?1 AND state = 'unknown' AND run_id = ?2",
                    rusqlite::params![id, run_id, state.as_str(), error_code, error_msg, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// UNKNOWN rows that still carry a persisted run_id (TASK 24 §9): after
    /// restart their run correlation must survive so a later authoritative
    /// terminal from a resumed engine can reconcile them — never guessed,
    /// always the exact persisted id.
    pub fn unknown_runs_with_ids(
        &self,
    ) -> Result<Vec<(String, String, String)>, QueueError> {
        Ok(self.db.with_conn(|conn| {
            conn.prepare(
                "SELECT id, run_id, engine_id FROM queue_items \
                 WHERE state = 'unknown' AND run_id IS NOT NULL AND run_id != ''",
            )
            .map_err(StorageError::from)?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map_err(StorageError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
        })?)
    }

    // ---- user mutations (revision CAS) ----

    /// Edit a QUEUED item (§43). Rejects anything not QUEUED — payload is
    /// immutable after claim (§10).
    pub fn edit(
        &self,
        id: &str,
        expected_revision: i64,
        payload: &str,
        model: Option<&str>,
    ) -> Result<QueueItem, QueueError> {
        if payload.trim().is_empty() {
            return Err(QueueError::EmptyPayload);
        }
        if payload.len() > crate::model::PAYLOAD_MAX_BYTES {
            return Err(QueueError::PayloadTooLarge {
                bytes: payload.len(),
                max: crate::model::PAYLOAD_MAX_BYTES,
            });
        }
        let changed = self.db.with_conn(|conn| {
            conn.execute(
                "UPDATE queue_items
                     SET payload = ?3, model = ?4, revision = revision + 1, updated_at = ?5
                     WHERE id = ?1 AND state = 'queued' AND revision = ?2",
                rusqlite::params![id, expected_revision, payload, model, now_ms()],
            )
            .map_err(StorageError::from)
        })?;
        if changed == 1 {
            self.get(id)?.ok_or_else(|| QueueError::NotFound(id.into()))
        } else {
            self.resolve_conflict(id, expected_revision, "edit")?;
            Err(QueueError::Internal("edit failed without conflict".into()))
        }
    }

    /// Cancel a QUEUED item: terminal CANCELLED with revision guard (§45).
    pub fn cancel_queued(&self, id: &str, expected_revision: i64) -> Result<bool, QueueError> {
        let changed = self.db.with_conn(|conn| {
            conn.execute(
                "UPDATE queue_items
                     SET state = 'cancelled', revision = revision + 1, updated_at = ?3
                     WHERE id = ?1 AND state = 'queued' AND revision = ?2",
                rusqlite::params![id, expected_revision, now_ms()],
            )
            .map_err(StorageError::from)
        })?;
        if changed == 1 {
            Ok(true)
        } else {
            self.resolve_conflict(id, expected_revision, "cancel")?;
            Ok(false)
        }
    }

    /// Set durable cancel intent on a LEASED item (state guard — the worker
    /// is the only state owner; the intent is honored at its next step).
    pub fn request_cancel_leased(&self, id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET cancel_requested = 1, updated_at = ?2
                     WHERE id = ?1 AND state = 'leased'",
                    rusqlite::params![id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Durable cancel intent on a DISPATCHED item (TASK 24 §9). The intent
    /// survives process death; the coordinator transitions the row only when
    /// the matching authoritative terminal arrives — never fabricated.
    pub fn request_cancel_dispatched(&self, id: &str) -> Result<bool, QueueError> {
        #[cfg(feature = "failpoints")]
        {
            let f = self
                .failpoints
                .lock()
                .expect("repo failpoints mutex poisoned");
            if let Some(pred) = &f.cancel_dispatched_error {
                if pred(id) {
                    return Err(QueueError::StorageUnavailable(
                        "injected cancel-intent durability failure (test)".into(),
                    ));
                }
            }
        }
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET cancel_requested = 1, updated_at = ?2
                     WHERE id = ?1 AND state = 'dispatched'",
                    rusqlite::params![id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    pub fn is_cancel_requested(&self, id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let v: Option<i64> = conn
                .query_row(
                    "SELECT cancel_requested FROM queue_items WHERE id = ?1",
                    rusqlite::params![id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(StorageError::from)?;
            Ok(v.unwrap_or(0) == 1)
        })?)
    }

    /// Worker honors a durable cancel intent: LEASED → CANCELLED.
    pub fn cancel_from_intent(&self, id: &str, lease_id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'cancelled', lease_id = NULL, leased_at = NULL,
                         dispatch_phase = 'prepare', revision = revision + 1, updated_at = ?3
                     WHERE id = ?1 AND state = 'leased' AND lease_id = ?2 AND cancel_requested = 1",
                    rusqlite::params![id, lease_id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Retry: FAILED → QUEUED or UNKNOWN → QUEUED (manual; keeps attempt
    /// history visible). Retrying an UNKNOWN item is an explicit user act that
    /// acknowledges possible duplication risk — the prior ambiguous evidence
    /// stays visible (`last_error`/`last_error_code` are preserved; only the
    /// stale run association is cleared so a late old-attempt terminal can
    /// never mutate the new attempt, TASK 23 §20, §109).
    ///
    /// The new attempt is genuinely fresh: `cancel_requested` is reset to 0
    /// atomically with the transition — a previously cancel-requested
    /// UNKNOWN/FAILED item must execute its explicitly requested retry
    /// instead of being claimed and immediately cancelled (TASK 24 §9).
    pub fn retry(&self, id: &str, expected_revision: i64) -> Result<bool, QueueError> {
        let changed = self.db.with_conn(|conn| {
            conn.execute(
                "UPDATE queue_items
                     SET state = 'queued', run_id = NULL, dispatch_phase = 'prepare',
                         lease_id = NULL, leased_at = NULL, revision = revision + 1,
                         cancel_requested = 0, updated_at = ?3
                     WHERE id = ?1 AND state IN ('failed','unknown') AND revision = ?2",
                rusqlite::params![id, expected_revision, now_ms()],
            )
            .map_err(StorageError::from)
        })?;
        if changed == 1 {
            Ok(true)
        } else {
            self.resolve_conflict(id, expected_revision, "retry")?;
            Ok(false)
        }
    }

    /// Explicitly abandon an UNKNOWN item (TASK 24 §9): terminal CANCELLED
    /// with the prior ambiguity evidence retained (`last_error`/
    /// `last_error_code` are preserved, never cleared) — the user accepted
    /// the risk that the external run is not stopped. Revision CAS.
    pub fn resolve_unknown(&self, id: &str, expected_revision: i64) -> Result<bool, QueueError> {
        let changed = self.db.with_conn(|conn| {
            conn.execute(
                "UPDATE queue_items
                     SET state = 'cancelled', revision = revision + 1, updated_at = ?3
                     WHERE id = ?1 AND state = 'unknown' AND revision = ?2",
                rusqlite::params![id, expected_revision, now_ms()],
            )
            .map_err(StorageError::from)
        })?;
        if changed == 1 {
            Ok(true)
        } else {
            self.resolve_conflict(id, expected_revision, "resolve_unknown")?;
            Ok(false)
        }
    }

    /// Durably associate a freshly created external session with the item
    /// (TASK 24 §9). Called immediately after `create_session` returns — the
    /// external side effect exists, so the dispatch phase becomes `sending`
    /// (never classified as side-effect-free by crash recovery) and every
    /// subsequent wake/retry reuses this session id instead of creating
    /// another external session.
    pub fn persist_session_created(&self, id: &str, session_id: &str) -> Result<bool, QueueError> {
        #[cfg(feature = "failpoints")]
        {
            let f = self
                .failpoints
                .lock()
                .expect("repo failpoints mutex poisoned");
            if let Some(pred) = &f.persist_created_error {
                if pred(id) {
                    return Err(QueueError::StorageUnavailable(
                        "injected persist_session_created failure (test)".into(),
                    ));
                }
            }
        }
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET session_id = ?2, dispatch_phase = 'sending', updated_at = ?3
                     WHERE id = ?1 AND state = 'leased' AND dispatch_phase = 'prepare'",
                    rusqlite::params![id, session_id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// True when the workspace already has an UNKNOWN item — the TASK 23
    /// ambiguity gate (§50): a possibly-mutating old run whose outcome is
    /// unknown must block further mutating queued dispatch in the same
    /// workspace (a new agent could race the unknown old one on the same
    /// files). Other workspaces are unaffected (§51).
    pub fn workspace_has_unknown(&self, workspace_id: &str) -> Result<bool, QueueError> {
        self.dispatch_scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.db.with_conn(|conn| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM queue_items WHERE state = 'unknown' AND workspace_id = ?1",
                rusqlite::params![workspace_id],
                |r| r.get(0),
            )
            .map_err(StorageError::from)?;
            Ok(n > 0)
        })?)
    }

    /// True when the workspace has any ACTIVE/NONTERMINAL durable item
    /// (QUEUED/LEASED/DISPATCHED/UNKNOWN). Safe Forget must reject while
    /// such work references the workspace — deleting the identity would
    /// strand it (TASK 24 §9). Terminal history rows do not block forget.
    pub fn workspace_has_nonterminal(&self, workspace_id: &str) -> Result<bool, QueueError> {
        self.dispatch_scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.db.with_conn(|conn| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM queue_items
                 WHERE workspace_id = ?1
                   AND state IN ('queued','leased','dispatched','unknown')",
                rusqlite::params![workspace_id],
                |r| r.get(0),
            )
            .map_err(StorageError::from)?;
            Ok(n > 0)
        })?)
    }

    /// Safe session-delete gate: existing-session queue work must be resolved
    /// before its target metadata/upstream session can be removed.
    pub fn session_has_nonterminal(&self, session_id: &str) -> Result<bool, QueueError> {
        self.dispatch_scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.db.with_conn(|conn| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM queue_items
                 WHERE session_id = ?1
                   AND state IN ('queued','leased','dispatched','unknown')",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .map_err(StorageError::from)?;
            Ok(n > 0)
        })?)
    }

    /// Transactional reorder of QUEUED items (§41–§42). The full QUEUED order
    /// is recomputed inside one transaction from current DB state, so a crash
    /// mid-reorder leaves either the old or the new complete order — never a
    /// half-state. Revision CAS guards the moved item against stale UIs.
    pub fn reorder(
        &self,
        id: &str,
        expected_revision: i64,
        new_index: usize,
    ) -> Result<(), QueueError> {
        self.db.transaction_with(|tx| {
            let current: Option<QueueItem> = query_item(
                &tx,
                &format!("SELECT {ITEM_COLUMNS} FROM queue_items WHERE id = ?1"),
                rusqlite::params![id],
            )?;
            let Some(item) = current else {
                return Err(QueueError::NotFound(id.into()));
            };
            if item.revision != expected_revision {
                return Err(QueueError::Conflict {
                    item_id: id.into(),
                    current: item.revision,
                    expected: expected_revision,
                });
            }
            if item.state != QueueState::Queued {
                return Err(QueueError::InvalidState {
                    item_id: id.into(),
                    detail: format!(
                        "only queued items can be reordered (state {})",
                        item.state.as_str()
                    ),
                });
            }
            let mut order: Vec<(String, i64)> = tx
                .prepare(
                    "SELECT id, order_key FROM queue_items WHERE state = 'queued' \
                     ORDER BY order_key, created_at, id",
                )
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            let pos = order
                .iter()
                .position(|(oid, _)| oid == id)
                .ok_or_else(|| QueueError::NotFound(id.into()))?;
            let idx = new_index.min(order.len().saturating_sub(1));
            if pos == idx {
                return Ok(());
            }
            if pos.abs_diff(idx) == 1 {
                let other_pos = idx;
                let (moved_id, moved_key) = &order[pos];
                let (other_id, other_key) = &order[other_pos];
                let now = now_ms();
                tx.execute(
                    "UPDATE queue_items SET order_key = ?2, revision = revision + 1, updated_at = ?3 WHERE id = ?1",
                    rusqlite::params![moved_id, other_key, now],
                )
                .map_err(StorageError::from)?;
                tx.execute(
                    "UPDATE queue_items SET order_key = ?2, updated_at = ?3 WHERE id = ?1",
                    rusqlite::params![other_id, moved_key, now],
                )
                .map_err(StorageError::from)?;
                return Ok(());
            }
            let (moved_id, _) = order.remove(pos);
            order.insert(idx, (moved_id, 0));
            for (i, (oid, old_key)) in order.iter().enumerate() {
                if *old_key != (i + 1) as i64 {
                    tx.execute(
                        "UPDATE queue_items SET order_key = ?2, updated_at = ?3 WHERE id = ?1",
                        rusqlite::params![oid, (i + 1) as i64, now_ms()],
                    )
                    .map_err(StorageError::from)?;
                }
            }
            // Bump the moved item's revision inside the same transaction.
            tx.execute(
                "UPDATE queue_items SET revision = revision + 1 WHERE id = ?1",
                rusqlite::params![id],
            )
            .map_err(StorageError::from)?;
            Ok(())
        })
    }

    // ---- startup recovery (§20–§23) ----

    /// Process-lifetime recovery: every LEASED row is from a previous app
    /// lifetime (single instance ⇒ no other live owner). Runs before any
    /// dispatch (§20).
    ///
    /// - phase `prepare` + `cancel_requested` → CANCELLED (no external side
    ///   effect exists; the durable user intent is honored exactly).
    /// - phase `prepare` (no external side effect) → QUEUED (safe restore).
    /// - phase `sending` (engine may have accepted) → **UNKNOWN** — never
    ///   blind redispatch (§23, §26, TASK 23 §17–§18). A `sending` row with
    ///   `cancel_requested` is also UNKNOWN, never CANCELLED: the engine may
    ///   have accepted the send and the cancel was never delivered, so
    ///   reporting cancellation would be fabricated. The cancel intent is
    ///   retained on the row and the ambiguity is surfaced; only explicit
    ///   manual abandonment or authoritative reconciliation may resolve it
    ///   (TASK 24 §9).
    /// - DISPATCHED rows → **UNKNOWN**: no engine in this baseline can
    ///   reconcile a dispatched run after an app restart (Harness ACP
    ///   sessions are connection-owned; OpenCode/Fake run registries are
    ///   in-memory), so the outcome cannot be proven and the item must not
    ///   silently re-run or be presented as a live run. It blocks its
    ///   workspace until the user resolves it (TASK 23 §28–§31, §50).
    pub fn recover(&self) -> Result<RecoveryReport, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let leased: Vec<(String, String, i64, i64)> = conn
                .prepare(
                    "SELECT id, dispatch_phase, cancel_requested, revision FROM queue_items \
                     WHERE state = 'leased'",
                )
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            let mut report = RecoveryReport::default();
            for (id, phase, cancel_requested, _revision) in leased {
                if phase == "prepare" {
                    if cancel_requested == 1 {
                        // No external side effect: the durable cancel intent
                        // is honored exactly.
                        conn.execute(
                            "UPDATE queue_items
                             SET state = 'cancelled', lease_id = NULL, leased_at = NULL,
                                 dispatch_phase = 'prepare', revision = revision + 1, updated_at = ?2
                             WHERE id = ?1 AND state = 'leased'",
                            rusqlite::params![id, now_ms()],
                        )
                        .map_err(StorageError::from)?;
                        report.cancelled_from_intent += 1;
                    } else {
                        conn.execute(
                            "UPDATE queue_items
                             SET state = 'queued', lease_id = NULL, leased_at = NULL,
                                 updated_at = ?2
                             WHERE id = ?1 AND state = 'leased'",
                            rusqlite::params![id, now_ms()],
                        )
                        .map_err(StorageError::from)?;
                        report.recovered_to_queued += 1;
                    }
                } else {
                    // phase `sending`: the engine may have accepted the send.
                    // UNKNOWN regardless of cancel intent — a cancel that was
                    // never delivered must not become a fabricated CANCELLED.
                    // The cancel_requested flag is retained on the row.
                    let (code, msg) = if cancel_requested == 1 {
                        (
                            "cancel_unknown",
                            "crash during engine handoff with an undelivered cancel request: the external run may still be active; cancellation was NOT delivered and the outcome cannot be proven",
                        )
                    } else {
                        (
                            "dispatch_unknown",
                            "crash during engine handoff: external acceptance cannot be proven; automatic retry disabled to avoid duplicate work",
                        )
                    };
                    conn.execute(
                        "UPDATE queue_items
                         SET state = 'unknown', last_error_code = ?2, last_error = ?3,
                             lease_id = NULL, leased_at = NULL, revision = revision + 1, updated_at = ?4
                         WHERE id = ?1 AND state = 'leased'",
                        rusqlite::params![id, code, msg, now_ms()],
                    )
                    .map_err(StorageError::from)?;
                    report.marked_unknown += 1;
                }
            }
            // DISPATCHED at restart: the run's authority is unrecoverable in
            // this process (engines are supervised children; run registries
            // are in-memory, Harness ACP sessions are connection-owned). Mark
            // UNKNOWN — never present a dead run as live, never resend.
            let dispatched: Vec<String> = conn
                .prepare("SELECT id FROM queue_items WHERE state = 'dispatched'")
                .map_err(StorageError::from)?
                .query_map([], |r| r.get(0))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            for id in &dispatched {
                conn.execute(
                    "UPDATE queue_items
                     SET state = 'unknown', last_error_code = 'dispatch_unknown',
                         last_error = 'run was dispatched at shutdown: its outcome cannot be reconciled after restart; automatic retry disabled to avoid duplicate work',
                         updated_at = ?2
                     WHERE id = ?1 AND state = 'dispatched'",
                    rusqlite::params![id, now_ms()],
                )
                .map_err(StorageError::from)?;
            }
            report.marked_unknown_dispatched = dispatched.len();
            Ok(report)
        })?)
    }

    /// Release a LEASED item back to QUEUED after an in-process abort that
    /// kept the session reference (no external side effect happened — the
    /// send was never called). Keeps `session_id` so a New-mode item reuses
    /// the session on re-dispatch (§59–§60).
    pub fn release_after_interrupt(&self, id: &str, lease_id: &str) -> Result<bool, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'queued', dispatch_phase = 'prepare',
                         lease_id = NULL, leased_at = NULL, updated_at = ?3
                     WHERE id = ?1 AND state = 'leased' AND lease_id = ?2 AND dispatch_phase = 'sending'",
                    rusqlite::params![id, lease_id, now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed == 1)
        })?)
    }

    /// Shutdown-safe lease release: LEASED items still in `prepare` have no
    /// external side effect and are restored to QUEUED before storage closes
    /// (§80). `sending`/DISPATCHED items are left for restart recovery.
    pub fn release_prepare_leases_on_shutdown(&self) -> Result<usize, QueueError> {
        Ok(self.db.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE queue_items
                     SET state = 'queued', lease_id = NULL, leased_at = NULL,
                         dispatch_phase = 'prepare', updated_at = ?1
                     WHERE state = 'leased' AND dispatch_phase = 'prepare' AND cancel_requested = 0",
                    rusqlite::params![now_ms()],
                )
                .map_err(StorageError::from)?;
            Ok(changed)
        })?)
    }

    // ---- conflict resolution ----

    /// Distinguish NotFound from Conflict after a failed CAS.
    fn resolve_conflict(&self, id: &str, expected: i64, op: &str) -> Result<(), QueueError> {
        match self.get(id)? {
            None => Err(QueueError::NotFound(id.into())),
            Some(item) => {
                if item.revision != expected {
                    Err(QueueError::Conflict {
                        item_id: id.into(),
                        current: item.revision,
                        expected,
                    })
                } else {
                    Err(QueueError::InvalidState {
                        item_id: id.into(),
                        detail: format!(
                            "{op} requires a different state (current {})",
                            item.state.as_str()
                        ),
                    })
                }
            }
        }
    }
}

/// Bounded safe error detail (never a raw HTTP body / secret-bearing stack).
fn cap(s: &str) -> &str {
    let max = crate::model::LAST_ERROR_MAX_CHARS;
    if s.len() <= max {
        s
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}
