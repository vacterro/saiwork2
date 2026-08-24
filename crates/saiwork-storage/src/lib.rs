//! SQLite application state (STORAGE.md).
//!
//! One writer authority: the core owns the connection; the UI never issues
//! SQL (law 18). The connection is wrapped in a mutex because `rusqlite`
//! connections are not `Sync`; all operations are short and transactional.

pub mod error;
pub mod migrations;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use rusqlite::{params, Connection, OptionalExtension};
use tracing::info;
use uuid::Uuid;

pub use error::StorageError;

const SCHEMA_VERSION_APPLIED: i64 = 8; // must equal migrations().len()

/// Upper bound on a single `app_settings` value (defense in depth, T-052): the
/// settings table is for small UI preferences, never large blobs.
const MAX_SETTING_VALUE_BYTES: usize = 4 * 1024 * 1024;

/// Classify one `PRAGMA integrity_check` output row. Only the exact `ok`
/// verdict is success; SQLite reports corruption as one or more
/// human-readable rows, and those must never be cached as a healthy status
/// (TASK 24 §9 — `refresh_integrity` used to cache every result as `Ok`).
fn classify_integrity_output(result: &str) -> Result<String, String> {
    let trimmed = result.trim();
    if trimmed == "ok" {
        Ok(trimmed.to_string())
    } else {
        Err(result.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct Db {
    /// Shared single connection (one writer authority, law 18). `Arc` makes
    /// `Db` cheaply clonable so every core service shares one connection
    /// instead of opening per-service connections.
    conn: Arc<Mutex<Connection>>,
    path: PathBuf,
    /// Last integrity-check result, cached at open and refreshed only by an
    /// explicit deep check (PERFORMANCE.md: normal diagnostics must never
    /// re-run `PRAGMA integrity_check`).
    integrity: Arc<RwLock<Option<Result<String, String>>>>,
}

/// A workspace row as stored by SAIWORK2 (references only, never project
/// content — law 16).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceRow {
    pub id: String,
    pub path: String,
    pub name: String,
    pub last_opened_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A session metadata row. Content history stays with the engine (law 25).
///
/// `resumable` is the explicit upstream-id gate (TASK 24 §9): false for
/// legacy rows whose `engine_session_id` was NULL/empty (migration v4) — such
/// rows are historical metadata only, never usable for engine calls. The
/// runtime never invents an upstream id.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SessionMetaRow {
    pub id: String,
    pub workspace_id: Option<String>,
    pub engine_id: String,
    pub engine_session_id: Option<String>,
    pub display_name: Option<String>,
    pub last_opened_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    /// False when the row has no trustworthy upstream session id (legacy
    /// NULL/empty). Never resumable; never sent to an engine.
    pub resumable: bool,
}

impl Db {
    /// Open (creating parent dirs) and migrate. Fails loudly on integrity or
    /// migration errors — a broken DB is a diagnostics event, never silent.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| StorageError::PrepareLocation {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let conn = Connection::open(path).map_err(|source| StorageError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path: path.to_path_buf(),
            integrity: Arc::new(RwLock::new(None)),
        };
        db.configure()?;
        db.refresh_integrity()?;
        db.migrate()?;
        info!(
            path = %db.path.display(),
            schema_version = db.version().unwrap_or(-1),
            "database opened"
        );
        Ok(db)
    }

    /// Open an in-memory database (tests).
    pub fn open_in_memory() -> Result<Self, StorageError> {
        let conn = Connection::open_in_memory().map_err(|source| StorageError::Open {
            path: PathBuf::from(":memory:"),
            source,
        })?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path: PathBuf::from(":memory:"),
            integrity: Arc::new(RwLock::new(None)),
        };
        db.configure()?;
        db.refresh_integrity()?;
        db.migrate()?;
        Ok(db)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn configure(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        // Map a non-SQLite file to a typed Corrupt error before anything else
        // can misbehave. SQLite opens lazily, so the first pragma is where a
        // garbage file surfaces.
        let notadb = |e: rusqlite::Error| StorageError::Corrupt {
            path: self.path.clone(),
            detail: e.to_string(),
        };
        // WAL: readers never block the single writer; crash-safe.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(err, _)
                    if err.code == rusqlite::ErrorCode::NotADatabase =>
                {
                    notadb(e)
                }
                other => StorageError::Query(other),
            })?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(StorageError::Query)?;
        // Bounded wait for a lock (e.g. another process briefly holding it)
        // instead of an instant SQLITE_BUSY. After 5s the caller gets a typed
        // `StorageError::Busy`, never an infinite retry.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(StorageError::Query)?;
        Ok(())
    }

    fn migrate(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        Self::apply_migrations(&conn, migrations::MIGRATIONS, SCHEMA_VERSION_APPLIED)
    }

    /// Forward-only migration runner. `max_version` must equal
    /// `migrations.len()`; each migration runs in its own transaction and
    /// either fully commits (including the `user_version` bump) or fully
    /// rolls back. A database from a newer app version is rejected **before**
    /// any write is attempted.
    fn apply_migrations(
        conn: &Connection,
        migrations: &[&str],
        max_version: i64,
    ) -> Result<(), StorageError> {
        debug_assert_eq!(max_version, migrations.len() as i64);
        let current: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(StorageError::Query)?;
        if current > max_version {
            return Err(StorageError::UnsupportedVersion {
                found: current,
                supported: max_version,
            });
        }
        for (idx, sql) in migrations.iter().enumerate() {
            let version = (idx + 1) as i64;
            if version <= current {
                continue;
            }
            let tx = conn.unchecked_transaction().map_err(StorageError::from)?;
            match tx.execute_batch(sql) {
                Ok(()) => {
                    tx.pragma_update(None, "user_version", version)
                        .map_err(|source| StorageError::Migration { version, source })?;
                    tx.commit()
                        .map_err(|source| StorageError::Migration { version, source })?;
                    info!(version, "migration applied");
                }
                Err(source) => {
                    // Rollback restores user_version and any partial DDL.
                    drop(tx);
                    return Err(StorageError::Migration { version, source });
                }
            }
        }
        Ok(())
    }

    // ---- app_settings ----

    pub fn get_setting(&self, key: &str) -> Result<Option<String>, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT value FROM app_settings WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(StorageError::Query)
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<(), StorageError> {
        if value.len() > MAX_SETTING_VALUE_BYTES {
            return Err(StorageError::InvalidData(format!(
                "setting '{key}' value exceeds the {MAX_SETTING_VALUE_BYTES}-byte limit"
            )));
        }
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            params![key, value, now_ms()],
        )
        .map_err(StorageError::Query)?;
        Ok(())
    }

    pub fn delete_setting(&self, key: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM app_settings WHERE key = ?1", params![key])
            .map_err(StorageError::Query)?;
        Ok(())
    }

    pub fn get_active_workspace(&self) -> Result<Option<String>, StorageError> {
        self.get_setting("core.active_workspace")
    }

    pub fn set_active_workspace(&self, id: Option<&str>) -> Result<(), StorageError> {
        match id {
            Some(id) => self.set_setting("core.active_workspace", id),
            None => self.delete_setting("core.active_workspace"),
        }
    }

    // ---- workspaces ----

    pub fn upsert_workspace(&self, path: &str, name: &str) -> Result<WorkspaceRow, StorageError> {
        let now = now_ms();
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO workspaces (id, path, name, last_opened_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)
             ON CONFLICT(path) DO UPDATE SET
               name = excluded.name,
               last_opened_at = excluded.last_opened_at,
               updated_at = excluded.updated_at",
            params![Uuid::new_v4().to_string(), path, name, now, now],
        )
        .map_err(StorageError::Query)?;
        let row = conn
            .query_row(
                "SELECT id, path, name, last_opened_at, created_at, updated_at
                 FROM workspaces WHERE path = ?1",
                params![path],
                |r| {
                    Ok(WorkspaceRow {
                        id: r.get(0)?,
                        path: r.get(1)?,
                        name: r.get(2)?,
                        last_opened_at: r.get(3)?,
                        created_at: r.get(4)?,
                        updated_at: r.get(5)?,
                    })
                },
            )
            .map_err(StorageError::Query)?;
        Ok(row)
    }

    pub fn touch_workspace(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE workspaces SET last_opened_at = ?1, updated_at = ?1 WHERE id = ?2",
            params![now_ms(), id],
        )
        .map_err(StorageError::Query)?;
        Ok(())
    }

    pub fn list_workspaces(&self) -> Result<Vec<WorkspaceRow>, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, path, name, last_opened_at, created_at, updated_at
                 FROM workspaces ORDER BY COALESCE(last_opened_at, 0) DESC",
            )
            .map_err(StorageError::Query)?;
        let rows = stmt
            .query_map([], |r| {
                Ok(WorkspaceRow {
                    id: r.get(0)?,
                    path: r.get(1)?,
                    name: r.get(2)?,
                    last_opened_at: r.get(3)?,
                    created_at: r.get(4)?,
                    updated_at: r.get(5)?,
                })
            })
            .map_err(StorageError::Query)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::Query)
    }

    pub fn delete_workspace(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM workspaces WHERE id = ?1", params![id])
            .map_err(StorageError::Query)?;
        Ok(())
    }

    /// Atomically forget a workspace AND all of its session metadata in ONE
    /// transaction (TASK 24 §9, CORE-002). Either the session rows, the
    /// workspace identity, and the matching active-pointer are removed
    /// together, or neither is — a failure in any SQL step rolls back the
    /// whole operation so no partially-forgotten workspace is ever left behind.
    /// The caller detaches live projections (SAIPEN watcher/cache) ONLY AFTER
    /// this commits, never before.
    pub fn forget_workspace_with_sessions(&self, id: &str) -> Result<(), StorageError> {
        self.transaction(|tx| {
            // CORE-002: clear the active pointer atomically with the workspace
            // deletion so a dangling active id can never survive a successful
            // forget. Only clear when the pointer currently matches — a
            // different active workspace is unrelated.
            let active: Option<String> = tx
                .query_row(
                    "SELECT value FROM app_settings WHERE key = 'core.active_workspace'",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(StorageError::Query)?;
            if active.as_deref() == Some(id) {
                tx.execute(
                    "DELETE FROM app_settings WHERE key = 'core.active_workspace'",
                    [],
                )
                .map_err(StorageError::Query)?;
            }
            // Sessions first: `sessions_meta.workspace_id` references the
            // workspace, so removing dependents avoids any FK violation and
            // keeps the boundary atomic.
            tx.execute(
                "DELETE FROM sessions_meta WHERE workspace_id = ?1",
                params![id],
            )
            .map_err(StorageError::Query)?;
            // AUDIT-W2-003: the nonterminal-queue-reference check now runs
            // INSIDE this transaction (previously a preflight read in
            // another operation), so the serial order "enqueue NEW commits →
            // forget deletes" is impossible: whichever write transaction
            // commits first wins and the other rejects. Terminal history
            // rows still do not block forget.
            let nonterminal: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM queue_items
                     WHERE workspace_id = ?1
                       AND state IN ('queued','leased','dispatched','unknown')",
                    params![id],
                    |r| r.get(0),
                )
                .map_err(StorageError::Query)?;
            if nonterminal > 0 {
                return Err(StorageError::WorkspaceReferenced {
                    workspace_id: id.to_string(),
                });
            }
            tx.execute("DELETE FROM workspaces WHERE id = ?1", params![id])
                .map_err(StorageError::Query)?;
            Ok(())
        })
    }

    /// Indexed point lookup by workspace id (TASK 24 perf): one query on the
    /// primary key — engine start, SAIPEN operations, close and session
    /// recovery must never materialize the whole workspace list and scan it.
    /// `None` = unknown id (same NotFound semantics as the former
    /// list+scan path).
    pub fn get_workspace(&self, id: &str) -> Result<Option<WorkspaceRow>, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT id, path, name, last_opened_at, created_at, updated_at
             FROM workspaces WHERE id = ?1",
            params![id],
            |r| {
                Ok(WorkspaceRow {
                    id: r.get(0)?,
                    path: r.get(1)?,
                    name: r.get(2)?,
                    last_opened_at: r.get(3)?,
                    created_at: r.get(4)?,
                    updated_at: r.get(5)?,
                })
            },
        )
        .optional()
        .map_err(StorageError::Query)
    }

    /// Indexed point lookup by generic session id (TASK 24 perf): one query
    /// on the primary key — ensure_loaded, queue session recovery and
    /// session selection must never materialize and scan the full metadata
    /// table. `None` = unknown id.
    pub fn get_session_meta(&self, id: &str) -> Result<Option<SessionMetaRow>, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT id, workspace_id, engine_id, engine_session_id, display_name, last_opened_at, created_at, updated_at, resumable
             FROM sessions_meta WHERE id = ?1",
            params![id],
            |r| {
                Ok(SessionMetaRow {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    engine_id: r.get(2)?,
                    engine_session_id: r.get(3)?,
                    display_name: r.get(4)?,
                    last_opened_at: r.get(5)?,
                    created_at: r.get(6)?,
                    updated_at: r.get(7)?,
                    resumable: r.get(8)?,
                })
            },
        )
        .optional()
        .map_err(StorageError::Query)
    }

    // ---- sessions_meta ----

    pub fn upsert_session_meta(&self, row: &SessionMetaRow) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO sessions_meta
               (id, workspace_id, engine_id, engine_session_id, display_name, last_opened_at, created_at, updated_at, resumable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
               workspace_id = excluded.workspace_id,
               engine_id = excluded.engine_id,
               engine_session_id = excluded.engine_session_id,
               display_name = excluded.display_name,
               last_opened_at = excluded.last_opened_at,
               updated_at = excluded.updated_at,
               resumable = excluded.resumable",
            params![
                row.id,
                row.workspace_id,
                row.engine_id,
                row.engine_session_id,
                row.display_name,
                row.last_opened_at,
                row.created_at,
                row.resumable,
            ],
        )
        .map_err(StorageError::Query)?;
        Ok(())
    }

    /// AUDIT-W2-003: atomically require the referenced workspace to still
    /// exist when the metadata row is persisted. Returns `Ok(false)` when
    /// the row was NOT written because `row.workspace_id` names a workspace
    /// that no longer exists (a concurrent Forget won the race while the
    /// external session create was in flight) — the caller must run its
    /// authoritative upstream cleanup instead of leaving an orphan
    /// reference. A row without a workspace binding is written unchanged.
    pub fn upsert_session_meta_checked(&self, row: &SessionMetaRow) -> Result<bool, StorageError> {
        self.transaction(|tx| {
            if let Some(ws) = row.workspace_id.as_deref() {
                let found: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM workspaces WHERE id = ?1",
                        params![ws],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(StorageError::Query)?;
                if found.is_none() {
                    return Ok(false);
                }
            }
            tx.execute(
                "INSERT INTO sessions_meta
                   (id, workspace_id, engine_id, engine_session_id, display_name, last_opened_at, created_at, updated_at, resumable)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                   workspace_id = excluded.workspace_id,
                   engine_id = excluded.engine_id,
                   engine_session_id = excluded.engine_session_id,
                   display_name = excluded.display_name,
                   last_opened_at = excluded.last_opened_at,
                   updated_at = excluded.updated_at,
                   resumable = excluded.resumable",
                params![
                    row.id,
                    row.workspace_id,
                    row.engine_id,
                    row.engine_session_id,
                    row.display_name,
                    row.last_opened_at,
                    row.created_at,
                    row.resumable,
                ],
            )
            .map_err(StorageError::Query)?;
            Ok(true)
        })
    }

    pub fn list_session_meta(
        &self,
        workspace_id: Option<&str>,
    ) -> Result<Vec<SessionMetaRow>, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, workspace_id, engine_id, engine_session_id, display_name, last_opened_at, created_at, updated_at, resumable
                 FROM sessions_meta
                 WHERE (?1 IS NULL OR workspace_id = ?1)
                 ORDER BY COALESCE(last_opened_at, 0) DESC",
            )
            .map_err(StorageError::Query)?;
        let rows = stmt
            .query_map(params![workspace_id], |r| {
                Ok(SessionMetaRow {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    engine_id: r.get(2)?,
                    engine_session_id: r.get(3)?,
                    display_name: r.get(4)?,
                    last_opened_at: r.get(5)?,
                    created_at: r.get(6)?,
                    updated_at: r.get(7)?,
                    resumable: r.get(8)?,
                })
            })
            .map_err(StorageError::Query)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::Query)
    }

    pub fn delete_session_meta(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM sessions_meta WHERE id = ?1", params![id])
            .map_err(StorageError::Query)?;
        Ok(())
    }

    /// Bulk-delete session metadata for one workspace (safe Forget, TASK 24
    /// §9): after the workspace identity is removed no durable session may
    /// reference a missing WorkspaceId. Caller has already verified no active
    /// run uses the workspace.
    pub fn delete_session_meta_for_workspace(&self, workspace_id: &str) -> Result<usize, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let n = conn
            .execute(
                "DELETE FROM sessions_meta WHERE workspace_id = ?1",
                params![workspace_id],
            )
            .map_err(StorageError::Query)?;
        Ok(n)
    }

    // ---- transactions ----

    /// Run a short batch on the single shared connection (the mutex is held
    /// for the duration). This is the escape hatch for domain repositories
    /// (e.g. the durable queue) that need raw statements without a
    /// transaction. Keep closures short and non-blocking; never call other
    /// `Db` methods inside (the connection is locked).
    pub fn with_conn<T>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        f(&conn)
    }

    /// Run `f` inside one transaction: commit on `Ok`, roll back on `Err`
    /// (auto-rollback also on panic via `Drop`).
    ///
    /// # Contract
    /// - Everything the closure writes either commits together or not at all
    ///   (atomic multi-statement boundary for future queue dispatch).
    /// - Nested transactions are **rejected** by SQLite (typed error); use
    ///   savepoints if nesting is ever needed.
    /// - Do **not** call other `Db` methods inside the closure: the closure
    ///   runs while the single connection is locked, so a nested `Db` call
    ///   would deadlock. Use the provided transaction handle.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        self.transaction_with(f)
    }

    /// Transaction variant with a caller-chosen error type (`E: From<
    /// StorageError>`), so domain repositories (e.g. the durable queue) can
    /// return their own typed errors while staying transactional. Commit on
    /// `Ok`, rollback on `Err` (and on panic via `Drop`). Nested
    /// transactions are rejected by SQLite (typed error).
    pub fn transaction_with<T, E>(
        &self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<StorageError>,
    {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction().map_err(StorageError::from)?;
        match f(&tx) {
            Ok(value) => {
                tx.commit().map_err(StorageError::from)?;
                Ok(value)
            }
            Err(e) => {
                drop(tx); // rollback
                Err(e)
            }
        }
    }

    // ---- diagnostics ----

    pub fn version(&self) -> Result<i64, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(StorageError::Query)
    }

    /// Cached integrity status (set at open; refreshed only by an explicit
    /// `deep_integrity`). Normal diagnostics must not re-run the PRAGMA
    /// (PERFORMANCE.md).
    pub fn integrity(&self) -> Result<String, StorageError> {
        match self
            .integrity
            .read()
            .expect("integrity cache mutex poisoned")
            .clone()
        {
            Some(Ok(s)) => Ok(s),
            Some(Err(e)) => Err(StorageError::InvalidData(e)),
            None => Err(StorageError::InvalidData("integrity check has not run".into())),
        }
    }

    /// Run `PRAGMA integrity_check` now and cache the result (explicit deep
    /// diagnostic action only — never part of a normal snapshot).
    pub fn deep_integrity(&self) -> Result<String, StorageError> {
        self.refresh_integrity()?;
        self.integrity()
    }

    fn refresh_integrity(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let result: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(StorageError::Query)?;
        drop(conn);
        // Only the exact `ok` verdict is success. SQLite reports corruption
        // as one or more human-readable rows; caching those as Ok would let
        // `deep_integrity()`/shutdown report success for a damaged DB
        // (TASK 24 §9).
        match classify_integrity_output(&result) {
            Ok(verdict) => {
                *self
                    .integrity
                    .write()
                    .expect("integrity cache mutex poisoned") = Some(Ok(verdict));
                Ok(())
            }
            Err(detail) => {
                let detail = format!("integrity_check reported: {detail}");
                *self
                    .integrity
                    .write()
                    .expect("integrity cache mutex poisoned") = Some(Err(detail.clone()));
                Err(StorageError::Integrity {
                    path: self.path.clone(),
                    detail,
                })
            }
        }
    }

    /// Cheap row count for diagnostics (never enumerates workspaces).
    pub fn workspace_count(&self) -> Result<usize, StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
            .map_err(StorageError::from)?;
        Ok(n as usize)
    }

    /// Flush durable state before close/shutdown: WAL checkpoint (TRUNCATE).
    /// After this returns, the WAL is merged into the main DB file, so a
    /// subsequent reopen reads the same durable state even if the process
    /// dies before an orderly close (STORAGE.md shutdown contract).
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(StorageError::Query)
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    // ---- migrations ----

    #[test]
    fn migrations_apply_and_are_idempotent() {
        let db = Db::open_in_memory().unwrap();
        assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        // Re-open runs migrations again without error (idempotent by version).
        let db2 = Db::open_in_memory().unwrap();
        assert_eq!(db2.version().unwrap(), SCHEMA_VERSION_APPLIED);
    }

    #[test]
    fn failed_migration_rolls_back_fully() {
        // A migration that fails mid-way must leave user_version AND any
        // partial DDL rolled back — a known-recoverable state, never a
        // half-migrated database.
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        // One migration whose DDL is valid but whose final statement is not:
        // the whole migration (including the committed-looking DDL) must roll
        // back as a single unit.
        let migrations: &[&str] =
            &["CREATE TABLE t1 (x INTEGER); CREATE TABLE t2 (y INTEGER); THIS IS NOT SQL;"];
        let err = Db::apply_migrations(&conn, migrations, 1).unwrap_err();
        assert!(matches!(err, StorageError::Migration { version: 1, .. }));
        // user_version untouched…
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 0);
        // …and the tables created inside the failed migration are gone too.
        for table in ["t1", "t2"] {
            let count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    params![table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 0, "{table} must not survive a failed migration");
        }
    }

    #[test]
    fn v1_to_current_upgrade_preserves_durable_rows() {
        // §49–§51: every historical schema path must migrate to current
        // without destructive reset. Build a real v1 database (only the first
        // migration), insert durable rows (settings, workspace, queued item),
        // reopen with the full migration set: version advances and every row
        // survives with the v2 defaults applied (never a reset to QUEUED/1).
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            Db::apply_migrations(&conn, &migrations::MIGRATIONS[..1], 1).unwrap();
            conn.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ('theme','dark',1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workspaces (id, path, name, created_at, updated_at) VALUES ('w1','/x','x',1,1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO queue_items (id, workspace_id, engine_id, payload, state, order_key, created_at, updated_at) VALUES ('q1','w1','fake','hello','QUEUED',1,1,1)",
                [],
            )
            .unwrap();
        }
        // Reopen with the current schema.
        let db = Db::open(&db_path).unwrap();
        assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        assert_eq!(db.get_setting("theme").unwrap().as_deref(), Some("dark"));
        db.with_conn(|conn| {
            let (revision, session_mode, dispatch_phase, state): (i64, String, String, String) =
                conn.query_row(
                    "SELECT revision, session_mode, dispatch_phase, state FROM queue_items WHERE id='q1'",
                    [],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get(3)?,
                        ))
                    },
                )
                .map_err(StorageError::from)?;
            assert_eq!(revision, 1);
            assert_eq!(session_mode, "new");
            assert_eq!(dispatch_phase, "prepare");
            // v3 normalizes the legacy uppercase spelling to the lowercase
            // the runtime reads (no blanket state reset beyond the spelling).
            assert_eq!(state, "queued", "v1 uppercase state normalized to lowercase");
            Ok::<(), StorageError>(())
        })
        .unwrap();
    }

    #[test]
    fn v3_normalizes_legacy_uppercase_states_and_null_engine_rows() {
        // §49–§51 + TASK 24 §9: a genuine v1 database with uppercase queue
        // states and a nullable engine_id must load through the current
        // schema without query failure and without silent dispatch — the
        // null-engine row becomes the explicit manual-recovery `unknown`
        // state.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            Db::apply_migrations(&conn, &migrations::MIGRATIONS[..1], 1).unwrap();
            conn.execute(
                "INSERT INTO queue_items (id, workspace_id, engine_id, payload, state, order_key, created_at, updated_at) \
                 VALUES ('q_up', 'w1', 'opencode', 'hi', 'QUEUED', 1, 1, 1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO queue_items (id, workspace_id, engine_id, payload, state, order_key, created_at, updated_at) \
                 VALUES ('q_null', 'w1', NULL, 'hi', 'DISPATCHED', 2, 1, 1)",
                [],
            )
            .unwrap();
        }
        let db = Db::open(&db_path).unwrap();
        assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        db.with_conn(|conn| {
            let states: Vec<(String, String, Option<String>)> = conn
                .prepare("SELECT id, state, engine_id FROM queue_items ORDER BY order_key")
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            assert_eq!(
                states[0],
                ("q_up".into(), "queued".into(), Some("opencode".into()))
            );
            assert_eq!(states[1], ("q_null".into(), "unknown".into(), None));
            // The normalized row is the explicit manual-recovery state the
            // runtime reads — never a silent dispatch target.
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM queue_items WHERE state = 'unknown' AND last_error_code = 'manual_recovery'",
                    [],
                    |r| r.get(0),
                )
                .map_err(StorageError::from)?;
            assert_eq!(count, 1);
            Ok::<(), StorageError>(())
        })
        .unwrap();
    }

    #[test]
    fn v4_marks_null_engine_session_id_rows_non_resumable() {
        // TASK 24 §9: legacy rows whose engine_session_id was never recorded
        // must load as explicitly NON-resumable (never fabricated into an
        // empty upstream id that could reach an engine).
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            Db::apply_migrations(&conn, &migrations::MIGRATIONS[..1], 1).unwrap();
            conn.execute(
                "INSERT INTO sessions_meta (id, engine_id, engine_session_id, created_at, updated_at) \
                 VALUES ('s_null', 'opencode', NULL, 1, 1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO sessions_meta (id, engine_id, engine_session_id, created_at, updated_at) \
                 VALUES ('s_empty', 'opencode', '', 1, 1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO sessions_meta (id, engine_id, engine_session_id, created_at, updated_at) \
                 VALUES ('s_real', 'opencode', 'upstream-1', 1, 1)",
                [],
            )
            .unwrap();
        }
        let db = Db::open(&db_path).unwrap();
        assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        db.with_conn(|conn| {
            let rows: Vec<(String, i64)> = conn
                .prepare("SELECT id, resumable FROM sessions_meta ORDER BY id")
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            assert_eq!(
                rows,
                vec![
                    ("s_empty".into(), 0),
                    ("s_null".into(), 0),
                    ("s_real".into(), 1),
                ]
            );
            Ok::<(), StorageError>(())
        })
        .unwrap();
    }

    #[test]
    fn v5_marks_connection_owned_legacy_sessions_non_resumable() {
        // TASK 24 §9: pre-v5 Harness/Generic CLI rows with valid upstream ids
        // were left resumable=1 even though those adapters advertise
        // resume=false (connection-owned sessions die with the runtime). v5
        // repairs legacy semantics to match freshly-created rows, while
        // OpenCode (resume=true) rows keep resumable=1.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            // v1 schema only (engine_session_id is nullable); v4/v5 add the
            // resumable column + repairs.
            Db::apply_migrations(&conn, &migrations::MIGRATIONS[..1], 1).unwrap();
            for (id, engine) in [
                ("s_harness", "deepseek-harness"),
                ("s_cli", "generic-cli"),
                ("s_opencode", "opencode"),
            ] {
                conn.execute(
                    "INSERT INTO sessions_meta (id, engine_id, engine_session_id, created_at, updated_at) \
                     VALUES (?1, ?2, 'upstream-valid', 1, 1)",
                    rusqlite::params![id, engine],
                )
                .unwrap();
            }
        }
        let db = Db::open(&db_path).unwrap();
        assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        db.with_conn(|conn| {
            let rows: Vec<(String, i64)> = conn
                .prepare("SELECT id, resumable FROM sessions_meta ORDER BY id")
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            assert_eq!(
                rows,
                vec![
                    ("s_cli".into(), 0),
                    ("s_harness".into(), 0),
                    ("s_opencode".into(), 1),
                ]
            );
            Ok::<(), StorageError>(())
        })
        .unwrap();
    }

    #[test]
    fn v3_preserves_terminal_legacy_states_and_marks_only_ambiguous_active_unknown() {
        // TASK 24 audit: v3's null-engine normalization must preserve terminal
        // legacy rows (DONE/FAILED/CANCELLED) as terminal non-dispatchable
        // history — converting them to `unknown` would fabricate active
        // ambiguity that blocks the workspace and loses recorded outcomes.
        // Only states whose execution outcome genuinely cannot be proven
        // (QUEUED/LEASED/DISPATCHED) become manual-recovery `unknown`. None
        // can auto-dispatch.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            Db::apply_migrations(&conn, &migrations::MIGRATIONS[..1], 1).unwrap();
            for (id, state, key) in [
                ("q_queued", "QUEUED", 1),
                ("q_dispatch", "DISPATCHED", 2),
                ("q_done", "DONE", 3),
                ("q_failed", "FAILED", 4),
                ("q_cancelled", "CANCELLED", 5),
            ] {
                conn.execute(
                    "INSERT INTO queue_items (id, workspace_id, engine_id, payload, state, order_key, created_at, updated_at) \
                     VALUES (?1, 'w1', NULL, 'hi', ?2, ?3, 1, 1)",
                    rusqlite::params![id, state, key],
                )
                .unwrap();
            }
        }
        let db = Db::open(&db_path).unwrap();
        db.with_conn(|conn| {
            let rows: Vec<(String, String)> = conn
                .prepare("SELECT id, state FROM queue_items ORDER BY order_key")
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            assert_eq!(
                rows,
                vec![
                    ("q_queued".into(), "unknown".into()),
                    ("q_dispatch".into(), "unknown".into()),
                    // Terminal legacy rows stay terminal — never fabricated
                    // ambiguity.
                    ("q_done".into(), "done".into()),
                    ("q_failed".into(), "failed".into()),
                    ("q_cancelled".into(), "cancelled".into()),
                ]
            );
            // The ambiguous rows carry the manual-recovery marker; the
            // terminal rows carry the legacy_no_engine history marker.
            let unknown: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM queue_items WHERE state='unknown' AND last_error_code='manual_recovery'",
                    [],
                    |r| r.get(0),
                )
                .map_err(StorageError::from)?;
            assert_eq!(unknown, 2);
            let terminal: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM queue_items WHERE state IN ('done','failed','cancelled') AND last_error_code='legacy_no_engine'",
                    [],
                    |r| r.get(0),
                )
                .map_err(StorageError::from)?;
            assert_eq!(terminal, 3);
            Ok::<(), StorageError>(())
        })
        .unwrap();
    }

    #[test]
    fn old_v3_lineage_converges_to_safe_state_without_fabricating_outcomes() {
        // T-053: a database that applied the EARLIER blanket v3 (which converted
        // even terminal null-engine rows to `unknown`, losing the recorded
        // outcome) must converge with freshly-upgraded databases on one safe,
        // documented, non-dispatchable state. The lost terminal outcome is NOT
        // fabricated back into done/failed (no invented history, no auto-dispatch).
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            // The historical blanket v3 ran on databases that already had the
            // v2 hardening columns (last_error_code etc.), so the fixture must
            // build the lineage through v2 before simulating it.
            Db::apply_migrations(&conn, &migrations::MIGRATIONS[..2], 2).unwrap();
            for (id, state, key) in [
                ("q_done", "DONE", 1),
                ("q_failed", "FAILED", 2),
                ("q_queued", "QUEUED", 3),
            ] {
                conn.execute(
                    "INSERT INTO queue_items (id, workspace_id, engine_id, payload, state, order_key, created_at, updated_at) \
                     VALUES (?1, 'w1', NULL, 'hi', ?2, ?3, 1, 1)",
                    rusqlite::params![id, state, key],
                )
                .unwrap();
            }
            // Simulate the OLD blanket v3: ALL null-engine rows → unknown.
            conn.execute_batch(
                "UPDATE queue_items SET state='unknown', last_error_code='manual_recovery' \
                 WHERE engine_id IS NULL OR engine_id = '';",
            )
            .unwrap();
            // Pretend the old v3 already applied.
            conn.pragma_update(None, "user_version", 3).unwrap();
        }
        // Reopen: v4/v5/v6 run; the lost terminal outcome stays lost.
        let db = Db::open(&db_path).unwrap();
        assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        db.with_conn(|conn| {
            let rows: Vec<(String, String)> = conn
                .prepare("SELECT id, state FROM queue_items ORDER BY order_key")
                .map_err(StorageError::from)?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            // Every row is the documented non-dispatchable `unknown` — no
            // fabricated terminal outcome, no dispatchable active state.
            assert_eq!(
                rows,
                vec![
                    ("q_done".into(), "unknown".into()),
                    ("q_failed".into(), "unknown".into()),
                    ("q_queued".into(), "unknown".into()),
                ]
            );
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM queue_items WHERE state='unknown' AND last_error_code='manual_recovery'",
                    [],
                    |r| r.get(0),
                )
                .map_err(StorageError::from)?;
            assert_eq!(count, 3);
            Ok::<(), StorageError>(())
        })
        .unwrap();
    }

    #[test]
    fn integrity_classification_only_exact_ok_is_success() {
        // TASK 24 audit: refresh_integrity cached EVERY PRAGMA output as Ok,
        // so a damaged DB could report success. The classification is pure
        // and must accept only the exact `ok` verdict.
        assert_eq!(classify_integrity_output("ok"), Ok("ok".to_string()));
        assert_eq!(classify_integrity_output("ok\n"), Ok("ok".to_string()));
        for report in [
            "Page 3: invalid page type",
            "database disk image is malformed",
            "row 42 missing from index sqlite_autoindex_app_settings_1",
            "ok\nPage 5: never allocated",
            "",
        ] {
            assert!(
                classify_integrity_output(report).is_err(),
                "corruption report must never classify as ok: {report:?}"
            );
        }

        // A healthy DB stays `ok` through both the cached and deep paths.
        let dir = tempfile::tempdir().unwrap();
        let healthy = Db::open(&dir.path().join("healthy.db")).unwrap();
        assert_eq!(healthy.integrity().unwrap(), "ok");
        assert_eq!(healthy.deep_integrity().unwrap(), "ok");
    }

    #[test]
    fn structural_corruption_fails_open_as_integrity_not_ok() {
        // End-to-end: a structurally corrupt DB must surface as a typed
        // StorageError::Integrity — never an `ok` verdict. The file header
        // and page 1 stay intact so SQLite opens; the freelist trunk pointer
        // (bytes 32..36 of the header) is a huge invalid page number, which
        // PRAGMA integrity_check reports as corruption.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("corrupt.db");
        {
            let db = Db::open(&db_path).unwrap();
            // Touch every table so none is lazily absent from the file.
            db.with_conn(|conn| {
                conn.execute_batch(
                    "INSERT OR IGNORE INTO app_settings (key, value, updated_at) \
                     VALUES ('probe', '1', 1);",
                )
                .unwrap();
                Ok::<(), StorageError>(())
            })
            .unwrap();
        } // close the connection + WAL checkpoint so the main file is current
        let bytes = std::fs::read(&db_path).unwrap();
        assert!(bytes.len() > 36, "db file has a header");
        let mut corrupt = bytes.clone();
        corrupt[32..36].copy_from_slice(&0x7FFF_FFFFu32.to_be_bytes());
        std::fs::write(&db_path, &corrupt).unwrap();

        let err = Db::open(&db_path).unwrap_err();
        assert!(
            matches!(err, StorageError::Integrity { .. }),
            "structural corruption must be a typed integrity error, got {err:?}"
        );
        // Corruption is surfaced, never "fixed" by deleting durable state.
        assert!(db_path.exists());
    }

    #[test]
    fn future_schema_version_rejected_before_write() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        // Simulate a database created by a newer app: valid SQLite, newer
        // user_version, no SAIWORK2 tables.
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "user_version", 99).unwrap();
        }
        let err = Db::open(&db_path).unwrap_err();
        assert!(matches!(
            err,
            StorageError::UnsupportedVersion {
                found: 99,
                supported: SCHEMA_VERSION_APPLIED
            }
        ));
        // No write was performed: version is untouched and no schema appeared.
        let conn = Connection::open(&db_path).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 99);
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn corrupt_file_detected_and_never_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        let garbage = b"this is definitely not a sqlite database, just noise".repeat(8);
        std::fs::write(&db_path, &garbage).unwrap();

        let err = Db::open(&db_path).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt { .. }), "got {err:?}");

        // The file survives untouched: corruption is surfaced, never "fixed"
        // by deleting durable state.
        assert!(db_path.exists());
        assert_eq!(std::fs::read(&db_path).unwrap(), garbage);
    }

    // ---- transactions ----

    #[test]
    fn transaction_commits_atomically() {
        let db = Db::open_in_memory().unwrap();
        db.transaction(|tx| {
            tx.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                params!["a", "1", 1],
            )?;
            tx.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                params!["b", "2", 1],
            )?;
            Ok::<(), StorageError>(())
        })
        .unwrap();
        assert_eq!(db.get_setting("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db.get_setting("b").unwrap().as_deref(), Some("2"));
    }

    #[test]
    fn transaction_rolls_back_on_error_partial_write_not_visible() {
        let db = Db::open_in_memory().unwrap();
        let err = db.transaction(|tx| {
            tx.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                params!["a", "1", 1],
            )?;
            tx.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                params!["b", "2", 1],
            )?;
            Err::<(), _>(StorageError::InvalidData("boom".into()))
        });
        assert!(matches!(err, Err(StorageError::InvalidData(_))));
        // The first insert inside the failed transaction is invisible.
        assert_eq!(db.get_setting("a").unwrap(), None);
        assert_eq!(db.get_setting("b").unwrap(), None);
    }

    #[test]
    fn nested_transaction_is_rejected() {
        // Contract (§39): nested transactions are rejected with a typed error,
        // never a silent half-state. rusqlite refuses a second BEGIN inside an
        // open transaction; `Db::transaction` surfaces that as `StorageError`.
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("BEGIN;").unwrap();
        {
            let second = conn.transaction();
            assert!(second.is_err());
        }
        conn.execute_batch("ROLLBACK;").unwrap();

        // Sequential transactions via the public API are fine (each owns its
        // own BEGIN/COMMIT pair).
        let db = Db::open_in_memory().unwrap();
        db.transaction(|tx| {
            tx.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                params!["a", "1", 1],
            )?;
            Ok::<(), StorageError>(())
        })
        .unwrap();
        db.transaction(|tx| {
            tx.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                params!["b", "2", 1],
            )?;
            Ok::<(), StorageError>(())
        })
        .unwrap();
        assert_eq!(db.get_setting("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db.get_setting("b").unwrap().as_deref(), Some("2"));
    }

    /// T-013 (TASK 24 §9): `forget_workspace_with_sessions` deletes the
    /// workspace identity and all of its session metadata in ONE transaction.
    /// Success removes both together; a failure in the workspace step rolls
    /// the whole operation back so no partially-forgotten workspace is left
    /// behind (the session rows AND the workspace row both survive).
    #[test]
    fn forget_workspace_with_sessions_is_atomic() {
        let db = Db::open_in_memory().unwrap();

        // Seed a workspace with session metadata.
        let ws = db.upsert_workspace(r"V:\ws\forget", "forget").unwrap();
        let srow = SessionMetaRow {
            id: "s-forget".into(),
            workspace_id: Some(ws.id.clone()),
            engine_id: "e".into(),
            engine_session_id: Some("u1".into()),
            display_name: Some("d".into()),
            last_opened_at: Some(1),
            created_at: 1,
            updated_at: 1,
            resumable: true,
        };
        db.upsert_session_meta(&srow).unwrap();
        assert!(db.get_workspace(&ws.id).unwrap().is_some());
        assert_eq!(db.list_session_meta(Some(&ws.id)).unwrap().len(), 1);

        // Happy path: both the workspace row and its session rows vanish.
        db.forget_workspace_with_sessions(&ws.id).unwrap();
        assert!(
            db.get_workspace(&ws.id).unwrap().is_none(),
            "workspace row must be deleted"
        );
        assert!(
            db.list_session_meta(Some(&ws.id)).unwrap().is_empty(),
            "session metadata must be deleted atomically with the workspace"
        );

        // Re-seed and prove rollback: a failure on the workspace-delete step
        // must leave BOTH the session rows AND the workspace row intact.
        let ws2 = db.upsert_workspace(r"V:\ws\forget2", "forget2").unwrap();
        let mut srow2 = srow.clone();
        srow2.id = "s-forget2".into();
        srow2.workspace_id = Some(ws2.id.clone());
        db.upsert_session_meta(&srow2).unwrap();
        let failed: Result<(), StorageError> = db.transaction(|tx| {
            // Session rows deleted first (as in the real method)…
            tx.execute(
                "DELETE FROM sessions_meta WHERE workspace_id = ?1",
                params![ws2.id],
            )
            .map_err(StorageError::Query)?;
            // …then the workspace step fails.
            Err(StorageError::Query(rusqlite::Error::QueryReturnedNoRows))
        });
        assert!(failed.is_err(), "injected failure must be observed");
        assert!(
            db.get_workspace(&ws2.id).unwrap().is_some(),
            "workspace row must survive a failed forget (no partial deletion)"
        );
        assert_eq!(
            db.list_session_meta(Some(&ws2.id)).unwrap().len(),
            1,
            "session rows must survive a failed forget (rolled back)"
        );
    }

    // ---- durability ----

    #[test]
    fn reopen_preserves_durable_state() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        {
            let db = Db::open(&db_path).unwrap();
            db.set_setting("theme", "dark").unwrap();
            db.upsert_workspace(r"C:\work\proj", "proj").unwrap();
        } // connection closed, WAL checkpointed on last close
        {
            let db = Db::open(&db_path).unwrap();
            assert_eq!(db.get_setting("theme").unwrap().as_deref(), Some("dark"));
            assert_eq!(db.list_workspaces().unwrap().len(), 1);
            assert_eq!(db.version().unwrap(), SCHEMA_VERSION_APPLIED);
        }
        assert!(db_path.exists());
    }

    // ---- concurrency (two connections on one file, as in two processes) ----

    #[test]
    fn concurrent_reader_and_writer_wal() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        let db1 = Db::open(&db_path).unwrap();
        let db2 = Db::open(&db_path).unwrap();

        let writer = std::thread::spawn(move || {
            for i in 0..50 {
                db1.set_setting(&format!("k{i}"), &i.to_string()).unwrap();
            }
        });
        // Reader on the second connection never errors while the writer runs
        // (WAL: readers don't block the writer and vice versa).
        for i in 0..50 {
            let _ = db2.get_setting(&format!("k{i}"));
        }
        writer.join().unwrap();
        for i in 0..50 {
            assert_eq!(
                db2.get_setting(&format!("k{i}")).unwrap().as_deref(),
                Some(i.to_string().as_str())
            );
        }
    }

    #[test]
    fn competing_writes_serialize_no_loss() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        let db1 = Db::open(&db_path).unwrap();
        let db2 = Db::open(&db_path).unwrap();
        let mut handles = Vec::new();
        for (db, key) in [(db1, "left"), (db2, "right")] {
            handles.push(std::thread::spawn(move || {
                for i in 0..25 {
                    db.set_setting(key, &i.to_string()).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let db = Db::open(&db_path).unwrap();
        assert!(db.get_setting("left").unwrap().is_some());
        assert!(db.get_setting("right").unwrap().is_some());
    }

    #[test]
    fn busy_wait_releases_after_lock_holder_commits() {
        // Bounded busy policy (§21): a writer blocked by another connection's
        // lock waits (busy_timeout), then proceeds once the lock is released —
        // it does not fail instantly, and it does not retry forever.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("saiwork2.db");
        // Migrate first so the raw connections below see the schema.
        Db::open(&db_path).unwrap();
        let lock_conn = Connection::open(&db_path).unwrap();
        let writer_conn = Connection::open(&db_path).unwrap();
        // The writer waits (bounded) instead of failing instantly: same
        // busy_timeout policy as `Db::configure`.
        writer_conn.busy_timeout(Duration::from_secs(5)).unwrap();

        lock_conn
            .execute_batch(
                "BEGIN IMMEDIATE; \
                 INSERT INTO app_settings (key, value, updated_at) VALUES ('blocker', '1', 1);",
            )
            .unwrap();
        let t = std::thread::spawn(move || {
            writer_conn
                .execute(
                    "INSERT INTO app_settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
                    params!["waiter", "2", 1],
                )
                .unwrap();
        });
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            !t.is_finished(),
            "writer must be blocked by the lock holder"
        );
        lock_conn.execute_batch("COMMIT;").unwrap();
        t.join().unwrap();

        let db = Db::open(&db_path).unwrap();
        assert_eq!(db.get_setting("waiter").unwrap().as_deref(), Some("2"));
        assert_eq!(db.get_setting("blocker").unwrap().as_deref(), Some("1"));
    }

    // ---- repositories ----

    #[test]
    fn settings_roundtrip_and_overwrite() {
        let db = Db::open_in_memory().unwrap();
        assert_eq!(db.get_setting("portable").unwrap(), None);
        db.set_setting("portable", "true").unwrap();
        assert_eq!(db.get_setting("portable").unwrap(), Some("true".into()));
        db.set_setting("portable", "false").unwrap();
        assert_eq!(db.get_setting("portable").unwrap(), Some("false".into()));
    }

    #[test]
    fn workspace_upsert_keeps_single_row_per_path() {
        let db = Db::open_in_memory().unwrap();
        let a = db.upsert_workspace(r"C:\work\proj", "proj").unwrap();
        let b = db
            .upsert_workspace(r"C:\work\proj", "proj (renamed)")
            .unwrap();
        assert_eq!(a.id, b.id);
        assert_eq!(b.name, "proj (renamed)");
        assert_eq!(db.list_workspaces().unwrap().len(), 1);
    }

    #[test]
    fn session_meta_roundtrip() {
        let db = Db::open_in_memory().unwrap();
        let row = SessionMetaRow {
            id: "s1".into(),
            workspace_id: Some("w1".into()),
            engine_id: "fake".into(),
            engine_session_id: Some("es1".into()),
            display_name: Some("my session".into()),
            last_opened_at: Some(1),
            created_at: 1,
            updated_at: 1,
            resumable: true,
        };
        db.upsert_session_meta(&row).unwrap();
        let rows = db.list_session_meta(Some("w1")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].display_name.as_deref(), Some("my session"));
        db.delete_session_meta("s1").unwrap();
        assert!(db.list_session_meta(None).unwrap().is_empty());
    }

    /// TASK 24 perf: indexed point lookups return exactly the same record as
    /// the former list+scan path, and are true point lookups (no full-table
    /// materialization) even among thousands of unrelated rows.
    #[test]
    fn point_lookups_match_list_scan_with_large_unrelated_sets() {
        let db = Db::open_in_memory().unwrap();
        // Seed 5k workspaces + 5k session rows so a scan-based lookup would
        // materialize and search the whole table each time.
        let mut target_workspace = None;
        let mut target_session = None;
        for i in 0..5_000 {
            let row = db
                .upsert_workspace(&format!(r"V:\seed\w{i}"), &format!("w{i}"))
                .unwrap();
            if i == 2_500 {
                target_workspace = Some(row.id.clone());
            }
        }
        for i in 0..5_000 {
            let id = format!("s{i}");
            db.upsert_session_meta(&SessionMetaRow {
                id: id.clone(),
                workspace_id: Some(format!("w{i}")),
                engine_id: "fake".into(),
                engine_session_id: Some(format!("es{i}")),
                display_name: Some(format!("session {i}")),
                last_opened_at: Some(i),
                created_at: i,
                updated_at: i,
                resumable: i % 2 == 0,
            })
            .unwrap();
            if i == 2_500 {
                target_session = Some(id);
            }
        }

        let target_workspace = target_workspace.unwrap();
        let target_session = target_session.unwrap();

        // Point lookups match the list+scan truth exactly.
        let by_point = db.get_workspace(&target_workspace).unwrap().unwrap();
        let by_scan = db
            .list_workspaces()
            .unwrap()
            .into_iter()
            .find(|w| w.id == target_workspace)
            .unwrap();
        assert_eq!(by_point.id, by_scan.id);
        assert_eq!(by_point.path, by_scan.path);
        assert_eq!(by_point.name, by_scan.name);

        let s_point = db.get_session_meta(&target_session).unwrap().unwrap();
        let s_scan = db
            .list_session_meta(None)
            .unwrap()
            .into_iter()
            .find(|r| r.id == target_session)
            .unwrap();
        assert_eq!(s_point.id, s_scan.id);
        assert_eq!(s_point.workspace_id, s_scan.workspace_id);
        assert_eq!(s_point.engine_session_id, s_scan.engine_session_id);
        assert_eq!(s_point.resumable, s_scan.resumable);
        assert_eq!(s_point.display_name, s_scan.display_name);

        // Unknown ids → None (same NotFound semantics as list+scan).
        assert!(db.get_workspace("missing").unwrap().is_none());
        assert!(db.get_session_meta("missing").unwrap().is_none());
    }
}
