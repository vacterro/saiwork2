//! Schema migrations, applied transactionally and tracked by `user_version`.
//!
//! Rules (STORAGE.md):
//! - append-only: never edit an applied migration, add a new one;
//! - each migration runs inside one transaction;
//! - migrations are idempotent-by-version: `user_version` decides what runs.

/// Migration 1 — SAIWORK2 application state.
///
/// Covers phase 0 needs (settings, workspaces, engine profiles, session
/// metadata, run metadata) plus the durable queue table whose manager lands
/// in phase 2. Schema follows KNOWLEDGE/STORAGE.md and KNOWLEDGE/QUEUE.md.
pub const MIGRATIONS: &[&str] = &[
    // v1
    r#"
    CREATE TABLE app_settings (
        key         TEXT PRIMARY KEY,
        value       TEXT NOT NULL,
        updated_at  INTEGER NOT NULL
    );

    CREATE TABLE workspaces (
        id              TEXT PRIMARY KEY,
        path            TEXT NOT NULL UNIQUE,
        name            TEXT NOT NULL,
        last_opened_at  INTEGER,
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL
    );

    CREATE TABLE engine_profiles (
        id          TEXT PRIMARY KEY,
        engine_id   TEXT NOT NULL,
        name        TEXT NOT NULL,
        settings    TEXT NOT NULL DEFAULT '{}',
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
    );

    CREATE TABLE sessions_meta (
        id                TEXT PRIMARY KEY,
        workspace_id      TEXT,
        engine_id         TEXT NOT NULL,
        engine_session_id TEXT,
        display_name      TEXT,
        last_opened_at    INTEGER,
        created_at        INTEGER NOT NULL,
        updated_at        INTEGER NOT NULL
    );

    -- Durable queue items (KNOWLEDGE/QUEUE.md). Manager implementation: phase 2.
    CREATE TABLE queue_items (
        id                TEXT PRIMARY KEY,
        workspace_id      TEXT NOT NULL,
        session_id        TEXT,
        engine_id         TEXT,
        payload           TEXT NOT NULL,
        state             TEXT NOT NULL,          -- QUEUED|LEASED|DISPATCHED|DONE|FAILED
        order_key         INTEGER NOT NULL,
        lease_id          TEXT,
        leased_at         INTEGER,
        lease_expires_at  INTEGER,
        attempt_count     INTEGER NOT NULL DEFAULT 0,
        created_at        INTEGER NOT NULL,
        updated_at        INTEGER NOT NULL,
        last_error        TEXT
    );

    CREATE INDEX idx_queue_items_state_order
        ON queue_items (state, order_key);

    CREATE TABLE run_meta (
        id           TEXT PRIMARY KEY,
        workspace_id TEXT,
        engine_id    TEXT,
        session_id   TEXT,
        status       TEXT NOT NULL,               -- RUNNING|COMPLETED|FAILED|CANCELED
        error        TEXT,
        started_at   INTEGER NOT NULL,
        ended_at     INTEGER
    );
    "#,
    // v2 — durable queue hardening (TASK 13). The v1 queue_items table was
    // the phase-2 placeholder; this migration adds the fields the QueueManager
    // needs for CAS edits, run correlation and crash-window recovery.
    //   revision          monotonic CAS counter for edit/reorder/delete/retry
    //   run_id            engine-accepted run identity (dispatch correlation)
    //   model             canonical model id, or NULL = UseEngineDefault
    //   session_mode      'new' | 'existing' — explicit target semantics
    //   dispatch_phase    'prepare' (no external side effect yet) | 'sending'
    //                     (engine may have accepted the send) — the crash-
    //                     window discriminator for LEASED recovery
    //   last_error_code   typed failure category (bounded, user-safe)
    r#"
    ALTER TABLE queue_items ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE queue_items ADD COLUMN run_id TEXT;
    ALTER TABLE queue_items ADD COLUMN model TEXT;
    ALTER TABLE queue_items ADD COLUMN session_mode TEXT NOT NULL DEFAULT 'new';
    ALTER TABLE queue_items ADD COLUMN dispatch_phase TEXT NOT NULL DEFAULT 'prepare';
    ALTER TABLE queue_items ADD COLUMN last_error_code TEXT;
    ALTER TABLE queue_items ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0;

    CREATE INDEX idx_queue_items_run_id
        ON queue_items (run_id) WHERE run_id IS NOT NULL;
    "#,
    // v3 — normalize legacy queue rows (TASK 24 §9). v1 documented uppercase
    // state spellings and a nullable engine_id, while the current runtime
    // reads lowercase states and requires a real engine target. This
    // migration normalizes known legacy spellings in place and converts rows
    // lacking a trustworthy engine target into the explicit non-dispatchable
    // `unknown` (manual-recovery) state — never inventing an engine, never
    // silently dispatching with an empty target.
    //
    // Terminal legacy rows (DONE/FAILED/CANCELLED) REMAIN TERMINAL: their
    // outcome is a recorded fact, not ambiguity, and converting them to
    // `unknown` would fabricate active ambiguity that blocks the workspace
    // and loses the history (TASK 24 audit). Only states whose execution
    // outcome genuinely cannot be proven (QUEUED/LEASED/DISPATCHED — possibly
    // dispatched, never proven done) become manual-recovery `unknown`.
    // Append-only: v1/v2 are never edited. (Databases that already applied
    // the earlier blanket v3 text keep rows in the safe non-dispatchable
    // `unknown` state; nothing there can auto-dispatch.)
    r#"
    UPDATE queue_items
      SET state = lower(state)
      WHERE state IN ('QUEUED','LEASED','DISPATCHED','DONE','FAILED','CANCELLED');

    -- Terminal history with a null engine: preserved as terminal,
    -- non-dispatchable, with the reason recorded.
    UPDATE queue_items
      SET last_error_code = 'legacy_no_engine',
          last_error = 'legacy terminal row without a trustworthy engine target: preserved as terminal history',
          dispatch_phase = 'prepare',
          cancel_requested = 0
      WHERE (engine_id IS NULL OR engine_id = '')
        AND lower(state) IN ('done', 'failed', 'cancelled');

    -- Ambiguous active rows (never proven terminal): explicit manual
    -- recovery — blocked, never auto-dispatched.
    UPDATE queue_items
      SET state = 'unknown',
          last_error_code = 'manual_recovery',
          last_error = 'legacy row without a trustworthy engine target: converted to manual recovery (resolved explicitly)',
          dispatch_phase = 'prepare',
          cancel_requested = 0
      WHERE (engine_id IS NULL OR engine_id = '')
        AND lower(state) NOT IN ('done', 'failed', 'cancelled');
    "#,
    // v4 — session resumability (TASK 24 §9). `engine_session_id` was
    // nullable in v1 while the current runtime requires a nonempty upstream
    // id for engine calls; legacy NULL rows must never be fabricated into
    // `""` and sent to an engine. This migration marks every row whose
    // upstream id is missing/empty as explicitly NON-resumable: the session
    // stays listed as historical metadata but every send/queue path rejects
    // it with a typed error — no invented upstream id, no engine call with an
    // empty id. New sessions always write a real engine_session_id and are
    // resumable by construction.
    r#"
    ALTER TABLE sessions_meta ADD COLUMN resumable INTEGER NOT NULL DEFAULT 1;
    UPDATE sessions_meta SET resumable = 0
      WHERE engine_session_id IS NULL OR engine_session_id = '';
    "#,
    // v5 — connection/runtime-owned sessions are NOT restart-resumable (TASK
    // 24 §9). v4 marked only rows lacking an upstream id non-resumable, but
    // pre-migration Harness/Generic CLI rows with nonempty ids were left
    // `resumable=1` even though those adapters advertise `resume=false`:
    // their sessions are connection-owned and die with the runtime. A fresh
    // equivalent row created today is `resumable=0`, so this migration makes
    // legacy rows match current behavior. OpenCode (`resume=true`) rows are
    // preserved. Append-only: v4 is never edited.
    r#"
    UPDATE sessions_meta SET resumable = 0
      WHERE engine_id IN ('deepseek-harness', 'generic-cli')
        AND engine_session_id IS NOT NULL
        AND engine_session_id != '';
    "#,
    // v6 — corrective reconciliation (T-053). The actually-shipped v1..v5
    // history is FROZEN: never edit an applied migration. This migration only
    // asserts safe, idempotent invariants without fabricating any outcome.
    //
    // A database that applied an EARLIER blanket v3 (which converted terminal
    // null-engine rows to `unknown`, losing the recorded terminal outcome)
    // CANNOT recover that lost fact — v6 deliberately does NOT invent terminal
    // results. It only guarantees that every `unknown` row is explicitly
    // marked manual-recovery (non-dispatchable), so no row is ever left in an
    // undocumented ambiguous state. Rerunning migrations is idempotent: the
    // `unknown` rows of both the old-v3 lineage and freshly-upgraded databases
    // converge on the same safe, documented, non-dispatchable state.
    r#"
    UPDATE queue_items
      SET last_error_code = 'manual_recovery'
      WHERE state = 'unknown'
        AND (last_error_code IS NULL OR last_error_code = '');
    "#,
    // v7 — queue query-plan indexes (PERF-004). Terminal-history snapshots and
    // enqueue order derivation must stay flat as terminal-row history grows; the
    // queries use `INDEXED BY` these indexes (the migration and the queries are
    // owned together and the predicates exactly match the partial definitions).
    // - `idx_queue_items_order_key`: order_key-leading index so
    //   `COALESCE(MAX(order_key),0)+1` resolves via the index instead of a full
    //   scan of the (state, order_key) composite.
    // - `idx_queue_items_terminal_updated`: partial index over terminal states
    //   keyed by `updated_at DESC`, so the newest-50 terminal snapshot avoids
    //   the temp B-tree ORDER BY. No rows are reinterpreted and no history is
    //   pruned — retention/count semantics are unchanged.
    r#"
    CREATE INDEX idx_queue_items_order_key
        ON queue_items (order_key);
    CREATE INDEX idx_queue_items_terminal_updated
        ON queue_items (updated_at DESC)
        WHERE state IN ('done','failed','cancelled');
    "#,
    // v8 — bounded queue dispatch keyset scan (AUDIT-PERF-001). The complete
    // deterministic ordering tuple lets each fixed-size candidate page seek
    // directly after its predecessor; no OFFSET walk and no full remaining-
    // queue materialization on every claimed item.
    r#"
    CREATE INDEX idx_queue_items_dispatch_keyset
        ON queue_items (state, order_key, created_at, id);
    "#,
];
