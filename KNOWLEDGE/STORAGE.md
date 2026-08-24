# STORAGE.md

SQLite stores only data **owned by SAIWORK2**.

## Stored

```text
workspaces              id, canonical path, name, timestamps
workspace settings      per-workspace preferences
engine profiles         engine id, display name, settings JSON
sessions metadata       engine id, engine session id, workspace, display meta
queue items             durable queue rows (QUEUE.md)
run metadata            run id, status, timestamps, error ref
application settings    key/value app settings
window/layout state     persisted UI layout
diagnostics metadata    bounded, retention-capped
recent projects         recency ordering
```

## Never stored

```text
project source files            (law 16)
.saipen authoritative files     (law 2)
external provider credential stores (law 14)
OpenCode internal sessions      (engine owns them)
Git repository state            (git owns it)
```

TASK 14 adds a hard rule: **no SAIPEN mirror tables** — `saipen_state`,
`saipen_board`, `saipen_task` do not exist and must never be created as an
authoritative copy. Canonical SAIPEN state stays on the filesystem; the
`SaipenService` cache is an in-memory projection (marked stale on read
failure), never durable (§7, §166, §230).

Principle: *store references and SAIWORK2-owned metadata; do not mirror
external authority* (law 25).

## SQLite requirements

- schema migrations via `user_version` runner, each migration transactional;
- WAL mode (with `busy_timeout`); explicit failure handling on open/migrate;
- startup integrity checks (`PRAGMA integrity_check` on open, reported to
  diagnostics, never silently ignored);
- indexes only after demonstrated need (law 11 / §15);
- bounded retention policies for diagnostics and run metadata (law 13);
- one writer authority: the core owns the connection; the UI never issues SQL
  (law 18).

## Storage failure contract (semantics, not platitudes)

| Failure | Required behavior |
| --- | --- |
| migration failure | startup aborts with a typed error surfaced in diagnostics; app does not run against a half-migrated schema |
| database unavailable (locked/corrupt/open error) | startup fails loudly (native dialog) if the DB cannot open; **queue dispatch stays disabled** until storage is usable — no in-memory-only fallback that could lose work |
| database corrupt | `PRAGMA integrity_check` failure is reported; corrupt queue rows are isolated and reported, never silently dropped |
| write failure (disk full, I/O) | mutation returns a typed STORAGE error; memory and events are NOT committed (donor: "no memory, revision, or event success" on write failure); user sees what failed and what remains |
| transaction rollback | all-or-nothing per mutation; no partial row states visible |
| locked database | `busy_timeout` (5 s) then a typed error; never an infinite wait, never a silent retry loop |
| partial startup recovery | recovery order is deterministic (QUEUE.md recovery sequence); dispatch never starts before recovery completes |
| WAL checkpoint failure at shutdown | recorded in diagnostics; WAL is crash-recoverable by design, so the DB remains consistent on next start |

## Schema versions

- **v1** (TASK 05): core tables — workspaces, engine profiles, sessions
  metadata, queue_items (base), run metadata, app settings, diagnostics,
  recent projects.
- **v2** (TASK 13): `queue_items` gains the durable queue contract columns
  `revision`, `model`, `session_mode`, `dispatch_phase`, `run_id`,
  `cancel_requested`, `last_error_code` plus a `run_id` index (dispatch
  correlation and CAS). Forward-only; a newer-DB version is rejected before
  any write.
- **v3** (TASK 24 §9): normalize legacy queue rows — lowercase states, null-
  engine active rows → non-dispatchable `unknown` (manual_recovery); terminal
  rows preserved. **History frozen**: an earlier blanket v3 text that converted
  terminal rows too is superseded and never re-applied (a DB at `user_version=3`
  skips v3). Lost terminal outcomes from that lineage are NOT reconstructed.
- **v4** (TASK 24 §9): `sessions_meta.resumable` — NULL/empty upstream ids marked
  non-resumable.
- **v5** (TASK 24 §9): connection/runtime-owned sessions (deepseek-harness,
  generic-cli) marked non-resumable to match `resume=false` adapters.
- **v6** (T-053): corrective reconciliation — freezes the v1..v5 history and
  only asserts a safe, idempotent invariant (every `unknown` queue row is
  explicitly `manual_recovery`, non-dispatchable). It never fabricates a
  terminal outcome lost to the old blanket v3. Rerunning migrations is
  idempotent; the documented current version (6) equals the runtime
  `SCHEMA_VERSION_APPLIED` at that release.
- **v7** (PERF-004): adds demonstrated query-plan indexes for order-key
  derivation and bounded terminal-history snapshots; no row semantics change.
- **v8** (AUDIT-PERF-001): adds
  `idx_queue_items_dispatch_keyset(state, order_key, created_at, id)` for
  fixed-size ordered candidate pages. This removes OFFSET/full-drain
  materialization and changes no durable row meaning. Current runtime
  `SCHEMA_VERSION_APPLIED` is 8.
- **TASK 23 adds no schema migration.** The `QueueState::Unknown` outcome is a
  new string value of the existing TEXT `state` column (no DDL change), and
  Harness correlation reuses the existing `session_id` + `run_id` columns
  (ACP exposes no durable TurnId to persist). Existing OpenCode/Fake queue
  rows are unaffected; a pre-TASK-23 DB opens unchanged (verified by the
  existing upgrade test).

## Upgrade evidence (TASK 18 / T-053)

- `v1_to_current_upgrade_preserves_durable_rows` test: a real v1 database
  with settings + workspace + queued item reopens under the current schema
  — version advances to 8 and every row survives with the v2 defaults
  (revision 1, session_mode 'new', dispatch_phase 'prepare', state queued —
  no blanket state reset across upgrade).
- `old_v3_lineage_converges_to_safe_state_without_fabricating_outcomes` test: a
  DB that applied the EARLIER blanket v3 (terminal null-engine rows lost to
  `unknown`) converges with freshly-upgraded DBs on the same safe,
  documented, non-dispatchable `unknown` state — no invented terminal outcome.
- `failed_migration_rolls_back_fully`: mid-migration failure leaves
  `user_version` AND partial DDL rolled back (recoverable, never
  half-migrated).
- `future_schema_version_rejected_before_write`: a newer DB is refused with
  no write and no schema change (downgrade protection).
- `corrupt_file_detected_and_never_deleted`: corruption is surfaced, never
  "fixed" by deleting durable state.

## Layout (portable root)

```text
data/saiwork2.db            SQLite (WAL: -wal, -shm)
data/config/                app config (non-secret)
data/logs/                  bounded/rotated structured logs
data/cache/                 deletable anytime
data/runtime/               temp runtime state, cleaned at startup
```

See PORTABILITY.md for root resolution.

## Implementation (TASK 05, `saiwork-storage`)

- **Stack**: `rusqlite 0.32` with the `bundled` feature (SQLite compiled in —
  no native dependency on the machine; Windows-friendly). No ORM, no second
  SQLite wrapper, no async driver (ADR-013).
- **Connection model**: one `Connection` per `Db`, wrapped in
  `Arc<Mutex<…>>`. `Db` is cheaply clonable; every core service shares the
  same connection. There is no pool: a local desktop SQLite has one writer,
  and WAL lets readers proceed without blocking it. All operations are short
  and transactional.
- **Lifecycle owner**: `App::bootstrap` resolves the root, opens the DB,
  migrates, and runs the integrity check before any service starts. The UI
  never sees SQL or the connection (law 18). On shutdown the app runs a
  final `checkpoint()` (WAL flush, bounded) and a final integrity check
  before STOPPED; failures are recorded as shutdown warnings, never fatal
  (the WAL is crash-recoverable by design).
- **Schema versioning**: forward-only migrations tracked by SQLite
  `user_version` (no separate `schema_migrations` table — the pragma is the
  single source of truth). Each migration runs in its own transaction that
  includes the version bump; a failed migration rolls back fully (DDL
  included), leaving the DB at a known previous version. A database with a
  version newer than this binary is **rejected before any write**
  (`UnsupportedVersion`). Downgrades are not implemented (forward-only is
  the explicit decision).
- **Transactions**: `Db::transaction(f)` — commit on `Ok`, rollback on `Err`
  (and on panic via `Drop`). Nested transactions are rejected by SQLite with
  a typed error (documented; savepoints are the escape hatch if ever
  needed). Inside the closure use the provided handle, never other `Db`
  methods (the connection is locked).
- **Timestamps**: UTC epoch milliseconds as `INTEGER` everywhere (Rust
  `now_ms()` / SQL params) — one representation, no mixed formats.
- **Busy policy**: `busy_timeout(5s)`, then a typed `StorageError::Busy` —
  never an infinite retry, never an instant fail on a transient lock. Both
  `SQLITE_BUSY` and `SQLITE_LOCKED` map to `Busy`.
- **Corruption policy**: a non-SQLite file is detected at configure time and
  mapped to a typed `StorageError::Corrupt`. `PRAGMA integrity_check` runs at
  open; failure is a typed `Integrity` error. **The DB is never deleted or
  rewritten automatically** — corruption is surfaced, and durable-state
  features stay disabled until the user acts.
- **Workspace paths** are canonicalized by `WorkspaceManager` before they
  reach storage; `workspaces.path` is `UNIQUE`, so duplicate casing/relative
  forms of one path converge to one row.
- **Logging**: database open (path + schema version), each applied migration,
  and migration failure are traced; SQL and stored values are not.

## Tests (TASK 05)

`cargo test -p saiwork-storage` — 14 tests, all isolated in `tempfile`
directories or `:memory:` (never the developer's real data root):

```text
migrations_apply_and_are_idempotent      fresh DB → latest; reopen no-rerun
failed_migration_rolls_back_fully        one failing migration → version + DDL rolled back
future_schema_version_rejected_before_write  newer DB refused, no write performed
corrupt_file_detected_and_never_deleted  typed Corrupt error; file untouched
transaction_commits_atomically           multi-statement commit visible
transaction_rolls_back_on_error_partial_write_not_visible
nested_transaction_is_rejected           typed error; sequential transactions fine
reopen_preserves_durable_state           write → close → reopen → read
concurrent_reader_and_writer_wal         two connections, reader never errors
competing_writes_serialize_no_loss       two writers, both keys survive
busy_wait_releases_after_lock_holder_commits  bounded wait, then success
settings_roundtrip_and_overwrite         key/value app settings
workspace_upsert_keeps_single_row_per_path
session_meta_roundtrip
```

Plus `cargo test -p saiwork-core` (11 tests): data-root precedence, portable
flag, CWD independence, invalid override, layout creation.

## Checkpoint under external lock (TASK 09 §76, measured)

`PRAGMA wal_checkpoint(TRUNCATE)` does **not** run the busy handler: when
another connection holds the write lock (BEGIN IMMEDIATE), the checkpoint
fails **fast** with a typed `Busy` error rather than waiting. Measured in
`storage_busy_during_shutdown_is_bounded_and_clean` (desktop lifecycle
suite): shutdown under lock contention completes immediately, records the
storage failure as a warning (`completed_with_warnings` outcome — a coherent
result, never a hang or endless retry), still ends STOPPED, and the
pre-shutdown write survives via the WAL on reopen. Durability therefore
does not depend on the checkpoint succeeding: WAL recovery on next open is
the guarantee; the checkpoint is an optimization on the clean path.
