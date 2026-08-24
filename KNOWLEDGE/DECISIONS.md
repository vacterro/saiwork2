# DECISIONS.md — Architecture Decision Records

Only decisions that are actually taken live here. Each: Decision, Reason,
Alternatives rejected, Consequences, Status. New decisions append; a change
supersedes via a new ADR.

## ADR-001 — Greenfield instead of SAIWORK fork
Decision: SAIWORK2 is a greenfield repository. Old SAIWORK is a donor, never a
foundation.
Reason: Donor carries dual desktop shells, competing process ownership, and a
whole-file queue persistence model — preserving proven contracts without the
accidental architecture requires a clean slate (master spec §0).
Alternatives rejected: forking SAIWORK; reusing its server as the core.
Consequences: every subsystem is classified (MIGRATION_SAIWORK.md) before any
code; KNOWLEDGE is the carrier of donor lessons (law 24).
Status: ACCEPTED (TASK 01/02).

## ADR-002 — Tauri 2 / Rust / React architecture
Decision: One desktop runtime (Tauri 2), Rust core owning all system/runtime
resources, React/TS UI as a projection.
Reason: Engineering priorities (correctness → stability → recoverability)
require a single authority with deterministic teardown; a type-safe runtime
layer provides it (laws 4/5/9).
Alternatives rejected: Electron (TS process layer, no Rust core); dual shells
(donor, DROP); Node core under Tauri (two runtimes, split authority).
Consequences: Rust must be in the toolchain; Windows packaging is WebView2;
UI never owns processes, DB, or durable state.
Status: ACCEPTED (TASK 02/03).

## ADR-003 — OpenCode as first production engine integration
Decision: The first real engine is OpenCode (`opencode serve` as a supervised
child, loopback, dynamic port, generated local secret, readiness probe).
Freebuff and Antigravity come later and only as isolated adapters.
Reason: Master spec §10; Antigravity-specific integration must first be
checked against OpenCode's provider/auth capability (spec §22).
Alternatives rejected: Freebuff-first; Antigravity-first.
Consequences: the EngineAdapter boundary is proven by FakeEngine + OpenCode
before any further engine work (ROADMAP TASK 10–12).
Status: ACCEPTED (implementation TASK 10).

## ADR-004 — SAIPEN remains the canonical authority
Decision: SAIPEN owns SAIPEN state. SAIWORK2 is a client: detect, read,
watch, validate; mutations only through canonical writers, read-only first;
never a second SAIPEN state machine, writer, or runtime.
Reason: Law 2; donor shipped multiple SAIPEN surfaces and needed a structural
no-second-runtime guard (MIGRATION_SAIWORK §22/§25.8).
Alternatives rejected: SAIWORK2-owned `.saipen` writes; vendored protocol copy;
own validation.
Consequences: SAIPENBAR shows truth or UNKNOWN (law 59); writes are a later,
canonical-path-only phase (ROADMAP TASK 15).
Status: ACCEPTED (TASK 02/13/14).

## ADR-005 — SQLite for SAIWORK2-owned durable state
Decision: SQLite (WAL, transactional migrations) is the durable authority for
settings, workspace registry, session metadata, and the queue. The UI never
issues SQL.
Reason: Donor whole-file JSON persistence blocked the event loop and could not
express atomic per-item claims (landmine 3). SQLite gives row-level atomic
claims, crash-safe leases, deterministic recovery (QUEUE.md).
Alternatives rejected: JSON snapshot (donor); in-memory + dump; external
queue service.
Consequences: `saiwork-storage` owns the connection; failure contract in
STORAGE.md; queue manager lands on this schema (ROADMAP TASK 13).
Status: ACCEPTED (TASK 02/05).

## ADR-006 — Single ProcessSupervisor authority
Decision: One supervisor owns every SAIWORK2 child process: spawn, bounded
output, stop/kill, orphan prevention. Readiness is **not** part of this
contract — it belongs to the engine adapter layer (superseded element,
ADR-015).
Reason: Law 6; donor's per-workspace runtimes + orphan registry +
background-process manager overlapped (landmine 2).
Alternatives rejected: per-engine process owners; frontend ownership (law 4).
Consequences: engines request spawns through the supervisor; shutdown kills
all trees; 0 orphans is a gate (PROCESS_LIFECYCLE.md).
Status: ACCEPTED (TASK 06; readiness element superseded by ADR-015).

## ADR-014 — Windows process-tree ownership via Job Objects
Decision: Every supervised child on Windows is created
`CREATE_SUSPENDED | CREATE_NO_WINDOW` and assigned to its own Job Object
(`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`) before it runs, then resumed.
Force kill = `TerminateJobObject` (whole tree in one OS call); closing the
last job handle kills any survivor even on abnormal app exit. `taskkill /T`
remains only as the graceful (console-app) hint, never the kill contract.
Reason: TASK 06 §27–28 — the donor's `taskkill /T /F`-based cleanup is
fragile (parsing, races, console-only semantics). A Job Object is an
OS-level ownership primitive: descendants cannot escape it, pid reuse is
irrelevant, and crash safety is structural. Isolated in
`saiwork-process::platform` with all `unsafe` confined there.
Alternatives rejected: taskkill-only cleanup; per-process WinAPI kill
without job assignment (grandchild escape); no tree cleanup (orphan leak).
Consequences: force-kill is reliable and race-free; graceful may escalate
for non-console children; Unix uses process groups (SIGTERM/SIGKILL to the
group); verified by `killing_parent_tree_kills_descendants`.
Status: ACCEPTED (TASK 06).

## ADR-015 — Readiness is not a supervisor concern
Decision: The ProcessSupervisor owns the OS process state machine only
(`SPAWNING → RUNNING → STOPPING → EXITED/FAILED`). Readiness probes,
markers, and `engine.ready` are the engine adapter layer's job (TASK 07+).
Reason: TASK 06 §12/§44–45 — the scaffold's supervisor carried a `READY`
state and output-marker probes, conflating "process alive" with "engine
ready" (the exact donor landmine PROCESS_LIFECYCLE warns about). Removing
it keeps the supervisor boring, generic, and engine-agnostic.
Alternatives rejected: keeping readiness inside the supervisor (the
scaffold's design — couples generic process infra to engine semantics and
would turn it into a spawn+probe+port-scanner); a readiness hook "just in
case".
Consequences: engine adapters implement readiness themselves against
`ManagedProcess::stdout/stderr/exit`; regression P-01 (predicate-based, no
fixed-sleep) now applies to the engine layer where it belongs.
Status: ACCEPTED (TASK 06).

## ADR-007 — Engine adapter boundary
Decision: All engines implement one logical contract (`EngineAdapter`):
identity, capabilities, lifecycle, sessions, send/cancel, event subscription.
Engine-specific behavior stops at the boundary; UI builds on normalized
capabilities, never engine ids.
Reason: Law 3; donor's provider abstraction stack was the landmine 6.
Alternatives rejected: per-engine UI branching; one trait per engine;
unbounded capability vocabulary.
Consequences: capability set is canonical (ENGINE_CONTRACT.md); engine-specific
UI only inside isolated feature boundaries; new engines are adapters, not
app rewrites.
Status: ACCEPTED (TASK 04).

## ADR-008 — Event-driven over broad polling
Decision: No polling where a reliable event/watch mechanism exists. The only
exception is a bounded, documented backstop re-sweep where a platform
demonstrably drops events (Windows rapid renames), taken from donor lessons.
Decision: All application events flow through one normalized, bounded EventBus
(law 8/12/13).
Reason: Donors converged on watcher-driven design with a guarded backstop;
unbounded SSE buffering for slow clients was landmine 4.
Alternatives rejected: polling loops; unbounded event queues; per-consumer
raw streams.
Consequences: watchers are workspace-bound and disposable; slow consumers
reconcile instead of buffering (EVENTS.md).
Status: ACCEPTED (TASK 04/14).

## ADR-011 — EventBus delivery model (bounded broadcast, seq identity)
Decision: The EventBus is a bounded `tokio::sync::broadcast` channel.
`publish` is synchronous and non-blocking; subscribers hold polled
`Subscription` handles (no callback API). Each envelope carries a global
monotonic `seq` (per-run event identity), UTC-ms `ts`, and the canonical
`type` tag. Events are classified `State` / `Stream` / `Diagnostic`.
Reason: Producer/consumer decoupling with bounded memory (law 13); no global
lock held during delivery means reentrant publishes are safe and a failing
consumer cannot poison the bus. The seq makes lag explicit (`Lagged(n)`)
and gives the frontend a stable projection key (TASK 04 §14–21).
Alternatives rejected: callback/observer bus (lock-during-callback hazard,
consumer panic risk); unbounded channel (memory growth, law 13);
self-rolled dispatcher (duplicate of a maintained primitive, no benefit).
Consequences: slow consumers MUST reconcile from authoritative state — the
bus is not a history store and does not replay. `seq` resets per app run;
cross-run identity comes from authoritative subsystem state, never events.
Status: ACCEPTED (TASK 04).

## ADR-012 — Typed domain identifiers
Decision: Event payloads and domain boundaries use typed IDs
(`WorkspaceId`, `EngineId`, `SessionId`, `RunId`, `MessageId`,
`QueueItemId`, `RequestId`) — opaque `Arc<str>` newtypes in
`saiwork-events::id`. They serialize as plain strings, so the Rust↔TS wire
shape is unchanged; the UI contract stays string-shaped.
Reason: Cross-domain substitution (`WorkspaceId` used where `SessionId`
belongs) becomes a compile error, not a runtime surprise; IDs are cheap,
hashable, comparable, `Send + Sync`, and stable-textual (TASK 04 §4–5).
Alternatives rejected: raw `String` everywhere (no type-level separation);
UUID dependency (no requirement for UUID semantics — IDs are opaque by
construction and their form is the allocating authority's choice); DB
integers as global identity (rejected in advance, QUEUE/STORAGE).
Consequences: core authorities own ID allocation; the engine-facing
`SessionInfo.id`/`engine_session_id` stay `String` (engine contract is a
string boundary by design). Adding a new ID type is one `id_type!` macro
invocation.
Status: ACCEPTED (TASK 04).

## ADR-009 — FakeEngine is the first engine adapter
Decision: The first `EngineAdapter` implementation is the FakeEngine
(in-process, failure-simulating); it is permanent test infrastructure.
Reason: The architecture must be testable independently of any real provider
(spec §32); hostile matrix runs against it (TESTING.md).
Alternatives rejected: OpenCode-first with mocks; no fake.
Consequences: FakeEngine is a normal registry engine; M0 gate runs on it.
Status: ACCEPTED (TASK 07).

## ADR-010 — Deterministic portable data root
Decision: Data root resolution order: `SAIWORK2_DATA_DIR` → `portable.flag`
beside the exe → OS app-data dir. Exactly one writable root (law 15).
Reason: Portability must be deterministic and relocatable (PORTABILITY.md).
Alternatives rejected: heuristic root detection; multiple writable roots.
Consequences: `portable.flag` is the single marker; relocation preserves DB;
engine secrets are never auto-copied (law 14).
Status: ACCEPTED (TASK 03/05).

## ADR-013 — Storage implementation (rusqlite, single connection, user_version)
Decision: `saiwork-storage` uses `rusqlite 0.32` with the `bundled` feature,
one `Connection` per `Db` behind `Arc<Mutex<…>>` (no pool, no async
driver), forward-only migrations tracked by the `user_version` pragma, and
UTC-ms `INTEGER` timestamps. `Db::transaction(f)` is the public atomic
boundary (commit on `Ok`, rollback on `Err`/panic, nested rejected).
Reason: SQLite is already the chosen durable authority (ADR-005). A local
desktop DB has exactly one writer; a pool or an async driver would add
machinery without a real concurrency target, and WAL already lets readers
proceed during writes. The `user_version` pragma is the simplest single
source of schema truth; per-migration transactions make failures recoverable
by construction (TASK 05 §9–§24).
Alternatives rejected: `sqlx`/async stack (no async requirement — the UI is
never blocked by DB work because calls are short and happen in core
services, not the event loop); `diesel`/ORM (four tables, five queries —
no demonstrated benefit); a `schema_migrations` table (the pragma suffices);
downgrade migrations (forward-only is the explicit policy); JSON snapshot
persistence (donor landmine 3).
Consequences: `tempfile` is a dev-dependency only (isolated test DBs); the
connection is held by the core and shared by all services; corrupt/newer
DBs are rejected with typed errors before any write; the future queue
(TASK 13) gets its atomic boundary from `Db::transaction`.
Status: ACCEPTED (TASK 05).

## ADR-016 — windows-gnu linking: rust-lld + empty cdylib export table
Decision: The desktop crate links with `rust-lld` (set in `.cargo/config.toml`
for `x86_64-pc-windows-gnu`) and its cdylib is built with
`cargo::rustc-cdylib-link-arg=--exclude-all-symbols` (from `build.rs`), so the
cdylib exports **nothing**.
Reason: rustc passes `--export-all-symbols` for cdylibs; the Tauri dependency
graph then lands ~102k symbols in the export table, overflowing the PE
16-bit ordinal limit. GNU ld (bundled 2.42 and newer 2.46 alike) dies with
"export ordinal too large: 102793"; lld dies with "too many exported symbols
(got 102791, max 65535)". The desktop binary links the app lib statically
(main.rs → `saiwork2_lib::run`), so the cdylib needs no exports at all
(tauri-apps/tauri#10843 is the same failure, closed without a fix).
Alternatives rejected: keeping `--export-all-symbols` (ordinal overflow);
`--exclude-all-symbols` with GNU ld unverified + still exporting def symbols;
MSVC toolchain (not installed on the build machine); `taskkill`/linker hacks
at the crate level.
Consequences: gates (`cargo test --workspace`, `cargo build`) link the desktop
lib and exe; a full rebuild additionally needs `dlltool` on PATH (raw-dylib
crates — getrandom 0.3/0.4, windows 0.52/0.53) or the `DLLTOOL` env var;
this machine's canonical gate commands export the MinGW bin directory.
Status: ACCEPTED (TASK 07; unblocked the desktop link gate).

## ADR-017 — FakeEngine hostile input arrives at the raw-frame boundary
Decision: FakeEngine's normalized path is a normal `EngineAdapter`; hostile
input (duplicate, malformed, out-of-order, unknown events) is pushed through
`pushRaw` — the same raw-frame boundary a future provider transport would
feed — and is contained there (typed protocol error / bounded
`engine.raw_event` diagnostic), never as arbitrary invalid canonical events
on the bus.
Reason: TASK 07 §31–§34 — malformed simulation must sit where a real adapter
parses provider input, not by corrupting the typed EventBus. This proves the
adapter boundary discipline OpenCode will need, without making the bus
responsible for provider ordering.
Alternatives rejected: publishing malformed Rust events directly on the bus
(breaks the type system for a test's sake); a separate FakeEngine-only event
system (second event bus, forbidden).
Consequences: duplicate normalized events are tolerated by consumers (dedup
by run seq); out-of-order raw frames are rejected, not silently reordered;
unknown raw frames are ignored with a diagnostic. Cancellation semantics are
canonical: `message.cancelled` is a terminal event, exactly one terminal per
run, no semantic events after it (asserted by the `engine-fake` suite).
Status: ACCEPTED (TASK 07).

## ADR-018 — Single instance: OS process-level mutex via tauri-plugin-single-instance
Decision: single-instance authority is `tauri-plugin-single-instance`, whose
plugin initialization acquires a process-level OS mutex (Windows named
mutex) **before** the Tauri `setup` hook runs core bootstrap. A second
launch relays typed launch args (intent) to the primary, requests window
activation, and exits without ever opening the database.
Reason: TASK 08 §14–§19 — protect against two processes opening the same
`saiwork2.db`, two ProcessSupervisors and two UI authorities. The authority
must be process-level, not frontend-level; the mutex must precede storage
open (a secondary that briefly opens the DB would violate the single-owner
contract). OS-owned mutexes release automatically on crash, so a stale
primary cannot permanently block future launches.
Alternatives rejected: lock files (a file's existence is not proof of a
live process, §19); custom IPC protocol framework (§16 forbids generic IPC
infrastructure); frontend-only "is a window open" checks (§15).
Consequences: launch intents stay tiny typed arg vectors (no raw command
execution, §17); window-close and OS quit converge on `App::shutdown()`
(§22); rapid relaunch works because the mutex is released at process exit
(§68, smoke-verified).
Status: ACCEPTED (TASK 08).

## ADR-019 — Application lifecycle authority: App runtime in saiwork-core
Decision: `App` (saiwork-core) is the single owner of the application
lifecycle: an explicit state machine (BOOTING / READY / SHUTTING_DOWN /
STOPPED / FAILED) with validated transitions, one deterministic startup
order, one shutdown sequence, and a `require_ready()` command guard that
rejects work as `NotReady` or `ShuttingDown`. The frontend holds a read-only
projection; Tauri commands are the controlled boundary.
Reason: TASK 08 §4–§7 — one canonical startup path (not "Tauri setup does
some, React calls initialize, a command lazily opens the DB"), no
Schrödinger state from boolean combinations (§5), and no service scattered
across ten Tauri commands (§3). Restart = a new OS process; within one
process lifetime `STOPPED → READY` is impossible (§6).
Alternatives rejected: lazy per-command initialization (multiple startup
authorities); a `DEGRADED` state without a precise allowed/forbidden
matrix (§35 — fail closed instead); UI-side lifecycle truth (law 23).
Consequences: `App::shutdown()` is idempotent (double shutdown → one
sequence, both callers observe the terminal outcome); shutdown during boot
is supported defensively; startup/shutdown timings are recorded as
baseline facts (§50–§51).
Status: ACCEPTED (TASK 08).

## ADR-020 — Logging bootstrap: canonical logs dir + bounded rotation + fallback
Decision: `saiwork-core::logging` initializes structured logging to
`<data-root>/logs/saiwork2.YYYY-MM-DD.log` (tracing-appender, daily
rotation, retention bounds), with stderr as a fallback if the file sink
cannot be opened — logging failure is never fatal and is reported via
diagnostics (`log_fallback`). A panic hook records panic context to the
log. The shell init happens after data-root resolution and before core
bootstrap, so all startup activity is captured.
Reason: TASK 08 §40–§44 — logs belong to the canonical data root, never
CWD/source tree (§41); unbounded single-file growth is unacceptable (§42);
log failure is a different criticality class from storage failure (§43);
panics should leave diagnostics without promising recovery (§44).
Alternatives rejected: logging to CWD or the source tree (PORTABILITY law);
stderr-only (no persistent record for desktop users); killing the app when
logging fails (out of proportion).
Consequences: environment variables are never dumped to logs (§92 — no
`std::env::vars()` anywhere in bootstrap); diagnostics expose the log dir
and fallback flag read-only.
Status: ACCEPTED (TASK 08).

## ADR-021 — Startup failure policy: fail closed, never fake READY
Decision: required bootstrap failure (data-root resolution, storage open,
migration, integrity) aborts startup: the app never enters READY, no
engine starts, no child process starts, and the shell shows a concise
error dialog (reason + data path) before exiting. No DEGRADED mode, no
silent fallback to a temporary database, no pretending durable state is
available.
Reason: TASK 08 §10–§11, §35 — a required-service failure must not be
papered over; "is_ready: bool" games create Schrödinger states. The
storage failure contract (STORAGE.md) already says corruption is surfaced,
never auto-deleted; the lifecycle layer extends that to the whole
bootstrap.
Alternatives rejected: universal DEGRADED state (no precise allowed/
forbidden matrix exists yet); in-memory-only storage fallback (could lose
work, violates the storage contract); continuing to READY with a warning
(false readiness, §111).
Consequences: startup error messages carry stage/category/user-safe cause;
rollback on partial bootstrap failure is exercised by the lifecycle test
suite (opened resources closed, mutex released); the `Failed` state exists
so shutdown-from-failed is still well-defined.
Status: ACCEPTED (TASK 08).

## ADR-022 — Desktop lib unit-test harness disabled (upstream Tauri loader bug)

Problem: `cargo test --workspace` failed on Windows because the desktop
crate's **lib unit-test harness** could not start:
`0xC0000139 STATUS_ENTRYPOINT_NOT_FOUND`.

Root cause: the harness links the whole Tauri/WebView2 stack, which
hard-imports `WebView2Loader.dll` at process start. Verified forensically:
LoadLibrary binds the harness PE fine; CreateProcess fails; the harness's
import table is a strict subset of the working main exe's; the DLL exports
the one imported symbol; the failure persists with the DLL beside the exe.
This is the known, still-open upstream bug tauri-apps/tauri#14580 (a test
binary referencing tauri types fails to start on Windows), not a SAIWORK2
defect.

Decision: disable the desktop lib's unit-test harness (`test = false`,
`doctest = false`). The lib is pure Tauri wiring with zero unit tests; all
real tests are integration tests (`tests/lifecycle.rs`, 8 tests) plus
`saiwork-core`/`saiwork-events`/`saiwork-process` unit suites. `cargo test
--workspace` is fully green (124 tests).

Alternatives rejected: linking WebView2Loader differently (environment
fork, no canonical fix; upstream issue still open); moving tests into the
bin target (binaries are not testable); keeping the harness broken
(blocks the Phase 0 gate §101).

Consequences: a future desktop-lib unit test must be written as an
integration test instead; if the upstream bug is fixed, `test = true` can
be restored. Revisit when tauri fixes #14580.
Status: ACCEPTED (TASK 09).
## ADR-023 — QueueManager is the single durable queue authority (TASK 13)

Problem: queued work needs one owner. A React array + SQLite rows + a
dispatcher-local list would be three competing truths (law 5/7).

Decision: exactly one `QueueManager` in `saiwork-queue` owns the queue state
machine, the durable SQLite transitions, and the single dispatch worker.
SQLite is the durable truth; the UI holds a projection refreshed via
`queue_snapshot`; every mutation (enqueue/edit/reorder/cancel/retry/pause/
resume) is a typed QueueManager command. Direct TASK 11 user sends coexist,
but queued dispatch goes only through QueueManager, and session-busy
arbitration is coherent (queue waits on `session_busy`, never races a direct
send).

Consequences: one authority to reason about; UI is a pure projection; any
future parallelism (TASK 18) is a conscious extension of the single worker.
Status: ACCEPTED (TASK 13).

## ADR-024 — LEASED prepare/sending phases and ambiguous-handoff policy

Problem: the crash window between claiming an item and the engine accepting
the send is where prompts get lost or duplicated (donor Q-01/Q-06).

Decision: `LEASED` carries a durably committed `dispatch_phase` written
**before** the engine call: `prepare` (no external side effect — startup
recovery restores to QUEUED) and `sending` (send may have been accepted).
An item found in `sending` at startup is marked `FAILED(ambiguous_handoff)`
for manual review — never auto-re-dispatched. Exactly-once external engine
effects are not claimed (OpenCode has no idempotency key); local claim
exactly-once is guaranteed by the atomic claim and run_id-guarded terminal
transitions.

Consequences: no prompt loss on clean crash windows; no blind duplicate
dispatch on ambiguous windows; a small manual-review surface instead of
fabricated certainty. Status: ACCEPTED (TASK 13).

## ADR-025 — One event-driven dispatch worker with a bounded backstop

Problem: a dispatcher that polls `SELECT queue; sleep(100ms)` wastes idle
CPU and reacts slowly; a pure-notify design can lose a wakeup.

Decision: one dispatcher task + one run-coordinator task (concurrency = 1).
Wakes come from `tokio::sync::Notify` (permit semantics — lost-wakeup-safe)
and bus events (engine.ready, session.changed, run terminals); the
original design kept a bounded 5 s backstop re-scan (ADR-008) as a safety
net, not polling. The dispatcher waits for the active run's terminal
before the next claim (concurrency = 1, §56–§57). No per-item timers, no
busy loop, no parallel agent scheduler.

Consequences: near-zero idle CPU; deterministic single-flight dispatch;
explicit, conscious path for future parallelism. Status: ACCEPTED (TASK 13).

Superseded note (TASK 24 perf pass): the Notify design is now proven
lost-wakeup-safe by tests (`lost_wakeup_enqueue_at_idle_is_never_missed`,
`idle_dispatcher_ignores_stream_flood_and_does_zero_scans`), so the 5 s
backstop re-scan was REMOVED — the dispatcher is Notify-only with zero
periodic DB reads (PERFORMANCE.md "Queue (TASK 13)"). This ADR is
historical; it does not describe current behavior.

## ADR-026 — Fail-closed on queue durability failure

Problem: if the queue DB becomes unusable mid-run, silently continuing in
memory could lose work.

Decision: any durability failure sets `QueueStatus::Failed`, publishes
`runtime.error`, and stops all new dispatch; recovery requires a restart.
No in-memory fallback queue exists. Active runs keep streaming; their
terminals still transition rows when storage recovers, but no new claim is
accepted.

Consequences: correctness over availability for durable work; the failure
is loud and typed. Status: ACCEPTED (TASK 13).
## ADR-027 — SAIPEN stays the authority; SAIWORK2 is a read-only projection (TASK 14)

Problem: the phase-0 reader drifted from the canonical contract (looked for
`next`/`current task`, no `schema_version`, no board, no path security), and
a full reader could have become a second SAIPEN state machine.

Decision: `saiwork-saipen` is a read-only client of the canonical protocol,
verified against the `donors/saipen` baseline v7.224.3 (schema_version 3).
It performs zero canonical writes, invokes no SAIPEN command, holds no SQLite
mirror, and never re-implements the canonical validator (TASK 14 §1–§7,
§228–§230). Validation display is `not run` until a proven-non-mutating
canonical invocation exists. The parser is strict on required structure
(duplicate keys = error, unsupported schema = rejected) and tolerant on
harmless formatting.

Consequences: no fabrication (UNKNOWN stays UNKNOWN), no drift from the
protocol, and a clean boundary for TASK 15's action path. Status: ACCEPTED
(TASK 14).

## ADR-028 — One notify watcher per root with coalescing and generation tags

Problem: filesystem watchers leak, storm, or miss changes (atomic replace /
rename on Windows); a per-component watcher would multiply.

Decision: one `notify` watcher per attached workspace `.saipen` root,
non-recursive, owned by `SaipenService` (never React). Events flow through a
bounded channel (64); a full channel drops and sets an overflow flag forcing
a full authoritative reread. A dirty-flag + 300 ms quiet window coalesces a
storm into ~1 refresh; unchanged content emits no `saipen.changed`. Every
watch session carries a generation token; late events from closed/replaced
sessions are discarded. Root replacement triggers a bounded rebind; watcher
failure restarts at most twice (backoff, shutdown-aware) then degrades the
projection to read-only with `watch_status: failed`.

Consequences: bounded memory, bounded CPU, deterministic cleanup on
workspace close and app shutdown, no stale cross-workspace updates. Status:
ACCEPTED (TASK 14).

## ADR-029 — Component-aware path containment as the SAIPEN security boundary

Problem: string-prefix containment is not a boundary (symlink/junction
escape, `C:\a\bc` vs `C:\a\b`).

Decision: every SAIPEN path is resolved via `fs::canonicalize` (following
links) and checked component-aware against the canonical workspace root;
Windows prefixes (`\?\`) are normalized, comparisons are case-insensitive,
and device paths are rejected. A `.saipen` or canonical file reference
resolving outside the workspace is a typed `PathEscape` error, never
followed. Residual TOCTOU is documented (re-resolve at open; not eliminable
locally).

Consequences: hostile path matrices (symlink, junction, `..`, absolute,
separators, case tricks) are unit-tested and blocked; frontend still holds
no filesystem authority. Status: ACCEPTED (TASK 14).

## ADR-030 — Canonical SAIPEN actions only; SAIWORK2 never writes canonical files (TASK 15)

Status: ACCEPTED (TASK 15).

Context: the SAIPENBAR needs Continue/Status/Board/Knowledge/Validate/Stop,
but SAIWORK2 must never mutate canonical SAIPEN state by direct file
editing — the separation `UI action → typed command → SaipenClient →
canonical tool → filesystem → watcher → fresh snapshot → UI projection`
is non-negotiable (TASK 15 §2). Verified contract (donors/saipen v7.224.3):
the canonical `saipen.py` CLI has `status|next|recover|claim|transition|
checkpoint|ticket|improve|ship|push|scope|first-publish-confirm|userperson|
sub|context`; there is NO `continue`/`board`/`knowledge`/`validate`/`stop`
CLI command. `tools/validate.py` is the standalone read-only validator
(0 = conformant, 1 = domain-invalid, 2 = usage error).

Decision: only actions that exist in the verified canonical surface are
spawnable (`status`, `validate`); Board/Knowledge are local view actions on
the reader snapshot; Continue is `UnsupportedAction` (shown disabled with
reason — the canonical "continue" is the agent's protocol instruction in
STATE `next_action`, not a CLI command); Stop cancels SAIWORK2-owned action
processes only. All processes run through the one ProcessSupervisor, no
shell, explicit cwd = validated workspace root, bounded output/timeouts.
The backend enforces one active action per workspace (double click → Busy).
Validate results are bound to the snapshot generation they validated and
rendered STALE when the snapshot moves.

Consequences: production code performs zero canonical writes (write-audit
clean; all fs::write in the crate are `#[cfg(test)]`). The action surface
cannot drift into invented commands. SAIPEN→Queue handoff is deliberately
deferred until a canonical Continue exists and a stable correlation id can
be established (TASK 15 §69–§73) — no `Continue = enqueue prompt` invention.

## ADR-031 — SAIPENBAR is a derived multi-authority projection, not a state owner (TASK 15)

Status: ACCEPTED (TASK 15).

Context: SAIPENBAR composes SAIPEN state, canonical validation, the durable
QueueManager count, and action status. Merging these into one mutable owner
would create a second truth for each authority (TASK 15 §89, §99, §202).

Decision: the bar is read-only composition. Each field is fetched from its
own authority (`get_saipen` for SAIPEN, `queue_snapshot` for the queue,
`saipen_action_status` for actions/validation); the store only bumps a
revision on `saipen.*` events and the bar refetches — it never holds a
second SAIPEN truth and never recomputes availability from random fields
(§56). No polling; updates ride the canonical event stream (§201).

Consequences: cross-field corruption is impossible by construction;
composer authority mapping is enforced by the backend commands, not the
frontend. Status: the bar renders UNKNOWN honestly and disables
unsupported actions with reasons.

## ADR-032 — Validation generation binding: no green stale validation (TASK 15)

Status: ACCEPTED (TASK 15).

Context: showing a "valid" result from snapshot A as current after the
canonical files moved to B is exactly how two truths are born (TASK 15
§87–§88, §126).

Decision: every Validate result is recorded in the ActionManager keyed by
the snapshot generation it ran against (captured before the action). The
bar renders `VALIDATION: VALID · STALE` when the current generation differs;
a fresh Validate for the new generation replaces it. Validation runs only on
explicit user action — never on every filesystem event (§77).

Consequences: stale green is impossible; the reader (filesystem) remains
the only authority that can make validation current again.

## ADR-033 — Three-pane cockpit: backend authoritative, frontend projection (TASK 16)

Status: ACCEPTED (TASK 16).

Context: the first user-facing V1 workflow must feel like one system while
internally preserving separate authorities (SAIPEN, Queue, Engine, Model).
The earlier 5-column grid and per-event global store made a token stream
rerender the whole app.

Decision: the layout is a three-pane cockpit — TitleBar (project/engine/
model) · left nav (projects + sessions) · Conversation · right ActivityPanel
(Activity/Queue/Diagnostics tabs) · Composer (Send vs Queue vs Cancel run) ·
SAIPENBAR strip · statusline. The frontend is a pure projection: it may
compose state and request actions, never become the authority for runtime or
durable domain state. `message.delta` is batched in the store (flush per
~16 ms frame, terminal flushes first) and excluded from the diagnostics log,
so a token never rerenders the app. Per-domain revisions (queue.revision,
saipenRevision) protect the initial-query/event race.

Consequences: refreshing the frontend loses nothing (backend snapshots are
authority); streaming stays responsive; UI layout metadata stays
frontend-owned. Status: ACCEPTED.

## ADR-034 — Golden Vintage primary design system, one token source (TASK 16)

Status: ACCEPTED (TASK 16).

Context: Golden Vintage direction was established but tokens were scattered
and geometry inconsistent.

Decision: one token system in `global.css` (:root) — surfaces, borders, ink
scale, accent/ok/bad/warn/danger, focus, radius (near-square 3 px), spacing
scale, mono/serif stacks. No component-local themes, no theme engine, no
runtime CSS deps. Typography stays compact and readable (13–14 px body,
9–11 px labels/mono). Focus is always visible (`:focus-visible`), state is
never color-only.

Consequences: consistent hierarchy with low visual noise; future themes
remain possible without building a theme marketplace. Status: ACCEPTED.

## ADR-035 — Stream batching is a frontend projection concern (TASK 16)

Status: ACCEPTED (TASK 16).

Context: 10k+ deltas through the UI must not freeze the interface or rerender
unrelated panels, and a terminal event must never lose the final chunk.

Decision: the event stream itself is unchanged; the store batches
`message.delta` (accumulate + flush once per ~16 ms frame), flushes pending
deltas before terminal events, and excludes streaming noise from the log.
Conversation memoizes its slice; session/queue/saipen/engine components only
react to their own event families. Verified by tests.

Consequences: N deltas → 1 render per frame; terminal text always complete;
idle CPU stays event-driven. Status: ACCEPTED.

## ADR-036 — Freebuff DEFERRED: remote-cloud + Node-only SDK is not an EngineAdapter fit (TASK 17)

**Status:** accepted. **Date:** 2026-08-17.

**Context.** TASK 17 required a second production engine with current upstream
evidence (TASK 01 was reconnaissance). Freebuff was the primary candidate.

**Current contract (verified from `donors/freebuff`, SDK 0.10.7):**
- `@codebuff/sdk` is TypeScript/Node ≥ 22 only, built with bun, dragging a
  large JS tree (AI SDK, quickjs-wasm, tree-sitter-wasm, zod, ws, undici).
- Runs execute **remotely in the Codebuff cloud** behind a mandatory API key.
- Session continuation is `previousRun` JSON; there is no local session store.
- The `cli/` in the repo is the full application, not a headless engine.

**Decision.** Classify Freebuff as **DEFERRED (not an EngineAdapter fit for
V1)**. Do not distort SAIWORK2 core for it:
- No Rust HTTP client to an undocumented cloud protocol; no embedded Node
  runtime; no cloud API-key vault (security architecture change).
- The generic engine architecture is instead proven by a second **production**
  adapter, the Generic CLI (`engine-generic-cli`), which is fully within the
  existing contracts (ProcessSupervisor, EnginePort, capabilities).

**Consequences.** Zero Freebuff code in SAIWORK2 (no source copy, no SDK
dependency, no license exposure beyond the vendored donor snapshot). ROADMAP
"additional engines" is satisfied honestly; Freebuff may be revisited only if
a maintained, documented, local-execution integration surface appears.

## ADR-037 — Generic CLI: bounded trusted one-shot adapter as second engine (TASK 17)

**Status:** accepted. **Date:** 2026-08-17.

**Context.** The safe second-engine path per TASK 17 §43–§53: a bounded,
typed integration (configured executable, fixed arg template, workspace cwd,
stdin prompt, stdout response) — never "run any command string".

**Decision.** Implement `engine-generic-cli` with the `OneShotText`
capability level:
- Trusted config only from SAIWORK2-owned env vars; project files and models
  can never supply an executable (§44).
- No shell anywhere: `ProcessSpec` direct spawn, args as separate OS args,
  prompt as **stdin bytes** (§46). New generic `StdinPolicy::Bytes` and
  `ProcessSpec.output_cap_bytes` are the only generic process changes —
  both bounded, both proven not to affect OpenCode/FakeEngine.
- Capabilities are truthful: sessions=true (SAIWORK2-owned metadata), each
  send = one fresh process; resume/streaming/tools/permissions/models all
  false. Cancel = terminating the managed run process (run == process, §52).
- Registered in the desktop shell only when configured; malformed config
  surfaces a precise error and is not registered.

**Consequences.** EngineRegistry now hosts three engines (fake, opencode,
generic-cli) with per-engine health/capabilities; the UI consumes
`capabilities`, not engine names. Cross-engine tests prove ID isolation, no
fallback, and failure isolation.

## ADR-038 — Parallelism V1: one agent run per workspace, queue concurrency = 1 (TASK 18)

**Status:** accepted. **Date:** 2026-08-17.

**Context.** TASK 18 required proving controlled parallelism without gambling.
Audit results: OpenCode's run registry already supports **different-session**
runs (keyed by RunId, per-session exclusion, generation guard, per-run
cancel), FakeEngine models the same, and the CLI adapter is per-send
processes. The one real correctness gap was **same-workspace** execution:
without worktrees, two agent runs in the same physical workspace can mutate
the same repository concurrently.

**Decision.**
- **Same-workspace: serialized (REJECT, not queue).** `SessionManager.send`
  rejects a send to another session in a workspace that already has an
  active run with the typed `CoreError::WorkspaceBusy`; the queue-facing
  `EnginePort::session_busy` honors the same gate (the queue WAITs, never
  claims then fails); `resolve_session` New-mode checks busy before the send
  (§20). Different workspaces run concurrently (proven by tests).
- **Different-workspace: concurrent.** Two sessions in distinct workspaces
  run simultaneously; cancellation, failure, and terminals stay isolated.
- **Same-session: unchanged REJECT** per the engine contract (§70–§72); the
  CLI adapter now also rejects a second concurrent send to the same session.
- **Queue concurrency = 1.** The durable queue keeps its single dispatcher:
  the strongest proven correctness boundary (exactly-one claim, coherent
  recovery, simple ordering). No scheduler permits were needed. Documented,
  not a defect (TASK 18 §16, §236).
- **Release boundary:** FakeEngine registration is `#[cfg(debug_assertions)]`
  — release builds ship OpenCode (+ configured Generic CLI) only; queue
  failpoints are feature-gated and no-op in release.

**Consequences.** Proven by the new `parallelism` integration suite
(4 tests, 3× stable): cross-workspace concurrency + isolation, typed
same-workspace rejection, serialize-then-allow, queue-port workspace gate.
Any future worktree/isolated-workspace architecture can relax the gate; the
gate is the V1 correctness boundary.

## ADR-039 — DeepSeek Harness: EXPERIMENTAL ENGINE CANDIDATE, ACP-over-stdio seam (TASK 19)

**Status:** Accepted (TASK 19, audit-only — no production code).

**Decision.** Classify DeepSeek Harness as **B — EXPERIMENTAL ENGINE CANDIDATE** (upstream
is Developer Preview with explicit compatibility-breaking changes). Preferred future
integration seam: **ACP over stdio** (`@deepseek-ai/dsh-acp`), with **SDK JSON-RPC**
(`dsh-sdk-jsonrpc-server`) as fallback for observability/durable-enqueue needs. Web UI and
headless one-shot are rejected for the primary adapter (human interface / no session model).

**Evidence (audited 2026-08-17, tree `47f9438`).** ACP is the only seam with session
lifecycle (new/prompt/cancel), one-shot permission resolution, version negotiation, and
clean `end_turn`/`cancelled` termination. SDK JSON-RPC streams full session-log envelopes
and a durable `messageId` enqueue receipt but has **no cancel, no permission round-trip, no
version negotiation** (wire `0.0.1` unvalidated). Windows is viable (dedicated
`pwsh-local` provider, taskkill tree termination, spawn-per-call shell — no PTY
requirement); Windows sandbox strength and project-config auto-load are UNKNOWN probe gates.

**Consequences.** No production adapter in TASK 19. TASK 20 builds
`engine-deepseek-harness` behind the adapter firewall (no Harness DTO in generic core),
probes ACP handshake/Windows/trust behavior, and declares capabilities only after proof.
ACP fresh-sessions-only ⇒ `session_resume=false`; no idempotency key in either seam ⇒ the
TASK 13 ambiguous-dispatch policy applies unchanged (no redispatch). Harness owns its
sessions/credentials; SAIWORK2 stores references + queue correlation only. Subagents/
workflows/goals/jobs stay adapter-internal or deferred — SAIPEN + QueueManager remain the
only task authorities. Full contract: KNOWLEDGE/DEEPSEEK_HARNESS.md.

## ADR-040 — DeepSeek Harness adapter foundation implemented over ACP stdio (TASK 20)

**Status:** Accepted (TASK 20, foundation — vertical slice is TASK 21).

**Decision.** Implement `crates/engine-deepseek-harness` as the TASK 20 foundation over
**ACP over stdio** (the TASK 19/ADR-039 seam): discovery (explicit path → PATH), cheap
`--version` probe, ProcessSupervisor ownership of the top-level runtime, NDJSON JSON-RPC
2.0 transport with one reader per generation and a bounded pending-request registry,
ACP `initialize` handshake (serverInfo required, protocol version recorded, newer/unknown
versions accepted), lifecycle with generation protection and stop-during-start
cancellation, typed errors, and a deterministic fake-ACP-server fixture. All capabilities
are `false` (foundation only). One generic `saiwork-process` extension was justified:
`StdinPolicy::Piped` + `ProcessSpec::stdout_protocol` (a long-lived interactive stdio
protocol child is a generic capability; defaults unchanged — OpenCode/FakeEngine/Generic
CLI regression green).

**Evidence.** 29-test hostile matrix (3× stable): normal/delayed/fragmented handshake,
unknown notification, duplicate/unknown response id, server-request → -32601, request
timeout operation-local, protocol/stderr flood, handshake hang → typed timeout + process
killed, reject, exit before/after handshake, malformed/oversized/partial-frame EOF,
stop-during-start (no late READY, no orphan), stop-during-request (pending settles),
ignored shutdown → force termination, restart generation, 25× resource baseline,
capability honesty, idle = zero work, registry isolation. Real Harness smoke:
**BLOCKED UPSTREAM** — the npm CLI exposes no `acp` entry; `@deepseek-ai/dsh-acp`
0.0.1-rc.1 is published (BSD-3-Clause) but needs a source-checkout composition; the
concrete profile + real Windows handshake is the TASK 21 probe gate.

**Consequences.** The engine is registered only when explicitly configured
(`SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE`); the UI sees foundation-only capabilities and
never offers session workflows; QueueManager cannot dispatch to it (§113). TASK 21 maps
`session/update` committed chunks to `message.completed`, adds permissions/cancel, and
proves the real handshake before any capability is advertised. Adapter firewall holds:
no Harness/ACP DTO outside the crate (verified by the source-tree audit in TASK 20).

## ADR-041 — DeepSeek Harness agent vertical slice over ACP stdio (TASK 21)

**Status:** Accepted (TASK 21 — first complete Harness agent workflow; TASK 22 next).

**Decision.** Implement the TASK 21 vertical slice on the TASK 20 foundation (ADR-040),
still entirely behind the adapter firewall and over the same ACP-over-stdio seam:
authoritative `session/new` (fresh + connection-owned; `session_resume = false`),
adapter-local registries (`sessions.rs`/`runs.rs`/`permissions.rs`), a `session/prompt`
prompt task that is the single terminal authority (stop reason → exactly one
`message.completed`/`cancelled`/`failed`; no auto-retry on ambiguous transport loss),
`session/update` `agent_message_chunk` → incremental `message.delta` and `tool_call` →
`tool.*` (bounded output), `session/request_permission` → generic permission round-trip
(fail-closed: no decision = reject, never default allow), scoped `session/cancel`
(race-safe, exactly-one-terminal CAS). Same-session concurrency REJECT; different
sessions parallel. Turn/step identity stays adapter-internal — no generic StepId
(OpenCode/FakeEngine have no equivalent). No SQLite transcript mirror; QueueManager
dispatch stays disabled (TASK 23). Generic contract: `EngineIdentity.experimental: bool`
(Harness = true, all other engines = false; UI marks ⚠ and never hides instability).

**Evidence.** 28-test vertical matrix (`tests/vertical.rs`, real stdio fixture through
the ProcessSupervisor): normal/multi-step/tool/tool-fail, permission allow/deny/
no-response (fail-closed), cancel before-first-chunk/mid-chunk/race/twice/after-complete,
provider failure (engine stays ready), runtime crash, transport loss,
accepted-then-response-lost (outcome unknown, no retry), duplicate chunk, wrong-session
isolation, session busy, second turn after terminal, restart connection-ownership, engine
stop settles runs, 10k-chunk stream, generic SessionManager flow + permission round-trip.
Foundation hostile matrix (29) unchanged and green. OpenCode/FakeEngine/Generic CLI
regression green (serial runs; parallel-process contention on this Windows box is a
pre-existing test-harness issue, not a code defect). Real Harness Windows E2E:
**BLOCKED EXTERNAL** — composing a runnable ACP profile still requires the full Cordis
plugin tree + provider config; the fixture matrix proves the workflow deterministically.

**Consequences.** The engine now advertises `streaming/sessions/cancel/tools/permissions/
parallel_sessions` (all fixture-proven); `resume`/`models` stay false. The UI marks the
engine experimental and offers the same generic session/conversation/tool/permission UX
as OpenCode — no Harness-specific UI, no WebView, no raw session-log dump. TASK 22 owns
any architectural donor improvements that survive real use.

## ADR-042 — Capability / runtime architecture audit: no generic refactor required (TASK 22)

**Status:** Accepted (TASK 22 — evidence-driven; the existing architecture handled a
third engine cleanly).

**Decision.** Classify every TASK 21 Harness-derived improvement candidate
(DEEPSEEK_HARNESS.md §23 candidates table) and implement only evidence-backed changes:

- **ADOPT NOW — event semantic classification.** Document per-family durable/live/stream/
  invalidation semantics + reconstruction sources in EVENTS.md (§31–§36, §170–§171) and
  enrich the `EventClass` doc comment. The EventBus remains runtime fact distribution,
  never a database (§30): no event replay, no event-sourcing, no SQLite replacement.
- **Fix one real cleanup bug** (§147/§17): the Harness adapter `start()` leaked a spawned
  runtime on a late serialization failure after the runtime was created — it now rolls
  back through `teardown_runtime` like every other late failure (partial-initialization
  cleanup).
- **Remove one generic-UI leak** (§88): the TitleBar "Start the OpenCode runtime"
  tooltip is now engine-agnostic.
- **ALREADY SOLVED / DEFER / REJECT** everything else: reversible-effect ownership
  (Rust ownership + `JoinSet` + `take()` already make cleanup clear — no `TaskScope`
  framework), capability model (flat truthful booleans suffice; no static/runtime split),
  operation correlation (events already carry session_id/run_id), turn/step (adapter-
  local, REJECT generic StepId), tool-cycle grouping (DEFER), process seams
  (ProcessSupervisor already owns top-level runtimes), filesystem (REJECT premature
  remote FS), config composition (REJECT), fail-closed permissions (already implemented
  in TASK 21), error model (no brittle string matching exists), engine runtime scope
  (EngineId + generation suffice), workspace trust (DEFER — no concrete gated path yet),
  static registration (already clean), generic contract test suite (DEFER — risks vendor
  special-casing).

**Evidence.** TASK 22 audits: vendor-leak search (generic core/queue/UI clean — only
static registration in the desktop shell, allowed); capability-branch search (UI is
capability-driven, no `if engine == X`); cleanup/ownership search (both adapters own
tasks via Rust ownership; the Harness transport self-owns its reader with idempotent
`close()`); state-authority search (frontend is a clean projection); generic-JSON search
(typed surfaces, no raw payloads); error-matching search (no `includes("429")`-style
brittle matching).

**Consequences.** No new runtime dependency, no dynamic plugin / service-locator / IoC /
effect-framework architecture, no DB migration, no capability ontology. Static
EngineAdapter registration remains the V1 architecture. The one cleanup-bug fix is
covered by the unchanged 29-test hostile + 28-test vertical Harness matrices (green
serial). QueueManager never dispatches to Harness (TASK 23). SAIPEN remains canonical
project protocol; Harness workflows/jobs remain engine-local and are explicitly rejected
as Queue/SAIPEN replacements (§95–§96).

## ADR-043 — Harness as a durable QueueManager target + OUTCOME_UNKNOWN (TASK 23)

**Status:** Accepted (TASK 23 — Harness is a trustworthy durable execution target;
SAIPEN→Queue explicitly deferred).

**Decision.** Enable DeepSeek Harness as a durable QueueManager target through the
**existing generic** path — `EnginePort` → `SessionManager` → `EngineAdapter` — with no
ACP/Harness protocol knowledge in the queue and no queue-DB write in the adapter. Add
`QueueState::Unknown` as a first-class durable state: a crash during the `sending`
handoff, or a DISPATCHED item at restart whose engine authority is unrecoverable (Harness
ACP sessions are connection-owned; OpenCode/Fake run registries are in-memory), becomes
`unknown` — never auto-dispatched, **blocks further mutating queued dispatch in its
workspace**, resolved only by explicit user action (risk-acknowledged retry as a new
attempt, cancel, or an externally found terminal). Persist correlation via the existing
`session_id` + `run_id` columns (ACP exposes no durable TurnId and no idempotency key —
the session is the correlation unit; exactly-once external effect is not claimable across
the crash boundary, so the conservative `UNKNOWN` fallback applies). **SAIPEN → Queue is
DEFERRED**: the canonical SAIPEN contract exposes no mutating `continue` and no stable
execution identity, so automatic handoff cannot be proven exactly-once; a future handoff
must use a stable canonical source id as the idempotency key, never a direct
`SaipenClient → engine.send` bypass and never `saipen.changed → enqueue` (a duplication
machine).

**Evidence.** `tests/queue_slice.rs` (4, real production wiring over the fixture-backed
adapter): enqueue→dispatch→Harness send→DONE; queue cancel→Harness `session/cancel`→
CANCELLED (engine stays READY); provider failure→FAILED (engine stays READY); engine
crash→FAILED no auto-requeue. `saiwork-queue` OUTCOME_UNKNOWN tests (6 new): crash in
`sending`→UNKNOWN never redispatch; DISPATCHED at restart→UNKNOWN (old correlation
retained); UNKNOWN blocks same workspace but not others (unblocks on resolution); retry
UNKNOWN→new attempt (old-attempt terminal cannot mutate it); cancel UNKNOWN→CANCELLED.
Repo-level recovery tests updated to UNKNOWN. Crash matrix failpoints (crash before
send / after acceptance) never duplicate an accepted send. Regression: Harness hostile
29 + vertical 28 + queue_slice 4, FakeEngine 39, OpenCode protocol 37, saiwork-core/
queue/saipen/storage, desktop parallelism 4 + lifecycle 8, frontend typecheck + 13 tests
all green. No DB migration (state is a TEXT column; new value only).

**Consequences.** Harness is a real queue target with no engine fallback; a crash after
possible acceptance never triggers blind replay (UNKNOWN blocks the workspace). The UI
shows `unknown` with an explicit "execution status uncertain" note and a
risk-acknowledged Retry. OpenCode/Fake queue rows are unaffected. TASK 24 owns
cross-engine hardening.

## ADR-044 — Multi-engine session-id collision guard: fail closed, do not namespace (TASK 24)

**Status:** Accepted (TASK 24 — post-V1 multi-engine hardening).

**Decision.** The generic session-id namespace is the **adapter's own id**: engine events
re-emit the exact `session_id` the adapter returned on `create_session` verbatim
(OpenCode server id, Harness `ses-{uuid}`, Fake `fake-session-…`), and the frontend
store keys sessions by that opaque id. Namespacing the SAIWORK2 id at the
`SessionManager` boundary (e.g. `{engine_id}:{info.id}`) would break event correlation
across every adapter unless each adapter also namespaced its emitted `session_id` — a
large, risky contract change. Instead: `SessionManager::create` now **fails closed** if
an engine returns a session id already owned by a *different* engine
(`CoreError::SessionIdConflict`), instead of silently overwriting the first engine's
session in the generic map and DB. All three real adapters generate uuid-derived ids, so
this is a defense-in-depth guard against hostile/misbehaving adapters (TASK 24 §9/§120)
— and it converts a silent cross-engine corruption into a loud, typed rejection.

**Evidence.** `tests/multi_engine.rs` (6, real production wiring — `EngineRegistry` +
`SessionManager` + `QueueEnginePort` + `QueueManager` — with FakeEngine + HarnessAdapter
registered simultaneously): cross-engine session/run isolation (same run ids distinct,
both complete on their own session ids); cross-engine queue routing to the exact stored
`engine_id`; queue target immutability (selection change never retargets); one-engine
failure isolation (Harness taken down while a FakeEngine hang run is live — the fake
run is untouched, a queued Harness item never falls back to FakeEngine, and FakeEngine
queued work in another workspace still dispatches); same-workspace cross-engine
serialization (FakeEngine run in `w1` → Harness send into `w1` rejected `WorkspaceBusy`,
`w2` unaffected); and the collision guard (two hostile adapters returning session id
`"1"` — second create rejected with `SessionIdConflict`, first engine's session intact).

**Consequences.** No schema/migration change. Generic state can never be silently
corrupted by colliding cross-engine session ids. The same-workspace serialization law
(ADR-038) is engine-independent: an active FakeEngine run blocks a Harness run in the
same physical workspace. TASK 24 needs no further architecture change.
