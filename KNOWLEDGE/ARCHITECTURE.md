# ARCHITECTURE.md

## Diagram

```
                    ┌───────────────────────┐
                    │      SAIWORK2 UI      │
                    │     React / TS        │
                    └───────────┬───────────┘
                                │
                          Commands / Events
                                │
              ┌─────────────────▼─────────────────┐
              │           SAIWORK2 CORE           │
              │                                   │
              │ WorkspaceManager                  │
              │ SessionManager                    │
              │ ProcessSupervisor                 │
              │ EngineRegistry                    │
              │ EventBus                          │
              │ QueueManager                      │
              │ Storage                           │
              │ SaipenClient                      │
              │ RuntimeDiagnostics                │
              └───────┬───────────────┬───────────┘
                      │               │
              EngineAdapter       SAIPEN
                      │               │
          ┌───────────┼───────────┐   │
          │           │           │   │
       OpenCode    Freebuff   Generic CLI
```

Architecture is layered. Dependencies point **down**. The UI never knows the
internals of the process supervisor, storage, or provider authentication.

## Application lifecycle (TASK 08)

The `App` runtime in `saiwork-core` is the **one application authority**
(law 9). It owns EventBus, Storage, ProcessSupervisor, the engine registry,
diagnostics and the lifecycle state machine. It is a **coordinator**, not a
god object: queue/SAIPEN/OpenCode/UI logic never lives there.

### Application states

```text
BOOTING → READY            (bootstrap completed)
BOOTING → FAILED           (required init failed)
BOOTING → SHUTTING_DOWN    (shutdown requested during boot)
READY   → SHUTTING_DOWN
FAILED  → SHUTTING_DOWN
SHUTTING_DOWN → STOPPED    (cleanup complete)
```

No resurrection within one process lifetime: `STOPPED → READY` and
`FAILED → READY` are impossible. Restart = a new OS process. The frontend
projects this state read-only (`booting` / `ready` / `shutting_down` /
`stopped` / `failed`); the Rust `AtomicU8` state is the single source of
truth (law 23, no Schrödinger state built from boolean combinations).

### Startup order (deterministic)

```text
1. resolve executable/application identity (AppConfig)
2. acquire single-instance authority      (desktop shell, process-level)
3. resolve data root
4. logging/diagnostics bootstrap          (desktop shell, before bootstrap)
5. open Storage + migrations + integrity preflight
6. EventBus
7. ProcessSupervisor (before any production spawn)
8. other foundation services (registries, managers)
9. publish app.started → state READY
```

Required-service failure is **fail-closed**: the app never enters READY, no
engine starts, no child process starts, and the error surfaces with stage /
category / user-safe message. No fallback to a temporary replacement
database; corruption is never silently reset.

### Shutdown order (deterministic, idempotent)

```text
1. → SHUTTING_DOWN (new work rejected via require_ready)
2. publish app.stopping
3. supervisor rejects new spawns
4. stop/dispose engines (cancel runs, release pending permissions)
5. ProcessSupervisor.shutdown(): graceful → bounded wait → force
6. storage checkpoint (WAL flush) + final integrity
7. → STOPPED; process exits
```

The EventBus is never closed first: `app.stopping` is published before
anything else dies so consumers observe the shutdown (EVENTS.md). All waits
are bounded; a repeated/concurrent shutdown request observes the same
terminal outcome (one sequence runs).

### Single instance (desktop shell)

`tauri-plugin-single-instance` acquires an OS process-level mutex **before**
core bootstrap (plugin init precedes the `setup` hook): a second launch
relays typed intent, activates the existing window and exits without ever
opening the database — two DB authorities / two supervisors can never
coexist. The OS mutex releases automatically on crash (stale-state safety).

## Ownership (exactly one authority per resource)

| Resource / State | Authority | Readers | Writers | Persistence |
| --- | --- | --- | --- | --- |
| Workspace metadata | `saiwork-storage` (core) | UI projection, core services | core only | SQLite `workspaces` |
| UI state | core state projection | UI | EventBus events (core) | none (rebuilt on start) |
| Queue | `saiwork-queue` + SQLite (law 7) | UI projection, dispatcher | QueueManager only | SQLite `queue_items` |
| Engine process state | `ProcessSupervisor` (law 6) | engines, diagnostics | supervisor only | runtime only (restart rebuilds) |
| Engine sessions content | the engine (OpenCode) | adapter | engine | engine-owned |
| Engine sessions metadata | `saiwork-storage` (core) | UI | core only | SQLite `sessions_meta` |
| SAIPEN state | canonical SAIPEN (`.saipen/`) | SaipenClient, UI projection | canonical SAIPEN writer only | `.saipen/*` |
| Provider credentials | the provider's own auth (OpenCode) | engine | engine auth | engine-owned store |
| Project files | the user's project | tools/engines on request | user tools | project filesystem |
| Application settings | `saiwork-storage` | core, UI projection | core only | SQLite `app_settings` |
| Diagnostics metadata | `saiwork-diagnostics` | UI, logs | core | bounded ring + DB retention |

## Dependency direction

```text
UI
↓
application/core contracts (saiwork-core)
↓
domain services (events, storage, process, queue, saipen, diagnostics)
↓
engine adapters (engine-fake, engine-opencode, …)
↓
external systems (OpenCode, SAIPEN, git, OS)
```

Forbidden reverse dependencies (examples, not exhaustive):

```text
engine-opencode → React UI
storage → UI component
SAIPEN parser → Freebuff adapter
saiwork-core → a specific engine
```

Engines never depend on each other; core never depends on a specific engine.
The crate layout may evolve (TASK 03+), but the logical dependency direction
is fixed.

## Module boundaries (crates)

| Crate | Responsibility | Depends on |
| --- | --- | --- |
| `saiwork-events` | Event taxonomy + bounded EventBus | — |
| `saiwork-storage` | SQLite, migrations, settings/workspaces/session meta | — |
| `saiwork-process` | ProcessSupervisor, bounded output, Job Object tree ownership, stop/kill policy | `saiwork-events` (ProcessId, process.* events) |
| `saiwork-queue` | durable queue authority (TASK 13): state machine, atomic claim, lease phases, single dispatcher + run coordinator, startup recovery, pause/revision/reorder, fail-closed | `saiwork-events`, `saiwork-storage` |
| `saiwork-saipen` | canonical SAIPEN integration: read (TASK 14) — discovery, strict STATE/BOARD parsers, path security, one notify watcher per root with debounce/coalesce/overflow recovery, snapshot projection service; actions (TASK 15) — SaipenTool discovery from STATE `saipen_home`, typed SaipenAction, ActionManager (per-workspace exclusivity, lifecycle, validation-generation staleness), canonical tool execution through ProcessSupervisor | `saiwork-events`, `saiwork-process`, notify |
| `saiwork-diagnostics` | runtime diagnostics, secret redaction | — |
| `saiwork-core` | orchestration: app bootstrap, lifecycle state machine, workspace, engine registry, shutdown | everything above |
| `engine-fake` | FakeEngine — first EngineAdapter, permanent test infrastructure | `saiwork-core` (contract), `saiwork-events` |
| `engine-opencode` | OpenCode adapter (phase 1) — discovery, probe, launch spec, supervisor-spawned `opencode serve`, endpoint, readiness, lifecycle | `saiwork-core` (contract), `saiwork-events`, `saiwork-process`, `saiwork-diagnostics`, reqwest (minimal) |
| `engine-generic-cli` | Generic CLI adapter (TASK 17) — second production engine, `OneShotText`: trusted env config, ProcessSupervisor spawn, prompt as stdin bytes, bounded output/timeout, run==process cancel | `saiwork-core` (contract), `saiwork-events`, `saiwork-process`, `saiwork-diagnostics` |
| Freebuff | **DEFERRED** (TASK 17, ADR-036) — remote-cloud-only, Node≥22-only SDK, credential vault required; no crate | — |

Dependency direction: UI → core → (events, storage, process, queue, saipen) →
engines. Engines never depend on each other. Core never depends on a specific
engine. `engine-opencode` consumes `saiwork-process` (builds `ProcessSpec`s,
never spawns directly); `saiwork-process` knows nothing about OpenCode
(TASK 10 §4). The adapter is registered in `EngineRegistry` (desktop shell)
but advertises only process-capable flags (`sessions/streaming/models` all

## Frontend architecture (TASK 16)

- **Three-pane cockpit** (UI_UX.md): TitleBar (project/engine/model) · left
  nav (projects + sessions) · Conversation · right ActivityPanel (tabs:
  Activity tools/permissions, Queue, Diagnostics) · Composer (Send vs Queue
  vs Cancel run) · SAIPENBAR strip · statusline.
- **Store**: one projection updated by canonical events + typed command
  results only; stream `message.delta` events are batched (accumulate in the
  store, flush once per ~16 ms frame; terminal events flush first) so a
  token never rerenders the app. Diagnostics log excludes streaming noise.
  Per-domain revisions (`queue.revision`, `saipenRevision`) protect the
  initial-query/event race — components refetch authoritative snapshots.
- **Permissions**: the engine adapter's `resolve_permission` is now routed
  end-to-end — `SessionManager::resolve_permission` → Tauri
  `resolve_permission` command → backend.ts → Allow/Deny buttons in the
  conversation (TASK 16 §36–§38, §166).
- **Markdown**: react-markdown + remark-gfm (only new runtime dep, TASK 16
  §27, §217); streaming renders plain text, Markdown finalized at terminal;
  fenced code blocks get a copy button; no raw HTML.
`false` until TASK 11 §80–§83) and is never auto-started at idle (§124).

The queue talks to engines only through the typed `EnginePort` boundary
(`saiwork-core::queue_port`, TASK 13): `engine_state`, `ensure_session`,
`session_busy`, `create_session`, `send`, `cancel`. The queue never knows
OpenCode DTOs, sessions internals, or provider details; `saiwork-core`
bridges the port onto the engine registry + SessionManager. Queue dispatch
is concurrency-1 and serialized with direct sends by the session-busy
arbitration (queue waits; direct send rejects with SessionBusy).

## The 25 non-negotiable laws

1. SAIWORK2 is an orchestration cockpit, not an AI provider implementation.
2. SAIPEN remains authoritative for SAIPEN state.
3. Engine-specific behavior must stop at EngineAdapter boundaries.
4. UI must never own child processes.
5. UI must never own durable queue state.
6. There must be exactly one ProcessSupervisor authority.
7. There must be exactly one durable QueueManager authority.
8. There must be one normalized application EventBus.
9. There must be exactly one desktop runtime.
10. No feature enters core merely because a donor project had it.
11. No dependency enters the project without an actual runtime need.
12. No polling when a reliable event/watch mechanism exists.
13. No unbounded queue, cache, log, listener set, retry loop, transcript buffer or process-output buffer.
14. External engine credentials remain owned by those engines whenever practical.
15. Portable mode has exactly one deterministic writable application data root.
16. Project source files are never silently copied into SAIWORK2 state.
17. Every external engine may fail, hang, restart or disappear at any time.
18. Every mutation has exactly one explicit authority.
19. Every subscription, timer, watcher and background task must have cleanup/cancellation.
20. Runtime behavior must remain deterministic after restart/crash.
21. Error paths are part of the architecture, not optional cleanup work.
22. Reliability beats feature count.
23. UI state is a projection of authoritative core state, not another authority.
24. Historical bugs migrated from SAIWORK must become regression tests.
25. Do not duplicate capabilities already safely provided by an engine unless there is a demonstrated reason.

Violating a law requires an explicit ADR in `KNOWLEDGE/DECISIONS.md` **before**
code lands.

## Confirmed donor landmines (TASK 01 evidence, must not return)

These are facts from the donor audit (MIGRATION_SAIWORK.md §25); they are
folded into the laws above and listed here because each one shipped in a real
product:

1. **Dual desktop shells** (Electron + Tauri) — duplicated lifecycle/state/fix surface. → law 9.
2. **Competing process ownership** — per-workspace runtimes + orphan registry + background-process manager overlapped. → law 6.
3. **Whole-file synchronous queue persistence** — every mutation rewrote one JSON snapshot on the main thread. → laws 7, 13; SQLite leases (ADR-005).
4. **Unbounded SSE buffering for slow clients** — write() returns ignored, memory grew. → law 13; disconnect-and-resync.
5. **Lexical-only path containment** — symlink/junction escape; fixed only after shipping. → SECURITY.md; fail-closed canonical resolution from day one.
6. **Provider abstraction over provider abstraction** — own provider layer + shim + mapping; fallback gating reinvented. → law 3; route through OpenCode (ADR-003).
7. **Fixed-sleep readiness** — a 1500 ms startup tax on every cold launch. → PROCESS_LIFECYCLE.md; predicate-based probes only.
8. **Multiple SAIPEN surfaces** — server module + embedded renderer + auto-update, guarded by a no-second-runtime test. → law 2; single SaipenClient.
9. **Frontend/backend responsibility leakage** — UI routes could request filesystem/instance writes; late writers resurrected deleted namespaces. → laws 18/23.
10. **Broad polling drift** — self-heal scans had to be bounded and documented to stay legal. → law 12; one guarded backstop.

## OpenCode adapter verification (TASK 10, 2026-08-16)

Verified against **opencode-ai@1.18.18** (npm global install, native
`opencode.exe` + `.cmd` shim): discovery resolves the native exe via PATH;
`opencode serve --hostname 127.0.0.1 --port N [--pure]` binds loopback only;
readiness = authenticated `GET /doc` (HTTP 200 + OpenAPI `info.title ==
"opencode"`); auth = HTTP Basic via `OPENCODE_SERVER_PASSWORD` env (no CLI
flag in 1.18.18); `--port 0` resolves to 4096, not an OS-assigned port, so
the adapter allocates an explicit available port (EADDRINUSE → bounded
retry). The adapter owns: discovery (explicit path → PATH, no disk scan),
probe (lightweight CLI version/help check, no server), launch spec (native
exe preferred; `.cmd` shim via `cmd.exe /D /S /C` + `raw_args`), readiness
loop (cancellable, bounded, process-death short-circuit), lifecycle
(STARTING/READY/STOPPING/STOPPED/FAILED + unexpected-exit detection), and
secret handling (random per-runtime password, env-only, redacted).

Ownership stays clean: one runtime per adapter instance, each with its own
`ProcessId`/endpoint/secret; `AppRuntime` shutdown stops engines before
`supervisor.shutdown()` — no OpenCode process survives normal app exit.

## Phase 0 verification (TASK 09, 2026-08-16)

Ownership audit (§4–§5) against the real code: each resource has exactly one
authoritative owner — `AppRuntime` (lifecycle), `EventBus` (events),
`saiwork-storage::Db` (SQLite, sole connection), `ProcessSupervisor`
(children, sole spawn authority), `EngineRegistry` (FakeEngine instances),
and shutdown is performed by `AppRuntime` alone. No duplicate authority was
found: no second event dispatcher, no second DB connection, no unmanaged
child process, no hidden global singleton (no `OnceLock`/`LazyLock` in
authority roles). No architecture correction was required; this file matched
the implementation.

## DeepSeek Harness adapter foundation (TASK 20 — implemented)

TASK 19 classified DeepSeek Harness as an EXPERIMENTAL ENGINE CANDIDATE with the
ACP-over-stdio seam (ADR-039; contract + implemented truth in DEEPSEEK_HARNESS.md).
`crates/engine-deepseek-harness` now implements the foundation behind the adapter
firewall (ADR-040):

```text
AppRuntime ─ EngineRegistry ─ EngineAdapter ─ HarnessAdapter ─ ACP stdio (NDJSON JSON-RPC)
                                                                         ↓
                                                          Harness runtime (ProcessSupervisor-owned)
```

Invariants are enforced by the existing authority map and the 25 laws:
ProcessSupervisor owns the top-level Harness process only (no double supervision of inner
tool subprocesses — PROCESS_LIFECYCLE.md); Harness owns its sessions and credentials;
SAIWORK2 stores only references + queue correlation (no transcript mirror); no Harness
DTO crosses into core/React/QueueManager/SAIPEN; engine-generation guards keep stale
runtime events out of current state. `saiwork-process` gained one generic capability
(`StdinPolicy::Piped` + `ProcessSpec::stdout_protocol`) for long-lived interactive
stdio-protocol children; defaults unchanged (OpenCode/FakeEngine/Generic CLI proven by
regression). All session capabilities are `false` until the TASK 21 vertical slice.

## DeepSeek Harness agent vertical slice (TASK 21 — implemented)

TASK 21 added the first complete Harness agent workflow on top of the foundation (ADR-041;
implemented truth in DEEPSEEK_HARNESS.md §23), still entirely behind the adapter firewall:

```text
HarnessAdapter
 ├─ sessions.rs    SAIWORK2 id ↔ opaque Harness session id (connection-owned, cleared on teardown)
 ├─ runs.rs        RunId ↔ HarnessSessionId, cancel signal, terminal CAS (exactly-one-terminal)
 ├─ permissions.rs pending permission requests (fail-closed decision oneshots)
 ├─ events.rs      session/update → message.* / tool.*; request_permission → permission.*
 └─ adapter.rs     create_session / send (prompt task) / cancel / resolve_permission
```

One `send` = one generic RunId ↔ one ACP `session/prompt`; same-session concurrency is
REJECT; different sessions run in parallel. Turn/step identity stays adapter-internal (no
generic StepId — OpenCode/FakeEngine have no equivalent). Sessions are fresh +
connection-owned (`session_resume = false`); a runtime restart empties the registry and
stale ids fail `SessionNotFound` — never a fabricated reconstruction. No SQLite transcript
mirror; QueueManager dispatch stays disabled (TASK 23). Generic contract gained
`EngineIdentity.experimental: bool` (Harness = `true`, all other engines = `false`); the
UI marks experimental engines and never hides instability.

## Capability / runtime architecture audit (TASK 22 — no generic refactor required)

TASK 22 audited the architecture against every Harness-derived improvement candidate
(DEEPSEEK_HARNESS.md §23 candidates table; ADR-042). Conclusion: the existing generic
architecture already handled a third engine cleanly — no dynamic-plugin / service-locator /
effect-framework redesign was warranted. Delivered:

- **Event semantic classification documented** (Candidate C ADOPT): EVENTS.md now has a
  per-family durable/live/stream/invalidation classification with reconstruction sources
  (§31–§36, §170–§171). The EventBus remains runtime fact distribution, never a database
  (§30); `EventClass` doc comment maps onto the model.
- **One real cleanup bug fixed** (§147/§17): the Harness adapter's `start()` had a
  partial-initialization leak — a late serialization failure after the runtime was
  created returned without tearing down the spawned process + transport reader. It now
  rolls back through the same `teardown_runtime` path as every other late failure.
- **One generic-UI leak removed** (§88): the TitleBar "Start the OpenCode runtime"
  tooltip is now engine-agnostic.

Audit results: vendor-leak search (generic core/queue/UI clean — the only references are
static engine registration in the desktop shell, explicitly allowed); capability-branch
search (UI is capability-driven, no `if engine == X`); cleanup search (adapters own their
tasks via Rust ownership — `Runtime` structs + `JoinSet` + `take()` idempotence;
Harness transport self-owns its reader with idempotent `close()`); state-authority search
(frontend is a clean projection); generic-JSON search (typed message/tool/permission
surfaces, no raw payloads). No capability/state-authority/event-model ambiguity requiring
an enum split or a new generic ID was found.

## Harness as a durable queue target (TASK 23 — implemented)

TASK 23 made DeepSeek Harness a trustworthy durable execution target through the
**existing generic** path — no ACP knowledge entered the queue, no queue-DB write entered
the adapter (ADR-043; implemented truth in DEEPSEEK_HARNESS.md §24, QUEUE.md):

```text
QueueManager ──EnginePort──▶ SessionManager ──EngineAdapter──▶ HarnessAdapter ──ACP──▶ Harness
     │                                                                    │
     └─ SQLite queue_items (session_id + run_id correlation)              └─ session/cancel (never engine kill)
```

- **`QueueState::Unknown`** added as a first-class durable state: a crash during the
  `sending` handoff, or a DISPATCHED item at restart whose engine authority is
  unrecoverable (Harness ACP sessions are connection-owned; other run registries are
  in-memory), becomes `unknown` — never auto-dispatched, **blocks further mutating
  dispatch in its workspace**, resolved only by explicit user action (risk-acknowledged
  retry as a new attempt, cancel, or an externally found terminal).
- **Workspace ambiguity gate:** an `unknown` item blocks its workspace; other workspaces
  proceed (TASK 23 §50–§51).
- **Correlation:** `session_id` + `run_id` persist on the queue row; ACP exposes no
  durable TurnId and no idempotency key, so the session is the correlation unit and
  exactly-once external effect is not claimable across the crash boundary — the honest
  `UNKNOWN` fallback applies.
- **SAIPEN → Queue: DEFERRED** — the canonical SAIPEN contract exposes no mutating
  `continue` and no stable execution identity; automatic handoff cannot be proven
  exactly-once (ADR-043).

## Post-V1 multi-engine hardening (TASK 24 — implemented, ADR-044)

TASK 24 is a **gate, not a refactor**: the generic architecture already isolated the
engines. New evidence-backed hardening:

- **Fail-closed session-id collision guard (ADR-044).** The generic session-id
  namespace is the adapter's own id (engine events re-emit it verbatim). A second
  engine returning the same id would have silently overwritten the first engine's
  session in the generic map/DB — `SessionManager::create` now rejects it with
  `SessionIdConflict`. No namespacing (that would break event correlation across
  adapters), no schema change.
- **Cross-engine hostile matrix** (`crates/engine-deepseek-harness/tests/multi_engine.rs`,
  6 tests, real production wiring with FakeEngine + HarnessAdapter in one registry):
  session/run isolation; queue routing to the exact stored `engine_id`; queue target
  immutability (selection changes never retarget); one-engine failure isolation (no
  fallback, other engine untouched); same-workspace cross-engine serialization
  (ADR-038 is engine-independent); collision guard.

EngineRegistry remains the single authority modeling simultaneous
OpenCode READY + Harness READY + another unavailable without a global "current engine"
collapse (TASK 24 §6–§8). EngineId (adapter identity) ≠ runtime generation (incarnation)
≠ SessionId (engine-scoped) ≠ RunId (one SAIWORK2 execution) — no conflation.
