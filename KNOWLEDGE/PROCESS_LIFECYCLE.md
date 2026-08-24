# PROCESS_LIFECYCLE.md

`ProcessSupervisor` is the **only** owner of SAIWORK2 child processes (law 6).
The UI never spawns or kills processes (law 4). Implemented in
`saiwork-process` (TASK 06).

## Process state machine (OS process, NOT engine lifecycle)

```text
SPAWNING → RUNNING → STOPPING → EXITED
              │         │
              ▼         ▼
            FAILED    FAILED
```

- `RUNNING` means **only** "the OS process exists and has not exited".
- `READY` / `engine.ready` is a **different** state owned by the engine
  adapter layer (TASK 07+), never by the supervisor (donor lesson: SAIWORK
  background-process manager conflated the two; regression P-01).
- Transitions are validated: terminal states never leave; a stopped process
  is never resurrected (restart = a new `ProcessId`).
- `FAILED` = spawn/wait failure (process never ran or wait broke). A non-zero
  exit code is **not** FAILED — it is `EXITED` with that code; interpretation
  belongs to the engine layer.

## Identity

- `ProcessId` (application identity, typed, `saiwork-events::id`) is distinct
  from the OS PID, which may be reused. No signal/cleanup is ever sent to a
  bare PID from the past; the supervisor only acts on processes it spawned
  this run (donor orphan-registry lesson).
- Every child has exactly one owner: the supervisor (law 6). Unmanaged
  `Command::new().spawn()` outside test fixtures is forbidden.

## Responsibilities

- spawn (command, separate args, cwd, env add/remove, stdin policy) with a
  unique `ProcessId`; direct execution, never a shell string; on Windows,
  `ProcessSpec::raw_args` optionally carries a verbatim quoted command line
  (for cmd.exe-wrapper launches such as npm `.cmd` shims — Rust passes it
  straight to CreateProcess with no MSYS/shell mangling);
- process-tree ownership: Windows **Job Object** (see below); Unix process
  group;
- bounded stdout/stderr, captured **separately** (ring buffers, 512 KiB cap /
  256 KiB retain per stream — donor values, revisit after measurement);
  per-process output cap override (`ProcessSpec::output_cap_bytes`, TASK 17
  §49) lets an engine preserve a bounded *response* channel independently of
  the diagnostic buffer policy;
- stdin policy `Null | Inherit | Bytes(Vec<u8>)` (TASK 17 §46): `Bytes`
  writes bounded prompt bytes to the child then closes (EOF) — never a
  shell string; Debug prints only the byte count (prompt redaction);
- lossy UTF-8 reading (invalid bytes become U+FFFD, never a panic); partial
  lines are held until newline/EOF and delivered exactly once;
- natural exit detection via `child.wait()` (event-driven, no polling loop);
- graceful stop → bounded wait → force kill (whole tree) → bounded wait;
- terminal events (`process.started` / `process.exited` / `process.failed`)
  on the canonical EventBus, published **after** exit is known, output
  readers have drained (≤ 2 s), and the record is updated; `raw_args` are
  redacted from the Debug snapshot (only the base command name is shown);
- orphan prevention: on shutdown every owned tree is killed; **0 orphan child
  processes** after normal exit (M0 gate). Closing the last Job Object handle
  terminates any survivor (`KILL_ON_JOB_CLOSE`), so even an abnormal app exit
  cannot leak children;
- exited records leave the registry (bounded registry); exit history lives in
  the events + diagnostics ring, never in an ever-growing map.

## Windows process tree (Job Object)

The supervisor does **not** rely on `taskkill /T` as the fundamental kill
contract. Each child is spawned `CREATE_SUSPENDED | CREATE_NO_WINDOW` into a
Job Object configured with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, assigned to
the job **before** it runs (no descendant can escape), then resumed
(Toolhelp primary-thread resume). Consequences:

- force kill = `TerminateJobObject` (one call kills the whole tree — no PID
  races, no process-table parsing);
- graceful hint stays console-aware (`taskkill /T` without `/F`, may fail for
  non-console children → escalation to force at the bounded deadline);
- closing the last job handle kills whatever remains (`KILL_ON_JOB_CLOSE`) —
  crash-safe orphan prevention;
- no console window flashes for CLI engines (`CREATE_NO_WINDOW`).
- Platform-specific code is isolated in `saiwork-process::platform`
  (`windows.rs` / `unix.rs`); all `unsafe` lives there with documented
  invariants.

Unix: the child is spawned with its own process group; graceful = SIGTERM to
the group, force = SIGKILL to the group.

## Timeouts (defaults, adjustable per `ProcessSpec`)

```text
graceful stop (exit_wait):  5 s
force kill (kill_timeout):  3 s
output drain after exit:    2 s (bounded tail capture)
final concurrent force proof: kill_timeout per process (3 s default)
```

Every wait is bounded; exhaustion returns a typed error, never a hang.

## Events

```text
process.started { process_id, pid }
process.exited  { process_id, pid, code, signaled }   ← after output drain
process.failed  { process_id, error }                 ← spawn/wait failure
```

`process.alive ≠ engine.ready`: engine adapters observe process lifecycle
events and map them to `engine.*` events at their own layer (TASK 07+).

## Error taxonomy (typed)

```text
InvalidSpec · DuplicateId · CommandNotFound · BadCwd · Spawn ·
Platform (job-assignment/resume failures, cause preserved) ·
NotRunning (idempotent stop of exited process) ·
TerminationTimeout · ShuttingDown (spawn rejected after shutdown started)
```

## Shutdown sequence (app close)

```text
1. block new dispatch (spawn rejected after mark_shutting_down)
2. queue shutdown barrier: stop new claims, release safe prepare leases
3. cancel app-owned background operations (incl. SAIPEN actions: reject new,
   request safe cancellation, bounded wait, force fallback — TASK 15 §67)
4. flush durable state
5. gracefully stop engines (queue coordinator observes run terminals)
6. supervisor.shutdown(): graceful stop → bounded concurrent force proof →
   discard proven exits; retain any live survivor records for teardown retry
7. queue finish_shutdown: bounded join of dispatcher/coordinator; leftover
   LEASED(sending) rows are intentionally left for startup recovery (ambiguous)
8. dispose watchers
9. dispose event subscriptions
10. close database
11. exit
```

SAIPEN actions on shutdown (TASK 15 §67–§68): `ActionManager::shutdown()`
rejects new actions and requests cancellation of active ones; the supervisor
force sweep is the final fallback. A force-killed mutating action is logged
as `mutation outcome uncertain` — canonical state is re-read on next launch;
SAIWORK2 never fabricates a rollback. On restart the action registry is
empty (in-memory only, §61) and SAIPEN is rediscovered fresh (§62).

Each step has a timeout; the sequence never hangs forever (law 13, 21).
`shutdown()` is idempotent and returns the ids whose exit remained unproven
after the final force pass. A later call retries every retained survivor.

Application wiring (TASK 08/13): `App::shutdown()` owns the overall sequence —
it calls `supervisor.mark_shutting_down()` (rejects new spawns), runs the
queue shutdown barrier (`shutdown_barrier`: stop new claims; active runs keep
streaming), stops engines (cancelling FakeEngine runs and pending
permissions), then `queue.finish_shutdown` (bounded join of dispatcher +
coordinator; any run still tracked past the drain window is force-failed
`engine_lost`; prepare-phase leases are released), then awaits
`supervisor.shutdown()`, then checkpoints storage. The supervisor remains
the **one** process authority: `App` never implements its own taskkill; it
reuses the supervisor's graceful → bounded wait → force path (§28). Leftover
`LEASED(sending)` rows survive restart recovery as `ambiguous_handoff` by
design (ADR-024) — shutdown never fabricates a completion.

## Guarantees (contract)

```text
- every child has exactly one owner: the ProcessSupervisor (law 6)
- every child has bounded, separately captured stdout/stderr (law 13)
- every child has a cleanup path: graceful → bounded wait → force kill (tree)
- shutdown blocks new spawns before stopping anything
- process-tree cleanup is enforced by OS ownership (Job Object / process
  group), not by fragile taskkill parsing
- an orphan child process after normal shutdown is a DEFECT (M0 gate: 0 orphans)
- process alive ≠ engine ready (readiness is the engine layer's probe)
- no automatic restart: unexpected exit is an event; policy is higher-layer
- no persistent PID authority: live processes are runtime state, never SQLite
```

## Force-escalation semantics (TASK 09, measured)

- Graceful means platform-aware best-effort: on Windows `taskkill /T /PID`
  (WM_CLOSE) — which **cannot** close a `CREATE_NO_WINDOW` child; on Unix
  SIGTERM to the group. The supervisor waits the full graceful budget
  (`exit_wait_timeout`, default 5 s), then escalates to force
  (`TerminateJobObject` / SIGKILL). Measured: the lifecycle test's
  windowless fixture takes exactly 5 006 ms to shut down — the graceful
  budget elapsed, then force landed instantly.
- `stop_all` returns ids that resisted its graceful→force sequence;
  `shutdown()` performs a fresh concurrent final force pass and returns only
  ids whose exit is still unproven after that pass. A routine escalation that
  succeeds is NOT a failure: the shutdown report stays `clean` and the final
  list stays empty. A force failure never causes `clear()`: the live record
  remains in ProcessSupervisor, preserving the single authority and allowing
  a later `shutdown()` final force pass to retry teardown. Normal escalation is
  observable via the elapsed graceful budget (see
  `forced_process_kill_does_not_skip_storage_checkpoint`, which also proves
  storage close still runs after an escalation — §37/§38 aggregation).

## No-double-supervision boundary (TASK 19, applies to engine-internal processes)

An engine adapter that owns an external agent runtime owns exactly **one** process level:

- **SAIWORK2 owns the top-level engine runtime process** (spawned through
  ProcessSupervisor with a typed ProcessSpec — explicit program/args/cwd/env, bounded
  stdio, job-object/taskkill-tree kill fallback). This is the only child SAIWORK2
  supervises for that engine.
- **The engine owns its internal agent/tool/subprocess lifecycle** (e.g. DeepSeek
  Harness `ctx.subprocess`/`ctx.jobs`/sandbox; OpenCode's own server children). SAIWORK2
  observes normalized tool/process events through the engine protocol; it never attaches
  ProcessSupervisor to inner shell/tool commands.

Rationale: attaching a second supervisor to the same process tree creates competing
termination/ownership authorities (double supervision), violates the one-authority rule,
and breaks on engines whose inner subprocesses are created and reaped inside the runtime.
This boundary is documented and implemented: the DeepSeek Harness adapter
(DEEPSEEK_HARNESS.md §7, §22–§23) owns exactly the top-level runtime process. The
TASK 21 vertical slice keeps this boundary: tool lifecycle arrives through the Harness
protocol (`session/update` `tool_call` → generic `tool.*`), never by supervising the
engine's inner shell/tool subprocesses (§54).

## Protocol-mode stdio children (TASK 20)

A long-lived interactive stdio protocol child (e.g. the Harness ACP runtime) uses two
generic extensions, both opt-in and default-off (OpenCode/FakeEngine/Generic CLI
unchanged):

- `StdinPolicy::Piped` — the child's stdin stays open; `ManagedProcess::stdin_write_all`
  serializes protocol writes (one writer owner) and `stdin_close` sends EOF. EOF is the
  protocol-level graceful shutdown for ACP-style connections, distinct from process
  termination.
- `ProcessSpec::stdout_protocol` — the stdout reader forwards **raw byte chunks** to a
  bounded channel (`ManagedProcess::protocol_stream`, taken once) with real
  backpressure, and still feeds the bounded lossy diagnostics ring. The protocol
  consumer owns framing; the supervisor never parses payloads.

Guarantees held: exactly one protocol reader per child; bounded channel (a slow consumer
stalls the child's stdout instead of unbounded buffering); protocol EOF is detected and
settles pending consumers; teardown drops the receiver to release backpressure.

## TASK 22 — process ownership audit (no change)

TASK 22 audited whether engine adapters bypass ProcessSupervisor or duplicate
short-process launch code (Candidate G). Result: no adapter bypasses the supervisor; the
no-double-supervision boundary holds (Harness tool lifecycle arrives via protocol, never
by supervising inner subprocesses). No `ManagedCommandRunner` was extracted — no real
duplication exists. The one TASK 22 cleanup fix (Harness `start()` partial-initialization
leak) strengthens the existing teardown path; it does not change process authority.
