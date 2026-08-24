# QUEUE.md

The queue is a first-class, durable subsystem. **SQLite is authoritative.**
The UI queue is a projection, never a second authority (law 5, 7). There is
exactly one `QueueManager` authority (`saiwork-queue`, TASK 13); direct
user sends from TASK 11 still exist, but queued work is dispatched **only**
through QueueManager.

## State machine (TASK 13 + TASK 23)

```text
QUEUED ──claim (atomic)──▶ LEASED(prepare) ──begin_send──▶ LEASED(sending)
   ──send accepted──▶ DISPATCHED(run_id, attempt++) ──terminal──▶ DONE | FAILED | CANCELLED
   ──crash / restart──▶ UNKNOWN (outcome unprovable; blocks workspace; user-resolvable)
```

- `LEASED` carries an explicit `dispatch_phase`: `prepare` (no external side
  effect exists yet — a crash here recovers to QUEUED without loss) or
  `sending` (the engine may have accepted the send — a crash here is
  **ambiguous**). Phase is committed durably **before** the engine call.
- `DISPATCHED` stores the engine-accepted `run_id` (dispatch correlation,
  §24). The coordinator transitions DISPATCHED → terminal only on an
  authoritative run event guarded by `state='dispatched' AND run_id=?`, so a
  duplicate/stale terminal cannot double-apply (§174–§178).
- Terminal states: `DONE`, `FAILED`, `CANCELLED` — exactly one per run.
  A later terminal event never mutates the final truth.
- **`UNKNOWN` (TASK 23):** a first-class durable state meaning SAIWORK2
  cannot prove whether the external work executed — a crash during the
  `sending` handoff, or a DISPATCHED run whose engine authority is
  unrecoverable after an app restart. It is **never auto-dispatched**, it
  **blocks further mutating queued dispatch in its workspace** (§50), and it
  is resolved only by explicit user action: `retry` (new attempt, risk
  acknowledged), `cancel` (abandon — never claims the work did not run), or
  an externally discovered authoritative terminal. It is distinct from
  `FAILED`, which asserts the attempt failed.
- `cancel_requested` is a durable intent flag honored by the worker at every
  handoff step (before claim effect, before send, after mark_dispatched) —
  one cancellation owner, no abort storm (§63).

Failure paths (explicit, never silent):

```text
LEASED(prepare)  ──engine not ready / session busy / shutdown──▶ QUEUED   (release; no side effect)
LEASED(prepare)  ──session_not_found──▶ FAILED
LEASED(sending)  ──send error / crash──▶ UNKNOWN (ambiguous: NEVER auto-requeued)
DISPATCHED       ──run failed / engine lost──▶ FAILED
DISPATCHED       ──app restart (authority unrecoverable)──▶ UNKNOWN
UNKNOWN          ──user retry (risk acknowledged)──▶ QUEUED (new attempt)
UNKNOWN          ──user cancel──▶ CANCELLED
QUEUED           ──user cancel──▶ CANCELLED
LEASED/DISPATCHED──user cancel──▶ CANCELLED (via intent → engine cancel → terminal)
```

## Dispatch boundary and exactly-once truth (§23–§27, §84–§86)

- **Local claim guarantee:** atomic `UPDATE ... WHERE state='queued'` — exactly
  one lease owner per item, proven by the concurrent-claim test.
- **External effect guarantee:** SAIWORK2 cannot prove exactly-once external
  engine side effects across the crash boundary (OpenCode has no idempotency
  key; the generic CLI has local process-start evidence but a tool may have
  written files before dying — TASK 17 §135–§137). This is documented, not
  papered over.
- **Cross-engine targeting (TASK 17):** the durable `engine_id` on each item
  is the only dispatch authority. OpenCode items dispatch OpenCode, CLI
  items dispatch the CLI adapter, and a stale UI engine selection never
  retargets a durable item (dispatch routes through `EnginePort` +
  SessionManager, which resolve the engine from the item's session —
  proven by `cross_engine.rs`: no-fallback and engine-unavailable tests).
  An item targeting an unavailable engine stays durable under the FIFO/
  eligibility policy; there is no automatic fallback engine (§22–§23).
- **Ambiguous handoff policy (TASK 23):** an item found in `LEASED(sending)`
  at startup is marked **`UNKNOWN`** (code `dispatch_unknown`) — outcome
  unprovable, **never automatically re-dispatched**, workspace blocked. One
  item awaiting explicit resolution beats two agents editing the same
  repository. Manual retry is available with explicit duplication-risk
  acknowledgment (§20).
- **Harness as a durable target (TASK 23):** the queue dispatches to
  `deepseek-harness` through the same generic `EnginePort` + SessionManager
  path as every engine — the queue never knows ACP/Harness protocol details
  (verified statically). Acceptance evidence is the `session/prompt` send
  returning a run handle; the Harness session id is persisted in
  `session_id` and the SAIWORK2 run id in `run_id` (ACP exposes no durable
  TurnId and no idempotency key — the session is the correlation unit, and
  across an app restart the ACP session is connection-owned and unrecoverable
  → honest `UNKNOWN`). Cancellation maps to Harness `session/cancel` (never
  an engine kill). No engine fallback exists.
- **No wall-clock-only ownership:** ownership is the lease token + startup
  recovery; a slow legitimate engine request is never requeued by a timer.
  No SQLite transaction is held across a network call — the handoff is a
  sequence of short atomic transitions.

## Startup recovery (before dispatch, §75–§77; TASK 23 §28–§31)

```text
storage open/migrate → QueueManager::init → recover stale state → spawn worker
```

- `LEASED(prepare)` from a prior lifetime → `QUEUED` (preserving attempt
  metadata where useful). No prompt loss.
- `LEASED(sending)` → **`UNKNOWN`** (`dispatch_unknown`) — the send may have
  crossed the boundary; never auto-redispatched, workspace blocked.
- `DISPATCHED` → **`UNKNOWN`**: no engine in this baseline can reconcile a
  dispatched run after an app restart (Harness ACP sessions are
  connection-owned; OpenCode/Fake run registries are in-memory), so the
  outcome cannot be proven. The old `run_id`/`session_id` correlation stays
  visible for the user; the item is never presented as a live run and never
  resent. This is the honest fallback TASK 23 §28–§31/§137 requires.
- Recovery completes **before** dispatch is enabled (init returns before the
  worker spawns). A `QUEUE_OUTCOME_UNKNOWN` runtime warning is published for
  every unknown item.

## Ordering, revision, edit/reorder/delete

- Deterministic `order_key` (insertion order; transactional renumbering on
  reorder — crash mid-reorder cannot produce duplicate/missing positions).
- Dispatch eligibility walks `(order_key, created_at, id)` with fixed
  128-row keyset pages backed by the v8 composite index. Only one page is
  materialized at a time; draining N ready rows never rebuilds the entire
  remaining queue on every claim, and a blocked first page cannot hide a
  later eligible workspace.
- Every user-visible mutation carries `expected_revision` (CAS):
  `UPDATE ... WHERE id=? AND revision=?`. Revision conflicts surface a typed
  `Conflict` — no silent last-write-wins.
- QUEUED → editable/reorderable/deletable. LEASED/DISPATCHED payload is
  locked; user intent routes through cancel (engine cancel → authoritative
  terminal), never a raw DELETE racing the dispatcher.
- Delete of a QUEUED item is a CAS to terminal `CANCELLED` (evidence kept;
  bulk cleanup policy is deferred, no indefinite unbounded growth).

## Pause / retry / fail-closed

- Pause is durable (SQLite flag), survives restart; it gates the claim
  atomically (a claim racing a pause commit resolves deterministically — a
  claim may only begin before the pause commit). Pause does not cancel an
  active run; it is a future-dispatch barrier.
- Retry categories: safe automatic retry = lease recovered before acceptance
  (prepare phase). **Never** automatic: ambiguous handoff, provider side
  effects then disconnect, unknown execution state. Manual `retry` is an
  explicit user act (`FAILED` → QUEUED or `UNKNOWN` → QUEUED, CAS-guarded; a
  retried UNKNOWN item keeps its ambiguous evidence and the UI acknowledges
  duplication risk — TASK 23 §20); attempts are counted per actual dispatch,
  not per eligibility scan.
- **Workspace ambiguity gate (TASK 23 §50–§51):** an `UNKNOWN` item blocks
  further queued mutating dispatch in its workspace (the unknown old run may
  have mutated the same files). Other workspaces proceed independently.
- Fail-closed: any durability failure sets `QueueStatus::Failed`, stops new
  dispatch, publishes `runtime.error`; recovery requires restart. No
  in-memory fallback queue exists.

## Worker model

- One owned dispatcher task + one coordinator task (**concurrency = 1**,
  §56–§57; confirmed as the V1 release boundary, ADR-038 / TASK 18 §15–§16).
  No parallel multi-agent scheduler. The dispatcher waits for the active
  run's terminal before the next claim.
- **Workspace gate (TASK 18):** `session_busy` is workspace-aware — an item
  targeting a session in a workspace with an active run in *another* session
  WAITs (never claims-then-fails), and `resolve_session` New-mode checks
  busy after creating the session, before the send (§20). A direct send to
  the same workspace is rejected with the typed `WorkspaceBusy` error.
- Event-driven: `tokio::sync::Notify` (permit semantics — lost-wakeup-safe)
  plus bus events (`engine.ready`, `session.changed`, run terminals) plus a
  bounded 5 s backstop re-scan (ADR-008 backstop — a safety net, not
  polling). No per-item timers, no 100 ms scan loop.
- Engine-availability/session-busy is a derived wait condition: the item
  stays QUEUED and becomes eligible again on the next wake — no repeated
  lease/fail churn, no prompt loss while the engine is stopped.

## Invariants (gate, TASK 13 §250)

1. A durably accepted queued item is never silently lost.
2. Two local dispatchers cannot claim the same item.
3. A stale UI cannot overwrite a newer queue mutation (revision CAS).
4. Queue order and pause state survive restart.
5. A crash before confirmed external dispatch safely restores work.
6. A crash after possible external acceptance never triggers blind duplicate
   execution (ambiguous → UNKNOWN, workspace blocked, manual resolution).
7. An engine failure cannot erase queue intent (item stays FAILED for retry).
8. A storage failure stops new dispatch (fail-closed), never degrades into
   volatile behavior.
9. A completed run moves exactly its associated attempt to terminal
   (run_id-guarded transition).
10. Old run callbacks cannot complete a newer retry (run_id CAS + index
    removal).
11. Shutdown leaves queue state recoverable (barrier → bounded drain →
    prepare-lease release → startup recovery handles the rest).
12. Idle queue consumes essentially no CPU (Notify sleep + 5 s backstop).

## Schema (`saiwork-storage`)

`queue_items` (v1: id, workspace_id, engine_id, session_id, payload, state,
order_key, lease_id, leased_at, attempt_count, created_at, updated_at,
last_error). Migration v2 adds: `revision`, `model`, `session_mode`,
`dispatch_phase`, `run_id`, `cancel_requested`, `last_error_code` and a
`run_id` index. Migration v8 adds the demonstrated dispatch keyset index
`(state, order_key, created_at, id)`; it changes no row semantics. Every
column has a consumer. No full transcript, no engine process state, no
provider configuration is mirrored (law 25).

## Events

`queue.changed` (item_id, state) after **every** committed transition;
`queue.dispatch_started` / `queue.dispatch_completed` / `queue.dispatch_failed`
around the engine handoff and run terminal; `runtime.warning`
(`QUEUE_OUTCOME_UNKNOWN`) for recovery ambiguity / unknown execution;
`runtime.error` (fail-closed). Events are announcements of
already-committed SQLite truth — a lagging UI reconciles via
`queue_snapshot`. Prompt text never appears in events or logs.

## SAIPEN ↔ Queue relation (TASK 15 + TASK 23)

**Not wired — explicitly DEFERRED (TASK 23 §73–§84, ADR-043).** There is no
canonical `saipen continue` CLI in the verified contract that could produce
agent work, and no stable execution/transition identity exists for
SAIPEN-produced work (TASK 15 §30–§33, §69–§73; TASK 23 §73). The queue
remains the SAIWORK2 authority for engine work; SAIPEN state is a separate
authority. If a future canonical handoff exists, it must flow
`SAIPEN action → durable QueueManager enqueue → normal dispatch` with an
idempotent correlation token — never a direct
`SaipenClient → OpenCode.send` bypass (§32, §74). Until a safe contract
exists, Continue stays a manual canonical action (§73).
