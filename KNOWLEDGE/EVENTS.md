# EVENTS.md

## Envelope

Every event crosses every boundary (Rust bus, Tauri, frontend store) in one
normalized envelope:

```json
{
  "seq": 42,
  "ts": 1720000000000,
  "type": "message.delta",
  "payload": { ... }
}
```

`seq` is monotonically increasing per app run (allows the frontend to detect
gaps/drops and to order events). `ts` is wall-clock ms for display.

## Taxonomy

```text
app.started / app.stopping

workspace.opened / workspace.closed / workspace.changed

engine.starting / engine.ready / engine.stopping / engine.stopped
engine.failed / engine.health_changed

process.started / process.exited / process.failed

session.created / session.loaded / session.changed / session.closed

message.started / message.delta / message.completed / message.failed / message.cancelled

tool.started / tool.output / tool.completed / tool.failed

permission.requested / permission.resolved

question.asked / question.resolved
queue.changed / queue.dispatch_started / queue.dispatch_completed
queue.dispatch_failed

saipen.detected / saipen.changed / saipen.validation_changed

git.changed

runtime.warning / runtime.error

engine.raw_event        (debug-only, bounded, never user-facing)
```

## Payload rules

1. Canonical events never carry raw provider-specific payloads unnormalized.
2. `message.delta` carries only `{ session_id, run_id, delta }` — a string
   chunk, not a transcript rebuild.
3. `queue.changed` is emitted **only after** a successful state transition
   (donor lesson from SAIWORK QueueManager).
4. `engine.raw_event` is debug-only: bounded ring, redacted, disabled by
   default, never rendered as primary content.
5. Errors in payloads are `{ code, message }` with code from the error domains
   in ENGINE_CONTRACT.md.

## Typed identifiers

Event payloads use typed IDs (`saiwork-events::id`): `WorkspaceId`,
`EngineId`, `SessionId`, `RunId`, `MessageId`, `QueueItemId`, `RequestId`.
`WorkspaceId != SessionId != EngineId` at the type level — passing the wrong
kind is a compile error. IDs are opaque `Arc<str>` newtypes: cheap to clone,
`Send + Sync`, hashable/comparable, and serde-transparent (they serialize as
plain strings, so the Rust↔TS wire shape is unchanged). IDs are created by
core authorities only, never generated in the UI. No UUID framework is
used; the concrete textual form is an implementation detail of the
allocating authority.

## OpenCode adapter (TASK 10)

The `engine-opencode` adapter publishes **only** the canonical `engine.*`
events above (via `EngineRegistry`): `engine.starting` on spawn,
`engine.ready` on authenticated readiness, `engine.stopping`/`engine.stopped`
on stop, `engine.failed` on startup failure or unexpected process exit. It
never emits OpenCode-specific event names into the generic taxonomy
(TASK 10 §66). Process-level facts (`process.started/exited/failed`) remain
ProcessSupervisor's. Raw OpenCode stdout/stderr never floods the bus: the
supervisor's bounded rolling output retains it for diagnostics only, and
nothing is forwarded to the UI by default (§68). The adapter emits no events
at all while idle — no polling, no health timer (§109–§110, §124).

## Events are facts, not commands

An event records that something happened; it never requests an action.
`start_engine` is a command (Tauri invoke). `engine.starting / engine.ready /
engine.failed` are events — the record of what the command caused.

Consequences:
- UI sends commands; core publishes events. The UI never publishes events to
  move core state (law 18/23).
- Events may be dropped for lagging consumers (bounded bus); consumers
  reconcile from authoritative state. Commands are never dropped silently.
- Unknown provider events are isolated at the adapter boundary; they never
  enter the canonical taxonomy except as `engine.raw_event` (debug-only).

## Per-family contract

| Family | Producer | Consumer | Minimum payload | Ordering | Idempotence | Persistence |
| --- | --- | --- | --- | --- | --- | --- |
| app.* | core | UI, logs | reason/version | seq | app.started exactly once per process; app.stopping at most once | log only |
| workspace.* | WorkspaceManager | UI, session mgr | workspace_id (+path on opened) | seq | opened×2 → second refreshes (re-open) | SQLite workspaces (authoritative) |
| engine.* | registry + adapters | UI, queue dispatcher | engine_id (+error) | seq | ready after failed must be re-achieved via start | none (runtime) |
| process.* | ProcessSupervisor | engine adapters, diagnostics | process_id, pid (+code/signaled/error) | seq | exited×2 impossible (single terminal transition, monitored); started always precedes exited for one id | none (runtime; exited records leave the registry) |
| session.* | SessionManager | UI | session_id (+engine_id) | seq | closed for unknown id → no-op | sessions_meta (metadata only) |
| message.* | engine adapter | UI, running-tracker | session_id, run_id (+delta/error) | per-run total order; deltas never reorder | deltas may be re-delivered → appending a duplicate delta must be tolerated (dedup by run seq); **exactly one terminal per run** (completed/failed/cancelled mutually exclusive, asserted) | none (transcript is engine-owned) |
| tool.* | engine adapter | UI | session_id, tool (+output/error) | per-run | duplicate output append tolerated | none |
| permission.* | engine adapter | UI, permission gate | session_id, request_id (+detail/allowed) | per-run | resolve×2 → second is no-op | none |
| question.* | engine adapter | UI, question gate | session_id, request_id (+detail) | per-run | resolve×2 → second is no-op; answered OR rejected (typed QuestionResolution, AUDIT-CORE-002) | none |
| queue.* | QueueManager | UI | item_id, state (+error) | seq | emitted only after committed transition; dispatch_* around engine handoff and run terminal | SQLite queue_items (authoritative) |
| saipen.* | SaipenClient | UI | workspace_id (+valid) | seq | changed during storm → coalesce | .saipen files (authoritative) |
| git.changed | core/watcher | UI | workspace_id | seq | — | none |
| runtime.* | any core module | UI, diagnostics | code, message | seq | — | diagnostics ring |
| engine.raw_event | engine adapter | debug tooling only | engine_id, kind, payload (redacted, bounded) | seq | — | none |

## Semantic classification (TASK 22 — durable vs live vs delta vs invalidation)

Evidence base: DeepSeek Harness integration (DEEPSEEK_HARNESS.md §39–§40) forced an
explicit classification of upstream data into durable session facts vs live runtime
effects. The EventBus is runtime fact distribution, never a database (§30): no event
is replayed at startup, no event is persisted here, and durable reconstruction stays
with the domain authority (reconstruction table below). The `EventClass` enum
(`State`/`Stream`/`Diagnostic`) expresses delivery policy; the durable/live/invalidation
dimensions are documented here per family.

Four semantic categories (§31):
- **DURABLE AUTHORITY** — the domain's source of truth (SQLite queue/settings, SAIPEN
  canonical files, engine-owned session history). Events announce transitions of it;
  they never replace it.
- **LIVE FACT** — committed runtime state (engine ready, run terminal, permission
  pending). Reconstructable from a snapshot (registry/process truth/engine session);
  not persisted by SAIWORK2.
- **STREAM DELTA** — high-frequency content (`message.delta`, `tool.output`).
  Batchable/coalescible; reconstructable from the engine session where supported;
  never from EventBus replay.
- **INVALIDATION HINT** — "source may need refresh" (`git.changed`, `saipen.changed`
  storm). The consumer refetches the authoritative snapshot; no raw fs-event pollution
  on the bus (§35).

Reconstruction contract (§36):

| Domain | Durable authority | Event class | Droppable / coalescible? | Terminal? |
| --- | --- | --- | --- | --- |
| queue.* | SQLite `queue_items` | State (live fact) | not safely droppable if a consumer relies on events alone, but the UI can always resnapshot | queue.dispatch_* are around-handoff markers, not terminal; `unknown` is a blocked state resolved by user action (TASK 23) |
| saipen.* | `.saipen` canonical files | State (live fact) + invalidation hint on `changed` | `changed` coalesces during storms | action_* are around-action markers |
| engine.* / process.* | runtime registry / ProcessSupervisor truth | State (live fact) | resnapshot from `list_engines`/diagnostics | engine.failed is terminal until a new start |
| session.* | SessionManager metadata | State (live fact) | resnapshot from `list_sessions` | session.closed is terminal for that id |
| message.* | engine-owned session (Harness session log / OpenCode authority) | State (terminal facts) + Stream (delta) | deltas batch/coalesce; **terminals must not be dropped** (exactly-one-terminal CAS) | completed/failed/cancelled are terminal per run |
| tool.* | engine-owned session | Stream (output) + State (terminal) | output coalesces; one terminal per ToolCallId | tool.completed/failed are terminal per tool |
| permission.* | engine-owned pending state | State (live fact) | **must not be dropped** (fail-closed on loss) | permission.resolved is terminal per request |
| question.* | engine-owned pending state | State (live fact) | **must not be dropped** (fail-closed on loss); reconstructable via pending_questions | question.resolved is terminal per request |
| workspace.* / git.changed | SQLite workspaces / watcher | State + invalidation hint | `changed` coalesces | — |
| runtime.* | diagnostics ring | Diagnostic | never recurses | — |
| engine.raw_event | none (debug only) | Stream | droppable | — |

Payload rule: events never carry raw provider payloads unnormalized (law 3);
errors are `{ code, message }` from the ENGINE_CONTRACT.md domains.

## Lifecycle semantics (TASK 08)

- `app.started` is the record of reaching **READY** — required foundation
  services initialized — never "main() ran". It is published after storage,
  EventBus and ProcessSupervisor exist (TASK 08 §49).
- `app.stopping` is published **first** in the shutdown sequence, before any
  service stops, so every consumer can observe the barrier and wind down
  (TASK 08 §30). The running-tracker task ends when it observes
  `app.stopping`; the bus stays open until the last sender drops at process
  exit. `app.started` after `app.stopping` is impossible (asserted).
- No `app.stopped` event: after cleanup there are no consumers left, and
  adding it for symmetry would be noise (TASK 08 §31). The terminal fact is
  the STOPPED state in the diagnostics snapshot.

## Frontend consumption (TASK 16)

- `message.delta` events are **batched on the frontend**: they accumulate in
  the store and flush once per ~16 ms frame; `message.completed|failed|
  cancelled` flush pending deltas first so the final text never loses its
  tail (§23). Batching is a UI concern — the event stream itself is
  unchanged.
- Streaming noise (`message.delta`, `tool.output`, `engine.raw_event`) never
  enters the diagnostics log, so a token does not rerender the whole app
  (§241). Meaningful transitions (started/completed/failed/cancelled,
  engine/session/queue/saipen facts, warnings/errors) do.
- `permission.requested` is resolved via the typed backend command
  `resolve_permission(session_id, request_id, allowed)`; the engine's
  authoritative `permission.resolved` updates the projection. Pending
  permissions on a dead run are released by the engine (drop/stop/crash) —
  the UI buttons simply disappear (§38).
- `question.asked` is resolved via the typed backend command
  `resolve_question(session_id, request_id, answers|null)` — answers carry one
  selected option label per asked question; `null` is an authoritative
  reject. Questions are NEVER forced through boolean permission semantics
  (AUDIT-CORE-002). The engine's authoritative `question.resolved` removes the
  card; pending questions on a dead run are released by the engine, and the
  bounded `pending_questions()` snapshot reconstructs missed events exactly
  like permissions.

## Delivery contract (implementation, TASK 04)

- **Model:** asynchronous, fire-and-forget broadcast over a bounded
  `tokio::sync::broadcast` channel. `publish` never blocks and never grows
  memory; the producer is never blocked by a slow consumer.
- **Identity:** `seq` is the per-run monotonic event identity (unique per app
  run; resets across restarts). There is no separate `event_id` — the seq IS
  the identity. Timestamps are UTC epoch milliseconds.
- **Ordering:** the bus assigns a global monotonic seq per publish. Events
  from one producer are delivered to a subscriber in emission order; after
  a `Lagged(n)` the next delivered event resumes at the first missed seq
  (contiguous accounting). No stronger total order is promised across
  producers.
- **Backpressure:** fixed capacity (default 1024, floored at 16). When a
  subscriber cannot keep up it observes `Lagged(n)` with the explicit missed
  count; the subscriber MUST reconcile from authoritative state. Drops are
  never silent — a lagging subscriber always learns how many events it
  missed. State events (`engine.ready`, `queue.*`) are announcements of
  already-committed authoritative state, so reconciliation is always
  possible; no event replay is offered (the bus is not a history store).
- **Event classes:** every event carries a semantic `class()`:
  `State` (authoritative facts), `Stream` (high-frequency content deltas:
  `message.delta`, `tool.output`, `engine.raw_event`), `Diagnostic`
  (`runtime.*`). The UI bridge may batch/coalesce Stream-class render
  updates without changing order; State-class events are never coalesced
  away.
- **Subscriber failure:** there is no callback API — subscribers hold polled
  handles (`Subscription`), so a slow or panicking consumer cannot poison
  other consumers or the bus. The bus never holds a lock across delivery.
- **Reentrancy:** publishing from inside a consumer task is safe by design
  (no lock held during delivery); tested.
- **Subscription lifecycle:** `Subscription` is the owned handle; `cancel()`
  is explicit, `Drop` unsubscribes automatically. Repeated subscribe/drop
  must return the bus to its baseline receiver count (tested, no listener
  multiplication).

## Consumer failure and error recursion (TASK 09 §21–§22, tested)

- **Consumer isolation:** subscribers are **polled handles**, not callbacks —
  the bus never holds a lock across delivery and there is no callback
  registry to corrupt. A subscriber that stops polling or panics cannot
  affect other subscribers or the bus (tested: `failing_consumer…`,
  `panicking_consumer…`). A panicking consumer propagates to its owner via
  the task handle; the bus keeps delivering.
- **No recursion:** publishing a `runtime.error`/`runtime.warning` is a
  plain channel send — it cannot trigger another publish (tested:
  `diagnostic_publish_never_recurses`: N publishes → exactly N events, no
  subscription growth). There is no feedback path that could storm.

## Declared vs implemented (TASK 09 §51)

Events are declared for the full roadmap taxonomy, and these are currently
emitted/consumed by implemented subsystems: `app.*`, `engine.*`,
`process.*`, `session.*`, `message.*`, `tool.*`, `permission.*`,
`workspace.*`, `queue.*` (TASK 13), `saipen.*` (TASK 14), `runtime.*`,
`engine.raw_event` (FakeEngine debug). `git.changed` remains **declared, not
yet implemented** and must not be treated as implemented until its task
lands (TASK 16).

## SAIPEN events (TASK 14 + TASK 15)

- `saipen.detected` — the **transition** NotPresent → Present (§52), owned
  by the SaipenService (emitted on attach, not on every startup reread).
- `saipen.changed` — emitted only when the normalized snapshot meaningfully
  changed (§53–§54): unchanged saves are suppressed (semantic equality
  ignores read timing/generation, §167). A failed refresh keeps the last
  good snapshot marked `stale` and emits once.
- `saipen.validation_changed` — **not emitted**; validation status is a
  derived projection (result + snapshot generation) fetched via
  `saipen_action_status` (§102, §87).
- `saipen.action_started` / `saipen.action_completed` / `saipen.action_failed`
  / `saipen.action_cancelled` (TASK 15 §58–§60) — payload is
  `{ workspace_id, action_id, kind, result|error }` only; **never stdout**
  (§59). `action_started` fires only after the backend accepted the action;
  each action emits exactly one terminal. Filesystem `saipen.changed` may
  interleave independently (§37, §60).
- `runtime.warning` codes: `SAIPEN_INVALID`, `SAIPEN_UNSUPPORTED`,
  `SAIPEN_READ_FAILED` — typed, distinct from NotPresent (absence is a
  normal state, never a warning).
- Events carry `workspace_id` only; the full snapshot is fetched via
  `get_saipen`, action records via `saipen_action_status`. Raw filesystem
  events never cross the bus (§144).

## Queue events (TASK 13)

- `queue.changed { item_id, state }` — emitted after every committed SQLite
transition (QUEUED/LEASED/DISPATCHED/DONE/FAILED/CANCELLED).
- `queue.dispatch_started { item_id }` — after `DISPATCHED` commit (run_id
  associated) and before/at run tracking.
- `queue.dispatch_completed { item_id }` — after the authoritative run
  terminal moved the item to DONE.
- `queue.dispatch_failed { item_id, error }` — after an authoritative
  terminal moved the item to FAILED (error code from the queue domain:
  `run_failed`, `engine_lost`, `ambiguous_handoff`, `session_not_found`, …).
- Queue events never carry payload text; a lagging consumer reconciles via
  `queue_snapshot` (SQLite is authoritative). The coordinator ignores
  `message.*` events for untracked run ids (direct sends, external
  activity) — old run callbacks cannot mutate a newer attempt (run_id CAS).

## Streaming pipeline

```
engine stream → adapter parser → normalized delta → small batching window
(16–33 ms target, measured not religious) → UI update
```

No full transcript reparse per token, no full session serialization per delta,
no global app rerender per token (PERFORMANCE.md).

## TASK 17 — no new event types

The additional-engine work adds **no new canonical event types**: the generic
CLI adapter publishes the existing `message.started → message.delta →
message.completed | failed | cancelled` surface and the supervisor's
`process.*` events. `StdinPolicy::Bytes` and the per-process output cap are
supervisor-internal; the frontend consumes the same typed events for every
engine, so no per-engine event handling exists anywhere (§142 cross-engine
event isolation is enforced by engine-scoped session/run ids, not by event
shapes).

## TASK 20 — DeepSeek Harness foundation: generic lifecycle events only

The Harness adapter emits **no new event types** — only the existing generic engine
lifecycle/health facts (`engine.starting/ready/stopped/failed` via the registry, and
`process.*` from the supervisor for the top-level runtime). Raw ACP/JSON-RPC messages are
adapter-local (bounded debug logs only) and are deliberately NOT global application
facts (TASK 20 §68). `engine.raw_event` is not used by this adapter in the foundation.

## TASK 21 — DeepSeek Harness vertical slice: normalized semantic events

The Harness adapter now normalizes the ACP agent stream onto the **existing generic
event surface** — still no new event types, still no raw envelopes on the bus
(DEEPSEEK_HARNESS.md §23):

- `session/update` `agent_message_chunk` (committed text) → `message.delta` (incremental;
  one canonical MessageId per upstream message, never one message per chunk).
- `session/update` `tool_call` → `tool.started` / `tool.output` / `tool.completed` /
  `tool.failed` (exactly one terminal per ToolCallId; bounded output).
- `session/request_permission` → `permission.requested` (bounded safe detail) →
  `resolve_permission` Allow/Deny → authoritative `permission.resolved`; fail-closed on
  no decision (reject, never default allow).
- Terminal: `message.completed` (stop reason `end_turn`) / `message.cancelled`
  (`cancelled`/`discarded`) / `message.failed` (anything else) — exactly one terminal per
  run (CAS-gated); `HARNESS_STREAM_OVERFLOW` runtime warning on frame drop.

Classification (DEEPSEEK_HARNESS.md §39–§40): `agent_message_chunk` is the **live**
committed-chunk stream; durable session-log facts (turn/step/session-log) are NOT on the
ACP wire and are never fabricated or mirrored. Unknown update kinds are ignored — not
every Harness internal fact becomes a public event (§97). Events are routed by the stable
upstream session id, never by selection; events for sessions without an active run are
ignored (external activity); events after a terminal are discarded.
