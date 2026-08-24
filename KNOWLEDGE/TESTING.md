# TESTING.md

Test contracts, not just happy paths. The historical regression backlog
(REGRESSION_BACKLOG.md) is the source of truth for donor-derived fixtures:
every row becomes a test when its target subsystem exists (law 24).
`cargo test` + `npm test` is NOT sufficient proof for a desktop app —
packaged smoke tests are separate.

## Groups

### 1. Unit
queue transitions; path boundaries; parser; state reducer; engine capability
mapping; data root; lease recovery; error classification; secret redaction;
bounded ring/backlog behavior.

### 2. Integration
SQLite + migrations (reopen/durability, transaction commit/rollback,
concurrent connections, busy policy); ProcessSupervisor with real child
processes; OpenCode lifecycle; EventBus ordering/lag; watcher; SAIPEN read;
queue dispatch.

Storage concurrency tests use two `Db`/connections on one file (the
in-process equivalent of two processes): WAL read-during-write, competing
writes, and the busy-wait-release path are covered in `saiwork-storage`
(TASK 05).

Process tests (TASK 06, `cargo test -p saiwork-process`, 19 tests) drive a
deterministic in-repo fixture binary (`proc_fixture`, located via
`CARGO_BIN_EXE_proc_fixture`) — never random external programs. The tree
test spawns a parent that spawns a sleeping child, force-kills the parent's
tree, and asserts the descendant PID is gone (0-orphan gate foundation).

### 2b. FakeEngine contract suite (TASK 07, `cargo test -p engine-fake`, 39 tests)

Deterministic in-process engine (ADR-009/017) with a small harness
(`Harness`: start → create session → collect events → wait-for-event →
assert terminal → dispose). No network, no credentials, no SQLite, no random
timing (test-time delays are short and fixed; hostile tests carry bounded
timeouts). Covers: lifecycle (start/stop/restart, start-twice, stop-twice,
start-failure, startup-hang cancelled by stop), sessions (create/list/resume,
unknown-session errors, multiple sessions isolated), streaming (empty,
single, large delta, slow, burst, 10k large stream), cancellation (mid-stream,
double cancel, cancel-after-complete, cancel-under-event-pressure,
cancel/completion race — exactly one terminal), failure (mid-stream failure,
hang terminated by cancel, engine crash failing all active runs), tools
(success/failure/interleave with text), permissions (allow/deny, cancel
while pending, engine stop while pending), hostile raw frames at the
`pushRaw` boundary (malformed → contained + stream continues, duplicate,
out-of-order → rejected not reordered, unknown → ignored with diagnostic),
same-session parallel run isolation, slow-consumer/backpressure (producer
never blocks), subscriber removal mid-run, dispose hygiene (0 active runs /
0 pending permissions / 0 background tasks).

Historical regression mapping (TASK 01 backlog → now covered):

```text
malformed/truncated stream        → pushRaw malformed-frame tests
                     duplicate events  → duplicate raw frame + duplicate-delta tolerance
dispatch precursor behavior       → run/engine failure isolation (run fail ≠ engine fail)
engine crash / connection loss    → engine_crash_fails_run_and_rejects_new_sends;
                                   engine_crash_terminates_other_active_runs
hang                              → hang_run_terminates_on_cancel; startup hang
listener/resource leak            → dispose_releases_runs_permissions_and_tasks;
                                   subscriber_removal_does_not_affect_run
                                   (subscription count back to baseline)
event storm                       → large_stream_completes_without_deadlock (10k);
                                   cancel_under_event_pressure
```

### 2c. Application lifecycle suite (TASK 08/09, `cargo test -p saiwork2 --test lifecycle`)

Desktop-shell-level integration (`apps/desktop/src-tauri/tests/lifecycle.rs`,
8 tests) drives the real `App` runtime with FakeEngine registered and a real
managed process fixture — no mocks of the lifecycle:

```text
boot_ready_shutdown_stopped_with_coherent_event_order  app.started … app.stopping,
                                                        never reversed, no event after
                                                        STOPPED; tracker subscription ends
fake_engine_active_run_is_cleaned_on_shutdown           FakeEngine /sim:hang run → shutdown
                                                        (runs 0, tasks 0, permissions 0)
managed_process_is_cleaned_on_app_shutdown              supervisor spawn → shutdown → fixture
                                                        gone, supervisor empty
storage_startup_failure_is_fail_closed                  injected DB failure → not READY,
                                                        typed error, recovery start works
corrupt_database_fails_boot_without_deletion            garbage DB → typed error, file never
                                                        deleted or rewritten
durable_state_survives_app_restart                      write meta → shutdown → restart → read
forced_process_kill_does_not_skip_storage_checkpoint    (§38 aggregation) fixture with no
                                                        window forces the graceful budget
                                                        (≥5 s elapsed on Windows) → force
                                                        kill → outcome still clean AND storage
                                                        checkpoint ran (durable write survives)
storage_busy_during_shutdown_is_bounded_and_clean       (§76) external BEGIN IMMEDIATE during
                                                        shutdown → wal_checkpoint fails fast
                                                        with typed Busy (it does not run the
                                                        busy handler), shutdown ends STOPPED
                                                        with a coherent outcome; WAL keeps the
                                                        pre-shutdown write durable
```

Plus `saiwork-core` unit tests (state machine transitions, shutdown from
BOOTING, double-shutdown idempotence, `require_ready` guards,
`concurrent_shutdown_requests_run_exactly_one_sequence` — 8 concurrent
callers, exactly one `clean` outcome, 7 observers; `no_lifecycle_events_after_app_stopping`)
and `saiwork-events` hostile tests (17 total):

```text
failing_consumer_does_not_affect_other_subscribers    stalled consumer costs others nothing
panicking_consumer_does_not_poison_the_bus            panic in one task can't poison the bus
                                                       (subscribers are polled handles)
diagnostic_publish_never_recurses                     publishing runtime.error emits exactly
                                                       one event — no recursion storm
```

### 2d. TASK 09 — known upstream blocker on the desktop lib harness

The desktop crate's **lib unit-test harness is disabled** (`test = false`,
`doctest = false` in `apps/desktop/src-tauri/Cargo.toml`): on Windows a test
binary that links the Tauri/WebView2 stack fails to *start* with
`0xC0000139 STATUS_ENTRYPOINT_NOT_FOUND` even with `WebView2Loader.dll`
beside it (verified: LoadLibrary binds the PE; CreateProcess does not;
import table is a strict subset of the working main exe). This is the known,
still-open upstream bug tauri-apps/tauri#14580 — not a SAIWORK2 defect. The
lib is pure Tauri wiring with zero unit tests; all real tests run as
integration tests (`tests/`) or in `saiwork-core`/`saiwork-events`.
`cargo test --workspace` is fully green with this configuration.

### 3. Hostile (FakeEngine-driven where possible)
```text
malformed SSE / malformed event
duplicate event
truncated event / truncated stream
process disappears
process refuses shutdown
database locked
corrupt state
inaccessible workspace
workspace removed while running
stale lease
restart during dispatch
rapid workspace switching
rapid start-stop
repeated reconnect
large transcript (10k deltas)
event storm
crash between claim and send
```

### 4. Recovery
Startup recovery order (QUEUE.md); stale lease sweep; restart mid-dispatch;
crash between claim and send; orphan sweep with process identity; corrupt
persistence fail-closed; stale temp cleanup; deterministic behavior after
crash (law 20).

### 5. Packaged smoke
Release build on Windows: starts → opens workspace → streams via FakeEngine →
cancels → exits with **0 orphan children**. Never replaced by dev-mode runs.

TASK 08 dev-mode smoke evidence (debug build, Windows): primary launch →
READY with storage + WAL; second instance relays intent and exits without
touching the DB or log (single authority remains, §66); graceful WM_CLOSE
→ canonical `Ready → ShuttingDown → Stopped`, clean, process gone (§69–§70);
rapid relaunch on the existing DB reopens at schema 1 with no re-migration
(§68); portable mode (`portable.flag` beside the exe, launched from a
different CWD) writes to the exe-adjacent data root and closes cleanly
(§103).

TASK 09 automated runtime torture (debug build, Windows 10 x64, Git Bash):
`bash scripts/torture.sh target/debug/saiwork2.exe` — a single command
covering rapid launch→READY→close cycles, single-instance stress (one
primary + N secondaries, all secondaries exit, primary log untouched),
force-kill crash + relaunch (DB byte-identical, no re-migration), portable
mode from a foreign CWD, and explicit `SAIWORK2_DATA_DIR` precedence over
`portable.flag`. All phases passed on 2026-08-16 (re-verified in-session);
rerun it after any lifecycle/desktop change.

Canonical Phase 0 gate (§100): `npm run test:phase0` (bash
`scripts/phase0.sh`) runs fmt --check → clippy --all-targets → workspace
tests → frontend typecheck → frontend build in one command, stopping at the
first failure; `--runtime` additionally runs the torture script. Packaged
release smoke and Windows path/read-only-location checks remain manual and
are never hidden inside this command.

### 2e. OpenCode adapter suites (TASK 10, `cargo test -p engine-opencode`, 32 tests)

Two strictly separated suites (§98) — fixture lifecycle vs real compatibility:

```text
tests/hostile.rs (27 tests, fixture-driven, always runs)
  executable missing → ExecutableNotFound, no fallback
  wrong executable (unrelated binary) → probe rejects it
  invalid workspace → InvalidWorkspace before any spawn
  port collision (fixture EADDRINUSE) → PortUnavailable, bounded retry
  never-ready → readiness timeout, process terminated, no orphan
  failed-start teardown failure → both causes surfaced, runtime stays owned,
    restart refused until explicit cleanup proves exit
  delayed ready → bounded retry then READY
  process exits during startup → ExitedDuringStartup short-circuit (no timeout wait)
  malformed readiness response (HTTP 200 + {}) → not READY, ProtocolUnexpected
  auth-required server: wrong/absent credential → 401, correct secret → READY
  stop during STARTING → readiness cancelled, process gone, STOPPED, no late READY
  double start → AlreadyStarted; double stop → idempotent
  unexpected exit after READY → engine.failed, endpoint considered dead
  stop → restart → stop → restart (fresh ProcessId/endpoint/secret each time)
  crash → explicit restart works; no automatic restart
  repeated start/stop cycles → supervisor active_count 0, endpoint closed, no leak
  secret redaction: ProcessSpec Debug/snapshot contains no secret value
  session methods → typed UnsupportedCapability (TASK 11 not leaked)

tests/real.rs (6 tests, skipped when opencode not discoverable — never faked)
  discover + probe real OpenCode (path + version from actual CLI output)
  spawn → authenticated /doc readiness → READY (real duration recorded)
  loopback bind verified (host 127.0.0.1), port closed after stop
  graceful stop → process gone, supervisor active_count 0, port closed
  repeated real start→READY→stop cycles (no orphan, no stale port)
  on-demand check_ready() against live real server
```

Fixture scenarios never stand in for real integration: real OpenCode smoke
is the only evidence of CLI discovery, launch args, readiness endpoint and
stop behavior (§98). If the environment lacks OpenCode the real suite
reports SKIPPED, never a fabricated PASS (§119).

### 6. Performance regression
Runs only against recorded baselines (PERFORMANCE.md). Guards: no full
transcript rebuild per token, no global rerender per delta, bounded memory
under flood, idle CPU ≈ 0, no listener/watcher multiplication.

## Required hostile scenarios by future subsystem

| Subsystem | Must be hostile-tested for |
| --- | --- |
| `saiwork-storage` | corrupt file (typed error, never deleted), future schema version (rejected before write), failed migration (full rollback incl. DDL), busy lock (bounded wait then typed `Busy`), concurrent readers/writers, competing writes (no loss), close/reopen durability |
| `saiwork-process` | spawn failure (no registry leak), invalid cwd, natural exit vs stop race, double stop, stop-after-exit, graceful timeout → force escalation, final-force failure → exact survivor report + retained authority + retry, **descendant tree kill (Job Object / process group)**, concurrent-process isolation, shutdown (rejects new spawn, clears proven exits, idempotent), repeated start/stop (registry returns to zero), large bounded output, partial lines, invalid UTF-8, nonzero exit codes |
| `saiwork-events` | storm (10k events), concurrent producers, slow consumer, lag + reconcile, reentrant publish, subscribe/drop loop (no listener leak), duplicate event |
| `saiwork-queue` | stale lease, crash-between-claim-and-send, double claim, revision CAS conflict, reorder crash, pause/restart persistence, ambiguous handoff (crash after engine acceptance), old-run-callback isolation, cancel races, session-busy arbitration, engine-unavailable wait, keyset paging beyond a blocked first page, lost wakeup, shutdown drain, fail-closed on storage error |
| `saiwork-saipen` | watcher storm, atomic replace/rename, root replacement, path escape (symlink/`..`/device), locked file, malformed state, unsupported schema, stale generation, read-only guarantee |
| engine adapters | malformed/truncated stream, CRLF framing, forged readiness, crash loop |
| `engine-opencode` | executable missing, wrong executable, invalid workspace, port collision, readiness timeout, exit-during-startup, malformed readiness, auth failure, stop-during-STARTING, double start/stop, unexpected exit after READY, restart-after-crash (no auto-restart), repeated start/stop leaks, secret redaction |
| UI store | unknown event, malformed payload, duplicate delta, flood rendering |

## Queue suites (TASK 13, `saiwork-queue`)

- `cargo test -p saiwork-queue` — 17 repo tests (no feature): enqueue /
  restart persistence, concurrent claim (exactly one winner), revision CAS
  edit/reorder/delete, reorder persistence, pause persistence,
  edit-after-claim rejection, ambiguous-handoff recovery, dispatched
  reconciliation stays, cancel intent, size bounds.
- `cargo test -p saiwork-queue --features failpoints` — 19 manager
  integration tests with FakeEngine: one → DONE, ordering, failure,
  hang + head-of-line block, cancel, engine-unavailable waits then proceeds,
  double-dispatch hostile, crash-matrix (prepare → requeued; sending →
  ambiguous; post-acceptance → ambiguous), stale-run-event isolation,
  lost-wakeup, session-busy arbitration, retry-after-failure, shutdown
  drain. Run serialized; race repeats are part of the Phase 1 gate.

## SAIPEN read suites (TASK 14, `saiwork-saipen`)

- `cargo test -p saiwork-saipen` — 29 tests, all in `tempfile` fixtures
  (real donor-shaped STATE/BOARD, never a user project):
  - **parser** (7): real canonical STATE (CRLF, quotes, `requires:` list),
    BOM+LF, duplicate-key error, missing delimiter, real board sections,
    ticket extraction with dash, non-ticket lines skipped;
  - **paths** (4): component containment (no `/a/bc` prefix confusion),
    plain root validation, missing root → NotPresent, symlink escape
    (unix), separator/`..` reference rejection;
  - **reader** (8): discover present/absent/unsupported/missing-file,
    full snapshot, size bounds, invalid UTF-8, read-only file assertion;
  - **watcher** (3): storm → ~1 refresh, idle → 0, root replacement
    → rebind refresh;
  - **service integration** (7): attach+detected, watcher change
    reflection + unchanged-save suppression, detach + late-event discard,
    read-only guarantee (content + no residue), absent = normal,
    invalid surfaced (never fabricated), unsupported schema rejected.

## SAIPEN action suites (TASK 15, `saiwork-saipen`)

- `cargo test -p saiwork-saipen --test action_tests` — **20 tests**: real
  subprocess execution through the ProcessSupervisor against a disposable
  **fake canonical tool** (a stand-in writer/validator, TASK 15 §120):
  tool discovery + version gate (unsupported schema blocks actions, missing
  `saipen_home` → NotAvailable), Status success, Validate valid,
  Validate **domain-invalid = result not failure** (exit 1, §41/§127),
  usage-error = infra failure (exit 2), double-start → Busy (backend
  authority), cancel → Cancelled + registry cleared, Stop = control
  (no spawn) + cancel, Stop with nothing running → typed error, Continue
  → Unsupported + disabled availability, Board/Knowledge view actions
  (no process, no lock), bounded timeout → Failed, validation
  generation staleness (§87–§88), shutdown rejects new actions, read-only
  guarantee (canonical files byte-identical, no residue), action events
  scoped by workspace.
- **Real canonical validator regression** (§217, §240): `real_validator_*`
  tests run the vendored `donors/saipen/tools/validate.py` end-to-end via
  std::process against synthetic fixtures — exit 0 = conformant
  (verified `Validation complete. Agent is conformant.`), exit 1 =
  domain-invalid, and the tool modifies nothing. This proves the contract
  the manager encodes against the actual tool, not a look-alike.
- Stability: action + watcher suites re-run 3× clean; queue manager suite
  (failpoints) 19/19; engine-opencode protocol 37/37 (Phase 1 gate).

## Cross-engine suites (TASK 17, `engine-generic-cli`)

- `crates/engine-generic-cli/tests/generic_cli.rs` (15): capability honesty
  (sessions/resume/streaming/cancel/models exactly as declared), start
  probe (missing executable fails), send-before-start rejected, echo run →
  MessageCompleted with full preserved output, nonzero exit → MessageFailed
  with code+stderr, real output streams as one terminal delta, oversized
  output bounded with truncation marker, timeout terminates + fails,
  cancel terminates + MessageCancelled (cancel wins ties), cancel-unknown
  no-op, prompt cap, engine-stop-does-not-kill-active-run, env config
  absent/present/malformed, capabilities never overclaim. Real `python`
  subprocesses through ProcessSupervisor (harness allowance §120).
- `crates/engine-generic-cli/tests/cross_engine.rs` (5): registry lists
  fake+generic-cli under distinct ids (unique EngineId), one engine's
  start failure never poisons the other, identical session strings stay
  isolated across engines (CLI output proves the CLI — not FakeEngine —
  executed), no automatic engine fallback (target engine fails honestly),
  `stop_all` stops every registered engine.
- `apps/desktop/src/components/TitleBar.test.ts` (3): model discovery is
  capability-driven (no `listModels` for `models=false` engines) and
  generation-guarded (stale response after an engine switch is discarded).
- Contract parity: FakeEngine and the CLI adapter are exercised against the
  same generic send→terminal contract; capability skips are explicit
  (resume/streaming/models tests assert `UnsupportedCapability` or false
  flags — §84).

## Frontend suites (TASK 16, `apps/desktop`)

- `npm test` (`vitest run`) — **13 tests**, 3 files:
  - `state/store.test.ts` (7): stream batching (N deltas → 1 mutation +
    1 notification), terminal-flush of pending deltas, log filtering
    (message.delta/tool.output never grow the log → no global rerender),
    permission pending→resolved lifecycle, queue/saipen revision guards
    (untouched-slice assertion), pure-reducer no-mutation contract;
  - `components/Conversation.test.tsx` (3, renderToString): streaming
    renders plain text (no Markdown conversion), terminal finalizes
    Markdown with a copyable fenced code block, interrupted runs never
    fabricate `complete`;
  - `components/TitleBar.test.ts` (3): model-load capability gating +
    engine-switch race guard (TASK 17).
- These cover the §161 behavior focus: events, buttons/disabled states,
  stream batching, revisions, errors, cleanup — not div-snapshot tests.
- Full-workspace gate: `cargo fmt --check`, `cargo clippy
  --workspace --all-targets`, `cargo test --workspace`, `npm run typecheck`,
  `npm run build`, `npm test` all green.

## Failure-first review (master spec §56)

Before a feature is DONE, answer:

```text
What if process never starts?
What if process starts but never becomes ready?
What if it dies midway?
What if network stream truncates?
What if event is malformed?
What if user presses cancel twice?
What if workspace disappears?
What if DB write fails?
What if app crashes at this exact state transition?
What survives restart?
What resource remains allocated?
Can the user retry safely?
Can data be lost?
```

Unknown answer → feature is not DONE.

## Fixtures and harnesses

- `tests/fixtures/` — malformed states, SAIPEN samples, SSE captures, path
  escape cases (each fixture cites its REGRESSION_BACKLOG row).
- `tests/fake-engines/` — FakeEngine scenario drivers.
- FakeEngine is permanent infrastructure (ADR-009), not throwaway.

## Provenance of every regression test

Each fixture records: source lesson (donor + commit), backlog row id, target
subsystem. A regression test without a cited lesson is still valid — but the
backlog entry is the reason it exists.

## Parallelism suites (TASK 18, `apps/desktop/src-tauri/tests/parallelism.rs`)

- 4 integration tests against the real `App` + FakeEngine (deterministic,
  isolated temp data roots), 3× race-repeat stable:
  - `different_workspaces_run_concurrently_and_isolated`: two runs in
    distinct workspaces coexist; distinct RunIds; B completes while A is
    active; cancel A never touches B (one terminal each).
  - `same_workspace_second_send_rejected_typed`: second send into a
    workspace with an active run → `CoreError::WorkspaceBusy`.
  - `same_workspace_serializes_then_allows_next`: after the run reaches
    terminal, the next send is accepted (no permanent lock).
  - `queue_port_busy_respects_workspace_boundary`: `EnginePort::session_busy`
    is workspace-aware (queue waits same-workspace, proceeds cross-workspace,
    releases after terminal).
- `engine-generic-cli` adds `same_session_second_send_rejected` (REJECT
  contract) and `saiwork-storage` adds `v1_to_current_upgrade_preserves_
  durable_rows` (upgrade evidence).

## TASK 20 — Harness adapter test tiers

Four tiers, all green in TASK 20:
1. **Harness protocol fixture** — `src/bin/fake-harness.rs`, a deterministic fake ACP
   server as a **real stdio process** (spawned through the ProcessSupervisor, not an
   in-process fake): handshake success/delay/fragmentation/reject/hang, malformed,
   oversized, partial-frame EOF, unknown notification, duplicate/unknown response id,
   server request, flood, stderr flood, ignored shutdown, exit-before/after-handshake.
   Scenario passed via **argv** (parallel-test-safe).
2. **Hostile matrix** — `tests/hostile.rs`, 30 tests, 3× stable: lifecycle,
   stop-during-start/request, direct start-task abort during handshake, crash→Failed+event,
   restart generation, 25× resource baseline, discovery/typed errors, version acceptance,
   capability honesty, idle=zero, registry isolation with FakeEngine.
3. **Real Harness Windows handshake** — BLOCKED UPSTREAM in TASK 20 (npm CLI has no
   `acp` entry; `@deepseek-ai/dsh-acp` 0.0.1-rc.1 published but needs a source-checkout
   composition) → the **TASK 21 probe gate**.
4. Future: real provider vertical slice (TASK 21+).

## TASK 21 — Harness agent vertical-slice test tier

`tests/vertical.rs` — 28 tests, real stdio fixture through the ProcessSupervisor,
covering the hostile run matrix (DEEPSEEK_HARNESS.md §23):
- Normal turn (streams + exactly one terminal); multi-step turn → one RunId with
  isolated tools; tool lifecycle (started/output/completed); tool failure continues.
- Permission allow round-trip; permission deny (fail + resolve); permission no-response
  fail-closed on cancel; `resolve_permission` unknown = idempotent no-op.
- Cancel before first chunk; cancel mid-chunk (one terminal, no deltas after); cancel
  race (authoritative finish wins, exactly one terminal); cancel twice / after-complete
  are no-ops; cancel unknown run is no-op.
- Provider failure (run fails, engine stays READY); runtime crash (run + engine fail);
  transport loss (run + engine fail); accepted-then-response-lost (outcome unknown, no
  retry); duplicate chunk (no corruption); wrong-session event isolation; session busy
  (same-session REJECT); second turn after terminal; restart = connection-owned fresh
  sessions (stale id → SessionNotFound); engine stop settles runs.
- Performance: 10k-chunk stream completes bounded (all deltas, exactly one terminal);
  resource cleanliness asserted per test (`assert_clean`).
- Cross-engine parity: generic SessionManager flow + permission round-trip produce
  canonical events (same consumers as OpenCode/FakeEngine).

Foundation hostile matrix (29) unchanged and green. Real Harness Windows E2E remains
**BLOCKED EXTERNAL** (ACP profile composition + provider config) — the fixture matrix
proves the workflow deterministically.

## TASK 23 — Harness queue + OUTCOME_UNKNOWN test tiers

- **`engine-deepseek-harness/tests/queue_slice.rs` (4)** — the REAL production wiring
  over the fixture-backed Harness adapter: `EngineRegistry` + `SessionManager` +
  `QueueEnginePort` + `QueueManager`. Proves Harness is a durable queue target through
  the generic path: enqueue → dispatch → Harness send → `message.completed` → DONE;
  queue cancel → Harness `session/cancel` → CANCELLED (engine stays READY); provider
  failure → FAILED (engine stays READY); engine crash → FAILED, no auto-requeue.
- **`saiwork-queue` OUTCOME_UNKNOWN tests (6 new, `queue_manager_tests.rs`)**: crash in
  `sending` → UNKNOWN never redispatch; DISPATCHED at restart → UNKNOWN (old correlation
  retained); UNKNOWN blocks same workspace but not others (and unblocks on resolution);
  retry UNKNOWN → new attempt (old-attempt terminal cannot mutate it); cancel UNKNOWN →
  CANCELLED. Repo-level: `recovery_sending_lease_is_unknown_not_resent` and
  `recovery_marks_dispatched_unknown_after_restart`.
- Crash matrix (existing failpoints, updated to UNKNOWN): crash before send / after
  acceptance — never duplicate an accepted send. Cross-engine parity: FakeEngine queue
  suite (23) + OpenCode protocol (37) remain green; the Harness queue path uses the same
  generic EnginePort as every engine.

## TASK 24 — post-V1 multi-engine hostile matrix (`tests/multi_engine.rs`, 6)

Real production wiring (`EngineRegistry` + `SessionManager` + `QueueEnginePort` +
`QueueManager`) with **FakeEngine + HarnessAdapter registered simultaneously** in one
registry:

- `cross_engine_session_and_run_isolation` — separate sessions/runs per engine, each
  completes on its own session id (no cross-talk).
- `queue_routes_mixed_targets_exactly` — Fake + Harness queue items each reach DONE via
  their exact stored `engine_id`; each item's session belongs to its engine.
- `queue_target_immutable_after_selection_change` — changing the selected engine never
  retargets queued work.
- `engine_crash_isolated_from_other_engine` — Harness taken down while a FakeEngine hang
  run is live: the fake run is untouched, a queued Harness item never falls back to
  FakeEngine and is never auto-failed, FakeEngine queued work in another workspace still
  dispatches.
- `same_workspace_cross_engine_serialized` — an active FakeEngine run in `w1` rejects a
  Harness send into `w1` (`WorkspaceBusy`); `w2` is unaffected (ADR-038 is
  engine-independent).
- `session_id_collision_fails_closed` — two hostile adapters returning session id `"1"`:
  the second create is rejected with `SessionIdConflict`, the first engine's session
  stays intact (ADR-044).

## TASK 22 — generic contract test suite (deferred)

TASK 22 audited whether a reusable generic EngineAdapter contract suite is justified
(Candidate K). The Harness and OpenCode suites duplicate helper patterns but differ in
protocol (ACP vs SSE) and fixture (stdio process vs HTTP server), and a truly reusable
suite needs a common fixture factory across very different adapters — high risk of
becoming vendor special-casing (a P1). Classified **DEFER**; the per-adapter suites
(Fake 39, OpenCode protocol 37 / hostile 26 / real 6, Harness hostile 29 / vertical 28,
Generic CLI 16 + cross-engine 5) already cover observable contract behavior thoroughly.
Trigger for reconsideration: a 4th engine or demonstrated contract drift between
adapters. The TASK 22 cleanup-bug fix is covered by the unchanged Harness hostile +
vertical matrices (green serial).
