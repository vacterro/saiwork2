# PERFORMANCE.md

## Invariants (laws, not guidelines)

```text
- No periodic whole-disk scanning.
- No periodic whole-project scanning while idle.
- No broad filesystem polling when watchers exist. (SAIPEN.md documents the
  one guarded bounded backstop exception.)
- No blocking filesystem/process operation on the UI thread.
- No unbounded runtime buffers (bounded ring buffers everywhere).
- No repeated full transcript serialization during streaming.
- No full app rerender per token (batched deltas; selective updates).
- No duplicate parsing unless measured and justified.
- No expensive work on every keystroke (composer is local until submit).
- No event-listener multiplication (one subscription per concern).
- No zombie background work after workspace close.
- No leaked child processes.
- No stale watchers after workspace switch.
- No runaway retry loops.
```

## Streaming pipeline

```
engine stream → adapter parser → normalized delta → Rust/Tauri coalescer
(single batching authority, ≤16 ms window) → JS store applies immediately
→ ONE render per emitted bridge batch
```

There is exactly ONE batching window from engine delta to React state: the
shell-side coalescer in `crates/saiwork-events/src/coalescing.rs` keyed by
`(SessionId, RunId)` concatenates consecutive `message.delta` facts in
arrival order and flushes on a ≤16 ms window AND synchronously before any
non-delta/terminal fact. The frontend store applies each already-coalesced
delta immediately — it has no second timer/chunk map. Proven by
`crates/saiwork-events` `coalescing` tests (10k deltas → O(batch-count)
emissions, flush-before-terminal, flush-failure terminates the forwarder)
and by `store.test.ts` ("applies each shell-coalesced delta immediately —
one mutation per bridge batch", "message.delta and tool.output never grow
the log").

Conversation history renders one bounded `pre` per message with stable
keys, and long transcripts are windowed: only the newest 50 messages are
mounted, with a deterministic "Load earlier" expansion and a permanent
active/streaming message in the window — proven by `Conversation.test.tsx`
("mounts only the newest window for a long transcript"). The complete
projection stays in the store; no history is truncated.

Pre-baseline guards already enforced by the FakeEngine suite (TASK 07): a
10,000-delta stream completes without deadlock or runaway accumulation;
zero-delay burst emits every delta; cancel under event pressure stops the
producer (not merely the UI); a slow bus consumer never blocks the
producer (bounded broadcast, EVENTS.md). These are architecture-collapse
checks — real numbers land in the baseline table below after Phase 1.

## Deterministic performance gates (executable, test-backed)

These gates run in CI/test and must stay green; they are the measurements
this document's claims rest on. Packaged-release timings are still TBD
(no packaged Windows run was measured in this audit pass) and are NOT
invented here:

| Claim | Executable gate | What it proves |
|---|---|---|
| Idle queue does zero periodic DB reads | `saiwork-queue` (feature `failpoints`) — `idle_dispatcher_ignores_stream_flood_and_does_zero_scans` | 10k `message.delta` flood ⇒ `dispatch_scan_count()` unchanged (no `list_queued`, no `workspace_has_unknown`); enqueue still reaches Done |
| Dispatcher wake matrix (no lost wakeup) | `lost_wakeup_enqueue_at_idle_is_never_missed`, `engine_unavailable_waits_then_dispatches_on_ready`, `existing_session_busy_waits_then_dispatches`, `shutdown_keeps_queued_items_durable_and_stops_claims` | enqueue / engine-ready / session-terminal / shutdown all wake the Notify-only dispatcher |
| Stream batching — ONE window, engine delta → React state | `crates/saiwork-events` `coalescing` tests + `store.test.ts` — `applies each shell-coalesced delta immediately — one mutation per bridge batch` | 10k deltas through the real coalescer ⇒ O(batch-count) IPC/store notifications, byte-identical final text, terminal after the final delta, flush failure terminates the forwarder (no zombie consumer) |
| OpenCode post-run grace — no unconditional 250 ms tax | `engine-opencode` lib `grace_math_is_deterministic` | stream quiet > grace ⇒ terminal immediately; last event 200 ms ago ⇒ waits only the remaining ~50 ms; recorded `session.error` wakes immediately; no-evidence case keeps the full safety cap |
| OpenCode model resolution — O(1) generation-scoped lookup | `engine-opencode` lib `lookup_uses_namespaced_key_and_is_unambiguous_across_providers` + `protocol.rs` `same_raw_key_across_providers_is_unambiguous` | 5k-model fixture: zero per-send clone/sort/dedup of the model vector; ids are `<provider>/<raw-key>` namespaced, so identical raw keys across providers stay distinct (no ambiguity enum); unknown id is a plain map miss |
| OpenCode provider catalog — bounded + strict fallback | `protocol.rs` `large_valid_catalog_exceeding_old_bound_succeeds` (600×40 models, ~5 MiB-class body), `catalog_over_configured_bound_is_typed_error_engine_default_works`, `provider_route_absent_falls_back_to_config_providers`, `provider_401_never_triggers_endpoint_fallback`, `provider_endpoint_500_is_safe_diagnostic_no_retry_storm` | a real-sized catalog loads under `provider_catalog_max_bytes` (16 MiB); an over-bound catalog is a typed PROTOCOL error that leaves the engine usable; `/config/providers` fallback fires only on 404/405, never on 401/403/500/timeout |
| SseParser linear scan | `engine-opencode` lib `sse` tests | one chunk with 100k short lines processes with a consumed cursor — no per-line front drain/move; fragmentation/CRLF/multiline/id/MAX_LINE behavior byte-identical |
| Queue snapshot single-flight | `QueuePanel.test.tsx` — `snapshot loading is single-flight across a burst of revisions`, `a slower older response can never overwrite newer snapshot truth` (driving the `queueSync.ts` owner via `installQueueSync`) | burst of 100 revisions ⇒ at most one in-flight request + one follow-up; final UI equals the newest authoritative snapshot |
| Queue snapshot bounded payload | `saiwork-queue` `queue_repo_tests` — `snapshot_payload_is_bounded_preview_full_payload_via_get`, `snapshot_preview_trims_a_split_multibyte_character` | 200 near-max items ⇒ snapshot payloads SQL-projected ≤ preview cap (the full body never enters Rust memory), `payload_truncated` flags the projection; `get` returns the exact full payload for edit |
| Queue dispatch candidate keyset pages | `saiwork-queue` — `candidate_keyset_pages_are_bounded_complete_and_ordered`, `candidate_keyset_page_uses_dispatch_index_without_temp_sort`, `eligible_item_beyond_first_candidate_page_is_dispatched` | 263 rows walk in exact order through ≤128-row pages with no duplicate/skip; SQLite seeks via the v8 composite index with no temp sort; a ready item beyond 128 blocked heads still dispatches |
| QueuePanel O(N) reorder render | `QueuePanel.tsx` (single `useMemo` queued derivation + index map) | 1,000 queued items ⇒ one derivation + O(1) index lookup per row, not ~1,000 filters/finds per render |
| SAIPEN reads never hold the global mutex | `saiwork-saipen` integration — `slow_workspace_read_never_blocks_other_workspaces`, `change_storm_yields_one_coalesced_reread_and_no_stale_commits` | a 250 ms blocked read of workspace A cannot block snapshot/detach/refresh of B; 100-event storm ⇒ one coalesced authoritative reread |
| Harness protocol stdout — no dual text processing | `saiwork-process` `process_supervisor` — `protocol_mode_skips_stdout_line_ring`, `protocol_mode_with_diagnostics_keeps_ring_and_raw` | protocol-mode stdout delivers byte-identical raw chunks with an EMPTY line ring; explicit diagnostic mode keeps both |
| Harness capped previews | `engine-deepseek-harness` lib `events` tests — `json_capped_aborts_well_below_full_serialization`, `bounded_tool_output_text_blocks_stop_at_cap` | multi-MB text/raw JSON ⇒ preview-side allocation O(cap); small payloads byte-identical |
| Concurrent stop_all — one budget, not N× | `saiwork-process` `process_supervisor` — `stop_all_concurrent_near_one_budget` | 4 stubborn owners each burning a full graceful budget stop in ≈1 budget total; every failure retained; zero survivors |
| Indexed point lookups | `saiwork-storage` lib — `point_lookups_match_list_scan_with_large_unrelated_sets` | 5k+5k unrelated rows: `get_workspace`/`get_session_meta` return exactly the list+scan record via one indexed query |
| Frontend end-to-end route | `src/app/firstPrompt.smoke.test.tsx` | fresh start → project → OpenCode default → Start → models/default → New Session → prompt → response → second prompt → Cancel → restart → resume → queue prompt, twice in a row, `lastError` null throughout |
| Diagnostics snapshot is cheap | `saiwork-storage` open path + `App::snapshot` | integrity checked once at DB open and cached (`Db::integrity` reads the cache; `deep_integrity` re-runs the PRAGMA); snapshot uses `workspace_count`, never workspace enumeration |
| Strict persisted-enum decode fails closed | `invalid_persisted_state_fails_closed_and_never_dispatches`, `invalid_persisted_session_mode_fails_closed` | unknown `state`/`session_mode` rows disable dispatch with the exact value named |

Metrics deliberately NOT packaged here (would be fabricated): packaged
release cold-start/warm-start/stream-render timings on a clean Windows
machine and OpenCode real-runtime numbers beyond the earlier dev-build
first facts.

## Baseline (after first vertical slice, packaged release build, Windows)

Measure and record in this document (table below) before setting regression
thresholds. Development mode is never a benchmark.

```text
cold startup
warm startup
idle CPU
idle memory
OpenCode startup
time to first response event
stream rendering responsiveness
workspace switch
shutdown time
child process cleanup
long transcript behavior (10k deltas)
```

## Startup/shutdown baselines (TASK 08, first facts)

Recorded from the TASK 08 desktop smoke on this machine (debug build,
Windows, warm FS cache — dev numbers, the first facts for trend tracking;
the packaged release baseline table below remains the Phase-1 gate):

```text
cold bootstrap (data root + storage + services)   6–8 ms (incl. DB migration on first run)
warm bootstrap (existing DB, no migration)        1 ms storage stage; READY in ~1 ms
clean shutdown (supervisor 0 + storage checkpoint) 1 ms
```

Startup noise check: a normal launch emits exactly the expected lines
(migration/open/ready/engines registered) with zero warnings; a second
instance contributes nothing (exits before storage). Idle policy is
enforced by design: no polling loops, no synthetic timers, no periodic DB
access — the running tracker ends on `app.stopping`, and the event
forwarder ends when the bus closes at process exit.

## TASK 09 measured baselines (Phase 0 gate, 2026-08-16)

Environment: Windows 10 x64, `x86_64-pc-windows-gnu` rustc 1.97.1,
**debug build** (release/packaged numbers land in the table below at the
TASK 12 gate; dev numbers are for trend tracking only):

```text
core bootstrap to READY (log timestamp)    1 ms (storage stage 1 ms; no migration)
idle CPU (10 s sample, no engine/process)  0% of one core
idle working set                           34.6 MB (private 10.1 MB, debug)
clean shutdown (supervisor 0, checkpoint)  shutdown_ms = 0, outcome clean
managed-process shutdown (graceful→force)  5 006 ms (full graceful budget on Windows:
                                           CREATE_NO_WINDOW child cannot be WM_CLOSE'd,
                                           then TerminateJobObject)
```

**Release binary** (same env, `cargo build --release`, portable layout,
measured in-session): bootstrap to READY 6 ms; idle CPU 0 ms over 10 s;
working set 25.9 MB (private 9.3 MB); clean close, relaunch reopens without
re-migration. The NSIS installer bundle is produced at the TASK 12 release
gate; the optimized executable itself is Phase 0-verified.

Idle observation: no repeated logs, no periodic DB access, no timer storm
(the 0% CPU reading over 10 s is direct evidence — §61).

TASK 09 audit results: zero production timers/polling loops (repo search:
`interval`, `tokio::time::sleep` only in FakeEngine simulation and tests);
zero clippy warnings; no `current_dir()`-derived writable paths outside the
canonical data-root resolver; no unbounded buffers in the event path.

TASK 10 OpenCode adapter measurements (opencode-ai@1.18.18, native exe via
PATH):

```text
probe (--version, cold)             ≈ 0.2 s
spawn → READY (real OpenCode)       5.7 s   (native exe, authenticated /doc
                                            probe; includes OpenCode's own
                                            server startup)
stop (graceful)                     < 1 s    (process gone, port closed,
                                            supervisor active_count 0)
idle with OpenCode NOT started      identical to Phase 0 baseline — the
                                    adapter registers but does nothing until
                                    an explicit start (no discovery loop,
                                    no health timer)
```

The adapter allocates an explicit available port (never a fixed global
port); startup deadline and per-request timeouts are bounded (default
startup 60 s comfortably above the observed 5.7 s, request 5 s).

## Queue (TASK 13)

- Idle queue: fully event-driven. The dispatcher waits ONLY on its `Notify`
  (permit semantics — a notify between a state check and `notified()` is
  never lost, `tokio::sync::Notify`); enqueue/edit/reorder/retry/resume/
  cancel and the coordinator (EngineReady / SessionChanged / run terminals)
  all notify it. There is NO backstop polling: an idle queue performs zero
  periodic DB reads, and stream deltas can neither wake it nor lag its
  bounded buffer (the coordinator + running tracker subscribe to the
  State-class channel only — `EventBus::subscribe_state`). Regression-tested:
  `idle_dispatcher_ignores_stream_flood_and_does_zero_scans` (10k-delta
  flood ⇒ zero `list_queued` calls; enqueue still dispatches to Done) and
  `lost_wakeup_enqueue_at_idle_is_never_missed`.
- Eligibility scan: fixed 128-row keyset pages over
  `(order_key, created_at, id)` (id/revision/engine/workspace/session fields
  only — never payload/error strings) plus ONE `unknown_workspaces` set fetch
  per paged scan (no N+1 COUNT). The v8 composite index supplies ordered
  seeks without OFFSET or a temp sort; only one page is resident, and the full
  `QueueItem` is materialized only for the claimed candidate
  (`QueueRepo::list_candidate_page`/`unknown_workspaces`).
- Hot path: one item at a time (concurrency = 1); each mutation is one short
SQLite transaction; no SQLite lock is held across the engine send; run
terminals are one guarded UPDATE. No full-queue serialization per event.
- A 10k-delta stream through the TASK 12 fixture reaches the consumer
  completely when the consumer drains continuously (bounded bus drops are
explicit `Lagged`, never silent).

## SAIPEN read path (TASK 14)

- Idle: the notify watcher uses event-driven normal updates plus one bounded filesystem liveness heartbeat/backstop (approximately every 2 seconds) for root-existence checks. This ensures Windows deletion/liveness events are reliably caught.
- Event storms coalesce: a 10-event save burst → ≤3 refreshes (dirty flag +
  300 ms quiet window); the channel is bounded (64), so a storm cannot grow
  memory (§112). Overflow forces one full authoritative reread.
- Reads are size-bounded (STATE ≤1 MiB, BOARD ≤8 MiB) and per-refresh each
  canonical file is read once (§107). Snapshot equality is semantic
  (field comparison, not JSON serialization, §108).
- Two-phase refresh (TASK 24 perf): the global `entries` map is never held
  across filesystem I/O — root/epoch/state are captured under lock, the
  bounded read/parse runs off-lock (spawn_blocking), the result commits only
  if the epoch is still current; one in-flight refresh per workspace is
  coalesced. Proven by `slow_workspace_read_never_blocks_other_workspaces`
  and `change_storm_yields_one_coalesced_reread_and_no_stale_commits`.
- Watcher scope is the `.saipen` dir non-recursively — `node_modules`
  storms never reach SAIPEN status (§32).

## SAIPEN actions (TASK 15)

- No validation on every keystroke / no polling: Validate runs on explicit
  user action only (§77, §201); results are cached in-memory tied to the
  snapshot generation, so a re-render never re-invokes the tool (§86).
- Per-kind bounded timeouts (read-only 20 s, mutating 60 s, §25) with
  graceful→force stop; a hung canonical tool cannot leak a process.
- Action events carry metadata only; the full record is fetched on demand
  (`saipen_action_status`) — no stdout through the bus (§59, §81).
- Real-tool validation latency is the canonical `validate.py` cost itself
  (a few seconds on a small project) — SAIWORK2 adds only the managed
  spawn + bounded capture; never cached as current after the snapshot
  moves (§87–§88).

## Frontend UI (TASK 16)

- **No global rerender per token**: `message.delta` is coalesced ONCE in the
  Rust/Tauri forwarder (`saiwork-events` coalescer, flush ≤16 ms and
  synchronously before any terminal/non-delta fact) and applied immediately
  by the store — there is exactly one batching window, never two. Verified
  by the coalescing tests + `store.test.ts` (`applies each shell-coalesced
  delta immediately`, `message.delta and tool.output never grow the log`).
  The Conversation is memoized on its own slice and renders the newest 50
  messages with "Load earlier" windowing; queue/session/saipen components
  only react to their own event families (§241).
- **Streaming cost**: streaming renders plain text (cheap); Markdown is
  finalized at terminal (§28). Fenced code blocks render only at terminal.
- **Bundle**: production JS 344 kB (106 kB gzip) — the delta is
  react-markdown + remark-gfm, the only new runtime dependency (justified:
  §27, §217).
- **No polling**: idle CPU is event-driven; the SAIPEN watcher has no
  setInterval/animation loops in normal state (§196). Its ONLY recurring
  timer is the bounded ~2 s liveness backstop for root-existence checks
  (SAIPEN section above) — a heartbeat, not a directory scan.

## Additional engines (TASK 17)

- **Idle**: the generic CLI adapter is registered only when configured; even
  when registered it adds **zero** recurring work while inactive — no
  process, no timer, no network, no discovery loop (§117). Readiness is a
  one-shot config probe at `start`; engine stop is state-only.
- **Registry**: `EngineRegistry` operations are O(engines) in-memory reads;
  no global scan on token events (§120). Per-engine health/capabilities are
  part of `list_engines` and the diagnostics snapshot.
- **Switching**: engine switch clears the model projection and re-discover
  only for `models`-capable engines; a stale discovery response is
  discarded by the generation guard — no whole-app reset (§118).
- **Stream**: the CLI adapter emits at most one bounded `message.delta` per
  run (real output at exit) — the same frontend bridge batching applies;
  no second stream renderer (§119).
- **Memory**: each run's record + process handle is removed at terminal;
  repeated start/run/stop cycles leave no adapter runtime retained (§121).

## DeepSeek Harness adapter (TASK 20–21)

- **Idle**: the Harness adapter adds zero recurring work while inactive — no
  timer, no polling loop, no reconnect (§59; idle = zero-work test). The
  runtime is a supervised child only while started.
- **Stream**: `session/update` committed chunks route through the same
  frontend bridge batching as every engine; no second renderer. The
  dispatcher is one task per runtime with a bounded route channel; on
  overflow it emits a `HARNESS_STREAM_OVERFLOW` runtime warning and drops
  (droppable deltas) — terminal/permission/session facts are never dropped
  (§101–§102). 10k synthetic chunks complete bounded with exactly one
  terminal (vertical test).
- **Tool/permission detail**: bounded (32 KiB output, 500-char input
  summary) — no unbounded bus/history duplication (§51, §62).
- **Memory**: session registry, run records, and pending permissions are
  cleared on teardown/terminal — repeated start/run/stop cycles retain no
  adapter runtime (§121, assert_clean in every vertical test).
- **Latency (fixture, debug build)**: session create, prompt acceptance,
  first delta, tool events, permission round-trip, cancel, second turn all
  complete well under 1 s over the local stdio fixture; real provider
  inference latency is separate and not measured (REAL INFERENCE =
  BLOCKED EXTERNAL).

## Baseline table (filled after phase 1 measurement)

| Metric | Value | Build | Date |
| --- | --- | --- | --- |
| cold startup | TBD | — | — |
| warm startup | TBD | — | — |
| idle CPU | TBD | — | — |
| idle memory | TBD | — | — |
| OpenCode startup (spawn→READY) | 5.7 s | debug, opencode-ai 1.18.18 | 2026-08-16 |
| first response event | TBD | — | — |
| workspace switch | TBD | — | — |
| shutdown | TBD | — | — |
| child processes left | TBD (must be 0) | — | — |

Regression gates are added only after baselines exist. Do not optimize
semantic behavior for synthetic benchmarks.

## Release baseline (TASK 18, measured 2026-08-17)

- **Startup:** release build, fresh portable data root, this machine
  (Windows 10 19045, i7-class): `application ready total_ms=7`
  (data_root 0 ms, storage 6 ms, services 0 ms). Cold launch is
  dominated by WebView2 + window creation, not the Rust bootstrap.
- **Idle:** fully event-driven; no polling timers, no per-item timers, no
  watcher scan loops (verified by the loop/lock audit); SAIPEN watcher uses event-driven updates plus a bounded 2s liveness backstop. The queue dispatcher is Notify-only with NO
  periodic DB polling and no backstop timer (proven by
  `idle_dispatcher_ignores_stream_flood_and_does_zero_scans`).
- **Release artifacts:** `saiwork2.exe` ≈ 10 MB; NSIS setup ≈ 3.3 MB; MSI
  ≈ 9.9 MB. Frontend bundle 343 kB (105.7 kB gzip).
- **Supported parallel load:** one agent run per workspace (ADR-038);
  different workspaces run concurrently. Idle CPU under a large paused
  queue = idle (no one-timer-per-item).
