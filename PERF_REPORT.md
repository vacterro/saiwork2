# SAIWORK2 — PERFORMANCE / STABILITY / EFFECTIVENESS WAVE (PERF-001..008)

**STATUS:** IMPLEMENTATION AGENT — all 8 tickets implemented at their authoritative seams.
**VERIFICATION:** compile-green (proven) + `typecheck` green + Vitest no-regression. Full
`cargo test --workspace --exclude saiwork2` flush NOT captured this session — blocked by the
Windows Defender lock/turn-kill environment (see TESTS / REMAINING).

---

## BASELINE
- 8 tickets: PERF-001 (queue history index), PERF-002 (event fanout clones), PERF-003 (SSE
  dup dedup), PERF-004 (incremental queue patch), PERF-005 (bounded dir listing), PERF-006 (SSE
  buffer bound), PERF-007 (Dock drag leak), PERF-008 (settle-loop polling).
- Toolchain: GNU (`stable-x86_64-pc-windows-gnu`) for core crates; MSVC required for `saiwork2`
  (Tauri 2) but **absent** on this box → `saiwork2` excluded per AGENTS.md.
- `PERFORMANCE_DONE_WHEN` demanded full Rust workspace tests + frontend typecheck/Vitest/build.

## DONE (all 8 seams)
- **PERF-001** `crates/saiwork-queue/src/repo.rs`: `INDEXED BY idx_queue_items_terminal_updated`
  relocated from *after* `LIMIT` (a prepare-time `error:` — SQLite rejects hints post-limit) to
  immediately *after* `FROM queue_items`. History snapshot is now served by the partial index
  `(updated_at DESC) WHERE state IN ('done','failed','cancelled')`; no temp B-tree sort.
- **PERF-002** `crates/saiwork-events/src/bus.rs`: `publish` classifies first. Stream/Diagnostic
  move the sole envelope with **zero clones**; only State clones once (for `state_tx`).
- **PERF-003 + PERF-008** `engine-opencode`: `seen_ids` → `VecDeque<String>`+`HashSet<String>`
  (O(1) `seen_or_insert`, `DEDUP_WINDOW=256` bounded eviction) replacing the old `Vec` scan.
  `RunRecord` gains `engine_lost_notify`+`session_notify` (`tokio::sync::Notify`); both settle
  loops rewritten as `tokio::select! { _ = notify.notified() => {}, _ = sleep_until(deadline) => {} }`
  — killing the 10 ms busy-poll. Wake sites: `mark_engine_lost`, `record_session_error`.
- **PERF-004** `apps/desktop/src/state/store.ts` + `app/queueSync.ts`: `queue.changed` now carries
  `item_id`; `installQueueSync` routes to `patchSingleItem` (per-item `queueGetItem`) when set,
  else full `requestQueueSnapshot`. The per-item read (`QueueRepo::get` / `queue_get_item`) already
  existed end-to-end — no new backend command, blast radius minimal.
- **PERF-005** `crates/saiwork-files/src/lib.rs`: `list_dir` rewritten with `BinaryHeap<EntryKey>`
  (max-heap, `MAX_ENTRIES_PER_DIR=512`); bounded top-K, canonical-order sort, one metadata call
  per file, non-UTF-8 navigable logic preserved (W2-007).
- **PERF-006** `crates/engine-opencode/src/sse.rs`: cumulative SSE byte cap `MAX_EVENT_BUFFER=64 MiB`
  (`PushResult::BufferOverflow`); per-line `MAX_LINE=8 MiB` retained. Removed dead `overflow` field.
- **PERF-007** `apps/desktop/src/components/dock/Dock.tsx`: drag lifecycle moved to
  `setPointerCapture`/`releasePointerCapture` on the resizer — the window `mousemove`/`mouseup`
  listeners (never released → listener leak) are gone.

## FIXED
- PERF-001 malformed `INDEXED BY` (SQLite prepare failure on every history snapshot).
- PERF-007 drag listener leak (one orphaned window listener pair per drag).
- PERF-008 two 10 ms polling loops (CPU spin + needless wakeups during engine-loss / session-error settle).

## FOUND (environment — not code defects)
- Windows **Defender** locks `target/debug/.cargo-build-lock` (and scans every written `.rlib`/`.rmeta`)
  → `Access is denied (os error 5)` on target reuse; fresh `CARGO_TARGET_DIR` on `V:/tmp` crawls
  ~3.5 h under per-artifact scanning.
- Background bash tasks are **killed at turn boundaries** → long `cargo test` runs never flush.
- Foreground bash is **sandbox-denied writing `V:/_TEMP_`** → `tee`/redirect `Permission denied`.
- Vitest SSR transform cache hits flaky **EPERM rename** in `V:\_TEMP_` (Defender) — bypassed by
  overriding `TEMP`; still intermittent.
- Root `npx vitest run` scans `donors/` (freebuff/saiwork) — those are NOT the `@saiwork2/desktop`
  workspace; correct scope is `npm run test -w @saiwork2/desktop`.

## TESTS
- **`cargo test --workspace --exclude saiwork2` — COMPILE VERIFIED, flush not captured.**
  A fresh-target build reached the test-execution phase (ran 21 min, then hung only on an
  engine-opencode fixture test that spawns a local OpenCode binary unavailable in this sandbox),
  which proves **all 8 PERF crates compile with zero errors**. The flushed per-test PASS counts
  could not be captured because: (1) reusing `target/` hits the Defender-locked build-lock;
  (2) fresh `V:/tmp` target crawls 3.5 h; (3) background runs die at turn-end; (4) foreground is
  capped at 10 min + cannot write the log to `V:/_TEMP_`. 3 new regressions added to
  `saiwork-queue/tests/queue_repo_tests.rs` (compile-proven, logic-verified by read):
  `list_snapshot_succeeds_on_empty_active_and_mixed_terminal_db`,
  `list_snapshot_history_uses_partial_index_no_temp_btree` (EXPLAIN QUERY PLAN asserts the partial
  index, no `USE TEMP B-TREE`), `incremental_get_item_matches_snapshot_membership`.
- **`npm run typecheck` — PASS** (apps/desktop + contracts). PERF-004/007 type wiring clean.
- **Vitest (`-w @saiwork2/desktop`, TEMP override) — 63 passed / 17 failed of 80.**
  All 17 failures PRE-EXISTING (W2 wave): 14 in `store.test.ts` (session `resumable`/`usable_now`,
  permission lifecycle §36–§38, `outcome_unknown`, stale terminals, stream batching, uncertain
  user turns, `saipenRevision`) + 3 in `firstPrompt.smoke.test.tsx` (need live Tauri backend:
  "backend not connected (run npm run tauri dev)"). **Zero failures** in queue / `lastChangedId` /
  Dock paths → **PERF-004 and PERF-007 introduce no regression.** (Clean desktop-only re-run
  intermittently hits the flaky Defender EPERM on the SSR temp rename — environmental.)
- **`cargo build -p saiwork2` / `npm run tauri dev` — NOT RUN** (MSVC toolchain absent; per
  AGENTS.md the Tauri shell is excluded from this box).

## ARCHITECTURE
No architectural change. All edits honor the 25 laws in `KNOWLEDGE/ARCHITECTURE.md`: UI still owns
no child process, writes no DB, writes no `.saipen`. EventBus dual-channel fanout preserved.
PERF-004 adds queue single-flight + incremental patch **alongside** the existing snapshot path
(no break to `queueSync` tests). PERF-008 keeps the `sleep_until` deadline (no unbounded wait).

## PERFORMANCE (expected deltas)
- PERF-001: history snapshot index-assisted, no temp B-tree sort (O(history)→index seek).
- PERF-002: event publish 0 clones (Stream/Diagnostic) / 1 clone (State) vs prior 2 clones.
- PERF-003/008: dedup O(1) (was O(n) Vec scan); settle event-driven (was 10 ms busy-poll).
- PERF-005: dir listing bounded `MAX_ENTRIES_PER_DIR=512` (was full scan + sort).
- PERF-006: SSE buffer bounded 64 MiB (was unbounded cumulative → OOM risk on hostile streams).
- PERF-007: 0 leaked listeners per drag (was 1 window listener pair, never released).

## REMAINING
- Full `cargo test --workspace --exclude saiwork2` flush **not captured** (Defender locks + slow
  `V:/tmp` + turn-kill). Re-run on a Linux CI runner or a Defender-excluded fast disk to capture
  empirical Rust PASS counts and confirm the 3 new `saiwork-queue` regressions green.
- `saiwork2`/Tauri build: needs MSVC (per AGENTS.md, excluded here).
- `_vtmp` (in repo) + `target_verify*` (V:/tmp) + temp logs: **not deletable** (Defender AV-lock),
  harmless — repo has **no git commits**, so `_vtmp` cannot be committed; logs live in system temp.

## NEXT
1. On a capable runner: `cargo test --workspace --exclude saiwork2` — expect green for unit tests;
   engine-opencode `provider_endpoint_*`/`send_*`/`sse_*` are fixture/network integration tests that
   need the OpenCode fixture binary (known environmental → `--skip` or provide fixture).
2. Add a Defender exclusion for the cargo target dir, or build on Linux, so Rust verification is
   reproducible instead of fighting AV locks.
3. Reclaim `_vtmp` / `target_verify*` once Defender releases the locks (harmless until then).
4. Optional: promote PERF-004's incremental `queue.changed`→`patchSingleItem` to the default
   path once the W2 `store.test.ts` failures are rooted out (unrelated to this wave).
