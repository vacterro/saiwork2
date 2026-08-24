# REGRESSION_BACKLOG.md — Historical Regression Backlog

Every meaningful historical failure across the four donors becomes a future
fixture (law 24). This file records the **exact test intent** and the
**source lesson**. A fixture is created when its target subsystem exists
(no premature implementation — ROADMAP gates first). Rows are added as
audits continue; status is `OPEN` until the fixture passes in CI.

Sources: SAIWORK `20132bdc` (fix commits cited as `(commit)`), SAIPENVIEW
`5b18d17`, SAIPEN `23bebea`, Freebuff `5661b80`.

## Queue

| # | Test intent | Source lesson | Target | Status |
| --- | --- | --- | --- | --- |
| Q-01 | A queued prompt is never lost between dequeue and dispatch, even across a crash in that window | donor had no per-item lease; restore-on-failure was manual | `saiwork-queue` | OPEN |
| Q-02 | Exactly one of two concurrent claims wins — never double dispatch | manager.test "at most one dispatch" | `saiwork-queue` | OPEN |
| Q-03 | Stale CAS mutation → structured conflict; nothing changes | manager.test "rejects a stale mutation" | `saiwork-queue` | OPEN |
| Q-04 | Stale lease recovers deterministically at startup, before any new dispatch | spec §14; donor lacked leases | `saiwork-queue` | OPEN |
| Q-05 | Failed dispatch re-queues (bounded attempts), never deletes work silently | manager.test "restore … at the front and pauses atomically" | `saiwork-queue` | OPEN |
| Q-06 | Crash between claim and send leaves a recoverable LEASED row, not invisible work | master gate phase 2 | `saiwork-queue` | OPEN |
| Q-07 | Corrupt/future persistence fails closed: no mutations, bytes untouched | manager.test "fails closed on corrupt and future persistence" | `saiwork-storage` | OPEN |
| Q-08 | Fan-out atomicity: any conflict/persist failure → zero targets changed, disk untouched | manager.test fan-out suite | `saiwork-queue` | OPEN |
| Q-09 | queue.changed emitted only after successful transition | manager.test "publishes queue.changed after a successful mutation" | `saiwork-queue` | OPEN |
| Q-10 | Queue persistence never blocks the UI/event path | fixed `5135c5d` (sync fs blocked event loop) | `saiwork-storage` | OPEN |
| Q-11 | Edit/move/delete/reorder are revision-safe | manager.test "moves, updates, removes, clears and pauses with revision bumps" | `saiwork-queue` | OPEN |
| Q-12 | Flush: an admitted mutation is durable before shutdown completes | manager.test "flush waits for a mutation admitted before shutdown" | `saiwork-queue` | OPEN |

## Process / lifecycle

| # | Test intent | Source lesson | Target | Status |
| --- | --- | --- | --- | --- |
| P-01 | Readiness is predicate-based; no fixed-sleep startup tax | fixed `c586934` (1500 ms sleep) | `engine adapters` (ADR-015: readiness left the supervisor) | OPEN |
| P-02 | Process starts but never becomes ready → killed + FAILED within timeout | supervisor test (already written) | `engine adapters` (ADR-015) | OPEN |
| P-03 | Graceful stop timeout escalates to force kill; force-kill timeout reports failure, never hangs | donor staged timeouts 2/5/3 s; Freebuff "never reports stopped when forced shutdown cannot contain the child" | `saiwork-process` | DONE (TASK 06: graceful_stop_is_bounded, force_kill_terminates, exit_vs_stop_race) |
| P-04 | 0 orphan child processes after normal exit | M0 gate; donor orphan registry sweep | `saiwork-process` | DONE (TASK 06: killing_parent_tree_kills_descendants + shutdown clears; packaged 0-orphan gate remains at TASK 09) |
| P-05 | Recycled pid is never touched: orphan sweep verifies process identity (start time), not pid alone | orphan-cleanup.ts identity match | `saiwork-process` | DONE by design (TASK 06: `ProcessId != PID`; no persistent PID authority, supervisor acts only on children it spawned this run) |
| P-06 | Crash loop is bounded (e.g. three respawns) and closes lifetime state | Freebuff engine.test "bounds a crash loop to three same-port respawns" | `engine-opencode` | OPEN |
| P-07 | Forged readiness is ignored: probe only the owned pid + launch identity | Freebuff engine.test "ignores forged readiness and only probes the owned PID" | `engine-opencode` | OPEN |
| P-08 | Output ring: rotation at capBytes (not retainBytes); UTF-8-safe truncation; hard read cap | fixed `0fa34df` (rotation fired at retainBytes) | `saiwork-process` | DONE (TASK 06: caps_total_bytes, large_output_stays_bounded, invalid_utf8 lossy) |
| P-09 | Single-flight output tick: slow read cannot duplicate bytes or reorder rotation | SingleFlightTicker | `saiwork-process` | DONE by design (TASK 06: one reader task per stream, per-line push — no tick to race) |
| P-10 | Windows tree termination via taskkill /T /F as last resort; console-only graceful path documented | background-process manager | `saiwork-process` | DONE (TASK 06: superseded by Job Object ownership, ADR-014; taskkill remains only the graceful hint) |

## SSE / streaming

| # | Test intent | Source lesson | Target | Status |
| --- | --- | --- | --- | --- |
| S-01 | SSE frames are line-ending normalized (CRLF) before parsing | Antigravity fix `8287287` (LF-only split collapsed the stream) | `engine-opencode` | OPEN |
| S-02 | One publish → exactly one consumer event; no double-send on drain | fixed `f2cd239` | `saiwork-events` forwarder | OPEN |
| S-03 | Slow consumer is bounded: overflow disconnects once and forces authoritative re-sync | fixed `ff1833e` | `saiwork-events` | DONE (TASK 07: `slow_consumer_never_blocks_producer` — bounded broadcast, Lagged path) |
| S-04 | Malformed/truncated stream event does not crash the app; contained as debug raw event | FakeEngine malformed test (already written); google sanitize | core+UI | DONE (TASK 07: `malformed_raw_frame_is_contained_and_stream_continues` at the pushRaw boundary) |
| S-05 | Unknown event type is preserved and ignored by the reducer, never a crash | FakeEngine gate | UI store | OPEN |
| S-06 | Duplicate event is idempotent or explicitly handled | Freebuff replay dedup; spec §32 | engine adapters | DONE (TASK 07: duplicate raw frame contained at boundary; duplicate-delta tolerance asserted) |
| S-07 | Event storm (10k deltas) stays responsive; bounded bus, batched UI | FakeEngine flood test (already written) | whole stack | DONE (TASK 07: `large_stream_completes_without_deadlock`; cancel under pressure stops the producer) |

## Paths / security

| # | Test intent | Source lesson | Target | Status |
| --- | --- | --- | --- | --- |
| X-01 | `..` escape blocked | saipenview paths; SAIWORK isPathWithin | `saiwork-core` | OPEN |
| X-02 | Symlink/junction inside workspace pointing outside → blocked (realpath containment) | fixed `0652a5b` (lexical-only containment escaped) | `saiwork-core` | OPEN |
| X-03 | Windows case-insensitive path equality; UNC / `\\?\` forms handled | saipenview canonical paths | `saiwork-core` | OPEN |
| X-04 | Late writer after delete cannot resurrect a namespace/entity | fixed `f2cd239` storage identity | `saiwork-storage` | OPEN |

## SAIPEN

| # | Test intent | Source lesson | Target | Status |
| --- | --- | --- | --- | --- |
| A-01 | STATE.md parsed as YAML frontmatter; duplicate scalar reported as issue, never silently resolved | saipen/state.ts | `saiwork-saipen` | OPEN |
| A-02 | BOARD ticket status from section, not checkbox | saipen/board.ts | `saiwork-saipen` | OPEN |
| A-03 | No second SAIPEN writer/runtime: structural guard test | saipen/no-second-runtime.test.ts | `saiwork-saipen` | OPEN |
| A-04 | Watcher survives rename/atomic-replace; one fs event → one structured event | saipenview watcher moved-event handling | `saiwork-saipen` | OPEN |
| A-05 | Event storm coalesced; bounded self-heal backstop, never full-tree polling | saipenview DEBOUNCE_DELAY; SAIWORK self-heal 10 s | `saiwork-saipen` | OPEN |
| A-06 | Malformed OUTBOX/state fails closed (structural error, not last-write-wins) | saipenview outbox.py | `saiwork-saipen` | OPEN |
| A-07 | Concurrent/locked SAIPEN state surfaces as WRITER_BUSY / STALE_STATE / CONFLICT, never success | saipenview protocol_write results | `saiwork-saipen` | OPEN |
| A-08 | STALE_STATE → re-run decision once on a fresh snapshot; never blind retry of stale bytes | saipenview protocol_write | queue/saipen retry policy | OPEN |

## Misc

| # | Test intent | Source lesson | Target | Status |
| --- | --- | --- | --- | --- |
| M-01 | Secrets never reach logs: auth headers, keys, tokens, passwords redacted at the boundary | fixed `0fa34df`; log-sanitize.ts; google/sanitize.ts | `saiwork-diagnostics` (tests written) | OPEN |
| M-02 | Single instance: second launch relays intent; backlog ≥ 2 so handoff never wedges | saipenview guard.py (backlog-1 bug) | src-tauri | OPEN |
| M-03 | Portable data root: exactly one writable root; relocation preserves DB; cache deletable | PORTABILITY.md + donor START.bat semantics | `saiwork-core` | OPEN |
| M-04 | Free-tier vs quota error classification prevents wrong user advice | google/errors.ts | error model | OPEN |
| M-05 | Provider fallback never implicit (billing-pool protection) | google/adapter allowFallback | engine adapters | OPEN |
