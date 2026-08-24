# Changelog

## 0.1.6 — 2026-08-24

### Bounded queue dispatch scans
- Queue eligibility now walks fixed 128-row keyset pages instead of materializing the complete remaining queue for every claim.
- Added the v8 composite dispatch index plus coverage for exact page ordering, query plans, and eligible work beyond a blocked first page.

## 0.1.5 — 2026-08-24

### Proven supervisor shutdown
- The final force sweep now waits concurrently for bounded per-process exit proof and reports exact failures.
- Shutdown removes only proven-exited records; live survivors remain under ProcessSupervisor authority and can be retried safely.

## 0.1.4 — 2026-08-24

### Cancellation-safe Harness startup
- DeepSeek Harness publishes partial runtime ownership before its handshake await and uses an RAII guard to clean process, transport, requests, and tasks when the start future is aborted.
- Failed teardown remains owned and blocks restart until exit is proven; hostile coverage now includes direct handshake-task abortion.

## 0.1.3 — 2026-08-24

### Failed-start process authority
- OpenCode now surfaces both readiness and teardown failures, retains runtime/watcher ownership while exit is unproven, blocks unsafe restart, and permits explicit cleanup retry.
- Added a release-excluded supervisor stop failpoint and hostile regression covering the complete failure/recovery path.

## 0.1.2 — 2026-08-24

### Agent UX and lifecycle correctness
- Added safe OpenCode undo/redo and session deletion, automatic session creation and engine startup, durable Enter-to-queue mode, structured agent questions, and per-message/tool timestamps.
- Removed the composer character bottleneck and redundant queue prompt box; repaired WebView-safe confirmations, project-name fallback, and timestamped session naming.
- Restored registration of the stale-terminal run-gate regression so the concurrency invariant is exercised by the normal Rust suite.

## 0.1.1 — 2026-08-20

### Test Rot Fix
- **HUNT-003**: `apps/desktop/src/state/store.test.ts` ROT fixed — seeded a real `w1` session in `sessions` + `currentWorkspaceId:"w1"` in `beforeEach` before scope-gated events (`message.started` / `session.created` / `saipen.changed`); all 14 prior false failures now pass. `store.ts` reducers confirmed correct (enforce CORE-008 / T-045 workspace-scope invariants). Verified `vitest` 26/26 green.

## 0.1.0 — 2026-08-20

First release. Desktop orchestration cockpit for coding agents.

### Core Security & Correctness (7 fixes)
- **CORE-001** (P0): Windows junction/reparse escape in workspace file browser — resolved-target containment + reparse-point rejection
- **CORE-002** (P1): Active-workspace transitions serialized with Mutex, failure-atomic across select/clear/forget/close
- **CORE-003** (P1): RAII ProcessId reservation guard — every post-admission spawn failure releases admission and emits one failure event
- **CORE-004** (P1): Newline-free child output bounded at 256 KiB before line framing
- **CORE-005** (P1): Watcher debounce preserves root-replacement/error events through storm coalescing
- **CORE-006** (P1): Active-workspace mutation rejected after shutdown begins; SaipenService stopped flag prevents watcher resurrection
- **CORE-007** (P2): Removed redundant touch_workspace from workspace open

### Frontend Correctness (5 fixes)
- **W2-001** (P1): SAIPEN stale/freshness single-source — snapshot.stale is the authority on all ingestion paths
- **W2-002** (P1): Editable state ownership generations — delayed async completions cannot overwrite newer user text
- **W2-003** (P1): Create-session responses scoped by workspace — superseded responses don't project into wrong workspace
- **W2-004** (P1): SAIPEN Board/Knowledge views bound to originating workspace
- **W2-006** (P2): Git detection recognizes linked worktrees (.git files with gitdir: pointers)

### Performance (4 fixes)
- **PERF-001** (P1): Queue sync single-flight + generation guard — burst collapses to bounded reads
- **PERF-004** (P2): Composer draft moved to local state — no global-store mutation per keystroke
- **PERF-005** (P2): Model cache single-flight via async OnceCell — concurrent consumers share one fetch
- **PERF-006** (P2): QueuePanel mount nudge only fires when projection is stale or empty

### Infrastructure
- RAII guard pattern for process admission (StartingReservation)
- Centralized Git worktree detection (detect_git_worktree)
- DebounceOutcome enum replaces boolean for watcher control events
- SaipenService persistent stopped flag
