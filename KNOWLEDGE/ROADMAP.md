# ROADMAP.md — 18 Sequential Tasks

One task at a time. A task is DONE only when its gate passes; a RED gate blocks
the next task. Detailed implementation instructions arrive per task.

| # | Task | Goal | Depends on | Main output | Gate (must pass) |
| --- | --- | --- | --- | --- | --- |
| 01 | Donor Audit & Salvage Map | factual baseline of 4 donors + salvage map | — | MIGRATION_SAIWORK.md, baselines, regression backlog | baselines recorded; every subsystem classified; backlog exists |
| 02 | KNOWLEDGE Foundation | canonical, self-sufficient KNOWLEDGE base | 01 | all KNOWLEDGE docs consistent | no contradictions; authorities explicit; 18-task roadmap |
| 03 | Repository + Desktop Skeleton | greenfield repo, Tauri shell, data root, single instance | 02 | repo layout, Tauri app starts, portable root | desktop starts; exits cleanly; second instance relays intent |
| 04 | Core Contracts + EventBus | EngineAdapter contract + normalized bounded EventBus | 03 | `saiwork-events`, contract types | taxonomy tests; lag/order tests; events-as-facts enforced |
| 05 | Storage Foundation | SQLite + migrations + failure contract | 03 | `saiwork-storage` | migrations idempotent; corrupt/locked semantics per STORAGE.md |
| 06 | ProcessSupervisor | single child-process owner | 03, 04 | `saiwork-process` | staged stop; bounded output; tree cleanup (Job Object); 0-orphan test |
| 07 | FakeEngine | first adapter with failure simulation | 04, 05 | `engine-fake` | streams; cancel; malformed contained; flood bounded |
| 08 | Application Lifecycle | bootstrap, shutdown order, diagnostics, workspace manager | 05, 06, 07 | `saiwork-core` | clean shutdown sequence; state survives restart; single instance |
| 09 | Phase 0 Integration Gate | wire UI + core end-to-end with FakeEngine | 03–08 | working M0 skeleton | M0 gate list (master §60): streams, cancels, 0 orphans, migrations, no crash on malformed |
| 10 | OpenCode Process Adapter | spawn `opencode serve` via supervisor, readiness, models | 06, 09 | `engine-opencode` process layer | spawn; readiness probe; model discovery; crash handled; clean stop |
| 11 | OpenCode Session Vertical Slice | sessions, send, stream, cancel, resume | 10 | full OpenCode path | session create; streaming; cancel; resume; UI responsive |
| 12 | Phase 1 Hardening | hostile matrix + baseline measurement | 11 | hardened slice + PERFORMANCE baselines | hostile tests green; baselines recorded; 0 orphans |
| 13 | Durable Queue | queue manager over SQLite schema | 05, 09 | `saiwork-queue` | invariants (QUEUE.md); crash-between-claim-and-send fixture |
| 14 | SAIPEN Read Integration | detect/read/watch/validate read-only | 05, 09 | `saiwork-saipen` (read) | detected; external changes reflected; no polling; path escape blocked |
| 15 | SAIPEN Actions + SAIPENBAR | canonical actions, full SAIPENBAR | 14 | write path via canonical writers | canonical mutation path used; no second writer; malformed/locked handled |
| 16 | Primary Desktop UX | polish: sidebar, sessions, composer, queue panel, keyboard (layout persistence DEFERRED — not in V1, see README) | 09, 13, 15 | v1 UI | UX truth rules hold; queue/SAIPEN visible; window/layout persistence not claimed (deferred) |
| 17 | Additional Engines | Freebuff (isolated), generic CLI adapter | 12 | `engine-freebuff`, `engine-generic-cli` | isolated adapters; no core leakage; capability-driven UI |
| 18 | Parallelism + Release Hardening | multiple sessions/engines, resource limits, packaging (worktrees DEFERRED — README states no worktrees in V1) | 16, 17 | parallel execution + release pipeline | per-run controls; resource limits; packaged Windows smoke green |

Current state: TASK 01–18 DONE — **SAIWORK2 V1 RELEASE GATE PASS** (ADR-038). Phase 1 gate (TASK 12) PASS: protocol
37/0, hostile 26/0, engine-opencode lib 30/0, Phase 0 green, race repeats
clean. TASK 13 durable queue PASS: `saiwork-queue` (17 repo + 19 manager
integration tests, race repeats clean), migration v2, QueuePanel UI proof.

> NOTE (T-024): two items named in TASK 16/18 are **deferred**, not completed,
> and must not count toward the V1 gate: window/layout persistence (no
> implementation exists in V1 — only remembered on explicit user intent) and
> worktrees (README/RELEASE_NOTES explicitly state "no worktrees in V1"). The
> gate above remains valid for implemented scope only.
TASK 14 SAIPEN read PASS: `saiwork-saipen` (22 unit + 7 service integration
tests) verified against donors/saipen v7.224.3 (schema_version 3) — strict
STATE/BOARD parsers, component-aware path containment, one notify watcher
per root with debounce/coalesce/overflow/generation tags, read-only
assertion suite, SaipenBar projection.
TASK 15 SAIPEN actions + SAIPENBAR PASS: `saiwork-saipen` action module
(20 action tests + 2 real-canonical-validator regression tests) — verified
action surface (status/validate canonical, board/knowledge views,
continue/stop honestly unsupported per v7.224.3 CLI), ActionManager with
backend-enforced per-workspace exclusivity, lifecycle, cancellation,
bounded timeout, validation-generation staleness; real `validate.py`
exit semantics proven end-to-end (0 conformant / 1 domain-invalid /
2 usage); SAIPENBAR composes SAIPEN + validation + action status from their
own authorities; zero canonical writes (write-audit clean); SAIPEN→Queue
handoff deliberately deferred (no canonical Continue exists).
TASK 16 PRIMARY DESKTOP UX PASS: three-pane cockpit (TitleBar · left nav
projects+sessions · Conversation · ActivityPanel tabs · Composer Send/Queue/
Cancel · SAIPENBAR strip · statusline), Golden Vintage token system,
stream batching (N deltas → 1 render per frame, terminal flush, no global
rerender — verified by tests), Markdown at terminal with copyable code
blocks, stick-to-bottom scroll + jump-to-latest, permission Allow/Deny via
new `resolve_permission` command, honest disabled states, diagnostics copy,
window title; frontend suite 13/13 (vitest); react-markdown+remark-gfm the
only new runtime dependency; responsive collapse; fmt/clippy/workspace-
tests/typecheck/build/test green.
TASK 17 ADDITIONAL ENGINES PASS: Freebuff re-verified from the vendored
snapshot (`@codebuff/sdk` 0.10.7, Apache-2.0) and classified **DEFERRED**
(remote-cloud-only, Node≥22-only SDK, cloud credential vault required,
CLI is the full app — ADR-036). Second production engine shipped:
`engine-generic-cli` (OneShotText — trusted env config, no shell, prompt
as stdin bytes, bounded output/timeout, run==process cancel, honest
capabilities sessions=true/resume=false/streaming=false/cancel=true;
ADR-037). Generic `saiwork-process` extensions: `StdinPolicy::Bytes` +
per-process output cap (both bounded, OpenCode/FakeEngine unaffected).
EngineRegistry hosts fake+opencode+generic-cli; per-engine health +
capabilities in `list_engines` and diagnostics; UI model selector is
capability-driven with a generation guard against stale discovery
responses; per-engine diagnostics row. Tests: 20 adapter tests + 5
cross-engine isolation tests (ID collision, no-fallback, failure
isolation, stop_all) + 3 frontend model-load tests; full workspace
regression green, race repeats clean, fmt/clippy clean.
TASK 18 PARALLELISM + RELEASE HARDENING PASS: parallelism decision =
**one agent run per workspace** (same-workspace send rejected with typed
`CoreError::WorkspaceBusy`; queue `session_busy` honors the same gate and
waits; New-mode dispatch checks busy before send), **different-workspace
runs concurrent and isolated**, **same-session REJECT unchanged** (CLI
adapter now enforces it), **queue concurrency = 1** (documented boundary,
ADR-038). New `parallelism` integration suite (4 tests, 3× stable) + CLI
same-session test + storage v1→current upgrade-preservation test.
Release hardening: lock/TODO/panic/unwrap/polling/duplication audits clean;
minimal CSP applied (no unsafe-eval, no unrestricted connect-src); input
bounds verified (queue 64 KiB payload, prompts 1 MiB/64 KiB); FakeEngine
gated to debug builds; queue failpoints no-op in release; upgrade/migration
fresh-vs-migrated + failed-migration + future-schema + corrupt-file tests;
packaged release build PASS: MSI + NSIS bundles, portable fresh launch
Ready in 7 ms, release registers 1 engine (OpenCode), single-instance
verified (second launch relays + exits, one process). V1 RELEASE GATE PASS.
TASK 18 DONE.

Milestone M0 — "Skeleton That Refuses To Rot" — is the aggregate gate of
TASK 03–09 (master spec §60); nothing beyond the skeleton opens before it.

---

## Post-V1 roadmap (TASK 19–24) — DeepSeek Harness + multi-engine hardening

Current state: TASK 01–18 DONE (V1 release gate PASS). TASK 19 DONE (audit + contract,
ADR-039, KNOWLEDGE/DEEPSEEK_HARNESS.md). TASK 20 DONE (adapter foundation, ADR-040,
DEEPSEEK_HARNESS.md §22). TASK 21 DONE (agent vertical slice, ADR-041,
DEEPSEEK_HARNESS.md §23). TASK 22 DONE (capability/runtime audit, ADR-042). TASK 23 DONE
(Harness queue + OUTCOME_UNKNOWN, ADR-043). TASK 24 DONE (post-V1 multi-engine hardening,
ADR-044).

- TASK 19 — Harness Audit + Integration Contract: DONE. Baseline tree `47f9438`,
  `@deepseek-ai/dsh` 0.1.0-rc.6, MIT. Classification B (EXPERIMENTAL ENGINE CANDIDATE).
  Seam decision: ACP over stdio preferred, SDK JSON-RPC fallback. No production code.
- TASK 20 — Harness Engine Adapter Foundation: **DONE**. `crates/engine-deepseek-harness`
  (ACP over stdio: NDJSON JSON-RPC transport, initialize handshake, lifecycle+
  generation, typed errors), `saiwork-process` protocol-mode stdio
  (`StdinPolicy::Piped` + `stdout_protocol`), desktop registration when configured,
  29-test hostile matrix (3× stable), registry isolation, clippy/fmt clean, real smoke
  BLOCKED UPSTREAM (npm CLI has no acp entry; `@deepseek-ai/dsh-acp` published but needs
  composition — TASK 21 probe gate). ADR-040.
- TASK 21 — Harness Agent Vertical Slice: **DONE**. `crates/engine-deepseek-harness`
  vertical slice (ADR-041): authoritative `session/new` (fresh + connection-owned),
  `session/prompt` runs with exactly-one-terminal stop-reason mapping, committed-chunk
  streaming → `message.delta`, `tool_call` → `tool.*`, `request_permission` → generic
  permission round-trip (fail-closed), scoped `session/cancel` (race-safe), transport
  loss/restart recovery, same-session REJECT + parallel sessions. New modules runs.rs /
  sessions.rs / permissions.rs / events.rs; 28-test vertical matrix; generic
  `EngineIdentity.experimental` flag (Harness = true, UI marks ⚠). Real Windows E2E
  BLOCKED EXTERNAL (ACP profile still needs source composition — fixture matrix proves
  the workflow deterministically). QueueManager dispatch still disabled.
- TASK 22 — Capability / Runtime Architecture Improvements: **DONE** (ADR-042). Every
  Harness-derived candidate classified (DEEPSEEK_HARNESS.md §23 table). Adopted: event
  semantic classification documented in EVENTS.md (durable/live/stream/invalidation +
  reconstruction sources), one real cleanup bug fixed (partial-initialization leak in
  Harness `start()`), one generic-UI leak removed (TitleBar tooltip). Everything else
  ALREADY SOLVED / DEFER / REJECT — no dynamic plugin / service-locator / effect
  framework, no capability ontology, no new dependency, no DB migration. Static
  EngineAdapter registration remains the V1 architecture.
- TASK 23 — Harness + Queue + SAIPEN Hardening: **DONE** (ADR-043). Harness is a proven
  durable QueueManager target through the generic EnginePort path
  (`tests/queue_slice.rs`); `QueueState::Unknown` added (crash during handoff or
  DISPATCHED at restart → unknown: never auto-dispatched, blocks its workspace,
  user-resolved via risk-acknowledged retry/cancel); correlation persists via existing
  `session_id` + `run_id` (ACP has no durable TurnId/idempotency key — honest UNKNOWN
  fallback); no DB migration; SAIPEN→Queue **explicitly DEFERRED** (no canonical
  `continue`, no stable execution identity).
- TASK 24 — Post-V1 Multi-Engine Hardening: **DONE** (ADR-044). New cross-engine hostile
  matrix (`tests/multi_engine.rs`, 6 tests) proves session/run isolation, exact queue
  routing, target immutability, one-engine failure isolation, same-workspace cross-engine
  serialization, and the fail-closed session-id collision guard (ADR-044). Static audits
  (vendor leaks, process spawn, frontend network, SAIPEN writes, queue SQL, polling,
  retries, TODO/panic, credentials, shell interpolation) all clean. DeepSeek Harness
  remains **EXPERIMENTAL** (upstream Developer Preview); real-Harness queue smoke stays
  BLOCKED EXTERNAL (no provider/dsh tree in this environment).

---

## Freebuff-inspired Desktop UX campaign (post-V1, in progress)

> NOTE: this campaign layers a Freebuff-style cockpit onto the existing SAIWORK2 core.
> It does NOT change queue authority, process ownership, workspace/session authority,
> engine capability truth, or SAIPEN integration. No fake controls: later-phase panels
> (Files/Changes/Preview/Terminal) ship as explicit placeholders until their backend
> domain owners exist.

| Phase | Scope | Status |
|-------|-------|--------|
| B | Dock shell + rail + Thread tabs + UI layout persistence | DONE (typecheck clean, vitest 55/55) |
| C | Files (read-only, project_files.rs) + Changes (git.rs, porcelain=v2 -z) | TODO |
| D | Queue Freebuff UX + RunProfile + Skills snapshot + Mission + structured To-dos | TODO |
| E | Preview (explicit managed targets, no port scan) | TODO |
| F | Reasoning effort (capability-driven, real adapter mapping) | TODO |
| G | Isolation / worktrees (ExecutionWorkspace) | TODO |
| H | Terminal (PTY via ProcessSupervisor) | TODO |
