<p align="center">
  <img src="apps/desktop/src-tauri/icons/128x128.png" width="96" alt="SAIWORK2 icon">
</p>

<h1 align="center">SAIWORK2</h1>

<p align="center"><strong>A durable desktop cockpit for coding agents.</strong></p>

<p align="center">
  <img alt="Version 0.1.6" src="https://img.shields.io/badge/version-0.1.6-C8A44D?style=flat-square">
  <img alt="Tauri 2" src="https://img.shields.io/badge/Tauri-2-8B6F32?style=flat-square">
  <img alt="Rust core" src="https://img.shields.io/badge/core-Rust-B7410E?style=flat-square">
  <a href="LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-6B5A2B?style=flat-square"></a>
</p>

<p align="center">
  <a href="#what-v1-can-do">Features</a> ·
  <a href="#build--run">Build and run</a> ·
  <a href="KNOWLEDGE/INDEX.md">Architecture knowledge</a> ·
  <a href="CHANGELOG.md">Changelog</a>
</p>

Desktop orchestration cockpit for coding agents — **v0.1.6**.

One workspace cockpit, one durable queue, one process lifecycle, one event
model, one SAIPEN control plane, one predictable desktop UX. The UI is a
projection of Rust-core state; it never becomes the authority for runtime or
durable state.

**Status: V1 release + multi-engine gate (TASK 24 DONE).** All 24 build
tasks are complete (see `KNOWLEDGE/ROADMAP.md`). SAIWORK2 is a Tauri 2
desktop app with a Rust core and a React/TS frontend. The primary engine is
**OpenCode** (local `opencode serve` child process) — SUPPORTED. A second
production engine, **Generic CLI** (trusted one-shot executable), is
supported when explicitly configured. **DeepSeek Harness is EXPERIMENTAL**
(upstream Developer Preview; adapter + queue target proven against a
deterministic ACP fixture, real-provider smoke blocked externally — see
`KNOWLEDGE/DEEPSEEK_HARNESS.md`). The durable queue and the SAIPEN
read/watch/action integration are fully implemented. **Freebuff is
deferred** (remote-cloud-only, Node-only SDK — see
`KNOWLEDGE/DECISIONS.md` ADR-036).

## What V1 can do

- Open/select/switch projects; SAIPEN presence indicator per project.
- Auto-start/rebind the selected OpenCode engine when a project opens; manual
  start/stop remains available; select models by canonical ID.
- Auto-create, select, resume and safely delete OpenCode sessions; send
  prompts, watch streamed answers, cancel active runs, and undo/redo turns.
- Answer structured agent questions (single/multi-select or custom text) and
  see absolute plus live relative timestamps on every message/tool call.
- Queue future work (add / edit / reorder / pause / resume / cancel /
  retry-safe items) through the one durable QueueManager.
- Run canonical SAIPEN actions (Status, Validate) and read-only
  Board/Knowledge views; SAIPENBAR shows project/state/task/next/blocker/
  queue/active-agents/engine/model/validation truth.
- Diagnostics panel with a redacted snapshot (copyable); logs under the
  data root.
- Multiple workspaces and sessions; **one agent run per workspace** at a
  time (same-workspace second runs are rejected with a clear message; the
  queue waits), different workspaces run concurrently (ADR-038).

## Concurrency policy (V1)

- Same-session: REJECT (one agent turn per thread).
- Same-workspace: serialized — one mutating agent run per physical
  workspace (no worktrees in V1).
- Different workspaces: concurrent and isolated.
- Durable queue: concurrency = 1 (single dispatcher; the strongest proven
  correctness boundary).

## Engines

| Engine | Status | Notes |
| --- | --- | --- |
| OpenCode | production | `opencode` ≥ 1.18 on PATH; sessions, models, streaming, tools, cancel, structured questions, undo/redo |
| Generic CLI | production, opt-in | one-shot trusted executable; configured via `SAIWORK2_CLI_EXECUTABLE` (+ optional `SAIWORK2_CLI_ARGS`, `SAIWORK2_CLI_LABEL`, `SAIWORK2_CLI_MAX_OUTPUT_BYTES`, `SAIWORK2_CLI_TIMEOUT_MS`); no shell, prompt via stdin, bounded output/timeout |
| FakeEngine | dev/test only | registered in debug builds for the failure-simulation suites (`/sim:normal`, `/sim:slow`, `/sim:hang`, …); never in release |
| Freebuff | deferred | remote-cloud-only; Node ≥ 22 SDK; credential vault required (ADR-036) |

## Prerequisites

- Windows 10/11 (primary), macOS/Linux buildable via the Tauri toolchain.
- Rust stable toolchain.
- Node 20+ (npm) for the frontend build.
- WebView2 runtime (preinstalled on Win 10/11).
- OpenCode for the primary engine (`npm i -g opencode-ai` or the binary on
  PATH). SAIWORK2 uses OpenCode's own provider/auth ecosystem — you do not
  re-enter provider keys in SAIWORK2.

## Build / run

```bash
npm install                 # frontend + contracts workspaces
cargo build --workspace     # compile core crates + shell (first run is long)
npm run tauri dev           # launch the desktop app (dev)
npm run tauri build         # release build + MSI/NSIS bundles
```

The app: open a project → select a model if needed → type a prompt. The engine
and first session start automatically. Enter sends by default; enable
`Enter queues` to make Enter durable-enqueue and Ctrl+Enter send.

## Data root

`SAIWORK2_DATA_DIR` → `portable.flag` beside the executable (`<exe>/data`)
→ OS application-data directory. Exactly one writable root
(`KNOWLEDGE/PORTABILITY.md`). Portable mode keeps everything (DB, logs,
settings) under the portable `data/`; it never drifts to OS AppData.

What SAIWORK2 stores: SQLite DB (app settings, project references, session
metadata, durable queue prompts — plaintext, documented in
`KNOWLEDGE/STORAGE.md` + `KNOWLEDGE/QUEUE.md`), logs, runtime dir.
What it never stores: provider credentials (engines own them), OpenCode
transcript mirrors, SAIPEN canonical file mirrors.

## Known V1 limitations (P2+, safe to defer)

- No terminal emulator, no full IDE/file explorer, no browser, no
  collaboration, no plugin marketplace, no worktrees.
- Freebuff integration deferred (no stable local-execution surface).
- SAIPEN→Queue handoff deferred (canonical SAIPEN has no CLI Continue).
- Windows path matrix beyond the tested set (spaces/Unicode/portable) and
  installer-driven upgrade flows are exercised on demand.

## Repository layout

```text
apps/desktop/            Tauri 2 shell + React/TS/Vite frontend
crates/
  saiwork-events/        canonical event taxonomy + bounded EventBus
  saiwork-storage/       SQLite, migrations, workspaces/settings/session meta
  saiwork-process/       ProcessSupervisor (single child-process owner)
  saiwork-diagnostics/   secret redaction + bounded diagnostics
  saiwork-core/          orchestration: app, workspaces, sessions, engine registry
  saiwork-queue/         durable queue (single authority)
  saiwork-saipen/        canonical SAIPEN read/watch/actions
  engine-fake/           FakeEngine — deterministic test engine
  engine-opencode/       OpenCode process adapter
  engine-generic-cli/    Generic CLI one-shot adapter
packages/contracts/      shared TS types mirroring the Rust contract
KNOWLEDGE/               engineering memory (INDEX.md is the map)
scripts/                 tooling (e.g. gen-icons.mjs)
```

## License

MIT — see [LICENSE](LICENSE). Third-party inventory: `KNOWLEDGE/THIRD_PARTY.md`.
