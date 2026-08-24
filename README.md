<p align="center">
  <img src="apps/desktop/src-tauri/icons/128x128.png" width="96" alt="SAIWORK2 icon">
</p>

<h1 align="center">SAIWORK2</h1>

<p align="center"><strong>A durable desktop control plane for coding agents.</strong></p>

<p align="center">
  <a href="https://github.com/vacterro/saiwork2/releases/tag/v0.1.7"><img alt="Release 0.1.7" src="https://img.shields.io/badge/release-0.1.7-C8A44D?style=flat-square"></a>
  <img alt="Tauri 2" src="https://img.shields.io/badge/Tauri-2-8B6F32?style=flat-square">
  <img alt="Rust core" src="https://img.shields.io/badge/core-Rust-B7410E?style=flat-square">
  <a href="LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-6B5A2B?style=flat-square"></a>
</p>

<p align="center">
  <a href="#highlights">Highlights</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#documentation">Documentation</a> ·
  <a href="https://github.com/vacterro/saiwork2/releases">Releases</a>
</p>

SAIWORK2 keeps projects, agent sessions, queued prompts, tool activity, and
engine processes in one predictable desktop workspace. The React interface
projects authoritative Rust-core state; it does not own processes, persistence,
or recovery.

Windows-first. OpenCode is the primary local engine, with an opt-in Generic CLI
adapter for trusted one-shot tools.

## Highlights

- **Start working immediately.** Opening a project starts or rebinds its engine;
  sending the first prompt creates a session automatically.
- **One composer, clear intent.** Enter sends by default, Shift+Enter adds a
  line, and Ctrl+Enter places work in the durable queue. The mapping is
  configurable.
- **Durable queued work.** Add, edit, reorder, pause, resume, cancel, and retry
  prompts without turning the frontend into a second queue authority.
- **Agent-native conversations.** Streaming answers, tool activity, permission
  decisions, structured questions, session deletion, and turn undo/redo share
  one transcript.
- **Time is visible.** Every message and tool call shows an absolute timestamp
  and a live relative age.
- **Failure stays honest.** Unknown outcomes remain `UNKNOWN`; uncertain work is
  never silently retried, and active-run ownership fails closed.
- **SAIPEN-aware.** Project state, task, blocker, validation, Board, and
  Knowledge views are available without mirroring canonical `.saipen` files.

## Quick start

Prerequisites: Windows 10/11 with WebView2, Node.js 20+, Rust stable with the
MSVC toolchain, Visual Studio Build Tools C++, and
[OpenCode](https://opencode.ai/) on `PATH`.

```bash
npm install
npm run tauri dev
```

Open a project, select a model when needed, and type a prompt. Engine and first
session startup are automatic; explicit start/stop controls remain available.

Build Windows installers locally with:

```bash
npm run tauri build
```

## Engine support

- **OpenCode — production:** local `opencode serve`; sessions, models,
  streaming, tools, structured questions, cancellation, and undo/redo.
- **Generic CLI — production, opt-in:** trusted executable, prompt over stdin,
  bounded output and timeout, no shell interpolation.
- **DeepSeek Harness — experimental:** ACP adapter and deterministic queue path
  are implemented; real-provider availability still depends on upstream tooling.
- **FakeEngine — development only:** deterministic failure simulation; excluded
  from release builds.

## Architecture

```text
React / TypeScript UI
          │ commands + events
          ▼
Rust orchestration core
 ├─ SQLite durable queue
 ├─ Process supervisor ── Engine adapters
 ├─ Workspace + session metadata
 └─ SAIPEN read / watch / actions
```

The boundaries are deliberate: one process owner, one durable queue owner, one
event bus, and one desktop runtime. Different workspaces may run concurrently;
mutating agent runs inside the same workspace are serialized.

## Data and privacy

- Local desktop app: no account, telemetry service, or SAIWORK2 cloud.
- Provider credentials stay in each engine's existing authentication store.
- SQLite stores settings, project references, session metadata, and queued
  prompts. Queue prompts are plaintext at rest.
- Portable mode keeps the database, logs, settings, and runtime data under one
  deterministic `data/` directory beside the executable.

## Development

```bash
cargo test --workspace --exclude saiwork2
cargo test -p saiwork2
npm run typecheck
npm test
npm run build
```

The Tauri development and release shell requires the Windows MSVC toolchain.
Core crates can also be tested with the Windows GNU toolchain.

## Documentation

- [Product contract](KNOWLEDGE/PRODUCT.md)
- [Architecture and ownership laws](KNOWLEDGE/ARCHITECTURE.md)
- [Engine contract](KNOWLEDGE/ENGINE_CONTRACT.md)
- [Queue guarantees](KNOWLEDGE/QUEUE.md)
- [Process lifecycle](KNOWLEDGE/PROCESS_LIFECYCLE.md)
- [Testing strategy](KNOWLEDGE/TESTING.md)
- [Changelog](CHANGELOG.md) and [release notes](RELEASE_NOTES.md)

## Current limitations

SAIWORK2 is Windows-first and is not a full IDE. It does not yet include a
terminal emulator, worktree isolation, cloud collaboration, or an automatic
SAIPEN-to-queue handoff. See the [roadmap](KNOWLEDGE/ROADMAP.md) for engineering
status and planned work.

## License

MIT — see [LICENSE](LICENSE). Third-party inventory:
[KNOWLEDGE/THIRD_PARTY.md](KNOWLEDGE/THIRD_PARTY.md).
