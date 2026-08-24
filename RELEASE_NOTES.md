# SAIWORK2 v0.1.7

Patch release of the durable desktop control plane for coding agents. Windows
10/11 is the primary platform; the application uses Tauri 2, a Rust core, and
a React/TypeScript frontend.

## Highlights

- Queued cancellation now fails closed on storage errors while preserving safe
  compare-and-swap race recovery.
- OpenCode sessions with model selection, streaming responses, tool activity,
  permission handling, cancellation, and history restoration.
- Automatic engine startup when a project is selected and automatic session
  creation on the first prompt.
- Safe session deletion plus turn undo/redo for engines that support it.
- Structured single-choice, multi-choice, and custom-text agent questions.
- Absolute and live relative timestamps for every message and tool call.
- Durable SQLite prompt queue with editing, ordering, pause/resume, recovery,
  and fail-closed `UNKNOWN` outcomes.
- Opt-in Generic CLI engine with bounded output and timeout.
- Experimental DeepSeek Harness ACP adapter behind the same capability-driven
  engine boundary.
- SAIPEN project status, validation, Board, and Knowledge views without a
  second canonical writer.

## Safety model

- One mutating agent run per workspace; different workspaces remain isolated
  and may run concurrently.
- One process supervisor owns every managed child process.
- Provider credentials remain in engine-owned authentication stores.
- No account, telemetry service, or SAIWORK2 cloud.
- Queue prompts are stored as plaintext in the local SQLite database.

## Build from source

Install Node.js 20+, Rust stable with the Windows MSVC toolchain, Visual Studio
Build Tools C++, WebView2, and OpenCode on `PATH`, then run:

```bash
npm install
npm run tauri build
```

This GitHub release provides source archives. MSI and NSIS installers can be
built locally but are not attached to the public release yet.

## Current limitations

- Windows-first; packaged macOS/Linux releases are not provided.
- No terminal emulator, full IDE, worktree isolation, or cloud collaboration.
- DeepSeek Harness remains experimental and depends on upstream provider
  tooling for a real-provider smoke test.
- SAIPEN actions do not automatically enqueue agent work.
