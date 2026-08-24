# SAIWORK2 V1 — Release Notes (0.1.0)

First release-grade desktop cockpit for coding agents. Windows 10/11
primary; built with Tauri 2 + Rust core + React/TS frontend.

## What's in V1

- **OpenCode engine** (primary): local `opencode serve` process owned by the
  ProcessSupervisor; models, sessions (create/select/resume), streaming
  answers, tool activity, per-run cancellation, engine lifecycle.
- **Generic CLI engine** (opt-in second production engine): one-shot trusted
  executable, prompt via stdin, bounded output/timeout, run==process cancel.
  Configure with `SAIWORK2_CLI_EXECUTABLE` (etc.).
- **Durable queue**: add / edit / reorder / pause / resume / cancel / safe
  retry; crash-recovery matrix with no duplicate dispatch; concurrency = 1.
- **SAIPEN**: read-only watcher projection (state/board/next/blocker),
  canonical `status` + `validate` actions through the process supervisor,
  SAIPENBAR status strip. SAIWORK2 never writes canonical SAIPEN files.
- **Workspaces**: open/select/switch; one agent run per workspace
  (same-workspace runs are serialized; different workspaces run
  concurrently).
- **Diagnostics**: redacted snapshot, copyable; logs under the data root.

## External requirements

- OpenCode on PATH for the primary engine (`npm i -g opencode-ai` or the
  binary). SAIWORK2 uses OpenCode's existing provider/auth setup — no keys
  are entered in SAIWORK2.
- WebView2 runtime (preinstalled on Windows 10/11).

## Data facts

- Data root: `SAIWORK2_DATA_DIR` → `portable.flag` → OS app-data. Portable
  mode keeps everything under `<exe>/data`.
- SAIWORK2 stores: SQLite (settings, project references, session metadata,
  **queue prompts in plaintext**), logs, runtime state. It does not store
  provider credentials, transcript mirrors, or SAIPEN canonical mirrors.
- No telemetry, no account, no cloud.

## Known limitations (P2+)

- Freebuff deferred (remote-cloud-only; Node-only SDK).
- SAIPEN→Queue handoff deferred (no canonical CLI Continue).
- No worktrees/terminal/IDE/browser/collaboration/marketplace.
- Drag-and-drop queue reorder not included (buttons used).

## Release artifacts

- `SAIWORK2_0.1.0_x64-setup.exe` (NSIS), `SAIWORK2_0.1.0_x64_en-US.msi`
  (WiX) under `target/release/bundle/`.
