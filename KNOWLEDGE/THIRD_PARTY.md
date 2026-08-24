# THIRD_PARTY.md — Provenance Ledger

Reuse types: `CONCEPT` (idea), `REFERENCE` (studied, not copied),
`REWRITTEN` (behavior preserved, implementation new), `COPIED` (verbatim,
with attribution), `DEPENDENCY` (linked library).

**Nothing is currently COPIED.** All code in this repository is original
implementation informed by the donors below. Any future verbatim reuse
updates this ledger and keeps the original attribution (law: never mask
copied code as original).

## Donor baselines (audited 2026-08-16)

| Repository | Branch | Commit | Version | License | Purpose for SAIWORK2 |
| --- | --- | --- | --- | --- | --- |
| github.com/vacterro/saiwork | `saiwork` | `20132bdcd8b1a4ac99b6f72b68df992a79e4c56f` | 0.1.40 | MIT (© Neural Nomads) | primary historical donor — queue, process, SSE, SAIPEN, OpenCode/Freebuff/Antigravity integration knowledge |
| github.com/vacterro/saipen | `main` | `23bebeafdcd1a2d972ebcde50b0521ca7f26435e` | — (SPEC/RFC versioned in-tree) | MIT (© vacterro) | external canonical protocol authority (SAIPEN.md) |
| github.com/vacterro/saipenview | `main` | `5b18d1710901485961c1a44a995140bcc549b40a` | — (canonical in `saipenview/__init__.py`) | MIT | reliability patterns: watcher, canonical paths, single-instance, ownership, canonical writer |
| github.com/CodebuffAI/freebuff | `main` | `5661b80732ca6cd36ceb7c83366a6ed45470e6e3` | cli 1.0.0 | Apache-2.0 | UX/architecture/optional-engine donor; SDK + agent-runtime concepts |
| github.com/deepseek-ai/deepseek-harness | `master` | tree `47f943859bef60e4160492346772ded9b24f765a` | 0.1.0-rc.6 (`@deepseek-ai/dsh`) | MIT | engine/runtime donor (TASK 19, audit-only): ACP + SDK JSON-RPC machine seams, capability seams, session log, process/sandbox boundary — REFERENCE, nothing copied, no dependency |
| npm `@deepseek-ai/dsh-acp` | — | — | 0.0.1-rc.1 | **BSD-3-Clause** | TASK 20 real-smoke probe: the published ACP plugin (built on `@agentclientprotocol/sdk` 0.25.1). Not a dependency; composition target for the real Windows handshake — still **BLOCKED EXTERNAL at TASK 24** (needs the full Cordis plugin tree + provider config; the deterministic fixture matrix proves the workflow) |

DeepSeek Harness audit + integration contract: KNOWLEDGE/DEEPSEEK_HARNESS.md (TASK 19).
Not vendored (no `donors/` clone); verified from current upstream + npm package.

SAIWORK note: it is itself a fork of CodeNomad 0.18.0 (commit `9f24190` in its
history); its hardening commits are audited as donor lessons.

Donor clones are **not** committed (gitignored `donors/`); baselines above
are the audit record. MIGRATION_SAIWORK.md maps every salvaged subsystem.

## Relevant donor source paths (audit map)

### SAIWORK (`packages/server/src/`)
- Queue: `queue/{manager,validation}.ts` + `queue/manager.test.ts`
- Processes: `background-processes/{manager,output-writer,stream-ticker}.ts`
- Orphans/identity: `workspaces/{orphan-cleanup,process-identity,spawn,runtime,loopback,workspace-identity}.ts`
- OpenCode auth: `workspaces/opencode-auth.ts`
- SSE: `server/routes/events.ts` + tests
- SAIPEN: `saipen/{core,state,board,file-watcher,path-security,utf8,auto-update}.ts` + `no-second-runtime.test.ts`
- Google/Antigravity: `google/{adapter,sanitize,errors,providers,shim,antigravity-session,tool-call-persistence}.ts`
- Freebuff: `freebuff/{engine,gateway,client,install,quota,shell-lifetime,types}.ts`
- Cross-cutting: `atomic-write.ts`, `shutdown.ts`, `log-sanitize.ts`, `events/bus.ts`, `launcher.ts`, `api-types.ts`
- Shells: `packages/electron-app/`, `packages/tauri-app/`, `packages/opencode-plugin/`

### SAIPENVIEW (`saipenview/`)
`watcher.py`, `paths.py`, `protocol_write.py`, `ownership.py`, `guard.py`,
`outbox.py`, `external_changes.py`, `scanner.py`, `service.py`, `runtime.py`,
`sessions.py`, `textio.py`, `api.py`, `app.py`, `saio.py` (canonical writer).

### SAIPEN (`saipen/`)
`SPEC.md` (RFC.md normative), `CONFORMANCE.md`, `MANIFEST.json` (single file
list), `BOOT.md`, `STYLE.md`, `CORE.md`, `MAINTENANCE.md`, `phases/`,
`runtime/`, `tools/validate.py` (canonical validator).

### Freebuff
`cli/src/` (TUI), `sdk/` (client SDK), `common/src/` (actions, mcp,
project-file-tree, api-keys), `packages/agent-runtime/` (run-agent-step,
prompt-agent-stream, tool-stream-parser, compact-history, system-prompt),
`packages/code-map/`, `packages/llm-providers/`, `agents/`, `freebuff/`.

## Reuse classification by component

| Component | Donor source | Type |
| --- | --- | --- |
| Queue semantics (CAS, atomicity, events-after-commit, restore) | SAIWORK queue | REWRITTEN (onto SQLite) |
| Process supervisor (states, staged stop, bounded output, tree kill) | SAIWORK background-processes + orphan registry + Freebuff engine | REWRITTEN |
| Orphan sweep with process identity | SAIWORK orphan-cleanup/process-identity | REWRITTEN (libc APIs, no shell probes) |
| Bounded event bus + forwarder | SAIWORK SSE route lessons | REWRITTEN |
| Event taxonomy | SAIWORK events/bus + Freebuff handleEvent shapes + master spec §12 | CONCEPT |
| Secret redaction | SAIWORK log-sanitize + google/sanitize | REWRITTEN |
| SAIPEN read/watch | SAIWORK saipen + SAIPENVIEW watcher/paths | REWRITTEN (phase 3) |
| Canonical path containment | SAIWORK path-security + SAIPENVIEW paths | REWRITTEN |
| EngineAdapter capability normalization | SAIWORK google adapter + master spec §9 | CONCEPT |
| FakeEngine | — | ORIGINAL |
| Tauri shell / single instance | SAIWORK shells (DROP) + saipenview guard idea | REWRITTEN |
| React UI | SAIWORK ui concepts + Freebuff UX ideas + master spec §24 | CONCEPT/REWRITTEN |

## Dependency ledger

Every dependency must have a justification (spec §40):

| Component | Version | Purpose | Runtime cost | Removable later |
| --- | --- | --- | --- | --- |
| Tauri 2 | ^2 | the one desktop shell (law 9) | binary | no |
| tauri-plugin-dialog | ^2 | folder picker | small | yes |
| tauri-plugin-single-instance | ^2 | single authority (spec §35) | small | no |
| tokio | 1 | async runtime, processes, timers, broadcast | moderate | no |
| rusqlite (bundled) | 0.32 | SQLite without system dep | moderate | no |
| serde / serde_json | 1 | typed events + config | low | no |
| thiserror | 2 | structured errors | none (macro) | yes |
| async-trait | 0.1 | dyn-compatible EngineAdapter | none (macro) | yes |
| tracing + tracing-subscriber | 0.1/0.3 | structured logs | low | yes |
| uuid | 1 | stable ids | low | yes |
| regex | 1 | secret redaction patterns | low | yes |
| notify | 6 | canonical SAIPEN filesystem watcher (TASK 14 §110): maintained Rust stack, solid Windows ReadDirectoryChangesW support; one non-recursive watcher per `.saipen` root, bounded channel, debounce/coalesce — no custom watcher implementation | low (idle watcher ≈ 0 CPU) | yes |
| (external, not a crate) | — | the **canonical SAIPEN tool itself** (`tools/saipen.py`, `tools/validate.py` from STATE `saipen_home`) is invoked by contract (TASK 15 §5–§8): SAIWORK2 adds zero new third-party Rust crates for actions — process execution reuses `saiwork-process`; python is a platform prerequisite resolved once from PATH | n/a (external authority) | n/a |
| reqwest | 0.12 (rustls, no default features) | engine-opencode readiness/probe HTTP client; minimal feature set; TASK 11 SSE reuses the same client strategy | moderate | yes |
| tempfile | 3 | disposable test workspaces (dev-dependency) | dev only | yes |
| rfd | 0.15 | startup-failure native dialog | small | yes |
| react / react-dom | ^18 | UI renderer | bundle | no |
| vite / typescript | ^6/^5 | build + types | dev only | no |
| @tauri-apps/api + plugins | ^2 | JS IPC | bundle | no |

## OpenCode contract evidence (verified 2026-08-16)

- Runtime: **opencode-ai@1.18.18** installed as a global npm package
  (`npm ls -g`), shimmed at `%NODE_HOME%/opencode` (`opencode.cmd` →
  `node_modules/opencode-ai/bin/opencode.exe`, a native binary).
- CLI contract verified by executing `opencode --version`, `opencode
  serve --help`, and a live `serve` run (see ENGINE_CONTRACT.md).
- The npm shim is a wrapper script; the adapter prefers the native
  `opencode.exe` it forwards to, and falls back to the `.cmd` shim via
  `cmd.exe /D /S /C` only when needed (TASK 10 §8).
- Upstream Tauri issue tauri-apps/tauri#14580 (desktop lib test harness on
  Windows, `0xC0000139`) is the one known external toolchain limitation;
  see TESTING.md §2d and ADR-019.

## Freebuff current contract (verified TASK 17, 2026-08-17)

Re-verified from the vendored snapshot `donors/freebuff` (commit
`5661b80732ca6cd36ceb7c83366a6ed45470e6e3`, Apache-2.0) — TASK 01 was
reconnaissance; this is the current integration evidence.

| Dimension | Evidence |
| --- | --- |
| Repository | `CodebuffAI/codebuff` (vendored at `donors/freebuff`) |
| License | Apache-2.0 (repo + `@codebuff/sdk`) |
| Official SDK | `@codebuff/sdk` v0.10.7 — **TypeScript/Node ≥ 22 only**, built with bun; heavy JS tree (`@ai-sdk/*`, `ai` v7, quickjs-wasm, tree-sitter-wasm, zod v4, ws, undici) |
| Auth | mandatory remote **API key** (codebuff.com account) — runs execute in the Codebuff **cloud**, not locally |
| Session model | continuation via `previousRun` RunState JSON — no local session store/files |
| Streaming | `handleEvent` callback (text/tool/error events); no token-level streaming (SDK: “likely add later”) |
| Tools | agent tools execute **remotely** on the service; custom tools are JS functions |
| Cancellation | SDK `AbortSignal` supported |
| Windows | dev setup only (WINDOWS.md); SDK is Node-based |
| CLI | `cli/` is the full application itself (bun), not a headless engine contract |
| Classification | **DEFERRED** (TASK 17 §4/§192): remote-cloud-only execution, Node-only SDK, mandatory cloud credential storage, and integration would duplicate the application itself rather than act as an engine |

### Generic CLI engine (TASK 17)

| Dimension | Value |
| --- | --- |
| Crate | `engine-generic-cli` (new, MIT) |
| Integration type | external-process protocol — trusted local executable, no SDK dependency |
| Runtime dependency | none added (uses `saiwork-process`) |
| Credentials authority | none — no credentials in scope |
| Session authority | SAIWORK2 session metadata only; each run is a fresh process |
| Code copied | NO |
| Env config | `SAIWORK2_CLI_EXECUTABLE` (+ optional `_ARGS`, `_LABEL`, `_MAX_OUTPUT_BYTES`, `_TIMEOUT_MS`) |

## Attribution / notices

- SAIWORK, SAIPEN, SAIPENVIEW are MIT; Freebuff is Apache-2.0. Any future
  verbatim reuse keeps the original license text and attribution.
- SAIWORK2 itself is MIT (LICENSE).
- No copied code exists today; if this changes, this ledger is updated in the
  same commit as the copied code (never after).
