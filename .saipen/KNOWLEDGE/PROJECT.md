# PROJECT.md — SAIWORK2 (this project)

Cold-start orientation for any agent landing in this `.saipen/`. Full
specification map: `KNOWLEDGE/INDEX.md` at the repo root
(`V:\___VAC\__K\__CODE\_AI_STUFF_AGENTIC\_SAIWORK2\KNOWLEDGE\`). Read that
INDEX before touching any governed area.

## What this is

SAIWORK2 = desktop application for queue-based agentic work: the user queues
prompts, a durable SQLite-backed queue dispatches them one workspace at a
time to an AI engine (OpenCode primary), results stream back with typed
events. Rust workspace core + Tauri 2 desktop shell + TypeScript UI.

## Layout (crates/ + apps/)

- `crates/saiwork-process` — process lifecycle authority: spawn/supervise
  engine binaries, kill, restart, crash detection.
- `crates/saiwork-events` — typed event bus (message deltas, tool calls,
  permission requests, terminal states).
- `crates/saiwork-core` — domain core: sessions, queue port, engine trait.
- `crates/saiwork-queue` — durable queue manager (SQLite), retries, failpoints.
- `crates/saiwork-storage` — SQLite storage authority.
- `crates/saiwork-saipen` — SAIPEN protocol integration (reader/watcher/actions).
- `crates/engine-opencode` — **primary engine adapter**: spawns
  `opencode serve` on a free port (--pure + OPENCODE_SERVER_PASSWORD),
  probes readiness, streams SSE events, resolves models, sessions, cancel.
  Fixture-based protocol tests (no real binary needed). `tests/real.rs` =
  real-binary verification, environment-gated.
- `crates/engine-deepseek-harness`, `crates/engine-generic-cli` — secondary
  engine candidates (experimental / generic CLI).
- `apps/desktop` — Tauri shell (crate `saiwork2`), MSVC toolchain required.

## Authority rules (non-negotiable, ARCHITECTURE.md 25 laws)

- Exactly one authority per capability: processes (saiwork-process), durable
  queue (SQLite), events (saiwork-events), SAIPEN (the protocol), desktop
  runtime (Tauri only — never Electron).
- UI never owns child processes, never writes the DB, never writes `.saipen`.
- No unbounded anything (queues, logs, buffers, retries, listener sets).
- No polling where a watcher/event exists.
- No engine-specific `if (engine === "opencode")` in generic UI.
- One agent run per workspace (ADR-038) — never bypass to "parallelize".
- Release builds never register FakeEngine, never enable queue failpoints.
- No dead code; comments explain WHY/INVARIANT, not "increment counter".

## Commands

```bash
cargo test --workspace --exclude saiwork2   # core (GNU toolchain on Windows)
cargo test -p engine-opencode               # engine adapter suites (fixture)
npm run typecheck                           # TS strict
npm run tauri dev                           # desktop app (MSVC toolchain)
```

## Current state (2026-08-18)

- **auth.json provider merge: DONE.** `engine-opencode` reads
  `~/.local/share/opencode/auth.json` (or `OpenCodeConfig.auth_json_path`),
  appends custom providers' models to the server catalog (catalog stays
  authoritative, credential-only entries dropped, malformed file = no-op,
  secrets never deserialized). Proven by unit tests + fixture integration
  test `auth_json_provider_is_merged_into_models`.
- Test state: `cargo test -p engine-opencode` all green (lib 45, hostile 26,
  protocol 51 — protocol suite ran 3x stable after the env-leak flake fix).
  Workspace gate green with `--skip real_`.
- **BLOCKER (environment):** `tests/real.rs` (real opencode binary, 1.18.18)
  fails with 401 "server rejected the runtime credential" while the user's
  own live opencode sessions share storage (`opencode.db` 3.4 GB, auth.json).
  Root cause not conclusively established; do NOT kill/spawn opencode
  servers while user sessions run (the user works THROUGH this engine).
  Manual repro: with OPENCODE_SERVER_PASSWORD set, server rejects even the
  correct basic auth — env semantics of opencode 1.18.18 suspect.
- Windows toolchains: core = GNU + MinGW; Tauri = MSVC + VS Build Tools 2022.

## Known debt (do not touch unless asked)

- Pre-existing clippy warnings in saiwork-process/saiwork-core/queue;
  fmt debt across many files (real.rs:311/343, apps/desktop/commands.rs, …).
- `tools/validate.py` git-freshness check BLOCKED: repo has no commits yet.