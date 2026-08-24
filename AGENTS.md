# AGENTS.md

Working rules for any agent (or human) editing this repository. The full
specification lives in `KNOWLEDGE/` — **read the relevant document before
touching the area it governs** (`KNOWLEDGE/INDEX.md` is the map).

## Development loop (every feature)

1. Read the relevant KNOWLEDGE document(s).
2. Identify the authority/boundary (ARCHITECTURE.md laws).
3. Inspect existing implementation.
4. Inspect relevant donor knowledge (MIGRATION_SAIWORK.md).
5. Define behavior AND failure paths (TESTING.md failure-first review).
6. Add/update tests.
7. Implement the smallest coherent change.
8. Run focused tests, then the relevant suite.
9. Inspect resource lifecycle (no orphans, no stale watchers, bounded buffers).
10. Update KNOWLEDGE if a contract changed; add an ADR if architecture changed.

## Non-negotiables

- The 25 laws in `KNOWLEDGE/ARCHITECTURE.md`. Violations require an ADR first.
- Exactly one authority per capability: processes (`saiwork-process`),
  durable queue (SQLite), events (`saiwork-events`), SAIPEN (the protocol),
  desktop runtime (Tauri only — never add Electron).
- UI never owns child processes, never writes the DB, never writes `.saipen`.
- No unbounded anything: queues, logs, buffers, retries, listener sets.
- No polling where a watcher/event exists.
- No engine-specific `if (engine === "opencode")` in generic UI.
- **One agent run per workspace** (ADR-038): same-workspace parallel runs
  are rejected by `SessionManager` (`WorkspaceBusy`) and waited on by the
  queue. Never bypass the gate to "parallelize" a workspace.
- Release builds never register FakeEngine and never enable queue failpoints
  (TASK 18 §64/§66).
- No dead code: every path has a caller and a product reason. Future ideas go
  to `KNOWLEDGE/ROADMAP.md`, not unreachable code.
- Comments explain WHY / INVARIANT / NON-OBVIOUS FAILURE MODE, not "increment
  counter".

## Commands

```bash
cargo test --workspace          # Rust unit + integration tests
cargo build --workspace         # compile everything (incl. the Tauri shell)
npm run typecheck               # TS strict across workspaces
npm run tauri dev               # launch the desktop app (from repo root)
```

Verification rule: non-trivial Rust changes require `cargo test --workspace`
(or at least `cargo check --workspace`); TS changes require `npm run
typecheck`. Packaged smoke tests on Windows are separate (TESTING.md).

## Windows toolchain note (IMPORTANT)

The **core workspace crates** (`crates/saiwork-events`, `saiwork-core`, …)
build and test on the GNU toolchain (`stable-x86_64-pc-windows-gnu` +
MinGW-w64): run `cargo test --workspace --exclude saiwork2`.

The **Tauri desktop shell** (`apps/desktop/src-tauri`, crate `saiwork2`)
requires the **MSVC toolchain** on Windows — Tauri 2 does not support GNU
on Windows, and the PE export table limit (65535) is a hard wall regardless
of linker (GNU ld and lld both fail with `too many exported symbols`).
Install: `rustup toolchain install stable-x86_64-pc-windows-msvc` + VS
Build Tools 2022 C++ workload (`Microsoft.VisualStudio.Workload.VCTools`).
`cargo check -p saiwork2` and `cargo test -p saiwork2` (4/4) were verified
WORKING on the GNU toolchain (22.08.26): unit tests link the lib target only,
which stays under the GNU export-table wall. `npm run tauri dev` / release
bundling may still require MSVC — treat those as unproven on GNU until run.
The VS Build Tools installer itself was observed failing with
exit 87 on this machine (bootstrapper→setup.exe handoff); if that recurs,
install via the GUI or winget instead of the CLI.

## Reporting

After meaningful work, report in the format defined by the master spec §61:
BASELINE / DONE / FIXED / FOUND / TESTS / ARCHITECTURE / PERFORMANCE /
REMAINING / NEXT. Evidence over narrative.
