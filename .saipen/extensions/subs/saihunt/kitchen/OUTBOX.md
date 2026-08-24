# OUTBOX

## HUNT-001: Six-signal clean sweep over first-party source (crew SC-2)
- **status:** ready
- **summary:** FORCE-FRESH sensor pass on 2026-08-23T12:36Z (re-sweep after T-076+T-077 source mutation; prior c12aa6e5 evidence stale by definition) over first-party surfaces (`apps/desktop/src/**`, `apps/desktop/src-tauri/src/**`, `crates/*/src/**` + tests, `packages/contracts/src/*`; donors/node_modules/target/.saipen excluded). Signals 1вЂ“6 executed for real, no mtime shortcuts. Three actionable findings survived dedup against Core's canonical ticket board (including BLOCKED); everything else came back clean or already-tracked.
- **critical:** false
- **producer:** saihunt
- **source_head:** no-git (no `.git` in tree or parents)
- **source_tree_fingerprint:** nogit-v1:084456dd3d67cc2dd615fd0d0c1e01ff3207cc2eaf47ee17f33e4b99683fd285
- **role_revision:** sha256:4edb04181cb07e0946afd06fbe711166fa9dcc403e56b52e9be3844f0a71b0a5
- **coverage:**
  - S1 failing tests: full workspace suite green Г—2 today (post-fix final sweep + post-T-035 sweep); vitest 97/97; saiwork2 8+4 green. One known load-dependent flake (see F-2) вЂ” not new signal beyond its ticket.
  - S2 commits unverified in LOG: N/A вЂ” no git repository exists; nothing commit-shaped to verify.
  - S3 stale TODO/FIXME/HACK: `rg "TODO|FIXME|HACK|XXX"` across first-party TS/RS в†’ exactly 1 hit and it is a doc comment describing queue states (`packages/contracts/src/index.ts:82`), not a deferred-work marker. CLEAN.
  - S4 silent failures: no empty catch blocks in first-party TS. All `.catch(() => undefined)` sites are documented degrade paths (frontendSync cold bootstrap W2-005 generation guard at `frontendSync.ts:205-215`; persistence fire-and-forget layout write `persistence.ts:61-63`). Rust `let _ =` sites are deliberate best-effort (UI dialogs, cancel_tx to possibly-dropped receivers, window focus) or test cleanup. No `#[allow(dead_code)]` in first-party crates. CLEAN apart from F-3's asymmetry observation.
  - S5 symmetry gaps: one real asymmetry (F-3): `get_setting` reads ANY key while `set_setting` is whitelist-gated (`WRITABLE_SETTING_KEYS`, commands.rs:175-193). Harmless today (all stored keys are non-security UI prefs) but the read path has no gate, so a future security-relevant key becomes readable through the generic IPC surface by default.
  - S6 dead code / orphan files: three zero-reference exports (F-1); no orphan files found (every source file is imported or is an entry/config).
- **payload:**
  - F-1 (P3, dead code, S6): dead exports with zero references outside their defining file вЂ” `filesSliceOf` (apps/desktop/src/components/FilesPanel.tsx:31, introduced with T-035), `activitySliceOf` (apps/desktop/src/components/ActivityPanel.tsx:109-111), `ALL_DOCK_TABS` (apps/desktop/src/components/dock/types.ts:24). Evidence: `rg -l` per symbol returns only the defining file. Fix: delete all three (no callers to migrate). REPRODUCED (static proof).
  - F-2 (P2, test robustness, S1): `engine-deepseek-harness` `vertical.rs::large_stream_is_bounded_and_completes` flaked exactly once under full-workspace parallel build load (main LOG E-156), then passed 3/3 isolated and 2/2 full sweeps. Failure mode: timing-sensitive bounded-stream assertion under CPU contention. Fix direction: raise its internal deadline proportionally to observed load or tag it to run without sibling parallelism. REPRODUCED (one observed failure with captured output).
  - F-3 (P3, hardening, S5): `get_setting` (apps/desktop/src-tauri/src/commands.rs:177-180) lacks the key allowlist its write twin has; symmetric contract should be read-gated the same way (or carry an explicit comment justifying open reads). REPRODUCED (static code comparison of the two commands).
- **verified:**
  - Re-sweep 2026-08-23T12:36Z confirmed all three findings against the moved tree (223 files): F-1 symbols still defining-file-only, F-3 asymmetry lines unchanged (commands.rs:175-193), S3 TODO scan clean.
  - Greps executed live this session: TODO/FIXME/HACK scan (1 benign hit), empty-catch scan (0), `.catch(() => undefined)` inventory (14 sites, each context-read and documented), Rust ignored-result scan (~20 sites reviewed), per-symbol reference checks for F-1 (defining-file-only).
  - Suite evidence inherited from today's runs recorded in main LOG E-149/E-155/E-156 (green sweeps, flake capture).
  - Dedup check against Core's canonical ticket board: none of F-1..F-3 already tracked (T-071 unrelated; T-005/T-007 user smokes unrelated).
- **instructions:**
  1. Core reviews these three findings as ordinary review hypotheses (SC-6 intake). Acceptance is Core's judgment; this package asserts evidence, not disposition.
  2. On acceptance, ticket shape: one ticket per finding, `verify:` lines вЂ” F-1: `npm run typecheck && npm test` after deletion; F-2: targeted harness vertical run Г—3 plus one full parallel sweep green; F-3: typecheck + cargo check -p saiwork2 (behavior unchanged for existing UI keys).
  3. Freshness gate for consumption: fingerprint must equal `nogit-v1:084456dd` above and role_revision `sha256:4edb0418…`; drift ⇒ re-run the sensor pass instead of consuming.
