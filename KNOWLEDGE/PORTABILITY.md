# PORTABILITY.md

## Data root resolution (deterministic order)

```text
1. SAIWORK2_DATA_DIR   environment variable
2. portable.flag       beside the executable → ./data
3. OS application-data directory (normal install)
```

Exactly one writable application data root at any time (law 15).

Implementation (TASK 05): the single resolver is
`AppConfig::resolve_from(explicit, exe_dir)` in `saiwork-core::config` —
`resolve()` feeds it `SAIWORK2_DATA_DIR` and the executable's directory.
Portable mode is decided **only** by `portable.flag` beside the executable;
`current_exe()` is never CWD-derived, so launching from cmd, a shortcut,
Explorer, or another process with a different working directory yields the
same root (tested: `portable_resolution_does_not_depend_on_cwd`). An invalid
`SAIWORK2_DATA_DIR` fails loudly at `ensure_layout` — no silent fallback to
`%APPDATA%`.

## Portable tree

```text
SAIWORK2/
├── SAIWORK2.exe
├── portable.flag
├── data/
│   ├── saiwork2.db
│   ├── config/
│   ├── logs/
│   ├── cache/
│   └── runtime/
└── tools/
```

## Portable mode rules

- application-owned data lives in the portable root;
- moving the folder must not break absolute-path assumptions where possible
  (store paths relative to the root when the value is a path inside the root);
- cache is deletable anytime (app survives);
- the database must not be accidentally lost (it lives with the exe, and
  relocation preserves it);
- third-party engine secrets are not copied automatically (law 14);
- the flag file is the single deterministic marker; never infer portability
  from "the exe sits in Program Files" or similar heuristics.

## TASK 23 — external Harness-session portability limitation

Queue/Harness correlation lives in the portable SQLite (`session_id` + `run_id`);
Harness sessions and credentials live in the **Harness-owned external store** (Harness
home / its own session authority), not in the portable root. Copying the SAIWORK2
portable folder alone does not carry Harness session authority: a queue item referencing
a Harness session may become unreconcilable on another machine without that Harness
store. This is an explicit, documented limitation (TASK 23 §159–§160) — SAIWORK2 never
claims fully self-contained portable execution state for Harness-targeted work.

## Relocation

On startup, validate the resolved root; if the DB is missing but `data/`
exists with a DB elsewhere (stale `SAIWORK2_DATA_DIR`), report via
diagnostics — never silently fork a second authority into two roots (law 15).
