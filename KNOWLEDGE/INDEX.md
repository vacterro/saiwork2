# KNOWLEDGE — Index

Engineering memory of SAIWORK2. This file is the entry point: read it, open
the document that owns the area you touch, then the implementation. If a
document stops being true, fix the document and the code in the same change.

| Document | Purpose | Authority | When to update | Implementation |
| --- | --- | --- | --- | --- |
| [PRODUCT.md](PRODUCT.md) | what SAIWORK2 is / is not, workflow, priorities | product owner | scope change, new feature class | README, ROADMAP |
| [ARCHITECTURE.md](ARCHITECTURE.md) | boundaries, ownership, the 25 laws, landmines | architecture contract | module ownership or dependency direction changes (also ADR) | `crates/*`, `src-tauri` |
| [ENGINE_CONTRACT.md](ENGINE_CONTRACT.md) | one logical engine contract, capabilities, state model | engine boundary | adapter contract change | `saiwork-core::engine`, engines |
| [EVENTS.md](EVENTS.md) | canonical taxonomy, envelope, per-family contracts, event rules | event model | new event type or payload change | `saiwork-events` |
| [SAIPEN.md](SAIPEN.md) | SAIPEN authority, read/watch/validate/mutate surface, MAY/MUST/MUST NOT | SAIPEN protocol | protocol discovery or watcher change | `saiwork-saipen` (phase 3) |
| [QUEUE.md](QUEUE.md) | durable queue state machine, invariants, recovery, OUTCOME_UNKNOWN + Harness target | queue subsystem | state machine or durability change | `saiwork-queue` (TASK 13/23) |
| [STORAGE.md](STORAGE.md) | what SQLite owns, what it never mirrors, failure contract | storage | schema or failure-contract change | `saiwork-storage` |
| [PROCESS_LIFECYCLE.md](PROCESS_LIFECYCLE.md) | child process ownership, lifecycle, guarantees | ProcessSupervisor | lifecycle or shutdown policy change | `saiwork-process` |
| [DEEPSEEK_HARNESS.md](DEEPSEEK_HARNESS.md) | DeepSeek Harness audit + integration contract + adapter + vertical-slice + TASK 22 candidates + TASK 23 queue truth + TASK 24 final status | planning (TASK 19–24) | seam/protocol change | `engine-deepseek-harness`, `saiwork-process`, `saiwork-queue` |
| [PORTABILITY.md](PORTABILITY.md) | data roots, portable mode, what survives | desktop | data root or relocation change | `saiwork-core::config`, `src-tauri` |
| [UI_UX.md](UI_UX.md) | stable UX principles, status truth, v1 scope | UX | UX contract change | `apps/desktop/src` |
| [PERFORMANCE.md](PERFORMANCE.md) | performance invariants, baselines, gates | performance | measured regression, new hot path | whole app |
| [SECURITY.md](SECURITY.md) | trust boundaries, path rules, credentials | security | new security-sensitive capability | `saiwork-core`, `saiwork-process` |
| [TESTING.md](TESTING.md) | test strategy, hostile matrix, gates | QA | new failure class | `tests/*`, crate tests |
| [REGRESSION_BACKLOG.md](REGRESSION_BACKLOG.md) | donor failures → future fixtures (law 24) | QA | new donor finding, fixture creation | `tests/*` |
| [THIRD_PARTY.md](THIRD_PARTY.md) | donor baselines, dependency ledger, licenses | legal/provenance | new dependency or donor insight | `Cargo.toml`, `package.json` |
| [MIGRATION_SAIWORK.md](MIGRATION_SAIWORK.md) | operational salvage ledger per subsystem | migration | new salvage finding | donor clones (gitignored `donors/`) |
| [DECISIONS.md](DECISIONS.md) | ADRs — decisions with reasons and consequences | architecture | any architecture decision | whole repo |
| [ROADMAP.md](ROADMAP.md) | 18 sequential tasks with gates | planning | phase completion | git history |

## Reading rules

1. Read INDEX → the document that owns the area → related implementation.
2. A change that violates an ARCHITECTURE law requires an ADR in DECISIONS.md **before** code.
3. A change that alters a contract updates the contract document in the same commit.
4. Donor bugs become regression fixtures (REGRESSION_BACKLOG.md → TESTING.md), never silent patches.
