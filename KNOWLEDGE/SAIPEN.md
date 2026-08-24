# SAIPEN.md

SAIPEN is a separate canonical protocol (the `vacterro/saipen` repository —
a Markdown protocol, not a service: BOOT.md, CORE.md, STYLE.md, MANIFEST.json,
`phases/`, `runtime/`). SAIWORK2 does **not** implement an alternative SAIPEN
and does **not** ship a vendored copy (donor lesson: SAIWORK points at the
live install and would silently drift otherwise).

## Architecture

```text
SAIWORK2
↓
SaipenClient
↓
canonical SAIPEN operations
↓
.saipen
```

## Read path (implemented — TASK 14, `saiwork-saipen`)

- detect a SAIPEN project (`.saipen/STATE.md` present) with typed results
  (`NotPresent | Present | Invalid | Unsupported | PermissionDenied`);
- read canonical state (STATE frontmatter scalars + BOARD section tickets);
- display status (SAIPENBAR);
- watch changes (watcher-driven, debounced/coalesced, generation-tagged);
- surface errors (typed `SaipenError`; UNKNOWN stays UNKNOWN);
- **read-only**: zero canonical writes, no second validator, no SQLite
  mirror, no polling (TASK 14 §1–§7, §228–§230; TASK 15 §2);
- canonical actions (TASK 15) invoke the **canonical tool only** through the
  managed process boundary — SAIWORK2 itself never edits canonical files.

## Actions (implemented — TASK 15, verified 2026-08-16)

Verified contract (donors/saipen **v7.224.3**): the canonical `saipen.py` CLI
surface is `status|next|recover|claim|transition|checkpoint|ticket|improve|
ship|push|scope|first-publish-confirm|userperson|sub|context` with
`--json`/`--dry-run`. **There is no `continue`, `board`, `knowledge`,
`validate`, or `stop` command in that CLI** (TASK 15 §3). The SAIPENBAR
labels are therefore mapped honestly, never invented:

| Bar label | Mapping | Kind |
|---|---|---|
| Status | canonical `saipen.py status --json` | READ_ONLY process |
| Validate | canonical `tools/validate.py --project-root <root>` | READ_ONLY process |
| Board | local view on the reader board snapshot | View (no process) |
| Knowledge | local view on canonical KNOWLEDGE path | View (no process) |
| Continue | **Unsupported** — no canonical CLI exists; the canonical
  "continue" is the agent's protocol instruction in STATE `next_action` | Unsupported |

**TASK 23 — SAIPEN → Queue handoff DEFERRED.** Because `Continue` is not a canonical
command and no stable execution/transition identity exists for SAIPEN-produced work,
automatic SAIPEN → QueueManager handoff cannot be proven exactly-once and is explicitly
deferred (TASK 23 §73–§84, ADR-043, QUEUE.md). A future handoff must flow
`canonical SAIPEN action → durable QueueManager enqueue` with a stable source id as the
idempotency key — never a direct `SaipenClient → engine.send` bypass, never
`saipen.changed → enqueue current task` (that would be a duplication machine, §82–§83).
| Stop | cancels SAIWORK2-owned action processes only — no canonical
  `saipen stop` exists | Control (no process) |

Canonical validator exit semantics (verified against the real tool):
**0 = conformant**, **1 = domain-invalid** (a *result*, not an action
failure — shown as `VALIDATION: INVALID`, never "validator crashed"),
**2 = usage/infrastructure error** (shown as action `Failed`). Real-tool
regression tests run the vendored validator end-to-end against synthetic
fixtures (exit 0/1 both proven; §217, §240).

Action lifecycle (§17): `Pending → Running → Succeeded | Failed |
Cancelling → Cancelled`; one terminal outcome per workspace. The backend
enforces one active action per workspace (mutation exclusivity §14) — a
double click returns typed `Busy` even if the frontend sends twice (§34,
§77, §119). Action processes run through the one `ProcessSupervisor` with
no shell, explicit cwd = validated **workspace** root (canonical tools
resolve `.saipen/` from the project root — `SaipenRoot.dir` is the `.saipen`
dir itself and is never used as cwd/argument), bounded output, bounded
per-kind timeout (§25), graceful→force stop on cancel/timeout (§26–§28).
After every terminal action the reader performs one authoritative refresh
(§19, §125) — the filesystem stays the truth; no optimistic projection.

**Validation staleness** (§87–§88): a Validate result is bound to the
snapshot generation it ran against; if the canonical files move afterward
the bar renders `VALIDATION: VALID · STALE`, never falsely current. No
polling, no auto-revalidate (§75, §77): Validate runs on explicit user
action only.

**SAIPEN → Queue**: intentionally **not wired** in TASK 15. No canonical
`continue` exists to produce work, and no stable correlation id exists for
SAIPEN-produced work (TASK 15 §30–§33, §69–§73); a manual canonical action
stays manual until a safe handoff contract exists. Do not invent
`Continue = enqueue prompt`.

## Read integration facts (TASK 14, verified 2026-08-16)

- Baseline: `donors/saipen` checkout **v7.224.3** (VERSION), STATE
  `schema_version: 3`, `saipen_version: 7`. STATE.md is YAML frontmatter
  (`--- … ---`, single-line scalars, optional `requires:` list, CRLF or LF,
  optional UTF-8 BOM). Canonical scalar keys: `phase`, `task`, `next_action`,
  `blocker`, `transition_from`, `saipen_version`, `schema_version`,
  `last_event`, `style_contract`, `saipen_home`, `agent`, `requires`,
  `mode`, `execution_intent`, `goal_waves`, `goal_tickets`, `updated`.
- BOARD.md sections `## DOING / ## TODO / ## DONE / ## BLOCKED`; ticket
  status derives from the section, never the checkbox. Ticket ids `T-###`.
- Supported schema versions: **3**. A newer version is surfaced as
  `Unsupported`, never parsed as current (TASK 14 §15, §177).
- Duplicate frontmatter keys are a parse error — never last-write-wins
  (donor lesson). Unknown optional keys are preserved (reader never writes
  back, §173).
- Consistency: read STATE + BOARD, then re-check (size, mtime) markers;
  bounded retry on movement — the writer's atomic-replace behavior is
  coalesced, never shown as permanent breakage (§25–§28, §131).

## Write path (forbidden from UI)

Arbitrary writes from the React UI are prohibited. In particular:

```text
frontend → fs.writeFile(".saipen/STATE.md")
```

is never a thing. All mutations go through a canonical writer/command layer
(phase 3+), and even then only after the write path is confirmed canonical.
`saipenview`'s `protocol_write.py` is the reference for canonical writer
semantics (ownership, outbox, guard), not for copying.

## Watcher (implemented — TASK 14)

```
notify (one watcher per .saipen root, non-recursive)
↓
bounded channel (cap 64; overflow → full reread required)
↓
debounce/coalesce (300 ms quiet window; N events → 1 reread)
↓
full authoritative reread (STATE + BOARD, consistency-marked)
↓
semantic comparison (unchanged content → NO event)
↓
saipen.detected (NotPresent→Present) / saipen.changed (meaningful change)
```

Facts (from saipenview T-124 rewrite + SAIWORK file-watcher, implemented):

- one owner: `SaipenService` (one watcher per attached workspace root,
  lifetime bound to the workspace, law 19); watchers are spawned by the
  backend service, never React;
- root replacement: rename/create/remove of the `.saipen` dir triggers a
  bounded rebind (watch handle may be attached to a deleted inode);
- coalescing: dirty-flag + 300 ms quiet window — a storm yields ~1 reread
  (tested: 10-event save burst → ≤3 refreshes, idle → 0);
- overflow: the bounded channel drops events and sets an overflow flag; the
  next refresh is a full authoritative reread (§36);
- watch failure: bounded restart (max 2, backoff, shutdown-aware); final
  failure surfaces `watch_status: failed` — the projection degrades to
  read-only, never silently freezes (§37, §61);
- generation-tagged: late events from a closed/replaced watch session are
  discarded (workspace close/reopen, §65–§66);
- one fs burst → one structured `saipen.changed`; never raw bytes through
  the bus.

## Path handling (Windows verbatim, regression 2026-08-22)

- Tauri on Windows may return verbatim paths `\\?\V:\path` (§76). Every
  workspace path stored in SQLite is normalized on write (`open_workspace`
  strips the `\\?\` drive prefix) and on read (`path_of`,
  `row_to_workspace` strip the same prefix). `get_saipen`’s fallback
  additionally checks the raw filesystem for `.saipen` when the snapshot is
  `NotPresent` — if the directory exists the UI shows a stale/error
  snapshot ("present but invalid") instead of the false-negative
  "no .saipen/ state" that the Files panel contradicts. This prevents the
  screenshot-2 bug where the bar claimed the folder was missing while the
  Files tab clearly listed it.

## Canonical protocol facts (from audit of `vacterro/saipen`)

- LOG event skeleton (verified against the real validator): lines of the
  form `- <dd.mm.yy hh:mm> [E-<n>] [parent: E-<m>] [T-###] [agent: <a>]
  [op: <op>] <TAXONOMY>: <text>`; `# Log` header; plain UTF-8 **without
  BOM** (utf-8-sig is rejected). `STATE last_event` must equal the LOG tail.
- DOING claim (verified): `- [/] T-### … | owner: <agent> | claim_time:
  <utc ISO>` under `## DOING`; `[x]` belongs only under `## DONE`; an open
  `[ ]` under `## DOING` is a FAIL. Ticket status still derives from the
  section for the reader projection.
- STATE answers "what do I do now"; BOARD "what task am I picking up"; LOG
  "why are we here"; KNOWLEDGE "durable truth"; `next_action` is the heart.
- The on-disk contract MUST remain stable; implementations MAY vary. SAIWORK2
  is an implementation client, never a spec author.
- `MANIFEST.json` is the single source of protocol files; `tools/validate.py`
  is the canonical validator (STATE.md against `state.schema.json`, E-###
  uniqueness/monotonicity, parent resolution). SAIWORK2 must consume these,
  not re-derive them.
- STATE.md is **YAML frontmatter** (`--- … ---`), single-line scalars; a
  duplicated key is an issue to surface, never silently resolved (donor:
  SAIWORK `saipen/state.ts`). BOARD ticket status comes from the section
  (`## DOING/TODO/DONE/BLOCKED`), never the checkbox alone.
- Conformance: board `needs:` graph acyclic and every reference resolves
  (a dangling reference is worse than a cycle); 16-phase enum with a legal
  transition table.

## Canonical writer pipeline (the reference SAIWORK2 must call, not copy)

SAIPENVIEW's canonical pipeline (saipenview `protocol_write.py` + `saio.py`)
is: OS writer lock → recovery preflight → immutable PREPARED journal →
ordered targets → byte + semantic verification → COMMITTED, with structured
results `WRITER_BUSY / STALE_STATE / RECOVERY_REQUIRED / CONFLICT`. SAIWORK2
is a **client** of this authority: it never builds a second transaction
engine (ADR-007, no-second-runtime law). A decision is bound to the snapshot
hashes it was made from; on STALE_STATE the decision re-runs ONCE on a fresh
snapshot — never a blind retry of stale bytes.

## SAIWORK2 MAY / MUST / MUST NOT

### MAY
- detect a SAIPEN project (`.saipen/STATE.md` present) and read canonical
  state (STATE/BOARD/LOG/KNOWLEDGE) with bounded reads;
- watch `.saipen/` changes (watcher-driven, debounced, workspace-bound);
- run/display canonical validation (SAIPEN `tools/validate.py` or equivalent)
  and surface errors;
- request canonical mutations **through the canonical writer/CLI path** in a
  later phase, and only after that path is confirmed canonical;
- consume `MANIFEST.json` and protocol files from a live `saipen/` install
  (never a vendored copy).

### MUST
- treat SAIPEN as the single authority for SAIPEN state (law 2);
- show UNKNOWN when state is unknown and "SAIPEN not initialized" when absent
  (law 59 — never fabricate);
- bind every watcher/subscription to the workspace lifecycle (law 19);
- route mutations through canonical writers with structured results
  (`WRITER_BUSY / STALE_STATE / RECOVERY_REQUIRED / CONFLICT`);
- re-run a decision on a fresh snapshot once on STALE_STATE — never blind-
  retry stale bytes;
- keep a structural no-second-writer/no-second-runtime guard (donor lesson:
  SAIWORK `no-second-runtime.test.ts`).

### MUST NOT
- invent a second SAIPEN state store or state machine;
- silently rewrite `.saipen` files (any write path requires canonical
  semantics and user-visible action);
- duplicate canonical validation rules (consume the canonical validator);
- treat UI/cache state as authoritative SAIPEN state;
- spawn a second SAIPEN runtime (one SaipenClient, one managed runtime).

## Display rules (SAIPENBAR)

- never fabricate state: unknown → `UNKNOWN`;
- no SAIPEN → `SAIPEN not initialized` + an explicit action;
- SAIWORK2 mirrors, never mutates, and never becomes a second SAIPEN authority
  (law 2).
