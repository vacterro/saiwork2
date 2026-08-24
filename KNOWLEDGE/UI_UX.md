# UI_UX.md

## Layout (TASK 16 — implemented cockpit)

```text
┌──────────────────────────────────────────────────────────────┐
│ PROJECT        ENGINE      MODEL                    ● health │
│ (TitleBar: project + engine/model selectors + start/stop)    │
├───────────────┬───────────────────────────┬──────────────────┤
│ PROJECTS      │ Conversation              │ ACTIVITY QUEUE   │
│   (SAIPEN S)  │  streaming → plain text   │ DIAG tabs        │
│ THREADS       │  terminal → Markdown      │  tools/permissions│
│               │  code blocks w/ copy      │  durable queue    │
│               │  stick-to-bottom scroll   │  redacted diag    │
├───────────────┴───────────────────────────┴──────────────────┤
│ Composer: [textarea]              [Queue] [Send ↵]           │
├──────────────────────────────────────────────────────────────┤
│ SAIPENBAR strip: PROJECT STATE TASK NEXT BLOCKER WATCH       │
│   VALIDATION [Continue Status Board Knowledge Validate Stop] │
├──────────────────────────────────────────────────────────────┤
│ statusline: backend · lifecycle · runs · last meaningful event│
└──────────────────────────────────────────────────────────────┘
```

- Primary conversation dominant; queue/tools/context accessible in the right
  activity panel (tabs); SAIPENBAR is an operational strip, never half the
  window (TASK 15 §55).
- Responsive (TASK 16 §9): <1100 px → activity panel collapses; <760 px →
  left nav collapses. Desktop-first; no phone UI.
- Golden Vintage: dark neutral vintage palette, sharp/near-square geometry
  (radius 3 px), compact dense controls, one token system in `global.css`
  (`--bg/--surface/--line/--ink/--accent/--ok/--bad/--warn/--focus`, spacing
  scale, mono/serif stacks). No component-local themes, no theme engine.

## Shell grid contract (regression guard for screenshots 1 & 3, 2026-08-22)

The desktop shell is a single CSS grid that must never be broken by empty
states or narrow viewports. This contract is the source of truth for
`apps/desktop/src/styles/global.css` and `apps/desktop/src/app/App.tsx`
child order; every change to either file must preserve it.

```
.app {
  grid-template-rows: auto auto 1fr auto auto auto; /* TitleBar | ThreadTabs | main(1fr) | Composer | SaipenBar | StatusLine */
  /* The 1fr row MUST be app__main — ThreadTabs is often empty (no sessions)
     and must never steal the flexible row. Extra banners/toasts are implicit
     auto rows at the end. */
}
.app__main {
  grid-template-columns: minmax(170px,230px) minmax(0,1fr) var(--dock-width,330px); /* nav | conversation | dock */
  /* At 1100px: nav 160-200 | conversation 1fr | rail 46px (dock hidden). */
  /* At 760px:  conversation 1fr | rail 46px (nav hidden).               */
}
```

Invariants that a regression test must enforce (see
`apps/desktop/src/app/shellLayout.test.ts`):
- `.app` has 6 explicit rows with the 1fr on the 3rd row (app__main).
- ThreadTabs is `auto`, never `1fr`, so an empty thread bar does not create a
  giant blank band and squash nav/conversation/dock to the bottom.
- `.app__main` is `display:grid` with 3 columns on desktop and keeps the 46px
  rail column at both breakpoints; `display:none` is applied only to `.dock`
  (full panel), never to `.dock-rail`, and never collapses the rail's column
  into a full-width row at the bottom.
- Every new top-level child added to `.app` must extend `grid-template-rows`
  explicitly; implicit auto rows are only for transient banners.

If any invariant is violated the shell looks exactly like screenshot 1 (huge
blank ThreadTabs band) or screenshot 3 (rail as a full-width black bar at the
bottom covering the composer). The fix is always in `global.css` grid
definitions, never in component JS.

## First-class UX

- fast project switching (SAIPEN presence badge `S` per project row);
- obvious current engine/model; visible running state;
- **Send vs Queue are distinct buttons** — no ambiguous dropdown (§42–§43);
- **Cancel run vs Stop engine are distinct controls** — Composer "Cancel run"
  (engine cancel semantics), TitleBar "Stop engine" (runtime stop) (§62);
- visible queued prompts; obvious stop/cancel; no hidden agent execution;
- no ambiguous click state; keyboard-friendly; status truth everywhere.

## Status truth (law 59)

Never show `Running / Connected / Ready / Saved / Completed` based on intent —
only on confirmed authoritative state:

```text
process spawned ≠ engine ready
message queued ≠ message sent
send accepted ≠ run completed
write requested ≠ persisted
SAIPEN command invoked ≠ SAIPEN mutation validated
```

Unknown → `UNKNOWN`, not an optimistic fake. No SAIPEN → explicit message +
actions disabled with the exact reason in the tooltip.

## Hotkeys (documented, minimal — §40, §132)

```text
Enter            → Send
Shift+Enter      → newline
Ctrl+Enter       → Queue (durable enqueue)
Tab / Enter      → standard button navigation
Esc              → dismiss toasts/menus
```

The durable `Enter queues` preference swaps Enter and Ctrl+Enter: plain Enter
queues, Ctrl+Enter sends, Shift+Enter always remains newline. The Composer is
the only prompt editor; QueuePanel edits/reorders existing durable items but
never owns a second enqueue textbox.

## Conversation (TASK 16 §20–§31)

- Streaming renders **plain text** (cheap per batched frame); Markdown is
  finalized at terminal (§28). react-markdown + remark-gfm, safe defaults
  (no raw HTML), links open externally.
- Fenced code blocks: language label + copy button; inline code plain.
- Scroll: auto-follow only while the user is near the bottom (90 px
  threshold); scrolling up shows `Jump to latest ↓` (§24).
- Tools: lifecycle row (running/completed/failed), bounded output preview,
  expandable detail (§32–§35). Permissions: Allow/Deny buttons resolve
  through the typed backend `resolve_permission` command (§36–§38, §166);
  a dead run's engine releases pending permissions — the buttons disappear.
- Every message and tool row shows absolute local time plus relative age in
  parentheses; one transcript-level minute clock keeps every visible relative
  age current. Authoritative history timestamps are preserved by adapters;
  receipt time is used only for old/unsupported history shapes.
- Structured agent questions render every bounded prompt, option description,
  multi-select/custom answer, and submit all answers atomically through
  `resolve_question`; malformed legacy detail remains rejectable.
- Engines declaring `session_revert` expose `Undo last turn` and `Redo` while
  the workspace is idle. Successful changes immediately reload authoritative
  history.

## Frontend state (TASK 16 §92–§98)

- Backend authoritative / frontend projection; the store is a single
  projection updated only by canonical events + typed command results.
- **Stream batching**: `message.delta` events accumulate in the store and
  flush once per ~16 ms frame — N deltas → 1 render. Terminal events flush
  pending deltas first (§23). The diagnostics log excludes streaming noise,
  so a token never rerenders the whole app (§197, §241).
- Per-domain revisions: `queue.revision` and `saipenRevision` bump only on
  their own event families; components refetch the authoritative snapshot
  (initial-query/event race protection, §97, §173).
- Listener ownership: app-level subscription → store; components subscribe
  to the store (useSyncExternalStore); memoized slices skip unrelated
  rerenders (§95, §241).
- UI metadata (active tab, drafts, collapse) lives in components; durable
  UI preferences belong to Rust Storage, never localStorage (§88–§89).
- Send with no active session creates one automatically. Selecting/restoring a
  project auto-starts (or rebinds) its selected engine through one serialized
  latest-intent owner, so rapid selection changes cannot leave an older
  workspace bound. Auto-create responses are committed only while their
  workspace/engine/session intent still owns the composer. Session deletion is
  an explicit confirmed action; active/unknown runs and nonterminal queue
  references fail closed, and no optimistic local deletion is shown.

## SAIPENBAR (TASK 15 + TASK 16)

Compact horizontal strip with per-field authority: PROJECT/STATE/TASK/NEXT/
BLOCKER/WATCH from the reader; VALIDATION (valid/invalid/not run, `· STALE`
when the snapshot moved) from the action registry; action buttons with
availability/disabled reasons; Continue always disabled (no canonical CLI);
Stop enabled only while an action runs; no optimistic state; refetches on
`saipen.*` revision bumps — no polling.

## State management (ownership buckets)

- core-authoritative (projection from events): engine health, queue, session
  metadata, SAIPEN status, workspace, run state;
- ephemeral UI: activity tab, composer draft, tool expand state, scroll
  stickiness;
- derived: never stored as duplicates.

## v1 scope (exactly)

In: Projects, Threads/Sessions, Conversation, Streaming output, Engine
selector, Model selector, Prompt composer, Send/Queue/Cancel, durable Queue
panel, SAIPENBAR + read-only SAIPEN actions, tools/permissions, redacted
diagnostics with copy.

Out: cloud account, voice, remote access, collaboration, plugin marketplace,
browser, terminal emulator, full IDE, file explorer, multiple themes, visual
workflow editor, mobile client, telemetry cloud, custom updater.

## The user must always know

Which project am I controlling? Which engine? Which model? Which session? Is
something running? Can I stop it? What is queued? What failed? What will
happen after restart? What does SAIPEN say? — if the UI cannot answer these,
the UX is incomplete.

## Engine switching + capability-driven controls (TASK 17)

- **Engine selector (TitleBar)** lists every registered engine (Fake,
  OpenCode, and the configured Generic CLI) with a readiness mark — from
  `EngineInfo` (identity + health + capabilities), never an engine-name
  switch statement (§60–§61).
- **Capabilities decide feature availability**: the MODEL selector is shown
  only when the selected engine declares `capabilities.models`; engines
  without models render "engine-controlled" instead of a dead dropdown.
  Model discovery is generation-guarded — a slow response from engine A is
  discarded if the user already switched to B (§111–§112, tested).
- **Session affinity**: sessions belong to the engine that created them
  (SessionManager metadata); switching engines never rewrites an active
  run's history — the run retains its engine (§67, §110).
- **Per-engine diagnostics**: the DIAG panel lists each engine with
  health, adapter version, and capabilities — errors render next to the
  engine they belong to, never as one global banner (§114–§115).
- **Honest unsupported**: engines with `resume=false` never offer Resume;
  the generic CLI shows no model list and no streaming spinner because it
  declares neither (§108, §15).

## Experimental engines (TASK 21)

- **Experimental mark**: `EngineIdentity.experimental` (new cross-engine
  field) is rendered in the engine selector as a `⚠` suffix. DeepSeek
  Harness = `true` (Developer Preview — the UI marks it and never hides
  instability, §88); all other engines = `false`. No engine-id branching
  anywhere in generic UI.
- **Harness uses the same generic surface**: the engine selector, session
  list, conversation view, tool activity, permission surface, and cancel all
  work for Harness exactly as for OpenCode/FakeEngine — capability-driven
  (TASK 21 §87–§95). Harness shows "engine-controlled" for models (no
  selector — `UseEngineDefault`), no Resume (fresh-sessions-only), and
  streams committed chunks as incremental deltas.
- **No Harness WebView / no raw session-log**: the engine is an ACP stdio
  runtime; the cockpit stays SAIWORK2-owned (§89–§90, §95).

## Workspace serialization error (TASK 18 §21–§22, ADR-038)

A direct Send into a workspace that already has an active run in another
session is rejected by the backend with a typed, self-explanatory error
("workspace 'X' has an active run in session 'Y' (one agent run per
workspace)"). The composer shows it inline (ErrorToast) — it is never
silently queued or routed elsewhere. Queue items targeting a busy workspace
wait (queue concurrency = 1), they do not fail. Different-workspace runs
show independent activity (per-project indicator, ACTIVE AGENTS >1 derived
from the actual running count).
