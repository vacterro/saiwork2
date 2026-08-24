# MIGRATION_SAIWORK.md — Donor Audit & Salvage Map

Salvage audit of SAIWORK (baseline `20132bdcd8b1a4ac99b6f72b68df992a79e4c56f`,
branch `saiwork`, v0.1.40, MIT) plus reliability patterns from SAIPENVIEW
(`5b18d1710901485961c1a44a995140bcc549b40a`, MIT), the SAIPEN protocol
(`23bebeafdcd1a2d972ebcde50b0521ca7f26435e`, MIT) and Freebuff
(`5661b80732ca6cd36ceb7c83366a6ed45470e6e3`, Apache-2.0). Audit date
2026-08-16. Old code is optional; old knowledge is mandatory.

SAIWORK itself is a fork of CodeNomad 0.18.0 (commit `9f24190`) with
hardening/isolation fixes — its own history encodes the bug classes below.

Classification legend: `KEEP-CONTRACT` (semantics preserved, implementation
new), `REWRITE` (same role, new implementation), `REFERENCE-ONLY` (study,
don't copy), `DROP` (do not carry).

---

## 1. Queue (deep audit)

Source: `packages/server/src/queue/manager.ts`, `validation.ts`,
`manager.test.ts`.
Baseline: 20132bdc.
Classification: **KEEP-CONTRACT** (semantics) + **REWRITE** (storage backend).
Target subsystem: `saiwork-queue` (phase 2) over `saiwork-storage` SQLite.

### Full lifecycle guarantees observed (from implementation + tests)

| Lifecycle step | Guarantee | Evidence |
| --- | --- | --- |
| create/enqueue | CAS revision hash of `{items, paused}`; duplicate concurrent enqueues serialize — exactly one wins, no lost item | `manager.test.ts` "lets exactly one of two concurrent enqueues win" |
| persist | one manager-wide transaction boundary; temp file → fsync → rename → dir-sync; in-memory `lastPersistedBytes` as rollback material | `persistSnapshot`, "rolls back a directory fsync failure" |
| read/load | one synchronous startup read; corrupt/future persistence **fails closed** (mutations disabled, bytes untouched, never overwritten) | "fails closed on corrupt and future persistence" |
| edit/move/delete/reorder | revision-safe: stale mutation → structured `conflict` + `currentRevision`, changes nothing | "rejects a stale mutation with a structured conflict" |
| claim/dequeue | refuses while paused or empty; final item dequeue → absent revision | "refuses dequeue while paused or empty" |
| dispatch failure | `restore` re-inserts the dequeued item at the front and pauses atomically | "restores a definitely unsent dequeue at the front and pauses atomically" |
| concurrent mutation | global serialization via promise chain — per-key locks are explicitly rejected (concurrent keys rewrite one snapshot) | "serializes concurrent mutations for different keys" |
| fan-out | validate ALL → build ALL tentative states → persist ONE snapshot → commit memory → publish; any CAS/persist failure → zero targets changed, disk untouched | `mutateMany`, "second target conflict => zero targets changed" |
| restart | persisted snapshot reloads; `flush()` awaits an admitted mutation before shutdown | "persists state and reloads it in a fresh manager" |
| event | `queue.changed` published only after successful transition; never on failure | "publishes queue.changed after a successful mutation" |

### GOOD CONTRACT (preserve)
- Single authoritative queue owner; revision CAS; events-after-commit; restore-on-failure; fail-closed on corrupt load; byte-bounded attachments (`MAX_QUEUED_ATTACHMENT_BYTES`, UTF-8 byte measure); key validation regex `[A-Za-z0-9._-]+:[A-Za-z0-9._-]+`; purge-by-prefix; keep/delete semantics for empty queues; `flush()` before shutdown.
- Test list in `manager.test.ts` is the contract spec — each becomes a SAIWORK2 fixture (see REGRESSION_BACKLOG.md).

### OLD IMPLEMENTATION DETAIL (replace)
- Whole-snapshot JSON file rewrite per mutation (contention; single-file bottleneck).
- No per-item lease/claim model — a crash between dequeue and dispatch is recoverable only via the manual `restore` op, not by construction.
- Load failure disables the whole queue (no partial recovery of valid keys).

### Where SQLite lease queue must be STRONGER
1. Per-row atomic claim: `UPDATE ... WHERE id=? AND state='QUEUED'` inside a transaction — no whole-file contention.
2. Crash-between-claim-and-send impossible to lose: item is LEASED (row-level), recovery re-queues stale leases deterministically at startup.
3. Concurrent dispatchers cannot double-claim by construction (row lock + state check).
4. Partial recovery: one corrupt row does not disable the queue.

### Known bugs / historical failures
- Queue persistence was synchronous fs on the Node main thread → every mutation blocked SSE/health/UI on disk (fixed in `5135c5d` "async queue persistence"). Lesson: durable writes never block the UI path; SQLite WAL in a core thread gives this.
- Whole-file transaction model made dispatch not crash-safe mid-flight (design limitation, not a single bug).

### Regression tests/fixtures required
See REGRESSION_BACKLOG.md: lost queued prompt, double dispatch, stale queue state, failed dispatch recovery, restart-during-dispatch, stale lease, corrupt persistence, concurrent CAS conflict, fan-out atomicity, crash-between-claim-and-send.

### Migration notes
Implement `saiwork-queue` with SQLite transactions. Map revision CAS → `updated_at`/revision column checked inside the transaction. Fan-out → single transaction across affected rows. Keep the donor's keep/delete + purge-prefix + restore semantics.

Dependencies: `saiwork-storage`, `saiwork-events`.
Risks: lease expiry vs slow engines; clock skew on lease timestamps (use monotonic-relative expiry with wall-clock fallback).

---

## 2. Queue persistence adapter

Source: `manager.ts` `QueuePersistenceAdapter` (async fs; `DEFAULT_PERSISTENCE`).
Classification: **REWRITE** (superseded by SQLite).
Target: `saiwork-storage`.
Behavior worth preserving: durability ordering temp→rename→fsync-dir; Windows skips dir fsync; rollback material from last persisted bytes.
Implementation to reject: manual temp/rename juggling in a whole-file model.
Notes: SQLite WAL + `busy_timeout` replaces this; the fsync discipline moves into SQLite.

---

## 3. Background-process manager (process ownership)

Source: `packages/server/src/background-processes/{manager,output-writer,stream-ticker}.ts`.
Classification: **KEEP-CONTRACT** + **REWRITE**.
Target: `saiwork-process` (ProcessSupervisor).

Behavior worth preserving:
- Bounded output: 512 KiB cap / 256 KiB retain, rotation only after cap exceeded, UTF-8-boundary-safe tail truncation, hard server-side read cap regardless of client maxBytes, observable `droppedBytes`.
- Serialized appends via promise chain; explicit file positions (append-mode handles reject `ftruncate` on Windows).
- Single-flight live ticks — a slow read can never duplicate bytes or reorder a rotation (SingleFlightTicker).
- Staged stop: graceful 2 s → exit wait 5 s → taskkill 3 s.
- Cleanup errors as typed failures (BackgroundProcessCleanupError).

Known bugs / historical failures:
- Rotation fired at `retainBytes` instead of `capBytes`, contradicting the 512/256 contract (fixed `0fa34df`). Lesson: SAIWORK2 caps are contract, not decoration; test the boundary.
- Per-workspace manager model → cross-manager cleanup coordination; SAIWORK2 replaces with one supervisor (ADR-004).
- A slow/dead SSE consumer caused unbounded server buffering (see §7) — output streaming and event streaming share the same boundedness discipline.

Regression tests: output rotation boundary, UTF-8 truncation safety, single-flight tick under slow read, staged-stop timeout matrix, hostile chatty child cannot grow disk/RSS.

Migration notes: ring buffer in `saiwork-process::output`; single-flight tick concept maps to bounded publish cadence in the event forwarder.

Dependencies: `saiwork-events` (optional), tokio process.
Risks: Windows `taskkill` only terminates console apps; force path is the fallback (documented in PROCESS_LIFECYCLE.md).

---

## 4. Orphan registry + process identity

Source: `packages/server/src/workspaces/{orphan-cleanup,process-identity,spawn}.ts`.
Classification: **KEEP-CONTRACT** + **REWRITE**.
Target: `saiwork-process` (phase 1+).

Behavior worth preserving:
- Each spawn recorded (workspace id + pid + immutable start time); next-start sweep verifies the recorded pid still refers to the SAME process (start-time identity) so a recycled pid is never touched.
- Clean stop forgets the entry; a non-empty registry at startup is exactly the orphan set.
- Guarded signals by process group with leader/members; launch cleanup token env var; WSL pid markers.
- `redactEnvironment` — env keys matching `/(PASSWORD|TOKEN|SECRET)/i` never reach logs.

Implementation to reject: shell probing scripts (`LINUX_IDENTITY_FUNCTIONS` — /proc parsing via sh) — SAIWORK2 uses `libc`/OS APIs.

Regression tests: recycled-pid protection, orphan sweep at startup, tree-kill on shutdown, 0 orphans after normal exit (M0 gate).

Notes: SAIWORK2 `ManagedProcess` carries pid; add identity capture (start time) in the OpenCode phase to protect against pid recycling.

---

## 5. Workspaces runtime (OpenCode spawn site)

Source: `packages/server/src/workspaces/runtime.ts`, `spawn.ts`, `loopback.ts`, `workspace-identity.ts`.
Classification: **KEEP-CONTRACT** (spawn discipline) + **REWRITE**.
Target: `saiwork-core::workspace` + `saiwork-process`.

Behavior worth preserving: exactly one managed `opencode serve` per workspace; spawn spec builder (`buildSpawnSpec`); readiness = port + health + config validity — **no fixed sleeps** (the 1500 ms startup tax was removed in `c586934`; readiness returns as soon as health+config valid, post-ready crash observation runs in background and transitions ready→error through the normal exit path).
Known bug class: fixed-sleep readiness guarantees latency on every cold launch. Lesson for ProcessSupervisor: readiness probes are predicate-based, never sleep-based.

---

## 6. OpenCode auth

Source: `packages/server/src/workspaces/opencode-auth.ts`.
Classification: **KEEP-CONTRACT**.
Target: `engine-opencode` (phase 1).

Behavior worth preserving: `OPENCODE_SERVER_USERNAME/PASSWORD/BASE_URL` envs; generated random password (32 bytes base64url); Basic auth header built from username+password; configured values override generated ones.
Notes: matches master spec §10 "generated local password when applicable". SAIWORK2 adds: loopback-only bind + dynamically assigned port; credential never logged (SECURITY.md).

---

## 7. SSE event route (framing + backpressure)

Source: `packages/server/src/server/routes/events.ts` (+ tests).
Classification: **KEEP-CONTRACT**.
Target: `saiwork-events` forwarder + `saiwork-core` event path.

Behavior worth preserving:
- Serialize each event ONCE as a complete `data: ...\n\n` SSE frame so a native EventSource/onmessage parses every event (double-send regression fixed `f2cd239`).
- Backpressure: writer `write()==false` marks the client backpressured; new events coalesce into a tiny newest-per-type backlog (max 64) preserving order and entity identity; drain resumes; heartbeat defers while backpressured; a client too far behind is **disconnected once and forced to authoritative re-sync** (`ff1833e`).
- No type-coalescing that loses entity identity.

Known bugs / historical failures:
- Slow/dead SSE client caused unbounded buffered events server-side (write() return values ignored; heartbeat never checked writer state) — `ff1833e`.
- Re-queued frames on drain caused double-delivery — `f2cd239`.

Regression tests: slow-client disconnect with bounded backlog, overflow disconnects once, one publish → exactly one onmessage, FIFO order preserved, post-close drop.

Migration notes: SAIWORK2 forwarder = tauri emit with the same contract: bounded bus (1024), lag → reconcile, never re-queue.

---

## 8. Storage identity (instance namespace)

Source: `workspaces/instance-client.ts`, `manager.ts`, commit `f2cd239`.
Classification: **REFERENCE-ONLY**.
Lesson: instance storage keyed ONLY by registered workspace path; raw-id fallback removed — a late client of a deleted instance cannot mint a fresh persistence namespace; unknown ids 404; delete-then-late-write cannot resurrect instance JSON.
Relevance: SAIWORK2 workspace/session ids are core-owned references; "late writer after delete cannot resurrect" is a fixture for `saiwork-storage`.

---

## 9. EventBus (Node)

Source: `packages/server/src/events/bus.ts`.
Classification: **REWRITE** (bounded typed bus in Rust).
Behavior worth preserving: late-joiner replay (onEvent re-emits current instance statuses to new subscribers) — SAIWORK2 solves via authoritative state queries on subscribe (frontend bootstrap loads state, events then delta).
Implementation to reject: unbounded Node EventEmitter listener model (law 13).

---

## 10. Google / Antigravity adapter

Source: `packages/server/src/google/{adapter,sanitize,errors,providers,shim,antigravity-session,tool-call-persistence}.ts`.
Classification: **REFERENCE-ONLY**. Direct Antigravity adapter is NOT carried (spec §22; ADR-010). Integration happens through the OpenCode adapter unless a documented gap exists.
Target: `engine-opencode` provider mechanisms (phase 1/6).

Behavior worth preserving (as knowledge):
- Execution-boundary adapter: provider+model → OpenCode provider/model/auth-mode converted ONLY at the boundary; no OpenCode-specific strings leak elsewhere.
- Provider fallback never implicit (`allowFallback` gate) — prevents silent paid-credit burn on a different billing pool.
- Normalized provider-aware error classification: `free_tier` failures are NOT subscription-quota failures (prevents "rotate your key" advice that cannot help).
- Secret shapes: Gemini key `AIza…`, OAuth `AQ.…`/`ya29.…`, refresh `1//…`, `ghp_…`, `sk-…`; header-line sanitization; per-line stderr sanitization.
- Antigravity runs through a local shim: OAuth session server-side, workspace receives no credential and no env var.

Known bugs / historical failures:
- Antigravity `cloudcode-pa` streams **CRLF-delimited SSE**; the shim split on LF only, collapsing the whole stream to the first `data:` line (text answers came back as their first word) — fixed `8287287` by normalizing line endings before splitting.
- Tool turns worked while text answers broke (tool decision is the first frame) — a subtle "works for tools, broken for text" failure class.

Regression tests: CRLF SSE parsing, LF-only collapse, malformed frame containment, provider fallback refused without flag, free-tier vs quota classification.

---

## 11. Freebuff engine + gateway (SAIWORK-side)

Source: `packages/server/src/freebuff/{engine,gateway,client,install,quota,shell-lifetime,types}.ts`.
Classification: **REFERENCE-ONLY** → isolated `engine-freebuff` adapter in phase 5.

Behavior worth preserving (as contract for the future adapter):
- Engine process lifecycle: PORT=0 dynamic port; readiness probe bound to the OWNED pid + launch identity (forged readiness is ignored); launch identity + piped token; kills/reaps a child that never announces readiness; initial timeout retried once ONLY after the failed child is gone; crashed child restarted on same port with fresh identity; crash loop bounded to three respawns; lifetime-close for graceful Windows shutdown before forced kill; never reports "stopped" when even forced shutdown cannot contain the child; refuses start when the installed version is unverifiable.
- Gateway: thread identity = workspace + session id + model, NEVER prompt text; replay dedup registry with tombstones + global byte cap + oldest-session eviction; concurrent identical requests surface in-flight instead of double-posting; one in-flight turn per thread; model switch → new thread; first user text is title seed only.
- Freebuff is a full agent orchestrator (threads + queue + own tools), surfaced as an OpenAI-compatible "super model"; OpenCode tool layer not involved.

Known bugs / failure classes: session-limit recognition; timeout → stop thread exactly once; abort stops exactly once.

Regression tests (phase 5): forged readiness rejected, crash-loop bound, replay dedup, double-posting prevention, stop-exactly-once.

---

## 12. SAIPEN modules (SAIWORK-side)

Source: `packages/server/src/saipen/{core,state,board,file-watcher,path-security,utf8,auto-update}.ts`.
Classification: **KEEP-CONTRACT** (read/watch) — writes only via canonical path.
Target: `saiwork-saipen` (phase 3) + `saiwork-core::saipen` (phase 0).

Behavior worth preserving:
- No vendored protocol copy; point at the live `saipen/` install (drift-free). `DEFAULT_SAIPEN_FILES = [BOOT.md, STYLE.md]` injected via OpenCode `instructions`.
- STATE.md is YAML frontmatter (`--- … ---`), single-line scalars; first-match wins on duplicates; a duplicated key is reported as an issue, never silently resolved; quotes stripped; `saipen_home` backslash unescaping is caller's concern.
- BOARD.md ticket status comes from the SECTION (`## DOING/TODO/DONE/BLOCKED`), never the checkbox alone.
- Watcher: debounce 300 ms + guarded 10 s self-heal re-sweep only where Windows drops rename events; watches `.saipen/` STATE/BOARD/LOG + `kitchen/*.md`; one fs event → one structured `saipen.changed {root,file}`.
- Path security: `resolvePathWithin` resolves existing OR future children through the nearest existing ancestor; lexical traversal AND symlink/junction escapes fail closed; `pathsEqual` case-insensitive on win32; `canonicalExistingPath` = realpath+normalize.
- UTF-8-safe head/tail truncation without splitting code points.
- Structural guard: SAIPEN module sources must never spawn/fork a second runtime (regex-based test `no-second-runtime.test.ts`).

Known bugs / historical failures:
- Restricted filesystem ops used lexical containment only → symlink/junction escape (fixed `0652a5b` realpath containment). This is the direct evidence behind SECURITY.md "no naive path.startsWith".

Regression tests: path escape (.., symlink, junction, absolute injection), frontmatter parsing, duplicate-scalar issue, board section semantics, UTF-8 truncation, no-second-runtime, watcher rename/replace, event storm coalescing.

---

## 13. atomic-write

Source: `packages/server/src/atomic-write.ts` (+ test).
Classification: **REWRITE** as SQLite transactions (queue/state) + std atomic rename (files that remain).
Target: `saiwork-storage`.
Behavior worth preserving: crash-atomic temp→rename discipline applied to auth store, orphan registry, TLS cert writes (`8c1381c`).

---

## 14. shutdown

Source: `packages/server/src/shutdown.ts` (+ test).
Classification: **KEEP-CONTRACT** + **REWRITE**.
Target: `saiwork-core::app::shutdown`.
Behavior worth preserving: ordered shutdown; `flush()` of admitted queue mutations before exit.
Notes: SAIWORK2 sequence documented in PROCESS_LIFECYCLE.md; adds engine stop + process tree kill + watcher disposal + bounded waits.

---

## 15. log-sanitize

Source: `packages/server/src/log-sanitize.ts` (+ test).
Classification: **KEEP-CONTRACT**.
Target: `saiwork-diagnostics::redact`.
Behavior worth preserving: one sanitizer boundary (recursive, secret-bearing keys, bounded strings) used by the global HTTP trace, proxy payload traces, and auth routes (`0fa34df`); tokens/keys/cookies never reach trace logs.
Notes: SAIWORK2 redaction at the log boundary, never after the fact (SECURITY.md).

---

## 16. launcher / browser launch

Source: `packages/server/src/launcher.ts`.
Classification: **DROP** (browser launcher out of core scope; Tauri opens windows, not browsers).

---

## 17. Desktop shells (Electron + Tauri)

Source: `packages/electron-app/`, `packages/tauri-app/`.
Classification: **DROP dual-shell** (ADR-001); keep expected user behavior.
Evidence of the landmine: two shells duplicating lifecycle/state logic; e.g. Electron-specific main-process guard (`2de91b7` URL scheme policy, `8287287` WebContents teardown race) had no Tauri counterpart — the fix surface was duplicated. SAIWORK2: one runtime.
Behavior worth preserving: window/layout persistence; portable start scripts semantics (START.bat → portable.flag mode); explicit URL scheme policy (never allow unmanaged `window.open`).

---

## 18. opencode-plugin

Source: `packages/opencode-plugin/`.
Classification: **REFERENCE-ONLY**. Optional later integration only on demonstrated need (law 25).

---

## 19. Permissions (auto-accept, opencode-replier, yolo metadata)

Source: `packages/server/src/permissions/`.
Classification: **REFERENCE-ONLY**.
Notes: `permission.requested`/`permission.resolved` are in the canonical taxonomy; auto-accept policy is a later, explicitly-configured feature. Bounded-Yolo-hydration failure handling (`dfd0fc2`) is a fixture idea: permission state must fail closed.

---

## 20. Out-of-scope subsystems

Source: `speech/`, `plugins/voice-mode.ts`, `previews/`, `releases/`, `opencode-update/`, `cloudflare/`, `usage/`, `auth/`.
Classification: **DROP** (master spec §25). Previews: only the sandbox/bounds lesson (`f9d99ff`) — bounded previews and cache invalidation are noted, not carried.

---

## 21. Provider/model normalization

Source: `packages/ui` model lists, `google/models.ts`, `api-types.ts` capability shapes.
Classification: **REFERENCE-ONLY** (idea level).
Behavior worth preserving: capability-normalized UI (no engine-string branching); model ids namespaced `provider/model`.
Notes: SAIWORK2 `EngineCapabilities` (ENGINE_CONTRACT.md) formalizes this; UI builds on capabilities only.

---

## 22. SAIPENVIEW reliability patterns (cross-donor)

Source: `saipenview/saipenview/` (watcher, paths, ownership, guard, protocol_write, outbox, runtime, service, external_changes).
Classification: **REFERENCE-ONLY** (patterns; implementation is Python and stays there).

| Pattern | Source location | Why useful | Reusable directly? | Target subsystem |
| --- | --- | --- | --- | --- |
| Canonical path layer (normcase → resolve → normpath → drive-root trailing sep) | `paths.py` | one true spelling for comparisons | concept only (Rust std) | `saiwork-core` path utils (SECURITY.md) |
| Watcher in project registry, not process manager | `watcher.py` | project state watched regardless of agent running | concept | `saiwork-saipen` (phase 3) |
| One fs event → one structured event; never string interpolation into JS | `watcher.py` | injection/robustness | concept | `saiwork-saipen` + event forwarder |
| Per-root single-writer ownership; reservation pair (app tx vs agent launch) under ONE lock | `ownership.py` | check-then-act atomicity across two actors | concept | `saiwork-saipen` write path (phase 3) |
| Canonical writer pipeline: OS lock, recovery preflight, immutable PREPARED journal, ordered targets, byte+semantic verification, COMMITTED | `protocol_write.py` + `saio.py` | SAIWORK2 must be a CLIENT of this, not a second engine | DO NOT reimplement — call canonical writer | `saiwork-saipen` (calls saipenview/saio or canonical CLI) |
| Decisions bound to snapshot hashes; STALE_STATE → re-run decision ONCE on fresh snapshot, never blind retry of stale bytes | `protocol_write.py` | retry correctness | concept | queue + saipen retry policy |
| Strict parsing fails closed (duplicate field = structural error, never last-write-wins; typed `critical`) | `outbox.py` | malformed state must be an error, not a guess | concept | `saiwork-saipen` parser |
| Single-instance TCP guard; bounded stale-bind retries; backlog=16; NO SO_REUSEADDR on Windows | `guard.py` | second launch relays intent; backlog-1 bug broke handoff forever | concept | `src-tauri` single-instance (tauri plugin covers) |
| Per-process output lock; finalize lock; launch reservation under same per-root lock | `runtime.py` | output append vs finalize races | concept | `saiwork-process` |
| External-change detection vs app writes | `external_changes.py` | who changed the file | concept | `saiwork-saipen` watcher |

---

## 23. SAIPEN protocol (canonical authority)

Source: `saipen/` repo (SPEC.md, RFC.md normative, CONFORMANCE.md, MANIFEST.json, BOOT/STYLE/CORE/MAINTENANCE/…).
Classification: external authority; SAIWORK2 is a **client** (ADR-007).

Canonical facts for SAIWORK2:
- STATE answers "what do I do now"; BOARD "what task"; LOG "why here"; KNOWLEDGE "durable truth"; `next_action` is the heart.
- MANIFEST.json is the single source of protocol files; `tools/validate.py` is canonical validator (STATE.md against `state.schema.json`, E-### uniqueness/monotonicity, parent resolution); shell scripts as portable floor.
- Conformance: board `needs:` graph acyclic + all references resolve (dangling reference worse than cycle); 16-phase enum + legal transition table; TEST-001 continuation test.
- On-disk contract MUST remain stable; implementations MAY vary.

WHAT SAIWORK2 MAY READ: STATE/BOARD/LOG/KNOWLEDGE canonical files; manifest; validation results.
WHAT SAIWORK2 MAY REQUEST/MUTATE: only through canonical commands/writers (saipen CLI, validate.py, canonical writer pipeline) — never direct `.saipen` writes (phase 3+).
WHAT SAIWORK2 MUST NEVER REIMPLEMENT: the protocol state machine, a second transaction engine, or a second writer (no-second-runtime guard becomes a SAIWORK2 structural test).

---

## 24. Freebuff (repository-level)

Source: `CodebuffAI/freebuff` repo.
Classification: UX/architecture/optional-engine donor. NOT foundation.

| Concept | Classification | Notes |
| --- | --- | --- |
| Terminal-first UX, run events (agent start/finish, tool calls/results, text, errors) via `handleEvent` | ARCHITECTURAL IDEA | SAIWORK2 canonical event taxonomy covers the same shapes |
| Agent id + prompt + `previousRun` continuation | SDK CAPABILITY / OPTIONAL FUTURE INTEGRATION | maps to `resume` capability in ENGINE_CONTRACT |
| Tool-stream parser, run-agent-step, compact-history | ARCHITECTURAL IDEA | streaming/batching discipline (PERFORMANCE.md) |
| Custom agents / custom tools / MCP | OPTIONAL FUTURE INTEGRATION | phase 8, via capability flags |
| Agent store / cloud account | NOT RELEVANT | spec §25 excludes cloud ecosystem |
| DI over module mocking; tmux CLI tests | UX/DEV IDEA | SAIWORK2 testing convention (TESTING.md) |

---

## Architectural landmines (evidence-based, NOT to repeat)

1. **Dual desktop shells** (Electron + Tauri): duplicated lifecycle/state/fix surface; Electron-only security fixes (URL scheme, WebContents race) had no counterpart. Evidence: packages/electron-app + packages/tauri-app. → One runtime (ADR-001).
2. **Competing process ownership**: per-workspace managed runtimes + orphan registry + background-process manager = three overlapping process books that needed cross-manager coordination. → Single ProcessSupervisor (ADR-004).
3. **Whole-file queue persistence**: every mutation rewrote one JSON snapshot; synchronous fs blocked the event loop (fixed 5135c5d); dispatch not crash-safe by construction. → SQLite lease queue (ADR-003).
4. **Unbounded SSE buffering for slow clients** (ff1833e): write() return ignored, heartbeat never checked → unbounded memory. → bounded bus + disconnect-and-resync (EVENTS.md).
5. **Lexical-only path containment** (0652a5b): symlink/junction escape; realpath containment added later. → fail-closed canonical path resolution from day one (SECURITY.md).
6. **Provider abstraction over provider abstraction**: SAIWORK had its own Google provider layer + shim + OpenCode provider mapping; fallback gating had to be re-invented. → route through OpenCode provider/auth; direct adapters only on documented gap (ADR-010).
7. **Fixed-sleep readiness** (c586934): 1500 ms startup tax. → predicate-based readiness probes only.
8. **Multiple SAIPEN surfaces**: server-side `saipen/` module + embedded SAIPENVIEW render + auto-update — with a structural test to stop a second runtime. → SAIWORK2 single `SaipenClient`, read/watch/validate only (ADR-007).
9. **Frontend/backend responsibility leakage**: UI could request filesystem ops and instance writes directly through routes; late writers of deleted instances could resurrect namespaces (f2cd239 storage identity). → UI is a projection; every mutation has one authority (laws 18/23).
10. **Broad polling / periodic scans**: watcher self-heal was kept bounded and documented as an exception; full-tree polling was never allowed in. → law 12 + the one guarded backstop (SAIPEN.md).

## TASK 13 — durable queue carry-over (2026-08-16)

Donor queue contract inherited and strengthened per QUEUE.md: revision CAS,
events only after committed transitions, restore-on-failure, bounded payload,
fail-closed on load failure. Strengthened in SAIWORK2: per-row atomic claims
(donor serialized the whole queue), `LEASED` prepare/sending phases with
startup recovery (donor had no lease → Q-01/Q-06 loss window), ambiguous-handoff
manual-review state instead of blind re-dispatch, run_id-guarded terminal
transitions so old callbacks cannot complete a newer attempt, durable pause
flag, and a single event-driven dispatcher (ADR-023/024/025/026).

## TASK 14 — SAIPEN read integration carry-over (2026-08-16)

Donor read-path lessons adopted (SAIPEN.md): `.saipen/STATE.md` presence
marker; STATE frontmatter with single-line scalars (duplicate key = error,
never last-wins); BOARD status from section, not checkbox; watcher debounce
~200–300 ms; one fs event → one structured `saipen.changed`; path security
per SECURITY.md. SAIWORK2 strengthened: typed discovery results
(NotPresent ≠ broken), schema-version gating (unsupported → rejected, never
parsed), component-aware path containment (string-prefix was the donor
landmine #5), generation-tagged watcher sessions, bounded channel with
overflow → full reread, semantic change suppression, and a strict read-only
assertion suite (no file modified, no residue, no mtime write).

## TASK 15 — SAIPEN actions + SAIPENBAR (2026-08-16)

Action path adopted: `UI button → typed backend command (workspace_id,
action string) → SaipenTool (resolved from STATE `saipen_home`) →
ProcessSupervisor (no shell, cwd = validated workspace root) → canonical
tool → filesystem change → TASK 14 watcher → fresh snapshot → UI`.
SAIWORK2 never edits canonical files directly. Verified against the real
tool: `saipen.py` CLI has NO `continue`/`board`/`knowledge`/`validate`/`stop`
commands, so those SAIPENBAR labels map honestly (views / unsupported /
control) and the UI disables Continue with the reason. `validate.py` exit
semantics (0 valid / 1 domain-invalid / 2 usage) are encoded exactly and
proven by regression tests running the vendored validator end-to-end.
State carried forward: action registry is in-memory only; validation
results are generation-bound; on restart everything is rediscovered fresh.

## TASK 16 — Primary Desktop UX (2026-08-16)

The 5-column dev grid became the three-pane cockpit: TitleBar (project/
engine/model + engine start/stop) · left nav (projects + sessions, SAIPEN
badge per project) · Conversation (streaming plain text → Markdown at
terminal, copyable code blocks, stick-to-bottom scroll + jump-to-latest,
tool/permission rows) · right ActivityPanel (Activity/Queue/Diagnostics
tabs) · Composer (Send / Queue / Cancel run, Enter/Shift+Enter/Ctrl+Enter)
· SAIPENBAR strip (compact fields + action buttons) · statusline. Golden
Vintage tokens centralized in `global.css`. Permissions now resolve end-to-end
(`SessionManager::resolve_permission` → Tauri command → Allow/Deny). The
critical fix: `message.delta` is batched in the store and excluded from the
log — a token never rerenders the app (previously every event pushed a log
entry → global rerender). Frontend state tests (10, vitest) cover batching,
terminal flush, log filtering, permission lifecycle, revision guards, and
Conversation rendering. Only new runtime dependency: react-markdown +
remark-gfm. State carried forward: no frontend authority over runtime or
durable state — a refresh reconstructs everything from backend snapshots.

## TASK 17 — Additional Engines (Freebuff DEFERRED + Generic CLI)

Freebuff was re-verified from the vendored snapshot (`@codebuff/sdk` 0.10.7,
Apache-2.0) and classified **DEFERRED**: remote-cloud-only execution behind a
mandatory API key, Node ≥ 22-only SDK with a heavy JS dependency tree, and a
`cli/` that is the full application rather than a headless engine. No
SAIWORK2 core distortion, no credential vault, no source copy (ADR-036).

The generic engine architecture is instead proven by a second **production**
adapter: `engine-generic-cli` (ADR-037), the safe `OneShotText` form —
explicit trusted env config, no shell, prompt as stdin bytes, bounded
output/timeout, run==process cancel, honest capabilities. The only generic
process changes: `StdinPolicy::Bytes` (bounded stdin) and a per-process
output cap — both bounded, both proven not to affect OpenCode/FakeEngine.
EngineRegistry now hosts fake + opencode + generic-cli with per-engine
health/capabilities; diagnostics expose a per-engine row; the UI model
selector is capability-driven with a generation guard. Cross-engine tests
prove ID isolation, no-fallback, and failure isolation; the durable queue's
`engine_id` targeting needs no change (typed EnginePort). State carried
forward: OpenCode and the queue behave exactly as before (full regression
green).

## TASK 18 — Parallelism + Release Hardening (V1 gate)

Parallelism decision: **one agent run per workspace** — `SessionManager`
rejects a same-workspace second send with typed `WorkspaceBusy`; the queue's
`session_busy` is workspace-aware and New-mode dispatch checks busy before
the send; different-workspace runs run concurrently and isolated; same-
session REJECT unchanged (CLI adapter now enforces it); **queue concurrency
= 1** kept as the proven boundary (ADR-038). Release hardening: minimal CSP,
release-gated FakeEngine + failpoints, input bounds verified, audits clean
(locks, panics, TODOs, duplication, polling), packaged release build PASS
(MSI + NSIS, portable launch Ready in 7 ms, single-instance verified).
V1 RELEASE GATE PASS. State carried forward: OpenCode, queue, SAIPEN, UI,
and the generic CLI behave exactly as designed; the workspace gate is the
one new correctness rule.
