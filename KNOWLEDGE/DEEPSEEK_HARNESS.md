# DEEPSEEK_HARNESS.md — DeepSeek Harness Audit + Integration Contract (TASK 19) + Adapter Foundation (TASK 20/24) + Vertical Slice (TASK 21/24) + Queue Target (TASK 23/24) + Final Status (TASK 24/24)

Audit date: **2026-08-17**. Sections 1–21 are the TASK 19 audit + integration contract.
Section 22 records the **TASK 20 implemented truth** (the foundation adapter that now
exists in `crates/engine-deepseek-harness`). Section 23 records the **TASK 21
implemented truth** (the first complete Harness agent vertical slice). Section 24 records
the **TASK 23 implemented truth** (Harness as a durable QueueManager target). Section 25
records the **TASK 24 final release status**.

---

## 1. Baseline (verified current upstream)

| Item | Value |
|---|---|
| Repository | `github.com/deepseek-ai/deepseek-harness` |
| Audited ref | `master` tree `47f943859bef60e4160492346772ded9b24f765a` (2026-08-17) |
| npm package | `@deepseek-ai/dsh` **0.1.0-rc.6** (`latest` tag), MIT |
| License | MIT (repo `LICENSE`, 1065 B; npm `license = MIT`) |
| Runtime | TypeScript on **Node ≥ 22.19 / 24+**; pnpm workspace; vendored Cordis kernel |
| CLI | `npx @deepseek-ai/dsh web` → Web UI at `http://127.0.0.1:3080` |
| Status | **Developer Preview** — README: *"THERE WILL BE COMPATIBILITY-BREAKING CHANGES."* |
| Windows | First-class shell story (`dsh-pwsh-local`), Node-based runtime — see §12 |
| Python SDK | `python/` mirrors the SDK wire shapes (no TS import dependency) |

Repo shape: `apps/`, `packages/` (~50 plugin packages), `native/`, `vendor/` (Cordis),
`docs/` (architecture, capability-seams, config-catalog, persistence-catalog,
event-producer-consumer, agent-lifecycle, …), `website/`, `examples/`.

---

## 2. Classification

**B — EXPERIMENTAL ENGINE CANDIDATE** (not yet Production).

Reason: real machine-facing seams exist (ACP server, SDK JSON-RPC server, headless bundle),
the Windows path is concrete (pwsh-local, taskkill tree termination, spawn-per-call shell —
no PTY requirement), and the session-log model is genuinely strong. But the product is
explicitly Developer Preview with compatibility-breaking changes, the SDK wire is `0.0.1`
with **no version negotiation**, ACP is **fresh-sessions-only**, and SDK has **no cancel /
no permission round-trip**. Production-grade integration is therefore deferred behind a
strict adapter firewall (§7) until a TASK 20–21 probe proves the chosen seam stable enough.

---

## 3. Harness architecture (what it is)

- **Everything is a plugin** on vendored Cordis. Plugins provide models, tools, skills,
  sessions, sandboxes, storage, loops, scheduling, UI. No privileged core to patch;
  registrations are reversible effects that unwind on plugin unload.
- **Profiles / bundles**: a running `dsh` is a plugin tree composed at boot from ordered
  layers — bundles (per profile), then profile `cordis.patch.yml`, then home-level patch,
  then `--patch` overlay. Profiles live in the **Harness home** (not the project workspace).
  `web` and `headless` ship as templates; `dsh-headless` = one-shot runner, no server.
- **Capability seams**: every swappable capability has Service Definition + Service Provider
  + Consumer. `ctx` keys: `sessions`, `systemPrompt`, `tools`, `agents`, `agentLoop`,
  `scope`, `llm`, `shell`, `subprocess`, `sandbox`, `fs`, `terminals`, `jobs`, `goals`,
  `commands`, `sessionTitle`. Filesystem and subprocess share one execution world.
- **Session log**: append-only `SessionEvent` log is the source of model context —
  *"model-visible means logged"* (runtime invariant). Fork, resume, search, replay,
  compaction, transcripts all derive from this stream.
- **Turn model**: a **step** = one model request + the tools it calls; a **turn** = zero or
  more steps. Durable session events: `turn/*`, `step/*`, `user/message`, `assistant/*`,
  `tool/*`. Live extension points: `agent/*`, `llm/stream`, `tools/*` (waterfalls).
- Standard mode: file editing, shell, file/web search, skills, planning, goals, subagents,
  workflows, code-mode SDK. Minimal mode: bash + str_replace_editor only.
- Subprocess: `dsh-subprocess` service with bounded spill-backed output, credential scrub,
  kill escalation, group mechanics; `dsh-subprocess-local` owns spawn/tee/join.
- Sandbox: `ctx.sandbox` backend wraps argv before spawning; confinement is composed, not
  built-in ("Unconfined by itself — deployments needing confinement compose a sandboxing
  executor or policy").

---

## 4. Transport comparison (decision matrix)

### 4.1 ACP — `@deepseek-ai/dsh-acp` (Agent Client Protocol over JSON-RPC stdio)

| Aspect | Evidence |
|---|---|
| Lifecycle | One `AgentSideConnection` on stdin/stdout; connection-owned sessions; teardown settles pending prompts, drains owned agents, disposes |
| Handshake | `initialize` negotiates the supported version; advertises baseline-only prompts (no image/audio/embedded-context) |
| Sessions | `session/new` = **fresh agent**, absolute primary `cwd`; empty `additionalDirectories`/`mcpServers` accepted, **non-empty rejected**. One in-flight prompt per session |
| Streaming | `session/update` emits one `agent_message_chunk` per non-empty text block of a **committed** `assistant/message` — no raw deltas, no token-level streaming |
| Tools | Tool activity stays in the session log, not on the wire |
| Permissions | `session/request_permission` — one-shot allow/reject choices for bridge-owned approval requests carrying a tool call id; clients may answer automatically |
| Cancel | `session/cancel` — cancels only the addressed agent; pending prompt settles `cancelled`; unknown ids are no-ops |
| Termination | Prompt waits for whole agent idle → `end_turn`; ACP cancel/disposal/discarded admission → `cancelled`; model error on correlated turn rejects the prompt immediately |
| Versioning | ACP `initialize` version negotiation ✓ |
| Windows | Transport is pure stdio JSON-RPC — Windows-viable (probe pending in TASK 20) |
| Pros | Full session lifecycle incl. **cancel + permissions**; clean automation result with stop reason; per-session workspace; narrow trust surface (rejects project MCP/additional dirs) |
| Cons | **Committed answers only** (no live deltas/reasoning/tools on wire); **fresh sessions only** (no load/list/resume/delete/fork); connection-owned lifetime (no per-session close); baseline prompts only |

### 4.2 SDK JSON-RPC — `dsh-sdk-jsonrpc-server` + `dsh-sdk-protocol`

| Aspect | Evidence |
|---|---|
| Transport | Newline-delimited JSON-RPC 2.0 over stdio (`JsonRpcLineTransport`); one compact JSON frame per `\n`-terminated line; malformed lines ignored; `-32601` unknown method; `-32603` handler rejection |
| Methods | `initialize` → `InitializeResult`; `session/prompt` → `{ messageId }` (durable enqueue receipt); `shutdown` → `{}` then dispose + exit 0 |
| Notifications | `session.event` (**every durable fact, every session, unfiltered** — full session-log envelopes), `session.status` (whole-agent running/idle), `subagent.started`, `subagent.finished` (in-process runs only) |
| Sessions | Server gets-or-creates one agent per `sessionId`; persistence roots and persona from `cordis.yml`; **no per-session close** — agents live until process shutdown |
| Streaming | Full session-log envelopes incl. reasoning/tool content — richest observability of the three seams |
| Permissions | **None current** — server→client requests are a *dead capability*; the Python SDK's responder surface exists for future approval flows |
| Cancel | **None** — a client abandons a turn by closing the runtime process |
| Versioning | **No protocol-version negotiation** — `serverInfo.name` is wire-stable `deepseek-harness-sdk-runtime`; `serverInfo.version` = `0.0.1`, **unvalidated by clients** |
| Per-prompt result | **None** — `messageId` identifies inbox admission only; clients own the automation interval themselves |
| Windows | Pure stdio JSON-RPC — viable; stdout purity is deployment-enforced (stdout logger would corrupt the channel) |
| Pros | Durable enqueue receipt; full session-log observability; get-or-create sessions; Python SDK mirrors shapes; `shutdown` is clean |
| Cons | No cancel; no permission flow; no version negotiation (pre-release wire); unfiltered full-log envelopes (volume/privacy); no per-prompt completion correlation; no per-session close |

### 4.3 Headless — `dsh-headless` bundle

| Aspect | Evidence |
|---|---|
| Shape | One-shot runner, **no server at all** (bundle template in `docs/architecture.md`) |
| Fit | Ideal for a future one-shot Harness queue engine (input → bounded run → terminal text), weak for persistent interactive sessions |
| Status | Only identified from architecture docs in this audit — no wire-level evidence collected; deeper probe belongs to TASK 20/21 |
| Verdict | Not the primary seam; possible **separate future one-shot engine** — must not be conflated with the persistent ACP adapter |

### 4.4 Web UI (`dsh web`)

Human interface (browser app, port 3080) — rejected as machine-facing integration.
No API/versioning contract for automation.

### 4.5 Summary ranking

1. **ACP over stdio** — the only seam with cancel + permissions + version negotiation +
   clean termination + narrow trust surface. Best EngineAdapter fit.
2. **SDK JSON-RPC** — richest observability and durable enqueue receipt; but missing
   cancel/permissions/versioning makes it the fallback for observability-critical or
   queue-enqueue semantics.
3. **Headless one-shot** — future separate one-shot engine only.
4. **Web UI** — not machine-facing.

---

## 5. Decision

- **PREFERRED SEAM (TASK 20):** ACP over stdio.
  Rationale: full session lifecycle (new/prompt/cancel), one-shot permission resolution
  compatible with SAIWORK2's existing `resolve_permission` path, ACP version negotiation,
  `end_turn`/`cancelled` termination maps directly onto the generic terminal contract,
  committed-message chunks map to `message.completed` without fake token deltas, and the
  narrow baseline surface (no project MCP, one workspace per session) is the safest trust
  boundary for an experimental runtime.
- **FALLBACK SEAM:** SDK JSON-RPC — used when full session-log observability or durable
  enqueue correlation is required; transport is the same class (NDJSON JSON-RPC over stdio),
  so a shared framing/transport layer in the adapter crate serves both.
- **REJECTED FOR PRIMARY:** Web UI (human interface); headless one-shot (no session model —
  revisit only as a distinct one-shot engine if the product wants it).
- **No two simultaneous authorities:** the adapter selects one seam per runtime; ACP for
  interactive engine use, SDK only where explicitly justified (TASK 23 decision).

---

## 6. Protocol stability firewall

- No Harness DTO may exist outside the future `engine-deepseek-harness` adapter crate.
  Generic core sees only `EngineCapabilities`, `EngineState`, `SessionId`, `RunId`,
  message/tool/permission events, and normalized errors (existing categories).
- Adapter probes `serverInfo`/`initialize` result, **records the tested version**,
  **rejects known-incompatible contract**, tolerates additive fields.
- ACP negotiates version on `initialize`; the SDK wire (`0.0.1`) has no negotiation — the
  adapter must treat the SDK wire as version-pinned until upstream adds negotiation.
- No DTO reuse in React/QueueManager/SAIPEN (same rule as TASK 17 for Freebuff).

---

## 7. Runtime / process ownership (no double supervision)

- **SAIWORK2 owns the top-level Harness runtime process**: spawn via ProcessSupervisor
  (typed ProcessSpec: executable = node/npx or bundled runtime, cwd = Harness home or
  workspace, bounded stdio, explicit env, taskkill-tree kill fallback on Windows).
- **Harness owns its internal agent/tool/subprocess lifecycle** (`ctx.subprocess`,
  `ctx.jobs`, sandbox, shell executors). SAIWORK2 observes normalized events through the
  seam; it must **never** attach ProcessSupervisor to inner shell/tool commands.
- Cancellation of a run = ACP `session/cancel` (scoped), **not** killing the runtime;
  stopping the engine = protocol `shutdown`/dispose, then ProcessSupervisor stop, then
  final kill fallback.
- Engine stop legitimately terminates all runs owned by that runtime; other engine
  runtimes (OpenCode, Generic CLI) survive — per TASK 17/18 per-EngineId health.

---

## 8. Session authority

- **Harness owns Harness sessions** (append-only SessionEvent log; resume/fork/search/
  replay/compaction are engine-local).
- SAIWORK2 stores only: engine reference, session reference (canonical upstream id mapped
  through a SAIWORK2 SessionId wrapper), queue correlation, app-owned metadata.
- **No SQLite transcript mirror** for Harness (same rule as OpenCode/Freebuff).
- ACP sessions are **fresh + connection-owned**: a SAIWORK2 session maps to a live ACP
  session and does **not** survive app restart (fresh-sessions-only). The SDK path
  get-or-creates per sessionId but agents live until process shutdown. Session-resume
  capability must be declared **false** until upstream adds it.

---

## 9. Credential authority

- Harness owns its credentials (credentials plugin + home-level settings).
- SAIWORK2 does **not** copy or vault DeepSeek API keys; provider/model selection is passed
  through the seam (`provider`/`model` config on ACP initialize; adapter records which it
  requested). No SAIWORK2-owned credential storage for Harness (matches ADR-036 rule).
- Environment: adapter passes a minimal inherited env (ProcessSpec restrictive mode);
  never echo Harness credentials into SAIWORK2 logs/diagnostics.

---

## 10. Sandbox boundary

- Sandbox is **engine-internal** (`ctx.sandbox` wraps argv before spawn; confinement is
  composed by profile, not guaranteed).
- SAIWORK2 must not claim Harness activity is sandboxed unless the selected Harness
  profile/policy proves it; capability/status exposed honestly.
- **No double/conflicting sandbox**: SAIWORK2's outer policy (one mutating run per
  workspace, per-workspace cwd, bounded output — TASK 18) stays; Harness inner sandbox
  is engine-local. Windows sandbox strength is UNKNOWN → TASK 20 probe must verify before
  any profile is offered as "safe default".

## 11. Permission model

- ACP: `session/request_permission` — one-shot allow/reject with tool call id. Compatible
  with SAIWORK2's existing Allow/Deny `resolve_permission` flow (adapter translates
  upstream request → generic permission event → resolved decision → upstream answer).
- SDK: **no current permission round-trip** — approval must be policy-resolved inside
  Harness (approval-policy plugin). Do not claim a permission capability on the SDK seam.
- Fail-closed: ACP prompt rejects on model error; unknown permission requests default to
  deny until a policy is configured (probe verifies the default approval policy).

## 12. Windows support (verified, not assumed)

| Feature | Status | Evidence |
|---|---|---|
| Core runtime (Node) | SUPPORTED | npm/npx install; official quick start runs on Windows; probe exit 0 |
| Shell | SUPPORTED | dedicated `dsh-pwsh-local`: `pwsh -NoLogo -NoProfile -NonInteractive -Command`, pwsh 7 + Windows PowerShell 5.1 fallback resolution, UTF-8 output pinned, native Win32 paths pass through |
| Process tree termination | SUPPORTED | taskkill on Windows (kill escalation + post-exit drain via `dsh-subprocess-local`) |
| PTY | NOT REQUIRED | shell executor is **spawn-per-call** (no persistent shell, no PTY); terminals are an optional plugin — machine seams need no PTY |
| ACP / SDK JSON-RPC | SUPPORTED (transport) | pure stdio JSON-RPC; no platform-specific transport code — TASK 20 probe to confirm end-to-end |
| Headless | SUPPORTED (shape) | `dsh-headless` bundle, no server |
| Sandbox | UNKNOWN | confinement composed by profile; Windows isolation strength unverified |
| Signal semantics | PARTIAL | Windows force-kill reports exit 1, `signal: null` (no POSIX `killed` stamp); adapter must classify by protocol stop reason (`end_turn`/`cancelled`), never by signal |

## 13. Queue / idempotency implications

- **ACP**: no client-provided request id; `session/prompt` blocks until agent idle with a
  stop reason; fresh-sessions-only means **no resume/query after reconnect**. A SAIWORK2
  crash between upstream accept and the local DISPATCHED commit is **ambiguous** — the
  fresh session dies with the connection. Automatic redispatch is **forbidden** (same
  conservative rule as OpenCode; §133/§137 of TASK 17).
- **SDK**: `session/prompt` returns a durable `messageId` — an enqueue receipt for the
  `UserMessage` only; it does **not** identify a turn, assistant message, or prompt result.
  No query-by-id, no replay of a specific prompt result. Admission correlation is the only
  crash-safe point.
- **No idempotency key** is supported by either seam at the audited commit. Exactly-once is
  not claimable; the TASK 13 ambiguity/reconciliation policy applies unchanged.

## 14. Security / trust

- Config composition is **home-owned** (profiles, bundles, patches in Harness home;
  `--patch` overlay explicit). Project-workspace auto-load of plugins/settings is
  **UNKNOWN** — project trust levels exist (evidence: DeepSeek Codex integration preserves
  "project trust levels"), but the adapter must probe before opening untrusted workspaces,
  and must not auto-enable project plugins.
- ACP seam narrows this: `session/new` rejects non-empty `mcpServers`/`additionalDirectories`
  — no project MCP auto-connect through ACP.
- Plugin/skills trust: skills are instructions/data vs executable code — UNKNOWN at this
  audit; adapter policy: never auto-enable project-defined plugins/MCP servers unless the
  user/upstream trust model explicitly supports it (per TASK 17 §54–§56).
- Environment: adapter passes minimal env (ProcessSpec restrictive mode); Harness credentials
  stay in Harness home; SAIWORK2 must validate machine-protocol trust boundary (trusted
  executable path, no project-controlled executable), workspace paths, and never expose
  arbitrary shell/process authority to the frontend.

## 15. Performance observation (rough probe only)

- Startup: `npx @deepseek-ai/dsh --version` / `--help` exit 0 immediately (probe §16);
  full Web UI is a Node app server. Machine seams add one stdio process per runtime.
- Idle: not measured — the runtime is a Node process tree; **measure in TASK 20 probe**
  (startup, handshake, idle CPU/memory, first session, shutdown) before any release promise.
- Footprint: Node ≥ 22 + npm package (heavy pnpm workspace when built from source; smaller
  via `npx`). Conceptual comparison with OpenCode runtime: same class (managed child
  runtime), exact numbers pending probe.

## 16. Probe (TASK 19, disposable)

| Experiment | Result |
|---|---|
| `npm view @deepseek-ai/dsh version engines license` | version 0.1.0-rc.6; license MIT; no engines field published (docs: Node ≥ 22.19/24+) |
| `npx --yes @deepseek-ai/dsh --version` (fresh temp dir) | exit 0, no stdout; no config/credentials/server created |
| `npx --yes @deepseek-ai/dsh --help` | exit 0, no output (CLI no-ops without a TTY; headless-safe) |
| Real ACP/SDK handshake | **NOT PERFORMED** — requires a harness composition; deferred to the TASK 20 foundation probe |

No credentials, no workspace access, no install beyond the npm cache in the disposable
temp dir. This probe does not prove the machine seams; TASK 20 owns that.

## 17. Known risks / deferred capabilities

- **Protocol instability**: explicit compatibility-breaking changes; SDK wire 0.0.1 with no
  negotiation. Adapter must version-pin and reject-on-mismatch.
- **SDK `session.event` is unfiltered full session-log envelopes** — volume + privacy
  exposure; adapter boundary must filter/redact, and SAIWORK2's bounded event/UI batching
  must not be bypassed (TASK 21).
- **ACP committed-answers-only**: no token-level streaming UX; SAIWORK2 renders committed
  chunks (same batching path), never fakes deltas.
- **Fresh-sessions-only + connection-owned** ACP sessions: `session_resume = false`;
  SAIWORK2 session list for Harness reflects live engine sessions only.
- **Windows sandbox unknown**; **project trust auto-load unverified**; **skills
  executable-vs-data unknown** — all probe gates for TASK 20.
- **Node runtime dependency**: SAIWORK2 gains a managed Node child runtime for this engine
  (ProcessSupervisor-owned). Protocol implemented in Rust over stdio — **no Node sidecar**
  layer for the seam itself.
- **Deferred capabilities** (adapter-internal or not advertised): subagents (observable via
  `subagent.*` notifications, not first-class generic runs), workflows/goals/jobs (third
  task authority **forbidden** — SAIPEN + QueueManager remain the only authorities),
  MCP/LSP/skills (adapter-internal unless a generic need appears), compaction (engine-local).

## 18. SAIWORK2 donor matrix

| Idea | Harness implementation | SAIWORK2 current | Action |
|---|---|---|---|
| Capability seams | Service Definition/Provider/Consumer | EngineAdapter trait + capability flags (TASK 17) | ALREADY EQUIVALENT |
| Reversible effects | Cordis register→unload unwind | symmetric start/stop; no enforcement utility | ADAPT LATER (TASK 22: cleanup-symmetry audit utility) |
| Durable vs live events | SessionEvent (durable) vs `agent/*`/`llm/stream` (live) | queue/SAIPEN durable; message.delta live; classification informal | ADAPT LATER (TASK 22: formal DURABLE vs LIVE classification; EventBus stays live-only) |
| Turn/step model | turn = 0+ steps; step = request+tools | generic Run = one send→terminal | REJECT for V1 (no demonstrated need; OpenCode/FakeEngine don't model steps) |
| Subprocess boundary | harness owns inner subprocesses | ProcessSupervisor owns top-level engine processes | ADOPT as documented rule (no double supervision) |
| Sandbox boundary | `ctx.sandbox` argv-wrap | outer workspace gate only (TASK 18) | REJECT for V1 (engine-local; no SAIWORK2 sandbox) |
| Permission flow | one-shot approval requests | `resolve_permission` Allow/Deny | ALREADY EQUIVALENT (ACP maps directly) |
| Context compaction | session-log compaction, engine-local | none (engine conversations not mirrored) | NOT RELEVANT to core; SAIPEN-RELEVANT? no |
| Subagents | `ctx.subagent`, `subagent.*` events | no subagent concept | DEFERRED (TASK 18 concurrency model can represent parent/child later without modification) |
| Workflows/goals/jobs | `ctx.goals`, `ctx.jobs`, workflows | SAIPEN tasks + QueueManager | REJECT (third task authority forbidden) |
| Profile composition | bundles + patches, home-owned | per-engine config via settings | DEFERRED (Harness owns its profile; SAIWORK2 passes profile name only) |
| Plugin lifecycle | dynamic plugin tree | static compile-time adapters | REJECT (no dynamic plugin loading; TASK 17 §86) |

## 19. SAIPEN donor matrix

| Idea | Action |
|---|---|
| Durable facts vs live effects | ALREADY EQUIVALENT — SAIPEN is the canonical file-only protocol; SAIWORK2 never mirrors it |
| Explicit capability boundaries | REJECT — SAIPEN stays minimal; no seam framework |
| Reversible effects / operation identity | ALREADY EQUIVALENT — canonical action protocol with operation identity |
| Event reconstruction / workflow identity / subagents | REJECT — would turn SAIPEN into a Harness clone |

## 20. Next-task contracts

### TASK 20 — DeepSeek Harness Engine Adapter Foundation
- Crate `crates/engine-deepseek-harness` (prototype-grade, behind adapter firewall).
- **Probe gate**: install/run current Harness per official instructions in a disposable
  temp profile; verify ACP handshake, version negotiation, one fresh session, prompt→idle,
  cancel, shutdown, Windows end-to-end; verify project-trust behavior (does opening an
  untrusted workspace auto-load project config?); measure startup/idle/shutdown + process
  tree. Record REAL vs BLOCKED EXTERNAL honestly.
- ProcessSupervisor ownership of the top-level runtime; typed ProcessSpec (no shell),
  bounded stdio, taskkill-tree fallback; restrictive env.
- Protocol transport: NDJSON JSON-RPC 2.0 framing over stdio (shared line transport for
  ACP and SDK seams), malformed-line tolerance, request correlation, error mapping
  (`-32601`/`-32603`), disconnect detection.
- Version compatibility: probe `serverInfo`/ACP version; record tested version; reject
  known-incompatible; tolerate additive fields.
- Capabilities (declared after the probe proves them — no fake parity): lifecycle,
  sessions, streaming=committed-chunks, cancel, permissions; `session_resume=false`,
  `parallel_sessions` per probe.
- Errors: map to existing categories (Disconnected, Protocol, Cancelled, Unsupported,
  Server/Internal, Timeout); no new taxonomy without cross-engine need.
- Tests: deterministic fake ACP server fixtures (handshake/session/prompt/cancel/error/
  disconnect/malformed/version-mismatch), real-runtime smoke where probe permits.
- No sessions/tools vertical slice beyond minimum handshake.

### TASK 21 — Harness Agent Vertical Slice
- Session create/resume (per probe), prompt, committed-chunk streaming → normalized
  `message.completed` events through the existing bridge batching, tool events
  (observability-level), permission request → generic `resolve_permission` round-trip,
  run cancellation scoped to RunId, terminal states (end_turn/cancelled/error), normalized
  events, UI proof in the conversation view with engine ownership labeling. Regression:
  OpenCode + FakeEngine + Generic CLI unaffected; contract parity suite extended.

### TASK 22 — Capability / Runtime Architecture Improvements
- Only donor ideas that fix a real SAIWORK2 defect: (a) reversible-registration cleanup
  utility (register→unregister symmetry enforcement); (b) formal DURABLE vs LIVE event
  classification doc (EventBus stays live-only; no event-sourcing); (c) refined capability
  model only if the Harness adapter exposes a genuine gap. No framework transplantation,
  no plugin loading.

### TASK 23 — Harness + Queue + SAIPEN Hardening
- Queue targeting EngineId=deepseek-harness; ambiguous-dispatch policy per audited seam
  (no idempotency key → conservative DISPATCHED reconciliation, no redispatch);
  session/workspace exclusions per TASK 18; SAIPEN untouched (no canonical writes, no
  third task authority).

### TASK 24 — Post-V1 Multi-Engine Hardening
- Cross-engine hostile suite with Harness: ID collisions, stale generation, engine-switch
  races, failure isolation, queue routing, shutdown; resource cleanliness (Node runtime
  teardown, port/process baseline); idle-CPU proof; packaged Windows smoke with the
  Harness runtime.

## 21. Rules carried from prior tasks (apply unchanged)

- One authority per domain fact; Harness is never authoritative for SAIWORK2 state.
- No automatic provider fallback ("Harness unavailable → use OpenCode" is forbidden).
- Capabilities are facts; unsupported → `false`, UI disables.
- Secrets never logged; bounded output; unknown protocol events cannot crash the app;
  stale runtime events cannot cross generations (engine-generation guard).
- No arbitrary shell execution; no silent install/auto-update of Harness (explicit path /
  PATH discovery / managed-bundled install only after a separate decision, per §30).

## 22. TASK 20 implemented truth — engine-deepseek-harness foundation

### Adapter crate
`crates/engine-deepseek-harness` — `EngineId = "deepseek-harness"`, display
"DeepSeek Harness", registered through the existing `EngineRegistry` (desktop shell)
**only when explicitly configured** via `SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE` (same
configured-only rule as Generic CLI; malformed values surface a precise config error and
the engine is not registered). Absent = not registered; no npm/global archaeology.

### Transport / protocol
- **Seam: ACP over stdio** (newline-delimited JSON-RPC 2.0), exactly per TASK 19 §4–§5.
- `transport.rs`: one reader task per runtime generation; NDJSON framing over the
  supervisor's raw protocol pipe; unique request ids from one correlation authority;
  pending-request registry with per-request deadlines; responses/notifications/
  server-requests routed by `id`+`method`; unknown notifications ignored safely;
  duplicate/unknown response ids never resolve a request twice; malformed/oversized
  frames kill the transport deterministically (fail-safe reset); `biased` select so a
  received response wins over concurrent death. Frame cap 1 MiB both directions.
- `protocol.rs`: ACP wire DTOs **adapter-local only** (InitializeParams/Result,
  camelCase serde renames). No Harness/ACP type escapes the crate (firewall §7).
- Handshake: ACP `initialize` with clientInfo `saiwork2`/crate version; `serverInfo`
  required (missing/empty name = typed HandshakeRejected); protocol version recorded;
  newer/unknown versions accepted (compatibility proven by the handshake, §13–§14).
  READY requires process alive + transport + accepted handshake — never PID alone.

### Lifecycle / process ownership
- Top-level Harness runtime spawned through ProcessSupervisor (typed `ProcessSpec`:
  `StdinPolicy::Piped`, `stdout_protocol`, bounded stderr, explicit cwd/env, graceful→
  force stop). Harness-internal tool/subprocess lifecycle stays Harness-owned — no
  double supervision (PROCESS_LIFECYCLE.md).
- `Unknown → Starting → Ready`; stop → `Stopped` (idempotent); unexpected death →
  `Failed` + `engine.failed` event (never silent); explicit restart heals with a fresh
  generation. Stop-during-start cancels via a watch signal (no late READY, no orphan).
  Protocol loss with process alive removes READY immediately (§57–§58). No automatic
  reconnect (§59). Stop = protocol stdin EOF → supervisor graceful → force escalation.
- **Generic `saiwork-process` extension (TASK 20 §21/§75/§78):** `StdinPolicy::Piped`
  (long-lived interactive protocol child; serialized `stdin_write_all`, `stdin_close`)
  and `ProcessSpec::stdout_protocol` (raw byte chunks to a bounded channel with real
  backpressure + lossy diagnostics ring). Defaults unchanged — OpenCode/FakeEngine/
  Generic CLI behavior untouched (proven by regression).

### Capabilities (foundation only, §40)
All `false`: sessions/resume/streaming/cancel/tools/permissions/models/etc. The UI
shows the engine as experimental/runtime-foundation; no session workflow is offered;
QueueManager cannot dispatch to it (§113). TASK 21 enables sessions/stream/… only
after the vertical slice proves them.

### Discovery / probe / errors
- Discovery precedence: explicit path (authoritative, invalid → typed config error,
  no silent fallback) → PATH lookup of `dsh`/`dsh.cmd`/`dsh.exe`. Pre-launch probe:
  bounded `--version` run through the supervisor; authoritative identity/version is
  the handshake.
- Typed errors (`HarnessError`): HarnessNotFound, ProbeFailed, ConfigurationInvalid,
  SpawnFailed, ExitedDuringStartup, TransportClosed, HandshakeTimeout/Rejected,
  MalformedFrame, MessageTooLarge, RequestTimeout/Rejected, RuntimeLost, Canceled
  (→ canonical `EngineError::Canceled`), StartupCleanupFailed,
  PreviousRuntimeTerminationUnproven, Unsupported, Internal — user-safe messages,
  no raw payloads (§71–§72).

### Hostile matrix (30 tests, 3× stable, real stdio fixture)
`tests/hostile.rs` + `src/bin/fake-harness.rs` (deterministic fake ACP server; scenario
via argv for parallel-test safety; `--version` probe support): normal handshake;
delayed; fragmented (1-byte writes); unknown notification; duplicate response; unknown
response id; server-request answered -32601; metadata request + operation-local
timeout; protocol flood; stderr flood; handshake hang → timeout + kill; reject;
exit-before/after-handshake; malformed frame; oversized frame; partial-frame EOF;
stop-during-start (cancel, no late READY, no orphan); direct start-task abort during
handshake (RAII cleanup, no process/transport orphan); stop-during-request (pending
settles); ignored shutdown → force termination; stop/start idempotence; restart
generation; 25× lifecycle baseline; discovery/typed errors; newer-version acceptance;
capability honesty; idle = zero work; registry isolation (FakeEngine unaffected).

### Real Harness smoke (TASK 20)
**BLOCKED UPSTREAM** (recorded honestly, not fabricated): the published
`@deepseek-ai/dsh` 0.1.0-rc.6 CLI exposes only `web`/`headless` profiles (TTY-gated;
`--profile headless --dump-config` exits 0 silently); there is **no `acp` subcommand**.
`@deepseek-ai/dsh-acp` **0.0.1-rc.1 is published** (BSD-3-Clause; built on
`@agentclientprotocol/sdk` 0.25.1) but composing a runnable ACP profile requires the
full Cordis plugin tree (peer deps: dsh-invariants, dsh-user-approval, …) plus
provider/model configuration — a source-checkout/pnpm-workspace install. The concrete
profile composition + real Windows ACP handshake is the **TASK 21 probe gate** (the
fixture matrix proves the protocol/lifecycle deterministically in the meantime).
No provider credential, no model call, no danger-full-access profile was used.

### TASK 20 scope check
No sessions/prompts/tools/permissions; no QueueManager dispatch; no SAIPEN handoff;
no subagents; no MCP/LSP UI; no plugin installation; no automatic Harness
install/update; no second engine registry; no Harness DTO outside the adapter crate.

---

## 23. TASK 21 implemented truth — Harness agent vertical slice

### Re-verified protocol contract (2026-08-17)
The ACP v1 (protocol version `2025-03-26`) machine contract was re-verified against the
`@agentclientprotocol` SDK schema and the agentclientprotocol.com v1 protocol docs before
implementing. Methods/events depended on:

| Method / event | Wire shape | Used for |
|---|---|---|
| `initialize` | `InitializeResult { protocolVersion, serverInfo, capabilities }` | handshake (TASK 20) |
| `session/new` | `NewSessionParams { cwd, additionalDirectories, mcpServers }` → `{ sessionId }` | authoritative session create (§8) |
| `session/prompt` | `PromptParams { sessionId, prompt: [{ type: "text", text }] }` → `{ stopReason }` | turn submission + authoritative terminal (§25, §67) |
| `session/update` (notification) | `{ sessionId, update }`; `sessionUpdate: "agent_message_chunk"` (content `{ type: "text", text }`, optional `messageId`) and `"tool_call"` (`{ toolCallId, name, status, rawInput, content }`) | committed-chunk stream + tool lifecycle (§31, §48) |
| `session/request_permission` (server request) | `{ sessionId, toolCall, options[] }` → `{ decision: "allow"\|"reject", optionId }` | permission round-trip (§55) |
| `session/cancel` (notification) | `{ sessionId }` | run cancellation (§63) |
| `session/delete` | `{ sessionId }` | best-effort delete (tolerates -32601) |

No durable `TurnId`/`StepId` exists on the ACP wire: `session/prompt` blocks until the
agent is idle and returns a stop reason; the prompt response IS the authoritative turn
result. Turn/step identity therefore stays adapter-internal (§16–§17).

### Adapter modules (engine-deepseek-harness)
- `runs.rs` — `RunRegistry`/`RunRecord`: one owner of RunId ↔ HarnessSessionId
  correlation, cancel signal, terminal CAS, terminal watch (permission fail-closed),
  terminal-tool set, prompt-task handle (§28).
- `sessions.rs` — `SessionRegistry`: SAIWORK2 id ↔ opaque Harness session id; cleared on
  teardown (sessions are connection-owned, §75).
- `permissions.rs` — `PermissionRegistry`: pending requests keyed by SAIWORK2 request id
  with a decision oneshot; `take` makes resolve idempotent (§58–§60).
- `events.rs` — `EventRouter` (session/update → message.*/tool.*) + `permission_handler`
  task + `emit_terminal` (exactly-one-terminal CAS) + `outcome_from_stop_reason` (§32–§41).

### Session authority
- Harness ACP sessions are **fresh + connection-owned**: created via authoritative
  `session/new`; no list/load/resume on the baseline wire (`session_resume = false`).
  `list_sessions` reflects only the adapter-created live sessions for this runtime;
  never filesystem scanning (§9). After a runtime restart the registry is empty and a
  stale id sends fail `SessionNotFound` — honest, never a fabricated reconstruction (§10,
  §75). No SQLite transcript mirror exists (§6). SAIWORK2 persists only session metadata
  through the existing SessionManager.
- **Prompt acceptance boundary (§25):** the prompt task emits `message.started` at
  dispatch; the first routed `session/update` for that session is authoritative
  acceptance evidence (CAS-guarded); the `session/prompt` response stop reason is the
  authoritative terminal. No auto-retry on ambiguous transport failure (§26, §128–§129):
  transport loss mid-turn fails the run with an honest outcome — never a duplicate prompt.

### Run / turn model
- One `send` → one generic RunId ↔ one `session/prompt` (one upstream turn). Same-session
  concurrency is **REJECT** (`SessionBusy`) — one in-flight prompt per ACP session (§80–§81);
  different sessions run independently (`parallel_sessions = true`).
- Exactly one terminal per run (CAS-gated): `MessageCompleted` (stop reason `end_turn`),
  `MessageCancelled` (`cancelled`/`discarded`), `MessageFailed` (anything else, incl.
  model/provider error, timeout, runtime loss). A racing cancel that loses to a normal
  finish reports `completed` — the authoritative stop reason wins (§67).
- Steps: Harness's step model has no generic equivalent in OpenCode/FakeEngine, so it
  stays **adapter-internal** — no generic StepId was added (§17–§19). A multi-step turn
  maps to one RunId; tool correlation survives step transitions via stable ToolCallId.

### Session log vs live notifications (§39–§40)
- `session/update` `agent_message_chunk` = **live committed-chunk** stream → incremental
  `message.delta` (one canonical MessageId per upstream message; never one message per
  chunk, §31/§35). Durable session-log facts (turn/step/session-log) are **not on the ACP
  wire** and are never fabricated or mirrored (§6). Unknown update kinds are ignored — not
  every Harness internal fact becomes a public event (§97).
- Duplicate chunks have no per-chunk identity on the wire → appended (documented
  limitation, no dangerous text-dedup heuristics, §37/§119). Events after a terminal are
  discarded (terminal stays terminal, §121). Events for sessions without an active run
  (external activity) are ignored and never cross-route (§122–§124).

### Tools
- `tool_call` updates → generic `tool.started` / `tool.output` / `tool.completed` /
  `tool.failed`, keyed by the generic tool name; ToolCallId stays adapter-internal for the
  exactly-one-terminal rule (§52). Output is bounded (32 KiB) and input summary bounded
  (500 chars) — never raw giant JSON, never secret-bearing dumps (§50–§51, §62). A tool
  failure does not fail the run — the run follows the upstream turn result (§53).

### Permissions
- `session/request_permission` → generic `permission.requested` (bounded safe detail) →
  `resolve_permission` Allow/Deny → adapter answers upstream with the matching option
  (`allow_once`/`reject_once` — only actual supported decisions are used, §56) →
  authoritative `permission.resolved`. Unknown/already-resolved/stale requests are
  idempotent no-ops (§58–§60). **Fail-closed:** a pending request with no decision
  (UI disconnect, shutdown, engine failure, run terminal) resolves reject — never
  default allow (§57). Engine stop clears pending permissions and settles runs (§71–§73).

### Cancellation
- `cancel(run_id)` marks the run cancel-requested (single CAS owner) and signals the
  prompt task, which sends `session/cancel` exactly once (§64–§65). Cancel-before-dispatch
  never sends the prompt — emits cancelled directly (§114). Cancel twice / after terminal
  are no-ops (§66). The prompt task's terminal is authoritative (race-safe, §67–§68).
  Normal cancel never kills the Harness runtime (§63). Cancel during a pending permission
  settles the turn and the permission handler fail-closes (§70).

### Provider / model
- The ACP baseline advertises **no machine-facing provider/model discovery** → `models =
  false`, `list_models` returns `UnsupportedCapability`. Model selection delegates to the
  Harness profile default (`UseEngineDefault`, §23); an explicit model is an honest
  unsupported error, never a silent fallback (§84). No hardcoded model list (§22).

### Resync / recovery
- Frontend remount/session-switch: backend runs continue; events are routed by stable
  upstream session id, never by selection, so a remount cannot duplicate or cross-route
  (§33–§34, §77–§78). Transport loss / runtime crash fail active runs (never eternal
  RUNNING) and fail the engine; a restart yields a fresh generation and empty session
  registry (§71–§75). No full session-log polling exists (there is nothing to poll on the
  ACP baseline, §44).

### Capabilities now advertised (TASK 21 §145–§146)
`streaming`, `sessions`, `cancel`, `tools`, `permissions`, `structured_events`,
`parallel_sessions` = **true** (all fixture-proven). `resume`, `models`, `attachments`,
`images`, `usage`, `reasoning`, `worktrees` = **false**. QueueManager dispatch remains
**disabled** for this engine (TASK 23).

### Generic contract changes (cross-engine, proven)
- `EngineIdentity.experimental: bool` added (saiwork-core + frontend contract).
  DeepSeek Harness = `true` (Developer Preview — UI marks it ⚠ and never hides
  instability, §88); OpenCode/FakeEngine/Generic CLI = `false`. No engine-specific
  branching in generic conversation/session/run/tool/permission UI.
- Harness adapter deliberately does **not** double-publish `session.created` (the OpenCode
  adapter's adapter+SessionManager double-publish is a latent pre-existing behavior, not
  replicated).

### Features still deferred
QueueManager dispatch (TASK 23); SAIPEN handoff; subagents; workflows/goals/jobs; MCP/LSP/
skills management UI; plugin management; custom sandbox implementation; SQLite transcript
mirror; credential vault; dynamic plugin loading; worktrees; broad parallel scheduler.

### Real Harness Windows E2E (TASK 21 §134–§139)
**BLOCKED EXTERNAL** (recorded honestly): composing a runnable ACP profile still requires
installing the full Cordis plugin tree plus provider/model configuration (see TASK 20
real-smoke note). The deterministic fixture matrix (`tests/vertical.rs`, 28 tests) proves
the complete agent workflow — session create, prompt, streaming, tools, permissions,
cancel/races, transport loss, restart — over a real stdio process through the
ProcessSupervisor. No provider credential, no model call, no danger-full-access profile
was used. REAL INFERENCE = BLOCKED EXTERNAL.

### Vertical slice tests (`tests/vertical.rs`, 28)
normal turn; multi-step; tool lifecycle; tool-failure-continues; permission allow/deny/
no-response (fail-closed); cancel before first chunk / mid-chunk / race / twice /
after-complete; provider failure (engine stays ready); runtime crash; transport loss;
accepted-then-response-lost (outcome unknown, no retry); duplicate chunk; wrong-session
event isolation; session busy; second turn after terminal; restart connection-ownership;
engine stop settles runs; large 10k-chunk stream; generic SessionManager flow + permission
round-trip (cross-engine parity). Plus the 29-test hostile foundation matrix unchanged.

### TASK 21 scope check
No QueueManager dispatch; no SAIPEN handoff; no subagents; no workflows/goals/jobs; no
MCP/LSP/skills management UI; no plugin management; no transcript SQLite mirror; no
credential vault; no dynamic plugin loading; no custom sandbox; no worktrees; no broad
parallel scheduler. OpenCode/FakeEngine/Generic CLI generic paths unchanged (regression
proven).

### HARNESS-DERIVED IMPROVEMENT CANDIDATES (TASK 22 §189 input)

Each candidate is the observed SAIWORK2 problem, the Harness idea, the proposed
adaptation, the affected engines, benefit/risk, and the TASK 22 classification
(recorded in DECISIONS.md ADR-042):

| Candidate | Observed problem | Harness idea | Proposed adaptation | Affected engines | Classification |
|---|---|---|---|---|---|
| Reversible effect ownership | Harness `Runtime` and OpenCode `Runtime` both hand-roll cancel-signal + JoinSet teardown (abort_all + join loop) | Cordis reversible registration | Narrow `TaskScope` owning JoinSet + cancel watch with deterministic shutdown | Harness, OpenCode | **ALREADY SOLVED** — Rust ownership (Runtime owned by slot, tasks in JoinSet, `take()` idempotence) already makes cleanup clear and tested; a new abstraction would be naming symmetry (§8) |
| Capability model | Flat boolean `EngineCapabilities`; Harness `models=false`/`resume=false` are static truths, `parallel_sessions=true` | Cordis capability seams | Split static/runtime/current capability dimensions | Harness, OpenCode, Fake | **ALREADY SOLVED** — no adapter has runtime-varying capabilities; the flat truthful model + `experimental` identity flag suffice; no contradiction found in audit |
| Durable vs live classification | Harness forced explicit durable-session-fact vs live-notification classification (§39–§40) | SessionEvent (durable) vs `agent/*` (live) | Document per-event durable/reconstruct/droppable/terminal semantics in EVENTS.md | all engines | **ADOPT NOW** — documentation + `EventClass` doc enrichment (EVENTS.md "Semantic classification") |
| Operation correlation | Adapters each carry run_id/session_id/generation | — | Generic `RunContext` struct | Harness, OpenCode | **ALREADY SOLVED** — normalized events already carry session_id + run_id; a non-owning context struct adds nothing |
| Turn/step model | Harness Steps have no OpenCode/Fake equivalent | turn=0+steps | Generic StepId | Harness | **REJECT** — stays adapter-local (TASK 21 §17–§19); no cross-engine evidence |
| Tool-cycle grouping | No UI confusion observed | — | optional phase/group metadata | — | **DEFER** — trigger: a second engine exposing multi-tool-cycles-per-run that the UI flattens confusingly |
| Process capability seams | ProcessSupervisor already owns top-level runtimes; no adapter bypasses it | `ctx.subprocess` | `ManagedCommandRunner` | — | **ALREADY SOLVED** — no duplication found; no-double-supervision holds (§54) |
| Filesystem capability | No second remote-FS use case | `ctx.fs` | generic `RemoteFilesystem` | — | **REJECT** — premature abstraction |
| Profile/config composition | Config is simple (env + defaults) | profile/bundle layering | precedence helper | — | **REJECT** — no duplicated priority rules |
| Fail-closed permissions | TASK 21 implemented fail-closed + idempotent resolve in the Harness adapter | one-shot approvals | generic typed decision richer than bool | all engines | **ALREADY SOLVED** — generic bool allow/reject + adapter fail-closed suffices; no engine needs richer decisions |
| Test contract harness | Harness/OpenCode suites duplicate helper patterns but differ in protocol + fixture | — | reusable generic EngineAdapter contract suite | Harness, OpenCode, Fake | **DEFER** — risks vendor special-casing (P1); trigger: a 4th engine or demonstrated contract drift |
| Error model refinement | No brittle string matching in UI (all typed codes) | — | recovery-hint enum | — | **ALREADY SOLVED** — UI displays typed errors; no `includes("429")` patterns |
| Engine runtime scope | One adapter instance = one runtime; generation already exists | — | `EngineRuntimeId` | — | **ALREADY SOLVED** — EngineId + generation cover it |
| Workspace trust | TASK 21 §140 repeated the concern; no SAIWORK2-owned trust metadata exists yet | project trust levels | app-owned trust metadata + migration | Harness, future plugins | **DEFER** — trigger: a concrete code-bearing workspace-config path SAIWORK2 must gate |
| Static capability registration | Explicit `register` calls in the desktop shell | everything-is-a-plugin | `EngineDescriptor` factory | all | **ALREADY SOLVED** — static registry is clear; no scattered match statements |

TASK 22 outcome: **1 ADOPT (event semantic classification docs), 1 real cleanup bug fixed
(partial-initialization leak in Harness `start()`, §17/§147), 1 UI leak removed
(TitleBar tooltip), everything else ALREADY SOLVED / DEFER / REJECT.** No dynamic
plugin/service-locator/effect-framework architecture was introduced.

---

## 24. TASK 23 implemented truth — Harness as a durable QueueManager target

### Harness queue capability
DeepSeek Harness is now a proven durable queue target through the **generic**
`EngineAdapter` → `SessionManager` → `EnginePort` → `QueueManager` path. The queue
never knows ACP/Harness protocol details (statically audited: no `deepseek`/`harness`/
`acp`/`TurnId` in `saiwork-queue`); the Harness adapter never writes the queue DB
(direct SQLite writes in the adapter = zero). Enabling was not a flag flip — it is the
generic path, proven by `tests/queue_slice.rs` (real production wiring over the real
fixture-backed adapter): enqueue → claim → durable sending phase → Harness `send` → run
→ `message.completed` → durable DONE; queue cancel → Harness `session/cancel` →
CANCELLED (engine stays READY); provider failure → FAILED (engine stays READY); engine
crash → FAILED, no auto-requeue. No engine fallback exists.

### Acceptance boundary / idempotency / correlation (TASK 23 §10–§17, §32–§34)
- **Acceptance evidence:** the `session/prompt` send returning a SAIWORK2 run handle is
the authoritative acceptance point (the prompt task is the terminal authority; the stop
reason maps to exactly one terminal — TASK 21 §24–§25). A transport write alone is not
acceptance (TASK 23 §11).
- **Idempotency:** ACP exposes **no client-supplied operation id and no durable TurnId**
— the Harness session id is the correlation unit. `session_id` + `run_id` are persisted
on the queue row; there is nothing more to persist (no `harness_turn_id` column, no
unstructured blob). Exactly-once external effect is NOT claimable across the crash
boundary — the conservative `UNKNOWN` fallback applies (TASK 23 §17, §137).
- **Across an app restart:** ACP sessions are connection-owned, so a dispatched run's
outcome is unrecoverable → the item is honestly marked `UNKNOWN` (never resend, never
presented as a live run). This is the documented ACP limitation, not a defect.

### OUTCOME_UNKNOWN (TASK 23 §17–§21, §50–§51)
A first-class durable `QueueState::Unknown`: crash during `sending` handoff or a
DISPATCHED item at restart → `unknown` (code `dispatch_unknown`). Never auto-dispatched;
**blocks further mutating queued dispatch in its workspace** (the unknown old run may
have mutated files); other workspaces proceed. Resolved only by explicit user action:
`retry` (new attempt, UI acknowledges duplication risk), `cancel` (abandon — never
claims the work did not run), or an externally discovered terminal. Old-attempt
terminals can never complete a newer attempt (run_id-guarded transition).

### SAIPEN → Queue (TASK 23 §73–§84)
**DEFERRED.** The canonical SAIPEN contract in this baseline does not expose a mutating
`continue` command (`ActionKind::Unsupported`) and has no stable execution/transition
identity to use as an idempotency key — automatic handoff cannot be proven exactly-once.
A future handoff must flow `canonical SAIPEN action → durable QueueManager enqueue` with
a stable source id as the idempotency key; never a direct bypass (ADR-043).

---

## 25. Final release status (TASK 24/24)

| Item | Value |
|---|---|
| Support level | **EXPERIMENTAL** (upstream Developer Preview — README: *"THERE WILL BE COMPATIBILITY-BREAKING CHANGES"*). Local adapter suites are green, but this does not make the upstream runtime stable. |
| Tested version | upstream `master` tree `47f9438` / `@deepseek-ai/dsh` 0.1.0-rc.6, ACP protocol `2025-03-26` (fixture-verified) |
| Windows | Adapter targets Windows (stdio ACP, ProcessSupervisor-owned); real-handshake smoke remains **BLOCKED EXTERNAL** (no dsh tree + provider config in this environment; the deterministic fixture matrix proves the workflow). |
| Sessions | Fresh ACP sessions, connection-owned; `resume` unsupported (never fabricated). |
| Queue | **ENABLED** through the generic path (ADR-043) — dispatch, cancel, provider failure, crash all durable; OUTCOME_UNKNOWN + workspace blocking. |
| Permissions | fail-closed, idempotent resolve, runtime-loss clears pending. |
| Cancel | Harness `session/cancel`, never an engine kill. |
| Sandbox truth | Harness sandbox is upstream-owned (Cordis/plugin tree); SAIWORK2 does not claim a safety property it does not enforce. |
| Known limitations | no durable TurnId / no idempotency key → exactly-once external effect not claimable across a crash; ACP sessions do not survive app restart → honest UNKNOWN; real provider smoke blocked externally. |
| Multi-engine | TASK 24 hostile matrix (`tests/multi_engine.rs`) proves session/run isolation, exact queue routing, no-fallback, same-workspace cross-engine serialization, and the fail-closed session-id collision guard (ADR-044). |
