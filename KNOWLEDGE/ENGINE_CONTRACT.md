# ENGINE_CONTRACT.md

One logical contract for every engine integration. Implementations differ,
the contract does not.

## Logical surface

```text
identity()
probe()
capabilities()

start(workspace)
ready()
health()
stop()
kill()

listModels()

listSessions()
createSession()
resumeSession()
deleteSession()
sessionHistory()
revertSession(messageId)
unrevertSession()

send()
cancel()
resolvePermission()
resolveQuestion()

pushRaw()          (test/harness boundary only: malformed/duplicate/out-of-order input)

subscribeEvents()
dispose()

subscribeEvents()

dispose()
```

Rust notes: the logical contract is expressed as `EngineAdapter` in
`saiwork-core::engine`. Do not turn it into an abstract-factory-builder-factory
hierarchy (law: do not over-abstract). A plain `async_trait`-style trait with
data types is enough until a second implementation proves the boundary.

## Capability normalization

Capabilities are a fixed set of booleans; UI builds on capabilities, never on
engine identity. The vocabulary is canonical from the master spec §9:

```text
streaming
sessions
resume
cancel
tools
permissions
attachments
images
models
usage
reasoning
context_window
worktrees
parallel_sessions
session_revert
structured_events
```

### Required vs optional

- **Required** (every engine must declare truthfully, may be false):
  `streaming`, `cancel`, `structured_events`. A non-streaming engine still
  completes a run with a terminal event; `cancel` may be a no-op only when
  the engine declares it false.
- **Optional** (advertise only what exists): everything else.
- **Unsupported behavior** is expressed as a false capability, never as a
  hidden fallback or engine-id special case in UI/core.
- **Capability discovery**: `capabilities()` is a fixed-shape record;
  unknown fields default to false. UI renders capabilities as flags, never
  by comparing engine ids (see Forbidden below).
- **Session revert**: `session_revert=true` means the adapter implements both
  revert-to-message and unrevert. The generic core chooses the last visible
  user boundary from authoritative `sessionHistory()`; UI never invents an
  upstream message id. Revert/delete take the same workspace-exclusive,
  cancellation-safe maintenance reservation as a run.

## Engine state model (never collapse into one boolean)

Five distinct state axes exist; they must not be conflated:

```text
process state      SPAWNING | RUNNING | STOPPING | EXITED | FAILED
                   (ProcessSupervisor only — the OS process state machine;
                   process alive ≠ engine ready, see ADR-015)
engine readiness   unknown | starting | ready | degraded | stopped | failed
                   (health probe outcome, published as engine.*)
connection state   connected | disconnected | reconnecting (transport level)
session state      created | loaded | active | closed (session lifecycle)
run state          created | running | cancelling | completed | failed | cancelled
                   (per run; exactly one terminal outcome)
```

Invariants:

```text
PID exists != engine ready
socket connected != session healthy
prompt sent != run completed
run completed != work persisted
```

`RUNNING` in the process state machine refers to a supervised process doing
work; the run-level activity is tracked per session/run via `message.*`
events. The UI derives "something is running" from run state, not from the
process state.

## Run lifecycle (per `send()`)

```text
send
  → run created (RunId allocated by the engine adapter)
  → message.started
  → message.delta*        (streaming; zero or more)
  → exactly one terminal: message.completed | message.failed | message.cancelled
```

- Exactly **one** terminal event per run (asserted by tests, §61). A terminal
  run never becomes RUNNING again; no semantic deltas/tool/permission events
  are emitted for a run after its terminal event (§62).
- Cancellation: `cancel(run_id)` → run emits `message.cancelled`; duplicate
  cancels, cancel-after-terminal and cancel of an unknown run are no-ops.
  The final-delta-vs-cancel race resolves to exactly one terminal outcome
  (ordering test in `engine-fake`).
- Run failure ≠ engine failure ≠ session failure: a failed run leaves the
  engine healthy and the session open (tests in `engine-fake`); an engine
  crash resolves all active runs to a terminal failure/cancelled state.

## Same-session concurrency

Policy: concurrent `sends` **in the same session are REJECTED** (TASK 11
§70–§72, TASK 18 §11): the adapter's run registry refuses a second send to a
session with an active run (`SessionBusy`), because two agent turns in one
thread are concurrent-but-nonsensical. The OpenCode adapter enforces it in
`RunRegistry::insert`; the Generic CLI adapter enforces it in `send` (TASK
18); FakeEngine models the boundary for tests. **Different sessions** are
independent — distinct `RunId`s, independent terminals, per-run cancel
(TASK 18 Level D/C, proven by the `parallelism` suite).

## Workspace concurrency (TASK 18 §21–§22, ADR-038)

Without worktrees, one mutating agent run per physical workspace is the
correctness boundary. `SessionManager.send` rejects a send to another
session in a workspace that already has an active run with the typed
`CoreError::WorkspaceBusy`; the queue-facing `EnginePort::session_busy`
returns busy for the same-workspace case (the queue WAITs, never claims
then fails), and New-mode dispatch checks busy before the send. Different
workspaces run concurrently. Same-session REJECT remains the engine's own
contract; workspace serialization is SAIWORK2-level and applies across all
engines.

## Permission flow

`send()` may publish `permission.requested` and await resolution;
`resolvePermission(request_id, allowed)` releases the pending wait. Engine
stop/dispose releases all pending permission waits (they resolve as denied /
run cancelled) — a pending permission never blocks shutdown forever (§26).

UI routing (TASK 16): `SessionManager::resolve_permission` (resolves the
session's engine) → Tauri `resolve_permission` command → frontend Allow/Deny
buttons. The engine's `permission.resolved` event is the authoritative
outcome; the UI never fabricates it. Engines declaring `permissions = false`
never need the command.

## Forbidden

```ts
if (engine === "opencode") ...
if (engine === "freebuff") ...
if (engine === "antigravity") ...
```

in the generic UI layer. Engine-specific UI lives only inside an isolated
engine feature boundary (and only if the capability set cannot express it).

## Normalization rules

1. Engine emits provider-specific events → adapter maps them to canonical
   events (EVENTS.md) **at the boundary**.
2. Raw provider payloads never cross the boundary except as `engine.raw_event`,
   which is debug-only and bounded (law 13).
3. Errors are typed (see ERROR MODEL below); an adapter never leaks an
   unclassified string as the primary user-visible message.
 4. Model lists are normalized to `{ id, displayName, capabilities }`; unknown
    capability fields default to false.
 5. Sessions are referenced by engine session id + SAIWORK2 session id; content
    history stays with the engine (law 16, 25).
 6. **OpenCode model identity** (TASK 25): the generic `ModelInfo.id` is the
    namespaced `<provider-id>/<raw-model-key>` pair — never a synthesized
    id. The wire `model` object sent to `POST /session/{id}/message` carries
    `{providerID: <provider-id>, modelID: <raw-key>}` with the raw key
    verbatim. Same raw key across two providers yields two distinct
    namespaced ids (unambiguous, no ambiguity enum needed). `selectedModelId
    == null` (Engine Default) omits the `model` field entirely.
 6b. **Provider attribution** (favorites feature): `ModelInfo.provider_name`
    carries the wire `Provider.name` (empty → `None`; the UI falls back to
    the raw `provider` key). auth.json-merged providers get name = key
    (OpenCode wire fact). The TS `ModelInfo` mirror adds `provider_name` —
    change both sides together (contracts).
 6c. **Model favorites** (durable UI preference): the app is the authority —
    `app_settings` k/v key `ui.models.favorites` holds a JSON array of model
    ids, capped at 50, deduplicated, corrupt values fail closed. IPC:
    `get_model_favorites` / `set_model_favorites` (the only favorites IPC;
    law 5 — the UI never writes the DB). Favorites are engine-independent
    because ids are globally namespaced.
 7. **Metadata body bounds** (TASK 25): the provider catalog (`/provider`
    and the `/config/providers` fallback) has its own bound
    `provider_catalog_max_bytes` (default 16 MiB — the real 1.18.18 catalog
    measures ~5.04 MiB); every other metadata read keeps the 4 MiB bound.
    A response over the bound is a typed `PROTOCOL` error, never a silent
    truncation or fake-empty success.

## Session-id namespace (TASK 24, ADR-044)

The generic SAIWORK2 session id **is the adapter's own id** — the exact value the
adapter returned from `create_session`, re-emitted verbatim in `message.*`/`tool.*`/
`permission.*` events. The frontend store keys sessions by that opaque id. It is
**never namespaced** at the `SessionManager` boundary (that would break event
correlation across adapters). Because the namespace is the adapter's, a second engine
returning the same id would collide in the generic map/DB — so `SessionManager::create`
fails closed with `SessionIdConflict` instead of silently overwriting the first
engine's session. All release adapters generate uuid-derived ids; the guard is
defense-in-depth against hostile/misbehaving adapters (TASK 24 §9/§120).

## Error model

Domains (fixed): `ENGINE PROCESS NETWORK PROTOCOL STORAGE QUEUE SAIPEN PATH
AUTH CONFIG INTERNAL`.

Every user-visible error must answer: what failed, what state remains, was user
data lost, can retry help, what can the user do. Raw stack traces go to
diagnostics, never as the primary message.

## Retry policy

Only operations that are idempotent, safe and plausibly transient retry.
Retries are bounded, with a max backoff. User cancellation stops retries.
Permanent protocol/auth errors never auto-retry forever (law 13).

## Engines

| Engine | Crate | Status |
| --- | --- | --- |
| Fake | `engine-fake` | Phase 0 — permanent test infrastructure |
| OpenCode | `engine-opencode` | Phase 1 — first production engine, `opencode serve` child process |
| Generic CLI | `engine-generic-cli` | TASK 17 — second production engine, one-shot trusted CLI (`OneShotText`) |
| Freebuff | — (no crate) | TASK 17 — **DEFERRED**: remote-cloud-only, Node≥22-only SDK, credential vault required. See DECISIONS.md ADR-036. |
| Antigravity | via OpenCode adapter | Only if a documented capability gap exists (MIGRATION_SAIWORK.md §8.5) |

### Generic CLI adapter (TASK 17 §43–§53)

`engine-generic-cli` is the second production engine and proves vendor
neutrality: the full generic workflow (workspace → select engine → start →
capabilities → create session → send → normalized events → cancel → terminal
→ queue → shutdown) runs with **zero OpenCode-specific branches outside the
adapters**.

- **Security model (§44–§47)**: the executable and fixed argument template
  come from SAIWORK2-owned env vars (`SAIWORK2_CLI_EXECUTABLE`,
  `SAIWORK2_CLI_ARGS`, `SAIWORK2_CLI_LABEL`, `SAIWORK2_CLI_MAX_OUTPUT_BYTES`,
  `SAIWORK2_CLI_TIMEOUT_MS`). Never from project files, never model-
  controlled. No shell — args are separate OS arguments, the prompt is
  **stdin bytes** (`ProcessSpec::StdinPolicy::Bytes`). Malformed config
  surfaces a precise error and the engine is not registered.
- **Capabilities (honest, §48)**: `sessions=true` (SAIWORK2-owned metadata;
  each send = one fresh process), `resume=false`, `streaming=false` (real
  output arrives at exit — no fake token deltas, §34), `cancel=true`
  (run == process: cancel terminates the managed tree, §52), everything else
  false. Engines without `models` never trigger model discovery in the UI.
- **Lifecycle (§26, §28)**: readiness is a config probe (absolute path must
  exist; bare names resolve against PATH incl. `.exe`), not a spawned
  process. Engine stop does not kill active runs; runs are stopped by
  `cancel` or the app-shutdown ProcessSupervisor sweep. Process state stays
  separate from engine readiness.
- **Output (§49)**: `ProcessSpec.output_cap_bytes` preserves the bounded
  answer independently of the diagnostic buffer; truncation appends an
  explicit marker. Timeout is bounded (§50); cancellation wins ties.
- **Queue**: durable `engine_id` targeting needs no adapter change —
  dispatch goes through the typed `EnginePort` (below). A CLI item targets
  the CLI engine; an unavailable engine keeps the item durable; there is no
  automatic fallback (§22–§23, §68).

## Queue port (TASK 13, `saiwork-core::queue_port`)

The queue dispatches through a typed `EnginePort` — never through engine
internals: `engine_state(engine_id) -> Ready|Starting|Unavailable`,
`ensure_session(session_id)`, `session_busy(session_id)`, `create_session`,
`send(session_id, payload, model) -> run_id`, `cancel(session_id, run_id)`.
`saiwork-core` bridges the port onto the engine registry + SessionManager;
`engine-fake` provides a port for tests. The port returns typed `PortError`
(codes) so the queue can classify session-not-found vs environmental vs
provider errors without knowing the engine. Queue dispatch and direct user
sends arbitrate through `session_busy` (queue waits; a direct send is
rejected with `SessionBusy`) — no hidden concurrency race.

## OpenCode adapter (TASK 10 — process layer only)

`engine-opencode` implements the process-capable layer of the contract:
**discovery → probe → launch spec → supervisor spawn → endpoint → readiness
→ lifecycle → failure handling**. Sessions/messages/SSE are TASK 11 and are
NOT implemented; the adapter reports all session-related capabilities as
`false` so the UI can never enter a dead chat workflow (§81–§83).

Verified contract (opencode-ai@1.18.18, Windows, npm global install):

```text
version        opencode --version          → opencode-ai@1.18.18
serve          opencode serve --port N --hostname 127.0.0.1 [--pure]
readiness      GET /doc → 200 + OpenAPI spec with info.title == "opencode"
auth           HTTP Basic; OPENCODE_SERVER_PASSWORD env var; any non-empty
               username + correct password → 200, else 401 (www-authenticate:
               Basic realm="Secure Area"). No CLI flag for auth in 1.18.18
               (factual limitation, §21).
port           --port 0 resolves to 4096 (OpenCode default), NOT OS-assigned
               (§16–§17) — the adapter allocates an available port and passes
               it explicitly; an EADDRINUSE startup failure is classified
               PortUnavailable and retried a bounded number of times.
output         "opencode server listening on http://127.0.0.1:PORT" goes to
               stdout; stderr carries non-fatal diagnostics (never treated as
               failure by itself, §88).
side effects    `opencode serve` does not mutate the workspace; its data is
               global (~/.local/share/opencode) — SAIWORK2 never touches it.
```

Launch mechanics (Windows):

- Preferred: the **native `opencode.exe`** shipped inside the npm package
  (spawned directly, no shell).
- Fallback: npm-global `.cmd` shim → launch through `cmd.exe /D /S /C` with
  `ProcessSpec::raw_args` (quoted command line passed verbatim, no MSYS
  mangling). Generic ProcessSupervisor stays shell-agnostic; the wrapper
  construction is OpenCode-specific and lives in the adapter (§8).

Discovery precedence (§5): explicit configured path → PATH resolution
(`opencode.exe` then `.cmd` shim) → `NotFound`. No recursive disk scan. An
invalid explicit path is a hard `ExecutableNotFound`, never a silent fallback
(§6).

Secrets (§21–§24): per-runtime random `OPENCODE_SERVER_PASSWORD` is generated
cryptographically, passed only via env (never argv), never persisted, never
logged. `ProcessSpec` snapshot/logging redacts it; a test asserts Debug output
contains no secret. On-demand health (`check_ready()`) uses the stored secret
without ever printing it.

Spawn auth is FULLY pinned, never ambient: `server_spec` sets BOTH
`OPENCODE_SERVER_PASSWORD` (the runtime secret) AND
`OPENCODE_SERVER_USERNAME=opencode` (the fixed identity the adapter's client
authenticates as) in the child env. Root-caused on 18.08.2026: an ambient
`OPENCODE_SERVER_USERNAME` inherited from the parent process chain (another
opencode deployment on the same machine) silently reconfigures the spawned
server's expected username — the server then 401s every Basic-auth request
with username `opencode` ("rejected the runtime credential"). The fix is the
pin, proven by real.rs 9/9 + real first-prompt smoke (two turns, real
provider) with the polluted env still present. The client username stays a
compile-time constant matching the pin.

Failed-start teardown is failure-atomic with respect to process authority. If
readiness fails and even forced supervisor cleanup cannot prove exit, the
adapter returns one error containing both causes, retains the runtime and exit
watcher, and refuses restart while that process remains live. Explicit
`stop`/`kill` can therefore retry teardown; runtime ownership is released only
after an exit observation (`Ok` or `NotRunning`).

Engine state stays canonical: `STARTING` until the authenticated `/doc` probe
succeeds — process RUNNING ≠ engine READY (ADR-015). Readiness is real
protocol evidence (HTTP 200 + OpenCode identity), never sleep; request timeout
+ overall startup deadline; process death short-circuits the retry loop
(§27–§32).

Model discovery (TASK 25 — real 1.18.18 behavior):

- Authoritative catalog: `GET /provider` (`{all, default, connected}`). The
  fallback `GET /config/providers` (`{providers, default}`) is used ONLY on a
  strict 404/405 from `/provider` (route absent) — never on auth/server/timeout
  failures: 401/403/500/timeout are typed errors, proven by the strict-fallback
  fixture (`FIXTURE_PROVIDER_HTTP` + `FIXTURE_PROVIDER_FALLBACK=1`).
- The real catalog measures ~5.04 MiB (191 providers / ~6,615 models) — it
  needs `provider_catalog_max_bytes` (default 16 MiB), see the body-bound rule
  above. A provider-level `key`/`apiKey` field is dropped by the DTO (secrets
  never cross the boundary).
- Model discovery is **non-fatal metadata**: on failure the engine stays
  READY, Engine Default remains selectable, and Send works (the `model` field
  is simply omitted). The UI surfaces the real backend diagnostic in
  `modelsError` ("… model discovery failed: <diagnostic>") — it never reduces
  the error to "failed to load models".
- Local credential file merge (auth.json): OpenCode's `auth.json` (standard
  per-user path `~/.local/share/opencode/auth.json`, or `OpenCodeConfig
  .auth_json_path`) may declare custom `type: api` providers that the server
  catalog does not expose. The adapter appends their declared `models` to the
  catalog after `GET /provider`. Policy, in order: (1) the server catalog is
  ALWAYS the authority — an auth provider id already in the catalog is never
  replaced or duplicated; (2) a credential-only entry (no `models`) is
  dropped — never an empty provider shell; (3) a missing or malformed file is
  a silent no-op (a credential file must never break discovery); (4) ONLY ids
  and model lists are read — `key`/`access`/`refresh` fields are never
  deserialized, never logged, never crossed (proven by the
  `auth_never_surfaces_secrets` unit test). Both wire shapes are parsed:
  flat `{ "<id>": {...} }` and legacy `{ "providers": {...} }`; `models` may
  be an array of ids or a map `id → options` (dynamic-fetch meta keys
  `url`/`baseURL`/`headers`/`options` are NOT model ids). Merge is
  generation-scoped like the catalog itself.

## Deferred engines and the adapter firewall (TASK 19–20)

DeepSeek Harness is classified EXPERIMENTAL ENGINE CANDIDATE (ADR-039,
KNOWLEDGE/DEEPSEEK_HARNESS.md). The TASK 20 foundation adapter
(`crates/engine-deepseek-harness`, ADR-040) now implements part of this contract and
will satisfy the rest at TASK 21: identity/probe/capabilities/start/ready/health/stop/kill,
listModels, sessions, send/cancel/resolvePermission, normalized message/tool/permission
events, terminal states, and normalized errors — no contract change per engine.

Current capability status for `deepseek-harness` (ENGINE_CONTRACT + DEEPSEEK_HARNESS §22–§23):

| Capability | Status |
|---|---|
| engine lifecycle (start/ready/stop/kill/crash/restart) | **IMPLEMENTED** (foundation, proven by 30-test hostile matrix) |
| protocol handshake (ACP initialize) + runtime metadata | **IMPLEMENTED** |
| sessions | **IMPLEMENTED** (TASK 21 — authoritative `session/new`, connection-owned registry) |
| streaming | **IMPLEMENTED** (TASK 21 — `agent_message_chunk` → incremental `message.delta`) |
| tools | **IMPLEMENTED** (TASK 21 — `tool_call` → `tool.*` lifecycle, bounded output) |
| permissions | **IMPLEMENTED** (TASK 21 — `request_permission` → generic round-trip, fail-closed) |
| cancel-run | **IMPLEMENTED** (TASK 21 — `session/cancel` scoped to RunId, race-safe) |
| resume | **FALSE** — ACP is fresh-sessions-only (never fake parity) |
| models | **FALSE** — ACP baseline exposes no provider/model discovery; `UseEngineDefault` only |
| queue dispatch | **ENABLED** (TASK 23) — through the generic `EnginePort` path, proven by `tests/queue_slice.rs`; the queue never knows ACP/Harness protocol details; crash/ambiguity handled by `QueueState::Unknown` |

Harness startup owns the partial runtime before its first `initialize` await.
Aborting the start future therefore schedules teardown through an RAII guard;
the adapter slot retains process/transport authority until exit is proven.
Failed teardown remains visible, blocks restart, and can be retried by explicit
`stop`/`kill`.

Generic contract: `EngineIdentity.experimental: bool` was added (cross-engine, proven by
OpenCode/FakeEngine/Generic CLI regression). `deepseek-harness` = `true` (Developer
Preview; UI marks it ⚠, never hides instability); all other engines = `false`. No
engine-specific branching in generic conversation/session/run/tool/permission UI.

## Capability model (TASK 22 audit — kept flat and truthful)

TASK 22 audited whether the flat boolean `EngineCapabilities` conflates static adapter
capability / runtime capability / current availability (Candidate B). Conclusion: no
adapter currently has runtime-varying capabilities — every flag is a static, truthful
fact of the adapter (e.g. Harness `models=false` because the ACP baseline exposes no
provider/model discovery; `resume=false` because ACP is fresh-sessions-only). The
`experimental` marker correctly lives on identity, not capabilities. A static/runtime
split, capability ontology, or richer enums would be architecture without evidence —
rejected (DECISIONS.md ADR-042). Capability truth is enforced by the per-adapter hostile
suites (advertised capability → operation works; unsupported → deterministic
`UnsupportedCapability`).

Firewall rules that apply to any future adapter (Freebuff, Harness, …):

- No upstream DTO outside the adapter crate. Generic core sees only the logical surface
  above (capabilities, state, SessionId/RunId wrappers, normalized events, errors).
- Capabilities are declared only after the seam is proven (no fake parity):
  e.g. Harness ACP is fresh-sessions-only → `session_resume = false` until upstream adds it;
  committed-chunk output maps to `message.completed`, never fake token deltas.
- Same-session concurrency stays REJECT; one mutating run per workspace stays the outer
  gate; queue concurrency stays 1 (ADR-038).
- Protocol version is probed and recorded at connect; known-incompatible contract is
  rejected; additive fields are tolerated. No idempotency key in either Harness seam ⇒ the
  TASK 13 ambiguous-dispatch policy applies unchanged (no automatic redispatch).
- Process ownership boundary: the adapter's top-level runtime belongs to ProcessSupervisor;
  the engine's internal agent/tool subprocesses belong to the engine (no double
  supervision).
