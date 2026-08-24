# SECURITY.md

## Workspace boundary (defensive path handling)

For every workspace-relative operation:

```text
requested path
↓
normalize
↓
canonicalize (as appropriate)
↓
verify boundary
↓
operate
```

Never use naive string prefix `path.startsWith(root)` as a security boundary.
Windows specifics are first-class: `C:\`, `\\server\share`, `\\?\`, junctions,
symlinks, case-insensitive comparison (donor lesson: saipenview `paths.py`
canonical form = normcase → resolve → normpath → drive-root trailing slash;
SAIWORK `path-security.ts` `resolvePathWithin`).

Forbidden: `..` escape, symlink escape where security-sensitive, absolute path
injection, unexpected UNC/path forms.

## Lifecycle surface (TASK 08)

- **Single-instance IPC**: a second launch relays typed launch args only
  (window activation / future `OpenPath` intents). No raw command execution,
  no arbitrary Tauri-command invocation, no deserializable executable
  action; the payload is the process argv, which is small and bounded (§17,
  §91). The secondary never opens the DB or a second runtime.
- **Launch intents** are treated as external input: future path intents must
  be validated (canonicalized, boundary-checked) before use.
- **Startup environment**: environment variables are never dumped to logs
  (no `std::env::vars()` anywhere in bootstrap) — dumping APPDATA/PATH/keys
  would leak secrets into a plain-text file (§92).
- **Diagnostics** (get_diagnostics snapshot): version, data root, portable
  flag, lifecycle state, storage status, schema version, engine/supervisor
  counts, process snapshots, workspace/session counts, bounded recent-error
  ring, event subscriber count, log dir. No API keys, tokens, passwords,
  authorization headers, or full prompts (§36, §39). Paths appear only where
  they serve local diagnostics; the recent-error ring is bounded (law 13).

## Credentials

SAIWORK2 is not an API-key warehouse:

```text
OpenCode credentials     → OpenCode authority (opencode auth)
Freebuff credentials     → Freebuff integration authority
OS secure storage        → only if SAIWORK2 genuinely owns the credential
```

Never log: tokens, authorization headers, API keys, refresh tokens,
passwords. Diagnostics include a secret-redaction layer (donor lesson:
SAIWORK `log-sanitize.ts`). Redaction is applied at the log boundary, not
after the fact.

## Localhost engines

Engine servers bind loopback only, with a dynamically assigned port and a
generated local secret when the engine supports one. SAIWORK2 never exposes an
engine to non-loopback interfaces by default.

## Engine trust

External engines may be malicious or compromised (law 17). SAIWORK2 treats
engine output as untrusted data: render as text, never as HTML/JS from the
engine, and never execute engine-supplied commands without the user's explicit
permission flow.

## Generic CLI security model (TASK 17 §44–§47, §96–§98)

- **Trusted config only:** executable + fixed args come from SAIWORK2-owned
  env vars (`SAIWORK2_CLI_EXECUTABLE` etc.). No project file can declare a
  command, no model can choose an executable, no template interpolation.
- **No shell anywhere:** `ProcessSupervisor` spawns the program directly;
  args are separate OS args; the prompt is **stdin bytes** (never
  `cmd.exe /C`, `sh -c`, or a quoted command string). Prompts never appear
  in logs; `StdinPolicy` Debug prints byte counts only.
- **Bounded everything:** prompt size cap, per-process output cap, bounded
  execution timeout, run == process cancellation (no eternal zombie).
- **No credentials:** the CLI adapter stores no secrets; engine readiness is
  a config probe, not an executed program.
- Malformed config → precise error + engine not registered (never a silent
  fallback).

## Data at rest

The SQLite DB may contain prompt text (queue payloads are plaintext in
`queue_items.payload` — documented truth, TASK 13 §91). Local DB permissions
default to the user only. Secrets are never stored in the DB unless SAIWORK2
owns them, which is avoided whenever possible.

## Queue logging (TASK 13)

Queue events (`queue.changed`, `queue.dispatch_*`) and logs carry item ids,
states, and error codes — **never payload/prompt text**. `last_error` is
bounded and stores only error codes/messages from the typed queue domain.
Prompts are written only to the SQLite row (plaintext at rest) and to the
engine via the port; they never cross the EventBus and never appear in
`queue_snapshot` diagnostics.

## OpenCode adapter security (TASK 10, 2026-08-16)

- **Bind**: `opencode serve --hostname 127.0.0.1` — loopback only, never
  `0.0.0.0` (TASK 10 §14). No SAIWORK2-side reverse proxy is added; the
  server is reached directly on loopback.
- **Local auth**: per-runtime `OPENCODE_SERVER_PASSWORD` (cryptographic
  randomness, runtime lifetime only) via env var — HTTP Basic, matching the
  verified 1.18.18 contract. The secret is never in argv, never persisted,
  never logged, never in `ProcessSpec` snapshots; a hostile test asserts the
  Debug output contains no secret and that env is passed as a redacted
  name-only list (§24, §74).
- **Discovery trust**: an explicit invalid executable path is a hard error —
  no silent fallback to another OpenCode (TASK 10 §6). The probe rejects
  unrelated binaries before any server launch (§9, §52).
- **No provider credentials**: TASK 10 manages only SAIWORK2↔local-server
  auth; OpenCode's own provider credentials are its business (§79).
- **Readiness hygiene**: HTTP redirects are not followed (policy `none`),
  response bodies are size-bounded, every request has a timeout — a malformed
  response is never mistaken for OpenCode readiness (§28, §70–§72).

## SAIPEN path boundary (TASK 14, 2026-08-16)

- Every SAIPEN path is validated: normalize → resolve (following
  symlinks/junctions) → component-aware containment in the canonical
  workspace root. Naive string prefix is never a boundary (§12).
- `.saipen` itself or any canonical file reference that resolves outside the
  workspace → typed `PathEscape`, never silently followed (§13, §145).
- Windows: `\\?\` canonicalize prefixes normalized away, drive-letter prefix
  comparison, case-insensitive components, device paths (`\\.\`, `\\?\`)
  rejected as roots (§146). Tested: symlink escape, `..` references,
  separator-in-reference rejection, component containment (no `/a/bc`
  prefix confusion).
- TOCTOU: re-resolve at open time; residual OS race documented, not
  eliminable locally (§149).
- Frontend never receives filesystem authority: `get_saipen(workspace_id)`
  resolves the trusted root from WorkspaceId; no raw path parameter (§152–§153).
- Reading uses native Rust APIs only — no shell `type`/`findstr`/
  `Get-Content` (§150).

## SAIPEN actions boundary (TASK 15, 2026-08-16)

- **Typed command boundary**: the frontend sends `(workspace_id, action)`
  strings only; `SaipenAction::from_str` is the sole decoder (unknown →
  typed error, never `run_saipen_command(String)`).
- **No shell by default**: the canonical tool is invoked as
  `python <tool> args…` via ProcessSupervisor with `cmd.arg(...)` — never
  `cmd /C` or a composed shell string (§9).
- **Explicit cwd**: every action runs with cwd = the validated workspace
  root resolved from WorkspaceId — never `set_current_dir` and never the
  `.saipen` dir itself (§10).
- **Tool discovery is bounded**: `SaipenTool::discover` resolves
  `saipen_home` from the parsed STATE (project-local canonical entrypoint);
  explicit invalid path is a typed `NotAvailable`, no disk-wide search, no
  `PATH` guess-by-filename (§6). Schema version is gated before any action
  (§7, §132).
- **Output/log hygiene**: bounded output capture; INFO logs carry
  `{action_id, workspace_id, kind, result}` only — never full stdout or
  project content (§21, §82–§84). No secrets in the action registry.
- **Exclusivity is backend-enforced**: one active action per workspace; a
  second `start` returns typed `Busy` even under double-click/retry (§14,
  §34, §77, §119).

## Frontend security (TASK 16, 2026-08-16)

- **Markdown XSS**: react-markdown renders no raw HTML by default (no
  rehype-raw); links open externally with `rel="noreferrer"`; code blocks
  are plain text. No `dangerouslySetInnerHTML` in the codebase.
- **No new Tauri capabilities**: capabilities stay exactly
  `core:default` + `dialog:default` — the new `resolve_permission` is a
  normal internal command, not a permission. No `shell:`/`fs:`/`process:`
  exposure. Clipboard uses the webview `navigator.clipboard` (copy message /
  code / diagnostics).
- **Diagnostics redaction**: `Copy diagnostics` serializes the already-
  redacted backend snapshot (no prompts/tool content — the Rust side
  redacts secrets; §70, §100).
- **No remote content**: no remote fonts/assets/scripts at runtime — the
  app renders offline (§234).

## Phase 0 verification (TASK 09, 2026-08-16)

- Tauri capabilities are minimal: `core:default` + `dialog:default` only. No
  `fs:`/`shell:`/`process:` permission is exposed to the frontend —
  ProcessSupervisor stays an internal Rust authority.
- IPC surface is typed Tauri commands gated by the core state machine
  (`require_ready` semantics; tested per-state). No arbitrary command
  execution, no path-accepting commands in Phase 0.
- Secret/path audits: no real secret in the repo; diagnostics redaction tests
  pass; runtime logs from hostile tests contain no secret material; no
  developer-specific absolute paths remain in committed sources.
- Single-instance uses an OS-named mutex, which releases automatically on
  crash (stale-state safety, ADR-018).

## Release hardening (TASK 18)

- **CSP (tauri.conf.json):** `default-src 'self'; script-src 'self';
  style-src 'self'; img-src 'self' data:; connect-src ipc:
  http://ipc.localhost ws://localhost:1420; font-src 'self' data:` — no
  `unsafe-eval`, no unrestricted connect-src; the `ws://localhost:1420`
  exception is the dev-HMR endpoint only (§69).
- **Release content (§64):** FakeEngine registration is
  `#[cfg(debug_assertions)]` (dev-only); queue failpoints are a non-default
  feature with no-op production hooks (§66); the packaged build ships the
  bundled frontend (no Vite/dev URL dependency, §68); release registers 1
  engine by default (OpenCode) plus an explicitly configured Generic CLI.
- **Input bounds (§73):** queue payload 64 KiB (`PAYLOAD_MAX_BYTES`),
  OpenCode prompt 1 MiB, Generic CLI prompt 64 KiB — typed errors, never
  silent truncation (§149). No unbounded IPC payloads.
- **Frontend surface (§248 audit):** zero `dangerouslySetInnerHTML`,
  `localStorage`, `eval`, `setInterval` in production code; Markdown is
  react-markdown safe-defaults (no raw HTML); no frontend process/
  filesystem/network authority.

## DeepSeek Harness trust model (TASK 19 audit — adapter not yet built)

Findings that any future Harness adapter must honor (DEEPSEEK_HARNESS.md §14):

- **Config is home-owned**: profiles/bundles/patches compose from the Harness home, not
  the project workspace; `--patch` overlays are explicit. Project-workspace auto-load of
  plugins/settings is UNKNOWN — the TASK 20 probe must verify it before any untrusted
  workspace is opened, and SAIWORK2 must never auto-enable project-defined plugins.
- **ACP narrows the surface**: `session/new` rejects non-empty `mcpServers`/
  `additionalDirectories` — no project MCP auto-connect through the ACP seam.
- **Credentials stay in Harness home** (credentials plugin); SAIWORK2 passes
  provider/model selection only and never mirrors DeepSeek API keys, logs them, or stores
  them in SQLite (same rule as ADR-036).
- **Sandbox is engine-internal and composed, not guaranteed** — SAIWORK2 must not claim
  Harness activity is sandboxed unless the selected profile proves it; Windows sandbox
  strength is UNKNOWN (probe gate).
- **SDK `session.event` streams unfiltered full session-log envelopes** — the adapter
  boundary must filter/redact and never bypass SAIWORK2's bounded event/UI batching;
  no raw protocol payloads in diagnostics.
- **No cancel on the SDK seam** (ACP has scoped `session/cancel`); capability truth,
  not UI optimism.

## TASK 20 — Harness adapter implemented trust boundary

`crates/engine-deepseek-harness` (foundation) enforces:
- **Configured-only registration**: the engine is registered only when
  `SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE` is set by the user — never discovered into
  the registry silently, never from project files (§58 rule). The executable is
  explicit trusted config; an invalid path is a typed error, no fallback.
- **No shell, no credentials**: the runtime is spawned through ProcessSupervisor with
  typed args (no shell string); SAIWORK2 never reads, stores, or logs Harness/DeepSeek
  credentials (they remain Harness-home-owned); env is explicit (no SAIWORK2 secrets
  injected). The `--version` probe and the ACP handshake exchange no secrets;
  clientInfo is name+version only.
- **Bounded protocol**: 1 MiB frame cap both directions; malformed/oversized frames
  kill the transport deterministically (no memory explosion, no panic); raw protocol
  payloads never reach diagnostics (bounded debug only); secrets are redacted by the
  shared redaction boundary.
- **Trust policy**: no project-local plugin/MCP/skill auto-load is enabled by this
  adapter; the ACP seam itself rejects non-empty `mcpServers`/`additionalDirectories`
  (upstream narrows the surface). Windows sandbox strength remains UNKNOWN — never
  claimed (TASK 21 probe gate).
- **Failure isolation**: one engine's failure cannot affect OpenCode/others (registry
  isolation test); no automatic fallback, no reconnect loop.

## TASK 21 — Harness adapter vertical-slice security truth

`crates/engine-deepseek-harness` (agent vertical slice, DEEPSEEK_HARNESS.md §23) adds:
- **Permission fail-closed**: a pending `session/request_permission` with no decision
  (UI disconnect, shutdown, engine failure, run terminal) resolves **reject** — never
  default allow. `resolve_permission` is idempotent (unknown/already-resolved/stale =
  no-op); the server never receives a second decision or a decision for a stale
  generation. Engine stop clears pending permissions and settles active runs (no
  orphaned UI permission, no eternal RUNNING).
- **Bounded, safe tool/permission detail**: tool output capped at 32 KiB, raw-input
  summary capped at 500 chars — never raw giant JSON, never secret-bearing env dumps
  (§50–§51, §62). Tool input is not logged in full by default.
- **Exactly-one mutation boundary**: one `send` = one `session/prompt`; no auto-retry
  after ambiguous transport failure (accepted-then-response-lost → honest outcome
  unknown, never a duplicate prompt). Same-session concurrency REJECT; cancel is scoped
  to a RunId and never kills the runtime process.
- **No credential handling**: the adapter never reads, stores, or mirrors Harness/
  DeepSeek credentials; provider/model selection delegates to the Harness profile
  default (`UseEngineDefault`). No SAIWORK2 credential entry offered (§24).
- **Workspace trust unchanged**: starting a session does not enable project
  plugins/MCP/extensions; the ACP seam rejects non-empty `mcpServers`/
  `additionalDirectories` (§140). Windows sandbox/profile truth remains UNKNOWN and is
  never claimed as sandboxed (§141).
- **Experimental honesty**: `EngineIdentity.experimental = true` — the UI marks the
  engine ⚠ and never hides instability (§88, §146 capability truth).

## TASK 23 — Harness queue correlation security

Queue persistence for Harness-targeted work stores only identity/recovery metadata
(`session_id`, `run_id`, error category) — **never credentials, permission secrets, full
tool output, or the Harness session log** (TASK 23 §59). Logs record QueueItemId/attempt/
EngineId/SessionId/RunId/transition/reconciliation result, never prompt bodies. The
`UNKNOWN` state blocks its workspace but grants nothing; retrying it is an explicit,
risk-acknowledged user act, never automatic. No direct SAIPEN file mutation was added;
SAIPEN Continue handoff is deferred precisely because it cannot be proven exactly-once
(no arbitrary file-path execution, no duplication machine).

## TASK 22 — architecture audit security result

No security regression and no security change: the audit introduced no new code path
that touches trust, credentials, process authority, or permissions. Workspace trust
metadata remains **DEFER** (no concrete code-bearing workspace-config path is gated by
SAIWORK2 yet; the ACP seam itself rejects project MCP/additional dirs). The one cleanup
fix (Harness `start()` partial-init leak) and the event-classification documentation do
not weaken Tauri permissions, CSP, path validation, credential boundaries, permission
fail-closed, or project trust.
