// Shared contracts between the SAIWORK2 core (Rust) and UI (TypeScript).
// These mirror `saiwork-events`/`saiwork-core` serde shapes. The Rust side is
// authoritative; change both sides together (KNOWLEDGE/EVENTS.md).

/** One normalized event envelope: `{ seq, ts, type, ...payload }`. */
export interface Envelope {
  seq: number;
  ts: number;
  type: string;
  [payload: string]: unknown;
}

// ---- Engine ----

// Mirrors the Rust `EngineHealth` serde shape (internal tag `kind`): unit
// variants serialize as plain strings, struct variants as tagged objects.
export type EngineHealth =
  | "unknown"
  | "starting"
  | "ready"
  | "stopped"
  | { kind: "degraded"; message: string }
  | { kind: "failed"; message: string };

export interface EngineIdentity {
  id: string;
  display_name: string;
  version: string;
  /** Engine is experimental (Developer Preview): the UI marks it and never hides instability. */
  experimental: boolean;
}

export interface EngineCapabilities {
  streaming: boolean;
  sessions: boolean;
  resume: boolean;
  cancel: boolean;
  tools: boolean;
  permissions: boolean;
  attachments: boolean;
  images: boolean;
  models: boolean;
  usage: boolean;
  reasoning: boolean;
  context_window: number | null;
  worktrees: boolean;
  parallel_sessions: boolean;
  /** Engine can revert the last visible user turn and restore it. */
  session_revert: boolean;
  structured_events: boolean;
}

export interface EngineInfo extends EngineIdentity {
  health: EngineHealth;
  capabilities: EngineCapabilities;
  /** Workspace the runtime is currently bound to (undefined/null = not bound /
   * unknown). A READY engine bound to a different workspace cannot serve the
   * current project until explicitly restarted for it (TASK 24 §9). */
  bound_workspace_id?: string | null;
}

/** Normalize `EngineHealth` (string | tagged object) to its kind string. */
export function healthKind(health: EngineHealth): string {
  return typeof health === "string" ? health : health.kind;
}

export interface ModelInfo {
  id: string;
  display_name: string;
  /** Provider key (part of the composite `id`), nullable for engines
   * without provider concepts. */
  provider: string | null;
  /** Provider display name (wire `Provider.name`), when the engine exposes
   * one — the UI shows it instead of the raw key when present. */
  provider_name: string | null;
}

// ---- Workspaces ----

// Mirrors the canonical `saiwork-saipen` SaipenSnapshot serde shape (TASK
// 14). SAIPEN is authoritative; this is a read-only projection. Every
// canonical field is nullable — null renders as UNKNOWN, never a fabricated
// default. `board.sections` status derives from the canonical BOARD section
// (DOING/TODO/DONE/BLOCKED), never the checkbox.
export type SaipenWatchStatus = "not_watching" | "live" | { failed: string };

export interface BoardSummary {
  sections: Record<string, string[]>;
  counts: Record<string, number>;
}

export interface SaipenState {
  generation: number;
  read_at_ms: number;
  root: string | null;
  schema_version: string | null;
  saipen_version: string | null;
  project: string | null;
  phase: string | null;
  task: string | null;
  next_action: string | null;
  blocker: string | null;
  mode: string | null;
  execution_intent: string | null;
  agent: string | null;
  updated: string | null;
  last_event: string | null;
  board: BoardSummary;
  watch_status: SaipenWatchStatus;
  last_error: string | null;
  stale: boolean;
}

/** Cheap SAIPEN presence/version badge carried on workspace rows (TASK 24
 * perf): produced from STATE discovery only — zero BOARD reads, no
 * consistency pipeline. The full projection is `SaipenState` via
 * `getSaipen` (the SaipenService cache for attached workspaces). */
export interface SaipenSummary {
  schema_version: string | null;
  saipen_version: string | null;
  project: string | null;
}

export interface WorkspaceSummary {
  id: string;
  path: string;
  name: string;
  has_git: boolean;
  saipen: SaipenSummary | null;
  last_opened_at: number | null;
}

// ---- SAIPEN actions (TASK 15) ----
// Mirrors the `saiwork-saipen` ActionManager serde shapes. Action state is
// an in-memory backend registry (§61); the UI never reconstructs availability
// — it fetches `saipen_action_status` (§56). `continue` is listed as
// unsupported: no canonical `saipen continue` CLI exists in the verified
// v7.224.3 contract, and the bar must disable it honestly (§3, §131).

export type SaipenActionState =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelling"
  | "cancelled";

export interface SaipenActionRecord {
  action_id: string;
  workspace_id: string;
  action: string;
  state: SaipenActionState;
  started_at_ms: number;
  duration_ms: number | null;
  result: string | null;
  error: string | null;
}

export interface SaipenActionAvailability {
  available: string[];
  running_action: string | null;
  unsupported: string[];
  /** When set, ALL executable actions (status/validate) are disabled for this
   * reason (T-080): e.g. "saipen_home ... is not an explicitly trusted SAIPEN
   * install". The bar shows the reason and MAY offer one-click trust. */
  disabled_reason: string | null;
}

export interface SaipenActionStatus {
  availability: SaipenActionAvailability;
  running: SaipenActionRecord | null;
  /** Canonical validation outcome ("valid"/"invalid"), tied to the snapshot
   * generation it validated (§87); null when never run. */
  validation_result: string | null;
  /** True when the snapshot moved past the validated generation (§88). */
  validation_stale: boolean | null;
  snapshot_generation: number;
}

// ---- Sessions ----

export interface Session {
  id: string;
  workspace_id: string | null;
  engine_id: string;
  engine_session_id: string;
  display_name: string;
  created_at: number;
  running: boolean;
  /** Strictly “survives runtime/app restart”. False for legacy metadata
   * whose upstream id was never recorded and for connection-owned engine
   * sessions (TASK 24 §9). NOT the same as current usability. */
  resumable: boolean;
  /** Transient usability with the current engine runtime generation — the
   * field the UI gates selection/send on. A fresh connection-owned
   * (resume=false) session is usable_now=true right after creation even
   * though resumable=false; after the runtime restarts its old sessions
   * become unusable history. Backend-computed, never fabricated (TASK 24
   * §9). */
  usable_now: boolean;
}

export interface RunHandle {
  run_id: string;
}

/** Authoritative direct-send outcome (mirrors the Rust `SendAcceptance`
 * serde shape). The UI must distinguish a definite rejection (safe to drop
 * the pending user turn) from an unknown outcome (the run may still be
 * executing — keep the turn marked UNCERTAIN, never blind-resend — TASK 24
 * §9). */
export type SendOutcome =
  | { kind: "accepted"; run_id: string }
  | { kind: "definitely_rejected"; code: string; message: string }
  | { kind: "outcome_unknown"; run_id: string; message: string };

// ---- Diagnostics ----

export interface ErrorRecord {
  code: string;
  message: string;
  ts_ms: number;
}

// Mirrors the `saiwork-process` ProcessSnapshot serde shape: the OS process
// state machine (SPAWNING/RUNNING/STOPPING/EXITED/FAILED) — never engine
// readiness. `env` is names only; values never cross the boundary.
export interface ProcessSnapshot {
  id: string;
  state: "SPAWNING" | "RUNNING" | "STOPPING" | "EXITED" | "FAILED";
  pid: number;
  command: string;
  env: string[];
  output_bytes: number;
  dropped_lines: number;
  exit_code: number | null;
}

/** Application lifecycle state (mirrors `saiwork-core::AppState`). */
export type LifecycleState = "booting" | "ready" | "shutting_down" | "stopped" | "failed";

/** Startup stage timings (baseline facts, not an SLA). */
export interface StartupTimings {
  data_root_ms: number;
  storage_ms: number;
  services_ms: number;
  total_ms: number;
}

// ---- Queue (TASK 13) ----

export type QueueState =
  | "queued"
  | "leased"
  | "dispatched"
  | "done"
  | "failed"
  | "cancelled"
  | "unknown";

export type QueueStatus = "ready" | "paused" | "failed" | "shutting_down" | "stopped";

export interface QueueItem {
  id: string;
  workspace_id: string;
  engine_id: string;
  session_id: string | null;
  session_mode: "new" | "existing";
  model: string | null;
  payload: string;
  /** True when `payload` is a truncated preview of the real (possibly very
   * large) prompt body — the UI must show a `…` and fetch the full item to
   * edit (TASK 24 perf + no-unbounded-everything law). */
  payload_truncated: boolean;
  state: QueueState;
  order_key: number;
  revision: number;
  lease_id: string | null;
  leased_at: number | null;
  attempt_count: number;
  run_id: string | null;
  last_error: string | null;
  last_error_code: string | null;
  created_at: number;
  updated_at: number;
}

export interface QueueSnapshot {
  status: QueueStatus;
  paused: boolean;
  items: QueueItem[];
  /** True when items[].payload is a bounded preview; fetch the full item for editing. */
  payload_preview?: boolean;
}

export interface DiagnosticsSnapshot {
  version: string;
  data_root: string;
  portable: boolean;
  lifecycle: LifecycleState;
  startup_ms: StartupTimings | null;
  last_shutdown_ms: number | null;
  db_integrity: string;
  db_schema_version: number;
  storage_status: string;
  engines: EngineInfo[];
  engine_count: number;
  supervisor_active: number;
  processes: ProcessSnapshot[];
  workspaces: number;
  sessions: number;
  recent_errors: ErrorRecord[];
  event_subscribers: number;
  log_dir: string | null;
  log_fallback: boolean;
  platform: string;
  architecture: string;
  timestamp_ms: number;
}

// ---- Files (Phase C, read-only) ----
// Mirrors the canonical `saiwork-files` serde shapes. The backend resolves
// the workspace root from WorkspaceId; `rel` is the untrusted workspace-
// relative path carried by the command args.

export type FileKind = "file" | "dir" | "symlink";

export interface FileEntry {
  name: string;
  /** Workspace-relative, forward slashes ("sub/name.txt"). */
  rel_path: string;
  kind: FileKind;
  /** Byte size; files only. */
  size: number | null;
  /** Last-modified ms since epoch; files only. */
  modified_ms: number | null;
  /** W2-007: false for entries the UI must not open (e.g. a non-UTF-8
   * filename that cannot be represented losslessly as a rel-path token). */
  navigable: boolean;
}

export interface DirListing {
  /** The rel path that was listed ("." = workspace root). */
  dir: string;
  entries: FileEntry[];
  /** True when the directory had more entries than the backend bound. */
  truncated: boolean;
}

export interface FilePreview {
  rel_path: string;
  /** Bounded UTF-8 head; char-boundary trimmed. Empty for binary files. */
  text: string;
  /** True when the file is longer than the preview bound. */
  truncated: boolean;
  /** True when a NUL byte was found in the sniff window. */
  binary: boolean;
  total_bytes: number;
}
