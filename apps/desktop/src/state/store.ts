// The UI store is a projection of authoritative core state (law 23). Events
// mutate exactly one store; ephemeral UI state lives in components, derived
// state is computed, never duplicated.

import type {
  EngineHealth,
  EngineInfo,
  LifecycleState,
  ModelInfo,
  QueueItem,
  QueueStatus,
  SaipenState,
  Session,
  WorkspaceSummary,
} from "@saiwork2/contracts";
import { useSyncExternalStore } from "react";
import { str, type TypedEvent } from "./events";
import type { DockTab } from "../components/dock/types";
import { applyWorkspaceClosed } from "../app/workspaceSelection";
import { loadSessionHistory } from "../app/sessionSelection";

// ---- Message model (UI-local projection of engine streams) ----

export type MessageStatus =
  | "streaming"
  | "complete"
  | "failed"
  | "cancelled"
  // Terminal-but-unprovable (TASK 24 §9): the backend cannot prove whether
  // the external run finished. Not a fake completion — the UI shows the
  // ambiguity and the workspace reservation stays until authoritative
  // reconciliation or explicit risk-confirmed resolution.
  | "outcome_unknown";

/** One normalized historical message from the owning engine (TASK 24 §9). */
export interface SessionHistoryMessage {
  id: string;
  role: string;
  text: string;
  tool_call_id: string | null;
  tool: string | null;
  order: number;
  /** Authoritative upstream creation time in epoch milliseconds. */
  ts?: number;
}

export interface ToolActivity {
  /** Stable upstream identity of ONE tool invocation (TASK 24 §9): two
   * same-named tools in one run never merge. */
  id: string;
  tool: string;
  status: "started" | "output" | "completed" | "failed";
  output: string;
  error?: string;
  /** Wall-clock ms of the tool's FIRST observation (T-081): the Activity
   * panel can render "how long ago" from a real timestamp, not a guess. */
  ts: number;
  /** PERF-002: per-tool stream-envelope watermark. A re-delivered
   * `tool.output` whose `event.seq` is not strictly greater is dropped before
   * append. Stored on the tool itself so it is reclaimed with the message. */
  lastToolSeq?: number;
}

export interface Message {
  id: string;
  role: "user" | "assistant";
  runId: string;
  status: MessageStatus;
  text: string;
  tools: ToolActivity[];
  error?: string;
  permissions: PermissionEntry[];
  questions: QuestionEntry[];
  /** Wall-clock ms of the message's FIRST observation (T-081): rendered as a
   * timestamp + "how long ago" in the conversation. Set once at creation;
   * stable for the message lifetime. */
  ts: number;
  /** PERF-002: per-message stream-envelope watermark. A re-delivered
   * `message.delta` whose `event.seq` is not strictly greater is a
   * duplicate/stale delivery and is dropped BEFORE append. Stored on the
   * message itself (not a lifetime-global map) so it disappears with the
   * message — no history-sized copy on every streaming token. */
  lastDeltaSeq?: number;
  /** User turns only: true when the send outcome was UNPROVEN (the run may
   * still be executing upstream). The turn stays visible and marked
   * UNCERTAIN — never removed, never blind-resent (TASK 24 §9). Cleared when
   * the engine authoritatively starts/completes the run. */
  uncertain?: boolean;
  /** The RunId the uncertainty is bound to — only matching execution
   * evidence / a definitive terminal may clear it (TASK 24 §9). */
  uncertainRunId?: string;
}

export interface PermissionEntry {
  requestId: string;
  detail: string;
  allowed: boolean | null;
}

/** AUDIT-CORE-002: a user question asked by the run. `resolved` is null
 * while open, then true (answered) / false (rejected). */
export interface QuestionEntry {
  requestId: string;
  detail: string;
  resolved: boolean | null;
}

export interface LogEntry {
  seq: number;
  ts: number;
  type: string;
  message: string;
}

/** Application lifecycle projection (TASK 08 §33): read-only mirror of the
 * Rust AppState; the Rust core is the single authority (law 23). */
/** Consumes the shared lifecycle contract (T-019): the UI must never duplicate
 * a narrower local type — `stopped` is part of the canonical `LifecycleState`
 * and the shell uses it after `app_shutdown` exits the process. */
export type Lifecycle = LifecycleState;

export interface AppState {
  backend: "connected" | "disconnected";
  lifecycle: Lifecycle;
  version: string | null;
  workspaces: WorkspaceSummary[];
  currentWorkspaceId: string | null;
  engines: EngineInfo[];
  /** Engines currently stopping (UI-local transitional projection): while
   * set, Start is NOT exposed and Stop is not repeatable — Start becomes
   * available only on the authoritative `engine.stopped`/`engine.failed`
   * terminal (TASK 24 §9). Never replaces engine health. */
  stoppingEngines: Record<string, boolean>;
  /** Engines currently starting (UI-local transitional projection, mirror of
   * `stoppingEngines`): set OPTIMISTICALLY on the Start click so the user
   * sees instant feedback, cleared on the authoritative terminal or when the
   * start command resolves. The event stream is the live path, the
   * post-invoke authoritative pull is the guarantee — a dead event stream
   * must never force a manual reload to see engine state (user report). */
  startingEngines: Record<string, boolean>;
  selectedEngineId: string | null;
  models: ModelInfo[];
  selectedModelId: string | null;
  /** Model-list fetch state (TASK 25 §22): a metadata failure must never make
   * the app look broken — Engine Default stays selectable and Send stays
   * usable. `modelsError` carries the exact safe backend diagnostic for the
   * compact "Models unavailable" warning. */
  modelsLoading: boolean;
  modelsError: string | null;
  /** Durable model favorites (model ids, engine-independent — ids are
   * globally namespaced `<provider>/<raw-key>`). Loaded once at bootstrap
   * from the app authority; every toggle persists through the backend. */
  favoriteModelIds: string[];
  /** Favorites-only projection toggle (ephemeral UI state). */
  favoritesOnly: boolean;
  sessions: Session[];
  activeSessionId: string | null;
  /** sessionId → COMPLETED transcript (append-only, bounded per message).
   *
   * INVARIANT (stream hot path): this array's identity is STABLE while a run
   * streams. Delta/tool/permission facts of the live run mutate ONLY
   * `activeMessage`, so the per-token cost is independent of transcript
   * length — a 10k-message session costs the same per token as an empty one.
   * The final assistant turn is appended here EXACTLY ONCE, on its
   * authoritative terminal. */
  messages: Record<string, Message[]>;
  /** sessionId → the live streaming assistant turn (the "active tail"), or
   * null when the session is idle. Replaced (new identity) per coalesced
   * delta batch; committed into `messages` on the run's terminal. The
   * rendered transcript is `messages[sid] + activeMessage[sid]`. */
  activeMessage: Record<string, Message | null>;
  /** Authoritative engine-history state per session (TASK 24 §9): distinct
   * from an empty conversation — `unavailable` means the engine exposes no
   * history capability, `error` means the authoritative read failed. The UI
   * shows these explicitly instead of fabricating a complete empty thread. */
  historyStatus: Record<string, "idle" | "loading" | "available" | "unavailable" | "error">;
  /** sessionId → active run id, or null when idle. */
  running: Record<string, string | null>;
  /** True when the last `active_runs` authoritative read FAILED (T-018):
   * running ownership is UNKNOWN, so ownership-sensitive Cancel/Send are
   * disabled until a fresh read succeeds. A failed read never fabricates
   * idle — the previous projection is preserved and marked stale. */
  runningStale: boolean;
  /** W2-003: per-session stream-gap marker (see `streamGaps` initial value). */
  streamGaps: Record<string, boolean>;
  /** UI queue projection of the durable QueueManager snapshot (TASK 13).
   * `revision` bumps on every queue.* event; components refetch the snapshot
   * (SQLite is the authority — the UI never holds a second truth, law 5). */
  queue: {
    status: QueueStatus;
    paused: boolean;
    items: QueueItem[];
    revision: number;
    /** True when the last authoritative snapshot fetch failed: the items
     * shown are stale and every revision-sensitive mutation is disabled
     * until a fresh snapshot succeeds (TASK 24 §9). */
    stale: boolean;
    /** True when `items[].payload` carries a bounded PREVIEW (the backend
     * never materializes full payloads for the snapshot): the exact payload
     * is fetched per item before editing. */
    payloadPreview: boolean;
    /** The single item_id a `queue.changed` is attributable to, if the core
     * could name exactly one row (PERF-004). The queue owner patches just that
     * row via the authoritative per-item read instead of re-snapshotting the
     * whole queue. `undefined` means the change is not attributable to a single
     * item (multi/reorder/bootstrap) and forces a full snapshot. */
    lastChangedId?: string;
  };
  saipen: SaipenState | null;
  /** True when the last authoritative SAIPEN read FAILED (T-018): the
   * projection is stale/unknown until a successful read clears it. */
  saipenStale: boolean;
  /** Bumped on every saipen.* / saipen.action_* event: the bar refetches the
   * authoritative snapshot + action status when it changes (watcher and
   * post-action refresh both land here — §37, §125). */
  saipenRevision: number;
  /** Bounded ring of recent canonical events (diagnostics display). */
  log: LogEntry[];
  // ---- Dock UI preferences (Phase B) ----
  // Tiny durable UI state, persisted via the core app_settings store (not
  // a second database). Ephemeral dock state stays component-local.
  dockWidth: number;
  dockCollapsed: boolean;
  activeDockTab: DockTab;
  closeQueueWhenDone: boolean;
  /** When true, plain Enter enqueues and Ctrl+Enter sends. */
  enterQueues: boolean;
  lastError: string | null;
}

export const initialState: AppState = {
  backend: "disconnected",
  lifecycle: "booting",
  version: null,
  workspaces: [],
  currentWorkspaceId: null,
  engines: [],
  stoppingEngines: {},
  startingEngines: {},
  selectedEngineId: null,
  models: [],
  selectedModelId: null,
  modelsLoading: false,
  modelsError: null,
  favoriteModelIds: [],
  favoritesOnly: false,
  sessions: [],
  activeSessionId: null,
  messages: {},
  activeMessage: {},
  historyStatus: {},
  running: {},
  runningStale: false,
  streamGaps: {},
  queue: {
    status: "stopped",
    paused: false,
    items: [],
    revision: 0,
    stale: false,
    payloadPreview: true,
    lastChangedId: undefined,
  },
  saipen: null,
  saipenStale: false,
  saipenRevision: 0,
  log: [],
  dockWidth: 360,
  dockCollapsed: false,
  activeDockTab: "activity",
  closeQueueWhenDone: false,
  enterQueues: false,
  lastError: null,
};

const MAX_LOG = 200; // bounded (law 13)

function pushLog(log: LogEntry[], entry: LogEntry): LogEntry[] {
  const next = [...log, entry];
  return next.length > MAX_LOG ? next.slice(next.length - MAX_LOG) : next;
}

/** Event families that earn a diagnostics-log entry. Streaming noise
 * (message.delta, tool.output, engine.raw_event) must NOT mutate the log —
 * otherwise every token rerenders every subscriber (TASK 16 §197, §241). */
const LOG_WORTHY = new Set<string>([
  "app.started",
  "app.stopping",
  "workspace.opened",
  "workspace.closed",
  "engine.starting",
  "engine.ready",
  "engine.stopping",
  "engine.stopped",
  "engine.failed",
  "engine.health_changed",
  "session.created",
  "session.loaded",
  "session.changed",
  "session.closed",
  "message.started",
  "message.completed",
  "message.failed",
  "message.cancelled",
  "tool.started",
  "tool.completed",
  "tool.failed",
  "permission.requested",
  "permission.resolved",
  "question.asked",
  "question.resolved",
  "queue.changed",
  "queue.dispatch_started",
  "queue.dispatch_completed",
  "queue.dispatch_failed",
  "saipen.detected",
  "saipen.changed",
  "saipen.validation_changed",
  "saipen.action_started",
  "saipen.action_completed",
  "saipen.action_failed",
  "saipen.action_cancelled",
  "runtime.warning",
  "runtime.error",
]);

export function isSessionInScope(state: AppState, sessionId: string): boolean {
  return state.sessions.some((s) => s.id === sessionId);
}

export function applyEvent(state: AppState, event: TypedEvent): AppState {
  const p = event.payload;
  const log = LOG_WORTHY.has(event.type)
    ? pushLog(state.log, {
        seq: event.seq,
        ts: event.ts,
        type: event.type,
        message: logMessage(event),
      })
    : state.log;

  switch (event.type) {
    case "app.started": {
      const version = str(p, "version");
      return { ...state, backend: "connected", lifecycle: "ready", version, log };
    }
    case "app.stopping":
      // Shutdown announced; interactive controls must be disabled (§33).
      return { ...state, lifecycle: "shutting_down", log };

    // ---- workspace ----
    case "workspace.opened": {
      const workspaceId = str(p, "workspace_id");
      if (!workspaceId) return { ...state, log };
      // NOTIFICATION / INVALIDATION ONLY (T-027): "a workspace is open at the
      // runtime level" is NOT "this workspace is what the UI shows". The one
      // authority for user-visible selection is `selectWorkspace` (its epoch
      // token commits `currentWorkspaceId` + the scoped projection). A
      // delayed `workspace.opened(A)` that lands after the user selected B
      // must never re-select A nor clear B's newer sessions/SAIPEN — that is
      // exactly the wrong-project-send seam. The `workspaces` list is also
      // not touched: the authoritative WorkspaceSummary (git + SAIPEN probe
      // truth) comes from the openWorkspace response.
      //
      // The only local effect is a projection-invalidation bump for the
      // workspace that is ALREADY current (its SAIPEN projection may have
      // been re-attached by the backend), which the SAIPEN owner refetches.
      return state.currentWorkspaceId === workspaceId
        ? { ...state, saipenRevision: state.saipenRevision + 1, log }
        : { ...state, log };
    }
    case "workspace.closed": {
      const id = str(p, "workspace_id");
      if (!id) return { ...state, log };
      // Route the clear through the selection owner (T-045): invalidates
      // in-flight selection epochs AND drops every scoped projection. A bare
      // `currentWorkspaceId = null` left old sessions/messages/SAIPEN residue.
      return { ...applyWorkspaceClosed(state, id), log };
    }

    // ---- engine ----
    case "engine.starting": {
      const id = str(p, "engine_id");
      if (!id) return { ...state, log };
      return {
        ...setEngineHealth(state, id, "starting", log),
        startingEngines: { ...state.startingEngines, [id]: true },
      };
    }
    case "engine.ready": {
      const id = str(p, "engine_id");
      const stopping = { ...state.stoppingEngines };
      const starting = { ...state.startingEngines };
      if (id) delete stopping[id];
      if (id) delete starting[id];
      return {
        ...setEngineHealth(state, id, "ready", log),
        stoppingEngines: stopping,
        startingEngines: starting,
      };
    }
    case "engine.stopping": {
      // Transitional, NOT the stopped terminal: keep prior health, mark the
      // engine non-startable/stopping until the authoritative terminal
      // (TASK 24 §9).
      const id = str(p, "engine_id");
      if (!id) return { ...state, log };
      const starting = { ...state.startingEngines };
      delete starting[id];
      return { ...state, stoppingEngines: { ...state.stoppingEngines, [id]: true }, startingEngines: starting, log };
    }
    case "engine.stopped": {
      const id = str(p, "engine_id");
      const stopping = { ...state.stoppingEngines };
      const starting = { ...state.startingEngines };
      if (id) delete stopping[id];
      if (id) delete starting[id];
      return {
        // The SELECTED engine leaving READY invalidates its model projection
        // immediately (TASK 24 §9): a stale id must never reach a Send after
        // a restart/provider change.
        ...(id && id === state.selectedEngineId
          ? {
              ...setEngineHealth(state, id, "stopped", log),
              models: [],
              selectedModelId: null,
              modelsLoading: false,
              modelsError: null,
            }
          : setEngineHealth(state, id, "stopped", log)),
        stoppingEngines: stopping,
        startingEngines: starting,
      };
    }
    case "engine.failed": {
      const engineId = str(p, "engine_id");
      const stopping = { ...state.stoppingEngines };
      const starting = { ...state.startingEngines };
      if (engineId) delete stopping[engineId];
      if (engineId) delete starting[engineId];
      const base = setEngineHealth(
        state,
        engineId,
        { kind: "failed", message: str(p, "error") ?? "unknown" },
        log,
      );
      return {
        ...(engineId && engineId === state.selectedEngineId
          ? {
              ...base,
              models: [],
              selectedModelId: null,
              modelsLoading: false,
              modelsError: null,
            }
          : base),
        stoppingEngines: stopping,
        startingEngines: starting,
      };
    }
    case "engine.health_changed": {
      const engineId = str(p, "engine_id");
      const healthy = p.healthy === true;
      return setEngineHealth(
        state,
        engineId,
        healthy ? "ready" : { kind: "degraded", message: "health check failed" },
        log,
      );
    }

    // ---- session ----
    case "session.created": {
      // The event carries the FULL authoritative DTO (SessionManager is the
      // sole lifecycle publisher, TASK 24 §9) — the reducer never fabricates
      // workspace/upstream-id/display-name from local UI state. Deduped by
      // generic session id so event-before-response ordering cannot create
      // duplicates; the command-returned DTO (upserted by SessionList) is
      // identical by construction.
      const sessionId = str(p, "session_id");
      if (!sessionId) return { ...state, log };
      // SCOPE FILTER (CORE-008): a session created in another workspace must never
      // enter or auto-activate in the visible current-workspace list. A
      // session.created(A) while B is current is ignored entirely.
      const createdWsId =
        typeof p.workspace_id === "string" && p.workspace_id !== "" ? p.workspace_id : null;
      if (createdWsId !== state.currentWorkspaceId) {
        return { ...state, log };
      }
      const session: Session = {
        id: sessionId,
        workspace_id:
          typeof p.workspace_id === "string" && p.workspace_id !== "" ? p.workspace_id : null,
        engine_id: str(p, "engine_id") ?? "?",
        engine_session_id: str(p, "engine_session_id") ?? "",
        display_name: str(p, "display_name") ?? sessionId,
        created_at: typeof p.created_at === "number" ? p.created_at : event.ts,
        running: false,
        // Authoritative from the event (TASK 24 §9): resumable ≠ usable-now.
        // A fresh connection-owned (resume=false) session is usable NOW even
        // though it is not restart-resumable — the reducer must never
        // fabricate `resumable: true` (that made first-prompt usability
        // event-order dependent).
        resumable: p.resumable === true,
        usable_now: p.usable_now !== false,
      };
      return {
        ...state,
        sessions: upsertSessionList(state.sessions, session),
        // A delayed create from a previously selected engine remains visible
        // in the project list but must not hijack the active thread after the
        // user switched engines while the upstream create was in flight.
        activeSessionId:
          state.activeSessionId ?? (session.engine_id === state.selectedEngineId ? sessionId : null),
        messages: { ...state.messages, [sessionId]: state.messages[sessionId] ?? [] },
        log,
      };
    }
    case "session.loaded":
    case "session.changed": {
      const sessionId = str(p, "session_id");
      if (!sessionId || !isSessionInScope(state, sessionId)) return { ...state, log };
      return {
        ...state,
        sessions: state.sessions.map((s) => (s.id === sessionId ? { ...s, running: s.running } : s)),
        log,
      };
    }
    case "session.closed": {
      const sessionId = str(p, "session_id");
      if (!sessionId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const sessions = state.sessions.filter((s) => s.id !== sessionId);
      const messages = { ...state.messages };
      delete messages[sessionId];
      const activeMessage = { ...state.activeMessage };
      delete activeMessage[sessionId];
      return {
        ...state,
        sessions,
        messages,
        activeMessage,
        activeSessionId: state.activeSessionId === sessionId ? (sessions[0]?.id ?? null) : state.activeSessionId,
        log,
      };
    }

    // ---- message stream ----
    case "message.started": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      if (!sessionId || !runId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const message: Message = {
        id: `${runId}-assistant`,
        role: "assistant",
        runId,
        status: "streaming",
        text: "",
        tools: [],
        permissions: [], questions: [],
        ts: event.ts,
      };
      // The engine authoritatively accepted THIS run: only the UNCERTAIN
      // user turn bound to THIS run_id is proven delivered — an unrelated
      // new run must never clear an older ambiguous prompt (TASK 24 §9).
      // Event-before-command-response: the turn is marked uncertain (with
      // its run_id) only when the send response lands, so this filter is
      // exact in both orderings.
      const completed = clearUncertainIn(state.messages[sessionId] ?? [], runId);
      const previousTail = state.activeMessage[sessionId] ?? null;
      // Re-delivered `message.started` for the SAME run is idempotent: never
      // discard the text already streamed into the live tail.
      if (previousTail !== null && previousTail.runId === runId) {
        return {
          ...state,
          messages:
            completed === state.messages[sessionId]
              ? state.messages
              : { ...state.messages, [sessionId]: completed },
          running: { ...state.running, [sessionId]: runId },
          sessions: state.sessions.map((s) => (s.id === sessionId ? { ...s, running: true } : s)),
          log,
        };
      }
      // A new run starting while an older tail never reached a terminal: the
      // older turn is committed as UNPROVEN rather than silently dropped
      // (its text is real; only its outcome is unknown).
      const carried =
        previousTail !== null
          ? [
              ...completed,
              previousTail.status === "streaming"
                ? {
                    ...previousTail,
                    status: "outcome_unknown" as MessageStatus,
                    error:
                      previousTail.error ??
                      "outcome unknown — no terminal arrived before the next run started",
                  }
                : previousTail,
            ]
          : completed;
      return {
        ...state,
        messages: { ...state.messages, [sessionId]: carried },
        activeMessage: { ...state.activeMessage, [sessionId]: message },
        running: { ...state.running, [sessionId]: runId },
        sessions: state.sessions.map((s) => (s.id === sessionId ? { ...s, running: true } : s)),
        log,
      };
    }
    case "message.delta": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      const delta = str(p, "delta");
      if (!sessionId || !runId || delta === null || !isSessionInScope(state, sessionId)) return { ...state, log };
      // PERF-002: the watermark lives on the message (see `deltaSeqFor`), not a
      // lifetime-global map. A re-delivered envelope (same or lower seq) is
      // dropped BEFORE append, so a lag-replay cannot double-write transcript
      // text and the per-token reducer cost no longer scales with historical
      // run count.
      const lastDelta = deltaSeqFor(state, sessionId, runId);
      if (event.seq <= lastDelta) return state;
      // HOT PATH: only the active tail (or the matching committed message) is
      // rebuilt; the completed transcript's identity is untouched, so per-token
      // cost does not scale with history length.
      return {
        ...state,
        ...patchStream(state, sessionId, runId, (m) => ({
          ...m,
          text: m.text + delta,
          lastDeltaSeq: event.seq,
        })),
        log,
      };
    }
    case "message.completed": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      if (!sessionId || !runId || !isSessionInScope(state, sessionId)) return { ...state, log };
      // TASK 24 §9: a stale terminal from an older run is a historical fact
      // for that message only — it must never clear the running state of a
      // NEWER run the backend still owns.
      const clearsRun = state.running[sessionId] === runId;
      return {
        ...state,
        ...commitTerminal(state, sessionId, runId, (m) => ({ ...m, status: "complete" })),
        running: clearsRun ? { ...state.running, [sessionId]: null } : state.running,
        sessions: clearsRun
          ? state.sessions.map((s) => (s.id === sessionId ? { ...s, running: false } : s))
          : state.sessions,
        log,
      };
    }
    case "message.failed": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      if (!sessionId || !runId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const clearsRun = state.running[sessionId] === runId;
      return {
        ...state,
        ...commitTerminal(state, sessionId, runId, (m) =>
          finalizeMessage(m, "failed", str(p, "error") ?? "unknown error"),
        ),
        running: clearsRun ? { ...state.running, [sessionId]: null } : state.running,
        sessions: clearsRun
          ? state.sessions.map((s) => (s.id === sessionId ? { ...s, running: false } : s))
          : state.sessions,
        log,
      };
    }
    case "message.cancelled": {
      // §37/§63: cancellation is a real terminal outcome — the projection must
      // leave the streaming state, exactly like failed/completed.
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      if (!sessionId || !runId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const clearsRun = state.running[sessionId] === runId;
      return {
        ...state,
        ...commitTerminal(state, sessionId, runId, (m) => finalizeMessage(m, "cancelled", null)),
        running: clearsRun ? { ...state.running, [sessionId]: null } : state.running,
        sessions: clearsRun
          ? state.sessions.map((s) => (s.id === sessionId ? { ...s, running: false } : s))
          : state.sessions,
        log,
      };
    }
    case "message.outcome_unknown": {
      // TASK 24 §9: the backend's terminal outcome is unprovable — the run
      // may still be live upstream. The message leaves the streaming state
      // and shows the ambiguity, but the session/workspace reservation is
      // PRESERVED (no fake completion, no blind resend); a later matching
      // authoritative terminal or explicit resolution clears it.
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      if (!sessionId || !runId || !isSessionInScope(state, sessionId)) return { ...state, log };
      // NOT a terminal: the tail stays ACTIVE (the run may still stream), so
      // later authoritative facts still land on the same turn.
      return {
        ...state,
        ...patchStream(state, sessionId, runId, (m) => ({
          ...m,
          status: "outcome_unknown",
          error: str(p, "error") ?? "outcome unknown — the run may still be executing upstream",
        })),
        log,
      };
    }

    // ---- tools ----
    // Tool lifecycle facts are keyed by run_id + tool_call_id (TASK 24 §9):
    // two same-named tools in one run are two independent cards with isolated
    // output/error/terminal state.
    case "tool.started": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      const tool = str(p, "tool");
      const toolCallId = str(p, "tool_call_id");
      if (!sessionId || !tool || !isSessionInScope(state, sessionId)) return { ...state, log };
      const effRunId = runId ?? activeRun(state, sessionId);
      if (!effRunId) return { ...state, log };
      const id = toolCallId ?? `${tool}-${effRunId}`;
      return {
        ...state,
        ...patchStream(state, sessionId, effRunId, (m) => ({
          ...m,
          tools: upsertTool(m.tools, { id, tool, status: "started", output: "", ts: event.ts }),
        })),
        log,
      };
    }
    case "tool.output": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      const tool = str(p, "tool");
      const toolCallId = str(p, "tool_call_id");
      const output = str(p, "output");
      if (!sessionId || !tool || output === null || !isSessionInScope(state, sessionId)) return { ...state, log };
      const effRunId = runId ?? activeRun(state, sessionId);
      if (!effRunId) return { ...state, log };
      const id = toolCallId ?? `${tool}-${effRunId}`;
      // PERF-002: the watermark lives on the tool (see `toolSeqFor`), not a
      // lifetime-global map. Same seq guard as message.delta (T-026).
      const lastTool = toolSeqFor(state, sessionId, effRunId, id);
      if (event.seq <= lastTool) return state;
      return {
        ...state,
        ...patchStream(state, sessionId, effRunId, (m) => ({
          ...m,
          tools: m.tools.map((t) =>
            t.id === id
              ? {
                  ...t,
                  status: "output",
                  output: appendBounded(t.output, output, MAX_TOOL_OUTPUT_UI),
                  lastToolSeq: event.seq,
                }
              : t,
          ),
        })),
        log,
      };
    }
    case "tool.completed": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      const tool = str(p, "tool");
      const toolCallId = str(p, "tool_call_id");
      if (!sessionId || !tool || !isSessionInScope(state, sessionId)) return { ...state, log };
      const effRunId = runId ?? activeRun(state, sessionId);
      if (!effRunId) return { ...state, log };
      const id = toolCallId ?? `${tool}-${effRunId}`;
      return {
        ...state,
        ...patchStream(state, sessionId, effRunId, (m) => ({
          ...m,
          tools: m.tools.map((t) => (t.id === id ? { ...t, status: "completed" } : t)),
        })),
        log,
      };
    }
    case "tool.failed": {
      const sessionId = str(p, "session_id");
      const runId = str(p, "run_id");
      const tool = str(p, "tool");
      const toolCallId = str(p, "tool_call_id");
      if (!sessionId || !tool || !isSessionInScope(state, sessionId)) return { ...state, log };
      const effRunId = runId ?? activeRun(state, sessionId);
      if (!effRunId) return { ...state, log };
      const id = toolCallId ?? `${tool}-${effRunId}`;
      return {
        ...state,
        ...patchStream(state, sessionId, effRunId, (m) => ({
          ...m,
          tools: m.tools.map((t) =>
            t.id === id ? { ...t, status: "failed", error: str(p, "error") ?? "failed" } : t,
          ),
        })),
        log,
      };
    }

    // ---- permissions ----
    // Routing identity is the event's OWN run_id (TASK 24 §9): a delayed
    // permission from run A must never be attached to run B because the
    // session advanced. The active-run fallback exists only for legacy
    // payloads without run_id; new events always carry it.
    case "permission.requested": {
      const sessionId = str(p, "session_id");
      const requestId = str(p, "request_id");
      if (!sessionId || !requestId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const runId = str(p, "run_id") ?? activeRun(state, sessionId);
      if (!runId) return { ...state, log };
      return {
        ...state,
        ...patchStream(state, sessionId, runId, (m) => ({
          ...m,
          permissions: [...m.permissions, { requestId, detail: str(p, "detail") ?? "", allowed: null }],
        })),
        log,
      };
    }
    case "permission.resolved": {
      const sessionId = str(p, "session_id");
      const requestId = str(p, "request_id");
      if (!sessionId || !requestId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const runId = str(p, "run_id") ?? activeRun(state, sessionId);
      if (!runId) return { ...state, log };
      const allowed = p.allowed === true;
      return {
        ...state,
        ...patchStream(state, sessionId, runId, (m) => ({
          ...m,
          permissions: m.permissions.map((pe) =>
            pe.requestId === requestId ? { ...pe, allowed } : pe,
          ),
        })),
        log,
      };
    }

    // ---- questions (AUDIT-CORE-002) ----
    // Same run-scoped routing rule as permissions: the event's OWN run_id is
    // the routing identity; the active-run fallback exists only for legacy
    // payloads.
    case "question.asked": {
      const sessionId = str(p, "session_id");
      const requestId = str(p, "request_id");
      if (!sessionId || !requestId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const runId = str(p, "run_id") ?? activeRun(state, sessionId);
      if (!runId) return { ...state, log };
      return {
        ...state,
        ...patchStream(state, sessionId, runId, (m) => ({
          ...m,
          questions: [
            ...m.questions.filter((q) => q.requestId !== requestId),
            { requestId, detail: str(p, "detail") ?? "", resolved: null },
          ],
        })),
        log,
      };
    }
    case "question.resolved": {
      const sessionId = str(p, "session_id");
      const requestId = str(p, "request_id");
      if (!sessionId || !requestId || !isSessionInScope(state, sessionId)) return { ...state, log };
      const runId = str(p, "run_id") ?? activeRun(state, sessionId);
      if (!runId) return { ...state, log };
      return {
        ...state,
        ...patchStream(state, sessionId, runId, (m) => ({
          ...m,
          // The card's job is done once the authoritative resolution lands:
          // remove it instead of keeping a stale open question rendered.
          questions: m.questions.filter((q) => q.requestId !== requestId),
        })),
        log,
      };
    }

    // ---- queue (durable truth; events only announce a committed transition) ----
    // `queue.changed` is the SOLE snapshot invalidation (TASK 24 perf): the
    // same durable transition commonly also publishes queue.dispatch_*, and
    // bumping the revision for both would launch duplicate full snapshots.
    // Dispatch-specific events are activity/log only.
    case "queue.changed": {
      // PERF-004: remember the single item this change is attributable to (if
      // the core named one) so the queue owner can patch just that row.
      const changedId = (event.payload as { item_id?: string }).item_id;
      return {
        ...state,
        queue: {
          ...state.queue,
          revision: state.queue.revision + 1,
          lastChangedId: changedId,
        },
        log,
      };
    }
    case "queue.dispatch_started":
    case "queue.dispatch_completed":
    case "queue.dispatch_failed":
      return { ...state, log };

    // ---- saipen (watcher facts; the bar refetches the authoritative
    // snapshot/action status — the store never holds a second SAIPEN truth) ----
    case "saipen.detected":
    case "saipen.changed":
    case "saipen.validation_changed": {
      // PERF-005: only the CURRENT workspace may drive the SaipenBar reads. A
      // change in a non-current retained workspace must not trigger
      // current-workspace IPC refetches.
      const wid = str(p, "workspace_id");
      return wid === state.currentWorkspaceId
        ? { ...state, saipenRevision: state.saipenRevision + 1, log }
        : { ...state, log };
    }

    // ---- saipen actions (TASK 15 §58–§60): revision bump only — action
    // records come from the backend `saipen_action_status` command ----
    case "saipen.action_started":
    case "saipen.action_completed":
    case "saipen.action_failed":
    case "saipen.action_cancelled": {
      const wid = str(p, "workspace_id");
      return wid === state.currentWorkspaceId
        ? { ...state, saipenRevision: state.saipenRevision + 1, log }
        : { ...state, log };
    }

    // ---- runtime ----
    // A warning (e.g. Harness stream overflow) is logged but must NEVER
    // erase an independent actionable lastError (TASK 24 §9): only a newer
    // runtime.error or an explicit dismiss changes lastError.
    case "runtime.warning":
      return { ...state, log };
    case "runtime.error":
      return { ...state, log, lastError: str(p, "message") ?? null };
    default:
      // Unknown events (incl. engine.raw_event) are preserved in the log and
      // must never crash the reducer (malformed-event gate).
      return { ...state, log };
  }
}

function setEngineHealth(
  state: AppState,
  engineId: string | null,
  health: EngineHealth,
  log: LogEntry[],
): AppState {
  if (!engineId) return { ...state, log };
  return {
    ...state,
    engines: state.engines.map((e) => (e.id === engineId ? { ...e, health } : e)),
    log,
  };
}

/**
 * Project the local user turn into the session's conversation with a stable
 * pending id, BEFORE the external send (TASK 24 §9): the assistant stream
 * (`message.started`/deltas) may arrive before the invoke promise resolves,
 * and the conversation must always read user → assistant. Returns the id so
 * the caller can drop the turn on a definite rejection (never on cancel —
 * the prompt was delivered).
 */
export function addLocalUserMessage(sessionId: string, text: string): string {
  const id = `user-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  store.patch((s) => ({
    ...s,
    messages: {
      ...s.messages,
      [sessionId]: [
        ...(s.messages[sessionId] ?? []),
        { id, role: "user", runId: "", status: "complete", text, tools: [], permissions: [], questions: [], ts: Date.now(), uncertain: false },
      ],
    },
  }));
  return id;
}

/** Drop a pending local user turn (definite send rejection only). */
export function removeLocalUserMessage(sessionId: string, id: string): void {
  store.patch((s) => ({
    ...s,
    messages: {
      ...s.messages,
      [sessionId]: (s.messages[sessionId] ?? []).filter((m) => m.id !== id),
    },
  }));
}

/** Mark a local user turn UNCERTAIN, bound to the returned RunId (send
 * outcome unproven — the run may still be executing upstream). The turn
 * stays visible; it is cleared ONLY by execution evidence / a definitive
 * terminal for the SAME run_id — an unrelated new run can never falsely
 * prove an older ambiguous prompt delivered (TASK 24 §9). */
export function markUserMessageUncertain(sessionId: string, id: string, runId: string): void {
  store.patch((s) => ({
    ...s,
    messages: {
      ...s.messages,
      [sessionId]: (s.messages[sessionId] ?? []).map((m) =>
        m.id === id ? { ...m, uncertain: true, uncertainRunId: runId } : m,
      ),
    },
  }));
}

/** Monotonic generation for favorite-state mutations (W2-005): lifted from
 * TitleBar so the cold-bootstrap hydration AND the toggle writes share ONE
 * counter. A newer write (higher generation) always wins; an older failed
 * write must never clobber a newer durable/UI set. */
let favoritesWriteGen = 0;

/** Current favorite-mutation generation (read-only snapshot for rollback guards). */
export function favoritesGen(): number {
  return favoritesWriteGen;
}

/** Claim the next favorite-mutation generation. Callers capture the returned
 * value and, on a failed backend write, revert only if `favoritesGen()` still
 * equals it — i.e. no newer write has superseded them. */
export function nextFavoritesGen(): number {
  favoritesWriteGen += 1;
  return favoritesWriteGen;
}

/** Replace the authoritative favorites set (bootstrap load). Bumps the shared
 * generation so a bootstrap that resolves LATE (after the user already toggled)
 * cannot clobber the newer optimistic set — callers also guard on
 * `favoritesGen()` before applying (see frontendSync.runColdBootstrap). W2-005. */
export function setFavorites(ids: string[]): void {
  favoritesWriteGen += 1;
  store.patch((s) => ({ ...s, favoriteModelIds: ids }));
}

/** The favorites set is bounded (app authority caps at 50): the toggle
 * never grows it past the cap — the UI mirrors the backend bound so the
 * optimistic projection cannot drift from the persisted truth. */
export const MAX_FAVORITES_UI = 50;

/** Optimistic toggle of one model favorite; the caller persists through
 * the backend and rolls back on failure (CodeNomad-style preference UX). */
export function toggleFavoriteModel(id: string): string[] {
  let next: string[] = [];
  store.patch((s) => {
    const has = s.favoriteModelIds.includes(id);
    next = has
      ? s.favoriteModelIds.filter((x) => x !== id)
      : s.favoriteModelIds.length >= MAX_FAVORITES_UI
        ? s.favoriteModelIds
        : [...s.favoriteModelIds, id];
    return { ...s, favoriteModelIds: next };
  });
  return next;
}

export function setFavoritesOnly(on: boolean): void {
  store.patch((s) => ({ ...s, favoritesOnly: on }));
}

/** Optimistic starting projection (mirror of the event-driven path): set on
 * the Start click so the button flips to "Starting…" INSTANTLY, before any
 * round-trip. Cleared by the authoritative terminal event or by the caller
 * when the start command resolves (whichever wins first is idempotent). */
export function markEngineStarting(id: string, on: boolean): void {
  store.patch((s) => {
    const startingEngines = { ...s.startingEngines };
    if (on) startingEngines[id] = true;
    else delete startingEngines[id];
    return { ...s, startingEngines };
  });
}

/** Restore the engine's authoritative session history (TASK 24 §9): a
 * resumed/restarted session must show its real user/assistant/tool order
 * from the engine — never a fabricated empty thread, never a SQLite
 * transcript mirror. Reconciled with the LIVE projection by stable message
 * / tool identities: an existing message (a live turn that arrived while
 * the history read was delayed) is kept, never duplicated; history-only
 * messages are appended in authoritative order; live-only messages follow.
 * Never a SQLite transcript mirror. */
export function hydrateSessionHistory(
  sessionId: string,
  history: SessionHistoryMessage[],
): void {
  store.patch((s) => {
    const existing = s.messages[sessionId] ?? [];
    // The live streaming tail is NOT part of the completed transcript and is
    // never merged/duplicated here: it stays in `activeMessage` and is
    // committed by its own terminal (its id is excluded below).
    const liveTailId = s.activeMessage[sessionId]?.id ?? null;
    const existingById = new Map(existing.map((m) => [m.id, m]));
    const merged: Message[] = [];
    const seenIds = new Set<string>();
    // AUDIT-CORE-005: exact text equality is NOT message identity. The old
    // global text-only suppression dropped a newly committed optimistic user
    // turn whenever ANY older history turn shared its text (e.g. repeated
    // `continue` prompts against a lagging snapshot) — deleting a legitimate
    // distinct send. Live turns are kept unless a stable backend-provided
    // correlation proves they are the same send; until such correlation
    // exists a temporary visual duplicate during reconciliation is preferred
    // over silently losing a real turn.
    for (const h of history) {
      if (h.role === "tool") {
        // Attach the tool to the previous assistant slot (history-only tools
        // always follow their run's assistant turn). If that assistant
        // message already exists live, attach by tool identity instead of
        // duplicating it.
        const tool: ToolActivity = {
          id: h.tool_call_id || h.id,
          tool: h.tool || "tool",
          status: "completed",
          output: h.text,
          ts: h.ts && h.ts > 0 ? h.ts : Date.now(),
        };
        let attached = false;
        for (let i = merged.length - 1; i >= 0 && !attached; i--) {
          const m = merged[i];
          if (!m || m.role !== "assistant") continue;
          if (!m.tools.some((t) => t.id === tool.id)) m.tools.push(tool);
          attached = true;
        }
        if (!attached) {
          const id = h.id;
          seenIds.add(id);
          merged.push({
            id,
            role: "assistant",
            runId: "",
            status: "complete",
            text: "",
            tools: [tool],
            permissions: [], questions: [],
            ts: h.ts && h.ts > 0 ? h.ts : Date.now(),
          });
        }
        continue;
      }
      const id = h.id;
      if (id === liveTailId) continue; // owned by the live tail, not history
      seenIds.add(id);
      const live = existingById.get(id);
      if (live) {
        // The live version is authoritative over the history snapshot
        // (streaming content / status): keep it as-is — the message's own
        // id dedupes, so the old history cannot double it.
        merged.push(live);
        continue;
      }
      merged.push({
        id,
        role: h.role === "user" ? "user" : "assistant",
        runId: "",
        status: "complete",
        text: h.text,
        tools: [],
        permissions: [], questions: [],
        ts: h.ts && h.ts > 0 ? h.ts : Date.now(),
      });
    }
    // AUDIT-CORE-005: append committed live turns NOT present in history.
    // Id-based dedup only; text is never treated as identity (see above).
    for (const m of existing) {
      if (seenIds.has(m.id)) continue;
      merged.push(m);
    }
    // AUDIT-CORE-003: the live tail is NOT appended to the completed
    // transcript here — `activeMessage` is its ONLY owner and Conversation
    // renders both slices, so appending it would display the streaming turn
    // twice and `commitTerminal` would later commit a second durable copy.
    // The tail survives hydration untouched (its id is excluded from the
    // history merge above) and transitions exactly once, at its terminal.
    return {
      ...s,
      historyStatus: { ...s.historyStatus, [sessionId]: "available" },
      messages: { ...s.messages, [sessionId]: merged },
    };
  });
}

/** Rebuild the live pending-permission cards from the authoritative snapshot
 * (W2-004). Called by `frontend.reconcile` after a bounded-bus lag, when a
 * `permission.requested` state event may have been dropped. For each open
 * request (exact session/run/request ownership) it ensures the owning message
 * carries the card; an already-locally-resolved entry keeps its decision, so a
 * repeated reconciliation never resets a user's Allow/Deny. A request that was
 * resolved or terminalized before the snapshot is simply absent and therefore
 * never resurrected. */
export function reconcilePendingPermissions(
  pending: { session_id: string; run_id: string; request_id: string; detail: string }[],
): void {
  // CORE-004: a successful snapshot (INCLUDING an empty one) is the EXACT set
  // of currently open requests. For every in-scope session, upsert the
  // authoritative open requests and DROP any unresolved (allowed === null)
  // card whose request_id is absent from the snapshot — it is no longer
  // actionable. Resolved cards (allowed set) are kept. No Allow/Deny decision
  // is ever invented from absence.
  store.patch((s) => {
    // Authoritative open request_ids per in-scope session.
    const openBySession = new Map<string, Set<string>>();
    for (const p of pending) {
      if (!isSessionInScope(s, p.session_id)) continue;
      const set = openBySession.get(p.session_id) ?? new Set<string>();
      set.add(p.request_id);
      openBySession.set(p.session_id, set);
    }
    // Sessions to reconcile: those named in the snapshot PLUS every in-scope
    // session that already has a stream, so an EMPTY successful snapshot still
    // clears stale unresolved cards (the old code did nothing on empty).
    const sessionIds = new Set<string>(openBySession.keys());
    for (const k of Object.keys(s.messages)) sessionIds.add(k);
    for (const k of Object.keys(s.activeMessage)) sessionIds.add(k);

    let next = s;
    for (const sessionId of sessionIds) {
      if (!isSessionInScope(next, sessionId)) continue;
      const openIds = openBySession.get(sessionId);
      const runId =
        pending.find((p) => p.session_id === sessionId)?.run_id ??
        activeRun(next, sessionId);
      if (!runId) continue;
      next = {
        ...next,
        ...patchStream(next, sessionId, runId, (m) => {
          let permissions = m.permissions;
          // Upsert the authoritative open requests.
          if (openIds) {
            for (const p of pending) {
              if (p.session_id !== sessionId) continue;
              if (!permissions.some((pe) => pe.requestId === p.request_id)) {
                permissions = [
                  ...permissions,
                  { requestId: p.request_id, detail: p.detail, allowed: null },
                ];
              }
            }
          }
          // Drop unresolved cards absent from the authoritative snapshot.
          permissions = permissions.filter((pe) =>
            pe.allowed !== null ? true : (openIds?.has(pe.requestId) ?? false),
          );
          return { ...m, permissions };
        }),
      };
    }
    return next;
  });
}

/** AUDIT-CORE-002: rebuild the live pending-question cards from the
 * authoritative snapshot. Same exact-set contract as
 * `reconcilePendingPermissions`: upsert authoritative open questions, drop
 * unresolved cards absent from the snapshot, never invent a resolution. */
export function reconcilePendingQuestions(
  pending: { session_id: string; run_id: string; request_id: string; detail: string }[],
): void {
  store.patch((s) => {
    const openBySession = new Map<string, Set<string>>();
    for (const p of pending) {
      if (!isSessionInScope(s, p.session_id)) continue;
      const set = openBySession.get(p.session_id) ?? new Set<string>();
      set.add(p.request_id);
      openBySession.set(p.session_id, set);
    }
    const sessionIds = new Set<string>(openBySession.keys());
    for (const k of Object.keys(s.messages)) sessionIds.add(k);
    for (const k of Object.keys(s.activeMessage)) sessionIds.add(k);

    let next = s;
    for (const sessionId of sessionIds) {
      if (!isSessionInScope(next, sessionId)) continue;
      const openIds = openBySession.get(sessionId);
      const runId =
        pending.find((p) => p.session_id === sessionId)?.run_id ??
        activeRun(next, sessionId);
      if (!runId) continue;
      next = {
        ...next,
        ...patchStream(next, sessionId, runId, (m) => {
          let questions = m.questions;
          if (openIds) {
            for (const p of pending) {
              if (p.session_id !== sessionId) continue;
              if (!questions.some((q) => q.requestId === p.request_id)) {
                questions = [
                  ...questions,
                  { requestId: p.request_id, detail: p.detail, resolved: null },
                ];
              }
            }
          }
          // Drop unresolved cards absent from the authoritative snapshot.
          questions = questions.filter((q) =>
            q.resolved !== null ? true : (openIds?.has(q.requestId) ?? false),
          );
          return { ...m, questions };
        }),
      };
    }
    return next;
  });
}

/** W2-003: discard a stale live streaming tail for a session that has been
 * proven non-running after a bounded-bus lag. The authoritative engine history
 * is reloaded separately; without this clear the old partial tail would remain
 * visibly streaming even though the run is over. */
export function clearActiveTail(sessionId: string): void {
  store.patch((s) => {
    if (s.activeMessage[sessionId] === undefined && s.streamGaps[sessionId] === undefined) {
      return s;
    }
    const activeMessage = { ...s.activeMessage };
    delete activeMessage[sessionId];
    const streamGaps = { ...s.streamGaps };
    delete streamGaps[sessionId];
    return { ...s, activeMessage, streamGaps };
  });
}

/** W2-003: set/clear the per-session stream-gap marker. */
export function markStreamGap(sessionId: string, on: boolean): void {
  store.patch((s) => {
    const streamGaps = { ...s.streamGaps };
    if (on) streamGaps[sessionId] = true;
    else delete streamGaps[sessionId];
    return { ...s, streamGaps };
  });
}

/** Record the authoritative-history state of a session (TASK 24 §9):
 * loading / available / unavailable (no engine capability) / error (read
 * failed). Never fabricates an empty conversation. */
export function setHistoryStatus(
  sessionId: string,
  status: "loading" | "available" | "unavailable" | "error",
): void {
  store.patch((s) => ({
    ...s,
    historyStatus: { ...s.historyStatus, [sessionId]: status },
  }));
}

/** Upsert the authoritative Session DTO (dedupe by generic id). The reducer's
 * `session.created` row and the command-returned DTO are identical by
 * construction — whichever lands first wins, the other replaces it. */
export function upsertSession(session: Session): void {
  store.patch((s) => ({
    ...s,
    sessions: upsertSessionList(s.sessions, session),
    activeSessionId:
      s.activeSessionId ?? (session.engine_id === s.selectedEngineId ? session.id : null),
    messages: {
      ...s.messages,
      [session.id]: s.messages[session.id] ?? [],
    },
  }));
}

function upsertSessionList(sessions: Session[], session: Session): Session[] {
  const idx = sessions.findIndex((x) => x.id === session.id);
  if (idx >= 0) {
    const next = [...sessions];
    next[idx] = session;
    return next;
  }
  return [session, ...sessions];
}

/** Upsert a tool card by its stable call id (TASK 24 §9). */
function upsertTool(tools: ToolActivity[], tool: ToolActivity): ToolActivity[] {
  const idx = tools.findIndex((t) => t.id === tool.id);
  if (idx >= 0) {
    const next = [...tools];
    next[idx] = tool;
    return next;
  }
  return [...tools, tool];
}

export function upsertWorkspace(workspaces: WorkspaceSummary[], w: WorkspaceSummary): WorkspaceSummary[] {
  const idx = workspaces.findIndex((x) => x.id === w.id);
  if (idx >= 0) {
    const next = [...workspaces];
    next[idx] = w;
    return next;
  }
  return [w, ...workspaces];
}

function activeRun(state: AppState, sessionId: string): string | null {
  return state.running[sessionId] ?? null;
}

/** Cumulative tool output shown in the UI is bounded (§80): a tool that emits
 * a huge or endless stream cannot grow the projection without limit. The
 * backend also caps per-event output; this caps the accumulation. */
const MAX_TOOL_OUTPUT_UI = 512 * 1024;

function appendBounded(current: string, delta: string, max: number): string {
  if (current.length >= max) return current;
  const room = max - current.length;
  if (delta.length <= room) return current + delta;
  return current + delta.slice(0, room) + "\n…(tool output truncated)";
}

/** Run-terminal projection (§35–§36): a failed/cancelled run must not leave
 * tools eternally "started" or permissions eternally "waiting…". Non-terminal
 * tools become interrupted; pending permissions resolve as unavailable. */
function finalizeMessage(m: Message, status: "failed" | "cancelled", error: string | null): Message {
  return {
    ...m,
    status,
    error: error ?? (status === "cancelled" ? "cancelled" : m.error),
    tools: m.tools.map((t) =>
      t.status === "completed" || t.status === "failed"
        ? t
        : { ...t, status: "failed", error: t.error ?? "run interrupted" },
    ),
    permissions: m.permissions.map((pe) =>
      pe.allowed === null ? { ...pe, allowed: false } : pe,
    ),
    questions: m.questions.map((q) =>
      q.resolved === null ? { ...q, resolved: false } : q,
    ),
  };
}

/** Clear the UNCERTAIN marker on the user turn bound to `runId` — matching
 * execution evidence / definitive terminal proves that exact prompt was
 * delivered; unrelated runs never clear it (TASK 24 §9). */
function clearUncertainForRun(
  messages: Record<string, Message[]>,
  sessionId: string,
  runId: string,
): Record<string, Message[]> {
  const list = messages[sessionId] ?? [];
  const next = clearUncertainIn(list, runId);
  return next === list ? messages : { ...messages, [sessionId]: next };
}

/** Same rule, on one already-materialized transcript (identity preserved when
 * nothing matches, so the hot path never invalidates the completed slice). */
function clearUncertainIn(list: Message[], runId: string): Message[] {
  if (!list.some((m) => m.role === "user" && m.uncertain && m.uncertainRunId === runId)) {
    return list;
  }
  return list.map((m) =>
    m.role === "user" && m.uncertain && m.uncertainRunId === runId
      ? { ...m, uncertain: false, uncertainRunId: undefined }
      : m,
  );
}

/** The two stream-owning slices. Every stream reducer returns exactly this
 * pair so a fact can never update one without the other. */
type StreamSlices = Pick<AppState, "messages" | "activeMessage">;

/** Apply an in-run fact (delta/tool/permission/ambiguity).
 *
 * INVARIANT: while the run owning `runId` is the live tail, ONLY
 * `activeMessage` is rebuilt — the completed transcript keeps its identity,
 * so the cost is O(1) in history length. A fact for an already-committed run
 * (late delivery after its terminal) patches the committed turn instead; that
 * path is rare and may copy the transcript. A fact for a run with no turn at
 * all (delta before `message.started`) OPENS a tail rather than dropping real
 * engine output. */
function patchStream(
  state: AppState,
  sessionId: string,
  runId: string,
  patch: (m: Message) => Message,
): StreamSlices {
  const tail = state.activeMessage[sessionId] ?? null;
  if (tail !== null && tail.runId === runId) {
    return {
      messages: state.messages,
      activeMessage: { ...state.activeMessage, [sessionId]: patch(tail) },
    };
  }
  const list = state.messages[sessionId] ?? [];
  if (list.some((m) => m.runId === runId)) {
    return {
      messages: {
        ...state.messages,
        [sessionId]: list.map((m) => (m.runId === runId ? patch(m) : m)),
      },
      activeMessage: state.activeMessage,
    };
  }
  return {
    messages: state.messages,
    activeMessage: {
      ...state.activeMessage,
      [sessionId]: patch(newAssistantTail(runId)),
    },
  };
}

/** PERF-002: locate the current stream-envelope watermark for a run's message
 * without a lifetime-global map. Mirrors `patchStream`'s targeting: the active
 * tail first, else the matching committed message. Returns 0 when none. */
function deltaSeqFor(state: AppState, sessionId: string, runId: string): number {
  const tail = state.activeMessage[sessionId];
  if (tail && tail.runId === runId) return tail.lastDeltaSeq ?? 0;
  return (state.messages[sessionId] ?? []).find((m) => m.runId === runId)?.lastDeltaSeq ?? 0;
}

/** PERF-002: locate the current stream-envelope watermark for a tool card,
 * mirroring `deltaSeqFor` (active tail first, else the matching committed
 * message). Returns 0 when none. */
function toolSeqFor(state: AppState, sessionId: string, runId: string, id: string): number {
  const tail = state.activeMessage[sessionId];
  if (tail && tail.runId === runId) {
    return tail.tools.find((t) => t.id === id)?.lastToolSeq ?? 0;
  }
  const list = state.messages[sessionId] ?? [];
  return list.find((m) => m.runId === runId)?.tools.find((t) => t.id === id)?.lastToolSeq ?? 0;
}

/** Commit the run's authoritative terminal: the live tail is finalized and
 * appended to the completed transcript EXACTLY ONCE, then cleared. A
 * re-delivered terminal (or one for an already-committed run) patches the
 * committed turn — it never appends a duplicate. */
function commitTerminal(
  state: AppState,
  sessionId: string,
  runId: string,
  patch: (m: Message) => Message,
): StreamSlices {
  const tail = state.activeMessage[sessionId] ?? null;
  if (tail !== null && tail.runId === runId) {
    const list = state.messages[sessionId] ?? [];
    return {
      messages: {
        ...state.messages,
        [sessionId]: clearUncertainIn([...list, patch(tail)], runId),
      },
      activeMessage: { ...state.activeMessage, [sessionId]: null },
    };
  }
  const patched = patchStream(state, sessionId, runId, patch);
  return {
    messages: clearUncertainForRun(patched.messages, sessionId, runId),
    activeMessage: patched.activeMessage,
  };
}

function newAssistantTail(runId: string): Message {
  return {
    id: `${runId}-assistant`,
    role: "assistant",
    runId,
    status: "streaming",
    text: "",
    tools: [],
    permissions: [], questions: [],
    ts: Date.now(),
  };
}

/** The rendered transcript of a session: completed history followed by the
 * live tail. Consumers that render a bounded window slice the completed part
 * themselves and append the tail (never copy the whole history per token). */
export function sessionTranscript(
  state: Pick<AppState, "messages" | "activeMessage">,
  sessionId: string | null,
): Message[] {
  if (!sessionId) return [];
  const completed = state.messages[sessionId] ?? [];
  const tail = state.activeMessage[sessionId] ?? null;
  return tail ? [...completed, tail] : completed;
}

/** The newest turn of a session (the live tail when streaming, else the last
 * committed turn) — what the Activity panel displays. */
export function newestMessage(
  state: Pick<AppState, "messages" | "activeMessage">,
  sessionId: string | null,
): Message | null {
  if (!sessionId) return null;
  const tail = state.activeMessage[sessionId] ?? null;
  if (tail) return tail;
  const completed = state.messages[sessionId] ?? [];
  return completed.length > 0 ? (completed[completed.length - 1] ?? null) : null;
}

function logMessage(event: TypedEvent): string {
  const p = event.payload;
  switch (event.type) {
    case "message.delta":
      return `delta (${(str(p, "delta") ?? "").length} chars)`;
    case "message.started":
      return `run started ${str(p, "run_id") ?? "?"}`;
    case "message.completed":
      return `run completed ${str(p, "run_id") ?? "?"}`;
    case "message.failed":
      return `run failed: ${str(p, "error") ?? "?"}`;
    case "engine.failed":
      return `engine failed: ${str(p, "error") ?? "?"}`;
    case "saipen.action_started":
      return `saipen action started: ${str(p, "kind") ?? "?"}`;
    case "saipen.action_completed":
      return `saipen action completed: ${str(p, "kind") ?? "?"} → ${str(p, "result") ?? "?"}`;
    case "saipen.action_failed":
      return `saipen action failed: ${str(p, "kind") ?? "?"} — ${str(p, "error") ?? "?"}`;
    case "saipen.action_cancelled":
      return `saipen action cancelled: ${str(p, "kind") ?? "?"}`;
    default:
      return "";
  }
}

// ---- Store wiring (single store, useSyncExternalStore) ----

type Listener = () => void;

/** Stream deltas are already coalesced by the shell transport
 * (`saiwork_events::coalescing::forward` — one envelope per (session, run)
 * per ~16 ms frame, flushed synchronously before every state/terminal fact).
 * The Store applies each already-coalesced delta IMMEDIATELY: exactly one
 * state mutation per bridge batch, no second timer/chunk map (TASK 24 perf —
 * ONE batching window from engine delta to React state). The log is
 * untouched by deltas (§241). */
class Store {
  private state: AppState = initialState;
  private listeners = new Set<Listener>();

  getState(): AppState {
    return this.state;
  }

  dispatch(event: TypedEvent): void {
    // message.delta arrives already coalesced per (session, run) from the
    // shell; apply immediately. Nothing to flush: the Rust coalescer already
    // emits pending text before every non-delta/terminal fact (§23), so no
    // tail can be lost.
    this.apply(event);
  }

  /** Test-only: reset to the initial state (mirrors backend test hooks). */
  resetForTest(): void {
    this.state = initialState;
    for (const l of this.listeners) l();
  }

  patch(p: (s: AppState) => AppState): void {
    const next = p(this.state);
    if (next === this.state) return;
    this.state = next;
    for (const l of this.listeners) l();
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private apply(event: TypedEvent): void {
    const prev = this.state;
    const next = applyEvent(prev, event);
    if (next === prev) return;
    this.state = next;
    for (const l of this.listeners) l();
    // EVENT-DRIVEN active-session transition (T-046): session.created /
    // session.closed can move `activeSessionId` from inside the reducer, so the
    // single owner's history guarantee is enforced here for those paths. Async
    // paths (select / restore / reconcile) call `activateSession` directly.
    if (next.activeSessionId !== prev.activeSessionId && next.activeSessionId !== null) {
      void loadSessionHistory(next.activeSessionId);
    }
  }
}

export const store = new Store();

export function useAppState(): AppState {
  return useSyncExternalStore(
    (cb) => store.subscribe(cb),
    () => store.getState(),
  );
}
