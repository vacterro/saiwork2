// Bridge between the UI and the SAIWORK2 core (Tauri commands + events).
//
// The UI never owns processes, queue state, or DB writes (laws 4/5): every
// mutation goes through these commands. Events are the only way core state
// reaches the store (law 23). In pure-browser dev (`vite` without Tauri) the
// bridge reports `disconnected` and the UI shows a banner instead of
// fabricating state.

import type {
  DiagnosticsSnapshot,
  EngineInfo,
  LifecycleState,
  ModelInfo,
  QueueItem,
  QueueSnapshot,
  SaipenActionRecord,
  SaipenActionStatus,
  SaipenState,
  SendOutcome,
  Session,
  WorkspaceSummary,
  DirListing,
  FilePreview,
} from "@saiwork2/contracts";

/** Authoritative pending-permission snapshot shape (W2-004) — mirrors the
 * Rust `PendingPermissionInfo`. Declared inline so the contracts package is
 * not coupled to a transient reconciliation payload. */
export interface PendingPermissionInfo {
  session_id: string;
  run_id: string;
  request_id: string;
  detail: string;
}

/** Mirror of Rust `PendingQuestionInfo` (AUDIT-CORE-002). Declared inline so
 * the contracts package is not coupled to a transient reconciliation
 * payload. */
export interface PendingQuestionInfo {
  session_id: string;
  run_id: string;
  request_id: string;
  detail: string;
}
import { parseEnvelope } from "../state/events";
import { store, type SessionHistoryMessage } from "../state/store";

type TauriWindow = { __TAURI_INTERNALS__?: unknown };

function isTauri(): boolean {
  return typeof window !== "undefined" && Boolean((window as TauriWindow).__TAURI_INTERNALS__);
}

async function invoke<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  if (!isTauri()) {
    throw new Error("backend not connected (run `npm run tauri dev`)");
  }
  const { invoke: tauriInvoke } = await import("@tauri-apps/api/core");
  return tauriInvoke<T>(command, args);
}

// ---- Commands ----

export interface AppInfo {
  version: string;
  data_root: string;
  portable: boolean;
  lifecycle: LifecycleState;
}

export const commands = {
  appInfo: () => invoke<AppInfo>("app_info"),
  listWorkspaces: () => invoke<WorkspaceSummary[]>("list_workspaces"),
  getActiveWorkspace: () => invoke<string | null>("get_active_workspace"),
  setActiveWorkspace: (id: string | null, gen?: number | null) =>
    invoke<void>("set_active_workspace", { id, gen: gen ?? null }),
  openWorkspace: (path: string) => invoke<WorkspaceSummary>("open_workspace", { path }),
  closeWorkspace: (id: string) => invoke<void>("close_workspace", { id }),
  forgetWorkspace: (id: string) => invoke<void>("forget_workspace", { id }),
  listEngines: () => invoke<EngineInfo[]>("list_engines"),
  startEngine: (engineId: string, workspaceId: string | null) => invoke<void>("start_engine", { engineId, workspaceId }),
  stopEngine: (engineId: string) => invoke<void>("stop_engine", { engineId }),
  listModels: (engineId: string) => invoke<ModelInfo[]>("list_models", { engineId }),
  /** Durable model favorites (app_settings k/v, app authority): the UI
   * never writes the DB directly (law 5). */
  getModelFavorites: () => invoke<string[]>("get_model_favorites"),
  setModelFavorites: (favorites: string[]) =>
    invoke<void>("set_model_favorites", { favorites }),
  createSession: (engineId: string, workspaceId: string | null, model: string | null) =>
    invoke<Session>("create_session", { engineId, workspaceId, model }),
  listSessions: (workspaceId: string | null) => invoke<Session[]>("list_sessions", { workspaceId }),
  /** Direct send with the UI's expected context (TASK 24 §9): the backend
   * rejects a stale UI (wrong workspace/engine) BEFORE any external call and
   * returns a TYPED outcome so the pending user turn is only removed on a
   * definite rejection. */
  sendPrompt: (
    sessionId: string,
    workspaceId: string | null,
    engineId: string | null,
    prompt: string,
    model: string | null,
  ) =>
    invoke<SendOutcome>("send_prompt", { sessionId, workspaceId, engineId, prompt, model }),
  cancelRun: (sessionId: string, runId: string) => invoke<void>("cancel_run", { sessionId, runId }),
  /** Read-only authoritative session history from the owning engine (TASK 24
   * §9): null when the engine has no history capability — the UI shows the
   * limitation instead of a fabricated empty thread. Never a SQLite mirror. */
  sessionHistory: (sessionId: string) =>
    invoke<SessionHistoryMessage[] | null>("session_history", { sessionId }),
  deleteSession: (sessionId: string) => invoke<void>("delete_session", { sessionId }),
  revertLastTurn: (sessionId: string) => invoke<void>("revert_last_turn", { sessionId }),
  unrevertSession: (sessionId: string) => invoke<void>("unrevert_session", { sessionId }),
  resolvePermission: (sessionId: string, requestId: string, allowed: boolean) =>
    invoke<void>("resolve_permission", { sessionId, requestId, allowed }),
  /** AUDIT-CORE-002: answer/reject a pending user question. `answers` carries
   * one selected option label per asked question; null = authoritative
   * reject. */
  resolveQuestion: (sessionId: string, requestId: string, answers: string[][] | null) =>
    invoke<void>("resolve_question", { sessionId, requestId, answers }),
  /** Authoritative pending-permission snapshot (W2-004): every open permission
   * request across engines, keyed by exact session/run/request ownership. Used
   * by `frontend.reconcile` to rebuild permission cards after a bounded-bus
   * lag — a missed `permission.requested` becomes recoverable. */
  pendingPermissions: () => invoke<PendingPermissionInfo[]>("pending_permissions"),
  /** AUDIT-CORE-002: authoritative pending-question snapshot — same
   * reconciliation contract as `pendingPermissions`. */
  pendingQuestions: () => invoke<PendingQuestionInfo[]>("pending_questions"),
  getSaipen: (workspaceId: string) => invoke<SaipenState | null>("get_saipen", { workspaceId }),
  // ---- SAIPEN actions (TASK 15) ----
  saipenActionStart: (workspaceId: string, action: string) =>
    invoke<SaipenActionRecord>("saipen_action_start", { workspaceId, action }),
  saipenActionCancel: (workspaceId: string) =>
    invoke<void>("saipen_action_cancel", { workspaceId }),
  saipenActionStatus: (workspaceId: string) =>
    invoke<SaipenActionStatus>("saipen_action_status", { workspaceId }),
  /** Persist the CURRENT workspace's SAIPEN home as the explicitly-trusted
   * install (T-080): the backend resolves saipen_home from the workspace's own
   * STATE and validates it is a real install before persisting. Used when the
   * SAIPENBAR reports executable actions disabled (untrusted path). */
  setSaipenTrustedHome: (workspaceId: string) =>
    invoke<void>("set_saipen_trusted_home", { workspaceId }),
  /** Clear the explicitly-trusted SAIPEN install (T-080). */
  clearSaipenTrustedHome: () => invoke<void>("clear_saipen_trusted_home"),
  diagnostics: () => invoke<DiagnosticsSnapshot>("get_diagnostics"),
  /** Generic durable UI setting (Phase B layout persistence): thin
   * delegation to the core app_settings k/v store. Only non-security,
   * versioned UI preferences (e.g. dock geometry) use this. */
  getSetting: (key: string) => invoke<string | null>("get_setting", { key }),
  setSetting: (key: string, value: string) => invoke<void>("set_setting", { key, value }),
  /** Import a durable-UI settings preset from a user-picked file (T-078). The
   * backend detects ZIP magic and refuses it with a clear error — a preset
   * must be .json, never JSON.parsed from a ZIP. */
  importPreset: (path: string) =>
    invoke<{ settings_applied: number; favorites_applied: number }>("import_preset", { path }),
  shutdown: () => invoke<void>("app_shutdown"),
  /** Exact (session_id, run_id) ownership for every session with a live or
   * unknown run — rebuilds `state.running` after reload / frontend.reconcile
   * (TASK 24 §9). */
  activeRuns: () => invoke<[string, string][]>("active_runs"),
  // ---- Queue (TASK 13) ----
  queueSnapshot: () => invoke<QueueSnapshot>("queue_snapshot"),
  /** Full durable item (exact payload) for editing/inspecting ONE row — the
   * snapshot carries only bounded payload previews (TASK 24 perf). */
  queueGetItem: (itemId: string) => invoke<QueueItem>("queue_get_item", { itemId }),
  queueEnqueue: (args: {
    workspaceId: string;
    engineId: string;
    sessionId: string | null;
    sessionMode: "new" | "existing";
    model: string | null;
    payload: string;
  }) => invoke<QueueItem>("queue_enqueue", args),
  queueEdit: (itemId: string, expectedRevision: number, payload: string, model: string | null) =>
    invoke<QueueItem>("queue_edit", { itemId, expectedRevision, payload, model }),
  queueCancel: (itemId: string) => invoke<void>("queue_cancel", { itemId }),
  /** Risk-confirmed abandonment of an UNKNOWN item (TASK 24 §9): the run may
   * still be mutating the workspace — the UI must confirm before calling. */
  queueResolveUnknown: (itemId: string, expectedRevision: number) =>
    invoke<void>("queue_resolve_unknown", { itemId, expectedRevision }),
  queueReorder: (itemId: string, expectedRevision: number, newIndex: number) =>
    invoke<void>("queue_reorder", { itemId, expectedRevision, newIndex }),
  queuePause: () => invoke<void>("queue_pause"),
  queueResume: () => invoke<void>("queue_resume"),
  queueRetry: (itemId: string, expectedRevision: number) =>
    invoke<void>("queue_retry", { itemId, expectedRevision }),
  // ---- Files (Phase C, read-only) ----
  filesListDir: (workspaceId: string, rel: string) =>
    invoke<DirListing>("files_list_dir", { workspaceId, rel }),
  filesReadPreview: (workspaceId: string, rel: string) =>
    invoke<FilePreview>("files_read_preview", { workspaceId, rel }),
};

/**
 * Open the folder picker. Returns `null` only for two legitimate cases:
 *  - web-dev mode (no Tauri dialog plugin) — silent cancel, no error;
 *  - the user deliberately cancelled (the dialog resolved `null`) — silent cancel.
 * Any OTHER failure (broken Tauri dialog plumbing: import rejection, open
 * throwing) is a real defect, NOT a cancel: it is surfaced via `onError` and
 * the function still returns `null` so no selection is made from a broken state
 * (T-022). The caller must pass `onError` to receive these diagnostics.
 */
export async function pickFolder(onError?: (message: string) => void): Promise<string | null> {
  if (!isTauri()) {
    // Web-dev mode: no real folder dialog available — silent cancel.
    return null;
  }
  try {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const selected = await open({ directory: true, multiple: false });
    // An actual null result is a deliberate user Cancel -> silent.
    return typeof selected === "string" ? selected : null;
  } catch (e) {
    // Tauri dialog plumbing is broken (import/open failed) — this is NOT a cancel.
    // Surface the error so the caller does not silently swallow a real failure.
    const message = e instanceof Error ? e.message : String(e);
    onError?.(`Folder picker failed: ${message}`);
    return null;
  }
}

/**
 * Native confirmation dialog (T-081). Uses the Tauri dialog plugin's
 * `confirm()` in the desktop app (WebView2 blocks `window.confirm` — the
 * "dialog.confirm not allowed" failure), falling back to `window.confirm`
 * in web-dev mode. Returns true only on an explicit user Yes.
 */
export async function confirmDialog(message: string): Promise<boolean> {
  if (typeof window !== "undefined" && "__TAURI_INTERNALS__" in window) {
    try {
      const { confirm } = await import("@tauri-apps/plugin-dialog");
      return await confirm(message, { kind: "warning" });
    } catch {
      // Broken dialog plumbing: fail closed — never confirm a destructive
      // action from a broken dialog.
      return false;
    }
  }
  return window.confirm(message);
}

// ---- Event subscription ----

/** Wire the canonical event stream into the store. Returns a handle with an
 *  AWAITABLE `ready` promise (the listener is installed before it resolves)
 *  and a `dispose` fn. `onReconcile` is invoked when the backend reports the
 *  event stream lagged and the frontend must re-snapshot authoritative state
 *  (live updates must not require a manual reload).
 *
 *  Subscription readiness (TASK 24 §9): bootstrap must not take its first
 *  authoritative snapshot until the listener is installed — otherwise events
 *  landing between `listen()` start and the first snapshot would be missed
 *  and then overwritten by that stale snapshot. */
export function subscribeToCoreEvents(onReconcile?: (reason?: "lag") => void): {
   ready: Promise<void>;
   dispose: () => void;
} {
  if (!isTauri()) {
    // Web-dev mode: mark disconnected; no fabricated state.
    store.patch((s) => ({ ...s, backend: "disconnected" }));
    return { ready: Promise.resolve(), dispose: () => undefined };
  }
  let disposed = false;
  let unlisten: (() => void) | undefined;
  let resolveReady: (() => void) | undefined;
  let rejectReady: ((e: unknown) => void) | undefined;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });

  // W2-001: bounded, disposable subscription-recovery loop. A transient
  // listener-setup failure (dynamic import / `listen()` rejection) must not
  // permanently strand this frontend lifetime before bootstrap. Retry
  // establishing the listener, then `startFrontendSession` runs its one cold
  // bootstrap once a listener is live. Only one attempt is live at a time; a
  // dispose cancels pending retries and a late successful `listen()` is
  // unlistened immediately (no leaked listener, no duplicate listener).
  const MAX_SUBSCRIBE_ATTEMPTS = 5;
  const attemptListen = (attempt: number) => {
    void (async () => {
      try {
        const { listen } = await import("@tauri-apps/api/event");
        if (disposed) {
          // T-075: disposal is NORMAL lifecycle (React StrictMode mounts,
          // unmounts and remounts the App in dev). A disposed session has no
          // consumer waiting — resolve silently so teardown can never surface
          // as a user-visible "subscription failed" error.
          resolveReady?.();
          return;
        }
        unlisten = await listen("event", (event) => {
          if (disposed) return;
          const payload = event.payload as Record<string, unknown>;
          if (payload && typeof payload.type === "string") {
            if (payload.type === "frontend.reconcile") {
              onReconcile?.(payload.reason === "lag" ? "lag" : undefined);
              return;
            }
            store.dispatch(parseEnvelope(payload as never));
          }
        });
        // The disposer may have already run while `listen` was still resolving
        // (fast unmount/HMR): detach immediately instead of leaking the listener.
        if (disposed) {
          unlisten();
          unlisten = undefined;
          resolveReady?.();
          return;
        }
        resolveReady?.();
      } catch (e) {
        unlisten?.();
        unlisten = undefined;
        if (disposed) {
          // T-075: a failure during an already-disposed session is teardown
          // noise, not a defect — resolve silently, never reject.
          resolveReady?.();
          return;
        }
        // Bounded backoff, then retry — at most MAX_SUBSCRIBE_ATTEMPTS total.
        if (attempt + 1 >= MAX_SUBSCRIBE_ATTEMPTS) {
          // The ONLY rejecting path: every attempt failed while the session
          // is live. This is a real, user-relevant defect.
          rejectReady?.(e);
          return;
        }
        await delay(subscribeBackoff(attempt));
        if (disposed) {
          // T-075: disposed during backoff — silent teardown, same rule.
          resolveReady?.();
          return;
        }
        attemptListen(attempt + 1);
      }
    })();
  };
  attemptListen(0);

  return {
    ready,
    dispose: () => {
      disposed = true;
      // Idempotent: clear the handle while detaching so a repeated dispose
      // (StrictMode teardown safety) cannot detach twice.
      const u = unlisten;
      unlisten = undefined;
      u?.();
    },
  };
}

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// Bounded exponential backoff for the subscription-recovery loop (W2-001).
function subscribeBackoff(attempt: number): number {
  return Math.min(25 * 2 ** attempt, 500);
}
