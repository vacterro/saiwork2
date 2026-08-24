// Canonical UI eligibility predicates (T-029).
//
// WHY ONE MODULE: an action's SAFETY condition and its EXPLANATION used to be
// derived in two places (`canSend` in the component vs `sendDisabledReasonFor`
// for the hint), and they disagreed — `runningStale` (ownership UNKNOWN after a
// failed authoritative read) produced a "run status unknown" hint while the
// button stayed clickable. Stale-queue policy was likewise duplicated: the
// Queue panel refused mutations while the Composer offered a second enqueue
// route that skipped the check.
//
// The rule here: exactly ONE predicate per capability, returning both the
// verdict and the reason. Buttons render the verdict; handlers ENFORCE the same
// verdict (a disabled attribute is never the guard — see app/singleFlight.ts).
// Every uncertainty is fail-closed: unknown ⇒ not allowed.

import { healthKind } from "@saiwork2/contracts";
import type { AppState } from "../state/store";

/** A verdict plus the exact reason it is negative (undefined when allowed). */
export interface Availability {
  allowed: boolean;
  reason?: string;
}

const allow: Availability = { allowed: true };
const deny = (reason: string): Availability => ({ allowed: false, reason });

// ---- Application lifecycle gate (W2-008) ----

/**
 * THE canonical application-lifecycle mutation gate. Exactly ONE predicate
 * decides whether the app may accept a NEW mutation (create session, send,
 * start/stop engine, queue edit, SAIPEN action, open/forget workspace).
 * `ready` allows; every other lifecycle state (`booting`, `shutting_down`,
 * `stopped`) rejects. `shutting_down` gets its own reason so the UI can tell
 * the user the shutdown already began. Every mutating availability predicate
 * AND every mutating handler composes this FIRST — the shutdown gate is never
 * re-implemented ad hoc (the pre-W2-008 code duplicated it in two predicates
 * and omitted it from session-create / SAIPEN entirely).
 */
export function lifecycleGate(state: Pick<AppState, "lifecycle">): Availability {
  if (state.lifecycle === "shutting_down") {
    return deny("Application is shutting down");
  }
  if (state.lifecycle !== "ready") {
    return deny("Application is not ready");
  }
  return allow;
}

/** Convenience boolean for handler-level enforcement. A `disabled` attribute is
 * never the guard (see app/singleFlight.ts) — handlers call this before issuing
 * a mutating command. True only when the app is fully `ready`. W2-008. */
export function mutationsAllowed(state: Pick<AppState, "lifecycle">): boolean {
  return state.lifecycle === "ready";
}

// ---- Send ----

export type SendSlice = Pick<
  AppState,
  | "activeSessionId"
  | "currentWorkspaceId"
  | "engines"
  | "lifecycle"
  | "running"
  | "runningStale"
  | "selectedEngineId"
  | "sessions"
>;

/**
 * Direct-send eligibility. The backend enforces the same boundary; this exists
 * so the UI never INVITES an impossible/unsafe send.
 *
 * Fail-closed on `runningStale`: when the last authoritative `active_runs` read
 * failed we cannot prove there is no live run, and a second concurrent run in
 * one workspace is exactly what ADR-038 forbids.
 */
export function sendAvailability(state: SendSlice): Availability {
  if (state.runningStale) {
    return deny("Run status unknown — waiting for a fresh authoritative read");
  }
  const engine = state.engines.find((e) => e.id === state.selectedEngineId) ?? null;
  const engineReady = engine !== null && healthKind(engine.health) === "ready";
  const engineReadyForWs =
    engineReady &&
    engine !== null &&
    (engine.bound_workspace_id == null || engine.bound_workspace_id === state.currentWorkspaceId);
  const activeSession = state.sessions.find((s) => s.id === state.activeSessionId) ?? null;
  const sessionAffinity = activeSession !== null &&
    (activeSession.workspace_id === null ||
      activeSession.workspace_id === state.currentWorkspaceId) &&
    (activeSession.engine_id === state.selectedEngineId || activeSession.engine_id === "?") &&
    activeSession.usable_now !== false;

  if (activeSession && !sessionAffinity) {
    if (
      activeSession.workspace_id !== null &&
      activeSession.workspace_id !== state.currentWorkspaceId
    ) {
      return deny("Session belongs to another project — switch to that project");
    }
    if (activeSession.engine_id !== state.selectedEngineId) {
      return deny(`Session belongs to engine ${activeSession.engine_id} — select that engine`);
    }
    return deny(
      activeSession.resumable === false && activeSession.engine_session_id !== ""
        ? "This session belongs to a previous engine runtime — restart the engine or create a new session"
        : "Session is not usable (no trustworthy upstream id)",
    );
  }
  if (!engineReady) {
    return deny(engine ? `Engine ${engine.display_name} is not ready` : "Start an engine first");
  }
  if (!engineReadyForWs) {
    return deny(
      engine
        ? `Engine ${engine.display_name} is running for another project — stop and restart it for this project`
        : "Start an engine first",
    );
  }
  if (!activeSession) {
    if (!state.currentWorkspaceId) return deny("Open a project first");
    if (!engine?.capabilities.sessions) return deny("Selected engine cannot create sessions");
  }
  if (state.activeSessionId && (state.running[state.activeSessionId] ?? null) !== null) {
    return deny("A run is active — cancel it first");
  }
  // W2-008: canonical lifecycle gate first.
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  return allow;
}

/** Backwards-compatible reason accessor (used by the first-prompt smoke). */
export function sendDisabledReasonFor(state: SendSlice): string | undefined {
  return sendAvailability(state).reason;
}

// ---- Cancel ----

export type CancelSlice = Pick<AppState, "activeSessionId" | "lifecycle" | "running" | "runningStale">;

/** Cancelling a run needs PROVEN ownership of that exact run id. */
export function cancelAvailability(state: CancelSlice): Availability {
  if (state.runningStale) return deny("Run status unknown — waiting for a fresh authoritative read");
  // W2-008: canonical lifecycle gate.
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  const runId = state.activeSessionId ? (state.running[state.activeSessionId] ?? null) : null;
  if (!runId) return deny("No active run");
  return allow;
}

// ---- Queue ----

export type QueueMutationSlice = Pick<
  AppState,
  "currentWorkspaceId" | "lifecycle" | "queue" | "selectedEngineId"
>;

export function queueMutationAvailability(state: QueueMutationSlice): Availability {
  if (state.queue.stale) {
    return deny("Queue projection is stale (read-only) — waiting for a fresh snapshot");
  }
  // W2-008: canonical lifecycle gate.
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  if (!state.currentWorkspaceId) return deny("Open a project first");
  if (!state.selectedEngineId) return deny("Select an engine first");
  return allow;
}

export type QueueAdminSlice = Pick<AppState, "lifecycle" | "queue">;

export function queueAdminAvailability(state: QueueAdminSlice): Availability {
  if (state.queue.stale) {
    return deny("Queue projection is stale (read-only) — waiting for a fresh snapshot");
  }
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  return allow;
}

export type QueueItemMutationSlice = Pick<AppState, "lifecycle" | "queue">;

export function queueItemMutationAvailability(state: QueueItemMutationSlice): Availability {
  if (state.queue.stale) {
    return deny("Queue projection is stale (read-only) — waiting for a fresh snapshot");
  }
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  return allow;
}

// ---- Session Create / Activation ----

export type SessionEligibilitySlice = Pick<
  AppState,
  "currentWorkspaceId" | "engines" | "selectedEngineId" | "stoppingEngines" | "lifecycle"
>;

export function sessionCreateAvailability(state: SessionEligibilitySlice): Availability {
  // W2-008: no new session may be created while the app is not fully ready
  // (including during shutdown — the pre-W2-008 code omitted this gate).
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  const engine = state.engines.find((e) => e.id === state.selectedEngineId) ?? null;
  const engineReady = engine !== null && engine.capabilities.sessions && healthKind(engine.health) === "ready";
  const engineReadyForWs =
    engineReady &&
    engine !== null &&
    (engine.bound_workspace_id == null || engine.bound_workspace_id === state.currentWorkspaceId);

  if (!state.currentWorkspaceId) return deny("Open a project first");
  if (!engineReady) return deny("Start the engine first");
  if (!engineReadyForWs) return deny("Engine is running for another project — stop and restart it for this project");
  if (state.stoppingEngines[engine?.id ?? ""]) return deny("Engine is stopping");

  return allow;
}

export function sessionActivationAvailability(session: AppState["sessions"][0], state: SessionEligibilitySlice): Availability {
  // W2-008: activation is gated by the same lifecycle as any mutation.
  const lg = lifecycleGate(state);
  if (!lg.allowed) return lg;
  const sessionAffinity =
    (session.workspace_id === null || session.workspace_id === state.currentWorkspaceId) &&
    (session.engine_id === state.selectedEngineId || session.engine_id === "?") &&
    session.usable_now !== false;

  if (!sessionAffinity) {
    if (session.workspace_id !== null && session.workspace_id !== state.currentWorkspaceId) {
      return deny("Session belongs to another project");
    }
    if (session.engine_id !== state.selectedEngineId) {
      return deny(`Session belongs to engine ${session.engine_id}`);
    }
    return deny("Session is not usable in the current runtime");
  }

  return allow;
}

// ---- SAIPEN ----

export type SaipenSlice = Pick<AppState, "currentWorkspaceId" | "saipen" | "saipenStale">;

/**
 * Explicit SAIPEN projection freshness — the bar must never present stale data
 * as current:
 *   `none`   no workspace open;
 *   `absent` workspace open, authoritatively has no SAIPEN;
 *   `fresh`  last authoritative read succeeded;
 *   `stale`  last authoritative read FAILED — shown data may be outdated.
 */
export type SaipenFreshness = "none" | "absent" | "fresh" | "stale";

export function saipenFreshness(state: SaipenSlice): SaipenFreshness {
  if (!state.currentWorkspaceId) return "none";
  if (state.saipenStale) return "stale";
  return state.saipen ? "fresh" : "absent";
}

/**
 * SAIPEN actions mutate the project's protocol state: never run one from an
 * unproven projection. `actionStatus` is the live (locally-held) action status;
 * the freshness gate is the store's `saipenStale` + whether a workspace is open.
 *
 * When `requestedAction` is supplied, the action is only allowed if it is in
 * the authoritative `availability.available` list — an action not currently
 * offered by the project must cause ZERO backend invokes (T-049).
 */
export function saipenActionAvailability(
  state: SaipenSlice,
  actionStatus?: import("@saiwork2/contracts").SaipenActionStatus | null,
  requestedAction?: string | null,
): Availability {
  const freshness = saipenFreshness(state);
  if (freshness === "none") return deny("Open a project first");
  if (freshness === "stale") {
    return deny("SAIPEN state is stale — the last authoritative read failed");
  }
  if (freshness === "absent") return deny("This project has no .saipen/ state");
  if (actionStatus) {
    // T-080: executable actions (status/validate) are unavailable when the
    // canonical tool cannot be resolved (untrusted path, missing install).
    const disabled = actionStatus.availability?.disabled_reason ?? null;
    if (disabled) {
      return deny(disabled);
    }
    const available = actionStatus.availability?.available ?? [];
    if (requestedAction && !available.includes(requestedAction)) {
      // The action is not offered by the project's authoritative status:
      // never invoke it against the backend.
      return deny(`Action "${requestedAction}" is not currently available for this project`);
    }
  }
  return allow;
}
