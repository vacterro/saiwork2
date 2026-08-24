// THE workspace-selection authority (T-027).
//
// Exactly one path may change the user-visible current workspace: this module.
// Open…, sidebar activation and cold bootstrap all call `selectWorkspace`.
// `workspace.opened` is a NOTIFICATION (runtime fact), never a selection — the
// reducer deliberately ignores it for selection purposes, otherwise a delayed
// `workspace.opened(A)` could re-select A after the user moved to B and feed a
// wrong-project send.
//
// Two invariants:
//
//  1. EPOCH COMMIT. Every selection takes a monotonic token. Only the newest
//     token may commit — the id, the scoped session list and the SAIPEN
//     projection are all written under it, so a slow A can never land inside B.
//
//  2. NO HALF-CLEARED PROJECTION. The previous workspace's scoped state is
//     cleared in the SAME patch that installs the new workspace, only after
//     the authoritative `open_workspace` succeeded. A failed open therefore
//     leaves the current workspace fully coherent (the old code cleared first
//     and left an empty session list under the still-current workspace).

import type { AppState } from "../state/store";
import { store, upsertWorkspace } from "../state/store";
import { mutationsAllowed } from "../app/eligibility";
import type { SaipenState, Session, WorkspaceSummary } from "@saiwork2/contracts";
import { commands } from "./backend";
import { activateSession } from "./sessionSelection";
import { requestEngineAutoStart } from "./engineAutoStart";

let selectionToken = 0;

/** The selection epoch a caller can carry to decide whether ITS follow-up
 * reads are still allowed to commit (used by cold bootstrap). */
export type SelectionEpoch = number;

export function currentSelectionEpoch(): SelectionEpoch {
  return selectionToken;
}

export function isCurrentEpoch(epoch: SelectionEpoch): boolean {
  return epoch === selectionToken;
}

export interface SelectionResult {
  /** The workspace id that was COMMITTED by this selection, or null when the
   * selection failed or was superseded. Callers must use this — never the id
   * they had before awaiting (that is the stale-bootstrap contamination bug). */
  workspaceId: string | null;
  epoch: SelectionEpoch;
  /** True when a newer selection superseded this one before it could commit. */
  superseded: boolean;
}

/**
 * Select a workspace by path. Returns the committed `WorkspaceSummary` (the
 * authoritative DTO) so callers can read id/path/has_git/saipen directly.
 * Throws only when the open itself fails — but preserves the previously
 * selected workspace (nothing is cleared), so the caller can fall back to it.
 */
export async function selectWorkspace(
  path: string,
  onError: (message: string) => void = () => undefined,
): Promise<WorkspaceSummary> {
  // W2-008: no workspace open/activation while the app is shutting down (the
  // backend rejects it too, but a late user click must not even attempt it).
  if (!mutationsAllowed(store.getState())) {
    const e = new Error("Application is shutting down");
    onError(String(e));
    throw e;
  }
  const token = ++selectionToken;
  let workspace: WorkspaceSummary;
  try {
    workspace = await commands.openWorkspace(path);
  } catch (e) {
    // Nothing was cleared: the previously selected workspace stays coherent.
    onError(String(e));
    throw e;
  }
  if (token !== selectionToken) return workspace; // superseded: ignore result

  // Stage the scoped reads BEFORE re-scope (T-045): a failure after open must
  // NOT leave the newly selected workspace with fabricated-empty sessions and
  // no session-staleness truth. The previous workspace (A) stays fully coherent
  // until B's scoped reads actually commit — a failed B init preserves A
  // rather than presenting an empty, wrongly-current B.
  let sessions: Session[];
  let saipen: SaipenState | null;
  try {
    [sessions, saipen] = await Promise.all([
      commands.listSessions(workspace.id),
      commands.getSaipen(workspace.id),
    ]);
  } catch (e) {
    onError(String(e));
    // Open succeeded but scoped reads failed: do NOT commit B. Invalidate any
    // in-flight B read and leave the current workspace (A) coherent.
    selectionToken += 1;
    throw e;
  }
  if (token !== selectionToken) return workspace; // superseded: ignore result

  // Commit exact active workspace to storage. The selection epoch (`token`)
  // rides along so the backend can enforce latest-wins: a superseded A whose
  // IPC lands after B already committed is ignored, never persisted (CORE-001).
  try {
    await commands.setActiveWorkspace(workspace.id, token);
  } catch (e) {
    onError(String(e));
    selectionToken += 1;
    throw e;
  }
  if (token !== selectionToken) return workspace;

  // ATOMIC re-scope: install the new id and drop the previous workspace's
  // sessions/SAIPEN in one transition. There is no intermediate state in
  // which workspace B is current while A's sessions are still listed.
  store.patch((s) => rescope(s, workspace.id, upsertWorkspace(s.workspaces, workspace)));

  store.patch((s) => {
    // Defensive: the epoch guard above already proves this, but the id is
    // re-checked so a future caller cannot commit A's sessions under B.
    if (s.currentWorkspaceId !== workspace.id) return s;
    return {
      ...s,
      sessions,
      // Always `result ?? null`: a project without SAIPEN clears the previous
      // project's projection instead of leaving stale state.
      saipen: saipen ?? null,
      // W2-006: derive freshness from the snapshot's authoritative stale flag.
      saipenStale: Boolean(saipen?.stale),
    };
  });
  // Route the active-session choice through the single owner so the newly
  // active session's authoritative history is hydrated exactly once (T-046).
  const activeSessionId =
    store.getState().activeSessionId &&
    sessions.some((x) => x.id === store.getState().activeSessionId)
      ? store.getState().activeSessionId
      : sessions[0]?.id ?? null;
  activateSession(activeSessionId);
  // Latest-intent scheduler includes bootstrap restore and serializes rapid
  // project/engine changes so an older async start cannot win the binding.
  const selected = store.getState();
  // Await this selection's intent: the returned selection is fully usable
  // when start succeeds, while the scheduler still resolves harmlessly if a
  // newer intent supersedes it.
  await requestEngineAutoStart(selected.selectedEngineId, selected.currentWorkspaceId, onError);
  return workspace;
}

/** Install `workspaceId` as current and drop every projection scoped to the
 * previous workspace. Session message projections are dropped with their
 * sessions — a stale transcript must not survive a project switch; history is
 * re-hydrated authoritatively from the engine when a session is reselected.
 *
 * `running` is deliberately PRESERVED: clearing it would fabricate "no run is
 * active" for runs the backend still owns. It is keyed by session id, so
 * entries for another workspace's sessions are inert, and the authoritative
 * `active_runs` reconciliation remains the only writer of that truth. */
function rescope(
  s: AppState,
  workspaceId: string,
  workspaces: AppState["workspaces"],
): AppState {
  if (s.currentWorkspaceId === workspaceId) {
    // Re-selecting the SAME workspace refreshes metadata without discarding
    // its live sessions/transcripts.
    return { ...s, workspaces };
  }
  return {
    ...s,
    workspaces,
    currentWorkspaceId: workspaceId,
    sessions: [],
    activeSessionId: null,
    messages: {},
    activeMessage: {},
    historyStatus: {},
    saipen: null,
    // The new workspace's SAIPEN projection has not been read yet: it is
    // explicitly UNKNOWN (stale) until the scoped read commits, never
    // presented as "this project has no SAIPEN".
    saipenStale: true,
  };
}

/** Invalidate any in-flight selection epoch. Called when the current workspace
 * is closed so a slow A read that lands after the close can no longer commit
 * into the now-empty state (T-045). */
export function invalidateSelectionEpoch(): void {
  selectionToken += 1;
}

/** Close a workspace: drop every scoped projection (sessions / messages /
 * SAIPEN / history) and invalidate in-flight selection epochs. The single
 * owner of ALL current-workspace clear/commit — both the user "close" action
 * and the `workspace.closed` event route through here (T-045). Returns the
 * next state for the given `state`; only acts when `closedId` is the current
 * workspace, so a close for another workspace is a no-op. */
export function applyWorkspaceClosed(state: AppState, closedId: string): AppState {
  if (state.currentWorkspaceId !== closedId) return state;
  selectionToken += 1;
  return {
    ...state,
    currentWorkspaceId: null,
    sessions: [],
    activeSessionId: null,
    messages: {},
    activeMessage: {},
    historyStatus: {},
    saipen: null,
    saipenStale: false,
  };
}

/** Close the current workspace (explicit user action): the scoped projection
 * is dropped and a NEW epoch is taken so any in-flight older selection can no
 * longer commit into the empty state. */
export function clearWorkspaceSelection(): void {
  store.patch((s) => applyWorkspaceClosed(s, s.currentWorkspaceId ?? ""));
  void requestEngineAutoStart(null, null);
  // The deselect carries the (already-incremented) selection epoch so a newer
  // selection that committed before this clear landed supersedes it via
  // latest-wins, instead of being wiped by a stale epoch-less clear (CORE-001).
  commands.setActiveWorkspace(null, currentSelectionEpoch()).catch(() => undefined);
}
