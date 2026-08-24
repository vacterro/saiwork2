// Canonical session selection (one owner for every entry point: the sessions
// list, the thread tabs, and any future navigation).
//
// Selecting a session has TWO effects: it changes the active id, and it
// restores the engine's authoritative history for that session. Both used to
// live inside SessionList, so ThreadTabs would have had to duplicate them — a
// second, drifting selection path. This module is the single owner.

import { commands } from "./backend";
import { hydrateSessionHistory, setHistoryStatus, store } from "../state/store";
import { currentSelectionEpoch, isCurrentEpoch } from "./workspaceSelection";

/** Monotonic per-session read generation: a slow history read for session A
 * that lands after the user moved to B must not overwrite B's projection, and
 * a re-selection of A supersedes its own older read. */
const historyGeneration = new Map<string, number>();

/**
 * THE active-session transition owner (T-046): the only place that moves
 * `activeSessionId` AND ensures the authoritative history for the newly active
 * session. Workspace restore, reconcile, session.create/close-replacement and
 * the user's explicit select all funnel through here, so history hydrates
 * exactly once per transition (never on every reconcile) and a failed read
 * retries only on a same-session retry.
 */
export function activateSession(id: string | null): void {
  const current = store.getState().activeSessionId;
  if (current === id) return;
  store.patch((s) => ({ ...s, activeSessionId: id }));
  if (id !== null) void loadSessionHistory(id);
}

export function selectSession(id: string): void {
  const current = store.getState().activeSessionId;
  if (current === id) {
    // SAME-ACTIVE RETRY (T-046): the documented "select the session again to
    // retry" after a history failure must be possible. An already-active
    // session is otherwise left alone to avoid a reload storm, but a prior
    // error/absent read is retried here.
    const status = store.getState().historyStatus[id];
    if (status === "error" || status === "unavailable" || status === undefined) {
      void loadSessionHistory(id);
    }
    return;
  }
  activateSession(id);
}

/** Restore the engine's authoritative history for a session. Explicit status
 * (loading / available / unavailable / error) — never a fabricated empty
 * thread: an engine without a history capability and a failed read are
 * distinct facts, and both differ from a genuinely empty conversation. */
export async function loadSessionHistory(id: string): Promise<void> {
  const epoch = currentSelectionEpoch();
  const gen = (historyGeneration.get(id) ?? 0) + 1;
  historyGeneration.set(id, gen);
  setHistoryStatus(id, "loading");
  try {
    const history = await commands.sessionHistory(id);
    if (historyGeneration.get(id) !== gen) return; // superseded by a newer read
    if (!isCurrentEpoch(epoch)) return; // CORE-008: workspace epoch changed

    if (history) {
      hydrateSessionHistory(id, history);
    } else {
      setHistoryStatus(id, "unavailable");
    }
  } catch {
    if (historyGeneration.get(id) !== gen) return;
    if (!isCurrentEpoch(epoch)) return;
    setHistoryStatus(id, "error");
  }
}

/** Delete through the App authority, then replace the scoped session
 * projection from the backend. No optimistic removal: an upstream/storage
 * failure leaves the thread visible and retryable. */
export async function deleteSession(id: string): Promise<void> {
  const workspaceId = store.getState().currentWorkspaceId;
  await commands.deleteSession(id);
  // A history read that began before deletion must never repopulate the
  // removed session's transcript after the local prune below.
  historyGeneration.set(id, (historyGeneration.get(id) ?? 0) + 1);
  if (store.getState().currentWorkspaceId !== workspaceId) return;
  // Backend deletion is already authoritative success. Refresh when possible,
  // but fall back to an exact local prune if that independent read fails; a
  // transient list error must not leave an undeletable ghost row in the UI.
  const sessions = await commands
    .listSessions(workspaceId)
    .catch(() => store.getState().sessions.filter((session) => session.id !== id));
  if (store.getState().currentWorkspaceId !== workspaceId) return;
  const wasActive = store.getState().activeSessionId === id;
  const nextActive = wasActive ? (sessions[0]?.id ?? null) : store.getState().activeSessionId;
  store.patch((state) => {
    const without = <T,>(record: Record<string, T>): Record<string, T> => {
      const next = { ...record };
      delete next[id];
      return next;
    };
    return {
      ...state,
      sessions,
      activeSessionId: nextActive,
      messages: without(state.messages),
      activeMessage: without(state.activeMessage),
      historyStatus: without(state.historyStatus),
      streamGaps: without(state.streamGaps),
      running: without(state.running),
    };
  });
  if (wasActive && nextActive) void loadSessionHistory(nextActive);
}
