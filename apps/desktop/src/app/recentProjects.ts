// Canonical recent-projects ordering (T-126).
//
// The sidebar's project list is ordered most-recently-opened first. The
// authoritative workspace list is the backend's `workspaces` snapshot (law 5 —
// the UI never owns workspace identity); this module owns only the DISPLAY
// ORDER: a bounded ring of workspace ids, bumped on every successful
// `selectWorkspace` and persisted via the generic `ui.recents.v1` setting so
// the order survives a restart. Ids of forgotten projects are filtered at
// render time (the ring is ordering metadata, never a workspace registry).
import { store } from "../state/store";
import { commands } from "./backend";

export const UI_RECENTS_KEY = "ui.recents.v1";
export const MAX_RECENT_WORKSPACES = 12;

/** Move `id` to the front of the recency ring (dedup + bounded) and persist. */
export function bumpRecentWorkspace(id: string): void {
  const next = [
    id,
    ...store.getState().recentWorkspaceIds.filter((x) => x !== id),
  ].slice(0, MAX_RECENT_WORKSPACES);
  store.patch((s) => ({ ...s, recentWorkspaceIds: next }));
  // Fire-and-forget: a settings write failure must never break selection; it
  // is surfaced only as a lost ordering preference, not an error toast.
  void commands
    .setSetting(UI_RECENTS_KEY, JSON.stringify({ version: 1, ids: next }))
    .catch(() => undefined);
}

/** Restore the persisted recency ring at bootstrap (fail-soft: a corrupt or
 * missing value keeps the default empty ring — ordering is cosmetic). */
export async function loadRecentProjects(): Promise<void> {
  try {
    const raw = await commands.getSetting(UI_RECENTS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw) as { version?: unknown; ids?: unknown };
    if (parsed.version !== 1 || !Array.isArray(parsed.ids)) return;
    const ids = parsed.ids
      .filter((x): x is string => typeof x === "string")
      .slice(0, MAX_RECENT_WORKSPACES);
    store.patch((s) => ({ ...s, recentWorkspaceIds: ids }));
  } catch {
    // Corrupt/unavailable recents are non-fatal: keep defaults (fail-soft).
  }
}

/** Test-only reset (the ring lives in the store; nothing module-local). */
export function resetRecentProjectsForTest(): void {
  store.patch((s) => ({ ...s, recentWorkspaceIds: [] }));
}