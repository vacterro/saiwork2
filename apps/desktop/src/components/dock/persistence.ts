// Phase B dock layout persistence (thin delegation to core app_settings).
//
// Durable dock geometry is the ONLY dock state that survives restart; it lives
// in the core k/v store (law 5 — the UI never owns the DB). Ephemeral dock
// state (hover, drag, transient loading) stays in component-local React state.
// Persisted geometry is validated + clamped on load so a stored width that
// exceeds the viewport can never make the dock unreachable.
import { store } from "../../state/store";
import { commands } from "../../app/backend";
import { UI_LAYOUT_KEY, type DockTab, type UiLayoutV1 } from "./types";

export const DOCK_MIN_WIDTH = 240;
export const DOCK_MAX_WIDTH = 720;

// W2-007: layout-mutation generation. A startup `getSetting` read is
// fire-and-forget and may land AFTER the user changed the layout; the read
// must not roll back that newer choice. Every layout setter bumps this
// synchronously before persisting; `loadUiLayout` applies the returned value
// only if no local mutation has occurred since the read started.
let layoutGen = 0;

function clampWidth(w: number): number {
  return Math.max(DOCK_MIN_WIDTH, Math.min(DOCK_MAX_WIDTH, Math.round(w)));
}

export function setDockWidth(width: number): void {
  // PERF-003: live geometry only. Durability is a discrete commit, not a
  // pointer-rate stream — `persistUiLayout` is called once at drag end (see
  // Dock.onResizeStart) so a fast drag does not fire a SQLite write per pixel.
  layoutGen++;
  store.patch((s) => ({ ...s, dockWidth: clampWidth(width) }));
}

export function toggleDockCollapsed(): void {
  layoutGen++;
  store.patch((s) => ({ ...s, dockCollapsed: !s.dockCollapsed }));
  persistUiLayout();
}

export function setDockTab(tab: DockTab): void {
  layoutGen++;
  store.patch((s) => ({ ...s, activeDockTab: tab }));
  persistUiLayout();
}

export function setCloseQueueWhenDone(on: boolean): void {
  layoutGen++;
  store.patch((s) => ({ ...s, closeQueueWhenDone: on }));
  persistUiLayout();
}

export function setEnterQueues(on: boolean): void {
  layoutGen++;
  store.patch((s) => ({ ...s, enterQueues: on }));
  persistUiLayout();
}

export function persistUiLayout(): void {
  const s = store.getState();
  const layout: UiLayoutV1 = {
    version: 1,
    dockWidth: s.dockWidth,
    dockCollapsed: s.dockCollapsed,
    activeDockTab: s.activeDockTab,
    closeQueueWhenDone: s.closeQueueWhenDone,
    enterQueues: s.enterQueues,
  };
  // Fire-and-forget: a settings write failure must never break the UI; it is
  // surfaced only as a lost preference, not as an error toast.
  void commands.setSetting(UI_LAYOUT_KEY, JSON.stringify(layout)).catch(() => undefined);
}

function isDockTab(v: unknown): v is DockTab {
  // Only shipped tabs are valid; a stored layout that names a
  // not-yet-shipped tab (changes/preview/terminal) is rejected and the
  // default tab is kept, so a stale preference can never render a dead panel.
  return v === "activity" || v === "queue" || v === "diag" || v === "files";
}

/** Test-only reset of the layout-hydration generation. */
export function resetUiLayoutForTest(): void {
  layoutGen = 0;
}

export async function loadUiLayout(): Promise<void> {
  // Capture the layout generation at read start. If a local mutation happens
  // while the read is in flight (W2-007), the generation advances and the
  // stale persisted value is discarded so the newer choice survives.
  const myGen = layoutGen;
  try {
    const raw = await commands.getSetting(UI_LAYOUT_KEY);
    if (myGen !== layoutGen) return; // a local mutation superseded this read
    if (!raw) return;
    const parsed = JSON.parse(raw) as Partial<UiLayoutV1>;
    if (parsed.version !== 1) return;
    if (myGen !== layoutGen) return; // re-check after parse (defensive)
    store.patch((s) => ({
      ...s,
      dockWidth:
        typeof parsed.dockWidth === "number" ? clampWidth(parsed.dockWidth) : s.dockWidth,
      dockCollapsed:
        typeof parsed.dockCollapsed === "boolean" ? parsed.dockCollapsed : s.dockCollapsed,
      activeDockTab: isDockTab(parsed.activeDockTab) ? parsed.activeDockTab : s.activeDockTab,
      closeQueueWhenDone:
        typeof parsed.closeQueueWhenDone === "boolean"
          ? parsed.closeQueueWhenDone
          : s.closeQueueWhenDone,
      enterQueues:
        typeof parsed.enterQueues === "boolean" ? parsed.enterQueues : s.enterQueues,
    }));
  } catch {
    // Corrupt/unavailable layout is non-fatal: keep defaults (fail-soft).
  }
}
