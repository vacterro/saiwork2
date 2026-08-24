// Phase B dock contract: tab identity + versioned durable layout.
//
// The tab set is exactly the features that have shipped. Changes / Preview /
// Terminal remain real, scoped capabilities (git status + bounded diff;
// explicit managed preview targets; PTY via ProcessSupervisor) but are NOT
// shipped yet — the directive forbids shipping fake controls or "coming soon"
// tabs that imply capability. They are intentionally absent from the type so
// no code path can render a non-functional tab; each earns its own DockTab
// only when its ticket lands (and its own authority + fail-closed reads).
// Their phase letters are recorded in KNOWLEDGE/ROADMAP.md, not in the UI.
export type DockTab = "activity" | "queue" | "diag" | "files";

export const UI_LAYOUT_KEY = "ui.layout.v1";

export interface UiLayoutV1 {
  version: 1;
  dockWidth: number;
  dockCollapsed: boolean;
  activeDockTab: DockTab;
  closeQueueWhenDone: boolean;
  enterQueues: boolean;
}

export const ALL_DOCK_TABS: DockTab[] = ["activity", "queue", "diag", "files"];
