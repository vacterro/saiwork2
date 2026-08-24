import React, { useCallback, useEffect, useRef, useState } from "react";
import type { SliceProps } from "../../state/slices";
import {
  DOCK_MAX_WIDTH,
  DOCK_MIN_WIDTH,
  setDockTab,
  setDockWidth,
  setCloseQueueWhenDone,
  toggleDockCollapsed,
  persistUiLayout,
} from "./persistence";
import { DockTabs } from "./DockTabs";
import { DockRail } from "./DockRail";
import { activityPanelKeys, activityEqual, ActivityDockPanel } from "../ActivityPanel";
import { QueuePanel, queuePanelKeys } from "../QueuePanel";
import { DiagnosticsPanel } from "../DiagnosticsPanel";
import { FilesPanel, filesPanelKeys } from "../FilesPanel";
import { sliceEqual } from "../../state/slices";

/** Dock-layout slice: what the shell, rail, tabs and badge read (excluding the
 * sub-panel payloads — those arrive as dedicated slices so each panel's memo
 * can compare its OWN domain). */
export const dockKeys = [
  "dockCollapsed",
  "activeDockTab",
  "dockWidth",
  "closeQueueWhenDone",
  "queue",
] as const;

type DockLayout = SliceProps<(typeof dockKeys)[number]>;
export type DockProps = DockLayout & {
  activity: SliceProps<(typeof activityPanelKeys)[number]>;
  queue: SliceProps<(typeof queuePanelKeys)[number]>;
  files: SliceProps<(typeof filesPanelKeys)[number]>;
};

const layoutEqual = sliceEqual(dockKeys);

/** The dock rerenders when the layout changes OR when the ACTIVE panel's own
 * slice changes (it must forward the new slice to the memoized sub-panel).
 * Inactive panels' slices are ignored — their memoized instances are unmounted
 * anyway (exactly one tab body is mounted). */
export function dockEqual(prev: DockProps, next: DockProps): boolean {
  if (!layoutEqual(prev, next)) return false;
  if (next.state.activeDockTab === "activity" && !activityEqual(prev.activity, next.activity)) return false;
  if (next.state.activeDockTab === "queue" && !sliceEqual(queuePanelKeys)(prev.queue, next.queue)) return false;
  if (next.state.activeDockTab === "files" && !sliceEqual(filesPanelKeys)(prev.files, next.files)) return false;
  return true;
}

const RESPONSIVE_MIN_WIDTH = 1100;

export function Dock({ state, activity, queue, files }: DockProps) {
  const { dockCollapsed, activeDockTab, dockWidth, closeQueueWhenDone } = state;
  const [dragging, setDragging] = useState(false);
  // Responsive collapse is separate from the user's preferred collapsed state:
  // window resize must never permanently overwrite dockCollapsed. The rail's
  // expand is TRANSIENT while narrow (see handleExpand) so a responsive
  // collapse never persists an unintended collapsed preference (T-050).
  const [narrow, setNarrow] = useState(
    typeof window !== "undefined" && window.innerWidth < RESPONSIVE_MIN_WIDTH,
  );
  const [transientExpand, setTransientExpand] = useState(false);

  useEffect(() => {
    const onResize = () => setNarrow(window.innerWidth < RESPONSIVE_MIN_WIDTH);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // A responsive expansion is transient only — drop it the moment we are wide
  // again so the durable user preference (dockCollapsed) is restored exactly.
  useEffect(() => {
    if (!narrow) setTransientExpand(false);
  }, [narrow]);

  const effectiveCollapsed = dockCollapsed || narrow;
  const railMode = effectiveCollapsed && !(narrow && transientExpand);

  // Expand never mutates the durable preference while the collapse is responsive
  // (narrow): it only lifts the transient overlay. Only a user collapse above
  // the breakpoint toggles the persisted dockCollapsed.
  const handleExpand = useCallback(() => {
    if (narrow) setTransientExpand(true);
    else toggleDockCollapsed();
  }, [narrow]);
  const handleCollapse = useCallback(() => {
    if (narrow) setTransientExpand(false);
    else toggleDockCollapsed();
  }, [narrow]);

  // PERF-007: pointer-capture drag. The resizer takes the pointer on
  // pointerdown via `setPointerCapture`, so move/up/cancel are delivered to the
  // element itself — even when the cursor leaves the window or the dock tabs
  // mount/unmount mid-drag. No `window` listeners, so there is nothing to leak
  // on unmount, blur or pointer-cancel.
  const dragStart = useRef<{ x: number; w: number } | null>(null);

  const onResizeStart = useCallback(
    (e: React.PointerEvent) => {
      e.preventDefault();
      dragStart.current = { x: e.clientX, w: state.dockWidth };
      setDragging(true);
      (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
    },
    [state.dockWidth],
  );

  const onResizeMove = useCallback((e: React.PointerEvent) => {
    const start = dragStart.current;
    if (!start) return;
    // Dock pinned to the right edge: dragging left grows the width. Only the
    // live projection updates at pointer rate; the durable write happens once
    // on pointerup (see onResizeEnd).
    setDockWidth(start.w + (start.x - e.clientX));
  }, []);

  const onResizeEnd = useCallback((e: React.PointerEvent) => {
    if (!dragStart.current) return;
    dragStart.current = null;
    setDragging(false);
    const el = e.currentTarget as HTMLElement;
    if (el.hasPointerCapture?.(e.pointerId)) {
      el.releasePointerCapture(e.pointerId);
    }
    // Exactly one durable commit for the completed drag (carries the final
    // released — and clamped — width). Tab/collapse/close-when-done preferences
    // keep their own one-action/one-write behavior.
    persistUiLayout();
  }, []);

  const queueBadge = state.queue.items.filter((i) => i.state === "queued").length;

  if (railMode) {
    return (
      <DockRail
        activeTab={activeDockTab}
        onSelect={setDockTab}
        onExpand={handleExpand}
      />
    );
  }

  return (
    <section className="dock" style={{ width: dockWidth }}>
      <DockTabs
        active={activeDockTab}
        onSelect={setDockTab}
        onCollapse={handleCollapse}
        badges={{ queue: queueBadge }}
        closeQueueWhenDone={closeQueueWhenDone}
        onCloseQueueWhenDone={setCloseQueueWhenDone}
        queueHasItems={queueBadge > 0}
      />
      <div className="dock__body">
        {/* Exactly ONE tab body is mounted — the dock's activeDockTab is the
            single tab authority (persisted), and each panel receives its own
            memoized slice (Activity: facts-only; Queue: full mutation slice;
            Diag: no props — a store read through its own subscription). */}
        {activeDockTab === "activity" && <MemoActivityDockPanel {...activity} />}
        {activeDockTab === "queue" && <MemoQueuePanel {...queue} />}
        {activeDockTab === "diag" && <DiagnosticsPanel />}
        {activeDockTab === "files" && <MemoFilesPanel {...files} />}
      </div>
      <div
        className={"dock__resizer" + (dragging ? " dock__resizer--active" : "")}
        onPointerDown={onResizeStart}
        onPointerMove={onResizeMove}
        onPointerUp={onResizeEnd}
        onPointerCancel={onResizeEnd}
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize dock"
        aria-valuemin={DOCK_MIN_WIDTH}
        aria-valuemax={DOCK_MAX_WIDTH}
        aria-valuenow={dockWidth}
      />
    </section>
  );
}

const MemoActivityDockPanel = React.memo(ActivityDockPanel, activityEqual);
const MemoQueuePanel = React.memo(QueuePanel, sliceEqual(queuePanelKeys));
const MemoFilesPanel = React.memo(FilesPanel, sliceEqual(filesPanelKeys));