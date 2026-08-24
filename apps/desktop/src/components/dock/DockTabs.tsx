import type { DockTab } from "./types";

interface Props {
  active: DockTab;
  onSelect: (t: DockTab) => void;
  onCollapse: () => void;
  badges: Partial<Record<DockTab, number>>;
  closeQueueWhenDone: boolean;
  onCloseQueueWhenDone: (on: boolean) => void;
  /** The close-when-done control is meaningful only while the queue actually
   * holds items; an empty queue is already "done". */
  queueHasItems: boolean;
}

// Exhaustive order of the shipped dock tabs — intentionally ONLY these.
// Changes / Preview / Terminal are not shipped yet and are NOT rendered as
// tabs (the directive forbids fake / "coming soon" controls).
const ORDER: DockTab[] = ["activity", "queue", "diag", "files"];
const LABEL: Record<DockTab, string> = {
  queue: "Queue",
  activity: "Activity",
  diag: "Diag",
  files: "Files",
};

export function DockTabs({
  active,
  onSelect,
  onCollapse,
  badges,
  closeQueueWhenDone,
  onCloseQueueWhenDone,
  queueHasItems,
}: Props) {
  return (
    <div className="dock-tabs">
      <div className="dock-tabs__list" role="tablist" aria-label="Dock panels">
        {ORDER.map((t) => {
          const badge = badges[t];
          return (
            <button
              key={t}
              role="tab"
              aria-selected={t === active}
              className={"dock-tab" + (t === active ? " dock-tab--active" : "")}
              onClick={() => onSelect(t)}
              title={LABEL[t]}
            >
              <span>{LABEL[t]}</span>
              {badge ? <span className="dock-tab__badge">{badge}</span> : null}
            </button>
          );
        })}
      </div>
      <div className="dock-tabs__tools">
        {active === "queue" && queueHasItems && (
          <label
            className="dock-tabs__closeq"
            title="Automatically collapse the Queue tab when the queue becomes empty"
          >
            <input
              type="checkbox"
              checked={closeQueueWhenDone}
              onChange={(e) => onCloseQueueWhenDone(e.target.checked)}
            />
            close when done
          </label>
        )}
        <button className="dock-tabs__collapse" onClick={onCollapse} title="Collapse dock to a rail">
          &#x2039;
        </button>
      </div>
    </div>
  );
}
