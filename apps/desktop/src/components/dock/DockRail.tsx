import type { DockTab } from "./types";

interface Props {
  activeTab: DockTab;
  onSelect: (t: DockTab) => void;
  onExpand: () => void;
}

// Exhaustive order of the shipped dock tabs — intentionally ONLY these.
const ORDER: DockTab[] = ["activity", "queue", "diag", "files"];
const GLYPH: Record<DockTab, string> = {
  activity: "A",
  queue: "Q",
  diag: "D",
  files: "F",
};

export function DockRail({ activeTab, onSelect, onExpand }: Props) {
  return (
    <nav className="dock-rail" aria-label="Dock tabs">
      <button className="dock-rail__expand" onClick={onExpand} title="Expand dock" aria-label="Expand dock">
        &#x203a;
      </button>
      {ORDER.map((t) => (
        <button
          key={t}
          className={"dock-rail__tab" + (t === activeTab ? " dock-rail__tab--active" : "")}
          onClick={() => onSelect(t)}
          title={t}
          aria-label={t}
        >
          {GLYPH[t]}
        </button>
      ))}
    </nav>
  );
}
