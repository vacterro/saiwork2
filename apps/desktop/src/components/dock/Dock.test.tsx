import { describe, it, expect } from "vitest";
import { renderToString } from "react-dom/server";
import { Dock } from "./Dock";
import { initialState } from "../../state/store";

function withState(patch: Record<string, unknown>): any {
  return { ...initialState, ...patch };
}

function dockProps(st: any) {
  const onError = () => {};
  return { state: st, activity: { state: st, onError }, queue: { state: st, onError }, files: { state: st, onError }, onError };
}

describe("Dock (Phase B)", () => {
  it("renders tabs and derives the Queue badge from authoritative queue state", () => {
    const st = withState({
      queue: {
        status: "ready",
        paused: false,
        items: [{ state: "queued" }, { state: "queued" }, { state: "running" }] as any,
        revision: 0,
        stale: false,
      },
    });
    const html = renderToString(<Dock {...dockProps(st)} />);
    expect(html).toContain("Queue");
    expect(html).toContain("dock-tab__badge");
    expect(html).toContain(">2<");
  });

  it("renders the collapsed rail (no tab list) when dockCollapsed", () => {
    const st = withState({ dockCollapsed: true });
    const html = renderToString(<Dock {...dockProps(st)} />);
    expect(html).toContain("dock-rail");
    expect(html).not.toContain("dock-tabs__list");
  });

  it("renders exactly the shipped dock tabs (no fake/coming-soon tabs)", () => {
    const html = renderToString(<Dock {...dockProps(withState({}))} />);
    // Shipped: Activity / Queue / Diag / Files (Phase C).
    for (const t of ["Activity", "Queue", "Diag", "Files"]) {
      expect(html).toContain(t);
    }
    // The dock's activeDockTab is the SINGLE tab authority — the old inner
    // ACTIVITY/QUEUE/DIAG sub-tab system (uppercase, local state) is gone.
    for (const t of ["ACTIVITY", "QUEUE", "DIAG"]) {
      expect(html).not.toContain(t);
    }
    // Changes / Preview / Terminal are not shipped yet and must NOT be
    // rendered as tabs (the directive forbids fake / "coming soon" controls).
    for (const t of ["Changes", "Preview", "Terminal"]) {
      expect(html).not.toContain(t);
    }
  });

  it("mounts the Files panel body for the files tab; no workspace shows the honest empty state", () => {
    const st = withState({ activeDockTab: "files" });
    const html = renderToString(<Dock {...dockProps(st)} />);
    expect(html).toContain("files__empty");
  });

  it("mounts exactly ONE tab body — the active tab's own panel", () => {
    const st = withState({ activeDockTab: "queue" });
    const html = renderToString(<Dock {...dockProps(st)} />);
    expect(html).toContain("queue-panel");
    expect(html).not.toContain("activity__run");
    expect(html).not.toContain("diag");
  });

  it("renders the activity facts panel for the activity tab", () => {
    const st = withState({
      activeDockTab: "activity",
      activeSessionId: "s1",
      running: { s1: "run-1" },
      activeMessage: {
        s1: { id: "run-1-assistant", runId: "run-1", status: "streaming", text: "hello", tools: [], permissions: [] },
      },
    });
    const html = renderToString(<Dock {...dockProps(st)} />);
    expect(html).toContain("activity__run");
    expect(html).toContain("running");
    expect(html).not.toContain("hello");
  });
});