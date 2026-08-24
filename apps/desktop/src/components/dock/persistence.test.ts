import { describe, it, expect, vi, beforeEach } from "vitest";
import { store } from "../../state/store";
import {
  loadUiLayout,
  persistUiLayout,
  setDockWidth,
  toggleDockCollapsed,
  setEnterQueues,
  DOCK_MIN_WIDTH,
  DOCK_MAX_WIDTH,
} from "./persistence";

const { setSetting, getSetting } = vi.hoisted(() => ({
  setSetting: vi.fn(async () => {}),
  getSetting: vi.fn(async () => null as string | null),
}));

vi.mock("../../app/backend", () => ({
  commands: { setSetting, getSetting },
}));

beforeEach(() => {
  store.resetForTest();
  setSetting.mockClear();
  getSetting.mockClear();
});

describe("dock persistence", () => {
  it("persists the current layout as versioned JSON", async () => {
    store.patch((s) => ({
      ...s,
      dockWidth: 400,
      dockCollapsed: true,
      activeDockTab: "queue",
      closeQueueWhenDone: true,
      enterQueues: true,
    }));
    await persistUiLayout();
    expect(setSetting).toHaveBeenCalledTimes(1);
    const [key, val] = setSetting.mock.calls[0] as unknown as [string, string];
    expect(key).toBe("ui.layout.v1");
    const parsed = JSON.parse(val);
    expect(parsed.version).toBe(1);
    expect(parsed.dockWidth).toBe(400);
    expect(parsed.dockCollapsed).toBe(true);
    expect(parsed.activeDockTab).toBe("queue");
    expect(parsed.closeQueueWhenDone).toBe(true);
    expect(parsed.enterQueues).toBe(true);
  });

  it("clamps width on set (min/max bounds)", () => {
    setDockWidth(99999);
    expect(store.getState().dockWidth).toBe(DOCK_MAX_WIDTH);
    setDockWidth(1);
    expect(store.getState().dockWidth).toBe(DOCK_MIN_WIDTH);
  });

  it("loads + clamps a valid layout, rejecting a bad tab", async () => {
    getSetting.mockResolvedValue(
      JSON.stringify({
        version: 1,
        dockWidth: 100000,
        dockCollapsed: true,
        activeDockTab: "bogus",
        closeQueueWhenDone: false,
        enterQueues: true,
      }),
    );
    await loadUiLayout();
    const s = store.getState();
    expect(s.dockWidth).toBe(DOCK_MAX_WIDTH);
    expect(s.dockCollapsed).toBe(true);
    expect(s.activeDockTab).toBe("activity");
    expect(s.closeQueueWhenDone).toBe(false);
    expect(s.enterQueues).toBe(true);
  });

  it("ignores non-versioned and corrupt payloads (fail-soft)", async () => {
    getSetting.mockResolvedValue(JSON.stringify({ foo: 1 }));
    await loadUiLayout();
    expect(store.getState().activeDockTab).toBe("activity");

    getSetting.mockResolvedValue("not-json");
    await loadUiLayout();
    expect(store.getState().dockCollapsed).toBe(false);
  });

  it("toggleDockCollapsed flips and persists", async () => {
    await toggleDockCollapsed();
    expect(store.getState().dockCollapsed).toBe(true);
    expect(setSetting).toHaveBeenCalled();
  });

  it("persists the Enter action preference", () => {
    setEnterQueues(true);
    expect(store.getState().enterQueues).toBe(true);
    expect(setSetting).toHaveBeenCalled();
  });
});
