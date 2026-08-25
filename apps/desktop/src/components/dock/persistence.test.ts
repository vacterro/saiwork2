import { describe, it, expect, vi, beforeEach } from "vitest";
import { store } from "../../state/store";
import {
  createDockResizeCoalescer,
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

  it("setDockWidth is a semantic NO-OP when the clamped width is unchanged (AUDIT-PERF-005)", () => {
    store.patch((s) => ({ ...s, dockWidth: 400 }));
    let notifications = 0;
    const unsub = store.subscribe(() => notifications++);
    // Hundreds of identical movements: zero Store commits.
    for (let i = 0; i < 200; i++) setDockWidth(400);
    expect(notifications).toBe(0);
    // A real change commits exactly once…
    setDockWidth(401.4); // rounds to 401
    expect(notifications).toBe(1);
    expect(store.getState().dockWidth).toBe(401);
    // …and a drag PAST the max rail commits only the first clamped landing;
    // every identical clamped movement afterwards is a no-op.
    for (let i = 0; i < 200; i++) setDockWidth(DOCK_MAX_WIDTH + 100 + i);
    expect(notifications).toBe(2);
    expect(store.getState().dockWidth).toBe(DOCK_MAX_WIDTH);
    unsub();
  });
});

describe("dock resize frame coalescing (AUDIT-PERF-005)", () => {
  function harness() {
    const queue: Array<() => void> = [];
    let cancelled = 0;
    const coalescer = createDockResizeCoalescer({
      raf: (cb) => {
        queue.push(cb);
        return queue.length;
      },
      caf: () => {
        cancelled += 1;
      },
    });
    return { coalescer, queue, cancelledCount: () => cancelled };
  }

  it("hundreds of pointermoves inside one frame commit at most once", () => {
    const { coalescer, queue } = harness();
    const commits: number[] = [];
    const start = { x: 100, w: 360 };
    // Dragging LEFT (clientX shrinks) grows the width; newest coordinate wins.
    for (let i = 0; i < 300; i++) {
      coalescer.move(start, 100 - i, (w) => commits.push(w));
    }
    expect(queue.length).toBe(1); // exactly one frame scheduled
    queue[0]!();
    expect(commits).toEqual([360 + 299]); // newest coordinate wins
    // A second burst schedules exactly one more frame.
    for (let i = 0; i < 300; i++) {
      coalescer.move(start, 90 - i, (w) => commits.push(w));
    }
    expect(queue.length).toBe(2);
  });

  it("end() cancels the pending frame and commits the FINAL released position", () => {
    const { coalescer, queue, cancelledCount } = harness();
    const start = { x: 100, w: 360 };
    const commits: number[] = [];
    coalescer.move(start, 80, (w) => commits.push(w)); // frame pending
    coalescer.end(start, 60, (w) => commits.push(w));
    expect(cancelledCount()).toBe(1); // pending frame must be cancelled
    expect(commits).toEqual([360 + 40]); // final released width is the authority
    // The cancelled frame never fires a stale commit.
    for (const cb of queue) cb();
    expect(commits).toHaveLength(1);
  });

  it("cancel()/no-drag boundaries are inert and leak nothing", () => {
    const { coalescer } = harness();
    const commits: number[] = [];
    expect(coalescer.move(null, 10, (w) => commits.push(w))).toBe(false);
    expect(coalescer.end(null, 10, (w) => commits.push(w))).toBe(false);
    coalescer.cancel(); // no pending frame: no caf call, no throw
    expect(commits).toEqual([]);
  });
});
