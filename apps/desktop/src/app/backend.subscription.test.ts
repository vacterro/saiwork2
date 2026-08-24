// T-075 regression tests: subscription lifecycle error hygiene.
//
// Disposal is NORMAL lifecycle (React StrictMode mount/unmount/remount in
// dev). These tests pin the contract: a disposed subscription NEVER rejects
// its `ready` promise — teardown resolves silently; the ONLY rejecting path
// is every retry attempt failing while the session is still live.
import { describe, expect, it, vi, afterEach, beforeEach } from "vitest";

let listenImpl: () => Promise<() => void>;

vi.mock("@tauri-apps/api/event", () => ({
  listen: () => listenImpl(),
}));

describe("subscribeToCoreEvents lifecycle (T-075)", () => {
  beforeEach(() => {
    (globalThis as Record<string, unknown>).window = { __TAURI_INTERNALS__: {} };
    vi.restoreAllMocks();
  });
  afterEach(() => {
    delete (globalThis as Record<string, unknown>).window;
  });

  it("dispose BEFORE the listener installs resolves silently — never 'subscribe disposed' (regression)", async () => {
    const { subscribeToCoreEvents } = await import("./backend");
    let listenCalls = 0;
    listenImpl = () => {
      listenCalls++;
      return Promise.resolve(() => {});
    };
    const sub = subscribeToCoreEvents();
    // Dispose lands while the dynamic import is still in flight.
    sub.dispose();
    await expect(sub.ready).resolves.toBeUndefined();
    // Give every pending macrotask a chance to misbehave.
    await new Promise((r) => setTimeout(r, 20));
    // The pre-import disposed guard short-circuits: listen is never invoked,
    // so there is nothing to leak and nothing to reject with.
    expect(listenCalls).toBe(0);
  });

  it("a listen failure while already disposed resolves silently (no rejection)", async () => {
    const { subscribeToCoreEvents } = await import("./backend");
    listenImpl = () => Promise.reject(new Error("boom"));
    const sub = subscribeToCoreEvents();
    // Dispose lands while the first failed attempt is still in flight.
    sub.dispose();
    await expect(sub.ready).resolves.toBeUndefined();
  });

  it("every attempt failing on a LIVE session rejects with the real error (only rejecting path)", async () => {
    const { subscribeToCoreEvents } = await import("./backend");
    listenImpl = () => Promise.reject(new Error("plumbing dead"));
    const sub = subscribeToCoreEvents();
    await expect(sub.ready).rejects.toThrow("plumbing dead");
  });

  it("success path resolves and dispose detaches exactly once", async () => {
    const { subscribeToCoreEvents } = await import("./backend");
    const unlisten = vi.fn();
    listenImpl = () => Promise.resolve(unlisten);
    const sub = subscribeToCoreEvents();
    await expect(sub.ready).resolves.toBeUndefined();
    sub.dispose();
    sub.dispose(); // idempotent
    expect(unlisten).toHaveBeenCalledTimes(1);
  });
});
