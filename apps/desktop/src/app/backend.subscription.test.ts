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

  it("persistent failures NEVER reject — warn once into the store, keep retrying (T-127)", async () => {
    const { subscribeToCoreEvents } = await import("./backend");
    const { store } = await import("../state/store");
    let listens = 0;
    listenImpl = () => {
      listens++;
      // Fail through the warn threshold (attempt 4 = 5th try), then succeed:
      // the reconnect loop must still be alive after the warning has fired.
      return listens <= 5 ? Promise.reject(new Error("transient")) : Promise.resolve(() => {});
    };
    const sub = subscribeToCoreEvents();
    // Fast-phase backoff caps at 2000ms, so 8 attempts settle in seconds.
    await expect(sub.ready).resolves.toBeUndefined();
    // The failure was surfaced exactly once (never silently, never a reject).
    expect(store.getState().lastError).toContain("reconnecting");
    sub.dispose();
    expect(listens).toBeGreaterThan(5);
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
