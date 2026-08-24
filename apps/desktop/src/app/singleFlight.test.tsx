// Same-tick single-flight regressions (T-028, TASK 17 §111): the guard is a
// synchronous ref latch, NOT React state — two native events in the same tick
// (double click, click + Enter) must collapse into one backend call. Rendered
// through renderToString only to obtain a live hook instance (no DOM needed).
import { describe, expect, it } from "vitest";
import { renderToString } from "react-dom/server";
import { useSingleFlight } from "./singleFlight";

interface Captured {
  busy: boolean;
  run: (fn: () => Promise<unknown>) => Promise<void>;
}

function captureHook(): Captured {
  let hook: Captured | null = null;
  function Grab() {
    hook = useSingleFlight();
    return null;
  }
  renderToString(<Grab />);
  return hook!;
}

describe("useSingleFlight (same-tick guard)", () => {
  it("two invocations in the same tick reach the backend exactly once", async () => {
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const { run } = captureHook();

    const first = run(async () => {
      calls += 1;
      await gate;
    });
    const second = run(async () => {
      calls += 1;
    });
    expect(calls).toBe(1); // synchronous latch: second call rejected
    release();
    await first;
    await second;
    expect(calls).toBe(1);
  });

  it("releases the latch on rejection so a retry can proceed", async () => {
    let calls = 0;
    const { run } = captureHook();

    await run(async () => {
      calls += 1;
      throw new Error("boom");
    }).catch(() => undefined);
    expect(calls).toBe(1);
    await run(async () => {
      calls += 1;
    });
    expect(calls).toBe(2); // released in finally: a retry is possible
  });
});