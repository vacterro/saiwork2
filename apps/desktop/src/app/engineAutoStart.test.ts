import { afterEach, describe, expect, it, vi } from "vitest";
import type { EngineInfo } from "@saiwork2/contracts";
import { commands } from "./backend";
import { store } from "../state/store";
import { requestEngineAutoStart, resetEngineAutoStartForTest } from "./engineAutoStart";
import { engineCanBeAutoStarted, ensureEngineStarted } from "./engineAutoStart";

function engine(health: EngineInfo["health"], bound: string | null): EngineInfo {
  return {
    id: "opencode",
    display_name: "OpenCode",
    version: "1",
    experimental: false,
    health,
    bound_workspace_id: bound,
    capabilities: {} as EngineInfo["capabilities"],
  };
}

afterEach(() => {
  vi.restoreAllMocks();
  resetEngineAutoStartForTest();
  store.resetForTest();
});

describe("latest-intent engine auto-start", () => {
  it("lets a newer workspace intent win while an older rebind is stopping", async () => {
    let releaseStop!: () => void;
    const stopGate = new Promise<void>((resolve) => { releaseStop = resolve; });
    let stopEntered!: () => void;
    const entered = new Promise<void>((resolve) => { stopEntered = resolve; });
    const starts: Array<[string, string | null]> = [];

    vi.spyOn(commands, "listEngines")
      .mockResolvedValueOnce([engine("ready", "old")])
      .mockResolvedValueOnce([engine("stopped", null)]);
    vi.spyOn(commands, "stopEngine").mockImplementation(async () => {
      stopEntered();
      await stopGate;
    });
    vi.spyOn(commands, "startEngine").mockImplementation(async (id, workspaceId) => {
      starts.push([id, workspaceId]);
    });

    const old = requestEngineAutoStart("opencode", "workspace-a");
    await entered;
    const superseded = requestEngineAutoStart("opencode", "workspace-middle");
    const latest = requestEngineAutoStart("opencode", "workspace-b");
    releaseStop();
    await Promise.all([old, superseded, latest]);

    expect(starts).toEqual([["opencode", "workspace-b"]]);
    expect(commands.listEngines).toHaveBeenCalledTimes(2);
  });

  it("does not restart a ready workspace-agnostic engine", async () => {
    vi.spyOn(commands, "listEngines").mockResolvedValue([engine("ready", null)]);
    const stop = vi.spyOn(commands, "stopEngine").mockResolvedValue(undefined);
    const start = vi.spyOn(commands, "startEngine").mockResolvedValue(undefined);

    await requestEngineAutoStart("opencode", "workspace-a");

    expect(stop).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  it("a foreign in-flight start does not strand the newest binding (AUDIT-W2-005)", async () => {
    // Start(A) is held in `starting`; the user selects B; A's terminal lands
    // bound to A. The owner must NOT treat B as satisfied at `starting` — it
    // waits for the terminal, observes Ready@A, and rebinds to B. Exactly one
    // start command ever runs (never a racing second start).
    const starts: Array<[string, string | null]> = [];
    vi.spyOn(commands, "listEngines")
      .mockResolvedValueOnce([engine("starting", null)]) // A round 1: sees starting
      .mockResolvedValueOnce([engine("ready", "workspace-a")]) // B: settled runtime mismatches
      .mockResolvedValueOnce([engine("stopped", null)]); // B: after stop, start
    const stop = vi.spyOn(commands, "stopEngine").mockResolvedValue(undefined);
    vi.spyOn(commands, "startEngine").mockImplementation(async (id, workspaceId) => {
      starts.push([id, workspaceId]);
    });

    const a = requestEngineAutoStart("opencode", "workspace-a");
    // Let intent A reach its starting-wait subscription.
    for (let i = 0; i < 8; i++) await Promise.resolve();
    const b = requestEngineAutoStart("opencode", "workspace-b");

    // The manual Start(A) settles terminal: READY bound to workspace.
    store.patch((s) => ({ ...s, engines: [engine("ready", "workspace-a")] }));

    await Promise.all([a, b]);
    expect(starts).toEqual([["opencode", "workspace-b"]]);
    expect(stop).toHaveBeenCalledTimes(1);
  });

  describe("engineCanBeAutoStarted / ensureEngineStarted (T-125)", () => {
  it("reports startable only for stopped/failed/unknown engines", async () => {
    store.resetForTest();
    store.patch((s) => ({ ...s, engines: [engine("ready", null)] }));
    expect(engineCanBeAutoStarted("opencode")).toBe(false);
    store.patch((s) => ({ ...s, engines: [engine("starting", null)] }));
    expect(engineCanBeAutoStarted("opencode")).toBe(false);
    store.patch((s) => ({ ...s, engines: [engine("stopped", null)] }));
    expect(engineCanBeAutoStarted("opencode")).toBe(true);
    store.patch((s) => ({ ...s, engines: [engine({ kind: "failed", message: "boom" }, null)] }));
    expect(engineCanBeAutoStarted("opencode")).toBe(true);
    expect(engineCanBeAutoStarted(null)).toBe(false);
  });

  it("starts a startable engine and resolves once it leaves startable", async () => {
    vi.spyOn(commands, "listEngines").mockResolvedValue([engine("stopped", null)]);
    const start = vi.spyOn(commands, "startEngine").mockResolvedValue(undefined);
    store.patch((s) => ({ ...s, engines: [engine("stopped", null)] }));

    const settled = ensureEngineStarted("opencode", "workspace-a");
    // The auto-start sends the start command and returns; the engine then gets
    // an authoritative READY event. Resolve the wait via the store watcher.
    await Promise.resolve();
    await Promise.resolve();
    store.patch((s) => ({ ...s, engines: [engine("ready", "workspace-a")] }));

    expect(start).toHaveBeenCalledWith("opencode", "workspace-a");
    expect(await settled).toBe(true);
  });

  it("already-ready engines resolve immediately without a start", async () => {
    vi.spyOn(commands, "listEngines").mockResolvedValue([engine("ready", null)]);
    const start = vi.spyOn(commands, "startEngine").mockResolvedValue(undefined);
    store.patch((s) => ({ ...s, engines: [engine("ready", null)] }));

    expect(await ensureEngineStarted("opencode", "workspace-a")).toBe(true);
    expect(start).not.toHaveBeenCalled();
  });
});
});
