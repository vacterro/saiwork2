import { afterEach, describe, expect, it, vi } from "vitest";
import type { EngineInfo } from "@saiwork2/contracts";
import { commands } from "./backend";
import { requestEngineAutoStart, resetEngineAutoStartForTest } from "./engineAutoStart";

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
});
