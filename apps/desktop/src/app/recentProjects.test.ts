// T-126: recent-projects ordering ring — bump/dedup/bound, persistence
// round-trip (fire-and-forget save, fail-soft validated load).
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { store } from "../state/store";
import { commands } from "./backend";
import {
  UI_RECENTS_KEY,
  MAX_RECENT_WORKSPACES,
  bumpRecentWorkspace,
  loadRecentProjects,
  resetRecentProjectsForTest,
} from "./recentProjects";

vi.mock("./backend", () => ({
  commands: {
    getSetting: vi.fn(),
    setSetting: vi.fn().mockResolvedValue(undefined),
  },
}));

describe("recent projects ordering (T-126)", () => {
  beforeEach(() => {
    store.resetForTest();
    resetRecentProjectsForTest();
    vi.clearAllMocks();
    vi.mocked(commands.setSetting).mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("bump moves the id to the front and dedups", () => {
    bumpRecentWorkspace("a");
    bumpRecentWorkspace("b");
    bumpRecentWorkspace("a");
    expect(store.getState().recentWorkspaceIds).toEqual(["a", "b"]);
  });

  it("the ring is bounded at MAX_RECENT_WORKSPACES (no unbounded growth)", () => {
    for (let i = 0; i < MAX_RECENT_WORKSPACES + 5; i++) bumpRecentWorkspace(`w${i}`);
    const ids = store.getState().recentWorkspaceIds;
    expect(ids.length).toBe(MAX_RECENT_WORKSPACES);
    expect(ids[0]).toBe(`w${MAX_RECENT_WORKSPACES + 4}`); // newest first
    expect(ids).not.toContain("w0"); // oldest evicted
  });

  it("bump persists through the generic ui.recents.v1 setting", () => {
    bumpRecentWorkspace("a");
    expect(commands.setSetting).toHaveBeenCalledWith(
      UI_RECENTS_KEY,
      JSON.stringify({ version: 1, ids: ["a"] }),
    );
  });

  it("load restores a valid persisted ring and rejects malformed shapes", async () => {
    vi.mocked(commands.getSetting).mockResolvedValueOnce(
      JSON.stringify({ version: 1, ids: ["x", "y"] }),
    );
    await loadRecentProjects();
    expect(store.getState().recentWorkspaceIds).toEqual(["x", "y"]);

    // Wrong version → ignored.
    vi.mocked(commands.getSetting).mockResolvedValueOnce(
      JSON.stringify({ version: 2, ids: ["z"] }),
    );
    await loadRecentProjects();
    expect(store.getState().recentWorkspaceIds).toEqual(["x", "y"]);

    // Non-string ids filtered out; corrupt JSON fails soft.
    vi.mocked(commands.getSetting).mockResolvedValueOnce(
      JSON.stringify({ version: 1, ids: ["ok", 7, null] }),
    );
    await loadRecentProjects();
    expect(store.getState().recentWorkspaceIds).toEqual(["ok"]);

    vi.mocked(commands.getSetting).mockResolvedValueOnce("{corrupt");
    await loadRecentProjects();
    expect(store.getState().recentWorkspaceIds).toEqual(["ok"]);
  });
});