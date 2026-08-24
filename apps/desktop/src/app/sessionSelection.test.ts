import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { commands } from "./backend";
import { deleteSession, loadSessionHistory } from "./sessionSelection";
import { store } from "../state/store";

const deleted = {
  id: "s-delete",
  workspace_id: "w1",
  engine_id: "opencode",
  engine_session_id: "up-delete",
  display_name: "delete me",
  created_at: 1,
  running: false,
  resumable: true,
  usable_now: true,
} as const;

const replacement = { ...deleted, id: "s-keep", engine_session_id: "up-keep", display_name: "keep" };

beforeEach(() => {
  store.resetForTest();
  store.patch((state) => ({
    ...state,
    currentWorkspaceId: "w1",
    activeSessionId: deleted.id,
    sessions: [deleted, replacement],
    messages: {
      [deleted.id]: [{
        id: "old",
        role: "assistant",
        runId: "r1",
        status: "complete",
        text: "old",
        tools: [],
        permissions: [],
        questions: [],
        ts: 1,
      }],
    },
  }));
});

afterEach(() => {
  vi.restoreAllMocks();
  store.resetForTest();
});

describe("session deletion projection", () => {
  it("prunes after authoritative delete even if refresh fails and discards an older history read", async () => {
    let releaseHistory!: (history: Awaited<ReturnType<typeof commands.sessionHistory>>) => void;
    const delayedHistory = new Promise<Awaited<ReturnType<typeof commands.sessionHistory>>>((resolve) => {
      releaseHistory = resolve;
    });
    vi.spyOn(commands, "sessionHistory").mockReturnValue(delayedHistory);
    vi.spyOn(commands, "deleteSession").mockResolvedValue(undefined);
    vi.spyOn(commands, "listSessions").mockRejectedValue(new Error("refresh unavailable"));

    const staleRead = loadSessionHistory(deleted.id);
    await Promise.resolve();
    await deleteSession(deleted.id);
    releaseHistory([{
      id: "late",
      role: "assistant",
      text: "must not return",
      tool_call_id: null,
      tool: null,
      order: 1,
      ts: 2,
    }]);
    await staleRead;

    const state = store.getState();
    expect(state.sessions.map((session) => session.id)).toEqual([replacement.id]);
    expect(state.activeSessionId).toBe(replacement.id);
    expect(state.messages[deleted.id]).toBeUndefined();
  });
});
