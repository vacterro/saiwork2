import { describe, expect, it } from "vitest";
import { composerActionForKey, ownsAutoCreatedSession } from "./Composer";

describe("composer Enter preference", () => {
  it("queues on Enter when enabled and keeps Shift+Enter for newline", () => {
    expect(composerActionForKey({ key: "Enter", shiftKey: false, ctrlKey: false }, true)).toBe("queue");
    expect(composerActionForKey({ key: "Enter", shiftKey: true, ctrlKey: false }, true)).toBe("newline");
    expect(composerActionForKey({ key: "Enter", shiftKey: false, ctrlKey: true }, true)).toBe("send");
  });

  it("preserves Send on Enter when the preference is disabled", () => {
    expect(composerActionForKey({ key: "Enter", shiftKey: false, ctrlKey: false }, false)).toBe("send");
    expect(composerActionForKey({ key: "Enter", shiftKey: false, ctrlKey: true }, false)).toBe("queue");
  });
});

describe("auto-created session ownership", () => {
  const created = {
    id: "s-new",
    workspace_id: "w1",
    engine_id: "opencode",
  } as never;

  it("accepts only the still-current workspace, engine and active selection", () => {
    expect(ownsAutoCreatedSession({ activeSessionId: null, currentWorkspaceId: "w1", selectedEngineId: "opencode" }, created, "w1", "opencode")).toBe(true);
    expect(ownsAutoCreatedSession({ activeSessionId: "s-new", currentWorkspaceId: "w1", selectedEngineId: "opencode" }, created, "w1", "opencode")).toBe(true);
    expect(ownsAutoCreatedSession({ activeSessionId: null, currentWorkspaceId: "w2", selectedEngineId: "opencode" }, created, "w1", "opencode")).toBe(false);
    expect(ownsAutoCreatedSession({ activeSessionId: null, currentWorkspaceId: "w1", selectedEngineId: "other" }, created, "w1", "opencode")).toBe(false);
    expect(ownsAutoCreatedSession({ activeSessionId: "s-user-picked", currentWorkspaceId: "w1", selectedEngineId: "opencode" }, created, "w1", "opencode")).toBe(false);
  });
});
