import { describe, it, expect } from "vitest";
import { renderToString } from "react-dom/server";
import { ThreadTabs } from "./ThreadTabs";
import { initialState } from "../state/store";

describe("ThreadTabs (Phase B)", () => {
  it("renders session tabs over the canonical session registry", () => {
    const st = {
      ...initialState,
      currentWorkspaceId: "w1",
      selectedEngineId: "e1",
      engines: [{ id: "e1", health: { kind: "ready" }, bound_workspace_id: null }] as any,
      sessions: [{ id: "s1", display_name: "Thread A" }] as any,
      activeSessionId: "s1",
    };
    const html = renderToString(<ThreadTabs state={st as any} onError={() => {}} />);
    expect(html).toContain("Thread A");
    expect(html).toContain("+");
    expect(html).toContain("thread-tab--active");
  });

  it("disables the new-thread button without a workspace", () => {
    const html = renderToString(
      <ThreadTabs state={{ ...initialState, currentWorkspaceId: null } as any} onError={() => {}} />,
    );
    expect(html).toContain("disabled");
  });

  it("disables the new-thread button when no engine is ready", () => {
    const st = {
      ...initialState,
      currentWorkspaceId: "w1",
      selectedEngineId: "e1",
      engines: [{ id: "e1", health: { kind: "stopped" }, bound_workspace_id: null }] as any,
    };
    const html = renderToString(<ThreadTabs state={st as any} onError={() => {}} />);
    expect(html).toContain("disabled");
  });
});
