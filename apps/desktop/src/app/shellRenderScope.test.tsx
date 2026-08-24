// Shell render-scope test (TASK 24 perf): App is the single store subscriber,
// so every batched token update used to rerender the whole shell. The memo
// comparators gate each panel to its own slice; this test drives a real 10k
// delta stream through the store and asserts that text-only batches rerender
// ONLY Conversation (which is intentionally unmemoized at App level — it may
// render per batch), never the title bar, lists, composer, SAIPEN bar, status
// line, or activity panel.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { store, type AppState } from "../state/store";
import type { Envelope } from "@saiwork2/contracts";
import { parseEnvelope } from "../state/events";
import { sliceEqual } from "../state/slices";
import { titleBarKeys } from "../components/TitleBar";
import { projectSidebarKeys } from "../components/ProjectSidebar";
import { sessionListKeys } from "../components/SessionList";
import { composerKeys } from "../components/Composer";
import { saipenBarKeys } from "../components/SaipenBar";
import { statusLineKeys } from "../components/StatusLine";
import { activityEqual } from "../components/ActivityPanel";

// Each panel declares ONE key tuple (co-located with the component) which types
// its props AND generates its memo comparator (state/slices.ts). These are the
// same comparators App uses to memoize the shell — so this test exercises the
// real gate, not a parallel copy.
const titleBarEqual = sliceEqual(titleBarKeys);
const projectSidebarEqual = sliceEqual(projectSidebarKeys);
const sessionListEqual = sliceEqual(sessionListKeys);
const composerEqual = sliceEqual(composerKeys);
const saipenBarEqual = sliceEqual(saipenBarKeys);
const statusLineEqual = sliceEqual(statusLineKeys);
const activityPanelEqual = activityEqual;

function env(type: string, payload: Record<string, unknown>, seq = 1): Envelope {
  return { seq, ts: Date.now(), type, ...payload } as Envelope;
}

function dispatch(type: string, payload: Record<string, unknown>, seq = 1): void {
  store.dispatch(parseEnvelope(env(type, payload, seq)));
}

beforeEach(() => {
  store.resetForTest();
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("shell render scope during streaming (TASK 24 perf)", () => {
  it("10k text deltas rerender only Conversation", () => {
    // Seed a realistic shell: one workspace, one engine, one session, one
    // active run with an assistant message.
    store.patch((s) => ({
      ...s,
      workspaces: [
        { id: "w1", path: "/w1", name: "W1", has_git: false, saipen: null, last_opened_at: 1 },
      ],
      currentWorkspaceId: "w1",
      engines: [
        {
          id: "fake",
          display_name: "Fake",
          version: "1",
          experimental: false,
          health: "ready",
          capabilities: {
            streaming: true,
            sessions: true,
            resume: false,
            cancel: true,
            tools: true,
            permissions: true,
            attachments: false,
            images: false,
            models: false,
            usage: false,
            reasoning: false,
            context_window: null,
            worktrees: false,
            parallel_sessions: true,
            session_revert: false,
            structured_events: true,
          },
        },
      ],
      selectedEngineId: "fake",
      models: [],
      selectedModelId: null,
      sessions: [
        {
          id: "s1",
          workspace_id: "w1",
          engine_id: "fake",
          engine_session_id: "es1",
          display_name: "S1",
          created_at: 1,
          running: false,
          resumable: true,
          usable_now: true,
        },
      ],
      activeSessionId: "s1",
    }));
    dispatch("message.started", {
      session_id: "s1",
      run_id: "r1",
      engine_id: "fake",
    });
    // One permission + one tool on the active message: ActivityPanel must not
    // lose these on text deltas (its comparator watches their refs).
    dispatch("permission.requested", {
      session_id: "s1",
      run_id: "r1",
      request_id: "req1",
      detail: "run a command?",
    });
    dispatch("tool.started", {
      session_id: "s1",
      run_id: "r1",
      tool_call_id: "tc1",
      tool: "bash",
    });

    const onError = () => {};
    const counters = {
      TitleBar: 0,
      ProjectSidebar: 0,
      SessionList: 0,
      Composer: 0,
      SaipenBar: 0,
      StatusLine: 0,
      ActivityPanel: 0,
    };
    const comparators = {
      TitleBar: titleBarEqual,
      ProjectSidebar: projectSidebarEqual,
      SessionList: sessionListEqual,
      Composer: composerEqual,
      SaipenBar: saipenBarEqual,
      StatusLine: statusLineEqual as unknown as (
        a: { state: AppState; onError: (m: string) => void },
        b: { state: AppState; onError: (m: string) => void },
      ) => boolean,
      ActivityPanel: activityPanelEqual,
    };

    let prev: AppState = store.getState();
    let transitionCount = 0;
    const unsub = store.subscribe(() => {
      const next = store.getState();
      transitionCount += 1;
      for (const [name, cmp] of Object.entries(comparators)) {
        const propsA = { state: prev, onError };
        const propsB = { state: next, onError };
        const skip = cmp(propsA, propsB);
        if (!skip) counters[name as keyof typeof counters] += 1;
      }
      prev = next;
    });

    // 10k tokens in 100 frames of 100 deltas each (real stream cadence).
    // Each delta arrives shell-coalesced: one state mutation per batch.
    let seq = 1;
    for (let frame = 0; frame < 100; frame++) {
      for (let i = 0; i < 100; i++) {
        dispatch("message.delta", { session_id: "s1", run_id: "r1", delta: "x" }, seq++);
      }
    }
    unsub();

    expect(transitionCount).toBe(10_000);
    // Only the deliberately-unmemoized Conversation may rerender per batch.
    expect(counters.TitleBar).toBe(0);
    expect(counters.ProjectSidebar).toBe(0);
    expect(counters.SessionList).toBe(0);
    expect(counters.Composer).toBe(0);
    expect(counters.SaipenBar).toBe(0);
    expect(counters.StatusLine).toBe(0);
    expect(counters.ActivityPanel).toBe(0);

    // The stream still landed: final text is complete and the tool/permission
    // facts survived (fast-path patching never dropped them). Live stream
    // tails live in activeMessage; transcript history is committed on
    // terminal events only.
    const tail = store.getState().activeMessage.s1!;
    expect(tail.text).toBe("x".repeat(10_000));
    expect(tail.tools.some((t) => t.id === "tc1" && t.status === "started")).toBe(true);
    expect(tail.permissions.some((p) => p.requestId === "req1")).toBe(true);
  });

  it("tool/permission/status changes DO rerender the activity panel", () => {
    store.patch((s) => ({
      ...s,
      workspaces: [
        { id: "w1", path: "/w1", name: "W1", has_git: false, saipen: null, last_opened_at: 1 },
      ],
      currentWorkspaceId: "w1",
      engines: [
        {
          id: "fake",
          display_name: "Fake",
          version: "1",
          experimental: false,
          health: "ready",
          capabilities: {
            streaming: true,
            sessions: true,
            resume: false,
            cancel: true,
            tools: true,
            permissions: true,
            attachments: false,
            images: false,
            models: false,
            usage: false,
            reasoning: false,
            context_window: null,
            worktrees: false,
            parallel_sessions: true,
            session_revert: false,
            structured_events: true,
          },
        },
      ],
      selectedEngineId: "fake",
      sessions: [
        {
          id: "s1",
          workspace_id: "w1",
          engine_id: "fake",
          engine_session_id: "es1",
          display_name: "S1",
          created_at: 1,
          running: false,
          resumable: true,
          usable_now: true,
        },
      ],
      activeSessionId: "s1",
    }));
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    const onError = () => {};

    const before = store.getState();
    dispatch("tool.started", {
      session_id: "s1",
      run_id: "r1",
      tool_call_id: "tc1",
      tool: "bash",
    });
    const after = store.getState();
    // A tool event is a real activity change: the panel MUST rerender.
    expect(activityPanelEqual({ state: before, onError }, { state: after, onError })).toBe(false);

    // Text-only change: no rerender.
    dispatch("message.delta", { session_id: "s1", run_id: "r1", delta: "t" });
    const afterText = store.getState();
    expect(
      activityPanelEqual({ state: after, onError }, { state: afterText, onError }),
    ).toBe(true);
  });
});
