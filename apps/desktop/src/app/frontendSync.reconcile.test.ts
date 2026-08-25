// AUDIT-W2-004: reconciliation may never overwrite events NEWER than its own
// authority reads. These tests hold the backend reads on deferred promises,
// deliver a newer authoritative event mid-flight, then resolve the OLD read —
// the stale domain result must be discarded, current ownership preserved, and
// exactly ONE bounded dirty follow-up must obtain final truth.
import { describe, expect, it, vi, beforeEach } from "vitest";
import { store } from "../state/store";
import { parseEnvelope } from "../state/events";
import type { Session } from "@saiwork2/contracts";
import { requestReconcile, resetFrontendSyncForTest } from "./frontendSync";

const cmds = vi.hoisted(() => ({
  appInfo: vi.fn(),
  listEngines: vi.fn(),
  listSessions: vi.fn(),
  activeRuns: vi.fn(),
  getSaipen: vi.fn(),
  pendingPermissions: vi.fn(),
  pendingQuestions: vi.fn(),
  queueSnapshot: vi.fn(),
}));

vi.mock("./backend", () => ({
  commands: cmds,
  subscribeToCoreEvents: () => ({ ready: Promise.resolve(), dispose: () => undefined }),
}));

vi.mock("./sessionSelection", () => ({
  activateSession: vi.fn(),
  loadSessionHistory: vi.fn(async () => undefined),
}));

function session(id: string): Session {
  return {
    id,
    workspace_id: "w1",
    engine_id: "e1",
    engine_session_id: `up-${id}`,
    display_name: id,
    created_at: 1,
    running: false,
    resumable: true,
    usable_now: true,
  };
}

function dispatch(type: string, payload: Record<string, unknown>, seq: number): void {
  store.dispatch(parseEnvelope({ seq, ts: Date.now(), type, ...payload }));
}

async function flush(times = 12): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

beforeEach(() => {
  vi.clearAllMocks();
  resetFrontendSyncForTest();
  store.resetForTest();
  store.patch((s) => ({
    ...s,
    currentWorkspaceId: "w1",
    sessions: [session("s0")],
    running: {},
  }));
  cmds.appInfo.mockResolvedValue({ lifecycle: "ready", version: "t" });
  cmds.listEngines.mockResolvedValue([]);
  cmds.activeRuns.mockResolvedValue([]);
  cmds.getSaipen.mockResolvedValue(null);
  cmds.pendingPermissions.mockResolvedValue([]);
  cmds.pendingQuestions.mockResolvedValue([]);
});

describe("reconcile event watermarks (AUDIT-W2-004)", () => {
  it("a session.created newer than listSessions is never erased by the old read", async () => {
    let resolveSessions!: (v: Session[]) => void;
    let calls = 0;
    cmds.listSessions.mockImplementation(
      () =>
        new Promise<Session[]>((resolve) => {
          calls += 1;
          if (calls === 1) resolveSessions = resolve;
          else resolve([session("s0"), session("sx")]);
        }),
    );

    const pass = requestReconcile();
    await flush(); // reads in flight
    // Newer authoritative event lands while the read is in flight.
    dispatch("session.created", { session_id: "sx", workspace_id: "w1" }, 5);
    expect(store.getState().sessions.some((s) => s.id === "sx")).toBe(true);

    // The STALE read resolves afterwards with pre-event truth…
    resolveSessions([session("s0")]);
    await flush(4);
    await pass;
    await flush(20); // allow the single bounded follow-up to settle

    // …and must not have erased it. The follow-up re-read owns final truth.
    const ids = store.getState().sessions.map((s) => s.id).sort();
    expect(ids).toEqual(["s0", "sx"]);
    expect(calls).toBe(2); // exactly ONE bounded follow-up
  });

  it("an activeRuns read older than message.started cannot fabricate idle", async () => {
    let resolveRuns!: (v: [string, string][]) => void;
    let calls = 0;
    cmds.listSessions.mockResolvedValue([session("s0")]);
    cmds.activeRuns.mockImplementation(
      () =>
        new Promise<[string, string][]>((resolve) => {
          calls += 1;
          if (calls === 1) resolveRuns = resolve;
          else resolve([["s0", "r9"]]);
        }),
    );

    const pass = requestReconcile();
    await flush();
    // Authoritative run start arrives mid-flight.
    dispatch("message.started", { session_id: "s0", run_id: "r9" }, 7);
    expect(store.getState().running.s0).toBe("r9");

    // Old empty activeRuns resolves — it must NOT clear the ownership.
    resolveRuns([]);
    await pass;
    await flush(20);

    expect(store.getState().running.s0).toBe("r9");
    expect(store.getState().runningStale).toBe(false); // follow-up restored exactness
    expect(calls).toBe(2);
  });

  it("quiet domains commit normally — no follow-up churn", async () => {
    cmds.listSessions.mockResolvedValue([session("s0")]);
    cmds.activeRuns.mockResolvedValue([]);
    await requestReconcile();
    await flush(20);
    expect(cmds.listSessions).toHaveBeenCalledTimes(1);
    expect(store.getState().runningStale).toBe(false);
  });
});
