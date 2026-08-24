import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  store,
  applyEvent,
  addLocalUserMessage,
  hydrateSessionHistory,
  markUserMessageUncertain,
  setFavorites,
  setFavoritesOnly,
  toggleFavoriteModel,
  favoritesGen,
  nextFavoritesGen,
  MAX_FAVORITES_UI,
  type AppState,
} from "./store";
import type { Envelope } from "@saiwork2/contracts";
import { parseEnvelope } from "./events";

function env(type: string, payload: Record<string, unknown>, seq = 1): Envelope {
  return { seq, ts: Date.now(), type, ...payload } as Envelope;
}

function dispatch(type: string, payload: Record<string, unknown>, seq = 1): void {
  store.dispatch(parseEnvelope(env(type, payload, seq)));
}

/** The transcript is split: the LIVE stream tail lives in `activeMessage`
 * while a run is in flight, and is committed to `messages` only on terminal.
 * A test helper that returns the current message for a run regardless of
 * which slice it is in. */
function msgFor(sessionId: string, runId: string) {
  const s = store.getState();
  const candidates = [
    ...(s.activeMessage[sessionId] ? [s.activeMessage[sessionId]!] : []),
    ...(s.messages[sessionId] ?? []),
  ] as Array<{ runId?: string | null; permissions?: Array<{ requestId: string; allowed: boolean | null }>; questions?: Array<{ requestId: string; resolved: boolean | null }>; tools?: Array<{ id: string; status?: string }> }>;
  return candidates.find((m) => m.runId === runId);
}

function msgDelta(sessionId: string, runId: string, delta: string, seq: number): Envelope {
  return env("message.delta", { session_id: sessionId, run_id: runId, delta }, seq);
}

/**
 * TEST-ROT FIX (HUNT-003): the store's scope guards are correct and intended
 * (CORE-008 session-scope + T-045 workspace-scope invariants in store.ts), but
 * the original tests dispatched scope-gated events (`message.started`,
 * `session.created`, `saipen.changed`) with NO session in `sessions` and
 * `currentWorkspaceId === null`. Every such dispatch hit the silent early-return
 * guard and the assertions then read an empty/ignored state -- 14 false
 * "failures". Seed a real `w1` session and set the current workspace before the
 * scope-gated events so the reducers actually run. Tests that intentionally
 * exercise the scope DROP (e.g. session.created for another workspace) must set
 * their own state; this helper only establishes a valid in-scope baseline.
 */
function seedSession(sessionId = "s1", workspaceId = "w1"): void {
  store.patch((s) => ({
    ...s,
    currentWorkspaceId: workspaceId,
    sessions: s.sessions.some((x) => x.id === sessionId)
      ? s.sessions
      : [
          ...s.sessions,
          {
            id: sessionId,
            workspace_id: workspaceId,
            engine_id: "fake",
            engine_session_id: "up-" + sessionId,
            display_name: sessionId,
            created_at: 1,
            running: false,
            resumable: true,
            usable_now: true,
          },
        ],
  }));
}

beforeEach(() => {
  store.resetForTest();
  vi.useFakeTimers();
  // HUNT-003: establish a valid in-scope baseline (a real `w1` session +
  // currentWorkspaceId) so scope-gated events are not silently dropped by the
  // CORE-008/T-045 guards. Previously the store started empty and every
  // message.started / session.created / saipen.changed hit the early-return,
  // producing 14 false failures.
  seedSession();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("stream batching (ONE window, §22–§23 + TASK 24 perf)", () => {
  it("applies each shell-coalesced delta immediately — one mutation per bridge batch", () => {
    // The shell transport (`saiwork_events::coalescing`) already merges N
    // raw tokens into one delta per (session, run) per frame; the Store must
    // NOT add a second 16 ms timer. Each received delta = one state mutation
    // = one listener notification (never per original token).
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    let notifications = 0;
    const unsub = store.subscribe(() => notifications++);
    for (let i = 0; i < 100; i++) {
      store.dispatch(parseEnvelope(msgDelta("s1", "r1", "x", i + 1)));
    }
    const after = store.getState();
    expect(after.activeMessage.s1?.text).toBe("x".repeat(100));
    expect(notifications).toBe(100); // one per emitted bridge batch
    unsub();
  });

  it("terminal after final delta keeps the final text (§23)", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    store.dispatch(parseEnvelope(msgDelta("s1", "r1", "tail ", 1)));
    store.dispatch(parseEnvelope(msgDelta("s1", "r1", "chunk", 2)));
    dispatch("message.completed", { session_id: "s1", run_id: "r1" });
    const state = store.getState();
    const msg = state.messages.s1?.[0];
    expect(msg?.text).toBe("tail chunk");
    expect(msg?.status).toBe("complete");
  });

  it("fallback patching updates a non-tail run correctly (TASK 24 perf)", () => {
    // Two completed runs in one session, then a THIRD active run: the active
    // run is the tail, but a late tool fact for run r1 must patch the OLD
    // (non-tail) message — the O(1) fast path must never be taken when the
    // invariant "active run is last" is false.
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("message.completed", { session_id: "s1", run_id: "r1" });
    dispatch("message.started", { session_id: "s1", run_id: "r2", engine_id: "fake" });
    dispatch("message.completed", { session_id: "s1", run_id: "r2" });
    dispatch("message.started", { session_id: "s1", run_id: "r3", engine_id: "fake" });

    // Late tool fact for the OLD run r1 (its message is NOT the tail).
    dispatch("tool.started", {
      session_id: "s1",
      run_id: "r1",
      tool_call_id: "tc-old",
      tool: "bash",
    });
    const s = store.getState();
    const msgs = [...(s.messages.s1 ?? []), ...(s.activeMessage.s1 ? [s.activeMessage.s1] : [])];
    expect(msgs.length).toBeGreaterThanOrEqual(3);
    const first = msgs.find((m) => m.runId === "r1")!;
    expect(first.runId).toBe("r1");
    expect(first.tools.some((t) => t.id === "tc-old" && t.status === "started")).toBe(true);
    // The active tail (r3) is untouched by the r1 patch.
    const tail = msgs.find((m) => m.runId === "r3")!;
    expect(tail.runId).toBe("r3");
    expect(tail.tools).toHaveLength(0);
  });
});

describe("log filtering (no global rerender per token — §241)", () => {
  it("message.delta and tool.output never grow the log", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    for (let i = 0; i < 50; i++) {
      store.dispatch(parseEnvelope(msgDelta("s1", "r1", "y", i)));
    }
    dispatch("tool.started", { session_id: "s1", tool: "bash" });
    dispatch("tool.output", { session_id: "s1", tool: "bash", output: "o" });
    const log = store.getState().log;
    expect(log.filter((l) => l.type === "message.delta")).toHaveLength(0);
    expect(log.filter((l) => l.type === "tool.output")).toHaveLength(0);
    // Meaningful events still log.
    dispatch("message.completed", { session_id: "s1", run_id: "r1" });
    expect(store.getState().log.at(-1)?.type).toBe("message.completed");
  });
});

describe("permission lifecycle (§36–§38)", () => {
  it("pending permission is stored and resolution marks it", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("permission.requested", { session_id: "s1", run_id: "r1", request_id: "p1", detail: "run tool" });
    const pending = msgFor("s1", "r1")?.permissions;
    expect(pending?.[0]).toMatchObject({ requestId: "p1", allowed: null });
    dispatch("permission.resolved", { session_id: "s1", run_id: "r1", request_id: "p1", allowed: true });
    expect(msgFor("s1", "r1")?.permissions?.[0]).toMatchObject({
      requestId: "p1",
      allowed: true,
    });
  });

  // AUDIT-CORE-002: questions are their own projection — asked adds a card,
  // resolved removes it (the card's job is done; no stale open question).
  it("question.asked stores a card and question.resolved removes it", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("question.asked", {
      session_id: "s1",
      run_id: "r1",
      request_id: "q1",
      detail: '{"questions":[{"question":"Proceed?","options":[{"label":"Yes"}]}]}',
    });
    const asked = msgFor("s1", "r1")?.questions;
    expect(asked).toHaveLength(1);
    expect(asked?.[0]).toMatchObject({ requestId: "q1", resolved: null });
    dispatch("question.resolved", { session_id: "s1", run_id: "r1", request_id: "q1" });
    expect(msgFor("s1", "r1")?.questions).toHaveLength(0);
  });

  it("delayed run-A permission never routes to run B (TASK 24 §9)", () => {
    // Run A requests permission, then run B starts (session advanced) before
    // the UI consumes the event. The delayed A request/resolution must patch
    // only A's message — never B's.
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("permission.requested", {
      session_id: "s1",
      run_id: "r1",
      request_id: "pA",
      detail: "run A tool",
    });
    // Newer run r2 is now the active run.
    dispatch("message.started", { session_id: "s1", run_id: "r2", engine_id: "fake" });
    // Delayed resolution for A arrives after B started.
    dispatch("permission.resolved", { session_id: "s1", run_id: "r1", request_id: "pA", allowed: true });
    const st = store.getState();
    // r1 was superseded: it lives in the transcript history, r2 is the LIVE
    // stream tail (activeMessage) — the store's fast-path split, not messages.
    const a = st.messages.s1!.find((m) => m.runId === "r1")!;
    const b = st.activeMessage.s1!;
    expect(b.runId).toBe("r2");
    expect(a.permissions).toHaveLength(1);
    expect(a.permissions[0]).toMatchObject({ requestId: "pA", allowed: true });
    expect(b.permissions).toHaveLength(0);
  });

  it("same request id in a different run cannot cross-route (TASK 24 §9)", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("permission.requested", {
      session_id: "s1",
      run_id: "r1",
      request_id: "same",
      detail: "first",
    });
    dispatch("message.completed", { session_id: "s1", run_id: "r1" });
    dispatch("message.started", { session_id: "s1", run_id: "r2", engine_id: "fake" });
    // Run B reuses the same request id string; the resolution routes by run.
    dispatch("permission.requested", {
      session_id: "s1",
      run_id: "r2",
      request_id: "same",
      detail: "second",
    });
    dispatch("permission.resolved", { session_id: "s1", run_id: "r2", request_id: "same", allowed: false });
    const a = msgFor("s1", "r1")!;
    const b = msgFor("s1", "r2")!;
    expect(a.permissions?.[0]).toMatchObject({ requestId: "same", allowed: null });
    expect(b.permissions?.[0]).toMatchObject({ requestId: "same", allowed: false });
  });
});

describe("outcome_unknown projection (TASK 24 §9)", () => {
  it("marks the run ambiguous without fabricating completion or clearing the reservation", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("message.delta", { session_id: "s1", run_id: "r1", delta: "partial " });
    dispatch("message.outcome_unknown", {
      session_id: "s1",
      run_id: "r1",
      error: "harness runtime lost",
    });
    const st = store.getState();
    const m = st.activeMessage.s1 ?? st.messages.s1?.find((x) => x.runId === "r1");
    expect(m?.status).toBe("outcome_unknown");
    expect(m?.text).toBe("partial ");
    expect(m?.error).toContain("harness runtime lost");
    // The reservation is preserved: the run is still "running" so direct UI
    // actions stay gated and Cancel stays available.
    expect(st.running.s1).toBe("r1");
  });

  it("a later matching authoritative terminal resolves the same row once", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("message.outcome_unknown", { session_id: "s1", run_id: "r1", error: "lost" });
    dispatch("message.failed", { session_id: "s1", run_id: "r1", error: "definitive" });
    const st = store.getState();
    const m = st.messages.s1?.find((x) => x.runId === "r1")!;
    expect(m.status).toBe("failed");
    expect(st.running.s1).toBeNull();
  });
});

describe("stale terminals cannot clear a newer run (TASK 24 §9)", () => {
  it("delayed run-1 terminal leaves run-2 running", () => {
    dispatch("message.started", { session_id: "s1", run_id: "r1", engine_id: "fake" });
    dispatch("message.completed", { session_id: "s1", run_id: "r1" });
    // Run r2 is active; a DELAYED stale terminal for r1 arrives afterwards.
    dispatch("message.started", { session_id: "s1", run_id: "r2", engine_id: "fake" });
    dispatch("message.failed", { session_id: "s1", run_id: "r1", error: "late stale" });
    let st = store.getState();
    expect(st.running.s1).toBe("r2");
    // r1's message is still a historical fact.
    expect(st.messages.s1?.find((x) => x.runId === "r1")?.status).toBe("failed");
    // The real terminal for r2 clears it.
    dispatch("message.completed", { session_id: "s1", run_id: "r2" });
    st = store.getState();
    expect(st.running.s1).toBeNull();
    // Duplicate r1 terminals are idempotent.
    dispatch("message.cancelled", { session_id: "s1", run_id: "r1" });
    expect(store.getState().running.s1).toBeNull();
  });
});

describe("runtime.warning never erases lastError (TASK 24 §9)", () => {
  it("keeps an actionable error across unrelated warnings", () => {
    dispatch("runtime.error", { message: "storage failure" });
    expect(store.getState().lastError).toBe("storage failure");
    dispatch("runtime.warning", { message: "HARNESS_STREAM_OVERFLOW" });
    expect(store.getState().lastError).toBe("storage failure");
    dispatch("runtime.error", { message: "newer failure" });
    expect(store.getState().lastError).toBe("newer failure");
  });
});

describe("session.created carries authoritative resumable/usable_now (TASK 24 §9)", () => {
  it("never fabricates resumable=true — a fresh connection-owned session is usable now", () => {
    // Harness/Generic (resume=false) first-prompt usability must not depend
    // on event ordering: the event itself carries usable_now=true even though
    // resumable=false, and the reducer must not invent resumable=true.
    dispatch("session.created", {
      session_id: "s1",
      engine_id: "deepseek-harness",
      workspace_id: "w1",
      engine_session_id: "up-1",
      display_name: "H1",
      created_at: 1,
      resumable: false,
      usable_now: true,
    });
    const s = store.getState().sessions.find((x) => x.id === "s1");
    expect(s).toBeDefined();
    expect(s!.resumable).toBe(false);
    expect(s!.usable_now).toBe(true);
  });

  it("event-before-response keeps the same usable-now truth", () => {
    // The command-returned DTO carries the same normalized fields; whichever
    // lands first wins, the other replaces it — no fabricated true.
    dispatch("session.created", {
      session_id: "s2",
      engine_id: "opencode",
      workspace_id: "w1",
      engine_session_id: "up-2",
      display_name: "O2",
      created_at: 2,
      resumable: true,
      usable_now: false, // engine not READY yet in this projection
    });
    const s = store.getState().sessions.find((x) => x.id === "s2");
    expect(s!.resumable).toBe(true);
    expect(s!.usable_now).toBe(false);
  });

  it("a delayed create from a previous engine does not hijack the active thread", () => {
    store.patch((state) => ({ ...state, activeSessionId: null, selectedEngineId: "other" }));
    dispatch("session.created", {
      session_id: "late",
      engine_id: "opencode",
      workspace_id: "w1",
      engine_session_id: "up-late",
      display_name: "Late",
      created_at: 3,
      resumable: true,
      usable_now: true,
    });
    expect(store.getState().sessions.some((session) => session.id === "late")).toBe(true);
    expect(store.getState().activeSessionId).toBeNull();
  });
});

describe("uncertain user turns are bound to their RunId (TASK 24 §9)", () => {
  it("an unrelated new run never clears an older ambiguous prompt", () => {
    dispatch("session.created", {
      session_id: "s1",
      engine_id: "fake",
      workspace_id: "w1",
      engine_session_id: "up",
      display_name: "S",
      created_at: 1,
      resumable: true,
      usable_now: true,
    });
    // Unknown r1, then a NEW run r2 starts: r2 must not clear r1.
    addLocalUserMessage("s1", "first prompt");
    const u1 = store.getState().messages.s1![0]!.id;
    markUserMessageUncertain("s1", u1, "r1");
    dispatch("message.started", { session_id: "s1", run_id: "r2" });
    const after = store.getState().messages.s1 ?? [];
    expect(after.find((m) => m.id === u1)!.uncertain).toBe(true);
    // Matching evidence clears only r1.
    dispatch("message.started", { session_id: "s1", run_id: "r1" });
    expect(store.getState().messages.s1!.find((m) => m.id === u1)!.uncertain).toBe(false);
  });

  it("event-before-command-response still yields one correctly correlated pair", () => {
    // started(r1) arrives BEFORE the send response marks the turn uncertain;
    // the later mark with run_id binds it, and a definitive terminal for r1
    // (not r2) resolves it.
    dispatch("session.created", {
      session_id: "s2",
      engine_id: "fake",
      workspace_id: "w1",
      engine_session_id: "up",
      display_name: "S",
      created_at: 1,
      resumable: true,
      usable_now: true,
    });
    addLocalUserMessage("s2", "second prompt");
    const u2 = store.getState().messages.s2![0]!.id;
    markUserMessageUncertain("s2", u2, "r1");
    // A definitive terminal for r1 proves the exact prompt was delivered.
    dispatch("message.completed", { session_id: "s2", run_id: "r1" });
    const m = store.getState().messages.s2!.find((x) => x.id === u2);
    expect(m).toBeDefined();
    expect(m!.uncertain).toBe(false);
    expect(m!.uncertainRunId).toBeUndefined();
    // A terminal for an unrelated r2 would NOT clear it.
    addLocalUserMessage("s2", "third prompt");
    const u3 = store.getState().messages.s2![1]!.id;
    markUserMessageUncertain("s2", u3, "r9");
    dispatch("message.completed", { session_id: "s2", run_id: "r2" });
    expect(store.getState().messages.s2!.find((x) => x.id === u3)!.uncertain).toBe(true);
  });
});

describe("revision guards (initial-query race protection — §97, §173)", () => {
  it("queue events bump the queue revision only", () => {
    const before = store.getState();
    dispatch("queue.changed", { item_id: "q1", state: "queued" });
    const after = store.getState();
    expect(after.queue.revision).toBe(before.queue.revision + 1);
    expect(after.messages).toBe(before.messages); // untouched slice
  });

  it("saipen events bump the saipen revision only", () => {
    const before = store.getState();
    dispatch("saipen.changed", { workspace_id: "w1" });
    const after = store.getState();
    expect(after.saipenRevision).toBe(before.saipenRevision + 1);
  });
});

describe("pure reducer contract", () => {
  it("applyEvent never mutates the input state", () => {
    const state: AppState = JSON.parse(JSON.stringify(store.getState()));
    const next = applyEvent(state, parseEnvelope(env("engine.ready", { engine_id: "fake" })));
    expect(next).not.toBe(state);
    expect(state.engines).toHaveLength(0);
  });
});

describe("authoritative history reconcile (TASK 24 §9)", () => {
  it("delayed history merges with a live turn exactly once (dedupe by id, keep live)", () => {
    store.patch((s) => ({ ...s, activeSessionId: "s1" }));
    // A live turn starts while the history read is still in flight.
    store.patch((s) => ({
      ...s,
      messages: {
        ...s.messages,
        s1: [
          {
            id: "live-user-1",
            role: "user",
            runId: "r1",
            status: "complete",
            text: "live question",
            tools: [],
            permissions: [], questions: [],
            ts: Date.now(),
          },
        ],
      },
    }));
    // History arrives late: one history user message (distinct id) plus the
    // same user id would NOT be duplicated — here the live turn is separate.
    hydrateSessionHistory("s1", [
      { id: "hist-user-1", role: "user", text: "old question", tool_call_id: null, tool: null, order: 0 },
      { id: "hist-asst-1", role: "assistant", text: "old answer", tool_call_id: null, tool: null, order: 1 },
    ]);
    const messages = store.getState().messages["s1"]!;
    expect(messages.map((m) => m.id)).toEqual([
      "hist-user-1",
      "hist-asst-1",
      "live-user-1",
    ]);
    // The live turn is untouched (exact object identity).
    expect(messages[2]!.text).toBe("live question");
    expect(store.getState().historyStatus["s1"]).toBe("available");
  });

  it("history is never applied twice when the same id arrives again", () => {
    store.patch((s) => ({ ...s, activeSessionId: "s1" }));
    const history = [{ id: "u1", role: "user", text: "once", tool_call_id: null, tool: null, order: 0 }];
    hydrateSessionHistory("s1", history);
    hydrateSessionHistory("s1", history);
    expect(store.getState().messages["s1"]!).toHaveLength(1);
  });

  // AUDIT-CORE-004: a tool entry following its parent assistant attaches to
  // that REAL assistant — no synthetic blank assistant is fabricated.
  it("AUDIT-CORE-004: tool history attaches to its preceding real assistant", () => {
    store.patch((s) => ({ ...s, activeSessionId: "s1" }));
    hydrateSessionHistory("s1", [
      { id: "u0", role: "user", text: "run it", tool_call_id: null, tool: null, order: 0 },
      { id: "a1", role: "assistant", text: "preloaded assistant answer", tool_call_id: null, tool: null, order: 2 },
      { id: "call_1", role: "tool", text: "ok", tool_call_id: "call_1", tool: "bash", order: 3 },
    ]);
    const msgs = store.getState().messages["s1"]!;
    expect(msgs.map((m) => m.id)).toEqual(["u0", "a1"]);
    const asst = msgs.find((m) => m.id === "a1")!;
    expect(asst.text).toBe("preloaded assistant answer");
    expect(asst.tools.map((t) => t.id)).toEqual(["call_1"]);
    expect(asst.tools[0]?.tool).toBe("bash");
  });

  // AUDIT-CORE-005 (supersedes the old text-dedup behavior): exact text
  // equality is NOT message identity. The scenario below used to DELETE the
  // optimistic live turn; now both logical turns remain visible.
  it("AUDIT-CORE-005: text equality is never message identity — a distinct repeated prompt survives hydration", () => {
    store.patch((s) => ({ ...s, activeSessionId: "s1" }));
    // An older AUTHORITATIVE turn says "continue". The user then sends a NEW
    // optimistic "continue" (synthetic id) whose upstream message is not in
    // the delayed snapshot yet. The old text-only suppression deleted the new
    // send; now both logical turns remain visible.
    store.patch((s) => ({
      ...s,
      messages: {
        ...s.messages,
        s1: [
          {
            id: "user-1700000000000-a1b2c3",
            role: "user",
            runId: "r1",
            status: "complete",
            text: "continue",
            tools: [],
            permissions: [], questions: [],
            ts: Date.now(),
          },
        ],
      },
    }));
    hydrateSessionHistory("s1", [
      { id: "upstream-u1", role: "user", text: "continue", tool_call_id: null, tool: null, order: 0 },
      { id: "upstream-a1", role: "assistant", text: "step done", tool_call_id: null, tool: null, order: 2 },
    ]);
    const msgs = store.getState().messages["s1"]!;
    const userTurns = msgs.filter((m) => m.role === "user" && m.text === "continue");
    expect(userTurns).toHaveLength(2);
    expect(msgs.map((m) => m.id)).toEqual([
      "upstream-u1",
      "upstream-a1",
      "user-1700000000000-a1b2c3",
    ]);
  });

  // AUDIT-CORE-003: hydration during a live stream must NOT copy the tail
  // into the completed transcript — activeMessage is its only owner and
  // commitTerminal performs the one active→completed transition.
  it("AUDIT-CORE-003: hydration never appends the live tail to completed messages", () => {
    store.patch((s) => ({ ...s, activeSessionId: "s1" }));
    store.patch((s) => ({
      ...s,
      activeMessage: {
        ...s.activeMessage,
        s1: {
          id: "r1-assistant",
          role: "assistant",
          runId: "r1",
          status: "streaming",
          text: "streaming partial answer",
          tools: [],
          permissions: [], questions: [],
          ts: Date.now(),
        },
      },
    }));
    hydrateSessionHistory("s1", [
      { id: "hist-user-1", role: "user", text: "old question", tool_call_id: null, tool: null, order: 0 },
    ]);
    const st = store.getState();
    // The completed slice holds ONLY history; the tail lives solely in
    // activeMessage (Conversation renders both slices exactly once).
    expect(st.messages.s1!.map((m) => m.id)).toEqual(["hist-user-1"]);
    expect(st.activeMessage.s1?.id).toBe("r1-assistant");
    expect(st.messages.s1!.some((m) => m.id === "r1-assistant")).toBe(false);

    // The terminal commits the tail exactly once.
    dispatch("message.completed", { session_id: "s1", run_id: "r1" });
    const after = store.getState();
    expect(after.activeMessage.s1).toBeNull();
    const committed = after.messages.s1!.filter((m) => m.id === "r1-assistant");
    expect(committed).toHaveLength(1);
    expect(committed[0]!.status).toBe("complete");
  });
});

describe("model favorites (durable UI preference)", () => {
  it("toggle adds and removes a model id", () => {
    expect(store.getState().favoriteModelIds).toEqual([]);
    const next = toggleFavoriteModel("anthropic/claude-3.5");
    expect(next).toEqual(["anthropic/claude-3.5"]);
    expect(store.getState().favoriteModelIds).toEqual(["anthropic/claude-3.5"]);
    const removed = toggleFavoriteModel("anthropic/claude-3.5");
    expect(removed).toEqual([]);
  });

  it("toggle never grows past the UI cap (mirrors the app authority bound)", () => {
    for (let i = 0; i < 60; i++) toggleFavoriteModel("m" + i);
    expect(store.getState().favoriteModelIds).toHaveLength(MAX_FAVORITES_UI);
    // Adding one more at the cap is a no-op.
    const next = toggleFavoriteModel("m-extra");
    expect(next).toHaveLength(MAX_FAVORITES_UI);
    expect(next).not.toContain("m-extra");
  });

  it("setFavorites replaces the set (bootstrap load)", () => {
    toggleFavoriteModel("a");
    setFavorites(["x", "y"]);
    expect(store.getState().favoriteModelIds).toEqual(["x", "y"]);
  });

  it("W2-005: favorite-mutation generation is shared across hydration and writes", () => {
    const start = favoritesGen();
    // A late bootstrap hydration bumps the SHARED counter (not a local one).
    setFavorites(["a", "b"]);
    expect(favoritesGen()).toBe(start + 1);
    // A user toggle also bumps the SAME counter, so rollback guards agree on
    // one source of truth (lifted from TitleBar into the store module).
    const myGen = nextFavoritesGen();
    expect(myGen).toBe(start + 2);
    expect(store.getState().favoriteModelIds).toEqual(["a", "b"]);
  });

  it("favoritesOnly toggle is ephemeral UI state", () => {
    expect(store.getState().favoritesOnly).toBe(false);
    setFavoritesOnly(true);
    expect(store.getState().favoritesOnly).toBe(true);
    setFavoritesOnly(false);
    expect(store.getState().favoritesOnly).toBe(false);
  });
});
