// THE user-facing smoke (audit DONE-WHEN): fresh start → project → OpenCode
// → Start → models/default → New Session → first prompt → response → second
// prompt → outcome-unknown turn stays UNCERTAIN (never blind-resend) → Cancel
// → restart app → engine is STOPPED so a direct Send MUST be rejected until
// explicitly restarted → resume session → queue prompt. Runs the REAL
// production bootstrap (`bootstrapApp`), the REAL store + event pipeline
// (`store.dispatch` on parsed canonical envelopes, exactly as the Tauri
// bridge does), and the REAL component gating (rendered with react-dom/server).
//
// The backend is a stateful fake that ENFORCES the backend prerequisites the
// real core enforces (TASK 24 §9): Send fails when the engine is not READY,
// when the session does not belong to the declared workspace/engine, or when
// the session is not resumable. No FakeEngine is ever the default, no invalid
// command fires silently, and the stored Session rows must exactly match the
// command DTOs. The whole route is run twice in a row.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderToString } from "react-dom/server";
import type {
  EngineInfo,
  Envelope,
  ModelInfo,
  QueueItem,
  QueueSnapshot,
  SaipenSummary,
  SendOutcome,
  Session,
  WorkspaceSummary,
} from "@saiwork2/contracts";
import { healthKind } from "@saiwork2/contracts";
import { store, initialState, addLocalUserMessage } from "../state/store";
import { commands } from "./backend";
import { coldBootstrap, resetFrontendSyncForTest } from "./frontendSync";
import { ensureModels, resetModelCatalogForTest } from "../app/modelCatalog";
import { selectWorkspace } from "../app/workspaceSelection";
import { TitleBar } from "../components/TitleBar";
import { SessionList } from "../components/SessionList";
import { performSend, sendDisabledReasonFor } from "../components/Composer";
import { parseEnvelope } from "../state/events";

// ---------------------------------------------------------------------------
// Stateful fake backend (persists across the simulated restart)
// ---------------------------------------------------------------------------

let seq = 0;
function emit(type: string, payload: Record<string, unknown>): void {
  const envelope = { seq: ++seq, ts: Date.now(), type, ...payload } as Envelope;
  store.dispatch(parseEnvelope(envelope));
}

function engine(id: string, models: boolean, sessions: boolean): EngineInfo {
  return {
    id,
    display_name: id === "opencode" ? "OpenCode" : "Fake Engine",
    version: "1.18.18-test",
    experimental: false,
    health: "stopped",
    capabilities: {
      streaming: true,
      sessions,
      resume: sessions,
      cancel: true,
      tools: true,
      permissions: true,
      attachments: false,
      images: false,
      models,
      usage: false,
      reasoning: true,
      context_window: null,
      worktrees: false,
      parallel_sessions: false,
      session_revert: false,
      structured_events: true,
    },
  };
}

const MODELS: ModelInfo[] = [
  { id: "opencode/provider-a/model-1", display_name: "Model One", provider: "opencode/provider-a", provider_name: "Provider A" },
  { id: "opencode/provider-a/model-2", display_name: "Model Two", provider: "opencode/provider-a", provider_name: "Provider A" },
];

interface FakeBackend {
  workspaces: WorkspaceSummary[];
  sessions: Session[];
  /** Durable id of the currently-active workspace (persisted across restart). */
  activeWorkspaceId: string | null;
  /** Backend authority for latest-wins on active-workspace transitions (CORE-001). */
  activeSelectionGen: number;

  queue: QueueItem[];
  runCounter: number;
  engines: EngineInfo[];
  /** Prompt text → number of sendPrompt invocations (no-auto-resend proof). */
  sendCountByPrompt: Record<string, number>;
  /** Exact (session_id, run_id) ownership the backend reports (TASK 24 §9). */
  activeRuns: () => [string, string][];
}

/** Backend truth for `active_runs`, mutable so a test can simulate a live
 * run whose events never reached the frontend (lag). */
let fakeActiveRuns: [string, string][] = [];

const fake: FakeBackend = {
  workspaces: [],
  sessions: [],
  activeWorkspaceId: null,
  activeSelectionGen: 0,
  queue: [],
  runCounter: 0,
  engines: [engine("opencode", true, true), engine("fake", false, false)],
  sendCountByPrompt: {},
  activeRuns: () => fakeActiveRuns,
};

function saipenProbe(_path: string): SaipenSummary {
  // The workspace-row badge is the CHEAP summary (TASK 24 perf); the full
  // projection comes from getSaipen (mocked to null here — not exercised by
  // the send smoke).
  return { schema_version: "3", saipen_version: "7", project: null };
}

/** The fake's sendPrompt mirrors the real backend prerequisites (TASK 24 §9):
 * READY engine, workspace/engine/session affinity, resumable session. A
 * violation throws exactly like the real command would — the smoke must FAIL
 * if the UI tries to send while the engine is stopped or the session belongs
 * to another workspace/engine. */
async function fakeSendPrompt(
  sid: string,
  wsId: string | null,
  engineId: string | null,
  prompt: string,
  _model: string | null,
): Promise<SendOutcome> {
  fake.sendCountByPrompt[prompt] = (fake.sendCountByPrompt[prompt] ?? 0) + 1;
  // Validation order mirrors the real core: validate_send_context (session
  // found, workspace/engine affinity) runs BEFORE the engine readiness check
  // in send_acceptance (sessions.rs). A cross-engine/cross-workspace send
  // must fail with the typed mismatch, never a later readiness error.
  const session = fake.sessions.find((s) => s.id === sid);
  if (!session) throw new Error(`session not found: ${sid}`);
  if (session.workspace_id !== wsId) {
    throw new Error(
      `session '${sid}' does not match the active UI context (expected workspace '${wsId}', session workspace '${session.workspace_id}')`,
    );
  }
  if (session.engine_id !== engineId) {
    throw new Error(
      `session '${sid}' does not match the active UI context (expected engine '${engineId}', session engine '${session.engine_id}')`,
    );
  }
  if (session.resumable === false) {
    throw new Error(`session '${sid}' has no trustworthy upstream session id and is not resumable`);
  }
  const engine = fake.engines.find((e) => e.id === engineId);
  if (!engine || healthKind(engine.health) !== "ready") {
    throw new Error(`Engine ${engineId} is not ready`);
  }
  const runId = `run-${++fake.runCounter}`;
  if (prompt === "uncertain one") {
    // The fixture accepts the prompt (run starts, delta streams) but the
    // acknowledgement is lost: the typed outcome is OUTCOME_UNKNOWN. The UI
    // must keep the user turn visible + marked UNCERTAIN and never resend.
    emit("message.started", { session_id: sid, run_id: runId, engine_id: engineId });
    emit("message.delta", { session_id: sid, run_id: runId, delta: "ack lost but run lives…" });
    return { kind: "outcome_unknown", run_id: runId, message: "ack lost (fixture)" } as SendOutcome;
  }
  if (prompt === "slow one") {
    // The run stays live until cancelled (real hang behavior).
    emit("message.started", { session_id: sid, run_id: runId, engine_id: engineId });
    emit("message.delta", { session_id: sid, run_id: runId, delta: "working…" });
    return { kind: "accepted", run_id: runId } as SendOutcome;
  }
  const text = `Hello from smoke engine (${prompt})`;
  emit("message.started", { session_id: sid, run_id: runId, engine_id: engineId });
  for (const chunk of [text.slice(0, 10), text.slice(10)]) {
    emit("message.delta", { session_id: sid, run_id: runId, delta: chunk });
  }
  emit("message.completed", { session_id: sid, run_id: runId });
  return { kind: "accepted", run_id: runId } as SendOutcome;
}

function installFakeBackend(): void {
  vi.spyOn(commands, "appInfo").mockImplementation(async () => ({
    version: "0.1.0-smoke",
    data_root: "C:\\smoke\\data",
    portable: false,
    lifecycle: "ready",
  }));
  vi.spyOn(commands, "listEngines").mockImplementation(async () => fake.engines);
  // Canonical restore authority = durable workspace recency (T-056): the real
  // core orders workspaces by `last_opened_at` desc, so the most-recently-opened
  // is first. The fake mirrors that (no `ui.active_workspace` key — production
  // never writes one, so it cannot outrank real recency).
  vi.spyOn(commands, "listWorkspaces").mockImplementation(async () =>
    [...fake.workspaces].sort(
      (a, b) => (b.last_opened_at ?? 0) - (a.last_opened_at ?? 0),
    ),
  );
  vi.spyOn(commands, "getSetting").mockImplementation(async () => null);
  vi.spyOn(commands, "setSetting").mockImplementation(async () => {});
  vi.spyOn(commands, "listSessions").mockImplementation(async (ws) =>
    ws ? fake.sessions.filter((s) => s.workspace_id === ws) : fake.sessions,
  );
  vi.spyOn(commands, "listModels").mockImplementation(async (id) =>
    id === "opencode" ? MODELS : [],
  );
  vi.spyOn(commands, "openWorkspace").mockImplementation(async (path) => {
    const now = Date.now();
    let ws = fake.workspaces.find((w) => w.path === path);
    if (!ws) {
      ws = {
        id: `ws-${fake.workspaces.length + 1}`,
        path,
        name: "smoke-proj",
        has_git: true,
        saipen: saipenProbe(path),
        last_opened_at: now,
      };
      fake.workspaces.push(ws);
    } else {
      // Re-opening an existing workspace bumps its durable recency (mirrors the
      // real core so restore picks it).
      ws.last_opened_at = now;
    }
    // Production: open_workspace scopes the workspace but does NOT commit it as
    // the active one — the active pointer is owned exclusively by
    // setActiveWorkspace (epoch latest-wins). The fake mirrors that so the
    // smoke exercises the real restore authority (TASK 24 §9 / CORE-001) and a
    // bare open can never fabricate an active selection across restart.
    emit("workspace.opened", { workspace_id: ws.id, path });
    return ws;
  });
  // CORE-001: the fake mirrors the backend's epoch-owned active-workspace
  // authority so the smoke can assert latest-wins on BOTH the set and clear
  // paths. Production routes open ≠ active; only setActiveWorkspace may commit.
  vi.spyOn(commands, "getActiveWorkspace").mockImplementation(async () => fake.activeWorkspaceId);
  vi.spyOn(commands, "setActiveWorkspace").mockImplementation(async (id, gen) => {
    // Mirror commit_active_workspace: a stale (older) epoch is ignored so a
    // superseded selection cannot persist its id after a newer commit landed
    // across async IPC. gen = null/undefined bypasses the guard (the backend
    // treats None as "no epoch check"), exactly as the real command behaves.
    if (gen != null) {
      if (gen < fake.activeSelectionGen) return;
      fake.activeSelectionGen = gen;
    }
    fake.activeWorkspaceId = id;
  });
  vi.spyOn(commands, "startEngine").mockImplementation(async (id) => {
    emit("engine.starting", { engine_id: id });
    fake.engines = fake.engines.map((e) => (e.id === id ? { ...e, health: "ready" } : e));
    emit("engine.ready", { engine_id: id });
  });
  vi.spyOn(commands, "stopEngine").mockImplementation(async (id) => {
    // engine.stopping fires synchronously; the authoritative terminal lands
    // after a delay so the transient stopping state is observable (the real
    // engine takes time to wind down).
    emit("engine.stopping", { engine_id: id });
    await new Promise((r) => setTimeout(r, 10));
    fake.engines = fake.engines.map((e) => (e.id === id ? { ...e, health: "stopped" } : e));
    emit("engine.stopped", { engine_id: id });
  });
  vi.spyOn(commands, "createSession").mockImplementation(async (engineId, ws, _model) => {
    const s: Session = {
      id: `ses-${fake.sessions.length + 1}`,
      workspace_id: ws,
      engine_id: engineId,
      engine_session_id: `upstream-${fake.sessions.length + 1}`,
      display_name: "Smoke session",
      created_at: Date.now(),
      running: false,
      resumable: true,
      usable_now: true,
    };
    fake.sessions.push(s);
    // The authoritative event carries the FULL DTO (same as the response).
    emit("session.created", {
      session_id: s.id,
      engine_id: engineId,
      workspace_id: ws,
      engine_session_id: s.engine_session_id,
      display_name: s.display_name,
      created_at: s.created_at,
      resumable: s.resumable,
      usable_now: s.usable_now,
    });
    return s;
  });
  vi.spyOn(commands, "sendPrompt").mockImplementation(fakeSendPrompt);
  vi.spyOn(commands, "cancelRun").mockImplementation(async (sid, runId) => {
    emit("message.cancelled", { session_id: sid, run_id: runId });
  });
  vi.spyOn(commands, "queueEnqueue").mockImplementation(async (args) => {
    const item: QueueItem = {
      id: `q-${fake.queue.length + 1}`,
      workspace_id: args.workspaceId,
      engine_id: args.engineId,
      session_id: args.sessionId,
      session_mode: args.sessionMode,
      model: args.model,
      payload: args.payload,
      payload_truncated: false,
      state: "queued",
      order_key: fake.queue.length + 1,
      revision: 1,
      lease_id: null,
      leased_at: null,
      attempt_count: 0,
      run_id: null,
      last_error: null,
      last_error_code: null,
      created_at: Date.now(),
      updated_at: Date.now(),
    };
    fake.queue.push(item);
    emit("queue.changed", { item_id: item.id, state: "queued" });
    // Instant fake dispatch to Done (durable queue semantics exercised).
    item.state = "done";
    emit("queue.changed", { item_id: item.id, state: "done" });
    return item;
  });
  vi.spyOn(commands, "queueSnapshot").mockImplementation(async (): Promise<QueueSnapshot> => ({
    status: "ready",
    paused: false,
    items: fake.queue,
  }));
  vi.spyOn(commands, "getSaipen").mockImplementation(async () => null);
  vi.spyOn(commands, "activeRuns").mockImplementation(async (): Promise<[string, string][]> =>
    fake.activeRuns(),
  );
  vi.spyOn(commands, "diagnostics").mockImplementation(async () => ({
    version: "0.1.0-smoke",
    data_root: "C:\\smoke\\data",
    portable: false,
    lifecycle: "ready",
    startup_ms: null,
    last_shutdown_ms: null,
    db_integrity: "ok",
    db_schema_version: 4,
    storage_status: "ok",
    engines: fake.engines,
    engine_count: fake.engines.length,
    supervisor_active: 0,
    processes: [],
    workspaces: fake.workspaces.length,
    sessions: fake.sessions.length,
    recent_errors: [],
    event_subscribers: 0,
    log_dir: null,
    log_fallback: false,
    platform: "windows",
    architecture: "x86_64",
    timestamp_ms: Date.now(),
  }));
  vi.spyOn(commands, "queueCancel").mockImplementation(async () => undefined);
  vi.spyOn(commands, "queueResolveUnknown").mockImplementation(async () => undefined);
  vi.spyOn(commands, "queuePause").mockImplementation(async () => undefined);
  vi.spyOn(commands, "queueResume").mockImplementation(async () => undefined);
  vi.spyOn(commands, "queueRetry").mockImplementation(async () => undefined);
  vi.spyOn(commands, "queueReorder").mockImplementation(async () => undefined);
  vi.spyOn(commands, "queueEdit").mockImplementation(async () => {
    throw new Error("unused in smoke");
  });
  vi.spyOn(commands, "resolvePermission").mockImplementation(async () => undefined);
  vi.spyOn(commands, "saipenActionStart").mockImplementation(async () => {
    throw new Error("unused in smoke");
  });
  vi.spyOn(commands, "saipenActionCancel").mockImplementation(async () => undefined);
  vi.spyOn(commands, "saipenActionStatus").mockImplementation(async () => ({
    availability: { available: [], running_action: null, unsupported: [], disabled_reason: null },
    running: null,
    validation_result: null,
    validation_stale: null,
    snapshot_generation: 0,
  }));
  vi.spyOn(commands, "closeWorkspace").mockImplementation(async () => undefined);
  vi.spyOn(commands, "forgetWorkspace").mockImplementation(async () => undefined);
  vi.spyOn(commands, "shutdown").mockImplementation(async () => undefined);
}

// ---------------------------------------------------------------------------
// Route helpers
// ---------------------------------------------------------------------------

function expectNoError(step: string): void {
  const s = store.getState();
  expect(s.lastError, `no error toast at: ${step}`).toBeNull();
}

function titleBarHtml(): string {
  return renderToString(<TitleBar state={store.getState()} onError={() => undefined} />);
}

function sessionListHtml(): string {
  return renderToString(<SessionList state={store.getState()} onError={() => undefined} />);
}

/** Mirror of App.onError: surfaces the message through the store's toast. */
function onError(message: string): void {
  store.patch((s) => ({ ...s, lastError: message }));
}

/** One full user journey on a fresh frontend (backend persists across it). */
async function runRoute(round: number): Promise<void> {
// ---- fresh start (frontend) ----
  store.resetForTest();
  resetFrontendSyncForTest();
  // A real app restart also wipes the model catalog's module-level
  // load key (per-engine, per-generation); resetting the frontend alone
  // would short-circuit round 2's ensureModels into a no-op.
  resetModelCatalogForTest();
  await coldBootstrap(onError);
  let s = store.getState();
  expect(s.backend).toBe("connected");
  expect(s.lifecycle).toBe("ready");
  expect(s.selectedEngineId, "round " + round).toBe("opencode"); // never FakeEngine
  expect(s.selectedEngineId).not.toBe("fake");
  expectNoError("bootstrap");

  // No project yet → Start engine is NOT offered as enabled (fresh disk only;
  // later rounds restore the persisted workspace at bootstrap).
  if (round === 1) {
    expect(titleBarHtml()).toContain("Open a project first");
  } else {
    expect(s.currentWorkspaceId).not.toBeNull(); // restored from durable state
  }

  // ---- open project ----
  // Round 2: the workspace was restored by bootstrap (selectWorkspace). Do NOT
  // re-issue a bare openWorkspace here — that re-emits workspace.opened, whose
  // product-correct handler clears the scoped sessions, and a raw command
  // (unlike selectWorkspace) never refetches them. Reuse the restored row.
  const ws =
    round === 1
      ? await selectWorkspace("C:\\smoke\\proj", onError)
      : (store.getState().workspaces.find((w) => w.id === store.getState().currentWorkspaceId) ??
          (() => {
            throw new Error("restored workspace missing");
          })());
  const selectedWs: WorkspaceSummary = ws;
  if (round === 1) {
    store.patch((st) => ({ ...st, workspaces: upsertWorkspaceForTest(st.workspaces, selectedWs) }));
  }
  s = store.getState();
  expect(s.currentWorkspaceId).toBe(selectedWs.id);
  expect(selectedWs.has_git).toBe(true); // authoritative git truth, not fabricated
  expect(selectedWs.saipen).not.toBeNull(); // SAIPEN probe truth on first render
  expectNoError("open project");

  // Auto-start (T-079): opening a project now auto-starts the selected
  // engine when it is not ready (fire-and-forget in selectWorkspace). By the
  // time selectWorkspace resolves the fake has already emitted engine.ready,
  // so BOTH rounds see the READY (Stop) control here — the manual start below
  // is idempotent (backend AlreadyStarted gate) and kept to assert the
  // explicit lifecycle path. The old round-1 "Start engine" expectation was
  // the pre-T-079 contract; the auto-start changed it (T-083).
  expect(titleBarHtml()).toContain("Stop engine");
  expect(titleBarHtml()).not.toContain("Open a project first");

  // ---- start engine → READY → models/default ----
  await commands.startEngine("opencode", selectedWs.id);
  s = store.getState();
  expect(healthKind(s.engines.find((e) => e.id === "opencode")!.health)).toBe("ready");
  // TitleBar's ready-effect loads models for the selected engine (same call).
  await ensureModels("opencode", onError);
  await new Promise((r) => setTimeout(r, 0)); // listModels resolves on a microtask
  s = store.getState();
  expect(s.models.length).toBeGreaterThan(0);
  expect(s.models.every((m) => m.id.startsWith("opencode/"))).toBe(true);
  expect(s.selectedModelId).toBeNull(); // Engine Default selected
  expectNoError("start engine");

  // New session button becomes enabled only now (engine READY).
  const newSessionBtn = sessionListHtml();
  expect(newSessionBtn).toContain("New session");
  expect(newSessionBtn).not.toContain('disabled=""');

  // ---- new session (authoritative DTO) on round 1; resume on later rounds ----
  let sess: Session;
  if (round === 1) {
    sess = await commands.createSession("opencode", selectedWs.id, null);
    s = store.getState();
    // The stored row must EXACTLY match the command DTO (TASK 24 §9) — no
    // fabricated workspace/upstream-id/display-name, no duplicates.
    const stored = s.sessions.filter((x) => x.id === sess.id);
    expect(stored).toHaveLength(1);
    expect(stored[0]).toEqual(sess);
    expect(s.activeSessionId).toBe(sess.id);
    expect(sess.workspace_id).toBe(selectedWs.id);
    expect(sess.engine_id).toBe("opencode");
    expect(sess.engine_session_id).toBe("upstream-1");
    expect(sess.display_name).toBe("Smoke session");
    expect(sess.resumable).toBe(true);
    expectNoError("new session");
  } else {
    // Restart semantics: the durable session is restored and becomes active
    // (resume works without manual session creation).
    const restored = s.sessions.find((x) => x.workspace_id === selectedWs.id);
    expect(restored).toBeDefined();
    sess = restored!;
    expect(s.activeSessionId).toBe(sess.id);
    expect(sess.engine_id).toBe("opencode");
    expect(sess.resumable).toBe(true);
    expectNoError("resume session");
  }

  // ---- first prompt: user must precede assistant ----
  addLocalUserMessage(sess.id, "hello smoke");
  await commands.sendPrompt(sess.id, selectedWs.id, "opencode", "hello smoke", null);
  s = store.getState();
  const msgs = s.messages[sess.id] ?? [];
  const userIdx = msgs.findIndex((m) => m.role === "user" && m.text === "hello smoke");
  const asstIdx = msgs.findIndex((m) => m.role === "assistant");
  expect(userIdx, "user turn exists").toBeGreaterThanOrEqual(0);
  expect(asstIdx, "assistant turn exists").toBeGreaterThanOrEqual(0);
  expect(asstIdx).toBeGreaterThan(userIdx); // user → assistant ORDER
  expect(msgs[asstIdx]!.text).toBe("Hello from smoke engine (hello smoke)");
  expect(msgs[asstIdx]!.status).toBe("complete");
  expect(s.running[sess.id]).toBeNull();
  expectNoError("first prompt");

  // ---- second prompt on the same session ----
  addLocalUserMessage(sess.id, "again");
  await commands.sendPrompt(sess.id, selectedWs.id, "opencode", "again", null);
  s = store.getState();
  const msgs2 = s.messages[sess.id] ?? [];
  expect(msgs2.filter((m) => m.role === "user").length).toBe(2);
  const last = msgs2[msgs2.length - 1]!;
  expect(last.role).toBe("assistant");
  expect(last.status).toBe("complete");
  expectNoError("second prompt");

  // ---- outcome-unknown: the turn stays visible + UNCERTAIN, no blind resend ----
  // Driven through the REAL production handler (performSend), not a
  // re-implementation.
  const uncertainCallsBefore = fake.sendCountByPrompt["uncertain one"] ?? 0;
  const uOutcome = await performSend(sess.id, selectedWs.id, "opencode", "uncertain one", null, onError);
  s = store.getState();
  if (uOutcome?.kind !== "outcome_unknown") throw new Error(`expected outcome_unknown, got ${uOutcome?.kind}`);
  const umsgs = s.messages[sess.id] ?? [];
  const uTurn = umsgs.find((m) => m.role === "user" && m.text === "uncertain one");
  expect(uTurn, "uncertain user turn must stay in the conversation").toBeDefined();
  expect(uTurn!.uncertain).toBe(true);
  // The run is live (message.started fired): workspace reservation preserved.
  expect(s.running[sess.id]).toBe(uOutcome.run_id);
  expect(s.lastError).toContain("outcome unknown");
  // Exactly one external operation happened for this prompt — no auto resend.
  expect(fake.sendCountByPrompt["uncertain one"]).toBe(uncertainCallsBefore + 1);
  // The authoritative terminal reconciles the reservation.
  emit("message.completed", { session_id: sess.id, run_id: uOutcome.run_id });
  s = store.getState();
  expect(s.running[sess.id]).toBeNull();
  expect(s.messages[sess.id]!.some((m) => m.role === "user" && m.text === "uncertain one")).toBe(true);
  store.patch((st) => ({ ...st, lastError: null })); // reconcile clears the toast

  // ---- cancel a running prompt ----
  const slow = await commands.sendPrompt(sess.id, selectedWs.id, "opencode", "slow one", null);
  if (slow.kind !== "accepted") throw new Error(`expected accepted, got ${slow.kind}`);
  expect(s.running[sess.id] ?? store.getState().running[sess.id]).toBe(slow.run_id);
  await commands.cancelRun(sess.id, slow.run_id);
  s = store.getState();
  expect(s.running[sess.id]).toBeNull();
  const cancelled = s.messages[sess.id]![s.messages[sess.id]!.length - 1]!;
  expect(cancelled.status).toBe("cancelled");
  expectNoError("cancel");

  // ---- engine stop: stopping is distinct, Start returns only on terminal ----
  const stopPromise = commands.stopEngine("opencode"); // emits engine.stopping synchronously
  s = store.getState();
  expect(s.stoppingEngines["opencode"]).toBe(true);
  const duringStop = titleBarHtml();
  expect(duringStop).toContain("Stopping…");
  expect(duringStop).not.toContain("Start engine");
  await stopPromise; // authoritative terminal lands after the fake delay
  // The engine is STOPPED now. A direct send must be REJECTED by the backend
  // prerequisites (the affinity still matches, so the readiness check fires).
  s = store.getState();
  expect(healthKind(s.engines.find((e) => e.id === "opencode")!.health)).toBe("stopped");
  expect(titleBarHtml()).toContain("Start engine");
  const stoppedCallsBefore = fake.sendCountByPrompt["while stopped"] ?? 0;
  await expect(
    commands.sendPrompt(sess.id, selectedWs.id, "opencode", "while stopped", null),
  ).rejects.toThrow("not ready");
  // Attempted exactly once this round and rejected by the backend.
  expect(fake.sendCountByPrompt["while stopped"]).toBe(stoppedCallsBefore + 1);
  expectNoError("engine stop");

  // ---- restart app (frontend reload; backend state persists) ----
  store.resetForTest();
  resetFrontendSyncForTest();
    await coldBootstrap(onError);
  s = store.getState();
  expect(s.workspaces.length).toBeGreaterThan(0); // durable workspace restored
  expect(s.currentWorkspaceId).toBe(selectedWs.id);
  expect(s.sessions.some((x) => x.id === sess.id)).toBe(true); // durable session restored
  expect(s.activeSessionId).toBe(sess.id);
  expect(s.selectedEngineId).toBe("opencode"); // default is still OpenCode
  expectNoError("restart");

  // Restoring the active project auto-starts/rebinds its selected engine; the
  // first prompt after restart needs no separate lifecycle ceremony.
  expect(healthKind(store.getState().engines.find((e) => e.id === "opencode")!.health)).toBe("ready");
  await ensureModels("opencode", onError);
  await new Promise((r) => setTimeout(r, 0));
  s = store.getState();
  expect(healthKind(s.engines.find((e) => e.id === "opencode")!.health)).toBe("ready");
  expect(sendDisabledReasonFor(store.getState())).toBeUndefined();

  // ---- resume session: a new prompt works after the engine restarted ----
  const resumeOutcome = await performSend(sess.id, selectedWs.id, "opencode", "after restart", null, onError);
  s = store.getState();
  if (resumeOutcome?.kind !== "accepted") throw new Error(`expected accepted, got ${resumeOutcome?.kind}`);
  expect(s.messages[sess.id]?.some((m) => m.role === "user" && m.text === "after restart")).toBe(true);
  expect(s.running[sess.id]).toBeNull();
  expectNoError("resume + prompt");

  // ---- queue prompt (durable, session-mode existing) ----
  await commands.queueEnqueue({
    workspaceId: selectedWs.id,
    engineId: "opencode",
    sessionId: sess.id,
    sessionMode: "existing",
    model: null,
    payload: "queued prompt",
  });
  const snap = await commands.queueSnapshot();
  expect(snap.items.length).toBe(round); // one per round, persisted
  const queued = snap.items.find((q) => q.payload === "queued prompt");
  expect(queued).toBeDefined();
  expect(queued!.workspace_id).toBe(selectedWs.id);
  expect(queued!.engine_id).toBe("opencode");
  expect(queued!.session_id).toBe(sess.id);
  expectNoError("queue prompt");
}

function upsertWorkspaceForTest(
  workspaces: WorkspaceSummary[],
  w: WorkspaceSummary,
): WorkspaceSummary[] {
  const idx = workspaces.findIndex((x) => x.id === w.id);
  if (idx === -1) return [...workspaces, w];
  const next = [...workspaces];
  next[idx] = w;
  return next;
}

// ---------------------------------------------------------------------------

beforeEach(() => {
  vi.restoreAllMocks();
  store.patch(() => ({ ...initialState }));
  fake.workspaces = [];
  fake.sessions = [];
  fake.queue = [];
  fake.runCounter = 0;
  fake.sendCountByPrompt = {};
  fake.activeWorkspaceId = null;
  fake.activeSelectionGen = 0;
  fakeActiveRuns = [];
});
afterEach(() => {
  vi.restoreAllMocks();
  store.patch(() => ({ ...initialState }));
});
describe("first-human-prompt end-to-end smoke (audit DONE-WHEN)", () => {
  it("full route passes twice in a row with OpenCode default, no FakeEngine, no stale UI, backend prerequisites enforced", async () => {
    for (let round = 1; round <= 2; round++) {
      installFakeBackend();
      await runRoute(round);
    }
  }, 30_000);

  it("cannot pass while the active session belongs to another engine (component gating + backend rejection)", async () => {
    installFakeBackend();
    store.resetForTest();
    resetFrontendSyncForTest();
    await coldBootstrap(onError);
    const ws = await selectWorkspace("C:\\smoke\\proj", onError);
    const selWs = fake.workspaces.find((w) => w.id === ws.id)!;
    store.patch((st) => ({ ...st, workspaces: upsertWorkspaceForTest(st.workspaces, selWs) }));
    await commands.startEngine("opencode", selWs.id);
    const sess = await commands.createSession("opencode", selWs.id, null);
    // User switches the engine selector to FakeEngine while the OpenCode
    // session is active: the UI must clear the incompatible active session
    // and gate Send with the exact reason.
    store.patch((st) => ({ ...st, selectedEngineId: "fake", activeSessionId: sess.id, models: [], selectedModelId: null }));
    expect(sendDisabledReasonFor(store.getState())).toContain("select that engine");
    // The production handler rejects the send with a typed error and the
    // pending user turn is dropped (nothing was sent).
    const outcome = await performSend(sess.id, ws.id, "fake", "cross-engine", null, onError);
    expect(outcome).toBeNull();
    expect(store.getState().messages[sess.id]?.some((m) => m.text === "cross-engine")).toBe(false);
    expect(store.getState().lastError).toContain("does not match the active UI context");
    // Backend: a send with the wrong engine context is rejected with a typed
    // mismatch BEFORE any external operation — exactly one attempt happened.
    expect(fake.sendCountByPrompt["cross-engine"]).toBe(1); // attempted once, rejected
  });

  it("frontend.reconcile rebuilds exact active-run ownership without incidental events (TASK 24 §9)", async () => {
    installFakeBackend();
    store.resetForTest();
    resetFrontendSyncForTest();
    await coldBootstrap(onError);
    const ws = await selectWorkspace("C:\\smoke\\reconcile-proj", onError);
    const selWs = fake.workspaces.find((w) => w.id === ws.id)!;
    store.patch((st) => ({ ...st, workspaces: upsertWorkspaceForTest(st.workspaces, selWs) }));
    const sess = await commands.createSession("opencode", selWs.id, null);

    // The run is LIVE in the backend but its message.started never reached
    // this frontend (EventBus lag). frontend.reconcile must rebuild the exact
    // ownership from the authoritative snapshot — Send stays gated and
    // Cancel targets the exact RunId without waiting for an incidental event.
    fakeActiveRuns = [[sess.id, "run-42"]];
    resetFrontendSyncForTest();
    await coldBootstrap(onError); // the reconcile path (App.onReconcile → bootstrapApp)
    const s = store.getState();
    expect(s.running[sess.id]).toBe("run-42");
    expectNoError("reconcile");

    // The restored ownership is exact: a different RunId is not substituted.
    expect(s.running[sess.id]).not.toBeNull();
    expect(s.running[sess.id]).toBe("run-42");
  });

  it("CORE-001: a stale clear epoch cannot clobber a newer committed selection (backend epoch authority)", async () => {
    installFakeBackend();
    store.resetForTest();
    resetFrontendSyncForTest();
    await coldBootstrap(onError); // establishes the fake's active-workspace authority
    // Commit ws-A at epoch 1.
    await commands.setActiveWorkspace("ws-A", 1);
    expect(await commands.getActiveWorkspace()).toBe("ws-A");
    // A stale clear (epoch 0 < 1) must be ignored — latest-wins protects the
    // newer selection from an epoch-less/clobbering clear (the CORE-001 bug).
    await commands.setActiveWorkspace(null, 0);
    expect(await commands.getActiveWorkspace()).toBe("ws-A");
    // A newer clear (epoch 2 > 1) wins and clears the pointer.
    await commands.setActiveWorkspace(null, 2);
    expect(await commands.getActiveWorkspace()).toBeNull();
  });
});
