// W2-04 regression: a successful `stopEngine` must settle the optimistic stop
// latch INDEPENDENTLY of the `engine.stopped` event. The event stream can drop
// that terminal (the exact case `refreshEngines` is documented to recover); when
// it does, the old code left `stoppingEngines[id]` set forever — Start stayed
// unreachable until a manual reload. We drive the real owner (`runStopEngine`)
// with the backend mocked, suppress the terminal event (it never reaches the
// store here), and assert the latch clears and Start is reachable.
import { describe, expect, it, vi, beforeEach } from "vitest";
import { renderToString } from "react-dom/server";
import type { EngineHealth, EngineInfo } from "@saiwork2/contracts";
import { store, initialState } from "../state/store";
import { commands } from "../app/backend";
import { pickSlice } from "../state/slices";
import { TitleBar, runStopEngine, titleBarKeys } from "./TitleBar";

function engine(id: string, health: EngineHealth): EngineInfo {
  return {
    id,
    display_name: id,
    version: "0",
    experimental: false,
    health,
    capabilities: {
      streaming: false,
      sessions: true,
      resume: false,
      cancel: true,
      tools: false,
      permissions: false,
      attachments: false,
      images: false,
      models: false,
      usage: false,
      reasoning: false,
      context_window: null,
      worktrees: false,
      parallel_sessions: false,
      session_revert: false,
      structured_events: false,
    },
  };
}

/** Find a rendered <button> by exact label and report whether it is disabled. */
function isButtonDisabled(html: string, label: string): boolean {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const m = html.match(new RegExp(`<button[^>]*>\\s*${escaped}\\s*</button>`));
  if (!m) throw new Error(`button not found: ${label}`);
  return m[0].includes("disabled");
}

function renderTitleBar(): string {
  return renderToString(
    <TitleBar state={pickSlice(store.getState(), titleBarKeys)} onError={() => {}} />,
  );
}

describe("stopEngine lifecycle (W2-04)", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    store.patch(() => ({ ...initialState }));
    // App ready + a workspace open + a READY engine selected, with the stop
    // latch already set (as if the optimistic click projection fired). The
    // `engine.stopped` event is intentionally NOT dispatched — these tests
    // exercise the command + refresh path the component owns.
    store.patch((s) => ({
      ...s,
      lifecycle: "ready",
      currentWorkspaceId: "w1",
      workspaces: [{ id: "w1", name: "W1", path: "/w1" }] as never,
      engines: [engine("e1", "ready")],
      selectedEngineId: "e1",
      stoppingEngines: { e1: true },
    }));
  });

  it("settles the stop latch on success even when the engine.stopped event is lost", async () => {
    // Command succeeds; the authoritative refresh confirms Stopped; the
    // terminal event is suppressed (never arrives in this harness).
    vi.spyOn(commands, "stopEngine").mockResolvedValue(undefined);
    vi.spyOn(commands, "listEngines").mockResolvedValue([engine("e1", "stopped")]);

    await runStopEngine("e1", () => {});

    const st = store.getState();
    // The latch is gone — Start is no longer wedged on a phantom "Stopping…".
    expect(st.stoppingEngines.e1).toBeUndefined();
    // Authoritative reconciliation reflected the terminal state.
    expect(st.engines[0]?.health).toBe("stopped");

    const html = renderTitleBar();
    expect(html).not.toContain("Stopping…");
    // Start engine is offered AND enabled (workspace open, engine stopped).
    expect(html).toContain("Start engine");
    expect(isButtonDisabled(html, "Start engine")).toBe(false);
    // Stop engine is no longer offered (engine is not ready).
    expect(html).not.toContain("Stop engine");
  });

  it("still clears the permanent stopping latch when the post-stop refresh fails", async () => {
    // Command succeeded, but the authoritative listEngines pull FAILS — the
    // success-branch latch clear must still hold so the UI stays recoverable
    // (not wedged until F5).
    vi.spyOn(commands, "stopEngine").mockResolvedValue(undefined);
    vi.spyOn(commands, "listEngines").mockRejectedValue(new Error("listEngines boom"));

    await runStopEngine("e1", () => {});

    const st = store.getState();
    expect(st.stoppingEngines.e1).toBeUndefined();
    // The optimistic Stopped projection survives the failed refresh (truthful).
    expect(st.engines[0]?.health).toBe("stopped");

    const html = renderTitleBar();
    expect(html).not.toContain("Stopping…");
    expect(html).toContain("Start engine");
    expect(isButtonDisabled(html, "Start engine")).toBe(false);
  });
});
