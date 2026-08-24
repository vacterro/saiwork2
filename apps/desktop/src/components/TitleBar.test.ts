import { describe, expect, it, vi, beforeEach } from "vitest";
import { store, initialState } from "../state/store";
import { commands } from "../app/backend";
import { ensureModels, installModelCatalog, resetModelCatalogForTest } from "../app/modelCatalog";

// Model discovery is now owned by the single app catalog (modelCatalog.ts):
// capability-driven (engines without `models` never trigger discovery) and
// generation-guarded (a slow response from engine A cannot overwrite engine
// B's selection). These tests exercise that owner directly.

// Model discovery is capability-driven and generation-guarded (TASK 17
// §60–§61, §111–§112): engines without `models` never trigger discovery,
// and a slow response from engine A cannot overwrite engine B's selection.

function engine(id: string, models: boolean) {
  return {
    id,
    display_name: id,
    version: "0",
    experimental: false,
    health: "ready" as const,
    capabilities: {
      streaming: false,
      sessions: true,
      resume: false,
      cancel: true,
      tools: false,
      permissions: false,
      attachments: false,
      images: false,
      models,
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

beforeEach(() => {
  vi.restoreAllMocks();
  // Reset the singleton store to a clean slate.
  store.patch(() => ({ ...initialState }));
  // Each test must issue a FRESH catalog load — the catalog is keyed by
  // (engine, generation) and would otherwise short-circuit a second load of
  // the same engine within the same generation.
  resetModelCatalogForTest();
});
describe("loadModelsFor (§111–§112)", () => {
  it("skips discovery for engines that do not declare models", async () => {
    const list = vi.spyOn(commands, "listModels");
    store.patch((s) => ({
      ...s,
      engines: [engine("generic-cli", false)],
      selectedEngineId: "generic-cli",
    }));
    ensureModels("generic-cli");
    await new Promise((r) => setTimeout(r, 0));
    expect(list).not.toHaveBeenCalled();
    expect(store.getState().models).toEqual([]);
  });

  it("loads models for a models-capable engine", async () => {
    vi.spyOn(commands, "listModels").mockResolvedValue([
      { id: "m1", display_name: "Model One", provider: "x", provider_name: "X" },
    ]);
    store.patch((s) => ({
      ...s,
      engines: [engine("opencode", true)],
      selectedEngineId: "opencode",
    }));
    ensureModels("opencode");
    await vi.waitFor(() => {
      expect(store.getState().models).toHaveLength(1);
    });
  });

  it("discards a stale response after the user switched engines (race guard)", async () => {
    // Engine A responds slowly; the user switches to engine B meanwhile.
    let resolveA!: (v: { id: string; display_name: string; provider: string | null; provider_name: string | null }[]) => void;
    const pending = new Promise<{ id: string; display_name: string; provider: string | null; provider_name: string | null }[]>(
      (r) => {
        resolveA = r;
      },
    );
    vi.spyOn(commands, "listModels").mockReturnValueOnce(pending as never);
    store.patch((s) => ({
      ...s,
      engines: [engine("a", true), engine("b", true)],
      selectedEngineId: "a",
    }));
    ensureModels("a");
    // User switches to B before A's response arrives.
    store.patch((s) => ({ ...s, selectedEngineId: "b" }));
    resolveA([{ id: "a-model", display_name: "A", provider: "p", provider_name: null }]);
    await new Promise((r) => setTimeout(r, 0));
    expect(store.getState().models).toEqual([]);
    expect(store.getState().selectedEngineId).toBe("b");
  });

  it("surfaces the REAL backend error as a non-fatal warning, not lastError", async () => {
    // TASK 25 §2/§23: the masked "failed to list models" must be gone; the
    // actual backend diagnostic survives in modelsError, lastError stays
    // untouched (non-fatal), and Send remains usable via Engine Default.
    vi.spyOn(commands, "listModels").mockRejectedValue(
      new Error("OpenCode returned HTTP 500 for list providers: provider list exploded"),
    );
    store.patch((s) => ({
      ...s,
      engines: [engine("opencode", true)],
      selectedEngineId: "opencode",
      lastError: "some unrelated previous error",
    }));
    ensureModels("opencode");
    await vi.waitFor(() => {
      expect(store.getState().modelsLoading).toBe(false);
    });
    const st = store.getState();
    expect(st.models).toEqual([]);
    expect(st.modelsError).toContain("opencode model discovery failed");
    expect(st.modelsError).toContain("HTTP 500 for list providers");
    expect(st.lastError).toBe("some unrelated previous error");
    expect(st.selectedModelId).toBeNull();
  });

  it("shows the loading projection while the request is in flight", async () => {
    let resolveA!: (v: { id: string; display_name: string; provider: string | null; provider_name: string | null }[]) => void;
    const pending = new Promise<{ id: string; display_name: string; provider: string | null; provider_name: string | null }[]>(
      (r) => {
        resolveA = r;
      },
    );
    vi.spyOn(commands, "listModels").mockReturnValueOnce(pending as never);
    store.patch((s) => ({
      ...s,
      engines: [engine("opencode", true)],
      selectedEngineId: "opencode",
    }));
    ensureModels("opencode");
    await new Promise((r) => setTimeout(r, 0));
    expect(store.getState().modelsLoading).toBe(true);
    resolveA([{ id: "m1", display_name: "Model One", provider: "x", provider_name: null }]);
    await vi.waitFor(() => {
      expect(store.getState().modelsLoading).toBe(false);
    });
    expect(store.getState().modelsError).toBeNull();
    expect(store.getState().models).toHaveLength(1);
  });

it("engine switch clears the previous engine's model error", async () => {
    vi.spyOn(commands, "listModels").mockRejectedValue(new Error("boom"));
    store.patch((s) => ({
      ...s,
      engines: [engine("a", true), engine("b", true)],
      selectedEngineId: "a",
    }));
    ensureModels("a");
    await vi.waitFor(() => {
      expect(store.getState().modelsError).not.toBeNull();
    });
    store.patch((s) => ({
      ...s,
      selectedEngineId: "b",
      models: [],
      selectedModelId: null,
      modelsLoading: false,
      modelsError: null,
    }));
    expect(store.getState().modelsError).toBeNull();
    expect(store.getState().modelsLoading).toBe(false);
  });

  it("installModelCatalog is the SINGLE owner: one load per ready transition", async () => {
    // The title bar no longer triggers discovery itself (the effect was
    // removed — the catalog owner is app-scoped). Only installModelCatalog
    // reacts to engine/health changes.
    const list = vi.spyOn(commands, "listModels").mockResolvedValue([
      { id: "m1", display_name: "Model One", provider: "x", provider_name: "X" },
    ]);
    store.patch((s) => ({
      ...s,
      engines: [engine("opencode", true)],
      selectedEngineId: "opencode",
    }));
    const dispose = installModelCatalog();
    await vi.waitFor(() => {
      expect(store.getState().models).toHaveLength(1);
    });
    expect(list).toHaveBeenCalledTimes(1);

    // Repeated reconcile ticks (store churn) must NOT reload: same engine,
    // same health, same generation.
    store.patch((s) => ({ ...s, modelsLoading: false }));
    store.patch((s) => ({ ...s, modelsError: null }));
    await new Promise((r) => setTimeout(r, 0));
    expect(list).toHaveBeenCalledTimes(1);

    // Engine → stopped → ready IS a new runtime generation: exactly one
    // reload.
    store.patch((s) => ({
      ...s,
      engines: [{ ...s.engines[0]!, health: "stopped" }],
    }));
    store.patch((s) => ({
      ...s,
      engines: [{ ...s.engines[0]!, health: "ready" }],
    }));
    await vi.waitFor(() => {
      expect(list).toHaveBeenCalledTimes(2);
    });
    dispose();
  });
});
