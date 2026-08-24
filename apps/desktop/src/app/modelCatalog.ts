// THE model-catalog owner (T-031).
//
// Model discovery previously had THREE independent owners: a TitleBar effect, the
// engine-selection store action, and cold bootstrap. Generation guards discarded
// the stale RESULT, but the expensive work still ran two or three times — with a
// catalog of ~6.6k models that is material. Worse, the loader lived inside a UI
// component, so mounting/unmounting the title bar changed backend traffic.
//
// This module owns:
//   * a single canonical `ensureModels(engineId)` entry point,
//   * SAME-GENERATION single-flight: concurrent requests for the same engine
//     within the same runtime generation SHARE one in-flight promise instead of
//     issuing a second `list_models`,
//   * generation invalidation: an engine restart / provider change bumps the
//     generation, so a newer request is a real reload and an older response can
//     no longer commit,
//   * capability + health gating: only a READY engine that declares `models`
//     gets a catalog; anything else is an authoritative empty projection.
//
// UI components only render; `installModelCatalog()` is the single reaction to
// selection/health changes.

import { healthKind } from "@saiwork2/contracts";
import { commands } from "./backend";
import { store } from "../state/store";
import { requestEngineAutoStart } from "./engineAutoStart";

/** Monotonic runtime generation for model truth. Bumped on every engine
 * selection change and on every engine health transition of the selected
 * engine, because an engine restart can legitimately change the catalog. */
let generation = 0;

/** The in-flight load, keyed by `${engineId}#${generation}` — the single-flight
 * identity. A second request with the same key awaits the same promise. */
let inFlight: { key: string; promise: Promise<void> } | null = null;

/** The (engine, generation) whose catalog is currently committed: a repeat
 * request for the same key is a no-op, not a second expensive read. */
let loadedKey: string | null = null;

function keyFor(engineId: string): string {
  return `${engineId}#${generation}`;
}

/** Invalidate the current catalog truth (engine restart / selection change). */
export function invalidateModelCatalog(): void {
  generation += 1;
  inFlight = null;
}

/** Clear the model projection authoritatively (no engine, or an engine that
 * cannot serve models). Never leaves a previous engine's ids selectable. */
function clearModels(): void {
  store.patch((st) =>
    st.models.length === 0 && st.selectedModelId === null && !st.modelsLoading && st.modelsError === null
      ? st
      : { ...st, models: [], selectedModelId: null, modelsLoading: false, modelsError: null },
  );
}

/**
 * Ensure the model catalog for `engineId` is loaded. Idempotent and
 * single-flight per (engine, generation): calling it from three places in the
 * same generation performs ONE backend read.
 */
export function ensureModels(
  engineId: string | null,
  onError: (message: string) => void = () => undefined,
): Promise<void> {
  if (!engineId) {
    clearModels();
    return Promise.resolve();
  }
  const s = store.getState();
  const engine = s.engines.find((e) => e.id === engineId);
  if (!engine || !engine.capabilities.models || healthKind(engine.health) !== "ready") {
    clearModels();
    return Promise.resolve();
  }
  const key = keyFor(engineId);
  if (inFlight && inFlight.key === key) return inFlight.promise;
  // Already loaded for this generation: no work at all (this is what made the
  // TitleBar effect + bootstrap + selection triple-load).
  if (loadedKey === key) return Promise.resolve();

  const myGeneration = generation;
  const promise = (async () => {
    store.patch((st) =>
      myGeneration === generation && st.selectedEngineId === engineId
        ? { ...st, modelsLoading: true, modelsError: null }
        : st,
    );
    try {
      const models = await commands.listModels(engineId);
      if (myGeneration !== generation) return; // superseded: discard
      loadedKey = key;
      store.patch((st) => {
        if (st.selectedEngineId !== engineId) return st;
        const eng = st.engines.find((e) => e.id === engineId);
        if (!eng || healthKind(eng.health) !== "ready") return st;
        // Retention rule: the selected id survives only if the exact id is
        // still present — a model that disappeared after a restart/provider
        // change becomes Engine Default instead of a stale id that would
        // surface ModelUnavailable at send time.
        const selectedModelId =
          st.selectedModelId !== null && models.some((m) => m.id === st.selectedModelId)
            ? st.selectedModelId
            : null;
        return { ...st, models, selectedModelId, modelsLoading: false, modelsError: null };
      });
    } catch (e) {
      if (myGeneration !== generation) return;
      store.patch((st) => {
        if (st.selectedEngineId !== engineId) return st;
        const eng = st.engines.find((x) => x.id === engineId);
        const engineName = eng ? eng.display_name : engineId;
        return {
          ...st,
          models: [],
          selectedModelId: null,
          modelsLoading: false,
          // The REAL backend diagnostic survives (never reduced to "failed to
          // load models"). Non-fatal: Engine Default and Send stay available.
          modelsError: `${engineName} model discovery failed: ${String(e)}`,
        };
      });
      // Model discovery is metadata, NOT a fatal error: surface it ONLY as the
      // title-bar warning (modelsError) above. We must NOT route it to the
      // global onError/lastError toast — that would clobber an unrelated real
      // error and make a recoverable condition look fatal (TASK 25 §2/§23).
      void onError;
    } finally {
      if (inFlight && inFlight.key === key) inFlight = null;
    }
  })();
  inFlight = { key, promise };
  return promise;
}

/**
 * Select an engine (the ONLY engine-selection path). Selection is a store
 * change; the catalog reaction is this module's job — the component never
 * fetches.
 */
export function selectEngine(id: string | null): void {
  invalidateModelCatalog();
  loadedKey = null;
  store.patch((s) => {
    // An engine switch clears an active session belonging to another engine:
    // the backend send boundary rejects the mismatch and the UI must not
    // invite it. Messages stay in the store (re-selectable later).
    const activeSession = s.activeSessionId
      ? s.sessions.find((x) => x.id === s.activeSessionId) ?? null
      : null;
    const activeSessionId =
      activeSession && id && activeSession.engine_id !== id ? null : s.activeSessionId;
    return {
      ...s,
      selectedEngineId: id,
      models: [],
      selectedModelId: null,
      modelsLoading: false,
      modelsError: null,
      activeSessionId,
    };
  });
  void ensureModels(id);
  void persistEngineState();
  // A selected engine is expected to be usable for the already-open project.
  // One latest-intent scheduler owns stop/start ordering across both engine
  // and workspace selections.
  const current = store.getState();
  void requestEngineAutoStart(id, current.currentWorkspaceId, (message) => {
    store.patch((state) => ({ ...state, lastError: message }));
  });
}

export function selectModel(id: string | null): void {
  store.patch((s) => (s.selectedModelId === id ? s : { ...s, selectedModelId: id }));
  void persistEngineState();
}

const ENGINE_STATE_KEY = "ui.engine.v1";

async function persistEngineState(): Promise<void> {
  const s = store.getState();
  const payload = JSON.stringify({
    engineId: s.selectedEngineId,
    modelId: s.selectedModelId,
  });
  try {
    await commands.setSetting(ENGINE_STATE_KEY, payload);
  } catch {
    // Non-fatal: persistence failure should not break selection.
  }
}

export async function restoreEngineState(): Promise<void> {
  try {
    const raw = await commands.getSetting(ENGINE_STATE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw) as { engineId?: string | null; modelId?: string | null };
    const s = store.getState();
    // Only restore if no engine is currently selected (cold boot) and the
    // stored engine still exists in the current engine list.
    if (s.selectedEngineId !== null) return;
    const engineId = parsed.engineId ?? null;
    const modelId = parsed.modelId ?? null;
    if (engineId && s.engines.some((e) => e.id === engineId)) {
      store.patch((st) => ({ ...st, selectedEngineId: engineId, selectedModelId: modelId }));
      void ensureModels(engineId);
    }
  } catch {
    // Corrupt or missing persistence is non-fatal.
  }
}

/**
 * Install the single orchestration reaction: whenever the selected engine or
 * its health changes, ensure the catalog matches. Returns a disposer.
 *
 * This is subscribed ONCE by the frontend-sync owner, so the catalog stays
 * correct regardless of which panels are mounted.
 */
export function installModelCatalog(): () => void {
  let lastEngineId = store.getState().selectedEngineId;
  let lastEngines = store.getState().engines;
  let lastHealth = healthOf(lastEngineId);
  // Reconcile immediately for the state that exists at install time.
  void ensureModels(lastEngineId);
  return store.subscribe(() => {
    const s = store.getState();
    // Cheap rejection FIRST: this listener runs on every store transition,
    // including every streamed token batch, so it must not scan engines then.
    if (s.selectedEngineId === lastEngineId && s.engines === lastEngines) return;
    lastEngines = s.engines;
    const engineId = s.selectedEngineId;
    const health = healthOf(engineId);
    if (engineId === lastEngineId && health === lastHealth) return;
    const engineChanged = engineId !== lastEngineId;
    lastEngineId = engineId;
    lastHealth = health;
    // A health transition of the SELECTED engine is a new runtime generation:
    // the previous catalog is not proven valid for the new runtime.
    invalidateModelCatalog();
    if (engineChanged) loadedKey = null;
    void ensureModels(engineId);
  });
}

function healthOf(engineId: string | null): string {
  if (!engineId) return "none";
  const engine = store.getState().engines.find((e) => e.id === engineId);
  return engine ? healthKind(engine.health) : "none";
}

/** Test-only reset of catalog ownership state. */
export function resetModelCatalogForTest(): void {
  generation += 1;
  inFlight = null;
  loadedKey = null;
}
