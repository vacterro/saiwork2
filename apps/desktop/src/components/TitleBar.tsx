import { healthKind } from "@saiwork2/contracts";
import type { ModelInfo } from "@saiwork2/contracts";
import type { SliceProps } from "../state/slices";
import { commands } from "../app/backend";
import { useSingleFlight } from "../app/singleFlight";
import { selectEngine, selectModel } from "../app/modelCatalog";
import { ModelPicker } from "./ModelPicker";
import { mutationsAllowed } from "../app/eligibility";
import { runPresetImport } from "../app/presetImport";

/** One definition of what the title bar consumes (state/slices.ts). */
export const titleBarKeys = [
  "workspaces",
  "engines",
  "selectedEngineId",
  "currentWorkspaceId",
  "startingEngines",
  "stoppingEngines",
  "models",
  "selectedModelId",
  "modelsLoading",
  "modelsError",
  "favoriteModelIds",
  "favoritesOnly",
  "lifecycle",
] as const;

// W2-005: the favorite-write generation now lives in the store module (lifted
// from here) so the cold-bootstrap hydration and these toggles share ONE
// counter. See `favoritesGen` / `nextFavoritesGen` in ../state/store.

type Props = SliceProps<(typeof titleBarKeys)[number]>;

export function TitleBar({ state, onError }: Props) {
  const workspace = state.workspaces.find((w) => w.id === state.currentWorkspaceId) ?? null;

  // W2-009: one synchronous single-flight latch for the engine lifecycle
  // controls. React `disabled` / state guards are not synchronous (they only
  // take effect after re-render), so two native Start clicks in the same tick
  // would both reach the backend — the loser gets `AlreadyStarted` and the UI
  // shows a false error. Start and Stop share one latch because they are
  // mutually exclusive for one selected engine.
  const engineLifecycle = useSingleFlight();

  async function startEngine() {
    const id = state.selectedEngineId;
    if (!id) return;
    // W2-008: no new engine lifecycle mutation while the app is shutting down
    // (the backend rejects it too, but the UI must not even invite it).
    if (!mutationsAllowed(store.getState())) return;
    await engineLifecycle.run(async () => {
      // Optimistic starting projection: the button flips to "Starting…"
      // INSTANTLY — no event round-trip, no manual reload needed to see
      // progress (user report: F5 was required to see Start→Ready). The id is
      // captured here and used consistently in `finally` so an engine
      // selection change while awaiting does not clear another engine's flag.
      markEngineStarting(id, true);
      try {
        await commands.startEngine(id, state.currentWorkspaceId);
        // Authoritative pull after the invoke resolves: if the event stream
        // is alive this is an idempotent no-op; if it is not, the UI is still
        // correct without a manual reload.
        await refreshEngines();
      } catch (e) {
        onError(String(e));
      } finally {
        markEngineStarting(id, false);
      }
    });
  }

  async function stopEngine() {
    const id = state.selectedEngineId;
    if (!id) return;
    // W2-008: no new engine lifecycle mutation while the app is shutting down.
    if (!mutationsAllowed(store.getState())) return;
    await engineLifecycle.run(() => runStopEngine(id, onError));
  }

  /** Optimistic star toggle, persisted through the app authority; rollback on a
   * definite backend rejection (CodeNomad preference pattern). The rollback is
   * generation-guarded (T-023): only the still-owning (most-recent) write may
   * revert its optimistic change. An older failed write that rejects AFTER a
   * newer toggle (success or failure) must NOT clobber the newer durable/UI set. */
  function toggleFavoriteForSelected() {
    if (!mutationsAllowed(store.getState())) return;
    const { selectedModelId, favoriteModelIds } = store.getState();
    if (!selectedModelId) return;
    const myGen = nextFavoritesGen();
    const prev = favoriteModelIds;
    const next = toggleFavoriteModel(selectedModelId);
    void commands.setModelFavorites(next).catch((e) => {
      // Only revert if we still own the latest write — a newer toggle (or a
      // late bootstrap hydration) has superseded ours, so reverting would
      // overwrite the newer set (W2-005: shared generation).
      if (myGen === favoritesGen()) {
        store.patch((s) => ({ ...s, favoriteModelIds: prev }));
      }
      onError(`favorites not saved: ${String(e)}`);
    });
  }

  const engine = state.engines.find((e) => e.id === state.selectedEngineId) ?? null;
  const ready = engine !== null && healthKind(engine.health) === "ready";
  const stopping = engine !== null && Boolean(state.stoppingEngines[engine.id]);
  const starting = engine !== null && Boolean(state.startingEngines[engine.id]);
  const health = engine ? healthKind(engine.health) : "unknown";
  const allowed = mutationsAllowed(state);
  // First-use gating (TASK 24 §9): Start is only offered when a workspace is
  // open (OpenCode requires one) and the engine is not in a transitional
  // stopping state — the UI never invokes an impossible command.
  const canStart =
    allowed && engine !== null && !ready && !stopping && !starting && state.currentWorkspaceId !== null;
  const canStop = allowed && engine !== null && ready && !stopping && !starting;
  const canFavorite = allowed && Boolean(state.selectedModelId);


  return (
    <header className="titlebar">
      <div className="titlebar__project" title={workspace?.path ?? undefined}>
        <span className="label">PROJECT</span>
        <span className="value">{workspace ? workspace.name : "— none —"}</span>
      </div>

      <div className="titlebar__engine">
        <span className="label">ENGINE</span>
        <select
          value={state.selectedEngineId ?? ""}
          onChange={(e) => {
            const id = e.target.value || null;
            selectEngine(id);
          }}
        >
          <option value="">— choose —</option>
          {state.engines.map((eng) => (
            <option key={eng.id} value={eng.id}>
              {eng.display_name}
              {eng.experimental ? " ⚠" : ""}
              {healthKind(eng.health) === "ready" ? " ✓" : ""}
            </option>
          ))}
        </select>
      </div>

       <div className="titlebar__model">
        <span className="label">MODEL</span>
        {engine?.capabilities.models ? (
          <>
            {state.modelsLoading ? (
              <button className="btn btn--small" disabled title="Loading model catalog…">
                Loading models…
              </button>
            ) : (
              <div className="titlebar__model-row">
                <ModelPicker
                  models={state.models}
                  favorites={state.favoriteModelIds}
                  favoritesOnly={state.favoritesOnly}
                  selectedModelId={state.selectedModelId}
                  onSelect={(id) => selectModel(id)}
                />
                <button
                  className="btn btn--small titlebar__star"
                  disabled={!canFavorite}
                  title={
                    !allowed
                      ? "Application is not ready"
                      : state.selectedModelId
                        ? state.favoriteModelIds.includes(state.selectedModelId)
                          ? "Remove from favorites"
                          : "Add to favorites"
                        : "Select a model first"
                  }
                  onClick={toggleFavoriteForSelected}
                >
                  {state.selectedModelId &&
                  state.favoriteModelIds.includes(state.selectedModelId)
                    ? "★"
                    : "☆"}
                </button>
                <button
                  className="btn btn--small"
                  aria-pressed={state.favoritesOnly}
                  disabled={state.favoriteModelIds.length === 0}
                  title={
                    state.favoriteModelIds.length === 0
                      ? "No favorites yet — star a model first"
                      : state.favoritesOnly
                        ? "Showing favorites only — click for all models"
                        : "Show only favorite models"
                  }
                  onClick={() => setFavoritesOnly(!state.favoritesOnly)}
                >
                  {state.favoritesOnly ? "★ all" : "★ only"}
                </button>
              </div>
            )}
            {/* Model discovery is metadata: on failure the selector stays
                usable with Engine Default and Send keeps working (§22–§23). */}
            {state.modelsError && (
              <span
                className="titlebar__model-warning muted"
                title={state.modelsError}
              >
                Models unavailable
              </span>
            )}
          </>
        ) : (
          <span className="value muted" title="This engine does not support model selection">
            engine-controlled
          </span>
        )}
      </div>

      <div className="titlebar__controls">
        <span className={`status-dot status-dot--${health}`} title={`engine health: ${health}`} />
        <span className="titlebar__health muted">{health}</span>
        {starting && (
          <button className="btn" disabled title="Engine is starting…">
            Starting…
          </button>
        )}
        {!starting && stopping && (
          <button className="btn" disabled title="Engine is stopping…">
            Stopping…
          </button>
        )}
        {!starting && !stopping && engine && !ready && (
          <button
            className="btn"
            onClick={startEngine}
            disabled={!canStart}
            title={
              !allowed
                ? "Application is not ready"
                : state.currentWorkspaceId
                  ? "Start the selected engine runtime"
                  : "Open a project first — the engine needs a workspace"
            }
          >
            Start engine
          </button>
        )}
{!starting && !stopping && engine && ready && (
          <button
            className="btn btn--danger"
            onClick={stopEngine}
            disabled={!canStop}
            title={
              !allowed
                ? "Application is not ready"
                : "Stop the engine runtime (not the active run)"
            }
          >
            Stop engine
          </button>
        )}
        <button
          className="btn btn--small"
          onClick={() => runPresetImport(onError).catch(() => {})}
          disabled={!mutationsAllowed(store.getState())}
          title="Import a settings preset (.json or .zip)"
        >
          Import preset
        </button>
      </div>
    </header>
  );
}

// Selection is ephemeral UI state; keep it in the store as UI state so the
// whole title bar stays consistent. Engine/model selection is delegated to
// the app catalog (single owner, generation-guarded) so two panels can never
// disagree about the chosen engine.
import {
  markEngineStarting,
  setFavoritesOnly,
  store,
  toggleFavoriteModel,
  favoritesGen,
  nextFavoritesGen,
} from "../state/store";

/** Provider-visible option label: display name + the provider display name
 * (raw key as fallback). The user's core complaint was a flat list where
 * 96% of models are paywalled — the provider attribution is the filter. */
export function modelLabel(m: ModelInfo): string {
  const provider = m.provider_name ?? m.provider;
  return provider ? `${m.display_name} — ${provider}` : m.display_name;
}

/**
 * Stop an engine and settle the UI. The optimistic stop latch is cleared
 * SYNCHRONOUSLY once the command resolves Ok — independent of the
 * `engine.stopped` event, which `refreshEngines` is documented to recover when
 * the stream misses or delays it (W2-04). Without this, a lost terminal event
 * leaves `stoppingEngines[id]` set forever and Start unreachable until a reload
 * (a panel reload does not repair the global store latch). On success we also
 * project the authoritative Stopped terminal, THEN reconcile via the idempotent
 * `listEngines` pull. The failure branch keeps clearing the latch on rejection;
 * `refreshEngines` itself never clears a stop latch, so an in-progress stop is
 * never blindly cleared from an arbitrary snapshot (preserves the event-driven
 * and failure paths).
 */
export async function runStopEngine(
  id: string,
  onError: (message: string) => void,
): Promise<void> {
  // Optimistic stopping projection (mirror of `engine.stopping`): instant
  // feedback; cleared by the authoritative terminal or on failure.
  store.patch((s) => ({
    ...s,
    stoppingEngines: { ...s.stoppingEngines, [id]: true },
  }));
  try {
    await commands.stopEngine(id);
    // W2-04: settle the latch NOW — the command itself proved the stop was
    // accepted. This must not wait for the event stream.
    store.patch((s) => {
      const stoppingEngines = { ...s.stoppingEngines };
      delete stoppingEngines[id];
      return {
        ...s,
        stoppingEngines,
        engines: s.engines.map((e) => (e.id === id ? { ...e, health: "stopped" as const } : e)),
      };
    });
    await refreshEngines();
  } catch (e) {
    store.patch((s) => {
      const stoppingEngines = { ...s.stoppingEngines };
      delete stoppingEngines[id];
      return { ...s, stoppingEngines };
    });
    onError(String(e));
  }
}

/** Authoritative engine-health pull after a start/stop invoke resolves:
 * guarantees the UI reflects engine state even when the event stream
 * misses or delays the terminal (the F5 report — a manual reload must
 * never be required to see Start→Ready). Idempotent when events arrive
 * normally. */
async function refreshEngines(): Promise<void> {
  try {
    const engines = await commands.listEngines();
    store.patch((s) => ({ ...s, engines }));
  } catch (e) {
    // Non-fatal: the event stream remains the live path; the next terminal
    // event or a later pull reconciles the truth.
    store.patch((s) => ({ ...s, lastError: `engine state refresh failed: ${String(e)}` }));
  }
}
