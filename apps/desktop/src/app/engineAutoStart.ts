import { healthKind } from "@saiwork2/contracts";
import { commands } from "./backend";
import { store } from "../state/store";

/** Latest-intent engine activation owner. Workspace and engine selection can
 * happen faster than stop/start IPC completes; serializing requests and
 * checking a generation between calls guarantees the newest selection gets
 * the final binding. */
let generation = 0;

interface StartIntent {
  generation: number;
  engineId: string | null;
  workspaceId: string | null;
  onError: (message: string) => void;
  settle: () => void;
}

// Bounded coalescing: at most one executing intent plus one latest pending
// intent. Repeated selection clicks replace (and settle) the pending request
// instead of growing an unbounded promise chain.
let running = false;
let pending: StartIntent | null = null;

export function requestEngineAutoStart(
  engineId: string | null,
  workspaceId: string | null,
  onError: (message: string) => void = () => undefined,
): Promise<void> {
  const myGeneration = ++generation;
  const done = new Promise<void>((settle) => {
    pending?.settle();
    pending = { generation: myGeneration, engineId, workspaceId, onError, settle };
  });
  if (!running) {
    running = true;
    void drainIntents();
  }
  return done;
}

async function drainIntents(): Promise<void> {
  while (pending) {
    const intent = pending;
    pending = null;
    try {
      await applyIntent(intent);
    } catch (error) {
      if (intent.generation === generation) {
        intent.onError(`Engine auto-start failed: ${String(error)}`);
      }
    } finally {
      intent.settle();
    }
  }
  running = false;
  // No await exists between the empty check and `running = false`, so a
  // request cannot interleave there. The defensive branch keeps this helper
  // correct if that implementation detail changes later.
  if (pending && !running) {
    running = true;
    void drainIntents();
  }
}

/** AUDIT-W2-005: bounded wait for the in-flight start to reach a terminal
 * lifecycle state (anything but `starting`), driven by the store's
 * authoritative engine events. Resolves false on the safety timeout so a
 * wedged runtime cannot park an intent forever. */
function waitForEngineTerminal(engineId: string, timeoutMs = 60_000): Promise<boolean> {
  const terminalNow = (): boolean => {
    const engine = store.getState().engines.find((item) => item.id === engineId);
    return Boolean(engine && healthKind(engine.health) !== "starting");
  };
  return new Promise((resolve) => {
    let done = false;
    let unsub: (() => void) | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const finish = (value: boolean) => {
      if (done) return;
      done = true;
      unsub?.();
      if (timer) clearTimeout(timer);
      resolve(value);
    };
    if (terminalNow()) {
      finish(true);
      return;
    }
    unsub = store.subscribe(() => {
      if (terminalNow()) finish(true);
    });
    timer = setTimeout(() => finish(false), timeoutMs);
  });
}

async function applyIntent(intent: StartIntent): Promise<void> {
  const { generation: myGeneration, engineId, workspaceId } = intent;
  if (myGeneration !== generation || !engineId || !workspaceId) return;

  // AUDIT-W2-005: one intent owns its lifecycle to satisfaction — including
  // re-evaluation after a foreign in-flight start settles. Bounded rounds:
  // stop→re-read→start and start-wait→re-read are each one round.
  for (let round = 0; round < 8; round++) {
    // Read the runtime after every older intent has settled. Store events may
    // lag behind command completion; this authoritative read prevents a stale
    // READY/STOPPED projection from producing a duplicate stop/start.
    const engines = await commands.listEngines();
    if (myGeneration !== generation) return;
    const engine = engines.find((item) => item.id === engineId);
    if (!engine) throw new Error(`Engine ${engineId} is not available`);

    const kind = healthKind(engine.health);
    // A READY unbound runtime is intentionally workspace-agnostic (the send
    // gate accepts it for every project), so it must not be restarted merely
    // because bound_workspace_id is null.
    if (kind === "ready" && (engine.bound_workspace_id == null || engine.bound_workspace_id === workspaceId)) {
      return;
    }

    if (kind === "ready" || kind === "degraded") {
      // A live runtime bound elsewhere must stop before rebinding. Degraded
      // is also live: EngineRegistry cannot safely start over it.
      await commands.stopEngine(engineId);
      if (myGeneration !== generation) return;
      continue; // re-read authoritatively before starting
    }

    if (kind === "starting") {
      // A start (manual or automatic) already owns the lifecycle. NEVER race
      // it with a second start command — but this intent must also NOT treat
      // itself as satisfied while its desired binding is still undecided: it
      // waits for the terminal transition and then re-evaluates (the old code
      // returned here, letting Start(A)-then-select(B) end bound to A while
      // selected on B).
      const settled = await waitForEngineTerminal(engineId);
      if (myGeneration !== generation) return;
      if (!settled) {
        throw new Error(`Engine ${engineId} stayed 'starting' too long`);
      }
      continue; // re-read: bind now if the settled runtime does not match
    }

    // stopped/failed/unknown: safe to start for the desired binding.
    await commands.startEngine(engineId, workspaceId);
    return;
  }
  throw new Error(`Engine ${engineId} activation did not settle`);
}

/** Test-only scheduler reset; callers await their requests before using it. */
export function resetEngineAutoStartForTest(): void {
  generation += 1;
  pending?.settle();
  pending = null;
  running = false;
}

/** T-125: true when the selected engine is in a state a `start` can leave
 * (never started / stopped / failed / unknown), so the user-facing send and
 * queue paths can auto-start it without demanding a manual engine toggle.
 * A READY runtime (already bound or workspace-agnostic), a bound-degraded one,
 * or one mid-`starting` is NOT this helper's job: the former needs no start,
 * the latter must not be raced, and degraded must be stopped before restart
 * (all owned by `requestEngineAutoStart` / the Engine toggle, not here). */
export function engineCanBeAutoStarted(engineId: string | null): boolean {
  if (!engineId) return false;
  const engine = store.getState().engines.find((item) => item.id === engineId);
  if (!engine) return false;
  const kind = healthKind(engine.health);
  return kind === "stopped" || kind === "failed" || kind === "unknown";
}

/** T-125: true when the engine has reached any terminal (non-`starting`) state.
 * Subscribe-driven, NOT a poll: one watcher on the authoritative store + a
 * bounded safety timeout. */

function waitUntilNotStartable(engineId: string, timeoutMs = 30_000): Promise<boolean> {
  const notStartableNow = (): boolean => !engineCanBeAutoStarted(engineId);
  return new Promise((resolve) => {
    if (notStartableNow()) {
      resolve(true);
      return;
    }
    let unsub: (() => void) | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const finish = (value: boolean) => {
      unsub?.();
      if (timer) clearTimeout(timer);
      resolve(value);
    };
    unsub = store.subscribe(() => {
      if (notStartableNow()) finish(true);
    });
    timer = setTimeout(() => finish(false), timeoutMs);
  });
}

/** T-125: auto-start the selected engine (latest-wins scheduler for project
 *  select + queue), then wait for it to leave the startable state so the
 *  caller's follow-up command (send / enqueue) can proceed. Bounded wait — a
 *  wedged start resolves false instead of parking the caller. Returns false
 *  when the engine could not be started or stayed startable too long. */
export async function ensureEngineStarted(
  engineId: string | null,
  workspaceId: string | null,
  onError: (message: string) => void = () => undefined,
  timeoutMs = 30_000,
): Promise<boolean> {
  if (!engineCanBeAutoStarted(engineId)) return true; // already terminal-safe
  await requestEngineAutoStart(engineId, workspaceId, onError);
  return waitUntilNotStartable(engineId ?? "", timeoutMs);
}
