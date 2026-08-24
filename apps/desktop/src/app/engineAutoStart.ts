import { healthKind } from "@saiwork2/contracts";
import { commands } from "./backend";

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

async function applyIntent(intent: StartIntent): Promise<void> {
  const { generation: myGeneration, engineId, workspaceId } = intent;
  if (myGeneration !== generation || !engineId || !workspaceId) return;

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

  // A live runtime bound elsewhere must stop before rebinding. Degraded is
  // also live: EngineRegistry cannot safely start over it.
  if (kind === "ready" || kind === "degraded") {
    await commands.stopEngine(engineId);
    if (myGeneration !== generation) return;
  } else if (kind === "starting") {
    // An explicit start already owns the lifecycle. Its terminal event will
    // expose the binding; never race it with a second start command.
    return;
  }

  await commands.startEngine(engineId, workspaceId);
}

/** Test-only scheduler reset; callers await their requests before using it. */
export function resetEngineAutoStartForTest(): void {
  generation += 1;
  pending?.settle();
  pending = null;
  running = false;
}
