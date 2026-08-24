// THE frontend lifecycle owner (T-033).
//
// Two problems this module fixes.
//
// 1. StrictMode-safe subscription. App's effect used a one-shot `booted` ref:
//    React's development StrictMode intentionally runs setup → cleanup → setup,
//    so the first cleanup disposed the event subscription and the second setup
//    returned early because `booted` was already true. Development builds could
//    end with ZERO live event subscription (a dead UI that only updates on
//    reload). The fix is NOT to weaken StrictMode: the effect must be
//    setup/cleanup/setup safe, and the expensive part (cold bootstrap) is
//    idempotent by its own single-flight, not by blocking React from
//    resubscribing.
//
// 2. Cold bootstrap ≠ reconciliation. `frontend.reconcile` (the backend telling
//    the UI its event stream lagged) used to re-run the FULL bootstrap:
//    app info + workspace registry + engine registry + workspace OPEN (SAIPEN
//    watcher re-attach) + session work + model discovery. A lag burst could
//    therefore restart lifecycle-owning work dozens of times. Reconciliation
//    now re-snapshots only event-backed projections, is single-flight, and
//    collapses a burst into exactly one follow-up pass.

import { commands, subscribeToCoreEvents } from "./backend";
import {
  store,
  type AppState,
  clearActiveTail,
  markStreamGap,
  reconcilePendingPermissions,
  reconcilePendingQuestions,
  setFavorites,
  favoritesGen,
} from "../state/store";
import { currentSelectionEpoch, isCurrentEpoch, selectWorkspace } from "./workspaceSelection";
import { installModelCatalog, restoreEngineState } from "./modelCatalog";
import { installQueueSync, requestQueueSnapshot } from "./queueSync";
import { activateSession, loadSessionHistory } from "./sessionSelection";
import { loadUiLayout } from "../components/dock/persistence";

export interface FrontendSession {
  /** Resolves when the event subscription is installed (or its failure was
   * surfaced) — never a pending-forever promise. */
  ready: Promise<void>;
  dispose: () => void;
}

type Subscriber = (onReconcile: (reason?: "lag") => void) => {
  ready: Promise<void>;
  dispose: () => void;
};

/**
 * Start one frontend session: exactly ONE live event subscription plus the
 * store reactions that own model-catalog and queue synchronization.
 *
 * Safe under setup → cleanup → setup: each call creates its own subscription
 * and its disposer detaches exactly that one; cold bootstrap is shared and runs
 * once per app lifetime.
 */
export function startFrontendSession(
  onError: (message: string) => void,
  subscribe: Subscriber = subscribeToCoreEvents,
): FrontendSession {
  installLagGapRecovery();
  const subscription = subscribe((reason) => requestReconcile(reason));
  const disposers: Array<() => void> = [subscription.dispose];
  // Guards against a dispose that lands BEFORE the subscription is ready: the
  // `then` continuation installs the model/queue controllers, and without this
  // flag they would be registered into a `disposers` array that has already
  // been torn down — leaving live controllers with no owner (T-044).
  let disposed = false;

  const ready = subscription.ready.then(
    () => {
      const d1 = installModelCatalog();
      const d2 = installQueueSync(onError);
      void coldBootstrap(onError);
      void loadUiLayout();
      if (disposed) {
        // Session disposed before ready resolved: tear the just-installed
        // controllers down immediately so exactly zero controllers leak.
        try {
          d1();
        } catch {
          /* ignore */
        }
        try {
          d2();
        } catch {
          /* ignore */
        }
        return;
      }
      disposers.push(d1, d2);
    },
    (e) => {
      store.patch((s) => ({
        ...s,
        lastError: `backend event subscription failed: ${String(e)}`,
      }));
    },
  );

  return {
    ready,
    dispose: () => {
      if (disposed) return;
      disposed = true;
      // Dispose in reverse order; a disposer must never throw past this point.
      for (const d of disposers.reverse()) {
        try {
          d();
        } catch {
          /* a failed detach must not leak the remaining ones */
        }
      }
      disposers.length = 0;
    },
  };
}

// ---- cold bootstrap (once per app lifetime) ----

let coldBootstrapPromise: Promise<void> | null = null;

/**
 * Cold bootstrap: the registries + selection restore + watcher-establishing
 * work that may happen only ONCE per app bootstrap lifecycle. Idempotent by
 * single-flight, so a StrictMode double setup (or a reconcile arriving during
 * startup) shares the same pass instead of duplicating lifecycle effects.
 */
export function coldBootstrap(onError: (message: string) => void): Promise<void> {
  if (coldBootstrapPromise) return coldBootstrapPromise;
  coldBootstrapPromise = runColdBootstrap(onError).finally(() => {
    // Keep the settled promise: a later request is a RECONCILE, not a second
    // cold bootstrap (that is what re-attached watchers repeatedly).
  });
  return coldBootstrapPromise;
}

async function runColdBootstrap(onError: (message: string) => void): Promise<void> {
  try {
    const info = await commands.appInfo();
    store.patch((s) => ({
      ...s,
      backend: "connected",
      lifecycle: info.lifecycle,
      version: info.version,
    }));

    // Registries: loaded here, NOT on every reconcile.
    const [workspaces, engines] = await Promise.all([
      commands.listWorkspaces(),
      commands.listEngines(),
    ]);
    store.patch((s) => ({
      ...s,
      workspaces,
      engines,
    }));

    // Restore engine/model BEFORE restoring the active project, so project
    // selection auto-starts the user's actual engine rather than a temporary
    // default. Fall back deterministically only when no valid preference exists.
    try {
      await restoreEngineState();
    } catch {
      // Non-fatal: persistence failure should not break bootstrap.
    }
    store.patch((s) => ({
      ...s,
      selectedEngineId: s.selectedEngineId ?? engines[0]?.id ?? null,
    }));

    // Restore the previously current project through THE canonical selection
    // authority (authoritative open + SAIPEN attach + scoped sessions/SAIPEN).
    //
    // CORE-002: three materially different states must NOT collapse into
    // "open the most recent project":
    //   1. authoritative active id present & valid  -> restore EXACTLY that id;
    //   2. authoritative read returned null (explicitly closed / first run)
    //      -> NO project is current; leave the scoped projection empty (an
    //      explicit close must survive restart, never fall back to recency);
    //   3. active-workspace read FAILED              -> surface the failure and
    //      select nothing (do not fabricate a recency selection);
    //   4. stored id absent from the registry         -> stale durable state:
    //      repair the pointer (clear it) and select nothing, never an unrelated
    //      project.
    let activeReadOk = true;
    let exactActiveId: string | null = null;
    try {
      exactActiveId = await commands.getActiveWorkspace();
    } catch (e) {
      activeReadOk = false;
    }
    if (activeReadOk) {
      if (exactActiveId == null) {
        // State 2: explicitly closed / first run. Leave the scoped projection
        // empty — no recency fallback (CORE-002).
      } else {
        const matched = workspaces.find((w) => w.id === exactActiveId);
        if (matched) {
          // State 1: restore exactly the known active id.
          await selectWorkspace(matched.path, onError);
        } else {
          // State 4: stale/invalid durable id. Repair the pointer through the
          // active-workspace authority and select nothing (CORE-002). The
          // current selection epoch rides along so the repair is epoch-owned:
          // a newer user selection that committed while this read was in flight
          // supersedes the repair instead of being clobbered (CORE-001).
          await commands.setActiveWorkspace(null, currentSelectionEpoch()).catch(() => undefined);
        }
      }
    } else {
      // State 3: read error — surface failure, fabricate no selection.
      onError("could not read the previously active workspace; no project restored");
    }

    // W2-005: favorites are durable app state, not workspace-scoped. A LATE
    // bootstrap read must NOT clobber a newer optimistic toggle the user made
    // while this read was in flight. Snapshot the shared favorite-mutation
    // generation (lifted into the store module, formerly local to TitleBar) and
    // only apply the backend value if no newer write landed meanwhile.
    const favGen = favoritesGen();
    const favorites = await commands.getModelFavorites().catch(() => undefined);
    if (favorites !== undefined && favoritesGen() === favGen) {
      setFavorites(favorites);
    }

    // Route the post-restore reconciliation through the single-flight
    // scheduler (never reconcileFrontend directly) so a bootstrap + an
    // in-flight event reconcile collapse into one pass (T-044).
    await requestReconcile();
  } catch (e) {
    store.patch((s) => ({ ...s, lastError: String(e) }));
  }
}

// ---- reconciliation (repeatable, bounded) ----

let reconcileInFlight = false;
let reconcileDirty = false;
// The currently-scheduled pass (or its follow-up chain) — lets a caller await
// the full single-flight + dirty-follow-up completion.
let reconcileDone: Promise<void> = Promise.resolve();

/// One-time subscriber (W2-003): when a session carrying a stream-gap marker
/// transitions to non-running (its matching terminal arrived), discard the
/// stale tail and authoritatively rehydrate from the engine — instead of
/// finalizing a knowingly incomplete tail as complete.
let lagGapRecoveryInstalled = false;
function installLagGapRecovery(): void {
  if (lagGapRecoveryInstalled) return;
  lagGapRecoveryInstalled = true;
  store.subscribe(() => {
    const s = store.getState();
    for (const sid of Object.keys(s.streamGaps)) {
      if (s.streamGaps[sid] && !s.running[sid]) {
        markStreamGap(sid, false);
        // W2-003: only drop the locally-observed tail if the authoritative
        // history reload actually succeeds (status "available"). With no
        // history authority the live tail is all the user has — keep it.
        const hadTail = store.getState().activeMessage[sid] !== undefined;
        void loadSessionHistory(sid)
          .then(() => {
            if (hadTail && store.getState().historyStatus[sid] === "available") {
              clearActiveTail(sid);
            }
          })
          .catch(() => undefined);
      }
    }
  });
}

/**
 * THE single reconciliation scheduler (T-044): every reconciliation — bootstrap
 * and event-driven — runs through here. A burst of lag notifications collapses
 * into the running pass + AT MOST ONE follow-up pass (never 50 full
 * re-snapshots). `reconcileFrontend` is the one private pass and is never
 * invoked outside this gate (cold bootstrap routes through it too).
 */
export function requestReconcile(reason?: "lag"): Promise<void> {
  if (reconcileInFlight) {
    reconcileDirty = true;
    return reconcileDone;
  }
  reconcileInFlight = true;

  const runner = async () => {
    try {
      do {
        reconcileDirty = false;
        await reconcileFrontend(reason).catch(() => undefined);
        // The lag cause applies only to the FIRST pass; follow-up passes from
        // a reconcile burst are ordinary (no repeated history re-reads).
        reason = undefined;
      } while (reconcileDirty);
    } finally {
      reconcileInFlight = false;
    }
  };

  reconcileDone = runner();
  return reconcileDone;
}

/**
 * Re-derive every EVENT-BACKED projection from its authority. Deliberately does
 * NOT reopen the workspace, re-attach watchers or reload the workspace/engine
 * registries' lifecycle work — those are cold-bootstrap concerns. Model
 * discovery is not triggered here either: the catalog owner reacts to engine
 * generation changes and is a no-op when nothing changed. Reachable ONLY
 * through `requestReconcile`'s single-flight scheduler (T-044).
 *
 * Fail-closed per sub-snapshot: a FAILED read never overwrites a projection
 * with fabricated absence — it preserves the previous truth and marks it stale
 * (queue read-only, SAIPEN stale, run ownership UNKNOWN which disables
 * Send/Cancel).
 */
export async function reconcileFrontend(reason?: "lag"): Promise<void> {
  try {
    const epoch = currentSelectionEpoch();
    const workspaceId = store.getState().currentWorkspaceId;

    const [info, engines, sessions, activeRuns, saipenSnap] = await Promise.all([
      commands.appInfo().catch(() => undefined),
      commands.listEngines().catch(() => undefined),
      workspaceId
        ? commands.listSessions(workspaceId).catch(() => undefined)
        : Promise.resolve<AppState["sessions"] | undefined>([]),
      commands.activeRuns().catch(() => undefined),
      workspaceId ? commands.getSaipen(workspaceId).catch(() => undefined) : Promise.resolve(null),
    ]);

    // The queue projection has its own owner (single-flight + dirty follow-up).
    requestQueueSnapshot();

    // Captured so the active-session transition is routed through the single
    // owner AFTER the patch (T-046) — never set inline here.
    let nextActiveSessionId: string | null = null;
    let applyActiveSession = false;

    store.patch((s) => {
      // Workspace-epoch guard: a selection that happened while these reads were
      // in flight owns the UI now; scoped results from the older epoch are
      // discarded instead of contaminating the newer workspace.
      const scopedValid = isCurrentEpoch(epoch) && s.currentWorkspaceId === workspaceId;
      let next: AppState = s;
      if (info) {
        next = { ...next, backend: "connected", lifecycle: info.lifecycle, version: info.version };
      }
      if (engines) next = { ...next, engines };
      if (scopedValid && sessions) {
        const activeSessionId =
          next.activeSessionId && sessions.some((x) => x.id === next.activeSessionId)
            ? next.activeSessionId
            : sessions[0]?.id ?? null;
        next = { ...next, sessions };
        // The active-session transition is owned by sessionSelection
        // (activateSession) so history hydrates exactly once per transition,
        // not on every reconcile pass (T-046).
        nextActiveSessionId = activeSessionId;
        applyActiveSession = true;
      }
      if (activeRuns) {
        // Exact ownership rebuild from the authority: every known session is
        // reset, then only really-owned runs are set.
        const running: Record<string, string | null> = { ...next.running };
        for (const session of next.sessions) running[session.id] = null;
        for (const [sid, rid] of activeRuns) running[sid] = rid;
        next = { ...next, running, runningStale: false };
      } else {
        // Preserve uncertainty (never fabricate idle).
        next = { ...next, runningStale: true };
      }
      if (scopedValid) {
        next =
          saipenSnap !== undefined
            ? { ...next, saipen: saipenSnap ?? null, saipenStale: Boolean(saipenSnap?.stale) }
            : { ...next, saipenStale: true };
      }
      return next;
    });
    // Route the active-session choice through the single owner (T-046) so its
    // authoritative history hydrates exactly once per transition.
    if (applyActiveSession) activateSession(nextActiveSessionId);

    // W2-003 / W2-004: a bounded-bus lag is a NORMAL recovery condition — the
    // event-backed projections may have missed State events. Reconstruct the
    // transcript (W2-003) and the open permission cards (W2-004) from their
    // authorities. An ordinary (non-lag) reconcile never triggers this, so
    // history is not re-read on every reconcile pass.
    if (reason === "lag") {
      await reconcileAfterLag().catch(() => undefined);
    }
  } catch (e) {
    store.patch((s) => ({ ...s, lastError: String(e) }));
  }
}

/**
 * Recover from a bounded-bus `Lagged` (W2-003 + W2-004). Runs only when the
 * reconcile was triggered by lag.
 *
 * - W2-004: rebuild the open permission cards from the authoritative pending
 *   snapshot, so a missed `permission.requested` is recoverable and the user
 *   can still resolve the upstream wait exactly once.
 * - W2-003: for the active session proven NON-RUNNING after the gap, clear any
 *   stale live tail and reload the authoritative engine history (covers both
 *   lag cases: skipped deltas before a delivered terminal, and a skipped
 *   terminal while `active_runs` reports no live run). For a run STILL active,
 *   mark a stream-gap so rehydration happens on its matching terminal instead
 *   of finalizing a knowingly incomplete tail.
 */
async function reconcileAfterLag(): Promise<void> {
  // W2-004: rebuild pending permission cards from the authoritative snapshot.
  // A SUCCESSFUL snapshot (including an EMPTY one) is the exact set of open
  // requests — reconcile even when empty, so stale unresolved cards are
  // cleared. A FAILED read is left undefined: do NOT fabricate emptiness,
  // preserve the prior cards (CORE-004).
  const pending = await commands.pendingPermissions().catch(() => undefined);
  if (pending !== undefined) reconcilePendingPermissions(pending);

  // AUDIT-CORE-002: same exact-set reconciliation for pending user questions.
  // A failed read preserves the prior cards; a successful (even empty)
  // snapshot is the authoritative open set.
  const pendingQ = await commands.pendingQuestions().catch(() => undefined);
  if (pendingQ !== undefined) reconcilePendingQuestions(pendingQ);

  // W2-003: transcript gap recovery for the active session.
  const wsId = store.getState().currentWorkspaceId;
  const sid = store.getState().activeSessionId;
  if (!wsId || !sid) return;
  // CORE-004: carry explicit success/error. A failed active-runs read means
  // liveness is UNKNOWN — do NOT destroy a valid live tail and do NOT reload
  // history as if the run were over. Only a successful snapshot that proves
  // the session is not running may clear/rehydrate.
  const activeRuns = await commands.activeRuns().catch(() => undefined);
  if (activeRuns === undefined) return;
  const stillRunning = activeRuns.some(([r]) => r === sid);
  if (stillRunning) {
    markStreamGap(sid, true);
  } else {
    // W2-003: only discard the locally-observed tail when an authoritative
    // history read will REPLACE it. `loadSessionHistory` sets historyStatus to
    // "available" on a successful authoritative read and "unavailable" when the
    // engine has no history authority (or "error" on read failure). With no
    // history authority we MUST keep the live tail — discarding it would erase
    // a real (possibly partial) turn and leave a blank thread. The in-flight
    // tail is only cleared once an authoritative baseline has superseded it.
    const hadTail =
      store.getState().activeMessage[sid] !== undefined ||
      store.getState().streamGaps[sid] === true;
    await loadSessionHistory(sid).catch(() => undefined);
    if (hadTail && store.getState().historyStatus[sid] === "available") {
      clearActiveTail(sid);
    }
  }
}

/** Test-only: forget the cold-bootstrap latch and reconcile lifecycle. */
export function resetFrontendSyncForTest(): void {
  coldBootstrapPromise = null;
  reconcileInFlight = false;
  reconcileDirty = false;
  reconcileDone = Promise.resolve();
}
