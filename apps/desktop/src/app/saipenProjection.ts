// THE SAIPEN projection owner (T-049).
//
// The SAIPEN strip previously kept a SECOND local snapshot of the project's
// SAIPEN state, fetched reactively by the component. That duplicated the
// canonical store projection, and its in-flight guard (a single global boolean
// per authority) DROPPED a refresh that arrived during an in-flight request
// with no dirty follow-up — so a `saipen.revision` bump that landed while a
// read was pending silently lost the update.
//
// This module is the one owner of `store.saipen` / `store.saipenStale`. It is
// single-flight with a dirty follow-up, and is correlated by a monotonically
// increasing generation so a slow read for workspace A that is still in flight
// when the user switches to B is discarded (B's refresh owns the projection)
// instead of clobbering B with A's data. A failed read marks the canonical
// state STALE (fail-closed) — it never overwrites with fabricated absence.

import { commands } from "./backend";
import { store } from "../state/store";

let inFlight = false;
let dirty = false;
let generation = 0;

/** Request a fresh SAIPEN projection for the current workspace. Burst-safe: a
 * burst collapses into the running pass + at most one follow-up. */
export function requestSaipenRefresh(): void {
  const wsId = store.getState().currentWorkspaceId;
  if (!wsId) {
    // W2-006: invalidate the generation BEFORE clearing the projection so a
    // still-in-flight read for the just-closed workspace is revoked. With the
    // earlier `return` before the bump, A's pending read kept
    // `myGen === generation` and could repopulate a supposedly-empty scope.
    // Also cancel any pending dirty refresh that no longer has a target.
    generation++;
    dirty = false;
    if (store.getState().saipenStale || store.getState().saipen !== null) {
      store.patch((s) => ({ ...s, saipen: null, saipenStale: false }));
    }
    return;
  }
  // Bump the generation so any older in-flight read (another workspace, or a
  // superseded refresh) is discarded when it resolves.
  const myGen = ++generation;
  if (inFlight) {
    dirty = true;
    return;
  }
  void run(myGen, wsId);
}

function run(myGen: number, wsId: string): void {
  inFlight = true;
  commands
    .getSaipen(wsId)
    .then(
      (snap) => {
        // Superseded by a newer refresh or a workspace switch: drop this result.
        if (myGen !== generation) return;
        // W2-006: derive freshness from the snapshot's authoritative stale flag
        // rather than unconditionally marking every resolved snapshot fresh.
        store.patch((s) => ({ ...s, saipen: snap, saipenStale: Boolean(snap?.stale) }));
      },
      () => {
        if (myGen !== generation) return;
        // Fail-closed: a failed read marks the canonical projection STALE
        // rather than fabricating absence.
        store.patch((s) => ({ ...s, saipenStale: true }));
      },
    )
    .finally(() => {
      inFlight = false;
      if (dirty) {
        dirty = false;
        requestSaipenRefresh();
      }
    });
}

/** Test-only reset of the SAIPEN projection owner lifecycle. */
export function resetSaipenProjectionForTest(): void {
  inFlight = false;
  dirty = false;
  generation = 0;
}
