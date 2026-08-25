// THE queue-projection owner (T-032).
//
// Queue synchronization used to live in module-level mutable state INSIDE
// QueuePanel (`snapshotInFlight` / `snapshotDirty` / `snapshotGeneration`),
// which silently assumed exactly one permanently mounted panel. With the dock
// the panel mounts/unmounts on tab switches, so hiding the Queue tab would have
// stopped queue synchronization entirely — the projection would go silently
// stale while the durable queue kept moving.
//
// This module owns the authoritative snapshot lifecycle independently of any
// component:
//   * single-flight: one snapshot read at a time;
//   * dirty follow-up: a burst of committed transitions during a read triggers
//     EXACTLY ONE follow-up read, never a per-revision request storm;
//   * generation invalidation: a slower older response can never replace newer
//     snapshot truth;
//   * fail-closed staleness: a failed read keeps the previous items but marks
//     the projection stale (read-only) until a fresh read succeeds;
//   * the "close Queue tab when done" transition, which is a QUEUE fact
//     (non-empty → empty) and therefore belongs to the queue owner, not to a
//     component's render.

import { commands } from "./backend";
import { store } from "../state/store";

let inFlight = false;
let dirty = false;
let generation = 0;
let pendingOffset: number | null = null;
let onErrorSink: ((message: string) => void) | null = null;

// AUDIT-W2-006: controller/lifecycle OWNERSHIP epoch. Every installed owner
// (and its async operations) captures the epoch at entry; the disposer bumps
// it, so completions from a disposed controller perform NO store/error/
// follow-up mutation and can never commit stale queue truth into a newly
// mounted session. The disposer also retires the dead owner's single-flight
// bookkeeping, so a new installation is never blocked by an in-flight read
// that belongs to a dead controller.
let epoch = 0;

// PERF-016: single-flight + dirty coalescing for incremental item patches.
// At most one item read is in flight; additional invalidations during that
// read accumulate and either extend the patch or collapse to a full snapshot.
let patchInFlight = false;
let patchDirty = false;
let patchGeneration = 0;
let pendingPatchIds: Set<string> = new Set();

/** Request an authoritative queue snapshot. Safe to call from anywhere and as
 * often as events arrive: the single-flight + dirty rules bound the work. */
export function requestQueueSnapshot(
  activeOffset = store.getState().queue.activeOffset,
): void {
  if (inFlight) {
    dirty = true;
    pendingOffset = activeOffset;
    return;
  }
  void loadSnapshot(activeOffset);
}

/** Replace the bounded active page. The backend normalizes stale/huge
 * offsets and remains the sole source of exact totals. */
export function requestQueuePage(activeOffset: number): void {
  requestQueueSnapshot(Math.max(0, Math.trunc(activeOffset)));
}

async function loadSnapshot(activeOffset: number): Promise<void> {
  // AUDIT-W2-006: ownership captured at entry — a disposed controller's
  // completion is an inert no-op (no store patch, no error sink, no flags).
  const ownerEpoch = epoch;
  inFlight = true;
  const gen = ++generation;
  try {
    const snap = await commands.queueSnapshot(activeOffset);
    if (gen !== generation || ownerEpoch !== epoch) return; // superseded or disposed
    store.patch((s) => {
      const activeBefore = countActive(s.queue.counts);
      const activeAfter = countActive(snap.counts);
      const next = {
        ...s,
        queue: {
          status: snap.status,
          paused: snap.paused,
          items: snap.items,
          counts: snap.counts,
          activeOffset: snap.active_offset,
          activeLimit: snap.active_limit,
          queuedBefore: snap.queued_before,
          activeHasMore: snap.active_has_more,
          revision: s.queue.revision,
          stale: false,
          payloadPreview: snap.payload_preview !== false,
          lastChangedId: undefined,
        },
      };
      // "Close tab when done" reacts to the TRANSITION only (non-empty →
      // empty), never to a steady empty queue — otherwise every startup
      // snapshot would close the tab the user just opened. It also never
      // touches anything but the dock tab: no app/session/engine/run effect.
      const emptied = activeBefore > 0 && activeAfter === 0;
      if (emptied && s.closeQueueWhenDone && s.activeDockTab === "queue" && !s.queue.stale) {
        return { ...next, activeDockTab: "activity" as const };
      }
      return next;
    });
  } catch (e) {
    if (gen !== generation || ownerEpoch !== epoch) return; // dead owner: silent
    onErrorSink?.(
      `queue snapshot failed: ${String(e)} — the shown queue is stale and read-only until it recovers`,
    );
    store.patch((s) => ({ ...s, queue: { ...s.queue, stale: true } }));
  } finally {
    // Only the LATEST in-flight read of the LIVE owner manages the shared
    // bookkeeping — never a superseded/dead completion racing a new one.
    if (gen === generation && ownerEpoch === epoch) {
      inFlight = false;
      if (dirty) {
        dirty = false;
        const nextOffset = pendingOffset ?? store.getState().queue.activeOffset;
        pendingOffset = null;
        // Exactly ONE follow-up for everything that arrived while reading.
        void loadSnapshot(nextOffset);
      }
    }
  }
}

function countActive(counts: {
  queued: number;
  leased: number;
  dispatched: number;
  unknown: number;
}): number {
  return counts.queued + counts.leased + counts.dispatched + counts.unknown;
}

/**
 * Install the queue-projection owner: it refetches whenever the core announces
 * a committed queue transition (`queue.changed` bumps `queue.revision` — the
 * SOLE invalidation; dispatch_* events are activity only). Returns a disposer.
 */
export function installQueueSync(onError: (message: string) => void): () => void {
  onErrorSink = onError;
  let lastRevision = store.getState().queue.revision;
  requestQueueSnapshot();
  const unsubscribe = store.subscribe(() => {
    const revision = store.getState().queue.revision;
    if (revision === lastRevision) return;
    lastRevision = revision;
    const changedId = store.getState().queue.lastChangedId;
    if (changedId) {
      // PERF-016: a single known item changed — enqueue an incremental patch.
      // The patch loop handles single-flight + dirty coalescing.
      enqueueItemPatch(changedId);
    } else {
      // No specific id: this is a collection invalidation (reorder, bulk
      // terminal, etc.) — force a full authoritative snapshot.
      requestQueueSnapshot();
    }
  });
  return () => {
    unsubscribe();
    onErrorSink = null;
    // AUDIT-W2-006: invalidate every in-flight operation of THIS controller
    // and retire its single-flight claims, so a subsequent installation
    // starts clean and dead-owner completions cannot commit.
    epoch += 1;
    generation += 1;
    patchGeneration += 1;
    inFlight = false;
    dirty = false;
    pendingOffset = null;
    patchInFlight = false;
    patchDirty = false;
    pendingPatchIds.clear();
  };
}

/**
 * PERF-016: queue item patch with single-flight + dirty coalescing.
 * At most one item read is in flight. Additional invalidations during
 * that read accumulate in `pendingPatchIds`. On completion:
 * - If only one id accumulated, patch it inline.
 * - If multiple distinct ids accumulated, collapse to a full snapshot
 *   (reorder and multi-row changes cannot be represented by one-row patches).
 * - If a reorder was indicated (lastChangedId is null), force a full snapshot.
 */
async function patchSingleItemLoop(): Promise<void> {
  // AUDIT-W2-006: same ownership rule as the snapshot path — a disposed
  // controller's patch completion mutates nothing.
  const ownerEpoch = epoch;
  const superseded = () =>
    myGen !== patchGeneration || ownerEpoch !== epoch;
  patchInFlight = true;
  const myGen = ++patchGeneration;
  while (!superseded() && pendingPatchIds.size > 0) {
    // Take one id at a time; if many accumulate, collapse to snapshot.
    if (pendingPatchIds.size > 3) {
      // Too many distinct items changed — this is a collection invalidation
      // (reorder, bulk terminal, etc.). Full snapshot is the only correct path.
      pendingPatchIds.clear();
      patchInFlight = false;
      requestQueueSnapshot();
      return;
    }
    const id = pendingPatchIds.values().next().value!;
    pendingPatchIds.delete(id);
    try {
      const item = await commands.queueGetItem(id);
      if (superseded()) return; // dead owner: no flags, no store, no follow-up
      const before = store.getState().queue.items.find((candidate) => candidate.id === id);
      if (!before || before.state !== item.state) {
        // A row entering/leaving this bounded page changes aggregate counts
        // and may require a replacement row. Only a fresh backend page can
        // reconcile membership without inventing frontend queue truth.
        pendingPatchIds.clear();
        patchInFlight = false;
        requestQueueSnapshot();
        return;
      }
      store.patch((s) => {
        if (s.queue.stale) return s;
        const items = s.queue.items.map((i) => (i.id === id ? item : i));
        return { ...s, queue: { ...s.queue, items, lastChangedId: undefined } };
      });
    } catch {
      if (superseded()) return; // dead owner: silent
      // Item no longer present or read failed: full snapshot.
      pendingPatchIds.clear();
      patchInFlight = false;
      requestQueueSnapshot();
      return;
    }
  }
  if (superseded()) return;
  patchInFlight = false;
  if (patchDirty) {
    patchDirty = false;
    void patchSingleItemLoop();
  }
}

/** Enqueue an item patch request. Coalesces with in-flight reads. */
function enqueueItemPatch(itemId: string): void {
  if (patchInFlight) {
    patchDirty = true;
    pendingPatchIds.add(itemId);
    return;
  }
  pendingPatchIds.add(itemId);
  void patchSingleItemLoop();
}

/** Public single-item patch entry; the backend can push a targeted
 * `queue-item-updated` row update and have the owner patch only that item. */
export function requestQueueItemPatch(itemId: string): void {
  enqueueItemPatch(itemId);
}

/** Test-only reset of the owner's internal lifecycle state. */
export function resetQueueSyncForTest(): void {
  epoch += 1;
  inFlight = false;
  dirty = false;
  pendingOffset = null;
  generation += 1;
  patchInFlight = false;
  patchDirty = false;
  patchGeneration += 1;
  pendingPatchIds.clear();
  onErrorSink = null;
}
