// Minimal functional Queue UI proof (TASK 13). The UI is a pure projection:
// every mutation goes through typed QueueManager commands; the SQLite-backed
// snapshot is refetched by the single queue-sync owner (queueSync.ts), which
// is mounted once for the whole app. The UI never holds a second queue truth
// (law 5) and every edit carries the expected revision (CAS — a stale UI
// cannot silently overwrite a newer mutation). A snapshot failure is NOT
// swallowed: the owner sets `queue.stale` and every revision-sensitive
// mutation is disabled until a fresh authoritative snapshot succeeds.
//
// The panel only NUDGES the owner on mount (requestQueueSnapshot is
// single-flight + generation-guarded inside queueSync); it never owns the
// read lifecycle, so mounting/unmounting the dock tab cannot stop queue
// synchronization and cannot create a second request stream.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { QueueItem } from "@saiwork2/contracts";
import type { SliceProps } from "../state/slices";
import { commands, confirmDialog } from "../app/backend";
import { requestQueueSnapshot } from "../app/queueSync";
import {
  queueAdminAvailability,
  queueItemMutationAvailability,
} from "../app/eligibility";

/** One definition of what the queue panel consumes (state/slices.ts). */
export const queuePanelKeys = [
  "queue",
  "lifecycle",
] as const;

/** Hard ceiling on rendered queue rows. The snapshot is the sole durable
 * truth, but a long queue must not mount unbounded DOM (TASK 24 perf +
 * no-unbounded-everything law): the panel renders at most PAGE rows and the
 * user paginates the rest. The full list is still addressed by commands. */
const PAGE = 100;

type Props = SliceProps<(typeof queuePanelKeys)[number]>;

export function QueuePanel({ state, onError }: Props) {
  const [editId, setEditId] = useState<string | null>(null);
  const [editDraft, setEditDraft] = useState("");
  // W2-011: monotonic edit ownership generation — prevents a slow Edit A
  // from overwriting a newer Edit B the user already started.
  const editGen = useRef(0);
  const [visible, setVisible] = useState(PAGE);
  const visibleResetFor = useRef<number>(-1);

  // PERF-021: only nudge the queue owner when the projection has never
  // bootstrapped or is stale — not on every mount/unmount cycle. The
  // globally owned queueSync already handles bootstrap and event-driven
  // synchronization; repeated tab switches must not re-fetch the full
  // queue when nothing changed.
  useEffect(() => {
    if (state.queue.stale || state.queue.items.length === 0) {
      requestQueueSnapshot();
    }
  }, []);

  // Reset pagination to the first page whenever the snapshot identity changes
  // (a refetch/append) — guard with a ref so re-renders within the same
  // revision don't keep resetting the user's scroll position.
  if (visibleResetFor.current !== state.queue.revision) {
    visibleResetFor.current = state.queue.revision;
    if (visible !== PAGE) setVisible(PAGE);
  }

  // Canonical queue-mutation policy (eligibility.ts): the handler ENFORCES the
  // same verdict the buttons render (a disabled attribute is never the guard).
  const canAdmin = queueAdminAvailability(state);
  const canMutateItem = queueItemMutationAvailability(state);

  // Derived ONCE per authoritative snapshot reference (TASK 24 perf): the
  // render previously re-filtered + re-found every queued item PER ROW,
  // making one render O(N²). Both the boundary buttons and move() now share
  // this single derivation + O(1) index lookup.
  const queuedItems = useMemo(
    () => state.queue.items.filter((i) => i.state === "queued"),
    [state.queue.items],
  );
  const queuedIndexById = useMemo(() => {
    const m = new Map<string, number>();
    queuedItems.forEach((i, idx) => m.set(i.id, idx));
    return m;
  }, [queuedItems]);

  const visibleItems = state.queue.items.slice(0, visible);

  const startEdit = useCallback(async (item: QueueItem) => {
    // The snapshot carries only a bounded payload preview (TASK 24 perf + the
    // Rust truncation flag): the FULL exact payload is fetched on demand when
    // the user actually edits the item — the UI never holds tens of MiB of
    // prompt bodies.
    // W2-011: capture edit generation before the async fetch.
    const myGen = ++editGen.current;
    try {
      const full = await commands.queueGetItem(item.id);
      // W2-011: only populate if this fetch still owns the edit slot.
      if (myGen !== editGen.current) return;
      setEditId(item.id);
      setEditDraft(full.payload);
    } catch (e) {
      if (myGen !== editGen.current) return;
      onError(`could not load full item for editing: ${String(e)}`);
    }
  }, [onError]);

  async function saveEdit(item: QueueItem) {
    if (!canMutateItem.allowed) {
      if (canMutateItem.reason) onError(canMutateItem.reason);
      return;
    }
    const payload = editDraft.trim();
    if (!payload) return;
    try {
      await commands.queueEdit(item.id, item.revision, payload, item.model);
      setEditId(null);
    } catch (e) {
      onError(String(e)); // includes Conflict when revision moved
    }
  }

  async function move(item: QueueItem, delta: number) {
    if (!canMutateItem.allowed) {
      if (canMutateItem.reason) onError(canMutateItem.reason);
      return;
    }
    // The API expects a ZERO-BASED insertion index (TASK 24 §9): derive the
    // current index from the authoritative ordered snapshot, never from the
    // durable one-based order_key (±1 arithmetic on order_key would jump a
    // first item to the end / no-op a middle item up).
    const index = queuedIndexById.get(item.id);
    if (index === undefined) return;
    const target = index + delta;
    if (target < 0 || target >= queuedItems.length) return; // boundary: disabled anyway
    try {
      await commands.queueReorder(item.id, item.revision, target);
    } catch (e) {
      onError(String(e));
    }
  }


  return (
    <section className="queue-panel">
      <header className="queue-panel__head">
        <span className="label">QUEUE</span>
        <span className="muted">
          {state.queue.status === "paused" ? "paused" : state.queue.status} · {state.queue.items.length} item
          {state.queue.items.length === 1 ? "" : "s"}
        </span>
        {state.queue.stale && (
          <span className="queue-panel__stale muted" title="The last authoritative snapshot fetch failed; this projection is stale and read-only.">
            ⚠ stale (read-only)
          </span>
        )}
        <span className="queue-panel__spacer" />
        {state.queue.paused ? (
          <button
            className="btn btn--small"
            onClick={() => {
              if (!canAdmin.allowed) {
                if (canAdmin.reason) onError(canAdmin.reason);
                return;
              }
              commands.queueResume().catch((e) => onError(String(e)));
            }}
            disabled={!canAdmin.allowed}
            title={canAdmin.reason}
          >
            Resume
          </button>
        ) : (
          <button
            className="btn btn--small"
            onClick={() => {
              if (!canAdmin.allowed) {
                if (canAdmin.reason) onError(canAdmin.reason);
                return;
              }
              commands.queuePause().catch((e) => onError(String(e)));
            }}
            disabled={state.queue.status !== "ready" || !canAdmin.allowed}
            title={canAdmin.reason}
          >
            Pause
          </button>
        )}
      </header>

      <ul className="queue-panel__list">
        {visibleItems.map((item) => (
          <li key={item.id} className={`queue-item queue-item--${item.state}`}>
            <div className="queue-item__row">
              <span className={`queue-item__state queue-item__state--${item.state}`}>{item.state}</span>
              <span className="queue-item__rev">rev {item.revision}</span>
              {item.attempt_count > 0 && <span className="queue-item__attempts">attempts {item.attempt_count}</span>}
              <span className="queue-item__spacer" />
              {(() => {
                // Boundary disable by ZERO-BASED index in the ordered set
                // (TASK 24 §9): order_key starts at 1, so first-item-Up must
                // be disabled at index 0 — never by order_key arithmetic.
                const index = queuedIndexById.get(item.id) ?? -1;
                const canUp = item.state === "queued" && index > 0;
                const canDown = item.state === "queued" && index >= 0 && index < queuedItems.length - 1;
                return (
                  <>
                    <button
                      className="btn btn--small"
                      onClick={() => move(item, -1)}
                      disabled={!canUp || !canMutateItem.allowed}
                      title={!canMutateItem.allowed ? canMutateItem.reason : undefined}
                    >
                      ↑
                    </button>
                    <button
                      className="btn btn--small"
                      onClick={() => move(item, 1)}
                      disabled={!canDown || !canMutateItem.allowed}
                      title={!canMutateItem.allowed ? canMutateItem.reason : undefined}
                    >
                      ↓
                    </button>
                  </>
                );
              })()}
            </div>
            {editId === item.id ? (
              <div className="queue-item__edit">
                <textarea rows={2} value={editDraft} onChange={(e) => setEditDraft(e.target.value)} />
                <button className="btn btn--small" onClick={() => saveEdit(item)} disabled={!canMutateItem.allowed}>
                  Save
                </button>
                <button className="btn btn--small" onClick={() => setEditId(null)}>
                  Cancel
                </button>
              </div>
            ) : (
              <div className="queue-item__payload" title={item.payload}>
                {item.payload}
                {item.payload_truncated ? "…" : ""}
              </div>
            )}
            <div className="queue-item__meta muted">
              {item.session_mode} session{item.session_id ? ` ${item.session_id.slice(0, 8)}` : ""}
              {item.model ? ` · ${item.model}` : ""}
              {item.run_id ? ` · run ${item.run_id.slice(0, 8)}` : ""}
              {item.last_error_code ? ` · ${item.last_error_code}: ${item.last_error ?? ""}` : ""}
            </div>
            {item.state === "unknown" && (
              <div className="queue-item__unknown-note">
                Execution status uncertain — the run may have started before a crash/restart.
                Automatic retry is disabled to avoid duplicate work; this workspace is blocked
                until you resolve it.
              </div>
            )}
            <div className="queue-item__actions">
              {item.state === "queued" && (
                <>
                  <button
                    className="btn btn--small"
                    onClick={() => {
                      if (!canMutateItem.allowed) {
                        if (canMutateItem.reason) onError(canMutateItem.reason);
                        return;
                      }
                      void startEdit(item);
                    }}
                    disabled={!canMutateItem.allowed}
                    title={!canMutateItem.allowed ? canMutateItem.reason : undefined}
                  >
                    Edit
                  </button>
                  <button
                    className="btn btn--small btn--danger"
                    onClick={() => {
                      if (!canMutateItem.allowed) {
                        if (canMutateItem.reason) onError(canMutateItem.reason);
                        return;
                      }
                      commands.queueCancel(item.id).catch((e) => onError(String(e)));
                    }}
                    disabled={!canMutateItem.allowed}
                    title={!canMutateItem.allowed ? canMutateItem.reason : undefined}
                  >
                    Cancel
                  </button>
                </>
              )}
              {(item.state === "failed" || item.state === "unknown") && (
                <button
                  className="btn btn--small"
                  title={
                    !canMutateItem.allowed
                      ? canMutateItem.reason
                      : item.state === "unknown"
                        ? "Retry as a new attempt — the previous run may have executed, so this can cause duplicate work"
                        : undefined
                  }
                  onClick={async () => {
                    if (!canMutateItem.allowed) {
                      if (canMutateItem.reason) onError(canMutateItem.reason);
                      return;
                    }
                    if (item.state === "unknown" && !(await confirmDialog("The previous run may have already executed. Retry as a new attempt anyway? This can cause duplicate work."))) {
                      return;
                    }
                    commands.queueRetry(item.id, item.revision).catch((e) => onError(String(e)));
                  }}
                  disabled={!canMutateItem.allowed}
                >
                  Retry
                </button>
              )}
              {item.state === "unknown" && (
                // UNKNOWN = external work may still be mutating the workspace.
                // Only the explicit, risk-confirmed abandon may transition it
                // (TASK 24 §9); ordinary Cancel is rejected by the backend
                // with a typed InvalidState error.
                <button
                  className="btn btn--small btn--danger"
                  onClick={async () => {
                    if (!canMutateItem.allowed) {
                      if (canMutateItem.reason) onError(canMutateItem.reason);
                      return;
                    }
                    if (!(await confirmDialog("This item's execution outcome is unknown — the external run may STILL be mutating the workspace. Abandoning unblocks the workspace WITHOUT stopping that external work. Abandon anyway?"))) {
                      return;
                    }
                    commands
                      .queueResolveUnknown(item.id, item.revision)
                      .catch((e) => onError(String(e)));
                  }}
                  disabled={!canMutateItem.allowed}
                  title={!canMutateItem.allowed ? canMutateItem.reason : undefined}
                >
                  Abandon (risk)
                </button>
              )}
              {(item.state === "dispatched" || item.state === "leased") && (
                // LEASED/DISPATCHED cancellation goes ONLY through the durable
                // QueueManager (`queueCancel`): it persists the cancel intent
                // BEFORE asking the adapter to cancel, so a crash mid-cancel
                // cannot lose the intent and the queue stays the sole durable
                // owner (TASK 24 §9). Direct `cancelRun` would skip that.
                // CORE-011: the Stop button is available during the LEASED
                // prepare/handshake phase before any run_id is known.
                <button
                  className="btn btn--small btn--danger"
                  onClick={() => {
                    if (!canMutateItem.allowed) {
                      if (canMutateItem.reason) onError(canMutateItem.reason);
                      return;
                    }
                    commands.queueCancel(item.id).catch((e) => onError(String(e)));
                  }}
                  disabled={!canMutateItem.allowed}
                  title={!canMutateItem.allowed ? canMutateItem.reason : undefined}
                >
                  Stop run
                </button>
              )}
            </div>
          </li>
        ))}
        {state.queue.items.length === 0 && <li className="muted queue-panel__empty">no queued items</li>}
        {visible < state.queue.items.length && (
          <li className="queue-panel__more">
            <button className="btn btn--small" onClick={() => setVisible((v) => v + PAGE)}>
              Show more ({state.queue.items.length - visible} hidden)
            </button>
          </li>
        )}
      </ul>
    </section>
  );
}
