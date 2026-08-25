// Queue sync + staleness tests (TASK 24 audit, T-032). The queue projection is
// owned by queueSync.ts — the panel NEVER fetches itself (it only nudges the
// owner on mount). These tests drive the real owner via installQueueSync, so
// the single-flight / dirty-follow-up / fail-closed-stale rules are exercised
// exactly as the running app runs them.
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { renderToString } from "react-dom/server";
import type { QueueItem, QueueSnapshot } from "@saiwork2/contracts";
import { QueuePanel } from "./QueuePanel";
import { commands } from "../app/backend";
import { installQueueSync, resetQueueSyncForTest, requestQueueItemPatch } from "../app/queueSync";
import { store, initialState } from "../state/store";

function item(id: string, state: QueueItem["state"]): QueueItem {
  return {
    id,
    workspace_id: "w1",
    engine_id: "fake",
    session_id: null,
    session_mode: "new",
    model: null,
    payload: `payload-${id}`,
    payload_truncated: false,
    state,
    order_key: 1,
    revision: 3,
    lease_id: null,
    leased_at: null,
    attempt_count: 0,
    run_id: null,
    last_error: null,
    last_error_code: null,
    created_at: 1,
    updated_at: 1,
  };
}

function buttonHtml(html: string, label: string): string | undefined {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return html.match(new RegExp(`<button[^>]*>\\s*${escaped}\\s*</button>`))?.[0];
}

function snapshot(items: QueueItem[]): QueueSnapshot {
  const counts = {
    queued: 0,
    leased: 0,
    dispatched: 0,
    done: 0,
    failed: 0,
    cancelled: 0,
    unknown: 0,
  };
  for (const i of items) counts[i.state as keyof typeof counts] += 1;
  return {
    status: "ready",
    paused: false,
    items,
    counts,
    active_offset: 0,
    active_limit: 100,
    queued_before: 0,
    active_has_more: false,
  };
}

describe("queue projection owner (queueSync + QueuePanel)", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    resetQueueSyncForTest();
    store.patch(() => ({ ...initialState, lifecycle: "ready" }));
  });

  afterEach(() => {
    vi.restoreAllMocks();
    resetQueueSyncForTest();
    store.patch(() => ({ ...initialState, lifecycle: "ready" }));
  });

  it("does not duplicate the main prompt composer", () => {
    const html = renderToString(<QueuePanel state={store.getState()} onError={() => undefined} />);
    expect(html).not.toContain("Enqueue a prompt");
    expect(html).not.toContain("queue-panel__add");
  });

  it("offers explicit terminal cleanup without treating unknown work as finished", () => {
    const done = item("done", "done");
    const failed = item("failed", "failed");
    const unknown = item("unknown", "unknown");
    store.patch((s) => ({
      ...s,
      queue: { ...s.queue, ...snapshot([done, failed, unknown]), revision: 1 },
    }));

    const html = renderToString(
      <QueuePanel state={store.getState()} onError={() => undefined} />,
    );
    expect(html.replaceAll("<!-- -->", "")).toContain("Clear finished (2)");
    expect(html).toContain("Abandon (risk)");
  });

  it("marks the projection stale, surfaces the error, and disables mutations until a fresh snapshot succeeds", async () => {
    const queued = item("q1", "queued");
    const unknown = item("q2", "unknown");
    const goodSnapshot: QueueSnapshot = snapshot([queued, unknown]);
    let failNext = false;
    vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => {
      if (failNext) throw new Error("storage unavailable");
      return goodSnapshot;
    });
    // Revision-sensitive mutations must never be issued from stale rows.
    const mutationCalls: string[] = [];
    for (const method of [
      "queueEdit",
      "queueReorder",
      "queueRetry",
      "queueCancel",
      "queueResolveUnknown",
      "queueEnqueue",
      "queuePause",
      "queueResume",
    ] as const) {
      vi.spyOn(commands, method).mockImplementation(async () => {
        mutationCalls.push(method);
        return undefined as never;
      });
    }
    const onError = vi.fn();

    // ---- snapshot A loads fine: controls are enabled ----
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose = installQueueSync(onError);
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["q1", "q2"]);
    expect(store.getState().queue.stale).toBe(false);
    expect(onError).not.toHaveBeenCalled();
    const freshHtml = renderToString(<QueuePanel state={store.getState()} onError={onError} />);
    expect(freshHtml).not.toContain("stale (read-only)");
    expect(buttonHtml(freshHtml, "Edit")).not.toContain("disabled");
    expect(buttonHtml(freshHtml, "Retry")).not.toContain("disabled");
    expect(buttonHtml(freshHtml, "Abandon (risk)")).not.toContain("disabled");

    // ---- next authoritative snapshot fails: stale + surfaced + read-only ----
    failNext = true;
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    await Promise.resolve();
    await Promise.resolve();
    const s = store.getState();
    expect(s.queue.stale).toBe(true);
    expect(onError).toHaveBeenCalledWith(expect.stringContaining("queue snapshot failed"));
    const staleHtml = renderToString(<QueuePanel state={store.getState()} onError={onError} />);
    expect(staleHtml).toContain("stale (read-only)");
    // Revision-sensitive mutations are disabled while the projection is stale.
    expect(buttonHtml(staleHtml, "Edit")).toContain("disabled");
    expect(buttonHtml(staleHtml, "Cancel")).toContain("disabled");
    expect(buttonHtml(staleHtml, "Retry")).toContain("disabled");
    expect(buttonHtml(staleHtml, "Abandon (risk)")).toContain("disabled");
    // No mutation call was issued from the stale projection.
    expect(mutationCalls).toEqual([]);

    // ---- fresh authoritative snapshot restores the controls ----
    failNext = false;
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getState().queue.stale).toBe(false);
    const restoredHtml = renderToString(<QueuePanel state={store.getState()} onError={onError} />);
    expect(restoredHtml).not.toContain("stale (read-only)");
    expect(buttonHtml(restoredHtml, "Edit")).not.toContain("disabled");
    expect(buttonHtml(restoredHtml, "Retry")).not.toContain("disabled");
    expect(mutationCalls).toEqual([]);
    dispose();
  });

  it("snapshot loading is single-flight across a burst of revisions (TASK 24 perf)", async () => {
    // queue.changed is the SOLE invalidation: a burst of committed
    // transitions while a snapshot is in flight must NOT launch one request
    // per revision — at most one in-flight read + one follow-up.
    let snapshotCalls = 0;
    let resolveSnap: (s: QueueSnapshot) => void = () => {};
    const pending = new Promise<QueueSnapshot>((r) => {
      resolveSnap = r;
    });
    vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => {
      snapshotCalls += 1;
      return pending;
    });
    const onError = vi.fn();

    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose = installQueueSync(onError);
    await Promise.resolve(); // first snapshot now in flight (pending)
    expect(snapshotCalls).toBe(1);

    // 100 committed transitions arrive while the snapshot is still pending.
    for (let i = 0; i < 100; i++) {
      store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    }
    // Still exactly one request in flight — no per-revision storm.
    expect(snapshotCalls).toBe(1);

    // The in-flight snapshot settles; the dirty flag allows AT MOST one
    // follow-up, and the final projection equals the authoritative result.
    resolveSnap(snapshot([item("q1", "queued")]));
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    expect(snapshotCalls).toBeLessThanOrEqual(2);
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["q1"]);
    expect(store.getState().queue.stale).toBe(false);
    dispose();
  });

  it("a slower older response can never overwrite newer snapshot truth (generation guard)", async () => {
    // Two snapshots resolve out of order: the first request (older queue
    // state) hangs and resolves LATE; the dirty follow-up fetches the
    // authoritative latest. The final projection must be the newest result.
    const gates: ((s: QueueSnapshot) => void)[] = [];
    const pendingFirst = new Promise<QueueSnapshot>((r) => gates.push(r));
    let call = 0;
    vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => {
      call += 1;
      if (call === 1) return pendingFirst; // first request hangs
      return snapshot([item("new", "queued")]);
    });
    const onError = vi.fn();

    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose = installQueueSync(onError);
    await Promise.resolve();
    expect(call).toBe(1);
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    expect(call).toBe(1); // still single-flight: no second request yet

    // First (older) response resolves late with an old projection; then the
    // single follow-up fetches the authoritative latest.
    gates[0]!(snapshot([item("old", "queued")]));
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    // Final projection is the NEWEST authoritative snapshot, never the late
    // older response.
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["new"]);
    expect(call).toBeLessThanOrEqual(2);
    dispose();
  });

  it("closes the Queue tab on the non-empty → empty TRANSITION only (T-032)", async () => {
    // Steady EMPTY queue at install: the startup snapshot must NOT close the
    // tab the user just opened.
    let next: QueueSnapshot = snapshot([]);
    vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => next);
    const onError = vi.fn();

    store.patch((s) => ({
      ...s,
      queue: { ...s.queue, revision: 1 },
      closeQueueWhenDone: true,
      activeDockTab: "queue",
    }));
    const dispose = installQueueSync(onError);
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getState().activeDockTab).toBe("queue");

    // Non-empty → empty transition closes the tab and switches to Activity.
    next = snapshot([item("q2", "queued")]);
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getState().activeDockTab).toBe("queue");
    next = snapshot([]);
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getState().activeDockTab).toBe("activity");

    // When the toggle is OFF, the transition must not move the tab.
    store.patch((s) => ({ ...s, closeQueueWhenDone: false, activeDockTab: "queue" }));
    next = snapshot([item("q3", "queued")]);
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    await Promise.resolve();
    await Promise.resolve();
    next = snapshot([]);
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: s.queue.revision + 1 } }));
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getState().activeDockTab).toBe("queue");
    dispose();
  });

  it("editing a row fetches the FULL payload on demand (TASK 24 perf)", async () => {
    // The snapshot carries only a bounded payload preview; the full durable
    // payload must be fetched exactly when the user edits the item — the
    // panel render itself must never fetch the full body.
    const preview = item("q1", "queued");
    preview.payload = "512-byte preview…(truncated)";
    store.patch((s) => ({
      ...s,
      queue: {
        ...s.queue,
        revision: 1,
        items: [preview],
        payloadPreview: true,
      },
    }));
    const getItem = vi.spyOn(commands, "queueGetItem").mockImplementation(async (id: string) => {
      const full = item(id, "queued");
      full.payload = "the-exact-full-64kb-payload";
      return full;
    });
    const onError = vi.fn();

    renderToString(<QueuePanel state={store.getState()} onError={onError} />);
    expect(getItem).not.toHaveBeenCalled();
    expect(onError).not.toHaveBeenCalled();
    // The preview is rendered, never the fabricated full payload.
    expect(store.getState().queue.items[0]!.payload).toContain("preview");
  });

  it("a patch for a row outside this page refreshes the authoritative page, never invents membership (W2-003 authority)", async () => {
    // The owner is the SINGLE authority for queue freshness. The frontend
    // must not append a fetched row it cannot prove belongs on this page:
    // a patch whose row is absent (or changed state) triggers exactly one
    // authoritative page refresh instead.
    const a = item("a", "queued");
    let page = snapshot([a]);
    let snapshotCalls = 0;
    vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => {
      snapshotCalls += 1;
      return page;
    });
    let getItemCalls = 0;
    vi.spyOn(commands, "queueGetItem").mockImplementation(async (id: string) => {
      getItemCalls += 1;
      const fetched = item(id, "dispatched");
      // By the time the refresh lands, the backend page contains the row.
      page = snapshot([a, fetched]);
      return fetched;
    });
    const onError = vi.fn();

    // Initial snapshot loads [a].
    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose = installQueueSync(onError);
    for (let i = 0; i < 4; i++) await Promise.resolve();
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["a"]);
    expect(getItemCalls).toBe(0);

    // A targeted patch for item "b" (not provably on this page) arrives.
    requestQueueItemPatch("b");
    for (let i = 0; i < 8; i++) await Promise.resolve();
    // Exactly one item read plus ONE authoritative refresh — no append, no storm.
    expect(getItemCalls).toBe(1);
    expect(snapshotCalls).toBe(2);
    expect(store.getState().queue.items.map((i) => i.id).sort()).toEqual(["a", "b"]);
    expect(store.getState().queue.items.find((i) => i.id === "b")?.state).toBe("dispatched");
    expect(onError).not.toHaveBeenCalled();
    dispose();
  });

  it("a same-state patch to an already-present item replaces it in place without a reload (W2-003 authority)", async () => {
    // A re-patch of an existing id with unchanged state must overwrite the
    // prior row in place — no duplicate, and no full-snapshot reload.
    const a = item("a", "queued");
    let snapshotCalls = 0;
    vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => {
      snapshotCalls += 1;
      return snapshot([a]);
    });
    vi.spyOn(commands, "queueGetItem").mockImplementation(async (id: string) => {
      const updated = item(id, "queued");
      updated.payload = "revised-payload";
      return updated;
    });
    const onError = vi.fn();

    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose = installQueueSync(onError);
    for (let i = 0; i < 4; i++) await Promise.resolve();
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["a"]);

    requestQueueItemPatch("a");
    for (let i = 0; i < 6; i++) await Promise.resolve();
    // Single membership: still exactly one "a", now reflecting the patch,
    // committed in place WITHOUT another snapshot round-trip.
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["a"]);
    expect(store.getState().queue.items[0]?.state).toBe("queued");
    expect(store.getState().queue.items[0]?.payload).toBe("revised-payload");
    expect(snapshotCalls).toBe(1);
    expect(onError).not.toHaveBeenCalled();
    dispose();
  });

  it("a disposed controller can never commit its late snapshot into a new session (AUDIT-W2-006)", async () => {
    // StrictMode-style lifecycle: setup #1 starts a read, cleanup disposes
    // it, setup #2 installs and fetches its OWN authoritative page. When
    // owner #1's OLD response finally lands it must be inert: no commit,
    // no error surfacing, no follow-up storm.
    const gates: ((s: QueueSnapshot) => void)[] = [];
    let call = 0;
    vi.spyOn(commands, "queueSnapshot").mockImplementation(() => {
      call += 1;
      if (call === 1) {
        return new Promise<QueueSnapshot>((resolve) => gates.push(resolve));
      }
      return Promise.resolve(snapshot([item("fresh", "queued")]));
    });
    const onError1 = vi.fn();
    const onError2 = vi.fn();

    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose1 = installQueueSync(onError1);
    await Promise.resolve();
    expect(call).toBe(1); // owner #1 read in flight

    dispose1(); // controller #1 dies mid-read
    const dispose2 = installQueueSync(onError2);
    for (let i = 0; i < 6; i++) await Promise.resolve();
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["fresh"]);

    // Owner #1's stale truth lands AFTER the new session is authoritative…
    gates[0]!(snapshot([item("stale-old", "queued")]));
    for (let i = 0; i < 8; i++) await Promise.resolve();
    // …and must NOT have overwritten it, nor surfaced into either sink.
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["fresh"]);
    expect(store.getState().queue.stale).toBe(false);
    expect(onError1).not.toHaveBeenCalled();
    expect(onError2).not.toHaveBeenCalled();
    dispose2();
  });

  it("a disposed controller's pending item patch is equally inert (AUDIT-W2-006)", async () => {
    let getItemResolve: ((i: QueueItem) => void) = () => {};
    vi.spyOn(commands, "queueSnapshot").mockResolvedValue(snapshot([item("a", "queued")]));
    vi.spyOn(commands, "queueGetItem").mockImplementation(
      () =>
        new Promise<QueueItem>((resolve) => {
          getItemResolve = resolve;
        }),
    );
    const onError1 = vi.fn();
    const onError2 = vi.fn();

    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose1 = installQueueSync(onError1);
    for (let i = 0; i < 4; i++) await Promise.resolve();

    requestQueueItemPatch("a"); // owner #1 patch in flight
    await Promise.resolve();
    dispose1(); // dies holding the patch
    const dispose2 = installQueueSync(onError2);
    for (let i = 0; i < 4; i++) await Promise.resolve();

    // The dead patch resolves with different truth — it must not commit.
    const deadRow = item("a", "done");
    deadRow.payload = "dead-owner-truth";
    getItemResolve(deadRow);
    for (let i = 0; i < 8; i++) await Promise.resolve();
    const row = store.getState().queue.items.find((i) => i.id === "a");
    expect(row?.state).toBe("queued"); // dead-owner patch must not commit
    expect(row?.payload).toBe("payload-a");
    dispose2();
  });

  it("a reorder invalidation routes to a full snapshot and renders the new order (AUDIT-W2-001)", async () => {
    // Backend contract: reorder publishes queue.changed WITHOUT an item id
    // — the collection-invalidation signal. The owner must take the FULL
    // snapshot path (single-row patching cannot express a multi-row move).
    let page = snapshot([item("a", "queued"), item("b", "queued"), item("c", "queued")]);
    const snapCalls = vi.spyOn(commands, "queueSnapshot").mockImplementation(async () => {
      return page;
    });
    const getItem = vi.spyOn(commands, "queueGetItem");
    const onError = vi.fn();

    store.patch((s) => ({ ...s, queue: { ...s.queue, revision: 1 } }));
    const dispose = installQueueSync(onError);
    for (let i = 0; i < 4; i++) await Promise.resolve();
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["a", "b", "c"]);
    const callsBefore = snapCalls.mock.calls.length;

    // SQLite reordered C to front; the durable page now reads c,a,b.
    page = snapshot([item("c", "queued"), item("a", "queued"), item("b", "queued")]);
    // Collection invalidation: NO lastChangedId (that is the reorder signal).
    store.patch((s) => ({
      ...s,
      queue: { ...s.queue, revision: s.queue.revision + 1, lastChangedId: undefined },
    }));
    for (let i = 0; i < 6; i++) await Promise.resolve();

    // Authoritative full snapshot happened (not a single-row patch).
    expect(snapCalls.mock.calls.length).toBe(callsBefore + 1);
    expect(getItem).not.toHaveBeenCalled();
    expect(store.getState().queue.items.map((i) => i.id)).toEqual(["c", "a", "b"]);
    dispose();
  });
});
