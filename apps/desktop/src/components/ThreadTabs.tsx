import { useRef } from "react";
import type { SliceProps } from "../state/slices";
import { selectSession } from "../app/sessionSelection";
import { commands } from "../app/backend";
import { store, upsertSession } from "../state/store";
import { useSingleFlight } from "../app/singleFlight";

import { sessionActivationAvailability, sessionCreateAvailability } from "../app/eligibility";

/** One definition of what this component consumes (state/slices.ts). */
export const threadTabsKeys = [
  "activeSessionId",
  "currentWorkspaceId",
  "engines",
  "selectedEngineId",
  "selectedModelId",
  "sessions",
  "stoppingEngines",
  "lifecycle",
] as const;

type Props = SliceProps<(typeof threadTabsKeys)[number]>;

/**
 * Thread tabs are a VIEW over the canonical session registry (Freebuff Phase
 * B): no second thread store, no second lifecycle. Select/create only — there
 * is deliberately no `×`, because "close tab" has four possible backend
 * meanings (hide / close session / stop engine session / delete persisted
 * session) and only one explicit contract may exist (§25).
 */
export function ThreadTabs({ state, onError }: Props) {
  // Synchronous single-flight latch: the guard is a ref flipped BEFORE the
  // await — `disabled` alone is one render behind and lets a same-tick double
  // activation create two sessions.
  const creating = useSingleFlight();
  const listRef = useRef<HTMLDivElement>(null);

  // CORE-014: Use canonical session eligibility.
  const create$ = sessionCreateAvailability(state);

  function newThread() {
    const engineId = state.selectedEngineId ?? state.engines[0]?.id;
    if (!engineId || !create$.allowed) return;
    // W2-012: capture workspace before the async gap so a workspace switch
    // during creation cannot project A's session into B.
    const wsId = state.currentWorkspaceId;
    void creating.run(async () => {
      try {
        // Canonical session creation: the command returns the authoritative
        // Session DTO, upserted as truth (deduped by id).
        const session = await commands.createSession(
          engineId,
          wsId,
          state.selectedModelId,
        );
        // W2-012: only project if the workspace hasn't changed since the create
        // was initiated. A superseded response leaves the durable session but
        // does not project it into the wrong workspace.
        if (store.getState().currentWorkspaceId !== wsId) return;
        upsertSession(session);
      } catch (e) {
        onError(String(e));
      }
    });
  }

  /** Roving-tabindex keyboard navigation (WAI-ARIA tabs): ←/→ move, Home/End
   * jump. Selection follows focus, exactly like the mouse path. */
  function onKeyDown(e: React.KeyboardEvent, index: number) {
    const ids = state.sessions.map((s) => s.id);
    if (ids.length === 0) return;
    let next: number | null = null;
    if (e.key === "ArrowRight") next = (index + 1) % ids.length;
    else if (e.key === "ArrowLeft") next = (index - 1 + ids.length) % ids.length;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = ids.length - 1;
    if (next === null) return;
    e.preventDefault();
    const id = ids[next];
    if (!id) return;

    // CORE-014: Keyboard navigation must enforce eligibility
    const targetSession = state.sessions.find(s => s.id === id);
    if (!targetSession || !sessionActivationAvailability(targetSession, state).allowed) {
      return;
    }

    selectSession(id);
    const el = listRef.current?.querySelector<HTMLButtonElement>(`[data-session-id="${id}"]`);
    el?.focus();
  }

  return (
    <div className="thread-tabs" ref={listRef}>
      <div className="thread-tabs__list" role="tablist" aria-label="Threads">
        {state.sessions.map((s, i) => {
          const active = s.id === state.activeSessionId;
          // CORE-014: Use canonical session eligibility.
          const activate$ = sessionActivationAvailability(s, state);
          const unusable = !activate$.allowed;
          return (
            <button
              key={s.id}
              role="tab"
              data-session-id={s.id}
              aria-selected={active}
              tabIndex={active ? 0 : -1}
              className={
                "thread-tab" +
                (active ? " thread-tab--active" : "") +
                (unusable ? " thread-tab--unusable" : "")
              }
              onClick={() => {
                if (activate$.allowed) selectSession(s.id);
              }}
              onKeyDown={(e) => onKeyDown(e, i)}
              title={activate$.reason ?? s.display_name}
            >
              <span className="thread-tab__label">{s.display_name}</span>
              {s.running && <span className="dot dot--running" title="running" />}
            </button>
          );
        })}
      </div>
      <button
        className="thread-tab thread-tab--new"
        onClick={newThread}
        disabled={!create$.allowed || creating.busy}
        title={create$.reason ?? "New thread"}
        aria-label="New thread"
      >
        +
      </button>
    </div>
  );
}
