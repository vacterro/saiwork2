import type { SliceProps } from "../state/slices";
import { commands, confirmDialog } from "../app/backend";
import { useSingleFlight } from "../app/singleFlight";
import { store } from "../state/store";

/** One definition of what this panel consumes (state/slices.ts): it types the
 * props AND generates the memo comparator. */
import { sessionActivationAvailability, sessionCreateAvailability } from "../app/eligibility";

export const sessionListKeys = [
  "activeSessionId",
  "currentWorkspaceId",
  "engines",
  "selectedEngineId",
  "selectedModelId",
  "sessions",
  "running",
  "runningStale",
  "stoppingEngines",
  "lifecycle",
] as const;

type Props = SliceProps<(typeof sessionListKeys)[number]>;

export function SessionList({ state, onError }: Props) {
  // Synchronous single-flight latch (app/singleFlight.ts): a ref flipped BEFORE
  // the await, so a same-tick double activation cannot create two sessions.
  const creating = useSingleFlight();
  const deleting = useSingleFlight();

  // CORE-014: Use canonical session eligibility.
  const create$ = sessionCreateAvailability(state);

  function newSession() {
    const engineId = state.selectedEngineId ?? state.engines[0]?.id;
    if (!engineId || !create$.allowed) return;
    // W2-012: capture workspace before the async gap so a workspace switch
    // during creation cannot project A's session into B.
    const wsId = state.currentWorkspaceId;
    void creating.run(async () => {
      try {
        // The command returns the AUTHORITATIVE Session DTO (TASK 24 §9): it is
        // upserted into the store as truth (deduped by id). `session.created`
        // carries the same full DTO, so event-before-response ordering cannot
        // fabricate or duplicate rows.
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

  return (
    <section className="sessions">
      <div className="sessions__header">
        <span>THREADS</span>
        <button
          className="btn btn--small"
          onClick={newSession}
          disabled={!create$.allowed || creating.busy}
          title={create$.reason ?? "New session"}
        >
          New session
        </button>
      </div>
      <ul className="sessions__list">
        {state.sessions.map((s) => {
          // CORE-014: Use canonical session eligibility.
          const activate$ = sessionActivationAvailability(s, state);
          const muted = !activate$.allowed;
          return (
            <li key={s.id} className="sessions__row">
              <button
                className={
                  s.id === state.activeSessionId
                    ? "sessions__item sessions__item--active"
                    : "sessions__item"
                }
                disabled={muted}
                title={activate$.reason ?? s.display_name}
                onClick={() => selectSession(s.id)}
              >
                <span className="sessions__item-title">
                  {s.display_name}
                  {s.running && <span className="dot dot--running" title="running" />}
                  {muted && <span className="sessions__item-muted">({s.usable_now === false ? "unusable now" : s.engine_id})</span>}
                </span>
                <span className="sessions__item-meta">
                  {s.engine_id} · {s.engine_session_id || "—"}
                </span>
              </button>
              <button
                className="btn btn--small sessions__delete"
                aria-label={`Delete ${s.display_name}`}
                title="Delete session and its upstream history"
                disabled={deleting.busy || s.running || Boolean(state.running[s.id]) || state.runningStale || state.lifecycle !== "ready"}
                onClick={() => void deleting.run(async () => {
                  const confirmed = await confirmDialog(`Delete session “${s.display_name}”? This removes its upstream history and cannot be undone.`);
                  if (!confirmed) return;
                  await deleteSession(s.id);
                }).catch((error) => onError(String(error)))}
              >
                Delete
              </button>
            </li>
          );
        })}
        {state.sessions.length === 0 && <li className="sessions__empty">No sessions yet</li>}
      </ul>
    </section>
  );
}

// Selection (and the authoritative history restore it triggers) has ONE owner
// shared with ThreadTabs — never a per-component copy.
import { upsertSession } from "../state/store";
import { deleteSession, selectSession } from "../app/sessionSelection";
