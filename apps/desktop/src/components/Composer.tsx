import { useCallback, useEffect, useRef, useState } from "react";
import type { SendOutcome } from "@saiwork2/contracts";
import type { AppState } from "../state/store";
import type { SliceProps } from "../state/slices";
import { commands } from "../app/backend";
import { useSingleFlight } from "../app/singleFlight";
import { cancelAvailability, queueMutationAvailability, sendAvailability } from "../app/eligibility";
import {
  addLocalUserMessage,
  markUserMessageUncertain,
  removeLocalUserMessage,
  store,
  upsertSession,
} from "../state/store";
import { COMPOSER_APPEND_EVENT, appendDraft } from "./composerBridge";
import { setEnterQueues } from "./dock/persistence";
import { activateSession } from "../app/sessionSelection";

/** Everything the Composer reads — one definition drives both its props type
 * and its memo comparator (state/slices.ts). */
export const composerKeys = [
  "activeSessionId",
  // PERF-022: composerDraft removed from global state — draft is local.
  "currentWorkspaceId",
  "engines",
  "enterQueues",
  "lifecycle",
  "queue",
  "running",
  "runningStale",
  "selectedEngineId",
  "selectedModelId",
  "sessions",
] as const;

export type ComposerSlice = Pick<AppState, (typeof composerKeys)[number]>;
type Props = SliceProps<(typeof composerKeys)[number]>;

/** Production direct-send handler (TASK 24 §9), also driven by the smoke so
 * the test exercises the REAL decision logic: project the local user turn
 * before the external call, send with the UI's expected context, and map the
 * typed outcome — the pending turn is dropped ONLY on a definite rejection;
 * an unprovable outcome keeps it visible and marks it UNCERTAIN (cleared by
 * the authoritative message.started), never a blind resend. */
export async function performSend(
  sessionId: string,
  workspaceId: string | null,
  engineId: string | null,
  prompt: string,
  model: string | null,
  onError: (message: string) => void,
): Promise<SendOutcome | null> {
  // Project the local user turn BEFORE the external send with a stable
  // pending id: the assistant stream (message.started/deltas) may arrive
  // before the invoke promise resolves, and the conversation must always
  // read user → assistant (TASK 24 §9).
  const userMsgId = addLocalUserMessage(sessionId, prompt);
  try {
    // The direct-send boundary (TASK 24 §9): the UI declares the workspace
    // + engine it currently shows; the backend rejects any mismatch BEFORE
    // an external call, and returns a TYPED outcome.
    const outcome = await commands.sendPrompt(
      sessionId,
      workspaceId,
      engineId,
      prompt,
      model,
    );
    if (outcome.kind === "definitely_rejected") {
      // Definite pre-execution rejection: drop the pending user turn
      // (nothing was sent); never fabricate a sent message (TASK 24 §9).
      removeLocalUserMessage(sessionId, userMsgId);
      onError(`${outcome.code}: ${outcome.message}`);
    } else if (outcome.kind === "outcome_unknown") {
      // The send boundary was crossed but acceptance cannot be proven: the
      // run may still be executing upstream. Keep the user turn visible and
      // mark it UNCERTAIN, bound to the returned run_id (cleared only by
      // matching execution evidence / a definitive terminal — never by an
      // unrelated new run, TASK 24 §9), show an explanatory error — never a
      // blind resend. The backend keeps the workspace reserved until
      // reconciliation.
      markUserMessageUncertain(sessionId, userMsgId, outcome.run_id);
      onError(
        `Send outcome unknown: ${outcome.message} — the run may still be executing; it will reconcile from the engine stream.`,
      );
    }
    return outcome;
  } catch (e) {
    // A typed pre-send error (context mismatch, busy, not ready, engine
    // error): the turn was definitely not accepted. Drop it and surface the
    // reason (TASK 24 §9).
    removeLocalUserMessage(sessionId, userMsgId);
    onError(String(e));
    return null;
  }
}

export type ComposerKeyAction = "send" | "queue" | "newline" | "none";

export function composerActionForKey(
  event: { key: string; shiftKey: boolean; ctrlKey: boolean },
  enterQueues: boolean,
): ComposerKeyAction {
  if (event.key !== "Enter") return "none";
  if (event.shiftKey) return "newline";
  if (event.ctrlKey) return enterQueues ? "send" : "queue";
  return enterQueues ? "queue" : "send";
}

export function ownsAutoCreatedSession(
  current: Pick<AppState, "activeSessionId" | "currentWorkspaceId" | "selectedEngineId">,
  created: AppState["sessions"][number],
  expectedWorkspaceId: string,
  expectedEngineId: string,
): boolean {
  return current.currentWorkspaceId === expectedWorkspaceId &&
    current.selectedEngineId === expectedEngineId &&
    created.workspace_id === expectedWorkspaceId &&
    created.engine_id === expectedEngineId &&
    (current.activeSessionId === null || current.activeSessionId === created.id);
}

export function Composer({ state, onError }: Props) {
  // PERF-022: draft lives in component-local state — no global-store
  // mutation per keystroke. Prompt-size policy belongs to the typed backend
  // boundary; the UI never silently truncates user text.
  const [localDraft, setLocalDraft] = useState("");
  const draft = localDraft;
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  // W2-011: monotonic draft ownership generation — prevents a delayed
  // send/enqueue completion from clearing a newer user-edited draft.
  const draftGen = useRef(0);
  const runningRunId = state.activeSessionId ? (state.running[state.activeSessionId] ?? null) : null;

  useEffect(() => {
    textareaRef.current?.focus();
  }, [state.activeSessionId]);

  // T-035: one-shot append requests from other panels (Files "copy path").
  // The draft stays local; the event only APPENDS (never overwrites/clears)
  // and refocuses so the inserted path is immediately editable.
  useEffect(() => {
    const onAppend = (e: Event) => {
      const text = (e as CustomEvent<string>).detail;
      if (typeof text !== "string" || text.length === 0) return;
      draftGen.current++;
      setLocalDraft((prev) => appendDraft(prev, text));
      textareaRef.current?.focus();
    };
    window.addEventListener(COMPOSER_APPEND_EVENT, onAppend);
    return () => window.removeEventListener(COMPOSER_APPEND_EVENT, onAppend);
  }, []);

  // ONE eligibility source (app/eligibility.ts): the same verdict renders the
  // button AND is enforced in the handler. `runningStale` therefore actually
  // blocks a send instead of only explaining why it would be unsafe.
  const send$ = sendAvailability(state);
  const queue$ = queueMutationAvailability(state);
  const cancel$ = cancelAvailability(state);
  const hasDraft = draft.trim().length > 0;

  // Synchronous single-flight latches (app/singleFlight.ts): the guard is a ref
  // flipped BEFORE the await, so two same-tick activations cannot both reach the
  // backend. `busy` is presentation only.
  const sending = useSingleFlight();
  const enqueuing = useSingleFlight();

  const send = useCallback(() => {
    const prompt = draft.trim();
    if (!prompt) return;
    // W2-011: capture ownership generation before the async gap.
    const myGen = draftGen.current;
    void sending.run(async () => {
      // Auto-create session if none is active (user request: "пусть сессию создаёт автоматически").
      // The session is created with the current workspace/engine/model and becomes the active session.
      let sessionId = state.activeSessionId;
      if (!sessionId) {
        if (!state.currentWorkspaceId || !state.selectedEngineId) {
          onError("Open a project and select an engine first");
          return;
        }
        const creationWorkspaceId = state.currentWorkspaceId;
        const creationEngineId = state.selectedEngineId;
        try {
          const s = await commands.createSession(
            creationEngineId,
            creationWorkspaceId,
            state.selectedModelId,
          );
          const current = store.getState();
          if (!ownsAutoCreatedSession(current, s, creationWorkspaceId, creationEngineId)) {
            // The durable session still belongs to its original project and
            // will appear there on reconciliation. Never project or send it
            // through a newer workspace/engine selection.
            if (current.activeSessionId === s.id && current.selectedEngineId !== creationEngineId) {
              activateSession(null);
            }
            onError("Project, engine, or active session changed while creating the session; the prompt was not sent");
            return;
          }
          // Project the new session as active; the store's session list will be reconciled
          // by the event stream, but we set the active id immediately for the send below.
          upsertSession(s);
          activateSession(s.id);
          sessionId = s.id;
        } catch (e) {
          onError(String(e));
          return;
        }
      }
      // Domain enforcement, not `disabled`: a keyboard path or a stale render
      // must not be able to bypass the eligibility verdict.
      // Re-read state after the potential auto-create/start above.
      const cur = store.getState();
      const sendCheck = sendAvailability(cur);
      if (!sendCheck.allowed) {
        if (sendCheck.reason) onError(sendCheck.reason);
        return;
      }
      const outcome = await performSend(
        sessionId!,
        cur.currentWorkspaceId,
        cur.selectedEngineId,
        prompt,
        cur.selectedModelId,
        onError,
      );
      // W2-011: only clear draft if this send still owns it.
      if (myGen !== draftGen.current) return; // user typed a new draft
      // CORE-010: clear draft only if it was successfully accepted or crossed the boundary (uncertain).
      if (outcome && (outcome.kind === "accepted" || outcome.kind === "outcome_unknown")) {
        setLocalDraft("");
      }
    });
  }, [draft, state.activeSessionId, state.currentWorkspaceId, state.selectedEngineId, state.selectedModelId, send$, sending, onError]);

  const queue = useCallback(() => {
    const prompt = draft.trim();
    if (!prompt) return;
    // The SAME queue policy the Queue panel uses — never a second enqueue
    // route that bypasses the stale/read-only rule.
    if (!queue$.allowed || !state.currentWorkspaceId || !state.selectedEngineId) {
      onError(queue$.reason ?? "select a workspace and engine first");
      return;
    }
    const workspaceId = state.currentWorkspaceId;
    const engineId = state.selectedEngineId;
    // W2-011: capture ownership generation before the async gap.
    const myGen = draftGen.current;
    void enqueuing.run(async () => {
      try {
        await commands.queueEnqueue({
          workspaceId,
          engineId,
          sessionId: state.activeSessionId,
          sessionMode: state.activeSessionId ? "existing" : "new",
          model: state.selectedModelId,
          payload: prompt,
        });
        // W2-011: only clear draft if this enqueue still owns it.
        if (myGen === draftGen.current) {
          setLocalDraft("");
        }
      } catch (e) {
        onError(String(e));
      }
    });
  }, [draft, state.activeSessionId, state.currentWorkspaceId, state.selectedEngineId, state.selectedModelId, queue$, enqueuing, onError]);

  async function stopRun() {
    if (!state.activeSessionId || !runningRunId || !cancel$.allowed) {
      if (cancel$.reason) onError(cancel$.reason);
      return;
    }
    try {
      await commands.cancelRun(state.activeSessionId, runningRunId);
    } catch (e) {
      onError(String(e));
    }
  }

  const canSend = hasDraft && send$.allowed;
  const canQueue = hasDraft && queue$.allowed;
  const sendDisabledReason = !hasDraft ? "Type a prompt first" : send$.reason;
  const appBusy = state.lifecycle !== "ready";

  return (
    <footer className="composer">
      <div className="composer__row">
        <textarea
          ref={textareaRef}
          className="composer__input"
          placeholder={
            state.activeSessionId
              ? state.enterQueues
                ? "Send a prompt…  (Enter to queue · Shift+Enter newline · Ctrl+Enter to send)"
                : "Send a prompt…  (Enter to send · Shift+Enter newline · Ctrl+Enter to queue)"
              : state.enterQueues
                ? "Enter queues; Ctrl+Enter sends and creates a session automatically"
                : "Enter sends and creates a session automatically; Ctrl+Enter queues"
          }
          value={draft}
          disabled={!state.currentWorkspaceId || !state.selectedEngineId || appBusy}
          onChange={(e) => { draftGen.current++; setLocalDraft(e.target.value); }}
          onKeyDown={(e) => {
            const action = composerActionForKey(e, state.enterQueues);
            if (action === "newline" || action === "none") return;
            e.preventDefault();
            if (action === "queue") queue();
            else send();
          }}
          rows={3}
          aria-label="Prompt composer"
        />
        <div className="composer__actions">
          {runningRunId && cancel$.allowed ? (
            <button className="btn btn--danger" onClick={stopRun} title="Cancel the active run (the engine keeps running)">
              Cancel run
            </button>
          ) : (
            <>
              <button
                className="btn"
                onClick={queue}
                disabled={!canQueue || enqueuing.busy}
                title={
                  queue$.reason ??
                  `Enqueue through the durable queue (${state.enterQueues ? "Enter" : "Ctrl+Enter"}) — works without a session or a running engine`
                }
              >
                Queue {state.enterQueues ? "↵" : "⌃↵"}
              </button>
              <button
                className="btn btn--primary"
                onClick={send}
                disabled={!canSend || sending.busy}
                title={sendDisabledReason ?? `Send (${state.enterQueues ? "Ctrl+Enter" : "Enter"})`}
              >
                Send {state.enterQueues ? "⌃↵" : "↵"}
              </button>
            </>
          )}
        </div>
      </div>
      <label className="composer__preference">
        <input
          type="checkbox"
          checked={state.enterQueues}
          onChange={(e) => setEnterQueues(e.target.checked)}
        />
        Enter queues (Ctrl+Enter sends)
      </label>
      {!canSend && !runningRunId && sendDisabledReason && (
        <div className="composer__hint muted">{sendDisabledReason}</div>
      )}
      {runningRunId && state.runningStale && (
        <div className="composer__hint muted">
          Run status unknown — Cancel is disabled until a fresh authoritative read succeeds.
        </div>
      )}
    </footer>
  );
}

export { sendDisabledReasonFor } from "../app/eligibility";
