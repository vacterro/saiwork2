import type { AppState, Message } from "../state/store";
import { pickSlice, sliceEqual, type SliceProps } from "../state/slices";

/** One definition of what the ACTIVITY dock tab consumes (state/slices.ts).
 * The Activity view shows ONLY structured, non-text facts (the live run's
 * tools + permission decisions + run status) — never message prose, so the
 * dock does not re-render the transcript. It is session-scoped and reads only
 * `running` + `activeSessionId` + the live stream tail, so it cannot grow
 * unbounded with every streamed token. The full non-text event ring lives in
 * the store `log` and is surfaced by DIAG. */
export const activityPanelKeys = ["running", "activeSessionId", "activeMessage"] as const;

type Props = SliceProps<(typeof activityPanelKeys)[number]>;

/**
 * Text-only churn is invisible to the dock: the tail's text mutates on every
 * streamed delta while its FACTS (run id, status, tools, permissions, error)
 * stay reference-identical — and a reference change in any fact must
 * rerender. Compares shallow keys (running/activeSessionId) by reference and
 * the tail by its structured facts only.
 */
function factsOnly(a: Pick<AppState, (typeof activityPanelKeys)[number]>, b: Pick<AppState, (typeof activityPanelKeys)[number]>): boolean {
  const sid = b.activeSessionId;
  if (!sid) return true;
  const ta = a.activeMessage[sid] ?? null;
  const tb = b.activeMessage[sid] ?? null;
  if (ta === tb) return true;
  if (!ta || !tb) return false;
  return (
    ta.runId === tb.runId &&
    ta.status === tb.status &&
    ta.tools === tb.tools &&
    ta.permissions === tb.permissions &&
    ta.error === tb.error
  );
}

export function activityEqual(prev: Props, next: Props): boolean {
  return sliceEqual(activityPanelKeys, factsOnly, ["running", "activeSessionId"])(prev, next);
}

/**
 * Right activity column (TASK 16 §68): current run/tools/permissions, the
 * durable queue, and redacted diagnostics. The DOCK owns tab selection
 * (`activeDockTab`, persisted) — this panel is a pure projection of one tab's
 * slice and never holds competing tab state.
 */
export function ActivityDockPanel({ state }: Props) {
  return <ActivityTab state={state} />;
}

/** The Activity view is a window over the LIVE run, not the transcript: it
 * renders only structured facts (tools + permissions + run state) for the
 * active session's current stream tail. Free-form message prose is NEVER
 * rendered here — that stays in the Conversation (the user complained the dock
 * duplicated the thread). */
function ActivityTab({ state }: { state: Pick<AppState, (typeof activityPanelKeys)[number]> }) {
  const sessionId = state.activeSessionId;
  if (!sessionId) {
    return <div className="activity__empty muted">No active session.</div>;
  }
  const tail: Message | null = state.activeMessage[sessionId] ?? null;
  const runningRunId = state.running[sessionId] ?? null;
  const tools = tail?.tools ?? [];
  const permissions = tail?.permissions ?? [];
  const hasFacts = tools.length > 0 || permissions.length > 0 || runningRunId !== null;
  if (!hasFacts) {
    return <div className="activity__empty muted">No tool activity.</div>;
  }
  return (
    <div className="activity">
      <div className="activity__run">
        <div className="activity__run-head">
          <span className="label">RUN</span>
          <span className={`status status--${runningRunId ? "streaming" : "idle"}`}>
            {runningRunId ? `running · ${runningRunId.slice(0, 8)}` : "idle"}
          </span>
        </div>
        {tools.length > 0 && (
          <div className="activity__section">
            <span className="label">TOOLS</span>
            {tools.map((t, i) => (
              <div key={`${t.tool}-${i}`} className={`activity__tool activity__tool--${t.status ?? "unknown"}`}>
                <span className="activity__tool-name">{t.tool}</span>
                {t.status ? <span className="activity__tool-status">{t.status}</span> : null}
                {t.error ? <span className="activity__tool-error">{t.error}</span> : null}
              </div>
            ))}
          </div>
        )}
        {permissions.length > 0 && (
          <div className="activity__section">
            <span className="label">PERMISSIONS</span>
            {permissions.map((pe) => (
              <div key={pe.requestId} className="perm">
                <span className="perm__detail">{pe.detail}</span>
                <span className="perm__status">
                  {pe.allowed === null ? "waiting…" : pe.allowed ? "allowed" : "denied"}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export function activitySliceOf(state: AppState) {
  return pickSlice(state, activityPanelKeys);
}