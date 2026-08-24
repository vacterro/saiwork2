import type { SliceProps } from "../state/slices";

/** One definition of what this line consumes (state/slices.ts). */
export const statusLineKeys = ["backend", "lifecycle", "log", "running", "version"] as const;

/** Thin bottom line (TASK 16 §69): application-level facts only — the last
 * meaningful (non-streaming) event, lifecycle, backend/version. Never per
 * token: the log excludes streaming noise, so this line only moves on real
 * state changes. */
export function StatusLine({ state }: SliceProps<(typeof statusLineKeys)[number]>) {
  const runningCount = Object.values(state.running).filter((r) => r !== null).length;
  const recent = state.log[state.log.length - 1];

  return (
    <div className="statusline">
      <span className={`status-dot status-dot--${state.backend === "connected" ? "ready" : "unknown"}`} />
      <span className="statusline__item">
        {state.backend === "connected" ? `v${state.version ?? "?"}` : "backend disconnected"}
      </span>
      <span className="statusline__item">{state.lifecycle.replace("_", " ")}</span>
      <span className="statusline__item">{runningCount > 0 ? `${runningCount} run(s) active` : "idle"}</span>
      {recent && (
        <span className="statusline__event muted" title={recent.message || recent.type}>
          {recent.type}
          {recent.message ? ` — ${recent.message}` : ""}
        </span>
      )}
    </div>
  );
}
