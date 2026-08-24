import { useEffect, useRef, useState } from "react";
import type { SliceProps } from "../state/slices";
import { commands } from "../app/backend";
import { saipenActionAvailability, saipenFreshness, mutationsAllowed } from "../app/eligibility";
import { useSingleFlight } from "../app/singleFlight";
import { requestSaipenRefresh } from "../app/saipenProjection";
import type { BoardSummary, FileEntry, SaipenActionStatus, SaipenState } from "@saiwork2/contracts";
import { store, type AppState } from "../state/store";

/**
 * CORE-005: Board / Knowledge are VIEW actions, not CLI actions. They must
 * route through a real local read/navigation contract — never a synthetic
 * "success" toast and never `saipenActionStart` (which would spawn a process
 * for something that has no canonical command). The board projection is the
 * authoritative current BOARD.md snapshot already owned by `saipenProjection`;
 * Knowledge is a bounded, workspace-scoped read of the canonical
 * `.saipen/KNOWLEDGE.md` (falling back to a bounded `.saipen/` listing) via the
 * hardened Phase-C read contract (symlink-safe, size-bounded).
 */
export type SaipenView =
  | { kind: "board"; board: BoardSummary; workspaceId: string }
  | { kind: "knowledge"; text: string; path: string; workspaceId: string }
  | { kind: "knowledge-dir"; entries: FileEntry[]; path: string; missing: boolean; workspaceId: string };

export type SaipenActionRoute =
  | { route: "exec" }
  | { route: "view"; view: SaipenView };

const KNOWLEDGE_REL = ".saipen/KNOWLEDGE.md";
const KNOWLEDGE_DIR = ".saipen";

/**
 * The single routing decision for a SAIPEN action. Pure and testable: it
 * either returns a real typed view payload (consumed by `runAction` to open
 * the drawer) or declares the action executable (Status / Validate stay on the
 * CLI lifecycle). `saipen` is the authoritative current projection; `board`
 * comes straight from it, so the Board view is never fabricated.
 */
export async function routeSaipenAction(opts: {
  action: string;
  workspaceId: string;
  saipen: SaipenState | null;
}): Promise<SaipenActionRoute> {
  if (opts.action === "board") {
    const board = opts.saipen?.board ?? { sections: {}, counts: {} };
    return { route: "view", view: { kind: "board", board, workspaceId: opts.workspaceId } };
  }
  if (opts.action === "knowledge") {
    try {
      const preview = await commands.filesReadPreview(opts.workspaceId, KNOWLEDGE_REL);
      return { route: "view", view: { kind: "knowledge", text: preview.text, path: KNOWLEDGE_REL, workspaceId: opts.workspaceId } };
    } catch {
      // No single KNOWLEDGE.md (or it is binary): bounded listing of the
      // .saipen/ dir, surfacing only KNOWLEDGE* entries — still workspace-scoped.
      const listing = await commands.filesListDir(opts.workspaceId, KNOWLEDGE_DIR);
      const entries = listing.entries.filter((e) => e.name.toUpperCase().startsWith("KNOWLEDGE"));
      return { route: "view", view: { kind: "knowledge-dir", entries, path: KNOWLEDGE_DIR, missing: entries.length === 0, workspaceId: opts.workspaceId } };
    }
  }
  // Status / Validate: executable CLI lifecycle (spawns the canonical tool).
  return { route: "exec" };
}

/** One definition of what the SAIPEN strip consumes (state/slices.ts). */
export const saipenBarKeys = [
  "currentWorkspaceId",
  "workspaces",
  "saipenRevision",
  "saipenStale",
  "saipen",
] as const;

type Props = SliceProps<(typeof saipenBarKeys)[number]>;

/**
 * SAIPENBAR strip (TASK 15 §47–§57 + TASK 16 §53–§55): a compact operational
 * summary, never half the window. The SAIPEN projection is owned by
 * `saipenProjection` (the canonical store projection: `saipen` + `saipenStale`),
 * so this strip only reads it and triggers refreshes. The strip is FAIL-CLOSED
 * on stale: when the probe clock is exceeded it renders `STALE` and gates every
 * mutating action behind `saipenActionAvailability`, so a silent backend lag
 * never lets the strip issue commands against an unverified SAIPEN state.
 * `continue` is disabled with the exact reason (no canonical CLI exists in the
 * verified contract); `stop` cancels SAIWORK2-owned action processes only,
 * enabled while an action runs.
 */
export function SaipenBar({ state, onError }: Props) {
  // The canonical SAIPEN projection lives in the store; we only trigger its
  // single owner here (no second local snapshot — T-049).
  const [actionStatus, setActionStatus] = useState<SaipenActionStatus | null>(null);
  // CORE-005: the open SAIPEN view (Board / Knowledge) — real read contract,
  // not a synthetic success. Local to the strip; closed on navigation away.
  const [view, setView] = useState<SaipenView | null>(null);
  // Single-flight guard for the START invoke only (T-015): prevents a second
  // concurrent start click, but is NOT the same as "an action is running". The
  // authoritative running status (actionStatus.running) — plus this just-invoked
  // flag — is what makes Stop reachable.
  const starting = useSingleFlight();
  const workspaceId = state.currentWorkspaceId;

  // Trigger the canonical SAIPEN projection owner on workspace/revision change.
  useEffect(() => {
    requestSaipenRefresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, state.saipenRevision]);

  // actionStatus fetch: single-flight + dirty follow-up + generation, so a
  // revision bump that lands during an in-flight read is never dropped and a
  // slow read for a previous workspace cannot clobber the current one (T-049).
  const statusGen = useRef(0);
  const statusInFlight = useRef(false);
  const statusDirty = useRef(false);
  const statusTarget = useRef<{ workspaceId: string; gen: number } | null>(null);

  // Clear actionStatus AND view immediately on workspace change so stale
  // targets are not shown (W2-013: view must not survive workspace switch).
  const lastWorkspaceId = useRef<string | null>(null);
  if (workspaceId !== lastWorkspaceId.current) {
    lastWorkspaceId.current = workspaceId ?? null;
    setActionStatus(null);
    setView(null);
  }

  useEffect(() => {
    if (!workspaceId) {
      setActionStatus(null);
      statusTarget.current = null;
      return;
    }
    const myGen = ++statusGen.current;
    statusTarget.current = { workspaceId, gen: myGen };

    const run = () => {
      if (statusInFlight.current) {
        statusDirty.current = true;
        return;
      }
      statusInFlight.current = true;
      const target = statusTarget.current;
      if (!target) {
        statusInFlight.current = false;
        return;
      }

      commands
        .saipenActionStatus(target.workspaceId)
        .then(
          (st) => {
            if (target.gen === statusGen.current) setActionStatus(st);
          },
          (e) => {
            if (target.gen === statusGen.current) onError(String(e));
          },
        )
        .finally(() => {
          statusInFlight.current = false;
          if (statusDirty.current) {
            statusDirty.current = false;
            run();
          }
        });
    };
    run();
  }, [workspaceId, state.saipenRevision, onError]);

  async function runAction(action: string) {
    // W2-008: no SAIPEN action while the app is shutting down (the backend
    // rejects it too, but the UI must not invite it).
    if (!mutationsAllowed(store.getState())) return;
    if (
      !workspaceId ||
      !saipenActionAvailability(state as AppState, actionStatus, action).allowed
    )
      return;
    // CORE-005: View actions navigate via the real local read contract — they
    // must NOT spawn a process or claim a synthetic success.
    if (action === "board" || action === "knowledge") {
      // W2-013: capture workspace before async gap so a slow Knowledge read
      // for workspace A cannot reopen A content after switching to B.
      const capturedWs = workspaceId;
      try {
        const routed = await routeSaipenAction({ action, workspaceId: capturedWs, saipen: state.saipen });
        // W2-013: only commit the view if the workspace hasn't changed.
        if (store.getState().currentWorkspaceId !== capturedWs) return;
        if (routed.route === "view") setView(routed.view);
      } catch (e) {
        onError(String(e));
      }
      return;
    }
    void starting.run(async () => {
      try {
        await commands.saipenActionStart(workspaceId, action);
      } catch (e) {
        onError(String(e));
      }
    });
  }

  async function stopAction() {
    if (!workspaceId || (!starting.busy && !running)) return;
    try {
      await commands.saipenActionCancel(workspaceId);
    } catch (e) {
      onError(String(e));
    }
  }

  // Authoritative running status is independent of the unresolved start invoke:
  // Stop must be reachable exactly when an action is in progress (just invoked
  // OR confirmed running), not gated on the single-flight `starting` flag alone.
  const running = actionStatus?.running;
  const actionActive = starting.busy || Boolean(running);
  const hasSaipen = Boolean(state.saipen);
  const freshness = saipenFreshness(state as AppState);
  const fresh = freshness === "fresh";
  const canAct =
    fresh && saipenActionAvailability(state as AppState, actionStatus).allowed && mutationsAllowed(store.getState());
  const isUntrusted = actionStatus?.availability?.disabled_reason?.includes("not trusted") ?? false;
  const isDisabled = actionStatus?.availability?.disabled_reason != null;

  return (
    <footer className="saipenbar">
      {freshness === "stale" && (
        <span
          className="saipenbar__stale"
          role="status"
          title="The last authoritative SAIPEN probe failed — the shown fields may be outdated and every action is gated until a fresh read succeeds."
        >
          ⚠ SAIPEN STALE
        </span>
      )}
      <div className="saipenbar__fields">
        {freshness === "none" ? (
          <span className="saipenbar__value saipenbar__value--unknown">SAIPEN — no project open</span>
        ) : freshness === "absent" ? (
          <span className="saipenbar__value saipenbar__value--unknown">SAIPEN — no .saipen/ state in this project</span>
        ) : !hasSaipen ? (
          <span className="saipenbar__value saipenbar__value--unknown">SAIPEN not initialized</span>
        ) : (
          <>
            {/* PROJECT: the canonical STATE `project` wins; when absent, fall
             * back to the workspace folder name — "UNKNOWN" was a lie for a
             * project we demonstrably opened (T-081). */}
            <Field
              label="PROJECT"
              value={
                state.saipen!.project ??
                (state.workspaces.find((w) => w.id === state.currentWorkspaceId)?.name ?? null)
              }
            />
            <Field label="STATE" value={state.saipen!.phase} />
            <Field label="TASK" value={state.saipen!.task} />
            <Field label="NEXT" value={state.saipen!.next_action} />
            <Field label="BLOCKER" value={state.saipen!.blocker} />
            <Field label="WATCH" value={state.saipen!.watch_status === "live" ? "live" : "failed"} />
          </>
        )}
        <ValidationField status={actionStatus} />
        {isUntrusted && workspaceId && (
          <button
            className="btn btn--small"
            title="This SAIPEN install is outside the project and is not explicitly trusted. Trust it once to enable executable actions (Status/Validate)."
            onClick={() => {
              commands.setSaipenTrustedHome(workspaceId).catch(onError);
            }}
          >
            Trust SAIPEN install
          </button>
        )}
        {running && (
          <span className="saipenbar__value saipenbar__value--action">
            {running.action}: {running.state}
            {running.error ? ` — ${running.error}` : ""}
          </span>
        )}
      </div>
      <div className="saipenbar__actions">
        <button
          className="btn btn--small"
          disabled
          title="No canonical `saipen continue` command exists in the verified SAIPEN contract (v7.224.3) — Continue is the agent's own protocol instruction."
        >
          Continue
        </button>
        <button className="btn btn--small" disabled={!canAct || actionActive || isDisabled} title={isDisabled ? (actionStatus?.availability?.disabled_reason ?? "Executable actions are disabled") : canAct ? "Run canonical saipen.py status" : (fresh ? "An action is already running" : "SAIPEN probe is stale — wait for the next refresh")} onClick={() => void runAction("status")}>
          Status
        </button>
        <button className="btn btn--small" disabled={!canAct || actionActive || isDisabled} title={canAct ? "Show the current canonical board snapshot" : (fresh ? "An action is already running" : "SAIPEN probe is stale — wait for the next refresh")} onClick={() => void runAction("board")}>
          Board
        </button>
        <button className="btn btn--small" disabled={!canAct || actionActive || isDisabled} title={canAct ? "Open the canonical KNOWLEDGE view" : (fresh ? "An action is already running" : "SAIPEN probe is stale — wait for the next refresh")} onClick={() => void runAction("knowledge")}>
          Knowledge
        </button>
        <button className="btn btn--small" disabled={!canAct || actionActive || isDisabled} title={isDisabled ? (actionStatus?.availability?.disabled_reason ?? "Executable actions are disabled") : canAct ? "Run the canonical read-only validate.py" : (fresh ? "An action is already running" : "SAIPEN probe is stale — wait for the next refresh")} onClick={() => void runAction("validate")}>
          Validate
        </button>
        <button
          className="btn btn--small"
          disabled={!actionActive}
          title={running ? "Stop the running action" : "No SAIWORK2-owned SAIPEN action is running"}
          onClick={() => void stopAction()}
        >
          Stop
        </button>
      </div>
      {/* W2-013: only render the view if its workspace matches the current one */}
      {view && view.workspaceId === workspaceId && <SaipenViewDrawer view={view} onClose={() => setView(null)} />}
    </footer>
  );
}

function Field({ label, value }: { label: string; value: string | null }) {
  return (
    <span className="saipenbar__field" title={`${label}: ${value ?? "UNKNOWN"}`}>
      <span className="saipenbar__label">{label}</span>
      <span className={`saipenbar__value ${value === null ? "saipenbar__value--unknown" : ""}`}>
        {value ?? "UNKNOWN"}
      </span>
    </span>
  );
}

function ValidationField({ status }: { status: SaipenActionStatus | null }) {
  const result = status?.validation_result ?? null;
  const stale = status?.validation_stale ?? false;
  if (result === null) {
    return (
      <span className="saipenbar__field">
        <span className="saipenbar__label">VALIDATION</span>
        <span className="saipenbar__value">not run</span>
      </span>
    );
  }
  const cls = stale
    ? "saipenbar__value saipenbar__value--unknown"
    : result === "valid"
      ? "saipenbar__value saipenbar__value--ok"
      : "saipenbar__value saipenbar__value--bad";
  return (
    <span className="saipenbar__field">
      <span className="saipenbar__label">VALIDATION</span>
      <span className={cls}>
        {result.toUpperCase()}
        {stale ? " · STALE" : ""}
      </span>
    </span>
  );
}

/**
 * CORE-005: the rendered SAIPEN view. Board shows the authoritative current
 * board projection (sections → ticket ids); Knowledge shows the bounded
 * canonical KNOWLEDGE read, or — when no single KNOWLEDGE.md exists — a bounded
 * listing of KNOWLEDGE* entries under `.saipen/`. This is the real navigation
 * target the Board / Knowledge buttons open; there is no synthetic success.
 */
export function SaipenViewDrawer({ view, onClose }: { view: SaipenView; onClose: () => void }) {
  const title =
    view.kind === "board"
      ? "BOARD"
      : view.kind === "knowledge"
        ? "KNOWLEDGE"
        : "KNOWLEDGE (list)";
  return (
    <div className="saipen-view" role="dialog" aria-label={`SAIPEN ${title} view`}>
      <div className="saipen-view__head">
        <span className="saipen-view__title">{title}</span>
        <button className="btn btn--small" onClick={onClose} title="Close the SAIPEN view">
          Close
        </button>
      </div>
      <div className="saipen-view__body">
        {view.kind === "board" && (
          <div className="saipen-board">
            {Object.keys(view.board.sections).length === 0 ? (
              <p className="muted">No board sections.</p>
            ) : (
              Object.entries(view.board.sections).map(([section, ids]) => (
                <div className="saipen-board__section" key={section}>
                  <h4 className="saipen-board__section-title">
                    {section} ({ids.length})
                  </h4>
                  <ul>
                    {ids.length === 0 ? (
                      <li className="muted">—</li>
                    ) : (
                      ids.map((id) => <li key={id}>{id}</li>)
                    )}
                  </ul>
                </div>
              ))
            )}
          </div>
        )}
        {view.kind === "knowledge" && (
          <div className="saipen-knowledge">
            <p className="saipen-view__path">{view.path}</p>
            <pre className="saipen-knowledge__text">{view.text}</pre>
          </div>
        )}
        {view.kind === "knowledge-dir" && (
          <div className="saipen-knowledge">
            <p className="saipen-view__path">{view.path}/</p>
            {view.missing ? (
              <p className="muted">No KNOWLEDGE material found in {view.path}/.</p>
            ) : (
              <ul>
                {view.entries.map((e) => (
                  <li key={e.rel_path}>
                    {e.name} ({e.kind})
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
