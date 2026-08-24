import { store } from "../state/store";
import { mutationsAllowed } from "../app/eligibility";
import type { SliceProps } from "../state/slices";
import { commands, confirmDialog, pickFolder } from "../app/backend";
import { clearWorkspaceSelection, selectWorkspace } from "../app/workspaceSelection";

/** One definition of what this panel consumes (state/slices.ts). */
export const projectSidebarKeys = ["workspaces", "currentWorkspaceId"] as const;

interface Props extends SliceProps<(typeof projectSidebarKeys)[number]> {
  /** Destructive-action confirmation (T-014). Defaults to the native Tauri
   * dialog and names the project and its metadata; injectable for tests. Must return a
   * Promise<boolean> so a rejection performs zero mutation. */
  confirmForget?: (name: string, path: string, metadata: string) => Promise<boolean>;
}

function defaultConfirmForget(name: string, path: string, metadata: string): Promise<boolean> {
  return confirmDialog(
    `Forget project "${name}"?\n\nPath: ${path}\n${metadata}\n\nThis permanently removes the SAIWORK2 workspace identity and all of its session metadata. This cannot be undone.`,
  );
}

export function ProjectSidebar({ state, onError, confirmForget }: Props) {
  const confirm = confirmForget ?? defaultConfirmForget;
  async function openFolder() {
    const path = await pickFolder(onError);
    if (!path) return;
    // ONE canonical selection path (TASK 24 §9): authoritative open + SAIPEN
    // attach + scoped session/SAIPEN fetch, generation-guarded.
    await selectWorkspace(path, onError);
  }

  async function activateWorkspace(id: string) {
    // Re-open the already-known workspace so its authoritative state (git,
    // SAIPEN) is refreshed; idempotent in the core. Same canonical path as
    // Open… (TASK 24 §9) — workspace switch re-scopes sessions/SAIPEN and a
    // delayed older response can never overwrite a newer selection.
    const workspace = state.workspaces.find((w) => w.id === id);
    if (!workspace) return;
    await selectWorkspace(workspace.path, onError);
  }

  async function removeWorkspace(e: React.MouseEvent, id: string) {
    e.stopPropagation();
    const workspace = state.workspaces.find((w) => w.id === id);
    if (!workspace) return;
    // W2-008: no workspace mutation (forget) while the app is shutting down.
    if (!mutationsAllowed(store.getState())) return;
    // Destructive confirmation (T-014): name the affected project and the
    // metadata being permanently removed. A cancel performs ZERO mutation —
    // no backend call, no UI removal — and the list is only pruned AFTER a
    // successful backend forget.
    const confirmed = await confirm(
      workspace.name,
      workspace.path,
      workspace.saipen ? "This project also has SAIPEN (git/board/log) data." : "This includes all session metadata for the project.",
    );
    if (!confirmed) return;
    try {
      await commands.forgetWorkspace(id);
      // Selection is owned by workspaceSelection.ts: forgetting the CURRENT
      // project goes through its clear (which also takes a new epoch, so an
      // in-flight older selection cannot resurrect the removed project).
      if (store.getState().currentWorkspaceId === id) clearWorkspaceSelection();
      store.patch((s) => ({
        ...s,
        workspaces: s.workspaces.filter((w) => w.id !== id),
      }));
    } catch (err) {
      onError(String(err));
    }
  }

  return (
    <aside className="sidebar">
      <div className="sidebar__header">
        <span>PROJECTS</span>
        <button className="btn btn--small" onClick={openFolder}>
          Open…
        </button>
      </div>
      <ul className="sidebar__list">
        {state.workspaces.map((w) => (
          <li
            key={w.id}
            className={w.id === state.currentWorkspaceId ? "sidebar__item sidebar__item--active" : "sidebar__item"}
          >
            <button className="sidebar__item-name" onClick={() => void activateWorkspace(w.id)} title={w.path}>
              <span className="sidebar__item-title">
                {w.name}
                {w.saipen ? (
                  <span className="sidebar__badge" title="SAIPEN detected">
                    S
                  </span>
                ) : null}
              </span>
              <span className="sidebar__item-path">{w.path}</span>
            </button>
            <button className="sidebar__item-remove" onClick={(e) => void removeWorkspace(e, w.id)} title="Remove project">×</button>
          </li>
        ))}
        {state.workspaces.length === 0 && <li className="sidebar__empty">No projects yet</li>}
      </ul>
    </aside>
  );
}
