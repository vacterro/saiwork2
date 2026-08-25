import { useCallback, useEffect } from "react";
import type { CSSProperties } from "react";
import { store } from "../state/store";
import { sliceSelect, pickEqual } from "../state/slices";
import type { AppState } from "../state/store";
import { useAppSlice } from "../state/useAppSlice";
import { startFrontendSession } from "./frontendSync";
import { TitleBar, titleBarKeys } from "../components/TitleBar";
import { ProjectSidebar, projectSidebarKeys } from "../components/ProjectSidebar";
import { SessionList, sessionListKeys } from "../components/SessionList";
import { Conversation, conversationKeys, conversationEqual } from "../components/Conversation";
import { Composer, composerKeys } from "../components/Composer";
import { SaipenBar, saipenBarKeys } from "../components/SaipenBar";
import { Dock, dockKeys } from "../components/dock/Dock";
import { activityPanelKeys } from "../components/ActivityPanel";
import { queuePanelKeys } from "../components/QueuePanel";
import { filesPanelKeys } from "../components/FilesPanel";
import { ThreadTabs, threadTabsKeys } from "../components/ThreadTabs";
import { StatusLine, statusLineKeys } from "../components/StatusLine";

// AUDIT-PERF-003: selection lives AT the subscription boundary. The root no
// longer subscribes to the ENTIRE AppState (every streamed delta used to
// re-execute the whole App render + 12 pickSlice allocations before any child
// memo could bail out). App itself renders only shell facts that never change
// on ordinary text deltas; every panel subscribes to its own declared key
// tuple through useAppSlice and stays identity-stable otherwise.

type OnError = (message: string) => void;

// ---- module-stable selectors + equalities (one per declared tuple) ----
const titleBarSelect = sliceSelect(titleBarKeys);
const titleBarEqual = pickEqual(titleBarKeys);
const threadTabsSelect = sliceSelect(threadTabsKeys);
const threadTabsEqual = pickEqual(threadTabsKeys);
const projectSidebarSelect = sliceSelect(projectSidebarKeys);
const projectSidebarEqual = pickEqual(projectSidebarKeys);
const sessionListSelect = sliceSelect(sessionListKeys);
const sessionListEqual = pickEqual(sessionListKeys);
const conversationSelect = sliceSelect(conversationKeys);
const composerSelect = sliceSelect(composerKeys);
const composerEqual = pickEqual(composerKeys);
const saipenBarSelect = sliceSelect(saipenBarKeys);
const saipenBarEqual = pickEqual(saipenBarKeys);
const statusLineSelect = sliceSelect(statusLineKeys);
const statusLineEqual = pickEqual(statusLineKeys);
const dockSelect = sliceSelect(dockKeys);
const dockSliceEqual = pickEqual(dockKeys);
const activitySelect = sliceSelect(activityPanelKeys);
const activitySliceEqual = pickEqual(activityPanelKeys);
const queuePanelSelect = sliceSelect(queuePanelKeys);
const queuePanelSliceEqual = pickEqual(queuePanelKeys);
const filesSelect = sliceSelect(filesPanelKeys);
const filesSliceEqual = pickEqual(filesPanelKeys);

/** Facts App itself renders: banners, error toast, dock CSS geometry, window
 * title inputs. None of these change on an ordinary message delta. */
const SHELL_KEYS = [
  "backend",
  "lifecycle",
  "lastError",
  "dockWidth",
  "dockCollapsed",
  "workspaces",
  "currentWorkspaceId",
] as const;
export type ShellKeys = typeof SHELL_KEYS;
export { SHELL_KEYS };
const shellSelect = sliceSelect(SHELL_KEYS);
const shellEqual = pickEqual(SHELL_KEYS);

export function App() {
  // Stable identity: a new onError per render would defeat every panel's
  // equality closure. The patch is idempotent, so the closure captures
  // nothing that changes.
  const onError = useCallback<OnError>((message) => {
    store.patch((s) => ({ ...s, lastError: message }));
  }, []);

  // ONE frontend session per mounted App (T-033). NO one-shot ref guard: React
  // StrictMode deliberately runs setup → cleanup → setup in development. This
  // effect is setup/cleanup/setup safe; the expensive cold bootstrap is
  // idempotent inside frontendSync (single-flight).
  useEffect(() => {
    const session = startFrontendSession(onError);
    return session.dispose;
  }, [onError]);

  const shell = useAppSlice(shellSelect, shellEqual);

  // Window title: static per project — never per-token (§134).
  const workspace =
    shell.workspaces.find((w) => w.id === shell.currentWorkspaceId) ?? null;
  useEffect(() => {
    document.title = workspace ? `SAIWORK2 — ${workspace.name}` : "SAIWORK2";
  }, [workspace?.id, workspace?.name]);

  return (
    <div className="app">
      <LiveTitleBar onError={onError} />
      <LiveThreadTabs onError={onError} />
      <div
        className="app__main"
        style={{ "--dock-width": (shell.dockCollapsed ? 46 : shell.dockWidth) + "px" } as CSSProperties}
      >
        <div className="app__nav">
          <LiveProjectSidebar onError={onError} />
          <LiveSessionList onError={onError} />
        </div>
        <LiveConversation onError={onError} />
        <LiveDock onError={onError} />
      </div>
      <LiveComposer onError={onError} />
      <LiveSaipenBar onError={onError} />
      <LiveStatusLine onError={onError} />
      {shell.backend === "disconnected" && (
        <div className="banner">
          Backend not connected — run <code>npm run tauri dev</code> from the repo root. Web-dev mode shows no
          fabricated state.
        </div>
      )}
      {shell.lifecycle !== "ready" && shell.backend === "connected" && (
        <div className="banner banner--lifecycle">
          Application: {shell.lifecycle.replace("_", " ")}
        </div>
      )}
      {shell.lastError && (
        <ErrorToast
          message={shell.lastError}
          onDismiss={() => store.patch((s) => ({ ...s, lastError: null }))}
        />
      )}
    </div>
  );
}

// ---- self-subscribing panel wrappers ----
//
// Each wrapper owns ONE useAppSlice over its co-located declared keys. A store
// transition that does not touch a tuple keeps the snapshot identity stable,
// so React never even invokes the component. Domain rules are preserved by
// delegating to the SAME equality logic the memo comparators used.

function LiveTitleBar({ onError }: { onError: OnError }) {
  const state = useAppSlice(titleBarSelect, titleBarEqual);
  return <TitleBar state={state} onError={onError} />;
}

function LiveThreadTabs({ onError }: { onError: OnError }) {
  const state = useAppSlice(threadTabsSelect, threadTabsEqual);
  return <ThreadTabs state={state} onError={onError} />;
}

function LiveProjectSidebar({ onError }: { onError: OnError }) {
  const state = useAppSlice(projectSidebarSelect, projectSidebarEqual);
  return <ProjectSidebar state={state} onError={onError} />;
}

function LiveSessionList({ onError }: { onError: OnError }) {
  const state = useAppSlice(sessionListSelect, sessionListEqual);
  return <SessionList state={state} onError={onError} />;
}

function LiveComposer({ onError }: { onError: OnError }) {
  const state = useAppSlice(composerSelect, composerEqual);
  return <Composer state={state} onError={onError} />;
}

function LiveSaipenBar({ onError }: { onError: OnError }) {
  const state = useAppSlice(saipenBarSelect, saipenBarEqual);
  return <SaipenBar state={state} onError={onError} />;
}

function LiveStatusLine({ onError }: { onError: OnError }) {
  const state = useAppSlice(statusLineSelect, statusLineEqual);
  return <StatusLine state={state} onError={onError} />;
}

function LiveConversation({ onError }: { onError: OnError }) {
  // Preserve the domain rule (windowed transcript semantics): equality is the
  // SAME conversationEqual decision, bound to this stable onError.
  const equal = useCallback(
    (a: Pick<AppState, (typeof conversationKeys)[number]>, b: Pick<AppState, (typeof conversationKeys)[number]>) =>
      conversationEqual({ state: a, onError }, { state: b, onError }),
    [onError],
  );
  const state = useAppSlice(conversationSelect, equal);
  return <Conversation state={state} onError={onError} />;
}

function LiveDock({ onError }: { onError: OnError }) {
  const state = useAppSlice(dockSelect, dockSliceEqual);
  const activity = useAppSlice(activitySelect, activitySliceEqual);
  const queue = useAppSlice(queuePanelSelect, queuePanelSliceEqual);
  const files = useAppSlice(filesSelect, filesSliceEqual);
  return (
    <Dock
      state={state}
      activity={{ state: activity, onError }}
      queue={{ state: queue, onError }}
      files={{ state: files, onError }}
      onError={onError}
    />
  );
}

function ErrorToast({ message, onDismiss }: { message: string; onDismiss: () => void }) {
  return (
    <div className="toast toast--error">
      <span>{message}</span>
      <button className="btn btn--small" onClick={onDismiss}>
        ×
      </button>
    </div>
  );
}
