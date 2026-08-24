import { memo, useCallback, useEffect } from "react";
import type { CSSProperties } from "react";
import { store, useAppState } from "../state/store";
import { pickSlice, sliceEqual } from "../state/slices";
import { startFrontendSession } from "./frontendSync";
import { TitleBar, titleBarKeys } from "../components/TitleBar";
import { ProjectSidebar, projectSidebarKeys } from "../components/ProjectSidebar";
import { SessionList, sessionListKeys } from "../components/SessionList";
import { Conversation, conversationKeys, conversationEqual } from "../components/Conversation";
import { Composer, composerKeys } from "../components/Composer";
import { SaipenBar, saipenBarKeys } from "../components/SaipenBar";
import { Dock, dockKeys, dockEqual } from "../components/dock/Dock";
import { activityPanelKeys } from "../components/ActivityPanel";
import { queuePanelKeys } from "../components/QueuePanel";
import { filesPanelKeys } from "../components/FilesPanel";
import { ThreadTabs, threadTabsKeys } from "../components/ThreadTabs";
import { StatusLine, statusLineKeys } from "../components/StatusLine";

export function App() {
  const state = useAppState();

  // Stable identity: a new onError per render would defeat every child memo
  // comparator. The patch is idempotent, so the closure captures nothing that
  // changes.
  const onError = useCallback((message: string) => {
    // PERF-006: the original error toast appears immediately. Diagnostics are
    // NOT eagerly requested here — every command failure used to trigger a
    // nontrivial DB/session/lock snapshot and another global store update whose
    // result was dead data (no reader existed). The diagnostics panel fetches
    // authoritative owner state on demand when opened/refreshed.
    store.patch((s) => ({ ...s, lastError: message }));
  }, []);

  // ONE frontend session per mounted App (T-033). NO one-shot ref guard: React
  // StrictMode deliberately runs setup → cleanup → setup in development, and a
  // `booted.current` guard made the second setup skip resubscribing — leaving
  // the development UI with ZERO live event subscription. This effect is
  // setup/cleanup/setup safe: each setup creates exactly one subscription, the
  // cleanup disposes exactly that one, and the expensive cold bootstrap is
  // idempotent inside frontendSync (single-flight), not suppressed here.
  useEffect(() => {
    const session = startFrontendSession(onError);
    return session.dispose;
  }, [onError]);

  // Window title: static per project — never per-token (§134).
  const workspace = state.workspaces.find((w) => w.id === state.currentWorkspaceId) ?? null;
  useEffect(() => {
    document.title = workspace ? `SAIWORK2 — ${workspace.name}` : "SAIWORK2";
  }, [workspace?.id, workspace?.name]);

  return (
    <div className="app">
      <MemoTitleBar state={pickSlice(state, titleBarKeys)} onError={onError} />
      <MemoThreadTabs state={pickSlice(state, threadTabsKeys)} onError={onError} />
      <div
        className="app__main"
        style={{ "--dock-width": (state.dockCollapsed ? 46 : state.dockWidth) + "px" } as CSSProperties}
      >
        <div className="app__nav">
          <MemoProjectSidebar state={pickSlice(state, projectSidebarKeys)} onError={onError} />
          <MemoSessionList state={pickSlice(state, sessionListKeys)} onError={onError} />
        </div>
        <MemoConversation state={pickSlice(state, conversationKeys)} onError={onError} />
        <MemoDock
          state={pickSlice(state, dockKeys)}
          activity={{ state: pickSlice(state, activityPanelKeys), onError }}
          queue={{ state: pickSlice(state, queuePanelKeys), onError }}
          files={{ state: pickSlice(state, filesPanelKeys), onError }}
          onError={onError}
        />
      </div>
      <MemoComposer state={pickSlice(state, composerKeys)} onError={onError} />
      <MemoSaipenBar state={pickSlice(state, saipenBarKeys)} onError={onError} />
      <MemoStatusLine state={pickSlice(state, statusLineKeys)} onError={onError} />
      {state.backend === "disconnected" && (
        <div className="banner">
          Backend not connected — run <code>npm run tauri dev</code> from the repo root. Web-dev mode shows no
          fabricated state.
        </div>
      )}
      {state.lifecycle !== "ready" && state.backend === "connected" && (
        <div className="banner banner--lifecycle">
          Application: {state.lifecycle.replace("_", " ")}
        </div>
      )}
      {state.lastError && <ErrorToast message={state.lastError} onDismiss={() => store.patch((s) => ({ ...s, lastError: null }))} />}
    </div>
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

// ---- slice-aware memoization ----
//
// App is the single store subscriber, so every batched token update would
// otherwise rerender the whole shell. Each panel declares ONE key tuple
// (co-located with the component) which simultaneously types its props and
// generates its comparator (state/slices.ts) — the previous hand-maintained
// comparator lists were a second dependency system that had already drifted
// from what the components read. Two panels additionally opt out of pure text
// churn with a domain rule (Conversation windows the transcript itself; the
// dock's Activity view displays only non-text facts).

const MemoTitleBar = memo(TitleBar, sliceEqual(titleBarKeys));
const MemoProjectSidebar = memo(ProjectSidebar, sliceEqual(projectSidebarKeys));
const MemoSessionList = memo(SessionList, sliceEqual(sessionListKeys));
const MemoThreadTabs = memo(ThreadTabs, sliceEqual(threadTabsKeys));
const MemoConversation = memo(Conversation, conversationEqual);
const MemoComposer = memo(Composer, sliceEqual(composerKeys));
const MemoSaipenBar = memo(SaipenBar, sliceEqual(saipenBarKeys));
const MemoStatusLine = memo(StatusLine, sliceEqual(statusLineKeys));
const MemoDock = memo(Dock, dockEqual);
