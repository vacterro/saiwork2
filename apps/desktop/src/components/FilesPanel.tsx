import { useCallback, useEffect, useRef, useState } from "react";
import type { DirListing, FileEntry, FilePreview } from "@saiwork2/contracts";
import type { AppState } from "../state/store";
import type { SliceProps } from "../state/slices";
import { pickSlice } from "../state/slices";
import { commands } from "../app/backend";
import { requestComposerAppend } from "./composerBridge";

/** One definition of what the FILES dock tab consumes (state/slices.ts).
 * The panel owns its directory/preview data as component-local state —
 * it is ephemeral UI (UI_UX.md ownership buckets), fetched lazily per
 * navigation and never polled. */
export const filesPanelKeys = ["currentWorkspaceId"] as const;

type Props = SliceProps<(typeof filesPanelKeys)[number]>;

export function filesSliceOf(state: AppState) {
  return pickSlice(state, filesPanelKeys);
}

interface BrowseState {
  rel: string;
  listing: DirListing | null;
  preview: FilePreview | null;
  selectedRel: string | null;
  loading: boolean;
}

const ROOT_REL = ".";

/** Pure path helpers (exported for tests): "." is the workspace root; child
 * segments join with forward slashes — the exact token shape the backend
 * contract uses, so a rendered row's rel_path is always replayable as an arg. */
export function breadcrumbSegments(rel: string): string[] {
  return rel === ROOT_REL ? [] : rel.split("/");
}

/**
 * Read-only workspace Files panel (Phase C / ROADMAP phase C).
 *
 * - The backend resolves the workspace root from WorkspaceId and enforces the
 *   containment contract (traversal + symlink rejection, CORE-008); this panel
 *   only ever sends workspace-relative paths it received FROM the backend
 *   (`entry.rel_path`) — never user-typed paths.
 * - Lazy: exactly one `files_list_dir` per navigation, one `files_read_preview`
 *   per file selection. No polling, no eager subtree reads.
 * - Every response is generation-guarded: a stale response (user navigated on)
 *   is discarded, never rendered.
 * - Non-navigable entries (non-UTF-8 names, W2-007) and symlinks are shown but
 *   never openable — honest UI instead of a guaranteed backend error.
 */
export function FilesPanel({ state, onError }: Props) {
  const workspaceId = state.currentWorkspaceId;

  const [browse, setBrowse] = useState<BrowseState>({
    rel: ROOT_REL,
    listing: null,
    preview: null,
    selectedRel: null,
    loading: false,
  });
  // Monotonic navigation generation: any navigate/workspace-switch bumps it,
  // and every in-flight response checks it before touching state (W2-001
  // spirit — a late response can never clobber newer UI).
  const navGen = useRef(0);

  useEffect(() => {
    // Workspace switched (or cleared): reset to the root unconditionally.
    const myGen = ++navGen.current;
    setBrowse({
      rel: ROOT_REL,
      listing: null,
      preview: null,
      selectedRel: null,
      loading: Boolean(workspaceId),
    });
    if (!workspaceId) return;
    commands
      .filesListDir(workspaceId, ROOT_REL)
      .then((listing) => {
        if (navGen.current !== myGen) return;
        setBrowse((b) => ({ ...b, listing, loading: false }));
      })
      .catch((e) => {
        if (navGen.current !== myGen) return;
        setBrowse((b) => ({ ...b, loading: false }));
        onError(String(e));
      });
  }, [workspaceId, onError]);

  const navigate = useCallback(
    (rel: string) => {
      if (!workspaceId) return;
      const myGen = ++navGen.current;
      setBrowse((b) => ({
        rel,
        listing: b.listing?.dir === rel ? b.listing : null,
        // Navigating away invalidates the open preview context.
        preview: null,
        selectedRel: null,
        loading: true,
      }));
      commands
        .filesListDir(workspaceId, rel)
        .then((listing) => {
          if (navGen.current !== myGen) return;
          setBrowse((b) => ({ ...b, listing, loading: false }));
        })
        .catch((e) => {
          if (navGen.current !== myGen) return;
          setBrowse((b) => ({ ...b, loading: false }));
          onError(String(e));
        });
    },
    [workspaceId, onError],
  );

  const refresh = useCallback(() => {
    if (!workspaceId) return;
    const myGen = ++navGen.current;
    const rel = browse.rel;
    setBrowse((b) => ({ ...b, loading: true }));
    commands
      .filesListDir(workspaceId, rel)
      .then((listing) => {
        if (navGen.current !== myGen) return;
        setBrowse((b) => ({ ...b, listing, loading: false }));
      })
      .catch((e) => {
        if (navGen.current !== myGen) return;
        setBrowse((b) => ({ ...b, loading: false }));
        onError(String(e));
      });
  }, [workspaceId, browse.rel, onError]);

  const openFile = useCallback(
    (entry: FileEntry) => {
      if (!workspaceId || !entry.navigable || entry.kind !== "file") return;
      const myGen = ++navGen.current;
      setBrowse((b) => ({ ...b, selectedRel: entry.rel_path, preview: null }));
      commands
        .filesReadPreview(workspaceId, entry.rel_path)
        .then((preview) => {
          if (navGen.current !== myGen) return;
          setBrowse((b) =>
            b.selectedRel === preview.rel_path ? { ...b, preview } : b,
          );
        })
        .catch((e) => {
          if (navGen.current !== myGen) return;
          onError(String(e));
        });
    },
    [workspaceId, onError],
  );

  if (!workspaceId) {
    return <div className="files__empty muted">Open a project to browse its files.</div>;
  }

  const crumbs = breadcrumbSegments(browse.rel);
  const listing = browse.listing;

  return (
    <div className="files">
      <div className="files__bar">
        <nav className="files__crumbs" aria-label="Directory path">
          <button className="files__crumb" onClick={() => navigate(ROOT_REL)} title={workspaceId}>
            root
          </button>
          {crumbs.map((seg, i) => (
            <span key={`${seg}-${i}`} className="files__crumb-seg">
              <span className="files__sep">/</span>
              <button
                className="files__crumb"
                onClick={() => navigate(i === crumbs.length - 1 ? browse.rel : crumbs.slice(0, i + 1).join("/"))}
              >
                {seg}
              </button>
            </span>
          ))}
        </nav>
        <button className="btn btn--small" onClick={refresh} disabled={browse.loading} title="Re-list this directory">
          ↻
        </button>
      </div>

      {browse.loading && !listing && <div className="files__empty muted">Listing…</div>}
      {!browse.loading && listing && listing.entries.length === 0 && (
        <div className="files__empty muted">Empty directory.</div>
      )}
      {listing && (
        <ul className="files__list">
          {listing.entries.map((e) => (
            <EntryRow
              key={e.rel_path}
              entry={e}
              selected={browse.selectedRel === e.rel_path}
              onNavigate={navigate}
              onOpen={openFile}
            />
          ))}
        </ul>
      )}
      {listing?.truncated && (
        <div className="files__note muted">Directory listing truncated by the backend bound.</div>
      )}

      {browse.preview &&
        (browse.preview.binary ? (
          <div className="files__preview-note muted">
            binary file · {browse.preview.total_bytes} bytes — no text preview
          </div>
        ) : (
          <div className="files__preview">
            <div className="files__preview-head label">{browse.preview.rel_path}</div>
            <pre className="files__preview-text">{browse.preview.text}</pre>
            {browse.preview.truncated && (
              <div className="files__note muted">
                bounded head only · {browse.preview.total_bytes} bytes total
              </div>
            )}
          </div>
        ))}
    </div>
  );
}

export function EntryRow({
  entry,
  selected,
  onNavigate,
  onOpen,
}: {
  entry: FileEntry;
  selected: boolean;
  onNavigate: (rel: string) => void;
  onOpen: (entry: FileEntry) => void;
}) {
  const openable =
    entry.navigable && entry.rel_path.length > 0 && entry.kind !== "symlink";
  const open = () => {
    if (!openable) return;
    if (entry.kind === "dir") onNavigate(entry.rel_path);
    else onOpen(entry);
  };
  const tooltip = !entry.navigable
    ? "not openable (name is not valid UTF-8)"
    : entry.kind === "symlink"
      ? "symlink — never followed (CORE-008)"
      : entry.rel_path;
  return (
    <li className={"files__row" + (selected ? " files__row--selected" : "")}>
      {openable ? (
        <button className="files__name" onClick={open} title={tooltip}>
          <span className={`files__kind files__kind--${entry.kind}`}>
            {entry.kind === "dir" ? "▸" : entry.kind === "symlink" ? "↝" : "·"}
          </span>
          {entry.name}
        </button>
      ) : (
        <span className="files__name files__name--dead" title={tooltip}>
          <span className={`files__kind files__kind--${entry.kind}`}>
            {entry.kind === "dir" ? "▸" : entry.kind === "symlink" ? "↝" : "·"}
          </span>
          {entry.name}
        </span>
      )}
      <span className="files__meta muted">
        {entry.size !== null ? `${entry.size} B` : ""}
      </span>
      {openable ? (
        <button
          className="btn btn--small files__copy"
          title="Insert this workspace-relative path into the composer"
          onClick={() => requestComposerAppend(entry.rel_path)}
        >
          ⧉
        </button>
      ) : null}
    </li>
  );
}
