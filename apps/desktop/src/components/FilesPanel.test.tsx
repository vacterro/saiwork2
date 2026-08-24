// FilesPanel (T-035) tests, repo test conventions: node env + renderToString
// (no jsdom). Behavior split: pure helpers get direct tests; SSR proves the
// structural contract (honest empty states, dead rows for non-openable
// entries); the async fetch paths are generation-guarded effects whose
// commands are thin typed wrappers over the audited Tauri surface.
import { describe, expect, it } from "vitest";
import { renderToString } from "react-dom/server";
import type { FileEntry } from "@saiwork2/contracts";
import { FilesPanel, EntryRow, breadcrumbSegments } from "./FilesPanel";
import { appendDraft, requestComposerAppend } from "./composerBridge";

function entry(patch: Partial<FileEntry> = {}): FileEntry {
  return {
    name: "a.txt",
    rel_path: "a.txt",
    kind: "file",
    size: 12,
    modified_ms: 1,
    navigable: true,
    ...patch,
  };
}

describe("files panel path helpers", () => {
  it("root has no breadcrumb segments; nested dirs split on forward slashes", () => {
    expect(breadcrumbSegments(".")).toEqual([]);
    expect(breadcrumbSegments("src")).toEqual(["src"]);
    expect(breadcrumbSegments("crates/saiwork-core/src")).toEqual([
      "crates",
      "saiwork-core",
      "src",
    ]);
  });
});

describe("composer append bridge (copy path to composer)", () => {
  it("appends to an empty draft without a leading space", () => {
    expect(appendDraft("", "src/lib.rs")).toBe("src/lib.rs");
  });
  it("joins with exactly one space", () => {
    expect(appendDraft("look at", "src/lib.rs")).toBe("look at src/lib.rs");
  });
  it("preserves a draft beyond the old 4096-character bottleneck", () => {
    const draft = appendDraft("x".repeat(5000), "more");
    expect(draft.length).toBe(5005);
    expect(draft.endsWith(" more")).toBe(true);
  });
  it("requestComposerAppend is a safe no-op outside a DOM window", () => {
    // In node there is no `window`; the guard must not throw.
    expect(() => requestComposerAppend("src/lib.rs")).not.toThrow();
  });
});

function rowHtml(e: FileEntry, selected = false): string {
  return renderToString(
    <EntryRow entry={e} selected={selected} onNavigate={() => {}} onOpen={() => {}} />,
  );
}

describe("entry rows (read-only honesty rules)", () => {
  it("an openable file renders a live open button and a copy-path button", () => {
    const html = rowHtml(entry());
    expect(html).toContain("files__name");
    expect(html).not.toContain("files__name--dead");
    expect(html).toContain("files__copy");
    expect(html).toContain(">a.txt<");
  });

  it("a non-navigable entry (W2-007) is a dead name with no copy path", () => {
    const html = rowHtml(entry({ navigable: false, rel_path: "", name: "\uFFFDbad" }));
    expect(html).toContain("files__name--dead");
    expect(html).not.toContain("files__copy");
    expect(html).toContain("not valid UTF-8");
  });

  it("a symlink entry is listed but never openable and says why", () => {
    const html = rowHtml(entry({ kind: "symlink", rel_path: "link", name: "link" }));
    expect(html).toContain("files__name--dead");
    expect(html).not.toContain("files__copy");
    expect(html).toContain("never followed");
  });

  it("selection highlights only the selected row", () => {
    expect(rowHtml(entry(), true)).toContain("files__row--selected");
    expect(rowHtml(entry(), false)).not.toContain("files__row--selected");
  });
});

describe("FilesPanel structure (SSR)", () => {
  it("without a workspace it shows the honest empty state and fetches nothing", () => {
    const html = renderToString(
      <FilesPanel state={{ currentWorkspaceId: null }} onError={() => {}} />,
    );
    expect(html).toContain("Open a project");
    expect(html).not.toContain("files__list");
  });

  it("with a workspace it renders the breadcrumb bar before the first listing lands", () => {
    const html = renderToString(
      <FilesPanel state={{ currentWorkspaceId: "w1" }} onError={() => {}} />,
    );
    expect(html).toContain("files__crumbs");
    expect(html).toContain("root");
  });
});
