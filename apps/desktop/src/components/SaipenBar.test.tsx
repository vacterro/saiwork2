// CORE-005 regression (AUDIT CORE): Board / Knowledge are VIEW actions that
// must route through a real local read/navigation contract — never a synthetic
// success and never `saipenActionStart` (which would spawn a process for an
// action with no canonical command). This proves:
//   - Board opens the AUTHORITATIVE current board projection (from the store,
//     which the projection owner read from BOARD.md — not fabricated).
//   - Knowledge performs a BOUNDED, workspace-scoped read of the canonical
//     `.saipen/KNOWLEDGE.md` via the hardened Phase-C read contract, falling
//     back to a bounded `.saipen/` listing when no single file exists.
//   - Status / Validate stay on the executable CLI lifecycle.
//   - The drawer RENDERS the real view (board sections / knowledge text).
import { describe, expect, it, vi, beforeEach } from "vitest";
import { renderToString } from "react-dom/server";
import type { BoardSummary, SaipenState } from "@saiwork2/contracts";
import { SaipenViewDrawer, routeSaipenAction } from "./SaipenBar";
import { commands } from "../app/backend";

const board: BoardSummary = {
  sections: { DOING: ["T-100"], DONE: ["T-99"], BLOCKED: [] },
  counts: { DOING: 1, DONE: 1, BLOCKED: 0 },
};

function saipenWith(boardArg: BoardSummary): SaipenState {
  return {
    generation: 1,
    read_at_ms: 0,
    root: "/w",
    schema_version: "3",
    saipen_version: "7",
    project: "P",
    phase: null,
    task: null,
    next_action: null,
    blocker: null,
    mode: null,
    execution_intent: null,
    agent: null,
    updated: null,
    last_event: null,
    board: boardArg,
    watch_status: "not_watching",
    last_error: null,
    stale: false,
  };
}

describe("CORE-005 SAIPEN view routing", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("Board routes to the authoritative board projection (no process, no synthetic success)", async () => {
    const r = await routeSaipenAction({ action: "board", workspaceId: "w1", saipen: saipenWith(board) });
    expect(r).toEqual({ route: "view", view: { kind: "board", board, workspaceId: "w1" } });
  });

  it("Board with no projection present still yields an (empty) view rather than calling the backend", async () => {
    const r = await routeSaipenAction({ action: "board", workspaceId: "w1", saipen: null });
    expect(r).toEqual({ route: "view", view: { kind: "board", board: { sections: {}, counts: {} }, workspaceId: "w1" } });
  });

  it("Status / Validate stay on the executable CLI lifecycle", async () => {
    expect(await routeSaipenAction({ action: "status", workspaceId: "w1", saipen: saipenWith(board) })).toEqual({ route: "exec" });
    expect(await routeSaipenAction({ action: "validate", workspaceId: "w1", saipen: saipenWith(board) })).toEqual({ route: "exec" });
  });

  it("Knowledge reads the bounded canonical KNOWLEDGE.md via the hardened read contract", async () => {
    commands.filesReadPreview = vi.fn().mockResolvedValue({
      rel_path: ".saipen/KNOWLEDGE.md",
      text: "# KNOWLEDGE\n- rule one\n- rule two",
      truncated: false,
      binary: false,
      total_bytes: 30,
    });
    const r = await routeSaipenAction({ action: "knowledge", workspaceId: "w1", saipen: saipenWith(board) });
    expect(commands.filesReadPreview).toHaveBeenCalledWith("w1", ".saipen/KNOWLEDGE.md");
    expect(r).toEqual({
      route: "view",
      view: { kind: "knowledge", text: "# KNOWLEDGE\n- rule one\n- rule two", path: ".saipen/KNOWLEDGE.md", workspaceId: "w1" },
    });
  });

  it("Knowledge falls back to a bounded .saipen/ listing when KNOWLEDGE.md is absent", async () => {
    commands.filesReadPreview = vi.fn().mockRejectedValue(new Error("not found"));
    commands.filesListDir = vi.fn().mockResolvedValue({
      dir: ".saipen",
      entries: [
        { name: "STATE.md", rel_path: ".saipen/STATE.md", kind: "file", size: 10, modified_ms: 0, navigable: true },
        { name: "KNOWLEDGE.md", rel_path: ".saipen/KNOWLEDGE.md", kind: "file", size: 12, modified_ms: 0, navigable: true },
        { name: "BOARD.md", rel_path: ".saipen/BOARD.md", kind: "file", size: 9, modified_ms: 0, navigable: true },
      ],
      truncated: false,
    });
    const r = await routeSaipenAction({ action: "knowledge", workspaceId: "w1", saipen: saipenWith(board) });
    expect(commands.filesListDir).toHaveBeenCalledWith("w1", ".saipen");
    expect(r).toEqual({
      route: "view",
      view: {
        kind: "knowledge-dir",
        entries: [{ name: "KNOWLEDGE.md", rel_path: ".saipen/KNOWLEDGE.md", kind: "file", size: 12, modified_ms: 0, navigable: true }],
        path: ".saipen",
        missing: false,
        workspaceId: "w1",
      },
    });
  });

  it("Board view RENDERS the authoritative board projection", () => {
    const html = renderToString(<SaipenViewDrawer view={{ kind: "board", board, workspaceId: "w1" }} onClose={() => {}} />);
    expect(html).toContain("DOING");
    expect(html).toContain("T-100");
    expect(html).toContain("DONE");
    expect(html).toContain("T-99");
  });

  it("Knowledge view RENDERS the canonical material with its path", () => {
    const html = renderToString(
      <SaipenViewDrawer view={{ kind: "knowledge", text: "# KNOWLEDGE\n- rule one", path: ".saipen/KNOWLEDGE.md", workspaceId: "w1" }} onClose={() => {}} />,
    );
    expect(html).toContain("KNOWLEDGE.md");
    expect(html).toContain("rule one");
  });

  it("Knowledge-dir view RENDERS the bounded listing (and a missing note when empty)", () => {
    const listed = renderToString(
      <SaipenViewDrawer
        view={{
          kind: "knowledge-dir",
          entries: [{ name: "KNOWLEDGE.md", rel_path: ".saipen/KNOWLEDGE.md", kind: "file", size: 12, modified_ms: 0, navigable: true }],
          path: ".saipen",
          missing: false,
          workspaceId: "w1",
        }}
        onClose={() => {}}
      />,
    );
    expect(listed).toContain("KNOWLEDGE.md");

    const missing = renderToString(
      <SaipenViewDrawer view={{ kind: "knowledge-dir", entries: [], path: ".saipen", missing: true, workspaceId: "w1" }} onClose={() => {}} />,
    );
    expect(missing).toContain("No KNOWLEDGE material found");
  });
});
