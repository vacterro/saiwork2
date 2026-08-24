// @ts-nocheck — regression guard reads CSS via Node fs; browser tsconfig has no node types.
import { describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";

// Regression guard for screenshots 1 & 3 (2026-08-22): the shell grid must
// keep its contract or the whole cockpit collapses into a squashed bottom
// strip (huge blank ThreadTabs band, nav/dock squeezed, rail as full-width
// bar covering the composer).
describe("shell layout contract (global.css)", () => {
  const css = fs.readFileSync(
    path.resolve(process.cwd(), "src/styles/global.css"),
    "utf-8",
  );

  it("app grid has 6 rows with 1fr on app__main, not on ThreadTabs", () => {
    // TitleBar | ThreadTabs | main(1fr) | Composer | SaipenBar | StatusLine
    // The bug was `grid-template-rows: auto 1fr auto auto auto` — ThreadTabs
    // stole the flexible row and squashed app__main to the bottom.
    expect(css).toContain("grid-template-rows: auto auto 1fr auto auto auto;");
    expect(css).not.toMatch(/\.app\s*\{[^}]*grid-template-rows:\s*auto\s+1fr\s+auto\s+auto\s+auto\s*;/);
  });

  it("1100px breakpoint keeps the 46px rail column (3-col grid), does not drop to 2-col", () => {
    // Previous 2-col grid `minmax(160px,200px) minmax(0,1fr)` made the rail
    // wrap as a full-width row at the bottom (black bar covering composer).
    expect(css).toContain("grid-template-columns: minmax(160px, 200px) minmax(0, 1fr) 46px;");
  });

  it("760px breakpoint keeps the rail column (2-col), does not collapse to single column", () => {
    expect(css).toContain("grid-template-columns: minmax(0, 1fr) 46px;");
  });

  it("only .dock is hidden at breakpoints, never .dock-rail", () => {
    // Hiding the rail made the dock unreachable between 760-1100px (T-050).
    // The full panel is hidden so lagging React state cannot mount it into
    // the collapsed layout; the rail stays reachable.
    const dockHidden = (css.match(/\.dock\s*\{\s*display:\s*none;/g) || []).length;
    expect(dockHidden).toBeGreaterThanOrEqual(1);
    expect(css).not.toMatch(/\.dock-rail\s*\{\s*display:\s*none/);
  });
});
