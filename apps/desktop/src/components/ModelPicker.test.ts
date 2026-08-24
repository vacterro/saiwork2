import { describe, expect, it } from "vitest";
import { buildModelWindow } from "./ModelPicker";

function model(id: string, provider = "p"): { id: string; display_name: string; provider: string; provider_name: string | null } {
  return { id, display_name: id, provider, provider_name: provider };
}

const BIG: ReturnType<typeof model>[] = Array.from({ length: 10_000 }, (_, i) =>
  model(`model-${String(i).padStart(5, "0")}`),
);

describe("buildModelWindow (bounded model selector)", () => {
  it("caps the rendered window at 200 rows on a 10k catalog", () => {
    const rows = buildModelWindow(BIG, [], false, "", null);
    expect(rows).toHaveLength(200);
  });

  it("keeps the SELECTED model visible even when the cap would hide it", () => {
    const rows = buildModelWindow(BIG, [], false, "", "model-05000");
    expect(rows[0]!.id).toBe("model-05000");
    expect(rows).toHaveLength(201); // cap + the pinned selection
  });

  it("does not pin a selected id that is not in the catalog", () => {
    const rows = buildModelWindow(BIG, [], false, "", "ghost");
    expect(rows).toHaveLength(200);
    expect(rows.some((m) => m.id === "ghost")).toBe(false);
  });

  it("favorites sort first and favorites-only filters to them", () => {
    const favs = ["model-09000", "model-00001"];
    const rows = buildModelWindow(BIG, favs, false, "", null);
    expect(rows[0]!.id).toBe("model-00001");
    expect(rows[1]!.id).toBe("model-09000");
    const only = buildModelWindow(BIG, favs, true, "", null);
    expect(only.map((m) => m.id)).toEqual(["model-00001", "model-09000"]);
  });

  it("filters by id prefix and pins the selection within the filter", () => {
    const rows = buildModelWindow(BIG, [], false, "model-0005", null);
    expect(rows.length).toBeLessThanOrEqual(200);
    expect(rows.every((m) => m.id.startsWith("model-0005"))).toBe(true);
  });
});