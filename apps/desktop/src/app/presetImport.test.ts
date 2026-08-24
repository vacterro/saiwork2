// T-078 preset import: the frontend picks a file and delegates to the
// backend authority; the ZIP-magic gate lives in saiwork-core. These tests
// pin the frontend contract: cancel/no-plugin → null (no import attempted),
// success → store favorites projection refreshed from the authoritative
// re-read, backend error → surfaced and rethrown.
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { store } from "../state/store";

let openImpl: (opts: unknown) => Promise<string | null>;
let importImpl: (path: string) => Promise<{ settings_applied: number; favorites_applied: number }>;
let favoritesImpl: () => Promise<string[]>;

vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: (opts: unknown) => openImpl(opts),
}));

vi.mock("./backend", () => ({
  commands: {
    importPreset: (path: string) => importImpl(path),
    getModelFavorites: () => favoritesImpl(),
  },
}));

async function importModule() {
  return await import("./presetImport");
}

describe("runPresetImport (T-078)", () => {
  beforeEach(() => {
    (globalThis as Record<string, unknown>).window = { __TAURI_INTERNALS__: {} };
    store.resetForTest();
    openImpl = () => Promise.resolve(null);
    importImpl = () => Promise.resolve({ settings_applied: 1, favorites_applied: 2 });
    favoritesImpl = () => Promise.resolve(["a/model", "b/model"]);
  });
  afterEach(() => {
    delete (globalThis as Record<string, unknown>).window;
    vi.restoreAllMocks();
  });

  it("returns null on user Cancel — no import is attempted", async () => {
    openImpl = () => Promise.resolve(null);
    const onError = vi.fn();
    const { runPresetImport } = await importModule();
    const outcome = await runPresetImport(onError);
    expect(outcome).toBeNull();
    expect(onError).not.toHaveBeenCalled();
  });

  it("returns null without the Tauri plugin (web-dev) — no error", async () => {
    delete (globalThis as Record<string, unknown>).window;
    const onError = vi.fn();
    const { runPresetImport } = await importModule();
    const outcome = await runPresetImport(onError);
    expect(outcome).toBeNull();
    expect(onError).not.toHaveBeenCalled();
  });

  it("applies the preset and refreshes the store favorites projection from the authoritative re-read", async () => {
    openImpl = () => Promise.resolve("C:/presets/team.json");
    favoritesImpl = () => Promise.resolve(["a/model", "b/model"]);
    const onError = vi.fn();
    const { runPresetImport } = await importModule();
    const outcome = await runPresetImport(onError);
    expect(outcome).toEqual({ settings_applied: 1, favorites_applied: 2 });
    // The store never trusts the preset text; it re-reads the backend truth
    // (capping/dedup happen in saiwork-core) — law 23 projection.
    expect(store.getState().favoriteModelIds).toEqual(["a/model", "b/model"]);
    expect(onError).not.toHaveBeenCalled();
  });

  it("surfaces a backend rejection (e.g. ZIP magic refusal) and rethrows", async () => {
    openImpl = () => Promise.resolve("C:/presets/bundle.zip");
    importImpl = () =>
      Promise.reject(new Error("preset import error: the selected file is a ZIP archive"));
    const onError = vi.fn();
    const { runPresetImport } = await importModule();
    await expect(runPresetImport(onError)).rejects.toThrow("ZIP archive");
    expect(onError).toHaveBeenCalledWith(expect.stringContaining("ZIP archive"));
    // No partial projection on failure.
    expect(store.getState().favoriteModelIds).toEqual([]);
  });

  it("surfaces a dialog plumbing failure as an error, not a silent cancel", async () => {
    openImpl = () => Promise.reject(new Error("dialog broken"));
    const onError = vi.fn();
    const { runPresetImport } = await importModule();
    const outcome = await runPresetImport(onError);
    expect(outcome).toBeNull();
    expect(onError).toHaveBeenCalledWith(expect.stringContaining("Preset picker failed"));
  });
});
