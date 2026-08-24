// Settings-preset import (T-078): pick a preset file (.json — or .zip, which
// is refused by the backend with a clear error) and apply it through the
// backend authority. The UI never owns the DB (law 5); this module only picks
// a path and delegates the parse/validate/apply to the `import_preset`
// command. The ZIP-magic gate lives in saiwork-core — a ZIP must never reach
// `JSON.parse`.

import { commands } from "./backend";
import { setFavorites } from "../state/store";

export interface PresetImportOutcome {
  settings_applied: number;
  favorites_applied: number;
}

/**
 * Open the file picker filtered to preset files (.json / .zip). Returns
 * `null` only for the two legitimate cases (web-dev mode without the Tauri
 * dialog plugin, or a deliberate user Cancel); any other failure is surfaced
 * via `onError` (mirror of `pickFolder`, T-022).
 */
export async function pickPresetFile(
  onError?: (message: string) => void,
): Promise<string | null> {
  if (typeof window === "undefined" || !("__TAURI_INTERNALS__" in window)) {
    return null;
  }
  try {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const selected = await open({
      multiple: false,
      filters: [{ name: "SAIWORK2 preset", extensions: ["json", "zip"] }],
    });
    return typeof selected === "string" ? selected : null;
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    onError?.(`Preset picker failed: ${message}`);
    return null;
  }
}

/**
 * Run the import flow: pick a preset file, delegate to the backend, and on
 * success refresh the store's favorites projection (the backend is the
 * authority; the store is a projection, law 23). Returns `false` on cancel,
 * throws with the backend's error on failure so the caller can surface it.
 */
export async function runPresetImport(
  onError: (message: string) => void,
): Promise<PresetImportOutcome | null> {
  const path = await pickPresetFile(onError);
  if (!path) return null;
  try {
    const outcome = await commands.importPreset(path);
    // The imported favorites are authoritative (backend write won). Re-read
    // them rather than trusting the preset text (capping/dedup happens in the
    // backend), then reflect the projection (law 23).
    const favorites = await commands.getModelFavorites();
    setFavorites(favorites);
    return outcome;
  } catch (e) {
    onError(String(e));
    throw e;
  }
}
