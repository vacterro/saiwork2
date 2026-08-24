import { useEffect, useMemo, useRef, useState } from "react";
import type { ModelInfo } from "@saiwork2/contracts";
import { modelLabel } from "./TitleBar";

/** Hard ceiling on rendered rows. A naive <select> with the full provider
 * catalog (thousands of paywalled models) mounted every option into the DOM;
 * this filters and renders at most MAX_ROWS, so typing is bounded and the
 * UI stays responsive even with a synthetic 10k catalog (TASK 17 §60/§112). */
const MAX_ROWS = 200;

interface Props {
  models: ModelInfo[];
  favorites: string[];
  favoritesOnly: boolean;
  selectedModelId: string | null;
  onSelect: (id: string | null) => void;
}

/** Precomputed, stable-per-(models,favorites)-generation search index
 * (PERF-007). Normalized search text, favorite membership and the
 * favorites-first ordering are derived ONCE here, not on every keystroke, so a
 * 6k/10k-model query walks a precomputed order with no repeated lowercasing or
 * intermediate full-catalog arrays. */
interface PreparedModel {
  model: ModelInfo;
  labelLower: string;
  idLower: string;
  isFav: boolean;
}

interface ModelIndex {
  byId: Map<string, ModelInfo>;
  favSet: Set<string>;
  /** Favorites-first ordered entries (the default walk order). */
  ordered: PreparedModel[];
  favorites: PreparedModel[];
  others: PreparedModel[];
}

export function prepareModelIndex(models: ModelInfo[], favorites: string[]): ModelIndex {
  const favSet = new Set(favorites);
  const byId = new Map<string, ModelInfo>();
  const favoritesList: PreparedModel[] = [];
  const othersList: PreparedModel[] = [];
  for (const m of models) {
    byId.set(m.id, m);
    const entry: PreparedModel = {
      model: m,
      labelLower: modelLabel(m).toLowerCase(),
      idLower: m.id.toLowerCase(),
      isFav: favSet.has(m.id),
    };
    if (entry.isFav) favoritesList.push(entry);
    else othersList.push(entry);
  }
  return {
    byId,
    favSet,
    ordered: [...favoritesList, ...othersList],
    favorites: favoritesList,
    others: othersList,
  };
}

/** Window a precomputed `ModelIndex` for one query (PERF-007): lowercase the
 * query once, walk the favorites-first order, and stop after `maxRows` matches.
 * Favorites-first ordering, favoritesOnly semantics, case-insensitive
 * display-name/id matching and the MAX_ROWS cap are preserved; the currently
 * selected model is always reachable (prepended when the cap would hide it). */
export function buildModelWindowWithIndex(
  index: ModelIndex,
  favoritesOnly: boolean,
  query: string,
  selectedModelId: string | null,
  maxRows = MAX_ROWS,
): ModelInfo[] {
  const q = query.trim().toLowerCase();
  const pool = favoritesOnly ? index.favorites : index.ordered;
  const rows: ModelInfo[] = [];
  for (const e of pool) {
    if (rows.length >= maxRows) break;
    if (!q || e.labelLower.includes(q) || e.idLower.includes(q)) rows.push(e.model);
  }
  if (!selectedModelId || rows.some((m) => m.id === selectedModelId)) return rows;
  const sel = index.byId.get(selectedModelId);
  return sel ? [sel, ...rows] : rows;
}

/** Pure windowing rule (unit-testable without a DOM): favorites first, then
 * query filter, then the MAX_ROWS cap — and the currently selected model is
 * ALWAYS present in the window (prepended when the cap would hide it). Kept as
 * the canonical entry point for tests; internally prepares the search index
 * once and delegates to `buildModelWindowWithIndex`. */
export function buildModelWindow(
  models: ModelInfo[],
  favorites: string[],
  favoritesOnly: boolean,
  query: string,
  selectedModelId: string | null,
  maxRows = MAX_ROWS,
): ModelInfo[] {
  const index = prepareModelIndex(models, favorites);
  return buildModelWindowWithIndex(index, favoritesOnly, query, selectedModelId, maxRows);
}

/** Bounded, searchable model selector (replaces the unbounded native
 * <select>). Favorites are always listed first; a free-text query filters by
 * display name / provider. Only the first MAX_ROWS matches are rendered. The
 * popover opens on focus so it never floods the DOM. Full keyboard
 * navigation: arrows move, Enter selects, Escape closes — and the currently
 * selected model is ALWAYS reachable in the window (prepended when it would
 * otherwise fall past the row cap). */
export function ModelPicker({ models, favorites, favoritesOnly, selectedModelId, onSelect }: Props) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  // Keyboard focus index into `options` (0 = "engine default" row).
  const [focusIdx, setFocusIdx] = useState(0);
  const wrapRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Close on outside click (no global key listener needed; the field is the
  // only focusable thing inside the popover).
  useEffect(() => {
    if (!open) return;
    function onDoc(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  // PERF-007: prepare the search index once per (models, favorites) generation,
  // not on every keystroke — the 6k/10k-model query then walks a precomputed
  // favorites-first order with no repeated lowercasing or full-catalog arrays.
  const index = useMemo(() => prepareModelIndex(models, favorites), [models, favorites]);

  const rows = useMemo(
    () => buildModelWindowWithIndex(index, favoritesOnly, query, selectedModelId),
    [index, favoritesOnly, query, selectedModelId],
  );

  // Selectable options: index 0 is always "engine default".
  const options = useMemo(
    () => [{ id: null as string | null }, ...rows.map((m) => ({ id: m.id }))],
    [rows],
  );

  // Typing/filtering restarts keyboard focus at the top of the list.
  useEffect(() => {
    setFocusIdx(0);
  }, [open, query, favoritesOnly]);

  // On open, keyboard focus lands on the CURRENT selection (or the top when
  // nothing is selected) — declared LAST so it wins over the reset above.
  useEffect(() => {
    if (!open) return;
    const idx = options.findIndex((o) => o.id === selectedModelId);
    setFocusIdx(idx >= 0 ? idx : 0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // Keep the focused option visible while arrowing through a long list.
  useEffect(() => {
    if (!open) return;
    const el = inputRef.current?.parentElement?.querySelector(`[data-idx="${focusIdx}"]`);
    el?.scrollIntoView({ block: "nearest" });
  }, [open, focusIdx]);

  function pick(id: string | null) {
    onSelect(id);
    setOpen(false);
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Escape") {
      setOpen(false);
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setFocusIdx((i) => Math.min(i + 1, options.length - 1));
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      setFocusIdx((i) => Math.max(i - 1, 0));
      return;
    }
    if (e.key === "Home") {
      e.preventDefault();
      setFocusIdx(0);
      return;
    }
    if (e.key === "End") {
      e.preventDefault();
      setFocusIdx(options.length - 1);
      return;
    }
    if (e.key === "Enter" && open) {
      e.preventDefault();
      pick(options[focusIdx]?.id ?? null);
    }
  }

  const selectedLabel = selectedModelId
    ? index.byId.get(selectedModelId)?.display_name ?? "engine default"
    : "— engine default —";

  return (
    <div className="modelpicker" ref={wrapRef}>
      <button
        type="button"
        className="modelpicker__trigger"
        onClick={() => {
          setOpen((o) => !o);
          if (!open) requestAnimationFrame(() => inputRef.current?.focus());
        }}
        title="Engine Default uses the OpenCode server's configured model"
      >
        {selectedLabel}
        <span className="modelpicker__caret">▾</span>
      </button>
      {open && (
        <div className="modelpicker__pop">
          <input
            ref={inputRef}
            className="modelpicker__search"
            placeholder="Search models…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            aria-activedescendant={`modelpicker-opt-${focusIdx}`}
            role="combobox"
            aria-expanded={open}
          />
          <div className="modelpicker__list" role="listbox">
            <button
              type="button"
              id="modelpicker-opt-0"
              data-idx={0}
              className={`modelpicker__opt${focusIdx === 0 ? " is-focused" : ""}${
                selectedModelId === null ? " is-selected" : ""
              }`}
              onMouseEnter={() => setFocusIdx(0)}
              onClick={() => pick(null)}
            >
              — engine default —
            </button>
            {rows.map((m, i) => {
              const idx = i + 1;
              return (
                <button
                  type="button"
                  key={m.id}
                  id={`modelpicker-opt-${idx}`}
                  data-idx={idx}
                  role="option"
                  aria-selected={m.id === selectedModelId}
                  className={`modelpicker__opt${focusIdx === idx ? " is-focused" : ""}${
                    m.id === selectedModelId ? " is-selected" : ""
                  }${index.favSet.has(m.id) ? " is-fav" : ""}`}
                  onMouseEnter={() => setFocusIdx(idx)}
                  onClick={() => pick(m.id)}
                  title={m.id}
                >
                  {index.favSet.has(m.id) ? "★ " : ""}
                  {modelLabel(m)}
                </button>
              );
            })}
            {rows.length === 0 && <div className="modelpicker__empty muted">No models match</div>}
            {models.length > MAX_ROWS && rows.length >= MAX_ROWS && (
              <div className="modelpicker__empty muted">Showing first {MAX_ROWS} matches — refine search</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}