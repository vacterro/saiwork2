// AUDIT-PERF-003: slice-level subscription boundary.
//
// `useAppState` subscribed the application ROOT to the ENTIRE AppState, so
// every shell-coalesced message delta re-executed the whole App render (and
// its 12 pickSlice allocations) before any child memo comparator could short-
// circuit. Selection must live AT the subscription: each consumer declares the
// keys it renders, gets a STABLE slice object identity while those references
// are unchanged, and React skips the component entirely on unrelated store
// transitions.
//
// One Store authority is preserved; this only moves WHERE components attach.

import { useCallback, useDebugValue, useRef } from "react";
import { useSyncExternalStore } from "react";
import { store, type AppState } from "./store";

export type SliceSelector<T> = (state: AppState) => T;

/** Subscribe to a projected slice of the store.
 *
 * `select` must be a STABLE reference (module-level selector — it runs inside
 * getSnapshot). `isEqual` decides whether the previously delivered value can
 * be reused, keeping snapshot identity stable across unrelated mutations so
 * React bails out. Defaults to Object.is for primitive/single-reference
 * selectors; pass a key-based shallow equality for pickSlice projections. */
export function useAppSlice<T>(
  select: SliceSelector<T>,
  isEqual: (a: T, b: T) => boolean = Object.is,
): T {
  // Cache: identity stays stable until isEqual says the projection changed.
  const lastRef = useRef<{ value: T } | null>(null);

  const getSnapshot = useCallback((): T => {
    const value = select(store.getState());
    const prev = lastRef.current;
    if (prev && isEqual(prev.value, value)) {
      return prev.value; // stable identity → React bailout
    }
    lastRef.current = { value };
    return value;
  }, [select, isEqual]);

  const value = useSyncExternalStore(
    (cb) => store.subscribe(cb),
    getSnapshot,
    getSnapshot,
  );
  useDebugValue(value);
  return value;
}
