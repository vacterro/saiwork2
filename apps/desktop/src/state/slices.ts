// Typed store slices for the shell's panels (T-030).
//
// WHY: App is the single store subscriber, so without memoization every token
// batch would rerender the whole shell. The previous solution passed the WHOLE
// `AppState` to each panel plus a hand-written comparator key list — a SECOND,
// manually maintained dependency system that silently drifted from what the
// components actually read (`favoriteModelIds`, `favoritesOnly`, `modelsError`,
// `runningStale`, `saipenStale`, `historyStatus` were all consumed but absent
// from their comparators, so the UI could show stale favorites/stale send
// gating).
//
// The fix is ONE definition per component: a `readonly` key tuple that
// simultaneously
//   * types the component's props (`Pick<AppState, keys[number]>`), so reading
//     an undeclared field is a COMPILE error, and
//   * generates the memo comparator, so a declared field is always compared.
// Drift is therefore not expressible: the type and the comparator cannot
// disagree.

import type { AppState } from "./store";

export type SliceKeys = readonly (keyof AppState)[];

/** Props of a slice-memoized panel. */
export interface SliceProps<K extends keyof AppState> {
  state: Pick<AppState, K>;
  onError: (message: string) => void;
}

/** Project one panel's declared slice out of the store state. */
export function pickSlice<K extends SliceKeys>(
  state: AppState,
  keys: K,
): Pick<AppState, K[number]> {
  const out: Partial<AppState> = {};
  for (const k of keys) {
    // Index-assignment through a partial: each key keeps its own value type.
    (out as Record<string, unknown>)[k as string] = state[k];
  }
  return out as Pick<AppState, K[number]>;
}

/**
 * Reference comparison over exactly the declared keys (+ the handler identity).
 * The store is immutable, so a reference change is a real data change.
 *
 * `extra` adds a domain-specific rule for keys whose reference changes on every
 * streamed token (the message slices): the panel opts out of text-only churn
 * while still reacting to the facts it displays.
 */
export function sliceEqual<K extends SliceKeys>(
  keys: K,
  extra?: (a: Pick<AppState, K[number]>, b: Pick<AppState, K[number]>) => boolean,
  shallowKeys: SliceKeys = keys,
): (a: SliceProps<K[number]>, b: SliceProps<K[number]>) => boolean {
  return (a, b) => {
    if (a.onError !== b.onError) return false;
    for (const k of shallowKeys) {
      if ((a.state as AppState)[k] !== (b.state as AppState)[k]) return false;
    }
    return extra ? extra(a.state, b.state) : true;
  };
}
