// Composer append bridge (T-035).
//
// The composer draft is component-local state (PERF-022) and MUST stay that
// way — a global store field per keystroke rerendered the shell. The Files
// panel still needs a one-shot "insert this path into the composer" action,
// which is a rare discrete user gesture, not keystroke churn: a window
// CustomEvent carries it without any store round-trip.
//
// Ownership rule (W2-011 spirit): the event APPENDS to whatever draft the
// user has right now; it never overwrites and never clears.

export const COMPOSER_APPEND_EVENT = "saiwork2:composer-append";

/** Pure append rule shared by the composer and its tests: join with a single
 * space and preserve every user-provided character. Typed backend boundaries
 * report their own finite transport limits; the editor never truncates. */
export function appendDraft(prev: string, text: string): string {
  return prev.length === 0 ? text : `${prev} ${text}`;
}

/** Fire-and-forget: ask the composer to append `text` (e.g. a workspace-
 * relative file path) to the current draft and take focus. No-op outside a
 * DOM window (SSR/tests without jsdom). */
export function requestComposerAppend(text: string): void {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new CustomEvent(COMPOSER_APPEND_EVENT, { detail: text }));
}
