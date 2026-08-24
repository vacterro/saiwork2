// Synchronous single-flight latch for non-idempotent UI actions (T-028).
//
// WHY THIS EXISTS (the bug it replaces): guards written as
//
//   if (sending) return; setSending(true); …
//
// are NOT single-flight. `sending` is React state: the captured value does not
// change until the component re-renders, so two native events delivered in the
// same tick (double click, key repeat, click + Enter) both observe `false` and
// both reach the backend — two sends, two sessions, two queue items. A
// `disabled` attribute has the same hole: it only takes effect after the
// re-render.
//
// The latch below flips a plain mutable field BEFORE the first await, so the
// second same-tick caller is rejected deterministically. React state is kept
// only for PRESENTATION (button label/disabled), never as the guard.
//
// Scope rule: one latch per logical action. A shared global mutex across
// unrelated actions would make the UI feel broken (queueing a prompt must not
// be blocked because a session is being created).

import { useCallback, useRef, useState } from "react";

/**
 * Component-side single-flight latch: `run` is the guard (a synchronous ref
 * latch that survives re-renders), `busy` is presentation only. Never use
 * `busy` as the guard.
 */
export function useSingleFlight(): {
  busy: boolean;
  run: (fn: () => Promise<unknown>) => Promise<void>;
} {
  const inFlightRef = useRef(false);
  const [busy, setBusy] = useState(false);

  const run = useCallback(async (fn: () => Promise<unknown>) => {
    if (inFlightRef.current) return; // synchronous rejection
    inFlightRef.current = true;
    setBusy(true);
    try {
      await fn();
    } finally {
      inFlightRef.current = false;
      setBusy(false);
    }
  }, []);

  return { busy, run };
}
