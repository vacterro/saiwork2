import { describe, expect, it } from "vitest";
import {
  lifecycleGate,
  mutationsAllowed,
  sendAvailability,
  sessionCreateAvailability,
  sessionActivationAvailability,
  type SessionEligibilitySlice,
} from "./eligibility";

function baseSlice(over: Partial<SessionEligibilitySlice> = {}): SessionEligibilitySlice {
  return {
    currentWorkspaceId: "w1",
    engines: [
      {
        id: "opencode",
        display_name: "OpenCode",
        version: "1",
        experimental: false,
        health: { kind: "ready" },
        capabilities: { sessions: true },
        bound_workspace_id: "w1",
      } as never,
    ],
    selectedEngineId: "opencode",
    stoppingEngines: {},
    lifecycle: "ready",
    ...over,
  };
}

describe("W2-008 canonical lifecycle gate", () => {
  it("lifecycleGate allows only ready and rejects booting/shutting_down/stopped/failed", () => {
    expect(lifecycleGate({ lifecycle: "ready" }).allowed).toBe(true);
    expect(lifecycleGate({ lifecycle: "booting" }).allowed).toBe(false);
    expect(lifecycleGate({ lifecycle: "failed" }).allowed).toBe(false);
    const sd = lifecycleGate({ lifecycle: "shutting_down" });
    expect(sd.allowed).toBe(false);
    expect(sd.reason).toBe("Application is shutting down");
    expect(lifecycleGate({ lifecycle: "stopped" }).allowed).toBe(false);
  });

  it("mutationsAllowed is true only when ready", () => {
    expect(mutationsAllowed({ lifecycle: "ready" })).toBe(true);
    expect(mutationsAllowed({ lifecycle: "shutting_down" })).toBe(false);
    expect(mutationsAllowed({ lifecycle: "booting" })).toBe(false);
  });

  it("sessionCreateAvailability is denied while shutting down (was previously unguarded)", () => {
    expect(sessionCreateAvailability(baseSlice()).allowed).toBe(true);
    const sd = sessionCreateAvailability(baseSlice({ lifecycle: "shutting_down" }));
    expect(sd.allowed).toBe(false);
    expect(sd.reason).toBe("Application is shutting down");
  });

  it("sessionActivationAvailability is denied while shutting down", () => {
    const session = {
      id: "s1",
      workspace_id: "w1",
      engine_id: "opencode",
      usable_now: true,
      resumable: true,
      engine_session_id: "e1",
      display_name: "s",
    } as never;
    expect(sessionActivationAvailability(session, baseSlice()).allowed).toBe(true);
    expect(
      sessionActivationAvailability(session, baseSlice({ lifecycle: "shutting_down" })).allowed,
    ).toBe(false);
  });
});

describe("composer first-message eligibility", () => {
  it("allows Send to create the first session when the selected engine is ready", () => {
    const state = {
      ...baseSlice(),
      activeSessionId: null,
      sessions: [],
      running: {},
      runningStale: false,
    };
    expect(sendAvailability(state)).toEqual({ allowed: true });
  });
});
