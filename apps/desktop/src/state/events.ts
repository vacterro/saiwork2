// Typed view over canonical events (KNOWLEDGE/EVENTS.md). The Rust side is
// authoritative; unknown/raw events are preserved as opaque payloads.

import type { Envelope } from "@saiwork2/contracts";

export interface TypedEvent {
  seq: number;
  ts: number;
  type: string;
  /** Normalized payload fields (string-typed helpers below). */
  payload: Record<string, unknown>;
}

export function parseEnvelope(env: Envelope): TypedEvent {
  const { seq, ts, type, ...payload } = env;
  return { seq, ts, type, payload };
}

export function str(payload: Record<string, unknown>, key: string): string | null {
  const v = payload[key];
  return typeof v === "string" ? v : null;
}

export function bool(payload: Record<string, unknown>, key: string): boolean | null {
  const v = payload[key];
  return typeof v === "boolean" ? v : null;
}
