import { useEffect, useState } from "react";
import type {
  DiagnosticsSnapshot,
  EngineCapabilities,
  EngineHealth,
} from "@saiwork2/contracts";
import { commands } from "../app/backend";

function healthText(h: EngineHealth): string {
  if (typeof h === "string") return h;
  return h.kind;
}

function capList(c: EngineCapabilities): string {
  const on = (
    [
      "sessions",
      "resume",
      "streaming",
      "cancel",
      "tools",
      "permissions",
      "models",
    ] as const
  ).filter((k) => c[k]);
  return on.length ? on.join("+") : "none";
}

export function DiagnosticsPanel() {
  const [open, setOpen] = useState(false);
  const [snap, setSnap] = useState<DiagnosticsSnapshot | null>(null);
  const [copied, setCopied] = useState(false);

  async function refresh() {
    try {
      setSnap(await commands.diagnostics());
    } catch {
      setSnap(null);
    }
  }

  useEffect(() => {
    if (open) void refresh();
  }, [open]);

  async function copy() {
    if (!snap) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(snap, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard unavailable (web dev); ignore
    }
  }

  return (
    <section className="diagnostics">
      <div className="diagnostics__head">
        <button className="btn btn--small" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
          Diagnostics {open ? "▾" : "▸"}
        </button>
        {open && snap && (
          <button className="btn btn--small" onClick={() => void copy()}>
            {copied ? "Copied" : "Copy"}
          </button>
        )}
      </div>
      {open && (
        <div className="diagnostics__body">
          {snap ? (
            <>
              <Row k="version" v={snap.version} />
              <Row k="data root" v={snap.data_root} />
              <Row k="portable" v={String(snap.portable)} />
              <Row k="db integrity" v={snap.db_integrity} />
              <Row k="db schema" v={String(snap.db_schema_version)} />
              <Row k="engines" v={String(snap.engine_count)} />
              {snap.engines.map((e) => (
                <Row
                  key={e.id}
                  k={`engine ${e.id}`}
                  v={`${healthText(e.health)} · v${e.version} · caps: ${capList(e.capabilities)}`}
                />
              ))}
              <Row k="workspaces" v={String(snap.workspaces)} />
              <Row k="sessions" v={String(snap.sessions)} />
              <Row k="processes" v={snap.processes.map((p) => `${p.id}:${p.state}`).join(", ") || "none"} />
              <Row k="event subscribers" v={String(snap.event_subscribers)} />
              {snap.recent_errors.length > 0 && (
                <div className="diagnostics__errors">
                  {snap.recent_errors.map((e, i) => (
                    <div key={i} className="diagnostics__error">
                      <span className="muted">{e.code}</span> {e.message}
                    </div>
                  ))}
                </div>
              )}
              <div className="muted diagnostics__note">Redacted snapshot — no prompts or tool content.</div>
            </>
          ) : (
            <p className="muted">no diagnostics (backend disconnected?)</p>
          )}
        </div>
      )}
    </section>
  );
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <div className="diagnostics__row">
      <span className="diagnostics__k">{k}</span>
      <span className="diagnostics__v">{v}</span>
    </div>
  );
}
