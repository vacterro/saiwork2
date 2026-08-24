import { useState } from "react";
import type { ToolActivity } from "../state/store";
import { timestampLabel } from "./Conversation";

/**
 * One tool invocation (TASK 16 §32–§35): lifecycle (running/completed/failed/
 * interrupted), bounded output preview, expandable detail. The backend
 * contract is the source; this only renders the projection.
 */
export function ToolActivityView({ tool, now = Date.now() }: { tool: ToolActivity; now?: number }) {
  const [open, setOpen] = useState(false);
  const statusLabel =
    tool.status === "started" || tool.status === "output"
      ? "running"
      : tool.status === "failed"
        ? "failed"
        : tool.status === "completed"
          ? "completed"
          : tool.status;

  return (
    <div className={`tool tool--${statusLabel}`}>
      <div className="tool__row">
        <span className="tool__name" title={`${tool.tool} · call ${tool.id}`}>
          {tool.tool}
          {tool.id && <span className="tool__call">#{tool.id.slice(-6)}</span>}
        </span>
        <span className={`status status--${statusLabel}`}>{statusLabel}</span>
        {tool.ts > 0 && (
          <span className="msg__time" title={new Date(tool.ts).toISOString()}>
            {timestampLabel(tool.ts, now)}
          </span>
        )}
        <button className="btn btn--small tool__toggle" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
          {open ? "▾" : "▸"}
        </button>
      </div>
      {open && tool.output && <pre className="tool__output">{tool.output}</pre>}
      {tool.error && <span className="msg__error">failed: {tool.error}</span>}
    </div>
  );
}
