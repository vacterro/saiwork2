import { describe, expect, it } from "vitest";
import { renderToString } from "react-dom/server";
import { Conversation, timestampLabel } from "./Conversation";
import type { AppState } from "../state/store";
import { initialState } from "../state/store";

function stateWith(sessionId: string, text: string, status: "streaming" | "complete"): AppState {
  return {
    ...initialState,
    activeSessionId: sessionId,
    running: { [sessionId]: status === "streaming" ? "r1" : null },
    messages: {
      [sessionId]: [
        {
          id: "m1",
          role: "assistant",
          runId: "r1",
          status,
          text,
          tools: [],
          permissions: [], questions: [],
          ts: Date.now(),
        },
      ],
    },
  };
}

describe("Conversation rendering (§20–§28)", () => {
  it("renders streaming text as plain pre, not Markdown", () => {
    const html = renderToString(<Conversation state={stateWith("s1", "# heading\n`code`", "streaming")} onError={() => undefined} />);
    // Streaming must stay raw text — no <h1> heading, no markdown conversion.
    expect(html).not.toContain("<h1>");
    expect(html).toContain("streaming");
    expect(html).toContain("# heading");
  });

  it("finalizes Markdown at terminal with a copyable code block (§28)", () => {
    const md = "```rust\nfn main() {}\n```\n\nDone.";
    const html = renderToString(<Conversation state={stateWith("s1", md, "complete")} onError={() => undefined} />);
    expect(html).toContain('class="md"');
    expect(html).toContain("codeblock");
    expect(html).toContain("rust");
    expect(html).toContain("fn main() {}");
    expect(html).toContain("Copy");
    expect(html).toContain("complete");
  });

  it("never fabricates a completed state for an interrupted run", () => {
    const html = renderToString(
      <Conversation state={stateWith("s1", "partial answer", "streaming")} onError={() => undefined} />,
    );
    expect(html).toContain("streaming");
    expect(html).not.toContain("complete");
  });
});

describe("Conversation bounded windowing (TASK 24 perf)", () => {
  function manyMessages(n: number): AppState {
    const msgs = Array.from({ length: n }, (_, i) => ({
      id: `m${i}`,
      role: (i % 2 === 0 ? "user" : "assistant") as "user" | "assistant",
      runId: i % 2 === 1 ? `r${i}` : undefined,
      status: (i % 2 === 1 ? "complete" : undefined) as "complete" | undefined,
      text: `message ${i}`,
      tools: [],
      permissions: [], questions: [],
      ts: Date.now(),
    }));
    return {
      ...initialState,
      activeSessionId: "s1",
      running: { s1: null },
      messages: { s1: msgs as AppState["messages"][string] },
    };
  }

  it("mounts only the newest window for a long transcript", () => {
    const state = manyMessages(1000);
    const html = renderToString(<Conversation state={state} onError={() => undefined} />);
    // The newest 50 messages are mounted (500–999), earlier ones are hidden
    // behind the Load-earlier control — the DOM never scales with history.
    expect(html).toContain("conversation__load-earlier");
    expect(html).toContain("950");
    expect(html).toContain("message 999");
    expect(html).toContain("message 950");
    expect(html).not.toContain("message 0");
    expect(html).not.toContain("message 949");
    // Bounded node count: 50 messages + the load-earlier button.
    expect(html.match(/data-mid=/g)?.length).toBe(50);
  });

  it("a short transcript mounts fully with no load-earlier control", () => {
    const state = manyMessages(10);
    const html = renderToString(<Conversation state={state} onError={() => undefined} />);
    expect(html).toContain("message 0");
    expect(html).toContain("message 9");
    expect(html).not.toContain("Load earlier");
    expect(html.match(/data-mid=/g)?.length).toBe(10);
  });
});

describe("timeAgo (T-081)", () => {
  it("returns 'now' for recent timestamps", async () => {
    const { timeAgo } = await import("./Conversation");
    expect(timeAgo(Date.now())).toBe("now");
    expect(timeAgo(Date.now() - 30_000)).toBe("now");
  });
  it("returns 'Xm ago' for minutes", async () => {
    const { timeAgo } = await import("./Conversation");
    expect(timeAgo(Date.now() - 120_000)).toBe("2m ago");
  });
  it("returns 'Xh ago' for hours", async () => {
    const { timeAgo } = await import("./Conversation");
    expect(timeAgo(Date.now() - 3_600_000)).toBe("1h ago");
  });
  it("returns 'Xd ago' for days", async () => {
    const { timeAgo } = await import("./Conversation");
    expect(timeAgo(Date.now() - 172_800_000)).toBe("2d ago");
  });
  it("returns 'Xw ago' for weeks", async () => {
    const { timeAgo } = await import("./Conversation");
    expect(timeAgo(Date.now() - 14 * 86_400_000)).toBe("2w ago");
  });

  it("shows an absolute timestamp and relative age together", () => {
    const ts = Date.now() - 120_000;
    expect(timestampLabel(ts)).toContain("2m ago");
    expect(timestampLabel(ts)).toMatch(/\(.+ago\)$/);
  });

  it("recomputes relative age from the shared transcript clock", () => {
    const ts = 1_000_000;
    expect(timestampLabel(ts, ts + 30_000)).toContain("(now)");
    expect(timestampLabel(ts, ts + 120_000)).toContain("(2m ago)");
  });
});

describe("agent question cards", () => {
  it("renders every structured prompt, descriptions, and one final submit action", () => {
    const state = stateWith("s1", "Need input", "streaming");
    state.messages = {};
    state.activeMessage = {
      s1: {
        ...stateWith("s1", "Need input", "streaming").messages.s1![0]!,
        questions: [{
          requestId: "q1",
          resolved: null,
          detail: JSON.stringify({ questions: [
            { header: "Scope", question: "Which files?", options: [{ label: "Focused", description: "Only touched files" }] },
            { header: "Mode", question: "How proceed?", multiple: true, options: [{ label: "Tests", description: "Run tests" }, { label: "Docs", description: "Update docs" }] },
          ] }),
        }],
      },
    };
    const html = renderToString(<Conversation state={state} onError={() => undefined} />);
    expect(html).toContain("Which files?");
    expect(html).toContain("Only touched files");
    expect(html).toContain("How proceed?");
    expect(html).toContain("Run tests");
    expect(html).toContain("Submit answers");
  });
});
