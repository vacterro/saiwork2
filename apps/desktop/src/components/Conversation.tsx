import { Children, memo, useEffect, useLayoutEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { Message } from "../state/store";
import type { SliceProps } from "../state/slices";
import { ToolActivityView } from "./ToolActivity";
import { commands } from "../app/backend";
import { loadSessionHistory } from "../app/sessionSelection";

/** Relative timestamp: "now", "2m ago", "1h ago", "3d ago", "5w ago". */
export function timeAgo(ts: number, now = Date.now()): string {
  const diff = now - ts;
  if (diff < 0) return "now";
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return "now";
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day < 7) return `${day}d ago`;
  const week = Math.floor(day / 7);
  if (week < 52) return `${week}w ago`;
  return `${Math.floor(week / 52)}y ago`;
}

/** Exact local time plus human age, shown together for every message/tool. */
export function timestampLabel(ts: number, now = Date.now()): string {
  return `${new Date(ts).toLocaleString()} (${timeAgo(ts, now)})`;
}

/** One definition of what the conversation consumes (state/slices.ts). */
export const conversationKeys = [
  "activeSessionId",
  "activeMessage",
  "historyStatus",
  "engines",
  "messages",
  "running",
  "runningStale",
  "sessions",
] as const;

type Props = SliceProps<(typeof conversationKeys)[number]>;

/** §75/§241: the conversation must not re-render for events that do not touch
 * its slice (engine health, other sessions, queue) — but it MUST re-render for
 * everything it displays, including `historyStatus` (loading / unavailable /
 * error), which the previous comparator ignored, and the live streaming tail.
 * Session-scoped by design: another session's stream is not this view's data. */
export function conversationEqual(prev: Props, next: Props): boolean {
  if (prev.onError !== next.onError) return false;
  if (prev.state.activeSessionId !== next.state.activeSessionId) return false;
  if (prev.state.engines !== next.state.engines) return false;
  if (prev.state.sessions !== next.state.sessions) return false;
  if (prev.state.runningStale !== next.state.runningStale) return false;
  const sid = next.state.activeSessionId;
  if (!sid) return true;
  return (
    prev.state.messages[sid] === next.state.messages[sid] &&
    prev.state.activeMessage[sid] === next.state.activeMessage[sid] &&
    prev.state.running[sid] === next.state.running[sid] &&
    prev.state.historyStatus[sid] === next.state.historyStatus[sid]
  );
}

const STICK_THRESHOLD_PX = 90;

/** Bounded transcript windowing (TASK 24 perf): only the newest `WINDOW`
 * messages are mounted; earlier history stays in the store projection and is
 * exposed deterministically via “Load earlier”. The active/streaming message
 * is always the last one, so it is permanently in the rendered window. */
const WINDOW = 50;

export function Conversation({ state, onError }: Props) {
  // The completed transcript and the live streaming tail are SEPARATE slices
  // (store.ts): the window is sliced out of the completed history, and the tail
  // is appended for rendering only. No per-token copy of the history array.
  const completed = state.activeSessionId ? (state.messages[state.activeSessionId] ?? []) : [];
  const tail = state.activeSessionId ? (state.activeMessage[state.activeSessionId] ?? null) : null;
  const containerRef = useRef<HTMLDivElement>(null);
  const sessionId = state.activeSessionId;
  const running = sessionId ? Boolean(state.running[sessionId]) : false;
  const [stick, setStick] = useState(true);
  const [showJump, setShowJump] = useState(false);
  const [historyBusy, setHistoryBusy] = useState(false);
  // One bounded clock for the whole transcript. Passing it through memoized
  // rows keeps every visible “ago” label current without one timer/listener
  // per message or tool call.
  const [relativeNow, setRelativeNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setRelativeNow(Date.now()), 60_000);
    return () => window.clearInterval(timer);
  }, []);
  // Each “Load earlier” click reveals one more WINDOW-sized chunk from the
  // top. Reset when switching sessions.
  const [extraChunks, setExtraChunks] = useState(0);
  // Scroll-anchor restoration: the element that was first before expansion +
  // the scroll offset at the moment of the click.
  const anchorRef = useRef<{ id: string; scrollTop: number } | null>(null);
  const windowSize = WINDOW * (1 + extraChunks);
  const start = Math.max(0, completed.length - windowSize);
  const visibleCompleted = start > 0 ? completed.slice(start) : completed;
  const lastTextLen = (tail ?? completed[completed.length - 1])?.text.length ?? 0;
  const messageCount = completed.length + (tail ? 1 : 0);
  const session = state.sessions.find((item) => item.id === sessionId) ?? null;
  const engine = state.engines.find((item) => item.id === session?.engine_id) ?? null;
  const canRevert = Boolean(engine?.capabilities.session_revert) && !running && !state.runningStale;

  function changeRevert(direction: "undo" | "redo") {
    if (!sessionId || !canRevert || historyBusy) return;
    setHistoryBusy(true);
    const command = direction === "undo"
      ? commands.revertLastTurn(sessionId)
      : commands.unrevertSession(sessionId);
    void command
      .then(() => loadSessionHistory(sessionId))
      .catch((error) => onError(String(error)))
      .finally(() => setHistoryBusy(false));
  }

  // Session switch / new message → follow the newest content.
  useEffect(() => {
    setStick(true);
    setShowJump(false);
    setExtraChunks(0);
    scrollToBottom();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId, messageCount]);

  // After “Load earlier” mounts the new chunk, keep the previously-first
  // message at the same viewport position (it moved down by the height of
  // the newly prepended content).
  useLayoutEffect(() => {
    const anchor = anchorRef.current;
    if (!anchor) return;
    anchorRef.current = null;
    const el = containerRef.current;
    if (!el) return;
    const target = el.querySelector(`[data-mid="${anchor.id}"]`) as HTMLElement | null;
    if (target) el.scrollTop = anchor.scrollTop + target.offsetTop;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [extraChunks]);

  function loadEarlier() {
    const el = containerRef.current;
    const first = el?.querySelector(".conversation__list [data-mid]") as HTMLElement | null;
    anchorRef.current = first ? { id: first.dataset.mid ?? "", scrollTop: el!.scrollTop } : null;
    setExtraChunks((e) => e + 1);
  }

  // While streaming, follow only when the user is near the bottom (§24).
  useEffect(() => {
    if (running && stick) scrollToBottom();
  }, [running, stick, lastTextLen]);

  function scrollToBottom() {
    const el = containerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }

  function onScroll() {
    const el = containerRef.current;
    if (!el) return;
    const distance = el.scrollHeight - el.scrollTop - el.clientHeight;
    const nearBottom = distance < STICK_THRESHOLD_PX;
    setStick(nearBottom);
    setShowJump(!nearBottom);
  }

  if (!sessionId) {
    return (
      <main className="conversation conversation--empty">
        <p>No active session.</p>
        <p className="muted">Send a prompt and a session will be created automatically.</p>
      </main>
    );
  }

  // Authoritative-history truth (TASK 24 §9): an empty message list must not
  // masquerade as a complete empty thread. Show the engine-history status
  // explicitly while it is loading, unavailable (no capability) or errored.
  const hist = state.historyStatus[sessionId];
  if (messageCount === 0 && (hist === "loading" || hist === "unavailable" || hist === "error")) {
    return (
      <main className="conversation conversation--empty">
        {hist === "loading" && <p className="muted">Loading conversation history…</p>}
        {hist === "unavailable" && (
          <>
            <p>Conversation history unavailable.</p>
            <p className="muted">This engine does not expose session history — only new messages are shown.</p>
          </>
        )}
        {hist === "error" && (
          <>
            <p>Could not load conversation history.</p>
            <p className="muted">The authoritative history read failed — select the session again to retry.</p>
          </>
        )}
      </main>
    );
  }

  return (
    <main className="conversation">
      {engine?.capabilities.session_revert && (
        <div className="conversation__history-actions">
          <button className="btn btn--small" disabled={!canRevert || historyBusy} onClick={() => changeRevert("undo")}>
            Undo last turn
          </button>
          <button className="btn btn--small" disabled={!canRevert || historyBusy} onClick={() => changeRevert("redo")}>
            Redo
          </button>
        </div>
      )}
      <div className="conversation__scroll" ref={containerRef} onScroll={onScroll}>
        <div className="conversation__list">
          {start > 0 && (
            <button
              className="btn btn--small conversation__load-earlier"
              onClick={loadEarlier}
            >
              Load earlier ({start} hidden)
            </button>
          )}
          {visibleCompleted.map((m) => (
            <MessageView key={m.id} message={m} sessionId={sessionId} canResolve={running} now={relativeNow} onError={onError} />
          ))}
          {tail && (
            <MessageView key={tail.id} message={tail} sessionId={sessionId} canResolve={running} now={relativeNow} onError={onError} />
          )}
        </div>
      </div>
      {showJump && (
        <button className="conversation__jump btn btn--small" onClick={() => { setStick(true); setShowJump(false); scrollToBottom(); }}>
          Jump to latest ↓
        </button>
      )}
    </main>
  );
}

const MessageView = memo(function MessageView({
  message,
  sessionId,
  canResolve,
  now,
  onError,
}: {
  message: Message;
  sessionId: string;
  canResolve: boolean;
  now: number;
  onError: (message: string) => void;
}) {
  if (message.role === "user") {
    return (
      <div className="msg msg--user" data-mid={message.id}>
        <div className="msg__bubble">
          <span className="msg__time" title={message.ts ? new Date(message.ts).toISOString() : undefined}>
            {message.ts ? timestampLabel(message.ts, now) : ""}
          </span>
          {message.text}
          {message.uncertain && (
            <span className="msg__uncertain" title="The send outcome was unproven — this prompt may still be executing upstream; it will reconcile from the engine stream.">
              ⚠ pending…
            </span>
          )}
        </div>
      </div>
    );
  }
  const streaming = message.status === "streaming";
  const statusLabel = streaming
    ? "streaming…"
    : message.status === "failed"
      ? "failed"
      : message.status === "cancelled"
        ? "interrupted"
        : message.status === "outcome_unknown"
          ? "outcome unknown"
          : "complete";
  return (
<div className={`msg msg--assistant msg--${message.status}`} data-mid={message.id}>
        <div className="msg__meta">
          <span>assistant</span>
          <span className={`status status--${message.status}`}>{statusLabel}</span>
          {message.ts > 0 && (
            <span className="msg__time" title={new Date(message.ts).toISOString()}>
              {timestampLabel(message.ts, now)}
            </span>
          )}
        {!streaming && message.text && (
          <button
            className="btn btn--small msg__copy"
            title="Copy message text"
            onClick={() => void navigator.clipboard?.writeText(message.text)}
          >
            Copy
          </button>
        )}
        {message.error && <span className="msg__error">{message.error}</span>}
      </div>
      {/* Streaming renders plain text (cheap per batched frame); Markdown is
          finalized at terminal — §28. */}
      {message.text &&
        (streaming ? <pre className="msg__text">{message.text}</pre> : <MarkdownBody text={message.text} />)}
      {message.tools.length > 0 && (
        <div className="tools">
          {message.tools.map((t) => (
            <ToolActivityView key={t.id} tool={t} now={now} />
          ))}
        </div>
      )}
      {message.permissions.length > 0 && (
        <div className="perms">
          {message.permissions.map((pe) => (
            <PermissionRow key={pe.requestId} requestId={pe.requestId} detail={pe.detail} allowed={pe.allowed} sessionId={sessionId} canResolve={canResolve} onError={onError} />
          ))}
        </div>
      )}
      {message.questions.length > 0 && (
        <div className="perms">
          {message.questions.map((q) => (
            <QuestionRow key={q.requestId} requestId={q.requestId} detail={q.detail} resolved={q.resolved} sessionId={sessionId} canResolve={canResolve} onError={onError} />
          ))}
        </div>
      )}
    </div>
  );
});

function MarkdownBody({ text }: { text: string }) {
  return (
    <div className="md">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          // Fenced code blocks: the `pre` wrapper owns the language + copy.
          pre({ children }) {
            const child = Children.only(children) as React.ReactElement<{
              className?: string;
              children?: React.ReactNode;
            }>;
            const match = /language-(\w+)/.exec(child?.props?.className ?? "");
            const code = String(child?.props?.children ?? "").replace(/\n$/, "");
            return <CodeBlock lang={match?.[1] ?? ""} code={code} />;
          },
          // Inline code stays plain and safe.
          code({ children }) {
            return <code>{children}</code>;
          },
          a({ href, children }) {
            return (
              <a href={href} target="_blank" rel="noreferrer">
                {children}
              </a>
            );
          },
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  );
}

function CodeBlock({ lang, code }: { lang: string; code: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="codeblock">
      <div className="codeblock__head">
        <span className="codeblock__lang">{lang || "text"}</span>
        <button
          className="btn btn--small"
          onClick={() => {
            void navigator.clipboard?.writeText(code).then(() => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1500);
            });
          }}
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre className="codeblock__pre">
        <code>{code}</code>
      </pre>
    </div>
  );
}

function PermissionRow({
  requestId,
  detail,
  allowed,
  sessionId,
  canResolve,
  onError,
}: {
  requestId: string;
  detail: string;
  allowed: boolean | null;
  sessionId: string;
  canResolve: boolean;
  onError: (message: string) => void;
}) {
  const [busy, setBusy] = useState(false);
  if (allowed !== null) {
    return (
      <div className="perm perm--resolved">
        <span className="perm__detail">{detail}</span>
        <span className="perm__status">{allowed ? "allowed" : "denied"}</span>
      </div>
    );
  }
  // Pending: only the active run can still resolve it (§38 — a dead run's
  // engine releases it; the button disappears once resolved).
  return (
    <div className="perm">
      <span className="perm__detail">{detail}</span>
      <div className="perm__actions">
        <button
          className="btn btn--small"
          disabled={busy || !canResolve}
          onClick={() => {
            setBusy(true);
            void commands
              .resolvePermission(sessionId, requestId, true)
              // A failed resolution must surface through the normal visible
              // error path (TASK 24 §9): the request stays PENDING and
              // retryable — never a swallowed failure that makes the agent
              // look hung at its most security-sensitive interaction.
              .catch((e) => onError(String(e)))
              .finally(() => setBusy(false));
          }}
        >
          Allow
        </button>
        <button
          className="btn btn--small"
          disabled={busy || !canResolve}
          onClick={() => {
            setBusy(true);
            void commands
              .resolvePermission(sessionId, requestId, false)
              .catch((e) => onError(String(e)))
              .finally(() => setBusy(false));
          }}
        >
          Deny
        </button>
      </div>
    </div>
  );
}

interface AgentQuestionOption {
  label: string;
  description: string;
}

interface AgentQuestion {
  header: string;
  question: string;
  options: AgentQuestionOption[];
  multiple: boolean;
  custom: boolean;
}

/** Parse only the bounded OpenCode question surface the generic UI supports. */
export function parseAgentQuestions(detail: string): AgentQuestion[] {
  const MAX_QUESTIONS = 8;
  const MAX_OPTIONS = 16;
  try {
    const parsed = JSON.parse(detail) as { questions?: unknown };
    if (!Array.isArray(parsed.questions)) return [];
    return parsed.questions.slice(0, MAX_QUESTIONS).flatMap((raw) => {
      if (!raw || typeof raw !== "object") return [];
      const q = raw as Record<string, unknown>;
      if (typeof q.question !== "string" || q.question.trim().length === 0) return [];
      const options = Array.isArray(q.options)
        ? q.options.slice(0, MAX_OPTIONS).flatMap((rawOption) => {
            if (!rawOption || typeof rawOption !== "object") return [];
            const option = rawOption as Record<string, unknown>;
            if (typeof option.label !== "string" || option.label.length === 0) return [];
            return [{
              label: option.label,
              description: typeof option.description === "string" ? option.description : "",
            }];
          })
        : [];
      return [{
        header: typeof q.header === "string" ? q.header : "Question",
        question: q.question,
        options,
        multiple: q.multiple === true,
        custom: q.custom !== false,
      }];
    });
  } catch {
    return [];
  }
}

/** One structured question request. All questions are answered atomically;
 * partial button clicks never resume the engine with a malformed answer. */
function QuestionRow({
  requestId,
  detail,
  resolved,
  sessionId,
  canResolve,
  onError,
}: {
  requestId: string;
  detail: string;
  resolved: boolean | null;
  sessionId: string;
  canResolve: boolean;
  onError: (message: string) => void;
}) {
  const [busy, setBusy] = useState(false);
  const questions = parseAgentQuestions(detail);
  const [answers, setAnswers] = useState<string[][]>(() => questions.map(() => []));
  const [custom, setCustom] = useState<string[]>(() => questions.map(() => ""));
  useEffect(() => {
    setAnswers(questions.map(() => []));
    setCustom(questions.map(() => ""));
  }, [detail]);
  if (resolved !== null) {
    return (
      <div className="perm perm--resolved">
        <span className="perm__detail">
          {questions.map((q) => q.question).join(" · ") || detail}
        </span>
        <span className="perm__status">{resolved ? "answered" : "rejected"}</span>
      </div>
    );
  }
  const resolve = (submitted: string[][] | null) => {
    setBusy(true);
    void commands
      .resolveQuestion(sessionId, requestId, submitted)
      // Same visible-error rule as permissions (TASK 24 §9): a failed
      // resolution stays PENDING and retryable — never swallowed.
      .catch((e) => onError(String(e)))
      .finally(() => setBusy(false));
  };
  const submittedAnswers = questions.map((question, index) => {
    const value = custom[index]?.trim();
    if (!value) return answers[index] ?? [];
    return question.multiple ? [...(answers[index] ?? []), value] : [value];
  });
  const complete = questions.length > 0 && submittedAnswers.every((answer) => answer.length > 0);
  return (
    <div className="perm">
      {questions.length === 0 ? (
        <span className="perm__detail">{detail}</span>
      ) : (
        <div className="question-card">
          {questions.map((question, questionIndex) => (
            <fieldset className="question-card__question" key={`${question.header}-${questionIndex}`}>
              <legend>{question.header}</legend>
              <p className="question-card__prompt">{question.question}</p>
              {question.options.map((option) => {
                const selected = answers[questionIndex]?.includes(option.label) ?? false;
                return (
                  <label className="question-card__option" key={option.label}>
                    <input
                      type={question.multiple ? "checkbox" : "radio"}
                      name={`${requestId}-${questionIndex}`}
                      checked={selected}
                      disabled={busy || !canResolve}
                      onChange={() => setAnswers((current) => current.map((answer, index) => {
                        if (index !== questionIndex) return answer;
                        if (!question.multiple) return [option.label];
                        return selected
                          ? answer.filter((label) => label !== option.label)
                          : [...answer, option.label];
                      }))}
                    />
                    <span><strong>{option.label}</strong>{option.description && ` — ${option.description}`}</span>
                  </label>
                );
              })}
              {question.custom && (
                <input
                  className="question-card__custom"
                  value={custom[questionIndex] ?? ""}
                  disabled={busy || !canResolve}
                  placeholder="Custom answer"
                  aria-label={`${question.header} custom answer`}
                  onChange={(event) => setCustom((current) => current.map((value, index) => (
                    index === questionIndex ? event.target.value : value
                  )))}
                />
              )}
            </fieldset>
          ))}
        </div>
      )}
      <div className="perm__actions">
        {questions.length > 0 && (
          <button
            className="btn btn--small"
            disabled={busy || !canResolve || !complete}
            onClick={() => resolve(submittedAnswers)}
          >
            Submit answers
          </button>
        )}
        <button
          className="btn btn--small"
          disabled={busy || !canResolve}
          onClick={() => resolve(null)}
        >
          Reject
        </button>
      </div>
    </div>
  );
}
