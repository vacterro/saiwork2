//! NDJSON JSON-RPC 2.0 transport over the supervisor's protocol pipe
//! (TASK 20 §21–§31, §75–§80; TASK 21 notification/server-request routing).
//!
//! One transport per runtime generation. The reader is exactly one task per
//! runtime (never one per request, §77); writes go through the supervisor's
//! serialized stdin (`ManagedProcess::stdin_write_all`, one writer owner,
//! §78). Framing is the documented ACP contract: one JSON object per
//! `\n`-terminated line; arbitrary byte chunking, fragmentation and UTF-8
//! splits are handled by the reader's line buffer (§22).
//!
//! Guarantees:
//! - every request gets a unique id from one correlation authority (§24–§25);
//! - pending requests always settle: response, timeout, or runtime death
//!   (§26) — the pending map is bounded by in-flight requests, each with a
//!   deadline (§80);
//! - duplicate/unknown response ids never resolve a request twice (§30–§31);
//! - `session/update` notifications are routed into a bounded channel and
//!   dispatched by session id (TASK 21 §33–§34); a full channel drops the
//!   frame with a coalesced overflow counter — stream-class deltas are
//!   batchable and the prompt response remains the terminal authority
//!   (§101–§102);
//! - `session/request_permission` (and any other) server requests are routed
//!   into a bounded channel; a full channel is answered `-32601` (reject,
//!   fail-closed) so the reader never blocks and a flood is safely denied
//!   (§57, §101);
//! - other unknown notifications are ignored safely (§27–§28);
//! - malformed/oversized frames kill the transport deterministically —
//!   fail-safe reset when framing synchronization is uncertain (§29/§92);
//! - the raw protocol channel is bounded (backpressure, §75/§103).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saiwork_process::ManagedProcess;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

use crate::error::HarnessError;
use crate::protocol::{
    Incoming, Request, ServerRequest, SessionUpdateNotification, METHOD_SESSION_UPDATE,
};

/// Default per-frame cap (both directions). Generous; documented in
/// KNOWLEDGE/DEEPSEEK_HARNESS.md.
pub const DEFAULT_FRAME_CAP_BYTES: usize = 1024 * 1024;

struct Pending {
    method: String,
    tx: oneshot::Sender<Result<Value, HarnessError>>,
}

/// Handle to a two-phase request: the frame was written (NOT acceptance — the
/// runtime may still reject the turn; the caller decides acceptance from
/// execution evidence, TASK 24 §9) and `await_response` resolves when the
/// runtime answers, times out, or dies.
pub struct RequestHandle {
    inner: Arc<Inner>,
    id: u64,
    method: String,
    timeout: Duration,
    rx: Option<oneshot::Receiver<Result<Value, HarnessError>>>,
}

/// Bounded capacity of the routed `session/update` and server-request
/// channels. Generous; overflow is handled (drop-with-counter for stream
/// notifications, fail-closed reject for server requests).
const ROUTE_CHANNEL_CAPACITY: usize = 1024;
/// Capacity of the NON-DROPPABLE tool lifecycle lane (TASK 24 §9). Tool
/// updates are rare vs. text chunks; the lane awaits capacity (bounded
/// backpressure on the reader) rather than dropping a terminal tool fact.
const TOOL_LANE_CAPACITY: usize = 64;

struct Inner {
    generation: u64,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    dead_tx: watch::Sender<Option<String>>,
    dead_rx: watch::Receiver<Option<String>>,
    process: Arc<ManagedProcess>,
    stop_tx: mpsc::Sender<()>,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
    frame_cap: usize,
    /// Routed `session/update` notifications (TASK 21 §33–§34). Bounded;
    /// overflow is counted, never buffered without limit.
    session_events_tx: mpsc::Sender<SessionUpdateNotification>,
    /// NON-DROPPABLE state lane for tool lifecycle updates (TASK 24 §9): a
    /// dropped `ToolCall completed/failed` would leave the UI tool
    /// permanently `started/output` because the final prompt response only
    /// terminalizes the run and carries no per-tool reconstruction. Tool
    /// facts are rare (vs. text chunks), so this lane awaits capacity
    /// (bounded backpressure) instead of dropping.
    tool_events_tx: mpsc::Sender<SessionUpdateNotification>,
    /// Routed server→client requests (e.g. `session/request_permission`).
    server_requests_tx: mpsc::Sender<ServerRequest>,
    /// Coalesced overflow counters (read by the dispatcher/handler tasks so a
    /// dropped frame is never silent — §102).
    dropped_events: AtomicU64,
    rejected_requests: AtomicU64,
}

/// Cloneable handle to the transport; all state is shared behind `Arc`.
#[derive(Clone)]
pub struct Transport {
    inner: Arc<Inner>,
}

impl Transport {
    /// Construct the transport, spawn the single reader task, and return a
    /// dead-watch receiver for the runtime monitor. The raw protocol stream
    /// is owned by the reader from here on.
    /// Construct the transport, spawn the single reader task, and return the
    /// dead-watch receiver plus the routed session-event and server-request
    /// receivers. The raw protocol stream is owned by the reader from here on.
    pub fn new(
        generation: u64,
        process: Arc<ManagedProcess>,
        protocol_rx: mpsc::Receiver<Vec<u8>>,
        frame_cap: usize,
    ) -> (
        Self,
        watch::Receiver<Option<String>>,
        mpsc::Receiver<SessionUpdateNotification>,
        mpsc::Receiver<SessionUpdateNotification>,
        mpsc::Receiver<ServerRequest>,
    ) {
        let (dead_tx, dead_rx) = watch::channel(None);
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let (session_events_tx, session_events_rx) = mpsc::channel(ROUTE_CHANNEL_CAPACITY);
        let (tool_events_tx, tool_events_rx) = mpsc::channel(TOOL_LANE_CAPACITY);
        let (server_requests_tx, server_requests_rx) = mpsc::channel(ROUTE_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            generation,
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            dead_tx,
            dead_rx: dead_rx.clone(),
            process,
            stop_tx,
            reader: Mutex::new(None),
            frame_cap,
            session_events_tx,
            tool_events_tx,
            server_requests_tx,
            dropped_events: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
        });
        let reader_inner = inner.clone();
        let handle = tokio::spawn(async move {
            reader_loop(&reader_inner, protocol_rx, stop_rx).await;
        });
        *inner.reader.lock().expect("reader mutex poisoned") = Some(handle);
        (
            Transport { inner },
            dead_rx,
            session_events_rx,
            tool_events_rx,
            server_requests_rx,
        )
    }

    /// Coalesced count of `session/update` frames dropped on a full route
    /// channel (read by the dispatcher; nonzero ⇒ a bounded warning).
    pub fn dropped_events(&self) -> u64 {
        self.inner.dropped_events.load(Ordering::SeqCst)
    }

    /// Number of `session/update` notifications still buffered in the route
    /// channel (used by the prompt task to drain final chunks before the
    /// terminal, §36–§37/§130). Never negative; `0` when the channel is empty.
    pub fn route_pending(&self) -> usize {
        let tx = &self.inner.session_events_tx;
        tx.max_capacity().saturating_sub(tx.capacity())
    }

    /// Number of tool-lifecycle updates still buffered in the NON-DROPPABLE
    /// state lane (the prompt task drains both lanes before the terminal so
    /// no tool terminal fact is ever lost to teardown, TASK 24 §9).
    pub fn tool_route_pending(&self) -> usize {
        let tx = &self.inner.tool_events_tx;
        tx.max_capacity().saturating_sub(tx.capacity())
    }

    /// Coalesced count of server requests rejected on a full route channel.
    pub fn rejected_requests(&self) -> u64 {
        self.inner.rejected_requests.load(Ordering::SeqCst)
    }

    /// Send a client→server notification (e.g. `session/cancel`).
    pub async fn notify(&self, method: &str, params: Value) -> Result<(), HarnessError> {
        let inner = &self.inner;
        if let Some(reason) = inner.dead_rx.borrow().clone() {
            return Err(HarnessError::RuntimeLost(reason));
        }
        let frame = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        write_frame(inner, &frame)
            .await
            .map_err(HarnessError::TransportClosed)
    }

    /// Respond to a server→client request (e.g. a permission decision).
    pub async fn respond(&self, id: u64, result: Value) -> Result<(), HarnessError> {
        let inner = &self.inner;
        let frame = json!({ "jsonrpc": "2.0", "id": id, "result": result });
        write_frame(inner, &frame)
            .await
            .map_err(HarnessError::TransportClosed)
    }

    /// Respond to a server→client request with an error (fail-closed path).
    pub async fn respond_error(
        &self,
        id: u64,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        let inner = &self.inner;
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message }
        });
        write_frame(inner, &frame)
            .await
            .map_err(HarnessError::TransportClosed)
    }

    pub fn generation(&self) -> u64 {
        self.inner.generation
    }

    /// A watch of the transport death reason: `None` while alive, then
    /// `Some(reason)` when the transport becomes unusable (EOF, malformed
    /// frame, process exit, explicit close). Every pending request observes
    /// this and settles with `RuntimeLost`.
    pub fn dead(&self) -> watch::Receiver<Option<String>> {
        self.inner.dead_rx.clone()
    }

    pub fn is_dead(&self) -> Option<String> {
        self.inner.dead_rx.borrow().clone()
    }

    /// Bounded JSON-RPC request: unique id, deadline, runtime-death
    /// awareness. The pending entry is removed on response, timeout, or
    /// death — never leaked (§26/§74).
    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, HarnessError> {
        self.request_start(method, params, timeout)
            .await?
            .await_response()
            .await
    }

    /// Two-phase request: write the request frame and return a handle that
    /// awaits the response. A successful write proves only that the bytes
    /// were delivered to the pipe — it is NOT acceptance (the runtime may
    /// still reject the turn, and the same request can later resolve with
    /// `RequestRejected`). Callers must derive acceptance from actual
    /// execution evidence (first routed session/update, or a successful final
    /// response). A write failure means nothing was sent.
    pub async fn request_start(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<RequestHandle, HarnessError> {
        let inner = &self.inner;
        if let Some(reason) = inner.dead_rx.borrow().clone() {
            return Err(HarnessError::RuntimeLost(reason));
        }
        let id = inner.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = inner.pending.lock().expect("pending mutex poisoned");
            pending.insert(
                id,
                Pending {
                    method: method.into(),
                    tx,
                },
            );
        }
        let frame = Request::new(id, method, Some(params));
        if let Err(reason) = write_frame(inner, &frame).await {
            inner
                .pending
                .lock()
                .expect("pending mutex poisoned")
                .remove(&id);
            return Err(HarnessError::TransportClosed(reason));
        }
        Ok(RequestHandle {
            inner: inner.clone(),
            id,
            method: method.to_string(),
            timeout,
            rx: Some(rx),
        })
    }

    /// Clean teardown: settle pending as dead, stop the reader, join it
    /// (bounded). Idempotent. On join timeout the reader is ABORTED and
    /// awaited — dropping a JoinHandle only detaches, and old-generation
    /// protocol work must never survive teardown (TASK 24 §9).
    pub async fn close(&self, reason: &str) {
        let _ = self.inner.dead_tx.send(Some(reason.into()));
        let _ = self.inner.stop_tx.send(()).await;
        let handle = self
            .inner
            .reader
            .lock()
            .expect("reader mutex poisoned")
            .take();
        if let Some(mut handle) = handle {
            // Await by reference (JoinHandle's Future consumes the handle);
            // on timeout abort and await the cancellation — a completed
            // handle must never be polled again.
            let pinned = std::pin::Pin::new(&mut handle); // JoinHandle is Unpin
            if tokio::time::timeout(Duration::from_secs(5), pinned).await.is_err() {
                warn!(
                    generation = self.inner.generation,
                    "transport reader join timed out; aborting reader task"
                );
                handle.abort();
                let _ = handle.await;
            }
        }
    }

    /// Bounded in-flight request count (tests + diagnostics).
    pub fn pending_count(&self) -> usize {
        self.inner
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .len()
    }
}

impl RequestHandle {
    /// Await the response to a two-phase request. `biased`: a real response
    /// wins over a concurrently-set death/timeout (frames are processed in
    /// order; a response already received for our id resolves the request
    /// even if a later malformed frame kills the transport). The pending
    /// entry is removed on response, timeout, or death — never leaked.
    pub async fn await_response(mut self) -> Result<Value, HarnessError> {
        let rx = self.rx.take().expect("await_response called once");
        let mut dead_rx = self.inner.dead_rx.clone();
        let id = self.id;
        tokio::select! {
            biased;
            r = rx => match r {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(HarnessError::TransportClosed("sender dropped".into())),
            },
            _ = tokio::time::sleep(self.timeout) => {
                self.inner.pending.lock().expect("pending mutex poisoned").remove(&id);
                Err(HarnessError::RequestTimeout { method: self.method.clone(), timeout: self.timeout })
            }
            _ = dead_rx.changed() => {
                let reason = dead_rx.borrow().clone().unwrap_or_else(|| "transport closed".into());
                self.inner.pending.lock().expect("pending mutex poisoned").remove(&id);
                Err(HarnessError::RuntimeLost(reason))
            }
        }
    }
}

/// The single reader task: chunk → line buffer → frame routing.
async fn reader_loop(
    inner: &Arc<Inner>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    mut stop_rx: mpsc::Receiver<()>,
) {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        tokio::select! {
            _ = stop_rx.recv() => {
                // Explicit clean stop (transport close).
                break;
            }
            chunk = rx.recv() => {
                let Some(chunk) = chunk else {
                    // Protocol EOF: supervisor reader ended (process exit or
                    // pipe close). Transport is dead.
                    let _ = inner.dead_tx.send(Some("protocol EOF".into()));
                    break;
                };
                buf.extend_from_slice(&chunk);
                let mut consumed = 0;
                while let Some(rel) = find_byte(&buf[consumed..], b'\n') {
                    let line_end = consumed + rel;
                    let line = &buf[consumed..line_end];
                    consumed = line_end + 1;
                    if line.is_empty() {
                        continue;
                    }
                    if line.len() > inner.frame_cap {
                        let _ = inner.dead_tx.send(Some(format!(
                            "protocol frame exceeds the {} byte cap",
                            inner.frame_cap
                        )));
                        return;
                    }
                    if let Err(reason) = handle_frame(inner, line).await {
                        let _ = inner.dead_tx.send(Some(reason));
                        return;
                    }
                }
                buf.drain(..consumed);
                // Bound the partial-line buffer (UTF-8 splits and slow
                // writers cannot grow it without bound).
                if buf.len() > inner.frame_cap {
                    let _ = inner.dead_tx.send(Some(format!(
                        "partial protocol frame exceeds the {} byte cap",
                        inner.frame_cap
                    )));
                    return;
                }
            }
        }
    }
}

fn find_byte(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Route one complete frame (TASK 20 §28–§31).
async fn handle_frame(inner: &Arc<Inner>, line: &[u8]) -> Result<(), String> {
    let frame: Incoming =
        serde_json::from_slice(line).map_err(|e| format!("malformed protocol frame: {e}"))?;
    match (frame.id.is_some(), frame.method.as_deref()) {
        // Response to one of our requests.
        (true, None) => {
            let id = frame
                .id
                .as_ref()
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "response id is not a u64".to_string())?;
            let mut pending = inner.pending.lock().expect("pending mutex poisoned");
            if let Some(p) = pending.remove(&id) {
                let outcome = if let Some(err) = frame.error {
                    Err(HarnessError::RequestRejected {
                        method: p.method.clone(),
                        code: err.code,
                        message: err.message,
                    })
                } else {
                    Ok(frame.result.unwrap_or(Value::Null))
                };
                let _ = p.tx.send(outcome);
            } else {
                // Unknown / duplicate / stale response id: never hand it to a
                // random waiter; bounded diagnostic (§30–§31).
                warn!(
                    generation = inner.generation,
                    id, "response for unknown or stale request id"
                );
            }
            Ok(())
        }
        // Server → client request. `session/request_permission` is routed to
        // the permission handler (TASK 21 §55–§60); everything else is
        // answered -32601 so the server never hangs on an unsupported method.
        (true, Some(_)) => {
            let id = frame
                .id
                .as_ref()
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "request id is not a u64".to_string())?;
            let method = frame.method.clone().unwrap_or_default();
            if method == crate::protocol::METHOD_SESSION_REQUEST_PERMISSION {
                if inner
                    .server_requests_tx
                    .try_send(ServerRequest {
                        id,
                        method,
                        params: frame.params.unwrap_or(Value::Null),
                    })
                    .is_err()
                {
                    // Route channel full (flood): fail closed — reject the
                    // request so the agent's tool call is denied and the
                    // reader never blocks (§57, §101).
                    inner.rejected_requests.fetch_add(1, Ordering::SeqCst);
                    let resp = json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": -32601, "message": "permission channel full (rejected)" }
                    });
                    write_frame(inner, &resp).await?;
                }
                return Ok(());
            }
            debug!(
                generation = inner.generation,
                method, id, "answering unsupported server request"
            );
            let resp = json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": "Method not found (SAIWORK2)" }
            });
            write_frame(inner, &resp).await?;
            Ok(())
        }
        // Notification: `session/update` is routed to the dispatcher; other
        // notifications are ignored safely (§27–§28).
        (false, Some(method)) => {
            if method == METHOD_SESSION_UPDATE {
                match serde_json::from_value::<SessionUpdateNotification>(
                    frame.params.unwrap_or(Value::Null),
                ) {
                    Ok(notification) => {
                        // TASK 24 §9: tool lifecycle updates (including
                        // completed/failed terminals) go to the NON-DROPPABLE
                        // state lane and await capacity. Text chunks stay on
                        // the drop-with-counter stream lane (batchable; the
                        // prompt response is the terminal authority —
                        // §101–§102).
                        if matches!(&notification.update, crate::protocol::SessionUpdate::ToolCall { .. }) {
                            if inner.tool_events_tx.send(notification).await.is_err() {
                                // Consumer gone (teardown): the run is being
                                // settled by the prompt task; nothing to
                                // retry.
                                debug!(
                                    generation = inner.generation,
                                    "tool lane closed (teardown); tool update not routed"
                                );
                            }
                        } else if inner.session_events_tx.try_send(notification).is_err() {
                            // Route channel full: the frame is dropped with a
                            // coalesced counter (stream-class deltas are
                            // batchable).
                            inner.dropped_events.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Err(_) => {
                        // Malformed session/update params: bounded diagnostic,
                        // never a crash and never a transport reset (the
                        // prompt response stays authoritative).
                        warn!(
                            generation = inner.generation,
                            "malformed session/update notification (ignored)"
                        );
                    }
                }
            } else {
                debug!(
                    generation = inner.generation,
                    method, "ignoring protocol notification"
                );
            }
            Ok(())
        }
        (false, None) => Err("frame has neither method nor id".into()),
    }
}

/// Serialize + frame one outbound JSON-RPC message (bounded, never string
/// concatenation — §129–§130).
async fn write_frame<T: serde::Serialize>(inner: &Arc<Inner>, value: &T) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(value).map_err(|e| format!("serialize failed: {e}"))?;
    if bytes.len() + 1 > inner.frame_cap {
        return Err(format!(
            "outbound frame exceeds the {} byte cap",
            inner.frame_cap
        ));
    }
    bytes.push(b'\n');
    inner
        .process
        .stdin_write_all(&bytes)
        .await
        .map_err(|e| format!("stdin write failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use saiwork_events::EventBus;
    use saiwork_process::{ProcessSpec, ProcessSupervisor, StdinPolicy};
    use tokio::time::timeout;

    fn frame(params: Value) -> Vec<u8> {
        let mut line =
            serde_json::to_vec(&json!({ "jsonrpc": "2.0", "method": "session/update", "params": params }))
                .unwrap();
        line.push(b'\n');
        line
    }

    fn chunk_frame(session: &str, text: &str) -> Vec<u8> {
        frame(json!({
            "sessionId": session,
            "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": text } }
        }))
    }

    fn tool_frame(session: &str, id: &str, status: &str) -> Vec<u8> {
        frame(json!({
            "sessionId": session,
            "update": { "sessionUpdate": "tool_call", "toolCall": { "toolCallId": id, "name": "bash", "status": status } }
        }))
    }

    /// TASK 24 §9: when the drop-with-counter stream lane is FULL, tool
    /// lifecycle updates (including the completed terminal) must still be
    /// routed through the NON-DROPPABLE state lane. Frames are fed directly
    /// to the transport reader (deterministic — no real runtime racing); the
    /// child process is never written to.
    #[tokio::test]
    async fn tool_terminal_survives_full_stream_lane() {
        let bus = EventBus::new();
        let supervisor = ProcessSupervisor::new(bus);
        let tmp = tempfile::tempdir().unwrap();
        // Trivial long-enough-lived child (exits immediately; the transport
        // only holds its handle for stdin writes, which this test never
        // performs).
        let mut spec = ProcessSpec::new("route-test", "cmd");
        spec.args = vec!["/C".into(), "exit 0".into()];
        spec.cwd = Some(tmp.path().to_path_buf());
        spec.stdin = StdinPolicy::Null;
        let process = supervisor.spawn(spec).await.expect("spawn trivial child");
        let (protocol_tx, protocol_rx) = mpsc::channel(4096);
        let (transport, _dead, mut stream_rx, mut tool_rx, _server_rx) =
            Transport::new(1, process, protocol_rx, 1024 * 1024);

        // Flood the stream lane WITHOUT consuming it: the 256-capacity route
        // channel must overflow (drops counted, never blocking).
        for i in 0..5000 {
            protocol_tx
                .send(chunk_frame("s1", &format!("b{i} ")))
                .await
                .expect("feed chunk");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            transport.dropped_events() > 0,
            "stream lane must overflow when unconsumed"
        );

        // Tool terminal frames now arrive on a FULL stream lane: they must be
        // routed to the non-droppable state lane, never dropped.
        protocol_tx
            .send(tool_frame("s1", "t1", "in_progress"))
            .await
            .expect("feed tool started");
        protocol_tx
            .send(tool_frame("s1", "t1", "completed"))
            .await
            .expect("feed tool completed");
        let started = timeout(Duration::from_secs(5), tool_rx.recv())
            .await
            .expect("tool started routed")
            .expect("tool lane open");
        let completed = timeout(Duration::from_secs(5), tool_rx.recv())
            .await
            .expect("tool completed routed")
            .expect("tool lane open");
        match started.update {
            crate::protocol::SessionUpdate::ToolCall { tool_call } => {
                assert_eq!(tool_call.status.as_deref(), Some("in_progress"));
            }
            other => panic!("expected ToolCall started, got {other:?}"),
        }
        match completed.update {
            crate::protocol::SessionUpdate::ToolCall { tool_call } => {
                assert_eq!(tool_call.status.as_deref(), Some("completed"));
            }
            other => panic!("expected ToolCall completed, got {other:?}"),
        }

        // Chunks that fit before overflow were still routed (stream lane
        // intact), and the drop counter reflects the overflow.
        assert!(transport.dropped_events() > 0);
        let drained = timeout(Duration::from_secs(5), stream_rx.recv())
            .await
            .expect("stream lane still routes")
            .expect("stream lane open");
        assert!(matches!(
            drained.update,
            crate::protocol::SessionUpdate::AgentMessageChunk { .. }
        ));

        // Clean teardown: EOF on the protocol feed ends the reader; close
        // joins/aborts it.
        drop(protocol_tx);
        transport.close("test teardown").await;
        assert_eq!(transport.pending_count(), 0);
        // No stray routed frames after close.
        let _ = stream_rx.try_recv();
        let _ = tool_rx.try_recv();
    }
}
