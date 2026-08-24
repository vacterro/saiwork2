//! TASK 11 protocol fixture: a fake `opencode` server the adapter's session
//! layer can talk to (ENGINE_CONTRACT.md — "mock proves the adapter, real
//! proves compatibility").
//!
//! Mirrors the verified 1.18.18 server contract:
//! - `GET  /doc`                       — OpenAPI identity
//! - `GET  /provider`                  — providers/models
//! - `GET  /session` | `POST /session` — list/create
//! - `GET  /session/{id}`              — get one
//! - `GET  /session/{id}/message`      — list messages
//! - `POST /session/{id}/message`      — run simulation; returns the final message JSON when the run ends
//! - `POST /session/{id}/abort`        — abort the active run → `true`
//! - `GET  /event`                     — global SSE stream (deltas, parts, session.status, session.error)
//! - `POST /session/{id}/permission/{requestID}/reply` — permission decision
//!
//! Run behavior is selected by env (read once at startup):
//! - `FIXTURE_MSG_MODE`: `complete` | `hang` | `error500` | `malformed` |
//!   `truncated` | `provider_error`
//! - `FIXTURE_MSG_DELAY_MS`: base run duration before the terminal (default 400)
//! - `FIXTURE_TOOL=1`: interleave a `bash` tool part (running → completed)
//! - `FIXTURE_EVENT_STYLE`: `normal` | `fragmented` | `multi` | `keepalive` |
//!   `malformed` | `unknown` | `duplicate` | `bad_utf8`
//! - `FIXTURE_EVENT_DROP_AFTER=N`: close the SSE connection after N batches
//!   (0 = immediately after headers, before any event)
//! - `FIXTURE_PROVIDER_HTTP=STATUS[:COUNT]`: /provider returns STATUS the
//!   first COUNT times, then succeeds (default COUNT=1)
//! - `FIXTURE_PROVIDER_BODY=malformed`: /provider returns 200 with invalid
//!   JSON (typed protocol-error fixture)
//! - `FIXTURE_PROVIDER_FALLBACK=1`: /provider returns 404 and
//!   `/config/providers` serves the same catalog (strict-fallback fixture)
//! - `FIXTURE_PROVIDER_COUNT=N`: number of providers in the catalog
//! - `FIXTURE_PROVIDER_MODELS_PER=N`: models per provider (default 2);
//!   combined with COUNT it can exceed any body bound (large-catalog tests)
//! - `FIXTURE_MSG_ERROR_BODY`: `huge` | `html` | `echo_secret` — the body of
//!   a failing message POST (bounded-decode / redaction fixtures)
//! - `FIXTURE_ABORT_MODE=hang`: /abort accepts the connection and never
//!   answers (bounded-request fixture)
//! - `FIXTURE_DELTA_COUNT=N`: publish N small text deltas before the terminal
//!   (stress/backpressure fixture)
//! - `FIXTURE_AUTH=1` + `FIXTURE_PASSWORD`: require Basic auth on every route

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::exit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SessionData {
    id: String,
    title: String,
    directory: String,
    revert: Option<String>,
}

struct RunHandle {
    abort: Arc<AtomicBool>,
    notify: Sender<()>,
}

struct State {
    sessions: Mutex<Vec<SessionData>>,
    /// Connected /event clients (SSE broadcast).
    event_clients: Mutex<Vec<Sender<String>>>,
    /// Active run per session id.
    runs: Mutex<HashMap<String, RunHandle>>,
    next_evt: AtomicU64,
    next_msg: AtomicU64,
    next_part: AtomicU64,
    /// Id of the assistant message currently being produced (per session).
    assistant_msg: Mutex<HashMap<String, String>>,
    permission_requests: Mutex<Vec<String>>,
    /// provider_error mode is a *transient* failure: the first run on a
    /// session hits the provider error, later runs complete normally
    /// (proves run-failure recovery, §175).
    provider_error_emitted: Mutex<HashMap<String, ()>>,
    /// /provider failure counter (FIXTURE_PROVIDER_HTTP).
    provider_failures_left: Mutex<u32>,
    /// Number of abort requests received (cancel-spam fixture, §63).
    abort_count: AtomicU64,
    /// Events published to the /event fan-out (diagnostic counters).
    published_events: AtomicU64,
    /// Cumulative /event connections accepted (perf test: one per runtime
    /// generation — the stream is reused, never reconnected per run).
    event_connections: AtomicU64,
    /// SSE lines actually written to the connected client (diagnostic).
    written_lines: AtomicU64,
    /// The (providerID, modelID) pair the last POST /session/{id}/message
    /// actually received — the discriminating model-identity probe (TASK 24
    /// §9: map key vs inner id differ, so the wire pair proves which layer is
    /// authoritative).
    last_model: Mutex<Option<(String, String)>>,
    /// Preloaded authoritative message history per session id, served by
    /// `GET /session/{id}/message` (TASK 24 §9: restart/select restores the
    /// exact user/assistant/tool order from the engine, never SQLite).
    history: Mutex<HashMap<String, Vec<serde_json::Value>>>,
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            sessions: Mutex::new(Vec::new()),
            event_clients: Mutex::new(Vec::new()),
            runs: Mutex::new(HashMap::new()),
            next_evt: AtomicU64::new(0),
            next_msg: AtomicU64::new(0),
            next_part: AtomicU64::new(0),
            assistant_msg: Mutex::new(HashMap::new()),
            permission_requests: Mutex::new(Vec::new()),
            provider_error_emitted: Mutex::new(HashMap::new()),
            provider_failures_left: Mutex::new(0),
            abort_count: AtomicU64::new(0),
            published_events: AtomicU64::new(0),
            event_connections: AtomicU64::new(0),
            written_lines: AtomicU64::new(0),
            last_model: Mutex::new(None),
            history: Mutex::new(HashMap::new()),
        })
    }

    fn evt_id(&self) -> String {
        format!("evt_{:016x}", self.next_evt.fetch_add(1, Ordering::Relaxed))
    }
    fn msg_id(&self) -> String {
        format!("msg_{:016x}", self.next_msg.fetch_add(1, Ordering::Relaxed))
    }
    fn part_id(&self) -> String {
        format!(
            "prt_{:016x}",
            self.next_part.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Publish one SSE `data:` line to every connected /event client.
    /// `published_events` counts only events delivered into at least one
    /// client channel — an event published before any /event client exists
    /// was dropped by design and must not make the fan-out barrier wait for
    /// a delivery that can never happen.
    fn publish(&self, event: serde_json::Value) {
        let line = format!("data: {}\n\n", event);
        let mut clients = self.event_clients.lock().unwrap();
        let delivered = !clients.is_empty();
        clients.retain(|tx| tx.send(line.clone()).is_ok());
        if delivered {
            self.published_events.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--version") {
        println!("1.18.18");
        exit(0);
    }
    if args.get(1).map(String::as_str) == Some("serve") && args.iter().any(|a| a == "--help") {
        println!("opencode serve\n\nstarts a headless opencode server\n\nOptions:\n  --port      port to listen on\n  --hostname  hostname to listen on\n");
        exit(0);
    }

    let port = parse_port(&args).unwrap_or(0);
    let listener = match TcpListener::bind(("127.0.0.1", port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fixture: address in use: {e}");
            eprintln!("Error: listen EADDRINUSE: address already in use 127.0.0.1:{port}");
            exit(1);
        }
    };
    let actual = listener.local_addr().unwrap().port();
    println!("opencode server listening on http://127.0.0.1:{actual}");

    // Crash mode: die hard after N ms (engine-crash test, §135).
    if let Some(ms) = env("FIXTURE_CRASH_AFTER_MS").and_then(|v| v.parse::<u64>().ok()) {
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(ms));
            std::process::exit(1);
        });
    }

    let state = State::new();
    // Seed one session so list/resume is testable without creating first.
    state.sessions.lock().unwrap().push(SessionData {
        id: "ses_fixture_seeded".into(),
        title: "Seeded session".into(),
        directory: env("FIXTURE_WORKSPACE").unwrap_or_default(),
        revert: None,
    });
    // FIXTURE_PROVIDER_HTTP=STATUS[:COUNT] — fail /provider the first COUNT
    // times (default 1), then succeed.
    if let Some(spec) = env("FIXTURE_PROVIDER_HTTP") {
        let count = spec
            .split_once(':')
            .and_then(|(_, c)| c.parse::<u32>().ok())
            .unwrap_or(1);
        *state.provider_failures_left.lock().unwrap() = count;
    }

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let state = state.clone();
        std::thread::spawn(move || serve_connection(stream, state));
    }
}

// ---------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------

struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    #[allow(dead_code)]
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut total = 0usize;
    // Header terminator search.
    let header_end = loop {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        total += n;
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        if total > 64 * 1024 {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while buf.len() < header_end + 4 + content_length {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[header_end + 4..]
        .get(..content_length)
        .unwrap_or(&[])
        .to_vec();
    Some(Request {
        method,
        path,
        headers,
        body,
    })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn write_response(stream: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

fn write_json(stream: &mut TcpStream, status: &str, value: &serde_json::Value) {
    let body = serde_json::to_vec(value).unwrap_or_default();
    write_response(stream, status, "application/json", &body);
}

fn error_body(name: &str, message: &str) -> serde_json::Value {
    serde_json::json!({ "name": name, "data": { "message": message, "kind": "Payload" } })
}

fn auth_ok(request: &Request) -> bool {
    if env("FIXTURE_AUTH").as_deref() != Some("1") {
        return true;
    }
    let expected = env("FIXTURE_PASSWORD");
    let Some(value) = request.headers.get("authorization") else {
        return false;
    };
    let Some((scheme, b64)) = value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("basic") {
        return false;
    }
    let Ok(decoded) = base64_simple::decode(b64.trim()) else {
        return false;
    };
    let decoded = String::from_utf8_lossy(&decoded);
    let Some((_, password)) = decoded.split_once(':') else {
        return false;
    };
    match expected {
        None => true,
        Some(p) => password == p,
    }
}

// ---------------------------------------------------------------------------
// Request routing
// ---------------------------------------------------------------------------

fn serve_connection(mut stream: TcpStream, state: Arc<State>) {
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    if !auth_ok(&request) {
        let _ = stream.write_all(
            b"HTTP/1.1 401 Unauthorized\r\nwww-authenticate: Basic realm=\"Secure Area\"\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
        return;
    }
    let method = request.method.clone();
    let path = request.path.clone();
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();

    match (method.as_str(), segments.as_slice()) {
        ("GET", ["doc"]) => {
            let body = r#"{"openapi":"3.1.0","info":{"title":"opencode","version":"1.0.0"}}"#;
            write_response(&mut stream, "200 OK", "application/json", body.as_bytes());
        }
        ("GET", ["__fixture", "abort_count"]) => {
            write_json(
                &mut stream,
                "200 OK",
                &serde_json::json!({"count": state.abort_count.load(Ordering::Relaxed)}),
            );
        }
        ("GET", ["__fixture", "last_model"]) => {
            let m = state.last_model.lock().unwrap().clone();
            write_json(
                &mut stream,
                "200 OK",
                &serde_json::json!({
                    "providerID": m.as_ref().map(|x| x.0.clone()),
                    "modelID": m.as_ref().map(|x| x.1.clone()),
                }),
            );
        }
        ("GET", ["__fixture", "counters"]) => {
            write_json(
                &mut stream,
                "200 OK",
                &serde_json::json!({
                    "published": state.published_events.load(Ordering::Relaxed),
                    "written": state.written_lines.load(Ordering::Relaxed),
                    "clients": state.event_clients.lock().unwrap().len(),
                    "event_connections": state.event_connections.load(Ordering::Relaxed),
                }),
            );
        }
        ("GET", ["provider"]) => {
            if env("FIXTURE_PROVIDER_FALLBACK").as_deref() == Some("1") {
                // Strict-fallback fixture: /config/providers exists and
                // serves a valid catalog; /provider either fails with the
                // configured HTTP status (proving auth/server failures do
                // NOT fall back) or 404s when no status is configured
                // (route-absent fallback).
                if env("FIXTURE_PROVIDER_HTTP").is_some() {
                    let status = env("FIXTURE_PROVIDER_HTTP")
                        .and_then(|s| s.split(':').next().map(str::to_string))
                        .unwrap_or_else(|| "500".into());
                    match status.as_str() {
                        "401" => {
                            write_response(
                                &mut stream,
                                "401 Unauthorized",
                                "application/json",
                                b"{}",
                            );
                        }
                        "403" => {
                            write_response(&mut stream, "403 Forbidden", "application/json", b"{}");
                        }
                        _ => {
                            write_response(
                                &mut stream,
                                "500 Internal Server Error",
                                "application/json",
                                b"{\"name\":\"InternalError\",\"data\":{\"message\":\"provider list exploded\"}}",
                            );
                        }
                    }
                    return;
                }
                write_json(
                    &mut stream,
                    "404 Not Found",
                    &error_body("NotFound", "no such route"),
                );
                return;
            }
            let failing = {
                let mut left = state.provider_failures_left.lock().unwrap();
                if *left > 0 {
                    *left -= 1;
                    true
                } else {
                    false
                }
            };
            if failing {
                let status = env("FIXTURE_PROVIDER_HTTP")
                    .and_then(|s| s.split(':').next().map(str::to_string))
                    .unwrap_or_else(|| "500".into());
                match status.as_str() {
                    "401" => {
                        write_response(&mut stream, "401 Unauthorized", "application/json", b"{}");
                    }
                    "403" => {
                        write_response(&mut stream, "403 Forbidden", "application/json", b"{}");
                    }
                    _ => {
                        write_response(
                            &mut stream,
                            "500 Internal Server Error",
                            "application/json",
                            b"{\"name\":\"InternalError\",\"data\":{\"message\":\"provider list exploded\"}}",
                        );
                    }
                }
            } else if env("FIXTURE_PROVIDER_BODY").as_deref() == Some("malformed") {
                // Malformed-catalog fixture: 200 with invalid JSON.
                write_response(
                    &mut stream,
                    "200 OK",
                    "application/json",
                    b"{not valid json",
                );
            } else {
                write_json(&mut stream, "200 OK", &providers_json());
            }
        }
        ("GET", ["config", "providers"]) => {
            // The strict-fallback endpoint, same normalized catalog in the
            // `{providers, default}` wire shape (1.18.18 contract).
            let mut providers = serde_json::Map::new();
            if let serde_json::Value::Object(map) = providers_json() {
                if let Some(all) = map.get("all").and_then(|v| v.as_array()) {
                    providers.insert("providers".into(), serde_json::Value::Array(all.clone()));
                }
                if let Some(default) = map.get("default") {
                    providers.insert("default".into(), default.clone());
                }
            }
            write_json(&mut stream, "200 OK", &serde_json::Value::Object(providers));
        }
        ("GET", ["session"]) => {
            let sessions = state.sessions.lock().unwrap().clone();
            let arr: Vec<serde_json::Value> = sessions.iter().map(session_json).collect();
            write_json(&mut stream, "200 OK", &serde_json::Value::Array(arr));
        }
        ("POST", ["session"]) => {
            let mut sessions = state.sessions.lock().unwrap();
            let id = format!("ses_fixture_{:016x}", sessions.len() + 1);
            let data = SessionData {
                id: id.clone(),
                title: "Test session".into(),
                directory: env("FIXTURE_WORKSPACE").unwrap_or_default(),
                revert: None,
            };
            sessions.push(data.clone());
            drop(sessions);
            // Preload authoritative history (TASK 24 §9): a resumed session
            // must be able to restore its exact user/assistant/tool order
            // from the engine's own endpoint.
            state.history.lock().unwrap().insert(
                id.clone(),
                vec![
                    serde_json::json!({
                        "id": "msg_pre_1", "role": "user",
                        "parts": [{ "id": "p1", "type": "text", "text": "preloaded user prompt" }],
                        "time": { "created": 1786863908016i64 }
                    }),
                    serde_json::json!({
                        "id": "msg_pre_2", "role": "assistant",
                        "parts": [
                            { "id": "p2", "type": "text", "text": "preloaded assistant answer" },
                            { "id": "call_1", "type": "tool", "tool": "bash",
                              "state": { "status": "completed", "output": "preloaded tool output" } }
                        ],
                        "time": { "created": 1786863909016i64 }
                    }),
                ],
            );
            state.publish(serde_json::json!({
                "id": state.evt_id(), "type": "session.created",
                "properties": { "sessionID": id }
            }));
            write_json(&mut stream, "200 OK", &session_json(&data));
        }
        ("GET", ["session", id]) => {
            let sessions = state.sessions.lock().unwrap();
            match sessions.iter().find(|s| &s.id == id) {
                Some(s) => write_json(&mut stream, "200 OK", &session_json(s)),
                None => write_json(
                    &mut stream,
                    "404 Not Found",
                    &error_body("NotFound", "session not found"),
                ),
            }
        }
        ("POST", ["session", id, "revert"]) => {
            let message_id = serde_json::from_slice::<serde_json::Value>(&request.body)
                .ok()
                .and_then(|body| body.get("messageID").and_then(|value| value.as_str()).map(str::to_string));
            let mut sessions = state.sessions.lock().unwrap();
            match (sessions.iter_mut().find(|session| &session.id == id), message_id) {
                (Some(session), Some(message_id)) => {
                    session.revert = Some(message_id);
                    write_response(&mut stream, "200 OK", "application/json", b"{}");
                }
                (None, _) => write_json(&mut stream, "404 Not Found", &error_body("NotFound", "session not found")),
                (_, None) => write_json(&mut stream, "400 Bad Request", &error_body("BadRequest", "messageID required")),
            }
        }
        ("POST", ["session", id, "unrevert"]) => {
            let mut sessions = state.sessions.lock().unwrap();
            match sessions.iter_mut().find(|session| &session.id == id) {
                Some(session) => {
                    session.revert = None;
                    write_response(&mut stream, "200 OK", "application/json", b"{}");
                }
                None => write_json(&mut stream, "404 Not Found", &error_body("NotFound", "session not found")),
            }
        }
        ("DELETE", ["session", id]) => {
            let mut sessions = state.sessions.lock().unwrap();
            let before = sessions.len();
            sessions.retain(|s| &s.id != id);
            if sessions.len() == before {
                write_json(
                    &mut stream,
                    "404 Not Found",
                    &error_body("NotFound", "session not found"),
                );
            } else {
                write_response(&mut stream, "200 OK", "application/json", b"{}");
            }
        }
        ("GET", ["session", id, "message"]) => {
            // Only sessions that exist can have messages.
            let exists = state.sessions.lock().unwrap().iter().any(|s| &s.id == id);
            if !exists {
                write_json(
                    &mut stream,
                    "404 Not Found",
                    &error_body("NotFound", "session not found"),
                );
                return;
            }
            let history = state
                .history
                .lock()
                .unwrap()
                .get(&id.to_string())
                .cloned()
                .unwrap_or_default();
            write_json(&mut stream, "200 OK", &serde_json::Value::Array(history));
        }
        ("POST", ["session", id, "message"]) => {
            handle_message_post(&mut stream, state, id, &request.body);
        }
        ("POST", ["session", id, "abort"]) => {
            handle_abort(&mut stream, state, id);
        }
        ("POST", ["session", id, "permission", request_id, "reply"]) => {
            state
                .permission_requests
                .lock()
                .unwrap()
                .push(request_id.to_string());
            // Reply is accepted; the (fixture) permission is then resolved.
            state.publish(serde_json::json!({
                "id": state.evt_id(), "type": "permission.resolved",
                "properties": { "sessionID": id, "requestID": request_id }
            }));
            write_response(&mut stream, "200 OK", "application/json", b"{}");
        }
        ("GET", ["event"]) => {
            handle_event_stream(&mut stream, state);
        }
        _ => {
            write_json(
                &mut stream,
                "404 Not Found",
                &error_body("NotFound", "no such route"),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Endpoint bodies
// ---------------------------------------------------------------------------

fn providers_json() -> serde_json::Value {
    let count: usize = env("FIXTURE_PROVIDER_COUNT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let models_per: usize = env("FIXTURE_PROVIDER_MODELS_PER")
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let mut all = Vec::new();
    for i in 1..=count {
        let provider_id = format!("fixture-p{i}");
        let mut models = serde_json::Map::new();
        for j in 1..=models_per {
            // REAL-server contract shape (verified 1.18.18): the map KEY is
            // the RAW model id the server itself reports (e.g. real openai
            // keys are `gpt-5.6-sol`, real hpc-ai keys are
            // `deepseek/deepseek-v4-flash`) — WITHOUT a provider prefix.
            // The generic identity `provider-id/raw-key` is synthesized by
            // the ADAPTER, never by the fixture. The inner `id`/`providerID`
            // are deliberately DIFFERENT legacy values; if any layer
            // substituted them, the discriminating wire assertion fails.
            let model_id = format!("model-{j}");
            models.insert(
                model_id.clone(),
                serde_json::json!({
                    "id": format!("inner-legacy-{j}"),
                    "providerID": "legacy-provider",
                    "name": format!("Fixture Model {j}"),
                    "family": "fixture",
                    "capabilities": {
                        "temperature": true,
                        "reasoning": j % 2 == 0,
                        "attachment": false,
                        "toolcall": true,
                        "input": { "text": true, "image": false },
                        "output": { "text": true, "image": false },
                        "interleaved": false
                    }
                }),
            );
        }
        all.push(serde_json::json!({
            "id": provider_id,
            "name": format!("Fixture Provider {i}"),
            "models": models,
        }));
    }
    // `connected` mirrors the real server: the subset of providers with
    // usable credentials. FIXTURE_PROVIDER_CONNECTED overrides it (comma
    // separated); the default keeps ALL fixture providers connected so the
    // bulk of existing tests exercise the unfiltered catalog.
    let connected: Vec<String> = env("FIXTURE_PROVIDER_CONNECTED")
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_else(|| (1..=count).map(|i| format!("fixture-p{i}")).collect());
    serde_json::json!({
        "all": all,
        "default": { "fixture-p1": "model-1" },
        "connected": connected,
    })
}

fn session_json(s: &SessionData) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": s.id,
        "slug": "fixture-session",
        "projectID": "global",
        "directory": s.directory,
        "title": s.title,
        "version": "1.18.18",
        "time": { "created": 1786863908016i64, "updated": 1786863908016i64 },
    });
    if let Some(message_id) = &s.revert {
        value["revert"] = serde_json::json!({ "messageID": message_id });
    }
    value
}

// ---------------------------------------------------------------------------
// Message run simulation
// ---------------------------------------------------------------------------

fn handle_message_post(stream: &mut TcpStream, state: Arc<State>, session_id: &str, body: &[u8]) {
    let sid = session_id.to_string();
    let exists = state.sessions.lock().unwrap().iter().any(|s| s.id == sid);
    if !exists {
        write_json(
            stream,
            "404 Not Found",
            &error_body("NotFound", "session not found"),
        );
        return;
    }
    // Record the EXACT model identity the client sent (the discriminating
    // probe: the adapter must send map-key/provider-id, never the inner
    // legacy fields).
    let body_json: serde_json::Value =
        serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
    let model = body_json.get("model");
    let pair = model.and_then(|m| {
        Some((
            m.get("providerID")?.as_str()?.to_string(),
            m.get("modelID")?.as_str()?.to_string(),
        ))
    });
    *state.last_model.lock().unwrap() = pair;
    // Same-session concurrency: a second run on a busy session is rejected by
    // the adapter before it ever reaches us, but the server must also be
    // defensible (409) like the real server would be.
    {
        let runs = state.runs.lock().unwrap();
        if runs.contains_key(&sid) && !runs[&sid].abort.load(Ordering::SeqCst) {
            write_json(
                stream,
                "409 Conflict",
                &error_body("Conflict", "session is busy"),
            );
            return;
        }
    }

    let user_msg = state.msg_id();
    let assistant_msg = state.msg_id();
    state
        .assistant_msg
        .lock()
        .unwrap()
        .insert(sid.clone(), assistant_msg.clone());
    let delay_ms: u64 = env("FIXTURE_MSG_DELAY_MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);

    let mode = env("FIXTURE_MSG_MODE").unwrap_or_else(|| "complete".into());

    // error500 / malformed / truncated fail before any run exists.
    match mode.as_str() {
        "error500" => {
            // §52–§53: the error body shape is selectable so the bounded /
            // redacted decode is fixture-proven.
            match env("FIXTURE_MSG_ERROR_BODY").as_deref() {
                Some("huge") => {
                    let junk = "x".repeat(8 * 1024 * 1024);
                    write_response(
                        stream,
                        "500 Internal Server Error",
                        "text/plain",
                        junk.as_bytes(),
                    );
                }
                Some("html") => {
                    let html = "<html><body>Internal Server Error</body></html>";
                    write_response(
                        stream,
                        "500 Internal Server Error",
                        "text/html",
                        html.as_bytes(),
                    );
                }
                Some("echo_secret") => {
                    let secret = env("OPENCODE_SERVER_PASSWORD").unwrap_or_default();
                    let body = format!("provider failed; Authorization: Basic opencode:{secret}");
                    write_response(
                        stream,
                        "500 Internal Server Error",
                        "text/plain",
                        body.as_bytes(),
                    );
                }
                _ => {
                    write_json(
                        stream,
                        "500 Internal Server Error",
                        &error_body("InternalError", "provider exploded"),
                    );
                }
            }
            return;
        }
        "malformed" => {
            write_response(stream, "200 OK", "application/json", b"{not valid json");
            return;
        }
        "truncated" => {
            let body = serde_json::to_vec(&partial_message(&sid, &assistant_msg, true)).unwrap();
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len() + 500 // promise more than we send
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&body[..body.len().saturating_sub(30)]);
            return; // close mid-body
        }
        _ => {}
    }

    // The real OpenCode server streams SSE inline with the run, so its
    // POST response never overtakes the last delta. This fixture fans out
    // through an unbounded channel + a separate writer thread, which can lag
    // the POST by an arbitrary amount under a burst. To mirror the real
    // ordering, the POST handler barriers on the fan-out: it does not write
    // the response BODY until every event published during the run has been
    // written to the connected clients (bounded wait).
    let publish_before = state.published_events.load(Ordering::Relaxed);

    let (notify_tx, notify_rx) = mpsc::channel::<()>();
    state.runs.lock().unwrap().insert(
        sid.clone(),
        RunHandle {
            abort: Arc::new(AtomicBool::new(false)),
            notify: notify_tx,
        },
    );

    // Real-server behavior: the POST response head (2xx) is sent as the run
    // starts streaming, before the body completes. Write it now so the
    // client's two-phase acceptance resolves at wire-acceptance (the run is
    // live) rather than at the terminal — this is what keeps the
    // cancel/abort window real. Body arrives after the run simulation.
    let _ = stream.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n",
    );
    let _ = stream.flush();

    state.publish(serde_json::json!({
        "id": state.evt_id(), "type": "message.updated",
        "properties": { "sessionID": sid,
            "info": { "id": user_msg, "role": "user", "sessionID": sid, "time": { "created": now_ms() } } }
    }));
    state.publish(serde_json::json!({
        "id": state.evt_id(), "type": "session.status",
        "properties": { "sessionID": sid, "status": { "type": "busy" } }
    }));
    state.publish(serde_json::json!({
        "id": state.evt_id(), "type": "message.part.updated",
        "properties": { "sessionID": sid,
            "part": { "type": "step-start", "id": state.part_id(), "messageID": assistant_msg, "sessionID": sid } }
    }));

    let (aborted, provider_failed) =
        run_simulation(&state, &sid, &assistant_msg, delay_ms, &notify_rx);

    state.runs.lock().unwrap().remove(&sid);
    state.assistant_msg.lock().unwrap().remove(&sid);
    state.publish(serde_json::json!({
        "id": state.evt_id(), "type": "session.status",
        "properties": { "sessionID": sid, "status": { "type": "idle" } }
    }));

    let msg = final_message(&sid, &assistant_msg, aborted, provider_failed);

    // Fan-out barrier: every event published for this run must be written to
    // the clients before the POST response (real-server inline ordering).
    // Skipped for drop modes, where the writer is *supposed* to break early.
    let deliver_all = env("FIXTURE_EVENT_DROP_AFTER").is_none();
    if deliver_all {
        let target = state.published_events.load(Ordering::Relaxed);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if state.written_lines.load(Ordering::Relaxed) >= target {
                break;
            }
            if std::time::Instant::now() > deadline {
                eprintln!(
                    "fixture: fan-out barrier timed out (written {} of {})",
                    state.written_lines.load(Ordering::Relaxed),
                    target
                );
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let _ = publish_before;
    let _ = stream.write_all(&serde_json::to_vec(&msg).unwrap_or_default());
    let _ = stream.flush();
    // Final beat so late channel flushes land before the connection closes.
    std::thread::sleep(Duration::from_millis(60));
}

/// Publish the streaming event sequence for a run. Returns `(aborted,
/// provider_failed)` — aborted when the /abort handler fired before the
/// terminal; provider_failed when the run ended with a session.error (the
/// POST response then carries no `finish`).
fn run_simulation(
    state: &Arc<State>,
    sid: &str,
    assistant_msg: &str,
    delay_ms: u64,
    notify_rx: &Receiver<()>,
) -> (bool, bool) {
    let tool = env("FIXTURE_TOOL").as_deref() == Some("1");
    let text_parts = ["Hello ", "from ", "the ", "fixture", " engine"];
    let mut aborted = false;

    // §78/§123 stress knob: publish N small deltas before the regular flow.
    // Non-blocking abort check (try_recv) so the burst is a genuine
    // throughput test — the bottleneck is the real SSE/socket/bus pipeline,
    // not a per-delta sleep.
    if let Some(n) = env("FIXTURE_DELTA_COUNT").and_then(|v| v.parse::<u64>().ok()) {
        let piece = "0123456789";
        for _ in 0..n {
            if notify_rx.try_recv().is_ok() {
                aborted = true;
                break;
            }
            state.publish(serde_json::json!({
                "id": state.evt_id(), "type": "message.part.delta",
                "properties": { "sessionID": sid, "messageID": assistant_msg,
                    "partID": format!("prt_text_{sid}"), "field": "text", "delta": piece }
            }));
        }
    }

    // Streaming deltas, checking for abort between chunks.
    let chunk = delay_ms.max(20) / text_parts.len().max(1) as u64;
    for (i, piece) in text_parts.iter().enumerate() {
        if abort_signaled(notify_rx, Duration::from_millis(chunk)) {
            aborted = true;
            break;
        }
        let _ = i;
        state.publish(serde_json::json!({
            "id": state.evt_id(), "type": "message.part.delta",
            "properties": { "sessionID": sid, "messageID": assistant_msg,
                "partID": format!("prt_text_{sid}"), "field": "text", "delta": piece }
        }));
    }

    if tool && !aborted {
        // Tool: running → (small delay) → completed with output.
        if abort_signaled(notify_rx, Duration::from_millis(50)) {
            aborted = true;
        } else {
            state.publish(serde_json::json!({
                "id": state.evt_id(), "type": "message.part.updated",
                "properties": { "sessionID": sid,
                    "part": {
                        "type": "tool", "tool": "bash", "callID": "call_fixture_1",
                        "id": format!("prt_tool_{sid}"), "messageID": assistant_msg, "sessionID": sid,
                        "state": { "status": "running", "input": { "command": "ls" },
                                   "metadata": {}, "time": { "start": now_ms() } } }
                }
            }));
            if abort_signaled(notify_rx, Duration::from_millis(60)) {
                aborted = true;
            } else {
                state.publish(serde_json::json!({
                    "id": state.evt_id(), "type": "message.part.updated",
                    "properties": { "sessionID": sid,
                        "part": {
                            "type": "tool", "tool": "bash", "callID": "call_fixture_1",
                            "id": format!("prt_tool_{sid}"), "messageID": assistant_msg, "sessionID": sid,
                            "state": { "status": "completed",
                                       "output": "README.md\nsample.txt\n",
                                       "metadata": { "output": "README.md\nsample.txt\n", "exit": 0 },
                                       "time": { "start": now_ms(), "end": now_ms() } } }
                    }
                }));
            }
        }
    }

    if env("FIXTURE_MSG_MODE").as_deref() == Some("provider_error")
        && !aborted
        && state
            .provider_error_emitted
            .lock()
            .unwrap()
            .insert(sid.to_string(), ())
            .is_none()
    {
        // First run on this session: transient provider failure (session.error,
        // message returned without a finish). Later runs on the same session
        // complete normally — this is what proves the session is released
        // after a failed run (§175 recovery).
        state.publish(serde_json::json!({
            "id": state.evt_id(), "type": "session.error",
            "properties": { "sessionID": sid,
                "error": { "name": "APIError", "data": { "message": "provider rate limit (429)", "kind": "Provider" } } }
        }));
        return (false, true); // message returned without a finish
    }

    if !aborted {
        state.publish(serde_json::json!({
            "id": state.evt_id(), "type": "message.updated",
            "properties": { "sessionID": sid,
                "info": { "id": assistant_msg, "role": "assistant", "sessionID": sid,
                          "modelID": "model-1", "providerID": "fixture-p1",
                          "finish": "stop", "tokens": { "input": 10, "output": 5, "total": 15 },
                          "time": { "created": now_ms(), "completed": now_ms() } } }
        }));
    }
    (aborted, false)
}

fn abort_signaled(rx: &Receiver<()>, wait: Duration) -> bool {
    rx.recv_timeout(wait).is_ok()
}

fn partial_message(sid: &str, assistant_msg: &str, include_finish: bool) -> serde_json::Value {
    let mut info = serde_json::json!({
        "id": assistant_msg, "role": "assistant", "sessionID": sid,
        "modelID": "model-1", "providerID": "fixture-p1",
        "tokens": { "input": 10, "output": 5, "total": 15 },
        "time": { "created": now_ms(), "completed": now_ms() }
    });
    if include_finish {
        info["finish"] = serde_json::json!("stop");
    }
    serde_json::json!({ "info": info, "parts": [] })
}

fn final_message(
    sid: &str,
    assistant_msg: &str,
    aborted: bool,
    provider_failed: bool,
) -> serde_json::Value {
    // A provider-failed run ends without a `finish` (the POST task reads the
    // session.error event to fail the run, §57–§59). Aborted runs also carry
    // no finish (the POST task maps cancel + no finish → CANCELLED).
    let provider_error = provider_failed;
    let mut info = serde_json::json!({
        "id": assistant_msg, "role": "assistant", "sessionID": sid,
        "modelID": "model-1", "providerID": "fixture-p1",
        "tokens": { "input": 10, "output": 5, "total": 15 },
        "time": { "created": now_ms(), "completed": now_ms() }
    });
    let mut parts = vec![
        serde_json::json!({
            "type": "step-start", "id": format!("prt_start_{sid}"), "messageID": assistant_msg, "sessionID": sid
        }),
        serde_json::json!({
            "type": "text", "text": "Hello from the fixture engine",
            "id": format!("prt_text_{sid}"), "messageID": assistant_msg, "sessionID": sid
        }),
    ];
    if !aborted && !provider_error {
        info["finish"] = serde_json::json!("stop");
        parts.push(serde_json::json!({
            "type": "step-finish", "reason": "stop",
            "id": format!("prt_finish_{sid}"), "messageID": assistant_msg, "sessionID": sid,
            "tokens": { "input": 10, "output": 5, "total": 15 }
        }));
    }
    serde_json::json!({ "info": info, "parts": parts })
}

// ---------------------------------------------------------------------------
// Abort
// ---------------------------------------------------------------------------

fn handle_abort(stream: &mut TcpStream, state: Arc<State>, session_id: &str) {
    state.abort_count.fetch_add(1, Ordering::Relaxed);
    if env("FIXTURE_ABORT_MODE").as_deref() == Some("hang") {
        // Accept the connection and never answer: the adapter's bounded
        // request timeout must fire, and the run outcome must still come
        // from the authoritative POST response (§64).
        let _ = stream.flush();
        std::thread::sleep(Duration::from_secs(3600));
        return;
    }
    let sid = session_id.to_string();
    let mut runs = state.runs.lock().unwrap();
    match runs.get_mut(&sid) {
        Some(run) => {
            run.abort.store(true, Ordering::SeqCst);
            let _ = run.notify.send(());
            // The run thread finishes and removes itself; report `true`.
            drop(runs);
            write_response(stream, "200 OK", "application/json", b"true");
        }
        None => {
            drop(runs);
            // No active run: OpenCode returns false for a no-op abort.
            write_response(stream, "200 OK", "application/json", b"false");
        }
    }
}

// ---------------------------------------------------------------------------
// Event stream (SSE)
// ---------------------------------------------------------------------------

fn handle_event_stream(stream: &mut TcpStream, state: Arc<State>) {
    state.event_connections.fetch_add(1, Ordering::Relaxed);
    // Register the client BEFORE writing the response headers: the adapter
    // treats the headers as "connected" and may dispatch a message POST
    // immediately — the POST's events must find this client already
    // registered, or early deltas would be lost.
    let (tx, rx) = mpsc::channel::<String>();
    state.event_clients.lock().unwrap().push(tx);
    let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: keep-alive\r\n\r\n";
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    let _ = stream.write_all(
        format!(
            "data: {}\n\n",
            serde_json::json!({
                "id": state.evt_id(), "type": "server.connected", "properties": {}
            })
        )
        .as_bytes(),
    );
    let _ = stream.flush();

    let style = env("FIXTURE_EVENT_STYLE").unwrap_or_else(|| "normal".into());
    // DROP_AFTER: 0 = close before any event; N = close after N events;
    // unset = unlimited.
    let drop_after: u64 = env("FIXTURE_EVENT_DROP_AFTER")
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX);
    let mut written = 0u64;
    let mut pending: Vec<String> = Vec::new();
    while let Ok(line) = rx.recv() {
        // The writer consumes line N only after having written line N-1
        // (recv → write are serialized in this loop), so the consumption
        // counter is a truthful proxy for socket writes completed — the
        // POST-handler fan-out barrier waits on it.
        state.written_lines.fetch_add(1, Ordering::Relaxed);
        if written >= drop_after {
            break;
        }
        match style.as_str() {
            "fragmented" => {
                // Write one event in several tiny pieces to exercise the
                // parser's chunk-boundary handling.
                for piece in split_pieces(&line, 7) {
                    if stream.write_all(piece.as_bytes()).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                    std::thread::sleep(Duration::from_millis(2));
                }
                written += 1;
            }
            "multi" => {
                // Collect up to 3 events and write them in one write call.
                pending.push(line);
                if pending.len() >= 3 {
                    let joined = pending.concat();
                    if stream.write_all(joined.as_bytes()).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                    pending.clear();
                    written += 1;
                }
            }
            "keepalive" => {
                if stream
                    .write_all(format!(": ping {}\n\n{}", now_ms(), line).as_bytes())
                    .is_err()
                {
                    return;
                }
                let _ = stream.flush();
                written += 1;
            }
            "malformed" => {
                if written == 0 {
                    // First event is a syntactically-invalid SSE payload.
                    let bad = format!(
                        "data: {{not json\n\ndata: {}\n\n",
                        line.trim_start_matches("data: ")
                    );
                    if stream.write_all(bad.as_bytes()).is_err() {
                        return;
                    }
                } else if stream.write_all(line.as_bytes()).is_err() {
                    return;
                }
                let _ = stream.flush();
                written += 1;
            }
            "unknown" => {
                // A future event type the adapter must tolerate.
                let unknown = format!(
                    "data: {}\n\n",
                    serde_json::json!({
                        "id": state.evt_id(), "type": "some.future.event",
                        "properties": { "whatever": true }
                    })
                );
                let payload = format!("{unknown}{line}");
                if stream.write_all(payload.as_bytes()).is_err() {
                    return;
                }
                let _ = stream.flush();
                written += 1;
            }
            "duplicate" => {
                // Same delta twice — adapter must not corrupt run state.
                let dup = format!("{line}{line}");
                if stream.write_all(dup.as_bytes()).is_err() {
                    return;
                }
                let _ = stream.flush();
                written += 1;
            }
            "bad_utf8" => {
                // §13: raw invalid UTF-8 bytes in the stream (a comment line)
                // must not panic the parser or corrupt the following event.
                let raw = [0xFFu8, 0xFE, 0x80];
                if stream.write_all(&raw).is_err() {
                    return;
                }
                let _ = stream.write_all(b"\n\n");
                if stream.write_all(line.as_bytes()).is_err() {
                    return;
                }
                let _ = stream.flush();
                written += 1;
            }
            _ => {
                // Batch pending lines into one syscall (real servers write in
                // bigger chunks); the adapter must still parse every event.
                // The batch consumes extra channel lines — count them so the
                // fan-out barrier can see the true consumption.
                let mut batch = String::with_capacity(16 * 1024);
                batch.push_str(&line);
                let mut batched = 0u64;
                for _ in 0..63 {
                    match rx.try_recv() {
                        Ok(next) => {
                            batch.push_str(&next);
                            batched += 1;
                        }
                        Err(_) => break,
                    }
                }
                state.written_lines.fetch_add(batched, Ordering::Relaxed);
                if stream.write_all(batch.as_bytes()).is_err() {
                    return;
                }
                let _ = stream.flush();
                let n = batch.matches("\n\n").count();
                written += n as u64;
            }
        }
    }
}

fn split_pieces(line: &str, piece_size: usize) -> Vec<String> {
    line.as_bytes()
        .chunks(piece_size)
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_port(args: &[String]) -> Option<u16> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--port" {
            return args.get(i + 1).and_then(|p| p.parse().ok());
        }
        i += 1;
    }
    None
}

/// Tiny base64 decoder (RFC 4648) — test-only, no dependency.
mod base64_simple {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn decode(input: &str) -> Result<Vec<u8>, ()> {
        let input = input.trim_matches('=');
        let mut out = Vec::with_capacity(input.len() * 3 / 4);
        let mut acc: u32 = 0;
        let mut bits = 0u32;
        for byte in input.bytes() {
            let value = TABLE.iter().position(|&t| t == byte).ok_or(())? as u32;
            acc = (acc << 6) | value;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
                acc &= (1 << bits) - 1;
            }
        }
        Ok(out)
    }
}
