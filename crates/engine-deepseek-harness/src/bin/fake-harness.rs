//! Deterministic fake DeepSeek Harness ACP server (TASK 20 + TASK 21).
//!
//! A real stdio process (spawned through the ProcessSupervisor in tests) that
//! speaks the ACP wire shape — newline-delimited JSON-RPC 2.0 — and drives
//! every hostile scenario from argv (`DSH_FIXTURE_SCENARIO`). No real Harness,
//! no Node, no inference: fully deterministic.
//!
//! TASK 20 scenarios (hostile matrix, `tests/hostile.rs`): normal | delay-<ms>
//! | reject | hang | exit-early | exit-after-handshake | malformed | oversized
//! | partial-eof | fragmented | unknown-notification | duplicate-response |
//! unknown-response-id | server-request | ignore-requests | flood |
//! stderr-flood | ignore-shutdown.
//!
//! TASK 21 agent scenarios (`tests/vertical.rs`) — a real agent turn driven
//! over the wire: `session/new` → `session/prompt` → `session/update`
//! notifications (+ `session/request_permission` round-trips) → prompt
//! response with a stop reason, with `session/cancel` honored mid-turn:
//!
//!   agent-normal | agent-multi-step | agent-tool | agent-tool-fail |
//!   agent-permission-allow | agent-permission-deny |
//!   agent-permission-no-response | agent-cancel | agent-cancel-race |
//!   agent-provider-fail | agent-crash | agent-transport-loss |
//!   agent-duplicate-chunk | agent-wrong-session | agent-large-stream |
//!   agent-accepted-response-lost
//!
//! A reader thread owns stdin and pushes lines over an mpsc channel so the
//! main (writer) thread can observe `session/cancel` and permission responses
//! while a prompt turn is in flight.
//!
//! `--version` (argv) exits 0 silently so the adapter's cheap pre-launch probe
//! passes; the authoritative handshake is what the scenarios exercise.

use std::io::{BufRead, Write};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut scenario: Option<String> = None;
    let mut version = "0.1.0".to_string();
    let mut server_name = "dsh-acp-fixture".to_string();
    let mut protocol_version = "2025-03-26".to_string();
    for arg in args.iter().skip(1) {
        if arg == "--version" {
            std::process::exit(0);
        } else if let Some(v) = arg.strip_prefix("--version-str=") {
            version = v.to_string();
        } else if let Some(v) = arg.strip_prefix("--name=") {
            server_name = v.to_string();
        } else if let Some(v) = arg.strip_prefix("--proto=") {
            protocol_version = v.to_string();
        } else if scenario.is_none() {
            scenario = Some(arg.clone());
        }
    }
    let scenario = scenario
        .or_else(|| std::env::var("DSH_FIXTURE_SCENARIO").ok())
        .unwrap_or_else(|| "normal".into());

    if scenario == "exit-early" {
        std::process::exit(1);
    }

    let mut out = std::io::stdout().lock();

    // stderr flood: spam bounded lines up front (protocol stdout untouched).
    if scenario == "stderr-flood" {
        for i in 0..2000 {
            let _ = writeln!(std::io::stderr(), "fixture stderr line {i}");
        }
    }

    // Reader thread: stdin → channel (so the writer can observe cancel /
    // permission responses mid-turn without blocking on a line read).
    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line).unwrap_or(0);
            if n == 0 {
                break; // EOF
            }
            if tx.send(line.trim_end().to_string()).is_err() {
                break; // writer gone
            }
        }
    });

    let mut handshake_done = false;
    // Session id handed out by `session/new` (stable per scenario).
    let session_id = format!("sess-{scenario}");

    loop {
        let line = match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(l) => l,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Idle tick: some scenarios need to act on their own (e.g.
                // exit-after-delay). Nothing to do otherwise.
                if scenario == "exit-after-delay-400" && handshake_done {
                    std::process::exit(0);
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Client closed stdin (protocol shutdown).
                match scenario.as_str() {
                    "hang" | "ignore-shutdown" => loop {
                        thread::sleep(Duration::from_millis(1000));
                    },
                    _ => break, // clean exit
                }
            }
        };

        let parsed: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue, // garbage line from client: ignore
        };
        let method = parsed
            .get("method")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();
        let id = parsed.get("id").and_then(|i| i.as_u64());

        match scenario.as_str() {
            "hang" => {
                // Never respond to anything; stay alive until killed.
            }
            "ignore-requests" => {
                // Respond only to initialize; ignore everything else (alive).
                if method == "initialize" && !handshake_done {
                    if let Some(id) = id {
                        respond_initialize(&mut out, id, &version, &server_name, &protocol_version);
                    }
                    handshake_done = true;
                }
            }
            "reject" => {
                if method == "initialize" && !handshake_done {
                    if let Some(id) = id {
                        let _ = writeln!(
                            out,
                            "{}",
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32603, "message": "unsupported protocol version" }
                            })
                        );
                    }
                    handshake_done = true;
                }
            }
            _ => {
                if method == "initialize" && !handshake_done {
                    if let Some(id) = id {
                        if let Some(ms) = scenario.strip_prefix("delay-") {
                            let ms: u64 = ms.parse().unwrap_or(0);
                            thread::sleep(Duration::from_millis(ms));
                        }
                        if scenario == "fragmented" {
                            let response = json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "protocolVersion": protocol_version,
                                    "serverInfo": { "name": server_name, "version": version },
                                    "capabilities": {}
                                }
                            })
                            .to_string();
                            for b in response.bytes() {
                                let _ = out.write_all(&[b]);
                                let _ = out.flush();
                                thread::sleep(Duration::from_millis(2));
                            }
                            let _ = out.write_all(b"\n");
                            let _ = out.flush();
                        } else {
                            respond_initialize(
                                &mut out,
                                id,
                                &version,
                                &server_name,
                                &protocol_version,
                            );
                        }
                    }
                    handshake_done = true;

                    if scenario == "duplicate-response" {
                        if let Some(id) = id {
                            respond_initialize(
                                &mut out,
                                id,
                                &version,
                                &server_name,
                                &protocol_version,
                            );
                        }
                    }

                    match scenario.as_str() {
                        "unknown-notification" => {
                            let _ = writeln!(
                                out,
                                "{}",
                                json!({
                                    "jsonrpc": "2.0",
                                    "method": "session/update",
                                    "params": { "sessionId": "s1", "messages": [] }
                                })
                            );
                        }
                        "unknown-response-id" => {
                            let _ = writeln!(
                                out,
                                "{}",
                                json!({ "jsonrpc": "2.0", "id": 424242, "result": {} })
                            );
                        }
                        "server-request" => {
                            let _ = writeln!(
                                out,
                                "{}",
                                json!({
                                    "jsonrpc": "2.0",
                                    "id": 7,
                                    "method": "session/request_permission",
                                    "params": {
                                        "sessionId": "s1",
                                        "toolCall": { "toolCallId": "t1" },
                                        "options": []
                                    }
                                })
                            );
                        }
                        "flood" => {
                            for i in 0..2000 {
                                let _ = writeln!(
                                    out,
                                    "{}",
                                    json!({
                                        "jsonrpc": "2.0",
                                        "method": "session/update",
                                        "params": { "sessionId": "s1", "seq": i, "messages": [] }
                                    })
                                );
                            }
                        }
                        "exit-after-handshake" => std::process::exit(0),
                        "malformed" => {
                            let _ = writeln!(out, "this is {{ not json");
                            std::process::exit(0);
                        }
                        "oversized" => {
                            let _ = writeln!(out, "{}", "x".repeat(1_100_000));
                            std::process::exit(0);
                        }
                        "partial-eof" => {
                            let _ = out.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":5,\"resu");
                            let _ = out.flush();
                            std::process::exit(0);
                        }
                        _ => {}
                    }
                } else if method == "session/new" {
                    // Authoritative session creation (TASK 21 §8).
                    let _ = writeln!(
                        out,
                        "{}",
                        json!({
                            "jsonrpc": "2.0",
                            "id": id.unwrap_or(0),
                            "result": { "sessionId": session_id }
                        })
                    );
                } else if method == "session/prompt" {
                    // Drive the agent turn for the scenario, echoing the
                    // client's request id in the prompt response.
                    let settled =
                        drive_agent_turn(&scenario, &session_id, id.unwrap_or(0), &mut out, &rx);
                    if settled == AgentTurnEnd::Exit {
                        std::process::exit(0);
                    } else if settled == AgentTurnEnd::Eof {
                        // Close stdout without responding (transport-loss /
                        // accepted-response-lost): drop the writer.
                        drop(out);
                        std::process::exit(0);
                    }
                } else if method == "session/cancel" {
                    // Cancel is a notification; the agent scenarios settle the
                    // in-flight prompt themselves. Nothing else to do here.
                    let _ = id;
                } else if method == "session/delete" || method == "session/list" {
                    // Fresh-sessions-only dsh-acp: these are not supported.
                    // Answer method-not-found so the adapter's best-effort
                    // paths tolerate them (§9, §150).
                    let _ = writeln!(
                        out,
                        "{}",
                        json!({
                            "jsonrpc": "2.0",
                            "id": id.unwrap_or(0),
                            "error": { "code": -32601, "message": "Method not found" }
                        })
                    );
                } else if let Some(id) = id {
                    // Any other request: normal = answer {}; hostile modes
                    // above already handled their post-handshake behavior.
                    let _ = writeln!(
                        out,
                        "{}",
                        json!({ "jsonrpc": "2.0", "id": id, "result": {} })
                    );
                }
            }
        }
    }
}

/// How a driven agent turn ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentTurnEnd {
    /// The prompt response was written (or the fixture otherwise settled).
    Settled,
    /// The process should exit now.
    Exit,
    /// The stdout should close without a prompt response (transport loss).
    Eof,
}

/// Drive one `session/prompt` turn for the given agent scenario. Emits
/// `session/update` notifications, handles `session/request_permission`
/// round-trips and `session/cancel` mid-turn, then writes the prompt response.
fn emit(out: &mut impl Write, v: &Value) {
    let _ = writeln!(out, "{v}");
    let _ = out.flush();
}

/// A prompt response that echoes the client's request id.
fn stop_resp(prompt_id: u64, reason: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": prompt_id, "result": { "stopReason": reason } })
}

fn drive_agent_turn(
    scenario: &str,
    session_id: &str,
    prompt_id: u64,
    out: &mut impl Write,
    rx: &Receiver<String>,
) -> AgentTurnEnd {
    let chunk = |text: &str, message_id: Option<&str>| {
        let mut update = json!({
            "sessionUpdate": "agent_message_chunk",
            "content": { "type": "text", "text": text }
        });
        if let Some(mid) = message_id {
            update["messageId"] = json!(mid);
        }
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": { "sessionId": session_id, "update": update }
        })
    };
    let tool = |tool_call_id: &str, name: &str, status: &str, raw_input: Option<Value>| {
        let mut tc = json!({
            "toolCallId": tool_call_id,
            "name": name,
            "status": status
        });
        if let Some(input) = raw_input {
            tc["rawInput"] = input;
        }
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": { "sessionUpdate": "tool_call", "toolCall": tc }
            }
        })
    };
    // A helper that polls the incoming channel for a `session/cancel`
    // notification (returns true) within a bounded window.
    let cancel_pending = |rx: &Receiver<String>| -> bool {
        match rx.try_recv() {
            Ok(line) => {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if v.get("method").and_then(|m| m.as_str()) == Some("session/cancel") {
                        return true;
                    }
                }
                false
            }
            Err(mpsc::TryRecvError::Empty) => false,
            Err(mpsc::TryRecvError::Disconnected) => true,
        }
    };

    match scenario {
        "agent-normal"
        | "agent-duplicate-chunk"
        | "agent-multi-step"
        | "agent-tool"
        | "agent-tool-fail"
        | "agent-large-stream"
        | "agent-wrong-session" => {
            if scenario == "agent-wrong-session" {
                // A session/update for a DIFFERENT session must not touch the
                // active run (§122).
                emit(
                    out,
                    &json!({
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": "sess-other",
                            "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "intruder" } }
                        }
                    }),
                );
            }
            if scenario == "agent-duplicate-chunk" {
                // Same messageId chunk delivered twice (§119): the adapter
                // appends both (no per-chunk identity to dedup on — documented
                // limitation) and must not corrupt or crash.
                let dup = chunk("duplicated", Some("msg-dup"));
                emit(out, &dup);
                thread::sleep(Duration::from_millis(10));
                emit(out, &dup);
                thread::sleep(Duration::from_millis(10));
            }
            if scenario == "agent-large-stream" {
                // 10k committed chunks — bounded, responsive (§98).
                for i in 0..10_000 {
                    if cancel_pending(rx) {
                        emit(out, &stop_resp(prompt_id, "cancelled"));
                        return AgentTurnEnd::Settled;
                    }
                    emit(out, &chunk(&format!("chunk-{i:04} "), None));
                    if i % 100 == 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
                emit(out, &stop_resp(prompt_id, "end_turn"));
                return AgentTurnEnd::Settled;
            }
            if scenario == "agent-tool" || scenario == "agent-tool-fail" {
                emit(out, &chunk("Analyzing… ", None));
                emit(
                    out,
                    &tool(
                        "t1",
                        "bash",
                        "in_progress",
                        Some(json!({ "command": "ls" })),
                    ),
                );
                thread::sleep(Duration::from_millis(10));
                if scenario == "agent-tool-fail" {
                    emit(
                        out,
                        &tool("t1", "bash", "failed", Some(json!({ "command": "ls" }))),
                    );
                } else {
                    // Single authoritative terminal update carrying the output
                    // (§52: exactly one terminal per ToolCallId — the adapter
                    // ignores any later completion).
                    emit(
                        out,
                        &json!({
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {
                                "sessionId": session_id,
                                "update": {
                                    "sessionUpdate": "tool_call",
                                    "toolCall": {
                                        "toolCallId": "t1",
                                        "name": "bash",
                                        "status": "completed",
                                        "rawInput": { "command": "ls" },
                                        "content": [{ "type": "content", "content": { "type": "text", "text": "file.txt\n" } }]
                                    }
                                }
                            }
                        }),
                    );
                }
                emit(out, &chunk("Done.", None));
            }
            if scenario == "agent-multi-step" {
                // Two steps: text → tool cycle → text → tool cycle → text.
                emit(out, &chunk("Step one: ", None));
                emit(out, &tool("t1", "bash", "in_progress", None));
                emit(out, &tool("t1", "bash", "completed", None));
                thread::sleep(Duration::from_millis(10));
                emit(out, &chunk("Step two: ", None));
                emit(out, &tool("t2", "read", "in_progress", None));
                emit(out, &tool("t2", "read", "completed", None));
                thread::sleep(Duration::from_millis(10));
                emit(out, &chunk("Finished.", None));
            }
            if scenario == "agent-normal" {
                emit(out, &chunk("Hello", None));
                thread::sleep(Duration::from_millis(10));
                emit(out, &chunk(" world", Some("msg-1")));
                thread::sleep(Duration::from_millis(10));
                emit(out, &chunk("!", None));
            }
            emit(out, &stop_resp(prompt_id, "end_turn"));
            AgentTurnEnd::Settled
        }
        "agent-permission-allow" | "agent-permission-deny" | "agent-permission-no-response" => {
            emit(out, &chunk("Checking… ", None));
            emit(
                out,
                &tool(
                    "p1",
                    "bash",
                    "in_progress",
                    Some(json!({ "command": "rm tmp" })),
                ),
            );
            // Send a permission request and wait for the client's decision.
            let req_id = 9000u64;
            emit(
                out,
                &json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": session_id,
                        "toolCall": { "toolCallId": "p1", "name": "bash", "status": "in_progress", "rawInput": { "command": "rm tmp" } },
                        "options": [
                            { "optionId": "allow-once", "name": "Allow once", "kind": "allow_once" },
                            { "optionId": "reject-once", "name": "Reject once", "kind": "reject_once" }
                        ]
                    }
                }),
            );
            // Wait for the client's response (or a cancel) with a bounded
            // window. The adapter answers fail-closed on teardown.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut decision: Option<bool> = None;
            while std::time::Instant::now() < deadline {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(line) => {
                        if let Ok(v) = serde_json::from_str::<Value>(&line) {
                            if v.get("method").and_then(|m| m.as_str()) == Some("session/cancel") {
                                // Run cancelled while the permission is pending
                                // (§70): settle the turn cancelled.
                                emit(out, &stop_resp(prompt_id, "cancelled"));
                                return AgentTurnEnd::Settled;
                            }
                            if v.get("id").and_then(|i| i.as_u64()) == Some(req_id) {
                                let allowed = v
                                    .pointer("/result/decision")
                                    .and_then(|d| d.as_str())
                                    .map(|d| d == "allow")
                                    .unwrap_or(false);
                                decision = Some(allowed);
                                break;
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            match decision {
                Some(true) => {
                    // Allowed → tool completes → turn completes (§111).
                    emit(out, &tool("p1", "bash", "completed", None));
                    emit(out, &chunk("Removed.", None));
                    emit(out, &stop_resp(prompt_id, "end_turn"));
                }
                Some(false) => {
                    // Denied → tool fails → turn fails (§112).
                    emit(out, &tool("p1", "bash", "failed", None));
                    emit(out, &stop_resp(prompt_id, "error"));
                }
                None => {
                    // No response (fail-closed path): settle the turn with a
                    // model error so the run reaches a terminal, never hangs.
                    emit(out, &stop_resp(prompt_id, "error"));
                }
            }
            AgentTurnEnd::Settled
        }
        "agent-cancel" | "agent-cancel-race" => {
            emit(out, &chunk("Starting… ", None));
            // Emit chunks slowly, watching for session/cancel.
            for i in 0..20 {
                if cancel_pending(rx) {
                    if scenario == "agent-cancel-race" {
                        // The cancel raced and lost: the agent still finishes
                        // normally — the authoritative stop reason wins (§67).
                        emit(out, &stop_resp(prompt_id, "end_turn"));
                        return AgentTurnEnd::Settled;
                    }
                    emit(out, &stop_resp(prompt_id, "cancelled"));
                    return AgentTurnEnd::Settled;
                }
                emit(out, &chunk(&format!("part {i} "), None));
                thread::sleep(Duration::from_millis(20));
            }
            emit(out, &stop_resp(prompt_id, "end_turn"));
            AgentTurnEnd::Settled
        }
        "agent-provider-fail" => {
            emit(out, &chunk("Attempting… ", None));
            emit(out, &stop_resp(prompt_id, "error"));
            AgentTurnEnd::Settled
        }
        "agent-crash" => {
            // Emit a chunk, then the runtime process exits mid-turn (§71).
            emit(out, &chunk("About to crash… ", None));
            thread::sleep(Duration::from_millis(30));
            AgentTurnEnd::Exit
        }
        "agent-transport-loss" | "agent-accepted-response-lost" => {
            // Emit acceptance evidence (a committed chunk), then close stdout
            // without a prompt response (§126, §128).
            emit(out, &chunk("Accepted… ", None));
            thread::sleep(Duration::from_millis(30));
            AgentTurnEnd::Eof
        }
        "agent-tool-burst" => {
            // Flood the stream lane with chunks (no sleeps) so the route
            // channel overflows, then emit a tool terminal. TASK 24 §9: the
            // tool completed fact must survive the overflow via the
            // NON-DROPPABLE state lane — it can never be silently dropped.
            for i in 0..5000 {
                emit(out, &chunk(&format!("b{i} "), None));
            }
            emit(
                out,
                &tool("t1", "bash", "in_progress", Some(json!({ "command": "ls" }))),
            );
            emit(out, &tool("t1", "bash", "completed", None));
            // One final stream frame AFTER the flood: the stream dispatcher
            // only notices the coalesced drop counter when it receives the
            // next stream notification, so this guarantees the overflow
            // warning is observable before the terminal.
            emit(out, &chunk("end ", None));
            emit(out, &stop_resp(prompt_id, "end_turn"));
            AgentTurnEnd::Settled
        }
        "agent-prompt-reject" => {
            // The runtime answers the prompt with an explicit rejection AFTER
            // the frame was written but before any execution evidence (TASK 24
            // §9): acceptance must NOT be reported — a definite rejection.
            let _ = writeln!(
                out,
                "{}",
                json!({
                    "jsonrpc": "2.0",
                    "id": prompt_id,
                    "error": { "code": -32000, "message": "turn rejected by runtime policy" }
                })
            );
            let _ = out.flush();
            AgentTurnEnd::Settled
        }
        "agent-loss-before-evidence" => {
            // Close stdout after the prompt frame was written but before any
            // session/update or response: transport loss with an unprovable
            // outcome — OutcomeUnknown, never Accepted or Failed (§128).
            AgentTurnEnd::Eof
        }
        _ => {
            // Unknown agent scenario: settle with end_turn defensively.
            emit(out, &stop_resp(prompt_id, "end_turn"));
            AgentTurnEnd::Settled
        }
    }
}

fn respond_initialize(
    out: &mut impl Write,
    id: u64,
    version: &str,
    server_name: &str,
    protocol_version: &str,
) {
    let _ = writeln!(
        out,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": protocol_version,
                "serverInfo": { "name": server_name, "version": version },
                "capabilities": {}
            }
        })
    );
}
