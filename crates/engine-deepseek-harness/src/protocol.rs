//! ACP wire types (TASK 20 §28/§131–§134, TASK 21 session/prompt/tool/
//! permission DTOs) — **adapter-local only**.
//!
//! The Agent Client Protocol (agentclientprotocol.com) used by
//! `@deepseek-ai/dsh-acp` is JSON-RPC 2.0 over stdio with newline-delimited
//! framing: one compact JSON object per `\n`-terminated line. This module
//! names the wire surface the adapter depends on: the `initialize` handshake
//! (TASK 20) and the session lifecycle (`session/new`, `session/prompt`,
//! `session/update` notifications, `session/request_permission`, `session/
//! cancel`) added by TASK 21.
//!
//! Wire provenance: Agent Client Protocol v1 (agentclientprotocol.com,
//! `@agentclientprotocol/sdk` 0.25.1, the SDK `@deepseek-ai/dsh-acp` 0.0.1-rc.1
//! is built on). Exact shapes verified against the published schema
//! (2026-08-17): `session/new` `{ cwd, additionalDirectories, mcpServers }` →
//! `{ sessionId }`; `session/prompt` `{ sessionId, prompt: ContentBlock[] }`
//! → `{ stopReason }`; `session/update` notification `{ sessionId, update }`
//! with `sessionUpdate` discriminator (`agent_message_chunk` carries optional
//! `messageId` + `content`); `session/request_permission` `{ sessionId,
//! toolCall, options }` → `{ decision, optionId? }`; `session/cancel`
//! notification `{ sessionId }`. Stop reasons: `end_turn` (completed),
//! `cancelled`/`discarded` (cancelled), everything else (error/rate_limited/
//! security_error/max_turns/input_required/unknown) → failed.
//!
//! Unknown extra fields are tolerated (forward-compatible, §131); required
//! fields missing at handshake are a typed incompatibility (§132). No DTO
//! here ever escapes the crate (firewall §7).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The ACP protocol version this adapter requests. `dsh-acp` negotiates the
/// supported version on `initialize`; the adapter records whatever the server
/// returns and treats a successful handshake as compatible (TASK 20 §13–§14).
pub const ACP_PROTOCOL_VERSION: &str = "2025-03-26";

/// ACP method names the adapter depends on (TASK 21).
pub const METHOD_SESSION_NEW: &str = "session/new";
pub const METHOD_SESSION_PROMPT: &str = "session/prompt";
pub const METHOD_SESSION_CANCEL: &str = "session/cancel";
pub const METHOD_SESSION_UPDATE: &str = "session/update";
pub const METHOD_SESSION_REQUEST_PERMISSION: &str = "session/request_permission";
pub const METHOD_SESSION_DELETE: &str = "session/delete";
pub const METHOD_SESSION_LIST: &str = "session/list";

/// ACP stop reasons that map to a completed run. Any other stop reason is a
/// failed run (cancelled has its own mapping).
pub const STOP_REASON_END_TURN: &str = "end_turn";
/// ACP stop reasons that map to a cancelled run.
pub const STOP_REASON_CANCELLED: &str = "cancelled";
pub const STOP_REASON_DISCARDED: &str = "discarded";

/// `session/new` params (TASK 21 §8). `mcpServers` must be empty (the ACP
/// baseline rejects non-empty MCP servers — the narrow trust surface).
#[derive(Debug, Clone, Serialize)]
pub struct NewSessionParams {
    pub cwd: String,
    #[serde(
        rename = "additionalDirectories",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub additional_directories: Vec<String>,
    #[serde(rename = "mcpServers", skip_serializing_if = "Vec::is_empty")]
    pub mcp_servers: Vec<Value>,
}

/// `session/new` result.
#[derive(Debug, Clone, Deserialize)]
pub struct NewSessionResult {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

/// One content block of a `session/prompt` (text only — TASK 21 §21).
#[derive(Debug, Clone, Serialize)]
pub struct ContentBlock {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}

/// `session/prompt` params.
#[derive(Debug, Clone, Serialize)]
pub struct PromptParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub prompt: Vec<ContentBlock>,
}

/// `session/prompt` result — the authoritative turn outcome (stop reason).
#[derive(Debug, Clone, Deserialize)]
pub struct PromptResult {
    #[serde(rename = "stopReason")]
    pub stop_reason: String,
}

/// `session/cancel` notification params.
#[derive(Debug, Clone, Serialize)]
pub struct CancelParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

/// Incoming `session/update` notification params (TASK 21 §32–§37).
#[derive(Debug, Clone, Deserialize)]
pub struct SessionUpdateNotification {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub update: SessionUpdate,
}

/// The typed `session/update` discriminator. Only the kinds the adapter
/// normalizes are named; everything else (plan/command/mode/usage/config/
/// session_info/…) is `Unknown` and ignored safely (§97: not every Harness
/// internal fact becomes a public event).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "sessionUpdate", rename_all = "snake_case")]
pub enum SessionUpdate {
    AgentMessageChunk {
        #[serde(default, rename = "messageId")]
        message_id: Option<String>,
        content: ContentBlockIn,
    },
    UserMessageChunk {
        #[serde(default, rename = "messageId")]
        message_id: Option<String>,
        content: ContentBlockIn,
    },
    AgentThoughtChunk {
        #[serde(default, rename = "messageId")]
        message_id: Option<String>,
        content: ContentBlockIn,
    },
    ToolCall {
        #[serde(rename = "toolCall")]
        tool_call: ToolCallUpdate,
    },
    #[serde(other)]
    Unknown,
}

/// The content of a message chunk. Only text is normalized into
/// `message.delta`; image/audio/resource blocks are tolerated and ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlockIn {
    Text {
        text: String,
    },
    #[serde(other)]
    Other,
}

/// A tool-call update (TASK 21 §48–§54). All fields except `toolCallId` are
/// optional; the adapter reads only the safe display surface (name/title/
/// kind/status) and a bounded raw input summary. No implementation details.
#[derive(Debug, Clone, Deserialize)]
pub struct ToolCallUpdate {
    #[serde(rename = "toolCallId")]
    pub tool_call_id: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub content: Option<Vec<Value>>,
    #[serde(default, rename = "rawInput")]
    pub raw_input: Option<Value>,
    #[serde(default, rename = "rawOutput")]
    pub raw_output: Option<Value>,
}

/// `session/request_permission` server→client params (TASK 21 §55).
#[derive(Debug, Clone, Deserialize)]
pub struct RequestPermissionParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "toolCall")]
    pub tool_call: ToolCallUpdate,
    #[serde(default)]
    pub options: Vec<PermissionOption>,
}

/// One permission option presented to the client.
#[derive(Debug, Clone, Deserialize)]
pub struct PermissionOption {
    #[serde(rename = "optionId")]
    pub option_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub kind: String,
}

/// `session/request_permission` client response. `decision` is `allow` or
/// `reject`; `optionId` names the chosen option when one matches (§56).
#[derive(Debug, Clone, Serialize)]
pub struct RequestPermissionResult {
    pub decision: String,
    #[serde(rename = "optionId", skip_serializing_if = "Option::is_none")]
    pub option_id: Option<String>,
}

/// An incoming server→client request (e.g. `session/request_permission`),
/// routed by the transport with its JSON-RPC id so a handler can respond.
#[derive(Debug, Clone)]
pub struct ServerRequest {
    pub id: u64,
    pub method: String,
    pub params: Value,
}

/// The `session/delete` params (best-effort; dsh-acp is fresh-sessions-only
/// and may answer -32601 — tolerated).
#[derive(Debug, Clone, Serialize)]
pub struct DeleteSessionParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

/// JSON-RPC 2.0 request (client → server).
#[derive(Debug, Clone, Serialize)]
pub struct Request {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// Incoming frame — a response, a notification, or a server→client request.
/// Routing is by presence of `id` and `method` (TASK 20 §27–§31).
#[derive(Debug, Clone, Deserialize)]
pub struct Incoming {
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<JsonRpcError>,
    #[serde(default)]
    pub params: Option<Value>,
}

/// JSON-RPC 2.0 error object (bounded extraction: code + safe message).
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcError {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub message: String,
    // `data` is deliberately not bound here — it is adapter-local debug.
}

/// ACP `initialize` params (TASK 20 §33/§37 — no machine/user secrets).
/// Wire field names are camelCase (ACP/JSON-RPC contract).
#[derive(Debug, Clone, Serialize)]
pub struct InitializeParams {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    #[serde(rename = "clientInfo")]
    pub client_info: ClientInfo,
    pub capabilities: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

/// ACP `initialize` result — the compatibility evidence of the handshake.
#[derive(Debug, Clone, Deserialize)]
pub struct InitializeResult {
    #[serde(default, rename = "protocolVersion")]
    pub protocol_version: String,
    #[serde(default, rename = "serverInfo")]
    pub server_info: Option<ServerInfo>,
    #[serde(default)]
    pub capabilities: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
}

impl Request {
    pub fn new(id: u64, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            method: method.into(),
            params,
        }
    }
}
