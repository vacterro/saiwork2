//! Typed OpenCode API client (TASK 11 §6–§9, §56).
//!
//! One client authority per runtime: it owns the base endpoint, the runtime
//! auth context, request construction, response validation, and error
//! mapping. It never owns UI state, storage, or the process. Timeouts are
//! split: short metadata requests get `metadata_timeout`; the message POST is
//! long-running (its lifetime is the run lifetime, §9) with only connection
//! establishment bounded; the abort request is short.
//!
//! Every request authenticates with Basic auth, username `opencode`
//! (verified 1.18.18: any other username → 401). The secret never leaves
//! this module's `Secret` type and is never logged.

use std::time::Duration;

use crate::endpoint::Endpoint;
use crate::errors::OpenCodeError;
use crate::models::{Message, ModelRef, ProviderList, Session};
use crate::secret::Secret;

/// The only accepted Basic-auth username (verified 1.18.18: `opencode`).
const AUTH_USER: &str = "opencode";

#[derive(Clone)]
pub(crate) struct ApiClient {
    inner: reqwest::Client,
    endpoint: Endpoint,
    secret: Secret,
    metadata_timeout: Duration,
    /// Bound for ordinary metadata responses (sessions, messages, error
    /// bodies). NOT the provider catalog bound: the real 1.18.18 `/provider`
    /// catalog is ~5 MiB (191 providers / 6615 models measured 2026-08-18),
    /// so the provider catalog has its own, larger bound.
    max_body_bytes: usize,
    /// Bound for the provider catalog endpoints (`/provider` and the
    /// `/config/providers` fallback). Larger than `max_body_bytes` by
    /// design — the catalog is the one metadata response that legitimately
    /// exceeds the ordinary bound.
    provider_catalog_max_bytes: usize,
}

impl ApiClient {
    pub(crate) fn new(
        endpoint: Endpoint,
        secret: Secret,
        connect_timeout: Duration,
        metadata_timeout: Duration,
        max_body_bytes: usize,
        provider_catalog_max_bytes: usize,
    ) -> Result<Self, OpenCodeError> {
        // No overall request timeout on the client: the message POST must be
        // allowed to live as long as the run (§9). Timeouts are applied per
        // request where they belong.
        let inner = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .redirect(reqwest::redirect::Policy::none()) // §71: redirects are suspicious
            .build()
            .map_err(|e| OpenCodeError::RequestFailed {
                detail: format!("http client: {e}"),
            })?;
        Ok(Self {
            inner,
            endpoint,
            secret,
            metadata_timeout,
            max_body_bytes,
            provider_catalog_max_bytes,
        })
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        rb.basic_auth(AUTH_USER, Some(self.secret.as_str()))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.endpoint.base_url(), path)
    }

    // -- providers / models -------------------------------------------------

    /// Fetch + normalize the provider catalog (TASK 11 §13, provider-bound).
    ///
    /// ONE normalization path: the raw server response → `ProviderList`,
    /// shared by both endpoints. Primary: `GET /provider`
    /// (`{all, default, connected}` — the verified 1.18.18 contract). Strict
    /// compatibility fallback: `GET /config/providers` (`{providers,
    /// default}`) ONLY when the primary answers 404/405 (route absent).
    /// Auth/config/server failures (401/403/429/5xx/timeouts) NEVER trigger
    /// the fallback — they stay visible as typed errors (TASK 11 §48, §71).
    ///
    /// Both catalog endpoints are read under `provider_catalog_max_bytes`,
    /// the dedicated catalog bound, never the ordinary metadata bound: the
    /// real 1.18.18 catalog is ~5 MiB, larger than the 4 MiB ordinary bound.
    pub(crate) async fn list_providers(&self) -> Result<ProviderList, OpenCodeError> {
        let resp = self
            .auth(self.inner.get(self.url("/provider")))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("list providers"))?;
        // §48: a non-2xx metadata response is a typed HTTP error (auth /
        // rate limit / server error), never a misleading JSON-parse failure.
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND
            || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
        {
            // Strict fallback: the route is absent on this OpenCode build.
            // Any other failure class stays visible below.
            return self.list_providers_fallback().await;
        }
        if !status.is_success() {
            let detail = self.read_error_detail(resp).await;
            return Err(http_error(status, "list providers", &detail));
        }
        let body = self
            .read_json_body_bounded(resp, "list providers", self.provider_catalog_max_bytes)
            .await?;
        parse_provider_list(&body, "list providers")
    }

    /// `/config/providers` fallback (`{providers, default}`). Called ONLY on
    /// a 404/405 from `/provider`. A failure here is surfaced as the same
    /// typed list-providers error — never silently swallowed.
    async fn list_providers_fallback(&self) -> Result<ProviderList, OpenCodeError> {
        let resp = self
            .auth(self.inner.get(self.url("/config/providers")))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("list providers"))?;
        let status = resp.status();
        if !status.is_success() {
            let detail = self.read_error_detail(resp).await;
            return Err(http_error(status, "list providers", &detail));
        }
        let body = self
            .read_json_body_bounded(resp, "list providers", self.provider_catalog_max_bytes)
            .await?;
        parse_provider_list(&body, "list providers")
    }

    // -- sessions -----------------------------------------------------------

    pub(crate) async fn list_sessions(&self) -> Result<Vec<Session>, OpenCodeError> {
        let resp = self
            .auth(self.inner.get(self.url("/session")))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("list sessions"))?;
        let status = resp.status();
        if !status.is_success() {
            let detail = self.read_error_detail(resp).await;
            return Err(http_error(status, "list sessions", &detail));
        }
        let body = self.read_json_body(resp, "list sessions").await?;
        serde_json::from_slice(&body).map_err(|e| OpenCodeError::Protocol {
            detail: format!("session list is not valid OpenCode data: {e}"),
        })
    }

    pub(crate) async fn create_session(&self) -> Result<Session, OpenCodeError> {
        let resp = self
            .auth(self.inner.post(self.url("/session")))
            .header("content-type", "application/json")
            .body("{}")
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("create session"))?;
        let status = resp.status();
        if !status.is_success() {
            let detail = self.read_error_detail(resp).await;
            return Err(http_error(status, "create session", &detail));
        }
        let body = self.read_json_body(resp, "create session").await?;
        serde_json::from_slice(&body).map_err(|e| OpenCodeError::Protocol {
            detail: format!("created session is not valid OpenCode data: {e}"),
        })
    }

    pub(crate) async fn get_session(&self, session_id: &str) -> Result<Session, OpenCodeError> {
        let resp = self
            .auth(self.inner.get(self.url(&format!("/session/{session_id}"))))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("get session"))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => {
                return Err(OpenCodeError::SessionNotFound {
                    session_id: session_id.to_string(),
                })
            }
            status if !status.is_success() => return Err(http_error(status, "get session", "")),
            _ => {}
        }
        let body = self.read_json_body(resp, "get session").await?;
        serde_json::from_slice(&body).map_err(|e| OpenCodeError::Protocol {
            detail: format!("session is not valid OpenCode data: {e}"),
        })
    }

    /// Read-only authoritative session history (`GET /session/{id}/message`,
    /// fixture-verified). Returns the raw message array; the adapter
    /// normalizes it. Missing session → SessionNotFound. Never used to
    /// mutate anything and never mirrored to SQLite (TASK 24 §9).
    pub(crate) async fn get_session_messages(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, OpenCodeError> {
        let resp = self
            .auth(
                self.inner
                    .get(self.url(&format!("/session/{session_id}/message"))),
            )
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("list session messages"))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status if status.is_success() => {
                let body = self.read_json_body(resp, "list session messages").await?;
                serde_json::from_slice(&body).map_err(|e| OpenCodeError::Protocol {
                    detail: format!("invalid session message history: {e}"),
                })
            }
            status => Err(http_error(status, "list session messages", "")),
        }
    }

    pub(crate) async fn delete_session(&self, session_id: &str) -> Result<(), OpenCodeError> {
        let resp = self
            .auth(
                self.inner
                    .delete(self.url(&format!("/session/{session_id}"))),
            )
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("delete session"))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status if status.is_success() => Ok(()),
            status => Err(http_error(status, "delete session", "")),
        }
    }

    pub(crate) async fn revert_session(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<(), OpenCodeError> {
        let resp = self
            .auth(self.inner.post(self.url(&format!("/session/{session_id}/revert"))))
            .json(&serde_json::json!({ "messageID": message_id }))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("revert session"))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status if status.is_success() => Ok(()),
            status => Err(http_error(status, "revert session", "")),
        }
    }

    pub(crate) async fn unrevert_session(&self, session_id: &str) -> Result<(), OpenCodeError> {
        let resp = self
            .auth(self.inner.post(self.url(&format!("/session/{session_id}/unrevert"))))
            .header("content-type", "application/json")
            .body("{}")
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("unrevert session"))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status if status.is_success() => Ok(()),
            status => Err(http_error(status, "unrevert session", "")),
        }
    }

    // -- message / abort / permission --------------------------------------

    /// Start a message send. **Two-phase**: the response resolves when the
    /// server ACCEPTS the request (HTTP 2xx headers — the run is live), and
    /// `PendingMessage::finish` reads the body (the final message, produced
    /// when the run ends, verified 1.18.18). No overall timeout; connection
    /// establishment is bounded by the client's `connect_timeout`. The
    /// response body is bounded defensively.
    pub(crate) async fn send_message_start(
        &self,
        session_id: &str,
        model: Option<&ModelRef>,
        text: &str,
    ) -> Result<PendingMessage, OpenCodeError> {
        let mut body = serde_json::Map::new();
        if let Some(model) = model {
            body.insert(
                "model".into(),
                serde_json::json!({ "providerID": model.providerID, "modelID": model.modelID }),
            );
        }
        body.insert(
            "parts".into(),
            serde_json::json!([{ "type": "text", "text": text }]),
        );
        let resp = self
            .auth(
                self.inner
                    .post(self.url(&format!("/session/{session_id}/message"))),
            )
            .header("content-type", "application/json")
            .body(serde_json::Value::Object(body).to_string())
            .send()
            .await
            .map_err(transport_error("send message"))?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            reqwest::StatusCode::CONFLICT => Err(OpenCodeError::SessionBusy {
                session_id: session_id.to_string(),
            }),
            status if !status.is_success() => {
                let detail = self.read_error_detail(resp).await;
                Err(http_error(status, "send message", &detail))
            }
            _ => Ok(PendingMessage {
                resp,
                max_body_bytes: self.max_body_bytes,
            }),
        }
    }

    /// Ask OpenCode to abort the active run of a session. Returns the
    /// server's verdict (true = a run was aborted). Short request.
    pub(crate) async fn abort(&self, session_id: &str) -> Result<bool, OpenCodeError> {
        let resp = self
            .auth(
                self.inner
                    .post(self.url(&format!("/session/{session_id}/abort"))),
            )
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("abort run"))?;
        match resp.status() {
            status if status.is_success() => {
                let body = self.read_json_body(resp, "abort run").await?;
                Ok(serde_json::from_slice(&body).unwrap_or(false))
            }
            status => Err(http_error(status, "abort run", "")),
        }
    }

    pub(crate) async fn reply_permission(
        &self,
        session_id: &str,
        request_id: &str,
        allowed: bool,
    ) -> Result<(), OpenCodeError> {
        let resp = self
            .auth(self.inner.post(self.url(&format!(
                "/session/{session_id}/permission/{request_id}/reply"
            ))))
            .header("content-type", "application/json")
            .body(format!("{{\"allowed\":{allowed}}}"))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("reply permission"))?;
        match resp.status() {
            status if status.is_success() => Ok(()),
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status => Err(http_error(status, "reply permission", "")),
        }
    }

    /// AUDIT-CORE-002: answer a pending user question. Wire shape is one
    /// selected option label per asked question (`answers: string[][]`;
    /// a single-selection answer maps to a one-element inner array).
    pub(crate) async fn reply_question(
        &self,
        session_id: &str,
        request_id: &str,
        answers: &[Vec<String>],
    ) -> Result<(), OpenCodeError> {
        let body = serde_json::json!({ "answers": answers }).to_string();
        let resp = self
            .auth(self.inner.post(self.url(&format!(
                "/session/{session_id}/question/{request_id}/reply"
            ))))
            .header("content-type", "application/json")
            .body(body)
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("reply question"))?;
        match resp.status() {
            status if status.is_success() => Ok(()),
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status => Err(http_error(status, "reply question", "")),
        }
    }

    /// AUDIT-CORE-002: authoritatively reject a pending user question.
    pub(crate) async fn reject_question(
        &self,
        session_id: &str,
        request_id: &str,
    ) -> Result<(), OpenCodeError> {
        let resp = self
            .auth(self.inner.post(self.url(&format!(
                "/session/{session_id}/question/{request_id}/reject"
            ))))
            .timeout(self.metadata_timeout)
            .send()
            .await
            .map_err(transport_error("reject question"))?;
        match resp.status() {
            status if status.is_success() => Ok(()),
            reqwest::StatusCode::NOT_FOUND => Err(OpenCodeError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
            status => Err(http_error(status, "reject question", "")),
        }
    }

    // -- body plumbing ------------------------------------------------------

    /// Read a JSON body with bounded size. HTTP error bodies are read with
    /// the same bound so a giant error page can never be buffered (§110).
    ///
    /// Streaming: the body is accumulated chunk-by-chunk and the response is
    /// dropped the instant cumulative bytes would exceed `cap` — a
    /// runaway/no-Content-Length response can never force a whole-body buffer
    /// plus a duplicate `Bytes -> Vec` allocation before the typed error
    /// (PERF-001).
    async fn read_json_body(
        &self,
        resp: reqwest::Response,
        op: &'static str,
    ) -> Result<Vec<u8>, OpenCodeError> {
        read_bounded_body_bytes(resp, self.max_body_bytes, op).await
    }

    /// Read a JSON body with a caller-supplied bound (the provider catalog
    /// uses its own larger bound; everything else uses `max_body_bytes`).
    async fn read_json_body_bounded(
        &self,
        resp: reqwest::Response,
        op: &'static str,
        bound: usize,
    ) -> Result<Vec<u8>, OpenCodeError> {
        read_bounded_body_bytes(resp, bound, op).await
    }

    /// Read an error-detail body bounded by `max_body_bytes`. Only enough
    /// bytes to preserve the existing <=400-char redacted diagnostic are ever
    /// retained — the response is dropped at the first over-cap chunk, so a
    /// huge error page is never fully buffered (PERF-001).
    async fn read_error_detail(&self, resp: reqwest::Response) -> String {
        match read_bounded_stream(resp, self.max_body_bytes, "read error detail").await {
            // Both outcomes keep at most `cap` bytes; take only 400 chars.
            Ok(BoundedBody::Complete(buf)) | Ok(BoundedBody::Exceeded { truncated: buf }) => {
                let detail: String = String::from_utf8_lossy(&buf).chars().take(400).collect();
                // §84: an error body may echo the Authorization header (or
                // otherwise carry the runtime secret); redact the actual
                // secret value before the detail can reach a log or the UI.
                self.secret.redact(&detail)
            }
            // Transport-level read failure: no diagnostic available.
            Err(_) => String::new(),
        }
    }
}

/// Two-phase message-send handle: the server accepted the request (2xx);
/// `finish` reads the final message body. `max_body_bytes` is carried so the
/// read stays bounded without holding the client.
pub(crate) struct PendingMessage {
    resp: reqwest::Response,
    max_body_bytes: usize,
}

impl PendingMessage {
    /// Finish the message send: read + parse the final message body. An error
    /// here means the run was accepted but its terminal is unreadable
    /// (truncated / malformed) — the caller must treat it as outcome-unknown,
    /// never a definite failure.
    pub(crate) async fn finish(self) -> Result<Message, OpenCodeError> {
        let body = read_bounded_body_bytes(self.resp, self.max_body_bytes, "send message").await?;
        serde_json::from_slice(&body).map_err(|e| OpenCodeError::Protocol {
            detail: format!("message response is not valid OpenCode data: {e}"),
        })
    }
}

/// Outcome of a size-bounded streaming body read (PERF-001).
///
/// INVARIANT: the client never holds more than `cap` bytes of body. When the
/// body exceeds `cap`, the response is dropped at the first over-cap chunk and
/// `truncated` (<= cap) is returned so a bounded diagnostic can still be
/// extracted — but the full body is never materialized, defeating the OOM
/// failure mode where a runaway/no-Content-Length response forced a whole
/// buffer plus a duplicate `Bytes -> Vec` allocation before the typed error.
#[derive(Debug)]
pub(crate) enum BoundedBody {
    /// Body fit within `cap`. `buf.len() <= cap`.
    Complete(Vec<u8>),
    /// Body exceeded `cap`. The response was dropped after the first over-cap
    /// chunk; `truncated` holds the bytes accumulated up to (and never
    /// exceeding) `cap`.
    Exceeded { truncated: Vec<u8> },
}

/// ONE shared streaming bounded-body reader (PERF-001).
///
/// Used by ordinary metadata, provider-catalog, readiness, message-finish, and
/// error-detail reads. Preserves the Content-Length fast reject (an oversize
/// *declared* length is an immediate protocol result, never an OpenCode error).
/// Then accumulates chunks only while cumulative bytes <= `cap`; the response
/// is dropped (no further pulls) the instant the next chunk would exceed `cap`.
/// The accumulated buffer is returned directly — no `Bytes -> Vec`
/// duplication. A transport read failure stays `Disconnected` (truncated
/// transport), so a body that breaks mid-stream is never mistaken for a
/// definite rejection.
pub(crate) async fn read_bounded_stream(
    resp: reqwest::Response,
    cap: usize,
    op: &'static str,
) -> Result<BoundedBody, OpenCodeError> {
    // Content-Length fast reject, preserved exactly from the prior design.
    if resp
        .content_length()
        .is_some_and(|len| len > cap as u64)
    {
        return Err(OpenCodeError::Protocol {
            detail: format!("{op} response exceeds {cap}-byte limit"),
        });
    }
    // Bounded initial capacity; the buffer can never grow past `cap`.
    let mut buf: Vec<u8> = Vec::with_capacity(cap.min(64 * 1024));
    let mut resp = resp;
    // `Response::chunk` pulls one framed chunk at a time (no `StreamExt`
    // dependency). The loop drops `resp` — closing the connection — the
    // instant the next chunk would exceed `cap`, so the server's tail is
    // never read (PERF-001).
    loop {
        let chunk = resp
            .chunk()
            .await
            .map_err(|e| OpenCodeError::Disconnected {
                detail: format!("{op}: response truncated: {e}"),
            })?;
        let Some(chunk) = chunk else {
            break;
        };
        if buf.len() + chunk.len() > cap {
            // First over-cap chunk: stop pulling and drop the response
            // immediately. `truncated` stays <= cap.
            return Ok(BoundedBody::Exceeded { truncated: buf });
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(BoundedBody::Complete(buf))
}

/// Read a JSON body with a caller-supplied bound via the shared streaming
/// reader. Over-cap bodies become a typed `Protocol` error; under/at-cap
/// bodies parse identically to before.
async fn read_bounded_body_bytes(
    resp: reqwest::Response,
    cap: usize,
    op: &'static str,
) -> Result<Vec<u8>, OpenCodeError> {
    match read_bounded_stream(resp, cap, op).await? {
        BoundedBody::Complete(buf) => Ok(buf),
        BoundedBody::Exceeded { .. } => Err(OpenCodeError::Protocol {
            detail: format!("{op} response exceeds {cap}-byte limit"),
        }),
    }
}

/// ONE provider-catalog normalization path (provider-bound policy): the raw
/// server JSON → `ProviderList`, accepting both documented wire shapes —
/// `/provider` (`{all, default, connected}`) and the `/config/providers`
/// fallback (`{providers, default}`). Malformed data is a typed protocol
/// error, never an empty fake-success list.
fn parse_provider_list(body: &[u8], op: &'static str) -> Result<ProviderList, OpenCodeError> {
    ProviderList::from_wire(body).map_err(|detail| OpenCodeError::Protocol {
        detail: format!("{op}: {detail}"),
    })
}

fn transport_error(op: &'static str) -> impl Fn(reqwest::Error) -> OpenCodeError {
    move |e| {
        if e.is_timeout() {
            OpenCodeError::RequestFailed {
                detail: format!("{op}: request timed out"),
            }
        } else if e.is_connect() {
            OpenCodeError::RequestFailed {
                detail: format!("{op}: connection failed: {e}"),
            }
        } else if e.is_body() {
            OpenCodeError::Disconnected {
                detail: format!("{op}: response truncated: {e}"),
            }
        } else {
            OpenCodeError::RequestFailed {
                detail: format!("{op}: {e}"),
            }
        }
    }
}

fn http_error(status: reqwest::StatusCode, op: &'static str, detail: &str) -> OpenCodeError {
    let detail = if detail.is_empty() {
        String::new()
    } else {
        format!(": {detail}")
    };
    match status.as_u16() {
        401 | 403 => OpenCodeError::Http {
            status: status.as_u16(),
            operation: op,
            detail: format!("authentication rejected{detail}"),
        },
        429 => OpenCodeError::Http {
            status: status.as_u16(),
            operation: op,
            detail: format!("rate limited{detail}"),
        },
        400 => OpenCodeError::Http {
            status: status.as_u16(),
            operation: op,
            detail: format!("invalid request{detail}"),
        },
        _ => OpenCodeError::Http {
            status: status.as_u16(),
            operation: op,
            detail: detail.trim_start_matches(':').to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// A minimal in-process HTTP/1.1 server (PERF-001 fixture). It serves one
    /// connection with a fixed body, either as a single `Content-Length`
    /// response or as a `Transfer-Encoding: chunked` stream of 1 KiB frames,
    /// and records how many bytes / chunks it actually pushed onto the socket.
    /// Because the streaming client drops the connection at the first over-cap
    /// chunk, the server's writes fail early — so `bytes_written` /
    /// `chunks_written` prove the client never read the tail.
    async fn serve_once(
        body: Vec<u8>,
        chunked: bool,
    ) -> (u16, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let written = Arc::new(AtomicUsize::new(0));
        let chunks = Arc::new(AtomicUsize::new(0));
        let w = written.clone();
        let c = chunks.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if let Ok((mut s, _)) = listener.accept().await {
                let mut req_buf = [0u8; 1024];
                let _ = s.read(&mut req_buf).await;
                let head = if chunked {
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n".to_string()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                        body.len()
                    )
                };
                let _ = s.write_all(head.as_bytes()).await;
                if chunked {
                    let mut i = 0;
                    while i < body.len() {
                        let end = (i + 1024).min(body.len());
                        let ch = &body[i..end];
                        let frame = format!("{:x}\r\n", ch.len());
                        if s.write_all(frame.as_bytes()).await.is_err() {
                            break;
                        }
                        if s.write_all(ch).await.is_err() {
                            break;
                        }
                        if s.write_all(b"\r\n").await.is_err() {
                            break;
                        }
                        w.fetch_add(ch.len(), Ordering::SeqCst);
                        c.fetch_add(1, Ordering::SeqCst);
                        i = end;
                    }
                    let _ = s.write_all(b"0\r\n\r\n").await;
                } else if s.write_all(&body).await.is_ok() {
                    w.fetch_add(body.len(), Ordering::SeqCst);
                }
                let _ = s.shutdown().await;
            }
        });
        (port, written, chunks)
    }

    fn test_client(port: u16, cap: usize, provider_cap: usize) -> ApiClient {
        ApiClient::new(
            Endpoint::http(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port),
            Secret::generate(),
            Duration::from_secs(3),
            Duration::from_secs(3),
            cap,
            provider_cap,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bounded_reader_stops_at_cap_and_skips_tail() {
        let cap = 8192usize;
        // 8 MiB body, far above any loopback socket buffer, so a single
        // assert (`written < total`) is robust against TCP backpressure: the
        // server can never push the whole tail before the client drops.
        let total: usize = 8 * 1024 * 1024;
        let body = vec![b'X'; total];
        let (port, written, chunks) = serve_once(body, true).await;
        let client = test_client(port, cap, cap * 4);
        let resp = client
            .inner
            .get(format!("http://127.0.0.1:{port}/x"))
            .send()
            .await
            .unwrap();
        let result = read_bounded_stream(resp, cap, "test").await.unwrap();
        match result {
            BoundedBody::Exceeded { truncated } => assert!(truncated.len() <= cap),
            BoundedBody::Complete(buf) => panic!("expected Exceeded, got {} bytes", buf.len()),
        }
        // Let the server observe the closed connection.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let w = written.load(Ordering::SeqCst);
        let c = chunks.load(Ordering::SeqCst);
        assert!(w < total, "client buffered the whole {total}-byte tail ({w})");
        assert!(c < (total / 1024), "client read all {c} chunks");
    }

    #[tokio::test]
    async fn bounded_reader_stores_only_o_cap_for_huge_body() {
        // A 5 MiB body with an 8 KiB cap: client-side storage must stay O(cap),
        // never O(response size). The strong, non-flaky proof is the retained
        // buffer length.
        let cap = 8192usize;
        let body = vec![b'Y'; 5 * 1024 * 1024];
        let (port, _, _) = serve_once(body, true).await;
        let client = test_client(port, cap, cap * 4);
        let resp = client
            .inner
            .get(format!("http://127.0.0.1:{port}/x"))
            .send()
            .await
            .unwrap();
        match read_bounded_stream(resp, cap, "test").await.unwrap() {
            BoundedBody::Exceeded { truncated } => assert!(truncated.len() <= cap),
            BoundedBody::Complete(buf) => panic!("expected Exceeded, got {} bytes", buf.len()),
        }
    }

    #[tokio::test]
    async fn bounded_reader_exact_cap_and_cap_minus_one_parse() {
        let cap = 4096usize;
        // exact cap -> Complete, len == cap
        let (port, _, _) = serve_once(vec![b'A'; cap], true).await;
        let client = test_client(port, cap, cap * 4);
        let resp = client
            .inner
            .get(format!("http://127.0.0.1:{port}/x"))
            .send()
            .await
            .unwrap();
        match read_bounded_stream(resp, cap, "test").await.unwrap() {
            BoundedBody::Complete(buf) => assert_eq!(buf.len(), cap),
            BoundedBody::Exceeded { .. } => panic!("exact cap must be Complete"),
        }
        // cap - 1 -> Complete, len == cap - 1
        let (port, _, _) = serve_once(vec![b'B'; cap - 1], true).await;
        let client = test_client(port, cap, cap * 4);
        let resp = client
            .inner
            .get(format!("http://127.0.0.1:{port}/x"))
            .send()
            .await
            .unwrap();
        match read_bounded_stream(resp, cap, "test").await.unwrap() {
            BoundedBody::Complete(buf) => assert_eq!(buf.len(), cap - 1),
            BoundedBody::Exceeded { .. } => panic!("cap-1 must be Complete"),
        }
    }

    #[tokio::test]
    async fn content_length_fast_reject_is_immediate_protocol_error() {
        let cap = 4096usize;
        // Declared Content-Length far exceeds cap: rejected before any read.
        let (port, _, _) = serve_once(vec![b'Z'; cap * 4], false).await;
        let client = test_client(port, cap, cap * 4);
        let resp = client
            .inner
            .get(format!("http://127.0.0.1:{port}/x"))
            .send()
            .await
            .unwrap();
        let err = read_bounded_stream(resp, cap, "test").await.unwrap_err();
        assert!(matches!(err, OpenCodeError::Protocol { .. }));
    }

    #[tokio::test]
    async fn error_detail_is_bounded_and_redacted_for_huge_secret_body() {
        let cap = 4096usize;
        let secret = Secret::generate();
        let secret_val = secret.as_str().to_string();
        // A huge error page that echoes the secret many times.
        let mut body = String::new();
        for _ in 0..5000 {
            body.push_str(&format!("boom secret={secret_val} more text\n"));
        }
        let (port, _, _) = serve_once(body.into_bytes(), true).await;
        let client = ApiClient::new(
            Endpoint::http(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port),
            secret,
            Duration::from_secs(3),
            Duration::from_secs(3),
            cap,
            cap * 4,
        )
        .unwrap();
        let resp = client
            .inner
            .get(format!("http://127.0.0.1:{port}/x"))
            .send()
            .await
            .unwrap();
        let detail = client.read_error_detail(resp).await;
        // Bounded to <=400 chars and the secret is redacted.
        assert!(detail.len() <= 400, "detail not bounded: {} chars", detail.len());
        assert!(
            !detail.contains(&secret_val),
            "secret leaked into error detail"
        );
        assert!(detail.contains("***"), "redaction marker missing");
    }

    #[tokio::test]
    async fn list_providers_rejects_giant_catalog_without_buffering() {
        let cap = 4096usize;
        // A provider catalog many times the (small test) provider cap.
        let catalog = format!(
            "{{\"all\":[{{\"id\":\"p\",\"models\":{{\"m\":{{}}}}}}],\"default\":{{}}}}"
        );
        let big = catalog.repeat(2000); // ~ 80 KiB, > cap
        let (port, _, _) = serve_once(big.into_bytes(), true).await;
        let client = test_client(port, cap, cap); // provider cap == ordinary cap for the test
        let err = client.list_providers().await.unwrap_err();
        assert!(matches!(err, OpenCodeError::Protocol { .. }));
    }

    #[tokio::test]
    async fn list_providers_parses_small_catalog() {
        let cap = 4096usize;
        let catalog = br#"{"all":[{"id":"p1","models":{"m1":{}}}],"default":{"k":"v"}}"#.to_vec();
        let (port, _, _) = serve_once(catalog, true).await;
        let client = test_client(port, cap, cap * 4);
        let list = client.list_providers().await.unwrap();
        assert_eq!(list.all.len(), 1);
        assert_eq!(list.all[0].id, "p1");
        assert!(list.all[0].models.contains_key("m1"));
        assert_eq!(list.default.get("k"), Some(&"v".to_string()));
    }

    #[tokio::test]
    async fn readiness_drops_oversized_doc_at_cap() {
        use crate::readiness::{probe_once, ProbeOutcome, ReadinessConfig};
        let cap = 4096usize;
        let doc = format!(
            "{{\"openapi\":\"3.1.0\",\"info\":{{\"title\":\"opencode\"}},\"pad\":\"{}\"}}",
            "x".repeat(cap * 4)
        );
        let (port, _, _) = serve_once(doc.into_bytes(), true).await;
        let req = reqwest::Client::new();
        let cfg = ReadinessConfig {
            startup_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(3),
            max_response_bytes: cap,
        };
        let outcome = probe_once(
            &req,
            &Endpoint::http(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port),
            &Secret::generate(),
            &cfg,
        )
        .await
        .unwrap();
        assert!(matches!(outcome, ProbeOutcome::NotOpenCode));
    }
}
