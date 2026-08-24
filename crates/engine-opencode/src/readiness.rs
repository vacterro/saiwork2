//! Readiness probing (TASK 10 §27–§32, §69–§72).
//!
//! Readiness is real protocol evidence, never `sleep(2s)`: `GET /doc` must
//! return HTTP 200 with an OpenAPI body whose `info.title` is `opencode`
//! (verified against 1.18.18). A plain TCP accept, a proxy page, or any
//! non-OpenCode JSON is NOT readiness. Every request carries the runtime's
//! Basic auth secret and a per-request timeout; the loop is bounded by an
//! outer startup deadline and honors a cancellation channel (application
//! shutdown during STARTING — §34, §76). Process exit short-circuits the
//! loop instead of waiting out the deadline (§31).

use std::sync::Arc;
use std::time::{Duration, Instant};

use saiwork_process::ManagedProcess;
use tokio::sync::watch;

use crate::client::{read_bounded_stream, BoundedBody};
use crate::endpoint::{Endpoint, LOOPBACK_HOST};
use crate::errors::OpenCodeError;
use crate::secret::Secret;

/// OpenCode server identity as reported by `GET /doc` (verified 1.18.18).
const EXPECTED_TITLE: &str = "opencode";

#[derive(Clone)]
pub struct ReadinessConfig {
    pub startup_timeout: Duration,
    pub request_timeout: Duration,
    pub max_response_bytes: usize,
}

/// Outcome of one readiness probe.
pub(crate) enum ProbeOutcome {
    Ready,
    /// Endpoint answered with HTTP but the body is not an OpenCode /doc.
    NotOpenCode,
    /// Connection-level unavailability (refused/reset/timeout/non-2xx).
    Unavailable,
    /// 401 — the server demands different credentials than we have.
    AuthFailed,
}

/// Poll the endpoint until READY, the startup deadline, process exit, or
/// cancellation. Returns the effective endpoint (confirmed by the server's
/// own "listening on" line when present).
pub async fn wait_ready(
    endpoint: &Endpoint,
    secret: &Secret,
    process: &Arc<ManagedProcess>,
    cfg: &ReadinessConfig,
    mut cancel: watch::Receiver<bool>,
) -> Result<Endpoint, OpenCodeError> {
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .connect_timeout(cfg.request_timeout)
        .redirect(reqwest::redirect::Policy::none()) // §71: redirects are suspicious
        .build()
        .map_err(|e| OpenCodeError::SpawnFailed {
            detail: format!("http client: {e}"),
        })?;

    let deadline = Instant::now() + cfg.startup_timeout;
    let mut effective = *endpoint;
    let mut saw_wrong_identity = false;
    let mut saw_http = false;
    let mut backoff = Duration::from_millis(250);

    loop {
        if *cancel.borrow() {
            return Err(OpenCodeError::Cancelled);
        }

        // §31: process exit short-circuits the timeout.
        if let Some(info) = *process.exit().borrow() {
            return Err(OpenCodeError::ExitedDuringStartup {
                code: info.code,
                tail: output_tail(process),
            });
        }

        // The server's own announcement is authoritative for the effective
        // endpoint (defense against the allocated port being rebound).
        if let Some(port) = parse_listening_port(&process.stdout()) {
            if port != effective.port {
                effective = Endpoint::http(LOOPBACK_HOST, port);
            }
        }

        match probe_once(&client, &effective, secret, cfg).await {
            Ok(ProbeOutcome::Ready) => return Ok(effective),
            Ok(ProbeOutcome::NotOpenCode) => {
                saw_http = true;
                saw_wrong_identity = true;
            }
            Ok(ProbeOutcome::Unavailable) => {
                if saw_http {
                    // Server answered once with HTTP but is now flaky — keep
                    // probing; deadline decides.
                }
            }
            Ok(ProbeOutcome::AuthFailed) => {
                return Err(OpenCodeError::AuthConfigurationFailed {
                    detail: format!(
                        "endpoint {} rejected the runtime credential (401)",
                        effective
                    ),
                });
            }
            Err(_) => {} // transport-level probe failure: treated as unavailable
        }

        if Instant::now() > deadline {
            if saw_wrong_identity {
                return Err(OpenCodeError::ProtocolUnexpected {
                    detail: format!(
                        "endpoint {} answered HTTP but never presented an OpenCode /doc",
                        effective
                    ),
                });
            }
            return Err(OpenCodeError::ReadinessTimeout {
                endpoint: effective.to_string(),
                timeout: cfg.startup_timeout,
                detail: if saw_http {
                    "endpoint answered but never became ready".into()
                } else {
                    "endpoint never accepted a request".into()
                },
            });
        }

        // Bounded wait honoring cancellation (§34).
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = cancel.changed() => return Err(OpenCodeError::Cancelled),
        }
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

pub(crate) async fn probe_once(
    client: &reqwest::Client,
    endpoint: &Endpoint,
    secret: &Secret,
    cfg: &ReadinessConfig,
) -> Result<ProbeOutcome, ()> {
    let resp = match client
        .get(endpoint.doc_url())
        .basic_auth("opencode", Some(secret.as_str()))
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return Ok(ProbeOutcome::Unavailable),
    };
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(ProbeOutcome::AuthFailed);
    }
    if !status.is_success() {
        return Ok(ProbeOutcome::Unavailable);
    }
    // §72: bounded response — refuse obviously oversized bodies up front.
    if resp
        .content_length()
        .is_some_and(|len| len > cfg.max_response_bytes as u64)
    {
        return Ok(ProbeOutcome::NotOpenCode);
    }
    // Streaming bounded read: a runaway/oversized /doc is dropped at the first
    // over-cap chunk (PERF-001) instead of being buffered whole.
    let body = match read_bounded_stream(resp, cfg.max_response_bytes, "readiness").await {
        Ok(BoundedBody::Complete(buf)) => buf,
        // Over-cap /doc: not a valid OpenCode identity document.
        Ok(BoundedBody::Exceeded { .. }) => return Ok(ProbeOutcome::NotOpenCode),
        // Truncated transport stays Unavailable (never a definite identity).
        Err(_) => return Ok(ProbeOutcome::Unavailable),
    };
    match is_opencode_doc(&body) {
        true => Ok(ProbeOutcome::Ready),
        false => Ok(ProbeOutcome::NotOpenCode),
    }
}

/// True when the body is an OpenAPI document identifying this server as
/// OpenCode (identity + capability evidence, §28).
fn is_opencode_doc(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    value
        .get("info")
        .and_then(|info| info.get("title"))
        .and_then(|title| title.as_str())
        .is_some_and(|title| title.eq_ignore_ascii_case(EXPECTED_TITLE))
}

/// Parse `opencode server listening on http://127.0.0.1:PORT` (stdout,
/// verified 1.18.18) to confirm the effective port.
fn parse_listening_port(lines: &[String]) -> Option<u16> {
    for line in lines {
        let marker = "listening on http://";
        let Some(idx) = line.find(marker) else {
            continue;
        };
        let rest = &line[idx + marker.len()..];
        let host_port = rest.split_whitespace().next()?;
        let port = host_port.rsplit(':').next()?;
        if let Ok(port) = port.parse::<u16>() {
            return Some(port);
        }
    }
    None
}

/// Bounded, redacted tail of captured output for failure diagnostics (§32).
fn output_tail(process: &ManagedProcess) -> String {
    let mut lines: Vec<String> = process.stdout().into_iter().collect();
    lines.extend(process.stderr());
    let joined = lines.join(" | ");
    truncate(&joined, 4000)
}

fn truncate(s: &str, max: usize) -> String {
    crate::events::truncate(s, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listening_line_parses_port() {
        let lines = vec!["opencode server listening on http://127.0.0.1:4096".to_string()];
        assert_eq!(parse_listening_port(&lines), Some(4096));
    }

    #[test]
    fn listening_line_missing_returns_none() {
        assert_eq!(parse_listening_port(&[]), None);
        assert_eq!(parse_listening_port(&["nothing here".to_string()]), None);
    }

    #[test]
    fn opencode_doc_is_recognized() {
        let doc = br#"{"openapi":"3.1.0","info":{"title":"opencode","version":"1.0.0"}}"#;
        assert!(is_opencode_doc(doc));
        // Wrong title / plain JSON / empty are NOT readiness.
        assert!(!is_opencode_doc(
            br#"{"openapi":"3.1.0","info":{"title":"proxy"}}"#
        ));
        assert!(!is_opencode_doc(br#"{}"#));
        assert!(!is_opencode_doc(br#"<html>proxy page</html>"#));
        assert!(!is_opencode_doc(b""));
    }

    #[test]
    fn readiness_truncate_multibyte_safe() {
        let kanji = "準備完了シグナル".repeat(10);
        let t = truncate(&kanji, 7);
        assert_eq!(t, "準備…(truncated)");
    }
}
