//! Typed local endpoint + free loopback port allocation (TASK 10 §14–§17,
//! §73–§74).
//!
//! The endpoint is never a free-form URL string: host/port/scheme are typed,
//! and `Debug` never shows an auth context (the secret lives in `Secret`,
//! separate from the endpoint). Port allocation binds a real loopback
//! listener to get an OS-assigned free port; the tiny bind→release window is
//! the classic TOCTOU race, so a `PortUnavailable` startup failure is
//! classified and retried a bounded number of times with a fresh port
//! (§17, §90–§91) rather than assumed impossible.

use std::io;
use std::net::{IpAddr, Ipv4Addr, TcpListener};

/// Canonical loopback binding (TASK 10 §14–§15). Explicit `127.0.0.1`, not
/// `localhost` (no `::1` ambiguity) and never `0.0.0.0`.
pub const LOOPBACK_HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// A local engine HTTP endpoint. Deliberately secret-free: the auth context
/// is a separate value that is never Display/Debug-visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint {
    pub host: IpAddr,
    pub port: u16,
    pub scheme: &'static str,
}

impl Endpoint {
    pub fn http(host: IpAddr, port: u16) -> Self {
        Self {
            host,
            port,
            scheme: "http",
        }
    }

    pub fn base_url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }

    /// The readiness document endpoint (verified: `GET /doc` returns the
    /// OpenAPI spec with `info.title == "opencode"`, 1.18.18).
    pub fn doc_url(&self) -> String {
        format!("{}/doc", self.base_url())
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// Ask the OS for a free loopback port by binding (then releasing) a probe
/// listener. The release window is the TOCTOU race the retry policy covers.
pub fn alloc_free_port() -> io::Result<u16> {
    let listener = TcpListener::bind((LOOPBACK_HOST, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
