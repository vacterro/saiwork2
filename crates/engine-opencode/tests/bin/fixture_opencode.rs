//! Hostile-test fixture: a fake `opencode` executable the adapter can be
//! pointed at (TASK 10 §97–§98).
//!
//! CLI surface mirrors the real contract (verified 1.18.18):
//! - `--version` → prints a version, exit 0
//! - `serve --help` → prints help advertising the headless server
//! - `serve --port N --hostname … [--pure]` → binds 127.0.0.1:N, prints
//!   `opencode server listening on http://127.0.0.1:N`, serves `GET /doc`
//!
//! Behavior is selected by env `FIXTURE_MODE`:
//! - `real`           — proper /doc, stays alive
//! - `never_ready`    — /doc with a non-OpenCode identity, forever
//! - `wrong_response` — HTTP 200 with `{}`
//! - `delayed_ready`  — drops connections for 3 s, then serves proper /doc
//! - `exit_now`       — exit(1) before binding
//! - `exit_after_bind`— bind + print listening, then exit(1)
//! - `hang`           — stay alive, never bind
//! - `collision`      — always fail with "address in use" (simulates
//!   EADDRINUSE even when the port is actually free)
//!
//! `FIXTURE_AUTH=1` + `FIXTURE_PASSWORD` require Basic auth on /doc
//! (any non-empty username, matching OpenCode 1.18.18).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::exit;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = std::env::var("FIXTURE_MODE").unwrap_or_else(|_| "real".into());

    // Probe contract (identity + capability).
    if args.get(1).map(String::as_str) == Some("--version") {
        println!("1.18.18");
        exit(0);
    }
    if args.get(1).map(String::as_str) == Some("serve") && args.iter().any(|a| a == "--help") {
        println!("opencode serve\n\nstarts a headless opencode server\n\nOptions:\n  --port      port to listen on\n  --hostname  hostname to listen on\n");
        exit(0);
    }

    // Server mode.
    let port = parse_port(&args).unwrap_or(0);
    let host = "127.0.0.1";

    if mode == "exit_now" {
        eprintln!("fixture: exiting before bind");
        exit(1);
    }

    let listener = match TcpListener::bind((host, port)) {
        Ok(l) => l,
        Err(e) => {
            // Mirror what the real server prints on a collision.
            eprintln!("fixture: address in use: {e}");
            eprintln!("Error: listen EADDRINUSE: address already in use {host}:{port}");
            exit(1);
        }
    };
    let actual = listener.local_addr().unwrap().port();
    println!("opencode server listening on http://{host}:{actual}");

    if mode == "collision" {
        // Simulate EADDRINUSE even when the port is actually free: the
        // adapter must classify this from the output and retry (§17, §50).
        eprintln!("fixture: address in use");
        eprintln!("Error: listen EADDRINUSE: address already in use {host}:{actual}");
        exit(1);
    }
    if mode == "exit_after_bind" {
        eprintln!("fixture: exiting after bind");
        exit(1);
    }
    if mode == "hang" {
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }

    let ready_at = if mode == "delayed_ready" {
        Some(Instant::now() + Duration::from_secs(3))
    } else {
        None
    };
    let doc = match mode.as_str() {
        "never_ready" => r#"{"openapi":"3.1.0","info":{"title":"not-opencode","version":"1.0.0"}}"#,
        "wrong_response" => r#"{}"#,
        _ => r#"{"openapi":"3.1.0","info":{"title":"opencode","version":"1.0.0"}}"#,
    };

    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let mut peer = stream.try_clone().unwrap();
        std::thread::spawn(move || {
            if let Some(deadline) = ready_at {
                let now = Instant::now();
                if now < deadline {
                    // Not ready yet: drop the connection (probe fails fast).
                    drop(stream);
                    return;
                }
            }
            handle(&mut stream, &mut peer, doc);
        });
    }
}

fn handle(stream: &mut TcpStream, peer: &mut TcpStream, doc: &str) {
    let mut buf = [0u8; 2048];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    // Keep the raw request: the base64 Authorization value is case-sensitive
    // and must not be lowercased. Only the method/path/header-name matching
    // is case-insensitive.
    let raw = String::from_utf8_lossy(&buf[..n]).to_string();
    let lower = raw.to_lowercase();
    if lower.starts_with("get /doc") || lower.contains(" /doc ") {
        if auth_required(&raw) {
            let _ = write_str(
                peer,
                "HTTP/1.1 401 Unauthorized\r\nwww-authenticate: Basic realm=\"Secure Area\"\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            );
            return;
        }
        let body = doc.as_bytes();
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        let _ = write_str(peer, &head);
        let _ = write_all(peer, body);
    } else {
        let _ = write_str(
            peer,
            "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
    }
}

/// True when the request lacks a valid Basic auth header. Mirrors OpenCode:
/// any non-empty username; the password must match FIXTURE_PASSWORD when one
/// is configured (when unset, any non-empty password is accepted).
fn auth_required(request: &str) -> bool {
    if std::env::var("FIXTURE_AUTH").ok().as_deref() != Some("1") {
        return false;
    }
    let expected = std::env::var("FIXTURE_PASSWORD").ok();
    // Header name match is case-insensitive; the raw (cased) value is used.
    let mut auth_value = None;
    let lower = request.to_lowercase();
    for (i, l) in lower.lines().enumerate() {
        if l.starts_with("authorization:") {
            if let Some(raw_line) = request.lines().nth(i) {
                auth_value = Some(raw_line);
            }
            break;
        }
    }
    let Some(line) = auth_value else {
        return true;
    };
    // Slice past the header-name colon (the name itself may be any case).
    let Some(colon) = line.find(':') else {
        return true;
    };
    let token = line[colon + 1..].trim();
    // Scheme is case-insensitive per RFC 7235.
    let Some((scheme, b64)) = token.split_once(' ') else {
        return true;
    };
    if !scheme.eq_ignore_ascii_case("basic") {
        return true;
    }
    use base64_simple;
    match base64_simple::decode(b64.trim()) {
        Ok(decoded) => {
            let decoded = String::from_utf8_lossy(&decoded);
            let Some((_, password)) = decoded.split_once(':') else {
                return true;
            };
            match expected {
                None => false, // any non-empty password passes
                Some(p) => password != p,
            }
        }
        Err(_) => true,
    }
}

fn write_all(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    stream.write_all(data)
}

#[allow(unused)]
fn write_str(stream: &mut TcpStream, data: &str) -> std::io::Result<()> {
    stream.write_all(data.as_bytes())
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

/// Tiny base64 decoder (RFC 4648) so the fixture needs no extra dependency.
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
