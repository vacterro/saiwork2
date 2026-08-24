//! Application logging bootstrap (TASK 08 §40–§44).
//!
//! - Logs live under the canonical data root: `<data-root>/logs/`
//!   (PORTABILITY.md). Never CWD, never the source tree.
//! - File logs are bounded: daily rolling files, `MAX_LOG_FILES` kept,
//!   oldest deleted (law 13 — a 90 GB saiwork2.log is not architecture).
//! - File-logging failure is **not** a storage-class failure: we fall back
//!   to stderr and record an explicit warning. Logging failure never kills
//!   the application.
//! - Secrets are redacted at the log boundary (SECURITY.md): never log
//!   environment dumps; messages pass through `redact`.
//!
//! The subscriber is installed exactly once, by the desktop shell, before
//! core services start (startup order §8). Core crates only emit `tracing`
//! events; they never install subscribers.

use std::path::PathBuf;
use std::sync::Once;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::config::AppConfig;

/// Keep this many rolling log files on disk (bounded retention).
pub const MAX_LOG_FILES: usize = 5;

/// Outcome of logging bootstrap; surfaces in diagnostics (no secrets).
#[derive(Debug, Clone)]
pub struct LoggingInfo {
    /// Canonical log directory (`<data-root>/logs/`) when file logging
    /// succeeded. Files inside are rolling and bounded (`MAX_LOG_FILES`).
    pub log_dir: Option<PathBuf>,
    /// True when file logging failed and stderr fallback is active.
    pub fallback: bool,
}

/// Guard keeping the non-blocking file writer alive for the app lifetime.
/// Held by the desktop shell; dropped at process exit.
pub struct LoggingGuard {
    pub info: LoggingInfo,
    _guard: Option<WorkerGuard>,
}

static INIT: Once = Once::new();

/// Install the application tracing subscriber (file under `<data-root>/logs/`
/// with stderr fallback). Idempotent: only the first call installs a
/// subscriber (the shell calls this exactly once).
///
/// This must run **before** `App::bootstrap_with`, after the data root is
/// resolved (startup order §8: resolve root → logging → storage → services).
pub fn init(config: &AppConfig) -> LoggingGuard {
    let mut guard = None;
    let mut info = LoggingInfo {
        log_dir: None,
        fallback: false,
    };

    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("saiwork=info,engine_fake=info,info"));
        let stderr_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(true);

        // File layer: rolling daily, bounded count. Failure to create the
        // file falls back to stderr (logging failure ≠ storage failure).
        let registry = tracing_subscriber::registry();
        match rolling_writer(config) {
            Ok((writer, worker_guard)) => {
                info.log_dir = Some(config.logs_dir());
                guard = Some(worker_guard);
                let file_layer = tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_target(true)
                    .with_writer(writer);
                // file.and_then(stderr) → one Layer<Registry>; EnvFilter is
                // generic over the subscriber, so it layers on top last.
                let combined = file_layer.and_then(stderr_layer);
                let _ = registry.with(combined).with(filter).try_init();
            }
            Err(e) => {
                info.fallback = true;
                eprintln!("[saiwork2] file logging unavailable, using stderr: {e}");
                let _ = registry.with(stderr_layer).with(filter).try_init();
            }
        }
    });

    LoggingGuard {
        info,
        _guard: guard,
    }
}

/// Build the rolling file writer, or the reason file logging failed.
fn rolling_writer(
    config: &AppConfig,
) -> Result<(tracing_appender::non_blocking::NonBlocking, WorkerGuard), String> {
    std::fs::create_dir_all(config.logs_dir())
        .map_err(|e| format!("cannot create {}: {e}", config.logs_dir().display()))?;
    let builder = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("saiwork2")
        .filename_suffix("log")
        .max_log_files(MAX_LOG_FILES)
        .build(config.logs_dir())
        .map_err(|e| format!("cannot open log file: {e}"))?;
    Ok(tracing_appender::non_blocking(builder))
}

/// A panic hook that records the panic payload (redacted) and location
/// through tracing — so an unexpected crash lands in the log file with
/// context, never as a blank window. We never try to "recover" and continue;
/// this is capture-only (TASK 08 §44).
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let payload = info.payload();
        let message = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "non-string panic payload".to_string()
        };
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "unknown location".into());
        // Redacted: a panic message may contain user/secret material.
        tracing::error!(
            location = %location,
            "panic: {}",
            saiwork_diagnostics::redact(&message)
        );
    }));
}
