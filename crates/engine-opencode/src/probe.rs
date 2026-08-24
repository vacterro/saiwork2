//! Lightweight OpenCode probe (TASK 10 §9–§11, §46–§47).
//!
//! Before any server launch the adapter proves the executable actually is
//! OpenCode and that server mode exists: `--version` must return a
//! version-looking string (identity), and `serve --help` must advertise the
//! serve command (capability). Never a full server launch for a probe (§47).
//! Probes are cheap CLI calls executed through the ProcessSupervisor like
//! every other OpenCode process (§12).

use std::sync::Arc;
use std::time::Duration;

use saiwork_events::ProcessId;
use saiwork_process::ProcessSupervisor;
use uuid::Uuid;

use crate::errors::OpenCodeError;
use crate::launch;
use crate::DiscoveredExecutable;

#[derive(Debug, Clone)]
pub struct ProbeResult {
    /// Raw reported version string (no invented SemVer parsing — §10).
    pub version: String,
    /// Resolved executable path.
    pub executable: String,
}

/// Probe the executable: identity via `--version`, capability via
/// `serve --help`. Both probes are bounded by `timeout`.
pub async fn probe(
    supervisor: &Arc<ProcessSupervisor>,
    discovered: &DiscoveredExecutable,
    timeout: Duration,
) -> Result<ProbeResult, OpenCodeError> {
    let executable = discovered.display();

    // Identity: `opencode --version` must look like a version.
    let version = run_capture(supervisor, discovered, &["--version".into()], timeout)
        .await?
        .trim()
        .to_string();
    let looks_like_version = version.chars().next().is_some_and(|c| c.is_ascii_digit());
    if version.is_empty() || !looks_like_version {
        return Err(OpenCodeError::ProbeFailed {
            executable,
            detail: format!("`--version` output does not look like OpenCode: {version:?}"),
        });
    }
    // One canonical compatibility rule (TASK 24 §9): OpenCode >= 1.18 is the
    // verified wire surface. An older (or unknown-major) runtime must fail
    // HERE with an actionable version error — never reach READY and fail
    // later as misleading model/session/protocol errors.
    if let Err(reason) = version_supported(&version) {
        return Err(OpenCodeError::ProbeFailed {
            executable,
            detail: format!(
                "OpenCode {version:?} is not supported: {reason} (SAIWORK2 supports opencode >= 1.18; upgrade or point SAIWORK2_OPENCODE at a supported binary)"
            ),
        });
    }

    // Capability: `serve --help` must advertise the headless server.
    let help = run_capture(
        supervisor,
        discovered,
        &["serve".into(), "--help".into()],
        timeout,
    )
    .await?;
    let lower = help.to_lowercase();
    if !(lower.contains("headless opencode server") || lower.contains("opencode serve")) {
        return Err(OpenCodeError::ProbeFailed {
            executable,
            detail: "`serve --help` does not advertise the serve command".into(),
        });
    }

    Ok(ProbeResult {
        version,
        executable,
    })
}

/// Canonical compatibility rule: accept `1.x` with `x >= 18`. Anything else
/// (older 1.x, major 0, unknown/future major) is rejected with a precise
/// reason. Kept as a pure function for direct testing.
fn version_supported(version: &str) -> Result<(), String> {
    let v = version.trim().trim_start_matches('v');
    let mut parts = v.split('.');
    let major: u64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "not a numeric version".to_string())?;
    let minor: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    if major == 1 && minor >= 18 {
        Ok(())
    } else if major == 0 {
        Err(format!("major version 0 is a pre-release"))
    } else if major < 1 {
        Err(format!("version {version} is too old (needs >= 1.18)"))
    } else if major > 1 {
        Err(format!(
            "major version {major} is unknown/unsupported (supported: 1.x >= 1.18)"
        ))
    } else {
        Err(format!("version {version} is too old (needs >= 1.18)"))
    }
}

/// Run one short-lived OpenCode CLI command through the supervisor and
/// return its combined captured output (bounded ring). Fails if the process
/// cannot be spawned, exits non-zero, or exceeds the timeout.
async fn run_capture(
    supervisor: &Arc<ProcessSupervisor>,
    discovered: &DiscoveredExecutable,
    args: &[String],
    timeout: Duration,
) -> Result<String, OpenCodeError> {
    let id = ProcessId::new(format!("opencode-probe-{}", Uuid::new_v4()));
    let spec = launch::spec_for_args(discovered, id.clone(), None, Vec::new(), args);
    let process = supervisor
        .spawn(spec)
        .await
        .map_err(|e| OpenCodeError::ProbeFailed {
            executable: discovered.display(),
            detail: format!("spawn failed: {e}"),
        })?;
    let code = wait_exit(&process, timeout, &discovered.display()).await?;
    let out = process.stdout().join("\n") + "\n" + &process.stderr().join("\n");
    let out = truncate(&out, 4000);
    if code != Some(0) {
        return Err(OpenCodeError::ProbeFailed {
            executable: discovered.display(),
            detail: format!("exit code {code:?}; output: {out}"),
        });
    }
    Ok(out)
}

/// Wait for process exit (bounded), short-circuiting on exit before timeout.
async fn wait_exit(
    process: &saiwork_process::ManagedProcess,
    timeout: Duration,
    executable: &str,
) -> Result<Option<i32>, OpenCodeError> {
    let mut rx = process.exit();
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(info) = *rx.borrow() {
                return info.code;
            }
            if rx.changed().await.is_err() {
                return None;
            }
        }
    })
    .await
    .map_err(|_| OpenCodeError::ProbeFailed {
        executable: executable.into(),
        detail: format!("probe exceeded {timeout:?}"),
    })
}

fn truncate(s: &str, max: usize) -> String {
    crate::events::truncate(s, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_versions_pass() {
        assert!(version_supported("1.18.0").is_ok());
        assert!(version_supported("1.18.18").is_ok());
        assert!(version_supported("1.99.0").is_ok());
        assert!(version_supported("v1.18.5").is_ok());
    }

    #[test]
    fn unsupported_versions_are_rejected_with_reason() {
        assert!(version_supported("0.5.0").is_err());
        assert!(version_supported("1.17.9").is_err());
        assert!(version_supported("2.0.0").is_err());
        assert!(version_supported("1").is_err()); // minor defaults to 0 — too old
        assert!(version_supported("1.18").is_ok());
        assert!(version_supported("banana").is_err());
    }

    #[test]
    fn probe_truncate_multibyte_safe() {
        let kanji = "診断テスト".repeat(10);
        let t = truncate(&kanji, 5);
        assert_eq!(t, "診…(truncated)");
    }
}
