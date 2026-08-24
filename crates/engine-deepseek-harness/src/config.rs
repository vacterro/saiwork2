//! Harness configuration, executable discovery, probe, and runtime
//! `ProcessSpec` construction (TASK 20 §9–§11, §17, §47–§49).
//!
//! Discovery precedence (§9): 1) explicit configured executable/path
//! (authoritative — an invalid explicit path is an error, never a silent
//! fallback, §10), 2) PATH lookup of known `dsh` launchers. No recursive disk
//! scan, no npm-global archaeology (§9). Nothing is ever a shell string: the
//! resolved executable is spawned directly with args as separate OS
//! arguments (§17).

use std::path::PathBuf;
use std::time::Duration;

use saiwork_process::{ProcessSpec, ProcessSupervisor, StdinPolicy};

use crate::error::HarnessError;

/// Launcher names probed on PATH (Windows-aware: `.cmd`/`.exe` shims).
pub const DISCOVERY_LAUNCHERS: &[&str] = &["dsh", "dsh.cmd", "dsh.exe"];

pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// Hard ceiling: a handshake that never completes would wedge startup (T-055).
pub const MAX_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_FRAME_CAP_BYTES: usize = 1024 * 1024;
pub const DEFAULT_DIAGNOSTICS_CAP_BYTES: usize = 256 * 1024;
pub const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(5);
pub const DEFAULT_STOP_FORCE: Duration = Duration::from_secs(3);
pub const DEFAULT_PROTOCOL_CHANNEL_MESSAGES: usize = 256;
/// Bounded lifetime of one `session/prompt` request (TASK 21 §26). The prompt
/// is a long-lived request (its lifetime is the run's lifetime); the timeout
/// is a fail-safe so an agent that stops responding cannot leave a run
/// eternally active. A timeout produces an honest failed/outcome-unknown
/// terminal (§127), never a fake cancel.
pub const DEFAULT_PROMPT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Hard ceiling: a prompt request must never be unbounded (T-055).
pub const MAX_PROMPT_TIMEOUT: Duration = Duration::from_secs(3600);
/// Bound on a single prompt's size before IPC serialization (TASK 21 §20).
pub const DEFAULT_MAX_PROMPT_BYTES: usize = 1024 * 1024;

/// Explicit, trusted, SAIWORK2-owned configuration (TASK 19/20). Never built
/// from project files; never model-controlled.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    /// Explicit executable (absolute path or PATH name). `None` = discover
    /// `dsh` on PATH at start.
    pub executable: Option<PathBuf>,
    /// Fixed machine-mode arguments (separate OS arguments, never a shell
    /// string). Default empty — the TASK 21 probe validates the concrete ACP
    /// composition entry for the installed runtime.
    pub args: Vec<String>,
    /// Runtime working directory (explicit validated path; falls back to the
    /// engine workspace context).
    pub cwd: Option<PathBuf>,
    /// Bounded handshake deadline.
    pub handshake_timeout: Duration,
    /// Bounded lifetime of one `session/prompt` request (fail-safe, §26).
    pub prompt_timeout: Duration,
    /// Local guard against pathological prompt sizes (TASK 21 §20).
    pub max_prompt_bytes: usize,
    /// Per-frame protocol cap (both directions).
    pub frame_cap_bytes: usize,
    /// Bounded raw protocol channel (messages).
    pub protocol_channel_messages: usize,
    /// Bounded diagnostics capture (stderr + stdout ring).
    pub diagnostics_cap_bytes: usize,
    /// Explicit machine-protocol diagnostic mode: additionally keep a bounded
    /// lossy stdout line ring alongside the raw protocol stream. Default
    /// `false` — protocol stdout is forwarded raw only and never pays a
    /// second text-processing path (TASK 24 perf). stderr diagnostics are
    /// always captured regardless of this flag.
    pub protocol_stdout_diagnostics: bool,
    pub stop_grace: Duration,
    pub stop_force: Duration,
    /// Environment overrides/additions (never SAIWORK2 secrets).
    pub env: Vec<(String, String)>,
    /// Environment variables removed from the inherited parent env.
    pub env_remove: Vec<String>,
    /// Cheap pre-launch `--version` probe (TASK 20 §12). The authoritative
    /// identity/version evidence is the ACP handshake itself.
    pub preflight_probe: bool,
    pub label: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            executable: None,
            args: Vec::new(),
            cwd: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            prompt_timeout: DEFAULT_PROMPT_TIMEOUT,
            max_prompt_bytes: DEFAULT_MAX_PROMPT_BYTES,
            frame_cap_bytes: DEFAULT_FRAME_CAP_BYTES,
            protocol_channel_messages: DEFAULT_PROTOCOL_CHANNEL_MESSAGES,
            diagnostics_cap_bytes: DEFAULT_DIAGNOSTICS_CAP_BYTES,
            protocol_stdout_diagnostics: false,
            stop_grace: DEFAULT_STOP_GRACE,
            stop_force: DEFAULT_STOP_FORCE,
            env: Vec::new(),
            env_remove: Vec::new(),
            preflight_probe: true,
            label: "DeepSeek Harness".into(),
        }
    }
}

impl HarnessConfig {
    /// Read explicit configuration from SAIWORK2-owned env vars. `None` = not
    /// configured (the engine is simply not registered — detection ≠
    /// installation, TASK 17 §58). `Some(Err)` = malformed value, surfaced
    /// precisely.
    ///
    /// - `SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE` (required to register)
    /// - `SAIWORK2_DEEPSEEK_HARNESS_ARGS` (optional, space-separated fixed args)
    /// - `SAIWORK2_DEEPSEEK_HARNESS_CWD` (optional)
    /// - `SAIWORK2_DEEPSEEK_HARNESS_LABEL` (optional, default "DeepSeek Harness")
    /// - `SAIWORK2_DEEPSEEK_HARNESS_HANDSHAKE_TIMEOUT_MS` (optional)
    pub fn from_env() -> Option<Result<Self, String>> {
        let executable = std::env::var("SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE").ok()?;
        let executable = executable.trim().to_string();
        if executable.is_empty() {
            return Some(Err("SAIWORK2_DEEPSEEK_HARNESS_EXECUTABLE is empty".into()));
        }
        let args: Vec<String> = std::env::var("SAIWORK2_DEEPSEEK_HARNESS_ARGS")
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default();
        let cwd = std::env::var("SAIWORK2_DEEPSEEK_HARNESS_CWD")
            .ok()
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty());
        let label = std::env::var("SAIWORK2_DEEPSEEK_HARNESS_LABEL")
            .unwrap_or_else(|_| "DeepSeek Harness".into());
        let handshake_ms = match std::env::var("SAIWORK2_DEEPSEEK_HARNESS_HANDSHAKE_TIMEOUT_MS") {
            Ok(s) => match s.parse::<u64>() {
                Ok(v) if v > 0 => v,
                Ok(_) => return Some(Err("handshake timeout must be positive".into())),
                Err(_) => {
                    return Some(Err(format!(
                        "SAIWORK2_DEEPSEEK_HARNESS_HANDSHAKE_TIMEOUT_MS is not a number: {s}"
                    )))
                }
            },
            Err(_) => DEFAULT_HANDSHAKE_TIMEOUT.as_millis() as u64,
        };
        if handshake_ms > MAX_HANDSHAKE_TIMEOUT.as_millis() as u64 {
            return Some(Err(format!(
                "SAIWORK2_DEEPSEEK_HARNESS_HANDSHAKE_TIMEOUT_MS exceeds the hard maximum of {}",
                MAX_HANDSHAKE_TIMEOUT.as_millis()
            )));
        }
        let prompt_ms = match std::env::var("SAIWORK2_DEEPSEEK_HARNESS_PROMPT_TIMEOUT_MS") {
            Ok(s) => match s.parse::<u64>() {
                Ok(v) if v > 0 => v,
                Ok(_) => return Some(Err("prompt timeout must be positive".into())),
                Err(_) => {
                    return Some(Err(format!(
                        "SAIWORK2_DEEPSEEK_HARNESS_PROMPT_TIMEOUT_MS is not a number: {s}"
                    )))
                }
            },
            Err(_) => DEFAULT_PROMPT_TIMEOUT.as_millis() as u64,
        };
        if prompt_ms > MAX_PROMPT_TIMEOUT.as_millis() as u64 {
            return Some(Err(format!(
                "SAIWORK2_DEEPSEEK_HARNESS_PROMPT_TIMEOUT_MS exceeds the hard maximum of {}",
                MAX_PROMPT_TIMEOUT.as_millis()
            )));
        }
        let cfg = Self {
            executable: Some(PathBuf::from(executable)),
            args,
            cwd,
            handshake_timeout: Duration::from_millis(handshake_ms),
            prompt_timeout: Duration::from_millis(prompt_ms),
            label,
            ..Self::default()
        };
        if let Err(e) = cfg.resolve_executable() {
            return Some(Err(e.to_string()));
        }
        Some(Ok(cfg))
    }

    /// Resolve the executable: explicit path is authoritative (invalid →
    /// error, never a silent PATH fallback, §10); otherwise PATH lookup of
    /// the known launchers (§9). Detection only — nothing is executed here.
    pub fn resolve_executable(&self) -> Result<PathBuf, HarnessError> {
        if let Some(explicit) = &self.executable {
            if explicit.is_absolute() {
                if explicit.is_file() {
                    return Ok(explicit.clone());
                }
                return Err(HarnessError::ConfigurationInvalid(format!(
                    "executable not found: {}",
                    explicit.to_string_lossy()
                )));
            }
            // Absolute-looking Windows path given as a bare name.
            if let Ok(candidate) = resolve_in_path(explicit) {
                return Ok(candidate);
            }
            return Err(HarnessError::HarnessNotFound);
        }
        for launcher in DISCOVERY_LAUNCHERS {
            if let Ok(candidate) = resolve_in_path(&PathBuf::from(launcher)) {
                return Ok(candidate);
            }
        }
        Err(HarnessError::HarnessNotFound)
    }

    /// Cheap pre-launch probe (TASK 20 §12): run `<exe> --version` through
    /// the ProcessSupervisor (law 6), bounded; exit 0 = runnable launcher.
    /// The npm `dsh` CLI prints nothing on `--version` (probe evidence), so
    /// empty output is tolerated; the authoritative identity + version
    /// evidence is the ACP `initialize` handshake.
    pub async fn probe(
        &self,
        supervisor: &ProcessSupervisor,
        executable: &std::path::Path,
        generation: u64,
    ) -> Result<(), HarnessError> {
        if !self.preflight_probe {
            return Ok(());
        }
        let mut spec = ProcessSpec::new(
            format!("dsh-probe-{generation}"),
            executable.to_string_lossy().into_owned(),
        );
        spec.args = vec!["--version".into()];
        spec.cwd = self.cwd.clone();
        spec.output_cap_bytes = Some(64 * 1024);
        spec.exit_wait_timeout = Duration::from_secs(1);
        spec.kill_timeout = Duration::from_secs(1);
        let process = supervisor
            .spawn(spec)
            .await
            .map_err(|e| HarnessError::ProbeFailed(format!("spawn: {e}")))?;
        let outcome = tokio::time::timeout(Duration::from_secs(3), wait_for_exit(&process)).await;
        match outcome {
            Ok(info) if info.success() => Ok(()),
            Ok(info) => Err(HarnessError::ProbeFailed(format!(
                "exited with code {:?}",
                info.code
            ))),
            Err(_) => {
                let _ = supervisor.stop(&process, true).await;
                Err(HarnessError::ProbeFailed("timed out".into()))
            }
        }
    }

    /// The runtime `ProcessSpec`: piped stdin (protocol), protocol-mode
    /// stdout, bounded stderr diagnostics, explicit cwd/env (TASK 20 §17).
    pub fn runtime_spec(
        &self,
        generation: u64,
        executable: &std::path::Path,
    ) -> ProcessSpec {
        let mut spec = ProcessSpec::new(
            format!("dsh-runtime-{generation}"),
            executable.to_string_lossy().into_owned(),
        );
        spec.args = self.args.clone();
        spec.cwd = self.cwd.clone();
        spec.stdin = StdinPolicy::Piped;
        spec.stdout_protocol = true;
        spec.protocol_stdout_diagnostics = self.protocol_stdout_diagnostics;
        spec.protocol_channel_messages = self.protocol_channel_messages;
        spec.output_cap_bytes = Some(self.diagnostics_cap_bytes);
        spec.env = self.env.clone();
        spec.env_remove = self.env_remove.clone();
        spec.exit_wait_timeout = self.stop_grace;
        spec.kill_timeout = self.stop_force;
        spec
    }
}

/// PATH resolution with Windows PATHEXT semantics (bare name → `.exe`/`.cmd`).
fn resolve_in_path(name: &std::path::Path) -> Result<PathBuf, ()> {
    let search = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&search) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
        #[cfg(windows)]
        {
            for ext in ["exe", "cmd", "bat", "ps1"] {
                let with_ext = candidate.with_extension(ext);
                if with_ext.is_file() {
                    return Ok(with_ext);
                }
            }
        }
    }
    Err(())
}

async fn wait_for_exit(process: &saiwork_process::ManagedProcess) -> saiwork_process::ExitInfo {
    let mut rx = process.exit();
    loop {
        if let Some(info) = *rx.borrow() {
            return info;
        }
        if rx.changed().await.is_err() {
            return saiwork_process::ExitInfo {
                code: None,
                signaled: true,
            };
        }
    }
}
