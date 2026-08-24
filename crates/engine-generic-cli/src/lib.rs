//! Generic CLI engine adapter (TASK 17 §43–§53) — the strongest safe
//! second-engine path. Freebuff is DEFERRED (remote-cloud-only, Node-only
//! SDK, credential-vault requirement); the generic CLI proves the engine
//! architecture is vendor-neutral with a second **production** adapter.
//!
//! Security model (TASK 17 §44–§47):
//! - The executable and its argument template are **explicit trusted
//!   configuration** from SAIWORK2-owned environment variables, never from
//!   project files and never model-controlled.
//! - No shell: the program is executed directly by `ProcessSupervisor` with
//!   args as separate OS arguments; the prompt is sent as **stdin bytes**
//!   (§46), never interpolated into a command string.
//! - `OneShotText` capability level (§48): one bounded process per send,
//!   bounded output, bounded execution time, cancellation = terminating the
//!   managed process tree (legitimate for a one-shot engine because run ==
//!   process, §52). No fake streaming, no tools, no permissions.
//!
//! Sessions: SAIWORK2 session metadata is SAIWORK2-owned; each send spawns a
//! fresh process (one-shot, no cross-run context). `resume = false` is
//! honest — there is no upstream session to reattach to (§30–§31, §53).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use saiwork_core::engine::{
    CreateSessionRequest, EngineAdapter, EngineCapabilities, EngineError, EngineHealth,
    EngineIdentity, EngineStartContext, ModelInfo, SendAcceptance, SendRequest, SessionCreation,
    SessionInfo,
};
use saiwork_events::{Event, EventBus};
use saiwork_process::{ManagedProcess, ProcessSpec, StdinPolicy};
use tracing::{info, warn};
use uuid::Uuid;

pub const ENGINE_ID: &str = "generic-cli";

/// Defaults for the one-shot response channel (§49: the answer is preserved
/// independently of the diagnostic buffer policy, still bounded).
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
/// Hard ceiling: a syntactically valid env value must never disable the memory
/// cap (T-055) — clamp/reject above this.
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard ceiling for a single one-shot run (T-055): above this the engine could
/// never be reclaimed.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(3600);
pub const DEFAULT_MAX_PROMPT_BYTES: usize = 64 * 1024;

/// Re-probe cadence after a termination could not be PROVEN: the run stays
/// active (workspace reserved) and is re-checked until the child actually
/// exits — no false terminal, no hot loop (TASK 24 §9).
pub const TERMINATION_PROBE_MS: Duration = Duration::from_millis(500);

/// Explicit trusted engine definition (TASK 17 §45). Constructed only from
/// SAIWORK2-owned configuration; there is no way for a project file or a
/// model to supply these fields.
#[derive(Debug, Clone)]
pub struct GenericCliConfig {
    /// Absolute path or PATH-resolved name. Never a shell string.
    pub executable: String,
    /// Fixed argument template, passed as separate OS arguments.
    pub args: Vec<String>,
    /// Display name shown in the engine selector.
    pub label: String,
    /// Bounded response channel (stdout preserved up to this cap).
    pub max_output_bytes: usize,
    /// Bounded execution lifetime; after this the run is terminated.
    pub timeout: Duration,
    /// Bounded prompt size (stdin bytes).
    pub max_prompt_bytes: usize,
}

impl GenericCliConfig {
    /// Read the explicit trusted configuration from the environment. `None`
    /// when the engine is not configured (it is simply not registered —
    /// detection ≠ installation, §58). `Some(Err)` on malformed values so the
    /// caller can surface a precise configuration error instead of silently
    /// dropping the engine.
    ///
    /// Env surface (documented in KNOWLEDGE/ENGINE_CONTRACT.md):
    /// - `SAIWORK2_CLI_EXECUTABLE` (required): path or PATH name.
    /// - `SAIWORK2_CLI_ARGS` (optional): space-separated fixed arguments.
    ///   Arguments containing spaces are out of scope for V1 (trusted,
    ///   user-authored config; keep them simple).
    /// - `SAIWORK2_CLI_LABEL` (optional, default "Generic CLI").
    /// - `SAIWORK2_CLI_MAX_OUTPUT_BYTES` (optional, default 1 MiB).
    /// - `SAIWORK2_CLI_TIMEOUT_MS` (optional, default 60000).
    pub fn from_env() -> Option<Result<Self, String>> {
        let executable = std::env::var("SAIWORK2_CLI_EXECUTABLE").ok()?;
        let executable = executable.trim().to_string();
        if executable.is_empty() {
            return Some(Err("SAIWORK2_CLI_EXECUTABLE is empty".into()));
        }
        let args: Vec<String> = std::env::var("SAIWORK2_CLI_ARGS")
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default();
        let label = std::env::var("SAIWORK2_CLI_LABEL").unwrap_or_else(|_| "Generic CLI".into());
        let max_output_bytes = match std::env::var("SAIWORK2_CLI_MAX_OUTPUT_BYTES") {
            Ok(s) => match s.parse::<usize>() {
                Ok(v) if v >= 1024 => v,
                Ok(_) => {
                    return Some(Err(
                        "SAIWORK2_CLI_MAX_OUTPUT_BYTES must be at least 1024".into(),
                    ))
                }
                Err(_) => {
                    return Some(Err(format!(
                        "SAIWORK2_CLI_MAX_OUTPUT_BYTES is not a number: {s}"
                    )))
                }
            },
            Err(_) => DEFAULT_MAX_OUTPUT_BYTES,
        };
        if max_output_bytes > MAX_OUTPUT_BYTES {
            return Some(Err(format!(
                "SAIWORK2_CLI_MAX_OUTPUT_BYTES exceeds the hard maximum of {MAX_OUTPUT_BYTES}"
            )));
        }
        let timeout_ms = match std::env::var("SAIWORK2_CLI_TIMEOUT_MS") {
            Ok(s) => match s.parse::<u64>() {
                Ok(v) if v >= 1000 => v,
                Ok(_) => {
                    return Some(Err("SAIWORK2_CLI_TIMEOUT_MS must be at least 1000".into()))
                }
                Err(_) => {
                    return Some(Err(format!("SAIWORK2_CLI_TIMEOUT_MS is not a number: {s}")))
                }
            },
            Err(_) => DEFAULT_TIMEOUT.as_millis() as u64,
        };
        if timeout_ms > MAX_TIMEOUT.as_millis() as u64 {
            return Some(Err(format!(
                "SAIWORK2_CLI_TIMEOUT_MS exceeds the hard maximum of {}",
                MAX_TIMEOUT.as_millis()
            )));
        }
        Some(Ok(Self {
            executable,
            args,
            label,
            max_output_bytes,
            timeout: Duration::from_millis(timeout_ms),
            max_prompt_bytes: DEFAULT_MAX_PROMPT_BYTES,
        }))
    }

    /// Validate the executable: absolute path must exist; bare names are
    /// resolved against PATH (detection only — nothing is executed here).
    pub fn validate_executable(&self) -> Result<(), String> {
        let path = PathBuf::from(&self.executable);
        if path.is_absolute() {
            if !path.is_file() {
                return Err(format!("executable not found: {}", path.to_string_lossy()));
            }
            return Ok(());
        }
        let search = std::env::var_os("PATH").unwrap_or_default();
        for dir in std::env::split_paths(&search) {
            let candidate = dir.join(&self.executable);
            if candidate.is_file() {
                return Ok(());
            }
            // Windows: bare names resolve with an extension (python →
            // python.exe) exactly like the OS process spawner does.
            #[cfg(windows)]
            if candidate.with_extension("exe").is_file() {
                return Ok(());
            }
        }
        Err(format!("executable not found in PATH: {}", self.executable))
    }
}

struct ActiveRun {
    session_id: String,
    process: Arc<ManagedProcess>,
    cancelled: AtomicBool,
}

/// One-shot CLI engine. Lifecycle: `Unknown → Starting → Ready` after a
/// successful executable probe; `stop` returns to `Stopped`. No process is
/// owned at the engine level — each send spawns its own bounded supervised
/// process and the run owns it (run == process, §52). Engine readiness
/// governs *new* sends; an active run is stopped by `cancel` (or app
/// shutdown through the ProcessSupervisor sweep), never by engine stop —
/// matching "process state separate from engine readiness" (§26).
pub struct GenericCliEngine {
    config: GenericCliConfig,
    health: RwLock<EngineHealth>,
    bus: Arc<RwLock<Option<EventBus>>>,
    supervisor: Arc<RwLock<Option<Arc<saiwork_process::ProcessSupervisor>>>>,
    workspace: RwLock<Option<PathBuf>>,
    runs: Arc<RwLock<HashMap<String, Arc<ActiveRun>>>>,
}

impl GenericCliEngine {
    pub fn new(config: GenericCliConfig) -> Self {
        Self {
            config,
            health: RwLock::new(EngineHealth::Unknown),
            bus: Arc::new(RwLock::new(None)),
            supervisor: Arc::new(RwLock::new(None)),
            workspace: RwLock::new(None),
            runs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn identity_static(&self) -> EngineIdentity {
        EngineIdentity {
            id: ENGINE_ID.into(),
            display_name: self.config.label.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            experimental: false,
        }
    }

    fn set_health(&self, health: EngineHealth) {
        *self.health.write().expect("cli health mutex poisoned") = health;
    }
}

#[async_trait]
impl EngineAdapter for GenericCliEngine {
    fn identity(&self) -> EngineIdentity {
        self.identity_static()
    }

    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            // SAIWORK2-owned sessions work (create → send → terminal); each
            // send is one fresh process.
            sessions: true,
            // No upstream session exists to reattach to — honest.
            resume: false,
            // One-shot: output arrives only at process exit (real output,
            // not fake token deltas — §34).
            streaming: false,
            // Run == process: cancel terminates the managed process (§52).
            cancel: true,
            tools: false,
            permissions: false,
            attachments: false,
            images: false,
            models: false,
            usage: false,
            reasoning: false,
            context_window: None,
            worktrees: false,
            parallel_sessions: false,
            session_revert: false,
            structured_events: false,
        }
    }

    async fn start(&self, ctx: &EngineStartContext) -> Result<(), EngineError> {
        if let Err(message) = self.config.validate_executable() {
            self.set_health(EngineHealth::Failed {
                message: message.clone(),
            });
            ctx.diagnostics
                .record_error("ENGINE_CONFIG", format!("{ENGINE_ID}: {message}"));
            return Err(EngineError::engine(ENGINE_ID, message));
        }
        self.set_health(EngineHealth::Starting);
        // No process is spawned: readiness is a configuration probe, not a
        // sleep-and-pretend (TASK 17 §28). The probe is cheap and explicit.
        *self.bus.write().expect("cli bus mutex poisoned") = Some(ctx.bus.clone());
        *self
            .supervisor
            .write()
            .expect("cli supervisor mutex poisoned") = Some(ctx.supervisor.clone());
        *self
            .workspace
            .write()
            .expect("cli workspace mutex poisoned") = ctx.workspace_path.clone();
        self.set_health(EngineHealth::Ready);
        info!(engine = ENGINE_ID, "generic cli engine ready");
        Ok(())
    }

    async fn stop(&self) -> Result<(), EngineError> {
        self.set_health(EngineHealth::Stopped);
        info!(engine = ENGINE_ID, "generic cli engine stopped");
        Ok(())
    }

    async fn kill(&self) -> Result<(), EngineError> {
        self.stop().await
    }

    fn health(&self) -> EngineHealth {
        self.health
            .read()
            .expect("cli health mutex poisoned")
            .clone()
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: ENGINE_ID.into(),
            capability: "models",
        })
    }

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, EngineError> {
        // No upstream session store: SAIWORK2's own SessionManager persists
        // our metadata; there is nothing engine-owned to list.
        Err(EngineError::UnsupportedCapability {
            engine_id: ENGINE_ID.into(),
            capability: "sessions",
        })
    }

    async fn create_session(
        &self,
        req: &CreateSessionRequest,
    ) -> Result<SessionCreation, EngineError> {
        // One-shot engine: no upstream session exists; the generic id is
        // echoed verbatim and doubles as the engine session id (TASK 24 §9).
        // Creation is synchronous metadata — always authoritative.
        Ok(SessionCreation::Created {
            engine_session_id: req.session_id.clone(),
            display_name: self.config.label.clone(),
        })
    }

    async fn resume_session(&self, _engine_session_id: &str) -> Result<SessionInfo, EngineError> {
        Err(EngineError::UnsupportedCapability {
            engine_id: ENGINE_ID.into(),
            capability: "resume",
        })
    }

    async fn delete_session(&self, _engine_session_id: &str) -> Result<(), EngineError> {
        // Nothing upstream to delete; SAIWORK2 metadata removal is the
        // SessionManager's own responsibility.
        Ok(())
    }

    /// Authoritative liveness for the core's lag-reconciliation (TASK 24
    /// §9): the CLI child IS the run, so an active run in `runs` must be
    /// reported — otherwise a Lagged EventBus would clear the workspace
    /// reservation while the child may still be mutating it.
    fn active_runs(&self) -> Vec<saiwork_core::engine::ActiveRun> {
        self.runs
            .read()
            .expect("cli runs mutex poisoned")
            .iter()
            .map(|(run_id, run)| saiwork_core::engine::ActiveRun {
                session_id: run.session_id.clone(),
                run_id: run_id.clone(),
            })
            .collect()
    }

    async fn send(&self, req: &SendRequest) -> Result<SendAcceptance, EngineError> {
        if !matches!(self.health(), EngineHealth::Ready) {
            return Err(EngineError::NotReady {
                engine_id: ENGINE_ID.into(),
            });
        }
        if req.prompt.len() > self.config.max_prompt_bytes {
            return Err(EngineError::engine(
                ENGINE_ID,
                format!(
                    "prompt exceeds the {} byte cap",
                    self.config.max_prompt_bytes
                ),
            ));
        }
        // Same-session concurrency: REJECT (ENGINE_CONTRACT.md §70–§72). Two
        // simultaneous agent turns in one thread are logically nonsensical.
        {
            let runs = self.runs.read().expect("cli runs mutex poisoned");
            if runs.values().any(|r| r.session_id == req.session_id) {
                return Err(EngineError::SessionBusy {
                    session_id: req.session_id.clone(),
                });
            }
        }
        let run_id = Uuid::new_v4().to_string();
        let ctx_workspace = self
            .workspace
            .read()
            .expect("cli workspace mutex poisoned")
            .clone()
            .ok_or_else(|| EngineError::engine(ENGINE_ID, "no workspace context"))?;
        let supervisor = self
            .supervisor
            .read()
            .expect("cli supervisor mutex poisoned")
            .clone()
            .ok_or_else(|| EngineError::engine(ENGINE_ID, "engine not started"))?;

        let mut spec = ProcessSpec::new(run_id.clone(), self.config.executable.clone());
        spec.args = self.config.args.clone();
        spec.cwd = Some(ctx_workspace);
        // Piped stdin, NOT the detached Bytes writer: acceptance must be
        // proven by the exact prompt bytes landing in the child BEFORE we
        // report Accepted — a child that closes stdin or exits early must
        // never be reported as accepted while the prompt never arrived
        // (TASK 24 §9).
        spec.stdin = StdinPolicy::Piped;
        spec.output_cap_bytes = Some(self.config.max_output_bytes);
        let process = supervisor
            .spawn(spec)
            .await
            .map_err(|e| EngineError::engine(ENGINE_ID, format!("spawn failed: {e}")))?;

        // Synchronous prompt delivery: write the exact bytes, then close
        // stdin (EOF) so the one-shot CLI sees a complete input. Only a
        // fully delivered prompt may become `Accepted`.
        let delivery = async {
            process
                .stdin_write_all(req.prompt.as_bytes())
                .await
                .map_err(|e| format!("prompt write failed: {e}"))?;
            process.stdin_close().await;
            Ok::<(), String>(())
        }
        .await;
        if let Err(message) = delivery {
            // The process was created and may already be executing: absence
            // of side effects is NOT provable — this is OutcomeUnknown, and
            // the child is terminated so the run cannot dangle. Never
            // Accepted, never DefinitelyRejected.
            warn!(run = %run_id, error = %message, "cli prompt delivery failed; terminating process");
            let _ = supervisor.stop(&process, true).await;
            let _ = supervisor.stop(&process, false).await;
            return Ok(SendAcceptance::OutcomeUnknown {
                run_id: run_id.clone(),
                message,
            });
        }

        let active = Arc::new(ActiveRun {
            session_id: req.session_id.clone(),
            process: process.clone(),
            cancelled: AtomicBool::new(false),
        });
        self.runs
            .write()
            .expect("cli runs mutex poisoned")
            .insert(run_id.clone(), active.clone());

        let bus = self
            .bus
            .read()
            .expect("cli bus mutex poisoned")
            .clone()
            .ok_or_else(|| EngineError::engine(ENGINE_ID, "engine not started"))?;
        let session_id = req.session_id.clone();
        let run_scope = run_id.clone();
        let timeout = self.config.timeout;
        let runs = self.runs.clone();
        // The CLI process is the upstream: a successful spawn + exact prompt
        // delivery IS the authoritative acceptance.
        let acceptance = SendAcceptance::Accepted {
            run_id: run_id.clone(),
        };
        tokio::spawn(async move {
            bus.publish(Event::MessageStarted {
                session_id: session_id.clone().into(),
                run_id: run_scope.clone().into(),
            });

            // A terminal (and the run-removal that releases the workspace)
            // is emitted ONLY when the process EXIT is proven. Termination
            // is request semantics: if the child ignores graceful/force
            // stop, NO false terminal is emitted and the run stays active —
            // a possibly-live mutating process must never release the
            // workspace (TASK 24 §9). When the child eventually exits,
            // exactly one terminal is emitted and the run is removed.
            let mut unproven = false;
            let mut forced = false;
            loop {
                let wait_budget = if unproven {
                    TERMINATION_PROBE_MS
                } else {
                    timeout
                };
                let outcome = tokio::time::timeout(wait_budget, wait_for_exit(&process)).await;
                let cancelled = active.cancelled.load(Ordering::SeqCst);
                match outcome {
                    Ok(info) => {
                        // EXIT PROVEN: exactly one terminal.
                        if cancelled {
                            // Cancel wins ties: terminal is Cancelled even if
                            // the process happened to exit naturally at the
                            // same moment.
                            bus.publish(Event::MessageCancelled {
                                session_id: session_id.clone().into(),
                                run_id: run_scope.clone().into(),
                            });
                        } else if forced {
                            // The bounded lifetime elapsed and termination
                            // took: the terminal is the TIMEOUT, never a raw
                            // kill code — but only after exit is proven.
                            bus.publish(Event::MessageFailed {
                                session_id: session_id.clone().into(),
                                run_id: run_scope.clone().into(),
                                error: format!("timed out after {}s", timeout.as_secs()),
                            });
                        } else if info.success() {
                            // One-shot non-streaming result: the full bounded
                            // stdout is the answer (§49: preserved even if
                            // the diagnostics buffer would have dropped it).
                            let mut out = process.stdout().join("\n");
                            if process.dropped_lines() > 0 {
                                out.push_str(&format!(
                                    "\n\n[output truncated: {} line(s) dropped]",
                                    process.dropped_lines()
                                ));
                            }
                            bus.publish(Event::MessageDelta {
                                session_id: session_id.clone().into(),
                                run_id: run_scope.clone().into(),
                                delta: out,
                            });
                            bus.publish(Event::MessageCompleted {
                                session_id: session_id.clone().into(),
                                run_id: run_scope.clone().into(),
                            });
                        } else {
                            let stderr_tail = process.stderr().join("\n");
                            let error = if stderr_tail.is_empty() {
                                format!("process exited with code {:?}", info.code)
                            } else {
                                format!(
                                    "process exited with code {:?}: {}",
                                    info.code, stderr_tail
                                )
                            };
                            bus.publish(Event::MessageFailed {
                                session_id: session_id.clone().into(),
                                run_id: run_scope.clone().into(),
                                error,
                            });
                        }
                        break;
                    }
                    Err(_) => {
                        if !unproven {
                            // Bounded lifetime elapsed (§50) or a cancel
                            // request did not terminate the child in time:
                            // attempt graceful → force, then PROVE the exit.
                            forced = !cancelled;
                            let graceful_ok =
                                supervisor.stop(&process, true).await.is_ok();
                            let stopped = graceful_ok
                                || supervisor.stop(&process, false).await.is_ok();
                            if stopped
                                && tokio::time::timeout(
                                    TERMINATION_PROBE_MS,
                                    wait_for_exit(&process),
                                )
                                .await
                                .is_ok()
                            {
                                continue; // exit proven → next iteration terminalizes
                            }
                            // Termination NOT proven: degrade, keep the run
                            // active (workspace stays reserved), keep probing
                            // until the child actually exits.
                            unproven = true;
                            if cancelled {
                                warn!(run = %run_scope, "cli cancel: process did not terminate; workspace stays reserved until exit");
                                bus.publish(Event::RuntimeWarning {
                                    code: "GENERIC_CLI_CANCEL_UNPROVEN".into(),
                                    message: "cancel requested but the CLI process did not terminate; the run stays active (workspace reserved) until it exits".into(),
                                });
                            } else {
                                warn!(run = %run_scope, "cli timeout: process did not terminate; workspace stays reserved until exit");
                                bus.publish(Event::RuntimeWarning {
                                    code: "GENERIC_CLI_TIMEOUT_UNPROVEN".into(),
                                    message: "run timed out but the CLI process did not terminate; the run stays active (workspace reserved) until it exits".into(),
                                });
                            }
                        }
                        tokio::time::sleep(TERMINATION_PROBE_MS).await;
                    }
                }
            }
            runs.write()
                .expect("cli runs mutex poisoned")
                .remove(&run_scope);
        });

        Ok(acceptance)
    }

    async fn cancel(&self, run_id: &str) -> Result<(), EngineError> {
        let run = {
            let runs = self.runs.read().expect("cli runs mutex poisoned");
            runs.get(run_id).cloned()
        };
        let Some(run) = run else {
            // Idempotent: cancelling a finished/unknown run is a no-op
            // (cancel-twice rule, EVENTS.md).
            return Ok(());
        };
        run.cancelled.store(true, Ordering::SeqCst);
        let Some(supervisor) = self
            .supervisor
            .read()
            .expect("cli supervisor mutex poisoned")
            .clone()
        else {
            // Engine was never started; nothing to stop.
            return Ok(());
        };
        // Cancellation is REQUEST semantics (TASK 24 §9): it becomes a real
        // Cancelled terminal only when the process EXIT is proven by the run
        // task. If neither graceful nor force termination takes, the caller
        // learns the request was NOT honored — the run stays active and the
        // workspace stays reserved until the child actually exits.
        let process = run.process.clone();
        if supervisor.stop(&process, true).await.is_err()
            && supervisor.stop(&process, false).await.is_err()
        {
            return Err(EngineError::engine(
                ENGINE_ID,
                format!(
                    "cancel requested for run {run_id} but the CLI process did not terminate; the run remains active until it exits"
                ),
            ));
        }
        Ok(())
    }
}

/// Await process exit through the supervisor's exit watch (bounded drain of
/// tail output already happens before the signal is delivered).
async fn wait_for_exit(process: &ManagedProcess) -> saiwork_process::ExitInfo {
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
