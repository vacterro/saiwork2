//! ProcessSupervisor — the single owner of SAIWORK2 child processes (law 6).
//!
//! Contract (PROCESS_LIFECYCLE.md, TASK 06):
//! - the OS process state machine is **separate** from engine lifecycle:
//!   `SPAWNING → RUNNING → STOPPING → EXITED` (or `FAILED`); `RUNNING` only
//!   means "the OS process exists". Readiness / `engine.ready` is a higher
//!   layer's concern (engine adapters, TASK 07+) and never lives here;
//! - every child is registered here before it can run (assigned to its
//!   process-tree ownership primitive on Windows: a Job Object);
//! - `ProcessId` (application identity) is distinct from the OS PID, which
//!   may be reused;
//! - graceful stop → bounded wait → force kill (whole tree) → bounded wait;
//! - stdout and stderr are captured **separately**, each into a bounded ring
//!   (law 13); readers are lossy-UTF-8 and never panic;
//! - terminal lifecycle events (`process.exited`/`process.failed`) are
//!   published **after** the OS exit is known, output readers have drained
//!   within a bounded window, and the record is updated;
//! - after `shutdown()` the supervisor owns zero processes (0-orphan M0 gate).

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saiwork_events::{Event, EventBus, ProcessId};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::{sleep, timeout as timeout_after};
use tracing::{debug, info, warn};

use crate::error::ProcessError;
use crate::output::BoundedOutputBuffer;
use crate::platform;

/// How long the monitor waits for output readers to drain after process exit
/// before publishing the terminal event (bounded tail capture, §32).
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// OS process state machine (separate from engine lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[repr(u8)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProcessState {
    Spawning = 0,
    Running = 1,
    Stopping = 2,
    Exited = 3,
    Failed = 4,
}

impl ProcessState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => ProcessState::Spawning,
            1 => ProcessState::Running,
            2 => ProcessState::Stopping,
            3 => ProcessState::Exited,
            _ => ProcessState::Failed,
        }
    }

    /// Legal transitions only (§74): terminal states never leave, and a
    /// stopped/exited process is never resurrected.
    fn allows(self, to: Self) -> bool {
        use ProcessState::*;
        matches!(
            (self, to),
            (Spawning, Running)
                | (Spawning, Failed)
                | (Running, Stopping)
                | (Running, Exited)
                | (Running, Failed)
                | (Stopping, Exited)
                | (Stopping, Failed)
        )
    }
}

/// What to feed the child on stdin. Interactive terminals are out of scope
/// for TASK 06; `Null` is the safe default for supervised engines.
#[derive(Clone, PartialEq, Eq)]
pub enum StdinPolicy {
    Null,
    Inherit,
    /// Write these exact bytes to the child's stdin, then close it (EOF).
    /// Bounded by the caller (generic CLI engine caps prompt size); used to
    /// feed a prompt to a one-shot CLI process as stdin bytes — never via a
    /// shell string (TASK 17 §46).
    Bytes(Vec<u8>),
    /// Keep the child's stdin open for a long-lived interactive machine
    /// protocol (e.g. a stdio NDJSON JSON-RPC engine child, TASK 20). Writes
    /// go through `ManagedProcess::stdin_write_all` (serialized, one writer
    /// owner); `stdin_close` sends EOF. The supervisor stays the sole spawn
    /// authority; it never sees protocol payloads.
    Piped,
}

impl std::fmt::Debug for StdinPolicy {
    /// Bytes content is a prompt — never printed (prompt redaction law).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StdinPolicy::Null => f.write_str("Null"),
            StdinPolicy::Inherit => f.write_str("Inherit"),
            StdinPolicy::Bytes(b) => write!(f, "Bytes({} bytes)", b.len()),
            StdinPolicy::Piped => f.write_str("Piped"),
        }
    }
}

/// Typed launch specification (§7–§11). Shell is never used: the program is
/// executed directly with args passed as separate OS arguments.
#[derive(Clone)]
pub struct ProcessSpec {
    /// Application identity of this child (unique in the supervisor).
    pub id: ProcessId,
    /// Executable: absolute path or PATH-resolved name. Never a shell string.
    pub command: String,
    /// Separate OS arguments (never one quoted command string).
    pub args: Vec<String>,
    /// Windows-only: arguments appended **verbatim** (no quoting), after
    /// `args`. Needed for the encapsulated `cmd.exe /D /S /C <raw line>`
    /// launch of `.cmd`/`.bat` shims whose path contains spaces (TASK 10 §8,
    /// §54): the raw `/C` argument must keep its own quoting, which a
    /// list-arg cannot express. Unix: unused and empty.
    #[cfg(windows)]
    pub raw_args: Vec<String>,
    /// Working directory; must exist before spawn (explicit, validated).
    pub cwd: Option<PathBuf>,
    /// Environment additions/overrides on top of the inherited parent env.
    pub env: Vec<(String, String)>,
    /// Environment variable names to remove from the inherited env.
    pub env_remove: Vec<String>,
    pub stdin: StdinPolicy,
    /// Arg positions whose values are secrets; shown as `***` in any
    /// diagnostic rendering of the command line (§41).
    pub redact_args: Vec<usize>,
    /// Per-process output cap in bytes (overrides `OUTPUT_CAP_BYTES`).
    /// `None` = crate default. Lets an engine preserve a bounded *response*
    /// channel (e.g. a CLI answer) independently of the diagnostic buffer
    /// policy (TASK 17 §49).
    pub output_cap_bytes: Option<usize>,
    /// True when stdout carries a machine protocol (e.g. NDJSON JSON-RPC,
    /// TASK 20 §21): the reader task forwards raw byte chunks to a bounded
    /// channel (`ManagedProcess::protocol_stream`). `false` = the default
    /// line-ring capture only.
    pub stdout_protocol: bool,
    /// Protocol-mode stdout diagnostics: when `stdout_protocol` is true and
    /// this is `false` (default), the raw chunks are forwarded verbatim and
    /// the lossy UTF-8 line-ring path is SKIPPED — machine protocol traffic
    /// is not paid a second text-processing path (TASK 24 perf). Set `true`
    /// only for an explicit diagnostic mode that wants human-readable stdout
    /// lines alongside the raw protocol stream. Never affects stderr (always
    /// captured as lines).
    pub protocol_stdout_diagnostics: bool,
    /// Bounded channel capacity (messages) for the raw protocol stream.
    /// Backpressure is real: a slow consumer stalls the child's stdout
    /// instead of unbounded buffering (TASK 20 §75/§103).
    pub protocol_channel_messages: usize,
    /// Bounded wait for a graceful termination signal to take effect.
    pub exit_wait_timeout: Duration,
    /// Bounded wait for force kill to take effect.
    pub kill_timeout: Duration,
}

impl std::fmt::Debug for ProcessSpec {
    /// Debug shows the command line with secret arg positions redacted and
    /// environment **names only** — never values.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessSpec")
            .field("id", &self.id)
            .field("command", &self.redacted_command_line())
            .field("cwd", &self.cwd)
            .field(
                "env",
                &self.env.iter().map(|(k, _)| (k, "***")).collect::<Vec<_>>(),
            )
            .field("env_remove", &self.env_remove)
            .field("stdin", &self.stdin)
            .field("exit_wait_timeout", &self.exit_wait_timeout)
            .field("kill_timeout", &self.kill_timeout)
            .finish_non_exhaustive()
    }
}

impl ProcessSpec {
    pub fn new(id: impl Into<ProcessId>, command: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            command: command.into(),
            args: Vec::new(),
            #[cfg(windows)]
            raw_args: Vec::new(),
            cwd: None,
            env: Vec::new(),
            env_remove: Vec::new(),
            stdin: StdinPolicy::Null,
            redact_args: Vec::new(),
            output_cap_bytes: None,
            stdout_protocol: false,
            protocol_stdout_diagnostics: false,
            protocol_channel_messages: 256,
            exit_wait_timeout: Duration::from_secs(5),
            kill_timeout: Duration::from_secs(3),
        }
    }

    /// Command line for diagnostics, with `redact_args` positions masked.
    pub fn redacted_command_line(&self) -> String {
        let mut parts = vec![self.command.clone()];
        for (i, arg) in self.args.iter().enumerate() {
            if self.redact_args.contains(&i) {
                parts.push("***".into());
            } else {
                parts.push(arg.clone());
            }
        }
        #[cfg(windows)]
        parts.extend(self.raw_args.iter().cloned());
        parts.join(" ")
    }
}

/// Test-only spawn failpoints (shutdown-race barrier tests). Feature-gated:
/// not reachable in production builds.
#[cfg(feature = "failpoints")]
#[derive(Default, Clone)]
pub struct SpawnHooks {
    /// Fires after the child exists (job assigned + resumed) but BEFORE the
    /// shutdown re-check and registration — the admission→registration
    /// window that shutdown's drain wait must cover.
    pub before_register: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Test-only stop failpoints. A hook returns the exact failure that `stop`
/// or the final shutdown force pass must surface before changing process
/// state or sending a signal.
#[cfg(feature = "failpoints")]
#[derive(Default)]
pub struct StopHooks {
    pub before_stop: Option<Arc<dyn Fn(&ProcessId, bool) -> Option<ProcessError> + Send + Sync>>,
}

/// No-op hooks in production builds.
#[cfg(not(feature = "failpoints"))]
#[derive(Default, Clone)]
pub struct SpawnHooks {}

/// Exit information observed by the supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ExitInfo {
    pub code: Option<i32>,
    /// True when the process was killed by a signal (unix) rather than
    /// exiting normally.
    pub signaled: bool,
}

impl ExitInfo {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillRequest {
    Graceful,
    Force,
}

/// A supervised child. Created only by `ProcessSupervisor::spawn`; the caller
/// holds a safe handle and never the raw OS child (§15).
pub struct ManagedProcess {
    id: ProcessId,
    pid: u32,
    state: AtomicU8,
    stdout: Mutex<BoundedOutputBuffer>,
    stderr: Mutex<BoundedOutputBuffer>,
    exit_wait_timeout: Duration,
    kill_timeout: Duration,
    exit_tx: watch::Sender<Option<ExitInfo>>,
    exit_rx: watch::Receiver<Option<ExitInfo>>,
    kill_tx: watch::Sender<Option<KillRequest>>,
    /// Live stdin for interactive protocol children (`StdinPolicy::Piped`);
    /// `None` otherwise or after `stdin_close`.
    stdin: tokio::sync::Mutex<Option<tokio::process::ChildStdin>>,
    /// Raw protocol stdout stream (`ProcessSpec::stdout_protocol`); taken
    /// once by the owning protocol consumer.
    protocol_rx: Mutex<Option<tokio::sync::mpsc::Receiver<Vec<u8>>>>,
    /// Windows: the Job Object owning this process tree; Unix: no-op.
    _job: platform::JobHandle,
    /// Redacted command line for diagnostics (secret args masked).
    command: String,
    /// Environment variable names only (values never stored/displayed).
    env_names: Vec<String>,
}

impl std::fmt::Debug for ManagedProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedProcess")
            .field("id", &self.id)
            .field("pid", &self.pid)
            .field("state", &self.state())
            .field("command", &self.command)
            .finish()
    }
}

impl ManagedProcess {
    pub fn id(&self) -> &ProcessId {
        &self.id
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn state(&self) -> ProcessState {
        ProcessState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Validate + apply a state transition. Returns `false` (no change) for
    /// impossible transitions — callers must treat that as a race outcome,
    /// never an error.
    fn transition(&self, to: ProcessState) -> bool {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if !ProcessState::from_u8(current).allows(to) {
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                to as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Captured stdout lines (bounded ring; oldest dropped on overflow).
    pub fn stdout(&self) -> Vec<String> {
        self.stdout
            .lock()
            .expect("stdout mutex poisoned")
            .all_lines()
    }

    /// Captured stderr lines (bounded ring; separate from stdout).
    pub fn stderr(&self) -> Vec<String> {
        self.stderr
            .lock()
            .expect("stderr mutex poisoned")
            .all_lines()
    }

    /// Total captured bytes across both rings (bounded by `OUTPUT_CAP_BYTES`
    /// each).
    pub fn output_bytes(&self) -> usize {
        self.stdout
            .lock()
            .expect("stdout mutex poisoned")
            .byte_len()
            + self
                .stderr
                .lock()
                .expect("stderr mutex poisoned")
                .byte_len()
    }

    /// Number of lines dropped from both rings due to the byte cap.
    pub fn dropped_lines(&self) -> u64 {
        self.stdout
            .lock()
            .expect("stdout mutex poisoned")
            .dropped_lines()
            + self
                .stderr
                .lock()
                .expect("stderr mutex poisoned")
                .dropped_lines()
    }

    /// Subscribe to process exit. `None` until the process exits; then the
    /// exit info. Late subscribers see the last value.
    pub fn exit(&self) -> watch::Receiver<Option<ExitInfo>> {
        self.exit_rx.clone()
    }

    pub fn has_exited(&self) -> bool {
        self.exit_rx.borrow().is_some()
    }

    /// Serialized protocol write to the child's stdin (`StdinPolicy::Piped`).
    /// One writer owner: concurrent writes are queued by the mutex, never
    /// interleaved bytes (TASK 20 §20/§78). Errors when stdin is closed or
    /// was never piped.
    pub async fn stdin_write_all(&self, bytes: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut guard = self.stdin.lock().await;
        let Some(writer) = guard.as_mut() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "stdin not piped or already closed",
            ));
        };
        writer.write_all(bytes).await?;
        writer.flush().await
    }

    /// Close the child's stdin (EOF). Idempotent. For ACP-style protocols
    /// this is the protocol-level graceful shutdown signal (connection
    /// teardown), distinct from process termination (TASK 20 §60–§61).
    pub async fn stdin_close(&self) {
        use tokio::io::AsyncWriteExt;
        let mut guard = self.stdin.lock().await;
        if let Some(mut writer) = guard.take() {
            let _ = writer.shutdown().await;
        }
    }

    /// Take the raw protocol stdout stream (once). `None` when stdout is not
    /// in protocol mode or the stream was already taken. The consumer owns
    /// draining it; dropping it applies backpressure release to the reader
    /// task (TASK 20 §75).
    pub fn protocol_stream(&self) -> Option<tokio::sync::mpsc::Receiver<Vec<u8>>> {
        self.protocol_rx
            .lock()
            .expect("protocol rx mutex poisoned")
            .take()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProcessSnapshot {
    pub id: String,
    pub state: ProcessState,
    pub pid: u32,
    pub command: String,
    /// Environment variable **names** only — values never leave the crate.
    pub env: Vec<String>,
    pub output_bytes: usize,
    pub dropped_lines: u64,
    pub exit_code: Option<i32>,
}

#[derive(Clone)]
pub struct ProcessSupervisor {
    bus: EventBus,
    processes: Arc<Mutex<HashMap<ProcessId, Arc<ManagedProcess>>>>,
    shutting_down: Arc<AtomicBool>,
    /// Spawns admitted but not yet registered (or aborted). Shutdown closes
    /// admission, then waits for this counter to drain BEFORE its stop pass
    /// — a child that registers after the sweep would survive unsupervised
    /// (TASK 24 §9). Decremented on every exit path after increment.
    spawning: Arc<std::sync::atomic::AtomicUsize>,
    /// ProcessIds reserved for in-flight spawns (admission→registration
    /// window). The duplicate check + reservation is ATOMIC: two concurrent
    /// same-id spawns cannot both pass the check and create two children —
    /// the second caller is rejected BEFORE any OS spawn (TASK 24 §9). The
    /// reservation is released on every failure path and replaced by the
    /// registered `ManagedProcess` on success.
    starting: Arc<Mutex<std::collections::HashSet<ProcessId>>>,
    #[cfg(feature = "failpoints")]
    hooks: Arc<Mutex<SpawnHooks>>,
    #[cfg(feature = "failpoints")]
    stop_hooks: Arc<Mutex<StopHooks>>,
}

/// RAII guard for a ProcessId reservation in the `starting` set.
/// Automatically releases the reservation (and decrements `spawning`)
/// on drop unless explicitly disarmed. This guarantees that every
/// post-admission spawn failure — including Job Object creation, assign,
/// resume, and shutdown-race — releases admission and emits exactly one
/// `process.failed` event (CORE-020).
struct StartingReservation {
    starting: Arc<Mutex<std::collections::HashSet<ProcessId>>>,
    spawning: Arc<std::sync::atomic::AtomicUsize>,
    id: ProcessId,
    armed: bool,
}

impl StartingReservation {
    fn new(
        starting: Arc<Mutex<std::collections::HashSet<ProcessId>>>,
        spawning: Arc<std::sync::atomic::AtomicUsize>,
        id: ProcessId,
    ) -> Self {
        spawning.fetch_add(1, Ordering::SeqCst);
        Self {
            starting,
            spawning,
            id,
            armed: true,
        }
    }

    /// Disarm the guard: the reservation has been converted into a registered
    /// `ManagedProcess` and must not be released on drop.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for StartingReservation {
    fn drop(&mut self) {
        if self.armed {
            self.starting
                .lock()
                .expect("starting set mutex poisoned")
                .remove(&self.id);
            self.spawning.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl ProcessSupervisor {
    pub fn new(bus: EventBus) -> Self {
        Self {
            bus,
            processes: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
            spawning: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            starting: Arc::new(Mutex::new(std::collections::HashSet::new())),
            #[cfg(feature = "failpoints")]
            hooks: Arc::new(Mutex::new(SpawnHooks::default())),
            #[cfg(feature = "failpoints")]
            stop_hooks: Arc::new(Mutex::new(StopHooks::default())),
        }
    }

    /// Spawn a supervised process. On success the process is `RUNNING` (the
    /// OS process exists) and registered; on failure no record is left
    /// behind (§13/§34) and a `process.failed` event is published.
    pub async fn spawn(&self, spec: ProcessSpec) -> Result<Arc<ManagedProcess>, ProcessError> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ProcessError::ShuttingDown);
        }
        if spec.command.trim().is_empty() {
            return Err(ProcessError::InvalidSpec("empty command".into()));
        }
        if let Some(cwd) = &spec.cwd {
            if !cwd.is_dir() {
                return Err(ProcessError::BadCwd { path: cwd.clone() });
            }
        }
        // ATOMIC duplicate check + reservation (TASK 24 §9, CORE-020): the
        // id is reserved BEFORE any async OS spawn, so a concurrent same-id
        // spawn is rejected here with DuplicateId and never creates a child.
        // The RAII guard guarantees that every post-admission failure
        // (Job Object create/assign/resume, OS spawn, shutdown-race)
        // releases the reservation and decrements `spawning` automatically.
        {
            let map = self.processes.lock().expect("process map mutex poisoned");
            let mut starting = self.starting.lock().expect("starting set mutex poisoned");
            if map.contains_key(&spec.id) || starting.contains(&spec.id) {
                return Err(ProcessError::DuplicateId {
                    id: spec.id.clone(),
                });
            }
            starting.insert(spec.id.clone());
        }
        let reservation = StartingReservation::new(
            self.starting.clone(),
            self.spawning.clone(),
            spec.id.clone(),
        );

        // Create the ownership primitive before the child exists.
        let job = platform::JobHandle::create().map_err(|source| {
            let error = ProcessError::Platform {
                id: spec.id.clone(),
                op: "create job object",
                source,
            };
            self.bus.publish(Event::ProcessFailed {
                process_id: spec.id.clone(),
                error: error.to_string(),
            });
            // Guard drops here, releasing the reservation.
            error
        })?;

        let mut cmd = Command::new(&spec.command);
        cmd.args(&spec.args)
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(windows)]
        for raw in &spec.raw_args {
            cmd.raw_arg(raw);
        }
        let stdin_bytes = match &spec.stdin {
            StdinPolicy::Null | StdinPolicy::Inherit => None,
            StdinPolicy::Bytes(bytes) => Some(bytes.clone()),
            StdinPolicy::Piped => None,
        };
        let stdin_piped = matches!(spec.stdin, StdinPolicy::Piped);
        cmd.stdin(if stdin_bytes.is_some() || stdin_piped {
            Stdio::piped()
        } else {
            match spec.stdin {
                StdinPolicy::Null => Stdio::null(),
                StdinPolicy::Inherit => Stdio::inherit(),
                StdinPolicy::Bytes(_) | StdinPolicy::Piped => unreachable!("handled above"),
            }
        });
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        for k in &spec.env_remove {
            cmd.env_remove(k);
        }
        #[cfg(windows)]
        {
            // Spawn suspended so the job assignment happens before any code
            // runs (no descendant can escape); no console window for CLI
            // engines (§81).
            cmd.creation_flags(platform::CREATE_SUSPENDED | platform::CREATE_NO_WINDOW);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // Own process group so the whole tree can be signaled.
            cmd.process_group(0);
        }

        let mut child = cmd.spawn().map_err(|e| {
            let error = if e.kind() == std::io::ErrorKind::NotFound {
                ProcessError::CommandNotFound {
                    command: spec.command.clone(),
                    source: e,
                }
            } else {
                ProcessError::Spawn {
                    id: spec.id.clone(),
                    source: e,
                }
            };
            self.bus.publish(Event::ProcessFailed {
                process_id: spec.id.clone(),
                error: error.to_string(),
            });
            // Guard drops here, releasing the reservation.
            error
        })?;
        let pid = child.id().expect("spawned child must have a pid");

        // Windows: assign the suspended child to its job, then resume it.
        if let Err(e) = job.assign(pid) {
            // Never leave a managed child behind on failure (§34).
            let _ = child.kill().await;
            let _ = child.wait().await;
            // Guard drops here, releasing the reservation.
            return Err(ProcessError::Platform {
                id: spec.id.clone(),
                op: "assign process to job",
                source: e,
            });
        }
        if let Err(e) = job.resume(pid) {
            let _ = child.kill().await;
            let _ = child.wait().await;
            // Guard drops here, releasing the reservation.
            return Err(ProcessError::Platform {
                id: spec.id.clone(),
                op: "resume process",
                source: e,
            });
        }
        // Test barrier: pause the spawn in the admission→registration window
        // so a test can start shutdown and prove the child is still reaped.
        #[cfg(feature = "failpoints")]
        {
            let hooks = self.hooks.lock().expect("spawn hooks mutex poisoned");
            if let Some(f) = &hooks.before_register {
                f();
            }
        }

        // Re-check shutdown AFTER the child exists: shutdown may have closed
        // admission and finished its sweep while we were creating the child
        // (our increment can land after the sweep's drain wait observed 0).
        // A child created after shutdown began must be terminated and reaped
        // HERE — never left to appear after the stop pass (TASK 24 §9).
        if self.shutting_down.load(Ordering::SeqCst) {
            let _ = child.kill().await;
            let _ = child.wait().await;
            // Guard drops here, releasing the reservation.
            return Err(ProcessError::ShuttingDown);
        }

        let (exit_tx, exit_rx) = watch::channel(None);
        let (kill_tx, _kill_rx) = watch::channel(None);
        // Protocol mode: a bounded raw byte channel for stdout, drained by
        // the owning protocol consumer (backpressure, never unbounded).
        let (protocol_tx, protocol_rx) = if spec.stdout_protocol {
            let (tx, rx) = tokio::sync::mpsc::channel(spec.protocol_channel_messages.max(1));
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let piped_stdin = if stdin_piped {
            child.stdin.take()
        } else {
            None
        };

        let output_cap = spec.output_cap_bytes.unwrap_or(crate::OUTPUT_CAP_BYTES);
        let process = Arc::new(ManagedProcess {
            id: spec.id.clone(),
            pid,
            state: AtomicU8::new(ProcessState::Spawning as u8),
            stdout: Mutex::new(BoundedOutputBuffer::with_cap(output_cap)),
            stderr: Mutex::new(BoundedOutputBuffer::with_cap(output_cap)),
            exit_wait_timeout: spec.exit_wait_timeout,
            kill_timeout: spec.kill_timeout,
            exit_tx,
            exit_rx,
            kill_tx,
            stdin: tokio::sync::Mutex::new(piped_stdin),
            protocol_rx: Mutex::new(protocol_rx),
            _job: job,
            command: spec.redacted_command_line(),
            env_names: spec
                .env
                .iter()
                .map(|(k, _)| k.clone())
                .chain(spec.env_remove.iter().cloned())
                .collect(),
        });
        process.transition(ProcessState::Running);

        // Register before the monitor can finalize (a fast process could
        // otherwise be removed before it is inserted).
        {
            let mut map = self.processes.lock().expect("process map mutex poisoned");
            map.insert(spec.id.clone(), process.clone());
        }
        // CORE-020: disarm the RAII reservation guard — the ProcessId is now
        // a registered ManagedProcess, not a pending reservation. This must
        // happen AFTER insertion so shutdown's drain wait sees the registered
        // process (or the spawn's own re-check above must have killed it).
        reservation.disarm();
        self.spawning.fetch_sub(1, Ordering::SeqCst);

        self.bus.publish(Event::ProcessStarted {
            process_id: spec.id.clone(),
            pid,
        });

        // Feed bounded stdin bytes, then drop the handle (EOF) so the child
        // sees end-of-input and can respond (generic CLI one-shot, §46).
        if let Some(bytes) = stdin_bytes {
            let stdin_handle = child.stdin.take();
            if let Some(mut writer) = stdin_handle {
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let _ = writer.write_all(&bytes).await;
                    let _ = writer.shutdown().await;
                });
            }
        }

        // Output readers: lossy UTF-8, bounded ring (§16–§21). In protocol
        // mode the stdout reader additionally forwards raw byte chunks to the
        // bounded protocol channel (exact bytes — the protocol consumer owns
        // framing); the ring stays diagnostics-only (TASK 20 §21/§75). When
        // protocol diagnostics are disabled the ring path is skipped entirely
        // (TASK 24 perf — no dual text processing).
        let stdout_stream = child.stdout.take();
        let stderr_stream = child.stderr.take();
        let protocol_stdout_diagnostics = spec.protocol_stdout_diagnostics;
        let out_proc = process.clone();
        let out_task = tokio::spawn(async move {
            let Some(mut stream) = stdout_stream else {
                return;
            };
            match protocol_tx {
            Some(tx) => {
                let mut buf = vec![0u8; 8192];
                // Machine-protocol mode with diagnostics OFF: raw chunks are
                // forwarded verbatim and the lossy UTF-8 line-ring path is
                // skipped entirely — protocol traffic never pays a second
                // text-processing path (TASK 24 perf). Backpressure: blocking
                // send stalls the child's stdout instead of unbounded
                // buffering; the consumer must drain.
                if !protocol_stdout_diagnostics {
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                if tx.send(buf[..n].to_vec()).await.is_err() {
                                    break; // consumer gone (teardown)
                                }
                            }
                            Err(_) => break,
                        }
                    }
                } else {
                    // Explicit diagnostic mode: raw chunks STILL go to the
                    // protocol channel verbatim (UTF-8 split is the
                    // consumer's concern, never lossy here), and a bounded
                    // lossy line ring is additionally kept for humans.
                    let mut pending = String::new();
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                pending.push_str(&String::from_utf8_lossy(&buf[..n]));
                                while let Some(pos) = pending.find('\n') {
                                    let line = pending[..pos].trim_end_matches(['\r']).to_owned();
                                    out_proc
                                        .stdout
                                        .lock()
                                        .expect("stdout mutex poisoned")
                                        .push_line(line);
                                    pending.drain(..=pos);
                                }
                                if pending.len() > crate::OUTPUT_RETAIN_BYTES {
                                    pending.clear();
                                }
                                if tx.send(buf[..n].to_vec()).await.is_err() {
                                    break; // consumer gone (teardown)
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    if !pending.is_empty() {
                        out_proc
                            .stdout
                            .lock()
                            .expect("stdout mutex poisoned")
                            .push_line(pending);
                    }
                }
            }
                None => {
                    read_lines(stream, |line| {
                        out_proc
                            .stdout
                            .lock()
                            .expect("stdout mutex poisoned")
                            .push_line(line);
                    })
                    .await;
                }
            }
        });
        let err_proc = process.clone();
        let err_task = tokio::spawn(async move {
            if let Some(stream) = stderr_stream {
                read_lines(stream, |line| {
                    err_proc
                        .stderr
                        .lock()
                        .expect("stderr mutex poisoned")
                        .push_line(line);
                })
                .await;
            }
        });

        let monitor_proc = process.clone();
        let mut monitor_child = child;
        let mut kill_rx = monitor_proc.kill_tx.subscribe();
        let registry = self.processes.clone();
        let bus = self.bus.clone();
        tokio::spawn(async move {
            let wait_result = loop {
                tokio::select! {
                    status = monitor_child.wait() => break status,
                    _ = kill_rx.changed() => {
                        let req = *kill_rx.borrow();
                        if let Some(req) = req {
                            apply_signal(&monitor_proc, req).await;
                        }
                    }
                }
            };

            // Bounded drain of the tail output before the terminal event
            // (§32, §76). On timeout the reader tasks are EXPLICITLY aborted
            // and awaited to cancellation — dropping a JoinHandle would
            // DETACH a reader blocked on bounded channel delivery, letting
            // it outlive the process/registry entry and retain buffers/Arcs
            // (TASK 24 §9: no owned task may survive process removal).
            let mut readers = vec![out_task, err_task];
            let drained = timeout_after(OUTPUT_DRAIN_TIMEOUT, async {
                for r in readers.iter_mut() {
                    let _ = r.await;
                }
            })
            .await;
            if drained.is_err() {
                for r in readers.iter_mut() {
                    r.abort();
                }
                for r in readers.iter_mut() {
                    let _ = r.await; // await cancellation, never detach
                }
            }

            let (info, terminal) = match wait_result {
                Ok(status) => (
                    ExitInfo {
                        code: status.code(),
                        signaled: status.code().is_none(),
                    },
                    ProcessState::Exited,
                ),
                Err(e) => {
                    warn!(process = %monitor_proc.id(), error = %e, "child wait failed");
                    (
                        ExitInfo {
                            code: None,
                            signaled: true,
                        },
                        ProcessState::Failed,
                    )
                }
            };
            let _ = monitor_proc.exit_tx.send(Some(info));
            // Only the monitor owns the terminal transition (§33): a
            // concurrent stop() may already have set STOPPING.
            monitor_proc.transition(terminal);

            match terminal {
                ProcessState::Exited => {
                    bus.publish(Event::ProcessExited {
                        process_id: monitor_proc.id.clone(),
                        pid: monitor_proc.pid,
                        code: info.code,
                        signaled: info.signaled,
                    });
                    debug!(process = %monitor_proc.id(), code = ?info.code, "supervised process exited");
                }
                _ => {
                    bus.publish(Event::ProcessFailed {
                        process_id: monitor_proc.id.clone(),
                        error: "child wait failed".into(),
                    });
                }
            }

            // Exited records leave the registry (bounded registry, §61);
            // diagnostics history lives in the events + diagnostics ring.
            registry
                .lock()
                .expect("process map mutex poisoned")
                .remove(&monitor_proc.id);
        });

        Ok(process)
    }

    /// Graceful or force stop with bounded waits (§25–§26). On graceful
    /// timeout, force kill is attempted automatically. Idempotent: stopping
    /// an already-exited process returns `NotRunning` without side effects;
    /// a concurrent stop shares one outcome instead of a second kill timer.
    pub async fn stop(
        &self,
        process: &Arc<ManagedProcess>,
        graceful: bool,
    ) -> Result<ExitInfo, ProcessError> {
        match process.state() {
            ProcessState::Exited | ProcessState::Failed => {
                return Err(ProcessError::NotRunning {
                    id: process.id.clone(),
                });
            }
            _ => {}
        }

        #[cfg(feature = "failpoints")]
        {
            let hooks = self.stop_hooks.lock().expect("stop hooks mutex poisoned");
            if let Some(error) = hooks
                .before_stop
                .as_ref()
                .and_then(|hook| hook(process.id(), graceful))
            {
                return Err(error);
            }
        }

        // Only one stop owns the STOPPING transition. If it fails, the
        // process already exited naturally or another stop is in flight —
        // wait for the (single) outcome instead of starting a second kill
        // timer (§30/§33).
        if !process.transition(ProcessState::Stopping) {
            let budget = process.exit_wait_timeout + process.kill_timeout + Duration::from_secs(1);
            return match timeout_after(budget, wait_for_exit(process)).await {
                Ok(info) => Ok(info),
                Err(_) => Err(ProcessError::TerminationTimeout {
                    id: process.id.clone(),
                }),
            };
        }

        if graceful {
            let _ = process.kill_tx.send(Some(KillRequest::Graceful));
            if let Ok(info) = timeout_after(process.exit_wait_timeout, wait_for_exit(process)).await
            {
                return Ok(info);
            }
            warn!(process = %process.id(), "graceful stop timed out; forcing");
        }
        let _ = process.kill_tx.send(Some(KillRequest::Force));
        match timeout_after(process.kill_timeout, wait_for_exit(process)).await {
            Ok(info) => Ok(info),
            Err(_) => Err(ProcessError::TerminationTimeout {
                id: process.id.clone(),
            }),
        }
    }

    /// Stop every registered process CONCURRENTLY (TASK 24 perf): each
    /// independent process receives its stop immediately instead of serially,
    /// so a hung owner can no longer delay every other stop by a full
    /// graceful+force budget. `stop(true)` already performs graceful→force
    /// escalation internally, so no second full stop sequence is issued per
    /// process — `shutdown()` retains the final `kill_all()` force sweep.
    /// Returns the ids that resisted the stop (not provably exited).
    pub async fn stop_all(&self) -> Vec<String> {
        let ids: Vec<ProcessId> = {
            let map = self.processes.lock().expect("process map mutex poisoned");
            map.keys().cloned().collect()
        };
        let mut handles = Vec::new();
        for id in ids {
            let Some(process) = self.get(&id) else {
                continue;
            };
            if process.has_exited() {
                continue;
            }
            let this = self.clone();
            handles.push(tokio::spawn(async move {
                (id.to_string(), this.stop(&process, true).await)
            }));
        }
        let mut forced = Vec::new();
        for handle in handles {
            match handle.await {
                Ok((_id, Ok(_))) => {}
                Ok((id, Err(e))) => {
                    warn!(process = %id, error = %e, "stop failed after graceful+force escalation");
                    forced.push(id);
                }
                Err(e) => warn!(error = %e, "stop task failed to join"),
            }
        }
        forced
    }

    /// Force every still-live process CONCURRENTLY and wait for bounded exit
    /// proof. Returns ids whose force request failed or whose exit remained
    /// unproven; their registry records remain owned by the supervisor.
    pub async fn kill_all(&self) -> Vec<String> {
        let ids: Vec<ProcessId> = {
            let map = self.processes.lock().expect("process map mutex poisoned");
            map.keys().cloned().collect()
        };
        let mut handles = Vec::new();
        for id in ids {
            let Some(process) = self.get(&id) else {
                continue;
            };
            if process.has_exited() {
                continue;
            }
            let this = self.clone();
            let report_id = id.to_string();
            handles.push((
                report_id,
                tokio::spawn(async move { this.force_and_wait(&process).await }),
            ));
        }
        let mut survivors = Vec::new();
        for (id, handle) in handles {
            match handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    warn!(process = %id, error = %error, "final force failed; retaining process authority");
                    survivors.push(id);
                }
                Err(error) => {
                    warn!(process = %id, error = %error, "final force task failed; retaining process authority");
                    survivors.push(id);
                }
            }
        }
        survivors.sort();
        survivors
    }

    /// Deterministic shutdown (§36): reject new spawns, gracefully stop,
    /// then force the rest with bounded exit proof. Proven-exited records are
    /// discarded; live survivors remain registered so the one process
    /// authority can retry teardown. Returns the ids that **resisted even the
    /// final force pass**. A routine escalation that succeeds is not listed.
    pub async fn shutdown(&self) -> Vec<String> {
        self.mark_shutting_down();
        // Close admission and wait for in-flight spawns to either register or
        // abort BEFORE the stop pass: a child that registers after the sweep
        // would survive unsupervised (TASK 24 §9). Bounded — a wedged spawn
        // must not hang shutdown; the spawn-side re-check above still kills
        // any child created after shutdown began.
        let drain_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while self.spawning.load(Ordering::SeqCst) > 0
            && std::time::Instant::now() < drain_deadline
        {
            sleep(Duration::from_millis(10)).await;
        }
        let in_flight = self.spawning.load(Ordering::SeqCst);
        if in_flight > 0 {
            warn!(
                in_flight,
                "spawn in-flight counter did not drain; late spawns self-kill via the shutdown re-check"
            );
        }
        let _initial_failures = self.stop_all().await;
        let survivors = self.kill_all().await;
        let active = {
            let mut map = self.processes.lock().expect("process map mutex poisoned");
            // Exit proof is published only after bounded reader cleanup, so
            // removing proven terminal records cannot detach owned tasks.
            map.retain(|_, process| !process.has_exited());
            map.len()
        };
        info!(
            active,
            survivors = survivors.len(),
            "supervisor shutdown complete"
        );
        survivors
    }

    /// Last-resort force that deliberately bypasses the STOPPING transition:
    /// an earlier stop may have timed out while owning that transition, but
    /// shutdown must still issue a fresh force request and prove the result.
    async fn force_and_wait(
        &self,
        process: &Arc<ManagedProcess>,
    ) -> Result<ExitInfo, ProcessError> {
        if let Some(info) = *process.exit_rx.borrow() {
            return Ok(info);
        }

        #[cfg(feature = "failpoints")]
        {
            let hooks = self.stop_hooks.lock().expect("stop hooks mutex poisoned");
            if let Some(error) = hooks
                .before_stop
                .as_ref()
                .and_then(|hook| hook(process.id(), false))
            {
                return Err(error);
            }
        }

        let _ = process.transition(ProcessState::Stopping);
        let _ = process.kill_tx.send(Some(KillRequest::Force));
        match timeout_after(process.kill_timeout, wait_for_exit(process)).await {
            Ok(info) => Ok(info),
            Err(_) => Err(ProcessError::TerminationTimeout {
                id: process.id.clone(),
            }),
        }
    }

    pub fn get(&self, id: &ProcessId) -> Option<Arc<ManagedProcess>> {
        self.processes
            .lock()
            .expect("process map mutex poisoned")
            .get(id)
            .cloned()
    }

    pub fn remove(&self, id: &ProcessId) {
        self.processes
            .lock()
            .expect("process map mutex poisoned")
            .remove(id);
    }

    pub fn count(&self) -> usize {
        self.processes
            .lock()
            .expect("process map mutex poisoned")
            .len()
    }

    pub fn snapshots(&self) -> Vec<ProcessSnapshot> {
        let map = self.processes.lock().expect("process map mutex poisoned");
        map.values()
            .map(|p| ProcessSnapshot {
                id: p.id.to_string(),
                state: p.state(),
                pid: p.pid,
                command: p.command.clone(),
                env: p.env_names.clone(),
                output_bytes: p.output_bytes(),
                dropped_lines: p.dropped_lines(),
                exit_code: (*p.exit_rx.borrow()).and_then(|e| e.code),
            })
            .collect()
    }

    pub fn mark_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// Release a ProcessId reservation on a spawn failure path. Idempotent.
    fn release_starting(&self, id: &ProcessId) {
        self.starting
            .lock()
            .expect("starting set mutex poisoned")
            .remove(id);
    }
}

#[cfg(feature = "failpoints")]
impl ProcessSupervisor {
    /// Test-only injection of spawn failpoints. Feature-gated.
    #[doc(hidden)]
    pub fn set_spawn_hooks_for_test(&self, hooks: SpawnHooks) {
        *self.hooks.lock().expect("spawn hooks mutex poisoned") = hooks;
    }

    /// Test-only injection of stop failures before any stop side effect.
    #[doc(hidden)]
    pub fn set_stop_hooks_for_test(&self, hooks: StopHooks) {
        *self.stop_hooks.lock().expect("stop hooks mutex poisoned") = hooks;
    }
}

/// Maximum bytes accumulated before a newline while reading. Lines that
/// exceed this are truncated with a marker, preventing unbounded memory
/// growth from a child that never emits a newline (CORE-021).
const PENDING_LINE_MAX_BYTES: usize = 256 * 1024;

/// Read a pipe to EOF, splitting on `\n`, converting lossily. The final
/// unterminated chunk is delivered as a line at EOF (§20): partial output is
/// never lost and never split arbitrarily.
///
/// CORE-021: pending line storage is bounded at `PENDING_LINE_MAX_BYTES`.
/// A child producing arbitrarily large newline-free output has the pending
/// buffer truncated with a marker, so transient memory use never exceeds
/// the policy bound. Normal newline-delimited output is unaffected.
async fn read_lines<R, F>(reader: R, mut push: F)
where
    R: tokio::io::AsyncRead + Unpin,
    F: FnMut(String),
{
    let mut reader = BufReader::new(reader);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                // CORE-021: if the pending line exceeds the bound, truncate
                // and append a marker so the caller knows output was clipped.
                if buf.len() > PENDING_LINE_MAX_BYTES {
                    buf.truncate(PENDING_LINE_MAX_BYTES);
                    // Append a truncation marker (lossy-safe: we just truncated
                    // at a byte boundary, so the marker is valid ASCII).
                    b"\n...[truncated]".iter().for_each(|&b| buf.push(b));
                }
                // Lossy UTF-8: invalid bytes become U+FFFD, never a panic (§21).
                let text = String::from_utf8_lossy(&buf);
                push(text.trim_end_matches(['\n', '\r']).to_owned());
            }
            Err(_) => break, // pipe closed
        }
    }
}

async fn wait_for_exit(process: &ManagedProcess) -> ExitInfo {
    let mut rx = process.exit();
    loop {
        if let Some(info) = *rx.borrow() {
            return info;
        }
        if rx.changed().await.is_err() {
            return ExitInfo {
                code: None,
                signaled: true,
            };
        }
    }
}

/// Deliver a stop signal. Graceful is platform-aware and best-effort; Force
/// terminates the whole owned tree (Job Object on Windows, process group on
/// Unix).
async fn apply_signal(process: &ManagedProcess, request: KillRequest) {
    match request {
        KillRequest::Graceful => {
            #[cfg(windows)]
            {
                // Console-aware graceful hint (taskkill without /F). It may
                // fail for non-console children — the supervisor escalates to
                // Force at the bounded deadline, which is the reliable path.
                let mut cmd = Command::new("taskkill");
                cmd.args(["/T", "/PID", &process.pid.to_string()]);
                if let Err(e) = cmd.output().await {
                    warn!(pid = process.pid, error = %e, "graceful taskkill failed");
                }
            }
            #[cfg(unix)]
            {
                // SIGTERM to the whole group (children included).
                // SAFETY: pid is our own spawned process group.
                let rc = unsafe { libc::kill(-(process.pid as i32), libc::SIGTERM) };
                if rc != 0 {
                    warn!(pid = process.pid, "SIGTERM failed; escalating at timeout");
                }
            }
        }
        KillRequest::Force => {
            #[cfg(windows)]
            {
                // One OS call kills the entire job = the entire tree.
                if let Err(e) = process._job.terminate(1) {
                    warn!(pid = process.pid, error = %e, "TerminateJobObject failed");
                }
            }
            #[cfg(unix)]
            {
                // SIGKILL to the whole group.
                // SAFETY: pid is our own spawned process group.
                unsafe { libc::kill(-(process.pid as i32), libc::SIGKILL) };
            }
        }
    }
}
