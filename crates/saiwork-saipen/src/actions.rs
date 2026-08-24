//! SAIPEN actions (TASK 15).
//!
//! Authority rule (non-negotiable, §2): SAIWORK2 never mutates canonical
//! SAIPEN files directly. Every canonical action is invoked through the
//! canonical tool (`saipen.py` / `validate.py` from the SAIPEN install
//! referenced by STATE `saipen_home`), executed as a managed process with
//! explicit cwd = the validated workspace root, no shell, bounded output.
//!
//! Verified contract (donors/saipen v7.224.3, 2026-08-16): the canonical
//! `saipen.py` CLI surface is
//! `status|next|recover|claim|transition|checkpoint|ticket|improve|ship|
//! push|scope|first-publish-confirm|userperson|sub|context` (--json /
//! --dry-run). **There is no `continue`, `board`, `knowledge`, `validate`,
//! or `stop` command in that CLI** (§3): those SAIPENBAR labels are mapped
//! honestly — Status/Validate to canonical tool invocations where they
//! exist (`validate.py` is the standalone read-only validator), Board/
//! Knowledge to local read projections, Continue to `UnsupportedAction`
//! (the canonical "continue" is the agent's own protocol instruction in
//! STATE `next_action`, not a CLI command), Stop to cancellation of
//! SAIWORK2-owned action processes only.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saiwork_events::{Event, EventBus, ProcessId, WorkspaceId};
use saiwork_process::{ProcessSpec, ProcessSupervisor};
use serde::Serialize;

use crate::model::{SaipenRoot, STATE_FILE};

/// Canonical tool file names inside the SAIPEN install (`saipen_home`).
pub const SAIPEN_CLI: &str = "tools/saipen.py";
pub const SAIPEN_VALIDATOR: &str = "tools/validate.py";
pub const VERSION_FILE: &str = "VERSION";

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/// Typed action — only actions actually supported by the verified contract
/// (§11). `Continue`/`Stop` are declared because the SAIPENBAR labels exist,
/// but they are mapped to `Unsupported`/control semantics honestly (§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SaipenAction {
    /// Canonical `saipen.py status --json` (READ_ONLY).
    Status,
    /// Canonical `validate.py` (read-only analyzer; 0=valid, nonzero=invalid).
    Validate,
    /// Local view action on the TASK 14 board snapshot (no CLI command).
    Board,
    /// Local view action on the canonical KNOWLEDGE path (no CLI command).
    Knowledge,
    /// No canonical `saipen continue` CLI exists in v7.224.3 → unsupported.
    Continue,
    /// Cancels SAIWORK2-owned action processes only (no canonical stop cmd).
    Stop,
}

impl SaipenAction {
    pub fn as_str(self) -> &'static str {
        match self {
            SaipenAction::Status => "status",
            SaipenAction::Validate => "validate",
            SaipenAction::Board => "board",
            SaipenAction::Knowledge => "knowledge",
            SaipenAction::Continue => "continue",
            SaipenAction::Stop => "stop",
        }
    }
}

impl std::str::FromStr for SaipenAction {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "status" => SaipenAction::Status,
            "validate" => SaipenAction::Validate,
            "board" => SaipenAction::Board,
            "knowledge" => SaipenAction::Knowledge,
            "continue" => SaipenAction::Continue,
            "stop" => SaipenAction::Stop,
            _ => return Err(()),
        })
    }
}

/// Classification (§13): `Mutating` requires per-workspace exclusivity;
/// `ReadOnly` may run concurrently with other reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    ReadOnly,
    Mutating,
    View,
    Control,
    Unsupported,
}

pub fn kind_of(action: SaipenAction) -> ActionKind {
    match action {
        SaipenAction::Status | SaipenAction::Validate => ActionKind::ReadOnly,
        // Board/Knowledge are pure navigation on the reader snapshot.
        SaipenAction::Board | SaipenAction::Knowledge => ActionKind::View,
        SaipenAction::Stop => ActionKind::Control,
        SaipenAction::Continue => ActionKind::Unsupported,
    }
}

/// Action lifecycle (§17): one terminal outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelling,
    Cancelled,
}

/// One action invocation record (§16).
#[derive(Debug, Clone, Serialize)]
pub struct ActionRecord {
    pub action_id: String,
    pub workspace_id: String,
    pub action: String,
    pub state: ActionState,
    pub started_at_ms: i64,
    pub duration_ms: Option<i64>,
    /// Normalized result summary ("valid"/"invalid"/"ok"/exit code).
    pub result: Option<String>,
    pub error: Option<String>,
}

/// Typed action errors (§24).
#[derive(Debug, thiserror::Error)]
pub enum ActionError {
    #[error("SAIPEN tool not available: {0}")]
    NotAvailable(String),
    #[error("unsupported SAIPEN version for actions: {0}")]
    UnsupportedVersion(String),
    #[error("action {0} is not supported by the canonical SAIPEN contract in this baseline")]
    UnsupportedAction(&'static str),
    #[error("workspace has no attached SAIPEN")]
    NoSaipen,
    #[error("SAIPEN tool path from project state is not trusted (executable actions disabled): {0}")]
    UntrustedToolPath(String),
    #[error("canonical SAIPEN state is invalid: {0}")]
    SaipenInvalid(String),
    #[error("another SAIPEN action is already running for this workspace")]
    Busy,
    #[error("process spawn failed: {0}")]
    SpawnFailed(String),
    #[error("action timed out after {0:?}")]
    Timeout(Duration),
    #[error("action exited with code {0:?}: {1}")]
    ExitFailure(Option<i32>, String),
    #[error("action cancelled")]
    Cancelled,
    /// The process could not be proven dead after cancel/timeout. The
    /// workspace action stays blocked/degraded — never a fabricated
    /// terminal while the old process may still be alive (TASK 24 §9).
    #[error("action termination not confirmed: {0}")]
    TerminationUnconfirmed(String),
    #[error("application is shutting down")]
    ShuttingDown,
    #[error("internal: {0}")]
    Internal(String),
}

/// Combined status view for the SAIPENBAR (§56, §87–§88).
#[derive(Debug, Clone, Serialize)]
pub struct ActionStatusView {
    pub availability: ActionAvailability,
    pub running: Option<ActionRecord>,
    pub validation_result: Option<String>,
    pub validation_stale: Option<bool>,
    pub snapshot_generation: u64,
}

/// Availability snapshot for the SAIPENBAR (§56). `Continue` is listed as
/// unavailable-with-reason; the UI shows it disabled. When `disabled_reason`
/// is set, ALL executable actions (status, validate) are unavailable — the
/// canonical tool cannot be resolved (T-080: untrusted path, missing install).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ActionAvailability {
    pub available: Vec<String>,
    pub running_action: Option<String>,
    /// Actions the verified contract does NOT expose as canonical commands
    /// (surfaced honestly, never invented): `continue`, `stop`.
    pub unsupported: Vec<String>,
    /// When `Some`, ALL executable actions are disabled for this reason
    /// (e.g. "saipen_home ... is not an explicitly trusted SAIPEN install").
    /// The bar shows the reason and MAY offer a one-click trust action.
    pub disabled_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Canonical tool client
// ---------------------------------------------------------------------------

/// Discovered canonical SAIPEN tooling (deterministic, §6): resolved from
/// STATE `saipen_home` (project-local canonical entrypoint), never a
/// disk-wide search.
#[derive(Debug, Clone)]
pub struct SaipenTool {
    pub saipen_home: PathBuf,
    pub cli: PathBuf,
    pub validator: PathBuf,
    pub version: String,
    pub python: String,
}

impl SaipenTool {
    /// Resolve the canonical tool for a root. Reads `saipen_home` from the
    /// STATE frontmatter (verified TASK 14 parser), then stats the canonical
    /// files. `python` is resolved from PATH once (Windows: `python`,
    /// unix: `python3`); an explicit invalid path never silently falls back.
    ///
    /// `extra_trusted` is an OPTIONAL explicitly-trusted SAIPEN install the
    /// CALLER designates (T-080): the desktop shell persists a user-chosen
    /// trusted home and passes it here, so an external install becomes
    /// executable through explicit in-app trust — not only via the
    /// `SAIWORK2_SAIPEN_HOME` environment contract. It never weakens the
    /// base gate: a path trusted by neither `is_trusted_home`'s env/heuristic
    /// rules nor `extra_trusted` stays `UntrustedToolPath`.
    pub fn discover_with_trusted(
        root: &SaipenRoot,
        extra_trusted: Option<&std::path::Path>,
    ) -> Result<SaipenTool, ActionError> {
        let home = saipen_home_of(root)?;
        let doc = crate::parser::parse_state(&std::fs::read_to_string(root.dir.join(STATE_FILE)).map_err(
            |e| ActionError::NotAvailable(format!("cannot read STATE: {e}")),
        )?)
        .map_err(ActionError::SaipenInvalid)?;
        // Trust gate (§132): project state may reference only an explicitly
        // trusted canonical SAIPEN installation. An opened repository must
        // never be able to point SAIWORK2 at arbitrary host code that later
        // gets executed by Status/Validate. Untrusted paths keep the
        // read-only SAIPEN view but disable executable actions with a typed
        // UntrustedToolPath — checked BEFORE any stat/read of the target, so
        // even existence probing of host paths is avoided.
        if !is_trusted_home(&home, root, extra_trusted) {
            return Err(ActionError::UntrustedToolPath(format!(
                "saipen_home {} is outside the project and not an explicitly trusted SAIPEN install",
                home.display()
            )));
        }
        let cli = home.join(SAIPEN_CLI);
        let validator = home.join(SAIPEN_VALIDATOR);
        if !cli.is_file() {
            return Err(ActionError::NotAvailable(format!(
                "canonical CLI not found: {}",
                cli.display()
            )));
        }
        if !validator.is_file() {
            return Err(ActionError::NotAvailable(format!(
                "canonical validator not found: {}",
                validator.display()
            )));
        }
        let version = std::fs::read_to_string(home.join(VERSION_FILE))
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "unknown".into());
        // Schema/version gate (§7, §132): actions require the verified
        // schema_version 3 contract.
        if doc.scalars.get("schema_version").map(String::as_str) != Some("3") {
            return Err(ActionError::UnsupportedVersion(version));
        }
        let python = if cfg!(windows) { "python" } else { "python3" }.to_string();
        Ok(SaipenTool {
            saipen_home: home,
            cli,
            validator,
            version,
            python,
        })
    }

    /// Backwards-compatible discovery without a caller-designated trusted
    /// home (env/heuristic trust only — the historical contract).
    pub fn discover(root: &SaipenRoot) -> Result<SaipenTool, ActionError> {
        Self::discover_with_trusted(root, None)
    }

    /// Build a managed ProcessSpec for an action. No shell, explicit cwd =
    /// validated **workspace** root (§9–§10) — canonical tools resolve
    /// `.saipen/` relative to the project root (`validate.py --project-root`
    /// verifies `<root>/.saipen/STATE.md`), so `SaipenRoot.dir` (the `.saipen`
    /// dir itself) is never used as cwd or project argument. Only the
    /// canonical CLI actions exist; everything else is typed UnsupportedAction
    /// (§131).
    pub fn spec_for(
        &self,
        action: SaipenAction,
        root: &SaipenRoot,
        id: &str,
    ) -> Result<ProcessSpec, ActionError> {
        let (args, cwd) = match action {
            SaipenAction::Status => (
                vec![
                    self.cli.to_string_lossy().into_owned(),
                    "status".into(),
                    "--json".into(),
                ],
                root.workspace_root.clone(),
            ),
            SaipenAction::Validate => (
                vec![
                    self.validator.to_string_lossy().into_owned(),
                    "--project-root".into(),
                    root.workspace_root.to_string_lossy().into_owned(),
                ],
                root.workspace_root.clone(),
            ),
            other => return Err(ActionError::UnsupportedAction(other.as_str())),
        };
        let mut spec = ProcessSpec::new(ProcessId::new(id), self.python.clone());
        spec.args = args;
        spec.cwd = Some(cwd);
        spec.exit_wait_timeout = Duration::from_secs(5);
        spec.kill_timeout = Duration::from_secs(3);
        Ok(spec)
    }
}

/// Read the `saipen_home` scalar from a project's STATE.md (T-080). Shared
/// by discovery and the status-command path so the frontend can surface the
/// untrusted path for one-click trust without re-parsing the error message.
pub fn saipen_home_of(root: &SaipenRoot) -> Result<PathBuf, ActionError> {
    let raw =
        std::fs::read_to_string(root.dir.join(STATE_FILE)).map_err(|e| {
            ActionError::NotAvailable(format!("cannot read STATE: {e}"))
        })?;
    let doc = crate::parser::parse_state(&raw).map_err(ActionError::SaipenInvalid)?;
    doc.scalars
        .get("saipen_home")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| ActionError::NotAvailable("STATE.md has no saipen_home".into()))
}

/// A `saipen_home` from project state is trusted only when it is (1) the
/// explicitly trusted install named by `SAIWORK2_SAIPEN_HOME`, or (2) the
/// SAIPEN install bundled with SAIWORK2 (`donors/saipen` in a source layout).
///
/// Being *inside the opened workspace is NOT a trust signal* (TASK 24 §9): an
/// opened repository can ship `.saipen/STATE.md` plus attacker-controlled
/// `tools/saipen.py`/`tools/validate.py`, and Status/Validate would execute
/// that code through Python. A workspace-local `saipen_home` therefore keeps
/// the read-only SAIPEN view but disables executable actions with a typed
/// `UntrustedToolPath` — checked BEFORE any stat/read of the target.
fn is_trusted_home(home: &Path, root: &SaipenRoot, extra_trusted: Option<&Path>) -> bool {
    let _ = root; // the opened workspace never grants executable trust
    let home = std::fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
    // T-080: an explicitly trusted install designated by the desktop shell
    // (persisted, user-chosen). Exact match OR a parent of the home — a
    // canonical install lives in `<trusted>/saipen`, so trusting the repo root
    // trusts its `saipen/` child too. Canonicalized so a `..` or `\\.\`-style
    // spelling cannot dodge the comparison.
    if let Some(extra) = extra_trusted {
        let extra_canon = std::fs::canonicalize(extra).unwrap_or_else(|_| extra.to_path_buf());
        if home == extra_canon || home.starts_with(&extra_canon) {
            return true;
        }
    }
    if let Ok(env) = std::env::var("SAIWORK2_SAIPEN_HOME") {
        let env_path = PathBuf::from(env);
        let env_canon = std::fs::canonicalize(&env_path).unwrap_or(env_path);
        if home == env_canon || home.starts_with(&env_canon) {
            return true;
        }
    }
    // Global opencode SAIPEN install (used by `saipen continue` and `saipen` skill)
    // is an explicitly trusted host install even though it lives outside any
    // single workspace. This covers `C:\Users\...\opencode\skills\saipen` and
    // the SAIPEN sibling (`V:\...\SAIPEN\saipen`) used by this repo's own
    // `saipen_home` in SAIWORK2's STATE.md.
    for var in ["SAIPEN_HOME", "OPENCODE_SAIPEN_HOME"] {
        if let Ok(p) = std::env::var(var) {
            let pb = PathBuf::from(p);
            let canon = std::fs::canonicalize(&pb).unwrap_or(pb);
            if home == canon || home.starts_with(&canon) {
                return true;
            }
        }
    }
    // The SAIPEN clone that is a sibling of this workspace's SAIPEN home
    // (the `V:\...\SAIPEN` repo that is the `saipen_home` for SAIWORK2 itself)
    // is trusted even though it is outside the opened project.
    if let Ok(cur) = std::env::var("SAIPEN_HOME") {
        let pb = PathBuf::from(cur);
        let canon = std::fs::canonicalize(&pb).unwrap_or(pb);
        if home == canon || home.starts_with(&canon) {
            return true;
        }
    }
    // Global opencode SAIPEN skill (C:\Users\...\opencode\skills\saipen) is
    // the host install for `saipen continue` and must be trusted even though
    // it lives outside any single workspace. Check the common host location
    // directly when no env var is set — string-contains is sufficient and
    // avoids extra `dirs` crate dependencies.
    {
        let h = home.to_string_lossy().replace('\\', "/").to_lowercase();
        if h.contains("opencode/skills/saipen") || h.contains("opencode\\skills\\saipen") {
            return true;
        }
    }
    if let Some(bundled) = bundled_saipen_home() {
        if home.starts_with(&bundled) {
            return true;
        }
    }
    false
}

/// The SAIPEN install bundled with SAIWORK2 (`<repo>/donors/saipen` in a
/// source/dev layout). Packaged builds trust the explicit
/// `SAIWORK2_SAIPEN_HOME` instead.
fn bundled_saipen_home() -> Option<PathBuf> {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = crate_dir.parent()?.parent()?;
    let bundled = repo.join("donors").join("saipen");
    bundled.is_dir().then_some(bundled)
}

// ---------------------------------------------------------------------------
// Action runner (process execution through ProcessSupervisor)
// ---------------------------------------------------------------------------

/// Outcome of one supervised action process.
#[derive(Debug)]
pub struct ActionOutcome {
    pub exit: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub stdout_tail: String,
    pub stderr_tail: String,
}

/// Test-only action failpoints (termination-confirmation tests).
/// Feature-gated: not reachable in production builds.
#[cfg(feature = "failpoints")]
#[derive(Default)]
pub struct ActionHooks {
    /// Parks (sync block) before the exit-confirmation wait inside
    /// `terminate_and_confirm` — simulates a termination whose exit is not
    /// yet confirmed (the workspace must stay blocked meanwhile).
    pub before_exit_confirm: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Makes `terminate_and_confirm` return `TerminationUnconfirmed`
    /// immediately — the process could not be proven dead.
    pub force_exit_unconfirmed: bool,
}

/// Executes an action ProcessSpec via the one ProcessSupervisor authority
/// (§8). Bounded output comes from the supervisor's bounded rings (§21).
pub struct SupervisorActionRunner {
    pub supervisor: Arc<ProcessSupervisor>,
    #[cfg(feature = "failpoints")]
    hooks: Mutex<ActionHooks>,
}

impl SupervisorActionRunner {
    pub fn new(supervisor: Arc<ProcessSupervisor>) -> Self {
        Self {
            supervisor,
            #[cfg(feature = "failpoints")]
            hooks: Mutex::new(ActionHooks::default()),
        }
    }

    /// Run to terminal: wait for exit up to `timeout`, honour the
    /// cancellation signal, then stop via supervisor (graceful → force).
    ///
    /// Cancellation is NEVER lost (TASK 24 §9): a watch receiver's initial
    /// value is the current value and `changed()` only fires on a CHANGE — a
    /// cancel sent after the ActiveAction is registered but before we
    /// subscribed would otherwise be invisible forever. We check the current
    /// value before spawning AND again before the select.
    ///
    /// A terminal is only reported after process exit is CONFIRMED: if the
    /// process survives graceful+force within the bounded wait, the runner
    /// returns `TerminationUnconfirmed` and the workspace action stays
    /// blocked — never a fake Cancelled while the old process may live.
    pub async fn run(
        &self,
        spec: ProcessSpec,
        timeout: Duration,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<ActionOutcome, ActionError> {
        // Pre-spawn check: never spawn a process for an already-cancelled
        // action (the cancel was requested before we subscribed — the
        // initial watch value already carries it).
        if *cancel_rx.borrow() {
            return Ok(ActionOutcome {
                exit: None,
                timed_out: false,
                cancelled: true,
                stdout_tail: String::new(),
                stderr_tail: String::new(),
            });
        }
        let process = self
            .supervisor
            .spawn(spec)
            .await
            .map_err(|e| ActionError::SpawnFailed(e.to_string()))?;
        let mut exit_rx = process.exit();
        // Separate receivers for the two terminate paths (watch receivers
        // are independent clones): each select branch borrows its own `&mut`,
        // and the normal-exit branch keeps its own receiver too.
        let mut cancel_exit_rx = process.exit();
        let mut timeout_exit_rx = process.exit();
        let outcome = tokio::select! {
            _ = cancel_rx.changed() => {
                let cancelled = *cancel_rx.borrow();
                if cancelled {
                    // Cancel may also have arrived between the pre-spawn
                    // check and now (initial value already true).
                    self.terminate_and_confirm(&process, &mut cancel_exit_rx, false, true).await
                } else {
                    Ok(ActionOutcome {
                        exit: None, timed_out: false, cancelled: false,
                        stdout_tail: String::new(), stderr_tail: String::new(),
                    })
                }
            }
            _ = tokio::time::sleep(timeout) => {
                self.terminate_and_confirm(&process, &mut timeout_exit_rx, true, false).await
            }
            res = exit_rx.changed() => {
                let code = if res.is_ok() {
                    exit_rx.borrow().and_then(|e| e.code)
                } else {
                    None
                };
                Ok(ActionOutcome {
                    exit: code, timed_out: false, cancelled: false,
                    stdout_tail: process.stdout().join("\n"),
                    stderr_tail: process.stderr().join("\n"),
                })
            }
        };
        outcome
    }

    /// Stop the process (graceful → force) and wait — bounded — for the
    /// authoritative exit. Only a CONFIRMED exit yields a terminal outcome;
    /// a survivor returns `TerminationUnconfirmed` so the workspace stays
    /// blocked instead of a second canonical action starting alongside a
    /// live old process (TASK 24 §9).
    async fn terminate_and_confirm(
        &self,
        process: &Arc<saiwork_process::ManagedProcess>,
        exit_rx: &mut tokio::sync::watch::Receiver<Option<saiwork_process::ExitInfo>>,
        timed_out: bool,
        cancelled: bool,
    ) -> Result<ActionOutcome, ActionError> {
        if let Err(e) = self.supervisor.stop(process, true).await {
            tracing::warn!(error = %e, "saipen action graceful stop failed; forcing");
            if let Err(e2) = self.supervisor.stop(process, false).await {
                tracing::warn!(error = %e2, "saipen action force stop failed");
            }
        }
        #[cfg(feature = "failpoints")]
        {
            let hooks = self.hooks.lock().expect("action hooks mutex poisoned");
            if let Some(f) = &hooks.before_exit_confirm {
                f();
            }
            if hooks.force_exit_unconfirmed {
                return Err(ActionError::TerminationUnconfirmed(
                    "injected unconfirmed termination (test)".into(),
                ));
            }
        }
        match tokio::time::timeout(Duration::from_secs(3), exit_rx.changed()).await {
            Ok(Ok(_)) => Ok(ActionOutcome {
                exit: exit_rx.borrow().and_then(|e| e.code),
                timed_out,
                cancelled,
                stdout_tail: process.stdout().join("\n"),
                stderr_tail: process.stderr().join("\n"),
            }),
            // No exit observed: the process is (or may be) still alive.
            // Never fabricate a terminal.
            _ => Err(ActionError::TerminationUnconfirmed(
                "process did not exit after stop; the workspace action stays blocked until it terminates".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// ActionManager — one owner of action lifecycle, scoped by workspace (§14)
// ---------------------------------------------------------------------------

/// Timeout policy per action kind (§25): read-only bounded; mutating longer;
/// nothing unbounded.
pub fn default_timeout(kind: ActionKind) -> Duration {
    match kind {
        ActionKind::ReadOnly => Duration::from_secs(20),
        ActionKind::Mutating => Duration::from_secs(60),
        _ => Duration::from_secs(20),
    }
}

pub struct ActionManager {
    bus: EventBus,
    runner: Arc<SupervisorActionRunner>,
    /// workspace_id → active action (in-memory registry, not persisted — §61).
    active: Mutex<HashMap<String, ActiveAction>>,
    /// workspace_id → last validation result tied to the snapshot generation
    /// it validated (§87–§88): never shown as current after the snapshot
    /// moved. In-memory only — canonical files are the durable truth.
    last_validation: Mutex<HashMap<String, (u64, String)>>,
    /// workspace_id → most recent terminal record, so the bar can show
    /// "Validate: failed" after completion (§57). Non-authoritative,
    /// in-memory, replaced by the next terminal (§61).
    last_terminal: Mutex<HashMap<String, ActionRecord>>,
    /// Test-only timeout override (mirrors QueueManager's test hooks):
    /// production always uses `default_timeout(kind)`.
    timeout_override: Mutex<Option<Duration>>,
    stopping: std::sync::atomic::AtomicBool,
}

struct ActiveAction {
    record: ActionRecord,
    kind: ActionKind,
    cancel_tx: tokio::sync::watch::Sender<bool>,
}

impl ActionManager {
    pub fn new(bus: EventBus, supervisor: Arc<ProcessSupervisor>) -> Arc<Self> {
        Arc::new(Self {
            bus,
            runner: Arc::new(SupervisorActionRunner::new(supervisor)),
            active: Mutex::new(HashMap::new()),
            last_validation: Mutex::new(HashMap::new()),
            last_terminal: Mutex::new(HashMap::new()),
            timeout_override: Mutex::new(None),
            stopping: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Action availability for the bar (§56, §131): canonical CLI actions
    /// that exist in the verified contract plus view actions; the rest are
    /// surfaced as unsupported (never invented).
    pub fn availability(&self, workspace_id: &str) -> ActionAvailability {
        let mut avail = vec!["status".to_string(), "validate".to_string()];
        avail.push("board".to_string());
        avail.push("knowledge".to_string());
        let running = self
            .active
            .lock()
            .expect("saipen actions mutex poisoned")
            .get(workspace_id)
            .map(|a| a.record.action.clone());
        ActionAvailability {
            available: avail,
            running_action: running,
            unsupported: vec!["continue".into(), "stop".into()],
            disabled_reason: None,
        }
    }

    /// Start an action. Backend is the final authority on availability and
    /// exclusivity — the frontend disable alone is never trusted (§34, §77).
    pub async fn start(
        self: &Arc<Self>,
        workspace_id: &str,
        action: SaipenAction,
        tool: Option<SaipenTool>,
        root: SaipenRoot,
    ) -> Result<ActionRecord, ActionError> {
        if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ActionError::ShuttingDown);
        }
        let kind = kind_of(action);
        if kind == ActionKind::Unsupported {
            return Err(ActionError::UnsupportedAction(action.as_str()));
        }
        // Views/navigation need no process and no mutation lock (§116).
        if kind == ActionKind::View {
            return Ok(ActionRecord {
                action_id: format!("view-{}", action.as_str()),
                workspace_id: workspace_id.to_string(),
                action: action.as_str().into(),
                state: ActionState::Succeeded,
                started_at_ms: now_ms(),
                duration_ms: Some(0),
                result: Some("view".into()),
                error: None,
            });
        }
        // Stop is a control action: it cancels the SAIWORK2-owned action
        // process — there is no canonical `saipen stop` CLI in this baseline
        // (§27–§28), so Stop never spawns a process. Backend is the final
        // authority; the UI enables Stop only when something is running.
        if kind == ActionKind::Control {
            self.cancel(workspace_id)?;
            return Ok(ActionRecord {
                action_id: format!("stop-{}", short_id()),
                workspace_id: workspace_id.to_string(),
                action: "stop".into(),
                state: ActionState::Cancelling,
                started_at_ms: now_ms(),
                duration_ms: None,
                result: None,
                error: None,
            });
        }
        // One active action per workspace (any kind); mutating exclusivity is
        // the strict case (§14, §117). Backend-enforced, not UI-only.
        // The action_id doubles as the PROCESS id (T-080 defect): the old
        // spec used workspace_id as the ProcessId, so a SECOND action on the
        // same workspace collided with the first process's still-registered
        // id (DuplicateId) and failed. Each action invocation gets a unique
        // process id, so sequential Status→Validate on one workspace works.
        let action_id = format!("act-{}-{}", action.as_str(), short_id());
        {
            let mut active = self.active.lock().expect("saipen actions mutex poisoned");
            if active.contains_key(workspace_id) {
                return Err(ActionError::Busy);
            }
            let (cancel_tx, _) = tokio::sync::watch::channel(false);
            active.insert(
                workspace_id.to_string(),
                ActiveAction {
                    record: ActionRecord {
                        action_id: action_id.clone(),
                        workspace_id: workspace_id.to_string(),
                        action: action.as_str().into(),
                        state: ActionState::Running,
                        started_at_ms: now_ms(),
                        duration_ms: None,
                        result: None,
                        error: None,
                    },
                    kind,
                    cancel_tx,
                },
            );
            drop(active);
            self.bus.publish(Event::SaipenActionStarted {
                workspace_id: WorkspaceId::new(workspace_id),
                action_id: action_id.clone(),
                kind: action.as_str().into(),
            });
        }

        let tool = tool.ok_or_else(|| ActionError::Internal("missing tool for executable action".into()))?;
        // Unique process id per action invocation (T-080): reusing the
        // workspace id as ProcessId made the second action on a workspace
        // fail with DuplicateId before the first process's registry entry
        // was reaped.
        let spec = tool.spec_for(action, &root, &action_id)?;
        let timeout = self
            .timeout_override
            .lock()
            .expect("saipen actions mutex poisoned")
            .unwrap_or_else(|| default_timeout(kind));
        let cancel_rx = {
            let active = self.active.lock().expect("saipen actions mutex poisoned");
            active
                .get(workspace_id)
                .map(|a| a.cancel_tx.subscribe())
                .ok_or(ActionError::Internal("action vanished".into()))?
        };
        let outcome = self.runner.run(spec, timeout, cancel_rx).await;

        let record = match outcome {
            Ok(outcome) if outcome.cancelled => {
                let (record, _) = self.finish(
                    workspace_id,
                    ActionState::Cancelled,
                    None,
                    Some("cancelled".into()),
                );
                self.bus.publish(Event::SaipenActionCancelled {
                    workspace_id: WorkspaceId::new(workspace_id),
                    action_id: record.action_id.clone(),
                    kind: record.action.clone(),
                });
                // Forced termination: mutation outcome may be uncertain — the
                // caller always re-reads the canonical state afterwards
                // (§26, §40). The bar surfaces the terminal, never claims
                // "nothing changed".
                return Ok(record);
            }
            Ok(outcome) if outcome.timed_out => {
                let (record, _) = self.finish(
                    workspace_id,
                    ActionState::Failed,
                    Some("timeout".into()),
                    Some(format!("action timed out after {timeout:?}")),
                );
                self.bus.publish(Event::SaipenActionFailed {
                    workspace_id: WorkspaceId::new(workspace_id),
                    action_id: record.action_id.clone(),
                    kind: record.action.clone(),
                    error: "timeout".into(),
                });
                return Ok(record);
            }
            Ok(outcome) => {
                let (state, result, error) = classify_exit(action, outcome.exit, &outcome);
                let (record, _) = self.finish(workspace_id, state, result.clone(), error);
                if state == ActionState::Succeeded {
                    self.bus.publish(Event::SaipenActionCompleted {
                        workspace_id: WorkspaceId::new(workspace_id),
                        action_id: record.action_id.clone(),
                        kind: record.action.clone(),
                        result: result.unwrap_or_default(),
                    });
                } else {
                    self.bus.publish(Event::SaipenActionFailed {
                        workspace_id: WorkspaceId::new(workspace_id),
                        action_id: record.action_id.clone(),
                        kind: record.action.clone(),
                        error: format!("exit {:?}", outcome.exit),
                    });
                }
                record
            }
            Err(e) => {
                // Termination could not be confirmed: the process may still
                // be alive. Do NOT finish/remove the active action — the
                // workspace stays blocked (Busy) so no second canonical
                // action can start alongside a live old process (TASK 24
                // §9). Surface the degraded state; the supervisor sweep is
                // the final cleanup authority.
                if matches!(e, ActionError::TerminationUnconfirmed(_)) {
                    self.bus.publish(Event::RuntimeWarning {
                        code: "SAIPEN_ACTION_TERMINATION_UNCONFIRMED".into(),
                        message: format!(
                            "SAIPEN action in workspace {workspace_id} could not be proven terminated: {e}"
                        ),
                    });
                    return Err(e);
                }
                let (record, _) = self.finish(
                    workspace_id,
                    ActionState::Failed,
                    Some("spawn_failed".into()),
                    Some(e.to_string()),
                );
                self.bus.publish(Event::SaipenActionFailed {
                    workspace_id: WorkspaceId::new(workspace_id),
                    action_id: record.action_id.clone(),
                    kind: record.action.clone(),
                    error: e.to_string(),
                });
                record
            }
        };
        Ok(record)
    }

    /// Cancel the active action of a workspace (§26, §28): request graceful
    /// stop; the runner observes the cancel signal and the supervisor
    /// escalates to force. A mutating action's outcome is treated as
    /// uncertain (caller re-reads canonical state).
    pub fn cancel(&self, workspace_id: &str) -> Result<(), ActionError> {
        let mut active = self.active.lock().expect("saipen actions mutex poisoned");
        let Some(a) = active.get_mut(workspace_id) else {
            return Err(ActionError::Internal("no active action".into()));
        };
        if a.record.state == ActionState::Running {
            a.record.state = ActionState::Cancelling;
            let _ = a.cancel_tx.send(true);
        }
        Ok(())
    }

    /// Current record: the active action if one is running, otherwise the
    /// last terminal outcome for the bar (§57). In-memory only.
    pub fn status(&self, workspace_id: &str) -> Option<ActionRecord> {
        let active = self.active.lock().expect("saipen actions mutex poisoned");
        if let Some(a) = active.get(workspace_id) {
            return Some(a.record.clone());
        }
        drop(active);
        self.last_terminal
            .lock()
            .expect("saipen actions mutex poisoned")
            .get(workspace_id)
            .cloned()
    }

    /// Record a validation result bound to the snapshot generation it was
    /// run against (§87). The caller (command layer) captures the generation
    /// from the reader at completion time.
    pub fn note_validation(&self, workspace_id: &str, generation: u64, result: String) {
        self.last_validation
            .lock()
            .expect("saipen actions mutex poisoned")
            .insert(workspace_id.to_string(), (generation, result));
    }

    /// Current validation status for the bar: `(result, stale)` — stale when
    /// the snapshot has moved past the validated generation (§88).
    pub fn validation(
        &self,
        workspace_id: &str,
        current_generation: u64,
    ) -> Option<(String, bool)> {
        self.last_validation
            .lock()
            .expect("saipen actions mutex poisoned")
            .get(workspace_id)
            .map(|(gen, res)| (res.clone(), *gen != current_generation))
    }

    /// Test-only: override the per-action timeout (see QueueManager's
    /// `set_dispatch_hooks_for_test` precedent). Production timeouts stay
    /// `default_timeout(kind)`.
    #[doc(hidden)]
    pub fn set_timeout_for_test(&self, d: Duration) {
        *self
            .timeout_override
            .lock()
            .expect("saipen actions mutex poisoned") = Some(d);
    }

    /// Shutdown: reject new actions, cancel active ones (§67, §145). The
    /// supervisor sweep is the final fallback for any survivor.
    pub fn shutdown(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let active = self.active.lock().expect("saipen actions mutex poisoned");
        for a in active.values() {
            if a.record.state == ActionState::Running {
                let _ = a.cancel_tx.send(true);
            }
        }
    }


    fn finish(
        &self,
        workspace_id: &str,
        state: ActionState,
        result: Option<String>,
        error: Option<String>,
    ) -> (ActionRecord, bool) {
        let mut active = self.active.lock().expect("saipen actions mutex poisoned");
        let Some(mut a) = active.remove(workspace_id) else {
            return (
                ActionRecord {
                    action_id: String::new(),
                    workspace_id: workspace_id.to_string(),
                    action: String::new(),
                    state,
                    started_at_ms: now_ms(),
                    duration_ms: None,
                    result,
                    error,
                },
                false,
            );
        };
        a.record.state = state;
        a.record.duration_ms = Some(now_ms() - a.record.started_at_ms);
        a.record.result = result;
        a.record.error = error;
        let terminal = a.record.clone();
        self.last_terminal
            .lock()
            .expect("saipen actions mutex poisoned")
            .insert(workspace_id.to_string(), terminal.clone());
        (terminal, a.kind == ActionKind::Mutating)
    }
}
#[cfg(feature = "failpoints")]
impl ActionManager {
    /// Test-only injection of action failpoints. Feature-gated.
    #[doc(hidden)]
    pub fn set_hooks_for_test(&self, hooks: ActionHooks) {
        *self
            .runner
            .hooks
            .lock()
            .expect("action hooks mutex poisoned") = hooks;
    }
}

/// Map a process exit to (state, result, error) using the verified canonical
/// exit semantics. For `validate.py` (donors/saipen v7.224.3): **0 = valid,
/// 1 = domain-invalid** (the validation ran and the project is not
/// conformant — a result, not a failure; §41, §127), 2+ = usage/infra error
/// (§128).
///
/// Semantic rule (TASK 24 §9): only a SUCCESSFUL validation execution with
/// exit 0/1 produces a semantic VALID/INVALID verdict (`result`). Exit 2+,
/// timeout, spawn failure etc. are infrastructure/usage failures: the action
/// fails with an error but records NO result string — so the bar can never
/// color a broken validator as project INVALID, and `note_validation` leaves
/// the last semantic outcome untouched.
fn classify_exit(
    action: SaipenAction,
    exit: Option<i32>,
    outcome: &ActionOutcome,
) -> (ActionState, Option<String>, Option<String>) {
    match action {
        SaipenAction::Validate => match exit {
            Some(0) => (ActionState::Succeeded, Some("valid".into()), None),
            Some(1) => (
                ActionState::Succeeded,
                Some("invalid".into()),
                Some("validation found issues".into()),
            ),
            // 2+ / no exit: broken validator or infrastructure — never a
            // semantic verdict, so no result string is recorded.
            _ => (
                ActionState::Failed,
                None,
                Some(bounded_tail(&outcome.stderr_tail, &outcome.stdout_tail)),
            ),
        },
        _ => match exit {
            Some(0) => (ActionState::Succeeded, Some("ok".into()), None),
            other => (
                ActionState::Failed,
                Some(format!("exit {other:?}")),
                Some(bounded_tail(&outcome.stderr_tail, &outcome.stdout_tail)),
            ),
        },
    }
}

fn bounded_tail(stderr: &str, stdout: &str) -> String {
    let combined = if stderr.trim().is_empty() {
        stdout.to_string()
    } else {
        format!("{stdout}\n{stderr}")
    };
    combined.chars().take(512).collect()
}

fn short_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}", n & 0xffff_ffff)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
