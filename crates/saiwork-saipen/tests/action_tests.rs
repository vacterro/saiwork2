//! SAIPEN action tests (TASK 15 §117–§130, §214): real subprocess execution
//! through the ProcessSupervisor against a disposable **fake canonical tool**
//! (per §120 — the harness may invoke a stand-in canonical writer/validator
//! in test setup; SAIWORK2 itself never mutates canonical files).
//!
//! The fake tool mirrors the verified donors/saipen v7.224.3 contract:
//! - `tools/saipen.py status --json` → exit 0
//! - `tools/validate.py --project-root X` → 0 valid / 1 domain-invalid /
//!   2 usage error (verified exit semantics)
//! - `.saipen/HANG` marker → sleep (for cancel/timeout tests)
//! - `.saipen/INVALID` marker → validator domain-invalid

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use saiwork_events::{Event, EventBus};
use saiwork_process::ProcessSupervisor;
use saiwork_saipen::{
    validate_root, ActionError, ActionManager, ActionRecord, ActionState, SaipenAction, SaipenTool,
};

const SAIPEN_PY: &str = r#"import sys, os, time
root = os.path.dirname(os.path.abspath(sys.argv[0]))
hang = os.path.join(os.getcwd(), ".saipen", "HANG")
if os.path.exists(hang):
    time.sleep(60)
if len(sys.argv) >= 2 and sys.argv[1] == "status":
    print('{"phase": "BUILD", "ok": true}')
    sys.exit(0)
print("FAIL: unknown command")
sys.exit(2)
"#;

const VALIDATE_PY: &str = r#"import sys, os, time
root = "."
args = sys.argv[1:]
if "--project-root" in args:
    i = args.index("--project-root")
    if i + 1 >= len(args):
        print("FAIL: --project-root requires a path")
        sys.exit(2)
    root = args[i + 1]
if not os.path.isdir(root):
    print("FAIL: project root is not a directory: " + str(root))
    sys.exit(2)
hang = os.path.join(root, ".saipen", "HANG")
if os.path.exists(hang):
    time.sleep(60)
if os.path.exists(os.path.join(root, ".saipen", "INVALID")):
    print("Validation FAILED: 1 problem(s)")
    sys.exit(1)
print("Validation complete. Agent is conformant.")
sys.exit(0)
"#;

struct Harness {
    dir: PathBuf,
    actions: Arc<ActionManager>,
    tool: SaipenTool,
    root: saiwork_saipen::SaipenRoot,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        // Fake canonical install (the tool SAIWORK2 is *allowed* to invoke).
        let install = dir.join("saipen_install");
        std::fs::create_dir_all(install.join("tools")).unwrap();
        std::fs::write(install.join("VERSION"), "7.224.3-test\n").unwrap();
        std::fs::write(install.join("tools/saipen.py"), SAIPEN_PY).unwrap();
        std::fs::write(install.join("tools/validate.py"), VALIDATE_PY).unwrap();
        // Workspace with canonical SAIPEN pointing at the install.
        let saipen = dir.join(".saipen");
        std::fs::create_dir_all(&saipen).unwrap();
        write_state(&dir, &install);
        std::fs::write(
            saipen.join("BOARD.md"),
            "## DOING\n\n## TODO\n- [ ] T-9 [P2] something\n\n## DONE\n\n## BLOCKED\n",
        )
        .unwrap();

        let bus = EventBus::new();
        let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
        let actions = ActionManager::new(bus, supervisor);
        let root = validate_root(&dir)
            .unwrap()
            .expect("fixture must have SAIPEN");
        // The fake install is executable ONLY through the explicit operator
        // trust path (SAIWORK2_SAIPEN_HOME) — an opened workspace never
        // grants executable trust (TASK 24 §9). The env lock is held across
        // discover (the only env reader), then released.
        let _g = trusted_env(&install);
        let tool = SaipenTool::discover(&root).unwrap();
        // Keep the tempdir alive for the whole test (TempDir owned here).
        let _ = tmp.keep();
        Self {
            dir,
            actions,
            tool,
            root,
        }
    }

    async fn start(&self, action: SaipenAction) -> Result<ActionRecord, ActionError> {
        self.actions
            .start("ws-act", action, Some(self.tool.clone()), self.root.clone())
            .await
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.dir.join(".saipen").join(name)
    }

    fn set_marker(&self, name: &str) {
        std::fs::write(self.marker(name), "").unwrap();
    }

    fn clear_marker(&self, name: &str) {
        let _ = std::fs::remove_file(self.marker(name));
    }
}

fn write_state(dir: &Path, install: &Path) {
    let home = install.display().to_string();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        format!(
            "---\nphase: BUILD\ntask: \"T-7\"\nnext_action: \"saipen continue\"\nblocker: \"\"\nsaipen_version: 7\nschema_version: 3\nsaipen_home: \"{home}\"\n---\n"
        ),
    )
    .unwrap();
}

async fn wait_action(
    actions: &ActionManager,
    ws: &str,
    state: ActionState,
    timeout: Duration,
) -> ActionRecord {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(r) = actions.status(ws) {
            if r.state == state {
                return r;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for action {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---- probe / discovery ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tool_discovery_resolves_canonical_entrypoints_and_version() {
    let h = Harness::new();
    assert!(h.tool.cli.to_string_lossy().ends_with("saipen.py"));
    assert!(h.tool.validator.to_string_lossy().ends_with("validate.py"));
    assert_eq!(h.tool.version, "7.224.3-test");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_schema_version_blocks_actions() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let install = dir.join("install");
    std::fs::create_dir_all(install.join("tools")).unwrap();
    std::fs::write(install.join("VERSION"), "99.0\n").unwrap();
    std::fs::write(install.join("tools/saipen.py"), SAIPEN_PY).unwrap();
    std::fs::write(install.join("tools/validate.py"), VALIDATE_PY).unwrap();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    let home = install.display().to_string();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        format!("---\nphase: BUILD\nschema_version: 99\nsaipen_home: \"{home}\"\n---\n"),
    )
    .unwrap();
    let root = validate_root(dir).unwrap().unwrap();
    // Explicit trust: the schema gate (not the trust gate) must be the
    // reason actions are blocked.
    let _g = trusted_env(&install);
    let err = SaipenTool::discover(&root).unwrap_err();
    assert!(
        matches!(err, ActionError::UnsupportedVersion(_)),
        "newer schema must block actions, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_saipen_home_is_typed_not_available() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        "---\nphase: BUILD\nschema_version: 3\n---\n",
    )
    .unwrap();
    let root = validate_root(dir).unwrap().unwrap();
    let err = SaipenTool::discover(&root).unwrap_err();
    assert!(
        matches!(err, ActionError::NotAvailable(_)),
        "missing saipen_home → NotAvailable, got {err:?}"
    );
}

// Serialize every test that touches the process-global SAIWORK2_SAIPEN_HOME
// env var (parallel tests share env).
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Hold the env lock and mark `install` as the explicitly trusted SAIPEN
/// install for the duration of `SaipenTool::discover` (the only env reader).
fn trusted_env(install: &Path) -> std::sync::MutexGuard<'static, ()> {
    let g = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::env::set_var("SAIWORK2_SAIPEN_HOME", install.display().to_string());
    g
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untrusted_saipen_home_disables_executable_actions() {
    // P0: an opened repository must never point SAIWORK2 at arbitrary host
    // code via STATE saipen_home. A home OUTSIDE the project (and not an
    // explicitly trusted install) → typed UntrustedToolPath; no process can
    // be spawned because `start` is never reached with a tool.
    let _g = ENV_LOCK.lock().unwrap();
    std::env::remove_var("SAIWORK2_SAIPEN_HOME");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    // Malicious install: a sibling directory OUTSIDE the opened workspace.
    let external = tempfile::tempdir().unwrap();
    let ext = external.path();
    std::fs::create_dir_all(ext.join("tools")).unwrap();
    std::fs::write(ext.join("VERSION"), "7.224.3-test\n").unwrap();
    std::fs::write(ext.join("tools/saipen.py"), SAIPEN_PY).unwrap();
    std::fs::write(ext.join("tools/validate.py"), VALIDATE_PY).unwrap();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    let home = ext.display().to_string();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        format!("---\nphase: BUILD\nschema_version: 3\nsaipen_home: \"{home}\"\n---\n"),
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/BOARD.md"),
        "## DOING\n\n## TODO\n\n## DONE\n\n## BLOCKED\n",
    )
    .unwrap();
    let root = validate_root(dir).unwrap().unwrap();
    let err = SaipenTool::discover(&root).unwrap_err();
    assert!(
        matches!(err, ActionError::UntrustedToolPath(_)),
        "external saipen_home must be UntrustedToolPath, got {err:?}"
    );
    // The read-only SAIPEN view stays available: discovery is independent of
    // the executable-tool trust gate.
    assert!(
        matches!(
            saiwork_saipen::discover(dir).unwrap(),
            saiwork_saipen::Discovery::Present(_)
        ),
        "read-only discovery must still see the project SAIPEN"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repo_local_saipen_home_is_never_executable_without_explicit_trust() {
    // P0 (TASK 24 §9): an opened repository can ship `.saipen/STATE.md` plus
    // attacker-controlled `tools/saipen.py`/`validate.py` INSIDE the
    // workspace. Being inside the opened workspace is NOT a trust signal —
    // Status/Validate must not execute repo-local code until the install is
    // explicitly trusted via SAIWORK2_SAIPEN_HOME.
    let _g = ENV_LOCK.lock().unwrap();
    std::env::remove_var("SAIWORK2_SAIPEN_HOME");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    // Malicious repo-local install INSIDE the opened workspace.
    let local = dir.join("tools");
    std::fs::create_dir_all(local.join("tools")).unwrap();
    std::fs::write(local.join("VERSION"), "7.224.3-test\n").unwrap();
    std::fs::write(local.join("tools/saipen.py"), SAIPEN_PY).unwrap();
    std::fs::write(local.join("tools/validate.py"), VALIDATE_PY).unwrap();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    let home = local.display().to_string();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        format!("---\nphase: BUILD\nschema_version: 3\nsaipen_home: \"{home}\"\n---\n"),
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/BOARD.md"),
        "## DOING\n\n## TODO\n\n## DONE\n\n## BLOCKED\n",
    )
    .unwrap();
    let root = validate_root(dir).unwrap().unwrap();
    // No spawn can ever be reached: discovery itself is typed UntrustedToolPath.
    let err = SaipenTool::discover(&root).unwrap_err();
    assert!(
        matches!(err, ActionError::UntrustedToolPath(_)),
        "repo-local saipen_home must be untrusted without explicit trust, got {err:?}"
    );
    // The read-only SAIPEN view stays available.
    assert!(matches!(
        saiwork_saipen::discover(dir).unwrap(),
        saiwork_saipen::Discovery::Present(_)
    ));
    // Explicit trust makes the same repo-local install executable.
    std::env::set_var("SAIWORK2_SAIPEN_HOME", &home);
    let tool = SaipenTool::discover(&root).unwrap();
    assert_eq!(tool.version, "7.224.3-test");
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let actions = ActionManager::new(bus, supervisor);
    let rec = actions
        .start("ws-local", SaipenAction::Status, Some(tool), root)
        .await
        .unwrap();
    assert_eq!(rec.state, ActionState::Succeeded);
    std::env::remove_var("SAIWORK2_SAIPEN_HOME");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicitly_trusted_install_via_env_is_executable() {
    // SAIWORK2_SAIPEN_HOME is the explicit operator trust path: an external
    // install named there is probed/trusted and executable — unlike an
    // arbitrary project-selected path, which stays disabled.
    let _g = ENV_LOCK.lock().unwrap();
    std::env::remove_var("SAIWORK2_SAIPEN_HOME");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let external = tempfile::tempdir().unwrap();
    let ext = external.path();
    std::fs::create_dir_all(ext.join("tools")).unwrap();
    std::fs::write(ext.join("VERSION"), "7.224.3-test\n").unwrap();
    std::fs::write(ext.join("tools/saipen.py"), SAIPEN_PY).unwrap();
    std::fs::write(ext.join("tools/validate.py"), VALIDATE_PY).unwrap();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    let home = ext.display().to_string();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        format!("---\nphase: BUILD\nschema_version: 3\nsaipen_home: \"{home}\"\n---\n"),
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/BOARD.md"),
        "## DOING\n\n## TODO\n\n## DONE\n\n## BLOCKED\n",
    )
    .unwrap();
    let root = validate_root(dir).unwrap().unwrap();
    // Without explicit trust: untrusted.
    assert!(matches!(
        SaipenTool::discover(&root).unwrap_err(),
        ActionError::UntrustedToolPath(_)
    ));
    // With explicit operator trust: discoverable AND executable.
    std::env::set_var("SAIWORK2_SAIPEN_HOME", &home);
    let tool = SaipenTool::discover(&root).unwrap();
    assert_eq!(tool.version, "7.224.3-test");
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let actions = ActionManager::new(bus, supervisor);
    let rec = actions
        .start("ws-env", SaipenAction::Status, Some(tool), root)
        .await
        .unwrap();
    assert_eq!(rec.state, ActionState::Succeeded);
    std::env::remove_var("SAIWORK2_SAIPEN_HOME");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_trusted_home_makes_external_install_executable() {
    // T-080: the desktop shell persists a user-chosen trusted SAIPEN home and
    // passes it via `discover_with_trusted` (not the env contract). An install
    // trusted by NEITHER the env rules NOR the extra path stays untrusted.
    let _g = ENV_LOCK.lock().unwrap();
    std::env::remove_var("SAIWORK2_SAIPEN_HOME");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let external = tempfile::tempdir().unwrap();
    let ext = external.path();
    std::fs::create_dir_all(ext.join("tools")).unwrap();
    std::fs::write(ext.join("VERSION"), "7.224.3-test\n").unwrap();
    std::fs::write(ext.join("tools/saipen.py"), SAIPEN_PY).unwrap();
    std::fs::write(ext.join("tools/validate.py"), VALIDATE_PY).unwrap();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    let home = ext.display().to_string();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        format!("---\nphase: BUILD\nschema_version: 3\nsaipen_home: \"{home}\"\n---\n"),
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/BOARD.md"),
        "## DOING\n\n## TODO\n\n## DONE\n\n## BLOCKED\n",
    )
    .unwrap();
    let root = validate_root(dir).unwrap().unwrap();
    // No env trust, no extra trusted path -> untrusted (unchanged gate).
    assert!(matches!(
        SaipenTool::discover(&root).unwrap_err(),
        ActionError::UntrustedToolPath(_)
    ));
    // The persisted trusted path (from the app DB) makes it executable.
    let tool = SaipenTool::discover_with_trusted(&root, Some(ext)).unwrap();
    assert_eq!(tool.version, "7.224.3-test");
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let actions = ActionManager::new(bus, supervisor);
    let rec = actions
        .start("ws-trust", SaipenAction::Status, Some(tool), root)
        .await
        .unwrap();
    assert_eq!(rec.state, ActionState::Succeeded);
}

// ---- read actions ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_action_succeeds_and_completes() {
    let h = Harness::new();
    let rec = h.start(SaipenAction::Status).await.unwrap();
    assert_eq!(rec.state, ActionState::Succeeded);
    assert_eq!(rec.result.as_deref(), Some("ok"));
    // Terminal record retained for the bar (§57); not stuck as running.
    let after = h.actions.status("ws-act").unwrap();
    assert_eq!(after.state, ActionState::Succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_valid_is_domain_valid() {
    let h = Harness::new();
    let rec = h.start(SaipenAction::Validate).await.unwrap();
    assert_eq!(rec.state, ActionState::Succeeded);
    assert_eq!(rec.result.as_deref(), Some("valid"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_invalid_is_domain_result_not_action_failure() {
    // §41, §127: validate.py exit 1 = the project is not conformant — a
    // successful validation run with an "invalid" result, NOT a crashed/
    // failed action.
    let h = Harness::new();
    h.set_marker("INVALID");
    let rec = h.start(SaipenAction::Validate).await.unwrap();
    assert_eq!(
        rec.state,
        ActionState::Succeeded,
        "domain-invalid is a result, not an action failure"
    );
    assert_eq!(rec.result.as_deref(), Some("invalid"));
    assert_eq!(rec.error.as_deref(), Some("validation found issues"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_usage_error_is_infra_failure() {
    // §128: an infrastructure/usage failure must NOT look like invalid SAIPEN.
    let h = Harness::new();
    // Replace the fixture validator with one that exits 2 (usage error) —
    // the verified canonical exit for argument misuse.
    std::fs::write(
        h.dir.join("saipen_install/tools/validate.py"),
        "import sys\nprint(\"FAIL: unknown argument: --bogus\")\nsys.exit(2)\n",
    )
    .unwrap();
let rec = h
        .actions
        .start("ws-act", SaipenAction::Validate, Some(h.tool.clone()), h.root.clone())
        .await
        .unwrap();
    assert_eq!(
        rec.state,
        ActionState::Failed,
        "exit 2 is an infra failure, never a domain-invalid result"
    );
    assert_eq!(
        h.actions.status("ws-act").unwrap().state,
        ActionState::Failed
    );
    // And it must NOT record a semantic validation result (never project
    // INVALID from a broken validator).
    assert_eq!(
        h.actions
            .validation("ws-act", 0)
            .map(|(r, _)| r),
        None,
        "infra failure must not update the semantic validation verdict"
    );
}

// ---- exclusivity (backend authority, not UI) ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_start_is_rejected_busy() {
    let h = Harness::new();
    h.set_marker("HANG");
    let t = tokio::spawn({
        let actions = h.actions.clone();
        let tool = h.tool.clone();
        let root = h.root.clone();
        async move {
            actions
                .start("ws-act", SaipenAction::Status, Some(tool), root)
                .await
        }
    });
    wait_action(
        &h.actions,
        "ws-act",
        ActionState::Running,
        Duration::from_secs(5),
    )
    .await;
    // Second invocation while the first is still running → Busy (backend
    // authority — §34, §77, §119; the frontend disable is never trusted).
    let err = h.start(SaipenAction::Validate).await.unwrap_err();
    assert!(matches!(err, ActionError::Busy), "got {err:?}");
    h.clear_marker("HANG");
    let rec = t.await.unwrap().unwrap();
    assert_eq!(rec.state, ActionState::Succeeded);
}

// ---- cancel / stop ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_stops_running_action_and_clears_registry() {
    let h = Harness::new();
    h.set_marker("HANG");
    let t = tokio::spawn({
        let actions = h.actions.clone();
        let tool = h.tool.clone();
        let root = h.root.clone();
        async move {
            actions
                .start("ws-act", SaipenAction::Status, Some(tool), root)
                .await
        }
    });
    wait_action(
        &h.actions,
        "ws-act",
        ActionState::Running,
        Duration::from_secs(5),
    )
    .await;
    h.actions.cancel("ws-act").unwrap();
    let rec = wait_action(
        &h.actions,
        "ws-act",
        ActionState::Cancelled,
        Duration::from_secs(10),
    )
    .await;
    assert!(rec.error.is_some(), "cancel record carries the reason");
    h.clear_marker("HANG");
    let _ = t.await.unwrap();
    assert_eq!(
        h.actions.status("ws-act").unwrap().state,
        ActionState::Cancelled,
        "terminal record retained; nothing stuck running"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_is_control_not_spawn() {
    // §27–§28: no canonical `saipen stop` CLI exists — Stop cancels the
    // SAIWORK2-owned action process; it must never spawn a process itself.
    let h = Harness::new();
    h.set_marker("HANG");
    let t = tokio::spawn({
        let actions = h.actions.clone();
        let tool = h.tool.clone();
        let root = h.root.clone();
        async move {
            actions
                .start("ws-act", SaipenAction::Status, Some(tool), root)
                .await
        }
    });
    wait_action(
        &h.actions,
        "ws-act",
        ActionState::Running,
        Duration::from_secs(5),
    )
    .await;
    let stop_rec = h.start(SaipenAction::Stop).await.unwrap();
    assert_eq!(stop_rec.state, ActionState::Cancelling);
    wait_action(
        &h.actions,
        "ws-act",
        ActionState::Cancelled,
        Duration::from_secs(10),
    )
    .await;
    h.clear_marker("HANG");
    let _ = t.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_with_nothing_running_errors_typed() {
    let h = Harness::new();
    let err = h.actions.cancel("ws-act").unwrap_err();
    assert!(
        matches!(err, ActionError::Internal(_)),
        "no active action → typed error, got {err:?}"
    );
}

// ---- unsupported / view actions ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continue_is_unsupported_and_leaves_no_registry_entry() {
    // §3, §131: there is no canonical `saipen continue` CLI in v7.224.3 — the
    // bar must disable it honestly, never invent the command.
    let h = Harness::new();
    let err = h.start(SaipenAction::Continue).await.unwrap_err();
    assert!(matches!(err, ActionError::UnsupportedAction("continue")));
    assert!(h.actions.status("ws-act").is_none());
    let avail = h.actions.availability("ws-act");
    assert!(!avail.available.iter().any(|a| a == "continue"));
    assert!(avail.unsupported.iter().any(|a| a == "continue"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn view_actions_need_no_process_and_no_lock() {
    // §90–§92, §116: Board/Knowledge are read projections — no process, no
    // mutation lock, instant.
    let h = Harness::new();
    let board = h.start(SaipenAction::Board).await.unwrap();
    assert_eq!(board.state, ActionState::Succeeded);
    assert_eq!(board.result.as_deref(), Some("view"));
    let knowledge = h.start(SaipenAction::Knowledge).await.unwrap();
    assert_eq!(knowledge.state, ActionState::Succeeded);
    assert!(h.actions.status("ws-act").is_none());
}

// ---- timeout ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_bounds_long_action_and_clears_registry() {
    // §25, §129: no unbounded wait; a hung canonical tool is bounded and the
    // process is stopped (graceful → force via supervisor).
    let h = Harness::new();
    h.actions.set_timeout_for_test(Duration::from_millis(600));
    h.set_marker("HANG");
    let rec = h.start(SaipenAction::Status).await.unwrap();
    assert_eq!(rec.state, ActionState::Failed);
    assert_eq!(rec.result.as_deref(), Some("timeout"));
    assert_eq!(
        h.actions.status("ws-act").unwrap().state,
        ActionState::Failed
    );
    h.clear_marker("HANG");
}

// ---- validation stale policy ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_is_tied_to_snapshot_generation_and_goes_stale() {
    // §87–§88, §126: a valid result from snapshot A must not be shown as
    // current after the snapshot moved to B.
    let h = Harness::new();
    assert!(h.actions.validation("ws-act", 1).is_none(), "no result yet");
    h.actions.note_validation("ws-act", 7, "valid".into());
    let (res, stale) = h.actions.validation("ws-act", 7).unwrap();
    assert_eq!(res, "valid");
    assert!(!stale, "current for the validated generation");
    let (res, stale) = h.actions.validation("ws-act", 8).unwrap();
    assert_eq!(res, "valid");
    assert!(stale, "STALE after the snapshot moved");
    // Fresh validation for the new generation replaces the old.
    h.actions.note_validation("ws-act", 8, "invalid".into());
    let (res, stale) = h.actions.validation("ws-act", 8).unwrap();
    assert_eq!(res, "invalid");
    assert!(!stale);
}

// ---- cancellation race + termination confirmation (TASK 24 audit) ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_runner_receiver_is_not_lost_and_never_spawns() {
    // A cancel sent after the ActiveAction is registered but before the
    // runner subscribed is carried as the receiver's INITIAL value — and
    // `changed()` only fires on a CHANGE. The runner must check the current
    // value BEFORE spawning, or the cancellation is lost forever and a
    // process is spawned for an action nobody wants (TASK 24 §9).
    let h = Harness::new();
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus));
    let runner = saiwork_saipen::SupervisorActionRunner::new(supervisor.clone());
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    // The cancel lands before the runner ever receives the channel.
    cancel_tx.send(true).unwrap();
    let spec = h
        .tool
        .spec_for(SaipenAction::Status, &h.root, "act-race")
        .unwrap();
    let outcome = runner
        .run(spec, Duration::from_secs(10), cancel_rx)
        .await
        .expect("runner settles");
    assert!(outcome.cancelled, "pre-spawn cancel must be honoured");
    assert_eq!(
        supervisor.count(),
        0,
        "no process may be spawned for an already-cancelled action"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspace_stays_blocked_until_cancel_exit_is_confirmed() {
    // The process is stopped but the runner cannot confirm the exit yet: the
    // workspace action must stay Cancelling (never a definitive Cancelled),
    // and a second canonical action must be BUSY until the exit is real.
    use std::sync::{Condvar, Mutex as StdMutex};
    let h = Harness::new();
    let gate = Arc::new((StdMutex::new(false), Condvar::new()));
    let gate2 = gate.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered2 = entered.clone();
    h.actions.set_hooks_for_test(saiwork_saipen::actions::ActionHooks {
        before_exit_confirm: Some(Arc::new(move || {
            entered2.notify_one();
            let (lock, cv) = &*gate2;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
        })),
        force_exit_unconfirmed: false,
    });
    h.set_marker("HANG");
    let t = tokio::spawn({
        let actions = h.actions.clone();
        let tool = h.tool.clone();
        let root = h.root.clone();
        async move {
            actions
                .start("ws-act", SaipenAction::Status, Some(tool), root)
                .await
        }
    });
    wait_action(
        &h.actions,
        "ws-act",
        ActionState::Running,
        Duration::from_secs(5),
    )
    .await;
    h.actions.cancel("ws-act").unwrap();
    // The runner is parked before exit confirmation.
    entered.notified().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let status = h.actions.status("ws-act").unwrap();
    assert_eq!(
        status.state,
        ActionState::Cancelling,
        "no definitive Cancelled before the process exit is confirmed"
    );
    // A second canonical action cannot start while the old process may live.
    let err = h.start(SaipenAction::Validate).await.unwrap_err();
    assert!(matches!(err, ActionError::Busy), "got {err:?}");
    // Release: the exit is now confirmed → terminal Cancelled → free again.
    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    let rec = wait_action(
        &h.actions,
        "ws-act",
        ActionState::Cancelled,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(rec.state, ActionState::Cancelled);
    h.clear_marker("HANG");
    let _ = t.await.unwrap();
    let again = h.start(SaipenAction::Status).await.unwrap();
    assert_eq!(again.state, ActionState::Succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconfirmed_termination_never_fabricates_terminal_and_keeps_blocked() {
    // terminate_and_confirm cannot prove the exit: the manager must NOT
    // finish/remove the active action (a second canonical action would race
    // a possibly-live old process). The workspace stays blocked/degraded.
    let h = Harness::new();
    h.actions.set_hooks_for_test(saiwork_saipen::actions::ActionHooks {
        before_exit_confirm: None,
        force_exit_unconfirmed: true,
    });
    h.actions.set_timeout_for_test(Duration::from_millis(300));
    h.set_marker("HANG");
    let err = h.start(SaipenAction::Status).await.unwrap_err();
    assert!(
        matches!(err, ActionError::TerminationUnconfirmed(_)),
        "typed unconfirmed termination, got {err:?}"
    );
    // The active action was NOT removed — the workspace stays blocked.
    assert_eq!(
        h.actions.status("ws-act").unwrap().state,
        ActionState::Running,
        "active action retained until exit is confirmed or supervisor cleanup"
    );
    let err2 = h.start(SaipenAction::Validate).await.unwrap_err();
    assert!(
        matches!(err2, ActionError::Busy),
        "workspace must stay blocked, got {err2:?}"
    );
    h.clear_marker("HANG");
}

// ---- shutdown ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_rejects_new_actions() {
    let h = Harness::new();
    h.actions.shutdown();
    let err = h.start(SaipenAction::Status).await.unwrap_err();
    assert!(matches!(err, ActionError::ShuttingDown), "got {err:?}");
}

// ---- read-only guarantee ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actions_never_modify_canonical_files() {
    // §121–§123: SAIWORK2 action execution must be side-effect free on the
    // canonical tree (the fake tool only reads; SAIWORK2 adds nothing).
    let h = Harness::new();
    let state_before = std::fs::read(h.dir.join(".saipen/STATE.md")).unwrap();
    let board_before = std::fs::read(h.dir.join(".saipen/BOARD.md")).unwrap();
    let rec1 = h.start(SaipenAction::Status).await.unwrap();
    let rec2 = h.start(SaipenAction::Validate).await.unwrap();
    assert_eq!(rec1.state, ActionState::Succeeded);
    assert_eq!(rec2.state, ActionState::Succeeded);
    assert_eq!(
        std::fs::read(h.dir.join(".saipen/STATE.md")).unwrap(),
        state_before,
        "STATE.md must be untouched by action execution"
    );
    assert_eq!(
        std::fs::read(h.dir.join(".saipen/BOARD.md")).unwrap(),
        board_before
    );
    // No residue files in .saipen (STATE, BOARD, and the marker we created).
    let mut names: Vec<String> = std::fs::read_dir(h.dir.join(".saipen"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, vec!["BOARD.md", "STATE.md"]);
}

// ---- event facts ---- //

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn action_events_are_emitted_and_scoped() {
    let h = Harness::new();
    let bus = EventBus::new();
    let supervisor = Arc::new(ProcessSupervisor::new(bus.clone()));
    let actions = ActionManager::new(bus.clone(), supervisor);
    let mut sub = bus.subscribe();
    let root = validate_root(&h.dir).unwrap().unwrap();
    actions
        .start("ws-ev", SaipenAction::Status, Some(h.tool.clone()), root)
        .await
        .unwrap();
    let mut saw_started = false;
    let mut saw_completed = false;
    let mut saw_other_ws = false;
    let mut count = 0;
    while let Ok(Some(env)) = sub.try_recv() {
        count += 1;
        if count > 64 {
            break;
        }
        match env.event {
            Event::SaipenActionStarted {
                ref workspace_id, ..
            } => {
                if workspace_id.as_str() == "ws-ev" {
                    saw_started = true;
                } else {
                    saw_other_ws = true;
                }
            }
            Event::SaipenActionCompleted {
                ref workspace_id,
                result,
                ..
            } => {
                if workspace_id.as_str() == "ws-ev" {
                    saw_completed = true;
                    assert_eq!(result, "ok");
                } else {
                    saw_other_ws = true;
                }
            }
            _ => {}
        }
    }
    assert!(saw_started, "action_started emitted");
    assert!(saw_completed, "action_completed emitted");
    assert!(!saw_other_ws, "no cross-workspace event leakage");
}

// ---- REAL canonical validator smoke (donors/saipen v7.224.3) ----
// §217/§240: proof against the actual vendored tool, not only synthetic
// look-alikes. The validator is invoked read-only via std::process (PYTHONPATH
// needs the vendored `saipen_engine`), so this bypasses ActionManager
// deliberately — it proves the *contract* the manager encodes.

fn real_tools_dir() -> Option<std::path::PathBuf> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let repo = std::path::Path::new(&manifest).parent()?.parent()?;
    let tools = repo.join("donors/saipen/tools");
    tools.is_dir().then_some(tools)
}

fn run_real_validator(root: &Path) -> (Option<i32>, String) {
    let tools = real_tools_dir().expect("vendored donors/saipen must be present");
    let out = std::process::Command::new("python")
        .arg(tools.join("validate.py"))
        .arg("--project-root")
        .arg(root)
        .env("PYTHONPATH", &tools)
        .output()
        .unwrap();
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned() + &String::from_utf8_lossy(&out.stdout),
    )
}

#[test]
fn real_validator_accepts_conformant_fixture() {
    let Some(tools) = real_tools_dir() else {
        eprintln!("SKIP: donors/saipen not vendored");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    std::fs::create_dir_all(dir.join("knowledge")).unwrap();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        "---\r\nphase: BUILD\r\ntask: T-7\r\nnext_action: \"saipen continue\"\r\nblocker: \"\"\r\ntransition_from: PLAN\r\nsaipen_version: 7\r\nschema_version: 3\r\nlast_event: 1\r\nstyle_contract: ded-4ae736e4\r\nagent: claude\r\nrequires:\r\n  - filesystem\r\n  - git\r\n  - python\r\nmode: full\r\nexecution_intent: goal\r\ngoal_waves: 1\r\ngoal_tickets: 3\r\nupdated: \"2026-08-16T00:00:00Z\"\r\n---\r\n",
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/BOARD.md"),
        "## DOING\n- [/] T-7 [P3] build stuff | owner: claude | claim_time: 2026-08-16T00:00:00Z\n\n## TODO\n\n## DONE\n\n## BLOCKED\n",
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/LOG.md"),
        "# Log\n- 16.08.26 00:00 [E-1] [agent: claude] INIT: created .saipen\n",
    )
    .unwrap();
    let before = std::fs::read(dir.join(".saipen/STATE.md")).unwrap();
    let (exit, out) = run_real_validator(dir);
    assert_eq!(exit, Some(0), "conformant fixture must be valid: {out}");
    assert!(
        out.contains("Validation complete"),
        "expected the canonical conformance line, got: {out}"
    );
    // Read-only guarantee of the real tool too.
    assert_eq!(
        std::fs::read(dir.join(".saipen/STATE.md")).unwrap(),
        before,
        "validator must not modify canonical files"
    );
    let _ = tools;
}

#[test]
fn real_validator_reports_domain_invalid_with_exit_1() {
    let Some(tools) = real_tools_dir() else {
        eprintln!("SKIP: donors/saipen not vendored");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::create_dir_all(dir.join(".saipen")).unwrap();
    std::fs::write(
        dir.join(".saipen/STATE.md"),
        "---\nphase: BUILD\ntask: T-7\nnext_action: \"x\"\nblocker: \"\"\nschema_version: 3\nsaipen_version: 7\n---\n",
    )
    .unwrap();
    std::fs::write(
        dir.join(".saipen/BOARD.md"),
        "## DOING\n\n## TODO\n\n## DONE\n\n## BLOCKED\n",
    )
    .unwrap();
    let (exit, out) = run_real_validator(dir);
    assert_eq!(
        exit,
        Some(1),
        "domain-invalid fixture must exit 1 (not 2, not a crash): {out}"
    );
    let _ = tools;
}
