//! Tauri commands — the only IPC surface of the UI into the core (no
//! separate application server; spec §3). Commands are thin: they delegate to
//! core authorities and never duplicate logic.

use std::sync::Arc;

use saiwork_core::engine::EngineInfo;
use saiwork_core::{App, CoreError};
use serde::Serialize;
use tauri::{AppHandle, State};

type CmdResult<T> = Result<T, String>;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// Command guard (TASK 08 §32, §64–§65): mutating domain commands reject
/// work while BOOTING (AppNotReady) and after shutdown began (ShuttingDown).
/// Never silently queues; never waits.
fn require_ready(core: &App) -> CmdResult<()> {
    core.require_ready().map_err(err)
}

#[derive(Serialize)]
pub struct AppInfo {
    version: String,
    data_root: String,
    portable: bool,
    lifecycle: saiwork_core::AppState,
}

#[tauri::command]
pub fn app_info(core: State<'_, Arc<App>>) -> CmdResult<AppInfo> {
    Ok(AppInfo {
        version: saiwork_core::config::APP_VERSION.into(),
        data_root: core.config.data_root.display().to_string(),
        portable: core.config.portable,
        lifecycle: core.state(),
    })
}

#[tauri::command]
pub fn list_workspaces(
    core: State<'_, Arc<App>>,
) -> CmdResult<Vec<saiwork_core::workspace::Workspace>> {
    core.workspaces.list().map_err(err)
}

#[tauri::command]
pub fn get_active_workspace(core: State<'_, Arc<App>>) -> CmdResult<Option<String>> {
    core.workspaces.get_active_workspace().map_err(err)
}

#[tauri::command]
pub async fn set_active_workspace(
    core: State<'_, Arc<App>>,
    id: Option<String>,
    gen: Option<u64>,
) -> CmdResult<()> {
    // CORE-006: enforce lifecycle guard — active selection must not mutate
    // durable/runtime state after shutdown has begun.
    require_ready(&core)?;
    // Route through the SINGLE active-workspace commit boundary (validate id,
    // transfer the SAIPEN watcher, persist exactly, latest-wins by epoch)
    // instead of a bare durable write.
    // Async so the SAIPEN watcher spawn runs inside the Tokio reactor
    // (sync Tauri commands have no reactor and would degrade the watch).
    core.commit_active_workspace(id.as_deref(), gen).map_err(err)
}

#[tauri::command]
pub async fn open_workspace(
    mut path: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_core::workspace::Workspace> {
    require_ready(&core)?;
    // Tauri on Windows can return verbatim paths like \\?\V:\path
    if path.starts_with(r"\\?\") && path.chars().nth(5) == Some(':') {
        path = path[4..].to_string();
    }
    // Routes through App so the SAIPEN read service attaches its watcher
    // (TASK 14 §58) and owns the saipen.detected transition.
    core.open_workspace(std::path::Path::new(&path))
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn close_workspace(id: String, core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.close_workspace(&id).map_err(err)
}

#[tauri::command]
pub async fn forget_workspace(id: String, core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    // Routes through App-owned lifecycle (TASK 24 §9): rejects with a typed
    // Busy while an engine binding / active run / nonterminal queue work
    // requires the workspace; detaches SAIPEN + session metadata on success.
    core.forget_workspace(&id).await.map_err(err)
}

// ---- Engines ----

#[tauri::command]
pub fn list_engines(core: State<'_, Arc<App>>) -> CmdResult<Vec<EngineInfo>> {
    Ok(core.engines.list_info())
}

#[tauri::command]
pub async fn start_engine(
    engine_id: String,
    workspace_id: Option<String>,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    // The runtime's workspace binding is persisted at start: the engine runs
    // with the cwd of THIS workspace and must be restarted to rebind
    // (TASK 24 §9). Both the id (for validation) and the path (for cwd) are
    // passed through the context.
    let (wid, path) = match workspace_id {
        Some(wid) => (
            Some(wid.clone()),
            Some(core.workspaces.path_of(&wid).map_err(err)?),
        ),
        None => (None, None),
    };
    let ctx = core.engines.start_context(wid, path);
    core.engines.start(&engine_id, &ctx).await.map_err(err)
}

#[tauri::command]
pub async fn stop_engine(engine_id: String, core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.engines.stop(&engine_id).await.map_err(err)
}

#[tauri::command]
pub async fn list_models(
    engine_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<Vec<saiwork_core::engine::ModelInfo>> {
    require_ready(&core)?;
    let engine = core
        .engines
        .get(&engine_id)
        .ok_or_else(|| CoreError::Internal(format!("unknown engine {engine_id}")))
        .map_err(err)?;
    engine.list_models().await.map_err(err)
}

// ---- Model favorites (durable UI preference) ----
//
// The app is the authority (law 5: the UI never writes the DB directly);
// these commands are the only favorites IPC. The set is engine-independent
// because model ids are globally namespaced (`<provider>/<raw-key>`).

#[tauri::command]
pub fn get_model_favorites(core: State<'_, Arc<App>>) -> CmdResult<Vec<String>> {
    core.model_favorites().map_err(err)
}

#[tauri::command]
pub fn set_model_favorites(favorites: Vec<String>, core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.set_model_favorites(&favorites).map_err(err)
}

// ---- Generic durable UI settings (Phase B dock/layout persistence) ----
// Thin delegation to the app_settings k/v store (law 5: UI never owns the DB).
// Used for versioned, non-security, user UI preferences such as dock geometry.
/// The only keys the generic `set_setting` command may write (T-052). Each is
/// a genuine, non-security UI preference. `queue.paused` and
/// `ui.models.favorites` are intentionally EXCLUDED — they remain behind their
/// own typed owners and must never be reachable through this generic path.
/// `ui.engine.v1` IS included (T-078): modelCatalog persists the selected
/// engine/model through this command, and it was silently swallowed before —
/// the write rejected, the rejection ignored, engine state never survived a
/// restart.
const WRITABLE_SETTING_KEYS: &[&str] = &["ui.layout.v1", "ui.engine.v1"];

#[tauri::command]
pub fn get_setting(core: State<'_, Arc<App>>, key: String) -> CmdResult<Option<String>> {
    core.db.get_setting(&key).map_err(err)
}

#[tauri::command]
pub fn set_setting(core: State<'_, Arc<App>>, key: String, value: String) -> CmdResult<()> {
    // READY gating (T-052): settings are durable UI prefs; never written while
    // booting or shutting down.
    require_ready(&core)?;
    if !WRITABLE_SETTING_KEYS.contains(&key.as_str()) {
        return Err(format!(
            "set_setting: '{key}' is not a writable UI preference (queue/feature state has its own typed owner)"
        ));
    }
    core.db.set_setting(&key, &value).map_err(err)
}

// ---- Settings preset import (T-078) ----

/// Import a durable-UI settings preset from a user-picked file. The file
/// must be a .json preset (or a .zip containing one — ZIP import is not yet
/// supported; the command detects ZIP magic and returns a clear error).
/// Never JSON.parses a ZIP (T-078).
#[tauri::command]
pub fn import_preset(
    path: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_core::PresetImportSummary> {
    require_ready(&core)?;
    let bytes = std::fs::read(&path).map_err(|e| format!("preset file read failed: {e}"))?;
    core.import_preset(&bytes).map_err(err)
}


// ---- Sessions ----

#[tauri::command]
pub async fn create_session(
    engine_id: String,
    workspace_id: Option<String>,
    model: Option<String>,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_core::sessions::Session> {
    require_ready(&core)?;
    core.sessions
        .create(&engine_id, workspace_id.as_deref(), model.as_deref())
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn list_sessions(
    workspace_id: Option<String>,
    core: State<'_, Arc<App>>,
) -> CmdResult<Vec<saiwork_core::sessions::Session>> {
    // A durable read failure must propagate as a typed command error — never
    // substitute an empty list for a failed authoritative read (TASK 24 §9).
    Ok(core
        .sessions
        .list_recent(workspace_id.as_deref(), saiwork_core::sessions::RECENT_SESSION_CAP)
        .map_err(err)?)
}

/// Typed direct-send outcome (TASK 24 §9): the UI must distinguish a definite
/// rejection (safe to drop the pending user turn) from an unprovable outcome
/// (the run may still be executing — keep the turn marked UNCERTAIN, never
/// blind-resend).
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendPromptOutcome {
    Accepted { run_id: String },
    DefinitelyRejected { code: String, message: String },
    OutcomeUnknown { run_id: String, message: String },
}

#[tauri::command]
pub async fn send_prompt(
    session_id: String,
    workspace_id: Option<String>,
    engine_id: Option<String>,
    prompt: String,
    model: Option<String>,
    core: State<'_, Arc<App>>,
) -> CmdResult<SendPromptOutcome> {
    require_ready(&core)?;
    // The direct-send boundary (TASK 24 §9): the UI passes the workspace and
    // engine it currently shows; any mismatch with the session's own metadata
    // is rejected BEFORE a reservation or external call. The typed receipt
    // lets the UI keep/mark an uncertain user turn instead of removing it.
    // The app-level direct-send boundary (TASK 24 §9): the UI passes the
    // workspace/engine it currently shows (context mismatch is rejected
    // before any reservation/external call), and durable Queue UNKNOWN
    // ambiguity in the session's workspace blocks the send with a typed
    // WorkspaceOutcomeUnknown — direct Send can never bypass durable
    // ambiguity after a restart.
    let receipt = core
        .send_scoped_receipt(
            &session_id,
            workspace_id.as_deref(),
            engine_id.as_deref(),
            &prompt,
            model.as_deref(),
        )
        .await
        .map_err(err)?;
    Ok(match receipt {
        saiwork_core::engine::SendAcceptance::Accepted { run_id } => {
            SendPromptOutcome::Accepted { run_id }
        }
        saiwork_core::engine::SendAcceptance::DefinitelyRejected {
            run_id: _,
            code,
            message,
        } => SendPromptOutcome::DefinitelyRejected { code, message },
        saiwork_core::engine::SendAcceptance::OutcomeUnknown { run_id, message } => {
            SendPromptOutcome::OutcomeUnknown { run_id, message }
        }
    })
}

/// Read-only authoritative session history from the engine that owns the
/// session (TASK 24 §9): engines without a history capability return null —
/// the UI shows that history is unavailable instead of fabricating an empty
/// thread. Never a SQLite transcript mirror.
#[tauri::command]
pub async fn session_history(
    session_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<Option<Vec<saiwork_core::engine::SessionMessage>>> {
    Ok(core
        .sessions
        .session_history(&session_id)
        .await
        .map_err(err)?)
}

#[tauri::command]
pub async fn delete_session(
    session_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.delete_session(&session_id).await.map_err(err)
}

#[tauri::command]
pub async fn revert_last_turn(
    session_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.sessions.revert_last_turn(&session_id).await.map_err(err)
}

#[tauri::command]
pub async fn unrevert_session(
    session_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.sessions.unrevert(&session_id).await.map_err(err)
}

#[tauri::command]
pub async fn cancel_run(
    session_id: String,
    run_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.sessions
        .cancel(&session_id, &run_id)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn resolve_permission(
    session_id: String,
    request_id: String,
    allowed: bool,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.sessions
        .resolve_permission(&session_id, &request_id, allowed)
        .await
        .map_err(err)
}

/// AUDIT-CORE-002: answer/reject a pending user question through the typed
/// resolution surface (selected option labels, or an authoritative reject).
#[tauri::command]
pub async fn resolve_question(
    session_id: String,
    request_id: String,
    answers: Option<Vec<Vec<String>>>,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    let resolution = match answers {
        Some(labels) => saiwork_core::engine::QuestionResolution::Answers(labels),
        None => saiwork_core::engine::QuestionResolution::Rejected,
    };
    core.sessions
        .resolve_question(&session_id, &request_id, &resolution)
        .await
        .map_err(err)
}

// ---- Queue (TASK 13) ----

#[tauri::command]
pub fn queue_snapshot(core: State<'_, Arc<App>>) -> CmdResult<saiwork_queue::QueueSnapshot> {
    core.queue.snapshot().map_err(err)
}

/// Authoritative frontend-reconciliation ownership (TASK 24 §9): exact
/// `(session_id, run_id)` pairs for every session with a live or unknown
/// run. `frontend.reconcile` (and app reload) rebuild `state.running` from
/// this — Send stays disabled and Cancel targets the exact RunId without
/// waiting for incidental events.
#[tauri::command]
pub fn active_runs(core: State<'_, Arc<App>>) -> CmdResult<Vec<(String, String)>> {
    Ok(core.sessions.active_run_ids())
}

/// Authoritative pending-permission snapshot (W2-004): every open permission
/// request across all engines, keyed by exact session/run/request ownership.
/// `frontend.reconcile` (after a bounded-bus lag) rebuilds the UI permission
/// cards from this — a missed `permission.requested` state event is recoverable
/// and the user can still resolve the upstream wait exactly once.
#[tauri::command]
pub fn pending_permissions(
    core: State<'_, Arc<App>>,
) -> CmdResult<Vec<saiwork_core::engine::PendingPermissionInfo>> {
    Ok(core.pending_permissions())
}

/// AUDIT-CORE-002: authoritative pending-question snapshot — every open user
/// question across all engines, keyed by exact session/run/request ownership.
/// Same reconciliation contract as `pending_permissions`: a missed
/// `question.asked` state event is recoverable and the question stays
/// answerable exactly once.
#[tauri::command]
pub fn pending_questions(
    core: State<'_, Arc<App>>,
) -> CmdResult<Vec<saiwork_core::engine::PendingQuestionInfo>> {
    Ok(core.pending_questions())
}

#[tauri::command]
pub fn queue_get_item(
    id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_queue::QueueItem> {
    // Full durable item (exact payload) for editing/inspecting ONE row — the
    // snapshot carries only bounded payload previews (TASK 24 perf).
    core.queue.get_item(&id).map_err(err)
}

#[tauri::command]
pub async fn queue_enqueue(
    workspace_id: String,
    engine_id: String,
    session_id: Option<String>,
    session_mode: String,
    model: Option<String>,
    payload: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_queue::QueueItem> {
    require_ready(&core)?;
    // Strict session_mode (TASK 24 §9): only the exact canonical values are
    // accepted. A typo/stale frontend must never silently map to a different
    // durable semantic and create a new external session.
    let mode = match session_mode.as_str() {
        "new" => saiwork_queue::SessionMode::New,
        "existing" => saiwork_queue::SessionMode::Existing,
        other => {
            return Err(format!(
                "invalid session_mode '{other}': must be 'new' or 'existing'"
            ))
        }
    };
    core.enqueue_prompt(saiwork_queue::EnqueueRequest {
        workspace_id,
        engine_id,
        session_id,
        session_mode: mode,
        model,
        payload,
    })
    .await
    .map_err(err)
}

#[tauri::command]
pub async fn queue_edit(
    item_id: String,
    expected_revision: i64,
    payload: String,
    model: Option<String>,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_queue::QueueItem> {
    require_ready(&core)?;
    core.queue
        .edit(&item_id, expected_revision, &payload, model.as_deref())
        .map_err(err)
}

#[tauri::command]
pub async fn queue_cancel(item_id: String, core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.queue.cancel(&item_id).await.map_err(err)
}

/// Explicit, risk-confirmed abandonment of an UNKNOWN item (TASK 24 §9): the
/// external run may still be mutating the workspace — the UI must state that
/// risk before calling. Ordinary Cancel never fabricates cancellation.
#[tauri::command]
pub fn queue_resolve_unknown(
    item_id: String,
    expected_revision: i64,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.queue
        .resolve_unknown(&item_id, expected_revision)
        .map_err(err)
}

#[tauri::command]
pub fn queue_reorder(
    item_id: String,
    expected_revision: i64,
    new_index: usize,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.queue
        .reorder(&item_id, expected_revision, new_index)
        .map_err(err)
}

#[tauri::command]
pub fn queue_pause(core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.queue.pause().map_err(err)
}

#[tauri::command]
pub fn queue_resume(core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.queue.resume().map_err(err)
}

#[tauri::command]
pub fn queue_retry(
    item_id: String,
    expected_revision: i64,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    core.queue.retry(&item_id, expected_revision).map_err(err)
}

// ---- SAIPEN ----

#[tauri::command]
pub fn get_saipen(
    workspace_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<Option<saiwork_core::saipen::SaipenSnapshot>> {
    // Cached projection from the SaipenService (watcher-updated). Falls back
    // to a fresh bounded read when the workspace is not attached. Never a
    // raw path parameter from the frontend (TASK 14 §153).
    if let Some(snap) = core.saipen.snapshot(&workspace_id) {
        return Ok(Some(snap));
    }
    core.workspaces
        .path_of(&workspace_id)
        .map_err(err)
        .and_then(|p| match saiwork_core::saipen::snapshot_for_workspace(&p, 0) {
            Ok(Some(snap)) => Ok(Some(snap)),
            Ok(None) => {
                // The snapshot reported NotPresent (no .saipen), but the
                // directory may still be visible to the user via the Files
                // panel (verbatim path, case, or stale cache). Check the raw
                // filesystem directly and, if a .saipen entry exists, surface
                // it as a stale/invalid snapshot instead of "absent" so the
                // SAIPEN bar does not claim the folder is missing when it is
                // clearly there. This matches the user's expectation and
                // prevents the "no .saipen/state" false negative.
                let candidate = p.join(saiwork_core::saipen::SAIPEN_DIR);
                if candidate.exists() {
                    Ok(Some(saiwork_core::saipen::SaipenSnapshot {
                        generation: 1,
                        project: Some("ERROR".into()),
                        phase: Some("ERROR".into()),
                        task: Some("ERROR".into()),
                        next_action: Some("ERROR".into()),
                        read_at_ms: saiwork_core::saipen::now_ms(),
                        stale: true,
                        last_error: Some(format!(
                            ".saipen directory exists at {} but no valid SAIPEN state was found (STATE.md missing or unreadable)",
                            candidate.display()
                        )),
                        watch_status: saiwork_core::saipen::WatchStatus::Failed(
                            "present but invalid".into(),
                        ),
                        root: Some(p),
                        ..Default::default()
                    }))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Ok(Some(saiwork_core::saipen::SaipenSnapshot {
                generation: 1,
                project: Some("ERROR".into()),
                phase: Some("ERROR".into()),
                task: Some("ERROR".into()),
                next_action: Some("ERROR".into()),
                read_at_ms: saiwork_core::saipen::now_ms(),
                stale: true,
                last_error: Some(e.to_string()),
                watch_status: saiwork_core::saipen::WatchStatus::Failed("initial read failed".into()),
                root: Some(p),
                ..Default::default()
            })),
        })
}

// ---- SAIPEN actions (TASK 15) ----

/// Persisted, user-chosen explicitly-trusted SAIPEN install (T-080). Read by
/// `saipen_action_start`/`saipen_action_status` and offered as a one-click
/// trust in the SAIPENBAR; never reachable through the generic `set_setting`
/// surface (T-052) — this key is security-relevant, so it has its own typed
/// command.
const SETTING_TRUSTED_SAIPEN_HOME: &str = "saipen.trusted_home";

fn trusted_saipen_home(core: &App) -> CmdResult<Option<String>> {
    Ok(core
        .db
        .get_setting(SETTING_TRUSTED_SAIPEN_HOME)
        .map_err(err)?
        .filter(|v| !v.is_empty()))
}

/// Persist (or clear) the explicitly-trusted SAIPEN install (T-080). The user
/// confirms trust for the CURRENT workspace's SAIPEN home when executable
/// actions are disabled; this makes that install executable for Status/
/// Validate. The trusted path is resolved FROM the workspace's own STATE
/// (never a frontend-supplied arbitrary path) and validated as a real SAIPEN
/// install (canonical CLI + validator present) BEFORE it is persisted.
#[tauri::command]
pub fn set_saipen_trusted_home(
    workspace_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<()> {
    require_ready(&core)?;
    let path = core.workspaces.path_of(&workspace_id).map_err(err)?;
    let root = saiwork_saipen::validate_root(&path)
        .map_err(err)?
        .ok_or_else(|| saiwork_core::error::CoreError::Internal("no SAIPEN in workspace".into()))
        .map_err(err)?;
    let home = saiwork_saipen::saipen_home_of(&root).map_err(err)?;
    // Fail-closed validation: only a real SAIPEN install may be marked
    // trusted. A garbage path is refused here rather than persisted
    // and surfaced as a confusing UntrustedToolPath later.
    let cli = home.join(saiwork_saipen::SAIPEN_CLI);
    let validator = home.join(saiwork_saipen::SAIPEN_VALIDATOR);
    if !cli.is_file() {
        return Err(format!(
            "set_saipen_trusted_home: no canonical CLI at {}",
            cli.display()
        ));
    }
    if !validator.is_file() {
        return Err(format!(
            "set_saipen_trusted_home: no canonical validator at {}",
            validator.display()
        ));
    }
    core.db
        .set_setting(SETTING_TRUSTED_SAIPEN_HOME, home.to_string_lossy().as_ref())
        .map_err(err)
}

/// Clear the explicitly-trusted SAIPEN install (T-080).
#[tauri::command]
pub fn clear_saipen_trusted_home(core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.db.delete_setting(SETTING_TRUSTED_SAIPEN_HOME).map_err(err)
}

#[tauri::command]
pub async fn saipen_action_start(
    workspace_id: String,
    action: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_saipen::ActionRecord> {
    require_ready(&core)?;
    let action = action
        .parse::<saiwork_saipen::SaipenAction>()
        .map_err(|_| saiwork_core::error::CoreError::Internal(format!("unknown action: {action}")))
        .map_err(err)?;
    // Resolve the validated root from WorkspaceId — never a frontend path
    // (§160). The canonical tool comes from STATE saipen_home (§6).
    let path = core.workspaces.path_of(&workspace_id).map_err(err)?;
    let root = saiwork_saipen::validate_root(&path)
        .map_err(err)?
        .ok_or_else(|| saiwork_core::error::CoreError::Internal("no SAIPEN in workspace".into()))
        .map_err(err)?;
    let tool = if saiwork_saipen::kind_of(action) == saiwork_saipen::ActionKind::View {
        None
    } else {
        let trusted = trusted_saipen_home(&core)?;
        Some(
            saiwork_saipen::SaipenTool::discover_with_trusted(
                &root,
                trusted.as_deref().map(std::path::Path::new),
            )
            .map_err(err)?,
        )
    };
    // Bind a Validate result to the snapshot generation the validator
    // actually ran against — captured BEFORE the action, so a concurrent
    // writer during the run makes it stale, never falsely current (§87).
    let gen_before = core
        .saipen
        .snapshot(&workspace_id)
        .map(|s| s.generation)
        .unwrap_or(0);
    let record = core
        .saipen_actions
        .start(&workspace_id, action, tool, root)
        .await
        .map_err(err)?;
    // Post-action: one authoritative refresh (§19, §125) — the filesystem is
    // truth, never a manual patch.
    core.saipen.force_refresh(&workspace_id);
    if action == saiwork_saipen::SaipenAction::Validate {
        // Only a semantic verdict (valid/invalid) updates the validation
        // projection (TASK 24 §9). An infrastructure failure (exit 2+,
        // timeout, spawn failure) records no result string — the last
        // semantic outcome is retained independently and the bar shows the
        // failed action + error instead of a fake project INVALID.
        if let Some(result) = record.result.as_deref() {
            if matches!(result, "valid" | "invalid") {
                core.saipen_actions
                    .note_validation(&workspace_id, gen_before, result.to_string());
            }
        }
    }
    Ok(record)
}

#[tauri::command]
pub fn saipen_action_cancel(workspace_id: String, core: State<'_, Arc<App>>) -> CmdResult<()> {
    require_ready(&core)?;
    core.saipen_actions.cancel(&workspace_id).map_err(err)?;
    core.saipen.force_refresh(&workspace_id);
    Ok(())
}

#[tauri::command]
pub fn saipen_action_status(
    workspace_id: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_saipen::ActionStatusView> {
    let mut availability = core.saipen_actions.availability(&workspace_id);
    let running = core.saipen_actions.status(&workspace_id);
    let current_gen = core
        .saipen
        .snapshot(&workspace_id)
        .map(|s| s.generation)
        .unwrap_or(0);
    let validation = core.saipen_actions.validation(&workspace_id, current_gen);
    // T-080: surface the disabled-reason by attempting canonical-tool
    // discovery. This lets the SAIPENBAR show WHY actions are unavailable
    // (untrusted path, missing install) instead of silently offering buttons
    // that fail when clicked.
    if let Ok(path) = core.workspaces.path_of(&workspace_id) {
        if let Ok(Some(root)) = saiwork_saipen::validate_root(&path) {
            let trusted = trusted_saipen_home(&core).ok().flatten();
            match saiwork_saipen::SaipenTool::discover_with_trusted(
                &root,
                trusted.as_deref().map(std::path::Path::new),
            ) {
                Ok(_) => {}
                Err(e) => {
                    availability.disabled_reason = Some(e.to_string());
                }
            }
        }
    }
    Ok(saiwork_saipen::ActionStatusView {
        availability,
        running,
        validation_result: validation.clone().map(|(r, _)| r),
        validation_stale: validation.map(|(_, s)| s),
        snapshot_generation: current_gen,
    })
}

// ---- FILES (Phase C) ----

/// Read-only directory listing. Root is resolved from WorkspaceId — never a
/// frontend path (SECURITY.md workspace boundary; mirror of saipen §152–§153).
#[tauri::command]
pub fn files_list_dir(
    workspace_id: String,
    rel: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_files::DirListing> {
    let path = core.workspaces.path_of(&workspace_id).map_err(err)?;
    saiwork_files::list_dir(&path, &rel).map_err(err)
}

/// Read-only bounded head preview. Same root resolution + containment rules.
#[tauri::command]
pub fn files_read_preview(
    workspace_id: String,
    rel: String,
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_files::FilePreview> {
    let path = core.workspaces.path_of(&workspace_id).map_err(err)?;
    saiwork_files::read_preview(&path, &rel).map_err(err)
}

// ---- Lifecycle ----

/// User-initiated shutdown (menu/button). Runs the one canonical sequence and
/// then exits the shell (T-019): `app_shutdown` must leave no orphan process
/// and no lingering window. Both this path and the window-close handler converge
/// on `App::shutdown` (idempotent — TASK 08 §22, §25); here we additionally
/// perform the shell exit so the user-initiated action is fully terminal.
#[tauri::command]
pub async fn app_shutdown(app: AppHandle, core: State<'_, Arc<App>>) -> CmdResult<()> {
    let _report = core.shutdown("user requested").await;
    // Canonical shutdown complete; exit the shell (STOPPED → process exit).
    app.exit(0);
    #[allow(unreachable_code)]
    Ok(())
}

// ---- Diagnostics ----

#[tauri::command]
pub fn get_diagnostics(
    core: State<'_, Arc<App>>,
) -> CmdResult<saiwork_core::app::DiagnosticsSnapshot> {
    Ok(core.snapshot())
}
