//! SAIWORK2 desktop shell (Tauri 2) — the one desktop runtime (law 9).
//!
//! This shell is deliberately thin: it resolves the data root, bootstraps
//! logging, runs the core bootstrap (`saiwork-core`), registers engines,
//! forwards normalized events to the frontend, and wires window-close to the
//! ordered shutdown sequence. All system/runtime resources belong to the
//! Rust core, never to the UI (law 4/5).
//!
//! Startup order (TASK 08 §8): resolve data root → logging → single-instance
//! (plugin init) → storage → services → READY → window content. Core
//! bootstrap runs inside the Tauri `setup` hook, i.e. **after** the
//! single-instance plugin acquired the process-level mutex: a second instance
//! exits before it ever opens the database, so two DB authorities can never
//! coexist (§15, §111). A second instance relays intent and exits.

mod commands;

use std::sync::Arc;

use serde_json::json;

use engine_deepseek_harness::{HarnessAdapter, HarnessConfig};
// FakeEngine is DEV/TEST-ONLY infrastructure (TASK 18 §64, T-020): it must
// NEVER be registerable in a release build. The import is gated so the symbol
// does not even exist in release, and the registration below is additionally
// `cfg(debug_assertions)`-gated — README/SECURITY/ADR require it to be
// unreachable for users regardless of the opt-in env var.
#[cfg(debug_assertions)]
use engine_fake::FakeEngine;
use engine_generic_cli::GenericCliConfig;
use engine_opencode::{OpenCodeAdapter, OpenCodeConfig};
use saiwork_core::App;
use saiwork_events::bus::Subscription;
use tauri::{Emitter, Manager, WindowEvent};
use tracing::{error, info, warn};

pub fn run() {
    // Data root first (startup order §8 step 3), so logging lands under the
    // canonical root (<data-root>/logs, TASK 08 §41) and never CWD.
    let config = match saiwork_core::AppConfig::resolve() {
        Ok(cfg) => cfg,
        Err(e) => {
            let _ = rfd::MessageDialog::new()
                .set_title("SAIWORK2 — cannot resolve data root")
                .set_description(format!("{e}"))
                .set_level(rfd::MessageLevel::Error)
                .show();
            std::process::exit(1);
        }
    };

    // Logging + panic hook before any service work (TASK 08 §8 step 4, §44).
    // The guard is held for the whole run() (process lifetime): dropping it
    // would stop the non-blocking file writer.
    let logging_guard = saiwork_core::logging::init(&config);
    saiwork_core::logging::install_panic_hook();
    let _ = &logging_guard;

    // The event-forwarder captures the bus; it is set during setup.
    let bus_placeholder = std::sync::Mutex::new(None::<saiwork_events::EventBus>);
    let _ = &bus_placeholder;

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_single_instance::init(
            move |app, _args, _cwd| {
                // A second launch only activates the first instance's window
                // (TASK 08 §14–§18): it never opens a second authority or a
                // second DB owner. If the primary is gone the OS mutex is
                // released automatically and a fresh launch acquires it
                // (stale-state safety). No typed-intent relay exists yet —
                // CLI args are intentionally ignored until a consumer needs
                // them (dead emits would violate the no-dead-code law).
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.unminimize();
                    let _ = window.set_focus();
                }
            },
        ))
        .invoke_handler(tauri::generate_handler![
            commands::app_info,
            commands::list_workspaces,
            commands::get_active_workspace,
            commands::set_active_workspace,
            commands::open_workspace,
            commands::close_workspace,
            commands::forget_workspace,
            commands::list_engines,
            commands::start_engine,
            commands::stop_engine,
            commands::list_models,
            commands::get_model_favorites,
            commands::set_model_favorites,
            commands::create_session,
            commands::list_sessions,
            commands::send_prompt,
            commands::cancel_run,
            commands::session_history,
            commands::delete_session,
            commands::revert_last_turn,
            commands::unrevert_session,
            commands::resolve_permission,
            commands::resolve_question,
            commands::get_saipen,
            commands::saipen_action_start,
            commands::saipen_action_cancel,
            commands::saipen_action_status,
            commands::set_saipen_trusted_home,
            commands::clear_saipen_trusted_home,
            commands::get_diagnostics,
            commands::get_setting,
            commands::set_setting,
            commands::import_preset,
            commands::app_shutdown,
            commands::queue_snapshot,
            commands::queue_get_item,
            commands::active_runs,
            commands::pending_permissions,
            commands::pending_questions,
            commands::queue_enqueue,
            commands::queue_edit,
            commands::queue_cancel,
            commands::queue_resolve_unknown,
            commands::queue_reorder,
            commands::queue_pause,
            commands::queue_resume,
            commands::queue_retry,
            commands::files_list_dir,
            commands::files_read_preview,
        ])
        .setup(move |app| {
            // Core bootstrap inside a tokio context (spawns background tasks).
            // Runs AFTER the single-instance plugin acquired the mutex, so a
            // second instance has already relayed and exited by now — it
            // never opened the database (TASK 08 §15, §111). Required-service
            // failure (storage/migration/…) is fail-closed: the app never
            // enters READY and no engine/process ever starts (§10–§11).
            let core = match tauri::async_runtime::block_on(async { App::bootstrap_with(config) })
            {
                Ok(core) => core,
                Err(e) => {
                    error!(error = %e, "core bootstrap failed");
                    let _ = rfd::MessageDialog::new()
                        .set_title("SAIWORK2 — startup failed")
                        .set_description(format!(
                            "Storage initialization failed.\nApplication cannot enter normal mode.\n\nReason: {e}\nData path: {}",
                            core_data_root_hint(&e)
                        ))
                        .set_level(rfd::MessageLevel::Error)
                        .show();
                    std::process::exit(1);
                }
            };
            core.set_logging_info(logging_guard.info.clone());

            // Register adapters. OpenCode is the canonical user default
            // (TASK 24 §9): it registers first and the deterministic registry
            // order keeps it at index 0 whenever present.
            core.engines
                .register(Arc::new(OpenCodeAdapter::new(OpenCodeConfig::default())));
            // FakeEngine is dev/test-only infrastructure (TASK 18 §64, T-020)
            // and must NEVER become a normal user engine. It is registered ONLY
            // in debug builds (`cfg(debug_assertions)`) AND behind an explicit
            // opt-in (SAIWORK2_ENABLE_FAKE_ENGINE=1). In a release build the
            // symbol is absent and this branch is compiled out entirely, so the
            // env var can never enable it for users. The registry order puts it
            // last, so it is never the default even when registered.
            #[cfg(debug_assertions)]
            if std::env::var("SAIWORK2_ENABLE_FAKE_ENGINE").as_deref() == Ok("1") {
                core.engines.register(Arc::new(FakeEngine::new()));
                info!("fake engine registered (debug-only + explicit SAIWORK2_ENABLE_FAKE_ENGINE=1 opt-in)");
            }
            // Generic CLI (TASK 17): registered only when explicitly
            // configured via SAIWORK2-owned env vars — never from project
            // files, never model-controlled (§44–§45). Absent = not
            // registered; malformed values surface a precise config error.
            if let Some(cfg) = GenericCliConfig::from_env() {
                match cfg {
                    Ok(cfg) => {
                        core.engines
                            .register(Arc::new(engine_generic_cli::GenericCliEngine::new(cfg)));
                        info!("generic-cli engine configured via environment");
                    }
                    Err(message) => {
                        error!(error = %message, "generic-cli engine configuration invalid; engine not registered");
                    }
                }
            }
            // DeepSeek Harness (TASK 20 + TASK 21 + TASK 23): adapter registered
            // only when explicitly configured via SAIWORK2-owned env vars. TASK 21
            // enables sessions/streaming/cancel/tools/permissions (all
            // fixture-proven); the capability-driven UI offers the same
            // generic chat/tool/permission surface as OpenCode and marks the
            // engine experimental (EngineIdentity.experimental). resume/models
            // stay false, but the generic EnginePort queue path (TASK 23) IS
            // enabled — QueueManager dispatches to it through the same durable
            // queue authority as every other engine. Absent = not registered;
            // malformed values surface a precise config error.
            if let Some(cfg) = HarnessConfig::from_env() {
                match cfg {
                    Ok(cfg) => {
                        core.engines
                            .register(Arc::new(HarnessAdapter::new(cfg)));
                        info!("deepseek-harness engine configured via environment");
                    }
                    Err(message) => {
                        error!(error = %message, "deepseek-harness engine configuration invalid; engine not registered");
                    }
                }
            }
            info!(engines = core.engines.count(), "engines registered");

            let bus = core.bus.clone();
            *bus_placeholder.lock().expect("bus placeholder poisoned") = Some(bus.clone());
            app.manage(core);

            // Forward every canonical event to the frontend (bounded bus),
            // with SHELL-ONLY delta coalescing (`saiwork_events::coalescing`,
            // tested in saiwork-events). The canonical EventBus is untouched;
            // the frontend store still applies its own batching, and this
            // coalescing is never a durable authority.
            let app_handle = app.handle().clone();
            let subscription: Subscription = bus.subscribe();
            let emit_handle = app_handle.clone();
            let lag_handle = app_handle.clone();
            tauri::async_runtime::spawn(saiwork_events::coalescing::forward(
                subscription,
                move |envelope| emit_handle.emit("event", &envelope).map_err(|_| ()),
                move |skipped| {
                    warn!(skipped, "event forwarder lagged; frontend will reconcile");
                    // The frontend missed `skipped` events; ask it to
                    // re-snapshot authoritative state instead of staying
                    // stale. The reconcile event itself is best-effort.
                    let _ = lag_handle.emit("event", json!({ "type": "frontend.reconcile", "reason": "lag" }));
                },
            ));
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                // Own the shutdown sequence: prevent default close, run the
                // canonical shutdown (one sequence, idempotent), then exit.
                api.prevent_close();
                let app_handle = window.app_handle().clone();
                tauri::async_runtime::spawn(async move {
                    if let Some(core) = app_handle.try_state::<Arc<App>>() {
                        let core = core.inner().clone();
                        let report = core.shutdown("window closed").await;
                        info!(
                            outcome = report.outcome,
                            shutdown_ms = report.shutdown_ms,
                            forced = report.forced_processes.len(),
                            "application shutdown"
                        );
                    }
                    app_handle.exit(0);
                });
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running SAIWORK2");
}

/// Best-effort user-safe hint for the startup-failure dialog: the data root
/// we tried (already resolved by the shell), or nothing.
fn core_data_root_hint(_e: &saiwork_core::CoreError) -> String {
    // The shell resolved the config before bootstrap; bootstrap_with can only
    // fail on storage/service init, so re-resolve for the hint.
    saiwork_core::AppConfig::resolve()
        .map(|c| c.data_root.display().to_string())
        .unwrap_or_else(|_| "<unresolved>".into())
}
