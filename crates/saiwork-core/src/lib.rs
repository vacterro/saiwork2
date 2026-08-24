//! SAIWORK2 core — orchestration layer.
//!
//! The core is the only authority over: workspaces, sessions (metadata),
//! engine registry, durable state (via `saiwork-storage`), child processes
//! (via `saiwork-process`), and the normalized event bus. The UI is a
//! projection of core state (law 23).

pub mod app;
pub mod config;
pub mod engine;
pub mod error;
pub mod logging;
pub mod queue_port;
pub mod saipen;
pub mod sessions;
pub mod workspace;

pub use app::{App, AppState, DiagnosticsSnapshot, PresetImportSummary, ShutdownReport, StartupTimings};
pub use config::AppConfig;
pub use engine::{EngineAdapter, EngineCapabilities, EngineHealth, EngineIdentity, EngineRegistry};
pub use error::CoreError;
pub use queue_port::QueueEnginePort;
