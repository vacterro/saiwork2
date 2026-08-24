//! SAIWORK2 read-only canonical SAIPEN integration (TASK 14).
//!
//! Invariant: **SAIPEN remains authoritative; SAIWORK2 is a reader and
//! projection.** This crate performs zero canonical writes, spawns zero
//! SAIPEN commands, holds no shadow database, and never re-implements the
//! canonical validator (TASK 14 §1–§7, §228).
//!
//! Modules:
//! - `model` — typed discovery/snapshot/error model with UNKNOWN semantics
//! - `parser` — strict canonical STATE/BOARD parsers (verified contract)
//! - `paths` — workspace-boundary security (symlink/junction escape)
//! - `reader` — bounded, side-effect-free reads + snapshot consistency
//! - `watcher` — one notify-based watcher per root, debounce/coalesce,
//!   overflow recovery, generation-tagged
//! - `service` — per-workspace service owning watchers + cached projection
//! - `actions` — canonical tool client + action manager (TASK 15)

pub mod actions;
pub mod model;
pub mod parser;
pub mod paths;
pub mod reader;
pub mod service;
pub mod watcher;

#[cfg(feature = "failpoints")]
pub mod test_hooks;

pub use actions::{
    kind_of, saipen_home_of, ActionAvailability, ActionError, ActionKind, ActionManager,
    ActionRecord, ActionState, ActionStatusView, SaipenAction, SaipenTool, SAIPEN_CLI,
    SAIPEN_VALIDATOR, SupervisorActionRunner,
};
pub use model::{
    BoardSummary, Discovery, SaipenDescriptor, SaipenError, SaipenRoot, SaipenSnapshot,
    SaipenSummary, WatchStatus, BOARD_FILE, FRONTMATTER_DELIM, LOG_FILE, SAIPEN_DIR, STATE_FILE,
};
pub use parser::{parse_board, parse_state};
pub use paths::{validate_file_in_root, validate_root};
pub use reader::{discover, read_snapshot, snapshot_for_workspace, summarize};
pub use service::SaipenService;
pub use watcher::{spawn as spawn_watcher, WatchHandle, WatcherConfig};
