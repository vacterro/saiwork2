//! SAIPEN read integration bridge (TASK 14).
//!
//! The canonical reader, parser, path security, watcher, and snapshot model
//! live in `saiwork-saipen` (one crate, not in core — TASK 14 §5). This
//! module re-exports the wire type and the one-shot helpers used by the
//! desktop commands, keeping core free of parser/watcher internals.
//!
//! Authority: SAIPEN is canonical; SAIWORK2 reads and projects. No writes.

pub use saiwork_saipen::{
    BoardSummary, Discovery, SaipenDescriptor, SaipenError, SaipenRoot, SaipenSnapshot,
    SaipenSummary, WatchStatus, SAIPEN_DIR, STATE_FILE,
};

pub use saiwork_saipen::reader::{discover, now_ms, read_snapshot, snapshot_for_workspace, summarize};
