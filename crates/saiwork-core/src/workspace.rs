//! WorkspaceManager (spec §36).
//!
//! Workspace identity is the **DB-owned opaque `WorkspaceId`** minted by
//! `Db::upsert_workspace` (a UUID, unique and durable for the lifetime of the
//! row). There is exactly ONE identity rule: the DB owns it — reopening an
//! existing workspace retains its stored id, and forget/recreate mints a new
//! one. Workspace ids are never path-derived; a canonicalized path maps to an
//! id only through the `workspaces` table. The manager owns: open/close,
//! recent list, identity, Git detection, SAIPEN detection, and (in later
//! phases) watchers and cleanup.

use std::path::{Path, PathBuf};

use saiwork_events::{Event, EventBus};
use saiwork_storage::{Db, WorkspaceRow};
use serde::Serialize;

use crate::error::CoreError;
use crate::saipen::SaipenSummary;

/// Detect whether `path` is inside a Git working tree (W2-010).
/// Returns true for:
/// - An ordinary `.git` directory (bare or non-bare repository)
/// - A `.git` file containing a valid `gitdir:` pointer (linked worktree
///   or submodule)
/// Returns false for: missing `.git`, malformed `.git` files, or any
/// other unexpected state. The `gitdir:` target is checked for existence
/// but not fully validated — a missing target is treated as non-Git.
fn detect_git_worktree(path: &Path) -> bool {
    let git_path = path.join(".git");
    match std::fs::metadata(&git_path) {
        Ok(md) if md.is_dir() => true,
        Ok(_) => {
            // `.git` is a file: check for a valid `gitdir:` pointer
            // (linked worktree or submodule).
            match std::fs::read_to_string(&git_path) {
                Ok(content) => {
                    // A valid gitdir pointer starts with "gitdir: " and the
                    // target path should exist.
                    if let Some(rest) = content.strip_prefix("gitdir: ") {
                        let target = rest.trim_end();
                        // Resolve relative paths against the working tree.
                        let resolved = if std::path::Path::new(target).is_absolute() {
                            std::path::PathBuf::from(target)
                        } else {
                            path.join(target)
                        };
                        resolved.exists()
                    } else {
                        false
                    }
                }
                Err(_) => false,
            }
        }
        Err(_) => false,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Workspace {
    pub id: String,
    pub path: PathBuf,
    pub name: String,
    pub has_git: bool,
    /// Cheap SAIPEN presence/version summary for the sidebar badge (TASK 24
    /// perf): STATE discovery only — zero BOARD reads, no consistency
    /// pipeline, no full snapshot. The live, watcher-updated FULL projection
    /// is the `SaipenService` cache (`get_saipen`); `attach` is its sole
    /// owner for an active workspace. SAIPEN is authoritative.
    pub saipen: Option<SaipenSummary>,
    pub last_opened_at: Option<i64>,
}

pub struct WorkspaceManager {
    db: Db,
    bus: EventBus,
}

impl WorkspaceManager {
    pub fn new(db: Db, bus: EventBus) -> Self {
        Self { db, bus }
    }

    /// Open a workspace: canonicalize, validate, persist, detect Git and
    /// SAIPEN, publish `workspace.opened`.
    pub async fn open(&self, path: &Path) -> Result<Workspace, CoreError> {
        if !path.is_dir() {
            return Err(CoreError::NotADirectory {
                path: path.to_path_buf(),
            });
        }
        let canonical = std::fs::canonicalize(path).map_err(|source| CoreError::Canonicalize {
            path: path.to_path_buf(),
            source,
        })?;
        let name = canonical
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| canonical.to_string_lossy().into_owned());

        // CORE-024: upsert_workspace already sets last_opened_at on both
        // INSERT and ON CONFLICT UPDATE — a second touch_workspace would add
        // no required state transition and creates a failure window where a
        // successful upsert reports failure due to a redundant second write.
        let row = self
            .db
            .upsert_workspace(&canonical.to_string_lossy(), &name)?;

        let has_git = detect_git_worktree(&canonical);
        // Cheap STATE-only summary for the sidebar badge (TASK 24 perf). The
        // FULL snapshot is read exactly once by `SaipenService::attach` when
        // the workspace is opened via `App::open_workspace` — never twice.
        // The SaipenDetected transition event is owned by the SaipenService
        // (TASK 14 §52), not here.
        let saipen = match saiwork_saipen::summarize(&canonical) {
            Ok(Some(s)) => Some(s),
            Ok(None) => None,
            Err(_) => Some(SaipenSummary {
                schema_version: None,
                saipen_version: None,
                project: Some("ERROR".into()),
            }),
        };
        if has_git {
            self.bus.publish(Event::GitChanged {
                workspace_id: row.id.clone().into(),
            });
        }
        self.bus.publish(Event::WorkspaceOpened {
            workspace_id: row.id.clone().into(),
            path: canonical.to_string_lossy().into_owned(),
        });

        Ok(Workspace {
            id: row.id,
            path: canonical,
            name,
            has_git,
            saipen,
            last_opened_at: row.last_opened_at,
        })
    }

    /// Close a workspace: persist recency, publish `workspace.closed`.
    pub fn close(&self, id: &str) -> Result<(), CoreError> {
        // Indexed point lookup (TASK 24 perf): one query on the primary key,
        // never a materialized list + scan.
        let row = self
            .db
            .get_workspace(id)?
            .ok_or_else(|| CoreError::WorkspaceNotFound(id.into()))?;
        self.db.touch_workspace(&row.id)?;
        self.bus.publish(Event::WorkspaceClosed {
            workspace_id: id.into(),
        });
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<Workspace>, CoreError> {
        let rows = self.db.list_workspaces()?;
        Ok(rows.into_iter().map(|r| self.row_to_workspace(r)).collect())
    }

    pub fn recent(&self, limit: usize) -> Result<Vec<Workspace>, CoreError> {
        Ok(self.list()?.into_iter().take(limit).collect())
    }

    pub fn forget(&self, id: &str) -> Result<(), CoreError> {
        self.db.delete_workspace(id)?;
        Ok(())
    }

    pub fn get_active_workspace(&self) -> Result<Option<String>, CoreError> {
        Ok(self.db.get_active_workspace()?)
    }

    pub fn set_active_workspace(&self, id: Option<&str>) -> Result<(), CoreError> {
        Ok(self.db.set_active_workspace(id)?)
    }

    /// Canonical path of a known workspace (for SAIPEN reads etc.).
    pub fn path_of(&self, id: &str) -> Result<PathBuf, CoreError> {
        // Indexed point lookup (TASK 24 perf): one query on the primary key,
        // never a materialized list + scan.
        self.db
            .get_workspace(id)?
            .map(|w| {
                let mut p = w.path;
                if p.starts_with(r"\\?\") && p.chars().nth(5) == Some(':') {
                    p = p[4..].to_string();
                }
                PathBuf::from(p)
            })
            .ok_or_else(|| CoreError::WorkspaceNotFound(id.into()))
    }

    fn row_to_workspace(&self, mut row: WorkspaceRow) -> Workspace {
        if row.path.starts_with(r"\\?\") && row.path.chars().nth(5) == Some(':') {
            row.path = row.path[4..].to_string();
        }
        let path = PathBuf::from(&row.path);
        // Status truth (law 59): listed workspaces report current detection
        // state from the filesystem — a bounded stat per known workspace at
        // refresh time, never a poll loop.
        let has_git = detect_git_worktree(&path);
        // Cheap STATE-only presence/version summary (TASK 24 perf): listing N
        // workspaces performs ZERO BOARD reads and no consistency pipeline.
        // The full projection belongs to the SaipenService cache.
        let saipen = match saiwork_saipen::summarize(&path) {
            Ok(Some(s)) => Some(s),
            Ok(None) => None,
            Err(_) => Some(SaipenSummary {
                schema_version: None,
                saipen_version: None,
                project: Some("ERROR".into()),
            }),
        };
        Workspace {
            id: row.id,
            path,
            name: row.name,
            has_git,
            saipen,
            last_opened_at: row.last_opened_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_rejects_non_directory() {
        let db = Db::open_in_memory().unwrap();
        let bus = EventBus::new();
        let mgr = WorkspaceManager::new(db, bus);
        let err = mgr
            .open(Path::new("definitely-not-a-real-path-xyz"))
            .await
            .unwrap_err();
        assert!(matches!(err, CoreError::NotADirectory { .. }));
    }

    #[tokio::test]
    async fn open_and_list_use_cheap_state_only_summary() {
        // TASK 24 perf: opening/listing a SAIPEN workspace must perform ZERO
        // BOARD reads. A workspace with a BROKEN BOARD still opens and lists
        // with the SAIPEN badge (STATE discovery suffices); the full
        // snapshot — which would have been read twice per open before —
        // fails on this fixture, proving the row never touched BOARD.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        std::fs::write(
            dir.path().join(".saipen/STATE.md"),
            "---\nphase: BUILD\ntask: T-1\nschema_version: 3\nsaipen_version: 7\n---\n",
        )
        .unwrap();
        // An oversized BOARD: the full snapshot rejects it as a bounded read
        // error, while the STATE-only summary never touches BOARD at all.
        let huge_board = "x".repeat(9 * 1024 * 1024);
        std::fs::write(dir.path().join(".saipen/BOARD.md"), huge_board).unwrap();

        let db = Db::open_in_memory().unwrap();
        let bus = EventBus::new();
        let mgr = WorkspaceManager::new(db, bus);
        let ws = mgr.open(dir.path()).await.expect("open");
        let summary = ws.saipen.expect("badge from STATE alone");
        assert_eq!(summary.schema_version.as_deref(), Some("3"));
        assert_eq!(summary.saipen_version.as_deref(), Some("7"));
        // The old path (full snapshot) is impossible on this fixture — proof
        // the row is STATE-only.
        assert!(saiwork_saipen::snapshot_for_workspace(dir.path(), 0).is_err());

        let listed = mgr.list().expect("list");
        assert_eq!(listed.len(), 1);
        assert!(listed[0].saipen.is_some(), "listed badge also STATE-only");
    }
}
