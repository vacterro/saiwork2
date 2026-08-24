//! Path boundary security (TASK 14 §10–§13, §145–§149).
//!
//! Every SAIPEN path is validated before use: normalize → resolve
//! (following symlinks/junctions) → verify the **resolved** target stays
//! inside the canonical workspace root. Naive string prefix checks
//! (`candidate.starts_with(root_string)`) are never a security boundary
//! (§12). A symlink/junction inside `.saipen` pointing outside is rejected.
//!
//! Windows realities handled here: `\\?\` canonicalize prefixes are
//! normalized away, component comparison is case-insensitive, device paths
//! (`\\.\`, `\\?\`) are rejected as roots, and component-aware comparison
//! (not raw string prefix) is used everywhere.
//!
//! Residual TOCTOU: the filesystem can change between validation and open.
//! We re-resolve at open time (reads go through the resolved path) and
//! document the residual OS race (§149) — it is not eliminable locally.

use std::path::{Component, Path, PathBuf};

use crate::model::{SaipenError, SaipenRoot, SAIPEN_DIR};

/// True on Windows-like hosts (target_os windows).
pub fn is_windows() -> bool {
    cfg!(windows)
}

/// Canonicalize and normalize a path for comparison: `fs::canonicalize`
/// resolves symlinks/junctions, then strip the `\\?\` prefix and (on
/// Windows) fold case for comparisons.
pub fn resolve(path: &Path) -> std::io::Result<PathBuf> {
    let canon = std::fs::canonicalize(path)?;
    Ok(normalize_for_compare(&canon))
}

fn normalize_for_compare(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_string_lossy().into_owned();
    if is_windows() {
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            s = rest.to_string();
        }
        if is_windows() {
            s = s.to_lowercase();
        }
    }
    PathBuf::from(s)
}

/// Component-aware containment: `child` is inside `parent` when every
/// component of `parent` prefixes `child`. Both must already be
/// canonicalized/normalized.
pub fn is_contained(child: &Path, parent: &Path) -> bool {
    let mut child_iter = child.components();
    for pc in parent.components() {
        match child_iter.next() {
            Some(cc) if same_component(pc, cc) => {}
            _ => return false,
        }
    }
    true
}

fn same_component(a: Component<'_>, b: Component<'_>) -> bool {
    match (a, b) {
        (Component::Normal(a), Component::Normal(b)) => {
            let sa = a.to_string_lossy();
            let sb = b.to_string_lossy();
            if is_windows() {
                sa.to_lowercase() == sb.to_lowercase()
            } else {
                sa == sb
            }
        }
        (Component::RootDir, Component::RootDir) => true,
        (Component::CurDir, Component::CurDir) => true,
        // Windows drive-letter prefixes (already case-folded by resolve()).
        (Component::Prefix(a), Component::Prefix(b)) => a.as_os_str() == b.as_os_str(),
        _ => false,
    }
}

/// Reject device/special paths that must never be treated as a workspace
/// root (§146): `\\.\` and the non-filesystem forms of `\\?\`. A drive form
/// `\\?\X:\…` is the ordinary extended-length prefix that `fs::canonicalize`
/// emits on Windows and is a legitimate filesystem path, so it is permitted.
pub fn reject_device_path(path: &Path) -> Result<(), SaipenError> {
    let s = path.as_os_str().to_string_lossy();
    // `\\.\` is the Win32 device namespace — never a workspace root.
    if s.starts_with(r"\\.\") {
        return Err(SaipenError::PathEscape(format!(
            "device path not allowed: {s}"
        )));
    }
    // `\\?\` is the extended-length prefix. Allow only the drive form
    // `\\?\X:\…`; reject all other `\\?\` forms (`\\?\GLOBALROOT`,
    // `\\?\Volume{…}`, `\\?\UNC\…`, bare `\\?\`) as device/namespace roots.
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        let b = rest.as_bytes();
        let is_drive_form = b.len() >= 3 && b[1] == b':' && matches!(b[2], b'\\' | b'/');
        if !is_drive_form {
            return Err(SaipenError::PathEscape(format!(
                "device path not allowed: {s}"
            )));
        }
    }
    Ok(())
}

/// Validate the canonical SAIPEN root for a workspace.
///
/// Returns `Ok(None)` when no `.saipen` exists (NotPresent is a normal
/// state). Returns the validated, resolved `SaipenRoot` otherwise. A
/// symlink/junction `.saipen` escaping the workspace is a `PathEscape`
/// error — never silently followed.
pub fn validate_root(workspace_root: &Path) -> Result<Option<SaipenRoot>, SaipenError> {
    reject_device_path(workspace_root)?;
    let candidate = workspace_root.join(SAIPEN_DIR);
    if !candidate.exists() {
        return Ok(None);
    }
    // Resolve the workspace root first (the anchor).
    let root = resolve(workspace_root).map_err(|e| SaipenError::Io {
        path: workspace_root.to_path_buf(),
        source: e,
    })?;
    // Resolve the candidate (follows symlinks/junctions to their target).
    let dir = resolve(&candidate).map_err(|e| match e.kind() {
        std::io::ErrorKind::PermissionDenied => SaipenError::PermissionDenied {
            path: candidate.clone(),
        },
        _ => SaipenError::Io {
            path: candidate.clone(),
            source: e,
        },
    })?;
    if !is_contained(&dir, &root) {
        return Err(SaipenError::PathEscape(format!(
            ".saipen resolves outside the workspace: {dir:?}"
        )));
    }
    Ok(Some(SaipenRoot {
        dir,
        workspace_root: root,
    }))
}

/// Validate a file reference inside the root: the canonical file
/// (`STATE.md`, `BOARD.md`, …) must resolve to a path contained in the root.
/// Used for reads; a canonical reference that escapes is rejected (§147).
pub fn validate_file_in_root(root: &SaipenRoot, file_name: &str) -> Result<PathBuf, SaipenError> {
    if file_name.contains('/') || file_name.contains('\\') || file_name == ".." {
        return Err(SaipenError::PathEscape(format!(
            "file reference must be a plain name: {file_name:?}"
        )));
    }
    let candidate = root.dir.join(file_name);
    let resolved = resolve(&candidate).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => SaipenError::Io {
            path: candidate.clone(),
            source: e,
        },
        std::io::ErrorKind::PermissionDenied => SaipenError::PermissionDenied {
            path: candidate.clone(),
        },
        _ => SaipenError::Io {
            path: candidate.clone(),
            source: e,
        },
    })?;
    if !is_contained(&resolved, &root.dir) {
        return Err(SaipenError::PathEscape(format!(
            "{file_name} resolves outside the SAIPEN root: {resolved:?}"
        )));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn component_containment_not_string_prefix() {
        // Must be component-aware: /a/b is NOT inside /a/bc.
        assert!(is_contained(Path::new("/a/b/c"), Path::new("/a/b"),));
        assert!(!is_contained(Path::new("/a/bc/c"), Path::new("/a/b"),));
    }

    #[test]
    fn validates_plain_root() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        let root = validate_root(dir.path()).unwrap().expect("present");
        assert_eq!(
            root.dir
                .file_name()
                .map(|s| s.to_string_lossy().into_owned()),
            Some(".saipen".into())
        );
    }

    #[cfg(windows)]
    #[test]
    fn allows_extended_drive_path() {
        // `\\?\X:\…` is a canonicalized drive path, not a device namespace.
        let p = std::path::Path::new(r"\\?\V:\foo\bar");
        assert!(reject_device_path(p).is_ok());
    }

    #[cfg(windows)]
    #[test]
    fn rejects_device_and_unc_forms() {
        assert!(reject_device_path(std::path::Path::new(r"\\.\C:")).is_err());
        assert!(reject_device_path(std::path::Path::new(r"\\?\GLOBALROOT")).is_err());
        assert!(reject_device_path(std::path::Path::new(r"\\?\UNC\server\share")).is_err());
    }

    #[test]
    fn missing_root_is_not_present() {
        let dir = tempfile::tempdir().unwrap();
        assert!(validate_root(dir.path()).unwrap().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        fs::remove_dir(dir.path().join(".saipen")).unwrap();
        symlink(outside.path(), dir.path().join(".saipen")).unwrap();
        let err = validate_root(dir.path()).unwrap_err();
        assert!(matches!(err, SaipenError::PathEscape(_)), "got {err:?}");
    }

    #[test]
    fn rejects_file_reference_with_path_separators() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".saipen")).unwrap();
        let root = validate_root(dir.path()).unwrap().unwrap();
        assert!(validate_file_in_root(&root, "../outside.md").is_err());
        assert!(validate_file_in_root(&root, "sub/STATE.md").is_err());
        assert!(validate_file_in_root(&root, "..").is_err());
    }
}
