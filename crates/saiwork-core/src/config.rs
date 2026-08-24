//! Application configuration and the deterministic data root
//! (PORTABILITY.md, law 15).

use std::path::{Path, PathBuf};

use crate::error::CoreError;

pub const APP_NAME: &str = "SAIWORK2";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Marker file that puts the app into portable mode.
pub const PORTABLE_FLAG: &str = "portable.flag";

#[derive(Debug, Clone)]
pub struct AppConfig {
    /// The one writable application data root.
    pub data_root: PathBuf,
    /// True when portable mode was selected by `portable.flag`.
    pub portable: bool,
}

impl AppConfig {
    /// Resolve the data root (PORTABILITY.md):
    /// 1. `SAIWORK2_DATA_DIR` env var;
    /// 2. `portable.flag` beside the executable → `<exe_dir>/data`;
    /// 3. OS application-data directory.
    pub fn resolve() -> Result<Self, CoreError> {
        let explicit = std::env::var_os("SAIWORK2_DATA_DIR").map(PathBuf::from);
        let exe_dir = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|p| p.to_path_buf()));
        Self::resolve_from(explicit.as_deref(), exe_dir.as_deref())
    }

    /// Pure data-root resolution used by `resolve()`; split out so the
    /// precedence logic is testable without mutating the process environment
    /// (env mutation races with parallel tests) or the executable location.
    ///
    /// Portable mode is defined by `portable.flag` **beside the executable**,
    /// never by the current working directory: the app may be launched from
    /// cmd, a shortcut, Explorer, or another process with a different CWD,
    /// and the root must not change (PORTABILITY.md law).
    fn resolve_from(explicit: Option<&Path>, exe_dir: Option<&Path>) -> Result<Self, CoreError> {
        if let Some(dir) = explicit {
            // The highest-precedence override must be absolute (TASK 24 §9):
            // a relative SAIWORK2_DATA_DIR would resolve against the caller's
            // CWD, so the same configuration launched from Explorer, a
            // shortcut, cmd or another process could silently point at
            // different SQLite/data roots and look like lost state. Fail
            // loudly instead of falling through to another root.
            if !dir.is_absolute() {
                return Err(CoreError::Config(format!(
                    "SAIWORK2_DATA_DIR must be an absolute path, got {}",
                    dir.display()
                )));
            }
            return Ok(Self {
                data_root: dir.to_path_buf(),
                portable: false,
            });
        }

        if let Some(exe_dir) = exe_dir {
            if exe_dir.join(PORTABLE_FLAG).is_file() {
                return Ok(Self {
                    data_root: exe_dir.join("data"),
                    portable: true,
                });
            }
        }

        let root = default_app_data_dir()?;
        Ok(Self {
            data_root: root,
            portable: false,
        })
    }

    /// Create the data root and its subdirectories.
    pub fn ensure_layout(&self) -> Result<(), CoreError> {
        for sub in ["", "config", "logs", "cache", "runtime"] {
            let dir = if sub.is_empty() {
                self.data_root.clone()
            } else {
                self.data_root.join(sub)
            };
            std::fs::create_dir_all(&dir)
                .map_err(|e| CoreError::Config(format!("cannot create {}: {e}", dir.display())))?;
        }
        Ok(())
    }

    pub fn database_path(&self) -> PathBuf {
        self.data_root.join("saiwork2.db")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.data_root.join("logs")
    }

    pub fn runtime_dir(&self) -> PathBuf {
        self.data_root.join("runtime")
    }
}

/// OS application-data directory (step 3 of resolution).
fn default_app_data_dir() -> Result<PathBuf, CoreError> {
    #[cfg(windows)]
    {
        if let Some(appdata) = std::env::var_os("APPDATA") {
            return Ok(PathBuf::from(appdata).join(APP_NAME));
        }
        Err(CoreError::Config(
            "APPDATA is not set; cannot resolve data root".into(),
        ))
    }
    #[cfg(target_os = "macos")]
    {
        if let Some(home) = std::env::var_os("HOME") {
            return Ok(PathBuf::from(home)
                .join("Library/Application Support")
                .join(APP_NAME));
        }
        Err(CoreError::Config(
            "HOME is not set; cannot resolve data root".into(),
        ))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
            return Ok(PathBuf::from(xdg).join(APP_NAME));
        }
        if let Some(home) = std::env::var_os("HOME") {
            return Ok(PathBuf::from(home).join(".local/share").join(APP_NAME));
        }
        Err(CoreError::Config(
            "HOME is not set; cannot resolve data root".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_var_wins_over_everything() {
        // Explicit override wins even when a portable.flag is present.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(PORTABLE_FLAG), b"").unwrap();
        let cfg =
            AppConfig::resolve_from(Some(Path::new(r"X:\custom\data")), Some(dir.path())).unwrap();
        assert_eq!(cfg.data_root, PathBuf::from(r"X:\custom\data"));
        assert!(!cfg.portable);
    }

    #[test]
    fn portable_flag_selects_exe_adjacent_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(PORTABLE_FLAG), b"").unwrap();
        let cfg = AppConfig::resolve_from(None, Some(dir.path())).unwrap();
        assert!(cfg.portable);
        assert_eq!(cfg.data_root, dir.path().join("data"));
    }

    #[test]
    fn portable_resolution_does_not_depend_on_cwd() {
        // The resolver consults only the executable directory: resolution is
        // identical no matter where the process started, so launching from
        // cmd/Explorer/a shortcut yields the same root.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(PORTABLE_FLAG), b"").unwrap();
        let cfg = AppConfig::resolve_from(None, Some(dir.path())).unwrap();
        assert_eq!(cfg.data_root, dir.path().join("data"));
        assert!(cfg.portable);
        // No CWD-derived component can appear in the root.
        assert!(!cfg.data_root.is_relative());
    }

    #[test]
    fn no_flag_falls_back_to_os_default() {
        // No explicit root and no portable flag beside the executable (the
        // temp dir has none): fall back to the OS application-data directory
        // instead of failing or inventing a root.
        let dir = tempfile::tempdir().unwrap();
        let cfg = AppConfig::resolve_from(None, Some(dir.path())).unwrap();
        assert!(!cfg.portable);
        assert!(cfg.data_root.file_name().is_some());
        assert!(cfg
            .data_root
            .as_os_str()
            .to_string_lossy()
            .contains("SAIWORK2"));
    }

    #[test]
    fn relative_override_is_rejected_not_cwd_dependent() {
        // A relative SAIWORK2_DATA_DIR would resolve against the caller's
        // CWD, so the same configuration launched from Explorer, a shortcut,
        // cmd or another process could silently point at different data
        // roots and look like lost state (TASK 24 §9). The override must be
        // absolute; a relative path is a precise Config error, never a silent
        // fall-through to another root.
        let dir = tempfile::tempdir().unwrap();
        let err = AppConfig::resolve_from(Some(Path::new("relative/data")), Some(dir.path()))
            .expect_err("relative override must fail loudly");
        assert!(
            err.to_string().contains("absolute"),
            "precise config error, got {err:?}"
        );
        // Absolute paths containing spaces/Unicode resolve identically
        // regardless of CWD.
        let exe_dir = dir.path().join("exe dir ü");
        std::fs::create_dir_all(&exe_dir).unwrap();
        let cfg =
            AppConfig::resolve_from(Some(&exe_dir.join("data root")), Some(&exe_dir)).unwrap();
        assert!(cfg.data_root.is_absolute());
        assert_eq!(cfg.data_root, exe_dir.join("data root"));
    }

    #[test]
    fn explicit_invalid_override_returns_error() {
        // SAIWORK2_DATA_DIR pointing at an unusable location must fail loudly,
        // never silently fall back to %APPDATA% (explicit override is
        // explicit). A file where a directory is required is the portable
        // probe.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let cfg = AppConfig::resolve_from(Some(&file), None).unwrap();
        assert!(cfg.ensure_layout().is_err());
    }

    #[test]
    fn ensure_layout_creates_expected_directories() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = AppConfig {
            data_root: dir.path().join("data"),
            portable: true,
        };
        cfg.ensure_layout().unwrap();
        for sub in ["", "config", "logs", "cache", "runtime"] {
            let p = dir.path().join("data").join(sub);
            assert!(p.is_dir(), "expected {p:?} to exist");
        }
        assert_eq!(
            cfg.database_path(),
            dir.path().join("data").join("saiwork2.db")
        );
    }
}
