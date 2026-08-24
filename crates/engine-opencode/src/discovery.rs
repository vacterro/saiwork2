//! Deterministic OpenCode executable discovery (TASK 10 §5–§8).
//!
//! Precedence: explicit configured path → native executable on PATH →
//! Windows `.cmd`/`.bat` shim on PATH (resolved to the real executable when
//! the shim names one; otherwise launched via the encapsulated `cmd.exe`
//! wrapper). No disk-wide search, no shell-string fallback, and an explicit
//! invalid path fails loudly instead of silently switching to another
//! OpenCode on PATH (§6).

use std::path::{Path, PathBuf};

use crate::errors::OpenCodeError;
use crate::OpenCodeConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LauncherKind {
    /// A direct executable (PE/ELF/Mach-O) spawned with no shell.
    Native,
    /// Windows `.cmd`/`.bat` shim that must go through the encapsulated
    /// `cmd.exe /D /S /C <raw line>` launch (TASK 10 §8).
    CmdWrapper,
}

#[derive(Debug, Clone)]
pub struct DiscoveredExecutable {
    /// What to launch: the real executable (`Native`) or the shim
    /// (`CmdWrapper` — passed to cmd.exe).
    pub path: PathBuf,
    pub kind: LauncherKind,
    /// Where it came from ("explicit", "PATH", "shim-resolved").
    pub source: &'static str,
}

const EXE: &str = "opencode.exe";
const CMD: &str = "opencode.cmd";
const BAT: &str = "opencode.bat";
#[cfg(unix)]
const BARE: &str = "opencode";

impl DiscoveredExecutable {
    pub fn display(&self) -> String {
        self.path.display().to_string()
    }
}

/// Discover the OpenCode executable with the documented precedence.
pub fn discover(config: &OpenCodeConfig) -> Result<DiscoveredExecutable, OpenCodeError> {
    // 1. Explicit path wins and never silently falls back (§6).
    if let Some(explicit) = &config.explicit_executable {
        return classify_explicit(explicit);
    }
    // 2/3. PATH discovery.
    let path_dirs: Vec<PathBuf> =
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()).collect();
    if let Some(found) = discover_on_path(&path_dirs) {
        return Ok(found);
    }
    Err(OpenCodeError::ExecutableNotFound {
        searched: path_dirs.iter().map(|d| d.display().to_string()).collect(),
    })
}

/// PATH scan for a native binary, then a Windows shim.
fn discover_on_path(dirs: &[PathBuf]) -> Option<DiscoveredExecutable> {
    for dir in dirs {
        if dir.as_os_str().is_empty() {
            continue;
        }
        #[cfg(windows)]
        let native = dir.join(EXE);
        #[cfg(unix)]
        let native = dir.join(BARE);
        if native.is_file() {
            return Some(DiscoveredExecutable {
                path: native,
                kind: LauncherKind::Native,
                source: "PATH",
            });
        }
    }
    #[cfg(windows)]
    for dir in dirs {
        if dir.as_os_str().is_empty() {
            continue;
        }
        for name in [CMD, BAT] {
            let shim = dir.join(name);
            if shim.is_file() {
                // Prefer the real executable the shim forwards to; only if
                // that cannot be determined, launch the shim via cmd.exe.
                if let Some(real) = resolve_shim_exe(&shim) {
                    return Some(DiscoveredExecutable {
                        path: real,
                        kind: LauncherKind::Native,
                        source: "shim-resolved",
                    });
                }
                return Some(DiscoveredExecutable {
                    path: shim,
                    kind: LauncherKind::CmdWrapper,
                    source: "PATH",
                });
            }
        }
    }
    None
}

/// An explicit path must exist and be usable; any problem is a hard error,
/// never a silent switch to PATH (§6).
fn classify_explicit(path: &Path) -> Result<DiscoveredExecutable, OpenCodeError> {
    if !path.exists() {
        return Err(OpenCodeError::ExplicitExecutableInvalid {
            path: path.to_path_buf(),
            reason: "path does not exist".into(),
        });
    }
    if !path.is_file() {
        return Err(OpenCodeError::ExplicitExecutableInvalid {
            path: path.to_path_buf(),
            reason: "not a file".into(),
        });
    }
    #[cfg(windows)]
    {
        let lower = path.to_string_lossy().to_lowercase();
        if lower.ends_with(".cmd") || lower.ends_with(".bat") {
            if let Some(real) = resolve_shim_exe(path) {
                return Ok(DiscoveredExecutable {
                    path: real,
                    kind: LauncherKind::Native,
                    source: "explicit",
                });
            }
            return Ok(DiscoveredExecutable {
                path: path.to_path_buf(),
                kind: LauncherKind::CmdWrapper,
                source: "explicit",
            });
        }
    }
    Ok(DiscoveredExecutable {
        path: path.to_path_buf(),
        kind: LauncherKind::Native,
        source: "explicit",
    })
}

/// Try to recover the real executable a Windows `.cmd` shim forwards to.
///
/// Handles the two dominant shim families:
/// - npm: `"%dp0%\node_modules\<pkg>\bin\<name>.exe" %*`
/// - generic: `"%~dp0\..\...\<name>.exe" %*`
///
/// Returns the resolved path only when it is a real file; otherwise `None`
/// (the caller then launches the shim through cmd.exe).
#[cfg(windows)]
fn resolve_shim_exe(shim: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(shim).ok()?;
    let dir = shim.parent()?.to_string_lossy().to_string();
    for line in text.lines() {
        let Some(open) = line.find('"') else { continue };
        let rest = &line[open + 1..];
        let Some(close) = rest.find('"') else {
            continue;
        };
        let token = &rest[..close];
        if !token.to_lowercase().ends_with(".exe") {
            continue;
        }
        // Expand the shim-directory placeholders npm and friends emit.
        let expanded = token
            .replace("%~dp0%", &dir)
            .replace("%~dp0", &dir)
            .replace("%dp0%", &dir);
        if expanded.contains('%') {
            continue; // some other variable we do not understand — skip
        }
        let candidate = PathBuf::from(expanded);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(unix)]
fn resolve_shim_exe(_shim: &Path) -> Option<PathBuf> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_missing_path_is_a_hard_error() {
        let config = OpenCodeConfig {
            explicit_executable: Some(PathBuf::from("Z:\\definitely\\missing\\opencode.exe")),
            ..OpenCodeConfig::default()
        };
        assert!(matches!(
            discover(&config),
            Err(OpenCodeError::ExplicitExecutableInvalid { .. })
        ));
    }

    #[test]
    fn explicit_directory_is_rejected() {
        let dir = std::env::temp_dir();
        let config = OpenCodeConfig {
            explicit_executable: Some(dir.clone()),
            ..OpenCodeConfig::default()
        };
        assert!(matches!(
            discover(&config),
            Err(OpenCodeError::ExplicitExecutableInvalid { .. })
        ));
    }

    #[test]
    fn path_scan_finds_native_executable() {
        let dir = std::env::temp_dir();
        let exe = dir.join(if cfg!(windows) { EXE } else { "opencode" });
        let created = {
            #[cfg(windows)]
            {
                std::fs::write(&exe, b"MZ fake").is_ok()
            }
            #[cfg(unix)]
            {
                std::fs::write(&exe, b"#!/bin/sh\n").is_ok()
                    && std::fs::set_permissions(
                        &exe,
                        std::os::unix::fs::PermissionsExt::from_mode(0o755),
                    )
                    .is_ok()
            }
        };
        if !created {
            return; // environment could not create the fixture
        }
        let found = discover_on_path(std::slice::from_ref(&dir));
        let _ = std::fs::remove_file(&exe);
        let found = found.expect("native exe on PATH must be discovered");
        assert_eq!(found.kind, LauncherKind::Native);
        assert_eq!(found.source, "PATH");
    }

    #[cfg(windows)]
    #[test]
    fn npm_shim_resolves_to_real_exe() {
        let dir = std::env::temp_dir().join(format!("oc-shim-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let real = dir.join("node_modules").join("fake-pkg").join("bin");
        std::fs::create_dir_all(&real).unwrap();
        let exe = real.join("opencode.exe");
        std::fs::write(&exe, b"MZ fake").unwrap();
        let shim = dir.join("opencode.cmd");
        std::fs::write(
            &shim,
            "@ECHO off\r\nGOTO start\r\n:find_dp0\r\nSET dp0=%~dp0\r\nEXIT /b\r\n:start\r\nSETLOCAL\r\nCALL :find_dp0\r\n\"%dp0%\\node_modules\\fake-pkg\\bin\\opencode.exe\"   %*\r\n",
        )
        .unwrap();
        let resolved = resolve_shim_exe(&shim).expect("npm shim must resolve");
        assert_eq!(resolved, exe);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(windows)]
    #[test]
    fn unknown_shim_falls_back_to_cmd_wrapper() {
        let dir = std::env::temp_dir().join(format!("oc-shim-unknown-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let shim = dir.join("opencode.cmd");
        std::fs::write(&shim, "@echo off\r\ncall %~dp0\\runner.bat %*\r\n").unwrap();
        let found = discover_on_path(std::slice::from_ref(&dir)).expect("shim must be discovered");
        assert_eq!(found.kind, LauncherKind::CmdWrapper);
        assert_eq!(found.path, shim);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
