//! ProcessSpec construction (TASK 10 §8, §12–§13).
//!
//! The adapter never spawns with a bare `Command::new("opencode")`: every
//! launch — probe and server alike — is a `ProcessSpec` owned by the
//! ProcessSupervisor. Two launcher kinds are supported:
//! - `Native`: the real executable, spawned directly (no shell);
//! - `CmdWrapper`: a Windows `.cmd`/`.bat` shim whose path may contain
//!   spaces, launched through the encapsulated `cmd.exe /D /S /C <raw line>`
//!   form. The `/C` argument is passed verbatim via `ProcessSpec::raw_args`
//!   with the doubled-quote pattern verified against a real shim with spaces
//!   in its path (TASK 10 §8, §54):
//!
//!   ```text
//!   cmd.exe /D /S /C ""C:\path with spaces\opencode.cmd" serve --port 4096"
//!   ```
//!
//! The server password travels via environment variable, never argv (§23).

use std::path::PathBuf;

use saiwork_events::ProcessId;
use saiwork_process::ProcessSpec;

use crate::endpoint::LOOPBACK_HOST;
use crate::secret::Secret;
use crate::DiscoveredExecutable;

/// Build a ProcessSpec for an arbitrary OpenCode argv (probe or server).
pub fn spec_for_args(
    discovered: &DiscoveredExecutable,
    id: ProcessId,
    cwd: Option<PathBuf>,
    env: Vec<(String, String)>,
    args: &[String],
) -> ProcessSpec {
    let mut spec = ProcessSpec::new(
        id,
        match discovered.kind {
            crate::LauncherKind::Native => discovered.path.to_string_lossy().into_owned(),
            crate::LauncherKind::CmdWrapper => "cmd.exe".into(),
        },
    );
    match discovered.kind {
        crate::LauncherKind::Native => {
            spec.args = args.to_vec();
        }
        crate::LauncherKind::CmdWrapper => {
            spec.args = vec!["/D".into(), "/S".into(), "/C".into()];
            #[cfg(windows)]
            {
                spec.raw_args = vec![cmd_raw_line(&discovered.path, args)];
            }
        }
    }
    spec.cwd = cwd;
    spec.env = env;
    spec
}

/// The ProcessSpec for the managed OpenCode server runtime (TASK 10 §12–§13,
/// §19, §21–§23).
pub fn server_spec(
    discovered: &DiscoveredExecutable,
    workspace: &std::path::Path,
    port: u16,
    secret: &Secret,
    id: ProcessId,
) -> ProcessSpec {
    let args: Vec<String> = vec![
        "serve".into(),
        "--port".into(),
        port.to_string(),
        "--hostname".into(),
        LOOPBACK_HOST.to_string(),
        "--pure".into(),
    ];
    let env = vec![
        (
            "OPENCODE_SERVER_PASSWORD".into(),
            secret.as_str().to_owned(),
        ),
        // PIN the username, never inherit it: an ambient
        // OPENCODE_SERVER_USERNAME in the parent env (e.g. another
        // opencode deployment on the same machine) would configure the
        // spawned server with that username, and the adapter's own client
        // always authenticates as `opencode` — a mismatch 401s every
        // request (§23: the auth contract is fully declared, never ambient).
        ("OPENCODE_SERVER_USERNAME".into(), "opencode".into()),
    ];
    spec_for_args(discovered, id, Some(workspace.to_path_buf()), env, &args)
}

/// Build the raw `/C` argument for a shim launch. The verified pattern:
/// `""<shim path>" <args joined>"` — the outer quotes are what `cmd /S`
/// strips, the inner quotes protect a path containing spaces (§54).
#[cfg(windows)]
fn cmd_raw_line(shim: &std::path::Path, args: &[String]) -> String {
    let win = shim.to_string_lossy().replace('/', "\\");
    format!("\"\"{win}\" {}\"", args.join(" "))
}

#[cfg(unix)]
fn cmd_raw_line(_shim: &std::path::Path, _args: &[String]) -> String {
    unreachable!("CmdWrapper is Windows-only")
}
