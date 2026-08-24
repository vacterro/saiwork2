//! Unix process-tree ownership: the process group created at spawn time
//! (`process_group(0)`) is the ownership primitive. `JobHandle` is a no-op —
//! group signaling happens through `supervisor::apply_signal`.

use std::io;

/// Unused on Unix (the group flag is set at spawn); kept for API symmetry.
pub const CREATE_SUSPENDED: u32 = 0;
/// Unused on Unix; kept for API symmetry.
pub const CREATE_NO_WINDOW: u32 = 0;

/// No-op on Unix: ownership is the process group, not a handle.
pub struct JobHandle;

impl JobHandle {
    pub fn create() -> io::Result<Self> {
        Ok(Self)
    }

    pub fn assign(&self, _pid: u32) -> io::Result<()> {
        Ok(())
    }

    pub fn resume(&self, _pid: u32) -> io::Result<()> {
        Ok(())
    }

    pub fn terminate(&self, _exit_code: u32) -> io::Result<()> {
        Ok(())
    }
}

/// Best-effort liveness check via `kill(pid, 0)`.
pub fn is_pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 never delivers a signal; it only probes existence.
    let rc = unsafe { libc::kill(pid as i32, 0) };
    if rc == 0 {
        return true;
    }
    // EPERM: exists but owned by another user → still alive.
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}
