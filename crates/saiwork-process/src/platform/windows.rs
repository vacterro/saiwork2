//! Windows process-tree ownership via Job Objects (TASK 06 §27–28).
//!
//! Invariants of this module:
//! - The `JobHandle` returned by `create()` owns a Job Object configured with
//!   `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`: when the last handle to the job is
//!   closed, every process in the job is terminated. This makes even an
//!   abnormal app exit (crash, killed shell) clean up the whole tree.
//! - `assign` must run while the child is still suspended (spawn with
//!   `CREATE_SUSPENDED`) so no descendant can escape the job before
//!   assignment.
//! - `terminate` is `TerminateJobObject`: one OS call kills the direct child
//!   and every descendant, with no PID-race and no `taskkill` parsing.
//!
//! `unsafe` is confined to this file and wrapped in safe, narrow APIs.

use std::io;
use std::mem::size_of;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Threading::{
    OpenProcess, OpenThread, ResumeThread, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SET_QUOTA,
    PROCESS_TERMINATE, THREAD_SUSPEND_RESUME,
};

/// `CREATE_SUSPENDED` — spawn with the primary thread suspended so we can
/// assign the process to its job before it executes a single instruction.
pub const CREATE_SUSPENDED: u32 = 0x0000_0004;
/// `CREATE_NO_WINDOW` — CLI engines must not flash a console window when
/// spawned from the desktop app.
pub const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// A safe wrapper around a Job Object handle.
///
/// # Safety
/// The raw `HANDLE` is stored directly. `CloseHandle` is thread-safe and
/// idempotent-per-handle, so `Send`/`Sync` are sound as long as every use of
/// the handle goes through this module's methods (it does). `Drop` closes the
/// handle, which — because of `KILL_ON_JOB_CLOSE` — terminates any process
/// still in the job.
pub struct JobHandle(HANDLE);

// SAFETY: the handle is only ever used via this module's functions
// (CreateJobObjectW/AssignProcessToJobObject/TerminateJobObject/CloseHandle),
// all of which are thread-safe on the Windows API. The invariant is enforced
// by keeping the field private.
unsafe impl Send for JobHandle {}
unsafe impl Sync for JobHandle {}

impl JobHandle {
    /// Create a new job with kill-on-close semantics.
    pub fn create() -> io::Result<Self> {
        // SAFETY: null name → unnamed job; null security attributes → default.
        let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: `info` is fully initialized (zeroed then flags set); the
        // size is exact for the extended-limit structure.
        let ok = unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            let err = io::Error::last_os_error();
            // SAFETY: valid handle from CreateJobObjectW above.
            unsafe { CloseHandle(job) };
            return Err(err);
        }
        Ok(Self(job))
    }

    /// Assign a child process (must still be suspended) to this job.
    pub fn assign(&self, pid: u32) -> io::Result<()> {
        // SAFETY: OpenProcess with the access rights AssignProcessToJobObject
        // requires; pid is our own freshly spawned child.
        let process = unsafe { OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, pid) };
        if process.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: handle is valid; job handle is valid and alive.
        let ok = unsafe { AssignProcessToJobObject(self.0, process) };
        // SAFETY: valid handle from OpenProcess above.
        unsafe { CloseHandle(process) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Resume the primary thread of a `CREATE_SUSPENDED` child.
    ///
    /// `CREATE_SUSPENDED` suspends only the primary thread. We find it via a
    /// Toolhelp thread snapshot (creation order ⇒ the first thread of the
    /// process is the primary one) and call `ResumeThread` on it.
    pub fn resume(&self, pid: u32) -> io::Result<()> {
        // SAFETY: snapshot with TH32CS_SNAPTHREAD; 0 = all processes.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
        // SAFETY: entry is zeroed with dwSize set; snapshot handle is valid.
        let mut found = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        let mut primary_tid = None;
        while found {
            if entry.th32OwnerProcessID == pid {
                primary_tid = Some(entry.th32ThreadID);
                break;
            }
            // SAFETY: snapshot valid; entry valid (last populated by Thread32Next).
            found = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        // SAFETY: valid snapshot handle.
        unsafe { CloseHandle(snapshot) };

        let tid = primary_tid.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no thread found for child process")
        })?;
        // SAFETY: OpenThread with THREAD_SUSPEND_RESUME on our own child's tid.
        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, tid) };
        if thread.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: valid thread handle; resumes the suspended primary thread.
        let prev = unsafe { ResumeThread(thread) };
        // SAFETY: valid handle from OpenThread above.
        unsafe { CloseHandle(thread) };
        if prev == u32::MAX {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Terminate every process in the job (the whole owned tree).
    pub fn terminate(&self, exit_code: u32) -> io::Result<()> {
        // SAFETY: valid job handle; terminating the job is always legal.
        let ok = unsafe { TerminateJobObject(self.0, exit_code) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for JobHandle {
    fn drop(&mut self) {
        // SAFETY: valid handle; CloseHandle never fails in a way that needs
        // handling here.
        unsafe { CloseHandle(self.0) };
    }
}

/// Best-effort liveness check (used by the process-tree test and the future
/// 0-orphan gate). A terminated process can no longer be opened.
pub fn is_pid_alive(pid: u32) -> bool {
    // SAFETY: OpenProcess with query rights; result checked immediately.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return false;
    }
    // SAFETY: valid handle from OpenProcess above.
    unsafe { CloseHandle(handle) };
    true
}
