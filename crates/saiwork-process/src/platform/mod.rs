//! Platform-specific process ownership (TASK 06 §27–29, §82–83).
//!
//! The supervisor never sees platform details: it asks this module for a
//! `JobHandle`, assigns its freshly spawned (suspended) child to it, resumes
//! it, and later terminates the whole tree through it.
//!
//! - Windows: Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. The child
//!   is spawned `CREATE_SUSPENDED`, assigned to the job before it can run,
//!   then resumed — descendants cannot escape the job, and closing the last
//!   job handle (or `TerminateJobObject`) terminates the entire tree. This is
//!   the ownership primitive; `taskkill /T` remains only as the graceful
//!   (console-app) hint, never as the fundamental kill contract.
//! - Unix: the child is spawned with its own process group
//!   (`process_group(0)`); tree termination is a signal to the whole group.
//!
//! All `unsafe` lives here, isolated and documented. See `windows.rs`.

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::*;

#[cfg(not(windows))]
mod unix;
#[cfg(not(windows))]
pub use unix::*;
