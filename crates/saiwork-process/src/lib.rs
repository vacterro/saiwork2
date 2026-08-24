//! ProcessSupervisor — the single owner of SAIWORK2 child processes (law 6).
//!
//! See `supervisor` for the state machine, stop/kill policy, and the
//! Windows Job Object ownership primitive (`platform`).

pub mod error;
pub mod output;
pub mod platform;
pub mod supervisor;

pub use error::ProcessError;
pub use output::{BoundedOutputBuffer, OUTPUT_CAP_BYTES, OUTPUT_RETAIN_BYTES};
pub use platform::is_pid_alive;
pub use supervisor::{
    ExitInfo, ManagedProcess, ProcessSnapshot, ProcessSpec, ProcessState, ProcessSupervisor,
    StdinPolicy,
};
#[cfg(feature = "failpoints")]
pub use supervisor::{SpawnHooks, StopHooks};
