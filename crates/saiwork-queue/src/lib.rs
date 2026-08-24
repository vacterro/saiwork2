//! SAIWORK2 durable queue (TASK 13) — the single authority for queued work
//! (law 7, KNOWLEDGE/QUEUE.md).
//!
//! SQLite is the durable truth; `QueueManager` owns the state machine and the
//! one dispatch worker; the UI is a projection. The queue never knows engine
//! internals — dispatch goes through the typed `EnginePort` boundary.

pub mod manager;
pub mod model;
pub mod port;
pub mod repo;

pub use manager::{DispatchHooks, QueueManager};
pub use model::{
    DispatchCandidate, EnqueueRequest, PortError, QueueDiagnostics, QueueError, QueueItem,
    QueueSnapshot, QueueState, QueueStatus, SessionMode, DISPATCH_CANDIDATE_PAGE_SIZE,
};
pub use port::{DispatchReceipt, EnginePort, EngineState, SessionCreateOutcome};
pub use repo::QueueRepo;
