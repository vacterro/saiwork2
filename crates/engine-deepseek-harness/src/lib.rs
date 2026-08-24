//! DeepSeek Harness engine adapter (TASK 20/24 — foundation).
//!
//! Implements the TASK 19 contract (KNOWLEDGE/DEEPSEEK_HARNESS.md, ADR-039):
//! **ACP over stdio** is the selected machine seam — a newline-delimited
//! JSON-RPC 2.0 agent protocol. This crate is the **adapter firewall**: every
//! Harness/ACP DTO stays here; generic core sees only the `EngineAdapter`
//! logical surface (identity, capabilities, health, normalized errors).
//!
//! TASK 20 scope (foundation only): discovery, cheap probe, ProcessSupervisor
//! ownership of the top-level Harness runtime, protocol transport with request
//! correlation/timeouts/bounds, ACP `initialize` handshake with version
//! compatibility, lifecycle (start/stop/kill/crash/restart) with generation
//! protection, typed errors, a deterministic fake-server fixture, and the
//! hostile matrix. No sessions, prompts, tools, or permissions yet — that is
//! TASK 21, and capabilities are `false` until proven (never fake parity).
//!
//! Process ownership boundary (PROCESS_LIFECYCLE.md, no double supervision):
//! SAIWORK2 owns the top-level Harness runtime process only; Harness owns its
//! internal agent/tool/subprocess lifecycle.

pub mod adapter;
pub mod config;
pub mod error;
pub mod events;
pub mod permissions;
pub mod protocol;
pub mod runs;
pub mod sessions;
pub mod transport;

pub use adapter::{HarnessAdapter, ENGINE_ID};
pub use config::{HarnessConfig, DISCOVERY_LAUNCHERS};
pub use error::HarnessError;
