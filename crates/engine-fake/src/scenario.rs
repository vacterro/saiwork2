//! Deterministic FakeEngine scenarios (TASK 07 §17–§42, §48–§50).
//!
//! A `FakeScenario` is a typed, validated run configuration. Presets cover
//! the whole matrix: normal/empty/single/large/slow/burst/large streaming,
//! mid-stream failure, hang, tool success/failure, permission allow/deny,
//! raw-frame hostility (duplicate/malformed/unknown/out-of-order/connection
//! loss), and engine crash. No randomness: every delay is a constant; every
//! test is reproducible 100%.

use std::time::Duration;

/// What happens at engine start (engine-level, not per-run).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupMode {
    /// Become Ready immediately.
    Immediate,
    /// Become Ready after this many ms; cancellable by `stop()`.
    DelayedMs(u64),
    /// `start()` returns a typed error; the engine ends FAILED.
    Fail,
    /// `start()` never completes on its own; `stop()` cancels it.
    Hang,
}

/// A permission gate attached to a tool (or standalone, as a `tool` named
/// `permission`). `Await` blocks until the caller resolves it via
/// `EngineAdapter::resolve_permission`; the auto variants resolve after a
/// fixed delay (deterministic, never random).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionStep {
    Await,
    AutoAllow { auto_after_ms: u64 },
    AutoDeny { auto_after_ms: u64 },
}

/// Tool activity interleaved into a run.
#[derive(Debug, Clone)]
pub struct ToolStep {
    pub name: &'static str,
    /// Optional permission gate: the tool pauses until decided.
    pub permission: Option<PermissionStep>,
    /// When true, the tool fails (`tool.failed`) and the run fails with it.
    pub fail: bool,
    pub output: String,
}

/// Raw adapter-boundary hostility (TASK 07 §31–§35). These are exercised by
/// feeding the same `normalize_frame` function the live `push_raw` API uses,
/// so the hostile path and the future OpenCode boundary share one policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostileMode {
    None,
    /// The same raw delta frame arrives twice; the boundary drops the
    /// duplicate with a protocol diagnostic.
    DuplicateFrame,
    /// A malformed raw frame arrives; contained as `engine.raw_event` +
    /// `runtime.warning`, the stream continues.
    MalformedFrame,
    /// An unknown raw frame kind arrives; ignored with a `runtime.warning`.
    UnknownFrame,
    /// Raw frames arrive out of order; the out-of-order one is rejected with
    /// a diagnostic (the bus never reorders provider frames).
    OutOfOrderFrame,
    /// The logical transport disappears mid-stream; the run fails with a
    /// typed outcome, the engine stays healthy.
    ConnectionLoss,
}

/// One deterministic run scenario.
#[derive(Debug, Clone)]
pub struct FakeScenario {
    /// Diagnostic label (recorded in the command history).
    pub label: &'static str,
    /// Number of deltas before the terminal event (normal flow).
    pub deltas: usize,
    /// Pacing between deltas; `Duration::ZERO` for burst.
    pub delta_delay: Duration,
    /// Bytes per delta chunk.
    pub delta_bytes: usize,
    /// Emit this many deltas, then fail the run.
    pub fail_after_delta: Option<usize>,
    /// No terminal event until cancel/stop/dispose.
    pub hang: bool,
    /// `message.started` → `message.completed` with zero deltas.
    pub empty: bool,
    /// Interleaved tool activity (with optional permission gate).
    pub tool: Option<ToolStep>,
    /// Raw-boundary hostility.
    pub hostile: HostileMode,
    /// After this many deltas the engine itself crashes (`engine.failed`,
    /// this run fails, every other active run fails too).
    pub engine_crash_after_delta: Option<usize>,
}

impl FakeScenario {
    pub fn normal() -> Self {
        Self {
            label: "normal",
            deltas: 12,
            delta_delay: Duration::from_millis(16),
            delta_bytes: 24,
            fail_after_delta: None,
            hang: false,
            empty: false,
            tool: None,
            hostile: HostileMode::None,
            engine_crash_after_delta: None,
        }
    }

    pub fn empty_response() -> Self {
        Self {
            label: "empty",
            deltas: 0,
            empty: true,
            ..Self::normal()
        }
    }

    pub fn single_delta() -> Self {
        Self {
            label: "single",
            deltas: 1,
            delta_delay: Duration::ZERO,
            ..Self::normal()
        }
    }

    pub fn large_delta() -> Self {
        Self {
            label: "large_delta",
            deltas: 1,
            delta_delay: Duration::ZERO,
            delta_bytes: 128 * 1024,
            ..Self::normal()
        }
    }

    pub fn slow_stream(deltas: usize, delay: Duration) -> Self {
        Self {
            label: "slow",
            deltas,
            delta_delay: delay,
            ..Self::normal()
        }
    }

    /// Many deltas with zero intentional delay (burst behavior, §38).
    pub fn burst(deltas: usize) -> Self {
        Self {
            label: "burst",
            deltas,
            delta_delay: Duration::ZERO,
            delta_bytes: 8,
            ..Self::normal()
        }
    }

    /// The long-transcript stress workload (§37). Not a product limit — a
    /// test load.
    pub fn large_stream(deltas: usize) -> Self {
        Self {
            label: "flood",
            deltas,
            delta_delay: Duration::ZERO,
            delta_bytes: 6,
            ..Self::normal()
        }
    }

    pub fn mid_stream_failure(after_deltas: usize) -> Self {
        Self {
            label: "fail",
            fail_after_delta: Some(after_deltas),
            ..Self::normal()
        }
    }

    pub fn hang() -> Self {
        Self {
            label: "hang",
            hang: true,
            ..Self::normal()
        }
    }

    pub fn tool_success() -> Self {
        Self {
            label: "tool",
            tool: Some(ToolStep {
                name: "read_file",
                permission: None,
                fail: false,
                output: "read src/main.rs (12 lines)".into(),
            }),
            ..Self::normal()
        }
    }

    pub fn tool_failure() -> Self {
        Self {
            label: "tool_fail",
            tool: Some(ToolStep {
                name: "write_file",
                permission: None,
                fail: true,
                output: "attempting write (will fail)".into(),
            }),
            ..Self::normal()
        }
    }

    /// Tool activity interleaved with text deltas (§83).
    pub fn tool_and_text() -> Self {
        Self {
            label: "tool_text",
            deltas: 6,
            tool: Some(ToolStep {
                name: "write_file",
                permission: None,
                fail: false,
                output: "writing src/main.rs (bounded)".into(),
            }),
            ..Self::normal()
        }
    }

    /// Tool gated by a permission decision (§84).
    pub fn permission(step: PermissionStep) -> Self {
        Self {
            label: match step {
                PermissionStep::Await => "permission_await",
                PermissionStep::AutoAllow { .. } => "permission_allow",
                PermissionStep::AutoDeny { .. } => "permission_deny",
            },
            deltas: 3,
            tool: Some(ToolStep {
                name: "shell",
                permission: Some(step),
                fail: false,
                output: "git status".into(),
            }),
            ..Self::normal()
        }
    }

    pub fn duplicate_frame() -> Self {
        Self {
            label: "duplicate",
            hostile: HostileMode::DuplicateFrame,
            ..Self::normal()
        }
    }

    pub fn malformed_frame() -> Self {
        Self {
            label: "malformed",
            hostile: HostileMode::MalformedFrame,
            ..Self::normal()
        }
    }

    pub fn unknown_frame() -> Self {
        Self {
            label: "unknown",
            hostile: HostileMode::UnknownFrame,
            ..Self::normal()
        }
    }

    pub fn out_of_order_frame() -> Self {
        Self {
            label: "out_of_order",
            hostile: HostileMode::OutOfOrderFrame,
            ..Self::normal()
        }
    }

    pub fn connection_loss() -> Self {
        Self {
            label: "conn_loss",
            hostile: HostileMode::ConnectionLoss,
            ..Self::normal()
        }
    }

    pub fn engine_crash() -> Self {
        Self {
            label: "crash",
            engine_crash_after_delta: Some(3),
            ..Self::normal()
        }
    }

    /// Reject contradictory configurations before a run starts (§86). An
    /// ambiguous scenario is a test bug, not nondeterminism at runtime.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.hang && (self.fail_after_delta.is_some() || self.engine_crash_after_delta.is_some())
        {
            return Err("hang conflicts with a failure terminal");
        }
        if self.empty && self.deltas > 0 {
            return Err("empty conflicts with deltas");
        }
        if self.fail_after_delta.is_some() && self.engine_crash_after_delta.is_some() {
            return Err("two failure modes configured");
        }
        Ok(())
    }

    /// Map a `/sim:<name>` directive to a preset (back-compat + the extended
    /// catalog). Unknown names fall back to `normal`.
    pub fn from_directive(name: &str) -> FakeScenario {
        match name {
            "normal" => Self::normal(),
            "slow" => Self::slow_stream(12, Duration::from_millis(80)),
            "flood" => Self::large_stream(10_000),
            "burst" => Self::burst(1_000),
            "tool" => Self::tool_and_text(),
            "toolfail" => Self::tool_failure(),
            "permission" => Self::permission(PermissionStep::AutoAllow { auto_after_ms: 800 }),
            "permdeny" => Self::permission(PermissionStep::AutoDeny { auto_after_ms: 800 }),
            "crash" => Self::engine_crash(),
            "malformed" => Self::malformed_frame(),
            "duplicate" => Self::duplicate_frame(),
            "unknown" => Self::unknown_frame(),
            "outoforder" => Self::out_of_order_frame(),
            "connloss" => Self::connection_loss(),
            "hang" => Self::hang(),
            "fail" => Self::mid_stream_failure(0),
            "empty" => Self::empty_response(),
            "single" => Self::single_delta(),
            "largedelta" => Self::large_delta(),
            _ => Self::normal(),
        }
    }
}

/// A raw provider frame **before** normalization — the adapter boundary
/// (§31–§35). The canonical stream only ever receives what `normalize_frame`
/// emits as a typed event.
#[derive(Debug, Clone)]
pub struct RawFrame {
    pub seq: u64,
    pub kind: &'static str,
    pub payload: Option<String>,
}

/// The outcome of normalizing one raw frame.
#[derive(Debug, Clone)]
pub enum NormalizedFrame {
    /// A canonical event ready for the bus.
    Event(saiwork_events::Event),
    /// Contained protocol note: publish `engine.raw_event` (debug) +
    /// `runtime.warning`; the stream continues.
    ProtocolNote { kind: &'static str, note: String },
    /// Unknown kind: ignored with a `runtime.warning`, never a crash.
    Unknown { kind: &'static str },
}

/// Normalize one raw frame against the transport's sequence policy
/// (duplicate/out-of-order dropped with a diagnostic, never reordered).
pub fn normalize_frame(
    last_seq: &std::sync::atomic::AtomicU64,
    session_id: &saiwork_events::SessionId,
    run_id: &saiwork_events::RunId,
    frame: &RawFrame,
) -> NormalizedFrame {
    use std::sync::atomic::Ordering;
    let last = last_seq.load(Ordering::SeqCst);
    if frame.seq <= last {
        let reason = if frame.seq == last {
            "duplicate frame"
        } else {
            "out-of-order frame"
        };
        return NormalizedFrame::ProtocolNote {
            kind: "raw_frame",
            note: format!("{reason} dropped (seq {} <= {last})", frame.seq),
        };
    }
    last_seq.store(frame.seq, Ordering::SeqCst);

    match frame.kind {
        "delta" => match &frame.payload {
            Some(payload) => NormalizedFrame::Event(Event::MessageDelta {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                delta: payload.clone(),
            }),
            None => NormalizedFrame::ProtocolNote {
                kind: "malformed_delta",
                note: "delta frame without payload".into(),
            },
        },
        "unknown" => NormalizedFrame::Unknown { kind: "unknown" },
        other => NormalizedFrame::ProtocolNote {
            kind: "unknown_kind",
            note: format!("unhandled frame kind '{other}'"),
        },
    }
}

use saiwork_events::Event;
