//! Shared artifact schemas and helpers for the `ursula-sim` CLI subcommands.
//!
//! Serialized field names and orders are stable: recorded corpora and CI jq
//! pipelines depend on them byte-for-byte.

use std::any::Any;

use crate::SimEvent;
use crate::SimReport;
use crate::SimSchedule;
use crate::SimTrace;

/// Failure summary written by `ursula-sim smoke` (`seed-N-failure.json`).
///
/// Deserialization only requires `schedule` and `panic`, so minimized failure
/// artifacts (`.minimized.failure`) parse as this type too.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct FailedSeedArtifact {
    /// Artifact schema version (`SIM_REGRESSION_SCHEMA_VERSION`).
    #[serde(default)]
    pub schema_version: u32,
    /// Seed whose schedule failed.
    #[serde(default)]
    pub seed: u64,
    /// The schedule that reproduces the failure.
    pub schedule: SimSchedule,
    /// Path to the sibling stable-trace artifact.
    #[serde(default)]
    pub stable_trace_path: String,
    /// Path to the sibling raw-event-log artifact.
    #[serde(default)]
    pub raw_event_log_path: String,
    /// Captured panic message.
    pub panic: String,
}

/// Stable-trace artifact written by `ursula-sim smoke`
/// (`seed-N-stable-trace.json`).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct StableTraceArtifact {
    /// Artifact schema version (`SIM_REGRESSION_SCHEMA_VERSION`).
    #[serde(default)]
    pub schema_version: u32,
    /// The schedule that produced the trace.
    pub schedule: SimSchedule,
    /// Stable replay projection of the recorded trace.
    pub stable_trace: SimTrace,
}

/// Raw event log written by `ursula-sim smoke` (`seed-N-raw-events.json`).
#[derive(serde::Serialize)]
pub struct RawEventLogArtifact {
    /// Artifact schema version (`SIM_REGRESSION_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Seed whose run produced the events.
    pub seed: u64,
    /// Every recorded event, unprojected.
    pub events: Vec<SimEvent>,
}

/// Renders a captured panic payload as a human-readable string.
pub fn panic_payload_to_string(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

/// True when the stable trace records a failure of `invariant`.
pub fn invariant_failed(trace: &SimTrace, invariant: &str) -> bool {
    trace.events.iter().any(|event| {
        matches!(
            event,
            SimEvent::InvariantFailed {
                invariant: candidate,
                ..
            } if candidate == invariant
        )
    })
}

/// Canonical stable-trace projection used by artifacts.
pub fn stable_trace(trace: SimTrace) -> SimTrace {
    trace.stable_replay()
}

/// Runs a schedule with the panic hook silenced, converting a panic into
/// `Err` with the rendered payload.
pub fn run_schedule_capturing_panic(schedule: &SimSchedule) -> Result<SimReport, String> {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| schedule.run()));
    std::panic::set_hook(previous_hook);
    result.map_err(panic_payload_to_string)
}
