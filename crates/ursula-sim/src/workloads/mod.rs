//! DoD #3 scaffold: trait surface for workloads.
//!
//! A `Workload` drives client-side actions against the simulated cluster
//! (appends, reads, snapshot ops, producer-session retries, etc.) and owns
//! the *expectations* that workload-derived invariants check against.
//!
//! Today the scenario-specific workloads live inline inside
//! `madsim_harness.rs` (see `run_*_inner` functions). The Phase B.1
//! refactor will migrate them into this module one at a time. Until then
//! this trait is a target shape, not a contract every scenario implements.

use crate::SimSchedule;

/// Future home of `Workload`. Kept minimal until the first scenario migrates.
pub trait Workload {
    /// A short stable identifier used in trace events / artifact filenames.
    fn name(&self) -> &'static str;

    /// Set up + drive the workload against the simulated cluster. The
    /// concrete cluster handle type lives in the harness today; this trait
    /// stays abstract until the harness is split.
    ///
    /// The default impl panics; concrete workloads override.
    fn run(&mut self, _schedule: &SimSchedule) {
        panic!("Workload::run not implemented for {}", self.name());
    }
}
