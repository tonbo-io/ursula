//! DoD #3 scaffold: trait surface for scenarios.
//!
//! A `Scenario` is the smallest unit a seed produces: a `(Workload,
//! Vec<Fault>, Vec<Invariant>)` triple. Once the harness is fully split
//! per DoD #3, adding a new scenario means declaring this triple — there
//! is no more `if seed == N` routing inside `SimSchedule::generate`.
//!
//! Today the routing still lives inline in `madsim_harness.rs` (see
//! `SimSchedule::generate*` functions and the `match` arms in
//! `crates/ursula-sim/src/bin/ursula-sim-smoke.rs`). The Phase B.1
//! refactor migrates each scenario family into this module.

use crate::faults::Fault;
use crate::invariants::Invariant;
use crate::workloads::Workload;

/// Future home of `Scenario`. Each migrated scenario will be a unit
/// struct implementing this trait.
pub trait Scenario {
    fn name(&self) -> &'static str;
    fn workload(&self) -> Box<dyn Workload>;
    fn faults(&self) -> Vec<Box<dyn Fault>>;
    fn invariants(&self) -> Vec<Box<dyn Invariant>>;
}
