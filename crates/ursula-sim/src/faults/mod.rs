//! DoD #3 scaffold: trait surface for faults.
//!
//! A `Fault` is a perturbation the simulator can inject into the cluster
//! at a named phase: partition / heal, kill / restart node, inject cold
//! store error, truncate cold read, delay timer, drop RPC, etc. The
//! concrete `SimFaultAction` enum currently lives in `madsim_harness.rs`;
//! the Phase B.1 refactor will move it here next to the trait so each
//! fault variant carries its own implementation alongside its data.

pub use crate::SimFaultAction;

/// Future home of `Fault`. The concrete `SimFaultAction` enum will
/// implement this trait once it migrates out of `madsim_harness.rs`.
pub trait Fault {
    /// Stable identifier (matches the serde discriminant emitted in
    /// `SimFaultStep.action.action`).
    fn name(&self) -> &'static str;
}
