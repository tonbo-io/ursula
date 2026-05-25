//! DoD #3 + #5 scaffold: trait surface for invariants.
//!
//! An `Invariant` observes simulated cluster state (via stable trace events
//! and direct reads) and asserts a property holds. Each invariant is paired
//! with at least one mutation test ("if you remove the SUT guard, this
//! invariant must catch it") — see `docs/architecture/deterministic-
//! simulation-testing.md` Section `Invariant Catalog`.
//!
//! Today most invariants live inline in `madsim_harness.rs` (look for
//! `assert_*_consistency` / `invariant_failed` helpers). The Phase B.1
//! refactor migrates them one at a time so each ends up here with a paired
//! mutation regression test.

/// Future home of `Invariant`. Kept minimal until the first invariant
/// migrates out of `madsim_harness.rs`.
pub trait Invariant {
    /// Stable identifier emitted on failure as `invariant_failed.invariant`.
    fn name(&self) -> &'static str;
}
