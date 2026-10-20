//! The closed loop of section 15.1 of the design (issue #768): the kernel
//! and every planner driven together over a model of the meta group, `N`
//! nodes with their journals and placement views, and one abstract Raft
//! group per data group with OpenRaft's membership semantics as section
//! 5.10 lists them.
//!
//! Nothing in the model decides placement or membership: the kernel's
//! dispatcher applies every meta entry, [`super::plan_hosting`] decides what
//! each node runs, [`super::plan_membership_step`] each leader's next step,
//! [`super::plan_decommission`] the decommission driver's moves and
//! [`super::Teardown`] each teardown. The model only carries their outputs
//! out, as the shells will, and adds the adversary.
//!
//! Module map:
//!
//! - `world`: the model, its actions and the invariants C1 to C9.
//! - `traces`: the scripted traces of C6, each passing with every rule on
//!   and failing with its rule off.
//! - `proptests`: the random closed loop, and every cluster shape forming
//!   without faults.

mod proptests;
mod traces;
mod world;
