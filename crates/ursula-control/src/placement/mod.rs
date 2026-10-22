//! The placement kernel of the meta group: desired state, its commands and
//! their pure dispatcher.
//!
//! Module map:
//!
//! - `state`: the replicated state, placements, targets, moves and
//!   retirements.
//! - `command`: the seven commands, their typed answers and errors.
//! - `apply`: the dispatcher and the history event, pure and deterministic.
//! - `seed`: where Bootstrap places each group and who initializes it.
//! - `view`: placement views and their confirmation.
//! - `reconcile`: the group leader's membership planner, one voter per step
//!   toward the placement's target.
//! - `hosting`: what a node runs, creates and tears down, the teardown
//!   steps, the seed role, whom a node asks about a group, and the one
//!   voter predicate with its left mark.
//! - `decommission`: the meta leader's moves off decommissioning nodes.
//! - `admin`: the admin JSON types of the placement routes.
//! - `samples` (tests and `test-support` only): one sample of every root.
//! - `rules`: the switches of the rules the closed loop's negative traces
//!   turn off, every rule always on outside tests.
//! - `closed_loop` (tests only): the kernel and every planner driven
//!   together over a model of meta, the nodes and their groups, checked
//!   against C1 to C9, with the scripted negative traces of C6.

mod admin;
mod apply;
mod command;
mod decommission;
mod hosting;
mod reconcile;
mod rules;
mod seed;
mod state;
mod view;

pub use admin::ClusterView;
pub use admin::MetaDamageKind;
pub use admin::MetaLeaderReport;
pub use admin::MetaReplicaStatus;
pub use admin::MetaRole;
pub use admin::MoveLimitRequest;
pub use admin::MoveLimitResponse;
pub use admin::MoveRequest;
pub use admin::MoveResponse;
pub use admin::PlacementApiError;
pub use admin::PlacementApiReason;
pub use admin::PlacementGroupStatus;
pub use admin::PlacementStatus;
pub use admin::RegistrationStatus;
pub use admin::ReplaceDiskRequest;
pub use command::BootstrapProblem;
pub use command::ControlApplied;
pub use command::ControlCommand;
pub use command::ControlError;
pub use command::ControlResponse;
pub use command::HistoryBegun;
pub use command::HistoryCause;
pub use command::LifecycleChange;
pub use command::Requeue;
pub use decommission::BlockReason;
pub use decommission::BlockedMove;
pub use decommission::DecommissionMove;
pub use decommission::DecommissionPlan;
pub use decommission::NodeEligibility;
pub use decommission::free_slots;
pub use decommission::plan_decommission;
pub use hosting::Admission;
pub use hosting::Asked;
pub use hosting::CandidatesOf;
pub use hosting::Counts;
pub use hosting::HostingInput;
pub use hosting::HostingPlan;
pub use hosting::HostingStep;
pub use hosting::LeaderCandidates;
pub use hosting::LeftMark;
pub use hosting::LocalLog;
pub use hosting::LocalReplica;
pub use hosting::ReplicaMembership;
pub use hosting::SeedRoleOf;
pub use hosting::Standing;
pub use hosting::Teardown;
pub use hosting::TeardownEnd;
pub use hosting::TeardownEvent;
pub use hosting::TeardownOutcome;
pub use hosting::TeardownStep;
pub use hosting::ViewInput;
pub use hosting::counts_as_voter;
pub use hosting::leader_candidates;
pub use hosting::plan_hosting;
pub use hosting::seed_role;
pub use hosting::shown_seed_role;
pub use reconcile::EffectiveMembership;
pub use reconcile::LeaderView;
pub use reconcile::Leadership;
pub use reconcile::MembershipConfig;
pub use reconcile::MembershipStep;
pub use reconcile::MembershipTarget;
pub use reconcile::MembershipView;
pub use reconcile::PeerGate;
pub use reconcile::TargetStage;
pub use reconcile::ViewFreshness;
pub use reconcile::membership_answer;
pub use reconcile::plan_membership_step;
pub use seed::default_meta_voters;
pub use seed::effective_replication_factor;
pub use seed::initializer;
pub use seed::seed_voters;
pub use state::Cluster;
pub use state::ClusterParams;
pub use state::ControlPlaneState;
pub use state::GroupPlacement;
pub use state::Move;
pub use state::MoveLimit;
pub use state::Placements;
pub use state::Registration;
pub use state::Retirement;
pub use state::Target;
pub use state::group_ids;
pub use view::ConfirmedStamp;
pub use view::ConfirmedView;
pub use view::PlacementView;
pub use view::UnconfirmedView;

#[cfg(test)]
mod closed_loop;
#[cfg(test)]
mod proptests;
#[cfg(any(test, feature = "test-support"))]
pub mod samples;
#[cfg(test)]
mod tests;
