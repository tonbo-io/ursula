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
//! - `admin`: the admin JSON types of the placement routes.

mod admin;
mod apply;
mod command;
mod seed;
mod state;
mod view;

pub use admin::ClusterView;
pub use admin::GenesisClaimView;
pub use admin::MoveLimitRequest;
pub use admin::MoveRequest;
pub use admin::PlacementApiError;
pub use admin::PlacementApiReason;
pub use command::BootstrapProblem;
pub use command::ControlApplied;
pub use command::ControlCommand;
pub use command::ControlError;
pub use command::ControlResponse;
pub use command::HistoryBegun;
pub use command::HistoryCause;
pub use command::LifecycleChange;
pub use seed::default_meta_voters;
pub use seed::effective_replication_factor;
pub use seed::initializer;
pub use seed::seed_voters;
pub use state::Cluster;
pub use state::ClusterParams;
pub use state::ControlPlaneState;
pub use state::GroupCount;
pub use state::GroupPlacement;
pub use state::MaintenanceAuthority;
pub use state::Move;
pub use state::MoveLimit;
pub use state::Placements;
pub use state::Registration;
pub use state::ReplicationFactor;
pub use state::Retirement;
pub use state::Target;
pub use view::ConfirmedStamp;
pub use view::ConfirmedView;
pub use view::PlacementView;
pub use view::UnconfirmedView;

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod samples;
#[cfg(test)]
mod tests;
