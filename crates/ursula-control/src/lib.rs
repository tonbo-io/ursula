//! Control-plane state for Ursula dynamic node registration, group placement,
//! and manual group migration.
//!
//! The crate is intentionally pure data plus deterministic state transitions:
//! no I/O, no async, and no wall-clock reads.
//!
//! Module map:
//! - [`command`]: replicated commands and responses.
//! - [`model`]: nodes, placement and migration metadata.
//! - [`operation`]: maintenance ownership, process epochs and prefix evidence.
//! - [`state`]: deterministic dispatcher and placement updates.
//! - [`view`]: placement views for routing.

mod command;
mod model;
mod operation;
mod state;
mod view;

pub use command::ControlCommand;
pub use command::ControlResponse;
pub use model::ClusterNode;
pub use model::DataGroupPlacement;
pub use model::GroupMigration;
pub use model::LearnerStatus;
pub use model::MetaConfig;
pub use model::MigrationPhase;
pub use model::NodeId;
pub use model::NodeState;
pub use operation::ActionRejection;
pub use operation::ActionRequest;
pub use operation::MaintenanceOperation;
pub use operation::MembershipAction;
pub use operation::OperationAction;
pub use operation::OperationCommand;
pub use operation::OperationError;
pub use operation::OperationKind;
pub use operation::OperationOutcome;
pub use operation::OperationPhase;
pub use operation::OperationRequest;
pub use operation::OperationState;
pub use operation::OperationSubmissionError;
pub use operation::OperationToken;
pub use operation::PrefixEvidence;
pub use operation::ProcessIdentity;
pub use operation::ProcessState;
pub use operation::ReplicaEvidence;
pub use operation::ReplicaState;
pub use operation::RetirementReason;
pub use state::ControlPlaneState;
pub use view::GroupPlacementView;
pub use view::PlacementNode;

#[cfg(test)]
mod replica_identity_tests;
#[cfg(test)]
mod tests;
