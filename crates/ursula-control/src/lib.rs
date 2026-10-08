//! Control-plane state for Ursula dynamic node registration, group placement,
//! and one deterministic maintenance operation model.
//!
//! The crate is intentionally pure data plus deterministic state transitions:
//! no I/O, no async, and no wall-clock reads.

//! Module map:
//!
//! - `command`: control inputs and typed responses.
//! - `identity`: process and durable replica identity values.
//! - `model`: registered nodes and committed placement.
//! - `operation`: maintenance ownership, actions and verified evidence.
//! - `state`: control-command dispatcher.
//! - `view`: read-only placement projections.

mod command;
mod identity;
mod model;
mod operation;
mod state;
mod view;

pub use command::ControlCommand;
pub use command::ControlError;
pub use command::ControlResponse;
pub use command::NodeEndpoint;
pub use model::ClusterNode;
pub use model::DataGroupPlacement;
pub use model::MetaConfig;
pub use model::NodeId;
pub use model::NodeState;
pub use state::ControlPlaneState;
pub use view::GroupPlacementView;
pub use view::PlacementNode;

#[cfg(test)]
mod tests;

pub use identity::InvalidProcessIncarnation;
pub use identity::ProcessIncarnation;
pub use identity::ReplicaIdentity;
pub use operation::ActionOutcome;
pub use operation::ActionSequence;
pub use operation::ExecutorGeneration;
pub use operation::MaintenanceOperation;
pub use operation::MembershipAction;
pub use operation::OperationAction;
pub use operation::OperationCommand;
pub use operation::OperationError;
pub use operation::OperationId;
pub use operation::OperationKind;
pub use operation::OperationOutcome;
pub use operation::OperationPhase;
pub use operation::OperationState;
pub use operation::OperationToken;
pub use operation::PendingAction;
pub use operation::PrefixEvidence;
pub use operation::ProcessIdentity;
pub use operation::ProcessState;
pub use operation::ReplicaEvidence;
pub use operation::ReplicaState;
pub use operation::RetirementReason;
