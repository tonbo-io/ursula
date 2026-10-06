//! Control-plane state for Ursula dynamic node registration, group placement,
//! and manual group migration.
//!
//! The crate is intentionally pure data plus deterministic state transitions:
//! no I/O, no async, and no wall-clock reads.
//!
//! Module map:
//!
//! - [`command`]: replicated control requests and responses.
//! - [`configuration`]: quorum-observed applied uniform/joint data configurations.
//! - [`cluster`]: immutable routing identity and trusted bootstrap inventory.
//! - [`model`]: nodes, placements and migration records.
//! - [`migration`]: intent-bound managed migrations and executor/evidence ordering.
//! - [`policy`]: managed replication and failure-domain validation.
//! - [`projection`]: complete ordered control snapshots and identity/rollback guards.
//! - [`receiver`]: durable node-local fencing and replica retirement state.
//! - [`state`]: deterministic control state transitions.
//! - [`view`]: routing projections consumed by data nodes and gateways.

mod cluster;
mod command;
mod configuration;
pub use configuration::CommittedGroupConfiguration;
mod migration;
mod model;
mod policy;
mod projection;
mod receiver;
pub use receiver::CompletedMembershipMutation;
pub use receiver::CompletedReceiverMutation;
pub use receiver::MAX_MEMBERSHIP_RECEIPTS;
pub use receiver::MembershipOutcome;
pub use receiver::MembershipStep;
pub use receiver::PendingReceiverMutation;
pub use receiver::ReceiverFencePhase;
pub use receiver::ReceiverFenceRecord;
pub use receiver::ReceiverLedger;
pub use receiver::ReceiverMutationKind;
pub use receiver::ReplicaAssignment;
pub use receiver::ReplicaAssignmentPhase;
pub use receiver::ReplicaMutationResult;
mod state;
mod view;

pub use cluster::ClusterBootstrap;
pub use cluster::ClusterBootstrapRecord;
pub use cluster::ClusterId;
pub use cluster::ClusterIdentity;
pub use cluster::MembershipLogId;
pub use cluster::MetaLocalIdentity;
pub use cluster::NodeRegistration;
pub use cluster::RoutingHashVersion;
pub use cluster::VerifiedGroupMembership;
pub use command::ControlCommand;
pub use command::ControlResponse;
pub use migration::ExecutorAssignment;
pub use migration::FinalMembershipEvidence;
pub use migration::ManagedMigration;
pub use migration::MigrationOperationRequest;
pub use migration::MigrationRequest;
pub use migration::MigrationToken;
pub use migration::MigrationUpdate;
pub use migration::ReceiverProcess;
pub use migration::ReplicaAppliedEvidence;
pub use migration::ReplicaRetirementEvidence;
pub use model::ClusterNode;
pub use model::DataGroupPlacement;
pub use model::GroupMigration;
pub use model::LearnerStatus;
pub use model::MetaConfig;
pub use model::MigrationPhase;
pub use model::NodeId;
pub use model::NodeState;
pub use policy::GroupPlacementPolicy;
pub use policy::GroupPolicyOverride;
pub use policy::ManagedPlacement;
pub use policy::PlacementPolicy;
pub use policy::ReplicationFactor;
pub use state::ControlPlaneState;
pub use view::GroupPlacementView;
pub use view::PlacementNode;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod policy_tests;

#[cfg(test)]
mod cluster_tests;
#[cfg(test)]
mod migration_tests;

pub use projection::ControlProjection;
pub use projection::ProjectionCursor;
pub use projection::ProjectionInstall;
