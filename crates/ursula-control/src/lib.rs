//! Control-plane state for Ursula dynamic node registration, group placement,
//! and manual group migration.
//!
//! The crate is intentionally pure data plus deterministic state transitions:
//! no I/O, no async, and no wall-clock reads.
//!
//! Module map:
//!
//! - [`command`]: replicated control requests and responses.
//! - [`cluster`]: immutable routing identity and trusted bootstrap inventory.
//! - [`model`]: nodes, placements and migration records.
//! - [`policy`]: managed replication and failure-domain validation.
//! - [`state`]: deterministic control state transitions.
//! - [`view`]: routing projections consumed by data nodes and gateways.

mod cluster;
mod command;
mod model;
mod policy;
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
