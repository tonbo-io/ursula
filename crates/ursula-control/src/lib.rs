//! Control-plane state for Ursula dynamic node registration, group placement,
//! and manual group migration.
//!
//! The crate is intentionally pure data plus deterministic state transitions:
//! no I/O, no async, and no wall-clock reads.

mod command;
mod model;
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
pub use state::ControlPlaneState;
pub use view::GroupPlacementView;
pub use view::PlacementNode;

#[cfg(test)]
mod tests;
