//! Pure identity values shared by the operation kernel and its adapters.

use serde::Deserialize;
use serde::Serialize;
pub use ursula_proto::admin::InvalidProcessIncarnation;
pub use ursula_proto::admin::ProcessIncarnation;

/// One durable replica lifetime, preserved across ordinary process restarts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReplicaIdentity {
    pub generation: u64,
    pub incarnation: ProcessIncarnation,
}
