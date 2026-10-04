//! Linearizable reads confirmed outside the group actor (D10, PR10b).
//!
//! A group actor runs one command at a time. A ReadIndex confirmation waits
//! one quorum round trip (longer on a degraded quorum), so running it inside
//! the actor would hold every command queued behind it. Instead the runtime
//! asks the group's [`LinearizableReadBarrier`] before it queues a
//! linearizable read, and stamps the confirmed index on the request
//! (`read_index`). The engine then serves the read from local state at or
//! after that index without awaiting the network.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::RwLock;

use ursula_shard::RaftGroupId;

use crate::engine::GroupEngineError;

/// Outcome of one confirmation: `Ok(Some(index))` once this replica, the
/// confirmed leader, has applied the read index; `Ok(None)` when it does not
/// lead (the engine forwards or refuses as before); `Err` when confirmation
/// failed (forward to the new leader, or leader unknown).
pub type ReadIndexFuture =
    Pin<Box<dyn Future<Output = Result<Option<u64>, GroupEngineError>> + Send + 'static>>;

/// A group's ReadIndex confirmation, callable without the group actor.
pub trait LinearizableReadBarrier: Send + Sync + 'static {
    /// Confirms a read index that was taken after this call started.
    /// Concurrent callers may share one confirmation round.
    fn confirm(&self) -> ReadIndexFuture;
}

/// The barrier of every group hosted on this node, installed by the core
/// worker when it starts a group actor and removed when it shuts one down.
/// A group without one (not started yet, or an engine without a barrier)
/// is linearized inside its engine, as before.
#[derive(Clone, Default)]
pub(crate) struct ReadIndexBarriers {
    groups: Arc<RwLock<HashMap<RaftGroupId, SharedBarrier>>>,
}

type SharedBarrier = Arc<dyn LinearizableReadBarrier>;

impl std::fmt::Debug for ReadIndexBarriers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadIndexBarriers").finish_non_exhaustive()
    }
}

impl ReadIndexBarriers {
    pub(crate) fn install(&self, group_id: RaftGroupId, barrier: Option<SharedBarrier>) {
        let mut groups = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        match barrier {
            Some(barrier) => groups.insert(group_id, barrier),
            None => groups.remove(&group_id),
        };
    }

    pub(crate) fn remove(&self, group_id: RaftGroupId) {
        self.groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&group_id);
    }

    pub(crate) fn get(&self, group_id: RaftGroupId) -> Option<SharedBarrier> {
        self.groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&group_id)
            .cloned()
    }
}
