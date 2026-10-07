//! The Raft WAL of simulated Ursula nodes: the production per-core journal on
//! the simulated disk, one directory per node.
//!
//! A [`SimNodeWal`] opens a group's log store from the node's journals, so
//! every restart recovers from what the simulated disk kept. Before it reopens
//! a group, cuts power or simulates a process crash, it waits until the stopped
//! engine has released the previous store, as a new process starts only after
//! the old one has exited.

use std::collections::BTreeMap;
#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use ursula_raft::DurableRaftLogStoreFactory;
use ursula_raft::RaftGroupFileLogStore;
use ursula_raft::SimDisk;
#[cfg(test)]
use ursula_raft::SimPowerLoss;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

/// Scheduler turns a stopped engine gets to drop its store before the wait
/// falls back to advancing simulated time.
const RELEASE_YIELDS: usize = 64;
const RELEASE_SLEEPS: usize = 1_000;

/// One simulated node's journals.
#[derive(Clone)]
pub(crate) struct SimNodeWal {
    #[cfg(test)]
    root: PathBuf,
    factory: DurableRaftLogStoreFactory,
    stores: Arc<Mutex<BTreeMap<RaftGroupId, Weak<RaftGroupFileLogStore>>>>,
}

impl SimNodeWal {
    /// Provisions a new, durable node directory named after `name`.
    pub(super) fn provision(name: &str) -> Self {
        let root = SimDisk::provision_dir(name).expect("provision a simulated node directory");
        Self {
            factory: DurableRaftLogStoreFactory::new(&root),
            #[cfg(test)]
            root,
            stores: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The node directory; a power loss of the node covers everything below it.
    #[cfg(test)]
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// Opens `placement`'s log store from the node's journals, waiting first
    /// until the store the group had before is released.
    pub(super) async fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Arc<RaftGroupFileLogStore> {
        if let Some(previous) = self.handed_out(placement.raft_group_id) {
            wait_released(&previous).await;
        }
        let store = self
            .factory
            .open(placement, metrics)
            .expect("open a simulated node's raft log store");
        self.stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(placement.raft_group_id, Arc::downgrade(&store));
        store
    }

    /// The group's open store, if its engine still holds it.
    pub(super) fn store(&self, raft_group_id: RaftGroupId) -> Option<Arc<RaftGroupFileLogStore>> {
        self.handed_out(raft_group_id)?.upgrade()
    }

    /// Cuts the node's power after its engines have stopped: unsynced pages
    /// and directory entries may be lost.
    #[cfg(test)]
    pub(super) async fn power_loss(&self) -> SimPowerLoss {
        self.wait_stopped().await;
        SimDisk::power_loss(&self.root).expect("cut the power of a stopped simulated node")
    }

    /// Crashes the node's process after its engines have stopped: the page
    /// cache survives.
    #[cfg(test)]
    pub(super) async fn process_crash(&self) {
        self.wait_stopped().await;
        SimDisk::process_crash(&self.root).expect("crash a stopped simulated node");
    }

    #[cfg(test)]
    async fn wait_stopped(&self) {
        let stores = self
            .stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for store in &stores {
            wait_released(store).await;
        }
    }

    fn handed_out(&self, raft_group_id: RaftGroupId) -> Option<Weak<RaftGroupFileLogStore>> {
        self.stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&raft_group_id)
            .cloned()
    }
}

/// Metrics for a store whose engine reports none, sized for `placement`.
pub(super) fn standalone_wal_metrics(placement: ShardPlacement) -> GroupEngineMetrics {
    RuntimeMetrics::new(
        usize::from(placement.core_id.0).saturating_add(1),
        usize::try_from(placement.raft_group_id.0)
            .unwrap_or(usize::MAX)
            .saturating_add(1),
    )
    .group_engine_metrics()
}

async fn wait_released(store: &Weak<RaftGroupFileLogStore>) {
    for _ in 0..RELEASE_YIELDS {
        if store.strong_count() == 0 {
            return;
        }
        madsim::task::yield_now().await;
    }
    for _ in 0..RELEASE_SLEEPS {
        if store.strong_count() == 0 {
            return;
        }
        madsim::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        store.strong_count(),
        0,
        "a stopped simulated engine still holds its raft log store"
    );
}
