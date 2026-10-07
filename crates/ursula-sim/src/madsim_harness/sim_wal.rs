//! The Raft WAL of simulated Ursula nodes: the production per-core journal on
//! the simulated disk, one directory per node.
//!
//! A [`SimNodeWal`] opens a group's log store from the node's journals, so
//! every restart recovers from what the simulated disk kept. The node's WAL
//! runs as it does in a process: the first store a stopped node opens starts
//! a run, which reads the run state the previous run left and decides how the
//! journals open. Stopping the node (a power loss, a process crash or a clean
//! shutdown) ends the run. Before it reopens a group, cuts power or simulates
//! a process crash, it waits until the stopped engine has released the
//! previous store, as a new process starts only after the old one has exited.

use std::collections::BTreeMap;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use ursula_config::WalFsync;
use ursula_raft::DurableRaftLogStoreFactory;
use ursula_raft::JournalTuning;
use ursula_raft::RaftGroupFileLogStore;
use ursula_raft::SimDisk;
#[cfg(test)]
use ursula_raft::SimPowerLoss;
#[cfg(test)]
use ursula_raft::WalOpening;
use ursula_runtime::GroupEngineError;
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
    root: PathBuf,
    tuning: JournalTuning,
    topology: ursula_shard::StaticShardMap,
    /// The node's current run of the WAL; `None` while the node is down.
    run: Arc<Mutex<Option<DurableRaftLogStoreFactory>>>,
    stores: Arc<Mutex<BTreeMap<RaftGroupId, Weak<RaftGroupFileLogStore>>>>,
}

impl SimNodeWal {
    /// Provisions a new, durable node directory named after `name`, whose
    /// journals are `fsync`ed on every append.
    pub(super) fn provision(name: &str) -> Self {
        Self::provision_with_fsync(name, WalFsync::Always)
    }

    /// Provisions a new node directory whose WAL runs with `fsync`, the
    /// simulator's small segments and small entry caches.
    pub(super) fn provision_with_fsync(name: &str, fsync: WalFsync) -> Self {
        Self::provision_with_tuning(name, JournalTuning::new(fsync))
    }

    /// Provisions a new node directory whose WAL runs with `tuning`.
    pub(super) fn provision_with_tuning(name: &str, tuning: JournalTuning) -> Self {
        let root = SimDisk::provision_dir(name).expect("provision a simulated node directory");
        Self {
            root,
            tuning,
            topology: ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
            run: Arc::new(Mutex::new(None)),
            stores: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Sets the number of groups before this simulated node first starts.
    #[cfg(test)]
    pub(super) fn with_group_count(mut self, groups: usize) -> Self {
        self.topology = ursula_shard::StaticShardMap::new(1, groups).expect("valid topology");
        self
    }

    /// The node directory; a power loss of the node covers everything below it.
    #[cfg(test)]
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// The node's current run, started when the node is down.
    fn run(&self) -> Result<DurableRaftLogStoreFactory, GroupEngineError> {
        let mut run = self.run.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(run) = run.as_ref() {
            return Ok(run.clone());
        }
        let started =
            DurableRaftLogStoreFactory::start_with(&self.root, self.tuning, &self.topology)
                .map_err(|err| GroupEngineError::new(format!("start the Raft WAL: {err}")))?;
        *run = Some(started.clone());
        Ok(started)
    }

    /// How the node's current run opened its journals.
    #[cfg(test)]
    pub(super) fn opening(&self) -> WalOpening {
        self.run()
            .expect("start a simulated node's Raft WAL")
            .opening()
    }

    /// Opens `placement`'s log store from the node's journals, waiting first
    /// until the store the group had before is released.
    pub(super) async fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Arc<RaftGroupFileLogStore> {
        self.try_open(placement, metrics)
            .await
            .expect("open a simulated node's raft log store")
    }

    /// [`SimNodeWal::open`], reporting a journal that does not open.
    pub(super) async fn try_open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, GroupEngineError> {
        if let Some(previous) = self.handed_out(placement.raft_group_id) {
            wait_released(&previous).await;
        }
        let store = self.run()?.open(placement, metrics)?;
        self.stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(placement.raft_group_id, Arc::downgrade(&store));
        Ok(store)
    }

    /// The group's open store, if its engine still holds it.
    pub(super) fn store(&self, raft_group_id: RaftGroupId) -> Option<Arc<RaftGroupFileLogStore>> {
        self.handed_out(raft_group_id)?.upgrade()
    }

    /// Cuts the node's power after its engines have stopped: unsynced pages
    /// and directory entries may be lost, and the host boots anew.
    #[cfg(test)]
    pub(super) async fn power_loss(&self) -> SimPowerLoss {
        self.stop().await;
        SimDisk::power_loss(&self.root).expect("cut the power of a stopped simulated node")
    }

    /// Crashes the node's process after its engines have stopped: the page
    /// cache survives.
    #[cfg(test)]
    pub(super) async fn process_crash(&self) {
        self.stop().await;
        SimDisk::process_crash(&self.root).expect("crash a stopped simulated node");
    }

    /// Shuts the node down gracefully, as the server does: `stop_engines`
    /// stops its Raft groups, then every core writer `fsync`s its journal and
    /// the run is recorded as clean.
    ///
    /// A production writer that loses its last handle `fsync`s on its way
    /// out; a simulated one stops like a killed process. So the node's
    /// stores are held while the engines stop, and the writers close here.
    #[cfg(test)]
    pub(super) async fn clean_shutdown(&self, stop_engines: impl Future<Output = ()>) {
        let held = self
            .stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        stop_engines.await;
        let run = self
            .run
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(run) = run {
            run.shutdown()
                .await
                .expect("shut down a simulated node's Raft WAL");
        }
        drop(held);
        self.wait_stopped().await;
    }

    /// Ends the node's run without a clean shutdown, as a stopped process
    /// does.
    #[cfg(test)]
    async fn stop(&self) {
        self.wait_stopped().await;
        drop(
            self.run
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take(),
        );
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
