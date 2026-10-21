//! The Raft WAL of simulated Ursula nodes: the production per-core journal on
//! the simulated disk, one directory per node.
//!
//! A [`SimNodeWal`] opens a group's log store from the node's journals, so
//! every restart recovers from what the simulated disk kept. The node's WAL
//! runs as it does in a process: the first store a stopped node opens starts
//! a run, which reads the run state the previous run left and decides how the
//! journals open. Stopping the node (a power loss, a process crash or a clean
//! shutdown) ends the run. A group reopens only once its stopped engine
//! released the previous store, which an engine's shutdown does before it
//! returns (#709). Before it cuts power or simulates a process crash, it
//! waits until the node's stores are released, as a new process starts only
//! after the old one has exited.
//!
//! The meta group's store is kept apart from the data groups' stores: it is
//! group 0 of the meta journal's own namespace, which keyed with them would
//! stand in for data group 0's.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use ursula_config::WalFsync;
use ursula_control::Admission;
use ursula_raft::JournalTuning;
use ursula_raft::MetaOpenError;
#[cfg(test)]
use ursula_raft::PlacedRaftGroupEngineFactory;
#[cfg(test)]
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupEngineOptions;
use ursula_raft::RaftGroupFileLogStore;
#[cfg(test)]
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftWal;
use ursula_raft::RaftWalError;
#[cfg(test)]
use ursula_raft::WalOpening;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_raft::wal::diagnostics::SimPowerLoss;
#[cfg(test)]
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

/// The Raft node id of a simulated single node.
pub(super) const SINGLE_VOTER: u64 = 1;

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
    run: Arc<Mutex<Option<RaftWal>>>,
    /// The data groups' stores the node handed out.
    stores: Arc<Mutex<BTreeMap<RaftGroupId, Weak<RaftGroupFileLogStore>>>>,
    /// The meta group's store the node handed out last.
    meta: Arc<Mutex<Weak<RaftGroupFileLogStore>>>,
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
            meta: Arc::new(Mutex::new(Weak::new())),
        }
    }

    /// Sets the number of groups before this simulated node first starts.
    pub(super) fn with_group_count(mut self, groups: usize) -> Self {
        self.topology = ursula_shard::StaticShardMap::new(1, groups).expect("valid topology");
        self
    }

    /// The same node directory, whose next run starts with the policy
    /// `fsync`, as when an operator changes `raft.wal.fsync`.
    #[cfg(test)]
    pub(super) fn with_fsync(&self, fsync: WalFsync) -> Self {
        Self {
            root: self.root.clone(),
            tuning: JournalTuning::new(fsync),
            topology: self.topology.clone(),
            run: Arc::new(Mutex::new(None)),
            stores: Arc::new(Mutex::new(BTreeMap::new())),
            meta: Arc::new(Mutex::new(Weak::new())),
        }
    }

    /// The node directory; a power loss of the node covers everything below it.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// The node's current run, started when the node is down.
    fn run(&self) -> Result<RaftWal, RaftWalError> {
        let mut run = self.run.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(run) = run.as_ref() {
            return Ok(run.clone());
        }
        let started = RaftWal::start_with(&self.root, self.tuning, &self.topology)?;
        *run = Some(started.clone());
        Ok(started)
    }

    /// The node's current run, for its snapshot driver, which runs the
    /// journals' maintenance passes (#669).
    pub(super) fn raft_wal(&self) -> RaftWal {
        self.run().expect("start a simulated node's Raft WAL")
    }

    /// How the node's current run opened its journals.
    #[cfg(test)]
    pub(super) fn opening(&self) -> WalOpening {
        self.run()
            .expect("start a simulated node's Raft WAL")
            .opening()
    }

    /// Opens `placement`'s log store from the node's journals. A group
    /// whose previous engine's OpenRaft work still runs is refused, as the
    /// production WAL refuses it (#709).
    pub(super) async fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Arc<RaftGroupFileLogStore> {
        self.try_open(placement, metrics)
            .await
            .expect("open a simulated node's raft log store")
    }

    /// [`SimNodeWal::open`], reporting a journal that does not open. Every
    /// simulated open may create its group, as the engine factories' do.
    pub(super) async fn try_open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, RaftWalError> {
        let store = self.run()?.open(placement, Admission::Create, metrics)?;
        self.stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(placement.raft_group_id, Arc::downgrade(&store));
        Ok(store)
    }

    /// Node 1's replica of `placement`'s group, the group's only voter, as
    /// a single node opens it: through the placed engine factory over the
    /// node's current run, on a store the node hands out. It waits until
    /// the replica leads.
    #[cfg(test)]
    pub(super) async fn single_voter(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<RaftGroupEngine, GroupEngineError> {
        let store = self.open(placement, metrics.clone()).await;
        let factory = PlacedRaftGroupEngineFactory::single_node(
            self.raft_wal(),
            RaftGroupHandleRegistry::default(),
            SINGLE_VOTER,
        );
        let engine = factory.start_on(placement, store, metrics).await?;
        engine
            .wait_for_current_leader(SINGLE_VOTER, Duration::from_secs(5))
            .await?;
        Ok(engine)
    }

    /// Opens the meta group's log store from the node's meta journal, kept
    /// apart from data group 0's store. A meta journal that does not open is
    /// reported, as the production WAL reports it.
    pub(super) fn open_meta(
        &self,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, MetaOpenError> {
        let store = self
            .run()
            .expect("start a simulated node's Raft WAL")
            .open_meta(metrics)?;
        *self
            .meta
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Arc::downgrade(&store);
        Ok(store)
    }

    /// `options`, with the engine recording its OpenRaft work in the node's
    /// current run as the production engine factories do (#709).
    pub(super) fn recording_work(&self, options: RaftGroupEngineOptions) -> RaftGroupEngineOptions {
        options.recording_work_in(&self.run().expect("start a simulated node's Raft WAL"))
    }

    /// Where `placement`'s record of its current snapshot lives in the
    /// node's current run, as the production engine factories place it.
    pub(super) fn snapshot_metadata_path(&self, placement: ShardPlacement) -> PathBuf {
        self.run()
            .expect("start a simulated node's Raft WAL")
            .sim_snapshot_metadata_path(placement)
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

    /// Writes the node's unsynced pages back to its disk while it runs, as
    /// the host's page cache does ([`SimDisk::write_back`]).
    pub(super) fn write_back(&self) {
        SimDisk::write_back(&self.root).expect("write back a simulated node's page cache");
    }

    /// Cuts the node's power after its engines have stopped, and loses
    /// everything written since the last `fsync`.
    pub(super) async fn power_loss_losing_unsynced(&self) -> SimPowerLoss {
        self.stop().await;
        SimDisk::power_loss_losing_unsynced(&self.root)
            .expect("cut the power of a stopped simulated node")
    }

    /// Crashes the node's process after its engines have stopped: the page
    /// cache survives, and the next start begins a new run.
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
    pub(super) async fn clean_shutdown(&self, stop_engines: impl Future<Output = ()>) {
        let held = self
            .handed_out_stores()
            .iter()
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
    async fn stop(&self) {
        self.wait_stopped().await;
        drop(
            self.run
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take(),
        );
    }

    async fn wait_stopped(&self) {
        for store in &self.handed_out_stores() {
            wait_released(store).await;
        }
    }

    /// Every store the node handed out, the meta group's included.
    fn handed_out_stores(&self) -> Vec<Weak<RaftGroupFileLogStore>> {
        let mut stores = self
            .stores
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        stores.push(
            self.meta
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
        );
        stores
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

/// The options of a simulated engine with `metrics` and `cold_store`, which
/// [`SimNodeWal::recording_work`] then records in its node's ledger.
pub(super) fn engine_options(
    metrics: Option<GroupEngineMetrics>,
    cold_store: Option<ursula_runtime::ColdStoreHandle>,
) -> RaftGroupEngineOptions {
    let mut options = RaftGroupEngineOptions::default();
    options.metrics = metrics;
    options.cold_store = cold_store;
    options
}
