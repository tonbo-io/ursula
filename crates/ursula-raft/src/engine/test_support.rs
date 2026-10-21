//! Single nodes on the per-core journal for the engine's tests: a runtime,
//! and one group's replica, each the only voter of its group and opened
//! through the placed engine factory.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use ursula_config::WalFsync;
use ursula_runtime::ColdStore;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::ProposedUpload;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use super::RaftGroupEngine;
use crate::PlacedRaftGroupEngineFactory;
use crate::RaftGroupHandleRegistry;
use crate::RaftWal;

/// The Raft node id of a test's single node.
pub(crate) const SINGLE_VOTER: u64 = 1;

/// The groups of a single node, [`SINGLE_VOTER`], over `wal`, registered in a
/// registry of their own.
pub(crate) fn single_node_factory(wal: &RaftWal) -> PlacedRaftGroupEngineFactory {
    PlacedRaftGroupEngineFactory::single_node(
        wal.clone(),
        RaftGroupHandleRegistry::default(),
        SINGLE_VOTER,
    )
}

/// Metrics for one group's engine, sized for `placement`.
pub(crate) fn engine_metrics(placement: ShardPlacement) -> GroupEngineMetrics {
    ursula_runtime::RuntimeMetrics::new(
        usize::from(placement.core_id.0).saturating_add(1),
        usize::try_from(placement.raft_group_id.0)
            .unwrap_or(usize::MAX)
            .saturating_add(1),
    )
    .group_engine_metrics()
}

/// The single node's replica of `placement`'s group over `wal`, once it
/// leads.
pub(crate) async fn single_voter(wal: &RaftWal, placement: ShardPlacement) -> RaftGroupEngine {
    open_single_voter(&single_node_factory(wal), placement).await
}

/// `factory`'s replica of `placement`'s group, once it leads: the open of a
/// group's only voter, which a test that writes at once waits for.
pub(crate) async fn open_single_voter(
    factory: &PlacedRaftGroupEngineFactory,
    placement: ShardPlacement,
) -> RaftGroupEngine {
    let engine = factory
        .open(placement, engine_metrics(placement))
        .await
        .expect("open the group's only voter");
    leading(engine).await
}

/// `factory`'s replica of `placement`'s group on `store`, a log store the
/// test opened from the factory's WAL, once it leads.
pub(crate) async fn start_single_voter_on(
    factory: &PlacedRaftGroupEngineFactory,
    placement: ShardPlacement,
    store: Arc<crate::RaftGroupFileLogStore>,
) -> RaftGroupEngine {
    let engine = factory
        .start_on(placement, store, engine_metrics(placement))
        .await
        .expect("start the group's only voter");
    leading(engine).await
}

/// `engine`, once the single node leads its group.
async fn leading(engine: RaftGroupEngine) -> RaftGroupEngine {
    engine
        .wait_for_current_leader(SINGLE_VOTER, Duration::from_secs(5))
        .await
        .expect("the group's only voter leads it");
    engine
}

/// A runtime of a single node's Raft groups whose journals live in a
/// temporary directory that is removed with it.
pub(crate) struct JournalRuntime {
    runtime: ShardRuntime,
    _wal_root: tempfile::TempDir,
}

impl Deref for JournalRuntime {
    type Target = ShardRuntime;

    fn deref(&self) -> &ShardRuntime {
        &self.runtime
    }
}

/// Spawns a single node's runtime with `cold_store`, on a fresh WAL.
pub(crate) fn spawn_journal_runtime(
    config: RuntimeConfig,
    cold_store: Option<Arc<ColdStore>>,
) -> JournalRuntime {
    spawn_journal_runtime_published(config, cold_store, None)
}

/// [`spawn_journal_runtime`] whose groups publish their resources, their
/// history among them, to `registry`.
pub(crate) fn spawn_journal_runtime_published(
    config: RuntimeConfig,
    cold_store: Option<Arc<ColdStore>>,
    registry: Option<RaftGroupHandleRegistry>,
) -> JournalRuntime {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let log_stores = RaftWal::start(
        wal_root.path(),
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(config.core_count, config.raft_group_count)
            .expect("valid topology"),
    )
    .expect("start the WAL");
    let factory = PlacedRaftGroupEngineFactory::single_node(
        log_stores,
        registry.unwrap_or_default(),
        SINGLE_VOTER,
    )
    .with_cold_store(cold_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)
            .expect("spawn raft runtime");
    JournalRuntime {
        runtime,
        _wal_root: wal_root,
    }
}

/// An upload a test proposes on `runtime`'s replica of `stream_id`'s group,
/// fenced to the leadership that replica leads in now (F14j).
pub(crate) async fn leader_upload(
    runtime: &ShardRuntime,
    stream_id: &BucketStreamId,
) -> Arc<ProposedUpload> {
    let fence = runtime
        .cold_upload_fence(runtime.locate(stream_id))
        .await
        .expect("read the upload fence")
        .expect("the replica leads its group");
    ProposedUpload::new(fence)
}
