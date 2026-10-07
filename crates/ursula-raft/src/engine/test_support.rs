//! A single-node runtime on the per-core journal for the engine's tests.

use std::ops::Deref;
use std::sync::Arc;

use ursula_config::WalFsync;
use ursula_runtime::ColdStore;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;

use super::DurableRaftGroupEngineFactory;
use crate::log_store::RaftWal;

/// A runtime of single-node Raft groups whose journals live in a temporary
/// directory that is removed with it.
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

/// Spawns a runtime of single-node groups with `cold_store`, on a fresh WAL.
pub(crate) fn spawn_journal_runtime(
    config: RuntimeConfig,
    cold_store: Option<Arc<ColdStore>>,
) -> JournalRuntime {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let log_stores = RaftWal::start(
        wal_root.path(),
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(config.core_count, config.raft_group_count)
            .expect("valid topology"),
    )
    .expect("start the WAL");
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        DurableRaftGroupEngineFactory::with_cold_store(log_stores, cold_store.clone()),
        cold_store,
    )
    .expect("spawn raft runtime");
    JournalRuntime {
        runtime,
        _wal_root: wal_root,
    }
}
