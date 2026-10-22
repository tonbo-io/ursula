//! The same OpenRaft storage contract as the native file-store suite, using
//! SimDisk rather than a host temporary directory. Power-loss/reordering faults
//! are exercised separately by `ursula-sim`'s disk and recovery suites.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use openraft::StorageError;
use openraft::testing::log::StoreBuilder;
use openraft::testing::log::Suite;
use openraft::type_config::TypeConfigExt;
use ursula_config::WalFsync;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::StaticShardMap;

use super::placement;
use crate::RaftWal;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::SimDisk;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Default)]
struct SimLogStoreBuilder(AtomicU64);

impl StoreBuilder<UrsulaRaftTypeConfig, Arc<RaftGroupFileLogStore>, RaftGroupStateMachine, ()>
    for SimLogStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<
        ((), Arc<RaftGroupFileLogStore>, RaftGroupStateMachine),
        StorageError<UrsulaRaftTypeConfig>,
    > {
        let ordinal = self.0.fetch_add(1, Ordering::Relaxed);
        let root = SimDisk::provision_dir(&format!("conformance-{ordinal}"))
            .map_err(|err| StorageError::write(UrsulaRaftTypeConfig::err_from_error(&err)))?;
        let wal = RaftWal::start(
            root,
            WalFsync::Always,
            &StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .map_err(|err| StorageError::write(UrsulaRaftTypeConfig::err_from_error(&err)))?;
        let store = wal
            .open(
                placement(),
                ursula_control::Admission::Create,
                RuntimeMetrics::new(1, 1).group_engine_metrics(),
            )
            .map_err(|err| StorageError::write(UrsulaRaftTypeConfig::err_from_error(&err)))?;
        Ok(((), store, RaftGroupStateMachine::new(placement())))
    }
}

#[test]
fn simulated_journal_passes_openraft_storage_conformance() {
    super::check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            Suite::test_all(SimLogStoreBuilder::default())
                .await
                .expect("OpenRaft simulated journal conformance");
        })
        .await;
    });
}

#[test]
fn simulated_append_batch_reserves_hot_capacity_across_unapplied_entries() {
    super::check_madsim_determinism(11, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(11, async {
            // A single node's group, its only voter, once it leads.
            let factory = crate::PlacedRaftGroupEngineFactory::single_node(
                super::sim_wal("batch-capacity"),
                crate::RaftGroupHandleRegistry::default(),
                1,
            );
            let mut engine = factory
                .open(
                    placement(),
                    RuntimeMetrics::new(1, 1).group_engine_metrics(),
                )
                .await
                .expect("create simulated engine");
            engine
                .wait_for_current_leader(1, std::time::Duration::from_secs(5))
                .await
                .expect("the group's only voter leads it");
            super::assert_append_batch_reserves_hot_capacity(&mut engine, placement()).await;
            ursula_runtime::GroupEngine::shutdown(&mut engine)
                .await
                .unwrap();
        })
        .await;
    });
}
