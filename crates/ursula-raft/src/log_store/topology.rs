//! Immutable routing configuration of a node's WAL root.

use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::StaticShardMap;

use super::RaftWalError;
use super::state_file;
use super::state_file::StateFileKind;

const TOPOLOGY_FILE: &str = "topology.bin";

/// The routing counts persisted before any group can write its journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalTopology {
    pub core_count: u16,
    pub group_count: u32,
}

impl From<&StaticShardMap> for WalTopology {
    fn from(map: &StaticShardMap) -> Self {
        Self {
            core_count: map.core_count(),
            group_count: map.raft_group_count(),
        }
    }
}

/// Called under the root lock, before recovery or run-state writes. Even
/// metadata-only cores count as old storage: votes must never be forgotten.
pub(super) fn check_or_create(
    root: &Path,
    map: &StaticShardMap,
    has_run_state: bool,
    cores: &[PathBuf],
) -> Result<(), RaftWalError> {
    let configured = WalTopology::from(map);
    let path = root.join(TOPOLOGY_FILE);
    match state_file::read::<WalTopology>(StateFileKind::Topology, &path)
        .map_err(RaftWalError::ReadTopology)?
    {
        Some(stored) if stored != configured => {
            return Err(RaftWalError::TopologyMismatch {
                root: root.to_owned(),
                stored,
                configured,
            });
        }
        Some(_) => return Ok(()),
        None if has_run_state || !cores.is_empty() => {
            return Err(RaftWalError::MissingTopology {
                root: root.to_owned(),
            });
        }
        None => {}
    }
    state_file::write(
        StateFileKind::Topology,
        &path,
        &path.with_extension("tmp"),
        &configured,
    )
    .map_err(RaftWalError::RecordTopology)?;
    Ok(())
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use openraft::storage::IOFlushed;
    use openraft::storage::RaftLogReader;
    use openraft::storage::RaftLogStorage;
    use openraft::vote::RaftLeaderId;
    use ursula_config::WalFsync;
    use ursula_runtime::RuntimeMetrics;
    use ursula_shard::RaftGroupId;

    use super::*;
    use crate::DurableRaftLogStoreFactory;
    use crate::GroupRejoin;
    use crate::log_store::RUN_STATE_FILE;
    use crate::types::UrsulaRaftTypeConfig;

    #[tokio::test]
    async fn topology_changes_are_refused_before_old_votes_can_be_reused() {
        let dir = tempfile::tempdir().unwrap();
        let original = StaticShardMap::new(4, 8).unwrap();
        let placement = original.placement(RaftGroupId(4)).unwrap();
        let metrics = RuntimeMetrics::new(4, 8).group_engine_metrics();
        let wal =
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always, &original).unwrap();
        let mut store = wal.open(placement, metrics.clone()).unwrap();
        let vote = crate::types::UrsulaVote::new(7, 1);
        store.save_vote(&vote).await.unwrap();
        type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
        let entry = openraft::alias::EntryOf::<UrsulaRaftTypeConfig> {
            log_id: openraft::LogId::new(LeaderId::new(7, 1), 1),
            payload: openraft::EntryPayload::Blank,
        };
        store.append([entry], IOFlushed::noop()).await.unwrap();
        wal.shutdown().await.unwrap();
        drop((store, wal));
        let run_before = std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap();
        let topology_before = std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap();
        for changed in [
            StaticShardMap::new(8, 8).unwrap(),
            StaticShardMap::new(4, 16).unwrap(),
        ] {
            let error = DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always, &changed)
                .unwrap_err();
            assert!(
                matches!(error, RaftWalError::TopologyMismatch { stored, configured, .. }
                if stored == WalTopology::from(&original) && configured == WalTopology::from(&changed))
            );
            assert_eq!(
                std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap(),
                run_before
            );
            assert_eq!(
                std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap(),
                topology_before
            );
            assert!(!dir.path().join("core-4").exists());
        }
        let wal =
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always, &original).unwrap();
        let mut store = wal.open(placement, metrics).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.try_get_log_entries(1..=1).await.unwrap().len(), 1);
        assert!(GroupRejoin::durable(1, placement.raft_group_id, &store).vote_gate_open());
        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn topology_is_required_even_without_journal_records() {
        let topology = StaticShardMap::new(4, 64).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let wal =
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Never, &topology).unwrap();
        wal.shutdown().await.unwrap();
        drop(wal);
        std::fs::remove_file(dir.path().join(TOPOLOGY_FILE)).unwrap();
        assert!(matches!(
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::MissingTopology { .. })
        ));
        // Losing the run state must not bless an old core's vote-only metadata.
        std::fs::remove_file(dir.path().join(RUN_STATE_FILE)).unwrap();
        std::fs::create_dir(dir.path().join("core-0")).unwrap();
        assert!(matches!(
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::MissingTopology { .. })
        ));
        assert!(!dir.path().join(TOPOLOGY_FILE).exists());
    }

    #[tokio::test]
    async fn corrupt_topology_is_not_overwritten_and_lock_precedes_validation() {
        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(4, 64).unwrap();
        let wal =
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always, &topology).unwrap();
        let changed = StaticShardMap::new(8, 64).unwrap();
        assert!(matches!(
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always, &changed),
            Err(RaftWalError::Locked { .. })
        ));
        wal.shutdown().await.unwrap();
        drop(wal);
        std::fs::write(dir.path().join(TOPOLOGY_FILE), b"URSWTOPO").unwrap();
        assert!(matches!(
            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always, &topology),
            Err(RaftWalError::ReadTopology(_))
        ));
        assert_eq!(
            std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap(),
            b"URSWTOPO"
        );
    }
}
