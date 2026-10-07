//! Immutable routing configuration of a node's WAL root.

use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::StaticShardMap;

use super::RaftWalError;
use super::state_file;
use super::state_file::StateFileKind;

const TOPOLOGY_FILE: &str = "topology.bin";

/// The routing counts persisted before any group can write its journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct WalTopology {
    core_count: u16,
    group_count: u32,
}

impl From<&StaticShardMap> for WalTopology {
    fn from(map: &StaticShardMap) -> Self {
        Self {
            core_count: map.core_count(),
            group_count: map.raft_group_count(),
        }
    }
}

/// Pure policy for binding a WAL root to its routing configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopologyDecision {
    Create,
    Match,
    Mismatch { stored: WalTopology },
    Missing,
}

impl TopologyDecision {
    fn decide(stored: Option<WalTopology>, configured: WalTopology, has_prior_state: bool) -> Self {
        match stored {
            Some(stored) if stored == configured => Self::Match,
            Some(stored) => Self::Mismatch { stored },
            None if has_prior_state => Self::Missing,
            None => Self::Create,
        }
    }
}

/// Called under the root lock, before recovery or run-state writes.
pub(super) fn check_or_create(
    root: &Path,
    map: &StaticShardMap,
    has_prior_state: bool,
) -> Result<(), RaftWalError> {
    let configured = WalTopology::from(map);
    let path = root.join(TOPOLOGY_FILE);
    let stored = state_file::read::<WalTopology>(StateFileKind::Topology, &path)
        .map_err(RaftWalError::ReadTopology)?;
    match TopologyDecision::decide(stored, configured, has_prior_state) {
        TopologyDecision::Create => {
            state_file::write(
                StateFileKind::Topology,
                &path,
                &path.with_extension("tmp"),
                &configured,
            )
            .map_err(RaftWalError::RecordTopology)?;
            Ok(())
        }
        TopologyDecision::Match => Ok(()),
        TopologyDecision::Mismatch { stored } => Err(RaftWalError::TopologyMismatch {
            root: root.to_owned(),
            stored_core_count: stored.core_count,
            stored_group_count: stored.group_count,
            configured_core_count: configured.core_count,
            configured_group_count: configured.group_count,
        }),
        TopologyDecision::Missing => Err(RaftWalError::MissingTopology {
            root: root.to_owned(),
        }),
    }
}

#[cfg(test)]
mod decision_tests {
    use super::*;

    #[test]
    fn topology_decisions_cover_new_existing_and_missing_records() {
        let configured = WalTopology {
            core_count: 4,
            group_count: 64,
        };
        for has_prior_state in [false, true] {
            assert_eq!(
                TopologyDecision::decide(None, configured, has_prior_state),
                if has_prior_state {
                    TopologyDecision::Missing
                } else {
                    TopologyDecision::Create
                },
            );
            assert_eq!(
                TopologyDecision::decide(Some(configured), configured, has_prior_state),
                TopologyDecision::Match,
            );
            for stored in [
                WalTopology {
                    core_count: 8,
                    ..configured
                },
                WalTopology {
                    group_count: 128,
                    ..configured
                },
            ] {
                assert_eq!(
                    TopologyDecision::decide(Some(stored), configured, has_prior_state),
                    TopologyDecision::Mismatch { stored },
                );
            }
        }
    }
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
            assert!(matches!(error, RaftWalError::TopologyMismatch {
                    stored_core_count: 4, stored_group_count: 8,
                    configured_core_count, configured_group_count, ..
                } if configured_core_count == changed.core_count()
                    && configured_group_count == changed.raft_group_count()));
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
