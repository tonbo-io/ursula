//! Immutable routing configuration of a node's WAL root.

use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::StaticShardMap;

use super::RaftWalError;
use super::state_file;
use super::state_file::StateFileError;
use super::state_file::StateFileKind;

const TOPOLOGY_FILE: &str = "topology.bin";

/// Whether a WAL root is exclusively data-only or explicitly admits control state.
/// Selecting a path alone never enables a managed namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalMode {
    DataOnly,
    Managed,
}

impl WalMode {
    fn kind(self) -> StateFileKind {
        match self {
            Self::DataOnly => StateFileKind::Topology,
            Self::Managed => StateFileKind::ManagedTopology,
        }
    }
}

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
pub(super) fn check_or_create_with_mode(
    root: &Path,
    map: &StaticShardMap,
    has_prior_state: bool,
    mode: WalMode,
) -> Result<(), RaftWalError> {
    let configured = WalTopology::from(map);
    let path = root.join(TOPOLOGY_FILE);
    let (stored, stored_mode) =
        match state_file::read::<WalTopology>(StateFileKind::ManagedTopology, &path) {
            Ok(stored) => (stored, WalMode::Managed),
            Err(StateFileError::UnsupportedVersion { version: 3, .. }) => (
                state_file::read::<WalTopology>(StateFileKind::Topology, &path)
                    .map_err(RaftWalError::ReadTopology)?,
                WalMode::DataOnly,
            ),
            Err(source) => return Err(RaftWalError::ReadTopology(source)),
        };
    match TopologyDecision::decide(stored, configured, has_prior_state) {
        TopologyDecision::Create => {
            state_file::write(mode.kind(), &path, &path.with_extension("tmp"), &configured)
                .map_err(RaftWalError::RecordTopology)?;
            Ok(())
        }
        TopologyDecision::Match => match (stored_mode, mode) {
            (WalMode::Managed, WalMode::DataOnly) => Err(RaftWalError::ManagedModeRequired {
                root: root.to_owned(),
            }),
            (WalMode::DataOnly, WalMode::Managed) => {
                // Under the node root lock, before recovery/run-state writes or
                // returning a handle from which any core can be opened.
                state_file::write(mode.kind(), &path, &path.with_extension("tmp"), &configured)
                    .map_err(RaftWalError::RecordTopology)?;
                Ok(())
            }
            _ => Ok(()),
        },
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
    use crate::GroupRejoin;
    use crate::RaftWal;
    use crate::log_store::RUN_STATE_FILE;
    use crate::types::UrsulaRaftTypeConfig;

    #[tokio::test]
    async fn managed_root_guard_precedes_all_core_access_and_preserves_data() {
        for fsync in [WalFsync::Always, WalFsync::Never] {
            let dir = tempfile::tempdir().unwrap();
            let map = StaticShardMap::new(2, 2).unwrap();
            let placement = map.placement(RaftGroupId(1)).unwrap();
            let metrics = RuntimeMetrics::new(2, 2).group_engine_metrics();
            let wal = RaftWal::start(dir.path(), fsync, &map).unwrap();
            let mut store = wal.open(placement, metrics.clone()).unwrap();
            let vote = crate::types::UrsulaVote::new(7, 1);
            store.save_vote(&vote).await.unwrap();
            wal.shutdown().await.unwrap();
            drop((store, wal));
            let old = std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap();
            assert_eq!(
                state_file::decode::<WalTopology>(StateFileKind::Topology, dir.path(), &old)
                    .unwrap(),
                WalTopology::from(&map)
            );
            let core_meta = dir.path().join("core-1/journal.meta");
            let core_before = std::fs::read(&core_meta).unwrap();
            let managed =
                RaftWal::start_managed(dir.path(), crate::JournalTuning::new(fsync), &map).unwrap();
            // The root barrier is durable before any core is requested.
            let guard = std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap();
            assert!(matches!(
                state_file::decode::<WalTopology>(StateFileKind::Topology, dir.path(), &guard),
                Err(StateFileError::UnsupportedVersion { version: 4, .. })
            ));
            assert_eq!(std::fs::read(&core_meta).unwrap(), core_before);
            let mut store = managed.open(placement, metrics).unwrap();
            assert_eq!(store.read_vote().await.unwrap(), Some(vote));
            managed.shutdown().await.unwrap();
            drop((store, managed));
            let run_before = std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap();
            assert!(matches!(
                RaftWal::start(dir.path(), fsync, &map),
                Err(RaftWalError::ManagedModeRequired { .. })
            ));
            assert_eq!(
                std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap(),
                run_before
            );
            assert_eq!(
                std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap(),
                guard
            );
            // Removing the root guard is not a data-only escape hatch.
            std::fs::remove_file(dir.path().join(TOPOLOGY_FILE)).unwrap();
            assert!(matches!(
                RaftWal::start(dir.path(), fsync, &map),
                Err(RaftWalError::MissingTopology { .. })
            ));
        }
    }

    #[tokio::test]
    async fn topology_changes_are_refused_before_old_votes_can_be_reused() {
        let dir = tempfile::tempdir().unwrap();
        let original = StaticShardMap::new(4, 8).unwrap();
        let placement = original.placement(RaftGroupId(4)).unwrap();
        let metrics = RuntimeMetrics::new(4, 8).group_engine_metrics();
        let wal = RaftWal::start(dir.path(), WalFsync::Always, &original).unwrap();
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
            let error = RaftWal::start(dir.path(), WalFsync::Always, &changed).unwrap_err();
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
        let wal = RaftWal::start(dir.path(), WalFsync::Always, &original).unwrap();
        let mut store = wal.open(placement, metrics).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.try_get_log_entries(1..=1).await.unwrap().len(), 1);
        assert!(
            GroupRejoin::durable(1, placement.raft_group_id, &store)
                .await
                .unwrap()
                .vote_gate_open()
        );
        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn topology_is_required_even_without_journal_records() {
        let topology = StaticShardMap::new(4, 64).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        wal.shutdown().await.unwrap();
        drop(wal);
        std::fs::remove_file(dir.path().join(TOPOLOGY_FILE)).unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::MissingTopology { .. })
        ));
        // Losing the run state must not bless an old core's vote-only metadata.
        std::fs::remove_file(dir.path().join(RUN_STATE_FILE)).unwrap();
        std::fs::create_dir(dir.path().join("core-0")).unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::MissingTopology { .. })
        ));
        assert!(!dir.path().join(TOPOLOGY_FILE).exists());
    }

    #[tokio::test]
    async fn corrupt_topology_is_not_overwritten_and_lock_precedes_validation() {
        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(4, 64).unwrap();
        let wal = RaftWal::start(dir.path(), WalFsync::Always, &topology).unwrap();
        let changed = StaticShardMap::new(8, 64).unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Always, &changed),
            Err(RaftWalError::Locked { .. })
        ));
        wal.shutdown().await.unwrap();
        drop(wal);
        std::fs::write(dir.path().join(TOPOLOGY_FILE), b"URSWTOPO").unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Always, &topology),
            Err(RaftWalError::ReadTopology(_))
        ));
        assert_eq!(
            std::fs::read(dir.path().join(TOPOLOGY_FILE)).unwrap(),
            b"URSWTOPO"
        );
    }
}

#[cfg(all(test, madsim))]
mod simulated_tests {
    use ursula_config::WalFsync;

    use super::*;
    use crate::JournalTuning;
    use crate::RaftWal;
    use crate::log_store::RUN_STATE_FILE;
    use crate::log_store::SimDisk;
    use crate::log_store::SimDiskFault;

    #[test]
    fn managed_root_guard_survives_power_loss_under_both_policies() {
        madsim::runtime::Runtime::new().block_on(async {
            for fsync in [WalFsync::Always, WalFsync::Never] {
                let root = SimDisk::provision_dir(&format!("managed-{fsync:?}")).unwrap();
                let map = StaticShardMap::new(2, 2).unwrap();
                let data = RaftWal::start(&root, fsync, &map).unwrap();
                data.shutdown().await.unwrap();
                drop(data);
                let managed =
                    RaftWal::start_managed(&root, JournalTuning::new(fsync), &map).unwrap();
                drop(managed);
                SimDisk::power_loss_losing_unsynced(&root).unwrap();
                let before = SimDisk::read(&root.join(RUN_STATE_FILE)).unwrap();
                assert!(matches!(
                    RaftWal::start(&root, fsync, &map),
                    Err(RaftWalError::ManagedModeRequired { .. })
                ));
                assert_eq!(SimDisk::read(&root.join(RUN_STATE_FILE)).unwrap(), before);
                let managed =
                    RaftWal::start_managed(&root, JournalTuning::new(fsync), &map).unwrap();
                managed.shutdown().await.unwrap();
            }
        });
    }

    #[test]
    fn failed_managed_guard_never_returns_a_wal_handle() {
        madsim::runtime::Runtime::new().block_on(async {
            for (name, relative, fault) in [
                ("write", "topology.tmp", SimDiskFault::Write),
                ("file-sync", "topology.tmp", SimDiskFault::Sync),
                ("directory-sync", "", SimDiskFault::Sync),
            ] {
                let root = SimDisk::provision_dir(name).unwrap();
                let map = StaticShardMap::new(2, 2).unwrap();
                let data = RaftWal::start(&root, WalFsync::Never, &map).unwrap();
                data.shutdown().await.unwrap();
                drop(data);
                let before = SimDisk::read(&root.join(RUN_STATE_FILE)).unwrap();
                SimDisk::inject_fault(&root.join(relative), fault).unwrap();
                assert!(matches!(
                    RaftWal::start_managed(&root, JournalTuning::new(WalFsync::Never), &map),
                    Err(RaftWalError::RecordTopology(_))
                ));
                assert_eq!(SimDisk::read(&root.join(RUN_STATE_FILE)).unwrap(), before);
                SimDisk::power_loss_losing_unsynced(&root).unwrap();
                // There was no returned handle and therefore no authorized
                // managed write. Retrying the explicit upgrade is safe.
                let managed =
                    RaftWal::start_managed(&root, JournalTuning::new(WalFsync::Never), &map)
                        .unwrap();
                managed.shutdown().await.unwrap();
            }
        });
    }
}
