//! Immutable configuration of a node's WAL root: its routing counts and the
//! cluster id (#581 section 6.5).
//!
//! The cluster id is bound to the WAL root when the root is created: the
//! configured `server.cluster_id`, or an id generated once for a single
//! node without one. A node whose configured id differs from the stored one
//! refuses to start, so a disk cannot move to another cluster unnoticed.

use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use ursula_headers::ClusterId;
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

/// The topology file: the routing counts and the cluster id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredTopology {
    routing: WalTopology,
    cluster_id: String,
}

/// A cluster id for a node that configures none: `u` followed by 32 hex
/// digits of 128 random bits.
pub fn generate_cluster_id() -> ClusterId {
    ClusterId::new(format!("u{:032x}", rand::random::<u128>()))
        .expect("a generated cluster id is 33 bytes of [0-9a-u]")
}

/// Pure policy for binding a WAL root to its configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TopologyDecision {
    Create,
    Match {
        cluster_id: String,
    },
    Mismatch {
        stored: WalTopology,
    },
    ClusterIdMismatch {
        stored: String,
        configured: ClusterId,
    },
    Missing,
}

impl TopologyDecision {
    fn decide(
        stored: Option<&StoredTopology>,
        configured: WalTopology,
        configured_cluster_id: Option<&ClusterId>,
        has_prior_state: bool,
    ) -> Self {
        match stored {
            Some(stored) if stored.routing != configured => Self::Mismatch {
                stored: stored.routing,
            },
            Some(stored) => match configured_cluster_id {
                Some(configured) if configured.as_str() != stored.cluster_id => {
                    Self::ClusterIdMismatch {
                        stored: stored.cluster_id.clone(),
                        configured: configured.clone(),
                    }
                }
                _ => Self::Match {
                    cluster_id: stored.cluster_id.clone(),
                },
            },
            None if has_prior_state => Self::Missing,
            None => Self::Create,
        }
    }
}

/// Called under the root lock, before recovery or run-state writes. Returns
/// the root's cluster id.
pub(super) fn check_or_create(
    root: &Path,
    map: &StaticShardMap,
    configured_cluster_id: Option<&ClusterId>,
    has_prior_state: bool,
) -> Result<ClusterId, RaftWalError> {
    let configured = WalTopology::from(map);
    let path = root.join(TOPOLOGY_FILE);
    let stored = state_file::read::<StoredTopology>(StateFileKind::Topology, &path)
        .map_err(RaftWalError::ReadTopology)?;
    match TopologyDecision::decide(
        stored.as_ref(),
        configured,
        configured_cluster_id,
        has_prior_state,
    ) {
        TopologyDecision::Create => {
            let cluster_id = configured_cluster_id
                .cloned()
                .unwrap_or_else(generate_cluster_id);
            state_file::write(
                StateFileKind::Topology,
                &path,
                &path.with_extension("tmp"),
                &StoredTopology {
                    routing: configured,
                    cluster_id: cluster_id.as_str().to_owned(),
                },
            )
            .map_err(RaftWalError::RecordTopology)?;
            Ok(cluster_id)
        }
        TopologyDecision::Match { cluster_id } => {
            ClusterId::new(cluster_id.clone()).map_err(|_invalid| {
                RaftWalError::InvalidStoredClusterId {
                    root: root.to_owned(),
                    stored: cluster_id,
                }
            })
        }
        TopologyDecision::Mismatch { stored } => Err(RaftWalError::TopologyMismatch {
            root: root.to_owned(),
            stored_core_count: stored.core_count,
            stored_group_count: stored.group_count,
            configured_core_count: configured.core_count,
            configured_group_count: configured.group_count,
        }),
        TopologyDecision::ClusterIdMismatch { stored, configured } => {
            Err(RaftWalError::ClusterIdMismatch {
                root: root.to_owned(),
                stored,
                configured,
            })
        }
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
        let id = ClusterId::new("prod-a").unwrap();
        let other = ClusterId::new("prod-b").unwrap();
        let stored_as = |routing: WalTopology| StoredTopology {
            routing,
            cluster_id: "prod-a".to_owned(),
        };
        for has_prior_state in [false, true] {
            for configured_id in [None, Some(&id), Some(&other)] {
                assert_eq!(
                    TopologyDecision::decide(None, configured, configured_id, has_prior_state),
                    if has_prior_state {
                        TopologyDecision::Missing
                    } else {
                        TopologyDecision::Create
                    },
                );
            }
            for configured_id in [None, Some(&id)] {
                assert_eq!(
                    TopologyDecision::decide(
                        Some(&stored_as(configured)),
                        configured,
                        configured_id,
                        has_prior_state
                    ),
                    TopologyDecision::Match {
                        cluster_id: "prod-a".to_owned()
                    },
                );
            }
            assert_eq!(
                TopologyDecision::decide(
                    Some(&stored_as(configured)),
                    configured,
                    Some(&other),
                    has_prior_state
                ),
                TopologyDecision::ClusterIdMismatch {
                    stored: "prod-a".to_owned(),
                    configured: other.clone(),
                },
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
                    TopologyDecision::decide(
                        Some(&stored_as(stored)),
                        configured,
                        Some(&id),
                        has_prior_state
                    ),
                    TopologyDecision::Mismatch { stored },
                );
            }
        }
    }

    #[test]
    fn generated_cluster_ids_are_u_and_32_hex_digits() {
        let generated = generate_cluster_id();
        assert_eq!(generated.as_str().len(), 33);
        assert!(generated.as_str().starts_with('u'));
        let digits = generated.as_str().strip_prefix('u').unwrap();
        assert!(
            digits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_ne!(generate_cluster_id(), generated);
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

    #[tokio::test]
    async fn the_wal_root_binds_its_cluster_id() {
        let topology = StaticShardMap::new(2, 4).unwrap();
        let tuning = crate::JournalTuning::new(WalFsync::Never);

        // A single node without a configured id keeps the one it generated.
        let dir = tempfile::tempdir().unwrap();
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        let generated = wal.cluster_id().clone();
        wal.shutdown().await.unwrap();
        drop(wal);
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        assert_eq!(wal.cluster_id(), &generated);
        wal.shutdown().await.unwrap();
        drop(wal);
        // Configuring the generated id later is accepted.
        let wal =
            RaftWal::start_in_cluster(dir.path(), tuning, &topology, Some(&generated)).unwrap();
        wal.shutdown().await.unwrap();
        drop(wal);

        // A configured id is recorded, and another one is refused without
        // touching the root.
        let dir = tempfile::tempdir().unwrap();
        let prod = ClusterId::new("prod-eu-1").unwrap();
        let wal = RaftWal::start_in_cluster(dir.path(), tuning, &topology, Some(&prod)).unwrap();
        assert_eq!(wal.cluster_id(), &prod);
        wal.shutdown().await.unwrap();
        drop(wal);
        let run_before = std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap();
        let other = ClusterId::new("prod-us-1").unwrap();
        let error =
            RaftWal::start_in_cluster(dir.path(), tuning, &topology, Some(&other)).unwrap_err();
        assert!(matches!(
            &error,
            RaftWalError::ClusterIdMismatch { stored, configured, .. }
                if stored == "prod-eu-1" && configured == &other
        ));
        assert_eq!(
            std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap(),
            run_before
        );
        // Without a configured id, the node keeps the stored one.
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        assert_eq!(wal.cluster_id(), &prod);
        wal.shutdown().await.unwrap();
    }
}
