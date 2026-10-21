//! Immutable configuration of a node's WAL root: its routing counts, the
//! cluster id (#581 section 6.5), the node id and the root's own id.
//!
//! The cluster id is bound to the WAL root when the root is created: the
//! configured `server.cluster_id`, or an id generated once for a single
//! node without one. A node whose configured id differs from the stored one
//! refuses to start, so a disk cannot move to another cluster unnoticed.
//! The node id is bound the same way: a root opens only under the node id
//! that created it ([`RaftWalError::OtherNode`]). The root id
//! ([`RootId`]) is drawn when the root is created and names this disk to
//! the meta group: a new disk is a new root.

use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use ursula_headers::ClusterId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::RootId;
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

/// The topology file's encoding: the routing counts, the cluster id, and
/// the node id and root id the root was created with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TopologyFile {
    routing: WalTopology,
    cluster_id: String,
    node: NodeId,
    root: RootId,
}

/// The topology a root holds, its cluster id parsed when the file is read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredTopology {
    routing: WalTopology,
    cluster_id: ClusterId,
    node: NodeId,
    root: RootId,
}

/// What a WAL root is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BoundRoot {
    pub(super) cluster_id: ClusterId,
    pub(super) root: RootId,
}

/// The root id of the topology sample: above 2^53, so the fixture shows its
/// hex string.
#[cfg(test)]
const SAMPLE_ROOT: u64 = 0x5eed_1d00_0000_00a7;

/// A topology file of 4 cores, 8 groups, the cluster id `prod-a`, node 3
/// and the root `5eed1d00000000a7`, for the persisted-envelopes fixture. It
/// decodes to the file it was encoded from and re-encodes to the same bytes.
#[cfg(test)]
pub(crate) fn sample_file() -> Vec<u8> {
    let file = TopologyFile {
        routing: WalTopology {
            core_count: 4,
            group_count: 8,
        },
        cluster_id: "prod-a".to_owned(),
        node: NodeId::new(3),
        root: RootId::new(std::num::NonZeroU64::new(SAMPLE_ROOT).expect("non-zero")),
    };
    let bytes = state_file::encode(StateFileKind::Topology, &file);
    let decoded: TopologyFile =
        state_file::decode(StateFileKind::Topology, Path::new(TOPOLOGY_FILE), &bytes)
            .expect("the sample decodes");
    assert_eq!(decoded, file);
    assert_eq!(state_file::encode(StateFileKind::Topology, &decoded), bytes);
    bytes
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
    Match(BoundRoot),
    Mismatch {
        stored: WalTopology,
    },
    ClusterIdMismatch {
        stored: ClusterId,
        configured: ClusterId,
    },
    OtherNode {
        stored: NodeId,
    },
    Missing,
}

impl TopologyDecision {
    fn decide(
        stored: Option<&StoredTopology>,
        configured: WalTopology,
        configured_cluster_id: Option<&ClusterId>,
        node: NodeId,
        has_prior_state: bool,
    ) -> Self {
        match stored {
            Some(stored) if stored.routing != configured => Self::Mismatch {
                stored: stored.routing,
            },
            Some(stored) => match configured_cluster_id {
                Some(configured) if *configured != stored.cluster_id => Self::ClusterIdMismatch {
                    stored: stored.cluster_id.clone(),
                    configured: configured.clone(),
                },
                _ if stored.node != node => Self::OtherNode {
                    stored: stored.node,
                },
                _ => Self::Match(BoundRoot {
                    cluster_id: stored.cluster_id.clone(),
                    root: stored.root,
                }),
            },
            None if has_prior_state => Self::Missing,
            None => Self::Create,
        }
    }
}

/// The cluster id `root` holds, if it holds one. Reads only.
fn stored_cluster_id(root: &Path) -> Result<Option<ClusterId>, RaftWalError> {
    let path = root.join(TOPOLOGY_FILE);
    state_file::read::<TopologyFile>(StateFileKind::Topology, &path)
        .map_err(RaftWalError::ReadTopology)?
        .map(|file| {
            ClusterId::new(file.cluster_id.clone()).map_err(|_invalid| {
                RaftWalError::InvalidStoredClusterId {
                    root: root.to_owned(),
                    stored: file.cluster_id,
                }
            })
        })
        .transpose()
}

/// Refuses `configured` when `root` already holds another cluster id, the
/// check [`check_or_create`] makes, without taking the root's lock or writing
/// anything. A root without a topology file passes.
pub(super) fn check_cluster_id(root: &Path, configured: &ClusterId) -> Result<(), RaftWalError> {
    match stored_cluster_id(root)? {
        Some(stored) if stored != *configured => Err(RaftWalError::ClusterIdMismatch {
            root: root.to_owned(),
            stored,
            configured: configured.clone(),
        }),
        _ => Ok(()),
    }
}

/// Called under the root lock, before recovery or run-state writes, for node
/// `node`. Returns the root's cluster id and root id, which a new root
/// draws.
pub(super) fn check_or_create(
    root: &Path,
    map: &StaticShardMap,
    configured_cluster_id: Option<&ClusterId>,
    node: NodeId,
    has_prior_state: bool,
) -> Result<BoundRoot, RaftWalError> {
    let configured = WalTopology::from(map);
    let path = root.join(TOPOLOGY_FILE);
    let stored = state_file::read::<TopologyFile>(StateFileKind::Topology, &path)
        .map_err(RaftWalError::ReadTopology)?
        .map(|file| {
            ClusterId::new(file.cluster_id.clone())
                .map(|cluster_id| StoredTopology {
                    routing: file.routing,
                    cluster_id,
                    node: file.node,
                    root: file.root,
                })
                .map_err(|_invalid| RaftWalError::InvalidStoredClusterId {
                    root: root.to_owned(),
                    stored: file.cluster_id,
                })
        })
        .transpose()?;
    match TopologyDecision::decide(
        stored.as_ref(),
        configured,
        configured_cluster_id,
        node,
        has_prior_state,
    ) {
        TopologyDecision::Create => {
            let bound = BoundRoot {
                cluster_id: configured_cluster_id
                    .cloned()
                    .unwrap_or_else(generate_cluster_id),
                root: RootId::new(rand::random()),
            };
            state_file::write(
                StateFileKind::Topology,
                &path,
                &path.with_extension("tmp"),
                &TopologyFile {
                    routing: configured,
                    cluster_id: bound.cluster_id.as_str().to_owned(),
                    node,
                    root: bound.root,
                },
            )
            .map_err(RaftWalError::RecordTopology)?;
            Ok(bound)
        }
        TopologyDecision::Match(bound) => Ok(bound),
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
        TopologyDecision::OtherNode { stored } => Err(RaftWalError::OtherNode {
            root: root.to_owned(),
            stored,
            configured: node,
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
        let id = ClusterId::new("prod-a").unwrap();
        let other = ClusterId::new("prod-b").unwrap();
        let node = NodeId::new(3);
        let root = RootId::new(std::num::NonZeroU64::new(SAMPLE_ROOT).unwrap());
        let stored_as = |routing: WalTopology| StoredTopology {
            routing,
            cluster_id: id.clone(),
            node,
            root,
        };
        let bound = BoundRoot {
            cluster_id: id.clone(),
            root,
        };
        for has_prior_state in [false, true] {
            for configured_id in [None, Some(&id), Some(&other)] {
                assert_eq!(
                    TopologyDecision::decide(
                        None,
                        configured,
                        configured_id,
                        node,
                        has_prior_state
                    ),
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
                        node,
                        has_prior_state
                    ),
                    TopologyDecision::Match(bound.clone()),
                );
                // A root opens only under the node id that created it.
                assert_eq!(
                    TopologyDecision::decide(
                        Some(&stored_as(configured)),
                        configured,
                        configured_id,
                        NodeId::new(4),
                        has_prior_state
                    ),
                    TopologyDecision::OtherNode { stored: node },
                );
            }
            assert_eq!(
                TopologyDecision::decide(
                    Some(&stored_as(configured)),
                    configured,
                    Some(&other),
                    node,
                    has_prior_state
                ),
                TopologyDecision::ClusterIdMismatch {
                    stored: id.clone(),
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
                        node,
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

    /// A WAL root records its topology as the persisted-envelopes fixture's
    /// sample holds it ([`sample_file`]), with the root id it drew.
    #[test]
    fn a_recorded_topology_is_the_fixture_sample() {
        let dir = tempfile::tempdir().unwrap();
        let map = StaticShardMap::new(4, 8).unwrap();
        let cluster_id = ClusterId::new("prod-a").unwrap();
        let node = NodeId::new(3);
        let bound = check_or_create(dir.path(), &map, Some(&cluster_id), node, false).unwrap();
        assert_eq!(bound.cluster_id, cluster_id);
        let path = dir.path().join(TOPOLOGY_FILE);
        let recorded: TopologyFile = state_file::read(StateFileKind::Topology, &path)
            .unwrap()
            .unwrap();
        assert_eq!(recorded.root, bound.root);
        let sample = TopologyFile {
            root: RootId::new(std::num::NonZeroU64::new(SAMPLE_ROOT).unwrap()),
            ..recorded
        };
        assert_eq!(
            state_file::encode(StateFileKind::Topology, &sample),
            sample_file()
        );
        assert_eq!(
            stored_cluster_id(dir.path()).unwrap(),
            Some(cluster_id.clone())
        );
        // The root keeps its id, and refuses another node id.
        assert_eq!(
            check_or_create(dir.path(), &map, Some(&cluster_id), node, true).unwrap(),
            bound
        );
        assert!(matches!(
            check_or_create(dir.path(), &map, Some(&cluster_id), NodeId::new(4), true),
            Err(RaftWalError::OtherNode { stored, configured, .. })
                if stored == node && configured == NodeId::new(4)
        ));
        // Another root draws another id.
        let other = tempfile::tempdir().unwrap();
        assert_ne!(
            check_or_create(other.path(), &map, Some(&cluster_id), node, false)
                .unwrap()
                .root,
            bound.root
        );
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
            GroupRejoin::durable(
                1,
                ursula_shard::RaftTarget::Data(placement.raft_group_id),
                &store
            )
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
        let wal = RaftWal::start_in_cluster(
            dir.path(),
            tuning,
            &topology,
            NodeId::new(1),
            Some(&generated),
            ursula_runtime::NodeFence::unleased(),
        )
        .unwrap();
        wal.shutdown().await.unwrap();
        drop(wal);

        // A configured id is recorded, and another one is refused without
        // touching the root.
        let dir = tempfile::tempdir().unwrap();
        let prod = ClusterId::new("prod-eu-1").unwrap();
        let wal = RaftWal::start_in_cluster(
            dir.path(),
            tuning,
            &topology,
            NodeId::new(1),
            Some(&prod),
            ursula_runtime::NodeFence::unleased(),
        )
        .unwrap();
        assert_eq!(wal.cluster_id(), &prod);
        wal.shutdown().await.unwrap();
        drop(wal);
        let run_before = std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap();
        let other = ClusterId::new("prod-us-1").unwrap();
        let error = RaftWal::start_in_cluster(
            dir.path(),
            tuning,
            &topology,
            NodeId::new(1),
            Some(&other),
            ursula_runtime::NodeFence::unleased(),
        )
        .unwrap_err();
        assert!(matches!(
            &error,
            RaftWalError::ClusterIdMismatch { stored, configured, .. }
                if *stored == prod && configured == &other
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

    #[tokio::test]
    async fn a_stored_cluster_id_is_parsed_when_it_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(2, 4).unwrap();
        let path = dir.path().join(TOPOLOGY_FILE);
        state_file::write(
            StateFileKind::Topology,
            &path,
            &path.with_extension("tmp"),
            &TopologyFile {
                routing: WalTopology::from(&topology),
                cluster_id: "not a/valid id".to_owned(),
                node: NodeId::new(1),
                root: RootId::new(std::num::NonZeroU64::MIN),
            },
        )
        .unwrap();
        // Configured or not, an invalid stored id is reported as itself,
        // never as a mismatch with the configured one.
        let configured = ClusterId::new("prod-eu-1").unwrap();
        for configured in [None, Some(&configured)] {
            let error = RaftWal::start_in_cluster(
                dir.path(),
                crate::JournalTuning::new(WalFsync::Never),
                &topology,
                NodeId::new(1),
                configured,
                ursula_runtime::NodeFence::unleased(),
            )
            .unwrap_err();
            assert!(
                matches!(&error, RaftWalError::InvalidStoredClusterId { stored, .. }
                    if stored == "not a/valid id"),
                "{error}"
            );
        }
    }
}
