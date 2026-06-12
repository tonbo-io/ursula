use std::collections::BTreeSet;
use std::path::PathBuf;

use ursula_raft::StaticGrpcRaftMembershipConfig;
use ursula_runtime::RuntimeError;
use ursula_shard::RaftGroupId;

/// Persistence strategy for the runtime.
#[derive(Debug, Clone)]
pub enum Persistence {
    InMemory,
    Wal { wal_dir: PathBuf },
    Raft { log_dir: Option<PathBuf> },
}

/// Deployment topology.
#[derive(Debug, Clone)]
pub enum Topology {
    SingleNode {
        raft_group_count: usize,
    },
    StaticCluster {
        node_id: u64,
        peers: Vec<(u64, String)>,
        raft_group_count: usize,
        initialize_membership: bool,
        membership_config: StaticGrpcRaftMembershipConfig,
    },
}

impl Topology {
    pub fn raft_group_count(&self) -> usize {
        match self {
            Topology::SingleNode { raft_group_count } => *raft_group_count,
            Topology::StaticCluster {
                raft_group_count, ..
            } => *raft_group_count,
        }
    }

    /// Construct a [`Topology::StaticCluster`] with validated membership.
    pub fn static_cluster(
        node_id: u64,
        peers: Vec<(u64, String)>,
        raft_group_count: usize,
        initialize_membership: bool,
        membership_config: StaticGrpcRaftMembershipConfig,
    ) -> Result<Self, RuntimeError> {
        Self::validate_static_cluster(raft_group_count, &peers, &membership_config)?;
        Ok(Self::StaticCluster {
            node_id,
            peers,
            raft_group_count,
            initialize_membership,
            membership_config,
        })
    }

    fn validate_static_cluster(
        raft_group_count: usize,
        peers: &[(u64, String)],
        membership_config: &StaticGrpcRaftMembershipConfig,
    ) -> Result<(), RuntimeError> {
        let per_group_voters = &membership_config.per_group_voters;
        if per_group_voters.is_empty() {
            return Ok(());
        }

        let peer_ids: BTreeSet<u64> = peers.iter().map(|(node_id, _)| *node_id).collect();
        let raft_group_count_u32 =
            u32::try_from(raft_group_count).map_err(|_| RuntimeError::StaticMembershipConfig {
                message: format!("raft_group_count {raft_group_count} exceeds u32::MAX"),
            })?;

        for (raft_group_id, voters) in per_group_voters {
            if raft_group_id.0 >= raft_group_count_u32 {
                return Err(RuntimeError::InvalidRaftGroup {
                    raft_group_id: *raft_group_id,
                    raft_group_count: raft_group_count_u32,
                });
            }
            if voters.is_empty() {
                return Err(RuntimeError::StaticMembershipConfig {
                    message: format!("raft group {} has no voters", raft_group_id.0),
                });
            }
            for voter in voters {
                if !peer_ids.contains(voter) {
                    return Err(RuntimeError::StaticMembershipConfig {
                        message: format!(
                            "raft group {} voter {} is not present in static peer config",
                            raft_group_id.0, voter
                        ),
                    });
                }
            }
        }

        for raw_group_id in 0..raft_group_count_u32 {
            let raft_group_id = RaftGroupId(raw_group_id);
            if !per_group_voters.contains_key(&raft_group_id) {
                return Err(RuntimeError::StaticMembershipConfig {
                    message: format!(
                        "partial raft_group_voters config is not supported; missing raft group {} of {}",
                        raw_group_id, raft_group_count
                    ),
                });
            }
        }

        Ok(())
    }
}
