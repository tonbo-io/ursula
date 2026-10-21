use std::path::PathBuf;

use ursula_proto::placement::ReplicationFactor;
use ursula_raft::ConfigSeeds;
use ursula_raft::PeerDirectory;

/// Persistence strategy for the runtime.
#[derive(Debug, Clone)]
pub enum Persistence {
    /// No Raft: the in-memory engine. Nothing survives the process.
    InMemory,
    /// Raft over the per-core journals under `log_dir`.
    Raft { log_dir: PathBuf },
}

impl Persistence {
    /// The Raft WAL's journal directory, if the runtime runs Raft.
    pub fn log_dir(&self) -> Option<&std::path::Path> {
        match self {
            Self::InMemory => None,
            Self::Raft { log_dir } => Some(log_dir),
        }
    }
}

/// Deployment topology.
#[derive(Debug, Clone)]
pub enum Topology {
    SingleNode {
        raft_group_count: usize,
    },
    StaticCluster {
        node_id: u64,
        /// `raft.peers`, this node among them.
        peers: PeerDirectory,
        raft_group_count: usize,
        /// Every data group's voters and initializer, by the seed rule over
        /// `peers`. Interim: removed in S2(h).
        seeds: ConfigSeeds,
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

    /// Whether the node's groups snapshot to the configured external
    /// snapshot store (`snapshot_store_configured`). A single node keeps its
    /// snapshots inline whatever `storage.snapshot.backend` says (#669): a
    /// single node rolled back to 0.8.0 reads only its inline pointer files.
    pub fn uses_snapshot_store(&self, snapshot_store_configured: bool) -> bool {
        match self {
            Topology::SingleNode { .. } => false,
            Topology::StaticCluster { .. } => snapshot_store_configured,
        }
    }

    /// A static cluster of `peers` whose data groups are seeded over the
    /// `peers` ids at `replication_factor`, at most the node count
    /// ([`ConfigSeeds`]).
    pub fn static_cluster(
        node_id: u64,
        peers: PeerDirectory,
        raft_group_count: usize,
        replication_factor: ReplicationFactor,
    ) -> Self {
        let seeds = ConfigSeeds::new(&peers, raft_group_count, replication_factor);
        Self::StaticCluster {
            node_id,
            peers,
            raft_group_count,
            seeds,
        }
    }

    /// The seeds of a static cluster's data groups: what the node hosts,
    /// where it routes a group it does not host, and what its maintenance
    /// inventory and egress gate expect. Interim: removed in S2(h), where
    /// meta's seeds take over. Meta's `Bootstrap` seeds the same rule over
    /// the `raft.peers` ids at genesis, so the two agree only while
    /// `raft.peers` keeps those ids: `raft.peers` may not grow until then.
    pub fn config_seeds(&self) -> Option<&ConfigSeeds> {
        match self {
            Topology::SingleNode { .. } => None,
            Topology::StaticCluster { seeds, .. } => Some(seeds),
        }
    }
}
