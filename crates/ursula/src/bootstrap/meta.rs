//! The meta group on a server node: the spawn path binds the node's peer
//! identity and opens the meta journal ([`open_meta`]), before the runtime
//! starts and so before any data group of the run opens. `init_state`
//! then starts the meta runtime on that journal ([`SpawnedMeta::start`]),
//! with the meta shed of a cluster node ([`super::meta_shed`]), and keeps
//! both in a [`MetaSlot`] until shutdown.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_control::default_meta_voters;
use ursula_control::effective_replication_factor;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeSet;
use ursula_raft::PeerDirectory;
use ursula_raft::PeerIdentity;
use ursula_raft::RaftEngineConfig;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftWal;
use ursula_raft::meta::formation::FormationParams;
use ursula_raft::meta::replica::MetaSnapshotTrigger;
use ursula_raft::meta::runtime::GrpcMetaPlans;
use ursula_raft::meta::runtime::GrpcMetaRuntime;
use ursula_raft::meta::runtime::MetaJournal;
use ursula_raft::meta::runtime::MetaRuntimeConfig;
use ursula_raft::meta::status::MetaStatusReader;
use ursula_shard::ShardMapError;

use crate::bootstrap::participation::PeerParticipation;
use crate::bootstrap::topology::Topology;

/// What the spawn path prepared for the meta runtime: the opened meta
/// journal, the node's peer identity, WAL and registry, the meta runtime's
/// config, and a cluster node's view of its peers' participation, which
/// the meta shed hands leaderships by.
#[derive(Debug)]
pub struct SpawnedMeta {
    journal: MetaJournal,
    identity: PeerIdentity,
    wal: RaftWal,
    registry: RaftGroupHandleRegistry,
    config: MetaRuntimeConfig,
    participation: Option<PeerParticipation>,
    /// The meta journal's WAL counters.
    wal_metrics: ursula_runtime::RuntimeMetrics,
}

impl SpawnedMeta {
    /// Starts the meta runtime over gRPC on the prepared journal, and on a
    /// cluster node its meta shed.
    pub(crate) async fn start(self) -> MetaSlot {
        let node_id = self.config.node;
        let registry = self.registry.clone();
        let runtime = GrpcMetaRuntime::start_over_grpc(
            self.config,
            self.journal,
            self.wal,
            self.registry,
            self.identity,
        )
        .await;
        let shed = self.participation.map(|participation| {
            super::meta_shed::spawn_meta_shed(
                registry,
                participation,
                runtime.status_reader().clone(),
                node_id,
            )
        });
        MetaSlot(Arc::new(Mutex::new(Some(ServerMeta {
            runtime,
            shed,
            wal_metrics: self.wal_metrics,
        }))))
    }
}

/// The meta runtime and the meta shed task of a server node.
#[derive(Debug)]
struct ServerMeta {
    runtime: GrpcMetaRuntime,
    shed: Option<tokio::task::JoinHandle<()>>,
    wal_metrics: ursula_runtime::RuntimeMetrics,
}

/// Where the server keeps its meta runtime until shutdown takes it.
#[derive(Debug, Clone, Default)]
pub struct MetaSlot(Arc<Mutex<Option<ServerMeta>>>);

impl MetaSlot {
    /// The meta runtime, once: shutdown stops it. The meta shed stops
    /// here.
    pub(crate) fn take(&self) -> Option<GrpcMetaRuntime> {
        let meta = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        if let Some(shed) = meta.shed {
            shed.abort();
        }
        Some(meta.runtime)
    }

    /// The meta journal's WAL counters, while the meta runtime runs.
    pub(crate) fn wal_metrics(&self) -> Option<ursula_runtime::RuntimeMetrics> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|meta| meta.wal_metrics.clone())
    }

    /// What this node's placement status reads, while the meta runtime
    /// runs.
    pub(crate) fn status_reader(&self) -> Option<MetaStatusReader> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|meta| meta.runtime.status_reader().clone())
    }

    /// Where the meta replica's recovery plans go, while the meta runtime
    /// runs.
    pub(crate) fn plans(&self) -> Option<GrpcMetaPlans> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|meta| meta.runtime.plans())
    }

    /// Reads the meta runtime, if it still runs.
    #[cfg(test)]
    pub(crate) fn with<T>(&self, read: impl FnOnce(&GrpcMetaRuntime) -> T) -> Option<T> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|meta| read(&meta.runtime))
    }
}

/// Binds this node's peer identity, its Raft node id and the cluster id
/// of its WAL root, with its peers, both read from `topology` alone
/// ([`node_and_peers`]), then opens the meta journal under
/// `raft-log/meta/`. A journal that does not open is handed on as such:
/// the meta runtime then runs `Damaged` and the node serves.
pub(crate) fn open_meta(
    config: &ursula_config::UrsulaConfig,
    topology: &Topology,
    wal: &RaftWal,
    registry: &RaftGroupHandleRegistry,
    shard_map: &ursula_shard::StaticShardMap,
    engine: RaftEngineConfig,
    snapshots: MetaSnapshotTrigger,
    participation: Option<PeerParticipation>,
) -> Result<SpawnedMeta, ShardMapError> {
    let raft = &config.raft;
    let (node_id, directory) = node_and_peers(config, topology);
    let identity = PeerIdentity::of_process(
        node_id,
        wal.cluster_id().clone(),
        wal.fence(),
        wal.incarnation(),
    );
    let peers = NodeSet::from_first(NodeId::new(node_id), directory.node_ids().map(NodeId::new));
    registry.peer_cluster().bind(identity.clone(), directory);
    // Before any group opens, so none campaigns while a wait is owed.
    ursula_raft::predecessor_wait::sync_fence_shed(registry, &wal.fence());

    let group_count = NonZeroU32::new(shard_map.raft_group_count())
        .map(GroupCount::new)
        .ok_or(ShardMapError::ZeroRaftGroups)?;
    let claim = GenesisClaim {
        meta_voters: raft
            .meta_voters
            .clone()
            .unwrap_or_else(|| default_meta_voters(&peers)),
        replication_factor: effective_replication_factor(raft.placement.replication_factor, &peers),
        peers,
        group_count,
    };
    // The meta journal's own counters, apart from the data groups'.
    let wal_metrics = ursula_runtime::RuntimeMetrics::new(1, 1);
    let metrics = wal_metrics.group_engine_metrics();
    Ok(SpawnedMeta {
        journal: MetaJournal {
            opened: wal.open_meta(metrics.clone()),
            metrics,
        },
        wal_metrics,
        identity,
        wal: wal.clone(),
        registry: registry.clone(),
        config: MetaRuntimeConfig {
            node: node_id,
            claim,
            formation: FormationParams {
                max_concurrent_moves: raft.placement.max_concurrent_moves,
            },
            engine,
            snapshots: Some(snapshots),
        },
        participation,
    })
}

/// The node this process runs as and its peers, from the topology the
/// runtime spawns with, so the bound identity and the data groups' static
/// factory read one source: a static cluster's node id and peers, or, on a
/// single node, `raft.node_id` with its address book
/// ([`ursula_config::RaftConfig::address_book`]).
fn node_and_peers(
    config: &ursula_config::UrsulaConfig,
    topology: &Topology,
) -> (u64, PeerDirectory) {
    match topology {
        Topology::StaticCluster { node_id, peers, .. } => (*node_id, peers.clone()),
        Topology::SingleNode { .. } => (
            config.raft.node_id,
            PeerDirectory::from_config(&config.raft.address_book(&self_url(&config.server))),
        ),
    }
}

/// The cluster URL of a node whose `raft.peers` is empty, for its own
/// address book entry: its cluster listener, else its client listener.
fn self_url(server: &ursula_config::ServerConfig) -> String {
    let listen = server.cluster_listen.as_deref().unwrap_or(&server.listen);
    format!("http://{listen}")
}
