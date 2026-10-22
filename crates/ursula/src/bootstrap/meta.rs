//! The meta group on a server node: the spawn path binds the node's peer
//! identity and opens the meta journal ([`open_meta`]), before the runtime
//! starts and so before any data group of the run opens. `init_state`
//! then starts the meta runtime on that journal ([`SpawnedMeta::start`]),
//! with the meta shed of a cluster node ([`super::meta_shed`]), then the
//! hosting driver over meta's views ([`MetaSlot::start_hosting`]), and
//! keeps all three in a [`MetaSlot`] until shutdown.

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
use ursula_raft::HostingTask;
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

use crate::bootstrap::hosting::SpawnedHosting;
use crate::bootstrap::participation::PeerParticipation;

/// What the spawn path prepared for the meta runtime: the opened meta
/// journal, the node's peer identity, WAL and registry, the meta runtime's
/// config, and the node's view of its peers' participation, which the meta
/// shed hands leaderships by.
#[derive(Debug)]
pub struct SpawnedMeta {
    journal: MetaJournal,
    identity: PeerIdentity,
    wal: RaftWal,
    registry: RaftGroupHandleRegistry,
    config: MetaRuntimeConfig,
    participation: PeerParticipation,
    /// The meta journal's WAL counters.
    wal_metrics: ursula_runtime::RuntimeMetrics,
}

impl SpawnedMeta {
    /// Starts the meta runtime over gRPC on the prepared journal, and its
    /// meta shed.
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
        let shed = super::meta_shed::spawn_meta_shed(
            registry,
            self.participation,
            runtime.status_reader().clone(),
            node_id,
        );
        MetaSlot(Arc::new(Mutex::new(Some(ServerMeta {
            runtime,
            shed,
            hosting: None,
            wal_metrics: self.wal_metrics,
        }))))
    }
}

/// The meta runtime, the meta shed task and the hosting driver of a server
/// node.
#[derive(Debug)]
struct ServerMeta {
    runtime: GrpcMetaRuntime,
    shed: tokio::task::JoinHandle<()>,
    /// The hosting driver, once [`MetaSlot::start_hosting`] started it.
    hosting: Option<HostingTask>,
    wal_metrics: ursula_runtime::RuntimeMetrics,
}

/// What shutdown stops of a node's meta: the hosting driver first, so no
/// replica opens again while the groups stop, then the meta runtime once
/// the groups stopped.
#[derive(Debug)]
pub(crate) struct TakenMeta {
    pub(crate) runtime: GrpcMetaRuntime,
    pub(crate) hosting: Option<HostingTask>,
}

/// Where the server keeps its meta runtime until shutdown takes it.
#[derive(Debug, Clone, Default)]
pub struct MetaSlot(Arc<Mutex<Option<ServerMeta>>>);

impl MetaSlot {
    /// The meta runtime and the hosting driver, once: shutdown stops them.
    /// The meta shed stops here.
    pub(crate) fn take(&self) -> Option<TakenMeta> {
        let meta = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        meta.shed.abort();
        Some(TakenMeta {
            runtime: meta.runtime,
            hosting: meta.hosting,
        })
    }

    /// Starts the hosting driver of `hosting` over `runtime`'s groups and
    /// the meta runtime's views ([`SpawnedHosting::start`]), and keeps it
    /// for shutdown. Without a meta runtime, nothing starts and the node
    /// hosts nothing.
    pub(crate) async fn start_hosting(
        &self,
        hosting: SpawnedHosting,
        runtime: ursula_runtime::ShardRuntime,
        registry: RaftGroupHandleRegistry,
        wal: RaftWal,
    ) {
        let Some(reader) = self.status_reader() else {
            tracing::warn!("hosting: no meta runtime runs; the node hosts nothing");
            return;
        };
        let task = hosting.start(runtime, registry, wal, reader).await;
        let mut meta = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match meta.as_mut() {
            Some(meta) => meta.hosting = Some(task),
            // Shutdown took the runtime meanwhile: the driver stops too.
            None => drop(tokio::spawn(task.shutdown())),
        }
    }

    /// The meta journal's WAL counters, while the meta runtime runs.
    pub(crate) fn wal_metrics(&self) -> Option<ursula_runtime::RuntimeMetrics> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|meta| meta.wal_metrics.clone())
    }

    /// The hosting driver's last status, once it started.
    pub(crate) fn hosting_status(&self) -> Option<Arc<ursula_raft::HostingStatus>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|meta| meta.hosting.as_ref())
            .map(|hosting| hosting.status().current())
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

/// Binds this node's peer identity, its Raft node id `node_id` and the
/// cluster id of its WAL root, with its peers `directory` (`raft.peers`,
/// which lists the node), then opens the meta journal under
/// `raft-log/meta/`. A journal that does not open is handed on as such:
/// the meta runtime then runs `Damaged` and the node serves.
pub(crate) fn open_meta(
    config: &ursula_config::UrsulaConfig,
    node_id: u64,
    directory: PeerDirectory,
    wal: &RaftWal,
    registry: &RaftGroupHandleRegistry,
    shard_map: &ursula_shard::StaticShardMap,
    engine: RaftEngineConfig,
    snapshots: MetaSnapshotTrigger,
    participation: PeerParticipation,
) -> Result<SpawnedMeta, ShardMapError> {
    let raft = &config.raft;
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
