use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use openraft::EmptyNode;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
#[cfg(any(test, feature = "fault-injection"))]
use openraft::storage::RaftSnapshotBuilder;
#[cfg(any(test, feature = "fault-injection"))]
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use tokio::sync::watch;
use ursula_proto::admin::RaftTargetKey;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ErrorChain;
use ursula_runtime::GroupEngineError;
use ursula_runtime::OwnedPointer;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::default_snapshot_store;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;
#[cfg(test)]
use ursula_shard::ShardPlacement;

use crate::election::ElectionPolicy;
pub use crate::election::LeadershipShedFlag;
pub use crate::election::LeadershipShedReason;
pub use crate::election::LeadershipShedState;
use crate::log_store::WalOpening;
use crate::owner::OwnerRaftHandle;
use crate::rejoin::GroupRejoin;
use crate::rejoin::RecoveryGateError;
use crate::rejoin::RecoveryGateStatus;
use crate::replica::ReplicaCore;
use crate::snapshot_codec::decode_group_snapshot;
use crate::snapshot_holds::HeldWriteError;
use crate::snapshot_holds::SnapshotHolds;
use crate::state_machine::RaftGroupStateMachine;
use crate::state_machine::SnapshotBuildCoordinator;
use crate::state_machine::SnapshotInstallCoordinator;
use crate::state_machine::group_snapshot_id;
use crate::target::WireTarget;
use crate::types::RaftGroupMetricsSnapshot;
use crate::types::RaftLogProgressSnapshot;
use crate::types::SnapshotData;
use crate::types::SnapshotLease;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaSnapshot;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetworkFactory;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetwork;

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for SingleNodeRaftNetworkFactory {
    type Network = SingleNodeRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &EmptyNode) -> Self::Network {
        SingleNodeRaftNetwork
    }
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for SingleNodeRaftNetwork {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send AppendEntries")
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send Vote")
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<UrsulaRaftTypeConfig>,
        _snapshot: UrsulaSnapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send snapshots")
    }

    async fn transfer_leader(
        &mut self,
        _req: TransferLeaderRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig>,
        RPCError<UrsulaRaftTypeConfig>,
    > {
        unreachable!("single-node raft group must not transfer leadership")
    }
}

/// Owned handle to a single Raft group's [`Raft`] instance, as stored in the
/// [`RaftGroupHandleRegistry`]. Spelled out as a type alias so callers (e.g. the
/// HTTP admin handlers) can name it without repeating the full type-config
/// generics.
pub type RaftGroupHandle = Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>;

/// A group's shared cold-index page cache.
pub type GroupColdIndexCache = Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>;

/// Resources published atomically with a live owner handle. Only cache and
/// recovery are optional capabilities; every registered group has a barrier.
#[derive(Debug, Clone)]
pub(crate) struct ActiveGroup {
    /// The replica's barrier, recovery gate, history and apply health,
    /// which every plane's replica has.
    pub(crate) core: ReplicaCore,
    /// The engine's admission of snapshot installs and of the GC passes
    /// that run on its holds, closed once the engine begins to shut down.
    pub(crate) snapshot_installs: Arc<crate::openraft_work::WorkAdmission>,
    pub(crate) cache: Option<GroupColdIndexCache>,
    /// The state machine's log gauge and applied membership (#613).
    pub(crate) log_gauge: Arc<crate::snapshot_cadence::GroupLogGauge>,
    /// The engine's snapshot store, install coordinator and holds: an
    /// incoming snapshot is copied with them, and the group's GC passes run
    /// on them (#687).
    pub(crate) snapshots: GroupSnapshots,
}

/// One engine incarnation's side of its group's snapshot objects: the store
/// it writes to, the install coordinator its state machine takes copies
/// from, and its holds. The registry copies an incoming snapshot and runs
/// the group's GC passes with the engine's own, so a copy is held and
/// prefetched where the engine's install finds it, and a pass reads the
/// holds of the incarnation that runs (#687).
#[derive(Debug, Clone)]
pub(crate) struct GroupSnapshots {
    pub(crate) store: SharedSnapshotStore,
    pub(crate) install: SnapshotInstallCoordinator,
    pub(crate) holds: Arc<SnapshotHolds>,
}

impl ActiveGroup {
    pub(crate) fn raft(&self) -> &OwnerRaftHandle {
        self.core.owner()
    }
}

#[derive(Debug, Clone)]
enum GroupEntry {
    Active(Arc<ActiveGroup>),
    ApplyStopped {
        node_id: u64,
        stopped: crate::apply_failure::StoppedApply,
    },
}
impl GroupEntry {
    fn active(&self) -> Option<&Arc<ActiveGroup>> {
        match self {
            Self::Active(active) => Some(active),
            Self::ApplyStopped { .. } => None,
        }
    }

    /// The recovery gate that governs this node's participation. A group
    /// stopped by an apply failure reports the stop instead: its gate can no
    /// longer open, and it must not hold back the node's other groups.
    fn participating_gate(&self) -> Option<&Arc<GroupRejoin>> {
        self.active()?.core.participating_gate()
    }
}

#[derive(Debug, Clone)]
pub struct RaftGroupHandleRegistry {
    pub(crate) append_send_budget: crate::grpc::AppendTransportBudget,
    pub(crate) append_receive_budget: crate::grpc::AppendTransportBudget,
    /// The meta plane's own append budgets
    /// ([`crate::grpc::META_APPEND_BUDGET_BYTES`]), apart from the data
    /// plane's, which [`Self::with_append_transport_budget_bytes`] sets.
    pub(crate) meta_append_send_budget: crate::grpc::AppendTransportBudget,
    pub(crate) meta_append_receive_budget: crate::grpc::AppendTransportBudget,
    groups: Arc<arc_swap::ArcSwap<BTreeMap<u32, Arc<GroupEntry>>>>,
    election: ElectionPolicy,
    transport_shutdown: watch::Sender<bool>,
    snapshot_store: Arc<Mutex<SharedSnapshotStore>>,
    snapshot_build: Arc<Mutex<SnapshotBuildCoordinator>>,
    snapshot_install: SnapshotInstallCoordinator,
    /// How this node's Raft WAL opened, when its logs are durable.
    wal_opening: Arc<Mutex<Option<WalOpening>>>,
    /// This node's cluster identity on the Raft gRPC plane and the peers
    /// refused for another cluster id.
    peer_cluster: crate::peer_cluster::PeerCluster,
    /// How often each follower lost entries it had acknowledged to a leader
    /// on this node ([`Self::follower_log_reversions`]).
    follower_log_reversions: crate::rejoin::FollowerLogReversions,
    /// This node's running meta replica, apart from every structure keyed
    /// by group id, where its group 0 would meet data group 0. A restart
    /// swaps it, and an RPC resolves it per call.
    meta: Arc<arc_swap::ArcSwapOption<crate::meta::replica::ActiveMeta>>,
    /// This node's placement views, which outlive every meta replica.
    placement: crate::meta::watch::PlacementPublisher,
    /// Changes whenever a data group's entry is published or removed, so
    /// the hosting driver plans again ([`crate::hosting`]).
    group_changes: Arc<crate::rt::sync::watch::Sender<()>>,
}

impl Default for RaftGroupHandleRegistry {
    fn default() -> Self {
        let (transport_shutdown, _) = watch::channel(false);
        let peer_cluster = crate::peer_cluster::PeerCluster::default();
        Self {
            append_send_budget: Default::default(),
            append_receive_budget: Default::default(),
            meta_append_send_budget: crate::grpc::AppendTransportBudget::new(
                crate::grpc::META_APPEND_BUDGET_BYTES,
            ),
            meta_append_receive_budget: crate::grpc::AppendTransportBudget::new(
                crate::grpc::META_APPEND_BUDGET_BYTES,
            ),
            groups: Arc::new(arc_swap::ArcSwap::from_pointee(BTreeMap::new())),
            election: ElectionPolicy::default(),
            transport_shutdown,
            snapshot_store: Arc::new(Mutex::new(default_snapshot_store())),
            snapshot_build: Arc::new(Mutex::new(SnapshotBuildCoordinator::new(1))),
            snapshot_install: SnapshotInstallCoordinator::new(1),
            wal_opening: Arc::new(Mutex::new(None)),
            placement: crate::meta::watch::PlacementPublisher::for_peers(peer_cluster.clone()),
            peer_cluster,
            follower_log_reversions: Default::default(),
            meta: Arc::default(),
            group_changes: Arc::new(crate::rt::sync::watch::channel(()).0),
        }
    }
}

/// What one round of snapshot GC passes did
/// ([`RaftGroupHandleRegistry::collect_snapshot_garbage`], #669).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotGcPass {
    /// The groups whose pass ran.
    pub groups: usize,
    /// The snapshot objects the passes deleted.
    pub deleted: u64,
    /// The passes that failed, which run again later.
    pub failed: usize,
    /// The groups whose pass is due but past `max_groups` or the budget.
    pub left: usize,
}

/// Failure of snapshot admission, of the prefetch and copy of an incoming
/// snapshot, or of its installation. It is the error of
/// [`RaftGroupHandleRegistry::install_full_snapshot`] and is not
/// part of any wire form. The gRPC plane converts it once, by [`From`] into
/// `tonic::Status`, whose code is [`SnapshotInstallError::code`].
///
/// The in-process network of the tests and the simulation classifies it on
/// its own, and does not yet back off for every refusal the gRPC plane
/// answers `UNAVAILABLE`. Admission failures stay network errors there, and
/// so does every group refusal but a refused overwrite,
/// `Group(OwnerStopped)` among them (#725).
#[derive(Debug, thiserror::Error)]
pub enum SnapshotInstallError {
    #[error("snapshot group {group:?} is not registered")]
    NotRegistered { group: RaftGroupId },
    #[error("snapshot group {group:?} is shutting down")]
    ShuttingDown { group: RaftGroupId },
    #[error(transparent)]
    Group(#[from] GroupEngineError),
    #[error("snapshot install task failed")]
    Task(#[source] crate::rt::task::JoinError),
    /// The node's snapshot install gate closed while the install waited for
    /// a permit.
    #[error("snapshot group {group:?}: the snapshot install gate is closed")]
    GateClosed { group: RaftGroupId },
    /// The incoming snapshot data is no snapshot pointer.
    #[error("snapshot group {group:?}: the incoming snapshot pointer does not decode")]
    PointerUndecodable {
        group: RaftGroupId,
        #[source]
        source: ursula_runtime::SnapshotStoreError,
    },
    /// The object the incoming pointer names is gone. The sender sends its
    /// current pointer on the next attempt.
    #[error("snapshot group {group:?}: the incoming snapshot object {source_key} is missing")]
    SourceMissing {
        group: RaftGroupId,
        source_key: String,
    },
    /// The object the incoming pointer names did not read.
    #[error("snapshot group {group:?}: read the incoming snapshot object {source_key}")]
    SourceRead {
        group: RaftGroupId,
        source_key: String,
        #[source]
        source: ursula_runtime::SnapshotStoreError,
    },
    /// The incoming snapshot's group state does not decode.
    #[error("snapshot group {group:?}: incoming snapshot {snapshot_id} does not decode")]
    Undecodable {
        group: RaftGroupId,
        snapshot_id: String,
        #[source]
        source: ursula_runtime::SnapshotStoreError,
    },
    /// Writing this node's copy of the incoming snapshot failed.
    #[error("snapshot group {group:?}: write the snapshot copy {own_key}")]
    CopyWrite {
        group: RaftGroupId,
        own_key: String,
        #[source]
        source: ursula_runtime::SnapshotStoreError,
    },
    /// This node's copy of the incoming snapshot stores other bytes than the
    /// source.
    #[error(
        "snapshot group {group:?}: the snapshot copy {own_key} stores {actual} bytes, expected \
         {expected}"
    )]
    CopyVerify {
        group: RaftGroupId,
        own_key: String,
        expected: u64,
        actual: u64,
    },
    /// The name drawn for this node's copy was taken, also on the retry.
    #[error("snapshot group {group:?}: the snapshot copy name {own_key} is taken")]
    NameCollision { group: RaftGroupId, own_key: String },
    /// The prefetched pointer handed to OpenRaft did not encode.
    #[error("snapshot group {group:?}: encode the prefetched snapshot pointer")]
    PointerEncode {
        group: RaftGroupId,
        #[source]
        source: ursula_runtime::SnapshotStoreError,
    },
    /// The incoming meta snapshot's pointer is not the meta group's own: it
    /// is not inline, or names more than the bound.
    #[error("the incoming meta snapshot's pointer is refused")]
    MetaPointer {
        #[source]
        source: crate::meta::snapshot::MetaSnapshotError,
    },
    /// The incoming meta snapshot's state does not decode.
    #[error("the incoming meta snapshot's state does not decode")]
    MetaState {
        #[source]
        source: ursula_stream::msgpack::DecodeError,
    },
}

impl SnapshotInstallError {
    /// The gRPC code the sender gets. A [`Self::Group`] refusal gets the
    /// code of the group's error ([`crate::grpc::group_rpc_code`]). No
    /// refusal is `FAILED_PRECONDITION` (#539).
    pub(crate) fn code(&self) -> tonic::Code {
        match self {
            Self::NotRegistered { .. } => tonic::Code::NotFound,
            Self::PointerUndecodable { .. } | Self::MetaPointer { .. } => {
                tonic::Code::InvalidArgument
            }
            Self::Undecodable { .. } | Self::MetaState { .. } => tonic::Code::DataLoss,
            Self::PointerEncode { .. } => tonic::Code::Internal,
            Self::Group(error) => crate::grpc::group_rpc_code(error),
            Self::ShuttingDown { .. }
            | Self::Task(_)
            | Self::GateClosed { .. }
            | Self::SourceMissing { .. }
            | Self::SourceRead { .. }
            | Self::CopyWrite { .. }
            | Self::CopyVerify { .. }
            | Self::NameCollision { .. } => tonic::Code::Unavailable,
        }
    }

    /// Whether the sender should back off before it sends again: the
    /// refusals the gRPC plane answers `UNAVAILABLE`.
    pub fn peer_backs_off(&self) -> bool {
        self.code() == tonic::Code::Unavailable
    }
}

impl From<SnapshotInstallError> for tonic::Status {
    fn from(error: SnapshotInstallError) -> Self {
        Self::new(error.code(), ErrorChain(&error).to_string())
    }
}

/// Why a snapshot built outside OpenRaft's own build path failed
/// ([`RaftGroupHandleRegistry::build_snapshot_for_transfer`], tests and
/// fault injection only).
#[cfg(any(test, feature = "fault-injection"))]
#[derive(Debug, thiserror::Error)]
pub enum SnapshotBuildError {
    /// The group is not registered, or its Raft stopped.
    #[error(transparent)]
    Group(#[from] GroupEngineError),
    /// The state machine's build failed.
    #[error("build the group's snapshot")]
    Build(#[source] std::io::Error),
}

#[derive(Debug)]
pub(crate) struct PrefetchedInstallSnapshot {
    pub(crate) snapshot: UrsulaSnapshot,
    guard: Option<PrefetchedSnapshotGuard>,
    /// The history event the snapshot's state holds, for the commit screen
    /// (#613); `None` when it is the snapshot this replica already holds.
    history: Option<Option<ursula_runtime::HistoryEvent>>,
}

#[derive(Debug)]
struct PrefetchedSnapshotGuard {
    snapshot_install: SnapshotInstallCoordinator,
    cache_key: String,
}

impl Drop for PrefetchedSnapshotGuard {
    fn drop(&mut self) {
        self.snapshot_install.clear_prefetched_key(&self.cache_key);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QuorumProofError {
    #[error("Raft group {group:?} is not registered on this node")]
    NotRegistered { group: RaftGroupId },
    #[error("Raft group {group:?} is not led by this node")]
    NotLeader { group: RaftGroupId },
    #[error("Raft group {group:?} changed leadership while confirming its quorum")]
    LeadershipChanged { group: RaftGroupId },
    #[error("Raft group {group:?} waits for its history event; its leader confirms no barrier")]
    HistoryPending { group: RaftGroupId },
    #[error("ReadIndex confirmation failed for {group:?}: {source}")]
    Read {
        group: RaftGroupId,
        #[source]
        source: GroupEngineError,
    },
}

/// A self-election ([`RaftGroupHandleRegistry::request_self_election`]) this
/// node refused or OpenRaft did not start.
#[derive(Debug, thiserror::Error)]
pub enum SelfElectionError {
    #[error("Raft group {group:?} is in term {observed}, not the requested {requested}")]
    TermChanged {
        group: RaftGroupId,
        requested: u64,
        observed: u64,
    },
    #[error(transparent)]
    Transfer(#[from] IncomingTransferError),
    #[error("OpenRaft rejected the self-election for {group:?}: {source}")]
    Rejected {
        group: RaftGroupId,
        #[source]
        source: openraft::raft::TransferLeaderError<UrsulaRaftTypeConfig>,
    },
}

/// Confirm a fresh ReadIndex under the same committed leader vote. The
/// barrier index is the higher of the read index and the leader's last log
/// index read after the round, and a gated replica opens its gate only once
/// it applied that index.
///
/// The read index alone can be too low. Say a follower acknowledged entry
/// 100 and its host crashed before 100 reached its disk, while the leader
/// had processed its acknowledgements only up to 99. The follower restarts
/// gated, still holding 99, so its heartbeat succeeds and the leader does
/// not rewind it. Its barrier round can complete with read index 99 before
/// the dead process's acknowledgement of 100 reaches the leader. Were the
/// gate to open at 99, that late acknowledgement would then commit 100 on
/// the leader and a follower that no longer holds it, and losing the leader
/// would lose 100. Within one leadership the leader sent nothing past its
/// last log index, so no acknowledgement of the dead process names more,
/// and a replica that applied that index holds every entry such an
/// acknowledgement could have counted. A deposed leader fails the round, and
/// the new leader's barrier takes its place.
pub(crate) async fn confirm_recovery_barrier<SM>(
    group: RaftGroupId,
    raft: &OwnerRaftHandle<SM>,
    barrier: &dyn ursula_runtime::LinearizableReadBarrier,
) -> Result<crate::BarrierAnswer, QuorumProofError>
where
    SM: openraft::storage::RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>,
{
    let before = raft.metrics().borrow_watched().clone();
    if before.current_leader != Some(before.id) || !before.vote.is_committed() {
        return Err(QuorumProofError::NotLeader { group });
    }
    let index = barrier
        .confirm()
        .await
        .map_err(|source| QuorumProofError::Read { group, source })?
        .ok_or(QuorumProofError::LeadershipChanged { group })?;
    let after = raft.metrics().borrow_watched().clone();
    if after.current_leader != Some(after.id) || after.vote != before.vote {
        return Err(QuorumProofError::LeadershipChanged { group });
    }
    let last_log_index = after.last_log_index.unwrap_or_default();
    Ok(crate::BarrierAnswer {
        vote: after.vote,
        index: index.max(last_log_index),
        // The memberships of the leader view the barrier confirmed, from
        // the same read of the metrics.
        memberships: crate::LeaderMemberships::of(&after),
    })
}

pub use crate::election::IncomingTransferError;
pub use crate::election::LeadershipTransferError;

impl RaftGroupHandleRegistry {
    /// Configure the node-wide budget of the data plane for each direction before constructing
    /// transports. The meta plane keeps its own.
    pub fn with_append_transport_budget_bytes(mut self, bytes: usize) -> Self {
        self.append_send_budget = crate::grpc::AppendTransportBudget::new(bytes);
        self.append_receive_budget = crate::grpc::AppendTransportBudget::new(bytes);
        self
    }
    /// The local campaign policy, shared by registration, recovery and RPCs.
    pub fn election_policy(&self) -> ElectionPolicy {
        self.election.clone()
    }

    pub fn may_campaign(&self, group: RaftGroupId) -> bool {
        self.entry(group)
            .is_ok_and(|entry| self.election.may_campaign(entry.core.recovery.as_deref()))
    }

    /// Submit a handoff after checking it against the handoff policy
    /// ([`Self::check_handoff`]). The receiver independently checks its own
    /// recovery gate and OpenRaft never reports its refusal, so a handoff
    /// that has not moved the leadership by its deadline is abandoned.
    pub async fn transfer_leader(
        &self,
        group: RaftGroupId,
        target: u64,
    ) -> Result<(), LeadershipTransferError> {
        let (entry, _metrics) = self.handoff_view(group)?;
        entry
            .core
            .transfer_leader(&self.election, group, target)
            .await
    }

    /// Whether this node could hand its leadership of `group` to `target`
    /// now, by the policy [`Self::transfer_leader`] enforces. Handoff
    /// planners pick their targets with it.
    pub fn check_handoff(
        &self,
        group: RaftGroupId,
        target: u64,
    ) -> Result<(), LeadershipTransferError> {
        let (entry, metrics) = self.handoff_view(group)?;
        entry
            .core
            .validate_handoff(&self.election, group, target, &metrics)
    }

    /// [`Self::transfer_leader`] on this node's meta replica.
    pub async fn transfer_meta_leader(&self, target: u64) -> Result<(), LeadershipTransferError> {
        let group = crate::meta::state_machine::META_GROUP;
        let meta = self
            .meta()
            .ok_or(LeadershipTransferError::NotRegistered { group })?;
        meta.core
            .transfer_leader(&self.election, group, target)
            .await
    }

    /// How long a handoff of the meta leadership takes to move it, at
    /// most, when this node runs a meta replica.
    pub fn meta_handoff_settle_time(&self) -> Option<std::time::Duration> {
        self.meta()
            .map(|meta| meta.core.owner().handoff_settle_time())
    }

    /// [`Self::check_handoff`] on this node's meta replica.
    pub fn check_meta_handoff(&self, target: u64) -> Result<(), LeadershipTransferError> {
        let group = crate::meta::state_machine::META_GROUP;
        let meta = self
            .meta()
            .ok_or(LeadershipTransferError::NotRegistered { group })?;
        let metrics = meta.core.handoff_view();
        meta.core
            .validate_handoff(&self.election, group, target, &metrics)
    }

    /// One registry entry, so the gate checked is the gate of the Raft that
    /// transfers.
    fn handoff_view(
        &self,
        group: RaftGroupId,
    ) -> Result<(Arc<ActiveGroup>, RaftMetrics<UrsulaRaftTypeConfig>), LeadershipTransferError>
    {
        let entry = self
            .groups
            .load()
            .get(&group.0)
            .and_then(|entry| entry.active().cloned())
            .ok_or(LeadershipTransferError::NotRegistered { group })?;
        let metrics = entry.core.handoff_view();
        Ok((entry, metrics))
    }

    pub async fn confirm_quorum_prefix(
        &self,
        group: RaftGroupId,
    ) -> Result<ursula_proto::admin::QuorumPrefix, QuorumProofError> {
        let answer = self.confirm_recovery_barrier(group).await?;
        Ok(quorum_prefix(group, &answer))
    }

    /// [`Self::confirm_quorum_prefix`] on this node's meta replica, which
    /// answers as group 0.
    pub async fn confirm_meta_quorum_prefix(
        &self,
    ) -> Result<ursula_proto::admin::QuorumPrefix, QuorumProofError> {
        let group = crate::meta::state_machine::META_GROUP;
        let meta = self
            .meta()
            .ok_or(QuorumProofError::NotRegistered { group })?;
        let answer = meta.core.confirm_barrier(group).await?;
        Ok(quorum_prefix(group, &answer))
    }

    /// This node's meta Raft, while it runs a meta replica: what the admin
    /// snapshot route triggers and waits on.
    pub fn meta_raft(&self) -> Option<OwnerRaftHandle<crate::meta::MetaStateMachine>> {
        self.meta().map(|meta| meta.core.owner().clone())
    }

    pub(crate) async fn confirm_recovery_barrier(
        &self,
        group: RaftGroupId,
    ) -> Result<crate::BarrierAnswer, QuorumProofError> {
        let core = {
            let groups = self.groups.load();
            let entry = groups
                .get(&group.0)
                .ok_or(QuorumProofError::NotRegistered { group })?;
            let entry = entry
                .active()
                .ok_or(QuorumProofError::NotRegistered { group })?;
            entry.core.clone()
        };
        core.confirm_barrier(group).await
    }

    /// Records how this node's Raft WAL opened.
    pub fn set_wal_opening(&self, opening: WalOpening) {
        *self
            .wal_opening
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(opening);
    }

    /// This node's side of the cluster-id check between peers: its identity
    /// once an engine factory binds its WAL's, and the peers it refused.
    pub fn peer_cluster(&self) -> &crate::peer_cluster::PeerCluster {
        &self.peer_cluster
    }

    /// How this node's Raft WAL opened; `None` until a WAL is attached.
    pub fn wal_opening(&self) -> Option<WalOpening> {
        *self
            .wal_opening
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Stops long-lived Raft transport sessions before the node server exits.
    ///
    /// This lets peers reconnect promptly instead of retaining a stream backed
    /// by a node instance that is already shutting down.
    pub fn shutdown_transport(&self) {
        self.transport_shutdown.send_replace(true);
    }

    pub(crate) fn subscribe_transport_shutdown(&self) -> watch::Receiver<bool> {
        self.transport_shutdown.subscribe()
    }

    /// Publish all group resources together, before any transport can find its Raft handle.
    pub(crate) fn register_engine(&self, engine: &crate::RaftGroupEngine) {
        let entry = Arc::new(GroupEntry::Active(Arc::new(ActiveGroup {
            core: ReplicaCore {
                barrier: engine.read_barrier.clone(),
                recovery: engine
                    .rejoin
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
                history: engine.history.clone(),
                apply_health: engine.apply_health.clone(),
            },
            snapshot_installs: engine.snapshot_installs.clone(),
            cache: engine.cold_index_cache.clone(),
            log_gauge: engine.log_gauge.clone(),
            snapshots: engine.snapshots.clone(),
        })));
        self.groups.rcu(|groups| {
            let mut groups = (**groups).clone();
            groups.insert(engine.placement.raft_group_id.0, entry.clone());
            groups
        });
        self.group_changes.send_replace(());
        self.refresh_group_elections(engine.placement.raft_group_id);
    }

    /// Publishes `engine` as this node's replica of its group, as starting
    /// the group does ([`Self::register_engine`]), for a simulation that
    /// builds engines itself and serves HTTP through this registry's
    /// lock-free leadership view ([`Self::leadership`]).
    #[cfg(madsim)]
    pub fn register_engine_for_simulation(&self, engine: &crate::RaftGroupEngine) {
        self.register_engine(engine);
    }

    /// Forgets this node's replica of `group`, as a node that stops
    /// hosting it does (step 2 of a close, design 7.3): no transport finds
    /// its handle and the leadership view no longer knows the group. The
    /// engine itself stops apart from this, through its runtime.
    pub fn unregister_group(&self, group: RaftGroupId) {
        self.groups.rcu(|groups| {
            let mut groups = (**groups).clone();
            groups.remove(&group.0);
            groups
        });
        self.group_changes.send_replace(());
    }

    /// The data groups this node publishes an entry of: every registered
    /// engine and every group whose apply stopped.
    pub fn registered_groups(&self) -> std::collections::BTreeSet<RaftGroupId> {
        self.groups
            .load()
            .keys()
            .copied()
            .map(RaftGroupId)
            .collect()
    }

    /// A watch that changes whenever a data group's entry is published or
    /// removed.
    pub(crate) fn subscribe_group_changes(&self) -> crate::rt::sync::watch::Receiver<()> {
        self.group_changes.subscribe()
    }

    pub(crate) fn register_apply_stopped(
        &self,
        group: RaftGroupId,
        node_id: u64,
        stopped: crate::apply_failure::StoppedApply,
    ) {
        let entry = Arc::new(GroupEntry::ApplyStopped { node_id, stopped });
        self.groups.rcu(|groups| {
            let mut groups = (**groups).clone();
            groups.insert(group.0, entry.clone());
            groups
        });
        self.group_changes.send_replace(());
    }

    /// The group's recovery gate, if it has one.
    pub fn rejoin(&self, raft_group_id: RaftGroupId) -> Option<Arc<GroupRejoin>> {
        self.groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active()?.core.recovery.clone())
    }

    /// Operator recovery when a majority of the group's voters are gated
    /// and may have lost acknowledged writes: this replica takes the
    /// recovery plan (#581 section 4.5; see
    /// [`GroupRejoin::accept_recovery_plan`]) as one owner job, its group's
    /// leader fence learns the plan, and the group's elections follow it.
    /// The replica core runs the plan ([`ReplicaCore::accept_plan`]). A
    /// plan that cuts restarts the replica through `restart`, and the core
    /// carries on with the replica this registry then holds.
    pub async fn accept_recovery_plan(
        &self,
        raft_group_id: RaftGroupId,
        plan: &RecoveryPlanRequest,
        restart: &mut dyn crate::rejoin::ReplicaRestart,
    ) -> Result<RecoveryPlanReport, RecoveryGateError> {
        let group = RaftTarget::Data(raft_group_id);
        check_plan_floor(group, plan)?;
        let entry = self
            .groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active().cloned())
            .ok_or(RecoveryGateError::NotRegistered { group })?;
        let core = entry.core.clone();
        let applied = entry.log_gauge.applied_membership();
        drop(entry);
        core.accept_plan(&self.election, group, plan, applied, move || {
            futures_util::FutureExt::boxed(async move {
                let Some(restarted) =
                    restart
                        .restart_replica(raft_group_id)
                        .await
                        .map_err(|source| RecoveryGateError::Restart {
                            group,
                            source: Box::new(source),
                        })?
                else {
                    return Ok(None);
                };
                // The replica this registry holds after the restart, with
                // the recovery gate the restart returned.
                Ok::<_, RecoveryGateError>(
                    self.groups
                        .load()
                        .get(&raft_group_id.0)
                        .and_then(|entry| entry.active().cloned())
                        .map(|entry| ReplicaCore {
                            recovery: Some(restarted),
                            ..entry.core.clone()
                        }),
                )
            })
        })
        .await
    }

    /// The recovery gate of every replica that has one on this node, by
    /// its group: each data group's by its id, the meta replica's as
    /// `meta`.
    pub fn recovery_gates(&self) -> BTreeMap<RaftTargetKey, RecoveryGateStatus> {
        let meta = self.meta();
        let meta_gate = meta
            .as_ref()
            .and_then(|meta| meta.core.participating_gate())
            .map(|gate| (RaftTargetKey::Meta, gate.status()));
        self.groups
            .load()
            .iter()
            .filter_map(|(id, entry)| {
                entry
                    .participating_gate()
                    .map(|gate| (RaftTargetKey::Data(*id), gate.status()))
            })
            .chain(meta_gate)
            .collect()
    }

    /// How many times each follower lost Raft log entries it had
    /// acknowledged to a leader on this node, by follower node id, over all
    /// groups since the node started. It only grows: a group's restart or
    /// stop keeps its counts. The leader rewinds such a follower in place. A
    /// follower that keeps appearing here without a host crash or a lost
    /// disk points at a disk that drops synced writes, or at a bug.
    pub fn follower_log_reversions(&self) -> BTreeMap<u64, u64> {
        self.follower_log_reversions.snapshot()
    }

    /// The counter behind [`Self::follower_log_reversions`], which the heal
    /// drivers of this node's groups count into.
    pub(crate) fn follower_log_reversion_counter(&self) -> crate::rejoin::FollowerLogReversions {
        self.follower_log_reversions.clone()
    }

    /// The recovery gates of [`Self::recovery_gates`] that are not open,
    /// and the groups among them whose gated replica on this node got no
    /// leader barrier and applied nothing for
    /// [`crate::RECOVERY_STALL_AFTER`]: they wait for an operator to accept
    /// the loss of the unsynced tail.
    pub fn recovery_report(&self) -> ursula_proto::admin::RecoveryGatesReport {
        ursula_proto::admin::RecoveryGatesReport::new(self.recovery_gates())
    }

    /// The data groups of [`Self::recovery_report`] that stalled, which
    /// readiness reports. The meta replica's gate never makes the node
    /// unready, as in [`Self::recovery_barriers_ready`]: readiness gates
    /// the data plane's serving.
    pub fn stalled_recovery_groups(&self) -> Vec<u32> {
        self.recovery_report()
            .stalled
            .into_iter()
            .filter_map(|group| match group {
                RaftTargetKey::Data(group) => Some(group),
                RaftTargetKey::Meta => None,
            })
            .collect()
    }

    /// The group's shared cold-index page cache, if one was registered.
    pub fn cold_index_cache(&self, raft_group_id: RaftGroupId) -> Option<GroupColdIndexCache> {
        self.groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active()?.cache.clone())
    }

    pub fn get(&self, raft_group_id: RaftGroupId) -> Option<OwnerRaftHandle> {
        self.groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active().map(|active| active.raft().clone()))
    }

    /// This replica's view of a group's leadership
    /// ([`OwnerRaftHandle::leadership`]), without cloning its handle. `None`
    /// when no active replica of the group is registered here.
    pub fn leadership(&self, raft_group_id: RaftGroupId) -> Option<crate::GroupLeadership> {
        self.groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active().map(|active| active.raft().leadership()))
    }

    pub fn contains_group(&self, raft_group_id: RaftGroupId) -> bool {
        self.groups.load().contains_key(&raft_group_id.0)
    }

    pub fn len(&self) -> usize {
        self.groups.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot installs: at most `max_concurrency` at once, and the wait
    /// of a group's second GC pass after a write that failed or a restore
    /// (`raft.install_snapshot_timeout` on a server), within which a PUT the
    /// store answered with an error lands if it lands at all (#687).
    pub fn with_snapshot_install(
        mut self,
        max_concurrency: usize,
        late_write: std::time::Duration,
    ) -> Self {
        self.snapshot_install =
            SnapshotInstallCoordinator::with_late_write(max_concurrency, late_write);
        self
    }

    /// Runs the snapshot GC passes due now (#669), for at most `max_groups`
    /// groups in group order, and starts none once `budget` has passed:
    /// each lists the node's directory for its group and deletes every
    /// object the group no longer holds (#687). A failed pass runs again
    /// later, and a pass left out stays due.
    pub async fn collect_snapshot_garbage(
        &self,
        max_groups: usize,
        budget: std::time::Duration,
    ) -> SnapshotGcPass {
        let started = crate::rt::time::Instant::now();
        // Each pass runs with the admission of the incarnation whose holds
        // it reads, so a group that restarts meanwhile does not have its new
        // objects deleted by the old holds (#687, #709).
        let due: Vec<(
            RaftGroupId,
            GroupSnapshots,
            Arc<crate::openraft_work::WorkAdmission>,
        )> = self
            .groups
            .load()
            .iter()
            .filter_map(|(group, entry)| {
                let active = entry.active()?;
                // An engine that stopped, and whose group did not open
                // again, owes passes no one runs: it takes no slot.
                let due =
                    active.snapshot_installs.is_open() && active.snapshots.holds.gc_due(started);
                due.then(|| {
                    (
                        RaftGroupId(*group),
                        active.snapshots.clone(),
                        active.snapshot_installs.clone(),
                    )
                })
            })
            .collect();
        let mut pass = SnapshotGcPass {
            left: due.len(),
            ..SnapshotGcPass::default()
        };
        for (group, snapshots, admission) in due.into_iter().take(max_groups) {
            if started.elapsed() >= budget {
                break;
            }
            let now = crate::rt::time::Instant::now();
            pass.left = pass.left.saturating_sub(1);
            let Some(collected) = snapshots.fenced_pass(&admission, now).await else {
                // The incarnation stopped: the next one runs its own passes.
                continue;
            };
            pass.groups = pass.groups.saturating_add(1);
            match collected {
                Ok(collected) => {
                    pass.deleted = pass.deleted.saturating_add(collected.deleted());
                }
                Err(err) => {
                    pass.failed = pass.failed.saturating_add(1);
                    tracing::warn!(
                        raft_group_id = group.0,
                        error = %ErrorChain(&err),
                        "snapshot GC pass failed; retrying later"
                    );
                }
            }
        }
        pass
    }

    pub fn set_snapshot_build_max_concurrency(&self, max_concurrency: usize) {
        let mut coordinator = self
            .snapshot_build
            .lock()
            .expect("raft group snapshot build coordinator mutex");
        *coordinator = coordinator.with_max_concurrency(max_concurrency);
    }

    pub fn set_snapshot_store(&self, snapshot_store: Option<SharedSnapshotStore>) {
        *self
            .snapshot_store
            .lock()
            .expect("raft group snapshot store mutex") =
            snapshot_store.unwrap_or_else(default_snapshot_store);
    }

    /// The store this node's engines write their snapshot objects to.
    pub(crate) fn snapshot_store(&self) -> SharedSnapshotStore {
        self.snapshot_store
            .lock()
            .expect("raft group snapshot store mutex")
            .clone()
    }

    pub fn snapshot_install_coordinator(&self) -> SnapshotInstallCoordinator {
        self.snapshot_install.clone()
    }

    pub fn snapshot_build_coordinator(&self) -> SnapshotBuildCoordinator {
        self.snapshot_build
            .lock()
            .expect("raft group snapshot build coordinator mutex")
            .clone()
    }

    pub fn leadership_shed_state(&self) -> LeadershipShedState {
        self.election.state()
    }

    pub fn mark_leadership_shed(&self, reason: LeadershipShedReason) {
        let previous = self
            .election
            .flag()
            .fetch_or(reason.bit(), Ordering::Release);
        let previous_state = LeadershipShedState::from_bits_truncate(previous);
        let current = LeadershipShedState::from_bits_truncate(previous | reason.bit());
        if previous_state.should_campaign() != current.should_campaign() {
            self.set_registered_group_elections();
        }
        if previous & reason.bit() == 0 {
            tracing::warn!("leadership-shed: mark {reason}; state={current}");
        }
    }

    pub fn clear_leadership_shed(&self, reason: LeadershipShedReason) {
        let previous = self
            .election
            .flag()
            .fetch_and(!reason.bit(), Ordering::Release);
        let previous_state = LeadershipShedState::from_bits_truncate(previous);
        let current = LeadershipShedState::from_bits_truncate(previous & !reason.bit());
        if previous_state.should_campaign() != current.should_campaign() {
            self.set_registered_group_elections();
        }
        if previous & reason.bit() != 0 {
            tracing::warn!("leadership-shed: clear {reason}; state={current}");
        }
    }

    fn set_registered_group_elections(&self) {
        let groups = self.groups.load();
        for entry in groups.values().filter_map(|entry| entry.active()) {
            self.election
                .refresh(entry.raft(), entry.core.recovery.clone());
        }
        if let Some(meta) = self.meta() {
            meta.refresh_elections();
        }
    }

    /// This node's running meta replica, if one runs.
    pub fn meta(&self) -> Option<Arc<crate::meta::replica::ActiveMeta>> {
        self.meta.load_full()
    }

    /// A watch of this node's placement views.
    pub fn placement_watch(&self) -> crate::meta::watch::PlacementWatch {
        self.placement.subscribe()
    }

    pub(crate) fn placement_publisher(&self) -> crate::meta::watch::PlacementPublisher {
        self.placement.clone()
    }

    /// Publishes `meta` as this node's running meta replica.
    pub(crate) fn publish_meta(&self, meta: Option<Arc<crate::meta::replica::ActiveMeta>>) {
        self.meta.store(meta);
    }

    /// Removes `meta` from the slot, if it still holds it: a replica that
    /// replaced it stays.
    pub(crate) fn unpublish_meta(&self, meta: &Arc<crate::meta::replica::ActiveMeta>) {
        self.meta.rcu(|current| match current {
            Some(current) if Arc::ptr_eq(current, meta) => None,
            other => other.clone(),
        });
    }

    /// A recovery plan on this node's meta replica, as
    /// [`Self::accept_recovery_plan`] runs one on a data group's: the
    /// replica takes it as one owner job, and a plan that cuts the meta log
    /// restarts the replica through `restart` before it answers
    /// ([`crate::meta::replica::MetaReplicaRestart::restart`]).
    pub async fn accept_meta_recovery_plan<N>(
        &self,
        plan: &RecoveryPlanRequest,
        restart: &mut crate::meta::replica::MetaReplicaRestart<N>,
    ) -> Result<RecoveryPlanReport, RecoveryGateError>
    where
        N: openraft::RaftNetworkFactory<UrsulaRaftTypeConfig> + Clone + Send,
        N::Network: RaftNetworkV2<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>,
    {
        let group = RaftTarget::Meta;
        check_plan_floor(group, plan)?;
        let core = self
            .meta()
            .ok_or(RecoveryGateError::NotRegistered { group })?
            .core
            .clone();
        let applied = core
            .owner()
            .with_state_machine(|state_machine: &mut crate::meta::MetaStateMachine| {
                Box::pin(std::future::ready(state_machine.applied_membership()))
            })
            .await
            .map_err(|source| RecoveryGateError::OwnerStopped { group, source })?;
        core.accept_plan(&self.election, group, plan, applied, move || {
            futures_util::FutureExt::boxed(async move {
                let restarted =
                    restart
                        .restart()
                        .await
                        .map_err(|source| RecoveryGateError::MetaRestart {
                            source: Box::new(source),
                        })?;
                Ok::<_, RecoveryGateError>(Some(restarted.active().core.clone()))
            })
        })
        .await
    }

    /// Serialize recovery-barrier changes with maintenance policy updates.
    pub(crate) fn refresh_group_elections(&self, group: RaftGroupId) {
        let groups = self.groups.load();
        if let Some(entry) = groups.get(&group.0).and_then(|entry| entry.active()) {
            self.election
                .refresh(entry.raft(), entry.core.recovery.clone());
        }
    }

    #[cfg(test)]
    fn is_leadership_shed(&self) -> bool {
        self.leadership_shed_state().is_shed()
    }

    /// Whether this node's leadership-shed policy lets it campaign.
    pub fn should_campaign(&self) -> bool {
        self.election.may_campaign(None) && self.recovery_barriers_ready()
    }

    /// Whether this node may take a planned leadership handoff: it may
    /// campaign and runs a meta replica.
    pub fn should_accept_transfer(&self) -> bool {
        self.election.may_accept_transfer(None) && self.recovery_barriers_ready()
    }

    /// This node's leadership-shed state, as a node of `cluster_id` answers
    /// it. A node cannot receive a planned leadership handoff until the
    /// recovery gate of every registered group is open.
    pub fn participation_status(
        &self,
        cluster_id: ursula_headers::ClusterId,
    ) -> ursula_proto::admin::LeadershipShedStatus {
        let state = self.election.state();
        ursula_proto::admin::LeadershipShedStatus {
            cluster_id,
            bits: state.bits(),
            state: state.to_string(),
            should_accept_transfer: self.should_accept_transfer(),
            should_campaign: self.should_campaign(),
            recovery_barriers_ready: self.recovery_barriers_ready(),
            should_shed_current_leaders: state.should_shed_current_leaders(),
        }
    }

    /// Every recovery gate on this node is open. A group stopped by an apply
    /// failure has no gate to open: its own campaign and transfer eligibility
    /// close, and it must not close planned handoffs of the node's other groups.
    pub fn recovery_barriers_ready(&self) -> bool {
        self.groups.load().values().all(|entry| {
            entry
                .participating_gate()
                .is_none_or(|gate| gate.may_campaign())
        })
    }

    pub fn metrics_snapshot(&self) -> Vec<RaftGroupMetricsSnapshot> {
        let groups = self
            .groups
            .load()
            .iter()
            .map(|(id, entry)| (*id, entry.clone()))
            .collect::<Vec<_>>();

        let log_progress = self.snapshot_build_coordinator().log_progress();
        let mut snapshots = Vec::with_capacity(groups.len());
        for (raft_group_id, entry) in groups {
            let log = log_progress
                .get(&raft_group_id)
                .copied()
                .unwrap_or_default();
            let entry = match entry.as_ref() {
                GroupEntry::Active(active) => active,
                GroupEntry::ApplyStopped { node_id, stopped } => {
                    snapshots.push(RaftGroupMetricsSnapshot {
                        raft_group_id,
                        node_id: *node_id,
                        current_term: stopped.term,
                        current_leader: None,
                        last_log: None,
                        committed: None,
                        last_applied: stopped.last_applied.map(log_progress_snapshot),
                        snapshot: None,
                        purged: None,
                        voter_ids: Vec::new(),
                        voter_configurations: Vec::new(),
                        committed_voter_configurations: Vec::new(),
                        learner_ids: Vec::new(),
                        maintenance: crate::types::RaftGroupMaintenanceState {
                            running: false,
                            recovery_ready: false,
                            accepting_transfers: false,
                            membership_joint: false,
                            membership_log_index: None,
                            stopped_for_operator: false,
                        },
                        apply_failure: Some(stopped.failure()),
                        log,
                        history: None,
                        recovery: None,
                        recovery_gate: ursula_proto::admin::RecoveryGateStatus::Open,
                        leads_live: false,
                    });
                    continue;
                }
            };
            let raft = entry.raft();
            // A removal's debt lasts until the log is purged through the
            // snapshot that dropped it (#669).
            entry.log_gauge.observe_purged(
                raft.metrics()
                    .borrow_watched()
                    .purged
                    .map(|purged| purged.index),
            );
            snapshots.push(entry.core.metrics_snapshot(
                raft_group_id,
                &self.election,
                entry.log_gauge.progress(),
                entry.log_gauge.applied_membership(),
            ));
        }
        snapshots
    }

    /// This node's meta replica's metrics snapshot, apart from
    /// [`Self::metrics_snapshot`], where its group 0 would meet data group
    /// 0. Its log progress since a snapshot is not tracked, and the
    /// membership it keeps on a plan's cut is read from its committed
    /// membership.
    pub fn meta_metrics_snapshot(&self) -> Option<RaftGroupMetricsSnapshot> {
        let meta = self.meta()?;
        let committed = openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::clone(
            &meta
                .core
                .owner()
                .metrics()
                .borrow_watched()
                .committed_membership_config,
        );
        Some(meta.core.metrics_snapshot(
            crate::meta::state_machine::META_GROUP.0,
            &self.election,
            crate::snapshot_cadence::GroupLogProgress::default(),
            committed,
        ))
    }

    pub(crate) fn entry(&self, group: RaftGroupId) -> Result<Arc<ActiveGroup>, GroupEngineError> {
        let groups = self.groups.load();
        let entry = groups.get(&group.0).ok_or(GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RaftGroupNotRegistered {
                group: RaftTarget::Data(group),
            },
        ))?;
        match entry.as_ref() {
            GroupEntry::Active(active) => {
                active.core.apply_health.check(RaftTarget::Data(group))?;
                Ok(active.clone())
            }
            GroupEntry::ApplyStopped { stopped, .. } => Err(stopped.error(RaftTarget::Data(group))),
        }
    }

    pub async fn append_entries(
        &self,
        raft_group_id: RaftGroupId,
        request: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let entry = self.entry(raft_group_id)?;
        entry
            .core
            .append_entries(RaftTarget::Data(raft_group_id), request)
            .await
    }

    pub async fn vote(
        &self,
        raft_group_id: RaftGroupId,
        request: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let entry = self.entry(raft_group_id)?;
        entry
            .core
            .vote(RaftTarget::Data(raft_group_id), request)
            .await
    }

    pub async fn install_full_snapshot(
        &self,
        raft_group_id: RaftGroupId,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: UrsulaSnapshot,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, SnapshotInstallError> {
        // A refused install must not queue for the group's lock or the
        // node's permit. The floor only rises and the Raft core's vote never
        // falls below it, so Raft refuses a vote the floor overtakes while
        // this request waits.
        let entry = self.entry(raft_group_id)?;
        if let Some(floor) = entry
            .core
            .check_recovery_vote(RaftTarget::Data(raft_group_id), vote)?
        {
            return Ok(SnapshotResponse::new(floor));
        }
        if let Some(refusal) = entry
            .core
            .recovery
            .as_ref()
            .and_then(|rejoin| rejoin.refused_snapshot(snapshot.meta.last_log_id))
        {
            return Err(SnapshotInstallError::Group(GroupEngineError::Infra(
                refusal,
            )));
        }
        // Wait in the RPC future, so canceled queued requests retain neither
        // an owned task nor shutdown-drain admission. Admit the captured engine
        // only after both resources are available; shutdown rejects stale waiters.
        let lock = self.snapshot_install.install_lock(raft_group_id.0);
        let serial = lock.lock_owned().await;
        let permit = self.snapshot_install.acquire(raft_group_id).await?;
        let raft = entry.raft().clone();
        let core = entry.core.clone();
        let snapshots = entry.snapshots.clone();
        let lifetime =
            entry
                .snapshot_installs
                .admit()
                .ok_or(SnapshotInstallError::ShuttingDown {
                    group: raft_group_id,
                })?;
        // The admitted task retains prefetch and permits through Raft consumption,
        // even when the RPC waiter disconnects or is canceled.
        crate::rt::spawn(async move {
            let _serial = serial;
            let _permit = permit;
            // A download of an engine that is shutting down stops: the
            // shutdown waits for this task (#709).
            let prepared = lifetime
                .until_stopping(async {
                    let current = raft
                        .get_snapshot()
                        .await
                        .map_err(crate::owner::owner_stopped)?;
                    // Only the meta compares: this node's pointer names its
                    // own copy, never the sender's object (#687).
                    let already_current = current
                        .as_ref()
                        .is_some_and(|current| current.meta == snapshot.meta);
                    drop(current);
                    if already_current {
                        Ok::<_, SnapshotInstallError>(PrefetchedInstallSnapshot {
                            snapshot,
                            guard: None,
                            history: None,
                        })
                    } else {
                        snapshots.prefetch(raft_group_id, snapshot).await
                    }
                })
                .await;
            let Some(prefetched) = prepared else {
                return Err(SnapshotInstallError::ShuttingDown {
                    group: raft_group_id,
                });
            };
            let prefetched = prefetched?;
            let _prefetch_guard = prefetched.guard;
            let snapshot = prefetched.snapshot;
            let incoming_history = prefetched.history;
            let result = raft
                .call(move |raft| async move {
                    core.screen_install(vote, snapshot.meta.last_log_id, incoming_history)
                        .await?;
                    Ok(raft.install_full_snapshot(vote, snapshot).await)
                })
                .await
                .map_err(crate::owner::owner_stopped)
                .and_then(|installed| {
                    installed
                        .map_err(GroupEngineError::Infra)?
                        .map_err(crate::owner::owner_stopped)
                })
                .map_err(SnapshotInstallError::Group);
            // A copy OpenRaft did not commit leaves the holds with its lease
            // here, and its group owes the pass that deletes it.
            drop(_prefetch_guard);
            drop(_permit);
            drop(lifetime);
            result
        })
        .await
        .map_err(SnapshotInstallError::Task)?
    }

    /// Hands a leadership transfer this node received from a leader to
    /// OpenRaft once the election policy accepts it.
    pub async fn handle_transfer_leader(
        &self,
        raft_group_id: RaftGroupId,
        request: TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> Result<openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig>, IncomingTransferError>
    {
        let entry = self.entry(raft_group_id)?;
        entry
            .core
            .deliver_transfer(&self.election, raft_group_id, request)
            .await
    }

    /// Starts an election on this node's replica of `group` while it is in
    /// `current_term`: a leadership transfer to itself, held to the policy
    /// of every transfer it receives. A replica that already leads, or may
    /// not campaign, is refused. OpenRaft acts on a transfer only while the
    /// replica holds the vote it carries, so the transfer carries the vote
    /// the replica observes. Success means OpenRaft took the transfer, not
    /// that the replica won the election.
    ///
    /// A replica whose Raft core stopped is refused first
    /// ([`IncomingTransferError::Raft`]): its metrics keep the last vote, so
    /// one that led when it stopped would otherwise read as the leader. A
    /// core that panicked never reports that it stopped (its metrics keep
    /// their last values), so it is refused as what they show.
    pub async fn request_self_election(
        &self,
        group: RaftGroupId,
        current_term: u64,
    ) -> Result<(), SelfElectionError> {
        let entry = self.entry(group).map_err(IncomingTransferError::Group)?;
        self_election(&self.election, group, &entry.core, current_term).await
    }

    /// [`Self::request_self_election`] on this node's meta replica, refused
    /// as group 0 when the node runs none.
    pub async fn request_meta_self_election(
        &self,
        current_term: u64,
    ) -> Result<(), SelfElectionError> {
        let group = crate::meta::state_machine::META_GROUP;
        let meta = self
            .meta()
            .ok_or(IncomingTransferError::Group(GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RaftGroupNotRegistered {
                    group: RaftTarget::Meta,
                },
            )))?;
        meta.core
            .apply_health
            .check(RaftTarget::Meta)
            .map_err(IncomingTransferError::Group)?;
        self_election(&self.election, group, &meta.core, current_term).await
    }

    /// Builds a snapshot outside OpenRaft's own build path, which OpenRaft
    /// never publishes: tests only, since the group's next purge waits for
    /// OpenRaft to publish every build (#669).
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn build_snapshot_for_transfer(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<UrsulaSnapshot, SnapshotBuildError> {
        let raft = self.require_group(raft_group_id)?;
        let snapshot = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    let mut builder = state_machine.get_snapshot_builder().await;
                    builder.build_snapshot().await
                })
            })
            .await
            .map_err(crate::owner::owner_stopped)?
            .map_err(SnapshotBuildError::Build)?;
        Ok(snapshot)
    }

    #[cfg(any(test, feature = "fault-injection"))]
    fn require_group(&self, group: RaftGroupId) -> Result<OwnerRaftHandle, GroupEngineError> {
        Ok(self.entry(group)?.raft().clone())
    }
}

impl GroupSnapshots {
    /// A GC pass on these holds, run as work of the incarnation they belong
    /// to: `admission` is that engine's, so the pass runs only while the
    /// engine runs, its shutdown stops the pass and waits for it, and the
    /// group reopens only after that (#709). `None` when the engine is
    /// stopping or stopped. Without the fence, a pass that read an old
    /// incarnation's holds could list a new incarnation's objects and
    /// delete them (#687).
    pub(crate) async fn fenced_pass(
        &self,
        admission: &crate::openraft_work::WorkAdmission,
        now: crate::rt::time::Instant,
    ) -> Option<Result<crate::snapshot_holds::SnapshotPass, crate::snapshot_holds::SnapshotGcError>>
    {
        let lifetime = admission.admit()?;
        lifetime
            .until_stopping(self.holds.collect_garbage(&self.store, now))
            .await
    }

    /// Reads the snapshot the leader's pointer names, decodes it, and writes
    /// this node's own copy of it, outside OpenRaft's state-machine worker,
    /// where a failed write is fatal to RaftCore. The pointer handed to
    /// OpenRaft names the copy, and the install commits only the copy: a
    /// node never names another node's object durably (#687). The sender
    /// holds its object until this RPC returns, so a read that finds it
    /// missing is a send that already ended or a pointer that is no longer
    /// current: the answer is `UNAVAILABLE`, and the sender's next attempt
    /// carries its current pointer.
    pub(crate) async fn prefetch(
        &self,
        raft_group_id: RaftGroupId,
        snapshot: UrsulaSnapshot,
    ) -> Result<PrefetchedInstallSnapshot, SnapshotInstallError> {
        let group = raft_group_id;
        // Kept until the prefetch returns.
        let (pointer_bytes, _incoming_lease) = snapshot.snapshot.into_parts();
        let pointer = SnapshotPointer::decode(&pointer_bytes)
            .map_err(|source| SnapshotInstallError::PointerUndecodable { group, source })?;
        let SnapshotPointer {
            snapshot_id,
            location,
        } = pointer;
        let source_key = match &location {
            SnapshotLocation::S3 { key, .. } => key.clone(),
            SnapshotLocation::Local { path, .. } => path.display().to_string(),
            SnapshotLocation::Inline { bytes } => {
                let history = decode_group_snapshot(bytes)
                    .map_err(|source| SnapshotInstallError::Undecodable {
                        group,
                        snapshot_id: snapshot_id.clone(),
                        source,
                    })?
                    .stream_snapshot
                    .history;
                return Ok(PrefetchedInstallSnapshot {
                    snapshot: UrsulaSnapshot {
                        meta: snapshot.meta,
                        snapshot: SnapshotData::new(pointer_bytes),
                    },
                    guard: None,
                    history: Some(history),
                });
            }
        };

        let source_read = |source| SnapshotInstallError::SourceRead {
            group,
            source_key: source_key.clone(),
            source,
        };
        let stored = self
            .store
            .read_stored(&location)
            .await
            .map_err(|source| match source {
                ursula_runtime::SnapshotStoreError::NotFound { key } => {
                    SnapshotInstallError::SourceMissing {
                        group,
                        source_key: key,
                    }
                }
                source => source_read(source),
            })?;
        let body = stored.body().map_err(source_read)?;
        let group_snapshot =
            decode_group_snapshot(&body).map_err(|source| SnapshotInstallError::Undecodable {
                group,
                snapshot_id: snapshot_id.clone(),
                source,
            })?;
        // Only the stored bytes are copied: the decompressed body goes now.
        drop(body);
        let history = group_snapshot.stream_snapshot.history;
        let (owned, lease) = match uncopied(group, &snapshot.meta, &location) {
            Some(owned) => (owned, None),
            None => self.copy(group, &snapshot.meta, stored).await?,
        };
        let mut handed = owned.pointer().clone();
        let cache_key = self
            .install
            .cache_prefetched(&mut handed, group_snapshot, owned, lease);
        let guard = PrefetchedSnapshotGuard {
            snapshot_install: self.install.clone(),
            cache_key,
        };
        let pointer_bytes = handed
            .encode_binary()
            .map_err(|source| SnapshotInstallError::PointerEncode { group, source })?;
        Ok(PrefetchedInstallSnapshot {
            snapshot: UrsulaSnapshot {
                meta: snapshot.meta,
                snapshot: SnapshotData::new(pointer_bytes),
            },
            guard: Some(guard),
            history: Some(history),
        })
    }

    /// Writes `stored`, the snapshot `meta` names as its sender's store
    /// holds it, as this node's own object under a fresh name, through the
    /// one write policy of every object a node writes
    /// ([`SnapshotHolds::write`]): held from before its PUT and checked with
    /// a HEAD (#687). The stored bytes go as they are, without decompressing
    /// and compressing them again. A name collision on the node's own fresh
    /// name is likelier its own write retried after a lost answer than
    /// another write's: the name is not held, the write owes the passes that
    /// delete what may have landed, and the copy runs once more under a new
    /// name. The pointer it answers carries the snapshot's own id
    /// ([`group_snapshot_id`]).
    async fn copy(
        &self,
        group: RaftGroupId,
        meta: &openraft::alias::SnapshotMetaOf<UrsulaRaftTypeConfig>,
        stored: ursula_runtime::StoredSnapshot,
    ) -> Result<(OwnedPointer, Option<SnapshotLease>), SnapshotInstallError> {
        let index = meta.last_log_id.map_or(0, |log_id| log_id.index());
        let snapshot_id = group_snapshot_id(group, meta.last_log_id);
        let mut collided = None;
        for _attempt in 0..2 {
            let staged = self
                .store
                .stage_stored(group, index, stored.clone())
                .await
                .map_err(|source| SnapshotInstallError::CopyWrite {
                    group,
                    own_key: self.store.namespace(group).directory().unwrap_or_default(),
                    source,
                })?;
            match self.holds.write(&self.store, staged).await {
                Ok(written) => {
                    return Ok((
                        OwnedPointer::new(snapshot_id, written.location),
                        written.lease,
                    ));
                }
                Err(HeldWriteError::Write {
                    key,
                    source: ursula_runtime::SnapshotStoreError::NameCollision { .. },
                }) => {
                    tracing::warn!(
                        raft_group_id = group.0,
                        key,
                        "the snapshot copy's fresh name is taken, likely by its own retried \
                         write; copying under a new name"
                    );
                    collided = Some(key);
                }
                Err(HeldWriteError::Write { key, source }) => {
                    return Err(SnapshotInstallError::CopyWrite {
                        group,
                        own_key: key,
                        source,
                    });
                }
                Err(HeldWriteError::Verify {
                    key,
                    source:
                        ursula_runtime::SnapshotStoreError::SizeMismatch {
                            expected, actual, ..
                        },
                }) => {
                    return Err(SnapshotInstallError::CopyVerify {
                        group,
                        own_key: key,
                        expected,
                        actual,
                    });
                }
                Err(HeldWriteError::Verify { key, source }) => {
                    return Err(SnapshotInstallError::CopyWrite {
                        group,
                        own_key: key,
                        source,
                    });
                }
            }
        }
        Err(SnapshotInstallError::NameCollision {
            group,
            own_key: collided.unwrap_or_default(),
        })
    }
}

/// The negative control of the install copy (`Defence::InstallCopy`): the
/// sender's pointer, uncopied, as the one the install commits. Only a
/// simulation can build such a pointer, so every other build copies.
#[cfg(madsim)]
fn uncopied(
    group: RaftGroupId,
    meta: &openraft::alias::SnapshotMetaOf<UrsulaRaftTypeConfig>,
    location: &SnapshotLocation,
) -> Option<OwnedPointer> {
    if !ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::InstallCopy) {
        return None;
    }
    ursula_stream::sim_controls::note_fired(ursula_stream::sim_controls::Defence::InstallCopy);
    Some(OwnedPointer::unowned_for_simulation(SnapshotPointer {
        snapshot_id: group_snapshot_id(group, meta.last_log_id),
        location: location.clone(),
    }))
}

/// Every build outside a simulation copies the snapshot it installs.
#[cfg(not(madsim))]
fn uncopied(
    _group: RaftGroupId,
    _meta: &openraft::alias::SnapshotMetaOf<UrsulaRaftTypeConfig>,
    _location: &SnapshotLocation,
) -> Option<OwnedPointer> {
    None
}

/// The admin answer of a confirmed quorum barrier of `group`.
fn quorum_prefix(
    group: RaftGroupId,
    answer: &crate::BarrierAnswer,
) -> ursula_proto::admin::QuorumPrefix {
    ursula_proto::admin::QuorumPrefix {
        raft_group_id: group.0,
        leader_id: *answer.vote.leader_id().node_id(),
        leader_term: answer.vote.leader_id().term(),
        required_applied_index: answer.index,
    }
}

/// A self-election of `core`, a replica of `group`, while it is in
/// `current_term` ([`RaftGroupHandleRegistry::request_self_election`]).
async fn self_election<SM>(
    election: &ElectionPolicy,
    group: RaftGroupId,
    core: &ReplicaCore<SM>,
    current_term: u64,
) -> Result<(), SelfElectionError>
where
    SM: openraft::storage::RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>,
{
    let observed = core.owner().metrics().borrow_watched().clone();
    if let Err(source) = observed.running_state {
        return Err(IncomingTransferError::Raft { group, source }.into());
    }
    if observed.current_term != current_term {
        return Err(SelfElectionError::TermChanged {
            group,
            requested: current_term,
            observed: observed.current_term,
        });
    }
    let request = TransferLeaderRequest::new(observed.vote, observed.id, None);
    core.deliver_transfer(election, group, request)
        .await?
        .map_err(|source| SelfElectionError::Rejected { group, source })
}

/// Refuses a recovery plan for `group` whose floor is of the other plane
/// ([`WireTarget::takes_floor`]), before anything runs.
fn check_plan_floor(
    group: RaftTarget,
    plan: &RecoveryPlanRequest,
) -> Result<(), RecoveryGateError> {
    if group.takes_floor(plan.floor) {
        return Ok(());
    }
    Err(RecoveryGateError::FloorOtherPlane {
        group,
        floor: plan.floor,
    })
}

pub(crate) fn log_progress_snapshot(
    log_id: LogIdOf<UrsulaRaftTypeConfig>,
) -> RaftLogProgressSnapshot {
    RaftLogProgressSnapshot {
        term: log_id.leader_id.term,
        index: log_id.index,
        node_id: log_id.leader_id.node_id,
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(madsim))]
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    #[cfg(not(madsim))]
    use std::time::Duration;

    use ursula_runtime::GroupSnapshot;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;
    use ursula_runtime::StagedSnapshot;
    use ursula_runtime::StoredSnapshot;
    use ursula_runtime::snapshot_harness::MemorySnapshotBucket;
    use ursula_runtime::snapshot_harness::MemorySnapshotStore;
    #[cfg(not(madsim))]
    use ursula_runtime::snapshot_harness::NodeNames;
    use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
    #[cfg(not(madsim))]
    use ursula_runtime::snapshot_harness::check_quiet_prefixes;

    use super::*;

    /// A real engine publishes its shared resources in one step.
    #[cfg_attr(madsim, madsim::test)]
    #[cfg_attr(not(madsim), tokio::test)]
    async fn registry_hands_out_the_registered_group_page_cache() {
        let check = async {
            let (registry, engine, _root) = install_engine(InstallStore::new()).await;
            let group = engine.placement.raft_group_id;
            let shared = registry.cold_index_cache(group).expect("registered cache");
            assert!(Arc::ptr_eq(
                &shared,
                engine.cold_index_cache.as_ref().unwrap()
            ));
            assert!(Arc::ptr_eq(
                &registry.entry(group).unwrap().core.barrier,
                &engine.read_barrier
            ));
            assert!(Arc::ptr_eq(
                &registry.entry(group).unwrap().snapshots.holds,
                &engine.snapshots.holds
            ));
            assert_eq!(registry.len(), 1);
            assert!(registry.get(group).is_some());
            engine.shutdown().await.unwrap();
        };
        #[cfg(madsim)]
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, check).await;
        #[cfg(not(madsim))]
        check.await;
    }

    /// Node 1's memory store, which also serves the objects of the leader:
    /// a GET of a key outside node 1's directory answers the test group's
    /// snapshot body, or `NotFound` once the source vanished. Its object
    /// writes can fail, and its GETs can pause.
    #[derive(Debug)]
    struct InstallStore {
        #[cfg_attr(
            madsim,
            expect(dead_code, reason = "only the tests outside madsim read the bucket")
        )]
        bucket: MemorySnapshotBucket,
        inner: MemorySnapshotStore,
        fail_write: AtomicBool,
        source_gone: AtomicBool,
        downloads: AtomicUsize,
        pause_download: AtomicBool,
        entered_download: crate::rt::sync::Notify,
        release_download: crate::rt::sync::Notify,
    }

    impl InstallStore {
        fn new() -> Arc<Self> {
            Self::with(SnapshotStoreFaults::none(), None)
        }

        fn with(faults: SnapshotStoreFaults, nonces: Option<fn() -> u64>) -> Arc<Self> {
            let bucket = MemorySnapshotBucket::new(
                7,
                ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
                faults,
            );
            let inner = match nonces {
                Some(nonces) => bucket.store(1).with_nonces(nonces),
                None => bucket.store(1),
            };
            Arc::new(Self {
                bucket,
                inner,
                fail_write: AtomicBool::new(false),
                source_gone: AtomicBool::new(false),
                downloads: AtomicUsize::new(0),
                pause_download: AtomicBool::new(false),
                entered_download: crate::rt::sync::Notify::new(),
                release_download: crate::rt::sync::Notify::new(),
            })
        }

        #[cfg(not(madsim))]
        /// The objects in node 1's directory.
        fn own_keys(&self) -> BTreeSet<String> {
            self.bucket
                .keys()
                .into_iter()
                .filter(|key| key.starts_with("snapshots/node-1/"))
                .collect()
        }
    }

    impl SnapshotStore for InstallStore {
        fn namespace(&self, raft_group_id: RaftGroupId) -> ursula_runtime::OwnedNamespace {
            self.inner.namespace(raft_group_id)
        }

        fn stage<'a>(
            &'a self,
            raft_group_id: RaftGroupId,
            index: u64,
            chunks: ursula_runtime::SnapshotBytesIterator,
        ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
            self.inner.stage(raft_group_id, index, chunks)
        }

        fn write_staged<'a>(
            &'a self,
            staged: StagedSnapshot,
        ) -> SnapshotStoreFuture<'a, ursula_runtime::OwnedLocation> {
            if self.fail_write.load(Ordering::SeqCst) {
                return Box::pin(async {
                    Err(SnapshotStoreError::backend(
                        ursula_runtime::SnapshotStoreOperation::Put,
                        "test",
                        opendal::Error::new(opendal::ErrorKind::Unexpected, "PUT outage"),
                    ))
                });
            }
            self.inner.write_staged(staged)
        }

        fn list_own<'a>(
            &'a self,
            raft_group_id: RaftGroupId,
        ) -> SnapshotStoreFuture<'a, ursula_runtime::OwnListing> {
            self.inner.list_own(raft_group_id)
        }

        fn delete_own<'a>(
            &'a self,
            object: ursula_runtime::OwnedObject,
        ) -> SnapshotStoreFuture<'a, ()> {
            self.inner.delete_own(object)
        }

        fn read_stored<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ursula_runtime::StoredSnapshot> {
            Box::pin(async move {
                self.downloads.fetch_add(1, Ordering::SeqCst);
                if self.pause_download.load(Ordering::SeqCst) {
                    self.entered_download.notify_one();
                    self.release_download.notified().await;
                }
                let key = crate::state_machine::object_key(location).expect("an object");
                if key.starts_with("snapshots/node-1/") {
                    return self.inner.read_stored(location).await;
                }
                if self.source_gone.load(Ordering::SeqCst) {
                    return Err(SnapshotStoreError::NotFound { key });
                }
                Ok(StoredSnapshot::inline(group_snapshot_bytes()))
            })
        }

        fn verify_uploaded<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            self.inner.verify_uploaded(location)
        }
    }

    struct ReferenceWalRoot {
        #[cfg(not(madsim))]
        root: tempfile::TempDir,
        #[cfg(madsim)]
        root: std::path::PathBuf,
    }
    impl ReferenceWalRoot {
        fn new() -> Self {
            Self {
                #[cfg(not(madsim))]
                root: tempfile::tempdir().unwrap(),
                #[cfg(madsim)]
                root: crate::log_store::SimDisk::provision_dir("snapshot-install").unwrap(),
            }
        }
        fn path(&self) -> &std::path::Path {
            #[cfg(not(madsim))]
            {
                self.root.path()
            }
            #[cfg(madsim)]
            {
                &self.root
            }
        }
    }

    #[cfg(not(madsim))]
    async fn install_group(
        store: Arc<InstallStore>,
    ) -> (RaftGroupHandleRegistry, RaftGroupHandle, ReferenceWalRoot) {
        let (registry, engine, root) = install_engine(store).await;
        (registry, engine.raft_handle(), root)
    }

    async fn install_engine(
        store: Arc<InstallStore>,
    ) -> (
        RaftGroupHandleRegistry,
        crate::RaftGroupEngine,
        ReferenceWalRoot,
    ) {
        install_engine_for_group(store, RaftGroupId(7), Default::default(), None).await
    }

    #[cfg(not(madsim))]
    /// [`install_engine_for_group`] for group 7, started with the recovery
    /// gate `recovery`, as a node's engine factory starts it.
    async fn gated_install_engine(
        store: Arc<InstallStore>,
        recovery: Arc<crate::GroupRejoin>,
    ) -> (
        RaftGroupHandleRegistry,
        crate::RaftGroupEngine,
        ReferenceWalRoot,
    ) {
        install_engine_for_group(store, RaftGroupId(7), Default::default(), Some(recovery)).await
    }

    async fn install_engine_for_group(
        store: Arc<InstallStore>,
        group: RaftGroupId,
        snapshot_install: SnapshotInstallCoordinator,
        recovery: Option<Arc<crate::GroupRejoin>>,
    ) -> (
        RaftGroupHandleRegistry,
        crate::RaftGroupEngine,
        ReferenceWalRoot,
    ) {
        let wal_root = ReferenceWalRoot::new();
        let registry = RaftGroupHandleRegistry {
            snapshot_install,
            ..Default::default()
        };
        registry.set_snapshot_store(Some(store.clone()));
        let placement = ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: group,
        };
        let config = Arc::new(
            openraft::Config {
                enable_tick: false,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let engine = crate::RaftGroupEngine::new_node(
            placement,
            1,
            config,
            SingleNodeRaftNetworkFactory,
            crate::log_store::RaftWal::start(
                wal_root.path(),
                ursula_config::WalFsync::Never,
                &ursula_shard::StaticShardMap::new(1, 8).expect("valid topology"),
            )
            .unwrap()
            .open(
                placement,
                ursula_control::Admission::Create,
                ursula_runtime::RuntimeMetrics::new(1, 8).group_engine_metrics(),
            )
            .unwrap(),
            crate::RaftGroupEngineOptions {
                snapshot_store: Some(store),
                snapshot_install: Some(registry.snapshot_install_coordinator()),
                cold_store: Some(Arc::new(ursula_runtime::ColdStore::memory().unwrap())),
                recovery,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        registry.register_engine(&engine);
        (registry, engine, wal_root)
    }

    #[cfg(not(madsim))]
    fn install_snapshot() -> UrsulaSnapshot {
        let mut snapshot = external_snapshot("leader-object");
        let id = openraft::LogId::new(openraft::vote::RaftLeaderId::new(1, 2), 1);
        snapshot.meta.last_log_id = Some(id);
        snapshot.meta.last_membership =
            openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                Some(id),
                openraft::Membership::new(
                    vec![BTreeSet::from([1])],
                    BTreeMap::from([(1, openraft::EmptyNode {})]),
                )
                .unwrap(),
            );
        snapshot
    }

    #[cfg(not(madsim))]
    /// The pointer of `raft`'s current snapshot.
    async fn current_pointer(raft: &RaftGroupHandle) -> SnapshotPointer {
        let current = raft
            .get_snapshot()
            .await
            .unwrap()
            .expect("a current snapshot");
        SnapshotPointer::decode(current.snapshot.pointer_bytes()).unwrap()
    }

    /// M1 of the third #581 review: a replica holding a recovery plan takes
    /// no full snapshot and no append from a leader at or below the plan
    /// term, a leader elected before the plan, and refuses it typed. A
    /// leader above the plan term is not fenced.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_replica_holding_a_plan_takes_no_snapshot_from_a_leader_before_it() {
        let store = InstallStore::new();
        let gate_root = tempfile::tempdir().expect("gate WAL root");
        let gate_store = crate::log_store::RaftWal::start(
            gate_root.path(),
            ursula_config::WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 8).expect("valid topology"),
        )
        .unwrap()
        .open(
            ShardPlacement {
                core_id: ursula_shard::CoreId(0),
                shard_id: ursula_shard::ShardId(0),
                raft_group_id: RaftGroupId(7),
            },
            ursula_control::Admission::Create,
            ursula_runtime::RuntimeMetrics::new(1, 8).group_engine_metrics(),
        )
        .unwrap();
        let gate = Arc::new(
            crate::GroupRejoin::durable(
                1,
                ursula_shard::RaftTarget::Data(RaftGroupId(7)),
                &gate_store,
            )
            .await
            .expect("open the gate"),
        );
        let (registry, engine, _root) = gated_install_engine(store, gate.clone()).await;
        engine.publish_recovery(gate.clone(), &registry).unwrap();
        gate.hold_plan(&crate::log_store::RecoveryRecord {
            leader: 2,
            point: 1,
            leader_term: 7,
            cut: crate::log_store::PlanCut::NoCut,
        });
        let fenced = |error: &GroupEngineError| {
            matches!(
                error,
                GroupEngineError::Infra(ursula_runtime::GroupInfraError::RecoveryPlanFenced {
                    group: RaftTarget::Data(RaftGroupId(7)),
                    plan_term: 7,
                })
            )
        };
        for term in [5, 7] {
            let installed = registry
                .install_full_snapshot(
                    RaftGroupId(7),
                    crate::UrsulaVote::new_committed(term, 2),
                    install_snapshot(),
                )
                .await;
            assert!(
                matches!(&installed, Err(SnapshotInstallError::Group(error)) if fenced(error)),
                "term {term}: {installed:?}"
            );
            let appended = registry
                .append_entries(RaftGroupId(7), openraft::raft::AppendEntriesRequest {
                    vote: crate::UrsulaVote::new_committed(term, 2),
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                })
                .await;
            assert!(
                matches!(&appended, Err(error) if fenced(error)),
                "term {term}: {appended:?}"
            );
        }
        let above = registry
            .append_entries(RaftGroupId(7), openraft::raft::AppendEntriesRequest {
                vote: crate::UrsulaVote::new_committed(8, 2),
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
            })
            .await;
        assert!(!matches!(&above, Err(error) if fenced(error)), "{above:?}");
        engine.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn queued_snapshot_cannot_switch_to_a_replacement_engine() {
        let store = InstallStore::new();
        let (registry, old, _old_root) = install_engine(store.clone()).await;
        let serial = registry.snapshot_install.install_lock(7).lock_owned().await;
        let request = registry.install_full_snapshot(
            RaftGroupId(7),
            crate::UrsulaVote::new_committed(1, 2),
            install_snapshot(),
        );
        tokio::pin!(request);
        assert!(futures_util::poll!(&mut request).is_pending());
        old.shutdown().await.unwrap();
        let (_, replacement, _new_root) = install_engine_for_group(
            store,
            RaftGroupId(7),
            registry.snapshot_install_coordinator(),
            None,
        )
        .await;
        registry.register_engine(&replacement);
        drop(serial);
        assert!(matches!(
            request.await,
            Err(SnapshotInstallError::ShuttingDown {
                group: RaftGroupId(7)
            })
        ));
        assert!(
            replacement
                .raft_handle()
                .get_snapshot()
                .await
                .unwrap()
                .is_none()
        );
        replacement.shutdown().await.unwrap();
    }

    /// #687: an install copies the leader's snapshot into this node's own
    /// object and commits the copy. A copy whose write fails refuses the
    /// install before OpenRaft takes it, answering `UNAVAILABLE`, and keeps
    /// the Raft core running; the next attempt copies again and installs.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn an_install_commits_its_own_copy_and_a_failed_copy_retries_without_restarting_raft() {
        let store = InstallStore::new();
        store.fail_write.store(true, Ordering::SeqCst);
        let (registry, raft, _wal_root) = install_group(store.clone()).await;
        let first = registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                install_snapshot(),
            )
            .await;
        let error = first.expect_err("a failed copy refuses the install");
        assert!(
            matches!(&error, SnapshotInstallError::CopyWrite { group, .. } if *group == RaftGroupId(7)),
            "{error:?}"
        );
        assert!(error.peer_backs_off());
        let metrics = raft.metrics().borrow_watched().clone();
        assert!(
            metrics.running_state.is_ok(),
            "a failed copy must not kill RaftCore"
        );
        assert!(
            metrics.last_applied.is_none(),
            "the copy precedes the install"
        );
        assert!(store.own_keys().is_empty());

        store.fail_write.store(false, Ordering::SeqCst);
        registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                install_snapshot(),
            )
            .await
            .unwrap();
        assert_eq!(store.downloads.load(Ordering::SeqCst), 2);
        assert_eq!(
            raft.metrics()
                .borrow_watched()
                .last_applied
                .unwrap()
                .index(),
            1
        );
        let pointer = current_pointer(&raft).await;
        assert_eq!(
            pointer.snapshot_id,
            group_snapshot_id(RaftGroupId(7), install_snapshot().meta.last_log_id)
        );
        let SnapshotLocation::S3 { key, .. } = &pointer.location else {
            panic!("expected the node's own copy, got {:?}", pointer.location);
        };
        assert!(
            key.starts_with("snapshots/node-1/group-7/00000000000000000001-"),
            "{key}"
        );
        assert_eq!(store.own_keys(), BTreeSet::from([key.clone()]));
        // A retry of the same snapshot needs no copy: only its meta
        // compares, and the node's pointer names its own copy.
        registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                install_snapshot(),
            )
            .await
            .unwrap();
        assert_eq!(store.downloads.load(Ordering::SeqCst), 2);
        assert_eq!(store.own_keys(), BTreeSet::from([key.clone()]));
        // Same Raft handle resumes an election and committed application.
        raft.trigger().elect(false).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "recovered group elects")
            .await
            .unwrap();
        raft.client_write(crate::tests::genesis_command())
            .await
            .unwrap();
        let response = raft
            .client_write(crate::types::RaftCommand::Stream(
                ursula_stream::StreamCommand::CreateStream {
                    stream_id: ursula_shard::BucketStreamId::new("after-retry", "events"),
                    content_type: "application/octet-stream".to_owned(),
                    initial_payload: bytes::Bytes::new(),
                    close_after: false,
                    stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    now_ms: 0,
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            response.data,
            crate::RaftGroupResponse::Write(Ok(ursula_runtime::GroupWriteResponse::CreateStream(
                _
            )))
        ));
        raft.shutdown().await.unwrap();
    }

    /// #687: the sender holds its object until the RPC returns, so a source
    /// that is gone is a pointer that is no longer current. The install
    /// answers `UNAVAILABLE`, installs nothing, and the next attempt, which
    /// carries the sender's current pointer, installs.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_vanished_source_answers_unavailable_and_the_next_pointer_installs() {
        let store = InstallStore::new();
        store.source_gone.store(true, Ordering::SeqCst);
        let (registry, raft, _wal_root) = install_group(store.clone()).await;
        let error = registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                install_snapshot(),
            )
            .await
            .expect_err("a vanished source refuses the install");
        assert!(
            matches!(
                &error,
                SnapshotInstallError::SourceMissing { group, source_key }
                    if *group == RaftGroupId(7) && source_key == "leader-object.snap"
            ),
            "{error:?}"
        );
        assert_eq!(tonic::Status::from(error).code(), tonic::Code::Unavailable);
        assert!(raft.metrics().borrow_watched().last_applied.is_none());
        assert!(store.own_keys().is_empty(), "nothing was copied");

        store.source_gone.store(false, Ordering::SeqCst);
        let mut current = install_snapshot();
        current.snapshot = external_snapshot("leader-current").snapshot;
        registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                current,
            )
            .await
            .unwrap();
        assert_eq!(
            raft.metrics()
                .borrow_watched()
                .last_applied
                .unwrap()
                .index(),
            1
        );
        raft.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn shutdown_stops_a_canceled_install_before_the_reopened_group_installs() {
        let store = InstallStore::new();
        store.pause_download.store(true, Ordering::SeqCst);
        let (registry, engine, _root) = install_engine(store.clone()).await;
        let request = crate::rt::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .install_full_snapshot(
                        RaftGroupId(7),
                        crate::types::UrsulaVote::new_committed(1, 2),
                        install_snapshot(),
                    )
                    .await
            }
        });
        store.entered_download.notified().await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        let raft = engine.raft_handle();
        let shutdown = crate::rt::spawn(async move { engine.shutdown().await });
        raft.wait(Some(Duration::from_secs(2)))
            .metrics(
                |metrics| matches!(metrics.state, openraft::ServerState::Shutdown),
                "Raft stops before the admitted install's download ends",
            )
            .await
            .unwrap();
        let refused = crate::rt::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .install_full_snapshot(
                        RaftGroupId(7),
                        crate::types::UrsulaVote::new_committed(1, 2),
                        install_snapshot(),
                    )
                    .await
            }
        });
        // The shutdown stops the admitted install's paused download instead
        // of waiting for it, and returns once the task ended (#709).
        shutdown.await.unwrap().unwrap();
        assert!(matches!(
            refused.await.unwrap().unwrap_err(),
            SnapshotInstallError::ShuttingDown { .. }
        ));
        assert!(
            store.own_keys().is_empty(),
            "the stopped install copied nothing"
        );
        store.pause_download.store(false, Ordering::SeqCst);
        store.release_download.notify_one();
        // Reopen against the same store. No canceled task from the retired
        // engine may install behind this.
        let (replacement_registry, replacement, _replacement_root) =
            install_engine(store.clone()).await;
        let mut snapshot = install_snapshot();
        snapshot.snapshot = external_snapshot("replacement").snapshot;
        replacement_registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                snapshot,
            )
            .await
            .unwrap();
        let pointer = current_pointer(&replacement.raft_handle()).await;
        let key = crate::state_machine::object_key(&pointer.location).unwrap();
        assert_eq!(store.own_keys(), BTreeSet::from([key]));
        replacement.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    /// The snapshot side of an engine of group 7 on `store`, as the
    /// registry copies with it, whose holds are restored.
    fn group_snapshots(store: Arc<InstallStore>) -> GroupSnapshots {
        let install = SnapshotInstallCoordinator::default();
        let holds = Arc::new(SnapshotHolds::new(RaftGroupId(7), install.late_write()));
        holds.mark_restored();
        GroupSnapshots {
            store,
            install,
            holds,
        }
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn duplicate_prefetch_guards_have_independent_entries() {
        let snapshots = group_snapshots(InstallStore::new());
        let first = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("same"))
            .await
            .unwrap();
        let second = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("same"))
            .await
            .unwrap();
        let first_pointer =
            SnapshotPointer::decode(first.snapshot.snapshot.pointer_bytes()).unwrap();
        let second_pointer =
            SnapshotPointer::decode(second.snapshot.snapshot.pointer_bytes()).unwrap();
        assert_ne!(first_pointer.snapshot_id, second_pointer.snapshot_id);
        assert_ne!(
            first_pointer.location, second_pointer.location,
            "each copy is an object of its own"
        );
        drop(first);
        assert!(snapshots.install.take_prefetched(&second_pointer).is_some());
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn cancelled_snapshot_rpc_keeps_install_alive_and_serializes_duplicate() {
        let store = InstallStore::new();
        store.pause_download.store(true, Ordering::SeqCst);
        let (registry, raft, _wal_root) = install_group(store.clone()).await;
        let request = crate::rt::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .install_full_snapshot(
                        RaftGroupId(7),
                        crate::types::UrsulaVote::new_committed(1, 2),
                        install_snapshot(),
                    )
                    .await
            }
        });
        store.entered_download.notified().await;
        request.abort();
        let _cancelled = request.await;
        // A canceled waiter behind this admitted install must not turn into
        // another owned task or perform a later object-store download.
        let mut queued = Box::pin(registry.install_full_snapshot(
            RaftGroupId(7),
            crate::types::UrsulaVote::new_committed(1, 2),
            external_snapshot("canceled-queued-install"),
        ));
        assert!(matches!(
            futures_util::poll!(queued.as_mut()),
            std::task::Poll::Pending
        ));
        drop(queued);
        store.pause_download.store(false, Ordering::SeqCst);
        store.release_download.notify_one();
        registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                install_snapshot(),
            )
            .await
            .unwrap();
        assert_eq!(store.downloads.load(Ordering::SeqCst), 1);
        assert_eq!(
            raft.metrics()
                .borrow_watched()
                .last_applied
                .unwrap()
                .index(),
            1
        );
        raft.metrics()
            .borrow_watched()
            .running_state
            .as_ref()
            .unwrap();
        raft.shutdown().await.unwrap();
    }

    /// An install OpenRaft refuses, here for a vote below the replica's,
    /// commits nothing. Its copy leaves the holds when the install task
    /// drops it, and the group's next GC pass deletes it (#687).
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_refused_install_leaves_its_copy_to_the_next_pass() {
        let store = InstallStore::new();
        let (registry, engine, _wal_root) = install_engine(store.clone()).await;
        let raft = engine.raft_handle();
        // The restore's first pass, so that only what the install owes is
        // due now.
        registry
            .collect_snapshot_garbage(usize::MAX, Duration::from_secs(5))
            .await;
        raft.vote(crate::types::UrsulaVoteRequest::new(
            crate::types::UrsulaVote::new(5, 1),
            None,
        ))
        .await
        .unwrap();
        registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                install_snapshot(),
            )
            .await
            .unwrap();
        assert!(raft.metrics().borrow_watched().last_applied.is_none());
        assert!(raft.get_snapshot().await.unwrap().is_none());
        assert_eq!(store.own_keys().len(), 1, "the copy was written");
        assert!(engine.snapshots.holds.held_keys().is_empty());
        let pass = registry
            .collect_snapshot_garbage(usize::MAX, Duration::from_secs(5))
            .await;
        assert_eq!((pass.groups, pass.deleted, pass.failed), (1, 1, 0));
        assert!(store.own_keys().is_empty());
        engine.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    fn snapshot_meta() -> openraft::alias::SnapshotMetaOf<UrsulaRaftTypeConfig> {
        openraft::alias::SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: None,
            last_membership: openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::default(),
        }
    }

    fn group_snapshot_bytes() -> Vec<u8> {
        crate::snapshot_codec::group_snapshot_frames(Arc::new(GroupSnapshot {
            placement: ShardPlacement {
                core_id: ursula_shard::CoreId(0),
                shard_id: ursula_shard::ShardId(0),
                raft_group_id: ursula_shard::RaftGroupId(7),
            },
            group_commit_index: 0,
            stream_snapshot: Default::default(),
            stream_append_counts: Vec::new(),
        }))
        .collect::<Result<Vec<_>, _>>()
        .expect("encode test group snapshot")
        .into_iter()
        .flat_map(|chunk| chunk.to_vec())
        .collect()
    }

    #[cfg(not(madsim))]
    /// The leader's snapshot: a pointer to its object `{snapshot_id}.snap`.
    fn external_snapshot(snapshot_id: &str) -> UrsulaSnapshot {
        let pointer = SnapshotPointer {
            snapshot_id: snapshot_id.to_owned(),
            location: SnapshotLocation::S3 {
                key: format!("{snapshot_id}.snap"),
                size_bytes: 1,
                stored_size_bytes: 1,
                compression: ursula_runtime::SnapshotCompression::None,
            },
        };
        UrsulaSnapshot {
            meta: snapshot_meta(),
            snapshot: SnapshotData::new(pointer.encode_binary().expect("encode test pointer")),
        }
    }

    /// The prefetch writes the node's own copy, held until it is committed,
    /// and hands OpenRaft a pointer to the copy with a transient id; the
    /// cached pointer carries the snapshot's own id.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn prefetch_caches_the_nodes_own_copy() {
        let snapshots = group_snapshots(InstallStore::new());
        let prefetched = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("snapshot-a"))
            .await
            .expect("prefetch external snapshot");

        let pointer =
            SnapshotPointer::decode(prefetched.snapshot.snapshot.pointer_bytes()).unwrap();
        assert!(
            pointer.snapshot_id.starts_with("install-"),
            "each admitted handoff has its own transient token: {}",
            pointer.snapshot_id
        );
        let key = crate::state_machine::object_key(&pointer.location).unwrap();
        assert!(key.starts_with("snapshots/node-1/group-7/"), "{key}");
        assert_eq!(snapshots.holds.held_keys(), BTreeSet::from([key]));
        let cached = snapshots
            .install
            .take_prefetched(&pointer)
            .expect("external snapshot is cached for install");
        assert_eq!(cached.snapshot.group_commit_index, 0);
        assert_eq!(cached.pointer.pointer().snapshot_id, "group-7-empty");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn prefetch_snapshot_for_install_installs_without_snapshot_store_download() {
        let store = InstallStore::new();
        let snapshots = group_snapshots(store.clone());
        let prefetched = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("snapshot-drop"))
            .await
            .expect("prefetch external snapshot");
        assert_eq!(store.downloads.load(Ordering::SeqCst), 1);

        let placement = ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let mut state_machine = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            store.clone(),
            SnapshotBuildCoordinator::default(),
            snapshots.install.clone(),
            crate::state_machine::RecordSite::NoRecord,
        );
        let decodes_before_install = crate::snapshot_codec::decode_calls_on_this_thread();
        state_machine
            .install_snapshot(&prefetched.snapshot.meta, prefetched.snapshot.snapshot)
            .await
            .expect("prefetched external snapshot installs without external store");
        // F12c: the prefetch decoded the snapshot; install reuses it.
        assert_eq!(
            crate::snapshot_codec::decode_calls_on_this_thread(),
            decodes_before_install
        );
        assert_eq!(store.downloads.load(Ordering::SeqCst), 1);
        // The install committed the node's own copy.
        let key = state_machine.holds.current().expect("an object is current");
        assert!(key.starts_with("snapshots/node-1/group-7/"), "{key}");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn prefetch_snapshot_for_install_guard_clears_unconsumed_cache() {
        let snapshots = group_snapshots(InstallStore::new());
        let prefetched = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("snapshot-unconsumed"))
            .await
            .expect("prefetch external snapshot");
        let pointer =
            SnapshotPointer::decode(prefetched.snapshot.snapshot.pointer_bytes()).unwrap();
        drop(prefetched);

        assert!(
            snapshots.install.take_prefetched(&pointer).is_none(),
            "unconsumed prefetched snapshot should be cleared when guard drops"
        );
        assert!(
            snapshots.holds.held_keys().is_empty(),
            "the copy left the holds"
        );
        assert!(snapshots.holds.gc_due(crate::rt::time::Instant::now()));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn prefetch_snapshot_for_install_keeps_prefetch_failure_outside_openraft() {
        let store = InstallStore::new();
        store.source_gone.store(true, Ordering::SeqCst);
        let err = group_snapshots(store)
            .prefetch(RaftGroupId(7), external_snapshot("missing"))
            .await
            .expect_err("missing snapshot should fail before OpenRaft install");

        assert!(
            matches!(
                &err,
                SnapshotInstallError::SourceMissing { group, source_key }
                    if *group == RaftGroupId(7)
                        && source_key == "missing.snap"
            ),
            "{err:?}"
        );
        assert!(
            err.peer_backs_off(),
            "the sender retries with its current pointer"
        );
        assert_eq!(tonic::Status::from(err).code(), tonic::Code::Unavailable);
    }

    /// A copy whose fresh name is taken on the retry too is refused typed,
    /// answering `UNAVAILABLE`. A copy whose write lands and answers a name
    /// collision, as a create retried after a lost answer does, may be the
    /// node's own: it is not held, and the next pass deletes what landed, so
    /// the node's directory holds only what it names (I5).
    #[cfg(not(madsim))]
    #[tokio::test(start_paused = true)]
    async fn a_copy_name_collision_is_retried_once_and_leaves_nothing_held() {
        // Every stage draws the same name: the second copy collides twice.
        let store = InstallStore::with(SnapshotStoreFaults::none(), Some(|| 7));
        let snapshots = group_snapshots(store.clone());
        let first = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("first"))
            .await
            .unwrap();
        drop(first);
        let refused = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("first"))
            .await
            .expect_err("the copy's name is taken on the retry too");
        assert!(
            matches!(&refused, SnapshotInstallError::NameCollision { group, own_key }
                if *group == RaftGroupId(7) && own_key.starts_with("snapshots/node-1/group-7/")),
            "{refused:?}"
        );
        assert_eq!(
            tonic::Status::from(refused).code(),
            tonic::Code::Unavailable
        );

        let collided = InstallStore::with(
            SnapshotStoreFaults::none()
                .with_collided_retries(ursula_runtime::snapshot_harness::PerMille::ALWAYS),
            None,
        );
        let snapshots = group_snapshots(collided.clone());
        let refused = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("collided"))
            .await
            .expect_err("both writes answer a collision");
        assert!(
            matches!(refused, SnapshotInstallError::NameCollision { .. }),
            "{refused:?}"
        );
        assert_eq!(collided.own_keys().len(), 2, "both writes landed");
        assert!(snapshots.holds.held_keys().is_empty());
        snapshots
            .holds
            .collect_garbage(&snapshots.store, crate::rt::time::Instant::now())
            .await
            .unwrap();
        collided.bucket.wait_for_late_puts().await;
        assert_eq!(
            check_quiet_prefixes(&collided.bucket.view(), [&NodeNames::new(
                1,
                Vec::<String>::new()
            )]),
            Ok(())
        );
        // The second pass, an install timeout later, finds nothing more.
        assert!(
            snapshots.holds.gc_due(
                crate::rt::time::Instant::now()
                    .checked_add(snapshots.install.late_write())
                    .unwrap()
            )
        );
    }

    /// A group whose engine stopped and did not open again stays registered
    /// with holds that owe passes. It takes no GC slot: the round counts it
    /// neither run nor left, so it neither starves live groups nor keeps
    /// the driver ticking again at once.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_stopped_engine_that_stays_registered_takes_no_gc_slot() {
        let store = InstallStore::new();
        let (registry, engine, _root) = install_engine(store).await;
        let holds = engine.snapshots.holds.clone();
        engine.shutdown().await.unwrap();
        holds.owe_pass(crate::rt::time::Instant::now());
        assert!(holds.gc_due(crate::rt::time::Instant::now()));
        let pass = registry
            .collect_snapshot_garbage(usize::MAX, Duration::from_secs(5))
            .await;
        assert_eq!((pass.groups, pass.left, pass.failed), (0, 0, 0));
    }

    /// A copy whose PUT answers an error and lands later is refused with
    /// `CopyWrite`, and owes a pass at once and another one an install
    /// timeout later. The first pass runs before the object lands, so only
    /// the second leaves the node's directory with what it names (I5): the
    /// control checks I5 between the landing and the second pass.
    #[cfg(not(madsim))]
    #[tokio::test(start_paused = true)]
    async fn a_copy_whose_put_lands_late_goes_at_the_second_pass() {
        let store = InstallStore::with(
            SnapshotStoreFaults {
                late_put: ursula_runtime::snapshot_harness::PerMille::ALWAYS,
                late_put_delay: ursula_runtime::snapshot_harness::DelayRange::fixed(
                    Duration::from_secs(30),
                ),
                ..SnapshotStoreFaults::none()
            },
            None,
        );
        let snapshots = group_snapshots(store.clone());
        // Both passes the restore owes, so that only the copy's own passes
        // are owed below.
        let late_write = snapshots.install.late_write();
        snapshots
            .holds
            .collect_garbage(
                &snapshots.store,
                crate::rt::time::Instant::now()
                    .checked_add(late_write)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !snapshots.holds.gc_due(
                crate::rt::time::Instant::now()
                    .checked_add(late_write.saturating_mul(10))
                    .unwrap()
            )
        );
        let refused = snapshots
            .prefetch(RaftGroupId(7), external_snapshot("late"))
            .await
            .expect_err("the copy's PUT answered an error");
        assert!(
            matches!(refused, SnapshotInstallError::CopyWrite { .. }),
            "{refused:?}"
        );
        let late_pass = crate::rt::time::Instant::now()
            .checked_add(snapshots.install.late_write())
            .unwrap();
        snapshots
            .holds
            .collect_garbage(&snapshots.store, crate::rt::time::Instant::now())
            .await
            .unwrap();
        assert!(
            snapshots.holds.gc_due(late_pass),
            "the failed copy owes its second pass"
        );
        store.bucket.wait_for_late_puts().await;
        let quiet = || {
            check_quiet_prefixes(&store.bucket.view(), [&NodeNames::new(
                1,
                snapshots.holds.held_keys(),
            )])
        };
        assert!(
            quiet().is_err(),
            "control: the PUT landed after the first pass"
        );
        tokio::time::sleep_until(late_pass).await;
        assert!(snapshots.holds.gc_due(crate::rt::time::Instant::now()));
        snapshots
            .holds
            .collect_garbage(&snapshots.store, crate::rt::time::Instant::now())
            .await
            .unwrap();
        assert_eq!(quiet(), Ok(()));
    }

    /// Every install refusal's answer: its gRPC code, whether the sender
    /// backs off, and whether the in-process network makes it an
    /// unreachable target. The table is an exhaustive match, and no refusal
    /// is `FAILED_PRECONDITION` (#539).
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn every_install_refusal_answers_its_table_row() {
        use tonic::Code;
        use ursula_runtime::GroupEngineError;
        use ursula_runtime::GroupInfraError;

        /// `(gRPC code, peer_backs_off, in-process unreachable)`.
        fn expected(error: &SnapshotInstallError) -> (Code, bool, bool) {
            match error {
                SnapshotInstallError::NotRegistered { .. } => (Code::NotFound, false, false),
                SnapshotInstallError::ShuttingDown { .. } | SnapshotInstallError::Task(_) => {
                    (Code::Unavailable, true, false)
                }
                // A group refusal answers by the group's own classification
                // on gRPC. In-process only a refused overwrite backs off
                // (#725).
                SnapshotInstallError::Group(group) => (
                    crate::grpc::group_rpc_code(group),
                    group.peer_backs_off(),
                    matches!(
                        group.infra(),
                        Some(
                            GroupInfraError::CommittedOverwrite { .. }
                                | GroupInfraError::UnrelatedHistory { .. }
                                | GroupInfraError::LogBehindCommit { .. }
                        )
                    ),
                ),
                SnapshotInstallError::GateClosed { .. }
                | SnapshotInstallError::SourceMissing { .. }
                | SnapshotInstallError::SourceRead { .. }
                | SnapshotInstallError::CopyWrite { .. }
                | SnapshotInstallError::CopyVerify { .. }
                | SnapshotInstallError::NameCollision { .. } => (Code::Unavailable, true, true),
                SnapshotInstallError::PointerUndecodable { .. }
                | SnapshotInstallError::MetaPointer { .. } => (Code::InvalidArgument, false, false),
                SnapshotInstallError::Undecodable { .. }
                | SnapshotInstallError::MetaState { .. } => (Code::DataLoss, false, false),
                SnapshotInstallError::PointerEncode { .. } => (Code::Internal, false, false),
            }
        }

        let group = RaftGroupId(7);
        let store_error = || SnapshotStoreError::NotFound {
            key: "snapshots/k.snap".to_owned(),
        };
        let cancelled = {
            let task = crate::rt::spawn(std::future::pending::<()>());
            task.abort();
            task.await.expect_err("an aborted task")
        };
        let samples = vec![
            SnapshotInstallError::NotRegistered { group },
            SnapshotInstallError::ShuttingDown { group },
            SnapshotInstallError::Group(GroupEngineError::Infra(
                GroupInfraError::CommittedOverwrite {
                    group: RaftTarget::Data(group),
                    committed: 2,
                    index: 1,
                },
            )),
            SnapshotInstallError::Group(GroupEngineError::Infra(
                GroupInfraError::RaftGroupNotRegistered {
                    group: RaftTarget::Data(group),
                },
            )),
            SnapshotInstallError::Group(GroupEngineError::Infra(
                GroupInfraError::UnexpectedResponse {
                    expected: ursula_runtime::ResponseKind::Append,
                    actual: ursula_runtime::ResponseKind::Missing,
                },
            )),
            SnapshotInstallError::Group(GroupEngineError::Infra(GroupInfraError::OwnerStopped)),
            SnapshotInstallError::Task(cancelled),
            SnapshotInstallError::GateClosed { group },
            SnapshotInstallError::PointerUndecodable {
                group,
                source: store_error(),
            },
            SnapshotInstallError::SourceMissing {
                group,
                source_key: "k".to_owned(),
            },
            SnapshotInstallError::SourceRead {
                group,
                source_key: "k".to_owned(),
                source: store_error(),
            },
            SnapshotInstallError::Undecodable {
                group,
                snapshot_id: "s".to_owned(),
                source: store_error(),
            },
            SnapshotInstallError::CopyWrite {
                group,
                own_key: "k".to_owned(),
                source: store_error(),
            },
            SnapshotInstallError::CopyVerify {
                group,
                own_key: "k".to_owned(),
                expected: 2,
                actual: 1,
            },
            SnapshotInstallError::NameCollision {
                group,
                own_key: "k".to_owned(),
            },
            SnapshotInstallError::PointerEncode {
                group,
                source: store_error(),
            },
            SnapshotInstallError::MetaPointer {
                source: crate::meta::snapshot::MetaSnapshotError::NotInline {
                    kind: ursula_runtime::SnapshotLocationKind::S3,
                },
            },
            SnapshotInstallError::MetaState {
                source: ursula_stream::msgpack::decode::<crate::meta::MetaState>(&[0xc1])
                    .expect_err("0xc1 is no MessagePack value"),
            },
        ];
        let owner_stopped =
            SnapshotInstallError::Group(GroupEngineError::Infra(GroupInfraError::OwnerStopped));
        assert_eq!(
            expected(&owner_stopped),
            (Code::Unavailable, true, false),
            "a stopped owner backs the sender off on gRPC, not in-process (#725)"
        );
        for error in samples {
            let row = (
                error.code(),
                error.peer_backs_off(),
                crate::in_process::install_error_backs_off(&error),
            );
            assert_eq!(row, expected(&error), "{error:?}");
            assert_ne!(error.code(), Code::FailedPrecondition, "{error:?}");
            let code = error.code();
            assert_eq!(tonic::Status::from(error).code(), code);
        }
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn prefetch_snapshot_for_install_does_not_download_inline_snapshot() {
        let store = InstallStore::new();
        let pointer = SnapshotPointer {
            snapshot_id: "inline".to_owned(),
            location: SnapshotLocation::Inline {
                bytes: group_snapshot_bytes(),
            },
        };
        let snapshot = UrsulaSnapshot {
            meta: snapshot_meta(),
            snapshot: SnapshotData::new(pointer.encode_binary().expect("encode test pointer")),
        };

        let prefetched = group_snapshots(store.clone())
            .prefetch(RaftGroupId(7), snapshot)
            .await
            .expect("inline snapshot does not touch snapshot store");
        let pointer =
            SnapshotPointer::decode(prefetched.snapshot.snapshot.pointer_bytes()).unwrap();
        assert!(matches!(pointer.location, SnapshotLocation::Inline { .. }));
        assert_eq!(store.downloads.load(Ordering::SeqCst), 0);
        assert!(store.own_keys().is_empty());
    }

    #[test]
    fn leadership_shed_policy_keeps_transfers_and_campaigning_consistent() {
        let empty = LeadershipShedState::default();
        assert!(empty.should_accept_transfer());
        assert!(empty.should_campaign());
        assert!(!empty.should_shed_current_leaders());
        assert_eq!(empty.transfer_rejection_reason(), None);

        let snapshot_shed = LeadershipShedState::from(LeadershipShedReason::SnapshotDriverS3);
        assert!(!snapshot_shed.should_accept_transfer());
        assert!(!snapshot_shed.should_campaign());
        assert!(snapshot_shed.should_shed_current_leaders());
        assert_eq!(
            snapshot_shed.transfer_rejection_reason(),
            Some(LeadershipShedReason::SnapshotDriverS3)
        );

        let cold_shed = LeadershipShedState::from(LeadershipShedReason::ColdHealth);
        assert!(cold_shed.should_accept_transfer());
        assert!(cold_shed.should_campaign());
        assert!(cold_shed.should_shed_current_leaders());
        assert_eq!(cold_shed.transfer_rejection_reason(), None);

        let egress_shed = LeadershipShedState::CLUSTER_EGRESS | LeadershipShedState::COLD_HEALTH;
        assert!(!egress_shed.should_accept_transfer());
        assert!(!egress_shed.should_campaign());
        assert!(egress_shed.should_shed_current_leaders());
        assert_eq!(
            egress_shed.transfer_rejection_reason(),
            Some(LeadershipShedReason::ClusterEgress)
        );

        let maintenance_shed = LeadershipShedState::from(LeadershipShedReason::MaintenanceDrain);
        assert!(!maintenance_shed.should_accept_transfer());
        assert!(!maintenance_shed.should_campaign());
        assert!(maintenance_shed.should_shed_current_leaders());
        assert_eq!(
            maintenance_shed.transfer_rejection_reason(),
            Some(LeadershipShedReason::MaintenanceDrain)
        );

        let wal_disk_shed = LeadershipShedState::from(LeadershipShedReason::WalDiskPressure);
        assert!(!wal_disk_shed.should_accept_transfer());
        assert!(!wal_disk_shed.should_campaign());
        assert!(wal_disk_shed.should_shed_current_leaders());
        assert_eq!(
            wal_disk_shed.transfer_rejection_reason(),
            Some(LeadershipShedReason::WalDiskPressure)
        );

        // A node without a meta replica campaigns, takes no transfer and
        // hands its leaderships away.
        let placement_shed = LeadershipShedState::from(LeadershipShedReason::PlacementUnavailable);
        assert!(!placement_shed.should_accept_transfer());
        assert!(placement_shed.should_campaign());
        assert!(placement_shed.should_shed_current_leaders());
        assert_eq!(
            placement_shed.transfer_rejection_reason(),
            Some(LeadershipShedReason::PlacementUnavailable)
        );
        let status = registry_with(LeadershipShedReason::PlacementUnavailable)
            .participation_status(ursula_headers::ClusterId::new("placement-shed").unwrap());
        assert!(status.should_campaign);
        assert!(!status.should_accept_transfer);
        assert!(status.should_shed_current_leaders);
    }

    fn registry_with(reason: LeadershipShedReason) -> RaftGroupHandleRegistry {
        let registry = RaftGroupHandleRegistry::default();
        registry.mark_leadership_shed(reason);
        registry
    }

    #[test]
    fn leadership_shed_reasons_are_tracked_independently() {
        let registry = RaftGroupHandleRegistry::default();
        assert!(!registry.is_leadership_shed());

        registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        assert!(registry.is_leadership_shed());
        assert!(!registry.leadership_shed_state().should_accept_transfer());
        assert!(!registry.leadership_shed_state().should_campaign());

        registry.mark_leadership_shed(LeadershipShedReason::ClusterEgress);
        registry.mark_leadership_shed(LeadershipShedReason::ColdHealth);
        assert!(registry.is_leadership_shed());
        assert!(!registry.leadership_shed_state().should_accept_transfer());

        registry.clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        assert!(registry.is_leadership_shed());
        assert!(!registry.leadership_shed_state().should_accept_transfer());

        registry.clear_leadership_shed(LeadershipShedReason::ClusterEgress);
        assert!(registry.is_leadership_shed());
        assert!(registry.leadership_shed_state().should_accept_transfer());

        registry.mark_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
        assert!(registry.is_leadership_shed());
        assert!(!registry.leadership_shed_state().should_accept_transfer());

        registry.clear_leadership_shed(LeadershipShedReason::ColdHealth);
        registry.clear_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
        assert!(!registry.is_leadership_shed());
    }
}
