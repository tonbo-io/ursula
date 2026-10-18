use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use futures_util::future::join_all;
use openraft::BasicNode;
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
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;
use openraft::vote::RaftLeaderId;
use tokio::sync::watch;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ErrorChain;
use ursula_runtime::GroupEngineError;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::default_snapshot_store;
use ursula_shard::RaftGroupId;
#[cfg(test)]
use ursula_shard::ShardPlacement;

use crate::election::ElectionPolicy;
pub use crate::election::LeadershipShedFlag;
pub use crate::election::LeadershipShedReason;
pub use crate::election::LeadershipShedState;
use crate::log_store::WalOpening;
use crate::meta::MetaRaftTypeConfig;
use crate::owner::OwnerRaftHandle;
use crate::read_index::ReadIndexBarrier;
use crate::rejoin::GroupRejoin;
use crate::rejoin::RecoveryGateError;
use crate::rejoin::RecoveryGateStatus;
use crate::snapshot_codec::decode_group_snapshot;
use crate::state_machine::RaftGroupStateMachine;
use crate::state_machine::SnapshotBuildCoordinator;
use crate::state_machine::SnapshotInstallCoordinator;
use crate::types::RaftGroupMetricsSnapshot;
use crate::types::RaftLogProgressSnapshot;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetworkFactory;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetwork;

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for SingleNodeRaftNetworkFactory {
    type Network = SingleNodeRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        SingleNodeRaftNetwork
    }
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for SingleNodeRaftNetwork {
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
        _snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
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

impl RaftNetworkFactory<MetaRaftTypeConfig> for SingleNodeRaftNetworkFactory {
    type Network = SingleNodeRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        SingleNodeRaftNetwork
    }
}

impl RaftNetworkV2<MetaRaftTypeConfig> for SingleNodeRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<MetaRaftTypeConfig>, RPCError<MetaRaftTypeConfig>> {
        unreachable!("single-node meta raft group must not send AppendEntries")
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<MetaRaftTypeConfig>, RPCError<MetaRaftTypeConfig>> {
        unreachable!("single-node meta raft group must not send Vote")
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<MetaRaftTypeConfig>,
        _snapshot: TypeConfigSnapshotOf<MetaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<MetaRaftTypeConfig>, StreamingError<MetaRaftTypeConfig>> {
        unreachable!("single-node meta raft group must not send snapshots")
    }

    async fn transfer_leader(
        &mut self,
        _req: TransferLeaderRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::TransferLeaderResponse<MetaRaftTypeConfig>,
        RPCError<MetaRaftTypeConfig>,
    > {
        unreachable!("single-node meta raft group must not transfer leadership")
    }
}

/// Owned handle to a single Raft group's [`Raft`] instance, as stored in the
/// [`RaftGroupHandleRegistry`]. Spelled out as a type alias so callers (e.g. the
/// HTTP admin handlers) can name it without repeating the full type-config
/// generics.
pub type RaftGroupHandle = Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>;

/// A group's shared cold-index page cache.
pub type GroupColdIndexCache = Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>;

/// How long a recovery plan's accept waits for a restarted replica to
/// publish the membership it rebuilt from its cut log (#613).
const RESTARTED_MEMBERSHIP_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Resources published atomically with a live owner handle. Only cache and
/// recovery are optional capabilities; every registered group has a barrier.
#[derive(Debug, Clone)]
pub(crate) struct ActiveGroup {
    pub(crate) apply_health: crate::apply_failure::ApplyHealth,
    pub(crate) snapshot_installs: Arc<crate::openraft_work::WorkAdmission>,
    pub(crate) cache: Option<GroupColdIndexCache>,
    pub(crate) barrier: Arc<ReadIndexBarrier>,
    pub(crate) recovery: Option<Arc<GroupRejoin>>,
    /// The group's history on this replica (#581 section 4.5): a leader
    /// whose history is pending proposes, serves and confirms nothing.
    pub(crate) history: Arc<crate::history::GroupHistory>,
    /// The state machine's log gauge and applied membership (#613).
    pub(crate) log_gauge: Arc<crate::snapshot_cadence::GroupLogGauge>,
}
impl ActiveGroup {
    pub(crate) fn raft(&self) -> &OwnerRaftHandle {
        self.barrier.owner()
    }
    fn check_recovery_vote(
        &self,
        group: RaftGroupId,
        vote: crate::UrsulaVote,
    ) -> Result<Option<crate::UrsulaVote>, GroupEngineError> {
        let Some(rejoin) = &self.recovery else {
            return Ok(None);
        };
        // A replica restarting for a plan takes no replication, and one
        // holding a plan none from a leader elected before it (M1, #581
        // review).
        if let Some(refusal) = rejoin.replication_refusal(&vote) {
            return Err(GroupEngineError::Infra(refusal));
        }
        let floor = rejoin.recovery_vote().ok_or(GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RecoveryVoteFloor {
                raft_group_id: group,
            },
        ))?;
        Ok((vote < floor).then_some(floor))
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
        let active = self.active()?;
        if active.apply_health.failure().is_some() {
            return None;
        }
        active.recovery.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct RaftGroupHandleRegistry {
    pub(crate) append_send_budget: crate::grpc::AppendTransportBudget,
    pub(crate) append_receive_budget: crate::grpc::AppendTransportBudget,
    groups: Arc<arc_swap::ArcSwap<BTreeMap<u32, Arc<GroupEntry>>>>,
    dynamic_hosted_groups: Arc<Mutex<BTreeSet<RaftGroupId>>>,
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
}

impl Default for RaftGroupHandleRegistry {
    fn default() -> Self {
        let (transport_shutdown, _) = watch::channel(false);
        Self {
            append_send_budget: Default::default(),
            append_receive_budget: Default::default(),
            groups: Arc::new(arc_swap::ArcSwap::from_pointee(BTreeMap::new())),
            dynamic_hosted_groups: Arc::new(Mutex::new(BTreeSet::new())),
            election: ElectionPolicy::default(),
            transport_shutdown,
            snapshot_store: Arc::new(Mutex::new(default_snapshot_store())),
            snapshot_build: Arc::new(Mutex::new(SnapshotBuildCoordinator::new(1))),
            snapshot_install: SnapshotInstallCoordinator::new(1),
            wal_opening: Arc::new(Mutex::new(None)),
            peer_cluster: crate::peer_cluster::PeerCluster::default(),
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
    /// The passes that deleted nothing because a configured voter of the
    /// group had published no reference.
    pub skipped_unreferenced_voter: u64,
    /// The passes that failed, which run again later.
    pub failed: usize,
    /// The groups whose pass is due but past `max_groups` or the budget.
    pub left: usize,
}

/// Failure of snapshot admission, of the prefetch and copy of an incoming
/// snapshot, of its installation, or of reference publication. It is the
/// error of [`RaftGroupHandleRegistry::install_full_snapshot`] and is not
/// part of any wire form. The gRPC plane converts it once, by [`From`] into
/// `tonic::Status`, whose code is [`SnapshotInstallError::code`].
///
/// The in-process network of the tests and the simulation classifies it on
/// its own, and does not yet back off for every refusal the gRPC plane
/// answers `UNAVAILABLE`. Admission and publication failures stay network
/// errors there, and so does every group refusal but a refused overwrite,
/// `Group(OwnerStopped)` among them (#725).
#[derive(Debug, thiserror::Error)]
pub enum SnapshotInstallError {
    #[error("snapshot group {group:?} is not registered")]
    NotRegistered { group: RaftGroupId },
    #[error("snapshot group {group:?} is shutting down")]
    ShuttingDown { group: RaftGroupId },
    #[error(transparent)]
    Group(#[from] GroupEngineError),
    #[error("publish installed snapshot reference")]
    Publication(#[source] ursula_runtime::SnapshotStoreError),
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
}

impl SnapshotInstallError {
    /// The gRPC code the sender gets. A [`Self::Group`] refusal gets the
    /// code of the group's error ([`crate::grpc::group_rpc_code`]). No
    /// refusal is `FAILED_PRECONDITION` (#539).
    pub(crate) fn code(&self) -> tonic::Code {
        match self {
            Self::NotRegistered { .. } => tonic::Code::NotFound,
            Self::PointerUndecodable { .. } => tonic::Code::InvalidArgument,
            Self::Undecodable { .. } => tonic::Code::DataLoss,
            Self::PointerEncode { .. } => tonic::Code::Internal,
            Self::Group(error) => crate::grpc::group_rpc_code(error),
            Self::ShuttingDown { .. }
            | Self::Publication(_)
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
struct PrefetchedInstallSnapshot {
    snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
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

/// Confirm a fresh ReadIndex under the same committed leader vote.
pub(crate) async fn confirm_recovery_barrier(
    group: RaftGroupId,
    raft: &OwnerRaftHandle,
    barrier: &dyn ursula_runtime::LinearizableReadBarrier,
) -> Result<(crate::UrsulaVote, u64), QuorumProofError> {
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
    Ok((after.vote, index))
}

pub use crate::election::IncomingTransferError;
pub use crate::election::LeadershipTransferError;

impl RaftGroupHandleRegistry {
    /// Configure the node-wide budget for each direction before constructing transports.
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
            .is_ok_and(|entry| self.election.may_campaign(entry.recovery.as_deref()))
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
        let (entry, metrics) = self.handoff_view(group)?;
        self.validate_handoff(group, target, &entry, &metrics)?;
        let raft = entry.raft();
        raft.trigger()
            .transfer_leader(target)
            .await
            .map_err(|source| LeadershipTransferError::Raft { group, source })?;
        raft.abandon_stalled_transfer(metrics.vote, target);
        Ok(())
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
        self.validate_handoff(group, target, &entry, &metrics)
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
        let metrics = entry.raft().metrics().borrow_watched().clone();
        Ok((entry, metrics))
    }

    fn validate_handoff(
        &self,
        group: RaftGroupId,
        target: u64,
        entry: &ActiveGroup,
        metrics: &RaftMetrics<UrsulaRaftTypeConfig>,
    ) -> Result<(), LeadershipTransferError> {
        let target_ack_age = metrics
            .heartbeat
            .as_ref()
            .and_then(|acks| acks.get(&target))
            .and_then(|acked| acked.as_ref())
            .map(|acked| openraft::Instant::elapsed(&**acked));
        self.election.validate_handoff(
            group,
            target,
            metrics,
            entry.recovery.as_deref(),
            target_ack_age,
            entry.raft().handoff_ack_window(),
        )
    }

    pub async fn confirm_quorum_prefix(
        &self,
        group: RaftGroupId,
    ) -> Result<ursula_proto::admin::QuorumPrefix, QuorumProofError> {
        let (vote, index) = self.confirm_recovery_barrier(group).await?;
        Ok(ursula_proto::admin::QuorumPrefix {
            raft_group_id: group.0,
            leader_id: *vote.leader_id().node_id(),
            leader_term: vote.leader_id().term(),
            required_applied_index: index,
        })
    }

    pub(crate) async fn confirm_recovery_barrier(
        &self,
        group: RaftGroupId,
    ) -> Result<(crate::UrsulaVote, u64), QuorumProofError> {
        let (raft, barrier, history) = {
            let groups = self.groups.load();
            let entry = groups
                .get(&group.0)
                .ok_or(QuorumProofError::NotRegistered { group })?;
            let entry = entry
                .active()
                .ok_or(QuorumProofError::NotRegistered { group })?;
            let raft = entry.raft().clone();
            let barrier = entry.barrier.clone();
            (raft, barrier, entry.history.clone())
        };
        // A leader whose history is pending confirms no barrier: a gate
        // opened on its state would follow a history it has not settled
        // (#581 section 4.5, #577 N1). It waits for the event a moment, as
        // a fenced proposal does.
        if history.admit_on(raft.metrics()).await.is_err() {
            return Err(QuorumProofError::HistoryPending { group });
        }
        confirm_recovery_barrier(group, &raft, barrier.as_ref()).await
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

    /// Confirm that API messages submitted before this observation have been
    /// processed by each captured local core. This is not a committed-prefix,
    /// state-machine completion or cluster-wide redundancy certificate.
    pub async fn confirm_admin_command_submission(&self) -> Result<(), String> {
        let groups = self
            .groups
            .load()
            .iter()
            .filter_map(|(id, entry)| entry.active().map(|active| (*id, active.raft().clone())))
            .collect::<Vec<_>>();
        let results = join_all(groups.into_iter().map(|(id, raft)| async move {
            match raft.with_raft_state(|_| ()).await {
                Ok(()) | Err(openraft::error::Fatal::Stopped) => Ok(()),
                Err(error) => Err(format!("group {id}: {error}")),
            }
        }))
        .await;
        let failures = results
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// The address each active group's effective membership records for
    /// `node_id`, read from the group's Raft core. Peers dial these addresses,
    /// not the configured ones, once a group is initialized. A group whose
    /// membership does not name the node, or whose core stopped, is left out.
    pub async fn recorded_node_addresses(&self, node_id: u64) -> Vec<(RaftGroupId, String)> {
        let groups = self
            .groups
            .load()
            .iter()
            .filter_map(|(id, entry)| entry.active().map(|active| (*id, active.raft().clone())))
            .collect::<Vec<_>>();
        join_all(groups.into_iter().map(|(id, raft)| async move {
            let address = raft
                .with_raft_state(move |state| {
                    state
                        .membership_state
                        .effective()
                        .membership()
                        .get_node(&node_id)
                        .map(|node| node.addr.clone())
                })
                .await
                .ok()
                .flatten();
            address.map(|address| (RaftGroupId(id), address))
        }))
        .await
        .into_iter()
        .flatten()
        .collect()
    }

    pub(crate) fn subscribe_transport_shutdown(&self) -> watch::Receiver<bool> {
        self.transport_shutdown.subscribe()
    }

    /// Publish all group resources together, before any transport can find its Raft handle.
    pub(crate) fn register_engine(&self, engine: &crate::RaftGroupEngine) {
        let entry = Arc::new(GroupEntry::Active(Arc::new(ActiveGroup {
            apply_health: engine.apply_health.clone(),
            snapshot_installs: engine.snapshot_installs.clone(),
            cache: engine.cold_index_cache.clone(),
            barrier: engine.read_barrier.clone(),
            history: engine.history.clone(),
            log_gauge: engine.log_gauge.clone(),
            recovery: engine
                .rejoin
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        })));
        self.groups.rcu(|groups| {
            let mut groups = (**groups).clone();
            groups.insert(engine.placement.raft_group_id.0, entry.clone());
            groups
        });
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

    /// Forgets this node's replica of `group`, as a node that stopped
    /// hosting it would: the leadership view no longer knows the group.
    #[cfg(madsim)]
    pub fn unregister_group_for_simulation(&self, group: RaftGroupId) {
        self.groups.rcu(|groups| {
            let mut groups = (**groups).clone();
            groups.remove(&group.0);
            groups
        });
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
    }

    /// The group's recovery gate, if it has one.
    pub fn rejoin(&self, raft_group_id: RaftGroupId) -> Option<Arc<GroupRejoin>> {
        self.groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active()?.recovery.clone())
    }

    /// Operator recovery when a majority of the group's voters are gated
    /// and may have lost acknowledged writes: this replica takes the
    /// recovery plan (#581 section 4.5; see
    /// [`GroupRejoin::accept_recovery_plan`]) as one owner job, its group's
    /// leader fence learns the plan, and the group's elections follow it.
    pub async fn accept_recovery_plan(
        &self,
        raft_group_id: RaftGroupId,
        plan: &RecoveryPlanRequest,
        restart: &mut dyn crate::rejoin::ReplicaRestart,
    ) -> Result<RecoveryPlanReport, RecoveryGateError> {
        let entry = self
            .groups
            .load()
            .get(&raft_group_id.0)
            .and_then(|entry| entry.active().cloned())
            .ok_or(RecoveryGateError::NotRegistered { raft_group_id })?;
        let rejoin = entry
            .recovery
            .clone()
            .ok_or(RecoveryGateError::NotRegistered { raft_group_id })?;
        let owner = entry.raft().clone();
        let plan = *plan;
        let job = rejoin.clone();
        let history = entry.history.clone();
        let applied = entry.log_gauge.applied_membership();
        // m1 (#581 review): the job, with its rollback, runs in a task of
        // its own, so a caller that stops waiting never cancels it half
        // way. The restart that follows a cut runs in the caller's future:
        // the HTTP route runs in the admin fence's own task (n6).
        let job = crate::rt::spawn(async move {
            owner
                .call(move |raft| async move {
                    let core = crate::rejoin::RaftPlanCore {
                        raft: &raft,
                        raft_group_id,
                    };
                    job.accept_recovery_plan(&plan, &core, &history, applied)
                        .await
                })
                .await
                .map_err(|source| RecoveryGateError::OwnerStopped {
                    raft_group_id,
                    source,
                })?
        });
        let taken = job
            .await
            .map_err(|_aborted| RecoveryGateError::JobAborted { raft_group_id })
            .and_then(|taken| taken);
        // m6 (#581 review): the job stopped the core campaigning; the
        // node's election policy decides again, taken or refused.
        self.refresh_group_elections(raft_group_id);
        let report = match taken? {
            crate::rejoin::PlanTaken::Holds(report) => report,
            // B1 (#581 review): the cut runs before the plan answers. The
            // group's engine stops, its log store closes, and the new
            // replica's recovery gate cuts the log before its Raft core
            // starts, then takes the plan's gate. A cut that cannot run
            // drops the plan and closes the gate (m3).
            crate::rejoin::PlanTaken::Restart(report) => {
                drop(entry);
                drop(rejoin);
                let restarted = restart
                    .restart_replica(raft_group_id)
                    .await
                    .map_err(|source| RecoveryGateError::Restart {
                        raft_group_id,
                        source: Box::new(source),
                    })?
                    .ok_or(RecoveryGateError::NotRegistered { raft_group_id })?;
                if let Some((committed, held)) = restarted.cut_refused() {
                    return Err(RecoveryGateError::CutRefused {
                        raft_group_id,
                        committed,
                        held,
                    });
                }
                // #613: the restart rebuilt the membership from the cut log.
                // A plan's leader that it leaves no voter can never
                // campaign: drop the plan rather than wedge its members.
                // The restarted core publishes the membership it rebuilt
                // once it runs; until then its metrics hold none.
                // Off under the R2 negative control, as before #613.
                let checks_kept = !ursula_stream::sim_controls::off(
                    ursula_stream::sim_controls::Defence::PlanKeptMembership,
                );
                if checks_kept && plan.cut && plan.leader == report.node_id {
                    let entry = self
                        .groups
                        .load()
                        .get(&raft_group_id.0)
                        .and_then(|entry| entry.active().cloned())
                        .ok_or(RecoveryGateError::NotRegistered { raft_group_id })?;
                    let published = entry
                        .raft()
                        .wait(Some(RESTARTED_MEMBERSHIP_WAIT))
                        .metrics(
                            |metrics| metrics.membership_config.log_id().is_some(),
                            "the restarted replica publishes its membership",
                        )
                        .await;
                    // A restart whose membership never shows cannot be
                    // shown to campaign: fail closed, as for no vote.
                    let refusal = match published {
                        Ok(metrics)
                            if metrics
                                .membership_config
                                .membership()
                                .voter_ids()
                                .any(|voter| voter == plan.leader) =>
                        {
                            None
                        }
                        Ok(_left_without_vote) => {
                            Some(RecoveryGateError::CutLeftNoVote { raft_group_id })
                        }
                        Err(_timed_out) => {
                            Some(RecoveryGateError::CutMembershipUnknown { raft_group_id })
                        }
                    };
                    if let Some(refusal) = refusal {
                        restarted
                            .drop_plan_left_without_vote(&entry.history)
                            .await?;
                        self.refresh_group_elections(raft_group_id);
                        return Err(refusal);
                    }
                }
                report
            }
        };
        self.refresh_group_elections(raft_group_id);
        Ok(report)
    }

    /// The recovery gate of every group that has one on this node.
    pub fn recovery_gates(&self) -> BTreeMap<u32, RecoveryGateStatus> {
        self.groups
            .load()
            .iter()
            .filter_map(|(id, entry)| entry.participating_gate().map(|gate| (*id, gate.status())))
            .collect()
    }

    /// Groups whose gated replica on this node got no leader barrier and
    /// applied nothing for [`crate::RECOVERY_STALL_AFTER`]: they wait for an
    /// operator to accept the loss of the unsynced tail.
    pub fn recovery_report(&self) -> ursula_proto::admin::RecoveryGatesReport {
        ursula_proto::admin::RecoveryGatesReport::new(self.recovery_gates())
    }

    pub fn stalled_recovery_groups(&self) -> Vec<u32> {
        self.recovery_report().stalled
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

    pub fn allow_dynamic_group_hosting(&self, raft_group_id: RaftGroupId) -> bool {
        self.dynamic_hosted_groups
            .lock()
            .expect("raft dynamic hosted groups mutex")
            .insert(raft_group_id)
    }

    pub fn dynamic_group_hosting_allowed(&self, raft_group_id: RaftGroupId) -> bool {
        self.dynamic_hosted_groups
            .lock()
            .expect("raft dynamic hosted groups mutex")
            .contains(&raft_group_id)
    }

    pub fn len(&self) -> usize {
        self.groups.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot installs: at most `max_concurrency` at once, and the
    /// snapshot GC grace G (#669), how long each group keeps the snapshot
    /// object it replaced pinned (`RaftConfig::snapshot_gc_grace` on a
    /// server).
    pub fn with_snapshot_install(
        mut self,
        max_concurrency: usize,
        gc_grace: std::time::Duration,
    ) -> Self {
        self.snapshot_install =
            SnapshotInstallCoordinator::with_gc_grace(max_concurrency, gc_grace);
        self
    }

    /// Runs the snapshot GC passes due now (#669), for at most `max_groups`
    /// groups in group order, and starts none once `budget` has passed:
    /// each drops the pins that expired, publishes the group's current
    /// reference and prunes the objects nothing names any more. A failed
    /// pass runs again later, and a pass left out stays due.
    pub async fn collect_snapshot_garbage(
        &self,
        max_groups: usize,
        budget: std::time::Duration,
    ) -> SnapshotGcPass {
        let started = crate::rt::time::Instant::now();
        let due = self.snapshot_install.gc_due(started);
        let store = self.snapshot_store();
        let mut pass = SnapshotGcPass {
            left: due.len(),
            ..SnapshotGcPass::default()
        };
        for (group, references) in due.into_iter().take(max_groups) {
            if started.elapsed() >= budget {
                break;
            }
            let now = crate::rt::time::Instant::now();
            pass.left = pass.left.saturating_sub(1);
            pass.groups = pass.groups.saturating_add(1);
            match references.collect_garbage(&store, group, now).await {
                Ok(prune) => {
                    pass.deleted = pass.deleted.saturating_add(prune.deleted());
                    pass.skipped_unreferenced_voter = pass
                        .skipped_unreferenced_voter
                        .saturating_add(u64::from(prune.skipped_unreferenced_voter()));
                }
                Err(err) => {
                    pass.failed = pass.failed.saturating_add(1);
                    tracing::warn!(
                        raft_group_id = group,
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

    fn snapshot_store(&self) -> SharedSnapshotStore {
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
            self.election.refresh(entry.raft(), entry.recovery.clone());
        }
    }

    /// Serialize recovery-barrier changes with maintenance policy updates.
    pub(crate) fn refresh_group_elections(&self, group: RaftGroupId) {
        let groups = self.groups.load();
        if let Some(entry) = groups.get(&group.0).and_then(|entry| entry.active()) {
            self.election.refresh(entry.raft(), entry.recovery.clone());
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

    /// This node's leadership-shed state, as a node of `cluster_id` answers
    /// it. A node cannot receive a planned leadership handoff until the
    /// recovery gate of every registered group is open.
    pub fn participation_status(
        &self,
        cluster_id: ursula_headers::ClusterId,
    ) -> ursula_proto::admin::LeadershipShedStatus {
        let state = self.election.state();
        let should_campaign = self.should_campaign();
        ursula_proto::admin::LeadershipShedStatus {
            cluster_id,
            bits: state.bits(),
            state: state.to_string(),
            should_accept_transfer: should_campaign,
            should_campaign,
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
                        current_term: stopped.failure.term,
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
                        apply_failure: Some(stopped.failure.clone()),
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
            let health = &entry.apply_health;
            let metrics = raft.metrics().borrow_watched().clone();
            // A removal's debt lasts until the log is purged through the
            // snapshot that dropped it (#669).
            entry
                .log_gauge
                .observe_purged(metrics.purged.map(|purged| purged.index));
            let log = entry.log_gauge.progress();
            // `RaftMetrics` carry only the index of the last log id. The data
            // metrics carry the whole id, so its term and index match.
            let last_log = raft.data_metrics().borrow_watched().last_log;
            let membership = metrics.membership_config.membership();
            let apply_failure = health.failure();
            let apply_running = apply_failure.is_none();
            let running = apply_running
                && metrics.running_state.is_ok()
                && metrics.state != openraft::ServerState::Shutdown;
            snapshots.push(RaftGroupMetricsSnapshot {
                apply_failure,
                raft_group_id,
                node_id: metrics.id,
                current_term: metrics.current_term,
                // A stopped replica leads nothing. Its last observation is
                // stale, so leader counts and drains must not see it.
                current_leader: metrics.current_leader.filter(|_| running),
                last_log: last_log.map(log_progress_snapshot),
                committed: metrics.local_committed.map(log_progress_snapshot),
                last_applied: metrics.last_applied.map(log_progress_snapshot),
                snapshot: metrics.snapshot.map(log_progress_snapshot),
                purged: metrics.purged.map(log_progress_snapshot),
                voter_ids: membership.voter_ids().collect(),
                voter_configurations: membership.get_joint_config().clone(),
                committed_voter_configurations: crate::rejoin::kept_membership_of(
                    entry.recovery.as_deref(),
                    metrics.local_committed.map(|id| id.index),
                    entry.log_gauge.applied_membership(),
                )
                .membership()
                .get_joint_config()
                .clone(),
                learner_ids: membership.learner_ids().collect(),
                maintenance: crate::types::RaftGroupMaintenanceState {
                    running,
                    recovery_ready: apply_running
                        && entry
                            .recovery
                            .as_ref()
                            .is_none_or(|rejoin| rejoin.vote_gate_open()),
                    accepting_transfers: apply_running
                        && self.election.may_campaign(entry.recovery.as_deref()),
                    membership_joint: membership.get_joint_config().len() != 1,
                    membership_log_index: metrics.membership_config.log_id().map(|id| id.index()),
                    stopped_for_operator: entry
                        .recovery
                        .as_ref()
                        .is_some_and(|rejoin| rejoin.status().is_stalled()),
                },
                log,
                history: entry.history.applied().map(|event| {
                    ursula_proto::admin::RaftGroupHistory {
                        event_index: event.event_index,
                        imported: event.imported.is_some(),
                    }
                }),
                recovery: entry
                    .recovery
                    .as_ref()
                    .and_then(|rejoin| rejoin.recovery_record())
                    .map(|record| ursula_proto::admin::RaftGroupRecovery {
                        leader: record.leader,
                        point: record.point,
                        leader_term: record.leader_term,
                        cut_pending: matches!(
                            record.cut,
                            crate::log_store::PlanCut::Pending { .. }
                        ),
                    }),
                leads_live: crate::rejoin::leads_live(&metrics),
                recovery_gate: entry
                    .recovery
                    .as_ref()
                    .map_or(ursula_proto::admin::RecoveryGateStatus::Open, |rejoin| {
                        rejoin.status()
                    }),
            });
        }
        snapshots
    }

    pub(crate) fn entry(&self, group: RaftGroupId) -> Result<Arc<ActiveGroup>, GroupEngineError> {
        let groups = self.groups.load();
        let entry = groups.get(&group.0).ok_or(GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RaftGroupNotRegistered {
                raft_group_id: group,
            },
        ))?;
        match entry.as_ref() {
            GroupEntry::Active(active) => {
                active.apply_health.check(group)?;
                Ok(active.clone())
            }
            GroupEntry::ApplyStopped { stopped, .. } => Err(GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::ApplyStopped {
                    raft_group_id: group,
                    term: stopped.failure.term,
                    index: stopped.failure.index,
                    kind: stopped.failure.kind,
                },
            )),
        }
    }

    pub async fn append_entries(
        &self,
        raft_group_id: RaftGroupId,
        request: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let entry = self.entry(raft_group_id)?;
        if let Some(floor) = entry.check_recovery_vote(raft_group_id, request.vote)? {
            return Ok(AppendEntriesResponse::HigherVote(floor));
        }
        let raft = entry.raft().clone();
        let rejoin = entry.recovery.clone();
        raft.call(move |raft| async move {
            if let Some(rejoin) = rejoin {
                // A recovery plan may have closed the replica since the
                // check above: an entry it acknowledged now would be cut, or
                // come from a leader elected before the plan.
                if let Some(refusal) = rejoin.replication_refusal(&request.vote) {
                    return Err(refusal);
                }
                let marker = rejoin
                    .effective_marker(&openraft::rt::WatchReceiver::borrow_watched(
                        &raft.metrics(),
                    ))
                    .inspect_err(|refusal| {
                        tracing::warn!(%refusal, "recovery: refused an append while the log lags");
                    })?;
                if let Some(refusal) =
                    rejoin.committed_overwrite(crate::rejoin::Incoming::Append(&request), marker)
                {
                    tracing::error!(%refusal, "recovery: refused an append below the commit marker");
                    return Err(refusal);
                }
                rejoin.observe_inbound_append(&request);
            }
            Ok(raft.append_entries(request).await)
        })
        .await
        .map_err(crate::owner::owner_stopped)?
        .map_err(GroupEngineError::Infra)?
        .map_err(crate::owner::owner_raft_error)
    }

    pub async fn vote(
        &self,
        raft_group_id: RaftGroupId,
        request: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let entry = self.entry(raft_group_id)?;
        let raft = entry.raft().clone();
        let rejoin = entry.recovery.clone();
        raft.call(move |raft| async move {
            if let Some(refusal) = rejoin.and_then(|rejoin| rejoin.screen_vote(&request)) {
                return Ok(refusal);
            }
            raft.vote(request).await
        })
        .await
        .map_err(crate::owner::owner_stopped)?
        .map_err(crate::owner::owner_raft_error)
    }

    pub async fn install_full_snapshot(
        &self,
        raft_group_id: RaftGroupId,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, SnapshotInstallError> {
        // A refused install must not queue for the group's lock or the
        // node's permit. The floor only rises and the Raft core's vote never
        // falls below it, so Raft refuses a vote the floor overtakes while
        // this request waits.
        let entry = self.entry(raft_group_id)?;
        if let Some(floor) = entry.check_recovery_vote(raft_group_id, vote)? {
            return Ok(SnapshotResponse::new(floor));
        }
        if let Some(refusal) = entry
            .recovery
            .as_ref()
            .and_then(|rejoin| rejoin.refused_snapshot(&snapshot.meta.snapshot_id))
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
        let rejoin = entry.recovery.clone();
        let history = entry.history.clone();
        let lifetime =
            entry
                .snapshot_installs
                .admit()
                .ok_or(SnapshotInstallError::ShuttingDown {
                    group: raft_group_id,
                })?;
        let registry = self.clone();
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
                    let already_current = current.as_ref().is_some_and(|current| {
                        current.meta == snapshot.meta
                            && current.snapshot.pointer_bytes() == snapshot.snapshot.pointer_bytes()
                    });
                    if already_current {
                        Ok::<_, SnapshotInstallError>(PrefetchedInstallSnapshot {
                            snapshot,
                            guard: None,
                            history: None,
                        })
                    } else {
                        registry
                            .prefetch_snapshot_for_install(raft_group_id, snapshot)
                            .await
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
                    // A recovery plan may have closed the replica while the
                    // install waited.
                    if let Some(refusal) = rejoin
                        .as_ref()
                        .and_then(|rejoin| rejoin.replication_refusal(&vote))
                    {
                        return Err(refusal);
                    }
                    // #613: like an append, a snapshot never replaces entries
                    // this replica knows are committed.
                    if let Some(incoming) = incoming_history
                        && let Some(rejoin) = rejoin.as_ref()
                    {
                        // The marker and the applied index, read once: the
                        // history at the marker is read against them.
                        let (marker, applied_index) = {
                            let watched = raft.metrics();
                            let metrics = openraft::rt::WatchReceiver::borrow_watched(&watched);
                            (
                                rejoin.effective_marker(&metrics),
                                metrics.last_applied.map(|applied| applied.index),
                            )
                        };
                        let marker = marker.inspect_err(|refusal| {
                            tracing::warn!(
                                %refusal,
                                "recovery: refused a snapshot install while the log lags"
                            );
                        })?;
                        let local = rejoin
                            .history_at_marker(history.applied(), applied_index, marker)
                            .await;
                        if let Some(refusal) = rejoin.committed_overwrite(
                            crate::rejoin::Incoming::Snapshot {
                                last: snapshot.meta.last_log_id,
                                history: incoming,
                                local,
                            },
                            marker,
                        ) {
                            tracing::error!(
                                %refusal,
                                "recovery: refused a snapshot install that would replace \
                                 committed entries"
                            );
                            rejoin.record_refused_snapshot(&snapshot.meta.snapshot_id, &refusal);
                            return Err(refusal);
                        }
                    }
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
            drop(_prefetch_guard);
            // Object-store publication and its retry backoff must not monopolize
            // the node-wide download/install budget. Keep the per-group lock
            // and engine lifetime until publication completes.
            drop(_permit);
            let references = registry.snapshot_install.references(raft_group_id.0);
            // The object the install replaced is pinned for the GC grace, and
            // the snapshot driver's next tick deletes what retired before
            // (#669).
            references.schedule_pass(crate::rt::time::Instant::now());
            // A publication of an engine that is shutting down stops: the
            // shutdown waits for this task, and the next engine publishes its
            // own current reference (#709).
            let publication = lifetime
                .until_stopping(
                    references.publish_current(&registry.snapshot_store(), raft_group_id.0),
                )
                .await
                .unwrap_or(Ok(()))
                .map_err(SnapshotInstallError::Publication);
            drop(lifetime);
            result.and_then(|response| publication.map(|()| response))
        })
        .await
        .map_err(SnapshotInstallError::Task)?
    }

    async fn prefetch_snapshot_for_install(
        &self,
        raft_group_id: RaftGroupId,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
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
                    snapshot: TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
                        meta: snapshot.meta,
                        snapshot: SnapshotData::new(pointer_bytes),
                    },
                    guard: None,
                    history: Some(history),
                });
            }
        };

        let snapshot_store = self.snapshot_store();
        let reference = self
            .snapshot_install
            .references(raft_group_id.0)
            .prepare(&snapshot_store, raft_group_id.0, &location)
            .await
            .map_err(|err| {
                // The pins go with P3's owned objects, and this site with them.
                GroupEngineError::new(format!(
                    "pin incoming snapshot before install: {}",
                    ErrorChain(&err)
                ))
            })?;
        let snapshot_bytes =
            snapshot_store
                .download(&location)
                .await
                .map_err(|source| match source {
                    ursula_runtime::SnapshotStoreError::NotFound { key } => {
                        SnapshotInstallError::SourceMissing {
                            group,
                            source_key: key,
                        }
                    }
                    source => SnapshotInstallError::SourceRead {
                        group,
                        source_key: source_key.clone(),
                        source,
                    },
                })?;
        let group_snapshot = decode_group_snapshot(&snapshot_bytes).map_err(|source| {
            SnapshotInstallError::Undecodable {
                group,
                snapshot_id: snapshot_id.clone(),
                source,
            }
        })?;
        drop(snapshot_bytes);
        let history = group_snapshot.stream_snapshot.history;
        // Keep fallible object-store I/O outside OpenRaft's state-machine
        // worker: a write-snapshot error there is fatal to RaftCore. Keep the
        // original external pointer so large snapshots are not duplicated in
        // the Raft RPC payload; install_snapshot consumes the decoded group.
        let mut pointer = SnapshotPointer {
            snapshot_id,
            location,
        };
        let cache_key =
            self.snapshot_install
                .cache_prefetched(&mut pointer, group_snapshot, reference);
        let guard = PrefetchedSnapshotGuard {
            snapshot_install: self.snapshot_install.clone(),
            cache_key,
        };
        let pointer_bytes = pointer
            .encode_binary()
            .map_err(|source| SnapshotInstallError::PointerEncode { group, source })?;
        Ok(PrefetchedInstallSnapshot {
            snapshot: TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
                meta: snapshot.meta,
                snapshot: SnapshotData::new(pointer_bytes),
            },
            guard: Some(guard),
            history: Some(history),
        })
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
        self.deliver_transfer(raft_group_id, &entry, request).await
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
        let observed = entry.raft().metrics().borrow_watched().clone();
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
        self.deliver_transfer(group, &entry, request)
            .await?
            .map_err(|source| SelfElectionError::Rejected { group, source })
    }

    /// Hands `request` to the entry's Raft once
    /// [`ElectionPolicy::validate_incoming_transfer`] accepts it, so the gate
    /// checked is the gate of the Raft that takes the transfer.
    async fn deliver_transfer(
        &self,
        group: RaftGroupId,
        entry: &ActiveGroup,
        request: TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> Result<openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig>, IncomingTransferError>
    {
        let raft = entry.raft();
        // Read first: a metrics guard held through the policy check would hold up the core's
        // next metrics update while the check takes the recovery gate's locks.
        let node_id = raft.metrics().borrow_watched().id;
        self.election.validate_incoming_transfer(
            group,
            node_id,
            &request,
            entry.recovery.as_deref(),
        )?;
        raft.handle_transfer_leader(request)
            .await
            .map_err(|source| IncomingTransferError::Raft { group, source })
    }

    /// Builds a snapshot outside OpenRaft's own build path, which OpenRaft
    /// never publishes: tests only, since the group's next purge waits for
    /// OpenRaft to publish every build (#669).
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn build_snapshot_for_transfer(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<TypeConfigSnapshotOf<UrsulaRaftTypeConfig>, SnapshotBuildError> {
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
    use std::time::Duration;

    use bytes::Bytes;
    use ursula_runtime::GroupSnapshot;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;

    use super::*;

    /// A real engine publishes its shared resources in one step.
    #[cfg_attr(madsim, madsim::test)]
    #[cfg_attr(not(madsim), tokio::test)]
    async fn registry_hands_out_the_registered_group_page_cache() {
        let check = async {
            let (registry, engine, _root) = reference_failure_engine(Arc::default()).await;
            let group = engine.placement.raft_group_id;
            let shared = registry.cold_index_cache(group).expect("registered cache");
            assert!(Arc::ptr_eq(
                &shared,
                engine.cold_index_cache.as_ref().unwrap()
            ));
            assert!(Arc::ptr_eq(
                &registry.entry(group).unwrap().barrier,
                &engine.read_barrier
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

    #[derive(Debug)]
    struct StaticSnapshotStore {
        bytes: Option<Vec<u8>>,
    }

    #[derive(Debug, Default)]
    struct FailingReferenceStore {
        fail_pin: std::sync::atomic::AtomicBool,
        fail_current: std::sync::atomic::AtomicBool,
        pins: Mutex<BTreeSet<String>>,
        current: Mutex<Option<String>>,
        pause_current: std::sync::atomic::AtomicBool,
        entered_current: crate::rt::sync::Notify,
        release_current: crate::rt::sync::Notify,
        downloads: std::sync::atomic::AtomicUsize,
        pause_download: std::sync::atomic::AtomicBool,
        entered_download: crate::rt::sync::Notify,
        release_download: crate::rt::sync::Notify,
    }

    impl SnapshotStore for FailingReferenceStore {
        fn upload<'a>(
            &'a self,
            _key: ursula_runtime::SnapshotKey,
            bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                Ok(SnapshotLocation::Inline {
                    bytes: bytes.to_vec(),
                })
            })
        }
        fn download<'a>(
            &'a self,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async {
                self.downloads.fetch_add(1, Ordering::SeqCst);
                if self.pause_download.load(Ordering::SeqCst) {
                    self.entered_download.notify_one();
                    self.release_download.notified().await;
                }
                Ok(group_snapshot_bytes())
            })
        }
        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
        fn pin_reference<'a>(
            &'a self,
            _group: u32,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                if self.fail_pin.load(Ordering::SeqCst) {
                    return Err(ursula_runtime::SnapshotStoreError::backend(
                        ursula_runtime::SnapshotStoreOperation::Put,
                        "test",
                        opendal::Error::new(
                            opendal::ErrorKind::Unexpected,
                            "temporary pin PUT outage",
                        ),
                    ));
                }
                if let SnapshotLocation::S3 { key, .. } = location {
                    self.pins.lock().unwrap().insert(key.clone());
                }
                Ok(())
            })
        }
        fn publish_reference<'a>(
            &'a self,
            _group: u32,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                if self.fail_current.load(Ordering::SeqCst) {
                    return Err(ursula_runtime::SnapshotStoreError::backend(
                        ursula_runtime::SnapshotStoreOperation::Put,
                        "test",
                        opendal::Error::new(
                            opendal::ErrorKind::Unexpected,
                            "temporary current-reference PUT outage",
                        ),
                    ));
                }
                if self.pause_current.load(Ordering::SeqCst) {
                    self.entered_current.notify_one();
                    self.release_current.notified().await;
                }
                *self.current.lock().unwrap() = match location {
                    SnapshotLocation::S3 { key, .. } => Some(key.clone()),
                    _ => None,
                };
                Ok(())
            })
        }
        fn reconcile_reference_pins<'a>(
            &'a self,
            _group: u32,
            retained: &'a [SnapshotLocation],
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let keys: BTreeSet<_> = retained
                    .iter()
                    .filter_map(|location| match location {
                        SnapshotLocation::S3 { key, .. } => Some(key.clone()),
                        _ => None,
                    })
                    .collect();
                self.pins.lock().unwrap().retain(|key| keys.contains(key));
                Ok(())
            })
        }

        ursula_runtime::inline_owned_snapshot_methods!();
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
                root: crate::log_store::SimDisk::provision_dir("snapshot-reference").unwrap(),
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
    async fn reference_failure_group(
        store: Arc<FailingReferenceStore>,
    ) -> (RaftGroupHandleRegistry, RaftGroupHandle, ReferenceWalRoot) {
        let (registry, engine, root) = reference_failure_engine(store).await;
        (registry, engine.raft_handle(), root)
    }

    async fn reference_failure_engine(
        store: Arc<FailingReferenceStore>,
    ) -> (
        RaftGroupHandleRegistry,
        crate::RaftGroupEngine,
        ReferenceWalRoot,
    ) {
        reference_failure_engine_for_group(store, RaftGroupId(7), Default::default()).await
    }

    async fn reference_failure_engine_for_group(
        store: Arc<FailingReferenceStore>,
        group: RaftGroupId,
        snapshot_install: SnapshotInstallCoordinator,
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
                ursula_runtime::RuntimeMetrics::new(1, 8).group_engine_metrics(),
            )
            .unwrap(),
            crate::RaftGroupEngineOptions {
                snapshot_store: Some(store),
                snapshot_install: Some(registry.snapshot_install_coordinator()),
                cold_store: Some(Arc::new(ursula_runtime::ColdStore::memory().unwrap())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        registry.register_engine(&engine);
        (registry, engine, wal_root)
    }

    fn reference_failure_snapshot() -> TypeConfigSnapshotOf<UrsulaRaftTypeConfig> {
        let mut snapshot = external_snapshot("reference-fault");
        let id = openraft::LogId::new(openraft::vote::RaftLeaderId::new(1, 2), 1);
        snapshot.meta.last_log_id = Some(id);
        snapshot.meta.last_membership =
            openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                Some(id),
                openraft::Membership::new(
                    vec![BTreeSet::from([1])],
                    BTreeMap::from([(1, openraft::BasicNode::new("local"))]),
                )
                .unwrap(),
            );
        snapshot
    }

    /// M1 of the third #581 review: a replica holding a recovery plan takes
    /// no full snapshot and no append from a leader at or below the plan
    /// term, a leader elected before the plan, and refuses it typed. A
    /// leader above the plan term is not fenced.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_replica_holding_a_plan_takes_no_snapshot_from_a_leader_before_it() {
        let store = Arc::new(FailingReferenceStore::default());
        let (registry, engine, _root) = reference_failure_engine(store).await;
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
            ursula_runtime::RuntimeMetrics::new(1, 8).group_engine_metrics(),
        )
        .unwrap();
        let gate = Arc::new(
            crate::GroupRejoin::durable(1, RaftGroupId(7), &gate_store)
                .await
                .expect("open the gate"),
        );
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
                    raft_group_id: RaftGroupId(7),
                    plan_term: 7,
                })
            )
        };
        for term in [5, 7] {
            let installed = registry
                .install_full_snapshot(
                    RaftGroupId(7),
                    crate::UrsulaVote::new_committed(term, 2),
                    reference_failure_snapshot(),
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
        let store = Arc::new(FailingReferenceStore::default());
        let (registry, old, _old_root) = reference_failure_engine(store.clone()).await;
        let serial = registry.snapshot_install.install_lock(7).lock_owned().await;
        let request = registry.install_full_snapshot(
            RaftGroupId(7),
            crate::UrsulaVote::new_committed(1, 2),
            reference_failure_snapshot(),
        );
        tokio::pin!(request);
        assert!(futures_util::poll!(&mut request).is_pending());
        old.shutdown().await.unwrap();
        let (_, replacement, _new_root) = reference_failure_engine_for_group(
            store,
            RaftGroupId(7),
            registry.snapshot_install_coordinator(),
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

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn snapshot_reference_put_failures_retry_without_restarting_raft() {
        snapshot_reference_put_failures_retry_without_restarting_raft_body().await;
    }
    #[cfg(madsim)]
    #[test]
    fn snapshot_reference_put_failures_retry_without_restarting_raft() {
        crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
            crate::sim_runtime::MadsimOpenRaftRuntime::scope(
                7,
                snapshot_reference_put_failures_retry_without_restarting_raft_body(),
            )
            .await
        });
    }
    async fn snapshot_reference_put_failures_retry_without_restarting_raft_body() {
        for fail_pin in [true, false] {
            let store = Arc::new(FailingReferenceStore::default());
            store.fail_pin.store(fail_pin, Ordering::SeqCst);
            store.fail_current.store(!fail_pin, Ordering::SeqCst);
            let (registry, raft, _wal_root) = reference_failure_group(store.clone()).await;
            let first = registry
                .install_full_snapshot(
                    RaftGroupId(7),
                    crate::types::UrsulaVote::new_committed(1, 2),
                    reference_failure_snapshot(),
                )
                .await;
            first.expect_err("a failing reference store must reject the snapshot install");
            let metrics = raft.metrics().borrow_watched().clone();
            assert!(
                metrics.running_state.is_ok(),
                "transient object-store failure must not kill RaftCore"
            );
            if fail_pin {
                assert!(
                    metrics.last_applied.is_none(),
                    "pin failure must precede installation"
                );
                assert!(store.pins.lock().unwrap().is_empty());
            } else {
                assert_eq!(metrics.last_applied.unwrap().index(), 1);
                assert!(store.pins.lock().unwrap().contains("reference-fault.snap"));
                let snapshot = raft.get_snapshot().await.unwrap().unwrap();
                let pointer = SnapshotPointer::decode(snapshot.snapshot.pointer_bytes()).unwrap();
                assert_eq!(pointer.snapshot_id, "reference-fault");
                assert_eq!(
                    snapshot.snapshot.pointer_bytes(),
                    reference_failure_snapshot().snapshot.pointer_bytes()
                );
                assert!(
                    matches!(pointer.location, SnapshotLocation::S3 { .. }),
                    "temporary reference failure must not retain a full inline snapshot"
                );
            }
            let retry_started = crate::rt::time::Instant::now();
            store.fail_pin.store(false, Ordering::SeqCst);
            store.fail_current.store(false, Ordering::SeqCst);
            registry
                .install_full_snapshot(
                    RaftGroupId(7),
                    crate::types::UrsulaVote::new_committed(1, 2),
                    reference_failure_snapshot(),
                )
                .await
                .unwrap();
            assert_eq!(
                store.downloads.load(Ordering::SeqCst),
                1,
                "publication retry must reuse the installed snapshot"
            );
            if !fail_pin {
                assert!(
                    retry_started.elapsed() >= Duration::from_millis(90),
                    "publication retry must back off"
                );
            }
            assert_eq!(
                store.current.lock().unwrap().as_deref(),
                Some("reference-fault.snap")
            );
            assert_eq!(store.pins.lock().unwrap().len(), 1);
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
                .client_write(ursula_runtime::GroupWriteCommand::Stream(
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
                crate::RaftGroupResponse::Write(Ok(
                    ursula_runtime::GroupWriteResponse::CreateStream(_)
                ))
            ));
            raft.metrics()
                .borrow_watched()
                .running_state
                .as_ref()
                .expect("raft core must keep running after the snapshot retry");
            raft.shutdown().await.unwrap();
        }
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn slow_reference_publication_does_not_block_another_groups_install() {
        let coordinator = SnapshotInstallCoordinator::new(1);
        let slow_store = Arc::new(FailingReferenceStore::default());
        slow_store.pause_current.store(true, Ordering::SeqCst);
        let (slow_registry, slow_engine, _slow_root) = reference_failure_engine_for_group(
            slow_store.clone(),
            RaftGroupId(7),
            coordinator.clone(),
        )
        .await;
        let (fast_registry, fast_engine, _fast_root) =
            reference_failure_engine_for_group(Arc::default(), RaftGroupId(6), coordinator).await;
        let slow = crate::rt::spawn(async move {
            slow_registry
                .install_full_snapshot(
                    RaftGroupId(7),
                    crate::types::UrsulaVote::new_committed(1, 2),
                    reference_failure_snapshot(),
                )
                .await
        });
        slow_store.entered_current.notified().await;
        // Cancellation must still retain the per-group publication and lifetime,
        // but the sole node-wide install permit belongs to neither I/O nor backoff.
        slow.abort();
        assert!(slow.await.unwrap_err().is_cancelled());
        let mut group = decode_group_snapshot(&group_snapshot_bytes()).unwrap();
        group.placement.raft_group_id = RaftGroupId(6);
        let bytes = crate::snapshot_codec::group_snapshot_frames(Arc::new(group))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect();
        let mut snapshot = reference_failure_snapshot();
        snapshot.snapshot = SnapshotData::new(
            SnapshotPointer {
                snapshot_id: snapshot.meta.snapshot_id.clone(),
                location: SnapshotLocation::Inline { bytes },
            }
            .encode_binary()
            .unwrap(),
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            fast_registry.install_full_snapshot(
                RaftGroupId(6),
                crate::types::UrsulaVote::new_committed(1, 2),
                snapshot,
            ),
        )
        .await
        .expect("another group must install while publication remains paused")
        .unwrap();
        assert_eq!(
            fast_engine
                .raft_handle()
                .metrics()
                .borrow_watched()
                .last_applied
                .unwrap()
                .index(),
            1
        );
        slow_store.pause_current.store(false, Ordering::SeqCst);
        slow_store.release_current.notify_one();
        slow_engine.shutdown().await.unwrap();
        fast_engine.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn shutdown_stops_a_canceled_install_before_reopened_group_publication() {
        let store = Arc::new(FailingReferenceStore::default());
        store.pause_current.store(true, Ordering::SeqCst);
        let (registry, engine, _root) = reference_failure_engine(store.clone()).await;
        let request = crate::rt::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .install_full_snapshot(
                        RaftGroupId(7),
                        crate::types::UrsulaVote::new_committed(1, 2),
                        reference_failure_snapshot(),
                    )
                    .await
            }
        });
        store.entered_current.notified().await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        let raft = engine.raft_handle();
        let shutdown = crate::rt::spawn(async move { engine.shutdown().await });
        raft.wait(Some(Duration::from_secs(2)))
            .metrics(
                |metrics| matches!(metrics.state, openraft::ServerState::Shutdown),
                "Raft stops before the admitted install's publication ends",
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
                        reference_failure_snapshot(),
                    )
                    .await
            }
        });
        // The shutdown stops the admitted install's paused publication
        // instead of waiting for it, and returns once the task ended (#709).
        shutdown.await.unwrap().unwrap();
        assert!(matches!(
            refused.await.unwrap().unwrap_err(),
            SnapshotInstallError::ShuttingDown { .. }
        ));
        store.pause_current.store(false, Ordering::SeqCst);
        store.release_current.notify_one();
        // Reopen against the same external reference namespace. No canceled
        // task from the retired engine may publish or reconcile behind this.
        let (replacement_registry, replacement, _replacement_root) =
            reference_failure_engine(store.clone()).await;
        let mut snapshot = reference_failure_snapshot();
        snapshot.snapshot = external_snapshot("replacement").snapshot;
        replacement_registry
            .install_full_snapshot(
                RaftGroupId(7),
                crate::types::UrsulaVote::new_committed(1, 2),
                snapshot,
            )
            .await
            .unwrap();
        assert_eq!(
            store.current.lock().unwrap().as_deref(),
            Some("replacement.snap")
        );
        assert!(store.pins.lock().unwrap().contains("replacement.snap"));
        replacement.shutdown().await.unwrap();
    }
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn duplicate_prefetch_guards_have_independent_entries() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
            bytes: Some(group_snapshot_bytes()),
        })));
        let first = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("same"))
            .await
            .unwrap();
        let second = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("same"))
            .await
            .unwrap();
        let first_pointer =
            SnapshotPointer::decode(first.snapshot.snapshot.pointer_bytes()).unwrap();
        let second_pointer =
            SnapshotPointer::decode(second.snapshot.snapshot.pointer_bytes()).unwrap();
        assert_ne!(first_pointer.snapshot_id, second_pointer.snapshot_id);
        drop(first);
        assert!(
            registry
                .snapshot_install
                .take_prefetched(&second_pointer)
                .is_some()
        );
    }
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn cancelled_snapshot_rpc_keeps_install_alive_and_serializes_duplicate() {
        let store = Arc::new(FailingReferenceStore::default());
        store.pause_download.store(true, Ordering::SeqCst);
        let (registry, raft, _wal_root) = reference_failure_group(store.clone()).await;
        let request = crate::rt::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .install_full_snapshot(
                        RaftGroupId(7),
                        crate::types::UrsulaVote::new_committed(1, 2),
                        reference_failure_snapshot(),
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
                reference_failure_snapshot(),
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
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn rejected_snapshot_releases_its_pin_without_publishing_a_current_pointer() {
        rejected_snapshot_releases_its_pin_without_publishing_a_current_pointer_body().await;
    }
    #[cfg(madsim)]
    #[test]
    fn rejected_snapshot_releases_its_pin_without_publishing_a_current_pointer() {
        crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
            crate::sim_runtime::MadsimOpenRaftRuntime::scope(
                7,
                rejected_snapshot_releases_its_pin_without_publishing_a_current_pointer_body(),
            )
            .await
        });
    }
    async fn rejected_snapshot_releases_its_pin_without_publishing_a_current_pointer_body() {
        let store = Arc::new(FailingReferenceStore::default());
        let (registry, raft, _wal_root) = reference_failure_group(store.clone()).await;
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
                reference_failure_snapshot(),
            )
            .await
            .unwrap();
        assert!(raft.metrics().borrow_watched().last_applied.is_none());
        assert!(store.current.lock().unwrap().is_none());
        assert!(store.pins.lock().unwrap().is_empty());
        raft.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn lagging_reference_put_keeps_the_new_current_and_every_prepared_pointer_pinned() {
        lagging_reference_put_keeps_the_new_current_and_every_prepared_pointer_pinned_body().await;
    }
    #[cfg(madsim)]
    #[test]
    fn lagging_reference_put_keeps_the_new_current_and_every_prepared_pointer_pinned() {
        crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
            crate::sim_runtime::MadsimOpenRaftRuntime::scope(
                7,
                lagging_reference_put_keeps_the_new_current_and_every_prepared_pointer_pinned_body(
                ),
            )
            .await
        });
    }
    async fn lagging_reference_put_keeps_the_new_current_and_every_prepared_pointer_pinned_body() {
        let store = Arc::new(FailingReferenceStore::default());
        let shared: SharedSnapshotStore = store.clone();
        let references = Arc::new(crate::snapshot_references::SnapshotReferences::new(
            std::time::Duration::from_secs(60),
        ));
        let location = |id: &str| {
            SnapshotPointer::decode(external_snapshot(id).snapshot.pointer_bytes())
                .unwrap()
                .location
        };
        let old = location("old-current");
        let incoming = location("new-current");
        let pending = location("pending-build");
        let old_lease = references.prepare(&shared, 7, &old).await.unwrap();
        references.commit_current(&old);
        drop(old_lease);
        let new_lease = references.prepare(&shared, 7, &incoming).await.unwrap();
        let pending_lease = references.prepare(&shared, 7, &pending).await.unwrap();
        store.pause_current.store(true, Ordering::SeqCst);
        let publication = crate::rt::spawn({
            let references = references.clone();
            let shared = shared.clone();
            async move { references.publish_current(&shared, 7).await }
        });
        store.entered_current.notified().await;
        // Installation performs no reference I/O and can finish while the
        // old reference PUT is still blocked outside RaftCore.
        references.commit_current(&incoming);
        drop(new_lease);
        store.release_current.notify_one();
        publication.await.unwrap().unwrap();
        assert_eq!(
            store.current.lock().unwrap().as_deref(),
            Some("old-current.snap")
        );
        // The replaced current stays pinned while it retires (#669).
        assert_eq!(
            *store.pins.lock().unwrap(),
            BTreeSet::from([
                "new-current.snap".to_owned(),
                "old-current.snap".to_owned(),
                "pending-build.snap".to_owned(),
            ])
        );
        store.pause_current.store(false, Ordering::SeqCst);
        drop(pending_lease);
        references.publish_current(&shared, 7).await.unwrap();
        assert_eq!(
            store.current.lock().unwrap().as_deref(),
            Some("new-current.snap")
        );
        assert_eq!(
            *store.pins.lock().unwrap(),
            BTreeSet::from(["new-current.snap".to_owned(), "old-current.snap".to_owned()])
        );
        // Once its pin expires, the next publication drops it.
        let expired = crate::rt::time::Instant::now()
            .checked_add(std::time::Duration::from_secs(61))
            .unwrap();
        assert!(references.gc_due(expired));
        assert!(references.expire_retiring(expired));
        references.publish_current(&shared, 7).await.unwrap();
        assert_eq!(
            *store.pins.lock().unwrap(),
            BTreeSet::from(["new-current.snap".to_owned()])
        );
    }

    impl SnapshotStore for StaticSnapshotStore {
        fn upload<'a>(
            &'a self,
            _key: ursula_runtime::SnapshotKey,
            bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                Ok(SnapshotLocation::Inline {
                    bytes: bytes.to_vec(),
                })
            })
        }

        fn download<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            let bytes = self.bytes.clone();
            Box::pin(async move {
                bytes.ok_or_else(|| SnapshotStoreError::NotFound {
                    key: crate::state_machine::object_key(location).expect("an external location"),
                })
            })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        ursula_runtime::inline_owned_snapshot_methods!();
    }

    fn snapshot_meta(snapshot_id: &str) -> openraft::alias::SnapshotMetaOf<UrsulaRaftTypeConfig> {
        openraft::alias::SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: None,
            last_membership: openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::default(),
            snapshot_id: snapshot_id.to_owned(),
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

    fn external_snapshot(snapshot_id: &str) -> TypeConfigSnapshotOf<UrsulaRaftTypeConfig> {
        let pointer = SnapshotPointer {
            snapshot_id: snapshot_id.to_owned(),
            location: SnapshotLocation::S3 {
                key: format!("{snapshot_id}.snap"),
                size_bytes: 1,
                stored_size_bytes: 1,
                compression: ursula_runtime::SnapshotCompression::None,
                shared_object: false,
            },
        };
        TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
            meta: snapshot_meta(snapshot_id),
            snapshot: SnapshotData::new(pointer.encode_binary().expect("encode test pointer")),
        }
    }

    #[tokio::test]
    async fn prefetch_snapshot_for_install_caches_external_snapshot() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
            bytes: Some(group_snapshot_bytes()),
        })));

        let prefetched = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("snapshot-a"))
            .await
            .expect("prefetch external snapshot");

        let pointer =
            SnapshotPointer::decode(prefetched.snapshot.snapshot.pointer_bytes()).unwrap();
        assert_ne!(
            pointer.snapshot_id, "snapshot-a",
            "each admitted handoff has its own transient token"
        );
        assert!(matches!(pointer.location, SnapshotLocation::S3 { .. }));
        let cached = registry
            .snapshot_install_coordinator()
            .take_prefetched(&pointer)
            .expect("external snapshot is cached for install");
        assert_eq!(cached.snapshot.group_commit_index, 0);
    }

    #[tokio::test]
    async fn prefetch_snapshot_for_install_installs_without_snapshot_store_download() {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
            bytes: Some(group_snapshot_bytes()),
        })));

        let prefetched = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("snapshot-drop"))
            .await
            .expect("prefetch external snapshot");
        let coordinator = registry.snapshot_install_coordinator();

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let mut state_machine = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            Arc::new(StaticSnapshotStore { bytes: None }),
            SnapshotBuildCoordinator::default(),
            coordinator,
            None,
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
    }

    #[tokio::test]
    async fn prefetch_snapshot_for_install_guard_clears_unconsumed_cache() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
            bytes: Some(group_snapshot_bytes()),
        })));

        let prefetched = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("snapshot-unconsumed"))
            .await
            .expect("prefetch external snapshot");
        let pointer =
            SnapshotPointer::decode(prefetched.snapshot.snapshot.pointer_bytes()).unwrap();
        drop(prefetched);

        assert!(
            registry
                .snapshot_install_coordinator()
                .take_prefetched(&pointer)
                .is_none(),
            "unconsumed prefetched snapshot should be cleared when guard drops"
        );
    }

    #[tokio::test]
    async fn prefetch_snapshot_for_install_keeps_prefetch_failure_outside_openraft() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore { bytes: None })));

        let err = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("missing"))
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
                SnapshotInstallError::ShuttingDown { .. }
                | SnapshotInstallError::Publication(_)
                | SnapshotInstallError::Task(_) => (Code::Unavailable, true, false),
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
                SnapshotInstallError::PointerUndecodable { .. } => {
                    (Code::InvalidArgument, false, false)
                }
                SnapshotInstallError::Undecodable { .. } => (Code::DataLoss, false, false),
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
                    raft_group_id: group,
                    committed: 2,
                    index: 1,
                },
            )),
            SnapshotInstallError::Group(GroupEngineError::Infra(
                GroupInfraError::RaftGroupNotRegistered {
                    raft_group_id: group,
                },
            )),
            SnapshotInstallError::Group(GroupEngineError::new("internal")),
            SnapshotInstallError::Group(GroupEngineError::Infra(GroupInfraError::OwnerStopped)),
            SnapshotInstallError::Publication(store_error()),
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

    #[tokio::test]
    async fn prefetch_snapshot_for_install_does_not_download_inline_snapshot() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore { bytes: None })));
        let pointer = SnapshotPointer {
            snapshot_id: "inline".to_owned(),
            location: SnapshotLocation::Inline {
                bytes: group_snapshot_bytes(),
            },
        };
        let snapshot = TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
            meta: snapshot_meta("inline"),
            snapshot: SnapshotData::new(pointer.encode_binary().expect("encode test pointer")),
        };

        let prefetched = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), snapshot)
            .await
            .expect("inline snapshot does not touch snapshot store");
        let pointer =
            SnapshotPointer::decode(prefetched.snapshot.snapshot.pointer_bytes()).unwrap();
        assert!(matches!(pointer.location, SnapshotLocation::Inline { .. }));
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
