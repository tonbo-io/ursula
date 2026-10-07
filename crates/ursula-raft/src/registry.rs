use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::future::join_all;
use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;
use tokio::sync::watch;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::GroupEngineError;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::default_snapshot_store;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::log_store::LaggingGroups;
use crate::log_store::RecoveryState;
use crate::log_store::WalOpening;
use crate::meta::MetaRaftTypeConfig;
use crate::read_index::ReadIndexBarrier;
use crate::rejoin::AcceptUnsyncedLossOutcome;
use crate::rejoin::GroupRejoin;
use crate::rejoin::RecoveryGateError;
use crate::rejoin::RecoveryGateStatus;
use crate::snapshot_codec::decode_group_snapshot;
use crate::state_machine::RaftGroupStateMachine;
use crate::state_machine::SnapshotBuildCoordinator;
use crate::state_machine::SnapshotInstallCoordinator;
use crate::types::RaftGroupMetricsSnapshot;
use crate::types::RaftLogProgressSnapshot;
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
    ) -> Result<(), RPCError<UrsulaRaftTypeConfig>> {
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
    ) -> Result<(), RPCError<MetaRaftTypeConfig>> {
        unreachable!("single-node meta raft group must not transfer leadership")
    }
}

#[derive(Debug, Clone, Default)]
pub struct InProcessRaftRegistry {
    nodes: Arc<Mutex<BTreeMap<u64, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
    full_snapshot_calls: Arc<Mutex<BTreeMap<u64, usize>>>,
    /// Each node's recovery gate, screening the votes and appends delivered
    /// to it.
    rejoins: Arc<Mutex<BTreeMap<u64, Arc<GroupRejoin>>>>,
}

impl InProcessRaftRegistry {
    pub fn register(&self, node_id: u64, raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>) {
        self.nodes
            .lock()
            .expect("in-process raft registry mutex")
            .insert(node_id, raft);
    }

    pub fn unregister(
        &self,
        node_id: u64,
    ) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.nodes
            .lock()
            .expect("in-process raft registry mutex")
            .remove(&node_id)
    }

    pub fn get(&self, node_id: u64) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.nodes
            .lock()
            .expect("in-process raft registry mutex")
            .get(&node_id)
            .cloned()
    }

    /// Screen the votes and appends delivered to `node_id` through its
    /// recovery gate (replaces a previous registration).
    pub fn register_rejoin(&self, node_id: u64, rejoin: Arc<GroupRejoin>) {
        self.rejoins
            .lock()
            .expect("in-process raft rejoin mutex")
            .insert(node_id, rejoin);
    }

    pub fn rejoin(&self, node_id: u64) -> Option<Arc<GroupRejoin>> {
        self.rejoins
            .lock()
            .expect("in-process raft rejoin mutex")
            .get(&node_id)
            .cloned()
    }

    pub fn full_snapshot_count(&self, node_id: u64) -> usize {
        self.full_snapshot_calls
            .lock()
            .expect("in-process raft full snapshot calls mutex")
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }

    fn record_full_snapshot(&self, node_id: u64) {
        let mut calls = self
            .full_snapshot_calls
            .lock()
            .expect("in-process raft full snapshot calls mutex");
        let count = calls.entry(node_id).or_insert(0);
        *count = count.saturating_add(1);
    }
}

#[derive(Debug, Clone)]
pub struct InProcessRaftNetworkFactory {
    registry: InProcessRaftRegistry,
    source: Option<u64>,
    policy: InProcessRaftNetworkPolicy,
    rejoin: Option<Arc<GroupRejoin>>,
}

impl InProcessRaftNetworkFactory {
    pub fn new(registry: InProcessRaftRegistry) -> Self {
        Self {
            registry,
            source: None,
            policy: InProcessRaftNetworkPolicy::default(),
            rejoin: None,
        }
    }

    /// The sending node's recovery gate: replication answers that show a
    /// follower lost its log go to it instead of to OpenRaft.
    pub fn with_rejoin(mut self, rejoin: Arc<GroupRejoin>) -> Self {
        self.rejoin = Some(rejoin);
        self
    }

    pub fn with_source(mut self, source: u64) -> Self {
        self.source = Some(source);
        self
    }

    pub fn with_policy(mut self, policy: InProcessRaftNetworkPolicy) -> Self {
        self.policy = policy;
        self
    }
}

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for InProcessRaftNetworkFactory {
    type Network = InProcessRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        InProcessRaftNetwork {
            source: self.source,
            target,
            registry: self.registry.clone(),
            policy: self.policy.clone(),
            rejoin: self.rejoin.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InProcessRaftRpcKind {
    AppendEntries,
    Vote,
    FullSnapshot,
    TransferLeader,
}

#[derive(Debug, Clone)]
pub enum InProcessRaftNetworkPolicyEvent {
    SetDelay(Option<Duration>),
    /// Retained for external observers (ursula-sim introspection) even though
    /// no in-crate policy method emits one-way events today.
    PartitionOneWay {
        source: u64,
        target: u64,
    },
    PartitionBidirectional {
        a: u64,
        b: u64,
    },
    /// Retained for external observers (ursula-sim introspection) even though
    /// no in-crate policy method emits one-way events today.
    HealOneWay {
        source: u64,
        target: u64,
    },
    HealBidirectional {
        a: u64,
        b: u64,
    },
    Clear,
}

#[derive(Debug, Clone)]
pub enum InProcessRaftNetworkEvent {
    PolicyChanged {
        action: InProcessRaftNetworkPolicyEvent,
    },
    RpcDecision {
        source: Option<u64>,
        target: u64,
        kind: InProcessRaftRpcKind,
        delay: Option<Duration>,
        partitioned: bool,
    },
    RpcDelivered {
        source: Option<u64>,
        target: u64,
        kind: InProcessRaftRpcKind,
    },
    RpcMissingTarget {
        source: Option<u64>,
        target: u64,
        kind: InProcessRaftRpcKind,
    },
    /// The target answered a vote request; `granted` when it accepted and
    /// saved the candidate's vote. `target_gate` is the target's recovery
    /// gate when the request reached it, if it has one.
    VoteAnswered {
        source: Option<u64>,
        target: u64,
        granted: bool,
        target_gate: Option<RecoveryGateStatus>,
    },
}

type InProcessRaftNetworkObserver = Arc<dyn Fn(InProcessRaftNetworkEvent) + Send + Sync>;

#[derive(Clone, Default)]
pub struct InProcessRaftNetworkPolicy {
    inner: Arc<Mutex<InProcessRaftNetworkPolicyState>>,
    observer: Arc<Mutex<Option<InProcessRaftNetworkObserver>>>,
}

#[derive(Debug, Default)]
struct InProcessRaftNetworkPolicyState {
    delay: Option<Duration>,
    partitions: BTreeSet<(u64, u64)>,
}

impl Debug for InProcessRaftNetworkPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessRaftNetworkPolicy")
            .finish_non_exhaustive()
    }
}

impl InProcessRaftNetworkPolicy {
    pub fn set_observer(
        &self,
        observer: impl Fn(InProcessRaftNetworkEvent) + Send + Sync + 'static,
    ) {
        *self
            .observer
            .lock()
            .expect("in-process raft network observer mutex") = Some(Arc::new(observer));
    }

    pub fn set_delay(&self, delay: Option<Duration>) {
        self.inner
            .lock()
            .expect("in-process raft network policy mutex")
            .delay = delay;
        self.notify_policy_changed(InProcessRaftNetworkPolicyEvent::SetDelay(delay));
    }

    pub fn partition_bidirectional(&self, a: u64, b: u64) {
        let mut inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        inner.partitions.insert((a, b));
        inner.partitions.insert((b, a));
        self.notify_policy_changed(InProcessRaftNetworkPolicyEvent::PartitionBidirectional {
            a,
            b,
        });
    }

    pub fn heal_bidirectional(&self, a: u64, b: u64) {
        let mut inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        inner.partitions.remove(&(a, b));
        inner.partitions.remove(&(b, a));
        self.notify_policy_changed(InProcessRaftNetworkPolicyEvent::HealBidirectional { a, b });
    }

    pub fn clear(&self) {
        let mut inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        inner.delay = None;
        inner.partitions.clear();
        self.notify_policy_changed(InProcessRaftNetworkPolicyEvent::Clear);
    }

    /// Whether `source` and `target` are cut off from each other in either
    /// direction. Calls that reach a peer outside the Raft network, such as
    /// the recovery gate's barrier and bootstrap probes, ask it so a
    /// partition stops them too.
    pub fn partitioned(&self, source: u64, target: u64) -> bool {
        let inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        inner.partitions.contains(&(source, target)) || inner.partitions.contains(&(target, source))
    }

    fn decision(
        &self,
        source: Option<u64>,
        target: u64,
        kind: InProcessRaftRpcKind,
    ) -> InProcessRaftNetworkDecision {
        let inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        let partitioned = source.is_some_and(|source| inner.partitions.contains(&(source, target)));
        InProcessRaftNetworkDecision {
            source,
            target,
            kind,
            delay: inner.delay,
            partitioned,
        }
    }

    fn notify(&self, event: InProcessRaftNetworkEvent) {
        let observer = self
            .observer
            .lock()
            .expect("in-process raft network observer mutex")
            .clone();
        if let Some(observer) = observer {
            observer(event);
        }
    }

    fn notify_policy_changed(&self, action: InProcessRaftNetworkPolicyEvent) {
        self.notify(InProcessRaftNetworkEvent::PolicyChanged { action });
    }
}

#[derive(Debug, Clone)]
pub struct InProcessRaftFaultScript {
    seed: u64,
    steps: Vec<InProcessRaftFaultStep>,
}

impl InProcessRaftFaultScript {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            steps: Vec::new(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn push(&mut self, phase: impl Into<String>, action: InProcessRaftFaultAction) {
        self.steps.push(InProcessRaftFaultStep {
            phase: phase.into(),
            action,
        });
    }

    pub fn apply_phase(&self, phase: &str, policy: &InProcessRaftNetworkPolicy) {
        for step in &self.steps {
            if step.phase == phase {
                step.action.apply(policy);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InProcessRaftFaultStep {
    pub phase: String,
    pub action: InProcessRaftFaultAction,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InProcessRaftFaultAction {
    SetDelay(Option<Duration>),
    PartitionBidirectional { a: u64, b: u64 },
    HealBidirectional { a: u64, b: u64 },
    Clear,
}

impl InProcessRaftFaultAction {
    fn apply(self, policy: &InProcessRaftNetworkPolicy) {
        match self {
            Self::SetDelay(delay) => policy.set_delay(delay),
            Self::PartitionBidirectional { a, b } => policy.partition_bidirectional(a, b),
            Self::HealBidirectional { a, b } => policy.heal_bidirectional(a, b),
            Self::Clear => policy.clear(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct InProcessRaftNetworkDecision {
    source: Option<u64>,
    target: u64,
    kind: InProcessRaftRpcKind,
    delay: Option<Duration>,
    partitioned: bool,
}

impl InProcessRaftNetworkDecision {
    fn partition_error(&self) -> String {
        format!(
            "in-process raft {:?} from {} to {} is partitioned",
            self.kind,
            self.source
                .map(|source| source.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            self.target
        )
    }
}

#[derive(Debug, Clone)]
pub struct InProcessRaftNetwork {
    source: Option<u64>,
    target: u64,
    registry: InProcessRaftRegistry,
    policy: InProcessRaftNetworkPolicy,
    rejoin: Option<Arc<GroupRejoin>>,
}

impl InProcessRaftNetwork {
    fn missing_target_error(&self) -> Unreachable<UrsulaRaftTypeConfig> {
        Unreachable::from_string(format!(
            "in-process raft node {} is not registered",
            self.target
        ))
    }

    async fn before_rpc(
        &self,
        kind: InProcessRaftRpcKind,
    ) -> Result<(), Unreachable<UrsulaRaftTypeConfig>> {
        let decision = self.policy.decision(self.source, self.target, kind);
        self.policy.notify(InProcessRaftNetworkEvent::RpcDecision {
            source: decision.source,
            target: decision.target,
            kind: decision.kind,
            delay: decision.delay,
            partitioned: decision.partitioned,
        });
        if decision.partitioned {
            return Err(Unreachable::from_string(decision.partition_error()));
        }
        if let Some(delay) = decision.delay {
            sleep_in_process_raft_network(delay).await;
        }
        Ok(())
    }

    async fn before_streaming_rpc(
        &self,
        kind: InProcessRaftRpcKind,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
    ) -> Result<(), StreamingError<UrsulaRaftTypeConfig>> {
        let decision = self.policy.decision(self.source, self.target, kind);
        self.policy.notify(InProcessRaftNetworkEvent::RpcDecision {
            source: decision.source,
            target: decision.target,
            kind: decision.kind,
            delay: decision.delay,
            partitioned: decision.partitioned,
        });
        if decision.partitioned {
            return Err(StreamingError::Unreachable(Unreachable::from_string(
                decision.partition_error(),
            )));
        }
        if let Some(delay) = decision.delay {
            let sleep = sleep_in_process_raft_network(delay);
            futures_util::pin_mut!(sleep);
            futures_util::pin_mut!(cancel);
            match futures_util::future::select(sleep, cancel).await {
                futures_util::future::Either::Left((_done, _cancel)) => {}
                futures_util::future::Either::Right((closed, _sleep)) => {
                    return Err(StreamingError::Closed(closed));
                }
            }
        }
        Ok(())
    }
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for InProcessRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        self.before_rpc(InProcessRaftRpcKind::AppendEntries)
            .await
            .map_err(RPCError::Unreachable)?;
        let target = self.registry.get(self.target).ok_or_else(|| {
            self.policy
                .notify(InProcessRaftNetworkEvent::RpcMissingTarget {
                    source: self.source,
                    target: self.target,
                    kind: InProcessRaftRpcKind::AppendEntries,
                });
            RPCError::Unreachable(self.missing_target_error())
        })?;
        self.policy.notify(InProcessRaftNetworkEvent::RpcDelivered {
            source: self.source,
            target: self.target,
            kind: InProcessRaftRpcKind::AppendEntries,
        });
        if let Some(target_rejoin) = self.registry.rejoin(self.target) {
            target_rejoin.observe_inbound_append(&rpc);
        }
        let leader = rpc.vote;
        let prev_log_id = rpc.prev_log_id;
        let sent_last_log_id = rpc.entries.last().map(|entry| entry.log_id);
        let response = target.append_entries(rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote AppendEntries on node {}: {err}",
                self.target
            )))
        })?;
        if let Some(rejoin) = &self.rejoin
            && rejoin.follower_lost_log(
                self.target,
                &leader,
                prev_log_id.as_ref(),
                sent_last_log_id.as_ref(),
                &response,
            )
        {
            return Err(RPCError::Network(NetworkError::from_string(format!(
                "node {} lost Raft log entries it had acknowledged",
                self.target
            ))));
        }
        Ok(response)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        self.before_rpc(InProcessRaftRpcKind::Vote)
            .await
            .map_err(RPCError::Unreachable)?;
        let target = self.registry.get(self.target).ok_or_else(|| {
            self.policy
                .notify(InProcessRaftNetworkEvent::RpcMissingTarget {
                    source: self.source,
                    target: self.target,
                    kind: InProcessRaftRpcKind::Vote,
                });
            RPCError::Unreachable(self.missing_target_error())
        })?;
        self.policy.notify(InProcessRaftNetworkEvent::RpcDelivered {
            source: self.source,
            target: self.target,
            kind: InProcessRaftRpcKind::Vote,
        });
        let target_rejoin = self.registry.rejoin(self.target);
        let target_gate = target_rejoin.as_ref().map(|rejoin| rejoin.status());
        let response = match target_rejoin.and_then(|rejoin| rejoin.screen_vote(&rpc)) {
            Some(refusal) => refusal,
            None => target.vote(rpc).await.map_err(|err| {
                RPCError::Network(NetworkError::from_string(format!(
                    "remote Vote on node {}: {err}",
                    self.target
                )))
            })?,
        };
        self.policy.notify(InProcessRaftNetworkEvent::VoteAnswered {
            source: self.source,
            target: self.target,
            granted: response.vote_granted,
            target_gate,
        });
        Ok(response)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        self.before_streaming_rpc(InProcessRaftRpcKind::FullSnapshot, cancel)
            .await?;
        self.registry.record_full_snapshot(self.target);
        let target = self.registry.get(self.target).ok_or_else(|| {
            self.policy
                .notify(InProcessRaftNetworkEvent::RpcMissingTarget {
                    source: self.source,
                    target: self.target,
                    kind: InProcessRaftRpcKind::FullSnapshot,
                });
            StreamingError::Unreachable(self.missing_target_error())
        })?;
        self.policy.notify(InProcessRaftNetworkEvent::RpcDelivered {
            source: self.source,
            target: self.target,
            kind: InProcessRaftRpcKind::FullSnapshot,
        });
        target
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| {
                StreamingError::Network(NetworkError::from_string(format!(
                    "remote full snapshot on node {}: {err}",
                    self.target
                )))
            })
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<(), RPCError<UrsulaRaftTypeConfig>> {
        self.before_rpc(InProcessRaftRpcKind::TransferLeader)
            .await
            .map_err(RPCError::Unreachable)?;
        let target = self.registry.get(self.target).ok_or_else(|| {
            self.policy
                .notify(InProcessRaftNetworkEvent::RpcMissingTarget {
                    source: self.source,
                    target: self.target,
                    kind: InProcessRaftRpcKind::TransferLeader,
                });
            RPCError::Unreachable(self.missing_target_error())
        })?;
        self.policy.notify(InProcessRaftNetworkEvent::RpcDelivered {
            source: self.source,
            target: self.target,
            kind: InProcessRaftRpcKind::TransferLeader,
        });
        if *req.to_node_id() == self.target
            && self
                .registry
                .rejoin(self.target)
                .is_some_and(|rejoin| !rejoin.may_campaign())
        {
            return Err(RPCError::Network(NetworkError::from_string(
                "the recovery gate is closed; refusing leadership transfer",
            )));
        }
        target.handle_transfer_leader(req).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote TransferLeader on node {}: {err}",
                self.target
            )))
        })
    }
}

#[cfg(madsim)]
async fn sleep_in_process_raft_network(delay: Duration) {
    sim_tokio::time::sleep(delay).await;
}

#[cfg(not(madsim))]
async fn sleep_in_process_raft_network(delay: Duration) {
    std::thread::sleep(delay);
}

pub type LeadershipShedFlag = Arc<AtomicU8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipShedReason {
    SnapshotDriverS3 = 0b001,
    ClusterEgress = 0b010,
    ColdHealth = 0b100,
    MaintenanceDrain = 0b1000,
    WalDiskPressure = 0b1_0000,
}

impl LeadershipShedReason {
    const ALL: [Self; 5] = [
        Self::SnapshotDriverS3,
        Self::ClusterEgress,
        Self::ColdHealth,
        Self::MaintenanceDrain,
        Self::WalDiskPressure,
    ];

    const fn bit(self) -> u8 {
        self as u8
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotDriverS3 => "snapshot-driver-s3",
            Self::ClusterEgress => "cluster-egress",
            Self::ColdHealth => "cold-health",
            Self::MaintenanceDrain => "maintenance-drain",
            Self::WalDiskPressure => "wal-disk-pressure",
        }
    }
}

impl fmt::Display for LeadershipShedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct LeadershipShedState: u8 {
        const SNAPSHOT_DRIVER_S3 = LeadershipShedReason::SnapshotDriverS3.bit();
        const CLUSTER_EGRESS = LeadershipShedReason::ClusterEgress.bit();
        const COLD_HEALTH = LeadershipShedReason::ColdHealth.bit();
        const MAINTENANCE_DRAIN = LeadershipShedReason::MaintenanceDrain.bit();
        const WAL_DISK_PRESSURE = LeadershipShedReason::WalDiskPressure.bit();
    }
}

impl From<LeadershipShedReason> for LeadershipShedState {
    fn from(reason: LeadershipShedReason) -> Self {
        Self::from_bits_truncate(reason.bit())
    }
}

impl LeadershipShedState {
    pub fn load(flag: &LeadershipShedFlag) -> Self {
        Self::from_bits_truncate(flag.load(Ordering::Acquire))
    }

    pub fn is_shed(self) -> bool {
        !self.is_empty()
    }

    /// Whether this node should accept an inbound TransferLeader request.
    ///
    /// Only cluster-egress impairment blocks this. Snapshot/cold-health
    /// impairments are cold-path pressure signals; refusing inbound transfer
    /// for those states can deadlock the balancer when every peer has a
    /// transient cold-path bit set.
    pub fn should_accept_transfer(self) -> bool {
        !self.intersects(Self::CLUSTER_EGRESS | Self::MAINTENANCE_DRAIN | Self::WAL_DISK_PRESSURE)
    }

    /// Whether local raft groups should be allowed to campaign.
    ///
    /// Cluster-egress and local S3 snapshot-driver impairment disable
    /// campaigning. Cold-health is softer: the node should shed excess current
    /// leadership, but it must remain electable so a cluster-wide hot backlog
    /// cannot exclude every node from leadership.
    pub fn should_campaign(self) -> bool {
        !self.intersects(
            Self::CLUSTER_EGRESS
                | Self::SNAPSHOT_DRIVER_S3
                | Self::MAINTENANCE_DRAIN
                | Self::WAL_DISK_PRESSURE,
        )
    }

    /// Whether local raft groups should actively move current leadership away.
    pub fn should_shed_current_leaders(self) -> bool {
        self.is_shed()
    }

    pub fn transfer_rejection_reason(self) -> Option<LeadershipShedReason> {
        if self.contains(Self::CLUSTER_EGRESS) {
            Some(LeadershipShedReason::ClusterEgress)
        } else if self.contains(Self::MAINTENANCE_DRAIN) {
            Some(LeadershipShedReason::MaintenanceDrain)
        } else if self.contains(Self::WAL_DISK_PRESSURE) {
            Some(LeadershipShedReason::WalDiskPressure)
        } else {
            None
        }
    }
}

impl fmt::Display for LeadershipShedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote = false;
        for reason in LeadershipShedReason::ALL {
            if self.contains(reason.into()) {
                if wrote {
                    f.write_str("|")?;
                }
                fmt::Display::fmt(&reason, f)?;
                wrote = true;
            }
        }
        if !wrote {
            f.write_str("none")?;
        }
        Ok(())
    }
}

/// Owned handle to a single Raft group's [`Raft`] instance, as stored in the
/// [`RaftGroupHandleRegistry`]. Spelled out as a type alias so callers (e.g. the
/// HTTP admin handlers) can name it without repeating the full type-config
/// generics.
pub type RaftGroupHandle = Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>;

/// A group's shared cold-index page cache.
pub type GroupColdIndexCache = Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>;

#[derive(Debug, Clone)]
pub struct RaftGroupHandleRegistry {
    groups: Arc<Mutex<BTreeMap<u32, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
    /// Each group's cold-index page cache, the one its state machine
    /// invalidates on apply, so forwarded gRPC reads share it (F13).
    cold_index_caches: Arc<Mutex<BTreeMap<u32, GroupColdIndexCache>>>,
    /// Each group's coalescing ReadIndex barrier, so forwarded gRPC reads
    /// share confirmation rounds with the group's local reads.
    read_barriers: Arc<Mutex<BTreeMap<u32, Arc<ReadIndexBarrier>>>>,
    /// Recovery gate per group: the vote gate and the followers a leader saw
    /// lose their log.
    rejoins: Arc<Mutex<BTreeMap<u32, Arc<GroupRejoin>>>>,
    dynamic_hosted_groups: Arc<Mutex<BTreeSet<RaftGroupId>>>,
    leadership_shed: LeadershipShedFlag,
    transport_shutdown: watch::Sender<bool>,
    snapshot_store: Arc<Mutex<SharedSnapshotStore>>,
    snapshot_build: Arc<Mutex<SnapshotBuildCoordinator>>,
    snapshot_install: SnapshotInstallCoordinator,
    /// How this node's Raft WAL opened, when its logs are durable.
    wal_opening: Arc<Mutex<Option<WalOpening>>>,
    /// The groups this node's WAL reports lagging, when its logs are durable.
    wal_lagging_groups: Arc<Mutex<Option<Arc<LaggingGroups>>>>,
}

impl Default for RaftGroupHandleRegistry {
    fn default() -> Self {
        let (transport_shutdown, _) = watch::channel(false);
        Self {
            groups: Arc::new(Mutex::new(BTreeMap::new())),
            cold_index_caches: Arc::new(Mutex::new(BTreeMap::new())),
            read_barriers: Arc::new(Mutex::new(BTreeMap::new())),
            rejoins: Arc::new(Mutex::new(BTreeMap::new())),
            dynamic_hosted_groups: Arc::new(Mutex::new(BTreeSet::new())),
            leadership_shed: Arc::new(AtomicU8::new(0)),
            transport_shutdown,
            snapshot_store: Arc::new(Mutex::new(default_snapshot_store())),
            snapshot_build: Arc::new(Mutex::new(SnapshotBuildCoordinator::new(1))),
            snapshot_install: SnapshotInstallCoordinator::new(1),
            wal_opening: Arc::new(Mutex::new(None)),
            wal_lagging_groups: Arc::new(Mutex::new(None)),
        }
    }
}

#[derive(Debug)]
struct PrefetchedInstallSnapshot {
    snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    guard: Option<PrefetchedSnapshotGuard>,
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

impl RaftGroupHandleRegistry {
    /// Records how this node's Raft WAL opened.
    pub fn set_wal_opening(&self, opening: WalOpening) {
        *self
            .wal_opening
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(opening);
    }

    /// How this node's Raft WAL opened; `None` until a WAL is attached.
    pub fn wal_opening(&self) -> Option<WalOpening> {
        *self
            .wal_opening
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Attaches the lagging groups this node's Raft WAL reports.
    pub fn set_wal_lagging_groups(&self, lagging: Arc<LaggingGroups>) {
        *self
            .wal_lagging_groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(lagging);
    }

    /// The groups whose live records keep old journal segments alive, which
    /// the snapshot driver snapshots first; `None` until a WAL is attached.
    pub fn wal_lagging_groups(&self) -> Option<Arc<LaggingGroups>> {
        self.wal_lagging_groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Whether this node's Raft logs may be missing entries it acknowledged;
    /// `None` until a WAL is attached.
    pub fn wal_recovery_state(&self) -> Option<RecoveryState> {
        self.wal_opening().map(|opening| opening.recovery)
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
            .lock()
            .expect("raft group handle registry mutex")
            .iter()
            .map(|(id, raft)| (*id, raft.clone()))
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

    pub(crate) fn subscribe_transport_shutdown(&self) -> watch::Receiver<bool> {
        self.transport_shutdown.subscribe()
    }

    pub fn register(
        &self,
        placement: ShardPlacement,
        raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    ) {
        // The leadership-shed flag may be installed before groups are warmed
        // (notably URSULA_START_MAINTENANCE_DRAINED during a replacement).
        // Apply the same election policy while holding the registry lock so a
        // concurrent shed transition cannot miss a newly registered group.
        let mut groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex");
        raft.runtime_config().elect(
            self.leadership_shed_state().should_campaign()
                && self
                    .rejoin(placement.raft_group_id)
                    .is_none_or(|rejoin| rejoin.may_campaign()),
        );
        groups.insert(placement.raft_group_id.0, raft);
    }

    /// Records the group's shared cold-index page cache, so forwarded reads
    /// use the cache that apply-time invalidation reaches (bounded-state F13).
    pub fn register_cold_index_cache(
        &self,
        raft_group_id: RaftGroupId,
        cache: Option<GroupColdIndexCache>,
    ) {
        let mut caches = self
            .cold_index_caches
            .lock()
            .expect("raft group cold index cache mutex");
        match cache {
            Some(cache) => {
                caches.insert(raft_group_id.0, cache);
            }
            None => {
                caches.remove(&raft_group_id.0);
            }
        }
    }

    /// Records the group engine's ReadIndex barrier.
    pub(crate) fn register_read_barrier(
        &self,
        raft_group_id: RaftGroupId,
        barrier: Arc<ReadIndexBarrier>,
    ) {
        self.read_barriers
            .lock()
            .expect("raft group read barrier mutex")
            .insert(raft_group_id.0, barrier);
    }

    /// The group's ReadIndex barrier, if its engine registered one.
    pub(crate) fn read_barrier(&self, raft_group_id: RaftGroupId) -> Option<Arc<ReadIndexBarrier>> {
        self.read_barriers
            .lock()
            .expect("raft group read barrier mutex")
            .get(&raft_group_id.0)
            .cloned()
    }

    /// Records the group's recovery gate. Register it before the Raft
    /// handle, so no vote reaches the group unscreened.
    pub fn register_rejoin(&self, raft_group_id: RaftGroupId, rejoin: Arc<GroupRejoin>) {
        self.rejoins
            .lock()
            .expect("raft group rejoin mutex")
            .insert(raft_group_id.0, rejoin);
    }

    /// The group's recovery gate, if it has one.
    pub fn rejoin(&self, raft_group_id: RaftGroupId) -> Option<Arc<GroupRejoin>> {
        self.rejoins
            .lock()
            .expect("raft group rejoin mutex")
            .get(&raft_group_id.0)
            .cloned()
    }

    /// Whether `target` lost its log under this node's leadership of the
    /// group (leadership handoffs skip such a follower).
    pub fn is_reverted_follower(&self, raft_group_id: RaftGroupId, target: u64) -> bool {
        self.rejoin(raft_group_id)
            .is_some_and(|rejoin| rejoin.is_reverted_follower(target))
    }

    /// Operator recovery when a majority of the group's voters are gated:
    /// open this node's recovery gate for the group, accepting that its
    /// replica may be missing entries it acknowledged, and let the group
    /// campaign again (see [`GroupRejoin::accept_unsynced_loss`]).
    pub async fn accept_unsynced_loss(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<AcceptUnsyncedLossOutcome, RecoveryGateError> {
        let rejoin = self
            .rejoin(raft_group_id)
            .filter(|_| self.contains_group(raft_group_id))
            .ok_or(RecoveryGateError::NotRegistered { raft_group_id })?;
        let outcome = rejoin.accept_unsynced_loss().await?;
        self.refresh_group_elections(raft_group_id);
        Ok(outcome)
    }

    /// The recovery gate of every group that has one on this node.
    pub fn recovery_gates(&self) -> BTreeMap<u32, RecoveryGateStatus> {
        self.rejoins
            .lock()
            .expect("raft group rejoin mutex")
            .iter()
            .map(|(group, rejoin)| (*group, rejoin.status()))
            .collect()
    }

    /// Groups whose gated replica on this node got no leader barrier and
    /// applied nothing for [`crate::RECOVERY_STALL_AFTER`]: they wait for an
    /// operator to accept the loss of the unsynced tail.
    pub fn stalled_recovery_groups(&self) -> Vec<u32> {
        self.recovery_gates()
            .into_iter()
            .filter(|(_, status)| *status == RecoveryGateStatus::Stalled)
            .map(|(group, _)| group)
            .collect()
    }

    /// The group's shared cold-index page cache, if one was registered.
    pub fn cold_index_cache(&self, raft_group_id: RaftGroupId) -> Option<GroupColdIndexCache> {
        self.cold_index_caches
            .lock()
            .expect("raft group cold index cache mutex")
            .get(&raft_group_id.0)
            .cloned()
    }

    pub fn get(&self, raft_group_id: RaftGroupId) -> Option<RaftGroupHandle> {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .get(&raft_group_id.0)
            .cloned()
    }

    pub fn contains_group(&self, raft_group_id: RaftGroupId) -> bool {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .contains_key(&raft_group_id.0)
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
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn with_snapshot_install_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.snapshot_install = SnapshotInstallCoordinator::new(max_concurrency);
        self
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

    pub fn leadership_shed_flag(&self) -> LeadershipShedFlag {
        self.leadership_shed.clone()
    }

    pub fn leadership_shed_state(&self) -> LeadershipShedState {
        LeadershipShedState::load(&self.leadership_shed)
    }

    pub fn mark_leadership_shed(&self, reason: LeadershipShedReason) {
        let previous = self
            .leadership_shed
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
            .leadership_shed
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
        let groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex");
        for (group, raft) in groups.iter() {
            raft.runtime_config().elect(
                self.leadership_shed_state().should_campaign()
                    && self
                        .rejoin(RaftGroupId(*group))
                        .is_none_or(|rejoin| rejoin.may_campaign()),
            );
        }
    }

    /// Serialize recovery-barrier changes with maintenance policy updates.
    pub(crate) fn refresh_group_elections(&self, group: RaftGroupId) {
        let groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex");
        if let Some(raft) = groups.get(&group.0) {
            raft.runtime_config().elect(
                self.leadership_shed_state().should_campaign()
                    && self
                        .rejoin(group)
                        .is_none_or(|rejoin| rejoin.may_campaign()),
            );
        }
    }

    pub fn is_leadership_shed(&self) -> bool {
        self.leadership_shed_state().is_shed()
    }

    /// A node cannot receive a planned leadership handoff until the recovery
    /// gate of every registered group is open.
    pub fn recovery_barriers_ready(&self) -> bool {
        self.rejoins
            .lock()
            .expect("raft group rejoin registry mutex")
            .values()
            .all(|rejoin| rejoin.may_campaign())
    }

    pub fn metrics_snapshot(&self) -> Vec<RaftGroupMetricsSnapshot> {
        let groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex")
            .iter()
            .map(|(raft_group_id, raft)| (*raft_group_id, raft.clone()))
            .collect::<Vec<_>>();

        let log_progress = self.snapshot_build_coordinator().log_progress();
        let mut snapshots = Vec::with_capacity(groups.len());
        for (raft_group_id, raft) in groups {
            let log = log_progress
                .get(&raft_group_id)
                .copied()
                .unwrap_or_default();
            let metrics = raft.metrics().borrow_watched().clone();
            let membership = metrics.membership_config.membership();
            snapshots.push(RaftGroupMetricsSnapshot {
                raft_group_id,
                node_id: metrics.id,
                current_term: metrics.current_term,
                current_leader: metrics.current_leader,
                last_log_index: metrics.last_log_index,
                committed: metrics.committed.map(log_progress_snapshot),
                last_applied: metrics.last_applied.map(log_progress_snapshot),
                snapshot: metrics.snapshot.map(log_progress_snapshot),
                purged: metrics.purged.map(log_progress_snapshot),
                voter_ids: membership.voter_ids().collect(),
                learner_ids: membership.learner_ids().collect(),
                maintenance: crate::types::RaftGroupMaintenanceState {
                    running: metrics.running_state.is_ok()
                        && metrics.state != openraft::ServerState::Shutdown,
                    recovery_ready: self
                        .rejoin(RaftGroupId(raft_group_id))
                        .is_none_or(|rejoin| rejoin.vote_gate_open()),
                    accepting_transfers: self.leadership_shed_state().should_accept_transfer(),
                    membership_joint: membership.get_joint_config().len() != 1,
                    membership_log_index: metrics.membership_config.log_id().map(|id| id.index()),
                    stopped_for_operator: self
                        .rejoin(RaftGroupId(raft_group_id))
                        .is_some_and(|rejoin| rejoin.status() == RecoveryGateStatus::Stalled),
                },
                log,
            });
        }
        snapshots
    }

    pub async fn append_entries(
        &self,
        raft_group_id: RaftGroupId,
        request: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        if let Some(rejoin) = self.rejoin(raft_group_id) {
            rejoin.observe_inbound_append(&request);
        }
        raft.append_entries(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft AppendEntries: {err}")))
    }

    pub async fn vote(
        &self,
        raft_group_id: RaftGroupId,
        request: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        if let Some(refusal) = self
            .rejoin(raft_group_id)
            .and_then(|rejoin| rejoin.screen_vote(&request))
        {
            return Ok(refusal);
        }
        raft.vote(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft Vote: {err}")))
    }

    pub async fn install_full_snapshot(
        &self,
        raft_group_id: RaftGroupId,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        let _install_permit = self.snapshot_install.acquire().await?;
        let prefetched = self
            .prefetch_snapshot_for_install(raft_group_id, snapshot)
            .await?;
        let snapshot = prefetched.snapshot;
        let _prefetch_guard = prefetched.guard;
        let result = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft install snapshot: {err}")));
        drop(_prefetch_guard);
        let publication = self
            .snapshot_install
            .references(raft_group_id.0)
            .publish_current(&self.snapshot_store(), raft_group_id.0)
            .await
            .map_err(|err| {
                GroupEngineError::new(format!("publish installed snapshot reference: {err}"))
            });
        // Publication errors remain ordinary transport errors. The accepted
        // pointer stays pinned, and a repeated install retries publication.
        result.and_then(|response| publication.map(|()| response))
    }

    async fn prefetch_snapshot_for_install(
        &self,
        raft_group_id: RaftGroupId,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    ) -> Result<PrefetchedInstallSnapshot, GroupEngineError> {
        let pointer_bytes = snapshot.snapshot.into_inner();
        let pointer = SnapshotPointer::decode(&pointer_bytes).map_err(|err| {
            GroupEngineError::new(format!("decode OpenRaft snapshot pointer: {err}"))
        })?;
        let SnapshotPointer {
            snapshot_id,
            location,
        } = pointer;
        if matches!(location, SnapshotLocation::Inline { .. }) {
            return Ok(PrefetchedInstallSnapshot {
                snapshot: TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
                    meta: snapshot.meta,
                    snapshot: Cursor::new(pointer_bytes),
                },
                guard: None,
            });
        }

        let snapshot_store = self.snapshot_store();
        let reference = self
            .snapshot_install
            .references(raft_group_id.0)
            .prepare(&snapshot_store, raft_group_id.0, &location)
            .await
            .map_err(|err| {
                GroupEngineError::new(format!("pin incoming snapshot before install: {err}"))
            })?;
        let snapshot_bytes = snapshot_store.download(&location).await.map_err(|err| {
            GroupEngineError::new(format!(
                "prefetch OpenRaft snapshot {snapshot_id} before install: {err}"
            ))
        })?;
        let group_snapshot = decode_group_snapshot(&snapshot_bytes).map_err(|err| {
            GroupEngineError::new(format!(
                "decode prefetched OpenRaft snapshot {snapshot_id}: {err}"
            ))
        })?;
        drop(snapshot_bytes);
        // Keep fallible object-store I/O outside OpenRaft's state-machine
        // worker: a write-snapshot error there is fatal to RaftCore. Keep the
        // original external pointer so large snapshots are not duplicated in
        // the Raft RPC payload; install_snapshot consumes the decoded group.
        let pointer = SnapshotPointer {
            snapshot_id,
            location,
        };
        let cache_key = self.snapshot_install.cache_prefetched(
            &pointer.snapshot_id,
            &pointer.location,
            group_snapshot,
            reference,
        );
        let guard = PrefetchedSnapshotGuard {
            snapshot_install: self.snapshot_install.clone(),
            cache_key,
        };
        let pointer_bytes = pointer.encode_binary().map_err(|err| {
            GroupEngineError::new(format!(
                "encode prefetched OpenRaft snapshot pointer: {err}"
            ))
        })?;
        Ok(PrefetchedInstallSnapshot {
            snapshot: TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
                meta: snapshot.meta,
                snapshot: Cursor::new(pointer_bytes),
            },
            guard: Some(guard),
        })
    }

    pub async fn handle_transfer_leader(
        &self,
        raft_group_id: RaftGroupId,
        request: TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> Result<(), GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        if *request.to_node_id() == raft.metrics().borrow_watched().id
            && (!self.leadership_shed_state().should_campaign()
                || self
                    .rejoin(raft_group_id)
                    .is_some_and(|rejoin| !rejoin.may_campaign()))
        {
            return Err(GroupEngineError::new(
                "the recovery gate is closed; refusing leadership transfer",
            ));
        }
        raft.handle_transfer_leader(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft handle_transfer_leader: {err}")))
    }

    pub async fn build_snapshot_for_transfer(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<TypeConfigSnapshotOf<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        let snapshot = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    let mut builder = state_machine.get_snapshot_builder().await;
                    builder.build_snapshot().await
                })
            })
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft build snapshot: {err}")))?
            .map_err(|err| GroupEngineError::new(format!("build OpenRaft snapshot: {err}")))?;
        Ok(snapshot)
    }

    fn require_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>, GroupEngineError> {
        self.get(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "raft group {} is not registered on this node",
                raft_group_id.0
            ))
        })
    }
}

pub(crate) fn log_progress_snapshot(
    log_id: LogIdOf<UrsulaRaftTypeConfig>,
) -> RaftLogProgressSnapshot {
    RaftLogProgressSnapshot {
        term: log_id.leader_id.term,
        index: log_id.index,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ursula_runtime::GroupSnapshot;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;

    use super::*;

    /// F13: forwarded gRPC reads look up the group's shared page cache here.
    #[test]
    fn registry_hands_out_the_registered_group_page_cache() {
        let registry = RaftGroupHandleRegistry::default();
        let group = ursula_shard::RaftGroupId(3);
        assert!(registry.cold_index_cache(group).is_none());
        let cache: GroupColdIndexCache = Arc::new(ColdIndexPageCache::new(
            Arc::new(ColdStoreColdIndexPageStore::new(Arc::new(
                ursula_runtime::ColdStore::memory().expect("memory cold store"),
            ))),
            8,
        ));
        registry.register_cold_index_cache(group, Some(cache.clone()));
        let shared = registry.cold_index_cache(group).expect("registered cache");
        assert!(Arc::ptr_eq(&shared, &cache));
        registry.register_cold_index_cache(group, None);
        assert!(registry.cold_index_cache(group).is_none());
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
            Box::pin(async { Ok(group_snapshot_bytes()) })
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
                    return Err(SnapshotStoreError::Backend(
                        "temporary pin PUT outage".to_owned(),
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
                    return Err(SnapshotStoreError::Backend(
                        "temporary current-reference PUT outage".to_owned(),
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
    }

    async fn reference_failure_group(
        store: Arc<FailingReferenceStore>,
    ) -> (RaftGroupHandleRegistry, RaftGroupHandle, tempfile::TempDir) {
        let wal_root = tempfile::tempdir().unwrap();
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(store.clone()));
        let placement = ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let state_machine = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            store,
            SnapshotBuildCoordinator::default(),
            registry.snapshot_install_coordinator(),
            None,
        );
        let config = Arc::new(
            openraft::Config {
                enable_tick: false,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = RaftGroupHandle::new(
            1,
            config,
            SingleNodeRaftNetworkFactory,
            crate::engine::DurableRaftLogStoreFactory::start(
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
            state_machine,
        )
        .await
        .unwrap();
        registry.register(placement, raft.clone());
        (registry, raft, wal_root)
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

    #[tokio::test]
    async fn snapshot_reference_put_failures_retry_without_restarting_raft() {
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
                let pointer = SnapshotPointer::decode(snapshot.snapshot.get_ref()).unwrap();
                assert!(
                    matches!(pointer.location, SnapshotLocation::S3 { .. }),
                    "temporary reference failure must not retain a full inline snapshot"
                );
            }
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
                store.current.lock().unwrap().as_deref(),
                Some("reference-fault.snap")
            );
            assert_eq!(store.pins.lock().unwrap().len(), 1);
            // Same Raft handle resumes an election and committed application.
            raft.trigger().elect().await.unwrap();
            raft.wait(Some(Duration::from_secs(2)))
                .current_leader(1, "recovered group elects")
                .await
                .unwrap();
            raft.client_write(ursula_runtime::GroupWriteCommand::Stream(
                ursula_stream::StreamCommand::CreateBucket {
                    bucket_id: "after-retry".to_owned(),
                },
            ))
            .await
            .unwrap();
            raft.metrics()
                .borrow_watched()
                .running_state
                .as_ref()
                .expect("raft core must keep running after the snapshot retry");
            raft.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn rejected_snapshot_releases_its_pin_without_publishing_a_current_pointer() {
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

    #[tokio::test]
    async fn lagging_reference_put_keeps_the_new_current_and_every_prepared_pointer_pinned() {
        let store = Arc::new(FailingReferenceStore::default());
        let shared: SharedSnapshotStore = store.clone();
        let references = Arc::new(crate::snapshot_references::SnapshotReferences::default());
        let location = |id: &str| {
            SnapshotPointer::decode(external_snapshot(id).snapshot.get_ref())
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
        assert_eq!(
            *store.pins.lock().unwrap(),
            BTreeSet::from([
                "new-current.snap".to_owned(),
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
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            let bytes = self.bytes.clone();
            Box::pin(async move {
                bytes.ok_or_else(|| SnapshotStoreError::NotFound("test snapshot missing".into()))
            })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }
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
            snapshot: Cursor::new(pointer.encode_binary().expect("encode test pointer")),
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

        let pointer = SnapshotPointer::decode(prefetched.snapshot.snapshot.get_ref()).unwrap();
        assert_eq!(pointer.snapshot_id, "snapshot-a");
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
        let pointer = SnapshotPointer::decode(prefetched.snapshot.snapshot.get_ref()).unwrap();
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

        assert!(err.message().contains("prefetch OpenRaft snapshot missing"));
        assert!(err.message().contains("snapshot not found"));
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
            snapshot: Cursor::new(pointer.encode_binary().expect("encode test pointer")),
        };

        let prefetched = registry
            .prefetch_snapshot_for_install(RaftGroupId(7), snapshot)
            .await
            .expect("inline snapshot does not touch snapshot store");
        let pointer = SnapshotPointer::decode(&prefetched.snapshot.snapshot.into_inner()).unwrap();
        assert!(matches!(pointer.location, SnapshotLocation::Inline { .. }));
    }

    #[test]
    fn leadership_shed_policy_splits_transfer_from_campaigning() {
        let empty = LeadershipShedState::default();
        assert!(empty.should_accept_transfer());
        assert!(empty.should_campaign());
        assert!(!empty.should_shed_current_leaders());
        assert_eq!(empty.transfer_rejection_reason(), None);

        let snapshot_shed = LeadershipShedState::from(LeadershipShedReason::SnapshotDriverS3);
        assert!(snapshot_shed.should_accept_transfer());
        assert!(!snapshot_shed.should_campaign());
        assert!(snapshot_shed.should_shed_current_leaders());
        assert_eq!(snapshot_shed.transfer_rejection_reason(), None);

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
        assert!(registry.leadership_shed_state().should_accept_transfer());

        registry.clear_leadership_shed(LeadershipShedReason::ColdHealth);
        registry.clear_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
        assert!(!registry.is_leadership_shed());
    }
}
