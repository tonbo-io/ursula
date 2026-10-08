//! In-process fault-injection network for tests and deterministic simulation.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
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
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;

use crate::rejoin::GroupRejoin;
use crate::rejoin::RecoveryGateStatus;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug, Clone, Default)]
pub struct InProcessRaftRegistry {
    endpoints:
        Arc<Mutex<BTreeMap<u64, (ursula_shard::RaftGroupId, crate::RaftGroupHandleRegistry)>>>,
    nodes: Arc<Mutex<BTreeMap<u64, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
    barriers: Arc<Mutex<BTreeMap<u64, Arc<crate::read_index::ReadIndexBarrier>>>>,
    full_snapshot_calls: Arc<Mutex<BTreeMap<u64, usize>>>,
    /// Each node's recovery gate, screening the votes and appends delivered
    /// to it.
    rejoins: Arc<Mutex<BTreeMap<u64, Arc<GroupRejoin>>>>,
}

impl InProcessRaftRegistry {
    pub fn register(&self, node_id: u64, engine: &crate::RaftGroupEngine) {
        let raft = engine.raft_handle();
        self.barriers
            .lock()
            .expect("in-process barrier mutex")
            .insert(node_id, engine.read_barrier.clone());
        let endpoint = crate::RaftGroupHandleRegistry::default();
        endpoint.register_engine(engine, self.rejoin(node_id));
        self.endpoints
            .lock()
            .expect("endpoint registry")
            .insert(node_id, (engine.placement.raft_group_id, endpoint));
        self.nodes
            .lock()
            .expect("in-process raft registry mutex")
            .insert(node_id, raft);
    }

    pub fn unregister(
        &self,
        node_id: u64,
    ) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.barriers
            .lock()
            .expect("in-process barrier mutex")
            .remove(&node_id);
        self.endpoints
            .lock()
            .expect("endpoint registry")
            .remove(&node_id);
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

    pub async fn confirm_recovery_barrier(
        &self,
        node_id: u64,
        group: ursula_shard::RaftGroupId,
    ) -> Result<(crate::UrsulaVote, u64), crate::QuorumProofError> {
        let barrier = self
            .barriers
            .lock()
            .expect("in-process barrier mutex")
            .get(&node_id)
            .cloned()
            .ok_or(crate::QuorumProofError::NotRegistered { group })?;
        crate::registry::confirm_recovery_barrier(group, barrier.owner(), barrier.as_ref()).await
    }

    fn endpoint(
        &self,
        node_id: u64,
    ) -> Option<(ursula_shard::RaftGroupId, crate::RaftGroupHandleRegistry)> {
        self.endpoints
            .lock()
            .expect("endpoint registry")
            .get(&node_id)
            .cloned()
    }

    /// Production inbound vote dispatch, also used by simulated recovery probes.
    pub async fn vote(
        &self,
        node_id: u64,
        request: crate::UrsulaVoteRequest,
    ) -> Result<crate::UrsulaVoteResponse, ursula_runtime::GroupEngineError> {
        let (group, endpoint) =
            self.endpoint(node_id)
                .ok_or(ursula_runtime::GroupEngineError::Infra(
                    ursula_runtime::GroupInfraError::OwnerStopped,
                ))?;
        endpoint.vote(group, request).await
    }

    /// Screen the votes and appends delivered to `node_id` through its
    /// recovery gate (replaces a previous registration).
    pub fn register_rejoin(
        &self,
        node_id: u64,
        engine: &crate::RaftGroupEngine,
        rejoin: Arc<GroupRejoin>,
    ) {
        self.rejoins
            .lock()
            .expect("in-process raft rejoin mutex")
            .insert(node_id, rejoin);
        self.register(node_id, engine);
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
        let (group, target) = self.registry.endpoint(self.target).ok_or_else(|| {
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
        let leader = rpc.vote;
        let prev_log_id = rpc.prev_log_id;
        let sent_last_log_id = rpc.entries.last().map(|entry| entry.log_id);
        let response = target.append_entries(group, rpc).await.map_err(|err| {
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
        let (group, target) = self.registry.endpoint(self.target).ok_or_else(|| {
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
        let response = target.vote(group, rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!("remote vote: {err}")))
        })?;
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
        let (group, target) = self.registry.endpoint(self.target).ok_or_else(|| {
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
            .install_full_snapshot(group, vote, snapshot)
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
    ) -> Result<
        openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig>,
        RPCError<UrsulaRaftTypeConfig>,
    > {
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
                .is_some_and(|rejoin| !crate::ElectionPolicy::default().may_campaign(Some(&rejoin)))
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
