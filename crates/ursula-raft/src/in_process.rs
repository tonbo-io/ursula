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

use crate::registry::SnapshotInstallError;
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
}

impl InProcessRaftRegistry {
    pub fn register(&self, node_id: u64, engine: &crate::RaftGroupEngine) {
        let raft = engine.raft_handle();
        self.barriers
            .lock()
            .expect("in-process barrier mutex")
            .insert(node_id, engine.read_barrier.clone());
        let endpoint = crate::RaftGroupHandleRegistry::default();
        endpoint.register_engine(engine);
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

    /// The barrier RPC's server side on `node_id`, through the node's
    /// production registry: a leader whose history is pending confirms none.
    pub async fn confirm_recovery_barrier(
        &self,
        node_id: u64,
        group: ursula_shard::RaftGroupId,
    ) -> Result<(crate::UrsulaVote, u64), crate::QuorumProofError> {
        let (_, endpoint) = self
            .endpoint(node_id)
            .ok_or(crate::QuorumProofError::NotRegistered { group })?;
        endpoint.confirm_recovery_barrier(group).await
    }

    /// The `accept-unsynced-loss` admin route on `node_id`, through the
    /// node's production registry: the replica takes the recovery plan.
    pub async fn accept_recovery_plan(
        &self,
        node_id: u64,
        plan: &ursula_proto::admin::RecoveryPlanRequest,
        restart: &mut dyn crate::ReplicaRestart,
    ) -> Result<ursula_proto::admin::RecoveryPlanReport, crate::RecoveryGateError> {
        let (group, endpoint) =
            self.endpoint(node_id)
                .ok_or(crate::RecoveryGateError::NotRegistered {
                    raft_group_id: ursula_shard::RaftGroupId(0),
                })?;
        endpoint.accept_recovery_plan(group, plan, restart).await
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

    pub fn rejoin(&self, node_id: u64) -> Option<Arc<GroupRejoin>> {
        let (group, endpoint) = self.endpoint(node_id)?;
        endpoint.rejoin(group)
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
    /// How long appends that carry entries take from one node to another
    /// ([`InProcessRaftNetworkPolicy::set_entries_delay`]).
    entries_delays: BTreeMap<(u64, u64), Duration>,
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

    /// Holds every append that carries entries from `source` to `target`
    /// for `delay`, or no longer with `None`. Heartbeats carry none and pass
    /// at once, so `target` keeps its leader while its log falls behind, and
    /// a leader's request to it stays in flight for `delay`.
    pub fn set_entries_delay(&self, source: u64, target: u64, delay: Option<Duration>) {
        let mut inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        match delay {
            Some(delay) => {
                inner.entries_delays.insert((source, target), delay);
            }
            None => {
                inner.entries_delays.remove(&(source, target));
            }
        }
    }

    /// How long an append that carries entries takes from `source` to
    /// `target`.
    fn entries_delay(&self, source: Option<u64>, target: u64) -> Option<Duration> {
        let source = source?;
        self.inner
            .lock()
            .expect("in-process raft network policy mutex")
            .entries_delays
            .get(&(source, target))
            .copied()
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
        inner.entries_delays.clear();
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

    async fn before_streaming_rpc<F>(
        &self,
        kind: InProcessRaftRpcKind,
        cancel: std::pin::Pin<&mut F>,
    ) -> Result<(), StreamingError<UrsulaRaftTypeConfig>>
    where
        F: Future<Output = ReplicationClosed> + OptionalSend,
    {
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
        if !rpc.entries.is_empty()
            && let Some(delay) = self.policy.entries_delay(self.source, self.target)
        {
            sleep_in_process_raft_network(delay).await;
        }
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
            let message = format!("remote AppendEntries on node {}: {err}", self.target);
            if refuses_overwrite(&err) {
                RPCError::Unreachable(Unreachable::from_string(message))
            } else {
                RPCError::Network(NetworkError::from_string(message))
            }
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
        let mut cancel = std::pin::pin!(cancel);
        self.before_streaming_rpc(InProcessRaftRpcKind::FullSnapshot, cancel.as_mut())
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
        // RaftCore cancels a send it no longer wants, and every send once it
        // stops, as the gRPC network honours it (#709).
        let install = std::pin::pin!(target.install_full_snapshot(group, vote, snapshot));
        let installed = match futures_util::future::select(install, cancel).await {
            futures_util::future::Either::Left((installed, _cancel)) => installed,
            futures_util::future::Either::Right((closed, _install)) => {
                return Err(StreamingError::Closed(closed));
            }
        };
        installed.map_err(|err| {
            let message = format!(
                "remote full snapshot on node {}: {}",
                self.target,
                ursula_runtime::ErrorChain(&err)
            );
            if install_error_backs_off(&err) {
                StreamingError::Unreachable(Unreachable::from_string(message))
            } else {
                StreamingError::Network(NetworkError::from_string(message))
            }
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
        if let Some((group, endpoint)) = self.registry.endpoint(self.target)
            && let Err(refusal) = endpoint.election_policy().validate_incoming_transfer(
                group,
                self.target,
                &req,
                endpoint.rejoin(group).as_deref(),
            )
        {
            return Err(RPCError::Network(NetworkError::new(&refusal)));
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

/// Whether a full snapshot refused with `error` backs the sender off, as an
/// unreachable target, rather than failing as a network error. A refused
/// overwrite backs it off, as does every typed refusal of the prefetch that
/// the gRPC plane answers `UNAVAILABLE`. Admission and publication failures
/// stay network errors, as on this network before, and so does every other
/// group refusal, `Group(OwnerStopped)` among them. Unifying this with
/// [`SnapshotInstallError::peer_backs_off`] shifts the simulation's seeds,
/// so it waits for a change that records them anew (#725).
pub(crate) fn install_error_backs_off(error: &SnapshotInstallError) -> bool {
    match error {
        SnapshotInstallError::Group(group) => refuses_overwrite(group),
        SnapshotInstallError::NotRegistered { .. }
        | SnapshotInstallError::ShuttingDown { .. }
        | SnapshotInstallError::Publication(_)
        | SnapshotInstallError::Task(_) => false,
        SnapshotInstallError::GateClosed { .. }
        | SnapshotInstallError::PointerUndecodable { .. }
        | SnapshotInstallError::SourceMissing { .. }
        | SnapshotInstallError::SourceRead { .. }
        | SnapshotInstallError::Undecodable { .. }
        | SnapshotInstallError::CopyWrite { .. }
        | SnapshotInstallError::CopyVerify { .. }
        | SnapshotInstallError::NameCollision { .. }
        | SnapshotInstallError::PointerEncode { .. } => error.peer_backs_off(),
    }
}

/// Whether the commit screen refused the request: it would replace entries
/// the replica knows are committed, or the replica's log lags what OpenRaft
/// committed (#613). The gRPC network answers it `Unavailable`,
/// which the client reports as unreachable, so OpenRaft backs off instead of
/// resending at once; this network classifies it the same.
fn refuses_overwrite(error: &ursula_runtime::GroupEngineError) -> bool {
    matches!(
        error.infra(),
        Some(
            ursula_runtime::GroupInfraError::CommittedOverwrite { .. }
                | ursula_runtime::GroupInfraError::UnrelatedHistory { .. }
                | ursula_runtime::GroupInfraError::LogBehindCommit { .. }
        )
    )
}
