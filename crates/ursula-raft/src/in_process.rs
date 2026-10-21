//! In-process fault-injection network for tests and deterministic simulation.
//!
//! The registry and the network are generic over the state machine: each
//! one delivers to the replicas of one plane, through what the plane's
//! state machine registers to answer RPCs ([`InProcessEndpoint`]). The data
//! plane keeps its names ([`InProcessRaftRegistry`],
//! [`InProcessRaftNetworkFactory`], [`InProcessRaftNetwork`]).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::EmptyNode;
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
use openraft::rt::WatchReceiver;
use openraft::storage::RaftStateMachine;

use crate::registry::SnapshotInstallError;
use crate::rejoin::GroupRejoin;
use crate::rejoin::RecoveryGateStatus;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaSnapshot;

/// How the in-process network delivers RPCs to the replicas of one state
/// machine: what a node registers to answer them, and the production entry
/// point each RPC goes through on it.
pub trait InProcessEndpoint:
    RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>
{
    /// What a node registers to answer this plane's RPCs.
    type Endpoint: Clone + Send + Sync + 'static;

    /// The target's recovery gate, as status reports show it.
    fn gate(endpoint: &Self::Endpoint) -> Option<RecoveryGateStatus>;

    fn append_entries(
        endpoint: &Self::Endpoint,
        rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> impl Future<
        Output = Result<
            AppendEntriesResponse<UrsulaRaftTypeConfig>,
            ursula_runtime::GroupEngineError,
        >,
    > + Send;

    fn vote(
        endpoint: &Self::Endpoint,
        rpc: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> impl Future<
        Output = Result<VoteResponse<UrsulaRaftTypeConfig>, ursula_runtime::GroupEngineError>,
    > + Send;

    fn install_full_snapshot(
        endpoint: &Self::Endpoint,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: UrsulaSnapshot,
    ) -> impl Future<Output = Result<SnapshotResponse<UrsulaRaftTypeConfig>, SnapshotInstallError>> + Send;

    /// The node fence of the node that registered `endpoint`.
    fn fence(endpoint: &Self::Endpoint) -> ursula_runtime::NodeFence;

    /// The target's own check of a leadership transfer to `target`, before
    /// OpenRaft sees it.
    fn validate_incoming_transfer(
        endpoint: &Self::Endpoint,
        target: u64,
        request: &TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> Result<(), crate::IncomingTransferError>;
}

/// The data plane delivers through each node's production registry.
impl InProcessEndpoint for RaftGroupStateMachine {
    type Endpoint = (ursula_shard::RaftGroupId, crate::RaftGroupHandleRegistry);

    fn gate((group, registry): &Self::Endpoint) -> Option<RecoveryGateStatus> {
        registry.rejoin(*group).map(|rejoin| rejoin.status())
    }

    fn append_entries(
        (group, registry): &Self::Endpoint,
        rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> impl Future<
        Output = Result<
            AppendEntriesResponse<UrsulaRaftTypeConfig>,
            ursula_runtime::GroupEngineError,
        >,
    > + Send {
        registry.append_entries(*group, rpc)
    }

    fn vote(
        (group, registry): &Self::Endpoint,
        rpc: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> impl Future<
        Output = Result<VoteResponse<UrsulaRaftTypeConfig>, ursula_runtime::GroupEngineError>,
    > + Send {
        registry.vote(*group, rpc)
    }

    fn install_full_snapshot(
        (group, registry): &Self::Endpoint,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: UrsulaSnapshot,
    ) -> impl Future<Output = Result<SnapshotResponse<UrsulaRaftTypeConfig>, SnapshotInstallError>> + Send
    {
        registry.install_full_snapshot(*group, vote, snapshot)
    }

    fn fence((_, registry): &Self::Endpoint) -> ursula_runtime::NodeFence {
        registry.peer_cluster().fence()
    }

    fn validate_incoming_transfer(
        (group, registry): &Self::Endpoint,
        target: u64,
        request: &TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> Result<(), crate::IncomingTransferError> {
        registry.election_policy().validate_incoming_transfer(
            *group,
            target,
            request,
            registry.rejoin(*group).as_deref(),
        )
    }
}

/// The in-process registry of one plane's replicas, by node id.
pub struct InProcessRegistry<SM: InProcessEndpoint> {
    endpoints: Arc<Mutex<BTreeMap<u64, SM::Endpoint>>>,
    nodes: Arc<Mutex<BTreeMap<u64, Raft<UrsulaRaftTypeConfig, SM>>>>,
    barriers: Arc<Mutex<BTreeMap<u64, Arc<crate::read_index::ReadIndexBarrier<SM>>>>>,
    full_snapshot_calls: Arc<Mutex<BTreeMap<u64, usize>>>,
}

/// The data plane's in-process registry.
pub type InProcessRaftRegistry = InProcessRegistry<RaftGroupStateMachine>;

impl<SM: InProcessEndpoint> Clone for InProcessRegistry<SM> {
    fn clone(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            nodes: self.nodes.clone(),
            barriers: self.barriers.clone(),
            full_snapshot_calls: self.full_snapshot_calls.clone(),
        }
    }
}

impl<SM: InProcessEndpoint> Default for InProcessRegistry<SM> {
    fn default() -> Self {
        Self {
            endpoints: Arc::default(),
            nodes: Arc::default(),
            barriers: Arc::default(),
            full_snapshot_calls: Arc::default(),
        }
    }
}

impl<SM: InProcessEndpoint> Debug for InProcessRegistry<SM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessRegistry").finish_non_exhaustive()
    }
}

impl<SM: InProcessEndpoint> InProcessRegistry<SM> {
    pub fn unregister(&self, node_id: u64) -> Option<Raft<UrsulaRaftTypeConfig, SM>> {
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

    pub fn get(&self, node_id: u64) -> Option<Raft<UrsulaRaftTypeConfig, SM>> {
        self.nodes
            .lock()
            .expect("in-process raft registry mutex")
            .get(&node_id)
            .cloned()
    }

    fn endpoint(&self, node_id: u64) -> Option<SM::Endpoint> {
        self.endpoints
            .lock()
            .expect("endpoint registry")
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

impl InProcessRegistry<RaftGroupStateMachine> {
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

    /// The barrier RPC's server side on `node_id` for `target`, through the
    /// node's production registry: a leader whose history is pending
    /// confirms none, and a node that runs no meta replica confirms no meta
    /// barrier.
    pub async fn confirm_recovery_barrier(
        &self,
        node_id: u64,
        target: crate::RaftTarget,
    ) -> Result<crate::BarrierAnswer, crate::QuorumProofError> {
        let group = match target {
            crate::RaftTarget::Data(group) => group,
            crate::RaftTarget::Meta => crate::meta::state_machine::META_GROUP,
        };
        let (_, endpoint) = self
            .endpoint(node_id)
            .ok_or(crate::QuorumProofError::NotRegistered { group })?;
        match target {
            crate::RaftTarget::Data(group) => endpoint.confirm_recovery_barrier(group).await,
            crate::RaftTarget::Meta => match endpoint.meta() {
                Some(meta) => meta.confirm_recovery_barrier().await,
                None => Err(crate::QuorumProofError::NotRegistered { group }),
            },
        }
    }

    /// The genesis probe RPC on `node_id`, through the node's production
    /// registry ([`genesis_probe_in_process`]).
    pub async fn genesis_probe(
        &self,
        node_id: u64,
        probe: crate::meta::genesis::GenesisProbe,
    ) -> Option<crate::PeerGroupLog> {
        let (_, endpoint) = self.endpoint(node_id)?;
        genesis_probe_in_process(&endpoint, probe).await
    }

    /// A control command `sender` forwards to the meta leader `node_id`,
    /// through the node's production registry
    /// ([`forward_control_in_process`]). A node that is not registered is a
    /// leader that cannot be reached.
    pub async fn write_meta(
        &self,
        node_id: u64,
        command: ursula_control::ControlCommand,
        sender: ursula_proto::placement::NodeId,
    ) -> Result<ursula_control::ControlResponse, crate::meta::MetaForwardError> {
        let Some((_, endpoint)) = self.endpoint(node_id) else {
            return Err(crate::meta::MetaForwardError::leader_unreachable());
        };
        forward_control_in_process(&endpoint, node_id, command, sender).await
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
                    group: crate::RaftTarget::Data(ursula_shard::RaftGroupId(0)),
                })?;
        endpoint.accept_recovery_plan(group, plan, restart).await
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
}

/// The genesis probe as the node whose production registry is `registry`
/// answers it, and as the gRPC sender reads the answer: no answer from a
/// node that runs no meta replica or whose replica refuses the probe vote.
pub(crate) async fn genesis_probe_in_process(
    registry: &crate::RaftGroupHandleRegistry,
    probe: crate::meta::genesis::GenesisProbe,
) -> Option<crate::PeerGroupLog> {
    let meta = registry.meta()?;
    meta.answer_genesis_probe(probe)
        .await
        .ok()
        .map(crate::PeerGroupLog::from)
}

/// A control command `sender` forwards to the meta leader `leader`, whose
/// production registry is `registry`, answered through the same forwarded
/// write as `GroupWrite` ([`crate::meta::replica::ActiveMeta`]) and
/// classified as the gRPC sender classifies the same answer: the guard's
/// refusal of a `RegisterNode` naming another node, and the refusal of a
/// node that runs no meta replica, are the leader's refusals of the request
/// ([`crate::meta::MetaForwardError::refused_in_process`]).
pub(crate) async fn forward_control_in_process(
    registry: &crate::RaftGroupHandleRegistry,
    leader: u64,
    command: ursula_control::ControlCommand,
    sender: ursula_proto::placement::NodeId,
) -> Result<ursula_control::ControlResponse, crate::meta::MetaForwardError> {
    let refused = |status| crate::meta::MetaForwardError::refused_in_process(leader, status);
    let meta = registry
        .meta()
        .ok_or_else(|| refused(crate::grpc::no_meta_replica("GroupWrite").into()))?;
    let answers = meta
        .write_forwarded(
            vec![crate::RaftCommand::Meta(crate::meta::MetaCommand::Control(
                command,
            ))],
            sender,
        )
        .await
        .map_err(refused)?;
    crate::forward::single_control_answer(answers).map_err(crate::meta::MetaForwardError::Leader)
}

/// The in-process network factory of one plane.
pub struct InProcessNetworkFactory<SM: InProcessEndpoint> {
    registry: InProcessRegistry<SM>,
    source: Option<u64>,
    policy: InProcessRaftNetworkPolicy,
}

/// The data plane's in-process network factory.
pub type InProcessRaftNetworkFactory = InProcessNetworkFactory<RaftGroupStateMachine>;

impl<SM: InProcessEndpoint> Clone for InProcessNetworkFactory<SM> {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            source: self.source,
            policy: self.policy.clone(),
        }
    }
}

impl<SM: InProcessEndpoint> Debug for InProcessNetworkFactory<SM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessNetworkFactory")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<SM: InProcessEndpoint> InProcessNetworkFactory<SM> {
    pub fn new(registry: InProcessRegistry<SM>) -> Self {
        Self {
            registry,
            source: None,
            policy: InProcessRaftNetworkPolicy::default(),
        }
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

impl<SM: InProcessEndpoint> RaftNetworkFactory<UrsulaRaftTypeConfig>
    for InProcessNetworkFactory<SM>
{
    type Network = InProcessNetwork<SM>;

    async fn new_client(&mut self, target: u64, _node: &EmptyNode) -> Self::Network {
        InProcessNetwork {
            source: self.source,
            target,
            registry: self.registry.clone(),
            policy: self.policy.clone(),
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
    /// The target answered `Conflict` to an append whose previous entry is
    /// at or below the index the sender's leadership saw it match: the
    /// target lost entries it had acknowledged. `target_gate` is the
    /// target's recovery gate when the request reached it. Only a replica
    /// whose gate is closed may lose acknowledged entries, so a simulation
    /// fails on one whose gate was `Open`.
    ConflictBelowMatching {
        source: Option<u64>,
        target: u64,
        conflict_index: u64,
        matched_index: u64,
        target_gate: Option<RecoveryGateStatus>,
    },
    /// A full snapshot reached its target, which reads the object `key`
    /// names, if its pointer names one, while the RPC runs. The sender
    /// holds that object until [`Self::FullSnapshotEnded`] (#687).
    FullSnapshotStarted {
        source: Option<u64>,
        target: u64,
        key: Option<String>,
    },
    /// The full snapshot of the matching [`Self::FullSnapshotStarted`]
    /// returned or was cancelled. It is sent before the sender's hold on
    /// the object drops.
    FullSnapshotEnded {
        source: Option<u64>,
        target: u64,
        key: Option<String>,
    },
}

/// Sends [`InProcessRaftNetworkEvent::FullSnapshotEnded`] when the RPC of a
/// full snapshot returns or is cancelled.
struct FullSnapshotWindow {
    policy: InProcessRaftNetworkPolicy,
    source: Option<u64>,
    target: u64,
    key: Option<String>,
}

impl Drop for FullSnapshotWindow {
    fn drop(&mut self) {
        self.policy
            .notify(InProcessRaftNetworkEvent::FullSnapshotEnded {
                source: self.source,
                target: self.target,
                key: self.key.take(),
            });
    }
}

/// The object key the encoded pointer of a full snapshot names, if it
/// names one.
fn sent_object_key(pointer_bytes: &[u8]) -> Option<String> {
    match ursula_runtime::SnapshotPointer::decode(pointer_bytes)
        .ok()?
        .location
    {
        ursula_runtime::SnapshotLocation::S3 { key, .. } => Some(key),
        ursula_runtime::SnapshotLocation::Inline { .. }
        | ursula_runtime::SnapshotLocation::Local { .. } => None,
    }
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
    /// Targets whose `Conflict` answers the sender gets as unreachable
    /// ([`InProcessRaftNetworkPolicy::hide_conflicts`]).
    hidden_conflicts: BTreeSet<u64>,
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

    /// Answers every `Conflict` that `target` delivers as an unreachable
    /// target, or no longer with `false`, as a network that hides a
    /// follower's conflicts from its leader would. OpenRaft counts a
    /// conflict as the follower's acknowledgement of the leader, and an
    /// error as none, so a negative control shows what hiding them costs.
    pub fn hide_conflicts(&self, target: u64, hide: bool) {
        let mut inner = self
            .inner
            .lock()
            .expect("in-process raft network policy mutex");
        if hide {
            inner.hidden_conflicts.insert(target);
        } else {
            inner.hidden_conflicts.remove(&target);
        }
    }

    fn hides_conflicts(&self, target: u64) -> bool {
        self.inner
            .lock()
            .expect("in-process raft network policy mutex")
            .hidden_conflicts
            .contains(&target)
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
        inner.hidden_conflicts.clear();
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

/// The in-process network of one plane, from one source to one target.
pub struct InProcessNetwork<SM: InProcessEndpoint> {
    source: Option<u64>,
    target: u64,
    registry: InProcessRegistry<SM>,
    policy: InProcessRaftNetworkPolicy,
}

/// The data plane's in-process network.
pub type InProcessRaftNetwork = InProcessNetwork<RaftGroupStateMachine>;

impl<SM: InProcessEndpoint> Clone for InProcessNetwork<SM> {
    fn clone(&self) -> Self {
        Self {
            source: self.source,
            target: self.target,
            registry: self.registry.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl<SM: InProcessEndpoint> Debug for InProcessNetwork<SM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessNetwork")
            .field("source", &self.source)
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl<SM: InProcessEndpoint> InProcessNetwork<SM> {
    fn missing_target_error(&self) -> Unreachable<UrsulaRaftTypeConfig> {
        Unreachable::from_string(format!(
            "in-process raft node {} is not registered",
            self.target
        ))
    }

    /// The index the sender's leadership under `leader` saw the target
    /// match, if it did.
    fn matched_by_sender(&self, leader: &VoteOf<UrsulaRaftTypeConfig>) -> Option<u64> {
        let raft = self.registry.get(self.source?)?;
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched();
        if &metrics.vote != leader {
            return None;
        }
        metrics
            .replication
            .as_ref()?
            .get(&self.target)?
            .as_ref()
            .map(|log_id| log_id.index)
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

impl<SM: InProcessEndpoint> InProcessNetwork<SM> {
    /// The fences of an RPC, as the gRPC plane checks them: the sender's
    /// before it sends and at the answer's arrival, the target's at the
    /// request's arrival and before its answer leaves. A fenced end loses
    /// the RPC, as an unreachable peer does.
    fn check_fences(
        &self,
        target: Option<&SM::Endpoint>,
    ) -> Result<(), Unreachable<UrsulaRaftTypeConfig>> {
        let sender = self
            .source
            .and_then(|source| self.registry.endpoint(source))
            .map(|endpoint| SM::fence(&endpoint));
        let target = target.map(SM::fence);
        for (end, fence) in [("sender", sender), ("target", target)] {
            if fence.is_some_and(|fence| !fence.admits_now()) {
                return Err(Unreachable::from_string(format!(
                    "in-process raft: the {end} of an RPC to node {} is fenced",
                    self.target
                )));
            }
        }
        Ok(())
    }
}

impl<SM: InProcessEndpoint> RaftNetworkV2<UrsulaRaftTypeConfig> for InProcessNetwork<SM> {
    type SnapshotData = SnapshotData;

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
        let target = self.registry.endpoint(self.target).ok_or_else(|| {
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
        self.check_fences(Some(&target))
            .map_err(RPCError::Unreachable)?;
        let matched = self.matched_by_sender(&rpc.vote);
        let conflict_index = rpc.prev_log_id.map(|log_id| log_id.index);
        let target_gate = SM::gate(&target);
        let response = SM::append_entries(&target, rpc).await.map_err(|err| {
            let message = format!("remote AppendEntries on node {}: {err}", self.target);
            if refuses_overwrite(&err) {
                RPCError::Unreachable(Unreachable::from_string(message))
            } else {
                RPCError::Network(NetworkError::from_string(message))
            }
        })?;
        self.check_fences(Some(&target))
            .map_err(RPCError::Unreachable)?;
        if matches!(response, AppendEntriesResponse::Conflict) {
            if let (Some(conflict_index), Some(matched_index)) = (conflict_index, matched)
                && conflict_index <= matched_index
            {
                self.policy
                    .notify(InProcessRaftNetworkEvent::ConflictBelowMatching {
                        source: self.source,
                        target: self.target,
                        conflict_index,
                        matched_index,
                        target_gate,
                    });
            }
            if self.policy.hides_conflicts(self.target) {
                return Err(RPCError::Unreachable(Unreachable::from_string(format!(
                    "in-process raft hides the conflicts of node {}",
                    self.target
                ))));
            }
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
        let target = self.registry.endpoint(self.target).ok_or_else(|| {
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
        self.check_fences(Some(&target))
            .map_err(RPCError::Unreachable)?;
        let target_gate = SM::gate(&target);
        let response = SM::vote(&target, rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!("remote vote: {err}")))
        })?;
        self.check_fences(Some(&target))
            .map_err(RPCError::Unreachable)?;
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
        snapshot: UrsulaSnapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        let mut cancel = std::pin::pin!(cancel);
        self.before_streaming_rpc(InProcessRaftRpcKind::FullSnapshot, cancel.as_mut())
            .await?;
        self.registry.record_full_snapshot(self.target);
        let target = self.registry.endpoint(self.target).ok_or_else(|| {
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
        // The lease keeps the object the pointer names until this RPC
        // returns, as the gRPC network keeps it: the receiver gets the
        // pointer only, and an install task that outlives the RPC finds the
        // object gone once the sender replaced it (#687).
        let UrsulaSnapshot { meta, snapshot } = snapshot;
        let (pointer_bytes, _lease) = snapshot.into_parts();
        let key = sent_object_key(&pointer_bytes);
        self.policy
            .notify(InProcessRaftNetworkEvent::FullSnapshotStarted {
                source: self.source,
                target: self.target,
                key: key.clone(),
            });
        // Dropped before the lease, so the send's window ends while the
        // sender still holds the object.
        let _window = FullSnapshotWindow {
            policy: self.policy.clone(),
            source: self.source,
            target: self.target,
            key,
        };
        let snapshot = UrsulaSnapshot {
            meta,
            snapshot: crate::types::SnapshotData::new(pointer_bytes),
        };
        self.check_fences(Some(&target))
            .map_err(StreamingError::Unreachable)?;
        // RaftCore cancels a send it no longer wants, and every send once it
        // stops, as the gRPC network honours it (#709).
        let install = std::pin::pin!(SM::install_full_snapshot(&target, vote, snapshot));
        let installed = match futures_util::future::select(install, cancel).await {
            futures_util::future::Either::Left((installed, _cancel)) => installed,
            futures_util::future::Either::Right((closed, _install)) => {
                return Err(StreamingError::Closed(closed));
            }
        };
        self.check_fences(Some(&target))
            .map_err(StreamingError::Unreachable)?;
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
        let endpoint = self.registry.endpoint(self.target);
        self.check_fences(endpoint.as_ref())
            .map_err(RPCError::Unreachable)?;
        if let Some(endpoint) = &endpoint
            && let Err(refusal) = SM::validate_incoming_transfer(endpoint, self.target, &req)
        {
            return Err(RPCError::Network(NetworkError::new(&refusal)));
        }
        let response = target.handle_transfer_leader(req).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote TransferLeader on node {}: {err}",
                self.target
            )))
        })?;
        self.check_fences(endpoint.as_ref())
            .map_err(RPCError::Unreachable)?;
        Ok(response)
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
        | SnapshotInstallError::Task(_) => false,
        SnapshotInstallError::GateClosed { .. }
        | SnapshotInstallError::PointerUndecodable { .. }
        | SnapshotInstallError::SourceMissing { .. }
        | SnapshotInstallError::SourceRead { .. }
        | SnapshotInstallError::Undecodable { .. }
        | SnapshotInstallError::CopyWrite { .. }
        | SnapshotInstallError::CopyVerify { .. }
        | SnapshotInstallError::NameCollision { .. }
        | SnapshotInstallError::PointerEncode { .. }
        | SnapshotInstallError::MetaPointer { .. }
        | SnapshotInstallError::MetaState { .. } => error.peer_backs_off(),
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
