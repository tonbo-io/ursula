use openraft::RaftNetworkV2;
use openraft::rt::WatchReceiver;
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

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
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
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;
use ursula_runtime::{
    GroupEngineError, GroupSnapshot, SharedSnapshotStore, SnapshotLocation, SnapshotPointer,
    default_snapshot_store,
};
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::state_machine::*;
use crate::types::*;

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

struct PrefetchedSnapshotGuard {
    snapshot_install: SnapshotInstallCoordinator,
    key: Option<String>,
}

impl PrefetchedSnapshotGuard {
    fn new(snapshot_install: SnapshotInstallCoordinator, key: String) -> Self {
        Self {
            snapshot_install,
            key: Some(key),
        }
    }
}

impl Drop for PrefetchedSnapshotGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.snapshot_install.clear_prefetched_key(&key);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InProcessRaftRegistry {
    nodes: Arc<Mutex<BTreeMap<u64, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
    full_snapshot_calls: Arc<Mutex<BTreeMap<u64, usize>>>,
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

    pub fn full_snapshot_count(&self, node_id: u64) -> usize {
        self.full_snapshot_calls
            .lock()
            .expect("in-process raft full snapshot calls mutex")
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }

    fn record_full_snapshot(&self, node_id: u64) {
        *self
            .full_snapshot_calls
            .lock()
            .expect("in-process raft full snapshot calls mutex")
            .entry(node_id)
            .or_insert(0) += 1;
    }
}

#[derive(Debug, Clone)]
pub struct InProcessRaftNetworkFactory {
    registry: InProcessRaftRegistry,
    source: Option<u64>,
    policy: InProcessRaftNetworkPolicy,
}

impl InProcessRaftNetworkFactory {
    pub fn new(registry: InProcessRaftRegistry) -> Self {
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

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for InProcessRaftNetworkFactory {
    type Network = InProcessRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        InProcessRaftNetwork {
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
    PartitionOneWay { source: u64, target: u64 },
    PartitionBidirectional { a: u64, b: u64 },
    HealOneWay { source: u64, target: u64 },
    HealBidirectional { a: u64, b: u64 },
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

    pub fn partition_one_way(&self, source: u64, target: u64) {
        self.inner
            .lock()
            .expect("in-process raft network policy mutex")
            .partitions
            .insert((source, target));
        self.notify_policy_changed(InProcessRaftNetworkPolicyEvent::PartitionOneWay {
            source,
            target,
        });
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

    pub fn heal_one_way(&self, source: u64, target: u64) {
        self.inner
            .lock()
            .expect("in-process raft network policy mutex")
            .partitions
            .remove(&(source, target));
        self.notify_policy_changed(InProcessRaftNetworkPolicyEvent::HealOneWay { source, target });
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
    PartitionOneWay { source: u64, target: u64 },
    PartitionBidirectional { a: u64, b: u64 },
    HealOneWay { source: u64, target: u64 },
    HealBidirectional { a: u64, b: u64 },
    Clear,
}

impl InProcessRaftFaultAction {
    fn apply(self, policy: &InProcessRaftNetworkPolicy) {
        match self {
            Self::SetDelay(delay) => policy.set_delay(delay),
            Self::PartitionOneWay { source, target } => policy.partition_one_way(source, target),
            Self::PartitionBidirectional { a, b } => policy.partition_bidirectional(a, b),
            Self::HealOneWay { source, target } => policy.heal_one_way(source, target),
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
        target.append_entries(rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote AppendEntries on node {}: {err}",
                self.target
            )))
        })
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
        target.vote(rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote Vote on node {}: {err}",
                self.target
            )))
        })
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
}

impl LeadershipShedReason {
    const ALL: [Self; 3] = [
        Self::SnapshotDriverS3,
        Self::ClusterEgress,
        Self::ColdHealth,
    ];

    const fn bit(self) -> u8 {
        self as u8
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotDriverS3 => "snapshot-driver-s3",
            Self::ClusterEgress => "cluster-egress",
            Self::ColdHealth => "cold-health",
        }
    }
}

impl fmt::Display for LeadershipShedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

const LEADERSHIP_SHED_KNOWN_BITS: u8 = LeadershipShedReason::SnapshotDriverS3.bit()
    | LeadershipShedReason::ClusterEgress.bit()
    | LeadershipShedReason::ColdHealth.bit();

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeadershipShedState {
    bits: u8,
}

impl LeadershipShedState {
    pub fn from_bits(bits: u8) -> Self {
        Self {
            bits: bits & LEADERSHIP_SHED_KNOWN_BITS,
        }
    }

    pub fn load(flag: &LeadershipShedFlag) -> Self {
        Self::from_bits(flag.load(Ordering::Acquire))
    }

    pub fn bits(self) -> u8 {
        self.bits
    }

    pub fn is_shed(self) -> bool {
        self.bits != 0
    }

    pub fn contains(self, reason: LeadershipShedReason) -> bool {
        self.bits & reason.bit() != 0
    }

    /// Whether this node should accept an inbound TransferLeader request.
    ///
    /// Only cluster-egress impairment blocks this. Snapshot/cold-health
    /// impairments are cold-path pressure signals; refusing inbound transfer
    /// for those states can deadlock the balancer when every peer has a
    /// transient cold-path bit set.
    pub fn should_accept_transfer(self) -> bool {
        !self.contains(LeadershipShedReason::ClusterEgress)
    }

    /// Whether local raft groups should be allowed to campaign.
    ///
    /// Cluster-egress and local S3 snapshot-driver impairment disable
    /// campaigning. Cold-health is softer: the node should shed excess current
    /// leadership, but it must remain electable so a cluster-wide hot backlog
    /// cannot exclude every node from leadership.
    pub fn should_campaign(self) -> bool {
        !self.contains(LeadershipShedReason::ClusterEgress)
            && !self.contains(LeadershipShedReason::SnapshotDriverS3)
    }

    /// Whether local raft groups should actively move current leadership away.
    pub fn should_shed_current_leaders(self) -> bool {
        self.is_shed()
    }

    pub fn transfer_rejection_reason(self) -> Option<LeadershipShedReason> {
        if self.contains(LeadershipShedReason::ClusterEgress) {
            Some(LeadershipShedReason::ClusterEgress)
        } else {
            None
        }
    }
}

impl fmt::Display for LeadershipShedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote = false;
        for reason in LeadershipShedReason::ALL {
            if self.contains(reason) {
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

#[derive(Debug, Clone)]
pub struct RaftGroupHandleRegistry {
    groups: Arc<Mutex<BTreeMap<u32, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
    leadership_shed: LeadershipShedFlag,
    snapshot_store: Arc<Mutex<SharedSnapshotStore>>,
    snapshot_install: SnapshotInstallCoordinator,
}

impl Default for RaftGroupHandleRegistry {
    fn default() -> Self {
        Self {
            groups: Arc::new(Mutex::new(BTreeMap::new())),
            leadership_shed: Arc::new(AtomicU8::new(0)),
            snapshot_store: Arc::new(Mutex::new(default_snapshot_store())),
            snapshot_install: SnapshotInstallCoordinator::default(),
        }
    }
}

impl RaftGroupHandleRegistry {
    pub fn register(
        &self,
        placement: ShardPlacement,
        raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    ) {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .insert(placement.raft_group_id.0, raft);
    }

    pub fn get(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
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

    pub fn len(&self) -> usize {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        if previous & reason.bit() == 0 {
            let current = LeadershipShedState::from_bits(previous | reason.bit());
            tracing::warn!("leadership-shed: mark {reason}; state={current}");
        }
    }

    pub fn clear_leadership_shed(&self, reason: LeadershipShedReason) {
        let previous = self
            .leadership_shed
            .fetch_and(!reason.bit(), Ordering::Release);
        if previous & reason.bit() != 0 {
            let current = LeadershipShedState::from_bits(previous & !reason.bit());
            tracing::warn!("leadership-shed: clear {reason}; state={current}");
        }
    }

    pub fn is_leadership_shed(&self) -> bool {
        self.leadership_shed_state().is_shed()
    }

    pub fn metrics_snapshot(&self) -> Vec<RaftGroupMetricsSnapshot> {
        let groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex")
            .iter()
            .map(|(raft_group_id, raft)| (*raft_group_id, raft.clone()))
            .collect::<Vec<_>>();

        let mut snapshots = Vec::with_capacity(groups.len());
        for (raft_group_id, raft) in groups {
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
        let (snapshot, prefetched_key) = self.prefetch_snapshot_for_install(snapshot).await?;
        let _prefetched_guard = prefetched_key
            .map(|key| PrefetchedSnapshotGuard::new(self.snapshot_install.clone(), key));
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft install snapshot: {err}")))
    }

    async fn prefetch_snapshot_for_install(
        &self,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    ) -> Result<(TypeConfigSnapshotOf<UrsulaRaftTypeConfig>, Option<String>), GroupEngineError>
    {
        let pointer_bytes = snapshot.snapshot.into_inner();
        let pointer = SnapshotPointer::decode(&pointer_bytes).map_err(|err| {
            GroupEngineError::new(format!("decode OpenRaft snapshot pointer: {err}"))
        })?;
        let SnapshotPointer {
            snapshot_id,
            location,
        } = pointer;
        if matches!(location, SnapshotLocation::Inline { .. }) {
            return Ok((
                TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
                    meta: snapshot.meta,
                    snapshot: Cursor::new(pointer_bytes),
                },
                None,
            ));
        }

        let snapshot_store = self.snapshot_store();
        let snapshot_bytes = snapshot_store.download(&location).await.map_err(|err| {
            GroupEngineError::new(format!(
                "prefetch OpenRaft snapshot {snapshot_id} before install: {err}"
            ))
        })?;
        serde_json::from_slice::<GroupSnapshot>(&snapshot_bytes).map_err(|err| {
            GroupEngineError::new(format!(
                "decode prefetched OpenRaft snapshot {snapshot_id}: {err}"
            ))
        })?;
        let prefetched_key =
            self.snapshot_install
                .cache_prefetched(&snapshot_id, &location, snapshot_bytes);
        Ok((
            TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
                meta: snapshot.meta,
                snapshot: Cursor::new(pointer_bytes),
            },
            Some(prefetched_key),
        ))
    }

    pub async fn handle_transfer_leader(
        &self,
        raft_group_id: RaftGroupId,
        request: TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> Result<(), GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
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
    use super::*;
    use ursula_runtime::{SnapshotStore, SnapshotStoreError, SnapshotStoreFuture};

    #[derive(Debug)]
    struct StaticSnapshotStore {
        bytes: Option<Vec<u8>>,
    }

    impl SnapshotStore for StaticSnapshotStore {
        fn upload<'a>(
            &'a self,
            _key: ursula_runtime::SnapshotKey,
            bytes: Vec<u8>,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move { Ok(SnapshotLocation::Inline { bytes }) })
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
        serde_json::to_vec(&serde_json::json!({
            "placement": {
                "core_id": 0,
                "shard_id": 0,
                "raft_group_id": 7,
            },
            "group_commit_index": 0,
            "stream_snapshot": {
                "buckets": [],
                "streams": [],
            },
            "stream_append_counts": [],
        }))
        .expect("serialize test group snapshot")
    }

    fn external_snapshot(snapshot_id: &str) -> TypeConfigSnapshotOf<UrsulaRaftTypeConfig> {
        let pointer = SnapshotPointer {
            snapshot_id: snapshot_id.to_owned(),
            location: SnapshotLocation::S3 {
                key: format!("{snapshot_id}.snap"),
                size_bytes: 1,
            },
        };
        TypeConfigSnapshotOf::<UrsulaRaftTypeConfig> {
            meta: snapshot_meta(snapshot_id),
            snapshot: Cursor::new(pointer.encode().expect("encode test pointer")),
        }
    }

    #[tokio::test]
    async fn prefetch_snapshot_for_install_keeps_external_pointer_and_caches_bytes() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
            bytes: Some(group_snapshot_bytes()),
        })));

        let (snapshot, prefetched_key) = registry
            .prefetch_snapshot_for_install(external_snapshot("snapshot-a"))
            .await
            .expect("prefetch external snapshot");

        let pointer = SnapshotPointer::decode(&snapshot.snapshot.into_inner()).unwrap();
        assert_eq!(pointer.snapshot_id, "snapshot-a");
        match pointer.location {
            SnapshotLocation::S3 { key, .. } => assert_eq!(key, "snapshot-a.snap"),
            other => panic!("expected s3 snapshot, got {other:?}"),
        }
        let prefetched_key = prefetched_key.expect("external snapshot is cached");
        let cached = registry
            .snapshot_install_coordinator()
            .clear_prefetched_key(&prefetched_key)
            .expect("prefetched snapshot bytes cached");
        serde_json::from_slice::<GroupSnapshot>(cached.as_slice()).unwrap();
    }

    #[tokio::test]
    async fn prefetched_snapshot_guard_clears_cache_on_drop() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
            bytes: Some(group_snapshot_bytes()),
        })));

        let (_, prefetched_key) = registry
            .prefetch_snapshot_for_install(external_snapshot("snapshot-drop"))
            .await
            .expect("prefetch external snapshot");
        let prefetched_key = prefetched_key.expect("external snapshot is cached");
        let coordinator = registry.snapshot_install_coordinator();

        let guard = PrefetchedSnapshotGuard::new(coordinator.clone(), prefetched_key.clone());
        drop(guard);

        assert!(
            coordinator.clear_prefetched_key(&prefetched_key).is_none(),
            "dropping an interrupted install must clear prefetched snapshot bytes"
        );
    }

    #[tokio::test]
    async fn prefetch_snapshot_for_install_keeps_prefetch_failure_outside_openraft() {
        let registry = RaftGroupHandleRegistry::default();
        registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore { bytes: None })));

        let err = registry
            .prefetch_snapshot_for_install(external_snapshot("missing"))
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
            snapshot: Cursor::new(pointer.encode().expect("encode test pointer")),
        };

        registry
            .prefetch_snapshot_for_install(snapshot)
            .await
            .expect("inline snapshot does not touch snapshot store");
    }

    #[test]
    fn leadership_shed_policy_splits_transfer_from_campaigning() {
        let empty = LeadershipShedState::default();
        assert!(empty.should_accept_transfer());
        assert!(empty.should_campaign());
        assert!(!empty.should_shed_current_leaders());
        assert_eq!(empty.transfer_rejection_reason(), None);

        let snapshot_shed =
            LeadershipShedState::from_bits(LeadershipShedReason::SnapshotDriverS3.bit());
        assert!(snapshot_shed.should_accept_transfer());
        assert!(!snapshot_shed.should_campaign());
        assert!(snapshot_shed.should_shed_current_leaders());
        assert_eq!(snapshot_shed.transfer_rejection_reason(), None);

        let cold_shed = LeadershipShedState::from_bits(LeadershipShedReason::ColdHealth.bit());
        assert!(cold_shed.should_accept_transfer());
        assert!(cold_shed.should_campaign());
        assert!(cold_shed.should_shed_current_leaders());
        assert_eq!(cold_shed.transfer_rejection_reason(), None);

        let egress_shed = LeadershipShedState::from_bits(
            LeadershipShedReason::ClusterEgress.bit() | LeadershipShedReason::ColdHealth.bit(),
        );
        assert!(!egress_shed.should_accept_transfer());
        assert!(!egress_shed.should_campaign());
        assert!(egress_shed.should_shed_current_leaders());
        assert_eq!(
            egress_shed.transfer_rejection_reason(),
            Some(LeadershipShedReason::ClusterEgress)
        );
    }

    #[test]
    fn leadership_shed_reasons_are_tracked_independently() {
        let registry = RaftGroupHandleRegistry::default();
        assert!(!registry.is_leadership_shed());

        registry.mark_leadership_shed(LeadershipShedReason::ClusterEgress);
        registry.mark_leadership_shed(LeadershipShedReason::ColdHealth);
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
