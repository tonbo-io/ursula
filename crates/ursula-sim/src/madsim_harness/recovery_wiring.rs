//! Recovery gates of simulated nodes over the in-process network: the
//! production gate, barrier driver and heal driver, wired the way the static
//! gRPC engine factory wires them.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_proto::placement::NodeId;
use ursula_raft::BarrierAnswer;
use ursula_raft::GroupRejoin;
use ursula_raft::InProcessRaftNetworkEvent;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::PeerGroupLog;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftTarget;
use ursula_raft::RecoveryGateStatus;
use ursula_raft::meta::MetaForwardError;
use ursula_raft::meta::confirm::BarrierFailure;
use ursula_raft::meta::genesis::GenesisProbe;
use ursula_runtime::snapshot_harness::MemorySnapshotBucket;
use ursula_shard::ShardPlacement;

use crate::madsim_harness::SimTrace;
use crate::madsim_harness::introspect::sim_event_from_network_event;

/// How often the recovery drivers re-read their group.
pub(super) const RECOVERY_DRIVER_INTERVAL: Duration = Duration::from_millis(50);
/// How long a simulated gated replica sees no leader before it reports its
/// group stalled. Simulated runs end after 30 s, so this is far shorter than
/// production's `RECOVERY_STALL_AFTER`.
pub(super) const RECOVERY_STALL_AFTER: Duration = Duration::from_secs(2);

/// The configured voters `node_ids` of a simulated group.
pub(super) fn configured_voters(node_ids: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
    node_ids.into_iter().collect()
}

#[derive(Debug)]
pub(super) enum RecoveryProbeError {
    Partitioned,
    Proof(ursula_raft::QuorumProofError),
}
impl From<RecoveryProbeError> for BarrierFailure {
    fn from(error: RecoveryProbeError) -> Self {
        match error {
            RecoveryProbeError::Partitioned => Self::Unreachable,
            RecoveryProbeError::Proof(error) => error.into(),
        }
    }
}

impl std::fmt::Display for RecoveryProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Partitioned => f.write_str("partitioned from leader"),
            Self::Proof(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

/// A fresh outbound ReadIndex barrier of `target` from `leader_id`,
/// confirmed the way the barrier RPC confirms one: a new ReadIndex round
/// with a quorum, applied by the leader, while it still leads. `policy`
/// cuts the RPC off as it cuts off the Raft RPCs between `node_id` and the
/// leader.
pub(super) async fn in_process_barrier(
    registry: &InProcessRaftRegistry,
    policy: &InProcessRaftNetworkPolicy,
    node_id: u64,
    leader_id: u64,
    target: RaftTarget,
) -> Result<BarrierAnswer, RecoveryProbeError> {
    if policy.partitioned(node_id, leader_id) {
        return Err(RecoveryProbeError::Partitioned);
    }
    registry
        .confirm_recovery_barrier(leader_id, target)
        .await
        .map_err(RecoveryProbeError::Proof)
}

/// The bootstrap probe vote of `node_id` to `peer_id`, screened by the
/// peer's gate and cut off by `policy` as the network would.
pub(super) async fn in_process_probe(
    registry: &InProcessRaftRegistry,
    policy: &InProcessRaftNetworkPolicy,
    node_id: u64,
    peer_id: u64,
) -> Option<PeerGroupLog> {
    if policy.partitioned(node_id, peer_id) {
        return None;
    }
    registry
        .vote(peer_id, ursula_raft::bootstrap_probe_vote())
        .await
        .ok()
        .map(|response| PeerGroupLog::from_vote_response(&response))
}

/// Binds `rejoin` to `node_id`'s replica of `placement`, puts it in front of
/// the votes and appends the network delivers to that replica, and spawns
/// the node's recovery drivers: the barrier driver and the heal driver. The
/// drivers belong to the node's process and must be aborted with it.
pub(super) fn wire_recovery(
    node_id: u64,
    _placement: ShardPlacement,
    engine: &RaftGroupEngine,
    rejoin: &Arc<GroupRejoin>,
    registry: &InProcessRaftRegistry,
    policy: &InProcessRaftNetworkPolicy,
    voters: &BTreeSet<u64>,
    recovery_registry: &RaftGroupHandleRegistry,
) {
    engine
        .attach_recovery(
            rejoin.clone(),
            recovery_registry,
            voters.clone(),
            InProcessRecoveryTransport {
                registry: registry.clone(),
                policy: policy.clone(),
                node_id,
            },
            ursula_raft::RecoveryConfig {
                initialize: node_id == 1,
                interval: RECOVERY_DRIVER_INTERVAL,
                barrier_timeout: Duration::from_secs(1),
                stall_after: RECOVERY_STALL_AFTER,
                bootstrap_interval: RECOVERY_DRIVER_INTERVAL,
                bootstrap_warn_after: Duration::from_secs(5),
            },
        )
        .expect("bind and publish recovery before network registration");
}

#[derive(Clone)]
struct InProcessRecoveryTransport {
    registry: InProcessRaftRegistry,
    policy: InProcessRaftNetworkPolicy,
    node_id: u64,
}

impl ursula_raft::RecoveryTransport for InProcessRecoveryTransport {
    type Error = RecoveryProbeError;
    async fn probe(&self, peer: u64) -> Option<PeerGroupLog> {
        in_process_probe(&self.registry, &self.policy, self.node_id, peer).await
    }
    async fn genesis_probe(&self, peer: u64, probe: &GenesisProbe) -> Option<PeerGroupLog> {
        if self.policy.partitioned(self.node_id, peer) {
            return None;
        }
        self.registry.genesis_probe(peer, probe.clone()).await
    }
    async fn barrier(&self, leader: u64, target: RaftTarget) -> Result<BarrierAnswer, Self::Error> {
        in_process_barrier(&self.registry, &self.policy, self.node_id, leader, target).await
    }
    async fn forward_control(
        &self,
        leader: u64,
        command: ControlCommand,
    ) -> Result<ControlResponse, MetaForwardError> {
        if self.policy.partitioned(self.node_id, leader) {
            return Err(MetaForwardError::leader_unreachable());
        }
        self.registry
            .write_meta(leader, command, NodeId::new(self.node_id))
            .await
    }
}

/// One vote request a replica answered over the in-process network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VoteAnswer {
    pub(super) source: Option<u64>,
    pub(super) target: u64,
    pub(super) granted: bool,
    /// The target's recovery gate when the request reached it.
    pub(super) target_gate: Option<RecoveryGateStatus>,
}

#[cfg(test)]
impl VoteAnswer {
    fn granted_while_gated(&self) -> bool {
        self.granted
            && self
                .target_gate
                .is_some_and(|gate| gate != RecoveryGateStatus::Open)
    }
}

/// Every vote answer a simulated network delivered.
#[derive(Debug, Clone, Default)]
pub(super) struct VoteLog(Arc<Mutex<Vec<VoteAnswer>>>);

impl VoteLog {
    fn push(&self, answer: VoteAnswer) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(answer);
    }

    #[cfg(test)]
    fn answers(&self) -> Vec<VoteAnswer> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Forgets every answer so far, such as the first election of a new
    /// group, which a gate with an unknown history may join.
    pub(super) fn clear(&self) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clear();
    }

    /// The votes a replica granted while its recovery gate was closed.
    #[cfg(test)]
    pub(super) fn granted_while_gated(&self) -> Vec<VoteAnswer> {
        self.answers()
            .into_iter()
            .filter(VoteAnswer::granted_while_gated)
            .collect()
    }

    /// How many vote requests `target` answered while its gate was closed.
    #[cfg(test)]
    pub(super) fn answered_while_gated(&self, target: u64) -> usize {
        self.answers()
            .iter()
            .filter(|answer| {
                answer.target == target
                    && answer
                        .target_gate
                        .is_some_and(|gate| gate != RecoveryGateStatus::Open)
            })
            .count()
    }
}

/// One conflict a replica answered below the index its leader had seen it
/// match: it lost entries it had acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReversionAnswer {
    pub(super) source: Option<u64>,
    pub(super) target: u64,
    pub(super) conflict_index: u64,
    pub(super) matched_index: u64,
    /// The target's recovery gate when the request reached it.
    pub(super) target_gate: Option<RecoveryGateStatus>,
}

/// The invariant a conflict below a matched index from a replica whose gate
/// was open breaks ([`ReversionLog::unaccounted`]).
pub(super) const REVERSION_INVARIANT: &str = "follower_reversion_needs_a_closed_gate";

/// Every conflict below a matched index a simulated network delivered.
#[derive(Debug, Clone, Default)]
pub(super) struct ReversionLog(Arc<Mutex<Vec<ReversionAnswer>>>);

impl ReversionLog {
    fn push(&self, answer: ReversionAnswer) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(answer);
    }

    /// How many conflicts below a matched index `target` answered.
    #[cfg(test)]
    pub(super) fn from(&self, target: u64) -> usize {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|answer| answer.target == target)
            .count()
    }

    /// The conflicts below a matched index that came from a replica whose
    /// recovery gate was open, or that has none. Only a replica that may
    /// have lost acknowledged entries starts gated (closed, or held by a
    /// recovery plan), so each of these is a reversion nothing accounts for
    /// ([`REVERSION_INVARIANT`]).
    pub(super) fn unaccounted(&self) -> Vec<ReversionAnswer> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|answer| {
                answer
                    .target_gate
                    .is_none_or(|gate| gate == RecoveryGateStatus::Open)
            })
            .copied()
            .collect()
    }
}

/// A simulated network that traces as `sim_network_policy` does and also
/// records every vote answer and every conflict below a matched index. With
/// `sends`, it opens a window in that bucket for each full snapshot it
/// delivers, while its sender holds the object it names (I3 of #687).
pub(super) fn vote_recording_network_policy(
    sends: Option<MemorySnapshotBucket>,
) -> (InProcessRaftNetworkPolicy, VoteLog, ReversionLog) {
    let policy = InProcessRaftNetworkPolicy::default();
    let votes = VoteLog::default();
    let reversions = ReversionLog::default();
    let recorded = votes.clone();
    let reverted = reversions.clone();
    policy.set_observer(move |event| {
        match &event {
            InProcessRaftNetworkEvent::VoteAnswered {
                source,
                target,
                granted,
                target_gate,
            } => recorded.push(VoteAnswer {
                source: *source,
                target: *target,
                granted: *granted,
                target_gate: *target_gate,
            }),
            InProcessRaftNetworkEvent::ConflictBelowMatching {
                source,
                target,
                conflict_index,
                matched_index,
                target_gate,
            } => reverted.push(ReversionAnswer {
                source: *source,
                target: *target,
                conflict_index: *conflict_index,
                matched_index: *matched_index,
                target_gate: *target_gate,
            }),
            // The windows in which a sender holds a sent object, for I3.
            InProcessRaftNetworkEvent::FullSnapshotStarted {
                target,
                key: Some(key),
                ..
            } => {
                if let Some(bucket) = &sends {
                    bucket.send_started(*target, key);
                }
            }
            InProcessRaftNetworkEvent::FullSnapshotEnded {
                target,
                key: Some(key),
                ..
            } => {
                if let Some(bucket) = &sends {
                    bucket.send_ended(*target, key);
                }
            }
            _ => {}
        }
        if let Some(event) = sim_event_from_network_event(event) {
            SimTrace::record(event);
        }
    });
    (policy, votes, reversions)
}
