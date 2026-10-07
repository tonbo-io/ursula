//! Recovery gates of simulated nodes over the in-process network: the
//! production gate, barrier driver and heal driver, wired the way the static
//! gRPC engine factory wires them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use ursula_raft::GroupRejoin;
use ursula_raft::InProcessRaftNetworkEvent;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::PeerGroupLog;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RecoveryGateStatus;
use ursula_raft::UrsulaVote;
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
pub(super) fn configured_voters(
    node_ids: impl IntoIterator<Item = u64>,
) -> BTreeMap<u64, BasicNode> {
    node_ids
        .into_iter()
        .map(|node_id| (node_id, BasicNode::new(format!("node-{node_id}"))))
        .collect()
}

#[derive(Debug)]
pub(super) enum RecoveryProbeError {
    Partitioned,
    MissingGate,
    Proof(ursula_raft::QuorumProofError),
}
impl std::fmt::Display for RecoveryProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Partitioned => f.write_str("partitioned from leader"),
            Self::MissingGate => f.write_str("leader gate is absent"),
            Self::Proof(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

/// A fresh outbound ReadIndex barrier from `leader_id`, confirmed the way the
/// barrier RPC confirms one: a new ReadIndex round with a quorum, applied by
/// the leader, while it still leads. `policy` cuts the RPC off as it cuts
/// off the Raft RPCs between `node_id` and the leader.
pub(super) async fn in_process_barrier(
    registry: &InProcessRaftRegistry,
    policy: &InProcessRaftNetworkPolicy,
    node_id: u64,
    leader_id: u64,
) -> Result<(UrsulaVote, u64), RecoveryProbeError> {
    if policy.partitioned(node_id, leader_id) {
        return Err(RecoveryProbeError::Partitioned);
    }
    let group = registry
        .rejoin(leader_id)
        .ok_or(RecoveryProbeError::MissingGate)?
        .raft_group_id();
    registry
        .confirm_recovery_barrier(leader_id, group)
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
        .vote(peer_id, ursula_raft::bootstrap_probe_vote(node_id))
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
    voters: &BTreeMap<u64, BasicNode>,
) {
    registry.register_rejoin(node_id, rejoin.clone());
    engine.attach_recovery(
        rejoin.clone(),
        &RaftGroupHandleRegistry::default(),
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
    );
}

#[derive(Clone)]
struct InProcessRecoveryTransport {
    registry: InProcessRaftRegistry,
    policy: InProcessRaftNetworkPolicy,
    node_id: u64,
}

impl ursula_raft::RecoveryTransport for InProcessRecoveryTransport {
    type Error = RecoveryProbeError;
    async fn probe(&self, peer: u64, _address: String) -> Option<PeerGroupLog> {
        in_process_probe(&self.registry, &self.policy, self.node_id, peer).await
    }
    async fn barrier(
        &self,
        leader: u64,
        _address: String,
    ) -> Result<(UrsulaVote, u64), Self::Error> {
        in_process_barrier(&self.registry, &self.policy, self.node_id, leader).await
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
    pub(super) fn granted_while_gated(&self) -> Vec<VoteAnswer> {
        self.answers()
            .into_iter()
            .filter(VoteAnswer::granted_while_gated)
            .collect()
    }

    /// How many vote requests `target` answered while its gate was closed.
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

/// A simulated network that traces as `sim_network_policy` does and also
/// records every vote answer.
pub(super) fn vote_recording_network_policy() -> (InProcessRaftNetworkPolicy, VoteLog) {
    let policy = InProcessRaftNetworkPolicy::default();
    let votes = VoteLog::default();
    let recorded = votes.clone();
    policy.set_observer(move |event| {
        if let InProcessRaftNetworkEvent::VoteAnswered {
            source,
            target,
            granted,
            target_gate,
        } = &event
        {
            recorded.push(VoteAnswer {
                source: *source,
                target: *target,
                granted: *granted,
                target_gate: *target_gate,
            });
        }
        if let Some(event) = sim_event_from_network_event(event) {
            SimTrace::record(event);
        }
    });
    (policy, votes)
}
