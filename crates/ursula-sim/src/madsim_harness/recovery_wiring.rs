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

/// A fresh outbound ReadIndex barrier from `leader_id`, confirmed the way the
/// barrier RPC confirms one: a new ReadIndex round with a quorum, applied by
/// the leader, while it still leads.
pub(super) async fn in_process_barrier(
    registry: &InProcessRaftRegistry,
    leader_id: u64,
) -> Result<(UrsulaVote, u64), String> {
    let leader = registry.get(leader_id).ok_or("leader is absent")?;
    let linearizer = leader
        .get_read_linearizer(openraft::ReadPolicy::ReadIndex)
        .await
        .map_err(|err| err.to_string())?;
    let index = linearizer.read_log_id().index();
    linearizer
        .try_await_ready(&leader, Some(Duration::from_secs(1)))
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| format!("leader apply timeout: {err:?}"))?;
    let metrics = openraft::rt::WatchReceiver::borrow_watched(&leader.metrics()).clone();
    if metrics.current_leader != Some(leader_id) || !metrics.vote.is_committed() {
        return Err("probe target lost leadership".to_owned());
    }
    Ok((metrics.vote, index))
}

/// The bootstrap probe vote of `node_id` to `peer_id`, screened by the
/// peer's gate as the network would.
pub(super) async fn in_process_probe(
    registry: &InProcessRaftRegistry,
    node_id: u64,
    peer_id: u64,
) -> Option<PeerGroupLog> {
    let target = registry.get(peer_id)?;
    let request = ursula_raft::bootstrap_probe_vote(node_id);
    if let Some(refusal) = registry
        .rejoin(peer_id)
        .and_then(|rejoin| rejoin.screen_vote(&request))
    {
        return Some(PeerGroupLog::from_vote_response(&refusal));
    }
    target
        .vote(request)
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
    placement: ShardPlacement,
    engine: &RaftGroupEngine,
    rejoin: &Arc<GroupRejoin>,
    registry: &InProcessRaftRegistry,
    voters: &BTreeMap<u64, BasicNode>,
) -> Vec<madsim::task::JoinHandle<()>> {
    rejoin.bind(&engine.raft_handle());
    registry.register_rejoin(node_id, rejoin.clone());
    // The node's own view of its groups, which applies its election policy.
    let participation = RaftGroupHandleRegistry::default();
    participation.register_rejoin(placement.raft_group_id, rejoin.clone());
    participation.register(placement, engine.raft_handle());
    let probe_registry = registry.clone();
    vec![
        madsim::task::spawn(ursula_raft::run_rejoin_vote_barrier(
            engine.raft_handle(),
            rejoin.clone(),
            participation,
            voters.clone(),
            move |leader_id, _address| {
                let registry = probe_registry.clone();
                async move { in_process_barrier(&registry, leader_id).await }
            },
            Duration::from_secs(1),
            RECOVERY_DRIVER_INTERVAL,
            RECOVERY_STALL_AFTER,
        )),
        madsim::task::spawn(ursula_raft::run_rejoin_heal(
            engine.raft_handle(),
            rejoin.clone(),
            voters.clone(),
            RECOVERY_DRIVER_INTERVAL,
        )),
    ]
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
