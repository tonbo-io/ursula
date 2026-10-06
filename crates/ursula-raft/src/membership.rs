//! Quorum-confirmed, applied uniform data membership for control adoption.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use openraft::vote::RaftLeaderId;
use serde::Deserialize;
use serde::Serialize;
use ursula_control::CommittedGroupConfiguration;
use ursula_control::MembershipLogId;
use ursula_control::VerifiedGroupMembership;
use ursula_shard::RaftGroupId;

use crate::codec::decode_wire;
use crate::grpc::GrpcRaftNetwork;
use crate::raft_internal_proto::RejoinBarrierRequestV1;
use crate::registry::RaftGroupHandleRegistry;
use crate::types::UrsulaVote;

/// The committed membership and the exact applied prefix sampled after a fresh
/// ReadIndex. Effective/uncommitted or joint membership never produces this
/// certificate. This observation is not a reservation against later changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumGroupMembership {
    pub raft_group_id: RaftGroupId,
    pub leader_id: u64,
    pub leader_term: u64,
    pub applied_index: u64,
    pub membership: VerifiedGroupMembership,
    pub nodes: BTreeMap<u64, String>,
}

/// Obtain applied uniform membership from the current data leader. No legacy
/// fallback: a peer without this capability cannot be adopted into managed mode.
pub async fn confirm_group_membership(
    group: RaftGroupId,
    leader_id: u64,
    address: &str,
    timeout: Duration,
) -> Result<QuorumGroupMembership, String> {
    if leader_id == 0 || timeout.is_zero() {
        return Err("membership observation requires a non-zero leader and timeout".to_owned());
    }
    let network = GrpcRaftNetwork::new(group, leader_id, address);
    let mut client = network.client().map_err(|error| error.to_string())?;
    let mut request = tonic::Request::new(RejoinBarrierRequestV1 {
        raft_group_id: group.0,
        protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
        include_membership: true,
        include_configuration: false,
        target_node_id: leader_id,
    });
    request.set_timeout(timeout);
    let response = client
        .rejoin_barrier(request)
        .await
        .map_err(|error| format!("confirm data membership: {error}"))?
        .into_inner();
    let vote: UrsulaVote = decode_wire(&response.vote, "membership barrier vote")
        .map_err(|error| error.to_string())?;
    let observation: QuorumGroupMembership =
        decode_wire(&response.membership, "membership barrier certificate")
            .map_err(|error| error.to_string())?;
    if !vote.is_committed()
        || *vote.leader_id().node_id() != leader_id
        || observation.raft_group_id != group
        || observation.leader_id != leader_id
        || observation.leader_term != vote.leader_id().term()
        || observation.applied_index < response.index
        || observation.membership.log_id.index > observation.applied_index
        || observation.membership.log_id.node_id == 0
        || observation.membership.voters.is_empty()
        || !observation.membership.voters.contains(&leader_id)
        || !observation
            .membership
            .voters
            .is_disjoint(&observation.membership.learners)
        || observation
            .nodes
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            != observation
                .membership
                .voters
                .union(&observation.membership.learners)
                .copied()
                .collect()
    {
        return Err("inconsistent data membership certificate".to_owned());
    }
    Ok(observation)
}

pub(crate) async fn read_applied_membership(
    registry: &RaftGroupHandleRegistry,
    group: RaftGroupId,
    read_index: u64,
) -> Result<QuorumGroupMembership, tonic::Status> {
    let observation = read_applied_configuration(registry, group, read_index).await?;
    let membership = observation
        .uniform_membership()
        .map_err(tonic::Status::failed_precondition)?;
    // Keep the original uniform-only response and contract unchanged.
    Ok(QuorumGroupMembership {
        raft_group_id: group,
        leader_id: observation.leader_id,
        leader_term: observation.leader_term,
        applied_index: observation.applied_log_id.index,
        membership,
        nodes: observation.nodes,
    })
}

pub(crate) async fn read_applied_configuration(
    registry: &RaftGroupHandleRegistry,
    group: RaftGroupId,
    read_index: u64,
) -> Result<CommittedGroupConfiguration, tonic::Status> {
    let raft = registry
        .get(group)
        .ok_or_else(|| tonic::Status::not_found("raft group is not registered"))?;
    let before = raft.metrics().borrow_watched().clone();
    let (applied, committed) = raft
        .with_state_machine(|machine| {
            let value = (machine.last_applied_log_id, machine.last_membership.clone());
            Box::pin(async move { value })
        })
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    let after = raft.metrics().borrow_watched().clone();
    let applied = applied
        .ok_or_else(|| tonic::Status::failed_precondition("configuration is not applied"))?;
    let log_id = committed.log_id().as_ref().copied().ok_or_else(|| {
        tonic::Status::failed_precondition("configuration has no committed log id")
    })?;
    if before.current_leader != Some(before.id)
        || after.current_leader != Some(after.id)
        || before.vote != after.vote
        || !after.vote.is_committed()
        || before.membership_config.as_ref() != &committed
        || after.membership_config.as_ref() != &committed
        || applied.index() < read_index
    {
        return Err(tonic::Status::failed_precondition(
            "configuration observation requires stable leadership and applied effective membership",
        ));
    }
    let convert = |log: openraft::alias::LogIdOf<crate::UrsulaRaftTypeConfig>| MembershipLogId {
        term: log.committed_leader_id().term(),
        node_id: *log.committed_leader_id().node_id(),
        index: log.index(),
    };
    let membership = committed.membership();
    let observation = CommittedGroupConfiguration {
        raft_group_id: group,
        leader_id: after.id,
        leader_term: after.vote.leader_id().term(),
        applied_log_id: convert(applied),
        membership_log_id: convert(log_id),
        voter_sets: membership.get_joint_config().clone(),
        learners: membership.learner_ids().collect(),
        nodes: membership
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect(),
    };
    observation
        .validate()
        .map_err(tonic::Status::failed_precondition)?;
    Ok(observation)
}

/// Observe applied uniform or joint configuration through its actual leader.
/// Older peers that omit the capability fail closed; there is no metrics fallback.
pub async fn confirm_group_configuration(
    group: RaftGroupId,
    leader_id: u64,
    address: &str,
    timeout: Duration,
) -> Result<CommittedGroupConfiguration, String> {
    if leader_id == 0 || timeout.is_zero() {
        return Err("configuration observation requires a non-zero leader and timeout".to_owned());
    }
    let network = GrpcRaftNetwork::new(group, leader_id, address);
    let mut client = network.client().map_err(|error| error.to_string())?;
    let mut request = tonic::Request::new(RejoinBarrierRequestV1 {
        raft_group_id: group.0,
        protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
        include_configuration: true,
        target_node_id: leader_id,
        ..Default::default()
    });
    request.set_timeout(timeout);
    let response = client
        .rejoin_barrier(request)
        .await
        .map_err(|error| format!("confirm data configuration: {error}"))?
        .into_inner();
    let vote: UrsulaVote = decode_wire(&response.vote, "configuration barrier vote")
        .map_err(|error| error.to_string())?;
    let observation: CommittedGroupConfiguration =
        decode_wire(&response.configuration, "configuration barrier certificate")
            .map_err(|error| error.to_string())?;
    observation.validate()?;
    if !vote.is_committed()
        || *vote.leader_id().node_id() != leader_id
        || observation.raft_group_id != group
        || observation.leader_id != leader_id
        || observation.leader_term != vote.leader_id().term()
        || observation.applied_log_id.index < response.index
    {
        return Err("inconsistent data configuration certificate".to_owned());
    }
    Ok(observation)
}

/// Collect real quorum evidence for every declared bootstrap group under one
/// total deadline. At most RF requests run at once; only its current leader can
/// answer. A valid certificate that differs from the recipe is a hard error.
pub async fn collect_bootstrap_memberships(
    bootstrap: &ursula_control::ClusterBootstrap,
    timeout: Duration,
) -> Result<BTreeMap<RaftGroupId, VerifiedGroupMembership>, String> {
    use futures_util::StreamExt;

    let bootstrap = bootstrap.clone().normalize()?;
    if timeout.is_zero() {
        return Err("bootstrap membership deadline must be non-zero".to_owned());
    }
    crate::rt::time::timeout(timeout, async {
        let mut result = BTreeMap::new();
        for (group, voters) in &bootstrap.voters {
            let attempts = voters.iter().map(|id| {
                let address = bootstrap.nodes.get(id).map(|node| node.cluster_url.clone());
                let id = *id;
                let group = *group;
                async move {
                    let address = address
                        .ok_or_else(|| format!("bootstrap voter {id} has no registered origin"))?;
                    confirm_group_membership(group, id, &address, timeout).await
                }
            });
            let mut pending = futures_util::stream::iter(attempts).buffer_unordered(voters.len());
            let mut proof = None;
            let mut errors = Vec::new();
            while let Some(attempt) = pending.next().await {
                let observed = match attempt {
                    Ok(observed) => observed,
                    Err(error) => {
                        errors.push(error);
                        continue;
                    }
                };
                if observed.membership.voters != *voters || !observed.membership.learners.is_empty()
                {
                    return Err(format!(
                        "group {} committed membership differs from settled bootstrap voters",
                        group.0
                    ));
                }
                for (id, address) in &observed.nodes {
                    let node = bootstrap
                        .nodes
                        .get(id)
                        .ok_or_else(|| format!("certificate node {id} is not registered"))?;
                    let normalized = ursula_control::NodeRegistration {
                        node_id: *id,
                        client_url: crate::grpc::normalize_grpc_endpoint(address.clone()),
                        cluster_url: crate::grpc::normalize_grpc_endpoint(address.clone()),
                        admin_url: crate::grpc::normalize_grpc_endpoint(address.clone()),
                        labels: BTreeMap::new(),
                    }
                    .normalize()?;
                    if normalized.cluster_url != node.cluster_url {
                        return Err(format!(
                            "group {} node {id} committed endpoint differs from trusted directory",
                            group.0
                        ));
                    }
                }
                proof = Some(observed.membership);
                break;
            }
            let proof = proof.ok_or_else(|| {
                format!(
                    "group {} has no certified leader: {}",
                    group.0,
                    errors.join("; ")
                )
            })?;
            result.insert(*group, proof);
        }
        Ok(result)
    })
    .await
    .map_err(|error| format!("bootstrap membership collection deadline: {error}"))?
}
