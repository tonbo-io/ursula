//! The placement view the waits and verifications check against, and each
//! group checked on its target voters in it.
//!
//! `wait`, `wait-ready`, `verify-cluster`, `verify-quorum` and
//! `verify-survivors` take every group, its target voters and whether it is
//! live from one view: the meta leader's. Whether a replica counts as a voter
//! they take from its node's maintenance report. No verb takes a group count,
//! and none reads voters from the manifest's node list.
//!
//! The view is read from `GET /__ursula/placement` at each node's metrics
//! address: the first node that answers names the meta leader, and the
//! leader's own status, naming itself, gives the view. A meta read barrier
//! (`?confirmed=true`) is served on the admin listener only, which a chart's
//! test pod does not reach. A view behind the meta log can only name a
//! placement the replicas' memberships still show: a group whose replicas
//! report other voters fails [`check_groups`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_control::Cluster;
use ursula_control::ClusterView;
use ursula_control::GroupPlacement;
use ursula_control::MetaReplicaStatus;
use ursula_control::PlacementStatus;
use ursula_control::Target;
use ursula_proto::placement::ClusterStage;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ReplicaRole;
use ursula_shard::RaftGroupId;

use crate::metrics::ClusterSnapshot;
use crate::metrics::MetricsClient;
use crate::metrics::RaftGroupView;
use crate::provider::NodeInfo;

/// Every group's target voters, by group id.
pub type Targets = BTreeMap<u32, BTreeSet<u64>>;

/// Why no view could be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ViewRefusal {
    #[error("no node of the manifest names a meta leader that answers as the leader")]
    NoMetaLeader,
    #[error(
        "the meta leader, node {leader}, has not bootstrapped the cluster: its view is {stage:?}"
    )]
    NotBootstrapped { leader: u64, stage: ClusterStage },
}

/// Why a group does not run on its target voters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GroupRefusal {
    #[error("group {group} is seeded: its leader has not reported its voters yet")]
    Seeded { group: u32 },
    #[error("group {group} moves to {to:?}")]
    Moving { group: u32, to: BTreeSet<u64> },
    #[error("group {group}: its target voter {node} gave no metrics")]
    Unobserved { group: u32, node: u64 },
    #[error("group {group}: the target voters observed, {observed:?}, are no quorum of {target:?}")]
    NoQuorum {
        group: u32,
        observed: BTreeSet<u64>,
        target: BTreeSet<u64>,
    },
    #[error(
        "group {group}: node {node} has no participating replica whose voters are exactly \
         {target:?}, with no learner and a leader among them"
    )]
    Replica {
        group: u32,
        node: u64,
        target: BTreeSet<u64>,
    },
}

/// The bootstrapped cluster as the meta leader's own view shows it.
#[derive(Debug, Clone)]
pub struct MetaLeaderView {
    pub leader: u64,
    pub cluster: Cluster,
}

impl MetaLeaderView {
    /// Every group's target voters, while every group is live with no move.
    pub fn targets(&self) -> Result<Targets, GroupRefusal> {
        settled_targets(self.cluster.groups().iter())
    }

    /// The groups whose current voters the view places node `node` among:
    /// it is a voter, or an outgoing one of a move.
    pub fn voter_groups(&self, node: u64) -> BTreeSet<u64> {
        let node = NodeId::new(node);
        self.cluster
            .groups()
            .iter()
            .filter(|(_, placement)| {
                matches!(
                    placement.role(node),
                    Some(ReplicaRole::Voter | ReplicaRole::Outgoing)
                )
            })
            .map(|(group, _)| u64::from(group.0))
            .collect()
    }
}

/// The meta leader's view: the leader the first answering node of `nodes`
/// names, read from that leader's own status once it names itself.
pub async fn read_view(
    client: &MetricsClient,
    nodes: &[NodeInfo],
) -> Result<MetaLeaderView, ViewRefusal> {
    for node in nodes {
        let Some(status) = status_of(client, node).await else {
            continue;
        };
        let Some(leader) = named_leader(&status) else {
            continue;
        };
        let own = if leader == node.id {
            Some(status)
        } else {
            match nodes.iter().find(|candidate| candidate.id == leader) {
                Some(leader_node) => status_of(client, leader_node).await,
                None => None,
            }
        };
        let Some(own) =
            own.filter(|own| own.node == NodeId::new(leader) && named_leader(own) == Some(leader))
        else {
            continue;
        };
        return match own.view {
            ClusterView::Bootstrapped(cluster) => Ok(MetaLeaderView { leader, cluster }),
            other => Err(ViewRefusal::NotBootstrapped {
                leader,
                stage: other.stage(),
            }),
        };
    }
    Err(ViewRefusal::NoMetaLeader)
}

async fn status_of(client: &MetricsClient, node: &NodeInfo) -> Option<PlacementStatus> {
    match client.placement_status(node).await {
        Ok(status) => Some(status),
        Err(error) => {
            tracing::debug!(node_id = node.id, %error, "view: no placement status");
            None
        }
    }
}

fn named_leader(status: &PlacementStatus) -> Option<u64> {
    match &status.meta {
        MetaReplicaStatus::Running {
            leader: Some(leader),
            ..
        } => Some(leader.get()),
        MetaReplicaStatus::Running { leader: None, .. }
        | MetaReplicaStatus::Damaged { .. }
        | MetaReplicaStatus::Absent => None,
    }
}

/// Every group's target voters, or the first group that is seeded or moves.
pub fn settled_targets<'a>(
    groups: impl IntoIterator<Item = (RaftGroupId, &'a GroupPlacement)>,
) -> Result<Targets, GroupRefusal> {
    let ids = |voters: &ursula_proto::placement::VoterSet| {
        voters.iter().map(NodeId::get).collect::<BTreeSet<_>>()
    };
    groups
        .into_iter()
        .map(|(group, placement)| match placement {
            GroupPlacement::Seeded { .. } => Err(GroupRefusal::Seeded { group: group.0 }),
            GroupPlacement::Live {
                target: Target::Moving(moving),
                ..
            } => Err(GroupRefusal::Moving {
                group: group.0,
                to: ids(&moving.to),
            }),
            GroupPlacement::Live {
                target: Target::Stable { voters },
                ..
            } => Ok((group.0, ids(voters))),
        })
        .collect()
}

/// Checks each group on its target voters other than `excluded`: each one
/// gave metrics, its replica participates with exactly the target as its
/// voters, no learner and a leader among them, and the checked voters form
/// a quorum of the target. A node outside a group's target is not checked
/// for that group.
pub fn check_groups(
    targets: &Targets,
    snapshot: &ClusterSnapshot,
    excluded: Option<u64>,
) -> Result<(), GroupRefusal> {
    for (&group, target) in targets {
        let mut observed = BTreeSet::new();
        for &node in target.iter().filter(|node| Some(**node) != excluded) {
            let view = snapshot
                .node(node)
                .ok_or(GroupRefusal::Unobserved { group, node })?;
            if !runs_on(view.group(u64::from(group)), target) {
                return Err(GroupRefusal::Replica {
                    group,
                    node,
                    target: target.clone(),
                });
            }
            observed.insert(node);
        }
        if observed.len() <= target.len() / 2 {
            return Err(GroupRefusal::NoQuorum {
                group,
                observed,
                target: target.clone(),
            });
        }
    }
    Ok(())
}

fn runs_on(replica: Option<&RaftGroupView>, target: &BTreeSet<u64>) -> bool {
    replica.is_some_and(|replica| {
        replica.voter_ids.len() == target.len()
            && replica.voter_ids.iter().copied().collect::<BTreeSet<u64>>() == *target
            && replica.learner_ids.is_empty()
            && replica.participation_ready()
            && replica.last_applied_index.is_some()
            && replica
                .current_leader
                .is_some_and(|leader| target.contains(&leader))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use url::Url;
    use ursula_control::ControlPlaneState;
    use ursula_control::GroupPlacement;
    use ursula_control::Target;
    use ursula_control::samples;
    use ursula_shard::RaftGroupId;

    use super::GroupRefusal;
    use super::check_groups;
    use super::settled_targets;
    use crate::metrics::ClusterSnapshot;
    use crate::provider::NodeInfo;

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            id,
            admin_url: Url::parse(&format!("http://10.0.0.{id}:4438")).unwrap(),
            host: format!("10.0.0.{id}"),
            http_url: None,
            metrics_url: None,
            client_url: None,
        }
    }

    /// Four nodes at three voters per group: group 0 on 1 to 3, group 1 on
    /// 2 to 4, each led by its first voter.
    fn four_nodes() -> (BTreeMap<u32, BTreeSet<u64>>, ClusterSnapshot) {
        let targets = BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3])),
            (1, BTreeSet::from([2, 3, 4])),
        ]);
        let per_node = (1..=4)
            .map(|id| {
                let groups = targets
                    .iter()
                    .filter(|(_, target)| target.contains(&id))
                    .map(|(&group, target)| {
                        crate::metrics::test_group(
                            u64::from(group),
                            id,
                            3,
                            target.first().copied(),
                            Some(9),
                            Some(9),
                            target.iter().copied().collect(),
                        )
                    })
                    .collect();
                crate::metrics::test_view(node(id), groups)
            })
            .collect();
        (targets, ClusterSnapshot { per_node })
    }

    /// Only a live group with a stable target has target voters to check
    /// against: the sample's seeded group 0 and moving group 2 do not.
    #[test]
    fn only_live_stable_groups_have_targets() {
        let ControlPlaneState::Bootstrapped(cluster) = samples::sample_state() else {
            panic!("the sample state is bootstrapped");
        };
        assert_eq!(
            settled_targets(cluster.groups().iter()),
            Err(GroupRefusal::Seeded { group: 0 })
        );
        let live = cluster.groups().iter().filter(|(group, _)| group.0 != 0);
        assert_eq!(
            settled_targets(live),
            Err(GroupRefusal::Moving {
                group: 2,
                to: BTreeSet::from([1, 2]),
            })
        );
        let stable = GroupPlacement::Live {
            since: ursula_proto::placement::PlacementEpoch::new(samples::meta(4, 22)),
            converged_at: samples::membership(5, 23),
            target: Target::Stable {
                voters: samples::voters(&[2, 3, 4]),
            },
        };
        assert_eq!(
            settled_targets([(RaftGroupId(7), &stable)]),
            Ok(BTreeMap::from([(7, BTreeSet::from([2, 3, 4]))]))
        );
    }

    /// Each group is checked on its own target voters only, so a node that
    /// hosts no replica of a group fails nothing, and every way a target
    /// voter's replica differs from its target is refused.
    #[test]
    fn each_group_runs_on_exactly_its_target_voters() {
        let (targets, snapshot) = four_nodes();
        check_groups(&targets, &snapshot, None).expect("both groups run on their targets");

        let replica = |node: u64, group: u32| GroupRefusal::Replica {
            group,
            node,
            target: targets[&group].clone(),
        };
        type Fault = fn(&mut crate::metrics::RaftGroupView);
        let faults: [(&str, Fault); 6] = [
            ("a voter missing", |replica| replica.voter_ids = vec![2, 3]),
            ("another voter", |replica| replica.voter_ids = vec![2, 3, 5]),
            ("a learner", |replica| replica.learner_ids = vec![1]),
            ("a joint membership", |replica| {
                replica.maintenance.membership_joint = true;
            }),
            ("a closed gate", |replica| {
                replica.maintenance.recovery_ready = false;
            }),
            ("a leader outside the target", |replica| {
                replica.current_leader = Some(1);
            }),
        ];
        for (name, fault) in faults {
            let mut faulty = snapshot.clone();
            fault(&mut faulty.per_node[3].groups[0]);
            assert_eq!(
                check_groups(&targets, &faulty, None),
                Err(replica(4, 1)),
                "{name}"
            );
        }

        let mut no_replica = snapshot.clone();
        no_replica.per_node[3].groups.clear();
        assert_eq!(
            check_groups(&targets, &no_replica, None),
            Err(replica(4, 1))
        );
    }

    /// A target voter that gave no metrics is refused unless it is the
    /// excluded one, and the excluded voter's group still needs a quorum of
    /// its target among the others.
    #[test]
    fn an_unobserved_voter_is_refused_unless_excluded() {
        let (targets, mut snapshot) = four_nodes();
        snapshot.per_node.retain(|view| view.node.id != 3);
        assert_eq!(
            check_groups(&targets, &snapshot, None),
            Err(GroupRefusal::Unobserved { group: 0, node: 3 })
        );
        check_groups(&targets, &snapshot, Some(3)).expect("node 3 is excluded");
        assert_eq!(
            check_groups(&targets, &snapshot, Some(2)),
            Err(GroupRefusal::Unobserved { group: 0, node: 3 })
        );

        let single = BTreeMap::from([(0, BTreeSet::from([1]))]);
        assert_eq!(
            check_groups(&single, &snapshot, Some(1)),
            Err(GroupRefusal::NoQuorum {
                group: 0,
                observed: BTreeSet::new(),
                target: BTreeSet::from([1]),
            })
        );
    }
}
