//! The decommission planner: which moves the meta leader submits to empty
//! decommissioning nodes, and why the rest wait.
//!
//! Each pass takes the decommissioning nodes in ascending id and each of
//! their stable groups in ascending id, and moves the leaving voter to the
//! eligible active node outside the group with the fewest roles, the
//! smallest id on a tie. It plans at most the free move slots and leaves
//! one slot to operators at a limit of two or more. The placement CAS
//! refuses a plan from an old state, so the planner needs no fence.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::state::Cluster;
use super::state::GroupPlacement;
use super::state::Target;

/// Whether a live node may receive a group now, as the meta leader sees
/// it. A live node acknowledged the meta leader within 10 s, and a node the
/// leader has no entry for is not live: absence is the one encoding of that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeEligibility {
    /// Sheds nothing for its WAL disk.
    Eligible,
    /// Sheds leadership for WAL disk pressure.
    DiskPressure,
    /// A state a newer patch of the minor reports, decoded so that an older
    /// `ursulactl` still reads the report. No planner produces it, and the
    /// planner treats it as not eligible.
    #[serde(other)]
    Unknown,
}

/// Why a decommissioning node's group does not move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockReason {
    /// No active node outside the group without a retirement in it is
    /// eligible.
    NoEligibleTarget,
    /// Every node that would qualify sheds for disk pressure.
    TargetsUnderDiskPressure,
    /// The group is still seeded: it moves once it reported.
    GroupNotLive,
    /// A reason a newer patch of the minor reports. No planner here
    /// produces it.
    #[serde(other)]
    Unknown,
}

/// A decommissioning node's group that cannot move now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockedMove {
    pub group: RaftGroupId,
    pub reason: BlockReason,
}

/// A move the meta leader submits as `RequestMove`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecommissionMove {
    pub group: RaftGroupId,
    pub expected: PlacementEpoch,
    pub to: VoterSet,
}

/// One pass of the decommission driver.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecommissionPlan {
    pub moves: Vec<DecommissionMove>,
    pub blocked: Vec<BlockedMove>,
}

/// How many moves a pass may start: the free slots under `limit`, less one
/// left to operators at a limit of two or more.
pub fn free_slots(limit: u32, moving: u32) -> u32 {
    let free = limit.saturating_sub(moving);
    if limit >= 2 {
        free.saturating_sub(1)
    } else {
        free
    }
}

/// The moves that empty decommissioning nodes now, and the groups that
/// cannot move. A cluster under the startup reservation has no
/// decommissioning node (the kernel refuses `Decommission` there), so it
/// plans nothing.
pub fn plan_decommission(
    cluster: &Cluster,
    eligibility: &BTreeMap<NodeId, NodeEligibility>,
) -> DecommissionPlan {
    let mut plan = DecommissionPlan::default();
    let mut slots = free_slots(
        cluster.params().max_concurrent_moves.get(),
        cluster.moving(),
    );
    let mut load: BTreeMap<NodeId, usize> = cluster
        .nodes()
        .keys()
        .map(|node| {
            let roles = cluster
                .groups()
                .iter()
                .filter(|(_, placement)| placement.role(*node).is_some())
                .count();
            (*node, roles)
        })
        .collect();
    let mut handled: BTreeSet<RaftGroupId> = BTreeSet::new();
    let leaving_nodes = cluster
        .nodes()
        .iter()
        .filter(|(_, record)| record.lifecycle == NodeLifecycle::Decommissioning)
        .map(|(node, _)| *node);
    for leaving in leaving_nodes {
        for (group, placement) in cluster.groups().iter() {
            if handled.contains(&group) || placement.role(leaving).is_none() {
                continue;
            }
            let voters = match placement {
                GroupPlacement::Seeded { .. } => {
                    handled.insert(group);
                    plan.blocked.push(BlockedMove {
                        group,
                        reason: BlockReason::GroupNotLive,
                    });
                    continue;
                }
                GroupPlacement::Live {
                    target: Target::Moving(_),
                    ..
                } => continue,
                GroupPlacement::Live {
                    target: Target::Stable { voters },
                    ..
                } => voters,
            };
            handled.insert(group);
            match choose_target(cluster, group, voters, eligibility, &load) {
                Ok(target) => {
                    if slots == 0 {
                        continue;
                    }
                    slots = slots.saturating_sub(1);
                    if let Some(count) = load.get_mut(&target) {
                        *count = count.saturating_add(1);
                    }
                    plan.moves.push(DecommissionMove {
                        group,
                        expected: placement.since(),
                        to: VoterSet::from_first(
                            target,
                            voters.iter().filter(|voter| *voter != leaving),
                        ),
                    });
                }
                Err(reason) => plan.blocked.push(BlockedMove { group, reason }),
            }
        }
    }
    plan
}

/// The node that replaces a leaving voter of `group`, whose voters are
/// `voters`: an eligible node outside them that the kernel's target check
/// accepts (so active and without a retirement in the group), with the
/// fewest roles, the smallest id on a tie.
fn choose_target(
    cluster: &Cluster,
    group: RaftGroupId,
    voters: &VoterSet,
    eligibility: &BTreeMap<NodeId, NodeEligibility>,
    load: &BTreeMap<NodeId, usize>,
) -> Result<NodeId, BlockReason> {
    let qualifying: Vec<NodeId> = cluster
        .nodes()
        .keys()
        .copied()
        .filter(|node| {
            !voters.contains(*node) && cluster.check_target(group, *node, voters).is_ok()
        })
        .collect();
    let mut best: Option<(usize, NodeId)> = None;
    for node in qualifying.iter().copied() {
        if eligibility.get(&node) != Some(&NodeEligibility::Eligible) {
            continue;
        }
        let count = load.get(&node).copied().unwrap_or(0);
        let better = best.is_none_or(|(best_count, best_node)| {
            count < best_count || (count == best_count && node.get() < best_node.get())
        });
        if better {
            best = Some((count, node));
        }
    }
    if let Some((_, node)) = best {
        return Ok(node);
    }
    let all_under_pressure = !qualifying.is_empty()
        && qualifying
            .iter()
            .all(|node| eligibility.get(node) == Some(&NodeEligibility::DiskPressure));
    Err(if all_under_pressure {
        BlockReason::TargetsUnderDiskPressure
    } else {
        BlockReason::NoEligibleTarget
    })
}

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod tests;
