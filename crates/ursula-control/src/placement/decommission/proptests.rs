//! Decommission property tests D1 to D6 of the design (issue #768), over
//! states the kernel reaches from random commands, decommissions included,
//! and random node eligibility. Every planned move must also be one the
//! kernel accepts, in order.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::prelude::ProptestConfig;
use proptest::test_runner::RngSeed;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_shard::RaftGroupId;

use super::BlockReason;
use super::DecommissionPlan;
use super::NodeEligibility;
use super::free_slots;
use super::plan_decommission;
use crate::placement::Cluster;
use crate::placement::ControlApplied;
use crate::placement::ControlCommand;
use crate::placement::GroupPlacement;
use crate::placement::Target;
use crate::placement::proptests::reachable;
use crate::placement::samples::node;

fn ensure(condition: bool, violation: impl FnOnce() -> String) -> Result<(), String> {
    if condition { Ok(()) } else { Err(violation()) }
}

fn decommissioning(cluster: &Cluster, node: NodeId) -> bool {
    cluster
        .node(node)
        .is_some_and(|record| record.lifecycle == NodeLifecycle::Decommissioning)
}

/// The nodes that could replace a voter of `group` but for their
/// eligibility.
fn qualifying(cluster: &Cluster, group: RaftGroupId, voters: &BTreeSet<NodeId>) -> Vec<NodeId> {
    cluster
        .nodes()
        .iter()
        .filter(|(id, record)| {
            record.lifecycle == NodeLifecycle::Active
                && !voters.contains(id)
                && cluster.retirement(group, **id).is_none()
        })
        .map(|(id, _)| *id)
        .collect()
}

fn check_plan(
    cluster: &Cluster,
    eligibility: &BTreeMap<NodeId, NodeEligibility>,
    plan: &DecommissionPlan,
) -> Result<(), String> {
    let limit = cluster.params().max_concurrent_moves.get();
    let moving = cluster.moving();
    // D2, D5: at most the free slots, one left to operators at a limit of
    // two or more.
    let planned = u32::try_from(plan.moves.len())
        .map_err(|_overflow| "D2: more moves than a u32 counts".to_owned())?;
    ensure(planned <= free_slots(limit, moving), || {
        format!("D2: {planned} moves at limit {limit} with {moving} moving")
    })?;
    if limit >= 2 {
        ensure(
            moving.saturating_add(planned) < limit || planned == 0,
            || "D5: no slot left to operators".to_owned(),
        )?;
    }
    let mut load: BTreeMap<NodeId, usize> = cluster
        .nodes()
        .keys()
        .map(|id| {
            let roles = cluster
                .groups()
                .iter()
                .filter(|(_, placement)| placement.role(*id).is_some())
                .count();
            (*id, roles)
        })
        .collect();
    let mut seen: BTreeSet<RaftGroupId> = BTreeSet::new();
    for planned in &plan.moves {
        // D3: one move per group, of a stable group, replacing its smallest
        // decommissioning voter.
        ensure(seen.insert(planned.group), || {
            format!("D3: group {} moved twice", planned.group.0)
        })?;
        let Some(GroupPlacement::Live {
            since,
            target: Target::Stable { voters },
            ..
        }) = cluster.group(planned.group)
        else {
            return Err(format!("D3: group {} is not stable", planned.group.0));
        };
        ensure(planned.expected == *since, || {
            "a move names an old placement".to_owned()
        })?;
        let voters = voters.to_set();
        let leaving: Vec<NodeId> = voters
            .iter()
            .copied()
            .filter(|voter| !planned.to.contains(*voter))
            .collect();
        let added: Vec<NodeId> = planned
            .to
            .iter()
            .filter(|id| !voters.contains(id))
            .collect();
        ensure(leaving.len() == 1 && added.len() == 1, || {
            format!("D3: {voters:?} to {:?} is not one replacement", planned.to)
        })?;
        let smallest_leaving = voters
            .iter()
            .copied()
            .find(|voter| decommissioning(cluster, *voter));
        ensure(smallest_leaving == leaving.first().copied(), || {
            format!("D3: group {} moves {leaving:?} off", planned.group.0)
        })?;
        let target = added[0];
        // D1: active, eligible, outside the group, without a record.
        let candidates = qualifying(cluster, planned.group, &voters);
        ensure(
            candidates.contains(&target)
                && eligibility.get(&target) == Some(&NodeEligibility::Eligible),
            || format!("D1: target {target} of group {}", planned.group.0),
        )?;
        // D4: the fewest roles counting this pass's earlier moves, then the
        // smallest id.
        let key = |id: NodeId| (load.get(&id).copied().unwrap_or(0), id.get());
        let best = candidates
            .iter()
            .copied()
            .filter(|id| eligibility.get(id) == Some(&NodeEligibility::Eligible))
            .min_by_key(|id| key(*id));
        ensure(best == Some(target), || {
            format!("D4: target {target}, the minimum is {best:?}")
        })?;
        let count = load
            .get_mut(&target)
            .ok_or_else(|| format!("target {target} is not a registered node"))?;
        *count = count.saturating_add(1);
    }
    for blocked in &plan.blocked {
        ensure(seen.insert(blocked.group), || {
            format!("D3: group {} planned twice", blocked.group.0)
        })?;
        let placement = cluster
            .group(blocked.group)
            .ok_or_else(|| format!("blocked group {} does not exist", blocked.group.0))?;
        let voters = placement.current_voters().to_set();
        ensure(
            voters.iter().any(|voter| decommissioning(cluster, *voter)),
            || format!("D3: group {} has no decommissioning voter", blocked.group.0),
        )?;
        // D6: the reasons of 8.2.
        match (placement, blocked.reason) {
            (GroupPlacement::Seeded { .. }, BlockReason::GroupNotLive) => {}
            (
                GroupPlacement::Live {
                    target: Target::Stable { .. },
                    ..
                },
                reason @ (BlockReason::NoEligibleTarget | BlockReason::TargetsUnderDiskPressure),
            ) => {
                let candidates = qualifying(cluster, blocked.group, &voters);
                ensure(
                    candidates
                        .iter()
                        .all(|id| eligibility.get(id) != Some(&NodeEligibility::Eligible)),
                    || {
                        format!(
                            "D6: group {} blocked with an eligible target",
                            blocked.group.0
                        )
                    },
                )?;
                let pressure = !candidates.is_empty()
                    && candidates
                        .iter()
                        .all(|id| eligibility.get(id) == Some(&NodeEligibility::DiskPressure));
                ensure(
                    (reason == BlockReason::TargetsUnderDiskPressure) == pressure,
                    || format!("D6: group {} blocked as {reason:?}", blocked.group.0),
                )?;
            }
            (placement, reason) => {
                return Err(format!("D6: {placement:?} blocked as {reason:?}"));
            }
        }
    }
    // Complete: with slots left, every decommissioning node's seeded or
    // stable group is planned.
    if planned < free_slots(limit, moving) {
        for (group, placement) in cluster.groups().iter() {
            let leaving = placement
                .current_voters()
                .iter()
                .any(|voter| decommissioning(cluster, voter));
            ensure(
                !leaving || placement.moving().is_some() || seen.contains(&group),
                || {
                    format!(
                        "group {} of a decommissioning node was not planned",
                        group.0
                    )
                },
            )?;
        }
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        rng_seed: RngSeed::Fixed(768),
        // The seed is fixed, so a failure reproduces without a regression file.
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// D1 to D6, and the kernel accepts every planned move in order.
    #[test]
    fn every_decommission_plan_follows_the_rules(
        node_count in 1_u64..=6,
        group_count in 1_u32..=4,
        rf in 1_u8..=3,
        live in proptest::bool::weighted(0.9),
        steps in proptest::collection::vec(
            (0_u8..13, proptest::num::u8::ANY, proptest::num::u8::ANY, proptest::num::u8::ANY),
            0..80,
        ),
        decommissions in proptest::collection::vec(1_u64..=6, 0..3),
        eligibility in proptest::collection::vec(proptest::option::of(0_u8..3), 7),
    ) {
        let mut driver = reachable(node_count, group_count, rf, live, &steps);
        for id in decommissions {
            let _response = driver.apply(ControlCommand::SetNodeLifecycle {
                node: node(id),
                change: crate::placement::LifecycleChange::Decommission,
                now_ms: 60,
            });
        }
        let eligibility: BTreeMap<NodeId, NodeEligibility> = eligibility
            .iter()
            .enumerate()
            .filter_map(|(slot, value)| {
                let eligibility = match (*value)? {
                    0 => return None,
                    1 => NodeEligibility::DiskPressure,
                    _ => NodeEligibility::Eligible,
                };
                Some((node(u64::try_from(slot).unwrap().checked_add(1).unwrap()), eligibility))
            })
            .collect();
        let plan = plan_decommission(driver.cluster(), &eligibility);
        if let Err(violation) = check_plan(driver.cluster(), &eligibility, &plan) {
            proptest::prop_assert!(false, "{violation}: {plan:?} on {:?}", driver.cluster());
        }
        for planned in plan.moves {
            let response = driver.apply(ControlCommand::RequestMove {
                group: planned.group,
                expected: planned.expected,
                to: planned.to,
                now_ms: 61,
            });
            proptest::prop_assert!(
                matches!(response, Ok(ControlApplied::MoveAccepted { .. })),
                "the kernel refused a planned move: {response:?}"
            );
        }
    }
}
