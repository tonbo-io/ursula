//! Unit tests of the decommission planner: target choice, slots and the
//! blocked reasons.

use std::collections::BTreeMap;

use ursula_proto::placement::NodeId;
use ursula_shard::RaftGroupId;

use super::BlockReason;
use super::BlockedMove;
use super::DecommissionMove;
use super::DecommissionPlan;
use super::NodeEligibility;
use super::free_slots;
use super::plan_decommission;
use crate::placement::ControlApplied;
use crate::placement::ControlCommand;
use crate::placement::LifecycleChange;
use crate::placement::MoveLimit;
use crate::placement::samples::Driver;
use crate::placement::samples::node;
use crate::placement::samples::params;
use crate::placement::samples::voters;

const G0: RaftGroupId = RaftGroupId(0);
const G1: RaftGroupId = RaftGroupId(1);
const G2: RaftGroupId = RaftGroupId(2);
const G3: RaftGroupId = RaftGroupId(3);

fn eligible(ids: &[u64]) -> BTreeMap<NodeId, NodeEligibility> {
    ids.iter()
        .map(|id| (node(*id), NodeEligibility::Eligible))
        .collect()
}

fn set_limit(driver: &mut Driver, limit: u32) {
    driver
        .apply(ControlCommand::SetMoveLimit {
            limit: MoveLimit::new(limit),
        })
        .unwrap();
}

#[test]
fn each_group_moves_to_the_least_loaded_eligible_node_and_the_kernel_accepts_it() {
    // Five nodes at RF 3: node 4 votes in groups 1, 2 and 3, every node in
    // three groups.
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 5, 3);
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    let plan = plan_decommission(driver.cluster(), &eligible(&[1, 2, 3, 4, 5]));
    let expected = vec![
        DecommissionMove {
            group: G1,
            expected: driver.since(G1),
            to: voters(&[1, 2, 3]),
        },
        DecommissionMove {
            group: G2,
            expected: driver.since(G2),
            to: voters(&[2, 3, 5]),
        },
        DecommissionMove {
            group: G3,
            expected: driver.since(G3),
            to: voters(&[1, 3, 5]),
        },
    ];
    assert_eq!(plan, DecommissionPlan {
        moves: expected,
        blocked: Vec::new(),
    });
    for planned in plan.moves {
        let accepted = driver.apply(ControlCommand::RequestMove {
            group: planned.group,
            expected: planned.expected,
            to: planned.to,
            now_ms: 50,
        });
        assert!(matches!(accepted, Ok(ControlApplied::MoveAccepted { .. })));
    }
    // Everything moves already: nothing more to plan.
    assert_eq!(
        plan_decommission(driver.cluster(), &eligible(&[1, 2, 3, 4, 5])),
        DecommissionPlan::default()
    );
}

#[test]
fn a_pass_leaves_one_slot_to_operators_at_a_limit_of_two_or_more() {
    assert_eq!(free_slots(0, 0), 0);
    assert_eq!(free_slots(1, 0), 1);
    assert_eq!(free_slots(1, 1), 0);
    assert_eq!(free_slots(2, 0), 1);
    assert_eq!(free_slots(2, 1), 0);
    // A limit lowered below the moving count frees nothing.
    assert_eq!(free_slots(2, 5), 0);
    assert_eq!(free_slots(4, 1), 2);
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 5, 3);
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    for (limit, moves) in [(0, 0), (1, 1), (2, 1), (3, 2), (4, 3)] {
        set_limit(&mut driver, limit);
        let plan = plan_decommission(driver.cluster(), &eligible(&[1, 2, 3, 4, 5]));
        assert_eq!(plan.moves.len(), moves, "limit {limit}");
    }
}

#[test]
fn an_unknown_or_dead_node_is_not_a_target() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 5, 3);
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    // Node 1 has no entry and node 5 an unknown one: neither is a target.
    let mut nodes = eligible(&[2, 3, 4]);
    nodes.insert(node(5), NodeEligibility::Unknown);
    let plan = plan_decommission(driver.cluster(), &nodes);
    // Group 1 needs node 1 or 5, group 3 node 2 or 3.
    assert_eq!(plan.blocked, vec![BlockedMove {
        group: G1,
        reason: BlockReason::NoEligibleTarget,
    }]);
    assert_eq!(
        plan.moves
            .iter()
            .map(|planned| planned.group)
            .collect::<Vec<_>>(),
        vec![G2, G3]
    );
}

#[test]
fn a_group_whose_only_candidates_shed_for_disk_names_the_pressure() {
    // Four nodes at RF 3: node 1 is the only node outside group 1.
    let mut driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    let mut nodes = eligible(&[2, 3, 4]);
    nodes.insert(node(1), NodeEligibility::DiskPressure);
    let plan = plan_decommission(driver.cluster(), &nodes);
    assert!(plan.blocked.contains(&BlockedMove {
        group: G1,
        reason: BlockReason::TargetsUnderDiskPressure,
    }));
    nodes.remove(&node(1));
    let plan = plan_decommission(driver.cluster(), &nodes);
    assert!(plan.blocked.contains(&BlockedMove {
        group: G1,
        reason: BlockReason::NoEligibleTarget,
    }));
}

#[test]
fn a_retired_replica_cordoned_node_or_moving_group_takes_no_move() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    // Group 0 moved off node 1, which owes its teardown report.
    driver.request_move(G0, &[2, 3, 4]).unwrap();
    driver.report(G0, &[2, 3, 4]).unwrap();
    // Group 1 is moving.
    driver.request_move(G1, &[1, 2, 3]).unwrap();
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    let plan = plan_decommission(driver.cluster(), &eligible(&[1, 2, 3, 4]));
    // Group 0's only candidate holds a record, group 1 is left alone, groups
    // 2 and 3 have node 2 and node 3 outside them.
    assert!(plan.blocked.contains(&BlockedMove {
        group: G0,
        reason: BlockReason::NoEligibleTarget,
    }));
    assert!(plan.moves.iter().all(|planned| planned.group != G1));
    assert!(plan.blocked.iter().all(|blocked| blocked.group != G1));
    // A cordoned node is no target either.
    driver.lifecycle(2, LifecycleChange::Cordon).unwrap();
    let plan = plan_decommission(driver.cluster(), &eligible(&[1, 2, 3, 4]));
    assert!(plan.blocked.contains(&BlockedMove {
        group: G2,
        reason: BlockReason::NoEligibleTarget,
    }));
}

#[test]
fn a_seeded_group_is_blocked_until_it_reports() {
    let mut driver = Driver::bootstrapped(&[1, 2, 3, 4], params(&[1, 2, 3, 4], 2, 3, 4));
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    let plan = plan_decommission(driver.cluster(), &eligible(&[1, 2, 3, 4]));
    assert_eq!(plan, DecommissionPlan {
        moves: Vec::new(),
        blocked: vec![BlockedMove {
            group: G1,
            reason: BlockReason::GroupNotLive,
        }],
    });
}
