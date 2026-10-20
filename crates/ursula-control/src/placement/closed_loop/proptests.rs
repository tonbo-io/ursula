//! The random closed loop (15.1): 1 to 6 nodes, 1 to 4 groups and
//! replication factors 1 to 3, seeded or formed, run through up to 200
//! random steps of every actor and of the adversary, checked after each
//! against C1 to C4 and C7 to C9. Then the faults stop, the runbook's moves
//! are made, and C5 must hold within a bound of fair rounds.
//!
//! The adversary crashes, restarts with regressed views, cuts nodes off on
//! the data plane (they keep their gates, and a cut-off leader keeps
//! leading its own copy once the others elected another), lags meta
//! replicas, keeps step futures past their leadership, holds reports in
//! flight, drops and delays second steps, lets a node that would win
//! campaign (an outgoing voter on the uniform entry included), moves
//! leadership and takes meta down. Each run also draws one family of
//! operator recoveries: lifecycle changes with the decommission driver and
//! the sweep, meta recovery plans, or data recovery plans. Combining them
//! reaches states the design leaves to the operator (a removed voter after
//! a meta plan, a data plan below a meta plan's loss), which the scripted
//! traces cover instead. A data plan here keeps no less than the last
//! certified membership, the only floor of 7.4's tool, and waits for the
//! group's reports in flight, as the tool acts only on a group stalled
//! longer than a report takes.
//!
//! The tally shows the random runs, not only the traces, reach the
//! removal and the sweep (C9), the placement CAS and the leadership proof.
//! Step 0's `NotAMember` wait and outgoing voters' wins are counted, not
//! asserted. The wait needs a leader to plan between the commit that
//! removes it and the next settle, which steps it down, and the fixed-seed
//! runs reach neither: the scripted C2 trace pins the wait, and the
//! reconciler's scripted model test the outgoing win.

use std::cell::Cell;

use proptest::prelude::ProptestConfig;
use proptest::prelude::any;
use proptest::test_runner::RngSeed;
use proptest::test_runner::TestCaseError;
use proptest::test_runner::TestCaseResult;
use proptest::test_runner::TestRunner;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::world::Action;
use super::world::Tally;
use super::world::ViewAt;
use super::world::Violation;
use super::world::World;
use crate::placement::ControlCommand;
use crate::placement::GroupPlacement;
use crate::placement::LifecycleChange;
use crate::placement::MoveLimit;

/// Fair rounds that form every group of a fresh cluster.
const FORM_ROUNDS: usize = 10;

/// Fair rounds within which C5 must hold once the faults stopped: enough
/// for every move in flight to replace each voter in a handful of rounds,
/// for each retired replica's seven teardown steps and report, and for the
/// runbook's moves after a recovery plan.
const SETTLE_ROUNDS: usize = 80;

/// Which operator recoveries a run draws.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recoveries {
    /// Cordons, uncordons and decommissions, with the decommission driver
    /// and the removed-node sweep.
    Lifecycles,
    MetaPlans,
    DataPlans,
}

/// `x mod n` as an index.
fn index(x: u8, n: usize) -> usize {
    usize::from(x).checked_rem(n).unwrap()
}

/// Whether `x` falls in one of `n` buckets.
fn one_in(x: u8, n: u8) -> bool {
    x.checked_rem(n) == Some(0)
}

/// A state of the current meta log, or the latest.
fn view_at(world: &World, z: u8) -> ViewAt {
    if one_in(z, 3) {
        ViewAt::Latest
    } else {
        ViewAt::Entry(index(z, world.meta.len()))
    }
}

/// `voters` with one of them replaced by a node outside them, when there is
/// one.
fn swap_one(world: &World, voters: &VoterSet, z: u8) -> VoterSet {
    let current: Vec<NodeId> = voters.iter().collect();
    let out = current[index(z, current.len())];
    let outside: Vec<NodeId> = world
        .nodes
        .keys()
        .copied()
        .filter(|node| !voters.contains(*node))
        .collect();
    if outside.is_empty() {
        return voters.clone();
    }
    let incoming = outside[index(z.checked_shr(2).unwrap(), outside.len())];
    VoterSet::from_first(incoming, current.into_iter().filter(|node| *node != out))
}

/// An operator's move of the group: a cancel now and then, else one voter
/// of its target swapped.
fn operator_move(world: &World, group: RaftGroupId, z: u8) -> Option<ControlCommand> {
    let placement = world.placement(group)?;
    let GroupPlacement::Live { since, .. } = &placement else {
        return None;
    };
    let to = match placement.moving() {
        Some(moving) if one_in(z, 4) => moving.from.clone(),
        Some(_) | None => swap_one(world, placement.target_voters(), z),
    };
    Some(ControlCommand::RequestMove {
        group,
        expected: *since,
        to,
        now_ms: 0,
    })
}

/// A data recovery plan of the group, when it formed and has no report in
/// flight: it keeps a position at or above its floor.
fn data_plan(world: &World, group: RaftGroupId, z: u8) -> Option<Action> {
    if world.report_in_flight(group) {
        return None;
    }
    let length = world.raft(group)?.committed_configs.len();
    let floor = world.plan_floor(group);
    let span = length.checked_sub(floor)?;
    if span == 0 {
        return None;
    }
    let at = floor.checked_add(index(z, span))?;
    Some(Action::DataPlan { group, at })
}

/// The step `(op, x, y, z)` chooses in `world`.
fn generate(world: &World, recoveries: Recoveries, (op, x, y, z): (u8, u8, u8, u8)) -> Action {
    let nodes: Vec<NodeId> = world.nodes.keys().copied().collect();
    let groups: Vec<RaftGroupId> = world.groups.keys().copied().collect();
    let node = nodes[index(x, nodes.len())];
    let group = groups[index(y, groups.len())];
    let host = Action::Host(node);
    match op.checked_rem(24).unwrap() {
        0..=3 => host,
        4..=6 => match world.leader(group) {
            Some(leader) => Action::Reconcile {
                node: leader,
                group,
                hold: one_in(z, 5),
            },
            None => Action::Settle(group),
        },
        7 => Action::Reconcile {
            node: world.deposed_leader(group).unwrap_or(node),
            group,
            hold: one_in(z, 3),
        },
        8 => Action::Seed(node, group),
        9 | 10 => Action::Settle(group),
        11 => match world.teardown_of(node) {
            Some(torn) => Action::Teardown(node, torn),
            None => host,
        },
        12 | 13 => operator_move(world, group, z).map_or(host, Action::Command),
        14 => match recoveries {
            Recoveries::Lifecycles => {
                let change = match index(z, 3) {
                    0 => LifecycleChange::Cordon,
                    1 => LifecycleChange::Uncordon,
                    _ => LifecycleChange::Decommission,
                };
                Action::Command(ControlCommand::SetNodeLifecycle {
                    node,
                    change,
                    now_ms: 0,
                })
            }
            Recoveries::MetaPlans | Recoveries::DataPlans => Action::Elect(group),
        },
        15 => {
            if one_in(z, 2) {
                Action::Decommission
            } else {
                Action::Sweep
            }
        }
        16 => {
            if world.alive(node) {
                Action::Crash(node)
            } else {
                Action::Restart(node, view_at(world, z))
            }
        }
        17 => {
            if world.partitioned(node) {
                Action::Heal(node)
            } else {
                Action::Partition(node)
            }
        }
        18 => Action::View(node, view_at(world, z)),
        19 => {
            if one_in(z, 2) {
                Action::DeliverStep(group)
            } else {
                Action::DeliverReports
            }
        }
        20 => match index(z, 5) {
            0 => Action::Elect(group),
            1 => Action::DropSecondStep(group),
            2 => Action::DelaySecondStep(group),
            3 => Action::Campaign(group, node),
            _ => Action::Transfer(group, node),
        },
        21 => match recoveries {
            Recoveries::MetaPlans if one_in(z, 3) => {
                let span = world.meta.len().saturating_sub(world.boot_len);
                let keep = world
                    .boot_len
                    .checked_add(index(x, span.checked_add(1).unwrap()))
                    .unwrap();
                Action::MetaPlan { keep }
            }
            Recoveries::DataPlans if one_in(z, 2) => data_plan(world, group, x).unwrap_or(host),
            Recoveries::Lifecycles | Recoveries::MetaPlans | Recoveries::DataPlans => host,
        },
        22 => {
            if !world.meta.up {
                Action::MetaAvailable(true)
            } else if one_in(z, 4) {
                Action::MetaAvailable(false)
            } else {
                host
            }
        }
        _ => Action::Command(ControlCommand::SetMoveLimit {
            limit: MoveLimit::new(u32::try_from(index(z, 4)).unwrap()),
        }),
    }
}

/// The fixed-seed config: 64 long runs.
fn config() -> ProptestConfig {
    ProptestConfig {
        cases: 64,
        rng_seed: RngSeed::Fixed(768),
        // The seed is fixed, so a failure reproduces without a regression file.
        failure_persistence: None,
        max_shrink_iters: 256,
        ..ProptestConfig::default()
    }
}

/// A failed check, with the actions that led to it.
fn fail_on(world: &World, result: Result<(), Violation>) -> TestCaseResult {
    result.map_err(|violation| TestCaseError::fail(format!("{violation}\n{}", world.history())))
}

/// C1 to C4 and C7 to C9 after every step of a random run, and C5 once its
/// faults stopped.
#[test]
fn the_closed_loop_keeps_c1_to_c9() {
    let tally = Cell::new(Tally::default());
    let strategy = (
        1_u64..=6,
        1_u32..=4,
        1_u8..=3,
        0_u8..3,
        proptest::bool::ANY,
        proptest::collection::vec(any::<(u8, u8, u8, u8)>(), 0..=200),
    );
    let result = TestRunner::new(config()).run(
        &strategy,
        |(nodes, groups, rf, family, formed, steps)| -> TestCaseResult {
            let recoveries = match family {
                0 => Recoveries::Lifecycles,
                1 => Recoveries::MetaPlans,
                _ => Recoveries::DataPlans,
            };
            let mut world = World::new(nodes, groups, rf);
            if formed {
                let result = world.quiesce(FORM_ROUNDS);
                fail_on(&world, result)?;
            }
            for raw in steps {
                let action = generate(&world, recoveries, raw);
                let result = world.step(action);
                fail_on(&world, result)?;
            }
            let result = world.stop_faults();
            fail_on(&world, result)?;
            let result = world.quiesce(SETTLE_ROUNDS);
            fail_on(&world, result)?;
            tally.set(tally.get().merge(world.tally()));
            Ok(())
        },
    );
    if let Err(failure) = result {
        panic!("{failure}");
    }
    let tally = tally.get();
    assert!(tally.moves > 0, "{tally:?}");
    assert!(tally.retired > 0, "{tally:?}");
    assert!(tally.meta_plans > 0, "{tally:?}");
    assert!(tally.data_plans > 0, "{tally:?}");
    assert!(tally.removals > 0, "{tally:?}");
    assert!(tally.sweeps > 0, "{tally:?}");
    assert!(tally.stale_reports > 0, "{tally:?}");
    assert!(tally.unproven_reports > 0, "{tally:?}");
}

/// Every cluster shape forms without faults: every group initializes on
/// its seed, reports and settles, and C1 to C9 hold throughout.
#[test]
fn every_cluster_shape_forms_and_settles() {
    for nodes in 1..=6 {
        for groups in 1..=4 {
            for rf in 1..=3 {
                let mut world = World::new(nodes, groups, rf);
                if let Err(violation) = world.quiesce(FORM_ROUNDS) {
                    panic!(
                        "{nodes} nodes, {groups} groups, rf {rf}: {violation}\n{}",
                        world.history()
                    );
                }
            }
        }
    }
}
