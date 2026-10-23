//! The scripted traces of C6 (15.1), one for C2, two for C4 and one of an
//! outgoing voter's win. Each negative trace is a formation and a
//! scenario. It runs once with every rule on and must pass. It then forms
//! again with every rule on, turns its rule off for the scenario alone, and
//! must fail the check named with it within the scenario. Every C6 trace
//! but (i) forms a cluster of nodes 1 to 5 with group 0 stable on
//! `{1, 2, 3}` and led by node 1. C2's, C4's and the outgoing voter's run
//! at replication factor 1.
//!
//! Two rules switch off model code only: `LeadershipProof` (b) the
//! reconcile shell's proof and `SweepAnswer` (g) the removed-node sweep,
//! which live outside this crate. Their traces show that the model's
//! checks catch a shell without them, not that the shells have them.

use ursula_proto::placement::TeardownStage;
use ursula_shard::RaftGroupId;

use super::world::Action;
use super::world::Invariant;
use super::world::Journal;
use super::world::Outage;
use super::world::ViewAt;
use super::world::Violation;
use super::world::World;
use super::world::node;
use super::world::voters;
use crate::placement::ControlCommand;
use crate::placement::LifecycleChange;
use crate::placement::MembershipConfig;
use crate::placement::MoveLimit;
use crate::placement::rules::Rule;
use crate::placement::rules::disable;

/// Fair rounds that form every group of a fresh cluster.
const FORM: usize = 10;

/// Rounds a scripted drive may take.
const DRIVE: usize = 20;

const GROUP: RaftGroupId = RaftGroupId(0);

type Script = fn(&mut World) -> Result<(), Violation>;

/// A negative trace: a cluster of five nodes, how it forms and the
/// scenario its rule guards.
struct Trace {
    rf: u8,
    groups: u32,
    form: Script,
    scenario: Script,
}

impl Trace {
    /// Group 0 of one group formed on `{1, 2, 3}`, then `scenario`.
    fn formed(scenario: Script) -> Self {
        Self {
            rf: 3,
            groups: 1,
            form,
            scenario,
        }
    }

    /// The trace forms and runs its scenario with every rule on and
    /// passes. It forms again with every rule on, and with `rule` off for
    /// the scenario alone fails one of `caught_by` there.
    fn negative(&self, rule: Rule, caught_by: &[Invariant]) {
        let mut on = World::new(5, self.groups, self.rf);
        let passed = (self.form)(&mut on).and_then(|()| (self.scenario)(&mut on));
        if let Err(violation) = passed {
            panic!("with every rule on: {violation}\n{}", on.history());
        }
        let mut world = World::new(5, self.groups, self.rf);
        if let Err(violation) = (self.form)(&mut world) {
            panic!(
                "forming with every rule on: {violation}\n{}",
                world.history()
            );
        }
        let formed = world.steps();
        let off = disable(rule);
        let result = (self.scenario)(&mut world);
        drop(off);
        match result {
            Ok(()) => panic!("with {rule:?} off the trace passes\n{}", world.history()),
            Err(violation) => assert!(
                caught_by.contains(&violation.invariant) && world.steps() > formed,
                "with {rule:?} off, {} steps after the formation's {formed}: {violation}\n{}",
                world.steps().saturating_sub(formed),
                world.history()
            ),
        }
    }
}

fn form(world: &mut World) -> Result<(), Violation> {
    world.quiesce(FORM)?;
    world.ensure(
        world.is_stable(GROUP, &[1, 2, 3]) && world.leader(GROUP) == Some(node(1)),
        "group 0 formed on {1, 2, 3} led by node 1",
    )
}

/// Replication factor 1: group 0 formed on `{1}` led by node 1.
fn form_single(world: &mut World) -> Result<(), Violation> {
    world.quiesce(FORM)?;
    world.ensure(
        world.is_stable(GROUP, &[1]) && world.leader(GROUP) == Some(node(1)),
        "group 0 formed on {1} led by node 1",
    )
}

fn reconcile(world: &mut World, id: u64, hold: bool) -> Result<(), Violation> {
    world.step(Action::Reconcile {
        node: node(id),
        group: GROUP,
        hold,
    })
}

/// Leadership moves to `id` once the group settled: the transfer
/// completes in the next settle.
fn transfer(world: &mut World, id: u64) -> Result<(), Violation> {
    world.step(Action::Settle(GROUP))?;
    world.step(Action::Transfer(GROUP, node(id)))?;
    world.step(Action::Settle(GROUP))?;
    world.ensure(world.leader(GROUP) == Some(node(id)), "leadership moved")
}

fn committed_on(world: &World, ids: &[u64]) -> bool {
    world.committed_voters(GROUP) == Some(voters(ids))
}

/// Node 4 joins group 0 as a caught-up learner of the move to `{1, 2, 4}`.
fn learner_four(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| {
        world.learners(GROUP).contains(&node(4)) && world.caught_up(GROUP, 4)
    })
}

/// C6(a) and the learner-only suffix of 5.6 (C3's pinned case): a move to
/// `{1, 2, 4}` is retargeted to `{1, 2, 5}`, leader 1 converges and node 4
/// is retired. Node 2 leads next with its view still at the first move:
/// it may add node 4 back as a learner, which node 4's teardown answers by
/// waiting, but promoting it needs a confirmed view, which shows the move
/// settled.
fn confirmation_before_a_voter_addition(world: &mut World) -> Result<(), Violation> {
    learner_four(world)?;
    world.freeze(2)?;
    world.request_move(GROUP, &[1, 2, 5])?;
    world.drive(GROUP, &[5], DRIVE, |world| {
        world.is_stable(GROUP, &[1, 2, 5])
    })?;
    transfer(world, 2)?;
    // Node 2 plans from its view of the first move: node 4 is a learner
    // again, which needs no confirmation.
    reconcile(world, 2, false)?;
    world.ensure(
        world.learners(GROUP).contains(&node(4)),
        "node 2 added node 4 back as a learner",
    )?;
    // Node 4 learns its retirement, and the group holds its teardown at
    // step 0 while it lists node 4.
    world.step(Action::Host(node(4)))?;
    world.step(Action::Teardown(node(4), GROUP))?;
    world.ensure(
        world.teardown_stage(4, GROUP) == Some(TeardownStage::Ask),
        "node 4's teardown waits at step 0 while it is a learner",
    )?;
    world.step(Action::Settle(GROUP))?;
    world.drive(GROUP, &[], DRIVE, |world| {
        !world.learners(GROUP).contains(&node(4))
    })?;
    world.finish_teardown(4, GROUP)
}

/// C6(a), a step outliving its leadership, which only removing the
/// learners before reporting `Converged` keeps safe: leader 1 plans the
/// promotion of node 4 and its future is kept, node 2 leads a cancel to
/// its convergence, and leader 1 leads again before the kept step runs.
/// The converge removed the learner first, so the kept step's CAS fails.
fn a_step_outliving_its_leadership(world: &mut World) -> Result<(), Violation> {
    learner_four(world)?;
    reconcile(world, 1, false)?;
    reconcile(world, 1, true)?;
    transfer(world, 2)?;
    world.request_move(GROUP, &[1, 2, 3])?;
    world.drive(GROUP, &[], DRIVE, |world| {
        world.is_stable(GROUP, &[1, 2, 3])
    })?;
    transfer(world, 1)?;
    world.step(Action::DeliverStep(GROUP))
}

/// C6(b): leader 1, cut off on the data plane after reaching `{1, 2, 4}`,
/// keeps leading its own copy. Node 2 leads the retarget to `{1, 2, 5}`,
/// and the move is retargeted back to `{1, 2, 4}`. Node 1's view is
/// current and its copy is at the target: only the proof stops its report,
/// which would retire node 5, a voter. The proof switched off is the
/// model's.
fn the_leadership_proof(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| committed_on(world, &[1, 2, 4]))?;
    world.step(Action::Partition(node(1)))?;
    world.request_move(GROUP, &[1, 2, 5])?;
    world.drive(GROUP, &[5], DRIVE, |world| committed_on(world, &[1, 2, 5]))?;
    world.ensure(
        world.leader(GROUP) == Some(node(2)),
        "node 2 leads the retarget",
    )?;
    world.request_move(GROUP, &[1, 2, 4])?;
    reconcile(world, 1, false)
}

/// C6(c) within one meta history (5.7): leader 1 proves at `{1, 2, 4}` and
/// its report is delayed. Node 2 leads a retarget to `{1, 2, 5}`, and the
/// move is retargeted back to `{1, 2, 4}`. The report's voters equal the
/// target again: only the placement CAS refuses it.
fn the_placement_cas(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| committed_on(world, &[1, 2, 4]))?;
    reconcile(world, 1, true)?;
    transfer(world, 2)?;
    world.request_move(GROUP, &[1, 2, 5])?;
    world.drive(GROUP, &[5], DRIVE, |world| committed_on(world, &[1, 2, 5]))?;
    world.request_move(GROUP, &[1, 2, 4])?;
    world.step(Action::DeliverReports)
}

/// C6(c) across a meta plan whose cut keeps the entry the report names:
/// leader 1's first report is delayed, its second converges, node 3 is
/// torn down and the group moves back onto it. The plan keeps the log up
/// to the first move, so the delayed report's voters equal the target
/// again. The history event moved the placement key, so the CAS refuses
/// the report, which would retire node 3, a voter.
fn the_placement_cas_across_a_meta_plan(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    let keep = world.meta.len();
    world.drive(GROUP, &[4], DRIVE, |world| committed_on(world, &[1, 2, 4]))?;
    reconcile(world, 1, true)?;
    reconcile(world, 1, false)?;
    world.ensure(
        world.is_stable(GROUP, &[1, 2, 4]),
        "the second report converged",
    )?;
    world.finish_teardown(3, GROUP)?;
    world.request_move(GROUP, &[1, 2, 3])?;
    world.drive(GROUP, &[3], DRIVE, |world| committed_on(world, &[1, 2, 3]))?;
    world.step(Action::MetaPlan { keep })?;
    world.step(Action::DeliverReports)
}

/// Leader 1 reaches `{1, 2, 4}` and its report goes in flight, then a data
/// plan whose floor predates the report cuts the group back to
/// `{1, 2, 3}`, and the report lands: it retires node 3, which the group
/// lists as a voter again. A data plan below a report voids the claims the
/// report certified, so only the group's answers keep node 3's teardown
/// from destroying a voter's log.
fn a_report_a_data_plan_makes_wrong(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| committed_on(world, &[1, 2, 4]))?;
    reconcile(world, 1, true)?;
    let at = world.position(GROUP, &[1, 2, 3]).unwrap_or(0);
    world.step(Action::DataPlan { group: GROUP, at })?;
    world.step(Action::DeliverReports)?;
    world.ensure(
        world.is_stable(GROUP, &[1, 2, 4]) && committed_on(world, &[1, 2, 3]),
        "the report landed after the cut",
    )
}

/// C6(d), a report a data plan made wrong: node 3's teardown waits at step
/// 0 while the group lists it as a voter.
fn the_answers_against_a_wrong_report(world: &mut World) -> Result<(), Violation> {
    a_report_a_data_plan_makes_wrong(world)?;
    world.step(Action::Host(node(3)))?;
    for _ in 0..8 {
        world.step(Action::Teardown(node(3), GROUP))?;
        world.step(Action::Host(node(3)))?;
    }
    world.ensure(
        world.teardown_stage(3, GROUP) == Some(TeardownStage::Ask),
        "node 3's teardown waits at step 0",
    )
}

/// C6(d), a re-placement before step 5 and a stopped learner promoted from
/// an old gate answer: node 3's teardown passes step 0, a meta plan drops
/// its retirement and the operator places it again. The leader adds it
/// back as a learner, which catches up before its replica stops, and then
/// promotes it from that old answer. Only step 5's second ask keeps the
/// voter's log.
fn the_answers_against_a_re_placement(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| {
        world.is_stable(GROUP, &[1, 2, 4])
    })?;
    world.step(Action::Host(node(3)))?;
    world.step(Action::Teardown(node(3), GROUP))?;
    world.ensure(
        world.teardown_stage(3, GROUP) == Some(TeardownStage::Fence),
        "node 3's teardown passed step 0",
    )?;
    let keep = world.meta.len();
    world.step(Action::MetaPlan { keep })?;
    world.request_move(GROUP, &[1, 2, 3])?;
    reconcile(world, 1, false)?;
    world.step(Action::Settle(GROUP))?;
    world.ensure(
        world.learners(GROUP).contains(&node(3)) && world.caught_up(GROUP, 3),
        "node 3 is a caught-up learner again",
    )?;
    // Fence, unregister, stop: the replica no longer runs.
    for _ in 0..3 {
        world.step(Action::Teardown(node(3), GROUP))?;
    }
    reconcile(world, 1, false)?;
    reconcile(world, 1, false)?;
    world.ensure(
        committed_on(world, &[1, 2, 3]),
        "leader 1 promoted the stopped node 3",
    )?;
    // Work ends, then step 5 asks again and, without its answer, retires.
    for _ in 0..3 {
        world.step(Action::Teardown(node(3), GROUP))?;
    }
    Ok(())
}

/// C6(e): node 4's view lags at the move to `{1, 2, 4}`, which a retarget
/// to `{1, 2, 5}` abandoned. Only a confirmed view creates a replica.
fn creation_only_when_confirmed(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.freeze(4)?;
    world.request_move(GROUP, &[1, 2, 5])?;
    world.step(Action::Host(node(4)))?;
    world.ensure(
        world.replica(node(4), GROUP).is_none(),
        "node 4 created nothing",
    )
}

/// C6(f): node 3 is retired at the converge onto `{1, 2, 4}`, reports its
/// teardown and is placed again, and the group moves back onto it. A meta
/// plan keeps the log up to the converge: the history event must drop the
/// retirement, whose claim the group's later membership broke.
fn the_history_drop(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| {
        world.is_stable(GROUP, &[1, 2, 4])
    })?;
    let keep = world.meta.len();
    world.finish_teardown(3, GROUP)?;
    world.request_move(GROUP, &[1, 2, 3])?;
    world.drive(GROUP, &[3], DRIVE, |world| committed_on(world, &[1, 2, 3]))?;
    world.step(Action::MetaPlan { keep })
}

/// C6(g): node 4 is decommissioned and the decommission driver moves the
/// group onto node 5. The report goes in flight, a data plan whose floor
/// predates it cuts the group back to `{1, 2, 4}`, and the report lands:
/// node 4 is removed while the group lists it as a voter. The removed node
/// keeps the replica open while it counts, and the sweep keeps its objects
/// while the group answers it is a voter. The sweep switched off is the
/// model's.
fn the_close_rule_and_the_sweep(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[1, 2, 4])?;
    world.drive(GROUP, &[4], DRIVE, |world| {
        world.is_stable(GROUP, &[1, 2, 4])
    })?;
    world.step(Action::Command(ControlCommand::SetNodeLifecycle {
        node: node(4),
        change: LifecycleChange::Decommission,
        now_ms: 0,
    }))?;
    world.step(Action::Decommission)?;
    world.ensure(
        world
            .placement(GROUP)
            .is_some_and(|placement| *placement.target_voters() == voters(&[1, 2, 5])),
        "the decommission driver moves the group onto node 5",
    )?;
    world.drive(GROUP, &[5], DRIVE, |world| committed_on(world, &[1, 2, 5]))?;
    reconcile(world, 1, true)?;
    let at = world.position(GROUP, &[1, 2, 4]).unwrap_or(0);
    world.step(Action::DataPlan { group: GROUP, at })?;
    world.step(Action::DeliverReports)?;
    world.ensure(
        committed_on(world, &[1, 2, 4]) && world.is_stable(GROUP, &[1, 2, 5]),
        "node 4 is removed while the group lists it",
    )?;
    world.step(Action::Host(node(4)))?;
    world.step(Action::Sweep)
}

/// C6(h): node 4's meta replica missed a meta plan and holds a higher
/// index of the older history, where it is a move's target. Confirming by
/// index would take that view as current and create the replica.
fn confirmation_by_log_id(world: &mut World) -> Result<(), Violation> {
    let keep = world.meta.len();
    world.request_move(GROUP, &[1, 2, 4])?;
    for limit in [3, 4, 3, 4] {
        world.step(Action::Command(ControlCommand::SetMoveLimit {
            limit: MoveLimit::new(limit),
        }))?;
    }
    world.freeze(4)?;
    world.step(Action::MetaPlan { keep })?;
    world.step(Action::Host(node(4)))?;
    world.ensure(
        world.replica(node(4), GROUP).is_none(),
        "node 4 created nothing",
    )
}

/// Two groups formed, group 1 on `{2, 3, 4}`.
fn form_two(world: &mut World) -> Result<(), Violation> {
    world.quiesce(FORM)?;
    world.ensure(
        world.is_stable(RaftGroupId(1), &[2, 3, 4]),
        "group 1 formed on {2, 3, 4}",
    )
}

/// C6(i): group 1, seeded on `{2, 3, 4}`, moves onto `{2, 3, 5}`, node 4
/// tears its replica down and is decommissioned, so removed at once. A
/// meta plan cuts below both groups' first reports: group 1 is seeded on
/// `{2, 3, 4}` again and node 4 active, but node 4 still believes itself
/// removed and creates nothing. A seeded target reports the voters it
/// finds and every group settles.
fn a_meta_plan_below_a_first_report(world: &mut World) -> Result<(), Violation> {
    let moved = RaftGroupId(1);
    world.request_move(moved, &[2, 3, 5])?;
    world.drive(moved, &[5], DRIVE, |world| {
        world.is_stable(moved, &[2, 3, 5])
    })?;
    world.finish_teardown(4, moved)?;
    world.step(Action::Command(ControlCommand::SetNodeLifecycle {
        node: node(4),
        change: LifecycleChange::Decommission,
        now_ms: 0,
    }))?;
    world.step(Action::Host(node(4)))?;
    world.ensure(
        world.nodes.get(&node(4)).is_some_and(|state| state.removed),
        "node 4 knows it is removed",
    )?;
    let keep = world.boot_len;
    world.step(Action::MetaPlan { keep })?;
    world.quiesce(DRIVE)
}

/// C2 and step 0 of 5.3, at replication factor 1: group 0 on `{1}` moves
/// to `{2}` with node 3 a caught-up learner, and the move's second step
/// waits for the group's next settle, which commits `{2}` without leader 1.
/// The move is retargeted to `{3}` before that settle, through which node 1
/// leads outside its config until the next one steps it down. Only waiting
/// as no member keeps node 1 from proposing `{3}` from outside its config.
fn a_leader_removed_by_a_delayed_second_step(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[3])?;
    world.drive(GROUP, &[3], DRIVE, |world| {
        world.learners(GROUP).contains(&node(3)) && world.caught_up(GROUP, 3)
    })?;
    world.request_move(GROUP, &[2])?;
    world.drive(GROUP, &[2], DRIVE, |world| {
        world.learners(GROUP).contains(&node(2)) && world.caught_up(GROUP, 2)
    })?;
    world.step(Action::DelaySecondStep(GROUP))?;
    // Leader 1 confirms its view if it has not, then changes the voters.
    for _ in 0..2 {
        if !joint(world) {
            reconcile(world, 1, false)?;
        }
    }
    world.ensure(joint(world), "the joint entry of the move to {2} committed")?;
    world.request_move(GROUP, &[3])?;
    world.step(Action::Settle(GROUP))?;
    world.ensure(
        world.leader(GROUP) == Some(node(1)) && committed_on(world, &[2]),
        "node 1 leads outside the committed {2}",
    )?;
    // Node 1 confirms its view if it has not, then waits.
    reconcile(world, 1, false)?;
    reconcile(world, 1, false)?;
    world.ensure(
        world.leader(GROUP) == Some(node(1)) && committed_on(world, &[2]),
        "node 1 waited for its step-down",
    )?;
    world.drive(GROUP, &[3], DRIVE, |world| world.is_stable(GROUP, &[3]))
}

/// Whether group 0's committed config is joint.
fn joint(world: &World) -> bool {
    world
        .raft(GROUP)
        .is_some_and(|raft| matches!(raft.committed, MembershipConfig::Joint { .. }))
}

#[test]
fn c2_a_leader_outside_its_config_waits_as_no_member() {
    Trace {
        rf: 1,
        groups: 1,
        form: form_single,
        scenario: a_leader_removed_by_a_delayed_second_step,
    }
    .negative(Rule::NotAMemberWaits, &[Invariant::C2]);
}

#[test]
fn c6_a_a_voter_addition_needs_a_confirmed_view() {
    Trace::formed(confirmation_before_a_voter_addition)
        .negative(Rule::ConfirmedVoterChange, &[Invariant::C3]);
}

#[test]
fn c6_a_a_kept_step_fails_its_cas_after_the_converge() {
    Trace::formed(a_step_outliving_its_leadership)
        .negative(Rule::LearnersBeforeReport, &[Invariant::C3]);
}

#[test]
fn c6_b_a_cut_off_leader_reports_nothing_without_the_proof() {
    Trace::formed(the_leadership_proof).negative(Rule::LeadershipProof, &[Invariant::C3]);
}

#[test]
fn c6_c_the_placement_cas_refuses_a_delayed_report() {
    Trace::formed(the_placement_cas).negative(Rule::PlacementCas, &[Invariant::C3]);
}

#[test]
fn c6_c_the_placement_cas_refuses_a_report_across_a_meta_plan() {
    Trace::formed(the_placement_cas_across_a_meta_plan)
        .negative(Rule::PlacementCas, &[Invariant::C3]);
}

#[test]
fn c6_d_the_group_refuses_the_teardown_a_wrong_report_asks_for() {
    Trace::formed(the_answers_against_a_wrong_report)
        .negative(Rule::RetireAnswer, &[Invariant::C1, Invariant::C3]);
}

#[test]
fn c6_d_step_five_keeps_a_voter_placed_again_before_it() {
    Trace::formed(the_answers_against_a_re_placement)
        .negative(Rule::RetireAnswer, &[Invariant::C1, Invariant::C3]);
}

#[test]
fn c6_e_only_a_confirmed_view_creates() {
    Trace::formed(creation_only_when_confirmed).negative(Rule::ConfirmedCreation, &[Invariant::C8]);
}

#[test]
fn c6_f_a_history_event_drops_every_retirement() {
    Trace::formed(the_history_drop).negative(Rule::HistoryDrop, &[Invariant::C3]);
}

#[test]
fn c6_g_a_removed_node_keeps_a_counting_replica_open() {
    Trace::formed(the_close_rule_and_the_sweep).negative(Rule::RemovedNodeCloses, &[Invariant::C1]);
}

#[test]
fn c6_g_the_sweep_keeps_the_objects_of_a_voter() {
    Trace::formed(the_close_rule_and_the_sweep).negative(Rule::SweepAnswer, &[Invariant::C1]);
}

#[test]
fn c6_h_a_view_confirms_by_log_id_not_by_index() {
    Trace::formed(confirmation_by_log_id).negative(Rule::ConfirmByLogId, &[Invariant::C8]);
}

#[test]
fn c6_i_a_seeded_target_reports_the_voters_it_finds() {
    Trace {
        rf: 3,
        groups: 2,
        form: form_two,
        scenario: a_meta_plan_below_a_first_report,
    }
    .negative(Rule::SeededOnlyReports, &[Invariant::C5]);
}

/// C4 at replication factor 1: group 0 moves from `{1}` onto `{2}` and node
/// 1 retires its log. A meta plan cuts below the move's first report, so
/// meta seeds the group on `{1}` again and node 1's `Create` reopens its
/// retired log. The reopen is initialized (7.2, 7.3): the seed probe finds
/// no empty replica, nothing initializes a second time, and the group
/// settles on the voters it has.
fn a_reopened_seed_voter(world: &mut World) -> Result<(), Violation> {
    form_single(world)?;
    world.request_move(GROUP, &[2])?;
    world.drive(GROUP, &[2], DRIVE, |world| world.is_stable(GROUP, &[2]))?;
    world.finish_teardown(1, GROUP)?;
    let keep = world.boot_len;
    world.step(Action::MetaPlan { keep })?;
    world.step(Action::Host(node(1)))?;
    world.ensure(
        world
            .replica(node(1), GROUP)
            .is_some_and(|replica| replica.log == Journal::Recovering),
        "node 1 reopened its retired log",
    )?;
    world.step(Action::Seed(node(1), GROUP))?;
    world.quiesce(DRIVE)
}

#[test]
fn c4_a_reopened_seed_voter_never_initializes_again() {
    let mut world = World::new(5, 1, 1);
    if let Err(violation) = a_reopened_seed_voter(&mut world) {
        panic!("{violation}\n{}", world.history());
    }
}

/// With every rule on, a restarted node that missed a meta plan views the
/// new history only after confirming, and creates nothing its old view
/// asked for: the restart's regressed view alone moves nothing.
#[test]
fn a_restart_with_a_regressed_view_creates_nothing_unconfirmed() {
    let mut world = World::new(5, 1, 3);
    form(&mut world).unwrap();
    world.request_move(GROUP, &[1, 2, 4]).unwrap();
    let at = world.meta.len().saturating_sub(1);
    world.request_move(GROUP, &[1, 2, 5]).unwrap();
    world.step(Action::Crash(node(4))).unwrap();
    world
        .step(Action::Restart(node(4), ViewAt::Entry(at)))
        .unwrap();
    world.step(Action::Host(node(4))).unwrap();
    assert!(world.replica(node(4), GROUP).is_none());
}

/// Group 0 on `{1}` moves to `{2}`. The move's joint entry commits and its
/// second step waits for the next settle, which appends the uniform `{2}`
/// while node 2 is cut off on the data plane: it stays uncommitted. Node 2
/// was cut off past an election timeout and campaigned,
/// so its higher vote deposes node 1 before node 1's next append reaches
/// it. Node 1, no voter of the uniform entry but one of the committed joint
/// config, wins the election that follows with node 2's vote, its log
/// being newer: its no-op commits `{2}`. Node 1 leads outside its config
/// and waits as no member, the next settle steps it down, node 2 reports,
/// and node 1's teardown retires its replica.
fn an_outgoing_voter_wins_on_the_uniform_entry(world: &mut World) -> Result<(), Violation> {
    world.request_move(GROUP, &[2])?;
    world.drive(GROUP, &[2], DRIVE, |world| {
        world.learners(GROUP).contains(&node(2)) && world.caught_up(GROUP, 2)
    })?;
    world.step(Action::DelaySecondStep(GROUP))?;
    // Leader 1 confirms its view if it has not, then changes the voters.
    for _ in 0..2 {
        if !joint(world) {
            reconcile(world, 1, false)?;
        }
    }
    world.ensure(joint(world), "the joint entry of the move to {2} committed")?;
    world.step(Action::Partition(node(2)))?;
    world.step(Action::Settle(GROUP))?;
    world.ensure(
        joint(world)
            && world
                .raft(GROUP)
                .is_some_and(|raft| raft.pending().is_some()),
        "the uniform {2} is appended and uncommitted",
    )?;
    world.ensure(
        world.tally().outgoing_wins == 0 && world.tally().not_a_member == 0,
        "no outgoing voter won and no leader waited as no member yet",
    )?;
    world.step(Action::Heal(node(2), Outage::Long))?;
    world.ensure(
        world.tally().deposed_by_returns > 0
            && world.tally().outgoing_wins > 0
            && world.leader(GROUP) == Some(node(1))
            && committed_on(world, &[2]),
        "node 2's return deposed node 1, which won on the uniform entry and \
         whose no-op committed {2}",
    )?;
    world.ensure(
        world.tally().not_a_member == 0,
        "node 1 did not wait as no member before it planned",
    )?;
    reconcile(world, 1, false)?;
    world.ensure(
        world.tally().not_a_member > 0,
        "node 1 waited as no member of {2}",
    )?;
    world.step(Action::Settle(GROUP))?;
    world.ensure(
        world.leader(GROUP) == Some(node(2)),
        "the step-down handed off to node 2",
    )?;
    world.drive(GROUP, &[], DRIVE, |world| world.is_stable(GROUP, &[2]))?;
    world.finish_teardown(1, GROUP)
}

#[test]
fn an_outgoing_voter_wins_on_the_uniform_entry_and_steps_down() {
    let mut world = World::new(5, 1, 1);
    let result = form_single(&mut world)
        .and_then(|()| an_outgoing_voter_wins_on_the_uniform_entry(&mut world));
    if let Err(violation) = result {
        panic!("{violation}\n{}", world.history());
    }
}

/// Every seed voter of group 0 holds an empty replica, and the group has
/// not initialized.
fn hosted_not_seeded(world: &mut World) -> Result<(), Violation> {
    for id in 1..=5 {
        world.step(Action::Host(node(id)))?;
    }
    let empty = [1, 2, 3].iter().all(|id| {
        world
            .replica(node(*id), GROUP)
            .is_some_and(|replica| replica.log == Journal::Empty)
    });
    world.ensure(
        empty && world.raft(GROUP).is_none(),
        "the seed voters hold empty replicas of group 0",
    )
}

/// C4: node 1, group 0's initializer, holds the bootstrap's view, and a meta
/// plan cuts below the bootstrap before node 1's seed driver runs. Its meta
/// replica missed the plan and holds the bootstrap at the index the plan
/// began its history at. Confirming by index would take that view as
/// current and initialize a group meta no longer places.
fn a_seed_after_a_formation_cut(world: &mut World) -> Result<(), Violation> {
    world.freeze(1)?;
    let keep = world.boot_len.saturating_sub(1);
    world.step(Action::MetaPlan { keep })?;
    world.step(Action::Seed(node(1), GROUP))?;
    world.ensure(world.raft(GROUP).is_none(), "group 0 did not initialize")
}

#[test]
fn c4_a_seed_driver_initializes_only_from_a_view_confirmed_by_log_id() {
    Trace {
        rf: 3,
        groups: 1,
        form: hosted_not_seeded,
        scenario: a_seed_after_a_formation_cut,
    }
    .negative(Rule::ConfirmByLogId, &[Invariant::C4]);
}
