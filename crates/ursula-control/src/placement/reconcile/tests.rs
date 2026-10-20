//! Unit tests of the reconciler's planner: the walk-throughs of the design's
//! step order (5.3), the joint exit, the wait reasons and the membership
//! answer.

use std::collections::BTreeSet;

use ursula_proto::placement::LearnerLag;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::StepKind;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WaitReason;
use ursula_shard::RaftGroupId;

use super::EffectiveMembership;
use super::LeaderView;
use super::Leadership;
use super::MembershipConfig;
use super::MembershipStep;
use super::MembershipTarget;
use super::MembershipView;
use super::PeerGate;
use super::TargetStage;
use super::ViewFreshness;
use super::membership_answer;
use super::model::Ended;
use super::model::Group;
use super::model::ids;
use super::model::node;
use super::model::round;
use super::model::run;
use super::model::voters;
use super::plan_membership_step;
use crate::placement::samples::Driver;

fn log_id(index: u64) -> MembershipLogId {
    MembershipLogId::new(RaftLogId {
        term: 3,
        leader: node(1),
        index,
    })
}

fn moving(to: &[u64]) -> MembershipTarget {
    MembershipTarget {
        voters: voters(to),
        learners: BTreeSet::new(),
        stage: TargetStage::Moving {
            since: PlacementEpoch::new(ursula_proto::placement::MetaLogId::new(RaftLogId {
                term: 1,
                leader: node(1),
                index: 7,
            })),
        },
    }
}

fn uniform(voter_ids: &[u64], learner_ids: &[u64]) -> MembershipConfig {
    MembershipConfig::Uniform {
        voters: voters(voter_ids),
        learners: ids(learner_ids),
    }
}

/// A confirmed leader `me` of `config` at index 10, every member live,
/// caught up, gated open and ready for a handoff.
fn leader(me: u64, config: MembershipConfig) -> LeaderView {
    let members = config.members();
    LeaderView {
        matched: members
            .iter()
            .filter(|member| **member != node(me))
            .map(|member| (*member, Some(10)))
            .collect(),
        live: members.clone(),
        handoff_ready: config
            .voters()
            .into_iter()
            .filter(|voter| *voter != node(me))
            .collect(),
        gates: config
            .learners()
            .iter()
            .map(|learner| (*learner, PeerGate::Open))
            .collect(),
        membership: EffectiveMembership::Committed {
            log_id: log_id(10),
            config,
        },
        committed_index: 10,
        unlisted: BTreeSet::new(),
        view: ViewFreshness::Confirmed,
    }
}

fn plan(me: u64, leader: LeaderView, target: &MembershipTarget) -> MembershipStep {
    plan_membership_step(
        &MembershipView {
            me: node(me),
            leadership: Leadership::Leading(Box::new(leader)),
        },
        target,
    )
}

/// The kinds of the steps the leaders take from `from` (led by `leader`)
/// to `to`, every node live, and the committed uniform voter sets on the
/// way.
fn walk(from: &[u64], leader: u64, to: &[u64]) -> (Vec<StepKind>, Vec<VoterSet>) {
    let alive: BTreeSet<NodeId> = (1..=6).map(node).collect();
    let mut group = Group::new(&voters(from), node(leader), alive);
    let target = moving(to);
    let mut kinds = Vec::new();
    let ended = run(&mut group, &target, 20, |_group, planned| {
        if let Some(kind) = planned.and_then(|planned| planned.step.kind()) {
            kinds.push(kind);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(ended, Ended::Converged);
    let uniform_voters = group
        .committed_configs
        .iter()
        .filter_map(|config| match config {
            MembershipConfig::Uniform { voters, .. } => Some(voters.clone()),
            MembershipConfig::Joint { .. } => None,
        })
        .collect::<Vec<_>>();
    let mut distinct: Vec<VoterSet> = Vec::new();
    for voters in uniform_voters {
        if distinct.last() != Some(&voters) {
            distinct.push(voters);
        }
    }
    (kinds, distinct)
}

#[test]
fn an_outgoing_leader_hands_off_before_it_is_replaced() {
    let (kinds, configs) = walk(&[1, 2, 3], 1, &[2, 3, 4]);
    assert_eq!(kinds, vec![
        StepKind::AddLearners,
        StepKind::ConfirmView,
        StepKind::TransferLeadership,
        StepKind::ConfirmView,
        StepKind::ChangeVoters,
        StepKind::ReportConverged,
    ]);
    assert_eq!(configs, vec![voters(&[1, 2, 3]), voters(&[2, 3, 4])]);
}

#[test]
fn a_full_replacement_swaps_one_voter_per_step_and_hands_off_last() {
    let (kinds, configs) = walk(&[1, 2, 3], 1, &[4, 5, 6]);
    assert_eq!(kinds, vec![
        StepKind::AddLearners,
        StepKind::ConfirmView,
        StepKind::ChangeVoters,
        StepKind::ChangeVoters,
        StepKind::TransferLeadership,
        StepKind::ConfirmView,
        StepKind::ChangeVoters,
        StepKind::ReportConverged,
    ]);
    assert_eq!(configs, vec![
        voters(&[1, 2, 3]),
        voters(&[1, 3, 4]),
        voters(&[1, 4, 5]),
        voters(&[4, 5, 6]),
    ]);
}

#[test]
fn a_single_voter_moves_by_the_rf_1_step_and_steps_down() {
    let (kinds, configs) = walk(&[1], 1, &[2]);
    assert_eq!(kinds, vec![
        StepKind::AddLearners,
        StepKind::ConfirmView,
        StepKind::ChangeVoters,
        StepKind::ReportConverged,
    ]);
    assert_eq!(configs, vec![voters(&[1]), voters(&[2])]);
}

#[test]
fn a_lost_second_step_is_flattened_onto_the_target_without_a_confirmation() {
    let alive: BTreeSet<NodeId> = (1..=4).map(node).collect();
    let mut group = Group::new(&voters(&[1, 2, 3]), node(1), alive);
    let target = moving(&[2, 3, 4]);
    group.drop_second_step = true;
    // The learner, a confirmation, the handoff to 2 and its confirmation,
    // then the change whose second step a leadership change loses.
    for _ in 0..5 {
        round(&mut group, &target, true);
    }
    assert!(matches!(group.committed, MembershipConfig::Joint { .. }));
    assert_eq!(group.leader, None);
    // The outgoing voter 1 wins the election and hands off to the target.
    group.settle();
    assert_eq!(group.leader, Some(node(1)));
    let step = plan_membership_step(&group.view(node(1)), &target);
    assert_eq!(step, MembershipStep::TransferLeadership { to: node(2) });
    group.execute(&step);
    // An unconfirmed leader flattens the joint config onto the target.
    let step = plan_membership_step(&group.view(node(2)), &target);
    assert_eq!(step, MembershipStep::ChangeVoters {
        expected: group.committed_id,
        voters: voters(&[2, 3, 4]),
    });
    group.execute(&step);
    assert_eq!(group.committed, uniform(&[2, 3, 4], &[]));
}

#[test]
fn a_joint_exit_takes_the_target_else_the_side_with_more_live_voters() {
    let joint = MembershipConfig::Joint {
        old: voters(&[1, 2, 3]),
        new: voters(&[1, 2, 4]),
        learners: BTreeSet::new(),
    };
    let mut view = leader(1, joint.clone());
    view.view = ViewFreshness::Unconfirmed;
    // A retarget to {1, 2, 5} with node 4 dead: {1, 2, 4} would keep a dead
    // voter.
    view.live.remove(&node(4));
    let step = plan(1, view.clone(), &moving(&[1, 2, 5]));
    assert_eq!(step, MembershipStep::ChangeVoters {
        expected: log_id(10),
        voters: voters(&[1, 2, 3]),
    });
    // The target side wins while it has a live majority.
    let step = plan(1, view.clone(), &moving(&[1, 2, 4]));
    assert_eq!(step, MembershipStep::ChangeVoters {
        expected: log_id(10),
        voters: voters(&[1, 2, 4]),
    });
    // A tie in live voters goes to the side sharing more with the target.
    let all_live = leader(1, joint);
    let step = plan(1, all_live, &moving(&[1, 4, 6]));
    assert_eq!(step, MembershipStep::ChangeVoters {
        expected: log_id(10),
        voters: voters(&[1, 2, 4]),
    });
    // No side with a live majority: nothing to exit onto.
    let mut dead = view;
    dead.live = ids(&[1]);
    assert_eq!(
        plan(1, dead, &moving(&[1, 2, 5])),
        MembershipStep::Wait(WaitReason::NoLiveQuorum)
    );
}

#[test]
fn a_leader_off_the_exit_side_hands_off_to_its_smallest_ready_voter() {
    let joint = MembershipConfig::Joint {
        old: voters(&[1, 2, 3]),
        new: voters(&[2, 3, 4]),
        learners: BTreeSet::new(),
    };
    let mut view = leader(1, joint);
    view.handoff_ready = ids(&[3, 4]);
    assert_eq!(
        plan(1, view.clone(), &moving(&[2, 3, 4])),
        MembershipStep::TransferLeadership { to: node(3) }
    );
    view.handoff_ready = BTreeSet::new();
    assert_eq!(
        plan(1, view, &moving(&[2, 3, 4])),
        MembershipStep::Wait(WaitReason::NoHandoffTarget)
    );
}

#[test]
fn a_replica_that_cannot_propose_waits_with_its_reason() {
    let target = moving(&[2, 3, 4]);
    let wait = |leadership| {
        plan_membership_step(
            &MembershipView {
                me: node(1),
                leadership,
            },
            &target,
        )
    };
    assert_eq!(
        wait(Leadership::NotLeading),
        MembershipStep::Wait(WaitReason::NotLeader)
    );
    assert_eq!(
        wait(Leadership::Transferring { to: node(2) }),
        MembershipStep::Wait(WaitReason::TransferPending)
    );
    assert_eq!(
        wait(Leadership::StepPending),
        MembershipStep::Wait(WaitReason::StepPending)
    );
    let mut uncommitted = leader(1, uniform(&[1, 2, 3], &[]));
    uncommitted.membership = EffectiveMembership::Uncommitted { log_id: log_id(11) };
    assert_eq!(
        wait(Leadership::Leading(Box::new(uncommitted))),
        MembershipStep::Wait(WaitReason::MembershipUncommitted)
    );
    let outsider = leader(1, uniform(&[2, 3, 4], &[1]));
    assert_eq!(
        wait(Leadership::Leading(Box::new(outsider))),
        MembershipStep::Wait(WaitReason::NotAMember)
    );
}

#[test]
fn only_a_moving_target_changes_voters() {
    let config = uniform(&[1, 2, 3], &[5]);
    let since = PlacementEpoch::new(ursula_proto::placement::MetaLogId::new(RaftLogId {
        term: 1,
        leader: node(1),
        index: 2,
    }));
    let settled = MembershipTarget {
        voters: voters(&[1, 2, 4]),
        learners: BTreeSet::new(),
        stage: TargetStage::Settled,
    };
    assert_eq!(
        plan(1, leader(1, config.clone()), &settled),
        MembershipStep::Wait(WaitReason::TargetSettled)
    );
    // A seeded target is reported as the group stands, after its stray
    // learners go.
    let seeded = MembershipTarget {
        stage: TargetStage::Seeded { since },
        ..settled.clone()
    };
    assert_eq!(
        plan(1, leader(1, config), &seeded),
        MembershipStep::RemoveLearners {
            expected: log_id(10),
            nodes: ids(&[5]),
        }
    );
    assert_eq!(
        plan(1, leader(1, uniform(&[1, 2, 3], &[])), &seeded),
        MembershipStep::Converged {
            voters: voters(&[1, 2, 3]),
        }
    );
    // A reached settled target answers `Converged`, which the shell does
    // not report.
    let reached = MembershipTarget {
        voters: voters(&[1, 2, 3]),
        ..settled
    };
    assert_eq!(
        plan(1, leader(1, uniform(&[1, 2, 3], &[])), &reached),
        MembershipStep::Converged {
            voters: voters(&[1, 2, 3]),
        }
    );
}

#[test]
fn a_meta_target_adds_registered_nodes_as_learners_and_removes_others() {
    let target = MembershipTarget {
        voters: voters(&[1, 2, 3]),
        learners: ids(&[4, 5]),
        stage: TargetStage::Settled,
    };
    assert_eq!(
        plan(1, leader(1, uniform(&[1, 2, 3], &[4, 6])), &target),
        MembershipStep::RemoveLearners {
            expected: log_id(10),
            nodes: ids(&[6]),
        }
    );
    assert_eq!(
        plan(1, leader(1, uniform(&[1, 2, 3], &[4])), &target),
        MembershipStep::AddLearners {
            expected: log_id(10),
            nodes: ids(&[5]),
        }
    );
}

#[test]
fn a_voter_change_waits_for_a_confirmed_view_and_a_ready_learner() {
    let target = moving(&[1, 2, 4]);
    // The missing target is added as a learner first, unconfirmed.
    let mut view = leader(1, uniform(&[1, 2, 3], &[]));
    view.view = ViewFreshness::Unconfirmed;
    assert_eq!(plan(1, view, &target), MembershipStep::AddLearners {
        expected: log_id(10),
        nodes: ids(&[4]),
    });
    let mut view = leader(1, uniform(&[1, 2, 3], &[4]));
    view.view = ViewFreshness::Unconfirmed;
    assert_eq!(plan(1, view.clone(), &target), MembershipStep::ConfirmView);
    view.view = ViewFreshness::Confirmed;
    assert_eq!(
        plan(1, view.clone(), &target),
        MembershipStep::ChangeVoters {
            expected: log_id(10),
            voters: voters(&[1, 2, 4]),
        }
    );
    // The reasons, by precedence.
    let mut blocked = view.clone();
    blocked.matched.insert(node(4), Some(9));
    assert_eq!(
        plan(1, blocked.clone(), &target),
        MembershipStep::Wait(WaitReason::CatchingUp {
            committed: 10,
            behind: vec![LearnerLag {
                node: node(4),
                matched: Some(9),
            }],
        })
    );
    let mut gated = view.clone();
    gated.gates.insert(node(4), PeerGate::Unknown);
    assert_eq!(
        plan(1, gated.clone(), &target),
        MembershipStep::Wait(WaitReason::GateUnknown { nodes: ids(&[4]) })
    );
    gated.gates.insert(node(4), PeerGate::Closed);
    assert_eq!(
        plan(1, gated.clone(), &target),
        MembershipStep::Wait(WaitReason::GateClosed { nodes: ids(&[4]) })
    );
    gated.unlisted.insert(node(4));
    assert_eq!(
        plan(1, gated, &target),
        MembershipStep::Wait(WaitReason::PeerNotListed { nodes: ids(&[4]) })
    );
}

#[test]
fn the_dropped_voter_is_the_smallest_dead_one_and_never_the_leader() {
    // Shrinking by one drops the smallest other voter, or the smallest dead
    // one.
    let shrink = leader(2, uniform(&[1, 2, 3, 4], &[]));
    let target = moving(&[1, 2, 5]);
    let mut with_learner = shrink.clone();
    with_learner.membership = EffectiveMembership::Committed {
        log_id: log_id(10),
        config: uniform(&[1, 2, 3, 4], &[5]),
    };
    assert_eq!(
        plan(2, with_learner.clone(), &target),
        MembershipStep::ChangeVoters {
            expected: log_id(10),
            voters: voters(&[1, 2, 4]),
        }
    );
    with_learner.live.remove(&node(4));
    assert_eq!(
        plan(2, with_learner, &target),
        MembershipStep::ChangeVoters {
            expected: log_id(10),
            voters: voters(&[1, 2, 3]),
        }
    );
    // The leader 2 is in the dropped set, the dead 3 goes first.
    let mut same_size = leader(2, uniform(&[1, 2, 3], &[5, 6]));
    same_size.live.remove(&node(3));
    assert_eq!(
        plan(2, same_size, &moving(&[1, 5, 6])),
        MembershipStep::ChangeVoters {
            expected: log_id(10),
            voters: voters(&[1, 2, 5]),
        }
    );
    // A missing learner is added first.
    assert_eq!(plan(2, shrink, &target), MembershipStep::AddLearners {
        expected: log_id(10),
        nodes: ids(&[5]),
    });
}

#[test]
fn the_membership_answer_classifies_by_both_memberships() {
    // The outgoing voter of a committed joint config with an appended
    // uniform entry is still a voter.
    let committed = MembershipConfig::Joint {
        old: voters(&[1, 2, 3]),
        new: voters(&[2, 3, 4]),
        learners: ids(&[5]),
    };
    let effective = uniform(&[2, 3, 4], &[5]);
    assert_eq!(
        membership_answer(node(1), &committed, &effective),
        MembershipAnswer::Voter
    );
    assert_eq!(
        membership_answer(node(4), &committed, &effective),
        MembershipAnswer::Voter
    );
    assert_eq!(
        membership_answer(node(5), &committed, &effective),
        MembershipAnswer::Learner
    );
    assert_eq!(
        membership_answer(node(6), &committed, &effective),
        MembershipAnswer::NotMember
    );
    // A learner of the committed membership the effective one dropped is
    // still a member.
    assert_eq!(
        membership_answer(node(5), &committed, &uniform(&[2, 3, 4], &[])),
        MembershipAnswer::Learner
    );
}

#[test]
fn a_placement_gives_its_groups_target() {
    let mut driver = Driver::bootstrapped(
        &[1, 2, 3, 4],
        crate::placement::samples::params(
            &[1, 2, 3, 4],
            2,
            3,
            4,
            crate::placement::MaintenanceAuthority::Placement,
        ),
    );
    let group = RaftGroupId(0);
    let seeded = MembershipTarget::of_placement(driver.placement(group));
    assert_eq!(seeded.stage, TargetStage::Seeded {
        since: driver.since(group)
    });
    assert_eq!(&seeded.voters, driver.placement(group).target_voters());
    assert!(seeded.learners.is_empty());
    let reported: Vec<u64> = seeded.voters.iter().map(NodeId::get).collect();
    driver.report(group, &reported).unwrap();
    let settled = MembershipTarget::of_placement(driver.placement(group));
    assert_eq!(settled.stage, TargetStage::Settled);
    driver.request_move(group, &[2, 3, 4]).unwrap();
    let moving = MembershipTarget::of_placement(driver.placement(group));
    assert_eq!(moving.stage, TargetStage::Moving {
        since: driver.since(group)
    });
    assert_eq!(moving.voters, voters(&[2, 3, 4]));
}
