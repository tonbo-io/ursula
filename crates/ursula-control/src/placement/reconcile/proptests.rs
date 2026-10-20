//! Planner property tests R1 to R12 of the design (issue #768).
//!
//! The single-step properties run the planner on arbitrary leader views:
//! any leadership, uniform or joint configs, live, caught-up, gated and
//! listed sets, confirmed or not, against any target. R6 and R10 run it
//! over the abstract group of [`super::model`], whose adversary moves
//! leadership onto any voter, drops second steps, revokes the
//! confirmation, holds catch-up and gates back, unlists targets and kills
//! nodes, before the environment turns fair.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::prelude::ProptestConfig;
use proptest::prelude::Strategy;
use proptest::test_runner::RngSeed;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WaitReason;

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
use super::model::Ended;
use super::model::Group;
use super::model::node;
use super::model::quorum_live;
use super::model::round;
use super::model::run;
use super::plan_membership_step;

/// Node ids `1..=6` chosen by the low bits of `mask`.
fn set_of(mask: u8) -> BTreeSet<NodeId> {
    (0_u32..6)
        .filter(|bit| (mask.checked_shr(*bit).unwrap() & 1) == 1)
        .map(|bit| node(u64::from(bit).checked_add(1).unwrap()))
        .collect()
}

/// The ids of `mask`, or `fallback` alone when it chooses none.
fn voter_set(mask: u8, fallback: NodeId) -> VoterSet {
    VoterSet::new(set_of(mask)).unwrap_or_else(|_empty| VoterSet::single(fallback))
}

fn since() -> PlacementEpoch {
    PlacementEpoch::new(MetaLogId::new(RaftLogId {
        term: 1,
        leader: node(1),
        index: 3,
    }))
}

fn committed_id() -> MembershipLogId {
    MembershipLogId::new(RaftLogId {
        term: 4,
        leader: node(2),
        index: 9,
    })
}

fn ensure(condition: bool, violation: impl FnOnce() -> String) -> Result<(), String> {
    if condition { Ok(()) } else { Err(violation()) }
}

/// The raw choices of one arbitrary planner input.
#[derive(Debug, Clone)]
struct Raw {
    me: u8,
    leadership: u8,
    uncommitted: bool,
    joint: bool,
    old: u8,
    new: u8,
    learners: u8,
    live: u8,
    ready: u8,
    unlisted: u8,
    confirmed: bool,
    matched: Vec<Option<u8>>,
    gates: Vec<u8>,
    target: u8,
    target_learners: u8,
    stage: u8,
}

fn raw() -> impl Strategy<Value = Raw> {
    (
        (
            0_u8..6,
            0_u8..8,
            proptest::bool::weighted(0.1),
            proptest::bool::weighted(0.3),
        ),
        (
            proptest::num::u8::ANY,
            proptest::num::u8::ANY,
            proptest::num::u8::ANY,
        ),
        (
            proptest::num::u8::ANY,
            proptest::num::u8::ANY,
            proptest::num::u8::ANY,
        ),
        proptest::bool::ANY,
        (
            proptest::collection::vec(proptest::option::of(5_u8..12), 6),
            proptest::collection::vec(0_u8..4, 6),
        ),
        (proptest::num::u8::ANY, proptest::num::u8::ANY, 0_u8..4),
    )
        .prop_map(
            |(
                (me, leadership, uncommitted, joint),
                (old, new, learners),
                (live, ready, unlisted),
                confirmed,
                (matched, gates),
                (target, target_learners, stage),
            )| Raw {
                me,
                leadership,
                uncommitted,
                joint,
                old,
                new,
                learners,
                live,
                ready,
                unlisted,
                confirmed,
                matched,
                gates,
                target,
                target_learners,
                stage,
            },
        )
}

/// The planner input `raw` describes. The leader is usually a voter.
fn build(raw: &Raw) -> (MembershipView, MembershipTarget) {
    let me = node(u64::from(raw.me).checked_add(1).unwrap());
    let me_bit = 1_u8.checked_shl(u32::from(raw.me)).unwrap();
    let old_mask = if raw.leadership == 7 {
        raw.old
    } else {
        raw.old | me_bit
    };
    let old = voter_set(old_mask, me);
    let new = voter_set(raw.new, me);
    let mut voters: BTreeSet<NodeId> = old.to_set();
    if raw.joint {
        voters.extend(new.iter());
    }
    let learners: BTreeSet<NodeId> = set_of(raw.learners)
        .into_iter()
        .filter(|learner| !voters.contains(learner))
        .collect();
    let config = if raw.joint {
        MembershipConfig::Joint { old, new, learners }
    } else {
        MembershipConfig::Uniform {
            voters: old,
            learners,
        }
    };
    let matched: BTreeMap<NodeId, Option<u64>> = config
        .members()
        .into_iter()
        .filter(|member| *member != me)
        .map(|member| {
            let slot = usize::try_from(member.get().checked_sub(1).unwrap()).unwrap();
            (member, raw.matched[slot].map(u64::from))
        })
        .collect();
    let gates: BTreeMap<NodeId, PeerGate> = config
        .learners()
        .iter()
        .filter_map(|learner| {
            let slot = usize::try_from(learner.get().checked_sub(1).unwrap()).unwrap();
            let gate = match raw.gates[slot] {
                0 => return None,
                1 => PeerGate::Closed,
                2 => PeerGate::Unknown,
                _ => PeerGate::Open,
            };
            Some((*learner, gate))
        })
        .collect();
    let membership = if raw.uncommitted {
        EffectiveMembership::Uncommitted {
            log_id: committed_id(),
        }
    } else {
        EffectiveMembership::Committed {
            log_id: committed_id(),
            config,
        }
    };
    let view = LeaderView {
        membership,
        committed_index: 8,
        matched,
        live: set_of(raw.live),
        handoff_ready: set_of(raw.ready),
        gates,
        unlisted: set_of(raw.unlisted),
        view: if raw.confirmed {
            ViewFreshness::Confirmed
        } else {
            ViewFreshness::Unconfirmed
        },
    };
    let leadership = match raw.leadership {
        0 => Leadership::NotLeading,
        1 => Leadership::Transferring { to: node(2) },
        2 => Leadership::StepPending,
        _ => Leadership::Leading(Box::new(view)),
    };
    let target_voters = voter_set(raw.target, node(6));
    let stage = match raw.stage {
        0 => TargetStage::Seeded { since: since() },
        1 => TargetStage::Settled,
        _ => TargetStage::Moving { since: since() },
    };
    // A settled target of the meta kind keeps learners. Data targets keep
    // none.
    let target_learners = if stage == TargetStage::Settled {
        set_of(raw.target_learners)
            .into_iter()
            .filter(|learner| !target_voters.contains(*learner))
            .collect()
    } else {
        BTreeSet::new()
    };
    (MembershipView { me, leadership }, MembershipTarget {
        voters: target_voters,
        learners: target_learners,
        stage,
    })
}

fn live_count(side: &VoterSet, live: &BTreeSet<NodeId>) -> usize {
    side.iter().filter(|node| live.contains(node)).count()
}

fn majority(side: &VoterSet, live: &BTreeSet<NodeId>) -> bool {
    live_count(side, live).checked_mul(2).unwrap() > side.len()
}

/// Every single-step property of one planner call.
fn check_step(
    view: &MembershipView,
    target: &MembershipTarget,
    step: &MembershipStep,
) -> Result<(), String> {
    let me = view.me;
    // R8: the planner is a function of its input.
    ensure(plan_membership_step(view, target) == *step, || {
        "R8: the planner answered twice differently".to_owned()
    })?;
    let leader = match &view.leadership {
        Leadership::Leading(leader) => leader,
        Leadership::NotLeading | Leadership::Transferring { .. } | Leadership::StepPending => {
            // R2: no proposal while not leading live, transferring or stepping.
            return ensure(matches!(step, MembershipStep::Wait(_)), || {
                format!("R2: {step:?} while not leading")
            });
        }
    };
    let config = match &leader.membership {
        EffectiveMembership::Uncommitted { .. } => {
            return ensure(
                *step == MembershipStep::Wait(WaitReason::MembershipUncommitted),
                || format!("R2: {step:?} on an uncommitted membership"),
            );
        }
        EffectiveMembership::Committed { config, .. } => config,
    };
    if !config.is_voter(me) {
        return ensure(
            *step == MembershipStep::Wait(WaitReason::NotAMember),
            || format!("R2: {step:?} from a replica in no voter config"),
        );
    }
    let mut live = leader.live.clone();
    live.insert(me);
    let caught_up = |node: NodeId| matches!(leader.matched.get(&node), Some(Some(index)) if *index >= leader.committed_index);
    match step {
        MembershipStep::AddLearners { expected, nodes } => {
            // R3, R5.
            ensure(*expected == committed_id(), || "R3: expected".to_owned())?;
            ensure(
                !nodes.is_empty()
                    && nodes.iter().all(|node| {
                        (target.voters.contains(*node) || target.learners.contains(node))
                            && !config.is_member(*node)
                    }),
                || format!("R5: learners {nodes:?} outside the target or already members"),
            )
        }
        MembershipStep::RemoveLearners { expected, nodes } => {
            ensure(*expected == committed_id(), || "R3: expected".to_owned())?;
            ensure(
                !nodes.is_empty()
                    && nodes.iter().all(|node| {
                        config.learners().contains(node) && !target.learners.contains(node)
                    }),
                || format!("removed learners {nodes:?} the target keeps"),
            )
        }
        MembershipStep::ChangeVoters { expected, voters } => {
            ensure(*expected == committed_id(), || "R3: expected".to_owned())?;
            match config {
                MembershipConfig::Joint { old, new, .. } => {
                    // R11: onto a side with a live majority, the target if it
                    // is one, else one with at least as many live voters.
                    ensure(voters == old || voters == new, || {
                        format!("R11: exit {voters:?} is no side")
                    })?;
                    ensure(voters.contains(me), || {
                        "R1: exit without the leader".to_owned()
                    })?;
                    ensure(majority(voters, &live), || {
                        "R11: exit side without a live majority".to_owned()
                    })?;
                    let other = if voters == old { new } else { old };
                    let target_side = (old == &target.voters || new == &target.voters)
                        && majority(&target.voters, &live);
                    if target_side {
                        ensure(voters == &target.voters, || {
                            "R11: the target side was passed over".to_owned()
                        })
                    } else {
                        ensure(
                            !majority(other, &live)
                                || live_count(voters, &live) >= live_count(other, &live),
                            || "R11: exit onto the side with fewer live voters".to_owned(),
                        )
                    }
                }
                MembershipConfig::Uniform {
                    voters: current,
                    learners,
                } => {
                    // R7, R12: only a confirmed view of a moving target.
                    ensure(leader.view == ViewFreshness::Confirmed, || {
                        "R7: a voter change from an unconfirmed view".to_owned()
                    })?;
                    ensure(matches!(target.stage, TargetStage::Moving { .. }), || {
                        format!("R12: a voter change toward a {:?} target", target.stage)
                    })?;
                    let added: Vec<NodeId> = voters
                        .iter()
                        .filter(|node| !current.contains(*node))
                        .collect();
                    let dropped: Vec<NodeId> = current
                        .iter()
                        .filter(|node| !voters.contains(*node))
                        .collect();
                    // R1: P-SELF, but the RF 1 step.
                    let rf_one = current.len() == 1 && voters.len() == 1;
                    ensure(voters.contains(me) || rf_one, || {
                        format!("R1: {voters:?} drops the leader {me}")
                    })?;
                    // R9: from two or more voters, one voter per step and never
                    // the leader.
                    if current.len() >= 2 {
                        ensure(added.len() <= 1 && dropped.len() <= 1, || {
                            format!("R9: {current:?} to {voters:?} in one step")
                        })?;
                    }
                    // R4: promotions of caught-up, listed learners with open gates.
                    ensure(
                        added.iter().all(|node| {
                            learners.contains(node)
                                && !leader.unlisted.contains(node)
                                && caught_up(*node)
                                && leader.gates.get(node) == Some(&PeerGate::Open)
                                && target.voters.contains(*node)
                        }),
                        || format!("R4: promoted {added:?}"),
                    )?;
                    ensure(
                        dropped.iter().all(|node| !target.voters.contains(*node)),
                        || format!("dropped a target voter in {dropped:?}"),
                    )
                }
            }
        }
        MembershipStep::TransferLeadership { to } => {
            ensure(*to != me && leader.handoff_ready.contains(to), || {
                format!("R8: transfer to {to}, not ready")
            })?;
            // R8: the smallest ready voter of the side it hands off to.
            let pool: Vec<BTreeSet<NodeId>> = match config {
                MembershipConfig::Joint { old, new, .. } => [old, new]
                    .into_iter()
                    .filter(|side| !side.contains(me) && majority(side, &live))
                    .map(VoterSet::to_set)
                    .collect(),
                MembershipConfig::Uniform { voters, .. } => vec![
                    voters
                        .iter()
                        .filter(|node| target.voters.contains(*node))
                        .collect(),
                ],
            };
            ensure(
                pool.iter().any(|side| {
                    side.iter()
                        .copied()
                        .find(|node| *node != me && leader.handoff_ready.contains(node))
                        == Some(*to)
                }),
                || format!("R8: transfer to {to} is not the smallest ready voter"),
            )
        }
        MembershipStep::ConfirmView => ensure(leader.view == ViewFreshness::Unconfirmed, || {
            "a confirmation from a confirmed view".to_owned()
        }),
        MembershipStep::Converged { voters } => {
            // R12: reported only as the group stands, at the target or seeded.
            let MembershipConfig::Uniform {
                voters: current,
                learners,
            } = config
            else {
                return Err("converged on a joint config".to_owned());
            };
            ensure(
                voters == current
                    && (voters == &target.voters
                        || matches!(target.stage, TargetStage::Seeded { .. }))
                    && learners == &target.learners,
                || format!("converged on {voters:?} toward {target:?}"),
            )
        }
        MembershipStep::Wait(_) => Ok(()),
    }
}

/// C7 and R1 over a run of the abstract group: every committed config of
/// the move has `size` voters in each config, and every voter change keeps
/// its proposer but the RF 1 step.
fn check_group(group: &Group, size: usize) -> Result<(), String> {
    for config in &group.committed_configs {
        ensure(
            config.voter_configs().all(|side| side.len() == size),
            || format!("C7: committed {config:?} is not of size {size}"),
        )?;
    }
    for (proposer, voters) in &group.proposals {
        ensure(
            voters.contains(*proposer) || (size == 1 && voters.len() == 1),
            || format!("R1: {proposer} proposed {voters:?}"),
        )?;
    }
    Ok(())
}

/// One adversary action before the environment turns fair.
#[derive(Debug, Clone, Copy)]
enum Fault {
    /// An election wins for any live voter of the effective membership.
    MoveLeader(u8),
    DropSecondStep,
    RevokeConfirmation,
    /// Catch-up, gates and listings stay as they are this round.
    HoldBack,
    /// A target's address drops out of the leader's book.
    Unlist(u8),
}

fn fault() -> impl Strategy<Value = Fault> {
    (0_u8..5, proptest::num::u8::ANY).prop_map(|(kind, x)| match kind {
        0 => Fault::MoveLeader(x),
        1 => Fault::DropSecondStep,
        2 => Fault::RevokeConfirmation,
        3 => Fault::HoldBack,
        _ => Fault::Unlist(x),
    })
}

fn pick(set: &BTreeSet<NodeId>, x: u8) -> Option<NodeId> {
    let slot = usize::from(x).checked_rem(set.len())?;
    set.iter().nth(slot).copied()
}

/// Rounds beyond the voters still to replace that a fair run may take: a
/// joint exit with its handoff, two more handoffs with their confirmations,
/// the learners in two steps, a learner removal and the report.
const FAIR_ROUNDS: usize = 12;

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        rng_seed: RngSeed::Fixed(768),
        // The seed is fixed, so a failure reproduces without a regression file.
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// R1 to R5, R7 to R9, R11 and R12 on one call.
    #[test]
    fn every_planned_step_keeps_the_planner_invariants(inputs in proptest::collection::vec(raw(), 64)) {
        for raw in &inputs {
            let (view, target) = build(raw);
            let step = plan_membership_step(&view, &target);
            if let Err(violation) = check_step(&view, &target, &step) {
                proptest::prop_assert!(false, "{violation}: {view:?} toward {target:?}: {step:?}");
            }
        }
    }

    /// R6: from every moving start of 1 to 5 voters, after any faults, a
    /// fair run converges within the voters still to replace plus
    /// `FAIR_ROUNDS`, keeping C7 and R1 throughout.
    #[test]
    fn every_moving_start_converges_within_the_bound(
        (from, to) in (1_usize..=5).prop_flat_map(|size| (
            proptest::collection::btree_set(1_u64..=10, size),
            proptest::collection::btree_set(1_u64..=10, size),
        )),
        leader in proptest::num::u8::ANY,
        faults in proptest::collection::vec(fault(), 0..10),
    ) {
        proptest::prop_assume!(from != to);
        let from = VoterSet::new(from.into_iter().map(node).collect()).unwrap();
        let to = VoterSet::new(to.into_iter().map(node).collect()).unwrap();
        let size = to.len();
        let alive: BTreeSet<NodeId> = (1..=10).map(node).collect();
        let first = pick(&from.to_set(), leader).unwrap();
        let mut group = Group::new(&from, first, alive);
        let target = MembershipTarget {
            voters: to.clone(),
            learners: BTreeSet::new(),
            stage: TargetStage::Moving { since: since() },
        };
        for fault in faults {
            let fair = match fault {
                Fault::MoveLeader(x) => {
                    let candidates: BTreeSet<NodeId> = group.effective().voters();
                    if let Some(next) = pick(&candidates, x) {
                        group.elect_as(next);
                    }
                    true
                }
                Fault::DropSecondStep => {
                    group.drop_second_step = true;
                    true
                }
                Fault::RevokeConfirmation => {
                    group.confirmed = false;
                    true
                }
                Fault::HoldBack => false,
                Fault::Unlist(x) => {
                    if let Some(unlisted) = pick(&to.to_set(), x) {
                        group.unlisted.insert(unlisted);
                    }
                    false
                }
            };
            round(&mut group, &target, fair);
            if let Err(violation) = check_group(&group, size) {
                proptest::prop_assert!(false, "{violation}");
            }
        }
        group.drop_second_step = false;
        let remaining = group.effective().voters().into_iter().filter(|voter| !to.contains(*voter)).count();
        let limit = remaining.checked_add(FAIR_ROUNDS).unwrap();
        let ended = run(&mut group, &target, limit, |group, _step| check_group(group, size));
        match ended {
            Ok(Ended::Converged) => {}
            Ok(Ended::Stuck) => proptest::prop_assert!(false, "R6: stuck after {limit} fair rounds: {group:?}"),
            Err(violation) => proptest::prop_assert!(false, "{violation}"),
        }
    }

    /// R10: with every target dead and a live majority of every config of
    /// the effective membership, the uncommitted entry included, a cancel
    /// reaches the move's source.
    #[test]
    fn a_cancel_with_every_target_dead_returns_to_the_source(
        (from, to) in (1_usize..=3).prop_flat_map(|size| (
            proptest::collection::btree_set(1_u64..=8, size),
            proptest::collection::btree_set(1_u64..=8, size),
        )),
        leader in proptest::num::u8::ANY,
        progress in 0_usize..12,
    ) {
        proptest::prop_assume!(from != to);
        let from = VoterSet::new(from.into_iter().map(node).collect()).unwrap();
        let to = VoterSet::new(to.into_iter().map(node).collect()).unwrap();
        let alive: BTreeSet<NodeId> = (1..=8).map(node).collect();
        let first = pick(&from.to_set(), leader).unwrap();
        let mut group = Group::new(&from, first, alive);
        let toward = MembershipTarget {
            voters: to.clone(),
            learners: BTreeSet::new(),
            stage: TargetStage::Moving { since: since() },
        };
        for _ in 0..progress {
            round(&mut group, &toward, true);
        }
        let reached = matches!(&group.committed, MembershipConfig::Uniform { voters, .. } if *voters == to);
        proptest::prop_assume!(!reached);
        for dead in to.iter().filter(|node| !from.contains(*node)) {
            group.alive.remove(&dead);
        }
        proptest::prop_assume!(quorum_live(group.effective(), &group.alive));
        let cancel = MembershipTarget {
            voters: from.clone(),
            learners: BTreeSet::new(),
            stage: TargetStage::Moving { since: since() },
        };
        let size = from.len();
        let remaining = group.effective().voters().into_iter().filter(|voter| !from.contains(*voter)).count();
        let limit = remaining.checked_add(FAIR_ROUNDS).unwrap();
        let ended = run(&mut group, &cancel, limit, |group, _step| check_group(group, size));
        match ended {
            Ok(Ended::Converged) => {}
            Ok(Ended::Stuck) => proptest::prop_assert!(false, "R10: stuck after {limit} rounds: {group:?}"),
            Err(violation) => proptest::prop_assert!(false, "{violation}"),
        }
    }
}
