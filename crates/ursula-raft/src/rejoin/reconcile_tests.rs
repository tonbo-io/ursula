use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::alias::LogIdOf;
use openraft::error::ChangeMembershipError;
use openraft::error::ClientWriteError;
use openraft::error::EmptyMembership;
use openraft::error::Fatal;
use openraft::error::ForwardToLeader;
use openraft::error::InProgress;
use openraft::error::LearnerNotFound;
use openraft::error::PreconditionFailed;
use openraft::error::RaftError;
use openraft::metrics::SerdeInstant;
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::StoredMembershipOf;
use openraft::vote::RaftLeaderId;
use ursula_control::ControlApplied;
use ursula_control::ControlError;
use ursula_control::EffectiveMembership;
use ursula_control::GroupPlacement;
use ursula_control::Leadership;
use ursula_control::ViewFreshness;
use ursula_control::samples;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::StepFailureKind;
use ursula_shard::RaftGroupId;

use super::PendingStep;
use super::ShellInputs;
use super::StepOutcome;
use super::membership_outcome;
use super::membership_view;
use super::report_outcome;
use crate::meta::handle::MetaWriteError;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaVote;

type LeaderId = <C as openraft::RaftTypeConfig>::LeaderId;

fn log_id(term: u64, leader: u64, index: u64) -> LogIdOf<C> {
    LogIdOf::<C> {
        leader_id: LeaderId::new(term, leader),
        index,
    }
}

fn membership(at: LogIdOf<C>, voters: &[u64], learners: &[u64]) -> Arc<StoredMembershipOf<C>> {
    Arc::new(StoredMembershipOf::<C>::new(
        Some(at),
        openraft::Membership::new_with_defaults(
            vec![voters.iter().copied().collect()],
            learners.iter().copied(),
        ),
    ))
}

fn inputs() -> ShellInputs {
    ShellInputs {
        freshness: ViewFreshness::Unconfirmed,
        handoff_ready: BTreeSet::new(),
        unlisted: BTreeSet::new(),
    }
}

/// Node 1 leads live at term 3 under a committed vote, and its effective
/// membership {1,2,3} at index 10 is committed. Node 2 answered just now,
/// node 3 never did in this leadership.
fn leader() -> RaftMetrics<C> {
    let mut metrics = RaftMetrics::<C>::new_initial(1);
    metrics.state = ServerState::Leader;
    metrics.current_leader = Some(1);
    metrics.vote = UrsulaVote::new_committed(3, 1);
    metrics.last_quorum_acked = Some(SerdeInstant::new(C::now()));
    metrics.local_committed = Some(log_id(3, 1, 12));
    let at = log_id(3, 1, 10);
    metrics.membership_config = membership(at, &[1, 2, 3], &[4]);
    metrics.committed_membership_config = membership(at, &[1, 2, 3], &[4]);
    metrics.heartbeat = Some(BTreeMap::from([
        (2, Some(SerdeInstant::new(C::now()))),
        (3, None),
    ]));
    metrics.replication = Some(BTreeMap::from([
        (2, Some(log_id(3, 1, 12))),
        (3, None),
        (4, Some(log_id(3, 1, 7))),
    ]));
    metrics
}

/// The shell plans only from a committed membership: an effective
/// membership whose log id is not the committed one is `Uncommitted`, so
/// no step is planned from a membership a new leader may truncate. A
/// leader that is not live, or whose vote is not committed, does not lead.
/// Catches a view that reads the effective membership as committed, or a
/// leader whose quorum went quiet planning anyway.
#[test]
fn the_view_plans_only_from_a_committed_membership_of_a_live_leader() {
    let me = NodeId::new(1);
    let window = Duration::from_secs(1);
    let Leadership::Leading(view) =
        membership_view(&leader(), me, window, PendingStep::None, inputs()).leadership
    else {
        panic!("a live leader leads");
    };
    assert!(matches!(
        &view.membership,
        EffectiveMembership::Committed { log_id, .. } if log_id.get().index == 10 && log_id.get().term == 3
    ));
    assert_eq!(view.committed_index, 12);
    assert_eq!(view.live, BTreeSet::from([NodeId::new(2)]));
    assert_eq!(view.matched.get(&NodeId::new(4)), Some(&Some(7)));

    let mut uncommitted = leader();
    uncommitted.membership_config = membership(log_id(3, 1, 11), &[1, 2, 3, 4], &[]);
    assert!(matches!(
        membership_view(&uncommitted, me, window, PendingStep::None, inputs()).leadership,
        Leadership::Leading(view) if matches!(
            view.membership,
            EffectiveMembership::Uncommitted { log_id } if log_id.get().index == 11
        )
    ));

    let mut quiet = leader();
    quiet.last_quorum_acked = None;
    assert_eq!(
        membership_view(&quiet, me, window, PendingStep::None, inputs()).leadership,
        Leadership::NotLeading
    );
    let mut campaigning = leader();
    campaigning.vote = UrsulaVote::new(3, 1);
    assert_eq!(
        membership_view(&campaigning, me, window, PendingStep::None, inputs()).leadership,
        Leadership::NotLeading
    );
    assert_eq!(
        membership_view(&leader(), me, window, PendingStep::Step, inputs()).leadership,
        Leadership::StepPending
    );
    assert_eq!(
        membership_view(
            &leader(),
            me,
            window,
            PendingStep::Transfer { to: NodeId::new(2) },
            inputs()
        )
        .leadership,
        Leadership::Transferring { to: NodeId::new(2) }
    );
}

fn refused(
    error: ClientWriteError<C>,
) -> Result<openraft::raft::ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>> {
    Err(RaftError::APIError(error))
}

/// How OpenRaft's refusals of a guarded membership change classify. A
/// mismatch on the guard the shell passed re-plans. A mismatch on another
/// membership log id is OpenRaft's guard of the second entry, so the joint
/// entry committed. A moved leadership is not leading, whichever guard
/// refused it. Catches a second-step refusal read as a stale plan, and a
/// deposed leader that keeps re-planning as if it led.
#[test]
fn a_refused_membership_step_classifies_by_the_guard_that_refused_it() {
    let expected = log_id(3, 1, 10);
    let joint = log_id(3, 1, 11);
    let stale = |guarded: LogIdOf<C>| {
        refused(ClientWriteError::PreconditionFailed(
            PreconditionFailed::LastMembershipLogIdMismatch {
                expected: Some(guarded),
                actual: Some(log_id(3, 1, 12)),
            },
        ))
    };
    assert!(matches!(
        membership_outcome(stale(expected), expected),
        StepOutcome::Replan {
            kind: StepFailureKind::PreconditionFailed,
            ..
        }
    ));
    assert!(matches!(
        membership_outcome(stale(joint), expected),
        StepOutcome::JointCommitted
    ));
    let other_leader = refused(ClientWriteError::PreconditionFailed(
        PreconditionFailed::CommittedLeaderIdMismatch {
            expected: LeaderId::new(3, 1).to_committed(),
            actual: Some(LeaderId::new(4, 2).to_committed()),
        },
    ));
    assert!(matches!(
        membership_outcome(other_leader, expected),
        StepOutcome::NotLeader
    ));
    assert!(matches!(
        membership_outcome(
            refused(ClientWriteError::ForwardToLeader(ForwardToLeader::empty())),
            expected
        ),
        StepOutcome::NotLeader
    ));
    assert!(matches!(
        membership_outcome(
            refused(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::InProgress(InProgress {
                    committed: Some(expected),
                    membership_log_id: Some(joint),
                })
            )),
            expected
        ),
        StepOutcome::Replan {
            kind: StepFailureKind::InProgress,
            ..
        }
    ));
    assert!(matches!(
        membership_outcome(
            refused(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::LearnerNotFound(LearnerNotFound { node_id: 4 })
            )),
            expected
        ),
        StepOutcome::Replan {
            kind: StepFailureKind::PreconditionFailed,
            ..
        }
    ));
    assert!(matches!(
        membership_outcome(Err(RaftError::Fatal(Fatal::Stopped)), expected),
        StepOutcome::Stopped
    ));
    // A change no plan makes is a planner bug, never planned again as if
    // the membership had moved.
    assert!(matches!(
        membership_outcome(
            refused(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::EmptyMembership(EmptyMembership {})
            )),
            expected
        ),
        StepOutcome::Unplannable(_)
    ));
}

/// How the meta group's answers to a report classify (design 5.7). An
/// applied report waits for the view, a stale one names the placement it
/// lost to, and a refusal whose class is to plan again from a confirmed
/// view makes the shell confirm its view, as does an answer that may have
/// applied. A write that did not apply is retried as it is. Catches a stale
/// report read as applied, which would wait for a view that never moves,
/// and a lost answer read as not applied, which would report again without
/// learning whether the first one applied.
#[test]
fn a_report_answer_classifies_by_what_the_meta_group_did() {
    let since = PlacementEpoch::new(samples::meta(2, 7));
    let applied = report_outcome(
        Ok(ControlApplied::Converged {
            since: PlacementEpoch::new(samples::meta(2, 8)),
            retired: Default::default(),
            removed: Default::default(),
        }),
        since,
    );
    assert!(matches!(applied, StepOutcome::Reported { since: reported, .. } if reported == since));
    assert!(!applied.rereads());
    assert_eq!(applied.failure(), None);

    let current = GroupPlacement::Seeded {
        since: PlacementEpoch::new(samples::meta(3, 9)),
        voters: samples::voters(&[1, 2, 3]),
        initializer: samples::node(1),
    };
    let stale = report_outcome(
        Err(MetaWriteError::Refused(ControlError::StalePlacement {
            group: RaftGroupId(1),
            current: Box::new(current.clone()),
        })),
        since,
    );
    assert!(matches!(
        stale,
        StepOutcome::StalePlacement { reported, current: now }
            if reported == since && now == current.since()
    ));
    assert!(stale.rereads());

    let not_moving = report_outcome(
        Err(MetaWriteError::Refused(ControlError::NotMoving {
            group: RaftGroupId(1),
        })),
        since,
    );
    assert!(matches!(not_moving, StepOutcome::ReportRefused(_)));
    assert!(not_moving.rereads());
    let not_target = report_outcome(
        Err(MetaWriteError::Refused(ControlError::NotTarget {
            group: RaftGroupId(1),
            reported: samples::voters(&[1, 2]),
        })),
        since,
    );
    assert!(matches!(not_target, StepOutcome::ReportRefused(_)));
    assert!(!not_target.rereads());
    assert_eq!(
        not_target.failure(),
        Some(StepFailureKind::PreconditionFailed)
    );

    let lost = report_outcome(
        Err(MetaWriteError::Leader {
            leader: NodeId::new(2),
            source: ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::OutcomeUnknown,
            ),
        }),
        since,
    );
    assert!(matches!(lost, StepOutcome::ReportUnknown(_)));
    assert!(lost.rereads());
    let no_leader = report_outcome(Err(MetaWriteError::NoLeader), since);
    assert!(matches!(no_leader, StepOutcome::ReportFailed(_)));
    assert!(!no_leader.rereads());
    assert_eq!(no_leader.failure(), Some(StepFailureKind::MetaUnavailable));
}
