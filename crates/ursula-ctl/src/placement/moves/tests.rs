//! The batch decisions and the `--replace` expansion, on states the real
//! kernel built.

use reqwest::StatusCode;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::ControlPlaneState;
use ursula_control::MoveRequest;
use ursula_control::MoveResponse;
use ursula_control::PlacementApiError;
use ursula_control::PlacementApiReason;
use ursula_control::samples::membership;
use ursula_control::samples::meta as meta_id;
use ursula_control::samples::node;
use ursula_control::samples::node_set;
use ursula_control::samples::params;
use ursula_control::samples::register;
use ursula_control::samples::voters;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WriteEffect;
use ursula_shard::RaftGroupId;

use super::Answered;
use super::Decision;
use super::Intent;
use super::MoveRefusal;
use super::Pending;
use super::Replacement;
use super::WriteAnswer;
use super::answered;
use super::decide;
use super::replaced;

/// A meta state driven through the kernel's own apply.
struct Meta {
    state: ControlPlaneState,
    index: u64,
}

impl Meta {
    /// Nodes 1 to 5, groups 0 and 1 reported on 1,2,3 and group 2 still
    /// seeded, with move limit `limit`.
    fn new(limit: u32) -> Self {
        let mut meta = Self {
            state: ControlPlaneState::default(),
            index: 0,
        };
        let nodes = [1, 2, 3, 4, 5];
        for id in nodes {
            meta.apply(register(id, 10));
        }
        meta.apply(ControlCommand::Bootstrap {
            params: params(&nodes, 3, 3, limit),
            nodes: node_set(&nodes),
            now_ms: 20,
        });
        for group in [0, 1] {
            let group = RaftGroupId(group);
            let placement = meta.state.cluster().unwrap().group(group).unwrap().clone();
            meta.apply(ControlCommand::ReportConverged {
                group,
                since: placement.since(),
                voters: voters(&[1, 2, 3]),
                membership: membership(1, u64::from(group.0)),
                now_ms: 25,
            });
        }
        meta
    }

    fn apply(&mut self, command: ControlCommand) -> PlacementEpoch {
        self.index = self.index.checked_add(1).unwrap();
        let at = meta_id(1, self.index);
        self.state.apply(at, command).expect("the kernel takes it");
        PlacementEpoch::new(at)
    }

    fn since(&self, group: u32) -> PlacementEpoch {
        let cluster = self.state.cluster().unwrap();
        cluster.group(RaftGroupId(group)).unwrap().since()
    }

    fn start_move(&mut self, group: u32, to: &[u64]) -> PlacementEpoch {
        let expected = self.since(group);
        self.apply(ControlCommand::RequestMove {
            group: RaftGroupId(group),
            expected,
            to: voters(to),
            now_ms: 30,
        })
    }

    fn decide(&self, group: u32, intent: &Intent, retarget: bool) -> Decision {
        decide(&self.state, RaftGroupId(group), intent, None, retarget)
    }
}

fn to(ids: &[u64]) -> Intent {
    Intent::To(voters(ids))
}

fn replace(old: u64, new: u64) -> Intent {
    Intent::Replace(Replacement {
        old: node(old),
        new: node(new),
    })
}

/// Catches a rerun that sends a request for a group the interrupted run
/// already moved: the meta group would refuse it with `no_change`, and the
/// rerun of a finished batch would fail.
#[test]
fn a_group_at_or_moving_to_its_target_is_done() {
    let mut meta = Meta::new(4);
    assert_eq!(meta.decide(0, &to(&[1, 2, 3]), false), Decision::Done {
        to: voters(&[1, 2, 3]),
        stable: true,
    });
    meta.start_move(0, &[1, 2, 4]);
    for intent in [to(&[1, 2, 4]), replace(3, 4)] {
        assert_eq!(meta.decide(0, &intent, false), Decision::Done {
            to: voters(&[1, 2, 4]),
            stable: false,
        });
    }
    // Group 1 still holds node 3, so the same replacement moves it.
    assert_eq!(
        meta.decide(1, &replace(3, 4), false),
        Decision::Submit(MoveRequest {
            expected: meta.since(1),
            to: voters(&[1, 2, 4]),
        })
    );
}

/// Catches a move that silently retargets another operator's move, and a
/// cancel that needs `--retarget` or sends a request for a stable group.
#[test]
fn a_moving_group_keeps_its_target_unless_the_move_retargets_or_cancels() {
    let mut meta = Meta::new(4);
    let since = meta.start_move(0, &[1, 2, 4]);
    assert_eq!(
        meta.decide(0, &to(&[1, 2, 5]), false),
        Decision::Refuse(MoveRefusal::Moving {
            group: RaftGroupId(0),
            to: voters(&[1, 2, 4]),
        })
    );
    let retarget = Decision::Submit(MoveRequest {
        expected: since,
        to: voters(&[1, 2, 5]),
    });
    assert_eq!(meta.decide(0, &to(&[1, 2, 5]), true), retarget);
    let expected = decide(
        &meta.state,
        RaftGroupId(0),
        &to(&[1, 2, 5]),
        Some(since),
        false,
    );
    assert_eq!(expected, retarget);
    assert_eq!(
        meta.decide(0, &Intent::Cancel, false),
        Decision::Submit(MoveRequest {
            expected: since,
            to: voters(&[1, 2, 3]),
        })
    );
    assert_eq!(
        meta.decide(1, &Intent::Cancel, false),
        Decision::Refuse(MoveRefusal::Kernel(ControlError::NotMoving {
            group: RaftGroupId(1),
        }))
    );
}

/// Catches a batch that reports a group over the move limit as failed
/// instead of queueing it, that counts no move it started in the same
/// pass, or that queues a refusal no later pass changes.
#[test]
fn a_request_over_the_move_limit_queues_and_other_refusals_end_the_group() {
    let mut meta = Meta::new(1);
    let Decision::Submit(request) = meta.decide(0, &to(&[1, 2, 4]), false) else {
        panic!("group 0 moves under the limit");
    };
    // The batch applies the accepted move to the pass's view.
    meta.apply(ControlCommand::RequestMove {
        group: RaftGroupId(0),
        expected: request.expected,
        to: request.to,
        now_ms: 30,
    });
    assert_eq!(
        meta.decide(1, &to(&[1, 2, 4]), false),
        Decision::Queue(ControlError::MoveLimit {
            limit: 1,
            moving: 1,
        })
    );
    // A retarget takes no new slot.
    assert!(matches!(
        meta.decide(0, &to(&[1, 2, 5]), true),
        Decision::Submit(_)
    ));
    let refused = |decision| match decision {
        Decision::Refuse(MoveRefusal::Kernel(error)) => error,
        other => panic!("not refused: {other:?}"),
    };
    assert_eq!(
        refused(meta.decide(1, &to(&[1, 2, 9]), false)),
        ControlError::UnknownNode { node: node(9) }
    );
    assert!(matches!(
        refused(meta.decide(1, &to(&[1, 2, 4, 5]), false)),
        ControlError::ReplicationFactorChange { .. }
    ));
    let seed = meta.state.cluster().unwrap().group(RaftGroupId(2)).unwrap();
    let seed = seed.target_voters().clone();
    let outside = (1..=5).map(node).find(|id| !seed.contains(*id)).unwrap();
    let off_seed = VoterSet::from_first(outside, seed.iter().skip(1));
    assert!(matches!(
        refused(meta.decide(2, &Intent::To(off_seed), false)),
        ControlError::GroupNotLive { .. }
    ));
    let stale = decide(
        &meta.state,
        RaftGroupId(1),
        &to(&[1, 2, 5]),
        Some(PlacementEpoch::new(meta_id(1, 1))),
        false,
    );
    assert!(matches!(
        refused(stale),
        ControlError::StalePlacement { .. }
    ));
}

/// Catches an expansion against the move's source instead of its target,
/// and a rerun that refuses a group the replacement already moved.
#[test]
fn a_replacement_expands_against_the_target() {
    let mut meta = Meta::new(4);
    let placement = |meta: &Meta, group| {
        let cluster = meta.state.cluster().unwrap();
        cluster.group(RaftGroupId(group)).unwrap().clone()
    };
    let replacement = |old, new| Replacement {
        old: node(old),
        new: node(new),
    };
    let stable = placement(&meta, 0);
    assert_eq!(
        replaced(&stable, replacement(3, 4)),
        Some(voters(&[1, 2, 4]))
    );
    assert_eq!(
        replaced(&stable, replacement(5, 3)),
        Some(voters(&[1, 2, 3]))
    );
    assert_eq!(replaced(&stable, replacement(5, 4)), None);
    meta.start_move(0, &[1, 2, 4]);
    let moving = placement(&meta, 0);
    assert_eq!(
        replaced(&moving, replacement(4, 5)),
        Some(voters(&[1, 2, 5]))
    );
    assert_eq!(replaced(&moving, replacement(3, 5)), None);
    assert_eq!(
        meta.decide(1, &replace(5, 4), false),
        Decision::Refuse(MoveRefusal::NotAVoter {
            group: RaftGroupId(1),
            node: node(5),
        })
    );
    assert_eq!("3=4".parse(), Ok(replacement(3, 4)));
    for invalid in ["3=3", "3", "=4", "a=4", "3=4=5", "-1=4"] {
        assert!(
            invalid.parse::<Replacement>().is_err(),
            "{invalid:?} parsed"
        );
    }
}

/// Catches a batch that gives up on a write that may have applied, that
/// plans a stale `--expect` again, or that retries a permanent refusal.
#[test]
fn an_answer_classifies_by_the_kernels_requeue() {
    let refused = |status, reason: PlacementApiReason, write_effect| WriteAnswer::Refused {
        status,
        refusal: Box::new(PlacementApiError {
            reason,
            write_effect,
            current: None,
            message: String::new(),
        }),
    };
    let kernel = |error| {
        refused(
            StatusCode::CONFLICT,
            PlacementApiReason::Refused(error),
            WriteEffect::NotApplied,
        )
    };
    let since = PlacementEpoch::new(meta_id(2, 7));
    assert_eq!(
        answered(2, WriteAnswer::Applied(MoveResponse { since }), false),
        Answered::Accepted(since)
    );
    let lost = WriteAnswer::Lost(anyhow::anyhow!("connection reset"));
    assert_eq!(
        answered(2, lost, false),
        Answered::Pending(Pending::MayHaveApplied)
    );
    let timed_out = refused(
        StatusCode::GATEWAY_TIMEOUT,
        PlacementApiReason::NoLeader,
        WriteEffect::MayHaveApplied,
    );
    assert_eq!(
        answered(2, timed_out, true),
        Answered::Pending(Pending::MayHaveApplied)
    );
    let limit = ControlError::MoveLimit {
        limit: 2,
        moving: 2,
    };
    assert_eq!(
        answered(2, kernel(limit.clone()), true),
        Answered::Pending(Pending::Queued(limit))
    );
    let stale = ControlError::StalePlacement {
        group: RaftGroupId(0),
        current: Box::new(
            Meta::new(4)
                .state
                .cluster()
                .unwrap()
                .group(RaftGroupId(0))
                .unwrap()
                .clone(),
        ),
    };
    assert_eq!(
        answered(2, kernel(stale.clone()), false),
        Answered::Pending(Pending::Replan(PlacementApiReason::Refused(stale.clone())))
    );
    assert_eq!(
        answered(2, kernel(stale.clone()), true),
        Answered::Refuse(MoveRefusal::Kernel(stale))
    );
    let no_change = ControlError::NoChange {
        group: RaftGroupId(0),
    };
    assert_eq!(
        answered(2, kernel(no_change.clone()), false),
        Answered::Refuse(MoveRefusal::Kernel(no_change))
    );
    let no_leader = refused(
        StatusCode::SERVICE_UNAVAILABLE,
        PlacementApiReason::NoLeader,
        WriteEffect::NotApplied,
    );
    assert_eq!(
        answered(2, no_leader, true),
        Answered::Pending(Pending::Replan(PlacementApiReason::NoLeader))
    );
    let stale_incarnation = WriteAnswer::Unexpected {
        status: StatusCode::PRECONDITION_FAILED,
        body: "stale incarnation".to_owned(),
    };
    assert!(matches!(
        answered(2, stale_incarnation, false),
        Answered::Refuse(MoveRefusal::Unexpected { via: 2, .. })
    ));
}
