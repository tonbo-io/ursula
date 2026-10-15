//! One test per row of the reconcile table of design §4.9, each through both columns: a takeover
//! and a continuation claim, driven through the replicator with the claim's answer and the read
//! as they travel.

use ursula_headers::Delivery;

use super::ACTIVATION;
use super::Harness;
use super::LINEAGE;
use super::cluster;
use super::commit;
use super::epoch;
use super::local;
use super::off;
use super::producer;
use super::read;
use super::receipt;
use super::reset_of;
use super::sends;
use super::token;
use crate::codec::frame;
use crate::codec::frame::Frame;
use crate::ids::Activation;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::replicator::Action;
use crate::replicator::ClaimKind;
use crate::replicator::Event;
use crate::replicator::ForeignFrame;
use crate::replicator::LocalState;
use crate::replicator::Pending;
use crate::replicator::Replicator;
use crate::replicator::ResetKind;
use crate::replicator::reconcile::FrameClass;
use crate::replicator::reconcile::Mode;
use crate::replicator::reconcile::RowDecision;
use crate::replicator::reconcile::decide_frame;
use crate::wire::RawAnswer;
use crate::wire::Target;

const FOREIGN: Activation = Activation([0xf0; 16]);
const OTHER_LINEAGE: Lineage = Lineage([0x22; 16]);

const KINDS: [ClaimKind; 2] = [ClaimKind::Takeover { round: 1 }, ClaimKind::Continuation {
    own: ursula_headers::Epoch::ZERO,
}];

/// Activates `kind` with pending records 1 and 2 from offset 0, wins the claim at the end of
/// `stream`, and answers the reconcile's read with `read_answer` (the stream by default).
fn reconcile(
    kind: ClaimKind,
    stream: &[u8],
    read_answer: Option<RawAnswer>,
) -> (Harness, Vec<Action>) {
    let pending = vec![commit(LINEAGE, 1, 1), commit(LINEAGE, 2, 2)];
    let (mut h, _) = Harness::activate(kind, local(pending, 0));
    let end = u64::try_from(stream.len()).unwrap();
    let claim = frame::encode_claim(epoch(1), ACTIVATION);
    let next = end + u64::try_from(claim.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        0,
        end,
        next,
        None,
    )));
    if end == 0 {
        return (h, actions);
    }
    assert_eq!(sends(&actions).len(), 1, "the reconcile reads: {actions:?}");
    let answer = read_answer.unwrap_or_else(|| read(stream, end));
    let actions = h.answer(&answer);
    (h, actions)
}

fn concat(frames: &[&[u8]]) -> Vec<u8> {
    frames.concat()
}

fn acked(actions: &[Action]) -> Option<Position> {
    actions.iter().rev().find_map(|a| match a {
        Action::Wake { acked, .. } => Some(*acked),
        _ => None,
    })
}

fn streaming(h: &Harness) -> bool {
    matches!(h.replicator, Replicator::Streaming { .. })
}

#[test]
fn row_1_a_claim_with_our_activation_is_skipped() {
    let own = frame::encode_claim(epoch(1), ACTIVATION);
    let p1 = commit(LINEAGE, 1, 1);
    for kind in KINDS {
        let mode = mode(kind);
        assert_eq!(decide_frame(mode, FrameClass::OwnClaim), RowDecision::Skip);
        let (h, actions) = reconcile(kind, &concat(&[&own, &p1.frame]), None);
        assert!(streaming(&h), "{kind:?}: {actions:?}");
        assert_eq!(acked(&actions), Some(Position(1)), "{kind:?}");
        assert_eq!(reset_of(&actions), None);
    }
}

#[test]
fn row_2_the_next_pending_commit_is_acknowledged() {
    let p1 = commit(LINEAGE, 1, 1);
    let p2 = commit(LINEAGE, 2, 2);
    for kind in KINDS {
        assert_eq!(
            decide_frame(mode(kind), FrameClass::NextPending),
            RowDecision::Acknowledge
        );
        let (h, actions) = reconcile(kind, &concat(&[&p1.frame, &p2.frame]), None);
        assert!(streaming(&h));
        assert_eq!(acked(&actions), Some(Position(2)), "{kind:?}");
        // Nothing is left to send.
        assert!(sends(&actions).is_empty(), "{actions:?}");
    }
    // A complete PART run with the next record's id and bytes acknowledges it too.
    let Frame::Commit { id, compressed } = frame::decode(&p1.frame, 1 << 20)
        .map(|d| match d {
            frame::Decoded::Frame { frame, .. } => frame,
            frame::Decoded::Partial => unreachable!(),
        })
        .unwrap()
    else {
        unreachable!()
    };
    let (a, b) = compressed.split_at(2);
    let parts: Vec<u8> = [(0, a), (1, b)]
        .iter()
        .flat_map(|(index, chunk)| {
            frame::encode(&Frame::Part {
                id,
                index: *index,
                count: 2,
                chunk: chunk.to_vec(),
            })
            .unwrap()
        })
        .collect();
    for kind in KINDS {
        let (_, actions) = reconcile(kind, &parts, None);
        assert_eq!(acked(&actions), Some(Position(1)), "{kind:?}: {actions:?}");
    }
}

#[test]
fn row_3_a_foreign_claim_is_skipped_by_a_takeover_and_fences_a_continuation() {
    let foreign = frame::encode_claim(epoch(1), FOREIGN);
    let p1 = commit(LINEAGE, 1, 1);
    let stream = concat(&[&foreign, &p1.frame]);
    assert_eq!(
        decide_frame(Mode::Takeover, FrameClass::ForeignClaim),
        RowDecision::Skip
    );
    let (h, actions) = reconcile(KINDS[0], &stream, None);
    assert!(streaming(&h));
    assert_eq!(acked(&actions), Some(Position(1)));
    assert_eq!(
        decide_frame(Mode::Continuation, FrameClass::ForeignClaim),
        RowDecision::Reset(ResetKind::Fenced)
    );
    let (h, actions) = reconcile(KINDS[1], &stream, None);
    assert_eq!(reset_of(&actions), Some(ResetKind::Fenced));
    assert!(matches!(h.replicator, Replicator::Reset(ResetKind::Fenced)));
}

#[test]
fn row_4_a_foreign_commit_diverges_a_takeover_and_resets_a_continuation_stale() {
    let p1 = commit(LINEAGE, 1, 1);
    let foreign = commit(OTHER_LINEAGE, 9, 9);
    // Our commit id with other bytes is the same row.
    let other_bytes = commit(LINEAGE, 2, 7);
    for (stream, discard_from) in [
        (concat(&[&p1.frame, &foreign.frame]), Position(2)),
        (concat(&[&p1.frame, &other_bytes.frame]), Position(2)),
        (concat(&[&foreign.frame, &p1.frame]), Position(1)),
    ] {
        assert_eq!(
            decide_frame(Mode::Takeover, FrameClass::ForeignCommit),
            RowDecision::Diverge
        );
        let (mut h, actions) = reconcile(KINDS[0], &stream, None);
        // A discarded tail always rebuilds, and nothing is applied in place.
        assert!(
            actions.contains(&rebuild(Some(discard_from), stream.len() + 37)),
            "{actions:?}"
        );
        assert!(!actions.iter().any(|a| matches!(a, Action::Apply(_))));
        assert!(sends(&actions).is_empty(), "{actions:?}");
        // The replicator binds once the driver caught up.
        assert!(!streaming(&h));
        let actions = h.on(Event::CaughtUp {
            last: Position(7),
            log_bytes: 900,
        });
        assert!(actions.contains(&Action::Bound { epoch: epoch(1) }));
        assert!(streaming(&h));
        assert_eq!(acked(&actions), Some(Position(7)));
        assert!(sends(&actions).is_empty(), "{actions:?}");

        assert_eq!(
            decide_frame(Mode::Continuation, FrameClass::ForeignCommit),
            RowDecision::Reset(ResetKind::Stale)
        );
        let (_, actions) = reconcile(KINDS[1], &stream, None);
        assert_eq!(reset_of(&actions), Some(ResetKind::Stale));
    }
    // With nothing pending a takeover catches up in place: no rebuild.
    let (mut h, _) = Harness::activate(KINDS[0], local(Vec::new(), 0));
    let end = u64::try_from(foreign.frame.len()).unwrap();
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        0,
        end,
        end + 37,
        None,
    )));
    h.on(Event::Sync {
        ticket: 3,
        target: Position(0),
    });
    let actions = h.answer(&read(&foreign.frame, end));
    assert!(!actions.iter().any(|a| matches!(a, Action::Rebuild { .. })));
    assert_eq!(actions, vec![Action::Apply(vec![ForeignFrame {
        frame: decoded(&foreign.frame),
        end_offset: off(end),
    }])]);
    // The caught-up frame took position 1, acknowledged: the ticket (called after the claim went
    // out) needs only a probe, not a commit.
    let actions = h.on(Event::CaughtUp {
        last: Position(1),
        log_bytes: 77,
    });
    assert!(actions.contains(&Action::PersistAck {
        acked: Position(1),
        acked_offset: off(end + 37),
        log_bytes: 77,
    }));
    assert_eq!(acked(&actions), Some(Position(1)));
    assert_eq!(sends(&actions).len(), 1, "{actions:?}");
    let actions = h.answer(&read(&[], end + 37));
    assert!(
        actions.contains(&Action::Synced { ticket: 3 }),
        "{actions:?}"
    );
    // Later commits take later positions.
    let actions = h.on(Event::Logged(commit(LINEAGE, 1, 1)));
    assert_eq!(reset_of(&actions), Some(ResetKind::Internal));
}

#[test]
fn row_5_undecodable_bytes_end_a_takeover_and_a_continuation_with_stream_state() {
    let garbage = b"not a frame at all".to_vec();
    assert_eq!(
        decide_frame(Mode::Takeover, FrameClass::Undecodable),
        RowDecision::Reset(ResetKind::StreamState)
    );
    let (h, actions) = reconcile(KINDS[0], &garbage, None);
    assert_eq!(reset_of(&actions), Some(ResetKind::StreamState));
    assert!(matches!(
        h.replicator,
        Replicator::Reset(ResetKind::StreamState)
    ));
    assert_eq!(
        decide_frame(Mode::Continuation, FrameClass::Undecodable),
        RowDecision::Reset(ResetKind::StreamState)
    );
    let (_, actions) = reconcile(KINDS[1], &garbage, None);
    assert_eq!(reset_of(&actions), Some(ResetKind::StreamState));
}

#[test]
fn row_6_a_410_at_the_start_rebuilds_a_takeover_and_resets_a_continuation_stale() {
    let p1 = commit(LINEAGE, 1, 1);
    assert_eq!(
        decide_frame(Mode::Takeover, FrameClass::Gone),
        RowDecision::Diverge
    );
    let (h, actions) = reconcile(KINDS[0], &p1.frame, Some(RawAnswer::new(410)));
    assert!(!streaming(&h));
    assert!(
        actions.contains(&rebuild(Some(Position(1)), p1.frame.len() + 37)),
        "{actions:?}"
    );
    assert_eq!(
        decide_frame(Mode::Continuation, FrameClass::Gone),
        RowDecision::Reset(ResetKind::Stale)
    );
    let (_, actions) = reconcile(KINDS[1], &p1.frame, Some(RawAnswer::new(410)));
    assert_eq!(reset_of(&actions), Some(ResetKind::Stale));
}

#[test]
fn row_6_a_410_with_nothing_pending_still_rebuilds() {
    let (mut h, _) = Harness::activate(KINDS[0], local(Vec::new(), 0));
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 500, 537, None)));
    let actions = h.answer(&RawAnswer::new(410));
    assert_eq!(actions, vec![rebuild(None, 537)]);
    let actions = h.on(Event::CaughtUp {
        last: Position(4),
        log_bytes: 0,
    });
    assert!(streaming(&h));
    assert!(actions.contains(&Action::Wake {
        acked: Position(4),
        offset: off(537),
    }));
}

/// Takes over with nothing pending and `db_bytes`, the claim at `2 * len`: two foreign commits of
/// `len` bytes each come before it.
fn two_foreign(db_bytes: u64) -> (Harness, Pending, Pending, u64) {
    let first = commit(OTHER_LINEAGE, 9, 9);
    let second = commit(OTHER_LINEAGE, 14, 9);
    assert_eq!(first.frame.len(), second.frame.len());
    let local = LocalState {
        db_bytes,
        ..local(Vec::new(), 0)
    };
    let (mut h, _) = Harness::activate(KINDS[0], local);
    let len = u64::try_from(first.frame.len()).unwrap();
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        0,
        2 * len,
        2 * len + 37,
        None,
    )));
    (h, first, second, len)
}

#[test]
fn a_catch_up_in_place_applies_each_read_before_the_next() {
    let (mut h, first, second, len) = two_foreign(u64::MAX);
    // The first read's frame is applied; the next read waits for the driver.
    let actions = h.answer(&read(&first.frame, len));
    assert_eq!(actions, vec![Action::Apply(vec![ForeignFrame {
        frame: decoded(&first.frame),
        end_offset: off(len),
    }])]);
    assert!(h.on(Event::Tick).is_empty());
    let actions = h.on(Event::CaughtUp {
        last: Position(1),
        log_bytes: 10,
    });
    let reads = sends(&actions);
    assert_eq!(reads.len(), 1);
    assert!(matches!(reads[0].target, Target::Read { from: Some(from), .. } if from == off(len)));
    let actions = h.answer(&read(&second.frame, 2 * len));
    assert_eq!(actions, vec![Action::Apply(vec![ForeignFrame {
        frame: decoded(&second.frame),
        end_offset: off(2 * len),
    }])]);
    assert!(!streaming(&h));
    let actions = h.on(Event::CaughtUp {
        last: Position(2),
        log_bytes: 20,
    });
    assert!(actions.contains(&Action::Bound { epoch: epoch(1) }));
    assert_eq!(acked(&actions), Some(Position(2)));
    assert!(streaming(&h));
}

#[test]
fn a_catch_up_in_place_that_grows_past_the_database_rebuilds() {
    let record = |p: &Pending| match decoded(&p.frame) {
        Frame::Commit { compressed, .. } => u64::try_from(compressed.len()).unwrap(),
        _ => unreachable!(),
    };
    let (mut h, first, second, len) = two_foreign(0);
    let one = record(&first);
    // Room for one frame's record: the first applies, the second rebuilds.
    let (mut h2, _, _, _) = two_foreign(one);
    let actions = h2.answer(&read(&first.frame, len));
    assert!(
        matches!(actions.as_slice(), [Action::Apply(_)]),
        "{actions:?}"
    );
    h2.on(Event::CaughtUp {
        last: Position(1),
        log_bytes: 10,
    });
    let actions = h2.answer(&read(&second.frame, 2 * len));
    let len_usize = usize::try_from(len).unwrap();
    assert_eq!(actions, vec![rebuild(None, 2 * len_usize + 37)]);
    assert!(record(&second) > 0);
    // An empty database rebuilds at the first frame.
    let actions = h.answer(&read(&first.frame, len));
    assert_eq!(actions, vec![rebuild(None, 2 * len_usize + 37)]);
}

#[test]
fn a_410_after_a_read_was_applied_rebuilds() {
    let (mut h, first, _, len) = two_foreign(u64::MAX);
    // The first read returns one foreign commit, applied; the next finds it retained away.
    let actions = h.answer(&read(&first.frame, len));
    assert!(
        matches!(actions.as_slice(), [Action::Apply(_)]),
        "{actions:?}"
    );
    let actions = h.on(Event::CaughtUp {
        last: Position(1),
        log_bytes: 10,
    });
    assert_eq!(sends(&actions).len(), 1);
    let actions = h.answer(&RawAnswer::new(410));
    let len = usize::try_from(len).unwrap();
    assert_eq!(actions, vec![rebuild(None, 2 * len + 37)]);
}

fn mode(kind: ClaimKind) -> Mode {
    match kind {
        ClaimKind::Takeover { .. } => Mode::Takeover,
        ClaimKind::Continuation { .. } => Mode::Continuation,
    }
}

/// The rebuild of a takeover that claimed epoch 1, up to the claim's end `to`.
fn rebuild(discard_from: Option<Position>, to: usize) -> Action {
    Action::Rebuild {
        discard_from,
        to: off(u64::try_from(to).unwrap()),
        epoch: epoch(1),
        activation: ACTIVATION,
        incarnation: token(),
        cluster_id: cluster(),
    }
}

fn decoded(bytes: &[u8]) -> Frame {
    match frame::decode(bytes, 1 << 20).unwrap() {
        frame::Decoded::Frame { frame, .. } => frame,
        frame::Decoded::Partial => unreachable!(),
    }
}
