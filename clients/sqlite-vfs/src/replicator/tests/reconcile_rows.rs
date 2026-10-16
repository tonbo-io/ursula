//! One test per row of the reconcile table (design: Reconcile), each through both columns: a
//! takeover and a continuation claim, driven through the replicator with the claim's answer and the
//! read as they travel.

use ursula_headers::Delivery;

use super::ACTIVATION;
use super::Harness;
use super::LINEAGE;
use super::cluster;
use super::commit;
use super::epoch;
use super::head_naming;
use super::local;
use super::off;
use super::producer;
use super::read;
use super::receipt;
use super::reset_of;
use super::sends;
use super::snapshot_body;
use super::snapshot_read;
use super::token;
use crate::codec::frame;
use crate::codec::frame::Frame;
use crate::ids::Activation;
use crate::ids::CommitId;
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
use crate::wire::Method;
use crate::wire::RawAnswer;
use crate::wire::Target;

const FOREIGN: Activation = Activation([0xf0; 16]);
const OTHER_LINEAGE: Lineage = Lineage([0x22; 16]);

const KINDS: [ClaimKind; 2] = [
    ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    },
    ClaimKind::Continuation {
        own: ursula_headers::Epoch::ZERO,
        resent: false,
    },
];

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
            lineage: LINEAGE,
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
        lineage: LINEAGE,
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
    let tail = u64::try_from(p1.frame.len()).unwrap() + 37;
    let (mut h, actions) = reconcile(KINDS[0], &p1.frame, Some(RawAnswer::new(410)));
    // The rule reads the latest snapshot's header: a HEAD names it first, and here none exists.
    assert_eq!(sends(&actions)[0].method, Method::Head, "{actions:?}");
    let actions = h.answer(&head_naming(None, tail));
    assert!(!streaming(&h));
    assert!(
        actions.contains(&rebuild(Some(Position(1)), p1.frame.len() + 37)),
        "{actions:?}"
    );
    assert_eq!(
        decide_frame(Mode::Continuation, FrameClass::Gone),
        RowDecision::Reset(ResetKind::Stale)
    );
    let (mut h, _) = reconcile(KINDS[1], &p1.frame, Some(RawAnswer::new(410)));
    let actions = h.answer(&head_naming(None, tail));
    assert_eq!(reset_of(&actions), Some(ResetKind::Stale));
}

/// Pending records 1 and 2 landed before the claim, retention passed them, and the latest
/// snapshot (at the end of record 2) is `last`'s: the 410 rule's input.
fn gone_with_snapshot(kind: ClaimKind, last: CommitId) -> (Harness, Vec<Action>, u64) {
    let p1 = commit(LINEAGE, 1, 1);
    let p2 = commit(LINEAGE, 2, 2);
    let w = u64::try_from(p1.frame.len() + p2.frame.len()).unwrap();
    let (mut h, actions) = reconcile(
        kind,
        &concat(&[&p1.frame, &p2.frame]),
        Some(RawAnswer::new(410)),
    );
    assert_eq!(sends(&actions)[0].method, Method::Head);
    let (body, snapshot) = snapshot_body(w, last);
    let actions = h.answer(&head_naming(Some(snapshot), w + 37));
    assert_eq!(
        sends(&actions)[0].target,
        Target::Snapshot { offset: off(w) },
        "{actions:?}"
    );
    let actions = h.answer(&snapshot_read(snapshot, body));
    (h, actions, w)
}

// A snapshot of our lineage at or below the acknowledged position: the stream holds that state
// under our lineage and this copy acknowledged it, so a takeover goes on from its offset (here the
// claim's start, other claims having come between), and nothing is rebuilt. A continuation never
// skips bytes it did not read: it goes on only from a snapshot where its reads are (design:
// Reconcile), and resets `stale` for one past them, whatever the bytes it would skip hold.
#[test]
fn row_6_a_410_continues_from_our_lineages_snapshot_at_or_below_acked() {
    // The continuation's snapshot sits at its acknowledged offset, and two claims of its own follow.
    for (kind, from, w) in [(KINDS[0], 100, 200), (KINDS[1], 126, 126)] {
        for position in [1, 2] {
            let state = LocalState {
                acked: Position(2),
                logged: Position(2),
                ..local(Vec::new(), from)
            };
            let (mut h, _) = Harness::activate(kind, state);
            h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 200, 237, None)));
            let actions = h.answer(&RawAnswer::new(410));
            assert_eq!(sends(&actions)[0].method, Method::Head);
            let last = CommitId {
                lineage: LINEAGE,
                position: Position(position),
            };
            let (body, snapshot) = snapshot_body(w, last);
            h.answer(&head_naming(Some(snapshot), 237));
            // Only the header is read.
            assert_eq!(
                h.last_request().range,
                Some(ursula_headers::ByteRange::Bounded { first: 0, last: 55 })
            );
            let mut actions = h.answer(&snapshot_read(snapshot, body[..56].to_vec()));
            if w < 200 {
                let own = frame::encode_claim(epoch(1), ACTIVATION);
                actions = h.answer(&read(&[own.clone(), own].concat(), 200));
            }
            assert!(streaming(&h), "{kind:?} {position}: {actions:?}");
            assert!(!actions.iter().any(|a| matches!(a, Action::Rebuild { .. })));
            assert!(
                actions.contains(&Action::Wake {
                    acked: Position(2),
                    offset: off(237),
                }),
                "{actions:?}"
            );
        }
    }
    let state = LocalState {
        acked: Position(2),
        logged: Position(2),
        ..local(Vec::new(), 100)
    };
    let (mut h, _) = Harness::activate(KINDS[1], state);
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 200, 237, None)));
    h.answer(&RawAnswer::new(410));
    let last = CommitId {
        lineage: LINEAGE,
        position: Position(2),
    };
    let (body, snapshot) = snapshot_body(200, last);
    h.answer(&head_naming(Some(snapshot), 237));
    let actions = h.answer(&snapshot_read(snapshot, body[..56].to_vec()));
    assert_eq!(reset_of(&actions), Some(ResetKind::Stale), "{actions:?}");
}

// Another lineage's snapshot, or ours past what we logged, or one whose body does not match
// its digest: a takeover diverges, a continuation resets `stale`.
#[test]
fn row_6_a_410_with_a_snapshot_that_does_not_cover_us_diverges() {
    let foreign = CommitId {
        lineage: OTHER_LINEAGE,
        position: Position(2),
    };
    let ahead = CommitId {
        lineage: LINEAGE,
        position: Position(3),
    };
    // Ours past the acknowledged position: maybe a clone's commit of our lineage with other
    // bytes, never trusted.
    let clone = CommitId {
        lineage: LINEAGE,
        position: Position(2),
    };
    for last in [foreign, ahead, clone] {
        let (_, actions, w) = gone_with_snapshot(KINDS[0], last);
        let w = usize::try_from(w).unwrap();
        assert!(
            actions.contains(&rebuild(Some(Position(1)), w + 37)),
            "{actions:?}"
        );
        let (_, actions, _) = gone_with_snapshot(KINDS[1], last);
        assert_eq!(reset_of(&actions), Some(ResetKind::Stale));
    }
    // A header whose checksum fails.
    let p1 = commit(LINEAGE, 1, 1);
    let w = u64::try_from(p1.frame.len()).unwrap();
    let (mut h, _) = reconcile(KINDS[0], &p1.frame, Some(RawAnswer::new(410)));
    let last = CommitId {
        lineage: LINEAGE,
        position: Position(1),
    };
    let (mut body, snapshot) = snapshot_body(w, last);
    h.answer(&head_naming(Some(snapshot), w + 37));
    body[20] ^= 1;
    let actions = h.answer(&snapshot_read(snapshot, body));
    assert!(
        matches!(actions.first(), Some(Action::Rebuild { .. })),
        "{actions:?}"
    );
}

// A snapshot superseded between the HEAD and its read: the HEAD again.
#[test]
fn row_6_a_snapshot_gone_before_its_read_heads_again() {
    let p1 = commit(LINEAGE, 1, 1);
    let w = u64::try_from(p1.frame.len()).unwrap();
    let (mut h, _) = reconcile(KINDS[0], &p1.frame, Some(RawAnswer::new(410)));
    let last = CommitId {
        lineage: LINEAGE,
        position: Position(1),
    };
    let (_, snapshot) = snapshot_body(w, last);
    h.answer(&head_naming(Some(snapshot), w + 37));
    let actions = h.answer(&RawAnswer::new(404));
    assert_eq!(sends(&actions)[0].method, Method::Head, "{actions:?}");
}

#[test]
fn row_6_a_410_with_nothing_pending_still_rebuilds() {
    let (mut h, _) = Harness::activate(KINDS[0], local(Vec::new(), 0));
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 500, 537, None)));
    h.answer(&RawAnswer::new(410));
    let actions = h.answer(&head_naming(None, 537));
    assert_eq!(actions, vec![rebuild(None, 537)]);
    let actions = h.on(Event::CaughtUp {
        last: Position(4),
        log_bytes: 0,
        lineage: LINEAGE,
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
        lineage: LINEAGE,
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
        lineage: LINEAGE,
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
        lineage: LINEAGE,
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
        lineage: LINEAGE,
    });
    assert_eq!(sends(&actions).len(), 1);
    h.answer(&RawAnswer::new(410));
    let actions = h.answer(&head_naming(None, 2 * len + 37));
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
        salvage: false,
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
