//! Re-keys (design: History; extensions: Snapshot Metadata on HEAD and Stream Incarnation): a HEAD
//! that names another token whose history inherits the generation rolls the log to the new token
//! and claims under it, from streaming, during a continuation claim (the repeat rule, design:
//! History), during a takeover and during a provisional open's claim, and what the claim's
//! reconcile finds after it.

use ursula_headers::ClosedEpoch;
use ursula_headers::Delivery;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationMismatch;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Inherited;
use ursula_headers::ProducerAnswer;
use ursula_headers::ServerIdentity;
use ursula_headers::StreamHead;

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
use super::seq;
use super::token;
use crate::codec::frame;
use crate::ids::Activation;
use crate::ids::Position;
use crate::replicator::Action;
use crate::replicator::ClaimKind;
use crate::replicator::Event;
use crate::replicator::Replicator;
use crate::replicator::ResetKind;
use crate::replicator::answer::HeadCheck;
use crate::replicator::answer::HeadDecision;
use crate::replicator::answer::decide_head;
use crate::replicator::answer::inherits;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::RawAnswer;
use crate::wire::Request;

const FOREIGN: Activation = Activation([0xf0; 16]);
/// The claim frame's length.
const CLAIM: u64 = 37;

fn new_token() -> Incarnation {
    Incarnation::try_from((1 << 52) | 8).unwrap()
}

fn precondition() -> IncarnationPrecondition {
    IncarnationPrecondition {
        incarnation: new_token(),
    }
}

fn other_token() -> Incarnation {
    Incarnation::try_from((1 << 52) | 9).unwrap()
}

/// A HEAD's `200` naming `incarnation` with its tail at `tail`, begun from `inherited`.
fn head(incarnation: Incarnation, tail: u64, inherited: Option<(Incarnation, u64)>) -> RawAnswer {
    let mut raw = RawAnswer::new(200);
    ServerIdentity::current(cluster()).render(raw.emit());
    StreamHead::new(incarnation, off(tail), off(0), 32 << 20)
        .unwrap()
        .with_inherited(inherited.map(|(previous_incarnation, offset)| Inherited {
            previous_incarnation,
            offset: off(offset),
        }))
        .unwrap()
        .render(raw.emit());
    raw
}

/// A `412` naming `current`.
fn mismatch(current: Incarnation) -> RawAnswer {
    let mut raw = RawAnswer::new(IncarnationMismatch::STATUS);
    IncarnationMismatch {
        current: Some(current),
    }
    .render(raw.emit());
    raw
}

/// A receipt under the new token.
fn receipt_new(
    delivery: Delivery,
    e: u64,
    s: u64,
    start: u64,
    next: u64,
    closed: Option<ClosedEpoch>,
) -> RawAnswer {
    let mut answer = receipt(delivery, e, s, start, next, closed);
    if let ProducerAnswer::Receipt { incarnation, .. } = &mut answer {
        *incarnation = new_token();
    }
    producer(&answer)
}

fn rekey_of(actions: &[Action]) -> Option<(Incarnation, u64)> {
    actions.iter().find_map(|a| match a {
        Action::ReKey {
            incarnation,
            epoch,
            activation,
        } => {
            assert_eq!(*activation, ACTIVATION);
            Some((*incarnation, epoch.get()))
        }
        _ => None,
    })
}

/// The claim request among `actions`: its epoch and seq, with its body checked.
fn claim_sent(actions: &[Action]) -> Option<(u64, &Request)> {
    sends(actions).into_iter().find_map(|r| {
        let write = r.keys.producer.as_ref()?;
        (write.seq.get() == 0).then(|| {
            assert_eq!(
                &*r.body,
                frame::encode_claim(write.epoch, ACTIVATION).as_slice()
            );
            (write.epoch.get(), r)
        })
    })
}

fn acked(actions: &[Action]) -> Option<Position> {
    actions.iter().rev().find_map(|a| match a {
        Action::Wake { acked, .. } => Some(*acked),
        _ => None,
    })
}

/// Streaming at epoch 1 with records `1..=n` pending, its first batch in flight (claim at
/// `0..37`): the batch's body.
fn streaming(n: u64) -> (Harness, Vec<u8>) {
    let pending = (1..=n).map(|p| commit(LINEAGE, p, 1)).collect();
    let kind = ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    };
    let (mut h, _) = Harness::activate(kind, local(pending, 0));
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, CLAIM, None)));
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
    let batch = h.last_request().body.to_vec();
    (h, batch)
}

/// The in-flight append meets the new token, and the HEAD shows its history inherits everything
/// acknowledged (offset 37) with the stream's tail at `tail`: the re-key's actions.
fn rekey_from_streaming(h: &mut Harness, tail: u64) -> Vec<Action> {
    h.answer(&mismatch(new_token()));
    assert_eq!(h.last_request().method, Method::Head);
    h.answer(&head(new_token(), tail, Some((token(), tail))))
}

#[test]
fn inherits_needs_our_token_and_every_acknowledged_byte() {
    let head_of = |previous: Incarnation, offset: u64| {
        StreamHead::new(new_token(), off(100), off(0), 1 << 20)
            .unwrap()
            .with_inherited(Some(Inherited {
                previous_incarnation: previous,
                offset: off(offset),
            }))
            .unwrap()
    };
    assert!(inherits(&head_of(token(), 50), token(), off(50)));
    assert!(inherits(&head_of(token(), 50), token(), off(10)));
    assert!(!inherits(&head_of(token(), 49), token(), off(50)));
    assert!(!inherits(&head_of(other_token(), 50), token(), off(10)));
    let plain = StreamHead::new(new_token(), off(100), off(0), 1 << 20).unwrap();
    assert!(!inherits(&plain, token(), off(0)));
    // Our own token is never a history we inherit.
    assert!(!inherits(&head_of(token(), 50), new_token(), off(0)));
}

#[test]
fn decide_head_rows() {
    let request = Request::new(Method::Head, crate::wire::Target::Stream);
    let decide = |raw: &RawAnswer, previous: Option<Incarnation>| {
        let exchange = Exchange::Answered(Box::new(raw.parse(&request)));
        decide_head(&exchange, HeadCheck {
            cluster_id: &cluster(),
            incarnation: token(),
            previous,
            acked_offset: off(40),
        })
    };
    let max = 32 << 20;
    assert_eq!(decide(&head(token(), 40, None), None), HeadDecision::Same {
        max_request_bytes: max
    });
    assert_eq!(
        decide(&head(new_token(), 40, Some((token(), 40))), None),
        HeadDecision::ReKey {
            incarnation: new_token(),
            from: token(),
            max_request_bytes: max
        }
    );
    // Lossy, or begun from another token's history, or from none.
    assert_eq!(
        decide(&head(new_token(), 40, Some((token(), 39))), None),
        HeadDecision::Rewound
    );
    assert_eq!(
        decide(&head(new_token(), 40, Some((other_token(), 40))), None),
        HeadDecision::Rewound
    );
    assert_eq!(
        decide(&head(new_token(), 40, None), None),
        HeadDecision::Rewound
    );
    // A re-key whose claim has not landed still inherits from the token before it: two history
    // events with no write between them name it (extensions: Snapshot Metadata on HEAD and Stream
    // Incarnation).
    assert_eq!(
        decide(&head(other_token(), 40, Some((new_token(), 40))), None),
        HeadDecision::Rewound
    );
    assert_eq!(
        decide(
            &head(other_token(), 40, Some((new_token(), 40))),
            Some(new_token())
        ),
        HeadDecision::ReKey {
            incarnation: other_token(),
            from: new_token(),
            max_request_bytes: max
        }
    );
    let mut missing = RawAnswer::new(404);
    ServerIdentity::current(cluster()).render(missing.emit());
    assert_eq!(decide(&missing, None), HeadDecision::Deleted);
    let mut elsewhere = RawAnswer::new(200);
    ServerIdentity::current(ursula_headers::ClusterId::new("cluster-b").unwrap())
        .render(elsewhere.emit());
    StreamHead::new(token(), off(40), off(0), max)
        .unwrap()
        .render(elsewhere.emit());
    assert_eq!(decide(&elsewhere, None), HeadDecision::WrongCluster);
}

#[test]
fn a_rekey_from_streaming_acknowledges_a_batch_that_survived() {
    let (mut h, batch) = streaming(1);
    let len = u64::try_from(batch.len()).unwrap();
    let actions = rekey_from_streaming(&mut h, CLAIM + len);
    assert_eq!(rekey_of(&actions), Some((new_token(), 2)));
    // The CLAIM record is logged before the claim goes, and the claim names the new token.
    let rekey_at = actions
        .iter()
        .position(|a| matches!(a, Action::ReKey { .. }))
        .unwrap();
    let send_at = actions
        .iter()
        .position(|a| matches!(a, Action::Send { .. }))
        .unwrap();
    assert!(rekey_at < send_at);
    let (e, claim) = claim_sent(&actions).unwrap();
    assert_eq!(e, 2);
    assert_eq!(claim.keys.precondition, Some(precondition()));
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::PersistClaim { .. }))
    );
    let closed = ClosedEpoch {
        epoch: epoch(1),
        seq: seq(1),
    };
    let start = CLAIM + len;
    h.answer(&receipt_new(
        Delivery::Applied,
        2,
        0,
        start,
        start + CLAIM,
        Some(closed),
    ));
    let actions = h.answer(&read(&batch, start));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(acked(&actions), Some(Position(1)));
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
    assert_eq!(h.replicator.ctx().unwrap().rekey_from, None);
}

#[test]
fn a_rekey_from_streaming_resends_a_batch_that_was_lost_from_seq_1() {
    let (mut h, batch) = streaming(1);
    let actions = rekey_from_streaming(&mut h, CLAIM);
    assert_eq!(rekey_of(&actions), Some((new_token(), 2)));
    let closed = ClosedEpoch {
        epoch: epoch(1),
        seq: seq(0),
    };
    let actions = h.answer(&receipt_new(
        Delivery::Applied,
        2,
        0,
        CLAIM,
        2 * CLAIM,
        Some(closed),
    ));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    let resent = h.last_request();
    let write = resent.keys.producer.clone().unwrap();
    assert_eq!((write.epoch, write.seq), (epoch(2), seq(1)));
    assert_eq!(&*resent.body, batch.as_slice());
    assert_eq!(resent.keys.precondition, Some(precondition()));
}

#[test]
fn a_batch_above_the_closed_seq_after_a_rekey_resets_stream_state() {
    let (mut h, batch) = streaming(1);
    let len = u64::try_from(batch.len()).unwrap();
    rekey_from_streaming(&mut h, CLAIM + len);
    // The claim says it closed epoch 1 at seq 0, yet the batch of seq 1 is in the stream.
    let closed = ClosedEpoch {
        epoch: epoch(1),
        seq: seq(0),
    };
    let start = CLAIM + len;
    h.answer(&receipt_new(
        Delivery::Applied,
        2,
        0,
        start,
        start + CLAIM,
        Some(closed),
    ));
    let actions = h.answer(&read(&batch, start));
    assert_eq!(reset_of(&actions), Some(ResetKind::StreamState));
}

#[test]
fn a_foreign_claim_after_a_rekey_fences() {
    let (mut h, _) = streaming(1);
    let foreign = frame::encode_claim(epoch(5), FOREIGN);
    let flen = u64::try_from(foreign.len()).unwrap();
    // Another claimant's frame landed after the history event, before our claim.
    rekey_from_streaming(&mut h, CLAIM + flen);
    let start = CLAIM + flen;
    h.answer(&receipt_new(
        Delivery::Applied,
        2,
        0,
        start,
        start + CLAIM,
        None,
    ));
    let actions = h.answer(&read(&foreign, start));
    assert_eq!(reset_of(&actions), Some(ResetKind::Fenced), "{actions:?}");
}

#[test]
fn a_rekey_with_a_lossy_history_resets_rewound() {
    let (mut h, _) = streaming(1);
    h.answer(&mismatch(new_token()));
    let actions = h.answer(&head(new_token(), 10, Some((token(), 10))));
    assert_eq!(reset_of(&actions), Some(ResetKind::Rewound));
    assert_eq!(rekey_of(&actions), None);
}

/// Streaming at epoch 1 with two records; the append meets `producer-expired` and the
/// continuation claim of epoch 2 goes out, then meets the new token: the re-key's actions.
fn rekey_during_a_continuation_claim() -> (Harness, Vec<Action>) {
    let (mut h, _) = streaming(2);
    let actions = h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(1),
    }));
    assert_eq!(claim_sent(&actions).map(|(e, _)| e), Some(2));
    h.answer(&mismatch(new_token()));
    // Whether the claim landed before the history event or not, the history kept it all.
    let actions = h.answer(&head(new_token(), 2 * CLAIM, Some((token(), 2 * CLAIM))));
    (h, actions)
}

#[test]
fn a_rekey_during_a_continuation_claim_sends_the_same_claim_again() {
    let (mut h, actions) = rekey_during_a_continuation_claim();
    // Same epoch and body, under the new token.
    assert_eq!(rekey_of(&actions), Some((new_token(), 2)));
    let (e, claim) = claim_sent(&actions).unwrap();
    assert_eq!(e, 2);
    assert_eq!(claim.keys.precondition, Some(precondition()));
    assert!(matches!(h.replicator, Replicator::Activating {
        kind: ClaimKind::Continuation {
            own: _,
            resent: true
        },
        ..
    }));
    // Applied under the new token: it binds.
    let actions = h.answer(&receipt_new(
        Delivery::Applied,
        2,
        0,
        CLAIM,
        2 * CLAIM,
        None,
    ));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(actions.iter().any(|a| matches!(a, Action::Bound { .. })));
}

#[test]
fn a_repeat_after_a_rekey_claims_once_more() {
    let (mut h, _) = rekey_during_a_continuation_claim();
    // The claim applied before the history event: the record stays history-fenced.
    let actions = h.answer(&receipt_new(Delivery::Repeat, 2, 0, CLAIM, 2 * CLAIM, None));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(!actions.iter().any(|a| matches!(a, Action::Bound { .. })));
    assert!(actions.contains(&Action::PersistClaim {
        epoch: epoch(3),
        activation: ACTIVATION,
    }));
    assert_eq!(claim_sent(&actions).map(|(e, _)| e), Some(3));
    let actions = h.answer(&receipt_new(
        Delivery::Applied,
        3,
        0,
        2 * CLAIM,
        3 * CLAIM,
        None,
    ));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    // The reconcile skips our own claim of epoch 2, and binds.
    let own = frame::encode_claim(epoch(2), ACTIVATION);
    let actions = h.answer(&read(&own, 2 * CLAIM));
    assert!(
        actions.iter().any(|a| matches!(a, Action::Bound { .. })),
        "{actions:?}"
    );
}

#[test]
fn a_rekey_during_a_takeover_claims_above_every_epoch_seen() {
    let kind = ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    };
    let (mut h, _) = Harness::activate(kind, local(Vec::new(), 0));
    h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(40),
    }));
    h.answer(&mismatch(new_token()));
    let actions = h.answer(&head(new_token(), 0, Some((token(), 0))));
    let (incarnation, e) = rekey_of(&actions).unwrap();
    assert_eq!(incarnation, new_token());
    assert!(e > 40, "{e}");
    assert!(matches!(h.replicator, Replicator::Activating {
        kind: ClaimKind::Takeover { rekeyed: true, .. },
        ..
    }));
}

#[test]
fn a_takeover_at_open_rekeys_before_it_claims() {
    let mut state = local(vec![commit(LINEAGE, 1, 1)], 0);
    state.incarnation = new_token();
    state.rekey_from = Some(token());
    let kind = ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    };
    let (h, actions) = Harness::activate(kind, state);
    assert_eq!(rekey_of(&actions), Some((new_token(), 1)));
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::PersistClaim { .. }))
    );
    assert_eq!(
        claim_sent(&actions).unwrap().1.keys.precondition,
        Some(precondition())
    );
    assert!(matches!(h.replicator, Replicator::Activating {
        kind: ClaimKind::Takeover { rekeyed: true, .. },
        ..
    }));
}

#[test]
fn tickets_wait_for_the_bind_after_a_rekey() {
    let (mut h, batch) = streaming(1);
    let len = u64::try_from(batch.len()).unwrap();
    rekey_from_streaming(&mut h, CLAIM + len);
    let actions = h.on(Event::Sync {
        ticket: 1,
        target: Position(1),
    });
    assert!(!actions.contains(&Action::Synced { ticket: 1 }));
    let closed = ClosedEpoch {
        epoch: epoch(1),
        seq: seq(1),
    };
    let start = CLAIM + len;
    let actions = h.answer(&receipt_new(
        Delivery::Applied,
        2,
        0,
        start,
        start + CLAIM,
        Some(closed),
    ));
    assert!(!actions.contains(&Action::Synced { ticket: 1 }));
    let mut actions = h.answer(&read(&batch, start));
    // The reconcile reads on to the claim's end.
    actions.extend(h.answer(&read(&[], start + CLAIM)));
    assert!(
        actions.contains(&Action::Synced { ticket: 1 }),
        "{actions:?}"
    );
}

fn t(n: u64) -> Incarnation {
    Incarnation::try_from((1 << 52) | n).unwrap()
}

/// A receipt under `token`.
fn receipt_under(
    token: Incarnation,
    delivery: Delivery,
    e: u64,
    s: u64,
    start: u64,
    next: u64,
    closed: Option<ClosedEpoch>,
) -> RawAnswer {
    let mut answer = receipt(delivery, e, s, start, next, closed);
    if let ProducerAnswer::Receipt { incarnation, .. } = &mut answer {
        *incarnation = token;
    }
    producer(&answer)
}

/// Loses the last request's answer and ticks until it goes again.
fn lose_and_resend(h: &mut Harness) -> Vec<Action> {
    h.lose();
    let at = h.now_ms + 120_000;
    h.tick_at(at)
}

/// A takeover re-keyed at open (T0 to T1) with nothing pending.
fn rekeyed_takeover(pending: Vec<crate::replicator::Pending>) -> (Harness, u64) {
    let mut state = local(pending, 0);
    state.incarnation = t(8);
    state.rekey_from = Some(token());
    let kind = ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    };
    let (h, actions) = Harness::activate(kind, state);
    let e = rekey_of(&actions).unwrap().1;
    (h, e)
}

/// Bound under T0; event 1 (T1) re-keys and claims epoch 2, which applies under T1 with its
/// answer lost. Event 2 (T2) names T1: the generation re-keys from T1 and resends epoch 2, a
/// repeat, so the repeat rule (design: History) claims epoch 3, which is lost before it applies.
/// Event 3 (T3) still names T1, the generation's own token with every acknowledged byte kept: it
/// re-keys, never resets `rewound`, after a pre-check, since claim 2 was never streamed in.
#[test]
fn a_third_event_after_an_unknown_claim_outcome_re_keys_from_the_token_it_names() {
    let (mut h, batch) = streaming(1);
    let tail1 = CLAIM + u64::try_from(batch.len()).unwrap();
    h.answer(&mismatch(t(8)));
    let actions = h.answer(&head(t(8), tail1, Some((token(), tail1))));
    assert_eq!(rekey_of(&actions), Some((t(8), 2)));
    assert_eq!(h.replicator.ctx().unwrap().rekey_from, Some(token()));
    let actions = lose_and_resend(&mut h);
    assert_eq!(claim_sent(&actions).map(|(e, _)| e), Some(2));
    let tail2 = tail1 + CLAIM;
    h.answer(&mismatch(t(9)));
    let actions = h.answer(&head(t(9), tail2, Some((t(8), tail2))));
    assert_eq!(rekey_of(&actions), Some((t(9), 2)), "{actions:?}");
    assert_eq!(h.replicator.ctx().unwrap().rekey_from, Some(t(8)));
    let closed = ClosedEpoch {
        epoch: epoch(1),
        seq: seq(1),
    };
    let actions = h.answer(&receipt_under(
        t(9),
        Delivery::Repeat,
        2,
        0,
        tail1,
        tail2,
        Some(closed),
    ));
    assert_eq!(claim_sent(&actions).map(|(e, _)| e), Some(3), "{actions:?}");
    assert_eq!(h.replicator.ctx().unwrap().rekey_from, Some(t(8)));
    let actions = lose_and_resend(&mut h);
    assert_eq!(claim_sent(&actions).map(|(e, _)| e), Some(3));
    h.answer(&mismatch(t(10)));
    let actions = h.answer(&head(t(10), tail2, Some((t(8), tail2))));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    // Claim 2 was never streamed in, and its frame lies past the acknowledged offset: the event
    // may have lost it, so the re-key pre-checks under T3 first (design: Pre-check).
    assert_eq!(rekey_of(&actions), None, "{actions:?}");
    assert_eq!(h.last_request().method, Method::Head);
    let actions = h.answer(&head(t(10), tail2, Some((t(8), tail2))));
    assert_eq!(h.last_request().method, Method::Get, "{actions:?}");
    let own = frame::encode_claim(epoch(2), ACTIVATION);
    // The reads find the batch that applied with claim 2, then claim 2.
    let actions = h.answer(&read(&[batch.clone(), own].concat(), tail2));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(rekey_of(&actions), Some((t(10), 3)));
}

/// A takeover re-keyed at open whose claim applied under T1, answered by a repeat after a resend,
/// then a restore of a T0 checkpoint during its reconcile: the stream was last written under T1,
/// so the history does not inherit from T0, whether the claim came back applied or a repeat.
#[test]
fn a_claim_receipt_applied_or_repeat_ends_the_trust_in_the_token_before_the_rekey() {
    for repeat in [false, true] {
        let (mut h, e) = rekeyed_takeover(vec![commit(LINEAGE, 1, 1)]);
        let delivery = if repeat {
            lose_and_resend(&mut h);
            Delivery::Repeat
        } else {
            Delivery::Applied
        };
        h.answer(&receipt_under(t(8), delivery, e, 0, 100, 137, None));
        assert_eq!(h.replicator.ctx().unwrap().rekey_from, None);
        assert_eq!(h.last_request().method, Method::Get);
        h.answer(&mismatch(t(9)));
        let actions = h.answer(&head(t(9), 50, Some((token(), 50))));
        assert_eq!(rekey_of(&actions), None, "repeat {repeat}: {actions:?}");
        assert_eq!(reset_of(&actions), Some(ResetKind::Rewound));
    }
}

/// The harm the rule above prevents: after a repeat, the reconcile catches up a commit written
/// under T1 in place, moving the acknowledged offset over T1-only bytes. A restore of a T0
/// checkpoint above that offset must not re-key the generation onto bytes that are not that
/// commit.
#[test]
fn a_restore_of_the_token_before_a_rekey_never_re_keys_over_a_caught_up_commit() {
    let (mut h, e) = rekeyed_takeover(Vec::new());
    lose_and_resend(&mut h);
    h.answer(&receipt_under(t(8), Delivery::Repeat, e, 0, 200, 237, None));
    assert_eq!(h.last_request().method, Method::Get);
    let foreign = commit(crate::ids::Lineage([0x22; 16]), 1, 9).frame;
    let n = u64::try_from(foreign.len()).unwrap();
    let actions = h.answer(&read(&foreign, n));
    assert!(
        actions.iter().any(|a| matches!(a, Action::Apply(_))),
        "{actions:?}"
    );
    h.on(Event::CaughtUp {
        last: Position(1),
        log_bytes: n,
        lineage: LINEAGE,
    });
    assert_eq!(h.replicator.ctx().unwrap().acked_offset, off(n));
    h.answer(&mismatch(t(9)));
    let actions = h.answer(&head(t(9), n + 10, Some((token(), n + 10))));
    assert_eq!(rekey_of(&actions), None, "{actions:?}");
}

/// A takeover with commits pending meets an inheriting history: it resets `rewound` before its
/// log rolls, so the next open finds the old token and salvages the generation.
#[test]
fn a_takeover_with_commits_pending_resets_before_it_re_keys() {
    let kind = ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    };
    let (mut h, _) = Harness::activate(kind, local(vec![commit(LINEAGE, 1, 1)], 0));
    h.answer(&mismatch(new_token()));
    let actions = h.answer(&head(new_token(), 0, Some((token(), 0))));
    assert_eq!(reset_of(&actions), Some(ResetKind::Rewound));
    assert_eq!(rekey_of(&actions), None);
}

/// The repeat rule's follow-up claim (design: History) closes only the repeated claim's epoch, so
/// its reconcile checks the epoch the binding streamed in against what the repeat said it closed: a
/// batch found above that seq resets `stream_state`.
#[test]
fn a_follow_up_after_a_repeat_cross_checks_the_streamed_epoch() {
    let (mut h, batch) = streaming(2);
    let len = u64::try_from(batch.len()).unwrap();
    h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(1),
    }));
    h.answer(&mismatch(new_token()));
    let tail = CLAIM + len + CLAIM;
    let actions = h.answer(&head(new_token(), tail, Some((token(), tail))));
    assert_eq!(rekey_of(&actions), Some((new_token(), 2)));
    // The repeat says the claim closed epoch 1 at seq 0, yet the batch of seq 1 is in the stream.
    let closed = ClosedEpoch {
        epoch: epoch(1),
        seq: seq(0),
    };
    let actions = h.answer(&receipt_new(
        Delivery::Repeat,
        2,
        0,
        CLAIM + len,
        tail,
        Some(closed),
    ));
    assert_eq!(claim_sent(&actions).map(|(e, _)| e), Some(3));
    let follow_up = ClosedEpoch {
        epoch: epoch(2),
        seq: seq(0),
    };
    h.answer(&receipt_new(
        Delivery::Applied,
        3,
        0,
        tail,
        tail + CLAIM,
        Some(follow_up),
    ));
    let actions = h.answer(&read(&batch, CLAIM + len));
    assert_eq!(
        reset_of(&actions),
        Some(ResetKind::StreamState),
        "{actions:?}"
    );
}

/// A `416` answers a read past the tail, which cannot happen under our token (H1): the history
/// changed, so it checks HEAD like a `412`.
#[test]
fn a_416_on_a_read_sends_head() {
    let request = Request::read(Some(off(40)), true, 1 << 20);
    let exchange = Exchange::Answered(Box::new(RawAnswer::new(416).parse(&request)));
    assert_eq!(
        crate::replicator::answer::decide_read(&exchange),
        crate::replicator::answer::ReadDecision::Common(crate::replicator::answer::Common::Head(
            crate::replicator::answer::HeadReason::Precondition
        ))
    );
}

/// A takeover re-keyed at open whose reconcile finds a foreign commit where its pending commit
/// should be discards the pending tail only with a salvage of the generation (design: History): the
/// open re-keys only with nothing pending, so this is the defence behind that gate. A takeover
/// that did not re-key rebuilds with no salvage.
#[test]
fn a_rebuild_that_discards_after_a_rekey_at_open_salvages() {
    let foreign = commit(crate::ids::Lineage([0x22; 16]), 1, 9).frame;
    let n = u64::try_from(foreign.len()).unwrap();
    let rebuild = |actions: &[Action]| {
        actions.iter().find_map(|a| match a {
            Action::Rebuild {
                discard_from,
                salvage,
                ..
            } => Some((*discard_from, *salvage)),
            _ => None,
        })
    };
    let (mut h, e) = rekeyed_takeover(vec![commit(LINEAGE, 1, 1)]);
    h.answer(&receipt_under(
        t(8),
        Delivery::Applied,
        e,
        0,
        n,
        n + 37,
        None,
    ));
    let actions = h.answer(&read(&foreign, n));
    assert_eq!(
        rebuild(&actions),
        Some((Some(Position(1)), true)),
        "{actions:?}"
    );

    let kind = ClaimKind::Takeover {
        round: 1,
        rekeyed: false,
    };
    let (mut h, actions) = Harness::activate(kind, local(vec![commit(LINEAGE, 1, 1)], 0));
    let e = claim_sent(&actions).unwrap().0;
    h.answer(&producer(&receipt(
        Delivery::Applied,
        e,
        0,
        n,
        n + 37,
        None,
    )));
    let actions = h.answer(&read(&foreign, n));
    assert_eq!(
        rebuild(&actions),
        Some((Some(Position(1)), false)),
        "{actions:?}"
    );
}
/// The activation of the open that logged a provisional open's claim.
const PRIOR: Activation = Activation([0x5a; 16]);

/// A provisional open (design: Open) of a generation whose log's latest claim is `(3, PRIOR)`,
/// acknowledged up to the claim at `0..37`, with `pending`: its pre-check's HEAD goes out first.
fn provisional(pending: Vec<crate::replicator::Pending>) -> Harness {
    let mut state = local(pending, CLAIM);
    state.claims = vec![(epoch(3), PRIOR)];
    let (h, actions) = Harness::provisional(state, (epoch(3), PRIOR));
    assert_eq!(sends(&actions).len(), 1);
    assert_eq!(sends(&actions)[0].method, Method::Head);
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::PersistClaim { .. } | Action::ReKey { .. })),
        "nothing is logged before the stream answers: {actions:?}"
    );
    h
}

/// The claim among `actions`: its epoch and the activation its body names.
fn claim_of(actions: &[Action]) -> Option<(u64, Activation)> {
    sends(actions).into_iter().find_map(|r| {
        let write = r.keys.producer.as_ref()?;
        if write.seq.get() != 0 {
            return None;
        }
        match frame::decode(&r.body, u64::MAX) {
            Ok(frame::Decoded::Frame {
                frame: frame::Frame::Claim { epoch, activation },
                ..
            }) => Some((epoch.get(), activation)),
            _ => None,
        }
    })
}

/// The pre-check finds nothing foreign up to the tail: the log's latest claim goes out again,
/// same body, its record already logged. A repeat means it applied before: one continuation claim
/// of the next epoch follows, logged first, and binds.
#[test]
fn a_provisional_open_sends_its_logs_claim_again_after_the_pre_check() {
    let mut h = provisional(Vec::new());
    let own = frame::encode_claim(epoch(3), PRIOR);
    let actions = h.answer(&head(token(), 2 * CLAIM, None));
    assert_eq!(h.last_request().method, Method::Get, "{actions:?}");
    let actions = h.answer(&read(&own, 2 * CLAIM));
    assert_eq!(claim_of(&actions), Some((3, PRIOR)), "{actions:?}");
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::PersistClaim { .. })),
        "{actions:?}"
    );
    let actions = h.answer(&producer(&receipt(
        Delivery::Repeat,
        3,
        0,
        CLAIM,
        2 * CLAIM,
        None,
    )));
    assert!(actions.contains(&Action::PersistClaim {
        epoch: epoch(4),
        activation: ACTIVATION,
    }));
    assert_eq!(claim_of(&actions), Some((4, ACTIVATION)));
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        4,
        0,
        2 * CLAIM,
        3 * CLAIM,
        None,
    )));
    let actions = if actions.iter().any(|a| matches!(a, Action::Bound { .. })) {
        actions
    } else {
        h.answer(&read(&own, 2 * CLAIM))
    };
    assert!(
        actions.iter().any(|a| matches!(a, Action::Bound { .. })),
        "{actions:?}"
    );
}

/// `receipt-evicted` for the claim sent again: its digest is ours, and later writes under it
/// evicted its receipt, so it applied before and the next epoch follows. `producer-expired` naming
/// its epoch, the pre-check without its frame, proves nothing about whose record retired: the
/// binding resets `stale`, and a reopen takes over.
#[test]
fn a_provisional_claim_meeting_an_evicted_receipt_claims_the_next_epoch_and_expiry_goes_stale() {
    let mut h = provisional(Vec::new());
    h.answer(&head(token(), CLAIM, None));
    let actions = h.answer(&producer(&ProducerAnswer::ReceiptEvicted {
        epoch: ursula_headers::CurrentEpoch::new(epoch(3)),
        seq: seq(5),
        incarnation: token(),
    }));
    assert_eq!(claim_of(&actions), Some((4, ACTIVATION)), "{actions:?}");
    let mut h = provisional(Vec::new());
    h.answer(&head(token(), CLAIM, None));
    let actions = h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(3),
    }));
    assert_eq!(reset_of(&actions), Some(ResetKind::Stale), "{actions:?}");
}

/// The pre-check finds another owner's claim after the log's acknowledged offset: it resets
/// `fenced` with nothing sent. One followed by a claim of its own is superseded (design: Claims),
/// and a foreign commit resets `stale`. A `410` sends a HEAD first (a read of another history gets
/// its `410` before its `412`): our token still there, retention passed the acknowledged offset,
/// `stale`. A new token that inherits the generation pre-checks again under it, from its HEAD.
#[test]
fn a_provisional_pre_check_resets_on_a_foreign_frame_with_nothing_sent() {
    let foreign = frame::encode_claim(epoch(4), FOREIGN);
    let own = frame::encode_claim(epoch(3), PRIOR);
    let commit_frame = commit(crate::ids::Lineage([0x22; 16]), 1, 9).frame;
    let cases: [(Vec<u8>, Option<ResetKind>); 3] = [
        (foreign.clone(), Some(ResetKind::Fenced)),
        ([foreign.clone(), own].concat(), None),
        (commit_frame.to_vec(), Some(ResetKind::Stale)),
    ];
    for (body, reset) in cases {
        let mut h = provisional(Vec::new());
        let tail = CLAIM + u64::try_from(body.len()).unwrap();
        h.answer(&head(token(), tail, None));
        let actions = h.answer(&read(&body, tail));
        assert_eq!(reset_of(&actions), reset, "{actions:?}");
        assert_eq!(claim_of(&actions).is_some(), reset.is_none(), "{actions:?}");
    }
    let mut h = provisional(Vec::new());
    h.answer(&head(token(), 2 * CLAIM, None));
    let actions = h.answer(&RawAnswer::new(410));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(h.last_request().method, Method::Head);
    let actions = h.answer(&head(token(), 2 * CLAIM, None));
    assert_eq!(reset_of(&actions), Some(ResetKind::Stale), "{actions:?}");
    let mut h = provisional(Vec::new());
    h.answer(&head(token(), 2 * CLAIM, None));
    h.answer(&RawAnswer::new(410));
    let actions = h.answer(&head(new_token(), CLAIM, Some((token(), CLAIM))));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(h.last_request().method, Method::Head, "{actions:?}");
    let actions = h.answer(&head(new_token(), CLAIM, Some((token(), CLAIM))));
    assert_eq!(claim_of(&actions), Some((3, PRIOR)), "{actions:?}");
}

/// The pre-check's HEAD names an inheriting history: the claim goes out again under the new
/// token once the reads find nothing foreign, the log rolling first. Applied afresh under it, the
/// epoch may carry writes of ours in the history before, so one more claim follows.
#[test]
fn a_provisional_pre_check_re_keys_and_claims_once_more_after_its_claim() {
    let mut h = provisional(Vec::new());
    let actions = h.answer(&head(new_token(), CLAIM, Some((token(), CLAIM))));
    assert_eq!(
        actions.iter().find_map(|a| match a {
            Action::ReKey {
                incarnation,
                epoch,
                activation,
            } => Some((*incarnation, epoch.get(), *activation)),
            _ => None,
        }),
        Some((new_token(), 3, PRIOR)),
        "{actions:?}"
    );
    assert_eq!(claim_of(&actions), Some((3, PRIOR)));
    assert_eq!(h.last_request().keys.precondition, Some(precondition()));
    let actions = h.answer(&receipt_new(
        Delivery::Applied,
        3,
        0,
        CLAIM,
        2 * CLAIM,
        None,
    ));
    assert_eq!(claim_of(&actions), Some((4, ACTIVATION)), "{actions:?}");
}

/// The history gate (design: History) holds for a provisional open as for a takeover: its log's ACK
/// may trail what the stream acknowledged of the commits after it, so an inheriting history resets
/// `rewound` before anything is logged or sent.
#[test]
fn a_provisional_pre_check_with_commits_after_its_ack_resets_before_it_re_keys() {
    let mut h = provisional(vec![commit(LINEAGE, 1, 1)]);
    let actions = h.answer(&head(new_token(), CLAIM, Some((token(), CLAIM))));
    assert_eq!(reset_of(&actions), Some(ResetKind::Rewound));
    assert!(sends(&actions).is_empty(), "{actions:?}");
}

/// A history event while the provisional claim is out: before its bind the binding acknowledged
/// nothing the new history must keep, so it pre-checks again under the new token before its claim
/// goes out again.
#[test]
fn a_re_key_before_a_provisional_bind_pre_checks_again() {
    let mut h = provisional(Vec::new());
    h.answer(&head(token(), CLAIM, None));
    assert_eq!(h.last_request().method, Method::Post);
    h.answer(&mismatch(new_token()));
    let actions = h.answer(&head(new_token(), CLAIM, Some((token(), CLAIM))));
    assert!(claim_of(&actions).is_none(), "{actions:?}");
    assert_eq!(h.last_request().method, Method::Head, "{actions:?}");
    let actions = h.answer(&head(new_token(), CLAIM, Some((token(), CLAIM))));
    assert!(
        actions.iter().any(|a| matches!(a, Action::ReKey { .. })),
        "{actions:?}"
    );
    assert_eq!(claim_of(&actions), Some((3, PRIOR)));
}

/// Once the provisional claim applied and its reconcile completed, the acknowledged state is the
/// stream's: a later history event while streaming re-keys, whatever the log held at the open.
#[test]
fn a_provisional_binding_that_went_live_re_keys_while_streaming() {
    let pending = commit(LINEAGE, 1, 1);
    let len = u64::try_from(pending.frame.len()).unwrap();
    let mut h = provisional(vec![pending]);
    h.answer(&head(token(), CLAIM, None));
    h.answer(&producer(&receipt(
        Delivery::Applied,
        3,
        0,
        CLAIM,
        2 * CLAIM,
        None,
    )));
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        4,
        0,
        2 * CLAIM,
        3 * CLAIM,
        None,
    )));
    let own = frame::encode_claim(epoch(3), PRIOR);
    let actions = if actions.iter().any(|a| matches!(a, Action::Bound { .. })) {
        actions
    } else {
        h.answer(&read(&own, 2 * CLAIM))
    };
    assert!(
        actions.iter().any(|a| matches!(a, Action::Bound { .. })),
        "{actions:?}"
    );
    let tail = 3 * CLAIM + len;
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        4,
        1,
        3 * CLAIM,
        tail,
        None,
    )));
    assert_eq!(acked(&actions), Some(Position(1)));
    h.on(Event::Logged(commit(LINEAGE, 2, 1)));
    h.answer(&mismatch(new_token()));
    let actions = h.answer(&head(new_token(), tail, Some((token(), tail))));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(rekey_of(&actions), Some((new_token(), 5)));
}

/// The pre-check found the claim's own frame: it applied, so `producer-expired` naming its epoch
/// is of its own retired record, and the next epoch follows (a continuation after idle expiry).
#[test]
fn a_provisional_claim_found_in_the_pre_check_claims_on_after_idle_expiry() {
    let mut h = provisional(Vec::new());
    let own = frame::encode_claim(epoch(3), PRIOR);
    h.answer(&head(token(), 2 * CLAIM, None));
    h.answer(&read(&own, 2 * CLAIM));
    let actions = h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(3),
    }));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(claim_of(&actions), Some((4, ACTIVATION)));
}

/// A restore while the owner streams idle: the `synced()` probe's read under the old token gets
/// the `410` (it comes before the `412`), and the HEAD it sends takes the history path. A history
/// that keeps every byte acknowledged re-keys, one that lost some resets `rewound`, never
/// `fenced`.
#[test]
fn a_410_on_a_probe_after_a_history_event_takes_the_history_path() {
    for (inherited, rewound) in [(CLAIM, false), (0, true)] {
        let mut h = Harness::activate(
            ClaimKind::Takeover {
                round: 1,
                rekeyed: false,
            },
            local(Vec::new(), 0),
        )
        .0;
        h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, CLAIM, None)));
        h.on(Event::Sync {
            ticket: 1,
            target: Position(0),
        });
        assert_eq!(h.last_request().method, Method::Get);
        let actions = h.answer(&RawAnswer::new(410));
        assert_eq!(reset_of(&actions), None, "{actions:?}");
        assert_eq!(h.last_request().method, Method::Head);
        let actions = h.answer(&head(new_token(), CLAIM, Some((token(), inherited))));
        if rewound {
            assert_eq!(reset_of(&actions), Some(ResetKind::Rewound), "{actions:?}");
        } else {
            assert_eq!(reset_of(&actions), None, "{actions:?}");
            assert_eq!(rekey_of(&actions), Some((new_token(), 2)), "{actions:?}");
        }
    }
}
