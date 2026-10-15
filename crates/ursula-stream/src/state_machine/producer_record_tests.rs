//! The producer record (#581 section 2.3), its F3 bounds and `TidyStream`
//! (`docs/architecture/bounded-stream-state.md` F0, F3).
//!
//! One test per row of the evaluation table (section 2.3.1), through the
//! production entry point `StreamStateMachine::apply`, then the epoch
//! floor, write identity, the early answers of section 2.5 and a proptest
//! of the record's transitions.

use bytes::Bytes;
use proptest::prelude::*;

use super::producers::PRODUCER_IDLE_EXPIRY_MS;
use super::*;

const OCTET: &str = "application/octet-stream";
const BUCKET: &str = "window";

fn fresh_machine() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: BUCKET.to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    machine
}

fn sid(name: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, name)
}

fn create(machine: &mut StreamStateMachine, name: &str, content_type: &str) -> BucketStreamId {
    let stream_id = sid(name);
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: content_type.to_owned(),
            initial_payload: Bytes::new(),
            close_after: false,
            stream_seq: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
        }),
        StreamResponse::Created { .. }
    ));
    stream_id
}

/// One producer write. `digest: Some(d)` sends `Producer-Digest` for the
/// body `[d; 4]`; `None` sends no digest with the body `[0; 4]`.
#[derive(Debug, Clone, Copy)]
struct Write<'a> {
    id: &'a str,
    epoch: u64,
    seq: u64,
    digest: Option<u8>,
    tokened: bool,
    now_ms: u64,
}

impl<'a> Write<'a> {
    fn new(id: &'a str, epoch: u64, seq: u64) -> Self {
        Self {
            id,
            epoch,
            seq,
            digest: None,
            tokened: false,
            now_ms: 1,
        }
    }

    fn digest(self, digest: u8) -> Self {
        Self {
            digest: Some(digest),
            ..self
        }
    }

    fn tokened(self) -> Self {
        Self {
            tokened: true,
            ..self
        }
    }

    fn at(self, now_ms: u64) -> Self {
        Self { now_ms, ..self }
    }

    fn producer(&self) -> ProducerRequest {
        ProducerRequest {
            producer_id: self.id.to_owned(),
            producer_epoch: self.epoch,
            producer_seq: self.seq,
            digest: self
                .digest
                .map(|digest| Bytes::copy_from_slice(&[digest; 32])),
            ..Default::default()
        }
    }

    fn body(&self) -> Bytes {
        Bytes::copy_from_slice(&[self.digest.unwrap_or(0); 4])
    }
}

/// Applies `write` as an append, wrapped in the stream's current
/// `Stream-Incarnation` when it is tokened.
fn send(
    machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
    write: Write<'_>,
) -> StreamResponse {
    let command = StreamCommand::Append {
        stream_id: stream_id.clone(),
        content_type: Some(OCTET.to_owned()),
        payload: write.body(),
        close_after: false,
        stream_seq: None,
        producer: Some(write.producer()),
        now_ms: write.now_ms,
    };
    let command = if write.tokened {
        StreamCommand::IfIncarnation {
            incarnation: machine.stream_incarnation(stream_id).unwrap(),
            command: Box::new(command),
        }
    } else {
        command
    };
    machine.apply(command)
}

fn ack(response: StreamResponse) -> ProducerAck {
    match response {
        StreamResponse::Appended {
            producer: Some(ack),
            ..
        }
        | StreamResponse::Closed {
            producer: Some(ack),
            ..
        } => ack,
        other => panic!("expected a producer ack, got {other:?}"),
    }
}

fn applied(response: StreamResponse) -> ProducerAck {
    let ack = ack(response);
    assert!(
        matches!(ack.delivery, ProducerDelivery::Applied(_)),
        "{ack:?}"
    );
    ack
}

fn refused(response: StreamResponse) -> (StreamErrorCode, Vec<StreamErrorContext>) {
    match response {
        StreamResponse::Error { code, context, .. } => (code, context),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn producer_refusal(response: StreamResponse) -> (StreamErrorCode, ProducerRefusal) {
    let (code, context) = refused(response);
    match context.as_slice() {
        [StreamErrorContext::Producer(refusal)] => (code, *refusal),
        other => panic!("expected a producer refusal, got {code:?} {other:?}"),
    }
}

fn tail(machine: &StreamStateMachine, stream_id: &BucketStreamId) -> u64 {
    machine.head(stream_id).unwrap().tail_offset
}

fn record(
    machine: &StreamStateMachine,
    stream_id: &BucketStreamId,
    id: &str,
) -> Option<ProducerState> {
    machine.stream_slot(stream_id)?.producers.get(id).cloned()
}

fn floor(machine: &StreamStateMachine, stream_id: &BucketStreamId) -> Option<u64> {
    machine.stream_slot(stream_id)?.retired_epoch_watermark
}

fn producer_snapshot(machine: &StreamStateMachine) -> Vec<Vec<ProducerSnapshot>> {
    machine
        .snapshot()
        .streams
        .into_iter()
        .map(|entry| entry.producer_states)
        .collect()
}

/// A stream whose producer `p` holds epoch 3 after epochs 1 and 2: epoch 1
/// applied seqs 0 to 2, epoch 2 applied seqs 0 to 1, epoch 3 seqs 0 to 1.
fn three_epochs(machine: &mut StreamStateMachine) -> BucketStreamId {
    let stream_id = create(machine, "epochs", OCTET);
    for (epoch, last) in [(1, 2), (2, 1), (3, 1)] {
        for seq in 0..=last {
            applied(send(machine, &stream_id, Write::new("p", epoch, seq)));
        }
    }
    stream_id
}

#[test]
fn row_1_a_tokened_write_at_or_below_the_floor_is_expired() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "floor", OCTET);
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 3, 0).tokened(),
    ));
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 3, 1).tokened(),
    ));
    let idle = 1 + PRODUCER_IDLE_EXPIRY_MS;
    let before = tail(&machine, &stream_id);

    // The zombie's own continuation claim retires the idle record, which
    // raises the floor to 3, and is refused with the floor.
    for epoch in [3, 2] {
        let response = send(
            &mut machine,
            &stream_id,
            Write::new("p", epoch, 0).tokened().at(idle),
        );
        assert_eq!(
            producer_refusal(response),
            (StreamErrorCode::ProducerExpired, ProducerRefusal::Expired {
                epoch: 3
            })
        );
    }
    // A tokened append at the floor is final too.
    let response = send(
        &mut machine,
        &stream_id,
        Write::new("p", 3, 2).tokened().at(idle),
    );
    assert_eq!(
        producer_refusal(response),
        (StreamErrorCode::ProducerExpired, ProducerRefusal::Expired {
            epoch: 3
        })
    );
    assert_eq!(tail(&machine, &stream_id), before);
    assert!(record(&machine, &stream_id, "p").is_none());
    assert_eq!(floor(&machine, &stream_id), Some(3));

    // The floor binds tokened writes of any producer the stream holds no
    // record for.
    assert_eq!(
        producer_refusal(send(
            &mut machine,
            &stream_id,
            Write::new("q", 0, 0).tokened().at(idle)
        )),
        (StreamErrorCode::ProducerExpired, ProducerRefusal::Expired {
            epoch: 3
        })
    );
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("q", 4, 0).tokened().at(idle),
    ));
    // Without the token the base protocol's rule holds: a forgotten
    // producer is new, at any epoch.
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("r", 0, 0).at(idle),
    ));
}

#[test]
fn a_live_record_claims_past_a_floor_other_producers_raised() {
    // `a` holds epoch 3 while `b`'s epoch 10 retires: the floor is 10. `a`'s
    // record opened above the floor of its time, so its claim of epoch 4
    // gets its closure back, tokened or not, instead of `producer-expired`.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "record-floor", OCTET);
    applied(send(&mut machine, &stream_id, Write::new("b", 10, 0)));
    let idle = 1 + PRODUCER_IDLE_EXPIRY_MS;
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("a", 3, 0).tokened().at(idle),
    ));
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("a", 3, 1).tokened().at(idle),
    ));
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: idle,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    assert_eq!(floor(&machine, &stream_id), Some(10));
    // Row 7 answers as the base protocol does.
    assert_eq!(
        producer_refusal(send(
            &mut machine,
            &stream_id,
            Write::new("a", 4, 1).tokened().at(idle)
        )),
        (
            StreamErrorCode::InvalidProducer,
            ProducerRefusal::EpochNotClaimed
        )
    );
    let claim = applied(send(
        &mut machine,
        &stream_id,
        Write::new("a", 4, 0).tokened().at(idle),
    ));
    assert_eq!(claim.closed_epoch, Some(ClosedEpoch { epoch: 3, seq: 1 }));
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("a", 4, 1).tokened().at(idle),
    ));
    assert_eq!(
        record(&machine, &stream_id, "a").unwrap().opened,
        Opened::AboveFloor
    );
}

#[test]
fn a_record_opened_below_the_floor_refuses_tokened_writes_until_it_claims_above_and_closes_nothing()
{
    // `p` at epoch 3 retires: the floor is 3. Without the token, the base
    // protocol treats `p` as new, and its claim of the forgotten epoch 3
    // opens a record below the floor. A tokened write on that record is
    // `producer-expired`, a claim at or below the floor too, until a claim
    // above the floor marks it above.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "marker", OCTET);
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 3, 0).tokened(),
    ));
    let idle = 1 + PRODUCER_IDLE_EXPIRY_MS;
    let reclaim = applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 3, 0).at(idle),
    ));
    assert_eq!(reclaim.closed_epoch, None);
    assert_eq!(floor(&machine, &stream_id), Some(3));
    assert_eq!(
        record(&machine, &stream_id, "p").unwrap().opened,
        Opened::BelowFloor
    );
    let expired = (StreamErrorCode::ProducerExpired, ProducerRefusal::Expired {
        epoch: 3,
    });
    for write in [Write::new("p", 3, 1), Write::new("p", 3, 0)] {
        assert_eq!(
            producer_refusal(send(&mut machine, &stream_id, write.tokened().at(idle))),
            expired,
            "{write:?}"
        );
    }
    // Untokened, the base rules still hold on it.
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 3, 1).at(idle),
    ));
    let claim = applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 4, 0).tokened().at(idle),
    ));
    // The lifting claim closes nothing: epoch 3 may be the forgotten one,
    // and its seqs may be another writer's.
    assert_eq!(claim.closed_epoch, None);
    assert_eq!(
        record(&machine, &stream_id, "p").unwrap().opened,
        Opened::AboveFloor
    );
    // The original writer's lost 3:1 is stale, never reported as closed.
    assert_eq!(
        producer_refusal(send(
            &mut machine,
            &stream_id,
            Write::new("p", 3, 1).tokened().at(idle)
        )),
        (
            StreamErrorCode::ProducerEpochStale,
            ProducerRefusal::StaleEpoch { epoch: 4 }
        )
    );
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 4, 1).tokened().at(idle),
    ));
}

#[test]
fn row_2_a_new_producer_claims_with_seq_zero() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "claim", OCTET);
    let ack = applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 5, 0).digest(7),
    ));
    assert_eq!((ack.epoch, ack.seq, ack.closed_epoch), (5, 0, None));
    let state = record(&machine, &stream_id, "p").unwrap();
    assert_eq!(state.epoch, 5);
    assert_eq!(state.last_seq, 0);
    assert_eq!(state.claim_digest, Some([7; 32]));
    assert_eq!(state.newest.digest, Some([7; 32]));
}

#[test]
fn row_3_a_new_producer_above_seq_zero_expects_zero() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "gap", OCTET);
    let (code, context) = refused(send(&mut machine, &stream_id, Write::new("p", 0, 4)));
    assert_eq!(code, StreamErrorCode::ProducerSeqConflict);
    assert_eq!(context, vec![StreamErrorContext::ProducerSeqConflict {
        expected_seq: 0,
        received_seq: 4,
    }]);
}

#[test]
fn row_4_a_write_of_the_closed_epoch_reports_its_closure() {
    let mut machine = fresh_machine();
    let stream_id = three_epochs(&mut machine);
    let closed = ProducerRefusal::EpochClosed {
        epoch: 3,
        by: ClosedBy::Claim,
        closed: ClosedEpoch { epoch: 2, seq: 1 },
    };
    // An applied write of the superseded epoch answers 403, not 204: the
    // closed seq says it applied (1 <= 1). A seq above it never will.
    for seq in [0, 1, 2] {
        assert_eq!(
            producer_refusal(send(&mut machine, &stream_id, Write::new("p", 2, seq))),
            (StreamErrorCode::ProducerEpochClosed, closed)
        );
    }
}

#[test]
fn row_5_an_epoch_two_claims_old_is_a_stale_epoch() {
    let mut machine = fresh_machine();
    let stream_id = three_epochs(&mut machine);
    assert_eq!(
        producer_refusal(send(&mut machine, &stream_id, Write::new("p", 1, 1))),
        (
            StreamErrorCode::ProducerEpochStale,
            ProducerRefusal::StaleEpoch { epoch: 3 }
        )
    );
}

#[test]
fn row_6_a_claim_reports_the_epoch_it_closed_on_every_repeat() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "close", OCTET);
    for seq in 0..=4 {
        applied(send(&mut machine, &stream_id, Write::new("p", 1, seq)));
    }
    let closed = Some(ClosedEpoch { epoch: 1, seq: 4 });
    let claim = applied(send(&mut machine, &stream_id, Write::new("p", 2, 0)));
    assert_eq!((claim.epoch, claim.seq, claim.closed_epoch), (2, 0, closed));
    let repeat = ack(send(&mut machine, &stream_id, Write::new("p", 2, 0)));
    assert!(matches!(repeat.delivery, ProducerDelivery::Repeat(_)));
    assert_eq!(repeat.closed_epoch, closed);
    // After the epoch moves on, a repeat of its claim still reports it,
    // without a range.
    applied(send(&mut machine, &stream_id, Write::new("p", 2, 1)));
    let repeat = ack(send(&mut machine, &stream_id, Write::new("p", 2, 0)));
    assert_eq!(repeat.delivery, ProducerDelivery::Evicted);
    assert_eq!((repeat.seq, repeat.closed_epoch), (1, closed));
    // A later seq's answer carries no closure.
    assert_eq!(
        ack(send(&mut machine, &stream_id, Write::new("p", 2, 1))).closed_epoch,
        None
    );
}

#[test]
fn row_7_a_later_epoch_above_seq_zero_is_not_claimed() {
    let mut machine = fresh_machine();
    let stream_id = three_epochs(&mut machine);
    assert_eq!(
        producer_refusal(send(&mut machine, &stream_id, Write::new("p", 4, 1))),
        (
            StreamErrorCode::InvalidProducer,
            ProducerRefusal::EpochNotClaimed
        )
    );
}

#[test]
fn row_8_another_identity_under_a_held_key_is_a_key_conflict() {
    let conflict = (
        StreamErrorCode::ProducerKeyConflict,
        ProducerRefusal::KeyConflict { epoch: 1 },
    );
    // The claim's identity lives as long as its epoch, in both directions
    // of digest presence.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "claim-digest", OCTET);
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 0).digest(1),
    ));
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 1).digest(2),
    ));
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 2).digest(3),
    ));
    let before = tail(&machine, &stream_id);
    assert_eq!(
        producer_refusal(send(
            &mut machine,
            &stream_id,
            Write::new("p", 1, 0).digest(9)
        )),
        conflict
    );
    assert_eq!(
        producer_refusal(send(&mut machine, &stream_id, Write::new("p", 1, 0))),
        conflict
    );
    // The newest write's identity.
    assert_eq!(
        producer_refusal(send(
            &mut machine,
            &stream_id,
            Write::new("p", 1, 2).digest(9)
        )),
        conflict
    );
    assert_eq!(
        producer_refusal(send(&mut machine, &stream_id, Write::new("p", 1, 2))),
        conflict
    );
    // An older write's identity is not held: another body is answered as a
    // repeat (row 10).
    assert_eq!(
        ack(send(
            &mut machine,
            &stream_id,
            Write::new("p", 1, 1).digest(9)
        ))
        .delivery,
        ProducerDelivery::Evicted
    );
    assert_eq!(tail(&machine, &stream_id), before);

    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "claim-plain", OCTET);
    applied(send(&mut machine, &stream_id, Write::new("p", 1, 0)));
    assert_eq!(
        producer_refusal(send(
            &mut machine,
            &stream_id,
            Write::new("p", 1, 0).digest(1)
        )),
        conflict
    );
    // Without digests on either side, any body under the key is the same
    // write, as the base protocol requires.
    assert!(matches!(
        ack(send(&mut machine, &stream_id, Write::new("p", 1, 0))).delivery,
        ProducerDelivery::Repeat(_)
    ));
}

#[test]
fn row_9_a_repeat_of_the_newest_write_gets_its_receipt() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "newest", OCTET);
    applied(send(&mut machine, &stream_id, Write::new("p", 0, 0)));
    let first = applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 0, 1).digest(5),
    ));
    let ProducerDelivery::Applied(receipt) = first.delivery else {
        unreachable!()
    };
    let before = tail(&machine, &stream_id);
    let repeat = ack(send(
        &mut machine,
        &stream_id,
        Write::new("p", 0, 1).digest(5),
    ));
    assert_eq!(repeat.delivery, ProducerDelivery::Repeat(receipt));
    assert_eq!(
        (repeat.epoch, repeat.seq, repeat.closed_epoch),
        (0, 1, None)
    );
    assert_eq!(tail(&machine, &stream_id), before);
}

#[test]
fn row_10_a_repeat_of_an_older_write_is_receipt_evicted_with_the_highest_seq() {
    // The conformance case: Producer-Seq on a duplicate is the highest
    // applied seq, not the request's.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "older", OCTET);
    for seq in 0..3 {
        applied(send(&mut machine, &stream_id, Write::new("p", 0, seq)));
    }
    let repeat = ack(send(&mut machine, &stream_id, Write::new("p", 0, 1)));
    assert_eq!(repeat.delivery, ProducerDelivery::Evicted);
    assert_eq!(repeat.seq, 2);
}

#[test]
fn rows_12_and_13_the_next_seq_applies_and_a_gap_expects_it() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "next", OCTET);
    applied(send(&mut machine, &stream_id, Write::new("p", 0, 0)));
    let next = applied(send(&mut machine, &stream_id, Write::new("p", 0, 1)));
    assert_eq!(next.seq, 1);
    let (code, context) = refused(send(&mut machine, &stream_id, Write::new("p", 0, 3)));
    assert_eq!(code, StreamErrorCode::ProducerSeqConflict);
    assert_eq!(context, vec![StreamErrorContext::ProducerSeqConflict {
        expected_seq: 2,
        received_seq: 3,
    }]);
}

#[test]
fn a_record_keeps_one_receipt_however_many_writes_applied() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "many", OCTET);
    for seq in 0..2_000 {
        applied(send(&mut machine, &stream_id, Write::new("p", 0, seq)));
    }
    let repeat = ack(send(&mut machine, &stream_id, Write::new("p", 0, 1)));
    assert_eq!(repeat.delivery, ProducerDelivery::Evicted);
    assert_eq!(repeat.seq, 1_999);
    assert_eq!(producer_snapshot(&machine), vec![vec![ProducerSnapshot {
        producer_id: "p".to_owned(),
        epoch: 0,
        last_seq: 1_999,
        claim_digest: None,
        newest: ProducerReceipt {
            receipt: WriteReceipt {
                start_offset: 7_996,
                next_offset: 8_000,
                closed: false,
            },
            digest: None,
        },
        closed_epoch: None,
        opened: Opened::AboveFloor,
        history: machine.history_id(),
        last_seen_ms: 1,
    }]]);
}

#[test]
fn a_producer_close_is_keyed_like_an_append() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "close-key", OCTET);
    applied(send(&mut machine, &stream_id, Write::new("p", 0, 0)));
    let close = |machine: &mut StreamStateMachine| {
        machine.apply(StreamCommand::Close {
            stream_id: stream_id.clone(),
            stream_seq: None,
            producer: Some(Write::new("p", 0, 1).producer()),
            now_ms: 1,
        })
    };
    let first = applied(close(&mut machine));
    let ProducerDelivery::Applied(receipt) = first.delivery else {
        unreachable!()
    };
    assert!(receipt.closed);
    assert_eq!(receipt.start_offset, receipt.next_offset);
    let repeat = ack(close(&mut machine));
    assert_eq!(repeat.delivery, ProducerDelivery::Repeat(receipt));
    // A keyed close that would apply to the closed stream is `409`, a claim
    // included, and the record does not move. Only the unkeyed close is
    // idempotent.
    for write in [
        Write::new("p", 0, 2),
        Write::new("q", 0, 0),
        Write::new("p", 1, 0),
    ] {
        let (code, _) = refused(machine.apply(StreamCommand::Close {
            stream_id: stream_id.clone(),
            stream_seq: None,
            producer: Some(write.producer()),
            now_ms: 1,
        }));
        assert_eq!(code, StreamErrorCode::StreamClosed, "{write:?}");
    }
    assert_eq!(record(&machine, &stream_id, "p").unwrap().last_seq, 1);
    assert!(record(&machine, &stream_id, "q").is_none());
    assert!(matches!(
        machine.apply(StreamCommand::Close {
            stream_id: stream_id.clone(),
            stream_seq: None,
            producer: None,
            now_ms: 1,
        }),
        StreamResponse::Closed { producer: None, .. }
    ));
}

#[test]
fn idle_producer_expires_at_its_own_next_write_and_raises_the_floor() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "idle", OCTET);
    let t0 = 1_000;
    for seq in 0..3 {
        applied(send(
            &mut machine,
            &stream_id,
            Write::new("p", 1, seq).at(t0),
        ));
    }
    // Just before the idle period: still a repeat.
    let just_before = t0 + PRODUCER_IDLE_EXPIRY_MS - 1;
    assert!(
        ack(send(
            &mut machine,
            &stream_id,
            Write::new("p", 1, 0).at(just_before)
        ))
        .is_repeat()
    );
    // At the idle period the record retires: without the token a later
    // sequence expects 0 and cannot double-write.
    let idle_at = t0 + PRODUCER_IDLE_EXPIRY_MS;
    let before = tail(&machine, &stream_id);
    let (code, _) = refused(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 3).at(idle_at),
    ));
    assert_eq!(code, StreamErrorCode::ProducerSeqConflict);
    assert_eq!(tail(&machine, &stream_id), before);
    assert!(record(&machine, &stream_id, "p").is_none());
    assert_eq!(floor(&machine, &stream_id), Some(1));
    // Without the token, sequence 0 is a new producer, below the floor.
    let claim = applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 0).at(idle_at),
    ));
    assert_eq!(claim.closed_epoch, None);
    assert_eq!(
        record(&machine, &stream_id, "p").unwrap().last_seen_ms,
        idle_at
    );
}

#[test]
fn every_retirement_raises_the_floor_and_restore_keeps_it() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "retire", OCTET);
    applied(send(&mut machine, &stream_id, Write::new("old", 9, 0)));
    applied(send(&mut machine, &stream_id, Write::new("young", 2, 0)));
    // `TidyStream` retires idle records in bulk; the floor is the highest.
    let idle = 1 + PRODUCER_IDLE_EXPIRY_MS;
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: idle,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    assert_eq!(floor(&machine, &stream_id), Some(9));
    let restored = StreamStateMachine::restore(machine.snapshot()).unwrap();
    assert_eq!(floor(&restored, &stream_id), Some(9));
    assert_eq!(restored.snapshot(), machine.snapshot());
    // The floor never decreases.
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("young", 10, 0).tokened().at(idle),
    ));
    let later = idle + PRODUCER_IDLE_EXPIRY_MS;
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: later,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    assert_eq!(floor(&machine, &stream_id), Some(10));
}

#[test]
fn early_answers_come_only_from_rows_8_to_10_under_the_applied_token() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "early", OCTET);
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 0).digest(1),
    ));
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 1).digest(2),
    ));
    let incarnation = machine.stream_incarnation(&stream_id).unwrap();
    let early = |machine: &StreamStateMachine, write: Write<'_>, incarnation| {
        machine.producer_early_answer(&stream_id, &write.producer(), incarnation, write.now_ms)
    };
    let repeat = |answer: Option<ProducerEarlyAnswer>| match answer {
        Some(ProducerEarlyAnswer::Repeat(ack)) => ack,
        other => panic!("expected a repeat, got {other:?}"),
    };
    let newest = repeat(early(
        &machine,
        Write::new("p", 1, 1).digest(2),
        incarnation,
    ));
    assert!(matches!(newest.delivery, ProducerDelivery::Repeat(_)));
    let older = repeat(early(
        &machine,
        Write::new("p", 1, 0).digest(1),
        incarnation,
    ));
    assert_eq!(older.delivery, ProducerDelivery::Evicted);
    assert_eq!(
        early(&machine, Write::new("p", 1, 1).digest(3), incarnation),
        Some(ProducerEarlyAnswer::KeyConflict { epoch: 1 })
    );
    // Everything else is decided at apply: the next seq, a gap, an older
    // epoch, and any write under another token.
    for write in [
        Write::new("p", 1, 2),
        Write::new("p", 1, 5),
        Write::new("p", 0, 0),
        Write::new("p", 2, 0),
    ] {
        assert_eq!(early(&machine, write, incarnation), None, "{write:?}");
    }
    assert_eq!(
        early(&machine, Write::new("p", 1, 1).digest(2), incarnation + 1),
        None
    );
    // The answer matches what apply answers.
    let at_apply = ack(send(
        &mut machine,
        &stream_id,
        Write::new("p", 1, 1).digest(2).tokened(),
    ));
    assert_eq!(at_apply, newest);
}

#[test]
fn snapshot_round_trip_and_restore_versus_live_differential() {
    let mut live = fresh_machine();
    let stream_id = create(&mut live, "diff", OCTET);
    applied(send(
        &mut live,
        &stream_id,
        Write::new("old", 1, 0).digest(1).at(10),
    ));
    applied(send(
        &mut live,
        &stream_id,
        Write::new("old", 1, 1).digest(2).at(10),
    ));
    applied(send(
        &mut live,
        &stream_id,
        Write::new("old", 2, 0).digest(3).at(10),
    ));
    for seq in 0..50 {
        applied(send(
            &mut live,
            &stream_id,
            Write::new("busy", 0, seq).at(20),
        ));
    }

    let snapshot = live.snapshot();
    let encoded = serde_json::to_vec(&snapshot).unwrap();
    let decoded: StreamSnapshot = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, snapshot);
    let mut restored = StreamStateMachine::restore(decoded).unwrap();
    assert_eq!(restored.snapshot(), snapshot);

    // Identical commands keep both replicas identical.
    let mut writes = Vec::new();
    for seq in 50..400 {
        writes.push(Write::new("busy", 0, seq).at(30));
        if seq % 7 == 0 {
            writes.push(Write::new("other", 0, (seq - 50) / 7).at(30));
        }
    }
    let idle = 40 + PRODUCER_IDLE_EXPIRY_MS;
    writes.push(Write::new("old", 2, 0).digest(3).at(30));
    writes.push(Write::new("old", 1, 1).digest(2).at(30));
    writes.push(Write::new("old", 2, 1).tokened().at(idle));
    writes.push(Write::new("old", 3, 0).tokened().at(idle));
    for write in writes {
        let left = send(&mut live, &stream_id, write);
        let right = send(&mut restored, &stream_id, write);
        assert_eq!(left, right, "{write:?}");
    }
    assert_eq!(live.snapshot(), restored.snapshot());
}

#[test]
fn tidy_stream_is_idempotent() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "tidy", OCTET);
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 1,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    let before = machine.snapshot();
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 1,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    assert_eq!(machine.snapshot(), before);
    assert!(matches!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: sid("missing"),
            now_ms: 1,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        }
    ));
}

#[test]
fn bootstrap_stays_honest_after_external_appends() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "boot", OCTET);
    let ext = |machine: &mut StreamStateMachine, path: &str| {
        let response = machine.apply(StreamCommand::AppendExternal {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: ExternalPayloadRef {
                s3_path: path.to_owned(),
                payload_len: 10,
                object_size: 10,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
        });
        assert!(matches!(response, StreamResponse::Appended { .. }));
    };
    ext(&mut machine, "a");
    ext(&mut machine, "b");
    ext(&mut machine, "c");
    // A snapshot inside the cold region is still accepted (F18),
    // and bootstrap from it is an honest partial: no update parts.
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 10,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            now_ms: 1,
            expected_incarnation: None,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 10);
    assert!(!plan.up_to_date);
    // A snapshot at the tail is up to date with no parts.
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 30,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            now_ms: 1,
            expected_incarnation: None,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 30);
    assert!(plan.up_to_date);
}

#[test]
fn bootstrap_is_partial_when_a_hot_prefix_flush_leaves_an_external_below_the_seal_point() {
    // D1 shape: small hot bytes, an external append, then a flush of only
    // the hot prefix. Every byte below the seal point is cold, so bootstrap
    // from inside it must be partial.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "d1", OCTET);
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: Bytes::from_static(b"0123456789"),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::AppendExternal {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: ExternalPayloadRef {
                s3_path: "big".to_owned(),
                payload_len: 100,
                object_size: 100,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream_id.clone(),
            cold_generation: machine
                .cold_index_generation(&stream_id)
                .unwrap_or_default(),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 10,
                s3_path: "chunk".to_owned(),
                object_size: 10,
                ..ColdChunkRef::default()
            },
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 10,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            now_ms: 1,
            expected_incarnation: None,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(plan.updates.is_empty(), "{plan:?}");
    assert_eq!(plan.next_offset, 10);
    assert!(!plan.up_to_date);
}

#[test]
fn tidy_stream_command_round_trips_through_serde() {
    let command = StreamCommand::TidyStream {
        stream_id: sid("serde"),
        now_ms: 9,
    };
    let encoded = serde_json::to_vec(&command).unwrap();
    assert_eq!(
        serde_json::from_slice::<StreamCommand>(&encoded).unwrap(),
        command
    );
    assert_eq!(command.to_string(), "tidy_stream:window/serde");
}

#[test]
fn producer_cap_evicts_hour_idle_producers_and_rejects_otherwise() {
    use super::producers::MAX_PRODUCERS_PER_STREAM;
    use super::producers::PRODUCER_CAP_EVICT_IDLE_MS;
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "cap", OCTET);
    let ids = (0..MAX_PRODUCERS_PER_STREAM)
        .map(|index| format!("p{index:04}"))
        .collect::<Vec<_>>();
    // Producer `p0000` writes first, so it is the least recently seen.
    for (index, id) in ids.iter().enumerate() {
        let now_ms = u64::try_from(index).unwrap();
        applied(send(
            &mut machine,
            &stream_id,
            Write::new(id, 1, 0).at(now_ms),
        ));
    }
    let count =
        |machine: &StreamStateMachine| machine.stream_slot(&stream_id).unwrap().producers.len();
    assert_eq!(count(&machine), MAX_PRODUCERS_PER_STREAM);

    // Nobody has been idle for an hour: a new producer gets 429 producer_limit
    // and nothing is written.
    let tail_before = tail(&machine, &stream_id);
    match send(
        &mut machine,
        &stream_id,
        Write::new("new-a", 1, 0).at(10_000),
    ) {
        StreamResponse::Error { code, message, .. } => {
            assert_eq!(code, StreamErrorCode::ProducerLimit);
            assert!(message.starts_with("producer_limit"), "{message}");
        }
        other => panic!("expected ProducerLimit, got {other:?}"),
    }
    assert_eq!(tail(&machine, &stream_id), tail_before);
    assert_eq!(count(&machine), MAX_PRODUCERS_PER_STREAM);
    // Existing producers keep writing at the cap.
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("p0001", 1, 1).at(10_000),
    ));

    // An hour after `p0000` and `p0002` last wrote they are evictable; the
    // least recently seen goes first, the cap holds, and the eviction
    // raises the floor.
    let later = PRODUCER_CAP_EVICT_IDLE_MS + 2;
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("new-a", 1, 0).at(later),
    ));
    assert_eq!(count(&machine), MAX_PRODUCERS_PER_STREAM);
    let slot = machine.stream_slot(&stream_id).unwrap();
    assert!(!slot.producers.contains_key("p0000"));
    assert!(
        slot.producers.contains_key("p0001"),
        "recently seen is kept"
    );
    assert!(slot.producers.contains_key("p0002"));
    assert!(slot.producers.contains_key("new-a"));
    assert_eq!(slot.retired_epoch_watermark, Some(1));
    // `p0000`'s eviction raised the floor to its epoch 1.
    applied(send(
        &mut machine,
        &stream_id,
        Write::new("new-b", 2, 0).at(later),
    ));
    let slot = machine.stream_slot(&stream_id).unwrap();
    assert_eq!(slot.producers.len(), MAX_PRODUCERS_PER_STREAM);
    assert!(!slot.producers.contains_key("p0002"));

    // A replica that restores the snapshot holds the same producers.
    let restored = StreamStateMachine::restore(machine.snapshot()).unwrap();
    assert_eq!(producer_snapshot(&restored), producer_snapshot(&machine));
}

/// How a proptest write closes the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Closing {
    /// An append that leaves the stream open.
    No,
    /// An append with `Stream-Closed: true`.
    WithBody,
    /// A close-only write.
    CloseOnly,
}

/// One step of the producer proptest.
#[derive(Debug, Clone)]
enum Op {
    /// The next seq of the producer's current epoch, or a claim of epoch 0.
    Next {
        producer: usize,
        digest: Option<u8>,
        tokened: bool,
    },
    /// A claim of the epoch `bump` above the producer's current one.
    Claim {
        producer: usize,
        bump: u64,
        digest: Option<u8>,
        tokened: bool,
    },
    /// A repeat of the `pick`-th key that applied, with `digest`.
    Repeat {
        pick: usize,
        digest: Option<u8>,
        tokened: bool,
    },
    /// Any key at all.
    Any {
        producer: usize,
        epoch: u64,
        seq: u64,
        digest: Option<u8>,
        tokened: bool,
    },
    /// The producer's next key closes the stream, with or without a body.
    Close {
        producer: usize,
        with_body: bool,
        tokened: bool,
    },
    /// The clock moves on: a second, past the producer cap's idle hour, or
    /// past idle expiry.
    Wait { by: u64 },
    /// `TidyStream` retires the idle records.
    Tidy,
    /// Other producers fill the stream to the producer cap, at `epoch`.
    Crowd { epoch: u64 },
    /// A replica restores the snapshot and carries on in its place.
    Restore,
}

const PRODUCERS: [&str; 2] = ["a", "b"];

fn op_strategy() -> impl Strategy<Value = Op> {
    use super::producers::PRODUCER_CAP_EVICT_IDLE_MS;
    let digest = prop_oneof![Just(None), Just(Some(1_u8)), Just(Some(2_u8))];
    prop_oneof![
        8 => (0..2_usize, digest.clone(), any::<bool>()).prop_map(|(producer, digest, tokened)| Op::Next {
            producer,
            digest,
            tokened,
        }),
        4 => (0..2_usize, 1..3_u64, digest.clone(), any::<bool>()).prop_map(
            |(producer, bump, digest, tokened)| Op::Claim {
                producer,
                bump,
                digest,
                tokened,
            }
        ),
        6 => (any::<usize>(), digest.clone(), any::<bool>()).prop_map(|(pick, digest, tokened)| Op::Repeat {
            pick,
            digest,
            tokened,
        }),
        4 => (0..2_usize, 0..6_u64, 0..4_u64, digest, any::<bool>()).prop_map(
            |(producer, epoch, seq, digest, tokened)| Op::Any {
                producer,
                epoch,
                seq,
                digest,
                tokened,
            }
        ),
        1 => (0..2_usize, any::<bool>(), any::<bool>()).prop_map(|(producer, with_body, tokened)| Op::Close {
            producer,
            with_body,
            tokened,
        }),
        2 => prop_oneof![
            Just(1_000_u64),
            Just(PRODUCER_CAP_EVICT_IDLE_MS),
            Just(PRODUCER_IDLE_EXPIRY_MS),
        ]
        .prop_map(|by| Op::Wait { by }),
        1 => Just(Op::Tidy),
        1 => (0..6_u64).prop_map(|epoch| Op::Crowd { epoch }),
        1 => Just(Op::Restore),
    ]
}

/// What applied, as the answers told it: each key's receipt and identity in
/// the life of its record.
#[derive(Debug, Default)]
struct Ledger {
    /// The keys of each producer's current record and their receipts.
    applied: HashMap<(String, u64, u64), (WriteReceipt, Option<u8>)>,
    /// The highest applied seq of each `(producer, epoch)`.
    last: HashMap<(String, u64), u64>,
    keys: Vec<(String, u64, u64)>,
    /// Epochs a claim closed or a retirement forgot, per producer (G2b:
    /// none of them may ever apply a tokened write again).
    dead: std::collections::HashSet<(String, u64)>,
    /// Epochs a retirement forgot, per producer, apart from those a claim
    /// closed: no answer may report one of them as closed, since its seqs
    /// may belong to another writer.
    forgotten: std::collections::HashSet<(String, u64)>,
}

impl Ledger {
    /// `producer`'s record retired at `epoch`: none of its keys can be
    /// repeated, and its epoch is dead to tokened writes.
    fn retire(&mut self, producer: &str, epoch: u64) {
        self.dead.insert((producer.to_owned(), epoch));
        self.forgotten.insert((producer.to_owned(), epoch));
        self.applied.retain(|(id, _, _), _| id != producer);
    }
}

/// What the test knew before one write.
struct Before<'a> {
    write: Write<'a>,
    closing: Closing,
    /// The producer's record, unless it was idle and retires at this write.
    record: Option<ProducerState>,
    /// Whether the stream was closed.
    closed: bool,
    /// The floor the write is evaluated against: the stream's, raised by
    /// the producer's own idle record. The cap may raise it after.
    floor: Option<u64>,
    tail: u64,
}

/// A write's producer ack, from an append or a close-only answer.
fn producer_ack(response: &StreamResponse) -> Option<ProducerAck> {
    match response {
        StreamResponse::Appended { producer, .. } | StreamResponse::Closed { producer, .. } => {
            *producer
        }
        _ => None,
    }
}

fn check_answer(
    ledger: &mut Ledger,
    before: &Before<'_>,
    floor_after: Option<u64>,
    tail_after: u64,
    response: StreamResponse,
) -> Result<(), TestCaseError> {
    let write = before.write;
    let record_before = before.record.as_ref();
    let tail_before = before.tail;
    let id = write.id.to_owned();
    let key = (id.clone(), write.epoch, write.seq);
    // No answer reports a forgotten epoch as closed: a closure tells the
    // writer which of its writes applied, and the stream no longer knows.
    let reported = match &response {
        StreamResponse::Error { context, .. } => context.iter().find_map(|context| match context {
            StreamErrorContext::Producer(ProducerRefusal::EpochClosed { closed, .. }) => {
                Some(*closed)
            }
            _ => None,
        }),
        other => producer_ack(other).and_then(|ack| ack.closed_epoch),
    };
    if let Some(closed) = reported {
        prop_assert!(
            !ledger.forgotten.contains(&(id.clone(), closed.epoch)),
            "{:?} reported forgotten epoch {} of {} as closed",
            write,
            closed.epoch,
            id
        );
    }
    if let Some(ack) = producer_ack(&response) {
        match ack.delivery {
            ProducerDelivery::Applied(receipt) => {
                prop_assert_eq!((ack.epoch, ack.seq), (write.epoch, write.seq));
                // Nothing applies to a closed stream.
                prop_assert!(!before.closed, "a write applied to a closed stream");
                prop_assert_eq!(receipt.closed, before.closing != Closing::No);
                // G2b: an epoch a claim closed or a retirement forgot never
                // applies a tokened write again. Without the token the base
                // protocol treats a forgotten producer as new (#581 section
                // 2.10), and its record opens below the floor.
                prop_assert!(
                    !(write.tokened && ledger.dead.contains(&(id.clone(), write.epoch))),
                    "epoch {} of {} reopened to a tokened write",
                    write.epoch,
                    id
                );
                if write.seq == 0 {
                    // L4, rows 1 and 2: a tokened claim without a record opens
                    // above the floor.
                    if write.tokened && record_before.is_none() {
                        prop_assert!(before.floor.is_none_or(|floor| write.epoch > floor));
                    }
                    // L1: a key applies at most once, except an untokened
                    // claim of an epoch the stream forgot, which is new: by a
                    // producer without a record, or on the record such a
                    // claim opened below the floor.
                    if ledger.last.contains_key(&(id.clone(), write.epoch)) {
                        prop_assert!(
                            !write.tokened
                                && record_before
                                    .is_none_or(|record| record.opened == Opened::BelowFloor)
                        );
                        ledger
                            .applied
                            .retain(|(p, e, _), _| !(p == &id && *e == write.epoch));
                    }
                    if let Some(closed) = ack.closed_epoch {
                        ledger.dead.insert((id.clone(), closed.epoch));
                    }
                } else {
                    // L1: exact seqs, in order.
                    prop_assert_eq!(
                        ledger.last.get(&(id.clone(), write.epoch)).copied(),
                        write.seq.checked_sub(1)
                    );
                }
                prop_assert_eq!(receipt.start_offset, tail_before);
                prop_assert_eq!(receipt.next_offset, tail_after);
                ledger.last.insert((id.clone(), write.epoch), write.seq);
                ledger.applied.insert(key.clone(), (receipt, write.digest));
                ledger.keys.push(key);
            }
            ProducerDelivery::Repeat(receipt) => {
                prop_assert_eq!(tail_after, tail_before);
                let original = ledger.applied.get(&key);
                prop_assert!(original.is_some(), "a repeat of a key that never applied");
                let (original_receipt, original_digest) = *original.unwrap();
                prop_assert_eq!(receipt, original_receipt);
                // L5: a receipt is never given for another identity.
                prop_assert_eq!(write.digest, original_digest);
                prop_assert_eq!(ledger.last.get(&(id, write.epoch)).copied(), Some(ack.seq));
            }
            ProducerDelivery::Evicted => {
                prop_assert_eq!(tail_after, tail_before);
                let original = ledger.applied.get(&key);
                prop_assert!(
                    original.is_some(),
                    "an evicted repeat of a key that never applied"
                );
                if write.seq == 0 {
                    prop_assert_eq!(write.digest, original.unwrap().1);
                }
                prop_assert_eq!(ledger.last.get(&(id, write.epoch)).copied(), Some(ack.seq));
            }
        }
        return Ok(());
    }
    let StreamResponse::Error { code, context, .. } = response else {
        prop_assert!(false, "unexpected answer {:?}", response);
        return Ok(());
    };
    prop_assert_eq!(
        tail_after,
        tail_before,
        "a refusal changed the stream: {:?}",
        code
    );
    if let [StreamErrorContext::Producer(refusal)] = context.as_slice() {
        prop_assert_eq!(code, refusal.code());
    }
    match context.as_slice() {
        [StreamErrorContext::Producer(ProducerRefusal::EpochClosed { epoch, closed, .. })] => {
            // L3: the closure is reported exactly, with the current epoch.
            let record = record_before.unwrap();
            prop_assert_eq!(*epoch, record.epoch);
            prop_assert_eq!(closed.epoch, write.epoch);
            prop_assert_eq!(
                ledger.last.get(&(id, closed.epoch)).copied(),
                Some(closed.seq)
            );
        }
        [StreamErrorContext::Producer(ProducerRefusal::StaleEpoch { epoch })] => {
            // Row 5: an epoch older than the current one and not the one its
            // claim closed.
            let record = record_before.unwrap();
            prop_assert_eq!(*epoch, record.epoch);
            prop_assert!(write.epoch < record.epoch);
            prop_assert!(
                record
                    .closed_epoch
                    .is_none_or(|closed| closed.epoch != write.epoch)
            );
        }
        [StreamErrorContext::Producer(ProducerRefusal::Expired { epoch })] => {
            // Row 1 and the marker: only a tokened write, only at or below
            // the floor, and the answer names the floor.
            prop_assert!(write.tokened, "an untokened write got producer-expired");
            prop_assert_eq!(Some(*epoch), before.floor);
            prop_assert_eq!(before.floor, floor_after);
            match record_before {
                None => prop_assert!(write.epoch <= *epoch),
                Some(record) => {
                    prop_assert_eq!(record.opened, Opened::BelowFloor);
                    prop_assert!(
                        !(write.seq == 0 && write.epoch > record.epoch && write.epoch > *epoch),
                        "a claim above the floor was expired"
                    );
                }
            }
        }
        [StreamErrorContext::Producer(ProducerRefusal::KeyConflict { .. })] => {
            let original = ledger.applied.get(&key);
            prop_assert!(original.is_some(), "a conflict on a key that never applied");
            prop_assert_ne!(original.unwrap().1, write.digest);
        }
        [StreamErrorContext::Producer(ProducerRefusal::EpochNotClaimed)] => {
            // Row 7: a seq above 0 of an epoch above the record's.
            let record = record_before.unwrap();
            prop_assert!(write.seq > 0 && write.epoch > record.epoch);
        }
        [
            StreamErrorContext::ProducerSeqConflict {
                expected_seq,
                received_seq,
            },
        ] => {
            // Row 3 expects 0 of a new producer; a gap expects the next seq.
            prop_assert_eq!(*received_seq, write.seq);
            let expected = match record_before {
                Some(record) if record.epoch == write.epoch => record.last_seq.saturating_add(1),
                Some(_) => {
                    prop_assert!(false, "a gap in another epoch than the record's");
                    0
                }
                None => 0,
            };
            prop_assert_eq!(*expected_seq, expected);
            prop_assert!(write.seq > expected);
        }
        _ if code == StreamErrorCode::StreamClosed => {
            // Step 4 comes after the producer rows: a closed stream refuses
            // only a write that would apply, never a repeat or a conflict.
            prop_assert!(before.closed || before.closing == Closing::CloseOnly);
            prop_assert!(
                record_before.is_none() || !ledger.applied.contains_key(&key),
                "a closed stream refused a key that applied"
            );
        }
        _ if code == StreamErrorCode::ProducerLimit => {
            // The cap refuses only a producer without a record.
            prop_assert!(record_before.is_none());
        }
        other => prop_assert!(false, "unexpected refusal {:?} {:?}", code, other),
    }
    Ok(())
}

/// Applies a proptest write: an append, an append that closes, or a
/// close-only write.
fn send_closing(
    machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
    write: Write<'_>,
    closing: Closing,
) -> StreamResponse {
    let command = match closing {
        Closing::No => return send(machine, stream_id, write),
        Closing::WithBody => StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: write.body(),
            close_after: true,
            stream_seq: None,
            producer: Some(write.producer()),
            now_ms: write.now_ms,
        },
        Closing::CloseOnly => StreamCommand::Close {
            stream_id: stream_id.clone(),
            stream_seq: None,
            producer: Some(write.producer()),
            now_ms: write.now_ms,
        },
    };
    let command = if write.tokened {
        StreamCommand::IfIncarnation {
            incarnation: machine.stream_incarnation(stream_id).unwrap(),
            command: Box::new(command),
        }
    } else {
        command
    };
    machine.apply(command)
}

/// Fills the stream with other producers up to the producer cap.
fn crowd(machine: &mut StreamStateMachine, stream_id: &BucketStreamId, epoch: u64, now_ms: u64) {
    use super::producers::MAX_PRODUCERS_PER_STREAM;
    let mut index = 0_usize;
    while machine.stream_slot(stream_id).unwrap().producers.len() < MAX_PRODUCERS_PER_STREAM {
        let id = format!("f{index:04}");
        index = index.saturating_add(1);
        if !matches!(
            send(machine, stream_id, Write::new(&id, epoch, 0).at(now_ms)),
            StreamResponse::Appended { .. }
        ) {
            return;
        }
    }
}

/// Runs `ops` against one stream and checks every answer. `Err` names the
/// first broken property.
fn run_producer_ops(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "prop", OCTET);
    let mut ledger = Ledger::default();
    let mut now_ms = 1_u64;
    let mut crowded = false;
    for op in ops {
        let records = PRODUCERS.map(|id| record(&machine, &stream_id, id));
        let floor_at_start = floor(&machine, &stream_id);
        let (write, closing) = match op {
            Op::Wait { by } => {
                now_ms = now_ms.saturating_add(by);
                continue;
            }
            Op::Restore => {
                // A replica that restores the snapshot holds the same state,
                // and the rest of the run goes on against it.
                let restored = StreamStateMachine::restore(machine.snapshot())
                    .map_err(|err| TestCaseError::fail(format!("restore: {err:?}")))?;
                prop_assert_eq!(restored.snapshot(), machine.snapshot());
                machine = restored;
                continue;
            }
            Op::Tidy => {
                let tidied = machine.apply(StreamCommand::TidyStream {
                    stream_id: stream_id.clone(),
                    now_ms,
                });
                prop_assert!(
                    matches!(tidied, StreamResponse::StreamTidied { .. }),
                    "{:?}",
                    tidied
                );
                retire_gone(&mut ledger, &machine, &stream_id, &records);
                prop_assert!(floor(&machine, &stream_id) >= floor_at_start);
                continue;
            }
            Op::Crowd { epoch } => {
                if !crowded {
                    crowded = true;
                    crowd(&mut machine, &stream_id, epoch, now_ms);
                }
                continue;
            }
            Op::Next {
                producer,
                digest,
                tokened,
            } => {
                let id = PRODUCERS[producer];
                let (epoch, seq) = record(&machine, &stream_id, id).map_or((0, 0), |state| {
                    (state.epoch, state.last_seq.saturating_add(1))
                });
                (
                    Write {
                        id,
                        epoch,
                        seq,
                        digest,
                        tokened,
                        now_ms,
                    },
                    Closing::No,
                )
            }
            Op::Close {
                producer,
                with_body,
                tokened,
            } => {
                let id = PRODUCERS[producer];
                let (epoch, seq) = record(&machine, &stream_id, id).map_or((0, 0), |state| {
                    (state.epoch, state.last_seq.saturating_add(1))
                });
                let closing = if with_body {
                    Closing::WithBody
                } else {
                    Closing::CloseOnly
                };
                (
                    Write {
                        id,
                        epoch,
                        seq,
                        digest: None,
                        tokened,
                        now_ms,
                    },
                    closing,
                )
            }
            Op::Claim {
                producer,
                bump,
                digest,
                tokened,
            } => {
                let id = PRODUCERS[producer];
                let epoch = record(&machine, &stream_id, id)
                    .map_or(0, |state| state.epoch)
                    .max(floor(&machine, &stream_id).unwrap_or(0))
                    .saturating_add(bump);
                (
                    Write {
                        id,
                        epoch,
                        seq: 0,
                        digest,
                        tokened,
                        now_ms,
                    },
                    Closing::No,
                )
            }
            Op::Repeat {
                pick,
                digest,
                tokened,
            } => {
                if ledger.keys.is_empty() {
                    continue;
                }
                let (id, epoch, seq) = &ledger.keys[pick.checked_rem(ledger.keys.len()).unwrap()];
                let id = PRODUCERS.iter().find(|candidate| *candidate == id).unwrap();
                (
                    Write {
                        id,
                        epoch: *epoch,
                        seq: *seq,
                        digest,
                        tokened,
                        now_ms,
                    },
                    Closing::No,
                )
            }
            Op::Any {
                producer,
                epoch,
                seq,
                digest,
                tokened,
            } => (
                Write {
                    id: PRODUCERS[producer],
                    epoch,
                    seq,
                    digest,
                    tokened,
                    now_ms,
                },
                Closing::No,
            ),
        };
        // The write retires its producer's idle record first: that epoch is
        // forgotten.
        let idle_epoch = record(&machine, &stream_id, write.id)
            .filter(|state| producers::producer_is_idle(state, write.now_ms))
            .map(|state| state.epoch);
        if let Some(retired) = idle_epoch {
            ledger.retire(write.id, retired);
        }
        let before = Before {
            write,
            closing,
            record: record(&machine, &stream_id, write.id)
                .filter(|state| !producers::producer_is_idle(state, write.now_ms)),
            closed: machine.head(&stream_id).unwrap().status == StreamStatus::Closed,
            floor: match (floor(&machine, &stream_id), idle_epoch) {
                (Some(floor), Some(idle)) => Some(floor.max(idle)),
                (floor, idle) => floor.or(idle),
            },
            tail: tail(&machine, &stream_id),
        };
        // The early answer, read before the write applies, must be what
        // apply answers whenever it gives one (#581 section 2.5).
        let early = machine.producer_early_answer(
            &stream_id,
            &write.producer(),
            machine.stream_incarnation(&stream_id).unwrap(),
            write.now_ms,
        );
        let response = send_closing(&mut machine, &stream_id, write, closing);
        // Only tokened writes are answered early.
        let early = early.filter(|_| write.tokened);
        match early {
            None => {}
            Some(ProducerEarlyAnswer::Repeat(early)) => {
                prop_assert_eq!(
                    producer_ack(&response),
                    Some(early),
                    "early repeat differs from apply"
                );
            }
            Some(ProducerEarlyAnswer::KeyConflict { epoch }) => {
                let conflict = matches!(
                    &response,
                    StreamResponse::Error { context, .. }
                        if context.as_slice()
                            == [StreamErrorContext::Producer(ProducerRefusal::KeyConflict { epoch })]
                );
                prop_assert!(
                    conflict,
                    "early key conflict differs from apply: {:?}",
                    response
                );
            }
        }
        // The producer cap may retire other records to admit this one.
        let floor_after = floor(&machine, &stream_id);
        let tail_after = tail(&machine, &stream_id);
        check_answer(&mut ledger, &before, floor_after, tail_after, response)?;
        retire_gone(&mut ledger, &machine, &stream_id, &records);
        // The floor never decreases.
        prop_assert!(floor_after >= floor_at_start);
    }
    let restored = StreamStateMachine::restore(machine.snapshot())
        .map_err(|err| TestCaseError::fail(format!("restore: {err:?}")))?;
    prop_assert_eq!(restored.snapshot(), machine.snapshot());
    Ok(())
}

/// Records of the tested producers that a command removed retired: their
/// epochs are dead to tokened writes.
fn retire_gone(
    ledger: &mut Ledger,
    machine: &StreamStateMachine,
    stream_id: &BucketStreamId,
    records: &[Option<ProducerState>; 2],
) {
    for (id, before) in PRODUCERS.iter().zip(records) {
        if let Some(before) = before
            && record(machine, stream_id, id).is_none()
        {
            ledger.retire(id, before.epoch);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Producers with one write in flight, re-claims, clones that share a
    /// key with other bodies, closes, idle expiry, `TidyStream`, the
    /// producer cap, untokened re-claims and restores: every answer is true
    /// against what applied, every refusal is the row the record calls for,
    /// every key applies at most once within its record's life, seqs apply
    /// in order, nothing applies to a closed stream, and no tokened write
    /// reaches an epoch the stream retired.
    #[test]
    fn producer_answers_are_true_and_keys_apply_once(ops in proptest::collection::vec(op_strategy(), 1..80)) {
        run_producer_ops(ops)?;
    }
}
