//! One test per row of the two answer tables of design §4.7, through [`decide_append`] and
//! [`decide_claim`], with answers rendered and parsed as they travel.

use std::sync::Arc;
use std::time::Duration;

use ursula_headers::ClosedBy;
use ursula_headers::ClosedEpoch;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::EpochClosure;
use ursula_headers::IncarnationMismatch;
use ursula_headers::ProducerAnswer;
use ursula_headers::SizeLimit;
use ursula_headers::StreamClosedConflict;

use super::epoch;
use super::local;
use super::off;
use super::producer;
use super::receipt;
use super::seq;
use super::token;
use crate::replicator::BlockedReason;
use crate::replicator::Ctx;
use crate::replicator::ResetKind;
use crate::replicator::answer::AppendDecision;
use crate::replicator::answer::ClaimDecision;
use crate::replicator::answer::Common;
use crate::replicator::answer::HeadReason;
use crate::replicator::answer::decide_append;
use crate::replicator::answer::decide_claim;
use crate::replicator::batch::Sealed;
use crate::wire::Exchange;
use crate::wire::LOCATION;
use crate::wire::NoAnswer;
use crate::wire::RETRY_AFTER;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Typed;

/// The in-flight append `(3, 4)`, expected at offset 100.
fn sealed() -> Sealed {
    Sealed {
        epoch: epoch(3),
        seq: seq(4),
        first: crate::ids::Position(5),
        last: crate::ids::Position(6),
        body: Arc::from(b"batch".to_vec()),
        expected_start: off(100),
    }
}

fn keyed(s: u64) -> Request {
    Ctx::new(local(Vec::new(), 0)).unwrap().write_request(
        epoch(3),
        seq(s),
        Arc::from(b"batch".to_vec()),
    )
}

fn answered(raw: &RawAnswer, s: u64) -> Exchange {
    Exchange::Answered(Box::new(raw.parse(&keyed(s))))
}

fn append(raw: &RawAnswer) -> AppendDecision {
    decide_append(&sealed(), &answered(raw, 4))
}

fn claim(raw: &RawAnswer) -> ClaimDecision {
    decide_claim(&answered(raw, 0))
}

fn closed(e: u64, s: u64) -> ClosedEpoch {
    ClosedEpoch {
        epoch: epoch(e),
        seq: seq(s),
    }
}

fn status(status: u16) -> RawAnswer {
    RawAnswer::new(status)
}

fn common(decision: Common) -> AppendDecision {
    AppendDecision::Common(decision)
}

#[test]
fn append_row_1_a_200_at_the_expected_tail_acknowledges() {
    let raw = producer(&receipt(Delivery::Applied, 3, 4, 100, 140, None));
    assert_eq!(append(&raw), AppendDecision::Acknowledge {
        next: off(140),
        delivery: Delivery::Applied,
    });
}

#[test]
fn append_row_2_a_204_at_the_expected_tail_acknowledges() {
    let raw = producer(&receipt(Delivery::Repeat, 3, 4, 100, 140, None));
    assert_eq!(append(&raw), AppendDecision::Acknowledge {
        next: off(140),
        delivery: Delivery::Repeat,
    });
}

#[test]
fn append_row_3_a_receipt_at_another_offset_resets_stream_state() {
    for delivery in [Delivery::Applied, Delivery::Repeat] {
        let raw = producer(&receipt(delivery, 3, 4, 120, 160, None));
        assert_eq!(append(&raw), common(Common::Reset(ResetKind::StreamState)));
    }
}

#[test]
fn append_row_4_key_conflict_or_receipt_evicted_fences() {
    let conflict = producer(&ProducerAnswer::KeyConflict { epoch: epoch(3) });
    assert_eq!(append(&conflict), common(Common::Reset(ResetKind::Fenced)));
    let evicted = producer(&ProducerAnswer::ReceiptEvicted {
        epoch: CurrentEpoch::new(epoch(3)),
        seq: seq(9),
        incarnation: token(),
    });
    assert_eq!(append(&evicted), common(Common::Reset(ResetKind::Fenced)));
}

#[test]
fn append_row_5_producer_expired_claims_own_plus_one() {
    let raw = producer(&ProducerAnswer::ProducerExpired { epoch: epoch(3) });
    assert_eq!(append(&raw), AppendDecision::ContinuationClaim);
}

#[test]
fn append_row_6_epoch_closed_or_another_epoch_403_fences() {
    let by_claim =
        ProducerAnswer::EpochClosed(EpochClosure::by_claim(epoch(4), closed(3, 2)).unwrap());
    let by_history = ProducerAnswer::EpochClosed(EpochClosure::by_history(closed(3, 3)));
    let stale = ProducerAnswer::StaleEpoch { epoch: epoch(5) };
    for answer in [by_claim, by_history, stale] {
        assert_eq!(
            append(&producer(&answer)),
            common(Common::Reset(ResetKind::Fenced)),
            "{answer:?}"
        );
    }
    let raw = producer(&ProducerAnswer::EpochClosed(EpochClosure::by_history(
        closed(3, 3),
    )));
    assert!(matches!(
        raw.parse(&keyed(4)).typed,
        Ok(Typed::Producer(ProducerAnswer::EpochClosed(c))) if c.by() == ClosedBy::History
    ));
}

#[test]
fn append_row_7_expected_seq_means_lost_state() {
    let gap = producer(&ProducerAnswer::SeqGap {
        expected: seq(2),
        received: seq(4),
    });
    assert_eq!(append(&gap), common(Common::Reset(ResetKind::StreamState)));
    let unclaimed = producer(&ProducerAnswer::EpochNotClaimed);
    assert_eq!(
        append(&unclaimed),
        common(Common::Reset(ResetKind::StreamState))
    );
}

#[test]
fn append_row_8_a_closed_stream_resets_stream_state() {
    let mut raw = status(StreamClosedConflict::STATUS);
    StreamClosedConflict {
        final_offset: off(500),
    }
    .render(raw.emit());
    assert_eq!(append(&raw), common(Common::Reset(ResetKind::StreamState)));
}

#[test]
fn append_row_9_a_412_or_404_sends_head() {
    let mut mismatch = status(IncarnationMismatch::STATUS);
    IncarnationMismatch {
        current: Some(token()),
    }
    .render(mismatch.emit());
    assert_eq!(
        append(&mismatch),
        common(Common::Head(HeadReason::Precondition))
    );
    assert_eq!(
        append(&status(404)),
        common(Common::Head(HeadReason::Precondition))
    );
}

#[test]
fn append_row_10_a_typed_413_lowers_the_limit() {
    let mut raw = status(SizeLimit::STATUS);
    SizeLimit::Request(1024).render(raw.emit());
    assert_eq!(append(&raw), common(Common::LowerLimit { bytes: 1024 }));
}

#[test]
fn append_row_11_an_untyped_413_sends_head() {
    assert_eq!(
        append(&status(413)),
        common(Common::Head(HeadReason::TooLarge))
    );
}

#[test]
fn append_row_12_a_401_rereads_the_token() {
    assert_eq!(append(&status(401)), common(Common::Reauthorize));
}

#[test]
fn append_row_13_a_402_or_an_authorizers_403_blocks() {
    assert_eq!(
        append(&status(402)),
        common(Common::Blocked(BlockedReason::PaymentRequired))
    );
    assert_eq!(
        append(&status(403)),
        common(Common::Blocked(BlockedReason::Forbidden))
    );
}

#[test]
fn append_row_14_a_redirect_is_resent_then_blocked() {
    let mut raw = status(307);
    raw.header(LOCATION, "http://elsewhere/s");
    assert_eq!(
        append(&raw),
        common(Common::Redirect {
            location: Some("http://elsewhere/s".to_owned())
        })
    );
}

#[test]
fn append_row_15_no_answer_5xx_429_408_resend_identically() {
    let lost = Exchange::NoAnswer(NoAnswer::Lost {
        reason: "reset".to_owned(),
    });
    assert_eq!(
        decide_append(&sealed(), &lost),
        common(Common::Resend {
            retry_after: None,
            fresh: true
        })
    );
    for code in [500, 503, 429, 408] {
        let mut raw = status(code);
        raw.header(RETRY_AFTER, "3");
        assert_eq!(
            append(&raw),
            common(Common::Resend {
                retry_after: Some(Duration::from_secs(3)),
                fresh: false
            }),
            "{code}"
        );
    }
}

#[test]
fn append_row_16_any_other_status_blocks_rejected() {
    for code in [400, 405, 409, 410, 418, 200, 204] {
        assert_eq!(
            append(&status(code)),
            common(Common::Blocked(BlockedReason::Rejected { status: code })),
            "{code}"
        );
    }
    // A malformed producer header is a protocol violation, whatever the status.
    let mut raw = status(403);
    raw.header("producer-epoch", "x");
    raw.header("producer-outcome", "key-conflict");
    assert_eq!(
        append(&raw),
        common(Common::Blocked(BlockedReason::Rejected { status: 403 }))
    );
}

#[test]
fn claim_row_1_a_receipt_wins_and_names_what_it_closed() {
    for delivery in [Delivery::Applied, Delivery::Repeat] {
        let answer = receipt(delivery, 4, 0, 300, 337, Some(closed(3, 6)));
        let ProducerAnswer::Receipt { receipt, .. } = answer else {
            unreachable!()
        };
        assert_eq!(claim(&producer(&answer)), ClaimDecision::Won {
            receipt,
            closed: Some(closed(3, 6)),
            delivery,
        });
    }
}

#[test]
fn claim_row_2_any_epoch_403_or_receipt_evicted_loses() {
    let answers = [
        ProducerAnswer::KeyConflict { epoch: epoch(4) },
        ProducerAnswer::ProducerExpired { epoch: epoch(7) },
        ProducerAnswer::StaleEpoch { epoch: epoch(8) },
        ProducerAnswer::EpochClosed(EpochClosure::by_claim(epoch(9), closed(4, 0)).unwrap()),
        ProducerAnswer::ReceiptEvicted {
            epoch: CurrentEpoch::new(epoch(4)),
            seq: seq(2),
            incarnation: token(),
        },
    ];
    let named = [4, 7, 8, 9, 4];
    for (answer, named) in answers.iter().zip(named) {
        assert_eq!(
            claim(&producer(answer)),
            ClaimDecision::Lost {
                epoch: Some(epoch(named))
            },
            "{answer:?}"
        );
    }
}

#[test]
fn claim_rows_otherwise_follow_the_append_rules() {
    assert_eq!(
        claim(&status(503)),
        ClaimDecision::Common(Common::Resend {
            retry_after: None,
            fresh: false
        })
    );
    assert_eq!(
        claim(&status(402)),
        ClaimDecision::Common(Common::Blocked(BlockedReason::PaymentRequired))
    );
    assert_eq!(
        claim(&status(404)),
        ClaimDecision::Common(Common::Head(HeadReason::Precondition))
    );
}
