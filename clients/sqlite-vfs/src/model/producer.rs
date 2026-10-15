//! The producer record and the evaluation of a producer write, #581 §2.3 and §2.3.1, as the text
//! states them. [`evaluate`] returns the row that decided, so tests cite rows by number.

use ursula_headers::BodyDigest;
use ursula_headers::ClosedEpoch;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::EpochClosure;
use ursula_headers::Incarnation;
use ursula_headers::ProducerAnswer;
use ursula_headers::ProducerWrite;
use ursula_headers::Receipt;
use ursula_headers::Seq;

/// A record whose producer wrote nothing for this long is retired at its producer's next write
/// (#581 §2.7).
pub(crate) const IDLE_RETIREMENT_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Per stream and producer (#581 §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProducerRecord {
    /// The current epoch, with the epoch its claim closed and that epoch's last seq.
    pub(crate) epoch: CurrentEpoch,
    /// The highest applied seq of the current epoch.
    pub(crate) last: Seq,
    /// The digest the claim of the current epoch carried.
    pub(crate) claim: Option<BodyDigest>,
    /// The receipt of the write at `last`.
    pub(crate) newest: Receipt,
    /// The digest the write at `last` carried.
    pub(crate) newest_digest: Option<BodyDigest>,
    /// The group's history id when the claim of the current epoch applied (G2).
    pub(crate) history: u64,
    /// Whether the record was opened at or below the stream's floor, which only an untokened claim
    /// can do, and no claim above the floor has applied since: tokened writes on such a record are
    /// refused (the floor decision on #592).
    pub(crate) below_floor: bool,
    pub(crate) last_seen_ms: u64,
}

impl ProducerRecord {
    /// Whether a history event happened since the claim of the current epoch (#581 §4.6).
    fn history_fenced(&self, group_history: u64) -> bool {
        self.history != group_history
    }

    /// The record after a claim of `write.epoch` applied with `receipt` (rows 2 and 6).
    pub(crate) fn claimed(
        write: &ProducerWrite,
        closes: Option<ClosedEpoch>,
        below_floor: bool,
        receipt: Receipt,
        group_history: u64,
        now_ms: u64,
    ) -> Self {
        let epoch = match closes {
            None => CurrentEpoch::new(write.epoch),
            Some(closed) => CurrentEpoch::closing(write.epoch, closed)
                .expect("row 6 claims an epoch above the one it closes"),
        };
        Self {
            epoch,
            last: Seq::ZERO,
            claim: write.digest,
            newest: receipt,
            newest_digest: write.digest,
            history: group_history,
            below_floor,
            last_seen_ms: now_ms,
        }
    }

    /// The record after the next seq of its epoch applied with `receipt` (row 12).
    pub(crate) fn advanced(&mut self, write: &ProducerWrite, receipt: Receipt, now_ms: u64) {
        self.last = write.seq;
        self.newest = receipt;
        self.newest_digest = write.digest;
        self.last_seen_ms = now_ms;
    }

    /// The epoch an answer about seq `seq` of the current epoch names: with the closed epoch on
    /// the claim's answers (rows 9 and 10 when `N = 0`).
    fn epoch_for(&self, seq: Seq) -> CurrentEpoch {
        if seq == Seq::ZERO {
            self.epoch
        } else {
            CurrentEpoch::new(self.epoch.epoch())
        }
    }
}

/// What a row decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Rows 2, 6 and 12: the write applies. `closes` is the epoch a claim closes (row 6), and
    /// `below_floor` whether a claim's epoch is at or below the floor, for its record's marker
    /// (always false for row 12).
    Apply {
        closes: Option<ClosedEpoch>,
        below_floor: bool,
    },
    /// Every other row: this answer, and no state change.
    Answer(ProducerAnswer),
}

/// The row of §2.3.1 that matched first, and its decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Evaluation {
    pub(crate) row: u8,
    pub(crate) decision: Decision,
}

impl Evaluation {
    fn answer(row: u8, answer: ProducerAnswer) -> Self {
        Self {
            row,
            decision: Decision::Answer(answer),
        }
    }

    fn apply(row: u8, closes: Option<ClosedEpoch>, below_floor: bool) -> Self {
        Self {
            row,
            decision: Decision::Apply {
                closes,
                below_floor,
            },
        }
    }
}

/// Evaluates producer write `write` (`(P, E, N)` with digest `d`) against P's record after idle
/// retirement, the stream's epoch floor `watermark` and the group's history id. `tokened` is
/// whether the write carries the stream's token, and `incarnation` is that token, which receipts
/// carry. Rows match in order.
///
/// The floor decision on #592 (superseding addendum 3 item 2) scopes row 1 to tokened writes. A
/// tokened write is refused `producer-expired` when it has no record and its epoch is at or below
/// the floor, or when its record was claimed at or below the floor, unless it claims above the
/// floor. An untokened writer keeps the base protocol's rule that a forgotten producer is new, so
/// its claim may open a record at or below the floor, marked as such. A record opened above the
/// floor stays so: its claims of later epochs are not checked against the floor (row 6 or 7). A
/// marked record is cleared only by a claim above the floor, tokened or not, and a claim on a
/// marked record closes no epoch.
pub(crate) fn evaluate(
    record: Option<&ProducerRecord>,
    watermark: Option<Epoch>,
    write: &ProducerWrite,
    tokened: bool,
    group_history: u64,
    incarnation: Incarnation,
) -> Evaluation {
    let (e, n, d) = (write.epoch, write.seq, write.digest);
    let at_or_below_floor = watermark.is_some_and(|w| e <= w);
    let expired = match record {
        None => at_or_below_floor,
        Some(r) => {
            let claims_above_floor = n == Seq::ZERO && e > r.epoch.epoch() && !at_or_below_floor;
            r.below_floor && !claims_above_floor
        }
    };
    if let Some(w) = watermark.filter(|_| tokened && expired) {
        return Evaluation::answer(1, ProducerAnswer::ProducerExpired { epoch: w });
    }
    let Some(r) = record else {
        if n == Seq::ZERO {
            return Evaluation::apply(2, None, at_or_below_floor);
        }
        return Evaluation::answer(3, ProducerAnswer::SeqGap {
            expected: Seq::ZERO,
            received: n,
        });
    };
    let current = r.epoch.epoch();
    if e < current {
        if let Some(closed) = r.epoch.closed_epoch().filter(|closed| closed.epoch == e) {
            let closure = EpochClosure::by_claim(current, closed)
                .expect("the record's epoch is above the epoch its claim closed");
            return Evaluation::answer(4, ProducerAnswer::EpochClosed(closure));
        }
        return Evaluation::answer(5, ProducerAnswer::StaleEpoch { epoch: current });
    }
    if e > current {
        if n == Seq::ZERO {
            // A claim on a record opened below the floor closes nothing: the epoch it would close
            // may be one the stream forgot, so no answer reports it as closed (#592).
            let closes = (!r.below_floor).then_some(ClosedEpoch {
                epoch: current,
                seq: r.last,
            });
            return Evaluation::apply(6, closes, r.below_floor && at_or_below_floor);
        }
        return Evaluation::answer(7, ProducerAnswer::EpochNotClaimed);
    }
    if (n == Seq::ZERO && d != r.claim) || (n == r.last && d != r.newest_digest) {
        return Evaluation::answer(8, ProducerAnswer::KeyConflict { epoch: e });
    }
    if n == r.last {
        return Evaluation::answer(9, ProducerAnswer::Receipt {
            delivery: Delivery::Repeat,
            epoch: r.epoch_for(n),
            seq: r.last,
            receipt: r.newest,
            incarnation,
        });
    }
    if n < r.last {
        return Evaluation::answer(10, ProducerAnswer::ReceiptEvicted {
            epoch: r.epoch_for(n),
            seq: r.last,
            incarnation,
        });
    }
    if r.history_fenced(group_history) {
        let closure = EpochClosure::by_history(ClosedEpoch {
            epoch: e,
            seq: r.last,
        });
        return Evaluation::answer(11, ProducerAnswer::EpochClosed(closure));
    }
    let next = r
        .last
        .get()
        .checked_add(1)
        .and_then(|next| Seq::try_from(next).ok());
    if Some(n) == next {
        return Evaluation::apply(12, None, false);
    }
    Evaluation::answer(13, ProducerAnswer::SeqGap {
        expected: next.unwrap_or(Seq::MAX),
        received: n,
    })
}

#[cfg(test)]
mod tests {
    use ursula_headers::BodyDigest;
    use ursula_headers::ClosedBy;
    use ursula_headers::ClosedEpoch;
    use ursula_headers::CurrentEpoch;
    use ursula_headers::Delivery;
    use ursula_headers::Epoch;
    use ursula_headers::Incarnation;
    use ursula_headers::Offset;
    use ursula_headers::ProducerAnswer;
    use ursula_headers::ProducerId;
    use ursula_headers::ProducerWrite;
    use ursula_headers::Receipt;
    use ursula_headers::Seq;

    use super::Decision;
    use super::Evaluation;
    use super::ProducerRecord;
    use super::evaluate;
    use crate::wire::PRODUCER_ID;

    const HISTORY: u64 = 77;

    fn token() -> Incarnation {
        Incarnation::try_from((1 << 52) | 5).unwrap()
    }

    fn epoch(e: u64) -> Epoch {
        Epoch::try_from(e).unwrap()
    }

    fn seq(n: u64) -> Seq {
        Seq::try_from(n).unwrap()
    }

    fn digest(b: u8) -> Option<BodyDigest> {
        Some(BodyDigest::from_bytes([b; 32]))
    }

    fn write(e: u64, n: u64, d: Option<BodyDigest>) -> ProducerWrite {
        ProducerWrite {
            id: ProducerId::new(PRODUCER_ID).unwrap(),
            epoch: epoch(e),
            seq: seq(n),
            digest: d,
        }
    }

    fn receipt(start: u64, next: u64) -> Receipt {
        Receipt::new(Offset::from_u64(start), Offset::from_u64(next), false).unwrap()
    }

    /// The record of epoch 5 after its claim (digest 1) and seqs up to `last` (newest digest 9),
    /// whose claim closed epoch 4 at seq 6.
    fn record(last: u64) -> ProducerRecord {
        let closed = ClosedEpoch {
            epoch: epoch(4),
            seq: seq(6),
        };
        ProducerRecord {
            epoch: CurrentEpoch::closing(epoch(5), closed).unwrap(),
            last: seq(last),
            claim: digest(1),
            newest: receipt(100, 120),
            newest_digest: if last == 0 { digest(1) } else { digest(9) },
            history: HISTORY,
            below_floor: false,
            last_seen_ms: 0,
        }
    }

    /// A tokened write's evaluation.
    fn eval(
        record: Option<&ProducerRecord>,
        watermark: Option<u64>,
        write: &ProducerWrite,
    ) -> Evaluation {
        evaluate(record, watermark.map(epoch), write, true, HISTORY, token())
    }

    /// An untokened write's evaluation.
    fn eval_untokened(
        record: Option<&ProducerRecord>,
        watermark: Option<u64>,
        write: &ProducerWrite,
    ) -> Evaluation {
        evaluate(record, watermark.map(epoch), write, false, HISTORY, token())
    }

    fn applied(closes: Option<ClosedEpoch>, below_floor: bool) -> Decision {
        Decision::Apply {
            closes,
            below_floor,
        }
    }

    fn answered(evaluation: &Evaluation) -> &ProducerAnswer {
        match &evaluation.decision {
            Decision::Answer(answer) => answer,
            Decision::Apply { .. } => panic!("row {} applied", evaluation.row),
        }
    }

    #[test]
    fn floor_decision_row_1_refuses_a_tokened_write_without_a_record_at_or_below_the_floor() {
        for n in [0, 3] {
            for e in [2, 3] {
                let evaluation = eval(None, Some(3), &write(e, n, digest(1)));
                assert_eq!(evaluation.row, 1);
                assert_eq!(answered(&evaluation), &ProducerAnswer::ProducerExpired {
                    epoch: epoch(3)
                });
            }
        }
        // Above the floor, a claim opens an epoch the stream never forgot (row 2).
        let evaluation = eval(None, Some(3), &write(4, 0, digest(1)));
        assert_eq!(evaluation.row, 2);
        assert_eq!(evaluation.decision, applied(None, false));
    }

    #[test]
    fn floor_decision_an_untokened_writer_without_a_record_is_a_new_producer() {
        // The base protocol's rule: a forgotten producer is new. Its claim opens a record marked
        // as claimed at or below the floor (row 2), and its seq gets the base gap (row 3).
        let evaluation = eval_untokened(None, Some(3), &write(3, 0, None));
        assert_eq!(evaluation.row, 2);
        assert_eq!(evaluation.decision, applied(None, true));
        assert_eq!(eval_untokened(None, Some(3), &write(2, 4, None)).row, 3);
        let above = eval_untokened(None, Some(3), &write(4, 0, None));
        assert_eq!(above.decision, applied(None, false));
    }

    #[test]
    fn floor_decision_a_tokened_write_on_a_record_claimed_below_the_floor_is_expired() {
        let mut below = record(2);
        below.below_floor = true;
        // Repeats, the next seq, a later seq and a claim at or below the floor are all refused.
        for w in [
            write(5, 2, digest(9)),
            write(5, 3, None),
            write(6, 1, None),
            write(6, 0, digest(3)),
            write(7, 0, digest(3)),
        ] {
            let evaluation = eval(Some(&below), Some(7), &w);
            assert_eq!(evaluation.row, 1, "{w:?}");
            assert_eq!(answered(&evaluation), &ProducerAnswer::ProducerExpired {
                epoch: epoch(7)
            });
        }
        // A claim above the floor applies and clears the marker (row 6).
        let evaluation = eval(Some(&below), Some(7), &write(8, 0, digest(3)));
        assert_eq!(evaluation.row, 6);
        assert_eq!(evaluation.decision, applied(None, false));
        // An untokened claim keeps the marker at or below the floor and clears it above.
        for (e, below_floor) in [(6, true), (7, true), (8, false)] {
            let evaluation = eval_untokened(Some(&below), Some(7), &write(e, 0, digest(3)));
            assert_eq!(evaluation.row, 6, "epoch {e}");
            assert_eq!(evaluation.decision, applied(None, below_floor), "epoch {e}");
        }
        // Untokened, the record is an ordinary one.
        assert_eq!(
            eval_untokened(Some(&below), Some(7), &write(5, 3, None)).row,
            12
        );
        assert_eq!(
            eval_untokened(Some(&below), Some(7), &write(5, 2, digest(9))).row,
            9
        );
    }

    #[test]
    fn a_below_floor_claim_closes_no_epoch() {
        // #592: tokened or not, a claim on a record opened below the floor closes nothing, so the
        // record it leaves names no closed epoch and a resend in the replaced epoch is stale.
        let mut below = record(2);
        below.below_floor = true;
        for evaluation in [
            eval(Some(&below), Some(7), &write(8, 0, digest(3))),
            eval_untokened(Some(&below), Some(7), &write(6, 0, digest(3))),
        ] {
            let Decision::Apply {
                closes,
                below_floor,
            } = evaluation.decision
            else {
                panic!("{evaluation:?}");
            };
            assert_eq!((evaluation.row, closes), (6, None));
            let claimed = ProducerRecord::claimed(
                &write(8, 0, digest(3)),
                closes,
                below_floor,
                receipt(120, 130),
                HISTORY,
                0,
            );
            assert_eq!(claimed.epoch.closed_epoch(), None);
            let resend = eval_untokened(Some(&claimed), Some(7), &write(5, 3, None));
            assert_eq!(resend.row, 5);
            assert_eq!(answered(&resend), &ProducerAnswer::StaleEpoch {
                epoch: epoch(8)
            });
        }
    }

    #[test]
    fn floor_decision_a_live_record_claims_a_later_epoch_without_the_floor_check() {
        // Another producer's retirement raised the floor to 7, above this record's epoch 5.
        // Row 6 applies, and the record stays opened above the floor.
        for (e, below_floor) in [(6, false), (7, false), (8, false)] {
            let evaluation = eval(Some(&record(2)), Some(7), &write(e, 0, digest(3)));
            assert_eq!(evaluation.row, 6, "epoch {e}");
            assert_eq!(
                evaluation.decision,
                applied(
                    Some(ClosedEpoch {
                        epoch: epoch(5),
                        seq: seq(2),
                    }),
                    below_floor
                ),
                "epoch {e}"
            );
        }
        // Row 7 answers 400 Expected-Seq 0 again, below the floor too.
        let evaluation = eval(Some(&record(2)), Some(7), &write(6, 1, None));
        assert_eq!(evaluation.row, 7);
        assert_eq!(answered(&evaluation), &ProducerAnswer::EpochNotClaimed);
        // A write in the record's own epoch, below the floor, still applies or repeats.
        assert_eq!(eval(Some(&record(2)), Some(7), &write(5, 3, None)).row, 12);
        assert_eq!(
            eval(Some(&record(2)), Some(7), &write(5, 2, digest(9))).row,
            9
        );
    }

    #[test]
    fn row_2_a_first_claim_applies() {
        let evaluation = eval(None, None, &write(0, 0, None));
        assert_eq!(evaluation.row, 2);
        assert_eq!(evaluation.decision, applied(None, false));
    }

    #[test]
    fn row_3_a_seq_without_a_record_expects_seq_0() {
        let evaluation = eval(None, None, &write(1, 4, None));
        assert_eq!(evaluation.row, 3);
        assert_eq!(answered(&evaluation), &ProducerAnswer::SeqGap {
            expected: Seq::ZERO,
            received: seq(4),
        });
    }

    #[test]
    fn row_4_the_closed_epoch_reports_its_last_seq() {
        for n in [0, 6, 7] {
            let evaluation = eval(Some(&record(2)), None, &write(4, n, digest(1)));
            assert_eq!(evaluation.row, 4);
            let ProducerAnswer::EpochClosed(closure) = answered(&evaluation) else {
                panic!("{evaluation:?}");
            };
            assert_eq!(closure.by(), ClosedBy::Claim);
            assert_eq!(closure.epoch(), epoch(5));
            assert_eq!(closure.closed_epoch(), ClosedEpoch {
                epoch: epoch(4),
                seq: seq(6),
            });
        }
    }

    #[test]
    fn row_5_an_older_epoch_is_stale() {
        // Addendum 3 item 1: an epoch below the record's, other than the one its claim closed.
        for n in [0, 1] {
            let evaluation = eval(Some(&record(2)), None, &write(3, n, None));
            assert_eq!(evaluation.row, 5);
            assert_eq!(answered(&evaluation), &ProducerAnswer::StaleEpoch {
                epoch: epoch(5)
            });
        }
    }

    #[test]
    fn row_6_a_later_claim_closes_the_epoch_at_its_last_seq() {
        let evaluation = eval(Some(&record(2)), None, &write(9, 0, digest(3)));
        assert_eq!(evaluation.row, 6);
        assert_eq!(
            evaluation.decision,
            applied(
                Some(ClosedEpoch {
                    epoch: epoch(5),
                    seq: seq(2),
                }),
                false
            )
        );
    }

    #[test]
    fn row_7_a_seq_of_an_unclaimed_later_epoch_expects_seq_0() {
        let evaluation = eval(Some(&record(2)), None, &write(6, 1, None));
        assert_eq!(evaluation.row, 7);
        assert_eq!(answered(&evaluation), &ProducerAnswer::EpochNotClaimed);
    }

    #[test]
    fn row_8_another_identity_under_a_kept_key_conflicts() {
        // The claim's identity (digest 1), against another digest and against none.
        for d in [digest(2), None] {
            let evaluation = eval(Some(&record(2)), None, &write(5, 0, d));
            assert_eq!(evaluation.row, 8, "{d:?}");
            assert_eq!(answered(&evaluation), &ProducerAnswer::KeyConflict {
                epoch: epoch(5)
            });
        }
        // The newest write's identity (digest 9).
        for d in [digest(1), None] {
            assert_eq!(
                eval(Some(&record(2)), None, &write(5, 2, d)).row,
                8,
                "{d:?}"
            );
        }
        // A claim without a digest: a repeat with one conflicts too.
        let mut bare = record(0);
        bare.claim = None;
        bare.newest_digest = None;
        assert_eq!(eval(Some(&bare), None, &write(5, 0, digest(1))).row, 8);
        assert_eq!(eval(Some(&bare), None, &write(5, 0, None)).row, 9);
    }

    #[test]
    fn row_9_a_repeat_of_the_newest_write_gets_its_receipt() {
        let evaluation = eval(Some(&record(2)), None, &write(5, 2, digest(9)));
        assert_eq!(evaluation.row, 9);
        assert_eq!(answered(&evaluation), &ProducerAnswer::Receipt {
            delivery: Delivery::Repeat,
            epoch: CurrentEpoch::new(epoch(5)),
            seq: seq(2),
            receipt: receipt(100, 120),
            incarnation: token(),
        });
    }

    #[test]
    fn row_9_a_repeated_claim_reports_what_it_closed() {
        let evaluation = eval(Some(&record(0)), None, &write(5, 0, digest(1)));
        assert_eq!(evaluation.row, 9);
        let ProducerAnswer::Receipt { epoch: current, .. } = answered(&evaluation) else {
            panic!("{evaluation:?}");
        };
        assert_eq!(
            current.closed_epoch(),
            Some(ClosedEpoch {
                epoch: epoch(4),
                seq: seq(6),
            })
        );
    }

    #[test]
    fn row_10_an_older_write_is_receipt_evicted() {
        let evaluation = eval(Some(&record(4)), None, &write(5, 2, digest(7)));
        assert_eq!(evaluation.row, 10);
        assert_eq!(answered(&evaluation), &ProducerAnswer::ReceiptEvicted {
            epoch: CurrentEpoch::new(epoch(5)),
            seq: seq(4),
            incarnation: token(),
        });
        // The claim's repeat, once later seqs applied, still reports what it closed.
        let claim = eval(Some(&record(4)), None, &write(5, 0, digest(1)));
        assert_eq!(claim.row, 10);
        assert_eq!(answered(&claim), &ProducerAnswer::ReceiptEvicted {
            epoch: record(4).epoch,
            seq: seq(4),
            incarnation: token(),
        });
    }

    #[test]
    fn row_11_a_history_event_closes_the_epoch_at_its_last_seq() {
        let mut fenced = record(4);
        fenced.history = HISTORY + 1;
        let evaluation = eval(Some(&fenced), None, &write(5, 5, digest(9)));
        assert_eq!(evaluation.row, 11);
        let ProducerAnswer::EpochClosed(closure) = answered(&evaluation) else {
            panic!("{evaluation:?}");
        };
        assert_eq!(closure.by(), ClosedBy::History);
        assert_eq!(closure.closed_epoch(), ClosedEpoch {
            epoch: epoch(5),
            seq: seq(4),
        });
        // Rows 9 and 10 still answer a fenced record: those writes applied in the surviving history.
        assert_eq!(eval(Some(&fenced), None, &write(5, 4, digest(9))).row, 9);
        assert_eq!(eval(Some(&fenced), None, &write(5, 3, None)).row, 10);
    }

    #[test]
    fn row_12_the_next_seq_applies() {
        let evaluation = eval(Some(&record(2)), None, &write(5, 3, None));
        assert_eq!(evaluation.row, 12);
        assert_eq!(evaluation.decision, applied(None, false));
    }

    #[test]
    fn row_13_a_seq_past_the_next_one_names_the_expected_seq() {
        let evaluation = eval(Some(&record(2)), None, &write(5, 5, None));
        assert_eq!(evaluation.row, 13);
        assert_eq!(answered(&evaluation), &ProducerAnswer::SeqGap {
            expected: seq(3),
            received: seq(5),
        });
    }

    #[test]
    fn the_epoch_is_checked_before_deduplication() {
        // A write of the closed epoch at a seq it applied is refused (row 4), never a duplicate.
        let mut closed_at = record(0);
        closed_at.newest_digest = digest(1);
        assert_eq!(eval(Some(&closed_at), None, &write(4, 6, None)).row, 4);
    }
}
