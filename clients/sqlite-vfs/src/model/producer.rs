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
    /// Rows 2, 6 and 12: the write applies. `closes` is the epoch a claim closes (row 6).
    Apply { closes: Option<ClosedEpoch> },
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

    fn apply(row: u8, closes: Option<ClosedEpoch>) -> Self {
        Self {
            row,
            decision: Decision::Apply { closes },
        }
    }
}

/// Evaluates producer write `write` (`(P, E, N)` with digest `d`) against P's record after idle
/// retirement, the stream's epoch floor `watermark`, whether the write is tokened, and the group's
/// history id. `incarnation` is the stream's token, which receipts carry. Rows match in order.
pub(crate) fn evaluate(
    record: Option<&ProducerRecord>,
    watermark: Option<Epoch>,
    write: &ProducerWrite,
    tokened: bool,
    group_history: u64,
    incarnation: Incarnation,
) -> Evaluation {
    let (e, n, d) = (write.epoch, write.seq, write.digest);
    let Some(r) = record else {
        if let Some(w) = watermark.filter(|w| tokened && e <= *w) {
            return Evaluation::answer(1, ProducerAnswer::ProducerExpired { epoch: w });
        }
        if n == Seq::ZERO {
            return Evaluation::apply(2, None);
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
        return Evaluation::answer(5, ProducerAnswer::ProducerExpired { epoch: current });
    }
    if e > current {
        if n == Seq::ZERO {
            return Evaluation::apply(
                6,
                Some(ClosedEpoch {
                    epoch: current,
                    seq: r.last,
                }),
            );
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
        return Evaluation::apply(12, None);
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
            last_seen_ms: 0,
        }
    }

    fn eval(
        record: Option<&ProducerRecord>,
        watermark: Option<u64>,
        write: &ProducerWrite,
        tokened: bool,
    ) -> Evaluation {
        evaluate(
            record,
            watermark.map(epoch),
            write,
            tokened,
            HISTORY,
            token(),
        )
    }

    fn answered(evaluation: &Evaluation) -> &ProducerAnswer {
        match &evaluation.decision {
            Decision::Answer(answer) => answer,
            Decision::Apply { .. } => panic!("row {} applied", evaluation.row),
        }
    }

    #[test]
    fn row_1_the_floor_refuses_tokened_claims_and_appends() {
        for n in [0, 3] {
            for e in [2, 3] {
                let evaluation = eval(None, Some(3), &write(e, n, digest(1)), true);
                assert_eq!(evaluation.row, 1);
                assert_eq!(answered(&evaluation), &ProducerAnswer::ProducerExpired {
                    epoch: epoch(3)
                });
            }
        }
        // Above the floor, a tokened claim opens an epoch the stream never forgot (row 2).
        assert_eq!(eval(None, Some(3), &write(4, 0, digest(1)), true).row, 2);
    }

    #[test]
    fn row_1_binds_only_tokened_writes() {
        // An untokened claim at the floor keeps the base protocol's rule: a new producer (row 2).
        let evaluation = eval(None, Some(3), &write(3, 0, digest(1)), false);
        assert_eq!(evaluation.row, 2);
        assert_eq!(evaluation.decision, Decision::Apply { closes: None });
    }

    #[test]
    fn row_2_a_first_claim_applies() {
        let evaluation = eval(None, None, &write(0, 0, None), true);
        assert_eq!(evaluation.row, 2);
        assert_eq!(evaluation.decision, Decision::Apply { closes: None });
    }

    #[test]
    fn row_3_a_seq_without_a_record_expects_seq_0() {
        let evaluation = eval(None, None, &write(1, 4, None), true);
        assert_eq!(evaluation.row, 3);
        assert_eq!(answered(&evaluation), &ProducerAnswer::SeqGap {
            expected: Seq::ZERO,
            received: seq(4),
        });
    }

    #[test]
    fn row_4_the_closed_epoch_reports_its_last_seq() {
        for n in [0, 6, 7] {
            let evaluation = eval(Some(&record(2)), None, &write(4, n, digest(1)), true);
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
    fn row_5_an_epoch_two_claims_old_is_expired() {
        let evaluation = eval(Some(&record(2)), None, &write(3, 1, None), true);
        assert_eq!(evaluation.row, 5);
        assert_eq!(answered(&evaluation), &ProducerAnswer::ProducerExpired {
            epoch: epoch(5)
        });
    }

    #[test]
    fn row_6_a_later_claim_closes_the_epoch_at_its_last_seq() {
        let evaluation = eval(Some(&record(2)), None, &write(9, 0, digest(3)), true);
        assert_eq!(evaluation.row, 6);
        assert_eq!(evaluation.decision, Decision::Apply {
            closes: Some(ClosedEpoch {
                epoch: epoch(5),
                seq: seq(2),
            }),
        });
    }

    #[test]
    fn row_7_a_seq_of_an_unclaimed_later_epoch_expects_seq_0() {
        let evaluation = eval(Some(&record(2)), None, &write(6, 1, None), true);
        assert_eq!(evaluation.row, 7);
        assert_eq!(answered(&evaluation), &ProducerAnswer::EpochNotClaimed);
    }

    #[test]
    fn row_8_another_identity_under_a_kept_key_conflicts() {
        // The claim's identity (digest 1), against another digest and against none.
        for d in [digest(2), None] {
            let evaluation = eval(Some(&record(2)), None, &write(5, 0, d), true);
            assert_eq!(evaluation.row, 8, "{d:?}");
            assert_eq!(answered(&evaluation), &ProducerAnswer::KeyConflict {
                epoch: epoch(5)
            });
        }
        // The newest write's identity (digest 9).
        for d in [digest(1), None] {
            assert_eq!(
                eval(Some(&record(2)), None, &write(5, 2, d), true).row,
                8,
                "{d:?}"
            );
        }
        // A claim without a digest: a repeat with one conflicts too.
        let mut bare = record(0);
        bare.claim = None;
        bare.newest_digest = None;
        assert_eq!(
            eval(Some(&bare), None, &write(5, 0, digest(1)), true).row,
            8
        );
        assert_eq!(eval(Some(&bare), None, &write(5, 0, None), true).row, 9);
    }

    #[test]
    fn row_9_a_repeat_of_the_newest_write_gets_its_receipt() {
        let evaluation = eval(Some(&record(2)), None, &write(5, 2, digest(9)), true);
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
        let evaluation = eval(Some(&record(0)), None, &write(5, 0, digest(1)), true);
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
        let evaluation = eval(Some(&record(4)), None, &write(5, 2, digest(7)), true);
        assert_eq!(evaluation.row, 10);
        assert_eq!(answered(&evaluation), &ProducerAnswer::ReceiptEvicted {
            epoch: CurrentEpoch::new(epoch(5)),
            seq: seq(4),
            incarnation: token(),
        });
        // The claim's repeat, once later seqs applied, still reports what it closed.
        let claim = eval(Some(&record(4)), None, &write(5, 0, digest(1)), true);
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
        let evaluation = eval(Some(&fenced), None, &write(5, 5, digest(9)), true);
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
        assert_eq!(
            eval(Some(&fenced), None, &write(5, 4, digest(9)), true).row,
            9
        );
        assert_eq!(eval(Some(&fenced), None, &write(5, 3, None), true).row, 10);
    }

    #[test]
    fn row_12_the_next_seq_applies() {
        let evaluation = eval(Some(&record(2)), None, &write(5, 3, None), true);
        assert_eq!(evaluation.row, 12);
        assert_eq!(evaluation.decision, Decision::Apply { closes: None });
    }

    #[test]
    fn row_13_a_seq_past_the_next_one_names_the_expected_seq() {
        let evaluation = eval(Some(&record(2)), None, &write(5, 5, None), true);
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
        assert_eq!(
            eval(Some(&closed_at), None, &write(4, 6, None), true).row,
            4
        );
    }
}
