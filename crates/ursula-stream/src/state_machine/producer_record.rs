//! The producer record and the evaluation of a producer write (#581
//! section 2.3).
//!
//! [`evaluate`] is the table of section 2.3.1 as a pure function of the
//! record, the stream's epoch floor and the write's key: the first row that
//! matches decides. Only a claim (rows 2 and 6) and the next seq (row 12)
//! change the record, through [`ProducerState::claimed`] and
//! [`ProducerState::advance`]. The epoch is checked before deduplication
//! (rows 4 to 7 before rows 8 to 13), so a write of an older epoch is never
//! answered as a repeat. Row 11, a record fenced by a history event, comes
//! with history identity.
//!
//! The epoch floor (G2b) binds every write, with or without
//! `Stream-Incarnation`: no record is ever created at, or moved to, an epoch
//! at or below the stream's `retired_epoch_watermark`. Every record's epoch is
//! therefore above the floor when it opens, so an epoch that was closed or
//! forgotten never opens again. Such a write answers `producer-expired` with
//! the floor, and the producer claims above it.

use super::ClosedBy;
use super::ClosedEpoch;
use super::ProducerAck;
use super::ProducerDelivery;
use super::ProducerReceipt;
use super::ProducerRefusal;
use super::ProducerState;
use super::WriteDigest;
use super::WriteReceipt;

/// The key `(epoch, seq)` of one producer write, with its identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteKey {
    pub(crate) epoch: u64,
    pub(crate) seq: u64,
    pub(crate) digest: Option<WriteDigest>,
}

/// The decision for one producer write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Evaluation {
    /// Rows 2 and 6: the write claims its epoch, closing `closes`.
    Claim { closes: Option<ClosedEpoch> },
    /// Row 12: the next seq of the current epoch.
    Next,
    /// Rows 9 and 10: the key applied before.
    Repeat(ProducerAck),
    /// Rows 1, 3 to 5, 7, 8 and 13.
    Refuse(Refusal),
}

/// A refused producer write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Rows 1, 4, 5, 7 and 8.
    Producer(ProducerRefusal),
    /// Rows 3 and 13: `409` with `Producer-Expected-Seq`.
    SeqGap { expected: u64, received: u64 },
}

/// `producer-expired` with the floor when `epoch` is at or below it: an
/// epoch the stream may have forgotten never opens.
fn at_or_below_floor(floor: Option<u64>, epoch: u64) -> Option<Evaluation> {
    let floor = floor.filter(|floor| epoch <= *floor)?;
    Some(Evaluation::Refuse(Refusal::Producer(
        ProducerRefusal::Expired { epoch: floor },
    )))
}

/// Evaluates the write `key` against the producer's record `record` (after
/// idle retirement) and the stream's epoch floor `floor`.
pub(crate) fn evaluate(
    record: Option<&ProducerState>,
    floor: Option<u64>,
    key: WriteKey,
) -> Evaluation {
    let Some(record) = record else {
        // Row 1, the epoch floor.
        if let Some(refused) = at_or_below_floor(floor, key.epoch) {
            return refused;
        }
        // Rows 2 and 3.
        return if key.seq == 0 {
            Evaluation::Claim { closes: None }
        } else {
            Evaluation::Refuse(Refusal::SeqGap {
                expected: 0,
                received: key.seq,
            })
        };
    };
    if key.epoch < record.epoch {
        // Rows 4 and 5: the record remembers only the epoch its own epoch's
        // claim closed. Any older epoch is the base protocol's stale epoch,
        // typed `stale-epoch` (#581 addendum 2).
        let refusal = match record.closed_epoch {
            Some(closed) if closed.epoch == key.epoch => ProducerRefusal::EpochClosed {
                epoch: record.epoch,
                by: ClosedBy::Claim,
                closed,
            },
            _ => ProducerRefusal::StaleEpoch {
                epoch: record.epoch,
            },
        };
        return Evaluation::Refuse(Refusal::Producer(refusal));
    }
    if key.epoch > record.epoch {
        // The floor first: a record never moves to an epoch at or below it.
        if let Some(refused) = at_or_below_floor(floor, key.epoch) {
            return refused;
        }
        // Rows 6 and 7.
        return if key.seq == 0 {
            Evaluation::Claim {
                closes: Some(ClosedEpoch {
                    epoch: record.epoch,
                    seq: record.last_seq,
                }),
            }
        } else {
            Evaluation::Refuse(Refusal::Producer(ProducerRefusal::EpochNotClaimed))
        };
    }
    // Row 8: the server holds the identity of the claim and of the newest
    // write; an absent digest matches only an absent one.
    let stored = if key.seq == 0 {
        Some(record.claim_digest)
    } else if key.seq == record.last_seq {
        Some(record.newest.digest)
    } else {
        None
    };
    if stored.is_some_and(|stored| stored != key.digest) {
        return Evaluation::Refuse(Refusal::Producer(ProducerRefusal::KeyConflict {
            epoch: record.epoch,
        }));
    }
    // Rows 9 and 10. A repeat of the claim reports what it closed.
    let closed_epoch = if key.seq == 0 {
        record.closed_epoch
    } else {
        None
    };
    let repeat = |delivery| {
        Evaluation::Repeat(ProducerAck {
            epoch: record.epoch,
            seq: record.last_seq,
            closed_epoch,
            delivery,
        })
    };
    if key.seq == record.last_seq {
        return repeat(ProducerDelivery::Repeat(record.newest.receipt));
    }
    if key.seq < record.last_seq {
        return repeat(ProducerDelivery::Evicted);
    }
    // Rows 12 and 13. `last_seq < key.seq`, so the increment cannot overflow.
    let expected = record.last_seq.saturating_add(1);
    if key.seq == expected {
        Evaluation::Next
    } else {
        Evaluation::Refuse(Refusal::SeqGap {
            expected,
            received: key.seq,
        })
    }
}

impl ProducerState {
    /// The record after a claim of `key.epoch` applied (rows 2 and 6).
    pub(crate) fn claimed(
        key: WriteKey,
        closes: Option<ClosedEpoch>,
        receipt: WriteReceipt,
        now_ms: u64,
    ) -> Self {
        Self {
            epoch: key.epoch,
            last_seq: key.seq,
            claim_digest: key.digest,
            newest: ProducerReceipt {
                receipt,
                digest: key.digest,
            },
            closed_epoch: closes,
            last_seen_ms: now_ms,
        }
    }

    /// The record after the next seq applied (row 12).
    pub(crate) fn advance(&mut self, key: WriteKey, receipt: WriteReceipt, now_ms: u64) {
        self.last_seq = key.seq;
        self.newest = ProducerReceipt {
            receipt,
            digest: key.digest,
        };
        self.last_seen_ms = now_ms;
    }

    /// The answer to the write this record's newest write is, applied by
    /// this command.
    pub(crate) fn applied_ack(&self) -> ProducerAck {
        ProducerAck {
            epoch: self.epoch,
            seq: self.last_seq,
            closed_epoch: if self.last_seq == 0 {
                self.closed_epoch
            } else {
                None
            },
            delivery: ProducerDelivery::Applied(self.newest.receipt),
        }
    }
}
