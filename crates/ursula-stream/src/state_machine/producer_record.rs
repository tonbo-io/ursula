//! The producer record and the evaluation of a producer write (#581
//! section 2.3).
//!
//! [`evaluate`] is the table of section 2.3.1 as a pure function of the
//! record, the stream's epoch floor and the write's key: the first row that
//! matches decides. Only a claim (rows 2 and 6) and the next seq (row 12)
//! change the record, through [`ProducerState::claimed`] and
//! [`ProducerState::advance`]. The epoch is checked before deduplication
//! (rows 4 to 7 before rows 8 to 13), so a write of an older epoch is never
//! answered as a repeat.
//!
//! Row 11 is G2 (#581 section 4.6): a record keeps the group's history id
//! from when its epoch was claimed. Once a history event changed the
//! group's id, the record is history-fenced: its epoch is closed at its
//! surviving `last_seq`, a write above it answers `epoch-closed;by=history`,
//! and the producer claims a later epoch. A repeat at or below `last_seq`
//! still gets its receipt (rows 9 and 10): it applied in the history that
//! survived. The comparison is derived at each write, so the event itself
//! touches no record.
//!
//! The epoch floor (G2b) binds writes that carry `Stream-Incarnation`, the
//! same opt-in as G1 and G2 (#581 section 2.3.1 row 1, section 13 item 10,
//! and the decision on PR #592). A tokened write from a producer without a
//! record, at or below the stream's `retired_epoch_watermark`, answers
//! `producer-expired` with the floor (row 1). Without the token the base
//! protocol's rule holds: a forgotten producer is new, at any epoch.
//!
//! Such an untokened claim can open a record at or below the floor, which
//! may hold an epoch the stream forgot. Every record therefore carries
//! [`Opened`]: a tokened write on a record opened below the floor answers
//! `producer-expired` too, unless it claims above the floor, which marks the
//! record [`Opened::AboveFloor`]. A record opened above the floor holds
//! epochs above every epoch its producer ever had retired, so claims on it
//! (row 6) are not checked against the floor again: a retirement elsewhere
//! on the stream never refuses a live producer.

use super::ClosedBy;
use super::ClosedEpoch;
use super::IncarnationCheck;
use super::Opened;
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
    /// Rows 2 and 6: the write claims its epoch, closing `closes`, and the
    /// record is `opened` above or below the floor.
    Claim {
        closes: Option<ClosedEpoch>,
        opened: Opened,
    },
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
    /// Rows 1, 4, 5, 7, 8 and 11.
    Producer(ProducerRefusal),
    /// Rows 3 and 13: `409` with `Producer-Expected-Seq`.
    SeqGap { expected: u64, received: u64 },
}

/// Whether `epoch` is at or below the floor: an epoch the stream may have
/// forgotten.
fn at_or_below(floor: Option<u64>, epoch: u64) -> bool {
    floor.is_some_and(|floor| epoch <= floor)
}

fn expired(floor: u64) -> Evaluation {
    Evaluation::Refuse(Refusal::Producer(ProducerRefusal::Expired { epoch: floor }))
}

/// Evaluates the write `key` against the producer's record `record` (after
/// idle retirement), the stream's epoch floor `floor` and the group's
/// history id `history`. `check` says whether the write carried
/// `Stream-Incarnation`.
pub(crate) fn evaluate(
    record: Option<&ProducerState>,
    floor: Option<u64>,
    history: u64,
    key: WriteKey,
    check: IncarnationCheck,
) -> Evaluation {
    let tokened = check == IncarnationCheck::Matched;
    let Some(record) = record else {
        // Row 1, the epoch floor, for tokened writes.
        if tokened && let Some(floor) = floor.filter(|floor| key.epoch <= *floor) {
            return expired(floor);
        }
        // Rows 2 and 3. An untokened claim at or below the floor opens a
        // record below it.
        return if key.seq == 0 {
            Evaluation::Claim {
                closes: None,
                opened: if at_or_below(floor, key.epoch) {
                    Opened::BelowFloor
                } else {
                    Opened::AboveFloor
                },
            }
        } else {
            Evaluation::Refuse(Refusal::SeqGap {
                expected: 0,
                received: key.seq,
            })
        };
    };
    // A record opened below the floor may hold a forgotten epoch: a tokened
    // write on it is expired, unless it claims above the floor.
    if tokened
        && record.opened == Opened::BelowFloor
        && let Some(floor) = floor
        && !(key.seq == 0 && key.epoch > record.epoch && key.epoch > floor)
    {
        return expired(floor);
    }
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
        // Rows 6 and 7. A record opened above the floor stays above it; one
        // opened below rises above only with a claim above the floor. A
        // claim on a record opened below the floor closes nothing: the
        // epoch it would close may be one the stream forgot, whose seqs may
        // belong to another writer, so no answer may report it as closed.
        return if key.seq == 0 {
            Evaluation::Claim {
                closes: match record.opened {
                    Opened::AboveFloor => Some(ClosedEpoch {
                        epoch: record.epoch,
                        seq: record.last_seq,
                    }),
                    Opened::BelowFloor => None,
                },
                opened: if record.opened == Opened::AboveFloor || !at_or_below(floor, key.epoch) {
                    Opened::AboveFloor
                } else {
                    Opened::BelowFloor
                },
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
    // Row 11: a history event closed the epoch at `last_seq`; no seq above
    // it ever applies.
    if record.history != history {
        return Evaluation::Refuse(Refusal::Producer(ProducerRefusal::EpochClosed {
            epoch: record.epoch,
            by: ClosedBy::History,
            closed: ClosedEpoch {
                epoch: record.epoch,
                seq: record.last_seq,
            },
        }));
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
    /// The record after a claim of `key.epoch` applied (rows 2 and 6) in
    /// the group's history `history`.
    pub(crate) fn claimed(
        key: WriteKey,
        closes: Option<ClosedEpoch>,
        opened: Opened,
        history: u64,
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
            opened,
            history,
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
