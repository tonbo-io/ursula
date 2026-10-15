//! Every typed answer to a producer write (extensions Section 5.3).
//!
//! Each answer keeps the base protocol's status code; the typed detail
//! travels in added headers. A `403` without `Producer-Epoch` comes from an
//! authorizer, not from the producer rules, and is not a producer answer.

use crate::error::HeaderError;
use crate::fields;
use crate::fields::Emit;
use crate::names;
use crate::value::parse_producer_integer;
use crate::value::render_decimal;
use crate::value::render_offset;

/// Whether a receipt reports a write applied by this request or a repeat of
/// the producer's newest write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// `200`: this request applied the write.
    Applied,
    /// `204`: the write had applied before; this request applied nothing.
    Repeat,
}

/// The range a write occupies: `Stream-Offset`, `Stream-Next-Offset` and
/// `Stream-Closed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    pub start: u64,
    pub next: u64,
    /// The write closed the stream.
    pub closed: bool,
}

/// An epoch that a claim or a history event closed, with its last applied
/// seq: every seq at or below `seq` applied, and no seq above it ever will.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedEpoch {
    pub epoch: u64,
    pub seq: u64,
}

/// What closed an epoch, the `by` parameter of `epoch-closed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClosedBy {
    /// A claim of a later epoch.
    Claim,
    /// A restore or an accepted loss rewrote the stream's history.
    History,
}

/// The value of `Producer-Outcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    ReceiptEvicted,
    EpochClosed(ClosedBy),
    KeyConflict,
    ProducerExpired,
}

impl Outcome {
    /// Parses an RFC 9651 token with an optional `by` parameter. Unknown
    /// parameters are ignored, as RFC 9651 asks.
    pub fn parse(text: &str) -> Result<Self, HeaderError> {
        let mut parts = text.split(';').map(|part| part.trim_matches([' ', '\t']));
        let token = parts.next().unwrap_or_default();
        let mut by = None;
        for parameter in parts {
            let (key, value) = parameter.split_once('=').unwrap_or((parameter, "?1"));
            if key == "by" {
                by = Some(match value {
                    "claim" => ClosedBy::Claim,
                    "history" => ClosedBy::History,
                    _ => return Err(HeaderError::InvalidOutcome),
                });
            }
        }
        match (token, by) {
            ("receipt-evicted", None) => Ok(Self::ReceiptEvicted),
            ("epoch-closed", Some(by)) => Ok(Self::EpochClosed(by)),
            ("key-conflict", None) => Ok(Self::KeyConflict),
            ("producer-expired", None) => Ok(Self::ProducerExpired),
            _ => Err(HeaderError::InvalidOutcome),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptEvicted => "receipt-evicted",
            Self::EpochClosed(ClosedBy::Claim) => "epoch-closed;by=claim",
            Self::EpochClosed(ClosedBy::History) => "epoch-closed;by=history",
            Self::KeyConflict => "key-conflict",
            Self::ProducerExpired => "producer-expired",
        }
    }
}

/// A typed answer to a producer write. One variant per answer of extensions
/// Section 5.3; each fixes its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerAnswer {
    /// `200` or `204`: the write applied. `seq` is the epoch's highest
    /// applied seq, and `receipt` describes the request's own write. A claim
    /// that closed an earlier epoch names it in `closed`, on every repeat too.
    Receipt {
        delivery: Delivery,
        epoch: u64,
        seq: u64,
        receipt: Receipt,
        closed: Option<ClosedEpoch>,
        incarnation: Option<u64>,
    },
    /// `204 receipt-evicted`: a write under this key applied; the server
    /// keeps no range for it. `seq` is the epoch's highest applied seq.
    ReceiptEvicted {
        epoch: u64,
        seq: u64,
        closed: Option<ClosedEpoch>,
        incarnation: Option<u64>,
    },
    /// `403 epoch-closed`: `closed` is closed. `epoch` is the producer's
    /// current epoch.
    EpochClosed {
        by: ClosedBy,
        epoch: u64,
        closed: ClosedEpoch,
    },
    /// `403 key-conflict`: the key holds a write with another identity. This
    /// write did not apply and never will under this key.
    KeyConflict { epoch: u64 },
    /// `403 producer-expired`: the server keeps no memory of the write's
    /// epoch. `epoch` is the producer's current epoch, or the stream's epoch
    /// floor.
    ProducerExpired { epoch: u64 },
    /// `409`: the epoch expects `expected` next; the request carried
    /// `received`.
    SeqGap { expected: u64, received: u64 },
    /// `400`: a seq above 0 of an epoch above the producer's, which is not
    /// claimed yet. `Producer-Expected-Seq: 0`.
    EpochNotClaimed,
}

impl ProducerAnswer {
    pub fn status(&self) -> u16 {
        match self {
            Self::Receipt {
                delivery: Delivery::Applied,
                ..
            } => 200,
            Self::Receipt {
                delivery: Delivery::Repeat,
                ..
            }
            | Self::ReceiptEvicted { .. } => 204,
            Self::EpochClosed { .. } | Self::KeyConflict { .. } | Self::ProducerExpired { .. } => {
                403
            }
            Self::SeqGap { .. } => 409,
            Self::EpochNotClaimed => 400,
        }
    }

    /// Emits every header of the answer, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        let emit = &mut emit;
        match self {
            Self::Receipt {
                delivery: _,
                epoch,
                seq,
                receipt,
                closed,
                incarnation,
            } => {
                render_progress(emit, *epoch, *seq, *closed, *incarnation);
                emit(names::STREAM_OFFSET, &render_offset(receipt.start));
                emit(names::STREAM_NEXT_OFFSET, &render_offset(receipt.next));
                if receipt.closed {
                    emit(names::STREAM_CLOSED, "true");
                }
            }
            Self::ReceiptEvicted {
                epoch,
                seq,
                closed,
                incarnation,
            } => {
                render_progress(emit, *epoch, *seq, *closed, *incarnation);
                emit(names::PRODUCER_OUTCOME, Outcome::ReceiptEvicted.as_str());
            }
            Self::EpochClosed { by, epoch, closed } => {
                integer(emit, names::PRODUCER_EPOCH, *epoch);
                render_closed(emit, Some(*closed));
                emit(names::PRODUCER_OUTCOME, Outcome::EpochClosed(*by).as_str());
            }
            Self::KeyConflict { epoch } => {
                integer(emit, names::PRODUCER_EPOCH, *epoch);
                emit(names::PRODUCER_OUTCOME, Outcome::KeyConflict.as_str());
            }
            Self::ProducerExpired { epoch } => {
                integer(emit, names::PRODUCER_EPOCH, *epoch);
                emit(names::PRODUCER_OUTCOME, Outcome::ProducerExpired.as_str());
            }
            Self::SeqGap { expected, received } => {
                integer(emit, names::PRODUCER_EXPECTED_SEQ, *expected);
                integer(emit, names::PRODUCER_RECEIVED_SEQ, *received);
            }
            Self::EpochNotClaimed => integer(emit, names::PRODUCER_EXPECTED_SEQ, 0),
        }
    }

    /// Parses an answer from its status and `(name, value)` header pairs.
    /// `Ok(None)` when the response is not a producer answer: a write without
    /// a key, an authorizer's `403`, a conflict of another rule, or any other
    /// status.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [
            epoch,
            seq,
            outcome,
            closed_epoch,
            closed_seq,
            expected,
            received,
            start,
            next,
            stream_closed,
            incarnation,
        ] = fields::collect(headers, [
            names::PRODUCER_EPOCH,
            names::PRODUCER_SEQ,
            names::PRODUCER_OUTCOME,
            names::PRODUCER_CLOSED_EPOCH,
            names::PRODUCER_CLOSED_SEQ,
            names::PRODUCER_EXPECTED_SEQ,
            names::PRODUCER_RECEIVED_SEQ,
            names::STREAM_OFFSET,
            names::STREAM_NEXT_OFFSET,
            names::STREAM_CLOSED,
            names::STREAM_INCARNATION,
        ])?;
        let producer_integer = |name, value| fields::parsed(name, value, parse_producer_integer);
        let epoch = producer_integer(names::PRODUCER_EPOCH, epoch)?;
        let expected = producer_integer(names::PRODUCER_EXPECTED_SEQ, expected)?;
        let outcome = fields::text(names::PRODUCER_OUTCOME, outcome)?
            .map(Outcome::parse)
            .transpose()?;
        let closed = fields::pair(
            (
                names::PRODUCER_CLOSED_EPOCH,
                producer_integer(names::PRODUCER_CLOSED_EPOCH, closed_epoch)?,
            ),
            (
                names::PRODUCER_CLOSED_SEQ,
                producer_integer(names::PRODUCER_CLOSED_SEQ, closed_seq)?,
            ),
        )?
        .map(|(epoch, seq)| ClosedEpoch { epoch, seq });
        let required = fields::required;
        match status {
            200 | 204 => {
                let Some(epoch) = epoch else {
                    return Ok(None);
                };
                let seq = required(
                    names::PRODUCER_SEQ,
                    producer_integer(names::PRODUCER_SEQ, seq)?,
                )?;
                let incarnation = fields::token(names::STREAM_INCARNATION, incarnation)?;
                match outcome {
                    Some(Outcome::ReceiptEvicted) if status == 204 => {
                        Ok(Some(Self::ReceiptEvicted {
                            epoch,
                            seq,
                            closed,
                            incarnation,
                        }))
                    }
                    Some(_) => Err(HeaderError::OutcomeStatusMismatch { status }),
                    None => {
                        let receipt = Receipt {
                            start: required(
                                names::STREAM_OFFSET,
                                fields::offset(names::STREAM_OFFSET, start)?,
                            )?,
                            next: required(
                                names::STREAM_NEXT_OFFSET,
                                fields::offset(names::STREAM_NEXT_OFFSET, next)?,
                            )?,
                            closed: fields::flag(names::STREAM_CLOSED, stream_closed)?,
                        };
                        let delivery = if status == 200 {
                            Delivery::Applied
                        } else {
                            Delivery::Repeat
                        };
                        Ok(Some(Self::Receipt {
                            delivery,
                            epoch,
                            seq,
                            receipt,
                            closed,
                            incarnation,
                        }))
                    }
                }
            }
            403 => {
                let Some(epoch) = epoch else {
                    return Ok(None);
                };
                match outcome {
                    None => Err(HeaderError::Missing {
                        name: names::PRODUCER_OUTCOME,
                    }),
                    Some(Outcome::EpochClosed(by)) => Ok(Some(Self::EpochClosed {
                        by,
                        epoch,
                        closed: closed.ok_or(HeaderError::Missing {
                            name: names::PRODUCER_CLOSED_EPOCH,
                        })?,
                    })),
                    Some(Outcome::KeyConflict) => Ok(Some(Self::KeyConflict { epoch })),
                    Some(Outcome::ProducerExpired) => Ok(Some(Self::ProducerExpired { epoch })),
                    Some(Outcome::ReceiptEvicted) => {
                        Err(HeaderError::OutcomeStatusMismatch { status })
                    }
                }
            }
            409 => {
                let Some(expected) = expected else {
                    return Ok(None);
                };
                let received = required(
                    names::PRODUCER_RECEIVED_SEQ,
                    producer_integer(names::PRODUCER_RECEIVED_SEQ, received)?,
                )?;
                Ok(Some(Self::SeqGap { expected, received }))
            }
            400 => match expected {
                None => Ok(None),
                Some(0) => Ok(Some(Self::EpochNotClaimed)),
                Some(expected) => Err(HeaderError::UnexpectedExpectedSeq { expected }),
            },
            _ => Ok(None),
        }
    }
}

fn integer(emit: &mut Emit<'_>, name: &'static str, value: u64) {
    emit(name, &render_decimal(value));
}

fn render_closed(emit: &mut Emit<'_>, closed: Option<ClosedEpoch>) {
    if let Some(closed) = closed {
        integer(emit, names::PRODUCER_CLOSED_EPOCH, closed.epoch);
        integer(emit, names::PRODUCER_CLOSED_SEQ, closed.seq);
    }
}

/// The headers every answer about an applied write carries.
fn render_progress(
    emit: &mut Emit<'_>,
    epoch: u64,
    seq: u64,
    closed: Option<ClosedEpoch>,
    incarnation: Option<u64>,
) {
    integer(emit, names::PRODUCER_EPOCH, epoch);
    integer(emit, names::PRODUCER_SEQ, seq);
    render_closed(emit, closed);
    if let Some(incarnation) = incarnation {
        integer(emit, names::STREAM_INCARNATION, incarnation);
    }
}

#[cfg(test)]
mod tests {
    use super::ClosedBy;
    use super::ClosedEpoch;
    use super::Delivery;
    use super::Outcome;
    use super::ProducerAnswer;
    use super::Receipt;
    use crate::error::HeaderError;
    use crate::value::MAX_PRODUCER_INTEGER;

    fn rendered(answer: &ProducerAnswer) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        answer.render(|name, value| out.push((name, value.to_owned())));
        out
    }

    fn parse(status: u16, pairs: &[(&str, &str)]) -> Result<Option<ProducerAnswer>, HeaderError> {
        ProducerAnswer::parse(
            status,
            pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
        )
    }

    fn every_answer() -> Vec<ProducerAnswer> {
        let closed = ClosedEpoch {
            epoch: 6,
            seq: MAX_PRODUCER_INTEGER,
        };
        vec![
            ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                epoch: 7,
                seq: 0,
                receipt: Receipt {
                    start: 0,
                    next: 12,
                    closed: false,
                },
                closed: Some(closed),
                incarnation: Some(4_503_599_627_370_497),
            },
            ProducerAnswer::Receipt {
                delivery: Delivery::Repeat,
                epoch: MAX_PRODUCER_INTEGER,
                seq: MAX_PRODUCER_INTEGER,
                receipt: Receipt {
                    start: 12,
                    next: u64::MAX,
                    closed: true,
                },
                closed: None,
                incarnation: None,
            },
            ProducerAnswer::ReceiptEvicted {
                epoch: 1,
                seq: 9,
                closed: None,
                incarnation: Some(5),
            },
            ProducerAnswer::ReceiptEvicted {
                epoch: 1,
                seq: 9,
                closed: Some(closed),
                incarnation: None,
            },
            ProducerAnswer::EpochClosed {
                by: ClosedBy::Claim,
                epoch: 7,
                closed,
            },
            ProducerAnswer::EpochClosed {
                by: ClosedBy::History,
                epoch: 7,
                closed: ClosedEpoch { epoch: 7, seq: 3 },
            },
            ProducerAnswer::KeyConflict { epoch: 2 },
            ProducerAnswer::ProducerExpired { epoch: 11 },
            ProducerAnswer::SeqGap {
                expected: 4,
                received: 6,
            },
            ProducerAnswer::EpochNotClaimed,
        ]
    }

    #[test]
    fn every_answer_round_trips() {
        for answer in every_answer() {
            let pairs = rendered(&answer);
            let parsed = ProducerAnswer::parse(
                answer.status(),
                pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
            );
            assert_eq!(parsed, Ok(Some(answer.clone())), "{pairs:?}");
        }
    }

    #[test]
    fn statuses_keep_the_base_protocol_codes() {
        let statuses: Vec<u16> = every_answer().iter().map(ProducerAnswer::status).collect();
        assert_eq!(statuses, [200, 204, 204, 204, 403, 403, 403, 403, 409, 400]);
    }

    #[test]
    fn outcomes_round_trip_and_tolerate_parameters() {
        for outcome in [
            Outcome::ReceiptEvicted,
            Outcome::EpochClosed(ClosedBy::Claim),
            Outcome::EpochClosed(ClosedBy::History),
            Outcome::KeyConflict,
            Outcome::ProducerExpired,
        ] {
            assert_eq!(Outcome::parse(outcome.as_str()), Ok(outcome));
        }
        assert_eq!(
            Outcome::parse("epoch-closed ; x=1; by=history"),
            Ok(Outcome::EpochClosed(ClosedBy::History))
        );
        for invalid in [
            "epoch-closed",
            "key-conflict;by=claim",
            "applied",
            "",
            "epoch-closed;by=query",
        ] {
            assert_eq!(
                Outcome::parse(invalid),
                Err(HeaderError::InvalidOutcome),
                "{invalid}"
            );
        }
    }

    #[test]
    fn responses_without_producer_detail_are_not_producer_answers() {
        assert_eq!(parse(403, &[]), Ok(None), "an authorizer's 403");
        assert_eq!(parse(204, &[("stream-next-offset", "5")]), Ok(None));
        assert_eq!(parse(409, &[("stream-closed", "true")]), Ok(None));
        assert_eq!(parse(400, &[]), Ok(None));
        assert_eq!(parse(412, &[("producer-epoch", "1")]), Ok(None));
        assert_eq!(parse(503, &[("producer-epoch", "1")]), Ok(None));
    }

    #[test]
    fn malformed_answers_are_typed() {
        assert_eq!(
            parse(403, &[("producer-epoch", "1")]),
            Err(HeaderError::Missing {
                name: "producer-outcome"
            })
        );
        assert_eq!(
            parse(403, &[
                ("producer-epoch", "1"),
                ("producer-outcome", "epoch-closed;by=claim")
            ]),
            Err(HeaderError::Missing {
                name: "producer-closed-epoch"
            })
        );
        assert_eq!(
            parse(403, &[
                ("producer-epoch", "1"),
                ("producer-outcome", "key-conflict"),
                ("producer-closed-epoch", "0"),
            ]),
            Err(HeaderError::Missing {
                name: "producer-closed-seq"
            })
        );
        assert_eq!(
            parse(200, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("producer-outcome", "receipt-evicted"),
            ]),
            Err(HeaderError::OutcomeStatusMismatch { status: 200 })
        );
        assert_eq!(
            parse(204, &[("producer-epoch", "1"), ("producer-seq", "1")]),
            Err(HeaderError::Missing {
                name: "stream-offset"
            })
        );
        assert_eq!(
            parse(409, &[("producer-expected-seq", "1")]),
            Err(HeaderError::Missing {
                name: "producer-received-seq"
            })
        );
        assert_eq!(
            parse(400, &[("producer-expected-seq", "3")]),
            Err(HeaderError::UnexpectedExpectedSeq { expected: 3 })
        );
        assert_eq!(
            parse(200, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("stream-offset", "00000000000000000000"),
                ("stream-next-offset", "00000000000000000001"),
                ("stream-closed", "yes"),
            ]),
            Err(HeaderError::InvalidFlag {
                name: "stream-closed"
            })
        );
        assert_eq!(
            parse(200, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("stream-offset", "0"),
                ("stream-next-offset", "00000000000000000001"),
            ]),
            Err(HeaderError::InvalidOffset {
                name: "stream-offset"
            })
        );
        assert_eq!(
            parse(200, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("stream-offset", "00000000000000000000"),
                ("stream-next-offset", "00000000000000000001"),
                ("stream-incarnation", "07"),
            ]),
            Err(HeaderError::NonCanonicalInteger {
                name: "stream-incarnation"
            })
        );
    }
}
