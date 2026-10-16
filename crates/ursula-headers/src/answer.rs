//! Every typed answer to a producer write (extensions Section 5.3).
//!
//! Each answer keeps the base protocol's status code; the typed detail
//! travels in added headers. A `403` without `Producer-Epoch` comes from an
//! authorizer, not from the producer rules, and is not a producer answer.
//!
//! A client that sent the producer headers and gets a `200` or `204` that
//! [`ProducerAnswer::parse`] reads as `Ok(None)` (no producer header at all)
//! got an answer from a server or layer that does not speak write keys; it
//! treats that as a protocol violation, not as a receipt.

use crate::error::HeaderError;
use crate::error_body::ErrorCode;
use crate::fields;
use crate::fields::Emit;
use crate::names;
use crate::value::Epoch;
use crate::value::Incarnation;
use crate::value::Offset;
use crate::value::Seq;

/// Whether a receipt reports a write applied by this request or a repeat of
/// the producer's newest write. The base protocol answers a close-only write
/// (an empty body with `Stream-Closed: true`) `204` whether it applied now or
/// before, so a `204` receipt for a close-only write does not tell new from
/// repeat: it parses as [`Delivery::Repeat`] either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// `200`: this request applied the write.
    Applied,
    /// `204`: the write had applied before; this request applied nothing.
    Repeat,
}

/// The range a write occupies: `Stream-Offset`, `Stream-Next-Offset` and
/// `Stream-Closed`. `start` is at most `next`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    start: Offset,
    next: Offset,
    closes_stream: bool,
}

impl Receipt {
    /// [`HeaderError::ReceiptRangeInverted`] when `start` is above `next`.
    pub fn new(start: Offset, next: Offset, closes_stream: bool) -> Result<Self, HeaderError> {
        if start > next {
            return Err(HeaderError::ReceiptRangeInverted { start, next });
        }
        Ok(Self {
            start,
            next,
            closes_stream,
        })
    }

    /// Where the write's bytes start.
    pub const fn start(&self) -> Offset {
        self.start
    }

    /// Where the write's bytes end.
    pub const fn next(&self) -> Offset {
        self.next
    }

    /// The write closed the stream.
    pub const fn closes_stream(&self) -> bool {
        self.closes_stream
    }
}

/// An epoch that a claim or a history event closed, with its last applied
/// seq: every seq at or below `seq` applied, and no seq above it ever will.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedEpoch {
    pub epoch: Epoch,
    pub seq: Seq,
}

/// The producer's current epoch on an answer about an applied write, and
/// the earlier epoch the claim of it closed, if any. The closed epoch is
/// below the current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentEpoch {
    epoch: Epoch,
    closed_epoch: Option<ClosedEpoch>,
}

impl CurrentEpoch {
    /// An epoch whose claim closed no other.
    pub const fn new(epoch: Epoch) -> Self {
        Self {
            epoch,
            closed_epoch: None,
        }
    }

    /// An epoch whose claim closed `closed_epoch`, which must be earlier.
    pub fn closing(epoch: Epoch, closed_epoch: ClosedEpoch) -> Result<Self, HeaderError> {
        if closed_epoch.epoch >= epoch {
            return Err(HeaderError::ClosedEpochNotEarlier {
                epoch,
                closed: closed_epoch.epoch,
            });
        }
        Ok(Self {
            epoch,
            closed_epoch: Some(closed_epoch),
        })
    }

    fn from_parts(epoch: Epoch, closed_epoch: Option<ClosedEpoch>) -> Result<Self, HeaderError> {
        match closed_epoch {
            None => Ok(Self::new(epoch)),
            Some(closed_epoch) => Self::closing(epoch, closed_epoch),
        }
    }

    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub const fn closed_epoch(&self) -> Option<ClosedEpoch> {
        self.closed_epoch
    }
}

/// What closed an epoch, the `by` parameter of `epoch-closed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClosedBy {
    /// A claim of a later epoch.
    Claim,
    /// A restore or an accepted loss rewrote the stream's history.
    History,
}

/// The detail of a `403 epoch-closed`: the producer's current epoch and the
/// epoch that is closed. A claim closes an earlier epoch; a history event
/// closes the current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochClosure {
    by: ClosedBy,
    epoch: Epoch,
    closed_epoch: ClosedEpoch,
}

impl EpochClosure {
    /// `closed_epoch` was closed by the claim of `epoch`, which is later.
    pub fn by_claim(epoch: Epoch, closed_epoch: ClosedEpoch) -> Result<Self, HeaderError> {
        let current = CurrentEpoch::closing(epoch, closed_epoch)?;
        Ok(Self {
            by: ClosedBy::Claim,
            epoch: current.epoch,
            closed_epoch,
        })
    }

    /// A history event closed the producer's current epoch.
    pub const fn by_history(closed_epoch: ClosedEpoch) -> Self {
        Self {
            by: ClosedBy::History,
            epoch: closed_epoch.epoch,
            closed_epoch,
        }
    }

    pub const fn by(&self) -> ClosedBy {
        self.by
    }

    /// The producer's current epoch (`Producer-Epoch`).
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The closed epoch and its last applied seq (`Producer-Closed-*`).
    pub const fn closed_epoch(&self) -> ClosedEpoch {
        self.closed_epoch
    }

    fn from_parts(
        by: ClosedBy,
        epoch: Epoch,
        closed_epoch: ClosedEpoch,
    ) -> Result<Self, HeaderError> {
        match by {
            ClosedBy::Claim => Self::by_claim(epoch, closed_epoch),
            ClosedBy::History if closed_epoch.epoch == epoch => Ok(Self::by_history(closed_epoch)),
            ClosedBy::History => Err(HeaderError::HistoryClosedOtherEpoch {
                epoch,
                closed: closed_epoch.epoch,
            }),
        }
    }
}

/// The RFC 9651 grammar `Producer-Outcome` needs.
mod sf {
    fn is_tchar(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
    }

    /// `sf-token = ( ALPHA / "*" ) *( tchar / ":" / "/" )`.
    pub(super) fn is_token(text: &str) -> bool {
        let mut bytes = text.bytes();
        bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == b'*')
            && bytes.all(|byte| is_tchar(byte) || byte == b':' || byte == b'/')
    }

    /// `key = ( lcalpha / "*" ) *( lcalpha / DIGIT / "_" / "-" / "." / "*" )`.
    pub(super) fn is_key(text: &str) -> bool {
        let mut bytes = text.bytes();
        bytes
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first == b'*')
            && bytes.all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.*".contains(&byte)
            })
    }

    /// A bare item: an integer or decimal, a string, a token, a byte
    /// sequence or a boolean. Dates and display strings are not accepted.
    pub(super) fn is_bare_item(text: &str) -> bool {
        is_number(text) || is_string(text) || is_token(text) || is_bytes(text) || is_boolean(text)
    }

    fn is_number(text: &str) -> bool {
        let digits = text.strip_prefix('-').unwrap_or(text);
        let (integer, fraction) = match digits.split_once('.') {
            Some((integer, fraction)) => (integer, Some(fraction)),
            None => (digits, None),
        };
        let all_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
        match fraction {
            None => (1..=15).contains(&integer.len()) && all_digits(integer),
            Some(fraction) => {
                (1..=12).contains(&integer.len())
                    && (1..=3).contains(&fraction.len())
                    && all_digits(integer)
                    && all_digits(fraction)
            }
        }
    }

    fn is_string(text: &str) -> bool {
        let Some(inner) = text
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
        else {
            return false;
        };
        let mut escaped = false;
        for byte in inner.bytes() {
            if escaped {
                if byte != b'"' && byte != b'\\' {
                    return false;
                }
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' || !(0x20..=0x7e).contains(&byte) {
                return false;
            }
        }
        !escaped
    }

    fn is_bytes(text: &str) -> bool {
        text.strip_prefix(':')
            .and_then(|rest| rest.strip_suffix(':'))
            .is_some_and(|inner| {
                inner
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"+/=".contains(&byte))
            })
    }

    fn is_boolean(text: &str) -> bool {
        text == "?0" || text == "?1"
    }
}

/// The value of `Producer-Outcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    ReceiptEvicted,
    EpochClosed(ClosedBy),
    KeyConflict,
    ProducerExpired,
    StaleEpoch,
}

impl Outcome {
    /// Parses an RFC 9651 item: a token, then parameters, each `;`, any
    /// number of spaces, a key and an optional `=` bare item, with nothing
    /// else (no space before a `;`, none at the ends: the field's own
    /// whitespace is already gone). The `by` parameter names what closed an
    /// epoch; other parameters must be well formed and are ignored, as RFC
    /// 9651 asks, and a repeated key keeps its last value.
    pub fn parse(text: &str) -> Result<Self, HeaderError> {
        let mut parts = text.split(';');
        let token = parts.next().unwrap_or_default();
        if !sf::is_token(token) {
            return Err(HeaderError::InvalidOutcome);
        }
        let mut by = None;
        for parameter in parts {
            let parameter = parameter.trim_start_matches(' ');
            let (key, value) = match parameter.split_once('=') {
                Some((key, value)) => (key, Some(value)),
                None => (parameter, None),
            };
            if !sf::is_key(key) || value.is_some_and(|value| !sf::is_bare_item(value)) {
                return Err(HeaderError::InvalidOutcome);
            }
            if key == "by" {
                by = Some(match value {
                    Some("claim") => ClosedBy::Claim,
                    Some("history") => ClosedBy::History,
                    _ => return Err(HeaderError::InvalidOutcome),
                });
            }
        }
        match (token, by) {
            ("receipt-evicted", None) => Ok(Self::ReceiptEvicted),
            ("epoch-closed", Some(by)) => Ok(Self::EpochClosed(by)),
            ("key-conflict", None) => Ok(Self::KeyConflict),
            ("producer-expired", None) => Ok(Self::ProducerExpired),
            ("stale-epoch", None) => Ok(Self::StaleEpoch),
            _ => Err(HeaderError::InvalidOutcome),
        }
    }

    /// The code of the error body of the `403` that names this outcome, the
    /// one list of the producer `403` codes. `None` for `receipt-evicted`,
    /// which answers a `204`.
    pub fn error_code(self) -> Option<ErrorCode> {
        match self {
            Self::ReceiptEvicted => None,
            Self::EpochClosed(_) => Some(ErrorCode::ProducerEpochClosed),
            Self::KeyConflict => Some(ErrorCode::ProducerKeyConflict),
            Self::ProducerExpired => Some(ErrorCode::ProducerExpired),
            Self::StaleEpoch => Some(ErrorCode::ProducerEpochStale),
        }
    }

    /// Whether `code` is the code of a producer `403`, which a `403` carries
    /// only with `Producer-Epoch` and `Producer-Outcome`.
    pub fn is_refusal_code(code: &ErrorCode) -> bool {
        [
            Self::EpochClosed(ClosedBy::Claim),
            Self::KeyConflict,
            Self::ProducerExpired,
            Self::StaleEpoch,
        ]
        .into_iter()
        .any(|outcome| outcome.error_code().as_ref() == Some(code))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptEvicted => "receipt-evicted",
            Self::EpochClosed(ClosedBy::Claim) => "epoch-closed;by=claim",
            Self::EpochClosed(ClosedBy::History) => "epoch-closed;by=history",
            Self::KeyConflict => "key-conflict",
            Self::ProducerExpired => "producer-expired",
            Self::StaleEpoch => "stale-epoch",
        }
    }
}

/// A typed answer to a producer write. One variant per answer of extensions
/// Section 5.3; each fixes its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerAnswer {
    /// `200` or `204`: the write applied. `seq` is the epoch's highest
    /// applied seq, and `receipt` describes the request's own write. A claim
    /// that closed an earlier epoch names it in `epoch`, on every repeat too.
    Receipt {
        delivery: Delivery,
        epoch: CurrentEpoch,
        seq: Seq,
        receipt: Receipt,
        incarnation: Incarnation,
    },
    /// `204 receipt-evicted`: a write under this key applied; the server
    /// keeps no range for it. `seq` is the epoch's highest applied seq.
    ReceiptEvicted {
        epoch: CurrentEpoch,
        seq: Seq,
        incarnation: Incarnation,
    },
    /// `403 epoch-closed`: an epoch of the producer is closed.
    EpochClosed(EpochClosure),
    /// `403 key-conflict`: the key holds a write with another identity. This
    /// write did not apply and never will under this key.
    KeyConflict { epoch: Epoch },
    /// `403 producer-expired`: the server keeps no memory of the write's
    /// epoch. `epoch` is the producer's current epoch, or the stream's epoch
    /// floor.
    ProducerExpired { epoch: Epoch },
    /// `403 stale-epoch`: the write's epoch is below the producer's current
    /// epoch `epoch` (the base protocol's stale epoch). Every producer `403`
    /// names its outcome, so a `403` without one is not a producer answer.
    StaleEpoch { epoch: Epoch },
    /// `409`: the epoch expects `expected` next; the request carried
    /// `received`.
    SeqGap { expected: Seq, received: Seq },
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
            Self::EpochClosed(_)
            | Self::KeyConflict { .. }
            | Self::ProducerExpired { .. }
            | Self::StaleEpoch { .. } => 403,
            Self::SeqGap { .. } => 409,
            Self::EpochNotClaimed => 400,
        }
    }

    /// The code of a refusal's error body, the code the server's late path
    /// gives the same refusal. `None` for a receipt.
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            Self::Receipt { .. } | Self::ReceiptEvicted { .. } => None,
            Self::EpochClosed(closure) => Outcome::EpochClosed(closure.by()).error_code(),
            Self::KeyConflict { .. } => Outcome::KeyConflict.error_code(),
            Self::ProducerExpired { .. } => Outcome::ProducerExpired.error_code(),
            Self::StaleEpoch { .. } => Outcome::StaleEpoch.error_code(),
            Self::SeqGap { .. } => Some(ErrorCode::ProducerSeqConflict),
            Self::EpochNotClaimed => Some(ErrorCode::InvalidProducer),
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
                incarnation,
            } => {
                render_progress(emit, *epoch, *seq, *incarnation);
                emit(names::STREAM_OFFSET, &receipt.start.to_string());
                emit(names::STREAM_NEXT_OFFSET, &receipt.next.to_string());
                if receipt.closes_stream {
                    emit(names::STREAM_CLOSED, "true");
                }
            }
            Self::ReceiptEvicted {
                epoch,
                seq,
                incarnation,
            } => {
                render_progress(emit, *epoch, *seq, *incarnation);
                emit(names::PRODUCER_OUTCOME, Outcome::ReceiptEvicted.as_str());
            }
            Self::EpochClosed(closure) => {
                emit(names::PRODUCER_EPOCH, &closure.epoch.to_string());
                render_closed(emit, closure.closed_epoch);
                emit(
                    names::PRODUCER_OUTCOME,
                    Outcome::EpochClosed(closure.by).as_str(),
                );
            }
            Self::KeyConflict { epoch } => {
                emit(names::PRODUCER_EPOCH, &epoch.to_string());
                emit(names::PRODUCER_OUTCOME, Outcome::KeyConflict.as_str());
            }
            Self::ProducerExpired { epoch } => {
                emit(names::PRODUCER_EPOCH, &epoch.to_string());
                emit(names::PRODUCER_OUTCOME, Outcome::ProducerExpired.as_str());
            }
            Self::StaleEpoch { epoch } => {
                emit(names::PRODUCER_EPOCH, &epoch.to_string());
                emit(names::PRODUCER_OUTCOME, Outcome::StaleEpoch.as_str());
            }
            Self::SeqGap { expected, received } => {
                emit(names::PRODUCER_EXPECTED_SEQ, &expected.to_string());
                emit(names::PRODUCER_RECEIVED_SEQ, &received.to_string());
            }
            Self::EpochNotClaimed => emit(names::PRODUCER_EXPECTED_SEQ, &Seq::ZERO.to_string()),
        }
    }

    /// Parses an answer from its status and `(name, value)` header pairs.
    /// `Ok(None)` when the response is not a producer answer: a write without
    /// a key, an authorizer's `403`, a conflict of another rule, or any other
    /// status. Only the headers the status uses are parsed, so a stray
    /// producer header on a `503` or a `412` is not an error.
    ///
    /// Answers come only from Ursula, so their integers are canonical
    /// decimal.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if !matches!(status, 200 | 204 | 400 | 403 | 409) {
            return Ok(None);
        }
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
        let seq_field = |name, value| fields::parsed(name, value, Seq::parse);
        match status {
            409 => {
                let Some(expected) = seq_field(names::PRODUCER_EXPECTED_SEQ, expected)? else {
                    // A `409` of another rule carries neither header; one
                    // with only the received seq is a malformed gap.
                    return if received.is_some() {
                        Err(HeaderError::Missing {
                            name: names::PRODUCER_EXPECTED_SEQ,
                        })
                    } else {
                        Ok(None)
                    };
                };
                let received = fields::required(
                    names::PRODUCER_RECEIVED_SEQ,
                    seq_field(names::PRODUCER_RECEIVED_SEQ, received)?,
                )?;
                return Ok(Some(Self::SeqGap { expected, received }));
            }
            400 => {
                return match seq_field(names::PRODUCER_EXPECTED_SEQ, expected)? {
                    None => Ok(None),
                    Some(Seq::ZERO) => Ok(Some(Self::EpochNotClaimed)),
                    Some(expected) => Err(HeaderError::UnexpectedExpectedSeq { expected }),
                };
            }
            _ => {}
        }
        // A 200, 204 or 403 that carries any producer header is a producer
        // answer, so it must name the epoch: one without is malformed, never
        // a plain success or an authorizer's refusal.
        let Some(epoch) = fields::parsed(names::PRODUCER_EPOCH, epoch, Epoch::parse)? else {
            let producer_answer = seq.is_some()
                || outcome.is_some()
                || closed_epoch.is_some()
                || closed_seq.is_some();
            return if producer_answer {
                Err(HeaderError::Missing {
                    name: names::PRODUCER_EPOCH,
                })
            } else {
                Ok(None)
            };
        };
        let outcome = fields::text(names::PRODUCER_OUTCOME, outcome)?
            .map(Outcome::parse)
            .transpose()?;
        let closed = fields::pair(
            (
                names::PRODUCER_CLOSED_EPOCH,
                fields::parsed(names::PRODUCER_CLOSED_EPOCH, closed_epoch, Epoch::parse)?,
            ),
            (
                names::PRODUCER_CLOSED_SEQ,
                seq_field(names::PRODUCER_CLOSED_SEQ, closed_seq)?,
            ),
        )?
        .map(|(epoch, seq)| ClosedEpoch { epoch, seq });
        if status == 403 {
            // A refusal carries `Producer-Epoch`, the outcome and, for
            // `epoch-closed` only, `Producer-Closed-*`. Any other producer
            // header means the answer is not what it says.
            let stray = [
                (names::PRODUCER_SEQ, seq.is_some()),
                (names::PRODUCER_EXPECTED_SEQ, expected.is_some()),
                (names::PRODUCER_RECEIVED_SEQ, received.is_some()),
                (
                    names::PRODUCER_CLOSED_EPOCH,
                    closed.is_some() && !matches!(outcome, Some(Outcome::EpochClosed(_))),
                ),
            ];
            if let Some((name, _)) = stray.into_iter().find(|(_, present)| *present) {
                return Err(HeaderError::UnexpectedHeader { name, status });
            }
            return match outcome {
                None => Err(HeaderError::Missing {
                    name: names::PRODUCER_OUTCOME,
                }),
                Some(Outcome::EpochClosed(by)) => {
                    let closed = fields::required(names::PRODUCER_CLOSED_EPOCH, closed)?;
                    Ok(Some(Self::EpochClosed(EpochClosure::from_parts(
                        by, epoch, closed,
                    )?)))
                }
                Some(Outcome::KeyConflict) => Ok(Some(Self::KeyConflict { epoch })),
                Some(Outcome::ProducerExpired) => Ok(Some(Self::ProducerExpired { epoch })),
                Some(Outcome::StaleEpoch) => Ok(Some(Self::StaleEpoch { epoch })),
                Some(Outcome::ReceiptEvicted) => Err(HeaderError::OutcomeStatusMismatch { status }),
            };
        }
        let seq = fields::required(names::PRODUCER_SEQ, seq_field(names::PRODUCER_SEQ, seq)?)?;
        let incarnation = fields::required(
            names::STREAM_INCARNATION,
            fields::incarnation(names::STREAM_INCARNATION, incarnation)?,
        )?;
        let epoch = CurrentEpoch::from_parts(epoch, closed)?;
        match outcome {
            Some(Outcome::ReceiptEvicted) if status == 204 => Ok(Some(Self::ReceiptEvicted {
                epoch,
                seq,
                incarnation,
            })),
            Some(_) => Err(HeaderError::OutcomeStatusMismatch { status }),
            None => {
                let receipt = Receipt::new(
                    fields::required(
                        names::STREAM_OFFSET,
                        fields::offset(names::STREAM_OFFSET, start)?,
                    )?,
                    fields::required(
                        names::STREAM_NEXT_OFFSET,
                        fields::offset(names::STREAM_NEXT_OFFSET, next)?,
                    )?,
                    fields::flag(names::STREAM_CLOSED, stream_closed)?,
                )?;
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
                    incarnation,
                }))
            }
        }
    }
}

fn render_closed(emit: &mut Emit<'_>, closed: ClosedEpoch) {
    emit(names::PRODUCER_CLOSED_EPOCH, &closed.epoch.to_string());
    emit(names::PRODUCER_CLOSED_SEQ, &closed.seq.to_string());
}

/// The headers every answer about an applied write carries.
fn render_progress(emit: &mut Emit<'_>, epoch: CurrentEpoch, seq: Seq, incarnation: Incarnation) {
    emit(names::PRODUCER_EPOCH, &epoch.epoch.to_string());
    emit(names::PRODUCER_SEQ, &seq.to_string());
    if let Some(closed) = epoch.closed_epoch {
        render_closed(emit, closed);
    }
    emit(names::STREAM_INCARNATION, &incarnation.to_string());
}

#[cfg(test)]
mod tests {
    use super::ClosedBy;
    use super::ClosedEpoch;
    use super::CurrentEpoch;
    use super::Delivery;
    use super::EpochClosure;
    use super::Outcome;
    use super::ProducerAnswer;
    use super::Receipt;
    use crate::error::HeaderError;
    use crate::value::Epoch;
    use crate::value::Incarnation;
    use crate::value::Offset;
    use crate::value::Seq;

    const TOKEN: &str = "4503599627370497";

    fn epoch(value: u64) -> Epoch {
        Epoch::try_from(value).unwrap()
    }

    fn seq(value: u64) -> Seq {
        Seq::try_from(value).unwrap()
    }

    fn incarnation(value: u64) -> Incarnation {
        Incarnation::try_from(value).unwrap()
    }

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
            epoch: epoch(6),
            seq: Seq::MAX,
        };
        vec![
            ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                epoch: CurrentEpoch::closing(epoch(7), closed).unwrap(),
                seq: Seq::ZERO,
                receipt: Receipt::new(Offset::ZERO, Offset::from_u64(12), false).unwrap(),
                incarnation: incarnation(4_503_599_627_370_497),
            },
            ProducerAnswer::Receipt {
                delivery: Delivery::Repeat,
                epoch: CurrentEpoch::new(Epoch::MAX),
                seq: Seq::MAX,
                receipt: Receipt::new(Offset::from_u64(12), Offset::from_u64(u64::MAX), true)
                    .unwrap(),
                incarnation: incarnation(1),
            },
            ProducerAnswer::ReceiptEvicted {
                epoch: CurrentEpoch::new(epoch(1)),
                seq: seq(9),
                incarnation: incarnation(5),
            },
            ProducerAnswer::ReceiptEvicted {
                epoch: CurrentEpoch::closing(epoch(7), closed).unwrap(),
                seq: seq(9),
                incarnation: incarnation(5),
            },
            ProducerAnswer::EpochClosed(EpochClosure::by_claim(epoch(7), closed).unwrap()),
            ProducerAnswer::EpochClosed(EpochClosure::by_history(ClosedEpoch {
                epoch: epoch(7),
                seq: seq(3),
            })),
            ProducerAnswer::KeyConflict { epoch: epoch(2) },
            ProducerAnswer::ProducerExpired { epoch: epoch(11) },
            ProducerAnswer::StaleEpoch { epoch: epoch(12) },
            ProducerAnswer::SeqGap {
                expected: seq(4),
                received: seq(6),
            },
            ProducerAnswer::EpochNotClaimed,
        ]
    }

    #[test]
    fn a_producer_header_without_an_epoch_is_malformed() {
        let missing = Err(HeaderError::Missing {
            name: crate::names::PRODUCER_EPOCH,
        });
        assert_eq!(parse(403, &[("producer-outcome", "key-conflict")]), missing);
        assert_eq!(
            parse(204, &[("producer-outcome", "receipt-evicted")]),
            missing
        );
        assert_eq!(parse(200, &[("producer-seq", "3")]), missing);
        // Without any producer header the response is not a producer answer.
        assert_eq!(parse(403, &[]), Ok(None));
        assert_eq!(
            parse(200, &[("stream-next-offset", "00000000000000000012")]),
            Ok(None)
        );
    }

    #[test]
    fn a_producer_403_without_an_outcome_is_malformed() {
        assert_eq!(
            parse(403, &[("producer-epoch", "4")]),
            Err(HeaderError::Missing {
                name: crate::names::PRODUCER_OUTCOME,
            })
        );
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
        assert_eq!(statuses, [
            200, 204, 204, 204, 403, 403, 403, 403, 403, 409, 400
        ]);
    }

    #[test]
    fn outcomes_round_trip_and_follow_rfc_9651_parameters() {
        for outcome in [
            Outcome::ReceiptEvicted,
            Outcome::EpochClosed(ClosedBy::Claim),
            Outcome::EpochClosed(ClosedBy::History),
            Outcome::KeyConflict,
            Outcome::StaleEpoch,
            Outcome::ProducerExpired,
        ] {
            assert_eq!(Outcome::parse(outcome.as_str()), Ok(outcome));
        }
        // Well-formed unknown parameters are ignored, spaces may follow a
        // `;`, and a repeated key keeps its last value.
        for (text, outcome) in [
            (
                "epoch-closed;x=1;  by=history",
                Outcome::EpochClosed(ClosedBy::History),
            ),
            (
                "epoch-closed;by=claim;by=history",
                Outcome::EpochClosed(ClosedBy::History),
            ),
            (
                "key-conflict;a;b=?0;c=-1.25;d=\"q\\\"s\";e=:AQ==:;f=tok/en",
                Outcome::KeyConflict,
            ),
        ] {
            assert_eq!(Outcome::parse(text), Ok(outcome), "{text}");
        }
        for invalid in [
            "epoch-closed",
            "key-conflict;by=claim",
            "applied",
            "",
            "epoch-closed;by=query",
            "epoch-closed ;by=claim",
            "epoch-closed;by=claim ",
            " key-conflict",
            "epoch-closed;by",
            "epoch-closed;by=\"claim\"",
            "key-conflict;",
            "key-conflict;X=1",
            "key-conflict;x=@",
            "key-conflict;x=1.2345",
            "key-conflict;x=\"open",
            "key-conflict;;x",
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
    fn stray_headers_on_other_statuses_are_ignored() {
        assert_eq!(parse(503, &[("producer-epoch", "abc")]), Ok(None));
        assert_eq!(
            parse(412, &[
                ("producer-closed-epoch", "1"),
                ("producer-seq", "x")
            ]),
            Ok(None)
        );
        assert_eq!(
            parse(413, &[("producer-epoch", "1"), ("producer-epoch", "1")]),
            Ok(None)
        );
        // A 409 or 400 reads only the expected and received seqs.
        assert_eq!(
            parse(409, &[
                ("producer-expected-seq", "2"),
                ("producer-received-seq", "5"),
                ("producer-epoch", "abc"),
            ]),
            Ok(Some(ProducerAnswer::SeqGap {
                expected: seq(2),
                received: seq(5)
            }))
        );
    }

    #[test]
    fn closed_epochs_must_fit_the_current_epoch() {
        let closed = |value| ClosedEpoch {
            epoch: epoch(value),
            seq: seq(1),
        };
        assert_eq!(
            CurrentEpoch::closing(epoch(3), closed(3)),
            Err(HeaderError::ClosedEpochNotEarlier {
                epoch: epoch(3),
                closed: epoch(3)
            })
        );
        assert_eq!(
            EpochClosure::by_claim(epoch(3), closed(4)),
            Err(HeaderError::ClosedEpochNotEarlier {
                epoch: epoch(3),
                closed: epoch(4)
            })
        );
        assert_eq!(
            parse(403, &[
                ("producer-epoch", "3"),
                ("producer-outcome", "epoch-closed;by=history"),
                ("producer-closed-epoch", "2"),
                ("producer-closed-seq", "1"),
            ]),
            Err(HeaderError::HistoryClosedOtherEpoch {
                epoch: epoch(3),
                closed: epoch(2)
            })
        );
        assert_eq!(
            parse(403, &[
                ("producer-epoch", "3"),
                ("producer-outcome", "epoch-closed;by=claim"),
                ("producer-closed-epoch", "3"),
                ("producer-closed-seq", "1"),
            ]),
            Err(HeaderError::ClosedEpochNotEarlier {
                epoch: epoch(3),
                closed: epoch(3)
            })
        );
        assert_eq!(
            parse(200, &[
                ("producer-epoch", "3"),
                ("producer-seq", "0"),
                ("producer-closed-epoch", "5"),
                ("producer-closed-seq", "1"),
                ("stream-incarnation", TOKEN),
                ("stream-offset", "00000000000000000000"),
                ("stream-next-offset", "00000000000000000001"),
            ]),
            Err(HeaderError::ClosedEpochNotEarlier {
                epoch: epoch(3),
                closed: epoch(5)
            })
        );
    }

    #[test]
    fn a_receipt_starts_at_or_before_its_end() {
        let (start, next) = (Offset::from_u64(9), Offset::from_u64(8));
        assert_eq!(
            Receipt::new(start, next, false),
            Err(HeaderError::ReceiptRangeInverted { start, next })
        );
        assert_eq!(
            parse(200, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("stream-incarnation", TOKEN),
                ("stream-offset", "00000000000000000009"),
                ("stream-next-offset", "00000000000000000008"),
            ]),
            Err(HeaderError::ReceiptRangeInverted { start, next })
        );
    }

    #[test]
    fn a_gap_without_its_expected_seq_and_stray_403_headers_are_malformed() {
        assert_eq!(
            parse(409, &[("producer-received-seq", "4")]),
            Err(HeaderError::Missing {
                name: "producer-expected-seq"
            })
        );
        for (stray, name) in [
            (("producer-seq", "1"), "producer-seq"),
            (("producer-expected-seq", "1"), "producer-expected-seq"),
            (("producer-received-seq", "1"), "producer-received-seq"),
        ] {
            assert_eq!(
                parse(403, &[
                    ("producer-epoch", "3"),
                    ("producer-outcome", "key-conflict"),
                    stray
                ]),
                Err(HeaderError::UnexpectedHeader { name, status: 403 }),
                "{name}"
            );
        }
        assert_eq!(
            parse(403, &[
                ("producer-epoch", "3"),
                ("producer-outcome", "stale-epoch"),
                ("producer-closed-epoch", "1"),
                ("producer-closed-seq", "2"),
            ]),
            Err(HeaderError::UnexpectedHeader {
                name: "producer-closed-epoch",
                status: 403
            })
        );
    }

    #[test]
    fn malformed_answers_are_typed() {
        let receipt = |extra: &[(&'static str, &'static str)]| {
            let mut pairs = vec![
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("stream-incarnation", TOKEN),
                ("stream-offset", "00000000000000000000"),
                ("stream-next-offset", "00000000000000000001"),
            ];
            for (name, value) in extra {
                pairs.retain(|(old, _)| old != name);
                pairs.push((name, value));
            }
            pairs
        };
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
                ("stream-incarnation", TOKEN),
                ("producer-outcome", "receipt-evicted"),
            ]),
            Err(HeaderError::OutcomeStatusMismatch { status: 200 })
        );
        assert_eq!(
            parse(204, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("stream-incarnation", TOKEN)
            ]),
            Err(HeaderError::Missing {
                name: "stream-offset"
            })
        );
        assert_eq!(
            parse(204, &[
                ("producer-epoch", "1"),
                ("producer-seq", "1"),
                ("producer-outcome", "receipt-evicted"),
            ]),
            Err(HeaderError::Missing {
                name: "stream-incarnation"
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
            Err(HeaderError::UnexpectedExpectedSeq { expected: seq(3) })
        );
        for (extra, expected) in [
            (("stream-closed", "yes"), HeaderError::InvalidFlag {
                name: "stream-closed",
            }),
            (("stream-closed", "TRUE"), HeaderError::InvalidFlag {
                name: "stream-closed",
            }),
            (("stream-offset", "0"), HeaderError::InvalidOffset {
                name: "stream-offset",
            }),
            (
                ("stream-incarnation", "07"),
                HeaderError::NonCanonicalInteger {
                    name: "stream-incarnation",
                },
            ),
            (
                ("producer-epoch", "004"),
                HeaderError::NonCanonicalInteger {
                    name: "producer-epoch",
                },
            ),
            (("producer-seq", "01"), HeaderError::NonCanonicalInteger {
                name: "producer-seq",
            }),
        ] {
            assert_eq!(parse(200, &receipt(&[extra])), Err(expected), "{extra:?}");
        }
        assert_eq!(
            parse(403, &[
                ("producer-epoch", "3"),
                ("producer-outcome", "epoch-closed;by=claim"),
                ("producer-closed-epoch", "02"),
                ("producer-closed-seq", "1"),
            ]),
            Err(HeaderError::NonCanonicalInteger {
                name: "producer-closed-epoch"
            })
        );
    }
}
