//! Refusals of other rules that a client acts on: the `412` of the
//! `Stream-Incarnation` precondition (extensions Section 2.4), the `409` of
//! an append to a closed stream (base protocol), and the `409` of a
//! `Stream-Seq` that is not above the stream's last one (Section 5.8).

use crate::error::HeaderError;
use crate::fields;
use crate::names;
use crate::value::Incarnation;
use crate::value::Offset;
use crate::value::StreamSeq;

/// `412 Precondition Failed` of the `Stream-Incarnation` precondition.
/// `current` is the stream's current token, absent when no stream exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncarnationMismatch {
    pub current: Option<Incarnation>,
}

impl IncarnationMismatch {
    pub const STATUS: u16 = 412;

    /// `Ok(None)` for any other status.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if status != Self::STATUS {
            return Ok(None);
        }
        let [current] = fields::collect(headers, [names::STREAM_INCARNATION])?;
        Ok(Some(Self {
            current: fields::incarnation(names::STREAM_INCARNATION, current)?,
        }))
    }

    /// Emits `Stream-Incarnation` when the stream exists.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        if let Some(current) = self.current {
            emit(names::STREAM_INCARNATION, &current.to_string());
        }
    }
}

/// `409 Conflict` with `Stream-Closed: true`: the stream is closed and the
/// append did not apply. `final_offset` is its `Stream-Next-Offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamClosedConflict {
    pub final_offset: Offset,
}

impl StreamClosedConflict {
    pub const STATUS: u16 = 409;

    /// `Ok(None)` for any other status, and for a `409` without
    /// `Stream-Closed`, which another rule refused.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if status != Self::STATUS {
            return Ok(None);
        }
        let [closed, next] =
            fields::collect(headers, [names::STREAM_CLOSED, names::STREAM_NEXT_OFFSET])?;
        if !fields::flag(names::STREAM_CLOSED, closed)? {
            return Ok(None);
        }
        Ok(Some(Self {
            final_offset: fields::required(
                names::STREAM_NEXT_OFFSET,
                fields::offset(names::STREAM_NEXT_OFFSET, next)?,
            )?,
        }))
    }

    /// Emits `Stream-Closed` and `Stream-Next-Offset`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_CLOSED, "true");
        emit(names::STREAM_NEXT_OFFSET, &self.final_offset.to_string());
    }
}

/// `409 Conflict` of a `Stream-Seq` that is not above the stream's last
/// accepted one, which `last` echoes: a client using the sequence as a
/// compare-and-set retries from it. Only a stream that accepted a
/// `Stream-Seq` refuses one, so the header is always there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSeqConflict {
    pub last: StreamSeq,
}

impl StreamSeqConflict {
    pub const STATUS: u16 = 409;

    /// `Ok(None)` for any other status, and for a `409` without
    /// `Stream-Last-Seq`, which another rule refused.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if status != Self::STATUS {
            return Ok(None);
        }
        let [last] = fields::collect(headers, [names::STREAM_LAST_SEQ])?;
        fields::text(names::STREAM_LAST_SEQ, last)?
            .map(|last| StreamSeq::new(last).map(|last| Self { last }))
            .transpose()
    }

    /// Emits `Stream-Last-Seq`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_LAST_SEQ, self.last.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::IncarnationMismatch;
    use super::StreamClosedConflict;
    use super::StreamSeqConflict;
    use crate::error::HeaderError;
    use crate::value::Incarnation;
    use crate::value::Offset;

    fn pairs<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Iterator<Item = (&'a str, &'a [u8])> {
        pairs.iter().map(|(name, value)| (*name, value.as_bytes()))
    }

    #[test]
    fn refusals_round_trip() {
        for mismatch in [IncarnationMismatch { current: None }, IncarnationMismatch {
            current: Some(Incarnation::try_from(42).unwrap()),
        }] {
            let mut out = Vec::new();
            mismatch.render(|name, value| out.push((name, value.to_owned())));
            let out: Vec<(&str, &str)> = out.iter().map(|(n, v)| (*n, v.as_str())).collect();
            assert_eq!(
                IncarnationMismatch::parse(412, pairs(&out)),
                Ok(Some(mismatch))
            );
        }
        let conflict = StreamClosedConflict {
            final_offset: Offset::from_u64(12),
        };
        let mut out = Vec::new();
        conflict.render(|name, value| out.push((name, value.to_owned())));
        let out: Vec<(&str, &str)> = out.iter().map(|(n, v)| (*n, v.as_str())).collect();
        assert_eq!(
            StreamClosedConflict::parse(409, pairs(&out)),
            Ok(Some(conflict))
        );
    }

    #[test]
    fn a_stream_seq_conflict_echoes_the_last_seq() {
        let conflict = StreamSeqConflict {
            last: crate::value::StreamSeq::new("refs/heads/main@0042").unwrap(),
        };
        let mut out = Vec::new();
        conflict.render(|name, value| out.push((name, value.to_owned())));
        assert_eq!(out, vec![(
            "stream-last-seq",
            "refs/heads/main@0042".to_owned()
        )]);
        let out: Vec<(&str, &str)> = out.iter().map(|(n, v)| (*n, v.as_str())).collect();
        assert_eq!(
            StreamSeqConflict::parse(409, pairs(&out)),
            Ok(Some(conflict))
        );
        // A content-type mismatch `409` carries no last seq.
        assert_eq!(
            StreamSeqConflict::parse(409, pairs(&[("stream-next-offset", "1")])),
            Ok(None)
        );
        assert_eq!(
            StreamSeqConflict::parse(412, pairs(&[("stream-last-seq", "a")])),
            Ok(None)
        );
        assert_eq!(
            StreamSeqConflict::parse(409, pairs(&[("stream-last-seq", "")])),
            Err(HeaderError::InvalidStreamSeq)
        );
        assert_eq!(
            StreamSeqConflict::parse(
                409,
                pairs(&[("stream-last-seq", "a"), ("stream-last-seq", "b")])
            ),
            Err(HeaderError::Duplicate {
                name: "stream-last-seq"
            })
        );
        assert_eq!(
            crate::value::StreamSeq::new("x".repeat(257)),
            Err(HeaderError::InvalidStreamSeq)
        );
    }

    #[test]
    fn refusals_skip_or_reject_what_they_do_not_type() {
        assert_eq!(
            IncarnationMismatch::parse(409, pairs(&[("stream-incarnation", "x")])),
            Ok(None)
        );
        assert_eq!(
            IncarnationMismatch::parse(412, pairs(&[("stream-incarnation", "0")])),
            Err(HeaderError::OutOfRange {
                name: "stream-incarnation",
                value: 0
            })
        );
        assert_eq!(
            StreamClosedConflict::parse(409, pairs(&[("producer-expected-seq", "1")])),
            Ok(None)
        );
        assert_eq!(
            StreamClosedConflict::parse(409, pairs(&[("stream-closed", "true")])),
            Err(HeaderError::Missing {
                name: "stream-next-offset"
            })
        );
        assert_eq!(
            StreamClosedConflict::parse(409, pairs(&[("stream-closed", "yes")])),
            Err(HeaderError::InvalidFlag {
                name: "stream-closed"
            })
        );
    }
}
