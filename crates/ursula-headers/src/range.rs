//! Byte ranges on a snapshot read (extensions Section 2.5): the one `Range`
//! a read honors, and the `Content-Range` of its `206` or `416`.
//!
//! A snapshot read honors a single range, `bytes=a-b`, `bytes=a-` or
//! `bytes=-n` (RFC 9110 section 14.1.2). Anything else, several ranges, a
//! value that does not parse, or a request that carries `If-Range`, gets the
//! whole body as `200`, which RFC 9110 lets a server do. Stream reads keep
//! their offset protocol and ignore `Range`.

use std::fmt;

use crate::error::HeaderError;
use crate::fields;
use crate::names;

/// The value of `Accept-Ranges` on a snapshot answer.
pub const ACCEPT_RANGES_BYTES: &str = "bytes";

/// The one byte range a snapshot read honors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=first-last`, with `first <= last`.
    Bounded { first: u64, last: u64 },
    /// `bytes=first-`: from `first` to the end.
    From { first: u64 },
    /// `bytes=-len`: the last `len` bytes.
    Suffix { len: u64 },
}

impl ByteRange {
    /// The range a request asks for and a snapshot read honors. `None` when
    /// it carries no `Range`, several, one with several ranges, another unit
    /// or a value that does not parse, or `If-Range`: the read then answers
    /// the whole body.
    pub fn parse<'a, I>(headers: I) -> Option<Self>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [range, if_range] = fields::collect(headers, [names::RANGE, names::IF_RANGE]).ok()?;
        if if_range.is_some() {
            return None;
        }
        Self::parse_value(fields::text(names::RANGE, range).ok()??)
    }

    fn parse_value(value: &str) -> Option<Self> {
        let (unit, spec) = value.split_once('=')?;
        if !unit.trim_matches([' ', '\t']).eq_ignore_ascii_case("bytes") {
            return None;
        }
        let (first, last) = spec.trim_matches([' ', '\t']).split_once('-')?;
        match (position(first), position(last)) {
            (Some(first), Some(last)) if first <= last => Some(Self::Bounded { first, last }),
            (Some(first), None) if last.is_empty() => Some(Self::From { first }),
            (None, Some(len)) if first.is_empty() => Some(Self::Suffix { len }),
            _ => None,
        }
    }

    /// The answer to this range on a body of `total` bytes: the bytes it
    /// selects, `416` when it selects none, or `None` for the whole body as
    /// `200`. A nonzero suffix of an empty body is satisfiable (RFC 9110
    /// section 14.1.1), but no `206` can express it, so it gets the empty
    /// body whole.
    pub fn resolve(self, total: u64) -> Option<ContentRange> {
        let unsatisfied = ContentRange::Unsatisfied { total };
        let Some(end) = total.checked_sub(1) else {
            return match self {
                Self::Suffix { len } if len > 0 => None,
                _ => Some(unsatisfied),
            };
        };
        let (first, last) = match self {
            Self::Bounded { first, last } => (first, last.min(end)),
            Self::From { first } => (first, end),
            Self::Suffix { len: 0 } => return Some(unsatisfied),
            Self::Suffix { len } => (total.saturating_sub(len), end),
        };
        Some(ByteSpan::new(first, last, total).map_or(unsatisfied, ContentRange::Partial))
    }

    /// Emits the `Range` header that asks for this range.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        let value = match self {
            Self::Bounded { first, last } => format!("bytes={first}-{last}"),
            Self::From { first } => format!("bytes={first}-"),
            Self::Suffix { len } => format!("bytes=-{len}"),
        };
        emit(names::RANGE, &value);
    }
}

/// A plain decimal byte position: digits only.
fn position(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// The bytes `first..=last` of a body of `total` bytes: `first <= last <
/// total`, so the span is never empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSpan {
    first: u64,
    last: u64,
    total: u64,
}

impl ByteSpan {
    pub fn new(first: u64, last: u64, total: u64) -> Result<Self, HeaderError> {
        if first > last || last >= total {
            return Err(HeaderError::InvalidContentRange);
        }
        Ok(Self { first, last, total })
    }

    pub const fn first(&self) -> u64 {
        self.first
    }

    pub const fn last(&self) -> u64 {
        self.last
    }

    pub const fn total(&self) -> u64 {
        self.total
    }

    /// How many bytes the span holds, at least 1.
    pub const fn len(&self) -> u64 {
        // `first <= last < total <= u64::MAX`, so neither step saturates.
        self.last.saturating_sub(self.first).saturating_add(1)
    }

    /// Never: a span holds at least one byte.
    pub const fn is_empty(&self) -> bool {
        false
    }
}

/// The `Content-Range` of a ranged snapshot answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentRange {
    /// `206 Partial Content` with `bytes first-last/total`.
    Partial(ByteSpan),
    /// `416 Range Not Satisfiable` with `bytes */total`.
    Unsatisfied { total: u64 },
}

impl ContentRange {
    pub fn status(&self) -> u16 {
        match self {
            Self::Partial(_) => 206,
            Self::Unsatisfied { .. } => 416,
        }
    }

    /// Emits `Content-Range`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::CONTENT_RANGE, &self.to_string());
    }

    /// Parses the `Content-Range` of a `206` or `416`. `Ok(None)` for any
    /// other status.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if !matches!(status, 206 | 416) {
            return Ok(None);
        }
        let [value] = fields::collect(headers, [names::CONTENT_RANGE])?;
        let value = fields::required(
            names::CONTENT_RANGE,
            fields::text(names::CONTENT_RANGE, value)?,
        )?;
        let parsed = Self::parse_value(value).ok_or(HeaderError::InvalidContentRange)?;
        if parsed.status() != status {
            return Err(HeaderError::InvalidContentRange);
        }
        Ok(Some(parsed))
    }

    fn parse_value(value: &str) -> Option<Self> {
        let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
        let total = position(total)?;
        if range == "*" {
            return Some(Self::Unsatisfied { total });
        }
        let (first, last) = range.split_once('-')?;
        ByteSpan::new(position(first)?, position(last)?, total)
            .ok()
            .map(Self::Partial)
    }
}

impl fmt::Display for ContentRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Partial(span) => write!(f, "bytes {}-{}/{}", span.first, span.last, span.total),
            Self::Unsatisfied { total } => write!(f, "bytes */{total}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ByteRange;
    use super::ByteSpan;
    use super::ContentRange;
    use crate::error::HeaderError;

    fn parse(pairs: &[(&str, &str)]) -> Option<ByteRange> {
        ByteRange::parse(pairs.iter().map(|(name, value)| (*name, value.as_bytes())))
    }

    fn span(first: u64, last: u64, total: u64) -> ContentRange {
        ContentRange::Partial(ByteSpan::new(first, last, total).unwrap())
    }

    #[test]
    fn single_ranges_parse() {
        for (value, expected) in [
            ("bytes=0-55", ByteRange::Bounded { first: 0, last: 55 }),
            ("bytes=7-7", ByteRange::Bounded { first: 7, last: 7 }),
            ("Bytes = 3-", ByteRange::From { first: 3 }),
            ("bytes=-9", ByteRange::Suffix { len: 9 }),
            ("bytes=-0", ByteRange::Suffix { len: 0 }),
        ] {
            assert_eq!(parse(&[("Range", value)]), Some(expected), "{value}");
        }
    }

    #[test]
    fn what_a_read_does_not_honor_answers_the_whole_body() {
        for value in [
            "bytes=0-1,4-5",
            "bytes=5-2",
            "bytes=-",
            "bytes=a-b",
            "bytes=+1-2",
            "bytes=0-18446744073709551616",
            "items=0-5",
            "bytes 0-5",
            "",
        ] {
            assert_eq!(parse(&[("range", value)]), None, "{value:?}");
        }
        assert_eq!(
            parse(&[("range", "bytes=0-1"), ("range", "bytes=2-3")]),
            None,
            "two Range headers"
        );
        assert_eq!(
            parse(&[("range", "bytes=0-1"), ("if-range", "\"etag\"")]),
            None,
            "If-Range"
        );
        assert_eq!(parse(&[]), None);
    }

    #[test]
    fn ranges_resolve_against_the_body() {
        for (range, total, expected) in [
            (
                ByteRange::Bounded { first: 0, last: 55 },
                100,
                span(0, 55, 100),
            ),
            (
                ByteRange::Bounded {
                    first: 90,
                    last: 200,
                },
                100,
                span(90, 99, 100),
            ),
            (
                ByteRange::Bounded {
                    first: 100,
                    last: 200,
                },
                100,
                ContentRange::Unsatisfied { total: 100 },
            ),
            (ByteRange::From { first: 99 }, 100, span(99, 99, 100)),
            (
                ByteRange::From { first: 100 },
                100,
                ContentRange::Unsatisfied { total: 100 },
            ),
            (ByteRange::Suffix { len: 10 }, 100, span(90, 99, 100)),
            (ByteRange::Suffix { len: 500 }, 100, span(0, 99, 100)),
            (
                ByteRange::Suffix { len: 0 },
                100,
                ContentRange::Unsatisfied { total: 100 },
            ),
            (ByteRange::From { first: 0 }, 0, ContentRange::Unsatisfied {
                total: 0,
            }),
            (ByteRange::Suffix { len: 0 }, 0, ContentRange::Unsatisfied {
                total: 0,
            }),
        ] {
            assert_eq!(range.resolve(total), Some(expected), "{range:?} of {total}");
        }
        // A nonzero suffix of an empty body selects it whole.
        assert_eq!(ByteRange::Suffix { len: 1 }.resolve(0), None);
    }

    #[test]
    fn ranges_and_content_ranges_round_trip() {
        for range in [
            ByteRange::Bounded { first: 0, last: 55 },
            ByteRange::From { first: u64::MAX },
            ByteRange::Suffix { len: 1 },
        ] {
            let mut pairs = Vec::new();
            range.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(
                ByteRange::parse(pairs.iter().map(|(name, value)| (*name, value.as_bytes()))),
                Some(range)
            );
        }
        for answer in [
            span(0, 0, 1),
            span(5, 9, u64::MAX),
            ContentRange::Unsatisfied { total: 0 },
            ContentRange::Unsatisfied { total: 42 },
        ] {
            let mut pairs = Vec::new();
            answer.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(
                ContentRange::parse(
                    answer.status(),
                    pairs.iter().map(|(name, value)| (*name, value.as_bytes()))
                ),
                Ok(Some(answer))
            );
        }
        assert_eq!(span(5, 9, 10).to_string(), "bytes 5-9/10");
        assert_eq!(
            ContentRange::Unsatisfied { total: 10 }.to_string(),
            "bytes */10"
        );
    }

    #[test]
    fn malformed_content_ranges_are_typed() {
        let parse = |status: u16, value: &str| {
            ContentRange::parse(status, [("content-range", value.as_bytes())])
        };
        assert_eq!(parse(200, "bytes 0-1/2"), Ok(None));
        for (status, value) in [
            (206, "bytes 2-1/5"),
            (206, "bytes 0-5/5"),
            (206, "bytes */5"),
            (416, "bytes 0-1/5"),
            (206, "items 0-1/5"),
            (206, "bytes 0-1/*"),
        ] {
            assert_eq!(
                parse(status, value),
                Err(HeaderError::InvalidContentRange),
                "{status} {value}"
            );
        }
        assert_eq!(
            ContentRange::parse(206, []),
            Err(HeaderError::Missing {
                name: "content-range"
            })
        );
        assert_eq!(
            ByteSpan::new(0, 0, 0),
            Err(HeaderError::InvalidContentRange)
        );
        assert_eq!(ByteSpan::new(0, 9, 10).unwrap().len(), 10);
    }
}
