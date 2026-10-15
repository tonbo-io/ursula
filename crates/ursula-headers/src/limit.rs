//! The typed `413` (extensions Section 10): every size refusal names the
//! limit it applied, by that limit's header set to its value.

use crate::error::HeaderError;
use crate::fields;
use crate::names;
use crate::value::render_decimal;

/// The limit a `413 Payload Too Large` applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeLimit {
    /// `Ursula-Max-Request-Bytes`: a request body exceeded it.
    Request(u64),
    /// `Ursula-Max-Snapshot-Bytes`: a snapshot assembled from parts exceeded
    /// it.
    Snapshot(u64),
}

impl SizeLimit {
    /// The status of every size refusal.
    pub const STATUS: u16 = 413;

    /// The limit, in bytes.
    pub fn bytes(self) -> u64 {
        match self {
            Self::Request(bytes) | Self::Snapshot(bytes) => bytes,
        }
    }

    pub fn header_name(self) -> &'static str {
        match self {
            Self::Request(_) => names::URSULA_MAX_REQUEST_BYTES,
            Self::Snapshot(_) => names::URSULA_MAX_SNAPSHOT_BYTES,
        }
    }

    /// Parses the limit a `413` names. `Ok(None)` when it names none: the
    /// refusal came from a layer that does not type its `413`, such as a
    /// proxy with its own body limit.
    pub fn parse<'a, I>(headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [request, snapshot] = fields::collect(headers, [
            names::URSULA_MAX_REQUEST_BYTES,
            names::URSULA_MAX_SNAPSHOT_BYTES,
        ])?;
        match (
            fields::decimal(names::URSULA_MAX_REQUEST_BYTES, request)?,
            fields::decimal(names::URSULA_MAX_SNAPSHOT_BYTES, snapshot)?,
        ) {
            (None, None) => Ok(None),
            (Some(bytes), None) => Ok(Some(Self::Request(bytes))),
            (None, Some(bytes)) => Ok(Some(Self::Snapshot(bytes))),
            (Some(_), Some(_)) => Err(HeaderError::AmbiguousLimit),
        }
    }

    /// Emits the header that names the limit.
    pub fn render(self, mut emit: impl FnMut(&'static str, &str)) {
        emit(self.header_name(), &render_decimal(self.bytes()));
    }
}

#[cfg(test)]
mod tests {
    use super::SizeLimit;
    use crate::error::HeaderError;

    fn parse(pairs: &[(&str, &str)]) -> Result<Option<SizeLimit>, HeaderError> {
        SizeLimit::parse(pairs.iter().map(|(name, value)| (*name, value.as_bytes())))
    }

    #[test]
    fn limits_round_trip() {
        for limit in [
            SizeLimit::Request(33_554_432),
            SizeLimit::Request(0),
            SizeLimit::Snapshot(1_073_741_824),
            SizeLimit::Snapshot(u64::MAX),
        ] {
            let mut pairs = Vec::new();
            limit.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].0, limit.header_name());
            let pairs: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(name, value)| (*name, value.as_str()))
                .collect();
            assert_eq!(parse(&pairs), Ok(Some(limit)));
        }
        assert_eq!(
            parse(&[("Ursula-Max-Request-Bytes", "33554432")]),
            Ok(Some(SizeLimit::Request(33_554_432)))
        );
    }

    #[test]
    fn an_untyped_413_names_no_limit() {
        assert_eq!(parse(&[("content-type", "text/plain")]), Ok(None));
    }

    #[test]
    fn malformed_limits_are_typed() {
        for (pairs, expected) in [
            (
                &[
                    ("ursula-max-request-bytes", "1"),
                    ("ursula-max-snapshot-bytes", "2"),
                ][..],
                HeaderError::AmbiguousLimit,
            ),
            (
                &[("ursula-max-request-bytes", "032")][..],
                HeaderError::NonCanonicalInteger {
                    name: "ursula-max-request-bytes",
                },
            ),
            (
                &[("ursula-max-snapshot-bytes", "-1")][..],
                HeaderError::InvalidInteger {
                    name: "ursula-max-snapshot-bytes",
                },
            ),
            (
                &[("ursula-max-snapshot-bytes", "18446744073709551616")][..],
                HeaderError::IntegerTooLarge {
                    name: "ursula-max-snapshot-bytes",
                },
            ),
            (
                &[
                    ("ursula-max-request-bytes", "1"),
                    ("ursula-max-request-bytes", "1"),
                ][..],
                HeaderError::Duplicate {
                    name: "ursula-max-request-bytes",
                },
            ),
        ] {
            assert_eq!(parse(pairs), Err(expected), "{pairs:?}");
        }
    }
}
