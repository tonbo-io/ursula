//! The typed `413` (extensions Section 10): every size refusal names the
//! limit it applied, by that limit's header set to its value.

use crate::error::HeaderError;
use crate::fields;
use crate::names;

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

    /// Parses the limit a `413` names. `Ok(None)` for any other status, and
    /// for a `413` that names none: the refusal came from a layer that does
    /// not type its `413`, such as a proxy with its own body limit. A
    /// successful HEAD carries `Ursula-Max-Request-Bytes` too, so only the
    /// status tells a refusal from metadata.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if status != Self::STATUS {
            return Ok(None);
        }
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
        emit(self.header_name(), &self.bytes().to_string());
    }
}

/// The `Ursula-Max-Request-Bytes` an intermediary forwards on a response
/// that carries it (Section 10): the smaller of the received value and
/// `limit`, the intermediary's own limit for the request. `Ok(None)` when the
/// response carries none, and then the intermediary adds none. `Err` when
/// the received value is malformed or repeated.
pub fn lowered_request_limit<'a, I>(headers: I, limit: u64) -> Result<Option<u64>, HeaderError>
where I: IntoIterator<Item = (&'a str, &'a [u8])> {
    let [received] = fields::collect(headers, [names::URSULA_MAX_REQUEST_BYTES])?;
    Ok(fields::decimal(names::URSULA_MAX_REQUEST_BYTES, received)?
        .map(|received| received.min(limit)))
}

#[cfg(test)]
mod tests {
    use super::SizeLimit;
    use super::lowered_request_limit;
    use crate::error::HeaderError;

    fn parse(pairs: &[(&str, &str)]) -> Result<Option<SizeLimit>, HeaderError> {
        SizeLimit::parse(
            SizeLimit::STATUS,
            pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
        )
    }

    #[test]
    fn only_a_413_names_a_limit() {
        // A successful HEAD carries the request limit as metadata.
        for status in [200, 400, 503] {
            assert_eq!(
                SizeLimit::parse(status, [(
                    "ursula-max-request-bytes",
                    b"33554432".as_slice()
                )]),
                Ok(None),
                "{status}"
            );
        }
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

    #[test]
    fn an_intermediary_lowers_the_request_limit_it_receives() {
        let lowered = |pairs: &[(&str, &str)], limit| {
            lowered_request_limit(
                pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
                limit,
            )
        };
        let received = [("Ursula-Max-Request-Bytes", "33554432")];
        assert_eq!(lowered(&received, 1_048_576), Ok(Some(1_048_576)));
        assert_eq!(lowered(&received, 33_554_432), Ok(Some(33_554_432)));
        assert_eq!(lowered(&received, u64::MAX), Ok(Some(33_554_432)));
        assert_eq!(lowered(&[("content-type", "text/plain")], 1), Ok(None));
        assert_eq!(
            lowered(&[("ursula-max-request-bytes", "1x")], 1),
            Err(HeaderError::InvalidInteger {
                name: "ursula-max-request-bytes",
            })
        );
        assert_eq!(
            lowered(
                &[
                    ("ursula-max-request-bytes", "1"),
                    ("ursula-max-request-bytes", "2"),
                ],
                1
            ),
            Err(HeaderError::Duplicate {
                name: "ursula-max-request-bytes",
            })
        );
    }
}
