//! The typed `413` (extensions Section 10): every size refusal names the
//! limit it applied, by that limit's header set to its value. The `429` of
//! a stream that holds as many staged snapshot parts as it may (Section 2.2)
//! names its limit the same way.

use std::time::Duration;

use crate::error::HeaderError;
use crate::error_body::ErrorCode;
use crate::fields;
use crate::names;
use crate::snapshot::PartCount;

/// The request limit of the protocol, `Ursula-Max-Request-Bytes` (32 MiB):
/// the largest request body any request to any stream may carry. A node
/// accepts exactly this much. An intermediary may lower it for its own
/// clients and never raises it: a body the node refuses for its size must
/// meet the intermediary's own `413` first, since the node answers it
/// before reading the body and the intermediary, still sending it, may see
/// a reset instead (extensions Section 10).
pub const MAX_REQUEST_BYTES: u64 = 32 << 20;

/// The snapshot limit of an Ursula node with a cold store,
/// `Ursula-Max-Snapshot-Bytes` (1 GiB): the largest snapshot it accepts in
/// parts (extensions Section 10). A node without a cold store sends none.
pub const MAX_SNAPSHOT_BYTES: u64 = 1 << 30;

/// The slowest pace at which a node is expected to store or copy the bytes
/// a request makes it write before it answers (8 MiB/s): a body it writes
/// to object storage whole, or the parts a commit copies. Whoever waits for
/// an answer's head, a gateway or a client, waits its own base wait plus
/// [`answer_allowance`].
pub const STORE_FLOOR_BYTES_PER_SECOND: u64 = 8 << 20;

/// How much longer than a base wait the head of a request's answer may
/// take while the node stores or copies the request's bytes at
/// [`STORE_FLOOR_BYTES_PER_SECOND`], to the millisecond. The bytes are its
/// `body_bytes`, or for a parts commit of `commit` parts the most it copies
/// ([`PartCount::copy_bytes`]). A 32 MiB body gets 4 s, and a commit of 32
/// parts or more 128 s.
pub fn answer_allowance(body_bytes: u64, commit: Option<PartCount>) -> Duration {
    let bytes = commit.map_or(body_bytes, PartCount::copy_bytes);
    Duration::from_millis(bytes.saturating_mul(1_000) / STORE_FLOOR_BYTES_PER_SECOND)
}

/// The limit a `413 Payload Too Large` applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeLimit {
    /// `Ursula-Max-Request-Bytes`: a request body exceeded it.
    Request(u64),
    /// `Ursula-Max-Snapshot-Bytes`: a snapshot assembled from parts
    /// exceeded it.
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

    /// The code of the refusal's error body.
    pub fn error_code(self) -> ErrorCode {
        match self {
            Self::Request(_) => ErrorCode::RequestTooLarge,
            Self::Snapshot(_) => ErrorCode::SnapshotTooLarge,
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
            (Some(_), Some(_)) => Err(HeaderError::AmbiguousLimit {
                first: names::URSULA_MAX_REQUEST_BYTES,
                second: names::URSULA_MAX_SNAPSHOT_BYTES,
            }),
        }
    }

    /// Emits the header that names the limit.
    pub fn render(self, mut emit: impl FnMut(&'static str, &str)) {
        emit(self.header_name(), &self.bytes().to_string());
    }
}

/// The staged-parts limit a `429 Too Many Requests` of a part upload
/// applied (Section 2.2): the stream holds as many staged, unused parts, or
/// as many bytes of them, as it may.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedPartsLimit {
    /// `Ursula-Max-Staged-Parts`: the stream holds this many parts.
    Parts(u64),
    /// `Ursula-Max-Staged-Bytes`: the part would take the stream's staged
    /// bytes above this.
    Bytes(u64),
}

impl StagedPartsLimit {
    /// The status of a staged-parts refusal.
    pub const STATUS: u16 = 429;

    /// The limit: a number of parts or of bytes.
    pub fn value(self) -> u64 {
        match self {
            Self::Parts(value) | Self::Bytes(value) => value,
        }
    }

    /// The code of the refusal's error body.
    pub fn error_code(self) -> ErrorCode {
        ErrorCode::StagedPartsLimit
    }

    pub fn header_name(self) -> &'static str {
        match self {
            Self::Parts(_) => names::URSULA_MAX_STAGED_PARTS,
            Self::Bytes(_) => names::URSULA_MAX_STAGED_BYTES,
        }
    }

    /// Parses the limit a `429` names. `Ok(None)` for any other status, and
    /// for a `429` that names none: another rule refused, such as the
    /// producer cap or a gateway's tenant limit.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if status != Self::STATUS {
            return Ok(None);
        }
        let [parts, bytes] = fields::collect(headers, [
            names::URSULA_MAX_STAGED_PARTS,
            names::URSULA_MAX_STAGED_BYTES,
        ])?;
        match (
            fields::decimal(names::URSULA_MAX_STAGED_PARTS, parts)?,
            fields::decimal(names::URSULA_MAX_STAGED_BYTES, bytes)?,
        ) {
            (None, None) => Ok(None),
            (Some(parts), None) => Ok(Some(Self::Parts(parts))),
            (None, Some(bytes)) => Ok(Some(Self::Bytes(bytes))),
            (Some(_), Some(_)) => Err(HeaderError::AmbiguousLimit {
                first: names::URSULA_MAX_STAGED_PARTS,
                second: names::URSULA_MAX_STAGED_BYTES,
            }),
        }
    }

    /// Emits the header that names the limit.
    pub fn render(self, mut emit: impl FnMut(&'static str, &str)) {
        emit(self.header_name(), &self.value().to_string());
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
    use super::StagedPartsLimit;
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
                HeaderError::AmbiguousLimit {
                    first: "ursula-max-request-bytes",
                    second: "ursula-max-snapshot-bytes",
                },
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
    fn staged_parts_limits_round_trip() {
        for limit in [
            StagedPartsLimit::Parts(8192),
            StagedPartsLimit::Bytes(2_147_483_648),
            StagedPartsLimit::Bytes(0),
        ] {
            let mut pairs = Vec::new();
            limit.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(pairs.len(), 1);
            let pairs: Vec<(&str, &[u8])> = pairs
                .iter()
                .map(|(name, value)| (*name, value.as_bytes()))
                .collect();
            assert_eq!(
                StagedPartsLimit::parse(429, pairs.iter().copied()),
                Ok(Some(limit))
            );
            assert_eq!(StagedPartsLimit::parse(503, pairs), Ok(None));
        }
        // The producer cap and a gateway's tenant limit name no staged limit.
        assert_eq!(
            StagedPartsLimit::parse(429, [("retry-after", b"1".as_slice())]),
            Ok(None)
        );
        assert_eq!(
            StagedPartsLimit::parse(429, [
                ("ursula-max-staged-parts", b"1".as_slice()),
                ("Ursula-Max-Staged-Bytes", b"2".as_slice()),
            ]),
            Err(HeaderError::AmbiguousLimit {
                first: "ursula-max-staged-parts",
                second: "ursula-max-staged-bytes",
            })
        );
        assert_eq!(
            StagedPartsLimit::parse(429, [("ursula-max-staged-parts", b"08".as_slice())]),
            Err(HeaderError::NonCanonicalInteger {
                name: "ursula-max-staged-parts",
            })
        );
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
