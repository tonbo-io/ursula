//! HEAD metadata (extensions Sections 2.4 and 10).
//!
//! Every HEAD answer, `404` included, carries the [`ServerIdentity`], so a
//! client tells a host alias routed to another deployment from a deleted
//! stream. A successful HEAD of a stream also carries the [`StreamHead`].

use std::fmt;

use crate::error::HeaderError;
use crate::fields;
use crate::names;
use crate::snapshot::SnapshotRef;
use crate::value::render_decimal;
use crate::value::render_offset;

/// The contract version this crate implements (`Ursula-Protocol`). A server
/// that sends none speaks version 1.
pub const PROTOCOL_VERSION: u32 = 2;

/// The longest `Ursula-Cluster-Id`, in bytes.
pub const CLUSTER_ID_MAX_BYTES: usize = 64;

/// The id of the deployment that serves a route: 1 to
/// [`CLUSTER_ID_MAX_BYTES`] bytes of `[A-Za-z0-9._-]`, compared byte for
/// byte.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn new(id: impl Into<String>) -> Result<Self, HeaderError> {
        let id = id.into();
        if id.is_empty()
            || id.len() > CLUSTER_ID_MAX_BYTES
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(HeaderError::InvalidClusterId);
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `Ursula-Protocol` and `Ursula-Cluster-Id`, on every HEAD answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerIdentity {
    /// The contract version the server speaks. A client refuses one below
    /// [`PROTOCOL_VERSION`].
    pub protocol: u32,
    pub cluster_id: ClusterId,
}

impl ServerIdentity {
    /// The identity this crate's server sends.
    pub fn current(cluster_id: ClusterId) -> Self {
        Self {
            protocol: PROTOCOL_VERSION,
            cluster_id,
        }
    }

    /// `Ok(None)` when neither header is present: a server of protocol 1,
    /// or a layer that is not Ursula. One without the other is
    /// [`HeaderError::Missing`].
    pub fn parse<'a, I>(headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [protocol, cluster_id] =
            fields::collect(headers, [names::URSULA_PROTOCOL, names::URSULA_CLUSTER_ID])?;
        let protocol = fields::decimal(names::URSULA_PROTOCOL, protocol)?
            .map(|protocol| {
                u32::try_from(protocol).map_err(|_overflow| HeaderError::IntegerTooLarge {
                    name: names::URSULA_PROTOCOL,
                })
            })
            .transpose()?;
        let cluster_id = fields::text(names::URSULA_CLUSTER_ID, cluster_id)?
            .map(ClusterId::new)
            .transpose()?;
        Ok(fields::pair(
            (names::URSULA_PROTOCOL, protocol),
            (names::URSULA_CLUSTER_ID, cluster_id),
        )?
        .map(|(protocol, cluster_id)| Self {
            protocol,
            cluster_id,
        }))
    }

    /// Emits `Ursula-Protocol` and `Ursula-Cluster-Id`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(
            names::URSULA_PROTOCOL,
            &render_decimal(u64::from(self.protocol)),
        );
        emit(names::URSULA_CLUSTER_ID, self.cluster_id.as_str());
    }
}

/// What a stream's current history began from, after a history event
/// (`Stream-Previous-Incarnation`, `Stream-Inherited-Offset`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inherited {
    /// The stream's token in the history the current one began from.
    pub previous_incarnation: u64,
    /// Bytes below this offset are the same under both tokens. It lies in
    /// the offset space of the previous token, so a client compares its own
    /// offsets with it by order.
    pub offset: u64,
}

/// The stream metadata of a successful HEAD of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamHead {
    /// `Stream-Incarnation`.
    pub incarnation: u64,
    /// `Stream-Next-Offset`: the stream's tail.
    pub next_offset: u64,
    /// `Stream-Retained-Offset`: the earliest offset the stream retains.
    pub retained_offset: u64,
    /// `Stream-Closed`.
    pub closed: bool,
    /// The latest published snapshot, if any.
    pub snapshot: Option<SnapshotRef>,
    /// Present after a history event.
    pub inherited: Option<Inherited>,
    /// `Ursula-Max-Request-Bytes`, as lowered by the gateways in front.
    pub max_request_bytes: u64,
    /// `Ursula-Max-Snapshot-Bytes`: present when the server accepts
    /// snapshots in parts.
    pub max_snapshot_bytes: Option<u64>,
}

impl StreamHead {
    pub fn parse<'a, I>(headers: I) -> Result<Self, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [
            incarnation,
            next,
            retained,
            closed,
            snapshot_offset,
            snapshot_digest,
            previous,
            inherited,
            max_request,
            max_snapshot,
        ] = fields::collect(headers, [
            names::STREAM_INCARNATION,
            names::STREAM_NEXT_OFFSET,
            names::STREAM_RETAINED_OFFSET,
            names::STREAM_CLOSED,
            names::STREAM_SNAPSHOT_OFFSET,
            names::STREAM_SNAPSHOT_DIGEST,
            names::STREAM_PREVIOUS_INCARNATION,
            names::STREAM_INHERITED_OFFSET,
            names::URSULA_MAX_REQUEST_BYTES,
            names::URSULA_MAX_SNAPSHOT_BYTES,
        ])?;
        let inherited = fields::pair(
            (
                names::STREAM_PREVIOUS_INCARNATION,
                fields::token(names::STREAM_PREVIOUS_INCARNATION, previous)?,
            ),
            (
                names::STREAM_INHERITED_OFFSET,
                fields::offset(names::STREAM_INHERITED_OFFSET, inherited)?,
            ),
        )?
        .map(|(previous_incarnation, offset)| Inherited {
            previous_incarnation,
            offset,
        });
        Ok(Self {
            incarnation: fields::required(
                names::STREAM_INCARNATION,
                fields::token(names::STREAM_INCARNATION, incarnation)?,
            )?,
            next_offset: fields::required(
                names::STREAM_NEXT_OFFSET,
                fields::offset(names::STREAM_NEXT_OFFSET, next)?,
            )?,
            retained_offset: fields::required(
                names::STREAM_RETAINED_OFFSET,
                fields::offset(names::STREAM_RETAINED_OFFSET, retained)?,
            )?,
            closed: fields::flag(names::STREAM_CLOSED, closed)?,
            snapshot: SnapshotRef::from_fields(snapshot_offset, snapshot_digest)?,
            inherited,
            max_request_bytes: fields::required(
                names::URSULA_MAX_REQUEST_BYTES,
                fields::decimal(names::URSULA_MAX_REQUEST_BYTES, max_request)?,
            )?,
            max_snapshot_bytes: fields::decimal(names::URSULA_MAX_SNAPSHOT_BYTES, max_snapshot)?,
        })
    }

    /// Emits every header this value carries, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_INCARNATION, &render_decimal(self.incarnation));
        emit(names::STREAM_NEXT_OFFSET, &render_offset(self.next_offset));
        emit(
            names::STREAM_RETAINED_OFFSET,
            &render_offset(self.retained_offset),
        );
        if self.closed {
            emit(names::STREAM_CLOSED, "true");
        }
        if let Some(snapshot) = &self.snapshot {
            snapshot.render_into(&mut emit);
        }
        if let Some(inherited) = &self.inherited {
            emit(
                names::STREAM_PREVIOUS_INCARNATION,
                &render_decimal(inherited.previous_incarnation),
            );
            emit(
                names::STREAM_INHERITED_OFFSET,
                &render_offset(inherited.offset),
            );
        }
        emit(
            names::URSULA_MAX_REQUEST_BYTES,
            &render_decimal(self.max_request_bytes),
        );
        if let Some(max_snapshot_bytes) = self.max_snapshot_bytes {
            emit(
                names::URSULA_MAX_SNAPSHOT_BYTES,
                &render_decimal(max_snapshot_bytes),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterId;
    use super::Inherited;
    use super::PROTOCOL_VERSION;
    use super::ServerIdentity;
    use super::StreamHead;
    use crate::error::HeaderError;
    use crate::snapshot::SnapshotRef;
    use crate::value::Digest;

    type Pairs = Vec<(&'static str, String)>;

    fn iter(pairs: &Pairs) -> impl Iterator<Item = (&str, &[u8])> {
        pairs.iter().map(|(name, value)| (*name, value.as_bytes()))
    }

    fn pairs_of(pairs: &[(&'static str, &str)]) -> Pairs {
        pairs
            .iter()
            .map(|(name, value)| (*name, (*value).to_owned()))
            .collect()
    }

    #[test]
    fn identities_round_trip() {
        for identity in [
            ServerIdentity::current(ClusterId::new("prod-us-east-1.a_1").unwrap()),
            ServerIdentity {
                protocol: u32::MAX,
                cluster_id: ClusterId::new(format!("u{}", "f".repeat(32))).unwrap(),
            },
            ServerIdentity {
                protocol: 0,
                cluster_id: ClusterId::new("X".repeat(64)).unwrap(),
            },
        ] {
            let mut pairs = Pairs::new();
            identity.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(ServerIdentity::parse(iter(&pairs)), Ok(Some(identity)));
        }
        let mut pairs = Pairs::new();
        ServerIdentity::current(ClusterId::new("c").unwrap())
            .render(|name, value| pairs.push((name, value.to_owned())));
        assert_eq!(
            pairs,
            pairs_of(&[("ursula-protocol", "2"), ("ursula-cluster-id", "c")])
        );
        assert_eq!(PROTOCOL_VERSION, 2);
    }

    #[test]
    fn a_server_without_identity_is_protocol_one() {
        assert_eq!(ServerIdentity::parse(iter(&Pairs::new())), Ok(None));
    }

    #[test]
    fn malformed_identities_are_typed() {
        for (pairs, expected) in [
            (
                pairs_of(&[("ursula-protocol", "2")]),
                HeaderError::Missing {
                    name: "ursula-cluster-id",
                },
            ),
            (
                pairs_of(&[("ursula-cluster-id", "c")]),
                HeaderError::Missing {
                    name: "ursula-protocol",
                },
            ),
            (
                pairs_of(&[("ursula-protocol", "02"), ("ursula-cluster-id", "c")]),
                HeaderError::NonCanonicalInteger {
                    name: "ursula-protocol",
                },
            ),
            (
                pairs_of(&[("ursula-protocol", "v2"), ("ursula-cluster-id", "c")]),
                HeaderError::InvalidInteger {
                    name: "ursula-protocol",
                },
            ),
            (
                pairs_of(&[
                    ("ursula-protocol", "4294967296"),
                    ("ursula-cluster-id", "c"),
                ]),
                HeaderError::IntegerTooLarge {
                    name: "ursula-protocol",
                },
            ),
            (
                pairs_of(&[("ursula-protocol", "2"), ("ursula-cluster-id", "")]),
                HeaderError::InvalidClusterId,
            ),
            (
                pairs_of(&[("ursula-protocol", "2"), ("ursula-cluster-id", "a/b")]),
                HeaderError::InvalidClusterId,
            ),
            (
                pairs_of(&[("ursula-protocol", "2"), ("ursula-cluster-id", "a b")]),
                HeaderError::InvalidClusterId,
            ),
            (
                pairs_of(&[
                    ("ursula-protocol", "2"),
                    ("ursula-cluster-id", &"x".repeat(65)),
                ]),
                HeaderError::InvalidClusterId,
            ),
            (
                pairs_of(&[
                    ("ursula-protocol", "2"),
                    ("ursula-cluster-id", "c"),
                    ("Ursula-Cluster-Id", "c"),
                ]),
                HeaderError::Duplicate {
                    name: "ursula-cluster-id",
                },
            ),
        ] {
            assert_eq!(
                ServerIdentity::parse(iter(&pairs)),
                Err(expected),
                "{pairs:?}"
            );
        }
    }

    fn every_head() -> Vec<StreamHead> {
        let snapshot = SnapshotRef {
            offset: 4096,
            digest: Digest::from_bytes([0x11; 32]),
        };
        vec![
            StreamHead {
                incarnation: 4_503_599_627_370_497,
                next_offset: 0,
                retained_offset: 0,
                closed: false,
                snapshot: None,
                inherited: None,
                max_request_bytes: 33_554_432,
                max_snapshot_bytes: None,
            },
            StreamHead {
                incarnation: 9_007_199_254_740_991,
                next_offset: 8192,
                retained_offset: 4096,
                closed: true,
                snapshot: Some(snapshot),
                inherited: Some(Inherited {
                    previous_incarnation: 4_503_599_627_370_496,
                    offset: 6000,
                }),
                max_request_bytes: 1_048_576,
                max_snapshot_bytes: Some(1_073_741_824),
            },
            StreamHead {
                incarnation: u64::MAX,
                next_offset: u64::MAX,
                retained_offset: u64::MAX,
                closed: false,
                snapshot: Some(snapshot),
                inherited: Some(Inherited {
                    previous_incarnation: u64::MAX,
                    offset: u64::MAX,
                }),
                max_request_bytes: u64::MAX,
                max_snapshot_bytes: Some(u64::MAX),
            },
        ]
    }

    #[test]
    fn heads_round_trip() {
        for head in every_head() {
            let mut pairs = Pairs::new();
            head.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(StreamHead::parse(iter(&pairs)), Ok(head), "{pairs:?}");
        }
    }

    #[test]
    fn heads_render_offsets_padded_and_tokens_canonical() {
        let mut pairs = Pairs::new();
        every_head()[1].render(|name, value| pairs.push((name, value.to_owned())));
        assert!(pairs.contains(&("stream-incarnation", "9007199254740991".to_owned())));
        assert!(pairs.contains(&("stream-next-offset", "00000000000000008192".to_owned())));
        assert!(pairs.contains(&("stream-retained-offset", "00000000000000004096".to_owned())));
        assert!(pairs.contains(&("stream-previous-incarnation", "4503599627370496".to_owned())));
        assert!(pairs.contains(&("stream-inherited-offset", "00000000000000006000".to_owned())));
        assert!(pairs.contains(&("ursula-max-snapshot-bytes", "1073741824".to_owned())));
    }

    #[test]
    fn malformed_heads_are_typed() {
        let full = || {
            let mut pairs = Pairs::new();
            every_head()[1].render(|name, value| pairs.push((name, value.to_owned())));
            pairs
        };
        for required in [
            "stream-incarnation",
            "stream-next-offset",
            "stream-retained-offset",
            "ursula-max-request-bytes",
        ] {
            let pairs: Pairs = full()
                .into_iter()
                .filter(|(name, _)| *name != required)
                .collect();
            assert_eq!(
                StreamHead::parse(iter(&pairs)),
                Err(HeaderError::Missing { name: required })
            );
        }
        for (dropped, missing) in [
            ("stream-previous-incarnation", "stream-previous-incarnation"),
            ("stream-inherited-offset", "stream-inherited-offset"),
            ("stream-snapshot-offset", "stream-snapshot-offset"),
            ("stream-snapshot-digest", "stream-snapshot-digest"),
        ] {
            let pairs: Pairs = full()
                .into_iter()
                .filter(|(name, _)| *name != dropped)
                .collect();
            assert_eq!(
                StreamHead::parse(iter(&pairs)),
                Err(HeaderError::Missing { name: missing }),
                "{dropped}"
            );
        }
        for (name, value, expected) in [
            ("stream-next-offset", "8192", HeaderError::InvalidOffset {
                name: "stream-next-offset",
            }),
            (
                "stream-inherited-offset",
                "-0000000000000006000",
                HeaderError::InvalidOffset {
                    name: "stream-inherited-offset",
                },
            ),
            (
                "stream-previous-incarnation",
                "04503599627370496",
                HeaderError::NonCanonicalInteger {
                    name: "stream-previous-incarnation",
                },
            ),
            (
                "stream-incarnation",
                "+9007199254740991",
                HeaderError::InvalidInteger {
                    name: "stream-incarnation",
                },
            ),
            (
                "ursula-max-request-bytes",
                "033554432",
                HeaderError::NonCanonicalInteger {
                    name: "ursula-max-request-bytes",
                },
            ),
            (
                "ursula-max-snapshot-bytes",
                "1 GiB",
                HeaderError::InvalidInteger {
                    name: "ursula-max-snapshot-bytes",
                },
            ),
            ("stream-closed", "false", HeaderError::InvalidFlag {
                name: "stream-closed",
            }),
        ] {
            let pairs: Pairs = full()
                .into_iter()
                .map(|(key, old)| (key, if key == name { value.to_owned() } else { old }))
                .collect();
            assert_eq!(StreamHead::parse(iter(&pairs)), Err(expected), "{name}");
        }
    }
}
