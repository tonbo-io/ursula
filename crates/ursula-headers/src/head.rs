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
use crate::value::Incarnation;
use crate::value::Offset;

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
                u32::try_from(protocol).map_err(|_overflow| HeaderError::U32TooLarge {
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
        emit(names::URSULA_PROTOCOL, &self.protocol.to_string());
        emit(names::URSULA_CLUSTER_ID, self.cluster_id.as_str());
    }
}

/// What a stream's current history began from, after a history event
/// (`Stream-Previous-Incarnation`, `Stream-Inherited-Offset`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inherited {
    /// The stream's token in the history the current one began from.
    pub previous_incarnation: Incarnation,
    /// Bytes below this offset are the same under both tokens. It lies in
    /// the offset space of the previous token, so a client compares its own
    /// offsets with it by order.
    pub offset: Offset,
}

/// The stream metadata of a successful HEAD of a stream.
///
/// The fields keep the invariants a parse checks: the retained offset and
/// the inherited offset are at most the tail, and the previous token is not
/// the current one. [`StreamHead::new`] and the `with_*` methods enforce
/// them, so every value renders headers that parse back to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamHead {
    incarnation: Incarnation,
    next_offset: Offset,
    retained_offset: Offset,
    closed: bool,
    snapshot: Option<SnapshotRef>,
    inherited: Option<Inherited>,
    max_request_bytes: u64,
    max_snapshot_bytes: Option<u64>,
}

impl StreamHead {
    /// An open stream with no snapshot and no history event. The retained
    /// offset must be at most the tail.
    pub fn new(
        incarnation: Incarnation,
        next_offset: Offset,
        retained_offset: Offset,
        max_request_bytes: u64,
    ) -> Result<Self, HeaderError> {
        if retained_offset > next_offset {
            return Err(HeaderError::RetainedPastNext {
                retained: retained_offset,
                next: next_offset,
            });
        }
        Ok(Self {
            incarnation,
            next_offset,
            retained_offset,
            closed: false,
            snapshot: None,
            inherited: None,
            max_request_bytes,
            max_snapshot_bytes: None,
        })
    }

    #[must_use]
    pub const fn with_closed(mut self, closed: bool) -> Self {
        self.closed = closed;
        self
    }

    /// The latest published snapshot, whose offset must be at most the
    /// tail.
    pub fn with_snapshot(mut self, snapshot: Option<SnapshotRef>) -> Result<Self, HeaderError> {
        if let Some(snapshot) = snapshot
            && snapshot.offset > self.next_offset
        {
            return Err(HeaderError::SnapshotPastNext {
                snapshot: snapshot.offset,
                next: self.next_offset,
            });
        }
        self.snapshot = snapshot;
        Ok(self)
    }

    #[must_use]
    pub const fn with_max_snapshot_bytes(mut self, max_snapshot_bytes: Option<u64>) -> Self {
        self.max_snapshot_bytes = max_snapshot_bytes;
        self
    }

    /// The history the stream began from. The inherited offset must be at
    /// most the tail, and the previous token must differ from the current.
    pub fn with_inherited(mut self, inherited: Option<Inherited>) -> Result<Self, HeaderError> {
        if let Some(inherited) = inherited {
            if inherited.offset > self.next_offset {
                return Err(HeaderError::InheritedPastNext {
                    inherited: inherited.offset,
                    next: self.next_offset,
                });
            }
            if inherited.previous_incarnation == self.incarnation {
                return Err(HeaderError::PreviousIncarnationIsCurrent);
            }
        }
        self.inherited = inherited;
        Ok(self)
    }

    /// `Stream-Incarnation`.
    pub const fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// `Stream-Next-Offset`: the stream's tail.
    pub const fn next_offset(&self) -> Offset {
        self.next_offset
    }

    /// `Stream-Retained-Offset`: the earliest offset the stream retains.
    pub const fn retained_offset(&self) -> Offset {
        self.retained_offset
    }

    /// `Stream-Closed`.
    pub const fn closed(&self) -> bool {
        self.closed
    }

    /// The latest published snapshot, if any.
    pub const fn snapshot(&self) -> Option<SnapshotRef> {
        self.snapshot
    }

    /// Present after a history event.
    pub const fn inherited(&self) -> Option<Inherited> {
        self.inherited
    }

    /// `Ursula-Max-Request-Bytes`, as lowered by the gateways in front.
    pub const fn max_request_bytes(&self) -> u64 {
        self.max_request_bytes
    }

    /// `Ursula-Max-Snapshot-Bytes`: present when the server accepts
    /// snapshots in parts.
    pub const fn max_snapshot_bytes(&self) -> Option<u64> {
        self.max_snapshot_bytes
    }

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
                fields::incarnation(names::STREAM_PREVIOUS_INCARNATION, previous)?,
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
        Self::new(
            fields::required(
                names::STREAM_INCARNATION,
                fields::incarnation(names::STREAM_INCARNATION, incarnation)?,
            )?,
            fields::required(
                names::STREAM_NEXT_OFFSET,
                fields::offset(names::STREAM_NEXT_OFFSET, next)?,
            )?,
            fields::required(
                names::STREAM_RETAINED_OFFSET,
                fields::offset(names::STREAM_RETAINED_OFFSET, retained)?,
            )?,
            fields::required(
                names::URSULA_MAX_REQUEST_BYTES,
                fields::decimal(names::URSULA_MAX_REQUEST_BYTES, max_request)?,
            )?,
        )?
        .with_closed(fields::flag(names::STREAM_CLOSED, closed)?)
        .with_snapshot(SnapshotRef::from_fields(snapshot_offset, snapshot_digest)?)?
        .with_max_snapshot_bytes(fields::decimal(
            names::URSULA_MAX_SNAPSHOT_BYTES,
            max_snapshot,
        )?)
        .with_inherited(inherited)
    }

    /// Emits every header this value carries, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_INCARNATION, &self.incarnation.to_string());
        emit(names::STREAM_NEXT_OFFSET, &self.next_offset.to_string());
        emit(
            names::STREAM_RETAINED_OFFSET,
            &self.retained_offset.to_string(),
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
                &inherited.previous_incarnation.to_string(),
            );
            emit(
                names::STREAM_INHERITED_OFFSET,
                &inherited.offset.to_string(),
            );
        }
        emit(
            names::URSULA_MAX_REQUEST_BYTES,
            &self.max_request_bytes.to_string(),
        );
        if let Some(max_snapshot_bytes) = self.max_snapshot_bytes {
            emit(
                names::URSULA_MAX_SNAPSHOT_BYTES,
                &max_snapshot_bytes.to_string(),
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
    use crate::value::Incarnation;
    use crate::value::Offset;
    use crate::value::SnapshotDigest;

    fn token(value: u64) -> Incarnation {
        Incarnation::try_from(value).unwrap()
    }

    fn offset(value: u64) -> Offset {
        Offset::from_u64(value)
    }

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
                HeaderError::U32TooLarge {
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
            offset: offset(4096),
            digest: SnapshotDigest::from_bytes([0x11; 32]),
        };
        vec![
            StreamHead::new(
                token(4_503_599_627_370_497),
                offset(0),
                offset(0),
                33_554_432,
            )
            .unwrap(),
            StreamHead::new(
                token(9_007_199_254_740_991),
                offset(8192),
                offset(4096),
                1_048_576,
            )
            .unwrap()
            .with_closed(true)
            .with_snapshot(Some(snapshot))
            .unwrap()
            .with_max_snapshot_bytes(Some(1_073_741_824))
            .with_inherited(Some(Inherited {
                previous_incarnation: token(4_503_599_627_370_496),
                offset: offset(6000),
            }))
            .unwrap(),
            StreamHead::new(token(1), offset(u64::MAX), offset(u64::MAX), u64::MAX)
                .unwrap()
                .with_snapshot(Some(snapshot))
                .unwrap()
                .with_max_snapshot_bytes(Some(u64::MAX))
                .with_inherited(Some(Inherited {
                    previous_incarnation: token(2),
                    offset: offset(u64::MAX),
                }))
                .unwrap(),
        ]
    }

    #[test]
    fn heads_keep_their_invariants() {
        assert_eq!(
            StreamHead::new(token(1), offset(5), offset(6), 1),
            Err(HeaderError::RetainedPastNext {
                retained: offset(6),
                next: offset(5)
            })
        );
        let head = StreamHead::new(token(1), offset(5), offset(0), 1).unwrap();
        assert_eq!(
            head.with_inherited(Some(Inherited {
                previous_incarnation: token(2),
                offset: offset(6),
            })),
            Err(HeaderError::InheritedPastNext {
                inherited: offset(6),
                next: offset(5)
            })
        );
        assert_eq!(
            head.with_inherited(Some(Inherited {
                previous_incarnation: token(1),
                offset: offset(5),
            })),
            Err(HeaderError::PreviousIncarnationIsCurrent)
        );
        let snapshot = crate::snapshot::SnapshotRef {
            offset: offset(6),
            digest: crate::value::SnapshotDigest::from_bytes([1; 32]),
        };
        assert_eq!(
            head.with_snapshot(Some(snapshot)),
            Err(HeaderError::SnapshotPastNext {
                snapshot: offset(6),
                next: offset(5)
            })
        );
        let full = || {
            let mut pairs = Pairs::new();
            every_head()[1].render(|name, value| pairs.push((name, value.to_owned())));
            pairs
        };
        for (name, value, expected) in [
            (
                "stream-retained-offset",
                "00000000000000008193",
                HeaderError::RetainedPastNext {
                    retained: offset(8193),
                    next: offset(8192),
                },
            ),
            (
                "stream-inherited-offset",
                "00000000000000008193",
                HeaderError::InheritedPastNext {
                    inherited: offset(8193),
                    next: offset(8192),
                },
            ),
            (
                "stream-previous-incarnation",
                "9007199254740991",
                HeaderError::PreviousIncarnationIsCurrent,
            ),
        ] {
            let pairs: Pairs = full()
                .into_iter()
                .map(|(key, old)| (key, if key == name { value.to_owned() } else { old }))
                .collect();
            assert_eq!(StreamHead::parse(iter(&pairs)), Err(expected), "{name}");
        }
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
            ("stream-closed", "True", HeaderError::InvalidFlag {
                name: "stream-closed",
            }),
            ("stream-incarnation", "0", HeaderError::OutOfRange {
                name: "stream-incarnation",
                value: 0,
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
