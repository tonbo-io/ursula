//! Immutable reads (extensions Section 4): the two resources of a stream
//! incarnation, named by its token and an offset in the path, the headers of
//! their answers with the `Stream-Immutable` marker, and the deployment's
//! `Stream-Read-Origin` hint.

use std::fmt;

use crate::error::HeaderError;
use crate::fields;
use crate::names;
use crate::snapshot::SnapshotRef;
use crate::value::Incarnation;
use crate::value::Offset;

/// The path segment that starts an immutable resource after the stream URL.
pub const INCARNATION_SEGMENT: &str = "incarnation";
/// The path segment of a chunk, after the token.
pub const CHUNK_SEGMENT: &str = "chunk";
/// The path segment of a snapshot, after the token.
pub const SNAPSHOT_SEGMENT: &str = "snapshot";

/// Which resource of an incarnation a path names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    /// `{stream_url}/incarnation/{token}/chunk/{offset}`: the stream's bytes
    /// from `offset` to the end of the chunk that holds it, or to the tail.
    Chunk,
    /// `{stream_url}/incarnation/{token}/snapshot/{offset}`: the snapshot
    /// published at `offset`.
    Snapshot,
}

impl ResourceKind {
    fn segment(self) -> &'static str {
        match self {
            Self::Chunk => CHUNK_SEGMENT,
            Self::Snapshot => SNAPSHOT_SEGMENT,
        }
    }

    /// The header the path's offset is echoed in.
    fn offset_header(self) -> &'static str {
        match self {
            Self::Chunk => names::STREAM_OFFSET,
            Self::Snapshot => names::STREAM_SNAPSHOT_OFFSET,
        }
    }
}

/// An immutable resource: what follows `{stream_url}` in its path. The token
/// and the offset are in the path, so every cache key tells two resources
/// apart, and the resource takes no query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImmutableResource {
    pub kind: ResourceKind,
    /// The `Stream-Incarnation` token the resource belongs to: the read's
    /// precondition.
    pub incarnation: Incarnation,
    /// The chunk's first offset, or the snapshot's offset.
    pub offset: Offset,
}

impl ImmutableResource {
    /// Parses the token and offset segments of a path whose kind the router
    /// matched. Both must be as the server issues them: the token canonical
    /// decimal, the offset exactly [`OFFSET_WIDTH`](crate::OFFSET_WIDTH)
    /// digits, so that one resource has one URL.
    pub fn parse(kind: ResourceKind, token: &str, offset: &str) -> Result<Self, HeaderError> {
        Ok(Self {
            kind,
            incarnation: Incarnation::parse(names::STREAM_INCARNATION, token)?,
            offset: Offset::parse(kind.offset_header(), offset)?,
        })
    }

    /// The path after `{stream_url}`, starting with `/`.
    pub fn path(&self) -> String {
        format!(
            "/{INCARNATION_SEGMENT}/{}/{}/{}",
            self.incarnation,
            self.kind.segment(),
            self.offset
        )
    }
}

/// What the marker says about a chunk answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChunkMark {
    /// No marker: the answer ends at the tail of an open stream, which lies
    /// within the chunk. A reader continues at the stream's own origin.
    Unmarked,
    /// `Stream-Immutable: true`: the answer reaches the end of its chunk
    /// strictly below the tail. A reader continues at `Stream-Next-Offset`.
    Immutable,
    /// `Stream-Immutable: true` with `Stream-Closed: true`: the answer ends
    /// at the tail of a closed stream, and nothing follows it.
    ClosedTail,
}

/// The headers of a chunk answer, `200` or `304` (extensions Section 4.1),
/// besides `Content-Type` and `ETag`.
///
/// `Stream-Offset` is the first offset the body holds, equal to the path's;
/// the body is exactly the stream's bytes in `[offset, next)`. Only a
/// closed tail may be empty and marked: an empty answer below the tail is
/// impossible, and one at an open tail is unmarked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkRead {
    incarnation: Incarnation,
    offset: Offset,
    next: Offset,
    mark: ChunkMark,
}

impl ChunkRead {
    pub fn new(
        incarnation: Incarnation,
        offset: Offset,
        next: Offset,
        mark: ChunkMark,
    ) -> Result<Self, HeaderError> {
        if next < offset {
            return Err(HeaderError::ChunkRangeInverted { offset, next });
        }
        if next == offset && mark == ChunkMark::Immutable {
            return Err(HeaderError::EmptyImmutableChunk { offset });
        }
        Ok(Self {
            incarnation,
            offset,
            next,
            mark,
        })
    }

    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    pub fn offset(&self) -> Offset {
        self.offset
    }

    pub fn next(&self) -> Offset {
        self.next
    }

    pub fn mark(&self) -> ChunkMark {
        self.mark
    }

    /// The body's length: `Stream-Next-Offset - Stream-Offset`.
    pub fn len(&self) -> u64 {
        // `new` keeps `next` at or above `offset`.
        self.next.get().saturating_sub(self.offset.get())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether this answer is one for `resource`: a chunk of the same token
    /// starting at the same offset (extensions Section 4.4, client rules).
    /// A client that reads from a read origin accepts nothing else, so a
    /// cache that merged two URLs can only make it fall back.
    pub fn answers(&self, resource: &ImmutableResource) -> bool {
        resource.kind == ResourceKind::Chunk
            && resource.incarnation == self.incarnation
            && resource.offset == self.offset
    }

    pub fn parse<'a, I>(headers: I) -> Result<Self, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [incarnation, offset, next, immutable, closed] = fields::collect(headers, [
            names::STREAM_INCARNATION,
            names::STREAM_OFFSET,
            names::STREAM_NEXT_OFFSET,
            names::STREAM_IMMUTABLE,
            names::STREAM_CLOSED,
        ])?;
        let mark = match (
            fields::flag(names::STREAM_IMMUTABLE, immutable)?,
            fields::flag(names::STREAM_CLOSED, closed)?,
        ) {
            (false, false) => ChunkMark::Unmarked,
            (true, false) => ChunkMark::Immutable,
            (true, true) => ChunkMark::ClosedTail,
            (false, true) => return Err(HeaderError::ClosedWithoutMarker),
        };
        Self::new(
            fields::required(
                names::STREAM_INCARNATION,
                fields::incarnation(names::STREAM_INCARNATION, incarnation)?,
            )?,
            fields::required(
                names::STREAM_OFFSET,
                fields::offset(names::STREAM_OFFSET, offset)?,
            )?,
            fields::required(
                names::STREAM_NEXT_OFFSET,
                fields::offset(names::STREAM_NEXT_OFFSET, next)?,
            )?,
            mark,
        )
    }

    /// Emits every header of the answer, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_INCARNATION, &self.incarnation.to_string());
        emit(names::STREAM_OFFSET, &self.offset.to_string());
        emit(names::STREAM_NEXT_OFFSET, &self.next.to_string());
        match self.mark {
            ChunkMark::Unmarked => {}
            ChunkMark::Immutable => emit(names::STREAM_IMMUTABLE, "true"),
            ChunkMark::ClosedTail => {
                emit(names::STREAM_IMMUTABLE, "true");
                emit(names::STREAM_CLOSED, "true");
            }
        }
    }
}

/// The headers of a snapshot answer of the immutable resource, `200`, `206`
/// or `304` (extensions Section 4.1), besides `Content-Type`, `ETag` and the
/// byte-range headers. It is always marked, and never carries
/// `Stream-Up-To-Date`, whose value changes once the stream grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableSnapshotRead {
    pub snapshot: SnapshotRef,
    pub incarnation: Incarnation,
}

impl ImmutableSnapshotRead {
    pub fn parse<'a, I>(headers: I) -> Result<Self, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [offset, digest, next, incarnation, immutable] = fields::collect(headers, [
            names::STREAM_SNAPSHOT_OFFSET,
            names::STREAM_SNAPSHOT_DIGEST,
            names::STREAM_NEXT_OFFSET,
            names::STREAM_INCARNATION,
            names::STREAM_IMMUTABLE,
        ])?;
        let snapshot = fields::required(
            names::STREAM_SNAPSHOT_OFFSET,
            SnapshotRef::from_fields(offset, digest)?,
        )?;
        let next = fields::required(
            names::STREAM_NEXT_OFFSET,
            fields::offset(names::STREAM_NEXT_OFFSET, next)?,
        )?;
        if next != snapshot.offset {
            return Err(HeaderError::SnapshotNextOffsetMismatch {
                snapshot: snapshot.offset,
                next,
            });
        }
        if !fields::flag(names::STREAM_IMMUTABLE, immutable)? {
            return Err(HeaderError::Missing {
                name: names::STREAM_IMMUTABLE,
            });
        }
        Ok(Self {
            snapshot,
            incarnation: fields::required(
                names::STREAM_INCARNATION,
                fields::incarnation(names::STREAM_INCARNATION, incarnation)?,
            )?,
        })
    }

    /// Whether this answer is one for `resource`: the snapshot of the same
    /// token at the same offset.
    pub fn answers(&self, resource: &ImmutableResource) -> bool {
        resource.kind == ResourceKind::Snapshot
            && resource.incarnation == self.incarnation
            && resource.offset == self.snapshot.offset
    }

    /// Emits every header of the answer, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        self.snapshot.render_into(&mut emit);
        emit(names::STREAM_NEXT_OFFSET, &self.snapshot.offset.to_string());
        emit(names::STREAM_INCARNATION, &self.incarnation.to_string());
        emit(names::STREAM_IMMUTABLE, "true");
    }
}

/// The scheme of a [`ReadOrigin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OriginScheme {
    Https,
    /// Parsed so that a client can tell it apart and ignore it: clients
    /// follow only an `https` hint.
    Http,
}

/// `Stream-Read-Origin` on a stream's `HEAD`, sent by the deployment, never
/// by an Ursula node (extensions Section 4.4): an origin where the immutable
/// resources of the stream are served at the same paths.
///
/// An origin is a scheme, a host and an optional port, with nothing else:
/// no user, path, query or fragment. Hosts are DNS names or IPv4 addresses,
/// labels of 1 to 63 letters, digits and inner hyphens joined by single
/// dots, or IPv6 addresses in brackets. A port is canonical
/// decimal from 1 to 65535.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadOrigin {
    scheme: OriginScheme,
    host: String,
    port: Option<u16>,
}

impl ReadOrigin {
    pub fn new(
        scheme: OriginScheme,
        host: impl Into<String>,
        port: Option<u16>,
    ) -> Result<Self, HeaderError> {
        let host = host.into();
        if !Self::valid_host(&host) || port == Some(0) {
            return Err(HeaderError::InvalidReadOrigin);
        }
        Ok(Self { scheme, host, port })
    }

    fn valid_host(host: &str) -> bool {
        if let Some(inner) = host.strip_prefix('[') {
            return inner.strip_suffix(']').is_some_and(|address| {
                !address.is_empty()
                    && address.contains(':')
                    && address
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() || byte == b':' || byte == b'.')
            });
        }
        // A DNS name or an IPv4 address: labels of 1 to 63 letters, digits
        // and inner hyphens, joined by single dots.
        host.len() <= 253
            && host.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
    }

    pub fn scheme(&self) -> OriginScheme {
        self.scheme
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Whether a client may follow the hint at all: only an `https` origin.
    pub fn is_https(&self) -> bool {
        self.scheme == OriginScheme::Https
    }

    /// Parses an origin's text.
    pub fn parse(text: &str) -> Result<Self, HeaderError> {
        let (scheme, rest) = if let Some(rest) = text.strip_prefix("https://") {
            (OriginScheme::Https, rest)
        } else if let Some(rest) = text.strip_prefix("http://") {
            (OriginScheme::Http, rest)
        } else {
            return Err(HeaderError::InvalidReadOrigin);
        };
        // A bracketed IPv6 host holds colons; the port follows its bracket.
        let (host, port) = match rest.rfind(':') {
            Some(colon) if !rest.ends_with(']') => {
                let (host, port) = rest.split_at(colon);
                let port = port.strip_prefix(':').unwrap_or(port);
                let port = crate::value::parse_decimal(names::STREAM_READ_ORIGIN, port)
                    .ok()
                    .and_then(|port| u16::try_from(port).ok())
                    .ok_or(HeaderError::InvalidReadOrigin)?;
                (host, Some(port))
            }
            _ => (rest, None),
        };
        Self::new(scheme, host, port)
    }

    /// `Stream-Read-Origin` from a `HEAD` answer's headers, or `None` when
    /// absent.
    pub fn parse_header<'a, I>(headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [origin] = fields::collect(headers, [names::STREAM_READ_ORIGIN])?;
        fields::text(names::STREAM_READ_ORIGIN, origin)?
            .map(Self::parse)
            .transpose()
    }

    /// Emits `Stream-Read-Origin`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_READ_ORIGIN, &self.to_string());
    }
}

impl fmt::Display for ReadOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scheme = match self.scheme {
            OriginScheme::Https => "https",
            OriginScheme::Http => "http",
        };
        write!(f, "{scheme}://{}", self.host)?;
        if let Some(port) = self.port {
            write!(f, ":{port}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkMark;
    use super::ChunkRead;
    use super::ImmutableResource;
    use super::ImmutableSnapshotRead;
    use super::OriginScheme;
    use super::ReadOrigin;
    use super::ResourceKind;
    use crate::error::HeaderError;
    use crate::snapshot::SnapshotRef;
    use crate::value::Incarnation;
    use crate::value::Offset;
    use crate::value::SnapshotDigest;

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

    fn token() -> Incarnation {
        Incarnation::try_from(4_503_599_627_370_497).unwrap()
    }

    #[test]
    fn resources_have_one_path() {
        let chunk = ImmutableResource {
            kind: ResourceKind::Chunk,
            incarnation: token(),
            offset: Offset::from_u64(4_194_304),
        };
        assert_eq!(
            chunk.path(),
            "/incarnation/4503599627370497/chunk/00000000000004194304"
        );
        assert_eq!(
            ImmutableResource::parse(
                ResourceKind::Chunk,
                "4503599627370497",
                "00000000000004194304"
            ),
            Ok(chunk)
        );
        let snapshot = ImmutableResource {
            kind: ResourceKind::Snapshot,
            ..chunk
        };
        assert_eq!(
            snapshot.path(),
            "/incarnation/4503599627370497/snapshot/00000000000004194304"
        );
        for (token, offset, expected) in [
            (
                "04503599627370497",
                "00000000000004194304",
                HeaderError::NonCanonicalInteger {
                    name: "stream-incarnation",
                },
            ),
            ("0", "00000000000004194304", HeaderError::OutOfRange {
                name: "stream-incarnation",
                value: 0,
            }),
            ("x", "00000000000004194304", HeaderError::InvalidInteger {
                name: "stream-incarnation",
            }),
            ("4503599627370497", "4194304", HeaderError::InvalidOffset {
                name: "stream-offset",
            }),
            ("4503599627370497", "-1", HeaderError::InvalidOffset {
                name: "stream-offset",
            }),
            (
                "4503599627370497",
                "+0000000000004194304",
                HeaderError::InvalidOffset {
                    name: "stream-offset",
                },
            ),
        ] {
            assert_eq!(
                ImmutableResource::parse(ResourceKind::Chunk, token, offset),
                Err(expected),
                "{token} {offset}"
            );
        }
        assert_eq!(
            ImmutableResource::parse(ResourceKind::Snapshot, "4503599627370497", "1"),
            Err(HeaderError::InvalidOffset {
                name: "stream-snapshot-offset"
            })
        );
    }

    #[test]
    fn chunk_reads_round_trip() {
        for (offset, next, mark) in [
            (0, 4_194_304, ChunkMark::Immutable),
            (5, 4_194_304, ChunkMark::Immutable),
            (0, 9, ChunkMark::Unmarked),
            (9, 9, ChunkMark::Unmarked),
            (0, 9, ChunkMark::ClosedTail),
            (9, 9, ChunkMark::ClosedTail),
        ] {
            let read = ChunkRead::new(
                token(),
                Offset::from_u64(offset),
                Offset::from_u64(next),
                mark,
            )
            .unwrap();
            let mut pairs = Pairs::new();
            read.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(ChunkRead::parse(iter(&pairs)), Ok(read), "{pairs:?}");
            assert_eq!(read.len(), next - offset);
            assert!(read.answers(&ImmutableResource {
                kind: ResourceKind::Chunk,
                incarnation: token(),
                offset: Offset::from_u64(offset),
            }));
            assert!(!read.answers(&ImmutableResource {
                kind: ResourceKind::Chunk,
                incarnation: token(),
                offset: Offset::from_u64(offset + 1),
            }));
            assert!(!read.answers(&ImmutableResource {
                kind: ResourceKind::Snapshot,
                incarnation: token(),
                offset: Offset::from_u64(offset),
            }));
        }
    }

    #[test]
    fn malformed_chunk_reads_are_typed() {
        let full = [
            ("stream-incarnation", "4503599627370497"),
            ("stream-offset", "00000000000000000005"),
            ("stream-next-offset", "00000000000000000009"),
        ];
        for index in 0..full.len() {
            let mut pairs = pairs_of(&full);
            let (name, _) = pairs.remove(index);
            assert_eq!(
                ChunkRead::parse(iter(&pairs)),
                Err(HeaderError::Missing { name })
            );
        }
        let mut pairs = pairs_of(&full);
        pairs.push(("stream-closed", "true".to_owned()));
        assert_eq!(
            ChunkRead::parse(iter(&pairs)),
            Err(HeaderError::ClosedWithoutMarker)
        );
        let mut pairs = pairs_of(&full);
        pairs[2].1 = "00000000000000000004".to_owned();
        assert_eq!(
            ChunkRead::parse(iter(&pairs)),
            Err(HeaderError::ChunkRangeInverted {
                offset: Offset::from_u64(5),
                next: Offset::from_u64(4),
            })
        );
        let mut pairs = pairs_of(&full);
        pairs[2].1 = "00000000000000000005".to_owned();
        pairs.push(("stream-immutable", "true".to_owned()));
        assert_eq!(
            ChunkRead::parse(iter(&pairs)),
            Err(HeaderError::EmptyImmutableChunk {
                offset: Offset::from_u64(5)
            })
        );
        let mut pairs = pairs_of(&full);
        pairs.push(("stream-immutable", "yes".to_owned()));
        assert_eq!(
            ChunkRead::parse(iter(&pairs)),
            Err(HeaderError::InvalidFlag {
                name: "stream-immutable"
            })
        );
    }

    #[test]
    fn immutable_snapshot_reads_are_marked() {
        let read = ImmutableSnapshotRead {
            snapshot: SnapshotRef {
                offset: Offset::from_u64(4096),
                digest: SnapshotDigest::from_bytes([0x5a; 32]),
            },
            incarnation: token(),
        };
        let mut pairs = Pairs::new();
        read.render(|name, value| pairs.push((name, value.to_owned())));
        assert!(pairs.contains(&("stream-immutable", "true".to_owned())));
        assert!(!pairs.iter().any(|(name, _)| *name == "stream-up-to-date"));
        assert_eq!(ImmutableSnapshotRead::parse(iter(&pairs)), Ok(read));
        assert!(read.answers(&ImmutableResource {
            kind: ResourceKind::Snapshot,
            incarnation: token(),
            offset: Offset::from_u64(4096),
        }));
        pairs.retain(|(name, _)| *name != "stream-immutable");
        assert_eq!(
            ImmutableSnapshotRead::parse(iter(&pairs)),
            Err(HeaderError::Missing {
                name: "stream-immutable"
            })
        );
    }

    #[test]
    fn read_origins_are_origins() {
        for (text, scheme, host, port) in [
            (
                "https://reads.example.com",
                OriginScheme::Https,
                "reads.example.com",
                None,
            ),
            (
                "https://reads.example.com:8443",
                OriginScheme::Https,
                "reads.example.com",
                Some(8443),
            ),
            (
                "http://127.0.0.1:4437",
                OriginScheme::Http,
                "127.0.0.1",
                Some(4437),
            ),
            ("https://[::1]", OriginScheme::Https, "[::1]", None),
            (
                "https://[2001:db8::1]:443",
                OriginScheme::Https,
                "[2001:db8::1]",
                Some(443),
            ),
        ] {
            let origin = ReadOrigin::parse(text).unwrap();
            assert_eq!(origin.scheme(), scheme);
            assert_eq!(origin.host(), host);
            assert_eq!(origin.port(), port);
            assert_eq!(origin.is_https(), scheme == OriginScheme::Https);
            assert_eq!(origin.to_string(), text);
            let mut pairs = Pairs::new();
            origin.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(ReadOrigin::parse_header(iter(&pairs)), Ok(Some(origin)));
        }
        for text in [
            "",
            "reads.example.com",
            "ftp://reads.example.com",
            "https://",
            "https://reads.example.com/",
            "https://reads.example.com/path",
            "https://reads.example.com?x",
            "https://user@reads.example.com",
            "https://reads.example.com:0",
            "https://reads.example.com:080",
            "https://reads.example.com:65536",
            "https://reads.example.com:",
            "https://[::1",
            "https://[]",
            "https://.",
            "https://a..b",
            "https://.reads.example.com",
            "https://reads.example.com.",
            "https://-reads.example.com",
            "https://reads-.example.com",
            "HTTPS://reads.example.com",
        ] {
            assert_eq!(
                ReadOrigin::parse(text),
                Err(HeaderError::InvalidReadOrigin),
                "{text}"
            );
        }
        assert_eq!(ReadOrigin::parse_header(iter(&Pairs::new())), Ok(None));
    }
}
