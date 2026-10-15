//! Snapshot headers: the answers of a snapshot read and a snapshot publish,
//! and the headers of a snapshot uploaded in parts (extensions Section 2.2).

use std::fmt;

use crate::error::HeaderError;
use crate::fields;
use crate::fields::Emit;
use crate::names;
use crate::value::Digest;
use crate::value::render_decimal;
use crate::value::render_offset;

/// The most parts one snapshot commit may list.
pub const MAX_SNAPSHOT_PARTS: u32 = 4096;

/// The longest part id, in bytes.
pub const PART_ID_MAX_BYTES: usize = 64;

/// A published snapshot: `Stream-Snapshot-Offset` and
/// `Stream-Snapshot-Digest`, which always travel together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRef {
    pub offset: u64,
    pub digest: Digest,
}

impl SnapshotRef {
    /// Both headers, or `None` when neither is present.
    pub(crate) fn from_fields(
        offset: Option<&[u8]>,
        digest: Option<&[u8]>,
    ) -> Result<Option<Self>, HeaderError> {
        Ok(fields::pair(
            (
                names::STREAM_SNAPSHOT_OFFSET,
                fields::offset(names::STREAM_SNAPSHOT_OFFSET, offset)?,
            ),
            (
                names::STREAM_SNAPSHOT_DIGEST,
                fields::digest(names::STREAM_SNAPSHOT_DIGEST, digest)?,
            ),
        )?
        .map(|(offset, digest)| Self { offset, digest }))
    }

    pub(crate) fn render_into(&self, emit: &mut Emit<'_>) {
        emit(names::STREAM_SNAPSHOT_OFFSET, &render_offset(self.offset));
        emit(names::STREAM_SNAPSHOT_DIGEST, &self.digest.to_string());
    }
}

/// The headers of a `200` snapshot read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRead {
    /// The snapshot served.
    pub snapshot: SnapshotRef,
    /// The stream's tail when the snapshot was served.
    pub next_offset: u64,
    /// The token of the stream that served the snapshot.
    pub incarnation: u64,
    /// `Stream-Up-To-Date`: the snapshot covers the whole stream.
    pub up_to_date: bool,
}

impl SnapshotRead {
    pub fn parse<'a, I>(headers: I) -> Result<Self, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [offset, digest, next, incarnation, up_to_date] = fields::collect(headers, [
            names::STREAM_SNAPSHOT_OFFSET,
            names::STREAM_SNAPSHOT_DIGEST,
            names::STREAM_NEXT_OFFSET,
            names::STREAM_INCARNATION,
            names::STREAM_UP_TO_DATE,
        ])?;
        Ok(Self {
            snapshot: fields::required(
                names::STREAM_SNAPSHOT_OFFSET,
                SnapshotRef::from_fields(offset, digest)?,
            )?,
            next_offset: fields::required(
                names::STREAM_NEXT_OFFSET,
                fields::offset(names::STREAM_NEXT_OFFSET, next)?,
            )?,
            incarnation: fields::required(
                names::STREAM_INCARNATION,
                fields::token(names::STREAM_INCARNATION, incarnation)?,
            )?,
            up_to_date: fields::flag(names::STREAM_UP_TO_DATE, up_to_date)?,
        })
    }

    /// Emits every header of the read, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        self.snapshot.render_into(&mut emit);
        emit(names::STREAM_NEXT_OFFSET, &render_offset(self.next_offset));
        emit(names::STREAM_INCARNATION, &render_decimal(self.incarnation));
        if self.up_to_date {
            emit(names::STREAM_UP_TO_DATE, "true");
        }
    }
}

/// The typed answer to a snapshot publish, single-request or a parts commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPublishAnswer {
    /// `204`: the snapshot is published, by this request or by an identical
    /// earlier one.
    Published {
        snapshot: SnapshotRef,
        incarnation: u64,
    },
    /// `409`: another snapshot at the same offset (a conflict), or a later
    /// one (the publish is superseded). `latest` is the stream's latest
    /// snapshot; a client compares its offset with its own to tell the two
    /// apart.
    Conflict { latest: SnapshotRef },
}

impl SnapshotPublishAnswer {
    pub fn status(&self) -> u16 {
        match self {
            Self::Published { .. } => 204,
            Self::Conflict { .. } => 409,
        }
    }

    /// Emits every header of the answer, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        match self {
            Self::Published {
                snapshot,
                incarnation,
            } => {
                snapshot.render_into(&mut emit);
                emit(names::STREAM_INCARNATION, &render_decimal(*incarnation));
            }
            Self::Conflict { latest } => latest.render_into(&mut emit),
        }
    }

    /// Parses an answer from its status and headers. `Ok(None)` for any
    /// other status, and for a `409` that names no snapshot, which another
    /// rule refused.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [offset, digest, incarnation] = fields::collect(headers, [
            names::STREAM_SNAPSHOT_OFFSET,
            names::STREAM_SNAPSHOT_DIGEST,
            names::STREAM_INCARNATION,
        ])?;
        match status {
            204 => Ok(Some(Self::Published {
                snapshot: fields::required(
                    names::STREAM_SNAPSHOT_OFFSET,
                    SnapshotRef::from_fields(offset, digest)?,
                )?,
                incarnation: fields::required(
                    names::STREAM_INCARNATION,
                    fields::token(names::STREAM_INCARNATION, incarnation)?,
                )?,
            })),
            409 => {
                Ok(SnapshotRef::from_fields(offset, digest)?
                    .map(|latest| Self::Conflict { latest }))
            }
            _ => Ok(None),
        }
    }
}

/// The opaque id of a staged snapshot part: 1 to [`PART_ID_MAX_BYTES`]
/// visible ASCII characters, so that a commit body lists one per line.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartId(String);

impl PartId {
    pub fn new(id: impl Into<String>) -> Result<Self, HeaderError> {
        let id = id.into();
        if id.is_empty()
            || id.len() > PART_ID_MAX_BYTES
            || !id.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(HeaderError::InvalidPartId);
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The headers of the `201` that stages a part:
/// `POST {stream}/snapshot/{offset}/parts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartStaged {
    pub part: PartId,
}

impl PartStaged {
    pub fn parse<'a, I>(headers: I) -> Result<Self, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [part] = fields::collect(headers, [names::STREAM_SNAPSHOT_PART])?;
        let part = fields::required(
            names::STREAM_SNAPSHOT_PART,
            fields::text(names::STREAM_SNAPSHOT_PART, part)?,
        )?;
        Ok(Self {
            part: PartId::new(part)?,
        })
    }

    /// Emits `Stream-Snapshot-Part`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_SNAPSHOT_PART, self.part.as_str());
    }
}

/// The headers of a parts commit, `PUT {stream}/snapshot/{offset}` whose body
/// lists the part ids: how many parts, and the digest of the whole snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartsCommit {
    /// 1 to [`MAX_SNAPSHOT_PARTS`].
    pub parts: u32,
    pub digest: Digest,
}

impl PartsCommit {
    /// `Ok(None)` when the request has no `Stream-Snapshot-Parts`: a publish
    /// whose body is the snapshot itself.
    pub fn parse<'a, I>(headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [parts, digest] = fields::collect(headers, [
            names::STREAM_SNAPSHOT_PARTS,
            names::STREAM_SNAPSHOT_DIGEST,
        ])?;
        let Some(count) = fields::decimal(names::STREAM_SNAPSHOT_PARTS, parts)? else {
            return Ok(None);
        };
        let parts = u32::try_from(count)
            .ok()
            .filter(|parts| (1..=MAX_SNAPSHOT_PARTS).contains(parts))
            .ok_or(HeaderError::PartCountOutOfRange { count })?;
        let digest = fields::required(
            names::STREAM_SNAPSHOT_DIGEST,
            fields::digest(names::STREAM_SNAPSHOT_DIGEST, digest)?,
        )?;
        Ok(Some(Self { parts, digest }))
    }

    /// Emits `Stream-Snapshot-Parts` and `Stream-Snapshot-Digest`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(
            names::STREAM_SNAPSHOT_PARTS,
            &render_decimal(u64::from(self.parts)),
        );
        emit(names::STREAM_SNAPSHOT_DIGEST, &self.digest.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::MAX_SNAPSHOT_PARTS;
    use super::PartId;
    use super::PartStaged;
    use super::PartsCommit;
    use super::SnapshotPublishAnswer;
    use super::SnapshotRead;
    use super::SnapshotRef;
    use crate::error::HeaderError;
    use crate::value::Digest;

    type Pairs = Vec<(&'static str, String)>;

    fn pairs_of(pairs: &[(&'static str, &str)]) -> Pairs {
        pairs
            .iter()
            .map(|(name, value)| (*name, (*value).to_owned()))
            .collect()
    }

    fn iter(pairs: &Pairs) -> impl Iterator<Item = (&str, &[u8])> {
        pairs.iter().map(|(name, value)| (*name, value.as_bytes()))
    }

    fn snapshot() -> SnapshotRef {
        SnapshotRef {
            offset: 4096,
            digest: Digest::from_bytes([0x5a; 32]),
        }
    }

    fn hex() -> String {
        Digest::from_bytes([0x5a; 32]).to_string()
    }

    #[test]
    fn snapshot_reads_round_trip() {
        for read in [
            SnapshotRead {
                snapshot: snapshot(),
                next_offset: 8192,
                incarnation: 4_503_599_627_370_497,
                up_to_date: false,
            },
            SnapshotRead {
                snapshot: SnapshotRef {
                    offset: 0,
                    digest: Digest::from_bytes([0; 32]),
                },
                next_offset: u64::MAX,
                incarnation: u64::MAX,
                up_to_date: true,
            },
        ] {
            let mut pairs = Pairs::new();
            read.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(SnapshotRead::parse(iter(&pairs)), Ok(read), "{pairs:?}");
        }
    }

    #[test]
    fn malformed_snapshot_reads_are_typed() {
        let hex = hex();
        let full = [
            ("stream-snapshot-offset", "00000000000000004096"),
            ("stream-snapshot-digest", hex.as_str()),
            ("stream-next-offset", "00000000000000008192"),
            ("stream-incarnation", "42"),
        ];
        for (index, expected) in [
            HeaderError::Missing {
                name: "stream-snapshot-offset",
            },
            HeaderError::Missing {
                name: "stream-snapshot-digest",
            },
            HeaderError::Missing {
                name: "stream-next-offset",
            },
            HeaderError::Missing {
                name: "stream-incarnation",
            },
        ]
        .into_iter()
        .enumerate()
        {
            let mut pairs = pairs_of(&full);
            pairs.remove(index);
            assert_eq!(SnapshotRead::parse(iter(&pairs)), Err(expected));
        }
        let mut pairs = pairs_of(&full);
        pairs[0].1 = "4096".to_owned();
        assert_eq!(
            SnapshotRead::parse(iter(&pairs)),
            Err(HeaderError::InvalidOffset {
                name: "stream-snapshot-offset"
            })
        );
        let mut pairs = pairs_of(&full);
        pairs[1].1 = hex.to_uppercase();
        assert_eq!(
            SnapshotRead::parse(iter(&pairs)),
            Err(HeaderError::InvalidDigest {
                name: "stream-snapshot-digest"
            })
        );
        let mut pairs = pairs_of(&full);
        pairs.push(("stream-up-to-date", "1".to_owned()));
        assert_eq!(
            SnapshotRead::parse(iter(&pairs)),
            Err(HeaderError::InvalidFlag {
                name: "stream-up-to-date"
            })
        );
    }

    #[test]
    fn publish_answers_round_trip() {
        for answer in [
            SnapshotPublishAnswer::Published {
                snapshot: snapshot(),
                incarnation: 7,
            },
            SnapshotPublishAnswer::Conflict { latest: snapshot() },
        ] {
            let mut pairs = Pairs::new();
            answer.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(
                SnapshotPublishAnswer::parse(answer.status(), iter(&pairs)),
                Ok(Some(answer)),
                "{pairs:?}"
            );
        }
    }

    #[test]
    fn publish_answers_reject_or_skip_what_they_do_not_type() {
        let hex = hex();
        assert_eq!(
            SnapshotPublishAnswer::parse(409, iter(&Pairs::new())),
            Ok(None)
        );
        assert_eq!(
            SnapshotPublishAnswer::parse(400, iter(&pairs_of(&[("stream-snapshot-digest", "x")]))),
            Ok(None)
        );
        assert_eq!(
            SnapshotPublishAnswer::parse(
                204,
                iter(&pairs_of(&[
                    ("stream-snapshot-offset", "00000000000000004096"),
                    ("stream-snapshot-digest", hex.as_str()),
                ]))
            ),
            Err(HeaderError::Missing {
                name: "stream-incarnation"
            })
        );
        assert_eq!(
            SnapshotPublishAnswer::parse(
                409,
                iter(&pairs_of(&[("stream-snapshot-digest", hex.as_str())]))
            ),
            Err(HeaderError::Missing {
                name: "stream-snapshot-offset"
            })
        );
        assert_eq!(
            SnapshotPublishAnswer::parse(
                409,
                iter(&pairs_of(&[(
                    "stream-snapshot-offset",
                    "00000000000000004096"
                )]))
            ),
            Err(HeaderError::Missing {
                name: "stream-snapshot-digest"
            })
        );
    }

    #[test]
    fn staged_parts_round_trip() {
        for id in ["p", "01J9Z3Q4-part.bin", &"x".repeat(64), "~!"] {
            let staged = PartStaged {
                part: PartId::new(id).unwrap(),
            };
            let mut pairs = Pairs::new();
            staged.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(PartStaged::parse(iter(&pairs)), Ok(staged));
        }
    }

    #[test]
    fn malformed_part_ids_are_typed() {
        for invalid in ["", "a b", "a\tb", &"x".repeat(65)] {
            assert_eq!(
                PartId::new(invalid),
                Err(HeaderError::InvalidPartId),
                "{invalid:?}"
            );
        }
        assert_eq!(
            PartStaged::parse(iter(&Pairs::new())),
            Err(HeaderError::Missing {
                name: "stream-snapshot-part"
            })
        );
        assert_eq!(
            PartStaged::parse(iter(&pairs_of(&[("stream-snapshot-part", "é")]))),
            Err(HeaderError::NotText {
                name: "stream-snapshot-part"
            })
        );
    }

    #[test]
    fn parts_commits_round_trip() {
        for parts in [1, 2, MAX_SNAPSHOT_PARTS] {
            let commit = PartsCommit {
                parts,
                digest: Digest::from_bytes([0xc3; 32]),
            };
            let mut pairs = Pairs::new();
            commit.render(|name, value| pairs.push((name, value.to_owned())));
            assert_eq!(PartsCommit::parse(iter(&pairs)), Ok(Some(commit)));
        }
        let hex = hex();
        assert_eq!(
            PartsCommit::parse(iter(&pairs_of(&[("stream-snapshot-digest", hex.as_str())]))),
            Ok(None),
            "a single-request publish"
        );
    }

    #[test]
    fn malformed_parts_commits_are_typed() {
        let hex = hex();
        for (parts, expected) in [
            ("0", HeaderError::PartCountOutOfRange { count: 0 }),
            ("4097", HeaderError::PartCountOutOfRange { count: 4097 }),
            ("4294967296", HeaderError::PartCountOutOfRange {
                count: 4_294_967_296,
            }),
            ("02", HeaderError::NonCanonicalInteger {
                name: "stream-snapshot-parts",
            }),
            ("-2", HeaderError::InvalidInteger {
                name: "stream-snapshot-parts",
            }),
        ] {
            assert_eq!(
                PartsCommit::parse(iter(&pairs_of(&[
                    ("stream-snapshot-parts", parts),
                    ("stream-snapshot-digest", hex.as_str()),
                ]))),
                Err(expected),
                "{parts}"
            );
        }
        assert_eq!(
            PartsCommit::parse(iter(&pairs_of(&[("stream-snapshot-parts", "2")]))),
            Err(HeaderError::Missing {
                name: "stream-snapshot-digest"
            })
        );
    }
}
