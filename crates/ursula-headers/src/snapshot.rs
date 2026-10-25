//! Snapshot headers: the answers of a snapshot read and a snapshot publish,
//! and a snapshot uploaded in parts: the staged part's id, the commit's
//! headers and the commit body, the [`PartsManifest`] (extensions Section
//! 2.2).

use std::fmt;

use crate::error::HeaderError;
use crate::fields;
use crate::fields::Emit;
use crate::limit::MAX_REQUEST_BYTES;
use crate::limit::MAX_SNAPSHOT_BYTES;
use crate::names;
use crate::value::Incarnation;
use crate::value::Offset;
use crate::value::SnapshotDigest;

/// The most parts one snapshot commit may list.
pub const MAX_SNAPSHOT_PARTS: u32 = 4096;

/// The longest part id, in bytes.
pub const PART_ID_MAX_BYTES: usize = 64;

/// The longest valid commit body: [`MAX_SNAPSHOT_PARTS`] ids of
/// [`PART_ID_MAX_BYTES`], each with its LF. A server reads no more of one.
pub const MAX_MANIFEST_BYTES: usize = (MAX_SNAPSHOT_PARTS as usize) * (PART_ID_MAX_BYTES + 1);

/// A published snapshot: `Stream-Snapshot-Offset` and
/// `Stream-Snapshot-Digest`, which always travel together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRef {
    pub offset: Offset,
    pub digest: SnapshotDigest,
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
                fields::parsed(names::STREAM_SNAPSHOT_DIGEST, digest, SnapshotDigest::parse)?,
            ),
        )?
        .map(|(offset, digest)| Self { offset, digest }))
    }

    pub(crate) fn render_into(&self, emit: &mut Emit<'_>) {
        emit(names::STREAM_SNAPSHOT_OFFSET, &self.offset.to_string());
        emit(names::STREAM_SNAPSHOT_DIGEST, &self.digest.to_string());
    }
}

/// The headers of a `200` snapshot read. Its `Stream-Next-Offset` is the
/// snapshot's offset, where a reader continues with ordinary reads, so it
/// is rendered from `snapshot.offset` and must equal it on parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRead {
    /// The snapshot served.
    pub snapshot: SnapshotRef,
    /// The token of the stream that served the snapshot.
    pub incarnation: Incarnation,
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
        Ok(Self {
            snapshot,
            incarnation: fields::required(
                names::STREAM_INCARNATION,
                fields::incarnation(names::STREAM_INCARNATION, incarnation)?,
            )?,
            up_to_date: fields::flag(names::STREAM_UP_TO_DATE, up_to_date)?,
        })
    }

    /// Emits every header of the read, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        self.snapshot.render_into(&mut emit);
        emit(names::STREAM_NEXT_OFFSET, &self.snapshot.offset.to_string());
        emit(names::STREAM_INCARNATION, &self.incarnation.to_string());
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
        incarnation: Incarnation,
    },
    /// `409`: another snapshot at the same offset (a conflict), or a later
    /// one (the publish is superseded). `latest` is the stream's latest
    /// snapshot; [`SnapshotPublishAnswer::supersedes`] tells the two apart.
    Conflict { latest: SnapshotRef },
}

impl SnapshotPublishAnswer {
    pub fn status(&self) -> u16 {
        match self {
            Self::Published { .. } => 204,
            Self::Conflict { .. } => 409,
        }
    }

    /// Whether this is a `409` whose latest snapshot is later than the
    /// published `offset`: the publish is superseded, not conflicting, and
    /// the client has nothing to repair.
    pub fn supersedes(&self, offset: Offset) -> bool {
        match self {
            Self::Published { .. } => false,
            Self::Conflict { latest } => latest.offset > offset,
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
                emit(names::STREAM_INCARNATION, &incarnation.to_string());
            }
            Self::Conflict { latest } => latest.render_into(&mut emit),
        }
    }

    /// Parses an answer from its status and headers. `Ok(None)` for any
    /// other status, and for a `409` that names no snapshot, which another
    /// rule refused.
    pub fn parse<'a, I>(status: u16, headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        if !matches!(status, 204 | 409) {
            return Ok(None);
        }
        let [offset, digest, incarnation] = fields::collect(headers, [
            names::STREAM_SNAPSHOT_OFFSET,
            names::STREAM_SNAPSHOT_DIGEST,
            names::STREAM_INCARNATION,
        ])?;
        let snapshot = SnapshotRef::from_fields(offset, digest)?;
        if status == 409 {
            return Ok(snapshot.map(|latest| Self::Conflict { latest }));
        }
        Ok(Some(Self::Published {
            snapshot: fields::required(names::STREAM_SNAPSHOT_OFFSET, snapshot)?,
            incarnation: fields::required(
                names::STREAM_INCARNATION,
                fields::incarnation(names::STREAM_INCARNATION, incarnation)?,
            )?,
        }))
    }
}

/// The opaque id of a staged snapshot part: 1 to [`PART_ID_MAX_BYTES`]
/// visible ASCII characters, so that a commit body lists one per line.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartId(String);

impl PartId {
    pub fn new(id: impl Into<String>) -> Result<Self, HeaderError> {
        let id = id.into();
        if !Self::valid(id.as_bytes()) {
            return Err(HeaderError::InvalidPartId);
        }
        Ok(Self(id))
    }

    fn valid(bytes: &[u8]) -> bool {
        !bytes.is_empty()
            && bytes.len() <= PART_ID_MAX_BYTES
            && bytes.iter().all(|byte| (0x21..=0x7e).contains(byte))
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

/// The number of parts a commit lists: 1 to [`MAX_SNAPSHOT_PARTS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartCount(u32);

impl PartCount {
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The most bytes a commit of this many parts makes a node copy: each
    /// part at the [`MAX_REQUEST_BYTES`] request limit, and at most the
    /// [`MAX_SNAPSHOT_BYTES`] snapshot limit in all.
    pub fn copy_bytes(self) -> u64 {
        u64::from(self.0)
            .saturating_mul(MAX_REQUEST_BYTES)
            .min(MAX_SNAPSHOT_BYTES)
    }

    fn checked(value: u64) -> Result<Self, HeaderError> {
        u32::try_from(value)
            .ok()
            .filter(|count| (1..=MAX_SNAPSHOT_PARTS).contains(count))
            .map(Self)
            .ok_or(HeaderError::OutOfRange {
                name: names::STREAM_SNAPSHOT_PARTS,
                value,
            })
    }
}

impl TryFrom<u32> for PartCount {
    type Error = HeaderError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::checked(u64::from(value))
    }
}

impl PartCount {
    /// The count as a length; [`HeaderError::ManifestPartCount`] where a
    /// `usize` cannot hold it.
    fn len(self) -> Result<usize, HeaderError> {
        usize::try_from(self.0)
            .map_err(|_overflow| HeaderError::ManifestPartCount { declared: self })
    }
}

impl TryFrom<usize> for PartCount {
    type Error = HeaderError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::checked(u64::try_from(value).unwrap_or(u64::MAX))
    }
}

impl fmt::Display for PartCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The headers of a parts commit, `PUT {stream}/snapshot/{offset}` whose body
/// is a [`PartsManifest`]: how many parts, and the digest of the whole
/// snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartsCommit {
    pub parts: PartCount,
    pub digest: SnapshotDigest,
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
        let parts = PartCount::checked(count)?;
        let digest = fields::required(
            names::STREAM_SNAPSHOT_DIGEST,
            fields::parsed(names::STREAM_SNAPSHOT_DIGEST, digest, SnapshotDigest::parse)?,
        )?;
        Ok(Some(Self { parts, digest }))
    }

    /// The commit of `manifest`, whose parts assemble a snapshot with
    /// `digest`.
    pub fn for_manifest(manifest: &PartsManifest, digest: SnapshotDigest) -> Self {
        Self {
            parts: manifest.count(),
            digest,
        }
    }

    /// Emits `Stream-Snapshot-Parts` and `Stream-Snapshot-Digest`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_SNAPSHOT_PARTS, &self.parts.to_string());
        emit(names::STREAM_SNAPSHOT_DIGEST, &self.digest.to_string());
    }
}

/// The body of a parts commit: the part ids in snapshot order.
///
/// Grammar: `manifest = count( part-id LF )`, where `count` is the commit's
/// `Stream-Snapshot-Parts` and `part-id` is a [`PartId`]. Every line ends
/// with a single LF, the last one too; there is no CR, no empty line and
/// nothing after the last LF. No id appears twice: a commit costs at most
/// the bytes its client staged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartsManifest(Vec<PartId>);

impl PartsManifest {
    /// 1 to [`MAX_SNAPSHOT_PARTS`] distinct part ids.
    pub fn new(parts: Vec<PartId>) -> Result<Self, HeaderError> {
        PartCount::try_from(parts.len())?;
        Self::check_distinct(&parts)?;
        Ok(Self(parts))
    }

    /// [`HeaderError::DuplicateManifestPart`] naming the first line whose id
    /// an earlier line holds.
    fn check_distinct(parts: &[PartId]) -> Result<(), HeaderError> {
        let mut seen = std::collections::HashSet::with_capacity(parts.len());
        for (index, part) in parts.iter().enumerate() {
            if !seen.insert(part.as_str()) {
                return Err(HeaderError::DuplicateManifestPart {
                    line: index.saturating_add(1),
                });
            }
        }
        Ok(())
    }

    pub fn parts(&self) -> &[PartId] {
        &self.0
    }

    /// The `Stream-Snapshot-Parts` of the commit that carries this manifest.
    pub fn count(&self) -> PartCount {
        // `new` and `parse` keep the length within the range of a count.
        PartCount(u32::try_from(self.0.len()).unwrap_or(MAX_SNAPSHOT_PARTS))
    }

    /// Parses a commit body that must list exactly `count` parts.
    pub fn parse(count: PartCount, body: &[u8]) -> Result<Self, HeaderError> {
        let Some(lines) = body.strip_suffix(b"\n") else {
            return Err(HeaderError::ManifestUnterminated);
        };
        let mut parts = Vec::new();
        for (index, line) in lines.split(|byte| *byte == b'\n').enumerate() {
            if parts.len() >= count.len()? {
                return Err(HeaderError::ManifestPartCount { declared: count });
            }
            let id = std::str::from_utf8(line)
                .ok()
                .filter(|id| PartId::valid(id.as_bytes()))
                .ok_or(HeaderError::InvalidManifestLine {
                    line: index.saturating_add(1),
                })?;
            parts.push(PartId(id.to_owned()));
        }
        if parts.len() != count.len()? {
            return Err(HeaderError::ManifestPartCount { declared: count });
        }
        Self::check_distinct(&parts)?;
        Ok(Self(parts))
    }

    /// The commit body: each id followed by LF.
    pub fn render(&self) -> String {
        let mut body = String::new();
        for part in &self.0 {
            body.push_str(part.as_str());
            body.push('\n');
        }
        body
    }
}

#[cfg(test)]
mod tests {
    use super::MAX_SNAPSHOT_PARTS;
    use super::PartCount;
    use super::PartId;
    use super::PartStaged;
    use super::PartsCommit;
    use super::PartsManifest;
    use super::SnapshotPublishAnswer;
    use super::SnapshotRead;
    use super::SnapshotRef;
    use crate::error::HeaderError;
    use crate::value::Incarnation;
    use crate::value::Offset;
    use crate::value::SnapshotDigest;

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
            offset: Offset::from_u64(4096),
            digest: SnapshotDigest::from_bytes([0x5a; 32]),
        }
    }

    fn hex() -> String {
        SnapshotDigest::from_bytes([0x5a; 32]).to_string()
    }

    fn incarnation(value: u64) -> Incarnation {
        Incarnation::try_from(value).unwrap()
    }

    fn count(value: u32) -> PartCount {
        PartCount::try_from(value).unwrap()
    }

    #[test]
    fn snapshot_reads_round_trip() {
        for read in [
            SnapshotRead {
                snapshot: snapshot(),
                incarnation: incarnation(4_503_599_627_370_497),
                up_to_date: false,
            },
            SnapshotRead {
                snapshot: SnapshotRef {
                    offset: Offset::from_u64(u64::MAX),
                    digest: SnapshotDigest::from_bytes([0; 32]),
                },
                incarnation: incarnation(1),
                up_to_date: true,
            },
        ] {
            let mut pairs = Pairs::new();
            read.render(|name, value| pairs.push((name, value.to_owned())));
            assert!(pairs.contains(&("stream-next-offset", read.snapshot.offset.to_string())));
            assert_eq!(SnapshotRead::parse(iter(&pairs)), Ok(read), "{pairs:?}");
        }
    }

    #[test]
    fn malformed_snapshot_reads_are_typed() {
        let hex = hex();
        let full = [
            ("stream-snapshot-offset", "00000000000000004096"),
            ("stream-snapshot-digest", hex.as_str()),
            ("stream-next-offset", "00000000000000004096"),
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
        pairs[2].1 = "00000000000000008192".to_owned();
        assert_eq!(
            SnapshotRead::parse(iter(&pairs)),
            Err(HeaderError::SnapshotNextOffsetMismatch {
                snapshot: Offset::from_u64(4096),
                next: Offset::from_u64(8192)
            })
        );
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
                incarnation: incarnation(7),
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
    fn a_conflict_tells_superseded_from_conflicting() {
        let conflict = SnapshotPublishAnswer::Conflict { latest: snapshot() };
        assert!(conflict.supersedes(Offset::from_u64(4095)));
        assert!(!conflict.supersedes(Offset::from_u64(4096)));
        let published = SnapshotPublishAnswer::Published {
            snapshot: snapshot(),
            incarnation: incarnation(7),
        };
        assert!(!published.supersedes(Offset::ZERO));
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
        for invalid in ["", "a b", "a\tb", "a\r", &"x".repeat(65)] {
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
    fn part_counts_stay_in_range() {
        assert_eq!(count(1).get(), 1);
        assert_eq!(count(MAX_SNAPSHOT_PARTS).get(), MAX_SNAPSHOT_PARTS);
        for value in [0, MAX_SNAPSHOT_PARTS + 1, u32::MAX] {
            assert_eq!(
                PartCount::try_from(value),
                Err(HeaderError::OutOfRange {
                    name: "stream-snapshot-parts",
                    value: u64::from(value)
                })
            );
        }
    }

    #[test]
    fn parts_commits_round_trip() {
        for parts in [1, 2, MAX_SNAPSHOT_PARTS] {
            let commit = PartsCommit {
                parts: count(parts),
                digest: SnapshotDigest::from_bytes([0xc3; 32]),
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
        let out_of_range = |value| HeaderError::OutOfRange {
            name: "stream-snapshot-parts",
            value,
        };
        for (parts, expected) in [
            ("0", out_of_range(0)),
            ("4097", out_of_range(4097)),
            ("4294967296", out_of_range(4_294_967_296)),
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

    #[test]
    fn manifests_round_trip() {
        let ids = |ids: &[&str]| -> Vec<PartId> {
            ids.iter().map(|id| PartId::new(*id).unwrap()).collect()
        };
        for parts in [
            ids(&["p"]),
            ids(&["a", "b", "c"]),
            ids(&[&"x".repeat(64), "~!"]),
            (0..MAX_SNAPSHOT_PARTS)
                .map(|index| PartId::new(format!("part-{index}")).unwrap())
                .collect(),
        ] {
            let manifest = PartsManifest::new(parts.clone()).unwrap();
            let commit = PartsCommit::for_manifest(&manifest, SnapshotDigest::from_bytes([7; 32]));
            assert_eq!(commit.parts, manifest.count());
            assert_eq!(manifest.parts(), parts.as_slice());
            let body = manifest.render();
            assert!(body.ends_with('\n') && !body.contains('\r'));
            assert_eq!(
                PartsManifest::parse(manifest.count(), body.as_bytes()),
                Ok(manifest)
            );
        }
        assert_eq!(
            PartsManifest::new(ids(&["a", "b"])).unwrap().render(),
            "a\nb\n"
        );
    }

    #[test]
    fn malformed_manifests_are_typed() {
        assert_eq!(
            PartsManifest::new(Vec::new()),
            Err(HeaderError::OutOfRange {
                name: "stream-snapshot-parts",
                value: 0
            })
        );
        let too_many = vec![PartId::new("p").unwrap(); 4097];
        assert_eq!(
            PartsManifest::new(too_many),
            Err(HeaderError::OutOfRange {
                name: "stream-snapshot-parts",
                value: 4097
            })
        );
        assert_eq!(
            PartsManifest::new(
                ["a", "b", "a"]
                    .into_iter()
                    .map(|id| PartId::new(id).unwrap())
                    .collect()
            ),
            Err(HeaderError::DuplicateManifestPart { line: 3 })
        );
        for (declared, body, expected) in [
            (2, &b"a\nb"[..], HeaderError::ManifestUnterminated),
            (2, b"a\na\n", HeaderError::DuplicateManifestPart { line: 2 }),
            (1, b"", HeaderError::ManifestUnterminated),
            (1, b"\n", HeaderError::InvalidManifestLine { line: 1 }),
            (2, b"a\n\n", HeaderError::InvalidManifestLine { line: 2 }),
            (2, b"a\r\nb\r\n", HeaderError::InvalidManifestLine {
                line: 1,
            }),
            (2, b"a\nb c\n", HeaderError::InvalidManifestLine { line: 2 }),
            (1, b"\xff\n", HeaderError::InvalidManifestLine { line: 1 }),
            (2, b"a\n", HeaderError::ManifestPartCount {
                declared: count(2),
            }),
            (1, b"a\nb\n", HeaderError::ManifestPartCount {
                declared: count(1),
            }),
            (1, b"a\n\n", HeaderError::ManifestPartCount {
                declared: count(1),
            }),
        ] {
            assert_eq!(
                PartsManifest::parse(count(declared), body),
                Err(expected),
                "{body:?}"
            );
        }
    }
}
