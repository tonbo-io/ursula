//! The segments of a core journal.
//!
//! A core's journal is the run of segment files in its directory,
//! `journal-<sequence>.seg`, with consecutive sequences. Appends go to the
//! newest segment. Once it reaches the target size the writer rotates
//! ([`rotate`]): it `fsync`s the segment, then creates the next one and makes
//! its header and directory entry durable before any frame goes to it. Every
//! segment older than the newest is therefore complete on disk, under either
//! fsync policy, and a crash can only cut the newest segment short.
//!
//! Purge deletes whole segments from the oldest one on, once no group keeps a
//! live record in them ([`delete_segments`]). Deletion never has to be
//! durable for the journal to stay correct: a deleted segment that a power
//! loss brings back replays history that later records supersede.
//!
//! Recovery reads the segments in order ([`recover_segments`]). Strict
//! replay tolerates only an incomplete final frame of the newest segment.
//! Verified-prefix replay keeps every frame before the first one that fails
//! verification, in whatever segment, and drops the rest of that segment and
//! every later one. The dropped segments are removed durably before that
//! segment is truncated, so a second crash cannot bring them back after a
//! shorter segment and replay them over a hole.

use std::fmt;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use super::disk::Disk;
use super::disk::JournalDisk;
use super::disk::JournalFile;
use super::journal;
use super::journal::FrameCodec;
use super::journal::FrameLoc;
use super::journal::HeaderDefect;
use super::journal::JournalError;
use super::journal::JournalOp;
use super::journal::JournalReplayMode;
use super::journal::JournalWriter;
use super::journal::ReplayTail;

const SEGMENT_PREFIX: &str = "journal-";
const SEGMENT_SUFFIX: &str = ".seg";
/// Bytes copied at a time when a segment is rewritten whole.
const COPY_CHUNK_BYTES: usize = 1024 * 1024;

/// The sequence of one segment of a core journal. It names the file and
/// seeds the checksum of every frame in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SegmentId(pub(crate) u64);

impl SegmentId {
    pub(crate) const FIRST: Self = Self(journal::FIRST_SEQUENCE);

    pub(crate) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The file of segment `id` in the core journal directory `dir`.
pub(crate) fn segment_path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{:020}{SEGMENT_SUFFIX}", id.0))
}

fn parse_segment_name(path: &Path) -> Option<SegmentId> {
    let name = path.file_name()?.to_str()?;
    let digits = name
        .strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(SEGMENT_SUFFIX)?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().map(SegmentId)
}

/// The segments of the core journal in `dir`, oldest first. A missing
/// directory holds none.
pub(crate) fn list_segments(dir: &Path) -> Result<Vec<SegmentId>, JournalError> {
    if !Disk::exists(dir) {
        return Ok(Vec::new());
    }
    let mut ids = Disk::read_dir(dir)
        .map_err(|source| JournalError::io(dir, JournalOp::Open, source))?
        .iter()
        .filter_map(|path| parse_segment_name(path))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    Ok(ids)
}

/// The segments of the core journal in `core_dir`, oldest first: each
/// one's sequence and file.
pub fn journal_segments(core_dir: &Path) -> Result<Vec<(u64, PathBuf)>, JournalError> {
    Ok(list_segments(core_dir)?
        .into_iter()
        .map(|id| (id.0, segment_path(core_dir, id)))
        .collect())
}

/// The file segment `sequence` of the core journal in `core_dir` has, or
/// will have.
pub fn journal_segment_path(core_dir: &Path, sequence: u64) -> PathBuf {
    segment_path(core_dir, SegmentId(sequence))
}

/// Whether any segment of the core journal in `dir` holds a record.
pub(crate) fn holds_records(dir: &Path) -> Result<bool, JournalError> {
    for id in list_segments(dir)? {
        if journal::holds_records(&segment_path(dir, id))? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn file_len(path: &Path) -> Result<u64, JournalError> {
    Disk::open_read(path)
        .and_then(|file| file.file_len())
        .map_err(|source| JournalError::io(path, JournalOp::Stat, source))
}

/// A segment kept by recovery and the length of what it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KeptSegment {
    pub(crate) id: SegmentId,
    pub(crate) len: u64,
}

/// Where the verified records of a core journal end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryEnd {
    /// Every segment verified to its end.
    Clean,
    /// Segment `segment` ends after `tail`; it was truncated after its
    /// verified frames and the `dropped_segments` after it were removed.
    Truncated {
        segment: SegmentId,
        tail: ReplayTail,
        dropped_segments: u64,
    },
    /// The newest segment held no complete header (a crash cut its creation
    /// short) and was removed.
    TornNewest { segment: SegmentId },
    /// Verified-prefix replay stopped before `segment`: its header or its
    /// sequence did not verify. It and every later segment were removed.
    Dropped {
        segment: SegmentId,
        dropped_segments: u64,
    },
}

/// What [`recover_segments`] kept of a core journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveredSegments {
    /// The segments kept, oldest first. Appends continue on the last one.
    pub(crate) segments: Vec<KeptSegment>,
    /// Frames verified and visited.
    pub(crate) frames: u64,
    /// The bytes of the kept segments.
    pub(crate) verified_bytes: u64,
    /// Bytes truncated or removed after the verified frames.
    pub(crate) dropped_bytes: u64,
    pub(crate) end: RecoveryEnd,
}

/// How the scan of the segments ended, before anything is changed on disk.
enum Cut {
    None,
    /// Truncate `segment` at `len`, after its verified frames.
    Truncate {
        segment: SegmentId,
        len: u64,
        tail: ReplayTail,
    },
    /// Remove the newest segment: its header is incomplete.
    TornNewest(SegmentId),
    /// Remove this segment and every later one.
    DropFrom(SegmentId),
}

/// Reads every segment of the core journal in `dir` in order and in `mode`,
/// streaming each record with its segment and position through `visit`, and
/// then cuts the journal after its verified records (see the module
/// documentation).
pub(crate) fn recover_segments<C: FrameCodec>(
    dir: &Path,
    mode: JournalReplayMode,
    mut visit: impl FnMut(SegmentId, FrameLoc, C::Record) -> io::Result<()>,
) -> Result<RecoveredSegments, JournalError> {
    let ids = list_segments(dir)?;
    let mut kept: Vec<KeptSegment> = Vec::with_capacity(ids.len());
    let mut frames = 0_u64;
    let mut verified_bytes = 0_u64;
    let mut cut = Cut::None;
    for (position, id) in ids.iter().copied().enumerate() {
        let newest = position.saturating_add(1) == ids.len();
        if let Some(previous) = kept.last()
            && previous.id.next() != id
        {
            match mode {
                JournalReplayMode::Strict => {
                    return Err(JournalError::MissingSegment {
                        dir: dir.to_owned(),
                        previous: previous.id.0,
                        found: id.0,
                    });
                }
                JournalReplayMode::VerifiedPrefix => {
                    cut = Cut::DropFrom(id);
                    break;
                }
            }
        }
        let path = segment_path(dir, id);
        let len = file_len(&path)?;
        if len < journal::JOURNAL_HEADER_LEN_U64 {
            match (newest, mode) {
                (true, _) => {
                    cut = Cut::TornNewest(id);
                    break;
                }
                (false, JournalReplayMode::VerifiedPrefix) => {
                    cut = Cut::DropFrom(id);
                    break;
                }
                (false, JournalReplayMode::Strict) => {
                    return Err(JournalError::CorruptHeader {
                        path,
                        defect: HeaderDefect::Torn,
                    });
                }
            }
        }
        let replayed = journal::replay::<C>(&path, mode, |loc, record| visit(id, loc, record))?;
        if let Some(found) = replayed.sequence
            && found != id.0
        {
            return Err(JournalError::CorruptHeader {
                path,
                defect: HeaderDefect::Sequence {
                    expected: id.0,
                    found,
                },
            });
        }
        frames = frames.saturating_add(replayed.frames);
        verified_bytes = verified_bytes.saturating_add(replayed.verified_len);
        match replayed.tail {
            ReplayTail::Clean => kept.push(KeptSegment { id, len }),
            ReplayTail::Incomplete { bytes } if !newest && mode == JournalReplayMode::Strict => {
                return Err(JournalError::IncompleteSealedSegment { path, bytes });
            }
            tail @ (ReplayTail::Incomplete { .. } | ReplayTail::Unverified { .. }) => {
                kept.push(KeptSegment {
                    id,
                    len: replayed.verified_len,
                });
                cut = Cut::Truncate {
                    segment: id,
                    len: replayed.verified_len,
                    tail,
                };
                break;
            }
        }
    }
    let first_dropped = match &cut {
        Cut::None => None,
        Cut::Truncate { segment, .. } => Some(segment.next()),
        Cut::TornNewest(segment) | Cut::DropFrom(segment) => Some(*segment),
    };
    let dropped = ids
        .iter()
        .copied()
        .filter(|id| first_dropped.is_some_and(|first| *id >= first))
        .collect::<Vec<_>>();
    let mut dropped_bytes = 0_u64;
    for id in &dropped {
        dropped_bytes = dropped_bytes.saturating_add(file_len(&segment_path(dir, *id))?);
    }
    // Later segments go first and durably: a crash after the truncation
    // must not find them again after a shorter segment.
    for id in &dropped {
        let path = segment_path(dir, *id);
        Disk::remove_file(&path)
            .map_err(|source| JournalError::io(&path, JournalOp::Remove, source))?;
    }
    if !dropped.is_empty() {
        Disk::sync_dir(dir).map_err(|source| JournalError::io(dir, JournalOp::SyncDir, source))?;
    }
    let dropped_segments = u64::try_from(dropped.len()).unwrap_or(u64::MAX);
    let end = match cut {
        Cut::None => RecoveryEnd::Clean,
        Cut::Truncate { segment, len, tail } => {
            let path = segment_path(dir, segment);
            let before = file_len(&path)?;
            dropped_bytes = dropped_bytes.saturating_add(before.saturating_sub(len));
            Disk::truncate(&path, len)
                .map_err(|source| JournalError::io(&path, JournalOp::Truncate, source))?;
            RecoveryEnd::Truncated {
                segment,
                tail,
                dropped_segments,
            }
        }
        Cut::TornNewest(segment) => RecoveryEnd::TornNewest { segment },
        Cut::DropFrom(segment) => RecoveryEnd::Dropped {
            segment,
            dropped_segments,
        },
    };
    Ok(RecoveredSegments {
        segments: kept,
        frames,
        verified_bytes,
        dropped_bytes,
        end,
    })
}

/// Rewrites every kept segment as a new file with the same contents and
/// replaces it, then `fsync`s the directory. A verified prefix may hold
/// frames whose `fsync` failed and that only the page cache still has; a
/// failed `fsync` may mark them clean, so only writing them again makes them
/// durable. Returns the number of `fsync`s.
pub(crate) fn persist_segments(dir: &Path, segments: &[KeptSegment]) -> Result<u64, JournalError> {
    let mut fsyncs = 0_u64;
    let mut buf = Vec::new();
    for segment in segments {
        let path = segment_path(dir, segment.id);
        let mut temp = path.as_os_str().to_owned();
        temp.push(".tmp");
        let temp = PathBuf::from(temp);
        if Disk::exists(&temp) {
            Disk::remove_file(&temp)
                .map_err(|source| JournalError::io(&temp, JournalOp::Remove, source))?;
        }
        let mut source = Disk::open_read(&path)
            .map_err(|source| JournalError::io(&path, JournalOp::Open, source))?;
        let mut target = Disk::open_append(&temp)
            .map_err(|source| JournalError::io(&temp, JournalOp::Open, source))?;
        let mut remaining = segment.len;
        while remaining > 0 {
            let chunk = usize::try_from(remaining)
                .unwrap_or(COPY_CHUNK_BYTES)
                .min(COPY_CHUNK_BYTES);
            buf.resize(chunk, 0);
            source
                .read_exact(&mut buf)
                .map_err(|source| JournalError::io(&path, JournalOp::Read, source))?;
            target
                .append(&buf)
                .map_err(|source| JournalError::io(&temp, JournalOp::Append, source))?;
            remaining = remaining.saturating_sub(u64::try_from(chunk).unwrap_or(u64::MAX));
        }
        target
            .sync_data()
            .map_err(|source| JournalError::io(&temp, JournalOp::Sync, source))?;
        fsyncs = fsyncs.saturating_add(1);
        Disk::rename(&temp, &path)
            .map_err(|source| JournalError::io(&path, JournalOp::Rename, source))?;
    }
    if !segments.is_empty() {
        Disk::sync_dir(dir).map_err(|source| JournalError::io(dir, JournalOp::SyncDir, source))?;
        fsyncs = fsyncs.saturating_add(1);
    }
    Ok(fsyncs)
}

/// Opens segment `id` of the journal in `dir` for appending; a missing
/// segment is created, and its header and directory entry made durable.
/// Returns the writer and the number of `fsync`s.
pub(crate) fn open_segment(
    dir: &Path,
    id: SegmentId,
) -> Result<(JournalWriter, u64), JournalError> {
    let mut writer = JournalWriter::open(&segment_path(dir, id), id.0)?;
    let fsyncs = if writer.pending_bytes() != 0 {
        writer.sync()?
    } else {
        0
    };
    Ok((writer, fsyncs))
}

/// Seals `active` and starts the next segment: `fsync`s what `active` holds,
/// then creates the next segment and makes its header and directory entry
/// durable before anything is appended to it. Returns the new segment's
/// writer and the number of `fsync`s.
pub(crate) fn rotate(
    dir: &Path,
    active: &mut JournalWriter,
) -> Result<(JournalWriter, u64), JournalError> {
    let mut fsyncs = active.sync_data()?;
    let next = SegmentId(active.sequence()).next();
    let path = segment_path(dir, next);
    if Disk::exists(&path) {
        // Recovery removes every segment after the newest it keeps, so the
        // next one never exists.
        return Err(JournalError::io(
            &path,
            JournalOp::Open,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the next journal segment already exists",
            ),
        ));
    }
    let (writer, created) = open_segment(dir, next)?;
    fsyncs = fsyncs.saturating_add(created);
    Ok((writer, fsyncs))
}

/// What [`delete_segments`] removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Deleted {
    pub(crate) segments: u64,
    pub(crate) bytes: u64,
}

/// Removes `segments`, oldest first, and then `fsync`s the directory. A
/// removal that fails stops it, and the segments not removed stay in the
/// journal, which remains correct. Returns what was removed, and the error
/// that stopped it with whether it was the directory `fsync`.
pub(crate) fn delete_segments(
    dir: &Path,
    segments: &[(SegmentId, u64)],
) -> (Deleted, Option<DeleteError>) {
    let mut deleted = Deleted::default();
    for (id, len) in segments {
        let path = segment_path(dir, *id);
        if let Err(source) = Disk::remove_file(&path) {
            let error = JournalError::io(&path, JournalOp::Remove, source);
            if deleted.segments != 0
                && let Err(source) = Disk::sync_dir(dir)
            {
                return (
                    deleted,
                    Some(DeleteError::SyncDir(JournalError::io(
                        dir,
                        JournalOp::SyncDir,
                        source,
                    ))),
                );
            }
            return (deleted, Some(DeleteError::Remove(error)));
        }
        deleted.segments = deleted.segments.saturating_add(1);
        deleted.bytes = deleted.bytes.saturating_add(*len);
    }
    if deleted.segments != 0
        && let Err(source) = Disk::sync_dir(dir)
    {
        return (
            deleted,
            Some(DeleteError::SyncDir(JournalError::io(
                dir,
                JournalOp::SyncDir,
                source,
            ))),
        );
    }
    (deleted, None)
}

/// Why [`delete_segments`] stopped.
#[derive(Debug)]
pub(crate) enum DeleteError {
    /// A segment could not be removed; it and every later one stay.
    Remove(JournalError),
    /// The directory `fsync` after the removals failed.
    SyncDir(JournalError),
}
