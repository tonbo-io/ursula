//! The commit log (design §5.2): a generation's `log/` directory of `USL1` segments, named by
//! their first position (`<20 digits>.usl`) and rolled at 16 MiB.
//!
//! The valid log is the longest prefix whose chains verify and whose positions grow by one across
//! segments. [`Log::open`] cuts the log after that prefix and reports the cut ([`Cut`]) with a
//! warning, whatever follows the first broken record: after a power loss the filesystem may keep
//! later unsynced records and lose an earlier one, and a database user can store bytes that look
//! like a record inside a commit. That keeps L3, since pages reach the db only after F1 fsyncs the
//! log, so every record whose pages could be there is in the durable prefix. A gap or a malformed
//! record under valid chains fails (`internal`: the extension wrote them), and so does a segment
//! header of another format or version. Segments are removed from the front (trim, oldest first)
//! and cut from the back (open deletes the later segments before it truncates), each step made
//! durable before the next, so a crash between steps leaves a log open reads the same way. [`Log::append`]
//! writes a record with one positioned write and no fsync, and cuts the segment back when the
//! write fails. [`Log::sync`] fdatasyncs the current segment when it holds unsynced records. A roll
//! fdatasyncs the segment it leaves and fsyncs the new one and the directory before the first
//! record goes there, so no record of a later segment outlives an earlier one's. [`Log::trim`]
//! deletes the segments whose records are all at or below `min(base, acked)` (F3), after an fsync
//! that makes those records durable, and does nothing (no fsync) when no segment would go.

use std::fs::File;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use ursula_headers::Epoch;
use ursula_headers::Offset;

use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::decode;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentError;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::Tail;
use crate::codec::segment::encode_header;
use crate::codec::segment::encode_record;
use crate::codec::segment::scan;
use crate::ids::Activation;
use crate::ids::Lineage;
use crate::ids::Position;

/// The size past which the next record starts a new segment.
pub(crate) const SEGMENT_BYTES: u64 = 16 << 20;

/// A segment file's extension.
const EXTENSION: &str = "usl";

#[derive(Debug, thiserror::Error)]
pub(crate) enum LogError {
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("log segment {}: {source}", .path.display())]
    Segment {
        path: PathBuf,
        #[source]
        source: SegmentError,
    },
    #[error(
        "log segment {}: the log starts at {first:?}, but records from {needed:?} on are needed",
        .path.display()
    )]
    MissingPrefix {
        path: PathBuf,
        first: Position,
        needed: Position,
    },
    #[error("log {}: no segment", .dir.display())]
    Empty { dir: PathBuf },
    #[error("log {}: a segment of lineage {found:?} in a log of lineage {expected:?}", .path.display())]
    Lineage {
        path: PathBuf,
        expected: Lineage,
        found: Lineage,
    },
    #[error(
        "log segment {} at byte {at}: position {found:?} where {expected:?} follows",
        .path.display()
    )]
    Gap {
        path: PathBuf,
        at: u64,
        expected: Position,
        found: Position,
    },
    #[error("log segment {} at byte {at}: a record of kind {kind} that no record has", .path.display())]
    Malformed { path: PathBuf, at: u64, kind: u8 },
    #[error("log {}: a record after position {after:?}, the last position there is", .dir.display())]
    PositionOverflow { dir: PathBuf, after: Position },
    #[error("log segment {}: encoding: {source}", .path.display())]
    Encode {
        path: PathBuf,
        #[source]
        source: SegmentError,
    },
    #[error(
        "log segment {}: a failed append ({write}) could not be cut back: {cut}",
        .path.display()
    )]
    Cut {
        path: PathBuf,
        write: std::io::Error,
        #[source]
        cut: std::io::Error,
    },
    #[error("log {}: refused after a failed cut", .dir.display())]
    Broken { dir: PathBuf },
}

/// A record of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRecord {
    pub(crate) position: Position,
    pub(crate) body: RecordBody,
}

/// What [`Log::open`] found: the state at the end of the valid log and the records it still
/// needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recovered {
    /// The latest segment's header: the generation, the stream and its token.
    pub(crate) header: SegmentHeader,
    /// The latest BASE.
    pub(crate) base: Position,
    /// The latest ACK.
    pub(crate) acked: Position,
    pub(crate) acked_offset: Offset,
    pub(crate) log_bytes: u64,
    /// The latest CLAIM, if any.
    pub(crate) claim: Option<(Epoch, Activation)>,
    /// Every CLAIM record the log holds, in log order.
    pub(crate) claims: Vec<(Epoch, Activation)>,
    /// Every record after `min(base, acked)`, in log order.
    pub(crate) records: Vec<LogRecord>,
    /// Where open cut the log, if it did.
    pub(crate) cut: Option<Cut>,
}

impl Recovered {
    /// This generation's COMMIT records after the latest ACK: logged, not acknowledged. Stream
    /// frames a catch-up logged carry another lineage and are never pending.
    pub(crate) fn pending(&self) -> Vec<(Position, Arc<[u8]>)> {
        self.records
            .iter()
            .filter(|r| r.position > self.acked)
            .filter_map(|r| match &r.body {
                RecordBody::Commit { frame } if !foreign(frame, self.header.lineage) => {
                    Some((r.position, Arc::from(frame.as_slice())))
                }
                _ => None,
            })
            .collect()
    }

    /// The records after the latest BASE, which materialization re-applies.
    pub(crate) fn after_base(&self) -> impl Iterator<Item = &LogRecord> {
        self.records.iter().filter(|r| r.position > self.base)
    }
}

/// Where [`Log::open`] cut the log: the segment and byte offset where the valid prefix ends. Every
/// byte after it, and every later segment, is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cut {
    pub(crate) segment: PathBuf,
    pub(crate) at: u64,
}

/// A segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Segment {
    first: Position,
    path: PathBuf,
}

/// An open log: its segments, the current one open for appends.
#[derive(Debug)]
pub(crate) struct Log {
    dir: PathBuf,
    /// By first position; the last one is current.
    segments: Vec<Segment>,
    file: File,
    /// The current segment's valid length.
    len: u64,
    /// The current segment's header length: a segment longer than it holds records.
    header_len: u64,
    /// The last record's chain (the header's checksum before the first record).
    chain: u32,
    next: Position,
    /// The current segment's header, with the carried-over fields brought to the end of the log.
    state: SegmentHeader,
    /// Records appended since the last fdatasync, or a log not synced since open.
    dirty: bool,
    /// A failed append could not be cut back: the segment may end in a partial record.
    broken: bool,
    roll_bytes: u64,
    /// Test hook: the next append writes this many bytes of its record, then fails.
    #[cfg(test)]
    pub(crate) fail_next_append: Option<usize>,
    /// Test hook: the next unlink in a trim fails.
    #[cfg(test)]
    pub(crate) fail_next_unlink: bool,
    /// Test hook: the next directory fsync in a trim fails, after its unlink succeeded.
    #[cfg(test)]
    pub(crate) fail_next_trim_fsync: bool,
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> LogError {
    let path = path.to_owned();
    move |source| LogError::Io { op, path, source }
}

/// fsyncs a directory, so the entries created or removed in it are durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<(), std::io::Error> {
    File::open(dir)?.sync_all()
}

fn segment_path(dir: &Path, first: Position) -> PathBuf {
    dir.join(format!("{:020}.{EXTENSION}", first.0))
}

/// The segments in `dir`, by first position. Files of other names are not the log's.
fn list(dir: &Path) -> Result<Vec<Segment>, LogError> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir).map_err(io("list", dir))? {
        let path = entry.map_err(io("list", dir))?.path();
        let first = path
            .extension()
            .filter(|e| *e == EXTENSION)
            .and(path.file_stem())
            .and_then(|s| s.to_str())
            .filter(|s| s.len() == 20)
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(first) = first {
            segments.push(Segment {
                first: Position(first),
                path,
            });
        }
    }
    segments.sort_by_key(|s| s.first);
    Ok(segments)
}

/// Whether a COMMIT record's frame names a commit of another lineage than `ours`.
fn foreign(frame: &[u8], ours: Lineage) -> bool {
    let max_body = u64::try_from(frame.len()).unwrap_or(u64::MAX);
    matches!(
        decode(frame, max_body),
        Ok(Decoded::Frame { frame: Frame::Commit { id, .. }, .. }) if id.lineage != ours
    )
}

/// A segment header error a crash during a roll leaves: cut short, not verifying, or a magic that
/// starts with a zero byte (a block allocated but never written before the roll's fsync). A
/// non-zero foreign magic or another version is a header of another format, refused.
fn torn_header(e: &SegmentError) -> bool {
    match e {
        SegmentError::HeaderTruncated { .. } | SegmentError::HeaderChecksum { .. } => true,
        SegmentError::Magic { found } => found.first().is_none_or(|&b| b == 0),
        _ => false,
    }
}

/// Brings the carried-over fields of `state` past `body`.
fn carry(state: &mut SegmentHeader, body: &RecordBody) {
    match body {
        RecordBody::Commit { .. } => {}
        RecordBody::Claim { epoch, activation } => {
            state.claim = Some((*epoch, *activation));
        }
        RecordBody::Ack {
            acked,
            acked_offset,
            log_bytes,
        } => {
            state.acked = *acked;
            state.acked_offset = *acked_offset;
            state.log_bytes = *log_bytes;
        }
        RecordBody::Base { position } => state.base = *position,
    }
}

impl Log {
    /// Creates the log in `dir` (created when missing) with its first segment, `h` its header,
    /// and fsyncs the segment and the directory.
    pub(crate) fn create(dir: &Path, h: SegmentHeader) -> Result<Log, LogError> {
        fs::create_dir_all(dir).map_err(io("create", dir))?;
        let segment = Segment {
            first: h.first_position,
            path: segment_path(dir, h.first_position),
        };
        let (file, header_len, seed) = write_segment(&segment.path, &h)?;
        sync_dir(dir).map_err(io("fsync", dir))?;
        Ok(Log {
            dir: dir.to_owned(),
            segments: vec![segment],
            file,
            len: header_len,
            header_len,
            chain: seed,
            next: h.first_position,
            state: h,
            dirty: false,
            broken: false,
            roll_bytes: SEGMENT_BYTES,
            #[cfg(test)]
            fail_next_append: None,
            #[cfg(test)]
            fail_next_unlink: false,
            #[cfg(test)]
            fail_next_trim_fsync: false,
        })
    }

    /// Opens the log in `dir`: the longest valid prefix, the rest cut (fdatasynced) and the
    /// segments after the cut deleted.
    pub(crate) fn open(dir: &Path) -> Result<(Log, Recovered), LogError> {
        let listed = list(dir)?;
        if listed.is_empty() {
            return Err(LogError::Empty {
                dir: dir.to_owned(),
            });
        }
        let mut kept: Vec<Segment> = Vec::new();
        let mut records: Vec<LogRecord> = Vec::new();
        let mut claims = Vec::new();
        // The current segment's header, length, header length and last chain.
        let mut current: Option<(SegmentHeader, u64, u64, u32)> = None;
        let mut state: Option<SegmentHeader> = None;
        let mut next = Position(0);
        let mut cut: Option<Cut> = None;
        let mut removed = Vec::new();
        for segment in listed {
            if cut.is_some() {
                removed.push(segment);
                continue;
            }
            let bytes = fs::read(&segment.path).map_err(io("read", &segment.path))?;
            let scanned = match scan(&bytes) {
                Ok(s) => s,
                // The first segment's header is the log's identity, and any header of another
                // format or version is a refusal, never a cut.
                Err(source) if kept.is_empty() || !torn_header(&source) => {
                    return Err(LogError::Segment {
                        path: segment.path,
                        source,
                    });
                }
                // A later segment's header cut short or not verifying: a crash during a roll.
                Err(_torn_header) => {
                    cut = Some(Cut {
                        segment: segment.path.clone(),
                        at: 0,
                    });
                    removed.push(segment);
                    continue;
                }
            };
            if let Some(first) = &state {
                if scanned.header.lineage != first.lineage {
                    return Err(LogError::Lineage {
                        path: segment.path,
                        expected: first.lineage,
                        found: scanned.header.lineage,
                    });
                }
                if scanned.header.first_position != next {
                    return Err(LogError::Gap {
                        path: segment.path,
                        at: 0,
                        expected: next,
                        found: scanned.header.first_position,
                    });
                }
            }
            let mut carried = scanned.header.clone();
            let mut chain = scanned.seed;
            let mut end = scanned.header_len;
            next = scanned.header.first_position;
            for r in scanned.records {
                carry(&mut carried, &r.body);
                if let RecordBody::Claim { epoch, activation } = r.body {
                    claims.push((epoch, activation));
                }
                chain = r.chain;
                end = r.end;
                next = r.position.checked_add_one(dir)?;
                records.push(LogRecord {
                    position: r.position,
                    body: r.body,
                });
            }
            let to_u64 = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
            match scanned.tail {
                Tail::Clean => {}
                Tail::Torn { at } => {
                    cut = Some(Cut {
                        segment: segment.path.clone(),
                        at: to_u64(at),
                    });
                }
                Tail::Gap {
                    at,
                    expected,
                    found,
                } => {
                    return Err(LogError::Gap {
                        path: segment.path,
                        at: to_u64(at),
                        expected,
                        found,
                    });
                }
                Tail::Malformed { at, kind } => {
                    return Err(LogError::Malformed {
                        path: segment.path,
                        at: to_u64(at),
                        kind,
                    });
                }
            }
            current = Some((
                scanned.header,
                to_u64(end),
                to_u64(scanned.header_len),
                chain,
            ));
            state = Some(carried);
            kept.push(segment);
        }
        let (Some((header, len, header_len, chain)), Some(mut state), Some(last)) =
            (current, state, kept.last())
        else {
            return Err(LogError::Empty {
                dir: dir.to_owned(),
            });
        };
        // Checked before the cut changes anything.
        let min = state.base.min(state.acked);
        // Trim deletes only segments whose records all lie at or below `min`: a log starting later
        // lost records it still needs.
        if let Some(first) = kept.first()
            && first.first.0 > min.0.saturating_add(1)
        {
            return Err(LogError::MissingPrefix {
                path: first.path.clone(),
                first: first.first,
                needed: Position(min.0.saturating_add(1)),
            });
        }
        let file = File::options()
            .read(true)
            .write(true)
            .open(&last.path)
            .map_err(io("open", &last.path))?;
        if let Some(c) = &cut {
            crate::log::emit(crate::log::Level::Warn, "log_cut", &[
                ("segment", &c.segment.display()),
                ("at", &c.at),
                ("removed_segments", &removed.len()),
            ]);
            // From the back: the later segments go first, then the current one is truncated.
            for segment in removed.iter().rev() {
                fs::remove_file(&segment.path).map_err(io("remove", &segment.path))?;
            }
            if !removed.is_empty() {
                sync_dir(dir).map_err(io("fsync", dir))?;
            }
            if file.metadata().map_err(io("stat", &last.path))?.len() > len {
                file.set_len(len).map_err(io("cut", &last.path))?;
                file.sync_data().map_err(io("fdatasync", &last.path))?;
            }
        }
        // The current segment's identity with the state at the end of the log.
        state.first_position = header.first_position;
        state.supersedes = header.supersedes;
        records.retain(|r| r.position > min);
        let recovered = Recovered {
            base: state.base,
            acked: state.acked,
            acked_offset: state.acked_offset,
            log_bytes: state.log_bytes,
            claim: state.claim,
            claims,
            records,
            cut,
            header,
        };
        let log = Log {
            dir: dir.to_owned(),
            segments: kept,
            file,
            len,
            header_len,
            chain,
            next,
            state,
            // Whatever was written before open may not be on disk yet.
            dirty: true,
            broken: false,
            roll_bytes: SEGMENT_BYTES,
            #[cfg(test)]
            fail_next_append: None,
            #[cfg(test)]
            fail_next_unlink: false,
            #[cfg(test)]
            fail_next_trim_fsync: false,
        };
        Ok((log, recovered))
    }

    /// The position the next record gets.
    pub(crate) fn next_position(&self) -> Position {
        self.next
    }

    /// The latest ACK, as appended.
    pub(crate) fn acked(&self) -> Position {
        self.state.acked
    }

    /// The frame bytes logged since the latest snapshot, as the latest ACK carries them.
    pub(crate) fn log_bytes(&self) -> u64 {
        self.state.log_bytes
    }

    /// Appends `body` as the next record: one positioned write, no fsync. A failed write is cut
    /// back, so the segment ends at the previous record; a failed cut refuses every later append.
    pub(crate) fn append(&mut self, body: &RecordBody) -> Result<Position, LogError> {
        if self.broken {
            return Err(LogError::Broken {
                dir: self.dir.clone(),
            });
        }
        if self.len >= self.roll_bytes && self.len > self.header_len {
            self.roll()?;
        }
        let position = self.next;
        let after = position.checked_add_one(&self.dir)?;
        let path = self.current_path();
        let (bytes, chain) =
            encode_record(self.chain, position, body).map_err(|source| LogError::Encode {
                path: path.clone(),
                source,
            })?;
        if let Err(write) = self.write_at_end(&bytes) {
            self.dirty = true;
            return Err(match self.file.set_len(self.len) {
                Ok(()) => LogError::Io {
                    op: "append to",
                    path,
                    source: write,
                },
                Err(cut) => {
                    self.broken = true;
                    LogError::Cut { path, write, cut }
                }
            });
        }
        self.len = self
            .len
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        self.chain = chain;
        self.next = after;
        self.dirty = true;
        carry(&mut self.state, body);
        Ok(position)
    }

    fn write_at_end(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        #[cfg(test)]
        if let Some(n) = self.fail_next_append.take() {
            self.file
                .write_all_at(bytes.get(..n).unwrap_or(bytes), self.len)?;
            return Err(std::io::Error::other("injected append failure"));
        }
        self.file.write_all_at(bytes, self.len)
    }

    fn current_path(&self) -> PathBuf {
        self.segments
            .last()
            .map_or_else(|| self.dir.clone(), |s| s.path.clone())
    }

    /// fdatasyncs the current segment if it holds records not yet synced.
    pub(crate) fn sync(&mut self) -> Result<(), LogError> {
        if self.dirty {
            let path = self.current_path();
            self.file.sync_data().map_err(io("fdatasync", &path))?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Starts a new segment at the next position, carrying the state over: the segment left is
    /// fdatasynced first, the new one and the directory fsynced before it takes a record.
    fn roll(&mut self) -> Result<(), LogError> {
        self.sync()?;
        let mut h = self.state.clone();
        h.first_position = self.next;
        h.supersedes = None;
        let segment = Segment {
            first: self.next,
            path: segment_path(&self.dir, self.next),
        };
        let (file, header_len, seed) = write_segment(&segment.path, &h)?;
        sync_dir(&self.dir).map_err(io("fsync", &self.dir))?;
        self.file = file;
        self.len = header_len;
        self.header_len = header_len;
        self.chain = seed;
        self.state = h;
        self.segments.push(segment);
        Ok(())
    }

    /// Deletes the segments whose records all lie at or below `min(base, acked)` (F3), after
    /// fsyncing the log so the records that make them unnecessary are durable. With nothing to
    /// delete it does nothing. The current segment stays. Returns
    /// how many were deleted.
    pub(crate) fn trim(&mut self) -> Result<usize, LogError> {
        let durable_min = self.state.base.min(self.state.acked);
        // Segment i holds positions below segment i+1's first.
        let removable = self
            .segments
            .windows(2)
            .take_while(
                |w| matches!(w, [_, next] if next.first.0 <= durable_min.0.saturating_add(1)),
            )
            .count();
        if removable == 0 {
            return Ok(0);
        }
        // The records that make the segments unnecessary are durable before any goes.
        self.sync()?;
        // From the front, oldest first, each unlink durable before the next, and a segment leaves
        // the list only once it is gone for good: a failed step leaves both as they were.
        for _ in 0..removable {
            let Some(segment) = self.segments.first() else {
                break;
            };
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_unlink) {
                return Err(io("remove", &segment.path)(std::io::Error::other(
                    "injected unlink failure",
                )));
            }
            // Gone already: an earlier trim unlinked it and then failed its fsync.
            match fs::remove_file(&segment.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io("remove", &segment.path)(e)),
            }
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_trim_fsync) {
                return Err(io("fsync", &self.dir)(std::io::Error::other(
                    "injected fsync failure",
                )));
            }
            sync_dir(&self.dir).map_err(io("fsync", &self.dir))?;
            self.segments.remove(0);
        }
        Ok(removable)
    }

    /// The segment files, by first position.
    #[cfg(test)]
    pub(crate) fn segment_paths(&self) -> Vec<PathBuf> {
        self.segments.iter().map(|s| s.path.clone()).collect()
    }

    /// Test hook: roll past `bytes` instead of 16 MiB.
    #[cfg(test)]
    pub(crate) fn set_roll_bytes(&mut self, bytes: u64) {
        self.roll_bytes = bytes;
    }
}

/// Creates the segment file at `path` holding header `h`, fsynced. Returns the file, the header's
/// length and its checksum.
fn write_segment(path: &Path, h: &SegmentHeader) -> Result<(File, u64, u32), LogError> {
    let (bytes, seed) = encode_header(h).map_err(|source| LogError::Encode {
        path: path.to_owned(),
        source,
    })?;
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io("create", path))?;
    file.write_all_at(&bytes, 0).map_err(io("write", path))?;
    file.sync_all().map_err(io("fsync", path))?;
    Ok((file, u64::try_from(bytes.len()).unwrap_or(u64::MAX), seed))
}

trait NextPosition {
    fn checked_add_one(self, dir: &Path) -> Result<Position, LogError>;
}

impl NextPosition for Position {
    fn checked_add_one(self, dir: &Path) -> Result<Position, LogError> {
        self.0
            .checked_add(1)
            .map(Position)
            .ok_or_else(|| LogError::PositionOverflow {
                dir: dir.to_owned(),
                after: self,
            })
    }
}

#[cfg(test)]
mod tests;
