//! The commit log (design: Log): a generation's `log/` directory of `USL1` segments, named by
//! their first position (`<20 digits>.usl`) and rolled at 16 MiB.
//!
//! The valid log is the longest prefix whose chains verify and whose positions grow by one across
//! segments. [`Log::read`], the one reading rule, finds it and changes nothing: it streams every
//! segment through one buffer ([`crate::codec::segment::Reader`]) and hands each record to a
//! [`Fold`], which keeps what its caller needs. Besides the list of segments, a read takes that
//! buffer, what the fold keeps, a mark per frame of the longest run of caught-up frames, and the
//! headers of the segments that run reaches ([`Log::read`]). [`Log::open`] reads the log so,
//! keeping every record's body, then cuts the log after the valid prefix and reports the cut
//! ([`Cut`]) with a warning, whatever follows the first broken record: after a power loss the
//! filesystem may keep later unsynced records and lose an earlier one, and a database user can
//! store bytes that look like a record inside a commit. That keeps L3, since pages reach the db
//! only after F1 fsyncs the log, so every record whose pages could be there is in the durable
//! prefix. Stream frames a catch-up logged with no ACK after them are cut the same way: a catch-up
//! applies its frames only once that ACK is fsynced, so they were never applied, and the valid log
//! then holds no COMMIT record after its latest ACK but the generation's own pending ones. A gap or
//! a malformed record under valid chains fails (`internal`: the extension wrote them), and so does
//! a segment header of another format or version. Segments are removed from the front (trim, oldest
//! first) and cut from the back (open deletes the later segments before it truncates), each step
//! made durable before the next, so a crash between steps leaves a log open reads the same way.
//! [`Log::append`] writes a record with one positioned write and no fsync, and cuts the segment
//! back when the write fails. [`Log::sync`] fdatasyncs the current segment when it holds unsynced
//! records. A roll fdatasyncs the segment it leaves and fsyncs the new one and the directory before
//! the first record goes there, so no record of a later segment outlives an earlier one's.
//! [`Log::trim`] deletes the segments whose records are all at or below `min(base, acked)` (F3),
//! after an fsync that makes those records durable, and does nothing (no fsync) when no segment
//! would go. [`Log::rekey`] re-keys the generation to a new stream token (design: Log, History):
//! the log rolls to a segment whose header names the token, and the CLAIM record of the claim that
//! follows goes there, fsynced. A crash before the CLAIM leaves a latest segment that names the new
//! token, which the next open reads as the generation's token. A clean close or a reset after the
//! roll and before the claim lands leaves the same, not only a crash: two history events with no
//! write between them then name a token from before the roll, and the next open salvages a
//! generation that lost nothing (accepted, design: History). A current segment with no record has
//! its header replaced instead (a temp file renamed over it), since a roll would start at the same
//! position. [`copy`] copies a log for a copy of its generation: every segment written again to
//! name the generation it supersedes.
//!
//! Every segment's header names the generation directory its generation superseded: a roll, a
//! re-key's included, carries it over from the segment it leaves. So whatever a trim leaves, the
//! first remaining segment still names it, and a crash after a publish's rename and before the
//! superseded generation goes is finished at the next open (`Root::finish_leftovers`). Once that
//! generation is gone the id names nothing, and nothing reads it.

use std::fs::File;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use crate::codec::frame::CommitIdReader;
use crate::codec::frame::commit_id;
use crate::codec::segment::Keep;
use crate::codec::segment::ReadError;
use crate::codec::segment::Reader;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentError;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::Skip;
use crate::codec::segment::Source;
use crate::codec::segment::Step as ReadStep;
use crate::codec::segment::Tail;
use crate::codec::segment::Whole;
use crate::codec::segment::encode_header;
use crate::codec::segment::encode_record;
use crate::codec::segment::scan;
use crate::config::Step;
use crate::config::abort_at;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::GenId;
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
    /// An append's write failed and was cut back: the log ends at the record before, as if
    /// nothing was appended. The only failure after which the log takes appends as before.
    #[error("log segment {}: an append failed and was cut back: {source}", .path.display())]
    CutBack {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("log {}: refused after a failed cut", .dir.display())]
    Broken { dir: PathBuf },
    /// The read's [`Fold`]'s deadline passed before the end of the log ([`Fold::deadline`]).
    #[error("log {}: the read stopped before the end of the log", .dir.display())]
    Stopped { dir: PathBuf },
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
    /// Whether the valid log holds a COMMIT record, at or below `min(base, acked)` included.
    pub(crate) any_commit: bool,
    /// Where open cut the log, if it did.
    pub(crate) cut: Option<Cut>,
}

impl Recovered {
    /// Whether a COMMIT record, the generation's own or a caught-up one, follows the latest ACK.
    /// [`Log::open`] cuts caught-up ones no ACK follows, so only the generation's own remain, but
    /// a caller deciding whether the history may have lost one checks both.
    pub(crate) fn commit_after_ack(&self) -> bool {
        self.records
            .iter()
            .any(|r| r.position > self.acked && matches!(r.body, RecordBody::Commit { .. }))
    }

    /// This generation's COMMIT records after the latest ACK: logged, not acknowledged. Stream
    /// frames a catch-up logged carry another lineage and are never pending: an ACK follows them,
    /// or [`Log::open`] cut them.
    pub(crate) fn pending(&self) -> Vec<(Position, Arc<[u8]>)> {
        self.records
            .iter()
            .filter(|r| r.position > self.acked)
            .filter_map(|r| match &r.body {
                RecordBody::Commit { frame } if !foreign(commit_id(frame), self.header.lineage) => {
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
    /// Test hook: the next roll leaves a partial segment and fails.
    #[cfg(test)]
    pub(crate) fail_next_roll: bool,
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

/// Whether a COMMIT record whose frame names commit `id` ([`commit_id`]) holds a commit of another
/// lineage than `ours`: a stream frame a catch-up logged.
fn foreign(id: Option<CommitId>, ours: Lineage) -> bool {
    id.is_some_and(|id| id.lineage != ours)
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// A segment file [`Log::read`] reads, until the read's deadline ([`Fold::deadline`]): every read
/// of its next bytes, at most a buffer's worth ([`crate::codec::segment::BUFFER`]), fails once the
/// deadline passed, so even a read in the middle of one long record stops.
#[derive(Debug)]
struct Timed {
    file: File,
    deadline: Option<Instant>,
}

/// Why a [`Timed`] read failed.
#[derive(Debug, thiserror::Error)]
enum TimedError {
    #[error(transparent)]
    Io(std::io::Error),
    #[error("the read's deadline passed")]
    Stopped,
}

impl Source for Timed {
    type Error = TimedError;

    fn read_some(&mut self, out: &mut [u8]) -> Result<usize, TimedError> {
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(TimedError::Stopped);
        }
        self.file.read_some(out).map_err(TimedError::Io)
    }
}

/// A segment [`Reader`]'s failure, of the segment at `path` in the log `dir`.
fn segment_error(dir: &Path, path: &Path, e: ReadError<TimedError>) -> LogError {
    match e {
        ReadError::Source(TimedError::Io(source)) => LogError::Io {
            op: "read",
            path: path.to_owned(),
            source,
        },
        ReadError::Source(TimedError::Stopped) => LogError::Stopped {
            dir: dir.to_owned(),
        },
        ReadError::Segment(source) => LogError::Segment {
            path: path.to_owned(),
            source,
        },
    }
}

/// Reads the rest of a segment whose records are not wanted, so it fails where a scan of the
/// whole segment fails.
fn drain(reader: &mut Reader<Timed>) -> Result<(), ReadError<TimedError>> {
    while let ReadStep::Record(_) = reader.next(&mut Skip)? {}
    Ok(())
}

/// A COMMIT record's frame as [`Log::read`] hands it to a [`Fold`]: the commit id the frame
/// names, if it is a whole COMMIT frame ([`commit_id`]), and what the fold's [`Keep`] kept of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Logged<K> {
    pub(crate) id: Option<CommitId>,
    pub(crate) kept: K,
}

/// What [`Log::read`] hands the records of the valid log to, in log order, and then its end. The
/// open keeps every record's body. A reader that needs less keeps less.
pub(crate) trait Fold {
    /// What the fold keeps of a COMMIT record's frame.
    type Frames: Keep + Default;

    /// The next record of the valid log, and the state carried past it: its segment's header
    /// with the carried-over fields brought past the record.
    fn record(
        &mut self,
        position: Position,
        body: RecordBody<Logged<<Self::Frames as Keep>::Kept>>,
        state: &SegmentHeader,
    );

    /// The records from `from` on are not part of the log: stream frames a catch-up logged with
    /// no ACK after them, which the read cuts once it reaches the end of the log.
    fn cut(&mut self, from: Position);

    /// The valid log ends, once, after every record and any [`Fold::cut`]: `state` is the current
    /// segment's header with the carried-over fields brought to the end of the log, and `cut`
    /// where the valid log ends before the bytes on disk do, if it does.
    fn end(&mut self, state: &SegmentHeader, cut: Option<&Cut>);

    /// When the read stops, failing [`LogError::Stopped`] at its next read of a segment's bytes:
    /// a host's read that ran out of time. The open never stops.
    fn deadline(&self) -> Option<Instant> {
        None
    }
}

/// The [`Keep`] [`Log::read`] reads COMMIT records with: the commit id each frame names, read as
/// the frame streams by, beside what the fold keeps.
#[derive(Debug, Default)]
struct WithId<K> {
    id: CommitIdReader,
    frames: K,
}

impl<K: Keep> Keep for WithId<K> {
    type Kept = Logged<K::Kept>;

    fn start(&mut self, len: u32) {
        self.id = CommitIdReader::default();
        self.frames.start(len);
    }

    fn piece(&mut self, bytes: &[u8]) {
        self.id.feed(bytes);
        self.frames.piece(bytes);
    }

    fn finish(&mut self) -> Logged<K::Kept> {
        Logged {
            id: self.id.finish(),
            kept: self.frames.finish(),
        }
    }
}

/// The open's [`Fold`]: every record after `min(base, acked)` with its body whole, every CLAIM,
/// and whether the valid log holds a COMMIT record.
#[derive(Debug, Default)]
struct Records {
    records: Vec<LogRecord>,
    claims: Vec<(Epoch, Activation)>,
    any_commit: bool,
}

impl Fold for Records {
    type Frames = Whole;

    fn record(
        &mut self,
        position: Position,
        body: RecordBody<Logged<Vec<u8>>>,
        _state: &SegmentHeader,
    ) {
        if let RecordBody::Claim { epoch, activation } = &body {
            self.claims.push((*epoch, *activation));
        }
        self.records.push(LogRecord {
            position,
            body: body.map_frame(|frame| frame.kept),
        });
    }

    fn cut(&mut self, from: Position) {
        self.records.retain(|r| r.position < from);
    }

    fn end(&mut self, state: &SegmentHeader, _cut: Option<&Cut>) {
        self.any_commit = self
            .records
            .iter()
            .any(|r| matches!(r.body, RecordBody::Commit { .. }));
        let min = state.base.min(state.acked);
        self.records.retain(|r| r.position > min);
    }
}

/// Keeps a COMMIT frame's length and none of its bytes.
#[derive(Debug, Default)]
pub(crate) struct Length(u32);

impl Keep for Length {
    type Kept = u32;

    fn start(&mut self, len: u32) {
        self.0 = len;
    }

    fn piece(&mut self, _bytes: &[u8]) {}

    fn finish(&mut self) -> u32 {
        self.0
    }
}

/// A host's fold over [`Log::read`] (design: Hosts): the position and frame length of each of the
/// generation's own COMMIT records above the latest ACK, dropped as later ACKs come, so it keeps no
/// frame and 16 bytes per pending commit. At the end it holds what [`Recovered::pending`] holds,
/// unless an ACK went below an earlier one and brought back a commit the fold had dropped: a second
/// read [`Unacknowledged::above`] the latest ACK then keeps exactly those. It stops the read once
/// its deadline passes ([`Fold::deadline`]).
#[derive(Debug, Default)]
pub(crate) struct Unacknowledged {
    commits: Vec<(Position, u64)>,
    /// Drop only the commits at or below this position, not those at or below each ACK.
    floor: Option<Position>,
    /// The highest position an ACK dropped.
    dropped: Option<Position>,
    /// At the end of the log: whether a lower ACK brought back a commit an earlier one dropped,
    /// which the fold no longer has.
    lowered: bool,
    /// The state at the end of the log, once the read ended.
    state: Option<SegmentHeader>,
    deadline: Option<Instant>,
}

impl Fold for Unacknowledged {
    type Frames = Length;

    fn record(&mut self, position: Position, body: RecordBody<Logged<u32>>, state: &SegmentHeader) {
        if let RecordBody::Commit { frame } = &body
            && !foreign(frame.id, state.lineage)
        {
            self.commits.push((position, u64::from(frame.kept)));
        }
        self.drop_through(state.acked);
    }

    fn cut(&mut self, from: Position) {
        self.commits.retain(|(p, _)| *p < from);
    }

    fn end(&mut self, state: &SegmentHeader, _cut: Option<&Cut>) {
        self.drop_through(state.acked);
        self.lowered = self.dropped.is_some_and(|d| d > state.acked);
        self.state = Some(state.clone());
    }

    fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

impl Unacknowledged {
    /// A fold that stops the read at `deadline`, when there is one.
    pub(crate) fn until(deadline: Option<Instant>) -> Unacknowledged {
        Unacknowledged {
            deadline,
            ..Unacknowledged::default()
        }
    }

    /// A fold for a second read of a log whose ACKs went below an earlier one: it keeps every own
    /// commit above `floor`, the latest ACK the first read found.
    pub(crate) fn above(floor: Position, deadline: Option<Instant>) -> Unacknowledged {
        Unacknowledged {
            floor: Some(floor),
            deadline,
            ..Unacknowledged::default()
        }
    }

    fn drop_through(&mut self, acked: Position) {
        let through = self.floor.unwrap_or(acked);
        for (p, _) in self.commits.iter().filter(|(p, _)| *p <= through) {
            self.dropped = self.dropped.max(Some(*p));
        }
        self.commits.retain(|(p, _)| *p > through);
    }

    /// The state at the end of the log, once the read ended.
    pub(crate) fn state(&self) -> Option<&SegmentHeader> {
        self.state.as_ref()
    }

    /// The pending commits once the read ended: `None` before, when a lower ACK brought back a
    /// commit an earlier one dropped, or when a read [`Unacknowledged::above`] a floor found
    /// another latest ACK (the log changed between the reads).
    pub(crate) fn pending(&self) -> Option<&[(Position, u64)]> {
        let state = self.state.as_ref()?;
        let exact = !self.lowered && self.floor.is_none_or(|f| f == state.acked);
        exact.then_some(self.commits.as_slice())
    }
}

/// The valid log as [`Log::read`] found it: where it ends, and what an open cuts after it.
#[derive(Debug)]
pub(crate) struct Valid {
    /// The segments of the valid log, by first position: the last one is current.
    segments: Vec<Segment>,
    /// The segments after the cut, in log order.
    removed: Vec<Segment>,
    /// The current segment's header, as written.
    header: SegmentHeader,
    header_len: u64,
    /// The current segment's valid length.
    len: u64,
    /// The last record's chain (the current header's checksum before its first record).
    chain: u32,
    /// The position the next record gets.
    next: Position,
    /// The current segment's identity with the state at the end of the valid log.
    state: SegmentHeader,
    /// Where the valid log ends before the bytes on disk do.
    cut: Option<Cut>,
}

/// The trailing run of stream frames a catch-up logged, as [`Log::read`] goes: at the end of the
/// log, those above the latest ACK are cut, since no ACK follows them.
#[derive(Debug, Default)]
struct Run {
    /// The position of its first record.
    first: Position,
    /// Per record, in log order: its segment in `reached`, where it starts, and the chain before
    /// it.
    marks: Vec<(usize, u64, u32)>,
    /// The segments it reaches, in log order.
    reached: Vec<Reached>,
}

/// A segment a [`Run`] reaches.
#[derive(Debug)]
struct Reached {
    /// The segment's index among the kept ones.
    index: usize,
    header: SegmentHeader,
    header_len: u64,
    /// The state carried to the run's first record there.
    carried: SegmentHeader,
}

/// Where [`Run::unapplied`] cuts: the record, its segment, where it starts and the chain before
/// it.
#[derive(Debug)]
struct Unapplied<'a> {
    position: Position,
    segment: &'a Reached,
    start: u64,
    before: u32,
}

impl Run {
    /// The record at `position`, starting at `start` after chain `before`, in the kept segment
    /// `index`, which `reach` describes when the run reaches it there.
    fn push(
        &mut self,
        position: Position,
        start: u64,
        before: u32,
        index: usize,
        reach: impl FnOnce() -> Reached,
    ) {
        if self.marks.is_empty() {
            self.first = position;
        }
        if self.reached.last().is_none_or(|r| r.index != index) {
            self.reached.push(reach());
        }
        let slot = self.reached.len().saturating_sub(1);
        self.marks.push((slot, start, before));
    }

    fn clear(&mut self) {
        self.marks.clear();
        self.reached.clear();
    }

    /// The first of its records above `acked`: it and every later one were never applied.
    fn unapplied(&self, acked: Position) -> Option<Unapplied<'_>> {
        // Positions grow by one along the run, so those at or below `acked` come first.
        let below = acked.0.checked_add(1)?.saturating_sub(self.first.0);
        let &(slot, start, before) = self.marks.get(usize::try_from(below).ok()?)?;
        Some(Unapplied {
            position: Position(self.first.0.checked_add(below)?),
            segment: self.reached.get(slot)?,
            start,
            before,
        })
    }
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
fn carry<F>(state: &mut SegmentHeader, body: &RecordBody<F>) {
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
            #[cfg(test)]
            fail_next_roll: false,
        })
    }

    /// Reads the log in `dir` (design: Log) and changes nothing: finds the valid prefix, a torn
    /// tail after it, and stream frames a catch-up logged with no ACK after them, hands each
    /// record of the valid log to `fold`, in log order, and then its end ([`Fold::end`]). A gap or
    /// a malformed record under valid chains, a segment header of another format or version, and
    /// a log missing records it still needs fail. Every segment streams through one buffer, so
    /// whatever the size of a record or a segment, the read takes the list of segments, that
    /// buffer, what `fold` keeps, a mark per frame of the longest run of caught-up frames, and the
    /// headers of the segments that run reaches ([`Run`]).
    pub(crate) fn read<F: Fold>(dir: &Path, fold: &mut F) -> Result<Valid, LogError> {
        let listed = list(dir)?;
        if listed.is_empty() {
            return Err(LogError::Empty {
                dir: dir.to_owned(),
            });
        }
        let mut kept: Vec<Segment> = Vec::new();
        let mut removed = Vec::new();
        // The current segment's header, length, header length and last chain.
        let mut current: Option<(SegmentHeader, u64, u64, u32)> = None;
        let mut state: Option<SegmentHeader> = None;
        let mut next = Position(0);
        let mut cut: Option<Cut> = None;
        let mut run = Run::default();
        let mut frames = WithId::<F::Frames>::default();
        for segment in listed {
            if cut.is_some() {
                removed.push(segment);
                continue;
            }
            let file = File::open(&segment.path).map_err(io("open", &segment.path))?;
            let deadline = fold.deadline();
            let mut reader = match Reader::new(Timed { file, deadline }) {
                Ok(reader) => reader,
                // The first segment's header is the log's identity, and any header of another
                // format or version is a refusal, never a cut.
                Err(ReadError::Segment(source)) if kept.is_empty() || !torn_header(&source) => {
                    return Err(LogError::Segment {
                        path: segment.path,
                        source,
                    });
                }
                // A later segment's header cut short or not verifying: a crash during a roll.
                Err(ReadError::Segment(_torn_header)) => {
                    cut = Some(Cut {
                        segment: segment.path.clone(),
                        at: 0,
                    });
                    removed.push(segment);
                    continue;
                }
                Err(e) => return Err(segment_error(dir, &segment.path, e)),
            };
            let header = reader.header().clone();
            if let Some(first) = &state {
                let mismatch = if header.lineage != first.lineage {
                    Some(LogError::Lineage {
                        path: segment.path.clone(),
                        expected: first.lineage,
                        found: header.lineage,
                    })
                } else if header.first_position != next {
                    Some(LogError::Gap {
                        path: segment.path.clone(),
                        at: 0,
                        expected: next,
                        found: header.first_position,
                    })
                } else {
                    None
                };
                if let Some(mismatch) = mismatch {
                    // A record the segment's reader refuses fails first, as it fails a scan of
                    // the whole segment.
                    return Err(match drain(&mut reader) {
                        Ok(()) => mismatch,
                        Err(e) => segment_error(dir, &segment.path, e),
                    });
                }
            }
            let index = kept.len();
            let header_len = to_u64(reader.header_len());
            let mut carried = header.clone();
            let mut chain = reader.seed();
            let mut end = header_len;
            let mut last = None;
            let tail = loop {
                let r = match reader
                    .next(&mut frames)
                    .map_err(|e| segment_error(dir, &segment.path, e))?
                {
                    ReadStep::Record(r) => r,
                    ReadStep::End(tail) => break tail,
                };
                if matches!(&r.body, RecordBody::Commit { frame } if foreign(frame.id, header.lineage))
                {
                    run.push(r.position, end, chain, index, || Reached {
                        index,
                        header: header.clone(),
                        header_len,
                        carried: carried.clone(),
                    });
                } else {
                    run.clear();
                }
                carry(&mut carried, &r.body);
                chain = r.chain;
                end = to_u64(r.end);
                last = Some(r.position);
                fold.record(r.position, r.body, &carried);
            };
            next = match last {
                Some(position) => position.checked_add_one(dir)?,
                None => header.first_position,
            };
            match tail {
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
            current = Some((header, end, header_len, chain));
            state = Some(carried);
            kept.push(segment);
        }
        // Stream frames a catch-up logged with no ACK after them were never applied: a catch-up
        // applies its frames only once the ACK after them is fsynced, and nothing logs a record
        // between them. They are the log's last records, and go like a torn tail, so the log
        // holds no COMMIT record after its latest ACK but the generation's own pending ones. The
        // state carried to the cut point comes back with them: a later segment (an empty one a
        // re-key rolled to, say) may name another token.
        if let Some(acked) = state.as_ref().map(|st| st.acked)
            && let Some(unapplied) = run.unapplied(acked)
            && let Some(path) = kept.get(unapplied.segment.index).map(|k| k.path.clone())
        {
            let at = unapplied.segment;
            let later = kept.split_off(at.index.saturating_add(1));
            removed.splice(0..0, later);
            state = Some(at.carried.clone());
            current = Some((
                at.header.clone(),
                unapplied.start,
                at.header_len,
                unapplied.before,
            ));
            next = unapplied.position;
            fold.cut(unapplied.position);
            cut = Some(Cut {
                segment: path,
                at: unapplied.start,
            });
        }
        let (Some((header, len, header_len, chain)), Some(mut state)) = (current, state) else {
            return Err(LogError::Empty {
                dir: dir.to_owned(),
            });
        };
        // Trim deletes only segments whose records all lie at or below `min(base, acked)`: a log
        // starting later lost records it still needs.
        let min = state.base.min(state.acked);
        if let Some(first) = kept.first()
            && first.first.0 > min.0.saturating_add(1)
        {
            return Err(LogError::MissingPrefix {
                path: first.path.clone(),
                first: first.first,
                needed: Position(min.0.saturating_add(1)),
            });
        }
        // The current segment's identity with the state at the end of the log.
        state.first_position = header.first_position;
        state.supersedes = header.supersedes;
        fold.end(&state, cut.as_ref());
        Ok(Valid {
            segments: kept,
            removed,
            header,
            header_len,
            len,
            chain,
            next,
            state,
            cut,
        })
    }

    /// Opens the log in `dir`: the valid log [`Log::read`] finds, every record's body kept, then
    /// the rest cut (fdatasynced) and the segments after the cut deleted.
    pub(crate) fn open(dir: &Path) -> Result<(Log, Recovered), LogError> {
        let mut found = Records::default();
        let Valid {
            segments,
            removed,
            header,
            header_len,
            len,
            chain,
            next,
            state,
            cut,
        } = Log::read(dir, &mut found)?;
        let Some(last) = segments.last() else {
            return Err(LogError::Empty {
                dir: dir.to_owned(),
            });
        };
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
            abort_at(Step::CutRemoved);
            if file.metadata().map_err(io("stat", &last.path))?.len() > len {
                file.set_len(len).map_err(io("cut", &last.path))?;
                file.sync_data().map_err(io("fdatasync", &last.path))?;
            }
        }
        let Records {
            records,
            claims,
            any_commit,
        } = found;
        let recovered = Recovered {
            base: state.base,
            acked: state.acked,
            acked_offset: state.acked_offset,
            log_bytes: state.log_bytes,
            claim: state.claim,
            claims,
            records,
            any_commit,
            cut,
            header,
        };
        let log = Log {
            dir: dir.to_owned(),
            segments,
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
            #[cfg(test)]
            fail_next_roll: false,
        };
        Ok((log, recovered))
    }

    /// The position the next record gets.
    pub(crate) fn next_position(&self) -> Position {
        self.next
    }

    /// The generation's lineage, as its segment headers name it.
    pub(crate) fn lineage(&self) -> Lineage {
        self.state.lineage
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
                Ok(()) => LogError::CutBack {
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

    /// Re-keys the generation to `incarnation` (design: History): a new segment naming it, holding
    /// the CLAIM record of `(epoch, activation)`, fsynced before this returns. A current segment
    /// with no record has its header replaced instead, atomically. Returns the CLAIM's position.
    pub(crate) fn rekey(
        &mut self,
        incarnation: Incarnation,
        epoch: Epoch,
        activation: Activation,
    ) -> Result<Position, LogError> {
        if self.broken {
            return Err(LogError::Broken {
                dir: self.dir.clone(),
            });
        }
        if self.len > self.header_len {
            self.roll_to(incarnation)?;
        } else {
            self.replace_header(incarnation)?;
        }
        abort_at(Step::RekeyRolled);
        let position = self.append(&RecordBody::Claim { epoch, activation })?;
        self.sync()?;
        abort_at(Step::RekeyClaimed);
        Ok(position)
    }

    /// Replaces the header of the current segment, which holds no record, by one naming
    /// `incarnation`: written to a temp file, fsynced, renamed over the segment, and the directory
    /// fsynced. A crash leaves the old header or the new one, and at most a temp file, which no
    /// listing reads.
    fn replace_header(&mut self, incarnation: Incarnation) -> Result<(), LogError> {
        let path = self.current_path();
        let temp = path.with_extension("usl.tmp");
        let mut h = self.state.clone();
        h.incarnation = incarnation;
        match fs::remove_file(&temp) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io("remove", &temp)(e)),
        }
        let (file, header_len, seed) = write_segment(&temp, &h)?;
        fs::rename(&temp, &path).map_err(io("rename", &path))?;
        sync_dir(&self.dir).map_err(io("fsync", &self.dir))?;
        self.file = file;
        self.len = header_len;
        self.header_len = header_len;
        self.chain = seed;
        self.state = h;
        self.dirty = false;
        Ok(())
    }

    /// Starts a new segment at the next position, carrying the state over: the segment left is
    /// fdatasynced first, the new one and the directory fsynced before it takes a record.
    fn roll(&mut self) -> Result<(), LogError> {
        let incarnation = self.state.incarnation;
        self.roll_to(incarnation)
    }

    /// [`Log::roll`] to a segment whose header names `incarnation`.
    fn roll_to(&mut self, incarnation: Incarnation) -> Result<(), LogError> {
        self.sync()?;
        // The state carries the header's `supersedes` over to the new segment.
        let mut h = self.state.clone();
        h.first_position = self.next;
        h.incarnation = incarnation;
        let segment = Segment {
            first: self.next,
            path: segment_path(&self.dir, self.next),
        };
        // A roll that failed before left at most this segment, which no record went into: it goes,
        // so one failure (`ENOSPC`, say) does not refuse every later roll.
        match fs::remove_file(&segment.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io("remove", &segment.path)(e)),
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_roll) {
            fs::write(&segment.path, b"USL1").map_err(io("write", &segment.path))?;
            return Err(io("write", &segment.path)(std::io::Error::other(
                "injected roll failure",
            )));
        }
        let written = write_segment(&segment.path, &h);
        let (file, header_len, seed) = match written {
            Ok(written) => written,
            Err(e) => {
                if let Err(left) = fs::remove_file(&segment.path) {
                    crate::log::emit(crate::log::Level::Warn, "log_roll_leftover", &[
                        ("segment", &segment.path.display()),
                        ("error", &left),
                    ]);
                }
                return Err(e);
            }
        };
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
            abort_at(Step::TrimUnlinked);
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

/// Copies the log in `from` into `to`, created, for a copy of its generation (design: Materialize):
/// every segment written again, its header naming the generation directory `supersedes` and its
/// records chained anew from that header, so every segment of the copy names what it supersedes, as
/// a roll keeps them. Each segment and the directory are fsynced. A segment that ends in a torn
/// record is copied up to it, and the segments after it are left out, as [`Log::open`] would cut
/// them.
pub(crate) fn copy(from: &Path, to: &Path, supersedes: GenId) -> Result<(), LogError> {
    let listed = list(from)?;
    if listed.is_empty() {
        return Err(LogError::Empty {
            dir: from.to_owned(),
        });
    }
    fs::create_dir_all(to).map_err(io("create", to))?;
    for segment in &listed {
        if copy_segment(segment, to, supersedes)? == Copied::Torn {
            break;
        }
    }
    sync_dir(to).map_err(io("fsync", to))
}

/// How [`copy_segment`] found a segment's end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Copied {
    Clean,
    /// Cut at a torn record: no later segment is part of the log.
    Torn,
}

/// Writes `segment` into `to` again, its header naming `supersedes`, its valid records chained
/// anew, fsynced.
fn copy_segment(segment: &Segment, to: &Path, supersedes: GenId) -> Result<Copied, LogError> {
    let bytes = fs::read(&segment.path).map_err(io("read", &segment.path))?;
    let scanned = scan(&bytes).map_err(|source| LogError::Segment {
        path: segment.path.clone(),
        source,
    })?;
    let copied = match scanned.tail {
        Tail::Clean => Copied::Clean,
        Tail::Torn { .. } => Copied::Torn,
        Tail::Gap {
            at,
            expected,
            found,
        } => {
            return Err(LogError::Gap {
                path: segment.path.clone(),
                at: to_u64(at),
                expected,
                found,
            });
        }
        Tail::Malformed { at, kind } => {
            return Err(LogError::Malformed {
                path: segment.path.clone(),
                at: to_u64(at),
                kind,
            });
        }
    };
    let target = segment_path(to, segment.first);
    let encode = |source| LogError::Encode {
        path: target.clone(),
        source,
    };
    let mut header = scanned.header;
    header.supersedes = Some(supersedes);
    let (mut out, mut chain) = encode_header(&header).map_err(encode)?;
    for r in &scanned.records {
        let (record, next) = encode_record(chain, r.position, &r.body).map_err(encode)?;
        out.extend_from_slice(&record);
        chain = next;
    }
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(&target)
        .map_err(io("create", &target))?;
    file.write_all_at(&out, 0).map_err(io("write", &target))?;
    file.sync_all().map_err(io("fsync", &target))?;
    Ok(copied)
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
