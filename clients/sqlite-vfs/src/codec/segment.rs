//! `USL1` local log segments: a header, then a chain of records.
//!
//! ```text
//! segment = header | record*
//! header  = "USL1" | u32 version=1 | [16] lineage | u64 first_position | opt supersedes
//!         | str cluster_id | str stream_path | u64 incarnation
//!         | u64 base | u64 acked | u64 acked_offset | u64 log_bytes      carried over
//!         | claim                                                       the latest CLAIM
//!         | u32 crc32c(header)
//! record  = u32 len | u8 kind | u64 position | payload (len bytes) | u32 chain
//! chain   = crc32c(le32 previous chain | kind | le64 position | payload), seeded by the header crc
//! str     = u16 len | utf-8
//! opt     = u8 0 | u8 1 | [16]
//! claim   = u8 0 | u8 1 | u64 epoch | [16] activation
//! ```
//!
//! | kind | payload |
//! | --- | --- |
//! | 1 COMMIT | a `USQ2` frame |
//! | 2 CLAIM | `u64 epoch`, `[16] activation` |
//! | 3 ACK | `u64 acked`, `u64 acked_offset`, `u64 log_bytes` |
//! | 4 BASE | `u64 position` |
//!
//! `supersedes` is the id of the generation directory (`gen-<id32>`) a new generation replaces:
//! directories are named by a random id, not by the lineage, since a copy of a generation keeps its
//! lineage. Every segment of a generation names it, a rolled one too, so a trim never removes it.
//!
//! Positions start at 1, so a `base` or `acked` of 0 means nothing yet. The valid log is the
//! longest prefix whose chain verifies and whose positions grow by one from the header's
//! `first_position`. [`Reader`] reads that prefix record by record and says how the rest ends, and
//! [`scan`] collects it from a segment held whole. A record that is cut short or does not verify
//! ends it as [`Tail::Torn`], whatever follows: bytes after a broken record that look like a
//! record prove nothing, since a database user can store them in a commit.
//!
//! A [`Reader`] passes every record's payload through one buffer of [`BUFFER`] bytes and keeps of
//! a COMMIT record's frame only what its [`Keep`] keeps, so reading a segment takes that buffer
//! whatever the size of a record or of the segment. Only the header is read whole: at most 131 KiB
//! by its format.

use std::convert::Infallible;
use std::fs::File;

use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::Take;
use crate::ids::Activation;
use crate::ids::GenId;
use crate::ids::Lineage;
use crate::ids::Position;

const MAGIC: &[u8; 4] = b"USL1";
const VERSION: u32 = 1;

const COMMIT: u8 = 1;
const CLAIM: u8 = 2;
const ACK: u8 = 3;
const BASE: u8 = 4;

/// A record's length, kind and position.
const RECORD_HEAD: usize = 13;
/// A record's chain.
const RECORD_TAIL: usize = 4;
/// The longest payload of a record of a fixed shape: a CLAIM's or an ACK's.
const FIXED_PAYLOAD: usize = 24;

/// The header up to the tag of `supersedes`: the magic, version, lineage and first position.
const HEADER_START: usize = 4 + 4 + 16 + 8 + 1;
/// The header from the incarnation up to the tag of `claim`: five `u64` fields.
const HEADER_COUNTERS: usize = 5 * 8 + 1;

/// The buffer a [`Reader`] reads a segment through: 64 KiB.
pub const BUFFER: usize = 64 << 10;

/// A segment's header: where its records start, the identity of the stream and generation, and
/// the state carried over from the segment before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub lineage: Lineage,
    /// The position of the segment's first record, at least 1.
    pub first_position: Position,
    /// The id of the generation directory this one replaces, named by the first segment of a new
    /// generation.
    pub supersedes: Option<GenId>,
    pub cluster_id: String,
    pub stream_path: String,
    /// The stream's incarnation token.
    pub incarnation: Incarnation,
    pub base: Position,
    pub acked: Position,
    /// The stream offset just after the last acknowledged commit.
    pub acked_offset: Offset,
    /// Frame bytes logged since the latest snapshot.
    pub log_bytes: u64,
    /// The latest CLAIM, if this generation made one.
    pub claim: Option<(Epoch, Activation)>,
}

/// A record's body. A COMMIT record's frame is `F`: the frame's bytes, or what a [`Reader`]'s
/// [`Keep`] kept of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordBody<F = Vec<u8>> {
    /// A `USQ2` frame: our commit, or a stream frame caught up at open.
    Commit { frame: F },
    /// A claim about to be sent.
    Claim {
        epoch: Epoch,
        activation: Activation,
    },
    /// How far the stream acknowledged the log.
    Ack {
        acked: Position,
        acked_offset: Offset,
        log_bytes: u64,
    },
    /// The db file durably holds every commit up to `position`.
    Base { position: Position },
}

impl<F> RecordBody<F> {
    /// The body with a COMMIT record's frame mapped by `f`.
    pub fn map_frame<G>(self, f: impl FnOnce(F) -> G) -> RecordBody<G> {
        match self {
            RecordBody::Commit { frame } => RecordBody::Commit { frame: f(frame) },
            RecordBody::Claim { epoch, activation } => RecordBody::Claim { epoch, activation },
            RecordBody::Ack {
                acked,
                acked_offset,
                log_bytes,
            } => RecordBody::Ack {
                acked,
                acked_offset,
                log_bytes,
            },
            RecordBody::Base { position } => RecordBody::Base { position },
        }
    }
}

/// A valid record and where it ends in the segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<F = Vec<u8>> {
    pub position: Position,
    pub body: RecordBody<F>,
    /// The record's chain, which seeds the next record's.
    pub chain: u32,
    /// The byte offset just after the record.
    pub end: usize,
}

/// How the bytes after the valid prefix end. `at` is the byte offset where the prefix ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tail {
    /// The valid prefix is the whole segment.
    Clean,
    /// An incomplete record or one whose chain does not verify: the log ends there, and open cuts
    /// the rest.
    Torn { at: usize },
    /// A record whose chain verifies but whose position does not follow.
    Gap {
        at: usize,
        expected: Position,
        found: Position,
    },
    /// A record whose chain verifies but whose kind or payload no record has.
    Malformed { at: usize, kind: u8 },
}

/// A segment's header and the longest valid prefix of its records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scan {
    pub header: SegmentHeader,
    /// The header's checksum, which seeds the first record's chain.
    pub seed: u32,
    /// The byte offset just after the header.
    pub header_len: usize,
    pub records: Vec<Record>,
    pub tail: Tail,
}

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error(
        "segment magic \"{}\": this extension reads \"USL1\" log segments",
        .found.escape_ascii()
    )]
    Magic { found: Vec<u8> },
    #[error("segment version {found}: this extension reads version 1")]
    Version { found: u32 },
    #[error("segment header cut short: {len} bytes")]
    HeaderTruncated { len: usize },
    #[error("segment header checksum {found:#010x}, expected {expected:#010x}")]
    HeaderChecksum { expected: u32, found: u32 },
    #[error("segment header: supersedes tag {tag}")]
    SupersedesTag { tag: u8 },
    #[error("segment header: claim tag {tag}")]
    ClaimTag { tag: u8 },
    #[error("segment header: incarnation {0}")]
    Incarnation(#[source] ursula_headers::HeaderError),
    #[error("segment header: claim epoch {0}")]
    Epoch(#[source] ursula_headers::HeaderError),
    #[error("segment header: first position 0, positions start at 1")]
    FirstPositionZero,
    #[error("segment header: {field} is not UTF-8: {source}")]
    Utf8 {
        field: &'static str,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("segment header: {field} of {len} bytes (at most 65535)")]
    StringTooLong { field: &'static str, len: usize },
    #[error("a record payload of {len} bytes is too large")]
    RecordTooLarge { len: usize },
    #[error("a record after position {after:?}, the last position there is")]
    PositionOverflow { after: Position },
}

fn put_str(out: &mut Vec<u8>, field: &'static str, s: &str) -> Result<(), SegmentError> {
    let len = u16::try_from(s.len()).map_err(|_too_long| SegmentError::StringTooLong {
        field,
        len: s.len(),
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

/// The header's bytes and its checksum, which seeds the first record's chain.
pub fn encode_header(h: &SegmentHeader) -> Result<(Vec<u8>, u32), SegmentError> {
    if h.first_position.0 == 0 {
        return Err(SegmentError::FirstPositionZero);
    }
    let mut out = Vec::with_capacity(
        160usize
            .saturating_add(h.cluster_id.len())
            .saturating_add(h.stream_path.len()),
    );
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&h.lineage.0);
    out.extend_from_slice(&h.first_position.0.to_le_bytes());
    match h.supersedes {
        None => out.push(0),
        Some(id) => {
            out.push(1);
            out.extend_from_slice(&id.0);
        }
    }
    put_str(&mut out, "cluster_id", &h.cluster_id)?;
    put_str(&mut out, "stream_path", &h.stream_path)?;
    for n in [
        h.incarnation.get(),
        h.base.0,
        h.acked.0,
        h.acked_offset.get(),
        h.log_bytes,
    ] {
        out.extend_from_slice(&n.to_le_bytes());
    }
    match h.claim {
        None => out.push(0),
        Some((epoch, activation)) => {
            out.push(1);
            out.extend_from_slice(&epoch.get().to_le_bytes());
            out.extend_from_slice(&activation.0);
        }
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok((out, crc))
}

fn chain(previous: u32, kind: u8, position: Position, payload: &[u8]) -> u32 {
    let mut head = [0u8; 13];
    let (prev, rest) = head.split_at_mut(4);
    prev.copy_from_slice(&previous.to_le_bytes());
    let (k, pos) = rest.split_at_mut(1);
    k.copy_from_slice(&[kind]);
    pos.copy_from_slice(&position.0.to_le_bytes());
    crc32c::crc32c_append(crc32c::crc32c(&head), payload)
}

/// The record's bytes and its chain, given the previous record's chain (or the header's checksum
/// for the first record).
pub fn encode_record(
    previous: u32,
    position: Position,
    body: &RecordBody,
) -> Result<(Vec<u8>, u32), SegmentError> {
    let mut fixed = Vec::new();
    let (kind, payload): (u8, &[u8]) = match body {
        RecordBody::Commit { frame } => (COMMIT, frame),
        RecordBody::Claim { epoch, activation } => {
            fixed.extend_from_slice(&epoch.get().to_le_bytes());
            fixed.extend_from_slice(&activation.0);
            (CLAIM, &fixed)
        }
        RecordBody::Ack {
            acked,
            acked_offset,
            log_bytes,
        } => {
            fixed.extend_from_slice(&acked.0.to_le_bytes());
            fixed.extend_from_slice(&acked_offset.get().to_le_bytes());
            fixed.extend_from_slice(&log_bytes.to_le_bytes());
            (ACK, &fixed)
        }
        RecordBody::Base { position } => {
            fixed.extend_from_slice(&position.0.to_le_bytes());
            (BASE, &fixed)
        }
    };
    let len = u32::try_from(payload.len())
        .map_err(|_too_long| SegmentError::RecordTooLarge { len: payload.len() })?;
    let chain = chain(previous, kind, position, payload);
    let mut out = Vec::with_capacity(
        payload
            .len()
            .saturating_add(RECORD_HEAD)
            .saturating_add(RECORD_TAIL),
    );
    out.extend_from_slice(&len.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&position.0.to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&chain.to_le_bytes());
    Ok((out, chain))
}

fn take_str(t: &mut Take<'_>, field: &'static str, len: usize) -> Result<String, SegmentError> {
    let truncated = || SegmentError::HeaderTruncated { len };
    let n = t.u16().ok_or_else(truncated)?;
    let bytes = t.bytes(usize::from(n)).ok_or_else(truncated)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|source| SegmentError::Utf8 { field, source })
}

fn header(bytes: &[u8]) -> Result<(SegmentHeader, u32, usize), SegmentError> {
    let len = bytes.len();
    let truncated = || SegmentError::HeaderTruncated { len };
    let mut t = Take(bytes);
    let Some(magic) = t.array::<4>() else {
        return Err(if MAGIC.starts_with(bytes) {
            truncated()
        } else {
            SegmentError::Magic {
                found: bytes.to_vec(),
            }
        });
    };
    if &magic != MAGIC {
        return Err(SegmentError::Magic {
            found: magic.to_vec(),
        });
    }
    let version = t.u32().ok_or_else(truncated)?;
    if version != VERSION {
        return Err(SegmentError::Version { found: version });
    }
    let lineage = Lineage(t.array().ok_or_else(truncated)?);
    let first_position = Position(t.u64().ok_or_else(truncated)?);
    if first_position.0 == 0 {
        return Err(SegmentError::FirstPositionZero);
    }
    let supersedes = match t.u8().ok_or_else(truncated)? {
        0 => None,
        1 => Some(GenId(t.array().ok_or_else(truncated)?)),
        tag => return Err(SegmentError::SupersedesTag { tag }),
    };
    let cluster_id = take_str(&mut t, "cluster_id", len)?;
    let stream_path = take_str(&mut t, "stream_path", len)?;
    let mut n = || t.u64().ok_or_else(truncated);
    let (incarnation, base, acked, acked_offset, log_bytes) = (n()?, n()?, n()?, n()?, n()?);
    let claim = match t.u8().ok_or_else(truncated)? {
        0 => None,
        1 => Some((
            Epoch::try_from(t.u64().ok_or_else(truncated)?).map_err(SegmentError::Epoch)?,
            Activation(t.array().ok_or_else(truncated)?),
        )),
        tag => return Err(SegmentError::ClaimTag { tag }),
    };
    let header_end = len.saturating_sub(t.rest_len());
    let found = t.u32().ok_or_else(truncated)?;
    let expected = crc32c::crc32c(bytes.get(..header_end).unwrap_or_default());
    if found != expected {
        return Err(SegmentError::HeaderChecksum { expected, found });
    }
    let header = SegmentHeader {
        lineage,
        first_position,
        supersedes,
        cluster_id,
        stream_path,
        incarnation: Incarnation::try_from(incarnation).map_err(SegmentError::Incarnation)?,
        base: Position(base),
        acked: Position(acked),
        acked_offset: Offset::from_u64(acked_offset),
        log_bytes,
        claim,
    };
    Ok((header, found, header_end.saturating_add(4)))
}

/// The body of a record of a fixed shape (CLAIM, ACK, BASE) from its whole payload, `None` for
/// any other kind or a payload of another size.
fn fixed_body<F>(kind: u8, payload: &[u8]) -> Option<RecordBody<F>> {
    let mut t = Take(payload);
    let body = match kind {
        CLAIM => RecordBody::Claim {
            epoch: Epoch::try_from(t.u64()?).ok()?,
            activation: Activation(t.array()?),
        },
        ACK => RecordBody::Ack {
            acked: Position(t.u64()?),
            acked_offset: Offset::from_u64(t.u64()?),
            log_bytes: t.u64()?,
        },
        BASE => RecordBody::Base {
            position: Position(t.u64()?),
        },
        _ => return None,
    };
    t.rest().is_empty().then_some(body)
}

/// What a [`Reader`] keeps of a COMMIT record's payload, the frame, which reaches it piece by
/// piece as it passes through the reader's buffer.
pub trait Keep {
    /// What is kept of one frame.
    type Kept;
    /// A COMMIT record's payload of `len` bytes starts. What was kept of an earlier one goes: its
    /// record did not verify.
    fn start(&mut self, len: u32);
    /// The payload's next piece.
    fn piece(&mut self, bytes: &[u8]);
    /// The record verified: what was kept of its frame.
    fn finish(&mut self) -> Self::Kept;
}

/// Keeps a frame whole. The frame's buffer grows with the bytes that arrive, at most doubling and
/// never past the length the record declares, so a kept frame holds no more than its own bytes.
/// The declared length is never reserved up front: a torn record's length is garbage.
#[derive(Debug, Default)]
pub struct Whole {
    frame: Vec<u8>,
    /// The payload length the record declares.
    declared: usize,
}

impl Keep for Whole {
    type Kept = Vec<u8>;

    fn start(&mut self, len: u32) {
        self.frame = Vec::new();
        self.declared = usize::try_from(len).unwrap_or(usize::MAX);
    }

    fn piece(&mut self, bytes: &[u8]) {
        if self.frame.spare_capacity_mut().len() < bytes.len() {
            let filled = self.frame.len();
            let left = self.declared.saturating_sub(filled);
            self.frame.reserve_exact(left.min(filled.max(bytes.len())));
        }
        self.frame.extend_from_slice(bytes);
    }

    fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.frame)
    }
}

/// Keeps nothing of a frame.
#[derive(Debug, Default)]
pub struct Skip;

impl Keep for Skip {
    type Kept = ();

    fn start(&mut self, _len: u32) {}

    fn piece(&mut self, _bytes: &[u8]) {}

    fn finish(&mut self) -> Self::Kept {}
}

/// Where a [`Reader`] reads a segment's bytes from: a segment file, or a segment held whole.
pub trait Source {
    type Error;
    /// Reads the next bytes into `out`: how many, 0 at the end.
    fn read_some(&mut self, out: &mut [u8]) -> Result<usize, Self::Error>;
}

impl Source for File {
    type Error = std::io::Error;

    fn read_some(&mut self, out: &mut [u8]) -> Result<usize, std::io::Error> {
        loop {
            match std::io::Read::read(self, out) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                read => return read,
            }
        }
    }
}

impl Source for &[u8] {
    type Error = Infallible;

    fn read_some(&mut self, out: &mut [u8]) -> Result<usize, Infallible> {
        let bytes = std::mem::take(self);
        let n = out.len().min(bytes.len());
        let (Some((head, rest)), Some(to)) = (bytes.split_at_checked(n), out.get_mut(..n)) else {
            *self = bytes;
            return Ok(0);
        };
        to.copy_from_slice(head);
        *self = rest;
        Ok(n)
    }
}

/// A [`Reader`] failed: its source, or a header or record no segment has.
#[derive(Debug, thiserror::Error)]
pub enum ReadError<E> {
    #[error("reading the segment: {0}")]
    Source(#[source] E),
    #[error(transparent)]
    Segment(#[from] SegmentError),
}

/// A [`Source`] read through a buffer of [`BUFFER`] bytes, and how far.
struct Buffered<S> {
    source: S,
    buf: Box<[u8]>,
    /// The unread bytes of the buffer: `pos..filled`.
    pos: usize,
    filled: usize,
    /// The bytes read so far.
    offset: usize,
}

impl<S: Source> Buffered<S> {
    fn new(source: S) -> Self {
        Self {
            source,
            buf: vec![0; BUFFER].into_boxed_slice(),
            pos: 0,
            filled: 0,
            offset: 0,
        }
    }

    /// Whether any byte is left, the buffer refilled once it is used up.
    fn more(&mut self) -> Result<bool, S::Error> {
        if self.pos < self.filled {
            return Ok(true);
        }
        let n = self.source.read_some(&mut self.buf)?;
        self.pos = 0;
        self.filled = n.min(self.buf.len());
        Ok(self.filled > 0)
    }

    /// Passes the next `len` bytes to `f`, piece by piece. False when the source ends first.
    fn stream(&mut self, len: usize, mut f: impl FnMut(&[u8])) -> Result<bool, S::Error> {
        let mut left = len;
        while left > 0 {
            if !self.more()? {
                return Ok(false);
            }
            let unread = self.buf.get(self.pos..self.filled).unwrap_or_default();
            let piece = unread.get(..left).unwrap_or(unread);
            f(piece);
            let n = piece.len();
            self.pos = self.pos.saturating_add(n);
            self.offset = self.offset.saturating_add(n);
            left = left.saturating_sub(n);
        }
        Ok(true)
    }

    /// Appends the next `len` bytes to `out`. False when the source ends first, what there was
    /// appended.
    fn take(&mut self, out: &mut Vec<u8>, len: usize) -> Result<bool, S::Error> {
        self.stream(len, |piece| out.extend_from_slice(piece))
    }

    /// The next `N` bytes, `None` when the source ends first.
    fn array<const N: usize>(&mut self) -> Result<Option<[u8; N]>, S::Error> {
        let mut out = [0u8; N];
        let mut at = 0usize;
        let whole = self.stream(N, |piece| {
            let end = at.saturating_add(piece.len());
            if let Some(to) = out.get_mut(at..end) {
                to.copy_from_slice(piece);
            }
            at = end;
        })?;
        Ok(whole.then_some(out))
    }
}

/// The bytes of a segment's header, read field by field as far as its lengths and tags say and no
/// further, so the records follow. It stops early at the end of the source or at a tag no header
/// has, and [`header`] then says what is wrong, as it does for the header of a segment held whole.
fn header_bytes<S: Source>(b: &mut Buffered<S>) -> Result<Vec<u8>, S::Error> {
    let mut head = Vec::new();
    if !b.take(&mut head, HEADER_START)? {
        return Ok(head);
    }
    let supersedes = match head.last().copied() {
        Some(0) => 0,
        Some(1) => 16,
        _ => return Ok(head),
    };
    if !b.take(&mut head, supersedes)? {
        return Ok(head);
    }
    // The cluster id and the stream path.
    for _ in 0..2 {
        if !b.take(&mut head, 2)? {
            return Ok(head);
        }
        let len = head.last_chunk::<2>().map_or(0, |n| u16::from_le_bytes(*n));
        if !b.take(&mut head, usize::from(len))? {
            return Ok(head);
        }
    }
    if !b.take(&mut head, HEADER_COUNTERS)? {
        return Ok(head);
    }
    let claim = match head.last().copied() {
        Some(0) => 0,
        // An epoch and an activation.
        Some(1) => 24,
        _ => return Ok(head),
    };
    if !b.take(&mut head, claim)? {
        return Ok(head);
    }
    // The checksum.
    b.take(&mut head, 4)?;
    Ok(head)
}

/// What [`Reader::next`] read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step<F> {
    /// The next valid record.
    Record(Record<F>),
    /// The valid prefix ends: how the rest ends.
    End(Tail),
}

/// Reads a segment: its header, then its records one at a time, each payload through a buffer of
/// [`BUFFER`] bytes. It reads what [`scan`] reads of a segment held whole: the same header, the
/// same valid prefix and the same [`Tail`], and fails where `scan` fails.
pub struct Reader<S> {
    bytes: Buffered<S>,
    header: SegmentHeader,
    seed: u32,
    header_len: usize,
    /// The last valid record's chain, the header's checksum before the first.
    previous: u32,
    /// The position the next record must have, `None` after the last position there is.
    expected: Option<Position>,
    /// The last valid record's position.
    last: Option<Position>,
    /// How the valid prefix ended, once it did.
    tail: Option<Tail>,
}

impl<S: Source> Reader<S> {
    /// Reads the segment's header. One cut short, damaged or of another format is an error: the
    /// records cannot be trusted without it.
    pub fn new(source: S) -> Result<Self, ReadError<S::Error>> {
        let mut bytes = Buffered::new(source);
        let head = header_bytes(&mut bytes).map_err(ReadError::Source)?;
        let (header, seed, header_len) = header(&head)?;
        let expected = Some(header.first_position);
        Ok(Self {
            bytes,
            header,
            seed,
            header_len,
            previous: seed,
            expected,
            last: None,
            tail: None,
        })
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn into_header(self) -> SegmentHeader {
        self.header
    }

    /// The header's checksum, which seeds the first record's chain.
    pub fn seed(&self) -> u32 {
        self.seed
    }

    /// The byte offset just after the header.
    pub fn header_len(&self) -> usize {
        self.header_len
    }

    /// The next record of the valid prefix, or how the prefix ends (again on every later call). A
    /// COMMIT record's frame is what `keep` keeps of it. An error ends the read: the reader may
    /// have stopped inside a record, so a caller drops it.
    pub fn next<K: Keep>(&mut self, keep: &mut K) -> Result<Step<K::Kept>, ReadError<S::Error>> {
        if let Some(tail) = self.tail {
            return Ok(Step::End(tail));
        }
        let at = self.bytes.offset;
        if !self.bytes.more().map_err(ReadError::Source)? {
            return Ok(self.end(Tail::Clean));
        }
        let Some(head) = self
            .bytes
            .array::<RECORD_HEAD>()
            .map_err(ReadError::Source)?
        else {
            return Ok(self.end(Tail::Torn { at }));
        };
        let mut t = Take(&head);
        let (Some(len), Some(kind), Some(position)) = (t.u32(), t.u8(), t.u64().map(Position))
        else {
            return Ok(self.end(Tail::Torn { at }));
        };
        // The chain covers the previous chain, the kind, the position and the payload.
        let mut chain = crc32c::crc32c(&self.previous.to_le_bytes());
        chain = crc32c::crc32c_append(chain, head.get(4..).unwrap_or_default());
        let mut fixed = [0u8; FIXED_PAYLOAD];
        let mut fixed_len = 0usize;
        if kind == COMMIT {
            keep.start(len);
        }
        let whole = self
            .bytes
            .stream(usize::try_from(len).unwrap_or(usize::MAX), |piece| {
                chain = crc32c::crc32c_append(chain, piece);
                if kind == COMMIT {
                    keep.piece(piece);
                } else if let Some(to) = fixed.get_mut(fixed_len..) {
                    let n = to.len().min(piece.len());
                    if let (Some(to), Some(from)) = (to.get_mut(..n), piece.get(..n)) {
                        to.copy_from_slice(from);
                    }
                    fixed_len = fixed_len.saturating_add(n);
                }
            })
            .map_err(ReadError::Source)?;
        if !whole {
            return Ok(self.end(Tail::Torn { at }));
        }
        let Some(found) = self
            .bytes
            .array::<RECORD_TAIL>()
            .map_err(ReadError::Source)?
        else {
            return Ok(self.end(Tail::Torn { at }));
        };
        if u32::from_le_bytes(found) != chain {
            return Ok(self.end(Tail::Torn { at }));
        }
        let Some(want) = self.expected else {
            return Err(SegmentError::PositionOverflow {
                after: self.last.unwrap_or(self.header.first_position),
            }
            .into());
        };
        if position != want {
            return Ok(self.end(Tail::Gap {
                at,
                expected: want,
                found: position,
            }));
        }
        let body = match kind {
            COMMIT if len > 0 => Some(RecordBody::Commit {
                frame: keep.finish(),
            }),
            COMMIT => None,
            // Every fixed shape's payload fits: a longer one is none.
            _ => usize::try_from(len)
                .ok()
                .filter(|&len| len <= fixed_len)
                .and_then(|len| fixed.get(..len))
                .and_then(|payload| fixed_body(kind, payload)),
        };
        let Some(body) = body else {
            return Ok(self.end(Tail::Malformed { at, kind }));
        };
        self.previous = chain;
        self.expected = position.0.checked_add(1).map(Position);
        self.last = Some(position);
        Ok(Step::Record(Record {
            position,
            body,
            chain,
            end: self.bytes.offset,
        }))
    }

    fn end<F>(&mut self, tail: Tail) -> Step<F> {
        self.tail = Some(tail);
        Step::End(tail)
    }
}

fn infallible(e: ReadError<Infallible>) -> SegmentError {
    match e {
        ReadError::Source(never) => match never {},
        ReadError::Segment(e) => e,
    }
}

/// The segment's header and its longest valid prefix of records, read by a [`Reader`]. A header
/// that is cut short, damaged or of another format is an error: the records cannot be trusted
/// without it.
pub fn scan(bytes: &[u8]) -> Result<Scan, SegmentError> {
    let mut reader = Reader::new(bytes).map_err(infallible)?;
    let mut whole = Whole::default();
    let mut records = Vec::new();
    let tail = loop {
        match reader.next(&mut whole).map_err(infallible)? {
            Step::Record(r) => records.push(r),
            Step::End(tail) => break tail,
        }
    };
    let (seed, header_len) = (reader.seed(), reader.header_len());
    Ok(Scan {
        header: reader.into_header(),
        seed,
        header_len,
        records,
        tail,
    })
}

#[cfg(test)]
mod tests;
