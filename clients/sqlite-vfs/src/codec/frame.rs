//! `USQ2` stream frames. A byte stream has no message boundaries on read, so every frame delimits
//! itself:
//!
//! ```text
//! frame  = "USQ2" | u8 kind | u32 body_len | u32 crc32c(kind | body) | body
//! kind 1 CLAIM   body = u64 epoch | [16] activation
//! kind 2 COMMIT  body = [16] lineage | u64 position | zstd(record)
//! kind 3 PART    body = [16] lineage | u64 position | u32 index | u32 count | chunk
//! record = u32 db_pages_after | u32 n | n x (u32 pgno | [4096] image)      (pgno ascending)
//! ```
//!
//! The checksum covers the kind with the body, so no damaged byte of a frame turns it into another
//! valid frame. The commit id `(lineage, position)` is in the clear, so a reader matches commits
//! without decompressing them. A commit too large for one append goes out as a run of PART frames
//! `0..count`, consecutive slices of the COMMIT body's compressed record, and [`PartRun`] puts the
//! run back together. Frames carry no epoch, sequence or stream identity, so a resend under any
//! epoch sends the bytes logged at commit time.

use std::io::Read;
use std::io::Write;

use ursula_headers::Epoch;

use super::PAGE;
use super::Take;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;

/// The stream's content type.
pub const CONTENT_TYPE: &str = "application/vnd.ursula.sqlite-vfs.v2";

/// The activation of every claim `ursula_import` makes, and of no other claim: an open refuses an
/// `activation` option equal to it, and a random activation equals it with probability 2^-128.
/// The stream holds the import's snapshot only once its publish completes, so a read of the
/// stream that meets this claim (a build or catch-up from offset 0, a reconcile) has met an import
/// in progress, or one that failed: it refuses `stream_state` and never builds from it
/// ([`crate::codec::stream::FrameStream`]). Frozen for 0.8.x (`codec/vectors`).
pub const IMPORT_ACTIVATION: Activation = Activation(*b"ursula-import-v1");

/// Whether `frame` is the claim of an import.
pub fn is_import_claim(frame: &Frame) -> bool {
    matches!(frame, Frame::Claim { activation, .. } if *activation == IMPORT_ACTIVATION)
}

const MAGIC: &[u8; 4] = b"USQ2";
/// Magic, kind, body length and checksum.
const HEADER: usize = 13;
const ZSTD_LEVEL: i32 = 3;
/// The largest zstd window a reader accepts (4 MiB): a frame asking for more is refused, so a
/// damaged or foreign frame cannot make the decoder allocate a large window.
pub(crate) const ZSTD_WINDOW_LOG_MAX: u32 = 22;

const CLAIM: u8 = 1;
const COMMIT: u8 = 2;
const PART: u8 = 3;

/// A CLAIM body: the epoch and the activation.
const CLAIM_BODY: u32 = 24;
/// The commit id that opens COMMIT and PART bodies.
const ID: usize = 24;
/// A PART body before its chunk: the commit id, the index and the count.
const PART_HEAD: usize = 32;
/// The bytes of a PART frame beside its chunk: the frame header and the PART head.
pub const PART_OVERHEAD: usize = HEADER + PART_HEAD;
/// The longest compressed record a COMMIT frame holds: its body length is a u32.
const MAX_COMPRESSED: u64 = (u32::MAX as u64).saturating_sub(ID as u64);
/// A commit record before its pages: the db size after the commit and the page count.
const RECORD_HEAD: usize = 8;
/// A commit record's page entry: the page number and the image.
const ENTRY: usize = 4 + PAGE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// An owner's claim of the stream at `epoch`.
    Claim {
        epoch: Epoch,
        activation: Activation,
    },
    /// A commit: its id and its zstd-compressed [`CommitRecord`] (see [`decompress`]).
    Commit { id: CommitId, compressed: Vec<u8> },
    /// Slice `index` of `count` of a commit's compressed record.
    Part {
        id: CommitId,
        index: u32,
        count: u32,
        chunk: Vec<u8>,
    },
}

/// What a commit changed: the database size after it, in pages, and the final image of every page
/// it wrote, by ascending page number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub db_pages_after: u32,
    pub pages: Vec<(u32, Box<[u8; PAGE]>)>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A whole frame of `len` bytes.
    Frame { frame: Frame, len: usize },
    /// The buffer ends inside a frame (or is empty).
    Partial,
}

/// A frame or commit record that cannot be encoded or decoded.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// Not a frame this extension reads: a frame of another minor version's format (an
    /// incompatible change bumps the magic), or not a frame at all.
    #[error(
        "frame magic \"{}\": this extension reads \"USQ2\" frames, so these bytes are a frame \
         of another minor version's extension, were not written by the extension, or are not the \
         start of a frame",
        .found.escape_ascii()
    )]
    Magic { found: Vec<u8> },
    #[error("frame kind {kind} is not a claim (1), a commit (2) or a part (3)")]
    UnknownKind { kind: u8 },
    #[error("frame kind {kind}: a body of {len} bytes")]
    BodyLength { kind: u8, len: u64 },
    #[error("claim frame: epoch {0}")]
    Epoch(#[source] ursula_headers::HeaderError),
    #[error("frame checksum {found:#010x}, expected {expected:#010x}")]
    Checksum { expected: u32, found: u32 },
    #[error("part {index} of {count}: a run has at least two parts, each below the count")]
    PartIndex { index: u32, count: u32 },
    #[error("a frame of kind {kind} cut into parts: only a COMMIT frame is")]
    NotCommit { kind: u8 },
    #[error("a compressed record of {len} bytes is not {count} parts of at most {chunk} bytes")]
    PartChunk { len: u64, count: u32, chunk: u64 },
    #[error("a frame body or commit record of {len} bytes is too large")]
    TooLarge { len: u64 },
    #[error(
        "a run of {count} parts of {len} bytes or more, longer than the {max} bytes of compressed \
         record a COMMIT frame holds"
    )]
    RunTooLong { count: u32, len: u64, max: u64 },
    #[error("a commit record of {pages} pages, above the 2^32 - 1 a record holds")]
    TooManyPages { pages: u64 },
    #[error("a frame body of {len} bytes, above the limit of {max}")]
    BodyTooLarge { len: u64, max: u64 },
    #[error("commit record: page {pgno} after page {previous}, pages go in ascending order")]
    PageOrder { pgno: u32, previous: u32 },
    #[error("commit record: page {pgno} outside a database of {db_pages_after} pages")]
    PageOutside { pgno: u32, db_pages_after: u32 },
    #[error(
        "commit record of {pages} pages ({expected} bytes), but its zstd frame declares {declared}"
    )]
    ContentSize {
        pages: u32,
        expected: u64,
        declared: u64,
    },
    #[error("commit record of {pages} pages: the record ends before its {expected} bytes")]
    RecordShort { pages: u32, expected: u64 },
    #[error("commit record of {pages} pages: bytes follow its {expected} bytes")]
    RecordLong { pages: u32, expected: u64 },
    #[error("{len} bytes: {source}")]
    Alloc {
        len: usize,
        #[source]
        source: std::collections::TryReserveError,
    },
    #[error("zstd compress: {0}")]
    Compress(#[source] std::io::Error),
    #[error("zstd decompress: {0}")]
    Decompress(#[source] std::io::Error),
}

fn checksum(kind: u8, body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&[kind]), body)
}

/// The frame of `kind` around `body`, whose length is `len`.
fn assemble(kind: u8, len: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len().saturating_add(HEADER));
    out.extend_from_slice(MAGIC);
    out.push(kind);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&checksum(kind, body).to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn wrap(kind: u8, body: &[u8]) -> Result<Vec<u8>, FrameError> {
    let len = u32::try_from(body.len()).map_err(|_too_long| FrameError::TooLarge {
        len: body.len() as u64,
    })?;
    Ok(assemble(kind, len, body))
}

fn put_id(body: &mut Vec<u8>, id: CommitId) {
    body.extend_from_slice(&id.lineage.0);
    body.extend_from_slice(&id.position.0.to_le_bytes());
}

fn check_part(index: u32, count: u32) -> Result<(), FrameError> {
    if count < 2 || index >= count {
        return Err(FrameError::PartIndex { index, count });
    }
    Ok(())
}

pub fn encode_claim(epoch: Epoch, activation: Activation) -> Vec<u8> {
    let mut body = Vec::with_capacity(CLAIM_BODY as usize);
    body.extend_from_slice(&epoch.get().to_le_bytes());
    body.extend_from_slice(&activation.0);
    assemble(CLAIM, CLAIM_BODY, &body)
}

/// The frame of any kind. Refuses what [`decode`] would refuse: a commit or part without bytes, or
/// a part outside its run.
pub fn encode(frame: &Frame) -> Result<Vec<u8>, FrameError> {
    match frame {
        Frame::Claim { epoch, activation } => Ok(encode_claim(*epoch, *activation)),
        Frame::Commit { id, compressed } => {
            if compressed.is_empty() {
                return Err(FrameError::BodyLength {
                    kind: COMMIT,
                    len: ID as u64,
                });
            }
            let mut body = Vec::with_capacity(compressed.len().saturating_add(ID));
            put_id(&mut body, *id);
            body.extend_from_slice(compressed);
            wrap(COMMIT, &body)
        }
        Frame::Part {
            id,
            index,
            count,
            chunk,
        } => {
            check_part(*index, *count)?;
            if chunk.is_empty() {
                return Err(FrameError::BodyLength {
                    kind: PART,
                    len: PART_HEAD as u64,
                });
            }
            let mut body = Vec::with_capacity(chunk.len().saturating_add(PART_HEAD));
            put_id(&mut body, *id);
            body.extend_from_slice(&index.to_le_bytes());
            body.extend_from_slice(&count.to_le_bytes());
            body.extend_from_slice(chunk);
            wrap(PART, &body)
        }
    }
}

/// The commit id and the compressed record of the COMMIT frame `commit`, checked to be one
/// whole COMMIT frame. Its checksum is not verified: the frame comes from our own log, which
/// verified it on read.
fn commit_slices(commit: &[u8]) -> Result<(&[u8], &[u8]), FrameError> {
    let (head, body) = commit
        .split_at_checked(HEADER)
        .ok_or(FrameError::BodyLength {
            kind: COMMIT,
            len: commit.len() as u64,
        })?;
    let mut t = Take(head);
    let magic = t.bytes(MAGIC.len()).unwrap_or_default();
    if magic != MAGIC {
        return Err(FrameError::Magic {
            found: magic.to_vec(),
        });
    }
    let kind = t.u8().unwrap_or_default();
    if kind != COMMIT {
        return Err(FrameError::NotCommit { kind });
    }
    let len = t.u32().unwrap_or_default();
    if len as usize != body.len() || body.len() <= ID {
        return Err(FrameError::BodyLength {
            kind,
            len: body.len() as u64,
        });
    }
    Ok(body.split_at(ID))
}

/// How many parts of at most `chunk` bytes of its compressed record the COMMIT frame `commit`
/// cuts into.
pub fn part_count(commit: &[u8], chunk: usize) -> Result<u32, FrameError> {
    let (_, compressed) = commit_slices(commit)?;
    let refused = || FrameError::PartChunk {
        len: compressed.len() as u64,
        count: 0,
        chunk: chunk as u64,
    };
    if chunk == 0 {
        return Err(refused());
    }
    u32::try_from(compressed.len().div_ceil(chunk)).map_err(|_too_many| refused())
}

/// PART `index` of `count` of the COMMIT frame `commit`: the `index`-th slice of `chunk` bytes of
/// its compressed record (the last one shorter), under the same commit id. `count` must be the
/// [`part_count`] for `chunk`, so the run covers the record exactly. Pure: the same arguments give
/// the same bytes, which is what keeps a part's body fixed across resends.
pub fn part_frame(
    commit: &[u8],
    index: u32,
    count: u32,
    chunk: usize,
) -> Result<Vec<u8>, FrameError> {
    check_part(index, count)?;
    let (id, compressed) = commit_slices(commit)?;
    let wrong = || FrameError::PartChunk {
        len: compressed.len() as u64,
        count,
        chunk: chunk as u64,
    };
    if part_count(commit, chunk)? != count {
        return Err(wrong());
    }
    let start = (index as usize).checked_mul(chunk).ok_or_else(wrong)?;
    let end = start.saturating_add(chunk).min(compressed.len());
    let slice = compressed.get(start..end).ok_or_else(wrong)?;
    let mut body = Vec::with_capacity(slice.len().saturating_add(PART_HEAD));
    body.extend_from_slice(id);
    body.extend_from_slice(&index.to_le_bytes());
    body.extend_from_slice(&count.to_le_bytes());
    body.extend_from_slice(slice);
    wrap(PART, &body)
}

/// The index of the PART frame that `bytes` start with, or `None` when they do not start with
/// one.
pub fn part_index(bytes: &[u8]) -> Option<u32> {
    let mut t = Take(bytes);
    if t.bytes(MAGIC.len())? != MAGIC || t.u8()? != PART {
        return None;
    }
    t.u32()?;
    t.u32()?;
    t.bytes(ID)?;
    t.u32()
}

/// The record's length in bytes for `pages` pages: at most 8 + (2^32 - 1) x 4100, so it never
/// saturates.
fn record_len(pages: u32) -> u64 {
    u64::from(pages)
        .saturating_mul(ENTRY as u64)
        .saturating_add(RECORD_HEAD as u64)
}

/// The COMMIT frame of `record`. Pages must ascend and lie within the database. The zstd frame
/// declares its content size and carries a content checksum.
pub fn encode_commit(id: CommitId, record: &CommitRecord) -> Result<Vec<u8>, FrameError> {
    let n = record.pages.len();
    let count =
        u32::try_from(n).map_err(|_too_many| FrameError::TooManyPages { pages: n as u64 })?;
    let size = record_len(count);
    let mut z =
        zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL).map_err(FrameError::Compress)?;
    z.include_checksum(true).map_err(FrameError::Compress)?;
    z.include_contentsize(true).map_err(FrameError::Compress)?;
    z.set_pledged_src_size(Some(size))
        .map_err(FrameError::Compress)?;
    let mut write = |bytes: &[u8]| z.write_all(bytes).map_err(FrameError::Compress);
    write(&record.db_pages_after.to_le_bytes())?;
    write(&count.to_le_bytes())?;
    let mut previous = 0;
    for (pgno, image) in &record.pages {
        check_page(*pgno, previous, record.db_pages_after)?;
        previous = *pgno;
        write(&pgno.to_le_bytes())?;
        write(image.as_slice())?;
    }
    let compressed = z.finish().map_err(FrameError::Compress)?;
    encode(&Frame::Commit { id, compressed })
}

/// Page `pgno` follows page `previous` (0 for none) in a database of `db_pages_after` pages.
fn check_page(pgno: u32, previous: u32, db_pages_after: u32) -> Result<(), FrameError> {
    if pgno == 0 || pgno > db_pages_after {
        return Err(FrameError::PageOutside {
            pgno,
            db_pages_after,
        });
    }
    if pgno <= previous {
        return Err(FrameError::PageOrder { pgno, previous });
    }
    Ok(())
}

/// Decodes the frame at the start of `buf`. A kind or length no frame can have, or a body above
/// `max_body` bytes, is an error as soon as it is read, so a reader never waits for the body of a
/// damaged header.
pub fn decode(buf: &[u8], max_body: u64) -> Result<Decoded, FrameError> {
    let Some((magic, rest)) = buf.split_first_chunk::<4>() else {
        // Shorter than the magic: what there is must be its start.
        return if MAGIC.starts_with(buf) {
            Ok(Decoded::Partial)
        } else {
            Err(FrameError::Magic {
                found: buf.to_vec(),
            })
        };
    };
    if magic != MAGIC {
        return Err(FrameError::Magic {
            found: magic.to_vec(),
        });
    }
    let mut t = Take(rest);
    let Some(kind) = t.u8() else {
        return Ok(Decoded::Partial);
    };
    if !matches!(kind, CLAIM | COMMIT | PART) {
        return Err(FrameError::UnknownKind { kind });
    }
    let Some(len) = t.u32() else {
        return Ok(Decoded::Partial);
    };
    let fits = match kind {
        CLAIM => len == CLAIM_BODY,
        COMMIT => len as usize > ID,
        _ => len as usize > PART_HEAD,
    };
    if !fits {
        return Err(FrameError::BodyLength {
            kind,
            len: u64::from(len),
        });
    }
    if u64::from(len) > max_body {
        return Err(FrameError::BodyTooLarge {
            len: u64::from(len),
            max: max_body,
        });
    }
    let Some(crc) = t.u32() else {
        return Ok(Decoded::Partial);
    };
    let Some(body) = t.bytes(len as usize) else {
        return Ok(Decoded::Partial);
    };
    let expected = checksum(kind, body);
    if expected != crc {
        return Err(FrameError::Checksum {
            expected,
            found: crc,
        });
    }
    Ok(Decoded::Frame {
        frame: body_of(kind, body)?,
        // The body is within `buf`: no overflow.
        len: HEADER.saturating_add(body.len()),
    })
}

/// The frame of `kind` from a checksummed body whose length suits the kind.
fn body_of(kind: u8, body: &[u8]) -> Result<Frame, FrameError> {
    let short = || FrameError::BodyLength {
        kind,
        len: body.len() as u64,
    };
    let mut t = Take(body);
    if kind == CLAIM {
        let epoch = t.u64().ok_or_else(short)?;
        let activation = t.array().ok_or_else(short)?;
        return Ok(Frame::Claim {
            epoch: Epoch::try_from(epoch).map_err(FrameError::Epoch)?,
            activation: Activation(activation),
        });
    }
    let lineage = t.array().ok_or_else(short)?;
    let position = t.u64().ok_or_else(short)?;
    let id = CommitId {
        lineage: Lineage(lineage),
        position: Position(position),
    };
    if kind == COMMIT {
        return Ok(Frame::Commit {
            id,
            compressed: t.rest().to_vec(),
        });
    }
    let index = t.u32().ok_or_else(short)?;
    let count = t.u32().ok_or_else(short)?;
    check_part(index, count)?;
    Ok(Frame::Part {
        id,
        index,
        count,
        chunk: t.rest().to_vec(),
    })
}

/// The header of a commit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub db_pages_after: u32,
    pub pages: u32,
}

/// Reads a commit record from a COMMIT frame's compressed bytes one page at a time, so no buffer
/// is sized from the bytes and no more than a page is held. The record header must agree with the
/// zstd frame's declared content size when it declares one, pages must ascend within the database,
/// and the record must end exactly after its last page, where zstd checks the content checksum.
pub struct RecordReader<'a> {
    z: zstd::stream::read::Decoder<'static, std::io::BufReader<&'a [u8]>>,
    header: RecordHeader,
    left: u32,
    previous: u32,
}

impl<'a> RecordReader<'a> {
    pub fn new(compressed: &'a [u8]) -> Result<Self, FrameError> {
        let mut z = zstd::stream::read::Decoder::new(compressed).map_err(FrameError::Decompress)?;
        z.window_log_max(ZSTD_WINDOW_LOG_MAX)
            .map_err(FrameError::Decompress)?;
        let head_short = FrameError::RecordShort {
            pages: 0,
            expected: RECORD_HEAD as u64,
        };
        let mut head = [0u8; RECORD_HEAD];
        if let Err(e) = z.read_exact(&mut head) {
            return Err(match e.kind() {
                std::io::ErrorKind::UnexpectedEof => head_short,
                _ => FrameError::Decompress(e),
            });
        }
        let mut t = Take(&head);
        let (Some(db_pages_after), Some(pages)) = (t.u32(), t.u32()) else {
            return Err(head_short);
        };
        let expected = record_len(pages);
        if let Ok(Some(declared)) = zstd::zstd_safe::get_frame_content_size(compressed)
            && declared != expected
        {
            return Err(FrameError::ContentSize {
                pages,
                expected,
                declared,
            });
        }
        Ok(Self {
            z,
            header: RecordHeader {
                db_pages_after,
                pages,
            },
            left: pages,
            previous: 0,
        })
    }

    pub fn header(&self) -> RecordHeader {
        self.header
    }

    fn expected(&self) -> u64 {
        record_len(self.header.pages)
    }

    /// The next page's number, its image written to `image`, or `None` after the last page.
    pub fn next_page(&mut self, image: &mut [u8; PAGE]) -> Result<Option<u32>, FrameError> {
        let Some(left) = self.left.checked_sub(1) else {
            self.finish()?;
            return Ok(None);
        };
        let mut pgno = [0u8; 4];
        for buf in [&mut pgno[..], &mut image[..]] {
            if let Err(e) = self.z.read_exact(buf) {
                return Err(match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => FrameError::RecordShort {
                        pages: self.header.pages,
                        expected: self.expected(),
                    },
                    _ => FrameError::Decompress(e),
                });
            }
        }
        let pgno = u32::from_le_bytes(pgno);
        check_page(pgno, self.previous, self.header.db_pages_after)?;
        self.previous = pgno;
        self.left = left;
        Ok(Some(pgno))
    }

    /// Checks that the record ends after its last page.
    fn finish(&mut self) -> Result<(), FrameError> {
        let mut extra = [0u8; 1];
        loop {
            return match self.z.read(&mut extra) {
                Ok(0) => Ok(()),
                Ok(_) => Err(FrameError::RecordLong {
                    pages: self.header.pages,
                    expected: self.expected(),
                }),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => Err(FrameError::Decompress(e)),
            };
        }
    }
}

/// The whole commit record in memory, for tests and tooling only. Production applies records
/// page by page through [`RecordReader`], so it never holds a whole record. Pages are allocated
/// as they are read, never from a declared size.
pub fn decompress(compressed: &[u8]) -> Result<CommitRecord, FrameError> {
    let mut reader = RecordReader::new(compressed)?;
    let mut pages = Vec::new();
    let mut image = Box::new([0u8; PAGE]);
    while let Some(pgno) = reader.next_page(&mut image)? {
        pages.push((pgno, image.clone()));
    }
    Ok(CommitRecord {
        db_pages_after: reader.header().db_pages_after,
        pages,
    })
}

/// Puts PART runs back together. Every frame read from the stream goes through [`PartRun::push`]
/// in stream order: a complete run `0..count` of one commit comes out as that COMMIT, and any
/// other frame inside a run voids it (its owner died mid-commit and resends the commit whole). A
/// run longer than any COMMIT frame holds is refused at its first part, where every part but the
/// last is as long as part 0, before anything is joined.
#[derive(Debug, Clone, Default)]
pub struct PartRun {
    run: Option<Run>,
}

#[derive(Debug, Clone)]
struct Run {
    id: CommitId,
    count: u32,
    next: u32,
    compressed: Vec<u8>,
}

impl PartRun {
    /// The frame to apply, if any: a CLAIM or COMMIT as it came, or the COMMIT a run completes. A
    /// part that does not continue the current run voids it and starts a new run at index 0, or is
    /// dropped as the rest of a voided run.
    pub fn push(&mut self, frame: Frame) -> Result<Option<Frame>, FrameError> {
        let Frame::Part {
            id,
            index,
            count,
            chunk,
        } = frame
        else {
            self.run = None;
            return Ok(Some(frame));
        };
        check_part(index, count)?;
        let continues = self
            .run
            .as_ref()
            .is_some_and(|r| r.id == id && r.count == count && r.next == index);
        let mut run = match self.run.take() {
            Some(run) if continues => run,
            _ if index == 0 => {
                run_fits(count, chunk.len())?;
                Run {
                    id,
                    count,
                    next: 0,
                    compressed: Vec::new(),
                }
            }
            _ => return Ok(None),
        };
        // The bytes joined so far with this part's.
        let joined = run.compressed.len().saturating_add(chunk.len());
        if joined as u64 > MAX_COMPRESSED {
            return Err(FrameError::RunTooLong {
                count,
                len: joined as u64,
                max: MAX_COMPRESSED,
            });
        }
        run.compressed
            .try_reserve(chunk.len())
            .map_err(|source| FrameError::Alloc {
                len: run.compressed.len().saturating_add(chunk.len()),
                source,
            })?;
        run.compressed.extend_from_slice(&chunk);
        // `index < count`, so this is at most `count`.
        run.next = index.saturating_add(1);
        if run.next < count {
            self.run = Some(run);
            return Ok(None);
        }
        Ok(Some(Frame::Commit {
            id: run.id,
            compressed: run.compressed,
        }))
    }
}

/// Refuses a run of `count` parts whose part 0 is `len` bytes when no COMMIT frame holds its
/// shortest record: every part but the last as long as part 0, the last a byte.
fn run_fits(count: u32, len: usize) -> Result<(), FrameError> {
    let full = u64::from(count.saturating_sub(1));
    let least = (len as u64).saturating_mul(full).saturating_add(1);
    if least > MAX_COMPRESSED {
        return Err(FrameError::RunTooLong {
            count,
            len: least,
            max: MAX_COMPRESSED,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
