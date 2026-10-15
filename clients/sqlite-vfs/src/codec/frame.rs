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

use super::PAGE;
use super::Take;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Epoch;
use crate::ids::Lineage;
use crate::ids::Position;

/// The stream's content type.
pub const CONTENT_TYPE: &str = "application/vnd.ursula.sqlite-vfs.v2";

const MAGIC: &[u8; 4] = b"USQ2";
/// Magic, kind, body length and checksum.
const HEADER: usize = 13;
const ZSTD_LEVEL: i32 = 3;

const CLAIM: u8 = 1;
const COMMIT: u8 = 2;
const PART: u8 = 3;

/// A CLAIM body: the epoch and the activation.
const CLAIM_BODY: u32 = 24;
/// The commit id that opens COMMIT and PART bodies.
const ID: usize = 24;
/// A PART body before its chunk: the commit id, the index and the count.
const PART_HEAD: usize = 32;
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
    #[error("frame checksum {found:#010x}, expected {expected:#010x}")]
    Checksum { expected: u32, found: u32 },
    #[error("part {index} of {count}: a run has at least two parts, each below the count")]
    PartIndex { index: u32, count: u32 },
    #[error("a frame body or commit record of {len} bytes is too large")]
    TooLarge { len: usize },
    #[error("commit record: page {pgno} after page {previous}, pages go in ascending order")]
    PageOrder { pgno: u32, previous: u32 },
    #[error("commit record: page {pgno} outside a database of {db_pages_after} pages")]
    PageOutside { pgno: u32, db_pages_after: u32 },
    #[error("commit record: {found:?} bytes declared by its zstd frame, not a whole record")]
    ContentSize { found: Option<u64> },
    #[error("commit record: {bytes} bytes for {pages} pages")]
    PageCount { pages: u32, bytes: usize },
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
    let len =
        u32::try_from(body.len()).map_err(|_too_long| FrameError::TooLarge { len: body.len() })?;
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
    body.extend_from_slice(&epoch.0.to_le_bytes());
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

/// The COMMIT frame of `record`. Pages must ascend and lie within the database.
pub fn encode_commit(id: CommitId, record: &CommitRecord) -> Result<Vec<u8>, FrameError> {
    let n = record.pages.len();
    let too_large = || FrameError::TooLarge { len: n };
    let count = u32::try_from(n).map_err(|_too_many| too_large())?;
    let size = n
        .checked_mul(ENTRY)
        .and_then(|b| b.checked_add(RECORD_HEAD))
        .ok_or_else(too_large)?;
    let mut raw = Vec::new();
    raw.try_reserve_exact(size)
        .map_err(|source| FrameError::Alloc { len: size, source })?;
    raw.extend_from_slice(&record.db_pages_after.to_le_bytes());
    raw.extend_from_slice(&count.to_le_bytes());
    let mut previous = 0;
    for (pgno, image) in &record.pages {
        check_page(*pgno, previous, record.db_pages_after)?;
        previous = *pgno;
        raw.extend_from_slice(&pgno.to_le_bytes());
        raw.extend_from_slice(image.as_slice());
    }
    let compressed = zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(FrameError::Compress)?;
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

/// Decodes the frame at the start of `buf`. A kind or length no frame can have is an error as soon
/// as it is read, so a reader never waits for the body of a damaged header.
pub fn decode(buf: &[u8]) -> Result<Decoded, FrameError> {
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
            epoch: Epoch(epoch),
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

/// The commit record in a COMMIT frame's compressed bytes. The record is sized from its zstd
/// frame's declared content size, which must be a whole record, and allocated fallibly, so damaged
/// bytes are an error rather than an abort.
pub fn decompress(compressed: &[u8]) -> Result<CommitRecord, FrameError> {
    let found = match zstd::zstd_safe::get_frame_content_size(compressed) {
        Ok(Some(n)) => n,
        Ok(None) | Err(_) => return Err(FrameError::ContentSize { found: None }),
    };
    let whole = usize::try_from(found).ok().filter(|&n| {
        n.checked_sub(RECORD_HEAD)
            .is_some_and(|pages| pages.is_multiple_of(ENTRY))
    });
    let Some(len) = whole else {
        return Err(FrameError::ContentSize { found: Some(found) });
    };
    let mut raw = Vec::new();
    raw.try_reserve_exact(len)
        .map_err(|source| FrameError::Alloc { len, source })?;
    raw.resize(len, 0); // within the reserved capacity
    let n = zstd::bulk::decompress_to_buffer(compressed, raw.as_mut_slice())
        .map_err(FrameError::Decompress)?;
    if n != len {
        return Err(FrameError::ContentSize {
            found: Some(n as u64),
        });
    }
    let mut t = Take(&raw);
    let (Some(db_pages_after), Some(count)) = (t.u32(), t.u32()) else {
        return Err(FrameError::ContentSize { found: Some(found) });
    };
    let entries = t.rest();
    if (count as usize).checked_mul(ENTRY) != Some(entries.len()) {
        return Err(FrameError::PageCount {
            pages: count,
            bytes: entries.len(),
        });
    }
    let mut pages = Vec::new();
    pages
        .try_reserve_exact(count as usize)
        .map_err(|source| FrameError::Alloc {
            len: count as usize,
            source,
        })?;
    let mut previous = 0;
    for entry in entries.chunks_exact(ENTRY) {
        let mut e = Take(entry);
        let (Some(pgno), Some(image)) = (e.u32(), e.array::<PAGE>()) else {
            return Err(FrameError::PageCount {
                pages: count,
                bytes: entries.len(),
            });
        };
        check_page(pgno, previous, db_pages_after)?;
        previous = pgno;
        pages.push((pgno, Box::new(image)));
    }
    Ok(CommitRecord {
        db_pages_after,
        pages,
    })
}

/// Puts PART runs back together. Every frame read from the stream goes through [`PartRun::push`]
/// in stream order: a complete run `0..count` of one commit comes out as that COMMIT, and any
/// other frame inside a run voids it (its owner died mid-commit and resends the commit whole).
#[derive(Debug, Default)]
pub struct PartRun {
    run: Option<Run>,
}

#[derive(Debug)]
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
            _ if index == 0 => Run {
                id,
                count,
                next: 0,
                compressed: Vec::new(),
            },
            _ => return Ok(None),
        };
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

#[cfg(test)]
mod tests;
