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
//! Positions start at 1, so a `base` or `acked` of 0 means nothing yet. The valid log is the
//! longest prefix whose chain verifies and whose positions grow by one from the header's
//! `first_position`. [`scan`] returns that prefix and says how the rest ends: a torn write at the
//! tail, or damage that a chain-valid record follows.

use super::Take;
use crate::ids::Activation;
use crate::ids::Epoch;
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

/// A segment's header: where its records start, the identity of the stream and generation, and
/// the state carried over from the segment before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub lineage: Lineage,
    /// The position of the segment's first record, at least 1.
    pub first_position: Position,
    /// The generation this one replaces, named by the first segment of a new generation.
    pub supersedes: Option<Lineage>,
    pub cluster_id: String,
    pub stream_path: String,
    /// The stream's incarnation token.
    pub incarnation: u64,
    pub base: Position,
    pub acked: Position,
    /// The stream offset just after the last acknowledged commit.
    pub acked_offset: u64,
    /// Frame bytes logged since the latest snapshot.
    pub log_bytes: u64,
    /// The latest CLAIM, if this generation made one.
    pub claim: Option<(Epoch, Activation)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordBody {
    /// A `USQ2` frame: our commit, or a stream frame caught up at open.
    Commit { frame: Vec<u8> },
    /// A claim about to be sent.
    Claim {
        epoch: Epoch,
        activation: Activation,
    },
    /// How far the stream acknowledged the log.
    Ack {
        acked: Position,
        acked_offset: u64,
        log_bytes: u64,
    },
    /// The db file durably holds every commit up to `position`.
    Base { position: Position },
}

/// A valid record and where it ends in the segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub position: Position,
    pub body: RecordBody,
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
    /// An incomplete record or one whose chain does not verify, with no chain-valid record after
    /// it: a torn write, cut at open.
    Torn { at: usize },
    /// A record whose chain does not verify, followed by one whose chain does: damage inside the
    /// log, which open never cuts silently, since its pages may already be in the db file.
    Damaged { at: usize },
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
        Some(lineage) => {
            out.push(1);
            out.extend_from_slice(&lineage.0);
        }
    }
    put_str(&mut out, "cluster_id", &h.cluster_id)?;
    put_str(&mut out, "stream_path", &h.stream_path)?;
    for n in [
        h.incarnation,
        h.base.0,
        h.acked.0,
        h.acked_offset,
        h.log_bytes,
    ] {
        out.extend_from_slice(&n.to_le_bytes());
    }
    match h.claim {
        None => out.push(0),
        Some((epoch, activation)) => {
            out.push(1);
            out.extend_from_slice(&epoch.0.to_le_bytes());
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
            fixed.extend_from_slice(&epoch.0.to_le_bytes());
            fixed.extend_from_slice(&activation.0);
            (CLAIM, &fixed)
        }
        RecordBody::Ack {
            acked,
            acked_offset,
            log_bytes,
        } => {
            fixed.extend_from_slice(&acked.0.to_le_bytes());
            fixed.extend_from_slice(&acked_offset.to_le_bytes());
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
        1 => Some(Lineage(t.array().ok_or_else(truncated)?)),
        tag => return Err(SegmentError::SupersedesTag { tag }),
    };
    let cluster_id = take_str(&mut t, "cluster_id", len)?;
    let stream_path = take_str(&mut t, "stream_path", len)?;
    let mut n = || t.u64().ok_or_else(truncated);
    let (incarnation, base, acked, acked_offset, log_bytes) = (n()?, n()?, n()?, n()?, n()?);
    let claim = match t.u8().ok_or_else(truncated)? {
        0 => None,
        1 => Some((
            Epoch(t.u64().ok_or_else(truncated)?),
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
        incarnation,
        base: Position(base),
        acked: Position(acked),
        acked_offset,
        log_bytes,
        claim,
    };
    Ok((header, found, header_end.saturating_add(4)))
}

fn body_of(kind: u8, payload: &[u8]) -> Option<RecordBody> {
    let mut t = Take(payload);
    let body = match kind {
        COMMIT if !payload.is_empty() => {
            return Some(RecordBody::Commit {
                frame: payload.to_vec(),
            });
        }
        CLAIM => RecordBody::Claim {
            epoch: Epoch(t.u64()?),
            activation: Activation(t.array()?),
        },
        ACK => RecordBody::Ack {
            acked: Position(t.u64()?),
            acked_offset: t.u64()?,
            log_bytes: t.u64()?,
        },
        BASE => RecordBody::Base {
            position: Position(t.u64()?),
        },
        _ => return None,
    };
    t.rest().is_empty().then_some(body)
}

/// The record at the start of `bytes`, if it is whole: its kind, position, payload, stored chain
/// and length.
fn record_at(bytes: &[u8]) -> Option<(u8, Position, &[u8], u32, usize)> {
    let mut t = Take(bytes);
    let len = t.u32()?;
    let kind = t.u8()?;
    let position = Position(t.u64()?);
    let payload = t.bytes(len as usize)?;
    let chain = t.u32()?;
    Some((
        kind,
        position,
        payload,
        chain,
        bytes.len().saturating_sub(t.rest_len()),
    ))
}

/// Whether a record whose chain verifies against the 4 bytes before it, at a position from `from`
/// on, starts anywhere after byte `at`.
fn chained_after(bytes: &[u8], at: usize, from: Option<Position>) -> bool {
    let start = at.saturating_add(1).max(RECORD_TAIL);
    (start..bytes.len()).any(|p| {
        let Some(before) = p.checked_sub(RECORD_TAIL) else {
            return false;
        };
        let (Some(previous), Some(rest)) = (
            bytes
                .get(before..p)
                .and_then(|b| <[u8; 4]>::try_from(b).ok()),
            bytes.get(p..),
        ) else {
            return false;
        };
        record_at(rest).is_some_and(|(kind, position, payload, found, _)| {
            from.is_none_or(|f| position >= f)
                && chain(u32::from_le_bytes(previous), kind, position, payload) == found
        })
    })
}

/// The segment's header and its longest valid prefix of records. A header that is cut short,
/// damaged or of another format is an error: the records cannot be trusted without it.
pub fn scan(bytes: &[u8]) -> Result<Scan, SegmentError> {
    let (header, seed, header_len) = header(bytes)?;
    let mut records = Vec::new();
    let mut previous = seed;
    let mut expected = Some(header.first_position);
    let mut t = Take(bytes.get(header_len..).unwrap_or_default());
    let tail = loop {
        let at = bytes.len().saturating_sub(t.rest_len());
        if t.rest_len() == 0 {
            break Tail::Clean;
        }
        let broken = || {
            if chained_after(bytes, at, expected) {
                Tail::Damaged { at }
            } else {
                Tail::Torn { at }
            }
        };
        let rest = t.rest_slice();
        let Some((kind, position, payload, found, size)) = record_at(rest) else {
            break broken();
        };
        let chain = chain(previous, kind, position, payload);
        if chain != found {
            break broken();
        }
        t = Take(rest.get(size..).unwrap_or_default());
        let Some(want) = expected else {
            let after = records
                .last()
                .map_or(header.first_position, |r: &Record| r.position);
            return Err(SegmentError::PositionOverflow { after });
        };
        if position != want {
            break Tail::Gap {
                at,
                expected: want,
                found: position,
            };
        }
        let Some(body) = body_of(kind, payload) else {
            break Tail::Malformed { at, kind };
        };
        records.push(Record {
            position,
            body,
            chain,
            end: bytes.len().saturating_sub(t.rest_len()),
        });
        previous = chain;
        expected = position.0.checked_add(1).map(Position);
    };
    Ok(Scan {
        header,
        seed,
        header_len,
        records,
        tail,
    })
}

#[cfg(test)]
mod tests;
