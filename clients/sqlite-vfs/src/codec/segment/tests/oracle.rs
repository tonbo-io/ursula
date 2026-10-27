//! The segment scan as it was before the streaming [`super::super::Reader`]: every record read
//! out of the segment held whole. The equivalence proptest holds the reader to it.

use ursula_headers::Epoch;
use ursula_headers::Offset;

use super::super::ACK;
use super::super::BASE;
use super::super::CLAIM;
use super::super::COMMIT;
use super::super::Record;
use super::super::RecordBody;
use super::super::Scan;
use super::super::SegmentError;
use super::super::Tail;
use super::super::chain;
use super::super::header;
use crate::codec::Take;
use crate::ids::Activation;
use crate::ids::Position;

fn body_of(kind: u8, payload: &[u8]) -> Option<RecordBody> {
    let mut t = Take(payload);
    let body = match kind {
        COMMIT if !payload.is_empty() => {
            return Some(RecordBody::Commit {
                frame: payload.to_vec(),
            });
        }
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

/// The segment's header and its longest valid prefix of records.
pub(super) fn scan(bytes: &[u8]) -> Result<Scan, SegmentError> {
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
        let rest = t.0;
        let Some((kind, position, payload, found, size)) = record_at(rest) else {
            break Tail::Torn { at };
        };
        let chain = chain(previous, kind, position, payload);
        if chain != found {
            break Tail::Torn { at };
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
