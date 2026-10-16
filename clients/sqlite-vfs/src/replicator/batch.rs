//! Sealing (design §4.6): whole frames of consecutive pending records from the first
//! unacknowledged one, at most the batch limit, ending exactly at a requested boundary. A seal
//! fixes the seq, the body and its digest until a definite answer.
//!
//! A first pending frame above the batch limit goes out as a run of PART frames, one per append
//! ([`Run`]): slices of its compressed record of the batch limit less [`frame::PART_OVERHEAD`]
//! bytes, cut once when the run starts, so every part of the run and every resend of a part has
//! the same bytes. A run lives in one epoch: any claim drops it, and the commit goes out again
//! from part 0, cut for the limit of then.

use std::sync::Arc;

use ursula_headers::Epoch;
use ursula_headers::Offset;
use ursula_headers::Seq;

use super::Ctx;
use crate::codec::frame;
use crate::codec::frame::FrameError;
use crate::ids::Position;

/// One append: records `first ..= last` under `(epoch, seq)`, expected to land at
/// `expected_start`. A part of a run holds one record, `first == last`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sealed {
    pub(crate) epoch: Epoch,
    pub(crate) seq: Seq,
    pub(crate) first: Position,
    pub(crate) last: Position,
    pub(crate) body: Arc<[u8]>,
    pub(crate) expected_start: Offset,
    /// Which part of a run the body is, if it is one.
    pub(crate) part: Option<PartOf>,
}

/// A part of a run: `index` of `count`, slices of `chunk` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartOf {
    pub(crate) index: u32,
    pub(crate) count: u32,
    pub(crate) chunk: usize,
}

impl PartOf {
    /// Whether this is the run's last part, whose receipt acknowledges the commit.
    pub(crate) fn last(self) -> bool {
        self.index.saturating_add(1) >= self.count
    }
}

/// The run of PART frames of the commit at `position` this epoch is sending: `next` is the part
/// out or to go out, and `tail` the end of the receipt of the part before it (the acknowledged
/// offset before part 0). Receipts of parts move only `tail`: the acknowledged offset and the ACK
/// stay at the commit boundary before the run until its last part's receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Run {
    pub(crate) position: Position,
    pub(crate) count: u32,
    pub(crate) chunk: usize,
    pub(crate) next: u32,
    pub(crate) tail: Offset,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SealError {
    /// Nothing is pending.
    #[error("nothing is pending")]
    Empty,
    /// The run in progress is not of the first pending record.
    #[error("a run of position {} with position {} first pending", .run.0, .first.0)]
    RunNotFirst { run: Position, first: Position },
    /// The first pending record does not cut into parts.
    #[error("cutting a commit into parts: {0}")]
    Part(#[source] FrameError),
}

/// Seals the next batch under `(epoch, seq)`: the next part of the run in progress, the first
/// part of a new run when the first pending frame is above the batch limit, or whole frames.
pub(crate) fn seal(epoch: Epoch, seq: Seq, ctx: &Ctx) -> Result<Sealed, SealError> {
    let limit = ctx.limits.batch();
    let first = ctx.pending.front().ok_or(SealError::Empty)?;
    let position = first.id.position;
    let len = u64::try_from(first.frame.len()).unwrap_or(u64::MAX);
    let part = match ctx.run {
        Some(run) if run.position == position => Some(PartOf {
            index: run.next,
            count: run.count,
            chunk: run.chunk,
        }),
        Some(run) => {
            return Err(SealError::RunNotFirst {
                run: run.position,
                first: position,
            });
        }
        None if len > limit => {
            // The limit is at least `MIN_REQUEST_BYTES`, so a chunk holds bytes.
            let chunk = usize::try_from(limit)
                .unwrap_or(usize::MAX)
                .saturating_sub(frame::PART_OVERHEAD);
            let count = frame::part_count(&first.frame, chunk).map_err(SealError::Part)?;
            Some(PartOf {
                index: 0,
                count,
                chunk,
            })
        }
        None => None,
    };
    if let Some(part) = part {
        let body = frame::part_frame(&first.frame, part.index, part.count, part.chunk)
            .map_err(SealError::Part)?;
        return Ok(Sealed {
            epoch,
            seq,
            first: position,
            last: position,
            body: Arc::from(body),
            expected_start: ctx.tail(),
            part: Some(part),
        });
    }
    let mut len: u64 = 0;
    let mut taken = Vec::new();
    for pending in &ctx.pending {
        let frame = u64::try_from(pending.frame.len()).unwrap_or(u64::MAX);
        let total = len.saturating_add(frame);
        if total > limit {
            break;
        }
        len = total;
        taken.push(pending);
        if ctx.boundaries.contains(&pending.id.position) {
            break;
        }
    }
    // The first frame fits, so at least it is taken.
    let last = taken.last().map_or(position, |p| p.id.position);
    let mut body = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    for pending in &taken {
        body.extend_from_slice(&pending.frame);
    }
    Ok(Sealed {
        epoch,
        seq,
        first: position,
        last,
        body: Arc::from(body),
        expected_start: ctx.tail(),
        part: None,
    })
}
