//! Sealing (design §4.6): whole frames of consecutive pending records from the first
//! unacknowledged one, at most the batch limit, ending exactly at a requested boundary. A seal
//! fixes the seq, the body and its digest until a definite answer.

use std::sync::Arc;

use ursula_headers::Epoch;
use ursula_headers::Offset;
use ursula_headers::Seq;

use super::Ctx;
use crate::ids::Position;

/// One append: records `first ..= last` under `(epoch, seq)`, expected to land at
/// `expected_start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sealed {
    pub(crate) epoch: Epoch,
    pub(crate) seq: Seq,
    pub(crate) first: Position,
    pub(crate) last: Position,
    pub(crate) body: Arc<[u8]>,
    pub(crate) expected_start: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SealError {
    /// Nothing is pending.
    Empty,
    /// The first pending frame alone is above the batch limit (no PART frames in M1).
    TooLarge { len: u64, limit: u64 },
}

/// Seals the next batch under `(epoch, seq)`.
pub(crate) fn seal(epoch: Epoch, seq: Seq, ctx: &Ctx) -> Result<Sealed, SealError> {
    let limit = ctx.limits.batch();
    let first = ctx.pending.front().ok_or(SealError::Empty)?;
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
    let Some(last) = taken.last() else {
        return Err(SealError::TooLarge {
            len: u64::try_from(first.frame.len()).unwrap_or(u64::MAX),
            limit,
        });
    };
    let mut body = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    for pending in &taken {
        body.extend_from_slice(&pending.frame);
    }
    Ok(Sealed {
        epoch,
        seq,
        first: first.id.position,
        last: last.id.position,
        body: Arc::from(body),
        expected_start: ctx.acked_offset,
    })
}
