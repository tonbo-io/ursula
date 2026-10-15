//! The reconcile of design §4.9: after a claim applies, read from the acknowledged offset to the
//! claim's start with `consistency=leader` and the precondition, and sort every frame there by
//! activation, and by commit id plus bytes, never by `(epoch, seq)`. [`decide_frame`] is the
//! table, one row per [`FrameClass`] and one column per [`Mode`].
//!
//! Once a claim at `E'` applies no older-epoch frame of the producer can apply, and the read sees
//! every frame applied before the claim, so each pending record has landed or never will. The
//! records that remain go out under the new epoch from seq 1, same bytes.

use std::sync::Arc;

use ursula_headers::ClosedEpoch;
use ursula_headers::Epoch;
use ursula_headers::Offset;
use ursula_headers::Receipt;

use super::Ctx;
use super::ForeignFrame;
use super::MAX_BATCH_BYTES;
use super::RequestId;
use super::ResetKind;
use crate::codec::frame;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::PartRun;
use crate::ids::Position;

/// The reconcile's column: a takeover at open, or a continuation while bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Takeover,
    Continuation,
}

/// What a frame (or the read) is, relative to this owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameClass {
    /// A CLAIM whose activation is in the log.
    OwnClaim,
    /// A COMMIT, or a complete PART run, with the next pending record's commit id and bytes.
    NextPending,
    /// A CLAIM of another activation.
    ForeignClaim,
    /// Any other COMMIT, our commit id with other bytes included.
    ForeignCommit,
    /// Bytes that are not `USQ2` frames.
    Undecodable,
    /// `410` at the start: below retention.
    Gone,
}

/// A row's decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowDecision {
    Skip,
    Acknowledge,
    /// Takeover: discard the remaining pending records and catch the stream up.
    Diverge,
    Reset(ResetKind),
}

/// The table of design §4.9 (M1: no `410` snapshot rule).
pub(crate) fn decide_frame(mode: Mode, class: FrameClass) -> RowDecision {
    match (class, mode) {
        (FrameClass::OwnClaim, _) => RowDecision::Skip,
        (FrameClass::NextPending, _) => RowDecision::Acknowledge,
        (FrameClass::ForeignClaim, Mode::Takeover) => RowDecision::Skip,
        (FrameClass::ForeignClaim, Mode::Continuation) => RowDecision::Reset(ResetKind::Fenced),
        (FrameClass::ForeignCommit | FrameClass::Gone, Mode::Takeover) => RowDecision::Diverge,
        (FrameClass::ForeignCommit | FrameClass::Gone, Mode::Continuation) => {
            RowDecision::Reset(ResetKind::Stale)
        }
        // The design refuses a takeover's open with `stream_state`: the same kind, one path.
        (FrameClass::Undecodable, _) => RowDecision::Reset(ResetKind::StreamState),
    }
}

/// What a read did to the reconcile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fed {
    /// Read on from [`Reconcile::at`].
    More,
    Done,
    Reset(ResetKind),
}

/// A complete reconcile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Done {
    pub(crate) epoch: Epoch,
    /// The claim's attempt when it applied the claim: a round trip for `synced()`.
    pub(crate) claim: Option<RequestId>,
    /// Where the next write starts: the claim's end.
    pub(crate) after_claim: Offset,
    pub(crate) catch_up: CatchUp,
}

/// How a takeover brings its generation up to the claim (design §5.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CatchUp {
    /// The stream before the claim held only our frames and claims.
    Nothing,
    /// Foreign commits, with nothing pending: applied in place, in order.
    Apply(Vec<ForeignFrame>),
    /// A divergence (from the first pending record discarded) or a `410` (frames below retention
    /// cannot be read): the generation is rebuilt from the stream up to the claim.
    Rebuild { discard_from: Option<Position> },
}

#[derive(Debug, Clone)]
pub(crate) struct Reconcile {
    mode: Mode,
    epoch: Epoch,
    claim: Option<RequestId>,
    /// The claim's start: the reconcile reads up to here.
    end: Offset,
    after_claim: Offset,
    closed: Option<ClosedEpoch>,
    /// The next offset to read.
    at: Offset,
    /// Bytes read and not yet decoded, starting at `buf_start`.
    buf: Vec<u8>,
    buf_start: Offset,
    parts: PartRun,
    catch_up: CatchUp,
}

impl Reconcile {
    pub(crate) fn new(
        mode: Mode,
        epoch: Epoch,
        claim: Option<RequestId>,
        receipt: Receipt,
        closed: Option<ClosedEpoch>,
        from: Offset,
    ) -> Self {
        Self {
            mode,
            epoch,
            claim,
            end: receipt.start(),
            after_claim: receipt.next(),
            closed,
            at: from,
            buf: Vec::new(),
            buf_start: from,
            parts: PartRun::default(),
            catch_up: CatchUp::Nothing,
        }
    }

    pub(crate) fn at(&self) -> Offset {
        self.at
    }

    /// Nothing (more) to read.
    pub(crate) fn complete(&self) -> bool {
        self.at >= self.end && self.buf.is_empty()
    }

    pub(crate) fn into_done(self) -> Done {
        Done {
            epoch: self.epoch,
            claim: self.claim,
            after_claim: self.after_claim,
            catch_up: self.catch_up,
        }
    }

    /// `410`: the frames below retention cannot be read.
    pub(crate) fn gone(&mut self, ctx: &mut Ctx) -> Fed {
        match decide_frame(self.mode, FrameClass::Gone) {
            RowDecision::Diverge => self.rebuild(ctx),
            RowDecision::Reset(kind) => Fed::Reset(kind),
            RowDecision::Skip | RowDecision::Acknowledge => Fed::Reset(ResetKind::Internal),
        }
    }

    /// A read's bytes from [`Reconcile::at`] up to `next`.
    pub(crate) fn feed(&mut self, body: &[u8], next: Offset, ctx: &mut Ctx) -> Fed {
        let wanted = self.end.get().saturating_sub(self.at.get());
        let got = next.get().saturating_sub(self.at.get());
        if next <= self.at || u64::try_from(body.len()).ok() != Some(got) {
            // No progress short of the claim, or a body that is not the range it names: the
            // stream contradicts the claim's receipt.
            return Fed::Reset(ResetKind::StreamState);
        }
        let take = usize::try_from(got.min(wanted)).unwrap_or(usize::MAX);
        self.buf.extend_from_slice(body.get(..take).unwrap_or(body));
        self.at = Offset::from_u64(self.at.get().saturating_add(got.min(wanted)));
        loop {
            match frame::decode(&self.buf, MAX_BATCH_BYTES) {
                Ok(Decoded::Partial) => break,
                Err(_undecodable) => return self.row(FrameClass::Undecodable, None, ctx),
                Ok(Decoded::Frame { frame, len }) => {
                    let raw: Arc<[u8]> = Arc::from(self.buf.get(..len).unwrap_or_default());
                    self.buf.drain(..len.min(self.buf.len()));
                    let end = Offset::from_u64(
                        self.buf_start
                            .get()
                            .saturating_add(u64::try_from(len).unwrap_or(u64::MAX)),
                    );
                    self.buf_start = end;
                    let fed = self.frame(frame, raw, end, ctx);
                    if fed != Fed::More {
                        return fed;
                    }
                }
            }
        }
        if self.at < self.end {
            return Fed::More;
        }
        if !self.buf.is_empty() {
            // A frame runs into the claim.
            return self.row(FrameClass::Undecodable, None, ctx);
        }
        Fed::Done
    }

    fn frame(&mut self, frame: Frame, raw: Arc<[u8]>, end: Offset, ctx: &mut Ctx) -> Fed {
        let direct = matches!(frame, Frame::Commit { .. });
        let frame = match self.parts.push(frame) {
            Err(_bad_part) => return self.row(FrameClass::Undecodable, None, ctx),
            Ok(None) => return Fed::More,
            Ok(Some(frame)) => frame,
        };
        match frame {
            Frame::Claim { activation, .. } => {
                let class = if ctx.owns_activation(activation) {
                    FrameClass::OwnClaim
                } else {
                    FrameClass::ForeignClaim
                };
                self.row(class, None, ctx)
            }
            Frame::Commit { id, .. } => {
                let bytes = if direct {
                    raw
                } else {
                    match frame::encode(&frame) {
                        Ok(bytes) => Arc::from(bytes),
                        Err(_unencodable) => return self.row(FrameClass::Undecodable, None, ctx),
                    }
                };
                let next = ctx
                    .pending
                    .front()
                    .is_some_and(|p| p.id == id && p.frame == bytes);
                let class = if next {
                    FrameClass::NextPending
                } else {
                    FrameClass::ForeignCommit
                };
                self.row(class, Some((frame, end)), ctx)
            }
            Frame::Part { .. } => Fed::More,
        }
    }

    fn row(&mut self, class: FrameClass, commit: Option<(Frame, Offset)>, ctx: &mut Ctx) -> Fed {
        match decide_frame(self.mode, class) {
            RowDecision::Skip => Fed::More,
            RowDecision::Acknowledge => {
                let Some((_, end)) = commit else {
                    return Fed::Reset(ResetKind::Internal);
                };
                let Some(position) = ctx.pending.front().map(|p| p.id.position) else {
                    return Fed::Reset(ResetKind::Internal);
                };
                if self.above_closed_seq(position, ctx) {
                    return Fed::Reset(ResetKind::StreamState);
                }
                ctx.acknowledge(position, end);
                Fed::More
            }
            RowDecision::Diverge if ctx.pending.is_empty() => {
                // Nothing pending to discard: a catch-up in place.
                let Some((frame, end)) = commit else {
                    return self.rebuild(ctx);
                };
                match &mut self.catch_up {
                    CatchUp::Nothing => {
                        self.catch_up = CatchUp::Apply(vec![ForeignFrame {
                            frame,
                            end_offset: end,
                        }]);
                    }
                    CatchUp::Apply(frames) => frames.push(ForeignFrame {
                        frame,
                        end_offset: end,
                    }),
                    CatchUp::Rebuild { .. } => {}
                }
                Fed::More
            }
            RowDecision::Diverge => self.rebuild(ctx),
            RowDecision::Reset(kind) => Fed::Reset(kind),
        }
    }

    /// Takeover: the remaining pending records are dropped and the generation is rebuilt from
    /// the stream, which reads what is left itself, so the reconcile ends here and any frames
    /// collected for a catch-up in place are dropped.
    fn rebuild(&mut self, ctx: &mut Ctx) -> Fed {
        let discard_from = ctx.pending.front().map(|first| first.id.position);
        ctx.pending.clear();
        self.catch_up = CatchUp::Rebuild { discard_from };
        self.at = self.end;
        self.buf.clear();
        Fed::Done
    }

    /// Whether `position` went out in a batch of the epoch the claim closed, above its last
    /// applied seq: that batch must not be in the stream (design §4.8).
    ///
    /// Dead in M1, kept for the continuations of M2 and M3: a takeover binds afresh (nothing
    /// sent), and M1's only continuation follows `producer-expired`, which the server sends only
    /// at or below the epoch floor, where the producer's record was retired, so the claim closes
    /// no epoch. Re-keys and re-cuts claim over a live record and make it live.
    fn above_closed_seq(&self, position: Position, ctx: &Ctx) -> bool {
        let Some(closed) = self.closed else {
            return false;
        };
        ctx.sent.iter().any(|batch| {
            batch.epoch == closed.epoch
                && batch.seq > closed.seq
                && (batch.first..=batch.last).contains(&position)
        })
    }
}

/// The reset for bytes a `synced()` probe found past the acknowledged tail: a valid frame (a
/// claim, a copy's commit or claim under our activation) fences, undecodable bytes are a foreign
/// append (design §7).
pub(crate) fn past_tail(body: &[u8]) -> ResetKind {
    match frame::decode(body, MAX_BATCH_BYTES) {
        Ok(Decoded::Frame { .. }) => ResetKind::Fenced,
        Ok(Decoded::Partial) | Err(_) => ResetKind::StreamState,
    }
}
