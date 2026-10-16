//! The reconcile (design: Reconcile): after a claim applies, read from the acknowledged offset to
//! the claim's start with `consistency=leader` and the precondition, and sort every frame there by
//! activation, and by commit id plus bytes, never by `(epoch, seq)`. [`decide_frame`] is the table,
//! one row per [`FrameClass`] and one column per [`Mode`].
//!
//! Once a claim at `E'` applies no older-epoch frame of the producer can apply, and the read sees
//! every frame applied before the claim, so each pending record has landed or never will. The
//! records that remain go out under the new epoch from seq 1, same bytes.
//!
//! A continuation reads from its log's acknowledged offset, which may trail the claims a takeover
//! of this generation skipped (an ACK lost to a crash before the bind). So a foreign CLAIM fences
//! a continuation only when no CLAIM of our own follows it in the range: one that does applied
//! after it, and supersedes it. Foreign commits still reset `stale` there, since this generation
//! never caught them up. The same reads, up to the tail a HEAD named, are a provisional
//! open's pre-check before it sends its claim (design: Pre-check, [`Reconcile::pre_check`]).

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
use crate::codec::stream::FrameStream;
use crate::codec::stream::StreamError;
use crate::codec::stream::StreamFrame;
use crate::ids::Activation;
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
    /// `410` and the latest snapshot does not cover this owner's acknowledged records (the
    /// `410` rule, design: Reconcile, did not continue from its offset): below retention.
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

/// The reconcile table (design: Reconcile). The `410` row's snapshot check runs first (the
/// replicator's `Step::Gone`): [`FrameClass::Gone`] is its failure.
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

/// What a continuation read after the last CLAIM of its own: another owner's claim (and writes)
/// with no claim of ours after them fences; an earlier owner's that a claim of ours superseded
/// leaves its commits, which this generation never caught up, `stale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Foreign {
    Nothing,
    Claim,
    ClaimAndCommits,
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

/// How a takeover brings its generation up to the claim (design: Materialize), once the reads are
/// done. Foreign commits found with nothing pending are applied in place read by read
/// ([`Reconcile::take_chunk`]), never collected here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CatchUp {
    /// Nothing left to bring in.
    Nothing,
    /// A divergence (from the first pending record discarded) or a `410` (frames below retention
    /// cannot be read): the generation is rebuilt from the stream up to the claim.
    Rebuild { discard_from: Option<Position> },
}

#[derive(Debug, Clone)]
pub(crate) struct Reconcile {
    mode: Mode,
    epoch: Epoch,
    claim: Option<RequestId>,
    after_claim: Offset,
    closed: Option<ClosedEpoch>,
    /// The frames from the acknowledged offset up to the claim's start.
    stream: FrameStream,
    catch_up: CatchUp,
    /// The current read's foreign commits, to apply in place before the next read.
    chunk: Vec<ForeignFrame>,
    /// The compressed bytes applied in place so far.
    applied_bytes: u64,
    /// Continuation: what follows the last CLAIM of our own.
    foreign: Foreign,
    /// The CLAIM frames of our own the reads found.
    own_claims: Vec<(Epoch, Activation)>,
}

/// The compressed record bytes of `frames`.
fn record_bytes(frames: &[ForeignFrame]) -> u64 {
    frames.iter().fold(0u64, |sum, f| match &f.frame {
        Frame::Commit { compressed, .. } => {
            sum.saturating_add(u64::try_from(compressed.len()).unwrap_or(u64::MAX))
        }
        Frame::Claim { .. } | Frame::Part { .. } => sum,
    })
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
            after_claim: receipt.next(),
            closed,
            stream: FrameStream::new(from, receipt.start(), MAX_BATCH_BYTES),
            catch_up: CatchUp::Nothing,
            chunk: Vec::new(),
            applied_bytes: 0,
            foreign: Foreign::Nothing,
            own_claims: Vec::new(),
        }
    }

    /// A provisional open's pre-check (design: Pre-check): the continuation's reads from `from`,
    /// its log's acknowledged offset, up to `to`, the tail a HEAD named, before any claim goes out.
    /// `epoch` is the claim it will send.
    pub(crate) fn pre_check(epoch: Epoch, from: Offset, to: Offset) -> Self {
        Self {
            mode: Mode::Continuation,
            epoch,
            claim: None,
            after_claim: to,
            closed: None,
            stream: FrameStream::new(from, to, MAX_BATCH_BYTES),
            catch_up: CatchUp::Nothing,
            chunk: Vec::new(),
            applied_bytes: 0,
            foreign: Foreign::Nothing,
            own_claims: Vec::new(),
        }
    }

    pub(crate) fn mode(&self) -> Mode {
        self.mode
    }

    /// Whether the reads found the frame of claim `claim`, epoch and activation: it applied.
    pub(crate) fn found_claim(&self, claim: (Epoch, Activation)) -> bool {
        self.own_claims.contains(&claim)
    }

    pub(crate) fn at(&self) -> Offset {
        self.stream.at()
    }

    /// The epoch of the claim this reconcile follows.
    pub(crate) fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Nothing (more) to read.
    pub(crate) fn complete(&self) -> bool {
        self.stream.done()
    }

    /// The claim's start, where the reads end.
    pub(crate) fn end(&self) -> Offset {
        self.stream.end()
    }

    /// The `410` rule found the latest snapshot covering this owner's acknowledged records: the
    /// reads go on from its offset `from` (design: Reconcile).
    pub(crate) fn restart(&mut self, from: Offset) {
        self.stream = FrameStream::new(from, self.stream.end(), MAX_BATCH_BYTES);
        self.chunk.clear();
    }

    pub(crate) fn into_done(self) -> Done {
        Done {
            epoch: self.epoch,
            claim: self.claim,
            after_claim: self.after_claim,
            catch_up: self.catch_up,
        }
    }

    /// The current read's foreign commits to apply in place, if any. Once everything applied in
    /// place would grow past the database, the takeover rebuilds instead (design: Materialize):
    /// `None`, and the reconcile is done with a rebuild.
    pub(crate) fn take_chunk(&mut self, ctx: &mut Ctx) -> Option<Vec<ForeignFrame>> {
        if self.chunk.is_empty() || matches!(self.catch_up, CatchUp::Rebuild { .. }) {
            self.chunk.clear();
            return None;
        }
        let applied = self.applied_bytes.saturating_add(record_bytes(&self.chunk));
        if applied > ctx.db_bytes {
            self.chunk.clear();
            // Nothing is pending (a catch-up in place), so nothing is discarded.
            self.start_rebuild(ctx);
            return None;
        }
        self.applied_bytes = applied;
        Some(std::mem::take(&mut self.chunk))
    }

    /// `410`, and the latest snapshot does not cover the acknowledged records: the frames below
    /// retention cannot be read.
    pub(crate) fn gone(&mut self, ctx: &mut Ctx) -> Fed {
        match decide_frame(self.mode, FrameClass::Gone) {
            RowDecision::Diverge => self.rebuild(ctx),
            RowDecision::Reset(kind) => Fed::Reset(kind),
            RowDecision::Skip | RowDecision::Acknowledge => Fed::Reset(ResetKind::Internal),
        }
    }

    /// A read's bytes from [`Reconcile::at`] up to `next`.
    pub(crate) fn feed(&mut self, body: &[u8], next: Offset, ctx: &mut Ctx) -> Fed {
        let (frames, error) = self.stream.feed(body, next);
        if let Some(StreamError::Range { .. }) = error {
            // No progress short of the claim, or a body that is not the range it names: the
            // stream contradicts the claim's receipt.
            return Fed::Reset(ResetKind::StreamState);
        }
        for frame in frames {
            let fed = self.frame(frame, ctx);
            if fed != Fed::More {
                return fed;
            }
        }
        if error.is_some() {
            // Undecodable bytes, a broken PART run, a frame running into the claim, or an
            // import's claim with no snapshot past it.
            return self.row(FrameClass::Undecodable, None, ctx);
        }
        if !self.stream.done() {
            return Fed::More;
        }
        // A foreign CLAIM no CLAIM of our own followed: another owner claimed after us.
        match self.foreign {
            Foreign::Claim | Foreign::ClaimAndCommits => {
                self.row(FrameClass::ForeignClaim, None, ctx)
            }
            Foreign::Nothing => Fed::Done,
        }
    }

    fn frame(&mut self, read: StreamFrame, ctx: &mut Ctx) -> Fed {
        let StreamFrame { frame, bytes, end } = read;
        match frame {
            Frame::Claim { epoch, activation } if ctx.owns_activation(activation) => {
                self.own_claims.push((epoch, activation));
                // Our own claim applied after any foreign one before it, but not after the commits
                // that came with it.
                if std::mem::replace(&mut self.foreign, Foreign::Nothing)
                    == Foreign::ClaimAndCommits
                {
                    return self.row(FrameClass::ForeignCommit, Some((frame, end)), ctx);
                }
                self.row(FrameClass::OwnClaim, None, ctx)
            }
            Frame::Claim { .. } if self.mode == Mode::Continuation => {
                // Decided once the range shows whether a claim of our own follows it.
                if self.foreign == Foreign::Nothing {
                    self.foreign = Foreign::Claim;
                }
                Fed::More
            }
            Frame::Claim { .. } => self.row(FrameClass::ForeignClaim, None, ctx),
            Frame::Commit { id, .. } => {
                let next = ctx
                    .pending
                    .front()
                    .is_some_and(|p| p.id == id && p.frame == bytes);
                let class = match (next, self.foreign) {
                    // Another owner's write after its claim: decided by whether a claim of our
                    // own follows them.
                    (false, Foreign::Claim | Foreign::ClaimAndCommits) => {
                        self.foreign = Foreign::ClaimAndCommits;
                        return Fed::More;
                    }
                    (false, Foreign::Nothing) => FrameClass::ForeignCommit,
                    // Our commit after another owner's claim with none of ours between: fenced.
                    (true, Foreign::Claim | Foreign::ClaimAndCommits) => FrameClass::ForeignClaim,
                    (true, Foreign::Nothing) => FrameClass::NextPending,
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
                if matches!(self.catch_up, CatchUp::Nothing) {
                    self.chunk.push(ForeignFrame {
                        frame,
                        end_offset: end,
                    });
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
        self.start_rebuild(ctx);
        Fed::Done
    }

    fn start_rebuild(&mut self, ctx: &mut Ctx) {
        let discard_from = ctx.pending.front().map(|first| first.id.position);
        ctx.pending.clear();
        self.catch_up = CatchUp::Rebuild { discard_from };
        self.chunk.clear();
        self.stream.finish();
    }

    /// Whether `position` went out in a batch of the epoch the claim closed, above its last
    /// applied seq: that batch must not be in the stream (design: Claims).
    ///
    /// A takeover binds afresh (nothing sent), and a continuation after `producer-expired` claims
    /// over a retired record, so the claim closes no epoch. A re-key while bound claims over the
    /// live, history-fenced record and closes the epoch the binding streamed in.
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
/// append (design: Clones).
pub(crate) fn past_tail(body: &[u8]) -> ResetKind {
    match frame::decode(body, MAX_BATCH_BYTES) {
        Ok(Decoded::Frame { .. }) => ResetKind::Fenced,
        Ok(Decoded::Partial) | Err(_) => ResetKind::StreamState,
    }
}
