//! The replicator of one bound database (design §4.6 to §4.10, §8.2): a pure state machine that
//! turns logged commits into appends under the producer key `(epoch, seq)`, claims epochs, reads
//! back what landed before its claims, and resolves `synced()` tickets.
//!
//! [`Replicator::on`] takes one [`Event`] and the time, and returns the [`Action`]s the driver
//! carries out, in order. It does no I/O and draws no randomness: backoff jitter is the driver's
//! (an [`Action::Timer`] names the earliest time and the jitter span), and every request goes out
//! through an [`Action::Send`] whose answer comes back as an [`Event::Answer`] with its
//! [`RequestId`]. The driver makes an [`Action::Persist`] of a CLAIM durable before it carries out
//! the actions that follow it, so a claim frame whose activation is in the log is always ours.
//!
//! At most one request is outstanding at a time, and an answer whose id is not the outstanding
//! one (a late duplicate, an earlier attempt) is ignored.
//!
//! Left for later milestones: re-key while bound (M2), resume (M4), re-cut and PART sending (M3),
//! the provisional deferred claim (M2) and the `410` snapshot rule (M2). In M1 a `410` in a
//! takeover's reconcile is a divergence, and in a continuation's it resets `stale`.
//!
//! Module map:
//!
//! - [`answer`]: the answer tables of §4.7 as one decide function each, and the HEAD check.
//! - [`backoff`]: the outstanding request, its retries, backoff and the HEAD it may wait on.
//! - [`batch`]: sealing pending records into one append.
//! - [`reconcile`]: the reconcile of §4.9, after every claim.
//! - [`sync`]: `synced()` tickets.

pub(crate) mod answer;
pub(crate) mod backoff;
pub(crate) mod batch;
pub(crate) mod reconcile;
pub(crate) mod sync;

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use ursula_headers::ClusterId;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::ProducerAnswer;
use ursula_headers::ProducerId;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::ProducerWrite;
use ursula_headers::Seq;

use self::answer::AppendDecision;
use self::answer::ClaimDecision;
use self::answer::ReadDecision;
use self::answer::decide_append;
use self::answer::decide_claim;
use self::answer::decide_read;
use self::backoff::Hit;
use self::backoff::Link;
use self::batch::SealError;
use self::batch::Sealed;
use self::reconcile::Fed;
use self::reconcile::Mode;
use self::reconcile::Reconcile;
use self::sync::Tickets;
use crate::codec::digest::producer_digest;
use crate::codec::frame;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::segment::RecordBody;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Position;
use crate::transport::Attempt;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::PRODUCER_ID;
use crate::wire::Request;
use crate::wire::Target;

/// The largest batch, whatever the request limit (design §4.6).
pub(crate) const MAX_BATCH_BYTES: u64 = 4 << 20;

/// The largest read answer the reconcile and the probe expect, which scales the time an answer
/// body may take.
pub(crate) const READ_ANSWER_BYTES: u64 = 4 << 20;

/// A takeover gives up after this many lost claims (`claim_races`, design §4.8).
pub(crate) const TAKEOVER_ROUNDS: u8 = 16;

/// Names one attempt of a request: ids grow with every attempt sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RequestId(pub(crate) u64);

/// A commit logged and not yet acknowledged: its id and its whole `USQ2` COMMIT frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pending {
    pub(crate) id: CommitId,
    pub(crate) frame: Arc<[u8]>,
}

/// A stream frame a takeover's reconcile found that changes pages and is not ours: a COMMIT, or
/// the COMMIT a complete PART run makes. `end` is the stream offset just after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignFrame {
    pub(crate) end: Offset,
    pub(crate) frame: Arc<[u8]>,
}

/// How the replicator claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimKind {
    /// `open()` with the stream answering: above every epoch seen, retried higher after a lost
    /// claim, at most [`TAKEOVER_ROUNDS`] rounds. `round` counts from 1.
    Takeover { round: u8 },
    /// After `producer-expired`: exactly `own + 1`, lost at the first answer but its receipt.
    Continuation { own: Epoch },
}

/// Why requests wait for something other than the network to change (design §4.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockedReason {
    RequestTooLarge,
    Unauthorized,
    PaymentRequired,
    Forbidden,
    Redirected { location: Option<String> },
    WrongCluster,
    Rejected { status: u16 },
}

/// The replication condition status reports (design §9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Condition {
    /// The outstanding request got no definite answer and is sent again after backoff.
    Retrying,
    /// The outstanding request is sent again every 30 s until the cause goes.
    Blocked(BlockedReason),
}

/// The resets of design §8.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResetKind {
    Fenced,
    Stale,
    Rewound,
    Deleted,
    StreamState,
    Internal,
}

/// What ends a takeover at open without binding (design §8.4 setup kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// [`TAKEOVER_ROUNDS`] claims lost to racing claimants.
    ClaimRaces,
    /// The reconcile read bytes that are not `USQ2` frames.
    StreamState,
}

/// What the driver tells the replicator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    /// A commit was appended to the log (no fsync needed).
    Logged(Pending),
    /// What came of attempt `id`.
    Answer { id: RequestId, exchange: Exchange },
    /// The time moved; timers that are due fire.
    Tick,
    /// A batch must end exactly at `position` (a snapshot's `k`, design §4.11).
    Boundary(Position),
    /// `synced()` with ticket `ticket`: `target` is the last logged position at the call.
    Sync { ticket: u64, target: Position },
    /// The binding retires.
    Stop,
}

/// What the replicator asks the driver to do, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action {
    Send {
        id: RequestId,
        request: Request,
        attempt: Attempt,
    },
    /// Append a record to the log. A CLAIM is fsynced before any later action.
    Persist(RecordBody),
    /// Every record up to `acked` is acknowledged; the next write of this owner starts at
    /// `offset`.
    Wake {
        acked: Position,
        offset: Offset,
    },
    Synced {
        ticket: u64,
    },
    /// The ticket's target was discarded by a takeover's divergence: it never resolves.
    Abandoned {
        ticket: u64,
    },
    /// Send an [`Event::Tick`] at a time in `at_ms ..= at_ms + jitter_ms`, chosen uniformly.
    Timer {
        at_ms: u64,
        jitter_ms: u64,
    },
    Condition(Option<Condition>),
    /// Takeover only: catch these frames up, in order, after any discard.
    Apply(Vec<ForeignFrame>),
    /// Takeover only: drop every record from `from` on and rebuild the generation from the stream
    /// up to the claim (design §5.6: a discarded tail always rebuilds).
    Discard {
        from: Position,
    },
    Reset(ResetKind),
    Refuse(Refusal),
}

/// What the log and the open's HEAD say when the replicator starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalState {
    pub(crate) activation: Activation,
    pub(crate) cluster_id: ClusterId,
    pub(crate) incarnation: Incarnation,
    pub(crate) acked: Position,
    /// Where the next write of this owner starts: the reconcile reads from here.
    pub(crate) acked_offset: Offset,
    pub(crate) log_bytes: u64,
    /// The last logged position.
    pub(crate) logged: Position,
    /// Every commit after `acked`, in log order.
    pub(crate) pending: Vec<Pending>,
    /// Every CLAIM record of the log.
    pub(crate) claims: Vec<(Epoch, Activation)>,
    /// The highest epoch seen elsewhere (the snapshot's).
    pub(crate) highest_epoch: Option<Epoch>,
    /// `Ursula-Max-Request-Bytes` of the open's HEAD.
    pub(crate) max_request_bytes: u64,
    /// `URSULA_VFS_MAX_REQUEST_BYTES`.
    pub(crate) configured_max_request_bytes: Option<u64>,
}

/// The request limit `L` (design §4.6): the latest HEAD's, any limit a typed `413` named since,
/// and the configured one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limits {
    pub(crate) head: u64,
    pub(crate) named: Option<u64>,
    pub(crate) configured: Option<u64>,
}

impl Limits {
    pub(crate) fn request(&self) -> u64 {
        [Some(self.head), self.named, self.configured]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(self.head)
    }

    pub(crate) fn batch(&self) -> u64 {
        self.request().min(MAX_BATCH_BYTES)
    }
}

/// A batch this binding sent in its current epoch, for the cross-check against
/// `Producer-Closed-Seq` (design §4.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SentBatch {
    pub(crate) epoch: Epoch,
    pub(crate) seq: Seq,
    pub(crate) first: Position,
    pub(crate) last: Position,
}

/// What every state but the terminal ones carries.
#[derive(Debug, Clone)]
pub(crate) struct Ctx {
    pub(crate) activation: Activation,
    pub(crate) cluster_id: ClusterId,
    pub(crate) incarnation: Incarnation,
    pub(crate) acked: Position,
    pub(crate) acked_offset: Offset,
    pub(crate) log_bytes: u64,
    pub(crate) logged: Position,
    pub(crate) pending: VecDeque<Pending>,
    pub(crate) claims: Vec<(Epoch, Activation)>,
    pub(crate) seen: Option<Epoch>,
    pub(crate) limits: Limits,
    pub(crate) boundaries: BTreeSet<Position>,
    pub(crate) tickets: Tickets,
    pub(crate) sent: Vec<SentBatch>,
    pub(crate) condition: Option<Condition>,
    next_id: u64,
    /// Negative control: re-seal the in-flight batch when resending it, so one key carries two
    /// bodies. The proptest checks that its properties catch this.
    #[cfg(test)]
    pub(crate) renumber: bool,
}

impl Ctx {
    fn new(local: LocalState) -> Self {
        Self {
            activation: local.activation,
            cluster_id: local.cluster_id,
            incarnation: local.incarnation,
            acked: local.acked,
            acked_offset: local.acked_offset,
            log_bytes: local.log_bytes,
            logged: local.logged,
            pending: local.pending.into(),
            claims: local.claims,
            seen: local.highest_epoch,
            limits: Limits {
                head: local.max_request_bytes,
                named: None,
                configured: local.configured_max_request_bytes,
            },
            boundaries: BTreeSet::new(),
            tickets: Tickets::default(),
            sent: Vec::new(),
            condition: None,
            next_id: 0,
            #[cfg(test)]
            renumber: false,
        }
    }

    /// The id of the next attempt.
    pub(crate) fn next_id(&mut self) -> RequestId {
        let id = RequestId(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    /// The id the next attempt will take: every attempt from it on starts after now.
    pub(crate) fn upcoming_id(&self) -> RequestId {
        RequestId(self.next_id)
    }

    pub(crate) fn set_condition(
        &mut self,
        condition: Option<Condition>,
        actions: &mut Vec<Action>,
    ) {
        if self.condition != condition {
            self.condition.clone_from(&condition);
            actions.push(Action::Condition(condition));
        }
    }

    /// Notes the epoch a producer answer names.
    fn see(&mut self, answer: &ProducerAnswer) {
        if let Some(epoch) = answer::answer_epoch(answer) {
            self.seen = Some(self.seen.map_or(epoch, |seen| seen.max(epoch)));
        }
    }

    fn see_epoch(&mut self, epoch: Epoch) {
        self.seen = Some(self.seen.map_or(epoch, |seen| seen.max(epoch)));
    }

    /// Whether a CLAIM frame of `activation` is ours.
    pub(crate) fn owns_activation(&self, activation: Activation) -> bool {
        activation == self.activation || self.claims.iter().any(|(_, a)| *a == activation)
    }

    /// Above every epoch seen: own claims, answers, the snapshot.
    fn takeover_epoch(&self) -> Option<Epoch> {
        let highest = self
            .claims
            .iter()
            .map(|(epoch, _)| *epoch)
            .chain(self.seen)
            .max();
        match highest {
            None => Epoch::try_from(1).ok(),
            Some(epoch) => next_epoch(epoch),
        }
    }

    /// The precondition every request but HEAD carries.
    fn precondition(&self) -> Option<IncarnationPrecondition> {
        Some(IncarnationPrecondition {
            incarnation: self.incarnation,
        })
    }

    /// A keyed append of `body` under `(epoch, seq)`.
    pub(crate) fn write_request(&self, epoch: Epoch, seq: Seq, body: Arc<[u8]>) -> Request {
        let producer = ProducerId::new(PRODUCER_ID).ok().map(|id| ProducerWrite {
            id,
            epoch,
            seq,
            digest: Some(producer_digest(&body)),
        });
        Request {
            keys: ProducerRequestHeaders {
                producer,
                precondition: self.precondition(),
            },
            content_type: Some(CONTENT_TYPE.to_owned()),
            body,
            ..Request::new(Method::Post, Target::Stream)
        }
    }

    /// A `consistency=leader` read from `from`, under the precondition.
    pub(crate) fn read_request(&self, from: Offset) -> Request {
        Request {
            keys: ProducerRequestHeaders {
                producer: None,
                precondition: self.precondition(),
            },
            ..Request::read(Some(from), true, READ_ANSWER_BYTES)
        }
    }

    /// Records acknowledged through `last`, the next write at `offset`.
    fn acknowledge(&mut self, last: Position, offset: Offset) {
        while let Some(front) = self.pending.front() {
            if front.id.position > last {
                break;
            }
            let len = u64::try_from(front.frame.len()).unwrap_or(u64::MAX);
            self.log_bytes = self.log_bytes.saturating_add(len);
            self.pending.pop_front();
        }
        self.acked = self.acked.max(last);
        self.acked_offset = offset;
        let acked = self.acked;
        self.boundaries.retain(|b| *b > acked);
    }

    fn ack_actions(&self, actions: &mut Vec<Action>) {
        actions.push(Action::Persist(RecordBody::Ack {
            acked: self.acked,
            acked_offset: self.acked_offset,
            log_bytes: self.log_bytes,
        }));
        actions.push(Action::Wake {
            acked: self.acked,
            offset: self.acked_offset,
        });
    }
}

fn next_epoch(epoch: Epoch) -> Option<Epoch> {
    epoch
        .get()
        .checked_add(1)
        .and_then(|next| Epoch::try_from(next).ok())
}

/// What an activating replicator is waiting for.
#[derive(Debug, Clone)]
pub(crate) enum Step {
    /// The claim of `epoch`.
    Claim { epoch: Epoch, link: Link },
    /// The reads of the reconcile after the claim applied.
    Reconcile {
        reconcile: Box<Reconcile>,
        link: Link,
    },
}

/// What a streaming replicator is doing.
#[derive(Debug, Clone)]
pub(crate) enum Work {
    /// Nothing outstanding.
    Idle,
    /// One sealed batch in flight until a definite answer.
    Append { sealed: Sealed, link: Link },
    /// A leader read at the expected tail, for `synced()` (design §8.2).
    Probe { from: Offset, link: Link },
    /// The first pending frame is above the batch limit; checked again at `at_ms`.
    Oversized { at_ms: u64 },
}

/// The replicator (design §4.10). Every non-terminal state carries the [`Ctx`].
#[derive(Debug, Clone)]
pub(crate) enum Replicator {
    Activating {
        kind: ClaimKind,
        step: Step,
        ctx: Box<Ctx>,
    },
    /// Appending under `epoch`; `seq` is the last seq of `epoch` acknowledged (0, the claim,
    /// first).
    Streaming {
        epoch: Epoch,
        seq: Seq,
        work: Work,
        ctx: Box<Ctx>,
    },
    Reset(ResetKind),
    Refused(Refusal),
    Stopped,
}

/// The time an oversized first frame is looked at again.
const OVERSIZED_RETRY_MS: u64 = backoff::BLOCKED_RETRY_MS;

impl Replicator {
    /// Starts claiming: the CLAIM record, then the claim.
    pub(crate) fn activate(kind: ClaimKind, local: LocalState) -> (Self, Vec<Action>) {
        let mut ctx = Box::new(Ctx::new(local));
        let mut actions = Vec::new();
        let epoch = match kind {
            ClaimKind::Takeover { .. } => ctx.takeover_epoch(),
            ClaimKind::Continuation { own } => next_epoch(own),
        };
        let Some(epoch) = epoch else {
            actions.push(Action::Reset(ResetKind::Internal));
            return (Self::Reset(ResetKind::Internal), actions);
        };
        let link = claim(&mut ctx, epoch, &mut actions);
        let state = Self::Activating {
            kind,
            step: Step::Claim { epoch, link },
            ctx,
        };
        (state, actions)
    }

    /// The test-only knob of the renumbering negative control.
    #[cfg(test)]
    pub(crate) fn set_renumber(&mut self, renumber: bool) {
        if let Some(ctx) = self.ctx_mut() {
            ctx.renumber = renumber;
        }
    }

    pub(crate) fn ctx(&self) -> Option<&Ctx> {
        match self {
            Self::Activating { ctx, .. } | Self::Streaming { ctx, .. } => Some(ctx),
            Self::Reset(_) | Self::Refused(_) | Self::Stopped => None,
        }
    }

    #[cfg(test)]
    fn ctx_mut(&mut self) -> Option<&mut Ctx> {
        match self {
            Self::Activating { ctx, .. } | Self::Streaming { ctx, .. } => Some(ctx),
            Self::Reset(_) | Self::Refused(_) | Self::Stopped => None,
        }
    }

    /// The one dispatcher: pure, no I/O, no randomness.
    pub(crate) fn on(&mut self, event: Event, now_ms: u64) -> Vec<Action> {
        let mut actions = Vec::new();
        let state = mem::replace(self, Self::Stopped);
        let next = match state {
            Self::Reset(_) | Self::Refused(_) | Self::Stopped => state,
            Self::Activating { kind, step, ctx } => {
                activating(kind, step, ctx, event, now_ms, &mut actions)
            }
            Self::Streaming {
                epoch,
                seq,
                work,
                ctx,
            } => streaming(epoch, seq, work, ctx, event, now_ms, &mut actions),
        };
        *self = next;
        actions
    }
}

/// Persists and sends the claim of `epoch`.
fn claim(ctx: &mut Ctx, epoch: Epoch, actions: &mut Vec<Action>) -> Link {
    let activation = ctx.activation;
    ctx.claims.push((epoch, activation));
    actions.push(Action::Persist(RecordBody::Claim { epoch, activation }));
    let body: Arc<[u8]> = Arc::from(frame::encode_claim(epoch, activation));
    let request = ctx.write_request(epoch, Seq::ZERO, body);
    Link::send(request, ctx, actions)
}

/// How a live state ends on an event every state handles alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Stopped,
    Reset(ResetKind),
}

impl From<Terminal> for Replicator {
    fn from(terminal: Terminal) -> Self {
        match terminal {
            Terminal::Stopped => Self::Stopped,
            Terminal::Reset(kind) => Self::Reset(kind),
        }
    }
}

fn reset(kind: ResetKind, actions: &mut Vec<Action>) -> Replicator {
    actions.push(Action::Reset(kind));
    Replicator::Reset(kind)
}

/// The events every live state handles alike. Returns the event back when the state must handle
/// it, or `Err` with the terminal state.
fn common_event(
    ctx: &mut Ctx,
    event: Event,
    actions: &mut Vec<Action>,
) -> Result<Option<Event>, Terminal> {
    match event {
        Event::Logged(pending) => {
            if pending.id.position <= ctx.logged {
                actions.push(Action::Reset(ResetKind::Internal));
                return Err(Terminal::Reset(ResetKind::Internal));
            }
            ctx.logged = pending.id.position;
            ctx.pending.push_back(pending);
            Ok(None)
        }
        Event::Boundary(position) => {
            if position > ctx.acked {
                ctx.boundaries.insert(position);
            }
            Ok(None)
        }
        Event::Sync { ticket, target } => {
            let after = ctx.upcoming_id();
            ctx.tickets.add(ticket, target, after);
            ctx.tickets.settle(ctx.acked, actions);
            Ok(None)
        }
        Event::Stop => Err(Terminal::Stopped),
        Event::Answer { .. } | Event::Tick => Ok(Some(event)),
    }
}

fn activating(
    kind: ClaimKind,
    step: Step,
    mut ctx: Box<Ctx>,
    event: Event,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    let event = match common_event(&mut ctx, event, actions) {
        Err(terminal) => return terminal.into(),
        Ok(None) => return Replicator::Activating { kind, step, ctx },
        Ok(Some(event)) => event,
    };
    match step {
        Step::Claim { epoch, mut link } => match event {
            Event::Tick => {
                link.tick(now_ms, &mut ctx, actions);
                Replicator::Activating {
                    kind,
                    step: Step::Claim { epoch, link },
                    ctx,
                }
            }
            Event::Answer { id, exchange } => match link.hit(id) {
                Hit::None => Replicator::Activating {
                    kind,
                    step: Step::Claim { epoch, link },
                    ctx,
                },
                Hit::Head => match link.head_answer(&exchange, now_ms, &mut ctx, actions) {
                    Err(kind) => reset(kind, actions),
                    Ok(()) => Replicator::Activating {
                        kind,
                        step: Step::Claim { epoch, link },
                        ctx,
                    },
                },
                Hit::Main => claim_answered(kind, epoch, link, ctx, id, &exchange, now_ms, actions),
            },
            Event::Logged(_) | Event::Boundary(_) | Event::Sync { .. } | Event::Stop => {
                Replicator::Activating {
                    kind,
                    step: Step::Claim { epoch, link },
                    ctx,
                }
            }
        },
        Step::Reconcile {
            mut reconcile,
            mut link,
        } => match event {
            Event::Tick => {
                link.tick(now_ms, &mut ctx, actions);
                Replicator::Activating {
                    kind,
                    step: Step::Reconcile { reconcile, link },
                    ctx,
                }
            }
            Event::Answer { id, exchange } => match link.hit(id) {
                Hit::None => Replicator::Activating {
                    kind,
                    step: Step::Reconcile { reconcile, link },
                    ctx,
                },
                Hit::Head => match link.head_answer(&exchange, now_ms, &mut ctx, actions) {
                    Err(kind) => reset(kind, actions),
                    Ok(()) => Replicator::Activating {
                        kind,
                        step: Step::Reconcile { reconcile, link },
                        ctx,
                    },
                },
                Hit::Main => match decide_read(&exchange) {
                    ReadDecision::Bytes { next } => {
                        ctx.set_condition(None, actions);
                        let body = match &exchange {
                            Exchange::Answered(answer) => answer.body.as_slice(),
                            Exchange::NoAnswer(_) => &[],
                        };
                        let fed = reconcile.feed(body, next, &mut ctx);
                        reconciled(kind, reconcile, fed, ctx, now_ms, actions)
                    }
                    ReadDecision::Gone => {
                        ctx.set_condition(None, actions);
                        let fed = reconcile.gone(&mut ctx);
                        reconciled(kind, reconcile, fed, ctx, now_ms, actions)
                    }
                    ReadDecision::Common(common) => {
                        match link.common(common, retry_after(&exchange), now_ms, &mut ctx, actions)
                        {
                            Err(kind) => reset(kind, actions),
                            Ok(()) => Replicator::Activating {
                                kind,
                                step: Step::Reconcile { reconcile, link },
                                ctx,
                            },
                        }
                    }
                },
            },
            Event::Logged(_) | Event::Boundary(_) | Event::Sync { .. } | Event::Stop => {
                Replicator::Activating {
                    kind,
                    step: Step::Reconcile { reconcile, link },
                    ctx,
                }
            }
        },
    }
}

fn retry_after(exchange: &Exchange) -> Option<std::time::Duration> {
    match exchange {
        Exchange::Answered(answer) => answer.retry_after,
        Exchange::NoAnswer(_) => None,
    }
}

fn claim_answered(
    kind: ClaimKind,
    epoch: Epoch,
    mut link: Link,
    mut ctx: Box<Ctx>,
    id: RequestId,
    exchange: &Exchange,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    if let Exchange::Answered(answer) = exchange
        && let Ok(crate::wire::Typed::Producer(producer)) = &answer.typed
    {
        ctx.see(producer);
    }
    match decide_claim(exchange) {
        ClaimDecision::Won {
            receipt,
            closed,
            delivery,
        } => {
            ctx.set_condition(None, actions);
            if receipt.start() < ctx.acked_offset {
                return reset(ResetKind::StreamState, actions);
            }
            let mode = match kind {
                ClaimKind::Takeover { .. } => Mode::Takeover,
                ClaimKind::Continuation { .. } => Mode::Continuation,
            };
            let reconcile = Box::new(Reconcile::new(
                mode,
                epoch,
                (delivery == Delivery::Applied).then_some(id),
                receipt,
                closed,
                ctx.acked_offset,
            ));
            if reconcile.complete() {
                return finish(*reconcile, ctx, now_ms, actions);
            }
            let link = Link::send(ctx.read_request(reconcile.at()), &mut ctx, actions);
            Replicator::Activating {
                kind,
                step: Step::Reconcile { reconcile, link },
                ctx,
            }
        }
        ClaimDecision::Lost { epoch: named } => {
            if let Some(named) = named {
                ctx.see_epoch(named);
            }
            match kind {
                ClaimKind::Continuation { .. } => reset(ResetKind::Fenced, actions),
                ClaimKind::Takeover { round } if round >= TAKEOVER_ROUNDS => {
                    actions.push(Action::Refuse(Refusal::ClaimRaces));
                    Replicator::Refused(Refusal::ClaimRaces)
                }
                ClaimKind::Takeover { round } => {
                    let Some(next) = ctx.takeover_epoch() else {
                        return reset(ResetKind::Internal, actions);
                    };
                    let link = claim(&mut ctx, next, actions);
                    Replicator::Activating {
                        kind: ClaimKind::Takeover {
                            round: round.saturating_add(1),
                        },
                        step: Step::Claim { epoch: next, link },
                        ctx,
                    }
                }
            }
        }
        ClaimDecision::Common(common) => {
            match link.common(common, retry_after(exchange), now_ms, &mut ctx, actions) {
                Err(kind) => reset(kind, actions),
                Ok(()) => Replicator::Activating {
                    kind,
                    step: Step::Claim { epoch, link },
                    ctx,
                },
            }
        }
    }
}

/// After a read of the reconcile.
fn reconciled(
    kind: ClaimKind,
    reconcile: Box<Reconcile>,
    fed: Fed,
    mut ctx: Box<Ctx>,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    match fed {
        Fed::Reset(kind) => reset(kind, actions),
        Fed::Refuse(refusal) => {
            actions.push(Action::Refuse(refusal));
            Replicator::Refused(refusal)
        }
        Fed::Done => finish(*reconcile, ctx, now_ms, actions),
        Fed::More => {
            let link = Link::send(ctx.read_request(reconcile.at()), &mut ctx, actions);
            Replicator::Activating {
                kind,
                step: Step::Reconcile { reconcile, link },
                ctx,
            }
        }
    }
}

/// The reconcile is complete: the pending tail goes out under the new epoch from seq 1.
fn finish(
    reconcile: Reconcile,
    mut ctx: Box<Ctx>,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    let done = reconcile.into_done();
    if let Some(from) = done.discard_from {
        actions.push(Action::Discard { from });
        ctx.tickets.abandon_from(from, actions);
    }
    if !done.foreign.is_empty() {
        actions.push(Action::Apply(done.foreign));
    }
    ctx.acked_offset = done.after_claim;
    ctx.ack_actions(actions);
    ctx.sent.clear();
    if let Some(claim) = done.claim {
        ctx.tickets.prove(claim);
    }
    ctx.tickets.settle(ctx.acked, actions);
    let mut work = Work::Idle;
    kick(done.epoch, Seq::ZERO, &mut work, &mut ctx, now_ms, actions);
    Replicator::Streaming {
        epoch: done.epoch,
        seq: Seq::ZERO,
        work,
        ctx,
    }
}

/// With nothing outstanding: send the next batch, or probe for waiting tickets.
fn kick(
    epoch: Epoch,
    seq: Seq,
    work: &mut Work,
    ctx: &mut Ctx,
    now_ms: u64,
    actions: &mut Vec<Action>,
) {
    if !matches!(work, Work::Idle) {
        return;
    }
    if !ctx.pending.is_empty() {
        let Some(next) = seq
            .get()
            .checked_add(1)
            .and_then(|next| Seq::try_from(next).ok())
        else {
            // The epoch is exhausted: only a claim moves on.
            return;
        };
        match batch::seal(epoch, next, ctx) {
            Ok(sealed) => {
                let request = ctx.write_request(epoch, next, Arc::clone(&sealed.body));
                ctx.sent.push(SentBatch {
                    epoch,
                    seq: next,
                    first: sealed.first,
                    last: sealed.last,
                });
                let link = Link::send(request, ctx, actions);
                *work = Work::Append { sealed, link };
            }
            Err(SealError::Empty) => {}
            Err(SealError::TooLarge { .. }) => {
                ctx.set_condition(
                    Some(Condition::Blocked(BlockedReason::RequestTooLarge)),
                    actions,
                );
                let at_ms = now_ms.saturating_add(OVERSIZED_RETRY_MS);
                actions.push(Action::Timer {
                    at_ms,
                    jitter_ms: 0,
                });
                *work = Work::Oversized { at_ms };
            }
        }
        return;
    }
    if ctx.tickets.wants_probe(ctx.acked) {
        let from = ctx.acked_offset;
        let link = Link::send(ctx.read_request(from), ctx, actions);
        *work = Work::Probe { from, link };
    }
}

/// The renumbering negative control: a due resend seals the batch again with what is pending
/// now, under the same seq. Only tests turn it on.
#[cfg(test)]
fn renumber(epoch: Epoch, sealed: &mut Sealed, link: &mut Link, ctx: &Ctx, now_ms: u64) {
    if ctx.renumber
        && link.due(now_ms)
        && let Ok(resealed) = batch::seal(epoch, sealed.seq, ctx)
    {
        *sealed = resealed;
        link.replace(ctx.write_request(epoch, sealed.seq, Arc::clone(&sealed.body)));
    }
}

#[cfg(not(test))]
fn renumber(_epoch: Epoch, _sealed: &mut Sealed, _link: &mut Link, _ctx: &Ctx, _now_ms: u64) {}

fn streaming(
    epoch: Epoch,
    seq: Seq,
    mut work: Work,
    mut ctx: Box<Ctx>,
    event: Event,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    let event = match common_event(&mut ctx, event, actions) {
        Err(terminal) => return terminal.into(),
        Ok(None) => {
            kick(epoch, seq, &mut work, &mut ctx, now_ms, actions);
            return Replicator::Streaming {
                epoch,
                seq,
                work,
                ctx,
            };
        }
        Ok(Some(event)) => event,
    };
    let keep = |work, ctx| Replicator::Streaming {
        epoch,
        seq,
        work,
        ctx,
    };
    match (work, event) {
        (Work::Idle, _) => {
            let mut work = Work::Idle;
            kick(epoch, seq, &mut work, &mut ctx, now_ms, actions);
            keep(work, ctx)
        }
        (Work::Oversized { at_ms }, Event::Tick) if now_ms >= at_ms => {
            let mut work = Work::Idle;
            kick(epoch, seq, &mut work, &mut ctx, now_ms, actions);
            keep(work, ctx)
        }
        (Work::Oversized { at_ms }, Event::Tick) => {
            actions.push(Action::Timer {
                at_ms,
                jitter_ms: 0,
            });
            keep(Work::Oversized { at_ms }, ctx)
        }
        (
            Work::Append {
                mut sealed,
                mut link,
            },
            Event::Tick,
        ) => {
            renumber(epoch, &mut sealed, &mut link, &ctx, now_ms);
            link.tick(now_ms, &mut ctx, actions);
            keep(Work::Append { sealed, link }, ctx)
        }
        (Work::Probe { from, mut link }, Event::Tick) => {
            link.tick(now_ms, &mut ctx, actions);
            keep(Work::Probe { from, link }, ctx)
        }
        (Work::Append { sealed, mut link }, Event::Answer { id, exchange }) => match link.hit(id) {
            Hit::None => keep(Work::Append { sealed, link }, ctx),
            Hit::Head => match link.head_answer(&exchange, now_ms, &mut ctx, actions) {
                Err(kind) => reset(kind, actions),
                Ok(()) => keep(Work::Append { sealed, link }, ctx),
            },
            Hit::Main => {
                if let Exchange::Answered(answer) = &exchange
                    && let Ok(crate::wire::Typed::Producer(producer)) = &answer.typed
                {
                    ctx.see(producer);
                }
                match decide_append(&sealed, &exchange) {
                    AppendDecision::Acknowledge { next, delivery } => {
                        ctx.set_condition(None, actions);
                        ctx.acknowledge(sealed.last, next);
                        ctx.ack_actions(actions);
                        // Only a write applied by an attempt that started after the call is
                        // at the tail: a repeat's bytes may have others after them.
                        if delivery == Delivery::Applied {
                            ctx.tickets.prove(id);
                        }
                        ctx.tickets.settle(ctx.acked, actions);
                        let mut work = Work::Idle;
                        kick(epoch, sealed.seq, &mut work, &mut ctx, now_ms, actions);
                        Replicator::Streaming {
                            epoch,
                            seq: sealed.seq,
                            work,
                            ctx,
                        }
                    }
                    AppendDecision::ContinuationClaim => {
                        ctx.set_condition(None, actions);
                        let Some(next) = next_epoch(epoch) else {
                            return reset(ResetKind::Internal, actions);
                        };
                        let link = claim(&mut ctx, next, actions);
                        Replicator::Activating {
                            kind: ClaimKind::Continuation { own: epoch },
                            step: Step::Claim { epoch: next, link },
                            ctx,
                        }
                    }
                    AppendDecision::Common(common) => {
                        match link.common(common, retry_after(&exchange), now_ms, &mut ctx, actions)
                        {
                            Err(kind) => reset(kind, actions),
                            Ok(()) => keep(Work::Append { sealed, link }, ctx),
                        }
                    }
                }
            }
        },
        (Work::Probe { from, mut link }, Event::Answer { id, exchange }) => match link.hit(id) {
            Hit::None => keep(Work::Probe { from, link }, ctx),
            Hit::Head => match link.head_answer(&exchange, now_ms, &mut ctx, actions) {
                Err(kind) => reset(kind, actions),
                Ok(()) => keep(Work::Probe { from, link }, ctx),
            },
            Hit::Main => match decide_read(&exchange) {
                ReadDecision::Bytes { next } => {
                    ctx.set_condition(None, actions);
                    let body = match &exchange {
                        Exchange::Answered(answer) => answer.body.as_slice(),
                        Exchange::NoAnswer(_) => &[],
                    };
                    if !body.is_empty() || next != from {
                        return reset(reconcile::past_tail(body, &ctx), actions);
                    }
                    ctx.tickets.prove(id);
                    ctx.tickets.settle(ctx.acked, actions);
                    let mut work = Work::Idle;
                    kick(epoch, seq, &mut work, &mut ctx, now_ms, actions);
                    keep(work, ctx)
                }
                ReadDecision::Gone => reset(ResetKind::StreamState, actions),
                ReadDecision::Common(common) => {
                    match link.common(common, retry_after(&exchange), now_ms, &mut ctx, actions) {
                        Err(kind) => reset(kind, actions),
                        Ok(()) => keep(Work::Probe { from, link }, ctx),
                    }
                }
            },
        },
        (work @ (Work::Append { .. } | Work::Probe { .. } | Work::Oversized { .. }), _) => {
            keep(work, ctx)
        }
    }
}
