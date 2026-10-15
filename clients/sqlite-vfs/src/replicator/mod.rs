//! The replicator of one bound database (design §4.6 to §4.10, §8.2): a pure state machine that
//! turns logged commits into appends under the producer key `(epoch, seq)`, claims epochs, reads
//! back what landed before its claims, and resolves `synced()` tickets.
//!
//! [`Replicator::on`] takes one [`Event`] and the time, and returns the [`Action`]s the driver
//! carries out, in order. It does no I/O and draws no randomness: backoff jitter is the driver's
//! (an [`Action::Timer`] names the earliest time and the jitter span), and every request goes out
//! through an [`Action::Send`] whose answer comes back as an [`Event::Answer`] with its
//! [`RequestId`]. The driver makes an [`Action::PersistClaim`] durable before it carries out the
//! actions that follow it, so a claim frame whose activation is in the log is always ours.
//!
//! At most one append or claim is outstanding at a time, beside at most one `synced()` probe, and
//! an answer whose id is not an outstanding one (a late duplicate, an earlier attempt) is ignored.
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
use self::reconcile::CatchUp;
use self::reconcile::Done;
use self::reconcile::Fed;
use self::reconcile::Mode;
use self::reconcile::Reconcile;
use self::sync::Tickets;
use crate::codec::digest::producer_digest;
use crate::codec::frame;
use crate::codec::frame::CONTENT_TYPE;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Position;
pub(crate) use crate::materialize::ForeignFrame;
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

/// The resets of design §8.3, one enum for the replicator and the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResetKind {
    Fenced,
    Stale,
    Rewound,
    Deleted,
    /// Raised by the driver (a failed fsync, a lost WAL page, an uncuttable log write).
    LocalDamage,
    StreamState,
    Internal,
}

/// What ends a takeover at open without binding, and is no reset (design §8.4 setup kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// [`TAKEOVER_ROUNDS`] claims lost to racing claimants.
    ClaimRaces,
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
    /// `synced()` with ticket `ticket`: `target` is at most the last COMMIT position at the call.
    /// The ticket waits for every commit at or below `target`, whatever record sits at `target`,
    /// so the driver delivers `Logged(p)` before any `Sync` whose target is at or above `p`.
    Sync { ticket: u64, target: Position },
    /// Takeover: the driver carried out an [`Action::Apply`] or [`Action::Rebuild`]. Every record
    /// it logged is at or below `last`, and `last` is at or above the last position logged
    /// before (a rebuilt generation's positions continue above the old ones). `log_bytes` is the
    /// log's frame byte count after it (the ACK's `log_bytes`). The ACK covering `last` is
    /// written and fsynced.
    CaughtUp { last: Position, log_bytes: u64 },
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
    /// Append a CLAIM record and fsync it before carrying out any later action: a claim frame
    /// whose activation is in the log is then always ours.
    PersistClaim {
        epoch: Epoch,
        activation: Activation,
    },
    /// Append an ACK record; no fsync needed.
    PersistAck {
        acked: Position,
        acked_offset: Offset,
        log_bytes: u64,
    },
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
    /// Takeover only, nothing pending: catch these frames up in place, in order, then answer
    /// with [`Event::CaughtUp`].
    Apply(Vec<ForeignFrame>),
    /// Takeover only: drop every record from `discard_from` on, if any, and rebuild the
    /// generation from the stream (its latest snapshot and tail) up to `to`, the claim's end
    /// (design §5.6: a divergence, or a `410`). The rebuilt generation's first segment names
    /// `incarnation` and `cluster_id`, its positions continue above the last one logged, and
    /// it holds the CLAIM record of `(epoch, activation)`. Then answer with
    /// [`Event::CaughtUp`].
    Rebuild {
        discard_from: Option<Position>,
        to: Offset,
        epoch: Epoch,
        activation: Activation,
        incarnation: Incarnation,
        cluster_id: ClusterId,
    },
    /// A claim of `epoch` applied and its reconcile completed: the replicator streams.
    Bound {
        epoch: Epoch,
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
    /// The db file's size: a takeover's catch-up in place whose frames grow past it rebuilds
    /// instead (design §5.6).
    pub(crate) db_bytes: u64,
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
    /// The db file's size at the start.
    pub(crate) db_bytes: u64,
    /// `Producer-Id`, checked once when the replicator starts.
    producer: ProducerId,
    next_id: u64,
    /// Negative control: re-seal the in-flight batch when resending it, so one key carries two
    /// bodies. The proptest checks that its properties catch this.
    #[cfg(test)]
    pub(crate) renumber: bool,
}

impl Ctx {
    /// `None` when [`PRODUCER_ID`] is not a valid `Producer-Id`.
    fn new(local: LocalState) -> Option<Self> {
        let producer = ProducerId::new(PRODUCER_ID).ok()?;
        Some(Self {
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
            db_bytes: local.db_bytes,
            producer,
            next_id: 0,
            #[cfg(test)]
            renumber: false,
        })
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
        let producer = ProducerWrite {
            id: self.producer.clone(),
            epoch,
            seq,
            digest: Some(producer_digest(&body)),
        };
        Request {
            keys: ProducerRequestHeaders {
                producer: Some(producer),
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

    /// The first position not acknowledged: tickets at or above it wait.
    pub(crate) fn first_pending(&self) -> Option<Position> {
        self.pending.front().map(|p| p.id.position)
    }

    pub(crate) fn settle(&mut self, actions: &mut Vec<Action>) {
        let first = self.first_pending();
        self.tickets.settle(first, actions);
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
        // An acknowledged batch is at or below any closed seq: only the in-flight one is kept.
        self.sent.retain(|b| b.last > acked);
    }

    fn ack_actions(&self, actions: &mut Vec<Action>) {
        actions.push(Action::PersistAck {
            acked: self.acked,
            acked_offset: self.acked_offset,
            log_bytes: self.log_bytes,
        });
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
    /// Takeover, nothing pending: the driver applies one read's foreign frames
    /// ([`Action::Apply`]) and answers with [`Event::CaughtUp`] before the next read goes out.
    /// `end` is the stream offset after the last of them.
    Applying {
        reconcile: Box<Reconcile>,
        end: Offset,
    },
    /// Takeover: the driver carries out an [`Action::Rebuild`] and answers with
    /// [`Event::CaughtUp`].
    CatchingUp(Box<Done>),
}

/// What a streaming replicator's append slot is doing.
#[derive(Debug, Clone)]
pub(crate) enum Work {
    /// Nothing outstanding.
    Idle,
    /// One sealed batch until a definite answer. `probed`: a probe found the batch at the
    /// expected tail while its answer was outstanding, so no other probe is sent for it.
    Append {
        sealed: Sealed,
        link: Box<Link>,
        probed: bool,
    },
    /// The first pending frame is above the batch limit; checked again at `at_ms`.
    Oversized { at_ms: u64 },
}

/// A leader read at the expected tail, for `synced()` (design §8.2). It goes out beside the
/// append slot, so tickets covered by acknowledgements do not wait on a blocked append.
#[derive(Debug, Clone)]
pub(crate) struct Probe {
    from: Offset,
    link: Box<Link>,
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
        probe: Option<Probe>,
        ctx: Box<Ctx>,
    },
    Reset(
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "the driver takes the kind from Action::Reset")
        )]
        ResetKind,
    ),
    Refused(Refusal),
    Stopped,
}

/// The time an oversized first frame is looked at again.
const OVERSIZED_RETRY_MS: u64 = backoff::BLOCKED_RETRY_MS;

impl Replicator {
    /// Starts claiming: the CLAIM record, then the claim.
    pub(crate) fn activate(kind: ClaimKind, local: LocalState) -> (Self, Vec<Action>) {
        let mut actions = Vec::new();
        let Some(ctx) = Ctx::new(local) else {
            actions.push(Action::Reset(ResetKind::Internal));
            return (Self::Reset(ResetKind::Internal), actions);
        };
        let mut ctx = Box::new(ctx);
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
                probe,
                ctx,
            } => streaming(
                Stream {
                    epoch,
                    seq,
                    work,
                    probe,
                    ctx,
                },
                event,
                now_ms,
                &mut actions,
            ),
        };
        *self = next;
        actions
    }
}

/// Persists and sends the claim of `epoch`.
fn claim(ctx: &mut Ctx, epoch: Epoch, actions: &mut Vec<Action>) -> Link {
    let activation = ctx.activation;
    ctx.claims.push((epoch, activation));
    actions.push(Action::PersistClaim { epoch, activation });
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
            ctx.settle(actions);
            Ok(None)
        }
        Event::Stop => Err(Terminal::Stopped),
        Event::Answer { .. } | Event::Tick | Event::CaughtUp { .. } => Ok(Some(event)),
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
    if matches!(step, Step::CatchingUp(_) | Step::Applying { .. })
        && matches!(event, Event::Logged(_))
    {
        // The catch-up takes the next positions: a commit logged meanwhile breaks them.
        return reset(ResetKind::Internal, actions);
    }
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
            Event::Logged(_)
            | Event::Boundary(_)
            | Event::Sync { .. }
            | Event::CaughtUp { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::Claim { epoch, link },
                ctx,
            },
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
            Event::Logged(_)
            | Event::Boundary(_)
            | Event::Sync { .. }
            | Event::CaughtUp { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::Reconcile { reconcile, link },
                ctx,
            },
        },
        Step::Applying { reconcile, end } => match event {
            Event::CaughtUp { last, log_bytes } => {
                if last < ctx.logged {
                    return reset(ResetKind::Internal, actions);
                }
                // The read's frames hold positions up to `last`, acknowledged up to `end`.
                ctx.logged = last;
                ctx.acknowledge(last, end);
                ctx.log_bytes = log_bytes;
                if reconcile.complete() {
                    return finish(kind, *reconcile, ctx, now_ms, actions);
                }
                let link = Link::send(ctx.read_request(reconcile.at()), &mut ctx, actions);
                Replicator::Activating {
                    kind,
                    step: Step::Reconcile { reconcile, link },
                    ctx,
                }
            }
            Event::Tick
            | Event::Answer { .. }
            | Event::Logged(_)
            | Event::Boundary(_)
            | Event::Sync { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::Applying { reconcile, end },
                ctx,
            },
        },
        Step::CatchingUp(done) => match event {
            Event::CaughtUp { last, log_bytes } => {
                if last < ctx.logged {
                    return reset(ResetKind::Internal, actions);
                }
                // The caught-up frames hold positions up to `last`, acknowledged.
                ctx.logged = last;
                let offset = ctx.acked_offset;
                ctx.acknowledge(last, offset);
                ctx.log_bytes = log_bytes;
                bind(*done, ctx, now_ms, actions)
            }
            Event::Tick
            | Event::Answer { .. }
            | Event::Logged(_)
            | Event::Boundary(_)
            | Event::Sync { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::CatchingUp(done),
                ctx,
            },
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
                return finish(kind, *reconcile, ctx, now_ms, actions);
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

/// After a read of the reconcile: a takeover with nothing pending first applies the read's
/// foreign frames, or rebuilds once they would grow past the database.
fn reconciled(
    kind: ClaimKind,
    mut reconcile: Box<Reconcile>,
    fed: Fed,
    mut ctx: Box<Ctx>,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    if !matches!(fed, Fed::Reset(_))
        && let Some(frames) = reconcile.take_chunk(&mut ctx)
    {
        let end = frames.last().map_or(ctx.acked_offset, |f| f.end_offset);
        actions.push(Action::Apply(frames));
        return Replicator::Activating {
            kind,
            step: Step::Applying { reconcile, end },
            ctx,
        };
    }
    match fed {
        Fed::Reset(kind) => reset(kind, actions),
        Fed::Done => finish(kind, *reconcile, ctx, now_ms, actions),
        Fed::More if reconcile.complete() => finish(kind, *reconcile, ctx, now_ms, actions),
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

/// The reconcile is complete. A takeover that must catch up waits for [`Event::CaughtUp`] first;
/// otherwise the pending tail goes out under the new epoch from seq 1.
fn finish(
    kind: ClaimKind,
    reconcile: Reconcile,
    mut ctx: Box<Ctx>,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    let done = reconcile.into_done();
    match &done.catch_up {
        CatchUp::Nothing => bind(done, ctx, now_ms, actions),
        CatchUp::Rebuild { discard_from } => {
            if let Some(from) = *discard_from {
                ctx.tickets.abandon_from(from, actions);
            }
            actions.push(Action::Rebuild {
                discard_from: *discard_from,
                to: done.after_claim,
                epoch: done.epoch,
                activation: ctx.activation,
                incarnation: ctx.incarnation,
                cluster_id: ctx.cluster_id.clone(),
            });
            Replicator::Activating {
                kind,
                step: Step::CatchingUp(Box::new(done)),
                ctx,
            }
        }
    }
}

/// Streams under the claimed epoch: the ACK, the tickets, then the first batch.
fn bind(done: Done, mut ctx: Box<Ctx>, now_ms: u64, actions: &mut Vec<Action>) -> Replicator {
    actions.push(Action::Bound { epoch: done.epoch });
    ctx.acked_offset = done.after_claim;
    ctx.ack_actions(actions);
    ctx.sent.clear();
    if let Some(claim) = done.claim {
        ctx.tickets.prove(claim);
    }
    ctx.settle(actions);
    let mut stream = Stream {
        epoch: done.epoch,
        seq: Seq::ZERO,
        work: Work::Idle,
        probe: None,
        ctx,
    };
    stream.kick(now_ms, actions);
    stream.into()
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

/// The fields of [`Replicator::Streaming`], while an event is handled.
struct Stream {
    epoch: Epoch,
    seq: Seq,
    work: Work,
    probe: Option<Probe>,
    ctx: Box<Ctx>,
}

impl From<Stream> for Replicator {
    fn from(stream: Stream) -> Self {
        Self::Streaming {
            epoch: stream.epoch,
            seq: stream.seq,
            work: stream.work,
            probe: stream.probe,
            ctx: stream.ctx,
        }
    }
}

impl Stream {
    /// Sends the next batch when the append slot is idle, and a probe for tickets that wait only
    /// for a round trip, unless an append in flight will be that round trip.
    fn kick(&mut self, now_ms: u64, actions: &mut Vec<Action>) {
        let ctx = &mut self.ctx;
        if matches!(self.work, Work::Idle)
            && !ctx.pending.is_empty()
            && let Some(next) = self
                .seq
                .get()
                .checked_add(1)
                .and_then(|next| Seq::try_from(next).ok())
        {
            // An exhausted epoch sends nothing: only a claim moves on.
            match batch::seal(self.epoch, next, ctx) {
                Ok(sealed) => {
                    let request = ctx.write_request(self.epoch, next, Arc::clone(&sealed.body));
                    ctx.sent.push(SentBatch {
                        epoch: self.epoch,
                        seq: next,
                        first: sealed.first,
                        last: sealed.last,
                    });
                    let link = Link::send(request, ctx, actions);
                    self.work = Work::Append {
                        sealed,
                        link: Box::new(link),
                        probed: false,
                    };
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
                    self.work = Work::Oversized { at_ms };
                }
            }
        }
        let append_answers = match &self.work {
            Work::Append { link, probed, .. } => link.in_flight() || *probed,
            Work::Idle | Work::Oversized { .. } => false,
        };
        if self.probe.is_none() && !append_answers && ctx.tickets.wants_probe(ctx.first_pending()) {
            let from = ctx.acked_offset;
            let link = Box::new(Link::send(ctx.read_request(from), ctx, actions));
            self.probe = Some(Probe { from, link });
        }
    }

    fn on(mut self, event: Event, now_ms: u64, actions: &mut Vec<Action>) -> Replicator {
        match event {
            Event::Tick => {
                match &mut self.work {
                    Work::Append { sealed, link, .. } => {
                        renumber(self.epoch, sealed, link, &self.ctx, now_ms);
                        link.tick(now_ms, &mut self.ctx, actions);
                    }
                    Work::Oversized { at_ms } if now_ms >= *at_ms => self.work = Work::Idle,
                    Work::Oversized { at_ms } => actions.push(Action::Timer {
                        at_ms: *at_ms,
                        jitter_ms: 0,
                    }),
                    Work::Idle => {}
                }
                if let Some(probe) = &mut self.probe {
                    probe.link.tick(now_ms, &mut self.ctx, actions);
                }
                self.kick(now_ms, actions);
                self.into()
            }
            Event::Answer { id, exchange } => {
                if let Some(probe) = &self.probe
                    && probe.link.hit(id) != Hit::None
                {
                    return self.probe_answered(id, &exchange, now_ms, actions);
                }
                if let Work::Append { link, .. } = &self.work
                    && link.hit(id) != Hit::None
                {
                    return self.append_answered(id, &exchange, now_ms, actions);
                }
                self.into()
            }
            Event::Logged(_)
            | Event::Boundary(_)
            | Event::Sync { .. }
            | Event::CaughtUp { .. }
            | Event::Stop => {
                self.kick(now_ms, actions);
                self.into()
            }
        }
    }

    fn append_answered(
        mut self,
        id: RequestId,
        exchange: &Exchange,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let Work::Append {
            sealed,
            mut link,
            probed,
        } = mem::replace(&mut self.work, Work::Idle)
        else {
            return self.into();
        };
        let ctx = &mut self.ctx;
        if link.hit(id) == Hit::Head {
            if let Err(kind) = link.head_answer(exchange, now_ms, ctx, actions) {
                return reset(kind, actions);
            }
            self.work = Work::Append {
                sealed,
                link,
                probed,
            };
            self.kick(now_ms, actions);
            return self.into();
        }
        if let Exchange::Answered(answer) = exchange
            && let Ok(crate::wire::Typed::Producer(producer)) = &answer.typed
        {
            ctx.see(producer);
        }
        match decide_append(&sealed, exchange) {
            AppendDecision::Acknowledge { next, delivery } => {
                ctx.set_condition(None, actions);
                ctx.acknowledge(sealed.last, next);
                ctx.ack_actions(actions);
                // Only a write applied by an attempt that started after the call is at the
                // tail: a repeat's bytes may have others after them.
                if delivery == Delivery::Applied {
                    ctx.tickets.prove(id);
                }
                ctx.settle(actions);
                self.seq = sealed.seq;
                // A probe from before this batch would read the batch as foreign bytes.
                if self
                    .probe
                    .as_ref()
                    .is_some_and(|probe| probe.from != self.ctx.acked_offset)
                {
                    self.probe = None;
                }
                self.kick(now_ms, actions);
                self.into()
            }
            AppendDecision::ContinuationClaim => {
                ctx.set_condition(None, actions);
                let Some(next) = next_epoch(self.epoch) else {
                    return reset(ResetKind::Internal, actions);
                };
                let mut ctx = self.ctx;
                let link = claim(&mut ctx, next, actions);
                Replicator::Activating {
                    kind: ClaimKind::Continuation { own: self.epoch },
                    step: Step::Claim { epoch: next, link },
                    ctx,
                }
            }
            AppendDecision::Common(common) => {
                if let Err(kind) = link.common(common, retry_after(exchange), now_ms, ctx, actions)
                {
                    return reset(kind, actions);
                }
                self.work = Work::Append {
                    sealed,
                    link,
                    probed,
                };
                self.kick(now_ms, actions);
                self.into()
            }
        }
    }

    fn probe_answered(
        mut self,
        id: RequestId,
        exchange: &Exchange,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let Some(Probe { from, mut link }) = self.probe.take() else {
            return self.into();
        };
        if from != self.ctx.acked_offset {
            // Stale: our own acknowledged bytes now follow `from`.
            self.kick(now_ms, actions);
            return self.into();
        }
        // The condition is the append slot's while it is busy: the probe leaves it alone.
        let quiet = !matches!(self.work, Work::Idle);
        let ctx = &mut self.ctx;
        if link.hit(id) == Hit::Head {
            let result = quietly(quiet, ctx, actions, |ctx, actions| {
                link.head_answer(exchange, now_ms, ctx, actions)
            });
            if let Err(kind) = result {
                return reset(kind, actions);
            }
            self.probe = Some(Probe { from, link });
            return self.into();
        }
        match decide_read(exchange) {
            ReadDecision::Bytes { next } => {
                let body = match exchange {
                    Exchange::Answered(answer) => answer.body.as_slice(),
                    Exchange::NoAnswer(_) => &[],
                };
                if body.is_empty() && next == from {
                    // The append slot keeps its own condition (a blocked append stays blocked).
                    if matches!(self.work, Work::Idle) {
                        ctx.set_condition(None, actions);
                    }
                    ctx.tickets.prove(id);
                    ctx.settle(actions);
                } else if let Work::Append { sealed, probed, .. } = &mut self.work
                    && sealed.expected_start == from
                    && (sealed.body.starts_with(body) || body.starts_with(&sealed.body))
                {
                    // Our own batch landed at the expected tail while its answer is
                    // outstanding: that answer decides, and proves nothing about the tail.
                    *probed = true;
                } else {
                    return reset(reconcile::past_tail(body), actions);
                }
                self.kick(now_ms, actions);
                self.into()
            }
            // Retention passed our acknowledged tail: another owner moved the stream on.
            ReadDecision::Gone => reset(ResetKind::Fenced, actions),
            ReadDecision::Common(common) => {
                let result = quietly(quiet, ctx, actions, |ctx, actions| {
                    link.common(common, retry_after(exchange), now_ms, ctx, actions)
                });
                if let Err(kind) = result {
                    return reset(kind, actions);
                }
                self.probe = Some(Probe { from, link });
                self.into()
            }
        }
    }
}

/// Runs `f`, and when `quiet` keeps the condition it found: no change, no `Condition` action.
fn quietly<R>(
    quiet: bool,
    ctx: &mut Ctx,
    actions: &mut Vec<Action>,
    f: impl FnOnce(&mut Ctx, &mut Vec<Action>) -> R,
) -> R {
    if !quiet {
        return f(ctx, actions);
    }
    let condition = ctx.condition.clone();
    let mut own = Vec::new();
    let result = f(ctx, &mut own);
    ctx.condition = condition;
    actions.extend(
        own.into_iter()
            .filter(|action| !matches!(action, Action::Condition(_))),
    );
    result
}

fn streaming(
    mut stream: Stream,
    event: Event,
    now_ms: u64,
    actions: &mut Vec<Action>,
) -> Replicator {
    match common_event(&mut stream.ctx, event, actions) {
        Err(terminal) => terminal.into(),
        Ok(None) => {
            stream.kick(now_ms, actions);
            stream.into()
        }
        Ok(Some(event)) => stream.on(event, now_ms, actions),
    }
}
