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
//! A `410` in a reconcile follows design §4.9's rule: a HEAD names the latest snapshot, its header
//! alone is read and its checksum verified, and when its last commit is of our lineage at or below
//! the acknowledged position, the reconcile reads on from its offset. Clones share a lineage, so
//! a snapshot above it is never trusted: a takeover diverges and rebuilds, and a continuation
//! resets `stale`.
//!
//! A snapshot asks for a batch ending exactly at its `k` ([`Event::Boundary`]); once `k` is
//! acknowledged, [`Action::Boundary`] hands back `W`, the end offset of that batch's receipt.
//!
//! A HEAD naming another token whose history inherits the generation ([`answer::inherits`], design
//! §6.2) re-keys it: [`Action::ReKey`] rolls the log to a segment naming the new token with the
//! CLAIM record of the claim that follows, and the replicator claims as after any fence. A
//! takeover claims above every epoch seen. While bound, a continuation claims `own + 1` from
//! streaming, and an unconfirmed continuation claim is sent again, same epoch and body, under the
//! new token: claiming higher at once would fence an owner that claimed `own + 1` meanwhile, since
//! every owner shares the producer id. A repeat receipt for that claim (or `receipt-evicted`)
//! means it applied before the history event, so the record stays history-fenced and the next
//! append would be refused (#581 §2.3.1 rows 9 and 11): one more continuation claim follows. A
//! continuation whose latest claim applied past its acknowledged offset (its reconcile not done,
//! a claim out over an epoch it never streamed in, or a provisional open before its bind) first
//! pre-checks under the new token (a HEAD, then the
//! reads from that offset to the tail): the new history may have lost that claim while an
//! explicit open claimed in it, and a background claim must never fence that owner. A takeover
//! re-keys only with nothing pending
//! ([`answer::takeover_may_rekey`]): otherwise it resets `rewound` before the log rolls, and the
//! next open salvages the generation, since the log's acknowledged offset may trail
//! acknowledgements the pending tail holds. A rebuild that discards pending records after a
//! re-key at open salvages the generation it replaces ([`Action::Rebuild`]'s `salvage`), so a
//! history change never discards a local commit without a salvage.
//!
//! A provisional open (design §5.5) defers its claim behind resume's sequence (design §7,
//! [`Replicator::provisional`]): a HEAD first, which learns the token, the cluster and the request
//! limit, then a pre-check that reads from the log's acknowledged offset to the tail the HEAD
//! named. Any foreign frame there resets it with nothing sent. A `410` sends a HEAD first: our
//! token still there resets `stale`, another takes the history path. Only then does it send its
//! claim, and nothing is logged before: it sends again the log's latest claim, same epoch and body,
//! as an unconfirmed claim is sent again after a re-key. A repeat receipt, or `receipt-evicted`
//! (its digest checked), means the claim applied before, and one continuation claim of the next
//! epoch follows. So a provisional open claims at most one above the latest claim of its own the
//! stream applied, and never above an owner an explicit open installed before its pre-check read
//! the tail. `producer-expired` naming the claim's epoch claims on too when the pre-check found
//! that claim's frame. Without the frame it is no proof the retired record was ours: the binding
//! resets `stale`, and the next open (a reopen) takes over with its pending commits. Any other
//! answer loses, `fenced`, as for any continuation.
//!
//! Until a claim of it applies, a provisional open's acknowledged state is the log's, as a
//! takeover's is, so a re-key follows the takeover's rule ([`answer::takeover_may_rekey`]) on the
//! commits the log held at the open. Those it logged while provisional were never sent, so a
//! history that loses nothing re-keys them and they replicate, with no salvage.
//!
//! Left for later milestones: resume (M4), re-cut and PART sending (M3).
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

use ursula_headers::ClosedEpoch;
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
use ursula_headers::SnapshotRef;

use self::answer::AppendDecision;
use self::answer::ClaimDecision;
use self::answer::Common;
use self::answer::HeadCheck;
use self::answer::HeadDecision;
use self::answer::HeadReason;
use self::answer::ReadDecision;
use self::answer::Unacked;
use self::answer::decide_append;
use self::answer::decide_claim;
use self::answer::decide_head;
use self::answer::decide_read;
use self::answer::decide_status;
use self::answer::takeover_may_rekey;
use self::backoff::HeadOutcome;
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
use crate::codec::snapshot::SnapshotHeader;
use crate::codec::snapshot::read_header;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::log;
use crate::log::Level;
pub(crate) use crate::materialize::ForeignFrame;
use crate::transport::Attempt;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::PRODUCER_ID;
use crate::wire::Request;
use crate::wire::SnapshotPart;
use crate::wire::Target;
use crate::wire::Typed;

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
    /// claim, at most [`TAKEOVER_ROUNDS`] rounds. `round` counts from 1. `rekeyed`: the
    /// generation re-keyed in this open (design §6.2).
    Takeover { round: u8, rekeyed: bool },
    /// After `producer-expired`, a re-key while bound, or a provisional open's pre-check: exactly
    /// `own + 1`, lost at the first answer but its receipt. `resent`: the claim went out again
    /// (under a new token, or by a provisional open, which sends its log's latest claim `own`
    /// again), so a repeat receipt may be of an attempt that applied before, and one more claim
    /// follows it.
    Continuation { own: Epoch, resent: bool },
}

/// The claim a replicator holds or is making when it re-keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Holding {
    /// The claim of this epoch is out, its outcome unknown.
    Claiming(Epoch),
    /// The claim of this epoch applied and the replicator streams under it: its frame lies at or
    /// before the acknowledged offset.
    Held(Epoch),
    /// The claim of this epoch applied and its reconcile is not done: its frame lies past the
    /// acknowledged offset.
    Reconciling(Epoch),
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
    /// A batch must end exactly at `position` (a snapshot's `k`, design §4.11): once it is
    /// acknowledged, [`Action::Boundary`] reports it.
    Boundary(Position),
    /// A snapshot published (or found superseded) covers `log_bytes` of the frame bytes counted
    /// since the previous one ([`BoundaryReceipt::log_bytes`]): they no longer count toward the
    /// next.
    Snapshotted { log_bytes: u64 },
    /// `synced()` with ticket `ticket`: `target` is at most the last COMMIT position at the call.
    /// The ticket waits for every commit at or below `target`, whatever record sits at `target`,
    /// so the driver delivers `Logged(p)` before any `Sync` whose target is at or above `p`.
    Sync { ticket: u64, target: Position },
    /// Takeover: the driver carried out an [`Action::Apply`] or [`Action::Rebuild`]. Every record
    /// it logged is at or below `last`, and `last` is at or above the last position logged
    /// before (a rebuilt generation's positions continue above the old ones). `log_bytes` is the
    /// log's frame byte count after it (the ACK's `log_bytes`). The ACK covering `last` is
    /// written and fsynced. `lineage` is the generation's: a rebuilt one has a new lineage.
    CaughtUp {
        last: Position,
        log_bytes: u64,
        lineage: Lineage,
    },
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
    /// Re-key the generation (design §6.2): roll the log to a new segment naming `incarnation`,
    /// append the CLAIM record of `(epoch, activation)` and fsync it, all before carrying out any
    /// later action, the claim's send among them. Every request after it carries `incarnation`.
    ReKey {
        incarnation: Incarnation,
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
    /// The ticket resolved (design §8.2). Only a streaming replicator resolves one, after the
    /// [`Action::Wake`] that covers its target.
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
    /// it holds the CLAIM record of `(epoch, activation)`. With `salvage`, the generation it
    /// replaces is salvaged (`history_changed`) rather than trashed: the takeover re-keyed it at
    /// open and the pending records it discards may hold commits acknowledged under the old
    /// token (design §6.2). Then answer with [`Event::CaughtUp`].
    Rebuild {
        discard_from: Option<Position>,
        salvage: bool,
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
    /// A boundary of [`Event::Boundary`] is acknowledged, or passed.
    Boundary(BoundaryReceipt),
}

/// What came of a boundary `position` (design §4.11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundaryReceipt {
    pub(crate) position: Position,
    /// `W`: the stream holds every record up to `position` before it and none after. The end
    /// offset of the receipt of the batch ending exactly at `position`, or the acknowledged
    /// offset when `position` was already the acknowledged one (only claims follow it). `None`
    /// when acknowledgements passed it without a receipt ending there.
    pub(crate) offset: Option<Offset>,
    /// The highest epoch known claimed when `position` was acknowledged: at or above every epoch
    /// claimed before `offset`.
    pub(crate) epoch: Option<Epoch>,
    /// The frame bytes acknowledged since the latest snapshot, up to `position`.
    pub(crate) log_bytes: u64,
}

/// What the log and the open's HEAD say when the replicator starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalState {
    /// The generation's: our commit ids carry it.
    pub(crate) lineage: Lineage,
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
    /// A takeover at open whose generation was built under this token and whose history the
    /// open's HEAD found inheriting: the replicator re-keys before it claims, and `incarnation`
    /// is the stream's new token.
    pub(crate) rekey_from: Option<Incarnation>,
    /// Whether the log held a COMMIT record after its latest ACK when the open read it: a
    /// takeover re-keys only without one ([`answer::takeover_may_rekey`]).
    pub(crate) unacked: Unacked,
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
    pub(crate) lineage: Lineage,
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
    /// Boundaries acknowledged or passed, reported with the next ACK.
    reached: Vec<BoundaryReceipt>,
    /// The db file's size at the start.
    pub(crate) db_bytes: u64,
    /// Whether the log held a COMMIT record after its latest ACK when the open read it, until a
    /// claim of this replicator applies and its reconcile completes: from then on the
    /// acknowledged state is the stream's, never the log's.
    unacked: Unacked,
    /// The token the generation re-keyed from, until a claim under the new one applies: a HEAD
    /// still inherits from it ([`HeadCheck::previous`]). Several re-keys with no write between
    /// them keep the first.
    pub(crate) rekey_from: Option<Incarnation>,
    /// The epoch this replicator last bound and streamed in: its claim's frame lies at or before
    /// the acknowledged offset.
    streamed: Option<Epoch>,
    /// The claim a pre-check sent again, while it is out, and whether the pre-check found its frame
    /// (it applied, so `producer-expired` naming its epoch is of its own retired record).
    pre_checked: Option<(Epoch, ClaimFrame)>,
    /// The epoch of a provisional open's claim, its log's latest sent again, until the bind: writes
    /// of ours under it may have gone out before (in a history since changed, or lost to a power
    /// loss), so whatever its receipt one more claim follows it, and no key of that epoch is used
    /// twice.
    log_claim: Option<Epoch>,
    /// What the repeat before D4's follow-up claim said it closed: the epoch the binding streamed
    /// in, which the follow-up's own receipt (closing the repeated claim's epoch) no longer names.
    repeat_closed: Option<ClosedEpoch>,
    /// `Producer-Id`, checked once when the replicator starts.
    producer: ProducerId,
    next_id: u64,
    /// The negative controls the proptest checks its properties against.
    #[cfg(test)]
    pub(crate) knobs: Knobs,
}

/// Negative controls: each breaks one rule, and the proptest checks that its properties catch it.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Knobs {
    /// Re-seal the in-flight batch when resending it, so one key carries two bodies.
    pub(crate) renumber: bool,
    /// Re-key on any other token, inheriting or not.
    pub(crate) rekey_always: bool,
    /// Bind on a repeat receipt for a claim sent again after a re-key, with no claim after it.
    pub(crate) no_claim_after_repeat: bool,
    /// Take a HEAD of another cluster for a deleted stream.
    pub(crate) misroute_as_deleted: bool,
    /// Take a HEAD of another cluster's stream for a history that inherits the generation.
    pub(crate) misroute_as_rekey: bool,
    /// Re-key a takeover whose log holds commits after its latest ACK: the #611 B2 gate undone.
    pub(crate) rekey_with_pending: bool,
    /// Settle `synced()` tickets while activating too, as before #604: a claim's reconcile
    /// acknowledges records before it announces them.
    pub(crate) settle_while_activating: bool,
}

impl Ctx {
    /// `None` when [`PRODUCER_ID`] is not a valid `Producer-Id`.
    fn new(local: LocalState) -> Option<Self> {
        let producer = ProducerId::new(PRODUCER_ID).ok()?;
        Some(Self {
            lineage: local.lineage,
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
            reached: Vec::new(),
            db_bytes: local.db_bytes,
            rekey_from: local.rekey_from,
            unacked: local.unacked,
            repeat_closed: None,
            log_claim: None,
            streamed: None,
            pre_checked: None,
            producer,
            next_id: 0,
            #[cfg(test)]
            knobs: Knobs::default(),
        })
    }

    /// What a HEAD is checked against.
    fn head_check(&self) -> HeadCheck<'_> {
        HeadCheck {
            cluster_id: &self.cluster_id,
            incarnation: self.incarnation,
            previous: self.rekey_from,
            acked_offset: self.acked_offset,
        }
    }

    /// What a HEAD answer decides for this generation.
    pub(crate) fn head_decision(&self, exchange: &Exchange) -> HeadDecision {
        let decision = decide_head(exchange, self.head_check());
        #[cfg(test)]
        let decision = self.knobs.head_decision(exchange, decision);
        decision
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

    /// The highest epoch seen: own claims, answers, the snapshot.
    fn highest_epoch(&self) -> Option<Epoch> {
        self.claims
            .iter()
            .map(|(epoch, _)| *epoch)
            .chain(self.seen)
            .max()
    }

    /// Above every epoch seen: own claims, answers, the snapshot.
    fn takeover_epoch(&self) -> Option<Epoch> {
        match self.highest_epoch() {
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

    /// A read of the header of the snapshot at `offset`, under the precondition.
    pub(crate) fn snapshot_header_request(&self, offset: Offset) -> Request {
        Request::snapshot_read(offset, self.incarnation, SnapshotPart::Header)
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
        let passed: Vec<Position> = self.boundaries.range(..=acked).copied().collect();
        for position in passed {
            self.reach(position, (position == last).then_some(offset));
        }
        self.boundaries.retain(|b| *b > acked);
        // An acknowledged batch is at or below any closed seq: only the in-flight one is kept.
        self.sent.retain(|b| b.last > acked);
    }

    /// Boundary `position` is acknowledged, at `offset` when a receipt ends exactly there.
    fn reach(&mut self, position: Position, offset: Option<Offset>) {
        self.reached.push(BoundaryReceipt {
            position,
            offset,
            epoch: self.highest_epoch(),
            log_bytes: self.log_bytes,
        });
    }

    fn ack_actions(&mut self, actions: &mut Vec<Action>) {
        actions.push(Action::PersistAck {
            acked: self.acked,
            acked_offset: self.acked_offset,
            log_bytes: self.log_bytes,
        });
        actions.push(Action::Wake {
            acked: self.acked,
            offset: self.acked_offset,
        });
        actions.extend(self.reached.drain(..).map(Action::Boundary));
    }
}

#[cfg(test)]
impl Knobs {
    /// What the HEAD knobs make of `decision`.
    fn head_decision(self, exchange: &Exchange, decision: HeadDecision) -> HeadDecision {
        let head = match exchange {
            Exchange::Answered(answer) => match &answer.typed {
                Ok(Typed::Head {
                    stream: Some(head), ..
                }) => Some(*head),
                _ => None,
            },
            Exchange::NoAnswer(_) => None,
        };
        let rekey = |head: ursula_headers::StreamHead| HeadDecision::ReKey {
            incarnation: head.incarnation(),
            from: head
                .inherited()
                .map_or(head.incarnation(), |i| i.previous_incarnation),
            max_request_bytes: head.max_request_bytes(),
        };
        match decision {
            HeadDecision::Rewound if self.rekey_always => head.map_or(HeadDecision::Rewound, rekey),
            HeadDecision::WrongCluster if self.misroute_as_deleted => HeadDecision::Deleted,
            HeadDecision::WrongCluster if self.misroute_as_rekey => {
                head.map_or(HeadDecision::WrongCluster, rekey)
            }
            other => other,
        }
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
    /// A provisional open's pre-check before it sends `claim`, its log's latest claim, again
    /// (design §7): the HEAD, then the reads up to the tail it named.
    PreCheck {
        claim: (Epoch, Activation),
        check: PreCheck,
        link: Link,
    },
    /// A read of the reconcile met a `410`: the rule of design §4.9 reads the latest snapshot's
    /// header. `snapshot` is `None` while the HEAD that names it is out, then the snapshot read.
    Gone {
        reconcile: Box<Reconcile>,
        link: Box<Link>,
        snapshot: Option<SnapshotRef>,
    },
}

/// Where a provisional open's pre-check is.
#[derive(Debug, Clone)]
pub(crate) enum PreCheck {
    /// The HEAD without a precondition.
    Head,
    /// The reads from the acknowledged offset to the tail the HEAD named.
    Read(Box<Reconcile>),
}

/// Whether a pre-check found the frame of the claim it sends again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimFrame {
    Found,
    Missing,
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
    /// Starts claiming: the CLAIM record, then the claim. A takeover whose `local` names a token
    /// to re-key from re-keys first ([`Action::ReKey`] in place of the CLAIM record).
    pub(crate) fn activate(kind: ClaimKind, local: LocalState) -> (Self, Vec<Action>) {
        let mut actions = Vec::new();
        let Some(ctx) = Ctx::new(local) else {
            actions.push(Action::Reset(ResetKind::Internal));
            return (Self::Reset(ResetKind::Internal), actions);
        };
        let mut ctx = Box::new(ctx);
        let (kind, epoch, rekeyed) = match kind {
            ClaimKind::Takeover { round, .. } => {
                let rekeyed = ctx.rekey_from.is_some();
                let kind = ClaimKind::Takeover { round, rekeyed };
                (kind, ctx.takeover_epoch(), rekeyed)
            }
            ClaimKind::Continuation { own, resent } => {
                let kind = ClaimKind::Continuation { own, resent };
                (kind, next_epoch(own), false)
            }
        };
        let Some(epoch) = epoch else {
            actions.push(Action::Reset(ResetKind::Internal));
            return (Self::Reset(ResetKind::Internal), actions);
        };
        let link = if rekeyed {
            rekey_claim(&mut ctx, epoch, &mut actions)
        } else {
            claim(&mut ctx, epoch, &mut actions)
        };
        let state = Self::Activating {
            kind,
            step: Step::Claim { epoch, link },
            ctx,
        };
        (state, actions)
    }

    /// A provisional open (design §5.5) of a generation whose log's latest claim is `claim`: the
    /// pre-check of design §7 starts with a HEAD, and nothing is logged until the stream answered.
    pub(crate) fn provisional(
        local: LocalState,
        claim: (Epoch, Activation),
    ) -> (Self, Vec<Action>) {
        let mut actions = Vec::new();
        let Some(ctx) = Ctx::new(local) else {
            actions.push(Action::Reset(ResetKind::Internal));
            return (Self::Reset(ResetKind::Internal), actions);
        };
        let mut ctx = Box::new(ctx);
        ctx.log_claim = Some(claim.0);
        let link = Link::send(head_request(), &mut ctx, &mut actions);
        let state = Self::Activating {
            kind: ClaimKind::Continuation {
                own: claim.0,
                resent: true,
            },
            step: Step::PreCheck {
                claim,
                check: PreCheck::Head,
                link,
            },
            ctx,
        };
        (state, actions)
    }

    /// The test-only negative controls.
    #[cfg(test)]
    pub(crate) fn set_knobs(&mut self, knobs: Knobs) {
        if let Some(ctx) = self.ctx_mut() {
            ctx.knobs = knobs;
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
    send_claim(ctx, epoch, actions)
}

/// Re-keys the generation to `ctx.incarnation` with the CLAIM record of `epoch`, and sends that
/// claim under the new token.
fn rekey_claim(ctx: &mut Ctx, epoch: Epoch, actions: &mut Vec<Action>) -> Link {
    let activation = claim_activation(ctx, epoch);
    if !ctx.claims.contains(&(epoch, activation)) {
        ctx.claims.push((epoch, activation));
    }
    actions.push(Action::ReKey {
        incarnation: ctx.incarnation,
        epoch,
        activation,
    });
    send_claim(ctx, epoch, actions)
}

/// The activation of the claim of `epoch`: the latest CLAIM record of that epoch (a claim sent
/// again keeps its body), else this activation's.
fn claim_activation(ctx: &Ctx, epoch: Epoch) -> Activation {
    ctx.claims
        .iter()
        .rev()
        .find(|(e, _)| *e == epoch)
        .map_or(ctx.activation, |(_, activation)| *activation)
}

/// Sends the claim of `epoch`, whose CLAIM record is logged.
fn send_claim(ctx: &mut Ctx, epoch: Epoch, actions: &mut Vec<Action>) -> Link {
    let body: Arc<[u8]> = Arc::from(frame::encode_claim(epoch, claim_activation(ctx, epoch)));
    let request = ctx.write_request(epoch, Seq::ZERO, body);
    Link::send(request, ctx, actions)
}

/// A HEAD named `incarnation`, another token whose history inherits the generation (design
/// §6.2): the generation re-keys to it and claims under it, as after any fence. A takeover claims
/// above every epoch seen. A continuation sends an unconfirmed claim again, same epoch and body,
/// and claims `own + 1` over a claim it holds. Whatever was outstanding goes: the claim's
/// reconcile reads what landed.
fn rekey(
    kind: ClaimKind,
    holding: Holding,
    incarnation: Incarnation,
    previous: Incarnation,
    mut ctx: Box<Ctx>,
    actions: &mut Vec<Action>,
) -> Replicator {
    if !may_rekey(matches!(kind, ClaimKind::Takeover { .. }), &ctx) {
        // Before the log rolls: the next open finds the old token and refuses the same way.
        return reset(ResetKind::Rewound, actions);
    }
    ctx.set_condition(None, actions);
    let from = ctx.incarnation;
    // The token the HEAD says the stream was last written under: a later history event with no
    // write between names it again.
    ctx.rekey_from = Some(previous);
    ctx.incarnation = incarnation;
    // A claim that applied past the acknowledged offset (one being reconciled, one a claim out
    // continues that the replicator never streamed in, or any of a provisional open before its
    // bind) may be lost with the old history while an explicit open
    // claimed in the new one, which saw no claim of ours above the one before: the latest claim
    // goes out again only after a pre-check under the new token found no other owner past that
    // offset (design §7), so a background claim never fences that owner.
    let past_acked = match (kind, holding) {
        (_, Holding::Reconciling(_)) => true,
        // A claim over the epoch it streamed in: that claim's frame is acknowledged.
        (ClaimKind::Continuation { own, .. }, Holding::Claiming(_)) => ctx.streamed != Some(own),
        (_, Holding::Held(_) | Holding::Claiming(_)) => false,
    } || ctx.log_claim.is_some();
    if let ClaimKind::Continuation { own, .. } = kind
        && past_acked
        && let Some(&claim) = ctx.claims.last()
    {
        log::emit(Level::Info, "rekey", &[
            ("from", &from),
            ("to", &incarnation),
        ]);
        let link = Link::send(head_request(), &mut ctx, actions);
        return Replicator::Activating {
            kind: ClaimKind::Continuation { own, resent: true },
            step: Step::PreCheck {
                claim,
                check: PreCheck::Head,
                link,
            },
            ctx,
        };
    }
    let (kind, epoch) = match (kind, holding) {
        (ClaimKind::Takeover { round, .. }, _) => (
            ClaimKind::Takeover {
                round,
                rekeyed: true,
            },
            ctx.takeover_epoch(),
        ),
        (ClaimKind::Continuation { own, .. }, Holding::Claiming(epoch)) => {
            (ClaimKind::Continuation { own, resent: true }, Some(epoch))
        }
        (ClaimKind::Continuation { .. }, Holding::Held(epoch) | Holding::Reconciling(epoch)) => (
            ClaimKind::Continuation {
                own: epoch,
                resent: true,
            },
            next_epoch(epoch),
        ),
    };
    let Some(epoch) = epoch else {
        return reset(ResetKind::Internal, actions);
    };
    log::emit(Level::Info, "rekey", &[
        ("from", &from),
        ("to", &incarnation),
        ("epoch", &epoch),
    ]);
    let link = rekey_claim(&mut ctx, epoch, actions);
    Replicator::Activating {
        kind,
        step: Step::Claim { epoch, link },
        ctx,
    }
}

/// What an activating replicator does with what its link's HEAD decided.
fn activating_head(
    outcome: HeadOutcome,
    kind: ClaimKind,
    holding: Holding,
    ctx: Box<Ctx>,
    step: Step,
    actions: &mut Vec<Action>,
) -> Replicator {
    match outcome {
        HeadOutcome::Waiting => Replicator::Activating { kind, step, ctx },
        HeadOutcome::ReKey { to, from } => rekey(kind, holding, to, from, ctx, actions),
        HeadOutcome::Reset(kind) => reset(kind, actions),
    }
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
            } else {
                // Acknowledged already: at the acknowledged position the stream holds it up to
                // the acknowledged offset, with only claims after it.
                let offset = (position == ctx.acked).then_some(ctx.acked_offset);
                ctx.reach(position, offset);
                actions.extend(ctx.reached.drain(..).map(Action::Boundary));
            }
            Ok(None)
        }
        Event::Snapshotted { log_bytes } => {
            ctx.log_bytes = ctx.log_bytes.saturating_sub(log_bytes);
            actions.push(Action::PersistAck {
                acked: ctx.acked,
                acked_offset: ctx.acked_offset,
                log_bytes: ctx.log_bytes,
            });
            Ok(None)
        }
        // Only the streaming state settles: a claim's reconcile acknowledges records before it
        // announces them, and outputs wait for the claim and its reconcile (design §5.5, §7).
        Event::Sync { ticket, target } => {
            let after = ctx.upcoming_id();
            ctx.tickets.add(ticket, target, after);
            settle_while_activating(ctx, actions);
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
                Hit::Head => {
                    let outcome = link.head_answer(&exchange, now_ms, &mut ctx, actions);
                    let step = Step::Claim { epoch, link };
                    activating_head(outcome, kind, Holding::Claiming(epoch), ctx, step, actions)
                }
                Hit::Main => claim_answered(kind, epoch, link, ctx, id, &exchange, now_ms, actions),
            },
            Event::Logged(_)
            | Event::Boundary(_)
            | Event::Snapshotted { .. }
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
                Hit::Head => {
                    let outcome = link.head_answer(&exchange, now_ms, &mut ctx, actions);
                    let holding = Holding::Reconciling(reconcile.epoch());
                    let step = Step::Reconcile { reconcile, link };
                    activating_head(outcome, kind, holding, ctx, step, actions)
                }
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
                        let link = Box::new(Link::send(head_request(), &mut ctx, actions));
                        Replicator::Activating {
                            kind,
                            step: Step::Gone {
                                reconcile,
                                link,
                                snapshot: None,
                            },
                            ctx,
                        }
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
            | Event::Snapshotted { .. }
            | Event::Sync { .. }
            | Event::CaughtUp { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::Reconcile { reconcile, link },
                ctx,
            },
        },
        Step::Applying { reconcile, end } => match event {
            Event::CaughtUp {
                last,
                log_bytes,
                lineage,
            } => {
                if last < ctx.logged {
                    return reset(ResetKind::Internal, actions);
                }
                // The read's frames hold positions up to `last`, acknowledged up to `end`.
                ctx.lineage = lineage;
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
            | Event::Snapshotted { .. }
            | Event::Sync { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::Applying { reconcile, end },
                ctx,
            },
        },
        Step::CatchingUp(done) => match event {
            Event::CaughtUp {
                last,
                log_bytes,
                lineage,
            } => {
                if last < ctx.logged {
                    return reset(ResetKind::Internal, actions);
                }
                // A ticket added while catching up whose target lies in the discarded tail never
                // resolves, like those the divergence abandoned.
                if let CatchUp::Rebuild {
                    discard_from: Some(from),
                } = done.catch_up
                {
                    ctx.tickets.abandon_from(from, actions);
                }
                // The caught-up frames hold positions up to `last`, acknowledged.
                ctx.lineage = lineage;
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
            | Event::Snapshotted { .. }
            | Event::Sync { .. }
            | Event::Stop => Replicator::Activating {
                kind,
                step: Step::CatchingUp(done),
                ctx,
            },
        },
        Step::PreCheck {
            claim,
            check,
            mut link,
        } => {
            let hit = match &event {
                Event::Answer { id, .. } => link.hit(*id),
                _ => Hit::None,
            };
            match (event, hit) {
                (Event::Tick, _) => link.tick(now_ms, &mut ctx, actions),
                (Event::Answer { exchange, .. }, Hit::Head) => {
                    return match link.head_answer(&exchange, now_ms, &mut ctx, actions) {
                        HeadOutcome::Waiting => Replicator::Activating {
                            kind,
                            step: Step::PreCheck { claim, check, link },
                            ctx,
                        },
                        HeadOutcome::ReKey { to, from } => {
                            pre_check_rekey(kind, claim, to, from, ctx, actions)
                                .unwrap_or_else(|terminal| terminal.into())
                        }
                        HeadOutcome::Reset(kind) => reset(kind, actions),
                    };
                }
                (Event::Answer { exchange, .. }, Hit::Main) => {
                    let pre = PreChecking { kind, claim, link };
                    return pre.answered(check, &exchange, ctx, now_ms, actions);
                }
                _ => {}
            }
            Replicator::Activating {
                kind,
                step: Step::PreCheck { claim, check, link },
                ctx,
            }
        }
        Step::Gone {
            reconcile,
            mut link,
            snapshot,
        } => {
            let hit = match &event {
                Event::Answer { id, .. } => link.hit(*id),
                _ => Hit::None,
            };
            match (event, hit) {
                (Event::Tick, _) => link.tick(now_ms, &mut ctx, actions),
                (Event::Answer { exchange, .. }, Hit::Head) => {
                    let outcome = link.head_answer(&exchange, now_ms, &mut ctx, actions);
                    let holding = Holding::Reconciling(reconcile.epoch());
                    let step = Step::Gone {
                        reconcile,
                        link,
                        snapshot,
                    };
                    return activating_head(outcome, kind, holding, ctx, step, actions);
                }
                (Event::Answer { exchange, .. }, Hit::Main) => {
                    let gone = Gone {
                        kind,
                        reconcile,
                        link,
                        snapshot,
                    };
                    return gone.answered(&exchange, ctx, now_ms, actions);
                }
                _ => {}
            }
            Replicator::Activating {
                kind,
                step: Step::Gone {
                    reconcile,
                    link,
                    snapshot,
                },
                ctx,
            }
        }
    }
}

/// A HEAD without a precondition.
fn head_request() -> Request {
    Request::new(Method::Head, Target::Stream)
}

/// The tail a HEAD's `200` names.
fn head_tail(exchange: &Exchange) -> Option<Offset> {
    match exchange {
        Exchange::Answered(answer) => match &answer.typed {
            Ok(Typed::Head {
                stream: Some(head), ..
            }) => Some(head.next_offset()),
            _ => None,
        },
        Exchange::NoAnswer(_) => None,
    }
}

/// The pre-check's HEAD found another token whose history inherits the generation: a re-key
/// waits for the claim, which rolls the log ([`Action::ReKey`]). Its log's ACK may trail what the
/// stream acknowledged of commits after it, so with one it resets `rewound`, as a takeover does.
fn re_key_for_pre_check(
    incarnation: Incarnation,
    from: Incarnation,
    ctx: &mut Ctx,
    actions: &mut Vec<Action>,
) -> Result<(), Terminal> {
    if !may_rekey(false, ctx) {
        actions.push(Action::Reset(ResetKind::Rewound));
        return Err(Terminal::Reset(ResetKind::Rewound));
    }
    ctx.set_condition(None, actions);
    log::emit(Level::Info, "rekey", &[
        ("from", &ctx.incarnation),
        ("to", &incarnation),
    ]);
    ctx.rekey_from = Some(from);
    ctx.incarnation = incarnation;
    Ok(())
}

/// A HEAD of the pre-check's link named an inheriting history: the pre-check starts again under
/// the new token, from its HEAD.
fn pre_check_rekey(
    kind: ClaimKind,
    claim: (Epoch, Activation),
    to: Incarnation,
    from: Incarnation,
    mut ctx: Box<Ctx>,
    actions: &mut Vec<Action>,
) -> Result<Replicator, Terminal> {
    re_key_for_pre_check(to, from, &mut ctx, actions)?;
    let link = Link::send(head_request(), &mut ctx, actions);
    Ok(Replicator::Activating {
        kind,
        step: Step::PreCheck {
            claim,
            check: PreCheck::Head,
            link,
        },
        ctx,
    })
}

/// A provisional open's pre-check while its HEAD or a read is out.
struct PreChecking {
    kind: ClaimKind,
    claim: (Epoch, Activation),
    link: Link,
}

impl PreChecking {
    fn waiting(self, check: PreCheck, ctx: Box<Ctx>) -> Replicator {
        Replicator::Activating {
            kind: self.kind,
            step: Step::PreCheck {
                claim: self.claim,
                check,
                link: self.link,
            },
            ctx,
        }
    }

    /// Carries out what an answer that decides nothing about the pre-check decided.
    fn common(
        mut self,
        check: PreCheck,
        common: Common,
        exchange: &Exchange,
        mut ctx: Box<Ctx>,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        match self
            .link
            .common(common, retry_after(exchange), now_ms, &mut ctx, actions)
        {
            Err(kind) => reset(kind, actions),
            Ok(()) => self.waiting(check, ctx),
        }
    }

    /// The answer to the HEAD or to a read.
    fn answered(
        self,
        check: PreCheck,
        exchange: &Exchange,
        mut ctx: Box<Ctx>,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let reconcile = match check {
            PreCheck::Head => return self.head_answered(exchange, ctx, now_ms, actions),
            PreCheck::Read(reconcile) => reconcile,
        };
        match decide_read(exchange) {
            ReadDecision::Bytes { next } => {
                ctx.set_condition(None, actions);
                let body = match exchange {
                    Exchange::Answered(answer) => answer.body.as_slice(),
                    Exchange::NoAnswer(_) => &[],
                };
                let mut reconcile = reconcile;
                match reconcile.feed(body, next, &mut ctx) {
                    Fed::Reset(kind) => reset(kind, actions),
                    Fed::More if !reconcile.complete() => self.read(reconcile, ctx, actions),
                    Fed::More | Fed::Done => {
                        let frame = if reconcile.found_claim(self.claim) {
                            ClaimFrame::Found
                        } else {
                            ClaimFrame::Missing
                        };
                        self.send(frame, ctx, actions)
                    }
                }
            }
            // Retention passed the log's acknowledged offset, or the read went to another
            // history (its `410` comes before the `412`): the link's HEAD decides. Our token
            // still there: the stream moved on, `stale`. Another takes the history path.
            ReadDecision::Gone => {
                let common = Common::Head(HeadReason::Gone(ResetKind::Stale));
                self.common(
                    PreCheck::Read(reconcile),
                    common,
                    exchange,
                    ctx,
                    now_ms,
                    actions,
                )
            }
            ReadDecision::Common(common) => self.common(
                PreCheck::Read(reconcile),
                common,
                exchange,
                ctx,
                now_ms,
                actions,
            ),
        }
    }

    fn head_answered(
        self,
        exchange: &Exchange,
        mut ctx: Box<Ctx>,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let tail = head_tail(exchange);
        match (ctx.head_decision(exchange), tail) {
            (HeadDecision::Same { max_request_bytes }, Some(tail)) => {
                ctx.set_condition(None, actions);
                ctx.limits.head = max_request_bytes;
                self.start_reads(tail, ctx, actions)
            }
            (
                HeadDecision::ReKey {
                    incarnation,
                    from,
                    max_request_bytes,
                },
                Some(tail),
            ) => {
                ctx.limits.head = max_request_bytes;
                if let Err(terminal) = re_key_for_pre_check(incarnation, from, &mut ctx, actions) {
                    return terminal.into();
                }
                self.start_reads(tail, ctx, actions)
            }
            (HeadDecision::Rewound, _) => reset(ResetKind::Rewound, actions),
            (HeadDecision::Deleted, _) => reset(ResetKind::Deleted, actions),
            (HeadDecision::WrongCluster, _) => {
                let common = Common::Blocked(BlockedReason::WrongCluster);
                self.common(PreCheck::Head, common, exchange, ctx, now_ms, actions)
            }
            (HeadDecision::Common(common), _) => {
                // A HEAD never waits on another HEAD.
                let common = match (common, exchange) {
                    (Common::Head(_), Exchange::Answered(answer)) => {
                        Common::Blocked(BlockedReason::Rejected {
                            status: answer.status,
                        })
                    }
                    (other, _) => other,
                };
                self.common(PreCheck::Head, common, exchange, ctx, now_ms, actions)
            }
            // A `200` names its stream.
            (HeadDecision::Same { .. } | HeadDecision::ReKey { .. }, None) => {
                reset(ResetKind::Internal, actions)
            }
        }
    }

    /// Reads from the log's acknowledged offset to `tail`.
    fn start_reads(self, tail: Offset, ctx: Box<Ctx>, actions: &mut Vec<Action>) -> Replicator {
        if tail < ctx.acked_offset {
            // The stream under this token holds less than this generation acknowledged.
            return reset(ResetKind::StreamState, actions);
        }
        let reconcile = Box::new(Reconcile::pre_check(self.claim.0, ctx.acked_offset, tail));
        if reconcile.complete() {
            // Nothing to read: the claim's frame lies at or before the acknowledged offset.
            return self.send(ClaimFrame::Missing, ctx, actions);
        }
        self.read(reconcile, ctx, actions)
    }

    fn read(
        self,
        reconcile: Box<Reconcile>,
        mut ctx: Box<Ctx>,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let link = Link::send(ctx.read_request(reconcile.at()), &mut ctx, actions);
        Self { link, ..self }.waiting(PreCheck::Read(reconcile), ctx)
    }

    /// Nothing foreign up to the tail: the log's latest claim goes out again, same epoch and
    /// body, its CLAIM record already logged (the log rolls first after a re-key). `frame`: whether
    /// the pre-check found that claim's frame.
    fn send(self, frame: ClaimFrame, mut ctx: Box<Ctx>, actions: &mut Vec<Action>) -> Replicator {
        let (epoch, activation) = self.claim;
        ctx.pre_checked = Some((epoch, frame));
        if ctx.rekey_from.is_some() {
            actions.push(Action::ReKey {
                incarnation: ctx.incarnation,
                epoch,
                activation,
            });
        }
        let link = send_claim(&mut ctx, epoch, actions);
        Replicator::Activating {
            kind: self.kind,
            step: Step::Claim { epoch, link },
            ctx,
        }
    }
}

/// The `410` rule of design §4.9 while its HEAD or snapshot read is out.
struct Gone {
    kind: ClaimKind,
    reconcile: Box<Reconcile>,
    link: Box<Link>,
    snapshot: Option<SnapshotRef>,
}

impl Gone {
    fn waiting(self, ctx: Box<Ctx>) -> Replicator {
        Replicator::Activating {
            kind: self.kind,
            step: Step::Gone {
                reconcile: self.reconcile,
                link: self.link,
                snapshot: self.snapshot,
            },
            ctx,
        }
    }

    /// Carries out what an answer that decides nothing about the rule decided.
    fn common(
        mut self,
        common: Common,
        exchange: &Exchange,
        mut ctx: Box<Ctx>,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        match self
            .link
            .common(common, retry_after(exchange), now_ms, &mut ctx, actions)
        {
            Err(kind) => reset(kind, actions),
            Ok(()) => self.waiting(ctx),
        }
    }

    /// The answer to the HEAD or to the snapshot read.
    fn answered(
        self,
        exchange: &Exchange,
        mut ctx: Box<Ctx>,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let Exchange::Answered(answer) = exchange else {
            let common = Common::Resend {
                retry_after: None,
                fresh: true,
            };
            return self.common(common, exchange, ctx, now_ms, actions);
        };
        let Some(snapshot) = self.snapshot else {
            return match ctx.head_decision(exchange) {
                HeadDecision::Same { max_request_bytes } => {
                    ctx.set_condition(None, actions);
                    ctx.limits.head = max_request_bytes;
                    let latest = match &answer.typed {
                        Ok(Typed::Head {
                            stream: Some(head), ..
                        }) => head.snapshot(),
                        _ => None,
                    };
                    match latest.filter(|s| s.offset >= self.reconcile.at()) {
                        None => self.fails(ctx, now_ms, actions),
                        Some(latest) => {
                            let request = ctx.snapshot_header_request(latest.offset);
                            let link = Link::send(request, &mut ctx, actions);
                            Self {
                                link: Box::new(link),
                                snapshot: Some(latest),
                                ..self
                            }
                            .waiting(ctx)
                        }
                    }
                }
                HeadDecision::ReKey {
                    incarnation,
                    from,
                    max_request_bytes,
                } => {
                    ctx.limits.head = max_request_bytes;
                    let holding = Holding::Reconciling(self.reconcile.epoch());
                    rekey(self.kind, holding, incarnation, from, ctx, actions)
                }
                HeadDecision::Rewound => reset(ResetKind::Rewound, actions),
                HeadDecision::Deleted => reset(ResetKind::Deleted, actions),
                HeadDecision::WrongCluster => {
                    let common = Common::Blocked(BlockedReason::WrongCluster);
                    self.common(common, exchange, ctx, now_ms, actions)
                }
                HeadDecision::Common(common) => {
                    // A HEAD never waits on another HEAD.
                    let common = match common {
                        Common::Head(_) => Common::Blocked(BlockedReason::Rejected {
                            status: answer.status,
                        }),
                        other => other,
                    };
                    self.common(common, exchange, ctx, now_ms, actions)
                }
            };
        };
        match &answer.typed {
            Ok(Typed::SnapshotRead(read))
                if matches!(answer.status, 200 | 206) && read.snapshot == snapshot =>
            {
                ctx.set_condition(None, actions);
                // The header verifies without the body (design §4.11).
                match read_header(&answer.body) {
                    Ok((header, _))
                        if header.fields.offset == snapshot.offset
                            && self.covers(&ctx, &header) =>
                    {
                        self.continue_from(&header, ctx, now_ms, actions)
                    }
                    // Not ours, or not at or below what this copy acknowledged.
                    Ok(_) => self.fails(ctx, now_ms, actions),
                    Err(e) => {
                        log::emit(Level::Warn, "snapshot_unreadable", &[
                            ("offset", &snapshot.offset),
                            ("error", &e),
                        ]);
                        self.fails(ctx, now_ms, actions)
                    }
                }
            }
            // Superseded since the HEAD: the HEAD again names the latest.
            Ok(Typed::Untyped) if answer.status == 404 => {
                let link = Link::send(head_request(), &mut ctx, actions);
                Self {
                    link: Box::new(link),
                    snapshot: None,
                    ..self
                }
                .waiting(ctx)
            }
            _ => {
                let common = decide_status(answer);
                self.common(common, exchange, ctx, now_ms, actions)
            }
        }
    }

    /// Whether the snapshot of `header` is a state this copy knows the stream holds: its last
    /// commit is of our lineage at or below the acknowledged position (durable, or matched by id
    /// and bytes in this reconcile), and its offset lies in what the reconcile reads. Clones
    /// share a lineage, so a position above our acknowledged one may be another copy's commit
    /// with other bytes: never trusted, a divergence. Our own snapshot never runs ahead of the
    /// durable acknowledged position (the snapshot thread fsyncs ACK(k, W) before publishing).
    ///
    /// A continuation never skips bytes it did not read: the snapshot must sit where the reads
    /// are. Our own snapshots never put it past the durable acknowledged offset, so one past it is
    /// another copy's, and whatever it skipped may hold a claim or commit of another owner.
    fn covers(&self, ctx: &Ctx, header: &SnapshotHeader) -> bool {
        let last = header.fields.last;
        let offset = header.fields.offset;
        let unread = match self.reconcile.mode() {
            Mode::Takeover => self.reconcile.at() <= offset,
            Mode::Continuation => self.reconcile.at() == offset,
        };
        last.lineage == ctx.lineage
            && last.position <= ctx.acked
            && unread
            && offset <= self.reconcile.end()
    }

    /// The snapshot covers the acknowledged records: those it includes are acknowledged, and the
    /// reconcile reads on from its offset.
    fn continue_from(
        mut self,
        header: &SnapshotHeader,
        mut ctx: Box<Ctx>,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        ctx.see_epoch(header.fields.epoch);
        ctx.acknowledge(header.fields.last.position, header.fields.offset);
        self.reconcile.restart(header.fields.offset);
        if self.reconcile.complete() {
            return finish(self.kind, *self.reconcile, ctx, now_ms, actions);
        }
        let link = Link::send(ctx.read_request(self.reconcile.at()), &mut ctx, actions);
        Replicator::Activating {
            kind: self.kind,
            step: Step::Reconcile {
                reconcile: self.reconcile,
                link,
            },
            ctx,
        }
    }

    /// The snapshot does not cover the acknowledged records: a takeover diverges, a continuation
    /// resets `stale`.
    fn fails(mut self, mut ctx: Box<Ctx>, now_ms: u64, actions: &mut Vec<Action>) -> Replicator {
        let fed = self.reconcile.gone(&mut ctx);
        reconciled(self.kind, self.reconcile, fed, ctx, now_ms, actions)
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
            let follow_up = match delivery {
                Delivery::Repeat => !repeat_binds(&ctx),
                // A provisional open never writes under the claim it sent again: writes of ours
                // may have gone out under that epoch in a history before.
                Delivery::Applied => ctx.log_claim == Some(epoch),
            };
            if follow_up && matches!(kind, ClaimKind::Continuation { resent: true, .. }) {
                // The claim applied before the history event, so the record stays
                // history-fenced: one more claim over it (design §6.2, D4). So does a provisional
                // open's claim, whatever its receipt.
                let Some(next) = next_epoch(epoch) else {
                    return reset(ResetKind::Internal, actions);
                };
                ctx.repeat_closed = closed;
                let link = claim(&mut ctx, next, actions);
                return Replicator::Activating {
                    kind: ClaimKind::Continuation {
                        own: epoch,
                        resent: false,
                    },
                    step: Step::Claim { epoch: next, link },
                    ctx,
                };
            }
            // Every claim but a repeat after a re-key (handled above) only ever went out under the
            // current token: it landed under it, and a later history event begins from it.
            ctx.rekey_from = None;
            // D4's follow-up closes only the repeated claim's epoch, at seq 0: the reconcile
            // cross-checks the epoch the binding streamed in against what the repeat named.
            let repeated = ctx.repeat_closed.take();
            let closed = match (closed, repeated) {
                (Some(own), Some(repeated))
                    if own.seq == Seq::ZERO && next_epoch(own.epoch) == Some(epoch) =>
                {
                    Some(repeated)
                }
                (closed, _) => closed,
            };
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
        // A claim sent again whose record retired, its frame not found in the pre-check: the
        // retired record may be another claimant's, and the only safe next step is a reopen, which
        // takes over (design §8.3: `stale`, action `reopen`).
        ClaimDecision::Lost { epoch: Some(named) }
            if named == epoch
                && expired(exchange)
                && ctx.pre_checked == Some((epoch, ClaimFrame::Missing)) =>
        {
            ctx.see_epoch(named);
            reset(ResetKind::Stale, actions)
        }
        // A claim sent again that applied before (its receipt evicted by later writes under it,
        // or its record retired with its frame found in the pre-check): one continuation claim
        // above it follows, as after a repeat (design §4.8).
        ClaimDecision::Lost { epoch: Some(named) }
            if named == epoch
                && matches!(kind, ClaimKind::Continuation { resent: true, .. })
                && applied_before(
                    exchange,
                    ctx.pre_checked == Some((epoch, ClaimFrame::Found)),
                ) =>
        {
            ctx.see_epoch(named);
            let Some(next) = next_epoch(epoch) else {
                return reset(ResetKind::Internal, actions);
            };
            let link = claim(&mut ctx, next, actions);
            Replicator::Activating {
                kind: ClaimKind::Continuation {
                    own: epoch,
                    resent: false,
                },
                step: Step::Claim { epoch: next, link },
                ctx,
            }
        }
        ClaimDecision::Lost { epoch: named } => {
            if let Some(named) = named {
                ctx.see_epoch(named);
            }
            match kind {
                ClaimKind::Continuation { .. } => reset(ResetKind::Fenced, actions),
                ClaimKind::Takeover { round, .. } if round >= TAKEOVER_ROUNDS => {
                    actions.push(Action::Refuse(Refusal::ClaimRaces));
                    Replicator::Refused(Refusal::ClaimRaces)
                }
                ClaimKind::Takeover { round, rekeyed } => {
                    let Some(next) = ctx.takeover_epoch() else {
                        return reset(ResetKind::Internal, actions);
                    };
                    let link = claim(&mut ctx, next, actions);
                    Replicator::Activating {
                        kind: ClaimKind::Takeover {
                            round: round.saturating_add(1),
                            rekeyed,
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

/// Whether the generation may re-key ([`answer::takeover_may_rekey`]): only the commits the log
/// held after its latest ACK at the open count (`ctx.unacked`), since only those may have been
/// sent, and a takeover's pending records are all such. A provisional open's own commits were
/// never sent: a history that loses nothing re-keys them. A bound continuation's acknowledged
/// state is the stream's, exact.
fn may_rekey(takeover: bool, ctx: &Ctx) -> bool {
    let unacked = if takeover && !ctx.pending.is_empty() {
        Unacked::Commits
    } else {
        ctx.unacked
    };
    takeover_may_rekey(unacked) || rekeys_with_pending(ctx)
}

/// Whether a takeover re-keys with commits pending: never, but under the negative control.
#[cfg(test)]
fn rekeys_with_pending(ctx: &Ctx) -> bool {
    ctx.knobs.rekey_with_pending
}

#[cfg(not(test))]
fn rekeys_with_pending(_ctx: &Ctx) -> bool {
    false
}

/// The negative control that settles tickets in every state, as before #604.
#[cfg(test)]
fn settle_while_activating(ctx: &mut Ctx, actions: &mut Vec<Action>) {
    if ctx.knobs.settle_while_activating {
        ctx.settle(actions);
    }
}

#[cfg(not(test))]
fn settle_while_activating(_ctx: &mut Ctx, _actions: &mut Vec<Action>) {}

/// Whether `exchange` says a claim of ours applied before: `receipt-evicted`, whose digest check
/// passed, so later writes under our claim evicted its receipt; or `producer-expired` naming its
/// epoch when the pre-check found that claim's frame (`found`), so the retired record is ours.
/// Otherwise `producer-expired` is no proof: the retired record of that epoch may be another
/// claimant's.
fn applied_before(exchange: &Exchange, found: bool) -> bool {
    let Exchange::Answered(answer) = exchange else {
        return false;
    };
    match &answer.typed {
        Ok(Typed::Producer(ProducerAnswer::ReceiptEvicted { .. })) => true,
        Ok(Typed::Producer(ProducerAnswer::ProducerExpired { .. })) => found,
        _ => false,
    }
}

/// Whether `exchange` is a `403 producer-expired`.
fn expired(exchange: &Exchange) -> bool {
    matches!(exchange, Exchange::Answered(answer) if matches!(
        &answer.typed,
        Ok(Typed::Producer(ProducerAnswer::ProducerExpired { .. }))
    ))
}

/// Whether a repeat receipt for a claim sent again after a re-key binds without the claim after
/// it: never, but under the negative control that leaves that claim out.
#[cfg(test)]
fn repeat_binds(ctx: &Ctx) -> bool {
    ctx.knobs.no_claim_after_repeat
}

#[cfg(not(test))]
fn repeat_binds(_ctx: &Ctx) -> bool {
    false
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
            let rekeyed = matches!(kind, ClaimKind::Takeover { rekeyed: true, .. });
            actions.push(Action::Rebuild {
                discard_from: *discard_from,
                salvage: rekeyed && discard_from.is_some(),
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
    // A claim under the current token applied: the stream was written under it.
    ctx.rekey_from = None;
    // The reconcile read what landed: the acknowledged state is the stream's from now on.
    ctx.unacked = Unacked::Nothing;
    ctx.log_claim = None;
    ctx.pre_checked = None;
    ctx.streamed = Some(done.epoch);
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
    if ctx.knobs.renumber
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
            | Event::Snapshotted { .. }
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
            match link.head_answer(exchange, now_ms, ctx, actions) {
                HeadOutcome::Waiting => {}
                HeadOutcome::ReKey { to, from } => return self.rekey(to, from, actions),
                HeadOutcome::Reset(kind) => return reset(kind, actions),
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
                    kind: ClaimKind::Continuation {
                        own: self.epoch,
                        resent: false,
                    },
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

    /// A HEAD of the append or the probe named a history that inherits the generation: the
    /// in-flight batch and the probe go, and a continuation claims `own + 1` under the new token.
    fn rekey(
        self,
        incarnation: Incarnation,
        from: Incarnation,
        actions: &mut Vec<Action>,
    ) -> Replicator {
        let kind = ClaimKind::Continuation {
            own: self.epoch,
            resent: false,
        };
        rekey(
            kind,
            Holding::Held(self.epoch),
            incarnation,
            from,
            self.ctx,
            actions,
        )
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
            let outcome = quietly(quiet, ctx, actions, |ctx, actions| {
                link.head_answer(exchange, now_ms, ctx, actions)
            });
            match outcome {
                HeadOutcome::Waiting => {}
                HeadOutcome::ReKey { to, from } => return self.rekey(to, from, actions),
                HeadOutcome::Reset(kind) => return reset(kind, actions),
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
            // Retention passed our acknowledged tail, or the read went to another history (its
            // `410` comes before the `412`): a HEAD decides. Our token still there: another owner
            // moved the stream on, `fenced`. Another token takes the history path.
            ReadDecision::Gone => {
                let result = quietly(quiet, ctx, actions, |ctx, actions| {
                    let common = Common::Head(HeadReason::Gone(ResetKind::Fenced));
                    link.common(common, None, now_ms, ctx, actions)
                });
                if let Err(kind) = result {
                    return reset(kind, actions);
                }
                self.probe = Some(Probe { from, link });
                self.into()
            }
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
            stream.ctx.settle(actions);
            stream.kick(now_ms, actions);
            stream.into()
        }
        Ok(Some(event)) => stream.on(event, now_ms, actions),
    }
}
