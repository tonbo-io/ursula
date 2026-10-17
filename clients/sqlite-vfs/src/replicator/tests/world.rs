//! The proptest world: the model server, owners (a replicator and a log model each), and a
//! network that delays, reorders, loses and duplicates requests and answers. Scripts mix commits,
//! deliveries, losses (before apply, after apply, of the answer), late duplicates, scripted
//! `5xx`/`429`/`408`/`307`/`401`/`402`/`403` and untyped `413` answers, a lowered request limit
//! (typed `413`), the clock moved past idle expiry, history events (a new history that loses
//! nothing, and restores of checkpoints), requests misrouted to another deployment, foreign
//! appends and claims, retention (and with it `410`s), clones (a memory snapshot restored with its
//! own disk: one activation, one epoch), copies of an owner's cache opened on another host (a new
//! activation over the copied log, durable or as it was, opening as a takeover or provisionally),
//! restarts (through the open table (design: Open): a restart whose history does not inherit the
//! log, or may have lost commits it holds after its latest ACK, salvages it and continues as a
//! fresh takeover), reopens that keep the activation (after a clone, two takeovers of one epoch),
//! provisional restarts (the open's first HEAD unanswered: a continuation of the log's latest
//! claim, bound before it applies), owners cut off from the network, power losses that drop a
//! log's unsynced trailing commits, takeovers by new owners and `synced()` calls, then a `Heal`
//! phase with a perfect network and the route restored. One seed in eight runs every instance
//! under one activation, as a platform passing a constant `activation` would.
//!
//! The monitors check, as the run goes:
//!
//! 1. acknowledged never regresses, per replicator;
//! 2. acknowledged records form a log prefix and appear in the stream in log order (L5);
//! 3. one lineage never sends two bodies under one key (per owner, clones aside: property 22);
//! 4. a new continuation claim is exactly one above the latest claim of its instance the server
//!    applied, the baseline of a provisional instance being the latest claim of its log the server
//!    applied (a claim sent again with the body of one its log held is not new);
//! 5. once a claim applies, no older key of the producer applies (within one history);
//! 6. a typed fence keeps pending records (only a takeover's divergence discards);
//! 7. after `Heal`, every run ends acknowledged, retrying, blocked or reset;
//! 8. of two divergent clone commits, at most one is acknowledged (commits at one commit id with
//!    other bytes diverge unless the later one was made after catching the earlier one up);
//! 9. `Synced` never fires while a pre-call foreign frame sits past the acknowledged offset, nor
//!    for a target later discarded (a claim is the owner's own only when its log holds that epoch
//!    and activation, so another copy's claim under a shared activation is foreign);
//! 10. the bytes between an owner's consecutive acknowledged offsets are its own frames (a claim
//!     as in 9), frames it caught up, or foreign claims in a takeover's reconcile or followed by a
//!     claim of its own before the one it binds with, unless it rebuilt (with one activation for
//!     every instance, any claim);
//! 11. after `Heal`, every ticket of an owner that ends acknowledged resolved or was abandoned;
//! 12. no append carries a frame the owner caught up;
//! 13. an owner resets `fenced` or `stale` only when another writer, owner or clone was on the
//!     stream, or `stale` at the first claim of a provisional instance after a power loss dropped
//!     commits from its log that may have reached the stream (design: Resets), `stale` when a claim
//!     sent again after a pre-check met `producer-expired`, or `fenced` when a provisional
//!     instance's log holds no claim that ever applied;
//! 14. a boundary's receipt names the end of its record's frame in the stream with only claims
//!     after it, and a receipt without an offset publishes no snapshot;
//! 15. a re-key happens only when the history inherits the generation: every record the owner
//!     acknowledged is in the stream the HEAD that decided it saw, in log order;
//! 16. nothing applies on the other deployment;
//! 17. a misrouted answer never resets an owner, and after `Heal` no owner ends blocked
//!     `wrong_cluster`;
//! 18. a rebuild that discards pending records of a log re-keyed at open (and not bound since,
//!     across restarts) carries `salvage: true`, records a provisional open logged after its open
//!     apart (never sent under the old token);
//! 19. after `Heal`, every commit an owner was told is acknowledged is in the stream, in a log,
//!     or in a salvage: none is lost, whatever history changes, clones and copies came (commits a
//!     power loss dropped from a log before an fsync apart);
//! 20. a background claim (a continuation's, a provisional open's) never applies above a claim an
//!     explicit open of another owner made past the background owner's acknowledged offset and its
//!     own latest claim (design: Claims, its last paragraph), unless that claim landed after a
//!     provisional instance's pre-check read the tail (the round trip the pre-check accepts,
//!     design: Pre-check), or that claim carries an activation the background instance holds
//!     (whose claims are all its own, design: Claims: a shared activation);
//! 21. an owner resets `stream_state` only when another writer, owner or clone was on the stream
//!     (the gate of property 13);
//! 22. within one history, every producer key the server acknowledged (a receipt, applied or a
//!     repeat) carries one body, whichever owner, clone or copy sent it.
//!
//! Snapshots carry real `USS3` headers (the owner's lineage, an acknowledged position and its
//! offset): the `410` rule reads them.
//!
//! Negative controls must fail them: the model with digest comparison off (requests reach it
//! without `Producer-Digest`), a replicator that re-seals its in-flight batch on resend, one that
//! re-keys on any other token, one that binds on a repeat receipt for a claim sent again after a
//! re-key, one that takes another cluster's HEAD for a deleted stream, one that re-keys a log
//! holding commits after its latest ACK (the #611 history gate undone, design: History), one that
//! settles `synced()` tickets while activating (as before #604), one that resets `stream_state` on a
//! repeat receipt for a claim sent again that ends at or below the acknowledged offset, a
//! provisional open that claims at once, above its log's highest CLAIM record, with no pre-check,
//! one that trusts a clone's snapshot of our lineage above the acknowledged position, one that
//! matches a reconcile's commits by commit id alone, and a `synced()` probe that skips claims of an
//! activation in its log.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use proptest::prelude::Just;
use proptest::prelude::Strategy;
use proptest::prop_oneof;
use proptest::test_runner::Config;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestCaseError;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;
use ursula_headers::BodyDigest;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;
use ursula_headers::ProducerAnswer;
use ursula_headers::Seq;

use super::cluster;
use crate::codec::digest::SnapshotDigester;
use crate::codec::digest::producer_digest;
use crate::codec::frame;
use crate::codec::frame::CONTENT_TYPE as STREAM_CONTENT_TYPE;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::snapshot::SnapshotFields;
use crate::codec::snapshot::write_body;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::model::CheckpointId;
use crate::model::Delivered;
use crate::model::HistoryEvent;
use crate::model::Limits;
use crate::model::ModelServer;
use crate::model::faults::Fault;
use crate::model::faults::Scripted;
use crate::model::producer::IDLE_RETIREMENT_MS;
use crate::open::table::Asked;
use crate::open::table::HeadSeen;
use crate::open::table::Local;
use crate::open::table::LocalSummary;
use crate::open::table::OpenRow;
use crate::open::table::Superseded;
use crate::open::table::UnreachableRow;
use crate::open::table::Why;
use crate::open::table::decide;
use crate::open::table::decide_unreachable;
use crate::replicator::Action;
use crate::replicator::BlockedReason;
use crate::replicator::BoundaryReceipt;
use crate::replicator::ClaimKind;
use crate::replicator::Condition;
use crate::replicator::Event;
use crate::replicator::Knobs as ReplicatorKnobs;
use crate::replicator::LocalState;
use crate::replicator::MAX_BATCH_BYTES;
use crate::replicator::Pending;
use crate::replicator::PreCheck;
use crate::replicator::Replicator;
use crate::replicator::RequestId;
use crate::replicator::ResetKind;
use crate::replicator::Step;
use crate::replicator::Work;
use crate::replicator::answer::Unacked;
use crate::replicator::answer::inherits;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::Typed;

const PATH: &str = "/v1/stream/b/db";
const MAX_OWNERS: usize = 6;
/// The longest virtual time the heal phase runs.
const HEAL_MS: u64 = 30 * 60 * 1000;
/// The default case count; `PROPTEST_CASES` overrides it.
const CASES: u32 = 1024;

/// The deterministic cases of the coverage check.
const COVERAGE_CASES: u32 = 4096;

/// A scripted answer of a layer in front of the server.
#[derive(Debug, Clone, Copy)]
enum FaultKind {
    Status(u16, Option<u64>),
    Redirect,
    UntypedTooLarge,
    Unreachable,
}

#[derive(Debug, Clone)]
enum Op {
    Commit(u8),
    Deliver(u16),
    /// Deliver every message in order.
    Flush,
    /// A request lost before the server.
    DropRequest(u16),
    /// A request applied, its answer lost.
    DropAnswerAfterApply(u16),
    /// An answer lost on the way back.
    DropAnswer(u16),
    /// A copy of a request reaches the server later; its answer goes nowhere.
    Duplicate(u16),
    Fault(FaultKind, bool, u8),
    LowerLimit,
    Advance(u32),
    ExpireIdle,
    Tick(u8),
    /// A history event that loses nothing: every token changes, every byte stays.
    NewHistory,
    /// A history event restoring the `n`-th latest checkpoint.
    Restore(u8),
    Checkpoint,
    /// Requests go to another deployment (cluster `cluster-b`) while `on`; with `exists` it holds a
    /// stream at the same path.
    Misroute(bool, bool),
    /// Retention moves to an owner's acknowledged offset, as its snapshot thread would.
    Retain(u8),
    /// An unkeyed append by another writer: garbage (0), a COMMIT (1) or a CLAIM frame (2).
    ForeignAppend(u8),
    ForeignClaim(bool),
    Clone(u8),
    /// Another host opens a copy of the owner's cache: a new owner with the log as copied (with
    /// `durable`, only what an fsync made durable), a fresh activation and commits of its own
    /// bytes, opening as a takeover, or provisionally when `offline`. The original keeps running.
    CopyCache(u8, bool, bool),
    Restart(u8, u8),
    /// The owner reopens in its process with the activation it had, as a platform passing one
    /// `activation` does: after a clone, both copies take over at one epoch.
    Reopen(u8),
    /// The owner restarts and its open's first HEAD gets no answer with nothing sent: the open
    /// table's unreachable rows decide (design: Open), with the ACK it reads lagging by `lag`.
    ProvisionalRestart(u8, u8),
    /// The owner's requests fail before connecting while `on`.
    Partition(u8, bool),
    /// The owner's host loses power: up to `k` of the log's unsynced trailing commits are gone,
    /// with the ACK records after them, and its process stops until a restart.
    PowerLoss(u8, u8),
    /// A new owner on a fresh host takes over; with `true` its build lags, from offset 0.
    Takeover(bool),
    Sync(u8),
    /// The owner's snapshot thread asks for a batch ending at its last logged commit; its
    /// receipt publishes a snapshot there (property 14).
    Boundary(u8),
}

fn op() -> impl Strategy<Value = Op> {
    let fault = prop_oneof![
        Just(FaultKind::Status(503, Some(1))),
        Just(FaultKind::Status(500, None)),
        Just(FaultKind::Status(429, Some(2))),
        Just(FaultKind::Status(408, None)),
        Just(FaultKind::Status(401, None)),
        Just(FaultKind::Status(402, None)),
        Just(FaultKind::Status(403, None)),
        Just(FaultKind::Redirect),
        Just(FaultKind::UntypedTooLarge),
        Just(FaultKind::Unreachable),
    ];
    prop_oneof![
        10 => (0u8..8).prop_map(Op::Commit),
        24 => (0u16..64).prop_map(Op::Deliver),
        3 => Just(Op::Flush),
        3 => (0u16..64).prop_map(Op::DropRequest),
        3 => (0u16..64).prop_map(Op::DropAnswerAfterApply),
        3 => (0u16..64).prop_map(Op::DropAnswer),
        3 => (0u16..64).prop_map(Op::Duplicate),
        3 => (fault, proptest::bool::ANY, 1u8..3).prop_map(|(f, post, n)| Op::Fault(f, post, n)),
        1 => Just(Op::LowerLimit),
        4 => (0u32..40_000).prop_map(Op::Advance),
        1 => Just(Op::ExpireIdle),
        6 => (0u8..8).prop_map(Op::Tick),
        1 => Just(Op::NewHistory),
        1 => (0u8..4).prop_map(Op::Restore),
        1 => Just(Op::Checkpoint),
        1 => (proptest::bool::ANY, proptest::bool::ANY).prop_map(|(on, exists)| Op::Misroute(on, exists)),
        2 => (0u8..8).prop_map(Op::Retain),
        2 => (0u8..3).prop_map(Op::ForeignAppend),
        1 => proptest::bool::ANY.prop_map(Op::ForeignClaim),
        1 => (0u8..8).prop_map(Op::Clone),
        1 => ((0u8..8), proptest::bool::ANY, proptest::bool::ANY)
            .prop_map(|(o, durable, offline)| Op::CopyCache(o, durable, offline)),
        1 => (0u8..8).prop_map(Op::Reopen),
        2 => ((0u8..8), (0u8..3)).prop_map(|(o, lag)| Op::Restart(o, lag)),
        2 => ((0u8..8), (0u8..3)).prop_map(|(o, lag)| Op::ProvisionalRestart(o, lag)),
        2 => ((0u8..8), proptest::bool::ANY).prop_map(|(o, on)| Op::Partition(o, on)),
        1 => ((0u8..8), (1u8..4)).prop_map(|(o, k)| Op::PowerLoss(o, k)),
        1 => proptest::bool::ANY.prop_map(Op::Takeover),
        5 => (0u8..8).prop_map(Op::Sync),
        2 => (0u8..8).prop_map(Op::Boundary),
    ]
}

/// The negative controls.
#[derive(Debug, Clone, Copy, Default)]
struct Knobs {
    /// Requests reach the model without `Producer-Digest`.
    strip_digests: bool,
    /// A provisional open claims one above its log's highest CLAIM record at once, with no
    /// pre-check (this PR's first revision).
    claim_without_pre_check: bool,
    /// Every instance runs under one activation, as a platform passing a constant `activation`
    /// would: not a negative control but a configuration, drawn from the seed in [`check`].
    shared_activation: bool,
    /// The replicators' own.
    replicator: ReplicatorKnobs,
}

/// What an owner's log holds.
#[derive(Debug, Clone)]
struct LogModel {
    lineage: Lineage,
    commits: BTreeMap<u64, Pending>,
    claims: Vec<(Epoch, Activation)>,
    /// `(acked, acked_offset, log_bytes)` of every ACK record.
    acks: Vec<(Position, Offset, u64)>,
    incarnation: Incarnation,
    next_position: u64,
    /// Every frame a catch-up applied.
    applied: Vec<Arc<[u8]>>,
    /// ACK records from this index on are durable: a catch-up fsyncs its ACK.
    durable_acks: usize,
    /// The next ACK record is a catch-up's.
    catching_up: bool,
    /// The last position when a rebuild published: the frames it brought in take no positions
    /// in this model (the driver logs each at the next one).
    rebuilt_through: u64,
    /// The stream's bytes the latest rebuild brought in: its db holds those commits.
    rebuilt: Arc<[u8]>,
    /// The log rolled to a new token at open (or by a provisional open before its bind), and no
    /// claim bound under it since: the last position logged then (property 18). Commits logged
    /// after the roll were never sent under the old token.
    rekeyed_at_open: Option<u64>,
    /// Every COMMIT record up to this position is durable: an fsync of the log (a CLAIM record, a
    /// re-key, a catch-up, a `rewound` reset, a snapshot's ACK) came after it.
    durable_through: u64,
}

impl LogModel {
    fn last_logged(&self) -> Position {
        Position(self.next_position.saturating_sub(1))
    }

    /// The log is fsynced: every record so far is durable.
    fn sync(&mut self) {
        self.durable_acks = self.acks.len();
        self.durable_through = self.last_logged().0;
    }

    /// Drops up to `k` of the unsynced trailing commits, with the ACK records after them, as a
    /// power loss does: the frames dropped. The next commits at those positions carry other bytes.
    fn drop_unsynced(&mut self, k: usize) -> Vec<Arc<[u8]>> {
        let lost: Vec<u64> = self
            .commits
            .range(self.durable_through + 1..)
            .map(|(p, _)| *p)
            .rev()
            .take(k)
            .collect();
        let Some(&first) = lost.last() else {
            return Vec::new();
        };
        let dropped: Vec<Arc<[u8]>> = lost
            .iter()
            .filter_map(|position| self.commits.remove(position))
            .map(|record| record.frame)
            .collect();
        let durable = self.durable_acks;
        let mut at = 0;
        self.acks.retain(|(acked, _, _)| {
            let keep = at < durable || acked.0 < first;
            at += 1;
            keep
        });
        let last = self.commits.keys().last().copied().unwrap_or(0);
        self.next_position = last.max(self.durable_through).max(self.rebuilt_through) + 1;
        dropped
    }
}

/// A `synced()` call: what the stream held at the call.
#[derive(Debug, Clone)]
struct Call {
    target: Position,
    stream: Vec<u8>,
    token: Option<Incarnation>,
    discards: usize,
}

/// One replicator's lifetime.
#[derive(Debug, Clone)]
struct Instance {
    uid: u64,
    replicator: Replicator,
    timer: Option<u64>,
    start_acked: Position,
    acked: Position,
    acked_offset: Offset,
    calls: BTreeMap<u64, Call>,
    discards: Vec<Position>,
    /// A rebuild read the stream up to the next acknowledged offset.
    rebuilt: bool,
    /// Reads applied in place by this takeover.
    applies: u32,
    /// The `CaughtUp` the driver sends for the last Apply or Rebuild, delivered at the end of the
    /// next op, so restarts, clock steps and late answers land while the replicator waits.
    caught_up_due: Option<Position>,
    /// The clock passed idle expiry while this owner's continuation claim was outstanding and
    /// already in the stream: the floor rose past its own claim, and it loses by design.
    claim_expired: bool,
    /// A rebuild's generation, published when its `CaughtUp` is delivered: only then do the
    /// discarded tail (from this position) and the new lineage replace the old generation. A
    /// restart before that finds the old generation whole, as a crash before the publishing
    /// rename does (design: Layout, Materialize).
    unpublished: Option<Option<Position>>,
    /// The token a rebuild reads under, and the stream's bytes up to its claim, which its
    /// generation holds once published.
    rebuilt_bytes: Option<(Incarnation, Arc<[u8]>)>,
    /// The replicator reported itself blocked `wrong_cluster` and has not resumed since.
    blocked_wrong_cluster: bool,
    /// The highest epoch of this instance's claims the world saw the server receipt (applied, or
    /// a repeat of the same claim).
    claim_applied: Option<Epoch>,
    /// The epochs of this instance's claims sent so far.
    claims_sent: Vec<Epoch>,
    /// Opened provisionally, and no claim of it has bound yet.
    provisional: bool,
    /// The last position a provisional instance's log held at its open (property 18).
    opened_through: Option<u64>,
    /// The claims its log held when it started: a claim sent with one of their bodies is sent
    /// again, not new (property 4).
    inherited: Vec<(Epoch, Activation)>,
    /// The tail the last HEAD of a provisional instance's pre-check saw, up to which it reads
    /// (property 20).
    checked_tail: Option<usize>,
    /// The claim a provisional instance sent again after its pre-check met `producer-expired`
    /// naming its epoch, before its first bind.
    expired: bool,
    /// The stream as the last HEAD of a pre-check saw it: a re-key it decided rolls the log only
    /// with the claim, after the reads (property 15).
    pre_check_head: Option<Vec<u8>>,
    /// The open of a copy of another owner's cache, until its first bind (coverage).
    copied: bool,
}

/// What the scripts of one [`check`] run reached, beyond the properties.
#[derive(Debug, Default)]
struct Coverage {
    /// Takeovers that caught up in place over two reads or more.
    chunked_catch_ups: AtomicU64,
    /// Takeovers that rebuilt after applying a read in place (grown past the database, or a
    /// `410`).
    rebuilds_after_apply: AtomicU64,
    /// Re-keys of a streaming owner.
    rekeys_streaming: AtomicU64,
    /// Re-keys while a continuation claim was out.
    rekeys_claiming: AtomicU64,
    /// Re-keys at open, or during a takeover's claim.
    rekeys_open: AtomicU64,
    /// Resets `rewound`.
    rewound: AtomicU64,
    /// Owners blocked `wrong_cluster` that resumed.
    blocks_resumed: AtomicU64,
    /// Restarts that salvaged the log and continued as a fresh takeover.
    salvages: AtomicU64,
    /// Provisional opens whose claim bound.
    provisional_live: AtomicU64,
    /// Provisional opens reset `fenced`.
    provisional_fenced: AtomicU64,
    /// Provisional opens reset `stale`.
    provisional_stale: AtomicU64,
    /// Owners sharing a lineage with another, fenced by `key-conflict`.
    clones_fenced_key_conflict: AtomicU64,
    /// Owners sharing a lineage with another, fenced by `receipt-evicted`.
    clones_fenced_receipt_evicted: AtomicU64,
    /// Continuation claims won by a repeat on their first attempt (another copy's identical claim).
    repeats_won_continuation: AtomicU64,
    /// Takeover claims won by a repeat on their first attempt (another copy's identical claim).
    repeats_won_takeover: AtomicU64,
    /// Copies of a cache that took over.
    copies_took_over: AtomicU64,
    /// Copies of a cache opened offline that went live.
    offline_copies_live: AtomicU64,
    /// Copies of a cache opened offline reset `stale`.
    offline_copies_stale: AtomicU64,
    /// Copies of a cache opened offline reset `fenced`.
    offline_copies_fenced: AtomicU64,
    /// Snapshots of another copy of our lineage, above our acknowledged position, that the `410`
    /// rule read (and never trusts).
    clone_snapshots_rejected: AtomicU64,
    /// Restores while two owners share a lineage.
    restores_shared_lineage: AtomicU64,
}

/// A refusal that fences an owner sharing its lineage with another (coverage).
#[derive(Debug, Clone, Copy)]
enum Fence {
    KeyConflict,
    ReceiptEvicted,
}

/// What an answer may reach for the coverage counters, seen before the replicator takes it.
#[derive(Debug, Clone, Copy)]
struct Watch {
    /// A refusal that may fence an owner sharing its lineage.
    fence: Option<Fence>,
    /// A repeat for the first attempt of a claim: `true` for a continuation's.
    won: Option<bool>,
    /// A snapshot read of the `410` rule, the snapshot another copy's of the lineage.
    rejected: bool,
}

/// What the replicator was doing when an event came, and where its answer came from.
#[derive(Debug, Clone, Default)]
struct Before {
    streaming: bool,
    /// A continuation claim was out.
    claiming: bool,
    takeover: bool,
    /// The stream as the HEAD this event answers saw it, if it answers one.
    head_saw: Option<Vec<u8>>,
    /// The answer came from the other deployment.
    misrouted: bool,
}

#[derive(Debug, Clone)]
struct Owner {
    family: u64,
    salt: u8,
    log: LogModel,
    inst: Instance,
    /// A power loss dropped commits from its log: the stream may hold them, so a reconcile may find
    /// them with no record to match (property 13's exemption).
    power_lost: bool,
    /// The activation of its latest open: a reopen keeps it.
    activation: Activation,
}

#[derive(Debug, Clone)]
enum Msg {
    Request {
        to: u64,
        id: RequestId,
        request: Box<Request>,
        late: bool,
    },
    Answer {
        to: u64,
        id: RequestId,
        exchange: Exchange,
        misrouted: bool,
    },
}

/// What a restart read of an owner's log.
struct Reopened {
    acked: Position,
    acked_offset: Offset,
    log_bytes: u64,
    pending: Vec<Pending>,
    unacked: Unacked,
    log: LogModel,
}

impl Reopened {
    /// What the replicator starts from, under `incarnation`.
    fn local(
        self,
        activation: Activation,
        incarnation: Incarnation,
        max_request_bytes: u64,
        family: u64,
        rekey_from: Option<Incarnation>,
    ) -> LocalState {
        let log = self.log;
        let last = log.commits.keys().last().copied().unwrap_or(0);
        LocalState {
            lineage: log.lineage,
            activation,
            cluster_id: cluster(),
            incarnation,
            acked: self.acked,
            acked_offset: self.acked_offset,
            log_bytes: self.log_bytes,
            logged: Position(last.max(self.acked.0)),
            pending: self.pending,
            claims: log.claims,
            highest_epoch: None,
            max_request_bytes,
            configured_max_request_bytes: None,
            db_bytes: db_bytes(family),
            rekey_from,
            unacked: self.unacked,
        }
    }
}

struct World {
    knobs: Knobs,
    server: ModelServer,
    /// Another deployment serving the same path, where misrouted requests go.
    other: ModelServer,
    misrouted: bool,
    /// The stream each outstanding HEAD saw when the server answered it, by owner and attempt.
    heads: HashMap<(u64, RequestId), Vec<u8>>,
    /// The clones that share each request an instance had out when they were cloned from it: what
    /// the server applies of it counts for them too (property 4).
    shared: HashMap<(u64, RequestId), Vec<u64>>,
    owners: Vec<Owner>,
    msgs: Vec<Msg>,
    next_uid: u64,
    next_family: u64,
    next_activation: u8,
    next_ticket: u64,
    checkpoints: Vec<(CheckpointId, Option<Epoch>)>,
    /// The highest epoch of an applied claim, in the current history line.
    max_claimed: Option<Epoch>,
    /// The highest epoch any answer named, for foreign claims.
    max_epoch: u64,
    /// Every key each owner family sent, with the digest.
    keys: HashMap<u64, HashMap<(u64, u64), BodyDigest>>,
    /// The frame acknowledged at every commit id.
    acked_frames: HashMap<CommitId, Arc<[u8]>>,
    violations: Vec<String>,
    /// Another writer (a foreign append or claim) wrote on the stream.
    others: bool,
    /// The commit frames of every log a restart salvaged.
    salvaged: Vec<Arc<[u8]>>,
    /// The commit frames power losses dropped from logs, unsynced: outside property 19.
    power_lost: Vec<Arc<[u8]>>,
    /// Every commit frame an owner was told its stream acknowledged (property 19).
    acknowledged: Vec<Arc<[u8]>>,
    /// The body digest of every producer key the server acknowledged, by history (property 22).
    bodies: HashMap<(Incarnation, Epoch, Seq), BodyDigest>,
    /// The first attempt of every claim an instance sent, by instance and attempt, and whether it
    /// was a continuation's (coverage).
    first_claims: HashMap<(u64, RequestId), bool>,
    /// The last commit each published snapshot names, and the family of the owner that published
    /// it (coverage).
    snapshots: Vec<(CommitId, u64)>,
    /// The families whose requests fail before connecting.
    partitioned: BTreeSet<u64>,
    /// The family of every instance that sent a request, so a dead instance's requests of a
    /// partitioned owner are cut too.
    families: HashMap<u64, u64>,
    /// Every claim the server took (applied, or a repeat), by epoch and activation.
    applied_claims: BTreeSet<(Epoch, Activation)>,
    /// The claims explicit opens (takeovers) sent (property 20).
    explicit: BTreeSet<(Epoch, Activation)>,
    /// The claim requests continuations sent, by instance and attempt (property 20).
    background: BTreeSet<(u64, RequestId)>,
    coverage: Arc<Coverage>,
}

fn decode_all(bytes: &[u8]) -> Vec<(usize, usize, Option<Frame>)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        match frame::decode(bytes.get(at..).unwrap_or_default(), u64::MAX) {
            Ok(Decoded::Frame { frame, len }) => {
                out.push((at, len, Some(frame)));
                at += len;
            }
            _ => {
                out.push((at, bytes.len() - at, None));
                break;
            }
        }
    }
    out
}

/// An owner's db file size: from empty (every catch-up in place rebuilds) to unbounded (none
/// does), with sizes in between that frames grow past midway.
fn db_bytes(family: u64) -> u64 {
    match family % 4 {
        0 => 0,
        // Records are 3 to 9 bytes: room for one or two, so the next read rebuilds.
        1 => 8,
        2 => 16,
        _ => u64::MAX,
    }
}

impl World {
    fn new(knobs: Knobs, seed: u64, coverage: Arc<Coverage>) -> Self {
        let mut server = ModelServer::new(cluster(), seed);
        // Small reads too, so a reconcile catches up in place over several of them.
        server.set_limits(Limits {
            read_chunk: [48, 160, 1 << 20][usize::try_from(seed % 3).unwrap()],
            ..server.limits()
        });
        let create = Request {
            content_type: Some(CONTENT_TYPE.to_owned()),
            ..Request::new(Method::Put, Target::Stream)
        };
        let created = server.handle(PATH, &create);
        assert_eq!(created.status, 201);
        let first = server.checkpoint();
        let other = ModelServer::new(
            ursula_headers::ClusterId::new("cluster-b").unwrap(),
            seed.wrapping_add(1),
        );
        let mut world = Self {
            knobs,
            server,
            other,
            misrouted: false,
            heads: HashMap::new(),
            shared: HashMap::new(),
            owners: Vec::new(),
            msgs: Vec::new(),
            next_uid: 1,
            next_family: 1,
            next_activation: 1,
            next_ticket: 1,
            checkpoints: vec![(first, None)],
            max_claimed: None,
            max_epoch: 0,
            keys: HashMap::new(),
            acked_frames: HashMap::new(),
            violations: Vec::new(),
            others: false,
            salvaged: Vec::new(),
            power_lost: Vec::new(),
            acknowledged: Vec::new(),
            bodies: HashMap::new(),
            first_claims: HashMap::new(),
            snapshots: Vec::new(),
            partitioned: BTreeSet::new(),
            families: HashMap::new(),
            applied_claims: BTreeSet::new(),
            explicit: BTreeSet::new(),
            background: BTreeSet::new(),
            coverage,
        };
        world.takeover(false);
        world
    }

    fn now(&self) -> u64 {
        self.server.now_ms()
    }

    fn token(&self) -> Incarnation {
        self.server.token(PATH).expect("the stream exists")
    }

    fn stream(&self) -> Vec<u8> {
        self.server.frames(PATH).unwrap_or_default().to_vec()
    }

    /// The activation of a new open: its own, or the one every instance shares.
    fn activation(&mut self) -> Activation {
        if self.knobs.shared_activation {
            return Activation([0xa5; 16]);
        }
        self.next_activation = self.next_activation.wrapping_add(1);
        Activation([self.next_activation; 16])
    }

    fn uid(&mut self) -> u64 {
        self.next_uid += 1;
        self.next_uid
    }

    /// A takeover's instance.
    fn instance(&mut self, local: LocalState) -> (Instance, Vec<Action>) {
        let kind = ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        };
        self.activated(kind, local)
    }

    /// An instance whose replicator claims as `kind`.
    fn activated(&mut self, kind: ClaimKind, local: LocalState) -> (Instance, Vec<Action>) {
        self.started(local, |local| Replicator::activate(kind, local))
    }

    /// An instance whose replicator `start` starts.
    fn started(
        &mut self,
        local: LocalState,
        start: impl FnOnce(LocalState) -> (Replicator, Vec<Action>),
    ) -> (Instance, Vec<Action>) {
        let start_acked = local.acked;
        let acked_offset = local.acked_offset;
        let inherited = local.claims.clone();
        let (mut replicator, actions) = start(local);
        replicator.set_knobs(self.knobs.replicator);
        let inst = self.stopped(start_acked, acked_offset);
        (
            Instance {
                replicator,
                inherited,
                ..inst
            },
            actions,
        )
    }

    /// An instance whose open was refused: no replicator runs.
    fn stopped(&mut self, start_acked: Position, acked_offset: Offset) -> Instance {
        Instance {
            uid: self.uid(),
            replicator: Replicator::Stopped,
            timer: None,
            start_acked,
            acked: start_acked,
            acked_offset,
            calls: BTreeMap::new(),
            discards: Vec::new(),
            rebuilt: false,
            applies: 0,
            caught_up_due: None,
            claim_expired: false,
            unpublished: None,
            rebuilt_bytes: None,
            blocked_wrong_cluster: false,
            claim_applied: None,
            claims_sent: Vec::new(),
            provisional: false,
            opened_through: None,
            inherited: Vec::new(),
            checked_tail: None,
            expired: false,
            pre_check_head: None,
            copied: false,
        }
    }

    /// The deployment requests reach now.
    fn routed(&mut self) -> &mut ModelServer {
        if self.misrouted {
            &mut self.other
        } else {
            &mut self.server
        }
    }

    fn limits(&self) -> u64 {
        self.server.limits().max_request_bytes
    }

    /// A new owner on a fresh host: it built the stream up to its tail and takes over.
    fn takeover(&mut self, lagging: bool) {
        if self.owners.len() >= MAX_OWNERS {
            return;
        }
        let activation = self.activation();
        let (owner, actions) = self.fresh_owner(lagging, activation);
        self.owners.push(owner);
        let index = self.owners.len() - 1;
        let before = Before {
            takeover: true,
            ..Before::default()
        };
        self.process(index, actions, before);
    }

    /// An owner of a new family whose generation was built from the stream up to its tail (from
    /// offset 0 when `lagging`), and the actions of its takeover under `activation`.
    fn fresh_owner(&mut self, lagging: bool, activation: Activation) -> (Owner, Vec<Action>) {
        let family = self.next_family;
        self.next_family += 1;
        let lineage = Lineage([u8::try_from(family % 250).unwrap() + 1; 16]);
        let tail = if lagging {
            Offset::ZERO
        } else {
            Offset::from_u64(u64::try_from(self.stream().len()).unwrap())
        };
        // A build notes the highest epoch of the claims it read (`open::Fetch`), so its takeover
        // claims above every claim in the stream, its own earlier generation's under the same
        // activation too.
        let highest_epoch = if lagging {
            None
        } else {
            decode_all(&self.stream())
                .into_iter()
                .filter_map(|(_, _, frame)| match frame {
                    Some(Frame::Claim { epoch, .. }) => Some(epoch),
                    _ => None,
                })
                .max()
        };
        let log = LogModel {
            lineage,
            commits: BTreeMap::new(),
            claims: Vec::new(),
            acks: vec![(Position(0), tail, 0)],
            incarnation: self.token(),
            next_position: 1,
            applied: Vec::new(),
            durable_acks: 0,
            catching_up: false,
            rebuilt_through: 0,
            rebuilt: Arc::from(Vec::new()),
            rekeyed_at_open: None,
            durable_through: 0,
        };
        let local = LocalState {
            lineage: log.lineage,
            activation,
            cluster_id: cluster(),
            incarnation: log.incarnation,
            acked: Position(0),
            acked_offset: tail,
            log_bytes: 0,
            logged: Position(0),
            pending: Vec::new(),
            claims: Vec::new(),
            highest_epoch,
            max_request_bytes: self.limits(),
            configured_max_request_bytes: None,
            db_bytes: db_bytes(family),
            rekey_from: None,
            unacked: Unacked::Nothing,
        };
        let (inst, actions) = self.instance(local);
        let owner = Owner {
            family,
            salt: u8::try_from(family % 250).unwrap(),
            log,
            inst,
            power_lost: false,
            activation,
        };
        (owner, actions)
    }

    /// What the owner's log holds when its process restarts and reads it, the ACK it reads lagging
    /// by `lag`: the ACK records the crash lost are gone for good, so the next restart never reads
    /// them.
    fn reopened(&mut self, index: usize, lag: u8) -> (Reopened, LocalSummary) {
        let log = self.owners[index].log.clone();
        let lagged = log
            .acks
            .len()
            .saturating_sub(1 + usize::from(lag))
            .max(log.durable_acks.saturating_sub(1));
        let (acked, acked_offset, log_bytes) = log
            .acks
            .get(lagged)
            .copied()
            .expect("a log starts with the ACK of its build");
        self.owners[index].log.acks.truncate(lagged + 1);
        let pending: Vec<Pending> = log
            .commits
            .range(acked.0 + 1..)
            .map(|(_, p)| p.clone())
            .collect();
        // The world's log holds COMMIT records (own, and caught up) only up to its ACKs or in
        // the pending tail.
        let unacked = if pending.is_empty() {
            Unacked::Nothing
        } else {
            Unacked::Commits
        };
        let summary = LocalSummary {
            cluster_id: cluster().as_str().to_owned(),
            incarnation: log.incarnation,
            acked_offset,
            pending: unacked,
            holds_data: !log.commits.is_empty() || log.rebuilt_through > 0,
            claim: log.claims.last().copied(),
        };
        let reopened = Reopened {
            acked,
            acked_offset,
            log_bytes,
            pending,
            unacked,
            log,
        };
        (reopened, summary)
    }

    /// The owner's process restarts and reopens from its log; the ACK it reads lags by `lag`. Its
    /// open's HEAD goes through the open table: the same token resumes, an inheriting history
    /// re-keys a log with nothing pending, any other history salvages the log and the owner
    /// continues as a fresh takeover, and a refusal runs no replicator. An owner cut off from the
    /// network gets no answer to that HEAD, and reopens provisionally. With `keep` the open runs
    /// under the activation the owner had.
    fn restart(&mut self, index: usize, lag: u8, keep: bool) {
        if self.partitioned.contains(&self.owners[index].family) {
            return self.provisional_restart(index, lag, keep);
        }
        let activation = self.open_activation(index, keep);
        let family = self.owners[index].family;
        let (reopened, summary) = self.reopened(index, lag);
        let head = Request::new(Method::Head, Target::Stream);
        let answer = self.routed().handle(PATH, &head).parse(&head);
        let (identity, stream) = match &answer.typed {
            Ok(Typed::Head { identity, stream }) => (identity.clone(), *stream),
            other => panic!("a HEAD answers a HEAD, not {other:?}"),
        };
        let seen = HeadSeen {
            identity: identity.as_ref(),
            stream: stream.as_ref(),
            content_type: answer.content_type.as_deref(),
        };
        let local = Local::<(), ()>::Trusted(summary.clone(), ());
        let row = match decide(local, Asked::default(), seen) {
            // The negative control: an inheriting history re-keys a log with commits pending.
            OpenRow::Build {
                superseded:
                    Superseded::Salvage {
                        why: Why::HistoryChanged,
                        ..
                    },
                cluster_id,
                head,
            } if self.knobs.replicator.rekey_with_pending
                && inherits(&head, summary.incarnation, summary.acked_offset) =>
            {
                OpenRow::ReKey {
                    local: (),
                    from: summary.incarnation,
                    cluster_id,
                    head,
                }
            }
            row => row,
        };
        let (incarnation, rekey_from) = match row {
            OpenRow::Resume { .. } => (reopened.log.incarnation, None),
            OpenRow::ReKey { from, head, .. } => (head.incarnation(), Some(from)),
            OpenRow::Refuse { .. } => {
                let inst = self.stopped(reopened.acked, reopened.acked_offset);
                self.owners[index].inst = inst;
                return;
            }
            OpenRow::Build { superseded, .. } => {
                if matches!(superseded, Superseded::Salvage { .. }) {
                    self.coverage.salvages.fetch_add(1, Ordering::Relaxed);
                    // The salvage holds what the log holds, and what its db holds.
                    let log = &reopened.log;
                    self.salvaged
                        .extend(log.commits.values().map(|p| Arc::clone(&p.frame)));
                    self.salvaged.extend(log.applied.iter().cloned());
                    self.salvaged.push(Arc::clone(&log.rebuilt));
                }
                let (owner, actions) = self.fresh_owner(false, activation);
                self.owners[index] = owner;
                let before = Before {
                    takeover: true,
                    ..Before::default()
                };
                self.process(index, actions, before);
                return;
            }
            row @ OpenRow::Create { .. } => {
                panic!("the world's stream is never missing, yet the open took row {row:?}")
            }
        };
        let limit = self.limits();
        let local = reopened.local(activation, incarnation, limit, family, rekey_from);
        let (inst, actions) = self.instance(local);
        self.owners[index].inst = inst;
        self.owners[index].activation = activation;
        let before = Before {
            takeover: true,
            head_saw: Some(self.routed().frames(PATH).unwrap_or_default().to_vec()),
            ..Before::default()
        };
        self.process(index, actions, before);
    }

    /// The owner's process restarts and its open's first HEAD gets no answer with nothing sent
    /// (design: Open): a log with a CLAIM record binds at once under the log's token, and once the
    /// stream answers its pre-check (design: Pre-check) decides whether that claim goes out again.
    /// A log without one waits, and the open's signal fires: no replicator runs. With `keep` the
    /// open runs under the activation the owner had.
    fn provisional_restart(&mut self, index: usize, lag: u8, keep: bool) {
        let activation = self.open_activation(index, keep);
        let family = self.owners[index].family;
        let (reopened, summary) = self.reopened(index, lag);
        let claim = match decide_unreachable(Local::<(), ()>::Trusted(summary, ())) {
            UnreachableRow::Provisional { claim, .. } => claim,
            UnreachableRow::Wait { .. } => {
                let inst = self.stopped(reopened.acked, reopened.acked_offset);
                self.owners[index].inst = inst;
                return;
            }
        };
        let incarnation = reopened.log.incarnation;
        let local = reopened.local(activation, incarnation, MAX_BATCH_BYTES, family, None);
        // Property 4: the baseline is the latest claim of its log the server applied.
        let applied = self.owners[index]
            .log
            .claims
            .iter()
            .filter(|c| self.applied_claims.contains(c))
            .map(|(epoch, _)| *epoch)
            .max();
        let (inst, actions) = if self.knobs.claim_without_pre_check {
            let own = local
                .claims
                .iter()
                .map(|(epoch, _)| *epoch)
                .max()
                .unwrap_or(claim.0);
            let kind = ClaimKind::Continuation { own, resent: false };
            self.activated(kind, local)
        } else {
            self.started(local, |local| Replicator::provisional(local, claim))
        };
        let opened_through = Some(self.owners[index].log.last_logged().0);
        self.owners[index].inst = Instance {
            claim_applied: applied,
            provisional: true,
            opened_through,
            ..inst
        };
        self.owners[index].activation = activation;
        self.process(index, actions, Before::default());
    }

    /// The activation of owner `index`'s next open: the one it had with `keep`, else a new one.
    fn open_activation(&mut self, index: usize, keep: bool) -> Activation {
        if keep {
            self.owners[index].activation
        } else {
            self.activation()
        }
    }

    fn clone_owner(&mut self, index: usize) {
        if self.owners.len() >= MAX_OWNERS {
            return;
        }
        let mut copy = self.owners[index].clone();
        let original = copy.inst.uid;
        copy.family = self.next_family;
        self.next_family += 1;
        copy.salt = u8::try_from(copy.family % 250).unwrap();
        copy.inst.uid = self.uid();
        let clone_uid = copy.inst.uid;
        // The copy's connections are gone: every attempt it waits on gets no answer.
        let mut lost = Vec::new();
        for msg in &self.msgs {
            let (to, id) = match msg {
                Msg::Request { to, id, late, .. } if !late => (*to, *id),
                Msg::Answer { to, id, .. } => (*to, *id),
                Msg::Request { .. } => continue,
            };
            if to == original {
                self.shared.entry((to, id)).or_default().push(clone_uid);
                lost.push(Msg::Answer {
                    to: clone_uid,
                    id,
                    exchange: Exchange::NoAnswer(NoAnswer::Lost {
                        reason: "restored copy".to_owned(),
                    }),
                    misrouted: false,
                });
            }
        }
        self.msgs.extend(lost);
        for sharers in self.shared.values_mut() {
            if sharers.contains(&original) {
                sharers.push(clone_uid);
            }
        }
        self.owners.push(copy);
    }

    /// Another host opens a copy of owner `index`'s cache (design: Clones, its host contract
    /// broken): a new owner of a new family, so its commits carry its own bytes, with the log as
    /// copied (with `durable`, only what an fsync made durable, as after a power loss). It opens
    /// through the open table under a fresh activation, as a takeover, or provisionally when
    /// `offline`. The original keeps running.
    fn copy_cache(&mut self, index: usize, durable: bool, offline: bool) {
        if self.owners.len() >= MAX_OWNERS {
            return;
        }
        let mut copy = self.owners[index].clone();
        copy.family = self.next_family;
        self.next_family += 1;
        copy.salt = u8::try_from(copy.family % 250).unwrap();
        copy.inst = self.stopped(copy.inst.acked, copy.inst.acked_offset);
        // The commits the copy lost may be in the stream (property 13's exemption); the original
        // still holds them.
        if durable && !copy.log.drop_unsynced(usize::MAX).is_empty() {
            copy.power_lost = true;
        }
        self.owners.push(copy);
        let index = self.owners.len() - 1;
        if offline {
            self.provisional_restart(index, 0, false);
        } else {
            self.restart(index, 0, false);
        }
        self.owners[index].inst.copied = true;
    }

    /// Whether another owner's log is of owner `index`'s lineage: a clone or a copy.
    fn shares_lineage(&self, index: usize) -> bool {
        let lineage = self.owners[index].log.lineage;
        self.owners
            .iter()
            .enumerate()
            .any(|(i, o)| i != index && o.log.lineage == lineage)
    }

    fn live(&self, index: usize) -> bool {
        self.owners[index].inst.replicator.ctx().is_some()
    }

    /// Whether a connection can commit or call `synced()`. A takeover runs inside `open()`,
    /// before any connection opens the generation (design: Open), so the driver logs no commit
    /// and takes no `synced()` call for a binding still opening.
    fn bound(&self, index: usize) -> bool {
        self.live(index)
            && !matches!(self.owners[index].inst.replicator, Replicator::Activating {
                kind: ClaimKind::Takeover { .. },
                ..
            })
    }

    fn commit(&mut self, index: usize) {
        if !self.bound(index) {
            return;
        }
        let owner = &mut self.owners[index];
        let position = owner.log.next_position;
        owner.log.next_position += 1;
        let id = CommitId {
            lineage: owner.log.lineage,
            position: Position(position),
        };
        let len = 3 + usize::try_from(position % 7).unwrap();
        let bytes = frame::encode(&Frame::Commit {
            id,
            compressed: vec![owner.salt; len],
        })
        .unwrap();
        let pending = Pending {
            id,
            frame: Arc::from(bytes),
        };
        owner.log.commits.insert(position, pending.clone());
        self.event(index, Event::Logged(pending));
    }

    fn sync(&mut self, index: usize) {
        if !self.bound(index) {
            return;
        }
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        let target = self.owners[index].log.last_logged();
        let call = Call {
            target,
            stream: self.stream(),
            token: self.server.token(PATH),
            discards: self.owners[index].inst.discards.len(),
        };
        self.owners[index].inst.calls.insert(ticket, call);
        self.event(index, Event::Sync { ticket, target });
    }

    fn event(&mut self, index: usize, event: Event) {
        self.event_from(index, event, Before::default());
    }

    /// One event, with what `before` says of its answer; the rest of `before` is the replicator's
    /// state now.
    fn event_from(&mut self, index: usize, event: Event, before: Before) {
        let replicator = &self.owners[index].inst.replicator;
        let before = Before {
            streaming: matches!(replicator, Replicator::Streaming { .. }),
            claiming: matches!(replicator, Replicator::Activating {
                kind: ClaimKind::Continuation { .. },
                step: Step::Claim { .. },
                ..
            }),
            takeover: matches!(replicator, Replicator::Activating {
                kind: ClaimKind::Takeover { .. },
                ..
            }),
            ..before
        };
        let now = self.now();
        let actions = self.owners[index].inst.replicator.on(event, now);
        self.process_in(index, actions, before);
    }

    fn violation(&mut self, what: String) {
        self.violations.push(what);
    }

    /// Owner `index` is the only writer on the stream: no foreign append or claim, no other owner
    /// or clone, and its continuation claim did not lose to idle expiry (properties 13 and 21).
    fn alone(&self, index: usize) -> bool {
        !self.others && self.owners.len() == 1 && !self.owners[index].inst.claim_expired
    }

    /// Carries out an owner's actions and checks them.
    fn process(&mut self, index: usize, actions: Vec<Action>, before: Before) {
        self.process_in(index, actions, before);
    }

    fn process_in(&mut self, index: usize, actions: Vec<Action>, before: Before) {
        let takeover = before.takeover;
        // A claim this sends is a continuation's (property 4).
        let continuation = matches!(
            &self.owners[index].inst.replicator,
            Replicator::Activating {
                kind: ClaimKind::Continuation { .. },
                ..
            }
        );
        if before.misrouted
            && let Some(kind) = actions.iter().find_map(|a| match a {
                Action::Reset(kind) => Some(*kind),
                _ => None,
            })
        {
            self.violation(format!(
                "property 17: a misrouted answer reset owner {index} {kind:?}"
            ));
        }
        let mut caught_up = false;
        let mut receipts = Vec::new();
        let fenced = actions.contains(&Action::Reset(ResetKind::Fenced));
        let discarded = actions.iter().any(|a| {
            matches!(a, Action::Rebuild {
                discard_from: Some(_),
                ..
            })
        });
        if fenced && discarded {
            self.violation("property 6: a fence discarded pending records".to_owned());
        }
        for action in actions {
            match action {
                Action::Send { id, request, .. } => self.sent(index, id, request, continuation),
                // Fsynced: every commit logged before it is durable.
                Action::PersistClaim { epoch, activation } => {
                    let log = &mut self.owners[index].log;
                    log.claims.push((epoch, activation));
                    log.durable_through = log.last_logged().0;
                }
                Action::ReKey {
                    incarnation,
                    epoch,
                    activation,
                } => self.rekeyed(index, incarnation, (epoch, activation), &before),
                Action::PersistAck {
                    acked,
                    acked_offset,
                    log_bytes,
                } => {
                    let log = &mut self.owners[index].log;
                    log.acks.push((acked, acked_offset, log_bytes));
                    if log.catching_up {
                        log.catching_up = false;
                        log.sync();
                    }
                }
                Action::Wake { acked, offset } => self.woken(index, acked, offset, takeover),
                Action::Synced { ticket } => self.synced(index, ticket),
                Action::Abandoned { ticket } => {
                    self.owners[index].inst.calls.remove(&ticket);
                }
                Action::Timer { at_ms, jitter_ms } => {
                    self.owners[index].inst.timer = Some(at_ms.saturating_add(jitter_ms));
                }
                Action::Condition(condition) => {
                    let inst = &mut self.owners[index].inst;
                    let blocked =
                        condition == Some(Condition::Blocked(BlockedReason::WrongCluster));
                    if inst.blocked_wrong_cluster && !blocked {
                        self.coverage.blocks_resumed.fetch_add(1, Ordering::Relaxed);
                    }
                    inst.blocked_wrong_cluster = blocked;
                }
                Action::Boundary(receipt) => receipts.push(receipt),
                // A catch-up logs every frame of one read as a COMMIT record at the next
                // positions, then the ACK covering them, fsynced, before the next read.
                Action::Apply(frames) => {
                    if !takeover {
                        self.violation("property 6: a catch-up outside a takeover".to_owned());
                    }
                    if caught_up {
                        self.violation("an Apply before the previous one was caught up".to_owned());
                    }
                    let end = frames.last().map(|f| f.end_offset);
                    let inst = &mut self.owners[index].inst;
                    inst.applies += 1;
                    if inst.applies == 2 {
                        self.coverage
                            .chunked_catch_ups
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let log = &mut self.owners[index].log;
                    for foreign in frames {
                        let Frame::Commit { id, .. } = foreign.frame else {
                            panic!("a catch-up applies COMMIT frames");
                        };
                        let bytes: Arc<[u8]> = Arc::from(frame::encode(&foreign.frame).unwrap());
                        log.commits.insert(log.next_position, Pending {
                            id,
                            frame: Arc::clone(&bytes),
                        });
                        log.next_position += 1;
                        log.applied.push(bytes);
                    }
                    if let Some(end) = end {
                        let last = log.last_logged();
                        log.acks.push((last, end, 0));
                        log.sync();
                    }
                    log.catching_up = false;
                    caught_up = true;
                }
                Action::Rebuild {
                    discard_from,
                    salvage,
                    to,
                    incarnation,
                    ..
                } => {
                    let stream = self.stream();
                    let to = usize::try_from(to.get()).unwrap().min(stream.len());
                    self.owners[index].inst.rebuilt_bytes =
                        Some((incarnation, Arc::from(&stream[..to])));
                    if !takeover {
                        self.violation("property 6: a rebuild outside a takeover".to_owned());
                    }
                    let rolled = self.owners[index].log.rekeyed_at_open;
                    if discard_from
                        .zip(rolled)
                        .is_some_and(|(from, through)| from.0 <= through)
                        && !salvage
                    {
                        self.violation(format!(
                            "property 18: owner {index} discarded pending records of a log \
                             re-keyed at open with no salvage"
                        ));
                    }
                    let owner = &mut self.owners[index];
                    if discard_from.is_none() && owner.inst.applies > 0 {
                        self.coverage
                            .rebuilds_after_apply
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    if let Some(from) = discard_from {
                        owner.inst.discards.push(from);
                    }
                    owner.inst.rebuilt = true;
                    owner.inst.unpublished = Some(discard_from);
                    caught_up = true;
                }
                Action::Reset(kind @ (ResetKind::Fenced | ResetKind::Stale)) => {
                    let owner = &self.owners[index];
                    // A power loss dropped commits the stream may hold (design: Resets).
                    let lost =
                        kind == ResetKind::Stale && owner.power_lost && owner.inst.provisional;
                    // A claim a pre-check sent again met `producer-expired` without its frame found:
                    // the retired record may be another claimant's, and it resets `stale`.
                    let expired = kind == ResetKind::Stale && owner.inst.expired;
                    // A generation whose takeover never bound (a fresh owner's, after a salvage)
                    // has no claim to continue: its provisional claim loses to its own earlier
                    // generation's.
                    let never_held = kind == ResetKind::Fenced
                        && owner.inst.provisional
                        && !owner
                            .log
                            .claims
                            .iter()
                            .any(|c| self.applied_claims.contains(c));
                    let alone = self.alone(index);
                    let provisional = owner.inst.provisional;
                    let copied = owner.inst.copied;
                    if alone && !lost && !expired && !never_held {
                        self.violation(format!(
                            "property 13: a {kind:?} reset with no other writer on the stream"
                        ));
                    }
                    if provisional {
                        let counter = match kind {
                            ResetKind::Fenced => &self.coverage.provisional_fenced,
                            _ => &self.coverage.provisional_stale,
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    if provisional && copied {
                        let counter = match kind {
                            ResetKind::Fenced => &self.coverage.offline_copies_fenced,
                            _ => &self.coverage.offline_copies_stale,
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Action::Reset(ResetKind::StreamState) if self.alone(index) => {
                    self.violation(format!(
                        "property 21: owner {index} reset stream_state with no other writer on the \
                         stream"
                    ));
                }
                // The retiring binding fsyncs its ACK (design: History, Log).
                Action::Reset(ResetKind::Rewound) => {
                    self.coverage.rewound.fetch_add(1, Ordering::Relaxed);
                    self.owners[index].log.sync();
                }
                Action::Bound { .. } => {
                    self.owners[index].log.rekeyed_at_open = None;
                    // The exemption of property 13 ends with the first claim that binds.
                    self.owners[index].power_lost = false;
                    let inst = &mut self.owners[index].inst;
                    inst.expired = false;
                    let provisional = std::mem::take(&mut inst.provisional);
                    let copied = std::mem::take(&mut inst.copied);
                    if provisional {
                        self.coverage
                            .provisional_live
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let counter = match (copied, provisional) {
                        (false, _) => None,
                        (true, false) => Some(&self.coverage.copies_took_over),
                        (true, true) => Some(&self.coverage.offline_copies_live),
                    };
                    if let Some(counter) = counter {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Action::Reset(_) | Action::Refuse(_) => {}
            }
        }
        if caught_up {
            // A rebuild's positions continue above the last one logged.
            let last = self.owners[index].log.last_logged();
            self.owners[index].inst.caught_up_due = Some(last);
        }
        for receipt in receipts {
            self.boundary_reached(index, receipt);
        }
    }

    /// Property 15, then the log rolls to a segment naming `incarnation` with the CLAIM record,
    /// fsynced: every ACK before it is durable too.
    fn rekeyed(
        &mut self,
        index: usize,
        incarnation: Incarnation,
        claim: (Epoch, Activation),
        before: &Before,
    ) {
        // A takeover's, at open or during its claim, or a provisional open's before its bind: its
        // ACK may trail the stream's (property 18).
        let at_open = before.takeover || self.owners[index].inst.provisional;
        let counter = if before.streaming {
            &self.coverage.rekeys_streaming
        } else if before.claiming {
            &self.coverage.rekeys_claiming
        } else {
            &self.coverage.rekeys_open
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let inst = &self.owners[index].inst;
        // A pre-check's HEAD decides a re-key that rolls the log only with the claim, after
        // the reads.
        let saw = before
            .head_saw
            .clone()
            .or_else(|| inst.pre_check_head.clone());
        match &saw {
            None => self.violation(format!(
                "property 15: owner {index} re-keyed with no HEAD answered"
            )),
            Some(stream) => {
                if let Some(missing) = self.acknowledged_missing(index, stream) {
                    self.violation(format!(
                        "property 15: owner {index} re-keyed to {incarnation} though the history \
                         lost its acknowledged {missing:?}"
                    ));
                }
            }
        }
        let opened_through = self.owners[index].inst.opened_through;
        let log = &mut self.owners[index].log;
        log.incarnation = incarnation;
        if !log.claims.contains(&claim) {
            log.claims.push(claim);
        }
        if at_open {
            // A provisional open's own commits, logged after its open, were never sent.
            let through = opened_through.unwrap_or(log.last_logged().0);
            log.rekeyed_at_open = Some(log.rekeyed_at_open.map_or(through, |t| t.max(through)));
        }
        log.sync();
    }

    /// The first record owner `index` acknowledged that `stream` does not hold in log order, if
    /// any. Positions a rebuild brought in name no record here.
    fn acknowledged_missing(&self, index: usize, stream: &[u8]) -> Option<Position> {
        let owner = &self.owners[index];
        let from = owner.log.rebuilt_through + 1;
        let mut at = 0;
        for (position, record) in owner.log.commits.range(from..=owner.inst.acked.0.max(from)) {
            if Position(*position) > owner.inst.acked {
                break;
            }
            let found = stream
                .get(at..)
                .unwrap_or_default()
                .windows(record.frame.len())
                .position(|w| w == &record.frame[..]);
            match found {
                Some(i) => at += i + record.frame.len(),
                None => return Some(Position(*position)),
            }
        }
        None
    }

    fn sent(&mut self, index: usize, id: RequestId, request: Request, continuation: bool) {
        if let Some(write) = &request.keys.producer {
            let key = (write.epoch.get(), write.seq.get());
            let digest = producer_digest(&request.body);
            let family = self.owners[index].family;
            let previous = self.keys.entry(family).or_default().insert(key, digest);
            if previous.is_some_and(|p| p != digest) {
                self.violation(format!(
                    "property 3: owner {family} sent two bodies under {key:?}"
                ));
            }
            let applied = &self.owners[index].log.applied;
            if write.seq != Seq::ZERO
                && applied
                    .iter()
                    .any(|f| request.body.windows(f.len()).any(|w| w == &f[..]))
            {
                self.violation("property 12: an append carries a caught-up frame".to_owned());
            }
            let claimed = match frame::decode(&request.body, u64::MAX) {
                Ok(Decoded::Frame {
                    frame: Frame::Claim { epoch, activation },
                    ..
                }) if write.seq == Seq::ZERO => Some((epoch, activation)),
                _ => None,
            };
            match claimed {
                Some(_) if continuation => {
                    self.background.insert((self.owners[index].inst.uid, id));
                }
                Some(claim) => {
                    self.explicit.insert(claim);
                }
                None => {}
            }
            let inst = &mut self.owners[index].inst;
            let resent = claimed.is_some_and(|c| inst.inherited.contains(&c));
            let first = claimed.is_some() && !resent && !inst.claims_sent.contains(&write.epoch);
            if first {
                let uid = inst.uid;
                self.first_claims.insert((uid, id), continuation);
            }
            let inst = &mut self.owners[index].inst;
            if write.seq == Seq::ZERO && !inst.claims_sent.contains(&write.epoch) {
                inst.claims_sent.push(write.epoch);
                // A new continuation claim, not one sent again: one above the latest claim of
                // this instance the server applied.
                let own = inst.claim_applied;
                if continuation
                    && !resent
                    && own.map(|own| own.get() + 1) != Some(write.epoch.get())
                {
                    self.violation(format!(
                        "property 4: a continuation claimed {} after applied claim {own:?}",
                        write.epoch
                    ));
                }
            }
        }
        let to = self.owners[index].inst.uid;
        self.families.insert(to, self.owners[index].family);
        self.msgs.push(Msg::Request {
            to,
            id,
            request: Box::new(request),
            late: false,
        });
    }

    fn woken(&mut self, index: usize, acked: Position, offset: Offset, takeover: bool) {
        self.between_acks(index, offset, takeover);
        let (was, start) = (
            self.owners[index].inst.acked,
            self.owners[index].inst.start_acked,
        );
        if acked < was {
            self.violation(format!("property 1: acked went from {was:?} to {acked:?}"));
        }
        let records: Vec<Pending> = self.owners[index]
            .log
            .commits
            .range(start.0 + 1..)
            .take_while(|(p, _)| **p <= acked.0)
            .map(|(_, p)| p.clone())
            .collect();
        // Property 19: the owner's own commits it was told are acknowledged.
        let lineage = self.owners[index].log.lineage;
        let acknowledged: Vec<Arc<[u8]>> = records
            .iter()
            .filter(|p| p.id.lineage == lineage)
            .map(|p| Arc::clone(&p.frame))
            .collect();
        self.acknowledged.extend(acknowledged);
        // Searched as bytes: foreign garbage may sit between frames. An answer that comes back
        // after a rewind is about the history it applied in, which the stream no longer shows.
        let stream = self.stream();
        let current = self.owners[index].log.incarnation == self.token();
        let mut from = 0;
        for record in &records {
            // Property 8 is about one history: a frame acknowledged earlier that the current
            // stream no longer holds was of a history it no longer shows, and an acknowledgement
            // of a frame it does not hold is about such a history too.
            let held = |frame: &[u8]| stream.windows(frame.len()).any(|w| w == frame);
            match self.acked_frames.get(&record.id) {
                // An owner left on another history acknowledges what applied there.
                Some(frame)
                    if current
                        && *frame != record.frame
                        && held(frame)
                        && !self.owners[index].log.applied.contains(frame) =>
                {
                    self.violation(format!(
                        "property 8: two commits acknowledged at {:?}",
                        record.id
                    ));
                }
                Some(_) => {}
                None if held(&record.frame) => {
                    self.acked_frames
                        .insert(record.id, Arc::clone(&record.frame));
                }
                None => {}
            }
            let found = stream
                .get(from..)
                .unwrap_or_default()
                .windows(record.frame.len())
                .position(|w| w == &record.frame[..]);
            match found {
                Some(i) => from += i + record.frame.len(),
                None if !current => break,
                None => {
                    self.violation(format!(
                        "property 2: acknowledged {:?} is not in the stream in log order",
                        record.id.position
                    ));
                    break;
                }
            }
        }
        let inst = &mut self.owners[index].inst;
        inst.acked = inst.acked.max(acked);
        inst.acked_offset = offset;
    }

    /// Publishes a snapshot at `offset` whose `USS3` header names `last`, as the snapshot thread
    /// of an owner of family `by` would (the image is a stand-in: only the header is ever read
    /// here).
    fn publish_snapshot(&mut self, offset: Offset, last: CommitId, by: u64) {
        let fields = SnapshotFields {
            offset,
            epoch: Epoch::try_from(1).unwrap(),
            last,
        };
        let mut body = Vec::new();
        write_body(
            &fields,
            &mut std::io::Cursor::new(vec![0u8; 4096]),
            &mut body,
        )
        .unwrap();
        let mut digester = SnapshotDigester::new(CONTENT_TYPE);
        digester.update(&body);
        let publish = Request {
            content_type: Some(CONTENT_TYPE.to_owned()),
            snapshot_digest: Some(digester.finalize()),
            body: Arc::from(body),
            ..Request::new(Method::Put, Target::Snapshot { offset })
        };
        if self.server.handle(PATH, &publish).status < 300 {
            self.snapshots.push((last, by));
        }
    }

    /// Property 14: a boundary's receipt names the end of record `k`'s frame in the stream with
    /// only claims after it; then the snapshot is published there, and its bytes stop counting.
    /// A receipt without an offset publishes nothing.
    fn boundary_reached(&mut self, index: usize, receipt: BoundaryReceipt) {
        let Some(offset) = receipt.offset else {
            return;
        };
        let owner = self.owners[index].clone();
        if owner.log.incarnation != self.token() {
            return;
        }
        // A boundary at or below `rebuilt_through` is published unchecked: the frames the rebuild
        // brought in take no positions in this model (the driver logs each at the next one), so
        // nothing here names the frame that ends at its receipt.
        let record = owner
            .log
            .commits
            .get(&receipt.position.0)
            .filter(|_| receipt.position.0 > owner.log.rebuilt_through);
        if let Some(record) = record {
            let stream = self.stream();
            let end = usize::try_from(offset.get()).unwrap();
            let holds = stream
                .windows(record.frame.len())
                .enumerate()
                .filter(|(_, w)| *w == &record.frame[..])
                .map(|(i, _)| i + record.frame.len())
                .any(|after| {
                    after <= end
                        && decode_all(stream.get(after..end).unwrap_or_default())
                            .iter()
                            .all(|(_, _, f)| matches!(f, Some(Frame::Claim { .. })))
                });
            if !holds {
                self.violation(format!(
                    "property 14: the receipt of boundary {:?} names {offset}, not the end of \
                     its frame with only claims after it",
                    receipt.position
                ));
                return;
            }
        }
        // The snapshot thread fsyncs ACK(k, W) before it publishes: no restart reads an older ACK.
        self.owners[index].log.sync();
        let last = CommitId {
            lineage: owner.log.lineage,
            position: receipt.position,
        };
        self.publish_snapshot(offset, last, owner.family);
        self.event(index, Event::Snapshotted {
            log_bytes: receipt.log_bytes,
        });
    }

    /// Property 10: what lies between the previous acknowledged offset and `offset`.
    fn between_acks(&mut self, index: usize, offset: Offset, takeover: bool) {
        let owner = &mut self.owners[index];
        let rebuilt = std::mem::take(&mut owner.inst.rebuilt);
        let previous = owner.inst.acked_offset;
        if rebuilt
            || offset <= previous
            || owner.log.incarnation != self.server.token(PATH).unwrap()
        {
            return;
        }
        let owner = self.owners[index].clone();
        let stream = self.stream();
        let range = stream
            .get(usize::try_from(previous.get()).unwrap()..usize::try_from(offset.get()).unwrap())
            .unwrap_or_default();
        let own_frames: Vec<&[u8]> = owner.log.commits.values().map(|p| &p.frame[..]).collect();
        let frames = decode_all(range);
        // Its own: a claim whose epoch and activation its log holds. Under one activation for every
        // instance, any claim is its own to the replicator (design: Claims).
        let own_claim = |frame: &Option<Frame>| {
            matches!(frame, Some(Frame::Claim { epoch, activation })
                if owner.log.claims.contains(&(*epoch, *activation)))
        };
        let shared = self.knobs.shared_activation;
        // A claim of its own after a foreign one, other than the claim it binds with at the end,
        // applied after it (design: Reconcile, superseded).
        let superseder = frames
            .iter()
            .take(frames.len().saturating_sub(1))
            .rposition(|(_, _, frame)| own_claim(frame));
        for (i, (start, len, frame)) in frames.iter().enumerate() {
            let (start, len) = (*start, *len);
            let bytes = range.get(start..start + len).unwrap_or_default();
            let fine = match frame {
                Some(Frame::Claim { .. }) => {
                    shared || takeover || own_claim(frame) || superseder.is_some_and(|s| s > i)
                }
                Some(Frame::Commit { .. }) => own_frames.contains(&bytes),
                _ => false,
            };
            if !fine {
                self.violation(format!(
                    "property 10: owner {index} acknowledged past a foreign frame at {}",
                    previous.get() + u64::try_from(start).unwrap()
                ));
                return;
            }
        }
    }

    fn synced(&mut self, index: usize, ticket: u64) {
        let owner = self.owners[index].clone();
        let Some(call) = owner.inst.calls.get(&ticket).cloned() else {
            self.violation(format!("property 9: unknown ticket {ticket}"));
            return;
        };
        if owner.inst.acked < call.target {
            self.violation(format!("property 9: ticket {ticket} below its target"));
        }
        if owner
            .inst
            .discards
            .iter()
            .skip(call.discards)
            .any(|from| *from <= call.target)
        {
            self.violation(format!(
                "property 9: ticket {ticket} for a discarded target"
            ));
        }
        if call.token == self.server.token(PATH) {
            let at = usize::try_from(owner.inst.acked_offset.get()).unwrap();
            let past = call.stream.get(at..).unwrap_or_default();
            let own_frames: Vec<&[u8]> = owner.log.commits.values().map(|p| &p.frame[..]).collect();
            for (start, len, frame) in decode_all(past) {
                let bytes = past.get(start..start + len).unwrap_or_default();
                let own = match frame {
                    Some(Frame::Claim { epoch, activation }) => {
                        owner.log.claims.contains(&(epoch, activation))
                    }
                    Some(Frame::Commit { .. }) => own_frames.contains(&bytes),
                    _ => false,
                };
                if !own {
                    self.violation(format!(
                        "property 9: ticket {ticket} synced past a foreign frame"
                    ));
                    break;
                }
            }
        }
        self.owners[index].inst.calls.remove(&ticket);
    }

    /// A request reaches the server.
    fn deliver_request(
        &mut self,
        to: u64,
        id: RequestId,
        request: Request,
        late: bool,
        answer: bool,
    ) {
        let mut request = request;
        if self.knobs.strip_digests
            && let Some(write) = &mut request.keys.producer
        {
            write.digest = None;
        }
        let cut = self
            .families
            .get(&to)
            .is_some_and(|family| self.partitioned.contains(family));
        if cut {
            // No connection is made: nothing goes out.
            if !late {
                let exchange = Exchange::NoAnswer(NoAnswer::NotSent {
                    reason: "partitioned".to_owned(),
                });
                self.msgs.push(Msg::Answer {
                    to,
                    id,
                    exchange,
                    misrouted: false,
                });
            }
            return;
        }
        let misrouted = self.misrouted;
        let delivered = self.routed().exchange(PATH, &request);
        // The pre-check reads up to the tail its HEAD named.
        if !late && request.method == Method::Head {
            let tail = self.routed().frames(PATH).map_or(0, <[u8]>::len);
            if let Some(owner) = self.owners.iter_mut().find(|o| o.inst.uid == to)
                && matches!(owner.inst.replicator, Replicator::Activating {
                    step: Step::PreCheck {
                        check: PreCheck::Head,
                        ..
                    },
                    ..
                })
            {
                owner.inst.checked_tail = Some(tail);
            }
        }
        if request.method == Method::Head && !late {
            let saw = self.routed().frames(PATH).unwrap_or_default().to_vec();
            self.heads.insert((to, id), saw);
        }
        if misrouted
            && self
                .other
                .frames(PATH)
                .is_some_and(|bytes| !bytes.is_empty())
        {
            self.violation("property 16: a write applied on the other deployment".to_owned());
        }
        let exchange = match delivered {
            Delivered::Answered { answer, .. } => {
                let parsed = answer.parse(&request);
                if !misrouted {
                    self.applied(to, Some(id), &parsed.typed, &request);
                    let sharers = self.shared.get(&(to, id)).cloned().unwrap_or_default();
                    for uid in sharers {
                        self.applied_claim(uid, &parsed.typed, &request);
                    }
                }
                Exchange::Answered(Box::new(parsed))
            }
            Delivered::Unreachable => Exchange::NoAnswer(NoAnswer::NotSent {
                reason: "unreachable".to_owned(),
            }),
            Delivered::DroppedBeforeApply | Delivered::DroppedAfterApply => {
                Exchange::NoAnswer(NoAnswer::Lost {
                    reason: "dropped".to_owned(),
                })
            }
        };
        if late {
            return;
        }
        let exchange = if answer {
            exchange
        } else {
            Exchange::NoAnswer(NoAnswer::Lost {
                reason: "answer lost after apply".to_owned(),
            })
        };
        self.msgs.push(Msg::Answer {
            to,
            id,
            exchange,
            misrouted,
        });
    }

    /// Property 5, on every applied producer write.
    fn applied(
        &mut self,
        to: u64,
        id: Option<RequestId>,
        typed: &Result<Typed, ursula_headers::HeaderError>,
        request: &Request,
    ) {
        self.applied_claim(to, typed, request);
        let Ok(Typed::Producer(answer)) = typed else {
            return;
        };
        if let Some(epoch) = crate::replicator::answer::answer_epoch(answer) {
            self.max_epoch = self.max_epoch.max(epoch.get());
        }
        if let (ProducerAnswer::Receipt { incarnation, .. }, Some(write), Some(_)) =
            (answer, &request.keys.producer, id)
        {
            self.one_body(*incarnation, write.epoch, write.seq, &request.body);
        }
        let (
            ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            },
            Some(write),
        ) = (answer, &request.keys.producer)
        else {
            return;
        };
        if self.max_claimed.is_some_and(|max| write.epoch < max) {
            self.violation(format!(
                "property 5: ({}, {}) applied after a claim of {:?}",
                write.epoch, write.seq, self.max_claimed
            ));
        }
        if write.seq == Seq::ZERO {
            self.max_claimed = Some(self.max_claimed.map_or(write.epoch, |m| m.max(write.epoch)));
            if let ProducerAnswer::Receipt { receipt, .. } = answer
                && id.is_some_and(|id| self.background.contains(&(to, id)))
            {
                self.background_claim_applied(to, write.epoch, receipt.start());
            }
        }
    }

    /// Property 22: the server acknowledged `body` under `(epoch, seq)` in the history of
    /// `incarnation`, and no other body under that key.
    fn one_body(&mut self, incarnation: Incarnation, epoch: Epoch, seq: Seq, body: &[u8]) {
        let digest = producer_digest(body);
        let previous = self.bodies.insert((incarnation, epoch, seq), digest);
        if previous.is_some_and(|p| p != digest) {
            self.violation(format!(
                "property 22: two bodies acknowledged under ({epoch}, {seq}) in history \
                 {incarnation}"
            ));
        }
    }

    /// Property 20: instance `uid`'s background claim of `epoch` applied at `start`: no
    /// claim an explicit open of another owner made past both the instance's
    /// acknowledged offset and its own latest claim in the stream lies below it, unless it landed
    /// after the instance's pre-check read the tail.
    fn background_claim_applied(&mut self, uid: u64, epoch: Epoch, start: Offset) {
        let Some(owner) = self.owners.iter().find(|o| o.inst.uid == uid) else {
            return;
        };
        // A claim frame of an activation in the log, or of the instance's own, is the replicator's
        // (design: Claims). One another owner made under that activation (one activation for every
        // instance, or a clone and its original reopened under the activation they share) is the
        // residual case of a shared activation, which nothing on the stream tells apart.
        let ours: Vec<Activation> = owner
            .log
            .claims
            .iter()
            .map(|(_, a)| *a)
            .chain(owner.inst.replicator.ctx().map(|ctx| ctx.activation))
            .collect();
        let race_from = owner
            .inst
            .provisional
            .then_some(owner.inst.checked_tail)
            .flatten();
        let stream = self.stream();
        let before = stream
            .get(..usize::try_from(start.get()).unwrap())
            .unwrap_or_default();
        let claims: Vec<(usize, Epoch, Activation)> = decode_all(before)
            .into_iter()
            .filter_map(|(at, _, frame)| match frame {
                Some(Frame::Claim { epoch, activation }) => Some((at, epoch, activation)),
                _ => None,
            })
            .collect();
        let since = claims
            .iter()
            .rposition(|(_, _, a)| ours.contains(a))
            .map_or(0, |i| i + 1);
        // What lies before its acknowledged offset is the state it built on.
        let acked = usize::try_from(owner.inst.acked_offset.get()).unwrap();
        let fenced = claims.iter().skip(since).find(|(at, e, a)| {
            *at >= acked
                && *e < epoch
                && !ours.contains(a)
                && self.explicit.contains(&(*e, *a))
                && race_from.is_none_or(|from| *at < from)
        });
        if let Some((at, e, _)) = fenced {
            self.violation(format!(
                "property 20: a background claim of {epoch} applied above the claim of {e} at {at} an \
                 explicit open made (pre-check tail {race_from:?})"
            ));
        }
    }

    /// Instance `uid` saw its claim in the stream, if `typed` is a receipt for the claim `request`.
    fn applied_claim(
        &mut self,
        uid: u64,
        typed: &Result<Typed, ursula_headers::HeaderError>,
        request: &Request,
    ) {
        // A repeat confirms the same claim, key and body, is in the stream: sent by a clone, or
        // applied before its answer was lost. So does `receipt-evicted`, whose digest check passed,
        // and `producer-expired` for a claim the server took before (its record retired).
        let (Ok(Typed::Producer(answer)), Some(write)) = (typed, &request.keys.producer) else {
            return;
        };
        if write.seq != Seq::ZERO {
            return;
        }
        let Ok(Decoded::Frame {
            frame: Frame::Claim { epoch, activation },
            ..
        }) = frame::decode(&request.body, u64::MAX)
        else {
            return;
        };
        let took = match answer {
            ProducerAnswer::Receipt { .. } | ProducerAnswer::ReceiptEvicted { .. } => true,
            ProducerAnswer::ProducerExpired { .. } => {
                self.applied_claims.contains(&(epoch, activation))
            }
            _ => false,
        };
        if !took {
            return;
        }
        self.applied_claims.insert((epoch, activation));
        if let Some(owner) = self.owners.iter_mut().find(|o| o.inst.uid == uid) {
            let applied = &mut owner.inst.claim_applied;
            *applied = Some(applied.map_or(write.epoch, |a| a.max(write.epoch)));
        }
    }

    fn deliver_answer(&mut self, to: u64, id: RequestId, exchange: Exchange, misrouted: bool) {
        let head_saw = self.heads.remove(&(to, id));
        let first_claim = self.first_claims.remove(&(to, id));
        if let Some(index) = self.owners.iter().position(|o| o.inst.uid == to) {
            let watch = self.watch(index, &exchange, first_claim);
            let inst = &mut self.owners[index].inst;
            // Only an expiry naming the claim a provisional instance sent again after its
            // pre-check (property 13).
            if let Exchange::Answered(answer) = &exchange
                && let Ok(Typed::Producer(ProducerAnswer::ProducerExpired { epoch: named })) =
                    &answer.typed
                && inst.provisional
                && let Replicator::Activating {
                    kind: ClaimKind::Continuation { resent: true, .. },
                    step: Step::Claim { epoch, .. },
                    ..
                } = &inst.replicator
                && named == epoch
            {
                inst.expired = true;
            }
            // A misrouted HEAD never decides a re-key.
            if head_saw.is_some()
                && !misrouted
                && matches!(inst.replicator, Replicator::Activating {
                    step: Step::PreCheck { .. },
                    ..
                })
            {
                inst.pre_check_head.clone_from(&head_saw);
            }
            let before = Before {
                head_saw,
                misrouted,
                ..Before::default()
            };
            self.event_from(index, Event::Answer { id, exchange }, before);
            self.watched(index, watch);
        }
    }

    /// What an answer to owner `index` may reach for the coverage counters, seen before the
    /// replicator takes it.
    fn watch(&self, index: usize, exchange: &Exchange, first_claim: Option<bool>) -> Watch {
        let owner = &self.owners[index];
        let producer = match exchange {
            Exchange::Answered(answer) => match &answer.typed {
                Ok(Typed::Producer(producer)) => Some(producer),
                _ => None,
            },
            Exchange::NoAnswer(_) => None,
        };
        let fence = match producer {
            Some(ProducerAnswer::KeyConflict { .. }) => Some(Fence::KeyConflict),
            Some(ProducerAnswer::ReceiptEvicted { .. }) => Some(Fence::ReceiptEvicted),
            _ => None,
        }
        .filter(|_| self.shares_lineage(index));
        let repeat = matches!(
            producer,
            Some(ProducerAnswer::Receipt {
                delivery: Delivery::Repeat,
                ..
            })
        );
        // A snapshot read of the `410` rule, the latest snapshot another copy's of our lineage
        // above our acknowledged position.
        let rejected = match &owner.inst.replicator {
            Replicator::Activating {
                step: Step::Gone {
                    snapshot: Some(_), ..
                },
                ctx,
                ..
            } => self.snapshots.last().is_some_and(|(last, family)| {
                last.lineage == ctx.lineage && last.position > ctx.acked && *family != owner.family
            }),
            _ => false,
        };
        Watch {
            fence,
            won: first_claim.filter(|_| repeat),
            rejected,
        }
    }

    /// Counts what the answer `watch` saw reached, now that owner `index`'s replicator took it.
    fn watched(&self, index: usize, watch: Watch) {
        let replicator = &self.owners[index].inst.replicator;
        let coverage = &self.coverage;
        let fenced = matches!(replicator, Replicator::Reset(ResetKind::Fenced));
        let fence = match watch.fence {
            Some(Fence::KeyConflict) if fenced => Some(&coverage.clones_fenced_key_conflict),
            Some(Fence::ReceiptEvicted) if fenced => Some(&coverage.clones_fenced_receipt_evicted),
            _ => None,
        };
        let won = match watch.won.filter(|_| replicator.ctx().is_some()) {
            Some(true) => Some(&coverage.repeats_won_continuation),
            Some(false) => Some(&coverage.repeats_won_takeover),
            None => None,
        };
        let rejected = watch.rejected.then_some(&coverage.clone_snapshots_rejected);
        for counter in [fence, won, rejected].into_iter().flatten() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn deliver(&mut self, at: usize) {
        if at >= self.msgs.len() {
            return;
        }
        match self.msgs.remove(at) {
            Msg::Request {
                to,
                id,
                request,
                late,
            } => self.deliver_request(to, id, *request, late, true),
            Msg::Answer {
                to,
                id,
                exchange,
                misrouted,
            } => self.deliver_answer(to, id, exchange, misrouted),
        }
    }

    fn pick(&self, pick: u16, requests: bool) -> Option<usize> {
        let candidates: Vec<usize> = self
            .msgs
            .iter()
            .enumerate()
            .filter(|(_, m)| matches!(m, Msg::Request { late: false, .. }) == requests)
            .filter(|(_, m)| requests || matches!(m, Msg::Answer { .. }))
            .map(|(i, _)| i)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        candidates
            .get(usize::from(pick) % candidates.len())
            .copied()
    }

    fn owner(&self, o: u8) -> usize {
        usize::from(o) % self.owners.len()
    }

    fn tick(&mut self, index: usize) {
        self.owners[index].inst.timer = None;
        self.event(index, Event::Tick);
    }

    fn flush(&mut self) {
        let mut budget = 2_000;
        while !self.msgs.is_empty() && budget > 0 {
            self.deliver(0);
            budget -= 1;
        }
    }

    fn step(&mut self, op: &Op) {
        let due: Vec<u64> = self
            .owners
            .iter()
            .filter(|o| o.inst.caught_up_due.is_some())
            .map(|o| o.inst.uid)
            .collect();
        self.apply_op(op);
        for uid in due {
            self.catch_up(uid);
        }
    }

    /// Sends the instance `uid` its due `CaughtUp`, if it still waits for one.
    fn catch_up(&mut self, uid: u64) {
        let Some(index) = self.owners.iter().position(|o| o.inst.uid == uid) else {
            return;
        };
        if let Some(last) = self.owners[index].inst.caught_up_due.take() {
            // A rebuild reads under its claim's token: a history change since, which an answer
            // from before it hid, fails the rebuild before it publishes, and the takeover resets
            // `rewound` with the old generation whole. Its ACK records stay as durable as they
            // were: the rebuild let go of the old log, which the reset does not fsync.
            let token = self.token();
            let inst = &mut self.owners[index].inst;
            if inst.unpublished.is_some()
                && inst
                    .rebuilt_bytes
                    .as_ref()
                    .is_some_and(|(t, _)| *t != token)
            {
                inst.unpublished = None;
                inst.rebuilt_bytes = None;
                inst.replicator = Replicator::Reset(ResetKind::Rewound);
                self.coverage.rewound.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if let Some(discard_from) = self.owners[index].inst.unpublished.take() {
                self.publish(index, discard_from);
            }
            self.event(index, Event::CaughtUp {
                last,
                log_bytes: 0,
                lineage: self.owners[index].log.lineage,
            });
        }
    }

    /// The rebuilt generation replaces the old one: the discarded tail goes, the lineage is new
    /// and random, and the ACK of the bind that follows is durable (a chunk's is already).
    fn publish(&mut self, index: usize, discard_from: Option<Position>) {
        let mut lineage = [0x7a; 16];
        lineage[..8].copy_from_slice(&self.next_uid.to_le_bytes());
        self.next_uid += 1;
        let log = &mut self.owners[index].log;
        if let Some(from) = discard_from {
            log.commits.retain(|p, _| *p < from.0);
        }
        log.lineage = Lineage(lineage);
        log.catching_up = true;
        log.rebuilt_through = log.next_position.saturating_sub(1);
        if let Some((_, bytes)) = self.owners[index].inst.rebuilt_bytes.take() {
            self.owners[index].log.rebuilt = bytes;
        }
        let log = &mut self.owners[index].log;
        log.rekeyed_at_open = None;
        log.durable_through = log.rebuilt_through;
    }

    /// The owner's host loses power: up to `k` of its log's unsynced trailing commits are gone,
    /// with the ACK records after them, and the next commits at those positions carry other
    /// bytes. Its process stops; requests it had out may still land.
    fn power_loss(&mut self, index: usize, k: u8) {
        let owner = &mut self.owners[index];
        let dropped = owner.log.drop_unsynced(usize::from(k));
        if !dropped.is_empty() {
            owner.power_lost = true;
            owner.salt = owner.salt.wrapping_add(128);
            self.power_lost.extend(dropped);
        }
        let (acked, acked_offset) = (owner.inst.acked, owner.inst.acked_offset);
        let inst = self.stopped(acked, acked_offset);
        self.owners[index].inst = inst;
    }

    fn apply_op(&mut self, op: &Op) {
        match *op {
            Op::Commit(o) => {
                let i = self.owner(o);
                self.commit(i);
            }
            Op::Deliver(pick) => {
                if !self.msgs.is_empty() {
                    self.deliver(usize::from(pick) % self.msgs.len());
                }
            }
            Op::Flush => self.flush(),
            Op::DropRequest(pick) => {
                if let Some(at) = self.pick(pick, true)
                    && let Msg::Request { to, id, .. } = self.msgs.remove(at)
                {
                    self.msgs.push(Msg::Answer {
                        to,
                        id,
                        exchange: Exchange::NoAnswer(NoAnswer::Lost {
                            reason: "request lost".to_owned(),
                        }),
                        misrouted: false,
                    });
                }
            }
            Op::DropAnswerAfterApply(pick) => {
                if let Some(at) = self.pick(pick, true)
                    && let Msg::Request {
                        to, id, request, ..
                    } = self.msgs.remove(at)
                {
                    self.deliver_request(to, id, *request, false, false);
                }
            }
            Op::DropAnswer(pick) => {
                if let Some(at) = self.pick(pick, false)
                    && let Some(Msg::Answer { exchange, .. }) = self.msgs.get_mut(at)
                {
                    *exchange = Exchange::NoAnswer(NoAnswer::Lost {
                        reason: "answer lost".to_owned(),
                    });
                }
            }
            Op::Duplicate(pick) => {
                if let Some(at) = self.pick(pick, true)
                    && let Some(Msg::Request {
                        to, id, request, ..
                    }) = self.msgs.get(at).cloned()
                {
                    self.msgs.push(Msg::Request {
                        to,
                        id,
                        request,
                        late: true,
                    });
                }
            }
            Op::Fault(kind, post_only, times) => {
                let fault = match kind {
                    FaultKind::Status(status, retry_after) => Fault::Status {
                        status,
                        retry_after,
                    },
                    FaultKind::Redirect => Fault::Redirect {
                        location: "http://elsewhere/s".to_owned(),
                    },
                    FaultKind::UntypedTooLarge => Fault::UntypedTooLarge,
                    FaultKind::Unreachable => Fault::Unreachable,
                };
                self.server.inject(Scripted {
                    method: post_only.then_some(Method::Post),
                    fault,
                    times: u32::from(times),
                });
            }
            Op::LowerLimit => self.server.set_limits(Limits {
                max_request_bytes: 48,
                ..self.server.limits()
            }),
            Op::Advance(ms) => self.server.advance(u64::from(ms)),
            Op::ExpireIdle => {
                let stream = self.stream();
                for owner in &mut self.owners {
                    if let Replicator::Activating {
                        kind: ClaimKind::Continuation { .. },
                        step: Step::Claim { epoch, .. },
                        ctx,
                    } = &owner.inst.replicator
                    {
                        let claim = frame::encode_claim(*epoch, ctx.activation);
                        if stream.windows(claim.len()).any(|w| w == &claim[..]) {
                            owner.inst.claim_expired = true;
                        }
                    }
                }
                self.server.advance(IDLE_RETIREMENT_MS + 1);
            }
            Op::Tick(o) => {
                let i = self.owner(o);
                self.tick(i);
            }
            Op::NewHistory => self.server.history_event(HistoryEvent::NewHistory).unwrap(),
            Op::Restore(n) => {
                if (0..self.owners.len()).any(|i| self.shares_lineage(i)) {
                    self.coverage
                        .restores_shared_lineage
                        .fetch_add(1, Ordering::Relaxed);
                }
                let latest = self.checkpoints.len() - 1;
                let (checkpoint, max) = self.checkpoints[latest - usize::from(n) % (latest + 1)];
                self.max_claimed = max;
                self.server
                    .history_event(HistoryEvent::Restore(checkpoint))
                    .unwrap();
                // Property 8 is about one history: acknowledgements the restore lost go with it.
                let stream = self.stream();
                self.acked_frames
                    .retain(|_, frame| stream.windows(frame.len()).any(|w| w == &frame[..]));
            }
            Op::Misroute(on, exists) => {
                if exists && self.other.frames(PATH).is_none() {
                    let create = Request {
                        content_type: Some(STREAM_CONTENT_TYPE.to_owned()),
                        ..Request::new(Method::Put, Target::Stream)
                    };
                    assert_eq!(self.other.handle(PATH, &create).status, 201);
                }
                self.misrouted = on;
            }
            Op::Retain(o) => {
                let i = self.owner(o);
                // A snapshot publish carries the binding's token: one of an owner left on another
                // history gets a `412` and publishes nothing.
                if self.owners[i].log.incarnation != self.token() {
                    return;
                }
                let offset = self.owners[i].inst.acked_offset;
                // A snapshot at the offset first, of the owner's lineage at its acknowledged
                // position: retention never passes the latest snapshot.
                let last = CommitId {
                    lineage: self.owners[i].log.lineage,
                    position: self.owners[i].inst.acked,
                };
                // The snapshot thread fsyncs ACK(k, W) before it publishes: no restart reads an
                // older ACK.
                self.owners[i].log.sync();
                let by = self.owners[i].family;
                self.publish_snapshot(offset, last, by);
                let request = Request::new(Method::Put, Target::Retention { offset });
                drop(self.server.handle(PATH, &request));
            }
            Op::Boundary(o) => {
                let i = self.owner(o);
                if let Some(&k) = self.owners[i].log.commits.keys().last() {
                    self.event(i, Event::Boundary(Position(k)));
                }
            }
            Op::Checkpoint => {
                let id = self.server.checkpoint();
                self.checkpoints.push((id, self.max_claimed));
            }
            Op::ForeignAppend(kind) => {
                self.others = true;
                let bytes = match kind {
                    0 => b"garbage, not a frame".to_vec(),
                    1 => frame::encode(&Frame::Commit {
                        id: CommitId {
                            lineage: Lineage([0xee; 16]),
                            position: Position(self.next_ticket),
                        },
                        compressed: vec![0xee; 6],
                    })
                    .unwrap(),
                    _ => frame::encode_claim(
                        Epoch::try_from(self.max_epoch).unwrap(),
                        Activation([0xfd; 16]),
                    ),
                };
                drop(self.server.foreign_append(PATH, &bytes));
            }
            Op::ForeignClaim(above) => {
                self.others = true;
                let epoch = if above {
                    self.max_epoch + 1
                } else {
                    self.max_epoch
                };
                let epoch = Epoch::try_from(epoch).unwrap();
                let activation = Activation([0xfe; 16]);
                let body = frame::encode_claim(epoch, activation);
                if let Ok(raw) = self.server.foreign_claim(PATH, epoch, &body) {
                    let request = Request {
                        keys: ursula_headers::ProducerRequestHeaders {
                            producer: Some(ursula_headers::ProducerWrite {
                                id: ursula_headers::ProducerId::new(crate::wire::PRODUCER_ID)
                                    .unwrap(),
                                epoch,
                                seq: Seq::ZERO,
                                digest: None,
                            }),
                            precondition: None,
                        },
                        ..Request::new(Method::Post, Target::Stream)
                    };
                    let parsed = raw.parse(&request);
                    // No instance has uid 0: the claim is another claimant's.
                    self.applied(0, None, &parsed.typed, &request);
                }
            }
            Op::Clone(o) => {
                let i = self.owner(o);
                self.clone_owner(i);
            }
            Op::Restart(o, lag) => {
                let i = self.owner(o);
                self.restart(i, lag, false);
            }
            Op::Reopen(o) => {
                let i = self.owner(o);
                self.restart(i, 0, true);
            }
            Op::CopyCache(o, durable, offline) => {
                let i = self.owner(o);
                self.copy_cache(i, durable, offline);
            }
            Op::ProvisionalRestart(o, lag) => {
                let i = self.owner(o);
                self.provisional_restart(i, lag, false);
            }
            Op::Partition(o, on) => {
                let family = self.owners[self.owner(o)].family;
                if on {
                    self.partitioned.insert(family);
                } else {
                    self.partitioned.remove(&family);
                }
            }
            Op::PowerLoss(o, k) => {
                let i = self.owner(o);
                self.power_loss(i, k);
            }
            Op::Takeover(lagging) => self.takeover(lagging),
            Op::Sync(o) => {
                let i = self.owner(o);
                self.sync(i);
            }
        }
    }

    /// A perfect network, the route restored and no owner cut off, until nothing is left to do,
    /// then properties 7, 11 and 17.
    fn heal(&mut self) {
        self.misrouted = false;
        self.partitioned.clear();
        let limits = self.server.limits();
        self.server.set_limits(Limits {
            max_request_bytes: Limits::default().max_request_bytes,
            ..limits
        });
        let until = self.now() + HEAL_MS;
        for _ in 0..50_000 {
            if let Some(uid) = self
                .owners
                .iter()
                .find(|o| o.inst.caught_up_due.is_some())
                .map(|o| o.inst.uid)
            {
                self.catch_up(uid);
                continue;
            }
            if !self.msgs.is_empty() {
                self.deliver(0);
                continue;
            }
            let next = self
                .owners
                .iter()
                .enumerate()
                .filter_map(|(i, o)| o.inst.timer.map(|t| (t, i)))
                .min();
            let Some((at, index)) = next else { break };
            if at > until {
                break;
            }
            let now = self.now();
            if at > now {
                self.server.advance(at - now);
            }
            self.tick(index);
        }
        for index in 0..self.owners.len() {
            let ending = ending(&self.owners[index].inst);
            match ending {
                Err(state) => {
                    self.violation(format!("property 7: owner {index} ends stuck: {state}"));
                }
                Ok("acknowledged") if !self.owners[index].inst.calls.is_empty() => {
                    let calls: Vec<u64> = self.owners[index].inst.calls.keys().copied().collect();
                    self.violation(format!(
                        "property 11: owner {index} ends acknowledged with tickets {calls:?} waiting"
                    ));
                }
                Ok(_) => {}
            }
            let blocked = self.owners[index].inst.replicator.ctx().is_some_and(|ctx| {
                ctx.condition == Some(Condition::Blocked(BlockedReason::WrongCluster))
            });
            if blocked {
                self.violation(format!(
                    "property 17: owner {index} ends blocked wrong_cluster"
                ));
            }
        }
        let stream = self.stream();
        let lost = self.acknowledged.iter().find(|frame| {
            let within = |bytes: &[u8]| bytes.windows(frame.len()).any(|w| w == &frame[..]);
            let held = self.owners.iter().any(|o| {
                o.log.commits.values().any(|p| p.frame == **frame)
                    || o.log.applied.contains(frame)
                    || within(&o.log.rebuilt)
            });
            !within(&stream)
                && !held
                && !self.salvaged.iter().any(|s| within(s))
                && !self.power_lost.contains(frame)
        });
        if let Some(frame) = lost {
            self.violation(format!(
                "property 19: an acknowledged commit is in no stream, log or salvage: {frame:?}"
            ));
        }
    }
}

/// How a run ended: acknowledged, retrying, blocked or reset, or stuck.
fn ending(inst: &Instance) -> Result<&'static str, String> {
    let replicator = &inst.replicator;
    let Some(ctx) = replicator.ctx() else {
        return Ok("reset");
    };
    match &ctx.condition {
        Some(Condition::Blocked(_)) => return Ok("blocked"),
        Some(Condition::Retrying) if inst.timer.is_some() => return Ok("retrying"),
        _ => {}
    }
    match replicator {
        Replicator::Streaming {
            work: Work::Idle, ..
        } if ctx.pending.is_empty() => Ok("acknowledged"),
        Replicator::Streaming { work, .. } => Err(format!(
            "streaming with {} pending, {}",
            ctx.pending.len(),
            match work {
                Work::Idle => "idle",
                Work::Append { .. } => "an append outstanding",
            }
        )),
        Replicator::Activating { step, .. } => Err(format!("activating, {}", match step {
            Step::Claim { .. } => "claiming",
            Step::Reconcile { .. } => "reconciling",
            Step::Applying { .. } => "applying a read",
            Step::CatchingUp(_) => "catching up",
            Step::Gone { .. } => "reading the latest snapshot after a 410",
            Step::PreCheck { .. } => "pre-checking",
        })),
        Replicator::Reset(_) | Replicator::Refused(_) | Replicator::Stopped => Ok("reset"),
    }
}

/// Runs `ops` then heals; the violations found.
fn run(knobs: Knobs, seed: u64, ops: &[Op]) -> Vec<String> {
    run_counted(knobs, seed, ops, Arc::default())
}

/// [`run`], counting what the script reached in `coverage`.
fn run_counted(knobs: Knobs, seed: u64, ops: &[Op], coverage: Arc<Coverage>) -> Vec<String> {
    let mut world = World::new(knobs, seed, coverage);
    for op in ops {
        world.step(op);
    }
    world.heal();
    world.violations
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(CASES)
}

/// Runs `cases` random scripts, from a fixed seed when `deterministic`, and returns what they
/// reached.
fn check(
    knobs: Knobs,
    cases: u32,
    deterministic: bool,
) -> Result<Arc<Coverage>, proptest::test_runner::TestError<(u64, Vec<Op>)>> {
    let coverage = Arc::new(Coverage::default());
    let config = Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = if deterministic {
        TestRunner::new_with_rng(config, TestRng::deterministic_rng(RngAlgorithm::ChaCha))
    } else {
        TestRunner::new(config)
    };
    let strategy = (
        proptest::num::u64::ANY,
        proptest::collection::vec(op(), 20..160),
    );
    runner.run(&strategy, |(seed, ops)| {
        // One seed in eight runs every instance under one activation, inside the same cases.
        let knobs = Knobs {
            shared_activation: knobs.shared_activation || seed % 8 == 0,
            ..knobs
        };
        let violations = run_counted(knobs, seed, &ops, Arc::clone(&coverage));
        if violations.is_empty() {
            Ok(())
        } else {
            Err(TestCaseError::fail(violations.join("; ")))
        }
    })?;
    Ok(coverage)
}

#[test]
fn the_replicator_keeps_its_properties_against_the_model() {
    if let Err(failure) = check(Knobs::default(), cases(), false) {
        panic!("{failure}");
    }
}

/// The scripts reach a catch-up in place read by read, one that rebuilds midway, re-keys while
/// streaming, while a continuation claim is out and at open, `rewound` resets, owners blocked
/// `wrong_cluster` that resumed, provisional opens that went live, were fenced and went stale,
/// clones and copies fenced by `key-conflict` and by `receipt-evicted`, claims of either kind won
/// by a repeat on their first attempt, copies of a cache that took over, offline copies that went
/// live, stale or fenced, clone snapshots the `410` rule rejected, and restores while two owners
/// share a lineage: checked on a fixed seed, so the coverage is reproducible.
#[test]
fn the_world_catches_up_in_place_read_by_read() {
    let coverage = match check(Knobs::default(), COVERAGE_CASES, true) {
        Ok(coverage) => coverage,
        Err(failure) => panic!("{failure}"),
    };
    let counts = [
        ("chunked catch-ups", &coverage.chunked_catch_ups),
        ("rebuilds after an apply", &coverage.rebuilds_after_apply),
        ("re-keys while streaming", &coverage.rekeys_streaming),
        ("re-keys during a claim", &coverage.rekeys_claiming),
        ("re-keys at open", &coverage.rekeys_open),
        ("rewound resets", &coverage.rewound),
        ("blocks that resumed", &coverage.blocks_resumed),
        ("salvages at open", &coverage.salvages),
        (
            "provisional opens that went live",
            &coverage.provisional_live,
        ),
        ("provisional opens fenced", &coverage.provisional_fenced),
        ("provisional opens gone stale", &coverage.provisional_stale),
        (
            "clones fenced by key-conflict",
            &coverage.clones_fenced_key_conflict,
        ),
        (
            "clones fenced by receipt-evicted",
            &coverage.clones_fenced_receipt_evicted,
        ),
        (
            "continuations won by a repeat",
            &coverage.repeats_won_continuation,
        ),
        ("takeovers won by a repeat", &coverage.repeats_won_takeover),
        ("copies that took over", &coverage.copies_took_over),
        (
            "offline copies that went live",
            &coverage.offline_copies_live,
        ),
        ("offline copies gone stale", &coverage.offline_copies_stale),
        ("offline copies fenced", &coverage.offline_copies_fenced),
        (
            "clone snapshots rejected",
            &coverage.clone_snapshots_rejected,
        ),
        (
            "restores of a shared lineage",
            &coverage.restores_shared_lineage,
        ),
    ];
    let mut missing = Vec::new();
    for (what, count) in counts {
        if count.load(Ordering::Relaxed) == 0 {
            missing.push(what);
        }
    }
    assert!(missing.is_empty(), "none of: {missing:?}");
}

/// The world with the replicators' negative controls `knobs`.
fn replicator_knobs(knobs: ReplicatorKnobs) -> Knobs {
    Knobs {
        replicator: knobs,
        ..Knobs::default()
    }
}

/// Owner 0 acknowledges a commit after a checkpoint, the stream is restored to that checkpoint,
/// and owner 0 commits again: its append meets the new token, whose history lost the
/// acknowledged commit.
fn lossy_restore() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Checkpoint,
        Op::Commit(0),
        Op::Flush,
        Op::Restore(0),
        Op::Commit(0),
        Op::Flush,
    ]
}

#[test]
fn negative_control_re_keying_on_any_token_drops_acknowledged_records() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &lossy_restore() {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Reset(ResetKind::Rewound)
    ));
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            rekey_always: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &lossy_restore(),
    );
    assert!(
        violations
            .iter()
            .any(|v| v.starts_with("property 15") || v.starts_with("property 2")),
        "{violations:?}"
    );
}

/// Owner 0's continuation claim (after idle expiry) applies and its answer is lost; a history
/// event that loses nothing follows, and the resent claim meets the new token. The re-key sends
/// it again, same epoch, under the new token, and gets a repeat receipt: it applied before the
/// history event, so the producer record stays history-fenced (extensions: Producer Write Answers,
/// `epoch-closed;by=history`).
fn repeat_after_a_rekey() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::ExpireIdle,
        Op::Commit(0),
        // The append meets `producer-expired`, and the continuation claim goes out.
        Op::Deliver(0),
        Op::Deliver(0),
        // The claim applies; its answer is lost.
        Op::DropAnswerAfterApply(0),
        Op::Deliver(0),
        Op::NewHistory,
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
    ]
}

#[test]
fn negative_control_binding_on_a_repeat_after_a_rekey_fences_falsely() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &repeat_after_a_rekey() {
        world.step(op);
    }
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Streaming { .. }
    ));
    assert!(world.coverage.rekeys_claiming.load(Ordering::Relaxed) > 0);
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            no_claim_after_repeat: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &repeat_after_a_rekey(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 13")),
        "{violations:?}"
    );
}

/// Owner 0 binds and never writes under its claim, then restarts offline: the open goes
/// provisional, the pre-check has nothing to read, and the claim sent again meets a repeat whose
/// receipt ends at the acknowledged offset.
fn provisional_over_an_idle_claim() -> Vec<Op> {
    vec![Op::Flush, Op::ProvisionalRestart(0, 0), Op::Flush]
}

#[test]
fn negative_control_guarding_the_repeat_start_resets_an_idle_owners_provisional_open() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &provisional_over_an_idle_claim() {
        world.step(op);
    }
    assert!(
        matches!(
            world.owners[0].inst.replicator,
            Replicator::Streaming { .. }
        ),
        "{:?}",
        world.owners[0].inst.replicator
    );
    // The provisional open went live with one continuation claim over its own: [own, own + 1].
    let epochs: Vec<u64> = world.owners[0]
        .log
        .claims
        .iter()
        .map(|(epoch, _)| epoch.get())
        .collect();
    assert!(
        matches!(epochs.as_slice(), [.., own, next] if own.checked_add(1) == Some(*next)),
        "{epochs:?}"
    );
    assert!(world.coverage.provisional_live.load(Ordering::Relaxed) > 0);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            guard_repeat_start: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &provisional_over_an_idle_claim(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 21")),
        "{violations:?}"
    );
}

/// Owner 0's requests go to another deployment, with and without a stream at the path, and
/// come back.
fn misrouted_and_back(exists: bool) -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Misroute(true, exists),
        Op::Commit(0),
        Op::Flush,
        Op::Sync(0),
        Op::Flush,
    ]
}

#[test]
fn negative_control_a_misroute_taken_for_a_deleted_stream_resets() {
    for exists in [false, true] {
        let mut world = World::new(Knobs::default(), 1, Arc::default());
        for op in &misrouted_and_back(exists) {
            world.step(op);
        }
        let ctx = world.owners[0].inst.replicator.ctx().unwrap();
        assert_eq!(
            ctx.condition,
            Some(Condition::Blocked(BlockedReason::WrongCluster))
        );
        world.heal();
        assert_eq!(world.violations, Vec::<String>::new());
        assert!(world.coverage.blocks_resumed.load(Ordering::Relaxed) > 0);
        let violations = run(
            replicator_knobs(ReplicatorKnobs {
                misroute_as_deleted: true,
                ..ReplicatorKnobs::default()
            }),
            1,
            &misrouted_and_back(exists),
        );
        assert!(
            violations.iter().any(|v| v.starts_with("property 17")),
            "{violations:?}"
        );
    }
}

#[test]
fn negative_control_a_misroute_taken_for_a_history_writes_to_the_other_deployment() {
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            misroute_as_rekey: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &misrouted_and_back(true),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 16")),
        "{violations:?}"
    );
}

/// A history that loses nothing while owner 0 streams re-keys it with no reset, and a restart
/// after another one re-keys at open; a restore that lost an acknowledged commit resets the
/// streaming owner `rewound`, and the reopen salvages its log, with the lost commit, and
/// continues as a fresh takeover.
#[test]
fn history_events_re_key_or_reset_rewound() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::NewHistory,
        Op::Commit(0),
        Op::Flush,
        Op::NewHistory,
        Op::Restart(0, 0),
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Streaming { .. }
    ));
    assert_eq!(world.owners[0].log.incarnation, world.token());
    assert_eq!(world.coverage.rekeys_streaming.load(Ordering::Relaxed), 1);
    assert_eq!(world.coverage.rekeys_open.load(Ordering::Relaxed), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());

    let mut world = World::new(Knobs::default(), 1, Arc::default());
    let lost = {
        for op in &lossy_restore() {
            world.step(op);
        }
        world.owners[0]
            .log
            .commits
            .values()
            .next()
            .unwrap()
            .frame
            .clone()
    };
    for op in [Op::Restart(0, 0), Op::Flush] {
        world.step(&op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Streaming { .. }
    ));
    assert_eq!(world.coverage.salvages.load(Ordering::Relaxed), 1);
    assert!(world.salvaged.contains(&lost));
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Owner 0 acknowledges a commit after a checkpoint (its ACK record not durable), the stream is
/// restored to that checkpoint and another writer commits, and owner 0 restarts with the lagging
/// ACK, under which the history inherits its log, and restarts again before its takeover binds.
/// With the #611 history gate (design: History) the first restart salvages the log; undone, it
/// re-keys the log with the acknowledged commit pending, the second restart resumes the re-keyed
/// log, and its takeover discards the commit with no salvage.
fn crash_after_a_rekey_at_open() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Checkpoint,
        Op::Commit(0),
        Op::Flush,
        Op::Restore(0),
        Op::ForeignAppend(1),
        Op::Restart(0, 1),
        Op::Restart(0, 0),
        Op::Flush,
    ]
}

/// A re-key while bound checks inheritance against the exact acknowledged offset, so the history
/// holds every commit the binding acknowledged: a later takeover that discards the pending tail
/// after a reset loses nothing acknowledged, and property 18 does not count that re-key as one at
/// open. A script the 1M-case run found when it did.
#[test]
fn a_rekey_while_bound_is_not_one_at_open() {
    let ops = vec![
        Op::Deliver(0),
        Op::ForeignAppend(1),
        Op::Deliver(0),
        Op::Commit(0),
        Op::ExpireIdle,
        Op::Deliver(0),
        Op::Deliver(0),
        Op::Deliver(0),
        Op::NewHistory,
        Op::Commit(0),
        Op::Flush,
        Op::Commit(0),
        Op::Commit(0),
        Op::Commit(0),
        Op::Retain(0),
        Op::Restart(0, 0),
        Op::Checkpoint,
        Op::Commit(0),
        Op::Checkpoint,
        Op::Commit(0),
    ];
    assert_eq!(run(Knobs::default(), 0, &ops), Vec::<String>::new());
}

/// A restart with a lagging ACK whose reconcile meets a `410` past it rebuilds, discarding the
/// acknowledged commit (it is in the snapshot the rebuild installs). A restore lands before the
/// rebuild reads: the rebuild meets the new token and resets `rewound`, the old generation whole,
/// so the commit the restore lost stays in the log (property 19). A script the 1M-case run found.
#[test]
fn a_rebuild_overtaken_by_a_restore_keeps_the_old_generation() {
    let mut ops = vec![
        Op::Commit(0),
        Op::Commit(0),
        Op::Advance(0),
        Op::Deliver(0),
        Op::Deliver(0),
        Op::Commit(0),
        Op::Deliver(0),
        Op::Deliver(0),
        Op::Sync(0),
        Op::Retain(0),
        Op::Restart(0, 1),
        Op::Duplicate(0),
    ];
    ops.extend(std::iter::repeat_n(Op::Deliver(0), 10));
    ops.extend([Op::Restore(0), Op::Commit(0), Op::Commit(0)]);
    assert_eq!(run(Knobs::default(), 0, &ops), Vec::<String>::new());
}

#[test]
fn negative_control_re_keying_with_commits_pending_discards_them_with_no_salvage() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &crash_after_a_rekey_at_open() {
        world.step(op);
    }
    assert_eq!(world.coverage.salvages.load(Ordering::Relaxed), 1);
    assert_eq!(world.coverage.rekeys_open.load(Ordering::Relaxed), 0);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Streaming { .. }
    ));
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            rekey_with_pending: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &crash_after_a_rekey_at_open(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 18")),
        "{violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 19")),
        "{violations:?}"
    );
}

/// [`crash_after_a_rekey_at_open`] with owner 0 cloned first: the commit the negative control
/// loses is one the clone's copy could have committed at too. Property 19 once left every commit
/// after a clone out, so only property 18 tripped here; it now checks them all.
#[test]
fn negative_control_re_keying_with_commits_pending_loses_a_commit_after_a_clone() {
    let mut ops = crash_after_a_rekey_at_open();
    ops.insert(1, Op::Clone(0));
    assert_eq!(run(Knobs::default(), 1, &ops), Vec::<String>::new());
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            rekey_with_pending: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &ops,
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 18")),
        "{violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 19")),
        "{violations:?}"
    );
}

/// Streaming owner 0 acknowledges a commit, is cloned, and both copies commit different bytes at
/// the same position.
fn divergent_clones() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Clone(0),
        Op::Commit(0),
        Op::Commit(1),
        Op::Flush,
    ]
}

/// Owner 0 sends a batch whose answer is lost after it applied, logs another commit, and
/// resends.
fn resend_after_a_lost_answer() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Commit(0),
        Op::DropAnswerAfterApply(0),
        Op::Commit(0),
        Op::Deliver(0),
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
    ]
}

#[test]
fn negative_control_the_model_without_digests_breaks_clone_safety() {
    assert_eq!(
        run(Knobs::default(), 1, &divergent_clones()),
        Vec::<String>::new()
    );
    let violations = run(
        Knobs {
            strip_digests: true,
            ..Knobs::default()
        },
        1,
        &divergent_clones(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 8")),
        "{violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 22")),
        "{violations:?}"
    );
}

#[test]
fn negative_control_a_renumbering_replicator_sends_two_bodies_under_one_key() {
    assert_eq!(
        run(Knobs::default(), 1, &resend_after_a_lost_answer()),
        Vec::<String>::new()
    );
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            renumber: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &resend_after_a_lost_answer(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 3")),
        "{violations:?}"
    );
}

#[test]
fn negative_controls_fail_the_proptest_too() {
    let mut passed = Vec::new();
    for knobs in [
        Knobs {
            strip_digests: true,
            ..Knobs::default()
        },
        replicator_knobs(ReplicatorKnobs {
            renumber: true,
            ..ReplicatorKnobs::default()
        }),
        replicator_knobs(ReplicatorKnobs {
            rekey_always: true,
            ..ReplicatorKnobs::default()
        }),
        // `no_claim_after_repeat` is left to its fixed script: its false fence needs a claim
        // applied with its answer lost, then a history event before the resend. Random seeds do
        // reach it (one 300k run did), but this fixed seed does not within the CI budget.
        replicator_knobs(ReplicatorKnobs {
            misroute_as_deleted: true,
            ..ReplicatorKnobs::default()
        }),
        replicator_knobs(ReplicatorKnobs {
            misroute_as_rekey: true,
            ..ReplicatorKnobs::default()
        }),
        Knobs {
            claim_without_pre_check: true,
            ..Knobs::default()
        },
        replicator_knobs(ReplicatorKnobs {
            guard_repeat_start: true,
            ..ReplicatorKnobs::default()
        }),
        // `settle_while_activating` is left to its fixed script too: its early `Synced` needs a
        // proof from before a continuation claim and a `synced()` call between two reads of
        // that claim's reconcile.
        // `trust_clone_snapshot`, `match_commits_by_id` and `probe_skips_own_claims` are left to
        // their fixed scripts as well: each needs two copies of one lineage at one moment (a
        // copy's snapshot above the other's acknowledged position read by a `410`, a copy's commit
        // at the id of the other's pending record in its reconcile, a copy's claim past the other's
        // tail when it probes), which this fixed seed does not reach within 512 cases.
    ] {
        // A fixed seed: the fixed scripts above always trip, and this search is a bonus that
        // must not flake.
        if check(knobs, 512, true).is_ok() {
            passed.push(knobs);
        }
    }
    assert!(passed.is_empty(), "{passed:?} passed");
}

/// A clone commits at the next position with its own bytes, its snapshot of our shared lineage
/// moves retention past the original's acknowledged offset, and the original's commit at the same
/// position meets the `410`. The rule must not take the clone's snapshot as covering the
/// original's own record above its acknowledged position: that would acknowledge a commit the
/// stream never held (property 8) and append after another copy's state.
fn a_clone_snapshot_past_the_original() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Clone(0),
        Op::Commit(1),
        Op::Flush,
        Op::Retain(1),
        Op::Commit(0),
        Op::Flush,
        Op::Restart(0, 0),
        Op::Flush,
    ]
}

#[test]
fn a_clones_snapshot_never_covers_the_originals_commit() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &a_clone_snapshot_past_the_original() {
        world.step(op);
    }
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    assert_eq!(
        world
            .coverage
            .clone_snapshots_rejected
            .load(Ordering::Relaxed),
        1
    );
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            trust_clone_snapshot: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &a_clone_snapshot_past_the_original(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 8")),
        "{violations:?}"
    );
}

/// A host whose build lags reconciles from below retention: the `410` rebuilds, never skips the
/// frames it cannot read.
#[test]
fn a_lagging_takeover_below_retention_rebuilds() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Retain(0),
        Op::Takeover(true),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    assert!(matches!(
        world.owners[1].inst.replicator,
        Replicator::Streaming { .. }
    ));
}

/// A takeover's reconcile meets a `410` below retention and rebuilds, discarding the record whose
/// append was lost, and the process restarts before the rebuilt generation is published. The old
/// generation stays whole until the publishing rename (design: Layout, Materialize), so the restart
/// finds the record still pending, and a `synced()` call after it waits for that record.
#[test]
fn a_restart_before_a_rebuild_publishes_keeps_the_old_generation() {
    let ops = vec![
        Op::Flush,
        Op::Retain(0),
        Op::Commit(0),
        Op::DropRequest(0),
        Op::Restart(0, 1),
        Op::Flush,
        Op::Restart(0, 0),
        Op::Flush,
        Op::Sync(0),
        Op::Flush,
    ];
    assert_eq!(run(Knobs::default(), 0, &ops), Vec::<String>::new());
}

/// The proptest's shrunk counterexamples, each a ticket resolved below its target (property 9).
/// The first three commit while a takeover is still opening, then restart before a rebuild that
/// discarded the commit is published. The last calls `synced()` while the takeover is opening,
/// and the ticket resolved between two reads of a later continuation's reconcile. The world no
/// longer commits or calls `synced()` during a takeover (the driver takes neither before the
/// bind, design: Open), so the scripts now pass without reaching those steps: the test above and
/// `machine::a_ticket_waits_for_the_continuation_reconcile_to_finish` keep both rules.
#[test]
fn the_proptests_shrunk_counterexamples_pass() {
    use Op::Checkpoint;
    use Op::Commit;
    use Op::Deliver;
    use Op::DropAnswerAfterApply;
    use Op::Duplicate;
    use Op::ExpireIdle;
    use Op::Fault;
    use Op::Flush;
    use Op::ForeignClaim;
    use Op::Restart;
    use Op::Retain;
    use Op::Sync;
    use Op::Takeover;
    use Op::Tick;
    let scripts: [(u64, Vec<Op>); 4] = [
        (0, vec![
            Deliver(0),
            Deliver(0),
            Restart(0, 0),
            Deliver(0),
            Retain(0),
            Deliver(0),
            Restart(0, 2),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Commit(0),
            Deliver(0),
            Restart(0, 0),
            Sync(0),
            ForeignClaim(false),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Commit(0),
        ]),
        (0, vec![
            Flush,
            Retain(0),
            Restart(0, 1),
            Deliver(0),
            Deliver(0),
            Commit(0),
            Deliver(0),
            Deliver(0),
            Restart(0, 0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Sync(0),
            Deliver(0),
            Commit(0),
            Commit(0),
            Takeover(false),
            Commit(0),
            Duplicate(0),
        ]),
        (
            0,
            [
                ForeignClaim(false),
                Flush,
                Retain(0),
                Restart(0, 1),
                Commit(0),
                Flush,
                Restart(0, 0),
                Flush,
                Sync(0),
                Commit(0),
                Flush,
            ]
            .into_iter()
            .chain(std::iter::repeat_n(Commit(0), 9))
            .collect(),
        ),
        (1_858_832_379_911_420_328, vec![
            Fault(FaultKind::Unreachable, false, 2),
            Deliver(0),
            Commit(0),
            Deliver(0),
            Sync(0),
            Tick(0),
            Deliver(0),
            Deliver(0),
            Tick(0),
            Deliver(0),
            Commit(0),
            Deliver(0),
            DropAnswerAfterApply(0),
            Deliver(0),
            Checkpoint,
            ExpireIdle,
            Tick(0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Deliver(0),
            Sync(0),
        ]),
    ];
    for (seed, ops) in &scripts {
        assert_eq!(run(Knobs::default(), *seed, ops), Vec::<String>::new());
    }
}

/// A restore, a re-key at open, a clone and a history event, then retention moved to the
/// acknowledged offset of an owner left on the first history (found at a million cases): that
/// owner's snapshot publish carries its old token and gets a `412`, so it never hides foreign
/// frames behind a snapshot of the current history.
#[test]
fn an_owner_on_another_history_publishes_no_snapshot() {
    use Op::Clone;
    use Op::Commit;
    use Op::Deliver;
    use Op::DropRequest;
    use Op::Duplicate;
    use Op::Restart;
    let ops = vec![
        Commit(0),
        Commit(0),
        Restart(0, 0),
        Op::Flush,
        Deliver(0),
        Clone(0),
        Commit(6),
        Deliver(0),
        Restart(6, 0),
        Op::Restore(0),
        Restart(6, 1),
        Commit(5),
        Commit(0),
        DropRequest(32),
        Deliver(19),
        Duplicate(0),
        Clone(0),
        Deliver(13),
        Deliver(26),
        Op::Advance(0),
        Commit(0),
        Commit(0),
        Deliver(0),
        Op::NewHistory,
        Duplicate(0),
        Deliver(4),
        Commit(0),
        DropRequest(0),
        Clone(0),
        Op::ForeignAppend(2),
        Op::Retain(1),
    ];
    assert_eq!(run(Knobs::default(), 0, &ops), Vec::<String>::new());
}

/// A lagging takeover re-keys at open, its claim applies under the restored history, and a history
/// event re-keys it again before it binds; then a restore of a checkpoint of the first history
/// (found at a million cases). The claim that applied is a write under the second token, so the
/// last history does not inherit the generation, though it begins from the token the generation
/// first re-keyed from: the takeover resets `rewound` instead of binding over frames it caught up
/// and the restore lost.
#[test]
fn a_rekey_after_a_claim_applied_inherits_only_from_the_current_token() {
    use Op::Commit;
    use Op::Deliver;
    use Op::ForeignAppend;
    use Op::Restore;
    use Op::Sync;
    let ops = vec![
        ForeignAppend(1),
        Commit(0),
        Commit(0),
        Commit(0),
        Deliver(0),
        Deliver(0),
        Sync(0),
        Deliver(0),
        Deliver(0),
        Op::Checkpoint,
        Sync(0),
        Op::Takeover(true),
        Commit(0),
        Commit(0),
        Commit(0),
        Op::DropRequest(0),
        Restore(1),
        Op::Restart(5, 0),
        ForeignAppend(1),
        Deliver(11),
        Op::NewHistory,
        Op::Flush,
        Commit(0),
        Restore(2),
    ];
    assert_eq!(
        run(Knobs::default(), 6_904_490_963_538_823_122, &ops),
        Vec::<String>::new()
    );
}

/// Owner 0's first append is lost and resent: the resend applies, acknowledging record 1 and
/// proving the tail for a ticket on record 2. The batch of records 2 and 3 applies with its answer
/// lost, idle expiry passes, and its resend meets `producer-expired`: the continuation claim's
/// reconcile acknowledges record 2 in its first read (48 bytes on seed 0), and a `synced()` call
/// lands before the next read. The ticket must wait for the bind, after the `Wake` (#604).
fn settle_while_activating_script() -> Vec<Op> {
    use Op::Advance;
    use Op::Commit;
    use Op::Deliver;
    use Op::DropAnswerAfterApply;
    use Op::DropRequest;
    use Op::ExpireIdle;
    use Op::Flush;
    use Op::Sync;
    use Op::Tick;
    vec![
        Flush,
        Commit(0),
        DropRequest(0),
        Commit(0),
        Sync(0),
        Deliver(0),
        Advance(10_000),
        Tick(0),
        Deliver(0),
        Commit(0),
        Deliver(0),
        DropAnswerAfterApply(0),
        ExpireIdle,
        Deliver(0),
        Advance(10_000),
        Tick(0),
        Deliver(0),
        Deliver(0),
        Deliver(0),
        Deliver(0),
        Deliver(0),
        Deliver(0),
        Sync(0),
    ]
}

#[test]
fn negative_control_settling_tickets_while_activating_resolves_below_the_target() {
    let script = settle_while_activating_script();
    assert_eq!(run(Knobs::default(), 0, &script), Vec::<String>::new());
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            settle_while_activating: true,
            ..ReplicatorKnobs::default()
        }),
        0,
        &script,
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 9")),
        "{violations:?}"
    );
}

/// Owner 0, cut off from the network, restarts: its open's HEAD gets no answer, and it binds at
/// once as a continuation of its log's claim. Commits and `synced()` wait; once the partition
/// heals, the claim of `own + 1` applies and the owner streams.
#[test]
fn a_provisional_restart_goes_live_once_the_partition_heals() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Partition(0, true),
        Op::Restart(0, 0),
        Op::Commit(0),
        Op::Sync(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Activating {
            kind: ClaimKind::Continuation { .. },
            ..
        }
    ));
    assert!(!world.owners[0].inst.calls.is_empty(), "synced() waits");
    for op in &[
        Op::Partition(0, false),
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
    ] {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Streaming { .. }
    ));
    assert!(world.owners[0].inst.calls.is_empty());
    assert_eq!(world.coverage.provisional_live.load(Ordering::Relaxed), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Owner 0 streams idle while a checkpoint is restored, then restarts and a second restore
/// follows: its `synced()` probe reads under the old token and gets the `410` (it comes before the
/// `412`). The probe sends a HEAD and takes the history path, never `fenced` with no other writer
/// (property 13; this reproduced on main on seed 0).
#[test]
fn a_probe_answered_410_after_a_restore_takes_the_history_path() {
    use Op::Commit;
    use Op::Deliver;
    let mut ops = vec![
        Deliver(0),
        Deliver(0),
        Commit(0),
        Deliver(0),
        Deliver(0),
        Op::Retain(0),
        Op::Checkpoint,
        Op::Restore(1),
        Op::Restart(0, 0),
        Deliver(0),
        Op::Restore(2),
        Deliver(0),
        Op::Sync(0),
        Commit(0),
        Op::Flush,
    ];
    ops.extend(std::iter::repeat_n(Commit(0), 5));
    assert_eq!(run(Knobs::default(), 0, &ops), Vec::<String>::new());
}

/// Owner 0, cut off, restarts provisionally and commits once offline; a history that loses
/// nothing replaces the stream meanwhile (a single voter's unclean restart). That commit was never
/// sent, so it does not hold the re-key back (the history gate, design: History, counts the log's
/// commits at the open, none after its ACK): once the partition heals the owner re-keys, its
/// offline commit replicates under the new token, and nothing is salvaged or reset.
#[test]
fn an_offline_commit_survives_a_history_that_keeps_everything() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Partition(0, true),
        Op::Restart(0, 0),
        Op::Commit(0),
        Op::NewHistory,
        Op::Sync(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(world.owners[0].inst.provisional);
    let token = world.token();
    for op in &[
        Op::Partition(0, false),
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
    ] {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Streaming { .. }
    ));
    assert!(world.owners[0].inst.calls.is_empty(), "synced() resolved");
    let log = &world.owners[0].log;
    assert_eq!(log.acks.last().map(|ack| ack.0), Some(log.last_logged()));
    assert_eq!(log.incarnation, token);
    let counted = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let rekeys = counted(&world.coverage.rekeys_open) + counted(&world.coverage.rekeys_claiming);
    assert_eq!(rekeys, 1);
    assert_eq!(counted(&world.coverage.salvages), 0);
    assert_eq!(counted(&world.coverage.rewound), 0);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Owner 0 is cut off and a new owner takes the stream over; owner 0 restarts provisionally, and
/// once the partition heals its claim of `own + 1` loses to the new owner's: `fenced`.
#[test]
fn a_provisional_restart_after_a_takeover_is_fenced() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Partition(0, true),
        Op::Takeover(false),
        Op::Flush,
        Op::Restart(0, 0),
        Op::Commit(0),
        Op::Partition(0, false),
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Reset(ResetKind::Fenced)
    ));
    assert_eq!(world.coverage.provisional_fenced.load(Ordering::Relaxed), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// A power loss drops owner 0's acknowledged commit from its log; it restarts provisionally and
/// commits other bytes at that position. Its claim's reconcile finds the lost commit in the stream
/// with no pending record to match it: `stale`, with no other writer (property 13's exemption).
#[test]
fn a_provisional_restart_after_a_power_loss_of_acknowledged_commits_goes_stale() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::PowerLoss(0, 1),
        Op::ProvisionalRestart(0, 0),
        Op::Commit(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Reset(ResetKind::Stale)
    ));
    assert_eq!(world.coverage.provisional_stale.load(Ordering::Relaxed), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Probe D of #615's review: owner 0 goes offline, a new owner takes the stream over by an
/// explicit open, and owner 0 restarts three times while offline. Once it reaches the stream, its
/// pre-check finds the new owner's claim and it resets `fenced` with nothing sent, and the new
/// owner goes on. Claiming at once, above its log's highest CLAIM record, would fence it.
fn offline_restarts_after_a_takeover() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::Partition(0, true),
        Op::Takeover(false),
        Op::Flush,
        Op::Restart(0, 0),
        Op::Restart(0, 0),
        Op::Restart(0, 0),
        Op::Commit(0),
        Op::Partition(0, false),
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
        Op::Commit(1),
        Op::Flush,
    ]
}

/// The epochs of the CLAIM frames of `activations` in `stream`.
fn claims_of(stream: &[u8], activations: &[Activation]) -> Vec<Epoch> {
    decode_all(stream)
        .into_iter()
        .filter_map(|(_, _, frame)| match frame {
            Some(Frame::Claim { epoch, activation }) if activations.contains(&activation) => {
                Some(epoch)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn offline_restarts_never_fence_an_owner_an_explicit_open_installed() {
    let mut world = World::new(Knobs::default(), 0, Arc::default());
    for op in &offline_restarts_after_a_takeover() {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Reset(ResetKind::Fenced)
    ));
    assert!(matches!(
        world.owners[1].inst.replicator,
        Replicator::Streaming { .. }
    ));
    // Owner 0's only claim in the stream is its first takeover's.
    let ours: Vec<Activation> = world.owners[0].log.claims.iter().map(|(_, a)| *a).collect();
    assert_eq!(claims_of(&world.stream(), &ours).len(), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    let violations = run(
        Knobs {
            claim_without_pre_check: true,
            ..Knobs::default()
        },
        0,
        &offline_restarts_after_a_takeover(),
    );
    assert!(
        violations
            .iter()
            .any(|v| v.starts_with("property 20") || v.starts_with("property 4")),
        "{violations:?}"
    );
}

/// Probe C of #615's review: owner 0's continuation claim after idle expiry is lost and its host
/// loses power, then a new owner takes over. Owner 0 restarts cut off, its log's latest CLAIM
/// record never applied. Once healed, its pre-check finds the new owner's claim.
#[test]
fn an_unapplied_claim_in_the_log_never_fences_an_explicit_owner() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::ExpireIdle,
        Op::Advance(10_000),
        Op::Commit(0),
        Op::Tick(0),
        Op::Deliver(0),
        Op::Deliver(0),
        Op::DropRequest(0),
        Op::DropRequest(0),
        Op::PowerLoss(0, 1),
        Op::Takeover(false),
        Op::Flush,
        Op::Partition(0, true),
        Op::Restart(0, 0),
        Op::Flush,
        Op::Partition(0, false),
        Op::Advance(10_000),
        Op::Tick(0),
        Op::Flush,
        Op::Commit(1),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 0, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Reset(ResetKind::Fenced)
    ));
    assert!(matches!(
        world.owners[1].inst.replicator,
        Replicator::Streaming { .. }
    ));
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Probe B of #615's review: clones, a provisional restart and a foreign claim, with the
/// provisional owner cut off. Once healed it resets, and the owner that bound goes on.
fn a_provisional_clone_beside_the_owner_that_bound() -> Vec<Op> {
    use Op::Clone;
    use Op::Commit;
    use Op::Deliver;
    use Op::DropAnswerAfterApply;
    use Op::DropRequest;
    use Op::Duplicate;
    use Op::Flush;
    use Op::ForeignClaim;
    use Op::Partition;
    use Op::ProvisionalRestart;
    use Op::Restart;
    vec![
        Clone(0),
        Restart(2, 0),
        Op::Fault(FaultKind::Status(503, Some(1)), false, 2),
        Duplicate(0),
        Duplicate(0),
        DropRequest(0),
        Restart(1, 0),
        ProvisionalRestart(2, 0),
        Op::Advance(0),
        DropAnswerAfterApply(8),
        DropAnswerAfterApply(62),
        Partition(0, true),
        Flush,
        Deliver(0),
        Commit(2),
        ForeignClaim(false),
        Deliver(0),
        Commit(0),
        Commit(2),
        Partition(0, false),
        Op::Advance(10_000),
        Op::Tick(0),
        Flush,
        Commit(1),
        Flush,
    ]
}

#[test]
fn a_provisional_clone_never_fences_the_owner_that_bound() {
    let mut world = World::new(Knobs::default(), 0, Arc::default());
    for op in &a_provisional_clone_beside_the_owner_that_bound() {
        world.step(op);
    }
    assert!(matches!(
        world.owners[0].inst.replicator,
        Replicator::Reset(ResetKind::Fenced)
    ));
    assert!(matches!(
        world.owners[1].inst.replicator,
        Replicator::Streaming { .. }
    ));
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    let violations = run(
        Knobs {
            claim_without_pre_check: true,
            ..Knobs::default()
        },
        0,
        &a_provisional_clone_beside_the_owner_that_bound(),
    );
    assert!(
        violations
            .iter()
            .any(|v| v.starts_with("property 20") || v.starts_with("property 4")),
        "{violations:?}"
    );
}

/// Probe F of #615's review (design: Claims): a takeover's claim applied after a foreign one, and
/// the process crashed before the bind, so the log's acknowledged offset lies before both. The
/// provisional reopen's pre-check finds the foreign claim followed by its own: superseded, and it
/// goes live. With a foreign commit there instead, which the generation never caught up, it resets
/// `stale`.
#[test]
fn a_crash_before_the_bind_supersedes_a_foreign_claim_and_goes_stale_on_a_foreign_commit() {
    for (foreign, stale) in [(2, false), (1, true)] {
        let ops = vec![
            Op::ForeignAppend(foreign),
            Op::Deliver(0),
            Op::ProvisionalRestart(0, 0),
            Op::Flush,
        ];
        let mut world = World::new(Knobs::default(), 0, Arc::default());
        for op in &ops {
            world.step(op);
        }
        let replicator = &world.owners[0].inst.replicator;
        if stale {
            assert!(
                matches!(replicator, Replicator::Reset(ResetKind::Stale)),
                "{foreign}"
            );
        } else {
            assert!(
                matches!(replicator, Replicator::Streaming { .. }),
                "{foreign}"
            );
        }
        world.heal();
        assert_eq!(world.violations, Vec::<String>::new());
    }
}

/// Found at a million cases: a continuation claim after idle expiry applies, a restore drops it
/// before its reconcile finished, and an explicit open claims that epoch in the new history. The
/// re-key pre-checks under the new token and finds that owner's claim (property 20).
#[test]
fn a_re_key_while_reconciling_never_fences_an_explicit_owner() {
    use Op::Commit;
    use Op::Deliver;
    use Op::Takeover;
    let ops = vec![
        Commit(0),
        Op::ProvisionalRestart(0, 0),
        Op::DropAnswer(0),
        Commit(0),
        Commit(0),
        Takeover(false),
        Commit(0),
        Op::Flush,
        Commit(1),
        Op::ExpireIdle,
        Op::Checkpoint,
        Commit(0),
        Op::ForeignAppend(0),
        Deliver(0),
        Deliver(0),
        Takeover(false),
        Commit(0),
        Deliver(14),
        Takeover(false),
        Op::DropRequest(0),
        Op::Partition(0, false),
        Op::Misroute(true, false),
        Op::Fault(FaultKind::Status(402, None), false, 1),
        Op::Restore(0),
        Op::Fault(FaultKind::Status(503, Some(1)), false, 1),
        Takeover(false),
        Op::Flush,
        Commit(0),
    ];
    assert_eq!(run(Knobs::default(), 0, &ops), Vec::<String>::new());
}

/// Owner 0 is cloned while it streams idle, idle expiry passes, and owner 0's commit meets
/// `producer-expired`: its continuation claim of the next epoch applies under the activation both
/// copies share. The clone's `synced()` probe then reads that claim past its tail. The claim's
/// epoch is in no CLAIM record of the clone's log, so it is another copy's: the probe fences.
fn a_clone_probes_past_a_continuation_claim() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Clone(0),
        Op::ExpireIdle,
        Op::Commit(0),
        // The append meets `producer-expired`, the continuation claim goes out and applies.
        Op::Deliver(0),
        Op::Deliver(0),
        Op::Deliver(0),
        Op::Sync(1),
        // The clone's probe reaches the server, and its answer the clone.
        Op::Deliver(1),
        Op::Deliver(1),
    ]
}

#[test]
fn negative_control_a_probe_skipping_claims_of_its_activation_syncs_past_a_copys_claim() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &a_clone_probes_past_a_continuation_claim() {
        world.step(op);
    }
    assert!(
        matches!(
            world.owners[1].inst.replicator,
            Replicator::Reset(ResetKind::Fenced)
        ),
        "{:?}",
        world.owners[1].inst.replicator
    );
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            probe_skips_own_claims: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &a_clone_probes_past_a_continuation_claim(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 9")),
        "{violations:?}"
    );
}

/// Owner 0 acknowledges a commit and another host opens a copy of its cache offline; owner 0
/// commits the next position and the copy commits its own bytes there. The copy's claim lands
/// after owner 0's commit, which its reconcile meets at the commit id of its pending record with
/// other bytes: another copy's state, never its own record.
fn an_offline_copy_behind_the_original() -> Vec<Op> {
    vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::CopyCache(0, false, true),
        Op::Commit(0),
        Op::Commit(1),
        Op::Flush,
    ]
}

#[test]
fn negative_control_matching_commits_by_id_acknowledges_a_copys_commit() {
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &an_offline_copy_behind_the_original() {
        world.step(op);
    }
    assert!(
        matches!(world.owners[1].inst.replicator, Replicator::Reset(_)),
        "{:?}",
        world.owners[1].inst.replicator
    );
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
    let violations = run(
        replicator_knobs(ReplicatorKnobs {
            match_commits_by_id: true,
            ..ReplicatorKnobs::default()
        }),
        1,
        &an_offline_copy_behind_the_original(),
    );
    assert!(
        violations.iter().any(|v| v.starts_with("property 2")
            || v.starts_with("property 8")
            || v.starts_with("property 19")),
        "{violations:?}"
    );
}

/// Owner 0 is cloned while it streams idle, and both copies reopen under the activation they
/// share: their takeovers compute one epoch and send byte-identical claims, the second answered by
/// a repeat, and both bind. At the first commit where they differ, the second copy's append meets
/// `key-conflict`: exactly one copy is fenced, and nothing either acknowledged is lost.
#[test]
fn a_shared_epoch_takeover_fences_exactly_one_copy() {
    let ops = vec![
        Op::Flush,
        Op::Clone(0),
        Op::Reopen(0),
        Op::Reopen(1),
        Op::Flush,
        Op::Commit(0),
        Op::Commit(1),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    let fenced = |i: usize| {
        matches!(
            world.owners[i].inst.replicator,
            Replicator::Reset(ResetKind::Fenced)
        )
    };
    let streaming = |i: usize| {
        matches!(
            world.owners[i].inst.replicator,
            Replicator::Streaming { .. }
        )
    };
    assert!(
        (fenced(0) && streaming(1)) || (fenced(1) && streaming(0)),
        "{:?} / {:?}",
        world.owners[0].inst.replicator,
        world.owners[1].inst.replicator
    );
    let counted = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    assert_eq!(counted(&world.coverage.repeats_won_takeover), 1);
    assert_eq!(counted(&world.coverage.clones_fenced_key_conflict), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Another host opens a copy of owner 0's cache and takes over, while owner 0 commits: owner 0's
/// append meets the copy's claim and it resets `fenced`, its commit kept in its log, and the copy
/// streams.
#[test]
fn a_copy_taken_over_while_the_original_writes_fences_the_original() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::CopyCache(0, false, false),
        Op::Commit(0),
        Op::Flush,
        Op::Commit(1),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(
        matches!(
            world.owners[0].inst.replicator,
            Replicator::Reset(ResetKind::Fenced)
        ),
        "{:?}",
        world.owners[0].inst.replicator
    );
    assert!(
        matches!(
            world.owners[1].inst.replicator,
            Replicator::Streaming { .. }
        ),
        "{:?}",
        world.owners[1].inst.replicator
    );
    assert_eq!(world.owners[0].log.commits.len(), 2);
    assert_eq!(world.coverage.copies_took_over.load(Ordering::Relaxed), 1);
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Another host opens a copy of idle owner 0's cache offline (the residual case of the design for
/// a copy of a live cache): the pre-check finds nothing past the acknowledged offset, the log's
/// claim sent again meets its repeat, and the copy's claim of the next epoch binds. Owner 0's next
/// append meets it: `fenced`.
#[test]
fn an_offline_copy_of_an_idle_cache_fences_the_original() {
    let ops = vec![
        Op::Flush,
        Op::CopyCache(0, false, true),
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(
        matches!(
            world.owners[0].inst.replicator,
            Replicator::Reset(ResetKind::Fenced)
        ),
        "{:?}",
        world.owners[0].inst.replicator
    );
    assert!(
        matches!(
            world.owners[1].inst.replicator,
            Replicator::Streaming { .. }
        ),
        "{:?}",
        world.owners[1].inst.replicator
    );
    assert_eq!(
        world.coverage.offline_copies_live.load(Ordering::Relaxed),
        1
    );
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Another host opens a copy of owner 0's cache offline; its pre-check finds nothing past the
/// acknowledged offset and sends the log's claim again, and owner 0's commit applies before that
/// claim lands. The repeat brings the copy's claim of the next epoch, applied after owner 0's
/// commit: its reconcile meets that commit, `stale`, and owner 0's next append meets the copy's
/// claim, `fenced`. Both reset, and owner 0's acknowledged commits stay in the stream.
#[test]
fn an_offline_copy_and_the_original_both_reset() {
    let ops = vec![
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
        Op::CopyCache(0, false, true),
        // The copy's HEAD, then its claim sent again.
        Op::Deliver(0),
        Op::Deliver(0),
        Op::Commit(0),
        // Owner 0's append applies first.
        Op::Deliver(1),
        Op::Flush,
        Op::Commit(0),
        Op::Flush,
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    assert!(
        matches!(
            world.owners[0].inst.replicator,
            Replicator::Reset(ResetKind::Fenced)
        ),
        "{:?}",
        world.owners[0].inst.replicator
    );
    assert!(
        matches!(
            world.owners[1].inst.replicator,
            Replicator::Reset(ResetKind::Stale)
        ),
        "{:?}",
        world.owners[1].inst.replicator
    );
    assert_eq!(
        world.coverage.offline_copies_stale.load(Ordering::Relaxed),
        1
    );
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
}

/// Found by the random run once reopens kept the activation: owner 0's commit is pending when a
/// history that loses nothing comes, and it reopens under the same activation. The open table
/// salvages the log (it holds a commit after its latest ACK) and a fresh build takes over. That
/// build read owner 0's first claim, so its takeover claims above it: a claim of the same epoch
/// would be byte-identical to that first claim, and its repeat, whose receipt lies below the build's
/// offset, would reset `stream_state` with no other writer (property 21).
#[test]
fn a_fresh_build_under_the_same_activation_claims_above_the_claims_it_read() {
    use Op::Commit;
    use Op::Tick;
    let ops = vec![
        Op::Flush,
        Op::NewHistory,
        Commit(0),
        Op::Reopen(0),
        Commit(0),
        Tick(0),
        Commit(0),
        Commit(0),
        Tick(0),
        Op::DropAnswerAfterApply(62),
        Commit(0),
        Op::Retain(0),
        Commit(0),
        Op::Flush,
        Commit(0),
        Commit(0),
        Commit(0),
        Commit(0),
        Commit(0),
        Commit(0),
    ];
    for shared_activation in [false, true] {
        let knobs = Knobs {
            shared_activation,
            ..Knobs::default()
        };
        assert_eq!(
            run(knobs, 11_059_917_130_020_041_608, &ops),
            Vec::<String>::new(),
            "shared activation {shared_activation}"
        );
    }
}

/// Found at a million cases: owner 0's takeover claim applies with its answer lost, it reopens
/// under the same activation, then provisionally under another, and is cloned; a restore empties
/// the stream and the clone reopens under the activation it shares with owner 0. The clone's log
/// lost commits to the restore, so it salvages and takes over afresh, claiming epoch 1 under that
/// activation. Owner 0's background claim then reads that claim as its own (design: Claims) and
/// applies above it: the residual case of a shared activation, outside property 20.
#[test]
fn a_clone_reopened_under_the_shared_activation_is_one_owner_to_the_other() {
    use Op::Commit;
    use Op::Reopen;
    let ops = vec![
        Op::DropAnswerAfterApply(0),
        Op::Deliver(0),
        Reopen(0),
        Op::ProvisionalRestart(0, 0),
        Op::Takeover(false),
        Op::Deliver(46),
        Commit(2),
        Op::Clone(2),
        Op::Restore(0),
        Reopen(2),
        Op::Flush,
        Commit(0),
        Commit(0),
    ];
    assert_eq!(
        run(Knobs::default(), 194_716_899_250_413_353, &ops),
        Vec::<String>::new()
    );
}
