//! The proptest world: the model server, owners (a replicator and a log model each), and a
//! network that delays, reorders, loses and duplicates requests and answers. Scripts mix commits,
//! deliveries, losses (before apply, after apply, of the answer), late duplicates, scripted
//! `5xx`/`429`/`408`/`307`/`401`/`402`/`403` and untyped `413` answers, a lowered request limit
//! (typed `413`), the clock moved past idle expiry, history events (a new history that loses
//! nothing, and restores of checkpoints), requests misrouted to another deployment, foreign
//! appends and claims, retention (and with it `410`s), clones, restarts (through the open table of
//! design §5.5: a restart whose history does not inherit the log, or may have lost commits it
//! holds after its latest ACK, salvages it and continues as a fresh takeover), takeovers by new
//! owners and `synced()` calls, then a `Heal` phase with a perfect network and the route restored.
//!
//! The monitors check, as the run goes:
//!
//! 1. acknowledged never regresses, per replicator;
//! 2. acknowledged records form a log prefix and appear in the stream in log order (L5);
//! 3. one lineage never sends two bodies under one key (per owner, clones aside);
//! 4. a new continuation claim is exactly one above the latest claim of its instance the server
//!    applied;
//! 5. once a claim applies, no older key of the producer applies (within one history);
//! 6. a typed fence keeps pending records (only a takeover's divergence discards);
//! 7. after `Heal`, every run ends acknowledged, retrying, blocked or reset;
//! 8. of two divergent clone commits, at most one is acknowledged (commits at one commit id with
//!    other bytes diverge unless the later one was made after catching the earlier one up);
//! 9. `Synced` never fires while a pre-call foreign frame sits past the acknowledged offset, nor
//!    for a target later discarded;
//! 10. the bytes between an owner's consecutive acknowledged offsets are its own frames, frames
//!     it caught up, or (in a takeover's reconcile) foreign claims, unless it rebuilt;
//! 11. after `Heal`, every ticket of an owner that ends acknowledged resolved or was abandoned;
//! 12. no append carries a frame the owner caught up;
//! 13. an owner resets `fenced` or `stale` only when another writer, owner or clone was on the
//!     stream;
//! 14. a boundary's receipt names the end of its record's frame in the stream with only claims
//!     after it, and a receipt without an offset publishes no snapshot;
//! 15. a re-key happens only when the history inherits the generation: every record the owner
//!     acknowledged is in the stream the HEAD that decided it saw, in log order;
//! 16. nothing applies on the other deployment;
//! 17. a misrouted answer never resets an owner, and after `Heal` no owner ends blocked
//!     `wrong_cluster`;
//! 18. a rebuild that discards pending records of a log re-keyed at open (and not bound since,
//!     across restarts) carries `salvage: true`;
//! 19. after `Heal`, every commit an owner was told is acknowledged is in the stream, in a log,
//!     or in a salvage: none is lost, whatever history changes came (clones apart, whose copies
//!     commit at one commit id).
//!
//! Snapshots carry real `USS3` headers (the owner's lineage, an acknowledged position and its
//! offset): the `410` rule reads them.
//!
//! Negative controls must fail them: the model with digest comparison off (requests reach it
//! without `Producer-Digest`), a replicator that re-seals its in-flight batch on resend, one that
//! re-keys on any other token, one that binds on a repeat receipt for a claim sent again after a
//! re-key, one that takes another cluster's HEAD for a deleted stream, and one that re-keys a log
//! holding commits after its latest ACK (the #611 B2 gate undone).

use std::collections::BTreeMap;
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
use crate::open::table::decide;
use crate::replicator::Action;
use crate::replicator::BlockedReason;
use crate::replicator::BoundaryReceipt;
use crate::replicator::ClaimKind;
use crate::replicator::Condition;
use crate::replicator::Event;
use crate::replicator::Knobs as ReplicatorKnobs;
use crate::replicator::LocalState;
use crate::replicator::Pending;
use crate::replicator::Replicator;
use crate::replicator::RequestId;
use crate::replicator::ResetKind;
use crate::replicator::Step;
use crate::replicator::Work;
use crate::replicator::answer::Unacked;
use crate::replicator::answer::inherits;
use crate::salvage::Reason;
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
    Restart(u8, u8),
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
        2 => ((0u8..8), (0u8..3)).prop_map(|(o, lag)| Op::Restart(o, lag)),
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
    /// The log rolled to a new token at open, and no claim bound under it since (property 18).
    rekeyed_at_open: bool,
}

impl LogModel {
    fn last_logged(&self) -> Position {
        Position(self.next_position.saturating_sub(1))
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
    /// rename does (design §5.1, §5.6).
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
}

#[derive(Debug, Clone)]
enum Msg {
    Request {
        to: u64,
        id: RequestId,
        request: Request,
        late: bool,
    },
    Answer {
        to: u64,
        id: RequestId,
        exchange: Exchange,
        misrouted: bool,
    },
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
    /// Every commit frame an owner was told its stream acknowledged (property 19).
    acknowledged: Vec<Arc<[u8]>>,
    /// The lineages of logs cloned: two copies commit at one commit id, and a takeover of one
    /// may discard what the other had acknowledged (clones are outside property 19).
    cloned: Vec<Lineage>,
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
            acknowledged: Vec::new(),
            cloned: Vec::new(),
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

    fn activation(&mut self) -> Activation {
        self.next_activation = self.next_activation.wrapping_add(1);
        Activation([self.next_activation; 16])
    }

    fn uid(&mut self) -> u64 {
        self.next_uid += 1;
        self.next_uid
    }

    fn instance(&mut self, local: LocalState) -> (Instance, Vec<Action>) {
        let start_acked = local.acked;
        let acked_offset = local.acked_offset;
        let kind = ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        };
        let (mut replicator, actions) = Replicator::activate(kind, local);
        replicator.set_knobs(self.knobs.replicator);
        let inst = self.stopped(start_acked, acked_offset);
        (Instance { replicator, ..inst }, actions)
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
        let (owner, actions) = self.fresh_owner(lagging);
        self.owners.push(owner);
        let index = self.owners.len() - 1;
        let before = Before {
            takeover: true,
            ..Before::default()
        };
        self.process(index, actions, before);
    }

    /// An owner of a new family whose generation was built from the stream up to its tail (from
    /// offset 0 when `lagging`), and the actions of its takeover.
    fn fresh_owner(&mut self, lagging: bool) -> (Owner, Vec<Action>) {
        let activation = self.activation();
        let family = self.next_family;
        self.next_family += 1;
        let lineage = Lineage([u8::try_from(family % 250).unwrap() + 1; 16]);
        let tail = if lagging {
            Offset::ZERO
        } else {
            Offset::from_u64(u64::try_from(self.stream().len()).unwrap())
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
            rekeyed_at_open: false,
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
            highest_epoch: None,
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
        };
        (owner, actions)
    }

    /// The owner's process restarts and reopens from its log; the ACK it reads lags by `lag`. Its
    /// open's HEAD goes through the open table: the same token resumes, an inheriting history
    /// re-keys a log with nothing pending, any other history salvages the log and the owner
    /// continues as a fresh takeover, and a refusal runs no replicator.
    fn restart(&mut self, index: usize, lag: u8) {
        let activation = self.activation();
        let owner = &self.owners[index];
        let family = owner.family;
        let log = owner.log.clone();
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
        // The ACK records the crash lost are gone for good: the next restart never reads them.
        self.owners[index].log.acks.truncate(lagged + 1);
        let pending: Vec<Pending> = log
            .commits
            .range(acked.0 + 1..)
            .map(|(_, p)| p.clone())
            .collect();
        let head = Request::new(Method::Head, Target::Stream);
        let answer = self.routed().handle(PATH, &head).parse(&head);
        let (identity, stream) = match &answer.typed {
            Ok(Typed::Head { identity, stream }) => (identity.clone(), *stream),
            other => panic!("a HEAD answers a HEAD, not {other:?}"),
        };
        let cluster = cluster();
        // The world's log holds COMMIT records (own, and caught up) only up to its ACKs or in
        // the pending tail.
        let unacked = if pending.is_empty() {
            Unacked::Nothing
        } else {
            Unacked::Commits
        };
        let summary = LocalSummary {
            cluster_id: cluster.as_str().to_owned(),
            incarnation: log.incarnation,
            acked_offset,
            pending: unacked,
            holds_data: !log.commits.is_empty() || log.rebuilt_through > 0,
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
                        reason: Reason::HistoryChanged,
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
            OpenRow::Resume { .. } => (log.incarnation, None),
            OpenRow::ReKey { from, head, .. } => (head.incarnation(), Some(from)),
            OpenRow::Refuse { .. } => {
                let inst = self.stopped(acked, acked_offset);
                self.owners[index].inst = inst;
                return;
            }
            OpenRow::Build { superseded, .. } => {
                if matches!(superseded, Superseded::Salvage { .. }) {
                    self.coverage.salvages.fetch_add(1, Ordering::Relaxed);
                    // The salvage holds what the log holds, and what its db holds.
                    self.salvaged
                        .extend(log.commits.values().map(|p| Arc::clone(&p.frame)));
                    self.salvaged.extend(log.applied.iter().cloned());
                    self.salvaged.push(Arc::clone(&log.rebuilt));
                }
                let (owner, actions) = self.fresh_owner(false);
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
        let local = LocalState {
            lineage: log.lineage,
            activation,
            cluster_id: cluster,
            incarnation,
            acked,
            acked_offset,
            log_bytes,
            logged: Position(log.commits.keys().last().copied().unwrap_or(0).max(acked.0)),
            pending,
            claims: log.claims.clone(),
            highest_epoch: None,
            max_request_bytes: self.limits(),
            configured_max_request_bytes: None,
            db_bytes: db_bytes(family),
            rekey_from,
            unacked,
        };
        let (inst, actions) = self.instance(local);
        self.owners[index].inst = inst;
        let before = Before {
            takeover: true,
            head_saw: Some(self.routed().frames(PATH).unwrap_or_default().to_vec()),
            ..Before::default()
        };
        self.process(index, actions, before);
    }

    fn clone_owner(&mut self, index: usize) {
        if self.owners.len() >= MAX_OWNERS {
            return;
        }
        let mut copy = self.owners[index].clone();
        let original = copy.inst.uid;
        self.cloned.push(copy.log.lineage);
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

    fn live(&self, index: usize) -> bool {
        self.owners[index].inst.replicator.ctx().is_some()
    }

    /// Whether a connection can commit or call `synced()`. A takeover runs inside `open()`,
    /// before any connection opens the generation (design §5.5), so the driver logs no commit
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
                Action::PersistClaim { epoch, activation } => {
                    self.owners[index].log.claims.push((epoch, activation));
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
                        log.durable_acks = log.acks.len();
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
                        log.durable_acks = log.acks.len();
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
                    if discard_from.is_some() && self.owners[index].log.rekeyed_at_open && !salvage
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
                Action::Reset(kind @ (ResetKind::Fenced | ResetKind::Stale))
                    if !self.others
                        && self.owners.len() == 1
                        && !self.owners[index].inst.claim_expired =>
                {
                    self.violation(format!(
                        "property 13: a {kind:?} reset with no other writer on the stream"
                    ));
                }
                // The retiring binding fsyncs its ACK (design §6.2, D5).
                Action::Reset(ResetKind::Rewound) => {
                    self.coverage.rewound.fetch_add(1, Ordering::Relaxed);
                    let log = &mut self.owners[index].log;
                    log.durable_acks = log.acks.len();
                }
                Action::Bound { .. } => self.owners[index].log.rekeyed_at_open = false,
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
        // A takeover's, at open or during its claim: its ACK may trail the stream's (property 18).
        let at_open = before.takeover;
        let counter = if before.streaming {
            &self.coverage.rekeys_streaming
        } else if before.claiming {
            &self.coverage.rekeys_claiming
        } else {
            &self.coverage.rekeys_open
        };
        counter.fetch_add(1, Ordering::Relaxed);
        match &before.head_saw {
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
        let log = &mut self.owners[index].log;
        log.incarnation = incarnation;
        if !log.claims.contains(&claim) {
            log.claims.push(claim);
        }
        log.durable_acks = log.acks.len();
        log.rekeyed_at_open |= at_open;
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
            let inst = &mut self.owners[index].inst;
            if write.seq == Seq::ZERO && !inst.claims_sent.contains(&write.epoch) {
                inst.claims_sent.push(write.epoch);
                // A new continuation claim, not one sent again: one above the latest claim of
                // this instance the server applied.
                let own = inst.claim_applied;
                if continuation && own.map(|own| own.get() + 1) != Some(write.epoch.get()) {
                    self.violation(format!(
                        "property 4: a continuation claimed {} after applied claim {own:?}",
                        write.epoch
                    ));
                }
            }
        }
        let to = self.owners[index].inst.uid;
        self.msgs.push(Msg::Request {
            to,
            id,
            request,
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
        // Property 19: the owner's own commits it was told are acknowledged, unless a clone
        // shares their lineage.
        let lineage = self.owners[index].log.lineage;
        if !self.cloned.contains(&lineage) {
            self.acknowledged.extend(
                records
                    .iter()
                    .filter(|p| p.id.lineage == lineage)
                    .map(|p| Arc::clone(&p.frame)),
            );
        }
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
                Some(frame)
                    if *frame != record.frame
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

    /// Publishes a snapshot at `offset` whose `USS3` header names `last`, as a snapshot thread
    /// would (the image is a stand-in: only the header is ever read here).
    fn publish_snapshot(&mut self, offset: Offset, last: CommitId) {
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
        drop(self.server.handle(PATH, &publish));
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
        self.publish_snapshot(offset, CommitId {
            lineage: owner.log.lineage,
            position: receipt.position,
        });
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
        for (start, len, frame) in decode_all(range) {
            let bytes = range.get(start..start + len).unwrap_or_default();
            let fine = match frame {
                Some(Frame::Claim { activation, .. }) => {
                    takeover || owner.log.claims.iter().any(|(_, a)| *a == activation)
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
                    Some(Frame::Claim { activation, .. }) => {
                        owner.log.claims.iter().any(|(_, a)| *a == activation)
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
        let misrouted = self.misrouted;
        let delivered = self.routed().exchange(PATH, &request);
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
                    self.applied(to, &parsed.typed, &request);
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
        // applied before its answer was lost.
        let (Ok(Typed::Producer(ProducerAnswer::Receipt { .. })), Some(write)) =
            (typed, &request.keys.producer)
        else {
            return;
        };
        if write.seq != Seq::ZERO {
            return;
        }
        if let Some(owner) = self.owners.iter_mut().find(|o| o.inst.uid == uid) {
            let applied = &mut owner.inst.claim_applied;
            *applied = Some(applied.map_or(write.epoch, |a| a.max(write.epoch)));
        }
    }

    fn deliver_answer(&mut self, to: u64, id: RequestId, exchange: Exchange, misrouted: bool) {
        let head_saw = self.heads.remove(&(to, id));
        if let Some(index) = self.owners.iter().position(|o| o.inst.uid == to) {
            let before = Before {
                head_saw,
                misrouted,
                ..Before::default()
            };
            self.event_from(index, Event::Answer { id, exchange }, before);
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
            } => self.deliver_request(to, id, request, late, true),
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
            // `rewound` with the old generation whole (its ACK fsynced, D5).
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
                let log = &mut self.owners[index].log;
                log.durable_acks = log.acks.len();
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
        log.rekeyed_at_open = false;
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
                    self.deliver_request(to, id, request, false, false);
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
                self.publish_snapshot(offset, last);
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
                    self.applied(0, &parsed.typed, &request);
                }
            }
            Op::Clone(o) => {
                let i = self.owner(o);
                self.clone_owner(i);
            }
            Op::Restart(o, lag) => {
                let i = self.owner(o);
                self.restart(i, lag);
            }
            Op::Takeover(lagging) => self.takeover(lagging),
            Op::Sync(o) => {
                let i = self.owner(o);
                self.sync(i);
            }
        }
    }

    /// A perfect network, the route restored, until nothing is left to do, then properties 7, 11
    /// and 17.
    fn heal(&mut self) {
        self.misrouted = false;
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
        let cloned = |frame: &[u8]| match frame::decode(frame, u64::MAX) {
            Ok(Decoded::Frame {
                frame: Frame::Commit { id, .. },
                ..
            }) => self.cloned.contains(&id.lineage),
            _ => false,
        };
        let lost = self.acknowledged.iter().find(|frame| {
            if cloned(frame) {
                return false;
            }
            let within = |bytes: &[u8]| bytes.windows(frame.len()).any(|w| w == &frame[..]);
            let held = self.owners.iter().any(|o| {
                o.log.commits.values().any(|p| p.frame == **frame)
                    || o.log.applied.contains(frame)
                    || within(&o.log.rebuilt)
            });
            !within(&stream) && !held && !self.salvaged.iter().any(|s| within(s))
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
                Work::Oversized { .. } => "an oversized frame",
            }
        )),
        Replicator::Activating { step, .. } => Err(format!("activating, {}", match step {
            Step::Claim { .. } => "claiming",
            Step::Reconcile { .. } => "reconciling",
            Step::Applying { .. } => "applying a read",
            Step::CatchingUp(_) => "catching up",
            Step::Gone { .. } => "reading the latest snapshot after a 410",
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
/// streaming, while a continuation claim is out and at open, `rewound` resets, and owners blocked
/// `wrong_cluster` that resumed: checked on a fixed seed, so the coverage is reproducible.
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
    ];
    for (what, count) in counts {
        let count = count.load(Ordering::Relaxed);
        assert!(count > 0, "no {what}");
    }
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
/// history event, so the producer record stays history-fenced (#581 §2.3.1 rows 9 and 11).
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
/// With the #611 B2 gate the first restart salvages the log; undone, it re-keys the log with the
/// acknowledged commit pending, the second restart resumes the re-keyed log, and its takeover
/// discards the commit with no salvage.
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
    ] {
        // A fixed seed: the fixed scripts above always trip, and this search is a bonus that
        // must not flake.
        assert!(check(knobs, 512, true).is_err(), "{knobs:?} passed");
    }
}

/// A clone commits at the next position with its own bytes, its snapshot of our shared lineage
/// moves retention past the original's acknowledged offset, and the original's commit at the same
/// position meets the `410`. The rule must not take the clone's snapshot as covering the
/// original's own record above its acknowledged position: that would acknowledge a commit the
/// stream never held (property 8) and append after another copy's state.
#[test]
fn a_clones_snapshot_never_covers_the_originals_commit() {
    let ops = vec![
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
    ];
    let mut world = World::new(Knobs::default(), 1, Arc::default());
    for op in &ops {
        world.step(op);
    }
    world.heal();
    assert_eq!(world.violations, Vec::<String>::new());
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
/// generation stays whole until the publishing rename (design §5.1, §5.6), so the restart finds
/// the record still pending, and a `synced()` call after it waits for that record.
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
/// bind, design §5.5), so the scripts now pass without reaching those steps: the test above and
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
/// owner's snapshot publish carries its old token and gets a `412`, so it never hides foreign frames
/// behind a snapshot of the current history.
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
