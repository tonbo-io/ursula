//! The proptest world: the model server, owners (a replicator and a log model each), and a
//! network that delays, reorders, loses and duplicates requests and answers. Scripts mix commits,
//! deliveries, losses (before apply, after apply, of the answer), late duplicates, scripted
//! `5xx`/`429`/`408`/`307`/`401`/`402`/`403` and untyped `413` answers, a lowered request limit
//! (typed `413`), the clock moved past idle expiry, rewinds with and without inheritance, foreign
//! appends and claims, retention (and with it `410`s), clones, restarts, takeovers by new owners
//! and `synced()` calls, then a
//! `Heal` phase with a perfect network.
//!
//! The monitors check, as the run goes:
//!
//! 1. acknowledged never regresses, per replicator;
//! 2. acknowledged records form a log prefix and appear in the stream in log order (L5);
//! 3. one lineage never sends two bodies under one key (per owner, clones aside);
//! 4. a continuation claim is exactly own + 1;
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
//!     after it, and a receipt without an offset publishes no snapshot.
//!
//! Snapshots carry real `USS3` headers (the owner's lineage, an acknowledged position and its
//! offset): the `410` rule reads them.
//!
//! Two negative controls must fail them: the model with digest comparison off (requests reach it
//! without `Producer-Digest`), and a replicator that re-seals its in-flight batch on resend.

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
use crate::replicator::Action;
use crate::replicator::BoundaryReceipt;
use crate::replicator::ClaimKind;
use crate::replicator::Condition;
use crate::replicator::Event;
use crate::replicator::LocalState;
use crate::replicator::Pending;
use crate::replicator::Replicator;
use crate::replicator::RequestId;
use crate::replicator::ResetKind;
use crate::replicator::Step;
use crate::replicator::Work;
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
    Rewind(bool),
    Checkpoint,
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
        1 => proptest::bool::ANY.prop_map(Op::Rewind),
        1 => Just(Op::Checkpoint),
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
    /// Every replicator re-seals its in-flight batch on resend.
    renumber: bool,
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
}

/// What the scripts of one [`check`] run reached, beyond the properties.
#[derive(Debug, Default)]
struct Coverage {
    /// Takeovers that caught up in place over two reads or more.
    chunked_catch_ups: AtomicU64,
    /// Takeovers that rebuilt after applying a read in place (grown past the database, or a
    /// `410`).
    rebuilds_after_apply: AtomicU64,
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
    },
}

struct World {
    knobs: Knobs,
    server: ModelServer,
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
        let mut world = Self {
            knobs,
            server,
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
        let (mut replicator, actions) =
            Replicator::activate(ClaimKind::Takeover { round: 1 }, local);
        if self.knobs.renumber {
            replicator.set_renumber(true);
        }
        let inst = Instance {
            uid: self.uid(),
            replicator,
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
        };
        (inst, actions)
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
        };
        let (inst, actions) = self.instance(local);
        self.owners.push(Owner {
            family,
            salt: u8::try_from(family % 250).unwrap(),
            log,
            inst,
        });
        let index = self.owners.len() - 1;
        self.process(index, actions, None);
    }

    /// The owner's process restarts and reopens from its log; the ACK it reads lags by `lag`.
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
        let pending: Vec<Pending> = log
            .commits
            .range(acked.0 + 1..)
            .map(|(_, p)| p.clone())
            .collect();
        let local = LocalState {
            lineage: log.lineage,
            activation,
            cluster_id: cluster(),
            incarnation: log.incarnation,
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
        };
        let (inst, actions) = self.instance(local);
        self.owners[index].inst = inst;
        self.process(index, actions, None);
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
                lost.push(Msg::Answer {
                    to: clone_uid,
                    id,
                    exchange: Exchange::NoAnswer(NoAnswer::Lost {
                        reason: "restored copy".to_owned(),
                    }),
                });
            }
        }
        self.msgs.extend(lost);
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
        let before = match &self.owners[index].inst.replicator {
            Replicator::Streaming { epoch, .. } => Some(*epoch),
            _ => None,
        };
        let takeover = matches!(self.owners[index].inst.replicator, Replicator::Activating {
            kind: ClaimKind::Takeover { .. },
            ..
        });
        let now = self.now();
        let actions = self.owners[index].inst.replicator.on(event, now);
        self.process_in(index, actions, before, takeover);
    }

    fn violation(&mut self, what: String) {
        self.violations.push(what);
    }

    /// Carries out an owner's actions and checks them.
    fn process(&mut self, index: usize, actions: Vec<Action>, streaming_epoch: Option<Epoch>) {
        self.process_in(index, actions, streaming_epoch, true);
    }

    fn process_in(
        &mut self,
        index: usize,
        actions: Vec<Action>,
        streaming_epoch: Option<Epoch>,
        takeover: bool,
    ) {
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
                Action::Send { id, request, .. } => self.sent(index, id, request, streaming_epoch),
                Action::PersistClaim { epoch, activation } => {
                    self.owners[index].log.claims.push((epoch, activation));
                }
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
                Action::Condition(_) => {}
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
                Action::Rebuild { discard_from, .. } => {
                    if !takeover {
                        self.violation("property 6: a rebuild outside a takeover".to_owned());
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
                Action::Bound { .. } | Action::Reset(_) | Action::Refuse(_) => {}
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

    fn sent(&mut self, index: usize, id: RequestId, request: Request, streaming: Option<Epoch>) {
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
            if write.seq == Seq::ZERO
                && let Some(own) = streaming
                && write.epoch.get() != own.get() + 1
            {
                self.violation(format!(
                    "property 4: a continuation from {own} claimed {}",
                    write.epoch
                ));
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
        // Searched as bytes: foreign garbage may sit between frames. An answer that comes back
        // after a rewind is about the history it applied in, which the stream no longer shows.
        let stream = self.stream();
        let current = self.owners[index].log.incarnation == self.token();
        let mut from = 0;
        for record in &records {
            match self.acked_frames.get(&record.id) {
                Some(frame)
                    if *frame != record.frame
                        && !self.owners[index].log.applied.contains(frame) =>
                {
                    self.violation(format!(
                        "property 8: two commits acknowledged at {:?}",
                        record.id
                    ));
                }
                Some(_) => {}
                None => {
                    self.acked_frames
                        .insert(record.id, Arc::clone(&record.frame));
                }
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
        let delivered = self.server.exchange(PATH, &request);
        let exchange = match delivered {
            Delivered::Answered { answer, .. } => {
                let parsed = answer.parse(&request);
                self.applied(&parsed.typed, &request);
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
        self.msgs.push(Msg::Answer { to, id, exchange });
    }

    /// Property 5, on every applied producer write.
    fn applied(&mut self, typed: &Result<Typed, ursula_headers::HeaderError>, request: &Request) {
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

    fn deliver_answer(&mut self, to: u64, id: RequestId, exchange: Exchange) {
        if let Some(index) = self.owners.iter().position(|o| o.inst.uid == to) {
            self.event(index, Event::Answer { id, exchange });
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
            Msg::Answer { to, id, exchange } => self.deliver_answer(to, id, exchange),
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
            Op::Rewind(inherit) => {
                let event = if inherit {
                    HistoryEvent::NewHistory
                } else {
                    let (checkpoint, max) = *self.checkpoints.last().expect("one at creation");
                    self.max_claimed = max;
                    HistoryEvent::Restore(checkpoint)
                };
                self.server.history_event(event).unwrap();
            }
            Op::Retain(o) => {
                let i = self.owner(o);
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
                    self.applied(&parsed.typed, &request);
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

    /// A perfect network until nothing is left to do, then property 7.
    fn heal(&mut self) {
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

/// The scripts reach a catch-up in place read by read, and one that rebuilds midway: checked on
/// a fixed seed, so the coverage is reproducible.
#[test]
fn the_world_catches_up_in_place_read_by_read() {
    let coverage = match check(Knobs::default(), COVERAGE_CASES, true) {
        Ok(coverage) => coverage,
        Err(failure) => panic!("{failure}"),
    };
    assert!(coverage.chunked_catch_ups.load(Ordering::Relaxed) > 0);
    assert!(coverage.rebuilds_after_apply.load(Ordering::Relaxed) > 0);
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
        Knobs {
            renumber: true,
            ..Knobs::default()
        },
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
        Knobs {
            renumber: true,
            ..Knobs::default()
        },
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
