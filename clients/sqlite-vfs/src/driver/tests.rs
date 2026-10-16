#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests count requests and move a virtual clock by small amounts"
)]
//! Tests of the driver: [`Core`]s against the model server in a virtual-time world with real log
//! and generation files in a temp dir, and the runtime over the model in process.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;

use super::Attach;
use super::BindingId;
use super::Bound;
use super::DiskIo;
use super::DriverEvent;
use super::EventLog;
use super::Holder;
use super::Io;
use super::Kept;
use super::Life;
use super::OpenId;
use super::Role;
use super::SyncError;
use super::Ticket;
use super::UNCONNECTED_MS;
use super::Undone;
use super::core::Core;
use super::disk::DiskDone;
use super::disk::DiskJob;
use super::runtime::Driver;
use super::runtime::Transports;
use crate::codec::frame;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::frame::CommitRecord;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::marker::SQLITE_MAGIC;
use crate::codec::marker::mark;
use crate::codec::segment::RecordBody;
use crate::codec::segment::scan;
use crate::generation::Root;
use crate::generation::canonical_path;
use crate::generation::root_name;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::model::Delivered;
use crate::model::HistoryEvent;
use crate::model::ModelServer;
use crate::model::faults::Fault;
use crate::model::faults::Scripted;
use crate::model::producer::IDLE_RETIREMENT_MS;
use crate::open::OpenError;
use crate::open::OpenSpec;
use crate::open::StreamFormat;
use crate::open::StreamState;
use crate::open::Unavailable;
use crate::options::Access;
use crate::replicator::Pending;
use crate::replicator::ResetKind;
use crate::salvage::Reason;
use crate::salvage::Salvaged;
use crate::salvage::State;
use crate::testdir::TempDir;
use crate::transport::Transport;
use crate::transport::model::ModelTransport;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Target;

const PATH: &str = "/v1/stream/b/db";
const URL: &str = "http://model.test/v1/stream/b/db";

fn spec(dir: &Path, activation: u8) -> OpenSpec {
    OpenSpec {
        url: URL.to_owned(),
        dir: dir.to_owned(),
        activation: Activation([activation; 16]),
        max_pending_bytes: 1 << 20,
        max_request_bytes: None,
        timeout_ms: 30_000,
        access: Access::Owner { recreate: false },
    }
}

/// `page` as SQLite writes page 1: its magic in front.
fn sqlite(mut page: Box<[u8; 4096]>) -> Box<[u8; 4096]> {
    page[..16].copy_from_slice(&SQLITE_MAGIC);
    page
}

/// `image` (pages as SQLite writes them) as the db file of a generation of `lineage` holds it:
/// page 1 marked.
fn on_disk(lineage: Lineage, mut image: Vec<u8>) -> Vec<u8> {
    image[..16].copy_from_slice(&mark(lineage));
    image
}

/// A one-page db file of a generation of `lineage` whose page 1 a commit wrote filled with `salt`.
fn page_on_disk(lineage: Lineage, salt: u8) -> Vec<u8> {
    on_disk(lineage, sqlite(Box::new([salt; 4096])).to_vec())
}

fn server() -> ModelServer {
    ModelServer::new(ClusterId::new("cluster-a").unwrap(), 7)
}

/// A request a core sent, as the world delivered it.
#[derive(Debug, Clone)]
struct Sent {
    core: usize,
    request: Request,
}

/// A change to the server, run at a chosen moment.
type ServerHook = Box<dyn FnOnce(&mut ModelServer)>;

/// A change to the server after every HEAD it answers.
type EveryHead = Box<dyn FnMut(&mut ModelServer)>;

/// Rewrites the server's answer to a request before the client reads it.
type AnswerHook = Box<dyn FnMut(&Request, &mut RawAnswer)>;

/// Cores, the model server, a network each core can lose, and a virtual clock.
struct World {
    server: ModelServer,
    cores: Vec<Option<Core>>,
    down: Vec<bool>,
    queue: VecDeque<(usize, super::Io)>,
    now: u64,
    sent: Vec<Sent>,
    /// HEADs are answered as a protocol 1 server would.
    legacy: bool,
    /// Disk jobs it matches wait in `held` instead of running.
    hold: Option<fn(&DiskJob) -> bool>,
    held: Vec<(usize, DiskIo)>,
    /// Runs on the server after the next HEAD it answers, once `skip_heads` more went by.
    after_head: Option<ServerHook>,
    skip_heads: u32,
    /// Runs on the server after every HEAD it answers.
    every_head: Option<EveryHead>,
    /// Rewrites every answer of the server.
    rewrite: Option<AnswerHook>,
    /// Another deployment, which every request reaches while `misrouted`.
    other: ModelServer,
    misrouted: bool,
    /// Every request reaches `other` once this many more HEADs were answered.
    misroute_after_heads: Option<u32>,
    /// Reads (GETs) reach `other`, the rest the server.
    misroute_reads: bool,
    next_id: u64,
    /// The binding of each ticket `sync_to` handed out.
    tickets: HashMap<Ticket, BindingId>,
    /// How long a request of a core that is down waits for its connect to fail (0: at once), as
    /// a connect to a host that does not answer times out.
    connect_ms: u64,
    /// Requests whose connect fails at the time each names.
    connecting: Vec<(u64, usize, super::IoId)>,
}

impl World {
    fn new() -> Self {
        Self {
            server: server(),
            cores: Vec::new(),
            down: Vec::new(),
            queue: VecDeque::new(),
            now: 0,
            sent: Vec::new(),
            legacy: false,
            after_head: None,
            skip_heads: 0,
            every_head: None,
            rewrite: None,
            other: ModelServer::new(ClusterId::new("cluster-b").unwrap(), 9),
            misrouted: false,
            misroute_after_heads: None,
            misroute_reads: false,
            hold: None,
            held: Vec::new(),
            next_id: 1,
            tickets: HashMap::new(),
            connect_ms: 0,
            connecting: Vec::new(),
        }
    }

    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// A new core; returns its index.
    fn core(&mut self) -> usize {
        let seed = [u8::try_from(self.cores.len()).unwrap(); 32];
        self.cores.push(Some(Core::new(seed)));
        self.down.push(false);
        self.cores.len() - 1
    }

    fn c(&mut self, core: usize) -> &mut Core {
        self.cores[core].as_mut().expect("the core crashed")
    }

    /// The core dies: its files stay as they are, its requests in the queue are lost.
    fn crash(&mut self, core: usize) {
        self.cores[core] = None;
        self.queue.retain(|(c, _)| *c != core);
    }

    fn queue_ios(&mut self, core: usize, ios: Vec<super::Io>) {
        self.queue.extend(ios.into_iter().map(|io| (core, io)));
    }

    fn exchange(&mut self, core: usize, request: &Request) -> Exchange {
        if self.down[core] {
            return Exchange::NoAnswer(NoAnswer::NotSent {
                reason: "down".to_owned(),
            });
        }
        if self.legacy && request.method == Method::Head {
            return Exchange::Answered(Box::new(RawAnswer::new(200).parse(request)));
        }
        if self.misroute_after_heads == Some(0) {
            self.misroute_after_heads = None;
            self.misrouted = true;
        }
        if request.method == Method::Head
            && let Some(n) = self.misroute_after_heads
        {
            self.misroute_after_heads = Some(n.saturating_sub(1));
        }
        if self.misrouted || (self.misroute_reads && request.method == Method::Get) {
            let delivered = self.other.exchange(PATH, request);
            let Delivered::Answered { answer, .. } = delivered else {
                panic!("the other deployment answers");
            };
            return Exchange::Answered(Box::new(answer.parse(request)));
        }
        let delivered = self.server.exchange(PATH, request);
        if request.method == Method::Head {
            if self.skip_heads > 0 {
                self.skip_heads -= 1;
            } else if let Some(hook) = self.after_head.take() {
                hook(&mut self.server);
            }
            if let Some(hook) = &mut self.every_head {
                hook(&mut self.server);
            }
        }
        match delivered {
            Delivered::Answered { mut answer, .. } => {
                if let Some(rewrite) = &mut self.rewrite {
                    rewrite(request, &mut answer);
                }
                Exchange::Answered(Box::new(answer.parse(request)))
            }
            Delivered::Unreachable => Exchange::NoAnswer(NoAnswer::NotSent {
                reason: "unreachable".to_owned(),
            }),
            Delivered::DroppedBeforeApply | Delivered::DroppedAfterApply => {
                Exchange::NoAnswer(NoAnswer::Lost {
                    reason: "dropped".to_owned(),
                })
            }
        }
    }

    /// Delivers every queued request and runs every disk job, and those they lead to.
    fn deliver(&mut self) {
        while let Some((core, io)) = self.queue.pop_front() {
            if self.cores[core].is_none() {
                continue;
            }
            let now = self.now;
            let ios = match io {
                Io::Http(io) if self.down[core] && self.connect_ms > 0 => {
                    self.connecting.push((now + self.connect_ms, core, io.id));
                    Vec::new()
                }
                Io::Http(io) => {
                    self.sent.push(Sent {
                        core,
                        request: io.request.clone(),
                    });
                    let exchange = self.exchange(core, &io.request);
                    self.c(core).completed(io.id, exchange, now)
                }
                Io::Disk(io) if self.hold.is_some_and(|hold| hold(&io.job)) => {
                    self.held.push((core, io));
                    Vec::new()
                }
                Io::Disk(DiskIo { id, job }) => {
                    let done =
                        crate::guard::contain("test_disk", || DiskDone::Panicked, || job.run());
                    self.c(core).disk_done(id, done, now)
                }
            };
            self.queue_ios(core, ios);
        }
    }

    /// Runs every timer due up to `until`, then sets the clock there.
    fn run_until(&mut self, until: u64) {
        self.deliver();
        loop {
            let next = self
                .cores
                .iter()
                .flatten()
                .filter_map(Core::next_deadline)
                .chain(self.connecting.iter().map(|(at, _, _)| *at))
                .min();
            match next {
                Some(at) if at <= until => {
                    let at = at.max(self.now);
                    self.server.advance(at - self.now);
                    self.now = at;
                    let (failed, waiting) = std::mem::take(&mut self.connecting)
                        .into_iter()
                        .partition(|(when, _, _)| *when <= at);
                    self.connecting = waiting;
                    for (_, core, id) in failed {
                        if let Some(c) = self.cores[core].as_mut() {
                            let exchange = Exchange::NoAnswer(NoAnswer::NotSent {
                                reason: "connect timed out".to_owned(),
                            });
                            let ios = c.completed(id, exchange, at);
                            self.queue_ios(core, ios);
                        }
                    }
                    for core in 0..self.cores.len() {
                        if let Some(c) = self.cores[core].as_mut() {
                            let ios = c.tick(at);
                            self.queue_ios(core, ios);
                        }
                    }
                    self.deliver();
                }
                _ => break,
            }
        }
        if until > self.now {
            self.server.advance(until - self.now);
            self.now = until;
        }
    }

    fn run_for(&mut self, ms: u64) {
        self.run_until(self.now + ms);
    }

    fn events(&self, core: usize) -> Vec<DriverEvent> {
        self.cores[core].as_ref().unwrap().events_since(0).0
    }

    fn open_result(&mut self, core: usize, spec: OpenSpec) -> Result<Bound, Arc<OpenError>> {
        let now = self.now;
        let open = OpenId(self.id());
        let ios = self.c(core).open(open, spec, now);
        self.queue_ios(core, ios);
        let mut result = None;
        for _ in 0..1000 {
            result = self.c(core).take_opened(open);
            if result.is_some() {
                break;
            }
            self.run_for(100);
        }
        result.expect("the open never resolved")
    }

    /// An open's binding, with a connection open on it as the VFS opens one.
    fn open(&mut self, core: usize, spec: OpenSpec) -> Bound {
        let bound = self.open_result(core, spec).unwrap();
        assert!(bound.handle.connect(Holder::Host).is_some());
        bound
    }

    /// Commits a one-page transaction through the binding's log, as the commit path does.
    fn commit(&mut self, core: usize, bound: &Bound, salt: u8) -> Option<Vec<u8>> {
        self.commit_page(core, bound, Box::new([salt; 4096]))
    }

    fn commit_page(
        &mut self,
        core: usize,
        bound: &Bound,
        page: Box<[u8; 4096]>,
    ) -> Option<Vec<u8>> {
        let pending = Self::append(bound, page)?;
        let frame = pending.frame.to_vec();
        self.logged(core, bound, pending);
        Some(frame)
    }

    /// The commit path's log append, recorded on the handle; `Logged` is the caller's.
    fn append(bound: &Bound, page: Box<[u8; 4096]>) -> Option<Pending> {
        let handle = &bound.handle;
        let mut log = handle.log().unwrap();
        let position = log.next_position();
        let id = CommitId {
            lineage: handle.generation.lineage,
            position,
        };
        let record = CommitRecord {
            db_pages_after: 1,
            pages: vec![(1, sqlite(page))],
        };
        let frame = frame::encode_commit(id, &record).unwrap();
        if handle.admit(frame.len() as u64).is_err() {
            return None;
        }
        log.append(&RecordBody::Commit {
            frame: frame.clone(),
        })
        .unwrap();
        handle.record_commit(position);
        handle.add_pending(frame.len() as u64);
        drop(log);
        Some(Pending {
            id,
            frame: Arc::from(frame),
        })
    }

    /// The commit path's `Logged` reaches the driver.
    fn logged(&mut self, core: usize, bound: &Bound, pending: Pending) {
        let now = self.now;
        let ios = self.c(core).logged(bound.binding, pending, now);
        self.queue_ios(core, ios);
        self.deliver();
    }
    /// Runs the held disk jobs, and what they lead to.
    fn release_held(&mut self) {
        self.hold = None;
        for (core, DiskIo { id, job }) in std::mem::take(&mut self.held) {
            let done = job.run();
            let now = self.now;
            let ios = self.c(core).disk_done(id, done, now);
            self.queue_ios(core, ios);
        }
        self.deliver();
    }

    fn sync(&mut self, core: usize, binding: BindingId) -> Ticket {
        self.sync_to(core, binding, None)
    }

    fn sync_to(&mut self, core: usize, binding: BindingId, target: Option<Position>) -> Ticket {
        let now = self.now;
        let ticket = Ticket(self.id());
        self.tickets.insert(ticket, binding);
        let ios = self.c(core).sync(ticket, binding, target, now);
        self.queue_ios(core, ios);
        self.deliver();
        ticket
    }

    /// The binding's connection closes: it drains, then retires.
    fn retire(&mut self, core: usize, bound: &Bound) {
        let now = self.now;
        assert!(bound.handle.disconnect(Holder::Host), "the last connection");
        let ios = self.c(core).retire(bound.binding, now);
        self.queue_ios(core, ios);
        self.run_for(1_000);
    }

    /// The ticket's outcome: its binding's watermark reaching it, its own failure, or its
    /// binding's reset.
    fn outcome(&self, core: usize, ticket: Ticket) -> Option<Result<(), SyncError>> {
        let of = self.tickets.get(&ticket).copied();
        self.events(core).into_iter().find_map(|e| match e {
            DriverEvent::Synced { binding, through }
                if Some(binding) == of && through >= ticket =>
            {
                Some(Ok(()))
            }
            DriverEvent::SyncFailed { ticket: t, error } if t == ticket => Some(Err(error)),
            DriverEvent::Reset { binding, cause } if Some(binding) == of => {
                Some(Err(SyncError::Reset(cause)))
            }
            _ => None,
        })
    }

    fn resets(&self, core: usize) -> Vec<ResetKind> {
        self.events(core)
            .into_iter()
            .filter_map(|e| match e {
                DriverEvent::Reset { cause, .. } => Some(cause),
                _ => None,
            })
            .collect()
    }

    /// The COMMIT frames of the stream, in order.
    fn stream_commits(&self) -> Vec<Vec<u8>> {
        frames(self.server.frames(PATH).unwrap_or_default())
            .into_iter()
            .filter(|f| matches!(decode(f), Frame::Commit { .. }))
            .collect()
    }

    /// The keyed appends core `core` sent, as `(epoch, seq, body)`.
    fn appends(&self, core: usize) -> Vec<(Epoch, u64, Vec<u8>)> {
        self.sent
            .iter()
            .filter(|s| s.core == core && s.request.method == Method::Post)
            .filter_map(|s| {
                let write = s.request.keys.producer.as_ref()?;
                Some((write.epoch, write.seq.get(), s.request.body.to_vec()))
            })
            .collect()
    }

    /// The epochs of the claims core `core` sent.
    fn claims(&self, core: usize) -> Vec<Epoch> {
        self.appends(core)
            .into_iter()
            .filter(|(_, seq, _)| *seq == 0)
            .map(|(epoch, _, _)| epoch)
            .collect()
    }
}

fn decode(bytes: &[u8]) -> Frame {
    match frame::decode(bytes, u64::MAX).unwrap() {
        Decoded::Frame { frame, .. } => frame,
        Decoded::Partial => panic!("partial frame"),
    }
}

/// Splits stream bytes into frames.
fn frames(mut bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let Decoded::Frame { len, .. } = frame::decode(bytes, u64::MAX).unwrap() else {
            panic!("partial frame");
        };
        out.push(bytes[..len].to_vec());
        bytes = &bytes[len..];
    }
    out
}

fn kind(result: Result<Bound, Arc<OpenError>>) -> Arc<OpenError> {
    match result {
        Ok(_) => panic!("the open succeeded"),
        Err(e) => e,
    }
}

/// The token each log segment of the generation at `dir` names, in order.
fn segment_tokens(gen_dir: &Path) -> Vec<ursula_headers::Incarnation> {
    let mut segments: Vec<_> = std::fs::read_dir(gen_dir.join("log"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    segments.sort();
    segments
        .iter()
        .map(|s| scan(&std::fs::read(s).unwrap()).unwrap().header.incarnation)
        .collect()
}

/// The records of every log segment of the generation at `dir`.
fn log_records(gen_dir: &Path) -> Vec<RecordBody> {
    let mut segments: Vec<_> = std::fs::read_dir(gen_dir.join("log"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    segments.sort();
    segments
        .iter()
        .flat_map(|s| scan(&std::fs::read(s).unwrap()).unwrap().records)
        .map(|r| r.body)
        .collect()
}

// Thousands of `synced()` calls resolve through one watermark event of their binding, which
// stays one event in the queue however many tickets it covers, so a reader never loses them.
#[test]
fn tickets_resolve_in_order_through_one_watermark() {
    let dir = TempDir::new("driver-watermark");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    let tickets: Vec<Ticket> = (0..5_000).map(|_| w.sync(c, bound.binding)).collect();
    w.run_for(1_000);
    for &ticket in &tickets {
        assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    }
    let watermarks: Vec<Ticket> = w
        .events(c)
        .into_iter()
        .filter_map(|e| match e {
            DriverEvent::Synced { through, .. } => Some(through),
            _ => None,
        })
        .collect();
    assert_eq!(
        watermarks,
        tickets.last().copied().into_iter().collect::<Vec<_>>()
    );
}

// Admission (design §4.4) counts a commit from its log write: back-to-back commits that the driver
// thread has not heard of yet are refused at the bound, and admitted again once acknowledged.
#[test]
fn admission_counts_commits_the_driver_thread_has_not_seen() {
    let dir = TempDir::new("driver-admission");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, OpenSpec {
        max_pending_bytes: 6_000,
        ..spec(dir.path(), 1)
    });
    // Pages that do not compress: each frame takes about 4 KiB, so two do not fit.
    let page = |seed: u64| {
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut page = Box::new([0u8; 4096]);
        for byte in page.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *byte = x.to_le_bytes()[0];
        }
        page
    };
    // The driver thread is stalled: it hears of no commit (no `Logged`).
    let first = World::append(&bound, page(1)).unwrap();
    assert!(bound.handle.pending_bytes() > 4_000);
    assert!(World::append(&bound, page(2)).is_none());
    assert!(World::append(&bound, page(3)).is_none());
    w.logged(c, &bound, first);
    w.run_for(1_000);
    assert_eq!(bound.handle.pending_bytes(), 0);
    assert!(World::append(&bound, page(4)).is_some());
}

#[test]
fn an_open_creates_the_stream_commits_replicate_and_synced_resolves() {
    let dir = TempDir::new("driver-basic");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let mut committed = Vec::new();
    for salt in 1..=3 {
        committed.push(w.commit(c, &bound, salt).unwrap());
    }
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits(), committed);
    assert_eq!(bound.handle.pending_bytes(), 0);
    assert_eq!(bound.handle.name, canonical_path(URL).unwrap());
    assert!(w.events(c).iter().any(|e| matches!(
        e,
        DriverEvent::Opened { result: Ok(binding), .. } if *binding == bound.binding
    )));
    assert!(w.resets(c).is_empty());
}

#[test]
fn a_ten_minute_outage_ends_in_full_catch_up_without_a_reset() {
    let dir = TempDir::new("driver-outage");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, OpenSpec {
        max_pending_bytes: 16 << 10,
        ..spec(dir.path(), 1)
    });
    let mut committed = vec![w.commit(c, &bound, 0).unwrap()];
    w.run_for(1_000);
    w.down[c] = true;
    let outage_start = w.sent.len();
    let mut refused = 0;
    let mut salt = 1u8;
    while w.now < 1_000 + 600_000 {
        match w.commit(c, &bound, salt) {
            Some(frame) => committed.push(frame),
            None => refused += 1,
        }
        salt = salt.wrapping_add(1);
        w.run_for(100);
    }
    let attempts = w.sent.len() - outage_start;
    assert!(refused > 0, "commits continue until backlog_full");
    // Backoff doubles to 5 s with full jitter: about 2.5 s an attempt.
    assert!(
        (100..1_000).contains(&attempts),
        "{attempts} attempts in ten minutes"
    );
    assert!(w.events(c).iter().any(|e| matches!(
        e,
        DriverEvent::Condition {
            binding,
            condition: Some(crate::replicator::Condition::Retrying),
        } if *binding == bound.binding
    )));
    // The first append after healing applies and its answer is lost: the resend is a repeat.
    w.server.inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropAnswer,
        times: 1,
    });
    w.down[c] = false;
    w.run_for(60_000);
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(w.resets(c).is_empty());
    assert_eq!(w.stream_commits(), committed);
    assert_eq!(bound.handle.pending_bytes(), 0);
    assert!(w.commit(c, &bound, 0).is_some(), "the backlog drained");
}

#[test]
fn an_append_applied_before_a_crash_is_acknowledged_by_the_reopen_and_not_resent() {
    let dir = TempDir::new("driver-crash-applied");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.server.inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropAnswer,
        times: 1,
    });
    let x = w.commit(c, &bound, 9).unwrap();
    drop(bound);
    w.crash(c);
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    let ticket = w.sync(c2, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c2, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits(), vec![x.clone()]);
    assert!(
        w.appends(c2)
            .iter()
            .all(|(_, seq, body)| *seq == 0 || *body != x),
        "the reopen resent an applied commit"
    );
}

#[test]
fn an_append_never_applied_before_a_crash_is_resent_identically_under_the_new_epoch() {
    let dir = TempDir::new("driver-crash-lost");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.server.inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropRequest,
        times: 1,
    });
    let x = w.commit(c, &bound, 9).unwrap();
    let first = w.appends(c).last().cloned().unwrap();
    assert_eq!(first.2, x);
    drop(bound);
    w.crash(c);
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    w.run_for(1_000);
    let ticket = w.sync(c2, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c2, ticket), Some(Ok(())));
    let resent: Vec<_> = w
        .appends(c2)
        .into_iter()
        .filter(|(_, seq, _)| *seq > 0)
        .collect();
    assert_eq!(resent.len(), 1);
    let (epoch, seq, body) = &resent[0];
    assert!(*epoch > first.0);
    assert_eq!((*seq, body), (1, &x));
    assert_eq!(w.stream_commits(), vec![x]);
}

#[test]
fn a_takeover_fences_the_owner_which_keeps_its_tail_then_discards_it_and_rebuilds() {
    let dir_a = TempDir::new("driver-takeover-a");
    let dir_b = TempDir::new("driver-takeover-b");
    let mut w = World::new();
    let a = w.core();
    let bound_a = w.open(a, spec(dir_a.path(), 1));
    let c1 = w.commit(a, &bound_a, 1).unwrap();
    w.run_for(1_000);
    w.down[a] = true;
    let c2 = w.commit(a, &bound_a, 2).unwrap();
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    let d1 = w.commit(b, &bound_b, 3).unwrap();
    w.run_for(1_000);
    w.down[a] = false;
    w.run_for(10_000);
    assert_eq!(w.resets(a), vec![ResetKind::Fenced]);
    // The fenced owner's log keeps its pending commit for the next open's reconcile.
    let old = bound_a.handle.generation.clone();
    drop(bound_a);
    let records = log_records(&old.dir);
    assert!(records.contains(&RecordBody::Commit { frame: c2.clone() }));
    let a2 = w.core();
    let bound = w.open(a2, spec(dir_a.path(), 4));
    assert_ne!(bound.handle.generation.lineage, old.lineage, "rebuilt");
    assert!(!old.dir.exists());
    assert_eq!(w.stream_commits(), vec![c1, d1.clone()]);
    // The rebuilt generation holds the stream's last page, and no pending tail.
    assert_eq!(bound.handle.pending_bytes(), 0);
    let db = std::fs::read(bound.handle.generation.db()).unwrap();
    assert_eq!(db, page_on_disk(bound.handle.generation.lineage, 3));
    let ticket = w.sync(a2, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(a2, ticket), Some(Ok(())));
}

#[test]
fn a_reopen_without_pending_commits_catches_up_in_place() {
    let dir_a = TempDir::new("driver-catch-up-a");
    let dir_b = TempDir::new("driver-catch-up-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    let now = w.now;
    assert!(bound.handle.disconnect(Holder::Host));
    let ios = w.c(a).retire(bound.binding, now);
    w.queue_ios(a, ios);
    w.run_for(1_000);
    let generation = bound.handle.generation.clone();
    drop(bound);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    let d1 = w.commit(b, &bound_b, 2).unwrap();
    w.run_for(1_000);
    let a2 = w.core();
    let bound = w.open(a2, spec(dir_a.path(), 3));
    // Caught up in place: the same generation, now holding the other owner's commit.
    assert_eq!(bound.handle.generation, generation);
    assert_eq!(
        std::fs::read(generation.db()).unwrap(),
        page_on_disk(generation.lineage, 2)
    );
    let records = log_records(&generation.dir);
    assert!(records.contains(&RecordBody::Commit { frame: d1 }));
    let e1 = w.commit(a2, &bound, 3).unwrap();
    let ticket = w.sync(a2, bound.binding);
    w.run_for(2_000);
    assert_eq!(w.outcome(a2, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits().last(), Some(&e1));
    // Its ACKs name the caught-up records, so the next open trusts the log again.
    let now = w.now;
    assert!(bound.handle.disconnect(Holder::Host));
    let ios = w.c(a2).retire(bound.binding, now);
    w.queue_ios(a2, ios);
    w.run_for(1_000);
    drop(bound);
    let a3 = w.core();
    let bound = w.open(a3, spec(dir_a.path(), 4));
    assert_eq!(bound.handle.generation, generation);
    assert_eq!(bound.handle.pending_bytes(), 0);
}

/// The db file a full replay of `bytes` (stream frames) builds: every commit's pages, in order,
/// the file cut or grown to each commit's size. A model independent of the Applier.
fn image_of(bytes: &[u8]) -> Vec<u8> {
    let mut image = Vec::new();
    for f in frames(bytes) {
        if let Frame::Commit { compressed, .. } = decode(&f) {
            let record = frame::decompress(&compressed).unwrap();
            image.resize(record.db_pages_after as usize * 4096, 0);
            for (pgno, page) in record.pages {
                let at = (pgno as usize - 1) * 4096;
                image[at..at + 4096].copy_from_slice(&page[..]);
            }
        }
    }
    image
}

/// The highest epoch the CLAIM frames of `bytes` name.
fn highest_claim(bytes: &[u8]) -> Epoch {
    frames(bytes)
        .iter()
        .filter_map(|f| match decode(f) {
            Frame::Claim { epoch, .. } => Some(epoch),
            _ => None,
        })
        .max()
        .unwrap()
}

/// Publishes a real `USS3` snapshot at the stream's tail, the image a full replay builds, whose
/// last commit is `last`, and moves retention up to it.
fn snapshot_as(server: &mut ModelServer, last: CommitId) {
    let bytes = server.frames(PATH).unwrap().to_vec();
    let tail = ursula_headers::Offset::from_u64(bytes.len() as u64);
    let fields = crate::codec::snapshot::SnapshotFields {
        offset: tail,
        epoch: highest_claim(&bytes),
        last,
    };
    let mut body = Vec::new();
    crate::codec::snapshot::write_body(
        &fields,
        &mut std::io::Cursor::new(image_of(&bytes)),
        &mut body,
    )
    .unwrap();
    let mut digester = crate::codec::digest::SnapshotDigester::new(CONTENT_TYPE);
    digester.update(&body);
    let token = server.token(PATH).unwrap();
    let keys = ursula_headers::ProducerRequestHeaders {
        producer: None,
        precondition: Some(ursula_headers::IncarnationPrecondition { incarnation: token }),
    };
    let publish = Request {
        keys: keys.clone(),
        content_type: Some(CONTENT_TYPE.to_owned()),
        snapshot_digest: Some(digester.finalize()),
        body: Arc::from(body),
        ..Request::new(Method::Put, Target::Snapshot { offset: tail })
    };
    assert_eq!(server.handle(PATH, &publish).status, 204);
    let retain = Request {
        keys,
        ..Request::new(Method::Put, Target::Retention { offset: tail })
    };
    assert_eq!(server.handle(PATH, &retain).status, 204);
}

/// [`snapshot_as`] by an owner of another lineage.
fn snapshot_and_retain(server: &mut ModelServer) {
    snapshot_as(server, CommitId {
        lineage: crate::ids::Lineage([0x5a; 16]),
        position: Position(9),
    });
}

// A snapshot's boundary seals a batch ending exactly at `k`, and the driver hands the snapshot
// thread `W`, the end offset of that batch's receipt.
#[test]
fn a_boundary_seals_a_batch_ending_at_k_and_hands_back_its_receipt_end() {
    let dir = TempDir::new("driver-boundary");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.down[c] = true;
    for salt in 1..=4 {
        w.commit(c, &bound, salt).unwrap();
    }
    let positions: Vec<Position> = log_records(&bound.handle.generation.dir)
        .iter()
        .enumerate()
        .filter(|(_, r)| matches!(r, RecordBody::Commit { .. }))
        .map(|(i, _)| Position(i as u64 + 1))
        .collect();
    let k = positions[1];
    let now = w.now;
    let ios = w.c(c).boundary(bound.binding, k, now);
    w.queue_ios(c, ios);
    w.down[c] = false;
    w.run_for(30_000);
    let receipt = bound.handle.snapshots.snapper.wait_receipt(k).unwrap();
    // The first append ended at the second commit: W is where the third begins.
    let commits = w.stream_commits();
    let bytes = w.server.frames(PATH).unwrap().to_vec();
    let third = bytes
        .windows(commits[2].len())
        .position(|win| win == commits[2].as_slice())
        .unwrap();
    assert_eq!(
        receipt.offset,
        Some(ursula_headers::Offset::from_u64(third as u64))
    );
    assert_eq!(receipt.epoch, Some(Epoch::try_from(1).unwrap()));
    let batches: Vec<_> = w
        .appends(c)
        .into_iter()
        .filter(|(_, seq, _)| *seq > 0)
        .map(|(_, _, body)| body)
        .collect();
    // The first commit's batch was sealed (alone) before the outage; the next one ends at `k`
    // although the third and fourth were pending, and they follow in a batch of their own.
    assert!(batches.contains(&commits[1]), "{batches:?}");
    assert!(batches.contains(&[commits[2].clone(), commits[3].clone()].concat()));
}

/// A model stream at `PATH` with a claim and a commit, for publishes.
fn published_world() -> (Arc<Mutex<ModelServer>>, ursula_headers::Incarnation) {
    let dir = TempDir::new("driver-publish");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let token = w.server.token(PATH).unwrap();
    let server = std::mem::replace(&mut w.server, server());
    (Arc::new(Mutex::new(server)), token)
}

/// A `USS3` body file of `pages` pages at `offset`, written as the snapshot thread writes it.
struct BodyFile {
    _dir: TempDir,
    path: std::path::PathBuf,
    offset: ursula_headers::Offset,
    len: u64,
    digest: ursula_headers::SnapshotDigest,
}

fn body_file(offset: u64, pages: usize) -> BodyFile {
    let dir = TempDir::new("driver-publish-body");
    let image_path = dir.path().join("image");
    let image: Vec<u8> = (0..pages * 4096).map(|i| (i * 31 % 251) as u8).collect();
    std::fs::write(&image_path, &image).unwrap();
    let mut image = std::fs::File::open(&image_path).unwrap();
    let fields = crate::codec::snapshot::SnapshotFields {
        offset: ursula_headers::Offset::from_u64(offset),
        epoch: Epoch::try_from(1).unwrap(),
        last: CommitId {
            lineage: crate::ids::Lineage([1; 16]),
            position: Position(2),
        },
    };
    let path = dir.path().join("body");
    let (len, digest) = crate::snapshotter::write_snapshot(&fields, &mut image, &path).unwrap();
    BodyFile {
        _dir: dir,
        path,
        offset: fields.offset,
        len,
        digest,
    }
}

/// Publishes `body` under `request_limit` through the model transport.
fn publish_file(
    server: &Arc<Mutex<ModelServer>>,
    token: ursula_headers::Incarnation,
    request_limit: u64,
    body: &BodyFile,
) -> Result<crate::snapshotter::Outcome, crate::snapshotter::SnapshotFailure> {
    let transport = ModelTransport::new(Arc::clone(server), PATH);
    let snapper = crate::snapshotter::Snapper::default();
    crate::snapshotter::publish(&crate::snapshotter::Publish {
        remote: crate::snapshotter::Remote {
            transport: &transport,
            snapper: &snapper,
            cluster_id: "cluster-a",
            incarnation: token,
        },
        request_limit,
        offset: body.offset,
        digest: body.digest,
        body: &body.path,
        len: body.len,
    })
}

/// Publishes a body of `pages` pages at the stream's tail under `request_limit`.
fn publish_at_tail(
    server: &Arc<Mutex<ModelServer>>,
    token: ursula_headers::Incarnation,
    request_limit: u64,
    pages: usize,
) -> Result<crate::snapshotter::Outcome, crate::snapshotter::SnapshotFailure> {
    let tail = tail_of(server);
    publish_file(server, token, request_limit, &body_file(tail, pages))
}

fn tail_of(server: &Arc<Mutex<ModelServer>>) -> u64 {
    server.lock().unwrap().frames(PATH).unwrap().len() as u64
}

/// The stream's retained offset and latest snapshot offset, from a HEAD.
fn retention(server: &Arc<Mutex<ModelServer>>) -> (u64, Option<u64>) {
    let request = Request::new(Method::Head, Target::Stream);
    let answer = server
        .lock()
        .unwrap()
        .handle(PATH, &request)
        .parse(&request);
    match answer.typed {
        Ok(crate::wire::Typed::Head {
            stream: Some(head), ..
        }) => (
            head.retained_offset().get(),
            head.snapshot().map(|s| s.offset.get()),
        ),
        other => panic!("{other:?}"),
    }
}

/// Appends an unkeyed frame of bytes so the tail moves, as another commit would.
fn grow(server: &Arc<Mutex<ModelServer>>) {
    let commit = frame::encode_commit(
        CommitId {
            lineage: crate::ids::Lineage([1; 16]),
            position: Position(3),
        },
        &CommitRecord {
            db_pages_after: 1,
            pages: vec![(1, sqlite(Box::new([5; 4096])))],
        },
    )
    .unwrap();
    server
        .lock()
        .unwrap()
        .foreign_append(PATH, &commit)
        .unwrap();
}

// A body at or under the request limit is published, its 204 naming the digest; retention moves
// to the snapshot before it, one behind, and never before a first one; a body above the limit is
// not sent.
#[test]
fn publishes_keep_retention_one_snapshot_behind_and_refuse_bodies_above_the_limit() {
    let (server, token) = published_world();
    let first = server.lock().unwrap().frames(PATH).unwrap().len() as u64;
    let outcome = publish_at_tail(&server, token, 32 << 20, 2).unwrap();
    assert_eq!(outcome, crate::snapshotter::Outcome::Published {
        offset: ursula_headers::Offset::from_u64(first),
    });
    assert_eq!(retention(&server), (0, Some(first)));
    // Publishing the same body again is a repeat: still published.
    assert!(matches!(
        publish_at_tail(&server, token, 32 << 20, 2),
        Ok(crate::snapshotter::Outcome::Published { .. })
    ));
    grow(&server);
    let second = server.lock().unwrap().frames(PATH).unwrap().len() as u64;
    publish_at_tail(&server, token, 32 << 20, 2).unwrap();
    assert_eq!(retention(&server), (first, Some(second)));
    grow(&server);
    let third = server.lock().unwrap().frames(PATH).unwrap().len() as u64;
    publish_at_tail(&server, token, 32 << 20, 2).unwrap();
    assert_eq!(retention(&server), (second, Some(third)));
    // Another body at the same offset conflicts: snapshots stop with stream_state.
    let e = publish_at_tail(&server, token, 32 << 20, 3).unwrap_err();
    assert!(
        matches!(e, crate::snapshotter::SnapshotFailure::Conflict { .. }),
        "{e}"
    );
    assert!(e.to_string().starts_with("stream_state: "));
    // Above the request limit: not published, reported as request_too_large.
    grow(&server);
    let e = publish_at_tail(&server, token, 100, 2).unwrap_err();
    assert!(
        matches!(e, crate::snapshotter::SnapshotFailure::TooLarge {
            limit: Some(100),
            ..
        }),
        "{e}"
    );
    assert!(e.to_string().starts_with("request_too_large: "));
    assert_eq!(retention(&server), (second, Some(third)));
}

// A publish behind a later snapshot is superseded, and moves nothing.
#[test]
fn a_publish_behind_a_later_snapshot_is_superseded() {
    let (server, token) = published_world();
    let stale = server.lock().unwrap().frames(PATH).unwrap().len() as u64;
    grow(&server);
    let later = server.lock().unwrap().frames(PATH).unwrap().len() as u64;
    publish_at_tail(&server, token, 32 << 20, 1).unwrap();
    // A body for the earlier offset, published after the later one.
    let outcome = publish_file(&server, token, 32 << 20, &body_file(stale, 1)).unwrap();
    assert_eq!(outcome, crate::snapshotter::Outcome::Superseded {
        offset: ursula_headers::Offset::from_u64(later),
    });
    assert_eq!(retention(&server), (0, Some(later)));
}

/// Commits a transaction writing pages `pgnos` (of a db of `size` pages) with `salt`.
fn commit_pages(w: &mut World, core: usize, bound: &Bound, size: u32, pgnos: &[u32], salt: u8) {
    let handle = &bound.handle;
    let pending = {
        let mut log = handle.log().unwrap();
        let position = log.next_position();
        let id = CommitId {
            lineage: handle.generation.lineage,
            position,
        };
        let record = CommitRecord {
            db_pages_after: size,
            pages: pgnos
                .iter()
                .map(|&p| {
                    let page = Box::new([salt.wrapping_add(p as u8); 4096]);
                    (p, if p == 1 { sqlite(page) } else { page })
                })
                .collect(),
        };
        let frame = frame::encode_commit(id, &record).unwrap();
        log.append(&RecordBody::Commit {
            frame: frame.clone(),
        })
        .unwrap();
        handle.record_commit(position);
        handle.add_pending(frame.len() as u64);
        Pending {
            id,
            frame: Arc::from(frame),
        }
    };
    w.logged(core, bound, pending);
}

// Below retention, a fresh directory builds from the latest snapshot and the tail after it, and
// a trusted generation's takeover meets a 410 whose snapshot is another owner's: it rebuilds from
// that snapshot. Both equal a full replay of the stream; nothing acknowledged is lost.
#[test]
fn an_open_below_retention_builds_from_the_snapshot_and_the_tail() {
    let dir_a = TempDir::new("driver-gone-a");
    let dir_b = TempDir::new("driver-gone-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    commit_pages(&mut w, a, &bound, 3, &[1, 2, 3], 1);
    w.run_for(1_000);
    w.retire(a, &bound);
    let generation = bound.handle.generation.clone();
    drop(bound);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    commit_pages(&mut w, b, &bound_b, 4, &[2, 4], 2);
    w.run_for(1_000);
    // A full replay before retention moves: what every build below must equal.
    let full_dir = TempDir::new("driver-gone-full");
    snapshot_and_retain(&mut w.server);
    let retained = w.server.frames(PATH).unwrap().len();
    // The tail after the snapshot: a shrink, and a page rewritten.
    commit_pages(&mut w, b, &bound_b, 2, &[1], 3);
    w.run_for(1_000);
    let expected = image_of(w.server.frames(PATH).unwrap());
    assert_eq!(expected.len(), 2 * 4096);
    // The fresh directory reads the snapshot, then only the frames after it.
    let c = w.core();
    let sent_before = w.sent.len();
    let bound_c = w.open(c, spec(full_dir.path(), 4));
    assert_eq!(
        std::fs::read(bound_c.handle.generation.db()).unwrap(),
        on_disk(bound_c.handle.generation.lineage, expected.clone())
    );
    let reads: Vec<_> = w.sent[sent_before..]
        .iter()
        .filter(|s| s.core == c)
        .map(|s| s.request.target)
        .collect();
    assert!(reads.contains(&Target::Snapshot {
        offset: ursula_headers::Offset::from_u64(retained as u64),
    }));
    assert!(!reads.contains(&Target::Read {
        from: None,
        leader: false
    }));
    w.retire(c, &bound_c);
    drop(bound_c);
    // The trusted generation's takeover: a 410, another owner's snapshot, a rebuild.
    let a2 = w.core();
    let bound = w.open(a2, spec(dir_a.path(), 3));
    assert_ne!(
        bound.handle.generation.lineage, generation.lineage,
        "rebuilt"
    );
    assert_eq!(
        std::fs::read(bound.handle.generation.db()).unwrap(),
        on_disk(bound.handle.generation.lineage, expected.clone())
    );
    // It streams from there: its next commit lands and synced() resolves.
    let e1 = w.commit(a2, &bound, 9).unwrap();
    let ticket = w.sync(a2, bound.binding);
    w.run_for(2_000);
    assert_eq!(w.outcome(a2, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits().last(), Some(&e1));
}

// A takeover whose records retention passed, with a snapshot of its own lineage past its durable
// acknowledged position (its ACK record lagging): clones share a lineage, so the 410 rule never
// trusts it, and the takeover rebuilds from it. Nothing acknowledged is lost.
#[test]
fn a_410_with_our_lineages_snapshot_past_the_durable_ack_rebuilds() {
    let dir = TempDir::new("driver-gone-own");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir.path(), 1));
    // Acknowledged within the ACK interval, then a crash: the log's ACK lags.
    let c1 = w.commit(a, &bound, 1).unwrap();
    w.deliver();
    let generation = bound.handle.generation.clone();
    let position = Position(bound.handle.last_commit().0);
    drop(bound);
    w.crash(a);
    assert!(
        !log_records(&generation.dir)
            .iter()
            .any(|r| matches!(r, RecordBody::Ack { acked, .. } if *acked == position))
    );
    snapshot_as(&mut w.server, CommitId {
        lineage: generation.lineage,
        position,
    });
    let a2 = w.core();
    let bound = w.open(a2, spec(dir.path(), 2));
    assert_ne!(bound.handle.generation, generation, "rebuilt");
    assert_eq!(bound.handle.pending_bytes(), 0);
    assert_eq!(w.stream_commits(), vec![c1]);
    assert_eq!(
        std::fs::read(bound.handle.generation.db()).unwrap(),
        page_on_disk(bound.handle.generation.lineage, 1)
    );
}

// A fresh open whose HEAD is overtaken: a frame lands and a snapshot past the HEAD's tail moves
// retention before the build's own HEAD. The build reads up to the tail its HEAD names, from that
// snapshot, and the open succeeds well within its deadline.
#[test]
fn a_fresh_open_overtaken_by_a_snapshot_past_its_head_builds_from_it() {
    let dir_b = TempDir::new("driver-overtaken-b");
    let dir_c = TempDir::new("driver-overtaken-c");
    let mut w = World::new();
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    commit_pages(&mut w, b, &bound_b, 2, &[1, 2], 4);
    w.run_for(1_000);
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        let frame = frame::encode_commit(
            CommitId {
                lineage: crate::ids::Lineage([0x6b; 16]),
                position: Position(1),
            },
            &CommitRecord {
                db_pages_after: 3,
                pages: vec![(3, Box::new([8; 4096]))],
            },
        )
        .unwrap();
        server.foreign_append(PATH, &frame).unwrap();
        snapshot_and_retain(server);
    }));
    let started = w.now;
    let c = w.core();
    let bound_c = w.open(c, spec(dir_c.path(), 3));
    assert!(w.now - started < 30_000, "{} ms", w.now - started);
    let expected = image_of(w.server.frames(PATH).unwrap());
    assert_eq!(expected.len(), 3 * 4096);
    assert_eq!(
        std::fs::read(bound_c.handle.generation.db()).unwrap(),
        on_disk(bound_c.handle.generation.lineage, expected)
    );
    // One HEAD for the open and one for the build: no restart loop.
    let heads = w.sent[..]
        .iter()
        .filter(|s| s.core == c && s.request.method == Method::Head)
        .count();
    assert_eq!(heads, 2);
}

/// Appends a commit of another writer, so the tail moves.
fn foreign_commit(server: &mut ModelServer, salt: u8) {
    let frame = frame::encode_commit(
        CommitId {
            lineage: crate::ids::Lineage([0x6c; 16]),
            position: Position(u64::from(salt)),
        },
        &CommitRecord {
            db_pages_after: 1,
            pages: vec![(1, sqlite(Box::new([salt; 4096])))],
        },
    )
    .unwrap();
    server.foreign_append(PATH, &frame).unwrap();
}

/// Appends a commit of another writer of `pages` pages that do not compress, so a snapshot of it
/// spans that many 4 KiB chunks and more.
fn incompressible_commit(server: &mut ModelServer, pages: u32) {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut page = |pgno: u32| {
        let mut page = Box::new([0u8; 4096]);
        for byte in page.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        if pgno == 1 { sqlite(page) } else { page }
    };
    let frame = frame::encode_commit(
        CommitId {
            lineage: crate::ids::Lineage([0x6d; 16]),
            position: Position(1),
        },
        &CommitRecord {
            db_pages_after: pages,
            pages: (1..=pages).map(|p| (p, page(p))).collect(),
        },
    )
    .unwrap();
    server.foreign_append(PATH, &frame).unwrap();
}

/// A stream whose latest snapshot (retention at it) spans five 4 KiB chunks and more, and a frame
/// after it: the image a fresh build must end with.
fn chunked_snapshot(w: &mut World, owner_dir: &Path) -> Vec<u8> {
    let a = w.core();
    let bound = w.open(a, spec(owner_dir, 1));
    w.retire(a, &bound);
    drop(bound);
    incompressible_commit(&mut w.server, 5);
    snapshot_and_retain(&mut w.server);
    foreign_commit(&mut w.server, 7);
    image_of(w.server.frames(PATH).unwrap())
}

/// The ranges of the snapshot reads `core` sent.
fn snapshot_ranges(w: &World, core: usize) -> Vec<Option<ursula_headers::ByteRange>> {
    w.sent
        .iter()
        .filter(|s| s.core == core && matches!(s.request.target, Target::Snapshot { .. }))
        .map(|s| s.request.range)
        .collect()
}

fn chunked_spec(dir: &Path) -> OpenSpec {
    OpenSpec {
        max_request_bytes: Some(4096),
        ..spec(dir, 3)
    }
}

// A fresh build reads the snapshot in chunks of the request limit, each from where the last
// ended, and installs it once whole: the image, and no downloaded body left in the generation.
#[test]
fn a_build_reads_the_snapshot_in_chunks_of_the_request_limit() {
    let dir = TempDir::new("driver-chunks");
    let owner = TempDir::new("driver-chunks-owner");
    let mut w = World::new();
    let expected = chunked_snapshot(&mut w, owner.path());
    let c = w.core();
    let bound = w.open(c, chunked_spec(dir.path()));
    assert_eq!(
        std::fs::read(bound.handle.generation.db()).unwrap(),
        on_disk(bound.handle.generation.lineage, expected)
    );
    assert!(!bound.handle.generation.dir.join("snapshot.body").exists());
    let ranges = snapshot_ranges(&w, c);
    assert!(ranges.len() >= 5, "{ranges:?}");
    for (i, range) in ranges.iter().enumerate() {
        let first = 4096 * i as u64;
        let Some(ursula_headers::ByteRange::Bounded { first: f, last }) = *range else {
            panic!("{range:?}");
        };
        assert_eq!(f, first, "{ranges:?}");
        // Every chunk asks for 4 KiB, the last one up to the body's end.
        assert!(last <= first + 4095, "{ranges:?}");
        assert_eq!(last == first + 4095, i + 1 < ranges.len(), "{ranges:?}");
    }
}

// A `206` whose `Content-Range` does not continue the download: what was downloaded is not
// trusted, the build deletes its `.tmp` and starts over from the HEAD, then completes.
#[test]
fn a_chunk_that_does_not_continue_the_download_restarts_the_build() {
    let dir = TempDir::new("driver-chunk-mismatch");
    let owner = TempDir::new("driver-chunk-mismatch-owner");
    let mut w = World::new();
    let expected = chunked_snapshot(&mut w, owner.path());
    let mut broken = false;
    w.rewrite = Some(Box::new(move |request, answer| {
        if broken || answer.status != 206 {
            return;
        }
        let Some(ursula_headers::ByteRange::Bounded { first: 8192, last }) = request.range else {
            return;
        };
        // The second chunk's bytes, named as if they were the first's.
        broken = true;
        let total = answer
            .headers
            .iter()
            .find(|(name, _)| *name == "content-range")
            .and_then(|(_, value)| value.rsplit('/').next())
            .unwrap()
            .to_owned();
        answer.headers.retain(|(name, _)| *name != "content-range");
        answer.header("content-range", format!("bytes 0-{}/{total}", last - 8192));
    }));
    let c = w.core();
    let bound = w.open(c, chunked_spec(dir.path()));
    assert_eq!(
        std::fs::read(bound.handle.generation.db()).unwrap(),
        on_disk(bound.handle.generation.lineage, expected)
    );
    let ranges = snapshot_ranges(&w, c);
    let starts: Vec<u64> = ranges
        .iter()
        .map(|r| match r {
            Some(ursula_headers::ByteRange::Bounded { first, .. }) => *first,
            other => panic!("{other:?}"),
        })
        .collect();
    // Two chunks, the broken third, then the whole body again from its first byte.
    assert_eq!(&starts[..4], &[0, 4096, 8192, 0], "{starts:?}");
    let heads = w
        .sent
        .iter()
        .filter(|s| s.core == c && s.request.method == Method::Head)
        .count();
    assert_eq!(
        heads, 3,
        "the open's HEAD, the build's, and the restarted build's"
    );
}

// A server that ignores `Range` answers `200` with the whole body: the build installs it at
// once, at the first read or after chunks were downloaded.
#[test]
fn a_whole_body_answer_to_a_ranged_read_installs_it() {
    for after_chunks in [0, 2] {
        let dir = TempDir::new("driver-chunk-ignored");
        let owner = TempDir::new("driver-chunk-ignored-owner");
        let mut w = World::new();
        let expected = chunked_snapshot(&mut w, owner.path());
        let c = w.core();
        let open = OpenId(w.id());
        if after_chunks == 0 {
            w.server.inject(Scripted {
                method: Some(Method::Get),
                fault: Fault::IgnoreRange,
                times: 1,
            });
            let now = w.now;
            let ios = w.c(c).open(open, chunked_spec(dir.path()), now);
            w.queue_ios(c, ios);
        } else {
            w.hold = Some(|job| matches!(job, DiskJob::Download { .. }));
            let now = w.now;
            let ios = w.c(c).open(open, chunked_spec(dir.path()), now);
            w.queue_ios(c, ios);
            // Each chunk's disk job runs alone, so the fault meets the read after the last.
            for _ in 0..after_chunks {
                w.deliver();
                let (core, DiskIo { id, job }) = w.held.pop().unwrap();
                let done = job.run();
                let now = w.now;
                let ios = w.c(core).disk_done(id, done, now);
                w.queue_ios(core, ios);
            }
            w.hold = None;
            w.server.inject(Scripted {
                method: Some(Method::Get),
                fault: Fault::IgnoreRange,
                times: 1,
            });
            w.deliver();
        }
        w.run_for(1_000);
        let bound = w.c(c).take_opened(open).unwrap().unwrap();
        assert_eq!(
            std::fs::read(bound.handle.generation.db()).unwrap(),
            on_disk(bound.handle.generation.lineage, expected)
        );
        assert_eq!(snapshot_ranges(&w, c).len(), after_chunks + 1);
    }
}

// A takeover's rebuild whose HEAD names a snapshot past its claim: another owner wrote after the
// claim, so the open fails with a fenced reset instead of reading the stream.
#[test]
fn a_rebuild_facing_a_snapshot_past_its_claim_is_fenced() {
    let dir_a = TempDir::new("driver-rebuild-fenced-a");
    let dir_b = TempDir::new("driver-rebuild-fenced-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.down[a] = true;
    w.commit(a, &bound, 2).unwrap();
    drop(bound);
    w.crash(a);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    w.commit(b, &bound_b, 3).unwrap();
    w.run_for(1_000);
    // The reopen diverges and rebuilds; before the rebuild's HEAD, the stream moves on past its
    // claim and a snapshot there moves retention.
    w.hold = Some(|job| matches!(job, DiskJob::NewBuild(_)));
    let a2 = w.core();
    let now = w.now;
    let open = OpenId(w.id());
    let ios = w.c(a2).open(open, spec(dir_a.path(), 3), now);
    w.queue_ios(a2, ios);
    w.run_for(5_000);
    assert_eq!(w.held.len(), 1, "the rebuild started");
    foreign_commit(&mut w.server, 4);
    snapshot_and_retain(&mut w.server);
    w.release_held();
    w.run_for(5_000);
    let e = kind(w.c(a2).take_opened(open).expect("resolved"));
    assert!(matches!(*e, OpenError::Reset(ResetKind::Fenced)), "{e}");
}

// Retention passing every snapshot a fresh build installs: the build restarts after the open's
// backoff (virtual time moves), at most 8 times, then fails with `BelowRetention`.
#[test]
fn a_build_that_keeps_meeting_410s_gives_up_after_its_restarts() {
    let dir_b = TempDir::new("driver-restart-cap-b");
    let dir_c = TempDir::new("driver-restart-cap-c");
    let mut w = World::new();
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    w.commit(b, &bound_b, 1).unwrap();
    w.run_for(1_000);
    snapshot_and_retain(&mut w.server);
    foreign_commit(&mut w.server, 9);
    let c = w.core();
    let started = w.now;
    let open = OpenId(w.id());
    let ios = w.c(c).open(open, spec(dir_c.path(), 3), started);
    w.queue_ios(c, ios);
    let mut installs = 0;
    let mut result = None;
    for salt in 10..200u8 {
        w.hold = Some(|job| matches!(job, DiskJob::Download { .. }));
        w.run_for(100);
        if let Some(r) = w.c(c).take_opened(open) {
            result = Some(r);
            break;
        }
        if !w.held.is_empty() {
            // Retention passes the snapshot being installed before its tail is read, and a
            // frame follows the new one, so the next build has a tail to read too.
            installs += 1;
            foreign_commit(&mut w.server, salt);
            snapshot_and_retain(&mut w.server);
            foreign_commit(&mut w.server, salt.wrapping_add(100));
            w.release_held();
        }
    }
    let e = kind(result.expect("the open resolved"));
    assert!(
        matches!(
            *e,
            OpenError::StreamState(StreamState::BelowRetention { .. })
        ),
        "{e}"
    );
    assert_eq!(installs, 9, "one install, then 8 restarts");
    assert!(
        w.now > started + 900,
        "restarts back off: {} ms",
        w.now - started
    );
    let heads = w
        .sent
        .iter()
        .filter(|s| s.core == c && s.request.method == Method::Head)
        .count();
    assert_eq!(heads, 10, "the open's HEAD and one per build");
}

#[test]
fn a_410_at_a_takeover_with_a_pending_commit_rebuilds_from_the_snapshot() {
    let dir_a = TempDir::new("driver-gone-pending-a");
    let dir_b = TempDir::new("driver-gone-pending-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.down[a] = true;
    let pending = w.commit(a, &bound, 2).unwrap();
    let generation = bound.handle.generation.clone();
    drop(bound);
    w.crash(a);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    w.commit(b, &bound_b, 3).unwrap();
    w.run_for(1_000);
    // Retention moves after the reopen's HEAD: the takeover claims, its reconcile meets a 410,
    // the latest snapshot is another owner's, and the pending commit (which never reached the
    // stream) is discarded by a rebuild from that snapshot.
    w.after_head = Some(Box::new(snapshot_and_retain));
    let a2 = w.core();
    let bound = w.open(a2, spec(dir_a.path(), 3));
    assert!(!generation.dir.exists());
    assert_ne!(bound.handle.generation.lineage, generation.lineage);
    assert_eq!(
        std::fs::read(bound.handle.generation.db()).unwrap(),
        page_on_disk(bound.handle.generation.lineage, 3)
    );
    assert!(!w.stream_commits().contains(&pending));
    assert_eq!(bound.handle.pending_bytes(), 0);
}

#[test]
fn idle_expiry_claims_own_plus_one_and_carries_on() {
    let dir = TempDir::new("driver-expiry");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let own = *w.claims(c).last().unwrap();
    w.run_for(IDLE_RETIREMENT_MS + 1);
    let c2 = w.commit(c, &bound, 2).unwrap();
    w.run_for(1_000);
    assert_eq!(w.claims(c), vec![
        own,
        Epoch::try_from(own.get() + 1).unwrap()
    ]);
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(w.resets(c).is_empty());
    assert_eq!(w.stream_commits().last(), Some(&c2));
}

#[test]
fn a_continuation_after_another_owners_claim_loses_and_fences() {
    let dir = TempDir::new("driver-expiry-lost");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let own = *w.claims(c).last().unwrap();
    w.run_for(IDLE_RETIREMENT_MS + 1);
    let other = Epoch::try_from(own.get() + 5).unwrap();
    let body = frame::encode_claim(other, Activation([0xee; 16]));
    let answer = w.server.foreign_claim(PATH, other, &body).unwrap();
    assert_eq!(answer.status, 200);
    w.run_for(IDLE_RETIREMENT_MS + 1);
    w.commit(c, &bound, 2).unwrap();
    w.run_for(10_000);
    assert_eq!(w.claims(c), vec![
        own,
        Epoch::try_from(own.get() + 1).unwrap()
    ]);
    assert_eq!(w.resets(c), vec![ResetKind::Fenced]);
}

/// Opens, commits once, changes the stream with `change`, commits again: the reset that follows.
fn reset_after(change: impl FnOnce(&mut ModelServer)) -> Vec<ResetKind> {
    let dir = TempDir::new("driver-reset");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    change(&mut w.server);
    // Waits for the commit the change keeps from being acknowledged: the call comes after its
    // append and before its `Logged`, so the ticket is held until then.
    let pending = World::append(&bound, Box::new([2; 4096])).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.logged(c, &bound, pending);
    w.run_for(10_000);
    let resets = w.resets(c);
    assert!(w.events(c).iter().all(|e| match e {
        DriverEvent::Reset { binding, .. } => *binding == bound.binding,
        _ => true,
    }));
    let [kind] = resets.as_slice() else {
        panic!("{resets:?}");
    };
    assert_eq!(w.outcome(c, ticket), Some(Err(SyncError::Reset(*kind))));
    resets
}

/// A history event that loses nothing while bound: the append meets the new token, the HEAD
/// shows the history inherits the generation, and it re-keys (design §6.2). The log rolls to a
/// segment naming the new token with the claim of `own + 1`, the binding takes the token, the
/// pending commit is resent under the new epoch, and `synced()` resolves with no reset.
#[test]
fn an_inheriting_history_re_keys_without_a_reset() {
    let dir = TempDir::new("driver-rekey");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let c1 = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let before = bound.handle.incarnation();
    let own = *w.claims(c).last().unwrap();
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c2 = w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.run_for(10_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(w.resets(c).is_empty());
    let token = w.server.token(PATH).unwrap();
    assert_ne!(token, before);
    assert_eq!(bound.handle.incarnation(), token);
    assert_eq!(w.claims(c), vec![
        own,
        Epoch::try_from(own.get() + 1).unwrap()
    ]);
    assert_eq!(w.stream_commits(), vec![c1, c2.clone()]);
    // The resent commit went out under the new epoch, from seq 1.
    let (epoch, seq, body) = w.appends(c).last().cloned().unwrap();
    assert_eq!((epoch.get(), seq, body), (own.get() + 1, 1, c2));
    assert_eq!(segment_tokens(&bound.handle.generation.dir), vec![
        before, token
    ]);
}

/// A restore that lost an acknowledged commit resets the binding `rewound` once its HEAD shows
/// the history does not inherit the generation, and its latest ACK names the exact acknowledged
/// offset, fsynced, so the next open's test fails the same way (design §6.2, D5): the reopen
/// salvages the generation (`history_changed`), whose db holds the lost commit, and builds anew.
#[test]
fn a_lossy_restore_resets_rewound_and_the_reopen_salvages() {
    let dir = TempDir::new("driver-rewound");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    let c1 = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let acked = bound.handle.acked();
    let before = bound.handle.incarnation();
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    let ticket = w.sync(c, bound.binding);
    w.run_for(10_000);
    assert_eq!(w.resets(c), vec![ResetKind::Rewound]);
    assert_eq!(
        w.outcome(c, ticket),
        Some(Err(SyncError::Reset(ResetKind::Rewound)))
    );
    let generation = bound.handle.generation.dir.clone();
    let last_ack = log_records(&generation)
        .into_iter()
        .rev()
        .find_map(|r| match r {
            RecordBody::Ack { acked, .. } => Some(acked),
            _ => None,
        });
    assert_eq!(last_ack, Some(acked));
    drop(bound);
    let c2 = w.core();
    let reopened = w.open(c2, spec(dir.path(), 2));
    assert_eq!(
        reopened.handle.attach,
        salvaged_attach(Reason::HistoryChanged)
    );
    assert!(!generation.exists());
    let s = only_salvage(&generation, Reason::HistoryChanged);
    let m = &s.manifest;
    assert_eq!(m.incarnation, Some(before));
    assert_eq!(m.stream_incarnation, Some(w.server.token(PATH).unwrap()));
    assert_eq!(m.acknowledged.map(|a| a.position), Some(acked));
    assert!(!w.stream_commits().contains(&c1));
    assert_eq!(salvaged_db(&s), sqlite(Box::new([1; 4096])).to_vec());
    assert!(holds_commit(&s, &c1));
}

/// A misrouted request meets another deployment: the HEAD names another cluster, the binding
/// blocks `wrong_cluster` with no reset, nothing applies there, and the binding resumes once the
/// route is fixed (design §6.3).
#[test]
fn a_misroute_blocks_and_then_resumes() {
    let dir = TempDir::new("driver-misroute");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.misrouted = true;
    let c2 = w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.run_for(5_000);
    let condition = bound.handle.replication().condition;
    assert_eq!(
        condition,
        Some(crate::replicator::Condition::Blocked(
            crate::replicator::BlockedReason::WrongCluster
        ))
    );
    assert!(w.resets(c).is_empty());
    assert_eq!(w.outcome(c, ticket), None);
    assert!(w.other.frames(PATH).is_none());
    w.misrouted = false;
    w.run_for(31_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(w.resets(c).is_empty());
    assert_eq!(bound.handle.replication().condition, None);
    assert_eq!(w.stream_commits().last(), Some(&c2));
}

/// After a re-key the binding's token is the new one, and a snapshot publish under it succeeds;
/// one under the old token, or answered as another deployment, fails without stopping snapshots
/// (design §6.2, G9).
#[test]
fn snapshots_publish_under_the_new_token_and_a_stale_token_is_transient() {
    let (server, old) = published_world();
    server
        .lock()
        .unwrap()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let new = server.lock().unwrap().token(PATH).unwrap();
    let body = body_file(tail_of(&server), 1);
    let stale = publish_file(&server, old, 1 << 20, &body).unwrap_err();
    assert!(
        matches!(stale, crate::snapshotter::SnapshotFailure::TokenChanged),
        "{stale}"
    );
    assert!(!stale.stops());
    let transport = ModelTransport::new(Arc::clone(&server), PATH);
    let snapper = crate::snapshotter::Snapper::default();
    let misrouted = crate::snapshotter::publish(&crate::snapshotter::Publish {
        remote: crate::snapshotter::Remote {
            transport: &transport,
            snapper: &snapper,
            cluster_id: "cluster-b",
            incarnation: new,
        },
        request_limit: 1 << 20,
        offset: body.offset,
        digest: body.digest,
        body: &body.path,
        len: body.len,
    })
    .unwrap_err();
    assert!(
        matches!(
            misrouted,
            crate::snapshotter::SnapshotFailure::WrongCluster { .. }
        ),
        "{misrouted}"
    );
    assert!(!misrouted.stops());
    let published = publish_file(&server, new, 1 << 20, &body).unwrap();
    assert!(matches!(
        published,
        crate::snapshotter::Outcome::Published { .. }
    ));
}

#[test]
fn a_404_confirmed_by_head_resets_deleted() {
    assert_eq!(reset_after(|s| s.delete(PATH).unwrap()), vec![
        ResetKind::Deleted
    ]);
}

#[test]
fn a_closed_stream_resets_stream_state() {
    assert_eq!(reset_after(|s| s.close(PATH).unwrap()), vec![
        ResetKind::StreamState
    ]);
}

#[test]
fn a_foreign_append_breaks_receipt_contiguity_and_resets_stream_state() {
    assert_eq!(
        reset_after(|s| {
            s.foreign_append(PATH, b"not ours").unwrap();
        }),
        vec![ResetKind::StreamState]
    );
}

#[test]
fn an_idle_owner_pays_one_probe_per_synced() {
    let dir = TempDir::new("driver-probe");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(5_000);
    for _ in 0..3 {
        let before = w.sent.len();
        let ticket = w.sync(c, bound.binding);
        w.run_for(5_000);
        assert_eq!(w.outcome(c, ticket), Some(Ok(())));
        let probes: Vec<_> = w.sent[before..].iter().map(|s| s.request.target).collect();
        assert_eq!(probes.len(), 1, "{probes:?}");
        assert!(matches!(probes[0], Target::Read { leader: true, .. }));
    }
}

#[test]
fn acks_are_written_at_most_once_a_second() {
    let dir = TempDir::new("driver-acks");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let start = w.now;
    let before = log_records(&bound.handle.generation.dir)
        .iter()
        .filter(|r| matches!(r, RecordBody::Ack { .. }))
        .count();
    for salt in 0..50 {
        w.commit(c, &bound, salt).unwrap();
        w.run_for(20);
    }
    w.run_for(2_000);
    let acks = log_records(&bound.handle.generation.dir)
        .iter()
        .filter(|r| matches!(r, RecordBody::Ack { .. }))
        .count()
        - before;
    let seconds = (w.now - start) / 1_000;
    assert!(
        acks >= 1 && acks <= usize::try_from(seconds).unwrap() + 1,
        "{acks} ACKs in {seconds} s"
    );
    // The last ACK names the last commit.
    let last_ack = log_records(&bound.handle.generation.dir)
        .into_iter()
        .rev()
        .find_map(|r| match r {
            RecordBody::Ack { acked, .. } => Some(acked),
            _ => None,
        });
    let last_commit = bound.handle.log().unwrap().next_position().0 - 2;
    assert!(last_ack >= Some(Position(last_commit)), "{last_ack:?}");
}

#[test]
fn a_server_without_protocol_2_is_refused() {
    let dir = TempDir::new("driver-too-old");
    let mut w = World::new();
    w.legacy = true;
    let c = w.core();
    let e = kind(w.open_result(c, spec(dir.path(), 1)));
    assert!(
        matches!(*e, OpenError::ServerTooOld { protocol: None }),
        "{e}"
    );
}

#[test]
fn a_stream_of_another_content_type_is_refused() {
    let dir = TempDir::new("driver-format");
    let mut w = World::new();
    let create = Request {
        content_type: Some("application/octet-stream".to_owned()),
        ..Request::new(Method::Put, Target::Stream)
    };
    assert_eq!(w.server.handle(PATH, &create).status, 201);
    let c = w.core();
    let e = kind(w.open_result(c, spec(dir.path(), 1)));
    assert!(
        matches!(
            &*e,
            OpenError::StreamFormat(StreamFormat::ContentType { .. })
        ),
        "{e}"
    );
    assert_eq!(e.error_kind().as_str(), "stream_format");
}

#[test]
fn a_stream_of_usq1_frames_is_refused() {
    let dir = TempDir::new("driver-usq1");
    let mut w = World::new();
    let create = Request {
        content_type: Some(CONTENT_TYPE.to_owned()),
        ..Request::new(Method::Put, Target::Stream)
    };
    assert_eq!(w.server.handle(PATH, &create).status, 201);
    w.server.foreign_append(PATH, b"USQ1\x01\0\0\0\0").unwrap();
    let c = w.core();
    let e = kind(w.open_result(c, spec(dir.path(), 1)));
    assert!(
        matches!(&*e, OpenError::StreamFormat(StreamFormat::Usq1)),
        "{e}"
    );
}

/// Opens, commits once and retires the binding, then changes the stream with `change` and opens
/// again in a new core.
fn reopen_after(
    change: impl FnOnce(&mut World, &Path),
) -> (Result<Bound, Arc<OpenError>>, std::path::PathBuf, TempDir) {
    let dir = TempDir::new("driver-reopen");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let now = w.now;
    assert!(bound.handle.disconnect(Holder::Host));
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    assert!(
        w.events(c)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding } if *binding == bound.binding))
    );
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    change(&mut w, &generation);
    let c2 = w.core();
    let result = w.open_result(c2, spec(dir.path(), 2));
    (result, generation, dir)
}

/// A clean close leaves every commit acknowledged; the stream then changes history and loses
/// nothing. The reopen re-keys the trusted generation (`rekeyed`): the log rolls to a segment
/// naming the new token, nothing is resent, and the next commit goes out under the new token.
#[test]
fn a_reopen_after_an_inheriting_history_re_keys_a_generation_with_nothing_pending() {
    let dir = TempDir::new("driver-reopen-rekey");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let acked = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let generation = bound.handle.generation.dir.clone();
    let before = bound.handle.incarnation();
    w.retire(c, &bound);
    drop(bound);
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    assert_eq!(bound.handle.generation.dir, generation);
    assert_eq!(bound.handle.attach, super::Attach::ReKeyed);
    let token = w.server.token(PATH).unwrap();
    assert_eq!(bound.handle.incarnation(), token);
    let next = w.commit(c2, &bound, 2).unwrap();
    let ticket = w.sync(c2, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c2, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits(), vec![acked, next.clone()]);
    let sent: Vec<_> = w
        .appends(c2)
        .into_iter()
        .filter(|(_, seq, _)| *seq > 0)
        .map(|(_, _, body)| body)
        .collect();
    assert_eq!(sent, vec![next]);
    let tokens = segment_tokens(&generation);
    assert_eq!(tokens.first(), Some(&before));
    assert_eq!(tokens.last(), Some(&token));
}

/// A clean close right after `synced()`, inside the ACK interval: the retire writes an ACK at the
/// exact acknowledged position, so the log holds no commit after its latest ACK. A history event
/// that loses nothing then re-keys the generation at the next open instead of salvaging it.
#[test]
fn a_clean_close_writes_an_exact_ack_so_an_inheriting_history_re_keys() {
    let dir = TempDir::new("driver-exact-ack");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let first = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    // The second commit's ACK is due a second after the first one's: unwritten at the close.
    let second = w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    while w.outcome(c, ticket).is_none() {
        w.run_for(1);
    }
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    let generation = bound.handle.generation.dir.clone();
    let ack_after_commit = |records: &[RecordBody]| {
        let ack = records
            .iter()
            .rposition(|r| matches!(r, RecordBody::Ack { .. }));
        let commit = records
            .iter()
            .rposition(|r| matches!(r, RecordBody::Commit { .. }));
        ack > commit
    };
    assert!(!ack_after_commit(&log_records(&generation)));
    w.retire(c, &bound);
    drop(bound);
    let records = log_records(&generation);
    assert!(ack_after_commit(&records), "{records:?}");
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    assert_eq!(bound.handle.generation.dir, generation);
    assert_eq!(bound.handle.attach, super::Attach::ReKeyed);
    assert!(salvages(&generation).is_empty());
    assert_eq!(w.stream_commits(), vec![first, second]);
}

/// A crash leaves a commit pending; the stream then changes history and loses nothing. The log's
/// ACK may trail what the stream acknowledged, so the reopen never re-keys: it salvages the
/// generation, whose db and log hold the pending commit, and builds from the new history. No
/// commit is lost: each is in the stream or in the salvage.
#[test]
fn a_reopen_after_an_inheriting_history_salvages_while_commits_are_pending() {
    let dir = TempDir::new("driver-reopen-rekey-pending");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let c1 = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.server.inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropRequest,
        times: 1,
    });
    let c2 = w.commit(c, &bound, 2).unwrap();
    let generation = bound.handle.generation.dir.clone();
    let before = bound.handle.incarnation();
    drop(bound);
    w.crash(c);
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c3 = w.core();
    let bound = w.open(c3, spec(dir.path(), 2));
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::HistoryChanged));
    let s = only_salvage(&generation, Reason::HistoryChanged);
    assert_eq!(s.manifest.incarnation, Some(before));
    assert!(
        s.manifest
            .acknowledged
            .is_some_and(|a| a.unacknowledged >= 1)
    );
    assert_eq!(w.stream_commits(), vec![c1]);
    assert!(holds_commit(&s, &c2));
    assert_eq!(salvaged_db(&s), sqlite(Box::new([2; 4096])).to_vec());
    let next = w.commit(c3, &bound, 3).unwrap();
    let ticket = w.sync(c3, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c3, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits().last(), Some(&next));
}

/// A crash leaves a commit pending, and a history event that loses nothing lands right after the
/// reopen's HEAD: the takeover's claim meets the new token with a commit pending, so the open fails
/// `rewound` before the log rolls (its ACK fsynced, D5), and the next open salvages the generation
/// with the pending commit in it.
#[test]
fn a_takeover_meeting_an_inheriting_history_with_commits_pending_fails_before_the_roll() {
    let dir = TempDir::new("driver-takeover-rekey-pending");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.server.inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropRequest,
        times: 1,
    });
    let c2 = w.commit(c, &bound, 2).unwrap();
    let generation = bound.handle.generation.dir.clone();
    let before = bound.handle.incarnation();
    drop(bound);
    w.crash(c);
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        server.history_event(HistoryEvent::NewHistory).unwrap();
    }));
    let core = w.core();
    let e = kind(w.open_result(core, spec(dir.path(), 2)));
    assert!(matches!(&*e, OpenError::Reset(ResetKind::Rewound)), "{e}");
    assert_eq!(segment_tokens(&generation), vec![before]);
    let c3 = w.core();
    let bound = w.open(c3, spec(dir.path(), 3));
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::HistoryChanged));
    let s = only_salvage(&generation, Reason::HistoryChanged);
    assert_eq!(segment_tokens(&s.dir), vec![before]);
    assert!(holds_commit(&s, &c2));
}

/// A commit acknowledged inside the ACK lag, a crash, a restore to before it that still inherits
/// by the lagging ACK, and a foreign commit in the new history. The acknowledged commit is pending
/// in the log, so the reopen never re-keys (a re-keyed log names the new token, and a later open
/// would resume it and discard the commit): it salvages the generation, whose db holds the commit,
/// and builds from the new history. A later reopen resumes the new generation.
#[test]
fn an_acknowledged_commit_behind_a_lagging_ack_is_salvaged() {
    let dir = TempDir::new("driver-rekey-lagging-ack");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    let c1 = w.commit(c, &bound, 1).unwrap();
    // Acknowledged by the stream, within the ACK interval: no ACK record covers it yet.
    w.run_for(200);
    assert!(w.stream_commits().contains(&c1));
    let generation = bound.handle.generation.dir.clone();
    let before = bound.handle.incarnation();
    drop(bound);
    w.crash(c);
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    foreign_commit(&mut w.server, 7);
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::HistoryChanged));
    let s = only_salvage(&generation, Reason::HistoryChanged);
    assert_eq!(s.manifest.incarnation, Some(before));
    assert_eq!(segment_tokens(&s.dir), vec![before]);
    // The commit the restore lost is in the salvage, its db plain.
    assert!(!w.stream_commits().contains(&c1));
    assert!(holds_commit(&s, &c1));
    assert_eq!(salvaged_db(&s), sqlite(Box::new([1; 4096])).to_vec());
    w.retire(c2, &bound);
    drop(bound);
    let c3 = w.core();
    let again = w.open(c3, spec(dir.path(), 3));
    assert_eq!(again.handle.attach, super::Attach::Resumed);
    assert_eq!(salvages(&generation).len(), 1);
}

/// A history that lost a commit the generation acknowledged is salvaged at open, with the commit
/// in it, and the open builds from the new history and takes over.
#[test]
fn a_reopen_after_a_lossy_history_is_salvaged() {
    let dir = TempDir::new("driver-reopen-lossy");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    let c1 = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(c, &bound);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::HistoryChanged));
    let s = only_salvage(&generation, Reason::HistoryChanged);
    assert_eq!(s.manifest.acknowledged.map(|a| a.unacknowledged), Some(0));
    assert!(holds_commit(&s, &c1));
    assert!(!w.stream_commits().contains(&c1));
    assert_eq!(w.claims(c2).len(), 1);
}

#[test]
fn a_missing_stream_with_a_local_generation_holding_data_is_not_found() {
    let (result, generation, _dir) = reopen_after(|w, _| w.server.delete(PATH).unwrap());
    let e = kind(result);
    assert!(matches!(*e, OpenError::NotFound), "{e}");
    assert!(generation.exists());
    assert!(salvages(&generation).is_empty());
}

/// With `recreate`, a missing stream is created again over a generation holding data: the
/// generation is salvaged (`recreated`), and the open builds the new stream and takes it over,
/// its epochs starting again.
#[test]
fn recreate_salvages_the_generation_and_creates_the_stream() {
    let dir = TempDir::new("driver-recreate");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let c1 = w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(c, &bound);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.server.delete(PATH).unwrap();
    let c2 = w.core();
    let bound = w.open(c2, OpenSpec {
        access: Access::Owner { recreate: true },
        ..spec(dir.path(), 2)
    });
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::Recreated));
    let s = only_salvage(&generation, Reason::Recreated);
    assert!(holds_commit(&s, &c1));
    assert_eq!(
        s.manifest.stream_incarnation,
        Some(w.server.token(PATH).unwrap())
    );
    assert_eq!(w.claims(c2), vec![Epoch::try_from(1).unwrap()]);
    assert!(w.stream_commits().is_empty());
}

/// A missing stream over a generation that holds no data (no commit, page 1 alone) is created
/// again with no salvage: nothing would be lost.
#[test]
fn a_missing_stream_over_a_generation_holding_no_data_is_created() {
    let dir = TempDir::new("driver-recreate-empty");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.run_for(1_000);
    w.retire(c, &bound);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.server.delete(PATH).unwrap();
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    assert_eq!(bound.handle.attach, super::Attach::Built {
        snapshot: None,
        salvaged: None
    });
    assert!(!generation.exists());
    assert!(salvages(&generation).is_empty());
}

/// A log whose first segment header does not read is salvaged as `replaced`, its files as they
/// were, and the open builds from the stream.
#[test]
fn an_unreadable_segment_header_is_salvaged_as_replaced() {
    let (result, generation, _dir) = reopen_after(|_, generation| {
        let log = generation.join("log");
        let first = std::fs::read_dir(&log)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut bytes = std::fs::read(&first).unwrap();
        bytes[..4].copy_from_slice(b"XXXX");
        std::fs::write(&first, bytes).unwrap();
    });
    let bound = result.unwrap();
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::Replaced));
    let s = only_salvage(&generation, Reason::Replaced);
    assert_eq!(s.manifest.lineage, None);
    assert_eq!(s.manifest.acknowledged, None);
    assert!(!s.manifest.plain);
}

// A db file that is not the generation's (plain SQLite copied over it) is salvaged as `replaced`
// before anything is written to it: the file and the log as they were found, and the open builds
// from the stream.
#[test]
fn a_replaced_db_file_is_salvaged_as_replaced() {
    let page = sqlite(Box::new([0; 4096]));
    let (result, generation, _dir) = reopen_after(|_, generation| {
        std::fs::write(generation.join("db"), &page[..]).unwrap();
    });
    assert!(!generation.exists(), "the generation moved");
    let bound = result.unwrap();
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::Replaced));
    let s = only_salvage(&generation, Reason::Replaced);
    assert!(matches!(s.manifest.reason, Reason::Replaced));
    assert!(s.manifest.plain);
    assert!(s.manifest.lineage.is_some());
    assert_eq!(salvaged_db(&s), page.to_vec());
    assert!(
        log_records(&s.dir)
            .iter()
            .any(|r| matches!(r, RecordBody::Commit { .. })),
        "the log is kept"
    );
}

#[test]
fn a_reopen_of_a_retired_binding_takes_over_its_trusted_generation() {
    let (result, generation, _dir) = reopen_after(|_, _| {});
    let bound = result.unwrap();
    assert_eq!(bound.handle.generation.dir, generation);
}

#[test]
fn a_root_locked_elsewhere_is_in_use() {
    let dir = TempDir::new("driver-in-use");
    let _held = Root::lock(dir.path(), &canonical_path(URL).unwrap()).unwrap();
    let mut w = World::new();
    let c = w.core();
    let e = kind(w.open_result(c, spec(dir.path(), 1)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
}

#[test]
fn an_unreachable_stream_fails_the_open_at_its_deadline() {
    let dir = TempDir::new("driver-unreachable");
    let mut w = World::new();
    let c = w.core();
    w.down[c] = true;
    let e = kind(w.open_result(c, OpenSpec {
        timeout_ms: 10_000,
        ..spec(dir.path(), 1)
    }));
    assert!(
        matches!(*e, OpenError::Unavailable(Unavailable::Unreachable)),
        "{e}"
    );
    assert_eq!(e.error_kind().as_str(), "unreachable");
}

#[test]
fn the_event_log_coalesces_acks_and_reports_what_it_dropped() {
    let mut log = EventLog::default();
    for n in 1..=3 {
        log.push(DriverEvent::Acked {
            binding: BindingId(1),
            acked: Position(n),
        });
    }
    let (events, cursor) = log.since(0);
    assert!(matches!(events.as_slice(), [DriverEvent::Acked {
        acked: Position(3),
        ..
    }]));
    for n in 0..super::EVENTS as u64 + 10 {
        log.push(DriverEvent::SyncFailed {
            ticket: Ticket(n),
            error: SyncError::NotBound,
        });
    }
    let (events, _) = log.since(cursor);
    assert!(matches!(events[0], DriverEvent::Gap { dropped } if dropped > 0));
    assert_eq!(events.len(), super::EVENTS + 1);
}

#[test]
fn the_runtime_opens_replicates_and_waits_for_synced() {
    let dir = TempDir::new("driver-runtime");
    let server = Arc::new(Mutex::new(server()));
    let model = Arc::clone(&server);
    let transports: Transports = Arc::new(move |_url: &str| {
        Arc::new(ModelTransport::new(Arc::clone(&model), PATH)) as Arc<dyn Transport>
    });
    let driver = Driver::start(transports, [5; 32]);
    let open = driver.open(spec(dir.path(), 1)).unwrap();
    let bound = driver
        .wait_open(open, Duration::from_secs(10))
        .expect("the open resolved")
        .unwrap();
    let handle = &bound.handle;
    let mut log = handle.log().unwrap();
    let position = log.next_position();
    let id = CommitId {
        lineage: handle.generation.lineage,
        position,
    };
    let frame = frame::encode_commit(id, &CommitRecord {
        db_pages_after: 1,
        pages: vec![(1, sqlite(Box::new([7; 4096])))],
    })
    .unwrap();
    log.append(&RecordBody::Commit {
        frame: frame.clone(),
    })
    .unwrap();
    drop(log);
    driver
        .logged(bound.binding, Pending {
            id,
            frame: Arc::from(frame.clone()),
        })
        .unwrap();
    let ticket = driver.sync(bound.binding, Some(position), true).unwrap();
    assert_eq!(driver.wait(ticket, Duration::from_secs(10)), Some(Ok(())));
    let (events, cursor) = driver.events(0, Duration::from_secs(1));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, DriverEvent::Acked { .. }))
    );
    // Its last connection closed.
    bound.handle.retiring();
    driver.retire(bound.binding).unwrap();
    let mut cursor = cursor;
    let mut retired = false;
    for _ in 0..100 {
        let (events, next) = driver.events(cursor, Duration::from_millis(100));
        cursor = next;
        if events
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { .. }))
        {
            retired = true;
            break;
        }
    }
    assert!(retired);
    let stream = server.lock().unwrap().frames(PATH).unwrap().to_vec();
    assert!(frames(&stream).contains(&frame));
    // A cancelled open that already resolved changes nothing.
    driver.cancel(open).unwrap();
    driver.boundary(bound.binding, position).unwrap();
}

#[test]
fn a_sync_ahead_of_its_logged_waits_for_it() {
    let dir = TempDir::new("driver-sync-ahead");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    // The commit is in the log, and the call comes before its `Logged` reaches the driver.
    let pending = World::append(&bound, Box::new([1; 4096])).unwrap();
    let frame = pending.frame.to_vec();
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), None);
    w.logged(c, &bound, pending);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits(), vec![frame]);
}

#[test]
fn a_reopen_while_old_handles_live_builds_from_a_copy() {
    // A binding that retired drained with no connection left: its root unlocks at once, though
    // the registry may still hold the handle.
    let dir = TempDir::new("driver-old-handles");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(c, &bound);
    assert!(
        w.events(c)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding } if *binding == bound.binding))
    );
    let c2 = w.core();
    let reopened = w.open(c2, spec(dir.path(), 2));
    assert_eq!(reopened.handle.pending_bytes(), 0);
    drop(bound);
    // A commit stays pending while the stream is down, then a reset finds the binding with a
    // connection open, which may still read its generation.
    w.down[c2] = true;
    let pending = w.commit(c2, &reopened, 2).unwrap();
    w.run_for(1_000);
    let now = w.now;
    w.c(c2)
        .reset_binding(reopened.binding, ResetKind::LocalDamage, now);
    assert_eq!(reopened.handle.reset(), Some(ResetKind::LocalDamage));
    assert!(reopened.handle.holds_root());
    let old = reopened.handle.generation.clone();
    w.down[c2] = false;
    // Another process is refused while the connection holds the root.
    let c3 = w.core();
    let e = kind(w.open_result(c3, spec(dir.path(), 3)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    // This process opens again beside it, from a copy of its generation: same lineage, another
    // directory, the pending commit carried and sent.
    let copy = w.open(c2, spec(dir.path(), 2));
    assert_eq!(copy.handle.generation.lineage, old.lineage);
    assert_ne!(copy.handle.generation.id, old.id);
    assert!(!old.dir.exists(), "the original is trashed");
    let ticket = w.sync(c2, copy.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c2, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits().last(), Some(&pending));
    assert_eq!(
        copy.handle.last_commit(),
        reopened.handle.last_commit(),
        "the copy's last commit is the one carried"
    );
    // The old connection closes: its root and hold go, and the new binding still holds the root.
    assert!(!reopened.handle.disconnect(Holder::Host));
    assert!(!reopened.handle.holds_root());
    let e = kind(w.open_result(c3, spec(dir.path(), 3)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    w.retire(c2, &copy);
    let bound = w.open(c3, spec(dir.path(), 4));
    assert_eq!(bound.handle.pending_bytes(), 0);
    assert_eq!(bound.handle.generation.id, copy.handle.generation.id);
}

// A binding that drains after its last connection closed still drives its replicator: an open of
// the stream in the same core never shares its root (no copy beside it), and is refused `in_use`.
#[test]
fn a_reopen_beside_a_draining_binding_is_in_use() {
    let dir = TempDir::new("driver-draining");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.down[c] = true;
    w.commit(c, &bound, 1).unwrap();
    w.retire(c, &bound);
    assert_eq!(bound.handle.life(), Life::Retiring);
    assert!(bound.handle.holds_root());
    let e = kind(w.open_result(c, spec(dir.path(), 1)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    assert_eq!(bound.handle.reset(), None);
}

// The roots a core shares are keyed by directory: a reopen beside a reset binding's connections
// that names another cache root binds there, from the stream.
#[test]
fn a_reopen_in_another_dir_binds_there() {
    let (dir_a, dir_b) = (TempDir::new("driver-dir-a"), TempDir::new("driver-dir-b"));
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir_a.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let now = w.now;
    w.c(c)
        .reset_binding(bound.binding, ResetKind::LocalDamage, now);
    let other = w.open(c, spec(dir_b.path(), 1));
    assert!(other.handle.generation.dir.starts_with(dir_b.path()));
    assert!(
        bound.handle.generation.dir.exists(),
        "the old one is not copied"
    );
}

// A reset its connections marked, which the core has not heard of yet, is carried out before an
// open of the stream copies the generation: the open never runs beside a binding the core still
// drives.
#[test]
fn a_reopen_carries_out_a_reset_the_core_has_not_heard_of() {
    let dir = TempDir::new("driver-reset-unheard");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    // The commit path marks the reset, and its message to the driver is still on its way.
    assert!(bound.handle.mark_reset(ResetKind::LocalDamage));
    let copy = w.open(c, spec(dir.path(), 1));
    assert!(
        w.events(c).iter().any(
            |e| matches!(e, DriverEvent::Reset { binding, cause: ResetKind::LocalDamage } if *binding == bound.binding)
        )
    );
    assert_ne!(copy.handle.generation.id, bound.handle.generation.id);
    assert_eq!(
        copy.handle.generation.lineage,
        bound.handle.generation.lineage
    );
    // The message arrives: the binding is gone, nothing happens.
    let now = w.now;
    w.c(c)
        .reset_binding(bound.binding, ResetKind::LocalDamage, now);
    assert_eq!(copy.handle.reset(), None);
}

// A connection or an open that takes a retiring binding back wins over the retire its last close
// asked for: the binding stays live and keeps its root, so no other process opens the cache.
#[test]
fn a_binding_taken_back_before_its_retire_stays_live() {
    let dir = TempDir::new("driver-revive");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let now = w.now;
    // Closed, then handed out again by an open before the retire arrives.
    assert!(bound.handle.disconnect(Holder::Host));
    assert_eq!(bound.handle.life(), Life::Retiring);
    assert!(bound.handle.revive());
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 1 });
    assert!(bound.handle.holds_root());
    // Closed, then opened again by a connection before the retire arrives: the same.
    assert!(bound.handle.connect(Holder::Host).is_some());
    assert!(bound.handle.disconnect(Holder::Host), "the last connection");
    assert!(
        bound.handle.connect(Holder::Host).is_some(),
        "a retiring binding is taken back"
    );
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    assert_eq!(
        (bound.handle.life(), bound.handle.connections()),
        (Life::Live, 1)
    );
    assert!(bound.handle.holds_root());
    let c2 = w.core();
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    // Its last connection closes: it drains, retires and unlocks the root.
    w.retire(c, &bound);
    assert_eq!(bound.handle.life(), Life::Retired);
    assert!(!bound.handle.holds_root());
    assert!(
        !bound.handle.connect(Holder::Host).is_some(),
        "a retired binding opens no connection"
    );
}

// A bound result that no connection opens retires after `UNCONNECTED_MS`, unless an open hands it
// out again meanwhile (which starts its own wait) or a connection opens it.
#[test]
fn a_binding_no_connection_opens_retires() {
    let dir = TempDir::new("driver-unconnected");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open_result(c, spec(dir.path(), 1)).unwrap();
    w.run_for(UNCONNECTED_MS / 2);
    // Handed out again halfway: the first wait no longer retires it.
    assert!(bound.handle.revive());
    let now = w.now;
    w.c(c).unconnected(bound.binding, now);
    w.run_for(UNCONNECTED_MS / 2 + 1_000);
    assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 2 });
    w.run_for(UNCONNECTED_MS / 2);
    assert_eq!(bound.handle.life(), Life::Retired);
    assert!(!bound.handle.holds_root());
    // One a connection opened stays.
    let c2 = w.core();
    let other = w.open(c2, spec(dir.path(), 2));
    w.run_for(2 * UNCONNECTED_MS);
    assert_eq!(other.handle.life(), Life::Live);
}

/// The driver hears that the binding's last connection closed, and runs a while.
fn retire_asked(w: &mut World, c: usize, bound: &Bound) {
    let now = w.now;
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
}

// The host closes its last connection while a snapshot attempt holds the private one, and
// `ursula_open` hands the binding out again before the attempt ends. The private connection's close
// must not retire it: the path handed out stays openable until a connection opens it, or until it
// waited `UNCONNECTED_MS` for one.
#[test]
fn a_hand_out_outlives_the_close_of_the_private_connection() {
    for connects in [true, false] {
        let dir = TempDir::new("driver-handout-private");
        let mut w = World::new();
        let c = w.core();
        let bound = w.open(c, spec(dir.path(), 1));
        assert!(bound.handle.connect(Holder::Private).is_some());
        // The host's close: retiring, though the private connection keeps it from retiring.
        assert!(bound.handle.disconnect(Holder::Host));
        retire_asked(&mut w, c, &bound);
        assert_eq!(bound.handle.life(), Life::Retiring);
        // `ursula_open` hands it out again.
        assert!(bound.handle.revive());
        let now = w.now;
        w.c(c).unconnected(bound.binding, now);
        // The attempt ends.
        assert!(!bound.handle.disconnect(Holder::Private));
        retire_asked(&mut w, c, &bound);
        assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 1 });
        assert!(bound.handle.holds_root());
        if connects {
            assert!(
                bound.handle.connect(Holder::Host).is_some(),
                "the path handed out opens"
            );
            w.run_for(2 * UNCONNECTED_MS);
            assert_eq!(bound.handle.life(), Life::Live);
            w.retire(c, &bound);
        } else {
            w.run_for(UNCONNECTED_MS + 1_000);
        }
        assert_eq!(bound.handle.life(), Life::Retired);
        assert!(!bound.handle.holds_root());
    }
}

// A hand-out beside a connection of the host still open: that connection's close leaves the
// binding to the hand-out's wait, after which the connections left keep it live.
#[test]
fn a_hand_out_beside_an_open_connection_waits_for_its_own() {
    let dir = TempDir::new("driver-handout-beside");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    assert!(bound.handle.connect(Holder::Host).is_some());
    assert!(bound.handle.revive());
    let now = w.now;
    w.c(c).unconnected(bound.binding, now);
    assert!(!bound.handle.disconnect(Holder::Host));
    w.run_for(UNCONNECTED_MS + 1_000);
    assert_eq!(
        (bound.handle.life(), bound.handle.connections()),
        (Life::Live, 1)
    );
    w.retire(c, &bound);
    assert_eq!(bound.handle.life(), Life::Retired);
}

// An open under another cache root gives a binding up only when nothing of the host holds it: no
// connection of the host, no hand-out waiting for one, no commit unacknowledged. Given up, nothing
// takes it back, and it retires once its private connection closed.
#[test]
fn a_binding_is_given_up_only_when_the_host_holds_none_of_it() {
    let dir = TempDir::new("driver-give-up");
    let mut w = World::new();
    let c = w.core();
    // Handed out, no connection yet.
    let bound = w.open_result(c, spec(dir.path(), 1)).unwrap();
    assert_eq!(bound.handle.give_up(), Err(Kept::HandedOut));
    // A connection of the host open, and a private one.
    assert!(bound.handle.connect(Holder::Host).is_some());
    assert!(bound.handle.connect(Holder::Private).is_some());
    assert_eq!(bound.handle.give_up(), Err(Kept::Connections(1)));
    // A commit not acknowledged yet.
    let pending = World::append(&bound, Box::new([1; 4096])).unwrap();
    assert!(bound.handle.disconnect(Holder::Host));
    let bytes = bound.handle.pending_bytes();
    assert_eq!(bound.handle.give_up(), Err(Kept::Pending { bytes }));
    w.logged(c, &bound, pending);
    w.run_for(1_000);
    assert_eq!(bound.handle.pending_bytes(), 0);
    // Held by the private connection only: given up, and nothing takes it back.
    assert_eq!(bound.handle.give_up(), Ok(()));
    assert_eq!(bound.handle.life(), Life::GivenUp);
    assert!(!bound.handle.revive());
    assert!(bound.handle.connect(Holder::Host).is_none());
    assert!(bound.handle.connect(Holder::Private).is_none());
    retire_asked(&mut w, c, &bound);
    assert_eq!(bound.handle.life(), Life::GivenUp);
    assert!(bound.handle.disconnect(Holder::Private));
    retire_asked(&mut w, c, &bound);
    assert_eq!(bound.handle.life(), Life::Retired);
    assert!(!bound.handle.holds_root());
    assert_eq!(
        bound.handle.give_up(),
        Ok(()),
        "a retired binding is no hold"
    );
}

/// Starts an open, leaving it to resolve.
fn start_open(w: &mut World, c: usize, spec: OpenSpec) -> OpenId {
    let now = w.now;
    let open = OpenId(w.id());
    let ios = w.c(c).open(open, spec, now);
    w.queue_ios(c, ios);
    open
}

// The open a binding was given up for waits, without blocking anyone, until the binding's
// private connection closed and it retired, then binds under its own cache root.
#[test]
fn an_open_after_a_given_up_binding_starts_once_it_retired() {
    let first = TempDir::new("driver-after-first");
    let second = TempDir::new("driver-after-second");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(first.path(), 1));
    assert!(bound.handle.connect(Holder::Private).is_some());
    assert!(bound.handle.disconnect(Holder::Host));
    assert_eq!(bound.handle.give_up(), Ok(()));
    let open = start_open(&mut w, c, spec(second.path(), 1));
    w.run_for(5_000);
    assert!(
        w.c(c).take_opened(open).is_none(),
        "it waits for the binding"
    );
    assert!(bound.handle.disconnect(Holder::Private));
    retire_asked(&mut w, c, &bound);
    assert_eq!(bound.handle.life(), Life::Retired);
    let mut opened = None;
    for _ in 0..100 {
        opened = w.c(c).take_opened(open);
        if opened.is_some() {
            break;
        }
        w.run_for(100);
    }
    let again = opened.expect("the open never resolved").unwrap();
    assert!(again.handle.generation.dir.starts_with(second.path()));
    assert_eq!(again.handle.dir, second.path());
    assert_eq!(again.handle.life(), Life::HandedOut { awaiting: 1 });
}

// One whose binding does not retire before its deadline fails `in_use`, naming both roots, and a
// cancelled one fails `cancelled`; the binding stays given up either way.
#[test]
fn an_open_after_a_given_up_binding_ends_at_its_deadline_or_cancel() {
    let first = TempDir::new("driver-after-late-first");
    let second = TempDir::new("driver-after-late-second");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(first.path(), 1));
    assert!(bound.handle.connect(Holder::Private).is_some());
    assert!(bound.handle.disconnect(Holder::Host));
    assert_eq!(bound.handle.give_up(), Ok(()));
    let late = OpenSpec {
        timeout_ms: 2_000,
        ..spec(second.path(), 1)
    };
    let open = start_open(&mut w, c, late);
    w.run_for(3_000);
    let e = kind(w.c(c).take_opened(open).expect("resolved at its deadline"));
    match &*e {
        OpenError::BoundElsewhere {
            bound: b,
            requested,
            kept: Kept::Draining,
        } => {
            assert_eq!(b, first.path());
            assert_eq!(requested, second.path());
        }
        other => panic!("{other}"),
    }
    let open = start_open(&mut w, c, spec(second.path(), 1));
    let now = w.now;
    let ios = w.c(c).cancel(open, now);
    w.queue_ios(c, ios);
    let e = kind(w.c(c).take_opened(open).expect("resolved at its cancel"));
    assert!(matches!(*e, OpenError::Cancelled), "{e}");
    assert_eq!(bound.handle.life(), Life::GivenUp);
    // A retry, under any root, waits for the binding too: never two bindings of the stream.
    let third = TempDir::new("driver-after-late-third");
    let retry = start_open(&mut w, c, spec(third.path(), 1));
    w.run_for(5_000);
    assert!(w.c(c).take_opened(retry).is_none(), "the retry waits");
    assert!(bound.handle.disconnect(Holder::Private));
    retire_asked(&mut w, c, &bound);
    let mut opened = None;
    for _ in 0..100 {
        opened = w.c(c).take_opened(retry);
        if opened.is_some() {
            break;
        }
        w.run_for(100);
    }
    let again = opened.expect("the retry never resolved").unwrap();
    assert_eq!(bound.handle.life(), Life::Retired);
    assert_eq!(again.handle.dir, third.path());
}

// Two hand-outs before any connection: the first caller's connection opening and closing leaves
// the second's path openable.
#[test]
fn every_hand_out_waits_for_its_own_connection() {
    let dir = TempDir::new("driver-two-handouts");
    let mut w = World::new();
    let c = w.core();
    // A's hand-out (the open's result), then B's.
    let bound = w.open_result(c, spec(dir.path(), 1)).unwrap();
    assert!(bound.handle.revive());
    let now = w.now;
    w.c(c).unconnected(bound.binding, now);
    assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 2 });
    // A connects and closes.
    assert!(bound.handle.connect(Holder::Host).is_some());
    assert!(!bound.handle.disconnect(Holder::Host));
    retire_asked(&mut w, c, &bound);
    assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 1 });
    // B connects.
    assert!(
        bound.handle.connect(Holder::Host).is_some(),
        "B's path opens"
    );
    assert_eq!(bound.handle.life(), Life::Live);
    w.retire(c, &bound);
    assert_eq!(bound.handle.life(), Life::Retired);
    // Hand-outs never taken up lapse with the wait.
    let other = w.open_result(c, spec(dir.path(), 2)).unwrap();
    assert!(other.handle.revive());
    let now = w.now;
    w.c(c).unconnected(other.binding, now);
    w.run_for(UNCONNECTED_MS + 1_000);
    assert_eq!(other.handle.life(), Life::Retired);
}

// An open of a main db handle that fails after its connection counted undoes it exactly: a
// hand-out it took up waits again, and a binding it took back from retiring retires again.
#[test]
fn a_failed_open_undoes_its_connection() {
    let dir = TempDir::new("driver-unconnect");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open_result(c, spec(dir.path(), 1)).unwrap();
    let connected = bound.handle.connect(Holder::Host).unwrap();
    assert_eq!(bound.handle.unconnect(connected), Undone::WaitsAgain);
    assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 1 });
    retire_asked(&mut w, c, &bound);
    assert!(
        bound.handle.connect(Holder::Host).is_some(),
        "the hand-out still opens"
    );
    // Closed, retiring, then a connection whose open fails: retiring again, and it retires.
    assert!(bound.handle.disconnect(Holder::Host));
    let connected = bound.handle.connect(Holder::Host).unwrap();
    assert_eq!(bound.handle.life(), Life::Live);
    assert_eq!(bound.handle.unconnect(connected), Undone::Retires);
    assert_eq!(bound.handle.life(), Life::Retiring);
    retire_asked(&mut w, c, &bound);
    assert_eq!(bound.handle.life(), Life::Retired);
}

// The runtime: an open cancelled after its result arrived, untaken, retires and unlocks its root,
// and so does one whose `wait_open` timed out as the result arrived, whichever came first.
#[test]
fn a_cancelled_or_timed_out_open_whose_result_arrived_retires() {
    let server = Arc::new(Mutex::new(server()));
    let model = Arc::clone(&server);
    let transports: Transports = Arc::new(move |_url: &str| {
        Arc::new(ModelTransport::new(Arc::clone(&model), PATH)) as Arc<dyn Transport>
    });
    let driver = Driver::start(transports, [8; 32]);
    let dir = TempDir::new("driver-runtime-untaken");
    // `None`: cancelled after the Opened event. `Some(ms)`: a `wait_open` of `ms` racing it.
    for wait in [None, Some(0), Some(1), Some(3), Some(10), Some(30)] {
        let open = driver.open(spec(dir.path(), 1)).unwrap();
        let taken = match wait {
            None => {
                let mut cursor = driver.events(0, Duration::ZERO).1.saturating_sub(1);
                let mut opened = false;
                for _ in 0..200 {
                    let (events, next) = driver.events(cursor, Duration::from_millis(50));
                    cursor = next;
                    opened |= events.iter().any(
                        |e| matches!(e, DriverEvent::Opened { open: o, result: Ok(_) } if *o == open),
                    );
                    if opened {
                        break;
                    }
                }
                assert!(opened);
                driver.cancel(open).unwrap();
                None
            }
            Some(ms) => driver.wait_open(open, Duration::from_millis(ms)),
        };
        // A result the wait took is the caller's: it closes its (only) connection.
        if let Some(Ok(bound)) = taken {
            assert!(bound.handle.connect(Holder::Host).is_some());
            assert!(bound.handle.disconnect(Holder::Host));
            driver.retire(bound.binding).unwrap();
        }
        // Whatever happened, the root comes free for the next open.
        let mut next = None;
        for _ in 0..100 {
            let again = driver.open(spec(dir.path(), 2)).unwrap();
            match driver.wait_open(again, Duration::from_secs(10)) {
                Some(Ok(bound)) => {
                    next = Some(bound);
                    break;
                }
                Some(Err(e)) if matches!(*e, OpenError::InUse { .. }) => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                other => panic!("{wait:?}: {other:?}"),
            }
        }
        let bound = next.unwrap_or_else(|| panic!("{wait:?}: the root stayed locked"));
        assert!(bound.handle.connect(Holder::Host).is_some());
        assert!(bound.handle.disconnect(Holder::Host));
        driver.retire(bound.binding).unwrap();
        drop(bound);
        // Wait for that one to unlock too before the next round.
        for _ in 0..100 {
            if bound_is_free(&driver, dir.path()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Whether an open of the stream in `dir` binds now (and then retires it again).
fn bound_is_free(driver: &Driver, dir: &Path) -> bool {
    let open = driver.open(spec(dir, 3)).unwrap();
    match driver.wait_open(open, Duration::from_secs(10)) {
        Some(Ok(bound)) => {
            assert!(bound.handle.connect(Holder::Host).is_some());
            assert!(bound.handle.disconnect(Holder::Host));
            driver.retire(bound.binding).unwrap();
            let handle = bound.handle;
            for _ in 0..100 {
                if !handle.holds_root() {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            false
        }
        _ => false,
    }
}

#[test]
fn a_failed_claim_write_sends_no_claim() {
    let dir = TempDir::new("driver-claim-fails");
    let mut w = World::new();
    let c = w.core();
    w.c(c).fail_next_claim = true;
    let e = kind(w.open_result(c, spec(dir.path(), 1)));
    assert!(
        matches!(*e, OpenError::Reset(ResetKind::LocalDamage)),
        "{e}"
    );
    assert!(w.claims(c).is_empty(), "a claim went out");
}

#[test]
fn a_catch_up_larger_than_the_database_rebuilds() {
    let dir_a = TempDir::new("driver-large-catch-up-a");
    let dir_b = TempDir::new("driver-large-catch-up-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(a, &bound);
    let old = bound.handle.generation.clone();
    drop(bound);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    // Pages that do not compress: the frames are larger than A's one-page database.
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut last = Vec::new();
    for _ in 0..3 {
        let mut page = Box::new([0u8; 4096]);
        for byte in page.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *byte = x as u8;
        }
        last = page.to_vec();
        w.commit_page(b, &bound_b, page).unwrap();
    }
    w.run_for(1_000);
    let a2 = w.core();
    let bound = w.open(a2, spec(dir_a.path(), 3));
    // The frames outgrow A's one-page database: a rebuild rather than a catch-up in place.
    assert_ne!(bound.handle.generation, old);
    assert!(!old.dir.exists());
    assert_eq!(
        std::fs::read(bound.handle.generation.db()).unwrap(),
        on_disk(bound.handle.generation.lineage, last)
    );
    let ticket = w.sync(a2, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(a2, ticket), Some(Ok(())));
}

#[test]
fn synced_on_an_idle_owner_resolves() {
    let dir = TempDir::new("driver-idle-sync");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    // Right after the open: nothing was ever committed.
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    // After a commit and the lazy ACK that follows it, nothing new.
    w.commit(c, &bound, 1).unwrap();
    w.run_for(3_000);
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    // An explicit target past every commit waits for the last one only.
    let ticket = w.sync_to(c, bound.binding, Some(Position(u64::MAX)));
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    // And after a reopen, from the log alone.
    w.retire(c, &bound);
    drop(bound);
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    let ticket = w.sync(c2, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c2, ticket), Some(Ok(())));
}

#[test]
fn an_open_timing_out_during_a_catch_up_keeps_the_root_until_the_job_returns() {
    let dir_a = TempDir::new("driver-timeout-catch-up-a");
    let dir_b = TempDir::new("driver-timeout-catch-up-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(a, &bound);
    let generation = bound.handle.generation.clone();
    drop(bound);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    w.commit(b, &bound_b, 2).unwrap();
    w.run_for(1_000);
    // A's reopen reaches its catch-up in place, whose disk job stalls past the deadline.
    w.hold = Some(|job| matches!(job, DiskJob::CatchUp { .. }));
    let a2 = w.core();
    let open = OpenId(w.id());
    let now = w.now;
    let ios = w.c(a2).open(
        open,
        OpenSpec {
            timeout_ms: 5_000,
            ..spec(dir_a.path(), 3)
        },
        now,
    );
    w.queue_ios(a2, ios);
    w.run_for(1_000);
    assert_eq!(w.held.len(), 1, "the catch-up runs");
    w.run_for(10_000);
    assert!(
        w.c(a2).take_opened(open).is_none(),
        "resolved before its job returned"
    );
    // An immediate reopen finds the root still locked: nothing runs beside the stalled job.
    let a3 = w.core();
    let e = kind(w.open_result(a3, spec(dir_a.path(), 4)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    // In the same core too: it never shares the root of the stalled job's tombstone.
    let e = kind(w.open_result(a2, spec(dir_a.path(), 5)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    // The job returns: the open resolves as timed out, and the root unlocks.
    w.release_held();
    let result = w
        .c(a2)
        .take_opened(open)
        .expect("resolved once its job returned");
    assert!(matches!(*kind(result), OpenError::Unavailable(_)));
    let a4 = w.core();
    let bound = w.open(a4, spec(dir_a.path(), 5));
    assert_eq!(bound.handle.generation, generation);
    let frame = w.commit(a4, &bound, 3).unwrap();
    let ticket = w.sync(a4, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(a4, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits().last(), Some(&frame));
    // The log is whole: it reopens with nothing lost.
    w.retire(a4, &bound);
    drop(bound);
    let records = log_records(&generation.dir);
    assert!(records.contains(&RecordBody::Commit { frame }));
    let a5 = w.core();
    let bound = w.open(a5, spec(dir_a.path(), 6));
    assert_eq!(bound.handle.pending_bytes(), 0);
}

#[test]
fn a_cancelled_open_whose_result_nobody_took_retires_its_binding() {
    let dir = TempDir::new("driver-cancel-untaken");
    let mut w = World::new();
    let c = w.core();
    let open = OpenId(w.id());
    let now = w.now;
    let ios = w.c(c).open(open, spec(dir.path(), 1), now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    let binding = w
        .events(c)
        .iter()
        .find_map(|e| match e {
            DriverEvent::Opened {
                open: o,
                result: Ok(binding),
            } if *o == open => Some(*binding),
            _ => None,
        })
        .expect("the open bound");
    let now = w.now;
    let ios = w.c(c).cancel(open, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    assert!(
        w.events(c)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding: b } if *b == binding))
    );
    assert!(w.c(c).take_opened(open).is_none());
    // The root is free again.
    let c2 = w.core();
    w.open(c2, spec(dir.path(), 2));
}

/// The ways an open fails while a disk job of it is out.
#[derive(Debug, Clone, Copy)]
enum Ending {
    Cancel,
    Deadline,
}

/// Starts an open of a stream another owner wrote, stalls its first disk job `hold` matches,
/// ends it `ending`, and checks that it resolves only once the job returned, with the root locked
/// until then.
fn tombstone_at_open(hold: fn(&DiskJob) -> bool, ending: Ending) {
    let dir_a = TempDir::new("driver-tombstone-a");
    let dir_b = TempDir::new("driver-tombstone-b");
    let mut w = World::new();
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    w.commit(b, &bound_b, 2).unwrap();
    w.run_for(1_000);
    w.hold = Some(hold);
    let a = w.core();
    let open = OpenId(w.id());
    let now = w.now;
    let ios = w.c(a).open(
        open,
        OpenSpec {
            timeout_ms: 5_000,
            ..spec(dir_a.path(), 1)
        },
        now,
    );
    w.queue_ios(a, ios);
    w.run_for(1_000);
    assert_eq!(w.held.len(), 1, "{ending:?}: the job runs");
    let prepared = !matches!(w.held[0].1.job, DiskJob::Prepare { .. });
    match ending {
        Ending::Cancel => {
            let now = w.now;
            let ios = w.c(a).cancel(open, now);
            w.queue_ios(a, ios);
        }
        Ending::Deadline => w.run_for(10_000),
    }
    w.run_for(1_000);
    assert!(
        w.c(a).take_opened(open).is_none(),
        "{ending:?}: resolved early"
    );
    if prepared {
        // The root stays locked while the job may still write, against this process too: an
        // open in the same core never shares the root of a job still out.
        assert!(matches!(
            Root::lock(dir_a.path(), &canonical_path(URL).unwrap()),
            Err(crate::generation::GenerationError::InUse { .. })
        ));
        let e = kind(w.open_result(a, spec(dir_a.path(), 4)));
        assert!(matches!(*e, OpenError::InUse { .. }), "{ending:?}: {e}");
    }
    w.release_held();
    let e = kind(
        w.c(a)
            .take_opened(open)
            .expect("resolved once its job returned"),
    );
    match ending {
        Ending::Cancel => assert!(matches!(*e, OpenError::Cancelled), "{e}"),
        Ending::Deadline => assert!(matches!(*e, OpenError::Unavailable(_)), "{e}"),
    }
    let c = w.core();
    w.open(c, spec(dir_a.path(), 3));
}

#[test]
fn an_open_ending_during_prepare_resolves_once_the_job_returned() {
    for ending in [Ending::Cancel, Ending::Deadline] {
        tombstone_at_open(|job| matches!(job, DiskJob::Prepare { .. }), ending);
    }
}

#[test]
fn an_open_ending_during_a_new_build_resolves_once_the_job_returned() {
    for ending in [Ending::Cancel, Ending::Deadline] {
        tombstone_at_open(|job| matches!(job, DiskJob::NewBuild(_)), ending);
    }
}

#[test]
fn an_open_ending_during_a_build_chunk_resolves_once_the_job_returned() {
    for ending in [Ending::Cancel, Ending::Deadline] {
        tombstone_at_open(|job| matches!(job, DiskJob::Chunk { .. }), ending);
    }
}

#[test]
fn an_open_ending_during_a_publish_resolves_once_the_job_returned() {
    for ending in [Ending::Cancel, Ending::Deadline] {
        tombstone_at_open(|job| matches!(job, DiskJob::Publish(_)), ending);
    }
}

#[test]
fn an_open_timing_out_during_a_rebuild_keeps_the_old_generation_and_the_root() {
    let dir_a = TempDir::new("driver-tombstone-rebuild-a");
    let dir_b = TempDir::new("driver-tombstone-rebuild-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.down[a] = true;
    let pending = w.commit(a, &bound, 2).unwrap();
    let generation = bound.handle.generation.clone();
    drop(bound);
    w.crash(a);
    let b = w.core();
    let bound_b = w.open(b, spec(dir_b.path(), 2));
    w.commit(b, &bound_b, 3).unwrap();
    w.run_for(1_000);
    // A's takeover diverges and rebuilds; the rebuild's first disk job stalls past the deadline.
    w.hold = Some(|job| matches!(job, DiskJob::NewBuild(_)));
    let a2 = w.core();
    let open = OpenId(w.id());
    let now = w.now;
    let ios = w.c(a2).open(
        open,
        OpenSpec {
            timeout_ms: 5_000,
            ..spec(dir_a.path(), 3)
        },
        now,
    );
    w.queue_ios(a2, ios);
    w.run_for(1_000);
    assert_eq!(w.held.len(), 1, "the rebuild runs");
    w.run_for(10_000);
    assert!(w.c(a2).take_opened(open).is_none());
    assert!(matches!(
        Root::lock(dir_a.path(), &canonical_path(URL).unwrap()),
        Err(crate::generation::GenerationError::InUse { .. })
    ));
    let e = kind(w.open_result(a2, spec(dir_a.path(), 5)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    w.release_held();
    let e = kind(
        w.c(a2)
            .take_opened(open)
            .expect("resolved once its job returned"),
    );
    assert!(matches!(*e, OpenError::Unavailable(_)), "{e}");
    // Nothing was published: the old generation, its pending commit included, is as it was.
    assert!(log_records(&generation.dir).contains(&RecordBody::Commit { frame: pending }));
    let a3 = w.core();
    let bound = w.open(a3, spec(dir_a.path(), 4));
    assert_ne!(
        bound.handle.generation.lineage, generation.lineage,
        "rebuilt now"
    );
}

#[test]
fn a_runtime_cancel_racing_the_result_retires_the_binding() {
    let dir = TempDir::new("driver-runtime-cancel");
    let server = Arc::new(Mutex::new(server()));
    let model = Arc::clone(&server);
    let transports: Transports = Arc::new(move |_url: &str| {
        Arc::new(ModelTransport::new(Arc::clone(&model), PATH)) as Arc<dyn Transport>
    });
    let driver = Driver::start(transports, [6; 32]);
    // Cancelled at once: the result arrives after, and nobody takes it.
    let open = driver.open(spec(dir.path(), 1)).unwrap();
    driver.cancel(open).unwrap();
    let mut cursor = 0;
    let mut opened = None;
    let mut retired = false;
    for _ in 0..200 {
        let (events, next) = driver.events(cursor, Duration::from_millis(50));
        cursor = next;
        for event in events {
            match event {
                DriverEvent::Opened { open: o, result } if o == open => opened = Some(result),
                DriverEvent::Retired { binding } => {
                    retired |= opened
                        .as_ref()
                        .is_some_and(|r| r.as_ref().is_ok_and(|b| *b == binding));
                }
                _ => {}
            }
        }
        if retired || matches!(opened, Some(Err(_))) {
            break;
        }
    }
    let opened = opened.expect("the open resolved");
    assert!(opened.is_err() || retired, "{opened:?}");
    assert!(driver.wait_open(open, Duration::ZERO).is_none());
    // The root is free: a new open binds, and a cancel after its result was taken leaves it bound.
    let open = driver.open(spec(dir.path(), 2)).unwrap();
    let bound = driver
        .wait_open(open, Duration::from_secs(10))
        .expect("the open resolved")
        .unwrap();
    driver.cancel(open).unwrap();
    let ticket = driver.sync(bound.binding, None, true).unwrap();
    assert_eq!(driver.wait(ticket, Duration::from_secs(10)), Some(Ok(())));
}

/// A closed owner's generation, a checkpoint of the stream, then another writer's commit of page
/// 2 that a catch-up in place logged with its ACK lost to a crash: the db holds that page past
/// the log's acknowledged offset. Returns the generation, the checkpoint and the frame.
fn caught_up_after_the_ack(
    w: &mut World,
    dir: &Path,
) -> (PathBuf, crate::model::CheckpointId, Vec<u8>) {
    let c = w.core();
    let bound = w.open(c, spec(dir, 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let now = w.now;
    assert!(bound.handle.disconnect(Holder::Host));
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    let checkpoint = w.server.checkpoint();
    let frame = frame::encode_commit(
        CommitId {
            lineage: crate::ids::Lineage([0x6d; 16]),
            position: Position(1),
        },
        &CommitRecord {
            db_pages_after: 2,
            pages: vec![(2, Box::new([0x5a; 4096]))],
        },
    )
    .unwrap();
    w.server.foreign_append(PATH, &frame).unwrap();
    let (mut log, _) = crate::replog::Log::open(&generation.join("log")).unwrap();
    log.append(&RecordBody::Commit {
        frame: frame.clone(),
    })
    .unwrap();
    log.sync().unwrap();
    (generation, checkpoint, frame)
}

/// A caught-up record after the log's ACK was never applied (a catch-up applies its frames only
/// once their ACK is fsynced), so the log's open cuts it. A restore of a checkpoint from before it
/// then inherits the generation, which holds nothing pending: the open re-keys it, and the db
/// never holds the record's page.
#[test]
fn a_caught_up_record_after_the_ack_is_cut_and_the_open_re_keys() {
    let dir = TempDir::new("driver-foreign-after-ack");
    let mut w = World::new();
    let (generation, checkpoint, frame) = caught_up_after_the_ack(&mut w, dir.path());
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    assert!(
        !w.server
            .frames(PATH)
            .unwrap()
            .windows(frame.len())
            .any(|x| x == frame.as_slice())
    );
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    assert_eq!(bound.handle.attach, super::Attach::ReKeyed);
    assert_eq!(bound.handle.generation.dir, generation);
    assert!(
        !log_records(&generation)
            .iter()
            .any(|r| matches!(r, RecordBody::Commit { frame: f } if *f == frame))
    );
    let db = std::fs::read(generation.join("db")).unwrap();
    assert!(!db.chunks(4096).any(|p| p.iter().all(|&b| b == 0x5a)));
    assert!(salvages(&generation).is_empty());
}

/// The same with the restore right after the open's HEAD: the open resumes, its takeover's claim
/// meets the new token whose history inherits by the log's ACK, and with nothing pending (the
/// caught-up record cut) it re-keys and binds.
#[test]
fn a_caught_up_record_after_the_ack_lets_a_takeover_re_key_over_a_restore() {
    let dir = TempDir::new("driver-foreign-mid-open");
    let mut w = World::new();
    let (generation, checkpoint, _) = caught_up_after_the_ack(&mut w, dir.path());
    let before = segment_tokens(&generation);
    w.after_head = Some(Box::new(move |server: &mut ModelServer| {
        server
            .history_event(HistoryEvent::Restore(checkpoint))
            .unwrap();
    }));
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 2));
    let token = w.server.token(PATH).unwrap();
    assert_eq!(bound.handle.incarnation(), token);
    let tokens = segment_tokens(&generation);
    assert_eq!(tokens.first(), before.first());
    assert_eq!(tokens.last(), Some(&token));
}

/// The salvages of the root the generation at `generation` lived in.
fn salvages(generation: &Path) -> Vec<Salvaged> {
    crate::salvage::list(generation.parent().unwrap()).unwrap()
}

/// The one salvage of the root the generation at `generation` lived in: for `reason`, and
/// complete.
fn only_salvage(generation: &Path, reason: Reason) -> Salvaged {
    let mut all = salvages(generation);
    assert_eq!(all.len(), 1, "{all:?}");
    let s = all.pop().unwrap();
    assert_eq!(s.manifest.reason, reason);
    assert_eq!(s.manifest.state, State::Complete);
    s
}

/// A salvage's db file.
fn salvaged_db(s: &Salvaged) -> Vec<u8> {
    std::fs::read(s.dir.join("db")).unwrap()
}

/// Whether a salvage's log holds the commit frame `frame`.
fn holds_commit(s: &Salvaged, frame: &[u8]) -> bool {
    log_records(&s.dir)
        .iter()
        .any(|r| matches!(r, RecordBody::Commit { frame: f } if f.as_slice() == frame))
}

/// The attach of an open that built a generation from the stream, no snapshot installed, after
/// salvaging the local one for `reason`.
fn salvaged_attach(reason: Reason) -> Attach {
    Attach::Built {
        snapshot: None,
        salvaged: Some(reason),
    }
}

// A generation built from a snapshot holds no COMMIT record, only the ACK naming the snapshot.
// Its db file emptied is salvaged as `replaced`, never taken for a new database (whose pages the
// next commits would ship on top of the snapshot), and the open builds anew.
#[test]
fn an_emptied_db_of_a_snapshot_built_generation_is_salvaged() {
    let dir_a = TempDir::new("driver-emptied-a");
    let dir_b = TempDir::new("driver-emptied-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    commit_pages(&mut w, a, &bound, 2, &[1, 2], 1);
    w.run_for(1_000);
    w.retire(a, &bound);
    drop(bound);
    snapshot_and_retain(&mut w.server);
    let b = w.core();
    let built = w.open(b, spec(dir_b.path(), 2));
    let generation = built.handle.generation.clone();
    assert!(
        !log_records(&generation.dir)
            .iter()
            .any(|r| matches!(r, RecordBody::Commit { .. })),
        "built from the snapshot alone"
    );
    w.retire(b, &built);
    drop(built);
    std::fs::File::options()
        .write(true)
        .open(generation.db())
        .unwrap()
        .set_len(0)
        .unwrap();
    let c = w.core();
    let bound = w.open(c, spec(dir_b.path(), 3));
    assert!(matches!(bound.handle.attach, Attach::Built {
        snapshot: Some(_),
        salvaged: Some(Reason::Replaced)
    }));
    let s = only_salvage(&generation.dir, Reason::Replaced);
    assert!(!s.manifest.plain);
    assert!(salvaged_db(&s).is_empty());
}

/// A history event lands while a fresh open builds (after the open's HEAD, before the build's):
/// the build starts over under the new token (design D8) and the open takes over with it.
#[test]
fn a_history_change_during_a_fresh_build_starts_it_over() {
    let dir_a = TempDir::new("driver-d8-a");
    let dir_b = TempDir::new("driver-d8-b");
    let mut w = World::new();
    let a = w.core();
    let bound = w.open(a, spec(dir_a.path(), 1));
    let c1 = w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(a, &bound);
    drop(bound);
    let before = w.server.token(PATH).unwrap();
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        server.history_event(HistoryEvent::NewHistory).unwrap();
    }));
    let b = w.core();
    let built = w.open(b, spec(dir_b.path(), 2));
    let token = w.server.token(PATH).unwrap();
    assert_ne!(token, before);
    assert_eq!(built.handle.incarnation(), token);
    assert_eq!(segment_tokens(&built.handle.generation.dir), vec![token]);
    assert_eq!(w.stream_commits(), vec![c1]);
    let next = w.commit(b, &built, 2).unwrap();
    let ticket = w.sync(b, built.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(b, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits().last(), Some(&next));
}

/// The HEADs core `core` sent.
fn heads(w: &World, core: usize) -> usize {
    w.sent
        .iter()
        .filter(|s| s.core == core && s.request.method == Method::Head)
        .count()
}

/// A stream with one commit, its owner retired.
fn stream_with_a_commit(w: &mut World) -> (Vec<u8>, TempDir) {
    let dir = TempDir::new("driver-d8-source");
    let a = w.core();
    let bound = w.open(a, spec(dir.path(), 1));
    let c1 = w.commit(a, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(a, &bound);
    (c1, dir)
}

/// A history event after a fresh build's HEAD: its tail read meets a `412`, the HEAD names the new
/// token, and the build starts over under it. Eight times in a row the build starts over, each
/// counted once, within its eight restarts, and the open takes over with the last token.
#[test]
fn a_412_during_a_fresh_builds_read_restarts_it_once_per_history_change() {
    let mut w = World::new();
    let (c1, _source) = stream_with_a_commit(&mut w);
    // HEADs alternate: the build's own (a history event follows) and the one its read's `412`
    // sends (none follows), after the open's first.
    let mut n = 0u32;
    w.every_head = Some(Box::new(move |server: &mut ModelServer| {
        n += 1;
        if n.is_multiple_of(2) && n <= 16 {
            server.history_event(HistoryEvent::NewHistory).unwrap();
        }
    }));
    let dir = TempDir::new("driver-d8-412");
    let b = w.core();
    let bound = w.open(b, spec(dir.path(), 2));
    w.every_head = None;
    assert_eq!(bound.handle.incarnation(), w.server.token(PATH).unwrap());
    assert_eq!(heads(&w, b), 18);
    assert_eq!(w.stream_commits(), vec![c1]);
}

/// A fresh build whose stream changes history before every HEAD it sends gives up after its eight
/// restarts, publishing nothing (the `.tmp` of its last build goes at the next open).
#[test]
fn a_fresh_build_gives_up_after_its_restarts() {
    let mut w = World::new();
    let _source = stream_with_a_commit(&mut w);
    w.every_head = Some(Box::new(|server: &mut ModelServer| {
        server.history_event(HistoryEvent::NewHistory).unwrap();
    }));
    let dir = TempDir::new("driver-d8-limit");
    let b = w.core();
    let e = kind(w.open_result(b, spec(dir.path(), 2)));
    w.every_head = None;
    assert!(
        matches!(&*e, OpenError::StreamState(StreamState::HistoryChanged)),
        "{e}"
    );
    // The open's HEAD, then nine builds' HEADs: the ninth history change is one too many.
    assert_eq!(heads(&w, b), 10);
    let root = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let gens: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("gen-") && !n.ends_with(".tmp"))
        .collect();
    assert!(gens.is_empty(), "{gens:?}");
}

/// The stream is deleted while a fresh open builds it: the build is abandoned and the open goes
/// back through the open table, which creates the stream (no local generation) and builds that.
#[test]
fn a_stream_deleted_during_a_fresh_build_is_created_by_the_open() {
    let mut w = World::new();
    let _source = stream_with_a_commit(&mut w);
    let before = w.server.token(PATH).unwrap();
    w.skip_heads = 1;
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        server.delete(PATH).unwrap();
    }));
    let dir = TempDir::new("driver-d8-deleted");
    let b = w.core();
    let bound = w.open(b, spec(dir.path(), 2));
    let token = w.server.token(PATH).unwrap();
    assert_ne!(token, before);
    assert_eq!(bound.handle.incarnation(), token);
    assert!(w.stream_commits().is_empty());
    let root = bound.handle.generation.dir.parent().unwrap().to_owned();
    let gens: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("gen-"))
        .collect();
    assert_eq!(gens.len(), 1, "{gens:?}");
}

/// The same while a build is about to salvage a trusted generation holding data: back through the
/// open table, the missing stream is `not_found`, and the generation stays, not salvaged.
#[test]
fn a_stream_deleted_during_a_salvaging_build_is_not_found() {
    let dir = TempDir::new("driver-d8-deleted-local");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(c, &bound);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    w.skip_heads = 1;
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        server.delete(PATH).unwrap();
    }));
    let c2 = w.core();
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(matches!(&*e, OpenError::NotFound), "{e}");
    assert!(generation.exists());
    assert!(salvages(&generation).is_empty());
}

/// Another writer creates the missing stream between the open's HEAD and its create: the create
/// answers `200`, so the stream is not this open's, and the generation is salvaged as a history
/// change, not `recreated`.
#[test]
fn a_create_answered_200_salvages_as_a_history_change() {
    let dir = TempDir::new("driver-recreate-200");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    w.retire(c, &bound);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.server.delete(PATH).unwrap();
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        let create = Request {
            content_type: Some(CONTENT_TYPE.to_owned()),
            ..Request::new(Method::Put, Target::Stream)
        };
        assert_eq!(server.handle(PATH, &create).status, 201);
    }));
    let c2 = w.core();
    let bound = w.open(c2, OpenSpec {
        access: Access::Owner { recreate: true },
        ..spec(dir.path(), 2)
    });
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::HistoryChanged));
    only_salvage(&generation, Reason::HistoryChanged);
}

/// A generation directory with no `log/` at all is salvaged as `replaced`.
#[test]
fn a_generation_without_a_log_is_salvaged_as_replaced() {
    let (result, generation, _dir) = reopen_after(|_, generation| {
        std::fs::remove_dir_all(generation.join("log")).unwrap();
    });
    let bound = result.unwrap();
    assert_eq!(bound.handle.attach, salvaged_attach(Reason::Replaced));
    only_salvage(&generation, Reason::Replaced);
}

/// A create of the stream on `server`.
fn create(server: &mut ModelServer) {
    let create = Request {
        content_type: Some(CONTENT_TYPE.to_owned()),
        ..Request::new(Method::Put, Target::Stream)
    };
    assert_eq!(server.handle(PATH, &create).status, 201);
}

/// The route flips to another deployment, which serves the stream under its own token, while a
/// fresh open builds: the tail read meets a `412`, and the HEAD after it names the other cluster.
/// The build refuses `wrong_cluster` instead of starting over under that token, and nothing goes
/// to the other deployment.
#[test]
fn a_build_whose_route_flips_to_another_deployment_is_refused() {
    let mut w = World::new();
    let _source = stream_with_a_commit(&mut w);
    create(&mut w.other);
    let before = w.other.frames(PATH).unwrap().len();
    // The open's HEAD and the build's reach the server; the build's read and HEAD do not.
    w.misroute_after_heads = Some(2);
    let dir = TempDir::new("driver-build-misroute");
    let b = w.core();
    let e = kind(w.open_result(b, spec(dir.path(), 2)));
    assert!(matches!(&*e, OpenError::WrongCluster { .. }), "{e}");
    assert_eq!(w.other.frames(PATH).unwrap().len(), before);
}

/// The same with no stream on the other deployment: the read meets a `404`, and the HEAD's `404`
/// names the other cluster. The build refuses `wrong_cluster` and creates nothing there.
#[test]
fn a_build_whose_route_flips_to_a_deployment_without_the_stream_is_refused() {
    let mut w = World::new();
    let _source = stream_with_a_commit(&mut w);
    w.misroute_after_heads = Some(2);
    let dir = TempDir::new("driver-build-misroute-404");
    let b = w.core();
    let e = kind(w.open_result(b, spec(dir.path(), 2)));
    assert!(matches!(&*e, OpenError::WrongCluster { .. }), "{e}");
    assert!(w.other.frames(PATH).is_none());
}

/// Reads keep meeting `412`s while the HEAD names the build's token (reads and HEADs reaching
/// different nodes): each check waits the open's backoff, and the build gives up after its checks
/// (or the open at its deadline) instead of looping.
#[test]
fn reads_and_heads_that_disagree_end_the_build_after_backoffs() {
    let mut w = World::new();
    let _source = stream_with_a_commit(&mut w);
    create(&mut w.other);
    w.misroute_reads = true;
    let dir = TempDir::new("driver-build-disagree");
    let b = w.core();
    let start = w.now;
    let e = kind(w.open_result(b, spec(dir.path(), 2)));
    w.misroute_reads = false;
    assert!(
        matches!(&*e, OpenError::GaveUp(crate::open::GaveUp::Rechecks)),
        "{e}"
    );
    assert_eq!(e.error_kind().as_str(), "unavailable");
    let heads = heads(&w, b);
    assert!((3..=20).contains(&heads), "{heads} HEADs");
    assert!(w.now - start >= 1_000, "{} ms", w.now - start);
}

/// An owner that committed `salt` and crashed: the generation holding its CLAIM record, the
/// epoch of that claim and the frame of its commit, acknowledged and its ACK written.
fn crashed_owner(w: &mut World, dir: &Path, salt: u8) -> (PathBuf, Epoch, Vec<u8>) {
    let c = w.core();
    let bound = w.open(c, spec(dir, 1));
    let frame = w.commit(c, &bound, salt).unwrap();
    // Past the ACK interval: the commit's ACK is written.
    w.run_for(2_000);
    assert_eq!(bound.handle.pending_bytes(), 0);
    let own = *w.claims(c).last().unwrap();
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.crash(c);
    (generation, own, frame)
}

/// Every file under `dir` with its bytes.
fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_owned()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }
    out
}

/// A provisional open on core `c` (the stream unreachable from it): it resolves within the 5 s
/// connect limit, before any answer, and its binding is provisional.
fn open_provisionally(w: &mut World, c: usize, dir: &Path) -> Bound {
    w.down[c] = true;
    let start = w.now;
    let bound = w.open(c, spec(dir, 2));
    assert!(w.now - start <= 5_000, "the open took {} ms", w.now - start);
    assert!(bound.handle.provisional());
    bound
}

/// The epochs of the claims core `c` sent, each attempt of one claim once.
fn claimed(w: &World, c: usize) -> Vec<Epoch> {
    let mut claims = w.claims(c);
    claims.dedup();
    claims
}

/// The stream is unreachable at the reopen (design §5.5): the trusted generation opens at once,
/// provisional, and nothing is logged. Commits log and stay pending, `synced()` waits with the
/// condition `retrying`. Once the stream answers, the pre-check (a HEAD, then the reads from the
/// log's acknowledged offset) finds nothing foreign, the log's claim `own` goes out again, its
/// repeat brings the claim of `own + 1`, the pending commit goes out, `synced()` resolves and the
/// binding is live.
#[test]
fn a_provisional_open_waits_for_its_claim_then_goes_live() {
    let dir = TempDir::new("driver-provisional");
    let mut w = World::new();
    let (generation, own, first) = crashed_owner(&mut w, dir.path(), 1);
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    assert_eq!(bound.handle.generation.dir, generation);
    assert_eq!(bound.handle.attach, super::Attach::Resumed);
    let x = w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.run_for(60_000);
    assert_eq!(w.outcome(c, ticket), None, "synced() waits for the claim");
    assert!(bound.handle.provisional());
    assert!(w.events(c).iter().any(|e| matches!(
        e,
        DriverEvent::Condition {
            binding,
            condition: Some(crate::replicator::Condition::Retrying),
        } if *binding == bound.binding
    )));
    assert_eq!(w.stream_commits(), vec![first.clone()]);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(!bound.handle.provisional());
    assert!(w.resets(c).is_empty());
    let next = Epoch::try_from(own.get() + 1).unwrap();
    assert_eq!(claimed(&w, c), vec![own, next]);
    assert_eq!(w.stream_commits(), vec![first, x]);
    assert!(log_records(&generation).contains(&RecordBody::Claim {
        epoch: next,
        activation: Activation([2; 16]),
    }));
}

/// A provisional binding with nothing pending retires once its last connection closes, its
/// pre-check still out, and logged nothing: the next open, which reaches the stream, takes over
/// the same generation, its claim above the log's.
#[test]
fn a_provisional_binding_with_nothing_pending_retires_while_its_pre_check_is_out() {
    let dir = TempDir::new("driver-provisional-retire");
    let mut w = World::new();
    let (generation, own, _) = crashed_owner(&mut w, dir.path(), 1);
    let records = log_records(&generation);
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    w.retire(c, &bound);
    assert!(
        w.events(c)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding } if *binding == bound.binding))
    );
    assert!(!bound.handle.holds_root());
    drop(bound);
    // No CLAIM record was logged.
    let after = log_records(&generation);
    assert!(
        after
            .iter()
            .skip(records.len())
            .all(|r| !matches!(r, RecordBody::Claim { .. })),
        "{after:?}"
    );
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 3));
    assert_eq!(bound.handle.generation.dir, generation);
    assert!(!bound.handle.provisional());
    assert!(w.claims(c2).iter().all(|e| *e > own), "{:?}", w.claims(c2));
}

/// Another owner took the stream over while it was out of reach: the pre-check finds its claim
/// and the binding resets `fenced` with nothing sent, its `synced()` rejects, none of its
/// provisional commits reaches the stream, and the other owner goes on.
#[test]
fn a_provisional_open_after_another_owners_takeover_is_fenced() {
    let dir = TempDir::new("driver-provisional-fenced");
    let other = TempDir::new("driver-provisional-other");
    let mut w = World::new();
    let (_, own, first) = crashed_owner(&mut w, dir.path(), 1);
    let b = w.core();
    let bound_b = w.open(b, spec(other.path(), 7));
    let theirs = w.commit(b, &bound_b, 3).unwrap();
    w.run_for(1_000);
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    let x = w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.resets(c), vec![ResetKind::Fenced]);
    assert_eq!(
        w.outcome(c, ticket),
        Some(Err(SyncError::Reset(ResetKind::Fenced)))
    );
    assert!(
        claimed(&w, c).is_empty(),
        "{:?} after {own}",
        claimed(&w, c)
    );
    assert_eq!(w.stream_commits(), vec![first, theirs]);
    assert!(!w.stream_commits().contains(&x));
    let more = w.commit(b, &bound_b, 4).unwrap();
    let ticket = w.sync(b, bound_b.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(b, ticket), Some(Ok(())));
    assert!(w.resets(b).is_empty());
    assert_eq!(w.stream_commits().last(), Some(&more));
}

/// A history event that loses nothing happened while the stream was out of reach: the
/// pre-check's HEAD shows the history inherits the generation, the log rolls to the new token
/// with the claim, which goes out again, same epoch and body, and the claim of `own + 1` follows
/// it (its epoch may carry writes of the history before). No reset, and later commits replicate.
#[test]
fn a_provisional_claim_meeting_an_inheriting_history_re_keys() {
    let dir = TempDir::new("driver-provisional-rekey");
    let mut w = World::new();
    let (generation, own, first) = crashed_owner(&mut w, dir.path(), 1);
    let before = segment_tokens(&generation);
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    let ticket = w.sync(c, bound.binding);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(w.resets(c).is_empty());
    assert!(!bound.handle.provisional());
    let token = w.server.token(PATH).unwrap();
    assert_eq!(bound.handle.incarnation(), token);
    let next = Epoch::try_from(own.get() + 1).unwrap();
    assert_eq!(claimed(&w, c), vec![own, next]);
    let x = w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert_eq!(w.stream_commits(), vec![first, x]);
    let mut tokens = before;
    tokens.push(token);
    assert_eq!(segment_tokens(&generation), tokens);
}

/// The same history event with a commit the provisional binding logged during the outage:
/// that commit was never sent, so the B2 gate counts only what the log held at the open, and
/// nothing. The binding re-keys, its commit replicates, and nothing is salvaged. On a single
/// voter with `fsync=never` every unclean restart is such an event, and offline work must survive
/// it.
#[test]
fn an_offline_commit_survives_a_history_that_keeps_everything() {
    let dir = TempDir::new("driver-provisional-rekey-offline");
    let mut w = World::new();
    let (generation, own, first) = crashed_owner(&mut w, dir.path(), 1);
    let before = segment_tokens(&generation);
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    let x = w.commit(c, &bound, 2).unwrap();
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(w.resets(c).is_empty());
    let token = w.server.token(PATH).unwrap();
    assert_eq!(bound.handle.incarnation(), token);
    let next = Epoch::try_from(own.get() + 1).unwrap();
    assert_eq!(claimed(&w, c), vec![own, next]);
    assert_eq!(w.stream_commits(), vec![first, x]);
    let mut tokens = before;
    tokens.push(token);
    assert_eq!(segment_tokens(&generation), tokens);
    assert!(bound.handle.salvage().unwrap().is_empty());
}

/// The opposite case: a commit logged before this open, after the log's latest ACK, may have been
/// sent and acknowledged, so the B2 gate holds. The binding resets `rewound` before its log rolls
/// and sends nothing (its connections are live, it cannot salvage under them), and the next open,
/// which reaches the stream, salvages the generation (`history_changed`). The manifest counts that
/// commit and the provisional one as unacknowledged.
#[test]
fn a_commit_before_the_open_after_the_ack_resets_rewound_at_a_history_and_the_next_open_salvages() {
    let dir = TempDir::new("driver-provisional-rekey-pending");
    let mut w = World::new();
    let first = w.core();
    let bound = w.open(first, spec(dir.path(), 1));
    w.commit(first, &bound, 1).unwrap();
    w.run_for(2_000);
    w.server.inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropRequest,
        times: 1,
    });
    w.commit(first, &bound, 3).unwrap();
    let generation = bound.handle.generation.dir.clone();
    let own = *w.claims(first).last().unwrap();
    drop(bound);
    w.crash(first);
    let before = segment_tokens(&generation);
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.resets(c), vec![ResetKind::Rewound]);
    assert_eq!(
        w.outcome(c, ticket),
        Some(Err(SyncError::Reset(ResetKind::Rewound)))
    );
    assert!(
        claimed(&w, c).is_empty(),
        "{:?} after {own}",
        claimed(&w, c)
    );
    assert_eq!(segment_tokens(&generation), before);
    drop(bound);
    let c2 = w.core();
    let bound = w.open(c2, spec(dir.path(), 3));
    assert!(
        matches!(bound.handle.attach, super::Attach::Built {
            salvaged: Some(_),
            ..
        }),
        "{:?}",
        bound.handle.attach
    );
    let salvages = bound.handle.salvage().unwrap();
    let [salvaged] = &salvages[..] else {
        panic!("{salvages:?}");
    };
    assert_eq!(
        salvaged
            .manifest
            .acknowledged
            .as_ref()
            .map(|a| a.unacknowledged),
        Some(2)
    );
}

/// A restore that lost an acknowledged commit happened while the stream was out of reach: the
/// provisional claim meets the new token, whose history does not inherit the generation, and the
/// binding resets `rewound`.
#[test]
fn a_provisional_claim_meeting_a_lossy_restore_resets_rewound() {
    let dir = TempDir::new("driver-provisional-rewound");
    let mut w = World::new();
    let first = w.core();
    let bound = w.open(first, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    w.commit(first, &bound, 1).unwrap();
    w.run_for(2_000);
    assert_eq!(bound.handle.pending_bytes(), 0);
    drop(bound);
    w.crash(first);
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    w.commit(c, &bound, 2).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.resets(c), vec![ResetKind::Rewound]);
    assert_eq!(
        w.outcome(c, ticket),
        Some(Err(SyncError::Reset(ResetKind::Rewound)))
    );
}

/// A power loss dropped a commit the stream acknowledged, with its ACK, from the log (design
/// §8.3): the generation is as it was before that commit. The provisional binding commits other
/// bytes at the same position, and once the stream answers, its pre-check finds the lost commit
/// in the stream with no pending record to match it: the binding resets `stale` with nothing sent,
/// and its provisional commit never reaches the stream.
#[test]
fn a_provisional_open_whose_log_lost_acknowledged_commits_goes_stale() {
    let dir = TempDir::new("driver-provisional-stale");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    w.run_for(2_000);
    let generation = bound.handle.generation.dir.clone();
    let before = files(&generation);
    let lost = w.commit(c, &bound, 1).unwrap();
    w.run_for(2_000);
    assert_eq!(bound.handle.pending_bytes(), 0);
    drop(bound);
    w.crash(c);
    // The power loss: the files as they were before the commit.
    std::fs::remove_dir_all(&generation).unwrap();
    for (path, bytes) in &before {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    let c2 = w.core();
    let bound = open_provisionally(&mut w, c2, dir.path());
    let x = w.commit(c2, &bound, 2).unwrap();
    let ticket = w.sync(c2, bound.binding);
    w.down[c2] = false;
    w.run_for(10_000);
    assert_eq!(w.resets(c2), vec![ResetKind::Stale]);
    assert_eq!(
        w.outcome(c2, ticket),
        Some(Err(SyncError::Reset(ResetKind::Stale)))
    );
    assert_eq!(w.stream_commits(), vec![lost]);
    assert!(!w.stream_commits().contains(&x));
}

/// With no local generation there is nothing to open provisionally: the open waits for the
/// stream and fails at its deadline with `unreachable`.
#[test]
fn an_unreachable_stream_with_no_generation_waits_until_the_deadline() {
    let dir = TempDir::new("driver-unreachable-fresh");
    let mut w = World::new();
    let c = w.core();
    w.down[c] = true;
    let start = w.now;
    let e = kind(w.open_result(c, OpenSpec {
        timeout_ms: 10_000,
        ..spec(dir.path(), 1)
    }));
    assert!(w.now - start >= 10_000);
    assert!(
        matches!(*e, OpenError::Unavailable(Unavailable::Unreachable)),
        "{e}"
    );
}

/// A stream that answered the first HEAD, even with a `503`, is reachable: an open that loses it
/// afterwards waits for the takeover until its deadline, never provisional.
#[test]
fn a_stream_that_answered_first_is_reachable_and_the_open_waits() {
    let dir = TempDir::new("driver-reachable-then-down");
    let mut w = World::new();
    crashed_owner(&mut w, dir.path(), 1);
    let c = w.core();
    w.server.inject(Scripted {
        method: Some(Method::Head),
        fault: Fault::Status {
            status: 503,
            retry_after: None,
        },
        times: 1,
    });
    // The stream goes out of reach right after the first HEAD's answer.
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        server.inject(Scripted {
            method: None,
            fault: Fault::Unreachable,
            times: 1_000,
        });
    }));
    let e = kind(w.open_result(c, OpenSpec {
        timeout_ms: 10_000,
        ..spec(dir.path(), 2)
    }));
    assert!(matches!(*e, OpenError::Unavailable(_)), "{e}");
    assert_ne!(e.error_kind().as_str(), "unreachable");
}

/// A connect to a host that does not answer fails after the 5 s connect limit: the reopen of a
/// trusted generation with a CLAIM record resolves then, provisional, within the limit of the
/// first request and before any answer.
#[test]
fn a_provisional_open_resolves_when_its_first_connect_times_out() {
    let dir = TempDir::new("driver-provisional-connect");
    let mut w = World::new();
    crashed_owner(&mut w, dir.path(), 1);
    let c = w.core();
    w.down[c] = true;
    w.connect_ms = 5_000;
    let start = w.now;
    let bound = w.open(c, spec(dir.path(), 2));
    let took = w.now - start;
    assert!((5_000..=5_100).contains(&took), "the open took {took} ms");
    assert!(bound.handle.provisional());
}

/// A provisional binding reset with its connection still open (the generation held), as each
/// of its resets comes: what a reopen beside it starts from.
struct ResetProvisional {
    w: World,
    c: usize,
    bound: Bound,
    /// The commits it logged while provisional, which never reached the stream.
    offline: Vec<Vec<u8>>,
}

/// `kind`'s scenario: `fenced` by another owner's takeover (from `other`), `stale` after a power
/// loss of an acknowledged commit, `rewound` by a lossy restore. Two commits are logged while
/// provisional, then the stream answers and the binding resets with its connection open.
fn provisional_reset(kind: ResetKind, dir: &Path, other: &Path) -> ResetProvisional {
    let mut w = World::new();
    match kind {
        ResetKind::Fenced => {
            crashed_owner(&mut w, dir, 1);
            let b = w.core();
            let bound_b = w.open(b, spec(other, 7));
            w.commit(b, &bound_b, 3).unwrap();
            w.run_for(1_000);
        }
        ResetKind::Stale => {
            let c = w.core();
            let bound = w.open(c, spec(dir, 1));
            w.run_for(2_000);
            let generation = bound.handle.generation.dir.clone();
            let before = files(&generation);
            w.commit(c, &bound, 1).unwrap();
            w.run_for(2_000);
            drop(bound);
            w.crash(c);
            // The power loss: the files as they were before the acknowledged commit.
            std::fs::remove_dir_all(&generation).unwrap();
            for (path, bytes) in &before {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, bytes).unwrap();
            }
        }
        _ => {
            let c = w.core();
            let bound = w.open(c, spec(dir, 1));
            let checkpoint = w.server.checkpoint();
            w.commit(c, &bound, 1).unwrap();
            w.run_for(2_000);
            drop(bound);
            w.crash(c);
            w.server
                .history_event(HistoryEvent::Restore(checkpoint))
                .unwrap();
        }
    }
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir);
    let offline = vec![
        w.commit(c, &bound, 2).unwrap(),
        w.commit(c, &bound, 4).unwrap(),
    ];
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.resets(c), vec![kind]);
    assert_eq!(bound.handle.reset(), Some(kind));
    assert!(
        bound.handle.holds_root(),
        "{kind:?}: the connection holds it"
    );
    ResetProvisional {
        w,
        c,
        bound,
        offline,
    }
}

/// The bytes of `dir`'s log segments, in order.
fn log_bytes(dir: &Path) -> Vec<u8> {
    files(&dir.join("log")).into_values().flatten().collect()
}

/// A provisional binding resets (`fenced`, `stale`, `rewound`) while its connection still holds
/// its generation. Its connection's commits are refused under the log's lock from the reset on
/// (G8), so the log stays as the reset left it. A reopen in the same core runs beside it from a
/// copy of that generation (design §5.6), and the original is trashed. After `rewound`, the
/// reopen's salvage (`history_changed`) takes the copy, whose log holds the original's, and its
/// manifest counts the provisional commits as unacknowledged. After `fenced` or `stale` the
/// reopen takes over and rebuilds from the stream, which never held the provisional commits.
#[test]
fn a_reset_provisional_binding_is_reopened_beside_its_connection_from_a_copy() {
    for kind in [ResetKind::Fenced, ResetKind::Stale, ResetKind::Rewound] {
        let (dir, other) = (
            TempDir::new("driver-provisional-copy"),
            TempDir::new("driver-provisional-copy-other"),
        );
        let ResetProvisional {
            mut w,
            c,
            bound,
            offline,
        } = provisional_reset(kind, dir.path(), other.path());
        let connection = crate::db::connection_for_tests(
            bound.binding,
            Arc::clone(&bound.handle),
            String::new(),
        );
        let next = bound.handle.log().unwrap().next_position();
        assert!(
            matches!(connection.log_unless_reset(), Err(crate::db::LogRefused::Reset(k)) if k == kind),
            "{kind:?}"
        );
        assert_eq!(bound.handle.log().unwrap().next_position(), next);
        let old = bound.handle.generation.clone();
        let copy = w.open(c, spec(dir.path(), 3));
        assert_ne!(copy.handle.generation.id, old.id, "{kind:?}");
        assert!(!old.dir.exists(), "{kind:?}: the original is trashed");
        assert!(bound.handle.holds_root(), "{kind:?}");
        let salvages = copy.handle.salvage().unwrap();
        if kind == ResetKind::Rewound {
            assert!(
                matches!(copy.handle.attach, super::Attach::Built {
                    salvaged: Some(Reason::HistoryChanged),
                    ..
                }),
                "{:?}",
                copy.handle.attach
            );
            let [salvaged] = &salvages[..] else {
                panic!("{salvages:?}");
            };
            // The copy's log, with the provisional commits, superseding the original's.
            let log = log_bytes(&salvaged.dir);
            for frame in &offline {
                assert!(log.windows(frame.len()).any(|w| w == &frame[..]));
            }
            assert_eq!(salvaged.manifest.lineage, Some(old.lineage));
            assert_eq!(
                salvaged
                    .manifest
                    .acknowledged
                    .as_ref()
                    .map(|a| a.unacknowledged),
                Some(2)
            );
        } else {
            assert!(
                matches!(copy.handle.attach, super::Attach::Built {
                    salvaged: None,
                    ..
                }),
                "{kind:?}: {:?}",
                copy.handle.attach
            );
            assert!(salvages.is_empty(), "{kind:?}: {salvages:?}");
        }
        let ticket = w.sync(c, copy.binding);
        w.run_for(5_000);
        assert_eq!(w.outcome(c, ticket), Some(Ok(())), "{kind:?}");
        assert_eq!(w.resets(c), vec![kind], "the copy is live");
        let stream = w.stream_commits();
        assert!(offline.iter().all(|f| !stream.contains(f)), "{kind:?}");
        drop(connection);
        assert!(!bound.handle.disconnect(Holder::Host));
        assert!(!bound.handle.holds_root());
    }
}

/// A provisional binding resets while the stream is still out of reach, its connection open. A
/// reopen in the same core opens provisionally again, from a copy of the generation and its CLAIM
/// records: nothing is logged and nothing sent. Once the stream answers, the copy claims as the
/// original would have, and the commit both carried replicates.
#[test]
fn a_reopen_beside_a_reset_provisional_binding_while_unreachable_is_provisional_again() {
    let dir = TempDir::new("driver-provisional-copy-offline");
    let mut w = World::new();
    let (_, own, first) = crashed_owner(&mut w, dir.path(), 1);
    let c = w.core();
    let bound = open_provisionally(&mut w, c, dir.path());
    let x = w.commit(c, &bound, 2).unwrap();
    w.run_for(1_000);
    let now = w.now;
    w.c(c)
        .reset_binding(bound.binding, ResetKind::LocalDamage, now);
    assert_eq!(bound.handle.reset(), Some(ResetKind::LocalDamage));
    assert!(bound.handle.holds_root());
    let old = bound.handle.generation.clone();
    let copy = open_provisionally(&mut w, c, dir.path());
    assert_eq!(copy.handle.generation.lineage, old.lineage);
    assert_ne!(copy.handle.generation.id, old.id);
    assert!(!old.dir.exists(), "the original is trashed");
    assert_eq!(copy.handle.last_commit(), bound.handle.last_commit());
    assert!(claimed(&w, c).is_empty(), "nothing sent while unreachable");
    let ticket = w.sync(c, copy.binding);
    w.down[c] = false;
    w.run_for(10_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    assert!(!copy.handle.provisional());
    let next = Epoch::try_from(own.get() + 1).unwrap();
    assert_eq!(claimed(&w, c), vec![own, next]);
    assert_eq!(w.stream_commits(), vec![first, x]);
}

// A hand-out a failed open gave back waits again from the start: the driver may have ended the
// first wait in the moment the binding was live, and the binding still retires if nobody connects.
#[test]
fn a_hand_out_given_back_by_a_failed_open_waits_again() {
    let dir = TempDir::new("driver-unconnect-wait");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open_result(c, spec(dir.path(), 1)).unwrap();
    let connected = bound.handle.connect(Holder::Host).unwrap();
    // A tick while it is live ends the hand-out's wait.
    let now = w.now;
    let ios = w.c(c).tick(now);
    w.queue_ios(c, ios);
    assert_eq!(bound.handle.unconnect(connected), Undone::WaitsAgain);
    // What `Binding::unconnect` tells the driver.
    w.c(c).unconnected(bound.binding, now);
    w.run_for(UNCONNECTED_MS / 2);
    assert_eq!(bound.handle.life(), Life::HandedOut { awaiting: 1 });
    w.run_for(UNCONNECTED_MS / 2 + 1_000);
    assert_eq!(bound.handle.life(), Life::Retired);
    assert!(!bound.handle.holds_root());
}

/// A read-only open's spec: the reader of design D6.
fn reader_spec(dir: &Path) -> OpenSpec {
    OpenSpec {
        access: Access::Reader,
        ..spec(dir, 9)
    }
}

/// The stream's root directory under the cache root `dir`.
fn stream_root(dir: &Path) -> PathBuf {
    dir.join(root_name(&canonical_path(URL).unwrap()))
}

/// The db file of the binding, as it is on disk.
fn db_bytes(bound: &Bound) -> Vec<u8> {
    std::fs::read(bound.handle.generation.db()).unwrap()
}

/// An owner bound on core `c` that committed a page of `salt` and saw it acknowledged.
fn owner_with_a_commit(w: &mut World, dir: &Path, c: usize, salt: u8) -> Bound {
    let bound = w.open(c, spec(dir, 1));
    w.commit(c, &bound, salt).unwrap();
    let ticket = w.sync(c, bound.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(c, ticket), Some(Ok(())));
    bound
}

/// A reader in another process (design D6) builds its generation in `reader-0/`, beside the
/// owner's lock, from HEADs and reads alone: it claims nothing, so the owner goes on committing
/// unfenced. Its `synced()` resolves at once, and it shows the stream as of its open. A second
/// reader takes `reader-1/`.
#[test]
fn a_reader_builds_in_a_reader_directory_and_claims_nothing() {
    let dir = TempDir::new("driver-reader");
    let mut w = World::new();
    let o = w.core();
    let owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let reader = w.open(r, reader_spec(dir.path()));
    let h = &reader.handle;
    assert_eq!(h.role, Role::Reader);
    assert_eq!(h.attach, Attach::Built {
        snapshot: None,
        salvaged: None
    });
    assert!(
        h.generation
            .dir
            .starts_with(stream_root(dir.path()).join("reader-0"))
    );
    assert_eq!(db_bytes(&reader), page_on_disk(h.generation.lineage, 1));
    assert_eq!(h.incarnation(), w.server.token(PATH).unwrap());
    let methods: Vec<Method> = w
        .sent
        .iter()
        .filter(|s| s.core == r)
        .map(|s| s.request.method)
        .collect();
    assert!(
        methods
            .iter()
            .all(|m| matches!(m, Method::Head | Method::Get)),
        "{methods:?}"
    );
    let ticket = w.sync(r, reader.binding);
    assert_eq!(w.outcome(r, ticket), Some(Ok(())));
    // The owner is not fenced, and the reader keeps the state as of its open.
    w.commit(o, &owner, 2).unwrap();
    let ticket = w.sync(o, owner.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(o, ticket), Some(Ok(())));
    assert!(w.resets(o).is_empty());
    assert_eq!(db_bytes(&reader), page_on_disk(h.generation.lineage, 1));
    let r2 = w.core();
    let second = w.open(r2, reader_spec(dir.path()));
    assert!(
        second
            .handle
            .generation
            .dir
            .starts_with(stream_root(dir.path()).join("reader-1"))
    );
    assert_eq!(
        db_bytes(&second),
        page_on_disk(second.handle.generation.lineage, 2)
    );
}

/// A reader retires as soon as its last connection closes, unlocking its directory. Its next open
/// takes the same directory and catches its generation up in place to the tail.
#[test]
fn a_reader_reopened_catches_its_generation_up_in_place() {
    let dir = TempDir::new("driver-reader-catch-up");
    let mut w = World::new();
    let o = w.core();
    let owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let first = w.open(r, reader_spec(dir.path()));
    let generation = first.handle.generation.clone();
    w.retire(r, &first);
    assert!(!first.handle.holds_root());
    assert!(
        w.events(r)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding } if *binding == first.binding))
    );
    w.commit(o, &owner, 2).unwrap();
    let ticket = w.sync(o, owner.binding);
    w.run_for(1_000);
    assert_eq!(w.outcome(o, ticket), Some(Ok(())));
    let again = w.open(r, reader_spec(dir.path()));
    assert_eq!(again.handle.attach, Attach::CaughtUp);
    assert_eq!(again.handle.generation, generation);
    assert_eq!(db_bytes(&again), page_on_disk(generation.lineage, 2));
    assert!(w.resets(o).is_empty());
}

/// A history change of the stream rebuilds a reader generation, the old one trashed: it holds no
/// commit of its own to keep, so it is neither re-keyed nor salvaged.
#[test]
fn a_reader_of_another_token_builds_anew() {
    let dir = TempDir::new("driver-reader-history");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let first = w.open(r, reader_spec(dir.path()));
    let old = first.handle.generation.clone();
    w.retire(r, &first);
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let again = w.open(r, reader_spec(dir.path()));
    assert!(matches!(again.handle.attach, Attach::Built { .. }));
    assert_ne!(again.handle.generation.dir, old.dir);
    assert!(!old.dir.exists());
    assert_eq!(again.handle.incarnation(), w.server.token(PATH).unwrap());
    assert_eq!(
        db_bytes(&again),
        page_on_disk(again.handle.generation.lineage, 1)
    );
    assert!(!stream_root(dir.path()).join("reader-0/salvage").exists());
}

/// A reader of a missing stream is refused `not_found`: it never creates one.
#[test]
fn a_reader_of_a_missing_stream_is_not_found() {
    let dir = TempDir::new("driver-reader-missing");
    let mut w = World::new();
    let r = w.core();
    let e = kind(w.open_result(r, reader_spec(dir.path())));
    assert!(matches!(&*e, OpenError::NotFound), "{e}");
    assert!(w.sent.iter().all(|s| s.request.method == Method::Head));
    assert!(w.server.frames(PATH).is_none());
}

/// An unreachable stream opens a trusted reader generation as it is, the stream as of its last
/// open. Without one the open waits, and fails `unreachable` at its deadline.
#[test]
fn an_unreachable_stream_opens_a_trusted_reader_generation_and_waits_without_one() {
    let dir = TempDir::new("driver-reader-unreachable");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let first = w.open(r, reader_spec(dir.path()));
    let generation = first.handle.generation.clone();
    w.retire(r, &first);
    w.down[r] = true;
    let start = w.now;
    let offline = w.open(r, reader_spec(dir.path()));
    assert!(w.now - start < 1_000, "{} ms", w.now - start);
    assert_eq!(offline.handle.attach, Attach::Resumed);
    assert_eq!(offline.handle.generation, generation);
    assert_eq!(db_bytes(&offline), page_on_disk(generation.lineage, 1));
    let empty = TempDir::new("driver-reader-unreachable-none");
    let start = w.now;
    let e = kind(w.open_result(r, OpenSpec {
        timeout_ms: 5_000,
        ..reader_spec(empty.path())
    }));
    assert!(
        matches!(&*e, OpenError::Unavailable(Unavailable::Unreachable)),
        "{e}"
    );
    assert!(w.now - start >= 5_000, "{} ms", w.now - start);
}

/// A reader opened and retired on core `r`: its generation.
fn reader_once(w: &mut World, r: usize, dir: &Path) -> crate::generation::Generation {
    let first = w.open(r, reader_spec(dir));
    let generation = first.handle.generation.clone();
    w.retire(r, &first);
    generation
}

/// Retention passed what a reader generation acknowledged: its catch-up meets a `410` and builds
/// anew from the latest snapshot, the old generation trashed.
#[test]
fn a_reader_whose_catch_up_meets_a_410_builds_anew() {
    let dir = TempDir::new("driver-reader-410");
    let mut w = World::new();
    let o = w.core();
    let owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let old = reader_once(&mut w, r, dir.path());
    w.commit(o, &owner, 2).unwrap();
    w.run_for(1_000);
    snapshot_and_retain(&mut w.server);
    let again = w.open(r, reader_spec(dir.path()));
    assert!(
        matches!(again.handle.attach, Attach::Built {
            snapshot: Some(_),
            salvaged: None
        }),
        "{:?}",
        again.handle.attach
    );
    assert_ne!(again.handle.generation.dir, old.dir);
    assert!(!old.dir.exists());
    let expected = image_of(w.server.frames(PATH).unwrap());
    assert_eq!(
        db_bytes(&again),
        on_disk(again.handle.generation.lineage, expected)
    );
}

/// The stream changes history between the reader's HEAD and its catch-up's read: the read meets a
/// `412`, the HEAD goes again, and the reader rows build anew under the new token.
#[test]
fn a_reader_whose_catch_up_meets_a_412_heads_again() {
    let dir = TempDir::new("driver-reader-412");
    let mut w = World::new();
    let o = w.core();
    let owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let old = reader_once(&mut w, r, dir.path());
    w.commit(o, &owner, 2).unwrap();
    w.run_for(1_000);
    let heads_before = heads(&w, r);
    w.after_head = Some(Box::new(|s: &mut ModelServer| {
        s.history_event(HistoryEvent::NewHistory).unwrap();
    }));
    let again = w.open(r, reader_spec(dir.path()));
    // The open's HEAD, the one after the `412`, and the build's own.
    assert_eq!(heads(&w, r) - heads_before, 3);
    assert!(matches!(again.handle.attach, Attach::Built { .. }));
    assert_ne!(again.handle.generation.dir, old.dir);
    assert_eq!(again.handle.incarnation(), w.server.token(PATH).unwrap());
    assert_eq!(
        db_bytes(&again),
        page_on_disk(again.handle.generation.lineage, 2)
    );
}

/// A reader reads a closed stream, which an owner refuses.
#[test]
fn a_reader_reads_a_closed_stream() {
    let dir = TempDir::new("driver-reader-closed");
    let mut w = World::new();
    let o = w.core();
    let owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    w.retire(o, &owner);
    w.server.close(PATH).unwrap();
    let r = w.core();
    let reader = w.open(r, reader_spec(dir.path()));
    assert!(reader.handle.closed);
    assert_eq!(
        db_bytes(&reader),
        page_on_disk(reader.handle.generation.lineage, 1)
    );
    w.retire(r, &reader);
    let again = w.open(r, reader_spec(dir.path()));
    assert_eq!(again.handle.attach, Attach::CaughtUp);
    assert!(again.handle.closed);
    let e = kind(w.open_result(w.cores.len() - 1, spec(dir.path(), 2)));
    assert!(
        matches!(&*e, OpenError::StreamState(StreamState::Closed)),
        "{e}"
    );
}

/// A reader generation whose db file is not its own is built anew, never salvaged.
#[test]
fn a_replaced_reader_generation_is_rebuilt_without_salvage() {
    let dir = TempDir::new("driver-reader-replaced");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let old = reader_once(&mut w, r, dir.path());
    let db = std::fs::OpenOptions::new()
        .write(true)
        .open(old.db())
        .unwrap();
    std::os::unix::fs::FileExt::write_all_at(&db, &SQLITE_MAGIC, 0).unwrap();
    drop(db);
    let again = w.open(r, reader_spec(dir.path()));
    assert_eq!(again.handle.attach, Attach::Built {
        snapshot: None,
        salvaged: None
    });
    assert_ne!(again.handle.generation.dir, old.dir);
    assert!(!old.dir.exists());
    assert!(!stream_root(dir.path()).join("reader-0/salvage").exists());
    assert_eq!(
        db_bytes(&again),
        page_on_disk(again.handle.generation.lineage, 1)
    );
}

/// A reader binding's connections found it damaged: it resets and retires, its connections keep
/// its directory locked until the last one closes, and the next reader takes that directory again.
#[test]
fn a_reader_resets_and_lets_its_directory_go_with_its_last_connection() {
    let dir = TempDir::new("driver-reader-reset");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let reader = w.open(r, reader_spec(dir.path()));
    let now = w.now;
    w.c(r)
        .reset_binding(reader.binding, ResetKind::LocalDamage, now);
    assert_eq!(reader.handle.reset(), Some(ResetKind::LocalDamage));
    assert_eq!(w.resets(r), vec![ResetKind::LocalDamage]);
    assert!(
        w.events(r)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding } if *binding == reader.binding))
    );
    let ticket = w.sync(r, reader.binding);
    assert_eq!(
        w.outcome(r, ticket),
        Some(Err(SyncError::Reset(ResetKind::LocalDamage)))
    );
    assert!(reader.handle.holds_root());
    let beside = w.open(r, reader_spec(dir.path()));
    assert!(
        beside
            .handle
            .generation
            .dir
            .starts_with(stream_root(dir.path()).join("reader-1"))
    );
    assert!(!reader.handle.disconnect(Holder::Host));
    assert!(!reader.handle.holds_root());
    w.retire(r, &beside);
    let again = w.open(r, reader_spec(dir.path()));
    assert!(
        again
            .handle
            .generation
            .dir
            .starts_with(stream_root(dir.path()).join("reader-0"))
    );
}

/// A reader `ursula_open` handed out that no connection opens retires after the hand-out window,
/// and one a connection opened stays.
#[test]
fn an_unconnected_reader_retires_after_the_hand_out_window() {
    let dir = TempDir::new("driver-reader-unconnected");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let idle = w.open_result(r, reader_spec(dir.path())).unwrap();
    let now = w.now;
    w.c(r).unconnected(idle.binding, now);
    w.run_for(UNCONNECTED_MS - 1_000);
    assert!(matches!(idle.handle.life(), Life::HandedOut { .. }));
    w.run_for(2_000);
    assert_eq!(idle.handle.life(), Life::Retired);
    assert!(!idle.handle.holds_root());
    let used = w.open(r, reader_spec(dir.path()));
    let now = w.now;
    w.c(r).unconnected(used.binding, now);
    w.run_for(UNCONNECTED_MS + 1_000);
    assert_eq!(used.handle.life(), Life::Live);
    assert!(used.handle.holds_root());
}

/// A reader's result that nobody took is given up: the binding retires and its directory unlocks.
#[test]
fn a_cancelled_reader_result_nobody_took_retires() {
    let dir = TempDir::new("driver-reader-cancel");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let r = w.core();
    let open = OpenId(w.id());
    let now = w.now;
    let ios = w.c(r).open(open, reader_spec(dir.path()), now);
    w.queue_ios(r, ios);
    w.run_for(1_000);
    let binding = w
        .events(r)
        .into_iter()
        .find_map(|e| match e {
            DriverEvent::Opened {
                open: o,
                result: Ok(binding),
            } if o == open => Some(binding),
            _ => None,
        })
        .expect("the reader bound");
    let now = w.now;
    let ios = w.c(r).cancel(open, now);
    w.queue_ios(r, ios);
    w.run_for(1_000);
    assert!(
        w.events(r)
            .iter()
            .any(|e| matches!(e, DriverEvent::Retired { binding: b } if *b == binding))
    );
    assert!(w.c(r).take_opened(open).is_none());
    let root = Root::reader(dir.path(), &canonical_path(URL).unwrap()).unwrap();
    assert_eq!(root.path(), stream_root(dir.path()).join("reader-0"));
}

/// Every reader directory held: the open is `in_use`.
#[test]
fn a_reader_with_every_reader_directory_held_is_in_use() {
    let dir = TempDir::new("driver-reader-in-use");
    let mut w = World::new();
    let o = w.core();
    let _owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let name = canonical_path(URL).unwrap();
    let held: Vec<Root> = (0..64)
        .map(|_| Root::reader(dir.path(), &name).unwrap())
        .collect();
    let r = w.core();
    let e = kind(w.open_result(r, reader_spec(dir.path())));
    assert!(matches!(&*e, OpenError::InUse { .. }), "{e}");
    drop(held);
    w.open(r, reader_spec(dir.path()));
}

/// A reader's open in progress works in a reader directory of its own: an owner's reopen beside a
/// reset binding's connections still shares the root instead of meeting `in_use`.
#[test]
fn a_reader_open_in_progress_does_not_keep_an_owner_reopen_from_the_root() {
    let dir = TempDir::new("driver-reader-busy");
    let mut w = World::new();
    let o = w.core();
    let owner = owner_with_a_commit(&mut w, dir.path(), o, 1);
    let now = w.now;
    w.c(o)
        .reset_binding(owner.binding, ResetKind::LocalDamage, now);
    // A reader's open on the same core, held at its first disk job.
    w.hold = Some(|job| matches!(job, DiskJob::Prepare { .. }));
    let open = OpenId(w.id());
    let ios = w.c(o).open(open, reader_spec(dir.path()), now);
    w.queue_ios(o, ios);
    w.deliver();
    assert_eq!(w.held.len(), 1);
    w.hold = None;
    let reopened = w.open(o, spec(dir.path(), 1));
    assert_ne!(reopened.handle.generation.dir, owner.handle.generation.dir);
    w.release_held();
    w.run_for(1_000);
    let reader = w.c(o).take_opened(open).unwrap().unwrap();
    assert_eq!(reader.handle.role, Role::Reader);
}
