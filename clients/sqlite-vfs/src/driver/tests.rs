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

use super::BindingId;
use super::Bound;
use super::DiskIo;
use super::DriverEvent;
use super::EventLog;
use super::Io;
use super::Life;
use super::OpenId;
use super::SyncError;
use super::Ticket;
use super::UNCONNECTED_MS;
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
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::materialize::LocalError;
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
use crate::replicator::Pending;
use crate::replicator::ResetKind;
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
    /// Runs on the server after the next HEAD it answers.
    after_head: Option<ServerHook>,
    /// Another deployment, which every request reaches while `misrouted`.
    other: ModelServer,
    misrouted: bool,
    next_id: u64,
    /// The binding of each ticket `sync_to` handed out.
    tickets: HashMap<Ticket, BindingId>,
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
            other: ModelServer::new(ClusterId::new("cluster-b").unwrap(), 9),
            misrouted: false,
            hold: None,
            held: Vec::new(),
            next_id: 1,
            tickets: HashMap::new(),
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
        if self.misrouted {
            let delivered = self.other.exchange(PATH, request);
            let Delivered::Answered { answer, .. } = delivered else {
                panic!("the other deployment answers");
            };
            return Exchange::Answered(Box::new(answer.parse(request)));
        }
        let delivered = self.server.exchange(PATH, request);
        if request.method == Method::Head
            && let Some(hook) = self.after_head.take()
        {
            hook(&mut self.server);
        }
        match delivered {
            Delivered::Answered { answer, .. } => {
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
                .min();
            match next {
                Some(at) if at <= until => {
                    let at = at.max(self.now);
                    self.server.advance(at - self.now);
                    self.now = at;
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
        assert!(bound.handle.connect());
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
        assert!(bound.handle.disconnect(), "the last connection");
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
    assert!(bound.handle.disconnect());
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
    assert!(bound.handle.disconnect());
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
        w.hold = Some(|job| matches!(job, DiskJob::Install { .. }));
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
/// offset, fsynced, so the next open's test fails the same way (design §6.2, D5).
#[test]
fn a_lossy_restore_resets_rewound_and_the_reopen_is_refused() {
    let dir = TempDir::new("driver-rewound");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let acked = bound.handle.acked();
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
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(
        matches!(
            &*e,
            OpenError::StreamState(StreamState::TokenChanged { .. })
        ),
        "{e}"
    );
    assert!(generation.exists());
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
    assert!(bound.handle.disconnect());
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

/// A crash leaves a commit pending; the stream then changes history and loses nothing. The log's
/// ACK may trail what the stream acknowledged, so the reopen refuses `stream_state` and keeps the
/// files, sending nothing and rolling nothing (salvage comes later in M2).
#[test]
fn a_reopen_after_an_inheriting_history_is_refused_while_commits_are_pending() {
    let dir = TempDir::new("driver-reopen-rekey-pending");
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
    w.commit(c, &bound, 2).unwrap();
    let generation = bound.handle.generation.dir.clone();
    let before = bound.handle.incarnation();
    drop(bound);
    w.crash(c);
    w.server.history_event(HistoryEvent::NewHistory).unwrap();
    let c2 = w.core();
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(
        matches!(
            &*e,
            OpenError::StreamState(StreamState::TokenChanged { .. })
        ),
        "{e}"
    );
    assert!(w.claims(c2).is_empty(), "a refusal sends nothing");
    assert_eq!(segment_tokens(&generation), vec![before]);
}

/// A crash leaves a commit pending, and a history event that loses nothing lands right after the
/// reopen's HEAD: the takeover's claim meets the new token with a commit pending, so the open fails
/// `rewound` before the log rolls (its ACK fsynced, D5), and the next open refuses with the files
/// kept.
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
    w.commit(c, &bound, 2).unwrap();
    let generation = bound.handle.generation.dir.clone();
    let before = bound.handle.incarnation();
    drop(bound);
    w.crash(c);
    w.after_head = Some(Box::new(|server: &mut ModelServer| {
        server.history_event(HistoryEvent::NewHistory).unwrap();
    }));
    let c2 = w.core();
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(matches!(&*e, OpenError::Reset(ResetKind::Rewound)), "{e}");
    assert_eq!(segment_tokens(&generation), vec![before]);
    let c3 = w.core();
    let e = kind(w.open_result(c3, spec(dir.path(), 3)));
    assert!(
        matches!(
            &*e,
            OpenError::StreamState(StreamState::TokenChanged { .. })
        ),
        "{e}"
    );
    assert_eq!(segment_tokens(&generation), vec![before]);
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

/// A commit acknowledged inside the ACK lag, a crash, a restore to before it that still inherits
/// by the lagging ACK, and a foreign commit in the new history. The acknowledged commit is pending
/// in the log, so every reopen refuses and the generation, that commit included, stays whole:
/// re-keying would roll the log, and a later open would resume it and discard the commit.
#[test]
fn an_acknowledged_commit_behind_a_lagging_ack_keeps_the_generation_refused() {
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
    let mut kept = None;
    for n in 2..5 {
        let core = w.core();
        let e = kind(w.open_result(core, spec(dir.path(), n)));
        assert!(
            matches!(
                &*e,
                OpenError::StreamState(StreamState::TokenChanged { .. })
            ),
            "reopen {n}: {e}"
        );
        assert!(w.claims(core).is_empty(), "reopen {n} sent a claim");
        assert_eq!(segment_tokens(&generation), vec![before]);
        let holds_c1 = log_records(&generation)
            .iter()
            .any(|r| matches!(r, RecordBody::Commit { .. }));
        assert!(holds_c1, "reopen {n} lost the acknowledged commit");
        // The first reopen materialized the generation; the later ones change nothing.
        let now = files(dir.path());
        if let Some(kept) = &kept {
            assert_eq!(&now, kept, "reopen {n} changed the files");
        }
        kept = Some(now);
    }
    assert!(!w.stream_commits().contains(&c1));
}

/// A history that lost a commit the generation acknowledged is refused at open, its files kept
/// (salvage comes later in M2).
#[test]
fn a_reopen_after_a_lossy_history_is_still_refused_and_kept() {
    let dir = TempDir::new("driver-reopen-lossy");
    let mut w = World::new();
    let c = w.core();
    let bound = w.open(c, spec(dir.path(), 1));
    let checkpoint = w.server.checkpoint();
    w.commit(c, &bound, 1).unwrap();
    w.run_for(1_000);
    let now = w.now;
    assert!(bound.handle.disconnect());
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    let generation = bound.handle.generation.dir.clone();
    drop(bound);
    w.server
        .history_event(HistoryEvent::Restore(checkpoint))
        .unwrap();
    let c2 = w.core();
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(
        matches!(
            &*e,
            OpenError::StreamState(StreamState::TokenChanged { .. })
        ),
        "{e}"
    );
    assert!(generation.exists());
    assert!(w.claims(c2).is_empty(), "a refusal sends nothing");
}

#[test]
fn a_missing_stream_with_a_local_generation_is_not_found() {
    let (result, generation, _dir) = reopen_after(|w, _| w.server.delete(PATH).unwrap());
    let e = kind(result);
    assert!(matches!(*e, OpenError::NotFound), "{e}");
    assert!(generation.exists());
}

#[test]
fn an_unreadable_segment_header_is_local_setup() {
    let (result, _, _dir) = reopen_after(|_, generation| {
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
    let e = kind(result);
    assert!(matches!(*e, OpenError::LocalSetup(_)), "{e}");
}

// A db file that is not the generation's (plain SQLite copied over it) is refused before anything
// is written or sent: `local_setup`, the files as they were (a later version salvages it).
#[test]
fn a_replaced_db_file_is_refused_and_kept() {
    let (result, generation, _dir) = reopen_after(|_, generation| {
        let page = sqlite(Box::new([0; 4096]));
        std::fs::write(generation.join("db"), &page[..]).unwrap();
    });
    let e = kind(result);
    assert!(
        matches!(
            &*e,
            OpenError::LocalSetup(crate::open::LocalSetup::Local(LocalError::Unmarked { .. }))
        ),
        "{e}"
    );
    assert!(e.to_string().contains("before the 0.8 cache format"), "{e}");
    assert_eq!(
        std::fs::read(generation.join("db")).unwrap()[..16],
        SQLITE_MAGIC
    );
}

// A generation built from a snapshot holds no COMMIT record, only the ACK naming the snapshot.
// Its db file emptied is refused, never taken for a new database (whose pages the next commits
// would ship on top of the snapshot), and stays as it was found.
#[test]
fn an_emptied_db_of_a_snapshot_built_generation_is_refused() {
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
    let e = kind(w.open_result(c, spec(dir_b.path(), 3)));
    assert!(
        matches!(
            &*e,
            OpenError::LocalSetup(crate::open::LocalSetup::Local(LocalError::Replaced {
                found,
                ..
            })) if found.is_empty()
        ),
        "{e}"
    );
    assert_eq!(std::fs::metadata(generation.db()).unwrap().len(), 0);
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
fn a_reopen_while_old_handles_live_is_in_use() {
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
    // A reset binding's connections may still write its generation: its root stays locked until
    // the last of them closes.
    let now = w.now;
    w.c(c2)
        .reset_binding(reopened.binding, ResetKind::LocalDamage, now);
    assert_eq!(reopened.handle.reset(), Some(ResetKind::LocalDamage));
    let c3 = w.core();
    let e = kind(w.open_result(c3, spec(dir.path(), 3)));
    assert!(matches!(*e, OpenError::InUse { .. }), "{e}");
    assert!(!reopened.handle.disconnect());
    assert!(!reopened.handle.holds_root());
    let bound = w.open(c3, spec(dir.path(), 4));
    assert_eq!(bound.handle.pending_bytes(), 0);
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
    assert!(bound.handle.disconnect());
    assert_eq!(bound.handle.life(), Life::Retiring);
    assert!(bound.handle.revive());
    let ios = w.c(c).retire(bound.binding, now);
    w.queue_ios(c, ios);
    w.run_for(1_000);
    assert_eq!(bound.handle.life(), Life::Live);
    assert!(bound.handle.holds_root());
    // Closed, then opened again by a connection before the retire arrives: the same.
    assert!(bound.handle.connect());
    assert!(bound.handle.disconnect(), "the last connection");
    assert!(bound.handle.connect(), "a retiring binding is taken back");
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
        !bound.handle.connect(),
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
    assert_eq!(bound.handle.life(), Life::Live);
    w.run_for(UNCONNECTED_MS / 2);
    assert_eq!(bound.handle.life(), Life::Retired);
    assert!(!bound.handle.holds_root());
    // One a connection opened stays.
    let c2 = w.core();
    let other = w.open(c2, spec(dir.path(), 2));
    w.run_for(2 * UNCONNECTED_MS);
    assert_eq!(other.handle.life(), Life::Live);
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
            assert!(bound.handle.connect());
            assert!(bound.handle.disconnect());
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
        assert!(bound.handle.connect());
        assert!(bound.handle.disconnect());
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
            assert!(bound.handle.connect());
            assert!(bound.handle.disconnect());
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
    let prepared = !matches!(w.held[0].1.job, DiskJob::Prepare(_));
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
        // The root stays locked while the job may still write.
        assert!(matches!(
            Root::lock(dir_a.path(), &canonical_path(URL).unwrap()),
            Err(crate::generation::GenerationError::InUse { .. })
        ));
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
        tombstone_at_open(|job| matches!(job, DiskJob::Prepare(_)), ending);
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
    assert!(bound.handle.disconnect());
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

/// A caught-up record after the log's ACK, and a restore of a checkpoint from before it, which
/// inherits by that ACK: the db holds a page the restored history lacks, so the open refuses and
/// keeps the files.
#[test]
fn a_caught_up_record_after_the_ack_is_refused_at_open() {
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
    let before = segment_tokens(&generation);
    let c2 = w.core();
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(
        matches!(
            &*e,
            OpenError::StreamState(StreamState::TokenChanged { .. })
        ),
        "{e}"
    );
    assert!(w.claims(c2).is_empty());
    assert_eq!(segment_tokens(&generation), before);
}

/// The same with the restore right after the open's HEAD: the open resumes, its takeover's claim
/// meets the new token whose history inherits by the log's ACK, and it fails `rewound` before the
/// log rolls instead of re-keying.
#[test]
fn a_caught_up_record_after_the_ack_fails_a_takeover_that_meets_a_restore() {
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
    let e = kind(w.open_result(c2, spec(dir.path(), 2)));
    assert!(matches!(&*e, OpenError::Reset(ResetKind::Rewound)), "{e}");
    assert_eq!(segment_tokens(&generation), before);
}
