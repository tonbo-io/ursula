//! The driver's deterministic core: every open and bound database of the process, single-threaded,
//! with the time passed in.
//!
//! Each call takes `now_ms` and returns what to carry out ([`Io`]): HTTP requests, whose answers
//! come back through [`Core::completed`], and disk jobs ([`super::disk`]), whose outcomes come
//! back through [`Core::disk_done`]. [`Core::tick`] runs whatever [`Core::next_deadline`] named.
//! The core carries out the replicator's actions in order:
//!
//! - A CLAIM record is appended and fsynced before anything after it, its send among them. A
//!   re-key rolls the log to a segment naming the new token first, and the binding's token
//!   follows it (design §6.2).
//! - ACK records are written lazily, at most once a second ([`ACK_INTERVAL_MS`]), and whenever a
//!   binding resets or retires; during an open they are written at once. A `rewound` reset also
//!   fsyncs its ACK, so the next open tests inheritance against the exact acknowledged offset, and
//!   so does a clean retire, so a cleanly closed log holds no commit after its latest ACK but ones
//!   never acknowledged.
//!   After an ACK, a log of more than one segment is trimmed (F3).
//! - A takeover's catch-up in place and its rebuild (after a divergence or a `410`) are disk jobs;
//!   then [`Event::CaughtUp`] names the last position the frames took, and the replicator binds
//!   ([`Action::Bound`]). A catch-up in place goes read by read, each answered before the
//!   replicator reads on.
//! - A failed log write or fsync resets the binding `local_damage`, and a panic inside a
//!   binding's step resets it `internal`.
//! - A reset takes the log's lock once it is marked, so no commit lands in the log after it (G8).
//!   An open of a stream first carries out a reset its connections marked that the core has not
//!   heard of yet, then shares the stream's root while a reset binding's connections hold it, and
//!   its first disk job copies the generation they hold (design §5.6).
//! - A provisional open (design §5.5: the stream unreachable, a trusted generation with a CLAIM
//!   record) binds at once and resolves before any answer. Its replicator claims only once the
//!   stream answered and a pre-check found nothing foreign (design §7). Commits log and wait,
//!   and so does `synced()`. The binding is live once its claim applies and its reconcile
//!   completes ([`Action::Bound`]), and resets `fenced` or `stale` when another owner moved the
//!   stream meanwhile.
//!
//! `synced()` waits for every commit at or below its target, at most the last commit the commit
//! path recorded on the handle; a target the core has not been told is logged yet waits for that
//! `Logged` before the replicator sees it.
//!
//! A reader binding (design D6) has no claim and no replicator: it binds as soon as its open is
//! done, its `synced()` resolves at once, and it retires as soon as its last connection closes.
//! Randomness (backoff jitter, lineages) comes from a seeded [`Rng`], so a run is reproducible.
//!
//! [`Action::Bound`]: crate::replicator::Action::Bound

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;

use ursula_headers::Epoch;
use ursula_headers::Incarnation;

use super::ACK_INTERVAL_MS;
use super::Attach;
use super::Bind;
use super::BindingHandle;
use super::BindingId;
use super::Bound;
use super::DiskIo;
use super::DriverEvent;
use super::EventLog;
use super::HttpIo;
use super::Io;
use super::IoId;
use super::Kept;
use super::Life;
use super::LogPoisoned;
use super::OpenId;
use super::Replication;
use super::Rng;
use super::Role;
use super::SyncError;
use super::Ticket;
use super::UNCONNECTED_MS;
use super::disk::DiskDone;
use super::disk::DiskJob;
use super::disk::NewBuild;
use super::disk::caught_up;
use crate::codec::frame;
use crate::codec::segment::RecordBody;
use crate::config::Step;
use crate::config::abort_at;
use crate::generation::CanonicalPath;
use crate::generation::Fate;
use crate::generation::Root;
use crate::generation::canonical_path;
use crate::generation::root_dir;
use crate::guard;
use crate::ids::Activation;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::log;
use crate::log::Level;
use crate::materialize::ForeignFrame;
use crate::materialize::LocalError;
use crate::materialize::Materialized;
use crate::open::Build;
use crate::open::BuildStep;
use crate::open::OpenError;
use crate::open::OpenSpec;
use crate::open::OpenStep;
use crate::open::Opening;
use crate::open::Published;
use crate::open::Ready;
use crate::open::Unavailable;
use crate::open::reader::ReaderReady;
use crate::open::salvage_manifest;
use crate::options::Access;
use crate::replicator::Action;
use crate::replicator::ClaimKind;
use crate::replicator::Event;
use crate::replicator::LocalState;
use crate::replicator::MAX_BATCH_BYTES;
use crate::replicator::Pending;
use crate::replicator::Refusal;
use crate::replicator::Replicator;
use crate::replicator::RequestId;
use crate::replicator::ResetKind;
use crate::replog::Log;
use crate::replog::LogError;
use crate::salvage::Reason;
use crate::transport::Attempt;
use crate::wire::Exchange;
use crate::wire::Request;

/// A local operation that failed: the binding resets `local_damage`.
#[derive(Debug, thiserror::Error)]
enum LocalFailure {
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Poisoned(#[from] LogPoisoned),
    #[error(transparent)]
    Local(#[from] LocalError),
    #[error("a disk job holds the log")]
    Busy,
}

/// Whose request or disk job an [`IoId`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
    Open(OpenId),
    Replicator(BindingId, RequestId),
    /// A rebuild's read.
    Rebuild(BindingId),
    /// A binding's disk job.
    Disk(BindingId),
}

/// An open before its takeover.
#[derive(Debug)]
struct OpenSlot {
    opening: Box<Opening>,
    /// The timer of a resend; `None` while a request or a disk job is out.
    wake_at: Option<u64>,
    /// A disk job of the open is out.
    busy: bool,
    /// The open failed while a disk job was out: it resolves with this once the job returns, so
    /// nothing touches the stream's files after the root unlocks.
    failing: Option<OpenError>,
}

impl OpenSlot {
    fn deadline(&self) -> Option<u64> {
        if self.failing.is_some() {
            return None;
        }
        min(self.wake_at, Some(self.opening.deadline_ms()))
    }
}

/// An open of a stream under another cache root than its binding's, which `ursula_open` gave up
/// ([`BindingHandle::give_up`]): it starts once that binding drained and retired, or fails
/// `in_use` at its deadline.
#[derive(Debug)]
struct Deferred {
    spec: OpenSpec,
    /// The binding given up.
    after: BindingId,
    /// Its cache root.
    bound: PathBuf,
    deadline_ms: u64,
}

/// An opening binding's generation.
#[derive(Debug)]
enum Local {
    Ready(Box<Materialized>),
    /// A disk job holds the generation or the build.
    Busy,
    /// Neither held nor out: a job returned without one, or a build failed, and the open is
    /// about to fail.
    Idle,
    /// A rebuild between reads.
    Rebuilding {
        build: Box<Build>,
        wake_at: Option<u64>,
    },
}

#[derive(Debug)]
enum Stage {
    /// Claiming, reconciling and catching up before the open resolves.
    Opening {
        open: OpenId,
        deadline_ms: u64,
        root: Arc<Root>,
        local: Local,
    },
    Bound {
        handle: Arc<BindingHandle>,
    },
}

/// The ACK records of a binding: the latest one not yet written, and when the last went out.
#[derive(Debug, Default)]
struct Acks {
    unwritten: Option<RecordBody>,
    last_ms: Option<u64>,
    flush_at: Option<u64>,
}

#[derive(Debug)]
struct Binding {
    url: Arc<str>,
    name: CanonicalPath,
    /// The cache root the open named.
    dir: PathBuf,
    replicator: Replicator,
    /// The replicator's timer.
    wake_at: Option<u64>,
    stage: Stage,
    /// Logged commits not yet acknowledged, with their frame lengths.
    unacked: VecDeque<(Position, u64)>,
    /// The last COMMIT record this core was told of: the commit path's `Logged`, or what a
    /// catch-up or rebuild logged.
    last_commit: Position,
    acks: Acks,
    /// `synced()` tickets not resolved yet. They arrive in ticket order, with targets that never
    /// decrease, so they resolve in ticket order (a prefix of them at a time) and
    /// [`DriverEvent::Synced`] can report one watermark per binding.
    tickets: BTreeSet<Ticket>,
    /// The highest target a ticket of this binding was given.
    last_target: Position,
    /// Every ticket of this binding up to this one resolved.
    synced_through: Option<Ticket>,
    /// The highest ticket that resolved.
    resolved_max: Ticket,
    /// `synced()` calls whose target is past `last_commit`, waiting for its `Logged`.
    held: Vec<(Ticket, Position)>,
    max_pending_bytes: u64,
    /// The hand-out it waits on a connection for.
    unconnected: Unconnected,
    /// The open failed while a disk job held the generation: the binding stays, its root locked
    /// and its replicator ignored, until the job returns, and the open resolves then.
    failing: Option<OpenError>,
    /// Where the open got the local database, for status.
    attach: Attach,
}

impl Binding {
    fn with_log<R>(
        &mut self,
        f: impl FnOnce(&mut Log) -> Result<R, LogError>,
    ) -> Result<R, LocalFailure> {
        match &mut self.stage {
            Stage::Opening {
                local: Local::Ready(m),
                ..
            } => Ok(f(&mut m.log)?),
            Stage::Opening { .. } => Err(LocalFailure::Busy),
            Stage::Bound { handle } => {
                let mut log = handle.log()?;
                Ok(f(&mut log)?)
            }
        }
    }

    fn handle(&self) -> Option<&Arc<BindingHandle>> {
        match &self.stage {
            Stage::Bound { handle } => Some(handle),
            Stage::Opening { .. } => None,
        }
    }

    fn write_ack(&mut self, now_ms: u64) -> Result<(), LocalFailure> {
        let Some(body) = self.acks.unwritten.take() else {
            return Ok(());
        };
        self.acks.flush_at = None;
        self.acks.last_ms = Some(now_ms);
        self.with_log(|log| {
            log.append(&body)?;
            log.trim().map(drop)
        })
    }

    fn persist_ack(&mut self, body: RecordBody, now_ms: u64) -> Result<(), LocalFailure> {
        self.acks.unwritten = Some(body);
        let due = match (&self.stage, self.acks.last_ms) {
            (Stage::Opening { .. }, _) | (Stage::Bound { .. }, None) => now_ms,
            (Stage::Bound { .. }, Some(last)) => last.saturating_add(ACK_INTERVAL_MS),
        };
        if now_ms >= due {
            self.write_ack(now_ms)
        } else {
            self.acks.flush_at = Some(due);
            Ok(())
        }
    }

    /// Every record up to `acked` is acknowledged: their bytes leave the backlog.
    fn acknowledged(&mut self, acked: Position) {
        let mut freed: u64 = 0;
        while let Some(&(position, len)) = self.unacked.front() {
            if position > acked {
                break;
            }
            freed = freed.saturating_add(len);
            self.unacked.pop_front();
        }
        if let Some(handle) = self.handle() {
            handle.remove_pending(freed);
        }
    }

    fn deadline(&self) -> Option<u64> {
        if self.failing.is_some() {
            return None;
        }
        let mut at = [
            min(self.wake_at, self.unconnected.deadline()),
            self.acks.flush_at,
        ];
        if let Stage::Opening {
            deadline_ms, local, ..
        } = &self.stage
        {
            at[0] = min(at[0], Some(*deadline_ms));
            if let Local::Rebuilding { wake_at, .. } = local {
                at[1] = min(at[1], *wake_at);
            }
        }
        at.into_iter().flatten().min()
    }
}

/// A reader binding (design D6): its files, and the hand-out it waits on a connection for. No
/// claim, no replicator, no commit.
#[derive(Debug)]
struct Reader {
    handle: Arc<BindingHandle>,
    unconnected: Unconnected,
}

/// Since when `ursula_open` handed a binding out (an owner's or a reader's) with no connection
/// open, and the hand-outs its handle had counted then: it retires [`UNCONNECTED_MS`] later
/// unless a connection opened meanwhile, or `ursula_open` handed it out again (whose own timer
/// runs then).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Unconnected(Option<(u64, u64)>);

impl Unconnected {
    fn since(now_ms: u64, handouts: u64) -> Self {
        Self(Some((now_ms, handouts)))
    }

    fn deadline(self) -> Option<u64> {
        self.0
            .map(|(since, _)| since.saturating_add(UNCONNECTED_MS))
    }

    /// Whether the hand-out is decided at `now_ms`: a connection of the host opened (the binding
    /// is no longer [`Life::HandedOut`]), or its time passed. Once decided it is cleared, and the
    /// binding retires unless a connection or a later hand-out keeps it
    /// ([`BindingHandle::idle_retire`]). True when it was decided now.
    fn settle(&mut self, handle: &BindingHandle, now_ms: u64) -> bool {
        let Some((since, handouts)) = self.0 else {
            return false;
        };
        if handle.handed_out() && now_ms < since.saturating_add(UNCONNECTED_MS) {
            return false;
        }
        self.0 = None;
        handle.idle_retire(handouts);
        true
    }
}

fn min(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

/// The deterministic core of the driver.
#[derive(Debug)]
pub(crate) struct Core {
    rng: Rng,
    next_id: u64,
    opens: BTreeMap<OpenId, OpenSlot>,
    bindings: BTreeMap<BindingId, Binding>,
    /// The reader bindings, by the same ids.
    readers: BTreeMap<BindingId, Reader>,
    ios: BTreeMap<IoId, Owner>,
    events: EventLog,
    /// Resolved opens not yet taken.
    opened: BTreeMap<OpenId, Result<Bound, Arc<OpenError>>>,
    /// Opens waiting for a binding of their stream under another cache root to retire.
    deferred: BTreeMap<OpenId, Deferred>,
    /// The roots this core's opens locked, by directory: a root stays locked while a binding, an
    /// open or a reset binding's connections hold it. An open of the stream shares it only when
    /// nothing but reset bindings' connections hold it, and otherwise locks it anew (`in_use`).
    /// Per core, so tests that run several cores as several processes keep them apart.
    roots: HashMap<PathBuf, Weak<Root>>,
    /// Test hook: the next CLAIM record's write fails.
    #[cfg(test)]
    pub(crate) fail_next_claim: bool,
}

impl Core {
    pub(crate) fn new(seed: [u8; 32]) -> Self {
        Self {
            rng: Rng::new(seed),
            next_id: 1,
            opens: BTreeMap::new(),
            bindings: BTreeMap::new(),
            readers: BTreeMap::new(),
            ios: BTreeMap::new(),
            events: EventLog::default(),
            opened: BTreeMap::new(),
            deferred: BTreeMap::new(),
            roots: HashMap::new(),
            #[cfg(test)]
            fail_next_claim: false,
        }
    }

    fn id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    fn lineage(&mut self) -> Lineage {
        Lineage(self.rng.bytes16())
    }

    fn send(
        &mut self,
        owner: Owner,
        url: &Arc<str>,
        request: Request,
        attempt: Attempt,
        ios: &mut Vec<Io>,
    ) {
        let id = IoId(self.id());
        self.ios.insert(id, owner);
        ios.push(Io::Http(HttpIo {
            id,
            url: Arc::clone(url),
            request,
            attempt,
        }));
    }

    fn disk(&mut self, owner: Owner, job: DiskJob, ios: &mut Vec<Io>) {
        let id = IoId(self.id());
        self.ios.insert(id, owner);
        ios.push(Io::Disk(DiskIo { id, job }));
    }

    /// The events from `cursor` on (at most the last [`super::EVENTS`], acknowledgements
    /// coalesced), and the cursor after them.
    pub(crate) fn events_since(&self, cursor: u64) -> (Vec<DriverEvent>, u64) {
        self.events.since(cursor)
    }

    /// The earliest time [`Core::tick`] has something to do.
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        let opens = self.opens.values().map(OpenSlot::deadline);
        let bindings = self.bindings.values().map(Binding::deadline);
        // A deferred open whose binding retired starts at once.
        let deferred = self.deferred.values().map(|d| {
            Some(if self.bindings.contains_key(&d.after) {
                d.deadline_ms
            } else {
                0
            })
        });
        let readers = self.readers.values().map(|r| r.unconnected.deadline());
        opens
            .chain(bindings)
            .chain(deferred)
            .chain(readers)
            .flatten()
            .min()
    }

    /// The binding of the stream at `url` given up for an open under another cache root
    /// ([`BindingHandle::give_up`]), still draining, and its cache root.
    fn given_up(&self, url: &str) -> Option<(BindingId, PathBuf)> {
        let name = canonical_path(url).ok()?;
        self.bindings.iter().find_map(|(id, b)| {
            let handle = b.handle().filter(|h| h.life() == Life::GivenUp)?;
            (b.name == name).then(|| (*id, handle.dir.clone()))
        })
    }

    /// Defers open `open` until binding `after`, given up, drained and retired. The wait counts
    /// toward the open's time.
    fn defer(
        &mut self,
        open: OpenId,
        spec: OpenSpec,
        (after, bound): (BindingId, PathBuf),
        now_ms: u64,
    ) -> Vec<Io> {
        let deadline_ms = now_ms.saturating_add(spec.timeout_ms);
        self.deferred.insert(open, Deferred {
            spec,
            after,
            bound,
            deadline_ms,
        });
        self.settle(after, now_ms);
        let mut ios = Vec::new();
        self.start_deferred(now_ms, &mut ios);
        ios
    }

    /// Starts the deferred opens whose binding retired, and fails those past their deadline.
    fn start_deferred(&mut self, now_ms: u64, ios: &mut Vec<Io>) {
        let opens: Vec<OpenId> = self.deferred.keys().copied().collect();
        for open in opens {
            let Some(d) = self.deferred.remove(&open) else {
                continue;
            };
            if !self.bindings.contains_key(&d.after) {
                let Deferred {
                    mut spec,
                    deadline_ms,
                    ..
                } = d;
                spec.timeout_ms = deadline_ms.saturating_sub(now_ms);
                ios.extend(self.open(open, spec, now_ms));
            } else if now_ms >= d.deadline_ms {
                self.opened(
                    open,
                    Err(OpenError::BoundElsewhere {
                        bound: d.bound,
                        requested: d.spec.dir,
                        kept: Kept::Draining,
                    }),
                );
            } else {
                self.deferred.insert(open, d);
            }
        }
    }

    /// Starts open `open` (design §5.5). Its outcome comes as [`DriverEvent::Opened`].
    ///
    /// While a binding of the stream given up for an open under another cache root drains, the
    /// open waits for it to retire ([`Core::defer`]), so no two bindings of a stream ever run in
    /// the process, whichever open of it comes first.
    ///
    /// A binding of the stream that its connections reset, and whose reset has not reached the
    /// core yet, resets first, so its log takes no append once the open may copy it (G8). Then the
    /// open shares the stream's root when only reset bindings' connections still hold it: with
    /// any other binding or open of the stream left in the core (one still draining, or a
    /// tombstone whose disk job is out), it locks the root anew and is refused `in_use`, so it
    /// never runs beside a replicator or a job that writes the stream's files. A reader's open
    /// locks a reader directory of its own and shares nothing.
    pub(crate) fn open(&mut self, open: OpenId, spec: OpenSpec, now_ms: u64) -> Vec<Io> {
        if let Some(given_up) = self.given_up(&spec.url) {
            return self.defer(open, spec, given_up, now_ms);
        }
        let mut ios = Vec::new();
        let root = match canonical_path(&spec.url) {
            Ok(_) if spec.access == Access::Reader => None,
            Ok(name) => {
                let reset: Vec<(BindingId, ResetKind)> = self
                    .bindings
                    .iter()
                    .filter(|(_, b)| b.name == name)
                    .filter_map(|(id, b)| Some((*id, b.handle()?.reset()?)))
                    .collect();
                for (binding, kind) in reset {
                    self.reset(binding, kind, now_ms);
                }
                // A reader's open in progress works in a reader directory of its own: it never
                // keeps an owner's open from sharing the root.
                let busy = self.bindings.values().any(|b| b.name == name)
                    || self.opens.values().any(|slot| {
                        *slot.opening.name() == name
                            && matches!(slot.opening.access(), Access::Owner { .. })
                    });
                if busy {
                    None
                } else {
                    self.roots
                        .get(&root_dir(&spec.dir, &name))
                        .and_then(Weak::upgrade)
                }
            }
            // The open fails on the URL itself.
            Err(_invalid) => None,
        };
        let step = guard::contain(
            "driver_open",
            || OpenStep::Fail(OpenError::Panicked),
            || Opening::start(spec, root, now_ms),
        );
        self.open_step(open, step, now_ms, &mut ios);
        ios
    }

    /// The root an open locked, or shares.
    fn remember_root(&mut self, root: &Arc<Root>) {
        self.roots.retain(|_, held| held.strong_count() > 0);
        self.roots
            .insert(root.path().to_owned(), Arc::downgrade(root));
    }

    fn open_step(&mut self, open: OpenId, step: OpenStep, now_ms: u64, ios: &mut Vec<Io>) {
        match step {
            OpenStep::Send {
                opening,
                request,
                attempt,
            } => {
                let url: Arc<str> = Arc::from(opening.url());
                self.send(Owner::Open(open), &url, request, attempt, ios);
                self.opens.insert(open, OpenSlot {
                    opening,
                    wake_at: None,
                    busy: false,
                    failing: None,
                });
            }
            OpenStep::Timer {
                opening,
                at_ms,
                jitter_ms,
            } => {
                let wake_at = at_ms.saturating_add(self.rng.up_to(jitter_ms));
                self.opens.insert(open, OpenSlot {
                    opening,
                    wake_at: Some(wake_at),
                    busy: false,
                    failing: None,
                });
            }
            OpenStep::Disk { opening, job } => {
                self.disk(Owner::Open(open), job, ios);
                self.opens.insert(open, OpenSlot {
                    opening,
                    wake_at: None,
                    busy: true,
                    failing: None,
                });
            }
            OpenStep::Takeover(takeover) => self.takeover(open, *takeover, now_ms, ios),
            OpenStep::Provisional { ready, claim } => {
                self.provisional(open, *ready, claim, now_ms, ios)
            }
            OpenStep::Reader(ready) => self.bind_reader(open, *ready, now_ms),
            OpenStep::Fail(e) => self.opened(open, Err(e)),
        }
    }

    /// A reader's open is done: it binds at once, with no claim and no replicator, and holds its
    /// reader directory's lock until it retires.
    fn bind_reader(&mut self, open: OpenId, ready: ReaderReady, now_ms: u64) {
        let ReaderReady {
            name,
            dir,
            root,
            m,
            cluster_id,
            incarnation,
            closed,
            attach,
        } = ready;
        let (last, _) = caught_up(&m);
        let Materialized {
            generation, log, ..
        } = *m;
        let bind = Bind {
            generation,
            name,
            cluster_id,
            incarnation,
            log,
            pending_bytes: 0,
            max_pending_bytes: 0,
            acked: last,
            attach,
            role: Role::Reader,
            closed,
            dir,
            // A reader claims nothing, so it has no claim to defer.
            provisional: false,
        };
        let handle = Arc::new(BindingHandle::new(bind, root));
        handle.record_commit(last);
        handle.set_replication(Replication {
            state: "none",
            request_limit: MAX_BATCH_BYTES,
            ..Replication::default()
        });
        let binding = BindingId(self.id());
        self.readers.insert(binding, Reader {
            handle: Arc::clone(&handle),
            // A result nobody takes or cancels retires too.
            unconnected: Unconnected::since(now_ms, 0),
        });
        self.opened(open, Ok(Bound { binding, handle }));
    }

    /// A reader binding whose last connection closed, or that nobody connected to, retires now:
    /// it has nothing to drain. False when a connection or an open took it back meanwhile.
    fn retire_reader(&mut self, binding: BindingId) -> bool {
        let retired = self
            .readers
            .get(&binding)
            .is_some_and(|r| r.handle.try_retire());
        if retired && let Some(reader) = self.readers.remove(&binding) {
            reader.handle.release_root();
            self.events.push(DriverEvent::Retired { binding });
        }
        retired
    }

    fn opened(&mut self, open: OpenId, result: Result<Bound, OpenError>) {
        let result = result.map_err(Arc::new);
        self.events.push(DriverEvent::Opened {
            open,
            result: result
                .as_ref()
                .map(|bound| bound.binding)
                .map_err(Arc::clone),
        });
        self.opened.insert(open, result);
    }

    /// The outcome of open `open`, once it resolved. It is handed out once.
    pub(crate) fn take_opened(&mut self, open: OpenId) -> Option<Result<Bound, Arc<OpenError>>> {
        self.opened.remove(&open)
    }

    fn takeover(&mut self, open: OpenId, ready: Ready, now_ms: u64, ios: &mut Vec<Io>) {
        let kind = ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        };
        let (binding, actions) =
            self.insert(open, ready, |local| Replicator::activate(kind, local));
        self.contained(binding, now_ms, |core| {
            core.execute(binding, actions, now_ms, ios);
            core.settle(binding, now_ms);
        });
    }

    /// A provisional open (design §5.5): the binding binds now and the open resolves with it,
    /// before any answer. Its replicator's pre-check (a HEAD, then reads up to the tail) goes out,
    /// and nothing is logged until the stream answered: its claim, the log's latest `claim` sent
    /// again, follows only a pre-check that found nothing foreign (design §7).
    fn provisional(
        &mut self,
        open: OpenId,
        ready: Ready,
        claim: (Epoch, Activation),
        now_ms: u64,
        ios: &mut Vec<Io>,
    ) {
        let (binding, actions) =
            self.insert(open, ready, |local| Replicator::provisional(local, claim));
        log::emit(Level::Info, "provisional", &[
            ("binding", &binding.0),
            ("claim", &claim.0),
        ]);
        self.contained(binding, now_ms, |core| {
            core.bind(binding, now_ms, true);
            core.execute(binding, actions, now_ms, ios);
            core.settle(binding, now_ms);
        });
    }

    /// A binding of open `open`, its replicator started by `start`: the actions it starts with.
    fn insert(
        &mut self,
        open: OpenId,
        ready: Ready,
        start: impl FnOnce(LocalState) -> (Replicator, Vec<Action>),
    ) -> (BindingId, Vec<Action>) {
        let Ready {
            spec,
            name,
            root,
            m,
            local,
            deadline_ms,
            attach,
        } = ready;
        let unacked = local
            .pending
            .iter()
            .map(|p| {
                let len = u64::try_from(p.frame.len()).unwrap_or(u64::MAX);
                (p.id.position, len)
            })
            .collect();
        // Everything up to the acknowledged position is a commit or older; the pending commits
        // follow it.
        let last_commit = local.pending.last().map_or(local.acked, |p| p.id.position);
        let (replicator, actions) = start(local);
        let binding = BindingId(self.id());
        self.bindings.insert(binding, Binding {
            url: Arc::from(spec.url.as_str()),
            name,
            dir: spec.dir.clone(),
            replicator,
            wake_at: None,
            stage: Stage::Opening {
                open,
                deadline_ms,
                root,
                local: Local::Ready(m),
            },
            unacked,
            last_commit,
            acks: Acks::default(),
            tickets: BTreeSet::new(),
            last_target: Position::default(),
            synced_through: None,
            resolved_max: Ticket(0),
            held: Vec::new(),
            max_pending_bytes: spec.max_pending_bytes,
            unconnected: Unconnected::default(),
            failing: None,
            attach,
        });
        (binding, actions)
    }

    /// Runs a step of `binding`: a panic in it resets the binding `internal`, which wakes its
    /// waiters, instead of unwinding out of the driver.
    fn contained(&mut self, binding: BindingId, now_ms: u64, step: impl FnOnce(&mut Self)) {
        let panicked = guard::contain(
            "driver",
            || true,
            || {
                step(self);
                false
            },
        );
        if panicked {
            self.reset(binding, ResetKind::Internal, now_ms);
        }
    }

    /// One event into a binding's replicator, and its actions carried out.
    fn drive(&mut self, binding: BindingId, event: Event, now_ms: u64, ios: &mut Vec<Io>) {
        self.contained(binding, now_ms, |core| {
            let Some(b) = core
                .bindings
                .get_mut(&binding)
                .filter(|b| b.failing.is_none())
            else {
                return;
            };
            let actions = b.replicator.on(event, now_ms);
            core.execute(binding, actions, now_ms, ios);
            core.settle(binding, now_ms);
        });
    }

    fn execute(
        &mut self,
        binding: BindingId,
        actions: Vec<Action>,
        now_ms: u64,
        ios: &mut Vec<Io>,
    ) {
        let mut bound = false;
        for action in actions {
            let Some(b) = self.bindings.get_mut(&binding) else {
                return;
            };
            match action {
                Action::Send {
                    id,
                    request,
                    attempt,
                } => {
                    if frame::part_index(&request.body).is_some_and(|index| index > 0) {
                        abort_at(Step::PartSent);
                    }
                    let url = Arc::clone(&b.url);
                    self.send(Owner::Replicator(binding, id), &url, request, attempt, ios);
                }
                Action::PersistClaim { epoch, activation } => {
                    #[cfg(test)]
                    let fail = std::mem::take(&mut self.fail_next_claim);
                    // Durable before any later action, its send among them.
                    let written = b.with_log(|log| {
                        #[cfg(test)]
                        if fail {
                            log.fail_next_append = Some(0);
                        }
                        log.append(&RecordBody::Claim { epoch, activation })?;
                        log.sync()
                    });
                    if let Err(e) = written {
                        return self.local_damage(binding, &e, now_ms);
                    }
                }
                Action::ReKey {
                    incarnation,
                    epoch,
                    activation,
                } => {
                    // Durable before any later action, its send among them.
                    let rekeyed = b.with_log(|log| log.rekey(incarnation, epoch, activation));
                    if let Err(e) = rekeyed {
                        return self.local_damage(binding, &e, now_ms);
                    }
                    if let Some(handle) = b.handle() {
                        handle.set_incarnation(incarnation);
                    }
                }
                Action::PersistAck {
                    acked,
                    acked_offset,
                    log_bytes,
                } => {
                    let body = RecordBody::Ack {
                        acked,
                        acked_offset,
                        log_bytes,
                    };
                    if let Err(e) = b.persist_ack(body, now_ms) {
                        return self.local_damage(binding, &e, now_ms);
                    }
                }
                Action::Wake { acked, .. } => {
                    b.acknowledged(acked);
                    if let Some(handle) = b.handle() {
                        handle.set_acked(acked);
                        let log_bytes = b.replicator.ctx().map_or(0, |ctx| ctx.log_bytes);
                        handle.snapshots.acknowledged(log_bytes);
                        self.events.push(DriverEvent::Acked { binding, acked });
                    }
                }
                Action::Boundary(receipt) => {
                    if let Some(handle) = b.handle() {
                        handle.snapshots.snapper.receipt(receipt);
                    }
                }
                Action::Synced { ticket } => {
                    self.resolve(binding, Ticket(ticket), Ok(()));
                }
                // A continuation claim discarded commits a `synced()` waits for: the binding is
                // stale, and its reset event fails every ticket with that one kind.
                Action::Abandoned { .. } => {
                    return self.reset(binding, ResetKind::Stale, now_ms);
                }
                Action::Timer { at_ms, jitter_ms } => {
                    let at = at_ms.saturating_add(self.rng.up_to(jitter_ms));
                    if let Some(b) = self.bindings.get_mut(&binding) {
                        b.wake_at = min(b.wake_at, Some(at));
                    }
                }
                Action::Condition(condition) => {
                    if b.handle().is_some() {
                        self.events
                            .push(DriverEvent::Condition { binding, condition });
                    }
                }
                Action::Apply(frames) => self.apply(binding, frames, now_ms, ios),
                Action::Rebuild {
                    discard_from,
                    salvage,
                    to,
                    epoch,
                    activation,
                    incarnation,
                    cluster_id,
                } => {
                    let lineage = self.lineage();
                    let salvage = salvage.then_some(incarnation);
                    self.rebuild(
                        binding,
                        discard_from,
                        salvage,
                        now_ms,
                        ios,
                        |root, name, first, fate| NewBuild {
                            root,
                            name,
                            lineage,
                            cluster_id,
                            incarnation,
                            first,
                            claim: Some((epoch, activation)),
                            end: to,
                            fate,
                        },
                    );
                }
                Action::Bound { .. } => bound = true,
                Action::Reset(kind) => return self.reset(binding, kind, now_ms),
                Action::Refuse(Refusal::ClaimRaces) => {
                    return self.fail_open(binding, OpenError::ClaimRaces);
                }
            }
        }
        if bound {
            self.bind(binding, now_ms, false);
        }
    }

    /// One read of a takeover's catch-up in place, with nothing pending (design §5.6): a disk job
    /// logs the frames, writes an ACK covering them and fsyncs before it applies them, and
    /// [`Event::CaughtUp`] lets the replicator send the next read.
    fn apply(
        &mut self,
        binding: BindingId,
        frames: Vec<ForeignFrame>,
        now_ms: u64,
        ios: &mut Vec<Io>,
    ) {
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        let Stage::Opening { local, root, .. } = &mut b.stage else {
            return self.reset(binding, ResetKind::Internal, now_ms);
        };
        let root = Arc::clone(root);
        // Checked first, so a mismatch never leaves a `Busy` with no job out.
        if !matches!(local, Local::Ready(_)) {
            return self.reset(binding, ResetKind::Internal, now_ms);
        }
        let Local::Ready(m) = std::mem::replace(local, Local::Busy) else {
            return self.reset(binding, ResetKind::Internal, now_ms);
        };
        self.disk(
            Owner::Disk(binding),
            DiskJob::CatchUp { m, frames, root },
            ios,
        );
    }

    /// A takeover's divergence (the pending tail from `discard_from` on dropped), a `410`, or a
    /// catch-up in place that grew past the database: a new generation is built from the stream in
    /// `gen-<id32>.tmp` and published by one rename once complete, and [`Event::CaughtUp`]
    /// follows (design §5.6).
    ///
    /// The old generation stays whole until that rename. A rebuild begins only once it is decided:
    /// the replicator found the pending tail diverged from the stream (it is discarded whatever
    /// happens next), or the stream no longer holds what catching it up would read. A failed
    /// build fails the open and leaves the old generation as it was. With `salvage` (the stream's
    /// token), the publish salvages the old generation (`history_changed`) instead of trashing it.
    fn rebuild(
        &mut self,
        binding: BindingId,
        discard_from: Option<Position>,
        salvage: Option<Incarnation>,
        now_ms: u64,
        ios: &mut Vec<Io>,
        job: impl FnOnce(Arc<Root>, CanonicalPath, Position, Fate) -> NewBuild,
    ) {
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        let name = b.name.clone();
        let Stage::Opening { root, local, .. } = &mut b.stage else {
            return self.reset(binding, ResetKind::Internal, now_ms);
        };
        if !matches!(local, Local::Ready(_)) {
            return self.reset(binding, ResetKind::Internal, now_ms);
        }
        let Local::Ready(old) = std::mem::replace(local, Local::Busy) else {
            return self.reset(binding, ResetKind::Internal, now_ms);
        };
        // Positions continue above the last one logged.
        let first = old.log.next_position();
        let fate = match salvage {
            Some(stream) => {
                Fate::Salvage(salvage_manifest(&old, Reason::HistoryChanged, Some(stream)))
            }
            None => Fate::Trash,
        };
        // Its files stay; only this handle on them closes.
        drop(old);
        let job = job(Arc::clone(root), name, first, fate);
        if let Some(from) = discard_from {
            b.unacked.retain(|(position, _)| *position < from);
        }
        self.disk(Owner::Disk(binding), DiskJob::NewBuild(job), ios);
    }

    fn build_step(
        &mut self,
        binding: BindingId,
        build: Box<Build>,
        step: BuildStep,
        ios: &mut Vec<Io>,
    ) {
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        let Stage::Opening { local, .. } = &mut b.stage else {
            return;
        };
        match step {
            BuildStep::Send { request, attempt } => {
                *local = Local::Rebuilding {
                    build,
                    wake_at: None,
                };
                let url = Arc::clone(&b.url);
                self.send(Owner::Rebuild(binding), &url, request, attempt, ios);
            }
            BuildStep::Timer { at_ms, jitter_ms } => {
                let at = at_ms.saturating_add(self.rng.up_to(jitter_ms));
                *local = Local::Rebuilding {
                    build,
                    wake_at: Some(at),
                };
            }
            BuildStep::Install(body) => {
                *local = Local::Busy;
                self.disk(Owner::Disk(binding), DiskJob::Install { build, body }, ios);
            }
            BuildStep::Restart => {
                *local = Local::Busy;
                self.disk(Owner::Disk(binding), DiskJob::Restart(build), ios);
            }
            BuildStep::Apply(frames) => {
                *local = Local::Busy;
                self.disk(Owner::Disk(binding), DiskJob::Chunk { build, frames }, ios);
            }
            BuildStep::Done => {
                *local = Local::Busy;
                self.disk(Owner::Disk(binding), DiskJob::Publish(build), ios);
            }
            // A rebuild reads under its claim: it fails `deleted` instead, so this never comes.
            BuildStep::Missing => {
                *local = Local::Idle;
                self.fail_open(binding, OpenError::Reset(ResetKind::Deleted));
            }
            BuildStep::Fail(e) => {
                *local = Local::Idle;
                self.fail_open(binding, e);
            }
        }
    }

    /// Resolves a ticket. A success moves the binding's watermark ([`DriverEvent::Synced`]) up to
    /// the ticket, or to just below the first ticket still waiting; a failure is reported alone.
    fn resolve(&mut self, binding: BindingId, ticket: Ticket, outcome: Result<(), SyncError>) {
        match (outcome, self.bindings.get_mut(&binding)) {
            (Ok(()), Some(b)) => {
                b.tickets.remove(&ticket);
                b.resolved_max = b.resolved_max.max(ticket);
                let through = match b.tickets.first() {
                    Some(&Ticket(first)) => Ticket(b.resolved_max.0.min(first.saturating_sub(1))),
                    None => b.resolved_max,
                };
                if b.synced_through.is_none_or(|before| through > before) {
                    b.synced_through = Some(through);
                    self.events.push(DriverEvent::Synced { binding, through });
                }
            }
            (Ok(()), None) => {}
            (Err(error), b) => {
                if let Some(b) = b {
                    b.tickets.remove(&ticket);
                }
                self.events.push(DriverEvent::SyncFailed { ticket, error });
            }
        }
    }

    fn local_damage(&mut self, binding: BindingId, e: &LocalFailure, now_ms: u64) {
        log::emit(Level::Error, "local_damage", &[
            ("binding", &binding.0),
            ("error", e),
        ]);
        self.reset(binding, ResetKind::LocalDamage, now_ms);
    }

    /// The binding resets: its latest ACK is written, its tickets fail, and it retires. Its root
    /// unlocks once the last handle drops. During an open, the open fails.
    ///
    /// A `rewound` reset also fsyncs the log, so the next open tests inheritance against the exact
    /// acknowledged offset (design §6.2, D5): an ACK lost to a power loss would let it re-key a
    /// generation that lost data. When that fsync fails the reset is `local_damage` instead.
    fn reset(&mut self, binding: BindingId, kind: ResetKind, now_ms: u64) {
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        let opening = matches!(b.stage, Stage::Opening { .. });
        let written = if opening {
            // An open writes its ACKs at once. A disk job that holds the log fsyncs it before it
            // returns.
            match kind {
                ResetKind::Rewound => match b.with_log(Log::sync) {
                    Err(LocalFailure::Busy) => Ok(()),
                    other => other,
                },
                _ => Ok(()),
            }
        } else {
            b.write_ack(now_ms).and_then(|()| match kind {
                ResetKind::Rewound => b.with_log(Log::sync),
                _ => Ok(()),
            })
        };
        let kind = match written {
            Ok(()) => kind,
            Err(e) if kind == ResetKind::Rewound => {
                log::emit(Level::Error, "local_damage", &[
                    ("binding", &binding.0),
                    ("error", &e),
                ]);
                ResetKind::LocalDamage
            }
            Err(e) => {
                log::emit(Level::Warn, "ack_lost", &[
                    ("binding", &binding.0),
                    ("error", &e),
                ]);
                kind
            }
        };
        if opening {
            return self.fail_open(binding, OpenError::Reset(kind));
        }
        let Some(mut b) = self.bindings.remove(&binding) else {
            return;
        };
        // Its waiting tickets fail with the binding's one `Reset` event.
        b.tickets.clear();
        match b.stage {
            Stage::Opening { open, .. } => self.opened(open, Err(OpenError::Reset(kind))),
            Stage::Bound { handle } => self.reset_bound(binding, &handle, kind),
        }
    }

    /// A bound binding, an owner's or a reader's, gone from the core, resets: marked, its log
    /// final, retired, and reported.
    fn reset_bound(&mut self, binding: BindingId, handle: &BindingHandle, kind: ResetKind) {
        handle.mark_reset(kind);
        // A barrier for the connections, which check the reset under the log's lock
        // (`Binding::log_unless_reset`): a COMMIT, ACK or BASE record whose append took the lock
        // before the mark is written, and every later one is refused. The core itself appends
        // nothing more, so the log is final once an open may copy it (G8).
        drop(handle.log());
        // Its connections hold its generation, and the root, until the last one closes.
        handle.reset_retired();
        handle.snapshots.snapper.stop();
        self.events.push(DriverEvent::Reset {
            binding,
            cause: kind,
        });
        self.events.push(DriverEvent::Retired { binding });
    }

    /// An opening binding fails its open and goes. While a disk job holds its generation it stays
    /// as a tombstone, its root locked, and the open resolves when the job returns: the next open
    /// of the stream never runs beside a job still writing its files.
    fn fail_open(&mut self, binding: BindingId, e: OpenError) {
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        if b.failing.is_some() {
            return;
        }
        if let Stage::Opening {
            local: Local::Busy, ..
        } = b.stage
        {
            b.failing = Some(e);
            return;
        }
        let Some(b) = self.bindings.remove(&binding) else {
            return;
        };
        if let Stage::Opening { open, .. } = b.stage {
            self.opened(open, Err(e));
        }
    }

    /// After a step: what status reports of the replicator is published, and a retiring binding
    /// with nothing pending retires.
    fn settle(&mut self, binding: BindingId, now_ms: u64) {
        let Some(b) = self.bindings.get(&binding) else {
            return;
        };
        let Some(handle) = b.handle() else {
            return;
        };
        let (state, epoch) = match &b.replicator {
            Replicator::Activating { .. } => ("activating", None),
            Replicator::Streaming { epoch, .. } => ("streaming", Some(epoch.get())),
            Replicator::Reset(_) => ("reset", None),
            Replicator::Refused(_) => ("refused", None),
            Replicator::Stopped => ("stopped", None),
        };
        let ctx = b.replicator.ctx();
        handle.set_replication(Replication {
            state,
            epoch,
            condition: ctx.and_then(|ctx| ctx.condition.clone()),
            pending: b.unacked.len(),
            log_bytes: ctx.map_or(0, |ctx| ctx.log_bytes),
            request_limit: ctx.map_or(MAX_BATCH_BYTES, |ctx| ctx.limits.request()),
        });
        // Nothing pending, even while a claim or a provisional pre-check is out: a claim's CLAIM
        // record is logged before it goes out, so the next open knows the claim frame as its own.
        let drained = b.unacked.is_empty()
            && matches!(
                b.replicator,
                Replicator::Streaming { .. } | Replicator::Activating { .. }
            );
        if drained && handle.leaving() && handle.try_retire() {
            self.retire_now(binding, now_ms);
        }
    }

    /// The replicator bound ([`Action::Bound`]), or a provisional open binds before its claim
    /// (`provisional`): the open resolves with a handle that now holds the log and the root's lock.
    /// A binding bound already is live from now on.
    fn bind(&mut self, binding: BindingId, now_ms: u64, provisional: bool) {
        if let Some(Stage::Bound { handle }) = self.bindings.get(&binding).map(|b| &b.stage) {
            if handle.set_live() {
                log::emit(Level::Info, "live", &[("binding", &binding.0)]);
            }
            return;
        }
        let Some(mut b) = self.bindings.remove(&binding) else {
            return;
        };
        // Bound, the replicator streams with its context.
        let Some(incarnation) = b.replicator.ctx().map(|c| c.incarnation) else {
            self.bindings.insert(binding, b);
            return self.fail_open(binding, OpenError::Panicked);
        };
        // A result nobody takes or cancels retires too.
        b.unconnected = Unconnected::since(now_ms, 0);
        let Stage::Opening {
            open,
            root,
            local: Local::Ready(m),
            ..
        } = b.stage
        else {
            self.bindings.insert(binding, b);
            return;
        };
        let Materialized {
            generation, log, ..
        } = *m;
        let pending = b
            .unacked
            .iter()
            .fold(0u64, |sum, (_, len)| sum.saturating_add(*len));
        let ctx = b.replicator.ctx();
        let bind = Bind {
            generation,
            name: b.name.clone(),
            cluster_id: ctx.map(|c| c.cluster_id.to_string()).unwrap_or_default(),
            incarnation,
            log,
            pending_bytes: pending,
            max_pending_bytes: b.max_pending_bytes,
            acked: ctx.map_or(Position(0), |c| c.acked),
            attach: b.attach,
            provisional,
            dir: b.dir.clone(),
            role: Role::Owner,
            // An owner's open refuses a closed stream.
            closed: false,
        };
        let handle = Arc::new(BindingHandle::new(bind, root));
        handle.record_commit(b.last_commit);
        b.stage = Stage::Bound {
            handle: Arc::clone(&handle),
        };
        self.bindings.insert(binding, b);
        self.opened(open, Ok(Bound { binding, handle }));
    }

    fn retire_now(&mut self, binding: BindingId, now_ms: u64) {
        let Some(mut b) = self.bindings.remove(&binding) else {
            return;
        };
        drop(b.replicator.on(Event::Stop, now_ms));
        // A clean retire leaves an ACK at the exact acknowledged position, fsynced: a cleanly
        // closed log then holds no commit after its latest ACK but ones never acknowledged, so a
        // history event that loses nothing re-keys it at the next open instead of salvaging.
        let exact = b
            .write_ack(now_ms)
            .and_then(|()| b.with_log(|log| log.sync()));
        if let Err(e) = exact {
            log::emit(Level::Warn, "ack_lost", &[
                ("binding", &binding.0),
                ("error", &e),
            ]);
        }
        if let Some(handle) = b.handle() {
            handle.snapshots.snapper.stop();
            handle.release_root();
        }
        for ticket in std::mem::take(&mut b.tickets) {
            self.resolve(binding, ticket, Err(SyncError::NotBound));
        }
        self.events.push(DriverEvent::Retired { binding });
    }

    /// What came of request `io`.
    pub(crate) fn completed(&mut self, io: IoId, exchange: Exchange, now_ms: u64) -> Vec<Io> {
        let mut ios = Vec::new();
        match self.ios.remove(&io) {
            None | Some(Owner::Disk(_)) => {}
            Some(Owner::Open(open)) => {
                if let Some(slot) = self.opens.remove(&open) {
                    let lineage = self.lineage();
                    let step = guard::contain(
                        "driver_open",
                        || OpenStep::Fail(OpenError::Panicked),
                        || slot.opening.answered(&exchange, lineage, now_ms),
                    );
                    self.open_step(open, step, now_ms, &mut ios);
                }
            }
            Some(Owner::Replicator(binding, id)) => {
                self.drive(binding, Event::Answer { id, exchange }, now_ms, &mut ios);
            }
            Some(Owner::Rebuild(binding)) => {
                self.contained(binding, now_ms, |core| {
                    let Some(b) = core.bindings.get_mut(&binding) else {
                        return;
                    };
                    let Stage::Opening { local, .. } = &mut b.stage else {
                        return;
                    };
                    let Local::Rebuilding { mut build, .. } = std::mem::replace(local, Local::Idle)
                    else {
                        return;
                    };
                    let step = build.answered(&exchange, now_ms);
                    core.build_step(binding, build, step, &mut ios);
                });
            }
        }
        ios
    }

    /// What came of disk job `io`.
    pub(crate) fn disk_done(&mut self, io: IoId, done: DiskDone, now_ms: u64) -> Vec<Io> {
        let mut ios = Vec::new();
        match self.ios.remove(&io) {
            None | Some(Owner::Replicator(..) | Owner::Rebuild(_)) => {}
            Some(Owner::Open(open)) => {
                if let Some(slot) = self.opens.remove(&open) {
                    if let Some(e) = slot.failing {
                        // What the job worked on drops here, its root lock with it.
                        drop((slot.opening, done));
                        self.opened(open, Err(e));
                        return ios;
                    }
                    if let DiskDone::Prepared(Ok(prepared)) = &done {
                        self.remember_root(prepared.root());
                    }
                    let step = guard::contain(
                        "driver_open",
                        || OpenStep::Fail(OpenError::Panicked),
                        || slot.opening.disk_done(done),
                    );
                    self.open_step(open, step, now_ms, &mut ios);
                }
            }
            Some(Owner::Disk(binding)) => {
                if self
                    .bindings
                    .get(&binding)
                    .is_some_and(|b| b.failing.is_some())
                    && let Some(b) = self.bindings.remove(&binding)
                {
                    drop(done);
                    if let (Stage::Opening { open, .. }, Some(e)) = (b.stage, b.failing) {
                        self.opened(open, Err(e));
                    }
                    return ios;
                }
                self.contained(binding, now_ms, |core| {
                    core.binding_disk_done(binding, done, now_ms, &mut ios);
                });
            }
        }
        ios
    }

    fn binding_disk_done(
        &mut self,
        binding: BindingId,
        done: DiskDone,
        now_ms: u64,
        ios: &mut Vec<Io>,
    ) {
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        let Stage::Opening { local, .. } = &mut b.stage else {
            return self.reset(binding, ResetKind::Internal, now_ms);
        };
        // The job returned: whatever it held comes back with `done`, or is gone.
        *local = Local::Idle;
        match done {
            DiskDone::Built(Ok(build))
            | DiskDone::Chunk {
                build,
                result: Ok(()),
            } => {
                let step = build.next();
                self.build_step(binding, build, step, ios);
            }
            DiskDone::Published(Ok(Published { m, attach, .. })) => {
                b.attach = attach;
                let (last, log_bytes) = caught_up(&m);
                let lineage = m.generation.lineage;
                *local = Local::Ready(m);
                // Every record of the new log is a stream frame, acknowledged.
                b.last_commit = last;
                let event = Event::CaughtUp {
                    last,
                    log_bytes,
                    lineage,
                };
                self.drive(binding, event, now_ms, ios);
            }
            DiskDone::CaughtUp {
                m,
                outcome: Ok((last, log_bytes)),
            } => {
                let lineage = m.generation.lineage;
                *local = Local::Ready(m);
                b.last_commit = last;
                let event = Event::CaughtUp {
                    last,
                    log_bytes,
                    lineage,
                };
                self.drive(binding, event, now_ms, ios);
            }
            DiskDone::CaughtUp {
                outcome: Err(e), ..
            } if e.is_stream_content() => {
                log::emit(Level::Error, "stream_content", &[
                    ("binding", &binding.0),
                    ("error", &e),
                ]);
                self.reset(binding, ResetKind::StreamState, now_ms);
            }
            DiskDone::CaughtUp {
                outcome: Err(e), ..
            } => {
                self.local_damage(binding, &LocalFailure::Local(e), now_ms);
            }
            DiskDone::Built(Err(e))
            | DiskDone::Chunk { result: Err(e), .. }
            | DiskDone::Published(Err(e))
            | DiskDone::Prepared(Err(e)) => self.fail_open(binding, e),
            DiskDone::Prepared(Ok(_)) | DiskDone::Panicked => {
                self.reset(binding, ResetKind::Internal, now_ms);
            }
        }
    }

    /// Runs what is due at `now_ms`: resends, deadlines, ACK flushes, replicator timers.
    pub(crate) fn tick(&mut self, now_ms: u64) -> Vec<Io> {
        let mut ios = Vec::new();
        self.start_deferred(now_ms, &mut ios);
        let opens: Vec<OpenId> = self.opens.keys().copied().collect();
        for open in opens {
            let Some(slot) = self.opens.remove(&open) else {
                continue;
            };
            if slot.failing.is_some() {
                self.opens.insert(open, slot);
            } else if now_ms >= slot.opening.deadline_ms() {
                let e = slot.opening.expired();
                if slot.busy {
                    self.opens.insert(open, OpenSlot {
                        failing: Some(e),
                        ..slot
                    });
                } else {
                    self.opened(open, Err(e));
                }
            } else if slot.wake_at.is_some_and(|at| now_ms >= at) {
                let step = guard::contain(
                    "driver_open",
                    || OpenStep::Fail(OpenError::Panicked),
                    || slot.opening.tick(),
                );
                self.open_step(open, step, now_ms, &mut ios);
            } else {
                self.opens.insert(open, slot);
            }
        }
        let bindings: Vec<BindingId> = self.bindings.keys().copied().collect();
        for binding in bindings {
            self.contained(binding, now_ms, |core| {
                core.tick_binding(binding, now_ms, &mut ios);
            });
        }
        let readers: Vec<BindingId> = self.readers.keys().copied().collect();
        for binding in readers {
            let Some(reader) = self.readers.get_mut(&binding) else {
                continue;
            };
            if reader.unconnected.settle(&reader.handle, now_ms) {
                self.retire_reader(binding);
            }
        }
        ios
    }

    fn tick_binding(&mut self, binding: BindingId, now_ms: u64, ios: &mut Vec<Io>) {
        let Some(b) = self
            .bindings
            .get_mut(&binding)
            .filter(|b| b.failing.is_none())
        else {
            return;
        };
        if let Stage::Bound { handle } = &b.stage
            && b.unconnected.settle(handle, now_ms)
        {
            self.settle(binding, now_ms);
            let Some(b) = self.bindings.get_mut(&binding) else {
                return;
            };
            if b.failing.is_some() {
                return;
            }
        }
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        if let Stage::Opening {
            deadline_ms, local, ..
        } = &mut b.stage
        {
            if now_ms >= *deadline_ms {
                let condition = match local {
                    Local::Rebuilding { build, .. } => build.condition(),
                    Local::Ready(_) | Local::Busy | Local::Idle => b
                        .replicator
                        .ctx()
                        .and_then(|ctx| ctx.condition.as_ref())
                        .map_or(Unavailable::Retrying, Unavailable::from),
                };
                return self.fail_open(binding, OpenError::Unavailable(condition));
            }
            if let Local::Rebuilding { wake_at, .. } = local
                && wake_at.is_some_and(|at| now_ms >= at)
                && let Local::Rebuilding { build, .. } = std::mem::replace(local, Local::Idle)
            {
                let step = build.next();
                self.build_step(binding, build, step, ios);
            }
        }
        let Some(b) = self.bindings.get_mut(&binding) else {
            return;
        };
        if b.acks.flush_at.is_some_and(|at| now_ms >= at)
            && let Err(e) = b.write_ack(now_ms)
        {
            return self.local_damage(binding, &e, now_ms);
        }
        if b.wake_at.is_some_and(|at| now_ms >= at) {
            b.wake_at = None;
            self.drive(binding, Event::Tick, now_ms, ios);
        }
    }

    /// A commit was appended to the binding's log (no fsync).
    pub(crate) fn logged(&mut self, binding: BindingId, rec: Pending, now_ms: u64) -> Vec<Io> {
        let mut ios = Vec::new();
        let Some(b) = self.bindings.get_mut(&binding) else {
            return ios;
        };
        if b.handle().is_none() {
            return ios;
        }
        // The commit path charged it to the pending bytes already.
        let len = u64::try_from(rec.frame.len()).unwrap_or(u64::MAX);
        b.unacked.push_back((rec.id.position, len));
        b.last_commit = b.last_commit.max(rec.id.position);
        let last_commit = b.last_commit;
        let mut released = Vec::new();
        b.held.retain(|&(ticket, target)| {
            let ready = target <= last_commit;
            if ready {
                released.push((ticket, target));
            }
            !ready
        });
        self.drive(binding, Event::Logged(rec), now_ms, &mut ios);
        for (ticket, target) in released {
            let event = Event::Sync {
                ticket: ticket.0,
                target,
            };
            self.drive(binding, event, now_ms, &mut ios);
        }
        ios
    }

    /// `synced()` with ticket `ticket` (design §8.2): it waits for every commit at or below
    /// `target`, taken at most as the last COMMIT position the commit path recorded on the handle
    /// ([`BindingHandle::last_commit`], also the default). A target past the last commit this core
    /// was told of waits for its `Logged`. The outcome comes as the binding's watermark
    /// ([`DriverEvent::Synced`]), as [`DriverEvent::SyncFailed`], or as the binding's
    /// [`DriverEvent::Reset`]. Tickets must arrive in ticket order ([`super::runtime::Driver::sync`]).
    pub(crate) fn sync(
        &mut self,
        ticket: Ticket,
        binding: BindingId,
        target: Option<Position>,
        now_ms: u64,
    ) -> Vec<Io> {
        let mut ios = Vec::new();
        // A reader holds no commit to wait for: its tickets resolve at once, in ticket order.
        if let Some(reader) = self.readers.get(&binding) {
            if reader.handle.life() == Life::Retired {
                self.resolve(binding, ticket, Err(SyncError::NotBound));
            } else {
                self.events.push(DriverEvent::Synced {
                    binding,
                    through: ticket,
                });
            }
            return ios;
        }
        let live = self
            .bindings
            .get_mut(&binding)
            .filter(|b| b.handle().is_some_and(|h| h.life() != Life::Retired));
        let Some(b) = live else {
            self.resolve(binding, ticket, Err(SyncError::NotBound));
            return ios;
        };
        // No commit lies past the last one the commit path recorded: a target beyond it waits
        // for that one, not for a commit that may never come.
        let recorded = b
            .handle()
            .map_or(b.last_commit, |handle| handle.last_commit());
        // Never below an earlier ticket's target: tickets resolve in ticket order.
        let target = target
            .map_or(recorded, |target| target.min(recorded))
            .max(b.last_target);
        b.last_target = target;
        b.tickets.insert(ticket);
        if target > b.last_commit {
            b.held.push((ticket, target));
            return ios;
        }
        let event = Event::Sync {
            ticket: ticket.0,
            target,
        };
        self.drive(binding, event, now_ms, &mut ios);
        ios
    }

    /// A batch must end exactly at `position` (a snapshot's `k`, design §4.11).
    pub(crate) fn boundary(
        &mut self,
        binding: BindingId,
        position: Position,
        now_ms: u64,
    ) -> Vec<Io> {
        let mut ios = Vec::new();
        self.drive(binding, Event::Boundary(position), now_ms, &mut ios);
        ios
    }

    /// A snapshot published (or found superseded) covers `log_bytes` of the frame bytes counted
    /// toward the next one.
    pub(crate) fn snapshotted(
        &mut self,
        binding: BindingId,
        log_bytes: u64,
        now_ms: u64,
    ) -> Vec<Io> {
        let mut ios = Vec::new();
        self.drive(binding, Event::Snapshotted { log_bytes }, now_ms, &mut ios);
        ios
    }

    /// The binding's last connection closed ([`BindingHandle::disconnect`] marked it retiring): it
    /// drains, then retires, unless a connection or an open takes it back first. A stale call, after
    /// one did, changes nothing.
    pub(crate) fn retire(&mut self, binding: BindingId, now_ms: u64) -> Vec<Io> {
        if self.readers.contains_key(&binding) {
            self.retire_reader(binding);
            return Vec::new();
        }
        if let Some(b) = self.bindings.get(&binding) {
            match &b.stage {
                Stage::Opening { .. } => {
                    self.fail_open(binding, OpenError::Cancelled);
                    return Vec::new();
                }
                // Its last connection closed and marked it retiring: it retires once drained,
                // unless a connection or an open takes it back first.
                Stage::Bound { .. } => {}
            }
        }
        self.settle(binding, now_ms);
        Vec::new()
    }

    /// `ursula_open` handed the binding out: it retires [`UNCONNECTED_MS`] from now unless a
    /// connection opens it meanwhile, so an open whose caller never connects does not hold the
    /// root for good.
    pub(crate) fn unconnected(&mut self, binding: BindingId, now_ms: u64) {
        if let Some(r) = self.readers.get_mut(&binding) {
            r.unconnected = Unconnected::since(now_ms, r.handle.handouts());
        }
        if let Some(b) = self.bindings.get_mut(&binding) {
            let handouts = b.handle().map_or(0, |h| h.handouts());
            b.unconnected = Unconnected::since(now_ms, handouts);
        }
    }

    /// A binding no connection holds (an open's result nobody took) drains, then retires.
    pub(crate) fn retire_untaken(&mut self, binding: BindingId, now_ms: u64) -> Vec<Io> {
        if let Some(Stage::Bound { handle }) = self.bindings.get(&binding).map(|b| &b.stage) {
            handle.retiring();
        }
        if let Some(reader) = self.readers.get(&binding) {
            reader.handle.retiring();
        }
        self.retire(binding, now_ms)
    }

    /// The binding's connections found it damaged (an unconfirmed commit, a failed fsync, a panic):
    /// it resets.
    pub(crate) fn reset_binding(&mut self, binding: BindingId, kind: ResetKind, now_ms: u64) {
        if let Some(reader) = self.readers.remove(&binding) {
            return self.reset_bound(binding, &reader.handle, kind);
        }
        self.reset(binding, kind, now_ms);
    }

    /// Gives up an open: one that has not resolved fails with the last condition it met, or
    /// `cancelled` when it met none (once its disk job, if any, returned), and a result nobody took
    /// goes, its binding retiring.
    pub(crate) fn cancel(&mut self, open: OpenId, now_ms: u64) -> Vec<Io> {
        if self.deferred.remove(&open).is_some() {
            self.opened(open, Err(OpenError::Cancelled));
            return Vec::new();
        }
        if let Some(result) = self.opened.remove(&open) {
            if let Ok(bound) = result {
                let binding = bound.binding;
                drop(bound);
                return self.retire_untaken(binding, now_ms);
            }
            return Vec::new();
        }
        if let Some(slot) = self.opens.get_mut(&open) {
            let given_up = slot.opening.given_up();
            if slot.busy {
                slot.failing.get_or_insert(given_up);
            } else {
                self.opens.remove(&open);
                self.opened(open, Err(given_up));
            }
            return Vec::new();
        }
        let binding = self.bindings.iter().find_map(|(id, b)| match b.stage {
            Stage::Opening { open: o, .. } if o == open => Some(*id),
            _ => None,
        });
        if let Some(binding) = binding {
            let given_up = self
                .bindings
                .get(&binding)
                .map_or(OpenError::Cancelled, |b| {
                    let seen = match &b.stage {
                        Stage::Opening {
                            local: Local::Rebuilding { build, .. },
                            ..
                        } => build.seen(),
                        _ => b
                            .replicator
                            .ctx()
                            .and_then(|ctx| ctx.condition.as_ref())
                            .map(Unavailable::from),
                    };
                    seen.map_or(OpenError::Cancelled, OpenError::Unavailable)
                });
            self.fail_open(binding, given_up);
        }
        Vec::new()
    }
}
