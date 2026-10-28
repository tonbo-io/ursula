//! The driver's runtime: one driver thread owns the [`Core`], blocking HTTP workers send its
//! requests, and its disk jobs run on threads of their own.
//!
//! Calls reach the driver thread over a channel and return at once: the driver allocates open ids
//! and tickets itself, so nothing waits for the driver thread, its local I/O or the network.
//! Outcomes are published to a table the callers wait on with a condition variable
//! ([`Driver::wait_open`], [`Driver::wait`], [`Driver::events`]).
//!
//! Each stream has two request queues, each with its worker: reads (the reconcile's, a build's,
//! the `synced()` probe) and everything else, so a stream's probe never waits behind its append,
//! and no stream waits behind another's. Each disk job runs on a thread of its own, so one
//! database's catch-up never stalls the others. A panic in the core is contained: inside a
//! binding's step it resets that binding `internal`.
//!
//! A token change reaches the core as [`Core::token_changed`] ([`token_changed`]): from
//! `ursula_set_token` at once, from the token file when a read finds a new token in it, a
//! request's or the driver thread's own check every [`crate::auth::FILE_CHECK_MS`] while requests
//! carry the file's token and an open or a writer may send them, and from an API key's refresher
//! when an exchange hands out a new token. The process's driver tells `auth` whether an open or a
//! binding may send requests ([`crate::auth::requests_may_go_out`]), from when an open is asked
//! for: the key is exchanged only then.
//!
//! The driver keeps every thread's `JoinHandle` and joins them when it drops.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use super::BindingId;
use super::Bound;
use super::DiskIo;
use super::DriverEvent;
use super::EventLog;
use super::HttpIo;
use super::Io;
use super::IoId;
use super::OpenId;
use super::SyncError;
use super::Ticket;
use super::core::Core;
use super::disk::DiskDone;
use crate::auth;
use crate::guard;
use crate::ids::Position;
use crate::open::OpenError;
use crate::open::OpenSpec;
use crate::replicator::Pending;
use crate::replicator::ResetKind;
use crate::transport::Transport;
use crate::transport::http::HttpTransport;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;

/// The transport of a stream URL.
pub(crate) type Transports = Arc<dyn Fn(&str) -> Arc<dyn Transport> + Send + Sync>;

/// The driver thread is gone (it panicked): nothing more is carried out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the driver thread is gone")]
pub(crate) struct DriverGone;

enum Command {
    Open {
        open: OpenId,
        spec: OpenSpec,
    },
    Logged {
        binding: BindingId,
        rec: Pending,
    },
    Sync {
        ticket: Ticket,
        binding: BindingId,
        target: Option<Position>,
    },
    Boundary {
        binding: BindingId,
        position: Position,
    },
    Snapshotted {
        binding: BindingId,
        log_bytes: u64,
    },
    Retire {
        binding: BindingId,
    },
    Reset {
        binding: BindingId,
        kind: ResetKind,
    },
    Unconnected {
        binding: BindingId,
    },
    Cancel {
        open: OpenId,
    },
    Completed {
        io: IoId,
        exchange: Exchange,
    },
    DiskDone {
        io: IoId,
        done: Box<DiskDone>,
    },
    TokenChanged,
    Shutdown,
}

/// What the driver thread publishes for the callers.
#[derive(Default)]
struct Published {
    events: EventLog,
    opened: BTreeMap<OpenId, Result<Bound, Arc<OpenError>>>,
    /// `synced()` tickets a caller waits for in [`Driver::wait`], with their binding. A ticket of a
    /// caller that waits 0 ms is never here: it learns the outcome from the events.
    waiting: BTreeMap<Ticket, BindingId>,
    /// Outcomes of waiting tickets, until [`Driver::wait`] takes them.
    synced: BTreeMap<Ticket, Result<(), SyncError>>,
    /// Opens given up before their result arrived: it is dropped, its binding retired.
    cancelled: BTreeSet<OpenId>,
    /// Cancelled opens whose caller waits for what they ended with ([`Driver::cancel_reporting`]).
    reporting: BTreeSet<OpenId>,
    /// What those opens ended with, until their caller takes it.
    given_up: BTreeMap<OpenId, Arc<OpenError>>,
    /// Opens started whose result has not arrived yet.
    pending: BTreeSet<OpenId>,
}

impl Published {
    /// Records the outcomes `event` gives the waiting tickets.
    fn settle(&mut self, event: &DriverEvent) {
        let mut outcome =
            |waiting: &mut BTreeMap<Ticket, BindingId>,
             pick: &dyn Fn(Ticket, BindingId) -> Option<Result<(), SyncError>>| {
                let done: Vec<(Ticket, Result<(), SyncError>)> = waiting
                    .iter()
                    .filter_map(|(&t, &b)| pick(t, b).map(|o| (t, o)))
                    .collect();
                for (t, o) in done {
                    waiting.remove(&t);
                    self.synced.insert(t, o);
                }
            };
        match *event {
            DriverEvent::Synced { binding, through } => outcome(&mut self.waiting, &|t, b| {
                (b == binding && t <= through).then_some(Ok(()))
            }),
            DriverEvent::SyncFailed { ticket, error } => {
                outcome(&mut self.waiting, &|t, _| {
                    (t == ticket).then_some(Err(error))
                });
            }
            DriverEvent::Reset { binding, cause } => outcome(&mut self.waiting, &|_, b| {
                (b == binding).then_some(Err(SyncError::Reset(cause)))
            }),
            _ => {}
        }
    }
}

struct Shared {
    state: Mutex<Published>,
    changed: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Published> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until `ready` holds or `timeout` passes.
    fn wait_for<T>(
        &self,
        timeout: Duration,
        mut ready: impl FnMut(&mut Published) -> Option<T>,
    ) -> Option<T> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.lock();
        loop {
            if let Some(found) = ready(&mut state) {
                return Some(found);
            }
            let left = match deadline {
                Some(deadline) => deadline.checked_duration_since(Instant::now())?,
                None => Duration::from_secs(3600),
            };
            if left.is_zero() {
                return None;
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

/// The process's driver.
pub(crate) struct Driver {
    commands: mpsc::Sender<Command>,
    transports: Transports,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    thread: Option<JoinHandle<()>>,
    role: Role,
}

/// Whether a driver is the process's ([`driver`]). Only that one tells `auth` whether requests
/// may go out ([`auth::requests_may_go_out`]), since the process's token is for its requests. A
/// driver a test starts of its own leaves it alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Process,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "drivers tests start of their own")
    )]
    OwnTest,
}

/// The process driver.
static DRIVER: OnceLock<Driver> = OnceLock::new();

/// The process driver, over HTTP, started on first use: by `ursula_open` or `ursula_events`, each
/// after it marked the process started (`db::start_process`, design: Forks).
pub(crate) fn driver() -> &'static Driver {
    DRIVER.get_or_init(|| {
        crate::db::expect_started("driver");
        let transports: Transports =
            Arc::new(|url: &str| Arc::new(HttpTransport::new(url)) as Arc<dyn Transport>);
        Driver::launch(transports, seed(), Role::Process)
    })
}

/// The token requests carry changed ([`crate::auth`]): the process driver, once started, sends
/// again at once every request a `401` blocked.
pub(crate) fn token_changed() {
    if let Some(driver) = DRIVER.get()
        && driver.command(Command::TokenChanged).is_err()
    {
        crate::log::emit(crate::log::Level::Warn, "driver_gone", &[]);
    }
}

/// 32 bytes from the random device, or from the clock and the process id without one.
fn seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    for half in seed.chunks_mut(16) {
        let bytes = crate::ids::random16().unwrap_or_else(|_unavailable| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            (nanos ^ u128::from(std::process::id())).to_le_bytes()
        });
        half.copy_from_slice(&bytes);
    }
    seed
}

impl Driver {
    /// Starts a driver of a test's own, beside the process's.
    #[cfg(test)]
    pub(crate) fn start(transports: Transports, seed: [u8; 32]) -> Self {
        Self::launch(transports, seed, Role::OwnTest)
    }

    /// Starts the driver thread.
    fn launch(transports: Transports, seed: [u8; 32], role: Role) -> Self {
        let (commands, inbox) = mpsc::channel::<Command>();
        let shared = Arc::new(Shared {
            state: Mutex::new(Published::default()),
            changed: Condvar::new(),
        });
        let published = Arc::clone(&shared);
        let completions = commands.clone();
        let own = Arc::clone(&transports);
        let spawned = std::thread::Builder::new()
            .name("ursula-vfs-driver".to_owned())
            .spawn(move || {
                let mut threads = Threads {
                    transports,
                    completions,
                    lanes: HashMap::new(),
                    handles: Vec::new(),
                };
                run(Core::new(seed), &inbox, &mut threads, &published, role);
                threads.join();
            });
        // Without its thread the driver's inbox is gone, so every call answers `DriverGone`.
        let thread = spawned
            .inspect_err(|e| {
                crate::log::emit(crate::log::Level::Error, "driver_thread_failed", &[(
                    "error", e,
                )]);
            })
            .ok();
        Self {
            commands,
            transports: own,
            shared,
            next_id: AtomicU64::new(1),
            thread,
            role,
        }
    }

    fn command(&self, command: Command) -> Result<(), DriverGone> {
        self.commands.send(command).map_err(|_gone| DriverGone)
    }

    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Starts an open; [`Driver::wait_open`] waits for its outcome.
    pub(crate) fn open(&self, spec: OpenSpec) -> Result<OpenId, DriverGone> {
        let open = OpenId(self.id());
        {
            // Requests may go out from the moment the open is asked for, before the driver
            // thread takes it: the key a host was handed for it is in use from then on.
            let mut state = self.shared.lock();
            state.pending.insert(open);
            if self.role == Role::Process {
                auth::requests_may_go_out(true);
            }
        }
        self.command(Command::Open { open, spec })?;
        Ok(open)
    }

    /// The open's outcome, waiting at most `timeout` for it. It is handed out once. When the
    /// time runs out the open is cancelled ([`Driver::cancel`]), so no result is left untaken.
    /// The open's outcome if it resolved, without waiting and without cancelling it otherwise
    /// (`ursula_open` with `timeout_ms` 0). It is handed out once.
    pub(crate) fn try_open(&self, open: OpenId) -> Option<Result<Bound, Arc<OpenError>>> {
        self.shared.lock().opened.remove(&open)
    }

    /// Waits at most `timeout` until open `open` resolved: its result waits to be taken
    /// ([`Driver::try_open`]), or it is no longer pending. True when it did.
    pub(crate) fn wait_resolved(&self, open: OpenId, timeout: Duration) -> bool {
        self.shared
            .wait_for(timeout, |state| {
                (state.opened.contains_key(&open) || !state.pending.contains(&open)).then_some(())
            })
            .is_some()
    }

    #[cfg(test)]
    pub(crate) fn wait_open(
        &self,
        open: OpenId,
        timeout: Duration,
    ) -> Option<Result<Bound, Arc<OpenError>>> {
        let result = self
            .shared
            .wait_for(timeout, |state| state.opened.remove(&open));
        if result.is_none() && self.cancel(open).is_err() {
            crate::log::emit(crate::log::Level::Warn, "driver_gone", &[]);
        }
        result
    }

    /// A commit was appended to the binding's log.
    pub(crate) fn logged(&self, binding: BindingId, rec: Pending) -> Result<(), DriverGone> {
        self.command(Command::Logged { binding, rec })
    }

    /// `synced()`. With `wait`, [`Driver::wait`] then waits for the ticket's outcome; without, the
    /// caller learns it from the events. Tickets are numbered and sent under one lock, so the core
    /// receives them in ticket order.
    pub(crate) fn sync(
        &self,
        binding: BindingId,
        target: Option<Position>,
        wait: bool,
    ) -> Result<Ticket, DriverGone> {
        let mut state = self.shared.lock();
        let ticket = Ticket(self.id());
        if wait {
            state.waiting.insert(ticket, binding);
        }
        let sent = self.command(Command::Sync {
            ticket,
            binding,
            target,
        });
        if sent.is_err() {
            state.waiting.remove(&ticket);
        }
        sent.map(|()| ticket)
    }

    /// The ticket's outcome, waiting at most `timeout` for it. It is handed out once.
    pub(crate) fn wait(&self, ticket: Ticket, timeout: Duration) -> Option<Result<(), SyncError>> {
        let outcome = self
            .shared
            .wait_for(timeout, |state| state.synced.remove(&ticket));
        if outcome.is_some() {
            return outcome;
        }
        // Timed out: nobody takes its outcome any more, unless it arrived just now.
        let mut state = self.shared.lock();
        state.waiting.remove(&ticket);
        state.synced.remove(&ticket)
    }

    /// The events from `cursor` on, waiting at most `timeout` for one.
    pub(crate) fn events(&self, cursor: u64, timeout: Duration) -> (Vec<DriverEvent>, u64) {
        self.shared
            .wait_for(timeout, |state| {
                (state.events.cursor() > cursor).then(|| state.events.since(cursor))
            })
            .unwrap_or_else(|| (Vec::new(), cursor))
    }

    /// Gives up an open: one that has not resolved fails, and a result nobody took is dropped, its
    /// binding retired, so it never keeps the stream's root locked.
    pub(crate) fn cancel(&self, open: OpenId) -> Result<(), DriverGone> {
        self.cancel_reporting(open, Duration::ZERO).map(drop)
    }

    /// [`Driver::cancel`], waiting at most `timeout` for what the open ended with: the last
    /// condition it met, or `cancelled` when it met none. `None` when it bound before the cancel,
    /// when its result was already taken, or when the time ran out.
    pub(crate) fn cancel_reporting(
        &self,
        open: OpenId,
        timeout: Duration,
    ) -> Result<Option<Arc<OpenError>>, DriverGone> {
        let untaken = {
            let mut state = self.shared.lock();
            match state.opened.remove(&open) {
                Some(result) => Some(result),
                None => {
                    // An open whose result was already taken is the caller's to retire.
                    if state.pending.contains(&open) {
                        state.cancelled.insert(open);
                        state.reporting.insert(open);
                    }
                    None
                }
            }
        };
        match untaken {
            Some(Ok(bound)) => {
                // Nobody took it, so no connection holds it.
                bound.handle.retiring();
                self.command(Command::Retire {
                    binding: bound.binding,
                })?;
                return Ok(None);
            }
            Some(Err(e)) => return Ok(Some(e)),
            None => {}
        }
        self.command(Command::Cancel { open })?;
        let ended = self
            .shared
            .wait_for(timeout, |state| state.given_up.remove(&open));
        if ended.is_some() {
            return Ok(ended);
        }
        let mut state = self.shared.lock();
        state.reporting.remove(&open);
        Ok(state.given_up.remove(&open))
    }

    pub(crate) fn retire(&self, binding: BindingId) -> Result<(), DriverGone> {
        self.command(Command::Retire { binding })
    }

    /// `ursula_open` handed the binding out: it retires unless a connection opens it soon.
    pub(crate) fn unconnected(&self, binding: BindingId) -> Result<(), DriverGone> {
        self.command(Command::Unconnected { binding })
    }

    /// The binding's connections found it damaged: it resets `kind`.
    pub(crate) fn reset(&self, binding: BindingId, kind: ResetKind) -> Result<(), DriverGone> {
        self.command(Command::Reset { binding, kind })
    }

    /// A batch must end exactly at `position`, a snapshot's `k`: its receipt reaches the
    /// binding's snapshot thread ([`crate::snapshotter::Snapper`]).
    pub(crate) fn boundary(
        &self,
        binding: BindingId,
        position: Position,
    ) -> Result<(), DriverGone> {
        self.command(Command::Boundary { binding, position })
    }

    /// A snapshot published (or found superseded) covers `log_bytes` of the frame bytes counted
    /// toward the next one.
    pub(crate) fn snapshotted(&self, binding: BindingId, log_bytes: u64) -> Result<(), DriverGone> {
        self.command(Command::Snapshotted { binding, log_bytes })
    }

    /// The transport of the stream at `url`: the snapshot thread's publishes go out on it.
    pub(crate) fn transport(&self, url: &str) -> Arc<dyn Transport> {
        (self.transports)(url)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        if self.command(Command::Shutdown).is_err() {
            crate::log::emit(crate::log::Level::Warn, "driver_gone", &[]);
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            crate::log::emit(crate::log::Level::Error, "driver_thread_panicked", &[]);
        }
    }
}

/// A stream's two queues: reads, and everything else.
struct Lane {
    reads: mpsc::Sender<HttpIo>,
    writes: mpsc::Sender<HttpIo>,
}

/// The driver thread's workers.
struct Threads {
    transports: Transports,
    completions: mpsc::Sender<Command>,
    lanes: HashMap<Arc<str>, Lane>,
    handles: Vec<JoinHandle<()>>,
}

impl Threads {
    /// A worker thread with its queue, or `None` when no thread could be made.
    fn worker(&mut self) -> Option<mpsc::Sender<HttpIo>> {
        let (jobs, queue) = mpsc::channel::<HttpIo>();
        let completions = self.completions.clone();
        let transports = Arc::clone(&self.transports);
        let spawned = std::thread::Builder::new()
            .name("ursula-vfs-http".to_owned())
            .spawn(move || {
                while let Ok(io) = queue.recv() {
                    let exchange = guard::contain("driver_http", lost, || {
                        transports(&io.url).send(&io.request, io.attempt)
                    });
                    let done = Command::Completed {
                        io: io.id,
                        exchange,
                    };
                    if completions.send(done).is_err() {
                        return;
                    }
                }
            });
        match spawned {
            Ok(handle) => {
                self.handles.push(handle);
                Some(jobs)
            }
            Err(e) => {
                crate::log::emit(crate::log::Level::Error, "driver_worker_failed", &[(
                    "error", &e,
                )]);
                None
            }
        }
    }

    /// Queues a request on its stream's lane. A lane whose worker died is spawned again; a
    /// request no worker takes is answered as lost, so the core retries it.
    ///
    /// Lanes are not removed: two idle threads per stream URL the process opened, a bound set by
    /// the streams an application opens.
    fn http(&mut self, io: HttpIo) {
        let mut io = io;
        for _ in 0..2 {
            if !self.lanes.contains_key(&io.url) {
                let (Some(reads), Some(writes)) = (self.worker(), self.worker()) else {
                    break;
                };
                self.lanes
                    .insert(Arc::clone(&io.url), Lane { reads, writes });
            }
            let Some(lane) = self.lanes.get(&io.url) else {
                break;
            };
            let queue = if io.request.method == Method::Get {
                &lane.reads
            } else {
                &lane.writes
            };
            match queue.send(io) {
                Ok(()) => return,
                Err(mpsc::SendError(back)) => {
                    crate::log::emit(crate::log::Level::Error, "driver_lane_died", &[(
                        "url", &back.url,
                    )]);
                    self.lanes.remove(&back.url);
                    io = back;
                }
            }
        }
        let done = Command::Completed {
            io: io.id,
            exchange: lost(),
        };
        if self.completions.send(done).is_err() {
            crate::log::emit(crate::log::Level::Warn, "driver_gone", &[]);
        }
    }

    /// Runs a disk job on a thread of its own. Without a thread the job is answered as
    /// panicked: what it would have worked on drops, and the core fails its owner.
    fn disk(&mut self, io: DiskIo) {
        self.handles.retain(|h| !h.is_finished());
        let completions = self.completions.clone();
        let DiskIo { id, job } = io;
        let spawned = std::thread::Builder::new()
            .name("ursula-vfs-disk".to_owned())
            .spawn(move || {
                let done = guard::contain("driver_disk", || DiskDone::Panicked, || job.run());
                let done = Command::DiskDone {
                    io: id,
                    done: Box::new(done),
                };
                if completions.send(done).is_err() {
                    crate::log::emit(crate::log::Level::Warn, "driver_gone", &[]);
                }
            });
        match spawned {
            Ok(handle) => self.handles.push(handle),
            Err(e) => {
                crate::log::emit(crate::log::Level::Error, "driver_worker_failed", &[(
                    "error", &e,
                )]);
                let done = Command::DiskDone {
                    io: id,
                    done: Box::new(DiskDone::Panicked),
                };
                if self.completions.send(done).is_err() {
                    crate::log::emit(crate::log::Level::Warn, "driver_gone", &[]);
                }
            }
        }
    }

    fn dispatch(&mut self, ios: Vec<Io>) {
        for io in ios {
            match io {
                Io::Http(io) => self.http(io),
                Io::Disk(io) => self.disk(io),
            }
        }
    }

    fn join(mut self) {
        self.lanes.clear();
        drop(self.completions);
        for handle in self.handles.drain(..) {
            if handle.join().is_err() {
                crate::log::emit(crate::log::Level::Error, "driver_worker_panicked", &[]);
            }
        }
    }
}

/// The answer of an attempt whose transport panicked or found no worker: it may have gone out.
fn lost() -> Exchange {
    Exchange::NoAnswer(NoAnswer::Lost {
        reason: "the transport failed inside the extension".to_owned(),
    })
}

/// The driver thread's loop.
fn run(
    mut core: Core,
    inbox: &mpsc::Receiver<Command>,
    threads: &mut Threads,
    shared: &Shared,
    role: Role,
) {
    let start = Instant::now();
    let now = || u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut cursor = 0;
    // When the token file was last checked for a change.
    let mut token_checked: u64 = 0;
    loop {
        // A key's exchanges run only while an open or a binding may send requests: an open counts
        // from when it was asked for ([`Driver::open`]) until its outcome is published. Both say
        // so under the shared lock, so an open asked for meanwhile is never undone here.
        if role == Role::Process {
            let state = shared.lock();
            auth::requests_may_go_out(core.requesting() || !state.pending.is_empty());
        }
        // While requests carry the token file's token and an open or a writer may send them, the
        // file is checked every `FILE_CHECK_MS`: a new token in it comes back as
        // `Command::TokenChanged`.
        let check = watch_file(&core).then(|| token_checked.saturating_add(auth::FILE_CHECK_MS));
        let received = match [core.next_deadline(), check].into_iter().flatten().min() {
            None => inbox
                .recv()
                .map_err(|_closed| mpsc::RecvTimeoutError::Disconnected),
            Some(at) => inbox.recv_timeout(Duration::from_millis(at.saturating_sub(now()))),
        };
        let command = match received {
            Ok(Command::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Ok(command) => Some(command),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
        };
        // The core contains panics inside a binding's step; one anywhere else is logged and the
        // command dropped, rather than taking every database of the process down.
        let mut ios = guard::contain("driver_core", Vec::new, || match command {
            None => core.tick(now()),
            Some(command) => handle(&mut core, command, now()),
        });
        if watch_file(&core) && now() >= token_checked.saturating_add(auth::FILE_CHECK_MS) {
            token_checked = now();
            auth::check_file();
        }
        if core.next_deadline().is_some_and(|at| at <= now()) {
            ios.extend(guard::contain("driver_core", Vec::new, || core.tick(now())));
        }
        threads.dispatch(ios);
        // Retiring a cancelled open's binding makes events too: publish until none is left.
        loop {
            let retire = publish(&mut core, &mut cursor, shared);
            if retire.is_empty() {
                break;
            }
            for binding in retire {
                let ios = guard::contain("driver_core", Vec::new, || {
                    core.retire_untaken(binding, now())
                });
                threads.dispatch(ios);
            }
        }
    }
}

/// Publishes the core's new events for the callers. Returns the bindings of cancelled opens whose
/// result arrived, to retire.
fn publish(core: &mut Core, cursor: &mut u64, shared: &Shared) -> Vec<BindingId> {
    let mut retire = Vec::new();
    let (events, next) = core.events_since(*cursor);
    if next != *cursor {
        *cursor = next;
        {
            let mut state = shared.lock();
            for event in events {
                if let DriverEvent::Opened { open, .. } = &event
                    && let Some(result) = core.take_opened(*open)
                {
                    state.pending.remove(open);
                    // A cancelled open's result is dropped, its binding retired, and its failure
                    // kept for a caller of `cancel_reporting` still waiting.
                    match result {
                        Ok(bound) if state.cancelled.remove(open) => {
                            retire.push(bound.binding);
                        }
                        Err(e) if state.cancelled.remove(open) => {
                            if state.reporting.remove(open) {
                                state.given_up.insert(*open, e);
                            }
                        }
                        result => {
                            state.opened.insert(*open, result);
                        }
                    }
                }
                state.settle(&event);
                state.events.push(event);
            }
        }
        shared.changed.notify_all();
    }
    retire
}

/// Whether the driver checks the token file for a change: requests carry its token, and an open or
/// a writer may send them.
fn watch_file(core: &Core) -> bool {
    core.requesting() && auth::watching()
}

fn handle(core: &mut Core, command: Command, now_ms: u64) -> Vec<Io> {
    match command {
        Command::Open { open, spec } => core.open(open, spec, now_ms),
        Command::Logged { binding, rec } => core.logged(binding, rec, now_ms),
        Command::Sync {
            ticket,
            binding,
            target,
        } => core.sync(ticket, binding, target, now_ms),
        Command::Boundary { binding, position } => core.boundary(binding, position, now_ms),
        Command::Snapshotted { binding, log_bytes } => core.snapshotted(binding, log_bytes, now_ms),
        Command::Retire { binding } => core.retire(binding, now_ms),
        Command::Unconnected { binding } => {
            core.unconnected(binding, now_ms);
            Vec::new()
        }
        Command::Reset { binding, kind } => {
            core.reset_binding(binding, kind, now_ms);
            Vec::new()
        }
        Command::Cancel { open } => core.cancel(open, now_ms),
        Command::Completed { io, exchange } => core.completed(io, exchange, now_ms),
        Command::DiskDone { io, done } => core.disk_done(io, *done, now_ms),
        Command::TokenChanged => core.token_changed(now_ms),
        Command::Shutdown => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::BindingId;
    use super::DriverEvent;
    use super::Published;
    use super::SyncError;
    use super::Ticket;
    use crate::replicator::ResetKind;

    // Only waiting tickets get an outcome stored: a host that waits 0 ms on `ursula_synced` learns
    // it from the events, and leaves nothing behind. A watermark settles a binding's tickets up to
    // it, a reset all of them.
    #[test]
    fn outcomes_are_kept_for_waiting_tickets_only() {
        let (a, b) = (BindingId(1), BindingId(2));
        let mut p = Published::default();
        p.waiting.insert(Ticket(3), a);
        p.waiting.insert(Ticket(5), a);
        p.waiting.insert(Ticket(4), b);
        p.settle(&DriverEvent::Synced {
            binding: a,
            through: Ticket(4),
        });
        assert_eq!(p.synced.get(&Ticket(3)), Some(&Ok(())));
        assert!(!p.synced.contains_key(&Ticket(5)));
        assert!(!p.synced.contains_key(&Ticket(4)));
        p.settle(&DriverEvent::Synced {
            binding: a,
            through: Ticket(9),
        });
        assert_eq!(p.synced.get(&Ticket(5)), Some(&Ok(())));
        p.settle(&DriverEvent::Reset {
            binding: b,
            cause: ResetKind::Fenced,
        });
        assert_eq!(
            p.synced.get(&Ticket(4)),
            Some(&Err(SyncError::Reset(ResetKind::Fenced)))
        );
        assert!(p.waiting.is_empty());
        let stored = p.synced.len();
        p.settle(&DriverEvent::SyncFailed {
            ticket: Ticket(7),
            error: SyncError::NotBound,
        });
        assert_eq!(p.synced.len(), stored);
    }
}
