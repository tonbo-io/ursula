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
//! binding's step it resets that binding `internal`. M3 replaces the workers with an async
//! transport, not the core.
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
use crate::guard;
use crate::ids::Position;
use crate::open::OpenError;
use crate::open::OpenSpec;
use crate::replicator::Pending;
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
    Retire {
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
    Shutdown,
}

/// What the driver thread publishes for the callers.
#[derive(Default)]
struct Published {
    events: EventLog,
    opened: BTreeMap<OpenId, Result<Bound, Arc<OpenError>>>,
    synced: BTreeMap<Ticket, Result<(), SyncError>>,
    /// Opens given up before their result arrived: it is dropped, its binding retired.
    cancelled: BTreeSet<OpenId>,
    /// Opens started whose result has not arrived yet.
    pending: BTreeSet<OpenId>,
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
    shared: Arc<Shared>,
    next_id: AtomicU64,
    thread: Option<JoinHandle<()>>,
}

/// The process driver, over HTTP, started on first use.
#[expect(
    dead_code,
    reason = "the SQL functions call it from the 0.8 switch PR; tests start their own drivers"
)]
pub(crate) fn driver() -> &'static Driver {
    static DRIVER: OnceLock<Driver> = OnceLock::new();
    DRIVER.get_or_init(|| {
        let transports: Transports =
            Arc::new(|url: &str| Arc::new(HttpTransport::new(url)) as Arc<dyn Transport>);
        Driver::start(transports, seed())
    })
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
    /// Starts the driver thread.
    pub(crate) fn start(transports: Transports, seed: [u8; 32]) -> Self {
        let (commands, inbox) = mpsc::channel::<Command>();
        let shared = Arc::new(Shared {
            state: Mutex::new(Published::default()),
            changed: Condvar::new(),
        });
        let published = Arc::clone(&shared);
        let completions = commands.clone();
        let spawned = std::thread::Builder::new()
            .name("ursula-vfs-driver".to_owned())
            .spawn(move || {
                let mut threads = Threads {
                    transports,
                    completions,
                    lanes: HashMap::new(),
                    handles: Vec::new(),
                };
                run(Core::new(seed), &inbox, &mut threads, &published);
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
            shared,
            next_id: AtomicU64::new(1),
            thread,
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
        self.shared.lock().pending.insert(open);
        self.command(Command::Open { open, spec })?;
        Ok(open)
    }

    /// The open's outcome, waiting at most `timeout` for it. It is handed out once. When the
    /// time runs out the open is cancelled ([`Driver::cancel`]), so no result is left untaken.
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

    /// `synced()`: [`Driver::wait`] waits for the ticket's outcome.
    pub(crate) fn sync(
        &self,
        binding: BindingId,
        target: Option<Position>,
    ) -> Result<Ticket, DriverGone> {
        let ticket = Ticket(self.id());
        self.command(Command::Sync {
            ticket,
            binding,
            target,
        })?;
        Ok(ticket)
    }

    /// The ticket's outcome, waiting at most `timeout` for it. It is handed out once.
    pub(crate) fn wait(&self, ticket: Ticket, timeout: Duration) -> Option<Result<(), SyncError>> {
        self.shared
            .wait_for(timeout, |state| state.synced.remove(&ticket))
    }

    /// The events from `cursor` on, waiting at most `timeout` for one.
    pub(crate) fn events(&self, cursor: u64, timeout: Duration) -> (Vec<DriverEvent>, u64) {
        self.shared
            .wait_for(timeout, |state| {
                (state.events.cursor() > cursor).then(|| state.events.since(cursor))
            })
            .unwrap_or_else(|| (Vec::new(), cursor))
    }

    /// Gives up an open: one that has not resolved fails `cancelled`, and a result nobody took
    /// is dropped, its binding retired, so it never keeps the stream's root locked.
    pub(crate) fn cancel(&self, open: OpenId) -> Result<(), DriverGone> {
        let untaken = {
            let mut state = self.shared.lock();
            match state.opened.remove(&open) {
                Some(result) => Some(result),
                None => {
                    // An open whose result was already taken is the caller's to retire.
                    if state.pending.contains(&open) {
                        state.cancelled.insert(open);
                    }
                    None
                }
            }
        };
        if let Some(Ok(bound)) = untaken {
            return self.command(Command::Retire {
                binding: bound.binding,
            });
        }
        self.command(Command::Cancel { open })
    }

    pub(crate) fn retire(&self, binding: BindingId) -> Result<(), DriverGone> {
        self.command(Command::Retire { binding })
    }

    pub(crate) fn boundary(
        &self,
        binding: BindingId,
        position: Position,
    ) -> Result<(), DriverGone> {
        self.command(Command::Boundary { binding, position })
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
    /// the streams an application opens. M3's async transport replaces them.
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
fn run(mut core: Core, inbox: &mpsc::Receiver<Command>, threads: &mut Threads, shared: &Shared) {
    let start = Instant::now();
    let now = || u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut cursor = 0;
    loop {
        let received = match core.next_deadline() {
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
                let ios = guard::contain("driver_core", Vec::new, || core.retire(binding, now()));
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
                match &event {
                    DriverEvent::Opened { open, .. } => {
                        if let Some(result) = core.take_opened(*open) {
                            state.pending.remove(open);
                            // A cancelled open's result is dropped, its binding retired.
                            match result {
                                Ok(bound) if state.cancelled.remove(open) => {
                                    retire.push(bound.binding);
                                }
                                Err(_) if state.cancelled.remove(open) => {}
                                result => {
                                    state.opened.insert(*open, result);
                                }
                            }
                        }
                    }
                    DriverEvent::Synced { ticket } => {
                        state.synced.insert(*ticket, Ok(()));
                    }
                    DriverEvent::SyncFailed { ticket, error } => {
                        state.synced.insert(*ticket, Err(*error));
                    }
                    _ => {}
                }
                state.events.push(event);
            }
        }
        shared.changed.notify_all();
    }
    retire
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
        Command::Retire { binding } => core.retire(binding, now_ms),
        Command::Cancel { open } => core.cancel(open, now_ms),
        Command::Completed { io, exchange } => core.completed(io, exchange, now_ms),
        Command::DiskDone { io, done } => core.disk_done(io, *done, now_ms),
        Command::Shutdown => Vec::new(),
    }
}
