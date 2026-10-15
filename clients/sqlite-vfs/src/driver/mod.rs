//! The process-wide driver (design §4.10, §4.12, §8.2, §8.6): it runs a [`Replicator`] per bound
//! database and the opens that lead to them, carries out their actions, and reports what came of
//! them as [`DriverEvent`]s.
//!
//! The work splits in two. [`core::Core`] is deterministic and single-threaded: it takes calls
//! and outcomes with the time, executes every replicator action, writes the small log records
//! itself (the CLAIM, fsynced before its send, and lazy ACKs), and hands back the HTTP requests
//! and the long disk jobs ([`disk`]) to carry out as [`Io`]s. Tests drive it with a virtual clock.
//! [`runtime`] runs one `Core` on a driver thread, sends its requests from blocking HTTP workers
//! (two queues per stream: reads, and everything else), and runs its disk jobs on threads of
//! their own.
//!
//! [`Replicator`]: crate::replicator::Replicator
//!
//! Module map:
//!
//! - [`core`]: the deterministic core.
//! - [`disk`]: the disk jobs of opens and takeovers.
//! - [`runtime`]: the driver thread, its HTTP workers and its disk jobs.

pub(crate) mod core;
pub(crate) mod disk;
pub(crate) mod runtime;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use ursula_headers::Incarnation;
use ursula_headers::Offset;

use self::disk::DiskJob;
use crate::generation::CanonicalPath;
use crate::generation::Generation;
use crate::generation::Hold;
use crate::generation::Root;
use crate::ids::Position;
use crate::open::OpenError;
use crate::replicator::Condition;
use crate::replicator::ResetKind;
use crate::replog::Log;
use crate::salvage;
use crate::salvage::Reason;
use crate::salvage::SalvageError;
use crate::salvage::Salvaged;
use crate::snapshotter::Snapshots;
use crate::transport::Attempt;
use crate::wire::Request;

/// The bounded event queue's length (design §8.6).
pub(crate) const EVENTS: usize = 4096;

/// How long a binding `ursula_open` handed out waits for a connection before it retires.
pub(crate) const UNCONNECTED_MS: u64 = 60_000;

/// How often, at most, a bound database writes an ACK record (design §5.2).
pub(crate) const ACK_INTERVAL_MS: u64 = 1_000;

/// One open, from the call to its outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct OpenId(pub(crate) u64);

/// One bound database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct BindingId(pub(crate) u64);

/// One request the core asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IoId(pub(crate) u64);

/// One `synced()` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Ticket(pub(crate) u64);

/// What the core asks the runtime to carry out.
#[derive(Debug)]
pub(crate) enum Io {
    Http(HttpIo),
    Disk(DiskIo),
}

/// A request to send to the stream at `url`, and hand back to [`core::Core::completed`] with its
/// `id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpIo {
    pub(crate) id: IoId,
    pub(crate) url: Arc<str>,
    pub(crate) request: Request,
    pub(crate) attempt: Attempt,
}

/// A disk job to run, and hand back to [`core::Core::disk_done`] with its `id`.
#[derive(Debug)]
pub(crate) struct DiskIo {
    pub(crate) id: IoId,
    pub(crate) job: DiskJob,
}

impl ResetKind {
    /// The stable kind token (design §8.3).
    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Fenced => "fenced",
            Self::Stale => "stale",
            Self::Rewound => "rewound",
            Self::Deleted => "deleted",
            Self::LocalDamage => "local_damage",
            Self::StreamState => "stream_state",
            Self::Internal => "internal",
        }
    }
}

/// Why a `synced()` call will never resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncError {
    Reset(ResetKind),
    /// No such binding, or it retired.
    NotBound,
}

/// A bound database's state that its connections share with the driver: the generation, the log
/// the commit path appends to, the pending bytes admission reads (design §4.4), the reset the
/// connections refuse their work on, what status reports of the replicator, and the root. The
/// root is released when the binding retires with no connection left. A binding that resets with
/// connections open holds its generation ([`Root::hold`]) until the last of them closes, and the
/// root with it: the root stays locked against other processes, and a reopen in this process
/// shares it and builds from a copy of the held generation, never materializing the files the
/// old connections still read (design §5.6).
#[derive(Debug)]
pub(crate) struct BindingHandle {
    pub(crate) generation: Generation,
    /// The cache root the open bound under, as its spec named it.
    pub(crate) dir: PathBuf,
    pub(crate) name: CanonicalPath,
    /// The stream's cluster id.
    pub(crate) cluster_id: String,
    /// The stream's token: the one it bound with, then the one of each re-key (design §6.2).
    incarnation: Mutex<Incarnation>,
    log: Mutex<Log>,
    pending_bytes: AtomicU64,
    max_pending_bytes: u64,
    /// The last COMMIT record's position, as the commit path records it.
    last_commit: AtomicU64,
    /// The largest commit frame the replicator can send in one batch.
    max_frame_bytes: AtomicU64,
    /// The last acknowledged position.
    acked: AtomicU64,
    /// The reset, once one happened: [`ResetKind`] + 1, 0 for none. The first one stays.
    reset: AtomicU8,
    life: Mutex<Lifecycle>,
    replication: Mutex<Replication>,
    /// The root, and the hold on the generation a reset with connections open takes: released
    /// when the binding retires with no connection left, or when the last connection of a reset
    /// binding closes.
    root: Mutex<Option<(Arc<Root>, Option<Hold>)>>,
    /// The snapshot thread's wake-up, the receipts of its boundaries and its status.
    pub(crate) snapshots: Snapshots,
    /// Where the open got the local database.
    pub(crate) attach: Attach,
    /// The root's salvages, read once while the binding is live: only an open of the stream
    /// salvages, and none runs beside a live binding of it in this process, nor in another while
    /// the root is locked. A reopen beside a reset binding may, so a reset binding lists them anew.
    salvage: OnceLock<Arc<[Salvaged]>>,
    /// Opened on an unreachable stream (design §5.5) and no claim of it has applied yet: reads
    /// come from files of unverified freshness, and `synced()` waits for the claim.
    provisional: AtomicBool,
}

/// Where a binding stands between its connections and the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Life {
    /// `ursula_open` handed it out `awaiting` times (at least once) that no connection of the host
    /// took up yet: each path it handed out stays openable until a connection opens it, or until
    /// the binding waited [`UNCONNECTED_MS`] since the latest hand-out. Closing connections leaves
    /// it be.
    HandedOut { awaiting: u32 },
    /// Every hand-out was taken up, and a connection of the host is open.
    Live,
    /// Its last connection of the host closed: it drains, then retires once no private connection
    /// is open either. A connection of the host, or a new open, takes it back.
    Retiring,
    /// Given up for an open of its stream under another cache root ([`BindingHandle::give_up`]):
    /// it drains, then retires once no private connection is open. Nothing takes it back.
    GivenUp,
    /// Gone from the driver.
    Retired,
}

impl Life {
    /// Whether the binding drains toward its retirement.
    fn leaving(self) -> bool {
        matches!(self, Life::Retiring | Life::GivenUp)
    }
}

/// Who opens a connection on a binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Holder {
    /// The host's: it takes a retiring binding back, and takes up a hand-out.
    Host,
    /// The extension's private connection (a snapshot attempt): it opens only beside a connection
    /// of the host, and while open it only delays the retirement.
    Private,
}

/// A connection [`BindingHandle::connect`] counted: whose it is, and where the binding stood
/// before, so an open that fails after it undoes it exactly ([`BindingHandle::unconnect`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Connected {
    pub(crate) holder: Holder,
    before: Life,
}

/// A binding's life with its open connections, decided under one lock: a connection opens only
/// while the binding is not retired, and a binding retires only while none is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lifecycle {
    life: Life,
    /// Open connections of the host.
    host: usize,
    /// Open private connections.
    private: usize,
    /// How many times `ursula_open` handed the binding out again.
    handouts: u64,
}

impl Lifecycle {
    fn count(&mut self, holder: Holder) -> &mut usize {
        match holder {
            Holder::Host => &mut self.host,
            Holder::Private => &mut self.private,
        }
    }

    /// Whether no connection is open.
    fn idle(&self) -> bool {
        self.host == 0 && self.private == 0
    }
}

/// Why a stream stays where it is open in this process: an open of it under another cache root is
/// refused `in_use` ([`BindingHandle::give_up`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Kept {
    /// Connections of the host are open on the binding: they would keep writing the other cache.
    #[error("connections are open on it ({0})")]
    Connections(usize),
    /// `ursula_open` handed the binding's path out and no connection opened it yet: the caller it
    /// went to would find it retired.
    #[error("ursula_open handed it out and no connection opened it yet")]
    HandedOut,
    /// Commits of the binding wait for the stream's acknowledgement: the other cache must not
    /// build from the stream without them.
    #[error("{bytes} bytes of its commits are not acknowledged yet")]
    Pending { bytes: u64 },
    /// The binding, given up, did not drain and retire before the open's deadline.
    #[error("it did not retire within the open's deadline")]
    Draining,
    /// Not bound yet: an open of the stream under the other cache root is in progress.
    #[error("an open of it is in progress")]
    Opening,
}

/// What status reports of a binding's replicator (design §9).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Replication {
    pub(crate) state: &'static str,
    pub(crate) epoch: Option<u64>,
    pub(crate) condition: Option<Condition>,
    /// Logged commits not yet acknowledged.
    pub(crate) pending: usize,
    /// Frame bytes acknowledged since the latest snapshot.
    pub(crate) log_bytes: u64,
    /// The request limit `L` (design §4.6).
    pub(crate) request_limit: u64,
}

const RESETS: [ResetKind; 7] = [
    ResetKind::Fenced,
    ResetKind::Stale,
    ResetKind::Rewound,
    ResetKind::Deleted,
    ResetKind::LocalDamage,
    ResetKind::StreamState,
    ResetKind::Internal,
];

/// A write transaction refused at admission (design §4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("backlog_full: {pending} bytes pending, at most {max}")]
pub(crate) struct BacklogFull {
    pub(crate) pending: u64,
    pub(crate) max: u64,
}

/// The log's lock was poisoned by a panic while held: the log may be torn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the log's lock was poisoned")]
pub(crate) struct LogPoisoned;

impl BindingHandle {
    pub(crate) fn new(bind: Bind, root: Arc<Root>) -> Self {
        Self {
            generation: bind.generation,
            dir: bind.dir,
            name: bind.name,
            cluster_id: bind.cluster_id,
            incarnation: Mutex::new(bind.incarnation),
            log: Mutex::new(bind.log),
            pending_bytes: AtomicU64::new(bind.pending_bytes),
            max_pending_bytes: bind.max_pending_bytes,
            max_frame_bytes: AtomicU64::new(bind.max_frame_bytes),
            acked: AtomicU64::new(bind.acked.0),
            reset: AtomicU8::new(0),
            life: Mutex::new(Lifecycle {
                life: Life::HandedOut { awaiting: 1 },
                host: 0,
                private: 0,
                handouts: 0,
            }),
            replication: Mutex::new(Replication::default()),
            last_commit: AtomicU64::new(0),
            root: Mutex::new(Some((root, None))),
            snapshots: Snapshots::default(),
            attach: bind.attach,
            salvage: OnceLock::new(),
            provisional: AtomicBool::new(bind.provisional),
        }
    }

    /// The salvages of the root the binding's generation is in (`status().salvage`).
    pub(crate) fn salvage(&self) -> Result<Arc<[Salvaged]>, SalvageError> {
        let reset = self.reset().is_some();
        if !reset && let Some(listed) = self.salvage.get() {
            return Ok(Arc::clone(listed));
        }
        let dir = &self.generation.dir;
        let listed: Arc<[Salvaged]> = salvage::list(dir.parent().unwrap_or(dir))?.into();
        if reset {
            return Ok(listed);
        }
        Ok(Arc::clone(self.salvage.get_or_init(|| listed)))
    }

    /// Whether the binding is still provisional: opened on an unreachable stream, no claim of it
    /// applied yet.
    pub(crate) fn provisional(&self) -> bool {
        self.provisional.load(Ordering::Acquire)
    }

    /// A claim of the binding applied and its reconcile completed: it is live. True when it was
    /// provisional until now.
    fn set_live(&self) -> bool {
        self.provisional.swap(false, Ordering::AcqRel)
    }

    /// The stream's token: the requests of the snapshot thread carry it.
    pub(crate) fn incarnation(&self) -> Incarnation {
        *self
            .incarnation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The generation re-keyed to `incarnation`.
    fn set_incarnation(&self, incarnation: Incarnation) {
        *self
            .incarnation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = incarnation;
    }

    /// The log. The commit path appends under it, and the driver appends CLAIM and ACK records
    /// under it.
    ///
    /// The lock is held across the fsync of a CLAIM record and, from PR6 on, across F1's log
    /// fdatasync at a checkpoint, so a commit may wait for one fsync. That is accepted: the log is
    /// one file and this lock is what guards it, so an fsync of records appended under it holds it
    /// too, and claims and checkpoints are rare.
    pub(crate) fn log(&self) -> Result<MutexGuard<'_, Log>, LogPoisoned> {
        self.log.lock().map_err(|_poisoned| LogPoisoned)
    }

    /// The commit path appended a COMMIT record at `position`, under the log's lock: `synced()`
    /// without a target waits for it.
    pub(crate) fn record_commit(&self, position: Position) {
        self.last_commit.fetch_max(position.0, Ordering::AcqRel);
    }

    /// The last COMMIT record's position: `synced()`'s default target.
    pub(crate) fn last_commit(&self) -> Position {
        Position(self.last_commit.load(Ordering::Acquire))
    }

    pub(crate) fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Acquire)
    }

    /// Admits a write transaction whose commit frame takes about `len` bytes: below the bound, or
    /// anything when nothing is pending.
    pub(crate) fn admit(&self, len: u64) -> Result<(), BacklogFull> {
        let pending = self.pending_bytes();
        if pending == 0 || pending.saturating_add(len) <= self.max_pending_bytes {
            Ok(())
        } else {
            Err(BacklogFull {
                pending,
                max: self.max_pending_bytes,
            })
        }
    }

    /// The commit path logged a frame of `len` bytes, under the log's lock: admission counts it at
    /// once, before the driver thread hears of it, so a burst of commits never outruns the bound.
    pub(crate) fn add_pending(&self, len: u64) {
        // The closure always answers, so the update always applies.
        let _before = self
            .pending_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_add(len))
            })
            .unwrap_or_else(|unchanged| unchanged);
    }

    /// The driver thread saw frames of `len` bytes acknowledged.
    fn remove_pending(&self, len: u64) {
        // The closure always answers, so the update always applies.
        let _before = self
            .pending_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(len))
            })
            .unwrap_or_else(|unchanged| unchanged);
    }

    pub(crate) fn max_frame_bytes(&self) -> u64 {
        self.max_frame_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn acked(&self) -> Position {
        Position(self.acked.load(Ordering::Acquire))
    }

    /// The reset, once one happened.
    pub(crate) fn reset(&self) -> Option<ResetKind> {
        let n = usize::from(self.reset.load(Ordering::Acquire));
        RESETS.get(n.checked_sub(1)?).copied()
    }

    /// Records a reset: true for the first one, which stays.
    pub(crate) fn mark_reset(&self, kind: ResetKind) -> bool {
        let n = RESETS
            .iter()
            .position(|k| *k == kind)
            .and_then(|i| u8::try_from(i.saturating_add(1)).ok())
            .unwrap_or(u8::MAX);
        self.reset
            .compare_exchange(0, n, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn life_lock(&self) -> MutexGuard<'_, Lifecycle> {
        self.life.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn life(&self) -> Life {
        self.life_lock().life
    }

    /// The open connections of the host.
    pub(crate) fn connections(&self) -> usize {
        self.life_lock().host
    }

    /// A connection opens: `None` once the binding retired or was given up. One of the host takes
    /// a retiring binding back and takes up a hand-out. The private one opens only while a
    /// connection of the host holds the binding.
    pub(crate) fn connect(&self, holder: Holder) -> Option<Connected> {
        let mut l = self.life_lock();
        let before = l.life;
        match (holder, before) {
            (_, Life::Retired | Life::GivenUp) => return None,
            (Holder::Host, Life::HandedOut { awaiting }) if awaiting > 1 => {
                l.life = Life::HandedOut {
                    awaiting: awaiting.saturating_sub(1),
                };
            }
            (Holder::Host, _) => l.life = Life::Live,
            (Holder::Private, Life::Live | Life::HandedOut { .. }) if l.host > 0 => {}
            (Holder::Private, _) => return None,
        }
        let n = l.count(holder);
        *n = n.saturating_add(1);
        Some(Connected { holder, before })
    }

    /// A connection closed. True when the driver should hear of it: the last connection of the
    /// host of a live binding closed, which now retires once drained, or the last connection of a
    /// leaving one closed. A binding with hand-outs not taken up stays: its wait for a connection
    /// decides ([`BindingHandle::idle_retire`]). The last connection of a retired binding (a reset)
    /// releases its root and its hold on the generation.
    pub(crate) fn disconnect(&self, holder: Holder) -> bool {
        let mut l = self.life_lock();
        let n = l.count(holder);
        *n = n.saturating_sub(1);
        self.after_close(&mut l)
    }

    /// Undoes `connected`, for an open that failed after it: the binding goes back to where it
    /// stood, a hand-out it took up waiting again. True when the driver should hear of it, as for
    /// [`BindingHandle::disconnect`].
    pub(crate) fn unconnect(&self, connected: Connected) -> bool {
        let mut l = self.life_lock();
        let n = l.count(connected.holder);
        *n = n.saturating_sub(1);
        if connected.holder == Holder::Host {
            match (connected.before, l.life) {
                // The hand-out it took up waits again.
                (Life::HandedOut { .. }, Life::Live) => l.life = Life::HandedOut { awaiting: 1 },
                (Life::HandedOut { .. }, Life::HandedOut { awaiting }) => {
                    l.life = Life::HandedOut {
                        awaiting: awaiting.saturating_add(1),
                    };
                }
                // Taken back from retiring by this connection alone: retiring again.
                (Life::Retiring, Life::Live) if l.host == 0 => l.life = Life::Retiring,
                _ => {}
            }
        }
        self.after_close(&mut l)
    }

    /// What a connection's close leads to, its count already taken off.
    fn after_close(&self, l: &mut Lifecycle) -> bool {
        match l.life {
            Life::Live if l.host == 0 => {
                l.life = Life::Retiring;
                true
            }
            Life::Retiring | Life::GivenUp => l.idle(),
            Life::Retired => {
                if l.idle() {
                    self.release_root();
                }
                false
            }
            Life::Live | Life::HandedOut { .. } => false,
        }
    }

    /// A binding no connection of the host holds retires once drained: an open's result nobody
    /// took, or one `ursula_open` handed out that no connection opened.
    pub(crate) fn retiring(&self) {
        let mut l = self.life_lock();
        if matches!(l.life, Life::Live | Life::HandedOut { .. }) && l.host == 0 {
            l.life = Life::Retiring;
        }
    }

    /// `ursula_open` hands the binding out again, taking a retiring one back: false once it
    /// retired or was given up. Until a connection of the host takes the hand-out up, no close
    /// retires it.
    pub(crate) fn revive(&self) -> bool {
        let mut l = self.life_lock();
        l.life = match l.life {
            Life::Retired | Life::GivenUp => return false,
            Life::HandedOut { awaiting } => Life::HandedOut {
                awaiting: awaiting.saturating_add(1),
            },
            Life::Live | Life::Retiring => Life::HandedOut { awaiting: 1 },
        };
        l.handouts = l.handouts.saturating_add(1);
        true
    }

    pub(crate) fn handouts(&self) -> u64 {
        self.life_lock().handouts
    }

    /// Whether hand-outs of the binding wait for a connection.
    pub(crate) fn handed_out(&self) -> bool {
        matches!(self.life_lock().life, Life::HandedOut { .. })
    }

    /// The `handouts`-th hand-out waited long enough for a connection: those not taken up lapse,
    /// and the binding retires once drained, or stays live with the connections of the host still
    /// open. A hand-out since, whose own wait runs, or a connection that took them all up, leaves
    /// it be.
    pub(crate) fn idle_retire(&self, handouts: u64) {
        let mut l = self.life_lock();
        if matches!(l.life, Life::HandedOut { .. }) && l.handouts == handouts {
            l.life = if l.host == 0 {
                Life::Retiring
            } else {
                Life::Live
            };
        }
    }

    /// Gives the binding up for an open of its stream under another cache root: it drains and
    /// retires once its private connection closed, and nothing takes it back. Refused while the
    /// host holds it: a connection of the host is open, a hand-out waits for one, or commits are
    /// not acknowledged. A retired binding is no hold.
    pub(crate) fn give_up(&self) -> Result<(), Kept> {
        let mut l = self.life_lock();
        match l.life {
            Life::Retired | Life::GivenUp => return Ok(()),
            _ if l.host > 0 => return Err(Kept::Connections(l.host)),
            Life::HandedOut { .. } => return Err(Kept::HandedOut),
            Life::Live | Life::Retiring => {}
        }
        match self.pending_bytes() {
            0 => {
                l.life = Life::GivenUp;
                Ok(())
            }
            bytes => Err(Kept::Pending { bytes }),
        }
    }

    /// Retires a binding that is still leaving with no connection open: false when a connection
    /// or an open took it back meanwhile, or the private connection is still open. Retired, no
    /// connection opens it again, so the driver unlocks its root once its last ACK is written.
    fn try_retire(&self) -> bool {
        let mut l = self.life_lock();
        let retiring = l.life.leaving() && l.idle();
        if retiring {
            l.life = Life::Retired;
        }
        retiring
    }

    /// Whether the binding drains toward its retirement.
    pub(crate) fn leaving(&self) -> bool {
        self.life_lock().life.leaving()
    }

    /// The binding reset: it retires at once. With no connection open its root goes now.
    /// Otherwise its connections hold the generation, and the root, until the last one closes.
    fn reset_retired(&self) {
        let mut l = self.life_lock();
        l.life = Life::Retired;
        if l.idle() {
            self.release_root();
        } else if let Some((root, hold)) = self
            .root
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            *hold = Some(root.hold(&self.generation));
        }
    }

    /// Whether the binding still holds the root and its generation (tests).
    #[cfg(test)]
    pub(crate) fn holds_root(&self) -> bool {
        self.root
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn release_root(&self) {
        drop(
            self.root
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
    }

    pub(crate) fn replication(&self) -> Replication {
        self.replication
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_replication(&self, replication: Replication, max_frame_bytes: Option<u64>) {
        *self
            .replication
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = replication;
        if let Some(max) = max_frame_bytes {
            self.max_frame_bytes.store(max, Ordering::Release);
        }
    }

    fn set_acked(&self, acked: Position) {
        self.acked.fetch_max(acked.0, Ordering::AcqRel);
    }
}

/// What a bound database starts with.
#[derive(Debug)]
pub(crate) struct Bind {
    pub(crate) generation: Generation,
    pub(crate) name: CanonicalPath,
    pub(crate) cluster_id: String,
    pub(crate) incarnation: Incarnation,
    pub(crate) log: Log,
    pub(crate) pending_bytes: u64,
    pub(crate) max_pending_bytes: u64,
    pub(crate) max_frame_bytes: u64,
    pub(crate) acked: Position,
    pub(crate) attach: Attach,
    /// Bound before its claim: the stream was unreachable at the open (design §5.5).
    pub(crate) provisional: bool,
    /// The cache root the open bound under, as its spec named it.
    pub(crate) dir: PathBuf,
}

/// Where an open got its local database, as status reports it (design §9, `attach`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Attach {
    /// A trusted local generation, materialized.
    Resumed,
    /// A trusted local generation of another token whose history inherits it, materialized and
    /// re-keyed (design §6.2).
    ReKeyed,
    /// A generation built from the stream: from its latest snapshot at `snapshot` and the tail
    /// after it, or from its start when `snapshot` is `None`. `salvaged` names why the local
    /// generation it replaced was salvaged, if it was.
    Built {
        snapshot: Option<Offset>,
        salvaged: Option<Reason>,
    },
}

/// A database bound by an open.
#[derive(Debug, Clone)]
pub(crate) struct Bound {
    pub(crate) binding: BindingId,
    pub(crate) handle: Arc<BindingHandle>,
}

/// What the driver reports (design §8.6): completions, acknowledgements, conditions, resets.
#[derive(Debug, Clone)]
pub(crate) enum DriverEvent {
    /// The open resolved; its outcome, with the handle when it bound, is taken once from
    /// [`core::Core::take_opened`]. The event holds no handle, so a queued event never keeps a
    /// root locked.
    Opened {
        open: OpenId,
        result: Result<BindingId, Arc<OpenError>>,
    },
    /// Every record up to `acked` is acknowledged. Coalesced: only the latest one of a binding
    /// stays in the queue.
    Acked { binding: BindingId, acked: Position },
    Condition {
        binding: BindingId,
        condition: Option<Condition>,
    },
    Reset {
        binding: BindingId,
        cause: ResetKind,
    },
    /// Every `synced()` ticket of the binding up to `through` resolved, except those a
    /// [`DriverEvent::SyncFailed`] before it failed. Coalesced like [`DriverEvent::Acked`]: the
    /// latest watermark covers the earlier ones, so a reader that falls behind loses none.
    Synced { binding: BindingId, through: Ticket },
    /// A ticket that failed alone: its commits were abandoned by a takeover, or its binding was
    /// not bound. A reset fails a binding's tickets with its [`DriverEvent::Reset`] instead.
    SyncFailed { ticket: Ticket, error: SyncError },
    /// The binding is gone from the driver. Its root and its hold on the generation go once its
    /// last connection closes.
    Retired { binding: BindingId },
    /// `dropped` events fell out of the bounded queue before this reader saw them.
    Gap { dropped: u64 },
}

/// The bounded queue of events, read by cursor: each event has a sequence number, and a reader
/// asks for the events from its cursor on.
#[derive(Debug, Default)]
pub(crate) struct EventLog {
    events: VecDeque<(u64, DriverEvent)>,
    next: u64,
    /// Every event below this sequence number that was still queued fell out of the queue.
    evicted_below: u64,
}

impl EventLog {
    pub(crate) fn push(&mut self, event: DriverEvent) {
        match &event {
            DriverEvent::Acked { binding, .. } => {
                let binding = *binding;
                self.events.retain(
                    |(_, e)| !matches!(e, DriverEvent::Acked { binding: b, .. } if *b == binding),
                );
            }
            DriverEvent::Synced { binding, .. } => {
                let binding = *binding;
                self.events.retain(
                    |(_, e)| !matches!(e, DriverEvent::Synced { binding: b, .. } if *b == binding),
                );
            }
            _ => {}
        }
        self.events.push_back((self.next, event));
        self.next = self.next.saturating_add(1);
        while self.events.len() > EVENTS {
            if let Some((seq, _)) = self.events.pop_front() {
                self.evicted_below = seq.saturating_add(1);
            }
        }
    }

    /// The cursor the next event will take.
    pub(crate) fn cursor(&self) -> u64 {
        self.next
    }

    /// The events from `cursor` on, and the cursor after them.
    pub(crate) fn since(&self, cursor: u64) -> (Vec<DriverEvent>, u64) {
        let mut out = Vec::new();
        if cursor < self.evicted_below {
            out.push(DriverEvent::Gap {
                dropped: self.evicted_below.saturating_sub(cursor),
            });
        }
        out.extend(
            self.events
                .iter()
                .filter(|(seq, _)| *seq >= cursor)
                .map(|(_, e)| e.clone()),
        );
        (out, self.next)
    }
}

/// The driver's randomness, from a seed: backoff jitter and lineages. Deterministic for tests,
/// seeded from the system's random device in the runtime.
#[derive(Debug, Clone)]
pub(crate) struct Rng {
    key: [u8; 32],
    counter: u64,
}

impl Rng {
    pub(crate) fn new(seed: [u8; 32]) -> Self {
        Self {
            key: seed,
            counter: 0,
        }
    }

    fn next_block(&mut self) -> [u8; 32] {
        self.counter = self.counter.wrapping_add(1);
        *blake3::keyed_hash(&self.key, &self.counter.to_le_bytes()).as_bytes()
    }

    /// Uniform in `0..=max`.
    pub(crate) fn up_to(&mut self, max: u64) -> u64 {
        let block = self.next_block();
        let mut first = [0u8; 8];
        first.copy_from_slice(block.first_chunk::<8>().unwrap_or(&[0; 8]));
        let n = u64::from_le_bytes(first);
        match max.checked_add(1) {
            Some(span) => n.checked_rem(span).unwrap_or(0),
            None => n,
        }
    }

    pub(crate) fn bytes16(&mut self) -> [u8; 16] {
        let block = self.next_block();
        let mut out = [0u8; 16];
        out.copy_from_slice(block.first_chunk::<16>().unwrap_or(&[0; 16]));
        out
    }
}
