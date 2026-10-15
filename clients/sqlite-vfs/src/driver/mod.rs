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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use self::disk::DiskJob;
use crate::generation::CanonicalPath;
use crate::generation::Generation;
use crate::generation::Root;
use crate::ids::Position;
use crate::open::OpenError;
use crate::replicator::Condition;
use crate::replicator::ResetKind;
use crate::replog::Log;
use crate::transport::Attempt;
use crate::wire::Request;

/// The bounded event queue's length (design §8.6).
pub(crate) const EVENTS: usize = 4096;

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
    /// A takeover's divergence discarded the target.
    Abandoned,
    /// No such binding, or it retired.
    NotBound,
}

/// A bound database's state that its connections share with the driver: the generation, the log
/// the commit path appends to, the pending bytes admission reads (design §4.4), the reset the
/// connections refuse their work on, what status reports of the replicator, and the root's lock.
/// The lock is released when the binding retires with no connection left, and otherwise (a reset)
/// only when the last handle drops. So a reopen while old handles live is refused `in_use` and
/// never materializes a generation they still write (M2 builds it a copy instead).
#[derive(Debug)]
pub(crate) struct BindingHandle {
    pub(crate) generation: Generation,
    pub(crate) name: CanonicalPath,
    /// The stream's cluster id and incarnation token when the binding bound.
    pub(crate) cluster_id: String,
    pub(crate) incarnation: String,
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
    life: Mutex<Life>,
    replication: Mutex<Replication>,
    /// Released when the binding retires with no connection left; otherwise with the last handle.
    root: Mutex<Option<Arc<Root>>>,
}

/// Where a binding stands between its connections and the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Life {
    Live,
    /// Its last connection closed: it drains, then retires. A new open revives it first.
    Retiring,
    /// Gone from the driver.
    Retired,
}

/// What status reports of a binding's replicator (design §9).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Replication {
    pub(crate) state: &'static str,
    pub(crate) epoch: Option<u64>,
    pub(crate) condition: Option<Condition>,
    /// Logged commits not yet acknowledged.
    pub(crate) pending: usize,
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
            name: bind.name,
            cluster_id: bind.cluster_id,
            incarnation: bind.incarnation,
            log: Mutex::new(bind.log),
            pending_bytes: AtomicU64::new(bind.pending_bytes),
            max_pending_bytes: bind.max_pending_bytes,
            max_frame_bytes: AtomicU64::new(bind.max_frame_bytes),
            acked: AtomicU64::new(bind.acked.0),
            reset: AtomicU8::new(0),
            life: Mutex::new(Life::Live),
            replication: Mutex::new(Replication::default()),
            last_commit: AtomicU64::new(0),
            root: Mutex::new(Some(root)),
        }
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

    fn add_pending(&self, len: u64) {
        // Only the driver thread changes it, so a load and a store do.
        let now = self.pending_bytes().saturating_add(len);
        self.pending_bytes.store(now, Ordering::Release);
    }

    fn remove_pending(&self, len: u64) {
        let now = self.pending_bytes().saturating_sub(len);
        self.pending_bytes.store(now, Ordering::Release);
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

    fn life_lock(&self) -> MutexGuard<'_, Life> {
        self.life.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn life(&self) -> Life {
        *self.life_lock()
    }

    /// Its last connection closed: it retires once drained.
    pub(crate) fn retiring(&self) {
        let mut life = self.life_lock();
        if *life == Life::Live {
            *life = Life::Retiring;
        }
    }

    /// A new open takes a retiring binding back: false once it retired.
    pub(crate) fn revive(&self) -> bool {
        let mut life = self.life_lock();
        match *life {
            Life::Live => true,
            Life::Retiring => {
                *life = Life::Live;
                true
            }
            Life::Retired => false,
        }
    }

    /// Retires a retiring binding: false when it was revived (or retired) meanwhile.
    fn try_retire(&self) -> bool {
        let mut life = self.life_lock();
        let retiring = *life == Life::Retiring;
        if retiring {
            *life = Life::Retired;
        }
        retiring
    }

    fn retired(&self) {
        *self.life_lock() = Life::Retired;
    }

    /// Unlocks the root of a binding that retired with no connection left, so another process
    /// opens the stream while this one still holds the handle (the registry keeps it until the next
    /// open of the name).
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
    pub(crate) incarnation: String,
    pub(crate) log: Log,
    pub(crate) pending_bytes: u64,
    pub(crate) max_pending_bytes: u64,
    pub(crate) max_frame_bytes: u64,
    pub(crate) acked: Position,
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
    Acked {
        binding: BindingId,
        acked: Position,
    },
    Condition {
        binding: BindingId,
        condition: Option<Condition>,
    },
    Reset {
        binding: BindingId,
        cause: ResetKind,
    },
    Synced {
        ticket: Ticket,
    },
    SyncFailed {
        ticket: Ticket,
        error: SyncError,
    },
    /// The binding is gone from the driver. Its root unlocks once the last handle drops.
    Retired {
        binding: BindingId,
    },
    /// `dropped` events fell out of the bounded queue before this reader saw them.
    Gap {
        dropped: u64,
    },
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
        if let DriverEvent::Acked { binding, .. } = &event {
            let binding = *binding;
            self.events.retain(
                |(_, e)| !matches!(e, DriverEvent::Acked { binding: b, .. } if *b == binding),
            );
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
