//! A faulty in-memory snapshot bucket that every node of a test shares,
//! and each node's [`SnapshotStore`] on it.
//!
//! The bucket is a map behind a mutex. Every request draws a delay to wait
//! before it acts and one to wait after it, and acts on the map in between,
//! so concurrent requests interleave as the seed's draws order them. Its
//! faults ([`SnapshotStoreFaults`]) model what an S3-compatible store does
//! to a client that the ownership protocol (#687) must survive:
//!
//! - a PUT that answers with an error and lands anyway, before or after
//!   the answer, or that never lands
//! - a PUT that lands its own bytes and then answers `NameCollision`, as a
//!   retried create that finds its own first attempt
//! - a LIST that omits recently created keys and repeats recently deleted
//!   ones, for changes younger than a bound
//! - a create that succeeds only if no object has the name (If-None-Match),
//!   checked again when a late PUT lands
//! - opt in: a DELETE that fails, one that deletes and answers an error,
//!   and transient LIST, GET and HEAD errors
//!
//! Every fault, delay and nonce comes from the bucket's seed, never from a
//! thread or process RNG, so one seed and one order of requests replay the
//! same run. Under madsim the delays are simulated time. Reading the
//! bucket ([`MemorySnapshotBucket::view`], [`MemorySnapshotBucket::keys`])
//! changes nothing in it, so a checker can run at any step without
//! changing the replay.
//!
//! A DELETE of a key outside the deleting node's own directory is a
//! violation of invariant I2, and the store panics on it. A negative control
//! that wants to see the damage builds its bucket with
//! [`MemorySnapshotBucket::recording_foreign_deletes`] instead.
//!
//! The store keeps no reference records and its `prune_retired` deletes
//! nothing, so on the protocol before P3 its objects only accumulate. GC is
//! the owner's pass over [`SnapshotStore::list_own`] and
//! [`SnapshotStore::delete_own`] that P3 brings.
//!
//! Quiescence. [`MemorySnapshotBucket::settle`] lands every late PUT at
//! once, pulling future landings forward. That hides the second GC pass P3
//! owes one install timeout after a failed write, so a simulation reaches
//! quiescence with [`MemorySnapshotBucket::wait_for_late_puts`] instead,
//! which advances simulated time until the last late PUT landed. For that
//! to model the protocol, `late_put_delay.max` must stay at or below the
//! protocol's second-pass delay (`raft.install_snapshot_timeout`).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;

use bytes::Bytes;
use ursula_shard::RaftGroupId;

use super::SplitMix64;
use super::invariants::SnapshotStoreView;
use super::invariants::StoreDelete;
use super::invariants::ViewObject;
use super::is_owned_by;
use crate::SnapshotStoreOperation;
use crate::rt::time::Instant;
use crate::snapshot_store::OwnListing;
use crate::snapshot_store::OwnedDirectory;
use crate::snapshot_store::OwnedKey;
use crate::snapshot_store::OwnedNamespace;
use crate::snapshot_store::OwnedObject;
use crate::snapshot_store::SnapshotBackend;
use crate::snapshot_store::SnapshotBytesIterator;
use crate::snapshot_store::SnapshotCompression;
use crate::snapshot_store::SnapshotKey;
use crate::snapshot_store::SnapshotLocation;
use crate::snapshot_store::SnapshotOwner;
use crate::snapshot_store::SnapshotPrefix;
use crate::snapshot_store::SnapshotStore;
use crate::snapshot_store::SnapshotStoreError;
use crate::snapshot_store::SnapshotStoreFuture;
use crate::snapshot_store::StagedObject;
use crate::snapshot_store::StagedSnapshot;

/// The zstd level of staged objects, as the S3 store compresses them.
const ZSTD_LEVEL: i32 = 3;
/// How many of the most recent creates and deletes the bucket remembers
/// for stale LISTs, whatever the faults are when they happen.
const HISTORY_CAPACITY: usize = 1024;
/// Mixed into the seed of [`SnapshotStoreFaults::from_seed`], so a profile
/// does not repeat the bucket's own draws of the same seed.
const PROFILE_SALT: u64 = 0x5052_4F46_494C_4531;
/// Mixed into each node's nonce seed, for the same reason.
const NONCE_SALT: u64 = 0x4E4F_4E43_4553_3031;

/// A probability in thousandths, at most 1000.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct PerMille(u16);

impl PerMille {
    pub const NEVER: Self = Self(0);
    pub const ALWAYS: Self = Self(1000);

    /// `thousandths`, capped at 1000.
    pub fn new(thousandths: u16) -> Self {
        Self(thousandths.min(1000))
    }

    pub fn get(self) -> u16 {
        self.0
    }

    /// One draw: whether the event happens.
    fn draw(self, rng: &mut SplitMix64) -> bool {
        rng.below(1000) < u64::from(self.0)
    }
}

/// A delay drawn uniformly from `min..=max`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DelayRange {
    pub min: Duration,
    pub max: Duration,
}

impl DelayRange {
    pub const ZERO: Self = Self {
        min: Duration::ZERO,
        max: Duration::ZERO,
    };

    /// A delay between `a` and `b`, in either order.
    pub fn between(a: Duration, b: Duration) -> Self {
        Self {
            min: a.min(b),
            max: a.max(b),
        }
    }

    /// Always `delay`.
    pub fn fixed(delay: Duration) -> Self {
        Self::between(delay, delay)
    }

    /// One draw. It always takes one value from `rng`, so a zero range
    /// leaves the later draws where a nonzero one would.
    fn draw(self, rng: &mut SplitMix64) -> Duration {
        let span = nanos(self.max.saturating_sub(self.min));
        let offset = rng.below(span.saturating_add(1));
        self.min.saturating_add(Duration::from_nanos(offset))
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What the store does on a DELETE outside the deleting node's own
/// directory (invariant I2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForeignDeletes {
    /// Records the attempt and panics in the deleting task.
    Panic,
    /// Records the attempt. [`SnapshotStore::delete_own`] refuses it as the
    /// S3 store does, and [`SnapshotStore::delete`], which S3 does not
    /// scope, deletes the key. The bucket panics when it is dropped with
    /// recorded attempts no one read.
    Record,
}

/// The fault knobs of a [`MemorySnapshotBucket`]. A test sets them when it
/// builds the bucket and may change them at any point
/// ([`MemorySnapshotBucket::set_faults`]). [`SnapshotStoreFaults::from_seed`]
/// draws a profile per seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotStoreFaults {
    /// Waited before a request acts on the bucket.
    pub request_start_delay: DelayRange,
    /// Waited after a request acted, before it answers.
    pub request_end_delay: DelayRange,
    /// A PUT that answers with an error and still lands.
    pub late_put: PerMille,
    /// How long after the PUT acted a late PUT lands. Shorter than the
    /// request's end delay, it lands before the client sees the error: the
    /// write committed and only its answer was lost.
    pub late_put_delay: DelayRange,
    /// A PUT that answers with an error and never lands.
    pub lost_put: PerMille,
    /// A PUT that creates its object and answers `NameCollision`, as the S3
    /// store's retried create does when its first attempt landed but timed
    /// out: the retry finds its own object. Opt in.
    pub collided_retry: PerMille,
    /// A LIST that is stale: each eligible recent create in its directory
    /// is left out, and each eligible recent delete listed again, with even
    /// odds.
    pub stale_list: PerMille,
    /// A stale LIST considers at most this many of the most recent changes.
    pub stale_window: usize,
    /// A stale LIST considers only changes younger than this, so a quiet
    /// bucket lists every key once `stale_for` passed.
    pub stale_for: Duration,
    /// A DELETE that answers with an error and deletes nothing. Opt in.
    pub failed_delete: PerMille,
    /// A DELETE that deletes and answers with an error. Opt in.
    pub lost_delete_answer: PerMille,
    /// A LIST, GET or HEAD that answers a temporary error. Opt in.
    pub transient_error: PerMille,
}

impl Default for SnapshotStoreFaults {
    fn default() -> Self {
        Self::none()
    }
}

impl SnapshotStoreFaults {
    /// A strict store that answers at once: no delays and no faults.
    pub fn none() -> Self {
        Self {
            request_start_delay: DelayRange::ZERO,
            request_end_delay: DelayRange::ZERO,
            late_put: PerMille::NEVER,
            late_put_delay: DelayRange::ZERO,
            lost_put: PerMille::NEVER,
            collided_retry: PerMille::NEVER,
            stale_list: PerMille::NEVER,
            stale_window: 0,
            stale_for: Duration::ZERO,
            failed_delete: PerMille::NEVER,
            lost_delete_answer: PerMille::NEVER,
            transient_error: PerMille::NEVER,
        }
    }

    /// The PUT and LIST faults at fixed rates, with request delays of up to
    /// `max_delay`, late PUTs landing up to four of them after they acted,
    /// and stale LISTs over changes younger than four of them. The opt-in
    /// DELETE and transient faults stay off.
    pub fn every_fault(max_delay: Duration) -> Self {
        let four = max_delay.saturating_mul(4);
        Self {
            request_start_delay: DelayRange::between(Duration::ZERO, max_delay),
            request_end_delay: DelayRange::between(Duration::ZERO, max_delay),
            late_put: PerMille::new(200),
            late_put_delay: DelayRange::between(Duration::ZERO, four),
            lost_put: PerMille::new(50),
            ..Self::none()
        }
        .with_stale_lists(PerMille::new(300), 8, four)
    }

    /// A profile drawn from `seed`: request delays of up to `max_delay`,
    /// and late and lost PUTs at rates the seed picks. Stale LISTs stay
    /// off, so a pass sees every key and a liveness check holds. Turn them
    /// on with [`Self::with_stale_lists`].
    pub fn from_seed(seed: u64, max_delay: Duration) -> Self {
        let mut rng = SplitMix64::new(seed ^ PROFILE_SALT);
        let mut rate = |most: u64| PerMille::new(u16::try_from(rng.below(most)).unwrap_or(0));
        let late_put = rate(300);
        let lost_put = rate(100);
        Self {
            late_put,
            lost_put,
            stale_list: PerMille::NEVER,
            ..Self::every_fault(max_delay)
        }
    }

    /// Stale LISTs at `rate`, over the `window` most recent changes younger
    /// than `stale_for`. A check of the #669 bound then allows the bound
    /// plus `stale_for`.
    pub fn with_stale_lists(self, rate: PerMille, window: usize, stale_for: Duration) -> Self {
        Self {
            stale_list: rate,
            stale_window: window,
            stale_for,
            ..self
        }
    }

    /// PUTs that land and answer `NameCollision`, at `rate`.
    pub fn with_collided_retries(self, rate: PerMille) -> Self {
        Self {
            collided_retry: rate,
            ..self
        }
    }

    /// The opt-in failures: DELETEs that fail, DELETEs whose answer is lost
    /// and transient LIST, GET and HEAD errors.
    pub fn with_failed_requests(
        self,
        failed_delete: PerMille,
        lost_delete_answer: PerMille,
        transient_error: PerMille,
    ) -> Self {
        Self {
            failed_delete,
            lost_delete_answer,
            transient_error,
            ..self
        }
    }
}

/// How a PUT answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object was created and the PUT answered success.
    Created,
    /// An object had the name, so nothing was written (If-None-Match).
    NameCollision,
    /// The PUT answered an error, and the object lands `after` the PUT
    /// acted unless an object has the name by then.
    Late { after: Duration },
    /// The PUT answered an error, and nothing lands.
    Lost,
    /// The object was created and the PUT answered `NameCollision`.
    CreatedThenCollided,
}

/// How a DELETE answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// The key is gone and the DELETE answered success.
    Deleted,
    /// The DELETE answered an error and deleted nothing.
    Failed,
    /// The key is gone and the DELETE answered an error.
    AnswerLost,
}

/// One request the bucket served, in the order it acted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotStoreEvent {
    Put {
        node_id: u64,
        path: String,
        outcome: PutOutcome,
    },
    /// A late PUT created its object.
    LatePutLanded { path: String },
    /// A late PUT found an object with its name and wrote nothing.
    LatePutRefused { path: String },
    /// A LIST of `node_id`'s directory for a group. `listed` is what it
    /// answered, `omitted` the recent keys it left out and `repeated` the
    /// deleted keys it listed again.
    List {
        node_id: u64,
        raft_group_id: RaftGroupId,
        listed: Vec<String>,
        omitted: Vec<String>,
        repeated: Vec<String>,
    },
    Get {
        node_id: u64,
        path: String,
        found: bool,
    },
    Head {
        node_id: u64,
        path: String,
        found: bool,
    },
    /// A DELETE in the deleting node's own directory.
    Delete {
        node_id: u64,
        path: String,
        existed: bool,
        outcome: DeleteOutcome,
    },
    /// A DELETE outside the deleting node's own directory (I2).
    ForeignDelete { node_id: u64, path: String },
    /// A LIST, GET or HEAD that answered a temporary error. `path` is the
    /// object, or the listed directory.
    TransientError {
        node_id: u64,
        operation: SnapshotStoreOperation,
        path: String,
    },
}

#[derive(Debug, Clone)]
struct StoredObject {
    stored: Vec<u8>,
    compression: SnapshotCompression,
    size_bytes: u64,
    landed_at: Instant,
}

#[derive(Debug)]
struct LatePut {
    lands_at: Instant,
    path: String,
    object: StoredObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeKind {
    Created,
    Deleted,
}

#[derive(Debug, Clone)]
struct Change {
    kind: ChangeKind,
    path: String,
    at: Instant,
}

#[derive(Debug)]
struct BucketState {
    faults: SnapshotStoreFaults,
    /// Draws faults and delays, in the order requests act.
    rng: SplitMix64,
    /// Each node's nonce sequence, kept across its stores so a node that
    /// restarts does not draw its earlier names again.
    nonces: BTreeMap<u64, SplitMix64>,
    objects: BTreeMap<String, StoredObject>,
    /// Late PUTs in the order they acted.
    pending: Vec<LatePut>,
    /// The most recent creates and deletes, oldest first.
    changes: VecDeque<Change>,
    events: Vec<SnapshotStoreEvent>,
    /// How many recorded foreign deletes a reader took.
    foreign_deletes_read: usize,
}

#[derive(Debug)]
struct Shared {
    seed: u64,
    prefix: SnapshotPrefix,
    foreign_deletes: ForeignDeletes,
    state: Mutex<BucketState>,
}

impl Drop for Shared {
    #[expect(
        clippy::panic,
        reason = "a negative control must read the foreign deletes it recorded"
    )]
    fn drop(&mut self) {
        if self.foreign_deletes != ForeignDeletes::Record || std::thread::panicking() {
            return;
        }
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        let recorded = state
            .events
            .iter()
            .filter(|event| matches!(event, SnapshotStoreEvent::ForeignDelete { .. }))
            .count();
        if recorded > state.foreign_deletes_read {
            panic!(
                "the bucket recorded {recorded} foreign deletes and a reader took only {}",
                state.foreign_deletes_read
            );
        }
    }
}

/// The bucket every node of a test shares. Clones share it.
#[derive(Debug, Clone)]
pub struct MemorySnapshotBucket {
    shared: Arc<Shared>,
}

impl MemorySnapshotBucket {
    /// An empty bucket that draws from `seed`, keeps objects under `prefix`
    /// and panics on a DELETE outside the deleting node's directory.
    pub fn new(seed: u64, prefix: SnapshotPrefix, faults: SnapshotStoreFaults) -> Self {
        Self::with_foreign_deletes(seed, prefix, faults, ForeignDeletes::Panic)
    }

    /// An empty bucket that records a DELETE outside the deleting node's
    /// directory and lets it go ahead as S3 would, for a negative control
    /// of I2. It panics when dropped unless [`Self::foreign_deletes`] took
    /// every recorded one.
    pub fn recording_foreign_deletes(
        seed: u64,
        prefix: SnapshotPrefix,
        faults: SnapshotStoreFaults,
    ) -> Self {
        Self::with_foreign_deletes(seed, prefix, faults, ForeignDeletes::Record)
    }

    fn with_foreign_deletes(
        seed: u64,
        prefix: SnapshotPrefix,
        faults: SnapshotStoreFaults,
        foreign_deletes: ForeignDeletes,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                seed,
                prefix,
                foreign_deletes,
                state: Mutex::new(BucketState {
                    faults,
                    rng: SplitMix64::new(seed),
                    nonces: BTreeMap::new(),
                    objects: BTreeMap::new(),
                    pending: Vec::new(),
                    changes: VecDeque::new(),
                    events: Vec::new(),
                    foreign_deletes_read: 0,
                }),
            }),
        }
    }

    /// Node `node_id`'s store on this bucket. A node that restarts takes a
    /// new store and continues its nonce sequence.
    pub fn store(&self, node_id: u64) -> MemorySnapshotStore {
        MemorySnapshotStore {
            bucket: self.clone(),
            node_id,
            nonces: None,
        }
    }

    pub fn seed(&self) -> u64 {
        self.shared.seed
    }

    pub fn prefix(&self) -> &SnapshotPrefix {
        &self.shared.prefix
    }

    pub fn faults(&self) -> SnapshotStoreFaults {
        self.state().faults.clone()
    }

    /// Changes the faults of every later request.
    pub fn set_faults(&self, faults: SnapshotStoreFaults) {
        self.state().faults = faults;
    }

    /// Lands every late PUT now, in the order they were due, logged as each
    /// landing. It pulls future landings forward, which hides the second GC
    /// pass a protocol owes after a failed write: a simulation uses
    /// [`Self::wait_for_late_puts`] instead.
    pub fn settle(&self) {
        let now = Instant::now();
        self.state().land_all(now);
    }

    /// Advances time, simulated under madsim, until every late PUT pending
    /// now landed, then lands them, logged as each landing. Nothing lands
    /// before its time.
    pub async fn wait_for_late_puts(&self) {
        let last = self.state().pending.iter().map(|put| put.lands_at).max();
        if let Some(last) = last {
            wait(last.saturating_duration_since(Instant::now())).await;
        }
        let now = Instant::now();
        self.state().land_due(now);
    }

    /// The late PUTs that have not landed by now.
    pub fn pending_late_puts(&self) -> usize {
        let now = Instant::now();
        self.state()
            .pending
            .iter()
            .filter(|put| put.lands_at > now)
            .count()
    }

    /// The keys the bucket holds now, late PUTs that are due included.
    pub fn keys(&self) -> BTreeSet<String> {
        self.state()
            .current_objects(Instant::now())
            .into_keys()
            .collect()
    }

    /// Every request the bucket served, in the order it acted.
    pub fn events(&self) -> Vec<SnapshotStoreEvent> {
        self.state().events.clone()
    }

    /// Every DELETE outside the deleting node's directory the bucket
    /// recorded. Taking them marks them read.
    pub fn foreign_deletes(&self) -> Vec<StoreDelete> {
        let mut state = self.state();
        let deletes: Vec<StoreDelete> = state
            .events
            .iter()
            .filter_map(|event| match event {
                SnapshotStoreEvent::ForeignDelete { node_id, path } => Some(StoreDelete {
                    node_id: *node_id,
                    path: path.clone(),
                }),
                _ => None,
            })
            .collect();
        state.foreign_deletes_read = deletes.len();
        deletes
    }

    /// Writes `bytes` at `path` with no checks, faults or delays, as
    /// another writer of the bucket would.
    pub fn insert_raw(&self, path: &str, bytes: Vec<u8>) {
        let now = Instant::now();
        let mut state = self.state();
        let size_bytes = bytes.len() as u64;
        state.objects.insert(path.to_owned(), StoredObject {
            stored: bytes,
            compression: SnapshotCompression::None,
            size_bytes,
            landed_at: now,
        });
        state.record_change(ChangeKind::Created, path.to_owned(), now);
    }

    /// What the invariant checkers read: every object with its logical
    /// bytes and age, late PUTs that are due included, and every DELETE
    /// the bucket served or refused. It changes nothing in the bucket, and
    /// does not take the recorded foreign deletes
    /// ([`Self::foreign_deletes`]).
    pub fn view(&self) -> SnapshotStoreView {
        let now = Instant::now();
        let state = self.state();
        let objects = state
            .current_objects(now)
            .into_iter()
            .map(|(path, object)| {
                let bytes =
                    decode(&path, &object).unwrap_or_else(|_undecodable| object.stored.clone());
                let age = now.saturating_duration_since(object.landed_at);
                (path, ViewObject { bytes, age })
            })
            .collect();
        let deletes = state
            .events
            .iter()
            .filter_map(|event| match event {
                SnapshotStoreEvent::Delete { node_id, path, .. }
                | SnapshotStoreEvent::ForeignDelete { node_id, path } => Some(StoreDelete {
                    node_id: *node_id,
                    path: path.clone(),
                }),
                _ => None,
            })
            .collect();
        SnapshotStoreView {
            prefix: self.shared.prefix.clone(),
            objects,
            deletes,
        }
    }

    fn state(&self) -> MutexGuard<'_, BucketState> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Serves one request: waits the drawn start delay, lands the late PUTs
    /// that are due, runs `act` on the bucket at the current time, then
    /// waits the drawn end delay.
    async fn request<T>(&self, act: impl FnOnce(&mut BucketState, Instant) -> T) -> T {
        let (start, end) = {
            let mut state = self.state();
            let faults = state.faults.clone();
            let start = faults.request_start_delay.draw(&mut state.rng);
            let end = faults.request_end_delay.draw(&mut state.rng);
            (start, end)
        };
        wait(start).await;
        let answer = {
            let mut state = self.state();
            let now = Instant::now();
            state.land_due(now);
            act(&mut state, now)
        };
        wait(end).await;
        answer
    }
}

async fn wait(delay: Duration) {
    if !delay.is_zero() {
        crate::rt::time::sleep(delay).await;
    }
}

/// `delay` after `now`, or `now` past the clock's end.
fn later(now: Instant, delay: Duration) -> Instant {
    now.checked_add(delay).unwrap_or(now)
}

impl BucketState {
    /// Records a change in time order: a late PUT lands at its own time,
    /// which may be before changes already recorded.
    fn record_change(&mut self, kind: ChangeKind, path: String, at: Instant) {
        let position = self
            .changes
            .iter()
            .rposition(|change| change.at <= at)
            .map_or(0, |index| index.saturating_add(1));
        self.changes.insert(position, Change { kind, path, at });
        while self.changes.len() > HISTORY_CAPACITY {
            self.changes.pop_front();
        }
    }

    /// The objects as of `now`, late PUTs that are due included, without
    /// landing them.
    fn current_objects(&self, now: Instant) -> BTreeMap<String, StoredObject> {
        let mut objects = self.objects.clone();
        let mut due: Vec<&LatePut> = self
            .pending
            .iter()
            .filter(|put| put.lands_at <= now)
            .collect();
        due.sort_by_key(|put| put.lands_at);
        for put in due {
            objects
                .entry(put.path.clone())
                .or_insert_with(|| StoredObject {
                    landed_at: put.lands_at,
                    ..put.object.clone()
                });
        }
        objects
    }

    fn land_due(&mut self, now: Instant) {
        let (mut due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|put| put.lands_at <= now);
        self.pending = rest;
        due.sort_by_key(|put| put.lands_at);
        for put in due {
            let at = put.lands_at;
            self.land(put, at);
        }
    }

    fn land_all(&mut self, now: Instant) {
        let mut due = std::mem::take(&mut self.pending);
        due.sort_by_key(|put| put.lands_at);
        for put in due {
            let at = put.lands_at.min(now);
            self.land(put, at);
        }
    }

    fn land(&mut self, put: LatePut, at: Instant) {
        if self.objects.contains_key(&put.path) {
            self.events
                .push(SnapshotStoreEvent::LatePutRefused { path: put.path });
            return;
        }
        self.objects.insert(put.path.clone(), StoredObject {
            landed_at: at,
            ..put.object
        });
        self.record_change(ChangeKind::Created, put.path.clone(), at);
        self.events
            .push(SnapshotStoreEvent::LatePutLanded { path: put.path });
    }

    fn next_nonce(&mut self, seed: u64, node_id: u64) -> u64 {
        self.nonces
            .entry(node_id)
            .or_insert_with(|| {
                SplitMix64::new(seed ^ NONCE_SALT ^ node_id.wrapping_mul(0xD6E8_FEB8_6659_FD93))
            })
            .next_u64()
    }

    /// A temporary error for `operation` on `path`, if the faults draw one.
    fn transient(
        &mut self,
        node_id: u64,
        operation: SnapshotStoreOperation,
        path: &str,
    ) -> Result<(), SnapshotStoreError> {
        let faults = self.faults.clone();
        if !faults.transient_error.draw(&mut self.rng) {
            return Ok(());
        }
        self.events.push(SnapshotStoreEvent::TransientError {
            node_id,
            operation,
            path: path.to_owned(),
        });
        Err(injected(operation, path.to_owned(), "a transient error"))
    }

    /// One PUT of `object` at `path` by `node_id`, acting at `now`.
    fn put(
        &mut self,
        node_id: u64,
        path: String,
        object: StoredObject,
        now: Instant,
    ) -> Result<(), SnapshotStoreError> {
        let faults = self.faults.clone();
        let outcome = if faults.late_put.draw(&mut self.rng) {
            let after = faults.late_put_delay.draw(&mut self.rng);
            self.pending.push(LatePut {
                lands_at: later(now, after),
                path: path.clone(),
                object,
            });
            PutOutcome::Late { after }
        } else if faults.lost_put.draw(&mut self.rng) {
            PutOutcome::Lost
        } else if self.objects.contains_key(&path) {
            PutOutcome::NameCollision
        } else {
            self.objects.insert(path.clone(), StoredObject {
                landed_at: now,
                ..object
            });
            self.record_change(ChangeKind::Created, path.clone(), now);
            if faults.collided_retry.draw(&mut self.rng) {
                PutOutcome::CreatedThenCollided
            } else {
                PutOutcome::Created
            }
        };
        self.events.push(SnapshotStoreEvent::Put {
            node_id,
            path: path.clone(),
            outcome: outcome.clone(),
        });
        match outcome {
            PutOutcome::Created => Ok(()),
            PutOutcome::NameCollision | PutOutcome::CreatedThenCollided => {
                Err(SnapshotStoreError::NameCollision { key: path })
            }
            PutOutcome::Late { .. } | PutOutcome::Lost => Err(injected(
                SnapshotStoreOperation::Put,
                path,
                "the PUT failed to the client",
            )),
        }
    }

    /// A LIST of `owner`'s directory under `prefix` at `now`.
    fn list(
        &mut self,
        prefix: &SnapshotPrefix,
        owner: SnapshotOwner,
        now: Instant,
    ) -> Result<OwnListing, SnapshotStoreError> {
        let directory = owner.directory(prefix);
        self.transient(owner.node_id, SnapshotStoreOperation::List, &directory)?;
        let mut listed: BTreeSet<String> = self
            .objects
            .keys()
            .filter(|path| path.starts_with(&directory))
            .cloned()
            .collect();
        let mut omitted = Vec::new();
        let mut repeated = Vec::new();
        let faults = self.faults.clone();
        if faults.stale_list.draw(&mut self.rng) {
            let eligible: Vec<Change> = self
                .changes
                .iter()
                .rev()
                .take(faults.stale_window)
                .filter(|change| now.saturating_duration_since(change.at) < faults.stale_for)
                .filter(|change| change.path.starts_with(&directory))
                .cloned()
                .collect();
            for change in eligible {
                let coin = self.rng.below(2) == 1;
                match change.kind {
                    ChangeKind::Created => {
                        if coin && listed.remove(&change.path) {
                            omitted.push(change.path);
                        }
                    }
                    ChangeKind::Deleted => {
                        if coin
                            && !self.objects.contains_key(&change.path)
                            && listed.insert(change.path.clone())
                        {
                            repeated.push(change.path);
                        }
                    }
                }
            }
        }
        let mut listing = OwnListing::default();
        for path in &listed {
            match OwnedKey::parse(prefix, path) {
                Ok(key) if key.owner == owner => listing.owned.push(OwnedObject {
                    key,
                    path: path.clone(),
                }),
                Ok(_) | Err(_) => listing.foreign.push(path.clone()),
            }
        }
        self.events.push(SnapshotStoreEvent::List {
            node_id: owner.node_id,
            raft_group_id: owner.raft_group_id,
            listed: listed.into_iter().collect(),
            omitted,
            repeated,
        });
        Ok(listing)
    }

    /// A DELETE of `path` by `node_id`, in its own directory, at `now`.
    fn delete(
        &mut self,
        node_id: u64,
        path: String,
        now: Instant,
    ) -> Result<(), SnapshotStoreError> {
        let faults = self.faults.clone();
        let existed = self.objects.contains_key(&path);
        let outcome = if faults.failed_delete.draw(&mut self.rng) {
            DeleteOutcome::Failed
        } else {
            if self.objects.remove(&path).is_some() {
                self.record_change(ChangeKind::Deleted, path.clone(), now);
            }
            if faults.lost_delete_answer.draw(&mut self.rng) {
                DeleteOutcome::AnswerLost
            } else {
                DeleteOutcome::Deleted
            }
        };
        self.events.push(SnapshotStoreEvent::Delete {
            node_id,
            path: path.clone(),
            existed,
            outcome,
        });
        match outcome {
            DeleteOutcome::Deleted => Ok(()),
            DeleteOutcome::Failed | DeleteOutcome::AnswerLost => Err(injected(
                SnapshotStoreOperation::Delete,
                path,
                "the DELETE failed to the client",
            )),
        }
    }
}

/// A backend failure the bucket injected.
fn injected(
    operation: SnapshotStoreOperation,
    path: String,
    what: &'static str,
) -> SnapshotStoreError {
    SnapshotStoreError::backend(
        operation,
        path,
        opendal::Error::new(opendal::ErrorKind::Unexpected, what).set_temporary(),
    )
}

/// The logical bytes of a stored object.
fn decode(path: &str, object: &StoredObject) -> Result<Vec<u8>, SnapshotStoreError> {
    match object.compression {
        SnapshotCompression::None => Ok(object.stored.clone()),
        SnapshotCompression::Zstd => {
            let capacity = usize::try_from(object.size_bytes).map_err(|_overflow| {
                SnapshotStoreError::LogicalSizeUnaddressable {
                    key: path.to_owned(),
                    size_bytes: object.size_bytes,
                }
            })?;
            zstd::bulk::decompress(&object.stored, capacity).map_err(|source| {
                SnapshotStoreError::Decompress {
                    key: path.to_owned(),
                    source,
                }
            })
        }
    }
}

/// Collects and compresses a snapshot body, inline: a test store has no
/// worker to keep free.
fn compress(chunks: SnapshotBytesIterator) -> Result<(Vec<u8>, u64), SnapshotStoreError> {
    let mut size_bytes = 0u64;
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL)
        .map_err(SnapshotStoreError::Compress)?;
    for chunk in chunks {
        let chunk = chunk?;
        size_bytes = size_bytes
            .checked_add(chunk.len() as u64)
            .ok_or(SnapshotStoreError::BodyTooLarge)?;
        encoder
            .write_all(&chunk)
            .map_err(SnapshotStoreError::Compress)?;
    }
    let stored = encoder.finish().map_err(SnapshotStoreError::Compress)?;
    Ok((stored, size_bytes))
}

/// The log index a snapshot id ends with (`group-{gid}-{leader}-{index}`).
/// An id without one, as `group-{gid}-empty`, is refused: an owned key
/// needs the index, and no index is made up for it. Only the legacy
/// `upload` path names a snapshot by id. OpenRaft alpha.37 leaves the id
/// empty, so after the bump that path is always refused, and P3, which
/// stages builds and copies by their log index, removes it.
fn snapshot_index(snapshot_id: &str) -> Result<u64, SnapshotStoreError> {
    snapshot_id
        .rsplit_once('-')
        .and_then(|(_, index)| index.parse().ok())
        .ok_or(SnapshotStoreError::MissingField {
            field: "the log index of the snapshot id",
        })
}

/// Node `node_id`'s store on a [`MemorySnapshotBucket`]. It implements the
/// whole [`SnapshotStore`] trait, so a simulation injects it with the
/// registry's `set_snapshot_store`.
///
/// - The owned-key methods work as the S3 store's do.
/// - `upload` and `upload_iter` stage and write under a fresh owned key at
///   the snapshot id's log index.
/// - `download` is a GET, and `verify_uploaded` a HEAD.
/// - `delete` is a DELETE that S3 does not scope, so I2 is asserted here.
/// - The reference methods and `prune_retired` are the trait's no-op
///   defaults. GC is the owner's pass over `list_own` and `delete_own`.
#[derive(Debug)]
pub struct MemorySnapshotStore {
    bucket: MemorySnapshotBucket,
    node_id: u64,
    /// Draws nonces in place of the bucket's seeded sequence for this node.
    nonces: Option<fn() -> u64>,
}

impl MemorySnapshotStore {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn bucket(&self) -> &MemorySnapshotBucket {
        &self.bucket
    }

    /// Draws the nonces of staged objects from `nonces` instead of the
    /// bucket's seed, as `S3SnapshotStore::with_nonces` does, so a test
    /// can force a name collision.
    pub fn with_nonces(mut self, nonces: fn() -> u64) -> Self {
        self.nonces = Some(nonces);
        self
    }

    fn owner(&self, raft_group_id: RaftGroupId) -> SnapshotOwner {
        SnapshotOwner {
            node_id: self.node_id,
            raft_group_id,
        }
    }

    fn prefix(&self) -> &SnapshotPrefix {
        self.bucket.prefix()
    }

    fn nonce(&self) -> u64 {
        match self.nonces {
            Some(nonces) => nonces(),
            None => self
                .bucket
                .state()
                .next_nonce(self.bucket.seed(), self.node_id),
        }
    }

    /// Records a DELETE of `path` outside this node's directory, and panics
    /// unless the bucket only records it.
    #[expect(
        clippy::panic,
        reason = "the I2 assertion of a test store fails the deleting task"
    )]
    fn foreign_delete(&self, path: &str) {
        self.bucket
            .state()
            .events
            .push(SnapshotStoreEvent::ForeignDelete {
                node_id: self.node_id,
                path: path.to_owned(),
            });
        if self.bucket.shared.foreign_deletes == ForeignDeletes::Panic {
            panic!(
                "I2: node {} deleted {path}, outside its own directory {}",
                self.node_id,
                super::node_directory(self.prefix(), self.node_id)
            );
        }
    }

    async fn stage_body(
        &self,
        raft_group_id: RaftGroupId,
        index: u64,
        chunks: SnapshotBytesIterator,
    ) -> Result<StagedSnapshot, SnapshotStoreError> {
        let (stored, size_bytes) = compress(chunks)?;
        Ok(StagedSnapshot::Object(StagedObject {
            key: OwnedKey {
                owner: self.owner(raft_group_id),
                index,
                nonce: self.nonce(),
            },
            prefix: self.prefix().clone(),
            size_bytes,
            stored,
        }))
    }

    async fn write(&self, staged: StagedSnapshot) -> Result<SnapshotLocation, SnapshotStoreError> {
        let object = match staged {
            StagedSnapshot::Inline { bytes } => return Ok(SnapshotLocation::Inline { bytes }),
            StagedSnapshot::Object(object) => object,
        };
        if &object.prefix != self.prefix() || object.key.owner.node_id != self.node_id {
            return Err(SnapshotStoreError::NotOwnedHere {
                path: object.path(),
            });
        }
        let location = object.location();
        let path = object.path();
        let compression = location.compression();
        let node_id = self.node_id;
        self.bucket
            .request(move |state, now| {
                let stored = StoredObject {
                    stored: object.stored,
                    compression,
                    size_bytes: object.size_bytes,
                    landed_at: now,
                };
                state.put(node_id, path, stored, now)
            })
            .await?;
        Ok(location)
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn upload<'a>(
        &'a self,
        key: SnapshotKey,
        bytes: Bytes,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        self.upload_iter(key, Box::new(std::iter::once(Ok(bytes))))
    }

    fn upload_iter<'a>(
        &'a self,
        key: SnapshotKey,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move {
            let index = snapshot_index(&key.snapshot_id)?;
            let staged = self
                .stage_body(RaftGroupId(key.raft_group_id), index, chunks)
                .await?;
            self.write(staged).await
        })
    }

    fn namespace(&self, raft_group_id: RaftGroupId) -> OwnedNamespace {
        OwnedNamespace {
            directory: Some(OwnedDirectory {
                prefix: self.prefix().clone(),
                owner: self.owner(raft_group_id),
            }),
        }
    }

    fn stage<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        Box::pin(self.stage_body(raft_group_id, index, chunks))
    }

    fn write_staged<'a>(
        &'a self,
        staged: StagedSnapshot,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(self.write(staged))
    }

    fn list_own<'a>(&'a self, raft_group_id: RaftGroupId) -> SnapshotStoreFuture<'a, OwnListing> {
        Box::pin(async move {
            let owner = self.owner(raft_group_id);
            let prefix = self.prefix().clone();
            self.bucket
                .request(move |state, now| state.list(&prefix, owner, now))
                .await
        })
    }

    fn delete_own<'a>(&'a self, object: OwnedObject) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            let owned = OwnedKey::parse(self.prefix(), &object.path)
                .is_ok_and(|key| key == object.key && key.owner.node_id == self.node_id);
            if !owned {
                self.foreign_delete(&object.path);
                return Err(SnapshotStoreError::NotOwnedHere { path: object.path });
            }
            let node_id = self.node_id;
            self.bucket
                .request(move |state, now| state.delete(node_id, object.path, now))
                .await
        })
    }

    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let SnapshotLocation::S3 {
                key, size_bytes, ..
            } = location
            else {
                return Err(SnapshotStoreError::UnsupportedLocation {
                    store: SnapshotBackend::S3,
                    location: location.kind(),
                });
            };
            let node_id = self.node_id;
            let object = self
                .bucket
                .request(|state, _now| {
                    state.transient(node_id, SnapshotStoreOperation::Get, key)?;
                    let object = state.objects.get(key).cloned();
                    state.events.push(SnapshotStoreEvent::Get {
                        node_id,
                        path: key.clone(),
                        found: object.is_some(),
                    });
                    object.ok_or_else(|| SnapshotStoreError::NotFound { key: key.clone() })
                })
                .await?;
            let expected = location.stored_size_hint();
            if object.stored.len() as u64 != expected {
                return Err(SnapshotStoreError::SizeMismatch {
                    key: key.clone(),
                    expected,
                    actual: object.stored.len() as u64,
                });
            }
            let bytes = decode(key, &StoredObject {
                compression: location.compression(),
                size_bytes: *size_bytes,
                ..object
            })?;
            if bytes.len() as u64 != *size_bytes {
                return Err(SnapshotStoreError::LogicalSizeMismatch {
                    key: key.clone(),
                    expected: *size_bytes,
                    actual: bytes.len() as u64,
                });
            }
            Ok(bytes)
        })
    }

    fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            let SnapshotLocation::S3 { key, .. } = location else {
                return Ok(());
            };
            let node_id = self.node_id;
            if is_owned_by(self.prefix(), node_id, key) {
                let path = key.clone();
                return self
                    .bucket
                    .request(move |state, now| state.delete(node_id, path, now))
                    .await;
            }
            // A recording bucket lets the delete go ahead, as S3 would.
            self.foreign_delete(key);
            let path = key.clone();
            self.bucket
                .request(move |state, now| {
                    if state.objects.remove(&path).is_some() {
                        state.record_change(ChangeKind::Deleted, path, now);
                    }
                })
                .await;
            Ok(())
        })
    }

    fn verify_uploaded<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            let SnapshotLocation::S3 { key, .. } = location else {
                return Ok(());
            };
            let node_id = self.node_id;
            let stored_size = self
                .bucket
                .request(|state, _now| {
                    state.transient(node_id, SnapshotStoreOperation::Head, key)?;
                    let stored_size = state
                        .objects
                        .get(key)
                        .map(|object| object.stored.len() as u64);
                    state.events.push(SnapshotStoreEvent::Head {
                        node_id,
                        path: key.clone(),
                        found: stored_size.is_some(),
                    });
                    stored_size.ok_or_else(|| SnapshotStoreError::NotFound { key: key.clone() })
                })
                .await?;
            let expected = location.stored_size_hint();
            if stored_size != expected {
                return Err(SnapshotStoreError::SizeMismatch {
                    key: key.clone(),
                    expected,
                    actual: stored_size,
                });
            }
            Ok(())
        })
    }
}
