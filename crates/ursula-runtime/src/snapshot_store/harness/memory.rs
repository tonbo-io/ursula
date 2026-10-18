//! A faulty in-memory snapshot bucket that every node of a test shares,
//! and each node's [`SnapshotStore`] on it.
//!
//! The bucket is a map behind a mutex. Every request draws a delay to wait
//! before it acts and one to wait after it, and acts on the map in between,
//! so concurrent requests interleave as the seed's draws order them. Its
//! faults ([`SnapshotStoreFaults`]) model what an S3-compatible store does
//! to a client that the ownership protocol (#687) must survive:
//!
//! - a PUT that answers with an error and still lands later, or never;
//! - a LIST that omits recently created keys and repeats recently deleted
//!   ones;
//! - a create that succeeds only if no object has the name (If-None-Match),
//!   checked again when a late PUT lands.
//!
//! Every fault, delay and nonce comes from the bucket's seed, never from a
//! thread or process RNG, so one seed and one order of requests replay the
//! same run. Under madsim the delays are simulated time.
//!
//! A DELETE of a key outside the deleting node's own directory is a
//! violation of invariant I2. By default the store panics on it
//! ([`ForeignDeletes::Panic`]). A negative control that wants to see the
//! damage records it instead ([`ForeignDeletes::Record`]).

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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ForeignDeletes {
    /// Records the attempt and panics in the deleting task.
    #[default]
    Panic,
    /// Records the attempt. [`SnapshotStore::delete_own`] refuses it as the
    /// S3 store does, and [`SnapshotStore::delete`], which S3 does not
    /// scope, deletes the key.
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
    /// A PUT that answers with an error and still lands later.
    pub late_put: PerMille,
    /// How long after its failed answer a late PUT lands.
    pub late_put_delay: DelayRange,
    /// A PUT that answers with an error and never lands.
    pub lost_put: PerMille,
    /// A LIST that is stale: each of the recent creates in its directory
    /// is left out, and each of the recent deletes listed again, with even
    /// odds.
    pub stale_list: PerMille,
    /// How many of the bucket's most recent creates and deletes a stale
    /// LIST may misreport.
    pub stale_window: usize,
    pub foreign_deletes: ForeignDeletes,
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
            stale_list: PerMille::NEVER,
            stale_window: 0,
            foreign_deletes: ForeignDeletes::Panic,
        }
    }

    /// Every fault, at fixed rates, with request delays of up to
    /// `max_delay` and late PUTs landing up to four of them later.
    pub fn every_fault(max_delay: Duration) -> Self {
        Self {
            request_start_delay: DelayRange::between(Duration::ZERO, max_delay),
            request_end_delay: DelayRange::between(Duration::ZERO, max_delay),
            late_put: PerMille::new(200),
            late_put_delay: DelayRange::between(Duration::ZERO, max_delay.saturating_mul(4)),
            lost_put: PerMille::new(50),
            stale_list: PerMille::new(300),
            stale_window: 8,
            foreign_deletes: ForeignDeletes::Panic,
        }
    }

    /// A profile drawn from `seed`: every fault is on, at a rate the seed
    /// picks, with request delays of up to `max_delay`.
    pub fn from_seed(seed: u64, max_delay: Duration) -> Self {
        let mut rng = SplitMix64::new(seed);
        let mut rate = |most: u64| PerMille::new(u16::try_from(rng.below(most)).unwrap_or(0));
        let late_put = rate(300);
        let lost_put = rate(100);
        let stale_list = rate(500);
        let stale_window = usize::try_from(rng.below(16))
            .unwrap_or(0)
            .saturating_add(1);
        Self {
            late_put,
            lost_put,
            stale_list,
            stale_window,
            ..Self::every_fault(max_delay)
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
    /// The PUT answered an error, and the object lands `after` the answer
    /// unless an object has the name by then.
    Late { after: Duration },
    /// The PUT answered an error, and nothing lands.
    Lost,
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
    },
    /// A DELETE outside the deleting node's own directory (I2).
    ForeignDelete { node_id: u64, path: String },
}

#[derive(Debug, Clone)]
struct StoredObject {
    stored: Vec<u8>,
    compression: SnapshotCompression,
    size_bytes: u64,
}

#[derive(Debug)]
struct LatePut {
    lands_at: Instant,
    path: String,
    object: StoredObject,
}

#[derive(Debug, Clone)]
enum Change {
    Created(String),
    Deleted(String),
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
    /// Late PUTs in the order they failed.
    pending: Vec<LatePut>,
    /// The most recent creates and deletes, oldest first.
    changes: VecDeque<Change>,
    events: Vec<SnapshotStoreEvent>,
}

#[derive(Debug)]
struct Shared {
    seed: u64,
    prefix: SnapshotPrefix,
    state: Mutex<BucketState>,
}

/// The bucket every node of a test shares. Clones share it.
#[derive(Debug, Clone)]
pub struct MemorySnapshotBucket {
    shared: Arc<Shared>,
}

impl MemorySnapshotBucket {
    /// An empty bucket that draws from `seed` and keeps objects under
    /// `prefix`.
    pub fn new(seed: u64, prefix: SnapshotPrefix, faults: SnapshotStoreFaults) -> Self {
        Self {
            shared: Arc::new(Shared {
                seed,
                prefix,
                state: Mutex::new(BucketState {
                    faults,
                    rng: SplitMix64::new(seed),
                    nonces: BTreeMap::new(),
                    objects: BTreeMap::new(),
                    pending: Vec::new(),
                    changes: VecDeque::new(),
                    events: Vec::new(),
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

    /// Lands every late PUT now, in the order they were due, as a quiet
    /// store eventually does.
    pub fn settle(&self) {
        self.state().land_all();
    }

    /// The late PUTs that have not landed yet.
    pub fn pending_late_puts(&self) -> usize {
        let mut state = self.state();
        state.land_due(Instant::now());
        state.pending.len()
    }

    /// The keys the bucket holds now, late PUTs that are due included.
    pub fn keys(&self) -> BTreeSet<String> {
        let mut state = self.state();
        state.land_due(Instant::now());
        state.objects.keys().cloned().collect()
    }

    /// Every request the bucket served, in the order it acted.
    pub fn events(&self) -> Vec<SnapshotStoreEvent> {
        self.state().events.clone()
    }

    /// Writes `bytes` at `path` with no checks, faults or delays, as
    /// another writer of the bucket would.
    pub fn insert_raw(&self, path: &str, bytes: Vec<u8>) {
        let mut state = self.state();
        let size_bytes = bytes.len() as u64;
        state.objects.insert(path.to_owned(), StoredObject {
            stored: bytes,
            compression: SnapshotCompression::None,
            size_bytes,
        });
        state.record_change(Change::Created(path.to_owned()));
    }

    /// What the invariant checkers read: every object with its logical
    /// bytes, and every DELETE the bucket served or refused.
    pub fn view(&self) -> SnapshotStoreView {
        let mut state = self.state();
        state.land_due(Instant::now());
        let objects = state
            .objects
            .iter()
            .map(|(path, object)| {
                let logical =
                    decode(path, object).unwrap_or_else(|_undecodable| object.stored.clone());
                (path.clone(), logical)
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
    /// that are due, runs `act` on the bucket with the current time and the
    /// end delay still to wait, then waits the end delay.
    async fn request<T>(&self, act: impl FnOnce(&mut BucketState, Instant, Duration) -> T) -> T {
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
            act(&mut state, now, end)
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
    fn record_change(&mut self, change: Change) {
        self.changes.push_back(change);
        while self.changes.len() > self.faults.stale_window {
            self.changes.pop_front();
        }
    }

    fn land_due(&mut self, now: Instant) {
        let (mut due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|put| put.lands_at <= now);
        self.pending = rest;
        due.sort_by_key(|put| put.lands_at);
        for put in due {
            self.land(put);
        }
    }

    fn land_all(&mut self) {
        let mut due = std::mem::take(&mut self.pending);
        due.sort_by_key(|put| put.lands_at);
        for put in due {
            self.land(put);
        }
    }

    fn land(&mut self, put: LatePut) {
        if self.objects.contains_key(&put.path) {
            self.events
                .push(SnapshotStoreEvent::LatePutRefused { path: put.path });
            return;
        }
        self.objects.insert(put.path.clone(), put.object);
        self.record_change(Change::Created(put.path.clone()));
        self.events
            .push(SnapshotStoreEvent::LatePutLanded { path: put.path });
    }

    fn next_nonce(&mut self, seed: u64, node_id: u64) -> u64 {
        self.nonces
            .entry(node_id)
            .or_insert_with(|| SplitMix64::new(seed ^ node_id.wrapping_mul(0xD6E8_FEB8_6659_FD93)))
            .next_u64()
    }

    /// One PUT of `object` at `path` by `node_id`, acting at `now` and
    /// answering `end` later.
    fn put(
        &mut self,
        node_id: u64,
        path: String,
        object: StoredObject,
        now: Instant,
        end: Duration,
    ) -> Result<(), SnapshotStoreError> {
        let faults = self.faults.clone();
        let outcome = if faults.late_put.draw(&mut self.rng) {
            let after = faults.late_put_delay.draw(&mut self.rng);
            self.pending.push(LatePut {
                lands_at: later(later(now, end), after),
                path: path.clone(),
                object,
            });
            PutOutcome::Late { after }
        } else if faults.lost_put.draw(&mut self.rng) {
            PutOutcome::Lost
        } else if self.objects.contains_key(&path) {
            PutOutcome::NameCollision
        } else {
            self.objects.insert(path.clone(), object);
            self.record_change(Change::Created(path.clone()));
            PutOutcome::Created
        };
        self.events.push(SnapshotStoreEvent::Put {
            node_id,
            path: path.clone(),
            outcome: outcome.clone(),
        });
        match outcome {
            PutOutcome::Created => Ok(()),
            PutOutcome::NameCollision => Err(SnapshotStoreError::NameCollision { key: path }),
            PutOutcome::Late { .. } | PutOutcome::Lost => Err(injected(
                SnapshotStoreOperation::Put,
                path,
                "the PUT failed to the client",
            )),
        }
    }

    /// A LIST of `owner`'s directory under `prefix`.
    fn list(&mut self, prefix: &SnapshotPrefix, owner: SnapshotOwner) -> OwnListing {
        let directory = owner.directory(prefix);
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
            for change in self.changes.clone() {
                match change {
                    Change::Created(path)
                        if path.starts_with(&directory) && listed.contains(&path) =>
                    {
                        if self.rng.below(2) == 1 {
                            listed.remove(&path);
                            omitted.push(path);
                        }
                    }
                    Change::Deleted(path)
                        if path.starts_with(&directory) && !self.objects.contains_key(&path) =>
                    {
                        if self.rng.below(2) == 1 && listed.insert(path.clone()) {
                            repeated.push(path);
                        }
                    }
                    Change::Created(_) | Change::Deleted(_) => {}
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
        listing
    }

    /// A DELETE of `path` by `node_id`, in its own directory.
    fn delete(&mut self, node_id: u64, path: String) {
        let existed = self.objects.remove(&path).is_some();
        if existed {
            self.record_change(Change::Deleted(path.clone()));
        }
        self.events.push(SnapshotStoreEvent::Delete {
            node_id,
            path,
            existed,
        });
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

/// The log index a snapshot id ends with (`group-{gid}-{leader}-{index}`),
/// or 0 for an id without one (`group-{gid}-empty`).
fn snapshot_index(snapshot_id: &str) -> u64 {
    snapshot_id
        .rsplit_once('-')
        .and_then(|(_, index)| index.parse().ok())
        .unwrap_or(0)
}

/// Node `node_id`'s store on a [`MemorySnapshotBucket`]. It implements the
/// whole [`SnapshotStore`] trait, so a simulation injects it with the
/// registry's `set_snapshot_store`:
///
/// - the owned-key methods as the S3 store does them;
/// - `upload` and `upload_iter` as a stage and a write under a fresh owned
///   key at the snapshot id's log index;
/// - `download` as a GET, and `verify_uploaded` as a HEAD;
/// - `delete` as a DELETE that S3 does not scope, so I2 is asserted here;
/// - the reference methods and `prune_retired` as the trait's no-op
///   defaults: GC is the owner's pass over `list_own` and `delete_own`.
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
    /// bucket's seed, as `S3SnapshotStore::with_nonces` does, so a
    /// test can force a name collision.
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
        let policy = {
            let mut state = self.bucket.state();
            state.events.push(SnapshotStoreEvent::ForeignDelete {
                node_id: self.node_id,
                path: path.to_owned(),
            });
            state.faults.foreign_deletes
        };
        if policy == ForeignDeletes::Panic {
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
        let stored = StoredObject {
            stored: object.stored,
            compression: location.compression(),
            size_bytes: object.size_bytes,
        };
        let node_id = self.node_id;
        self.bucket
            .request(move |state, now, end| state.put(node_id, path, stored, now, end))
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
            let index = snapshot_index(&key.snapshot_id);
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
            Ok(self
                .bucket
                .request(move |state, _now, _end| state.list(&prefix, owner))
                .await)
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
                .request(move |state, _now, _end| state.delete(node_id, object.path))
                .await;
            Ok(())
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
                .request(|state, _now, _end| {
                    let object = state.objects.get(key).cloned();
                    state.events.push(SnapshotStoreEvent::Get {
                        node_id,
                        path: key.clone(),
                        found: object.is_some(),
                    });
                    object
                })
                .await
                .ok_or_else(|| SnapshotStoreError::NotFound { key: key.clone() })?;
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
            let owned = is_owned_by(self.prefix(), node_id, key);
            if !owned {
                // Under `ForeignDeletes::Record`, the delete goes ahead.
                self.foreign_delete(key);
            }
            let path = key.clone();
            self.bucket
                .request(move |state, _now, _end| {
                    if owned {
                        state.delete(node_id, path);
                    } else if state.objects.remove(&path).is_some() {
                        state.record_change(Change::Deleted(path));
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
                .request(|state, _now, _end| {
                    let stored_size = state
                        .objects
                        .get(key)
                        .map(|object| object.stored.len() as u64);
                    state.events.push(SnapshotStoreEvent::Head {
                        node_id,
                        path: key.clone(),
                        found: stored_size.is_some(),
                    });
                    stored_size
                })
                .await
                .ok_or_else(|| SnapshotStoreError::NotFound { key: key.clone() })?;
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
