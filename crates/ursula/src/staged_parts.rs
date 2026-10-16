//! The bound on a stream's staged, unused snapshot parts (#622; extensions
//! Section 2.2), and the node's count of them for usage (#639, the
//! per-group totals of [`groups`]).
//!
//! A stream holds at most [`MAX_STAGED_PARTS`] staged parts and
//! [`MAX_STAGED_BYTES`] of them, twice what one commit may use, so an
//! abandoned upload and a full retry fit. The bound is per stream id: the
//! parts of an earlier incarnation count until they are deleted, so a delete
//! and a recreate do not reset it. A part past either answers `429` naming
//! the limit it applied ([`StagedPartsLimit`]), before its body is read.
//!
//! The count is node-local state of the leader, never replicated: one
//! [`Tally`] per stream. A listing of the stream's run of names in the
//! group's part directory ([`snapshot_part_stream_prefix`]) sets it. A part
//! lists when the node has no fresh tally for the stream's incarnation in
//! the current Raft term, and when it would be refused and the last listing
//! is older than [`RELIST_AFTER`]. A commit and a discard list after their
//! deletes. Every listing holds the stream's lock, so no reservation, commit
//! or discard of the stream interleaves with it, and the parts that finish
//! uploading while it runs are added on top of it.
//!
//! Only a listing that a fresh linearizable `HEAD` anchors deletes parts of
//! other incarnations ([`retired`]): a part's and a discard's. A commit's
//! listing only counts, since the incarnation the node last saw may be
//! terms old. A delete of the stream deletes the incarnations the node knew
//! before the delete at any age, which no later incarnation can be, and the
//! others staged more than [`RETIRED_MIN_AGE_MS`] before the delete. That
//! margin assumes the nodes' clocks agree within it.
//!
//! The count is an estimate between listings, over or under. The orphan
//! sweep tells the tracker what it reclaimed ([`SweptPartsObserver`]): the
//! tally gives those bytes back and is marked stale, so the next part lists.
//! It misses the parts still uploading on an old leader when this node
//! started to lead, at most what the old leader's in-flight body budget held,
//! and those stay over the bound until a commit, a discard, a delete or the
//! sweep removes them. A part a listing saw that settles during it counts
//! twice until the next listing.
//!
//! A node tracks at most [`MAX_TRACKED_STREAMS`] streams that no request
//! holds, evicting the least recently used, whose next part lists again.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use groups::GroupCounts;
pub(crate) use groups::GroupReport;
use tokio::time::Instant;
use ursula_headers::MAX_SNAPSHOT_PARTS;
use ursula_headers::StagedPartsLimit;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::SweptPart;
use ursula_runtime::SweptPartsObserver;
use ursula_runtime::cold_object_written_unix_ms;
use ursula_runtime::snapshot_part_dir;
use ursula_runtime::snapshot_part_object_incarnation;
use ursula_runtime::snapshot_part_stream_prefix;
use ursula_shard::BucketStreamId;

use crate::snapshot_parts::PartsRefusal;

/// The most staged, unused parts a stream holds: two commits' worth.
pub(crate) const MAX_STAGED_PARTS: u64 = 2 * MAX_SNAPSHOT_PARTS as u64;

/// The most bytes of staged, unused parts a stream holds: two snapshots of
/// the snapshot limit.
pub(crate) const MAX_STAGED_BYTES: u64 = 2 * MAX_COLD_SNAPSHOT_BYTES;

/// The streams a node tracks at once that no request holds, about 400
/// bytes each.
pub(crate) const MAX_TRACKED_STREAMS: usize = 16_384;

/// How old a stream's last listing must be before a part that would be
/// refused lists again: at most one listing a minute for a stream at its
/// limit.
pub(crate) const RELIST_AFTER: Duration = Duration::from_secs(60);

/// How old a part of an incarnation the node cannot name must be before a
/// listing or a stream's delete deletes it. A younger one may belong to an
/// incarnation created after the token the listing knows, so it is only
/// counted. The margin also covers the clocks of the nodes that name parts,
/// which must agree within it.
pub(crate) const RETIRED_MIN_AGE_MS: u64 = 60_000;

/// Stream deletes whose parts a node cleans up at once. Past them a delete
/// leaves its parts to the orphan sweep.
const DELETE_CLEANUPS: usize = 4;

/// Whether a part object named `name` belongs to an incarnation other than
/// `live` (no incarnation is live after a delete) and was staged at least
/// [`RETIRED_MIN_AGE_MS`] before `anchor_ms`, a time no later than the
/// view of the stream that names `live`. The one age rule of every listing
/// and of a stream's delete.
pub(crate) fn retired(name: &str, live: Option<u64>, anchor_ms: u64) -> bool {
    let Some(incarnation) = snapshot_part_object_incarnation(name) else {
        return false;
    };
    live != Some(incarnation)
        && cold_object_written_unix_ms(name)
            .is_some_and(|written| written.saturating_add(RETIRED_MIN_AGE_MS) <= anchor_ms)
}

/// The limits a node applies.
#[derive(Debug, Clone, Copy)]
struct Limits {
    parts: u64,
    bytes: u64,
    relist_after: Duration,
}

/// The staged parts of the streams this node leads, as far as it knows.
#[derive(Clone)]
pub(crate) struct StagedParts {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for StagedParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedParts").finish_non_exhaustive()
    }
}

struct Inner {
    limits: Limits,
    streams: Mutex<Streams>,
    /// The staged-part totals of the groups this node leads.
    groups: Arc<GroupCounts>,
    /// A logical clock for the least recently used eviction.
    uses: AtomicU64,
    /// Room for stream deletes' cleanups.
    cleanups: Arc<tokio::sync::Semaphore>,
    /// Test pause point: how many deletes' cleanups have ended.
    #[cfg(test)]
    cleaned: tokio::sync::watch::Sender<u64>,
}

/// The tracked streams, and the stream of each part-name prefix, so a swept
/// part finds its stream.
#[derive(Default)]
struct Streams {
    slots: HashMap<BucketStreamId, Arc<Slot>>,
    by_prefix: HashMap<String, BucketStreamId>,
}

impl Default for StagedParts {
    fn default() -> Self {
        Self::with_limits(Limits {
            parts: MAX_STAGED_PARTS,
            bytes: MAX_STAGED_BYTES,
            relist_after: RELIST_AFTER,
        })
    }
}

/// One stream's tally, and the lock that serializes its listings with its
/// reservations, commits, discards and deletes.
struct Slot {
    lock: tokio::sync::Mutex<()>,
    state: Mutex<SlotState>,
    last_used: AtomicU64,
    /// The stream's part-name prefix, [`snapshot_part_stream_prefix`].
    prefix: String,
    bucket_id: String,
    /// The node's group totals, which the slot's changes keep up to date.
    groups: Arc<GroupCounts>,
}

#[derive(Default)]
struct SlotState {
    tally: Option<Tally>,
    /// Parts uploading now, with the bytes they reserved. Kept apart from
    /// the tally, so a reset of the tally loses none.
    pending_parts: u64,
    pending_bytes: u64,
    /// Every part staged through this slot and its bytes, never decreased:
    /// a listing adds what was staged while it ran.
    settled_parts: u64,
    settled_bytes: u64,
}

impl SlotState {
    /// The limit a part of `reserve` bytes would pass, if any.
    fn exceeded(&self, limits: Limits, reserve: u64) -> Option<StagedPartsLimit> {
        let (parts, bytes) = self
            .tally
            .map_or((0, 0), |tally| (tally.parts, tally.bytes));
        if parts.saturating_add(self.pending_parts) >= limits.parts {
            return Some(StagedPartsLimit::Parts(limits.parts));
        }
        let bytes = bytes
            .saturating_add(self.pending_bytes)
            .saturating_add(reserve);
        (bytes > limits.bytes).then_some(StagedPartsLimit::Bytes(limits.bytes))
    }
}

/// What a node knows of a stream's staged parts.
#[derive(Debug, Clone, Copy)]
struct Tally {
    /// The incarnation of the stream that the listing's view named.
    incarnation: u64,
    /// The Raft term the listing ran in.
    term: u64,
    raft_group_id: u32,
    parts: u64,
    bytes: u64,
    listed_at: Instant,
    /// Something changed the parts behind the tally's back (the sweep, a
    /// failed listing): the next part lists.
    stale: bool,
}

impl Tally {
    fn of(&self, owner: StagedOwner<'_>) -> bool {
        !self.stale && self.incarnation == owner.incarnation && self.term == owner.term
    }
}

/// The stream incarnation a part is staged for, in the term its leader
/// leads.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StagedOwner<'a> {
    pub(crate) stream_id: &'a BucketStreamId,
    pub(crate) raft_group_id: u32,
    pub(crate) incarnation: u64,
    pub(crate) term: u64,
}

/// What a listing found and did.
#[derive(Debug)]
pub(crate) struct Listed {
    /// Parts it counted, and their bytes.
    pub(crate) parts: u64,
    pub(crate) bytes: u64,
    /// Parts it deleted.
    pub(crate) deleted: u64,
    /// The cold store's failure to delete the parts it meant to, which
    /// it then counted.
    pub(crate) delete_failed: Option<io::Error>,
}

/// What a stream's delete knew of its parts before the delete.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KnownParts {
    incarnation: u64,
    term: u64,
}

impl StagedParts {
    fn with_limits(limits: Limits) -> Self {
        Self {
            inner: Arc::new(Inner {
                limits,
                streams: Mutex::default(),
                uses: AtomicU64::new(0),
                groups: Arc::new(GroupCounts::new(Arc::new(|_| Some(0)))),
                cleanups: Arc::new(tokio::sync::Semaphore::new(DELETE_CLEANUPS)),
                #[cfg(test)]
                cleaned: tokio::sync::watch::Sender::new(0),
            }),
        }
    }

    /// The tracker of a node, which `runtime`'s orphan sweep tells of the
    /// parts it deletes, and whose group totals follow the leadership that
    /// `registry` shows: every group without one.
    pub(crate) fn observing(
        runtime: &ursula_runtime::ShardRuntime,
        registry: Option<ursula_raft::RaftGroupHandleRegistry>,
    ) -> Self {
        let staged = Self::default();
        staged.lead_by(registry);
        runtime.observe_swept_parts(Arc::new(staged.clone()));
        staged
    }

    /// The group totals follow the leadership that `registry` shows.
    pub(crate) fn lead_by(&self, registry: Option<ursula_raft::RaftGroupHandleRegistry>) {
        self.inner.groups.set_leads(Arc::new(move |group| {
            crate::front_door::registry_leader_term(
                registry.as_ref(),
                ursula_shard::RaftGroupId(group),
            )
        }));
    }

    /// The group totals follow `leads`, the term a test says the node leads
    /// a group in.
    #[cfg(test)]
    pub(crate) fn lead_as(&self, leads: impl Fn(u32) -> Option<u64> + Send + Sync + 'static) {
        self.inner.groups.set_leads(Arc::new(leads));
    }

    /// The group totals, for a test to drive.
    #[cfg(test)]
    pub(crate) fn groups(&self) -> &GroupCounts {
        &self.inner.groups
    }

    /// Small limits that list again at every part that would be refused,
    /// for a test to reach.
    #[cfg(test)]
    pub(crate) fn for_test(parts: u64, bytes: u64) -> Self {
        Self::for_test_relisting_after(parts, bytes, Duration::ZERO)
    }

    /// Small limits with the given relisting gate.
    #[cfg(test)]
    pub(crate) fn for_test_relisting_after(parts: u64, bytes: u64, relist_after: Duration) -> Self {
        Self::with_limits(Limits {
            parts,
            bytes,
            relist_after,
        })
    }

    /// How many streams this node tracks, and whether `stream_id` is one.
    #[cfg(test)]
    pub(crate) fn tracks(&self, stream_id: &BucketStreamId) -> (usize, bool) {
        let streams = self.streams();
        (streams.slots.len(), streams.slots.contains_key(stream_id))
    }

    /// The parts and bytes the node counts for `stream_id`, uploads
    /// included.
    #[cfg(test)]
    pub(crate) fn counted(&self, stream_id: &BucketStreamId) -> Option<(u64, u64)> {
        let slot = self.existing_slot(stream_id)?;
        let state = slot.state();
        let tally = state.tally?;
        Some((
            tally.parts.saturating_add(state.pending_parts),
            tally.bytes.saturating_add(state.pending_bytes),
        ))
    }

    /// How many deletes' cleanups have ended, for a test to wait on.
    #[cfg(test)]
    pub(crate) fn cleaned(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.cleaned.subscribe()
    }

    /// Reserves one part of up to `reserve` bytes for `owner`, before its
    /// body is read: lists the stream's parts first when the node has no
    /// fresh tally for this incarnation in this term, or when the part
    /// would be refused and the last listing is old. `anchor_ms` is the time
    /// of the `HEAD` that named `owner.incarnation`. `Err` with the limit
    /// the part would pass, or with the cold store's failure to list when
    /// the node has no tally to answer from.
    pub(crate) async fn reserve(
        &self,
        cold_store: &ColdStoreHandle,
        owner: StagedOwner<'_>,
        reserve: u64,
        anchor_ms: u64,
    ) -> Result<StagedReservation, PartsRefusal> {
        let limits = self.inner.limits;
        let slot = self.slot(owner.stream_id);
        let _lock = slot.lock.lock().await;
        let (current, exceeded) = {
            let state = slot.state();
            let current = state.tally.filter(|tally| tally.of(owner));
            (current, state.exceeded(limits, reserve))
        };
        let fresh_enough = current.is_some_and(|tally| {
            exceeded.is_none() || tally.listed_at.elapsed() < limits.relist_after
        });
        if !fresh_enough {
            let listed = list_and_delete(&slot, cold_store, owner, |name| {
                retired(name, Some(owner.incarnation), anchor_ms)
            })
            .await;
            if let Err(err) = listed {
                // An old count still answers while the store fails.
                if current.is_none() {
                    return Err(err);
                }
                tracing::warn!(
                    stream = %owner.stream_id,
                    error = %err,
                    "could not list a stream's staged parts; answering from its count"
                );
            }
        }
        let mut state = slot.state();
        if let Some(limit) = state.exceeded(limits, reserve) {
            return Err(PartsRefusal::StagedLimit {
                limit,
                incarnation: owner.incarnation,
            });
        }
        state.pending_parts = state.pending_parts.saturating_add(1);
        state.pending_bytes = state.pending_bytes.saturating_add(reserve);
        drop(state);
        Ok(StagedReservation {
            slot: Arc::clone(&slot),
            reserved: reserve,
            staged: None,
        })
    }

    /// Deletes `paths`, the parts a commit of `stream_id` spent, then lists
    /// the stream again, so the tally gives back exactly what is gone. The
    /// listing only counts: it deletes no other incarnation's parts, since
    /// the incarnation the node last saw may be terms old. A part already
    /// gone, or a delete that failed, gives back nothing. The parts of a
    /// failed delete stay until a discard, a delete of the stream or the
    /// orphan sweep removes them.
    pub(crate) async fn delete_spent(
        &self,
        cold_store: &ColdStoreHandle,
        stream_id: &BucketStreamId,
        raft_group_id: u32,
        paths: Vec<String>,
    ) {
        let slot = self.existing_slot(stream_id);
        let _lock = match &slot {
            Some(slot) => Some(slot.lock.lock().await),
            None => None,
        };
        if let Err(err) = cold_store.delete_objects(paths).await {
            tracing::warn!(
                stream = %stream_id,
                error = %err,
                "failed to delete the parts of a snapshot commit; a discard or the orphan sweep \
                 reclaims them"
            );
        }
        let tally = slot.as_ref().and_then(|slot| slot.state().tally);
        let listed = match (&slot, tally) {
            (Some(slot), Some(tally)) => {
                let owner = StagedOwner {
                    stream_id,
                    raft_group_id: tally.raft_group_id,
                    incarnation: tally.incarnation,
                    term: tally.term,
                };
                list_and_delete(slot, cold_store, owner, |_| false)
                    .await
                    .map(|_| ())
            }
            // The node holds no count of the stream, for one staged its
            // parts before it led: the group totals still count them.
            _ => {
                self.count_stream(cold_store, stream_id, raft_group_id)
                    .await
            }
        };
        if let Err(err) = listed {
            tracing::warn!(
                stream = %stream_id,
                error = %err,
                "could not list a stream's staged parts after a commit; its next part lists again"
            );
            if let Some(slot) = &slot {
                slot.mark_stale();
            }
        }
    }

    /// Lists `stream_id`'s parts and sets its group totals to them, without
    /// a tally of its own.
    async fn count_stream(
        &self,
        cold_store: &ColdStoreHandle,
        stream_id: &BucketStreamId,
        raft_group_id: u32,
    ) -> Result<(), PartsRefusal> {
        let files = list_files(cold_store, stream_id, raft_group_id).await?;
        let (parts, bytes) = files
            .iter()
            .fold((0_u64, 0_u64), |(parts, bytes), (_, size)| {
                (parts.saturating_add(1), bytes.saturating_add(*size))
            });
        self.inner.groups.listed(
            raft_group_id,
            &snapshot_part_stream_prefix(stream_id),
            &stream_id.bucket_id,
            parts,
            bytes,
        );
        Ok(())
    }

    /// Discards every staged part of `owner`'s incarnation, which the
    /// caller's claim keeps any commit off, and the retired parts of other
    /// incarnations, then counts what is left. `anchor_ms` is the time of
    /// the `HEAD` that named the incarnation. A part whose upload ends
    /// during the discard may or may not survive it. `Err` when the cold
    /// store failed to list or to delete.
    pub(crate) async fn discard(
        &self,
        cold_store: &ColdStoreHandle,
        owner: StagedOwner<'_>,
        anchor_ms: u64,
    ) -> Result<Listed, PartsRefusal> {
        let slot = self.slot(owner.stream_id);
        let _lock = slot.lock.lock().await;
        let mut listed = list_and_delete(&slot, cold_store, owner, |name| {
            snapshot_part_object_incarnation(name) == Some(owner.incarnation)
                || retired(name, Some(owner.incarnation), anchor_ms)
        })
        .await?;
        match listed.delete_failed.take() {
            Some(err) => Err(PartsRefusal::ColdStore(err)),
            None => Ok(listed),
        }
    }

    /// What the node knows of `stream_id`'s parts, read before a delete of
    /// the stream: `None` when it tracks no count of them, and then the
    /// delete leaves them to the sweep.
    pub(crate) fn before_delete(&self, stream_id: &BucketStreamId) -> Option<KnownParts> {
        let slot = self.existing_slot(stream_id)?;
        let state = slot.state();
        let tally = state.tally?;
        (tally.parts > 0 || state.pending_parts > 0).then_some(KnownParts {
            incarnation: tally.incarnation,
            term: tally.term,
        })
    }

    /// After a delete of `stream_id`: in a task the node owns, deletes the
    /// parts of `known.incarnation` and of `named` (the delete's
    /// `Stream-Incarnation`), all of them dead since the delete, at any age,
    /// and the stream's other parts staged at least [`RETIRED_MIN_AGE_MS`]
    /// before `anchor_ms`, a time before the delete. The answer does not
    /// wait for it. Past [`DELETE_CLEANUPS`] cleanups at once it leaves the
    /// parts to the orphan sweep. Adds no slot.
    pub(crate) fn deleted(
        &self,
        cold_store: ColdStoreHandle,
        stream_id: BucketStreamId,
        raft_group_id: u32,
        known: KnownParts,
        named: Option<u64>,
        anchor_ms: u64,
    ) {
        let Some(slot) = self.existing_slot(&stream_id) else {
            return;
        };
        let Ok(permit) = Arc::clone(&self.inner.cleanups).try_acquire_owned() else {
            tracing::debug!(
                stream = %stream_id,
                "too many stream deletes clean up their parts; the orphan sweep reclaims them"
            );
            return;
        };
        #[cfg(test)]
        let cleaned = self.inner.cleaned.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _lock = slot.lock.lock().await;
            let owner = StagedOwner {
                stream_id: &stream_id,
                raft_group_id,
                incarnation: known.incarnation,
                term: known.term,
            };
            let dead = |incarnation| incarnation == known.incarnation || Some(incarnation) == named;
            let listed = list_and_delete(&slot, &cold_store, owner, |name| {
                snapshot_part_object_incarnation(name).is_some_and(dead)
                    || retired(name, None, anchor_ms)
            })
            .await;
            match listed {
                Ok(Listed {
                    delete_failed: Some(err),
                    ..
                })
                | Err(PartsRefusal::ColdStore(err)) => tracing::warn!(
                    stream = %stream_id,
                    error = %err,
                    "could not delete a deleted stream's staged parts; the orphan sweep reclaims \
                     them"
                ),
                Ok(_) | Err(_) => {}
            }
            // The stream's next incarnation lists again.
            slot.mark_stale();
            #[cfg(test)]
            cleaned.send_modify(|cleaned| *cleaned = cleaned.saturating_add(1));
        });
    }

    /// Forgets the streams of a purged bucket, whose purge erased its parts.
    /// A slot a request holds is kept, its count reset, so its uploads stay
    /// counted and its listings serialized.
    pub(crate) fn purged(&self, bucket_id: &str) {
        let mut streams = self.streams();
        let Streams { slots, by_prefix } = &mut *streams;
        slots.retain(|stream_id, slot| {
            if stream_id.bucket_id != bucket_id {
                return true;
            }
            if Arc::strong_count(slot) > 1 {
                slot.state().tally = None;
                return true;
            }
            by_prefix.remove(&slot.prefix);
            false
        });
        drop(streams);
        self.inner.groups.purged(bucket_id);
    }

    /// The staged-part bytes per bucket of each group this node leads now,
    /// in the term it leads it in. Never lists, and changes nothing.
    pub(crate) fn report(&self) -> Vec<GroupReport> {
        self.inner.groups.report()
    }

    /// Waits until an update started a term's group totals, which the
    /// counting task then seeds.
    pub(crate) async fn count_started(&self) {
        self.inner.groups.started().await;
    }

    /// One round of the counting task: for every group of `runtime`,
    /// starts or drops its totals by the term this node leads the group in
    /// now, and seeds the totals of each group it leads whose counting has
    /// not finished, from one listing of the group's part directories.
    pub(crate) async fn count_led_groups(
        &self,
        runtime: &ursula_runtime::ShardRuntime,
        cold_store: &ColdStoreHandle,
    ) {
        for group in 0..runtime.raft_group_count() {
            let Some(term) = self.inner.groups.leader_term(group) else {
                self.inner.groups.follow(group);
                continue;
            };
            if !self.inner.groups.lead(group, term) {
                continue;
            }
            match seed_group(runtime, cold_store, group).await {
                Ok(Some(seeded)) => self.inner.groups.seeded(group, term, seeded),
                Ok(None) => {}
                Err(err) => tracing::warn!(
                    raft_group_id = group,
                    error = %err,
                    "could not count a group's staged snapshot parts; counting again next round"
                ),
            }
        }
    }

    fn streams(&self) -> std::sync::MutexGuard<'_, Streams> {
        self.inner
            .streams
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The slot of `stream_id`, made when the node has none. Past
    /// [`MAX_TRACKED_STREAMS`] it evicts the least recently used slot that
    /// no request holds. Held slots are never evicted, so the map may pass
    /// the cap by the slots requests hold.
    fn slot(&self, stream_id: &BucketStreamId) -> Arc<Slot> {
        let used = self.inner.uses.fetch_add(1, Ordering::Relaxed);
        let mut streams = self.streams();
        if let Some(slot) = streams.slots.get(stream_id) {
            slot.last_used.store(used, Ordering::Relaxed);
            return Arc::clone(slot);
        }
        if streams.slots.len() >= MAX_TRACKED_STREAMS {
            let evicted = streams
                .slots
                .iter()
                .filter(|(_, slot)| Arc::strong_count(slot) == 1)
                .min_by_key(|(_, slot)| slot.last_used.load(Ordering::Relaxed))
                .map(|(stream_id, _)| stream_id.clone());
            if let Some(evicted) = evicted
                && let Some(slot) = streams.slots.remove(&evicted)
            {
                streams.by_prefix.remove(&slot.prefix);
            }
        }
        let slot = Arc::new(Slot {
            lock: tokio::sync::Mutex::new(()),
            state: Mutex::default(),
            last_used: AtomicU64::new(used),
            prefix: snapshot_part_stream_prefix(stream_id),
            bucket_id: stream_id.bucket_id.clone(),
            groups: Arc::clone(&self.inner.groups),
        });
        streams
            .by_prefix
            .insert(slot.prefix.clone(), stream_id.clone());
        streams.slots.insert(stream_id.clone(), Arc::clone(&slot));
        slot
    }

    fn existing_slot(&self, stream_id: &BucketStreamId) -> Option<Arc<Slot>> {
        self.streams().slots.get(stream_id).cloned()
    }
}

/// The sweep reclaimed parts: the tallies of their streams give the bytes
/// back and are marked stale, so the next part of each lists.
impl SweptPartsObserver for StagedParts {
    fn swept(&self, parts: &[SweptPart]) {
        for part in parts {
            let Some(name) = part.path.rsplit('/').next() else {
                continue;
            };
            let Some(prefix) = name.get(..33) else {
                continue;
            };
            if let Some(group) = part_dir_group(&part.path) {
                self.inner.groups.swept(group, prefix, part.bytes);
            }
            let slot = {
                let streams = self.streams();
                streams
                    .by_prefix
                    .get(prefix)
                    .and_then(|stream_id| streams.slots.get(stream_id))
                    .cloned()
            };
            if let Some(slot) = slot {
                let mut state = slot.state();
                if let Some(tally) = state.tally.as_mut() {
                    tally.parts = tally.parts.saturating_sub(1);
                    tally.bytes = tally.bytes.saturating_sub(part.bytes);
                    tally.stale = true;
                }
            }
        }
    }
}

impl Slot {
    fn state(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn mark_stale(&self) {
        if let Some(tally) = self.state().tally.as_mut() {
            tally.stale = true;
        }
    }
}

/// Lists the parts of `owner`'s stream, deletes those `delete` names, and
/// sets the tally to the rest, plus every part staged through the slot
/// while the listing ran. The caller holds the slot's lock, and the tally
/// is replaced in one section of its state lock, so no settlement is lost
/// or kept twice. A part the listing saw and that settled during it counts
/// twice, an overcount the next listing repairs. A failed delete counts its
/// parts and is reported in [`Listed::delete_failed`].
async fn list_and_delete(
    slot: &Slot,
    cold_store: &ColdStoreHandle,
    owner: StagedOwner<'_>,
    delete: impl Fn(&str) -> bool,
) -> Result<Listed, PartsRefusal> {
    let (settled_parts, settled_bytes) = {
        let state = slot.state();
        (state.settled_parts, state.settled_bytes)
    };
    let files = list_files(cold_store, owner.stream_id, owner.raft_group_id).await?;
    let dir = snapshot_part_dir(&owner.stream_id.bucket_id, owner.raft_group_id);
    let (doomed, kept): (Vec<_>, Vec<_>) = files.into_iter().partition(|(name, _)| delete(name));
    let mut listed = Listed {
        parts: 0,
        bytes: 0,
        deleted: 0,
        delete_failed: None,
    };
    for (_, size) in &kept {
        listed.parts = listed.parts.saturating_add(1);
        listed.bytes = listed.bytes.saturating_add(*size);
    }
    if !doomed.is_empty() {
        let count = u64::try_from(doomed.len()).unwrap_or(u64::MAX);
        let bytes = doomed
            .iter()
            .fold(0_u64, |bytes, (_, size)| bytes.saturating_add(*size));
        let paths = doomed
            .into_iter()
            .map(|(name, _)| format!("{dir}{name}"))
            .collect::<Vec<_>>();
        match cold_store.delete_objects(paths).await {
            Ok(()) => listed.deleted = count,
            Err(err) => {
                // Some may be gone: count them all, an overcount.
                listed.parts = listed.parts.saturating_add(count);
                listed.bytes = listed.bytes.saturating_add(bytes);
                listed.delete_failed = Some(err);
            }
        }
    }
    let mut state = slot.state();
    let delta_parts = state.settled_parts.saturating_sub(settled_parts);
    let delta_bytes = state.settled_bytes.saturating_sub(settled_bytes);
    state.tally = Some(Tally {
        incarnation: owner.incarnation,
        term: owner.term,
        raft_group_id: owner.raft_group_id,
        parts: listed.parts.saturating_add(delta_parts),
        bytes: listed.bytes.saturating_add(delta_bytes),
        listed_at: Instant::now(),
        stale: listed.delete_failed.is_some(),
    });
    if let Some(tally) = state.tally {
        slot.groups.listed(
            owner.raft_group_id,
            &slot.prefix,
            &slot.bucket_id,
            tally.parts,
            tally.bytes,
        );
    }
    drop(state);
    tracing::debug!(
        stream = %owner.stream_id,
        parts = listed.parts,
        bytes = listed.bytes,
        deleted = listed.deleted,
        "counted a stream's staged parts from a listing"
    );
    Ok(listed)
}

/// Why a group's seed listing failed.
#[derive(Debug, thiserror::Error)]
enum CountError {
    #[error("read the group's part directories: {0}")]
    Runtime(#[from] ursula_runtime::RuntimeError),
    #[error("list the group's staged parts: {0}")]
    ColdStore(#[source] io::Error),
}

/// The group of a part object path, `{bucket}/_parts/{group:08x}/{name}`.
fn part_dir_group(path: &str) -> Option<u32> {
    let mut segments = path.rsplit('/');
    let _name = segments.next()?;
    u32::from_str_radix(segments.next()?, 16).ok()
}

/// Lists the part directories of `group`, one per bucket, and sums their
/// parts per stream: `None` when this node does not lead the group.
async fn seed_group(
    runtime: &ursula_runtime::ShardRuntime,
    cold_store: &ColdStoreHandle,
    group: u32,
) -> Result<Option<HashMap<String, (String, u64, u64)>>, CountError> {
    let dirs = runtime
        .snapshot_part_dirs(ursula_shard::RaftGroupId(group))
        .await?;
    let Some(dirs) = dirs else {
        return Ok(None);
    };
    let mut seeded = HashMap::<String, (String, u64, u64)>::new();
    for dir in dirs {
        let bucket_id = dir.split('/').next().unwrap_or_default().to_owned();
        cold_store
            .fold_file_sizes_with_prefix(&dir, "", |name, size| {
                if snapshot_part_object_incarnation(name).is_some()
                    && let Some(prefix) = name.get(..33)
                {
                    let entry = seeded
                        .entry(prefix.to_owned())
                        .or_insert_with(|| (bucket_id.clone(), 0, 0));
                    entry.1 = entry.1.saturating_add(1);
                    entry.2 = entry.2.saturating_add(size);
                }
                true
            })
            .await
            .map_err(CountError::ColdStore)?;
    }
    Ok(Some(seeded))
}

/// The part objects of every incarnation of `stream_id`, with their sizes,
/// from one prefix listing. A stream past its limits keeps the run short;
/// the listing stops at twice [`MAX_STAGED_PARTS`].
async fn list_files(
    cold_store: &ColdStoreHandle,
    stream_id: &BucketStreamId,
    raft_group_id: u32,
) -> Result<Vec<(String, u64)>, PartsRefusal> {
    let max = usize::try_from(MAX_STAGED_PARTS.saturating_mul(2)).unwrap_or(usize::MAX);
    let files = cold_store
        .list_file_sizes_with_prefix(
            &snapshot_part_dir(&stream_id.bucket_id, raft_group_id),
            &snapshot_part_stream_prefix(stream_id),
            max,
        )
        .await
        .map_err(PartsRefusal::ColdStore)?;
    Ok(files
        .into_iter()
        .filter(|(name, _)| snapshot_part_object_incarnation(name).is_some())
        .collect())
}

/// A part that is uploading: it counts as one part of its reserved bytes
/// until it is staged, and then of its size. Dropped unstaged, it releases
/// its reservation.
pub(crate) struct StagedReservation {
    slot: Arc<Slot>,
    reserved: u64,
    staged: Option<u64>,
}

impl StagedReservation {
    /// The part is staged with `size` bytes.
    pub(crate) fn staged(mut self, size: u64) {
        self.staged = Some(size);
    }
}

impl Drop for StagedReservation {
    fn drop(&mut self) {
        let (reserved, staged) = (self.reserved, self.staged);
        let mut state = self.slot.state();
        state.pending_parts = state.pending_parts.saturating_sub(1);
        state.pending_bytes = state.pending_bytes.saturating_sub(reserved);
        if let Some(size) = staged {
            state.settled_parts = state.settled_parts.saturating_add(1);
            state.settled_bytes = state.settled_bytes.saturating_add(size);
            if let Some(tally) = state.tally.as_mut() {
                tally.parts = tally.parts.saturating_add(1);
                tally.bytes = tally.bytes.saturating_add(size);
                self.slot.groups.staged(
                    tally.raft_group_id,
                    &self.slot.prefix,
                    &self.slot.bucket_id,
                    size,
                );
            }
        }
    }
}

mod groups;
#[cfg(test)]
pub(crate) mod tests;
