//! One raft group's log as its core journal holds it.
//!
//! A [`GroupLog`] keeps the group's purge and committed markers, an index of
//! where each live entry's frame is ([`IndexedEntry`]), the live bytes each
//! journal segment holds for the group, and a cache of the group's most
//! recent entries bounded by a byte budget. Memory per group is therefore
//! the index (a few dozen bytes per retained entry) plus the cache, whatever
//! the retained entries weigh.
//!
//! The core writer is the only one that changes it, in journal order: it
//! applies a record once the record is written, with the position it was
//! written at, and it moves the positions of the entries a rewrite copied.
//! The group's store reads it to answer OpenRaft: the log state, the cached
//! entries, and the positions of the entries it has to read from disk.
//!
//! Replay applies the journal's records in segment order. A rewrite copies a
//! group's live entries from an old segment to the newest one, so after the
//! old segment is gone those entries come back after newer ones. Replay
//! therefore allows a gap while it runs and checks that the log is
//! consecutive once every segment is read ([`GroupLog::finish_replay`]).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;

use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::StoredMembershipOf;

use super::RaftGroupLogRecord;
use super::journal::FrameLoc;
use super::segment::SegmentId;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::entry_log_bytes;

type Entry = EntryOf<UrsulaRaftTypeConfig>;
type LogId = LogIdOf<UrsulaRaftTypeConfig>;
type StoredMembership = StoredMembershipOf<UrsulaRaftTypeConfig>;

/// Bytes a committed or purge marker counts in its segment.
pub(crate) const MARKER_BYTES: u64 = 64;

/// Where a frame is in a core journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FramePos {
    pub(crate) segment: SegmentId,
    pub(crate) loc: FrameLoc,
}

/// One live entry: its log id and where its frame is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexedEntry {
    pub(crate) log_id: LogId,
    pub(crate) frame: FramePos,
    /// Payload weight for read materialization budgets and cache accounting.
    pub(crate) bytes: u32,
    /// Journal space; archived payload retains its original read budget above.
    pub(crate) storage_bytes: u32,
}

/// How a record reaches [`GroupLog::apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyMode {
    /// The writer wrote it: an entry must extend or overlap the log.
    Live,
    /// Replay read it: a rewritten entry may arrive before the entries it
    /// precedes, so a gap is allowed until [`GroupLog::finish_replay`].
    Replay,
}

/// What a read of a range needs: entries the cache holds, and the frames of
/// the ones before them, oldest first.
#[derive(Debug)]
pub(crate) struct ReadPlan {
    pub(crate) disk: Vec<DiskRead>,
    pub(crate) cached: Vec<Entry>,
    /// What the planned entries, cached or not, left of the read's budget,
    /// for the pending entries the store adds after them. A plan that
    /// stopped before the end of its range leaves nothing.
    pub(crate) budget: ReadBudget,
}

/// The weight limit of one read, with entries weighed by [`entry_log_bytes`].
/// A limited read takes the longest prefix of its range whose entries weigh
/// at most the limit, and never less than one entry, as OpenRaft forbids an
/// empty read. One budget covers the entries [`GroupLog::plan_read`] plans
/// and the pending ones the store adds after them.
#[derive(Debug)]
pub(crate) struct ReadBudget {
    /// The limit, or `None` to read the whole range.
    max: Option<u64>,
    taken: Taken,
}

/// What a [`ReadBudget`] has taken so far.
#[derive(Debug)]
enum Taken {
    /// No entry yet, so the next one is taken whatever it weighs.
    Nothing,
    /// Entries of this total weight.
    Bytes(u64),
    /// The budget refused an entry, which ends the read.
    Ended,
}

impl ReadBudget {
    // Private: a read starts its budget in `plan_read`, and `merge_pending`
    // can only spend what the plan left, never a fresh or unlimited budget.
    fn new(max: Option<u64>) -> Self {
        Self {
            max,
            taken: Taken::Nothing,
        }
    }

    /// Takes the read's next entry, which weighs `bytes`, and says whether it
    /// did. The first entry is always taken, whatever it weighs. A later one
    /// is refused if it would carry the read past its limit, and so is every
    /// entry after a refusal, so what a read takes is a prefix of its range.
    pub(crate) fn take(&mut self, bytes: u64) -> bool {
        let total = match self.taken {
            Taken::Nothing => Some(bytes),
            Taken::Bytes(taken) => Some(taken.saturating_add(bytes))
                .filter(|total| self.max.is_none_or(|max| *total <= max)),
            Taken::Ended => None,
        };
        self.taken = total.map_or(Taken::Ended, Taken::Bytes);
        total.is_some()
    }
}

/// Entries to read from one frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskRead {
    pub(crate) frame: FramePos,
    /// The log ids the index holds for the entries wanted from this frame,
    /// in index order.
    pub(crate) log_ids: Vec<LogId>,
}

/// A group's live records in one segment, for a rewrite.
#[derive(Debug, Default)]
pub(crate) struct SegmentRecords {
    /// Live entries whose frame is in the segment, by index.
    pub(crate) entries: Vec<IndexedEntry>,
    /// The committed marker, when its latest record is in the segment.
    pub(crate) committed: Option<Option<LogId>>,
    /// The purge marker, when its latest record is in the segment.
    pub(crate) purged: Option<LogId>,
}

impl SegmentRecords {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.committed.is_none() && self.purged.is_none()
    }
}

/// A raft group's log in its core journal; see the module documentation.
#[derive(Debug)]
pub(crate) struct GroupLog {
    pub(crate) apply_stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    last_purged: Option<LogId>,
    committed: Option<LogId>,
    /// The segment of the latest committed record, if any was written.
    committed_at: Option<SegmentId>,
    /// The segment of the latest purge record.
    purged_at: Option<SegmentId>,
    index: EntryIndex,
    /// Live entry bytes per segment.
    live: BTreeMap<SegmentId, u64>,
    /// Segments in which an entry stopped being live (purged, truncated,
    /// written again or copied away by a rewrite) since the writer last
    /// took them ([`GroupLog::take_died`]).
    died: BTreeSet<SegmentId>,
    cache: EntryCache,
    /// The membership entries the log holds, by index (#613). A recovery
    /// plan's cut keeps the newest one at or below the committed index.
    memberships: BTreeMap<u64, StoredMembership>,
}

impl GroupLog {
    pub(crate) fn new(cache_budget: u64) -> Self {
        Self {
            apply_stopped: Default::default(),
            last_purged: None,
            committed: None,
            committed_at: None,
            purged_at: None,
            index: EntryIndex::default(),
            live: BTreeMap::new(),
            died: BTreeSet::new(),
            cache: EntryCache::new(cache_budget),
            memberships: BTreeMap::new(),
        }
    }

    /// A log that replay rebuilds: its index allows gaps until
    /// [`GroupLog::finish_replay`].
    pub(crate) fn replaying(cache_budget: u64) -> Self {
        Self {
            index: EntryIndex::Replaying(BTreeMap::new()),
            ..Self::new(cache_budget)
        }
    }

    pub(crate) fn last_purged(&self) -> Option<LogId> {
        self.last_purged
    }

    /// The newest membership entry the log holds at or below `index`.
    pub(crate) fn membership_at_or_below(&self, index: u64) -> Option<StoredMembership> {
        self.memberships
            .range(..=index)
            .next_back()
            .map(|(_, membership)| membership.clone())
    }

    /// Records whether the entry at its index is a membership entry.
    fn index_membership(&mut self, entry: &Entry) {
        let index = entry.log_id.index;
        match &entry.payload {
            openraft::EntryPayload::Membership(membership) => {
                self.memberships.insert(
                    index,
                    StoredMembership::new(Some(entry.log_id), membership.clone()),
                );
            }
            openraft::EntryPayload::Blank | openraft::EntryPayload::Normal(_) => {
                self.memberships.remove(&index);
            }
        }
    }

    pub(crate) fn committed(&self) -> Option<LogId> {
        self.committed
    }

    /// The id of the last entry, or of the last purged one.
    pub(crate) fn last_log_id(&self) -> Option<LogId> {
        self.index
            .last()
            .map(|entry| entry.log_id)
            .or(self.last_purged)
    }

    /// The index of the last entry the log holds.
    pub(crate) fn last_index(&self) -> Option<u64> {
        self.index.last().map(|entry| entry.log_id.index)
    }

    /// Whether the log holds anything: entries or a purge.
    pub(crate) fn holds_log(&self) -> bool {
        !self.index.is_empty() || self.last_purged.is_some()
    }

    pub(crate) fn indexed_entries(&self) -> u64 {
        u64::try_from(self.index.len()).unwrap_or(u64::MAX)
    }

    pub(crate) fn cache_bytes(&self) -> u64 {
        self.cache.bytes
    }

    /// Whether `record` can be written next: an append must extend or
    /// overlap the log, and a purge must not move back.
    pub(crate) fn validate(&self, record: &RaftGroupLogRecord) -> Result<(), io::Error> {
        match record {
            RaftGroupLogRecord::Append(entries) => {
                super::ensure_consecutive_entries::<UrsulaRaftTypeConfig>(entries)?;
                let Some(first) = entries.first().map(|entry| entry.log_id.index) else {
                    return Ok(());
                };
                match self.last_index() {
                    Some(last) if last.checked_add(1).is_some_and(|next| first > next) => {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("raft log store has a hole: {last} then {first}"),
                        ))
                    }
                    _ => Ok(()),
                }
            }
            RaftGroupLogRecord::Purge(log_id) if self.last_purged > Some(*log_id) => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "cannot move last purged log id backward from {:?} to {log_id:?}",
                        self.last_purged
                    ),
                ))
            }
            RaftGroupLogRecord::FrozenAppend(_)
            | RaftGroupLogRecord::Purge(_)
            | RaftGroupLogRecord::SaveCommitted(_)
            | RaftGroupLogRecord::TruncateAfter(_) => Ok(()),
        }
    }

    /// Applies `record`, written at `at`. A rejected record leaves the log
    /// as it was.
    pub(crate) fn apply(
        &mut self,
        record: RaftGroupLogRecord,
        at: FramePos,
        mode: ApplyMode,
    ) -> Result<(), io::Error> {
        match record {
            RaftGroupLogRecord::SaveCommitted(committed) => {
                self.committed = committed;
                self.committed_at = Some(at.segment);
                Ok(())
            }
            RaftGroupLogRecord::Append(entries) => self.append(entries, at, mode),
            // Its read weights come from the archive it names.
            RaftGroupLogRecord::FrozenAppend(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an archived append is applied with its resolved archive entries",
            )),
            RaftGroupLogRecord::TruncateAfter(last) => {
                self.truncate_after(last.map(|log_id| log_id.index));
                Ok(())
            }
            RaftGroupLogRecord::Purge(log_id) => {
                if self.last_purged > Some(log_id) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "cannot move last purged log id backward from {:?} to {log_id:?}",
                            self.last_purged
                        ),
                    ));
                }
                self.last_purged = Some(log_id);
                self.purged_at = Some(at.segment);
                self.purge_through(log_id.index);
                Ok(())
            }
        }
    }

    fn append(
        &mut self,
        entries: Vec<Entry>,
        at: FramePos,
        mode: ApplyMode,
    ) -> Result<(), io::Error> {
        // A live record passed `validate` before it was written.
        if mode == ApplyMode::Replay {
            super::ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
        }
        let Some(first) = entries.first().map(|entry| entry.log_id.index) else {
            return Ok(());
        };
        if mode == ApplyMode::Live
            && let Some(last_existing) = self.last_index()
            && last_existing
                .checked_add(1)
                .is_some_and(|next| first > next)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log store has a hole: {last_existing} then {first}"),
            ));
        }
        // The record replaces any cached entry from `first` on. OpenRaft
        // truncates before it overwrites, so a live append only extends the
        // cache; a replayed rewrite of older entries empties it.
        self.cache.truncate_after(first.checked_sub(1));
        let mut live = 0_u64;
        for entry in entries {
            let index = entry.log_id.index;
            let bytes = u32::try_from(entry_log_bytes(&entry)).unwrap_or(u32::MAX);
            let indexed = IndexedEntry {
                log_id: entry.log_id,
                frame: at,
                bytes,
                storage_bytes: bytes,
            };
            if let Some(replaced) = self.index.insert(indexed)? {
                self.unlive(replaced);
            }
            self.index_membership(&entry);
            live = live.saturating_add(u64::from(bytes));
            if self.last_index() == Some(index) {
                self.cache.push_newest(entry, u64::from(bytes));
            }
        }
        self.add_live(at.segment, live);
        Ok(())
    }

    /// Indexes the archive reference written at `at`. `selected` holds
    /// exactly the live entries it names, read from the archive at open, so
    /// this binary derives their read weights instead of trusting stored ones.
    pub(crate) fn apply_archived(
        &mut self,
        selected: &[Entry],
        at: FramePos,
    ) -> Result<(), io::Error> {
        let Some(first) = selected.first().map(|entry| entry.log_id.index) else {
            return Ok(());
        };
        self.cache.truncate_after(first.checked_sub(1));
        let storage_bytes = u32::try_from(
            at.loc
                .file_bytes()
                .div_ceil(u64::try_from(selected.len()).unwrap_or(u64::MAX)),
        )
        .unwrap_or(u32::MAX);
        for entry in selected {
            let indexed = IndexedEntry {
                log_id: entry.log_id,
                frame: at,
                bytes: u32::try_from(entry_log_bytes(entry)).unwrap_or(u32::MAX),
                storage_bytes,
            };
            if let Some(replaced) = self.index.insert(indexed)? {
                self.unlive(replaced);
            }
            self.index_membership(entry);
            self.add_live(at.segment, u64::from(storage_bytes));
        }
        Ok(())
    }

    fn truncate_after(&mut self, last: Option<u64>) {
        let (live, died) = (&mut self.live, &mut self.died);
        self.index
            .remove_after(last, |entry| remove_live(live, died, entry));
        self.cache.truncate_after(last);
        match last {
            Some(last) => drop(self.memberships.split_off(&last.saturating_add(1))),
            None => self.memberships.clear(),
        }
    }

    fn purge_through(&mut self, index: u64) {
        let (live, died) = (&mut self.live, &mut self.died);
        self.index
            .remove_through(index, |entry| remove_live(live, died, entry));
        self.cache.purge_through(index);
        self.memberships = self.memberships.split_off(&index.saturating_add(1));
    }

    fn add_live(&mut self, segment: SegmentId, bytes: u64) {
        let live = self.live.entry(segment).or_default();
        *live = live.saturating_add(bytes);
    }

    fn unlive(&mut self, entry: IndexedEntry) {
        remove_live(&mut self.live, &mut self.died, &entry);
    }

    /// The segments in which an entry stopped being live since the last
    /// call: they hold dead frames of this group.
    pub(crate) fn take_died(&mut self) -> BTreeSet<SegmentId> {
        std::mem::take(&mut self.died)
    }

    /// Every segment that holds a frame of this log, for a replay that
    /// drops the log for a later generation of its group or a retirement
    /// that drops it: all of them now hold dead frames.
    pub(crate) fn dead_segments(&mut self) -> BTreeSet<SegmentId> {
        let mut dead = std::mem::take(&mut self.died);
        dead.extend(self.live.keys().copied());
        dead.extend(self.committed_at);
        dead.extend(self.purged_at);
        dead
    }

    /// Fails unless the log replay rebuilt is consecutive; then its index
    /// accepts only entries that extend or overlap it.
    pub(crate) fn finish_replay(&mut self) -> Result<(), io::Error> {
        self.index.make_consecutive()?;
        if let (Some(purged), Some(first)) = (self.last_purged, self.index.first_index())
            && purged.index.checked_add(1) != Some(first)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "raft log store starts at {first} after purging through {}",
                    purged.index
                ),
            ));
        }
        Ok(())
    }

    /// The oldest segment holding a live record of the group.
    pub(crate) fn oldest_segment(&self) -> Option<SegmentId> {
        [
            self.live.keys().next().copied(),
            self.committed_at,
            self.purged_at,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// The bytes the group holds live in `segment`.
    pub(crate) fn live_in(&self, segment: SegmentId) -> u64 {
        let markers = [self.committed_at, self.purged_at]
            .into_iter()
            .filter(|at| *at == Some(segment))
            .count();
        self.live
            .get(&segment)
            .copied()
            .unwrap_or(0)
            .saturating_add(MARKER_BYTES.saturating_mul(u64::try_from(markers).unwrap_or(0)))
    }

    /// The bytes the group holds live across the journal.
    pub(crate) fn live_bytes(&self) -> u64 {
        let markers = [self.committed_at, self.purged_at]
            .into_iter()
            .flatten()
            .count();
        self.live
            .values()
            .fold(0_u64, |total, bytes| total.saturating_add(*bytes))
            .saturating_add(MARKER_BYTES.saturating_mul(u64::try_from(markers).unwrap_or(0)))
    }

    /// The group's live records in `segment`, entries by index.
    pub(crate) fn records_in(&self, segment: SegmentId) -> SegmentRecords {
        let entries = if self.live.contains_key(&segment) {
            self.index
                .range((Bound::Unbounded, Bound::Unbounded))
                .filter(|entry| entry.frame.segment == segment)
                .copied()
                .collect()
        } else {
            Vec::new()
        };
        SegmentRecords {
            entries,
            committed: (self.committed_at == Some(segment)).then_some(self.committed),
            purged: self.last_purged.filter(|_| self.purged_at == Some(segment)),
        }
    }

    /// Records that a rewrite copied `entry`, last seen at `from`, to
    /// `to`. An entry the log no longer holds there (truncated, purged or
    /// written again since) keeps its position.
    #[cfg(test)]
    pub(crate) fn relocate(&mut self, index: u64, from: FramePos, to: FramePos) {
        self.relocate_sized(index, from, to, None);
    }

    pub(crate) fn relocate_sized(
        &mut self,
        index: u64,
        from: FramePos,
        to: FramePos,
        bytes: Option<u32>,
    ) {
        let Some(entry) = self.index.get_mut(index) else {
            return;
        };
        if entry.frame != from {
            return;
        }
        let previous_bytes = entry.storage_bytes;
        entry.frame = to;
        if let Some(bytes) = bytes {
            entry.storage_bytes = bytes;
        }
        let moved = *entry;
        self.unlive(IndexedEntry {
            frame: from,
            storage_bytes: previous_bytes,
            ..moved
        });
        self.add_live(to.segment, u64::from(moved.storage_bytes));
    }

    /// Records that a rewrite wrote the committed marker again at `to`.
    pub(crate) fn relocate_committed(&mut self, from: SegmentId, to: SegmentId) {
        if self.committed_at == Some(from) {
            self.committed_at = Some(to);
        }
    }

    /// Records that a rewrite wrote the purge marker again at `to`.
    pub(crate) fn relocate_purged(&mut self, from: SegmentId, to: SegmentId) {
        if self.purged_at == Some(from) {
            self.purged_at = Some(to);
        }
    }

    /// What reading `range` needs: the cached entries, and the frames of the
    /// entries before the cache. With `max_bytes`, the read covers the
    /// longest prefix of the range whose entries weigh at most that much,
    /// cached or not, and never less than one entry ([`ReadBudget`]).
    pub(crate) fn plan_read(
        &self,
        range: impl RangeBounds<u64>,
        max_bytes: Option<u64>,
    ) -> ReadPlan {
        let start = match range.start_bound() {
            Bound::Included(start) => Bound::Included(*start),
            Bound::Excluded(start) => Bound::Excluded(*start),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(end) => Bound::Included(*end),
            Bound::Excluded(end) => Bound::Excluded(*end),
            Bound::Unbounded => Bound::Unbounded,
        };
        let mut plan = ReadPlan {
            disk: Vec::new(),
            cached: Vec::new(),
            budget: ReadBudget::new(max_bytes),
        };
        let cache_first = self.cache.first_index();
        for entry in self.index.range((start, end)) {
            if !plan.budget.take(u64::from(entry.bytes)) {
                break;
            }
            let index = entry.log_id.index;
            if cache_first.is_some_and(|first| index >= first)
                && let Some(cached) = self.cache.get(index)
            {
                plan.cached.push(cached.clone());
                continue;
            }
            match plan.disk.last_mut() {
                Some(read) if read.frame == entry.frame => read.log_ids.push(entry.log_id),
                _ => plan.disk.push(DiskRead {
                    frame: entry.frame,
                    log_ids: vec![entry.log_id],
                }),
            }
        }
        plan
    }

    /// The id of the entry at `index`, held or the last purged one.
    pub(crate) fn log_id_at(&self, index: u64) -> Option<LogId> {
        self.key_log_ids(index, index)
            .into_iter()
            .next()
            .or(self.last_purged.filter(|purged| purged.index == index))
    }

    /// An entry this log holds at or after `snapshot`'s index that is
    /// below `snapshot` in log-id order, if any: the last entry of the
    /// first leader's run with a lower leader id. Entries at or after the
    /// index with `snapshot`'s own leader id are at or above it.
    pub(crate) fn held_below_snapshot(&self, snapshot: LogId) -> Option<LogId> {
        let last = self.last_index().filter(|last| *last >= snapshot.index)?;
        self.key_log_ids(snapshot.index, last)
            .into_iter()
            .find(|key| *key < snapshot)
    }

    /// The last log id of each leader's run of entries within
    /// `first..=last`, as OpenRaft's key log ids are.
    pub(crate) fn key_log_ids(&self, first: u64, last: u64) -> Vec<LogId> {
        let mut keys: Vec<LogId> = Vec::new();
        for entry in self
            .index
            .range((Bound::Included(first), Bound::Included(last)))
        {
            match keys.last_mut() {
                Some(key) if key.leader_id == entry.log_id.leader_id => *key = entry.log_id,
                _ => keys.push(entry.log_id),
            }
        }
        keys
    }
}

/// Where each live entry of a group is, by log index.
#[derive(Debug)]
enum EntryIndex {
    /// While replay reads the segments: a rewrite's copies may arrive before
    /// the entries they precede, so the index may have gaps.
    Replaying(BTreeMap<u64, IndexedEntry>),
    /// A consecutive run of entries starting at log index `first`.
    Consecutive {
        first: u64,
        entries: VecDeque<IndexedEntry>,
    },
}

impl Default for EntryIndex {
    fn default() -> Self {
        Self::Consecutive {
            first: 0,
            entries: VecDeque::new(),
        }
    }
}

/// Takes `entry` out of the live bytes of its segment, which then holds a
/// dead frame.
fn remove_live(
    live: &mut BTreeMap<SegmentId, u64>,
    died: &mut BTreeSet<SegmentId>,
    entry: &IndexedEntry,
) {
    died.insert(entry.frame.segment);
    if let Some(bytes) = live.get_mut(&entry.frame.segment) {
        *bytes = bytes.saturating_sub(u64::from(entry.storage_bytes));
        if *bytes == 0 {
            live.remove(&entry.frame.segment);
        }
    }
}

/// Gives back most of a run's capacity once it holds far fewer entries,
/// so the index follows the retained log down after a purge.
fn shrink(entries: &mut VecDeque<IndexedEntry>) {
    let floor = entries.len().max(64);
    if entries.capacity() > floor.saturating_mul(4) {
        entries.shrink_to(floor.saturating_mul(2));
    }
}

fn hole(previous: u64, next: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("raft log store has a hole: {previous} then {next}"),
    )
}

impl EntryIndex {
    fn len(&self) -> usize {
        match self {
            Self::Replaying(entries) => entries.len(),
            Self::Consecutive { entries, .. } => entries.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn first_index(&self) -> Option<u64> {
        match self {
            Self::Replaying(entries) => entries.keys().next().copied(),
            Self::Consecutive { first, entries } => (!entries.is_empty()).then_some(*first),
        }
    }

    fn last(&self) -> Option<&IndexedEntry> {
        match self {
            Self::Replaying(entries) => entries.values().next_back(),
            Self::Consecutive { entries, .. } => entries.back(),
        }
    }

    /// The position of log index `index` in a consecutive run.
    fn offset(first: u64, entries: &VecDeque<IndexedEntry>, index: u64) -> Option<usize> {
        let offset = usize::try_from(index.checked_sub(first)?).ok()?;
        (offset < entries.len()).then_some(offset)
    }

    fn get_mut(&mut self, index: u64) -> Option<&mut IndexedEntry> {
        match self {
            Self::Replaying(entries) => entries.get_mut(&index),
            Self::Consecutive { first, entries } => {
                let offset = Self::offset(*first, entries, index)?;
                entries.get_mut(offset)
            }
        }
    }

    /// Adds `entry`, returning the one it replaces. A consecutive index
    /// accepts only an entry that extends, overlaps or directly precedes it.
    fn insert(&mut self, entry: IndexedEntry) -> Result<Option<IndexedEntry>, io::Error> {
        let index = entry.log_id.index;
        match self {
            Self::Replaying(entries) => Ok(entries.insert(index, entry)),
            Self::Consecutive { first, entries } => {
                if entries.is_empty() {
                    *first = index;
                    entries.push_back(entry);
                    return Ok(None);
                }
                let next = u64::try_from(entries.len())
                    .ok()
                    .and_then(|len| first.checked_add(len))
                    .ok_or_else(|| hole(*first, index))?;
                if index == next {
                    entries.push_back(entry);
                    return Ok(None);
                }
                if let Some(offset) = Self::offset(*first, entries, index)
                    && let Some(slot) = entries.get_mut(offset)
                {
                    return Ok(Some(std::mem::replace(slot, entry)));
                }
                if index.checked_add(1) == Some(*first) {
                    *first = index;
                    entries.push_front(entry);
                    return Ok(None);
                }
                Err(hole(next.saturating_sub(1), index))
            }
        }
    }

    /// Removes every entry after log index `last`, or all of them, and
    /// hands each to `removed`.
    fn remove_after(&mut self, last: Option<u64>, mut removed: impl FnMut(&IndexedEntry)) {
        match self {
            Self::Replaying(entries) => {
                let tail = match last.map(|last| last.checked_add(1)) {
                    Some(Some(next)) => entries.split_off(&next),
                    Some(None) => BTreeMap::new(),
                    None => std::mem::take(entries),
                };
                tail.values().for_each(removed);
            }
            Self::Consecutive { first, entries } => {
                let keep = match last {
                    Some(last) if last < *first => 0,
                    Some(last) => usize::try_from(last.saturating_sub(*first).saturating_add(1))
                        .unwrap_or(usize::MAX)
                        .min(entries.len()),
                    None => 0,
                };
                entries.range(keep..).for_each(&mut removed);
                entries.truncate(keep);
                shrink(entries);
            }
        }
    }

    /// Removes every entry up to and including log index `index`, and
    /// hands each to `removed`.
    fn remove_through(&mut self, index: u64, mut removed: impl FnMut(&IndexedEntry)) {
        match self {
            Self::Replaying(entries) => {
                let kept = match index.checked_add(1) {
                    Some(next) => entries.split_off(&next),
                    None => BTreeMap::new(),
                };
                std::mem::replace(entries, kept).values().for_each(removed);
            }
            Self::Consecutive { first, entries } => {
                if entries.is_empty() || index < *first {
                    return;
                }
                let count = usize::try_from(index.saturating_sub(*first).saturating_add(1))
                    .unwrap_or(usize::MAX)
                    .min(entries.len());
                entries.drain(..count).for_each(|entry| removed(&entry));
                *first = index.saturating_add(1);
                shrink(entries);
            }
        }
    }

    /// The entries whose log index is within `range`, in order.
    fn range(
        &self,
        range: (Bound<u64>, Bound<u64>),
    ) -> Box<dyn Iterator<Item = &IndexedEntry> + '_> {
        match self {
            Self::Replaying(entries) => Box::new(entries.range(range).map(|(_, entry)| entry)),
            Self::Consecutive { first, entries } => {
                let start = match range.0 {
                    Bound::Included(start) => start.saturating_sub(*first),
                    Bound::Excluded(start) => start.saturating_add(1).saturating_sub(*first),
                    Bound::Unbounded => 0,
                };
                let end = match range.1 {
                    Bound::Included(end) if end < *first => 0,
                    Bound::Included(end) => end.saturating_sub(*first).saturating_add(1),
                    Bound::Excluded(end) => end.saturating_sub(*first),
                    Bound::Unbounded => u64::MAX,
                };
                let start = usize::try_from(start).unwrap_or(usize::MAX);
                let end = usize::try_from(end).unwrap_or(usize::MAX);
                let len = end.saturating_sub(start);
                Box::new(entries.iter().skip(start).take(len))
            }
        }
    }

    /// Turns a replayed index into a consecutive one, or fails on a gap and
    /// leaves it as it was.
    fn make_consecutive(&mut self) -> Result<(), io::Error> {
        let Self::Replaying(replayed) = self else {
            return Ok(());
        };
        let mut previous: Option<u64> = None;
        for index in replayed.keys().copied() {
            if let Some(previous) = previous
                && previous.checked_add(1) != Some(index)
            {
                return Err(hole(previous, index));
            }
            previous = Some(index);
        }
        let entries = std::mem::take(replayed)
            .into_values()
            .collect::<VecDeque<_>>();
        let first = entries.front().map_or(0, |entry| entry.log_id.index);
        *self = Self::Consecutive { first, entries };
        Ok(())
    }
}

/// The newest entries of a group, a contiguous run of indexes, within a byte
/// budget. Older entries are evicted first.
#[derive(Debug)]
struct EntryCache {
    entries: VecDeque<(Entry, u64)>,
    bytes: u64,
    budget: u64,
}

impl EntryCache {
    fn new(budget: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            budget,
        }
    }

    fn first_index(&self) -> Option<u64> {
        self.entries.front().map(|(entry, _)| entry.log_id.index)
    }

    fn last_index(&self) -> Option<u64> {
        self.entries.back().map(|(entry, _)| entry.log_id.index)
    }

    fn get(&self, index: u64) -> Option<&Entry> {
        let first = self.first_index()?;
        let offset = usize::try_from(index.checked_sub(first)?).ok()?;
        self.entries.get(offset).map(|(entry, _)| entry)
    }

    /// Caches `entry`, the log's newest entry.
    fn push_newest(&mut self, entry: Entry, bytes: u64) {
        let index = entry.log_id.index;
        if self
            .last_index()
            .is_some_and(|last| last.checked_add(1) != Some(index))
        {
            self.entries.clear();
            self.bytes = 0;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push_back((entry, bytes));
        while self.bytes > self.budget {
            let Some((_, evicted)) = self.entries.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted);
        }
    }

    fn truncate_after(&mut self, last: Option<u64>) {
        while let Some(index) = self.last_index() {
            if last.is_some_and(|last| index <= last) {
                break;
            }
            if let Some((_, bytes)) = self.entries.pop_back() {
                self.bytes = self.bytes.saturating_sub(bytes);
            }
        }
    }

    fn purge_through(&mut self, index: u64) {
        while self.first_index().is_some_and(|first| first <= index) {
            if let Some((_, bytes)) = self.entries.pop_front() {
                self.bytes = self.bytes.saturating_sub(bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::entry::RaftEntry;
    use openraft::vote::RaftLeaderId;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use ursula_shard::BucketStreamId;
    use ursula_stream::StreamCommand;

    use super::ApplyMode;
    use super::DiskRead;
    use super::Entry;
    use super::FrameLoc;
    use super::FramePos;
    use super::GroupLog;
    use super::LogId as GroupLogId;
    use super::MARKER_BYTES;
    use super::RaftGroupLogRecord;
    use super::ReadBudget;
    use super::SegmentId;
    use super::entry_log_bytes;
    use crate::RaftCommand;

    fn log_id(term: u64, index: u64) -> GroupLogId {
        LogId {
            leader_id: CommittedLeaderId::new(term, 1),
            index,
        }
    }

    fn entry(term: u64, index: u64, payload: usize) -> Entry {
        Entry::new(
            log_id(term, index),
            EntryPayload::Normal(RaftCommand::Stream(StreamCommand::Append {
                stream_id: BucketStreamId::new("group-log", "test"),
                content_type: None,
                payload: Bytes::from(vec![u8::try_from(index % 251).unwrap_or(0); payload]),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            })),
        )
    }

    fn at(segment: u64, offset: u64) -> FramePos {
        FramePos {
            segment: SegmentId(segment),
            loc: FrameLoc { offset, len: 100 },
        }
    }

    fn append(log: &mut GroupLog, entries: Vec<Entry>, pos: FramePos, mode: ApplyMode) {
        log.apply(RaftGroupLogRecord::Append(entries), pos, mode)
            .expect("apply an append");
    }

    fn weight(payload: usize) -> u64 {
        entry_log_bytes(&entry(1, 1, payload))
    }

    #[test]
    fn live_bytes_follow_entries_and_markers_per_segment() {
        let mut log = GroupLog::new(1 << 20);
        append(
            &mut log,
            (1..=3).map(|i| entry(1, i, 100)).collect(),
            at(1, 32),
            ApplyMode::Live,
        );
        append(
            &mut log,
            (4..=5).map(|i| entry(1, i, 100)).collect(),
            at(2, 32),
            ApplyMode::Live,
        );
        log.apply(
            RaftGroupLogRecord::SaveCommitted(Some(log_id(1, 5))),
            at(2, 900),
            ApplyMode::Live,
        )
        .expect("commit");
        assert_eq!(log.oldest_segment(), Some(SegmentId(1)));
        assert_eq!(log.live_in(SegmentId(1)), 3 * weight(100));
        assert_eq!(log.live_in(SegmentId(2)), 2 * weight(100) + MARKER_BYTES);

        log.apply(
            RaftGroupLogRecord::Purge(log_id(1, 3)),
            at(3, 32),
            ApplyMode::Live,
        )
        .expect("purge");
        assert_eq!(log.live_in(SegmentId(1)), 0);
        assert_eq!(log.oldest_segment(), Some(SegmentId(2)));
        assert_eq!(log.live_in(SegmentId(3)), MARKER_BYTES);

        log.apply(
            RaftGroupLogRecord::TruncateAfter(Some(log_id(1, 4))),
            at(3, 64),
            ApplyMode::Live,
        )
        .expect("truncate");
        assert_eq!(log.live_in(SegmentId(2)), weight(100) + MARKER_BYTES);
        assert_eq!(log.last_log_id(), Some(log_id(1, 4)));
        assert_eq!(log.live_bytes(), weight(100) + 2 * MARKER_BYTES);
    }

    /// An archive reference stores only log IDs: a corrected binary may
    /// change the size estimate. Replay derives read weights from the
    /// archived payload and journal space from the reference frame.
    #[test]
    fn archived_entries_take_read_weights_from_the_replaying_binary() {
        let mut log = GroupLog::replaying(1 << 20);
        let reference = at(2, 32);
        log.apply_archived(&[entry(1, 1, 300), entry(1, 2, 10)], reference)
            .expect("index the reference");
        log.finish_replay().expect("finish replay");
        assert_eq!(
            log.live_in(SegmentId(2)),
            2 * reference.loc.file_bytes().div_ceil(2)
        );
        let plan = log.plan_read(1..=2, Some(weight(300)));
        assert!(plan.cached.is_empty());
        assert_eq!(plan.disk.len(), 1);
        assert_eq!(plan.disk[0].log_ids, vec![log_id(1, 1)]);
        log.apply(
            RaftGroupLogRecord::Append(vec![entry(1, 3, 10)]),
            at(2, 200),
            ApplyMode::Live,
        )
        .expect("a live append extends the archived prefix");
    }

    #[test]
    fn a_live_append_must_extend_the_log() {
        let mut log = GroupLog::new(1 << 20);
        append(&mut log, vec![entry(1, 1, 10)], at(1, 32), ApplyMode::Live);
        let hole = RaftGroupLogRecord::Append(vec![entry(1, 3, 10)]);
        assert_eq!(
            log.validate(&hole).expect_err("a hole").kind(),
            std::io::ErrorKind::InvalidData
        );
        log.apply(
            RaftGroupLogRecord::Purge(log_id(1, 1)),
            at(1, 64),
            ApplyMode::Live,
        )
        .expect("purge");
        assert_eq!(
            log.validate(&RaftGroupLogRecord::Purge(log_id(1, 0)))
                .expect_err("a purge cannot move back")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        log.validate(&RaftGroupLogRecord::Append(vec![entry(1, 2, 10)]))
            .expect("the next entry fits");
    }

    /// A rewrite copies a group's entries out of an old segment after newer
    /// ones, so replay sees a gap until every segment is read.
    #[test]
    fn replay_allows_a_gap_until_it_finishes() {
        let mut log = GroupLog::replaying(1 << 20);
        append(
            &mut log,
            (11..=20).map(|i| entry(1, i, 10)).collect(),
            at(3, 32),
            ApplyMode::Replay,
        );
        append(
            &mut log,
            (4..=6).map(|i| entry(1, i, 10)).collect(),
            at(9, 32),
            ApplyMode::Replay,
        );
        let err = log
            .finish_replay()
            .expect_err("4..=6 then 11..=20 is not a log");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        append(
            &mut log,
            (7..=10).map(|i| entry(1, i, 10)).collect(),
            at(10, 32),
            ApplyMode::Replay,
        );
        log.finish_replay().expect("the copies close the gap");
        assert_eq!(log.indexed_entries(), 17);
    }

    #[test]
    fn the_cache_keeps_the_newest_entries_within_its_budget() {
        let budget = 3 * weight(100);
        let mut log = GroupLog::new(budget);
        for index in 1..=6 {
            append(
                &mut log,
                vec![entry(1, index, 100)],
                at(1, index * 200),
                ApplyMode::Live,
            );
        }
        assert_eq!(log.cache_bytes(), budget);
        let plan = log.plan_read(1..=6, None);
        assert_eq!(
            plan.cached
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            [4, 5, 6]
        );
        assert_eq!(
            plan.disk,
            (1..=3)
                .map(|index| DiskRead {
                    frame: at(1, index * 200),
                    log_ids: vec![log_id(1, index)],
                })
                .collect::<Vec<_>>()
        );
        // A truncate drops the cached tail, a purge the cached head.
        log.apply(
            RaftGroupLogRecord::TruncateAfter(Some(log_id(1, 5))),
            at(1, 2000),
            ApplyMode::Live,
        )
        .expect("truncate");
        log.apply(
            RaftGroupLogRecord::Purge(log_id(1, 4)),
            at(1, 2100),
            ApplyMode::Live,
        )
        .expect("purge");
        assert_eq!(log.cache_bytes(), weight(100));
        let plan = log.plan_read(.., None);
        assert!(plan.disk.is_empty());
        assert_eq!(plan.cached, vec![entry(1, 5, 100)]);
        // An entry over the whole budget is not kept.
        let mut tiny = GroupLog::new(10);
        append(
            &mut tiny,
            vec![entry(1, 1, 100)],
            at(1, 32),
            ApplyMode::Live,
        );
        assert_eq!(tiny.cache_bytes(), 0);
        assert_eq!(tiny.plan_read(.., None).disk.len(), 1);
    }

    #[test]
    fn a_limited_read_plans_a_prefix_of_at_least_one_entry() {
        let mut log = GroupLog::new(0);
        append(
            &mut log,
            (1..=4).map(|i| entry(1, i, 100)).collect(),
            at(1, 32),
            ApplyMode::Live,
        );
        append(
            &mut log,
            (5..=8).map(|i| entry(1, i, 100)).collect(),
            at(1, 900),
            ApplyMode::Live,
        );
        let plan = log.plan_read(1..=8, Some(2 * weight(100)));
        assert_eq!(plan.disk, vec![DiskRead {
            frame: at(1, 32),
            log_ids: vec![log_id(1, 1), log_id(1, 2)],
        }]);
        let plan = log.plan_read(1..=8, Some(1));
        assert_eq!(
            plan.disk
                .iter()
                .map(|read| read.log_ids.len())
                .sum::<usize>(),
            1
        );
    }

    /// #507: the limit weighs cached entries as well as the ones read from
    /// disk; a cached tail used to come whole, however large.
    #[test]
    fn a_limited_read_weighs_cached_entries_too() {
        let mut log = GroupLog::new(1 << 20);
        append(
            &mut log,
            (1..=8).map(|i| entry(1, i, 100)).collect(),
            at(1, 32),
            ApplyMode::Live,
        );
        let mut plan = log.plan_read(1..=8, Some(3 * weight(100)));
        assert!(plan.disk.is_empty());
        assert_eq!(
            plan.cached
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        // The plan stopped before entry 4, so no pending entry may follow it.
        assert!(!plan.budget.take(0));
        // A plan that covers its range leaves the rest of the limit to the
        // pending entries after it.
        let mut covered = log.plan_read(1..=2, Some(3 * weight(100)));
        assert!(covered.budget.take(weight(100)));
        assert!(!covered.budget.take(1));
        assert_eq!(log.plan_read(1..=8, None).cached.len(), 8);
    }

    /// One rule weighs a read, the entries `plan_read` plans and the pending
    /// ones after them alike: it takes entries while their total stays within
    /// the limit, and the first one whatever it weighs. A refusal ends the
    /// read, so a lighter entry after it is refused too, and nothing follows
    /// a first entry over the limit.
    #[test]
    fn a_read_budget_takes_a_prefix_within_its_limit_and_at_least_one_entry() {
        let mut budget = ReadBudget::new(Some(10));
        assert!(budget.take(4));
        assert!(budget.take(6));
        assert!(!budget.take(1));
        let mut stopped = ReadBudget::new(Some(10));
        assert!(stopped.take(4));
        assert!(!stopped.take(7));
        assert!(!stopped.take(6));
        let mut oversized = ReadBudget::new(Some(10));
        assert!(oversized.take(11));
        assert!(!oversized.take(0));
        let mut whole = ReadBudget::new(None);
        assert!(whole.take(u64::MAX));
        assert!(whole.take(u64::MAX));
    }

    #[test]
    fn relocation_moves_live_entries_only_from_where_they_were() {
        let mut log = GroupLog::new(0);
        append(
            &mut log,
            (1..=2).map(|i| entry(1, i, 100)).collect(),
            at(1, 32),
            ApplyMode::Live,
        );
        log.apply(
            RaftGroupLogRecord::SaveCommitted(Some(log_id(1, 2))),
            at(1, 500),
            ApplyMode::Live,
        )
        .expect("commit");
        let records = log.records_in(SegmentId(1));
        assert_eq!(records.entries.len(), 2);
        assert_eq!(records.committed, Some(Some(log_id(1, 2))));
        assert_eq!(records.purged, None);

        log.relocate(1, at(1, 32), at(5, 64));
        log.relocate(2, at(2, 32), at(5, 64));
        log.relocate_committed(SegmentId(1), SegmentId(5));
        assert_eq!(
            log.live_in(SegmentId(1)),
            weight(100),
            "entry 2 was not at that position"
        );
        assert_eq!(log.live_in(SegmentId(5)), weight(100) + MARKER_BYTES);
        log.relocate(2, at(1, 32), at(5, 64));
        assert_eq!(log.oldest_segment(), Some(SegmentId(5)));
        assert!(log.records_in(SegmentId(1)).is_empty());
    }

    /// The runtime index is a consecutive run: it extends at either end,
    /// replaces in place, and drops a suffix or a prefix.
    #[test]
    fn the_consecutive_index_follows_appends_truncates_and_purges() {
        use std::ops::Bound;

        use super::EntryIndex;
        use super::IndexedEntry;

        let indexed = |term: u64, index: u64| IndexedEntry {
            log_id: log_id(term, index),
            frame: at(1, index),
            bytes: 1,
            storage_bytes: 1,
        };
        let indexes = |index: &EntryIndex, range: (Bound<u64>, Bound<u64>)| {
            index
                .range(range)
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>()
        };
        let mut index = EntryIndex::default();
        for i in 5..=9 {
            assert_eq!(index.insert(indexed(1, i)).expect("extend"), None);
        }
        assert_eq!(index.insert(indexed(1, 4)).expect("precede"), None);
        assert_eq!(
            index.insert(indexed(2, 6)).expect("replace"),
            Some(indexed(1, 6))
        );
        assert!(index.insert(indexed(1, 11)).is_err(), "a gap after the run");
        assert!(index.insert(indexed(1, 2)).is_err(), "a gap before the run");
        let all = (Bound::Unbounded, Bound::Unbounded);
        assert_eq!(indexes(&index, all), [4, 5, 6, 7, 8, 9]);
        assert_eq!(indexes(&index, (Bound::Included(6), Bound::Excluded(9))), [
            6, 7, 8
        ]);
        assert_eq!(indexes(&index, (Bound::Excluded(2), Bound::Included(5))), [
            4, 5
        ]);
        assert!(indexes(&index, (Bound::Included(1), Bound::Included(3))).is_empty());
        assert!(indexes(&index, (Bound::Included(10), Bound::Unbounded)).is_empty());
        assert_eq!(
            index.get_mut(6).map(|entry| entry.log_id),
            Some(log_id(2, 6))
        );

        let mut truncated = Vec::new();
        index.remove_after(Some(7), |entry| truncated.push(entry.log_id.index));
        assert_eq!(truncated, [8, 9]);
        let mut purged = Vec::new();
        index.remove_through(5, |entry| purged.push(entry.log_id.index));
        assert_eq!(purged, [4, 5]);
        assert_eq!(index.first_index(), Some(6));
        assert_eq!(index.last().map(|entry| entry.log_id.index), Some(7));
        let mut none = Vec::new();
        index.remove_through(3, |entry| none.push(entry.log_id.index));
        assert!(none.is_empty());
        let mut rest = 0;
        index.remove_after(None, |_| rest += 1);
        assert_eq!(rest, 2);
        assert!(index.is_empty());
        assert_eq!(
            index.insert(indexed(3, 20)).expect("restart anywhere"),
            None
        );
        assert_eq!(index.first_index(), Some(20));
    }

    #[test]
    fn key_log_ids_are_the_last_of_each_leaders_run() {
        let mut log = GroupLog::new(0);
        append(
            &mut log,
            (1..=3).map(|i| entry(1, i, 1)).collect(),
            at(1, 32),
            ApplyMode::Live,
        );
        append(
            &mut log,
            (4..=5).map(|i| entry(2, i, 1)).collect(),
            at(1, 64),
            ApplyMode::Live,
        );
        append(&mut log, vec![entry(4, 6, 1)], at(1, 96), ApplyMode::Live);
        assert_eq!(log.key_log_ids(1, 6), [
            log_id(1, 3),
            log_id(2, 5),
            log_id(4, 6)
        ]);
        assert_eq!(log.key_log_ids(2, 4), [log_id(1, 3), log_id(2, 4)]);
    }

    fn membership(term: u64, index: u64, voters: &[u64]) -> Entry {
        let voters = voters
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        Entry::new(
            log_id(term, index),
            EntryPayload::Membership(openraft::Membership::new_with_defaults(vec![voters], [])),
        )
    }

    fn voters_at(log: &GroupLog, index: u64) -> Option<Vec<u64>> {
        log.membership_at_or_below(index)
            .map(|membership| membership.membership().voter_ids().collect())
    }

    /// #613: the log tracks the membership entries it holds, through a live
    /// append, an overwrite by a later leader, a truncation and a purge.
    #[test]
    fn the_log_tracks_its_membership_entries() {
        let mut log = GroupLog::new(1 << 20);
        append(
            &mut log,
            vec![
                membership(1, 1, &[1, 2, 3]),
                entry(1, 2, 8),
                membership(1, 3, &[1, 2, 3, 4]),
                entry(1, 4, 8),
            ],
            at(1, 0),
            ApplyMode::Live,
        );
        assert_eq!(voters_at(&log, 2), Some(vec![1, 2, 3]));
        assert_eq!(voters_at(&log, 4), Some(vec![1, 2, 3, 4]));
        // A later leader overwrites the membership entry at 3 with data.
        log.apply(
            RaftGroupLogRecord::TruncateAfter(Some(log_id(1, 2))),
            at(1, 100),
            ApplyMode::Live,
        )
        .expect("truncate");
        assert_eq!(voters_at(&log, 4), Some(vec![1, 2, 3]));
        append(&mut log, vec![entry(2, 3, 8)], at(1, 200), ApplyMode::Live);
        assert_eq!(voters_at(&log, 3), Some(vec![1, 2, 3]));
        // A purge drops what the state machine's membership covers.
        log.apply(
            RaftGroupLogRecord::Purge(log_id(1, 2)),
            at(1, 300),
            ApplyMode::Live,
        )
        .expect("purge");
        assert_eq!(voters_at(&log, 3), None);
        // Truncating everything drops every membership entry.
        append(
            &mut log,
            vec![membership(2, 4, &[1, 2])],
            at(1, 400),
            ApplyMode::Live,
        );
        assert_eq!(voters_at(&log, 4), Some(vec![1, 2]));
        log.apply(
            RaftGroupLogRecord::TruncateAfter(Some(log_id(2, 3))),
            at(1, 500),
            ApplyMode::Live,
        )
        .expect("truncate");
        assert_eq!(voters_at(&log, 9), None);
    }

    /// #613: replay rebuilds the same membership entries, a rewritten entry
    /// replacing the one before it at its index.
    #[test]
    fn replay_rebuilds_the_membership_entries() {
        let mut log = GroupLog::replaying(1 << 20);
        append(
            &mut log,
            vec![
                membership(1, 1, &[1, 2, 3]),
                membership(1, 2, &[1, 2, 3, 4]),
            ],
            at(1, 0),
            ApplyMode::Replay,
        );
        append(&mut log, vec![entry(2, 2, 8)], at(2, 0), ApplyMode::Replay);
        log.finish_replay().expect("a consecutive log");
        assert_eq!(voters_at(&log, 2), Some(vec![1, 2, 3]));
    }
}
