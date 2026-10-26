//! A metering consumer of the per-stream usage export, as the export's
//! contract tells one to read it (operations, "Per-stream usage", rules 1
//! to 9 and 12), for the `stream-usage-bills-exactly` family
//! ([`super::stream_usage`]). It models what a consumer accounts, not when
//! it reads or what gauges it samples: rules 10 and 11, and the zero gauge
//! samples of rules 6 and 7, are not modelled.
//!
//! It reads each replica through the route's own conversions: a read
//! built by [`ursula::stream_usage_read`] from its cursor, and the answer of
//! [`ursula_runtime::GroupEngine::stream_usage`] rendered by
//! [`ursula::group_usage_answer`] and sent through its JSON. Per group it
//! keeps the listed page with the highest log id, asks for a `more` page's
//! next position at once, and accounts each committed unit to a version, or
//! to its bucket when the version's end record folded before it read it.
//! What the contract tells a consumer to alarm on lands in
//! [`MeteredGroup::alarms`], which the scenario fails on.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_proto::admin::BucketUsageRow;
use ursula_proto::admin::CommittedUsageReport;
use ursula_proto::admin::GroupPurgeIndex;
use ursula_proto::admin::GroupStreamUsage;
use ursula_proto::admin::GroupUsagePage;
use ursula_proto::admin::HistoryId;
use ursula_proto::admin::RelistReason;
use ursula_proto::admin::StreamUsageEnd;
use ursula_proto::admin::StreamUsageEntry;
use ursula_proto::admin::UsageCursor;
use ursula_proto::admin::UsageNext;
use ursula_proto::admin::UsagePage;
use ursula_proto::admin::UsagePosition;
use ursula_proto::admin::VersionCountersReport;
use ursula_proto::admin::VersionLifecycle;

/// A version's counters, as the export lists them.
pub(super) type Counters = VersionCountersReport;

/// A version's key (rule 4): never its incarnation, which changes at every
/// history event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct VersionKey {
    pub(super) bucket_id: String,
    pub(super) stream_id: String,
    pub(super) created_at_ms: u64,
    pub(super) counted_since: HistoryId,
}

impl VersionKey {
    pub(super) fn of(entry: &StreamUsageEntry) -> Self {
        Self {
            bucket_id: entry.bucket_id.clone(),
            stream_id: entry.stream_id.clone(),
            created_at_ms: entry.created_at_ms,
            counted_since: entry.counted_since,
        }
    }
}

pub(super) fn committed(counters: &Counters) -> CommittedUsageReport {
    CommittedUsageReport {
        committed_append_bytes: counters.committed_append_bytes,
        committed_records: counters.committed_records,
        committed_write_units: counters.committed_write_units,
    }
}

pub(super) fn add(to: &mut Counters, by: Counters) {
    to.committed_append_bytes = to
        .committed_append_bytes
        .saturating_add(by.committed_append_bytes);
    to.committed_records = to.committed_records.saturating_add(by.committed_records);
    to.committed_write_units = to
        .committed_write_units
        .saturating_add(by.committed_write_units);
    to.appends = to.appends.saturating_add(by.appends);
}

pub(super) fn add_committed(to: &mut CommittedUsageReport, by: CommittedUsageReport) {
    to.committed_append_bytes = to
        .committed_append_bytes
        .saturating_add(by.committed_append_bytes);
    to.committed_records = to.committed_records.saturating_add(by.committed_records);
    to.committed_write_units = to
        .committed_write_units
        .saturating_add(by.committed_write_units);
}

/// `counters - level`, `None` when a counter is below its level.
fn minus(counters: Counters, level: Counters) -> Option<Counters> {
    Some(Counters {
        committed_append_bytes: counters
            .committed_append_bytes
            .checked_sub(level.committed_append_bytes)?,
        committed_records: counters
            .committed_records
            .checked_sub(level.committed_records)?,
        committed_write_units: counters
            .committed_write_units
            .checked_sub(level.committed_write_units)?,
        appends: counters.appends.checked_sub(level.appends)?,
    })
}

/// `total - part`, `None` when a counter is below its part.
fn committed_minus(
    total: CommittedUsageReport,
    part: CommittedUsageReport,
) -> Option<CommittedUsageReport> {
    Some(CommittedUsageReport {
        committed_append_bytes: total
            .committed_append_bytes
            .checked_sub(part.committed_append_bytes)?,
        committed_records: total
            .committed_records
            .checked_sub(part.committed_records)?,
        committed_write_units: total
            .committed_write_units
            .checked_sub(part.committed_write_units)?,
    })
}

/// `max(0, total - part)`, per counter.
fn committed_minus_floor(
    total: CommittedUsageReport,
    part: CommittedUsageReport,
) -> CommittedUsageReport {
    CommittedUsageReport {
        committed_append_bytes: total
            .committed_append_bytes
            .saturating_sub(part.committed_append_bytes),
        committed_records: total
            .committed_records
            .saturating_sub(part.committed_records),
        committed_write_units: total
            .committed_write_units
            .saturating_sub(part.committed_write_units),
    }
}

/// The lower of `level` and `baseline`, per counter.
fn lowered(level: Counters, baseline: Counters) -> Counters {
    Counters {
        committed_append_bytes: level
            .committed_append_bytes
            .min(baseline.committed_append_bytes),
        committed_records: level.committed_records.min(baseline.committed_records),
        committed_write_units: level
            .committed_write_units
            .min(baseline.committed_write_units),
        appends: level.appends.min(baseline.appends),
    }
}

/// Whether an answer holds a value of a later release anywhere, which
/// makes it no answer (rule 2).
fn holds_unknown(page: &GroupUsagePage) -> bool {
    matches!(page.page, UsagePage::Unknown)
        || matches!(page.page, UsagePage::More {
            next: UsageNext::Unknown
        })
        || page.relist == Some(RelistReason::Unknown)
        || page.entries.iter().any(|entry| {
            matches!(
                entry.lifecycle,
                VersionLifecycle::Unknown
                    | VersionLifecycle::Ended {
                        end: StreamUsageEnd::Unknown,
                        ..
                    }
            )
        })
}

/// One group as the consumer follows it.
#[derive(Debug, Default)]
pub(super) struct MeteredGroup {
    /// The cursor of the next read, `start` before the first (rule 1).
    cursor: Option<UsageCursor>,
    /// The page kept last.
    kept: Option<GroupUsagePage>,
    /// The pages of a relisting in progress, from its first.
    relisting: Vec<GroupUsagePage>,
    /// The history of the last listing applied, `None` before the first.
    history: Option<HistoryId>,
    /// The billed level of each version held (rule 5).
    billed: BTreeMap<VersionKey, Counters>,
    /// The held versions that ended, by end-record seq, until
    /// `retired_through` reaches it (rule 6).
    ended: BTreeMap<VersionKey, u64>,
    /// The versions whose end record folded after they were read, with
    /// their final counters and when the fold was seen, for one more
    /// retention (rule 6).
    folded: BTreeMap<VersionKey, (Counters, madsim::time::Instant)>,
    /// The retired usage accounted per bucket (rule 5).
    retired_view: BTreeMap<String, CommittedUsageReport>,
    /// What was accounted to each version.
    pub(super) accounted: BTreeMap<VersionKey, Counters>,
    /// What was accounted to each bucket rather than to a version: the
    /// usage of versions whose end record folded before a read listed it.
    pub(super) to_buckets: BTreeMap<String, CommittedUsageReport>,
    /// Versions a relisting of the same history closed without listing
    /// them: their end record folded before the consumer read it.
    pub(super) closed_unlisted: BTreeSet<VersionKey>,
    /// Versions a relisting of another history closed: a restore replaced
    /// them, or an accepted loss removed them.
    pub(super) closed_by_history: BTreeSet<VersionKey>,
    /// How each version listed as ended ended.
    pub(super) ends: BTreeMap<VersionKey, StreamUsageEnd>,
    /// The relistings applied, by reason.
    pub(super) relists: Vec<RelistReason>,
    /// What the contract tells a consumer to alarm on.
    pub(super) alarms: Vec<String>,
    /// Held versions whose end record the consumer saw fold (rule 6).
    pub(super) folds: u64,
    /// Relistings applied across a history change (rule 7).
    pub(super) history_changes: u64,
}

impl MeteredGroup {
    /// The cursor of the next read (rules 1, 3).
    pub(super) fn cursor(&self) -> UsageCursor {
        self.cursor.clone().unwrap_or(UsageCursor::Start)
    }

    /// The last page kept is complete, and no relisting is in progress
    /// (rule 9).
    pub(super) fn complete(&self) -> bool {
        self.relisting.is_empty()
            && self
                .kept
                .as_ref()
                .is_some_and(|page| page.page == UsagePage::Complete)
    }

    /// The last page kept is `more`: its next page is asked for at once
    /// (rule 3).
    pub(super) fn wants_next_page(&self) -> bool {
        self.kept
            .as_ref()
            .is_some_and(|page| matches!(page.page, UsagePage::More { .. }))
    }

    /// Asks the next read to relist the group (`requested`), to check its
    /// totals (rule 8).
    pub(super) fn request_relisting(&mut self) {
        if let Some(UsageCursor::After { position, .. }) = &mut self.cursor {
            *position = UsagePosition::Rows { after_bucket: None };
        }
    }

    /// Whether a purge of a bucket in this group, at `index`, is complete
    /// for metering (rule 12): the kept complete page is of the purge's
    /// history and at or past its entry.
    pub(super) fn purge_complete(&self, index: &GroupPurgeIndex) -> bool {
        self.complete()
            && self.kept.as_ref().is_some_and(|page| {
                page.history.id == index.history && page.applied.index >= index.commit_index
            })
    }

    /// The live versions held of `bucket_id`.
    pub(super) fn live_of(&self, bucket_id: &str) -> Vec<VersionKey> {
        self.billed
            .keys()
            .filter(|key| key.bucket_id == bucket_id && !self.ended.contains_key(*key))
            .cloned()
            .collect()
    }

    fn alarm(&mut self, message: String) {
        self.alarms.push(message);
    }

    /// Keeps, of one read's `answers` from the replicas it reached, the
    /// listed page with the highest log id, by term and then index (rule
    /// 2): `behind`, `unavailable` and an answer that holds an unknown
    /// value are no answer. Returns whether a page was kept, and counts the
    /// `behind` answers in `behind`.
    pub(super) fn take_answers(
        &mut self,
        answers: Vec<GroupStreamUsage>,
        now: madsim::time::Instant,
        retention: Duration,
        behind: &mut u64,
    ) -> bool {
        let mut newest: Option<GroupUsagePage> = None;
        for answer in answers {
            match answer {
                GroupStreamUsage::Listed(page) if !holds_unknown(&page) => {
                    if newest
                        .as_ref()
                        .is_none_or(|kept| kept.applied < page.applied)
                    {
                        newest = Some(page);
                    }
                }
                GroupStreamUsage::Behind { .. } => *behind = behind.saturating_add(1),
                GroupStreamUsage::Listed(_)
                | GroupStreamUsage::Unavailable { .. }
                | GroupStreamUsage::Unknown => {}
            }
        }
        match newest {
            Some(page) => {
                self.keep(page, now, retention);
                true
            }
            None => false,
        }
    }

    /// Keeps `page`: the next cursor is its own (rule 3), it continues the
    /// listing (rule 6) or a relisting (rule 7), and a complete page checks
    /// the counts (rule 8).
    fn keep(&mut self, page: GroupUsagePage, now: madsim::time::Instant, retention: Duration) {
        let Some(cursor) = page.next_cursor() else {
            return;
        };
        self.cursor = Some(cursor);
        let complete = page.page == UsagePage::Complete;
        if page.relist.is_some() {
            // A relisting starts, or restarts one in progress.
            self.relisting = vec![page.clone()];
        } else if !self.relisting.is_empty() {
            self.relisting.push(page.clone());
        } else {
            self.continue_listing(&page);
            self.retire_held(page.retired_through, now);
        }
        if complete && !self.relisting.is_empty() {
            let pages = std::mem::take(&mut self.relisting);
            self.apply_relisting(&pages);
            self.retire_held(page.retired_through, now);
        }
        if complete && self.relisting.is_empty() {
            let live = self.billed.len().saturating_sub(self.ended.len());
            let held = (
                u64::try_from(live).unwrap_or(u64::MAX),
                u64::try_from(self.ended.len()).unwrap_or(u64::MAX),
            );
            if (page.live_versions, page.end_records) != held {
                self.alarm(format!(
                    "group {}: a complete page counts {} live versions and {} end records at \
                     {:?}, the consumer holds {held:?}",
                    page.raft_group_id, page.live_versions, page.end_records, page.applied
                ));
            }
        }
        self.kept = Some(page);
        self.folded
            .retain(|_, (_, seen)| now.saturating_duration_since(*seen) < retention);
    }

    /// Rule 6: each entry accounts what it committed above its billed
    /// level.
    fn continue_listing(&mut self, page: &GroupUsagePage) {
        for entry in &page.entries {
            let key = VersionKey::of(entry);
            if self.ended.contains_key(&key)
                && matches!(entry.lifecycle, VersionLifecycle::Live { .. })
            {
                self.alarm(format!(
                    "group {}: {key:?} is listed live after its end record",
                    page.raft_group_id
                ));
            }
            let level = self.billed.get(&key).copied().unwrap_or_default();
            self.account(page.raft_group_id, entry, level);
        }
    }

    /// Accounts `entry`'s counters above `level`, and holds it at its
    /// counters, ended or live.
    fn account(&mut self, group: u32, entry: &StreamUsageEntry, level: Counters) {
        let key = VersionKey::of(entry);
        match minus(entry.counters, level) {
            Some(delta) => add(self.accounted.entry(key.clone()).or_default(), delta),
            None => self.alarm(format!(
                "group {group}: {key:?} lists counters {:?} below its billed level {level:?}",
                entry.counters
            )),
        }
        self.billed.insert(key.clone(), entry.counters);
        match entry.lifecycle {
            // Across histories a version held as ended can relist live: the
            // removal was in a replaced suffix.
            VersionLifecycle::Live { .. } => {
                self.ended.remove(&key);
            }
            VersionLifecycle::Ended { end, .. } => {
                self.ended.insert(key.clone(), entry.seq);
                self.ends.insert(key, end);
            }
            VersionLifecycle::Unknown => {}
        }
    }

    /// Rule 7: applies a relisting once it completed, by its history
    /// compared with that of the last listing applied.
    fn apply_relisting(&mut self, pages: &[GroupUsagePage]) {
        let Some(first) = pages.first() else {
            return;
        };
        let group = first.raft_group_id;
        let reason = first.relist.unwrap_or(RelistReason::Unknown);
        let history = first.history.id;
        let rows: BTreeMap<String, BucketUsageRow> = pages
            .iter()
            .flat_map(|page| &page.rows)
            .map(|row| (row.bucket_id.clone(), row.clone()))
            .collect();
        let mut listed = BTreeMap::<VersionKey, StreamUsageEntry>::new();
        for entry in pages.iter().flat_map(|page| &page.entries) {
            let key = VersionKey::of(entry);
            if listed.get(&key).is_none_or(|seen| seen.seq < entry.seq) {
                listed.insert(key, entry.clone());
            }
        }
        match self.history {
            // Nothing applied yet: the baseline, which accounts nothing.
            None => {
                for entry in listed.values() {
                    let key = VersionKey::of(entry);
                    self.billed.insert(key.clone(), entry.counters);
                    if let VersionLifecycle::Ended { end, .. } = entry.lifecycle {
                        self.ended.insert(key.clone(), entry.seq);
                        self.ends.insert(key, end);
                    }
                }
                for (bucket_id, row) in &rows {
                    self.retired_view.insert(bucket_id.clone(), row.retired);
                }
            }
            // The same history (`requested` or `retired`).
            Some(previous) if previous == history => {
                for entry in listed.values() {
                    let level = self
                        .billed
                        .get(&VersionKey::of(entry))
                        .copied()
                        .unwrap_or_default();
                    self.account(group, entry, level);
                }
                let closed = self.close_unlisted(&listed, false);
                // Rows are never removed within one history: a bucket that
                // closed a version and lists no row has retired nothing.
                let buckets: BTreeSet<String> = rows.keys().chain(closed.keys()).cloned().collect();
                for bucket_id in buckets {
                    let retired = rows
                        .get(&bucket_id)
                        .map(|row| row.retired)
                        .unwrap_or_default();
                    let view = self
                        .retired_view
                        .get(&bucket_id)
                        .copied()
                        .unwrap_or_default();
                    let levels = closed.get(&bucket_id).copied().unwrap_or_default();
                    match committed_minus(retired, view)
                        .and_then(|share| committed_minus(share, levels))
                    {
                        Some(share) => add_committed(
                            self.to_buckets.entry(bucket_id.clone()).or_default(),
                            share,
                        ),
                        None => self.alarm(format!(
                            "group {group}: bucket {bucket_id}'s share is negative: retired \
                             {retired:?}, retired view {view:?}, closed levels {levels:?}"
                        )),
                    }
                    self.retired_view.insert(bucket_id, retired);
                }
            }
            // Another history: an accepted loss, a restore, or another
            // cluster's group.
            Some(_) => {
                self.history_changes = self.history_changes.saturating_add(1);
                for key in listed.keys() {
                    if let Some((counters, _)) = self.folded.remove(key) {
                        // Its fold was in the replaced suffix: held again.
                        let view = self.retired_view.entry(key.bucket_id.clone()).or_default();
                        match committed_minus(*view, committed(&counters)) {
                            Some(rest) => *view = rest,
                            None => {
                                let view = *view;
                                self.alarm(format!(
                                    "group {group}: {key:?} folded with {counters:?} above its \
                                     bucket's retired view {view:?}"
                                ));
                            }
                        }
                        self.billed.insert(key.clone(), counters);
                    }
                }
                for entry in listed.values() {
                    let level = lowered(
                        self.billed
                            .get(&VersionKey::of(entry))
                            .copied()
                            .unwrap_or_default(),
                        entry.history_baseline,
                    );
                    self.account(group, entry, level);
                }
                let closed = self.close_unlisted(&listed, true);
                for (bucket_id, row) in &rows {
                    let view = self
                        .retired_view
                        .get(bucket_id)
                        .copied()
                        .unwrap_or_default();
                    let levels = closed.get(bucket_id).copied().unwrap_or_default();
                    let share =
                        committed_minus_floor(committed_minus_floor(row.retired, view), levels);
                    add_committed(self.to_buckets.entry(bucket_id.clone()).or_default(), share);
                    self.retired_view.insert(bucket_id.clone(), row.retired);
                }
                self.keep_unlisted_rows(&rows, closed);
            }
        }
        self.history = Some(history);
        self.relists.push(reason);
        // Rule 8: a relisting that completed in its first page explains
        // each row's totals by its parts.
        if let [page] = pages {
            for row in &page.rows {
                let mut parts = row.retired;
                let mut retained_bytes = 0_u64;
                let mut snapshot_bytes = 0_u64;
                let mut stream_count = 0_u64;
                for entry in listed
                    .values()
                    .filter(|entry| entry.bucket_id == row.bucket_id)
                {
                    add_committed(&mut parts, committed(&entry.counters));
                    if let VersionLifecycle::Live {
                        retained_bytes: retained,
                        snapshot_bytes: snapshot,
                    } = entry.lifecycle
                    {
                        retained_bytes = retained_bytes.saturating_add(retained);
                        snapshot_bytes = snapshot_bytes.saturating_add(snapshot);
                        stream_count = stream_count.saturating_add(1);
                    }
                }
                let totals = CommittedUsageReport {
                    committed_append_bytes: row.totals.committed_append_bytes,
                    committed_records: row.totals.committed_records,
                    committed_write_units: row.totals.committed_write_units,
                };
                if (
                    totals,
                    row.totals.retained_bytes,
                    row.totals.snapshot_bytes,
                    row.totals.stream_count,
                ) != (parts, retained_bytes, Some(snapshot_bytes), stream_count)
                {
                    self.alarm(format!(
                        "group {group}: bucket {}'s totals {:?} are not its retired usage plus \
                         its versions: counters {parts:?}, retained {retained_bytes}, snapshot \
                         {snapshot_bytes}, streams {stream_count}",
                        row.bucket_id, row.totals
                    ));
                }
            }
        }
    }

    /// Closes each held version `listed` does not list, and returns their
    /// billed levels per bucket.
    fn close_unlisted(
        &mut self,
        listed: &BTreeMap<VersionKey, StreamUsageEntry>,
        other_history: bool,
    ) -> BTreeMap<String, CommittedUsageReport> {
        let closed: Vec<VersionKey> = self
            .billed
            .keys()
            .filter(|key| !listed.contains_key(*key))
            .cloned()
            .collect();
        let mut levels = BTreeMap::<String, CommittedUsageReport>::new();
        for key in closed {
            let Some(level) = self.billed.remove(&key) else {
                continue;
            };
            self.ended.remove(&key);
            add_committed(
                levels.entry(key.bucket_id.clone()).or_default(),
                committed(&level),
            );
            if other_history {
                self.closed_by_history.insert(key);
            } else {
                self.closed_unlisted.insert(key);
            }
        }
        levels
    }

    /// Of another history: a bucket the relisting lists no row for keeps
    /// its retired view, and the levels of its closed versions join it
    /// (rule 7).
    fn keep_unlisted_rows(
        &mut self,
        rows: &BTreeMap<String, BucketUsageRow>,
        closed: BTreeMap<String, CommittedUsageReport>,
    ) {
        for (bucket_id, levels) in closed {
            if !rows.contains_key(&bucket_id) {
                add_committed(self.retired_view.entry(bucket_id).or_default(), levels);
            }
        }
    }

    /// Rule 6: an ended version is held until `retired_through` reaches its
    /// seq, then its final counters join its bucket's retired view and it
    /// is kept as folded for one more retention.
    fn retire_held(&mut self, retired_through: u64, now: madsim::time::Instant) {
        let due: Vec<VersionKey> = self
            .ended
            .iter()
            .filter(|(_, seq)| **seq <= retired_through)
            .map(|(key, _)| key.clone())
            .collect();
        for key in due {
            self.ended.remove(&key);
            let Some(level) = self.billed.remove(&key) else {
                continue;
            };
            add_committed(
                self.retired_view.entry(key.bucket_id.clone()).or_default(),
                committed(&level),
            );
            self.folded.insert(key, (level, now));
            self.folds = self.folds.saturating_add(1);
        }
    }

    /// What was accounted to the versions of `bucket_id`'s stream
    /// `stream_id`, all its keys together.
    pub(super) fn accounted_to(&self, bucket_id: &str, stream_id: &str) -> Counters {
        let mut total = Counters::default();
        for (_, counters) in self
            .accounted
            .iter()
            .filter(|(key, _)| key.bucket_id == bucket_id && key.stream_id == stream_id)
        {
            add(&mut total, *counters);
        }
        total
    }

    /// Whether a relisting of the same history closed a version of
    /// `bucket_id`'s stream `stream_id` without listing it.
    pub(super) fn closed_unlisted_stream(&self, bucket_id: &str, stream_id: &str) -> bool {
        self.closed_unlisted
            .iter()
            .any(|key| key.bucket_id == bucket_id && key.stream_id == stream_id)
    }

    /// What was accounted to `bucket_id`, to its versions and to the
    /// bucket together.
    pub(super) fn accounted_to_bucket(&self, bucket_id: &str) -> CommittedUsageReport {
        let mut total = self.to_buckets.get(bucket_id).copied().unwrap_or_default();
        for (_, counters) in self
            .accounted
            .iter()
            .filter(|(key, _)| key.bucket_id == bucket_id)
        {
            add_committed(&mut total, committed(counters));
        }
        total
    }

    /// The buckets anything was accounted to.
    pub(super) fn buckets(&self) -> BTreeSet<String> {
        self.accounted
            .keys()
            .map(|key| key.bucket_id.clone())
            .chain(self.to_buckets.keys().cloned())
            .collect()
    }
}

/// A metering consumer of every group of a cluster.
#[derive(Debug)]
pub(super) struct UsageMeter {
    pub(super) groups: BTreeMap<u32, MeteredGroup>,
    /// The nodes' `end_record_retention_ms`: how long a folded version is
    /// kept (rule 6).
    pub(super) retention: Duration,
    /// `behind` answers seen.
    pub(super) behind: u64,
    /// Pages kept.
    pub(super) pages: u64,
}

impl UsageMeter {
    pub(super) fn new(groups: impl IntoIterator<Item = u32>, retention: Duration) -> Self {
        Self {
            groups: groups
                .into_iter()
                .map(|group| (group, MeteredGroup::default()))
                .collect(),
            retention,
            behind: 0,
            pages: 0,
        }
    }

    pub(super) fn group(&self, group: u32) -> &MeteredGroup {
        &self.groups[&group]
    }

    /// The held versions whose end record every group saw fold.
    pub(super) fn folds(&self) -> u64 {
        self.groups
            .values()
            .map(|group| group.folds)
            .fold(0, u64::saturating_add)
    }

    /// The relistings every group applied across a history change.
    pub(super) fn history_changes(&self) -> u64 {
        self.groups
            .values()
            .map(|group| group.history_changes)
            .fold(0, u64::saturating_add)
    }

    /// The relistings every group applied with `reason`.
    pub(super) fn relists(&self, reason: RelistReason) -> u64 {
        let count = self
            .groups
            .values()
            .flat_map(|group| &group.relists)
            .filter(|applied| **applied == reason)
            .count();
        u64::try_from(count).unwrap_or(u64::MAX)
    }
}
