//! The staged-part totals of the groups a node leads, for usage (#639).
//!
//! A group's totals belong to the term the node leads it in. The first
//! update or leadership change in a new term starts them empty, and the
//! counting task then lists the group's part directories, one per bucket,
//! to seed one entry per stream that holds parts. Until that seed finishes
//! the totals are partial and say so. Afterwards they follow every change
//! the node makes or sees: a listing of a stream sets its entry, whatever
//! term the stream's own count came from, a staged part adds to it, and a
//! swept part and a purge take from it. A stream listed while the seed runs
//! keeps its own count, a zero included, over the seed's older one.
//!
//! The one change the totals can miss is a part the orphan sweep deletes
//! after the seed listed it and before the seed finished, which they keep
//! until the stream is listed again. The sweep only deletes parts a day
//! old, so a seed rarely meets one.
//!
//! One entry per stream with staged parts in a group the node leads, about
//! 100 bytes each, and during a seed one per stream listed to zero. A group
//! the node stops leading is dropped.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::RwLock;

/// The term in which this node leads a group now, or `None`.
pub(crate) type Leads = Arc<dyn Fn(u32) -> Option<u64> + Send + Sync>;

/// The totals of every group the node leads.
pub(crate) struct GroupCounts {
    groups: Mutex<HashMap<u32, GroupCount>>,
    leads: RwLock<Leads>,
    /// Woken when an update starts a term's totals, so the counting task
    /// seeds them.
    started: tokio::sync::Notify,
}

/// One group's totals in one term.
struct GroupCount {
    term: u64,
    /// The seed listing finished: the totals are complete.
    finished: bool,
    /// Per stream part-name prefix.
    streams: HashMap<String, StreamCount>,
}

struct StreamCount {
    bucket_id: String,
    parts: u64,
    bytes: u64,
    /// A listing of the stream itself set it, not the seed.
    listed: bool,
}

/// One group's staged-part bytes per bucket, as the node counts them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GroupReport {
    pub(crate) raft_group_id: u32,
    pub(crate) term: u64,
    pub(crate) counting_finished: bool,
    pub(crate) buckets: BTreeMap<String, u64>,
}

impl GroupCounts {
    pub(crate) fn new(leads: Leads) -> Self {
        Self {
            groups: Mutex::default(),
            leads: RwLock::new(leads),
            started: tokio::sync::Notify::new(),
        }
    }

    /// Replaces the source of the node's leadership.
    pub(crate) fn set_leads(&self, leads: Leads) {
        *self.leads.write().unwrap_or_else(PoisonError::into_inner) = leads;
    }

    /// The term in which the node leads `group` now.
    pub(crate) fn leader_term(&self, group: u32) -> Option<u64> {
        let leads = Arc::clone(&self.leads.read().unwrap_or_else(PoisonError::into_inner));
        leads(group)
    }

    /// Waits until an update started a term's totals.
    pub(crate) async fn started(&self) {
        self.started.notified().await;
    }

    fn groups(&self) -> std::sync::MutexGuard<'_, HashMap<u32, GroupCount>> {
        self.groups.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The totals of `group` in `term`, started empty when the group has
    /// none or an older term's. `None` for an older term than the totals',
    /// whose update a newer leader's totals ignore.
    fn count<'a>(
        groups: &'a mut HashMap<u32, GroupCount>,
        group: u32,
        term: u64,
        started: &tokio::sync::Notify,
    ) -> Option<&'a mut GroupCount> {
        let stale = groups.get(&group).is_none_or(|count| count.term < term);
        if stale {
            groups.insert(group, GroupCount {
                term,
                finished: false,
                streams: HashMap::new(),
            });
            started.notify_one();
        }
        groups.get_mut(&group).filter(|count| count.term == term)
    }

    /// The node leads `group` in `term`: `true` when its totals need a seed
    /// listing, because the term is new or the last seed did not finish.
    pub(crate) fn lead(&self, group: u32, term: u64) -> bool {
        let mut groups = self.groups();
        Self::count(&mut groups, group, term, &self.started).is_some_and(|count| !count.finished)
    }

    /// The node no longer leads `group`.
    pub(crate) fn follow(&self, group: u32) {
        self.groups().remove(&group);
    }

    /// The seed listing of `group` in `term` found `seeded`: per stream
    /// prefix, its bucket, parts and bytes. A stream a listing of its own
    /// set meanwhile keeps that count.
    pub(crate) fn seeded(
        &self,
        group: u32,
        term: u64,
        seeded: HashMap<String, (String, u64, u64)>,
    ) {
        let mut groups = self.groups();
        let Some(count) = groups.get_mut(&group).filter(|count| count.term == term) else {
            return;
        };
        for (prefix, (bucket_id, parts, bytes)) in seeded {
            if count
                .streams
                .get(&prefix)
                .is_some_and(|stream| stream.listed)
            {
                continue;
            }
            count.streams.insert(prefix, StreamCount {
                bucket_id,
                parts,
                bytes,
                listed: false,
            });
        }
        count.streams.retain(|_, stream| stream.parts > 0);
        count.finished = true;
    }

    /// A listing of one stream counted `parts` and `bytes`, which is true
    /// now whatever term the stream's own count came from.
    pub(crate) fn listed(&self, group: u32, prefix: &str, bucket_id: &str, parts: u64, bytes: u64) {
        let Some(term) = self.leader_term(group) else {
            return;
        };
        let mut groups = self.groups();
        let Some(count) = Self::count(&mut groups, group, term, &self.started) else {
            return;
        };
        if parts == 0 && count.finished {
            count.streams.remove(prefix);
            return;
        }
        // A zero stays until the seed finishes, so the seed's older count
        // of the stream does not come back.
        count.streams.insert(prefix.to_owned(), StreamCount {
            bucket_id: bucket_id.to_owned(),
            parts,
            bytes,
            listed: true,
        });
    }

    /// A part of `size` bytes was staged. The stream was listed before it
    /// reserved the part, in this term.
    pub(crate) fn staged(&self, group: u32, prefix: &str, bucket_id: &str, size: u64) {
        let Some(term) = self.leader_term(group) else {
            return;
        };
        let mut groups = self.groups();
        let Some(count) = Self::count(&mut groups, group, term, &self.started) else {
            return;
        };
        let stream = count
            .streams
            .entry(prefix.to_owned())
            .or_insert_with(|| StreamCount {
                bucket_id: bucket_id.to_owned(),
                parts: 0,
                bytes: 0,
                listed: true,
            });
        stream.parts = stream.parts.saturating_add(1);
        stream.bytes = stream.bytes.saturating_add(size);
    }

    /// The orphan sweep deleted a part of `size` bytes.
    pub(crate) fn swept(&self, group: u32, prefix: &str, size: u64) {
        let mut groups = self.groups();
        let Some(count) = groups.get_mut(&group) else {
            return;
        };
        if let Some(stream) = count.streams.get_mut(prefix) {
            stream.parts = stream.parts.saturating_sub(1);
            stream.bytes = stream.bytes.saturating_sub(size);
            if stream.parts == 0 && count.finished {
                count.streams.remove(prefix);
            }
        }
    }

    /// A purge erased every part of `bucket_id`.
    pub(crate) fn purged(&self, bucket_id: &str) {
        for count in self.groups().values_mut() {
            count
                .streams
                .retain(|_, stream| stream.bucket_id != bucket_id);
        }
    }

    /// The totals of the groups the node leads now, in the term it leads
    /// each in, in group order.
    pub(crate) fn report(&self) -> Vec<GroupReport> {
        let groups = self.groups();
        let mut reports = groups
            .iter()
            .filter(|(group, count)| self.leader_term(**group) == Some(count.term))
            .map(|(group, count)| {
                let mut buckets = BTreeMap::<String, u64>::new();
                for stream in count.streams.values().filter(|stream| stream.parts > 0) {
                    let bytes = buckets.entry(stream.bucket_id.clone()).or_default();
                    *bytes = bytes.saturating_add(stream.bytes);
                }
                GroupReport {
                    raft_group_id: *group,
                    term: count.term,
                    counting_finished: count.finished,
                    buckets,
                }
            })
            .collect::<Vec<_>>();
        reports.sort_by_key(|report| report.raft_group_id);
        reports
    }
}
