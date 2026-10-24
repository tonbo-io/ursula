//! The checks of removed nodes in the `placement-move` family (design 8.1,
//! 8.3 and 15.2), over a placed [`MetaCluster`], at every observation of a
//! run, and what each judged:
//!
//! - I13 ([`I13`]), per life: a life of a node id is its root. A life the
//!   latest view shows removed stays out: the latest view shows that root's
//!   record removed, or the id vacant on that root released, or a new life
//!   on another root, which the kernel binds only on a vacant id. No running node's
//!   view at or past the entry the life was first seen removed at gives
//!   that life a role (a role of a new life's record is the new life's),
//!   and until a new life binds, no running replica names the id its
//!   group's leader in a term above every term the group's replicas held
//!   then. Its close half is a wait ([`MetaCluster::removed_open`]): once
//!   every group answered, a running removed node holds no replica open.
//! - I8's sweep half ([`I8`]): every DELETE the removed-node sweep sent
//!   names a node the latest view shows removed, and no running replica's
//!   latest committed membership of the object's group lists that node as
//!   a voter.
//! - I5's removal half ([`I5`]) is a wait ([`MetaCluster::unswept`]): once
//!   the sweep passed, the removed node's object directory is empty.
//!
//! A run that removed a node fails at its end when I13's role, leader or
//! close half, I5's removal half or I8's sweep half judged nothing of it
//! ([`RemovalChecks::judged_the_removals`]), so a check that never saw
//! the removal cannot pass it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_runtime::OwnedKey;
use ursula_runtime::snapshot_harness::DeleteKind;
use ursula_runtime::snapshot_harness::SnapshotStoreEvent;
use ursula_shard::RaftGroupId;

use super::SimTrace;
use super::journal_cluster::JOURNAL_GROUPS;
use super::meta_checks::fail;
use super::meta_cluster::MetaCluster;
use super::placed_hosting::I5;
use super::placed_hosting::I8;

pub(super) const I13: &str = "a_removed_node_stays_out";

/// When the checks first saw a node's life removed, and how far its id
/// got since.
#[derive(Debug, Clone)]
struct Removal {
    /// The root of the removed life.
    root: RootId,
    /// The latest view's applied index then.
    applied: u64,
    /// The highest term each group's running replicas held then.
    terms: BTreeMap<u32, u64>,
    /// Whether a latest view showed a new life of the id, on another root.
    reused: bool,
}

/// Whether `record` is the removed life on `root`'s record: its slot names
/// that root.
fn of_life(record: &NodeRecord, root: RootId) -> bool {
    record.root.root() == root
}

/// What the removal checks judged of one removed node.
#[derive(Debug, Default)]
struct Judged {
    /// I13's role half: the views at or past its removal it read.
    views: u64,
    /// I13's leader half: the running replicas whose leader it read.
    leaders: u64,
    /// I13's close half: whether a wait found the node holding nothing
    /// open.
    closed: bool,
    /// I5's removal half: whether a wait found its directory empty after
    /// the sweep.
    emptied: bool,
    /// I8's sweep half: the sweep's DELETEs of the node's objects it
    /// judged, by group.
    sweeps: BTreeMap<u32, u64>,
}

/// What the removal checks keep across a run.
#[derive(Debug, Default)]
pub(super) struct RemovalChecks {
    removed: BTreeMap<u64, Removal>,
    judged: BTreeMap<u64, Judged>,
    /// How many of the snapshot bucket's events I8's sweep half read.
    events_read: usize,
}

impl MetaCluster {
    /// I13's close half: why a running node the latest view shows removed
    /// still holds a replica open, if one does.
    pub(super) fn removed_open(&self, node: u64) -> Option<String> {
        if !self.runtimes.contains_key(&node) {
            return None;
        }
        let open: Vec<u32> = self
            .hosted_replicas(node)
            .iter()
            .map(|replica| replica.group)
            .collect();
        (!open.is_empty()).then(|| format!("removed node {node} still runs groups {open:?}"))
    }

    /// The object keys of `node` the snapshot bucket holds, by group.
    pub(super) fn objects_of(&self, node: u64) -> BTreeMap<u32, BTreeSet<String>> {
        let bucket = self.data.snapshot_bucket();
        let mut objects: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
        for key in bucket.keys() {
            if let Ok(owned) = OwnedKey::parse(bucket.prefix(), &key)
                && owned.owner().node_id() == node
            {
                objects
                    .entry(owned.owner().raft_group_id().0)
                    .or_default()
                    .insert(key);
            }
        }
        objects
    }

    /// I5's removal half: why `node`'s object directory is not empty, if it
    /// is not.
    pub(super) fn unswept(&self, node: u64) -> Option<String> {
        let objects = self.objects_of(node);
        (!objects.is_empty())
            .then(|| format!("the sweep left removed node {node}'s snapshot objects {objects:?}"))
    }
}

impl RemovalChecks {
    /// I13's role and leader halves and I8's sweep half now.
    pub(super) fn observe(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        self.learn_removals(cluster, trace, after_event);
        for (node, removal) in &self.removed {
            let judged = self.judged.entry(*node).or_default();
            stays_without_role(cluster, *node, removal, judged, trace, after_event);
            leads_nothing(cluster, *node, removal, judged, trace, after_event);
        }
        self.sweeps(cluster, trace, after_event);
    }

    /// Notes each node life the latest view shows removed, and fails I13
    /// for a life seen removed whose id the latest view shows anywhere but
    /// out: removed on its root, vacant on it released, or in a new life on
    /// another root.
    fn learn_removals(&mut self, cluster: &MetaCluster, trace: &mut SimTrace, after_event: &str) {
        let latest = cluster.latest_view();
        let Some(bootstrapped) = latest.cluster() else {
            return;
        };
        let applied = latest.applied().map_or(0, |applied| applied.get().index);
        for (node, record) in bootstrapped.nodes() {
            let seen = self
                .removed
                .get(&node.get())
                .is_some_and(|removal| of_life(record, removal.root));
            if record.lifecycle == NodeLifecycle::Removed && !seen {
                let terms = JOURNAL_GROUPS
                    .iter()
                    .map(|group| (*group, cluster.max_term(*group)))
                    .collect();
                self.removed.insert(node.get(), Removal {
                    root: record.root.root(),
                    applied,
                    terms,
                    reused: false,
                });
            }
        }
        for (node, removal) in &mut self.removed {
            let record = bootstrapped.node(NodeId::new(*node)).copied();
            let out = match record {
                Some(record) if of_life(&record, removal.root) => match record.lifecycle {
                    NodeLifecycle::Removed => true,
                    NodeLifecycle::Vacant => record.root == RootSlot::Released(removal.root),
                    NodeLifecycle::Active
                    | NodeLifecycle::Cordoned
                    | NodeLifecycle::Decommissioning => false,
                },
                // A new life: the kernel binds one only on a vacant id
                // (its K5), which a step between the two entries may miss.
                Some(_) => {
                    removal.reused = true;
                    true
                }
                None => false,
            };
            if !out {
                fail(
                    trace,
                    I13,
                    after_event,
                    format!(
                        "node {node}'s life on root {} was removed, but the latest view shows \
                         {record:?}",
                        removal.root
                    ),
                );
            }
        }
    }

    /// I8's sweep half over the DELETEs the bucket served since the last
    /// observation.
    fn sweeps(&mut self, cluster: &MetaCluster, trace: &mut SimTrace, after_event: &str) {
        let Some(objects) = cluster.data.snapshot_objects.as_ref() else {
            return;
        };
        let events = objects.bucket.events_from(self.events_read);
        self.events_read = self.events_read.saturating_add(events.len());
        for event in events {
            let SnapshotStoreEvent::Delete {
                node_id,
                path,
                kind: DeleteKind::Swept { of },
                ..
            } = event
            else {
                continue;
            };
            let group = OwnedKey::parse(objects.bucket.prefix(), &path)
                .map(|owned| owned.owner().raft_group_id().0)
                .ok();
            if !self.removed.contains_key(&of) {
                fail(
                    trace,
                    I8,
                    after_event,
                    format!(
                        "node {node_id}'s sweep deleted {path} of node {of}, which the latest \
                         view does not show removed"
                    ),
                );
            }
            if let Some(group) = group {
                let voter = cluster
                    .latest_committed(group)
                    .is_some_and(|(_, configs, _)| {
                        configs.iter().any(|voters| voters.contains(&of))
                    });
                if voter {
                    fail(
                        trace,
                        I8,
                        after_event,
                        format!(
                            "node {node_id}'s sweep deleted {path} of node {of}, a voter of \
                             group {group}'s latest committed membership"
                        ),
                    );
                }
                let judged = self.judged.entry(of).or_default();
                let count = judged.sweeps.entry(group).or_insert(0);
                *count = count.saturating_add(1);
            }
        }
    }

    /// Every node id the checks saw removed, whatever life of it runs now.
    pub(super) fn ever_removed(&self) -> BTreeSet<u64> {
        self.removed.keys().copied().collect()
    }

    /// How many of the sweep's DELETEs of `node`'s objects I8's sweep half
    /// judged so far.
    pub(super) fn swept(&self, node: u64) -> u64 {
        self.judged.get(&node).map_or(0, |judged| {
            judged
                .sweeps
                .values()
                .fold(0, |swept, count| swept.saturating_add(*count))
        })
    }

    /// Notes that a wait found `node`'s object directory empty.
    pub(super) fn emptied(&mut self, node: u64) {
        self.judged.entry(node).or_default().emptied = true;
    }

    /// Notes that a wait found the removed `node` holding nothing open.
    pub(super) fn closed(&mut self, node: u64) {
        self.judged.entry(node).or_default().closed = true;
    }

    /// At a run's end, for each removed node: I13 read a view past its
    /// removal and a replica's leader, and found it holding nothing open,
    /// I8's sweep half judged a DELETE of its objects, and I5's removal
    /// half found its directory swept empty.
    pub(super) fn judged_the_removals(&self, trace: &mut SimTrace, after_event: &str) {
        for node in self.removed.keys() {
            let removed = format!("node {node} was removed");
            let Some(judged) = self.judged.get(node) else {
                fail(
                    trace,
                    I13,
                    after_event,
                    format!("{removed}, but no removal check judged it"),
                );
            };
            if judged.views == 0 || judged.leaders == 0 {
                fail(
                    trace,
                    I13,
                    after_event,
                    format!("{removed}, but I13 read no view or leader past its removal"),
                );
            }
            if !judged.closed {
                fail(
                    trace,
                    I13,
                    after_event,
                    format!("{removed}, but I13 never found it holding nothing open"),
                );
            }
            if judged.sweeps.is_empty() {
                fail(
                    trace,
                    I8,
                    after_event,
                    format!("{removed}, but I8 judged no sweep DELETE of its objects"),
                );
            }
            if !judged.emptied {
                fail(
                    trace,
                    I5,
                    after_event,
                    format!("{removed}, but I5 never found its directory swept empty"),
                );
            }
        }
    }
}

/// I13's role half for the removed `node`: every running node's view at
/// or past its removal shows it removed, without a role.
fn stays_without_role(
    cluster: &MetaCluster,
    node: u64,
    removal: &Removal,
    judged: &mut Judged,
    trace: &mut SimTrace,
    after_event: &str,
) {
    for viewer in cluster.running() {
        let view = cluster.view(viewer);
        let past = view
            .applied()
            .is_some_and(|applied| applied.get().index >= removal.applied);
        let Some(bootstrapped) = view.cluster().filter(|_| past) else {
            continue;
        };
        let id = NodeId::new(node);
        let Some(record) = bootstrapped.node(id) else {
            continue;
        };
        // A new life's record, and its roles, are the new life's.
        if !of_life(record, removal.root) {
            continue;
        }
        judged.views = judged.views.saturating_add(1);
        let lifecycle = Some(record.lifecycle);
        let roles: Vec<u32> = JOURNAL_GROUPS
            .iter()
            .copied()
            .filter(|group| bootstrapped.role(RaftGroupId(*group), id).is_some())
            .collect();
        if record.lifecycle.has_life() || !roles.is_empty() {
            fail(
                trace,
                I13,
                after_event,
                format!(
                    "node {viewer}'s view past node {node}'s removal shows it {lifecycle:?} with \
                     a role in groups {roles:?}"
                ),
            );
        }
    }
}

/// I13's leader half for the removed `node`: no running replica names it
/// its group's leader in a term above the group's terms at its removal.
fn leads_nothing(
    cluster: &MetaCluster,
    node: u64,
    removal: &Removal,
    judged: &mut Judged,
    trace: &mut SimTrace,
    after_event: &str,
) {
    // A new life of the id may lead: its leaderships are its own.
    if removal.reused {
        return;
    }
    for group in JOURNAL_GROUPS {
        let before = removal.terms.get(&group).copied().unwrap_or(0);
        for replica in cluster.running() {
            let Some(raft) = cluster.hosted_raft(replica, group) else {
                continue;
            };
            judged.leaders = judged.leaders.saturating_add(1);
            let metrics = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).clone();
            if metrics.current_leader == Some(node) && metrics.current_term > before {
                fail(
                    trace,
                    I13,
                    after_event,
                    format!(
                        "node {replica}'s replica of group {group} names the removed node \
                         {node} its leader in term {}, above term {before} at its removal",
                        metrics.current_term
                    ),
                );
            }
        }
    }
}
