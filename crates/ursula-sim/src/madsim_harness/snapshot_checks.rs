//! Checks of snapshot object ownership (#687) over a [`JournalCluster`]
//! whose nodes run the in-memory snapshot store
//! ([`super::journal_cluster::set_snapshot_store`]), shared by the scenarios
//! that give their nodes one.
//!
//! - [`SnapshotChecks::step`], after every action of a scenario and every
//!   [`STEP`] while it waits: I1 (`snapshot_named_keys_exist`) over what
//!   every node's snapshot records name, a stopped node's included, I2
//!   (`snapshot_no_foreign_delete`) over every DELETE the bucket served (the
//!   bucket panics on a foreign DELETE in the deleting task first, so this
//!   check records `invariant_failed` only for a recording bucket), and
//!   I3 (`snapshot_reads_found`) over the reads the bucket logged since the
//!   last step: a GET of a node's own object is a restore, the only reader
//!   of its own objects, and a GET inside a send's window is a read of an
//!   object the sender holds. It also notes when each key stopped being
//!   named, for I4.
//! - [`SnapshotChecks::bound`]: I4 (`snapshot_superseded_without_marker`),
//!   no object that no node names holds the marker past the bound.
//! - [`SnapshotChecks::quiesce`], the only route to I5
//!   (`snapshot_quiet_prefixes`): the faults off, every replica caught up
//!   with no build in flight, every late PUT landed, then one install
//!   timeout and three driver intervals for the passes the nodes owe. It
//!   checks I1 and I2 first, then I5, and puts the faults back.
//!
//! Each check reads the records and the bucket with no await between them,
//! so it sees one state of the simulation. A failed check records
//! `invariant_failed` with the violation and panics.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_runtime::SnapshotPrefix;
use ursula_runtime::snapshot_harness::CommittedRead;
use ursula_runtime::snapshot_harness::CommittedReadPurpose;
use ursula_runtime::snapshot_harness::NodeNames;
use ursula_runtime::snapshot_harness::ReadOutcome;
use ursula_runtime::snapshot_harness::SnapshotInvariantViolation;
use ursula_runtime::snapshot_harness::SnapshotStoreEvent;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::check_committed_reads_found;
use ursula_runtime::snapshot_harness::check_named_keys_exist;
use ursula_runtime::snapshot_harness::check_no_foreign_delete;
use ursula_runtime::snapshot_harness::check_quiet_prefixes;
use ursula_runtime::snapshot_harness::check_superseded_without_marker;
use ursula_runtime::snapshot_harness::contains_marker;
use ursula_runtime::snapshot_harness::is_owned_by;

use super::SimEvent;
use super::SimTrace;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::leader_by_state;
use super::journal_cluster::metrics;

/// I1: every key a node's snapshot records name exists.
pub(super) const NAMED_KEYS_EXIST: &str = "snapshot_named_keys_exist";
/// I2: no node deletes outside its own directory.
pub(super) const NO_FOREIGN_DELETE: &str = "snapshot_no_foreign_delete";
/// I3: no restore and no read inside a send's RPC sees `NotFound`.
pub(super) const READS_FOUND: &str = "snapshot_reads_found";
/// I4: past the bound, no object that no node names holds the marker.
pub(super) const SUPERSEDED_WITHOUT_MARKER: &str = "snapshot_superseded_without_marker";
/// I5: once quiet, each node's directory holds only the keys it names.
pub(super) const QUIET_PREFIXES: &str = "snapshot_quiet_prefixes";

/// How often a check runs while a scenario waits.
pub(super) const STEP: Duration = Duration::from_millis(25);
/// How long a quiet point waits for the replicas to catch up.
const CATCH_UP: Duration = Duration::from_secs(10);

/// Records `invariant` failed after `after_event` with `violation`, and
/// panics.
pub(super) fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

fn check(
    trace: &mut SimTrace,
    invariant: &str,
    after_event: &str,
    checked: Result<(), SnapshotInvariantViolation>,
) {
    if let Err(violation) = checked {
        fail(trace, invariant, after_event, violation.to_string());
    }
}

/// The read `event` logged that I3 covers, if it is one.
fn covered_read(prefix: &SnapshotPrefix, event: &SnapshotStoreEvent) -> Option<CommittedRead> {
    match event {
        // Only a restore reads a node's own objects.
        SnapshotStoreEvent::Get {
            node_id,
            path,
            found,
        } if is_owned_by(prefix, *node_id, path) => Some(CommittedRead {
            node_id: *node_id,
            path: path.clone(),
            purpose: CommittedReadPurpose::Restore,
            outcome: if *found {
                ReadOutcome::Found
            } else {
                ReadOutcome::NotFound
            },
        }),
        SnapshotStoreEvent::SentObjectMissing { node_id, path } => Some(CommittedRead {
            node_id: *node_id,
            path: path.clone(),
            purpose: CommittedReadPurpose::SentObject,
            outcome: ReadOutcome::NotFound,
        }),
        _ => None,
    }
}

/// What the checks of one run remember between steps.
#[derive(Debug, Default)]
pub(super) struct SnapshotChecks {
    /// The keys some node named at the last step.
    named: BTreeSet<String>,
    /// When each key that no node names any more stopped being named.
    superseded: BTreeMap<String, madsim::time::Instant>,
    /// How many of the bucket's events I3 read.
    events_read: usize,
}

impl SnapshotChecks {
    /// I1, I2 and I3 now, after `after_event`.
    pub(super) fn step(
        &mut self,
        cluster: &JournalCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let names = cluster.snapshot_names();
        let bucket = cluster.snapshot_bucket();
        let view = bucket.key_view();
        check(
            trace,
            NAMED_KEYS_EXIST,
            after_event,
            check_named_keys_exist(&view, &names),
        );
        check(
            trace,
            NO_FOREIGN_DELETE,
            after_event,
            check_no_foreign_delete(&view),
        );
        let events = bucket.events_from(self.events_read);
        self.events_read = self.events_read.saturating_add(events.len());
        let reads: Vec<CommittedRead> = events
            .iter()
            .filter_map(|event| covered_read(&view.prefix, event))
            .collect();
        check(
            trace,
            READS_FOUND,
            after_event,
            check_committed_reads_found(&reads),
        );
        self.note_superseded(&names);
    }

    fn note_superseded(&mut self, names: &[NodeNames]) {
        let now = madsim::time::Instant::now();
        let named: BTreeSet<String> = names
            .iter()
            .flat_map(|node| node.named.iter().cloned())
            .collect();
        for key in self.named.difference(&named) {
            self.superseded.entry(key.clone()).or_insert(now);
        }
        for key in &named {
            self.superseded.remove(key);
        }
        self.named = named;
    }

    /// Waits `duration`, stepping every [`STEP`].
    pub(super) async fn wait(
        &mut self,
        cluster: &JournalCluster,
        trace: &mut SimTrace,
        after_event: &str,
        duration: Duration,
    ) {
        let deadline = madsim::time::Instant::now() + duration;
        while madsim::time::Instant::now() < deadline {
            madsim::time::sleep(STEP).await;
            self.step(cluster, trace, after_event);
        }
    }

    /// Waits until every running replica applied its group leader's last
    /// log index and no node has a snapshot build in flight, stepping.
    pub(super) async fn wait_caught_up(
        &mut self,
        cluster: &JournalCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let deadline = madsim::time::Instant::now() + CATCH_UP;
        while !caught_up(cluster) {
            assert!(
                madsim::time::Instant::now() < deadline,
                "after `{after_event}`: the replicas never caught up"
            );
            madsim::time::sleep(STEP).await;
            self.step(cluster, trace, after_event);
        }
        self.step(cluster, trace, after_event);
    }

    /// I4 now: no object that no node names holds `marker` longer than
    /// `bound` after it landed or stopped being named.
    pub(super) fn bound(
        &mut self,
        cluster: &JournalCluster,
        trace: &mut SimTrace,
        after_event: &str,
        marker: &[u8],
        bound: Duration,
    ) {
        self.step(cluster, trace, after_event);
        let names = cluster.snapshot_names();
        let view = cluster.snapshot_bucket().view();
        let now = madsim::time::Instant::now();
        let superseded: BTreeMap<String, Duration> = self
            .superseded
            .iter()
            .map(|(key, since)| (key.clone(), now.saturating_duration_since(*since)))
            .collect();
        check(
            trace,
            SUPERSEDED_WITHOUT_MARKER,
            after_event,
            check_superseded_without_marker(&view, &names, &superseded, bound, |bytes| {
                contains_marker(bytes, marker)
            }),
        );
    }

    /// A quiet point: the faults off, every replica caught up, every late
    /// PUT landed, and an install timeout and three driver intervals for the
    /// passes the nodes owe. Then I1 and I2, then I5, and the faults back.
    pub(super) async fn quiesce(
        &mut self,
        cluster: &JournalCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let bucket = cluster.snapshot_bucket();
        let faults = bucket.faults();
        bucket.set_faults(SnapshotStoreFaults::none());
        self.wait_caught_up(cluster, trace, after_event).await;
        bucket.wait_for_late_puts().await;
        self.step(cluster, trace, after_event);
        let interval = cluster
            .drivers
            .map_or(Duration::ZERO, |drivers| drivers.interval);
        let settle = cluster
            .snapshot_install_timeout()
            .saturating_add(interval.saturating_mul(3));
        self.wait(cluster, trace, after_event, settle).await;
        self.step(cluster, trace, after_event);
        let names = cluster.snapshot_names();
        let view = bucket.key_view();
        check(
            trace,
            QUIET_PREFIXES,
            after_event,
            check_quiet_prefixes(&view, &names),
        );
        bucket.set_faults(faults);
    }
}

/// Whether every running replica applied its group leader's last log index
/// and no node has a snapshot build in flight.
fn caught_up(cluster: &JournalCluster) -> bool {
    let groups = JOURNAL_GROUPS.iter().all(|group| {
        let Some(leader) = leader_by_state(cluster, *group) else {
            return false;
        };
        let last = metrics(cluster, *group, leader).last_log_index;
        cluster
            .engines
            .keys()
            .filter(|(engine_group, _)| engine_group == group)
            .all(|(_, node_id)| {
                metrics(cluster, *group, *node_id)
                    .last_applied
                    .map(|applied| applied.index)
                    >= last
            })
    });
    groups
        && cluster
            .nodes()
            .filter(|node_id| cluster.engines.keys().any(|(_, node)| node == node_id))
            .all(|node_id| cluster.coordinator(node_id).builds_in_flight().is_empty())
}
