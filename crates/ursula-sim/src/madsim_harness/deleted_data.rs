//! Deleted data leaves each replica within `raft.deleted_data_max_age`
//! (#669), on journal-backed replicas that run the production snapshot
//! driver ([`JournalCluster`] with [`DriverPlan`]).
//!
//! Invariant `deleted_data_leaves_within_bound` (scenario
//! `deleted-data-leaves-within-bound`): a stream whose records carry a
//! marker is snapshotted on every replica, then deleted or its bucket
//! purged. On each replica, from when it applied the removal, within the
//! bound: its current snapshot holds neither the stream nor the marker, no
//! entry its log retains holds the marker, and a read of the stream answers
//! not found. Seeds stop a follower across the removal, stop the leader
//! right after it, or cut a replica's power right after the snapshot that
//! covers the removal. A replica's time counts from when it applies the
//! removal, again after it restarts.
//!
//! A second scenario interleaves an operator's snapshot with a purge
//! through a covering snapshot that the leader defers while a slow
//! follower's replication reads the range: the operator's build starts only
//! once that purge landed.
//!
//! Negative controls switch off the defences each part of the bound needs:
//!
//! - [`Controls::FLOOR_OFF`]: a small removal no longer makes its group due,
//!   so the snapshot keeps the stream.
//! - [`Controls::PURGE_OFF`]: the snapshot drops the stream, but OpenRaft
//!   keeps `max_in_snapshot_log_to_keep` entries below it, the marker's
//!   among them.
//! - [`Controls::HOLD_OFF`]: the operator's build ends under the deferred
//!   purge and schedules a lower one, which OpenRaft's debug assertion
//!   reports.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use openraft::storage::RaftLogReader;
use ursula_config::WalFsync;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::NODES;
use super::journal_cluster::group_placement;
use super::journal_cluster::metrics;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::wait_leader;

pub(super) const BOUND_INVARIANT: &str = "deleted_data_leaves_within_bound";
#[cfg(test)]
pub(super) const HOLD_INVARIANT: &str = "operator_snapshot_waits_for_a_scrub_purge";

/// The group the marked stream lives in.
const GROUP: u32 = 0;
/// The bytes every record of the removed stream carries.
const MARKER: &[u8] = b"deleted-data-marker-669;";
/// The bucket of the removed stream, which a purge erases.
const BUCKET: &str = "erased";
/// The drivers' settings: a bound of a few seconds of virtual time, so the
/// floor is one second, and OpenRaft keeps 8 entries below a snapshot.
pub(super) const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(4),
    keep: 8,
    interval: Duration::from_millis(200),
};
#[cfg(test)]
/// How long the leader's appends to the slow follower take in the
/// operator-snapshot scenario.
const SLOW_FOLLOWER: Duration = Duration::from_secs(5);
/// How often the run observes the replicas.
const POLL: Duration = Duration::from_millis(50);

fn marked_stream() -> BucketStreamId {
    BucketStreamId::new(BUCKET, "marked")
}

/// Server defences a negative control turns off. Production runs use the
/// default, with every defence on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// A removal no snapshot covers never makes its group due on its age.
    floor_off: bool,
    /// The driver never purges a log through a covering snapshot.
    purge_off: bool,
    /// A purge through a covering snapshot neither waits for its group's
    /// builds nor holds them.
    hold_off: bool,
}

#[cfg(test)]
impl Controls {
    const NONE: Self = Self {
        floor_off: false,
        purge_off: false,
        hold_off: false,
    };
    pub(super) const FLOOR_OFF: Self = Self {
        floor_off: true,
        ..Self::NONE
    };
    pub(super) const PURGE_OFF: Self = Self {
        purge_off: true,
        ..Self::NONE
    };
    pub(super) const HOLD_OFF: Self = Self {
        hold_off: true,
        ..Self::NONE
    };
}

/// The defences [`Controls`] turned off for this simulation, and the
/// drivers its cluster runs, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_defences(controls);
        set_snapshot_drivers(Some(DRIVERS));
        ursula_stream::sim_controls::reset_fired();
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::default());
        set_snapshot_drivers(None);
    }
}

fn set_defences(controls: Controls) {
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::set_off;
    set_off(Defence::RemovalFloor, controls.floor_off);
    set_off(Defence::ScrubPurge, controls.purge_off);
    set_off(Defence::ScrubPurgeHold, controls.hold_off);
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// What the run does around the removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Nothing.
    None,
    /// A follower is stopped across the removal and comes back a second
    /// later: it applies the removal from the leader's log.
    FollowerDown,
    /// The leader stops right after it applied the removal and comes back
    /// a second and a half later: it applies the removal again as it replays
    /// its log.
    LeaderStopped,
    /// A replica loses power right after the snapshot that covers the
    /// removal, under `fsync = always`: it restores that snapshot and
    /// purges its log through it.
    PowerLossAfterSnapshot,
}

impl Fault {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::FollowerDown => "follower_down",
            Self::LeaderStopped => "leader_stopped",
            Self::PowerLossAfterSnapshot => "power_loss_after_snapshot",
        }
    }
}

/// The plan of one run.
#[derive(Debug)]
struct Plan {
    /// A bucket purge, else a deletion of the stream.
    purge: bool,
    fault: Fault,
    /// The marked records the stream holds.
    records: usize,
    /// Picks the replica a fault hits among the candidates.
    pick: u64,
}

impl Plan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6465_6c65_7465_6421);
        let fault = match seed % 4 {
            0 => Fault::None,
            1 => Fault::FollowerDown,
            2 => Fault::LeaderStopped,
            _ => Fault::PowerLossAfterSnapshot,
        };
        Self {
            purge: rng.next_bounded(2) == 0,
            fault,
            records: 1_usize.saturating_add(usize::try_from(rng.next_bounded(3)).expect("small")),
            pick: rng.next(),
        }
    }
}

/// Runs `operation` on the group's leader until it answers, while the
/// leadership moves.
macro_rules! on_leader {
    ($cluster:expr, $context:expr, |$engine:ident| $operation:expr) => {{
        let mut answered = None;
        for _ in 0..100 {
            let leader = $cluster.leader(GROUP).await;
            if let Some($engine) = $cluster.engines.get_mut(&(GROUP, leader))
                && let Ok(answer) = $operation.await
            {
                answered = Some((leader, answer));
                break;
            }
            madsim::time::sleep(POLL).await;
        }
        answered.unwrap_or_else(|| panic!("{}: the group's leader never answered", $context))
    }};
}

/// Creates the marked stream on the group's leader and appends `records`
/// marked records, then snapshots every replica once it applied them, so
/// that every current snapshot holds the stream before it is removed.
async fn write_marked_stream(cluster: &mut JournalCluster, records: usize, context: &str) {
    let placement = group_placement(GROUP);
    on_leader!(cluster, context, |engine| engine.create_stream(
        CreateStreamRequest::new(marked_stream(), "application/octet-stream"),
        placement,
        ColdWriteAdmission::default(),
    ));
    for record in 0..records {
        let mut payload = MARKER.to_vec();
        payload.extend_from_slice(format!("{record};").as_bytes());
        on_leader!(cluster, context, |engine| engine.append(
            AppendRequest::from_bytes(marked_stream(), payload.clone()),
            placement,
            ColdWriteAdmission::default(),
        ));
    }
    let leader = wait_leader(cluster, GROUP, context).await;
    let written = metrics(cluster, GROUP, leader)
        .last_applied
        .map_or(0, |log_id| log_id.index);
    for node_id in NODES {
        wait_applied(cluster, node_id, written, context).await;
        cluster
            .operator_snapshot(GROUP, node_id, Duration::from_secs(10))
            .await
            .unwrap_or_else(|stage| panic!("{context}: node {node_id}: {stage}"));
        assert!(
            snapshot_holds_marker(cluster, node_id).await.is_some(),
            "{context}: node {node_id}: the snapshot before the removal holds the stream"
        );
    }
}

/// Waits until `node_id`'s replica applied `index`.
async fn wait_applied(cluster: &JournalCluster, node_id: u64, index: u64, context: &str) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
    while applied(cluster, node_id) < Some(index) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {node_id} never applied {index}"
        );
        madsim::time::sleep(POLL).await;
    }
}

fn applied(cluster: &JournalCluster, node_id: u64) -> Option<u64> {
    cluster
        .engines
        .contains_key(&(GROUP, node_id))
        .then(|| metrics(cluster, GROUP, node_id).last_applied)
        .flatten()
        .map(|log_id| log_id.index)
}

fn holds_marker(bytes: &[u8]) -> bool {
    bytes.windows(MARKER.len()).any(|window| window == MARKER)
}

/// What of the removed stream `node_id`'s current snapshot holds, if any.
async fn snapshot_holds_marker(cluster: &JournalCluster, node_id: u64) -> Option<String> {
    let raft = cluster.engines[&(GROUP, node_id)].raft_handle();
    let snapshot = raft
        .get_snapshot()
        .await
        .unwrap_or_else(|error| panic!("node {node_id}: read the current snapshot: {error}"))?;
    let pointer = SnapshotPointer::decode(snapshot.snapshot.get_ref())
        .unwrap_or_else(|error| panic!("node {node_id}: decode the snapshot pointer: {error}"));
    let SnapshotLocation::Inline { bytes } = &pointer.location else {
        panic!("node {node_id}: a simulated snapshot is inline");
    };
    let group = ursula_raft::decode_group_snapshot(bytes)
        .unwrap_or_else(|error| panic!("node {node_id}: decode the snapshot: {error}"));
    group
        .stream_snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == marked_stream() || holds_marker(&entry.payload))
        .map(|entry| {
            format!(
                "snapshot {} holds stream {} with {} hot bytes",
                snapshot.meta.snapshot_id,
                entry.metadata.stream_id,
                entry.payload.len()
            )
        })
}

/// The first entry `node_id`'s log retains that holds the marker.
async fn log_holds_marker(cluster: &JournalCluster, node_id: u64) -> Option<String> {
    let mut store = cluster.wals[&node_id]
        .store(RaftGroupId(GROUP))
        .expect("a running replica holds its store");
    let entries = store
        .try_get_log_entries(..)
        .await
        .unwrap_or_else(|error| panic!("node {node_id}: read the log: {error}"));
    entries.iter().find_map(|entry| {
        let encoded = rmp_serde::to_vec(entry).expect("encode a log entry");
        holds_marker(&encoded).then(|| format!("log entry {} holds the marker", entry.log_id))
    })
}

/// What `node_id`'s replica shows of its snapshot work, for a failure.
fn replica_state(cluster: &JournalCluster, node_id: u64) -> String {
    let raft = metrics(cluster, GROUP, node_id);
    let published = raft.snapshot.map(|snapshot| snapshot.index);
    let log = cluster.node_registries[&node_id]
        .metrics_snapshot()
        .into_iter()
        .find(|group| group.raft_group_id == GROUP)
        .map(|group| group.log);
    let coordinator = cluster.coordinator(node_id);
    format!(
        "applied {:?}, snapshot {published:?}, purged {:?}, state {:?}, work {:?}, log {log:?}, \
         driver running {}, builds {}, builds in flight {:?}, other groups {:?}",
        raft.last_applied.map(|log_id| log_id.index),
        raft.purged.map(|log_id| log_id.index),
        raft.state,
        coordinator.work(GROUP, published),
        cluster.driver_running(node_id),
        coordinator.running_builds(),
        coordinator.builds_in_flight(),
        cluster.node_registries[&node_id]
            .metrics_snapshot()
            .into_iter()
            .filter(|group| group.raft_group_id != GROUP)
            .map(|group| (group.raft_group_id, group.snapshot, group.purged, group.log))
            .collect::<Vec<_>>(),
    )
}

/// Fails the invariant when `node_id` still holds the removed stream.
async fn check_replica(
    cluster: &JournalCluster,
    node_id: u64,
    trace: &mut SimTrace,
    context: &str,
) {
    let after = "deleted_data_bound_passed";
    if let Some(held) = snapshot_holds_marker(cluster, node_id).await {
        fail(
            trace,
            BOUND_INVARIANT,
            after,
            format!(
                "{context}: node {node_id}: {held} ({})",
                replica_state(cluster, node_id)
            ),
        );
    }
    if let Some(held) = log_holds_marker(cluster, node_id).await {
        fail(
            trace,
            BOUND_INVARIANT,
            after,
            format!(
                "{context}: node {node_id}: {held} ({})",
                replica_state(cluster, node_id)
            ),
        );
    }
    let read = cluster.engines[&(GROUP, node_id)]
        .sim_read_local_stream(
            ReadStreamRequest {
                stream_id: marked_stream(),
                offset: 0,
                max_len: 1 << 20,
                now_ms: 0,
                leader_only: false,
                read_index: None,
                follower: FollowerRead::Prefix,
            },
            group_placement(GROUP),
        )
        .await;
    use ursula_runtime::StreamErrorCode;
    match read {
        Err(error)
            if matches!(
                error.code(),
                Some(
                    StreamErrorCode::StreamNotFound
                        | StreamErrorCode::BucketNotFound
                        | StreamErrorCode::BucketErased
                )
            ) => {}
        other => fail(
            trace,
            BOUND_INVARIANT,
            after,
            format!("{context}: node {node_id}: a read of the removed stream answered {other:?}"),
        ),
    }
}

/// When each running replica applied the removal at `index`, as observed.
#[derive(Debug, Default)]
struct Applied {
    at: BTreeMap<u64, madsim::time::Instant>,
}

impl Applied {
    /// Records the replicas that applied `index` since the last look.
    fn observe(&mut self, cluster: &JournalCluster, index: u64) {
        let now = madsim::time::Instant::now();
        for node_id in NODES {
            if applied(cluster, node_id) >= Some(index) {
                self.at.entry(node_id).or_insert(now);
            }
        }
    }

    /// Forgets `node_id`, which stopped: it keeps its copies until it is
    /// back, and its time counts again from when it applies the removal.
    fn forget(&mut self, node_id: u64) {
        self.at.remove(&node_id);
    }

    /// Waits `duration`, observing all along.
    async fn wait(&mut self, cluster: &JournalCluster, index: u64, duration: Duration) {
        let until = madsim::time::Instant::now() + duration;
        loop {
            self.observe(cluster, index);
            if madsim::time::Instant::now() >= until {
                return;
            }
            madsim::time::sleep(POLL).await;
        }
    }
}

pub(super) async fn run_deleted_data_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = Plan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    let context = format!("seed {} {plan:?}", config.seed);
    let mut cluster = JournalCluster::start_with_fsync(
        &format!("deleted-data-{}", config.seed),
        WalFsync::Always,
    )
    .await;
    write_marked_stream(&mut cluster, plan.records, &context).await;
    let leader = wait_leader(&cluster, GROUP, &context).await;
    trace.push(SimEvent::LeaderElected { leader_id: leader });
    let followers = NODES
        .into_iter()
        .filter(|node_id| *node_id != leader)
        .collect::<Vec<_>>();
    let pick = |candidates: &[u64]| {
        let count = u64::try_from(candidates.len()).expect("node count fits u64");
        candidates[usize::try_from(plan.pick % count).expect("index fits usize")]
    };

    let down = (plan.fault == Fault::FollowerDown).then(|| pick(&followers));
    if let Some(node_id) = down {
        cluster.stop_node(node_id).await;
        trace.push(SimEvent::FaultApplied {
            phase: "follower_stopped_before_removal".to_owned(),
        });
    }
    let placement = group_placement(GROUP);
    let (remover, ()) = if plan.purge {
        on_leader!(cluster, &context, |engine| async {
            engine
                .purge_bucket(BUCKET.to_owned(), placement)
                .await
                .map(drop)
        })
    } else {
        on_leader!(cluster, &context, |engine| async {
            engine
                .delete_stream(
                    DeleteStreamRequest {
                        stream_id: marked_stream(),
                        if_incarnation: None,
                    },
                    placement,
                )
                .await
                .map(drop)
        })
    };
    let removal_index = applied(&cluster, remover).expect("the leader applied the removal");
    let mut seen = Applied::default();
    seen.observe(&cluster, removal_index);

    match plan.fault {
        Fault::None => {}
        Fault::FollowerDown => {
            let node_id = down.expect("a follower is down");
            seen.wait(&cluster, removal_index, Duration::from_secs(1))
                .await;
            cluster.start_node(node_id).await;
            trace.push(SimEvent::FaultApplied {
                phase: "follower_restarted_after_removal".to_owned(),
            });
        }
        Fault::LeaderStopped => {
            cluster.stop_node(remover).await;
            seen.forget(remover);
            trace.push(SimEvent::FaultApplied {
                phase: "leader_stopped_after_removal".to_owned(),
            });
            seen.wait(&cluster, removal_index, Duration::from_millis(1_500))
                .await;
            cluster.start_node(remover).await;
        }
        Fault::PowerLossAfterSnapshot => {
            // The first replica whose snapshot covers the removal.
            let deadline = madsim::time::Instant::now() + DRIVERS.max_age;
            let covered = loop {
                seen.observe(&cluster, removal_index);
                let covered = NODES
                    .into_iter()
                    .filter(|node_id| {
                        metrics(&cluster, GROUP, *node_id)
                            .snapshot
                            .is_some_and(|snapshot| snapshot.index >= removal_index)
                    })
                    .collect::<Vec<_>>();
                if !covered.is_empty() {
                    break pick(&covered);
                }
                if madsim::time::Instant::now() >= deadline {
                    fail(
                        &mut trace,
                        BOUND_INVARIANT,
                        "removal_applied",
                        format!("{context}: no replica snapshotted the removal within the bound"),
                    );
                }
                madsim::time::sleep(POLL).await;
            };
            cluster.stop_node(covered).await;
            seen.forget(covered);
            cluster.wals[&covered].power_loss_losing_unsynced().await;
            trace.push(SimEvent::FaultApplied {
                phase: "power_lost_after_covering_snapshot".to_owned(),
            });
            cluster.start_node(covered).await;
        }
    }

    // Every replica is checked once the bound passed since it applied the
    // removal.
    let mut checked = BTreeSet::new();
    let deadline = madsim::time::Instant::now() + DRIVERS.max_age.saturating_mul(4);
    while checked.len() < NODES.len() {
        seen.observe(&cluster, removal_index);
        let now = madsim::time::Instant::now();
        let due = seen
            .at
            .iter()
            .filter(|(node_id, at)| !checked.contains(*node_id) && now >= **at + DRIVERS.max_age)
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        for node_id in due {
            check_replica(&cluster, node_id, &mut trace, &context).await;
            checked.insert(node_id);
        }
        assert!(
            now < deadline,
            "{context}: replicas {:?} never applied the removal at {removal_index}",
            NODES
                .iter()
                .filter(|node_id| !seen.at.contains_key(*node_id))
                .collect::<Vec<_>>()
        );
        madsim::time::sleep(POLL).await;
    }
    trace.push(SimEvent::DeletedDataLeft {
        removal_index,
        purge: plan.purge,
        fault: plan.fault.name().to_owned(),
        replicas: u64::try_from(checked.len()).expect("replica count fits u64"),
    });
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: leader,
        target_node_id: down,
        appended_log_index: removal_index,
        trace,
    }
}

#[cfg(test)]
/// An operator's snapshot interleaved with a purge through a covering
/// snapshot that the leader defers: the leader's appends to one follower
/// take [`SLOW_FOLLOWER`], so its replication of the removal is in flight
/// when the driver purges the log through the snapshot that covers it. A
/// few writes later an operator snapshots the leader. Its build must start
/// only once that purge landed: a build that ends while the purge is
/// deferred schedules OpenRaft's own purge below it, which OpenRaft asserts
/// never happens.
pub(super) async fn run_operator_snapshot_under_purge_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    let context = format!("seed {} operator snapshot under a purge", config.seed);
    let mut cluster = JournalCluster::start_with_fsync(
        &format!("operator-snapshot-under-purge-{}", config.seed),
        WalFsync::Always,
    )
    .await;
    write_marked_stream(&mut cluster, 2, &context).await;
    let leader = wait_leader(&cluster, GROUP, &context).await;
    trace.push(SimEvent::LeaderElected { leader_id: leader });
    let slow = NODES
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("a follower");
    cluster
        .policy
        .set_entries_delay(leader, slow, Some(SLOW_FOLLOWER));
    let placement = group_placement(GROUP);
    let (remover, ()) = on_leader!(cluster, &context, |engine| async {
        engine
            .delete_stream(
                DeleteStreamRequest {
                    stream_id: marked_stream(),
                    if_incarnation: None,
                },
                placement,
            )
            .await
            .map(drop)
    });
    assert_eq!(remover, leader, "{context}: the leader stays");
    let removal_index = applied(&cluster, leader).expect("the leader applied the removal");
    // The driver snapshots the removal once it is a quarter of the bound
    // old, and purges the log through it a tick later.
    let deadline = madsim::time::Instant::now() + DRIVERS.max_age;
    while metrics(&cluster, GROUP, leader)
        .snapshot
        .is_none_or(|snapshot| snapshot.index < removal_index)
    {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the leader never snapshotted the removal"
        );
        madsim::time::sleep(POLL).await;
    }
    madsim::time::sleep(DRIVERS.interval.saturating_mul(2)).await;
    let purged = |cluster: &JournalCluster| {
        metrics(cluster, GROUP, leader)
            .purged
            .map_or(0, |purged| purged.index)
    };
    assert!(
        purged(&cluster) < removal_index,
        "{context}: the leader defers the purge while the slow follower's replication is in flight"
    );
    trace.push(SimEvent::FaultApplied {
        phase: "purge_deferred".to_owned(),
    });
    // A few writes, fewer than OpenRaft keeps below a snapshot, then the
    // operator's snapshot of the leader.
    for record in 0..3 {
        let payload = format!("after-{record};").into_bytes();
        on_leader!(cluster, &context, |engine| engine.append(
            AppendRequest::from_bytes(super::journal_cluster::group_stream(GROUP), payload.clone()),
            placement,
            ColdWriteAdmission::default(),
        ));
    }
    let snapshot = cluster
        .operator_snapshot(GROUP, leader, Duration::from_secs(30))
        .await
        .unwrap_or_else(|stage| panic!("{context}: {stage}"));
    let purged_at_build = purged(&cluster);
    if purged_at_build < removal_index {
        fail(
            &mut trace,
            HOLD_INVARIANT,
            "operator_snapshot_built",
            format!(
                "{context}: the operator's snapshot at {snapshot} was built before the purge \
                 through {removal_index} landed (purged {purged_at_build})"
            ),
        );
    }
    cluster.policy.set_entries_delay(leader, slow, None);
    trace.push(SimEvent::OperatorSnapshotHeldForPurge {
        purged: purged_at_build,
        snapshot,
    });
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: leader,
        target_node_id: Some(slow),
        appended_log_index: removal_index,
        trace,
    }
}

#[cfg(test)]
mod tests {
    use super::BOUND_INVARIANT;
    use super::Controls;
    use super::HOLD_INVARIANT;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::DeletedDataLeavesWithinBound;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug()))
    }

    /// Every fault of the family keeps the bound, the same way twice per
    /// seed.
    #[test]
    fn deleted_data_leaves_within_bound_replays() {
        let _guard = sim_test_guard();
        for seed in 520..=523 {
            let first = ThreeNodeRaftSim::run_deleted_data_with_controls_report(
                config(seed),
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_deleted_data_with_controls_report(
                config(seed),
                Controls::default(),
            );
            assert_eq!(first, second, "seed {seed}");
            assert!(
                first
                    .outcome
                    .trace
                    .events
                    .iter()
                    .any(|event| matches!(event, SimEvent::DeletedDataLeft { replicas: 3, .. })),
                "seed {seed}: {:?}",
                first.outcome.trace.events.last()
            );
        }
    }

    /// Runs `seed` under `controls`, which must fail `invariant`, and
    /// returns the failure's message.
    fn assert_fails(seed: u64, controls: Controls, invariant: &str) -> String {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_deleted_data_with_controls_report(config(seed), controls)
        });
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{controls:?} left seed {seed} passing");
        let trace = SimTrace::last_recorded();
        assert!(
            invariant_failed(&trace, invariant),
            "{controls:?} did not fail `{invariant}` on seed {seed}: {:?}",
            trace.events.last()
        );
        trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::InvariantFailed { message, .. } => Some(message.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Without the removal floor, a small removal on a quiet group never
    /// makes it snapshot: the snapshot keeps the removed stream.
    #[test]
    fn negative_control_a_removal_without_its_floor_stays_in_the_snapshot() {
        let _guard = sim_test_guard();
        let message = assert_fails(520, Controls::FLOOR_OFF, BOUND_INVARIANT);
        assert!(message.contains("holds stream"), "{message}");
    }

    /// Without the purge through the covering snapshot, the snapshot drops
    /// the stream but OpenRaft keeps the entries just below it, the marked
    /// records among them.
    #[test]
    fn negative_control_a_log_kept_below_the_snapshot_keeps_the_records() {
        let _guard = sim_test_guard();
        let message = assert_fails(520, Controls::PURGE_OFF, BOUND_INVARIANT);
        assert!(message.contains("holds the marker"), "{message}");
    }

    /// #669 review: an operator's snapshot that interleaves with a purge
    /// through a covering snapshot the leader defers starts only once that
    /// purge landed, the same way twice.
    #[test]
    fn scrub_purge_holds_an_operator_snapshot() {
        let _guard = sim_test_guard();
        let first = ThreeNodeRaftSim::run_operator_snapshot_under_purge_report(
            config(524),
            Controls::default(),
        );
        let second = ThreeNodeRaftSim::run_operator_snapshot_under_purge_report(
            config(524),
            Controls::default(),
        );
        assert_eq!(first, second);
        assert!(
            first
                .outcome
                .trace
                .events
                .iter()
                .any(|event| matches!(event, SimEvent::OperatorSnapshotHeldForPurge { .. })),
            "{:?}",
            first.outcome.trace.events.last()
        );
    }

    /// Without the hold, the operator's build ends while the leader still
    /// defers the purge through the covering snapshot, and schedules
    /// OpenRaft's own purge below it: OpenRaft's debug assertion that the
    /// purge target never goes back reports it in this build. A release
    /// build would lower the target.
    #[test]
    fn negative_control_an_operator_snapshot_under_a_deferred_purge_trips_openraft() {
        let _guard = sim_test_guard();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_operator_snapshot_under_purge_report(
                config(524),
                Controls::HOLD_OFF,
            )
        });
        std::panic::set_hook(previous_hook);
        let payload = result.expect_err("the run panicked");
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
            })
            .unwrap_or_default();
        assert!(
            message.contains("purge_upto") || message.contains(HOLD_INVARIANT),
            "{message}"
        );
    }
}
