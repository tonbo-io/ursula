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
//! not found. (a) No file under its WAL root, a journal segment or another,
//! holds the marker within the bound of when it first applied the removal,
//! or, when it applied it again as it replayed its log after a restart, within
//! half the bound and a second of that: a removal applied again is due at
//! once, and the purge after it frees the segments a quarter of the bound
//! later. So a replica that restarts soon after the removal keeps the
//! bound, and only one down for most of it gets the restart's margin. Seeds stop a
//! follower across the removal, stop the leader right after it, or cut a
//! replica's power right after the snapshot that covers the removal. A
//! replica's time counts from when it applies the removal, again after it
//! restarts.
//!
//! The `deleted-data-after-expiry` seeds remove the stream by its TTL
//! instead: it expires, nothing reads it, and the group either writes
//! nothing more or takes plain appends to another stream, which never sweep.
//! The run plays the tidy worker's role, which cannot run under madsim: every
//! [`ursula_runtime::expiry_sweep::interval`] of the bound, each replica's
//! tidy pass sweeps at the run's virtual wall clock, and only the leader's
//! proposes. A replica's time counts from the first look at which its local
//! read answers not found, again after it restarts. Every replica keeps the
//! bound within the sweep's interval, its clock grace and the bound of the
//! expiry. Seeds stop a follower across the expiry, stop the leader just
//! before it, or cut a replica's power right after its covering snapshot.
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
//! - [`Controls::RESTORE_OFF`]: a replica that restarts between the
//!   snapshot that dropped the stream and the purge through it does not
//!   purge through the snapshot it restores, so its log keeps the records.
//!   With the purge on ([`Controls::CRASH_BEFORE_PURGE`]) the bound holds.
//! - [`Controls::WAL_AGE_OFF`]: the log drops the records, but a journal
//!   segment that held their dead frames is freed only once the journal
//!   outgrows its budget, so a small journal keeps them in its files (a).
//! - [`Controls::SWEEP_OFF`]: no leader sweeps, so an expired stream that
//!   nothing reads stays in the group, quiet or taking plain appends, and in
//!   its snapshot.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use openraft::storage::RaftLogReader;
use ursula_config::WalFsync;
use ursula_raft::JournalTuning;
use ursula_raft::wal::diagnostics::JournalDisk;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::TidyStreamsRequest;
use ursula_runtime::expiry_sweep;
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
use super::journal_cluster::group_stream;
use super::journal_cluster::leader_by_state;
use super::journal_cluster::metrics;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::wait_leader;

pub(super) const BOUND_INVARIANT: &str = "deleted_data_leaves_within_bound";
#[cfg(test)]
pub(super) const HOLD_INVARIANT: &str = "operator_snapshot_waits_for_a_scrub_purge";

/// The group the marked stream lives in.
const GROUP: u32 = 0;
/// The other group, which writes nothing after the cluster starts but in
/// the `deleted-data-across-segments` families.
const QUIET_GROUP: u32 = 1;
/// The bytes every record of the removed stream carries.
const MARKER: &[u8] = b"deleted-data-marker-669;";
/// The bucket of the removed stream, which a purge erases.
const BUCKET: &str = "erased";
/// The drivers' settings with a short bound: four seconds of virtual time,
/// so the floor is one second, and OpenRaft keeps 8 entries below a
/// snapshot. Short enough that a purge the leader defers for a slow
/// follower is still pending when the operator snapshots.
pub(super) const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(4),
    keep: 8,
    interval: Duration::from_millis(200),
};
/// The drivers' settings the bound is checked with: twelve seconds, so the
/// floor is three. The bound covers a pass of the driver too, which may wait
/// [`ursula_raft::SNAPSHOT_HANDOFF_WAIT`] for a trigger another group's
/// build does not claim, and a run fits the simulation's 30 s of virtual
/// time.
const BOUND_DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(12),
    ..DRIVERS
};
#[cfg(test)]
/// How long the leader's appends to the slow follower take in the
/// operator-snapshot scenario.
const SLOW_FOLLOWER: Duration = Duration::from_secs(5);
/// How long a stopped follower stays down after the removal applied.
const FOLLOWER_DOWN_AFTER_REMOVAL: Duration = Duration::from_secs(1);
/// How long the leader stays down after it applied the removal: longer
/// than the bound less a quarter of it.
const LEADER_DOWN: Duration = Duration::from_secs(10);
/// How long past half the bound after its restart a replica's files may
/// still hold the marker: the replayed removal's snapshot and purge, and the
/// driver's passes that free the segments (invariant (a)).
const RESTART_FILES_SLACK: Duration = Duration::from_secs(1);
/// How often the run observes the replicas.
const POLL: Duration = Duration::from_millis(50);

/// Seeds of the `deleted-data-across-segments` families, whose journals
/// run segments of 4, 16 or 64 KiB and whose quiet group holds half a segment
/// of entries before the removal: freeing the removed records copies across
/// several segments and reports the quiet group lagging (#669).
const ACROSS_SEGMENTS: [std::ops::RangeInclusive<u64>; 2] = [528..=531, 630..=649];

/// Seeds of the `deleted-data-after-expiry` families: the marked stream
/// expires, and its group's leader sweeps it in the background (#669).
const AFTER_EXPIRY: [std::ops::RangeInclusive<u64>; 2] = [532..=539, 650..=679];
/// How long after the cluster is up the marked stream expires: past its
/// writes and the snapshots of every replica.
const EXPIRES_AFTER: Duration = Duration::from_secs(3);
/// How long before the expiry the leader stops in a `leader_stopped` run.
const STOP_BEFORE_EXPIRY: Duration = Duration::from_millis(200);
/// How long that leader stays down.
const EXPIRY_LEADER_DOWN: Duration = Duration::from_secs(4);
/// How often an `appends` run appends to the group's other stream.
const TRAFFIC_EVERY: Duration = Duration::from_millis(100);
/// The run's virtual wall clock at its start: any time past the epoch.
const SIM_WALL_START_MS: u64 = 1_700_000_000_000;
/// Past the clock grace, how late after the sweep's interval a replica may
/// drop the expired stream: the sweep's own commands, and a new leader's
/// election after a `leader_stopped` fault.
const EXPIRY_SLACK: Duration = Duration::from_millis(500);

/// The journals of a run of `seed`, and how many entries the quiet group
/// writes before the removal.
#[derive(Debug, Clone, Copy)]
struct Journals {
    tuning: JournalTuning,
    quiet_entries: usize,
}

impl Journals {
    fn for_seed(seed: u64) -> Self {
        let mut tuning = JournalTuning::new(WalFsync::Always);
        if !ACROSS_SEGMENTS.iter().any(|seeds| seeds.contains(&seed)) {
            return Self {
                tuning,
                quiet_entries: 0,
            };
        }
        tuning.segment_bytes = match seed % 3 {
            0 => 4 * 1024,
            1 => 16 * 1024,
            _ => 64 * 1024,
        };
        Self {
            tuning,
            // Half a segment: a quiet entry weighs about 150 bytes in the
            // journal.
            quiet_entries: usize::try_from(tuning.segment_bytes / 300).expect("a small count"),
        }
    }
}

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
    /// A replica that restores its snapshot at startup does not purge its
    /// log through it.
    restore_off: bool,
    /// The drivers purge nothing until the replica that lost power is back,
    /// so it crashes between the snapshot that dropped the stream and the
    /// purge through it.
    purge_after_restart: bool,
    /// A journal segment that held dead frames is never sealed or freed for
    /// their age.
    wal_age_off: bool,
    /// No leader sweeps expired streams in the background.
    sweep_off: bool,
}

#[cfg(test)]
impl Controls {
    const NONE: Self = Self {
        floor_off: false,
        purge_off: false,
        hold_off: false,
        restore_off: false,
        purge_after_restart: false,
        wal_age_off: false,
        sweep_off: false,
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
    /// Every defence on, and the crash between the covering snapshot and
    /// the purge through it.
    pub(super) const CRASH_BEFORE_PURGE: Self = Self {
        purge_after_restart: true,
        ..Self::NONE
    };
    /// The same crash without the purge through the restored snapshot.
    pub(super) const RESTORE_OFF: Self = Self {
        restore_off: true,
        purge_after_restart: true,
        ..Self::NONE
    };
    pub(super) const WAL_AGE_OFF: Self = Self {
        wal_age_off: true,
        ..Self::NONE
    };
    pub(super) const SWEEP_OFF: Self = Self {
        sweep_off: true,
        ..Self::NONE
    };
}

/// The defences [`Controls`] turned off for this simulation, and the
/// drivers its cluster runs, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls, drivers: DriverPlan) -> Self {
        set_defences(controls);
        set_snapshot_drivers(Some(drivers));
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
    set_off(Defence::RestorePurge, controls.restore_off);
    set_off(Defence::WalDirtyAge, controls.wal_age_off);
    set_off(Defence::ExpirySweep, controls.sweep_off);
    if controls.purge_after_restart {
        set_off(Defence::ScrubPurge, true);
    }
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

/// How the marked stream leaves its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Removal {
    /// A deletion of the stream.
    Deletion,
    /// A purge of its bucket.
    BucketPurge,
    /// Its TTL runs out and the leader's background sweep removes it.
    Expiry(Traffic),
}

/// What else the marked stream's group writes in an expiry run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Traffic {
    /// Nothing.
    Quiet,
    /// A plain append to the group's other stream every [`TRAFFIC_EVERY`],
    /// which never sweeps.
    Appends,
}

impl Traffic {
    fn name(self) -> &'static str {
        match self {
            Self::Quiet => "quiet",
            Self::Appends => "appends",
        }
    }
}

/// The plan of one run.
#[derive(Debug)]
struct Plan {
    removal: Removal,
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
        let purge = rng.next_bounded(2) == 0;
        let removal = if AFTER_EXPIRY.iter().any(|seeds| seeds.contains(&seed)) {
            Removal::Expiry(if (seed / 4) % 2 == 1 {
                Traffic::Quiet
            } else {
                Traffic::Appends
            })
        } else if purge {
            Removal::BucketPurge
        } else {
            Removal::Deletion
        };
        Self {
            removal,
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

/// Creates the marked stream on the group's leader, expiring at
/// `expires_at_ms` when set, and appends `records` marked records, then
/// snapshots every replica once it applied them, so that every current
/// snapshot holds the stream before it is removed.
async fn write_marked_stream(
    cluster: &mut JournalCluster,
    records: usize,
    expires_at_ms: Option<u64>,
    context: &str,
) {
    let placement = group_placement(GROUP);
    let mut create = CreateStreamRequest::new(marked_stream(), "application/octet-stream");
    create.stream_expires_at_ms = expires_at_ms;
    on_leader!(cluster, context, |engine| engine.create_stream(
        create.clone(),
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

/// The highest index any running replica applied.
fn applied_anywhere(cluster: &JournalCluster) -> u64 {
    NODES
        .into_iter()
        .filter_map(|node_id| applied(cluster, node_id))
        .max()
        .unwrap_or(0)
}

/// The log index of the removal `remover` answered, nothing else being
/// written: the first index past `before` its metrics show applied. Its
/// metrics follow the apply a moment after the answer.
async fn removal_applied(
    cluster: &JournalCluster,
    remover: u64,
    before: u64,
    context: &str,
) -> u64 {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(index) = applied(cluster, remover).filter(|index| *index > before) {
            return index;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {remover} never shows the removal applied"
        );
        madsim::time::sleep(Duration::from_millis(5)).await;
    }
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

/// The first file under `node_id`'s WAL root that holds the marker: a
/// journal segment, a snapshot file or any other.
fn wal_file_holds_marker(cluster: &JournalCluster, node_id: u64) -> Option<String> {
    let mut dirs = vec![cluster.wals[&node_id].root().to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries = SimDisk::read_dir(&dir)
            .unwrap_or_else(|error| panic!("node {node_id}: list {}: {error}", dir.display()));
        for path in entries {
            match SimDisk::read(&path) {
                Ok(bytes) if holds_marker(&bytes) => {
                    return Some(format!("file {} holds the marker", path.display()));
                }
                Ok(_) => {}
                // A directory.
                Err(_not_a_file) => dirs.push(path),
            }
        }
    }
    None
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

/// How the run sees that a replica removed the stream.
#[derive(Debug, Clone, Copy)]
enum Detect {
    /// It applied the removal at this log index.
    Applied(u64),
    /// Its local read of the stream answers not found: the stream expired
    /// and a sweep the run cannot index in advance removed it.
    Dropped,
}

/// When each running replica removed the stream, as observed: first, and
/// last, which a restart resets as the replica removes it again.
#[derive(Debug)]
struct Applied {
    detect: Detect,
    at: BTreeMap<u64, madsim::time::Instant>,
    last: BTreeMap<u64, madsim::time::Instant>,
}

impl Applied {
    fn new(detect: Detect) -> Self {
        Self {
            detect,
            at: BTreeMap::new(),
            last: BTreeMap::new(),
        }
    }

    /// Records the replicas that removed the stream since the last look.
    async fn observe(&mut self, cluster: &JournalCluster) {
        let now = madsim::time::Instant::now();
        for node_id in NODES {
            let removed = match self.detect {
                Detect::Applied(index) => applied(cluster, node_id) >= Some(index),
                Detect::Dropped => dropped(cluster, node_id).await,
            };
            if removed {
                self.at.entry(node_id).or_insert(now);
                self.last.entry(node_id).or_insert(now);
            } else {
                self.last.remove(&node_id);
            }
        }
    }

    /// `node_id` restarted: it applies the removal again as it replays.
    fn restarted(&mut self, node_id: u64) {
        self.last.remove(&node_id);
    }

    /// Waits `duration`, observing all along, while `background` runs.
    async fn wait(
        &mut self,
        cluster: &mut JournalCluster,
        background: &mut Option<Background>,
        duration: Duration,
    ) {
        let until = madsim::time::Instant::now() + duration;
        loop {
            tick(background, cluster).await;
            self.observe(cluster).await;
            if madsim::time::Instant::now() >= until {
                return;
            }
            madsim::time::sleep(POLL).await;
        }
    }
}

/// A read of the marked stream from `node_id`'s local state, at a clock
/// before every expiry, so that the read itself removes nothing.
async fn read_marked(
    cluster: &JournalCluster,
    node_id: u64,
) -> Option<Result<ursula_runtime::ReadStreamResponse, ursula_runtime::GroupEngineError>> {
    let engine = cluster.engines.get(&(GROUP, node_id))?;
    Some(
        engine
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
            .await,
    )
}

/// Whether `node_id` runs and its local state no longer holds the stream.
async fn dropped(cluster: &JournalCluster, node_id: u64) -> bool {
    use ursula_runtime::StreamErrorCode;
    matches!(
        read_marked(cluster, node_id).await,
        Some(Err(error)) if matches!(
            error.code(),
            Some(
                StreamErrorCode::StreamNotFound
                    | StreamErrorCode::BucketNotFound
                    | StreamErrorCode::BucketErased
            )
        )
    )
}

/// What runs beside the checks of an expiry run: the tidy worker's sweep,
/// which cannot run under madsim (its wall clock reads 0 there), and the
/// group's traffic.
#[derive(Debug)]
struct Background {
    started: madsim::time::Instant,
    traffic: Traffic,
    next_sweep: madsim::time::Instant,
    next_append: madsim::time::Instant,
    /// Sweep commands the leaders committed.
    sweeps: u64,
}

impl Background {
    fn new(traffic: Traffic) -> Self {
        let now = madsim::time::Instant::now();
        Self {
            started: now,
            traffic,
            next_sweep: now + expiry_sweep::interval(BOUND_DRIVERS.max_age),
            next_append: now,
            sweeps: 0,
        }
    }

    /// The run's virtual wall clock.
    fn wall_ms(&self) -> u64 {
        SIM_WALL_START_MS
            .saturating_add(u64::try_from(self.started.elapsed().as_millis()).expect("a short run"))
    }

    /// Runs what is due: an append to the group's other stream, then every
    /// running replica's tidy pass, of which only the leader's proposes.
    async fn tick(&mut self, cluster: &mut JournalCluster) {
        let now = madsim::time::Instant::now();
        if self.traffic == Traffic::Appends && now >= self.next_append {
            self.next_append = now + TRAFFIC_EVERY;
            if let Some(leader) = leader_by_state(cluster, GROUP)
                && let Some(engine) = cluster.engines.get_mut(&(GROUP, leader))
            {
                // A lost append is followed by the next one.
                drop(
                    engine
                        .append(
                            AppendRequest::from_bytes(group_stream(GROUP), b"traffic;".to_vec()),
                            group_placement(GROUP),
                            ColdWriteAdmission::default(),
                        )
                        .await,
                );
            }
        }
        if now < self.next_sweep {
            return;
        }
        self.next_sweep = now + expiry_sweep::interval(BOUND_DRIVERS.max_age);
        let now_ms = self.wall_ms();
        for node_id in NODES {
            if let Some(engine) = cluster.engines.get_mut(&(GROUP, node_id))
                && let Ok(report) = engine
                    .tidy_streams(
                        TidyStreamsRequest {
                            max_streams: 0,
                            max_expiry_sweeps: expiry_sweep::MAX_COMMANDS_PER_GROUP,
                            now_ms,
                        },
                        group_placement(GROUP),
                    )
                    .await
            {
                self.sweeps = self.sweeps.saturating_add(report.expiry_sweeps);
            }
        }
    }
}

/// Runs what of `background` is due, if the run has one.
async fn tick(background: &mut Option<Background>, cluster: &mut JournalCluster) {
    if let Some(background) = background {
        background.tick(cluster).await;
    }
}

pub(super) async fn run_deleted_data_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls, BOUND_DRIVERS);
    let plan = Plan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    let journals = Journals::for_seed(config.seed);
    let context = format!("seed {} {plan:?} {journals:?}", config.seed);
    let mut cluster = JournalCluster::start_with_tuning(
        &format!("deleted-data-{}", config.seed),
        journals.tuning,
    )
    .await;
    // The quiet group's entries go first, so they share the segments of
    // the removed records and pin them.
    cluster.append(QUIET_GROUP, journals.quiet_entries).await;
    let mut background = match plan.removal {
        Removal::Expiry(traffic) => Some(Background::new(traffic)),
        Removal::Deletion | Removal::BucketPurge => None,
    };
    // When the expiry comes in this run, and on its virtual wall clock:
    // with the cluster up, past the stream's writes and snapshots.
    let expiry = background.as_ref().map(|background| {
        let at = madsim::time::Instant::now() + EXPIRES_AFTER;
        let at_ms = background
            .wall_ms()
            .saturating_add(u64::try_from(EXPIRES_AFTER.as_millis()).expect("a short expiry"));
        (at, at_ms)
    });
    write_marked_stream(
        &mut cluster,
        plan.records,
        expiry.map(|(_, at_ms)| at_ms),
        &context,
    )
    .await;
    if let Some((expires_at, _)) = expiry {
        assert!(
            madsim::time::Instant::now() + STOP_BEFORE_EXPIRY < expires_at,
            "{context}: the marked stream's writes and snapshots outlasted its expiry"
        );
    }
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
    let before = applied_anywhere(&cluster);
    // The replica that removed the stream first, the log index it applied
    // through then, and how the run sees the others remove it.
    let (remover, removal_index, mut seen) = match plan.removal {
        Removal::Deletion | Removal::BucketPurge => {
            let (remover, ()) = if plan.removal == Removal::BucketPurge {
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
            let removal_index = removal_applied(&cluster, remover, before, &context).await;
            let mut seen = Applied::new(Detect::Applied(removal_index));
            seen.observe(&cluster).await;
            (remover, removal_index, seen)
        }
        Removal::Expiry(_) => {
            let (expires_at, _) = expiry.expect("an expiry run has an expiry");
            let mut seen = Applied::new(Detect::Dropped);
            if plan.fault == Fault::LeaderStopped {
                let stop_at = expires_at
                    .checked_sub(STOP_BEFORE_EXPIRY)
                    .expect("the expiry is past the start");
                let now = madsim::time::Instant::now();
                if stop_at > now {
                    seen.wait(&mut cluster, &mut background, stop_at - now)
                        .await;
                }
                cluster.stop_node(leader).await;
                trace.push(SimEvent::FaultApplied {
                    phase: "leader_stopped_before_expiry".to_owned(),
                });
            }
            // Within the sweep's interval and grace after the expiry, the
            // first replica drops the stream.
            let deadline = expires_at
                + expiry_sweep::interval(BOUND_DRIVERS.max_age)
                + expiry_sweep::CLOCK_GRACE
                + EXPIRY_SLACK
                + if plan.fault == Fault::LeaderStopped {
                    EXPIRY_LEADER_DOWN
                } else {
                    Duration::ZERO
                };
            loop {
                tick(&mut background, &mut cluster).await;
                seen.observe(&cluster).await;
                let first = seen.at.keys().next().copied();
                if let Some(node_id) = first {
                    let index = applied(&cluster, node_id).unwrap_or(0);
                    break (node_id, index, seen);
                }
                if madsim::time::Instant::now() >= deadline {
                    let holder = NODES
                        .into_iter()
                        .find(|node_id| cluster.engines.contains_key(&(GROUP, *node_id)))
                        .expect("a running replica");
                    let held = snapshot_holds_marker(&cluster, holder)
                        .await
                        .unwrap_or_else(|| "its state holds the stream".to_owned());
                    fail(
                        &mut trace,
                        BOUND_INVARIANT,
                        "stream_expired",
                        format!(
                            "{context}: node {holder}: {held}, past the sweep after the expiry \
                             ({})",
                            replica_state(&cluster, holder)
                        ),
                    );
                }
                madsim::time::sleep(POLL).await;
            }
        }
    };
    // The replica restarted after its covering snapshot, and the full
    // snapshots sent to it by then.
    let mut restart = None;

    match plan.fault {
        Fault::None => {}
        Fault::FollowerDown => {
            let node_id = down.expect("a follower is down");
            seen.wait(&mut cluster, &mut background, FOLLOWER_DOWN_AFTER_REMOVAL)
                .await;
            cluster.start_node(node_id).await;
            trace.push(SimEvent::FaultApplied {
                phase: "follower_restarted_after_removal".to_owned(),
            });
        }
        Fault::LeaderStopped if matches!(plan.removal, Removal::Expiry(_)) => {
            // The leader stopped before the expiry: it drops the stream once
            // it is back, from the new leader's log or snapshot.
            let (expires_at, _) = expiry.expect("an expiry run has an expiry");
            let back_at = expires_at - STOP_BEFORE_EXPIRY + EXPIRY_LEADER_DOWN;
            let now = madsim::time::Instant::now();
            if back_at > now {
                seen.wait(&mut cluster, &mut background, back_at - now)
                    .await;
            }
            cluster.start_node(leader).await;
            seen.restarted(leader);
            trace.push(SimEvent::FaultApplied {
                phase: "leader_restarted_after_expiry".to_owned(),
            });
        }
        Fault::LeaderStopped => {
            // Its time still counts from when it first applied the removal:
            // down longer than the bound less its floor, it would miss the
            // bound if the time counted from when it applies the removal
            // again at its restart. A replayed removal is due at once.
            cluster.stop_node(remover).await;
            trace.push(SimEvent::FaultApplied {
                phase: "leader_stopped_after_removal".to_owned(),
            });
            seen.wait(&mut cluster, &mut background, LEADER_DOWN).await;
            cluster.start_node(remover).await;
            seen.restarted(remover);
        }
        Fault::PowerLossAfterSnapshot => {
            // The first replica whose snapshot covers the removal.
            let deadline = madsim::time::Instant::now() + BOUND_DRIVERS.max_age;
            let covered = loop {
                tick(&mut background, &mut cluster).await;
                seen.observe(&cluster).await;
                let covered = NODES
                    .into_iter()
                    .filter(|node_id| {
                        cluster.engines.contains_key(&(GROUP, *node_id))
                            && metrics(&cluster, GROUP, *node_id)
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
            let before_stop = metrics(&cluster, GROUP, covered);
            // Counted before the restart, so the no-install check covers it.
            let sent_before_restart = cluster.full_snapshots_to(GROUP, covered);
            cluster.stop_node(covered).await;
            cluster.wals[&covered].power_loss_losing_unsynced().await;
            trace.push(SimEvent::FaultApplied {
                phase: "power_lost_after_covering_snapshot".to_owned(),
            });
            cluster.start_node(covered).await;
            seen.restarted(covered);
            if controls.purge_after_restart {
                // OpenRaft publishes the restored state from its core task,
                // after the engine started.
                let deadline = madsim::time::Instant::now() + Duration::from_secs(1);
                while metrics(&cluster, GROUP, covered).last_applied.is_none()
                    && madsim::time::Instant::now() < deadline
                {
                    madsim::time::sleep(Duration::from_millis(1)).await;
                }
                let restarted = metrics(&cluster, GROUP, covered);
                // What the restore purge is for: the replica restored the
                // snapshot that dropped the stream, and OpenRaft's startup
                // purged none of its log (it purges only a log that ends
                // below the snapshot).
                let restored = restarted.snapshot.map(|snapshot| snapshot.index);
                assert!(
                    restored >= Some(removal_index),
                    "{context}: node {covered} restored snapshot {restored:?}, below the \
                     removal at {removal_index} ({})",
                    replica_state(&cluster, covered)
                );
                assert_eq!(
                    restarted.purged.map(|log_id| log_id.index),
                    before_stop.purged.map(|log_id| log_id.index),
                    "{context}: node {covered} purged its log at startup"
                );
                restart = Some((covered, sent_before_restart));
                ursula_stream::sim_controls::set_off(
                    ursula_stream::sim_controls::Defence::ScrubPurge,
                    false,
                );
            }
        }
    }

    // Every replica is checked once the bound passed since it removed the
    // stream, and its files once the bound passed since it last did.
    let mut checked = BTreeSet::new();
    let mut files_checked = BTreeSet::new();
    let deadline = madsim::time::Instant::now() + BOUND_DRIVERS.max_age.saturating_mul(2);
    while checked.len() < NODES.len() || files_checked.len() < NODES.len() {
        tick(&mut background, &mut cluster).await;
        seen.observe(&cluster).await;
        let now = madsim::time::Instant::now();
        let due = seen
            .at
            .iter()
            .filter(|(node_id, at)| {
                !checked.contains(*node_id) && now >= **at + BOUND_DRIVERS.max_age
            })
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        for node_id in due {
            if let Some((restarted, sent)) = restart
                && restarted == node_id
            {
                // No leader's snapshot replaced the restored one.
                assert_eq!(
                    cluster.full_snapshots_to(GROUP, node_id),
                    sent,
                    "{context}: node {node_id} installed a snapshot after its restart"
                );
            }
            check_replica(&cluster, node_id, &mut trace, &context).await;
            checked.insert(node_id);
        }
        let files_due = seen
            .last
            .iter()
            .filter(|(node_id, _)| !files_checked.contains(*node_id))
            .filter_map(|(node_id, last)| {
                let first = seen.at.get(node_id)?;
                let due = (*first + BOUND_DRIVERS.max_age)
                    .max(*last + BOUND_DRIVERS.max_age / 2 + RESTART_FILES_SLACK);
                (now >= due).then_some(*node_id)
            })
            .collect::<Vec<_>>();
        for node_id in files_due {
            if let Some(held) = wal_file_holds_marker(&cluster, node_id) {
                fail(
                    &mut trace,
                    BOUND_INVARIANT,
                    "deleted_data_bound_passed",
                    format!(
                        "{context}: node {node_id}: {held} ({})",
                        replica_state(&cluster, node_id)
                    ),
                );
            }
            files_checked.insert(node_id);
        }
        assert!(
            now < deadline,
            "{context}: replicas {:?} never removed the stream (removal at {removal_index})",
            NODES
                .iter()
                .filter(|node_id| !seen.at.contains_key(*node_id))
                .collect::<Vec<_>>()
        );
        madsim::time::sleep(POLL).await;
    }
    let replicas = u64::try_from(checked.len()).expect("replica count fits u64");
    match (plan.removal, background, expiry) {
        (Removal::Expiry(traffic), Some(background), Some((expires_at, _))) => {
            // The expiry anchor: every replica kept the bound within the
            // sweep's interval and grace, and the bound, of the expiry, and
            // a replica a fault kept down within the bound of its return.
            let anchor = expires_at
                + expiry_sweep::interval(BOUND_DRIVERS.max_age)
                + expiry_sweep::CLOCK_GRACE
                + EXPIRY_SLACK
                + BOUND_DRIVERS.max_age
                + match plan.fault {
                    Fault::LeaderStopped => EXPIRY_LEADER_DOWN,
                    Fault::FollowerDown => FOLLOWER_DOWN_AFTER_REMOVAL + EXPIRY_SLACK,
                    Fault::None | Fault::PowerLossAfterSnapshot => Duration::ZERO,
                };
            if madsim::time::Instant::now() > anchor + POLL {
                fail(
                    &mut trace,
                    BOUND_INVARIANT,
                    "deleted_data_bound_passed",
                    format!(
                        "{context}: the replicas kept the bound only {:?} after the expiry",
                        madsim::time::Instant::now() - expires_at
                    ),
                );
            }
            trace.push(SimEvent::ExpiredDataLeft {
                removal_index,
                traffic: traffic.name().to_owned(),
                fault: plan.fault.name().to_owned(),
                replicas,
                sweeps: background.sweeps,
            });
        }
        _ => trace.push(SimEvent::DeletedDataLeft {
            removal_index,
            purge: plan.removal == Removal::BucketPurge,
            fault: plan.fault.name().to_owned(),
            replicas,
            restore_purges: ursula_stream::sim_controls::fired(
                ursula_stream::sim_controls::Defence::RestorePurge,
            ),
        }),
    }
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
    let _switched = Switched::apply(controls, DRIVERS);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    let context = format!("seed {} operator snapshot under a purge", config.seed);
    let mut cluster = JournalCluster::start_with_tuning(
        &format!("operator-snapshot-under-purge-{}", config.seed),
        Journals::for_seed(config.seed).tuning,
    )
    .await;
    write_marked_stream(&mut cluster, 2, None, &context).await;
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
    let before = applied_anywhere(&cluster);
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
    let removal_index = removal_applied(&cluster, leader, before, &context).await;
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

    /// The same with journal segments of 4 to 64 KiB and a quiet group that
    /// pins them: the removed records leave every segment, across several,
    /// the quiet group's entries are copied forward or, past a quarter of a
    /// segment, reported lagging and snapshotted or purged.
    #[test]
    fn deleted_data_across_segments_replays() {
        let _guard = sim_test_guard();
        for seed in 528..=531 {
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

    /// #669: an expired stream that nothing reads, in a quiet group or one
    /// that takes only plain appends, leaves every replica within the bound
    /// once the leader's background sweep removed it, under every fault of
    /// the family, the same way twice per seed.
    #[test]
    fn deleted_data_after_expiry_replays() {
        let _guard = sim_test_guard();
        for seed in 532..=539 {
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
                first.outcome.trace.events.iter().any(|event| matches!(
                    event,
                    SimEvent::ExpiredDataLeft {
                        replicas: 3,
                        sweeps: 1..,
                        ..
                    }
                )),
                "seed {seed}: {:?}",
                first.outcome.trace.events.last()
            );
        }
    }

    /// Without the leader's sweep, an expired stream in a quiet group stays
    /// in the group and its snapshot.
    #[test]
    fn negative_control_an_expired_stream_without_the_sweep_stays_in_a_quiet_group() {
        let _guard = sim_test_guard();
        let message = assert_fails(532, Controls::SWEEP_OFF, BOUND_INVARIANT);
        assert!(message.contains("holds stream"), "{message}");
    }

    /// Without the leader's sweep, plain appends to the group's other stream
    /// never remove the expired stream either: an engine applies a plain
    /// append without the state machine's sweep.
    #[test]
    fn negative_control_plain_appends_never_sweep_an_expired_stream() {
        let _guard = sim_test_guard();
        let message = assert_fails(536, Controls::SWEEP_OFF, BOUND_INVARIANT);
        assert!(message.contains("holds stream"), "{message}");
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
        assert!(
            result.is_err(),
            "{controls:?} left seed {seed} passing: {:?}",
            SimTrace::last_recorded()
                .events
                .iter()
                .filter(|event| matches!(event, SimEvent::FaultApplied { .. }))
                .collect::<Vec<_>>()
        );
        let panic = result.err().map(|payload| {
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|text| (*text).to_owned())
                })
                .unwrap_or_default()
        });
        let trace = SimTrace::last_recorded();
        assert!(
            invariant_failed(&trace, invariant),
            "{controls:?} did not fail `{invariant}` on seed {seed}: {panic:?}, {:?}",
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

    /// Without the age rule of the journal's dead frames, the log drops the
    /// marked records, but the segment files that hold them stay: the small
    /// journal never outgrows its budget (invariant (a)).
    #[test]
    fn negative_control_a_journal_without_its_age_rule_keeps_the_records_in_its_files() {
        let _guard = sim_test_guard();
        let message = assert_fails(520, Controls::WAL_AGE_OFF, BOUND_INVARIANT);
        assert!(
            message.contains("/journal-") && message.contains(".seg holds the marker"),
            "a journal segment holds it: {message}"
        );
    }

    /// A replica that restarts between the snapshot that dropped the stream
    /// and the purge through it restores that snapshot and purges
    /// its log through it within the bound.
    #[test]
    fn scrub_a_restored_snapshot_purges_what_a_crash_left() {
        let _guard = sim_test_guard();
        let report = ThreeNodeRaftSim::run_deleted_data_with_controls_report(
            config(CRASH_SEED),
            Controls::CRASH_BEFORE_PURGE,
        );
        // The bound held, and the restore purge is what held it.
        assert!(
            report.outcome.trace.events.iter().any(|event| matches!(
                event,
                SimEvent::DeletedDataLeft {
                    replicas: 3,
                    restore_purges: 1..,
                    ..
                }
            )),
            "{:?}",
            report.outcome.trace.events.last()
        );
    }

    /// Without the purge through the restored snapshot, the same crash
    /// leaves the stream's records in the restarted replica's log.
    #[test]
    fn negative_control_a_restore_without_its_purge_keeps_the_records() {
        let _guard = sim_test_guard();
        let message = assert_fails(CRASH_SEED, Controls::RESTORE_OFF, BOUND_INVARIANT);
        assert!(message.contains("holds the marker"), "{message}");
    }

    /// A seed whose run cuts a replica's power after its covering
    /// snapshot.
    const CRASH_SEED: u64 = 523;

    /// Nightly seeds of the same fault (`seed % 4 == 3`).
    const NIGHTLY_CRASH_SEEDS: [u64; 3] = [603, 615, 627];

    /// The same crash and its negative control on nightly seeds.
    #[test]
    fn scrub_restore_purge_holds_on_nightly_seeds() {
        let _guard = sim_test_guard();
        for seed in NIGHTLY_CRASH_SEEDS {
            let report = ThreeNodeRaftSim::run_deleted_data_with_controls_report(
                config(seed),
                Controls::CRASH_BEFORE_PURGE,
            );
            assert!(
                report.outcome.trace.events.iter().any(|event| matches!(
                    event,
                    SimEvent::DeletedDataLeft {
                        replicas: 3,
                        restore_purges: 1..,
                        ..
                    }
                )),
                "seed {seed}: {:?}",
                report.outcome.trace.events.last()
            );
            let message = assert_fails(seed, Controls::RESTORE_OFF, BOUND_INVARIANT);
            assert!(
                message.contains("holds the marker"),
                "seed {seed}: {message}"
            );
        }
    }

    /// #669 review: an operator's snapshot that interleaves with a purge
    /// through a covering snapshot the leader defers starts only once that
    /// purge landed, the same way twice.
    #[test]
    fn scrub_purge_holds_an_operator_snapshot() {
        let _guard = sim_test_guard();
        // Seed 529 runs 16 KiB segments.
        for seed in [524, 529] {
            let first = ThreeNodeRaftSim::run_operator_snapshot_under_purge_report(
                config(seed),
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_operator_snapshot_under_purge_report(
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
                    .any(|event| matches!(event, SimEvent::OperatorSnapshotHeldForPurge { .. })),
                "seed {seed}: {:?}",
                first.outcome.trace.events.last()
            );
        }
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
        assert!(message.contains("purge_upto"), "{message}");
    }
}
