//! Per-stream usage under faults (bounded-state F20, the export's consumer
//! contract in operations, "Per-stream usage"), on journal-backed replicas
//! with the production snapshot driver and real OpenRaft
//! ([`JournalCluster`] with [`DriverPlan`]). The accepted loss runs without
//! the driver and with segments larger than its writes, so only the
//! script's own write-backs make durable what the nodes that lose power
//! wrote before the power cut.
//!
//! Scenario `stream-usage-bills-exactly`: writers create streams in two
//! buckets of both groups, write them with producer keys, so every
//! acknowledged write applies exactly once, and delete some, one of them
//! created, written and deleted between two reads. A metering consumer
//! ([`UsageMeter`]) reads every group from a `start` baseline taken before
//! the first write, at random points, each page from a drawn subset of the
//! replicas and from every running one when none of those answers a page,
//! so its pages move between replicas. The run plays every replica's tidy
//! pass on virtual time, so each group engine's retirement clock samples
//! and the leaders' sweeps fold end records half of
//! `raft.deleted_data_max_age` after their removal. Each seed runs one
//! script ([`Script`]) of faults: leader kills, transfers and a cut-off
//! replica; a follower stopped across a snapshot and purge, which installs
//! a snapshot; a power cut after a covering snapshot; a follower cut off
//! while the consumer continues a relisting on it; TTL expiry and a bucket
//! purge; reads skipped past the retention; an accepted loss of a suffix
//! the consumer read; and a restore of both groups into wiped nodes, whose
//! new genesis the consumer's cursors do not name. Every run ends with the
//! consumer reading through the fold of the end records it holds.
//!
//! Invariant `usage_totals_equal_their_parts`: at checkpoints, once every
//! running replica of a group applied the same entry, each replica's
//! `/__ursula/usage` totals ([`ursula_runtime::GroupEngine::bucket_usage`])
//! equal the rows and entries of a `start` relisting at the same applied
//! index, replicas at one applied log id list identical pages, and of two
//! reads of one group in one history, on any replicas, the one at the
//! higher log id lists no version's counters lower and no history baseline
//! changed. Reads of different histories are not compared.
//!
//! Invariant `usage_versions_bill_exactly`: the consumer raises no alarm
//! the contract names, a purge is complete for it by rule 12 with every
//! purged version closed, and at the end, with writes stopped and a
//! requested relisting whose rows check the totals, each version is
//! accounted exactly the units its acknowledged writes committed, which in
//! an accepted loss are the units the final lineage holds plus those the
//! consumer listed in the replaced suffix. The versions the skipped reads
//! script ends while reads are skipped are accounted at most that: their
//! end records fold before the consumer reads them, and what they committed
//! unread goes to their bucket. Per group and bucket, what is accounted to
//! the versions and to the bucket equals what they committed. In a
//! restore, a second consumer that starts on the restored nodes once the
//! import applied accounts exactly what was written after it. The accepted
//! loss's replaced suffix holds writes and creates but no fold and no
//! history event, so rule 7's case of a folded version held again is not
//! reached.
//!
//! Negative controls switch off one defence each ([`Controls`]):
//!
//! - [`Controls::END_RECORDS_OFF`]: a removal folds its final usage straight
//!   into its bucket, so the version created, written and deleted between
//!   two reads is never listed.
//! - [`Controls::SNAPSHOT_USAGE_OFF`]: an install restores as a backup does,
//!   so the follower that installed lists every version below the others.
//! - [`Controls::HISTORY_BASELINE_OFF`]: the first write after the accepted
//!   loss records no baseline, so the consumer accounts the kept prefix
//!   again.
//! - [`Controls::READ_ORDER_OFF`]: the cut-off follower lists the rest of a
//!   relisting from its older state, which misses a version the consumer
//!   holds, so that version's bucket share is negative.
//!
//! [`Controls::START_BEFORE_IMPORT`] switches off no defence: the restore's
//! second consumer starts before the import, against what operations tells
//! a consumer to do, and accounts the backup's totals a second time.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use ursula_config::WalFsync;
use ursula_proto::admin::GroupPurgeIndex;
use ursula_proto::admin::GroupStreamUsage;
use ursula_proto::admin::GroupUsagePage;
use ursula_proto::admin::HistoryId;
use ursula_proto::admin::RelistReason;
use ursula_proto::admin::StreamUsageEnd;
use ursula_proto::admin::UsageCursor;
use ursula_proto::admin::UsagePage;
use ursula_proto::admin::VersionLifecycle;
use ursula_raft::JournalTuning;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupBucketUsage;
use ursula_runtime::GroupEngine;
use ursula_runtime::ImportGroupStateRequest;
use ursula_runtime::ProducerRequest;
use ursula_runtime::PurgeBucketRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::TidyStreamsRequest;
use ursula_runtime::expiry_sweep;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::sim_controls::Defence;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::deleted_data::BOUND_DRIVERS;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::NODES;
use super::journal_cluster::group_placement;
use super::journal_cluster::lead_every_group;
use super::journal_cluster::leader_by_state;
use super::journal_cluster::metrics;
use super::journal_cluster::recover_group;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::wait_leader;
use super::journal_cluster::wait_plan_satisfied;
use super::recovery_wiring::RECOVERY_STALL_AFTER;
use super::usage_meter::Counters;
use super::usage_meter::UsageMeter;
use super::usage_meter::VersionKey;
use super::usage_meter::add;
use super::usage_meter::add_committed;
use super::usage_meter::committed;

pub(super) const TOTALS_INVARIANT: &str = "usage_totals_equal_their_parts";
pub(super) const ACCOUNTING_INVARIANT: &str = "usage_versions_bill_exactly";

/// The drivers and the bound of the deleted-data scenario: sixteen seconds,
/// at or above the config's floor for their drive interval, so an end
/// record folds eight seconds and at most two one-second sweep intervals
/// after its removal.
const DRIVERS: DriverPlan = BOUND_DRIVERS;
/// The virtual time a run may take: its script, an accepted loss's stall
/// and plan, or a restore's wipe, and the final reads through a fold.
pub(super) const STREAM_USAGE_LIMIT: Duration = Duration::from_secs(300);
/// The buckets the writers write.
const BUCKETS: [&str; 2] = ["tenant-a", "tenant-b"];
/// The bucket a purge erases.
const PURGED: &str = "tenant-b";
/// The run's virtual wall clock at its start: any time past the epoch.
const SIM_WALL_START_MS: u64 = 1_700_000_000_000;
/// Workload rounds of a script, each a few writes and a read.
const ROUNDS: u64 = 8;
/// The lengths of the writers' bodies: one write unit, one, and two.
const BODY_LENS: [usize; 3] = [64, 700, 10_241];
/// The page limits a read draws: pages of one to three items, or one page.
const PAGE_LIMITS: [usize; 4] = [1, 2, 3, 10_000];
/// The most pages one read of a group asks for before it gives up.
const MAX_PAGES: usize = 400;
/// How long one write may take to be acknowledged while leadership moves.
const WRITE_WAIT: Duration = Duration::from_secs(30);
/// The run's step: how long it waits between tries.
const STEP: Duration = Duration::from_millis(50);
/// How long after its create a TTL stream expires.
const TTL: Duration = Duration::from_secs(3);

/// Server defences a negative control turns off, and the restore's second
/// consumer's start. Production runs use the default, with every defence
/// on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// A removal folds its final usage straight into its bucket.
    end_records_off: bool,
    /// An install restores a group snapshot as a backup is restored.
    snapshot_usage_off: bool,
    /// The first write after a history event records no baseline.
    history_baseline_off: bool,
    /// A replica below a cursor's log id lists from its older state.
    read_order_off: bool,
    /// The restore's second consumer starts before the import.
    start_before_import: bool,
}

#[cfg(test)]
impl Controls {
    const NONE: Self = Self {
        end_records_off: false,
        snapshot_usage_off: false,
        history_baseline_off: false,
        read_order_off: false,
        start_before_import: false,
    };
    pub(super) const END_RECORDS_OFF: Self = Self {
        end_records_off: true,
        ..Self::NONE
    };
    pub(super) const SNAPSHOT_USAGE_OFF: Self = Self {
        snapshot_usage_off: true,
        ..Self::NONE
    };
    pub(super) const HISTORY_BASELINE_OFF: Self = Self {
        history_baseline_off: true,
        ..Self::NONE
    };
    pub(super) const READ_ORDER_OFF: Self = Self {
        read_order_off: true,
        ..Self::NONE
    };
    pub(super) const START_BEFORE_IMPORT: Self = Self {
        start_before_import: true,
        ..Self::NONE
    };
}

/// The defences [`Controls`] turned off for this simulation, and the
/// drivers its cluster runs, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls, script: Script) -> Self {
        set_defences(controls);
        set_snapshot_drivers(script.drivers());
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
    use ursula_stream::sim_controls::set_off;
    set_off(Defence::EndRecords, controls.end_records_off);
    set_off(Defence::SnapshotUsage, controls.snapshot_usage_off);
    set_off(Defence::HistoryBaseline, controls.history_baseline_off);
    set_off(Defence::UsageReadOrder, controls.read_order_off);
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// The faults of one run, by its seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Script {
    /// The leader of a group killed and restarted, a leadership transfer,
    /// and a replica cut off and healed, between reads.
    Churn,
    /// A follower stopped while the others snapshot and purge their logs,
    /// so it installs a snapshot when it returns, then read at once.
    Install,
    /// A replica's power cut right after its snapshot, under `fsync =
    /// always`: it restores the snapshot and replays its log.
    PowerCut,
    /// A follower cut off while a stream is created, and a relisting whose
    /// pages after the first go to it first.
    Lagging,
    /// Streams that expire and the leaders' sweeps remove, then a purge of
    /// their bucket, complete for the consumer by rule 12. Each group keeps
    /// a stream of the purged bucket the writers write and never delete, so
    /// each group's purge removes one.
    PurgeAndExpiry,
    /// No read for longer than the retention after a version was written
    /// and deleted, and one created, written and deleted unread, so their
    /// end records fold unread and the group relists `retired`.
    SkipPastRetention,
    /// Under `fsync = never`, the leader cut off, the others take writes
    /// the consumer reads and lose them in a power cut, and a recovery
    /// plan accepts the loss: a new history. It runs without the snapshot
    /// driver and with segments no run fills, so no snapshot, rotation or
    /// reclaim syncs the window before the power cut. On half of the seeds
    /// the others write back the window's creates and first appends before
    /// its last appends, so the plan keeps their longest log without a cut
    /// and versions created in the window live on into the new history. On
    /// the other half they lose the whole window, and the plan cuts.
    AcceptedLoss,
    /// Both groups backed up, every node wiped, and the backups imported
    /// into the new groups, whose genesis the consumer's cursors do not
    /// name.
    Restore,
}

/// Scripts in seed order: a seed runs `SCRIPTS[seed % 8]`.
pub(super) const SCRIPTS: [Script; 8] = [
    Script::Churn,
    Script::Install,
    Script::PowerCut,
    Script::Lagging,
    Script::PurgeAndExpiry,
    Script::SkipPastRetention,
    Script::AcceptedLoss,
    Script::Restore,
];

impl Script {
    pub(super) fn of(seed: u64) -> Self {
        let index = usize::try_from(seed % 8).expect("an index below 8");
        SCRIPTS[index]
    }

    fn name(self) -> &'static str {
        match self {
            Self::Churn => "churn",
            Self::Install => "install",
            Self::PowerCut => "power_cut",
            Self::Lagging => "lagging",
            Self::PurgeAndExpiry => "purge_and_expiry",
            Self::SkipPastRetention => "skip_past_retention",
            Self::AcceptedLoss => "accepted_loss",
            Self::Restore => "restore",
        }
    }

    /// The journals of its nodes. An accepted loss's segments are larger
    /// than anything a run writes, so no rotation, and no reclaim, syncs
    /// the window under `fsync = never`.
    fn tuning(self) -> JournalTuning {
        match self {
            Self::AcceptedLoss => JournalTuning {
                segment_bytes: 64 * 1024 * 1024,
                ..JournalTuning::new(WalFsync::Never)
            },
            _ => JournalTuning::new(WalFsync::Always),
        }
    }

    /// The snapshot drivers its nodes run. An accepted loss runs none: each
    /// build or install syncs the journal before it publishes, which would
    /// make the window durable before the power cut. OpenRaft's own policy
    /// then builds no snapshot within a run.
    fn drivers(self) -> Option<DriverPlan> {
        match self {
            Self::AcceptedLoss => None,
            _ => Some(DRIVERS),
        }
    }
}

/// One stream version the writers made, as the oracle counts it.
#[derive(Debug, Clone)]
struct Version {
    group: u32,
    stream: BucketStreamId,
    /// What its acknowledged writes committed, by the counting rule.
    counters: Counters,
    /// What it had committed when a backup copied it.
    before_restore: Counters,
    /// Writes go to it: it is live and expires at no time the run reaches.
    writable: bool,
    /// The producer generation its writer last wrote in.
    generation: u64,
    /// The next producer seq of its writer in that generation.
    seq: u64,
    /// Writes acknowledged during an accepted loss's window: each body and
    /// what it committed.
    window: Vec<(Vec<u8>, Counters)>,
    /// It was created during that window.
    created_in_window: bool,
    /// It ended while the skipped reads script read nothing, so its end
    /// record may fold before the consumer reads it.
    ended_unread: bool,
    /// Its writers wrote it in more than one history.
    written_across_history: bool,
    /// A purge removes it: the writers write it but never delete it.
    purge_target: bool,
}

/// What a create without a body commits: one write unit.
fn created() -> Counters {
    Counters {
        committed_write_units: 1,
        ..Counters::default()
    }
}

/// What a byte append of `len` bytes commits: its bytes, one record,
/// `max(1, ceil(len / 10240))` units and one append.
fn appended(len: usize) -> Counters {
    let bytes = u64::try_from(len).expect("a short body");
    Counters {
        committed_append_bytes: bytes,
        committed_records: 1,
        committed_write_units: bytes.div_ceil(10_240).max(1),
        appends: 1,
    }
}

/// The run's virtual wall clock: [`SIM_WALL_START_MS`] when the run starts,
/// then madsim's time.
#[derive(Debug, Clone, Copy)]
struct RunClock {
    started: madsim::time::Instant,
}

impl RunClock {
    fn now_ms(self) -> u64 {
        SIM_WALL_START_MS
            .saturating_add(u64::try_from(self.started.elapsed().as_millis()).expect("a short run"))
    }
}

/// Which replicas a read of a group asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// A drawn subset of the running replicas, each page.
    Drawn,
    /// Every running replica.
    All,
    /// Every running replica but this one.
    Avoid(u64),
    /// The first page from the others, every later page from this replica
    /// first.
    LaterTo(u64),
}

/// One replica's read of a group at a checkpoint: a `start` relisting in
/// one page and `/__ursula/usage` at the same applied index.
#[derive(Debug, Clone)]
struct ReplicaRead {
    node_id: u64,
    page: GroupUsagePage,
    totals: GroupBucketUsage,
}

/// What one checkpoint read of a group listed of each version, for the
/// monotonic part of `usage_totals_equal_their_parts`.
#[derive(Debug, Clone)]
struct Observation {
    node_id: u64,
    history: HistoryId,
    applied: ursula_proto::admin::UsageLogId,
    versions: BTreeMap<VersionKey, (Counters, Counters)>,
}

/// One run.
struct Run {
    seed: u64,
    script: Script,
    controls: Controls,
    context: String,
    rng: SplitMix64,
    clock: RunClock,
    cluster: JournalCluster,
    tuning: JournalTuning,
    trace: SimTrace,
    /// The consumer that reads the cluster from its first `start`.
    meter: UsageMeter,
    /// The restore's second consumer, which starts on the restored nodes.
    fresh: Option<UsageMeter>,
    /// The oracle: every version the writers made, by stream id, which no
    /// two of them share.
    versions: BTreeMap<String, Version>,
    next_stream: u64,
    /// The writers' producer generation: a history event closes their
    /// epochs, so each history's writers are new producers.
    generation: u64,
    /// An accepted loss's window runs: writes are recorded in it.
    window: bool,
    /// The bucket was purged: nothing creates in it again.
    purged: bool,
    /// Nodes stopped by the script.
    stopped: BTreeSet<u64>,
    /// Nodes cut off from the others, whose tidy passes the run skips: a
    /// cut-off leader's sweep would wait on an entry that never commits.
    cut_off: BTreeSet<u64>,
    next_tidy: madsim::time::Instant,
    /// Checkpoint reads, per group.
    observations: BTreeMap<u32, Vec<Observation>>,
    /// Replica reads a checkpoint judged.
    judged: u64,
    /// The write units the accepted loss lost, counted right after its
    /// plan, before a later delete of a window stream counts as loss.
    lost: u64,
}

impl Run {
    async fn start(config: &ThreeNodeRaftSimConfig, controls: Controls) -> Self {
        let script = Script::of(config.seed);
        let tuning = script.tuning();
        let mut trace = SimTrace::default();
        trace.push(SimEvent::ClusterBuilt { seed: config.seed });
        let cluster =
            JournalCluster::start_with_tuning(&format!("stream-usage-{}", config.seed), tuning)
                .await;
        let now = madsim::time::Instant::now();
        Self {
            seed: config.seed,
            script,
            controls,
            context: format!("seed {} script {}", config.seed, script.name()),
            rng: SplitMix64::new(config.seed ^ 0x7573_6167_655f_6d74),
            clock: RunClock { started: now },
            cluster,
            tuning,
            trace,
            meter: UsageMeter::new(JOURNAL_GROUPS, retention()),
            fresh: None,
            versions: BTreeMap::new(),
            next_stream: 0,
            generation: 0,
            window: false,
            purged: false,
            stopped: BTreeSet::new(),
            cut_off: BTreeSet::new(),
            next_tidy: now,
            observations: BTreeMap::new(),
            judged: 0,
            lost: 0,
        }
    }

    fn wall_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    fn draw(&mut self, upper: u64) -> u64 {
        self.rng.next_bounded(upper)
    }

    fn fault(&mut self, phase: &str) {
        self.trace.push(SimEvent::FaultApplied {
            phase: phase.to_owned(),
        });
    }

    /// The running replicas of `group`.
    fn running(&self, group: u32) -> Vec<u64> {
        NODES
            .into_iter()
            .filter(|node_id| self.cluster.engines.contains_key(&(group, *node_id)))
            .collect()
    }

    /// The node that leads `group`, of those not cut off.
    fn leader(&self, group: u32) -> Option<u64> {
        NODES.into_iter().find(|node_id| {
            !self.cut_off.contains(node_id)
                && self.cluster.engines.contains_key(&(group, *node_id))
                && metrics(&self.cluster, group, *node_id).state == openraft::ServerState::Leader
        })
    }

    /// Every [`expiry_sweep::interval`] of the bound, each running replica
    /// not cut off runs its tidy pass at the run's wall clock, as the tidy
    /// worker does: it samples its retirement clock, and a leader sweeps.
    async fn tick(&mut self) {
        let now = madsim::time::Instant::now();
        if now < self.next_tidy {
            return;
        }
        self.next_tidy = now + expiry_sweep::interval(DRIVERS.max_age);
        let now_ms = self.wall_ms();
        for group in JOURNAL_GROUPS {
            for node_id in NODES {
                if self.cut_off.contains(&node_id) {
                    continue;
                }
                if let Some(engine) = self.cluster.engines.get_mut(&(group, node_id)) {
                    drop(
                        engine
                            .tidy_streams(
                                TidyStreamsRequest {
                                    max_streams: 0,
                                    max_expiry_sweeps: expiry_sweep::MAX_COMMANDS_PER_GROUP,
                                    expiry_sweep_budget: expiry_sweep::GROUP_BUDGET,
                                    deleted_data_max_age: DRIVERS.max_age,
                                    now_ms,
                                },
                                group_placement(group),
                            )
                            .await,
                    );
                }
            }
        }
    }

    /// Waits `duration`, running the tidy passes all along.
    async fn pause(&mut self, duration: Duration) {
        let until = madsim::time::Instant::now() + duration;
        loop {
            self.tick().await;
            let now = madsim::time::Instant::now();
            if now >= until {
                return;
            }
            madsim::time::sleep(STEP.min(until - now)).await;
        }
    }

    /// Waits for the next try of a write, failing the run once `deadline`
    /// passed.
    async fn retry(&mut self, deadline: madsim::time::Instant, what: &str) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{}: {what} was never acknowledged",
            self.context
        );
        self.pause(STEP).await;
    }

    // ----- the writers -----

    /// A stream that `group`'s writers create in `bucket_id`, expiring
    /// after [`TTL`] when `expires`, acknowledged once.
    async fn create(&mut self, group: u32, bucket_id: &str, expires: bool) -> BucketStreamId {
        let stream = BucketStreamId::new(bucket_id, format!("g{group}-s{}", self.next_stream));
        self.next_stream = self.next_stream.saturating_add(1);
        let mut request = CreateStreamRequest::new(stream.clone(), "application/octet-stream");
        request.now_ms = self.wall_ms();
        if expires {
            request.stream_expires_at_ms = Some(
                request
                    .now_ms
                    .saturating_add(u64::try_from(TTL.as_millis()).expect("a short TTL")),
            );
        }
        let deadline = madsim::time::Instant::now() + WRITE_WAIT;
        loop {
            if let Some(leader) = self.leader(group)
                && let Some(engine) = self.cluster.engines.get_mut(&(group, leader))
                && engine
                    .create_stream(
                        request.clone(),
                        group_placement(group),
                        ColdWriteAdmission::default(),
                    )
                    .await
                    .is_ok()
            {
                break;
            }
            self.retry(deadline, &format!("the create of {stream}"))
                .await;
        }
        self.versions.insert(stream.stream_id.clone(), Version {
            group,
            stream: stream.clone(),
            counters: created(),
            before_restore: Counters::default(),
            writable: !expires,
            generation: self.generation,
            seq: 0,
            window: Vec::new(),
            created_in_window: self.window,
            ended_unread: false,
            written_across_history: false,
            purge_target: false,
        });
        stream
    }

    /// One keyed write of a drawn length to `stream`, sent until a leader
    /// acknowledges it: a repeat after a lost answer applies once.
    async fn append(&mut self, stream: &BucketStreamId) {
        let len = BODY_LENS[usize::try_from(self.draw(3)).expect("an index")];
        let (group, seq) = {
            let version = &self.versions[&stream.stream_id];
            // A new generation's writer is a new producer.
            let seq = if version.generation == self.generation {
                version.seq
            } else {
                0
            };
            (version.group, seq)
        };
        let mut body = format!("<{}:{}:{seq}>", stream.stream_id, self.generation).into_bytes();
        body.resize(len.max(body.len()), b'.');
        let len = body.len();
        let mut request = AppendRequest::from_bytes(stream.clone(), body.clone());
        request.now_ms = self.wall_ms();
        request.producer = Some(ProducerRequest {
            producer_id: format!("writer-{}-{}", stream.stream_id, self.generation),
            producer_epoch: 0,
            producer_seq: seq,
            digest: None,
            ..Default::default()
        });
        let deadline = madsim::time::Instant::now() + WRITE_WAIT;
        loop {
            if let Some(leader) = self.leader(group)
                && let Some(engine) = self.cluster.engines.get_mut(&(group, leader))
                && engine
                    .append(
                        request.clone(),
                        group_placement(group),
                        ColdWriteAdmission::default(),
                    )
                    .await
                    .is_ok()
            {
                break;
            }
            self.retry(deadline, &format!("write {seq} of {stream}"))
                .await;
        }
        let window = self.window;
        let generation = self.generation;
        let version = self
            .versions
            .get_mut(&stream.stream_id)
            .expect("a version the writers made");
        add(&mut version.counters, appended(len));
        if version.generation != generation {
            version.written_across_history = true;
        }
        version.generation = generation;
        version.seq = seq.saturating_add(1);
        if window {
            version.window.push((body, appended(len)));
        }
    }

    /// Deletes `stream`, again until it is gone.
    async fn delete(&mut self, stream: &BucketStreamId) {
        let group = self.versions[&stream.stream_id].group;
        let request = DeleteStreamRequest {
            stream_id: stream.clone(),
            now_ms: self.wall_ms(),
            if_incarnation: None,
        };
        let deadline = madsim::time::Instant::now() + WRITE_WAIT;
        let mut tried = false;
        loop {
            if let Some(leader) = self.leader(group)
                && let Some(engine) = self.cluster.engines.get_mut(&(group, leader))
            {
                match engine
                    .delete_stream(request.clone(), group_placement(group))
                    .await
                {
                    Ok(_) => break,
                    // A repeat after an answer that was lost finds it gone.
                    Err(error)
                        if tried && error.code() == Some(StreamErrorCode::StreamNotFound) =>
                    {
                        break;
                    }
                    Err(_retry) => tried = true,
                }
            }
            self.retry(deadline, &format!("the delete of {stream}"))
                .await;
        }
        if let Some(version) = self.versions.get_mut(&stream.stream_id) {
            version.writable = false;
        }
    }

    /// The streams the writers write, by stream id.
    fn writable(&self) -> Vec<BucketStreamId> {
        self.versions
            .values()
            .filter(|version| version.writable)
            .map(|version| version.stream.clone())
            .collect()
    }

    fn draw_writable(&mut self) -> Option<BucketStreamId> {
        let writable = self.writable();
        self.draw_of(writable)
    }

    /// A writable stream the writers may delete: any but a purge's targets.
    fn draw_deletable(&mut self) -> Option<BucketStreamId> {
        let deletable = self
            .versions
            .values()
            .filter(|version| version.writable && !version.purge_target)
            .map(|version| version.stream.clone())
            .collect();
        self.draw_of(deletable)
    }

    fn draw_of(&mut self, streams: Vec<BucketStreamId>) -> Option<BucketStreamId> {
        let count = u64::try_from(streams.len()).expect("a count");
        if count == 0 {
            return None;
        }
        let index = usize::try_from(self.draw(count)).expect("an index");
        streams.into_iter().nth(index)
    }

    fn draw_bucket(&mut self) -> &'static str {
        if self.purged {
            return BUCKETS[0];
        }
        BUCKETS[usize::try_from(self.draw(2)).expect("an index")]
    }

    /// A version created, written and deleted between two reads.
    async fn ephemeral(&mut self) -> BucketStreamId {
        let group = JOURNAL_GROUPS[usize::try_from(self.draw(2)).expect("an index")];
        let bucket_id = self.draw_bucket();
        let stream = self.create(group, bucket_id, false).await;
        self.append(&stream).await;
        self.append(&stream).await;
        self.delete(&stream).await;
        stream
    }

    /// One round of writes: a new stream with up to two writes now and
    /// then, writes to up to three streams, and a delete now and then.
    async fn workload(&mut self) {
        if self.draw(2) == 0 || self.writable().len() < 3 {
            let group = JOURNAL_GROUPS[usize::try_from(self.draw(2)).expect("an index")];
            let bucket_id = self.draw_bucket();
            let stream = self.create(group, bucket_id, false).await;
            for _ in 0..self.draw(3) {
                self.append(&stream).await;
            }
        }
        for _ in 0..self.draw(3).saturating_add(1) {
            if let Some(stream) = self.draw_writable() {
                self.append(&stream).await;
            }
        }
        if self.draw(4) == 0
            && self.writable().len() > 2
            && let Some(stream) = self.draw_deletable()
        {
            self.delete(&stream).await;
        }
    }

    // ----- the consumer -----

    /// Reads every group of `meter` (the run's own consumer, or the
    /// restore's second one), each from its cursor until its listing
    /// completes, with a drawn page limit. Whether every group completed.
    async fn poll(&mut self, fresh: bool) -> bool {
        self.poll_via(fresh, Route::Drawn).await
    }

    /// [`Self::poll`], each page asking the replicas `route` reaches.
    async fn poll_via(&mut self, fresh: bool, route: Route) -> bool {
        let mut complete = true;
        for group in JOURNAL_GROUPS {
            let limit = PAGE_LIMITS[usize::try_from(self.draw(4)).expect("an index")];
            complete &= self.read_group(fresh, group, limit, route).await;
        }
        complete
    }

    /// Polls until a poll is complete, failing `usage_versions_bill_exactly`
    /// when none is within a while.
    async fn poll_until_complete(&mut self, fresh: bool, phase: &str) {
        self.poll_until_complete_via(fresh, phase, Route::Drawn)
            .await;
    }

    /// [`Self::poll_until_complete`] through `route`. A poll that must read
    /// the newest state, before a history event closes the versions it
    /// holds, asks every running replica and keeps the highest answer.
    async fn poll_until_complete_via(&mut self, fresh: bool, phase: &str, route: Route) {
        for _ in 0..100 {
            if self.poll_via(fresh, route).await {
                return;
            }
            self.pause(Duration::from_millis(200)).await;
        }
        let context = self.context.clone();
        fail(
            &mut self.trace,
            ACCOUNTING_INVARIANT,
            phase,
            format!("{context}: the consumer never completed a poll"),
        );
    }

    /// Reads `group` until its listing completes (rules 1 to 3): each page
    /// from the replicas `route` reaches, and from every running one it
    /// allows when those answer no page; a `more` page's next at once.
    /// Fails `usage_versions_bill_exactly` on the consumer's first alarm.
    /// Whether the listing completed.
    async fn read_group(&mut self, fresh: bool, group: u32, limit: usize, route: Route) -> bool {
        let limit = NonZeroUsize::new(limit).expect("a page limit");
        for page in 0..MAX_PAGES {
            let running: Vec<u64> = self
                .running(group)
                .into_iter()
                .filter(|node_id| route != Route::Avoid(*node_id))
                .collect();
            if running.is_empty() {
                return false;
            }
            let reached = match route {
                Route::LaterTo(node_id) if page > 0 => vec![node_id],
                Route::LaterTo(node_id) => running
                    .iter()
                    .copied()
                    .filter(|running| *running != node_id)
                    .collect(),
                Route::Avoid(_) | Route::All => running.clone(),
                Route::Drawn => {
                    let drawn: Vec<u64> = running
                        .iter()
                        .copied()
                        .filter(|_| self.rng.next_bounded(2) == 0)
                        .collect();
                    if drawn.is_empty() {
                        vec![
                            running[usize::try_from(
                                self.draw(u64::try_from(running.len()).expect("a count")),
                            )
                            .expect("an index")],
                        ]
                    } else {
                        drawn
                    }
                }
            };
            let mut kept = self.answer(fresh, group, &reached, limit).await;
            if !kept {
                kept = self.answer(fresh, group, &running, limit).await;
            }
            if !kept {
                return false;
            }
            let (alarm, wants_next_page, complete) = {
                let meter = self.meter_mut(fresh);
                meter.pages = meter.pages.saturating_add(1);
                let metered = &meter.groups[&group];
                (
                    metered.alarms.first().cloned(),
                    metered.wants_next_page(),
                    metered.complete(),
                )
            };
            if let Some(alarm) = alarm {
                let message = format!("{}: group {group}: {alarm}", self.context);
                fail(&mut self.trace, ACCOUNTING_INVARIANT, "page_kept", message);
            }
            if !wants_next_page {
                return complete;
            }
        }
        false
    }

    fn meter_mut(&mut self, fresh: bool) -> &mut UsageMeter {
        if fresh {
            self.fresh.as_mut().expect("the restore's second consumer")
        } else {
            &mut self.meter
        }
    }

    /// Asks `nodes` for one page of `group` with the consumer's cursor, and
    /// keeps the listed answer with the highest log id. Whether one was
    /// kept.
    async fn answer(
        &mut self,
        fresh: bool,
        group: u32,
        nodes: &[u64],
        limit: NonZeroUsize,
    ) -> bool {
        let cursor = self.meter_mut(fresh).groups[&group].cursor();
        let answers = read_answers(&mut self.cluster, group, nodes, cursor, limit).await;
        let now = madsim::time::Instant::now();
        let meter = self.meter_mut(fresh);
        let retention = meter.retention;
        let UsageMeter { groups, behind, .. } = meter;
        groups
            .get_mut(&group)
            .expect("a metered group")
            .take_answers(answers, now, retention, behind)
    }

    /// Each round's reads: of the run's consumer, and of the restore's
    /// second one once it started.
    async fn poll_round(&mut self) {
        self.poll(false).await;
        if self.fresh.is_some() {
            self.poll(true).await;
        }
    }

    /// A round: writes, a pause and the round's reads.
    async fn round(&mut self) {
        self.workload().await;
        let pause = Duration::from_millis(100_u64.saturating_mul(self.draw(5).saturating_add(1)));
        self.pause(pause).await;
        self.poll_round().await;
    }

    // ----- usage_totals_equal_their_parts -----

    /// Once every running replica of each group applied the same entry,
    /// judges `usage_totals_equal_their_parts` on their reads.
    async fn checkpoint(&mut self, phase: &str) {
        for group in JOURNAL_GROUPS {
            let mut settled = false;
            for _ in 0..200 {
                let running = self.running(group);
                let applied: Vec<_> = running
                    .iter()
                    .map(|node_id| metrics(&self.cluster, group, *node_id).last_applied)
                    .collect();
                if running.len() > 1
                    && applied.windows(2).all(|pair| pair[0] == pair[1])
                    && leader_by_state(&self.cluster, group).is_some()
                {
                    settled = true;
                    break;
                }
                madsim::time::sleep(STEP).await;
            }
            if !settled {
                let context = self.context.clone();
                fail(
                    &mut self.trace,
                    TOTALS_INVARIANT,
                    phase,
                    format!("{context}: group {group}'s replicas never applied one entry together"),
                );
            }
            let mut reads = Vec::new();
            for node_id in self.running(group) {
                let Some(read) = replica_read(&mut self.cluster, group, node_id).await else {
                    let context = self.context.clone();
                    fail(
                        &mut self.trace,
                        TOTALS_INVARIANT,
                        phase,
                        format!(
                            "{context}: group {group}: node {node_id} gave no settled read of \
                             its relisting and totals"
                        ),
                    );
                };
                reads.push(read);
            }
            self.judge(group, &reads, phase);
        }
    }

    fn judge(&mut self, group: u32, reads: &[ReplicaRead], phase: &str) {
        let context = format!("{}: group {group}", self.context);
        let mut failures = Vec::new();
        for read in reads {
            failures.extend(parts_disagree(read));
        }
        for (index, left) in reads.iter().enumerate() {
            for right in reads.iter().skip(index.saturating_add(1)) {
                if left.page.applied == right.page.applied && left.page != right.page {
                    failures.push(format!(
                        "nodes {} and {} list different pages at {:?}: {:?} and {:?}",
                        left.node_id, right.node_id, left.page.applied, left.page, right.page
                    ));
                }
            }
        }
        let observations = self.observations.entry(group).or_default();
        for read in reads {
            let observation = Observation {
                node_id: read.node_id,
                history: read.page.history.id,
                applied: read.page.applied,
                versions: read
                    .page
                    .entries
                    .iter()
                    .map(|entry| {
                        (
                            VersionKey::of(entry),
                            (entry.counters, entry.history_baseline),
                        )
                    })
                    .collect(),
            };
            for earlier in observations
                .iter()
                .filter(|earlier| earlier.history == observation.history)
            {
                let (older, newer) = if earlier.applied <= observation.applied {
                    (earlier, &observation)
                } else {
                    (&observation, earlier)
                };
                for (key, (counters, baseline)) in &older.versions {
                    let Some((later, later_baseline)) = newer.versions.get(key) else {
                        continue;
                    };
                    let fell = later.committed_append_bytes < counters.committed_append_bytes
                        || later.committed_records < counters.committed_records
                        || later.committed_write_units < counters.committed_write_units
                        || later.appends < counters.appends;
                    if fell || later_baseline != baseline {
                        failures.push(format!(
                            "{key:?}: node {} at {:?} lists {later:?} with baseline \
                             {later_baseline:?}, node {} at {:?} listed {counters:?} with \
                             baseline {baseline:?}",
                            newer.node_id, newer.applied, older.node_id, older.applied
                        ));
                    }
                }
            }
            observations.push(observation);
        }
        if let Some(failure) = failures.into_iter().next() {
            fail(
                &mut self.trace,
                TOTALS_INVARIANT,
                phase,
                format!("{context}: {failure}"),
            );
        }
        self.judged = self
            .judged
            .saturating_add(u64::try_from(reads.len()).expect("a count"));
    }

    // ----- faults -----

    async fn stop(&mut self, node_id: u64) {
        self.cluster.stop_node(node_id).await;
        self.stopped.insert(node_id);
        self.trace.push(SimEvent::NodeStopped { node_id });
    }

    async fn restart(&mut self, node_id: u64) {
        self.cluster.start_node(node_id).await;
        self.stopped.remove(&node_id);
        self.trace.push(SimEvent::NodeRestarted { node_id });
    }

    /// Cuts `node_id` off from the other nodes.
    fn cut(&mut self, node_id: u64) {
        for other in NODES.into_iter().filter(|other| *other != node_id) {
            self.cluster.policy.partition_bidirectional(node_id, other);
        }
        self.cut_off.insert(node_id);
    }

    fn heal(&mut self) {
        for node_id in std::mem::take(&mut self.cut_off) {
            for other in NODES.into_iter().filter(|other| *other != node_id) {
                self.cluster.policy.heal_bidirectional(node_id, other);
            }
        }
    }

    /// A node other than `group`'s leader, drawn.
    async fn follower(&mut self, group: u32) -> u64 {
        let leader = wait_leader(&self.cluster, group, &self.context).await;
        let followers: Vec<u64> = NODES
            .into_iter()
            .filter(|node_id| *node_id != leader)
            .collect();
        followers[usize::try_from(self.draw(2)).expect("an index")]
    }

    /// Waits until every running replica of `group` applied what its
    /// leader applied.
    async fn wait_caught_up(&mut self, group: u32) {
        let deadline = madsim::time::Instant::now() + Duration::from_secs(20);
        loop {
            let leader = wait_leader(&self.cluster, group, &self.context).await;
            let target = metrics(&self.cluster, group, leader).last_applied;
            if self
                .running(group)
                .iter()
                .all(|node_id| metrics(&self.cluster, group, *node_id).last_applied >= target)
            {
                return;
            }
            assert!(
                madsim::time::Instant::now() < deadline,
                "{}: group {group}'s replicas never caught up",
                self.context
            );
            self.pause(STEP).await;
        }
    }

    // ----- the scripts -----

    async fn churn(&mut self) {
        let mut killed = None;
        for round in 0..ROUNDS {
            self.round().await;
            match round {
                1 => {
                    let group = JOURNAL_GROUPS[usize::try_from(self.draw(2)).expect("an index")];
                    let leader = wait_leader(&self.cluster, group, &self.context).await;
                    self.fault("leader_killed");
                    self.stop(leader).await;
                    killed = Some(leader);
                }
                3 => {
                    if let Some(node_id) = killed.take() {
                        self.restart(node_id).await;
                    }
                    self.wait_caught_up(0).await;
                    self.wait_caught_up(1).await;
                    self.checkpoint("leader_restarted").await;
                }
                4 => {
                    let group = JOURNAL_GROUPS[usize::try_from(self.draw(2)).expect("an index")];
                    let leader = wait_leader(&self.cluster, group, &self.context).await;
                    let to = self.follower(group).await;
                    self.fault("leadership_transferred");
                    drop(
                        self.cluster.engines[&(group, leader)]
                            .raft_handle()
                            .trigger()
                            .transfer_leader(to)
                            .await,
                    );
                }
                5 => {
                    let node_id = NODES[usize::try_from(self.draw(3)).expect("an index")];
                    self.fault("replica_cut_off");
                    self.cut(node_id);
                }
                6 => {
                    self.heal();
                    self.fault("healed");
                }
                _ => {}
            }
        }
    }

    async fn install(&mut self) {
        for _ in 0..2 {
            self.round().await;
        }
        let down = self.follower(0).await;
        self.fault("follower_stopped");
        self.stop(down).await;
        // Group 0 takes writes the stopped follower misses.
        let stream = self.create(0, BUCKETS[0], false).await;
        self.append(&stream).await;
        for _ in 0..2 {
            self.round().await;
        }
        let before = self.cluster.full_snapshots_to(0, down);
        // The others snapshot and purge their logs, so the stopped
        // follower can only catch up by a snapshot.
        for group in JOURNAL_GROUPS {
            for node_id in self.running(group) {
                self.cluster
                    .snapshot_and_purge_node(group, node_id, 0)
                    .await;
            }
        }
        self.fault("logs_purged");
        self.restart(down).await;
        self.wait_caught_up(0).await;
        self.wait_caught_up(1).await;
        if self.cluster.full_snapshots_to(0, down) <= before {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                TOTALS_INVARIANT,
                "follower_restarted",
                format!("{context}: node {down} caught up without installing a snapshot"),
            );
        }
        self.fault("snapshot_installed");
        // Read at once, before the consumer reads the installed replica.
        self.checkpoint("snapshot_installed").await;
        for _ in 2..ROUNDS {
            self.round().await;
        }
    }

    async fn power_cut(&mut self) {
        for _ in 0..3 {
            self.round().await;
        }
        let node_id = NODES[usize::try_from(self.draw(3)).expect("an index")];
        for group in JOURNAL_GROUPS {
            self.cluster
                .operator_snapshot(group, node_id, Duration::from_secs(10))
                .await
                .unwrap_or_else(|stage| panic!("{}: node {node_id}: {stage}", self.context));
        }
        self.stop(node_id).await;
        self.cluster.wals[&node_id]
            .power_loss_losing_unsynced()
            .await;
        self.fault("power_lost_after_snapshot");
        self.restart(node_id).await;
        let deadline = madsim::time::Instant::now() + Duration::from_secs(2);
        while metrics(&self.cluster, 0, node_id).snapshot.is_none() {
            assert!(
                madsim::time::Instant::now() < deadline,
                "{}: node {node_id} restored no snapshot",
                self.context
            );
            madsim::time::sleep(Duration::from_millis(5)).await;
        }
        self.wait_caught_up(0).await;
        self.wait_caught_up(1).await;
        self.checkpoint("snapshot_restored").await;
        for _ in 3..ROUNDS {
            self.round().await;
        }
    }

    async fn lagging(&mut self) {
        for _ in 0..2 {
            self.round().await;
        }
        let lagging = self.follower(0).await;
        // It holds everything before the cut, so a relisting it answers
        // from its older state misses only the stream created next.
        self.wait_caught_up(0).await;
        self.fault("follower_cut_off");
        self.cut(lagging);
        // A stream the cut-off follower never sees, which the consumer
        // reads and holds.
        let stream = self.create(0, BUCKETS[0], false).await;
        self.append(&stream).await;
        if !self
            .read_group(false, 0, 10_000, Route::Avoid(lagging))
            .await
        {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "follower_cut_off",
                format!("{context}: the others never listed the group"),
            );
        }
        // A relisting whose first page comes from the others and whose
        // later pages go to the cut-off follower first. Pages of one item:
        // the first is a row, and group 0 holds at least the new stream
        // besides, so the relisting always has a later page.
        self.meter
            .groups
            .get_mut(&0)
            .expect("a metered group")
            .request_relisting();
        let fired_before = ursula_stream::sim_controls::fired(Defence::UsageReadOrder);
        if !self.read_group(false, 0, 1, Route::LaterTo(lagging)).await {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "relisting_on_a_lagging_replica",
                format!("{context}: the relisting never completed"),
            );
        }
        // Rule 4 answered the relisting's later pages `behind`.
        if ursula_stream::sim_controls::fired(Defence::UsageReadOrder) <= fired_before {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "relisting_on_a_lagging_replica",
                format!("{context}: node {lagging} never answered the relisting behind"),
            );
        }
        self.round().await;
        self.heal();
        self.fault("healed");
        self.wait_caught_up(0).await;
        self.wait_caught_up(1).await;
        self.checkpoint("healed").await;
        for _ in 4..ROUNDS {
            self.round().await;
        }
    }

    async fn purge_and_expiry(&mut self) {
        // Streams that expire, written at once, and a stream of the same
        // bucket that does not, in each group. The writers never delete the
        // kept one, so it is live when the purge runs.
        for group in JOURNAL_GROUPS {
            let expiring = self.create(group, PURGED, true).await;
            self.append(&expiring).await;
            let kept = self.create(group, PURGED, false).await;
            if let Some(version) = self.versions.get_mut(&kept.stream_id) {
                version.purge_target = true;
            }
            self.append(&kept).await;
        }
        self.poll_round().await;
        // Past the expiry, the clock grace and two sweeps.
        let until = madsim::time::Instant::now()
            + TTL
            + expiry_sweep::CLOCK_GRACE
            + expiry_sweep::interval(DRIVERS.max_age) * 3;
        while madsim::time::Instant::now() < until {
            self.round().await;
            self.pause(Duration::from_millis(500)).await;
        }
        self.poll_until_complete(false, "streams_expired").await;
        // Each group's leader swept its own expiring stream, which no
        // writer deletes, and the consumer listed it expired.
        for group in JOURNAL_GROUPS {
            let expired = self
                .meter
                .group(group)
                .ends
                .values()
                .filter(|end| **end == StreamUsageEnd::Expired)
                .count();
            if expired == 0 {
                let context = self.context.clone();
                fail(
                    &mut self.trace,
                    ACCOUNTING_INVARIANT,
                    "streams_expired",
                    format!("{context}: group {group}: the consumer listed no expired version"),
                );
            }
        }
        // The purge, stamped once for every group.
        let now_ms = self.wall_ms();
        let mut indexes = Vec::new();
        let mut removed = BTreeMap::new();
        for group in JOURNAL_GROUPS {
            let deadline = madsim::time::Instant::now() + WRITE_WAIT;
            // Whether a purge sent before the answered one failed with an
            // outcome unknown (a lost answer or a `503`): it may have
            // removed the streams, so the answer counts none of them. A
            // refusal that applied nothing excuses no undercount.
            let mut interrupted = false;
            let response = loop {
                if let Some(leader) = self.leader(group)
                    && let Some(engine) = self.cluster.engines.get_mut(&(group, leader))
                {
                    match engine
                        .purge_bucket(
                            PurgeBucketRequest {
                                bucket_id: PURGED.to_owned(),
                                now_ms,
                            },
                            group_placement(group),
                        )
                        .await
                    {
                        Ok(response) => break response,
                        Err(error) => interrupted |= !error.applied_nothing(),
                    }
                }
                self.retry(deadline, "the purge").await;
            };
            removed.insert(group, (response.removed_streams, interrupted));
            indexes.push(GroupPurgeIndex {
                raft_group_id: group,
                commit_index: response.group_commit_index,
                history: HistoryId::new(response.history),
            });
        }
        self.purged = true;
        for version in self.versions.values_mut() {
            if version.stream.bucket_id == PURGED {
                version.writable = false;
            }
        }
        self.fault("bucket_purged");
        // Rule 12: each group read at or past its purge, in its history.
        let mut complete = false;
        for _ in 0..100 {
            self.poll(false).await;
            if indexes
                .iter()
                .all(|index| self.meter.group(index.raft_group_id).purge_complete(index))
            {
                complete = true;
                break;
            }
            self.pause(Duration::from_millis(200)).await;
        }
        let context = self.context.clone();
        if !complete {
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "bucket_purged",
                format!("{context}: the purge never completed for the consumer: {indexes:?}"),
            );
        }
        for index in &indexes {
            let live = self.meter.group(index.raft_group_id).live_of(PURGED);
            if !live.is_empty() {
                fail(
                    &mut self.trace,
                    ACCOUNTING_INVARIANT,
                    "bucket_purged",
                    format!(
                        "{context}: group {}: the consumer holds {live:?} live once the purge \
                         completed",
                        index.raft_group_id
                    ),
                );
            }
        }
        // Each group's purge removed its kept stream, and the consumer
        // listed it purged. `removed_streams` counts only what the answered
        // request removed, so after a try whose outcome is unknown only the
        // listing shows the removal.
        for group in JOURNAL_GROUPS {
            let (removed, interrupted) = removed.get(&group).copied().unwrap_or_default();
            let purged = self
                .meter
                .group(group)
                .ends
                .values()
                .filter(|end| **end == StreamUsageEnd::Purged)
                .count();
            if (removed == 0 && !interrupted) || purged == 0 {
                fail(
                    &mut self.trace,
                    ACCOUNTING_INVARIANT,
                    "bucket_purged",
                    format!(
                        "{context}: group {group}: the purge removed {removed} streams, \
                         {purged} listed purged"
                    ),
                );
            }
        }
        for _ in 0..3 {
            self.round().await;
        }
    }

    async fn skip_past_retention(&mut self) {
        for _ in 0..2 {
            self.round().await;
        }
        // A version the consumer reads, then written and deleted unread.
        let group = JOURNAL_GROUPS[usize::try_from(self.draw(2)).expect("an index")];
        let read = self.create(group, BUCKETS[0], false).await;
        self.append(&read).await;
        self.poll_until_complete(false, "version_read").await;
        self.append(&read).await;
        self.delete(&read).await;
        // One created, written and deleted unread.
        let unread = self.ephemeral().await;
        for stream in [&read, &unread] {
            if let Some(version) = self.versions.get_mut(&stream.stream_id) {
                version.ended_unread = true;
            }
        }
        self.fault("reads_skipped");
        self.pause(DRIVERS.max_age / 2 + expiry_sweep::interval(DRIVERS.max_age) * 4)
            .await;
        self.poll_until_complete(false, "reads_resumed").await;
        if self.meter.relists(RelistReason::Retired) == 0 {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "reads_resumed",
                format!("{context}: the group never relisted `retired`"),
            );
        }
        for _ in 2..ROUNDS {
            self.round().await;
        }
    }

    async fn accepted_loss(&mut self) {
        for _ in 0..3 {
            self.round().await;
        }
        self.poll_until_complete(false, "before_the_loss").await;
        let cut = NODES[usize::try_from(self.draw(3)).expect("an index")];
        lead_every_group(&self.cluster, cut, &self.context).await;
        // Everything so far is durable on every node: only what the others
        // take from here on can be lost.
        for wal in self.cluster.wals.values() {
            wal.write_back();
        }
        self.cut(cut);
        self.fault("leader_cut_off");
        for group in JOURNAL_GROUPS {
            let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
            while self.leader(group).is_none() {
                assert!(
                    madsim::time::Instant::now() < deadline,
                    "{}: group {group}: the others elected no leader",
                    self.context
                );
                madsim::time::sleep(STEP).await;
            }
        }
        let others: Vec<u64> = NODES
            .into_iter()
            .filter(|node_id| *node_id != cut)
            .collect();
        // The window: writes the others acknowledge and the consumer reads.
        self.window = true;
        for group in JOURNAL_GROUPS {
            let stream = self.create(group, BUCKETS[0], false).await;
            self.append(&stream).await;
            self.append(&stream).await;
        }
        // On half of the seeds the others write the window's creates and
        // first appends back, so those survive the power cut, their logs
        // end in the window's term, and the plan keeps the longest log
        // without a cut. Otherwise they lose the whole window, and the plan
        // cuts to the committed markers. The drawn appends are lost either
        // way.
        if self.draw(2) == 0 {
            for node_id in &others {
                self.cluster.wals[node_id].write_back();
            }
            self.fault("window_prefix_written_back");
        }
        for _ in 0..3 {
            if let Some(stream) = self.draw_writable() {
                self.append(&stream).await;
            }
        }
        self.window = false;
        // The consumer reads the window from the others, which applied it.
        self.poll_until_complete_via(false, "window_read", Route::Avoid(cut))
            .await;
        // The others lose power and every page written since the last
        // write-back: no snapshot driver runs and no segment fills, so
        // nothing synced their journals since.
        for node_id in &others {
            self.stop(*node_id).await;
        }
        for node_id in &others {
            self.cluster.wals[node_id]
                .power_loss_losing_unsynced()
                .await;
        }
        for node_id in &others {
            self.restart(*node_id).await;
        }
        self.heal();
        self.fault("quorum_lost_power");
        // The groups stall until the operator plans.
        madsim::time::sleep(RECOVERY_STALL_AFTER.saturating_mul(2)).await;
        let mut points = Vec::new();
        for group in JOURNAL_GROUPS {
            if leader_by_state(&self.cluster, group).is_some() {
                continue;
            }
            let plan = recover_group(&mut self.cluster, group, &NODES, &self.context).await;
            points.push((group, plan.point));
        }
        if points.is_empty() {
            panic!("{}: no group stalled after the power loss", self.context);
        }
        for (group, point) in &points {
            wait_leader(&self.cluster, *group, &self.context).await;
            wait_plan_satisfied(&self.cluster, *group, &NODES, *point, &self.context).await;
        }
        self.fault("loss_accepted");
        self.lost = self.lost_write_units().await;
        if self.lost == 0 {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "loss_accepted",
                format!("{context}: the accepted loss lost nothing the consumer listed"),
            );
        }
        // The writers of the new history are new producers. Each live
        // stream is written before the consumer reads it again, so its
        // first write after the event records its baseline.
        self.generation = self.generation.saturating_add(1);
        for stream in self.writable() {
            if self.versions[&stream.stream_id].created_in_window
                && !self.stream_exists(&stream).await
            {
                if let Some(version) = self.versions.get_mut(&stream.stream_id) {
                    version.writable = false;
                }
                continue;
            }
            self.append(&stream).await;
        }
        self.poll_until_complete(false, "loss_accepted").await;
        if self.meter.history_changes() == 0 {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "loss_accepted",
                format!("{context}: the consumer never relisted `history`"),
            );
        }
        for _ in 3..ROUNDS {
            self.round().await;
        }
    }

    /// Whether `stream`'s group's leader holds it.
    async fn stream_exists(&mut self, stream: &BucketStreamId) -> bool {
        self.read_stream(stream).await.is_some()
    }

    /// `stream`'s bytes on its group's leader, `None` once it is gone.
    async fn read_stream(&mut self, stream: &BucketStreamId) -> Option<Vec<u8>> {
        let group = self.versions[&stream.stream_id].group;
        let leader = wait_leader(&self.cluster, group, &self.context).await;
        let read = self.cluster.engines[&(group, leader)]
            .sim_read_local_stream(
                ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 1 << 22,
                    now_ms: self.wall_ms(),
                    leader_only: false,
                    read_index: None,
                    follower: FollowerRead::Prefix,
                },
                group_placement(group),
            )
            .await;
        read.ok().map(|read| read.payload.to_vec())
    }

    async fn restore(&mut self) {
        for _ in 0..4 {
            self.round().await;
        }
        // The consumer reads the newest state before the backup.
        self.poll_until_complete_via(false, "before_the_backup", Route::All)
            .await;
        // Back both groups up, as `ursulactl backup` does through each
        // group's export.
        let mut backups = Vec::new();
        for group in JOURNAL_GROUPS {
            let deadline = madsim::time::Instant::now() + WRITE_WAIT;
            let snapshot = loop {
                if let Some(leader) = self.leader(group)
                    && let Some(engine) = self.cluster.engines.get_mut(&(group, leader))
                    && let Ok(snapshot) = engine.snapshot(group_placement(group)).await
                {
                    break snapshot;
                }
                self.retry(deadline, "the backup").await;
            };
            let bytes = ursula_stream::msgpack::encode(&snapshot.stream_snapshot)
                .expect("encode the backup");
            backups.push((group, bytes));
        }
        for version in self.versions.values_mut() {
            version.before_restore = version.counters;
        }
        // Every node loses its disk: the restored cluster's groups are new.
        for node_id in NODES {
            self.cluster.stop_node(node_id).await;
            self.cluster.new_disk(
                node_id,
                &format!("stream-usage-{}-restored-{node_id}", self.seed),
                self.tuning,
            );
        }
        for node_id in NODES {
            self.cluster.start_node(node_id).await;
        }
        for group in JOURNAL_GROUPS {
            self.cluster.leader(group).await;
        }
        self.cluster.wait_gates_open(Duration::from_secs(10)).await;
        self.fault("nodes_wiped");
        if self.controls.start_before_import {
            self.fresh = Some(UsageMeter::new(JOURNAL_GROUPS, retention()));
            self.poll_until_complete(true, "nodes_wiped").await;
        }
        for (group, bytes) in backups {
            let snapshot =
                ursula_stream::decode_backup_snapshot(&bytes).expect("decode the backup");
            let request = ImportGroupStateRequest {
                snapshot: Box::new(snapshot),
                history: ursula_runtime::draw_history_id(),
                backup_digest: *blake3::hash(&bytes).as_bytes(),
                now_ms: self.wall_ms(),
            };
            let deadline = madsim::time::Instant::now() + WRITE_WAIT;
            loop {
                if let Some(leader) = self.leader(group)
                    && let Some(engine) = self.cluster.engines.get_mut(&(group, leader))
                {
                    match engine
                        .import_group_state(request.clone(), group_placement(group))
                        .await
                    {
                        Ok(_) => break,
                        Err(error) if error.code() == Some(StreamErrorCode::ImportConflict) => {
                            panic!(
                                "{}: group {group}: the import conflicts: {error}",
                                self.context
                            )
                        }
                        Err(_retry) => {}
                    }
                }
                self.retry(deadline, "the import").await;
            }
            self.wait_caught_up(group).await;
        }
        self.generation = self.generation.saturating_add(1);
        self.fault("backups_imported");
        if !self.controls.start_before_import {
            // Started once the restore finished, as operations says.
            self.fresh = Some(UsageMeter::new(JOURNAL_GROUPS, retention()));
            self.poll_until_complete_via(true, "backups_imported", Route::All)
                .await;
        }
        self.poll_until_complete(false, "backups_imported").await;
        if self.meter.history_changes() < 2 {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "backups_imported",
                format!("{context}: the consumer did not relist both groups `history`"),
            );
        }
        for _ in 4..ROUNDS {
            self.round().await;
        }
    }

    /// The end of every run: the faults healed, a version created, written
    /// and deleted, reads every second through the fold of its end record,
    /// a checkpoint, a complete poll and a requested relisting in one page,
    /// and the accounting judged.
    async fn finish(mut self) -> ThreeNodeRaftSimOutcome {
        self.heal();
        for node_id in std::mem::take(&mut self.stopped) {
            self.restart(node_id).await;
        }
        self.ephemeral().await;
        let until = madsim::time::Instant::now()
            + DRIVERS.max_age / 2
            + expiry_sweep::interval(DRIVERS.max_age) * 4;
        while madsim::time::Instant::now() < until {
            self.pause(Duration::from_secs(1)).await;
            self.poll_round().await;
        }
        self.wait_caught_up(0).await;
        self.wait_caught_up(1).await;
        self.checkpoint("writes_stopped").await;
        for fresh in [false, true] {
            if fresh && self.fresh.is_none() {
                continue;
            }
            self.poll_until_complete(fresh, "writes_stopped").await;
            for group in JOURNAL_GROUPS {
                self.meter_mut(fresh)
                    .groups
                    .get_mut(&group)
                    .expect("a metered group")
                    .request_relisting();
                if !self.read_group(fresh, group, 10_000, Route::Drawn).await {
                    let context = self.context.clone();
                    fail(
                        &mut self.trace,
                        ACCOUNTING_INVARIANT,
                        "relisting_requested",
                        format!("{context}: group {group}'s requested relisting never completed"),
                    );
                }
            }
        }
        if self.meter.folds() == 0 {
            let context = self.context.clone();
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "relisting_requested",
                format!("{context}: the consumer saw no end record fold"),
            );
        }
        self.judge_accounting();
        let meter = &self.meter;
        let event = SimEvent::StreamUsageBilled {
            script: self.script.name().to_owned(),
            versions: u64::try_from(self.versions.len()).expect("a count"),
            pages: meter.pages,
            behind: meter.behind,
            relists_retired: meter.relists(RelistReason::Retired),
            relists_history: meter.relists(RelistReason::History),
            history_changes: meter.history_changes(),
            folds: meter.folds(),
            replicas_judged: self.judged,
            lost_write_units: self.lost,
        };
        self.trace.push(event);
        ThreeNodeRaftSimOutcome {
            seed: self.seed,
            leader_id: leader_by_state(&self.cluster, 0).unwrap_or(0),
            target_node_id: None,
            appended_log_index: metrics(&self.cluster, 0, NODES[0])
                .last_applied
                .map_or(0, |log_id| log_id.index),
            trace: self.trace,
        }
    }

    /// `usage_versions_bill_exactly` at the end of the run.
    fn judge_accounting(&mut self) {
        let context = self.context.clone();
        let mut failures = Vec::new();
        // Per version.
        for version in self.versions.values() {
            let metered = self.meter.group(version.group);
            let accounted =
                metered.accounted_to(&version.stream.bucket_id, &version.stream.stream_id);
            let unread = metered
                .closed_unlisted_stream(&version.stream.bucket_id, &version.stream.stream_id);
            let within = accounted.committed_append_bytes
                <= version.counters.committed_append_bytes
                && accounted.committed_records <= version.counters.committed_records
                && accounted.committed_write_units <= version.counters.committed_write_units
                && accounted.appends <= version.counters.appends;
            let exact = accounted == version.counters;
            if !(exact || (version.ended_unread && within)) {
                let above = accounted.committed_append_bytes
                    >= version.counters.committed_append_bytes
                    && accounted.committed_records >= version.counters.committed_records
                    && accounted.committed_write_units >= version.counters.committed_write_units
                    && accounted.appends >= version.counters.appends;
                let relation = if above {
                    "above"
                } else if within {
                    "below"
                } else {
                    "apart from"
                };
                failures.push(format!(
                    "{} accounted {accounted:?}, {relation} what its writes committed {:?} \
                     (written across a history change: {}, ended unread: {}, folded unread: \
                     {unread})",
                    version.stream,
                    version.counters,
                    version.written_across_history,
                    version.ended_unread
                ));
            }
            if unread && !version.ended_unread {
                failures.push(format!(
                    "{}'s end record folded before the consumer read it, though the consumer \
                     read within the retention while it ended",
                    version.stream
                ));
            }
        }
        // Per group and bucket.
        for group in JOURNAL_GROUPS {
            let metered = self.meter.group(group);
            let mut expected = BTreeMap::<String, _>::new();
            for version in self
                .versions
                .values()
                .filter(|version| version.group == group)
            {
                add_committed(
                    expected
                        .entry(version.stream.bucket_id.clone())
                        .or_default(),
                    committed(&version.counters),
                );
            }
            let buckets: BTreeSet<String> =
                expected.keys().cloned().chain(metered.buckets()).collect();
            for bucket_id in buckets {
                let accounted = metered.accounted_to_bucket(&bucket_id);
                let wanted = expected.get(&bucket_id).copied().unwrap_or_default();
                if accounted != wanted {
                    failures.push(format!(
                        "group {group}: bucket {bucket_id} accounted {accounted:?}, its versions \
                         committed {wanted:?}"
                    ));
                }
            }
        }
        // The restore's second consumer: what was written after the import.
        if let Some(fresh) = &self.fresh {
            for group in JOURNAL_GROUPS {
                let metered = fresh.group(group);
                let mut expected = BTreeMap::<String, _>::new();
                for version in self
                    .versions
                    .values()
                    .filter(|version| version.group == group)
                {
                    let mut after = version.counters;
                    after.committed_append_bytes = after
                        .committed_append_bytes
                        .saturating_sub(version.before_restore.committed_append_bytes);
                    after.committed_records = after
                        .committed_records
                        .saturating_sub(version.before_restore.committed_records);
                    after.committed_write_units = after
                        .committed_write_units
                        .saturating_sub(version.before_restore.committed_write_units);
                    add_committed(
                        expected
                            .entry(version.stream.bucket_id.clone())
                            .or_default(),
                        committed(&after),
                    );
                }
                let buckets: BTreeSet<String> =
                    expected.keys().cloned().chain(metered.buckets()).collect();
                for bucket_id in buckets {
                    let accounted = metered.accounted_to_bucket(&bucket_id);
                    let wanted = expected.get(&bucket_id).copied().unwrap_or_default();
                    if accounted != wanted {
                        failures.push(format!(
                            "the restore's second consumer: group {group}: bucket {bucket_id} \
                             accounted {accounted:?}, the writes after the import committed \
                             {wanted:?}"
                        ));
                    }
                }
            }
        }
        if let Some(failure) = failures.into_iter().next() {
            fail(
                &mut self.trace,
                ACCOUNTING_INVARIANT,
                "relisting_requested",
                format!("{context}: {failure}"),
            );
        }
    }

    /// The write units an accepted loss's window acknowledged that the
    /// lineage the plan kept does not hold.
    async fn lost_write_units(&mut self) -> u64 {
        let mut lost = 0_u64;
        let windowed: Vec<BucketStreamId> = self
            .versions
            .values()
            .filter(|version| !version.window.is_empty() || version.created_in_window)
            .map(|version| version.stream.clone())
            .collect();
        for stream in windowed {
            let bytes = self.read_stream(&stream).await;
            let version = &self.versions[&stream.stream_id];
            for (body, counters) in &version.window {
                let held = bytes.as_ref().is_some_and(|bytes| {
                    bytes
                        .windows(body.len())
                        .any(|window| window == body.as_slice())
                });
                if !held {
                    lost = lost.saturating_add(counters.committed_write_units);
                }
            }
            if version.created_in_window && bytes.is_none() {
                lost = lost.saturating_add(created().committed_write_units);
            }
        }
        lost
    }
}

/// The nodes' end-record retention: half the bound.
fn retention() -> Duration {
    ursula_config::RaftConfig::end_record_retention(DRIVERS.max_age)
}

/// The answers of `nodes`' replicas of `group` to one read with `cursor`
/// and `limit`: each replica's read built and rendered by the route's own
/// conversions, and sent through its JSON. A replica that refuses the read
/// answers nothing, as the route would answer it `unavailable`.
async fn read_answers(
    cluster: &mut JournalCluster,
    group: u32,
    nodes: &[u64],
    cursor: UsageCursor,
    limit: NonZeroUsize,
) -> Vec<GroupStreamUsage> {
    let read = ursula::stream_usage_read(cursor, limit);
    let mut answers = Vec::new();
    for node_id in nodes {
        let Some(engine) = cluster.engines.get_mut(&(group, *node_id)) else {
            continue;
        };
        let Ok(view) = engine
            .stream_usage(read.clone(), group_placement(group))
            .await
        else {
            continue;
        };
        let answer = ursula::group_usage_answer(RaftGroupId(group), view);
        let json = serde_json::to_vec(&answer).expect("render the answer");
        answers.push(serde_json::from_slice(&json).expect("read the answer"));
    }
    answers
}

/// `node_id`'s checkpoint read of `group`: a `start` relisting in one page,
/// `/__ursula/usage`, and the relisting again, kept once the two pages are
/// equal and read at the index `/__ursula/usage` was.
async fn replica_read(
    cluster: &mut JournalCluster,
    group: u32,
    node_id: u64,
) -> Option<ReplicaRead> {
    let limit = NonZeroUsize::new(10_000).expect("a page limit");
    let listed = async |cluster: &mut JournalCluster| -> Option<GroupUsagePage> {
        let answers = read_answers(cluster, group, &[node_id], UsageCursor::Start, limit).await;
        match answers.into_iter().next()? {
            GroupStreamUsage::Listed(page) if page.page == UsagePage::Complete => Some(page),
            _ => None,
        }
    };
    for _ in 0..20 {
        let first = listed(cluster).await;
        let totals = match cluster.engines.get_mut(&(group, node_id)) {
            Some(engine) => engine.bucket_usage(group_placement(group)).await.ok(),
            None => return None,
        };
        let second = listed(cluster).await;
        if let (Some(first), Some(totals), Some(second)) = (first, totals, second)
            && first == second
            && first.applied.index == totals.applied_index
        {
            return Some(ReplicaRead {
                node_id,
                page: first,
                totals,
            });
        }
        madsim::time::sleep(STEP).await;
    }
    None
}

/// Where one replica's read breaks `usage_totals_equal_their_parts`: a
/// row's totals that are not its retired usage plus its versions, or
/// `/__ursula/usage` that differs from the rows.
fn parts_disagree(read: &ReplicaRead) -> Vec<String> {
    let mut failures = Vec::new();
    let page = &read.page;
    let mut rows = BTreeMap::new();
    for row in &page.rows {
        let mut parts = row.retired;
        let mut retained_bytes = 0_u64;
        let mut snapshot_bytes = 0_u64;
        let mut stream_count = 0_u64;
        for entry in page
            .entries
            .iter()
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
        let totals = (
            row.totals.committed_append_bytes,
            row.totals.committed_records,
            row.totals.committed_write_units,
            row.totals.retained_bytes,
            row.totals.stream_count,
            row.totals.snapshot_bytes.unwrap_or(0),
        );
        let derived = (
            parts.committed_append_bytes,
            parts.committed_records,
            parts.committed_write_units,
            retained_bytes,
            stream_count,
            snapshot_bytes,
        );
        if totals != derived {
            failures.push(format!(
                "node {} at {:?}: bucket {}'s totals {totals:?} are not its retired usage plus \
                 its versions {derived:?}",
                read.node_id, page.applied, row.bucket_id
            ));
        }
        rows.insert(row.bucket_id.clone(), totals);
    }
    let v2: BTreeMap<String, _> = read
        .totals
        .buckets
        .iter()
        .map(|bucket| {
            let usage = &bucket.usage;
            (
                bucket.bucket_id.clone(),
                (
                    usage.committed_append_bytes,
                    usage.committed_records,
                    usage.committed_write_units,
                    usage.retained_bytes,
                    usage.stream_count,
                    read.totals
                        .snapshot_bytes
                        .get(&bucket.bucket_id)
                        .copied()
                        .unwrap_or(0),
                ),
            )
        })
        .collect();
    let buckets: BTreeSet<&String> = rows.keys().chain(v2.keys()).collect();
    for bucket_id in buckets {
        let row = rows.get(bucket_id).copied().unwrap_or_default();
        let total = v2.get(bucket_id).copied().unwrap_or_default();
        if row != total {
            failures.push(format!(
                "node {} at {:?}: bucket {bucket_id}'s /__ursula/usage totals {total:?} are not \
                 its relisting's {row:?}",
                read.node_id, page.applied
            ));
        }
    }
    failures
}

pub(super) async fn run_stream_usage_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls, Script::of(config.seed));
    let mut run = Run::start(&config, controls).await;
    // The baseline: a `start` listing of every group's newest state before
    // the first write, so every unit the writers commit is accounted after
    // it.
    run.wait_caught_up(0).await;
    run.wait_caught_up(1).await;
    run.poll_until_complete_via(false, "baseline", Route::All)
        .await;
    // A version the consumer reads live, then deleted, and one created,
    // written and deleted between the same two reads.
    let group = JOURNAL_GROUPS[usize::try_from(run.draw(2)).expect("an index")];
    let held = run.create(group, BUCKETS[0], false).await;
    run.append(&held).await;
    run.poll_until_complete(false, "version_read").await;
    run.ephemeral().await;
    run.delete(&held).await;
    run.poll_until_complete(false, "ephemeral_version").await;
    match run.script {
        Script::Churn => run.churn().await,
        Script::Install => run.install().await,
        Script::PowerCut => run.power_cut().await,
        Script::Lagging => run.lagging().await,
        Script::PurgeAndExpiry => run.purge_and_expiry().await,
        Script::SkipPastRetention => run.skip_past_retention().await,
        Script::AcceptedLoss => run.accepted_loss().await,
        Script::Restore => run.restore().await,
    }
    run.finish().await
}

#[cfg(test)]
mod tests {
    use super::ACCOUNTING_INVARIANT;
    use super::Controls;
    use super::SCRIPTS;
    use super::Script;
    use super::TOTALS_INVARIANT;
    use crate::madsim_harness::STREAM_USAGE_EXTENDED_SEEDS;
    use crate::madsim_harness::STREAM_USAGE_SEEDS;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimReport;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::StreamUsageBillsExactly;

    fn run(seed: u64, controls: Controls) -> SimReport {
        ThreeNodeRaftSim::run_stream_usage_with_controls_report(
            ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug())),
            controls,
        )
    }

    /// The seed of the family's default range that runs `script`.
    fn seed_of(script: Script) -> u64 {
        STREAM_USAGE_SEEDS
            .clone()
            .find(|seed| Script::of(*seed) == script)
            .expect("the default range runs every script")
    }

    /// What the run's summary reports, if it passed.
    fn billed(report: &SimReport) -> Option<&SimEvent> {
        report
            .outcome
            .trace
            .events
            .iter()
            .find(|event| matches!(event, SimEvent::StreamUsageBilled { .. }))
    }

    /// The family's default range runs every script once, and its
    /// extended range four times.
    #[test]
    fn stream_usage_seeds_cover_every_script() {
        for script in SCRIPTS {
            let runs = |seeds: std::ops::RangeInclusive<u64>| {
                seeds.filter(|seed| Script::of(*seed) == script).count()
            };
            assert_eq!(runs(STREAM_USAGE_SEEDS), 1, "{script:?}");
            assert_eq!(runs(STREAM_USAGE_EXTENDED_SEEDS), 4, "{script:?}");
        }
    }

    /// Every script bills exactly, the same way twice per seed, and each
    /// reaches what it checks.
    #[test]
    fn stream_usage_bills_exactly_replays() {
        let _guard = sim_test_guard();
        for seed in STREAM_USAGE_SEEDS {
            let first = run(seed, Controls::default());
            let second = run(seed, Controls::default());
            assert_eq!(first, second, "seed {seed}");
            let Some(SimEvent::StreamUsageBilled {
                behind,
                relists_retired,
                history_changes,
                folds,
                replicas_judged,
                lost_write_units,
                ..
            }) = billed(&first)
            else {
                panic!("seed {seed}: {:?}", first.outcome.trace.events.last());
            };
            assert!(*folds > 0 && *replicas_judged > 0, "seed {seed}");
            match Script::of(seed) {
                Script::Lagging => assert!(*behind > 0, "seed {seed}"),
                Script::SkipPastRetention => assert!(*relists_retired > 0, "seed {seed}"),
                Script::AcceptedLoss => {
                    assert!(*history_changes > 0 && *lost_write_units > 0, "seed {seed}");
                }
                Script::Restore => assert!(*history_changes > 0, "seed {seed}"),
                _ => {}
            }
        }
    }

    /// Runs `seed` under `controls`, which must fail `invariant` after
    /// `phase`, and returns the failure's message and the failed run's
    /// trace.
    fn assert_fails(
        seed: u64,
        controls: Controls,
        invariant: &str,
        phase: &str,
    ) -> (String, SimTrace) {
        assert!(
            billed(&run(seed, Controls::default())).is_some(),
            "seed {seed} passes with every defence on"
        );
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| run(seed, controls));
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{controls:?} left seed {seed} passing");
        let trace = SimTrace::last_recorded();
        assert!(
            invariant_failed(&trace, invariant),
            "{controls:?} did not fail `{invariant}` on seed {seed}: {:?}",
            trace.events.last()
        );
        let message = trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::InvariantFailed {
                    after_event,
                    message,
                    ..
                } => {
                    assert_eq!(after_event, phase, "{controls:?}: {message}");
                    Some(message.clone())
                }
                _ => None,
            })
            .unwrap_or_default();
        (message, trace)
    }

    /// Without end records, a removal folds the version's usage straight
    /// into its bucket: the version created, written and deleted between
    /// two reads is never listed, and the consumer holds a removed version
    /// live.
    #[test]
    fn negative_control_a_version_without_its_end_record_is_never_billed() {
        let _guard = sim_test_guard();
        let (message, _) = assert_fails(
            seed_of(Script::Churn),
            Controls::END_RECORDS_OFF,
            ACCOUNTING_INVARIANT,
            "page_kept",
        );
        assert!(message.contains("live versions"), "{message}");
    }

    /// An install that restores the group snapshot as a backup resets each
    /// version on the follower that installed it, which lists them below
    /// what the others list at the same applied entry.
    #[test]
    fn negative_control_an_install_without_its_usage_resets_every_version() {
        let _guard = sim_test_guard();
        let (message, trace) = assert_fails(
            seed_of(Script::Install),
            Controls::SNAPSHOT_USAGE_OFF,
            TOTALS_INVARIANT,
            "snapshot_installed",
        );
        // The install script stops the follower that installs before any
        // other node.
        let installed = trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::NodeStopped { node_id } => Some(*node_id),
                _ => None,
            })
            .expect("the install script stops a follower");
        assert!(message.contains("list different pages"), "{message}");
        assert!(
            message.contains(&format!("nodes {installed} and"))
                || message.contains(&format!("and {installed} list")),
            "node {installed}: {message}"
        );
    }

    /// Without the baseline of the first write after the accepted loss, the
    /// consumer lowers each version's level to zero and accounts what the
    /// final lineage kept a second time.
    #[test]
    fn negative_control_a_write_without_its_baseline_bills_the_kept_prefix_again() {
        let _guard = sim_test_guard();
        let (message, _) = assert_fails(
            seed_of(Script::AcceptedLoss),
            Controls::HISTORY_BASELINE_OFF,
            ACCOUNTING_INVARIANT,
            "relisting_requested",
        );
        assert!(
            message.contains("above what its writes committed")
                && message.contains("written across a history change: true"),
            "{message}"
        );
    }

    /// A cut-off follower that answers a relisting's later pages from its
    /// older state misses the version the consumer holds, whose level the
    /// consumer then takes out of its bucket's share.
    #[test]
    fn negative_control_a_relisting_continued_on_a_lagging_replica_misses_a_version() {
        let _guard = sim_test_guard();
        let (message, _) = assert_fails(
            seed_of(Script::Lagging),
            Controls::READ_ORDER_OFF,
            ACCOUNTING_INVARIANT,
            "page_kept",
        );
        assert!(
            message.contains("bucket tenant-a's share is negative"),
            "{message}"
        );
    }

    /// A consumer that starts on the restored nodes before the import
    /// takes an empty baseline, and the import's relisting accounts the
    /// backup's totals, which the cluster's consumer accounted before, a
    /// second time.
    #[test]
    fn negative_control_a_consumer_started_before_the_import_bills_the_backup_twice() {
        let _guard = sim_test_guard();
        let (message, _) = assert_fails(
            seed_of(Script::Restore),
            Controls::START_BEFORE_IMPORT,
            ACCOUNTING_INVARIANT,
            "relisting_requested",
        );
        assert!(message.contains("second consumer"), "{message}");
    }
}
