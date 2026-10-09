//! DST coverage of the simulated disk and of the production per-core journal
//! running on it.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use ursula_config::WalFsync;
use ursula_raft::GroupRejoin;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::JournalTuning;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupEngineOptions;
use ursula_raft::RaftGroupFileLogStore;
use ursula_raft::RaftWal;
use ursula_raft::RaftWalError;
use ursula_raft::RecoveryState;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::WalOpening;
use ursula_raft::apply_failure::ApplyFault;
use ursula_raft::wal::diagnostics::CoreJournalError;
use ursula_raft::wal::diagnostics::GroupLogState;
use ursula_raft::wal::diagnostics::JournalDisk;
use ursula_raft::wal::diagnostics::JournalError;
use ursula_raft::wal::diagnostics::JournalFile;
use ursula_raft::wal::diagnostics::JournalOp;
use ursula_raft::wal::diagnostics::JournalReplayMode;
use ursula_raft::wal::diagnostics::LockAttempt;
use ursula_raft::wal::diagnostics::PreviousRun;
use ursula_raft::wal::diagnostics::RUN_STATE_FILE;
use ursula_raft::wal::diagnostics::RecoveryReason;
use ursula_raft::wal::diagnostics::SIM_DISK_PAGE_SIZE;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_raft::wal::diagnostics::SimDiskError;
use ursula_raft::wal::diagnostics::SimDiskFault;
use ursula_raft::wal::diagnostics::journal_segment_path;
use ursula_raft::wal::diagnostics::journal_segments;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::recovery_wiring;
use super::recovery_wiring::VoteLog;
use super::seeds_from_env;
use super::sim_test_guard;
use crate::madsim_harness::run_with_madsim;
use crate::madsim_harness::sim_wal::SimNodeWal;
use crate::madsim_harness::sim_wal::standalone_wal_metrics;

const PAGE: usize = SIM_DISK_PAGE_SIZE;

fn sim_dir(name: &str) -> PathBuf {
    SimDisk::provision_dir(name).expect("provision a simulated directory")
}

fn write_file(path: &Path, bytes: &[u8], sync: bool) {
    let mut file = SimDisk::open_append(path).expect("open simulated file");
    file.append(bytes).expect("append to simulated file");
    if sync {
        file.sync_data().expect("fsync simulated file");
    }
}

fn sim_disk_error(err: &io::Error) -> Option<&SimDiskError> {
    err.get_ref()?.downcast_ref::<SimDiskError>()
}

fn core_journal_error(err: &io::Error) -> Option<&CoreJournalError> {
    err.get_ref()?.downcast_ref::<CoreJournalError>()
}

/// The segment of core 0's journal under the node directory `root` that
/// appends go to, and the one a rotation starts next.
fn active_segments(root: &Path) -> [PathBuf; 2] {
    let core = root.join("core-0");
    let newest = journal_segments(&core)
        .expect("list the journal segments")
        .last()
        .map(|(sequence, _)| *sequence)
        .expect("the journal has a segment");
    [
        journal_segment_path(&core, newest),
        journal_segment_path(&core, newest.saturating_add(1)),
    ]
}

/// Asserts how a node's WAL opened: what it read of the previous run, how it
/// read the journals, and whether its logs may be missing acknowledged
/// entries.
fn assert_opening(
    opening: WalOpening,
    previous_run: PreviousRun,
    replay_mode: JournalReplayMode,
    recovery: RecoveryState,
    context: &str,
) {
    assert_eq!(
        (opening.previous_run, opening.replay_mode, opening.recovery),
        (previous_run, replay_mode, recovery),
        "{context}: {opening:?}"
    );
}

const RECOVERING_AFTER_HOST_CRASH: RecoveryState = RecoveryState::Recovering {
    reason: RecoveryReason::HostCrash,
};

#[test]
fn sim_disk_power_loss_keeps_synced_bytes_and_may_reorder_unsynced_pages() {
    let _guard = sim_test_guard();
    let synced = vec![1_u8; PAGE + 100];
    let unsynced = vec![2_u8; 3 * PAGE];
    let (mut holes, mut whole_tails, mut lost_tails) = (0, 0, 0);
    for seed in 0..32 {
        let (synced_bytes, unsynced_bytes) = (synced.clone(), unsynced.clone());
        let after = run_with_madsim(seed, async move {
            let dir = sim_dir("pages");
            let path = dir.join("data");
            write_file(&path, &synced_bytes, true);
            SimDisk::sync_dir(&dir).expect("make the file durable");
            write_file(&path, &unsynced_bytes, false);
            let report = SimDisk::power_loss(&dir).expect("power loss");
            assert_eq!(report.files, 1);
            assert_eq!(report.kept_pages + report.dropped_pages, 4);
            SimDisk::read(&path).expect("file survives")
        });
        assert_eq!(&after[..synced.len()], &synced[..], "seed {seed}");
        let tail = &after[synced.len()..];
        assert!(tail.len() <= unsynced.len(), "seed {seed}");
        assert!(
            tail.iter().all(|byte| *byte == 2 || *byte == 0),
            "seed {seed}"
        );
        if tail.contains(&0) {
            holes += 1;
        } else if tail.len() == unsynced.len() {
            whole_tails += 1;
        } else if tail.is_empty() {
            lost_tails += 1;
        }
    }
    assert!(
        holes > 0,
        "a later page must sometimes survive an earlier one"
    );
    assert!(
        whole_tails > 0,
        "every unsynced page must sometimes survive"
    );
    assert!(lost_tails > 0, "every unsynced page must sometimes be lost");
}

#[test]
fn sim_disk_process_crash_keeps_the_page_cache() {
    let _guard = sim_test_guard();
    run_with_madsim(1, async {
        let dir = sim_dir("process-crash");
        let path = dir.join("data");
        write_file(&path, b"unsynced", false);
        SimDisk::process_crash(&dir).expect("crash a stopped process");
        assert_eq!(SimDisk::read(&path).expect("file survives"), b"unsynced");
    });
}

#[test]
fn sim_disk_directory_entries_are_durable_only_after_directory_fsync() {
    let _guard = sim_test_guard();
    run_with_madsim(1, async {
        let dir = sim_dir("rename");
        let target = dir.join("journal");
        let temporary = dir.join("journal.tmp");
        write_file(&target, b"old", true);
        SimDisk::sync_dir(&dir).expect("make the target durable");

        write_file(&temporary, b"new", true);
        SimDisk::rename(&temporary, &target).expect("replace the target");
        assert_eq!(SimDisk::read(&target).expect("renamed"), b"new");
        SimDisk::power_loss(&dir).expect("power loss");
        assert_eq!(
            SimDisk::read(&target).expect("old target survives"),
            b"old",
            "an un-fsynced rename reverts"
        );
        assert!(!SimDisk::exists(&temporary));

        write_file(&temporary, b"new", true);
        SimDisk::rename(&temporary, &target).expect("replace the target");
        SimDisk::sync_dir(&dir).expect("make the rename durable");
        SimDisk::power_loss(&dir).expect("power loss");
        assert_eq!(SimDisk::read(&target).expect("new target"), b"new");

        let created = dir.join("created");
        write_file(&created, b"data", true);
        SimDisk::power_loss(&dir).expect("power loss");
        assert!(
            !SimDisk::exists(&created),
            "a new file whose directory was not fsynced disappears"
        );

        // A new directory whose own entry was never fsynced disappears with
        // everything in it, even entries fsynced inside it.
        let nested = dir.join("core-0");
        let journal = nested.join("journal.bin");
        SimDisk::create_dir_all(&nested).expect("create a directory");
        write_file(&journal, b"acknowledged", true);
        SimDisk::sync_dir(&nested).expect("make the journal entry durable");
        SimDisk::power_loss(&dir).expect("power loss");
        assert!(!SimDisk::exists(&nested));
        SimDisk::create_dir_all(&nested).expect("create the directory again");
        assert!(
            !SimDisk::exists(&journal),
            "a directory created again starts empty"
        );
    });
}

#[test]
fn sim_disk_injects_write_and_fsync_errors() {
    let _guard = sim_test_guard();
    run_with_madsim(1, async {
        let dir = sim_dir("faults");
        let path = dir.join("data");
        let mut file = SimDisk::open_append(&path).expect("open");
        SimDisk::sync_dir(&dir).expect("make the file durable");

        SimDisk::inject_fault(&path, SimDiskFault::Write).expect("arm write fault");
        let err = file.append(b"lost").expect_err("the armed write fails");
        assert!(matches!(
            sim_disk_error(&err),
            Some(SimDiskError::Injected {
                fault: SimDiskFault::Write,
                ..
            })
        ));
        assert!(SimDisk::read(&path).expect("read").is_empty());
        file.append(b"").expect("the fault fires once");

        // A failed fsync marks its pages clean without persisting them, so a
        // later successful fsync of other pages leaves a hole.
        file.append(&vec![3_u8; PAGE]).expect("first page");
        SimDisk::inject_fault(&path, SimDiskFault::Sync).expect("arm fsync fault");
        let err = file.sync_data().expect_err("the armed fsync fails");
        assert!(matches!(
            sim_disk_error(&err),
            Some(SimDiskError::Injected {
                fault: SimDiskFault::Sync,
                ..
            })
        ));
        file.append(&vec![4_u8; PAGE]).expect("second page");
        file.sync_data().expect("the next fsync succeeds");
        drop(file);
        SimDisk::power_loss(&dir).expect("power loss");
        let after = SimDisk::read(&path).expect("read");
        assert_eq!(after.len(), 2 * PAGE);
        assert!(after[..PAGE].iter().all(|byte| *byte == 0));
        assert!(after[PAGE..].iter().all(|byte| *byte == 4));

        SimDisk::inject_fault(&dir, SimDiskFault::Sync).expect("arm directory fault");
        SimDisk::sync_dir(&dir).expect_err("the armed directory fsync fails");
        SimDisk::sync_dir(&dir).expect("the fault fires once");
    });
}

#[test]
fn sim_disk_refuses_power_loss_while_a_node_holds_its_journal_lock() {
    let _guard = sim_test_guard();
    run_with_madsim(1, async {
        let dir = sim_dir("running");
        let LockAttempt::Acquired(lock) =
            SimDisk::try_lock(&dir.join("journal.lock")).expect("try lock")
        else {
            panic!("the first owner takes the lock");
        };
        assert!(matches!(
            SimDisk::try_lock(&dir.join("journal.lock")).expect("try lock"),
            LockAttempt::Held { .. }
        ));
        let err = SimDisk::power_loss(&dir).expect_err("a running node keeps its power");
        assert!(matches!(
            sim_disk_error(&err),
            Some(SimDiskError::NodeRunning { .. })
        ));
        drop(lock);
        SimDisk::power_loss(&dir).expect("a stopped node loses power");
    });
}

#[test]
fn sim_disk_is_scoped_to_one_runtime() {
    let _guard = sim_test_guard();
    let path = run_with_madsim(1, async {
        let path = sim_dir("scoped").join("data");
        write_file(&path, b"first run", true);
        path
    });
    run_with_madsim(1, async move {
        assert_eq!(sim_dir("scoped").join("data"), path);
        assert!(
            !SimDisk::exists(&path),
            "a new runtime starts on an empty disk"
        );
    });
}

pub(super) const JOURNAL_POWER_LOSS_SEEDS: [u64; 6] = [1, 2, 3, 5, 8, 13];
pub(super) const JOURNAL_GROUPS: [u32; 2] = [0, 1];

pub(super) fn group_placement(raft_group_id: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(raft_group_id),
        raft_group_id: RaftGroupId(raft_group_id),
    }
}

pub(super) fn group_stream(raft_group_id: u32) -> BucketStreamId {
    BucketStreamId::new("simulated", format!("journal-group-{raft_group_id}"))
}

/// What one replica holds for a group, read from its log store.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct DurableGroupLog {
    pub(super) vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    pub(super) log_ids: Vec<LogIdOf<UrsulaRaftTypeConfig>>,
}

impl DurableGroupLog {
    pub(super) async fn read(store: &Arc<RaftGroupFileLogStore>) -> Self {
        let mut store = store.clone();
        let vote = store.read_vote().await.expect("read vote");
        let entries = store
            .try_get_log_entries(..)
            .await
            .expect("read log entries");
        Self {
            vote,
            log_ids: entries.iter().map(|entry| entry.log_id).collect(),
        }
    }
}

/// Three nodes whose two raft groups share each node's core-0 journal. Every
/// replica runs the production recovery gate and its drivers, and node 1
/// bootstraps each group as an initializer does.
pub(super) struct JournalCluster {
    config: Arc<Config>,
    pub(super) policy: InProcessRaftNetworkPolicy,
    /// Every vote answer the network delivered.
    pub(super) votes: VoteLog,
    pub(super) wals: BTreeMap<u64, SimNodeWal>,
    registries: BTreeMap<u32, InProcessRaftRegistry>,
    pub(super) engines: BTreeMap<(u32, u64), RaftGroupEngine>,
    pub(super) rejoins: BTreeMap<(u32, u64), Arc<GroupRejoin>>,
    /// Each node's WAL metrics, kept across its restarts.
    pub(super) metrics: BTreeMap<u64, RuntimeMetrics>,
    pub(super) acknowledged: BTreeMap<u32, Vec<u8>>,
    /// The deterministic bug a group's replicas run, if the nodes start
    /// faulty code.
    faulty: Option<(u32, ApplyFault)>,
    /// Replicas whose startup replay stopped at a committed record, by the
    /// failing index. They have no engine.
    replay_stopped: BTreeMap<(u32, u64), u64>,
}

impl JournalCluster {
    async fn start(name: &str) -> Self {
        Self::start_with_fsync(name, WalFsync::Always).await
    }

    pub(super) async fn start_with_fsync(name: &str, fsync: WalFsync) -> Self {
        Self::start_with_tuning(name, JournalTuning::new(fsync)).await
    }

    /// Three nodes whose WALs run with `tuning`.
    pub(super) async fn start_with_tuning(name: &str, tuning: JournalTuning) -> Self {
        let mut cluster = Self::unstarted_with_tuning(name, tuning);
        for node_id in 1..=3 {
            cluster.start_node(node_id).await;
        }
        for group in JOURNAL_GROUPS {
            let leader = cluster.leader(group).await;
            cluster
                .engines
                .get_mut(&(group, leader))
                .expect("leader replica")
                .create_stream(
                    CreateStreamRequest::new(group_stream(group), "application/octet-stream"),
                    group_placement(group),
                    ColdWriteAdmission::default(),
                )
                .await
                .expect("create the group's stream");
        }
        // The followers' gates open once they applied the leader's barrier.
        cluster.wait_gates_open(Duration::from_secs(5)).await;
        // The first election of a new group may take the votes of replicas
        // whose history is still unknown; scenarios check the votes after it.
        cluster.votes.clear();
        cluster
    }

    pub(super) fn unstarted(name: &str, fsync: WalFsync) -> Self {
        Self::unstarted_with_tuning(name, JournalTuning::new(fsync))
    }

    fn unstarted_with_tuning(name: &str, tuning: JournalTuning) -> Self {
        let config = Arc::new(
            Config {
                cluster_name: name.to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 50,
                election_timeout_max: 100,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );
        let (policy, votes) = recovery_wiring::vote_recording_network_policy();
        Self {
            config,
            policy,
            votes,
            wals: (1..=3)
                .map(|node_id| {
                    (
                        node_id,
                        SimNodeWal::provision_with_tuning(&format!("{name}-{node_id}"), tuning)
                            .with_group_count(JOURNAL_GROUPS.len()),
                    )
                })
                .collect(),
            registries: JOURNAL_GROUPS
                .iter()
                .map(|group| (*group, InProcessRaftRegistry::default()))
                .collect(),
            engines: BTreeMap::new(),
            rejoins: BTreeMap::new(),
            metrics: (1..=3)
                .map(|node_id| (node_id, RuntimeMetrics::new(1, JOURNAL_GROUPS.len())))
                .collect(),
            acknowledged: BTreeMap::new(),
            faulty: None,
            replay_stopped: BTreeMap::new(),
        }
    }

    pub(super) async fn start_node(&mut self, node_id: u64) {
        let voters = recovery_wiring::configured_voters(1..=3);
        for group in JOURNAL_GROUPS {
            let placement = group_placement(group);
            let store = self.wals[&node_id]
                .open(placement, self.metrics[&node_id].group_engine_metrics())
                .await;
            let rejoin = Arc::new(
                GroupRejoin::durable(node_id, placement.raft_group_id, &store)
                    .await
                    .expect("open the recovery gate"),
            );
            let registry = self.registries[&group].clone();
            let mut config = (*self.config).clone();
            config.enable_elect =
                ursula_raft::ElectionPolicy::default().may_campaign(Some(&rejoin));
            let mut options = RaftGroupEngineOptions::default();
            options.apply_fault = self
                .faulty
                .filter(|(faulty, _)| *faulty == group)
                .map(|(_, fault)| fault);
            let started = RaftGroupEngine::new_node(
                placement,
                node_id,
                Arc::new(config),
                InProcessRaftNetworkFactory::new(registry.clone())
                    .with_source(node_id)
                    .with_policy(self.policy.clone())
                    .with_rejoin(rejoin.clone()),
                store,
                options,
            )
            .await;
            let engine = match started {
                Ok(engine) => engine,
                // Faulty code replays a committed poison record: that group
                // stays stopped and the node's other groups start.
                Err(GroupEngineError::Infra(GroupInfraError::ApplyStopped { index, .. }))
                    if self.faulty.is_some() =>
                {
                    self.replay_stopped.insert((group, node_id), index);
                    continue;
                }
                Err(error) => panic!("start a journal-backed replica: {error}"),
            };
            recovery_wiring::wire_recovery(
                node_id,
                placement,
                &engine,
                &rejoin,
                &registry,
                &self.policy,
                &voters,
            );
            registry.register(node_id, &engine);
            self.engines.insert((group, node_id), engine);
            self.rejoins.insert((group, node_id), rejoin);
        }
    }

    pub(super) async fn stop_node(&mut self, node_id: u64) {
        for group in JOURNAL_GROUPS {
            self.registries[&group].unregister(node_id);
            match self.engines.remove(&(group, node_id)) {
                Some(engine) => engine.shutdown().await.expect("stop the replica"),
                None => {
                    self.replay_stopped
                        .remove(&(group, node_id))
                        .expect("a running replica, or one whose replay stopped");
                }
            }
        }
    }

    pub(super) async fn leader(&self, group: u32) -> u64 {
        let (_, engine) = self
            .engines
            .iter()
            .find(|((engine_group, node_id), _)| {
                *engine_group == group && !self.stopped_by_storage_error(group, *node_id)
            })
            .expect("a running replica of the group");
        engine
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
            .await
            .expect("wait for a leader")
            .current_leader
            .expect("leader id")
    }

    /// Appends `count` payloads through the group's leader, retrying while
    /// leadership moves.
    pub(super) async fn append(&mut self, group: u32, count: usize) {
        for _ in 0..count {
            let acknowledged = self.acknowledged.entry(group).or_default();
            let payload = format!("g{group}-{};", acknowledged.len()).into_bytes();
            let mut appended = false;
            for _ in 0..50 {
                let leader = self.leader(group).await;
                if let Some(engine) = self.engines.get_mut(&(group, leader))
                    && engine
                        .append(
                            AppendRequest::from_bytes(group_stream(group), payload.clone()),
                            group_placement(group),
                            ColdWriteAdmission::default(),
                        )
                        .await
                        .is_ok()
                {
                    appended = true;
                    break;
                }
                madsim::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(appended, "group {group} accepts no append");
            self.acknowledged
                .entry(group)
                .or_default()
                .extend_from_slice(&payload);
        }
    }

    /// Whether OpenRaft stopped the replica after a storage error.
    fn stopped_by_storage_error(&self, group: u32, node_id: u64) -> bool {
        let metrics = openraft::rt::WatchReceiver::borrow_watched(
            &self.engines[&(group, node_id)].raft_handle().metrics(),
        )
        .clone();
        metrics.running_state.is_err()
    }

    /// Waits until OpenRaft stopped every replica of `node_id` after a
    /// storage error.
    async fn wait_stopped_by_storage_error(&self, node_id: u64) -> bool {
        for _ in 0..200 {
            if JOURNAL_GROUPS
                .iter()
                .all(|group| self.stopped_by_storage_error(*group, node_id))
            {
                return true;
            }
            madsim::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    /// A node that leads no group, chosen from the seed.
    pub(super) async fn follower_of_every_group(&self, seed: u64) -> u64 {
        let mut leaders = Vec::new();
        for group in JOURNAL_GROUPS {
            leaders.push(self.leader(group).await);
        }
        let followers = (1..=3)
            .filter(|node_id| !leaders.contains(node_id))
            .collect::<Vec<_>>();
        let count = u64::try_from(followers.len()).expect("follower count fits u64");
        let index = usize::try_from(seed.checked_rem(count).expect("a node leads no group"))
            .expect("index fits usize");
        followers[index]
    }

    pub(super) async fn durable_logs(&self, node_id: u64) -> BTreeMap<u32, DurableGroupLog> {
        let mut logs = BTreeMap::new();
        for group in JOURNAL_GROUPS {
            let store = self.wals[&node_id]
                .store(RaftGroupId(group))
                .expect("a running replica holds its store");
            logs.insert(group, DurableGroupLog::read(&store).await);
        }
        logs
    }

    /// Waits until the recovery gate of every running replica is open.
    pub(super) async fn wait_gates_open(&self, timeout: Duration) {
        let deadline = madsim::time::Instant::now() + timeout;
        while let Some(((group, node_id), rejoin)) = self
            .rejoins
            .iter()
            .filter(|(key, _)| self.engines.contains_key(key))
            .find(|(_, rejoin)| !rejoin.vote_gate_open())
        {
            assert!(
                madsim::time::Instant::now() < deadline,
                "node {node_id} group {group}: the recovery gate never opened ({:?})",
                rejoin.status()
            );
            madsim::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Snapshots `group` on every running replica but `skip` and purges all
    /// but the last `keep` entries the snapshot covers, as the snapshot
    /// driver's snapshots do.
    pub(super) async fn snapshot_and_purge(&self, group: u32, keep: u64, skip: u64) {
        for ((engine_group, node_id), engine) in &self.engines {
            if *engine_group != group
                || *node_id == skip
                || self.stopped_by_storage_error(group, *node_id)
            {
                continue;
            }
            let raft = engine.raft_handle();
            let applied = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics())
                .last_applied
                .map_or(0, |log_id| log_id.index);
            raft.trigger().snapshot().await.expect("trigger a snapshot");
            let snapshot = raft
                .wait(Some(Duration::from_secs(5)))
                .metrics(
                    |metrics| {
                        metrics
                            .snapshot
                            .is_some_and(|snapshot| snapshot.index >= applied)
                    },
                    "snapshot built",
                )
                .await
                .expect("wait for the snapshot")
                .snapshot
                .expect("snapshot log id");
            let purge_upto = snapshot.index.saturating_sub(keep);
            raft.trigger()
                .purge_log(purge_upto)
                .await
                .expect("trigger a purge");
            raft.wait(Some(Duration::from_secs(5)))
                .metrics(
                    |metrics| {
                        metrics
                            .purged
                            .is_some_and(|purged| purged.index >= purge_upto)
                    },
                    "log purged",
                )
                .await
                .expect("wait for the purge");
        }
    }

    /// Every acknowledged write is readable from every replica.
    pub(super) async fn verify_reads(&self) {
        for group in JOURNAL_GROUPS {
            self.verify_group_reads(group).await;
        }
    }

    /// Every running replica of `group` reads the group's acknowledged
    /// payloads.
    pub(super) async fn verify_group_reads(&self, group: u32) {
        for ((_, node_id), engine) in self
            .engines
            .iter()
            .filter(|((engine_group, _), _)| *engine_group == group)
        {
            let expected = &self.acknowledged[&group];
            let mut last = Vec::new();
            for _ in 0..100 {
                last = engine
                    .sim_read_local_stream(
                        ReadStreamRequest {
                            stream_id: group_stream(group),
                            offset: 0,
                            max_len: expected.len().saturating_add(64),
                            now_ms: 0,
                            leader_only: false,
                            read_index: None,
                        },
                        group_placement(group),
                    )
                    .await
                    .map(|read| read.payload)
                    .unwrap_or_default();
                if &last == expected {
                    break;
                }
                madsim::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(
                &last, expected,
                "node {node_id} group {group} lost an acknowledged write"
            );
        }
    }
}

/// With every acknowledged append `fsync`ed, a follower whose host loses
/// power restarts from its journal with every entry it acknowledged and with
/// its vote, even though unsynced replay hints may be lost.
#[test]
fn power_loss_restart_keeps_every_acknowledged_write() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("JOURNAL_POWER_LOSS_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let mut cluster = JournalCluster::start("journal-power-loss").await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            let before = cluster.durable_logs(victim).await;

            cluster.stop_node(victim).await;
            let report = cluster.wals[&victim].power_loss().await;
            cluster.start_node(victim).await;
            // Every acknowledged append was synced, so the node does not
            // recover. Sealed segments read strictly, and the newest one keeps
            // its verified prefix: an incomplete final frame is cut, and a
            // complete invalid frame is cut after the core's groups are gated.
            assert_opening(
                cluster.wals[&victim].opening(),
                PreviousRun::HostCrash {
                    fsync: WalFsync::Always,
                },
                JournalReplayMode::Strict,
                RecoveryState::Normal,
                &format!("seed {seed}"),
            );
            for group in JOURNAL_GROUPS {
                let store = cluster.wals[&victim]
                    .store(RaftGroupId(group))
                    .expect("running store");
                assert_eq!(store.journal_replay_mode(), JournalReplayMode::Strict);
            }
            let after = cluster.durable_logs(victim).await;
            for group in JOURNAL_GROUPS {
                let (before, after) = (&before[&group], &after[&group]);
                assert_eq!(
                    after.vote, before.vote,
                    "seed {seed}: node {victim} group {group} lost its vote ({report:?})"
                );
                assert!(
                    after.log_ids.starts_with(&before.log_ids),
                    "seed {seed}: node {victim} group {group} lost acknowledged entries \
                     ({report:?}): before {:?}, after {:?}",
                    before.log_ids,
                    after.log_ids
                );
            }

            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            cluster.verify_reads().await;
        });
    }
}

/// An injected I/O error poisons the node's core journal: the failing
/// append, every later write of every group on that core fails with
/// `WriterPoisoned`, and OpenRaft stops each of the node's replicas. The
/// others keep committing. A restart (the process aborts in production)
/// recovers the node from its journal, and no acknowledged write is lost.
fn journal_io_error_poisons_the_node_until_restart(
    name: &str,
    fault: SimDiskFault,
    failed_op: JournalOp,
) {
    for seed in seeds_from_env("JOURNAL_IO_ERROR_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        let name = name.to_owned();
        run_with_madsim(seed, async move {
            let mut cluster = JournalCluster::start(&name).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            // Keeps the victim's journal writer alive after OpenRaft stops
            // its replicas and drops their stores.
            let mut store = cluster.wals[&victim]
                .store(RaftGroupId(0))
                .expect("a running replica holds its store");
            // The fault fires on the segment appends go to, or on the next
            // one when a rotation comes first.
            for segment in active_segments(cluster.wals[&victim].root()) {
                SimDisk::inject_fault(&segment, fault).expect("arm the fault");
            }

            for _ in 0..20 {
                for group in JOURNAL_GROUPS {
                    cluster.append(group, 1).await;
                }
                if JOURNAL_GROUPS
                    .iter()
                    .all(|group| cluster.stopped_by_storage_error(*group, victim))
                {
                    break;
                }
            }
            assert!(
                cluster.wait_stopped_by_storage_error(victim).await,
                "seed {seed}: the error stops every replica on node {victim}"
            );
            SimDisk::clear_faults(cluster.wals[&victim].root()).expect("disarm the other fault");
            let last = match store.get_log_state().await {
                Ok(state) => state.last_log_id,
                Err(error) => {
                    // A submitted append that failed its asynchronous flush
                    // also poisons the readable pending suffix. No caller may
                    // mistake those entries for a healthy log after the error.
                    assert!(
                        matches!(
                            core_journal_error(&error),
                            Some(CoreJournalError::WriterPoisoned { .. })
                        ),
                        "seed {seed}: unexpected log-state error: {error}"
                    );
                    None
                }
            };
            let err = store
                .truncate_after(last)
                .await
                .expect_err("a poisoned journal refuses every write");
            let Some(CoreJournalError::WriterPoisoned { cause, .. }) = core_journal_error(&err)
            else {
                panic!("seed {seed}: expected a poisoned writer, got {err}");
            };
            assert!(
                matches!(**cause, JournalError::Io { op, .. } if op == failed_op),
                "seed {seed}: unexpected cause {cause}"
            );
            drop(store);

            cluster.stop_node(victim).await;
            cluster.wals[&victim].process_crash().await;
            cluster.start_node(victim).await;
            // The writer recorded the failure before the process stopped, so
            // the restart reads every journal as a verified prefix.
            assert_opening(
                cluster.wals[&victim].opening(),
                PreviousRun::Poisoned,
                JournalReplayMode::VerifiedPrefix,
                RecoveryState::Recovering {
                    reason: RecoveryReason::Poisoned,
                },
                &format!("seed {seed}"),
            );
            for group in JOURNAL_GROUPS {
                cluster.append(group, 2).await;
            }
            cluster.verify_reads().await;
        });
    }
}

#[test]
fn journal_write_error_poisons_the_node_until_restart() {
    let _guard = sim_test_guard();
    journal_io_error_poisons_the_node_until_restart(
        "journal-write-error",
        SimDiskFault::Write,
        JournalOp::Append,
    );
}

#[test]
fn journal_fsync_error_poisons_the_node_until_restart() {
    let _guard = sim_test_guard();
    journal_io_error_poisons_the_node_until_restart(
        "journal-fsync-error",
        SimDiskFault::Sync,
        JournalOp::Sync,
    );
}

/// A single-node group with enough history to span several journal
/// segments (the simulator rotates every 4 KiB), so a purge deletes some.
struct ReclaimGroup {
    wal: SimNodeWal,
    metrics: RuntimeMetrics,
    engine: RaftGroupEngine,
}

impl ReclaimGroup {
    async fn start(name: &str) -> Self {
        let wal = SimNodeWal::provision(name);
        let placement = group_placement(0);
        let metrics = RuntimeMetrics::new(1, 1);
        let mut engine = RaftGroupEngine::new_single_node_on_log_store(
            placement,
            wal.open(placement, metrics.group_engine_metrics()).await,
            None,
        )
        .await
        .expect("start a single-node group");
        engine
            .create_stream(
                CreateStreamRequest::new(group_stream(0), "application/octet-stream"),
                placement,
                ColdWriteAdmission::default(),
            )
            .await
            .expect("create a stream");
        let mut group = Self {
            wal,
            metrics,
            engine,
        };
        group.append(0..120).await;
        group
    }

    async fn append(&mut self, payloads: std::ops::Range<u8>) {
        for index in payloads {
            self.engine
                .append(
                    AppendRequest::from_bytes(group_stream(0), vec![index; 128]),
                    group_placement(0),
                    ColdWriteAdmission::default(),
                )
                .await
                .expect("append");
        }
    }

    /// Snapshots the group and purges all but its last few entries.
    async fn purge(&self) {
        let raft = self.engine.raft_handle();
        raft.trigger().snapshot().await.expect("trigger a snapshot");
        let snapshot = raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|metrics| metrics.snapshot.is_some(), "snapshot built")
            .await
            .expect("wait for the snapshot")
            .snapshot
            .expect("snapshot log id");
        let purge_upto = snapshot.index.saturating_sub(8);
        raft.trigger()
            .purge_log(purge_upto)
            .await
            .expect("trigger a purge");
        raft.wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    metrics
                        .purged
                        .is_some_and(|purged| purged.index >= purge_upto)
                },
                "log purged",
            )
            .await
            .expect("wait for the purge");
    }

    /// What the group's store holds now.
    async fn durable_log(&self) -> (Option<LogIdOf<UrsulaRaftTypeConfig>>, DurableGroupLog) {
        let store = self.wal.store(RaftGroupId(0)).expect("running store");
        let mut reader = store.clone();
        let state = reader.get_log_state().await.expect("log state");
        (
            state.last_purged_log_id,
            DurableGroupLog::read(&store).await,
        )
    }

    /// Stops the group, cuts the node's power and reopens the store.
    async fn power_loss_and_reopen(
        self,
    ) -> (Option<LogIdOf<UrsulaRaftTypeConfig>>, DurableGroupLog) {
        self.engine.shutdown().await.expect("stop the group");
        drop(self.engine);
        self.wal.power_loss().await;
        let mut store = self
            .wal
            .open(group_placement(0), self.metrics.group_engine_metrics())
            .await;
        let state = store.get_log_state().await.expect("recovered log state");
        (
            state.last_purged_log_id,
            DurableGroupLog::read(&store).await,
        )
    }
}

/// A purge deletes the segments no entry is left in. The purge record was
/// synced before them, so a power loss right after keeps the purge and every
/// retained entry, whether or not the deletions reached the disk.
#[test]
fn a_purge_deletes_segments_and_survives_power_loss() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("ONLINE_RECLAIM_SEEDS", &[1, 2, 3]) {
        run_with_madsim(seed, async move {
            let group = ReclaimGroup::start("segment-purge").await;
            let segments = journal_segments(&group.wal.root().join("core-0"))
                .expect("segments")
                .len();
            assert!(
                segments >= 3,
                "seed {seed}: 120 entries span {segments} segments"
            );
            group.purge().await;
            let metrics = group.metrics.snapshot();
            assert!(
                metrics.wal_reclaims >= 1,
                "seed {seed}: the purge deleted a segment"
            );
            assert_eq!(metrics.wal_reclaim_failures, 0);
            let before = group.durable_log().await;
            assert_eq!(group.power_loss_and_reopen().await, before);
        });
    }
}

/// A segment that cannot be removed stays in the journal, which remains
/// correct: the purge that freed it stays acknowledged, the failure is
/// counted, the group keeps writing, a later pass removes the segment, and
/// a power loss loses nothing.
#[test]
fn a_segment_that_cannot_be_removed_keeps_the_journal_and_the_group_running() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("ONLINE_RECLAIM_SEEDS", &[1, 2, 3]) {
        run_with_madsim(seed, async move {
            let mut group = ReclaimGroup::start("failed-reclaim").await;
            let core = group.wal.root().join("core-0");
            let oldest = journal_segment_path(&core, 1);
            SimDisk::inject_fault(&oldest, SimDiskFault::Remove).expect("fail the removal");
            group.purge().await;
            let metrics = group.metrics.snapshot();
            assert!(
                metrics.wal_reclaim_failures >= 1,
                "seed {seed}: the removal failed"
            );

            group.append(120..130).await;
            let raft_metrics =
                openraft::rt::WatchReceiver::borrow_watched(&group.engine.raft_handle().metrics())
                    .clone();
            assert!(
                raft_metrics.running_state.is_ok(),
                "seed {seed}: the group keeps running"
            );
            group.purge().await;
            assert!(
                !SimDisk::exists(&oldest),
                "seed {seed}: a later pass removed the segment"
            );
            let before = group.durable_log().await;
            assert_eq!(group.power_loss_and_reopen().await, before);
        });
    }
}

/// A failed directory `fsync` after segments were removed, or after a
/// rotation created one, poisons the writer: a later segment's durability
/// relies on that directory. The purge was durable and stays acknowledged,
/// and a power loss then recovers every retained entry.
#[test]
fn a_journal_directory_fsync_failure_poisons_the_writer() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("ONLINE_RECLAIM_SEEDS", &[1, 2, 3]) {
        run_with_madsim(seed, async move {
            let group = ReclaimGroup::start("unpublished-reclaim").await;
            let core_dir = group.wal.root().join("core-0");
            SimDisk::inject_fault(&core_dir, SimDiskFault::Sync).expect("fail the directory fsync");
            group.purge().await;
            assert!(
                group.metrics.snapshot().wal_reclaim_failures >= 1,
                "seed {seed}: the rewrite failed"
            );
            let (purged, log) = group.durable_log().await;

            let mut store = group.wal.store(RaftGroupId(0)).expect("running store");
            let err = store
                .truncate_after(log.log_ids.last().copied())
                .await
                .expect_err("a poisoned journal refuses every write");
            assert!(
                matches!(
                    core_journal_error(&err),
                    Some(CoreJournalError::WriterPoisoned { .. })
                ),
                "seed {seed}: {err}"
            );
            drop(store);
            let (recovered_purged, recovered) = group.power_loss_and_reopen().await;
            assert_eq!(recovered.vote, log.vote, "seed {seed}");
            assert!(
                recovered.log_ids.ends_with(&log.log_ids),
                "seed {seed}: every retained entry survives: {:?} then {:?}",
                log.log_ids,
                recovered.log_ids
            );
            assert!(recovered_purged <= purged, "seed {seed}");
        });
    }
}

/// What a node finds of its run state after a power loss.
#[derive(Debug, Clone, Copy)]
enum RunStateAfterPowerLoss {
    /// As the node left it: the run did not end cleanly and the host booted
    /// anew.
    Kept,
    /// Removed by hand while the journal holds records: how the run ended is
    /// unknown.
    Removed,
}

/// A single-node group whose synced entries are followed by an unsynced
/// tail of committed markers, after a power loss that may reorder the tail's
/// writeback. Returns what was synced, how the WAL opened afterwards, and
/// how a store reopened then fared.
async fn reopen_after_a_reordered_unsynced_tail(
    run_state: RunStateAfterPowerLoss,
) -> (
    DurableGroupLog,
    WalOpening,
    Result<DurableGroupLog, String>,
    bool,
) {
    // The simulator defaults to one-page segments, whose rotation fsync
    // prevents multi-page holes. Keep this entire unacknowledged tail active.
    let wal = SimNodeWal::provision_with_tuning("unsynced-tail", JournalTuning {
        segment_bytes: 1024 * 1024,
        ..JournalTuning::new(WalFsync::Always)
    });
    let placement = group_placement(0);
    let metrics = standalone_wal_metrics(placement);
    let mut engine = RaftGroupEngine::new_single_node_on_log_store(
        placement,
        wal.open(placement, metrics.clone()).await,
        None,
    )
    .await
    .expect("start a single-node group");
    engine
        .create_stream(
            CreateStreamRequest::new(group_stream(0), "application/octet-stream"),
            placement,
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create a stream");
    engine.shutdown().await.expect("stop the group");
    drop(engine);

    // Committed markers are journaled without an fsync.
    let mut store = wal.open(placement, metrics.clone()).await;
    let synced = DurableGroupLog::read(&store).await;
    let last = *synced.log_ids.last().expect("synced entries");
    for marker in 0..4096 {
        // The compacted log may retain only one entry. Alternate None/Some
        // so every marker is distinct instead of silently deduplicating.
        let committed = if marker % 2 == 0 { None } else { Some(last) };
        store
            .save_committed(committed)
            .await
            .expect("journal a committed marker");
    }
    drop(store);
    let report = wal.power_loss().await;
    assert!(
        report.kept_pages + report.dropped_pages >= 4,
        "multiple dirty pages: {report:?}"
    );
    let bytes = SimDisk::read(&active_segments(wal.root())[0]).unwrap();
    let hole = bytes
        .chunks(PAGE)
        .any(|page| page.len() == PAGE && page.iter().all(|byte| *byte == 0));
    if let RunStateAfterPowerLoss::Removed = run_state {
        SimDisk::remove_file(&wal.root().join(RUN_STATE_FILE)).expect("remove the run state");
    }
    let opening = wal.opening();
    let reopened = match wal.try_open(placement, metrics).await {
        Ok(store) => {
            let recovered = DurableGroupLog::read(&store).await;
            if hole {
                assert_eq!(
                    store.log_state(),
                    GroupLogState::Recovering,
                    "a complete invalid tail must close the durable recovery gate: {report:?}"
                );
                assert!(
                    !GroupRejoin::durable(1, placement.raft_group_id, &store)
                        .await
                        .unwrap()
                        .vote_gate_open()
                );
            }
            Ok(recovered)
        }
        Err(err) => Err(format!("{report:?}: {err}")),
    };
    (synced, opening, reopened, hole)
}

const UNSYNCED_TAIL_SEEDS: [u64; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

/// A multi-page unsynced tail may contain zero holes after power loss.
/// Only the newest segment is repaired, with a durable recovery gate;
/// every acknowledged entry and the vote must survive.
#[test]
fn gated_newest_tail_repair_keeps_every_acknowledged_write_after_reordered_pages() {
    let _guard = sim_test_guard();
    let mut holes = 0;
    for seed in seeds_from_env("UNSYNCED_TAIL_SEEDS", &UNSYNCED_TAIL_SEEDS) {
        let (synced, opening, reopened, hole) = run_with_madsim(
            seed,
            reopen_after_a_reordered_unsynced_tail(RunStateAfterPowerLoss::Kept),
        );
        holes += usize::from(hole);
        assert_eq!(opening.replay_mode, JournalReplayMode::Strict);
        assert_eq!(
            reopened.unwrap_or_else(|err| panic!("seed {seed}: {err}")),
            synced,
            "seed {seed}"
        );
    }
    assert!(
        holes > 0,
        "the schedules must exercise a complete zero hole"
    );
}

/// A run state removed while the journal holds records says nothing about
/// how the run that wrote them ended, so the node fails safe: it reads the
/// journal as a verified prefix, which recovers every synced entry even
/// across the reordered tail, and reports that it is recovering.
#[test]
fn a_removed_run_state_still_reads_the_journal_as_a_verified_prefix() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("UNSYNCED_TAIL_SEEDS", &UNSYNCED_TAIL_SEEDS) {
        let (synced, opening, reopened, _hole) = run_with_madsim(
            seed,
            reopen_after_a_reordered_unsynced_tail(RunStateAfterPowerLoss::Removed),
        );
        assert_opening(
            opening,
            PreviousRun::Unrecorded,
            JournalReplayMode::VerifiedPrefix,
            RecoveryState::Recovering {
                reason: RecoveryReason::UnknownHistory,
            },
            &format!("seed {seed}"),
        );
        assert_eq!(
            reopened.unwrap_or_else(|err| panic!("seed {seed}: {err}")),
            synced,
            "seed {seed}"
        );
    }
}

/// The opening and the durable logs of `node_id`'s groups, read from stores
/// opened without starting the replicas.
async fn reopen_stores(
    cluster: &JournalCluster,
    node_id: u64,
) -> (WalOpening, BTreeMap<u32, (DurableGroupLog, GroupLogState)>) {
    let wal = &cluster.wals[&node_id];
    let mut logs = BTreeMap::new();
    for group in JOURNAL_GROUPS {
        let placement = group_placement(group);
        let store = wal.open(placement, standalone_wal_metrics(placement)).await;
        logs.insert(
            group,
            (DurableGroupLog::read(&store).await, store.log_state()),
        );
    }
    (wal.opening(), logs)
}

/// Under `never` a follower acknowledges appends from the page cache, so a
/// power loss can cost it acknowledged entries. The restart detects it: the
/// run state shows a host crash under `never`, the journals are read as a
/// verified prefix and the node reports that it is recovering. What it keeps
/// is a prefix of its log, and its vote and log state, which are always
/// `fsync`ed, survive; every group it held is recovering. The stores are
/// only reopened here; `recovery_tests` covers the replica's rejoin.
#[test]
fn fsync_never_power_loss_is_detected_and_keeps_a_verified_prefix_and_the_vote() {
    let _guard = sim_test_guard();
    let mut lost = 0_usize;
    for seed in seeds_from_env("JOURNAL_POWER_LOSS_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        let lost_on_seed = run_with_madsim(seed, async move {
            let mut cluster =
                JournalCluster::start_with_fsync("never-power-loss", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            let before = cluster.durable_logs(victim).await;
            cluster.stop_node(victim).await;
            let report = cluster.wals[&victim].power_loss().await;

            let (opening, after) = reopen_stores(&cluster, victim).await;
            assert_opening(
                opening,
                PreviousRun::HostCrash {
                    fsync: WalFsync::Never,
                },
                JournalReplayMode::VerifiedPrefix,
                RECOVERING_AFTER_HOST_CRASH,
                &format!("seed {seed}"),
            );
            let mut lost = 0_usize;
            for group in JOURNAL_GROUPS {
                let (before, (after, log_state)) = (&before[&group], &after[&group]);
                assert_eq!(
                    after.vote, before.vote,
                    "seed {seed}: node {victim} group {group} lost its vote ({report:?})"
                );
                assert!(
                    before.log_ids.starts_with(&after.log_ids),
                    "seed {seed}: node {victim} group {group} kept no prefix of its log \
                     ({report:?}): before {:?}, after {:?}",
                    before.log_ids,
                    after.log_ids
                );
                assert_eq!(
                    *log_state,
                    GroupLogState::Recovering,
                    "seed {seed}: node {victim} group {group} is not recovering"
                );
                lost =
                    lost.saturating_add(before.log_ids.len().saturating_sub(after.log_ids.len()));
            }
            lost
        });
        lost = lost.saturating_add(lost_on_seed);
    }
    assert!(
        lost > 0,
        "a power loss under fsync = never must sometimes cost a follower acknowledged entries"
    );
}

/// A graceful shutdown `fsync`s every journal before it records a clean
/// run, so under `never` even a power loss right after it loses nothing,
/// and the restart reads the journals strictly.
#[test]
fn clean_shutdown_then_power_loss_reads_strictly_and_loses_nothing() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("JOURNAL_POWER_LOSS_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let mut cluster =
                JournalCluster::start_with_fsync("never-clean-shutdown", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            let before = cluster.durable_logs(victim).await;
            let wal = cluster.wals[&victim].clone();
            wal.clean_shutdown(cluster.stop_node(victim)).await;
            wal.power_loss().await;
            cluster.start_node(victim).await;
            assert_opening(
                cluster.wals[&victim].opening(),
                PreviousRun::Clean,
                JournalReplayMode::Strict,
                RecoveryState::Normal,
                &format!("seed {seed}"),
            );
            let after = cluster.durable_logs(victim).await;
            for group in JOURNAL_GROUPS {
                let (before, after) = (&before[&group], &after[&group]);
                assert_eq!(after.vote, before.vote, "seed {seed}");
                assert!(
                    after.log_ids.starts_with(&before.log_ids),
                    "seed {seed}: group {group} lost entries: before {:?}, after {:?}",
                    before.log_ids,
                    after.log_ids
                );
            }
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            cluster.verify_reads().await;
        });
    }
}

const APPLY_POISON_SEEDS: [u64; 3] = [4, 17, 31];

/// A deterministic poison record in group 0 stops every replica that applies
/// it, and no replica applies past it. Group 1 shares each node's core
/// journal and keeps serving. A node restarted with the faulty code starts
/// group 1 and leaves group 0 stopped at the same record. Corrected code then
/// replays the intact journals on every replica, poison record included.
#[test]
fn a_poison_record_isolates_its_group_until_corrected_replay() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("APPLY_POISON_SEEDS", &APPLY_POISON_SEEDS) {
        run_with_madsim(seed, async move {
            let mut cluster = JournalCluster::start("apply-poison").await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            let leader = cluster.leader(0).await;
            let last = openraft::rt::WatchReceiver::borrow_watched(
                &cluster.engines[&(0, leader)].raft_handle().metrics(),
            )
            .last_log_index
            .expect("group 0 holds entries");
            let poison = last.checked_add(1).expect("next index");
            let fault = ApplyFault::PanicAfterMutation { index: poison };
            cluster.faulty = Some((0, fault));
            for node_id in 1..=3 {
                cluster.engines[&(0, node_id)]
                    .inject_apply_fault(fault)
                    .await
                    .expect("run the faulty code");
            }
            let written = cluster
                .engines
                .get_mut(&(0, leader))
                .expect("group 0 leader")
                .append(
                    AppendRequest::from_bytes(group_stream(0), b"poison;".to_vec()),
                    group_placement(0),
                    ColdWriteAdmission::default(),
                )
                .await;
            // Committed but stopped before its response: the outcome is
            // unknown, and corrected code applies the record.
            assert!(
                matches!(
                    written,
                    Err(GroupEngineError::Infra(GroupInfraError::OutcomeUnknown))
                ),
                "seed {seed}: {written:?}"
            );
            cluster
                .acknowledged
                .entry(0)
                .or_default()
                .extend_from_slice(b"poison;");

            // A re-elected leader applies the committed record and stops too.
            let mut stopped = Vec::new();
            for _ in 0..200 {
                stopped = (1..=3)
                    .filter(|node_id| cluster.stopped_by_storage_error(0, *node_id))
                    .collect::<Vec<_>>();
                if stopped.len() >= 2 {
                    break;
                }
                madsim::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(stopped.len() >= 2, "seed {seed}: stopped {stopped:?}");
            for node_id in 1..=3 {
                let applied = openraft::rt::WatchReceiver::borrow_watched(
                    &cluster.engines[&(0, node_id)].raft_handle().metrics(),
                )
                .last_applied;
                assert!(
                    applied.is_none_or(|applied| applied.index < poison),
                    "seed {seed}: node {node_id} applied {applied:?} past poison {poison}"
                );
            }

            // Group 1 on the same core journals keeps serving.
            cluster.append(1, 3).await;
            cluster.verify_group_reads(1).await;

            // A restart with the faulty code keeps the node up.
            let victim = stopped[0];
            let wal = cluster.wals[&victim].clone();
            wal.clean_shutdown(cluster.stop_node(victim)).await;
            cluster.start_node(victim).await;
            assert_eq!(
                cluster.replay_stopped.get(&(0, victim)),
                Some(&poison),
                "seed {seed}: node {victim} replays and stops at the same record"
            );
            assert!(cluster.engines.contains_key(&(1, victim)));
            cluster.wait_gates_open(Duration::from_secs(5)).await;
            cluster.append(1, 2).await;
            cluster.verify_group_reads(1).await;

            // Corrected code replays every replica's intact journal.
            cluster.faulty = None;
            for node_id in 1..=3 {
                let wal = cluster.wals[&node_id].clone();
                wal.clean_shutdown(cluster.stop_node(node_id)).await;
            }
            for node_id in 1..=3 {
                cluster.start_node(node_id).await;
            }
            assert!(cluster.replay_stopped.is_empty());
            cluster.wait_gates_open(Duration::from_secs(5)).await;
            cluster.append(0, 2).await;
            cluster.append(1, 1).await;
            cluster.verify_reads().await;
        });
    }
}

/// A process crash keeps the page cache, so under `never` the restart on
/// the same boot loses nothing and reads the journals strictly.
#[test]
fn process_crash_under_never_reads_strictly_and_loses_nothing() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("JOURNAL_POWER_LOSS_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let mut cluster =
                JournalCluster::start_with_fsync("never-process-crash", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            let before = cluster.durable_logs(victim).await;
            cluster.stop_node(victim).await;
            cluster.wals[&victim].process_crash().await;
            cluster.start_node(victim).await;
            assert_opening(
                cluster.wals[&victim].opening(),
                PreviousRun::ProcessCrash,
                JournalReplayMode::Strict,
                RecoveryState::Normal,
                &format!("seed {seed}"),
            );
            let after = cluster.durable_logs(victim).await;
            for group in JOURNAL_GROUPS {
                let (before, after) = (&before[&group], &after[&group]);
                assert_eq!(after.vote, before.vote, "seed {seed}");
                assert!(
                    after.log_ids.starts_with(&before.log_ids),
                    "seed {seed}: group {group} lost entries: before {:?}, after {:?}",
                    before.log_ids,
                    after.log_ids
                );
            }
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            cluster.verify_reads().await;
        });
    }
}

/// The run state is replaced through a temporary file, an `fsync`, a rename
/// and a directory `fsync`. A power loss at any step leaves the old or the
/// new version, never a torn one: a run that could not publish its run state
/// fails to start, and after the power loss the next run reads the clean
/// shutdown before it.
#[test]
fn the_run_state_is_the_old_or_the_new_version_after_a_power_loss() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("STATE_FILE_SEEDS", &[1, 2, 3, 5, 8]) {
        run_with_madsim(seed, async move {
            let root = sim_dir("run-state");
            let start = || {
                RaftWal::start(
                    &root,
                    WalFsync::Never,
                    &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
                )
            };
            let temp = root.join("run-state.running.tmp");
            start()
                .expect("a first run")
                .shutdown()
                .await
                .expect("ends cleanly");
            for (path, fault) in [
                (temp.clone(), SimDiskFault::Write),
                (temp.clone(), SimDiskFault::Sync),
                (root.clone(), SimDiskFault::Sync),
            ] {
                SimDisk::inject_fault(&path, fault).expect("arm the fault");
                let err = start().expect_err("the run state is not published");
                assert!(
                    matches!(err, RaftWalError::RecordRunState(_)),
                    "seed {seed} {fault:?}: {err}"
                );
                SimDisk::power_loss(&root).expect("power loss");
                let run = start().expect("the old run state still reads");
                assert_eq!(
                    run.opening().previous_run,
                    PreviousRun::Clean,
                    "seed {seed} {fault:?}"
                );
                run.shutdown().await.expect("ends cleanly");
            }

            // A published run state survives the power loss.
            drop(start().expect("a run that crashes"));
            SimDisk::power_loss(&root).expect("power loss");
            assert_eq!(
                start()
                    .expect("the new run state reads")
                    .opening()
                    .previous_run,
                PreviousRun::HostCrash {
                    fsync: WalFsync::Never
                },
                "seed {seed}"
            );
        });
    }
}

/// Votes live in the core's metadata file, which is `fsync`ed under any
/// policy: a power loss under `never` keeps the vote and the `initialized`
/// flag. A vote whose metadata replacement cannot be published poisons the
/// writer, and after a power loss the previous vote reads back whole.
#[test]
fn votes_survive_power_loss_under_never_and_the_metadata_is_old_or_new() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("STATE_FILE_SEEDS", &[1, 2, 3, 5, 8]) {
        run_with_madsim(seed, async move {
            let wal = SimNodeWal::provision_with_fsync("never-votes", WalFsync::Never);
            let placement = group_placement(0);
            let metrics = standalone_wal_metrics(placement);
            let mut engine = RaftGroupEngine::new_single_node_on_log_store(
                placement,
                wal.open(placement, metrics.clone()).await,
                None,
            )
            .await
            .expect("start a single-node group");
            engine
                .create_stream(
                    CreateStreamRequest::new(group_stream(0), "application/octet-stream"),
                    placement,
                    ColdWriteAdmission::default(),
                )
                .await
                .expect("create a stream");
            let store = wal.store(RaftGroupId(0)).expect("running store");
            let first = DurableGroupLog::read(&store).await.vote.expect("a vote");
            drop(store);
            engine.shutdown().await.expect("stop the group");
            drop(engine);

            wal.power_loss().await;
            let mut store = wal.open(placement, metrics.clone()).await;
            assert_eq!(
                store.read_vote().await.expect("vote"),
                Some(first),
                "seed {seed}"
            );
            assert!(store.initialized(), "seed {seed}");

            // A later term than any a new single-node group reaches.
            let second = openraft::Vote::new_committed(1_000, 1);
            assert_ne!(first, second);
            SimDisk::inject_fault(&wal.root().join("core-0"), SimDiskFault::Sync)
                .expect("fail the metadata publish");
            let err = store
                .save_vote(&second)
                .await
                .expect_err("an unpublished vote is not acknowledged");
            assert!(
                matches!(
                    core_journal_error(&err),
                    Some(CoreJournalError::WriterPoisoned { .. })
                ),
                "seed {seed}: {err}"
            );
            drop(store);
            wal.power_loss().await;
            let mut store = wal.open(placement, metrics.clone()).await;
            assert_eq!(
                store.read_vote().await.expect("vote"),
                Some(first),
                "seed {seed}"
            );

            store.save_vote(&second).await.expect("save the vote");
            drop(store);
            wal.power_loss().await;
            let mut store = wal.open(placement, metrics).await;
            assert_eq!(
                store.read_vote().await.expect("vote"),
                Some(second),
                "seed {seed}"
            );
        });
    }
}

fn sim_log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
    use openraft::vote::RaftLeaderId;

    openraft::LogId {
        leader_id: openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
        index,
    }
}

fn blank_entry(index: u64) -> openraft::alias::EntryOf<UrsulaRaftTypeConfig> {
    use openraft::entry::RaftEntry;

    openraft::alias::EntryOf::<UrsulaRaftTypeConfig>::new(
        sim_log_id(index),
        openraft::EntryPayload::Blank,
    )
}

const PURGE_DURABILITY_SEEDS: [u64; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

/// Under `never` a purge reaches the journal without an `fsync`, and the
/// reclaim pass after it deletes the segments it freed. The deletion becomes
/// durable only after the purge does, so a power loss that drops unsynced
/// pages never keeps the deletion without the purge: the store reopens as
/// a verified prefix, its purge boundary covers every deleted segment, and
/// its entries are consecutive from there.
#[test]
fn a_purge_is_durable_before_the_segments_it_frees_are_deleted() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("PURGE_DURABILITY_SEEDS", &PURGE_DURABILITY_SEEDS) {
        run_with_madsim(seed, async move {
            let wal = SimNodeWal::provision_with_fsync("purge-durability", WalFsync::Never);
            let placement = group_placement(0);
            let metrics = standalone_wal_metrics(placement);
            let mut store = wal.open(placement, metrics.clone()).await;
            for chunk in 0..60_u64 {
                store
                    .blocking_append((1..=10).map(|offset| blank_entry(chunk * 10 + offset)))
                    .await
                    .expect("append");
            }
            let core = wal.root().join("core-0");
            let before = journal_segments(&core).expect("segments");
            assert!(
                before.len() >= 4,
                "seed {seed}: 600 entries span several segments: {}",
                before.len()
            );
            let purged = sim_log_id(590);
            store.purge(purged).await.expect("purge");
            // The reclaim pass after the purge's batch has run once the next
            // write returns.
            store
                .save_committed(Some(sim_log_id(600)))
                .await
                .expect("commit");
            let after = journal_segments(&core).expect("segments");
            assert!(
                after.len() < before.len(),
                "seed {seed}: reclaim deleted the segments the purge freed"
            );
            drop(store);

            let report = wal.power_loss().await;
            assert_opening(
                wal.opening(),
                PreviousRun::HostCrash {
                    fsync: WalFsync::Never,
                },
                JournalReplayMode::VerifiedPrefix,
                RECOVERING_AFTER_HOST_CRASH,
                &format!("seed {seed}"),
            );
            let mut store = wal
                .try_open(placement, metrics)
                .await
                .unwrap_or_else(|err| panic!("seed {seed}: the store reopens ({report:?}): {err}"));
            let state = store.get_log_state().await.expect("log state");
            assert_eq!(
                state.last_purged_log_id,
                Some(purged),
                "seed {seed}: the purge that freed the deleted segments survives ({report:?})"
            );
            let log = DurableGroupLog::read(&store).await;
            let expected = (591..)
                .take(log.log_ids.len())
                .map(sim_log_id)
                .collect::<Vec<_>>();
            assert_eq!(
                log.log_ids, expected,
                "seed {seed}: the retained entries are consecutive after the purge"
            );
        });
    }
}

/// Appends entry `index` to every store, the `n`th append `gap * n` after
/// the first, and returns the `fsync`s they cost.
async fn append_spread(
    stores: &[Arc<RaftGroupFileLogStore>],
    metrics: &RuntimeMetrics,
    index: u64,
    gap: Duration,
) -> u64 {
    let before = metrics.snapshot().wal_fsyncs;
    let appends = stores
        .iter()
        .enumerate()
        .map(|(position, store)| {
            let mut store = store.clone();
            let delay = gap.saturating_mul(u32::try_from(position).expect("position fits u32"));
            madsim::task::spawn(async move {
                madsim::time::sleep(delay).await;
                store
                    .blocking_append([blank_entry(index)])
                    .await
                    .expect("append");
            })
        })
        .collect::<Vec<_>>();
    for append in appends {
        append.await.expect("append task");
    }
    metrics.snapshot().wal_fsyncs.saturating_sub(before)
}

/// Under `always` a batch is a group commit: it keeps collecting while
/// appends keep arriving, so a burst costs one `fsync` in whatever order its
/// senders run, and appends that arrive after the window closed each get
/// their own. madsim timers fire no earlier than 1 ms, so the sub-millisecond
/// window itself is covered by a unit test of the writer.
#[test]
fn a_burst_of_appends_lands_in_one_group_commit() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("GROUP_COMMIT_SEEDS", &[1, 2, 3, 5, 8, 13]) {
        run_with_madsim(seed, async move {
            // Segments large enough that no rotation adds its `fsync`s.
            let wal = SimNodeWal::provision_with_tuning("group-commit", JournalTuning {
                segment_bytes: 1024 * 1024,
                ..JournalTuning::new(WalFsync::Always)
            })
            .with_group_count(16);
            let metrics = RuntimeMetrics::new(1, 16);
            let mut stores = Vec::new();
            for group in 0..16 {
                stores.push(
                    wal.open(group_placement(group), metrics.group_engine_metrics())
                        .await,
                );
            }
            // First entries also mark each group initialized.
            append_spread(&stores, &metrics, 1, Duration::ZERO).await;

            assert_eq!(
                append_spread(&stores, &metrics, 2, Duration::ZERO).await,
                1,
                "seed {seed}: a burst of 16 appends is one group commit"
            );
            assert_eq!(
                append_spread(&stores, &metrics, 3, Duration::from_millis(5)).await,
                16,
                "seed {seed}: appends 5 ms apart each arrive after the window closed"
            );
        });
    }
}

/// What the journals of a cluster did over a scenario, summed over nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct JournalWork {
    pub(super) rotations: u64,
    pub(super) reclaims: u64,
    pub(super) rewritten_bytes: u64,
    pub(super) disk_reads: u64,
}

impl JournalWork {
    fn of(cluster: &JournalCluster) -> Self {
        cluster.metrics.values().map(RuntimeMetrics::snapshot).fold(
            Self::default(),
            |work, metrics| Self {
                rotations: work.rotations.saturating_add(metrics.wal_rotations),
                reclaims: work.reclaims.saturating_add(metrics.wal_reclaims),
                rewritten_bytes: work
                    .rewritten_bytes
                    .saturating_add(metrics.wal_rewritten_bytes),
                disk_reads: work.disk_reads.saturating_add(metrics.wal_disk_reads),
            },
        )
    }
}

/// Crashes the stopped node `node_id` within the durability contract of
/// `fsync`: a power loss under `always`, a process crash under `never`.
async fn crash_within_contract(cluster: &JournalCluster, node_id: u64, fsync: WalFsync) {
    match fsync {
        WalFsync::Always => {
            cluster.wals[&node_id].power_loss().await;
        }
        WalFsync::Never => cluster.wals[&node_id].process_crash().await,
    }
}

/// Rounds of writes, snapshots and purges with a crash in each, on a
/// three-node cluster whose journals run with `fsync`. Group 0 is busy: each
/// round appends to it, then the replicas snapshot and purge it. Group 1 is
/// quiet: its few entries stay in old segments until reclaim rewrites them
/// out. Each round one follower crashes and restarts. It never purges,
/// because a replica restarts its state machine from a persisted snapshot,
/// which the simulated disk does not hold, so it also keeps group 0's log
/// and reports the group lagging. Every follower crash stays within the
/// durability contract. At the end a node that purged and rewrote segments
/// loses power under either policy and its journal reopens: under `always`
/// with every entry it held, under `never` as a verified prefix of its log
/// that a purge boundary still covers.
pub(super) async fn segment_crash_rounds(seed: u64, fsync: WalFsync) -> JournalWork {
    let mut cluster = JournalCluster::start_with_fsync("segment-crash", fsync).await;
    cluster.append(1, 3).await;
    let victim = cluster.follower_of_every_group(seed).await;
    for _round in 0..4 {
        cluster.append(0, 40).await;
        cluster.snapshot_and_purge(0, 8, victim).await;
        cluster.stop_node(victim).await;
        crash_within_contract(&cluster, victim, fsync).await;
        cluster.start_node(victim).await;
        cluster.wait_gates_open(Duration::from_secs(5)).await;
    }
    cluster.append(0, 5).await;
    cluster.append(1, 1).await;
    cluster.verify_reads().await;
    let work = JournalWork::of(&cluster);

    let purged = cluster.leader(0).await;
    let before = cluster.durable_logs(purged).await;
    cluster.stop_node(purged).await;
    cluster.wals[&purged].power_loss().await;
    // The stores reopen: no deleted segment outlives the purge that freed it.
    let (_, after) = reopen_stores(&cluster, purged).await;
    for group in JOURNAL_GROUPS {
        let (after, before) = (&after[&group].0, &before[&group]);
        assert_eq!(after.vote, before.vote, "seed {seed}");
        match fsync {
            WalFsync::Always => assert_eq!(
                after.log_ids, before.log_ids,
                "seed {seed}: node {purged} group {group} lost entries in a power loss"
            ),
            // Unsynced appends and purges may be lost, never mixed up: the
            // log stays consecutive, ends no later than before, and agrees
            // with it wherever both hold an entry.
            WalFsync::Never => {
                assert!(
                    after
                        .log_ids
                        .windows(2)
                        .all(|pair| pair[0].index.checked_add(1) == Some(pair[1].index)),
                    "seed {seed}: node {purged} group {group} reopened with a hole: {:?}",
                    after.log_ids
                );
                assert!(
                    after.log_ids.last().map(|log_id| log_id.index)
                        <= before.log_ids.last().map(|log_id| log_id.index),
                    "seed {seed}: node {purged} group {group}"
                );
                for log_id in &after.log_ids {
                    if let Some(kept) = before
                        .log_ids
                        .iter()
                        .find(|before| before.index == log_id.index)
                    {
                        assert_eq!(kept, log_id, "seed {seed}: node {purged} group {group}");
                    }
                }
            }
        }
    }
    work
}

const SEGMENT_CRASH_SEEDS: [u64; 6] = [1, 2, 3, 4, 5, 6];

/// Rotation, purge and the rewrite of a quiet group's entries run on every
/// node's journal while followers crash, and every acknowledged write stays
/// readable on every replica (`segment_crash_rounds`).
#[test]
fn rotation_purge_and_rewrite_survive_crashes() {
    let _guard = sim_test_guard();
    let mut total = JournalWork::default();
    for seed in seeds_from_env("SEGMENT_CRASH_SEEDS", &SEGMENT_CRASH_SEEDS) {
        let fsync = if seed % 2 == 0 {
            WalFsync::Always
        } else {
            WalFsync::Never
        };
        let work = run_with_madsim(seed, segment_crash_rounds(seed, fsync));
        assert!(
            work.rotations > 0,
            "seed {seed}: the journals rotated: {work:?}"
        );
        assert!(
            work.reclaims > 0,
            "seed {seed}: purges deleted segments: {work:?}"
        );
        total.rewritten_bytes = total.rewritten_bytes.saturating_add(work.rewritten_bytes);
    }
    assert!(
        total.rewritten_bytes > 0,
        "reclaim rewrote the quiet group's entries: {total:?}"
    );
}

/// A follower that was down while the leader wrote far more than its entry
/// cache holds catches up from the leader's journal on disk, and every
/// acknowledged write reads back on it.
#[test]
fn a_lagging_follower_catches_up_from_the_leaders_disk() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("JOURNAL_POWER_LOSS_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let mut cluster =
                JournalCluster::start_with_fsync("lagging-follower", WalFsync::Never).await;
            let victim = cluster.follower_of_every_group(seed).await;
            cluster.stop_node(victim).await;
            cluster.wals[&victim].process_crash().await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 60).await;
            }
            let leaders = {
                let mut leaders = Vec::new();
                for group in JOURNAL_GROUPS {
                    leaders.push(cluster.leader(group).await);
                }
                leaders
            };
            let before = leaders
                .iter()
                .map(|leader| cluster.metrics[leader].snapshot().wal_disk_reads)
                .sum::<u64>();
            cluster.start_node(victim).await;
            cluster.wait_gates_open(Duration::from_secs(10)).await;
            cluster.verify_reads().await;
            let after = leaders
                .iter()
                .map(|leader| cluster.metrics[leader].snapshot().wal_disk_reads)
                .sum::<u64>();
            assert!(
                after > before,
                "seed {seed}: the leaders read the entries the follower missed from disk"
            );
        });
    }
}

/// No journal can be opened before the topology record is durable. A failed
/// publication is retryable on an empty root; a published one survives power
/// loss and still rejects a different layout.
#[test]
fn wal_topology_publication_survives_power_loss() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("STATE_FILE_SEEDS", &[1, 2, 3, 5, 8]) {
        run_with_madsim(seed, async move {
            for (name, fault, relative_path) in [
                ("write", SimDiskFault::Write, "topology.tmp"),
                ("file-sync", SimDiskFault::Sync, "topology.tmp"),
                ("directory-sync", SimDiskFault::Sync, ""),
            ] {
                let root = sim_dir(&format!("topology-{name}"));
                let topology = ursula_shard::StaticShardMap::new(4, 64).unwrap();
                SimDisk::inject_fault(&root.join(relative_path), fault).unwrap();
                assert!(matches!(
                    RaftWal::start(&root, WalFsync::Never, &topology),
                    Err(RaftWalError::RecordTopology(_))
                ));
                assert!(!SimDisk::exists(&root.join(RUN_STATE_FILE)));
                SimDisk::power_loss(&root).unwrap();
                let wal = RaftWal::start(&root, WalFsync::Never, &topology).unwrap();
                drop(wal);
                SimDisk::power_loss(&root).unwrap();
                let changed = ursula_shard::StaticShardMap::new(8, 64).unwrap();
                assert!(matches!(
                    RaftWal::start(&root, WalFsync::Never, &changed),
                    Err(RaftWalError::TopologyMismatch { .. })
                ));
                RaftWal::start(&root, WalFsync::Never, &topology)
                    .unwrap()
                    .shutdown()
                    .await
                    .unwrap();
            }
        });
    }
}

#[path = "poison_marker_tests.rs"]
mod poison_marker;
