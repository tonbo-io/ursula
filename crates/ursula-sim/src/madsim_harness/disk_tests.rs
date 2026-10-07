//! DST coverage of the simulated disk and of the production per-core journal
//! running on it.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use ursula_raft::CoreJournalError;
use ursula_raft::DurableRaftLogStoreFactory;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::JournalDisk;
use ursula_raft::JournalError;
use ursula_raft::JournalFile;
use ursula_raft::JournalOp;
use ursula_raft::JournalReplayMode;
use ursula_raft::LockAttempt;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupFileLogStore;
use ursula_raft::SIM_DISK_PAGE_SIZE;
use ursula_raft::SimDisk;
use ursula_raft::SimDiskError;
use ursula_raft::SimDiskFault;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::seeds_from_env;
use super::sim_test_guard;
use crate::madsim_harness::run_with_madsim;
use crate::madsim_harness::sim_network_policy;
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

fn core_journal(root: &Path) -> PathBuf {
    root.join("core-0").join("journal.bin")
}

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
            SimDisk::try_lock(&dir.join("journal.bin.lock")).expect("try lock")
        else {
            panic!("the first owner takes the lock");
        };
        assert!(matches!(
            SimDisk::try_lock(&dir.join("journal.bin.lock")).expect("try lock"),
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

const JOURNAL_POWER_LOSS_SEEDS: [u64; 6] = [1, 2, 3, 5, 8, 13];
const JOURNAL_GROUPS: [u32; 2] = [0, 1];

fn group_placement(raft_group_id: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(raft_group_id),
        raft_group_id: RaftGroupId(raft_group_id),
    }
}

fn group_stream(raft_group_id: u32) -> BucketStreamId {
    BucketStreamId::new("simulated", format!("journal-group-{raft_group_id}"))
}

/// What one replica holds for a group, read from its log store.
#[derive(Debug, PartialEq, Eq)]
struct DurableGroupLog {
    vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    log_ids: Vec<LogIdOf<UrsulaRaftTypeConfig>>,
}

impl DurableGroupLog {
    async fn read(store: &Arc<RaftGroupFileLogStore>) -> Self {
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

/// Three nodes whose two raft groups share each node's core-0 journal.
struct JournalCluster {
    config: Arc<Config>,
    policy: InProcessRaftNetworkPolicy,
    wals: BTreeMap<u64, SimNodeWal>,
    registries: BTreeMap<u32, InProcessRaftRegistry>,
    engines: BTreeMap<(u32, u64), RaftGroupEngine>,
    acknowledged: BTreeMap<u32, Vec<u8>>,
}

impl JournalCluster {
    async fn start(name: &str) -> Self {
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
        let mut cluster = Self {
            config,
            policy: sim_network_policy(),
            wals: (1..=3)
                .map(|node_id| (node_id, SimNodeWal::provision(&format!("{name}-{node_id}"))))
                .collect(),
            registries: JOURNAL_GROUPS
                .iter()
                .map(|group| (*group, InProcessRaftRegistry::default()))
                .collect(),
            engines: BTreeMap::new(),
            acknowledged: BTreeMap::new(),
        };
        for node_id in 1..=3 {
            cluster.start_node(node_id).await;
        }
        let voters = (1..=3)
            .map(|node_id| (node_id, BasicNode::new(format!("node-{node_id}"))))
            .collect::<BTreeMap<_, _>>();
        for group in JOURNAL_GROUPS {
            cluster.engines[&(group, 1)]
                .initialize_membership(voters.clone())
                .await
                .expect("initialize the group");
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
        cluster
    }

    async fn start_node(&mut self, node_id: u64) {
        for group in JOURNAL_GROUPS {
            let placement = group_placement(group);
            let store = self.wals[&node_id]
                .open(placement, standalone_wal_metrics(placement))
                .await;
            let registry = self.registries[&group].clone();
            let engine = RaftGroupEngine::new_node_with_log_store_and_network(
                placement,
                node_id,
                self.config.clone(),
                InProcessRaftNetworkFactory::new(registry.clone())
                    .with_source(node_id)
                    .with_policy(self.policy.clone()),
                store,
                None,
                None,
            )
            .await
            .expect("start a journal-backed replica");
            registry.register(node_id, engine.raft_handle());
            self.engines.insert((group, node_id), engine);
        }
    }

    async fn stop_node(&mut self, node_id: u64) {
        for group in JOURNAL_GROUPS {
            self.registries[&group].unregister(node_id);
            let engine = self
                .engines
                .remove(&(group, node_id))
                .expect("running replica");
            engine.shutdown().await.expect("stop the replica");
        }
    }

    async fn leader(&self, group: u32) -> u64 {
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
    async fn append(&mut self, group: u32, count: usize) {
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
    async fn follower_of_every_group(&self, seed: u64) -> u64 {
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

    async fn durable_logs(&self, node_id: u64) -> BTreeMap<u32, DurableGroupLog> {
        let mut logs = BTreeMap::new();
        for group in JOURNAL_GROUPS {
            let store = self.wals[&node_id]
                .store(RaftGroupId(group))
                .expect("a running replica holds its store");
            logs.insert(group, DurableGroupLog::read(&store).await);
        }
        logs
    }

    /// Every acknowledged write is readable from every replica.
    async fn verify_reads(&self) {
        for ((group, node_id), engine) in &self.engines {
            let expected = &self.acknowledged[group];
            let mut last = Vec::new();
            for _ in 0..100 {
                last = engine
                    .sim_read_local_stream(
                        ReadStreamRequest {
                            stream_id: group_stream(*group),
                            offset: 0,
                            max_len: expected.len().saturating_add(64),
                            now_ms: 0,
                            leader_only: false,
                            read_index: None,
                        },
                        group_placement(*group),
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
            let journal = core_journal(cluster.wals[&victim].root());
            SimDisk::inject_fault(&journal, fault).expect("arm the fault");

            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            assert!(
                cluster.wait_stopped_by_storage_error(victim).await,
                "seed {seed}: the error stops every replica on node {victim}"
            );
            let last = store.get_log_state().await.expect("log state").last_log_id;
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

/// A single-node group with enough history that a purge rewrites its
/// journal online (the simulator lowers the reclaim threshold).
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

/// A purge rewrites a large journal online, writing every group's live
/// entries in several chunks (the simulator lowers the chunk size). The
/// rewrite replaces the journal with a rename and a directory `fsync`, so a
/// power loss right after it keeps the purge and every retained entry.
#[test]
fn online_reclaim_survives_power_loss() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("ONLINE_RECLAIM_SEEDS", &[1, 2, 3]) {
        run_with_madsim(seed, async move {
            let group = ReclaimGroup::start("online-reclaim").await;
            group.purge().await;
            let metrics = group.metrics.snapshot();
            assert!(
                metrics.wal_reclaims >= 1,
                "seed {seed}: the purge rewrote the journal online"
            );
            assert_eq!(metrics.wal_reclaim_failures, 0);
            let before = group.durable_log().await;
            assert_eq!(group.power_loss_and_reopen().await, before);
        });
    }
}

/// A rewrite that fails before it replaces the journal leaves the journal as
/// it was: the purge that triggered it stays acknowledged, the failure is
/// counted, the group keeps writing, and a power loss loses nothing.
#[test]
fn a_failed_online_reclaim_keeps_the_journal_and_the_group_running() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("ONLINE_RECLAIM_SEEDS", &[1, 2, 3]) {
        run_with_madsim(seed, async move {
            let mut group = ReclaimGroup::start("failed-reclaim").await;
            let next_generation = core_journal(group.wal.root()).with_extension("compact");
            SimDisk::inject_fault(&next_generation, SimDiskFault::Write)
                .expect("fail the next generation");
            group.purge().await;
            let metrics = group.metrics.snapshot();
            assert!(
                metrics.wal_reclaim_failures >= 1,
                "seed {seed}: the rewrite failed"
            );

            group.append(120..130).await;
            let raft_metrics =
                openraft::rt::WatchReceiver::borrow_watched(&group.engine.raft_handle().metrics())
                    .clone();
            assert!(
                raft_metrics.running_state.is_ok(),
                "seed {seed}: the group keeps running"
            );
            let before = group.durable_log().await;
            assert_eq!(group.power_loss_and_reopen().await, before);
        });
    }
}

/// A rewrite whose directory `fsync` fails leaves it unknown which
/// generation a crash keeps, so it poisons the writer. The purge that
/// triggered it was durable and stays acknowledged, and a power loss then
/// recovers either generation with every acknowledged entry.
#[test]
fn a_reclaim_that_cannot_publish_its_generation_poisons_the_writer() {
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

/// A single-node group whose synced entries are followed by an unsynced
/// tail of committed markers, after a power loss that may reorder the tail's
/// writeback. Returns what was synced and how a store reopened in `mode`
/// fared.
async fn reopen_after_a_reordered_unsynced_tail(
    mode: JournalReplayMode,
) -> (DurableGroupLog, Result<DurableGroupLog, String>) {
    let wal = SimNodeWal::provision("unsynced-tail");
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
    let (first, last) = (
        *synced.log_ids.first().expect("synced entries"),
        *synced.log_ids.last().expect("synced entries"),
    );
    for marker in 0..256 {
        let committed = if marker % 2 == 0 { first } else { last };
        store
            .save_committed(Some(committed))
            .await
            .expect("journal a committed marker");
    }
    drop(store);
    let report = wal.power_loss().await;
    let reopened = DurableRaftLogStoreFactory::new(wal.root())
        .with_replay_mode(mode)
        .open(placement, metrics);
    let reopened = match reopened {
        Ok(store) => Ok(DurableGroupLog::read(&store).await),
        Err(err) => Err(format!("{report:?}: {}", err.message())),
    };
    (synced, reopened)
}

const UNSYNCED_TAIL_SEEDS: [u64; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

/// Strict recovery expects every write on disk, so it refuses the hole a
/// reordered writeback of unsynced committed markers can leave. It never
/// recovers less than what was synced.
#[test]
fn strict_recovery_refuses_a_reordered_unsynced_tail() {
    let _guard = sim_test_guard();
    let mut refused = 0;
    for seed in seeds_from_env("UNSYNCED_TAIL_SEEDS", &UNSYNCED_TAIL_SEEDS) {
        let (synced, reopened) = run_with_madsim(
            seed,
            reopen_after_a_reordered_unsynced_tail(JournalReplayMode::Strict),
        );
        match reopened {
            Ok(recovered) => assert_eq!(recovered, synced, "seed {seed}"),
            Err(err) => {
                assert!(err.contains("checksum mismatch"), "seed {seed}: {err}");
                refused += 1;
            }
        }
    }
    assert!(
        refused > 0,
        "a reordered writeback must sometimes leave a hole strict recovery refuses"
    );
}

/// Recovery that keeps the verified prefix recovers every synced entry and
/// the vote after the same power losses.
#[test]
fn verified_prefix_recovery_keeps_every_acknowledged_write_after_a_reordered_tail() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("UNSYNCED_TAIL_SEEDS", &UNSYNCED_TAIL_SEEDS) {
        let (synced, reopened) = run_with_madsim(
            seed,
            reopen_after_a_reordered_unsynced_tail(JournalReplayMode::VerifiedPrefix),
        );
        assert_eq!(
            reopened.unwrap_or_else(|err| panic!("seed {seed}: {err}")),
            synced,
            "seed {seed}"
        );
    }
}
