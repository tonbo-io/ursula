//! DST of the recovery gate on the disk WAL under `raft.wal.fsync = never`:
//! replicas that may have lost acknowledged entries (a host crash, or a lost
//! disk) never help elect a leader that lacks them, rejoin through a fresh
//! leader barrier, and a group whose majority lost entries stops until an
//! operator accepts the loss.
//!
//! Every scenario runs three nodes with two raft groups sharing each node's
//! core journal (`JournalCluster`), with the production gate, barrier
//! driver, heal driver and bootstrap on every node. The simulated network
//! records every vote answer with the target's gate at that moment.
//! Schedules that once broke the gate or the run state live in
//! `regression`.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::RaftMetrics;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use ursula_config::WalFsync;
use ursula_proto::admin::AcceptUnsyncedLossRequest;
use ursula_raft::AcceptUnsyncedLossOutcome;
use ursula_raft::AcceptUnsyncedLossReport;
use ursula_raft::RecoveryGateError;
use ursula_raft::RecoveryGateStatus;
use ursula_raft::UrsulaAppendEntriesRequest;
use ursula_raft::UrsulaAppendEntriesResponse;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::wal::GroupLogState;
use ursula_raft::wal::JournalDisk;
use ursula_raft::wal::JournalReplayMode;
use ursula_raft::wal::PreviousRun;
use ursula_raft::wal::RUN_STATE_FILE;
use ursula_raft::wal::RecoveryReason;
use ursula_raft::wal::RecoveryState;
use ursula_raft::wal::SimDisk;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::RaftGroupId;

use super::disk_tests::JOURNAL_GROUPS;
use super::disk_tests::JOURNAL_POWER_LOSS_SEEDS;
use super::disk_tests::JournalCluster;
use super::disk_tests::group_placement;
use super::disk_tests::group_stream;
use super::recovery_wiring::RECOVERY_STALL_AFTER;
use super::seeds_from_env;
use super::sim_test_guard;
use crate::madsim_harness::run_with_madsim;
use crate::madsim_harness::sim_wal::SimNodeWal;

#[path = "recovery_regression_tests.rs"]
mod regression;

const NODES: [u64; 3] = [1, 2, 3];

fn metrics(
    cluster: &JournalCluster,
    group: u32,
    node_id: u64,
) -> RaftMetrics<UrsulaRaftTypeConfig> {
    openraft::rt::WatchReceiver::borrow_watched(
        &cluster.engines[&(group, node_id)].raft_handle().metrics(),
    )
    .clone()
}

/// The replica of `group` that currently is in the leader state, if any.
fn leader_by_state(cluster: &JournalCluster, group: u32) -> Option<u64> {
    NODES.into_iter().find(|node_id| {
        cluster.engines.contains_key(&(group, *node_id))
            && metrics(cluster, group, *node_id).state == openraft::ServerState::Leader
    })
}

async fn wait_leader(cluster: &JournalCluster, group: u32, context: &str) -> u64 {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(leader) = leader_by_state(cluster, group) {
            return leader;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: group {group} elected no leader"
        );
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
}

fn gate(cluster: &JournalCluster, group: u32, node_id: u64) -> RecoveryGateStatus {
    cluster.rejoins[&(group, node_id)].status()
}

fn log_state(cluster: &JournalCluster, group: u32, node_id: u64) -> GroupLogState {
    cluster.wals[&node_id]
        .store(RaftGroupId(group))
        .expect("a running replica holds its store")
        .log_state()
}

/// Every group of `node_id` is gated and durably recovering.
fn assert_gated(cluster: &JournalCluster, node_id: u64, context: &str) {
    for group in JOURNAL_GROUPS {
        assert_ne!(
            gate(cluster, group, node_id),
            RecoveryGateStatus::Open,
            "{context}: node {node_id} group {group} is not gated"
        );
        assert_eq!(
            log_state(cluster, group, node_id),
            GroupLogState::Recovering,
            "{context}: node {node_id} group {group}"
        );
    }
}

/// Waits until every group has a leader and every replica's gate is open,
/// it is a voter of the leader's membership and it applied what the leader
/// committed.
async fn wait_healed(cluster: &JournalCluster, context: &str, timeout: Duration) {
    let deadline = madsim::time::Instant::now() + timeout;
    loop {
        let healed = JOURNAL_GROUPS.into_iter().all(|group| {
            let Some(leader) = leader_by_state(cluster, group) else {
                return false;
            };
            let leader_metrics = metrics(cluster, group, leader);
            let committed = leader_metrics.committed.map(|log_id| log_id.index);
            NODES.into_iter().all(|node_id| {
                let replica = metrics(cluster, group, node_id);
                cluster.rejoins[&(group, node_id)].vote_gate_open()
                    && leader_metrics
                        .membership_config
                        .voter_ids()
                        .any(|voter| voter == node_id)
                    && committed.is_some()
                    && replica.last_applied.map(|log_id| log_id.index) >= committed
            })
        });
        if healed {
            return;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the cluster never healed: {:?}",
            JOURNAL_GROUPS
                .into_iter()
                .flat_map(|group| NODES.into_iter().map(move |node_id| (group, node_id)))
                .map(|(group, node_id)| (
                    group,
                    node_id,
                    gate(cluster, group, node_id),
                    metrics(cluster, group, node_id).state,
                    metrics(cluster, group, node_id).last_applied,
                ))
                .collect::<Vec<_>>()
        );
        madsim::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The operator's acceptance of the unsynced loss on `node_id`'s replica of
/// `group`, naming the log the group's metrics show there.
async fn accept_observed_loss(
    cluster: &JournalCluster,
    group: u32,
    node_id: u64,
) -> Result<AcceptUnsyncedLossReport, RecoveryGateError> {
    let replica = metrics(cluster, group, node_id);
    cluster.rejoins[&(group, node_id)]
        .accept_unsynced_loss(&AcceptUnsyncedLossRequest {
            expected_last_log_index: replica.last_log_index,
            expected_current_term: replica.current_term,
        })
        .await
}

/// Moves the leadership of every group to `leader`.
async fn lead_every_group(cluster: &JournalCluster, leader: u64, context: &str) {
    for group in JOURNAL_GROUPS {
        let current = wait_leader(cluster, group, context).await;
        if current != leader {
            cluster.engines[&(group, current)]
                .raft_handle()
                .trigger()
                .transfer_leader(leader)
                .await
                .expect("transfer the leadership");
        }
        let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
        while leader_by_state(cluster, group) != Some(leader) {
            assert!(
                madsim::time::Instant::now() < deadline,
                "{context}: group {group} never moved to node {leader}"
            );
            madsim::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

/// No vote was granted by a replica whose gate was closed.
fn assert_no_vote_while_gated(cluster: &JournalCluster, context: &str) {
    let granted = cluster.votes.granted_while_gated();
    assert!(
        granted.is_empty(),
        "{context}: gated replicas granted votes: {granted:?}"
    );
}

/// What one append attempt that is expected to fail did.
#[derive(Debug, PartialEq, Eq)]
enum Attempt {
    Acknowledged,
    Refused,
    TimedOut,
}

async fn attempt_append(
    cluster: &mut JournalCluster,
    group: u32,
    node_id: u64,
    payload: &[u8],
) -> Attempt {
    let engine = cluster
        .engines
        .get_mut(&(group, node_id))
        .expect("running replica");
    let append = engine.append(
        AppendRequest::from_bytes(group_stream(group), payload.to_vec()),
        group_placement(group),
        ColdWriteAdmission::default(),
    );
    match madsim::time::timeout(Duration::from_secs(1), append).await {
        Ok(Ok(_)) => Attempt::Acknowledged,
        Ok(Err(_)) => Attempt::Refused,
        Err(_elapsed) => Attempt::TimedOut,
    }
}

async fn read_local(cluster: &JournalCluster, group: u32, node_id: u64) -> Vec<u8> {
    cluster.engines[&(group, node_id)]
        .sim_read_local_stream(
            ReadStreamRequest {
                stream_id: group_stream(group),
                offset: 0,
                max_len: 1 << 20,
                now_ms: 0,
                leader_only: false,
                read_index: None,
            },
            group_placement(group),
        )
        .await
        .expect("read the group's stream")
        .payload
        .to_vec()
}

/// Power loss of `node_id` under `never`, and its restart.
async fn power_loss_and_restart(cluster: &mut JournalCluster, node_id: u64) -> usize {
    cluster.stop_node(node_id).await;
    let report = cluster.wals[&node_id].power_loss().await;
    cluster.start_node(node_id).await;
    report.dropped_pages
}

fn assert_host_crash_opening(cluster: &JournalCluster, node_id: u64, context: &str) {
    let opening = cluster.wals[&node_id].opening();
    assert_eq!(
        (opening.previous_run, opening.replay_mode, opening.recovery),
        (
            PreviousRun::HostCrash {
                fsync: WalFsync::Never
            },
            JournalReplayMode::VerifiedPrefix,
            RecoveryState::Recovering {
                reason: RecoveryReason::HostCrash
            },
        ),
        "{context}: node {node_id}"
    );
}

/// (a) A follower loses power under `never` and comes back with a verified
/// prefix of its log. Every group it held is gated: while it is cut off from
/// the leader, a healthy follower campaigns and the gated replica refuses
/// its vote. Once it can reach the leader, the leader rebuilds it, it
/// applies a fresh barrier, its gate opens and records the group
/// initialized again. No acknowledged write is lost and no gated replica
/// ever grants a vote.
#[test]
fn a_follower_power_loss_rejoins_through_the_recovery_gate() {
    let _guard = sim_test_guard();
    let mut refused = 0_usize;
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        refused = refused.saturating_add(run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-follower-power-loss", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            let leaders = JOURNAL_GROUPS
                .into_iter()
                .filter_map(|group| leader_by_state(&cluster, group))
                .collect::<Vec<_>>();
            let healthy = NODES
                .into_iter()
                .find(|node_id| *node_id != victim && !leaders.contains(node_id));
            for leader in &leaders {
                cluster.policy.partition_bidirectional(victim, *leader);
            }
            power_loss_and_restart(&mut cluster, victim).await;
            assert_host_crash_opening(&cluster, victim, &context);
            assert_gated(&cluster, victim, &context);

            // A healthy follower campaigns while the victim cannot catch up.
            if let Some(healthy) = healthy {
                for group in JOURNAL_GROUPS {
                    cluster.engines[&(group, healthy)]
                        .raft_handle()
                        .trigger()
                        .elect()
                        .await
                        .expect("trigger an election");
                }
                madsim::time::sleep(Duration::from_millis(300)).await;
            }
            let refused = cluster.votes.answered_while_gated(victim);
            assert_no_vote_while_gated(&cluster, &context);

            cluster.policy.clear();
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(15)).await;
            for group in JOURNAL_GROUPS {
                assert_eq!(
                    log_state(&cluster, group, victim),
                    GroupLogState::Initialized,
                    "{context}: the open gate is recorded"
                );
            }
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
            refused
        }));
    }
    assert!(
        refused > 0,
        "a gated follower must sometimes be asked for its vote, and refuse"
    );
}

/// A follower loses its disk and comes back on a new, empty WAL directory,
/// holding nothing of either group. Its gates are closed with the groups'
/// history unknown: it would join only a new group's first election, and
/// both groups hold entries, so it refuses every vote. As the initializer of
/// a group it holds nothing of, it probes the other voters and does not
/// initialize the group again. Writes keep flowing while the leaders see it
/// lost the entries it had acknowledged and rebuild it through remove,
/// learner, catch-up and promote, with no operator. Its gates open once it
/// has applied a fresh barrier, and no acknowledged write is lost.
#[test]
fn a_follower_that_lost_its_disk_is_rebuilt_while_writes_continue() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-lost-disk", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            cluster.stop_node(victim).await;
            cluster.wals.insert(
                victim,
                SimNodeWal::provision_with_fsync(
                    &format!("gate-lost-disk-replacement-{victim}"),
                    WalFsync::Never,
                )
                .with_group_count(JOURNAL_GROUPS.len()),
            );
            cluster.start_node(victim).await;
            assert_eq!(
                cluster.wals[&victim].opening().previous_run,
                PreviousRun::Absent,
                "{context}: a new disk has no run state"
            );
            for group in JOURNAL_GROUPS {
                assert_eq!(
                    gate(&cluster, group, victim),
                    RecoveryGateStatus::AwaitingBarrier,
                    "{context}: node {victim} group {group}"
                );
                assert_eq!(
                    log_state(&cluster, group, victim),
                    GroupLogState::Empty,
                    "{context}: node {victim} group {group}"
                );
            }

            // Writes keep flowing while the emptied replica is rebuilt.
            for group in JOURNAL_GROUPS {
                cluster.append(group, 12).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(20)).await;
            for group in JOURNAL_GROUPS {
                assert_eq!(
                    log_state(&cluster, group, victim),
                    GroupLogState::Initialized,
                    "{context}: the open gate is recorded"
                );
            }
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
        });
    }
}

/// (b) The leader of a group loses power under `never`. The two followers
/// elect a new leader, which the restarted replica cannot join while it is
/// gated: it refuses their votes. It accepts the new leader's appends, applies
/// a fresh barrier and rejoins. Every acknowledged write survives on every
/// replica, although the old leader's power loss dropped unsynced pages.
#[test]
fn a_leader_power_loss_never_elects_from_the_lost_log() {
    let _guard = sim_test_guard();
    let mut dropped_pages = 0_usize;
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        dropped_pages = dropped_pages.saturating_add(run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-leader-power-loss", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let leader = wait_leader(&cluster, 0, &context).await;
            let dropped = power_loss_and_restart(&mut cluster, leader).await;
            assert_host_crash_opening(&cluster, leader, &context);
            assert_gated(&cluster, leader, &context);

            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(15)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
            dropped
        }));
    }
    assert!(
        dropped_pages > 0,
        "a leader's power loss under never must sometimes drop unsynced journal pages"
    );
}

/// Both followers of every group lose power at once under `never` while the
/// leader survives. They are a majority, so the leader cannot remove them;
/// it holds every committed entry, so it rewinds their replication and sends
/// them its log again. No operator is needed, writes resume, and every
/// acknowledged write is on every replica.
#[test]
fn a_surviving_leader_rewinds_a_majority_of_followers_that_lost_their_tail() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-surviving-leader", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            // One node leads every group.
            let leader = wait_leader(&cluster, 0, &context).await;
            lead_every_group(&cluster, leader, &context).await;
            let followers = NODES
                .into_iter()
                .filter(|node_id| *node_id != leader)
                .collect::<Vec<_>>();
            for node_id in &followers {
                cluster.stop_node(*node_id).await;
            }
            for node_id in &followers {
                cluster.wals[node_id].power_loss().await;
            }
            for node_id in &followers {
                cluster.start_node(*node_id).await;
                assert_gated(&cluster, *node_id, &context);
            }

            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(15)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
        });
    }
}

/// (c) A gated replica that crashes again before its gate opened comes back
/// gated: after a process crash (the run state reads normal) and after a
/// clean shutdown, its groups' recovering state is still in the metadata
/// file. Once it can reach its peers it rejoins with nothing lost.
#[test]
fn a_gated_replica_stays_gated_across_restarts() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-restarts", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            for peer in NODES.into_iter().filter(|node_id| *node_id != victim) {
                cluster.policy.partition_bidirectional(victim, peer);
            }
            power_loss_and_restart(&mut cluster, victim).await;
            assert_gated(&cluster, victim, &context);

            cluster.stop_node(victim).await;
            cluster.wals[&victim].process_crash().await;
            cluster.start_node(victim).await;
            let opening = cluster.wals[&victim].opening();
            assert_eq!(opening.previous_run, PreviousRun::ProcessCrash, "{context}");
            assert_eq!(opening.recovery, RecoveryState::Normal, "{context}");
            assert_gated(
                &cluster,
                victim,
                &format!("{context}, after a process crash"),
            );

            let wal = cluster.wals[&victim].clone();
            wal.clean_shutdown(cluster.stop_node(victim)).await;
            cluster.start_node(victim).await;
            assert_eq!(
                cluster.wals[&victim].opening().previous_run,
                PreviousRun::Clean,
                "{context}"
            );
            assert_gated(
                &cluster,
                victim,
                &format!("{context}, after a clean shutdown"),
            );

            cluster.policy.clear();
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(15)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
        });
    }
}

/// (d) Two voters lose power one after the other, with time to rejoin in
/// between (the second is a leader). Each rejoins through the gate and no
/// acknowledged write is lost.
#[test]
fn staggered_power_losses_of_two_voters_lose_nothing() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-staggered", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            let first = cluster.follower_of_every_group(seed).await;
            power_loss_and_restart(&mut cluster, first).await;
            assert_gated(&cluster, first, &context);
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;

            let leader = wait_leader(&cluster, 0, &context).await;
            let second = if leader == first {
                NODES
                    .into_iter()
                    .find(|node_id| *node_id != first)
                    .expect("another voter")
            } else {
                leader
            };
            power_loss_and_restart(&mut cluster, second).await;
            assert_gated(&cluster, second, &context);
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
        });
    }
}

/// (e) A majority loses power at once under `never`: every node on even
/// seeds; on odd seeds every node but one that leads no group, so each group
/// lost its leader. The gated replicas refuse every vote, so no group elects
/// a leader, every write is refused, and the gated replicas report their
/// groups stalled. The operator accepts the loss on the gated replicas with
/// the longest logs until, with the survivors, they form a quorum. Each
/// group then elects a leader and heals.
///
/// What is asserted about the data, per group: before the power loss every
/// node was cleanly restarted one at a time, which `fsync`s its journal, after
/// every replica applied every write acknowledged until then (the synced
/// prefix). After recovery the group's stream starts with the synced prefix
/// (nothing acknowledged before the last fsync point is lost) and is itself a
/// prefix of everything acknowledged (no write is reordered, invented or
/// resurrected; acknowledged writes after the fsync point may be lost).
#[test]
fn a_majority_power_loss_stops_until_the_operator_accepts_the_loss() {
    let _guard = sim_test_guard();
    let mut lost_bytes = 0_usize;
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        lost_bytes = lost_bytes.saturating_add(run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-majority", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            // The fsync point: every replica holds every acknowledged write,
            // and a clean restart of each node `fsync`s its journals.
            cluster.verify_reads().await;
            for node_id in NODES {
                let wal = cluster.wals[&node_id].clone();
                wal.clean_shutdown(cluster.stop_node(node_id)).await;
                cluster.start_node(node_id).await;
                wait_healed(&cluster, &context, Duration::from_secs(5)).await;
            }
            let synced = cluster.acknowledged.clone();
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }

            let survivors = if seed % 2 == 0 {
                Vec::new()
            } else {
                vec![cluster.follower_of_every_group(seed).await]
            };
            let lost = NODES
                .into_iter()
                .filter(|node_id| !survivors.contains(node_id))
                .collect::<Vec<_>>();
            cluster.votes.clear();
            for node_id in &lost {
                cluster.stop_node(*node_id).await;
            }
            for node_id in &lost {
                cluster.wals[node_id].power_loss().await;
            }
            for node_id in &lost {
                cluster.start_node(*node_id).await;
            }

            // Stopped: no leader, no write, the gated replicas say so.
            madsim::time::sleep(RECOVERY_STALL_AFTER * 2).await;
            for group in JOURNAL_GROUPS {
                assert_eq!(
                    leader_by_state(&cluster, group),
                    None,
                    "{context}: group {group} elected a leader from gated replicas"
                );
                for node_id in NODES {
                    let attempt = attempt_append(&mut cluster, group, node_id, b"refused;").await;
                    assert_eq!(
                        attempt,
                        Attempt::Refused,
                        "{context}: node {node_id} group {group}"
                    );
                }
                for node_id in &lost {
                    assert_eq!(
                        gate(&cluster, group, *node_id),
                        RecoveryGateStatus::Stalled,
                        "{context}: node {node_id} group {group}"
                    );
                    assert_eq!(
                        log_state(&cluster, group, *node_id),
                        GroupLogState::Recovering,
                        "{context}: node {node_id} group {group}"
                    );
                }
            }
            assert_no_vote_while_gated(&cluster, &context);

            // The operator accepts the loss, longest log first, until the
            // open replicas form a quorum.
            for group in JOURNAL_GROUPS {
                let mut gated = Vec::new();
                for node_id in &lost {
                    let mut store = cluster.wals[node_id]
                        .store(RaftGroupId(group))
                        .expect("running store");
                    let last = store.get_log_state().await.expect("log state").last_log_id;
                    gated.push((last, *node_id));
                }
                gated.sort_unstable_by(|a, b| b.cmp(a));
                let needed = 2_usize.saturating_sub(survivors.len());
                for (_, node_id) in gated.into_iter().take(needed) {
                    assert_eq!(
                        accept_observed_loss(&cluster, group, node_id)
                            .await
                            .expect("accept the unsynced loss")
                            .outcome,
                        AcceptUnsyncedLossOutcome::GateOpened,
                        "{context}: node {node_id} group {group}"
                    );
                }
            }
            for group in JOURNAL_GROUPS {
                wait_leader(&cluster, group, &context).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;

            let mut lost_bytes = 0_usize;
            for group in JOURNAL_GROUPS {
                let leader = wait_leader(&cluster, group, &context).await;
                let recovered = read_local(&cluster, group, leader).await;
                assert!(
                    recovered.starts_with(&synced[&group]),
                    "{context}: group {group} lost a write acknowledged before the fsync point"
                );
                assert!(
                    cluster.acknowledged[&group].starts_with(&recovered),
                    "{context}: group {group} holds what was never acknowledged in that order"
                );
                lost_bytes = lost_bytes.saturating_add(
                    cluster.acknowledged[&group]
                        .len()
                        .saturating_sub(recovered.len()),
                );
                cluster.acknowledged.insert(group, recovered);
            }
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            cluster.verify_reads().await;
            lost_bytes
        }));
    }
    // Not asserted to be positive: whether the unsynced tail survives a
    // power loss depends on the seed, and losing it is what the operator
    // accepted.
    tracing::info!(lost_bytes, "acknowledged bytes after the fsync point lost");
}

/// (f) A run state removed while the journals hold records reads as an
/// unknown history: the node reads its journals as a verified prefix, every
/// group it held is gated, and it rejoins with nothing lost.
#[test]
fn a_removed_run_state_with_journals_comes_back_gated() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("gate-unknown-history", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            let victim = cluster.follower_of_every_group(seed).await;
            cluster.stop_node(victim).await;
            cluster.wals[&victim].process_crash().await;
            SimDisk::remove_file(&cluster.wals[&victim].root().join(RUN_STATE_FILE))
                .expect("remove the run state");
            cluster.start_node(victim).await;
            let opening = cluster.wals[&victim].opening();
            assert_eq!(
                (opening.previous_run, opening.replay_mode, opening.recovery),
                (
                    PreviousRun::Unrecorded,
                    JournalReplayMode::VerifiedPrefix,
                    RecoveryState::Recovering {
                        reason: RecoveryReason::UnknownHistory
                    },
                ),
                "{context}"
            );
            assert_gated(&cluster, victim, &context);
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
        });
    }
}

/// A replica that voted in a newer term and then lost its log keeps that
/// vote, which the metadata file `fsync`s under any policy. A leader of the
/// older term, cut off while the others elected a new leader, can therefore
/// not use the emptied replica to commit: the replica answers its appends
/// with the newer vote.
#[test]
fn a_replica_that_lost_its_log_rejects_a_stale_term_leader() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let group = 0;
            let mut cluster =
                JournalCluster::start_with_fsync("gate-stale-term", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            let old_leader = wait_leader(&cluster, group, &context).await;
            let old_vote = metrics(&cluster, group, old_leader).vote;
            let others = NODES
                .into_iter()
                .filter(|node_id| *node_id != old_leader)
                .collect::<Vec<_>>();
            for node_id in &others {
                cluster.policy.partition_bidirectional(old_leader, *node_id);
            }
            // The other two elect a leader of a newer term.
            let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
            let new_leader = loop {
                if let Some(leader) = others.iter().copied().find(|node_id| {
                    let replica = metrics(&cluster, group, *node_id);
                    replica.state == openraft::ServerState::Leader
                        && replica.vote.leader_id().term() > old_vote.leader_id().term()
                }) {
                    break leader;
                }
                assert!(
                    madsim::time::Instant::now() < deadline,
                    "{context}: no new leader"
                );
                madsim::time::sleep(Duration::from_millis(25)).await;
            };
            let voter = others
                .iter()
                .copied()
                .find(|node_id| *node_id != new_leader)
                .expect("a voter of the new leader");
            let mut store = cluster.wals[&voter]
                .store(RaftGroupId(group))
                .expect("running store");
            let newer_vote = store.read_vote().await.expect("vote").expect("a vote");
            assert!(
                newer_vote.leader_id().term() > old_vote.leader_id().term(),
                "{context}: node {voter} voted in the new term"
            );
            drop(store);

            power_loss_and_restart(&mut cluster, voter).await;
            let mut store = cluster.wals[&voter]
                .store(RaftGroupId(group))
                .expect("running store");
            assert!(
                store.read_vote().await.expect("vote").expect("a vote") >= newer_vote,
                "{context}: the power loss cost node {voter} its vote"
            );
            drop(store);
            let response = cluster.engines[&(group, voter)]
                .raft_handle()
                .append_entries(UrsulaAppendEntriesRequest {
                    vote: old_vote,
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                })
                .await
                .expect("append entries");
            assert!(
                matches!(response, UrsulaAppendEntriesResponse::HigherVote(vote) if vote >= newer_vote),
                "{context}: node {voter} answered an older-term leader with {response:?}"
            );

            // The old leader reaches the emptied voter only: it cannot commit.
            cluster.policy.heal_bidirectional(old_leader, voter);
            let attempt = attempt_append(&mut cluster, group, old_leader, b"stale;").await;
            assert_ne!(attempt, Attempt::Acknowledged, "{context}");

            cluster.policy.clear();
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
            let leader = wait_leader(&cluster, group, &context).await;
            let mut acknowledged = BTreeMap::new();
            acknowledged.insert(group, read_local(&cluster, group, leader).await);
            assert_eq!(
                acknowledged[&group], cluster.acknowledged[&group],
                "{context}: the stale leader's write must never commit"
            );
            cluster.verify_reads().await;
        });
    }
}

/// A wiped disk loses votes too; fresh peer floors must fence stale leaders.
#[test]
fn a_wiped_voter_cannot_help_a_restarted_stale_leader_commit() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let group = 0;
            let mut cluster =
                JournalCluster::start_with_fsync("gate-wiped-stale-term", WalFsync::Always).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            let old_leader = wait_leader(&cluster, group, &context).await;
            let old_vote = metrics(&cluster, group, old_leader).vote;
            let others = NODES
                .into_iter()
                .filter(|node_id| *node_id != old_leader)
                .collect::<Vec<_>>();
            for node_id in &others {
                cluster.policy.partition_bidirectional(old_leader, *node_id);
            }
            // The other two elect a leader of a newer term.
            let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
            let new_leader = loop {
                if let Some(leader) = others.iter().copied().find(|node_id| {
                    let replica = metrics(&cluster, group, *node_id);
                    replica.state == openraft::ServerState::Leader
                        && replica.vote.leader_id().term() > old_vote.leader_id().term()
                }) {
                    break leader;
                }
                assert!(
                    madsim::time::Instant::now() < deadline,
                    "{context}: no new leader"
                );
                madsim::time::sleep(Duration::from_millis(25)).await;
            };
            let voter = others
                .iter()
                .copied()
                .find(|node_id| *node_id != new_leader)
                .expect("a voter of the new leader");
            assert_eq!(
                attempt_append(&mut cluster, group, new_leader, b"new-term;").await,
                Attempt::Acknowledged,
                "new leader commits with victim before disk loss"
            );
            cluster
                .acknowledged
                .get_mut(&group)
                .expect("group")
                .extend_from_slice(b"new-term;");
            cluster.policy.partition_bidirectional(new_leader, voter);
            cluster.stop_node(voter).await;
            cluster.wals.insert(
                voter,
                SimNodeWal::provision_with_fsync("wiped-voter-replacement", WalFsync::Always)
                    .with_group_count(JOURNAL_GROUPS.len()),
            );
            cluster.start_node(voter).await;
            // Restarting the stale leader erases its follower matched indexes.
            cluster.stop_node(old_leader).await;
            cluster.wals[&old_leader].process_crash().await;
            cluster.start_node(old_leader).await;
            // The old leader reaches the emptied voter only: it cannot commit.
            cluster.policy.heal_bidirectional(old_leader, voter);
            let attempt = attempt_append(&mut cluster, group, old_leader, b"stale;").await;
            assert_ne!(attempt, Attempt::Acknowledged, "{context}");

            cluster.policy.clear();
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
            let leader = wait_leader(&cluster, group, &context).await;
            let mut acknowledged = BTreeMap::new();
            acknowledged.insert(group, read_local(&cluster, group, leader).await);
            assert_eq!(
                acknowledged[&group], cluster.acknowledged[&group],
                "{context}: the stale leader's write must never commit"
            );
            cluster.verify_reads().await;
        });
    }
}

#[test]
fn bootstrap_with_absent_peers_advances_time_and_recovers_when_they_arrive() {
    let _guard = sim_test_guard();
    run_with_madsim(7, async {
        let mut cluster = JournalCluster::unstarted("bootstrap-absent", WalFsync::Never);
        cluster.start_node(1).await;
        let began = madsim::time::Instant::now();
        madsim::time::sleep(Duration::from_millis(250)).await;
        assert!(began.elapsed() >= Duration::from_millis(250));
        assert!(metrics(&cluster, 0, 1).current_leader.is_none());
        cluster.start_node(2).await;
        cluster.start_node(3).await;
        cluster.wait_gates_open(Duration::from_secs(5)).await;
    });
}

#[test]
fn mutable_membership_recovery_preserves_and_completes_the_joint() {
    let _guard = sim_test_guard();
    // With owner mailbox dispatch, seed 5 drops both tails. Keep the loss and
    // joint-membership preconditions asserted so scheduling drift cannot skip the scenario.
    for seed in seeds_from_env("JOINT_LOSS_SEEDS", &[5]) {
        run_with_madsim(seed, async move {
            let mut cluster =
                JournalCluster::start_with_fsync("joint-second-loss", WalFsync::Never).await;
            cluster.membership_authority = ursula_raft::RecoveryMembershipAuthority::CurrentLeader;
            let context = format!("joint second loss seed {seed}");
            for group in JOURNAL_GROUPS {
                cluster.append(group, 6).await;
            }
            let leader = wait_leader(&cluster, 0, &context).await;
            for group in JOURNAL_GROUPS {
                let current = wait_leader(&cluster, group, &context).await;
                if current != leader {
                    cluster.engines[&(group, current)]
                        .raft_handle()
                        .trigger()
                        .transfer_leader(leader)
                        .await
                        .unwrap();
                }
                cluster.engines[&(group, leader)]
                    .raft_handle()
                    .wait(Some(Duration::from_secs(5)))
                    .current_leader(leader, "align leaders")
                    .await
                    .unwrap();
            }
            // Create a fresh unsynced application tail after leadership alignment.
            for group in JOURNAL_GROUPS {
                cluster.append(group, 20).await;
            }
            let followers: Vec<_> = NODES.into_iter().filter(|id| *id != leader).collect();
            let (a, b) = (followers[0], followers[1]);
            for group in JOURNAL_GROUPS {
                let committed = metrics(&cluster, group, leader)
                    .last_applied
                    .map(|log| log.index());
                for follower in [a, b] {
                    cluster.engines[&(group, follower)]
                        .raft_handle()
                        .wait(Some(Duration::from_secs(5)))
                        .applied_index_at_least(committed, "both followers hold the unsynced tail")
                        .await
                        .unwrap();
                }
            }
            // Keep B alive, but prevent it from acknowledging RemoveVoter(A).
            cluster.policy.partition_bidirectional(leader, b);
            cluster.policy.partition_bidirectional(a, b);
            cluster.policy.partition_bidirectional(leader, a);
            power_loss_and_restart(&mut cluster, a).await;
            // A recovering replica now waits for peer vote floors before it
            // can return a lost-log conflict. Initiate the same RemoveVoter
            // transition explicitly while B is unavailable.
            let raft = cluster.engines[&(0, leader)].raft_handle();
            let remove = madsim::task::spawn(async move {
                raft.change_membership(std::collections::BTreeSet::from([leader, b]), false)
                    .await
            });
            let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
            let joint_group = loop {
                if let Some(group) = JOURNAL_GROUPS.into_iter().find(|group| {
                    metrics(&cluster, *group, leader)
                        .membership_config
                        .membership()
                        .get_joint_config()
                        .len()
                        > 1
                }) {
                    break group;
                }
                assert!(
                    madsim::time::Instant::now() < deadline,
                    "{context}: no joint configuration"
                );
                madsim::time::sleep(Duration::from_millis(1)).await;
            };
            let joint = metrics(&cluster, joint_group, leader);
            assert!(joint.committed < *joint.membership_config.log_id());
            // Crash B only after A's removal is pending. Without a meta
            // authority this changing configuration cannot be inferred from
            // static bootstrap peers, even if this crash retains every page.
            power_loss_and_restart(&mut cluster, b).await;
            cluster.policy.clear();
            // Both durable votes survived. Recovery must admit replication
            // under the current leader's vote, repair both suffixes, and let
            // OpenRaft finish the existing joint transition without inventing
            // another membership configuration.
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            madsim::time::timeout(Duration::from_secs(10), remove)
                .await
                .expect("joint transition completes after both prefixes repair")
                .expect("membership task joins")
                .expect("membership transition succeeds");
            let final_membership = metrics(&cluster, joint_group, leader);
            assert_eq!(
                final_membership
                    .membership_config
                    .membership()
                    .get_joint_config(),
                &vec![std::collections::BTreeSet::from([leader, b])]
            );
            for group in JOURNAL_GROUPS {
                assert_eq!(
                    read_local(&cluster, group, leader).await,
                    cluster.acknowledged[&group]
                );
                cluster.append(group, 1).await;
            }
        });
    }
}

#[test]
fn an_append_ack_delayed_across_reboot_and_barrier_is_fenced() {
    use openraft::RaftNetworkFactory;
    use openraft::RaftNetworkV2;
    let _guard = sim_test_guard();
    run_with_madsim(7, async {
        let mut cluster = JournalCluster::start_with_fsync("delayed-ack", WalFsync::Never).await;
        for group in JOURNAL_GROUPS {
            cluster.append(group, 3).await;
        }
        let leader = wait_leader(&cluster, 0, "delayed ack").await;
        let victim = NODES
            .into_iter()
            .find(|node| *node != leader)
            .expect("follower");
        let before = metrics(&cluster, 0, leader);
        let policy = ursula_raft::InProcessRaftNetworkPolicy::default();
        policy.set_append_response_delay(Some(Duration::from_secs(20)));
        let mut factory =
            ursula_raft::InProcessRaftNetworkFactory::new(cluster.registries[&0].clone())
                .with_source(leader)
                .with_policy(policy)
                .with_rejoin(cluster.rejoins[&(0, leader)].clone());
        let mut network = factory
            .new_client(victim, &openraft::BasicNode::default())
            .await;
        let pending = madsim::task::spawn(async move {
            network
                .append_entries(
                    UrsulaAppendEntriesRequest {
                        vote: before.vote,
                        prev_log_id: before.committed,
                        entries: Vec::new(),
                        leader_commit: before.committed,
                    },
                    openraft::network::RPCOption::new(Duration::from_secs(30)),
                )
                .await
        });
        madsim::time::sleep(Duration::from_millis(100)).await;
        power_loss_and_restart(&mut cluster, victim).await;
        wait_healed(&cluster, "delayed ack", Duration::from_secs(10)).await;
        assert!(
            matches!(
                pending.await.expect("reply task"),
                Err(openraft::error::RPCError::Network(_))
            ),
            "the pre-reboot response must not be usable after recovery"
        );
        cluster.verify_reads().await;
    });
}

/// A process crash loses a leader's enqueued (not yet written) suffix even
/// though both followers have already persisted and applied it. Restoring the
/// old committed self-vote must not reuse those log ids for different commands.
#[test]
fn leader_process_crash_with_pending_tail_never_reuses_log_ids() {
    let _guard = sim_test_guard();
    for fsync in [WalFsync::Always, WalFsync::Never] {
        for seed in [60, 61, 62] {
            run_with_madsim(seed, async move {
                let mut cluster = JournalCluster::start_with_fsync("pending-leader", fsync).await;
                let group = JOURNAL_GROUPS[0];
                for initial_group in JOURNAL_GROUPS {
                    cluster.append(initial_group, 2).await;
                }
                let leader = wait_leader(&cluster, group, "initial leader").await;
                let old_vote = metrics(&cluster, group, leader).vote;
                for engine in cluster.engines.values() {
                    engine.raft_handle().runtime_config().tick(false);
                }
                let store = cluster.wals[&leader].store(RaftGroupId(group)).unwrap();
                store.pause_simulated_writer(true);
                let raft = cluster.engines[&(group, leader)].raft_handle();
                let writing = raft.clone();
                let before = metrics(&cluster, group, leader).last_log_index.unwrap();
                let append = madsim::task::spawn(async move {
                    writing
                        .client_write(ursula_runtime::GroupWriteCommand::from(
                            AppendRequest::from_bytes(
                                group_stream(group),
                                b"lost-on-leader;".to_vec(),
                            ),
                        ))
                        .await
                });
                for follower in NODES.into_iter().filter(|id| *id != leader) {
                    cluster.engines[&(group, follower)]
                        .raft_handle()
                        .wait(Some(Duration::from_secs(2)))
                        .metrics(
                            |m| m.last_applied.is_some_and(|id| id.index() > before),
                            "follower durably applied pending leader tail",
                        )
                        .await
                        .unwrap();
                }
                assert!(
                    !append.is_finished(),
                    "leader must not acknowledge before local durability"
                );
                // Kill the writer before shutdown can flush the queued batch.
                store.abort_simulated_writer();
                let mut failed_store = store.clone();
                let failure = madsim::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if let Err(error) = failed_store.get_log_state().await {
                            break error;
                        }
                        madsim::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect("aborting the writer fails its pending append");
                assert!(matches!(
                    failure
                        .get_ref()
                        .and_then(|error| error
                            .downcast_ref::<ursula_raft::wal::diagnostics::CoreJournalError>()),
                    Some(ursula_raft::wal::diagnostics::CoreJournalError::WriterStopped { .. })
                ));
                drop(failed_store);
                drop(store);
                drop(raft);
                append.abort();
                cluster.stop_node(leader).await;
                cluster.wals[&leader].process_crash().await;
                cluster.start_node(leader).await;
                assert_eq!(
                    cluster.wals[&leader].opening().previous_run,
                    PreviousRun::ProcessCrash
                );
                let restarted = metrics(&cluster, group, leader);
                assert!(
                    !(restarted.state == openraft::ServerState::Leader
                        && restarted.vote == old_vote)
                );
                for engine in cluster.engines.values() {
                    engine.raft_handle().runtime_config().tick(true);
                }
                cluster
                    .acknowledged
                    .get_mut(&group)
                    .unwrap()
                    .extend_from_slice(b"lost-on-leader;");
                wait_healed(
                    &cluster,
                    "recover pending leader tail",
                    Duration::from_secs(10),
                )
                .await;
                cluster.append(group, 2).await;
                cluster.verify_reads().await;
            });
        }
    }
}
