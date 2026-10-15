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

use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use ursula_config::WalFsync;
use ursula_raft::JournalTuning;
use ursula_raft::RecoveryGateStatus;
use ursula_raft::RecoveryState;
use ursula_raft::UrsulaAppendEntriesRequest;
use ursula_raft::UrsulaAppendEntriesResponse;
use ursula_raft::wal::diagnostics::GroupLogState;
use ursula_raft::wal::diagnostics::JournalDisk;
use ursula_raft::wal::diagnostics::JournalReplayMode;
use ursula_raft::wal::diagnostics::PreviousRun;
use ursula_raft::wal::diagnostics::RUN_STATE_FILE;
use ursula_raft::wal::diagnostics::RecoveryReason;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::RaftGroupId;

use super::seeds_from_env;
use super::sim_test_guard;
use crate::madsim_harness::journal_cluster::JOURNAL_GROUPS;
use crate::madsim_harness::journal_cluster::JOURNAL_POWER_LOSS_SEEDS;
use crate::madsim_harness::journal_cluster::JournalCluster;
use crate::madsim_harness::journal_cluster::NODES;
use crate::madsim_harness::journal_cluster::gate;
use crate::madsim_harness::journal_cluster::group_placement;
use crate::madsim_harness::journal_cluster::group_stream;
use crate::madsim_harness::journal_cluster::lead_every_group;
use crate::madsim_harness::journal_cluster::leader_by_state;
use crate::madsim_harness::journal_cluster::log_state;
use crate::madsim_harness::journal_cluster::member_view;
use crate::madsim_harness::journal_cluster::metrics;
use crate::madsim_harness::journal_cluster::recover_group;
use crate::madsim_harness::journal_cluster::wait_leader;
use crate::madsim_harness::journal_cluster::wait_plan_satisfied;
use crate::madsim_harness::recovery_wiring::RECOVERY_STALL_AFTER;
use crate::madsim_harness::run_with_madsim;
use crate::madsim_harness::sim_wal::SimNodeWal;

#[path = "recovery_regression_tests.rs"]
mod regression;

#[path = "foreign_entry_tests.rs"]
mod foreign_entry;

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
            let committed = leader_metrics.local_committed.map(|log_id| log_id.index);
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

/// No vote was granted by a replica whose gate was closed.
fn assert_no_vote_while_gated(cluster: &JournalCluster, context: &str) {
    // A planned replica votes for its plan's leader (#581 section 4.5).
    let granted = cluster
        .votes
        .granted_while_gated()
        .into_iter()
        .filter(|answer| {
            answer.target_gate != Some(RecoveryGateStatus::Planned)
                || !answer
                    .source
                    .is_some_and(|source| cluster.plan_leaders.contains(&source))
        })
        .collect::<Vec<_>>();
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

/// Power loss of `node_id` that loses every page written since the last
/// `fsync`, and its restart.
async fn unsynced_loss_and_restart(cluster: &mut JournalCluster, node_id: u64) -> usize {
    cluster.stop_node(node_id).await;
    let report = cluster.wals[&node_id].power_loss_losing_unsynced().await;
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
                        .elect(false)
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

            // The operator runs the recovery tool: one plan per group, sent
            // to every reachable member, the leader last (#581 section 4.5).
            let mut points = BTreeMap::new();
            for group in JOURNAL_GROUPS {
                let plan = recover_group(&mut cluster, group, &NODES, &context).await;
                points.insert(group, plan.point);
            }
            for group in JOURNAL_GROUPS {
                wait_leader(&cluster, group, &context).await;
                wait_plan_satisfied(&cluster, group, &NODES, points[&group], &context).await;
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

/// Waits until `group` commits a fresh append through a replica in the
/// leader state, and records it as acknowledged.
async fn wait_writable(cluster: &mut JournalCluster, group: u32, context: &str) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
    let payload = format!("g{group}-writable;").into_bytes();
    loop {
        if let Some(leader) = leader_by_state(cluster, group) {
            let engine = cluster
                .engines
                .get_mut(&(group, leader))
                .expect("running leader replica");
            let append = engine.append(
                AppendRequest::from_bytes(group_stream(group), payload.clone()),
                group_placement(group),
                ColdWriteAdmission::default(),
            );
            match madsim::time::timeout(Duration::from_secs(2), append).await {
                Ok(Ok(_)) => {
                    cluster
                        .acknowledged
                        .entry(group)
                        .or_default()
                        .extend_from_slice(&payload);
                    return;
                }
                Ok(Err(_refused)) => {}
                Err(_elapsed) => panic!(
                    "{context}: group {group}: node {leader} leads but an append neither \
                     committed nor failed"
                ),
            }
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: group {group} takes no write; (node, state, term, last log, gate): {:?}",
            NODES
                .into_iter()
                .map(|node_id| {
                    let replica = metrics(cluster, group, node_id);
                    (
                        node_id,
                        replica.state,
                        replica.current_term,
                        replica.last_log_index,
                        gate(cluster, group, node_id),
                    )
                })
                .collect::<Vec<_>>()
        );
        madsim::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Three nodes whose journals rotate no segment during the test, so a power
/// loss drops exactly what followed the last clean restart. One node leads
/// every group, every group then commits six more entries, and both
/// followers lose power and those entries with it. They stay down for a
/// second, long enough for the leader's replication to them to back off, so
/// their conflicts reach it one at a time once they are back. Returns the
/// cluster with the followers down, the surviving leader and the followers.
async fn followers_lose_a_committed_tail(
    name: &str,
    context: &str,
) -> (JournalCluster, u64, Vec<u64>) {
    // Segments larger than the test writes: no rotation `fsync`s the tail.
    let mut cluster = JournalCluster::start_with_tuning(name, JournalTuning {
        segment_bytes: 1024 * 1024,
        ..JournalTuning::new(WalFsync::Never)
    })
    .await;
    for group in JOURNAL_GROUPS {
        cluster.append(group, 4).await;
    }
    // The fsync point: a clean restart of each node `fsync`s its journal, so
    // the followers lose exactly what follows it.
    for node_id in NODES {
        let wal = cluster.wals[&node_id].clone();
        wal.clean_shutdown(cluster.stop_node(node_id)).await;
        cluster.start_node(node_id).await;
        wait_healed(&cluster, context, Duration::from_secs(5)).await;
    }
    let survivor = wait_leader(&cluster, 0, context).await;
    lead_every_group(&cluster, survivor, context).await;
    for group in JOURNAL_GROUPS {
        cluster.append(group, 6).await;
    }
    let followers = NODES
        .into_iter()
        .filter(|node_id| *node_id != survivor)
        .collect::<Vec<_>>();
    cluster.votes.clear();
    for node_id in &followers {
        cluster.stop_node(*node_id).await;
    }
    for node_id in &followers {
        cluster.wals[node_id].power_loss_losing_unsynced().await;
    }
    madsim::time::sleep(Duration::from_secs(1)).await;
    (cluster, survivor, followers)
}

/// Restarts the followers of [`followers_lose_a_committed_tail`]: they come
/// back gated, each with a shorter log than the survivor in every group.
async fn restart_shorter_followers(
    cluster: &mut JournalCluster,
    survivor: u64,
    followers: &[u64],
    context: &str,
) {
    for node_id in followers {
        cluster.start_node(*node_id).await;
        assert_gated(cluster, *node_id, context);
    }
    for group in JOURNAL_GROUPS {
        let survivor_log = metrics(cluster, group, survivor).last_log_index;
        for node_id in followers {
            let mut store = cluster.wals[node_id]
                .store(RaftGroupId(group))
                .expect("running store");
            let last = store.get_log_state().await.expect("log state").last_log_id;
            assert!(
                last.map(|log_id| log_id.index) < survivor_log,
                "{context}: node {node_id} group {group} kept its unsynced tail"
            );
        }
    }
}

/// (e2) Both followers of every idle group lose a tail of committed entries
/// at once while the leader survives, and nothing is written afterwards.
/// They are a majority, so the leader cannot remove them: it rewinds their
/// replication and sends them its log again, with an unchanged membership
/// entry to carry the rewind. The first conflict to arrive must not make the
/// leader remove that follower while the other one has not answered yet: a
/// joint config that only the other follower could commit would wedge the
/// group until the heal step times out. Every group heals before a gated
/// replica reports it stalled, without an operator, and keeps every
/// acknowledged write.
#[test]
fn a_surviving_leader_rewinds_an_idle_majority_that_lost_committed_entries() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let (mut cluster, survivor, followers) =
                followers_lose_a_committed_tail("gate-idle-majority", &context).await;
            restart_shorter_followers(&mut cluster, survivor, &followers, &context).await;
            // Healed before any gated replica reports its group stalled:
            // nothing here needs an operator.
            wait_healed(&cluster, &context, RECOVERY_STALL_AFTER).await;
            assert_no_vote_while_gated(&cluster, &context);
            for group in JOURNAL_GROUPS {
                wait_writable(&mut cluster, group, &context).await;
            }
            cluster.verify_reads().await;
        });
    }
}

/// (e3) Both followers of every group lose power at once while the leader
/// survives, and their unsynced tail of committed entries with it. While
/// they are down, the survivor hands every group to one of them through the
/// production handoff entry point, as the cluster-egress yield and the
/// commit-stall watchdog did in the 0.7.0 S4 chaos run. The followers come
/// back gated with shorter logs: only the survivor holds every acknowledged
/// write, so only it can lead.
///
/// After at most the documented runbook (accept the loss on the gated
/// replicas with the longest logs that report their group stalled, until the
/// open replicas form a majority), every group takes writes again and keeps
/// every acknowledged write. A survivor parked in a leadership transfer that
/// cannot complete forwards every write to the target, sends no heartbeats,
/// never campaigns, and refuses every candidate with a shorter log without
/// adopting its term, so the group never recovers.
#[test]
fn a_handoff_to_a_powered_off_follower_never_parks_the_survivor() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let (mut cluster, survivor, followers) =
                followers_lose_a_committed_tail("gate-parked-handoff", &context).await;

            // The survivor has reached no peer for a few probe ticks and
            // hands every group it leads to a follower.
            let handoffs = ursula_raft::RaftGroupHandleRegistry::default();
            for group in JOURNAL_GROUPS {
                cluster.engines[&(group, survivor)]
                    .publish_recovery(cluster.rejoins[&(group, survivor)].clone(), &handoffs)
                    .expect("publish the survivor's bound engine");
                let handoff = handoffs
                    .transfer_leader(RaftGroupId(group), followers[0])
                    .await;
                tracing::info!(
                    group,
                    survivor,
                    target = followers[0],
                    ?handoff,
                    "handoff to a powered-off follower"
                );
            }

            restart_shorter_followers(&mut cluster, survivor, &followers, &context).await;

            // The runbook: once every gated replica either opened or
            // reports its group stalled, accept the loss on the longest
            // stalled replicas until the open replicas form a majority.
            let settled_by = madsim::time::Instant::now() + RECOVERY_STALL_AFTER * 4;
            while !JOURNAL_GROUPS.into_iter().all(|group| {
                followers.iter().all(|node_id| {
                    matches!(
                        gate(&cluster, group, *node_id),
                        RecoveryGateStatus::Open | RecoveryGateStatus::Stalled
                    )
                })
            }) {
                assert!(
                    madsim::time::Instant::now() < settled_by,
                    "{context}: the gated replicas neither opened nor stalled"
                );
                madsim::time::sleep(Duration::from_millis(50)).await;
            }
            for group in JOURNAL_GROUPS {
                let open = NODES
                    .into_iter()
                    .filter(|node_id| cluster.rejoins[&(group, *node_id)].vote_gate_open())
                    .count();
                if open < 2 && leader_by_state(&cluster, group).is_none() {
                    recover_group(&mut cluster, group, &NODES, &context).await;
                }
            }

            for group in JOURNAL_GROUPS {
                wait_writable(&mut cluster, group, &context).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
            drop(handoffs);
        });
    }
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

/// Both voters that do not initialize restart after the initializer ran
/// `Initialize` and before its first vote request reaches them. Their durable
/// genesis vote floor proves that they granted and acknowledged nothing
/// since, so they keep it and the new group still elects a leader. Dropped,
/// it would leave them refusing the initializer's votes while their genesis
/// probe sees it in a later term, and the group would never start.
#[test]
fn a_genesis_vote_floor_survives_a_restart_before_the_first_election() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster = JournalCluster::unstarted("genesis-floor-restart", WalFsync::Always);
            // Raft RPCs take two seconds. The bootstrap probes are not delayed,
            // so the floors and `Initialize` happen before any vote arrives.
            cluster.policy.set_delay(Some(Duration::from_secs(2)));
            for node_id in NODES {
                cluster.start_node(node_id).await;
            }
            let deadline = madsim::time::Instant::now() + Duration::from_secs(1);
            for group in JOURNAL_GROUPS {
                while !cluster.engines[&(group, 1)]
                    .raft_handle()
                    .is_initialized()
                    .await
                    .expect("read the initializer's state")
                {
                    assert!(
                        madsim::time::Instant::now() < deadline,
                        "{context}: group {group} was not initialized"
                    );
                    madsim::time::sleep(Duration::from_millis(5)).await;
                }
            }
            for node_id in [2, 3] {
                for group in JOURNAL_GROUPS {
                    let mut store = cluster.wals[&node_id]
                        .store(RaftGroupId(group))
                        .expect("a running replica holds its store");
                    assert_eq!(
                        store.read_vote().await.expect("read the vote"),
                        Some(ursula_raft::UrsulaVote::new(0, 0)),
                        "{context}: node {node_id} group {group} holds only its genesis floor"
                    );
                }
                let wal = cluster.wals[&node_id].clone();
                wal.clean_shutdown(cluster.stop_node(node_id)).await;
                cluster.start_node(node_id).await;
            }
            cluster.policy.set_delay(None);
            for group in JOURNAL_GROUPS {
                wait_leader(&cluster, group, &context).await;
            }
            cluster.wait_gates_open(Duration::from_secs(5)).await;
        });
    }
}

#[test]
fn another_power_loss_during_remove_voter_finishes_the_joint_without_losing_acks() {
    let _guard = sim_test_guard();
    // Both power losses drop every unsynced page, so every seed loses both
    // tails, whatever the schedule before them.
    for seed in seeds_from_env("JOINT_LOSS_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let mut cluster =
                JournalCluster::start_with_fsync("joint-second-loss", WalFsync::Never).await;
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
            let followers: Vec<_> = NODES.into_iter().filter(|id| *id != leader).collect();
            let (a, b) = (followers[0], followers[1]);
            // Keep B alive, but prevent it from acknowledging RemoveVoter(A).
            cluster.policy.partition_bidirectional(leader, b);
            cluster.policy.partition_bidirectional(a, b);
            assert!(unsynced_loss_and_restart(&mut cluster, a).await > 0);
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
            assert!(joint.local_committed < *joint.membership_config.log_id());
            // B loses its tail only after A's removal is pending.
            let before_loss = metrics(&cluster, joint_group, b).last_log_index;
            assert!(unsynced_loss_and_restart(&mut cluster, b).await > 0);
            assert!(metrics(&cluster, joint_group, b).last_log_index < before_loss);
            cluster.policy.clear();
            wait_healed(&cluster, &context, Duration::from_secs(15)).await;
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
        });
    }
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

/// M1 and M3 (#581 review): a replica that crashes the moment it applied
/// its plan's history event, before the removal of its record reached its
/// disk, restarts with the record and never votes before a leader's barrier
/// opens its gate; a plan's leader that crashes so restarts under the same
/// history and starts no second one.
#[test]
fn a_crash_between_the_plans_event_and_its_record_removal_keeps_one_history() {
    let _guard = sim_test_guard();
    let mut windows_hit = 0_usize;
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        windows_hit = windows_hit.saturating_add(run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster =
                JournalCluster::start_with_fsync("plan-event-crash", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            cluster.verify_reads().await;
            for node_id in NODES {
                cluster.stop_node(node_id).await;
            }
            for node_id in NODES {
                cluster.wals[&node_id].power_loss().await;
            }
            for node_id in NODES {
                cluster.start_node(node_id).await;
            }
            madsim::time::sleep(RECOVERY_STALL_AFTER * 2).await;
            cluster.votes.clear();

            // Group 1 recovers first; then group 0's member, or its plan's
            // leader, crashes the moment it applied the event.
            let plan = recover_group(&mut cluster, 1, &NODES, &context).await;
            wait_plan_satisfied(&cluster, 1, &NODES, plan.point, &context).await;
            let plan = recover_group(&mut cluster, 0, &NODES, &context).await;
            let crashed = if seed % 2 == 0 {
                NODES
                    .into_iter()
                    .find(|node_id| *node_id != plan.leader)
                    .expect("a member")
            } else {
                plan.leader
            };
            let mut watch = cluster.engines[&(0, crashed)]
                .history_watch()
                .expect("a raft group publishes its history");
            let deadline = madsim::time::Instant::now() + Duration::from_secs(15);
            let event = loop {
                if let Some(event) = watch
                    .current()
                    .filter(|event| event.event_index > plan.point)
                {
                    break event;
                }
                madsim::time::timeout(Duration::from_millis(50), watch.changed())
                    .await
                    .ok();
                assert!(
                    madsim::time::Instant::now() < deadline,
                    "{context}: node {crashed} never applied the plan's event"
                );
            };
            // Process death right at the event: the record's removal, if
            // queued, never reaches the disk.
            let store = cluster.wals[&crashed]
                .store(RaftGroupId(0))
                .expect("a running replica holds its store");
            store.abort_simulated_writer();
            drop(store);
            cluster.stop_node(crashed).await;
            cluster.wals[&crashed].process_crash().await;
            cluster.start_node(crashed).await;
            let window_hit = cluster.engines[&(0, crashed)].history().plan().is_some();
            if window_hit && crashed != plan.leader {
                assert_eq!(
                    gate(&cluster, 0, crashed),
                    RecoveryGateStatus::Planned,
                    "{context}: a member that restarts with its record votes only for its leader"
                );
            }
            wait_plan_satisfied(&cluster, 0, &NODES, plan.point, &context).await;
            for node_id in NODES {
                assert_eq!(
                    cluster.engines[&(0, node_id)].history().applied(),
                    Some(event),
                    "{context}: node {node_id}: a crash after the event started another history"
                );
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
            for group in JOURNAL_GROUPS {
                cluster.append(group, 2).await;
            }
            usize::from(window_hit)
        }));
    }
    assert!(
        windows_hit > 0,
        "no seed crashed a replica between its plan's event and the record's removal"
    );
}

/// The setup of the plan tests below: `voters` nodes whose every replica of
/// both groups lost power and is gated, until the groups stall.
async fn stalled_cluster(name: &str, voters: u64, survivors: &[u64]) -> JournalCluster {
    let survivors = survivors.to_vec();
    stalled_cluster_with(name, voters, move |_leaders| survivors).await
}

/// [`stalled_cluster`] whose survivors `survivors` picks from the groups'
/// leaders.
async fn stalled_cluster_with(
    name: &str,
    voters: u64,
    survivors: impl FnOnce(&[u64]) -> Vec<u64>,
) -> JournalCluster {
    let mut cluster =
        JournalCluster::start_voters(name, JournalTuning::new(WalFsync::Never), voters).await;
    for group in JOURNAL_GROUPS {
        cluster.append(group, 4).await;
    }
    cluster.verify_reads().await;
    let mut leaders = Vec::new();
    for group in JOURNAL_GROUPS {
        leaders.push(cluster.leader(group).await);
    }
    let survivors = survivors(&leaders);
    let lost = (1..=voters)
        .filter(|node_id| !survivors.contains(node_id))
        .collect::<Vec<_>>();
    for node_id in &lost {
        cluster.stop_node(*node_id).await;
    }
    for node_id in &lost {
        cluster.wals[node_id].power_loss().await;
    }
    for node_id in &lost {
        cluster.start_node(*node_id).await;
    }
    madsim::time::sleep(RECOVERY_STALL_AFTER * 2).await;
    cluster.votes.clear();
    cluster
}

/// Whether every replica of `group` in `nodes` applied a history event
/// above `point` and dropped its plan.
fn plan_satisfied_everywhere(
    cluster: &JournalCluster,
    group: u32,
    nodes: &[u64],
    point: u64,
) -> bool {
    nodes.iter().all(|node_id| {
        cluster
            .engines
            .get(&(group, *node_id))
            .is_some_and(|engine| {
                engine
                    .history()
                    .applied()
                    .is_some_and(|event| event.event_index > point)
                    && engine.history().plan().is_none()
            })
    })
}

/// M3 (#581 review): a plan's leader that crashes once elected, before its
/// history event committed. With its blank entry kept, it is elected again
/// and starts exactly one new history. When the crash lost the blank entry
/// its members hold, they refuse it (their logs are longer) and vote for no
/// one else: the group waits for a second plan, which elects a member with
/// the longest log. Either way every replica ends on one history.
#[test]
fn a_plan_leader_that_crashes_before_its_event_starts_one_history() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let mut cluster = stalled_cluster("plan-leader-crash", 3, &[]).await;
            for (group, loses_its_blank) in [(0, false), (1, true)] {
                // A crash of another group's leader restarted this group's
                // replica too: it reports the group stalled again first.
                madsim::time::sleep(RECOVERY_STALL_AFTER * 2).await;
                let plan = recover_group(&mut cluster, group, &NODES, &context).await;
                cluster.engines[&(group, plan.leader)]
                    .raft_handle()
                    .wait(Some(Duration::from_secs(10)))
                    .metrics(
                        |metrics| metrics.state == openraft::ServerState::Leader,
                        "the plan's leader is elected",
                    )
                    .await
                    .expect("the plan's leader is elected");
                if loses_its_blank {
                    // Process death that drops the writes still queued.
                    let store = cluster.wals[&plan.leader]
                        .store(RaftGroupId(group))
                        .expect("a running replica holds its store");
                    store.abort_simulated_writer();
                    drop(store);
                }
                cluster.stop_node(plan.leader).await;
                cluster.wals[&plan.leader].process_crash().await;
                cluster.start_node(plan.leader).await;
                let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
                while !plan_satisfied_everywhere(&cluster, group, &NODES, plan.point)
                    && madsim::time::Instant::now() < deadline
                {
                    madsim::time::sleep(Duration::from_millis(25)).await;
                }
                if !plan_satisfied_everywhere(&cluster, group, &NODES, plan.point) {
                    assert!(
                        loses_its_blank,
                        "{context}: group {group}: a leader that kept its log was not elected again"
                    );
                    let again = recover_group(&mut cluster, group, &NODES, &context).await;
                    wait_plan_satisfied(&cluster, group, &NODES, again.point, &context).await;
                }
                let event = cluster.engines[&(group, plan.leader)].history().applied();
                assert!(event.is_some_and(|event| event.event_index > plan.point));
                for node_id in NODES {
                    assert_eq!(
                        cluster.engines[&(group, node_id)].history().applied(),
                        event,
                        "{context}: group {group} node {node_id} holds another history"
                    );
                }
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            assert_no_vote_while_gated(&cluster, &context);
        });
    }
}

/// M3 (#581 review): five voters, three of which lost power: the two
/// survivors are no quorum, the group stalls, and a plan over all five
/// recovers it with one history and no vote from a gated replica.
#[test]
fn a_five_voter_group_that_lost_three_voters_recovers_through_a_plan() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let voters = [1, 2, 3, 4, 5];
            // Two followers of every group survive: a surviving leader
            // rewinds the others without a plan.
            let mut cluster = stalled_cluster_with("five-voters", 5, |leaders| {
                let followers = (1..=5)
                    .filter(|node_id| !leaders.contains(node_id))
                    .collect::<Vec<_>>();
                let first = usize::try_from(seed).unwrap_or(0) % followers.len();
                vec![followers[first], followers[(first + 1) % followers.len()]]
            })
            .await;
            for group in JOURNAL_GROUPS {
                assert_eq!(
                    leader_by_state(&cluster, group),
                    None,
                    "{context}: group {group} elected a leader without a quorum"
                );
            }
            for group in JOURNAL_GROUPS {
                let plan = recover_group(&mut cluster, group, &voters, &context).await;
                wait_plan_satisfied(&cluster, group, &voters, plan.point, &context).await;
                let event = cluster.engines[&(group, plan.leader)].history().applied();
                for node_id in voters {
                    assert_eq!(
                        cluster.engines[&(group, node_id)].history().applied(),
                        event,
                        "{context}: group {group} node {node_id} holds another history"
                    );
                }
            }
            assert_no_vote_while_gated(&cluster, &context);
            for group in JOURNAL_GROUPS {
                wait_leader(&cluster, group, &context).await;
                cluster.append(group, 2).await;
            }
        });
    }
}

/// M2 of the third #581 review: the plan's leader takes `lead` while it is
/// a stale leader (it lost its quorum but stays in OpenRaft's leader state)
/// or a candidate. It restarts its group before it answers, so it drops that
/// leadership or campaign: its durable vote for itself is uncommitted, it is
/// elected only above the plan term, and the plan is satisfied. The plan is
/// sent by hand, as the runbook does, over each replica's admin entry point.
#[test]
fn a_stale_leader_or_candidate_that_takes_a_plan_restarts_first() {
    let _guard = sim_test_guard();
    for (seed, stale_leader) in [(1_u64, true), (2, false), (3, true), (5, false)] {
        run_with_madsim(seed, async move {
            const P: u64 = 3;
            let context = format!("seed {seed} stale leader {stale_leader}");
            let mut cluster =
                JournalCluster::start_with_fsync("demote-on-plan", WalFsync::Never).await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            cluster.verify_reads().await;
            lead_every_group(&cluster, if stale_leader { P } else { 1 }, &context).await;
            for node_id in [1, 2] {
                cluster.policy.partition_bidirectional(P, node_id);
            }
            if !stale_leader {
                // P campaigns alone while the others still lead.
                madsim::time::sleep(Duration::from_secs(1)).await;
            }
            for node_id in [1, 2] {
                cluster.stop_node(node_id).await;
            }
            for node_id in [1, 2] {
                cluster.wals[&node_id].power_loss().await;
            }
            for node_id in [1, 2] {
                cluster.start_node(node_id).await;
            }
            // The others stall, and P's quorum acknowledgement grows old.
            madsim::time::sleep(RECOVERY_STALL_AFTER * 2 + Duration::from_secs(4)).await;
            cluster.votes.clear();
            let mut sent_plans = Vec::new();
            for group in JOURNAL_GROUPS {
                let state = metrics(&cluster, group, P).state;
                let expected = if stale_leader {
                    openraft::ServerState::Leader
                } else {
                    openraft::ServerState::Candidate
                };
                assert_eq!(state, expected, "{context}: group {group}");
                let mut members = Vec::new();
                for node_id in NODES {
                    members.push(member_view(&cluster, group, node_id).await);
                }
                let plan = ursula_ctl::recovery::plan_group(&members)
                    .unwrap_or_else(|error| panic!("{context}: group {group}: {error}"));
                assert_eq!(plan.leader, P, "{context}: group {group}: {plan:?}");
                cluster.plan_leaders.insert(P);
                let before = cluster.rejoins[&(group, P)].clone();
                let sent = ursula_ctl::recovery::send_plan(
                    &mut crate::madsim_harness::journal_cluster::SimPlanTransport {
                        cluster: &mut cluster,
                        group,
                    },
                    plan,
                    &members,
                )
                .await
                .unwrap_or_else(|error| panic!("{context}: group {group}: {error}"));
                assert!(sent.refused.is_empty(), "{context}: {:?}", sent.refused);
                // It restarted: a new replica, a follower, its vote for
                // itself uncommitted.
                assert!(
                    !std::sync::Arc::ptr_eq(&before, &cluster.rejoins[&(group, P)]),
                    "{context}: group {group}: P answered without restarting"
                );
                assert_ne!(
                    metrics(&cluster, group, P).state,
                    openraft::ServerState::Leader,
                    "{context}: group {group}"
                );
                let vote = cluster.wals[&P]
                    .store(RaftGroupId(group))
                    .expect("a running replica holds its store")
                    .read_vote()
                    .await
                    .expect("vote")
                    .expect("a vote");
                assert!(!vote.is_committed(), "{context}: group {group}: {vote:?}");
                sent_plans.push((group, plan, sent.leader_term));
            }
            for node_id in [1, 2] {
                cluster.policy.heal_bidirectional(P, node_id);
            }
            for (group, plan, leader_term) in sent_plans {
                wait_plan_satisfied(&cluster, group, &NODES, plan.point, &context).await;
                let leader = wait_leader(&cluster, group, &context).await;
                assert_eq!(leader, P, "{context}: group {group}");
                let term = metrics(&cluster, group, P).current_term;
                assert!(
                    term > leader_term,
                    "{context}: group {group}: elected at {term}, plan term {leader_term}"
                );
            }
            assert_no_vote_while_gated(&cluster, &context);
        });
    }
}
