//! Regression DST of the recovery gate and the run state: schedules that
//! once broke them, each asserting the property it broke.
//!
//! - An operator accepts the unsynced loss on the replica that led its
//!   groups, and it restarts before it records a newer vote, after a process
//!   crash and after a clean shutdown: it never leads again without an
//!   election and the log never forks. On the way, an acceptance opens only
//!   a stalled gate, and only for the log the operator saw.
//! - A node switches from `never` to `always` across a process crash, and its
//!   host loses power before the new run writes: every acknowledged write
//!   survives, or the group is gated.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::alias::EntryOf;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use ursula_config::WalFsync;
use ursula_proto::admin::AcceptUnsyncedLossRequest;
use ursula_raft::AcceptUnsyncedLossOutcome;
use ursula_raft::JournalTuning;
use ursula_raft::RecoveryGateError;
use ursula_raft::RecoveryGateStatus;
use ursula_raft::RecoveryState;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::wal::diagnostics::GroupLogState;
use ursula_raft::wal::diagnostics::PreviousRun;
use ursula_shard::RaftGroupId;

use super::Attempt;
use super::JOURNAL_GROUPS;
use super::JOURNAL_POWER_LOSS_SEEDS;
use super::JournalCluster;
use super::NODES;
use super::RECOVERY_STALL_AFTER;
use super::SimNodeWal;
use super::accept_observed_loss;
use super::assert_no_vote_while_gated;
use super::attempt_append;
use super::gate;
use super::group_placement;
use super::lead_every_group;
use super::leader_by_state;
use super::log_state;
use super::metrics;
use super::read_local;
use super::seeds_from_env;
use super::sim_test_guard;
use super::wait_healed;
use super::wait_leader;
use crate::madsim_harness::run_with_madsim;
use crate::madsim_harness::sim_wal::standalone_wal_metrics;

/// Every entry `node_id`'s replica of `group` holds up to `last`, by index:
/// its log id and its payload.
async fn entries(
    cluster: &JournalCluster,
    group: u32,
    node_id: u64,
    last: Option<u64>,
) -> BTreeMap<u64, (String, String)> {
    let Some(mut store) = cluster.wals[&node_id].store(RaftGroupId(group)) else {
        return BTreeMap::new();
    };
    let entries = match last {
        Some(last) => store.try_get_log_entries(..=last).await,
        None => store.try_get_log_entries(..).await,
    };
    entries
        .expect("read the log")
        .into_iter()
        .map(|entry| {
            (
                entry.log_id.index,
                (entry.log_id.to_string(), format!("{:?}", entry.payload)),
            )
        })
        .collect()
}

/// Raft's log matching property over the running replicas of `group`: two
/// entries with the same log id hold the same payload.
async fn assert_log_matching(cluster: &JournalCluster, group: u32, context: &str) {
    let mut seen = BTreeMap::<String, (u64, String)>::new();
    for node_id in NODES {
        for (log_id, payload) in entries(cluster, group, node_id, None).await.into_values() {
            match seen.get(&log_id) {
                Some((holder, held)) => assert_eq!(
                    held, &payload,
                    "{context}: nodes {holder} and {node_id} hold different entries under log id \
                     {log_id} of group {group}"
                ),
                None => {
                    seen.insert(log_id, (node_id, payload));
                }
            }
        }
    }
}

/// Appends `count` payloads of about 1 KiB to `group` through `leader`, so
/// that what the replicas write spans several pages.
async fn append_pages(cluster: &mut JournalCluster, group: u32, leader: u64, count: usize) {
    for index in 0..count {
        let payload = format!("{:-<1024};", format!("g{group}-page-{index}"));
        assert_eq!(
            attempt_append(cluster, group, leader, payload.as_bytes()).await,
            Attempt::Acknowledged,
            "group {group} refused a write through node {leader}"
        );
        cluster
            .acknowledged
            .entry(group)
            .or_default()
            .extend_from_slice(payload.as_bytes());
    }
}

/// How a node restarts.
#[derive(Debug, Clone, Copy)]
enum Restart {
    /// The process crashes; the page cache keeps every write.
    ProcessCrash,
    /// A graceful shutdown `fsync`s every journal and records a clean run.
    Clean,
}

/// Stops `node_id` as `restart` says and starts it again.
async fn restart(cluster: &mut JournalCluster, node_id: u64, restart: Restart) {
    match restart {
        Restart::ProcessCrash => {
            cluster.stop_node(node_id).await;
            cluster.wals[&node_id].process_crash().await;
        }
        Restart::Clean => {
            let wal = cluster.wals[&node_id].clone();
            wal.clean_shutdown(cluster.stop_node(node_id)).await;
        }
    }
    cluster.start_node(node_id).await;
}

/// Restarts `node_id` cleanly, which `fsync`s its journal, and waits until
/// the cluster healed.
async fn clean_restart(cluster: &mut JournalCluster, node_id: u64, context: &str) {
    restart(cluster, node_id, Restart::Clean).await;
    wait_healed(cluster, context, Duration::from_secs(5)).await;
}

/// An operator accepts the unsynced loss on the replica that led both
/// groups, and it restarts before it records a newer vote, once after a
/// process crash and once after a clean shutdown. It restarts as a follower
/// each time: its start behind the gate recorded its vote for itself
/// uncommitted. Restored as the leader of its old term over the log it kept,
/// it would append under the log ids of entries it lost, which the other
/// replicas still hold with other payloads, and the group's log would fork.
///
/// The followers `fsync` every entry before the power loss and the leader
/// keeps a random prefix of an unsynced tail of several pages, so its log is
/// usually the shortest. On the way, the operator's acceptance is refused
/// while the gate awaits a barrier and when it names a log the replica does
/// not hold.
#[test]
fn an_accepted_ex_leader_restarts_as_a_follower_and_the_log_never_forks() {
    let _guard = sim_test_guard();
    let mut shorter_ex_leaders = 0_usize;
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        let shorter = run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            // Segments larger than the test writes: no rotation `fsync`s
            // the tail.
            let mut cluster =
                JournalCluster::start_with_tuning("gate-accept-ex-leader", JournalTuning {
                    segment_bytes: 1024 * 1024,
                    ..JournalTuning::new(WalFsync::Never)
                })
                .await;
            for group in JOURNAL_GROUPS {
                cluster.append(group, 4).await;
            }
            // The fsync point: every node holds every entry so far on disk.
            for node_id in NODES {
                clean_restart(&mut cluster, node_id, &context).await;
            }
            let ex_leader = wait_leader(&cluster, 0, &context).await;
            lead_every_group(&cluster, ex_leader, &context).await;
            for group in JOURNAL_GROUPS {
                append_pages(&mut cluster, group, ex_leader, 8).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(5)).await;
            // The followers make the tail durable; the leader does not.
            for node_id in NODES.into_iter().filter(|node_id| *node_id != ex_leader) {
                clean_restart(&mut cluster, node_id, &context).await;
            }
            lead_every_group(&cluster, ex_leader, &context).await;
            let old_votes = JOURNAL_GROUPS
                .into_iter()
                .map(|group| (group, metrics(&cluster, group, ex_leader).vote))
                .collect::<BTreeMap<_, _>>();

            // Every node loses power at once: every replica is gated.
            for node_id in NODES {
                cluster.stop_node(node_id).await;
            }
            for node_id in NODES {
                cluster.wals[&node_id].power_loss().await;
            }
            for node_id in NODES {
                cluster.start_node(node_id).await;
            }
            // Long enough for the metrics to show the log, well short of a
            // stall.
            madsim::time::sleep(Duration::from_millis(200)).await;
            for group in JOURNAL_GROUPS {
                assert!(
                    matches!(
                        accept_observed_loss(&cluster, group, ex_leader).await,
                        Err(RecoveryGateError::NotStalled {
                            status: RecoveryGateStatus::AwaitingBarrier,
                            ..
                        })
                    ),
                    "{context}: group {group}: a gate awaiting a barrier accepted the loss"
                );
            }
            madsim::time::sleep(RECOVERY_STALL_AFTER * 2).await;

            let mut ex_leader_shorter = false;
            for group in JOURNAL_GROUPS {
                assert_eq!(leader_by_state(&cluster, group), None, "{context}");
                let mut lengths = BTreeMap::new();
                for node_id in NODES {
                    assert_eq!(
                        gate(&cluster, group, node_id),
                        RecoveryGateStatus::Stalled,
                        "{context}: node {node_id} group {group}"
                    );
                    lengths.insert(node_id, metrics(&cluster, group, node_id).last_log_index);
                }
                ex_leader_shorter |= lengths.values().any(|length| *length > lengths[&ex_leader]);

                // An acceptance that names another log changes nothing.
                let replica = metrics(&cluster, group, ex_leader);
                let other_log = AcceptUnsyncedLossRequest {
                    expected_last_log_index: Some(
                        replica
                            .last_log_index
                            .map_or(0, |index| index.saturating_add(1)),
                    ),
                    expected_current_term: replica.current_term,
                };
                assert!(
                    matches!(
                        cluster.rejoins[&(group, ex_leader)]
                            .accept_unsynced_loss(&other_log)
                            .await,
                        Err(RecoveryGateError::ReplicaChanged { .. })
                    ),
                    "{context}: group {group}: an acceptance of another log was not refused"
                );
                assert_eq!(
                    gate(&cluster, group, ex_leader),
                    RecoveryGateStatus::Stalled,
                    "{context}: group {group}"
                );

                // The operator accepts the loss on the ex-leader.
                assert_eq!(
                    accept_observed_loss(&cluster, group, ex_leader)
                        .await
                        .expect("accept the unsynced loss")
                        .outcome,
                    AcceptUnsyncedLossOutcome::GateOpened,
                    "{context}: group {group}"
                );
            }

            // It restarts twice before it records a newer vote: its process
            // crashes, and it shuts down cleanly. Which comes first alternates
            // with the seed, so each is the first restart after the
            // acceptance on some seeds.
            let restarts = if seed % 2 == 0 {
                [Restart::ProcessCrash, Restart::Clean]
            } else {
                [Restart::Clean, Restart::ProcessCrash]
            };
            for how in restarts {
                restart(&mut cluster, ex_leader, how).await;
                madsim::time::sleep(Duration::from_millis(300)).await;
                for group in JOURNAL_GROUPS {
                    // One open voter of three elects no leader.
                    assert_eq!(
                        leader_by_state(&cluster, group),
                        None,
                        "{context}: group {group}: after a {how:?} restart node {ex_leader} leads \
                         with vote {} (it led with {} before) while the other voters are gated",
                        metrics(&cluster, group, ex_leader).vote,
                        old_votes[&group]
                    );
                    assert_eq!(
                        log_state(&cluster, group, ex_leader),
                        GroupLogState::Initialized,
                        "{context}: group {group}: the accepted loss is recorded"
                    );
                }
            }
            // Only one voter is open: nothing commits, and no replica holds
            // two entries under one log id.
            for group in JOURNAL_GROUPS {
                for attempt in 0..4 {
                    assert_ne!(
                        attempt_append(
                            &mut cluster,
                            group,
                            ex_leader,
                            format!("lost-{attempt};").as_bytes()
                        )
                        .await,
                        Attempt::Acknowledged,
                        "{context}: group {group} committed with one open voter"
                    );
                }
                assert_log_matching(&cluster, group, &context).await;
            }

            // The operator opens the longest of the other replicas. The
            // group elects a leader among the open replicas and the last one
            // rejoins through a barrier.
            for group in JOURNAL_GROUPS {
                let second = NODES
                    .into_iter()
                    .filter(|node_id| *node_id != ex_leader)
                    .max_by_key(|node_id| metrics(&cluster, group, *node_id).last_log_index)
                    .expect("another voter");
                assert_eq!(
                    accept_observed_loss(&cluster, group, second)
                        .await
                        .expect("accept the unsynced loss")
                        .outcome,
                    AcceptUnsyncedLossOutcome::GateOpened,
                    "{context}: node {second} group {group}"
                );
            }
            wait_healed(&cluster, &context, Duration::from_secs(15)).await;
            for group in JOURNAL_GROUPS {
                assert_log_matching(&cluster, group, &context).await;
                let leader = wait_leader(&cluster, group, &context).await;
                let recovered = read_local(&cluster, group, leader).await;
                assert!(
                    cluster.acknowledged[&group].starts_with(&recovered),
                    "{context}: group {group} holds what was never acknowledged in that order"
                );
                cluster.acknowledged.insert(group, recovered);
            }
            for group in JOURNAL_GROUPS {
                cluster.append(group, 3).await;
            }
            wait_healed(&cluster, &context, Duration::from_secs(10)).await;
            for group in JOURNAL_GROUPS {
                assert_log_matching(&cluster, group, &context).await;
            }
            assert_no_vote_while_gated(&cluster, &context);
            cluster.verify_reads().await;
            ex_leader_shorter
        });
        shorter_ex_leaders = shorter_ex_leaders.saturating_add(usize::from(shorter));
    }
    assert!(
        shorter_ex_leaders > 0,
        "the ex-leader must sometimes keep a shorter log than another replica, or no seed could \
         fork the log"
    );
}

fn blank_entry(index: u64) -> EntryOf<UrsulaRaftTypeConfig> {
    use openraft::entry::RaftEntry;
    use openraft::vote::RaftLeaderId;

    EntryOf::<UrsulaRaftTypeConfig>::new(
        openraft::LogId {
            leader_id: openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
            index,
        },
        openraft::EntryPayload::Blank,
    )
}

/// A node acknowledges entries under `never` from the page cache and its
/// process crashes. The next run starts with `always`, and the host loses
/// power before that run writes to the journal. The run state then names
/// `always`, under which a host crash reads as losing nothing, so every entry
/// the `never` run acknowledged must have reached the disk before it was
/// recorded, or the group must come back gated.
#[test]
fn a_switch_to_always_after_a_process_crash_keeps_every_acknowledged_write_or_gates() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("RECOVERY_GATE_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        run_with_madsim(seed, async move {
            let context = format!("seed {seed}");
            let never = SimNodeWal::provision_with_fsync("gate-policy-switch", WalFsync::Never);
            let always = never.with_fsync(WalFsync::Always);
            let placement = group_placement(0);
            let metrics = standalone_wal_metrics(placement);

            // Entries 1..=4, made durable by a graceful shutdown.
            let mut store = never.open(placement, metrics.clone()).await;
            store
                .append((1..=4).map(blank_entry), IOFlushed::noop())
                .await
                .expect("append 1..=4");
            never
                .clean_shutdown(async move {
                    drop(store);
                })
                .await;

            // Entries 5..=10, acknowledged from the page cache; then the
            // process crashes.
            let mut store = never.open(placement, metrics.clone()).await;
            store
                .append((5..=10).map(blank_entry), IOFlushed::noop())
                .await
                .expect("append 5..=10");
            drop(store);
            never.process_crash().await;

            // A run under `always` starts and the host loses power before it
            // writes to the journal.
            assert_eq!(
                always.opening().previous_run,
                PreviousRun::ProcessCrash,
                "{context}"
            );
            always.power_loss().await;

            let opening = always.opening();
            let mut store = always.open(placement, metrics).await;
            let last = store
                .try_get_log_entries(..)
                .await
                .expect("read the log")
                .last()
                .map(|entry| entry.log_id.index);
            let gated = matches!(opening.recovery, RecoveryState::Recovering { .. })
                && store.log_state() == GroupLogState::Recovering;
            assert!(
                gated || last == Some(10),
                "{context}: the group is open with last index {last:?}, but entries up to 10 \
                 were acknowledged ({opening:?})"
            );
        });
    }
}
