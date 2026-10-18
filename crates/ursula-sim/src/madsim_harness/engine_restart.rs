//! A replica's engine restarts in its process while OpenRaft still runs
//! work for it (#709), on journal-backed replicas ([`JournalCluster`]).
//!
//! Invariant `engine_shutdown_leaves_no_work` (scenario
//! `engine-restart-leaves-no-work`): once a replica's engine shutdown
//! returns, nothing of the engine holds the group's log store, so the
//! replica starts again from it at once, nothing of it changes what the
//! node shows of the group (its log gauge, its builds in flight and its
//! snapshot metadata) before the replica starts again, and no replica
//! loses an acknowledged write. The nodes run the production snapshot
//! driver, so the group's gauge and builds are the node's. Each round appends to one of the cluster's groups,
//! may leave a write in flight while a follower restarts, may queue a
//! snapshot build on the replica it restarts, and restarts it after a
//! seeded pause: its state-machine worker may still hold queued applies and
//! the build, and a build may run detached. The negative control skips the
//! wait in the engine's shutdown (`Defence::EngineWorkWait`).

use std::time::Duration;

use openraft::async_runtime::OneshotSender;
use openraft::type_config::TypeConfigExt;
use ursula_config::WalFsync;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_runtime::AppendRequest;
use ursula_runtime::GroupWriteCommand;
use ursula_shard::RaftGroupId;
use ursula_stream::sim_controls::Defence;
use ursula_stream::sim_controls::set_off;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::wait_leader;

pub(super) const INVARIANT: &str = "engine_shutdown_leaves_no_work";

/// Restarts per run.
const ROUNDS: u64 = 8;

/// The snapshot driver every node runs: no removal is due within a run.
const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(600),
    keep: 8,
    interval: Duration::from_millis(200),
};

/// How long after a replica's engine stopped the run looks again at what
/// the node shows of the group, before the replica starts again.
const SETTLE: Duration = Duration::from_millis(50);

/// What a node shows of one group: its log gauge, its builds in flight and
/// its snapshot metadata.
#[derive(Debug, PartialEq, Eq)]
struct GroupView {
    progress: ursula_raft::snapshot_cadence::GroupLogProgress,
    builds: Option<u64>,
    metadata: Option<Vec<u8>>,
}

fn group_view(cluster: &JournalCluster, group: u32, node_id: u64) -> GroupView {
    let coordinator = cluster.coordinator(node_id);
    let path = cluster.wals[&node_id].snapshot_metadata_path(group_placement(group));
    GroupView {
        progress: coordinator.log_gauge(group).progress(),
        builds: coordinator.builds_in_flight().get(&group).copied(),
        metadata: SimDisk::read(&path).ok(),
    }
}

/// Which engine shutdown the run's replicas use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "only the negative controls build the controlled runs"
    )
)]
pub(super) enum Controls {
    /// The production shutdown, which waits for OpenRaft's work.
    #[default]
    Production,
    /// A shutdown that returns once RaftCore stopped, as before #709.
    NoWorkWait,
    /// The same shutdown, with only the view comparison of the invariant
    /// checked, and the first round holding a follower's worker with a
    /// committed apply queued behind it: the queued apply runs after the
    /// shutdown returned, so the view comparison alone catches it.
    NoWorkWaitViewOnly,
}

/// The run's defences, reset when the run ends.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_off(
            Defence::EngineWorkWait,
            matches!(
                controls,
                Controls::NoWorkWait | Controls::NoWorkWaitViewOnly
            ),
        );
        set_snapshot_drivers(Some(DRIVERS));
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_off(Defence::EngineWorkWait, false);
        set_snapshot_drivers(None);
    }
}

fn fail(trace: &mut SimTrace, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: INVARIANT.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{INVARIANT}` failed after `{after_event}`: {message}");
}

/// Holds `node_id`'s state-machine worker of `group`, appends one write
/// through `leader` and waits until the follower knows it committed, so its
/// apply queues behind the held worker. Returns the task that releases the
/// worker after a seeded while.
async fn hold_worker_with_a_queued_apply(
    cluster: &mut JournalCluster,
    group: u32,
    leader: u64,
    node_id: u64,
    round: u64,
    rng: &mut SplitMix64,
) -> madsim::task::JoinHandle<()> {
    let follower = cluster.engines[&(group, node_id)].raft_handle();
    let (entered_tx, entered_rx) = UrsulaRaftTypeConfig::oneshot();
    let (release_tx, release_rx) = UrsulaRaftTypeConfig::oneshot::<()>();
    let worker = follower.clone();
    let held = madsim::task::spawn(async move {
        // A worker that stopped first answers nothing, which is no failure
        // here.
        let _answered = worker
            .with_state_machine(move |_state_machine| {
                Box::pin(async move {
                    let _entered = entered_tx.send(());
                    let _released = release_rx.await;
                })
            })
            .await;
    });
    entered_rx
        .await
        .expect("the follower's worker runs the hold");
    let payload = format!("g{group}-r{round}-queued;").into_bytes();
    let written = cluster.engines[&(group, leader)]
        .raft_handle()
        .client_write(GroupWriteCommand::from(AppendRequest::from_bytes(
            group_stream(group),
            payload.clone(),
        )))
        .await
        .expect("a write while a follower's worker is held commits");
    cluster
        .acknowledged
        .entry(group)
        .or_default()
        .extend_from_slice(&payload);
    let index = written.log_id.index;
    follower
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .local_committed
                    .is_some_and(|committed| committed.index >= index)
            },
            "the held follower learns the write committed",
        )
        .await
        .expect("the held follower learns the write committed");
    let release_after = Duration::from_micros(rng.next_bounded(5_000));
    madsim::task::spawn(async move {
        madsim::time::sleep(release_after).await;
        let _released = release_tx.send(());
        held.await.expect("the hold's task");
    })
}

pub(super) async fn run_engine_restart_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let seed = config.seed;
    let mut rng = SplitMix64::new(seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed });
    let mut cluster =
        JournalCluster::start_with_fsync(&format!("engine-restart-{seed}"), WalFsync::Never).await;
    // Every group holds a write before the restarts, so the final reads
    // check both.
    for group in JOURNAL_GROUPS {
        cluster.append(group, 1).await;
    }
    for round in 0..ROUNDS {
        let context = format!("seed {seed} round {round}");
        let group = u32::try_from(rng.next_bounded(2)).expect("a journal group");
        let node_id = rng.next_bounded(3).saturating_add(1);
        let entries = usize::try_from(rng.next_bounded(4))
            .expect("an entry count")
            .saturating_add(1);
        cluster.append(group, entries).await;
        let leader = wait_leader(&cluster, group, &context).await;
        trace.push(SimEvent::LeaderElected { leader_id: leader });
        // The view-only control's first round restarts a follower.
        let node_id = if controls == Controls::NoWorkWaitViewOnly && round == 0 && node_id == leader
        {
            (leader % 3).saturating_add(1)
        } else {
            node_id
        };
        // A follower may restart with its state-machine worker held
        // and a committed apply queued behind it; the worker goes on a
        // seeded while later, after the shutdown began.
        let hold = node_id != leader
            && ((controls == Controls::NoWorkWaitViewOnly && round == 0)
                || rng.next_bounded(3) == 0);
        // Otherwise a follower may restart with a write in flight, which its
        // worker may hold as a queued apply. A leader's restart would leave
        // such a write's outcome unknown, so a leader restarts with none.
        // A held round has no write in flight, so the order in which the
        // group takes its writes stays known.
        let in_flight = (node_id != leader && !hold && rng.next_bounded(2) == 0).then(|| {
            let payload = format!("g{group}-r{round}-in-flight;").into_bytes();
            let raft = cluster.engines[&(group, leader)].raft_handle();
            let command = GroupWriteCommand::from(AppendRequest::from_bytes(
                group_stream(group),
                payload.clone(),
            ));
            let write = madsim::task::spawn(async move { raft.client_write(command).await });
            (payload, write)
        });
        let held = if hold {
            Some(
                hold_worker_with_a_queued_apply(
                    &mut cluster,
                    group,
                    leader,
                    node_id,
                    round,
                    &mut rng,
                )
                .await,
            )
        } else {
            None
        };
        if rng.next_bounded(2) == 0 {
            cluster.engines[&(group, node_id)]
                .raft_handle()
                .trigger()
                .snapshot()
                .await
                .expect("queue a snapshot build on the replica");
        }
        madsim::time::sleep(Duration::from_micros(rng.next_bounded(2_000))).await;

        cluster.stop_replica(group, node_id).await;
        trace.push(SimEvent::NodeStopped { node_id });
        let still_held = cluster.wals[&node_id].store(RaftGroupId(group)).is_some();
        if still_held && controls != Controls::NoWorkWaitViewOnly {
            fail(
                &mut trace,
                "replica_engine_stopped",
                format!(
                    "{context}: node {node_id}'s engine of group {group} returned from its \
                     shutdown while OpenRaft's work still held the group's log store"
                ),
            );
        }
        let stopped = group_view(&cluster, group, node_id);
        madsim::time::sleep(SETTLE).await;
        let settled = group_view(&cluster, group, node_id);
        if settled != stopped {
            fail(
                &mut trace,
                "replica_engine_stopped",
                format!(
                    "{context}: node {node_id}'s stopped engine of group {group} changed what \
                     the node shows of the group after its shutdown returned: {stopped:?} then \
                     {settled:?}"
                ),
            );
        }
        if let Some(held) = held {
            held.await.expect("the held worker's task");
        }
        cluster.start_replica(group, node_id).await;
        trace.push(SimEvent::NodeRestarted { node_id });
        if let Some((payload, write)) = in_flight {
            write
                .await
                .expect("the in-flight write's task")
                .expect("a write in flight while a follower restarts commits");
            cluster
                .acknowledged
                .entry(group)
                .or_default()
                .extend_from_slice(&payload);
        }
    }
    cluster.wait_gates_open(Duration::from_secs(5)).await;
    cluster.verify_reads().await;
    let leader = wait_leader(&cluster, 0, &format!("seed {seed}")).await;
    ThreeNodeRaftSimOutcome {
        seed,
        leader_id: leader,
        target_node_id: None,
        appended_log_index: ROUNDS,
        trace,
    }
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::INVARIANT;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::EngineRestartLeavesNoWork;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug()))
    }

    /// Every restart leaves nothing of the stopped engine, the same way
    /// twice per seed.
    #[test]
    fn engine_restart_leaves_no_work_replays() {
        let _guard = sim_test_guard();
        for seed in 540..=542 {
            let first = ThreeNodeRaftSim::run_engine_restart_with_controls_report(
                config(seed),
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_engine_restart_with_controls_report(
                config(seed),
                Controls::default(),
            );
            assert_eq!(first, second, "seed {seed}");
            assert_eq!(first.scenario, SCENARIO);
        }
    }

    /// Without the wait for OpenRaft's work, an engine's shutdown returns
    /// while its state-machine worker still holds the group's log store,
    /// and the replica could not start again from it at once.
    #[test]
    fn negative_control_a_shutdown_that_skips_openraft_work_leaves_the_store_held() {
        let _guard = sim_test_guard();
        let message = assert_fails(540, Controls::NoWorkWait);
        assert!(
            message.contains("returned from its shutdown while OpenRaft's work still held"),
            "{message}"
        );
    }

    /// Without the wait, a held follower's queued apply runs after its
    /// engine's shutdown returned: with the store check left out, the
    /// comparison of what the node shows of the group catches it.
    #[test]
    fn negative_control_a_shutdown_that_skips_openraft_work_changes_the_view_after_it() {
        let _guard = sim_test_guard();
        let message = assert_fails(540, Controls::NoWorkWaitViewOnly);
        assert!(
            message
                .contains("changed what the node shows of the group after its shutdown returned"),
            "{message}"
        );
    }

    /// Runs `seed` under `controls`, which must fail the invariant, and
    /// returns the failure's message.
    fn assert_fails(seed: u64, controls: Controls) -> String {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_engine_restart_with_controls_report(config(seed), controls)
        });
        std::panic::set_hook(previous_hook);
        let Err(_panic) = result else {
            panic!("{controls:?} left seed {seed} passing");
        };
        let trace = SimTrace::last_recorded();
        assert!(
            invariant_failed(&trace, INVARIANT),
            "{controls:?} did not fail `{INVARIANT}` on seed {seed}: {:?}",
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
}
