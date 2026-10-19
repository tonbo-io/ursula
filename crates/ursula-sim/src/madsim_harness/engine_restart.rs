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
//!
//! The nodes write their snapshot objects to the faulty in-memory store
//! (#687), and the ownership checks of [`super::snapshot_checks`] run after
//! every restart and at a quiet point after the last one, so each reopen
//! leaves every node's directory holding only what it names (I5). A final
//! round wipes a follower whose build's PUT is held at its store's gate:
//! the shutdown abandons the PUT, so nothing the old process wrote stays.
//! Its negative control skips the wait in that one shutdown, and the PUT
//! lands after the new process started. Its publication was refused, so no
//! node names it (I1 holds), and it stays in the wiped node's directory
//! until the node's next pass: with that pass held, the quiet point finds
//! it (I5). In an in-process restart the WAL refuses to reopen the group
//! until the old work ended, and the restore's passes then delete the
//! object, so only a wipe, which starts a new WAL, shows it.

use std::time::Duration;

use openraft::async_runtime::OneshotSender;
use openraft::type_config::TypeConfigExt;
use ursula_config::WalFsync;
use ursula_raft::JournalTuning;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_runtime::AppendRequest;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::snapshot_harness::Gate;
use ursula_runtime::snapshot_harness::GatePass;
use ursula_runtime::snapshot_harness::PerMille;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::StoreOp;
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
use super::journal_cluster::NODES;
use super::journal_cluster::SnapshotStorePlan;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::journal_cluster::metrics;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::set_snapshot_store;
use super::journal_cluster::wait_leader;
use super::snapshot_checks::STEP;
use super::snapshot_checks::SnapshotChecks;

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

/// `raft.install_snapshot_timeout` of every node: the wait of a group's
/// second GC pass after a write that failed or a restore.
const INSTALL_TIMEOUT: Duration = Duration::from_secs(1);
/// The longest delay of a store request before and after it acts.
const MAX_DELAY: Duration = Duration::from_millis(20);
/// How long the wipe round waits for one of its steps.
const PATIENCE: Duration = Duration::from_secs(5);

/// The store's faults: request delays and the seed's late and lost PUTs.
fn store_faults(seed: u64) -> SnapshotStoreFaults {
    SnapshotStoreFaults::from_seed(seed, MAX_DELAY)
}

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
    /// The production shutdown in every round, and the shutdown that
    /// returns once RaftCore stopped in the final wipe round only.
    NoWorkWaitAcrossWipe,
}

/// The run's defences, drivers and snapshot store, reset when the run ends.
struct Switched;

impl Switched {
    fn apply(controls: Controls, seed: u64) -> Self {
        set_off(
            Defence::EngineWorkWait,
            matches!(
                controls,
                Controls::NoWorkWait | Controls::NoWorkWaitViewOnly
            ),
        );
        set_snapshot_drivers(Some(DRIVERS));
        set_snapshot_store(Some(SnapshotStorePlan {
            seed,
            faults: store_faults(seed),
            install_timeout: INSTALL_TIMEOUT,
        }));
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_off(Defence::EngineWorkWait, false);
        set_snapshot_drivers(None);
        set_snapshot_store(None);
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

/// The final round: a follower's build holds its PUT at the store's gate,
/// the follower loses its disk, and it installs from the leader. The old
/// process's shutdown abandons the PUT; under
/// [`Controls::NoWorkWaitAcrossWipe`] it skips the wait, and the PUT lands
/// once released, after the new process started. The new process's next
/// pass is held from before the release until the quiet point, which then
/// sees what the release left (I5).
async fn wipe_round(
    cluster: &mut JournalCluster,
    checks: &mut SnapshotChecks,
    trace: &mut SimTrace,
    controls: Controls,
    seed: u64,
    rng: &mut SplitMix64,
) {
    let context = "wipe round";
    let group = u32::try_from(rng.next_bounded(2)).expect("a journal group");
    let leader = wait_leader(cluster, group, context).await;
    let followers: Vec<u64> = NODES.into_iter().filter(|node| *node != leader).collect();
    let wiped = followers
        .get(usize::try_from(rng.next_bounded(2)).expect("an index"))
        .copied()
        .expect("a follower");
    // Every PUT lands as it answers, so the held one lands once released.
    let bucket = cluster.snapshot_bucket();
    let faults = bucket.faults();
    bucket.set_faults(SnapshotStoreFaults {
        late_put: PerMille::NEVER,
        lost_put: PerMille::NEVER,
        ..faults.clone()
    });

    // The old process's PUT, LIST and DELETE wait at its gates.
    let old = cluster.snapshot_gates(wiped);
    let put = Gate::before(wiped, StoreOp::WriteStaged);
    let old_list = Gate::before(wiped, StoreOp::ListOwn);
    let old_delete = Gate::before(wiped, StoreOp::DeleteOwn);
    let puts_before = old.arrived(put);
    old.hold(put);
    old.hold(old_list);
    old.hold(old_delete);
    cluster.append(group, 1).await;
    let last = metrics(cluster, group, leader).last_log_index;
    let deadline = madsim::time::Instant::now() + PATIENCE;
    while metrics(cluster, group, wiped)
        .last_applied
        .map(|applied| applied.index)
        < last
    {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {wiped} applied the write"
        );
        madsim::time::sleep(STEP).await;
    }
    cluster.engines[&(group, wiped)]
        .raft_handle()
        .trigger()
        .snapshot()
        .await
        .expect("queue a snapshot build on the follower");
    let deadline = madsim::time::Instant::now() + PATIENCE;
    while old.arrived(put) <= puts_before {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {wiped}'s build never reached its PUT"
        );
        madsim::time::sleep(STEP).await;
    }
    let held = old
        .log()
        .into_iter()
        .rev()
        .find(|event| event.gate == put && event.pass == GatePass::Arrived)
        .expect("the held PUT's arrival");

    // The disk goes, with the shutdown under the control skipping its wait.
    let skip_wait = controls == Controls::NoWorkWaitAcrossWipe;
    set_off(Defence::EngineWorkWait, skip_wait);
    cluster
        .wipe_node(
            wiped,
            &format!("engine-restart-{seed}-wiped-{wiped}"),
            JournalTuning::new(WalFsync::Never),
        )
        .await;
    set_off(Defence::EngineWorkWait, false);
    trace.push(SimEvent::NodeStopped { node_id: wiped });
    checks.step(cluster, trace, "node_wiped");
    let purged = cluster
        .try_snapshot_and_purge_node(group, leader, 0, PATIENCE)
        .await;
    if let Err(stage) = purged {
        panic!("{context}: group {group}: node {leader}: {stage}");
    }
    cluster.start_node(wiped).await;
    trace.push(SimEvent::NodeRestarted { node_id: wiped });
    cluster.wait_gates_open(PATIENCE).await;
    checks
        .wait_caught_up(cluster, trace, "wiped_node_started")
        .await;
    // The new process's pass after its restore, and the one an install
    // timeout later.
    checks
        .wait(
            cluster,
            trace,
            "wiped_node_started",
            INSTALL_TIMEOUT.saturating_add(DRIVERS.interval.saturating_mul(3)),
        )
        .await;

    // No pass of the new process lists the directory until the quiet point.
    let new = cluster.snapshot_gates(wiped);
    let new_list = Gate::before(wiped, StoreOp::ListOwn);
    new.hold(new_list);
    old.open(put);
    old.open(old_list);
    old.open(old_delete);
    if !skip_wait {
        assert!(
            old.log().iter().any(|event| event.gate == put
                && event.ticket == held.ticket
                && event.pass == GatePass::Abandoned),
            "{context}: node {wiped}'s shutdown abandoned the PUT of {}",
            held.subject
        );
    }
    checks
        .wait(
            cluster,
            trace,
            "old_put_released",
            Duration::from_millis(500),
        )
        .await;
    checks
        .quiesce(
            cluster,
            trace,
            &format!("old_put_released {}", held.subject),
        )
        .await;
    new.open(new_list);
    bucket.set_faults(faults);
}

pub(super) async fn run_engine_restart_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let seed = config.seed;
    let _switched = Switched::apply(controls, seed);
    let mut rng = SplitMix64::new(seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed });
    let mut cluster =
        JournalCluster::start_with_fsync(&format!("engine-restart-{seed}"), WalFsync::Never).await;
    let mut checks = SnapshotChecks::default();
    checks.step(&cluster, &mut trace, "cluster_started");
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
        checks.step(&cluster, &mut trace, "replica_restarted");
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
    // Every in-process reopen left each node's directory with only what it
    // names.
    checks
        .quiesce(&cluster, &mut trace, "replicas_restarted")
        .await;
    wipe_round(
        &mut cluster,
        &mut checks,
        &mut trace,
        controls,
        seed,
        &mut rng,
    )
    .await;
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

    /// Without the wait in the wiped follower's shutdown, the PUT its build
    /// held lands after the new process started. Its publication was
    /// refused, so no node names it and I1 holds, but it stays in the wiped
    /// node's directory until the node's next pass, which the run holds:
    /// the quiet point finds it (I5).
    #[test]
    fn negative_control_a_shutdown_that_skips_openraft_work_leaves_an_unnamed_object_after_a_wipe()
    {
        let _guard = sim_test_guard();
        let (after_event, message) = assert_fails_with(
            540,
            Controls::NoWorkWaitAcrossWipe,
            crate::madsim_harness::snapshot_checks::QUIET_PREFIXES,
        );
        let held = after_event
            .strip_prefix("old_put_released ")
            .unwrap_or_else(|| panic!("the quiet point after the release: {after_event}"));
        assert!(
            message.contains(&format!("holds {held}, which its metadata does not name")),
            "the held PUT stayed: {message}"
        );
    }

    /// Runs `seed` under `controls`, which must fail `invariant`, and
    /// returns the event the failure came after and its message.
    fn assert_fails_with(seed: u64, controls: Controls, invariant: &str) -> (String, String) {
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
        trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::InvariantFailed {
                    invariant: failed,
                    after_event,
                    message,
                } if failed == invariant => Some((after_event.clone(), message.clone())),
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!(
                    "{controls:?} did not fail `{invariant}` on seed {seed}: {:?}",
                    trace.events.last()
                )
            })
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
