//! A three-node cluster of journal-backed Raft groups on the simulated disk
//! ([`JournalCluster`]), with the production recovery gate and its drivers on
//! every replica, and the recovery tool's plan over it (#581 section 4.5).
//! The disk, recovery and recovery-plan simulations run on it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::RaftMetrics;
#[cfg(test)]
use openraft::alias::LogIdOf;
#[cfg(test)]
use openraft::alias::VoteOf;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use ursula_config::WalFsync;
use ursula_ctl::recovery::GroupPlan;
use ursula_ctl::recovery::PlanMember;
use ursula_ctl::recovery::PlanTransport;
use ursula_ctl::recovery::SendError;
use ursula_ctl::recovery::plan_group;
use ursula_ctl::recovery::send_plan;
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::admin::ReplicaLogView;
use ursula_raft::GroupRejoin;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::JournalTuning;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupEngineOptions;
#[cfg(test)]
use ursula_raft::RaftGroupFileLogStore;
use ursula_raft::RecoveryGateStatus;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::apply_failure::ApplyFault;
#[cfg(test)]
use ursula_raft::wal::diagnostics::GroupLogState;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FollowerRead;
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
use super::sim_wal::SimNodeWal;

/// The production snapshot driver on every node of a [`JournalCluster`]
/// (#669): its settings, and the entries OpenRaft keeps below a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DriverPlan {
    /// `raft.deleted_data_max_age`.
    pub(super) max_age: Duration,
    /// `raft.max_in_snapshot_log_to_keep`.
    pub(super) keep: u64,
    /// The pause between the driver's ticks.
    pub(super) interval: Duration,
}

std::thread_local! {
    /// The drivers the next [`JournalCluster`] on this simulation's thread
    /// runs ([`set_snapshot_drivers`]).
    static SNAPSHOT_DRIVERS: std::cell::Cell<Option<DriverPlan>> =
        const { std::cell::Cell::new(None) };
}

/// Makes the [`JournalCluster`]s this simulation's thread starts run the
/// production snapshot driver on every node, under OpenRaft's
/// `SnapshotPolicy::Never` as in production, or none with `None`. A
/// scenario sets it inside its simulation, which runs on a thread of its
/// own.
pub(super) fn set_snapshot_drivers(plan: Option<DriverPlan>) {
    SNAPSHOT_DRIVERS.with(|drivers| drivers.set(plan));
}

/// The cluster's nodes.
pub(super) const NODES: [u64; 3] = [1, 2, 3];

#[cfg(test)]
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

#[cfg(test)]
/// What one replica holds for a group, read from its log store.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct DurableGroupLog {
    pub(super) vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    pub(super) log_ids: Vec<LogIdOf<UrsulaRaftTypeConfig>>,
}

#[cfg(test)]
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

/// Restarts one node's replica of a group in a [`JournalCluster`], for a
/// recovery plan's cut.
struct ReplicaRestarter<'a> {
    cluster: &'a mut JournalCluster,
    node_id: u64,
}

impl ursula_raft::ReplicaRestart for ReplicaRestarter<'_> {
    fn restart_replica(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Option<std::sync::Arc<ursula_raft::GroupRejoin>>,
                        ursula_runtime::RuntimeError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.cluster
                .restart_replica(raft_group_id.0, self.node_id)
                .await;
            Ok(self
                .cluster
                .rejoins
                .get(&(raft_group_id.0, self.node_id))
                .cloned())
        })
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
    pub(super) faulty: Option<(u32, ApplyFault)>,
    /// Replicas whose startup replay stopped at a committed record, by the
    /// failing index. They have no engine.
    pub(super) replay_stopped: BTreeMap<(u32, u64), u64>,
    /// The leaders recovery plans elected: a planned replica votes for its
    /// plan's leader only.
    pub(super) plan_leaders: std::collections::BTreeSet<u64>,
    /// The voters, nodes `1..=voters`.
    pub(super) voters: u64,
    /// Nodes `voters + 1..=voters + spares`: provisioned, outside the
    /// bootstrap membership, started only when a scenario starts them.
    pub(super) spares: u64,
    /// The snapshot driver every node runs, if any ([`set_snapshot_drivers`]).
    pub(super) drivers: Option<DriverPlan>,
    /// Each node's registry of its replicas, as a node's runtime keeps it:
    /// its snapshot driver plans over it, and its replicas share its
    /// snapshot build coordinator when the nodes run drivers.
    pub(super) node_registries: BTreeMap<u64, ursula_raft::RaftGroupHandleRegistry>,
    /// The snapshot driver task of each running node.
    driver_tasks: BTreeMap<u64, madsim::task::JoinHandle<()>>,
}

impl JournalCluster {
    #[cfg(test)]
    pub(super) async fn start(name: &str) -> Self {
        Self::start_with_fsync(name, WalFsync::Always).await
    }

    pub(super) async fn start_with_fsync(name: &str, fsync: WalFsync) -> Self {
        Self::start_with_tuning(name, JournalTuning::new(fsync)).await
    }

    /// Three nodes whose WALs run with `tuning`.
    pub(super) async fn start_with_tuning(name: &str, tuning: JournalTuning) -> Self {
        Self::start_voters(name, tuning, 3).await
    }

    /// `voters` nodes, `1..=voters`, whose WALs run with `tuning`.
    pub(super) async fn start_voters(name: &str, tuning: JournalTuning, voters: u64) -> Self {
        Self::start_with_spares(name, tuning, voters, 0).await
    }

    /// `voters` nodes, `1..=voters`, whose WALs run with `tuning`, and
    /// `spares` more nodes provisioned but not started: they join a group
    /// only as a scenario adds them.
    pub(super) async fn start_with_spares(
        name: &str,
        tuning: JournalTuning,
        voters: u64,
        spares: u64,
    ) -> Self {
        let mut cluster = Self::unstarted_voters(name, tuning, voters, spares);
        for node_id in 1..=voters {
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

    #[cfg(test)]
    pub(super) fn unstarted(name: &str, fsync: WalFsync) -> Self {
        Self::unstarted_with_tuning(name, JournalTuning::new(fsync))
    }

    #[cfg(test)]
    fn unstarted_with_tuning(name: &str, tuning: JournalTuning) -> Self {
        Self::unstarted_voters(name, tuning, 3, 0)
    }

    fn unstarted_voters(name: &str, tuning: JournalTuning, voters: u64, spares: u64) -> Self {
        let nodes = voters.saturating_add(spares);
        let drivers = SNAPSHOT_DRIVERS.with(std::cell::Cell::get);
        let mut config = Config {
            cluster_name: name.to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
            ..Default::default()
        };
        if let Some(drivers) = drivers {
            // As a node with a snapshot driver configures OpenRaft.
            config.snapshot_policy = openraft::SnapshotPolicy::Never;
            config.max_in_snapshot_log_to_keep = drivers.keep;
            config.purge_batch_size = 1;
        }
        let config = Arc::new(config.validate().expect("valid raft config"));
        let (policy, votes) = recovery_wiring::vote_recording_network_policy();
        Self {
            config,
            policy,
            votes,
            wals: (1..=nodes)
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
            metrics: (1..=nodes)
                .map(|node_id| (node_id, RuntimeMetrics::new(1, JOURNAL_GROUPS.len())))
                .collect(),
            acknowledged: BTreeMap::new(),
            faulty: None,
            replay_stopped: BTreeMap::new(),
            plan_leaders: std::collections::BTreeSet::new(),
            voters,
            spares,
            drivers,
            node_registries: (1..=nodes)
                .map(|node_id| (node_id, ursula_raft::RaftGroupHandleRegistry::default()))
                .collect(),
            driver_tasks: BTreeMap::new(),
        }
    }

    /// Every node, the spares included.
    pub(super) fn nodes(&self) -> std::ops::RangeInclusive<u64> {
        1..=self.voters.saturating_add(self.spares)
    }

    pub(super) async fn start_node(&mut self, node_id: u64) {
        for group in JOURNAL_GROUPS {
            self.start_replica(group, node_id).await;
        }
        if let Some(drivers) = self.drivers
            && !self.driver_tasks.contains_key(&node_id)
        {
            let max_age = drivers.max_age;
            let driver = ursula::SnapshotDriver::new(
                &self.node_registries[&node_id],
                self.metrics[&node_id].clone(),
                None,
                ursula::SnapshotDriveSettings {
                    interval: drivers.interval,
                    cadence: ursula_raft::snapshot_cadence::SnapshotCadence::new(
                        ursula_raft::snapshot_cadence::DEFAULT_NODE_LOG_BUDGET_BYTES,
                        JOURNAL_GROUPS.len(),
                        ursula_raft::snapshot_cadence::DEFAULT_SNAPSHOT_BACKSTOP_ENTRIES,
                    )
                    .with_deleted_data_max_age(max_age)
                    .with_log_keep(drivers.keep),
                    max_groups_per_tick: 16,
                },
            );
            self.driver_tasks
                .insert(node_id, madsim::task::spawn(driver.run()));
        }
    }

    /// Whether `node_id`'s snapshot driver task runs.
    pub(super) fn driver_running(&self, node_id: u64) -> bool {
        self.driver_tasks
            .get(&node_id)
            .is_some_and(|driver| !driver.is_finished())
    }

    /// The snapshot build coordinator `node_id`'s replicas share when the
    /// nodes run drivers.
    pub(super) fn coordinator(&self, node_id: u64) -> ursula_raft::SnapshotBuildCoordinator {
        self.node_registries[&node_id].snapshot_build_coordinator()
    }

    /// Starts `node_id`'s replica of `group` from its log store, as the
    /// node's engine factory does.
    async fn start_replica(&mut self, group: u32, node_id: u64) {
        let voters = recovery_wiring::configured_voters(1..=self.voters);
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
        config.enable_elect = ursula_raft::ElectionPolicy::default().may_campaign(Some(&rejoin));
        let mut options = RaftGroupEngineOptions::default();
        if self.drivers.is_some() {
            options.snapshot_build = Some(self.coordinator(node_id));
        }
        options.apply_fault = self
            .faulty
            .filter(|(faulty, _)| *faulty == group)
            .map(|(_, fault)| fault);
        options.recovery = Some(rejoin.clone());
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
                return;
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
        if self.drivers.is_some() {
            self.node_registries[&node_id].register_engine_for_simulation(&engine);
        }
        self.engines.insert((group, node_id), engine);
        self.rejoins.insert((group, node_id), rejoin);
    }

    /// Restarts `node_id`'s replica of `group` alone, as the node's runtime
    /// restarts one group's engine for a recovery plan's cut: the engine
    /// stops, its log store closes, and the replica starts again from it.
    pub(super) async fn restart_replica(&mut self, group: u32, node_id: u64) {
        self.registries[&group].unregister(node_id);
        self.node_registries[&node_id].unregister_group_for_simulation(RaftGroupId(group));
        self.rejoins.remove(&(group, node_id));
        if let Some(engine) = self.engines.remove(&(group, node_id)) {
            engine.shutdown().await.expect("stop the replica");
        }
        self.start_replica(group, node_id).await;
    }

    /// The `accept-unsynced-loss` admin route on `node_id` for `group`: the
    /// replica takes `plan`, and a plan that cuts restarts its replica.
    pub(super) async fn accept_recovery_plan(
        &mut self,
        group: u32,
        node_id: u64,
        plan: &ursula_proto::admin::RecoveryPlanRequest,
    ) -> Result<ursula_proto::admin::RecoveryPlanReport, ursula_raft::RecoveryGateError> {
        let registry = self.registries[&group].clone();
        let mut restart = ReplicaRestarter {
            cluster: self,
            node_id,
        };
        registry
            .accept_recovery_plan(node_id, plan, &mut restart)
            .await
    }

    pub(super) async fn stop_node(&mut self, node_id: u64) {
        // The node's driver goes with its process.
        if let Some(driver) = self.driver_tasks.remove(&node_id) {
            driver.abort();
        }
        for group in JOURNAL_GROUPS {
            self.registries[&group].unregister(node_id);
            self.node_registries[&node_id].unregister_group_for_simulation(RaftGroupId(group));
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
    pub(super) fn stopped_by_storage_error(&self, group: u32, node_id: u64) -> bool {
        let metrics = openraft::rt::WatchReceiver::borrow_watched(
            &self.engines[&(group, node_id)].raft_handle().metrics(),
        )
        .clone();
        metrics.running_state.is_err()
    }

    #[cfg(test)]
    /// Waits until OpenRaft stopped every replica of `node_id` after a
    /// storage error.
    pub(super) async fn wait_stopped_by_storage_error(&self, node_id: u64) -> bool {
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

    #[cfg(test)]
    /// A node that leads no group, chosen from the seed.
    /// The in-process network of `group`, which also reaches each node's
    /// production admin and barrier entry points.
    pub(super) async fn follower_of_every_group(&self, seed: u64) -> u64 {
        let mut leaders = Vec::new();
        for group in JOURNAL_GROUPS {
            leaders.push(self.leader(group).await);
        }
        let followers = (1..=self.voters)
            .filter(|node_id| !leaders.contains(node_id))
            .collect::<Vec<_>>();
        let count = u64::try_from(followers.len()).expect("follower count fits u64");
        let index = usize::try_from(seed.checked_rem(count).expect("a node leads no group"))
            .expect("index fits usize");
        followers[index]
    }

    #[cfg(test)]
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

    #[cfg(test)]
    /// Snapshots `group` on every running replica but `skip` and purges all
    /// but the last `keep` entries the snapshot covers, as the snapshot
    /// driver's snapshots do.
    pub(super) async fn snapshot_and_purge(&self, group: u32, keep: u64, skip: u64) {
        for ((engine_group, node_id), _engine) in &self.engines {
            if *engine_group != group
                || *node_id == skip
                || self.stopped_by_storage_error(group, *node_id)
            {
                continue;
            }
            self.snapshot_and_purge_node(group, *node_id, keep).await;
        }
    }

    /// Snapshots `node_id`'s replica of `group` and purges all but the last
    /// `keep` entries the snapshot covers.
    pub(super) async fn snapshot_and_purge_node(&self, group: u32, node_id: u64, keep: u64) {
        self.try_snapshot_and_purge_node(group, node_id, keep, Duration::from_secs(5))
            .await
            .unwrap_or_else(|stage| panic!("group {group}: node {node_id}: {stage}"));
    }

    /// [`Self::snapshot_and_purge_node`], answering which stage did not
    /// finish within `wait` instead of panicking.
    pub(super) async fn try_snapshot_and_purge_node(
        &self,
        group: u32,
        node_id: u64,
        keep: u64,
        wait: Duration,
    ) -> Result<u64, &'static str> {
        let raft = self.engines[&(group, node_id)].raft_handle();
        let snapshot = self.operator_snapshot(group, node_id, wait).await?;
        let purge_upto = snapshot.saturating_sub(keep);
        if self.drivers.is_some() {
            // Under the hold rule every purge of the drivers' nodes keeps
            // (#669): while a build of the group runs or OpenRaft has not
            // published one, a purge could stop the group's Raft core.
            let coordinator = self.coordinator(node_id);
            let deadline = madsim::time::Instant::now() + wait;
            while coordinator
                .hold_for_purge(
                    group,
                    Some(snapshot),
                    purge_upto,
                    self.config.max_in_snapshot_log_to_keep,
                    raft.metrics(),
                )
                .is_err()
            {
                if madsim::time::Instant::now() >= deadline {
                    return Err("hold the group's builds for the purge");
                }
                madsim::time::sleep(Duration::from_millis(10)).await;
            }
        }
        raft.trigger()
            .purge_log(purge_upto)
            .await
            .map_err(|_fatal| "trigger a purge")?;
        raft.wait(Some(wait))
            .metrics(
                |metrics| {
                    metrics
                        .purged
                        .is_some_and(|purged| purged.index >= purge_upto)
                },
                "log purged",
            )
            .await
            .map_err(|_timeout| "wait for the purge")?;
        Ok(purge_upto)
    }

    /// Snapshots `node_id`'s replica of `group` as an operator's
    /// `POST /__ursula/raft/{group}/snapshot` does, and answers the last log
    /// index of the snapshot OpenRaft then shows, once it covers what the
    /// replica applied. On the drivers' nodes the start goes through the
    /// node's build coordinator (#669); without drivers each replica's own
    /// coordinator takes the trigger.
    pub(super) async fn operator_snapshot(
        &self,
        group: u32,
        node_id: u64,
        wait: Duration,
    ) -> Result<u64, &'static str> {
        let raft = self.engines[&(group, node_id)].raft_handle();
        let wanted = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).last_applied;
        let applied = wanted.map_or(0, |log_id| log_id.index);
        let published = |metrics: &RaftMetrics<UrsulaRaftTypeConfig>| {
            metrics
                .snapshot
                .is_some_and(|snapshot| snapshot.index >= applied)
        };
        if self.drivers.is_some() {
            self.coordinator(node_id)
                .start_snapshot(
                    group,
                    wanted,
                    wait,
                    || {
                        published(&openraft::rt::WatchReceiver::borrow_watched(
                            &raft.metrics(),
                        ))
                    },
                    std::future::pending(),
                    || async { raft.trigger().snapshot().await },
                )
                .await
                .map_err(|_not_started| "start a snapshot")?;
        } else {
            raft.trigger()
                .snapshot()
                .await
                .map_err(|_fatal| "trigger a snapshot")?;
        }
        raft.wait(Some(wait))
            .metrics(published, "snapshot built")
            .await
            .map_err(|_timeout| "wait for the snapshot")?
            .snapshot
            .map(|snapshot| snapshot.index)
            .ok_or("snapshot log id")
    }

    /// Stops `node_id` and gives it an empty WAL named `name`, as a lost
    /// disk would: it rejoins as a wiped node when it starts again.
    pub(super) async fn wipe_node(&mut self, node_id: u64, name: &str, tuning: JournalTuning) {
        self.stop_node(node_id).await;
        self.wals.insert(
            node_id,
            SimNodeWal::provision_with_tuning(name, tuning).with_group_count(JOURNAL_GROUPS.len()),
        );
    }

    /// How many full snapshots the network delivered to `node_id`'s replica
    /// of `group`.
    pub(super) fn full_snapshots(&self, group: u32, node_id: u64) -> usize {
        self.registries[&group].full_snapshot_count(node_id)
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
                            follower: FollowerRead::Prefix,
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

pub(super) fn metrics(
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
pub(super) fn leader_by_state(cluster: &JournalCluster, group: u32) -> Option<u64> {
    cluster.nodes().find(|node_id| {
        cluster.engines.contains_key(&(group, *node_id))
            && metrics(cluster, group, *node_id).state == openraft::ServerState::Leader
    })
}

pub(super) async fn wait_leader(cluster: &JournalCluster, group: u32, context: &str) -> u64 {
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

pub(super) fn gate(cluster: &JournalCluster, group: u32, node_id: u64) -> RecoveryGateStatus {
    cluster.rejoins[&(group, node_id)].status()
}

#[cfg(test)]
pub(super) fn log_state(cluster: &JournalCluster, group: u32, node_id: u64) -> GroupLogState {
    cluster.wals[&node_id]
        .store(RaftGroupId(group))
        .expect("a running replica holds its store")
        .log_state()
}

/// `node_id`'s replica of `group` as the recovery tool reads it from the
/// metrics ([`PlanMember`]).
pub(super) async fn member_view(cluster: &JournalCluster, group: u32, node_id: u64) -> PlanMember {
    let mut store = cluster.wals[&node_id]
        .store(RaftGroupId(group))
        .expect("a running replica holds its store");
    let last = store.get_log_state().await.expect("log state").last_log_id;
    let vote = store.read_vote().await.expect("vote");
    let replica = metrics(cluster, group, node_id);
    let history = cluster.engines[&(group, node_id)].history();
    PlanMember {
        node_id,
        log: ReplicaLogView {
            last_log_index: last.map(|last| last.index),
            last_log_term: last.map(|last| last.leader_id.term),
            current_term: vote.map_or(0, |vote| vote.leader_id().term()),
        },
        committed: replica.local_committed.map(|committed| committed.index),
        committed_entry: replica
            .local_committed
            .map(|committed| ursula_proto::admin::EntryId {
                term: committed.leader_id.term,
                node_id: committed.leader_id.node_id,
            }),
        last_applied: replica.last_applied.map(|applied| applied.index),
        gate: gate(cluster, group, node_id),
        plan: history
            .plan()
            .map(|plan| ursula_proto::admin::RaftGroupRecovery {
                leader: plan.leader,
                point: plan.point,
                leader_term: plan.leader_term,
                cut_pending: matches!(plan.cut, ursula_raft::PlanCut::Pending { .. }),
            }),
        leads_live: ursula_raft::leads_live(&replica),
        history_event_index: history.applied().map(|event| event.event_index),
        current_leader: replica.current_leader,
        voter_configs: replica
            .membership_config
            .membership()
            .get_joint_config()
            .clone(),
        committed_voter_configs: cluster.engines[&(group, node_id)].kept_voter_configs(),
        running: true,
        learner_ids: replica
            .membership_config
            .membership()
            .learner_ids()
            .collect(),
    }
}

/// [`PlanTransport`] over each replica's production admin entry point
/// ([`JournalCluster::accept_recovery_plan`]), with the refusal it answers
/// typed as the node's HTTP route types it.
pub(super) struct SimPlanTransport<'a> {
    pub(super) cluster: &'a mut JournalCluster,
    pub(super) group: u32,
}

impl SimPlanTransport<'_> {
    pub(super) async fn accept(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        let group = self.group;
        self.cluster
            .accept_recovery_plan(group, node_id, request)
            .await
            .map_err(|error| match error.refusal() {
                Some(refusal) => SendError::Refused {
                    node_id,
                    refused: RecoveryPlanRefused {
                        raft_group_id: u64::from(group),
                        node_id,
                        refusal,
                        message: error.to_string(),
                    },
                },
                None => SendError::Failed {
                    node_id,
                    source: Box::new(error),
                },
            })
    }
}

impl PlanTransport for SimPlanTransport<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl std::future::Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.accept(node_id, request)
    }
}

/// The recovery tool for `group` over the members in `reachable`: the
/// shipped planner and sender of `ursulactl recover-group`
/// ([`ursula_ctl::recovery`], m5 of the #581 review), over each member's
/// production admin entry point. The leader goes first, then every other
/// member with the leader's plan term. A plan that cuts restarts each
/// replica inside its accept. Returns the plan.
pub(super) async fn recover_group(
    cluster: &mut JournalCluster,
    group: u32,
    reachable: &[u64],
    context: &str,
) -> GroupPlan {
    let mut members = Vec::new();
    for node_id in reachable {
        members.push(member_view(cluster, group, *node_id).await);
    }
    let plan = plan_group(&members)
        .unwrap_or_else(|error| panic!("{context}: group {group}: no plan: {error}"));
    cluster.plan_leaders.insert(plan.leader);
    let sent = send_plan(&mut SimPlanTransport { cluster, group }, plan, &members)
        .await
        .unwrap_or_else(|error| panic!("{context}: group {group}: {plan:?}: {error}"));
    assert!(
        sent.refused.is_empty(),
        "{context}: group {group}: {plan:?}: members refused: {:?}",
        sent.refused
    );
    plan
}

/// Waits until every replica of `group` in `nodes` applied a history event
/// above `point`: the plan is satisfied everywhere.
pub(super) async fn wait_plan_satisfied(
    cluster: &JournalCluster,
    group: u32,
    nodes: &[u64],
    point: u64,
    context: &str,
) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(15);
    loop {
        let satisfied = nodes.iter().all(|node_id| {
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
        });
        if satisfied {
            return;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: group {group}: the recovery plan's history event never applied everywhere"
        );
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Moves the leadership of every group to `leader`.
pub(super) async fn lead_every_group(cluster: &JournalCluster, leader: u64, context: &str) {
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
