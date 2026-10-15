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
use ursula_proto::admin::PlanStep;
use ursula_proto::admin::RecoveryPlanOutcome;
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
        let mut cluster = Self::unstarted_voters(name, tuning, voters);
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
        Self::unstarted_voters(name, tuning, 3)
    }

    fn unstarted_voters(name: &str, tuning: JournalTuning, voters: u64) -> Self {
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
            wals: (1..=voters)
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
            metrics: (1..=voters)
                .map(|node_id| (node_id, RuntimeMetrics::new(1, JOURNAL_GROUPS.len())))
                .collect(),
            acknowledged: BTreeMap::new(),
            faulty: None,
            replay_stopped: BTreeMap::new(),
            plan_leaders: std::collections::BTreeSet::new(),
            voters,
        }
    }

    pub(super) async fn start_node(&mut self, node_id: u64) {
        for group in JOURNAL_GROUPS {
            self.start_replica(group, node_id).await;
        }
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
        self.engines.insert((group, node_id), engine);
        self.rejoins.insert((group, node_id), rejoin);
    }

    /// Restarts `node_id`'s replica of `group` alone, as the node's runtime
    /// restarts one group's engine for a recovery plan's cut: the engine
    /// stops, its log store closes, and the replica starts again from it.
    pub(super) async fn restart_replica(&mut self, group: u32, node_id: u64) {
        self.registries[&group].unregister(node_id);
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
    (1..=cluster.voters).find(|node_id| {
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
/// metrics: its log, its vote term, its committed index and whether its
/// gate restricts its vote.
#[derive(Debug, Clone, Copy)]
pub(super) struct MemberView {
    pub(super) node_id: u64,
    pub(super) log: ReplicaLogView,
    pub(super) committed: Option<u64>,
    pub(super) restricted: bool,
}

pub(super) async fn member_view(cluster: &JournalCluster, group: u32, node_id: u64) -> MemberView {
    let mut store = cluster.wals[&node_id]
        .store(RaftGroupId(group))
        .expect("a running replica holds its store");
    let last = store.get_log_state().await.expect("log state").last_log_id;
    let vote = store.read_vote().await.expect("vote");
    MemberView {
        node_id,
        log: ReplicaLogView {
            last_log_index: last.map(|last| last.index),
            last_log_term: last.map(|last| last.leader_id.term),
            current_term: vote.map_or(0, |vote| vote.leader_id().term()),
        },
        committed: metrics(cluster, group, node_id)
            .local_committed
            .map(|committed| committed.index),
        // A closed gate or an unsatisfied plan: the replica kept its vote
        // while it may have lost entries (#581 section 4.5, `floor`).
        restricted: gate(cluster, group, node_id) != RecoveryGateStatus::Open
            || cluster.engines[&(group, node_id)]
                .history()
                .plan()
                .is_some(),
    }
}

/// The recovery tool's plan (#581 section 4.5) from the members it reached:
/// the leader is the member with the greatest last log id; when its last
/// log term is below the highest vote term a restricted member (a closed
/// gate or an unsatisfied plan) holds, a stale entry is possible, so every
/// member cuts its log to what it knew was committed and the leader is the
/// member with the greatest committed index.
pub(super) fn recovery_plan(members: &[MemberView]) -> (u64, u64, bool) {
    let floor = members
        .iter()
        .filter(|member| member.restricted)
        .map(|member| member.log.current_term)
        .max()
        .unwrap_or(0);
    let longest = members
        .iter()
        .max_by_key(|member| {
            (
                member.log.last_log_term,
                member.log.last_log_index,
                member.node_id,
            )
        })
        .expect("a reachable member");
    let cut = members.len() > 1 && longest.log.last_log_term.unwrap_or(0) < floor;
    if cut {
        let leader = members
            .iter()
            .max_by_key(|member| (member.committed, member.node_id))
            .expect("a reachable member");
        (leader.node_id, leader.committed.unwrap_or(0), true)
    } else {
        (
            longest.node_id,
            longest.log.last_log_index.unwrap_or(0),
            false,
        )
    }
}

/// The recovery tool for `group` over the members in `reachable`: computes
/// the plan and sends it through each member's production admin entry point,
/// the leader first and then every other member with the leader's plan term
/// (N1, #581 review). A plan that cuts restarts each replica inside its
/// accept. Returns the leader's request.
pub(super) async fn recover_group(
    cluster: &mut JournalCluster,
    group: u32,
    reachable: &[u64],
    context: &str,
) -> RecoveryPlanRequest {
    let mut members = Vec::new();
    for node_id in reachable {
        members.push(member_view(cluster, group, *node_id).await);
    }
    let (leader, point, cut) = recovery_plan(&members);
    cluster.plan_leaders.insert(leader);
    let request = |member: &MemberView, step: PlanStep| RecoveryPlanRequest {
        leader,
        point,
        cut,
        step,
        expected: member.log,
    };
    let leader_view = members
        .iter()
        .find(|member| member.node_id == leader)
        .copied()
        .expect("the plan's leader is reachable");
    let lead = request(&leader_view, PlanStep::Lead);
    let report: RecoveryPlanReport = cluster
        .accept_recovery_plan(group, leader, &lead)
        .await
        .unwrap_or_else(|error| {
            panic!("{context}: group {group}: the plan's leader {leader} refused {lead:?}: {error}")
        });
    assert_eq!(report.outcome, RecoveryPlanOutcome::Leads, "{context}");
    for member in members.iter().filter(|member| member.node_id != leader) {
        let follow = request(member, PlanStep::Follow {
            leader_term: report.leader_term,
        });
        match cluster
            .accept_recovery_plan(group, member.node_id, &follow)
            .await
        {
            Ok(answer) => {
                assert_eq!(answer.outcome, RecoveryPlanOutcome::Follows, "{context}");
            }
            // The members that took the plan already elected its leader,
            // which reached this one: it rejoins through the leader.
            Err(
                error @ (ursula_raft::RecoveryGateError::FollowsLeader { .. }
                | ursula_raft::RecoveryGateError::NotStalled { .. }),
            ) if leader_by_state(cluster, group) == Some(leader) => {
                tracing::info!(
                    %error,
                    "{context}: group {group}: node {} rejoins through the plan's leader",
                    member.node_id
                );
            }
            Err(error) => panic!(
                "{context}: group {group}: node {} refused {follow:?}: {error}",
                member.node_id
            ),
        }
    }
    lead
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
