//! Placement-driven hosting in the `placement-move` family (design 4.1 to
//! 4.3 and 15.2). Every node of a [`MetaCluster`] started placed runs the
//! production hosting driver ([`HostingDriver`]) over its journals and its
//! meta runtime's views. The driver opens and closes the node's data
//! replicas through a [`SimHost`], the simulation's [`GroupHost`], and every
//! open takes [`MetaPlacement`] as its placement inputs, as the server's
//! engine factory does. Nothing else starts a placed replica, so a node runs
//! what its journals hold and what a confirmed placement gives it.
//!
//! Each placed replica's metrics are watched for the run, so every
//! membership entry some replica held, effective or committed, is kept with
//! the leader that appended it ([`MembershipLog`]). The leader of a group
//! asks the recovery gates of the learners it would promote from each
//! learner's node registry ([`SimPeerGates`]), as a participation answer
//! carries them.
//!
//! The hosting checks ([`HostingChecks`]) run at every observation of the
//! meta checks:
//!
//! - I2 ([`I2`]): no group is initialized twice, and no two replicas lead
//!   one group in one term.
//! - I3 ([`I3`]): every membership a running replica holds names only nodes
//!   some placement of the group gave a role. Its live half is a wait: once
//!   the faults and moves are over, every running node's view shows every
//!   group `Live` and stable on one voter set, and the group's leader holds
//!   a committed uniform membership of exactly those voters, with no
//!   learner. Its teardown half is a wait too: no running node holds a log
//!   of a group the latest view gives it no role in. Both halves leave out
//!   the nodes the latest view shows removed: a removed node closes its
//!   replicas and keeps their logs (design 8.1), and once meta drops it as
//!   a learner its view stays where it was.
//! - I4 ([`I4`]), P-SELF: every membership entry lists the leader that
//!   appended it in its last config, but the RF 1 step, whose configs have
//!   one voter each.
//! - I5 ([`I5`]) is a wait and a script's check: once a group's replicas
//!   without a role are torn down, the latest view keeps no retirement
//!   record of the group, and a log a node retired and reopened replays
//!   nothing of its retired life. Its removal half, the swept objects of a
//!   removed node, is [`super::placed_removals`]'s.
//! - I8 ([`I8`]): no voter change appended in a term above every term the
//!   group's replicas held when the placement took a node's role away
//!   (abandoned or retired) adds that node. A leader elected later confirms
//!   its view after the change, so only a step planned in an earlier
//!   leadership, or from an unconfirmed view, adds it. And no running node
//!   has retired its log of a group whose latest committed membership lists
//!   it as a voter.
//! - I9 ([`I9`]): once a view shows a move's report, no membership of the
//!   group above the one it names was committed before (checked when the
//!   report is first seen, which follows its proof by the report's
//!   latency), and the latest committed membership never lists a node the
//!   placement retired as a voter.
//! - The hosting check ([`HOSTED`]): a node runs only the groups some
//!   placement gave it a role in, no node runs one before the view of some
//!   running node bootstrapped, and a node creates a log it did not hold
//!   only while its own view is bootstrapped. It does not see whether the
//!   placement that created a replica was confirmed.
//!
//! A run that moved a group fails when I4, I5's record half, either half
//! of I8, I9 or the creation rule judged nothing of that group, but I5's
//! record half and I8's teardown half for a group whose dropped voters were
//! all removed: a removal drops their retirement records and retires no
//! log, and the removal checks judge in their place
//! ([`HostingChecks::judged_the_moves`]), so a check that never saw the
//! move cannot pass it.
//!
//! I1 ([`I1`]), I7 ([`I7`]), I14 ([`I14`]) and I15 ([`I15`]) are waits:
//! every voter reads the acknowledged appends, a node restarted without
//! meta serves its groups, a settled cluster's leaderships stay where they
//! are, and every seeded group forms.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use futures_util::future::BoxFuture;
use futures_util::lock::Mutex as AsyncMutex;
use openraft::Raft;
use openraft::ServerState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use ursula_control::LocalLog;
use ursula_ctl::recovery::Floor;
use ursula_ctl::recovery::FloorError;
use ursula_ctl::recovery::LoopState;
use ursula_ctl::recovery::PlanError;
use ursula_ctl::recovery::PlanMember;
use ursula_ctl::recovery::PlanTransport;
use ursula_ctl::recovery::RecoverError;
use ursula_ctl::recovery::RecoverOutcome;
use ursula_ctl::recovery::SendError;
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::placement::GenesisClaim;
use ursula_raft::GroupHost;
use ursula_raft::GroupRejoin;
use ursula_raft::HostingDriver;
use ursula_raft::HostingSet;
use ursula_raft::HostingTask;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::MadsimOpenRaftRuntime;
use ursula_raft::MetaPlacement;
use ursula_raft::PlacementInputs;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupEngineOptions;
use ursula_raft::RaftGroupStateMachine;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::AppendRequest;
use ursula_runtime::ClosedBy;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::ProducerRefusal;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::StreamErrorContext;
use ursula_shard::RaftGroupId;

use super::SimTrace;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::ReplicaParts;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::meta_checks::BOUND;
use super::meta_checks::STEP;
use super::meta_checks::fail;
use super::meta_cluster::MetaCluster;
use super::meta_cluster::MetaRecoverFailed;
use super::meta_cluster::recover_round;
use super::recovery_wiring;

pub(super) const I1: &str = "acknowledged_writes_read_back";
pub(super) const I2: &str = "no_group_initialized_twice";
pub(super) const I3: &str = "every_group_settles_on_its_placement";
pub(super) const I4: &str = "membership_entries_keep_their_leader";
pub(super) const I5: &str = "a_teardown_leaves_nothing_behind";
pub(super) const I7: &str = "a_node_restarted_without_meta_serves";
pub(super) const I8: &str = "a_voter_change_adds_only_placed_nodes";
pub(super) const I9: &str = "a_report_names_the_latest_membership";
pub(super) const I14: &str = "a_settled_reconciler_makes_no_transfer";
pub(super) const I15: &str = "every_seeded_group_forms";
pub(super) const HOSTED: &str = "a_node_runs_only_its_placed_groups";
pub(super) const DATA_PLAN: &str = "data_plans_recover_after_meta";

/// Mixed into the OpenRaft seed of each placed replica's start.
const HOST_SEED: u64 = 0x686f_7374_6564_0000;

/// Every gate a placed replica ever opened with, by group and node, so I2
/// counts the initializations of replicas that stopped too.
#[derive(Clone, Default)]
pub(super) struct StartedGates(Arc<Mutex<Vec<(u32, u64, Arc<GroupRejoin>)>>>);

impl StartedGates {
    fn push(&self, group: u32, node_id: u64, rejoin: Arc<GroupRejoin>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((group, node_id, rejoin));
    }

    /// How many replicas started so far.
    fn len(&self) -> u64 {
        let started = self
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();
        u64::try_from(started).unwrap_or(u64::MAX)
    }

    /// How many replicas of `node_id` started so far, in any of its
    /// processes.
    fn of_node(&self, node_id: u64) -> u64 {
        let started = self
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|(_, node, _)| *node == node_id)
            .count();
        u64::try_from(started).unwrap_or(u64::MAX)
    }

    /// The nodes whose replicas of each group ran `Initialize`.
    fn initializers(&self) -> BTreeMap<u32, Vec<u64>> {
        let mut initializers: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        for (group, node_id, rejoin) in self
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
        {
            if rejoin.initialized_here() {
                initializers.entry(*group).or_default().push(*node_id);
            }
        }
        initializers
    }
}

/// One membership entry as some placed replica's metrics showed it.
#[derive(Debug, Clone)]
pub(super) struct SeenMembership {
    /// The leader that appended it.
    pub(super) leader: u64,
    /// Its voter configs: two for a joint entry.
    pub(super) configs: Vec<BTreeSet<u64>>,
    /// Whether some replica showed it committed.
    pub(super) committed: bool,
}

/// Every membership entry some placed replica showed, effective or
/// committed, by group and then by log id as (index, term).
#[derive(Clone, Default)]
pub(super) struct MembershipLog(Arc<Mutex<BTreeMap<u32, BTreeMap<(u64, u64), SeenMembership>>>>);

impl MembershipLog {
    fn record(&self, group: u32, metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>) {
        let mut log = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        let entries = log.entry(group).or_default();
        for (stored, committed) in [
            (&metrics.membership_config, false),
            (&metrics.committed_membership_config, true),
        ] {
            let Some(log_id) = stored.log_id() else {
                continue;
            };
            let key = (log_id.index, log_id.leader_id.term);
            let membership = stored.membership();
            let seen = entries.entry(key).or_insert_with(|| SeenMembership {
                leader: log_id.leader_id.node_id,
                configs: membership.get_joint_config().clone(),
                committed: false,
            });
            seen.committed |= committed;
        }
    }

    /// The entries of `group` seen so far.
    pub(super) fn of(&self, group: u32) -> BTreeMap<(u64, u64), SeenMembership> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&group)
            .cloned()
            .unwrap_or_default()
    }
}

/// Watches `raft`'s metrics until it stops, keeping every membership
/// entry it shows in `log`.
fn watch_memberships(
    log: MembershipLog,
    group: u32,
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
) -> madsim::task::JoinHandle<()> {
    madsim::task::spawn(async move {
        let mut metrics = raft.metrics();
        loop {
            log.record(
                group,
                &openraft::rt::WatchReceiver::borrow_watched(&metrics),
            );
            if openraft::rt::WatchReceiver::changed(&mut metrics)
                .await
                .is_err()
            {
                return;
            }
        }
    })
}

/// A log a node created that its journals did not hold, and whether the
/// node's own view was bootstrapped then.
#[derive(Debug, Clone, Copy)]
struct Creation {
    group: u32,
    node_id: u64,
    bootstrapped: bool,
}

/// Every creation of a run's nodes ([`Creation`]).
#[derive(Clone, Default)]
pub(super) struct Creations(Arc<Mutex<Vec<Creation>>>);

impl Creations {
    fn push(&self, creation: Creation) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(creation);
    }

    fn unbootstrapped(&self) -> Vec<Creation> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|creation| !creation.bootstrapped)
            .copied()
            .collect()
    }

    /// The nodes that created a log of `group`.
    fn creators(&self, group: u32) -> BTreeSet<u64> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|creation| creation.group == group)
            .map(|creation| creation.node_id)
            .collect()
    }
}

/// The registry of each node whose process runs, which answers for its
/// node's participation.
#[derive(Clone, Default)]
pub(super) struct NodeRegistries(Arc<Mutex<BTreeMap<u64, ursula_raft::RaftGroupHandleRegistry>>>);

impl NodeRegistries {
    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<u64, ursula_raft::RaftGroupHandleRegistry>> {
        self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }
}

/// The peer gates of one node's reconcile shells (design 5.8): each
/// learner's recovery gate as its node's registry reports it
/// ([`ursula_raft::RaftGroupHandleRegistry::recovery_gates`]), the state a
/// participation answer carries, read right before a promotion. A learner
/// whose process does not run, or that the data plane cuts off from this
/// node, answers nothing and stays `Unknown`.
#[derive(Debug)]
struct SimPeerGates {
    node_id: u64,
    policy: ursula_raft::InProcessRaftNetworkPolicy,
    registries: NodeRegistries,
}

impl std::fmt::Debug for NodeRegistries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeRegistries")
    }
}

impl ursula_raft::PeerGates for SimPeerGates {
    fn gates(
        &self,
        replica: ursula_raft::RaftTarget,
        learners: &BTreeSet<ursula_proto::placement::NodeId>,
    ) -> BoxFuture<'_, BTreeMap<ursula_proto::placement::NodeId, ursula_control::PeerGate>> {
        let key = match replica {
            ursula_raft::RaftTarget::Data(group) => {
                ursula_proto::admin::RaftTargetKey::Data(group.0)
            }
            ursula_raft::RaftTarget::Meta => ursula_proto::admin::RaftTargetKey::Meta,
        };
        let registries = self.registries.lock();
        let gates = learners
            .iter()
            .filter(|learner| !self.policy.partitioned(self.node_id, learner.get()))
            .filter_map(|learner| {
                let status = registries
                    .get(&learner.get())?
                    .recovery_gates()
                    .get(&key)
                    .copied()?;
                Some((*learner, ursula_control::PeerGate::of_status(status)))
            })
            .collect();
        Box::pin(std::future::ready(gates))
    }
}

/// One placed replica.
struct Hosted {
    engine: Arc<AsyncMutex<RaftGroupEngine>>,
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    rejoin: Arc<GroupRejoin>,
    /// What the replica showed once it opened, before its drivers ran,
    /// with the vote its store held.
    opened: ReplicaMarks,
    /// The task that keeps its memberships in the run's log.
    watcher: madsim::task::JoinHandle<()>,
}

/// What a replica's recovery gate shows, of either plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GateMarks {
    /// Whether its vote gate is open.
    pub(super) gate_open: bool,
    /// Whether it holds a vote floor.
    pub(super) floor: bool,
}

impl GateMarks {
    pub(super) fn of(rejoin: &GroupRejoin) -> Self {
        Self {
            gate_open: rejoin.vote_gate_open(),
            floor: rejoin.holds_vote_floor(),
        }
    }

    /// What the gate took since it showed `opened`, read now: it opened,
    /// or it wrote a vote floor.
    pub(super) fn taken_since(self, opened: Self) -> Option<&'static str> {
        if self.gate_open && !opened.gate_open {
            Some("opened its gate")
        } else if self.floor && !opened.floor {
            Some("wrote a vote floor")
        } else {
            None
        }
    }
}

/// What a data replica's gate and vote show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReplicaMarks {
    pub(super) gate: GateMarks,
    pub(super) vote: ursula_raft::UrsulaVote,
}

impl ReplicaMarks {
    pub(super) fn of(
        raft: &Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
        rejoin: &GroupRejoin,
    ) -> Self {
        Self {
            gate: GateMarks::of(rejoin),
            vote: openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).vote,
        }
    }
}

/// Why a [`SimHost`] did not open or close a replica.
#[derive(Debug, thiserror::Error)]
pub(super) enum SimHostError {
    #[error("group {group} is not in the hosting set")]
    NotAdmitted { group: u32 },
    #[error("the log store of group {group} did not open")]
    Store {
        group: u32,
        #[source]
        source: ursula_raft::RaftWalError,
    },
    #[error("the recovery gate of group {group} did not open")]
    Gate {
        group: u32,
        #[source]
        source: ursula_raft::GateOpenError,
    },
    #[error("the engine of group {group} did not start or stop")]
    Engine {
        group: u32,
        #[source]
        source: GroupEngineError,
    },
}

/// One node process's [`GroupHost`]: it opens a group's replica as the
/// engine factory does, over the node's journals, the in-process network and
/// the node's [`MetaPlacement`], and keeps it until a close.
pub(super) struct SimHost {
    parts: ReplicaParts,
    inputs: MetaPlacement,
    replicas: Mutex<BTreeMap<u32, Hosted>>,
    started: StartedGates,
    memberships: MembershipLog,
    creations: Creations,
    /// Whether the process no longer answers for its node
    /// ([`MetaCluster::take_host`]).
    detached: std::sync::atomic::AtomicBool,
}

impl SimHost {
    async fn start(&self, group: u32) -> Result<(), SimHostError> {
        let node_id = self.parts.node_id;
        let placement = group_placement(group);
        let admission = self
            .inputs
            .admission(placement.raft_group_id)
            .ok_or(SimHostError::NotAdmitted { group })?;
        let wal = &self.parts.wal;
        if admission == ursula_control::Admission::Create {
            let held = wal
                .raft_wal()
                .local_groups()
                .ok()
                .and_then(|local| local.get(placement.raft_group_id))
                .is_some_and(ursula_control::LocalLog::is_held);
            if !held {
                self.creations.push(Creation {
                    group,
                    node_id,
                    bootstrapped: self
                        .parts
                        .registry
                        .placement_watch()
                        .current()
                        .cluster()
                        .is_some(),
                });
            }
        }
        let store = wal
            .open_admitted(
                placement,
                admission,
                self.parts.metrics.group_engine_metrics(),
            )
            .map_err(|source| SimHostError::Store { group, source })?;
        let rejoin = Arc::new(
            GroupRejoin::durable(
                node_id,
                ursula_shard::RaftTarget::Data(placement.raft_group_id),
                &store,
            )
            .await
            .map_err(|source| SimHostError::Gate { group, source })?,
        );
        // The vote the replica opens with, for I16 of the identity-fence
        // family, whose processes alone read it, so other families' runs
        // keep their schedules.
        let stored_vote = match self.parts.process {
            Some(_) => {
                let mut store = Arc::clone(&store);
                store.read_vote().await.ok().flatten()
            }
            None => None,
        };
        let registry = &self.parts.registry;
        let network = self.parts.registries[&group].clone();
        let mut config = (*self.parts.config).clone();
        config.enable_elect = registry.election_policy().may_campaign(Some(&rejoin));
        let mut options = RaftGroupEngineOptions::gated(Arc::clone(&rejoin));
        options.metrics = Some(self.parts.metrics.group_engine_metrics());
        options.peer_cluster = Some(registry.peer_cluster().clone());
        options.fence = Some(wal.raft_wal().fence());
        options.cold_store = self.parts.cold_store.clone();
        if self.parts.snapshots {
            // As the engine factory wires a node with a snapshot driver:
            // its registry's coordinators and store, and the store's record.
            options.snapshot_build = Some(registry.snapshot_build_coordinator());
            options.snapshot_install = Some(registry.snapshot_install_coordinator());
            options.snapshot_store = self.parts.snapshot_store.clone();
            options.snapshot_record = Some(store.snapshot_record());
        }
        // The driver's tasks run outside the simulation's OpenRaft seed
        // scope, so each start gets a seed of its own, as the runtime
        // scenarios' engine factory gives each create.
        let seed = HOST_SEED
            ^ node_id.rotate_left(32)
            ^ u64::from(group).rotate_left(16)
            ^ self.started.len();
        let mut factory = InProcessRaftNetworkFactory::new(network.clone())
            .with_source(node_id)
            .with_policy(self.parts.policy.clone());
        if let Some(process) = self.parts.process {
            factory = factory.with_process(process, (placement.raft_group_id, registry.clone()));
        }
        let engine = MadsimOpenRaftRuntime::scope(
            seed,
            RaftGroupEngine::new_node(
                placement,
                node_id,
                Arc::new(config),
                factory,
                store,
                wal.recording_work(options),
            ),
        )
        .await
        .map_err(|source| SimHostError::Engine { group, source })?;
        // The process's recovery calls carry its fence and incarnation and
        // follow its routes, as its Raft RPCs do.
        let caller = match self.parts.process {
            Some(process) => ursula_raft::InProcessCaller::process(
                node_id,
                process,
                registry.peer_cluster().clone(),
            )
            .with_policy(self.parts.policy.clone()),
            None => ursula_raft::InProcessCaller::node(node_id),
        };
        // Nothing below awaits, so a stopped driver never leaves a replica
        // half published.
        recovery_wiring::wire_recovery(
            caller,
            placement,
            &engine,
            &rejoin,
            &network,
            &self.parts.policy,
            &self.inputs,
            registry,
        );
        match self.parts.process {
            Some(_) => network.register_process(node_id, &engine, registry),
            None => network.register(node_id, &engine),
        }
        self.started.push(group, node_id, Arc::clone(&rejoin));
        let raft = engine.raft_handle();
        let mut opened = ReplicaMarks::of(&raft, &rejoin);
        if let Some(vote) = stored_vote {
            opened.vote = vote;
        }
        let watcher = watch_memberships(self.memberships.clone(), group, raft.clone());
        let hosted = Hosted {
            opened,
            raft,
            engine: Arc::new(AsyncMutex::new(engine)),
            rejoin,
            watcher,
        };
        self.lock().insert(group, hosted);
        Ok(())
    }

    /// Steps 2 and 3 of a close (design 7.3): the handle goes, then the
    /// engine stops and releases its store. A detached process leaves the
    /// in-process network as it is: its node answers through its next
    /// process there.
    async fn stop(&self, group: u32) -> Result<(), SimHostError> {
        let hosted = self.lock().remove(&group);
        self.parts.registry.unregister_group(RaftGroupId(group));
        if !self.detached.load(std::sync::atomic::Ordering::Acquire) {
            self.parts.registries[&group].unregister(self.parts.node_id);
        }
        let Some(hosted) = hosted else {
            return Ok(());
        };
        hosted.watcher.abort();
        let engine = hosted.engine.lock().await;
        engine
            .shutdown()
            .await
            .map_err(|source| SimHostError::Engine { group, source })
    }

    /// Restarts the replica of `group` for a recovery plan's cut, as the
    /// node's runtime restarts one group's engine: it stops, and opens
    /// again from its store. The hosting driver may open it first, as it
    /// opens every admitted group the node does not run, and that replica
    /// is the restarted one.
    async fn restart(&self, group: u32) -> Option<Arc<GroupRejoin>> {
        if let Err(error) = self.stop(group).await {
            panic!(
                "node {}: stop group {group} for its plan: {error}",
                self.parts.node_id
            );
        }
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            if let Some(rejoin) = self
                .lock()
                .get(&group)
                .map(|hosted| Arc::clone(&hosted.rejoin))
            {
                return Some(rejoin);
            }
            match self.start(group).await {
                Ok(()) => {}
                Err(error) if madsim::time::Instant::now() >= deadline => panic!(
                    "node {}: restart group {group} for its plan: {error}",
                    self.parts.node_id
                ),
                Err(_opening) => madsim::time::sleep(STEP).await,
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, Hosted>> {
        self.replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn groups(&self) -> Vec<u32> {
        self.lock().keys().copied().collect()
    }

    fn raft(&self, group: u32) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.lock().get(&group).map(|hosted| hosted.raft.clone())
    }

    fn engine(&self, group: u32) -> Option<Arc<AsyncMutex<RaftGroupEngine>>> {
        self.lock()
            .get(&group)
            .map(|hosted| Arc::clone(&hosted.engine))
    }

    /// Each replica this host runs, by group.
    fn replicas(&self) -> Vec<HostedReplica> {
        self.lock()
            .iter()
            .map(|(group, hosted)| HostedReplica {
                group: *group,
                raft: hosted.raft.clone(),
                rejoin: Arc::clone(&hosted.rejoin),
                opened: hosted.opened,
            })
            .collect()
    }

    fn gates_open(&self) -> bool {
        self.lock()
            .values()
            .all(|hosted| hosted.rejoin.vote_gate_open())
    }
}

impl GroupHost for SimHost {
    type Error = SimHostError;

    fn open(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), SimHostError>> {
        Box::pin(self.start(group.0))
    }

    fn close(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), SimHostError>> {
        Box::pin(self.stop(group.0))
    }

    /// A simulated node keeps no gauges or open back-off of its own.
    fn forget(&self, _group: RaftGroupId) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A running node's host and hosting driver, and its process's snapshot
/// driver when the cluster runs them.
struct NodeHost {
    host: Arc<SimHost>,
    driver: HostingTask,
    snapshots: Option<madsim::task::JoinHandle<()>>,
}

/// One placed replica as the checks read it.
pub(super) struct HostedReplica {
    pub(super) group: u32,
    pub(super) raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    pub(super) rejoin: Arc<GroupRejoin>,
    /// What it showed once it opened.
    pub(super) opened: ReplicaMarks,
}

/// The host and hosting driver of a node process that no longer answers
/// for its node ([`MetaCluster::take_host`]), and runs on until it is
/// killed.
pub(super) struct DetachedHost(NodeHost);

impl DetachedHost {
    /// Kills the process's driver and replicas. Their disk may be gone, so
    /// a stop that fails is only logged, as a killed process ends either
    /// way.
    pub(super) async fn kill(self) {
        let NodeHost {
            host,
            driver,
            snapshots,
        } = self.0;
        if let Some(snapshots) = snapshots {
            snapshots.abort();
        }
        driver.shutdown().await;
        for group in host.groups() {
            if let Err(error) = host.stop(group).await {
                tracing::info!(group, %error, "a detached process's replica stopped with an error");
            }
        }
    }
}

/// The placed data plane of a [`MetaCluster`]: each running node's host.
#[derive(Default)]
pub(super) struct Placed {
    hosts: BTreeMap<u64, NodeHost>,
    started: StartedGates,
    /// The groups whose stream was created.
    created: BTreeSet<u32>,
    pub(super) memberships: MembershipLog,
    creations: Creations,
    /// The registry of each node whose host runs, for its peers' gates.
    registries: NodeRegistries,
    /// The key of each group's next keyed append.
    keys: BTreeMap<u32, NextKey>,
}

/// The producer key `(epoch, seq)` of a group's next keyed append. The seq
/// counts the appends the epoch acknowledged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct NextKey {
    epoch: u64,
    seq: u64,
}

impl NextKey {
    /// The key after the one acknowledged.
    fn acknowledged(self) -> Self {
        Self {
            seq: self.seq.saturating_add(1),
            ..self
        }
    }

    /// The key to send again after `error`, if it refused this key's write
    /// as closed by a history event (G2): the first seq of the next epoch,
    /// which the producer claims. A seq above the closed one never applied
    /// in the history that survived, so its payload goes again under the
    /// claim. Any other answer keeps the key, so a retry of a write that
    /// applied is answered as a repeat.
    fn after(self, error: &GroupEngineError) -> Self {
        let GroupEngineError::Stream(refusal) = error else {
            return self;
        };
        refusal
            .context
            .iter()
            .find_map(|context| match context {
                StreamErrorContext::Producer(ProducerRefusal::EpochClosed {
                    by: ClosedBy::History,
                    closed,
                    ..
                }) if closed.epoch == self.epoch => Some(Self {
                    epoch: closed.epoch.saturating_add(1),
                    seq: 0,
                }),
                _ => None,
            })
            .unwrap_or(self)
    }
}

/// Whether `metrics` show a published snapshot through log index `applied`.
fn publishes_through(metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>, applied: u64) -> bool {
    metrics
        .snapshot
        .is_some_and(|snapshot| snapshot.index >= applied)
}

/// `group`'s seed voters by the kernel's seed rule over `claim`.
pub(super) fn seed_voters(claim: &GenesisClaim, group: u32) -> BTreeSet<u64> {
    let factor =
        ursula_control::effective_replication_factor(claim.replication_factor, &claim.peers);
    ursula_control::seed_voters(RaftGroupId(group), factor, &claim.peers)
        .iter()
        .map(ursula_proto::placement::NodeId::get)
        .collect()
}

/// `group`'s initializer by the kernel's seed rule over `claim`.
pub(super) fn initializer(claim: &GenesisClaim, group: u32) -> u64 {
    ursula_control::initializer(RaftGroupId(group), &claim.peers).get()
}

impl MetaCluster {
    /// Opens `node_id`'s held cores and starts its hosting driver, after
    /// its meta runtime started, as the server's spawn path does (4.3).
    pub(super) fn start_host(&mut self, node_id: u64) {
        let wal = self.data.wals[&node_id].raft_wal();
        if let Err(error) = wal.open_held_cores(self.data.metrics[&node_id].group_engine_metrics())
        {
            panic!("node {node_id}: the held cores do not open: {error}");
        }
        let reader = self.runtimes[&node_id].status_reader().clone();
        let snapshot_store = self.data.host_snapshot_objects(node_id);
        let snapshots = self.data.spawn_snapshot_driver(node_id);
        let hosting = HostingSet::default();
        let mut parts = self.data.replica_parts(node_id);
        parts.process = self.process(node_id);
        parts.snapshot_store = snapshot_store;
        parts.cold_store = self.data.cold_store(node_id);
        let placed = self.placed_mut();
        let started = placed.started.clone();
        let memberships = placed.memberships.clone();
        let creations = placed.creations.clone();
        let registries = placed.registries.clone();
        registries.lock().insert(node_id, parts.registry.clone());
        let gates = SimPeerGates {
            node_id,
            policy: parts.policy.clone(),
            registries,
        };
        let inputs = MetaPlacement::new(
            ursula_proto::placement::NodeId::new(node_id),
            parts.registry.clone(),
            hosting.clone(),
        )
        .with_peer_gates(Arc::new(gates));
        inputs.bind_meta(reader.clone());
        let host = Arc::new(SimHost {
            parts,
            inputs,
            replicas: Mutex::new(BTreeMap::new()),
            started,
            memberships,
            creations,
            detached: std::sync::atomic::AtomicBool::new(false),
        });
        let driver = HostingDriver::start(Arc::clone(&host), wal, reader, hosting);
        self.placed_mut().hosts.insert(node_id, NodeHost {
            host,
            driver,
            snapshots,
        });
    }

    /// Puts `host`, taken out by [`Self::take_host`], back as `node_id`'s
    /// running host.
    pub(super) fn put_host(&mut self, node_id: u64, host: DetachedHost) {
        let DetachedHost(node) = host;
        node.host
            .detached
            .store(false, std::sync::atomic::Ordering::Release);
        let registry = node.host.parts.registry.clone();
        self.placed_mut()
            .registries
            .lock()
            .insert(node_id, registry);
        self.placed_mut().hosts.insert(node_id, node);
    }

    /// Takes `node_id`'s running host and driver out of the cluster, which
    /// keeps running them until [`DetachedHost::kill`].
    pub(super) fn take_host(&mut self, node_id: u64) -> Option<DetachedHost> {
        self.placed_mut().registries.lock().remove(&node_id);
        let node = self.placed_mut().hosts.remove(&node_id)?;
        node.host
            .detached
            .store(true, std::sync::atomic::Ordering::Release);
        Some(DetachedHost(node))
    }

    /// Each replica `node_id`'s current process runs.
    pub(super) fn hosted_replicas(&self, node_id: u64) -> Vec<HostedReplica> {
        self.placed()
            .hosts
            .get(&node_id)
            .map(|node| node.host.replicas())
            .unwrap_or_default()
    }

    /// How many replicas `node_id` started in the run, each start counted
    /// once it opened, so a replica that opened and closed between two
    /// observations still counts.
    pub(super) fn replica_starts(&self, node_id: u64) -> u64 {
        self.placed().started.of_node(node_id)
    }

    /// `node_id`'s current replica of `group`, if it runs one.
    pub(super) fn hosted_engine(
        &self,
        node_id: u64,
        group: u32,
    ) -> Option<Arc<AsyncMutex<RaftGroupEngine>>> {
        self.placed().hosts.get(&node_id)?.host.engine(group)
    }

    /// An operator's snapshot of `group` on `node_id`'s current replica,
    /// through its process's build coordinator, as `POST /snapshot` asks
    /// it of a node with a snapshot driver. Answers the index of the
    /// snapshot published within `wait`.
    pub(super) async fn placed_snapshot(
        &self,
        group: u32,
        node_id: u64,
        wait: std::time::Duration,
    ) -> Result<u64, &'static str> {
        let (raft, applied) = self.started_placed_snapshot(group, node_id, wait).await?;
        raft.wait(Some(wait))
            .metrics(
                |metrics| publishes_through(metrics, applied),
                "snapshot built",
            )
            .await
            .map_err(|_timeout| "wait for the snapshot")?
            .snapshot
            .map(|snapshot| snapshot.index)
            .ok_or("snapshot log id")
    }

    /// [`Self::placed_snapshot`] up to the start of the build: answers once
    /// a build that covers the replica's applied log started within `wait`,
    /// or ran, without waiting for OpenRaft to publish its snapshot.
    pub(super) async fn start_placed_snapshot(
        &self,
        group: u32,
        node_id: u64,
        wait: std::time::Duration,
    ) -> Result<(), &'static str> {
        self.started_placed_snapshot(group, node_id, wait)
            .await
            .map(|_started| ())
    }

    /// Starts [`Self::placed_snapshot`]'s build, and answers the replica
    /// and the index of the log it applied, which the build covers.
    async fn started_placed_snapshot(
        &self,
        group: u32,
        node_id: u64,
        wait: std::time::Duration,
    ) -> Result<(Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>, u64), &'static str> {
        let raft = self
            .placed()
            .hosts
            .get(&node_id)
            .and_then(|node| node.host.raft(group))
            .ok_or("no replica of the group runs")?;
        let wanted = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).last_applied;
        let applied = wanted.map_or(0, |log_id| log_id.index);
        self.registry(node_id)
            .snapshot_build_coordinator()
            .start_snapshot(
                group,
                wanted,
                wait,
                || {
                    publishes_through(
                        &openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()),
                        applied,
                    )
                },
                std::future::pending(),
                || async { raft.trigger().snapshot().await },
            )
            .await
            .map_err(|_not_started| "start a snapshot")?;
        Ok((raft, applied))
    }

    /// Stops `node_id`'s hosting driver, then every replica it runs.
    pub(super) async fn stop_host(&mut self, node_id: u64) {
        self.placed_mut().registries.lock().remove(&node_id);
        let Some(node) = self.placed_mut().hosts.remove(&node_id) else {
            return;
        };
        if let Some(snapshots) = &node.snapshots {
            snapshots.abort();
        }
        node.driver.shutdown().await;
        for group in node.host.groups() {
            if let Err(error) = node.host.stop(group).await {
                panic!("node {node_id}: stop the replica of group {group}: {error}");
            }
        }
    }

    fn placed(&self) -> &Placed {
        self.placed
            .as_ref()
            .expect("the cluster's data replicas are placed")
    }

    fn placed_mut(&mut self) -> &mut Placed {
        self.placed
            .as_mut()
            .expect("the cluster's data replicas are placed")
    }

    /// The seed voters of `group` by the claim of node 1.
    pub(super) fn seeds(&self, group: u32) -> BTreeSet<u64> {
        seed_voters(&self.claims[&1], group)
    }

    /// The view of the running node whose meta replica applied the most.
    pub(super) fn latest_view(&self) -> Arc<ursula_control::PlacementView> {
        self.running()
            .into_iter()
            .map(|node_id| self.view(node_id))
            .max_by_key(|view| view.applied().map(|applied| applied.get().index))
            .unwrap_or_else(|| Arc::new(ursula_control::PlacementView::empty()))
    }

    /// `group`'s placement in the latest view.
    pub(super) fn placement(&self, group: u32) -> Option<ursula_control::GroupPlacement> {
        self.latest_view()
            .cluster()
            .and_then(|cluster| cluster.group(RaftGroupId(group)))
            .cloned()
    }

    /// The voters `group` is placed on now, by the latest view: its seed
    /// voters until the view shows a placement.
    pub(super) fn placed_voters(&self, group: u32) -> BTreeSet<u64> {
        match self.placement(group) {
            Some(placement) => node_ids(placement.current_voters().iter()),
            None => self.seeds(group),
        }
    }

    /// The nodes the latest view shows with no life, `Removed` or
    /// `Vacant`: a new life of an id is a node again.
    pub(super) fn removed_nodes(&self) -> BTreeSet<u64> {
        self.latest_view()
            .cluster()
            .map(|bootstrapped| {
                bootstrapped
                    .nodes()
                    .iter()
                    .filter(|(_, record)| !record.lifecycle.has_life())
                    .map(|(node, _)| node.get())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The replica of `group` on `node_id`'s running host.
    pub(super) fn hosted_raft(
        &self,
        node_id: u64,
        group: u32,
    ) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.placed().hosts.get(&node_id)?.host.raft(group)
    }

    /// The leader of `group` some running replica names, if it runs.
    pub(super) fn leader_of(&self, group: u32) -> Option<u64> {
        self.placed_leader(group)
    }

    /// The highest term any running replica of `group` holds.
    pub(super) fn max_term(&self, group: u32) -> u64 {
        self.hosting(group)
            .iter()
            .filter_map(|(_, host)| host.raft(group))
            .map(|raft| openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).current_term)
            .max()
            .unwrap_or(0)
    }

    /// The latest committed membership of `group` among its running
    /// replicas, by log index: its voter configs and learners.
    pub(super) fn latest_committed(
        &self,
        group: u32,
    ) -> Option<(u64, Vec<BTreeSet<u64>>, BTreeSet<u64>)> {
        self.hosting(group)
            .iter()
            .filter_map(|(_, host)| host.raft(group))
            .filter_map(|raft| {
                let metrics = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).clone();
                let stored = &metrics.committed_membership_config;
                let index = stored.log_id().as_ref()?.index;
                Some((
                    index,
                    stored.membership().get_joint_config().clone(),
                    stored.membership().learner_ids().collect(),
                ))
            })
            .max_by_key(|(index, _, _)| *index)
    }

    /// The voters of `group`'s latest committed membership among its
    /// running replicas, when it is uniform.
    pub(super) fn committed_voters(&self, group: u32) -> Option<BTreeSet<u64>> {
        let (_, configs, _) = self.latest_committed(group)?;
        match configs.as_slice() {
            [voters] => Some(voters.clone()),
            _ => None,
        }
    }

    /// What `node_id`'s journals hold of `group`, if its process runs and
    /// they name the group.
    pub(super) fn local_log(&self, node_id: u64, group: u32) -> Option<LocalLog> {
        if !self.runtimes.contains_key(&node_id) {
            return None;
        }
        self.data
            .wals
            .get(&node_id)?
            .raft_wal()
            .local_groups()
            .ok()?
            .get(RaftGroupId(group))
    }

    /// I3's teardown half: a running node that holds a log of `group`
    /// which the latest view gives it no role in, if one does. A log on a
    /// core that did not open is never torn down, and neither is a removed
    /// node's.
    pub(super) fn unplaced_replica(&self, group: u32) -> Option<String> {
        let latest = self.latest_view();
        let bootstrapped = latest.cluster()?;
        let removed = self.removed_nodes();
        self.running().into_iter().find_map(|node_id| {
            if removed.contains(&node_id) {
                return None;
            }
            let log = self.local_log(node_id, group)?;
            let role = bootstrapped.role(
                RaftGroupId(group),
                ursula_proto::placement::NodeId::new(node_id),
            );
            (role.is_none() && matches!(log, LocalLog::Empty | LocalLog::Holding)).then(|| {
                format!(
                    "node {node_id} holds a {log:?} log of group {group}, which the latest \
                     view gives it no role in"
                )
            })
        })
    }

    /// I5's record half: the retirement records of `group` the latest view
    /// keeps, if it keeps any.
    pub(super) fn unreported(&self, group: u32) -> Option<String> {
        let latest = self.latest_view();
        let records = latest.cluster()?.retirements().get(&RaftGroupId(group))?;
        (!records.is_empty()).then(|| {
            format!(
                "the latest view keeps group {group}'s retirement records of nodes {:?}",
                node_ids(records.keys().copied())
            )
        })
    }

    /// I5's replay half, for a log `node_id` retired, reopened and opened
    /// again before its new life took an entry: what the log holds, if it
    /// holds anything. Such a log is empty, so an entry or a purge in it
    /// came from the retired life.
    pub(super) async fn replayed_life(&self, node_id: u64, group: u32) -> Option<String> {
        let Some(store) = self.data.wals.get(&node_id)?.store(RaftGroupId(group)) else {
            return Some(format!(
                "node {node_id}'s reopened log of group {group} has no open store"
            ));
        };
        let mut store = Arc::clone(&store);
        let state = match store.get_log_state().await {
            Ok(state) => state,
            Err(error) => {
                return Some(format!(
                    "node {node_id}'s reopened log of group {group} does not read: {error}"
                ));
            }
        };
        (state.last_log_id.is_some() || state.last_purged_log_id.is_some()).then(|| {
            format!(
                "node {node_id}'s reopened log of group {group}, which took no entry since the \
                 reopen, holds entries up to {:?} and a purge at {:?}: its retired life replays",
                state.last_log_id, state.last_purged_log_id
            )
        })
    }

    /// The running nodes that host a replica of `group`.
    fn hosting(&self, group: u32) -> Vec<(u64, Arc<SimHost>)> {
        self.placed()
            .hosts
            .iter()
            .filter(|(_, node)| node.host.raft(group).is_some())
            .map(|(node_id, node)| (*node_id, Arc::clone(&node.host)))
            .collect()
    }

    /// The leader some running replica of `group` names, if that leader's
    /// replica runs.
    fn placed_leader(&self, group: u32) -> Option<u64> {
        let hosting = self.hosting(group);
        hosting.iter().find_map(|(_, host)| {
            let leader = openraft::rt::WatchReceiver::borrow_watched(&host.raft(group)?.metrics())
                .current_leader?;
            hosting
                .iter()
                .any(|(node_id, _)| *node_id == leader)
                .then_some(leader)
        })
    }

    /// Whether the voters of `group` that run hold a majority of them: its
    /// seed voters when placed, else every node.
    pub(super) fn has_quorum(&self, group: u32) -> bool {
        let voters: BTreeSet<u64> = if self.placed.is_some() {
            self.seeds(group)
        } else {
            self.data.nodes().collect()
        };
        let running = voters
            .iter()
            .filter(|node_id| self.runtimes.contains_key(node_id))
            .count();
        running.saturating_mul(2) > voters.len()
    }

    /// Appends `count` payloads to `group` through its leader, retrying
    /// while leadership moves, and answers `false` at the first one the
    /// group does not take within the bound.
    /// Each append is keyed: its producer sequence number is the count the
    /// group acknowledged in the producer's epoch, so a retry of one whose
    /// answer was lost applies once. A history event (an accepted loss or a
    /// restore) closes the epoch, and the appender claims the next one, as
    /// a producer does on `epoch-closed;by=history`.
    pub(super) async fn placed_append(&mut self, group: u32, count: usize) -> bool {
        for _ in 0..count {
            let acknowledged = self.data.acknowledged.entry(group).or_default();
            let payload = format!("g{group}-{};", acknowledged.len()).into_bytes();
            let mut key = self.placed().keys.get(&group).copied().unwrap_or_default();
            let deadline = madsim::time::Instant::now() + BOUND;
            loop {
                let engine = self
                    .placed_leader(group)
                    .and_then(|leader| self.placed().hosts.get(&leader)?.host.engine(group));
                let mut request = AppendRequest::from_bytes(group_stream(group), payload.clone());
                request.producer = Some(ursula_runtime::ProducerRequest {
                    producer_id: format!("sim-placed-{group}"),
                    producer_epoch: key.epoch,
                    producer_seq: key.seq,
                    digest: None,
                    ..Default::default()
                });
                if let Some(engine) = engine {
                    let appended = engine
                        .lock()
                        .await
                        .append(
                            request,
                            group_placement(group),
                            ColdWriteAdmission::default(),
                        )
                        .await;
                    match appended {
                        Ok(_) => break,
                        Err(error) => key = key.after(&error),
                    }
                }
                if madsim::time::Instant::now() >= deadline {
                    return false;
                }
                madsim::time::sleep(STEP).await;
            }
            self.data
                .acknowledged
                .entry(group)
                .or_default()
                .extend_from_slice(&payload);
            self.placed_mut().keys.insert(group, key.acknowledged());
        }
        true
    }

    /// Why `node_id`'s replica of `group` does not read every acknowledged
    /// payload of the group, if it does not.
    async fn read_lag(&self, group: u32, node_id: u64) -> Option<String> {
        let expected = self
            .data
            .acknowledged
            .get(&group)
            .cloned()
            .unwrap_or_default();
        let Some(engine) = self
            .placed()
            .hosts
            .get(&node_id)
            .and_then(|node| node.host.engine(group))
        else {
            return Some(format!("node {node_id} runs no replica of group {group}"));
        };
        let read = engine
            .lock()
            .await
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
            .map(|read| read.payload);
        match read {
            Ok(payload) if payload == expected => None,
            Ok(payload) => Some(format!(
                "node {node_id} reads {} of group {group}'s {} acknowledged bytes",
                payload.len(),
                expected.len()
            )),
            Err(error) => Some(format!(
                "node {node_id} does not read group {group}: {error}"
            )),
        }
    }

    /// I1: every running voter of `group`, as the latest view places it,
    /// reads the group's acknowledged payloads within the bound.
    pub(super) async fn placed_reads(&self, trace: &mut SimTrace, after_event: &str, group: u32) {
        let deadline = madsim::time::Instant::now() + BOUND;
        for node_id in self.placed_voters(group) {
            if !self.runtimes.contains_key(&node_id) {
                continue;
            }
            while let Some(lag) = self.read_lag(group, node_id).await {
                if madsim::time::Instant::now() >= deadline {
                    fail(trace, I1, after_event, lag);
                }
                madsim::time::sleep(STEP).await;
            }
        }
    }

    /// I15: every group forms within the bound: it elects a leader among its
    /// seed voters, its stream exists, and every running seed voter's gate
    /// opens.
    pub(super) async fn groups_form(&mut self, trace: &mut SimTrace, after_event: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        for group in JOURNAL_GROUPS {
            loop {
                let leader = self.placed_leader(group);
                let formed = leader.is_some()
                    && self.seeds(group).iter().all(|node_id| {
                        !self.runtimes.contains_key(node_id)
                            || self
                                .placed()
                                .hosts
                                .get(node_id)
                                .is_some_and(|node| node.host.raft(group).is_some())
                    });
                if formed && self.placed().created.contains(&group) {
                    break;
                }
                if formed && let Some(leader) = leader {
                    let engine = self.placed().hosts[&leader].host.engine(group);
                    let created = match engine {
                        Some(engine) => engine
                            .lock()
                            .await
                            .create_stream(
                                CreateStreamRequest::new(
                                    group_stream(group),
                                    "application/octet-stream",
                                ),
                                group_placement(group),
                                ColdWriteAdmission::default(),
                            )
                            .await
                            .is_ok(),
                        None => false,
                    };
                    if created {
                        self.placed_mut().created.insert(group);
                        continue;
                    }
                }
                if madsim::time::Instant::now() >= deadline {
                    fail(
                        trace,
                        I15,
                        after_event,
                        format!(
                            "group {group} did not form on its seed voters {:?}: leader {leader:?}, \
                             hosted on {:?}",
                            self.seeds(group),
                            self.hosting(group)
                                .iter()
                                .map(|(node_id, _)| *node_id)
                                .collect::<Vec<_>>()
                        ),
                    );
                }
                madsim::time::sleep(STEP).await;
            }
        }
        while !self
            .placed()
            .hosts
            .values()
            .all(|node| node.host.gates_open())
        {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    trace,
                    I15,
                    after_event,
                    "a placed replica's recovery gate stayed closed".to_owned(),
                );
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// I3's live half: within the bound, every running node's view shows
    /// every group `Live` and stable on one voter set, and the group's
    /// leader holds a committed uniform membership of exactly those voters,
    /// with no learner.
    pub(super) async fn groups_live(&self, trace: &mut SimTrace, after_event: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        for group in JOURNAL_GROUPS {
            while let Some(behind) = self.not_settled(group) {
                if madsim::time::Instant::now() >= deadline {
                    fail(trace, I3, after_event, behind);
                }
                madsim::time::sleep(STEP).await;
            }
        }
    }

    /// Why `group` has not settled on its placement, if it has not. A
    /// removed node's view is left out: meta stops replicating to it.
    pub(super) fn not_settled(&self, group: u32) -> Option<String> {
        let mut placed: Option<BTreeSet<u64>> = None;
        let removed = self.removed_nodes();
        for node_id in self.running() {
            if removed.contains(&node_id) {
                continue;
            }
            let placement = self
                .view(node_id)
                .cluster()
                .and_then(|cluster| cluster.group(RaftGroupId(group)))
                .cloned();
            let stable = placement.as_ref().and_then(|placement| {
                (matches!(placement, ursula_control::GroupPlacement::Live { .. })
                    && placement.moving().is_none())
                .then(|| node_ids(placement.current_voters().iter()))
            });
            let Some(voters) = stable else {
                return Some(format!(
                    "group {group} is not live and stable in node {node_id}'s view: {placement:?}"
                ));
            };
            if placed.as_ref().is_some_and(|placed| *placed != voters) {
                return Some(format!(
                    "the views place group {group} on {placed:?} and on {voters:?}"
                ));
            }
            placed = Some(voters);
        }
        let placed = placed.unwrap_or_default();
        let Some(leader) = self.placed_leader(group) else {
            return Some(format!("group {group} has no running leader"));
        };
        let raft = self.hosted_raft(leader, group)?;
        let metrics = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).clone();
        let membership = metrics.committed_membership_config.membership();
        let configs = membership.get_joint_config();
        let learners: BTreeSet<u64> = membership.learner_ids().collect();
        let uncommitted =
            metrics.committed_membership_config.log_id() != metrics.membership_config.log_id();
        if uncommitted
            || configs.len() != 1
            || configs.first() != Some(&placed)
            || !learners.is_empty()
        {
            return Some(format!(
                "group {group}'s leader {leader} holds {configs:?} with learners {learners:?} \
                 (uncommitted change: {uncommitted}), not its placement {placed:?}"
            ));
        }
        None
    }

    /// I7: `node_id`, restarted while meta has no quorum, serves every
    /// group it is a seed voter of whose seed voters keep a quorum: its
    /// hosting driver opened the replica from the journals, the group takes
    /// appends, and the replica reads them back.
    pub(super) async fn serves_without_meta(
        &mut self,
        trace: &mut SimTrace,
        after_event: &str,
        node_id: u64,
    ) {
        for group in JOURNAL_GROUPS {
            if !self.seeds(group).contains(&node_id) || !self.has_quorum(group) {
                continue;
            }
            if !self.placed_append(group, 2).await {
                fail(
                    trace,
                    I7,
                    after_event,
                    format!("group {group} took no append after node {node_id} restarted"),
                );
            }
            let deadline = madsim::time::Instant::now() + BOUND;
            while let Some(lag) = self.read_lag(group, node_id).await {
                if madsim::time::Instant::now() >= deadline {
                    fail(trace, I7, after_event, lag);
                }
                madsim::time::sleep(STEP).await;
            }
        }
    }
}

/// Restarts one placed replica for a recovery plan's cut.
struct HostRestart {
    host: Arc<SimHost>,
}

impl ursula_raft::ReplicaRestart for HostRestart {
    fn restart_replica(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> BoxFuture<'_, Result<Option<Arc<GroupRejoin>>, ursula_runtime::RuntimeError>> {
        Box::pin(async move { Ok(self.host.restart(raft_group_id.0).await) })
    }
}

/// [`PlanTransport`] over each node's production data plan entry point
/// ([`ursula_raft::RaftGroupHandleRegistry::accept_recovery_plan`]), its
/// refusal typed as the node's route types it.
struct DataPlanTransport<'a> {
    cluster: &'a MetaCluster,
    group: u32,
}

impl DataPlanTransport<'_> {
    async fn accept(
        &self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        let host = self
            .cluster
            .placed()
            .hosts
            .get(&node_id)
            .map(|node| Arc::clone(&node.host))
            .ok_or(SendError::NotInManifest { node_id })?;
        let registry = host.parts.registry.clone();
        registry
            .accept_recovery_plan(RaftGroupId(self.group), request, &mut HostRestart { host })
            .await
            .map_err(|error| match error.refusal() {
                Some(refusal) => SendError::Refused {
                    node_id,
                    refused: RecoveryPlanRefused {
                        raft_group_id: u64::from(self.group),
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

impl PlanTransport for DataPlanTransport<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl std::future::Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.accept(node_id, request)
    }
}

impl MetaCluster {
    /// The replicas of `group` on `nodes` as the recovery tool reads them
    /// from each node's metrics, skipping nodes that publish none.
    fn data_members(&self, group: u32, nodes: &[u64]) -> Vec<PlanMember> {
        nodes
            .iter()
            .filter_map(|node_id| {
                self.registry(*node_id)
                    .metrics_snapshot()
                    .into_iter()
                    .find(|snapshot| snapshot.raft_group_id == group)
            })
            .map(|snapshot| PlanMember::from_metrics(&snapshot.to_admin()))
            .collect()
    }

    /// The floor `ursulactl recover-group` plans data group `group` with
    /// (`group_floor`), from the view of the first running node that
    /// confirms one, as the tool asks the manifest's nodes in turn.
    async fn data_floor(&self, group: u32) -> Result<Floor, FloorError> {
        let mut confirmed = None;
        for runtime in self.runtimes.values() {
            if let Ok(status) = runtime.status_reader().confirmed_placement_status().await {
                confirmed = Some(status.view);
                break;
            }
        }
        ursula_ctl::recovery::group_floor(group, confirmed.as_ref())
    }

    /// `ursulactl recover-group --accept-loss` for each data group of
    /// `groups` after a meta plan, as the operations order has it (meta
    /// first): the floor the tool reads from a confirmed view, then over the
    /// group's running seed voters, the tool's rounds until its plan is
    /// satisfied, or until it finds the group healthy.
    pub(super) async fn recover_data(
        &mut self,
        trace: &mut SimTrace,
        after_event: &str,
        groups: &[u32],
    ) {
        for group in groups.iter().copied() {
            let floor = match self.data_floor(group).await {
                Ok(floor) => floor,
                Err(error) => fail(
                    trace,
                    DATA_PLAN,
                    after_event,
                    format!("group {group} has no plan floor after the meta plan: {error}"),
                ),
            };
            let nodes: Vec<u64> = self
                .seeds(group)
                .into_iter()
                .filter(|node_id| self.runtimes.contains_key(node_id))
                .collect();
            let mut state = LoopState::default();
            let within = super::recovery_wiring::RECOVERY_STALL_AFTER + BOUND;
            let deadline = madsim::time::Instant::now() + within;
            loop {
                let members = self.data_members(group, &nodes);
                let mut transport = DataPlanTransport {
                    cluster: self,
                    group,
                };
                match recover_round(&mut state, &members, &mut transport, &floor).await {
                    Ok(Some(_))
                    | Err(MetaRecoverFailed::NoPlan {
                        outcome: RecoverOutcome::Healthy,
                    }) => break,
                    // A group whose leader outlived the loss needs no plan:
                    // its gated replicas open through its barrier.
                    Err(MetaRecoverFailed::Recover(RecoverError::Plan(PlanError::HasLeader {
                        ..
                    }))) if state.sent.is_none() => break,
                    Ok(None) if madsim::time::Instant::now() < deadline => {
                        madsim::time::sleep(STEP).await;
                    }
                    Ok(None) => fail(
                        trace,
                        DATA_PLAN,
                        after_event,
                        format!(
                            "no plan of group {group} was satisfied within {within:?}: {state:?}"
                        ),
                    ),
                    Err(error) => fail(
                        trace,
                        DATA_PLAN,
                        after_event,
                        format!("the recovery of group {group} stopped: {error}; {state:?}"),
                    ),
                }
            }
        }
    }
}

/// What the hosting checks keep across a run.
#[derive(Debug, Default)]
pub(super) struct HostingChecks {
    /// The leader of each group in each term some replica led it in.
    leaders: BTreeMap<(u32, u64), u64>,
    /// Every node some view gave a role in each group.
    placed: BTreeMap<u32, BTreeSet<u64>>,
    /// Each node whose role in a group the latest view took away, with
    /// the highest term the group's replicas held when it was first seen.
    lost: BTreeMap<(u32, u64), u64>,
    /// The furthest placement of each group, by its key, that the latest
    /// view showed at an observation.
    last: BTreeMap<u32, ursula_control::GroupPlacement>,
    /// The nodes whose retirement records of each group the latest view
    /// kept at the last observation.
    kept: BTreeMap<u32, BTreeSet<u64>>,
    /// What I4, I5, I8 and I9 judged.
    judged: Judged,
}

/// What I4, I5, I8 and I9 judged across a run, by group.
#[derive(Debug, Default)]
struct Judged {
    /// I4: the membership entries whose leader it checked, by index and
    /// term.
    entries: BTreeMap<u32, BTreeSet<(u64, u64)>>,
    /// I5's record half: how many retirement records it saw the latest
    /// view keep and a later latest view drop.
    dropped_records: BTreeMap<u32, u64>,
    /// I8: the voter changes among the entries, by index and term.
    voter_changes: BTreeMap<u32, BTreeSet<(u64, u64)>>,
    /// I8's teardown half: the nodes whose `Retired` log it checked
    /// against a committed membership.
    retired_logs: BTreeMap<u32, BTreeSet<u64>>,
    /// I9: how many moves it saw converge.
    convergences: BTreeMap<u32, u64>,
}

/// `nodes` as node ids.
pub(super) fn node_ids(
    nodes: impl Iterator<Item = ursula_proto::placement::NodeId>,
) -> BTreeSet<u64> {
    nodes.map(ursula_proto::placement::NodeId::get).collect()
}

impl HostingChecks {
    /// I2, I3, I4, I8, I9 and the hosting check, over every running placed
    /// replica and every membership entry the run's replicas showed, and
    /// what I5's record half judges.
    pub(super) fn observe(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let Some(placed) = cluster.placed.as_ref() else {
            return;
        };
        for (group, nodes) in placed.started.initializers() {
            if nodes.len() > 1 {
                fail(
                    trace,
                    I2,
                    after_event,
                    format!("group {group} was initialized more than once, by nodes {nodes:?}"),
                );
            }
        }
        self.learn_roles(cluster);
        self.hosted(cluster, placed, trace, after_event);
        for group in JOURNAL_GROUPS {
            self.entries(placed, group, trace, after_event);
            self.reports(cluster, placed, group, trace, after_event);
            self.voters_keep_their_logs(cluster, group, trace, after_event);
            self.records_dropped(cluster, group);
        }
    }

    /// I8's teardown half: no running node has retired its log of `group`
    /// while the group's latest committed membership lists it as a voter.
    /// A teardown retires a log only once a live leader answered that the
    /// node is no member, after the replica stopped.
    fn voters_keep_their_logs(
        &mut self,
        cluster: &MetaCluster,
        group: u32,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let Some((index, configs, _)) = cluster.latest_committed(group) else {
            return;
        };
        for node_id in cluster.running() {
            if cluster.local_log(node_id, group) != Some(LocalLog::Retired) {
                continue;
            }
            self.judged
                .retired_logs
                .entry(group)
                .or_default()
                .insert(node_id);
            if configs.iter().any(|voters| voters.contains(&node_id)) {
                fail(
                    trace,
                    I8,
                    after_event,
                    format!(
                        "node {node_id} retired its log of group {group}, whose latest committed \
                         membership {configs:?} (index {index}) lists it as a voter"
                    ),
                );
            }
        }
    }

    /// Counts the retirement records of `group` that an earlier latest
    /// view kept and the latest view no longer keeps, so a moved group's
    /// I5 wait shows that it saw a record go.
    fn records_dropped(&mut self, cluster: &MetaCluster, group: u32) {
        let latest = cluster.latest_view();
        let Some(bootstrapped) = latest.cluster() else {
            return;
        };
        let kept = bootstrapped
            .retirements()
            .get(&RaftGroupId(group))
            .map(|records| node_ids(records.keys().copied()))
            .unwrap_or_default();
        let previous = self.kept.insert(group, kept.clone()).unwrap_or_default();
        let dropped = u64::try_from(previous.difference(&kept).count()).unwrap_or(u64::MAX);
        if dropped > 0 {
            let count = self.judged.dropped_records.entry(group).or_insert(0);
            *count = count.saturating_add(dropped);
        }
    }

    /// Every role each running node's view gives, and the roles the latest
    /// view took away or gave back.
    fn learn_roles(&mut self, cluster: &MetaCluster) {
        let nodes: Vec<u64> = cluster.data.nodes().collect();
        for node_id in cluster.running() {
            let view = cluster.view(node_id);
            let Some(bootstrapped) = view.cluster() else {
                continue;
            };
            for group in JOURNAL_GROUPS {
                for node in &nodes {
                    if bootstrapped
                        .role(
                            RaftGroupId(group),
                            ursula_proto::placement::NodeId::new(*node),
                        )
                        .is_some()
                    {
                        self.placed.entry(group).or_default().insert(*node);
                    }
                }
            }
        }
        let latest = cluster.latest_view();
        let Some(bootstrapped) = latest.cluster() else {
            return;
        };
        for group in JOURNAL_GROUPS {
            let term = cluster.max_term(group);
            for node in self.placed.get(&group).cloned().unwrap_or_default() {
                let role = bootstrapped.role(
                    RaftGroupId(group),
                    ursula_proto::placement::NodeId::new(node),
                );
                match role {
                    None | Some(ursula_proto::placement::ReplicaRole::Abandoned) => {
                        self.lost.entry((group, node)).or_insert(term);
                    }
                    Some(
                        ursula_proto::placement::ReplicaRole::Voter
                        | ursula_proto::placement::ReplicaRole::Incoming
                        | ursula_proto::placement::ReplicaRole::Outgoing,
                    ) => {
                        self.lost.remove(&(group, node));
                    }
                }
            }
        }
    }

    /// The hosting check, I2's leaders and I3's memberships, over every
    /// running placed replica.
    fn hosted(
        &mut self,
        cluster: &MetaCluster,
        placed: &Placed,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        if let Some(creation) = placed.creations.unbootstrapped().first() {
            fail(
                trace,
                HOSTED,
                after_event,
                format!(
                    "node {} created group {} before its own view bootstrapped",
                    creation.node_id, creation.group
                ),
            );
        }
        let bootstrapped = cluster
            .running()
            .iter()
            .any(|node_id| cluster.view(*node_id).cluster().is_some());
        for (node_id, node) in &placed.hosts {
            let groups = node.host.groups();
            let unplaced: Vec<u32> = groups
                .iter()
                .copied()
                .filter(|group| {
                    !bootstrapped
                        || !self
                            .placed
                            .get(group)
                            .is_some_and(|nodes| nodes.contains(node_id))
                })
                .collect();
            if !unplaced.is_empty() {
                fail(
                    trace,
                    HOSTED,
                    after_event,
                    format!(
                        "node {node_id} runs groups {unplaced:?} no placement gave it a role in \
                         (bootstrapped: {bootstrapped})"
                    ),
                );
            }
            for group in groups {
                let Some(raft) = node.host.raft(group) else {
                    continue;
                };
                let metrics = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).clone();
                if metrics.state == ServerState::Leader {
                    let leader = *self
                        .leaders
                        .entry((group, metrics.current_term))
                        .or_insert(*node_id);
                    if leader != *node_id {
                        fail(
                            trace,
                            I2,
                            after_event,
                            format!(
                                "nodes {leader} and {node_id} both led group {group} in term {}",
                                metrics.current_term
                            ),
                        );
                    }
                }
                let membership = metrics.membership_config.membership();
                let named: BTreeSet<u64> = membership
                    .get_joint_config()
                    .iter()
                    .flatten()
                    .copied()
                    .chain(membership.learner_ids())
                    .collect();
                let roles = self.placed.get(&group).cloned().unwrap_or_default();
                let strangers: Vec<u64> = named.difference(&roles).copied().collect();
                if !strangers.is_empty() {
                    fail(
                        trace,
                        I3,
                        after_event,
                        format!(
                            "node {node_id} holds group {group}'s membership {:?} naming nodes \
                             {strangers:?} no placement gave a role",
                            membership.get_joint_config()
                        ),
                    );
                }
            }
        }
    }

    /// I4 and I8 over every membership entry of `group` the run's replicas
    /// showed.
    fn entries(&mut self, placed: &Placed, group: u32, trace: &mut SimTrace, after_event: &str) {
        for ((index, term), entry) in placed.memberships.of(group) {
            // The initial membership has no leader.
            if term == 0 {
                continue;
            }
            let rf_one_step = entry.configs.iter().all(|config| config.len() == 1);
            if !rf_one_step {
                self.judged
                    .entries
                    .entry(group)
                    .or_default()
                    .insert((index, term));
            }
            if !rf_one_step
                && !entry
                    .configs
                    .last()
                    .is_some_and(|config| config.contains(&entry.leader))
            {
                fail(
                    trace,
                    I4,
                    after_event,
                    format!(
                        "leader {} of term {term} appended group {group}'s membership {:?} at \
                         index {index}, which leaves it out",
                        entry.leader, entry.configs
                    ),
                );
            }
            let [old, new] = entry.configs.as_slice() else {
                continue;
            };
            if old != new {
                self.judged
                    .voter_changes
                    .entry(group)
                    .or_default()
                    .insert((index, term));
            }
            for added in new.difference(old) {
                if let Some(lost) = self.lost.get(&(group, *added))
                    && term > *lost
                {
                    fail(
                        trace,
                        I8,
                        after_event,
                        format!(
                            "leader {} of term {term} made node {added} a voter of group \
                             {group} ({old:?} to {new:?}, index {index}) after the placement \
                             took its role away at term {lost}",
                            entry.leader
                        ),
                    );
                }
            }
        }
    }

    /// I9 over `group`'s placement in the latest view.
    fn reports(
        &mut self,
        cluster: &MetaCluster,
        placed: &Placed,
        group: u32,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let Some(placement) = cluster.placement(group) else {
            return;
        };
        // The latest view falls behind the last one when the node whose meta
        // replica applied the most stops, so only a placement keyed above the
        // furthest one seen is new. A move's report is first seen when a new
        // placement is settled and the furthest one before it was moving, or
        // named another membership: only a converge changes `converged_at`,
        // and a move planned and converged between two observations shows
        // only its report.
        let newer = self
            .last
            .get(&group)
            .is_none_or(|last| last.since() < placement.since());
        let previous = if newer {
            self.last.insert(group, placement.clone())
        } else {
            None
        };
        if let ursula_control::GroupPlacement::Live { converged_at, .. } = &placement
            && let Some(
                previous @ ursula_control::GroupPlacement::Live {
                    converged_at: converged_before,
                    ..
                },
            ) = &previous
            && (previous.moving().is_some() || converged_before != converged_at)
            && placement.moving().is_none()
        {
            let convergences = self.judged.convergences.entry(group).or_default();
            *convergences = convergences.saturating_add(1);
            let reported = converged_at.get().index;
            let later: Vec<(u64, u64)> = placed
                .memberships
                .of(group)
                .into_iter()
                .filter(|((index, _), entry)| entry.committed && *index > reported)
                .map(|(key, _)| key)
                .collect();
            if !later.is_empty() {
                fail(
                    trace,
                    I9,
                    after_event,
                    format!(
                        "group {group}'s move converged at membership index {reported}, but \
                         memberships {later:?} (index, term) were committed before"
                    ),
                );
            }
        }
        let retired: BTreeSet<u64> = cluster
            .latest_view()
            .cluster()
            .and_then(|bootstrapped| bootstrapped.retirements().get(&RaftGroupId(group)))
            .map(|records| node_ids(records.keys().copied()))
            .unwrap_or_default();
        if let Some((index, configs, _)) = cluster.latest_committed(group) {
            let voters: BTreeSet<u64> = configs.iter().flatten().copied().collect();
            let listed: Vec<u64> = voters.intersection(&retired).copied().collect();
            if !listed.is_empty() {
                fail(
                    trace,
                    I9,
                    after_event,
                    format!(
                        "group {group}'s latest committed membership {configs:?} (index {index}) \
                         lists retired nodes {listed:?} as voters"
                    ),
                );
            }
        }
    }

    /// At a run's end, for each group whose voters are no longer its seed
    /// voters: I4 checked some entry's leader (unless the group has one
    /// voter, whose entries I4 exempts), I5 saw a retirement record of the
    /// group dropped, I8 saw a voter change and checked a `Retired` log of
    /// the group against a committed membership, I9 saw the move converge,
    /// and the creation rule judged a node outside the seed voters creating
    /// the group's log. A check that judged nothing of a moved group fails,
    /// so a vacuous pass shows. A group whose dropped seed voters were all
    /// removed has no record to drop and no log retired: the removal that
    /// dropped them drops their records and closes their replicas, and
    /// [`super::placed_removals::RemovalChecks::judged_the_removals`] judges them.
    pub(super) fn judged_the_moves(
        &self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let Some(placed) = cluster.placed.as_ref() else {
            return;
        };
        let removed = cluster.removed_nodes();
        for group in JOURNAL_GROUPS {
            let seeds = cluster.seeds(group);
            let voters = cluster.placed_voters(group);
            if voters == seeds {
                continue;
            }
            let dropped: BTreeSet<u64> = seeds.difference(&voters).copied().collect();
            let decommissioned = !dropped.is_empty() && dropped.is_subset(&removed);
            let moved = format!("group {group} moved from {seeds:?} to {voters:?}");
            let judged = |judged: &BTreeMap<u32, BTreeSet<(u64, u64)>>| {
                judged.get(&group).map_or(0, BTreeSet::len)
            };
            if voters.len() > 1 && judged(&self.judged.entries) == 0 {
                fail(
                    trace,
                    I4,
                    after_event,
                    format!("{moved}, but I4 judged no membership entry of it"),
                );
            }
            if !decommissioned
                && self
                    .judged
                    .dropped_records
                    .get(&group)
                    .copied()
                    .unwrap_or(0)
                    == 0
            {
                fail(
                    trace,
                    I5,
                    after_event,
                    format!("{moved}, but I5 saw no retirement record of it dropped"),
                );
            }
            if judged(&self.judged.voter_changes) == 0 {
                fail(
                    trace,
                    I8,
                    after_event,
                    format!("{moved}, but I8 judged no voter change of it"),
                );
            }
            if !decommissioned
                && self
                    .judged
                    .retired_logs
                    .get(&group)
                    .is_none_or(BTreeSet::is_empty)
            {
                fail(
                    trace,
                    I8,
                    after_event,
                    format!("{moved}, but I8 saw no node's log of it retired"),
                );
            }
            if self.judged.convergences.get(&group).copied().unwrap_or(0) == 0 {
                fail(
                    trace,
                    I9,
                    after_event,
                    format!("{moved}, but I9 saw no move of it converge"),
                );
            }
            let created: BTreeSet<u64> = placed
                .creations
                .creators(group)
                .difference(&seeds)
                .copied()
                .collect();
            if created.is_empty() {
                fail(
                    trace,
                    HOSTED,
                    after_event,
                    format!(
                        "{moved}, but the creation rule judged no creation of it outside its \
                         seed voters"
                    ),
                );
            }
        }
    }
}
