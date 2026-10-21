//! This node's replica of the meta group: its Raft over the meta journal,
//! the tasks it owns, the registry slot inbound RPCs find it in, and its
//! restart for a recovery plan.
//!
//! [`MetaReplica::open`] opens the meta group's log store on the meta
//! journal ([`crate::RaftWal::open_meta`]), its recovery gate, its state
//! machine with its restored snapshot record, and its Raft, with the one
//! Raft config of both planes (`cluster_raft_config`) and log reversion
//! allowed, since the replica is gated. It records the Raft's work in the
//! meta slot of the WAL's work ledger, starts the replica's tasks
//! ([`MetaReplicaTasks`]) and keeps their handles, and publishes the
//! replica in the registry as an [`ActiveMeta`]. The meta group uses none of
//! the node-wide structures the data groups share by group id (the
//! registry's group map, the snapshot build and install coordinators, the
//! snapshot store), so its group 0 never meets data group 0.
//!
//! [`MetaReplica::shutdown`] stops the tasks, then the Raft and the work
//! OpenRaft runs for it, as a data group's engine stops. A replica dropped
//! before its shutdown stops the same way in the background, in a task the
//! ledger keeps, so the meta journal does not reopen under it.
//!
//! [`MetaReplicaRestart`] holds the running replica with what reopens it.
//! A recovery plan that cuts the meta log restarts the replica through it
//! (`RaftGroupHandleRegistry::accept_meta_recovery_plan`): the replica
//! stops with its tasks, the store reopens on the held meta journal, where
//! the recovery gate runs the cut, and a new replica with the same tasks
//! takes the registry slot.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::error::Fatal;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::MetaDamageKind;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::HistoryPublisher;
use ursula_runtime::SnapshotPointer;

use super::MetaCommand;
use super::damage::CommitScreenDamage;
use super::damage::Screened;
use super::damage::commit_screen_damage;
use super::genesis::GenesisAnswer;
use super::genesis::GenesisProbe;
use super::handle::MetaWriteError;
use super::snapshot::MetaSnapshotError;
use super::snapshot::MetaSnapshotPointer;
use super::state_machine::META_GROUP;
use super::state_machine::MetaStateMachine;
use crate::GateOpenError;
use crate::GroupRejoin;
use crate::IncomingTransferError;
use crate::MetaOpenError;
use crate::QuorumProofError;
use crate::RaftEngineConfig;
use crate::RaftGroupFileLogStore;
use crate::RaftGroupHandleRegistry;
use crate::RaftWal;
use crate::SnapshotInstallError;
use crate::SnapshotOrderError;
use crate::SnapshotRecordError;
use crate::election::ElectionPolicy;
use crate::engine::EngineStopper;
use crate::history::GroupHistory;
use crate::openraft_work::EngineWorkLedger;
use crate::openraft_work::OpenRaftWork;
use crate::openraft_work::WorkAdmission;
use crate::openraft_work::WorkKind;
use crate::read_index::ReadIndexBarrier;
use crate::rejoin::RecoveryGate;
use crate::replica::ReplicaCore;
use crate::rt::sync::watch;
use crate::types::RaftCommand;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaSnapshot;
use crate::types::UrsulaVote;

/// What a meta replica opens with, and opens with again on a restart.
#[derive(Debug, Clone)]
pub struct MetaReplicaConfig {
    /// This node's id.
    pub node: u64,
    /// The node's Raft engine settings, which the meta Raft's config takes
    /// as the data groups' do.
    pub engine: RaftEngineConfig,
    /// The values this node would start the meta group with, which it
    /// answers a genesis probe with when the prober claims others.
    pub claim: GenesisClaim,
}

/// The tasks a meta replica runs and owns besides its history driver, which
/// every replica runs. A restarted replica starts the same ones.
#[derive(Debug, Clone, Default)]
pub struct MetaReplicaTasks {
    /// The meta snapshot trigger ([`super::snapshot::run_meta_snapshot_trigger`]),
    /// if the replica runs one.
    pub snapshots: Option<MetaSnapshotTrigger>,
    /// The gate drivers and the membership reconcile shell, which the meta
    /// runtime gives every replica it opens.
    pub(crate) drivers: Option<Arc<dyn MetaDrivers>>,
    /// Where the replica reports a for-good refusal of its commit screen.
    pub(crate) damage: Option<watch::Sender<Option<CommitScreenDamage>>>,
}

/// Spawns a meta replica's gate drivers and reconcile shell onto its
/// tasks. Type-erased, so the replica's tasks do not name the transport.
pub(crate) trait MetaDrivers: Send + Sync + std::fmt::Debug {
    fn spawn(&self, replica: MetaDriverContext<'_>);
}

/// What a meta replica's drivers run on.
pub(crate) struct MetaDriverContext<'a> {
    pub(crate) raft: &'a Raft<C, MetaStateMachine>,
    pub(crate) core: &'a ReplicaCore<MetaStateMachine>,
    pub(crate) gate: &'a Arc<GroupRejoin>,
    pub(crate) election: &'a ElectionPolicy,
    pub(crate) registry: &'a RaftGroupHandleRegistry,
    pub(crate) tasks: &'a RecoveryGate,
}

/// How often the meta snapshot trigger runs, and the journal age rule it
/// maintains the meta journal with.
#[derive(Debug, Clone, Copy)]
pub struct MetaSnapshotTrigger {
    /// The pause between two passes.
    pub interval: Duration,
    /// How long the meta journal's active segment may hold dead frames
    /// before it is sealed.
    pub max_dirty_age: Duration,
}

/// Why a meta replica did not open, or did not stop.
#[derive(Debug, thiserror::Error)]
pub enum MetaReplicaError {
    /// The meta group's log store did not open.
    #[error("open the meta group's log store")]
    Open(#[source] MetaOpenError),
    /// The meta group's recovery gate did not open.
    #[error("open the meta group's recovery gate")]
    Gate(#[source] GateOpenError),
    /// The meta snapshot record did not read.
    #[error("read the meta snapshot record")]
    Record(#[source] SnapshotRecordError),
    /// The meta snapshot record's pointer is not the meta group's own.
    #[error("the meta snapshot record's pointer is refused")]
    Pointer(#[source] MetaSnapshotError),
    /// The meta log contradicts the restored meta snapshot record (R1).
    #[error("the meta log contradicts the restored meta snapshot record")]
    LogBelowSnapshot(#[source] SnapshotOrderError),
    /// The meta snapshot record's state does not decode.
    #[error("decode the meta snapshot record's state")]
    State(#[source] ursula_stream::msgpack::DecodeError),
    /// OpenRaft refused the meta Raft's config.
    #[error("the meta Raft's config is refused")]
    Config(#[source] GroupEngineError),
    /// The meta Raft did not start.
    #[error("start the meta Raft")]
    Raft(#[source] Fatal<C>),
    /// The recovery gate did not bind to the meta Raft.
    #[error("bind the meta group's recovery gate")]
    Bind(#[source] GroupEngineError),
    /// The replica did not stop: its Raft, or the work OpenRaft runs for
    /// it, outlived the shutdown.
    #[error("stop the meta replica")]
    Stop(#[source] GroupEngineError),
}

impl MetaReplicaError {
    /// How a replica that did not open is damaged, as status reports name
    /// it. `None` for a stop, which is no failure to open.
    pub fn damage_kind(&self) -> Option<MetaDamageKind> {
        match self {
            Self::Open(_) => Some(MetaDamageKind::Journal),
            Self::Pointer(_) => Some(MetaDamageKind::Pointer),
            Self::Record(_) | Self::LogBelowSnapshot(_) | Self::State(_) => {
                Some(MetaDamageKind::Restore)
            }
            Self::Gate(_) | Self::Config(_) | Self::Raft(_) | Self::Bind(_) => {
                Some(MetaDamageKind::Start)
            }
            Self::Stop(_) => None,
        }
    }
}

/// Who asks the meta group to write: this node, or another node that
/// forwarded the write, as its admitted node id names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOrigin {
    Local(NodeId),
    Forwarded { admitted: NodeId },
}

impl WriteOrigin {
    /// The node the write comes from.
    fn node(self) -> NodeId {
        match self {
            Self::Local(node) | Self::Forwarded { admitted: node } => node,
        }
    }
}

/// This node's running meta replica, as the registry publishes it to the
/// inbound RPCs and the meta runtime: the gate screens every RPC passes,
/// and the write path.
#[derive(Debug)]
pub struct ActiveMeta {
    pub(crate) core: ReplicaCore<MetaStateMachine>,
    /// Admits the snapshot installs peers send, each holding the replica's
    /// token until it ends; closed once the replica begins to stop.
    installs: Arc<WorkAdmission>,
    election: ElectionPolicy,
    claim: GenesisClaim,
    /// This node's id.
    node: NodeId,
    /// Where a for-good refusal of the commit screen is reported.
    damage: Option<watch::Sender<Option<CommitScreenDamage>>>,
}

impl ActiveMeta {
    /// An inbound append, through the recovery gate's screens.
    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, GroupEngineError> {
        self.core.apply_health.check(META_GROUP)?;
        let vote = request.vote;
        let answer = self.core.append_entries(META_GROUP, request).await;
        if let Err(GroupEngineError::Infra(refusal)) = &answer {
            self.screened(Screened::Append, refusal, &vote);
        }
        answer
    }

    /// Reports the commit screen's `refusal` of a `screened` request that
    /// carried `vote` when it damaged this replica for good
    /// ([`super::damage`]).
    fn screened(
        &self,
        screened: Screened,
        refusal: &ursula_runtime::GroupInfraError,
        vote: &UrsulaVote,
    ) {
        let own = WatchReceiver::borrow_watched(&self.core.owner().metrics()).vote;
        if !commit_screen_damage(screened, refusal, vote, &own) {
            return;
        }
        tracing::error!(
            %refusal,
            "meta replica: the commit screen refused the meta leader for good; this node runs no \
             meta replica until the operator discards its meta directory"
        );
        if let Some(damage) = &self.damage {
            damage.send_replace(Some(CommitScreenDamage {
                refusal: refusal.clone(),
            }));
        }
    }

    /// An inbound vote, through the recovery gate's vote screen.
    pub async fn vote(&self, request: VoteRequest<C>) -> Result<VoteResponse<C>, GroupEngineError> {
        self.core.apply_health.check(META_GROUP)?;
        self.core.vote(request).await
    }

    /// An inbound full snapshot. A meta snapshot is inline, so nothing is
    /// prefetched and no node-wide install permit is taken. Its pointer is
    /// parsed first: one that is not inline, or above the bound, is refused
    /// before the screens and before OpenRaft sees it. The commit screen
    /// reads the history the snapshot's state holds.
    pub async fn install_full_snapshot(
        &self,
        vote: UrsulaVote,
        snapshot: UrsulaSnapshot,
    ) -> Result<SnapshotResponse<C>, SnapshotInstallError> {
        let answer = self.install_screened(vote, snapshot).await;
        if let Err(SnapshotInstallError::Group(GroupEngineError::Infra(refusal))) = &answer {
            self.screened(Screened::Install, refusal, &vote);
        }
        answer
    }

    async fn install_screened(
        &self,
        vote: UrsulaVote,
        snapshot: UrsulaSnapshot,
    ) -> Result<SnapshotResponse<C>, SnapshotInstallError> {
        self.core.apply_health.check(META_GROUP)?;
        if let Some(floor) = self.core.check_recovery_vote(META_GROUP, vote)? {
            return Ok(SnapshotResponse::new(floor));
        }
        if let Some(refusal) = self
            .core
            .recovery
            .as_ref()
            .and_then(|rejoin| rejoin.refused_snapshot(snapshot.meta.last_log_id))
        {
            return Err(SnapshotInstallError::Group(GroupEngineError::Infra(
                refusal,
            )));
        }
        let pointer =
            SnapshotPointer::decode(snapshot.snapshot.pointer_bytes()).map_err(|source| {
                SnapshotInstallError::PointerUndecodable {
                    group: META_GROUP,
                    source,
                }
            })?;
        let history = MetaSnapshotPointer::try_from(pointer)
            .map_err(|source| SnapshotInstallError::MetaPointer { source })?
            .state()
            .map_err(|source| SnapshotInstallError::MetaState { source })?
            .history;
        let _lifetime = self
            .installs
            .admit()
            .ok_or(SnapshotInstallError::ShuttingDown { group: META_GROUP })?;
        let core = self.core.clone();
        self.core
            .owner()
            .call(move |raft| async move {
                core.screen_install(vote, snapshot.meta.last_log_id, Some(history))
                    .await?;
                Ok(raft.install_full_snapshot(vote, snapshot).await)
            })
            .await
            .map_err(crate::owner::owner_stopped)
            .and_then(|installed| {
                installed
                    .map_err(GroupEngineError::Infra)?
                    .map_err(crate::owner::owner_stopped)
            })
            .map_err(SnapshotInstallError::Group)
    }

    /// The answer to a peer's genesis probe: this node's own claim when the
    /// prober claims other values, else this replica's answer to the probe
    /// vote, through the recovery gate's vote screen as a data group's
    /// bootstrap probe vote is answered.
    pub(crate) async fn answer_genesis_probe(
        &self,
        probe: GenesisProbe,
    ) -> Result<GenesisAnswer, GroupEngineError> {
        if probe.claim != self.claim {
            return Ok(GenesisAnswer::Mismatch(self.claim.clone()));
        }
        Ok(GenesisAnswer::Vote(self.vote(probe.vote).await?))
    }

    /// A leadership transfer this node received, once the election policy
    /// accepts it against this replica's own gate.
    pub async fn handle_transfer_leader(
        &self,
        request: TransferLeaderRequest<C>,
    ) -> Result<TransferLeaderResponse<C>, IncomingTransferError> {
        self.core
            .apply_health
            .check(META_GROUP)
            .map_err(IncomingTransferError::Group)?;
        self.core
            .deliver_transfer(&self.election, META_GROUP, request)
            .await
    }

    /// The recovery barrier this replica answers as the meta leader, after
    /// its history fence.
    pub async fn confirm_recovery_barrier(&self) -> Result<crate::BarrierAnswer, QuorumProofError> {
        self.core.confirm_barrier(META_GROUP).await
    }

    /// Writes `commands` to the meta group from `origin`, as one batch. A
    /// `RegisterNode` that names another node than `origin`'s refuses the
    /// batch before anything is proposed. Only control commands are
    /// proposed: a stream command is refused as of the other plane
    /// (`OtherPlane`) and a history event as one only the history driver
    /// proposes (`NewHistory`), both before anything is proposed. The
    /// replica's own refusal names this node as the leader that refused.
    pub async fn write(
        &self,
        commands: Vec<RaftCommand>,
        origin: WriteOrigin,
    ) -> Result<Vec<Result<ControlResponse, GroupEngineError>>, MetaWriteError> {
        if let Some(refusal) = guard_registration(&commands, origin) {
            return Err(refusal);
        }
        self.propose(commands)
            .await
            .map_err(|source| MetaWriteError::Leader {
                leader: self.node,
                source,
            })
    }

    async fn propose(
        &self,
        commands: Vec<RaftCommand>,
    ) -> Result<Vec<Result<ControlResponse, GroupEngineError>>, GroupEngineError> {
        self.core.apply_health.check(META_GROUP)?;
        let history = Arc::clone(&self.core.history);
        self.core
            .owner()
            .call(move |raft| async move {
                crate::forward::write_commands_on_raft(raft, &history, commands).await
            })
            .await
            .map_err(crate::owner::owner_stopped)?
    }

    /// A write another node forwarded, which `admitted` names, answered as
    /// both transports send it back (design 3.7): each command's answer,
    /// the meta group's refusal of the whole batch as each command's
    /// answer, and the guard's refusal of a `RegisterNode` naming another
    /// node as an `INVALID_ARGUMENT` refusal of the request, before anything
    /// is proposed, which the sender reads as permanent
    /// ([`crate::ForwardError::status`]).
    pub(crate) async fn write_forwarded(
        &self,
        commands: Vec<RaftCommand>,
        admitted: NodeId,
    ) -> Result<Vec<Result<ControlResponse, GroupEngineError>>, tonic::Status> {
        if let Some(refusal) = guard_registration(&commands, WriteOrigin::Forwarded { admitted }) {
            return Err(crate::grpc::GrpcRpcError::invalid_argument(refusal.to_string()).into());
        }
        let count = commands.len();
        Ok(self
            .propose(commands)
            .await
            .unwrap_or_else(|refusal| vec![Err(refusal); count]))
    }

    /// Lets the node's election policy decide again whether this replica
    /// campaigns.
    pub(crate) fn refresh_elections(&self) {
        self.core.refresh_elections(&self.election);
    }
}

/// The refusal of a batch with a `RegisterNode` that names another node
/// than the one `origin` names: a node registers only itself.
fn guard_registration(commands: &[RaftCommand], origin: WriteOrigin) -> Option<MetaWriteError> {
    commands.iter().find_map(|command| match command {
        RaftCommand::Meta(MetaCommand::Control(ControlCommand::RegisterNode { node, .. }))
            if *node != origin.node() =>
        {
            Some(MetaWriteError::OtherNode {
                origin: origin.node(),
                named: *node,
            })
        }
        RaftCommand::Meta(_) | RaftCommand::Stream(_) => None,
    })
}

/// This node's meta replica (see the module docs).
pub struct MetaReplica {
    active: Arc<ActiveMeta>,
    raft: Raft<C, MetaStateMachine>,
    tasks: RecoveryGate,
    metadata_serial: Arc<crate::rt::sync::Mutex<crate::state_machine::MetadataPublicationState>>,
    openraft_work: OpenRaftWork,
    registry: RaftGroupHandleRegistry,
    /// The ledger that keeps a stop in the background, until the shutdown
    /// ran: what dropping the replica stops.
    drop_stop: Mutex<Option<EngineWorkLedger>>,
}

impl std::fmt::Debug for MetaReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaReplica")
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl MetaReplica {
    /// Opens this node's meta replica over `wal`'s meta journal, with
    /// `network` to its peers, and publishes it in `registry` (see the
    /// module docs).
    pub async fn open<N>(
        config: &MetaReplicaConfig,
        wal: &RaftWal,
        metrics: GroupEngineMetrics,
        network: N,
        registry: &RaftGroupHandleRegistry,
        tasks: &MetaReplicaTasks,
    ) -> Result<Self, MetaReplicaError>
    where
        N: RaftNetworkFactory<C>,
        N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
    {
        let store = wal.open_meta(metrics).map_err(MetaReplicaError::Open)?;
        Self::open_on(config, store, wal, network, registry, tasks).await
    }

    /// [`Self::open`] over `store`, the meta group's log store already
    /// opened on `wal`'s meta journal.
    pub async fn open_on<N>(
        config: &MetaReplicaConfig,
        store: Arc<RaftGroupFileLogStore>,
        wal: &RaftWal,
        network: N,
        registry: &RaftGroupHandleRegistry,
        tasks: &MetaReplicaTasks,
    ) -> Result<Self, MetaReplicaError>
    where
        N: RaftNetworkFactory<C>,
        N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
    {
        // The recovery gate goes first: a gated replica's Raft starts with
        // elections disabled, and a plan's cut runs before it reads the log.
        let gate = Arc::new(
            GroupRejoin::durable(config.node, META_GROUP, &store)
                .await
                .map_err(MetaReplicaError::Gate)?,
        );
        let history_publisher = HistoryPublisher::default();
        let mut state_machine = MetaStateMachine::new(
            store.snapshot_record(),
            history_publisher.clone(),
            registry.placement_publisher(),
            store.apply_stop_signal(),
        );
        state_machine.restore()?;
        let history = GroupHistory::new(META_GROUP, history_publisher.subscribe());
        history.set_plan(gate.recovery_record());
        let metadata_serial = Arc::clone(&state_machine.metadata_serial);
        let apply_health = state_machine.apply_health.clone();
        let (openraft_work, work) = OpenRaftWork::start();
        let installs = Arc::new(WorkAdmission::new(work.as_kind(WorkKind::Install)));
        let network = crate::openraft_work::TrackedNetworkFactory::new(
            network,
            work.as_kind(WorkKind::Client),
        );
        let store =
            crate::openraft_work::TrackedLogStore::new(store, work.as_kind(WorkKind::Reader));
        state_machine.work = work;
        let election = registry.election_policy();
        let raft_config =
            crate::openraft_error::validated_config(crate::engine::cluster_raft_config(
                "ursula-meta".to_owned(),
                &config.engine,
                election.may_campaign(Some(&gate)),
            ))
            .map_err(MetaReplicaError::Config)?;
        let raft_config = crate::engine::config_for_recovery(Arc::new(raft_config), true);
        let raft = Raft::new(config.node, raft_config, network, store, state_machine)
            .await
            .map_err(MetaReplicaError::Raft)?;
        // From here the Raft runs: the meta journal opens again only once
        // its work ended, whether the replica opens or not.
        let ledger = wal.engine_work_ledger();
        ledger.record_meta(openraft_work.ended());
        let core = ReplicaCore {
            barrier: Arc::new(ReadIndexBarrier::new(raft.clone())),
            recovery: Some(Arc::clone(&gate)),
            history: Arc::clone(&history),
            apply_health,
        };
        let replica = Self {
            active: Arc::new(ActiveMeta {
                core,
                installs,
                election,
                claim: config.claim.clone(),
                node: NodeId::new(config.node),
                damage: tasks.damage.clone(),
            }),
            raft: raft.clone(),
            tasks: RecoveryGate::default(),
            metadata_serial,
            openraft_work,
            registry: registry.clone(),
            drop_stop: Mutex::new(Some(ledger)),
        };
        // A failure from here drops the replica, which stops it.
        gate.bind(&raft).map_err(MetaReplicaError::Bind)?;
        history.bind_rejoin(&gate);
        replica
            .tasks
            .push(crate::rt::spawn(crate::history::run_history_driver(
                raft.clone(),
                history,
            )));
        if let Some(drivers) = &tasks.drivers {
            drivers.spawn(MetaDriverContext {
                raft: &raft,
                core: &replica.active.core,
                gate: &gate,
                election: &replica.active.election,
                registry,
                tasks: &replica.tasks,
            });
        }
        if let Some(trigger) = tasks.snapshots {
            replica.tasks.push(crate::rt::spawn(
                super::snapshot::run_meta_snapshot_trigger(
                    replica.active.core.owner().clone(),
                    wal.meta_maintenance(),
                    trigger,
                ),
            ));
        }
        registry.publish_meta(Some(Arc::clone(&replica.active)));
        replica.active.refresh_elections();
        Ok(replica)
    }

    /// The replica as the registry publishes it.
    pub fn active(&self) -> &Arc<ActiveMeta> {
        &self.active
    }

    /// The meta Raft, for tests that drive it directly.
    #[cfg(all(test, not(madsim)))]
    pub(crate) fn raft(&self) -> &Raft<C, MetaStateMachine> {
        &self.raft
    }

    /// What the replica's shutdown stops.
    fn stopper(&self) -> EngineStopper<MetaStateMachine> {
        EngineStopper {
            snapshot_installs: Arc::clone(&self.active.installs),
            raft: self.raft.clone(),
            metadata_serial: Arc::clone(&self.metadata_serial),
            openraft_work: self.openraft_work.clone(),
            raft_group_id: META_GROUP,
            work_stop_timeout: crate::openraft_work::ENGINE_WORK_STOP_TIMEOUT,
        }
    }

    /// Stops the replica: it leaves the registry, its tasks stop, then its
    /// Raft and the work OpenRaft runs for it.
    pub async fn shutdown(&self) -> Result<(), MetaReplicaError> {
        self.registry.unpublish_meta(&self.active);
        let stopped = self.stopper().stop(&self.tasks).await;
        // Dropping the replica stops nothing more: its Raft stopped, and the
        // ledger keeps the meta journal closed until the work ended.
        *self
            .drop_stop
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        stopped.map_err(MetaReplicaError::Stop)
    }
}

impl Drop for MetaReplica {
    /// Stops a replica dropped before its shutdown, in the background.
    fn drop(&mut self) {
        let Some(ledger) = self
            .drop_stop
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };
        self.registry.unpublish_meta(&self.active);
        let stopper = self.stopper();
        let tasks = std::mem::take(&mut self.tasks);
        let stopping = crate::rt::try_spawn(async move {
            if let Err(error) = stopper.stop(&tasks).await {
                tracing::warn!(%error, "failed to stop a meta replica dropped before its shutdown");
            }
        });
        match stopping {
            Some(stopping) => ledger.keep_stopping_meta(stopping),
            None => tracing::error!(
                "a meta replica was dropped before its shutdown outside a runtime, so the meta \
                 journal stays closed until the process restarts"
            ),
        }
    }
}

/// The running meta replica with what reopens it after a recovery plan's
/// cut: its config, its WAL, its network and its tasks.
pub struct MetaReplicaRestart<N> {
    config: MetaReplicaConfig,
    wal: RaftWal,
    metrics: GroupEngineMetrics,
    network: N,
    registry: RaftGroupHandleRegistry,
    tasks: MetaReplicaTasks,
    replica: MetaReplica,
}

impl<N> MetaReplicaRestart<N>
where
    N: RaftNetworkFactory<C> + Clone,
    N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
{
    /// Opens the meta replica ([`MetaReplica::open`]) and keeps what
    /// reopens it.
    pub async fn open(
        config: MetaReplicaConfig,
        wal: RaftWal,
        metrics: GroupEngineMetrics,
        network: N,
        registry: RaftGroupHandleRegistry,
        tasks: MetaReplicaTasks,
    ) -> Result<Self, MetaReplicaError> {
        let store = wal
            .open_meta(metrics.clone())
            .map_err(MetaReplicaError::Open)?;
        Self::open_on(config, store, wal, metrics, network, registry, tasks).await
    }

    /// [`Self::open`] over `store`, the meta group's log store already
    /// opened on `wal`'s meta journal: a restart reopens it on the journal.
    pub async fn open_on(
        config: MetaReplicaConfig,
        store: Arc<RaftGroupFileLogStore>,
        wal: RaftWal,
        metrics: GroupEngineMetrics,
        network: N,
        registry: RaftGroupHandleRegistry,
        tasks: MetaReplicaTasks,
    ) -> Result<Self, MetaReplicaError> {
        let replica =
            MetaReplica::open_on(&config, store, &wal, network.clone(), &registry, &tasks).await?;
        Ok(Self {
            config,
            wal,
            metrics,
            network,
            registry,
            tasks,
            replica,
        })
    }

    /// The running replica.
    pub fn replica(&self) -> &MetaReplica {
        &self.replica
    }

    /// Stops the replica with its tasks and opens it again on the held meta
    /// journal, where its recovery gate runs a plan's cut before the Raft
    /// reads the log. The new replica, with the same tasks, takes the
    /// registry slot. A replica that does not reopen leaves the slot empty.
    pub async fn restart(&mut self) -> Result<&MetaReplica, MetaReplicaError> {
        self.replica.shutdown().await?;
        self.replica = MetaReplica::open(
            &self.config,
            &self.wal,
            self.metrics.clone(),
            self.network.clone(),
            &self.registry,
            &self.tasks,
        )
        .await?;
        Ok(&self.replica)
    }

    /// Stops the replica.
    pub async fn shutdown(self) -> Result<(), MetaReplicaError> {
        self.replica.shutdown().await
    }
}
