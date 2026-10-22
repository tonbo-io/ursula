//! This node's meta runtime: its meta replica with every task the replica
//! owns, and the node tasks around it.
//!
//! [`MetaRuntime::start`] takes the meta journal the caller opened
//! ([`MetaJournal`]) and opens the meta replica on it. The replica's tasks
//! ([`MetaReplicaTasks`]) are its history driver, its snapshot trigger, its
//! gate drivers with the genesis probe ([`super::genesis_driver`]) and the
//! membership reconcile shell over the meta target
//! (`MetaTargetSource`), so a recovery plan's restart
//! reopens the replica with all of them. The node tasks are the
//! registration ([`super::registration`]), the formation
//! ([`super::formation`]) and the damage supervisor.
//!
//! No meta failure stops the node. A journal that did not open, or a
//! replica that did not, leaves the runtime [`MetaRuntimeState::Damaged`]
//! from the start. A recovery plan's restart that does not reopen the
//! replica, or a for-good refusal of the commit screen ([`super::damage`]),
//! makes it so later: the supervisor stops the replica with its tasks,
//! which clears the registry's slot. Every way into `Damaged` publishes the
//! empty view (design 3.2), since a replica that restored or replayed its
//! state before failing may have published a view no replica backs, and
//! marks the node's `PlacementUnavailable` leadership shed for the rest of
//! the run: a group this node led would never reconcile. A plan's restart
//! that reopens the replica marks nothing, and neither does the time
//! before the replica first opens nor a shutdown. The meta directory stays, and its discard is the
//! operator's step.
//!
//! [`MetaRuntime::status_reader`] reads the node's placement status
//! ([`super::status`]), [`MetaRuntime::plans`] takes the meta replica's
//! recovery plans ([`MetaPlans`]), and [`MetaRuntime::pauses`] holds the
//! pause points tests stop the runtime at ([`super::pause`]).
//!
//! Generic over the network and the recovery transport, so a simulation
//! runs it in process.

use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::RaftNetworkFactory;
use ursula_control::ControlError;
use ursula_control::MetaDamageKind;
use ursula_control::PlacementView;
use ursula_control::RegistrationStatus;
use ursula_control::SeedRoleOf;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ReconcileStatus;
use ursula_runtime::GroupEngineMetrics;

use super::damage::CommitScreenDamage;
use super::formation::FormationParams;
use super::formation::run_formation;
use super::genesis_driver::GenesisStatus;
use super::genesis_driver::genesis_gate_probe;
use super::handle::MetaHandle;
use super::pause::MetaPauses;
use super::registration::MetaLeaderless;
use super::registration::Registrant;
use super::registration::run_registration;
use super::registration::track_meta_leader;
use super::replica::MetaDriverContext;
use super::replica::MetaDrivers;
use super::replica::MetaReplicaConfig;
use super::replica::MetaReplicaRestart;
use super::replica::MetaReplicaTasks;
use super::replica::MetaSnapshotTrigger;
use super::root::RootGuard;
use super::status::MetaStatusReader;
use super::target::MetaTargetSource;
use crate::FixedNodes;
use crate::FixedRole;
use crate::LeadershipShedReason;
use crate::MetaOpenError;
use crate::RaftEngineConfig;
use crate::RaftGroupFileLogStore;
use crate::RaftGroupHandleRegistry;
use crate::RaftTarget;
use crate::RaftWal;
use crate::RecoveryConfig;
use crate::RecoveryGateError;
use crate::RecoveryTransport;
use crate::rejoin::NoPeerGates;
use crate::rejoin::ReconcileShell;
use crate::rt::sync::Mutex;
use crate::rt::sync::watch;
use crate::rt::task::JoinHandle;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;

/// What this node runs the meta group with.
#[derive(Debug, Clone)]
pub struct MetaRuntimeConfig {
    /// This node's id.
    pub node: u64,
    /// The values this node starts the meta group with.
    pub claim: GenesisClaim,
    /// What this node bootstraps the cluster with.
    pub formation: FormationParams,
    /// The node's Raft engine settings.
    pub engine: RaftEngineConfig,
    /// The meta snapshot trigger, if the replica runs one.
    pub snapshots: Option<MetaSnapshotTrigger>,
}

/// The meta journal as the caller opened it
/// ([`crate::RaftWal::open_meta`]), with the meta group's own WAL metrics.
#[derive(Debug)]
pub struct MetaJournal {
    pub opened: Result<Arc<RaftGroupFileLogStore>, MetaOpenError>,
    pub metrics: GroupEngineMetrics,
}

/// Whether this node runs a meta replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaRuntimeState {
    Running,
    /// No meta replica runs in this run, for `kind`. `message` is the
    /// rendered error.
    Damaged {
        kind: MetaDamageKind,
        message: String,
    },
}

/// The watches status reports read.
#[derive(Debug, Clone)]
pub struct MetaRuntimeStatus {
    pub state: watch::Receiver<MetaRuntimeState>,
    pub genesis: watch::Receiver<GenesisStatus>,
    pub registration: watch::Receiver<RegistrationStatus>,
    /// Why the last `Bootstrap` this node submitted was refused.
    pub bootstrap_refusal: watch::Receiver<Option<ControlError>>,
    pub reconcile: watch::Receiver<ReconcileStatus>,
    /// Since when this node has known no meta leader, as the meta leader's
    /// tracker publishes it, counted from the start until the tracker runs.
    pub leaderless: watch::Receiver<MetaLeaderless>,
}

/// This node's meta runtime (see the module docs).
pub struct MetaRuntime<N> {
    status: MetaRuntimeStatus,
    state: watch::Sender<MetaRuntimeState>,
    replica: Arc<Mutex<Option<MetaReplicaRestart<N>>>>,
    registry: RaftGroupHandleRegistry,
    reader: MetaStatusReader,
    pauses: MetaPauses,
    tasks: Vec<JoinHandle<()>>,
}

impl<N> std::fmt::Debug for MetaRuntime<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaRuntime")
            .field("state", &*self.status.state.borrow())
            .finish_non_exhaustive()
    }
}

impl<N> MetaRuntime<N>
where
    N: RaftNetworkFactory<C> + Clone + Send + Sync + 'static,
    N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
{
    /// Starts the meta runtime over `journal` (see the module docs).
    pub async fn start<T: RecoveryTransport>(
        config: MetaRuntimeConfig,
        journal: MetaJournal,
        wal: RaftWal,
        registry: RaftGroupHandleRegistry,
        network: N,
        transport: T,
    ) -> Self {
        let me = NodeId::new(config.node);
        let (state, state_rx) = watch::channel(MetaRuntimeState::Running);
        let (genesis, genesis_rx) =
            watch::channel(GenesisStatus::before_answers(me, &config.claim));
        let (registration, registration_rx) = watch::channel(RegistrationStatus::Pending);
        let (refusal, refusal_rx) = watch::channel(None);
        let (reconcile, reconcile_rx) =
            watch::channel(crate::rejoin::initial_status(ursula_runtime::unix_time_ms()));
        let (damage, damage_rx) = watch::channel(None);
        let (leaderless, leaderless_rx) = watch::channel(Some(crate::rt::time::Instant::now()));
        let status = MetaRuntimeStatus {
            state: state_rx,
            genesis: genesis_rx,
            registration: registration_rx,
            bootstrap_refusal: refusal_rx,
            reconcile: reconcile_rx,
            leaderless: leaderless_rx,
        };
        let guard = RootGuard::new(
            me,
            wal.incarnation().root,
            wal.binding(),
            wal.fence(),
            registry.clone(),
        );
        let handle = MetaHandle::new(me, registry.clone(), transport.clone(), &config.claim)
            .with_root_guard(guard.clone());
        let pauses = MetaPauses::default();
        let mut runtime = Self {
            reader: MetaStatusReader::new(
                me,
                config.claim.clone(),
                status.clone(),
                registry.clone(),
                handle.clone(),
            ),
            status,
            state,
            replica: Arc::new(Mutex::new(None)),
            registry: registry.clone(),
            pauses: pauses.clone(),
            tasks: Vec::new(),
        };
        // Before the meta replica opens, and whether it opens or not: a
        // bound root ends its wait without meta, and an unbound root waits
        // for its binding, holding its data until meta answers.
        runtime.tasks.push(crate::rt::spawn(
            crate::predecessor_wait::run_predecessor_wait(
                wal.predecessor_wait(),
                registry.clone(),
                runtime.status.registration.clone(),
            ),
        ));
        runtime
            .tasks
            .push(crate::rt::spawn(crate::predecessor_wait::run_fence_gate(
                registry.clone(),
                wal.fence(),
            )));
        let store = match journal.opened {
            Ok(store) => store,
            Err(error) => {
                tracing::error!(%error, "meta runtime: the meta journal did not open; this node runs no meta replica");
                mark_damaged(&runtime.state, &registry, MetaDamageKind::Journal, &error);
                return runtime;
            }
        };
        let tasks = MetaReplicaTasks {
            snapshots: config.snapshots,
            drivers: Some(Arc::new(ReplicaDrivers {
                node: me,
                claim: config.claim.clone(),
                transport,
                handle: handle.clone(),
                recovery: recovery_config(&config.engine),
                genesis,
                reconcile,
            })),
            damage: Some(damage),
            pauses: pauses.clone(),
        };
        let registrant = Registrant {
            node: me,
            root: wal.incarnation().root,
        };
        let replica_config = MetaReplicaConfig {
            node: config.node,
            engine: config.engine.clone(),
            claim: config.claim.clone(),
        };
        let restart = match MetaReplicaRestart::open_on(
            replica_config,
            store,
            wal,
            journal.metrics,
            network,
            registry.clone(),
            tasks,
        )
        .await
        {
            Ok(restart) => restart,
            Err(error) => {
                let kind = opening_damage(&error);
                tracing::error!(%error, ?kind, "meta runtime: the meta replica did not open; this node runs no meta replica");
                mark_damaged(&runtime.state, &registry, kind, &error);
                return runtime;
            }
        };
        *runtime.replica.lock().await = Some(restart);
        let stopped = {
            let state = runtime.status.state.clone();
            move || matches!(*state.borrow(), MetaRuntimeState::Damaged { .. })
        };
        runtime
            .tasks
            .push(crate::rt::spawn(super::root::watch_views(
                guard.clone(),
                registry.placement_watch(),
            )));
        runtime.tasks.push(crate::rt::spawn(track_meta_leader(
            registry.clone(),
            leaderless,
        )));
        runtime.tasks.push(crate::rt::spawn(run_registration(
            handle.clone(),
            registrant,
            guard,
            runtime.status.leaderless.clone(),
            registration,
            pauses.clone(),
            stopped.clone(),
        )));
        runtime.tasks.push(crate::rt::spawn(run_formation(
            handle,
            me,
            config.claim,
            config.formation,
            registry.clone(),
            refusal,
            pauses,
            stopped,
        )));
        runtime.tasks.push(crate::rt::spawn(supervise_damage(
            damage_rx,
            Arc::clone(&runtime.replica),
            registry,
            runtime.state.clone(),
        )));
        runtime
    }

    /// The watches status reports read.
    pub fn status(&self) -> &MetaRuntimeStatus {
        &self.status
    }

    /// Whether this node runs a meta replica now.
    pub fn state(&self) -> MetaRuntimeState {
        self.status.state.borrow().clone()
    }

    /// What this node's placement status reads.
    pub fn status_reader(&self) -> &MetaStatusReader {
        &self.reader
    }

    /// The pause points tests stop this runtime at.
    pub fn pauses(&self) -> &MetaPauses {
        &self.pauses
    }

    /// Where recovery plans of this node's meta replica go, apart from the
    /// runtime, so the admin route takes plans while shutdown owns the
    /// runtime. A plan after shutdown finds no replica.
    pub fn plans(&self) -> MetaPlans<N> {
        MetaPlans {
            replica: Arc::clone(&self.replica),
            registry: self.registry.clone(),
            state: self.state.clone(),
        }
    }

    /// Stops the node tasks, then the meta replica with its own tasks. The
    /// predecessor wait is one of the node tasks, so it ends here, before
    /// the WAL's shutdown closes the fence and records the clean end.
    pub async fn shutdown(self) -> Result<(), super::replica::MetaReplicaError> {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                tracing::error!(%error, "meta runtime: a node task failed");
            }
        }
        let replica = self.replica.lock().await.take();
        match replica {
            Some(replica) => replica.shutdown().await,
            None => Ok(()),
        }
    }
}

/// A server node's meta runtime, over gRPC.
pub type GrpcMetaRuntime = MetaRuntime<crate::GrpcRaftNetworkFactory>;

/// Takes recovery plans on this node's meta replica
/// ([`MetaRuntime::plans`]).
pub struct MetaPlans<N> {
    replica: Arc<Mutex<Option<MetaReplicaRestart<N>>>>,
    registry: RaftGroupHandleRegistry,
    state: watch::Sender<MetaRuntimeState>,
}

impl<N> Clone for MetaPlans<N> {
    fn clone(&self) -> Self {
        Self {
            replica: Arc::clone(&self.replica),
            registry: self.registry.clone(),
            state: self.state.clone(),
        }
    }
}

impl<N> std::fmt::Debug for MetaPlans<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPlans").finish_non_exhaustive()
    }
}

impl<N> MetaPlans<N>
where
    N: RaftNetworkFactory<C> + Clone + Send + Sync + 'static,
    N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
{
    /// A recovery plan on this node's meta replica
    /// ([`RaftGroupHandleRegistry::accept_meta_recovery_plan`]), which
    /// restarts the replica with its tasks when the plan cuts its log. A
    /// restart that does not reopen the replica leaves the runtime
    /// `Damaged`.
    pub async fn accept_recovery_plan(
        &self,
        plan: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, RecoveryGateError> {
        let mut replica = self.replica.lock().await;
        let restart = replica.as_mut().ok_or(RecoveryGateError::NotRegistered {
            group: RaftTarget::Meta,
        })?;
        let accepted = self.registry.accept_meta_recovery_plan(plan, restart).await;
        if let Err(RecoveryGateError::MetaRestart { source }) = &accepted {
            // The restart stopped the old replica, which unpublished it, and
            // no new one opened: the replica held here runs nothing.
            let kind = opening_damage(source);
            tracing::error!(%source, ?kind, "meta runtime: the meta replica did not reopen after a recovery plan; this node runs no meta replica");
            *replica = None;
            mark_damaged(&self.state, &self.registry, kind, source);
        }
        accepted
    }
}

/// [`MetaPlans`] of a server node, over gRPC.
pub type GrpcMetaPlans = MetaPlans<crate::GrpcRaftNetworkFactory>;

impl GrpcMetaRuntime {
    /// Starts a server node's meta runtime over gRPC: meta's own transport
    /// ([`crate::CoreRaftTransport::for_meta`]) sending as `identity`, the
    /// meta network over it with the data groups' reconnect threshold, and
    /// the recovery transport over it with their probe timeout
    /// (`bootstrap_peer_connect`).
    pub async fn start_over_grpc(
        config: MetaRuntimeConfig,
        journal: MetaJournal,
        wal: RaftWal,
        registry: RaftGroupHandleRegistry,
        identity: crate::PeerIdentity,
    ) -> Self {
        let transport = Arc::new(crate::CoreRaftTransport::for_meta(&registry, identity));
        let network = crate::GrpcRaftNetworkFactory::new(transport.clone(), RaftTarget::Meta)
            .with_reconnect_threshold(config.engine.grpc_reconnect_after_failures);
        let recovery = crate::recovery_transport::GrpcRecoveryTransport {
            transport,
            target: RaftTarget::Meta,
            timeout: config.engine.bootstrap_peer_connect,
        };
        Self::start(config, journal, wal, registry, network, recovery).await
    }
}

/// The recovery drivers' settings of the meta replica, those of the data
/// groups.
fn recovery_config(engine: &RaftEngineConfig) -> RecoveryConfig {
    RecoveryConfig {
        interval: crate::rejoin::REJOIN_HEAL_INTERVAL,
        barrier_timeout: crate::rejoin::RECOVERY_BARRIER_TIMEOUT,
        stall_after: crate::rejoin::RECOVERY_STALL_AFTER,
        bootstrap_interval: engine.bootstrap_peer_probe_interval,
        bootstrap_warn_after: engine.bootstrap_peer_probe,
    }
}

/// How a replica that did not open, or did not reopen, is damaged: as its
/// error names it, and `Start` for a stop, which names none.
fn opening_damage(error: &super::replica::MetaReplicaError) -> MetaDamageKind {
    error.damage_kind().unwrap_or(MetaDamageKind::Start)
}

/// Marks the runtime damaged for `kind` by `error`, publishes the empty
/// view and sheds the node's leaderships, the one way into `Damaged` (see
/// the module docs). Callers stop or drop the replica first, so no replica
/// publishes after the empty view.
fn mark_damaged(
    state: &watch::Sender<MetaRuntimeState>,
    registry: &RaftGroupHandleRegistry,
    kind: MetaDamageKind,
    error: &(dyn std::error::Error + 'static),
) {
    state.send_replace(MetaRuntimeState::Damaged {
        kind,
        message: ursula_runtime::ErrorChain(error).to_string(),
    });
    registry
        .placement_publisher()
        .publish(PlacementView::empty());
    registry.mark_leadership_shed(LeadershipShedReason::PlacementUnavailable);
}

/// Waits for a for-good refusal of the commit screen, then stops the
/// replica with its tasks and marks the runtime damaged.
async fn supervise_damage<N>(
    mut damage: watch::Receiver<Option<CommitScreenDamage>>,
    replica: Arc<Mutex<Option<MetaReplicaRestart<N>>>>,
    registry: RaftGroupHandleRegistry,
    state: watch::Sender<MetaRuntimeState>,
) where
    N: RaftNetworkFactory<C> + Clone + Send + Sync + 'static,
    N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
{
    let damaged = loop {
        if let Some(damaged) = damage.borrow().clone() {
            break damaged;
        }
        if damage.changed().await.is_err() {
            return;
        }
    };
    // No replica here: a failed plan restart already made the runtime
    // damaged.
    let Some(stopped) = replica.lock().await.take() else {
        return;
    };
    if let Err(error) = stopped.shutdown().await {
        tracing::error!(%error, "meta runtime: the damaged meta replica did not stop cleanly");
    }
    mark_damaged(
        &state,
        &registry,
        MetaDamageKind::CommitScreen,
        &damaged.refusal,
    );
}

/// The gate drivers and the reconcile shell of every meta replica the
/// runtime opens.
struct ReplicaDrivers<T> {
    node: NodeId,
    claim: GenesisClaim,
    transport: T,
    handle: MetaHandle<T>,
    recovery: RecoveryConfig,
    genesis: watch::Sender<GenesisStatus>,
    reconcile: watch::Sender<ReconcileStatus>,
}

impl<T> std::fmt::Debug for ReplicaDrivers<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaDrivers")
            .field("node", &self.node)
            .field("claim", &self.claim)
            .finish_non_exhaustive()
    }
}

impl<T: RecoveryTransport> MetaDrivers for ReplicaDrivers<T> {
    fn spawn(&self, replica: MetaDriverContext<'_>) {
        let peers = super::genesis_driver::probed_peers(&self.claim);
        let voters: BTreeSet<u64> = self.claim.meta_voters.iter().map(NodeId::get).collect();
        // The claim decides meta's seed: the genesis node initializes, and
        // every node writes its floor without asking.
        let seed = FixedRole(Some(if self.claim.is_genesis_node(self.node) {
            SeedRoleOf::Initializer
        } else {
            SeedRoleOf::Voter
        }));
        let probe = genesis_gate_probe(
            self.transport.clone(),
            self.claim.clone(),
            self.genesis.clone(),
        );
        replica.tasks.spawn_gate_drivers(
            replica.raft.clone(),
            replica.core.owner().clone(),
            Arc::clone(replica.gate),
            replica.election.clone(),
            Arc::new(FixedNodes {
                candidates: peers,
                voters,
            }),
            Arc::new(seed),
            probe,
            self.transport.clone(),
            self.recovery.clone(),
        );
        let shell = ReconcileShell {
            core: replica.core.clone(),
            replica: RaftTarget::Meta,
            me: self.node,
            source: Arc::new(MetaTargetSource {
                owner: replica.core.owner().clone(),
                watch: replica.registry.placement_watch(),
                handle: self.handle.clone(),
            }),
            gates: Arc::new(NoPeerGates),
            peers: replica.registry.peer_cluster().clone(),
            election: replica.election.clone(),
            status: self.reconcile.clone(),
        };
        replica.tasks.push(crate::rt::spawn(shell.run(
            self.recovery.interval,
            replica.registry.follower_log_reversion_counter(),
        )));
    }
}
