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
//! from the start, and a for-good refusal of the commit screen
//! ([`super::damage`]) makes it so later: the supervisor stops the replica
//! with its tasks, which clears the registry's slot, publishes the empty
//! view and keeps the meta directory, whose discard is the operator's step.
//!
//! Generic over the network and the recovery transport, so a simulation
//! runs it in process.

use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::RaftNetworkFactory;
use ursula_control::ControlError;
use ursula_control::MetaDamageKind;
use ursula_control::PlacementView;
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
use super::registration::Registration;
use super::registration::run_registration;
use super::replica::MetaDriverContext;
use super::replica::MetaDrivers;
use super::replica::MetaReplicaConfig;
use super::replica::MetaReplicaRestart;
use super::replica::MetaReplicaTasks;
use super::replica::MetaSnapshotTrigger;
use super::target::MetaTargetSource;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaRuntimeState {
    Running,
    /// No meta replica runs in this run, for `kind`.
    Damaged(MetaDamageKind),
}

/// The watches status reports read.
#[derive(Debug, Clone)]
pub struct MetaRuntimeStatus {
    pub state: watch::Receiver<MetaRuntimeState>,
    pub genesis: watch::Receiver<GenesisStatus>,
    pub registration: watch::Receiver<Registration>,
    /// Why the last `Bootstrap` this node submitted was refused.
    pub bootstrap_refusal: watch::Receiver<Option<ControlError>>,
    pub reconcile: watch::Receiver<ReconcileStatus>,
}

/// This node's meta runtime (see the module docs).
pub struct MetaRuntime<N> {
    status: MetaRuntimeStatus,
    replica: Arc<Mutex<Option<MetaReplicaRestart<N>>>>,
    registry: RaftGroupHandleRegistry,
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
        let (registration, registration_rx) = watch::channel(Registration::Pending);
        let (refusal, refusal_rx) = watch::channel(None);
        let (reconcile, reconcile_rx) =
            watch::channel(crate::rejoin::initial_status(ursula_runtime::unix_time_ms()));
        let (damage, damage_rx) = watch::channel(None);
        let mut runtime = Self {
            status: MetaRuntimeStatus {
                state: state_rx,
                genesis: genesis_rx,
                registration: registration_rx,
                bootstrap_refusal: refusal_rx,
                reconcile: reconcile_rx,
            },
            replica: Arc::new(Mutex::new(None)),
            registry: registry.clone(),
            tasks: Vec::new(),
        };
        let store = match journal.opened {
            Ok(store) => store,
            Err(error) => {
                tracing::error!(%error, "meta runtime: the meta journal did not open; this node runs no meta replica");
                state.send_replace(MetaRuntimeState::Damaged(MetaDamageKind::Journal));
                return runtime;
            }
        };
        let handle = MetaHandle::new(me, registry.clone(), transport.clone(), &config.claim);
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
                let kind = error.damage_kind().unwrap_or(MetaDamageKind::Start);
                tracing::error!(%error, ?kind, "meta runtime: the meta replica did not open; this node runs no meta replica");
                state.send_replace(MetaRuntimeState::Damaged(kind));
                return runtime;
            }
        };
        *runtime.replica.lock().await = Some(restart);
        let stopped = {
            let state = runtime.status.state.clone();
            move || matches!(*state.borrow(), MetaRuntimeState::Damaged(_))
        };
        runtime.tasks.push(crate::rt::spawn(run_registration(
            handle.clone(),
            me,
            registry.clone(),
            registration,
            stopped.clone(),
        )));
        runtime.tasks.push(crate::rt::spawn(run_formation(
            handle,
            me,
            config.claim,
            config.formation,
            registry.clone(),
            refusal,
            stopped,
        )));
        runtime.tasks.push(crate::rt::spawn(supervise_damage(
            damage_rx,
            Arc::clone(&runtime.replica),
            registry,
            state,
        )));
        runtime
    }

    /// The watches status reports read.
    pub fn status(&self) -> &MetaRuntimeStatus {
        &self.status
    }

    /// Whether this node runs a meta replica now.
    pub fn state(&self) -> MetaRuntimeState {
        *self.status.state.borrow()
    }

    /// A recovery plan on this node's meta replica
    /// ([`RaftGroupHandleRegistry::accept_meta_recovery_plan`]), which
    /// restarts the replica with its tasks when the plan cuts its log.
    pub async fn accept_recovery_plan(
        &self,
        plan: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, RecoveryGateError> {
        let mut replica = self.replica.lock().await;
        let restart = replica.as_mut().ok_or(RecoveryGateError::NotRegistered {
            raft_group_id: super::state_machine::META_GROUP,
        })?;
        self.registry.accept_meta_recovery_plan(plan, restart).await
    }

    /// Stops the node tasks, then the meta replica with its own tasks.
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

/// The recovery drivers' settings of the meta replica, those of the data
/// groups.
fn recovery_config(engine: &RaftEngineConfig) -> RecoveryConfig {
    RecoveryConfig {
        initialize: false,
        interval: crate::rejoin::REJOIN_HEAL_INTERVAL,
        barrier_timeout: crate::rejoin::RECOVERY_BARRIER_TIMEOUT,
        stall_after: crate::rejoin::RECOVERY_STALL_AFTER,
        bootstrap_interval: engine.bootstrap_peer_probe_interval,
        bootstrap_warn_after: engine.bootstrap_peer_probe,
    }
}

/// Waits for a for-good refusal of the commit screen, then stops the
/// replica with its tasks, publishes the empty view and marks the runtime
/// damaged.
async fn supervise_damage<N>(
    mut damage: watch::Receiver<Option<CommitScreenDamage>>,
    replica: Arc<Mutex<Option<MetaReplicaRestart<N>>>>,
    registry: RaftGroupHandleRegistry,
    state: watch::Sender<MetaRuntimeState>,
) where
    N: RaftNetworkFactory<C> + Clone + Send + Sync + 'static,
    N::Network: openraft::RaftNetworkV2<C, SnapshotData = SnapshotData>,
{
    loop {
        if damage.borrow().is_some() {
            break;
        }
        if damage.changed().await.is_err() {
            return;
        }
    }
    state.send_replace(MetaRuntimeState::Damaged(MetaDamageKind::CommitScreen));
    let stopped = replica.lock().await.take();
    if let Some(stopped) = stopped
        && let Err(error) = stopped.shutdown().await
    {
        tracing::error!(%error, "meta runtime: the damaged meta replica did not stop cleanly");
    }
    registry
        .placement_publisher()
        .publish(PlacementView::empty());
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
        let peers: BTreeSet<u64> = self.claim.peers.iter().map(NodeId::get).collect();
        let voters: BTreeSet<u64> = self.claim.meta_voters.iter().map(NodeId::get).collect();
        let mut recovery = self.recovery.clone();
        // Only the smallest claimed meta voter initializes the meta group.
        recovery.initialize = self.claim.meta_voters.min() == self.node;
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
            peers,
            voters,
            probe,
            self.transport.clone(),
            recovery,
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
