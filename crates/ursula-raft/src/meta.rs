use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::future::Shared;
use openraft::BasicNode;
use openraft::Config;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::alias::VoteOf;
use openraft::rt::WatchReceiver;
use openraft::storage::EntryResponder;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;
use ursula_control::NodeId;
use ursula_shard::RaftGroupId;

use crate::registry::SingleNodeRaftNetworkFactory;

#[cfg(madsim)]
type MetaOpenRaftRuntime = crate::sim_runtime::MadsimOpenRaftRuntime;
#[cfg(not(madsim))]
type MetaOpenRaftRuntime = openraft::impls::TokioRuntime;

openraft::declare_raft_types!(
    pub MetaRaftTypeConfig:
        D = ControlCommand,
        R = ControlResponse,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = MetaOpenRaftRuntime,
);

pub type MetaRaft = Raft<MetaRaftTypeConfig, MetaRaftStateMachine>;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{operation}{}{message}", if .message.is_empty() { "" } else { ": " })]
pub struct MetaRaftError {
    operation: &'static str,
    message: String,
    redirect: MetaRedirect,
    #[source]
    source: Option<Arc<dyn Error + Send + Sync + 'static>>,
}

#[derive(Debug, Clone)]
pub(crate) enum MetaRedirect {
    None,
    NotLeader(Option<String>),
}

impl MetaRaftError {
    pub fn new(operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            operation,
            message: message.into(),
            redirect: MetaRedirect::None,
            source: None,
        }
    }

    pub fn with_source(
        operation: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            operation,
            message: source.to_string(),
            redirect: MetaRedirect::None,
            source: Some(Arc::new(source)),
        }
    }

    pub(crate) fn not_leader(endpoint: Option<String>) -> Self {
        let mut error = Self::new("meta write", "not the current leader");
        error.redirect = MetaRedirect::NotLeader(endpoint);
        error
    }

    pub(crate) fn redirect(&self) -> &MetaRedirect {
        &self.redirect
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }
}

fn meta_replica_caught_up(
    metrics: &openraft::RaftMetrics<MetaRaftTypeConfig>,
    source: u64,
) -> bool {
    if source == metrics.id {
        return true;
    }
    metrics
        .replication
        .as_ref()
        .and_then(|progress| progress.get(&source))
        .copied()
        .flatten()
        .is_some_and(|matched| metrics.last_applied.is_some_and(|prefix| matched >= prefix))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaNodeRegistration {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub labels: BTreeMap<String, String>,
}

impl MetaNodeRegistration {
    pub fn new(
        node_id: NodeId,
        client_url: impl Into<String>,
        cluster_url: impl Into<String>,
    ) -> Self {
        Self {
            node_id,
            client_url: client_url.into(),
            cluster_url: cluster_url.into(),
            labels: BTreeMap::new(),
        }
    }

    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    pub fn into_command(self, now_ms: u64) -> ControlCommand {
        ControlCommand::RegisterNode {
            node_id: self.node_id,
            client_url: self.client_url,
            cluster_url: self.cluster_url,
            labels: self.labels,
            now_ms,
        }
    }
}

#[derive(Clone)]
pub struct MetaRaftHandle {
    raft: MetaRaft,
    process_identity: Arc<Mutex<Option<ursula_control::ProcessIdentity>>>,
    committed: watch::Receiver<ControlPlaneState>,
    read_rounds: Arc<Mutex<MetaReadRounds<ControlPlaneState>>>,
    local_read_rounds: Arc<Mutex<MetaReadRounds<ControlPlaneState>>>,
    process_rounds: Arc<Mutex<MetaReadRounds<ProcessEpochs>>>,
    local_process_rounds: Arc<Mutex<MetaReadRounds<ProcessEpochs>>>,
    meta_actions: Arc<crate::rt::sync::Mutex<()>>,
    membership_tasks: Arc<Mutex<MetaMembershipTasks>>,
    snapshot_serial: Arc<crate::rt::sync::Mutex<()>>,
    durable_store: Option<Arc<crate::MetaDiskLogStore>>,
    recovery_nonce: Option<ursula_proto::admin::ProcessIncarnation>,
    replication_enabled: Arc<AtomicBool>,
}

pub(crate) type ProcessEpochs = BTreeMap<u64, ursula_control::ProcessState>;
type MetaReadRound<T> = Shared<BoxFuture<'static, Result<Arc<T>, MetaRaftError>>>;
#[derive(Default)]
struct MetaReadRounds<T> {
    latest: Option<MetaReadRound<T>>,
    open: bool,
}

type MetaMembershipTask = Shared<BoxFuture<'static, Result<(), MetaRaftError>>>;
#[derive(Default)]
struct MetaMembershipTasks {
    closed: bool,
    tasks: Vec<MetaMembershipTask>,
}

impl std::fmt::Debug for MetaRaftHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaRaftHandle").finish_non_exhaustive()
    }
}

impl MetaRaftHandle {
    pub async fn new_durable(
        node_id: u64,
        root: PathBuf,
        config: Arc<Config>,
    ) -> Result<Self, MetaRaftError> {
        let store = crate::MetaDiskLogStore::open(root.clone())
            .await
            .map_err(|error| MetaRaftError::with_source("open meta WAL", error))?;
        let mut machine = MetaRaftStateMachine {
            snapshot_path: Some(root.join("meta-snapshot.msgpack")),
            ..Default::default()
        };
        let path = machine.snapshot_path.clone().expect("snapshot path set");
        let restored = crate::meta_disk::run_io(move || match std::fs::read(path) {
            Ok(bytes) => crate::meta_disk::decode_record::<MetaCurrentSnapshot>(&bytes).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .await
        .map_err(|error| MetaRaftError::with_source("restore meta snapshot", error))?;
        let purged = store
            .clone()
            .get_log_state()
            .await
            .map_err(|error| MetaRaftError::with_source("read meta purge boundary", error))?
            .last_purged_log_id;
        let snapshot_boundary = restored
            .as_ref()
            .and_then(|snapshot| snapshot.meta.last_log_id);
        if purged
            .is_some_and(|required| snapshot_boundary.is_none_or(|available| available < required))
        {
            return Err(MetaRaftError::new(
                "restore meta snapshot",
                "snapshot does not cover the durable purged prefix",
            ));
        }
        if let Some(snapshot) = restored {
            machine.state = serde_json::from_slice(&snapshot.bytes)
                .map_err(|error| MetaRaftError::with_source("decode meta snapshot", error))?;
            machine.last_applied_log_id = snapshot.meta.last_log_id;
            machine.last_membership = snapshot.meta.last_membership.clone();
            machine.committed.send_replace(machine.state.clone());
            *machine.current_snapshot.lock().expect("meta snapshot lock") = Some(snapshot);
        }
        let enable_elect = config.enable_elect;
        let mut quiet_config = (*config).clone();
        quiet_config.enable_elect = false;
        let network = crate::MetaGrpcNetworkFactory::default();
        let process_identity = network.identity.clone();
        let mut handle = Self::new_node_with_state_machine(
            node_id,
            Arc::new(quiet_config),
            network,
            store.clone(),
            machine,
        )
        .await?;
        handle.durable_store = Some(store);
        handle.process_identity = process_identity;
        let restored_identity = match handle.committed.borrow().operations.processes.get(&node_id) {
            Some(ursula_control::ProcessState::Active(identity)) => Some(identity.clone()),
            _ => None,
        };
        if let Some(identity) = restored_identity {
            handle.set_process_identity(identity)?;
        }
        handle.raft.runtime_config().elect(enable_elect);
        Ok(handle)
    }

    /// Fresh durable replicas cannot participate until startup establishes a
    /// survivor vote floor or a nonce-bound all-empty genesis authorization.
    pub async fn new_durable_recovering(
        node_id: u64,
        root: PathBuf,
        config: Arc<Config>,
        nonce: ursula_proto::admin::ProcessIncarnation,
    ) -> Result<Self, MetaRaftError> {
        let mut config = (*config).clone();
        config.enable_elect = false;
        let mut handle = Self::new_durable(node_id, root, Arc::new(config)).await?;
        handle.recovery_nonce = Some(nonce);
        let initialized =
            handle.raft.is_initialized().await.map_err(|error| {
                MetaRaftError::with_source("meta recovery initialization", error)
            })?;
        handle
            .replication_enabled
            .store(initialized, Ordering::Release);
        handle.raft.runtime_config().elect(initialized);
        Ok(handle)
    }

    /// Pin the identity admitted for this boot. Only startup claim completion may
    /// replace the replayed identity; never follow a remote process-state watch.
    pub fn set_process_identity(
        &self,
        identity: ursula_control::ProcessIdentity,
    ) -> Result<(), MetaRaftError> {
        *self
            .process_identity
            .lock()
            .map_err(|_poisoned| MetaRaftError::new("meta sender identity", "lock poisoned"))? =
            Some(identity);
        Ok(())
    }

    pub fn replication_enabled(&self) -> bool {
        self.replication_enabled.load(Ordering::Acquire)
    }

    pub async fn recovery_status(
        &self,
    ) -> Result<crate::meta_transport::MetaRecoveryStatus, MetaRaftError> {
        let vote =
            match &self.durable_store {
                Some(store) => store.clone().read_vote().await.map_err(|error| {
                    MetaRaftError::with_source("meta durable recovery vote", error)
                })?,
                None => None,
            };
        Ok(crate::meta_transport::MetaRecoveryStatus {
            initialized: self.raft.is_initialized().await.map_err(|error| {
                MetaRaftError::with_source("meta recovery initialization", error)
            })?,
            vote,
            nonce: self.recovery_nonce.clone(),
        })
    }

    pub async fn authorize_genesis(
        &self,
        nonce: &ursula_proto::admin::ProcessIncarnation,
    ) -> Result<(), MetaRaftError> {
        if self.recovery_nonce.as_ref() != Some(nonce) {
            return Err(MetaRaftError::new(
                "meta genesis authorization",
                "receiver process changed",
            ));
        }
        let status = self.recovery_status().await?;
        if status.initialized || status.vote.is_some() {
            return Err(MetaRaftError::new(
                "meta genesis authorization",
                "receiver has prior consensus state",
            ));
        }
        self.replication_enabled.store(true, Ordering::Release);
        self.raft.runtime_config().elect(true);
        Ok(())
    }

    pub async fn install_recovery_vote_floor(
        &self,
        vote: VoteOf<MetaRaftTypeConfig>,
    ) -> Result<(), MetaRaftError> {
        if self.replication_enabled() {
            return Ok(());
        }
        self.raft
            .vote(openraft::raft::VoteRequest {
                vote,
                last_log_id: None,
            })
            .await
            .map_err(|error| {
                MetaRaftError::with_source("persist meta recovery vote floor", error)
            })?;
        let durable = self.recovery_status().await?.vote;
        if durable.is_none_or(|stored| stored < vote) {
            return Err(MetaRaftError::new(
                "meta recovery vote floor",
                "vote was not durably accepted",
            ));
        }
        self.replication_enabled.store(true, Ordering::Release);
        // A recovered learner waits for its own committed process claim before
        // campaigning, even while it replays historical voter configurations.
        Ok(())
    }

    pub async fn new_node_with_log_store_and_network<NF, LS>(
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
    ) -> Result<Self, MetaRaftError>
    where
        NF: RaftNetworkFactory<MetaRaftTypeConfig>,
        LS: RaftLogStorage<MetaRaftTypeConfig>,
    {
        Self::new_node_with_state_machine(
            node_id,
            config,
            network_factory,
            log_store,
            MetaRaftStateMachine::default(),
        )
        .await
    }

    pub(crate) async fn new_node_with_state_machine<NF, LS>(
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
        state_machine: MetaRaftStateMachine,
    ) -> Result<Self, MetaRaftError>
    where
        NF: RaftNetworkFactory<MetaRaftTypeConfig>,
        LS: RaftLogStorage<MetaRaftTypeConfig>,
    {
        let committed = state_machine.committed.subscribe();
        let snapshot_serial = state_machine.snapshot_serial.clone();
        let raft = MetaRaft::new(node_id, config, network_factory, log_store, state_machine)
            .await
            .map_err(|err| MetaRaftError::with_source("create meta OpenRaft group", err))?;

        Ok(Self {
            raft,
            process_identity: Arc::default(),
            committed,
            read_rounds: Arc::default(),
            local_read_rounds: Arc::default(),
            process_rounds: Arc::default(),
            local_process_rounds: Arc::default(),
            meta_actions: Arc::default(),
            membership_tasks: Arc::default(),
            snapshot_serial,
            durable_store: None,
            recovery_nonce: None,
            replication_enabled: Arc::new(AtomicBool::new(true)),
        })
    }

    pub async fn new_single_node_with_log_store<LS>(
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
    ) -> Result<Self, MetaRaftError>
    where
        LS: RaftLogStorage<MetaRaftTypeConfig>,
    {
        let handle = Self::new_node_with_log_store_and_network(
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store,
        )
        .await?;

        let mut nodes = BTreeMap::new();
        nodes.insert(node_id, node);
        handle.initialize_membership(nodes).await?;
        handle
            .wait_for_current_leader(node_id, Duration::from_secs(2))
            .await?;

        Ok(handle)
    }

    pub async fn initialize_membership(
        &self,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<(), MetaRaftError> {
        let initialized =
            self.raft.is_initialized().await.map_err(|err| {
                MetaRaftError::with_source("check meta OpenRaft initialization", err)
            })?;
        if initialized {
            return Ok(());
        }

        self.raft
            .initialize(nodes)
            .await
            .map_err(|err| MetaRaftError::with_source("initialize meta OpenRaft group", err))
    }

    pub async fn wait_for_current_leader(
        &self,
        node_id: u64,
        timeout: Duration,
    ) -> Result<(), MetaRaftError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(
                node_id,
                "meta OpenRaft group should observe expected leader",
            )
            .await
            .map(|_| ())
            .map_err(|err| MetaRaftError::with_source("wait for meta OpenRaft leadership", err))
    }

    pub fn raft_handle(&self) -> MetaRaft {
        self.raft.clone()
    }

    pub async fn write(&self, command: ControlCommand) -> Result<ControlResponse, MetaRaftError> {
        // Only a typed pre-proposal leader rejection is retried. Transport
        // failures may hide a committed command and retain outcome uncertainty.
        crate::rt::time::timeout(Duration::from_secs(15), async {
            let mut endpoint = self.remote_leader_endpoint();
            let mut metrics = self.raft.metrics();
            loop {
                let result = if let Some(target) = &endpoint {
                    crate::meta_transport::forward_write(target, command.clone()).await
                } else {
                    self.write_local(command.clone()).await
                };
                match result {
                    Err(error) => match error.redirect() {
                        MetaRedirect::NotLeader(Some(target)) => endpoint = Some(target.clone()),
                        MetaRedirect::NotLeader(None) => {
                            metrics.changed().await.map_err(|error| {
                                MetaRaftError::with_source("wait for meta leader", error)
                            })?;
                            endpoint = self.remote_leader_endpoint();
                        }
                        MetaRedirect::None => return Err(error),
                    },
                    Ok(response) => return Ok(response),
                }
            }
        })
        .await
        .map_err(|error| MetaRaftError::with_source("wait for meta write leader", error))?
    }

    pub(crate) async fn write_local(
        &self,
        command: ControlCommand,
    ) -> Result<ControlResponse, MetaRaftError> {
        let _action = self.meta_actions.lock().await;
        self.raft
            .client_write(command)
            .await
            .map(|response| response.data)
            .map_err(|err| {
                let redirect = err
                    .forward_to_leader()
                    .map(|forward| forward.leader_node.as_ref().map(|node| node.addr.clone()));
                let mut error = MetaRaftError::with_source("write meta OpenRaft command", err);
                if let Some(endpoint) = redirect {
                    error.redirect = MetaRedirect::NotLeader(endpoint);
                }
                error
            })
    }

    pub async fn reconcile_operation_membership(
        &self,
        token: ursula_control::OperationToken,
        restore: bool,
    ) -> Result<(), MetaRaftError> {
        if let Some(endpoint) = self.remote_leader_endpoint() {
            return crate::meta_transport::forward_membership(&endpoint, token, restore).await;
        }
        self.reconcile_operation_membership_local(token, restore)
            .await
    }

    pub(crate) async fn reconcile_operation_membership_local(
        &self,
        token: ursula_control::OperationToken,
        restore: bool,
    ) -> Result<(), MetaRaftError> {
        let task = {
            let mut tasks = self
                .membership_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if tasks.closed {
                return Err(MetaRaftError::new(
                    "meta membership",
                    "authority is shutting down",
                ));
            }
            tasks
                .tasks
                .retain(|task| task.clone().now_or_never().is_none());
            let handle = self.clone();
            // The mutation stays owned after caller cancellation. Catch-up is a
            // bounded observation outside the mutation gate, so an unreachable
            // learner cannot indefinitely block unrelated control-plane writes.
            let task = crate::rt::spawn(async move {
                let operation = handle.checked_membership_operation(&token, restore).await?;
                let (source, rebuild) = match operation.kind {
                    ursula_control::OperationKind::RebuildReplica { node_id } => (node_id, true),
                    ursula_control::OperationKind::DecommissionNode { node_id, .. } => {
                        (node_id, false)
                    }
                    ursula_control::OperationKind::MoveReplicas { .. } => {
                        return Err(MetaRaftError::new(
                            "meta membership",
                            "move does not change meta voters",
                        ));
                    }
                };
                let mut desired = operation.meta_voters;
                if !restore {
                    desired.remove(&source);
                }
                if desired.is_empty() {
                    return Err(MetaRaftError::new(
                        "meta membership",
                        "cannot remove last meta voter",
                    ));
                }
                if restore {
                    let ready = handle
                        .raft
                        .wait(Some(Duration::from_secs(30)))
                        .metrics(
                            move |metrics| {
                                metrics.current_leader != Some(metrics.id)
                                    || meta_replica_caught_up(metrics, source)
                            },
                            "rebuilt meta learner catches up before promotion",
                        )
                        .await
                        .map_err(|error| {
                            MetaRaftError::with_source("wait for rebuilt meta learner", error)
                        })?;
                    if ready.current_leader != Some(ready.id) {
                        return Err(MetaRaftError::new(
                            "meta rebuild",
                            "leadership changed while waiting for learner",
                        ));
                    }
                }
                let _action = handle.meta_actions.lock().await;
                handle.checked_membership_operation(&token, restore).await?;
                let metrics = handle.raft.metrics().borrow_watched().clone();
                if metrics.membership_config.membership().get_joint_config()
                    == &vec![desired.clone()]
                {
                    return Ok(());
                }
                if restore
                    && (!meta_replica_caught_up(&metrics, source)
                        || metrics
                            .membership_config
                            .membership()
                            .get_node(&source)
                            .is_none())
                {
                    return Err(MetaRaftError::new(
                        "meta rebuild",
                        "retained learner is not caught up with current committed prefix",
                    ));
                }
                handle
                    .raft
                    .change_membership(desired, rebuild && !restore)
                    .await
                    .map_err(|error| {
                        MetaRaftError::with_source("change operation meta membership", error)
                    })?;
                Ok(())
            })
            .map(|joined| {
                joined.map_err(|error| {
                    MetaRaftError::with_source("join meta membership task", error)
                })?
            })
            .boxed()
            .shared();
            tasks.tasks.push(task.clone());
            task
        };
        task.await
    }

    async fn checked_membership_operation(
        &self,
        token: &ursula_control::OperationToken,
        restore: bool,
    ) -> Result<ursula_control::MaintenanceOperation, MetaRaftError> {
        let state = self.read_linearizable_local().await?;
        let operation = state
            .operations
            .active
            .as_ref()
            .filter(|operation| &operation.token == token)
            .ok_or_else(|| MetaRaftError::new("meta membership", "operation token changed"))?;
        if restore {
            let ursula_control::OperationKind::RebuildReplica { node_id } = operation.kind else {
                return Err(MetaRaftError::new(
                    "meta membership",
                    "only rebuild restores voters",
                ));
            };
            let replaced = match (
                state.operations.processes.get(&node_id),
                operation.participants.get(&node_id),
            ) {
                (Some(ursula_control::ProcessState::Active(current)), Some(previous)) => {
                    current == previous
                }
                _ => false,
            };
            if operation.phase != ursula_control::OperationPhase::Retired || !replaced {
                return Err(MetaRaftError::new(
                    "meta rebuild",
                    "replacement process has not claimed its new epoch",
                ));
            }
        } else if operation.phase != ursula_control::OperationPhase::Preparing {
            return Err(MetaRaftError::new(
                "meta membership",
                "source retirement phase changed",
            ));
        }
        Ok(operation.clone())
    }

    pub async fn register_node(
        &self,
        registration: MetaNodeRegistration,
        now_ms: u64,
    ) -> Result<ControlResponse, MetaRaftError> {
        self.write(registration.into_command(now_ms)).await
    }

    pub async fn begin_migration(
        &self,
        raft_group_id: RaftGroupId,
        target_voters: BTreeSet<u64>,
        retain_removed: bool,
        now_ms: u64,
    ) -> Result<ControlResponse, MetaRaftError> {
        self.write(ControlCommand::BeginMigration {
            raft_group_id,
            target_voters,
            retain_removed,
            now_ms,
        })
        .await
    }

    pub async fn commit_placement(
        &self,
        raft_group_id: RaftGroupId,
        voters: BTreeSet<u64>,
        learners: BTreeSet<u64>,
        draining: BTreeSet<u64>,
        now_ms: u64,
    ) -> Result<ControlResponse, MetaRaftError> {
        self.write(ControlCommand::CommitPlacement {
            raft_group_id,
            voters,
            learners,
            draining,
            now_ms,
        })
        .await
    }

    pub async fn finish_migration(
        &self,
        migration_id: u64,
        success: bool,
        now_ms: u64,
    ) -> Result<ControlResponse, MetaRaftError> {
        self.write(ControlCommand::FinishMigration {
            migration_id,
            success,
            now_ms,
        })
        .await
    }

    pub async fn register_initial_data_nodes(
        &self,
        registrations: impl IntoIterator<Item = MetaNodeRegistration>,
        now_ms: u64,
    ) -> Result<(), MetaRaftError> {
        for registration in registrations {
            let node_id = registration.node_id;
            match self.register_node(registration, now_ms).await? {
                ControlResponse::Ok => {}
                ControlResponse::Rejected { reason } => {
                    return Err(MetaRaftError::new(
                        "register initial data-capable node",
                        format!("node {node_id} rejected: {reason}"),
                    ));
                }
                response => {
                    return Err(MetaRaftError::new(
                        "register initial data-capable node",
                        format!("node {node_id} returned unexpected response {response}"),
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn with_state_machine<V>(
        &self,
        f: impl FnOnce(&mut MetaRaftStateMachine) -> openraft::base::BoxFuture<V>
        + OptionalSend
        + 'static,
    ) -> Result<V, MetaRaftError>
    where
        V: OptionalSend + 'static,
    {
        self.raft
            .with_state_machine(f)
            .await
            .map_err(|err| MetaRaftError::with_source("access meta OpenRaft state machine", err))
    }

    pub async fn read_state<V>(
        &self,
        f: impl FnOnce(&ControlPlaneState) -> V + OptionalSend + 'static,
    ) -> Result<V, MetaRaftError>
    where
        V: OptionalSend + 'static,
    {
        self.with_state_machine(move |state_machine| {
            let value = f(state_machine.state());
            Box::pin(async move { value })
        })
        .await
    }

    /// The latest locally applied committed control-plane state.
    pub fn committed_state(&self) -> watch::Receiver<ControlPlaneState> {
        self.committed.clone()
    }

    fn remote_leader_endpoint(&self) -> Option<String> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let leader = metrics.current_leader?;
        if leader == metrics.id {
            return None;
        }
        metrics
            .membership_config
            .membership()
            .get_node(&leader)
            .map(|node| node.addr.clone())
    }

    /// Confirm a quorum read at the leader; followers forward and return the
    /// leader's applied state rather than their potentially stale local view.
    pub async fn read_linearizable_state(&self) -> Result<ControlPlaneState, MetaRaftError> {
        self.read_round(true).await.map(|state| (*state).clone())
    }

    pub(crate) async fn read_linearizable_local(&self) -> Result<ControlPlaneState, MetaRaftError> {
        self.read_round(false).await.map(|state| (*state).clone())
    }

    fn read_round(&self, forward: bool) -> MetaReadRound<ControlPlaneState> {
        let queue = if forward {
            &self.read_rounds
        } else {
            &self.local_read_rounds
        };
        let mut rounds = queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if rounds.open
            && let Some(round) = &rounds.latest
        {
            return round.clone();
        }
        let previous = rounds.latest.take();
        let weak = Arc::downgrade(queue);
        let raft = self.raft.clone();
        let endpoint = if forward {
            self.remote_leader_endpoint()
        } else {
            None
        };
        let round = async move {
            if let Some(previous) = previous {
                let _previous = previous.await;
            }
            if let Some(rounds) = weak.upgrade() {
                rounds
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .open = false;
            }
            if let Some(endpoint) = endpoint {
                return crate::meta_transport::forward_read(&endpoint)
                    .await
                    .map(Arc::new);
            }
            raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await
                .map_err(|error| MetaRaftError::with_source("meta read barrier", error))?;
            raft.with_state_machine(|machine: &mut MetaRaftStateMachine| {
                let state = Arc::new(machine.state.clone());
                Box::pin(async move { state })
            })
            .await
            .map_err(|error| MetaRaftError::with_source("read confirmed meta state", error))
        }
        .boxed()
        .shared();
        rounds.latest = Some(round.clone());
        rounds.open = true;
        round
    }

    /// Fresh process authority only: no placements are copied or sent on data RPCs.
    pub(crate) async fn read_linearizable_processes(
        &self,
    ) -> Result<Arc<ProcessEpochs>, MetaRaftError> {
        self.process_round(true).await
    }

    pub(crate) async fn read_linearizable_processes_local(
        &self,
    ) -> Result<Arc<ProcessEpochs>, MetaRaftError> {
        self.process_round(false).await
    }

    fn process_round(&self, forward: bool) -> MetaReadRound<ProcessEpochs> {
        let queue = if forward {
            &self.process_rounds
        } else {
            &self.local_process_rounds
        };
        let mut rounds = queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if rounds.open
            && let Some(round) = &rounds.latest
        {
            return round.clone();
        }
        let previous = rounds.latest.take();
        let weak = Arc::downgrade(queue);
        let raft = self.raft.clone();
        let endpoint = if forward {
            self.remote_leader_endpoint()
        } else {
            None
        };
        let round = async move {
            if let Some(previous) = previous {
                let _previous = previous.await;
            }
            if let Some(rounds) = weak.upgrade() {
                rounds
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .open = false;
            }
            if let Some(endpoint) = endpoint {
                return crate::meta_transport::forward_processes(&endpoint)
                    .await
                    .map(Arc::new);
            }
            raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await
                .map_err(|error| MetaRaftError::with_source("meta process barrier", error))?;
            raft.with_state_machine(|machine: &mut MetaRaftStateMachine| {
                let processes = Arc::new(machine.state.operations.processes.clone());
                Box::pin(async move { processes })
            })
            .await
            .map_err(|error| MetaRaftError::with_source("read confirmed process epochs", error))
        }
        .boxed()
        .shared();
        rounds.latest = Some(round.clone());
        rounds.open = true;
        round
    }

    /// Capture placement state and applied metadata membership at one state-machine boundary.
    pub async fn read_linearizable_topology(
        &self,
    ) -> Result<(ControlPlaneState, BTreeSet<u64>), MetaRaftError> {
        if let Some(endpoint) = self.remote_leader_endpoint() {
            return crate::meta_transport::forward_topology(&endpoint).await;
        }
        self.read_linearizable_topology_local().await
    }

    pub(crate) async fn read_linearizable_topology_local(
        &self,
    ) -> Result<(ControlPlaneState, BTreeSet<u64>), MetaRaftError> {
        self.raft
            .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await
            .map_err(|error| MetaRaftError::with_source("meta topology barrier", error))?;
        self.raft
            .with_state_machine(|machine: &mut MetaRaftStateMachine| {
                let state = machine.state.clone();
                let voters = machine.last_membership.membership().voter_ids().collect();
                Box::pin(async move { (state, voters) })
            })
            .await
            .map_err(|error| MetaRaftError::with_source("read confirmed topology", error))
    }

    pub async fn shutdown(&self) -> Result<(), MetaRaftError> {
        let tasks = {
            let mut tracker = self
                .membership_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            tracker.closed = true;
            std::mem::take(&mut tracker.tasks)
        };
        // Stop Raft before draining owned actions: a quorum wait must not keep
        // the storage owner alive forever after ingress has stopped.
        let stopped = self
            .raft
            .shutdown()
            .await
            .map_err(|err| MetaRaftError::with_source("shutdown meta OpenRaft group", err));
        for task in tasks {
            if let Err(error) = task.await {
                tracing::debug!(%error, "meta membership task ended during shutdown");
            }
        }
        // A canceled snapshot future may still own a spawn_blocking publication.
        // Join that serialized publication before the durable store can drop.
        let _snapshot_publication = self.snapshot_serial.lock().await;
        stopped
    }
}

#[derive(Debug, Clone)]
pub struct MetaRaftStateMachine {
    pub(crate) committed: watch::Sender<ControlPlaneState>,
    snapshot_path: Option<PathBuf>,
    snapshot_serial: Arc<crate::rt::sync::Mutex<()>>,
    state: ControlPlaneState,
    last_response: Option<ControlResponse>,
    last_applied_log_id: Option<LogIdOf<MetaRaftTypeConfig>>,
    last_membership: StoredMembershipOf<MetaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<MetaCurrentSnapshot>>>,
}

impl Default for MetaRaftStateMachine {
    fn default() -> Self {
        Self {
            state: ControlPlaneState::default(),
            snapshot_path: None,
            snapshot_serial: Arc::default(),
            committed: watch::channel(ControlPlaneState::default()).0,
            last_response: None,
            last_applied_log_id: None,
            last_membership: StoredMembershipOf::<MetaRaftTypeConfig>::default(),
            current_snapshot: Arc::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaCurrentSnapshot {
    meta: SnapshotMetaOf<MetaRaftTypeConfig>,
    bytes: Vec<u8>,
}

impl MetaRaftStateMachine {
    pub fn state(&self) -> &ControlPlaneState {
        &self.state
    }

    pub fn applied_log_id(&self) -> Option<LogIdOf<MetaRaftTypeConfig>> {
        self.last_applied_log_id
    }

    pub fn last_response(&self) -> Option<&ControlResponse> {
        self.last_response.as_ref()
    }

    fn snapshot_meta(&self) -> SnapshotMetaOf<MetaRaftTypeConfig> {
        SnapshotMetaOf::<MetaRaftTypeConfig> {
            last_log_id: self.last_applied_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: self
                .last_applied_log_id
                .map(|log_id| format!("meta-{}-{}", log_id.committed_leader_id(), log_id.index()))
                .unwrap_or_else(|| "meta-empty".to_owned()),
        }
    }
}

impl RaftStateMachine<MetaRaftTypeConfig> for MetaRaftStateMachine {
    type SnapshotBuilder = MetaRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<MetaRaftTypeConfig>>,
            StoredMembershipOf<MetaRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied_log_id, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<MetaRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend {
        while let Some((entry, responder)) = entries.try_next().await? {
            self.last_applied_log_id = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => ControlResponse::Ok,
                EntryPayload::Normal(command) => self.state.apply(command),
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembershipOf::<MetaRaftTypeConfig>::new(
                        Some(entry.log_id),
                        membership,
                    );
                    ControlResponse::Ok
                }
            };
            self.last_response = Some(response.clone());
            self.committed.send_replace(self.state.clone());
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaRaftSnapshotBuilder {
            state: self.state.clone(),
            meta: self.snapshot_meta(),
            current_snapshot: self.current_snapshot.clone(),
            snapshot_path: self.snapshot_path.clone(),
            snapshot_serial: self.snapshot_serial.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<MetaRaftTypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<MetaRaftTypeConfig>,
        snapshot: SnapshotDataOf<MetaRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        if meta.last_log_id < self.last_applied_log_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "meta snapshot precedes applied state",
            ));
        }
        let bytes = snapshot.into_inner();
        let state = serde_json::from_slice(&bytes).map_err(invalid_snapshot)?;
        persist_meta_snapshot(
            &self.current_snapshot,
            &self.snapshot_serial,
            self.snapshot_path.clone(),
            MetaCurrentSnapshot {
                meta: meta.clone(),
                bytes,
            },
        )
        .await?;
        self.state = state;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.committed.send_replace(self.state.clone());
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self
            .current_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .map(|snapshot| SnapshotOf::<MetaRaftTypeConfig> {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.bytes.clone()),
            }))
    }
}

#[derive(Debug, Clone)]
pub struct MetaRaftSnapshotBuilder {
    snapshot_path: Option<PathBuf>,
    snapshot_serial: Arc<crate::rt::sync::Mutex<()>>,
    state: ControlPlaneState,
    meta: SnapshotMetaOf<MetaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<MetaCurrentSnapshot>>>,
}

impl RaftSnapshotBuilder<MetaRaftTypeConfig> for MetaRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<MetaRaftTypeConfig>, io::Error> {
        let bytes = serde_json::to_vec(&self.state).map_err(invalid_snapshot)?;
        let chosen = persist_meta_snapshot(
            &self.current_snapshot,
            &self.snapshot_serial,
            self.snapshot_path.clone(),
            MetaCurrentSnapshot {
                meta: self.meta.clone(),
                bytes,
            },
        )
        .await?;
        Ok(SnapshotOf::<MetaRaftTypeConfig> {
            meta: chosen.meta,
            snapshot: Cursor::new(chosen.bytes),
        })
    }
}

async fn persist_meta_snapshot(
    current: &Arc<Mutex<Option<MetaCurrentSnapshot>>>,
    serial: &Arc<crate::rt::sync::Mutex<()>>,
    path: Option<PathBuf>,
    candidate: MetaCurrentSnapshot,
) -> Result<MetaCurrentSnapshot, io::Error> {
    let serial = serial.clone().lock_owned().await;
    let current = current.clone();
    crate::meta_disk::run_io(move || {
        let _serial = serial;
        if let Some(existing) = current.lock().expect("meta snapshot lock").as_ref()
            && existing.meta.last_log_id > candidate.meta.last_log_id
        {
            return Ok(existing.clone());
        }
        if let Some(path) = path {
            let encoded = crate::meta_disk::encode_record(&candidate)?;
            crate::meta_disk::atomic_write(&path, &encoded)?;
        }
        *current.lock().expect("meta snapshot lock") = Some(candidate.clone());
        Ok(candidate)
    })
    .await
}

fn invalid_snapshot(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use futures_util::stream;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::RaftTypeConfig;
    use openraft::entry::RaftEntry;
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::storage::RaftStateMachine;
    use openraft::vote::RaftLeaderId;
    use ursula_control::ControlCommand;
    use ursula_control::ControlResponse;

    use super::*;

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn durable_sender_identity_is_fixed_until_this_boot_claims_and_restored_on_restart() {
        let root = tempfile::tempdir().unwrap();
        let handle =
            MetaRaftHandle::new_durable(1, root.path().to_owned(), Arc::new(Config::default()))
                .await
                .unwrap();
        handle
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("http://unused"))]))
            .await
            .unwrap();
        handle
            .wait_for_current_leader(1, Duration::from_secs(3))
            .await
            .unwrap();
        handle
            .register_node(
                MetaNodeRegistration::new(1, "http://local", "http://local"),
                0,
            )
            .await
            .unwrap();
        let mut claimed = Vec::new();
        for epoch in 0..2 {
            let response = handle
                .write(ControlCommand::Operation {
                    command: ursula_control::OperationCommand::ClaimProcess {
                        node_id: 1,
                        expected_epoch: epoch,
                        incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(
                            u128::from(epoch) + 1,
                        ),
                    },
                    now_ms: epoch,
                })
                .await
                .unwrap();
            let ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ProcessClaimed(
                identity,
            ))) = response
            else {
                panic!("claim rejected")
            };
            if epoch == 0 {
                handle.set_process_identity(identity.clone()).unwrap();
            }
            claimed.push(identity);
        }
        assert_eq!(
            *handle.process_identity.lock().unwrap(),
            Some(claimed[0].clone()),
            "committed remote claim cannot update zombie sender identity"
        );
        handle.shutdown().await.unwrap();
        let restarted = MetaRaftHandle::new_durable_recovering(
            1,
            root.path().to_owned(),
            Arc::new(Config::default()),
            ursula_proto::admin::ProcessIncarnation::from_bits(3),
        )
        .await
        .unwrap();
        assert_eq!(
            *restarted.process_identity.lock().unwrap(),
            Some(claimed[1].clone()),
            "same-PV restart captures the committed replay identity once"
        );
        restarted.shutdown().await.unwrap();
    }

    type LeaderId = <MetaRaftTypeConfig as RaftTypeConfig>::LeaderId;

    fn log_id(index: u64) -> LogId<LeaderId> {
        LogId {
            leader_id: LeaderId::new(1, 1),
            index,
        }
    }

    fn entry(index: u64, command: ControlCommand) -> <MetaRaftTypeConfig as RaftTypeConfig>::Entry {
        <MetaRaftTypeConfig as RaftTypeConfig>::Entry::new(
            log_id(index),
            EntryPayload::Normal(command),
        )
    }

    fn set(values: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
        values.into_iter().collect()
    }

    fn membership_entry(
        index: u64,
        voters: impl IntoIterator<Item = u64>,
    ) -> <MetaRaftTypeConfig as RaftTypeConfig>::Entry {
        let voters = set(voters);
        let membership =
            openraft::Membership::new_with_defaults(vec![voters.clone()], voters.clone());
        <MetaRaftTypeConfig as RaftTypeConfig>::Entry::new(
            log_id(index),
            EntryPayload::Membership(membership),
        )
    }

    #[tokio::test]
    async fn meta_state_machine_applies_control_commands() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 5,
                    client_url: "http://node5:4491/".to_owned(),
                    cluster_url: "http://node5:4492/".to_owned(),
                    labels: BTreeMap::from([("az".to_owned(), "a".to_owned())]),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let node = machine.state().nodes.get(&5).expect("node registered");
        assert_eq!(node.client_url, "http://node5:4491");
        assert_eq!(node.cluster_url, "http://node5:4492");
        assert_eq!(node.labels.get("az").map(String::as_str), Some("a"));
        assert_eq!(machine.applied_log_id(), Some(log_id(1)));
    }

    #[tokio::test]
    async fn meta_state_machine_records_rejected_command_responses() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 5,
                    client_url: "   ".to_owned(),
                    cluster_url: "http://node5:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply rejected register node");

        assert!(!machine.state().nodes.contains_key(&5));
        assert_eq!(
            machine.last_response(),
            Some(&ControlResponse::Rejected {
                reason: "client_url must not be empty".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn meta_state_machine_records_membership_logs() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((membership_entry(1, [1, 2, 3]), None))]))
            .await
            .expect("apply membership");

        let (applied, membership) = machine.applied_state().await.expect("applied state");
        assert_eq!(applied, Some(log_id(1)));
        assert_eq!(membership.log_id(), &Some(log_id(1)));
        assert_eq!(
            membership.voter_ids().collect::<BTreeSet<_>>(),
            set([1, 2, 3])
        );
        assert_eq!(machine.last_response(), Some(&ControlResponse::Ok));
    }

    #[tokio::test]
    async fn meta_snapshot_builder_round_trips_control_state() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut restored = MetaRaftStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        assert!(restored.state().nodes.contains_key(&7));
        assert_eq!(restored.applied_log_id(), Some(log_id(1)));
    }

    #[tokio::test]
    async fn meta_snapshot_builder_updates_current_snapshot() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("build snapshot");
        let current = machine
            .get_current_snapshot()
            .await
            .expect("current snapshot")
            .expect("snapshot present");

        assert_eq!(current.meta.last_log_id, Some(log_id(1)));
        let decoded: ursula_control::ControlPlaneState =
            serde_json::from_slice(current.snapshot.into_inner().as_slice())
                .expect("decode current snapshot");
        assert!(decoded.nodes.contains_key(&7));
    }

    #[tokio::test]
    async fn meta_state_machine_reports_current_snapshot_after_install() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut restored = MetaRaftStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");
        let current = restored
            .get_current_snapshot()
            .await
            .expect("current snapshot")
            .expect("snapshot present");

        assert_eq!(current.meta.last_log_id, Some(log_id(1)));
        let decoded: ursula_control::ControlPlaneState =
            serde_json::from_slice(current.snapshot.into_inner().as_slice())
                .expect("decode current snapshot");
        assert!(decoded.nodes.contains_key(&7));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn canceled_meta_snapshot_retains_serialization_until_publication() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.msgpack");
        let current = Arc::new(Mutex::new(None));
        let serial = Arc::new(crate::rt::sync::Mutex::new(()));
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking({
            let current = current.clone();
            move || {
                let _guard = current.lock().unwrap();
                entered.send(()).unwrap();
                blocked.recv().unwrap();
            }
        });
        started.await.unwrap();
        let candidate = MetaCurrentSnapshot {
            meta: SnapshotMetaOf::<MetaRaftTypeConfig> {
                last_log_id: Some(log_id(1)),
                last_membership: StoredMembershipOf::<MetaRaftTypeConfig>::default(),
                snapshot_id: "first".to_owned(),
            },
            bytes: vec![1],
        };
        let first = tokio::spawn({
            let current = current.clone();
            let serial = serial.clone();
            let path = path.clone();
            async move { persist_meta_snapshot(&current, &serial, Some(path), candidate).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while serial.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(
            serial.try_lock().is_err(),
            "cancellation must not release snapshot serialization"
        );
        release.send(()).unwrap();
        blocker.await.unwrap();
        let candidate = MetaCurrentSnapshot {
            meta: SnapshotMetaOf::<MetaRaftTypeConfig> {
                last_log_id: Some(log_id(2)),
                last_membership: StoredMembershipOf::<MetaRaftTypeConfig>::default(),
                snapshot_id: "second".to_owned(),
            },
            bytes: vec![2],
        };
        persist_meta_snapshot(&current, &serial, Some(path.clone()), candidate)
            .await
            .unwrap();
        let disk: MetaCurrentSnapshot =
            crate::meta_disk::decode_record(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(disk.meta.last_log_id, Some(log_id(2)));
        assert_eq!(
            current.lock().unwrap().as_ref().unwrap().meta.last_log_id,
            disk.meta.last_log_id
        );
    }

    #[tokio::test]
    async fn meta_snapshot_cannot_regress_applied_state_or_watch() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(2, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7".to_owned(),
                    cluster_url: "http://node7".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .unwrap();
        let mut empty = MetaRaftStateMachine::default();
        let mut builder = empty.get_snapshot_builder().await;
        let stale = builder.build_snapshot().await.unwrap();
        let error = machine
            .install_snapshot(&stale.meta, stale.snapshot)
            .await
            .expect_err("stale snapshot rejected before changing state");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(machine.last_applied_log_id, Some(log_id(2)));
        assert!(machine.committed.borrow().nodes.contains_key(&7));
    }

    #[test]
    fn public_meta_types_are_exported_from_crate_root() {
        fn assert_types(
            _machine: crate::MetaRaftStateMachine,
            _builder: Option<crate::MetaRaftSnapshotBuilder>,
        ) {
        }

        assert_types(crate::MetaRaftStateMachine::default(), None);
        let _type_name = std::any::type_name::<crate::MetaRaftTypeConfig>();
    }
}

#[cfg(all(test, not(madsim)))]
mod owned_membership_tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_drains_membership_work_after_its_caller_is_cancelled() {
        let config = Arc::new(
            Config {
                cluster_name: "owned-meta-membership".to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let handle = MetaRaftHandle::new_single_node_with_log_store(
            1,
            BasicNode::new("local"),
            config,
            crate::log_store::MetaTestLogStore::shared(),
        )
        .await
        .unwrap();
        let identity = ursula_control::ProcessIdentity {
            epoch: 1,
            incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
        };
        handle
            .write(ControlCommand::RegisterNode {
                node_id: 1,
                client_url: "http://one".to_owned(),
                cluster_url: "http://one".to_owned(),
                labels: BTreeMap::new(),
                now_ms: 0,
            })
            .await
            .unwrap();
        handle
            .write(ControlCommand::SeedPlacement {
                raft_group_id: ursula_shard::RaftGroupId(0),
                voters: BTreeSet::from([1]),
                now_ms: 0,
            })
            .await
            .unwrap();
        handle
            .write(ControlCommand::Operation {
                command: ursula_control::OperationCommand::ClaimProcess {
                    node_id: 1,
                    expected_epoch: 0,
                    incarnation: identity.incarnation.clone(),
                },
                now_ms: 0,
            })
            .await
            .unwrap();
        let result = handle
            .write(ControlCommand::Operation {
                command: ursula_control::OperationCommand::Begin {
                    kind: ursula_control::OperationKind::RebuildReplica { node_id: 1 },
                    executor: ursula_proto::admin::ProcessIncarnation::from_bits(10),
                    participants: BTreeMap::from([(1, identity)]),
                    meta_voters: BTreeSet::from([1, 2, 3]),
                },
                now_ms: 0,
            })
            .await
            .unwrap();
        let ControlResponse::Operation(Ok(ursula_control::OperationOutcome::Acquired(token))) =
            result
        else {
            panic!("operation acquisition");
        };
        let gate = handle.meta_actions.lock().await;
        let caller_handle = handle.clone();
        let caller_token = token.clone();
        let caller = tokio::spawn(async move {
            caller_handle
                .reconcile_operation_membership_local(caller_token, false)
                .await
        });
        while handle.membership_tasks.lock().unwrap().tasks.is_empty() {
            tokio::task::yield_now().await;
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let stopping_handle = handle.clone();
        let stopping = tokio::spawn(async move { stopping_handle.shutdown().await });
        while !handle.membership_tasks.lock().unwrap().closed {
            tokio::task::yield_now().await;
        }
        assert!(!stopping.is_finished());
        drop(gate);
        tokio::time::timeout(Duration::from_secs(2), stopping)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let rejected = handle
            .reconcile_operation_membership_local(token, false)
            .await
            .unwrap_err();
        assert_eq!(rejected.operation(), "meta membership");
        assert!(handle.membership_tasks.lock().unwrap().tasks.is_empty());
    }
}
