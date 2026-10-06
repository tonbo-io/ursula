use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::BasicNode;
use openraft::Config;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::ReadPolicy;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::rt::WatchReceiver;
use openraft::storage::EntryResponder;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use serde::Deserialize;
use serde::Serialize;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlProjection;
use ursula_control::ControlResponse;
use ursula_control::MembershipLogId;
use ursula_control::MetaLocalIdentity;
use ursula_control::NodeId;
use ursula_shard::RaftGroupId;

use crate::log_store::MetaRaftFileLogStore;
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

#[derive(Debug, thiserror::Error)]
#[error("{operation}{}{message}", if .message.is_empty() { "" } else { ": " })]
pub struct MetaRaftError {
    operation: &'static str,
    message: String,
    #[source]
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl MetaRaftError {
    pub fn new(operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            operation,
            message: message.into(),
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
            source: Some(Box::new(source)),
        }
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }
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
    local_identity: Option<MetaLocalIdentity>,
}

impl MetaRaftHandle {
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

    /// Recover one durable meta replica. Initialization remains an explicit,
    /// one-time bootstrap operation; reopening never creates a new membership.
    pub async fn new_durable_node_with_network<NF>(
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        journal_path: impl Into<PathBuf>,
    ) -> Result<Self, MetaRaftError>
    where
        NF: RaftNetworkFactory<MetaRaftTypeConfig>,
    {
        let path = journal_path.into();
        let (store, state_machine) = crate::log_store::spawn_log_store_blocking(None, move || {
            let store = MetaRaftFileLogStore::open(path)?;
            let state_machine = MetaRaftStateMachine::open_durable(store.clone())?;
            Ok((store, state_machine))
        })
        .await
        .map_err(|err| MetaRaftError::with_source("recover durable meta storage", err))?;
        Self::new_node_with_state_machine(node_id, config, network_factory, store, state_machine)
            .await
    }

    /// Managed-mode startup: reject storage/node/cluster/routing drift before
    /// constructing Raft or accepting any consensus traffic.
    pub async fn new_bound_durable_node_with_network<NF>(
        identity: MetaLocalIdentity,
        config: Arc<Config>,
        network_factory: NF,
        journal_path: impl Into<PathBuf>,
    ) -> Result<Self, MetaRaftError>
    where
        NF: RaftNetworkFactory<MetaRaftTypeConfig>,
    {
        let path = journal_path.into();
        let node_id = identity.node.node_id;
        let (store, state_machine) = crate::log_store::spawn_log_store_blocking(None, move || {
            let store = MetaRaftFileLogStore::open_bound(path, identity)?;
            let state_machine = MetaRaftStateMachine::open_durable(store.clone())?;
            Ok((store, state_machine))
        })
        .await
        .map_err(|error| MetaRaftError::with_source("recover bound durable meta storage", error))?;
        Self::new_node_with_state_machine(node_id, config, network_factory, store, state_machine)
            .await
    }

    pub fn local_identity(&self) -> Option<&MetaLocalIdentity> {
        self.local_identity.as_ref()
    }

    async fn new_node_with_state_machine<NF, LS>(
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
        let local_identity = state_machine
            .durable_store
            .as_ref()
            .and_then(|store| store.identity())
            .cloned();
        if let (Some(identity), Some(bootstrap)) =
            (&local_identity, &state_machine.state.cluster_bootstrap)
            && identity.cluster != bootstrap.recipe.identity
        {
            return Err(MetaRaftError::new(
                "recover meta bootstrap identity",
                "control snapshot differs from local storage identity",
            ));
        }
        let raft = MetaRaft::new(node_id, config, network_factory, log_store, state_machine)
            .await
            .map_err(|err| MetaRaftError::with_source("create meta OpenRaft group", err))?;

        Ok(Self {
            raft,
            local_identity,
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
        if let (Some(identity), ControlCommand::BootstrapCluster { bootstrap, .. }) =
            (&self.local_identity, &command)
            && identity.cluster != bootstrap.identity
        {
            return Err(MetaRaftError::new(
                "write meta bootstrap",
                "bootstrap differs from bound cluster/routing identity",
            ));
        }
        self.raft
            .client_write(command)
            .await
            .map(|response| response.data)
            .map_err(|err| MetaRaftError::with_source("write meta OpenRaft command", err))
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

    /// Adopt the declared settled data groups only after observing each one's
    /// actual applied membership through a fresh data-quorum barrier. A replay
    /// of an established recipe never re-reads or resets later live placements.
    pub async fn bootstrap_cluster_from_quorums(
        &self,
        bootstrap: ursula_control::ClusterBootstrap,
        timeout: Duration,
        now_ms: u64,
    ) -> Result<ControlResponse, MetaRaftError> {
        let bootstrap = bootstrap
            .normalize()
            .map_err(|reason| MetaRaftError::new("validate managed bootstrap", reason))?;
        let local = self.local_identity.as_ref().ok_or_else(|| {
            MetaRaftError::new(
                "bootstrap managed cluster",
                "durable local identity required",
            )
        })?;
        if local.cluster != bootstrap.identity {
            return Err(MetaRaftError::new(
                "bootstrap managed cluster",
                "routing identity differs from local binding",
            ));
        }
        let replay = self
            .read_state(|state| {
                state
                    .cluster_bootstrap
                    .as_ref()
                    .map(|record| record.recipe.clone())
            })
            .await?;
        let memberships = match replay {
            Some(recipe) if recipe == bootstrap => BTreeMap::new(),
            Some(_) => {
                return Err(MetaRaftError::new(
                    "bootstrap managed cluster",
                    "bootstrap recipe drift",
                ));
            }
            None => crate::membership::collect_bootstrap_memberships(&bootstrap, timeout)
                .await
                .map_err(|reason| {
                    MetaRaftError::new("collect bootstrap data memberships", reason)
                })?,
        };
        self.write(ControlCommand::BootstrapCluster {
            bootstrap,
            memberships,
            now_ms,
        })
        .await
    }

    /// A fresh ReadIndex, followed by the complete applied state. Intended for
    /// startup/projection refresh, never an ordinary stream request's hot path.
    pub async fn read_projection(
        &self,
        timeout: Duration,
    ) -> Result<ControlProjection, MetaRaftError> {
        let identity = self
            .local_identity
            .as_ref()
            .ok_or_else(|| {
                MetaRaftError::new(
                    "read control projection",
                    "durable local identity is required",
                )
            })?
            .cluster
            .clone();
        if timeout.is_zero() {
            return Err(MetaRaftError::new(
                "read control projection",
                "timeout must be non-zero",
            ));
        }
        crate::rt::time::timeout(timeout, async {
            let before = self.raft.metrics().borrow_watched().clone();
            let linearizer = self
                .raft
                .get_read_linearizer(ReadPolicy::ReadIndex)
                .await
                .map_err(|error| {
                    MetaRaftError::with_source("confirm meta projection quorum", error)
                })?;
            let read_index = linearizer.read_log_id().index();
            linearizer
                .try_await_ready(&self.raft, Some(timeout))
                .await
                .map_err(|error| {
                    MetaRaftError::with_source("await meta projection application", error)
                })?
                .map_err(|error| {
                    MetaRaftError::new("await meta projection application", format!("{error:?}"))
                })?;
            let projection = self
                .with_state_machine(move |machine| {
                    let result = machine.applied_log_id().map(|id| ControlProjection {
                        identity,
                        applied_log_id: MembershipLogId {
                            term: id.committed_leader_id().term(),
                            node_id: *id.committed_leader_id().node_id(),
                            index: id.index(),
                        },
                        state: machine.state().clone(),
                    });
                    Box::pin(async move { result })
                })
                .await?
                .ok_or_else(|| {
                    MetaRaftError::new("read control projection", "no applied meta log")
                })?;
            let after = self.raft.metrics().borrow_watched().clone();
            if before.current_leader != Some(before.id)
                || after.current_leader != Some(after.id)
                || before.vote != after.vote
                || !after.vote.is_committed()
                || projection.applied_log_id.index < read_index
            {
                return Err(MetaRaftError::new(
                    "read control projection",
                    "leadership changed during projection read",
                ));
            }
            projection
                .validate()
                .map_err(|reason| MetaRaftError::new("read control projection", reason))?;
            Ok(projection)
        })
        .await
        .map_err(|error| MetaRaftError::with_source("read control projection deadline", error))?
    }

    pub async fn shutdown(&self) -> Result<(), MetaRaftError> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| MetaRaftError::with_source("shutdown meta OpenRaft group", err))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetaRaftStateMachine {
    state: ControlPlaneState,
    last_response: Option<ControlResponse>,
    last_applied_log_id: Option<LogIdOf<MetaRaftTypeConfig>>,
    last_membership: StoredMembershipOf<MetaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<MetaCurrentSnapshot>>>,
    durable_store: Option<Arc<MetaRaftFileLogStore>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetaCurrentSnapshot {
    pub(crate) meta: SnapshotMetaOf<MetaRaftTypeConfig>,
    #[serde(with = "serde_bytes")]
    pub(crate) bytes: Vec<u8>,
}

impl MetaRaftStateMachine {
    pub fn open_durable(store: Arc<MetaRaftFileLogStore>) -> io::Result<Self> {
        let snapshot = store.snapshot()?;
        let mut machine = Self::default();
        if let Some(snapshot) = &snapshot {
            if snapshot.meta.last_membership.log_id() > &snapshot.meta.last_log_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "meta snapshot membership exceeds its applied log",
                ));
            }
            machine.state = serde_json::from_slice(&snapshot.bytes).map_err(invalid_snapshot)?;
            machine.last_applied_log_id = snapshot.meta.last_log_id;
            machine.last_membership = snapshot.meta.last_membership.clone();
        }
        machine.current_snapshot = Arc::new(Mutex::new(snapshot));
        machine.durable_store = Some(store);
        Ok(machine)
    }

    pub fn state(&self) -> &ControlPlaneState {
        &self.state
    }

    pub fn applied_log_id(&self) -> Option<LogIdOf<MetaRaftTypeConfig>> {
        self.last_applied_log_id
    }

    pub fn last_response(&self) -> Option<&ControlResponse> {
        self.last_response.as_ref()
    }

    fn apply_control_command(&mut self, command: ControlCommand) -> ControlResponse {
        if let ControlCommand::BootstrapCluster { bootstrap, .. } = &command {
            let bootstrap = match bootstrap.clone().normalize() {
                Ok(bootstrap) => bootstrap,
                Err(reason) => return ControlResponse::Rejected { reason },
            };
            // Bound peers must share the routing contract: RPC preambles reject
            // a different binding before Raft. This also guards raw in-process
            // callers that bypass MetaRaftHandle::write.
            if self
                .durable_store
                .as_ref()
                .and_then(|store| store.identity())
                .is_some_and(|identity| identity.cluster != bootstrap.identity)
            {
                return ControlResponse::Rejected {
                    reason: "bootstrap differs from bound cluster/routing identity".to_owned(),
                };
            }
            if self.state.cluster_bootstrap.is_none() {
                let membership = self.last_membership.membership();
                if membership.get_joint_config().as_slice()
                    != [bootstrap.initial_meta_voters.clone()]
                {
                    return ControlResponse::Rejected { reason: "bootstrap meta voters differ from the committed uniform meta membership".to_owned() };
                }
                for voter in &bootstrap.initial_meta_voters {
                    let matches = bootstrap.nodes.get(voter).is_some_and(|node| {
                        membership.get_node(voter).is_some_and(|actual| {
                            crate::grpc::normalize_grpc_endpoint(actual.addr.clone())
                                == node.cluster_url.trim_end_matches('/')
                        })
                    });
                    if !matches {
                        return ControlResponse::Rejected {
                            reason: format!(
                                "bootstrap meta voter {voter} endpoint differs from its committed membership"
                            ),
                        };
                    }
                }
            }
        }
        self.state.apply(command)
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
                EntryPayload::Normal(command) => self.apply_control_command(command),
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembershipOf::<MetaRaftTypeConfig>::new(
                        Some(entry.log_id),
                        membership,
                    );
                    ControlResponse::Ok
                }
            };
            self.last_response = Some(response.clone());
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
            durable_store: self.durable_store.clone(),
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
        let bytes = snapshot.into_inner();
        if meta.last_log_id < self.last_applied_log_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stale meta snapshot",
            ));
        }
        if meta.last_membership.log_id() > &meta.last_log_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "meta snapshot membership exceeds its applied log",
            ));
        }
        let state: ControlPlaneState = serde_json::from_slice(&bytes).map_err(invalid_snapshot)?;
        if let Some(identity) = self
            .durable_store
            .as_ref()
            .and_then(|store| store.identity())
            && state
                .cluster_bootstrap
                .as_ref()
                .is_some_and(|bootstrap| bootstrap.recipe.identity != identity.cluster)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incoming control snapshot differs from local cluster/routing identity",
            ));
        }
        let current = MetaCurrentSnapshot {
            meta: meta.clone(),
            bytes,
        };
        if let Some(store) = &self.durable_store {
            store.persist_snapshot(current.clone()).await?;
        }
        self.state = state;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(current);
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
    state: ControlPlaneState,
    meta: SnapshotMetaOf<MetaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<MetaCurrentSnapshot>>>,
    durable_store: Option<Arc<MetaRaftFileLogStore>>,
}

impl RaftSnapshotBuilder<MetaRaftTypeConfig> for MetaRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<MetaRaftTypeConfig>, io::Error> {
        let bytes = serde_json::to_vec(&self.state).map_err(invalid_snapshot)?;
        let snapshot = MetaCurrentSnapshot {
            meta: self.meta.clone(),
            bytes: bytes.clone(),
        };
        if let Some(store) = &self.durable_store {
            store.persist_snapshot(snapshot.clone()).await?;
        }
        let mut current = self.current_snapshot.lock().expect("snapshot mutex");
        if current
            .as_ref()
            .is_none_or(|old| old.meta.last_log_id <= snapshot.meta.last_log_id)
        {
            *current = Some(snapshot);
        }
        Ok(SnapshotOf::<MetaRaftTypeConfig> {
            meta: self.meta.clone(),
            snapshot: Cursor::new(bytes),
        })
    }
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
