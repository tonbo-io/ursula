use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogStorage;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use ursula_runtime::{
    AppendBatchRequest, AppendExternalRequest, AppendRequest, BootstrapStreamRequest,
    CloseStreamRequest, ColdStoreHandle, ColdWriteAdmission, CreateStreamExternalRequest,
    CreateStreamRequest, DeleteSnapshotRequest, DeleteStreamRequest, FlushColdRequest,
    GroupAppendBatchFuture, GroupAppendFuture, GroupBootstrapStreamFuture, GroupCloseStreamFuture,
    GroupColdHotBacklogFuture, GroupCreateStreamFuture, GroupDeleteSnapshotFuture,
    GroupDeleteStreamFuture, GroupEngine, GroupEngineCreateFuture, GroupEngineError,
    GroupEngineFactory, GroupEngineMetrics, GroupFlushColdFuture, GroupForkRefFuture,
    GroupHeadStreamFuture, GroupInstallSnapshotFuture, GroupPlanColdFlushFuture,
    GroupPlanNextColdFlushBatchFuture, GroupPlanNextColdFlushFuture, GroupPublishSnapshotFuture,
    GroupReadSnapshotFuture, GroupReadStreamFuture, GroupReadStreamParts,
    GroupReadStreamPartsFuture, GroupSnapshot, GroupSnapshotFuture, GroupTouchStreamAccessFuture,
    GroupWriteBatchFuture, GroupWriteCommand, GroupWriteResponse, HeadStreamRequest,
    PlanColdFlushRequest, PlanGroupColdFlushRequest, PublishSnapshotRequest, ReadSnapshotRequest,
    ReadStreamRequest, StreamErrorCode, TouchStreamAccessResponse,
};
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::CoreId;
use ursula_shard::ShardPlacement;

use crate::codec::*;
use crate::forward::*;
use crate::grpc::GrpcRaftNetworkFactory;
use crate::log_store::*;
use crate::log_store::{RaftGroupFileLogStore, RaftGroupLogStore};
use crate::registry::RaftGroupHandleRegistry;
use crate::registry::*;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::*;

pub struct RaftGroupEngine {
    pub(crate) raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    pub(crate) placement: ShardPlacement,
    pub(crate) metrics: Option<GroupEngineMetrics>,
    pub(crate) cold_store: Option<ColdStoreHandle>,
}

impl RaftGroupEngine {
    pub async fn new_single_node(placement: ShardPlacement) -> Result<Self, GroupEngineError> {
        Self::new_single_node_with_optional_metrics(placement, None).await
    }

    pub(crate) async fn new_single_node_with_optional_metrics(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
    ) -> Result<Self, GroupEngineError> {
        let config = Arc::new(
            Config {
                cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
        );
        Self::new_single_node_with_config_and_metrics(
            placement,
            1,
            BasicNode::new("local"),
            config,
            metrics,
        )
        .await
    }

    pub async fn new_single_node_with_file_log(
        placement: ShardPlacement,
        log_path: impl Into<PathBuf>,
    ) -> Result<Self, GroupEngineError> {
        let config = Arc::new(
            Config {
                cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
        );
        let log_store = RaftGroupFileLogStore::shared(log_path)
            .map_err(|err| GroupEngineError::new(format!("open OpenRaft file log: {err}")))?;
        Self::new_single_node_with_log_store(
            placement,
            1,
            BasicNode::new("local"),
            config,
            log_store,
        )
        .await
    }

    pub async fn new_single_node_with_config(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
    ) -> Result<Self, GroupEngineError> {
        Self::new_single_node_with_config_and_metrics(placement, node_id, node, config, None).await
    }

    pub(crate) async fn new_single_node_with_config_and_metrics(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        metrics: Option<GroupEngineMetrics>,
    ) -> Result<Self, GroupEngineError> {
        Self::new_single_node_with_log_store_and_metrics(
            placement,
            node_id,
            node,
            config,
            RaftGroupLogStore::shared(),
            metrics,
            None,
        )
        .await
    }

    pub async fn new_single_node_with_log_store<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        Self::new_single_node_with_log_store_and_metrics(
            placement, node_id, node, config, log_store, None, None,
        )
        .await
    }

    pub(crate) async fn new_single_node_with_log_store_and_metrics<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let engine = Self::new_node_with_log_store_and_network(
            placement,
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store,
            metrics,
            cold_store,
        )
        .await?;

        let initialized = engine.raft.is_initialized().await.map_err(|err| {
            GroupEngineError::new(format!("check OpenRaft initialization: {err}"))
        })?;
        if !initialized {
            let mut nodes = BTreeMap::new();
            nodes.insert(node_id, node);
            engine.raft.initialize(nodes).await.map_err(|err| {
                GroupEngineError::new(format!("initialize OpenRaft group: {err}"))
            })?;
        }
        engine
            .raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(node_id, "single-node OpenRaft group should elect itself")
            .await
            .map_err(|err| GroupEngineError::new(format!("wait for OpenRaft leadership: {err}")))?;

        Ok(engine)
    }

    pub async fn new_node_with_log_store_and_network<NF, LS>(
        placement: ShardPlacement,
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
    ) -> Result<Self, GroupEngineError>
    where
        NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
            node_id,
            config,
            network_factory,
            log_store,
            RaftGroupStateMachine::new_with_metrics_and_cold_store(
                placement,
                metrics.clone(),
                cold_store.clone(),
            ),
        )
        .await
        .map_err(|err| GroupEngineError::new(format!("create OpenRaft group: {err}")))?;

        Ok(Self {
            raft,
            placement,
            metrics,
            cold_store,
        })
    }

    pub async fn initialize_membership(
        &self,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<(), GroupEngineError> {
        let initialized = self.raft.is_initialized().await.map_err(|err| {
            GroupEngineError::new(format!("check OpenRaft initialization: {err}"))
        })?;
        if initialized {
            return Ok(());
        }
        self.raft
            .initialize(nodes)
            .await
            .map_err(|err| GroupEngineError::new(format!("initialize OpenRaft group: {err}")))
    }

    pub async fn wait_for_current_leader(
        &self,
        node_id: u64,
        timeout: Duration,
    ) -> Result<(), GroupEngineError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(node_id, "OpenRaft group should observe expected leader")
            .await
            .map(|_| ())
            .map_err(|err| GroupEngineError::new(format!("wait for OpenRaft leadership: {err}")))
    }

    pub fn raft_handle(&self) -> Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine> {
        self.raft.clone()
    }

    pub async fn shutdown(&self) -> Result<(), GroupEngineError> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| GroupEngineError::new(format!("shutdown OpenRaft group: {err}")))
    }

    pub(crate) async fn write(
        &self,
        command: GroupWriteCommand,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        let response = match self.raft.client_write(command.into()).await {
            Ok(response) => response,
            Err(err) => return Err(group_engine_client_write_error(err)),
        };
        group_write_result_from_raft_response(response.data)?
    }

    pub(crate) async fn write_commands(
        &self,
        commands: Vec<GroupWriteCommand>,
    ) -> Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, GroupEngineError> {
        write_commands_on_raft(
            self.raft.clone(),
            self.placement,
            self.metrics.clone(),
            commands,
        )
        .await
    }

    pub(crate) async fn forward_write_to_leader_if_follower(
        &self,
        _command: GroupWriteCommand,
    ) -> Result<Option<GroupWriteResponse>, GroupEngineError> {
        if self.raft.is_leader() {
            return Ok(None);
        }
        let leader_id = self.raft.current_leader().await;
        let leader_node = self.current_leader_node().await;
        Err(group_engine_forward_to_leader_error(
            "OpenRaft group write has to run on the local leader runtime",
            leader_id,
            leader_node.as_ref(),
        ))
    }

    pub(crate) async fn with_state_machine<V>(
        &self,
        f: impl FnOnce(&mut RaftGroupStateMachine) -> openraft::base::BoxFuture<V>
        + OptionalSend
        + 'static,
    ) -> Result<V, GroupEngineError>
    where
        V: OptionalSend + 'static,
    {
        self.raft
            .with_state_machine(f)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft state-machine access: {err}")))
    }

    pub(crate) async fn access_requires_write(
        &self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, GroupEngineError> {
        self.with_state_machine(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .access_requires_write(&stream_id, now_ms, renew_ttl)
                    .await
            })
        })
        .await?
    }

    pub(crate) async fn ensure_stream_access(
        &self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<Option<TouchStreamAccessResponse>, GroupEngineError> {
        if !self
            .access_requires_write(stream_id.clone(), now_ms, renew_ttl)
            .await?
        {
            return Ok(None);
        }
        let response = match self
            .write(GroupWriteCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms,
                renew_ttl,
            })
            .await?
        {
            GroupWriteResponse::TouchStreamAccess(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
                )));
            }
        };
        if response.expired {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        Ok(Some(response))
    }

    pub(crate) async fn require_local_leader_for_read(
        &self,
        operation: &str,
    ) -> Result<(), GroupEngineError> {
        if self.raft.is_leader() {
            return Ok(());
        }
        Err(group_engine_forward_to_leader_error(
            format!("OpenRaft {operation} has to forward request to leader"),
            self.raft.current_leader().await,
            None,
        ))
    }

    pub(crate) async fn current_leader_node(&self) -> Option<BasicNode> {
        let leader_id = self.raft.current_leader().await?;
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .get_node(&leader_id)
            .cloned()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RaftGroupEngineFactory;

impl GroupEngineFactory for RaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_optional_metrics(placement, Some(metrics))
                    .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredRaftGroupEngineFactory {
    registry: RaftGroupHandleRegistry,
}

impl RegisteredRaftGroupEngineFactory {
    pub fn new(registry: RaftGroupHandleRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }
}

impl GroupEngineFactory for RegisteredRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine =
                RaftGroupEngine::new_single_node_with_optional_metrics(placement, Some(metrics))
                    .await?;
            self.registry.register(placement, engine.raft.clone());
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColdRaftGroupEngineFactory {
    cold_store: ColdStoreHandle,
}

impl ColdRaftGroupEngineFactory {
    pub fn new(cold_store: ColdStoreHandle) -> Self {
        Self { cold_store }
    }
}

impl GroupEngineFactory for ColdRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 10,
                    election_timeout_min: 30,
                    election_timeout_max: 60,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_log_store_and_metrics(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    Some(self.cold_store.clone()),
                )
                .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftLogStoreFactory {
    root: PathBuf,
    core_writers: Arc<Mutex<BTreeMap<u16, Arc<CoreFileLogWriter>>>>,
}

impl DurableRaftLogStoreFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            core_writers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn log_path(&self, placement: ShardPlacement) -> PathBuf {
        self.root
            .join(format!("core-{}", placement.core_id.0))
            .join(format!("group-{}.json", placement.raft_group_id.0))
    }

    pub(crate) fn core_journal_path(&self, core_id: CoreId) -> PathBuf {
        self.root
            .join(format!("core-{}", core_id.0))
            .join("journal.bin")
    }

    pub(crate) fn core_writer(
        &self,
        core_id: CoreId,
    ) -> Result<Arc<CoreFileLogWriter>, GroupEngineError> {
        let mut writers = self
            .core_writers
            .lock()
            .map_err(|_| GroupEngineError::new("core file log writer mutex poisoned"))?;
        if let Some(writer) = writers.get(&core_id.0) {
            return Ok(writer.clone());
        }

        let writer = CoreFileLogWriter::shared(self.core_journal_path(core_id))
            .map_err(|err| GroupEngineError::new(format!("open OpenRaft core journal: {err}")))?;
        writers.insert(core_id.0, writer.clone());
        Ok(writer)
    }

    pub fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, GroupEngineError> {
        RaftGroupFileLogStore::shared_with_core_writer(
            self.log_path(placement),
            placement,
            metrics,
            self.core_writer(placement.core_id)?,
        )
        .map_err(|err| GroupEngineError::new(format!("open OpenRaft file log: {err}")))
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftGroupEngineFactory {
    log_stores: DurableRaftLogStoreFactory,
    cold_store: Option<ColdStoreHandle>,
}

impl DurableRaftGroupEngineFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            log_stores: DurableRaftLogStoreFactory::new(root),
            cold_store: None,
        }
    }

    pub fn with_cold_store(root: impl Into<PathBuf>, cold_store: Option<ColdStoreHandle>) -> Self {
        Self {
            log_stores: DurableRaftLogStoreFactory::new(root),
            cold_store,
        }
    }
}

impl GroupEngineFactory for DurableRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 10,
                    election_timeout_min: 30,
                    election_timeout_max: 60,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let log_store = self.log_stores.open(placement, metrics.clone())?;
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_log_store_and_metrics(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    log_store,
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?,
            );
            Ok(engine)
        })
    }
}

impl GroupEngine for RaftGroupEngine {
    fn accepts_local_writes(&self) -> bool {
        self.raft.is_leader()
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return self.create_stream(request, placement);
        }
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::CreateStream(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create stream write response: {other:?}"
                    ))),
                };
            }
            self.with_state_machine({
                let request = request.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_create_stream_cold_admission(request, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader()
                && let Some(leader_node) = self.current_leader_node().await
            {
                return forward_head_stream_to_leader(placement, &leader_node, request).await;
            }
            self.require_local_leader_for_read("head_stream").await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.head_stream(request, placement).await })
            })
            .await?
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            self.read_stream_parts(request, placement)
                .await?
                .into_response()
                .await
        })
    }

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader()
                && let Some(leader_node) = self.current_leader_node().await
            {
                let response =
                    forward_read_stream_to_leader(placement, &leader_node, request).await?;
                return Ok(GroupReadStreamParts::from_response(response));
            }
            let stream_id = request.stream_id.clone();
            self.require_local_leader_for_read("read_stream").await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            let plan = self
                .with_state_machine(move |state_machine| {
                    Box::pin(
                        async move { state_machine.engine.read_stream_plan_after_access(&request) },
                    )
                })
                .await??;
            Ok(GroupReadStreamParts::from_plan(
                placement,
                stream_id,
                plan,
                self.cold_store.clone(),
            ))
        })
    }

    fn require_local_live_read_owner<'a>(
        &'a mut self,
        _placement: ShardPlacement,
    ) -> ursula_runtime::GroupRequireLiveReadOwnerFuture<'a> {
        Box::pin(async move { self.require_local_leader_for_read("live_read").await })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::PublishSnapshot(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected publish snapshot write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::PublishSnapshot(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected publish snapshot write response: {other:?}"
                ))),
            }
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            self.require_local_leader_for_read("read_snapshot").await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.read_snapshot(request, placement).await })
            })
            .await?
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.delete_snapshot(request, placement).await })
            })
            .await?
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            self.require_local_leader_for_read("bootstrap_stream")
                .await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.bootstrap_stream(request, placement).await })
            })
            .await?
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        _placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::TouchStreamAccess {
                    stream_id,
                    now_ms,
                    renew_ttl,
                })
                .await?
            {
                GroupWriteResponse::TouchStreamAccess(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
                ))),
            }
        })
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::AddForkRef { stream_id, now_ms })
                .await?
            {
                GroupWriteResponse::AddForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected add fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::ReleaseForkRef { stream_id })
                .await?
            {
                GroupWriteResponse::ReleaseForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected release fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.plan_cold_flush(request, placement).await })
            })
            .await?
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(
                    async move { state_machine.plan_next_cold_flush(request, placement).await },
                )
            })
            .await?
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .plan_next_cold_flush_batch(request, placement, max_candidates)
                        .await
                })
            })
            .await?
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.cold_hot_backlog(stream_id, placement).await })
            })
            .await?
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::CloseStream(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected close stream write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CloseStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected close stream write response: {other:?}"
                ))),
            }
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::DeleteStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected delete stream write response: {other:?}"
                ))),
            }
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected external append write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return self.append(request, placement);
        }
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine({
                let request = request.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_append_cold_admission(request, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::AppendBatch(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            let mut responses = self
                .write_commands(vec![GroupWriteCommand::from(request)])
                .await?;
            let response = responses.pop().ok_or_else(|| {
                GroupEngineError::new("OpenRaft append batch returned no response")
            })?;
            match response? {
                GroupWriteResponse::AppendBatch(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return self.append_batch(request, placement);
        }
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::AppendBatch(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine({
                let request = request.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_append_batch_cold_admission(request, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            let mut responses = self
                .write_commands(vec![GroupWriteCommand::from(request)])
                .await?;
            let response = responses.pop().ok_or_else(|| {
                GroupEngineError::new("OpenRaft append batch returned no response")
            })?;
            match response? {
                GroupWriteResponse::AppendBatch(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch_many_with_cold_admission<'a>(
        &'a mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupWriteBatchFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            let commands = requests
                .into_iter()
                .map(GroupWriteCommand::from)
                .collect::<Vec<_>>();
            return self.write_batch(commands, placement);
        }
        Box::pin(async move {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let command = GroupWriteCommand::Batch {
                commands: requests
                    .iter()
                    .cloned()
                    .map(GroupWriteCommand::from)
                    .collect(),
            };
            if let Some(response) = self
                .forward_write_to_leader_if_follower(command.clone())
                .await?
            {
                return match response {
                    GroupWriteResponse::Batch(responses) => Ok(responses),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append batch many write response: {other:?}"
                    ))),
                };
            }
            self.with_state_machine({
                let requests = requests.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_append_batch_many_cold_admission(requests, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            let mut responses = self.write_commands(vec![command]).await?;
            let response = responses.pop().ok_or_else(|| {
                GroupEngineError::new("OpenRaft append batch many returned no response")
            })?;
            match response? {
                GroupWriteResponse::Batch(responses) => Ok(responses),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch many write response: {other:?}"
                ))),
            }
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::FlushCold(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected flush cold write response: {other:?}"
                ))),
            }
        })
    }

    fn write_batch<'a>(
        &'a mut self,
        commands: Vec<GroupWriteCommand>,
        _placement: ShardPlacement,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(async move { self.write_commands(commands).await })
    }

    fn snapshot<'a>(&'a mut self, _placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .group_snapshot()
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))
                })
            })
            .await?
        })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.install_group_snapshot(snapshot).await })
            })
            .await?
        })
    }
}

pub(crate) fn group_engine_io_error(err: ursula_runtime::GroupEngineError) -> io::Error {
    io::Error::other(err.message().to_owned())
}

pub(crate) fn invalid_data(err: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[derive(Debug, Clone)]
pub struct StaticGrpcRaftGroupEngineFactory {
    node_id: u64,
    peers: BTreeMap<u64, String>,
    initialize_membership: bool,
    initialize_membership_per_group: bool,
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
    log_stores: Option<DurableRaftLogStoreFactory>,
}

impl StaticGrpcRaftGroupEngineFactory {
    pub fn new(
        node_id: u64,
        peers: impl IntoIterator<Item = (u64, String)>,
        initialize_membership: bool,
        registry: RaftGroupHandleRegistry,
    ) -> Self {
        Self {
            node_id,
            peers: peers.into_iter().collect(),
            initialize_membership,
            initialize_membership_per_group: false,
            registry,
            cold_store: None,
            log_stores: None,
        }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }

    pub fn with_raft_log_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.log_stores = Some(DurableRaftLogStoreFactory::new(root));
        self
    }

    pub fn with_per_group_membership_initializers(mut self, enabled: bool) -> Self {
        self.initialize_membership_per_group = enabled;
        self
    }

    fn peer_nodes(&self) -> BTreeMap<u64, BasicNode> {
        self.peers
            .iter()
            .map(|(node_id, address)| (*node_id, BasicNode::new(address.clone())))
            .collect()
    }

    fn should_initialize_membership(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.initialize_membership {
            return false;
        }
        if !self.initialize_membership_per_group {
            return true;
        }
        let peer_count = self.peers.len();
        if peer_count == 0 {
            return false;
        }
        let initializer_index =
            usize::try_from(raft_group_id.0).expect("raft group id fits usize") % peer_count;
        self.peers
            .keys()
            .nth(initializer_index)
            .is_some_and(|node_id| *node_id == self.node_id)
    }
}

impl GroupEngineFactory for StaticGrpcRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ursula_shard::ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            if !self.peers.contains_key(&self.node_id) {
                return Err(GroupEngineError::new(format!(
                    "raft node {} is not present in static peer config",
                    self.node_id
                )));
            }
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 100,
                    election_timeout_min: 300,
                    election_timeout_max: 600,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let engine = if let Some(log_stores) = &self.log_stores {
                RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    log_stores.open(placement, metrics.clone())?,
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?
            } else {
                RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?
            };
            self.registry.register(placement, engine.raft_handle());
            if self.should_initialize_membership(placement.raft_group_id) {
                engine.initialize_membership(self.peer_nodes()).await?;
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}
