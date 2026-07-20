mod factory;

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub use factory::ColdRaftGroupEngineFactory;
pub use factory::DurableRaftGroupEngineFactory;
pub use factory::DurableRaftLogStoreFactory;
pub use factory::RaftEngineConfig;
pub use factory::RaftGroupEngineFactory;
pub use factory::RegisteredRaftGroupEngineFactory;
pub use factory::StaticGrpcRaftGroupEngineFactory;
use openraft::BasicNode;
use openraft::Config;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogStorage;
use ursula_runtime::AppendBatchRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteSnapshotRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::GetStreamAttrsRequest;
use ursula_runtime::GroupAckColdGcFuture;
use ursula_runtime::GroupAppendBatchFuture;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupBootstrapStreamFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupColdHotBacklogFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeleteSnapshotFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupFlushColdFuture;
use ursula_runtime::GroupGetStreamAttrsFuture;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupPlanColdFlushFuture;
use ursula_runtime::GroupPlanColdGcFuture;
use ursula_runtime::GroupPlanNextColdFlushBatchFuture;
use ursula_runtime::GroupPublishSnapshotFuture;
use ursula_runtime::GroupReadSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupReadStreamParts;
use ursula_runtime::GroupReadStreamPartsFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::GroupUpdateStreamAttrsFuture;
use ursula_runtime::GroupWriteBatchFuture;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::TouchStreamAccessResponse;
use ursula_runtime::UpdateStreamAttrsRequest;
use ursula_runtime::default_snapshot_store;
use ursula_runtime::write_cold_chunk_index_pages;
use ursula_runtime::write_external_segment_index_pages;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::codec::group_write_result_from_raft_response;
use crate::forward::forward_get_stream_attrs_to_leader;
use crate::forward::forward_head_stream_to_leader;
use crate::forward::forward_read_stream_to_leader;
use crate::forward::group_engine_client_write_error;
use crate::forward::group_engine_forward_to_leader_error;
use crate::forward::write_commands_on_raft;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::RaftGroupLogStore;
use crate::registry::SingleNodeRaftNetworkFactory;
use crate::state_machine::RaftGroupStateMachine;
use crate::state_machine::SnapshotBuildCoordinator;
use crate::state_machine::SnapshotInstallCoordinator;
use crate::types::UrsulaRaftTypeConfig;

pub struct RaftGroupEngine {
    pub(crate) raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    pub(crate) placement: ShardPlacement,
    pub(crate) metrics: Option<GroupEngineMetrics>,
    pub(crate) cold_store: Option<ColdStoreHandle>,
    pub(crate) cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
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
        let engine = Self::new_node_full(
            placement,
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store,
            metrics,
            cold_store,
            None,
            None,
            None,
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
        Self::new_node_full(
            placement,
            node_id,
            config,
            network_factory,
            log_store,
            metrics,
            cold_store,
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_node_full<NF, LS>(
        placement: ShardPlacement,
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_store: Option<SharedSnapshotStore>,
        snapshot_build: Option<SnapshotBuildCoordinator>,
        snapshot_install: Option<SnapshotInstallCoordinator>,
    ) -> Result<Self, GroupEngineError>
    where
        NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let snapshot_store = snapshot_store.unwrap_or_else(default_snapshot_store);
        let snapshot_build = snapshot_build.unwrap_or_default();
        let snapshot_install = snapshot_install.unwrap_or_default();
        let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
            node_id,
            config,
            network_factory,
            log_store,
            RaftGroupStateMachine::new_with_stores_and_snapshot_install(
                placement,
                metrics.clone(),
                cold_store.clone(),
                snapshot_store,
                snapshot_build,
                snapshot_install,
            ),
        )
        .await
        .map_err(|err| GroupEngineError::new(format!("create OpenRaft group: {err}")))?;

        let cold_index_cache = cold_store.as_ref().map(|cold_store| {
            Arc::new(ColdIndexPageCache::new(
                Arc::new(ColdStoreColdIndexPageStore::new(cold_store.clone())),
                1024,
            ))
        });

        Ok(Self {
            raft,
            placement,
            metrics,
            cold_store,
            cold_index_cache,
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

    /// Returns true if this group observes an established raft leader within
    /// `timeout`. Used at startup to distinguish a fresh bootstrap (no leader
    /// can appear until someone initializes) from a restart rejoining an
    /// existing cluster (peers re-elect a leader that then contacts us).
    pub async fn observe_any_leader(&self, timeout: Duration) -> bool {
        self.raft
            .wait(Some(timeout))
            .metrics(
                |metrics| metrics.current_leader.is_some(),
                "observe an existing raft leader before bootstrap",
            )
            .await
            .is_ok()
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

    #[cfg(madsim)]
    pub async fn sim_read_local_stream(
        &self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<ursula_runtime::ReadStreamResponse, GroupEngineError> {
        let stream_id = request.stream_id.clone();
        let read_request = request.clone();
        let plan = self
            .with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .engine
                        .read_stream_plan_after_access(&read_request)
                })
            })
            .await??;
        GroupReadStreamParts::from_plan(
            placement,
            stream_id,
            plan,
            self.cold_store.clone(),
            self.cold_index_cache.clone(),
        )
        .into_response()
        .await
    }

    pub(crate) async fn write(
        &self,
        command: GroupWriteCommand,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        let response = match self.raft.client_write(command.into()).await {
            Ok(response) => response,
            Err(err) => {
                let self_id = self.raft.metrics().borrow_watched().id;
                return Err(group_engine_client_write_error(err, self_id));
            }
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
        let self_id = self.raft.metrics().borrow_watched().id;
        Err(group_engine_forward_to_leader_error(
            "OpenRaft group write has to run on the local leader runtime",
            leader_id,
            leader_node.as_ref(),
            self_id,
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
        let self_id = self.raft.metrics().borrow_watched().id;
        Err(group_engine_forward_to_leader_error(
            format!("OpenRaft {operation} has to forward request to leader"),
            self.raft.current_leader().await,
            None,
            self_id,
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

impl GroupEngine for RaftGroupEngine {
    fn accepts_local_writes(&self) -> bool {
        self.raft.is_leader()
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return Box::pin(async move {
                match self.write(GroupWriteCommand::from(request)).await? {
                    GroupWriteResponse::CreateStream(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create stream write response: {other:?}"
                    ))),
                }
            });
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
            if let Some(cold_store) = self.cold_store.as_ref() {
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_external_segment_index_pages(
                    &store,
                    &request.stream_id,
                    0,
                    &request.initial_payload,
                )
                .await
                .map_err(|err| GroupEngineError::new(err.to_string()))?;
            }
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
                Box::pin(async move {
                    state_machine
                        .engine
                        .head_stream_after_access(&request, placement)
                })
            })
            .await?
        })
    }

    fn get_stream_attrs<'a>(
        &'a mut self,
        request: GetStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> GroupGetStreamAttrsFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader()
                && let Some(leader_node) = self.current_leader_node().await
            {
                return forward_get_stream_attrs_to_leader(placement, &leader_node, request).await;
            }
            self.require_local_leader_for_read("get_stream_attrs")
                .await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .engine
                        .get_stream_attrs_after_access(&request, placement)
                })
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
            let original_request = request.clone();
            if !self.raft.is_leader()
                && self
                    .access_requires_write(request.stream_id.clone(), request.now_ms, true)
                    .await?
            {
                if let Some(leader_node) = self.current_leader_node().await {
                    let response =
                        forward_read_stream_to_leader(placement, &leader_node, request).await?;
                    return Ok(GroupReadStreamParts::from_response(response));
                }
                self.require_local_leader_for_read("read_stream").await?;
            }
            let stream_id = request.stream_id.clone();
            if self.raft.is_leader() {
                self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                    .await?;
            }
            let read_request = request.clone();
            let plan = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .engine
                            .read_stream_plan_after_access(&read_request)
                    })
                })
                .await??;
            let mut parts = GroupReadStreamParts::from_plan(
                placement,
                stream_id,
                plan,
                self.cold_store.clone(),
                self.cold_index_cache.clone(),
            );
            if !self.raft.is_leader() && parts.up_to_date && !parts.closed {
                if parts.payload_is_empty()
                    && let Some(leader_node) = self.current_leader_node().await
                {
                    let response =
                        forward_read_stream_to_leader(placement, &leader_node, original_request)
                            .await?;
                    return Ok(GroupReadStreamParts::from_response(response));
                }
                parts.up_to_date = false;
            }
            Ok(parts)
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

    fn update_stream_attrs<'a>(
        &'a mut self,
        request: UpdateStreamAttrsRequest,
        _placement: ShardPlacement,
    ) -> GroupUpdateStreamAttrsFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::UpdateStreamAttrs(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected update stream attrs write response: {other:?}"
                    ))),
                };
            }
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::UpdateStreamAttrs(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected update stream attrs write response: {other:?}"
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

    fn ack_cold_gc<'a>(
        &'a mut self,
        up_to_seq: u64,
        _placement: ShardPlacement,
    ) -> GroupAckColdGcFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::AckColdGc { up_to_seq })
                .await?
            {
                GroupWriteResponse::AckColdGc(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected ack cold gc write response: {other:?}"
                ))),
            }
        })
    }

    fn plan_cold_gc<'a>(
        &'a mut self,
        max: usize,
        placement: ShardPlacement,
    ) -> GroupPlanColdGcFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.plan_cold_gc(max, placement).await })
            })
            .await?
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
            if let Some(cold_store) = self.cold_store.as_ref() {
                let stream_id = request.stream_id.clone();
                let start_offset = self
                    .with_state_machine(move |state_machine| {
                        Box::pin(async move {
                            state_machine
                                .engine
                                .stream_tail_offset(&stream_id)
                                .ok_or_else(|| {
                                    GroupEngineError::stream(
                                        StreamErrorCode::StreamNotFound,
                                        format!("stream '{stream_id}' does not exist"),
                                    )
                                })
                        })
                    })
                    .await??;
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_external_segment_index_pages(
                    &store,
                    &request.stream_id,
                    start_offset,
                    &request.payload,
                )
                .await
                .map_err(|err| GroupEngineError::new(err.to_string()))?;
            }
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external append write response: {other:?}"
                ))),
            }
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
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
            if admission.max_hot_bytes_per_group.is_some() {
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
            }
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
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
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
            if admission.max_hot_bytes_per_group.is_some() {
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
            }
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

    fn append_batch_many<'a>(
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
            if let Some(cold_store) = self.cold_store.as_ref() {
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_cold_chunk_index_pages(&store, &request.stream_id, &request.chunk)
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
            }
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

    fn shutdown<'a>(&'a mut self) -> ursula_runtime::GroupShutdownFuture<'a> {
        Box::pin(async move { RaftGroupEngine::shutdown(self).await })
    }
}

pub(crate) fn group_engine_io_error(err: ursula_runtime::GroupEngineError) -> io::Error {
    io::Error::other(err.message().into_owned())
}

pub(crate) fn invalid_data(err: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
