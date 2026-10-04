#[cfg(test)]
mod cold_drivers_tests;
#[cfg(test)]
mod compact_tests;
#[cfg(test)]
mod external_index_after_commit_tests;
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
use openraft::ReadPolicy;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogStorage;
use openraft::type_config::TypeConfigExt;
use ursula_runtime::AdvanceRetentionRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::AppendTransactionRequest;
use ursula_runtime::AppendTransactionResponse;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdOrphanSweepPlan;
use ursula_runtime::ColdOrphanSweepRequest;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CompactColdRequest;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::GroupAckColdGcFuture;
use ursula_runtime::GroupAdvanceRetentionFuture;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupAppendTransactionFuture;
use ursula_runtime::GroupBootstrapStreamFuture;
use ursula_runtime::GroupBucketUsageFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupColdHotBacklogFuture;
use ursula_runtime::GroupCompactColdFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeferColdGcFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupFeatureLevelFuture;
use ursula_runtime::GroupFlushColdFuture;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupPlanColdFlushFuture;
use ursula_runtime::GroupPlanColdGcFuture;
use ursula_runtime::GroupPlanColdOrphanSweepFuture;
use ursula_runtime::GroupPlanNextColdFlushBatchFuture;
use ursula_runtime::GroupPlanSharedRefCompactionFuture;
use ursula_runtime::GroupPublishSnapshotFuture;
use ursula_runtime::GroupPurgeBucketFuture;
use ursula_runtime::GroupReadSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupReadStreamParts;
use ursula_runtime::GroupReadStreamPartsFuture;
use ursula_runtime::GroupRepairColdIndexFuture;
use ursula_runtime::GroupSetFeatureLevelFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupStateGaugesFuture;
use ursula_runtime::GroupTidyStreamFuture;
use ursula_runtime::GroupTidyStreamsFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RepairColdIndexRequest;
use ursula_runtime::RepairColdIndexResponse;
use ursula_runtime::SetFeatureLevelRequest;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::TidyStreamsRequest;
use ursula_runtime::TidyStreamsResponse;
use ursula_runtime::TouchStreamAccessResponse;
use ursula_runtime::clipped_entries;
use ursula_runtime::collect_retained_cold_objects;
use ursula_runtime::default_snapshot_store;
use ursula_runtime::repair_cold_index_response;
use ursula_runtime::repair_cold_index_streams;
use ursula_runtime::replace_cold_chunk_index_pages_with_rollback_in_generation;
use ursula_runtime::rollback_cold_index_pages;
use ursula_runtime::write_cold_chunk_index_pages_with_rollback_in_generation;
use ursula_runtime::write_external_segment_index_pages;
use ursula_runtime::write_external_segment_index_pages_in_generation;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::SharedRefCompactionRequest;
use ursula_stream::StreamCommand;

use crate::forward::forward_head_stream_to_leader;
use crate::forward::forward_purge_bucket_to_leader;
use crate::forward::forward_read_stream_to_leader;
use crate::forward::group_engine_client_write_error;
use crate::forward::group_engine_forward_to_leader_error;
use crate::forward::group_engine_leader_read_unavailable;
use crate::forward::group_engine_linearizable_read_error;
use crate::forward::write_result_from_raft_response;
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
    pub(crate) cold_store: Option<ColdStoreHandle>,
    pub(crate) cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
}

pub(crate) fn should_forward_stale_follower_read_error(
    is_leader: bool,
    error: &GroupEngineError,
) -> bool {
    !is_leader
        && matches!(
            error.code(),
            Some(StreamErrorCode::InvalidRecordBoundaries | StreamErrorCode::StreamNotFound)
        )
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
        Self::new_single_node_with_log_store_metrics_and_snapshot_metadata(
            placement, node_id, node, config, log_store, metrics, cold_store, None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new_single_node_with_log_store_metrics_and_snapshot_metadata<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_metadata_path: Option<PathBuf>,
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
            snapshot_metadata_path,
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
        snapshot_metadata_path: Option<PathBuf>,
    ) -> Result<Self, GroupEngineError>
    where
        NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let snapshot_store = snapshot_store.unwrap_or_else(default_snapshot_store);
        let snapshot_build = snapshot_build.unwrap_or_default();
        let snapshot_install = snapshot_install.unwrap_or_default();
        let mut state_machine = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            metrics,
            cold_store.clone(),
            snapshot_store,
            snapshot_build,
            snapshot_install,
            snapshot_metadata_path,
        );
        state_machine
            .restore_persisted_snapshot()
            .await
            .map_err(|err| GroupEngineError::new(format!("restore OpenRaft snapshot: {err}")))?;
        // One page cache per group, shared by the read path and the state
        // machine: invalidations on apply (FlushCold, CompactCold, snapshot
        // install) then reach reads on every replica, followers included.
        let cold_index_cache = state_machine.engine.cold_index_cache();
        let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
            node_id,
            config,
            network_factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|err| GroupEngineError::new(format!("create OpenRaft group: {err}")))?;

        Ok(Self {
            raft,
            placement,
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

    /// This replica's applied group state, leader or follower (simulation
    /// introspection: replicas compare producer state, bounded-state
    /// Invariant 12).
    #[cfg(madsim)]
    pub async fn sim_local_group_snapshot(
        &self,
    ) -> Result<ursula_runtime::GroupSnapshot, GroupEngineError> {
        self.with_state_machine(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .group_snapshot()
                    .await
                    .map_err(|err| GroupEngineError::new(format!("group snapshot: {err}")))
            })
        })
        .await?
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
        let response = match self.raft.client_write(command).await {
            Ok(response) => response,
            Err(err) => {
                let self_id = self.raft.metrics().borrow_watched().id;
                return Err(group_engine_client_write_error(err, self_id));
            }
        };
        write_result_from_raft_response(response.data)?
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
            true,
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

    /// The local applied feature level and, for `stream_id`, the live
    /// stream's cold-index generation (F14g; 0 when absent). Pre-proposal
    /// cold-index page writes use them; both are monotone with respect to
    /// what apply later sees for the same incarnation.
    pub(crate) async fn local_cold_index_generation(
        &self,
        stream_id: Option<BucketStreamId>,
    ) -> Result<(u32, u64), GroupEngineError> {
        self.with_state_machine(move |state_machine| {
            Box::pin(async move {
                let engine = &state_machine.engine;
                let generation = stream_id
                    .as_ref()
                    .and_then(|stream_id| engine.cold_index_generation(stream_id))
                    .unwrap_or(0);
                (engine.feature_level(), generation)
            })
        })
        .await
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
            .write(GroupWriteCommand::Stream(
                StreamCommand::TouchStreamAccess {
                    stream_id: stream_id.clone(),
                    now_ms,
                    renew_ttl,
                },
            ))
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
            true,
        ))
    }

    /// Linearizable leader read (ReadIndex): on the leader, confirm
    /// leadership with a quorum round trip and wait until the local state
    /// machine has applied the read index, so the read that follows cannot
    /// miss a write any leader acknowledged before it started. A deposed
    /// leader cut off from the quorum answers a forward (or 503) instead of
    /// a stale view. A follower gets the plain forward-to-leader error.
    ///
    /// One confirmation round waits one heartbeat interval for the quorum,
    /// which a loaded leader can miss. While this node still believes it
    /// leads, `QuorumNotEnough` is retried (at most one round per heartbeat
    /// interval) until `election_timeout_min` has passed; only then is it a
    /// 503. The wait for the local apply is bounded by the same timeout and
    /// also ends in a 503, so a read never hangs on a stalled state machine.
    pub(crate) async fn require_linearizable_leader_read(
        &self,
        operation: &str,
    ) -> Result<(), GroupEngineError> {
        self.require_local_leader_for_read(operation).await?;
        let config = self.raft.config();
        let budget = Duration::from_millis(config.election_timeout_min);
        let round = Duration::from_millis(config.heartbeat_interval);
        let deadline = UrsulaRaftTypeConfig::now() + budget;
        let self_id = || self.raft.metrics().borrow_watched().id;
        let linearizer = loop {
            let started = UrsulaRaftTypeConfig::now();
            match self.raft.get_read_linearizer(ReadPolicy::ReadIndex).await {
                Ok(linearizer) => break linearizer,
                Err(err)
                    if matches!(
                        err.api_error(),
                        Some(openraft::error::LinearizableReadError::QuorumNotEnough(_))
                    ) && self.raft.is_leader()
                        && UrsulaRaftTypeConfig::now() < deadline =>
                {
                    tracing::debug!("OpenRaft {operation} retrying leadership confirmation: {err}");
                    let next_round = (started + round).min(deadline);
                    UrsulaRaftTypeConfig::sleep_until(next_round).await;
                }
                Err(err) => {
                    return Err(group_engine_linearizable_read_error(
                        err,
                        operation,
                        self_id(),
                    ));
                }
            }
        };
        match linearizer.try_await_ready(&self.raft, Some(budget)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(state)) => {
                tracing::debug!(
                    "OpenRaft {operation} timed out waiting to apply the read index: {state:?}"
                );
                Err(group_engine_leader_read_unavailable(
                    format!("OpenRaft {operation} did not apply the read index in time"),
                    self_id(),
                ))
            }
            Err(fatal) => Err(GroupEngineError::new(format!(
                "OpenRaft {operation} could not apply the read index: {fatal}"
            ))),
        }
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
            // A create of a live stream never applies its initial payload, so
            // it must not write a page entry at offset 0 of that stream.
            let stream_is_live = {
                let stream_id = request.stream_id.clone();
                let now_ms = request.now_ms;
                self.with_state_machine(move |state_machine| {
                    Box::pin(async move { state_machine.engine.stream_is_live(&stream_id, now_ms) })
                })
                .await?
            };
            // From feature level 1 the state keeps the initial payload as a
            // direct reference (F14g). The local level never exceeds the
            // level at apply, so skipping the page is always safe.
            if let Some(cold_store) = self.cold_store.as_ref()
                && !stream_is_live
                && self.local_cold_index_generation(None).await?.0
                    < ursula_runtime::FEATURE_LEVEL_KEYED_STREAMS
            {
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
            self.require_linearizable_leader_read("head_stream").await?;
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

    fn bucket_usage<'a>(&'a mut self, _placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        Box::pin(async move {
            // Served from the local applied state machine, follower or
            // leader: usage export tolerates replication lag, and a
            // leadership requirement would break node-local aggregation
            // whenever any group is led elsewhere.
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { Ok(state_machine.engine.bucket_usage_report()) })
            })
            .await?
        })
    }

    fn feature_level<'a>(&'a mut self, _placement: ShardPlacement) -> GroupFeatureLevelFuture<'a> {
        Box::pin(async move {
            // Local applied state, follower or leader: `ursulactl cluster
            // enable-feature` verifies every replica, not just leaders.
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { Ok(state_machine.engine.feature_level()) })
            })
            .await?
        })
    }

    fn state_gauges<'a>(&'a mut self, _placement: ShardPlacement) -> GroupStateGaugesFuture<'a> {
        Box::pin(async move {
            // Local applied state, follower or leader, like `feature_level`.
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { Ok(state_machine.engine.state_gauges()) })
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
            if request.leader_only {
                // A follower forwards with `leader_only` set, so the leader
                // linearizes the read too (field 6 of `ReadStreamReadV1`).
                if !self.raft.is_leader()
                    && let Some(leader_node) = self.current_leader_node().await
                {
                    let response =
                        forward_read_stream_to_leader(placement, &leader_node, request).await?;
                    return Ok(GroupReadStreamParts::from_response(response));
                }
                self.require_linearizable_leader_read("leader-only read_stream")
                    .await?;
            }
            if !self.raft.is_leader() {
                match self
                    .access_requires_write(request.stream_id.clone(), request.now_ms, true)
                    .await
                {
                    Ok(false) => {}
                    Ok(true) | Err(_) => {
                        if let Some(leader_node) = self.current_leader_node().await {
                            let response =
                                forward_read_stream_to_leader(placement, &leader_node, request)
                                    .await?;
                            return Ok(GroupReadStreamParts::from_response(response));
                        }
                        self.require_local_leader_for_read("read_stream").await?;
                    }
                }
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
                .await?;
            let plan = match plan {
                Ok(plan) => plan,
                // A follower may be behind a write that the leader has
                // already acknowledged. In that window, a read-after-write
                // cursor is beyond only the follower's local tail. Forward
                // instead of exposing a false permanent boundary error.
                Err(error)
                    if should_forward_stale_follower_read_error(self.raft.is_leader(), &error) =>
                {
                    if let Some(leader_node) = self.current_leader_node().await {
                        let response = forward_read_stream_to_leader(
                            placement,
                            &leader_node,
                            original_request,
                        )
                        .await?;
                        return Ok(GroupReadStreamParts::from_response(response));
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let mut parts = GroupReadStreamParts::from_plan(
                placement,
                stream_id,
                plan,
                self.cold_store.clone(),
                self.cold_index_cache.clone(),
            );
            if !self.raft.is_leader() {
                // A bracketed record read decides `up_to_date` only after
                // trimming; on a follower it never claims it (F1).
                parts.forbid_trimmed_up_to_date();
            }
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
        Box::pin(async move { self.require_linearizable_leader_read("live_read").await })
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

    fn advance_retention<'a>(
        &'a mut self,
        request: AdvanceRetentionRequest,
        _placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::AdvanceRetention(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected advance retention write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::AdvanceRetention(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected advance retention write response: {other:?}"
                ))),
            }
        })
    }

    fn import_group_state<'a>(
        &'a mut self,
        request: ursula_runtime::ImportGroupStateRequest,
        _placement: ShardPlacement,
    ) -> ursula_runtime::GroupImportGroupStateFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(ursula_stream::StreamCommand::from(request));
            if let Some(response) = self
                .forward_write_to_leader_if_follower(command.clone())
                .await?
            {
                return match response {
                    GroupWriteResponse::ImportGroupState(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected group state import response: {other:?}"
                    ))),
                };
            }
            match self.write(command).await? {
                GroupWriteResponse::ImportGroupState(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected group state import response: {other:?}"
                ))),
            }
        })
    }

    fn tidy_stream<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupTidyStreamFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(StreamCommand::TidyStream { stream_id, now_ms });
            if let Some(response) = self
                .forward_write_to_leader_if_follower(command.clone())
                .await?
            {
                return match response {
                    GroupWriteResponse::TidyStream(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected tidy stream write response: {other:?}"
                    ))),
                };
            }
            match self.write(command).await? {
                GroupWriteResponse::TidyStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected tidy stream write response: {other:?}"
                ))),
            }
        })
    }

    fn offload_cold_refs<'a>(
        &'a mut self,
        request: ursula_runtime::OffloadColdRefsRequest,
        _placement: ShardPlacement,
    ) -> ursula_runtime::GroupOffloadColdRefsFuture<'a> {
        Box::pin(async move {
            let mut report = ursula_runtime::OffloadColdRefsResponse::default();
            // Leader-side driver: followers write no pages and propose nothing.
            let Some(cold_store) = self.cold_store.clone() else {
                return Ok(report);
            };
            if !self.raft.is_leader() {
                return Ok(report);
            }
            let candidates = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .engine
                            .staged_external_ref_candidates(&request)
                    })
                })
                .await?;
            let store = ColdStoreColdIndexPageStore::new(cold_store);
            for candidate in candidates {
                // Index after commit: every ref is committed, so its entries
                // are correct whatever happens to the proposal below, and a
                // retried pass rewrites them unchanged.
                for object in &candidate.refs {
                    let clipped = ursula_runtime::write_proven_external_index_pages(
                        &store,
                        &candidate.stream_id,
                        candidate.cold_generation,
                        object,
                    )
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    report.page_entries_clipped =
                        report.page_entries_clipped.saturating_add(clipped);
                }
                if let Some(cache) = self.cold_index_cache.as_ref() {
                    cache.invalidate_stream(&candidate.stream_id);
                }
                let command = GroupWriteCommand::from(StreamCommand::OffloadColdRefs {
                    stream_id: candidate.stream_id,
                    refs: candidate.refs,
                });
                match self.write(command).await {
                    Ok(GroupWriteResponse::OffloadColdRefs(response)) => {
                        report.streams = report.streams.saturating_add(1);
                        report.refs_offloaded =
                            report.refs_offloaded.saturating_add(response.removed);
                    }
                    Ok(other) => {
                        return Err(GroupEngineError::new(format!(
                            "unexpected offload cold refs write response: {other:?}"
                        )));
                    }
                    Err(err) if err.code().is_some() => {
                        report.rejected = report.rejected.saturating_add(1);
                    }
                    // Ambiguous (lost leadership, transport): the refs stay
                    // in state until a later pass; the pages are correct.
                    Err(err) => return Err(err),
                }
            }
            Ok(report)
        })
    }

    fn tidy_streams<'a>(
        &'a mut self,
        request: TidyStreamsRequest,
        placement: ShardPlacement,
    ) -> GroupTidyStreamsFuture<'a> {
        Box::pin(async move {
            // Leader-side driver: followers propose nothing.
            if !self.raft.is_leader() {
                return Ok(TidyStreamsResponse::default());
            }
            let candidates = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        Ok(state_machine
                            .engine
                            .tidy_candidates(request.now_ms, request.max_streams))
                    })
                })
                .await??;
            let mut report = TidyStreamsResponse::default();
            for stream_id in candidates {
                let response = self
                    .tidy_stream(stream_id, request.now_ms, placement)
                    .await?;
                report.tidied = report.tidied.saturating_add(1);
                if response.debt_remaining {
                    report.debt_remaining = report.debt_remaining.saturating_add(1);
                }
            }
            Ok(report)
        })
    }

    fn set_feature_level<'a>(
        &'a mut self,
        request: SetFeatureLevelRequest,
        _placement: ShardPlacement,
    ) -> GroupSetFeatureLevelFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request);
            if let Some(response) = self
                .forward_write_to_leader_if_follower(command.clone())
                .await?
            {
                return match response {
                    GroupWriteResponse::SetFeatureLevel(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected set feature level write response: {other:?}"
                    ))),
                };
            }
            match self.write(command).await? {
                GroupWriteResponse::SetFeatureLevel(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected set feature level write response: {other:?}"
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
            self.require_linearizable_leader_read("read_snapshot")
                .await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.read_snapshot(request, placement).await })
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
            self.require_linearizable_leader_read("bootstrap_stream")
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
                .write(GroupWriteCommand::Stream(
                    StreamCommand::TouchStreamAccess {
                        stream_id,
                        now_ms,
                        renew_ttl,
                    },
                ))
                .await?
            {
                GroupWriteResponse::TouchStreamAccess(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
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

    fn purge_bucket<'a>(
        &'a mut self,
        bucket_id: String,
        _placement: ShardPlacement,
    ) -> GroupPurgeBucketFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader() {
                let leader_id = self.raft.current_leader().await;
                let leader_node = self.current_leader_node().await;
                let self_id = self.raft.metrics().borrow_watched().id;
                let Some(leader_node) = leader_node else {
                    return Err(group_engine_forward_to_leader_error(
                        "OpenRaft bucket purge has no known group leader",
                        leader_id,
                        None,
                        self_id,
                        true,
                    ));
                };
                return forward_purge_bucket_to_leader(self.placement, &leader_node, bucket_id)
                    .await;
            }
            match self
                .write(GroupWriteCommand::Stream(StreamCommand::PurgeBucket {
                    bucket_id,
                }))
                .await?
            {
                GroupWriteResponse::PurgeBucket(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected purge bucket write response: {other:?}"
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
                .write(GroupWriteCommand::Stream(StreamCommand::AckColdGc {
                    up_to_seq,
                }))
                .await?
            {
                GroupWriteResponse::AckColdGc(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected ack cold gc write response: {other:?}"
                ))),
            }
        })
    }

    fn defer_cold_gc<'a>(
        &'a mut self,
        seq: u64,
        not_before_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupDeferColdGcFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::Stream(StreamCommand::DeferColdGc {
                    seq,
                    not_before_ms,
                }))
                .await?
            {
                GroupWriteResponse::DeferColdGc(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected defer cold gc write response: {other:?}"
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

    fn plan_shared_ref_compaction<'a>(
        &'a mut self,
        request: SharedRefCompactionRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanSharedRefCompactionFuture<'a> {
        Box::pin(async move {
            // The legacy-pack migration counts the global debt on every node
            // (bucket purge must not proceed while any group holds legacy
            // slices); its publishes still redirect to the leader.
            if !request.legacy_packs_only && !self.raft.is_leader() {
                return Ok(Vec::new());
            }
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .engine
                        .plan_shared_ref_compaction_candidates(&request)
                })
            })
            .await
        })
    }

    fn plan_cold_orphan_sweep<'a>(
        &'a mut self,
        request: ColdOrphanSweepRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdOrphanSweepFuture<'a> {
        Box::pin(async move {
            if self.cold_store.is_none() || !self.raft.is_leader() {
                return Ok(ColdOrphanSweepPlan::default());
            }
            let raft_group_id = placement.raft_group_id.0;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .engine
                        .cold_orphan_sweep_plan(&request, raft_group_id)
                })
            })
            .await
        })
    }

    fn repair_cold_index<'a>(
        &'a mut self,
        request: RepairColdIndexRequest,
        _placement: ShardPlacement,
    ) -> GroupRepairColdIndexFuture<'a> {
        Box::pin(async move {
            let Some(cold_store) = self.cold_store.clone() else {
                return Ok(RepairColdIndexResponse::default());
            };
            if !self.raft.is_leader() {
                return Ok(RepairColdIndexResponse::default());
            }
            let step = request.clone();
            let (inputs, retention_targets) = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        let engine = &mut state_machine.engine;
                        let inputs = engine.cold_index_repair_inputs_for(&step);
                        let retention_targets = engine.retention_gc_targets(&step, &inputs);
                        (inputs, retention_targets)
                    })
                })
                .await?;
            let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
            let (report, compaction_pages) =
                repair_cold_index_streams(&store, self.cold_index_cache.as_deref(), &inputs)
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
            // F14f: reclaim objects wholly below each visited stream's
            // retained offset once the retention grace has passed.
            if !retention_targets.is_empty() {
                let (_, completed) = collect_retained_cold_objects(
                    &cold_store,
                    self.cold_index_cache.as_deref(),
                    &retention_targets,
                )
                .await;
                if !completed.is_empty() {
                    self.with_state_machine(move |state_machine| {
                        Box::pin(async move {
                            state_machine.engine.retention_gc_collected(&completed);
                        })
                    })
                    .await?;
                }
            }
            Ok(repair_cold_index_response(
                &request,
                &inputs,
                report,
                compaction_pages,
            ))
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
            // F5 (level 3): commit first, index after. Apply keeps the locator
            // in state and the offload pass writes the page entry once the
            // append committed. The leader's applied level never exceeds the
            // level at apply, so skipping the write here is always safe.
            // Below level 3 the page entry written here is the only locator.
            let locators_in_state = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move { state_machine.engine.external_locators_in_state() })
                })
                .await?;
            if let Some(cold_store) = self.cold_store.as_ref().filter(|_| !locators_in_state) {
                let stream_id = request.stream_id.clone();
                let (start_offset, generation) = self
                    .with_state_machine(move |state_machine| {
                        Box::pin(async move {
                            let engine = &state_machine.engine;
                            engine
                                .stream_tail_offset(&stream_id)
                                .map(|tail| {
                                    (tail, engine.cold_index_generation(&stream_id).unwrap_or(0))
                                })
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
                write_external_segment_index_pages_in_generation(
                    &store,
                    &request.stream_id,
                    generation,
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

    fn append_transaction<'a>(
        &'a mut self,
        request: AppendTransactionRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendTransactionFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::Transaction {
                commands: request
                    .operations
                    .iter()
                    .cloned()
                    .map(StreamCommand::from)
                    .collect(),
            };
            if let Some(response) = self
                .forward_write_to_leader_if_follower(command.clone())
                .await?
            {
                return append_transaction_response(response, placement);
            }
            if admission.max_hot_bytes_per_group.is_some() {
                self.with_state_machine({
                    let request = request.clone();
                    move |state_machine| {
                        Box::pin(async move {
                            state_machine
                                .check_append_transaction_cold_admission(
                                    request, placement, admission,
                                )
                                .await
                        })
                    }
                })
                .await??;
            }
            append_transaction_response(self.write(command).await?, placement)
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            let mut index_rollback = None;
            if !request.chunk.shared_object
                && let Some(cold_store) = self.cold_store.as_ref()
            {
                // The group actor runs this flush's check, page write and
                // proposal one at a time with every other page writer, so a
                // passing check proves the range is hot when the write clips.
                let check = request.clone();
                self.with_state_machine(move |state_machine| {
                    Box::pin(async move { state_machine.engine.check_cold_flush(&check) })
                })
                .await??;
                let (_, generation) = self
                    .local_cold_index_generation(Some(request.stream_id.clone()))
                    .await?;
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                let rollback = write_cold_chunk_index_pages_with_rollback_in_generation(
                    &store,
                    &request.stream_id,
                    generation,
                    &request.chunk,
                )
                .await
                .map_err(|err| GroupEngineError::new(err.to_string()))?;
                // The clip rule may have removed stale entries that a cached
                // page still holds.
                if clipped_entries(&rollback) > 0
                    && let Some(cache) = self.cold_index_cache.as_ref()
                {
                    cache.invalidate_stream(&request.stream_id);
                }
                index_rollback = Some((store, rollback));
            }
            let (result, rollback_safe) = match self.write(GroupWriteCommand::from(request)).await {
                Ok(GroupWriteResponse::FlushCold(response)) => (Ok(response), false),
                Ok(other) => (
                    Err(GroupEngineError::new(format!(
                        "unexpected flush cold write response: {other:?}"
                    ))),
                    false,
                ),
                Err(err) => {
                    // F14e: a pre-proposal redirect means the write was never
                    // proposed (RT1: OpenRaft's own ForwardToLeader may follow
                    // a committed entry), and a typed stream error means apply
                    // rejected it (a stale flush), so the page entry is
                    // definitely unreferenced. Other failures are ambiguous:
                    // the flush may still commit, so the entry stays.
                    let rollback_safe = err.is_forward_before_proposal() || err.code().is_some();
                    (Err(err), rollback_safe)
                }
            };
            if rollback_safe && let Some((store, rollback)) = index_rollback {
                rollback_cold_index_pages(&store, rollback)
                    .await
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "rollback cold index after flush rejection: {err}"
                        ))
                    })?;
            }
            result
        })
    }

    fn compact_cold<'a>(
        &'a mut self,
        request: CompactColdRequest,
        _placement: ShardPlacement,
    ) -> GroupCompactColdFuture<'a> {
        Box::pin(async move {
            self.require_local_leader_for_read("cold_compaction")
                .await?;
            let mut index_rollback = None;
            if let Some(cold_store) = self.cold_store.as_ref() {
                let (_, generation) = self
                    .local_cold_index_generation(Some(request.stream_id.clone()))
                    .await?;
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                // Shared pack slices live only in replicated state, never in
                // cold-index pages, so an all-shared input has nothing to
                // replace: index the replacement as a fresh entry instead.
                let rollback = if request.old_chunks.iter().all(|chunk| chunk.shared_object) {
                    write_cold_chunk_index_pages_with_rollback_in_generation(
                        &store,
                        &request.stream_id,
                        generation,
                        &request.replacement,
                    )
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?
                } else {
                    let Some(rollback) =
                        replace_cold_chunk_index_pages_with_rollback_in_generation(
                            &store,
                            &request.stream_id,
                            generation,
                            &request.old_chunks,
                            &request.replacement,
                        )
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))?
                    else {
                        return Err(GroupEngineError::new(
                            "cold compaction input no longer matches the cold index",
                        ));
                    };
                    rollback
                };
                index_rollback = Some((store, rollback));
            }
            let (result, rollback_safe) = match self.write(GroupWriteCommand::from(request)).await {
                Ok(GroupWriteResponse::CompactCold(response)) => (Ok(response), false),
                Ok(other) => (
                    Err(GroupEngineError::new(format!(
                        "unexpected compact cold write response: {other:?}"
                    ))),
                    false,
                ),
                Err(err) => {
                    // A pre-proposal redirect means the write was never
                    // proposed (RT1). A typed stream error was committed but the
                    // state machine rejected it without enqueueing GC. Other
                    // Raft failures have an ambiguous commit outcome, so keep
                    // the replacement index rather than risk restoring inputs
                    // that a committed GC command will later delete.
                    let rollback_safe = err.is_forward_before_proposal() || err.code().is_some();
                    (Err(err), rollback_safe)
                }
            };
            if rollback_safe && let Some((store, rollback)) = index_rollback {
                rollback_cold_index_pages(&store, rollback)
                    .await
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "rollback cold index after compaction failure: {err}"
                        ))
                    })?;
            }
            result
        })
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

fn append_transaction_response(
    response: GroupWriteResponse,
    placement: ShardPlacement,
) -> Result<AppendTransactionResponse, GroupEngineError> {
    let GroupWriteResponse::Batch(items) = response else {
        return Err(GroupEngineError::new(
            "unexpected append transaction write response",
        ));
    };
    let items = items
        .into_iter()
        .map(|item| match item? {
            GroupWriteResponse::Append(response) => Ok(response),
            other => Err(GroupEngineError::new(format!(
                "unexpected append transaction item response: {other:?}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AppendTransactionResponse { placement, items })
}

pub(crate) fn group_engine_io_error(err: ursula_runtime::GroupEngineError) -> io::Error {
    io::Error::other(err.message().into_owned())
}

pub(crate) fn invalid_data(err: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
