#[cfg(all(test, not(madsim)))]
mod cold_drivers_tests;
#[cfg(all(test, not(madsim)))]
mod compact_tests;
#[cfg(all(test, not(madsim)))]
mod external_index_after_commit_tests;
mod factory;
#[cfg(all(test, not(madsim)))]
mod history_pending_cold_tests;
#[cfg(all(test, not(madsim)))]
mod test_support;
mod wal_error;

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub use factory::DurableRaftGroupEngineFactory;
pub use factory::RaftEngineConfig;
pub use factory::StaticGrpcRaftGroupEngineFactory;
use openraft::BasicNode;
use openraft::Config;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogStorage;
use ursula_runtime::AdvanceRetentionRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
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
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupAckColdGcFuture;
use ursula_runtime::GroupAdvanceRetentionFuture;
use ursula_runtime::GroupAppendFuture;
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
use ursula_runtime::GroupReadRoute;
use ursula_runtime::GroupReadSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupReadStreamParts;
use ursula_runtime::GroupReadStreamPartsFuture;
use ursula_runtime::GroupRepairColdIndexFuture;
use ursula_runtime::GroupRouteHeadStreamFuture;
use ursula_runtime::GroupRouteReadStreamFuture;
use ursula_runtime::GroupRouteWriteFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupStateGaugesFuture;
use ursula_runtime::GroupTidyStreamFuture;
use ursula_runtime::GroupTidyStreamsFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LinearizableReadBarrier;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::Proposal;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadEnd;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RepairColdIndexRequest;
use ursula_runtime::RepairColdIndexResponse;
use ursula_runtime::RouteWriteRequest;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::TidyStreamsRequest;
use ursula_runtime::TidyStreamsResponse;
use ursula_runtime::TouchStreamAccessResponse;
use ursula_runtime::WriteRoute;
use ursula_runtime::clipped_entries;
use ursula_runtime::collect_retained_cold_objects;
use ursula_runtime::default_snapshot_store;
use ursula_runtime::expiry_sweep;
use ursula_runtime::immutable_read;
use ursula_runtime::repair_cold_index_response;
use ursula_runtime::repair_cold_index_streams;
use ursula_runtime::replace_cold_chunk_index_pages_with_rollback_in_generation;
use ursula_runtime::rollback_cold_index_pages;
use ursula_runtime::write_cold_chunk_index_pages_with_rollback_in_generation;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::SharedRefCompactionRequest;
use ursula_stream::StreamCommand;
use ursula_stream::StreamReadPlan;

use crate::GroupLeadership;
use crate::forward::LeaderRoute;
use crate::forward::forward_head_stream_to_leader;
use crate::forward::forward_purge_bucket_to_leader;
use crate::forward::forward_read_stream_to_leader;
use crate::forward::group_engine_client_write_error;
use crate::forward::leadership_refusal;
use crate::forward::write_result_from_raft_response;
use crate::read_index::ReadIndexBarrier;
use crate::registry::SingleNodeRaftNetworkFactory;
use crate::state_machine::RaftGroupStateMachine;
use crate::state_machine::SnapshotBuildCoordinator;
use crate::state_machine::SnapshotInstallCoordinator;
use crate::types::UrsulaRaftTypeConfig;

/// Optional capabilities of a group engine, independent of its transport and log store.
#[derive(Default)]
pub struct RaftGroupEngineOptions {
    pub(crate) apply_stop_signal: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Faulty code for tests and simulations; production has none.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub apply_fault: Option<crate::apply_failure::ApplyFault>,
    pub metrics: Option<GroupEngineMetrics>,
    pub cold_store: Option<ColdStoreHandle>,
    pub snapshot_store: Option<SharedSnapshotStore>,
    pub snapshot_build: Option<SnapshotBuildCoordinator>,
    pub snapshot_install: Option<SnapshotInstallCoordinator>,
    pub snapshot_metadata_path: Option<PathBuf>,
    /// The replica's recovery gate. Its recovery plan fences the history
    /// before the Raft core starts, so a plan's leader that restarts never
    /// leads without it (N1, #581 review). The gate is bound later by
    /// [`RaftGroupEngine::publish_recovery`].
    pub recovery: Option<Arc<crate::GroupRejoin>>,
    /// This node's side of the cluster-id check: its identity, stamped on
    /// the reads and purges it forwards to a leader, and where a leader's
    /// refusal is recorded. `None` for a replica without a gRPC transport.
    pub peer_cluster: Option<crate::PeerCluster>,
}

pub struct RaftGroupEngine {
    pub(crate) rejoin: std::sync::Mutex<Option<Arc<crate::GroupRejoin>>>,
    /// The group's history on this replica, and the leader's fence while it
    /// is pending (#581 sections 4.3 and 4.5).
    pub(crate) history: Arc<crate::history::GroupHistory>,
    pub(crate) apply_health: crate::apply_failure::ApplyHealth,
    pub(crate) snapshot_installs: Arc<crate::state_machine::SnapshotInstallLifecycle>,
    pub(crate) metadata_serial:
        Arc<crate::rt::sync::Mutex<crate::state_machine::MetadataPublicationState>>,
    pub(crate) recovery_tasks: crate::rejoin::RecoveryGate,
    pub(crate) raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    pub(crate) placement: ShardPlacement,
    pub(crate) cold_store: Option<ColdStoreHandle>,
    pub(crate) cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
    /// The group's coalescing ReadIndex barrier, shared with the runtime and
    /// with forwarded gRPC reads (through the registry).
    pub(crate) read_barrier: Arc<ReadIndexBarrier>,
    /// See [`RaftGroupEngineOptions::peer_cluster`].
    pub(crate) peer_cluster: Option<crate::PeerCluster>,
    /// The state machine's log gauge, which also holds its applied
    /// membership (#613).
    pub(crate) log_gauge: Arc<crate::snapshot_cadence::GroupLogGauge>,
}

/// Whether a read's error may hold only for a replica's lagging state: a
/// missing stream, an offset beyond the tail and an offset below the
/// retained offset may all be the answer of an incarnation the replica has
/// not seen replaced, or of a tail it has not applied yet. A follower
/// forwards such a read to the leader, and the leader answers it only after
/// a ReadIndex round, so none of these comes from a state that lags. They
/// are rare, and the rounds coalesce.
pub(crate) fn may_hold_only_for_a_lagging_state(error: &GroupEngineError) -> bool {
    matches!(
        error.code(),
        Some(
            StreamErrorCode::StreamNotFound
                | StreamErrorCode::OffsetOutOfRange
                | StreamErrorCode::StreamGone
        )
    )
}

impl RaftGroupEngine {
    /// A single-node group over `log_store`, with the timeouts of a
    /// single-node runtime.
    #[cfg(any(test, madsim))]
    pub async fn new_single_node_on_log_store<LS>(
        placement: ShardPlacement,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
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
        Self::new_single_node_with_log_store_and_metrics(
            placement,
            1,
            BasicNode::new("local"),
            config,
            log_store,
            metrics,
            None,
        )
        .await
    }

    #[cfg(any(test, madsim))]
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

    #[cfg(any(test, madsim))]
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
        Self::new_single_node(
            placement,
            node_id,
            node,
            config,
            log_store,
            RaftGroupEngineOptions {
                metrics,
                cold_store,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn new_single_node<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
        options: RaftGroupEngineOptions,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        Self::new_single_node_observed(
            placement,
            node_id,
            node,
            config,
            log_store,
            options,
            Default::default(),
        )
        .await
    }

    pub(crate) async fn new_single_node_observed<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
        options: RaftGroupEngineOptions,
        apply_health: crate::apply_failure::ApplyHealth,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let engine = Self::new_node_observed(
            placement,
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store,
            options,
            apply_health,
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

    #[cfg(any(test, madsim))]
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
        Self::new_node(
            placement,
            node_id,
            config,
            network_factory,
            log_store,
            RaftGroupEngineOptions {
                metrics,
                cold_store,
                peer_cluster: Some(crate::peer_cluster::test_peer_cluster(node_id)),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn new_node<NF, LS>(
        placement: ShardPlacement,
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
        options: RaftGroupEngineOptions,
    ) -> Result<Self, GroupEngineError>
    where
        NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        Self::new_node_observed(
            placement,
            node_id,
            config,
            network_factory,
            log_store,
            options,
            Default::default(),
        )
        .await
    }

    pub(crate) async fn new_node_observed<NF, LS>(
        placement: ShardPlacement,
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
        options: RaftGroupEngineOptions,
        apply_health: crate::apply_failure::ApplyHealth,
    ) -> Result<Self, GroupEngineError>
    where
        NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let RaftGroupEngineOptions {
            apply_stop_signal,
            #[cfg(any(test, madsim, feature = "fault-injection"))]
            apply_fault,
            metrics,
            cold_store,
            snapshot_store,
            snapshot_build,
            snapshot_install,
            snapshot_metadata_path,
            recovery,
            peer_cluster,
        } = options;
        let snapshot_store = snapshot_store.unwrap_or_else(default_snapshot_store);
        let snapshot_build = snapshot_build.unwrap_or_default();
        let log_gauge = snapshot_build.log_gauge(placement.raft_group_id.0);
        // A purge an earlier incarnation of the group's Raft held its builds
        // for is not pending in the new one (#669).
        snapshot_build.release_purge(placement.raft_group_id.0);
        // A purge through a snapshot holds the group's builds until the log
        // is purged or as many entries applied past it as OpenRaft keeps.
        log_gauge.set_log_keep(config.max_in_snapshot_log_to_keep);
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
        state_machine.apply_stop_signal = apply_stop_signal;
        state_machine.apply_health = apply_health.clone();
        #[cfg(any(test, madsim, feature = "fault-injection"))]
        {
            state_machine.apply_fault = apply_fault;
        }
        state_machine
            .restore_persisted_snapshot()
            .await
            .map_err(|err| GroupEngineError::new(format!("restore OpenRaft snapshot: {err}")))?;
        // One page cache per group, shared by the read path and the state
        // machine: invalidations on apply (FlushCold, CompactCold, snapshot
        // install) then reach reads on every replica, followers included.
        let cold_index_cache = state_machine.engine.cold_index_cache();
        let history = crate::history::GroupHistory::new(
            placement.raft_group_id,
            state_machine.engine.subscribe_history(),
        );
        if let Some(gate) = &recovery {
            history.set_plan(gate.recovery_record());
        }
        let metadata_serial = state_machine.metadata_serial.clone();
        let apply_health = state_machine.apply_health.clone();
        let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
            node_id,
            config,
            network_factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|err| {
            tracing::error!(raft_group_id = placement.raft_group_id.0, error = %err, "Raft initialization failed");
            apply_health
                .check(placement.raft_group_id)
                .err()
                .unwrap_or_else(|| GroupEngineError::new(format!("create OpenRaft group: {err}")))
        })?;
        // `Raft::new` applied the log the restored snapshot does not cover:
        // a removal it applied again is due at once (#669).
        log_gauge.replay_ended();

        // Every replica runs the history driver: a leader proposes the
        // event it owes, and every replica retires a satisfied plan.
        let recovery_tasks = crate::rejoin::RecoveryGate::default();
        recovery_tasks.push(crate::rt::spawn(crate::history::run_history_driver(
            raft.clone(),
            history.clone(),
        )));
        Ok(Self {
            rejoin: Default::default(),
            history,
            apply_health,
            recovery_tasks,
            snapshot_installs: Arc::default(),
            metadata_serial,
            read_barrier: Arc::new(ReadIndexBarrier::new(raft.clone())),
            raft,
            placement,
            cold_store,
            cold_index_cache,
            peer_cluster,
            log_gauge,
        })
    }

    /// The voter configs of the membership this replica keeps when a
    /// recovery plan cuts its log back to its committed entry (#613).
    pub fn kept_voter_configs(&self) -> Vec<std::collections::BTreeSet<u64>> {
        let committed = openraft::rt::WatchReceiver::borrow_watched(&self.raft.metrics())
            .local_committed
            .map(|id| id.index);
        let rejoin = self
            .rejoin
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        crate::rejoin::kept_membership_of(
            rejoin.as_deref(),
            committed,
            self.log_gauge.applied_membership(),
        )
        .membership()
        .get_joint_config()
        .clone()
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

    /// The group's history on this replica (#581 section 4.3).
    pub fn history(&self) -> &crate::GroupHistory {
        &self.history
    }

    /// Bind one recovery gate to this exact engine and publish its complete
    /// resources. Re-publication preserves that gate; another gate is refused.
    pub fn publish_recovery(
        &self,
        gate: Arc<crate::GroupRejoin>,
        registry: &crate::RaftGroupHandleRegistry,
    ) -> Result<(), GroupEngineError> {
        if gate.raft_group_id() != self.placement.raft_group_id {
            return Err(GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RecoveryGroupMismatch {
                    expected: self.placement.raft_group_id,
                    actual: gate.raft_group_id(),
                },
            ));
        }
        let mut binding = self
            .rejoin
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(bound) = binding.as_ref() {
            if !Arc::ptr_eq(bound, &gate) {
                return Err(GroupEngineError::Infra(
                    ursula_runtime::GroupInfraError::RecoveryAlreadyBound {
                        raft_group_id: self.placement.raft_group_id,
                    },
                ));
            }
        } else {
            gate.bind(&self.raft_handle())?;
            self.history.bind_rejoin(&gate);
            *binding = Some(gate);
        }
        drop(binding);
        registry.register_engine(self);
        Ok(())
    }

    /// Attach production recovery drivers with an injected transport.
    pub fn attach_recovery<T: crate::RecoveryTransport>(
        &self,
        gate: Arc<crate::GroupRejoin>,
        registry: &crate::RaftGroupHandleRegistry,
        nodes: BTreeMap<u64, BasicNode>,
        transport: T,
        config: crate::RecoveryConfig,
    ) -> Result<(), GroupEngineError> {
        self.recovery_tasks
            .attach(self, gate, registry, nodes, transport, config)
    }

    pub async fn shutdown(&self) -> Result<(), GroupEngineError> {
        self.snapshot_installs.close();
        self.recovery_tasks.shutdown().await;
        let result = self
            .raft
            .shutdown()
            .await
            .map_err(|err| GroupEngineError::new(format!("shutdown OpenRaft group: {err}")));
        self.snapshot_installs.drain().await;
        // OpenRaft's shutdown leaves a snapshot build it spawned running, so
        // the publication closes here: a publication already running ends
        // first, since cancellation cannot stop an admitted blocking fsync,
        // and none runs after. Only then may the WAL close or a restarted
        // replica of the group open the same metadata file (#489).
        crate::state_machine::close_snapshot_metadata(&self.metadata_serial).await;
        result
    }

    /// This replica's applied group state, leader or follower (simulation
    /// introspection: replicas compare producer state, bounded-state
    /// Invariant 12).
    #[cfg(madsim)]
    pub async fn sim_local_group_snapshot(
        &self,
    ) -> Result<ursula_runtime::GroupSnapshot, GroupEngineError> {
        self.with_applied_state(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .group_snapshot()
                    .await
                    .map_err(|err| GroupEngineError::new(format!("group snapshot: {err}")))
            })
        })
        .await?
    }

    /// What this replica's applied state holds for a stream, read past every
    /// fence: simulation introspection only, never a client answer.
    #[cfg(madsim)]
    pub async fn sim_read_local_stream(
        &self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<ursula_runtime::ReadStreamResponse, GroupEngineError> {
        let stream_id = request.stream_id.clone();
        let read_request = request.clone();
        let plan = self
            .with_applied_state(move |state_machine| {
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
        // Refused before proposal: nothing was written.
        self.apply_health.check(self.placement.raft_group_id)?;
        crate::forward::validate_proposal(&command)?;
        self.history.admit_proposal(&self.raft, &command).await?;
        crate::forward::refuse_unless_leader(&self.raft)?;
        let response = match self.raft.client_write(command).await {
            Ok(response) => response,
            // Once submitted, a group that stops may already have committed
            // the command and applies it after repair: the outcome is unknown.
            Err(err) => {
                let leadership = GroupLeadership::of(&self.raft.metrics().borrow_watched());
                return Err(group_engine_client_write_error(err, &leadership));
            }
        };
        write_result_from_raft_response(response.data)?
    }

    pub(crate) async fn forward_write_to_leader_if_follower(
        &self,
        _command: GroupWriteCommand,
    ) -> Result<Option<GroupWriteResponse>, GroupEngineError> {
        crate::forward::refuse_unless_leader(&self.raft)?;
        Ok(None)
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
        self.apply_health.check(self.placement.raft_group_id)?;
        // A leader whose history is pending serves nothing (#581 section
        // 4.5); this covers every engine read and the early answers.
        self.history.admit(&self.raft).await?;
        self.with_applied_state(f).await
    }

    /// The applied state, without the leader's history fence: replica
    /// introspection only, never a client answer.
    pub(crate) async fn with_applied_state<V>(
        &self,
        f: impl FnOnce(&mut RaftGroupStateMachine) -> openraft::base::BoxFuture<V>
        + OptionalSend
        + 'static,
    ) -> Result<V, GroupEngineError>
    where
        V: OptionalSend + 'static,
    {
        self.apply_health.check(self.placement.raft_group_id)?;
        self.raft
            .with_state_machine(f)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft state-machine access: {err}")))
    }

    /// Run faulty code from now on, as a deployment of a buggy binary does.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub async fn inject_apply_fault(
        &self,
        fault: crate::apply_failure::ApplyFault,
    ) -> Result<(), GroupEngineError> {
        self.with_state_machine(move |state| {
            Box::pin(async move {
                state.apply_fault = Some(fault);
            })
        })
        .await
    }

    /// The live stream's cold-index generation (F14g; 0 when absent).
    /// Pre-proposal cold-index page writes use it; it is what apply later
    /// sees for the same incarnation.
    pub(crate) async fn local_cold_index_generation(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<u64, GroupEngineError> {
        self.with_state_machine(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .engine
                    .cold_index_generation(&stream_id)
                    .unwrap_or(0)
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

    /// The leader's expiry sweep of one tidy pass (#669,
    /// [`ursula_runtime::expiry_sweep`]): while the front of the group's TTL
    /// index is due at the pass's sweep clock, proposes the
    /// `TouchStreamAccess { renew_ttl: false }` that names it, at most
    /// `request.max_expiry_sweeps` times and for at most
    /// [`ursula_runtime::expiry_sweep::GROUP_BUDGET`]. Each command applies
    /// through [`Self::write`], with its apply-health, history and
    /// leadership checks. An error ends the sweep until the next pass,
    /// keeps the commands committed so far in the report, and lets the
    /// pass go on to `TidyStream`.
    async fn sweep_expired_streams(&self, request: TidyStreamsRequest) -> TidyStreamsResponse {
        let mut report = TidyStreamsResponse::default();
        if request.max_expiry_sweeps == 0 || !expiry_sweep::enabled() {
            return report;
        }
        let max = u64::try_from(request.max_expiry_sweeps).unwrap_or(u64::MAX);
        let now_ms = expiry_sweep::sweep_now_ms(request.now_ms);
        let started = crate::rt::time::Instant::now();
        loop {
            let target = match self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move { state_machine.engine.expiry_sweep_target(now_ms) })
                })
                .await
            {
                Ok(target) => target,
                Err(err) => {
                    expiry_sweep::interrupted(self.placement.raft_group_id, &err, &mut report);
                    break;
                }
            };
            let Some(stream_id) = target else {
                break;
            };
            if report.expiry_sweeps >= max || started.elapsed() >= expiry_sweep::GROUP_BUDGET {
                report.expiry_due_groups = 1;
                break;
            }
            if let Err(err) =
                expiry_sweep::settle(self.write(expiry_sweep::command(stream_id, now_ms)).await)
            {
                expiry_sweep::interrupted(self.placement.raft_group_id, &err, &mut report);
                break;
            }
            report.expiry_sweeps = report.expiry_sweeps.saturating_add(1);
        }
        report
    }

    pub(crate) async fn require_local_leader_for_read(
        &self,
        operation: &str,
    ) -> Result<(), GroupEngineError> {
        if self.raft.is_leader() {
            return Ok(());
        }
        Err(self.not_leader_for_read(operation).await)
    }

    async fn not_leader_for_read(&self, operation: &str) -> GroupEngineError {
        tracing::debug!(operation, "OpenRaft read has to run on the leader");
        let leadership = GroupLeadership::of(&self.raft.metrics().borrow_watched());
        leadership_refusal(
            LeadershipReason::Follower,
            Proposal::NotProposed,
            self.raft.current_leader().await,
            None,
            &leadership,
        )
    }

    /// Linearizable leader read (ReadIndex, D10): the read that follows
    /// cannot miss a write any leader acknowledged before it started. A
    /// deposed leader cut off from the quorum answers a forward (or 503)
    /// instead of a stale view; a follower gets the plain forward error.
    ///
    /// `read_index` is the index the runtime confirmed for this request
    /// before queueing it. While this replica leads and has applied
    /// it, the read needs no further round trip. Otherwise (forwarded gRPC
    /// reads, direct engine calls, an engine swapped since) the engine joins
    /// the group's coalescing barrier itself; see [`ReadIndexBarrier`].
    pub(crate) async fn require_linearizable_leader_read(
        &self,
        operation: &str,
        read_index: Option<u64>,
    ) -> Result<(), GroupEngineError> {
        self.linearizable_read_index(operation, read_index)
            .await
            .map(|_| ())
    }

    /// The plan of `request` against this replica's applied state. The
    /// outer error is the state machine's; the inner one is the read's.
    async fn plan_read(
        &self,
        request: &ReadStreamRequest,
    ) -> Result<Result<StreamReadPlan, GroupEngineError>, GroupEngineError> {
        let read_request = request.clone();
        self.with_state_machine(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .engine
                    .read_stream_plan_after_access(&read_request)
            })
        })
        .await
    }

    /// [`Self::require_linearizable_leader_read`] that also returns the
    /// read index the read is served at: `read_index` itself when it was
    /// already confirmed and applied, otherwise the index of a new round.
    pub(crate) async fn linearizable_read_index(
        &self,
        operation: &str,
        read_index: Option<u64>,
    ) -> Result<u64, GroupEngineError> {
        self.require_local_leader_for_read(operation).await?;
        if let Some(read_index) = read_index
            && self.applied_index() >= read_index
        {
            return Ok(read_index);
        }
        match self.read_barrier.round().await? {
            Some(read_index) => Ok(read_index),
            None => Err(self.not_leader_for_read(operation).await),
        }
    }

    fn applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow_watched()
            .last_applied
            .as_ref()
            .map_or(0, |log_id| log_id.index())
    }

    /// Where this follower forwards a leader RPC, when it knows the leader.
    pub(crate) async fn leader_route(&self) -> Option<LeaderRoute> {
        let leader_id = self.raft.current_leader().await?;
        let leader = self
            .raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .get_node(&leader_id)
            .cloned()?;
        Some(LeaderRoute {
            peer_cluster: self.peer_cluster.clone(),
            leader_id,
            leader,
        })
    }
}

/// A read the group leader answers: the forwarded RPC, which the runtime
/// awaits outside the group actor.
fn leader_read(
    route: LeaderRoute,
    placement: ShardPlacement,
    request: ReadStreamRequest,
) -> GroupReadRoute<GroupReadStreamParts> {
    GroupReadRoute::Leader(Box::pin(async move {
        forward_read_stream_to_leader(route, placement, request)
            .await
            .map(GroupReadStreamParts::from_response)
    }))
}

impl GroupEngine for RaftGroupEngine {
    fn accepts_local_writes(&self) -> bool {
        self.raft.is_leader()
    }

    fn history_watch(&self) -> Option<ursula_runtime::HistoryWatch> {
        Some(self.history.subscribe())
    }

    fn linearizable_read_barrier(&self) -> Option<Arc<dyn LinearizableReadBarrier>> {
        Some(self.read_barrier.clone())
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
            // The state keeps the initial payload as a direct reference
            // (F14g), so the engine writes no page entry before proposing.
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
            self.route_head_stream(request, placement)
                .await?
                .resolve()
                .await
        })
    }

    fn route_write<'a>(
        &'a mut self,
        request: RouteWriteRequest,
        _placement: ShardPlacement,
    ) -> GroupRouteWriteFuture<'a> {
        Box::pin(async move {
            // The one leadership policy the front door routes by too.
            let route = GroupLeadership::of(&self.raft.metrics().borrow_watched()).write_route();
            if !matches!(route, WriteRoute::Leader { .. }) {
                return Ok(route);
            }
            // Only the leader answers early (#581 section 2.5). Applied
            // entries are committed, so a leader that was deposed a moment
            // ago still answers truly under the request's token.
            let Some(early) = request.early_answer_request() else {
                return Ok(WriteRoute::Leader { early_answer: None });
            };
            let early_answer = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move { state_machine.engine.producer_early_answer_now(&early) })
                })
                .await?;
            Ok(WriteRoute::Leader { early_answer })
        })
    }

    fn route_head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupRouteHeadStreamFuture<'a> {
        Box::pin(async move {
            // #613: a replica holding an unsatisfied plan serves no client read.
            self.history
                .refuse_planned_read(&self.raft.metrics().borrow_watched())?;
            if !self.raft.is_leader()
                && let Some(route) = self.leader_route().await
            {
                return Ok(GroupReadRoute::Leader(Box::pin(
                    forward_head_stream_to_leader(route, placement, request),
                )));
            }
            if request.linearizable {
                self.require_linearizable_leader_read("head_stream", request.read_index)
                    .await?;
            } else {
                self.require_local_leader_for_read("head_stream").await?;
            }
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
            .map(GroupReadRoute::Local)
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

    fn state_gauges<'a>(&'a mut self, _placement: ShardPlacement) -> GroupStateGaugesFuture<'a> {
        Box::pin(async move {
            // Local applied state, follower or leader, like `bucket_usage`.
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
            self.route_read_stream(request, placement)
                .await?
                .resolve()
                .await
        })
    }

    fn route_read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupRouteReadStreamFuture<'a> {
        Box::pin(async move {
            // #613: a replica holding an unsatisfied plan serves no client read.
            self.history
                .refuse_planned_read(&self.raft.metrics().borrow_watched())?;
            let original_request = request.clone();
            // A live read pinned to its owner's confirmed read index is
            // served only here, while this replica leads and has applied
            // that index; it is never forwarded (a just-elected leader may
            // not have applied what the owner confirmed).
            let owner_pinned = !request.leader_only && request.read_index.is_some();
            if owner_pinned {
                self.require_linearizable_leader_read("live read_stream", request.read_index)
                    .await?;
            }
            if request.leader_only {
                // A follower forwards with `leader_only` set, so the leader
                // linearizes the read too (field 6 of `ReadStreamReadV1`).
                if !self.raft.is_leader()
                    && let Some(route) = self.leader_route().await
                {
                    return Ok(leader_read(route, placement, request));
                }
                self.require_linearizable_leader_read(
                    "leader-only read_stream",
                    request.read_index,
                )
                .await?;
            }
            if owner_pinned && !self.raft.is_leader() {
                return Err(self.not_leader_for_read("live read_stream").await);
            }
            if !self.raft.is_leader() {
                match self
                    .access_requires_write(request.stream_id.clone(), request.now_ms, true)
                    .await
                {
                    Ok(false) => {}
                    Ok(true) | Err(_) => {
                        if let Some(route) = self.leader_route().await {
                            return Ok(leader_read(route, placement, request));
                        }
                        self.require_local_leader_for_read("read_stream").await?;
                    }
                }
            }
            let stream_id = request.stream_id.clone();
            // The barrier below for what may hold only for a lagging state,
            // which this check's 404 is too.
            let linearize = !original_request.leader_only
                && !owner_pinned
                && !ursula_stream::sim_controls::off(
                    ursula_stream::sim_controls::Defence::ImmutableLeaderBarrier,
                );
            if self.raft.is_leader() {
                match self
                    .ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                    .await
                {
                    Ok(_) => {}
                    Err(error) if linearize && may_hold_only_for_a_lagging_state(&error) => {
                        self.require_linearizable_leader_read("read_stream", None)
                            .await?;
                        self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                            .await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            let mut plan = self.plan_read(&request).await?;
            // A leader answers a read that may hold only for a lagging state
            // (a 404, 410 or 416), and an immutable-read chunk it cannot
            // mark, only from state that reflects every acknowledged write
            // (#581 section 5.3): one that was just elected, or was deposed
            // without knowing it, may not have applied them yet. Its 404 or
            // 410 could be for the stream before a recreate, and its chunk
            // short of one another replica marked. A markable answer holds
            // committed bytes and needs no round.
            if self.raft.is_leader()
                && linearize
                && match &plan {
                    Ok(plan) => {
                        original_request.follower == FollowerRead::Immutable
                            && !immutable_read(
                                plan.offset,
                                plan.next_offset,
                                original_request.max_len,
                                ReadEnd::of(plan.up_to_date, plan.closed),
                            )
                    }
                    Err(error) => may_hold_only_for_a_lagging_state(error),
                }
            {
                self.require_linearizable_leader_read("read_stream", None)
                    .await?;
                plan = self.plan_read(&request).await?;
            }
            let plan = match plan {
                Ok(plan) => plan,
                // A follower may be behind a write that the leader has
                // already acknowledged. In that window, a read-after-write
                // cursor is beyond only the follower's local tail. Forward
                // instead of exposing a false permanent boundary error.
                Err(error)
                    if !owner_pinned
                        && !self.raft.is_leader()
                        && may_hold_only_for_a_lagging_state(&error) =>
                {
                    if let Some(route) = self.leader_route().await {
                        // A stale local boundary is not authoritative. Confirm
                        // the leader and its applied prefix before deciding
                        // whether this stream/cursor actually exists.
                        let mut authoritative_request = original_request;
                        authoritative_request.leader_only = true;
                        return Ok(leader_read(route, placement, authoritative_request));
                    }
                    return Err(self.not_leader_for_read("read_stream boundary").await);
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
            if owner_pinned && !self.raft.is_leader() {
                return Err(self.not_leader_for_read("live read_stream").await);
            }
            if !self.raft.is_leader() {
                match original_request.follower {
                    FollowerRead::Prefix => {
                        if parts.end() == ReadEnd::OpenTail {
                            if parts.payload_is_empty()
                                && let Some(route) = self.leader_route().await
                            {
                                return Ok(leader_read(route, placement, original_request));
                            }
                            parts.up_to_date = false;
                        }
                    }
                    // A follower answers an immutable-read chunk only when
                    // the answer is marked (#581 section 5.3): a chunk that
                    // reaches the follower's own open tail, or stops short of
                    // its end, may hold fewer bytes than the leader's, so the
                    // leader answers it. The leader never forwards.
                    FollowerRead::Immutable
                        if ursula_stream::sim_controls::off(
                            ursula_stream::sim_controls::Defence::FollowerMarkCheck,
                        ) =>
                    {
                        if parts.end() == ReadEnd::OpenTail {
                            parts.up_to_date = false;
                        }
                    }
                    FollowerRead::Immutable => {
                        if !parts.is_immutable(original_request.max_len) {
                            if let Some(route) = self.leader_route().await {
                                return Ok(leader_read(route, placement, original_request));
                            }
                            return Err(self.not_leader_for_read("immutable read_stream").await);
                        }
                    }
                }
            }
            Ok(GroupReadRoute::Local(parts))
        })
    }

    fn open_live_read<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> ursula_runtime::GroupOpenLiveReadFuture<'a> {
        Box::pin(async move {
            // #613: a replica holding an unsatisfied plan serves no client read.
            self.history
                .refuse_planned_read(&self.raft.metrics().borrow_watched())?;
            let read_index = self
                .linearizable_read_index("live_read", request.read_index)
                .await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            let head = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .engine
                            .head_stream_after_access(&request, placement)
                    })
                })
                .await??;
            Ok(ursula_runtime::LiveReadOwner {
                read_index: Some(read_index),
                head,
            })
        })
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
            let mut report = self.sweep_expired_streams(request).await;
            let candidates = self
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        Ok::<_, GroupEngineError>(
                            state_machine
                                .engine
                                .tidy_candidates(request.now_ms, request.max_streams),
                        )
                    })
                })
                .await??;
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

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            // #613: a replica holding an unsatisfied plan serves no client read.
            self.history
                .refuse_planned_read(&self.raft.metrics().borrow_watched())?;
            self.require_linearizable_leader_read("read_snapshot", request.read_index)
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
            // #613: a replica holding an unsatisfied plan serves no client read.
            self.history
                .refuse_planned_read(&self.raft.metrics().borrow_watched())?;
            self.require_linearizable_leader_read("bootstrap_stream", request.read_index)
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
                let route = self.leader_route().await;
                let leadership = GroupLeadership::of(&self.raft.metrics().borrow_watched());
                let Some(route) = route else {
                    return Err(leadership_refusal(
                        LeadershipReason::Follower,
                        Proposal::NotProposed,
                        leader_id,
                        None,
                        &leadership,
                    ));
                };
                return forward_purge_bucket_to_leader(&route, self.placement, bucket_id).await;
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
            if !self.raft.is_leader() {
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

    fn supports_append_batch(&self) -> bool {
        true
    }

    fn append_batch<'a>(
        &'a mut self,
        requests: Vec<AppendRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> ursula_runtime::GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader() {
                let mut results = Vec::with_capacity(requests.len());
                for request in requests {
                    results.push(self.append(request, placement, admission).await);
                }
                return results;
            }

            // Check the entire burst against applied hot bytes plus conservative
            // reservations for earlier accepted entries. Do not admit every item
            // against the same stale applied total.
            let count = requests.len();
            let checked = if admission.max_hot_bytes_per_group.is_none() {
                Ok((
                    requests.into_iter().map(GroupWriteCommand::from).collect(),
                    vec![Ok(()); count],
                ))
            } else {
                self.with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        let mut reserved = 0_u64;
                        let mut commands = Vec::new();
                        let mut slots = Vec::with_capacity(count);
                        for request in requests {
                            match state_machine.engine.check_cold_write_admission(
                                &request.stream_id,
                                admission,
                                request.payload_len().saturating_add(reserved),
                            ) {
                                Ok(()) => {
                                    reserved = reserved.saturating_add(request.payload_len());
                                    commands.push(GroupWriteCommand::from(request));
                                    slots.push(Ok(()));
                                }
                                Err(error) => slots.push(Err(error)),
                            }
                        }
                        (commands, slots)
                    })
                })
                .await
            };
            let (commands, slots) = match checked {
                Ok(checked) => checked,
                Err(error) => return vec![Err(error); count],
            };
            let responses =
                crate::forward::write_commands_on_raft(self.raft.clone(), &self.history, commands)
                    .await;
            let mut responses = match responses {
                Ok(responses) => responses.into_iter(),
                Err(error) => {
                    return slots
                        .into_iter()
                        .map(|slot| slot.and(Err(error.clone())))
                        .collect();
                }
            };
            slots
                .into_iter()
                .map(|slot| {
                    slot?;
                    match responses.next() {
                        Some(Ok(GroupWriteResponse::Append(response))) => Ok(response),
                        Some(Err(error)) => Err(error),
                        _ => Err(GroupEngineError::new("missing append batch response")),
                    }
                })
                .collect()
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
            // F5: commit first, index after. Apply keeps the locator in
            // state and the offload pass writes the page entry once the
            // append committed.
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
                let generation = self
                    .local_cold_index_generation(request.stream_id.clone())
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
                    // F14e: a refusal before proposal means the write was never
                    // proposed (RT1: OpenRaft's own forward may follow a
                    // committed entry), and a typed stream error means apply
                    // rejected it (a stale flush), so the page entry is
                    // definitely unreferenced. Other failures are ambiguous:
                    // the flush may still commit, so the entry stays.
                    let rollback_safe = err.applied_nothing();
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
                let generation = self
                    .local_cold_index_generation(request.stream_id.clone())
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
                    // A refusal before proposal means the write was never
                    // proposed (RT1). A typed stream error was committed but the
                    // state machine rejected it without enqueueing GC. Other
                    // Raft failures have an ambiguous commit outcome, so keep
                    // the replacement index rather than risk restoring inputs
                    // that a committed GC command will later delete.
                    let rollback_safe = err.applied_nothing();
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

pub(crate) fn group_engine_io_error(err: ursula_runtime::GroupEngineError) -> io::Error {
    // OpenRaft requires io::Error; retain the typed application source.
    io::Error::other(err)
}

pub(crate) fn invalid_data(err: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
