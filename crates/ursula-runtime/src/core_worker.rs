use std::collections::HashMap;
use std::sync::Arc;

use crate::rt::time::Instant;
use ursula_shard::{BucketStreamId, CoreId, RaftGroupId, ShardPlacement};
use ursula_stream::{ColdFlushCandidate, ColdGcEntry};

use crate::admission::{
    RaftUncommittedAdmission, SharedRaftUncommittedBytes, UncommittedBytesGuard,
};
use crate::command::GroupSnapshot;
use crate::engine::{
    GroupEngine, GroupEngineError, GroupEngineFactory, GroupEngineMetrics, GroupWriteResponse,
};
use crate::error::RuntimeError;
use crate::group_actor::{
    AppendBatchEntry, GroupActor, GroupCommand, GroupMailbox, PendingAppendBatch,
};
use crate::metrics::{
    RuntimeMetricsInner, append_batch_payload_bytes, elapsed_ns, record_cold_backpressure_error,
    record_cold_hot_backlog,
};
use crate::request::{
    AckColdGcResponse, AppendBatchRequest, AppendBatchResponse, AppendExternalRequest,
    AppendRequest, AppendResponse, BootstrapStreamRequest, BootstrapStreamResponse,
    CloseStreamRequest, CloseStreamResponse, ColdWriteAdmission, CreateStreamExternalRequest,
    CreateStreamRequest, CreateStreamResponse, DeleteSnapshotRequest, DeleteStreamRequest,
    DeleteStreamResponse, FlushColdRequest, FlushColdResponse, ForkRefResponse,
    GroupReadStreamParts, HeadStreamRequest, HeadStreamResponse, PlanColdFlushRequest,
    PlanGroupColdFlushRequest, PublishSnapshotRequest, PublishSnapshotResponse,
    ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest, ReadStreamResponse,
};
use crate::rt::sync::{Semaphore, mpsc, oneshot};

#[derive(Debug, Clone)]
pub(crate) struct CoreMailbox {
    pub(crate) core_id: CoreId,
    pub(crate) tx: mpsc::Sender<CoreCommand>,
}

impl CoreMailbox {
    pub(crate) fn depth(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}

pub(crate) enum CoreCommand {
    CreateStream {
        request: CreateStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    CreateExternal {
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    HeadStream {
        request: HeadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<HeadStreamResponse, RuntimeError>>,
    },
    ReadStream {
        request: ReadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    PublishSnapshot {
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<PublishSnapshotResponse, RuntimeError>>,
    },
    ReadSnapshot {
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ReadSnapshotResponse, RuntimeError>>,
    },
    DeleteSnapshot {
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    BootstrapStream {
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<BootstrapStreamResponse, RuntimeError>>,
    },
    WaitRead {
        request: ReadStreamRequest,
        placement: ShardPlacement,
        waiter_id: u64,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    RequireLiveReadOwner {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    CancelWaitRead {
        stream_id: BucketStreamId,
        placement: ShardPlacement,
        waiter_id: u64,
    },
    CloseStream {
        request: CloseStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<CloseStreamResponse, RuntimeError>>,
    },
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    DeleteStream {
        request: DeleteStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<DeleteStreamResponse, RuntimeError>>,
    },
    FlushCold {
        request: FlushColdRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<FlushColdResponse, RuntimeError>>,
    },
    PlanColdFlush {
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlush {
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlushBatch {
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
        response_tx: oneshot::Sender<Result<Vec<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanColdGc {
        max: usize,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<Vec<ColdGcEntry>, RuntimeError>>,
    },
    AckColdGc {
        up_to_seq: u64,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AckColdGcResponse, RuntimeError>>,
    },
    Append {
        request: AppendRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendExternal {
        request: AppendExternalRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendBatch {
        request: AppendBatchRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    },
    WarmGroup {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ShardPlacement, RuntimeError>>,
    },
    SnapshotGroup {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<GroupSnapshot, RuntimeError>>,
    },
    InstallGroupSnapshot {
        snapshot: GroupSnapshot,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    #[cfg(madsim)]
    ShutdownGroupEngine {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    #[cfg(madsim)]
    InstallGroupEngine {
        placement: ShardPlacement,
        engine: Box<dyn GroupEngine>,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

pub(crate) struct CoreWorker {
    pub(crate) core_id: CoreId,
    pub(crate) rx: mpsc::Receiver<CoreCommand>,
    pub(crate) engine_factory: Arc<dyn GroupEngineFactory>,
    pub(crate) groups: HashMap<RaftGroupId, GroupMailbox>,
    pub(crate) metrics: Arc<RuntimeMetricsInner>,
    pub(crate) group_mailbox_capacity: usize,
    pub(crate) cold_write_admission: ColdWriteAdmission,
    pub(crate) raft_uncommitted_admission: RaftUncommittedAdmission,
    pub(crate) raft_uncommitted_bytes: SharedRaftUncommittedBytes,
    pub(crate) live_read_max_waiters_per_core: Option<u64>,
    pub(crate) read_materialization: Arc<Semaphore>,
}

#[derive(Clone)]
pub(crate) struct AppendBatchRuntime {
    pub(crate) metrics: Arc<RuntimeMetricsInner>,
    pub(crate) read_materialization: Arc<Semaphore>,
    pub(crate) placement: ShardPlacement,
}

pub(crate) type ReadWatchers = HashMap<BucketStreamId, Vec<ReadWatcher>>;

pub(crate) struct ReadWatcher {
    pub(crate) waiter_id: u64,
    pub(crate) request: ReadStreamRequest,
    pub(crate) response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
}

fn live_read_watcher_count(read_watchers: &HashMap<BucketStreamId, Vec<ReadWatcher>>) -> u64 {
    read_watchers
        .values()
        .map(|watchers| u64::try_from(watchers.len()).expect("watcher count fits u64"))
        .sum()
}

pub(crate) struct WaitReadCancel {
    tx: mpsc::Sender<CoreCommand>,
    stream_id: Option<BucketStreamId>,
    placement: ShardPlacement,
    waiter_id: u64,
}

impl WaitReadCancel {
    pub(crate) fn new(
        tx: mpsc::Sender<CoreCommand>,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
        waiter_id: u64,
    ) -> Self {
        Self {
            tx,
            stream_id: Some(stream_id),
            placement,
            waiter_id,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.stream_id = None;
    }
}

impl Drop for WaitReadCancel {
    fn drop(&mut self) {
        if let Some(stream_id) = self.stream_id.take() {
            // Drop cannot await. If the owner mailbox is full, the stale
            // waiter is still removed when the next stream notification
            // consumes the closed oneshot sender.
            let _ = self.tx.try_send(CoreCommand::CancelWaitRead {
                stream_id,
                placement: self.placement,
                waiter_id: self.waiter_id,
            });
        }
    }
}

impl CoreWorker {
    pub(crate) async fn run(mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                CoreCommand::CreateStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    let incoming_bytes =
                        u64::try_from(request.initial_payload.len()).expect("payload len fits u64");
                    if let Some(err) = self.early_cold_backpressure(placement, incoming_bytes) {
                        let _ = response_tx.send(Err(err));
                        continue;
                    }
                    if let Some(err) =
                        self.early_raft_uncommitted_backpressure(placement, incoming_bytes)
                    {
                        let _ = response_tx.send(Err(err));
                        continue;
                    }
                    let raft_uncommitted =
                        self.acquire_raft_uncommitted_guard(placement, incoming_bytes);
                    self.send_group_command(
                        placement,
                        GroupCommand::CreateStream {
                            request,
                            response_tx,
                            raft_uncommitted,
                        },
                    )
                    .await;
                }
                CoreCommand::CreateExternal {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CreateExternal {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::HeadStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::HeadStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::ReadStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::ReadStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PublishSnapshot {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PublishSnapshot {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::ReadSnapshot {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::ReadSnapshot {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::DeleteSnapshot {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::DeleteSnapshot {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::BootstrapStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::BootstrapStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::WaitRead {
                    request,
                    placement,
                    waiter_id,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::WaitRead {
                            request,
                            waiter_id,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::RequireLiveReadOwner {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::RequireLiveReadOwner { response_tx },
                    )
                    .await;
                }
                CoreCommand::CancelWaitRead {
                    stream_id,
                    placement,
                    waiter_id,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CancelWaitRead {
                            stream_id,
                            waiter_id,
                        },
                    )
                    .await;
                }
                CoreCommand::CloseStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CloseStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::AddForkRef {
                    stream_id,
                    now_ms,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::AddForkRef {
                            stream_id,
                            now_ms,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::ReleaseForkRef {
                    stream_id,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::ReleaseForkRef {
                            stream_id,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::DeleteStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::DeleteStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::FlushCold {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::FlushCold {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanColdFlush {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanColdFlush {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanNextColdFlush {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanNextColdFlush {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanNextColdFlushBatch {
                    request,
                    placement,
                    max_candidates,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanNextColdFlushBatch {
                            request,
                            max_candidates,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanColdGc {
                    max,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanColdGc { max, response_tx },
                    )
                    .await;
                }
                CoreCommand::AckColdGc {
                    up_to_seq,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::AckColdGc {
                            up_to_seq,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::Append {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    let incoming_bytes = request.payload_len();
                    if let Some(err) = self.early_cold_backpressure(placement, incoming_bytes) {
                        let _ = response_tx.send(Err(err));
                        continue;
                    }
                    if let Some(err) =
                        self.early_raft_uncommitted_backpressure(placement, incoming_bytes)
                    {
                        let _ = response_tx.send(Err(err));
                        continue;
                    }
                    let raft_uncommitted =
                        self.acquire_raft_uncommitted_guard(placement, incoming_bytes);
                    self.send_group_command(
                        placement,
                        GroupCommand::Append {
                            request,
                            response_tx,
                            raft_uncommitted,
                        },
                    )
                    .await;
                }
                CoreCommand::AppendExternal {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::AppendExternal {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::AppendBatch {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    let incoming_bytes = append_batch_payload_bytes(&request);
                    if let Some(err) = self.early_cold_backpressure(placement, incoming_bytes) {
                        let _ = response_tx.send(Err(err));
                        continue;
                    }
                    if let Some(err) =
                        self.early_raft_uncommitted_backpressure(placement, incoming_bytes)
                    {
                        let _ = response_tx.send(Err(err));
                        continue;
                    }
                    let raft_uncommitted =
                        self.acquire_raft_uncommitted_guard(placement, incoming_bytes);
                    self.send_group_command(
                        placement,
                        GroupCommand::AppendBatch {
                            request,
                            response_tx,
                            raft_uncommitted,
                        },
                    )
                    .await;
                }
                CoreCommand::WarmGroup {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    let response = self.group(placement).await.map(|_| placement);
                    let _ = response_tx.send(response);
                }
                CoreCommand::SnapshotGroup {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(placement, GroupCommand::SnapshotGroup { response_tx })
                        .await;
                }
                CoreCommand::InstallGroupSnapshot {
                    snapshot,
                    response_tx,
                } => {
                    debug_assert_eq!(snapshot.placement.core_id, self.core_id);
                    self.send_group_command(
                        snapshot.placement,
                        GroupCommand::InstallGroupSnapshot {
                            snapshot,
                            response_tx,
                        },
                    )
                    .await;
                }
                #[cfg(madsim)]
                CoreCommand::ShutdownGroupEngine {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.shutdown_group_engine(placement, response_tx).await;
                }
                #[cfg(madsim)]
                CoreCommand::InstallGroupEngine {
                    placement,
                    engine,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    let response = self.install_group_engine(placement, engine).await;
                    let _ = response_tx.send(response);
                }
            }
        }
    }

    fn early_cold_backpressure(
        &self,
        placement: ShardPlacement,
        incoming_bytes: u64,
    ) -> Option<RuntimeError> {
        let limit = self.cold_write_admission.max_hot_bytes_per_group?;
        let current_hot_bytes = self
            .metrics
            .cold_hot_bytes_for_group(placement.raft_group_id);
        if current_hot_bytes.saturating_add(incoming_bytes) <= limit {
            return None;
        }
        self.metrics.record_cold_backpressure(
            placement.core_id,
            placement.raft_group_id,
            incoming_bytes,
            limit,
        );
        Some(RuntimeError::GroupEngine {
            core_id: placement.core_id,
            raft_group_id: placement.raft_group_id,
            message: format!(
                "ColdBackpressure: group hot bytes {current_hot_bytes} plus incoming {incoming_bytes} would exceed limit {limit}"
            ),
            next_offset: None,
            leader_hint: None,
        })
    }

    fn early_raft_uncommitted_backpressure(
        &self,
        placement: ShardPlacement,
        incoming_bytes: u64,
    ) -> Option<RuntimeError> {
        let limit = self
            .raft_uncommitted_admission
            .max_uncommitted_bytes_per_group?;
        let current = self.raft_uncommitted_bytes.load(placement.raft_group_id);
        if current.saturating_add(incoming_bytes) <= limit {
            return None;
        }
        Some(RuntimeError::GroupEngine {
            core_id: placement.core_id,
            raft_group_id: placement.raft_group_id,
            message: format!(
                "RaftUncommittedBackpressure: group uncommitted bytes {current} plus incoming {incoming_bytes} would exceed limit {limit}"
            ),
            next_offset: None,
            leader_hint: None,
        })
    }

    /// Acquire a guard that credits `incoming_bytes` to this group's
    /// uncommitted-bytes counter for as long as the guard lives. Returns
    /// `None` when the admission is disabled, in which case the credit
    /// counter is irrelevant.
    fn acquire_raft_uncommitted_guard(
        &self,
        placement: ShardPlacement,
        incoming_bytes: u64,
    ) -> Option<UncommittedBytesGuard> {
        if !self.raft_uncommitted_admission.is_enabled() {
            return None;
        }
        Some(UncommittedBytesGuard::new(
            self.raft_uncommitted_bytes.clone(),
            placement.raft_group_id,
            incoming_bytes,
        ))
    }

    pub(crate) async fn send_group_command(
        &mut self,
        placement: ShardPlacement,
        command: GroupCommand,
    ) {
        let core_id = placement.core_id;
        match self.group(placement).await {
            Ok(group) => {
                if let Err(command) = group.send(command).await {
                    (*command).send_error(RuntimeError::MailboxClosed { core_id });
                }
            }
            Err(err) => command.send_error(err),
        }
    }

    pub(crate) async fn group(
        &mut self,
        placement: ShardPlacement,
    ) -> Result<GroupMailbox, RuntimeError> {
        if !self.groups.contains_key(&placement.raft_group_id) {
            let engine_factory = self.engine_factory.clone();
            let metrics = GroupEngineMetrics {
                inner: self.metrics.clone(),
            };
            let engine = engine_factory
                .create(placement, metrics)
                .await
                .map_err(|err| RuntimeError::group_engine(placement, err))?;
            let (tx, rx) = mpsc::channel(self.group_mailbox_capacity);
            let actor = GroupActor {
                placement,
                engine,
                rx,
                read_watchers: HashMap::new(),
                metrics: self.metrics.clone(),
                cold_write_admission: self.cold_write_admission,
                live_read_max_waiters_per_core: self.live_read_max_waiters_per_core,
                read_materialization: self.read_materialization.clone(),
            };
            crate::rt::spawn(actor.run());
            self.groups.insert(
                placement.raft_group_id,
                GroupMailbox {
                    group_id: placement.raft_group_id,
                    tx,
                    metrics: self.metrics.clone(),
                },
            );
        }
        Ok(self
            .groups
            .get(&placement.raft_group_id)
            .expect("group was just inserted")
            .clone())
    }

    #[cfg(madsim)]
    pub(crate) async fn shutdown_group_engine(
        &mut self,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    ) {
        let Some(group) = self.groups.remove(&placement.raft_group_id) else {
            let _ = response_tx.send(Ok(()));
            return;
        };
        if let Err(command) = group
            .send(GroupCommand::ShutdownEngine { response_tx })
            .await
        {
            (*command).send_error(RuntimeError::MailboxClosed {
                core_id: placement.core_id,
            });
        }
    }

    #[cfg(madsim)]
    pub(crate) async fn install_group_engine(
        &mut self,
        placement: ShardPlacement,
        engine: Box<dyn GroupEngine>,
    ) -> Result<(), RuntimeError> {
        if self.groups.contains_key(&placement.raft_group_id) {
            return Err(RuntimeError::GroupEngine {
                core_id: placement.core_id,
                raft_group_id: placement.raft_group_id,
                message: "group engine already installed".to_owned(),
                next_offset: None,
                leader_hint: None,
            });
        }
        let (tx, rx) = mpsc::channel(self.group_mailbox_capacity);
        let actor = GroupActor {
            placement,
            engine,
            rx,
            read_watchers: HashMap::new(),
            metrics: self.metrics.clone(),
            cold_write_admission: self.cold_write_admission,
            live_read_max_waiters_per_core: self.live_read_max_waiters_per_core,
            read_materialization: self.read_materialization.clone(),
        };
        crate::rt::spawn(actor.run());
        self.groups.insert(
            placement.raft_group_id,
            GroupMailbox {
                group_id: placement.raft_group_id,
                tx,
                metrics: self.metrics.clone(),
            },
        );
        Ok(())
    }

    pub(crate) async fn read_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        request: ReadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    ) {
        let exec_started_at = Instant::now();
        let parts = group
            .read_stream_parts(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        match parts {
            Ok(parts) => {
                Self::send_read_parts_response(placement, read_materialization, parts, response_tx);
            }
            Err(err) => {
                let _ = response_tx.send(Err(err));
            }
        }
    }

    pub(crate) fn send_read_parts_response(
        placement: ShardPlacement,
        read_materialization: Arc<Semaphore>,
        parts: GroupReadStreamParts,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    ) {
        crate::rt::spawn(async move {
            let response = match read_materialization.acquire_owned().await {
                Ok(_permit) => parts
                    .into_response()
                    .await
                    .map_err(|err| RuntimeError::group_engine(placement, err)),
                Err(_) => Err(RuntimeError::MailboxClosed {
                    core_id: placement.core_id,
                }),
            };
            let _ = response_tx.send(response);
        });
    }

    pub(crate) fn send_read_parts_to_watchers(
        placement: ShardPlacement,
        read_materialization: Arc<Semaphore>,
        parts: GroupReadStreamParts,
        watchers: Vec<ReadWatcher>,
    ) {
        crate::rt::spawn(async move {
            let response = match read_materialization.acquire_owned().await {
                Ok(_permit) => parts
                    .into_response()
                    .await
                    .map_err(|err| RuntimeError::group_engine(placement, err)),
                Err(_) => Err(RuntimeError::MailboxClosed {
                    core_id: placement.core_id,
                }),
            };
            for watcher in watchers {
                let _ = watcher.response_tx.send(response.clone());
            }
        });
    }

    pub(crate) async fn publish_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<PublishSnapshotResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .publish_snapshot(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    pub(crate) async fn read_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<ReadSnapshotResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .read_snapshot(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn delete_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<(), RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .delete_snapshot(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn bootstrap_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> Result<BootstrapStreamResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .bootstrap_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn wait_read_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        placement: ShardPlacement,
        watcher: ReadWatcher,
        live_read_max_waiters_per_core: Option<u64>,
    ) {
        let exec_started_at = Instant::now();
        let parts = group
            .read_stream_parts(watcher.request.clone(), placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        match parts {
            Ok(parts) if parts.payload_is_empty() && parts.up_to_date && !parts.closed => {
                if watcher.response_tx.is_closed() {
                    return;
                }
                let current_waiters = live_read_watcher_count(read_watchers);
                if let Some(limit) = live_read_max_waiters_per_core
                    && current_waiters >= limit
                {
                    metrics.record_live_read_backpressure(placement.core_id);
                    let _ = watcher
                        .response_tx
                        .send(Err(RuntimeError::LiveReadBackpressure {
                            core_id: placement.core_id,
                            current_waiters,
                            limit,
                        }));
                    return;
                }
                metrics.record_read_watcher_added(placement.core_id);
                read_watchers
                    .entry(watcher.request.stream_id.clone())
                    .or_default()
                    .push(watcher);
            }
            Ok(parts) => {
                Self::send_read_parts_response(
                    placement,
                    read_materialization.clone(),
                    parts,
                    watcher.response_tx,
                );
            }
            Err(err) => {
                let _ = watcher.response_tx.send(Err(err));
            }
        }
    }

    pub(crate) fn cancel_read_watcher(
        read_watchers: &mut ReadWatchers,
        metrics: Arc<RuntimeMetricsInner>,
        core_id: CoreId,
        stream_id: BucketStreamId,
        waiter_id: u64,
    ) {
        let Some(watchers) = read_watchers.get_mut(&stream_id) else {
            return;
        };
        let before = watchers.len();
        watchers.retain(|watcher| watcher.waiter_id != waiter_id);
        let removed = before - watchers.len();
        let is_empty = watchers.is_empty();
        if removed > 0 {
            metrics.record_read_watchers_removed(core_id, removed);
        }
        if is_empty {
            read_watchers.remove(&stream_id);
        }
    }

    pub(crate) async fn close_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> Result<CloseStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .close_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response
            .as_ref()
            .is_ok_and(|response| !response.deduplicated)
        {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    pub(crate) async fn add_fork_ref(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .add_fork_ref(stream_id, now_ms, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
        }
        response
    }

    pub(crate) async fn release_fork_ref(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .release_fork_ref(stream_id.clone(), placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    pub(crate) async fn delete_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> Result<DeleteStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .delete_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    pub(crate) async fn flush_cold(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .flush_cold(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    pub(crate) async fn plan_cold_gc(
        group: &mut Box<dyn GroupEngine>,
        max: usize,
        placement: ShardPlacement,
    ) -> Result<Vec<ColdGcEntry>, RuntimeError> {
        // GC is leader-side side-effecting work: only the local leader reclaims
        // and acks, mirroring the cold-flush planner's leadership gate.
        if !group.accepts_local_writes() {
            return Ok(Vec::new());
        }
        group
            .plan_cold_gc(max, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))
    }

    pub(crate) async fn ack_cold_gc(
        group: &mut Box<dyn GroupEngine>,
        up_to_seq: u64,
        placement: ShardPlacement,
    ) -> Result<AckColdGcResponse, RuntimeError> {
        group
            .ack_cold_gc(up_to_seq, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))
    }

    pub(crate) async fn plan_cold_flush(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .plan_cold_flush(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn plan_next_cold_flush(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        if !group.accepts_local_writes() {
            return Ok(None);
        }
        let exec_started_at = Instant::now();
        let response = group
            .plan_next_cold_flush(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn plan_next_cold_flush_batch(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, RuntimeError> {
        if !group.accepts_local_writes() {
            return Ok(Vec::new());
        }
        let exec_started_at = Instant::now();
        let response = group
            .plan_next_cold_flush_batch(request, placement, max_candidates)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn head_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<HeadStreamResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .head_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn snapshot_group(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        placement: ShardPlacement,
    ) -> Result<GroupSnapshot, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .snapshot(placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn install_group_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        snapshot: GroupSnapshot,
    ) -> Result<(), RuntimeError> {
        let placement = snapshot.placement;
        let exec_started_at = Instant::now();
        let response = group
            .install_snapshot(snapshot)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn create_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let incoming_bytes =
            u64::try_from(request.initial_payload.len()).expect("payload len fits u64");
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .create_stream_with_cold_admission(request, placement, admission)
            .await
            .map_err(|err| {
                record_cold_backpressure_error(
                    &metrics,
                    placement,
                    incoming_bytes,
                    admission,
                    &err,
                );
                RuntimeError::group_engine(placement, err)
            })?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if !response.already_exists {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id, placement).await;
        }
        Ok(response)
    }

    pub(crate) async fn create_stream_external(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .create_stream_external(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if !response.already_exists {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id, placement).await;
        }
        Ok(response)
    }

    pub(crate) async fn apply_append(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<AppendResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let incoming_bytes = request.payload_len();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .append_with_cold_admission(request, placement, admission)
            .await
            .map_err(|err| {
                record_cold_backpressure_error(
                    &metrics,
                    placement,
                    incoming_bytes,
                    admission,
                    &err,
                );
                RuntimeError::group_engine(placement, err)
            })?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );

        if !response.deduplicated {
            metrics.record_append(placement.core_id, placement.raft_group_id);
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        Ok(response)
    }

    pub(crate) async fn apply_append_external(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> Result<AppendResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .append_external(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );

        if !response.deduplicated {
            metrics.record_append(placement.core_id, placement.raft_group_id);
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        Ok(response)
    }

    pub(crate) fn prepare_append_batch_requests(
        batch: Vec<AppendBatchEntry>,
    ) -> (Vec<AppendBatchRequest>, Vec<PendingAppendBatch>) {
        let mut requests = Vec::with_capacity(batch.len());
        let mut pending = Vec::with_capacity(batch.len());
        for (request, response_tx, raft_uncommitted) in batch {
            pending.push(PendingAppendBatch {
                stream_id: request.stream_id.clone(),
                incoming_bytes: append_batch_payload_bytes(&request),
                response_tx,
                started_at: Instant::now(),
                raft_uncommitted,
            });
            requests.push(request);
        }
        (requests, pending)
    }

    pub(crate) async fn apply_prepared_append_batch_requests(
        group: &mut Box<dyn GroupEngine>,
        runtime: AppendBatchRuntime,
        read_watchers: &mut ReadWatchers,
        pending: Vec<PendingAppendBatch>,
        requests: Vec<AppendBatchRequest>,
        admission: ColdWriteAdmission,
    ) {
        let exec_started_at = Instant::now();
        let responses = group
            .append_batch_many_with_cold_admission(requests, runtime.placement, admission)
            .await
            .map_err(|err| RuntimeError::group_engine(runtime.placement, err));
        runtime.metrics.record_group_engine_exec(
            runtime.placement.core_id,
            runtime.placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        Self::finish_append_batch_commands(
            group,
            runtime,
            read_watchers,
            pending,
            responses,
            admission,
        )
        .await;
    }

    pub(crate) async fn finish_append_batch_commands(
        group: &mut Box<dyn GroupEngine>,
        runtime: AppendBatchRuntime,
        read_watchers: &mut ReadWatchers,
        pending: Vec<PendingAppendBatch>,
        responses: Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, RuntimeError>,
        admission: ColdWriteAdmission,
    ) {
        let placement = runtime.placement;
        let responses = match responses {
            Ok(responses) => responses,
            Err(err) => {
                for pending in pending {
                    if admission.is_enabled()
                        && let RuntimeError::GroupEngine { message, .. } = &err
                        && message.contains("ColdBackpressure")
                    {
                        runtime.metrics.record_cold_backpressure(
                            placement.core_id,
                            placement.raft_group_id,
                            pending.incoming_bytes,
                            admission.max_hot_bytes_per_group.unwrap_or(0),
                        );
                    }
                    let _ = pending.response_tx.send(Err(err.clone()));
                }
                return;
            }
        };

        if responses.len() != pending.len() {
            let err = RuntimeError::GroupEngine {
                core_id: placement.core_id,
                raft_group_id: placement.raft_group_id,
                message: format!(
                    "batched append response count {} does not match request count {}",
                    responses.len(),
                    pending.len()
                ),
                next_offset: None,
                leader_hint: None,
            };
            for pending in pending {
                let _ = pending.response_tx.send(Err(err.clone()));
            }
            return;
        }

        for (pending, response) in pending.into_iter().zip(responses) {
            let response = match response {
                Ok(GroupWriteResponse::AppendBatch(response)) => Ok(response),
                Ok(other) => Err(RuntimeError::GroupEngine {
                    core_id: placement.core_id,
                    raft_group_id: placement.raft_group_id,
                    message: format!("unexpected batched append response: {other:?}"),
                    next_offset: None,
                    leader_hint: None,
                }),
                Err(err) => Err(RuntimeError::group_engine(placement, err)),
            };

            match response {
                Ok(response) => {
                    let success_count = response
                        .items
                        .iter()
                        .filter(|item| matches!(item, Ok(response) if !response.deduplicated))
                        .count();
                    if success_count > 0 {
                        let success_count = u64::try_from(success_count).expect("count fits u64");
                        runtime.metrics.record_append_batch(
                            placement.core_id,
                            placement.raft_group_id,
                            success_count,
                        );
                        runtime.metrics.record_applied_mutation_batch(
                            placement.core_id,
                            placement.raft_group_id,
                            success_count,
                            elapsed_ns(pending.started_at),
                        );
                        Self::notify_read_watchers(
                            group,
                            runtime.metrics.clone(),
                            runtime.read_materialization.clone(),
                            read_watchers,
                            &pending.stream_id,
                            placement,
                        )
                        .await;
                    }

                    let items = response
                        .items
                        .into_iter()
                        .map(|item| item.map_err(|err| RuntimeError::group_engine(placement, err)))
                        .collect();
                    let _ = pending
                        .response_tx
                        .send(Ok(AppendBatchResponse { placement, items }));
                }
                Err(err) => {
                    if admission.is_enabled()
                        && let RuntimeError::GroupEngine { message, .. } = &err
                        && message.contains("ColdBackpressure")
                    {
                        runtime.metrics.record_cold_backpressure(
                            placement.core_id,
                            placement.raft_group_id,
                            pending.incoming_bytes,
                            admission.max_hot_bytes_per_group.unwrap_or(0),
                        );
                    }
                    let _ = pending.response_tx.send(Err(err));
                }
            }
        }
    }

    pub(crate) async fn notify_read_watchers(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        stream_id: &BucketStreamId,
        placement: ShardPlacement,
    ) {
        let Some(watchers) = read_watchers.remove(stream_id) else {
            return;
        };
        metrics.record_read_watchers_removed(placement.core_id, watchers.len());

        let mut request_groups: Vec<(ReadStreamRequest, Vec<ReadWatcher>)> = Vec::new();
        for watcher in watchers {
            if let Some((_, grouped)) = request_groups
                .iter_mut()
                .find(|(request, _)| *request == watcher.request)
            {
                grouped.push(watcher);
            } else {
                request_groups.push((watcher.request.clone(), vec![watcher]));
            }
        }

        let mut pending = Vec::new();
        for (request, watchers) in request_groups {
            let parts = group
                .read_stream_parts(request, placement)
                .await
                .map_err(|err| RuntimeError::group_engine(placement, err));
            match parts {
                Ok(parts) if parts.payload_is_empty() && parts.up_to_date && !parts.closed => {
                    pending.extend(watchers);
                }
                Ok(parts) => {
                    Self::send_read_parts_to_watchers(
                        placement,
                        read_materialization.clone(),
                        parts,
                        watchers,
                    );
                }
                Err(err) => {
                    for watcher in watchers {
                        let _ = watcher.response_tx.send(Err(err.clone()));
                    }
                }
            }
        }

        if !pending.is_empty() {
            metrics.record_read_watchers_added(placement.core_id, pending.len());
            read_watchers
                .entry(stream_id.clone())
                .or_default()
                .extend(pending);
        }
    }
}
