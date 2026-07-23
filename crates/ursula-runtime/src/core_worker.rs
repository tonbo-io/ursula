use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use tracing::Instrument;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcEntry;

use crate::admission::RaftUncommittedAdmission;
use crate::admission::SharedRaftUncommittedBytes;
use crate::admission::UncommittedBytesGuard;
use crate::command::GroupSnapshot;
use crate::engine::GroupEngine;
use crate::engine::GroupEngineError;
use crate::engine::GroupEngineFactory;
use crate::engine::GroupEngineMetrics;
use crate::engine::GroupWriteResponse;
use crate::error::RuntimeError;
use crate::group_actor::AppendBatchEntry;
use crate::group_actor::GroupActor;
use crate::group_actor::GroupCommand;
use crate::group_actor::GroupMailbox;
use crate::group_actor::PendingAppendBatch;
use crate::metrics::RuntimeMetricsInner;
use crate::metrics::append_batch_payload_bytes;
use crate::metrics::elapsed_ns;
use crate::metrics::record_cold_backpressure_error;
use crate::metrics::record_cold_hot_backlog;
use crate::request::AckColdGcResponse;
use crate::request::AppendBatchRequest;
use crate::request::AppendBatchResponse;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::AppendResponse;
use crate::request::BootstrapStreamRequest;
use crate::request::BootstrapStreamResponse;
use crate::request::CloseStreamRequest;
use crate::request::CloseStreamResponse;
use crate::request::ColdWriteAdmission;
use crate::request::CompactColdRequest;
use crate::request::CompactColdResponse;
use crate::request::CreateStreamExternalRequest;
use crate::request::CreateStreamRequest;
use crate::request::CreateStreamResponse;
use crate::request::DeleteSnapshotRequest;
use crate::request::DeleteStreamRequest;
use crate::request::DeleteStreamResponse;
use crate::request::FlushColdRequest;
use crate::request::FlushColdResponse;
use crate::request::GetStreamAttrsRequest;
use crate::request::GetStreamAttrsResponse;
use crate::request::GroupReadStreamParts;
use crate::request::HeadStreamRequest;
use crate::request::HeadStreamResponse;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::ReadStreamResponse;
use crate::request::UpdateStreamAttrsRequest;
use crate::request::UpdateStreamAttrsResponse;
use crate::rt::sync::Semaphore;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::time::Instant;
use crate::trace::Traced;

const WARM_GROUP_CONCURRENCY_PER_CORE: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct CoreMailbox {
    pub(crate) core_id: CoreId,
    pub(crate) tx: mpsc::Sender<Traced<CoreCommand>>,
}

impl CoreMailbox {
    pub(crate) fn depth(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "Group is the hot-path variant moved through the core mailbox; boxing it would add \
              a per-operation allocation to every append"
)]
pub(crate) enum CoreCommand {
    /// A group-actor command routed to its owning core. `admission` carries
    /// the incoming payload bytes for raft-uncommitted-backpressure-guarded
    /// writes; the early admission check and guard acquisition run on the
    /// owning core before the command is forwarded to the group mailbox.
    Group {
        placement: ShardPlacement,
        admission: Option<u64>,
        command: GroupCommand,
    },
    WarmGroup {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ShardPlacement, RuntimeError>>,
    },
    WarmGroups {
        placements: Vec<ShardPlacement>,
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
    pub(crate) rx: mpsc::Receiver<Traced<CoreCommand>>,
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
    tx: mpsc::Sender<Traced<CoreCommand>>,
    stream_id: Option<BucketStreamId>,
    placement: ShardPlacement,
    waiter_id: u64,
}

impl WaitReadCancel {
    pub(crate) fn new(
        tx: mpsc::Sender<Traced<CoreCommand>>,
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
            let _ = self.tx.try_send(Traced::capture(CoreCommand::Group {
                placement: self.placement,
                admission: None,
                command: GroupCommand::CancelWaitRead {
                    stream_id,
                    waiter_id: self.waiter_id,
                },
            }));
        }
    }
}

impl CoreWorker {
    pub(crate) async fn run(mut self) {
        while let Some(Traced {
            value: command,
            parent,
        }) = self.rx.recv().await
        {
            // Re-establish the sender's span so on-core work links back to the
            // originating request, then dispatch under it.
            self.dispatch(command).instrument(parent).await;
        }
    }

    async fn dispatch(&mut self, command: CoreCommand) {
        match command {
            CoreCommand::Group {
                placement,
                admission,
                command,
            } => {
                debug_assert_eq!(placement.core_id, self.core_id);
                let command = match admission {
                    Some(incoming_bytes) => {
                        if let Some(err) =
                            self.early_raft_uncommitted_backpressure(placement, incoming_bytes)
                        {
                            command.send_error(err);
                            return;
                        }
                        let raft_uncommitted =
                            self.acquire_raft_uncommitted_guard(placement, incoming_bytes);
                        command.with_raft_uncommitted(raft_uncommitted)
                    }
                    None => command,
                };
                self.send_group_command(placement, command).await;
            }
            CoreCommand::WarmGroup {
                placement,
                response_tx,
            } => {
                debug_assert_eq!(placement.core_id, self.core_id);
                let response = self.group(placement).await.map(|_| placement);
                let _ = response_tx.send(response);
            }
            CoreCommand::WarmGroups {
                placements,
                response_tx,
            } => {
                let response = self.warm_groups(placements).await;
                let _ = response_tx.send(response);
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
            error: GroupEngineError::raft_uncommitted_backpressure(current, incoming_bytes, limit),
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
            if !engine_factory.hosts_group(placement) {
                return Err(RuntimeError::GroupNotHosted {
                    core_id: placement.core_id,
                    raft_group_id: placement.raft_group_id,
                });
            }
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
            self.groups.insert(placement.raft_group_id, GroupMailbox {
                group_id: placement.raft_group_id,
                tx,
                metrics: self.metrics.clone(),
            });
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

    pub(crate) async fn install_group_engine(
        &mut self,
        placement: ShardPlacement,
        engine: Box<dyn GroupEngine>,
    ) -> Result<(), RuntimeError> {
        if self.groups.contains_key(&placement.raft_group_id) {
            return Err(RuntimeError::GroupEngine {
                core_id: placement.core_id,
                raft_group_id: placement.raft_group_id,
                error: GroupEngineError::new("group engine already installed"),
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
        self.groups.insert(placement.raft_group_id, GroupMailbox {
            group_id: placement.raft_group_id,
            tx,
            metrics: self.metrics.clone(),
        });
        Ok(())
    }

    async fn warm_groups(&mut self, placements: Vec<ShardPlacement>) -> Result<(), RuntimeError> {
        let placements = placements
            .into_iter()
            .filter(|placement| !self.groups.contains_key(&placement.raft_group_id))
            .collect::<Vec<_>>();
        let engine_factory = self.engine_factory.clone();
        let metrics = self.metrics.clone();
        let core_id = self.core_id;
        let mut engines = futures_util::stream::iter(placements)
            .map(|placement| {
                let engine_factory = engine_factory.clone();
                let metrics = metrics.clone();
                async move {
                    debug_assert_eq!(placement.core_id, core_id);
                    if !engine_factory.hosts_group(placement) {
                        return Err(RuntimeError::GroupNotHosted {
                            core_id: placement.core_id,
                            raft_group_id: placement.raft_group_id,
                        });
                    }
                    let engine = engine_factory
                        .create(placement, GroupEngineMetrics { inner: metrics })
                        .await
                        .map_err(|err| RuntimeError::group_engine(placement, err))?;
                    Ok((placement, engine))
                }
            })
            .buffer_unordered(WARM_GROUP_CONCURRENCY_PER_CORE);
        while let Some(engine) = engines.next().await {
            let (placement, engine) = engine?;
            self.install_group_engine(placement, engine).await?;
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "core.read",
        level = "debug",
        skip_all,
        fields(
            group = placement.raft_group_id.0,
            bucket = %request.stream_id.bucket_id,
            stream = %request.stream_id.stream_id,
            offset = request.offset,
        ),
    )]
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

    pub(crate) async fn require_live_read_owner(
        group: &mut Box<dyn GroupEngine>,
        placement: ShardPlacement,
    ) -> Result<(), RuntimeError> {
        group
            .require_local_live_read_owner(placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))
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

    #[tracing::instrument(
        name = "runtime.cold_flush",
        skip_all,
        fields(
            group = placement.raft_group_id.0,
            bucket = %request.stream_id.bucket_id,
            stream = %request.stream_id.stream_id,
            start_offset = request.chunk.start_offset,
            end_offset = request.chunk.end_offset,
            bytes = request.chunk.object_size,
        ),
    )]
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

    #[tracing::instrument(
        name = "runtime.cold_compact",
        skip_all,
        fields(
            group = placement.raft_group_id.0,
            bucket = %request.stream_id.bucket_id,
            stream = %request.stream_id.stream_id,
            chunks = request.old_chunks.len(),
            bytes = request.replacement.object_size,
        ),
    )]
    pub(crate) async fn compact_cold(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: CompactColdRequest,
        placement: ShardPlacement,
    ) -> Result<CompactColdResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .compact_cold(request, placement)
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

    pub(crate) async fn get_stream_attrs(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: GetStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> Result<GetStreamAttrsResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .get_stream_attrs(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn update_stream_attrs(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: UpdateStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> Result<UpdateStreamAttrsResponse, RuntimeError> {
        let started_at = Instant::now();
        let response = group
            .update_stream_attrs(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(started_at),
        );
        if response.changed {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
        }
        Ok(response)
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
            .create_stream(request, placement, admission)
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

    #[tracing::instrument(
        name = "core.append",
        level = "debug",
        skip_all,
        fields(
            group = placement.raft_group_id.0,
            bucket = %request.stream_id.bucket_id,
            stream = %request.stream_id.stream_id,
        ),
    )]
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
            .append(request, placement, admission)
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

    #[tracing::instrument(
        name = "core.append_external",
        level = "debug",
        skip_all,
        fields(
            group = placement.raft_group_id.0,
            bucket = %request.stream_id.bucket_id,
            stream = %request.stream_id.stream_id,
        ),
    )]
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

    // The `core.append_batch` span is created by the caller so it can link
    // (follows_from) every coalesced request, not just the triggering one.
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
            .append_batch_many(requests, runtime.placement, admission)
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
                        && let RuntimeError::GroupEngine { error, .. } = &err
                        && error.is_cold_backpressure()
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
                error: GroupEngineError::new(format!(
                    "batched append response count {} does not match request count {}",
                    responses.len(),
                    pending.len()
                )),
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
                    error: GroupEngineError::new(format!(
                        "unexpected batched append response: {other:?}"
                    )),
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
                        && let RuntimeError::GroupEngine { error, .. } = &err
                        && error.is_cold_backpressure()
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
