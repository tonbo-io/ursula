use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use tracing::Instrument;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcPlanEntry;

use crate::admission::RaftUncommittedAdmission;
use crate::admission::SharedRaftUncommittedBytes;
use crate::admission::UncommittedBytesGuard;
use crate::cold_index::RepairColdIndexRequest;
use crate::cold_index::RepairColdIndexResponse;
use crate::cold_refs::ColdOrphanSweepPlan;
use crate::cold_refs::ColdOrphanSweepRequest;
use crate::command::GroupSnapshot;
use crate::engine::GroupEngine;
use crate::engine::GroupEngineError;
use crate::engine::GroupEngineFactory;
use crate::engine::GroupEngineMetrics;
use crate::engine::GroupLeaderReadFuture;
use crate::engine::GroupReadRoute;
use crate::error::RuntimeError;
use crate::group_actor::GroupActor;
use crate::group_actor::GroupCommand;
use crate::group_actor::GroupMailbox;
use crate::metrics::RuntimeMetricsInner;
use crate::metrics::elapsed_ns;
use crate::metrics::record_cold_backpressure_error;
use crate::metrics::record_write_hot_backlog;
use crate::read_index::ReadIndexBarriers;
use crate::request::AckColdGcResponse;
use crate::request::AdvanceRetentionRequest;
use crate::request::AdvanceRetentionResponse;
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
use crate::request::DeferColdGcResponse;
use crate::request::DeleteStreamRequest;
use crate::request::DeleteStreamResponse;
use crate::request::FlushColdRequest;
use crate::request::FlushColdResponse;
use crate::request::GroupReadStreamParts;
use crate::request::HeadStreamRequest;
use crate::request::HeadStreamResponse;
use crate::request::ImportGroupStateRequest;
use crate::request::ImportGroupStateResponse;
use crate::request::LiveReadOwner;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::PurgeBucketResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::ReadStreamResponse;
use crate::request::TidyStreamsRequest;
use crate::request::TidyStreamsResponse;
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
        self.tx.max_capacity().saturating_sub(self.tx.capacity())
    }

    pub(crate) fn capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}

/// Reply to a caller that may have stopped waiting (cancelled or timed out).
/// A dropped receiver makes the reply moot.
pub(crate) fn reply<T>(tx: oneshot::Sender<T>, value: T) {
    if tx.send(value).is_err() {
        tracing::trace!("caller stopped waiting before the reply");
    }
}

#[expect(
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
    /// Each started group's ReadIndex barrier, shared with `ShardRuntime`.
    pub(crate) read_barriers: ReadIndexBarriers,
}

pub(crate) type ReadWatchers = HashMap<BucketStreamId, Vec<ReadWatcher>>;

pub(crate) struct ReadWatcher {
    pub(crate) waiter_id: u64,
    pub(crate) request: ReadStreamRequest,
    /// The stream incarnation a live read with a `Stream-Incarnation`
    /// precondition was opened against (D12): a read of any other
    /// incarnation releases the watcher instead of parking it.
    pub(crate) incarnation: Option<u64>,
    pub(crate) response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
}

impl ReadWatcher {
    /// Whether a read of stream incarnation `incarnation` is still the
    /// stream this watcher waits on.
    fn waits_on(&self, incarnation: u64) -> bool {
        self.incarnation.is_none_or(|pinned| pinned == incarnation)
    }
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
            if self
                .tx
                .try_send(Traced::capture(CoreCommand::Group {
                    placement: self.placement,
                    admission: None,
                    command: GroupCommand::CancelWaitRead {
                        stream_id,
                        waiter_id: self.waiter_id,
                    },
                }))
                .is_err()
            {
                tracing::trace!(
                    "owner mailbox full; the stale waiter is removed on the next notification"
                );
            }
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
                reply(response_tx, response);
            }
            CoreCommand::WarmGroups {
                placements,
                response_tx,
            } => {
                let response = self.warm_groups(placements).await;
                reply(response_tx, response);
            }
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
                reply(response_tx, response);
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
            self.read_barriers
                .install(placement.raft_group_id, engine.linearizable_read_barrier());
            let (tx, rx) = mpsc::channel(self.group_mailbox_capacity);
            let actor = GroupActor {
                deferred: None,
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

    pub(crate) async fn shutdown_group_engine(
        &mut self,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    ) {
        let Some(group) = self.groups.remove(&placement.raft_group_id) else {
            reply(response_tx, Ok(()));
            return;
        };
        self.read_barriers.remove(placement.raft_group_id);
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
        self.read_barriers
            .install(placement.raft_group_id, engine.linearizable_read_barrier());
        let (tx, rx) = mpsc::channel(self.group_mailbox_capacity);
        let actor = GroupActor {
            deferred: None,
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
        let route = group
            .route_read_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        match route {
            Ok(GroupReadRoute::Local(parts)) => {
                Self::send_read_parts_response(placement, read_materialization, parts, response_tx);
            }
            Ok(GroupReadRoute::Leader(parts)) => {
                let response = Box::pin(async move { parts.await?.into_response().await });
                Self::send_leader_answer(placement, read_materialization, response, response_tx);
            }
            Err(err) => {
                reply(response_tx, Err(err));
            }
        }
    }

    /// Answers `response_tx` with the group leader's answer to a read this
    /// replica forwarded, outside the group actor, which goes on with its
    /// mailbox meanwhile. The forwarded RPC holds a read-materialization
    /// permit (the node's bound on read work that left its actor), ends at
    /// its own deadline, and is dropped once the caller stops waiting.
    fn send_leader_answer<T: Send + 'static>(
        placement: ShardPlacement,
        read_materialization: Arc<Semaphore>,
        answer: GroupLeaderReadFuture<T>,
        mut response_tx: oneshot::Sender<Result<T, RuntimeError>>,
    ) {
        crate::rt::spawn(async move {
            let answer = async move {
                let _permit = read_materialization
                    .acquire_owned()
                    .await
                    .map_err(|_closed| RuntimeError::MailboxClosed {
                        core_id: placement.core_id,
                    })?;
                answer
                    .await
                    .map_err(|err| RuntimeError::group_engine(placement, err))
            };
            let response = {
                let caller_gone = std::pin::pin!(response_tx.closed());
                match futures_util::future::select(caller_gone, std::pin::pin!(answer)).await {
                    futures_util::future::Either::Left(((), _answer)) => return,
                    futures_util::future::Either::Right((response, _caller_gone)) => response,
                }
            };
            reply(response_tx, response);
        });
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
            reply(response_tx, response);
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
                reply(watcher.response_tx, response.clone());
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
            record_write_hot_backlog(
                group,
                &metrics,
                response
                    .as_ref()
                    .ok()
                    .and_then(|response| response.hot_backlog),
                stream_id.clone(),
                placement,
            )
            .await;
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

    pub(crate) async fn advance_retention(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: AdvanceRetentionRequest,
        placement: ShardPlacement,
    ) -> Result<AdvanceRetentionResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .advance_retention(request, placement)
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
            record_write_hot_backlog(
                group,
                &metrics,
                response
                    .as_ref()
                    .ok()
                    .and_then(|response| response.hot_backlog),
                stream_id.clone(),
                placement,
            )
            .await;
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

    pub(crate) async fn import_group_state(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: ImportGroupStateRequest,
        placement: ShardPlacement,
    ) -> Result<ImportGroupStateResponse, RuntimeError> {
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .import_group_state(request, placement)
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
            Ok(parts)
                if parts.payload_is_empty()
                    && parts.up_to_date
                    && !parts.closed
                    && watcher.waits_on(parts.incarnation) =>
            {
                if watcher.response_tx.is_closed() {
                    return;
                }
                let current_waiters = live_read_watcher_count(read_watchers);
                if let Some(limit) = live_read_max_waiters_per_core
                    && current_waiters >= limit
                {
                    metrics.record_live_read_backpressure(placement.core_id);
                    reply(
                        watcher.response_tx,
                        Err(RuntimeError::LiveReadBackpressure {
                            core_id: placement.core_id,
                            current_waiters,
                            limit,
                        }),
                    );
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
                reply(watcher.response_tx, Err(err));
            }
        }
    }

    pub(crate) async fn open_live_read(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<LiveReadOwner, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .open_live_read(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    /// Plain leadership check with no quorum round trip, for background
    /// leader-side work (cold compaction) that does not serve a client read.
    pub(crate) async fn accepts_local_writes(
        group: &mut Box<dyn GroupEngine>,
    ) -> Result<bool, RuntimeError> {
        Ok(group.accepts_local_writes())
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
        let removed = before.saturating_sub(watchers.len());
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
            record_write_hot_backlog(
                group,
                &metrics,
                response
                    .as_ref()
                    .ok()
                    .and_then(|response| response.hot_backlog),
                stream_id.clone(),
                placement,
            )
            .await;
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
            record_write_hot_backlog(
                group,
                &metrics,
                response
                    .as_ref()
                    .ok()
                    .and_then(|response| response.hot_backlog),
                stream_id.clone(),
                placement,
            )
            .await;
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
    ) -> Result<Vec<ColdGcPlanEntry>, RuntimeError> {
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

    pub(crate) async fn repair_cold_index(
        group: &mut Box<dyn GroupEngine>,
        request: RepairColdIndexRequest,
        placement: ShardPlacement,
    ) -> Result<RepairColdIndexResponse, RuntimeError> {
        // Page repair rewrites pages, so only the local leader runs it, in
        // the group actor with every other page writer. A follower reports an
        // empty, finished step.
        if !group.accepts_local_writes() {
            return Ok(RepairColdIndexResponse::default());
        }
        group
            .repair_cold_index(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))
    }

    pub(crate) async fn plan_shared_ref_compaction(
        group: &mut Box<dyn GroupEngine>,
        request: ursula_stream::SharedRefCompactionRequest,
        placement: ShardPlacement,
    ) -> Result<Vec<ursula_stream::SharedRefCandidate>, RuntimeError> {
        // Compaction publishes replacements, so only the local leader plans.
        if !group.accepts_local_writes() {
            return Ok(Vec::new());
        }
        group
            .plan_shared_ref_compaction(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))
    }

    pub(crate) async fn plan_cold_orphan_sweep(
        group: &mut Box<dyn GroupEngine>,
        request: ColdOrphanSweepRequest,
        placement: ShardPlacement,
    ) -> Result<ColdOrphanSweepPlan, RuntimeError> {
        // The sweep deletes objects, so only the local leader plans one; a
        // follower answers with an empty, non-leader plan.
        if !group.accepts_local_writes() {
            return Ok(ColdOrphanSweepPlan::default());
        }
        group
            .plan_cold_orphan_sweep(request, placement)
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

    pub(crate) async fn defer_cold_gc(
        group: &mut Box<dyn GroupEngine>,
        seq: u64,
        not_before_ms: u64,
        placement: ShardPlacement,
    ) -> Result<DeferColdGcResponse, RuntimeError> {
        group
            .defer_cold_gc(seq, not_before_ms, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))
    }

    pub(crate) async fn purge_bucket(
        group: &mut Box<dyn GroupEngine>,
        bucket_id: String,
        placement: ShardPlacement,
    ) -> Result<PurgeBucketResponse, RuntimeError> {
        group
            .purge_bucket(bucket_id, placement)
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
            // The aggregate pressure gauge represents only groups currently
            // led by this node. Clear values left behind by leadership moves
            // so a former leader cannot keep activating empty pressure passes.
            metrics.record_cold_hot_backlog(placement.raft_group_id, 0, 0);
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

    pub(crate) async fn bucket_usage(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        placement: ShardPlacement,
    ) -> Result<Vec<ursula_stream::BucketUsageSnapshot>, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .bucket_usage(placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    pub(crate) async fn state_gauges(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        placement: ShardPlacement,
    ) -> Result<ursula_stream::GroupStateGauges, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .state_gauges(placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    /// One F5 external-locator offload pass. Offloading
    /// writes cold-index pages and proposes, so only the local leader runs
    /// it; a follower reports an empty pass.
    pub(crate) async fn offload_cold_refs(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: crate::cold_refs::OffloadColdRefsRequest,
        placement: ShardPlacement,
    ) -> Result<crate::cold_refs::OffloadColdRefsResponse, RuntimeError> {
        if !group.accepts_local_writes() {
            return Ok(crate::cold_refs::OffloadColdRefsResponse::default());
        }
        let started_at = Instant::now();
        let response = group
            .offload_cold_refs(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(started_at),
        );
        response
    }

    pub(crate) async fn tidy_streams(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: TidyStreamsRequest,
        placement: ShardPlacement,
    ) -> Result<TidyStreamsResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .tidy_streams(request, placement)
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
        read_materialization: Arc<Semaphore>,
        request: HeadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<HeadStreamResponse, RuntimeError>>,
    ) {
        let exec_started_at = Instant::now();
        let route = group
            .route_head_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        match route {
            Ok(GroupReadRoute::Local(head)) => reply(response_tx, Ok(head)),
            Ok(GroupReadRoute::Leader(head)) => {
                Self::send_leader_answer(placement, read_materialization, head, response_tx);
            }
            Err(err) => reply(response_tx, Err(err)),
        }
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
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
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
            record_write_hot_backlog(
                group,
                &metrics,
                response.hot_backlog,
                stream_id.clone(),
                placement,
            )
            .await;
            Self::notify_pinned_read_watchers(
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

    pub(crate) async fn create_stream_external(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
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
            record_write_hot_backlog(
                group,
                &metrics,
                response.hot_backlog,
                stream_id.clone(),
                placement,
            )
            .await;
            Self::notify_pinned_read_watchers(
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
        name = "core.append",
        level = "debug",
        skip_all,
        fields(
            group = placement.raft_group_id.0,
            bucket = %request.stream_id.bucket_id,
            stream = %request.stream_id.stream_id,
        ),
    )]
    pub(crate) async fn commit_append(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<AppendResponse, RuntimeError> {
        let incoming_bytes = request.payload_len();
        let started_at = Instant::now();
        let response = group.append(request, placement, admission).await;
        let elapsed = elapsed_ns(started_at);
        metrics.record_group_engine_exec(placement.core_id, placement.raft_group_id, elapsed);
        Self::record_append_result(
            &metrics,
            placement,
            incoming_bytes,
            admission,
            elapsed,
            response,
        )
    }

    pub(crate) fn record_append_result(
        metrics: &RuntimeMetricsInner,
        placement: ShardPlacement,
        incoming_bytes: u64,
        admission: ColdWriteAdmission,
        elapsed: u64,
        response: Result<AppendResponse, GroupEngineError>,
    ) -> Result<AppendResponse, RuntimeError> {
        let response = response.map_err(|error| {
            record_cold_backpressure_error(metrics, placement, incoming_bytes, admission, &error);
            RuntimeError::group_engine(placement, error)
        })?;
        if !response.deduplicated {
            metrics.record_append(placement.core_id, placement.raft_group_id);
            metrics.record_applied_mutation(placement.core_id, placement.raft_group_id, elapsed);
            metrics.record_cold_hot_backlog(
                placement.raft_group_id,
                response.stream_hot_bytes,
                response.group_hot_bytes,
            );
        }
        Ok(response)
    }

    pub(crate) async fn finish_append(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) {
        let started_at = Instant::now();
        Self::notify_read_watchers(
            group,
            metrics.clone(),
            read_materialization,
            read_watchers,
            &stream_id,
            placement,
        )
        .await;
        metrics.record_append_post_commit(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(started_at),
        );
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
            record_write_hot_backlog(
                group,
                &metrics,
                Some(crate::request::WriteHotBacklog {
                    stream_hot_bytes: response.stream_hot_bytes,
                    group_hot_bytes: response.group_hot_bytes,
                }),
                stream_id.clone(),
                placement,
            )
            .await;
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

    /// A create may replace an expired stream that live reads still wait
    /// on. Watchers pinned to an incarnation (D12) re-read and, the stream
    /// being another incarnation now, end; the others keep waiting exactly
    /// as before.
    pub(crate) async fn notify_pinned_read_watchers(
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
        let (pinned, unpinned): (Vec<_>, Vec<_>) = watchers
            .into_iter()
            .partition(|watcher| watcher.incarnation.is_some());
        if !pinned.is_empty() {
            read_watchers.insert(stream_id.clone(), pinned);
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                stream_id,
                placement,
            )
            .await;
        }
        if !unpinned.is_empty() {
            read_watchers
                .entry(stream_id.clone())
                .or_default()
                .extend(unpinned);
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
        let notify_started_at = Instant::now();
        metrics.record_read_watchers_removed(placement.core_id, watchers.len());

        let mut request_groups: Vec<(ReadStreamRequest, Vec<ReadWatcher>)> = Vec::new();
        for watcher in watchers {
            if let Some((request, grouped)) = request_groups
                .iter_mut()
                .find(|(request, _)| request.same_wait_plan(&watcher.request))
            {
                request.now_ms = request.now_ms.max(watcher.request.now_ms);
                grouped.push(watcher);
            } else {
                request_groups.push((watcher.request.clone(), vec![watcher]));
            }
        }

        let replan_count = request_groups.len();
        let mut pending = Vec::new();
        for (request, watchers) in request_groups {
            let parts = group
                .read_stream_parts(request, placement)
                .await
                .map_err(|err| RuntimeError::group_engine(placement, err));
            match parts {
                Ok(parts) if parts.payload_is_empty() && parts.up_to_date && !parts.closed => {
                    // A watcher pinned to another incarnation is released
                    // with this read: its stream was recreated (D12).
                    let (waiting, recreated): (Vec<_>, Vec<_>) = watchers
                        .into_iter()
                        .partition(|watcher| watcher.waits_on(parts.incarnation));
                    pending.extend(waiting);
                    if !recreated.is_empty() {
                        Self::send_read_parts_to_watchers(
                            placement,
                            read_materialization.clone(),
                            parts,
                            recreated,
                        );
                    }
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
                        reply(watcher.response_tx, Err(err.clone()));
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
        metrics.record_read_watcher_notify(
            placement.core_id,
            placement.raft_group_id,
            replan_count,
            elapsed_ns(notify_started_at),
        );
    }
}
