use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::Arc;

use tracing::Instrument;
use tracing::Span;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcEntry;

use crate::admission::UncommittedBytesGuard;
use crate::command::GroupSnapshot;
use crate::core_worker::AppendBatchRuntime;
use crate::core_worker::CoreWorker;
use crate::core_worker::ReadWatcher;
use crate::core_worker::ReadWatchers;
use crate::engine::GroupEngine;
use crate::error::RuntimeError;
use crate::metrics::GROUP_ACTOR_MAX_WRITE_BATCH;
use crate::metrics::RuntimeMetricsInner;
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

#[derive(Clone)]
pub(crate) struct GroupMailbox {
    pub(crate) group_id: RaftGroupId,
    pub(crate) tx: mpsc::Sender<Traced<GroupCommand>>,
    pub(crate) metrics: Arc<RuntimeMetricsInner>,
}

impl GroupMailbox {
    pub(crate) async fn send(&self, command: GroupCommand) -> Result<(), Box<GroupCommand>> {
        self.metrics.record_group_mailbox_enqueued(self.group_id);
        // Capture the sender's span so the group actor can re-parent the work.
        match self.tx.try_send(Traced::capture(command)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                self.metrics.record_group_mailbox_dequeued(self.group_id);
                self.metrics.record_group_mailbox_full(self.group_id);
                self.metrics.record_group_mailbox_enqueued(self.group_id);
                match self.tx.send(command).await {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        self.metrics.record_group_mailbox_dequeued(self.group_id);
                        Err(Box::new(err.0.value))
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                self.metrics.record_group_mailbox_dequeued(self.group_id);
                Err(Box::new(command.value))
            }
        }
    }
}

/// (request, response channel, optional raft-uncommitted credit) tuple
/// passed through the append-batch hot path. Aliased to keep clippy's
/// type-complexity lint happy.
pub(crate) type AppendBatchEntry = (
    AppendBatchRequest,
    oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    Option<UncommittedBytesGuard>,
);

pub(crate) struct PendingAppendBatch {
    pub(crate) stream_id: BucketStreamId,
    pub(crate) incoming_bytes: u64,
    pub(crate) response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    pub(crate) started_at: Instant,
    /// Released when this pending entry resolves (success or error), so the
    /// per-group raft uncommitted-bytes counter decrements once apply
    /// completes. The field is "unused" — its Drop is the load-bearing side
    /// effect.
    #[allow(dead_code)]
    pub(crate) raft_uncommitted: Option<UncommittedBytesGuard>,
}

pub(crate) enum GroupCommand {
    CreateStream {
        request: CreateStreamRequest,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
        raft_uncommitted: Option<UncommittedBytesGuard>,
    },
    CreateExternal {
        request: CreateStreamExternalRequest,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    HeadStream {
        request: HeadStreamRequest,
        response_tx: oneshot::Sender<Result<HeadStreamResponse, RuntimeError>>,
    },
    GetStreamAttrs {
        request: GetStreamAttrsRequest,
        response_tx: oneshot::Sender<Result<GetStreamAttrsResponse, RuntimeError>>,
    },
    ReadStream {
        request: ReadStreamRequest,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    PublishSnapshot {
        request: PublishSnapshotRequest,
        response_tx: oneshot::Sender<Result<PublishSnapshotResponse, RuntimeError>>,
    },
    ReadSnapshot {
        request: ReadSnapshotRequest,
        response_tx: oneshot::Sender<Result<ReadSnapshotResponse, RuntimeError>>,
    },
    DeleteSnapshot {
        request: DeleteSnapshotRequest,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    BootstrapStream {
        request: BootstrapStreamRequest,
        response_tx: oneshot::Sender<Result<BootstrapStreamResponse, RuntimeError>>,
    },
    WaitRead {
        request: ReadStreamRequest,
        waiter_id: u64,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    CancelWaitRead {
        stream_id: BucketStreamId,
        waiter_id: u64,
    },
    RequireLiveReadOwner {
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    CloseStream {
        request: CloseStreamRequest,
        response_tx: oneshot::Sender<Result<CloseStreamResponse, RuntimeError>>,
    },
    UpdateStreamAttrs {
        request: UpdateStreamAttrsRequest,
        response_tx: oneshot::Sender<Result<UpdateStreamAttrsResponse, RuntimeError>>,
    },
    DeleteStream {
        request: DeleteStreamRequest,
        response_tx: oneshot::Sender<Result<DeleteStreamResponse, RuntimeError>>,
    },
    FlushCold {
        request: FlushColdRequest,
        response_tx: oneshot::Sender<Result<FlushColdResponse, RuntimeError>>,
    },
    PlanColdFlush {
        request: PlanColdFlushRequest,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlushBatch {
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
        response_tx: oneshot::Sender<Result<Vec<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanColdGc {
        max: usize,
        response_tx: oneshot::Sender<Result<Vec<ColdGcEntry>, RuntimeError>>,
    },
    AckColdGc {
        up_to_seq: u64,
        response_tx: oneshot::Sender<Result<AckColdGcResponse, RuntimeError>>,
    },
    Append {
        request: AppendRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
        raft_uncommitted: Option<UncommittedBytesGuard>,
    },
    AppendExternal {
        request: AppendExternalRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendBatch {
        request: AppendBatchRequest,
        response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
        raft_uncommitted: Option<UncommittedBytesGuard>,
    },
    SnapshotGroup {
        response_tx: oneshot::Sender<Result<GroupSnapshot, RuntimeError>>,
    },
    InstallGroupSnapshot {
        snapshot: GroupSnapshot,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    #[cfg(madsim)]
    ShutdownEngine {
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

impl GroupCommand {
    pub(crate) fn send_error(self, err: RuntimeError) {
        match self {
            Self::CreateStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::CreateExternal { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::HeadStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::GetStreamAttrs { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::ReadStream { response_tx, .. } | Self::WaitRead { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::CancelWaitRead { .. } => {}
            Self::RequireLiveReadOwner { response_tx } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PublishSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::ReadSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::DeleteSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::BootstrapStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::CloseStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::UpdateStreamAttrs { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::DeleteStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::FlushCold { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanColdFlush { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanNextColdFlushBatch { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanColdGc { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::AckColdGc { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::Append { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::AppendExternal { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::AppendBatch { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::SnapshotGroup { response_tx } => {
                let _ = response_tx.send(Err(err));
            }
            Self::InstallGroupSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            #[cfg(madsim)]
            Self::ShutdownEngine { response_tx } => {
                let _ = response_tx.send(Err(err));
            }
        }
    }
}

pub(crate) struct GroupActor {
    pub(crate) placement: ShardPlacement,
    pub(crate) engine: Box<dyn GroupEngine>,
    pub(crate) rx: mpsc::Receiver<Traced<GroupCommand>>,
    pub(crate) read_watchers: ReadWatchers,
    pub(crate) metrics: Arc<RuntimeMetricsInner>,
    pub(crate) cold_write_admission: ColdWriteAdmission,
    pub(crate) live_read_max_waiters_per_core: Option<u64>,
    pub(crate) read_materialization: Arc<Semaphore>,
}

impl GroupActor {
    pub(crate) async fn run(mut self) {
        let mut pending = VecDeque::new();
        loop {
            let Some(Traced {
                value: command,
                parent,
            }) = self.next_command(&mut pending).await
            else {
                break;
            };
            // Re-establish the sender's span so on-core apply work links back to
            // the originating request.
            if self
                .handle(command, &mut pending)
                .instrument(parent)
                .await
                .is_break()
            {
                break;
            }
        }
    }

    async fn handle(
        &mut self,
        command: GroupCommand,
        pending: &mut VecDeque<Traced<GroupCommand>>,
    ) -> ControlFlow<()> {
        match command {
            GroupCommand::CreateStream {
                request,
                response_tx,
                raft_uncommitted,
            } => {
                let response = CoreWorker::create_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                    self.cold_write_admission,
                )
                .await;
                drop(raft_uncommitted);
                let _ = response_tx.send(response);
            }
            GroupCommand::CreateExternal {
                request,
                response_tx,
            } => {
                let response = CoreWorker::create_stream_external(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::HeadStream {
                request,
                response_tx,
            } => {
                let response = CoreWorker::head_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::GetStreamAttrs {
                request,
                response_tx,
            } => {
                let response = CoreWorker::get_stream_attrs(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::ReadStream {
                request,
                response_tx,
            } => {
                CoreWorker::read_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    request,
                    self.placement,
                    response_tx,
                )
                .await;
            }
            GroupCommand::PublishSnapshot {
                request,
                response_tx,
            } => {
                let response = CoreWorker::publish_snapshot(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::ReadSnapshot {
                request,
                response_tx,
            } => {
                let response = CoreWorker::read_snapshot(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::DeleteSnapshot {
                request,
                response_tx,
            } => {
                let response = CoreWorker::delete_snapshot(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::BootstrapStream {
                request,
                response_tx,
            } => {
                let response = CoreWorker::bootstrap_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::WaitRead {
                request,
                waiter_id,
                response_tx,
            } => {
                let watcher = ReadWatcher {
                    waiter_id,
                    request,
                    response_tx,
                };
                CoreWorker::wait_read_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    self.placement,
                    watcher,
                    self.live_read_max_waiters_per_core,
                )
                .await;
            }
            GroupCommand::CancelWaitRead {
                stream_id,
                waiter_id,
            } => {
                CoreWorker::cancel_read_watcher(
                    &mut self.read_watchers,
                    self.metrics.clone(),
                    self.placement.core_id,
                    stream_id,
                    waiter_id,
                );
            }
            GroupCommand::RequireLiveReadOwner { response_tx } => {
                let response = self
                    .engine
                    .require_local_live_read_owner(self.placement)
                    .await
                    .map_err(|err| RuntimeError::group_engine(self.placement, err));
                let _ = response_tx.send(response);
            }
            GroupCommand::CloseStream {
                request,
                response_tx,
            } => {
                let response = CoreWorker::close_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::UpdateStreamAttrs {
                request,
                response_tx,
            } => {
                let response = CoreWorker::update_stream_attrs(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::DeleteStream {
                request,
                response_tx,
            } => {
                let response = CoreWorker::delete_stream(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::FlushCold {
                request,
                response_tx,
            } => {
                let response = CoreWorker::flush_cold(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::PlanColdFlush {
                request,
                response_tx,
            } => {
                let response = CoreWorker::plan_cold_flush(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::PlanNextColdFlushBatch {
                request,
                max_candidates,
                response_tx,
            } => {
                let response = CoreWorker::plan_next_cold_flush_batch(
                    &mut self.engine,
                    self.metrics.clone(),
                    request,
                    self.placement,
                    max_candidates,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::PlanColdGc { max, response_tx } => {
                let response =
                    CoreWorker::plan_cold_gc(&mut self.engine, max, self.placement).await;
                let _ = response_tx.send(response);
            }
            GroupCommand::AckColdGc {
                up_to_seq,
                response_tx,
            } => {
                let response =
                    CoreWorker::ack_cold_gc(&mut self.engine, up_to_seq, self.placement).await;
                let _ = response_tx.send(response);
            }
            GroupCommand::Append {
                request,
                response_tx,
                raft_uncommitted,
            } => {
                let response = CoreWorker::apply_append(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    request,
                    self.placement,
                    self.cold_write_admission,
                )
                .await;
                drop(raft_uncommitted);
                let _ = response_tx.send(response);
            }
            GroupCommand::AppendExternal {
                request,
                response_tx,
            } => {
                let response = CoreWorker::apply_append_external(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.read_materialization.clone(),
                    &mut self.read_watchers,
                    request,
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::AppendBatch {
                request,
                response_tx,
                raft_uncommitted,
            } => {
                let mut batch = vec![(request, response_tx, raft_uncommitted)];
                let mut coalesced_parents = Vec::new();
                self.collect_append_batch_commands(pending, &mut batch, &mut coalesced_parents);
                // One apply span for the coalesced batch: a child of the
                // triggering request (the current span), linked via
                // follows_from to every other coalesced request so their
                // traces show the shared apply instead of being silently
                // attributed to the triggering request alone.
                let span = tracing::debug_span!(
                    "core.append_batch",
                    group = self.placement.raft_group_id.0,
                    batch = batch.len(),
                );
                for parent in &coalesced_parents {
                    span.follows_from(parent);
                }
                let (requests, pending_batch) = CoreWorker::prepare_append_batch_requests(batch);
                CoreWorker::apply_prepared_append_batch_requests(
                    &mut self.engine,
                    AppendBatchRuntime {
                        metrics: self.metrics.clone(),
                        read_materialization: self.read_materialization.clone(),
                        placement: self.placement,
                    },
                    &mut self.read_watchers,
                    pending_batch,
                    requests,
                    self.cold_write_admission,
                )
                .instrument(span)
                .await;
            }
            GroupCommand::SnapshotGroup { response_tx } => {
                let response = CoreWorker::snapshot_group(
                    &mut self.engine,
                    self.metrics.clone(),
                    self.placement,
                )
                .await;
                let _ = response_tx.send(response);
            }
            GroupCommand::InstallGroupSnapshot {
                snapshot,
                response_tx,
            } => {
                let response = CoreWorker::install_group_snapshot(
                    &mut self.engine,
                    self.metrics.clone(),
                    snapshot,
                )
                .await;
                let _ = response_tx.send(response);
            }
            #[cfg(madsim)]
            GroupCommand::ShutdownEngine { response_tx } => {
                let response = self
                    .engine
                    .shutdown()
                    .await
                    .map_err(|err| RuntimeError::group_engine(self.placement, err));
                let _ = response_tx.send(response);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    pub(crate) async fn next_command(
        &mut self,
        pending: &mut VecDeque<Traced<GroupCommand>>,
    ) -> Option<Traced<GroupCommand>> {
        match pending.pop_front() {
            Some(command) => Some(command),
            None => {
                let command = self.rx.recv().await;
                if command.is_some() {
                    self.metrics
                        .record_group_mailbox_dequeued(self.placement.raft_group_id);
                }
                command
            }
        }
    }

    pub(crate) fn collect_append_batch_commands(
        &mut self,
        pending: &mut VecDeque<Traced<GroupCommand>>,
        batch: &mut Vec<AppendBatchEntry>,
        coalesced_parents: &mut Vec<Span>,
    ) {
        while batch.len() < GROUP_ACTOR_MAX_WRITE_BATCH {
            let command = match pending.pop_front() {
                Some(command) => Some(command),
                None => match self.rx.try_recv() {
                    Ok(command) => {
                        self.metrics
                            .record_group_mailbox_dequeued(self.placement.raft_group_id);
                        Some(command)
                    }
                    Err(_) => None,
                },
            };
            match command {
                // Coalesced appends carry distinct parent spans; keep each so
                // the batch apply span can follows_from-link them all.
                Some(Traced {
                    value:
                        GroupCommand::AppendBatch {
                            request,
                            response_tx,
                            raft_uncommitted,
                        },
                    parent,
                }) => {
                    batch.push((request, response_tx, raft_uncommitted));
                    coalesced_parents.push(parent);
                }
                Some(other) => {
                    pending.push_front(other);
                    break;
                }
                None => break,
            }
        }
    }
}
