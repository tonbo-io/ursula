use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Semaphore, mpsc, oneshot};
use ursula_shard::{BucketStreamId, RaftGroupId, ShardPlacement};
use ursula_stream::ColdFlushCandidate;

use crate::command::GroupSnapshot;
use crate::core_worker::{AppendBatchRuntime, CoreWorker, ReadWatcher, ReadWatchers};
use crate::engine::GroupEngine;
use crate::error::RuntimeError;
use crate::metrics::{GROUP_ACTOR_MAX_WRITE_BATCH, RuntimeMetricsInner};
use crate::request::{
    AppendBatchRequest, AppendBatchResponse, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, BootstrapStreamResponse, CloseStreamRequest, CloseStreamResponse,
    ColdWriteAdmission, CreateStreamExternalRequest, CreateStreamRequest, CreateStreamResponse,
    DeleteSnapshotRequest, DeleteStreamRequest, DeleteStreamResponse, FlushColdRequest,
    FlushColdResponse, ForkRefResponse, HeadStreamRequest, HeadStreamResponse,
    PlanColdFlushRequest, PlanGroupColdFlushRequest, PublishSnapshotRequest,
    PublishSnapshotResponse, ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest,
    ReadStreamResponse,
};

#[derive(Clone)]
pub(crate) struct GroupMailbox {
    pub(crate) group_id: RaftGroupId,
    pub(crate) tx: mpsc::Sender<GroupCommand>,
    pub(crate) metrics: Arc<RuntimeMetricsInner>,
}

impl GroupMailbox {
    pub(crate) async fn send(&self, command: GroupCommand) -> Result<(), Box<GroupCommand>> {
        match self.tx.try_send(command) {
            Ok(()) => {
                self.metrics.record_group_mailbox_enqueued(self.group_id);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(command)) => {
                self.metrics.record_group_mailbox_full(self.group_id);
                match self.tx.send(command).await {
                    Ok(()) => {
                        self.metrics.record_group_mailbox_enqueued(self.group_id);
                        Ok(())
                    }
                    Err(err) => Err(Box::new(err.0)),
                }
            }
            Err(mpsc::error::TrySendError::Closed(command)) => Err(Box::new(command)),
        }
    }
}

pub(crate) struct PendingAppendBatch {
    pub(crate) stream_id: BucketStreamId,
    pub(crate) incoming_bytes: u64,
    pub(crate) response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    pub(crate) started_at: Instant,
}

#[derive(Debug)]
pub(crate) enum GroupCommand {
    CreateStream {
        request: CreateStreamRequest,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    CreateExternal {
        request: CreateStreamExternalRequest,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    HeadStream {
        request: HeadStreamRequest,
        response_tx: oneshot::Sender<Result<HeadStreamResponse, RuntimeError>>,
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
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    DeleteStream {
        request: DeleteStreamRequest,
        response_tx: oneshot::Sender<Result<DeleteStreamResponse, RuntimeError>>,
    },
    FlushCold {
        request: FlushColdRequest,
        response_tx: oneshot::Sender<Result<FlushColdResponse, RuntimeError>>,
    },
    FlushColdBatch {
        requests: Vec<FlushColdRequest>,
        response_tx: oneshot::Sender<Result<Vec<FlushColdResponse>, RuntimeError>>,
    },
    PlanColdFlush {
        request: PlanColdFlushRequest,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlush {
        request: PlanGroupColdFlushRequest,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlushBatch {
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
        response_tx: oneshot::Sender<Result<Vec<ColdFlushCandidate>, RuntimeError>>,
    },
    Append {
        request: AppendRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendExternal {
        request: AppendExternalRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendBatch {
        request: AppendBatchRequest,
        response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    },
    SnapshotGroup {
        response_tx: oneshot::Sender<Result<GroupSnapshot, RuntimeError>>,
    },
    InstallGroupSnapshot {
        snapshot: GroupSnapshot,
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
            Self::AddForkRef { response_tx, .. } | Self::ReleaseForkRef { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::DeleteStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::FlushCold { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::FlushColdBatch { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanColdFlush { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanNextColdFlush { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanNextColdFlushBatch { response_tx, .. } => {
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
        }
    }
}

pub(crate) struct GroupActor {
    pub(crate) placement: ShardPlacement,
    pub(crate) engine: Box<dyn GroupEngine>,
    pub(crate) rx: mpsc::Receiver<GroupCommand>,
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
            let Some(command) = self.next_command(&mut pending).await else {
                break;
            };
            match command {
                GroupCommand::CreateStream {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::create_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                        self.cold_write_admission,
                    )
                    .await;
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
                GroupCommand::AddForkRef {
                    stream_id,
                    now_ms,
                    response_tx,
                } => {
                    let response = CoreWorker::add_fork_ref(
                        &mut self.engine,
                        self.metrics.clone(),
                        stream_id,
                        now_ms,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::ReleaseForkRef {
                    stream_id,
                    response_tx,
                } => {
                    let response = CoreWorker::release_fork_ref(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        stream_id,
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
                GroupCommand::FlushColdBatch {
                    requests,
                    response_tx,
                } => {
                    let response = CoreWorker::flush_cold_batch(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        requests,
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
                GroupCommand::PlanNextColdFlush {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::plan_next_cold_flush(
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
                GroupCommand::Append {
                    request,
                    response_tx,
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
                } => {
                    let mut batch = vec![(request, response_tx)];
                    self.collect_append_batch_commands(&mut pending, &mut batch);
                    if self.cold_write_admission.is_enabled() {
                        let (requests, pending_batch) =
                            CoreWorker::prepare_append_batch_requests(batch);
                        CoreWorker::apply_prepared_append_batch_requests_with_cold_admission(
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
                        .await;
                    } else {
                        let (commands, pending_batch) =
                            CoreWorker::prepare_append_batch_commands(batch);
                        CoreWorker::apply_prepared_append_batch_commands(
                            &mut self.engine,
                            AppendBatchRuntime {
                                metrics: self.metrics.clone(),
                                read_materialization: self.read_materialization.clone(),
                                placement: self.placement,
                            },
                            &mut self.read_watchers,
                            pending_batch,
                            commands,
                        )
                        .await;
                    }
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
            }
        }
    }

    pub(crate) async fn next_command(
        &mut self,
        pending: &mut VecDeque<GroupCommand>,
    ) -> Option<GroupCommand> {
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
        pending: &mut VecDeque<GroupCommand>,
        batch: &mut Vec<(
            AppendBatchRequest,
            oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
        )>,
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
                Some(GroupCommand::AppendBatch {
                    request,
                    response_tx,
                }) => batch.push((request, response_tx)),
                Some(other) => {
                    pending.push_front(other);
                    break;
                }
                None => break,
            }
        }
    }
}
