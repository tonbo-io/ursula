use std::ops::ControlFlow;
use std::sync::Arc;

use tracing::Instrument;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcPlanEntry;

use crate::admission::UncommittedBytesGuard;
use crate::cold_index::RepairColdIndexRequest;
use crate::cold_index::RepairColdIndexResponse;
use crate::cold_refs::ColdOrphanSweepPlan;
use crate::cold_refs::ColdOrphanSweepRequest;
use crate::command::GroupSnapshot;
use crate::core_worker::CoreWorker;
use crate::core_worker::ReadWatcher;
use crate::core_worker::ReadWatchers;
use crate::core_worker::reply;
use crate::engine::GroupEngine;
use crate::error::RuntimeError;
use crate::metrics::RuntimeMetricsInner;
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

/// Resolves a `handle { ... }` argument keyword from the operation manifest
/// ([`crate::ops::runtime_operations`]) to the matching group-actor
/// expression. Any identifier that is not a context keyword falls through to
/// the like-named binding destructured from the command variant.
macro_rules! group_op_arg {
    ($actor:ident, engine) => {
        &mut $actor.engine
    };
    ($actor:ident, metrics) => {
        $actor.metrics.clone()
    };
    ($actor:ident, read_materialization) => {
        $actor.read_materialization.clone()
    };
    ($actor:ident, read_watchers) => {
        &mut $actor.read_watchers
    };
    ($actor:ident, placement) => {
        $actor.placement
    };
    ($actor:ident, core_id) => {
        $actor.placement.core_id
    };
    ($actor:ident, cold_admission) => {
        $actor.cold_write_admission
    };
    ($actor:ident, $field:ident) => {
        $field
    };
}

/// Expands the operation manifest into the group-actor plumbing: the
/// [`GroupCommand`] enum, `GroupCommand::send_error` /
/// `GroupCommand::with_raft_uncommitted`, and `GroupActor::handle`. One
/// `@munch` rule exists per dispatch-arm shape (see the manifest grammar in
/// [`crate::ops`]); `ctx` threads the actor/queue/error/guard identifiers so
/// arms accumulated across expansion steps resolve hygienically.
macro_rules! group_operations {
    // `call` without an admission guard: await the worker, send the result.
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { none }
                handle { call $worker:ident($($arg:ident),* $(,)?) }
                client { $($client:tt)* }
            }
            $($rest:tt)*
        }
    ) => {
        group_operations! {
            @munch
            ctx { $actor $err $guard }
            variants {
                $($variants)*
                $(#[$attr])*
                $Variant {
                    $($field: $field_ty,)*
                    $tx: oneshot::Sender<Result<$Resp, RuntimeError>>,
                },
            }
            rejects {
                $($rejects)*
                $(#[$attr])*
                GroupCommand::$Variant { $tx, .. } => {
                    reply($tx, Err($err));
                }
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx } => {
                    let response =
                        CoreWorker::$worker($(group_op_arg!($actor, $arg)),*).await;
                    reply($tx, response);
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `call` with an admission guard: hold the guard across apply, drop it
    // before sending the result.
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { $g:ident }
                handle { call $worker:ident($($arg:ident),* $(,)?) }
                client { $($client:tt)* }
            }
            $($rest:tt)*
        }
    ) => {
        group_operations! {
            @munch
            ctx { $actor $err $guard }
            variants {
                $($variants)*
                $(#[$attr])*
                $Variant {
                    $($field: $field_ty,)*
                    $tx: oneshot::Sender<Result<$Resp, RuntimeError>>,
                    $g: Option<UncommittedBytesGuard>,
                },
            }
            rejects {
                $($rejects)*
                $(#[$attr])*
                GroupCommand::$Variant { $tx, .. } => {
                    reply($tx, Err($err));
                }
            }
            attach {
                $($attach)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx, $g: _ } => GroupCommand::$Variant {
                    $($field,)*
                    $tx,
                    $g: $guard,
                },
            }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx, $g } => {
                    let response =
                        CoreWorker::$worker($(group_op_arg!($actor, $arg)),*).await;
                    drop($g);
                    reply($tx, response);
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `tail`: the worker consumes the reply channel itself.
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { none }
                handle { tail $worker:ident($($arg:ident),* $(,)?) }
                client { $($client:tt)* }
            }
            $($rest:tt)*
        }
    ) => {
        group_operations! {
            @munch
            ctx { $actor $err $guard }
            variants {
                $($variants)*
                $(#[$attr])*
                $Variant {
                    $($field: $field_ty,)*
                    $tx: oneshot::Sender<Result<$Resp, RuntimeError>>,
                },
            }
            rejects {
                $($rejects)*
                $(#[$attr])*
                GroupCommand::$Variant { $tx, .. } => {
                    reply($tx, Err($err));
                }
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx } => {
                    CoreWorker::$worker($(group_op_arg!($actor, $arg)),*).await;
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `sync`: synchronous worker call, no reply channel to reject on error.
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { none }
                guard { none }
                handle { sync $worker:ident($($arg:ident),* $(,)?) }
                client { $($client:tt)* }
            }
            $($rest:tt)*
        }
    ) => {
        group_operations! {
            @munch
            ctx { $actor $err $guard }
            variants {
                $($variants)*
                $(#[$attr])*
                $Variant {
                    $($field: $field_ty,)*
                },
            }
            rejects {
                $($rejects)*
                $(#[$attr])*
                GroupCommand::$Variant { .. } => {}
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field),* } => {
                    CoreWorker::$worker($(group_op_arg!($actor, $arg)),*);
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `actor` without a guard: delegate to a hand-written `GroupActor`
    // method returning the loop `ControlFlow`. Must precede the guarded
    // `actor` rule so `guard { none }` is not captured as a guard name.
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { none }
                handle { actor $method:ident($($arg:ident),* $(,)?) }
                client { $($client:tt)* }
            }
            $($rest:tt)*
        }
    ) => {
        group_operations! {
            @munch
            ctx { $actor $err $guard }
            variants {
                $($variants)*
                $(#[$attr])*
                $Variant {
                    $($field: $field_ty,)*
                    $tx: oneshot::Sender<Result<$Resp, RuntimeError>>,
                },
            }
            rejects {
                $($rejects)*
                $(#[$attr])*
                GroupCommand::$Variant { $tx, .. } => {
                    reply($tx, Err($err));
                }
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx } => {
                    $actor.$method($(group_op_arg!($actor, $arg)),*).await
                }
            }
            rest { $($rest)* }
        }
    };
    // `actor` with an admission guard: delegate to a hand-written
    // `GroupActor` method that owns the guard.
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { $g:ident }
                handle { actor $method:ident($($arg:ident),* $(,)?) }
                client { $($client:tt)* }
            }
            $($rest:tt)*
        }
    ) => {
        group_operations! {
            @munch
            ctx { $actor $err $guard }
            variants {
                $($variants)*
                $(#[$attr])*
                $Variant {
                    $($field: $field_ty,)*
                    $tx: oneshot::Sender<Result<$Resp, RuntimeError>>,
                    $g: Option<UncommittedBytesGuard>,
                },
            }
            rejects {
                $($rejects)*
                $(#[$attr])*
                GroupCommand::$Variant { $tx, .. } => {
                    reply($tx, Err($err));
                }
            }
            attach {
                $($attach)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx, $g: _ } => GroupCommand::$Variant {
                    $($field,)*
                    $tx,
                    $g: $guard,
                },
            }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx, $g } => {
                    $actor.$method($(group_op_arg!($actor, $arg)),*).await
                }
            }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $actor:ident $err:ident $guard:ident }
        variants { $($variants:tt)* }
        rejects { $($rejects:tt)* }
        attach { $($attach:tt)* }
        handles { $($handles:tt)* }
        rest {}
    ) => {
        pub(crate) enum GroupCommand {
            $($variants)*
        }

        impl GroupCommand {
            /// Resolves the command with `err` without running it, so a
            /// rejected or undeliverable command never leaves its caller
            /// waiting on the reply channel.
            pub(crate) fn send_error(self, $err: RuntimeError) {
                match self {
                    $($rejects)*
                }
            }

            /// Attaches the raft-uncommitted admission credit acquired on the
            /// owning core. Commands without a guard slot pass through
            /// unchanged (the dispatcher only calls this for admission-guarded
            /// submissions).
            pub(crate) fn with_raft_uncommitted(
                self,
                $guard: Option<UncommittedBytesGuard>,
            ) -> Self {
                match self {
                    $($attach)*
                    other => other,
                }
            }
        }

        impl GroupActor {
            pub(crate) async fn handle(&mut self, command: GroupCommand) -> ControlFlow<()> {
                let $actor = self;
                match command {
                    $($handles)*
                }
            }
        }
    };
    ($($manifest:tt)*) => {
        group_operations! {
            @munch
            ctx { actor err raft_uncommitted }
            variants {}
            rejects {}
            attach {}
            handles {}
            rest { $($manifest)* }
        }
    };
}

crate::ops::runtime_operations!(group_operations);

pub(crate) struct GroupActor {
    pub(crate) deferred: Option<Traced<GroupCommand>>,
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
        let mut explicitly_shutdown = false;
        loop {
            let Some(Traced {
                value: command,
                parent,
            }) = self.next_command().await
            else {
                break;
            };
            // Re-establish the sender's span so on-core apply work links back to
            // the originating request.
            if self.handle(command).instrument(parent).await.is_break() {
                explicitly_shutdown = true;
                break;
            }
        }
        if !explicitly_shutdown && let Err(err) = self.engine.shutdown().await {
            tracing::warn!(
                core_id = self.placement.core_id.0,
                raft_group_id = self.placement.raft_group_id.0,
                %err,
                "group engine shutdown failed after its actor mailbox closed"
            );
        }
    }

    async fn handle_wait_read(
        &mut self,
        request: ReadStreamRequest,
        waiter_id: u64,
        incarnation: Option<u64>,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    ) -> ControlFlow<()> {
        let watcher = ReadWatcher {
            waiter_id,
            request,
            incarnation,
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
        ControlFlow::Continue(())
    }

    async fn handle_append(
        &mut self,
        request: AppendRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
        raft_uncommitted: Option<UncommittedBytesGuard>,
    ) -> ControlFlow<()> {
        if self.engine.supports_append_batch() {
            return self
                .handle_append_batch(request, response_tx, raft_uncommitted)
                .await;
        }
        let stream_id = request.stream_id.clone();
        let response = CoreWorker::commit_append(
            &mut self.engine,
            self.metrics.clone(),
            request,
            self.placement,
            self.cold_write_admission,
        )
        .await;
        drop(raft_uncommitted);
        match response {
            Ok(response) => {
                let deduplicated = response.deduplicated;
                // Durability is established when the group engine returns.
                // Wake/materialize blocked readers afterwards so a large fanout
                // cannot inflate the writer's acknowledgement latency.
                reply(response_tx, Ok(response));
                if !deduplicated {
                    CoreWorker::finish_append(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        stream_id,
                        self.placement,
                    )
                    .await;
                }
            }
            Err(err) => {
                reply(response_tx, Err(err));
            }
        }
        ControlFlow::Continue(())
    }

    async fn handle_append_batch(
        &mut self,
        request: AppendRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
        guard: Option<UncommittedBytesGuard>,
    ) -> ControlFlow<()> {
        const MAX_BATCH: usize = 32;
        const MAX_BATCH_BYTES: u64 = 1024 * 1024;
        // An individually larger request keeps its existing single-write path.
        let mut batch_bytes = request.payload_len();
        let mut requests = vec![request];
        let mut replies = vec![(response_tx, guard)];
        while requests.len() < MAX_BATCH {
            let Ok(command) = self.rx.try_recv() else {
                break;
            };
            match command.value {
                GroupCommand::Append { ref request, .. }
                    if batch_bytes.saturating_add(request.payload_len()) > MAX_BATCH_BYTES =>
                {
                    self.deferred = Some(command);
                    break;
                }
                GroupCommand::Append {
                    request,
                    response_tx,
                    raft_uncommitted,
                } => {
                    self.metrics
                        .record_group_mailbox_dequeued(self.placement.raft_group_id);
                    batch_bytes = batch_bytes.saturating_add(request.payload_len());
                    requests.push(request);
                    replies.push((response_tx, raft_uncommitted));
                }
                _ => {
                    self.deferred = Some(command);
                    break;
                }
            }
        }
        let streams = requests
            .iter()
            .map(|request| (request.stream_id.clone(), request.payload_len()))
            .collect::<Vec<_>>();
        let started = crate::rt::time::Instant::now();
        let responses = self
            .engine
            .append_batch(requests, self.placement, self.cold_write_admission)
            .await;
        let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.metrics.record_group_engine_exec(
            self.placement.core_id,
            self.placement.raft_group_id,
            elapsed,
        );
        let mut changed_streams = Vec::new();
        for (((stream, incoming_bytes), (tx, guard)), response) in
            streams.into_iter().zip(replies).zip(responses)
        {
            drop(guard);
            let response = response.map_err(|error| {
                crate::metrics::record_cold_backpressure_error(
                    &self.metrics,
                    self.placement,
                    incoming_bytes,
                    self.cold_write_admission,
                    &error,
                );
                RuntimeError::group_engine(self.placement, error)
            });
            let changed = response
                .as_ref()
                .is_ok_and(|response| !response.deduplicated);
            if let Ok(response) = &response
                && changed
            {
                self.metrics
                    .record_append(self.placement.core_id, self.placement.raft_group_id);
                self.metrics.record_applied_mutation(
                    self.placement.core_id,
                    self.placement.raft_group_id,
                    elapsed,
                );
                self.metrics.record_cold_hot_backlog(
                    self.placement.raft_group_id,
                    response.stream_hot_bytes,
                    response.group_hot_bytes,
                );
            }
            reply(tx, response);
            if changed && !changed_streams.contains(&stream) {
                changed_streams.push(stream);
            }
        }
        // Acknowledge the whole durable burst before waking readers.
        for stream in changed_streams {
            CoreWorker::finish_append(
                &mut self.engine,
                self.metrics.clone(),
                self.read_materialization.clone(),
                &mut self.read_watchers,
                stream,
                self.placement,
            )
            .await;
        }
        ControlFlow::Continue(())
    }

    async fn handle_shutdown_engine(
        &mut self,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    ) -> ControlFlow<()> {
        let response = self
            .engine
            .shutdown()
            .await
            .map_err(|err| RuntimeError::group_engine(self.placement, err));
        reply(response_tx, response);
        ControlFlow::Break(())
    }

    pub(crate) async fn next_command(&mut self) -> Option<Traced<GroupCommand>> {
        let command = match self.deferred.take() {
            Some(command) => Some(command),
            None => self.rx.recv().await,
        };
        if command.is_some() {
            self.metrics
                .record_group_mailbox_dequeued(self.placement.raft_group_id);
        }
        command
    }
}
