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

/// Resolves a `handle { ... }` argument keyword from the operation manifest
/// ([`crate::ops::runtime_operations`]) to the matching group-actor
/// expression. Any identifier that is not a context keyword falls through to
/// the like-named binding destructured from the command variant.
macro_rules! group_op_arg {
    ($actor:ident, $pending:ident, engine) => {
        &mut $actor.engine
    };
    ($actor:ident, $pending:ident, metrics) => {
        $actor.metrics.clone()
    };
    ($actor:ident, $pending:ident, read_materialization) => {
        $actor.read_materialization.clone()
    };
    ($actor:ident, $pending:ident, read_watchers) => {
        &mut $actor.read_watchers
    };
    ($actor:ident, $pending:ident, placement) => {
        $actor.placement
    };
    ($actor:ident, $pending:ident, core_id) => {
        $actor.placement.core_id
    };
    ($actor:ident, $pending:ident, cold_admission) => {
        $actor.cold_write_admission
    };
    ($actor:ident, $pending:ident, pending) => {
        $pending
    };
    ($actor:ident, $pending:ident, $field:ident) => {
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
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            ctx { $actor $pending $err $guard }
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
                    let _ = $tx.send(Err($err));
                }
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx } => {
                    let response =
                        CoreWorker::$worker($(group_op_arg!($actor, $pending, $arg)),*).await;
                    let _ = $tx.send(response);
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `call` with an admission guard: hold the guard across apply, drop it
    // before sending the result.
    (@munch
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            ctx { $actor $pending $err $guard }
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
                    let _ = $tx.send(Err($err));
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
                        CoreWorker::$worker($(group_op_arg!($actor, $pending, $arg)),*).await;
                    drop($g);
                    let _ = $tx.send(response);
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `tail`: the worker consumes the reply channel itself.
    (@munch
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            ctx { $actor $pending $err $guard }
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
                    let _ = $tx.send(Err($err));
                }
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx } => {
                    CoreWorker::$worker($(group_op_arg!($actor, $pending, $arg)),*).await;
                    ControlFlow::Continue(())
                }
            }
            rest { $($rest)* }
        }
    };
    // `sync`: synchronous worker call, no reply channel to reject on error.
    (@munch
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            ctx { $actor $pending $err $guard }
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
                    CoreWorker::$worker($(group_op_arg!($actor, $pending, $arg)),*);
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
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            ctx { $actor $pending $err $guard }
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
                    let _ = $tx.send(Err($err));
                }
            }
            attach { $($attach)* }
            handles {
                $($handles)*
                $(#[$attr])*
                GroupCommand::$Variant { $($field,)* $tx } => {
                    $actor.$method($(group_op_arg!($actor, $pending, $arg)),*).await
                }
            }
            rest { $($rest)* }
        }
    };
    // `actor` with an admission guard: delegate to a hand-written
    // `GroupActor` method that owns the guard (append-batch coalescing).
    (@munch
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            ctx { $actor $pending $err $guard }
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
                    let _ = $tx.send(Err($err));
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
                    $actor.$method($(group_op_arg!($actor, $pending, $arg)),*).await
                }
            }
            rest { $($rest)* }
        }
    };
    (@munch
        ctx { $actor:ident $pending:ident $err:ident $guard:ident }
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
            pub(crate) async fn handle(
                &mut self,
                command: GroupCommand,
                pending: &mut VecDeque<Traced<GroupCommand>>,
            ) -> ControlFlow<()> {
                let $actor = self;
                let $pending = pending;
                match command {
                    $($handles)*
                }
            }
        }
    };
    ($($manifest:tt)*) => {
        group_operations! {
            @munch
            ctx { actor pending err raft_uncommitted }
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

    async fn handle_wait_read(
        &mut self,
        request: ReadStreamRequest,
        waiter_id: u64,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    ) -> ControlFlow<()> {
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
        ControlFlow::Continue(())
    }

    async fn handle_append_batch(
        &mut self,
        request: AppendBatchRequest,
        response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
        raft_uncommitted: Option<UncommittedBytesGuard>,
        pending: &mut VecDeque<Traced<GroupCommand>>,
    ) -> ControlFlow<()> {
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
        ControlFlow::Continue(())
    }

    #[cfg(madsim)]
    async fn handle_shutdown_engine(
        &mut self,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    ) -> ControlFlow<()> {
        let response = self
            .engine
            .shutdown()
            .await
            .map_err(|err| RuntimeError::group_engine(self.placement, err));
        let _ = response_tx.send(response);
        ControlFlow::Break(())
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
