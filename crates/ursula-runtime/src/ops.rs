//! Declarative manifest of every uniform runtime operation.
//!
//! Each entry describes one operation that flows `ShardRuntime` → core worker
//! → group actor and expands, via the generator macro passed to
//! [`runtime_operations!`], into the per-operation plumbing that used to be
//! hand-written four times per operation:
//!
//! - the `GroupCommand` enum variant (`group_actor.rs`),
//! - the reject-on-error arm in `GroupCommand::send_error`,
//! - the dispatch arm in `GroupActor::handle` calling the `CoreWorker` method,
//! - the public `ShardRuntime` client method (`runtime.rs`).
//!
//! `runtime_operations!(generator)` invokes `generator!` with the manifest, so
//! each module expands only the sites it owns (`group_operations!` in
//! `group_actor.rs`, `shard_runtime_operations!` in `runtime.rs`).
//!
//! Entry grammar (all sections required, in this order):
//!
//! ```text
//! op VariantName {
//!     fields { name: Type, ... }
//!     reply { response_tx: ResponseType } | reply { none }
//!     guard { raft_uncommitted } | guard { none }
//!     handle { KIND callee(arg, ...) }
//!     client { CLIENT }
//! }
//! ```
//!
//! - `fields` are the payload fields carried by the `GroupCommand` variant.
//! - `reply` names the oneshot sender field and its success type; `none`
//!   means the command carries no response channel (fire-and-forget).
//! - `guard` names the raft-uncommitted admission guard field for
//!   backpressure-guarded writes; the guard is attached on the owning core
//!   after the early admission check and dropped once apply completes.
//! - `handle` picks the dispatch-arm shape:
//!   - `call f(args)`: `CoreWorker::f(args).await` then send the result on
//!     the reply channel (dropping the guard first when one is present),
//!   - `tail f(args)`: `CoreWorker::f(args).await` where `f` consumes the
//!     reply channel itself,
//!   - `sync f(args)`: synchronous `CoreWorker::f(args)`, no reply channel,
//!   - `actor f(args)`: delegate to the hand-written `GroupActor::f`, which
//!     returns the loop `ControlFlow` (used by the genuinely non-uniform
//!     arms: read-watcher registration, append-batch coalescing, engine
//!     shutdown).
//!
//!   Arguments are context keywords resolved by `group_op_arg!` (`engine`,
//!   `metrics`, `read_materialization`, `read_watchers`, `placement`,
//!   `core_id`, `cold_admission`, `pending`) or the name of a `fields`
//!   entry, the `reply` channel, or the `guard`, so per-operation
//!   differences in what a dispatch arm passes stay explicit here.
//! - `client` picks the generated `ShardRuntime` method, or `none` to keep
//!   the client method hand-written:
//!   - `VIS stream fn name`: route by `request.stream_id`,
//!   - `VIS stream fn name, admit: EXPR`: same, and submit `EXPR` (over the
//!     single `request` field) as the incoming-bytes admission debit,
//!   - `VIS stream fn name, non_empty: FIELD, admit: EXPR`: same, rejecting
//!     `request.FIELD.is_empty()` with `RuntimeError::EmptyAppend` first,
//!   - `VIS group fn name`: route by an explicit `RaftGroupId` argument
//!     followed by the `fields` in declaration order.
//!
//! Operations that do not fit these shapes (wait-read registration and
//! cancellation, group warm-up, snapshot install placement checks, and the
//! madsim engine swap commands) keep hand-written client methods or
//! `CoreCommand` variants next to the generated code.
macro_rules! runtime_operations {
    ($generate:ident) => {
        $generate! {
            op CreateStream {
                fields { request: CreateStreamRequest }
                reply { response_tx: CreateStreamResponse }
                guard { raft_uncommitted }
                handle { call create_stream(engine, metrics, request, placement, cold_admission) }
                client {
                    pub stream fn create_stream,
                    admit: u64::try_from(request.initial_payload.len())
                        .expect("payload len fits u64")
                }
            }
            op CreateExternal {
                fields { request: CreateStreamExternalRequest }
                reply { response_tx: CreateStreamResponse }
                guard { none }
                handle { call create_stream_external(engine, metrics, request, placement) }
                client { pub stream fn create_stream_external }
            }
            op HeadStream {
                fields { request: HeadStreamRequest }
                reply { response_tx: HeadStreamResponse }
                guard { none }
                handle { call head_stream(engine, metrics, request, placement) }
                client { pub stream fn head_stream }
            }
            op GetStreamAttrs {
                fields { request: GetStreamAttrsRequest }
                reply { response_tx: GetStreamAttrsResponse }
                guard { none }
                handle { call get_stream_attrs(engine, metrics, request, placement) }
                client { pub stream fn get_stream_attrs }
            }
            op ReadStream {
                fields { request: ReadStreamRequest }
                reply { response_tx: ReadStreamResponse }
                guard { none }
                handle {
                    tail read_stream(
                        engine,
                        metrics,
                        read_materialization,
                        request,
                        placement,
                        response_tx
                    )
                }
                client { pub stream fn read_stream }
            }
            op PublishSnapshot {
                fields { request: PublishSnapshotRequest }
                reply { response_tx: PublishSnapshotResponse }
                guard { none }
                handle {
                    call publish_snapshot(
                        engine,
                        metrics,
                        read_materialization,
                        read_watchers,
                        request,
                        placement
                    )
                }
                client { pub stream fn publish_snapshot }
            }
            op ReadSnapshot {
                fields { request: ReadSnapshotRequest }
                reply { response_tx: ReadSnapshotResponse }
                guard { none }
                handle { call read_snapshot(engine, metrics, request, placement) }
                client { pub stream fn read_snapshot }
            }
            op DeleteSnapshot {
                fields { request: DeleteSnapshotRequest }
                reply { response_tx: () }
                guard { none }
                handle { call delete_snapshot(engine, metrics, request, placement) }
                client { pub stream fn delete_snapshot }
            }
            op BootstrapStream {
                fields { request: BootstrapStreamRequest }
                reply { response_tx: BootstrapStreamResponse }
                guard { none }
                handle { call bootstrap_stream(engine, metrics, request, placement) }
                client { pub stream fn bootstrap_stream }
            }
            op WaitRead {
                fields { request: ReadStreamRequest, waiter_id: u64 }
                reply { response_tx: ReadStreamResponse }
                guard { none }
                handle { actor handle_wait_read(request, waiter_id, response_tx) }
                client { none }
            }
            op CancelWaitRead {
                fields { stream_id: BucketStreamId, waiter_id: u64 }
                reply { none }
                guard { none }
                handle {
                    sync cancel_read_watcher(read_watchers, metrics, core_id, stream_id, waiter_id)
                }
                client { none }
            }
            op RequireLiveReadOwner {
                fields {}
                reply { response_tx: () }
                guard { none }
                handle { call require_live_read_owner(engine, placement) }
                client { none }
            }
            op CloseStream {
                fields { request: CloseStreamRequest }
                reply { response_tx: CloseStreamResponse }
                guard { none }
                handle {
                    call close_stream(
                        engine,
                        metrics,
                        read_materialization,
                        read_watchers,
                        request,
                        placement
                    )
                }
                client { pub stream fn close_stream }
            }
            op UpdateStreamAttrs {
                fields { request: UpdateStreamAttrsRequest }
                reply { response_tx: UpdateStreamAttrsResponse }
                guard { none }
                handle { call update_stream_attrs(engine, metrics, request, placement) }
                client { pub stream fn update_stream_attrs }
            }
            op DeleteStream {
                fields { request: DeleteStreamRequest }
                reply { response_tx: DeleteStreamResponse }
                guard { none }
                handle {
                    call delete_stream(
                        engine,
                        metrics,
                        read_materialization,
                        read_watchers,
                        request,
                        placement
                    )
                }
                client { pub stream fn delete_stream }
            }
            op FlushCold {
                fields { request: FlushColdRequest }
                reply { response_tx: FlushColdResponse }
                guard { none }
                handle {
                    call flush_cold(
                        engine,
                        metrics,
                        read_materialization,
                        read_watchers,
                        request,
                        placement
                    )
                }
                client { pub stream fn flush_cold }
            }
            op PlanColdFlush {
                fields { request: PlanColdFlushRequest }
                reply { response_tx: Option<ColdFlushCandidate> }
                guard { none }
                handle { call plan_cold_flush(engine, metrics, request, placement) }
                client { pub stream fn plan_cold_flush }
            }
            op PlanNextColdFlushBatch {
                fields { request: PlanGroupColdFlushRequest, max_candidates: usize }
                reply { response_tx: Vec<ColdFlushCandidate> }
                guard { none }
                handle {
                    call plan_next_cold_flush_batch(
                        engine,
                        metrics,
                        request,
                        placement,
                        max_candidates
                    )
                }
                client { pub group fn plan_next_cold_flush_batch }
            }
            op PlanColdGc {
                fields { max: usize }
                reply { response_tx: Vec<ColdGcEntry> }
                guard { none }
                handle { call plan_cold_gc(engine, max, placement) }
                client { group fn plan_cold_gc }
            }
            op AckColdGc {
                fields { up_to_seq: u64 }
                reply { response_tx: AckColdGcResponse }
                guard { none }
                handle { call ack_cold_gc(engine, up_to_seq, placement) }
                client { group fn ack_cold_gc }
            }
            op Append {
                fields { request: AppendRequest }
                reply { response_tx: AppendResponse }
                guard { raft_uncommitted }
                handle {
                    call apply_append(
                        engine,
                        metrics,
                        read_materialization,
                        read_watchers,
                        request,
                        placement,
                        cold_admission
                    )
                }
                client {
                    pub stream fn append,
                    non_empty: payload,
                    admit: request.payload_len()
                }
            }
            op AppendExternal {
                fields { request: AppendExternalRequest }
                reply { response_tx: AppendResponse }
                guard { none }
                handle {
                    call apply_append_external(
                        engine,
                        metrics,
                        read_materialization,
                        read_watchers,
                        request,
                        placement
                    )
                }
                client { pub stream fn append_external }
            }
            op AppendBatch {
                fields { request: AppendBatchRequest }
                reply { response_tx: AppendBatchResponse }
                guard { raft_uncommitted }
                handle { actor handle_append_batch(request, response_tx, raft_uncommitted, pending) }
                client {
                    pub stream fn append_batch,
                    non_empty: payloads,
                    admit: append_batch_payload_bytes(&request)
                }
            }
            op SnapshotGroup {
                fields {}
                reply { response_tx: GroupSnapshot }
                guard { none }
                handle { call snapshot_group(engine, metrics, placement) }
                client { pub group fn snapshot_group }
            }
            op InstallGroupSnapshot {
                fields { snapshot: GroupSnapshot }
                reply { response_tx: () }
                guard { none }
                handle { call install_group_snapshot(engine, metrics, snapshot) }
                client { none }
            }
            #[cfg(madsim)]
            op ShutdownEngine {
                fields {}
                reply { response_tx: () }
                guard { none }
                handle { actor handle_shutdown_engine(response_tx) }
                client { none }
            }
        }
    };
}

pub(crate) use runtime_operations;
