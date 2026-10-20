mod error;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod hygiene_tests;
pub mod in_memory;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use error::AnswerProblem;
pub use error::ColdOperation;
pub use error::EngineOperation;
pub use error::ForwardFailure;
pub use error::ForwardedRpc;
pub use error::GroupEngineError;
pub use error::GroupInfraError;
pub use error::GroupSnapshotError;
pub use error::KnownLeader;
pub use error::LeadershipReason;
pub use error::LeadershipRefusal;
pub use error::LogPosition;
pub use error::MembershipInitRefusal;
pub use error::Proposal;
pub use error::ResponseKind;
pub use error::SnapshotObjectCheck;
pub use error::SnapshotStoreOperation;
pub use error::StreamRefusal;
pub use error::UnproposableCommand;
pub use error::WalOpenFailureKind;
pub use error::WriteEffect;
pub use error::cold_store_failure;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use error::samples::every;
#[cfg(any(test, feature = "test-support"))]
pub use error::samples::group_engine_errors as group_engine_error_samples;
use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::ShardPlacement;
use ursula_stream::BucketUsageSnapshot;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcPlanEntry;

use crate::cold_uploads::UploadFence;
use crate::command::GroupSnapshot;
use crate::metrics::RaftSnapshotBuildSample;
use crate::metrics::RuntimeMetricsInner;
use crate::metrics::WalJournalSample;
use crate::metrics::WalMemorySample;
use crate::metrics::WalReadSample;
use crate::metrics::WalStorageSample;
use crate::read_index::LinearizableReadBarrier;
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
use crate::request::ColdHotBacklog;
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
use crate::request::RouteWriteRequest;
use crate::request::TouchStreamAccessResponse;
use crate::request::WriteRoute;

pub type GroupAppendBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Vec<Result<AppendResponse, GroupEngineError>>> + Send + 'a>>;
pub type GroupAppendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AppendResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupFlushColdFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FlushColdResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupCompactColdFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CompactColdResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanColdFlushFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanNextColdFlushBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupColdHotBacklogFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ColdHotBacklog, GroupEngineError>> + Send + 'a>>;
pub type GroupBucketUsageFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<BucketUsageSnapshot>, GroupEngineError>> + Send + 'a>>;
pub type GroupCreateStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CreateStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupRouteWriteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WriteRoute, GroupEngineError>> + Send + 'a>>;
pub type GroupHeadStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HeadStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadStreamPartsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupReadStreamParts, GroupEngineError>> + Send + 'a>>;
pub type GroupOpenLiveReadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<LiveReadOwner, GroupEngineError>> + Send + 'a>>;
pub type GroupRouteReadStreamFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<GroupReadRoute<GroupReadStreamParts>, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupRouteHeadStreamFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<GroupReadRoute<HeadStreamResponse>, GroupEngineError>>
            + Send
            + 'a,
    >,
>;

/// The group leader's answer to a read this replica forwarded. It owns
/// everything it needs, so the group actor hands it off and goes on with its
/// mailbox while the forwarded RPC waits on the network.
pub type GroupLeaderReadFuture<T> =
    Pin<Box<dyn Future<Output = Result<T, GroupEngineError>> + Send + 'static>>;

/// Where a read is answered.
pub enum GroupReadRoute<T> {
    /// This replica answered it.
    Local(T),
    /// The group leader answers it, through this forwarded RPC.
    Leader(GroupLeaderReadFuture<T>),
}

impl<T> GroupReadRoute<T> {
    /// The answer, waiting in place for a forwarded one.
    pub async fn resolve(self) -> Result<T, GroupEngineError> {
        match self {
            Self::Local(answer) => Ok(answer),
            Self::Leader(answer) => answer.await,
        }
    }
}
pub type GroupPublishSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PublishSnapshotResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupAdvanceRetentionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AdvanceRetentionResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupTidyStreamFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::request::TidyStreamResponse, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupOffloadColdRefsFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::cold_refs::OffloadColdRefsResponse, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupTidyStreamsFuture<'a> =
    Pin<Box<dyn Future<Output = crate::request::TidyStreamsPass<GroupEngineError>> + Send + 'a>>;
pub type GroupStateGaugesFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ursula_stream::GroupStateGauges, GroupEngineError>> + Send + 'a>,
>;
pub type GroupReadSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadSnapshotResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupBootstrapStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BootstrapStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupTouchStreamAccessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TouchStreamAccessResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupCloseStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CloseStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeleteStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeleteStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupAckColdGcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AckColdGcResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeferColdGcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeferColdGcResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupPurgeBucketFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PurgeBucketResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanColdGcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ColdGcPlanEntry>, GroupEngineError>> + Send + 'a>>;
pub type GroupRepairColdIndexFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::cold_index::RepairColdIndexResponse, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupPlanSharedRefCompactionFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Vec<ursula_stream::SharedRefCandidate>, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupPlanColdOrphanSweepFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::cold_refs::ColdOrphanSweepPlan, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupImportGroupStateFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::request::ImportGroupStateResponse, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
pub type GroupSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupSnapshot, GroupEngineError>> + Send + 'a>>;
pub type GroupInstallSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupShutdownFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupEngineCreateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn GroupEngine>, GroupEngineError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupWriteResponse {
    CreateStream(CreateStreamResponse),
    Append(AppendResponse),
    PublishSnapshot(PublishSnapshotResponse),
    AdvanceRetention(AdvanceRetentionResponse),
    TouchStreamAccess(TouchStreamAccessResponse),
    FlushCold(FlushColdResponse),
    CompactCold(CompactColdResponse),
    CloseStream(CloseStreamResponse),
    DeleteStream(DeleteStreamResponse),
    AckColdGc(AckColdGcResponse),
    PurgeBucket(PurgeBucketResponse),
    ImportGroupState(crate::request::ImportGroupStateResponse),
    /// A history event applied (#581 section 4.3).
    NewHistory(crate::request::NewHistoryResponse),
    TidyStream(crate::request::TidyStreamResponse),
    DeferColdGc(DeferColdGcResponse),
    OffloadColdRefs(crate::cold_refs::OffloadStreamColdRefsResponse),
    /// An `ExpireStreams` entry of the leader's expiry sweep applied.
    ExpireStreams(crate::request::ExpireStreamsResponse),
}

/// `GroupWriteResponse::kind` and one `TryFrom<GroupWriteResponse>` per
/// response: a caller that expects one kind of response gets it, or
/// [`GroupInfraError::UnexpectedResponse`] naming what it got.
macro_rules! group_write_responses {
    ($($variant:ident($response:ty)),+ $(,)?) => {
        impl GroupWriteResponse {
            pub fn kind(&self) -> ResponseKind {
                match self {
                    $(Self::$variant(_) => ResponseKind::$variant,)+
                }
            }
        }

        $(
            impl TryFrom<GroupWriteResponse> for $response {
                type Error = GroupEngineError;

                fn try_from(response: GroupWriteResponse) -> Result<Self, Self::Error> {
                    match response {
                        GroupWriteResponse::$variant(response) => Ok(response),
                        other => Err(GroupEngineError::Infra(
                            GroupInfraError::UnexpectedResponse {
                                expected: ResponseKind::$variant,
                                actual: other.kind(),
                            },
                        )),
                    }
                }
            }
        )+
    };
}

group_write_responses! {
    CreateStream(CreateStreamResponse),
    Append(AppendResponse),
    PublishSnapshot(PublishSnapshotResponse),
    AdvanceRetention(AdvanceRetentionResponse),
    TouchStreamAccess(TouchStreamAccessResponse),
    FlushCold(FlushColdResponse),
    CompactCold(CompactColdResponse),
    CloseStream(CloseStreamResponse),
    DeleteStream(DeleteStreamResponse),
    AckColdGc(AckColdGcResponse),
    PurgeBucket(PurgeBucketResponse),
    ImportGroupState(crate::request::ImportGroupStateResponse),
    NewHistory(crate::request::NewHistoryResponse),
    TidyStream(crate::request::TidyStreamResponse),
    DeferColdGc(DeferColdGcResponse),
    OffloadColdRefs(crate::cold_refs::OffloadStreamColdRefsResponse),
    ExpireStreams(crate::request::ExpireStreamsResponse),
}

/// The error of a [`GroupEngine`] operation the engine leaves
/// unimplemented: refused before anything was proposed.
fn unsupported(operation: EngineOperation) -> GroupEngineError {
    GroupEngineError::Infra(GroupInfraError::Unsupported { operation })
}

pub trait GroupEngine: Send + 'static {
    fn accepts_local_writes(&self) -> bool {
        true
    }

    /// The leadership a cold upload this replica plans now is fenced to
    /// (F14j), or `None` while it does not lead. An engine without
    /// replication leads in one leadership for as long as it runs. A view
    /// that lags OpenRaft's names a leadership that already ended, whose
    /// proposals OpenRaft refuses, so a lag only ever refuses an upload.
    /// Every engine names it: a default could give a replicated engine a
    /// fence it does not hold.
    fn upload_fence(&self) -> Option<UploadFence>;

    /// The group's ReadIndex barrier, which the runtime calls before it
    /// queues a linearizable read (D10). `None` (the default) keeps
    /// such reads linearized inside the engine, if at all.
    fn linearizable_read_barrier(&self) -> Option<Arc<dyn LinearizableReadBarrier>> {
        None
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a>;

    fn create_stream_external<'a>(
        &'a mut self,
        _request: CreateStreamExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::CreateStreamExternal)) })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a>;

    /// Routes a write before its body is read (#581 section 6.1): whether
    /// this replica leads the group, and otherwise the leader and its term.
    /// A leader also answers a tokened producer write that its head alone
    /// decides (section 2.5, W7): a repeat of a write that applied under the
    /// request's token, or a key conflict, from its applied state. Every
    /// engine routes: one without replication always leads.
    fn route_write<'a>(
        &'a mut self,
        request: RouteWriteRequest,
        placement: ShardPlacement,
    ) -> GroupRouteWriteFuture<'a>;

    /// Per-bucket committed usage held by this group's replicated state.
    ///
    /// Served from local replica state, leader or follower: usage export
    /// tolerates replication lag, and requiring leadership would make a
    /// node-local aggregate fail whenever any group is led elsewhere.
    /// Deliberately a required method — an engine that silently reported
    /// nothing would underbill.
    fn bucket_usage<'a>(&'a mut self, placement: ShardPlacement) -> GroupBucketUsageFuture<'a>;

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a>;

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            let response = self.read_stream(request, placement).await?;
            Ok(GroupReadStreamParts::from_response(response))
        })
    }

    /// [`Self::read_stream_parts`] for a caller that must not wait on the
    /// network: a read this replica forwards to its group leader comes back
    /// as [`GroupReadRoute::Leader`], unanswered. The group actor answers it
    /// outside the actor, so one forwarded read waiting on a silent leader
    /// does not hold every later command of the group. The default, for an
    /// engine that never forwards, answers locally.
    fn route_read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupRouteReadStreamFuture<'a> {
        Box::pin(async move {
            self.read_stream_parts(request, placement)
                .await
                .map(GroupReadRoute::Local)
        })
    }

    /// [`Self::head_stream`], with a forwarded HEAD left unanswered as in
    /// [`Self::route_read_stream`].
    fn route_head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupRouteHeadStreamFuture<'a> {
        Box::pin(async move {
            self.head_stream(request, placement)
                .await
                .map(GroupReadRoute::Local)
        })
    }

    /// Live-read registration (SSE, long-poll): requires this replica to
    /// own the stream's live reads and reads the stream's state there (see
    /// [`LiveReadOwner`]). A replicated engine confirms it leads at a read
    /// index (`request.read_index` when the runtime already confirmed one)
    /// and never forwards. The default, for an engine without replicas, is
    /// a plain HEAD.
    fn open_live_read<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupOpenLiveReadFuture<'a> {
        Box::pin(async move {
            let head = self.head_stream(request, placement).await?;
            Ok(LiveReadOwner {
                read_index: None,
                head,
            })
        })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        _request: PublishSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::PublishSnapshot)) })
    }

    fn advance_retention<'a>(
        &'a mut self,
        _request: AdvanceRetentionRequest,
        _placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::AdvanceRetention)) })
    }

    /// Restore path: replaces an empty group's state with a backup snapshot
    /// as one replicated write. See `StreamCommand::ImportSnapshot`.
    fn import_group_state<'a>(
        &'a mut self,
        _request: ImportGroupStateRequest,
        _placement: ShardPlacement,
    ) -> GroupImportGroupStateFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::ImportGroupState)) })
    }

    /// Bounded-state gauges of this replica's applied state
    /// (`docs/architecture/bounded-stream-state.md`, Gauges). Served from local
    /// state, leader or follower, like [`GroupEngine::bucket_usage`].
    /// Default unsupported.
    fn state_gauges<'a>(&'a mut self, _placement: ShardPlacement) -> GroupStateGaugesFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::StateGauges)) })
    }

    /// Replicated `TidyStream` write (bounded-state F0); see
    /// `StreamCommand::TidyStream`. Default unsupported.
    fn tidy_stream<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _now_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupTidyStreamFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::TidyStream)) })
    }

    /// One leader-side tidy pass (bounded-state F0): first the expiry sweep
    /// ([`crate::expiry_sweep`]), at most `request.max_expiry_sweeps`
    /// `ExpireStreams` entries, then `TidyStream`
    /// for up to `request.max_streams` streams with debt. A follower or an
    /// engine without support proposes nothing.
    fn tidy_streams<'a>(
        &'a mut self,
        _request: crate::request::TidyStreamsRequest,
        _placement: ShardPlacement,
    ) -> GroupTidyStreamsFuture<'a> {
        Box::pin(async move { crate::request::TidyStreamsPass::empty() })
    }

    /// One leader-side external-locator offload pass (bounded-state F5): for
    /// each stream whose state-held external refs are due, writes their
    /// cold-index page entries (clipping overlapping entries) and then
    /// proposes `OffloadColdRefs`. A follower or an engine without a cold
    /// store offloads nothing.
    fn offload_cold_refs<'a>(
        &'a mut self,
        _request: crate::cold_refs::OffloadColdRefsRequest,
        _placement: ShardPlacement,
    ) -> GroupOffloadColdRefsFuture<'a> {
        Box::pin(async move { Ok(crate::cold_refs::OffloadColdRefsResponse::default()) })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        _request: ReadSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::ReadSnapshot)) })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        _request: BootstrapStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::BootstrapStream)) })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a>;

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a>;

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a>;

    /// Replicated confirmation that cold-GC entries up to `up_to_seq` have been
    /// physically reclaimed; pops them from the queue. Default unsupported.
    fn ack_cold_gc<'a>(
        &'a mut self,
        _up_to_seq: u64,
        _placement: ShardPlacement,
    ) -> GroupAckColdGcFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::AckColdGc)) })
    }

    /// Replicated `DeferColdGc` (bounded-state F14b): moves
    /// the failing cold-GC entry `seq` to the tail of the queue, due no
    /// earlier than `not_before_ms`. Default unsupported.
    fn defer_cold_gc<'a>(
        &'a mut self,
        _seq: u64,
        _not_before_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupDeferColdGcFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::DeferColdGc)) })
    }

    /// Replicated tenant offboarding: removes every stream in the bucket and
    /// the bucket in this group. Monotonic aggregate usage remains
    /// available to asynchronous accounting readers. Default unsupported.
    fn purge_bucket<'a>(
        &'a mut self,
        _bucket_id: String,
        _placement: ShardPlacement,
    ) -> GroupPurgeBucketFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::PurgeBucket)) })
    }

    /// Leader-local read of the front of the cold-GC queue for the background
    /// worker to reclaim. Default returns an empty batch.
    fn plan_cold_gc<'a>(
        &'a mut self,
        _max: usize,
        _placement: ShardPlacement,
    ) -> GroupPlanColdGcFuture<'a> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Leader-side cold-index page repair for one step of the group's stream
    /// cursor (bounded-state F19 step 2). It runs in the group actor, one at a
    /// time with every other page writer. Default repairs nothing and ends
    /// the cycle.
    fn repair_cold_index<'a>(
        &'a mut self,
        _request: crate::cold_index::RepairColdIndexRequest,
        _placement: ShardPlacement,
    ) -> GroupRepairColdIndexFuture<'a> {
        Box::pin(async { Ok(crate::cold_index::RepairColdIndexResponse::default()) })
    }

    /// Leader-local discovery for the shared pack-reference compaction
    /// driver (bounded-state F2): the streams to compact next and the run of
    /// shared refs to compact for each. Default finds none.
    fn plan_shared_ref_compaction<'a>(
        &'a mut self,
        _request: ursula_stream::SharedRefCompactionRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanSharedRefCompactionFuture<'a> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// What applied state references for one orphan-sweep step (bounded-state
    /// F14h). Default reports a non-leader with nothing to sweep.
    fn plan_cold_orphan_sweep<'a>(
        &'a mut self,
        _request: crate::cold_refs::ColdOrphanSweepRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdOrphanSweepFuture<'a> {
        Box::pin(async { Ok(crate::cold_refs::ColdOrphanSweepPlan::default()) })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a>;

    /// Engines with a replicated log can submit a bounded burst together.
    fn supports_append_batch(&self) -> bool {
        false
    }

    /// Submit an actor-bounded burst (at most 32 requests and 1 MiB of payload,
    /// or one individually larger request), preserving response order.
    fn append_batch<'a>(
        &'a mut self,
        requests: Vec<AppendRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(requests.len());
            for request in requests {
                results.push(self.append(request, placement, admission).await);
            }
            results
        })
    }

    fn append_external<'a>(
        &'a mut self,
        _request: AppendExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::AppendExternal)) })
    }

    fn flush_cold<'a>(
        &'a mut self,
        _request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::FlushCold)) })
    }

    fn compact_cold<'a>(
        &'a mut self,
        _request: CompactColdRequest,
        _placement: ShardPlacement,
    ) -> GroupCompactColdFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::CompactCold)) })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        _request: PlanColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::PlanColdFlush)) })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        _request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
        _max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::PlanGroupColdFlush)) })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async { Err(unsupported(EngineOperation::ColdHotBacklog)) })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a>;

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a>;

    fn shutdown<'a>(&'a mut self) -> GroupShutdownFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// The group's applied history (#581 section 4.3), which changes at
    /// every history event this replica applies, by a command, a snapshot
    /// install or a startup replay. The group actor wakes every live read of
    /// the group when it changes. `None` for an engine whose history never
    /// changes.
    fn history_watch(&self) -> Option<HistoryWatch> {
        None
    }
}

/// The shared state of a [`HistoryPublisher`] and its [`HistoryWatch`]es.
#[derive(Debug, Default)]
struct HistoryShared {
    value: std::sync::Mutex<Option<ursula_stream::HistoryEvent>>,
    /// Bumped by every change, under the value's lock.
    version: std::sync::atomic::AtomicU64,
    changed: crate::rt::sync::Notify,
}

/// The sending side of a group's applied history (#581 section 4.7), kept
/// with the group's state. Clones publish to the same receivers.
///
/// Not a tokio `watch`: that wakes concurrent receivers in an order drawn
/// from tokio's own thread RNG, which a simulation cannot replay. A
/// `Notify` wakes its waiters in the order they began to wait.
#[derive(Debug, Clone, Default)]
pub struct HistoryPublisher(std::sync::Arc<HistoryShared>);

impl HistoryPublisher {
    /// Publishes `history` when it differs from what was published.
    pub fn publish(&self, history: Option<ursula_stream::HistoryEvent>) {
        let mut value = self
            .0
            .value
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *value == history {
            return;
        }
        *value = history;
        self.0
            .version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        drop(value);
        self.0.changed.notify_waiters();
    }

    /// A receiver that has seen what was published so far.
    pub fn subscribe(&self) -> HistoryWatch {
        HistoryWatch {
            seen: self.0.version.load(std::sync::atomic::Ordering::SeqCst),
            shared: self.0.clone(),
        }
    }
}

/// A receiver of a group's applied history: `None` before its genesis.
#[derive(Debug, Clone)]
pub struct HistoryWatch {
    shared: std::sync::Arc<HistoryShared>,
    seen: u64,
}

impl HistoryWatch {
    /// The history published last.
    pub fn current(&self) -> Option<ursula_stream::HistoryEvent> {
        *self
            .shared
            .value
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Waits until a history this receiver has not seen is published.
    pub async fn changed(&mut self) {
        loop {
            let notified = self.shared.changed.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            let version = self
                .shared
                .version
                .load(std::sync::atomic::Ordering::SeqCst);
            if version != self.seen {
                self.seen = version;
                return;
            }
            notified.await;
        }
    }
}

pub trait GroupEngineFactory: Send + Sync + 'static {
    fn hosts_group(&self, _placement: ShardPlacement) -> bool {
        true
    }

    /// Opens `placement`'s engine. A core drops the future when the
    /// group's shutdown begins during the open (#741), so a future dropped
    /// at any await must leave nothing running: the Raft engines await
    /// nothing once `Raft::new` started the group's Raft.
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a>;
}

#[derive(Debug, Clone)]
pub struct GroupEngineMetrics {
    pub(crate) inner: Arc<RuntimeMetricsInner>,
}

impl GroupEngineMetrics {
    pub fn record_wal_batch(
        &self,
        placement: ShardPlacement,
        record_count: usize,
        write_ns: u64,
        sync_ns: u64,
    ) {
        self.inner.record_wal_batch(
            placement.core_id,
            placement.raft_group_id,
            u64::try_from(record_count).expect("record count fits u64"),
            write_ns,
            sync_ns,
        );
    }

    pub fn record_wal_storage(&self, placement: ShardPlacement, sample: WalStorageSample) {
        self.inner
            .record_wal_storage(placement.core_id, placement.raft_group_id, sample);
    }

    /// Records what a core journal's writer did besides writing batches.
    pub fn record_wal_journal(&self, core_id: CoreId, sample: WalJournalSample) {
        self.inner.record_wal_journal(core_id, sample);
    }

    /// Records what a read of a group's log cost.
    pub fn record_wal_read(&self, placement: ShardPlacement, sample: WalReadSample) {
        self.inner
            .record_wal_read(placement.core_id, placement.raft_group_id, sample);
    }

    /// Records the size of a group's log in memory.
    pub fn record_wal_memory(&self, placement: ShardPlacement, sample: WalMemorySample) {
        self.inner
            .record_wal_memory(placement.raft_group_id, sample);
    }

    pub fn record_wal_recovery(
        &self,
        placement: ShardPlacement,
        recovery_ns: u64,
        records: u64,
        bytes: u64,
        live_entries: u64,
    ) {
        self.inner.record_wal_recovery(
            placement.core_id,
            recovery_ns,
            records,
            bytes,
            live_entries,
        );
    }

    pub fn record_raft_apply_batch(
        &self,
        placement: ShardPlacement,
        entry_count: usize,
        apply_ns: u64,
    ) {
        self.inner.record_raft_apply_batch(
            placement.core_id,
            placement.raft_group_id,
            u64::try_from(entry_count).expect("entry count fits u64"),
            apply_ns,
        );
    }

    /// Records one snapshot GC pass (#669).
    pub fn record_snapshot_gc(&self, sample: crate::metrics::SnapshotGcSample) {
        self.inner.record_snapshot_gc(sample);
    }

    pub fn record_raft_snapshot_build(
        &self,
        placement: ShardPlacement,
        stream_count: usize,
        body_bytes: usize,
        pointer_bytes: usize,
        build_ns: u64,
        external_upload: bool,
        inline_fallback: bool,
    ) {
        self.inner
            .record_raft_snapshot_build(placement.raft_group_id, RaftSnapshotBuildSample {
                streams: u64::try_from(stream_count).expect("stream count fits u64"),
                body_bytes: u64::try_from(body_bytes).expect("snapshot body bytes fits u64"),
                pointer_bytes: u64::try_from(pointer_bytes)
                    .expect("snapshot pointer bytes fits u64"),
                build_ns,
                external_upload,
                inline_fallback,
            });
    }
}
