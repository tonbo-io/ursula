#[cfg(test)]
mod hygiene_tests;
pub mod in_memory;

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::BucketUsageSnapshot;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcPlanEntry;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;

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
use crate::request::TouchStreamAccessResponse;

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
pub type GroupTidyStreamsFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::request::TidyStreamsResponse, GroupEngineError>>
            + Send
            + 'a,
    >,
>;
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
    TidyStream(crate::request::TidyStreamResponse),
    DeferColdGc(DeferColdGcResponse),
    OffloadColdRefs(crate::cold_refs::OffloadStreamColdRefsResponse),
}

pub trait GroupEngine: Send + 'static {
    fn accepts_local_writes(&self) -> bool {
        true
    }

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
        request: CreateStreamExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "external stream create is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a>;

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
        request: PublishSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot publish is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn advance_retention<'a>(
        &'a mut self,
        request: AdvanceRetentionRequest,
        _placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "retention advance is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    /// Restore path: replaces an empty group's state with a backup snapshot
    /// as one replicated write. See `StreamCommand::ImportSnapshot`.
    fn import_group_state<'a>(
        &'a mut self,
        _request: ImportGroupStateRequest,
        placement: ShardPlacement,
    ) -> GroupImportGroupStateFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "group state import is not supported for group {}",
                placement.raft_group_id.0
            )))
        })
    }

    /// Bounded-state gauges of this replica's applied state
    /// (`docs/architecture/bounded-stream-state.md` §7.5). Served from local
    /// state, leader or follower, like [`GroupEngine::bucket_usage`].
    /// Default unsupported.
    fn state_gauges<'a>(&'a mut self, placement: ShardPlacement) -> GroupStateGaugesFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "state gauges are not supported for group {}",
                placement.raft_group_id.0
            )))
        })
    }

    /// Replicated `TidyStream` write (bounded-state F0); see
    /// `StreamCommand::TidyStream`. Default unsupported.
    fn tidy_stream<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupTidyStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "stream tidy is not supported for group {}",
                placement.raft_group_id.0
            )))
        })
    }

    /// One leader-side `TidyStream` pass (bounded-state F0): proposes
    /// `TidyStream` for up to `request.max_streams` streams with debt. A
    /// follower or an engine without support tidies nothing.
    fn tidy_streams<'a>(
        &'a mut self,
        _request: crate::request::TidyStreamsRequest,
        _placement: ShardPlacement,
    ) -> GroupTidyStreamsFuture<'a> {
        Box::pin(async move { Ok(crate::request::TidyStreamsResponse::default()) })
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
        request: ReadSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot read is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "bootstrap is not supported for stream '{}'",
                request.stream_id
            )))
        })
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
        Box::pin(async { Err(GroupEngineError::new("cold GC ack is not supported")) })
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
        Box::pin(async { Err(GroupEngineError::new("cold GC deferral is not supported")) })
    }

    /// Replicated tenant offboarding: removes every stream in the bucket and
    /// the bucket in this group. Monotonic aggregate usage remains
    /// available to asynchronous accounting readers. Default unsupported.
    fn purge_bucket<'a>(
        &'a mut self,
        _bucket_id: String,
        _placement: ShardPlacement,
    ) -> GroupPurgeBucketFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("bucket purge is not supported")) })
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
        request: AppendExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "external append is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold flush is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn compact_cold<'a>(
        &'a mut self,
        request: CompactColdRequest,
        _placement: ShardPlacement,
    ) -> GroupCompactColdFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold compaction is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold flush planning is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        _request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
        _max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(
                "group cold flush planning is not supported",
            ))
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold hot backlog is not supported for stream '{stream_id}'"
            )))
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a>;

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a>;

    fn shutdown<'a>(&'a mut self) -> GroupShutdownFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

pub trait GroupEngineFactory: Send + Sync + 'static {
    fn hosts_group(&self, _placement: ShardPlacement) -> bool {
        true
    }

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupLeaderHint {
    pub node_id: Option<u64>,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEngineError {
    message: String,
    code: StreamErrorCode,
    next_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    context: Vec<StreamErrorContext>,
}

/// A command the data-group apply dispatcher cannot execute. The proposal
/// boundary refuses it before anything is written, because once committed it
/// would stop every replica of the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnproposableCommand {
    /// Stream commands create their bucket inside their own apply.
    CreateBucket,
}

/// Serializable startup classification. The WAL facade retains the native
/// source chain; the engine boundary logs it before converting to this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalOpenFailureKind {
    Io,
    Corrupt,
    IncompatibleFormat,
    Locked,
    DuplicateGroup,
    Stopped,
    InvalidConfiguration,
    InvalidRecord,
    Poisoned,
}

/// Infra error variants with structured fields render their human message on
/// demand (`message`) instead of storing a denormalized copy alongside the
/// fields. `Internal` is the exception: it carries free-form text with no
/// structured source, so it keeps an owned `message`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GroupInfraError {
    #[error(
        "recovery gate node {actual} differs from engine node {expected} for {raft_group_id:?}"
    )]
    RecoveryNodeMismatch {
        raft_group_id: RaftGroupId,
        expected: u64,
        actual: u64,
    },
    #[error("raft group {raft_group_id:?} is not registered on this node")]
    RaftGroupNotRegistered { raft_group_id: RaftGroupId },
    #[error("recovery gate for group {raft_group_id:?} is already bound")]
    RecoveryAlreadyBound { raft_group_id: RaftGroupId },
    #[error("recovery gate group {actual:?} differs from engine group {expected:?}")]
    RecoveryGroupMismatch {
        expected: RaftGroupId,
        actual: RaftGroupId,
    },
    #[error(
        "raft group {raft_group_id:?} stopped after a committed apply failure; retain WAL for corrected-code replay"
    )]
    ApplyStopped {
        raft_group_id: RaftGroupId,
        term: u64,
        index: u64,
        kind: ursula_proto::admin::ApplyFailureKind,
    },
    #[error("command {command:?} cannot be proposed to a data Raft group")]
    InvalidRaftCommand { command: UnproposableCommand },
    #[error("open WAL for raft group {raft_group_id:?} on core {core_id:?}: {failure:?}")]
    WalOpen {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        failure: WalOpenFailureKind,
    },
    #[error("raft group {raft_group_id:?} has not established its recovery vote floor")]
    RecoveryVoteFloor { raft_group_id: RaftGroupId },
    #[error("the Raft owner has stopped")]
    OwnerStopped,
    #[error("write outcome is unknown; the proposal may have committed")]
    OutcomeUnknown,
    #[error("{message}")]
    Internal { message: String },
    #[error("ProtoDecode: protobuf raft payload missing {field}")]
    ProtoDecode { field: String },
    #[error(
        "ColdBackpressure: stream '{stream_id}' would raise group hot bytes from {before_group_hot_bytes} to {after_group_hot_bytes}, above limit {limit}"
    )]
    ColdBackpressure {
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    },
    #[error(
        "RaftUncommittedBackpressure: group uncommitted bytes {current} plus incoming {incoming} would exceed limit {limit}"
    )]
    RaftUncommittedBackpressure {
        current: u64,
        incoming: u64,
        limit: u64,
    },
}

impl GroupInfraError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn proto_decode(field: impl Into<String>) -> Self {
        Self::ProtoDecode {
            field: field.into(),
        }
    }

    pub fn cold_backpressure(
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    ) -> Self {
        Self::ColdBackpressure {
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        }
    }

    pub fn raft_uncommitted_backpressure(current: u64, incoming: u64, limit: u64) -> Self {
        Self::RaftUncommittedBackpressure {
            current,
            incoming,
            limit,
        }
    }

    pub fn message(&self) -> Cow<'_, str> {
        match self {
            Self::Internal { message } => Cow::Borrowed(message),
            other => Cow::Owned(other.to_string()),
        }
    }

    pub fn is_cold_backpressure(&self) -> bool {
        matches!(self, Self::ColdBackpressure { .. })
    }

    pub fn is_backpressure(&self) -> bool {
        matches!(
            self,
            Self::ColdBackpressure { .. } | Self::RaftUncommittedBackpressure { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GroupEngineError {
    #[error("{}", .0.message)]
    Stream(StreamEngineError),
    #[error("{}", .0.message())]
    Infra(GroupInfraError),
    #[error("{message}")]
    ForwardToLeader {
        message: String,
        leader_hint: GroupLeaderHint,
        /// True only when the node checked leadership locally before it
        /// proposed anything (RT1). A forward reported by OpenRaft after
        /// `client_write` (a responder dropped on step-down or log purge) may
        /// follow a committed entry, so it stays `false`: ambiguous.
        #[serde(default)]
        before_proposal: bool,
    },
}

impl GroupEngineError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::Infra(GroupInfraError::internal(message))
    }

    pub fn cold_backpressure(
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    ) -> Self {
        Self::Infra(GroupInfraError::cold_backpressure(
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        ))
    }

    pub fn raft_uncommitted_backpressure(current: u64, incoming: u64, limit: u64) -> Self {
        Self::Infra(GroupInfraError::raft_uncommitted_backpressure(
            current, incoming, limit,
        ))
    }

    pub fn stream(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::stream_with_next_offset(code, message, None)
    }

    pub fn stream_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: Option<u64>,
    ) -> Self {
        Self::stream_with_context(code, message, next_offset, vec![])
    }

    pub fn stream_with_context(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: Option<u64>,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Stream(StreamEngineError {
            message: format!("{code:?}: {}", message.into()),
            code,
            next_offset,
            context,
        })
    }

    pub fn stream_from_replicated(
        message: impl Into<String>,
        code: StreamErrorCode,
        next_offset: Option<u64>,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Stream(StreamEngineError {
            message: message.into(),
            code,
            next_offset,
            context,
        })
    }

    /// A forward-to-leader error whose command may already have been
    /// proposed (and even committed): callers must not treat it as a
    /// definite rejection.
    pub fn forward_to_leader(
        message: impl Into<String>,
        node_id: Option<u64>,
        address: Option<String>,
    ) -> Self {
        Self::ForwardToLeader {
            message: message.into(),
            leader_hint: GroupLeaderHint { node_id, address },
            before_proposal: false,
        }
    }

    /// A forward-to-leader error raised by a local leadership check before
    /// anything was proposed: the command definitely did not commit here.
    pub fn forward_to_leader_before_proposal(
        message: impl Into<String>,
        node_id: Option<u64>,
        address: Option<String>,
    ) -> Self {
        Self::ForwardToLeader {
            message: message.into(),
            leader_hint: GroupLeaderHint { node_id, address },
            before_proposal: true,
        }
    }

    /// True when this is a forward-to-leader error raised before proposal.
    pub fn is_forward_before_proposal(&self) -> bool {
        matches!(self, Self::ForwardToLeader {
            before_proposal: true,
            ..
        })
    }

    pub fn message(&self) -> Cow<'_, str> {
        match self {
            Self::Stream(err) => Cow::Borrowed(&err.message),
            Self::Infra(err) => err.message(),
            Self::ForwardToLeader { message, .. } => Cow::Borrowed(message),
        }
    }

    pub fn code(&self) -> Option<StreamErrorCode> {
        match self {
            Self::Stream(err) => Some(err.code),
            Self::Infra(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn stream_parts(
        &self,
    ) -> Option<(&str, StreamErrorCode, Option<u64>, &[StreamErrorContext])> {
        match self {
            Self::Stream(err) => Some((&err.message, err.code, err.next_offset, &err.context)),
            Self::Infra(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn next_offset(&self) -> Option<u64> {
        match self {
            Self::Stream(err) => err.next_offset,
            Self::Infra(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn context(&self) -> &[StreamErrorContext] {
        match self {
            Self::Stream(err) => &err.context,
            Self::Infra(_) | Self::ForwardToLeader { .. } => &[],
        }
    }

    pub fn leader_hint(&self) -> Option<&GroupLeaderHint> {
        match self {
            Self::ForwardToLeader { leader_hint, .. } => Some(leader_hint),
            Self::Stream(_) | Self::Infra(_) => None,
        }
    }

    pub fn infra(&self) -> Option<&GroupInfraError> {
        match self {
            Self::Infra(err) => Some(err),
            Self::Stream(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn is_cold_backpressure(&self) -> bool {
        self.infra()
            .is_some_and(GroupInfraError::is_cold_backpressure)
    }

    pub fn is_backpressure(&self) -> bool {
        self.infra().is_some_and(GroupInfraError::is_backpressure)
    }
}
