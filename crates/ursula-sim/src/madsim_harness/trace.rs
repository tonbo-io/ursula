//! Trace types extracted from madsim_harness/mod.rs (DoD #3 modularity refactor).
//! `SimTrace` and `SimEvent` are the stable observation surface used by every
//! scenario, invariant helper, and replay/minimize tool.

use std::cell::RefCell;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimTrace {
    pub events: Vec<SimEvent>,
}

thread_local! {
    static LAST_SIM_TRACE: RefCell<SimTrace> = RefCell::new(SimTrace::default());
}

impl SimTrace {
    pub fn stable_replay(self) -> Self {
        Self {
            events: self
                .events
                .into_iter()
                .filter_map(SimEvent::stable_replay)
                .collect(),
        }
    }

    pub(super) fn push(&mut self, event: SimEvent) {
        self.events.push(event.clone());
        Self::record(event);
    }

    pub(super) fn record(event: SimEvent) {
        LAST_SIM_TRACE.with(|trace| {
            trace.borrow_mut().events.push(event);
        });
    }

    pub fn last_recorded() -> Self {
        LAST_SIM_TRACE.with(|trace| trace.borrow().clone())
    }

    pub(super) fn clear_recorded() {
        LAST_SIM_TRACE.with(|trace| {
            *trace.borrow_mut() = Self::default();
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum SimEvent {
    ClusterBuilt {
        seed: u64,
    },
    LeaderElected {
        leader_id: u64,
    },
    FaultApplied {
        phase: String,
    },
    StreamCreated {
        stream: BucketStreamId,
    },
    AppendCommitted {
        stream: BucketStreamId,
        log_index: u64,
    },
    MajorityApplied {
        log_index: u64,
    },
    AllNodesApplied {
        log_index: u64,
    },
    WaitAppliedBegin {
        node_id: u64,
        log_index: u64,
        description: String,
    },
    WaitAppliedComplete {
        node_id: u64,
        log_index: u64,
        description: String,
    },
    IsolatedFollowerLagged {
        node_id: u64,
        log_index: u64,
    },
    FollowerCaughtUp {
        node_id: u64,
        log_index: u64,
    },
    FollowerReadVerified {
        node_id: u64,
        stream: BucketStreamId,
    },
    LeaderFailoverAppendVerified {
        old_leader_id: u64,
        new_leader_id: u64,
        stream: BucketStreamId,
        first_next_offset: u64,
        second_next_offset: u64,
        log_index: u64,
    },
    LeaderFailoverReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
        node_count: usize,
    },
    AllNodesReadVerified {
        stream: BucketStreamId,
    },
    ReadAttempt {
        node_id: u64,
        stream: BucketStreamId,
        offset: u64,
        max_len: usize,
        attempt: usize,
        payload_len: usize,
    },
    ReadSatisfied {
        node_id: u64,
        stream: BucketStreamId,
        offset: u64,
        max_len: usize,
        attempt: usize,
    },
    SnapshotCreated {
        log_index: u64,
    },
    LogPurged {
        log_index: u64,
    },
    LearnerAdded {
        node_id: u64,
        log_index: u64,
    },
    FullSnapshotTransferred {
        node_id: u64,
        count: usize,
    },
    HeartbeatTriggered {
        node_id: u64,
        reason: String,
        attempt: usize,
    },
    NetworkPolicyChanged {
        action: String,
        source: Option<u64>,
        target: Option<u64>,
        delay_ms: Option<u64>,
    },
    NetworkRpcDecision {
        source: Option<u64>,
        target: u64,
        kind: String,
        delay_ms: Option<u64>,
        partitioned: bool,
    },
    NetworkRpcDelivered {
        source: Option<u64>,
        target: u64,
        kind: String,
    },
    NetworkRpcMissingTarget {
        source: Option<u64>,
        target: u64,
        kind: String,
    },
    SnapshotCatchUpReadVerified {
        node_id: u64,
        stream: BucketStreamId,
    },
    NodeStopped {
        node_id: u64,
    },
    NodeRestarted {
        node_id: u64,
    },
    RestartedNodeReadVerified {
        node_id: u64,
        stream: BucketStreamId,
    },
    ColdChunkWritten {
        stream: BucketStreamId,
        start_offset: u64,
        end_offset: u64,
    },
    ColdFlushed {
        stream: BucketStreamId,
        hot_start_offset: u64,
        log_index: u64,
    },
    ColdLiveReadVerified {
        node_id: u64,
        stream: BucketStreamId,
    },
    ColdReadFaultObserved {
        node_id: u64,
        stream: BucketStreamId,
        message: String,
    },
    ColdWriteFaultObserved {
        stream: BucketStreamId,
        path: String,
        message: String,
    },
    RuntimeRaftNetworkColdWriteFaultRecovered {
        stream_count: usize,
        upload_count_before_retry: u64,
        publish_count_before_retry: u64,
    },
    RuntimeRaftNetworkColdWriteDelayVerified {
        stream_count: usize,
        delay_ms: u64,
        upload_count: u64,
        publish_count: u64,
    },
    RuntimeRaftNetworkColdReadFaultRecovered {
        stream: BucketStreamId,
        returned_len: usize,
    },
    RuntimeRaftNetworkColdReadDelayVerified {
        stream: BucketStreamId,
        delay_ms: u64,
    },
    ColdWriteDelayVerified {
        stream: BucketStreamId,
        delay_ms: u64,
    },
    ColdDeleteFaultObserved {
        stream: BucketStreamId,
        cleanup_attempts: u64,
        cleanup_errors: u64,
    },
    HotReadAfterColdWriteFailureVerified {
        stream: BucketStreamId,
    },
    ColdReadDelayVerified {
        stream: BucketStreamId,
        delay_ms: u64,
    },
    ColdReadTruncateObserved {
        node_id: u64,
        stream: BucketStreamId,
        requested_len: usize,
        returned_len: usize,
        message: String,
    },
    ColdObjectWriteBegin {
        path: String,
        payload_len: usize,
    },
    ColdObjectWriteComplete {
        path: String,
        object_size: u64,
    },
    ColdObjectDeleteBegin {
        path: String,
    },
    ColdObjectDeleteComplete {
        path: String,
    },
    ColdObjectRemoveAllBegin {
        path: String,
    },
    ColdObjectRemoveAllComplete {
        path: String,
    },
    ColdObjectReadBegin {
        stream: Option<BucketStreamId>,
        path: String,
        read_start_offset: u64,
        len: usize,
        object_start: u64,
        object_end: u64,
        cached: bool,
    },
    ColdObjectReadComplete {
        stream: Option<BucketStreamId>,
        path: String,
        read_start_offset: u64,
        len: usize,
        returned_len: usize,
        cached: bool,
    },
    ColdStoreFaultInjected {
        operation: String,
        stream: Option<BucketStreamId>,
        path: String,
        message: String,
    },
    ColdStoreDelayInjected {
        operation: String,
        stream: Option<BucketStreamId>,
        path: String,
        delay_ms: u64,
    },
    ColdStoreTruncateInjected {
        stream: Option<BucketStreamId>,
        path: String,
        requested_len: usize,
        returned_len: usize,
    },
    RuntimeActorsBuilt {
        core_count: usize,
        raft_group_count: usize,
    },
    RuntimeWaitReadStarted {
        stream: BucketStreamId,
        offset: u64,
        max_len: usize,
    },
    RuntimeAppendAfterDelay {
        stream: BucketStreamId,
        delay_ms: u64,
    },
    RuntimeAppendCommitted {
        stream: BucketStreamId,
        start_offset: u64,
        next_offset: u64,
    },
    RuntimeWaitReadSatisfied {
        stream: BucketStreamId,
        payload_len: usize,
    },
    RuntimeReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
    },
    RuntimeMultiClientActorsBuilt {
        stream_count: usize,
        core_count: usize,
        raft_group_count: usize,
    },
    RuntimeMultiClientStreamCreated {
        stream: BucketStreamId,
        core_id: u16,
        raft_group_id: u32,
    },
    RuntimeMultiClientAppendCommitted {
        stream: BucketStreamId,
        client_id: usize,
        append_index: usize,
        start_offset: u64,
        next_offset: u64,
    },
    RuntimeMultiClientReadVerified {
        stream: BucketStreamId,
        client_id: usize,
        expected_len: usize,
        next_offset: u64,
    },
    RuntimeMultiClientVerified {
        stream_count: usize,
        total_appends: usize,
    },
    RuntimeColdFlushActorsBuilt {
        stream_count: usize,
        core_count: usize,
        raft_group_count: usize,
    },
    RuntimeColdFlushStreamCreated {
        stream: BucketStreamId,
        core_id: u16,
        raft_group_id: u32,
    },
    RuntimeColdFlushCompleted {
        flushed_count: usize,
        upload_count: u64,
        publish_count: u64,
    },
    RuntimeColdLiveReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
    },
    RuntimeInterleavingActorsBuilt {
        client_count: usize,
        stream_count: usize,
        core_count: usize,
        raft_group_count: usize,
    },
    RuntimeInterleavingPlanSelected {
        flush_delay_ms: u64,
        read_verify_delay_ms: u64,
    },
    RuntimeInterleavingClientPlanned {
        client_id: usize,
        stream: BucketStreamId,
        first_append_delay_ms: u64,
        second_append_delay_ms: u64,
        core_id: u16,
        raft_group_id: u32,
    },
    RuntimeInterleavingAppendCommitted {
        client_id: usize,
        append_index: usize,
        stream: BucketStreamId,
        start_offset: u64,
        next_offset: u64,
    },
    RuntimeInterleavingFlushCompleted {
        flushed_count: usize,
        upload_count: u64,
        publish_count: u64,
    },
    RuntimeInterleavingReadVerified {
        client_id: usize,
        stream: BucketStreamId,
        expected_len: usize,
        next_offset: u64,
    },
    RuntimeInterleavingColdReadDelayVerified {
        delay_ms: u64,
    },
    RuntimeInterleavingVerified {
        client_count: usize,
        total_appends: usize,
    },
    RuntimeRaftEngineBuilt {
        core_count: usize,
        raft_group_count: usize,
        raft_node_count: usize,
    },
    RuntimeRaftEngineAppendCommitted {
        stream: BucketStreamId,
        start_offset: u64,
        next_offset: u64,
        group_commit_index: u64,
    },
    RuntimeRaftEngineReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
        raft_write_many_batches: u64,
        raft_apply_entries: u64,
    },
    RuntimeRaftSnapshotCaptured {
        stream: BucketStreamId,
        group_commit_index: u64,
        stream_count: usize,
    },
    RuntimeRaftSnapshotInstalledVerified {
        stream: BucketStreamId,
        snapshot_next_offset: u64,
        post_restore_next_offset: u64,
    },
    RuntimeRaftNetworkBuilt {
        core_count: usize,
        raft_group_count: usize,
        raft_node_count: usize,
        leader_id: u64,
    },
    RuntimeRaftNetworkReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
        raft_write_many_batches: u64,
        raft_apply_entries: u64,
        delivered_rpc_count: usize,
    },
    RuntimeRaftNetworkPartialReadVerified {
        stream: BucketStreamId,
        after_event: String,
        offset: u64,
        max_len: usize,
        next_offset: u64,
    },
    RuntimeRaftNetworkTailReadVerified {
        stream: BucketStreamId,
        after_event: String,
        offset: u64,
        next_offset: u64,
    },
    RuntimeRaftNetworkCloseVerified {
        stream: BucketStreamId,
        after_event: String,
        next_offset: u64,
        group_commit_index: u64,
        append_rejected: bool,
    },
    RuntimeRaftNetworkSnapshotPublishedVerified {
        stream: BucketStreamId,
        after_event: String,
        snapshot_offset: u64,
        snapshot_len: usize,
        next_offset: u64,
        group_commit_index: u64,
    },
    RuntimeRaftNetworkLeaderFailoverVerified {
        stream: BucketStreamId,
        old_leader_id: u64,
        new_leader_id: u64,
        next_offset: u64,
        group_commit_index: u64,
    },
    RuntimeRaftNetworkLeaderFailoverReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
    },
    RuntimeRaftNetworkLeaderFailoverStageReached {
        stage: String,
        old_leader_id: u64,
        current_leader_id: Option<u64>,
        log_index: Option<u64>,
    },
    HttpProtocolSurfaceVerified {
        stream: BucketStreamId,
        next_offset: u64,
        expired_at_ms: u64,
    },
    HttpSnapshotProtocolSurfaceVerified {
        stream: BucketStreamId,
        snapshot_offset: u64,
        next_offset: u64,
    },
    HttpLiveProtocolSurfaceVerified {
        stream: BucketStreamId,
        long_poll_next_offset: u64,
        sse_next_offset: u64,
    },
    HttpLiveLimitProtocolSurfaceVerified {
        stream: BucketStreamId,
        timeout_next_offset: u64,
        backpressure_events: u64,
    },
    HttpProducerProtocolSurfaceVerified {
        stream: BucketStreamId,
        producer_count: usize,
        final_next_offset: u64,
        gap_expected_seq: u64,
        stale_epoch: u64,
    },
    HttpProtocolSurfaceRandomizedPlanSelected {
        stream: BucketStreamId,
        ttl: bool,
        producer_sessions: bool,
        #[serde(default)]
        producer_sequence_gap: bool,
        producer_epoch_bump: bool,
        #[serde(default)]
        concurrent_producers: bool,
        long_poll: bool,
        sse_close: bool,
        live_limit: bool,
        #[serde(default)]
        live_timeout: bool,
        #[serde(default)]
        partial_reads: bool,
    },
    HttpProtocolSurfaceRandomizedVerified {
        stream: BucketStreamId,
        final_next_offset: u64,
        ttl_checked: bool,
        producer_sessions: bool,
        #[serde(default)]
        producer_sequence_gap: bool,
        #[serde(default)]
        concurrent_producers: bool,
        long_poll: bool,
        sse_close: bool,
        live_limit: bool,
        #[serde(default)]
        live_timeout: bool,
        #[serde(default)]
        partial_reads: bool,
    },
    HttpProtocolSurfaceRandomizedProducerGapRejected {
        stream: BucketStreamId,
        expected_seq: u64,
        received_seq: u64,
    },
    HttpProtocolSurfaceRandomizedConcurrentProducersVerified {
        stream: BucketStreamId,
        producer_count: usize,
        next_offset: u64,
    },
    HttpProtocolSurfaceRandomizedPartialReadVerified {
        stream: BucketStreamId,
        offset: u64,
        max_bytes: u64,
        next_offset: u64,
    },
    HttpProtocolSurfaceRandomizedLiveTimeoutVerified {
        stream: BucketStreamId,
        timeout_ms: u64,
    },
    RuntimeRaftNetworkProducerDuplicateVerified {
        stream: BucketStreamId,
        producer_id: String,
        producer_seq: u64,
        item_count: usize,
    },
    RuntimeRaftNetworkProducerStaleEpochRejected {
        stream: BucketStreamId,
        producer_id: String,
        producer_epoch: u64,
        producer_seq: u64,
    },
    RuntimeRaftNetworkConcurrentProducersVerified {
        stream: BucketStreamId,
        producer_count: usize,
        start_offset: u64,
        next_offset: u64,
    },
    RuntimeRaftNetworkColdLiveReadVerified {
        stream: BucketStreamId,
        next_offset: u64,
        flushed_count: usize,
        upload_count: u64,
        publish_count: u64,
    },
    RuntimeRaftNetworkLeaderFailoverColdLiveReadVerified {
        stream_count: usize,
        old_leader_id: u64,
        current_leader_id: u64,
        flushed_count: usize,
    },
    InvariantFailed {
        invariant: String,
        after_event: String,
        message: String,
    },
}

impl SimEvent {
    pub fn stable_replay(self) -> Option<Self> {
        match self {
            Self::ColdObjectWriteBegin { .. }
            | Self::ColdObjectWriteComplete { .. }
            | Self::ColdObjectDeleteBegin { .. }
            | Self::ColdObjectDeleteComplete { .. }
            | Self::ColdObjectRemoveAllBegin { .. }
            | Self::ColdObjectRemoveAllComplete { .. }
            | Self::ColdObjectReadBegin { .. }
            | Self::ColdObjectReadComplete { .. }
            | Self::ColdStoreFaultInjected { .. }
            | Self::ColdStoreDelayInjected { .. }
            | Self::ColdStoreTruncateInjected { .. } => None,
            Self::RuntimeRaftNetworkReadVerified {
                stream,
                next_offset,
                raft_write_many_batches,
                raft_apply_entries,
                delivered_rpc_count,
            } => Some(Self::RuntimeRaftNetworkReadVerified {
                stream,
                next_offset,
                raft_write_many_batches: u64::from(raft_write_many_batches > 0),
                raft_apply_entries: u64::from(raft_apply_entries > 0),
                delivered_rpc_count: usize::from(delivered_rpc_count > 0),
            }),
            event => Some(event),
        }
    }
}
