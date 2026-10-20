use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use futures_util::Stream;
use futures_util::StreamExt;
use openraft::EmptyNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::VoteOf;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::vote::RaftLeaderId;
use prost::Message;
use serde::de::DeserializeOwned;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ErrorChain;
use ursula_runtime::FollowerRead;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use crate::codec::decode_wire;
use crate::codec::encode_wire;
use crate::codec::placement_from_parts;
use crate::codec::required;
use crate::format_epoch::PROTOCOL_MISMATCH_TEXT;
use crate::format_epoch::observe_outbound_status;
use crate::format_epoch::record_format_epoch_mismatch;
use crate::forward::write_commands_on_raft;
use crate::memberships::BarrierAnswer;
use crate::memberships::LeaderMemberships;
use crate::peer_channel::peer_endpoint;
use crate::peer_cluster::ClusterCheck;
use crate::peer_cluster::PeerCluster;
use crate::peer_cluster::PeerIdentity;
use crate::raft_internal_proto;
use crate::target::RaftTarget;
use crate::target::TargetedRequest;
use crate::types::SnapshotData;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaSnapshot;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

/// Reply to an append-stream caller that may have stopped waiting. A dropped
/// receiver makes the reply moot.
fn reply_to<T>(tx: oneshot::Sender<T>, value: T) {
    if tx.send(value).is_err() {
        tracing::trace!("append-stream caller stopped waiting before the reply");
    }
}

/// Shared only by groups constructed on one owner core. Connections and
/// encoders are created by that core, never by a process-wide first caller.
#[derive(Debug)]
pub struct CoreRaftTransport {
    pub(crate) budget: AppendTransportBudget,
    /// Stamped on every request this transport sends.
    identity: PeerIdentity,
    /// Where a peer's refusal for another cluster id is recorded.
    peer_cluster: PeerCluster,
    channels: Mutex<BTreeMap<String, SharedRaftChannel>>,
    sessions: Mutex<BTreeMap<String, SharedAppendSession>>,
}
/// Node-wide accounting shared across owner pools and peers. Connections remain core-local.
#[derive(Debug, Clone)]
pub(crate) struct AppendTransportBudget {
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) bytes: usize,
}
impl AppendTransportBudget {
    pub(crate) fn new(bytes: usize) -> Self {
        let bytes = bytes
            .clamp(1, u32::MAX as usize)
            .min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(bytes)),
            bytes,
        }
    }
}
impl Default for AppendTransportBudget {
    fn default() -> Self {
        Self::new(128 * 1024 * 1024)
    }
}
impl CoreRaftTransport {
    /// A transport that sends as `identity` and records peers that refuse it
    /// for another cluster id in `peer_cluster`.
    pub fn new(identity: PeerIdentity, peer_cluster: PeerCluster) -> Self {
        Self::with_budget(
            AppendTransportBudget::new(RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES),
            identity,
            peer_cluster,
        )
    }

    pub(crate) fn with_budget(
        budget: AppendTransportBudget,
        identity: PeerIdentity,
        peer_cluster: PeerCluster,
    ) -> Self {
        Self {
            channels: Mutex::default(),
            sessions: Mutex::default(),
            budget,
            identity,
            peer_cluster,
        }
    }
}
/// A transport of the test cluster ([`crate::peer_cluster::test_identity`]).
#[cfg(test)]
impl Default for CoreRaftTransport {
    fn default() -> Self {
        Self::new(
            crate::peer_cluster::test_identity(0),
            PeerCluster::default(),
        )
    }
}
static GRPC_APPEND_STREAM_SESSIONS_OPENED: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_SESSION_FAILURES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_RESPONSES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_REQUEST_FRAMES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_RESPONSE_FRAMES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_BATCH_FRAMES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_BATCH_ITEMS_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_FRAME_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_INFLIGHT: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_INFLIGHT_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_QUEUED_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_QUEUED_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_BACKPRESSURE_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_EXPIRED_UNSENT: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_STALLS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
/// Logical accounting for heartbeat Append RPCs. One call updates both
/// counters, so they share a cache line.
struct AppendHeartbeatCounters {
    requests: AtomicU64,
    request_bytes: AtomicU64,
}

/// Logical accounting for replicating Append RPCs, updated together like
/// [`AppendHeartbeatCounters`].
struct AppendReplicationCounters {
    requests: AtomicU64,
    request_bytes: AtomicU64,
    entries: AtomicU64,
}

// Heartbeat and replication accounting run on different cores, so each kind
// gets its own cache line. Padding every counter separately makes one call
// touch two or three contended lines, which measured slower than no padding at
// eight cores (`benches/support/grpc_counters.rs`).
static GRPC_APPEND_HEARTBEAT: CachePadded<AppendHeartbeatCounters> =
    CachePadded::new(AppendHeartbeatCounters {
        requests: AtomicU64::new(0),
        request_bytes: AtomicU64::new(0),
    });
static GRPC_APPEND_REPLICATION: CachePadded<AppendReplicationCounters> =
    CachePadded::new(AppendReplicationCounters {
        requests: AtomicU64::new(0),
        request_bytes: AtomicU64::new(0),
        entries: AtomicU64::new(0),
    });
static GRPC_APPEND_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_VOTE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_VOTE_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_VOTE_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_PAYLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
use crate::registry::RaftGroupHandleRegistry;

/// Why an AppendStream call failed: its session's byte budget stayed full
/// until the call's deadline, or the call got a gRPC error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AppendCallError {
    /// Congestion on a live stream, not a broken channel: rebuilding the
    /// channel would not drain it. To OpenRaft the peer is unreachable, so
    /// replication backs off.
    #[error("raft append stream backlog full: {queued} of {budget} bytes queued")]
    BacklogFull { queued: usize, budget: usize },
    #[error(transparent)]
    Status(#[from] tonic::Status),
}

impl From<AppendCallError> for tonic::Status {
    fn from(error: AppendCallError) -> Self {
        match error {
            AppendCallError::BacklogFull { .. } => tonic::Status::unavailable(error.to_string()),
            AppendCallError::Status(status) => status,
        }
    }
}

/// A Raft gRPC client that stamps this node's [`PeerIdentity`] on every
/// request.
pub(crate) type RaftClient = raft_internal_proto::raft_internal_client::RaftInternalClient<
    InterceptedService<Channel, PeerIdentity>,
>;

/// A Raft gRPC server that admits only peers of this node's cluster.
pub type RaftGrpcServer<S> = InterceptedService<
    raft_internal_proto::raft_internal_server::RaftInternalServer<S>,
    ClusterCheck,
>;

pub use ursula_proto::admin::QuorumPrefix;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecoveryProbeError {
    /// The peer has no channel: `raft.peers` does not list it, or its URL
    /// is invalid. Nothing was sent.
    #[error("recovery transport: {0}")]
    Channel(#[from] RaftChannelError),
    #[error("recovery RPC: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("recovery payload: {}", ErrorChain(.0))]
    Payload(#[from] crate::codec::WireDecodeError),
    #[error("recovery peer does not report itself as committed leader")]
    NotLeader,
    /// The address dialed serves another node now, which refused the
    /// barrier as meant for another node: no answer.
    #[error("the recovery peer's address serves node {node} now")]
    OtherNode { node: u64 },
    /// The answer is about another group than the one asked about.
    #[error("the recovery peer answered about plane {plane} group {raft_group_id}")]
    OtherTarget { plane: i32, raft_group_id: u32 },
    /// The answer's memberships are not a live leader's.
    #[error("recovery answer: {0}")]
    Memberships(#[from] crate::memberships::MalformedMembership),
}

/// Confirm a group's current quorum without issuing an application write.
/// This is a point-in-time observation, not a maintenance reservation or a
/// promise that another participant cannot disrupt a voter immediately after.
#[cfg(all(test, not(madsim)))]
pub(crate) async fn confirm_quorum_prefix(
    transport: Arc<CoreRaftTransport>,
    placement: ursula_shard::ShardPlacement,
    leader_id: u64,
    timeout: Duration,
) -> Result<QuorumPrefix, RecoveryProbeError> {
    let answer = probe_rejoin_vote_barrier(transport, placement, leader_id, timeout).await?;
    Ok(QuorumPrefix {
        raft_group_id: placement.raft_group_id.0,
        leader_id,
        leader_term: answer.vote.leader_id().term(),
        required_applied_index: answer.index,
    })
}

/// Fresh recovery evidence through the explicit ReadIndex barrier RPC.
/// ReadIndexBarrier only coalesces rounds whose confirmation has not started;
/// an inbound request never joins an already-started confirmation round.
/// The leader is dialed at the cluster URL `raft.peers` lists for it.
pub(crate) async fn probe_rejoin_vote_barrier(
    transport: Arc<CoreRaftTransport>,
    placement: ursula_shard::ShardPlacement,
    leader_id: u64,
    timeout: Duration,
) -> Result<BarrierAnswer, RecoveryProbeError> {
    let network = GrpcRaftNetwork::new(transport, placement.raft_group_id, leader_id);
    let channel = network.channel.as_ref().map_err(Clone::clone)?;
    let mut client = channel.client.clone();
    let mut request = tonic::Request::new(network.stamped(
        raft_internal_proto::RejoinBarrierRequestV1 {
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            ..Default::default()
        },
    ));
    request.set_timeout(timeout);
    let response = match client.rejoin_barrier(request).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            if let Some(node) = crate::target::refusing_node(&status) {
                return Err(RecoveryProbeError::OtherNode { node });
            }
            network.transport.peer_cluster.observe_refusal(
                leader_id,
                &channel.endpoint,
                &status,
                crate::rt::time::Instant::now(),
            );
            return Err(status.into());
        }
    };
    // The answer must echo the target, and its memberships are required:
    // empty bytes do not decode, and a membership without voters is refused,
    // never read as an answer that a node is no member.
    let target = network.raft_target();
    if response.plane != i32::from(target.plane())
        || response.raft_group_id != target.wire_group_id()
    {
        return Err(RecoveryProbeError::OtherTarget {
            plane: response.plane,
            raft_group_id: response.raft_group_id,
        });
    }
    let vote: UrsulaVote = decode_wire(&response.vote, "rejoin barrier vote")?;
    if !vote.is_committed() || *vote.leader_id().node_id() != leader_id {
        return Err(RecoveryProbeError::NotLeader);
    }
    let memberships: LeaderMemberships =
        decode_wire(&response.memberships, "rejoin barrier memberships")?;
    memberships.check()?;
    Ok(BarrierAnswer {
        vote,
        index: response.index,
        memberships,
    })
}

#[derive(Debug, Clone)]
struct SharedRaftChannel {
    generation: u64,
    channel: Channel,
}

#[derive(Debug, Clone)]
struct SharedAppendSession {
    _task: Arc<AppendSessionTask>,
    sender: mpsc::UnboundedSender<AppendStreamCall>,
    /// Bytes of Append calls this session may hold before they reach the HTTP/2 encoder
    /// ([`RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES`]).
    budget: Arc<Semaphore>,
}

#[derive(Debug)]
enum AppendSessionTask {
    Running(tokio::task::JoinHandle<()>),
    #[cfg(test)]
    Paused,
}

impl Drop for AppendSessionTask {
    fn drop(&mut self) {
        match self {
            Self::Running(task) => task.abort(),
            #[cfg(test)]
            Self::Paused => {}
        }
    }
}

struct AppendStreamCall {
    envelope: raft_internal_proto::RaftRpcEnvelopeV1,
    response: oneshot::Sender<Result<raft_internal_proto::RaftRpcAckV1, tonic::Status>>,
    queued: QueuedAppendBytes,
}

/// A share of a byte budget, held from the moment an Append call (or an inbound frame) is
/// buffered until it leaves the buffer: on the leader when the HTTP/2 encoder takes the frame or
/// the caller gave up before it was sent; on the follower when the frame's items are answered.
/// Keeps the process-wide gauge in step.
struct QueuedAppendBytes {
    _permit: OwnedSemaphorePermit,
    bytes: u64,
    gauge: &'static AtomicU64,
}

impl QueuedAppendBytes {
    fn new(
        permit: OwnedSemaphorePermit,
        bytes: u64,
        gauge: &'static AtomicU64,
        max: &'static AtomicU64,
    ) -> Self {
        let now = gauge
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        max.fetch_max(now, Ordering::Relaxed);
        Self {
            _permit: permit,
            bytes,
            gauge,
        }
    }
}

impl Drop for QueuedAppendBytes {
    fn drop(&mut self) {
        self.gauge.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Budget charge of a buffered message of `encoded_len` bytes: at least
/// [`RAFT_GRPC_APPEND_STREAM_MIN_CHARGE_BYTES`] so tiny heartbeats bound the queue length too, and
/// at most the whole budget so one oversized message still passes when nothing else is queued.
fn append_budget_charge(encoded_len: usize, budget: usize) -> u32 {
    let charge = encoded_len
        .max(RAFT_GRPC_APPEND_STREAM_MIN_CHARGE_BYTES)
        .min(budget);
    u32::try_from(charge).unwrap_or(u32::MAX)
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct RaftGrpcMetricsSnapshot {
    pub raft_grpc_append_stream_sessions_opened: u64,
    pub raft_grpc_append_stream_session_failures: u64,
    pub raft_grpc_append_stream_requests: u64,
    pub raft_grpc_append_stream_responses: u64,
    pub raft_grpc_append_stream_request_bytes: u64,
    pub raft_grpc_append_stream_response_bytes: u64,
    pub raft_grpc_append_stream_request_frames: u64,
    pub raft_grpc_append_stream_response_frames: u64,
    pub raft_grpc_append_stream_batch_frames: u64,
    pub raft_grpc_append_stream_batch_items_max: u64,
    /// Encoded bytes of the largest request frame sent, before compression. Bounded by
    /// `RAFT_GRPC_APPEND_STREAM_MAX_FRAME_BYTES` plus one call.
    pub raft_grpc_append_stream_frame_bytes_max: u64,
    pub raft_grpc_append_stream_inflight: u64,
    pub raft_grpc_append_stream_inflight_max: u64,
    /// Leader side: bytes of Append calls queued for peers but not yet taken by the HTTP/2
    /// encoder, bounded per peer by `RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES`.
    pub raft_grpc_append_stream_queued_bytes: u64,
    pub raft_grpc_append_stream_queued_bytes_max: u64,
    /// Append calls refused because the peer's queue was full (the replication
    /// stream backs off instead of piling up more copies of its entries).
    pub raft_grpc_append_stream_backpressure_rejections: u64,
    /// Queued Append calls dropped unsent because their caller had already timed out.
    pub raft_grpc_append_stream_expired_unsent: u64,
    /// Append sessions closed because the peer stopped answering.
    pub raft_grpc_append_stream_stalls: u64,
    /// Follower side: decoded inbound Append frames not yet answered.
    pub raft_grpc_append_stream_server_buffered_bytes: u64,
    pub raft_grpc_append_stream_server_buffered_bytes_max: u64,
    /// Logical protobuf bytes before tonic's optional ZSTD compression and
    /// HTTP/2 framing. Compare these counters with VPC/CUR bytes to calculate
    /// transport and billing amplification.
    pub raft_grpc_append_heartbeat_requests: u64,
    pub raft_grpc_append_heartbeat_request_bytes: u64,
    pub raft_grpc_append_replication_requests: u64,
    pub raft_grpc_append_replication_request_bytes: u64,
    pub raft_grpc_append_replication_entries: u64,
    pub raft_grpc_append_response_bytes: u64,
    pub raft_grpc_vote_requests: u64,
    pub raft_grpc_vote_request_bytes: u64,
    pub raft_grpc_vote_response_bytes: u64,
    pub raft_grpc_snapshot_requests: u64,
    pub raft_grpc_snapshot_request_bytes: u64,
    pub raft_grpc_snapshot_payload_bytes: u64,
    pub raft_grpc_snapshot_response_bytes: u64,
}

pub fn raft_grpc_metrics_snapshot() -> RaftGrpcMetricsSnapshot {
    RaftGrpcMetricsSnapshot {
        raft_grpc_append_stream_sessions_opened: GRPC_APPEND_STREAM_SESSIONS_OPENED
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_session_failures: GRPC_APPEND_STREAM_SESSION_FAILURES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_requests: GRPC_APPEND_STREAM_REQUESTS.load(Ordering::Relaxed),
        raft_grpc_append_stream_responses: GRPC_APPEND_STREAM_RESPONSES.load(Ordering::Relaxed),
        raft_grpc_append_stream_request_bytes: GRPC_APPEND_STREAM_REQUEST_BYTES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_response_bytes: GRPC_APPEND_STREAM_RESPONSE_BYTES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_request_frames: GRPC_APPEND_STREAM_REQUEST_FRAMES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_response_frames: GRPC_APPEND_STREAM_RESPONSE_FRAMES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_batch_frames: GRPC_APPEND_STREAM_BATCH_FRAMES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_batch_items_max: GRPC_APPEND_STREAM_BATCH_ITEMS_MAX
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_frame_bytes_max: GRPC_APPEND_STREAM_FRAME_BYTES_MAX
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_inflight: GRPC_APPEND_STREAM_INFLIGHT.load(Ordering::Relaxed),
        raft_grpc_append_stream_inflight_max: GRPC_APPEND_STREAM_INFLIGHT_MAX
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_queued_bytes: GRPC_APPEND_STREAM_QUEUED_BYTES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_queued_bytes_max: GRPC_APPEND_STREAM_QUEUED_BYTES_MAX
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_backpressure_rejections: GRPC_APPEND_STREAM_BACKPRESSURE_REJECTIONS
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_expired_unsent: GRPC_APPEND_STREAM_EXPIRED_UNSENT
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_stalls: GRPC_APPEND_STREAM_STALLS.load(Ordering::Relaxed),
        raft_grpc_append_stream_server_buffered_bytes: GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES
            .load(Ordering::Relaxed),
        raft_grpc_append_stream_server_buffered_bytes_max:
            GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES_MAX.load(Ordering::Relaxed),
        raft_grpc_append_heartbeat_requests: GRPC_APPEND_HEARTBEAT.requests.load(Ordering::Relaxed),
        raft_grpc_append_heartbeat_request_bytes: GRPC_APPEND_HEARTBEAT
            .request_bytes
            .load(Ordering::Relaxed),
        raft_grpc_append_replication_requests: GRPC_APPEND_REPLICATION
            .requests
            .load(Ordering::Relaxed),
        raft_grpc_append_replication_request_bytes: GRPC_APPEND_REPLICATION
            .request_bytes
            .load(Ordering::Relaxed),
        raft_grpc_append_replication_entries: GRPC_APPEND_REPLICATION
            .entries
            .load(Ordering::Relaxed),
        raft_grpc_append_response_bytes: GRPC_APPEND_RESPONSE_BYTES.load(Ordering::Relaxed),
        raft_grpc_vote_requests: GRPC_VOTE_REQUESTS.load(Ordering::Relaxed),
        raft_grpc_vote_request_bytes: GRPC_VOTE_REQUEST_BYTES.load(Ordering::Relaxed),
        raft_grpc_vote_response_bytes: GRPC_VOTE_RESPONSE_BYTES.load(Ordering::Relaxed),
        raft_grpc_snapshot_requests: GRPC_SNAPSHOT_REQUESTS.load(Ordering::Relaxed),
        raft_grpc_snapshot_request_bytes: GRPC_SNAPSHOT_REQUEST_BYTES.load(Ordering::Relaxed),
        raft_grpc_snapshot_payload_bytes: GRPC_SNAPSHOT_PAYLOAD_BYTES.load(Ordering::Relaxed),
        raft_grpc_snapshot_response_bytes: GRPC_SNAPSHOT_RESPONSE_BYTES.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppendLogicalSample {
    heartbeat: bool,
    request_bytes: u64,
    entries: u64,
}

fn append_logical_sample(
    request: &UrsulaAppendEntriesRequest,
    envelope_bytes: usize,
) -> AppendLogicalSample {
    AppendLogicalSample {
        heartbeat: request.entries.is_empty(),
        request_bytes: envelope_bytes as u64,
        entries: request.entries.len() as u64,
    }
}

fn record_append_logical_sample(sample: AppendLogicalSample) {
    if sample.heartbeat {
        GRPC_APPEND_HEARTBEAT
            .requests
            .fetch_add(1, Ordering::Relaxed);
        GRPC_APPEND_HEARTBEAT
            .request_bytes
            .fetch_add(sample.request_bytes, Ordering::Relaxed);
        return;
    }
    GRPC_APPEND_REPLICATION
        .requests
        .fetch_add(1, Ordering::Relaxed);
    GRPC_APPEND_REPLICATION
        .request_bytes
        .fetch_add(sample.request_bytes, Ordering::Relaxed);
    GRPC_APPEND_REPLICATION
        .entries
        .fetch_add(sample.entries, Ordering::Relaxed);
}

pub const RAFT_GRPC_APPEND_PATH: &str = "/ursula.raft.v1.RaftInternal/Append";
pub const RAFT_GRPC_APPEND_STREAM_PATH: &str = "/ursula.raft.v1.RaftInternal/AppendStream";
pub const RAFT_GRPC_VOTE_PATH: &str = "/ursula.raft.v1.RaftInternal/Vote";
pub const RAFT_GRPC_FULL_SNAPSHOT_PATH: &str = "/ursula.raft.v1.RaftInternal/FullSnapshot";
pub const RAFT_GRPC_GROUP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupWrite";
pub const RAFT_GRPC_GROUP_READ_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupRead";
pub const RAFT_GRPC_REJOIN_BARRIER_PATH: &str = "/ursula.raft.v1.RaftInternal/RejoinBarrier";
pub const RAFT_GRPC_TRANSFER_LEADER_PATH: &str = "/ursula.raft.v1.RaftInternal/TransferLeader";
pub const RAFT_GRPC_GENESIS_PROBE_PATH: &str = "/ursula.raft.v1.RaftInternal/GenesisProbe";
pub const RAFT_GRPC_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
/// The Raft gRPC protocol version is the format epoch: Ursula 0.7 speaks
/// protocol 3, 0.8 protocol 4 and 0.9 protocol 5, each refuses this one with
/// its own check, and this binary refuses theirs.
pub(crate) const RAFT_GRPC_PROTOCOL_VERSION: u32 = ursula_stream::FORMAT_EPOCH;
const RAFT_GRPC_APPEND_STREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const RAFT_GRPC_APPEND_STREAM_MAX_BATCH_ITEMS: usize = 32;
/// Encoded bytes of the Append calls an AppendStream frame collects, besides the call count. A
/// frame stops taking calls once it holds this many, so it exceeds it by at most one call, which
/// the log reader bounds to 4 MiB of entries unless one entry alone is larger. tonic keeps each
/// stream's encode, compression and decode buffers at the largest message the stream ever
/// carried, and a session lives until its peer restarts: unbounded frames left every node holding
/// GBs of idle buffers after one burst of large appends (#507). An entry larger than a read's
/// budget still travels alone and leaves its stream's buffers at its size (#518).
const RAFT_GRPC_APPEND_STREAM_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
/// Leader side, per peer: bytes of Append calls queued (not yet taken by the HTTP/2 encoder).
/// Beyond it a call fails fast as `Unavailable`, which OpenRaft treats as an unreachable peer and
/// backs off on. Before this bound a lagging peer's queue grew without limit: every replication
/// retry after a 250 ms RPC timeout queued another copy of the same entries while the timed-out
/// copies were still sent (EKS, 128 SQLite-VFS owners: ~0.7 GB/min per node to OOM).
const RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;
const RAFT_GRPC_APPEND_STREAM_MIN_CHARGE_BYTES: usize = 1024;
/// Request frames between the session loop and the HTTP/2 encoder. Kept small so queued calls
/// wait where an expired caller can still be skipped.
const RAFT_GRPC_APPEND_STREAM_WIRE_FRAMES: usize = 4;
/// A session with calls outstanding and no response for this long is closed (its calls fail as
/// `Unavailable`); the next call opens a fresh stream.
const RAFT_GRPC_APPEND_STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(15);
const RAFT_GRPC_ZSTD_MIN_MESSAGE_BYTES: usize = 1024;

#[derive(Debug)]
pub(crate) struct GrpcRpcError {
    code: tonic::Code,
    message: String,
    /// This node's id, on a refusal of a request meant for another node
    /// ([`Self::misdelivered`]).
    refusing_node: Option<u64>,
}

impl GrpcRpcError {
    fn new(code: tonic::Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            refusing_node: None,
        }
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(tonic::Code::InvalidArgument, message)
    }

    pub(crate) fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(tonic::Code::FailedPrecondition, message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(tonic::Code::NotFound, message)
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(tonic::Code::Unavailable, message)
    }

    /// The refusal of a request for `target_node` that reached node `local`:
    /// the address the sender dialed serves another node now.
    /// `FAILED_PRECONDITION` with `local` in the status's `ursula-node-id`
    /// metadata, which the sender reads with
    /// [`crate::target::refusing_node`].
    pub(crate) fn misdelivered(local: u64, target_node: u64) -> Self {
        Self {
            code: tonic::Code::FailedPrecondition,
            message: format!(
                "this node is node {local}, and the request is for node {target_node}: the \
                 address serves another node now; check raft.peers on the sender"
            ),
            refusing_node: Some(local),
        }
    }
}

/// The one rendering of a Raft gRPC refusal: a misdelivery refusal carries
/// the refusing node's id in its status metadata.
impl From<GrpcRpcError> for tonic::Status {
    fn from(error: GrpcRpcError) -> Self {
        refusal_status(error.code, error.message, error.refusing_node)
    }
}

/// A refusal's status, with the refusing node of a misdelivery refusal in
/// its `ursula-node-id` metadata, where [`crate::target::refusing_node`]
/// reads it.
fn refusal_status(code: tonic::Code, message: String, refusing_node: Option<u64>) -> tonic::Status {
    let mut status = tonic::Status::new(code, message);
    if let Some(node) = refusing_node {
        status.metadata_mut().insert(
            crate::peer_cluster::NODE_ID_METADATA,
            tonic::metadata::AsciiMetadataValue::from(node),
        );
    }
    status
}

/// A refused `AppendStream` item as the stream carries it: an item has no
/// status metadata, so the misdelivery refusal's node travels in
/// `refusing_node` (0 for none, as no node id is 0).
pub(crate) fn stream_item_error(
    status: &tonic::Status,
) -> raft_internal_proto::RaftAppendStreamError {
    raft_internal_proto::RaftAppendStreamError {
        code: status.code() as i32,
        message: status.message().to_owned(),
        refusing_node: crate::target::refusing_node(status).unwrap_or(0),
    }
}

/// The sender's status of a refused `AppendStream` item, with a misdelivery
/// refusal's node back in its metadata, so it reads as the same refusal of
/// a unary call does.
fn stream_item_status(error: raft_internal_proto::RaftAppendStreamError) -> tonic::Status {
    refusal_status(
        tonic::Code::from_i32(error.code),
        error.message,
        (error.refusing_node != 0).then_some(error.refusing_node),
    )
}

#[derive(Debug, Clone)]
pub struct RaftGrpcService {
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
}

impl RaftGrpcService {
    pub fn new(registry: RaftGroupHandleRegistry) -> Self {
        Self {
            registry,
            cold_store: None,
        }
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }
}

pub fn raft_grpc_service(registry: RaftGroupHandleRegistry) -> RaftGrpcServer<RaftGrpcService> {
    let peer_cluster = registry.peer_cluster().clone();
    raft_grpc_server(RaftGrpcService::new(registry), &peer_cluster)
}

/// Serves `service` on the Raft gRPC plane: the cluster-id check of
/// `peer_cluster` runs before a request is decoded, then the plane's
/// compression and message limits apply.
pub fn raft_grpc_server<S>(service: S, peer_cluster: &PeerCluster) -> RaftGrpcServer<S>
where S: raft_internal_proto::raft_internal_server::RaftInternal {
    InterceptedService::new(
        raft_internal_proto::raft_internal_server::RaftInternalServer::new(service)
            .accept_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES),
        peer_cluster.server_check(),
    )
}

/// The gRPC status of a group engine's error, its one conversion on the Raft
/// plane: `NOT_FOUND` for a group this node does not host, `UNAVAILABLE`
/// when the peer should back off before it sends again
/// ([`ursula_runtime::GroupEngineError::peer_backs_off`]), else `INTERNAL`.
pub(crate) fn group_rpc_status(error: ursula_runtime::GroupEngineError) -> tonic::Status {
    tonic::Status::new(group_rpc_code(&error), error.to_string())
}

/// The gRPC code of a group's error: `NOT_FOUND` for a group this node does
/// not run, `UNAVAILABLE` when the peer backs off, `INTERNAL` otherwise.
pub(crate) fn group_rpc_code(error: &ursula_runtime::GroupEngineError) -> tonic::Code {
    if let Some(ursula_runtime::GroupInfraError::RaftGroupNotRegistered { .. }) = error.infra() {
        return tonic::Code::NotFound;
    }
    if error.peer_backs_off() {
        return tonic::Code::Unavailable;
    }
    tonic::Code::Internal
}

/// The gRPC status of a leadership transfer this node refused or could not
/// hand to OpenRaft, its one conversion (#539): the group's own error as
/// [`group_rpc_status`] converts it (`NOT_FOUND` for a group this node does
/// not run), `UNAVAILABLE` for a stopped Raft core, and
/// `FAILED_PRECONDITION` for the election policy's refusal. No refusal
/// carries the protocol-mismatch text, so none reads as another format
/// epoch ([`crate::format_epoch`]).
impl From<crate::IncomingTransferError> for tonic::Status {
    fn from(error: crate::IncomingTransferError) -> Self {
        use crate::IncomingTransferError;
        match error {
            IncomingTransferError::Group(error) => group_rpc_status(error),
            IncomingTransferError::Raft { .. } => {
                tonic::Status::unavailable(ErrorChain(&error).to_string())
            }
            IncomingTransferError::OwnCommittedVote { .. }
            | IncomingTransferError::MayNotCampaign { .. } => {
                tonic::Status::failed_precondition(error.to_string())
            }
        }
    }
}

/// The gRPC status of a recovery barrier this node could not confirm, its
/// one conversion: `NOT_FOUND` for a group it does not run, `UNAVAILABLE`
/// when the ReadIndex round failed, and `FAILED_PRECONDITION` when this
/// replica does not lead (or not yet, or no longer).
impl From<crate::QuorumProofError> for tonic::Status {
    fn from(error: crate::QuorumProofError) -> Self {
        use crate::QuorumProofError;
        let code = match &error {
            QuorumProofError::NotRegistered { .. } => tonic::Code::NotFound,
            QuorumProofError::Read { .. } => tonic::Code::Unavailable,
            QuorumProofError::NotLeader { .. }
            | QuorumProofError::LeadershipChanged { .. }
            | QuorumProofError::HistoryPending { .. } => tonic::Code::FailedPrecondition,
        };
        tonic::Status::new(code, error.to_string())
    }
}

/// How an Append arrived: alone, or as an item of an `AppendStream` whose
/// `ursula-raft-plane` metadata named a plane (`None`: none, or an unknown
/// one).
#[derive(Debug, Clone, Copy)]
enum AppendVia {
    Unary,
    Stream(Option<raft_internal_proto::RaftPlaneV1>),
}

async fn handle_append_envelope(
    registry: RaftGroupHandleRegistry,
    envelope: raft_internal_proto::RaftRpcEnvelopeV1,
    via: AppendVia,
) -> Result<raft_internal_proto::RaftRpcAckV1, tonic::Status> {
    let raft_group_id = validate_raft_rpc_preamble_with(
        &registry,
        envelope.protocol_version,
        &envelope,
        |target| match via {
            AppendVia::Unary => Ok(()),
            AppendVia::Stream(Some(plane)) if plane == target.plane() => Ok(()),
            AppendVia::Stream(plane) => Err(GrpcRpcError::invalid_argument(format!(
                "an append stream of plane {plane:?} carries an item of plane {:?}",
                target.plane()
            ))),
        },
    )?;
    let request: UrsulaAppendEntriesRequest =
        decode_rpc_payload(&envelope.payload, "raft append request")?;
    // A data group's log takes no command of the meta plane: refused before
    // OpenRaft sees it.
    let target = RaftTarget::Data(raft_group_id);
    if request.entries.iter().any(|entry| match &entry.payload {
        openraft::EntryPayload::Normal(command) => !target.admits(command),
        openraft::EntryPayload::Blank | openraft::EntryPayload::Membership(_) => false,
    }) {
        return Err(GrpcRpcError::invalid_argument(format!(
            "an append to raft group {} carries a command of another plane",
            raft_group_id.0
        ))
        .into());
    }
    let response = registry
        .append_entries(raft_group_id, request)
        .await
        .map_err(group_rpc_status)?;
    Ok(raft_internal_proto::RaftRpcAckV1 {
        payload: encode_wire(&response),
    })
}

async fn handle_append_stream_item(
    registry: RaftGroupHandleRegistry,
    concurrency: Arc<Semaphore>,
    plane: Option<raft_internal_proto::RaftPlaneV1>,
    item: raft_internal_proto::RaftAppendStreamRequestItem,
) -> raft_internal_proto::RaftAppendStreamResponseItem {
    let request_id = item.request_id;
    let Ok(_permit) = concurrency.acquire_owned().await else {
        return raft_internal_proto::RaftAppendStreamResponseItem {
            request_id,
            result: Some(
                raft_internal_proto::raft_append_stream_response_item::Result::Error(
                    stream_item_error(&tonic::Status::unavailable(
                        "append stream concurrency guard is closed",
                    )),
                ),
            ),
        };
    };
    let result = match item.envelope {
        Some(envelope) => {
            match handle_append_envelope(registry, envelope, AppendVia::Stream(plane)).await {
                Ok(ack) => {
                    Some(raft_internal_proto::raft_append_stream_response_item::Result::Ack(ack))
                }
                Err(status) => Some(
                    raft_internal_proto::raft_append_stream_response_item::Result::Error(
                        stream_item_error(&status),
                    ),
                ),
            }
        }
        None => Some(
            raft_internal_proto::raft_append_stream_response_item::Result::Error(
                stream_item_error(&tonic::Status::invalid_argument(
                    "append stream batch item is missing its envelope",
                )),
            ),
        ),
    };
    raft_internal_proto::RaftAppendStreamResponseItem { request_id, result }
}

#[tonic::async_trait]
impl raft_internal_proto::raft_internal_server::RaftInternal for RaftGrpcService {
    type AppendStreamStream = Pin<
        Box<
            dyn Stream<Item = Result<raft_internal_proto::RaftAppendStreamResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn append(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        let ack = handle_append_envelope(
            self.registry.clone(),
            request.into_inner(),
            AppendVia::Unary,
        )
        .await?;
        Ok(tonic::Response::new(ack))
    }

    async fn append_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft_internal_proto::RaftAppendStreamRequest>>,
    ) -> Result<tonic::Response<Self::AppendStreamStream>, tonic::Status> {
        let registry = self.registry.clone();
        let plane = crate::target::stream_plane(request.metadata());
        let concurrency = Arc::new(Semaphore::new(64));
        // Decoded frames waiting for (or in) processing hold a share of this budget; the next
        // frame is not read off the stream until it fits, so a slow follower pushes back through
        // HTTP/2 flow control instead of buffering the leader's backlog in memory.
        let budget = registry.append_receive_budget.clone();
        let shutdown = registry.subscribe_transport_shutdown();
        let requests = futures_util::stream::unfold(
            (request.into_inner(), shutdown, budget),
            |(mut requests, mut shutdown, budget)| async move {
                if *shutdown.borrow() {
                    return None;
                }
                let request = tokio::select! {
                    biased;
                    _ = shutdown.changed() => return None,
                    request = requests.next() => request?,
                };
                let buffered = match &request {
                    Ok(frame) => {
                        let charge = append_budget_charge(frame.encoded_len(), budget.bytes);
                        let permit = tokio::select! {
                            biased;
                            _ = shutdown.changed() => return None,
                            permit = budget.semaphore.clone().acquire_many_owned(charge) => permit.ok()?,
                        };
                        Some(QueuedAppendBytes::new(
                            permit,
                            u64::from(charge),
                            &GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES,
                            &GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES_MAX,
                        ))
                    }
                    Err(_) => None,
                };
                Some(((request, buffered), (requests, shutdown, budget)))
            },
        );
        let responses = requests
            .map(move |(request, buffered)| {
                let registry = registry.clone();
                let concurrency = concurrency.clone();
                async move {
                    let _buffered = buffered;
                    let request = request?;
                    let items = futures_util::stream::iter(request.items)
                        .map(|item| {
                            handle_append_stream_item(
                                registry.clone(),
                                concurrency.clone(),
                                plane,
                                item,
                            )
                        })
                        .buffer_unordered(64)
                        .collect()
                        .await;
                    Ok(raft_internal_proto::RaftAppendStreamResponse { items })
                }
            })
            .buffer_unordered(64);
        Ok(tonic::Response::new(Box::pin(responses)))
    }

    async fn vote(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        let raft_group_id =
            validate_raft_rpc_preamble(&self.registry, envelope.protocol_version, &envelope)?;
        let request: UrsulaVoteRequest =
            decode_rpc_payload(&envelope.payload, "raft vote request")?;
        let response = self
            .registry
            .vote(raft_group_id, request)
            .await
            .map_err(group_rpc_status)?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: encode_wire(&response),
        }))
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<raft_internal_proto::RaftFullSnapshotRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftFullSnapshotAckV1>, tonic::Status> {
        let request = request.into_inner();
        let raft_group_id =
            validate_raft_rpc_preamble(&self.registry, request.protocol_version, &request)?;
        let vote: VoteOf<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.vote, "full snapshot vote")?;
        let meta: SnapshotMetaOf<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.snapshot_meta, "full snapshot meta")?;
        let snapshot = UrsulaSnapshot {
            meta,
            snapshot: SnapshotData::new(request.snapshot_payload.to_vec()),
        };
        let response = self
            .registry
            .install_full_snapshot(raft_group_id, vote, snapshot)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(
            raft_internal_proto::RaftFullSnapshotAckV1 {
                response: encode_wire(&response),
            },
        ))
    }

    async fn group_write(
        &self,
        request: tonic::Request<raft_internal_proto::GroupWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupWriteResponseV1>, tonic::Status> {
        // Link this forwarded write to the originating request's trace.
        let span = tracing::info_span!("raft.group_write");
        span.set_parent(crate::telemetry::extract_parent_context(request.metadata()));
        async move {
            let request = request.into_inner();
            validate_grpc_metadata(request.protocol_version)?;
            let group = data_group(
                RaftTarget::parse(&request, local_node(&self.registry)?)?,
                "GroupWrite",
            )?;
            let placement = placement_from_parts(
                request.core_id,
                request.shard_id,
                group.0,
                "group_write_request",
            )?;
            let entry = self
                .registry
                .entry(placement.raft_group_id)
                .map_err(group_rpc_status)?;
            let raft = entry.raft().clone();
            let history = entry.history.clone();
            let commands = request
                .command_payloads
                .iter()
                .map(|payload| decode_rpc_payload(payload, "group command"))
                .collect::<Result<Vec<_>, _>>()?;
            let count = commands.len();
            // A refusal of the whole batch answers each command with it, so
            // the sender keeps the leader's typed answer (a pending history,
            // an unknown outcome).
            let results =
                raft.call(move |raft| async move {
                    write_commands_on_raft(raft, &history, commands).await
                })
                .await
                .map_err(|fatal| group_rpc_status(crate::owner::owner_stopped(fatal)))?
                .unwrap_or_else(|refusal| vec![Err(refusal); count]);
            let results = results
                .into_iter()
                .map(|result| match result {
                    Ok(response) => raft_internal_proto::GroupWriteResultV1 {
                        ok: true,
                        payload: encode_wire(&response),
                    },
                    Err(err) => raft_internal_proto::GroupWriteResultV1 {
                        ok: false,
                        payload: encode_wire(&err),
                    },
                })
                .collect();
            Ok(tonic::Response::new(
                raft_internal_proto::GroupWriteResponseV1 { results },
            ))
        }
        .instrument(span)
        .await
    }

    async fn rejoin_barrier(
        &self,
        request: tonic::Request<raft_internal_proto::RejoinBarrierRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RejoinBarrierResponseV1>, tonic::Status> {
        let request = request.into_inner();
        let group = validate_raft_rpc_preamble(&self.registry, request.protocol_version, &request)?;
        let answer = self
            .registry
            .confirm_recovery_barrier(group)
            .await
            .map_err(tonic::Status::from)?;
        let target = RaftTarget::Data(group);
        Ok(tonic::Response::new(
            raft_internal_proto::RejoinBarrierResponseV1 {
                vote: encode_wire(&answer.vote),
                index: answer.index,
                plane: target.plane().into(),
                raft_group_id: target.wire_group_id(),
                memberships: encode_wire(&answer.memberships),
            },
        ))
    }

    async fn transfer_leader(
        &self,
        request: tonic::Request<raft_internal_proto::RaftTransferLeaderRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftTransferLeaderAckV1>, tonic::Status> {
        let request = request.into_inner();
        let raft_group_id =
            validate_raft_rpc_preamble(&self.registry, request.protocol_version, &request)?;
        let openraft_request: TransferLeaderRequest<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.request, "transfer leader request")?;
        let response = self
            .registry
            .handle_transfer_leader(raft_group_id, openraft_request)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(
            raft_internal_proto::RaftTransferLeaderAckV1 {
                response: encode_wire(&response),
            },
        ))
    }

    /// The meta group's genesis probe, checked as 0.10 checks it: the
    /// protocol version first, then the target node, then the probe, which
    /// must decode. The answer comes from the meta replica's recovery gate
    /// and this node's genesis claim, which the meta runtime brings (#768).
    /// No node runs a meta replica yet, so every probe is refused as one for
    /// a group this node does not run (`NOT_FOUND`), never `UNIMPLEMENTED`,
    /// which a prober records as another format epoch.
    async fn genesis_probe(
        &self,
        request: tonic::Request<raft_internal_proto::GenesisProbeRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GenesisProbeAckV1>, tonic::Status> {
        let request = request.into_inner();
        validate_grpc_metadata(request.protocol_version)?;
        crate::target::check_target_node(request.target_node, local_node(&self.registry)?)?;
        let _probe: crate::meta::genesis::GenesisProbe =
            decode_rpc_payload(&request.probe, "genesis probe")?;
        Err(no_meta_replica("GenesisProbe").into())
    }

    async fn group_read(
        &self,
        request: tonic::Request<raft_internal_proto::GroupReadRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupReadResponseV1>, tonic::Status> {
        // Link this forwarded read to the originating request's trace.
        let span = tracing::info_span!("raft.group_read");
        span.set_parent(crate::telemetry::extract_parent_context(request.metadata()));
        async move {
            let request = request.into_inner();
            validate_grpc_metadata(request.protocol_version)?;
            // The meta group serves no stream reads.
            let RaftTarget::Data(group) = RaftTarget::parse(&request, local_node(&self.registry)?)?
            else {
                return Err(GrpcRpcError::invalid_argument(
                    "GroupRead names the meta plane, which serves no stream reads",
                )
                .into());
            };
            let placement = placement_from_parts(
                request.core_id,
                request.shard_id,
                group.0,
                "group_read_request",
            )?;
            let entry = self
                .registry
                .entry(placement.raft_group_id)
                .map_err(group_rpc_status)?;
            let raft = entry.raft().clone();
            let read_barrier = entry.barrier.clone();
            let cold_store = self.cold_store.clone();
            let cold_index_cache = entry.cache.clone().or_else(|| {
                cold_store.as_ref().map(|cold_store| {
                    Arc::new(ColdIndexPageCache::new(
                        Arc::new(ColdStoreColdIndexPageStore::new(cold_store.clone())),
                        1024,
                    ))
                })
            });
            let apply_health = entry.apply_health.clone();
            let history = entry.history.clone();
            let peer_cluster = Some(self.registry.peer_cluster().clone());
            let log_gauge = entry.log_gauge.clone();
            let snapshots = entry.snapshots.clone();
            let result = raft
                .call(move |raft| async move {
                    let engine = crate::engine::ForwardedReadEngine {
                        snapshots,
                        history,
                        apply_health,
                        raft,
                        placement,
                        read_barrier,
                        cold_store,
                        // The group's shared page cache (bounded-state F13), which
                        // apply-time invalidation reaches; a request-scoped cache only
                        // when none was registered.
                        cold_index_cache,
                        peer_cluster,
                        log_gauge,
                    };
                    let stream_id = BucketStreamId::new(request.bucket_id, request.stream_id);
                    let result = match required(request.read, "group_read.read")? {
                        raft_internal_proto::group_read_request_v1::Read::Head(head) => engine
                            .head_stream(
                                head_stream_request_from_v1(stream_id, request.now_ms, head),
                                placement,
                            )
                            .await
                            .map(|response| raft_internal_proto::GroupReadResponseV1 {
                                ok: true,
                                payload: encode_wire(&response),
                            }),
                        raft_internal_proto::group_read_request_v1::Read::ReadStream(read) => {
                            let read = read_stream_request_from_v1(stream_id, request.now_ms, read)
                                .map_err(tonic::Status::invalid_argument)?;
                            engine.read_stream(read, placement).await.map(|response| {
                                raft_internal_proto::GroupReadResponseV1 {
                                    ok: true,
                                    payload: encode_wire(&response),
                                }
                            })
                        }
                    };
                    Ok::<_, tonic::Status>(result)
                })
                .await
                .map_err(|fatal| group_rpc_status(crate::owner::owner_stopped(fatal)))??;
            let response = match result {
                Ok(response) => response,
                Err(err) => raft_internal_proto::GroupReadResponseV1 {
                    ok: false,
                    payload: encode_wire(&err),
                },
            };
            Ok(tonic::Response::new(response))
        }
        .instrument(span)
        .await
    }
}

/// Server half of a forwarded HEAD: a client HEAD stays linearizable on the
/// leader, an internal one reads the leader's applied state (D10).
pub(crate) fn head_stream_request_from_v1(
    stream_id: BucketStreamId,
    now_ms: u64,
    head: raft_internal_proto::HeadStreamReadV1,
) -> HeadStreamRequest {
    HeadStreamRequest {
        stream_id,
        now_ms,
        linearizable: !head.applied_state_only,
        read_index: None,
    }
}

/// Server half of a forwarded read: the engine request with the follower's
/// `leader_only` flag, so a forwarded `consistency=leader` read is
/// linearized on the leader, and its follower policy, so a receiver that no
/// longer leads still serves an immutable-read chunk only when it can mark
/// it.
/// `Err` names the invalid field.
pub(crate) fn read_stream_request_from_v1(
    stream_id: BucketStreamId,
    now_ms: u64,
    read: raft_internal_proto::ReadStreamReadV1,
) -> Result<ReadStreamRequest, &'static str> {
    let max_len = usize::try_from(read.max_len)
        .map_err(|_overflow| "group_read.read_stream.max_len too large")?;
    let follower = match raft_internal_proto::FollowerReadV1::try_from(read.follower) {
        Ok(raft_internal_proto::FollowerReadV1::Prefix) => FollowerRead::Prefix,
        Ok(raft_internal_proto::FollowerReadV1::Immutable) => FollowerRead::Immutable,
        Err(_unknown) => return Err("group_read.read_stream.follower unknown"),
    };
    Ok(ReadStreamRequest {
        stream_id,
        offset: read.offset,
        max_len,
        now_ms,
        leader_only: read.leader_only,
        read_index: None,
        follower,
    })
}

/// Decode a MessagePack RPC payload carried inside a proto envelope, mapping
/// failures to `InvalidArgument`.
fn decode_rpc_payload<T: DeserializeOwned>(
    bytes: &[u8],
    what: &'static str,
) -> Result<T, GrpcRpcError> {
    decode_wire(bytes, what)
        .map_err(|err| GrpcRpcError::invalid_argument(ErrorChain(&err).to_string()))
}

/// Validates the preamble of an inbound Raft RPC in its one order: the
/// protocol version, then the target (plane, group and target node,
/// [`RaftTarget::parse`]), then the group lookup, which answers `NOT_FOUND`
/// for a group this node does not run.
pub(crate) fn validate_raft_rpc_preamble<R: TargetedRequest>(
    registry: &RaftGroupHandleRegistry,
    protocol_version: u32,
    request: &R,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_raft_rpc_preamble_with(registry, protocol_version, request, |_| Ok(()))
}

/// [`validate_raft_rpc_preamble`] with `check` on the parsed target before
/// the group lookup.
fn validate_raft_rpc_preamble_with<R: TargetedRequest>(
    registry: &RaftGroupHandleRegistry,
    protocol_version: u32,
    request: &R,
    check: impl FnOnce(RaftTarget) -> Result<(), GrpcRpcError>,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_grpc_metadata(protocol_version)?;
    let target = RaftTarget::parse(request, local_node(registry)?)?;
    check(target)?;
    let raft_group_id = data_group(target, R::NAME)?;
    if !registry.contains_group(raft_group_id) {
        return Err(GrpcRpcError::not_found(format!(
            "raft group {} is not registered on this node",
            raft_group_id.0
        )));
    }
    Ok(raft_group_id)
}

/// The data group `target` names. No node runs a meta group replica yet, so
/// a request for the meta group is answered as one for a group this node
/// does not run.
fn data_group(target: RaftTarget, request: &str) -> Result<RaftGroupId, GrpcRpcError> {
    match target {
        RaftTarget::Data(group) => Ok(group),
        RaftTarget::Meta => Err(no_meta_replica(request)),
    }
}

/// The refusal of a meta-plane `request`: this node runs no meta group
/// replica, as a group lookup that finds none answers.
fn no_meta_replica(request: &str) -> GrpcRpcError {
    GrpcRpcError::not_found(format!(
        "{request} names the meta group, and this node runs no meta group replica"
    ))
}

/// This node's id, which every request it serves must be for. A node
/// without one has no cluster id either and serves no peers.
fn local_node(registry: &RaftGroupHandleRegistry) -> Result<u64, GrpcRpcError> {
    registry
        .peer_cluster()
        .identity()
        .map(|identity| identity.node_id())
        .ok_or_else(|| {
            GrpcRpcError::unavailable("this node has no Raft identity and serves no peers")
        })
}

pub(crate) fn validate_grpc_metadata(protocol_version: u32) -> Result<(), GrpcRpcError> {
    if protocol_version != RAFT_GRPC_PROTOCOL_VERSION {
        // E8 at runtime. A sender before format epoch 2 omits the field on
        // GroupWrite/GroupRead, so it reads as 0 here.
        let message = format!(
            "{PROTOCOL_MISMATCH_TEXT}: local={RAFT_GRPC_PROTOCOL_VERSION}, \
             remote={protocol_version} (format epochs differ; a node joins only a cluster of its \
             own format epoch)"
        );
        record_format_epoch_mismatch("inbound", &message);
        return Err(GrpcRpcError::failed_precondition(message));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct GrpcRaftNetworkFactory {
    transport: Arc<CoreRaftTransport>,
    raft_group_id: RaftGroupId,
    reconnect_threshold: u32,
}

impl GrpcRaftNetworkFactory {
    pub fn new(transport: Arc<CoreRaftTransport>, raft_group_id: RaftGroupId) -> Self {
        Self {
            transport,
            raft_group_id,
            reconnect_threshold: 8,
        }
    }

    pub fn with_reconnect_threshold(mut self, threshold: u32) -> Self {
        self.reconnect_threshold = threshold;
        self
    }
}

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    /// A client to `target` at the cluster URL `raft.peers` lists for it.
    /// Membership names node ids only.
    async fn new_client(&mut self, target: u64, _node: &EmptyNode) -> Self::Network {
        GrpcRaftNetwork::resolve(
            self.transport.clone(),
            self.raft_group_id,
            target,
            self.reconnect_threshold,
        )
    }
}

/// Why a Raft client to a peer has no channel. OpenRaft sees each as an
/// unreachable peer and backs off.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum RaftChannelError {
    /// `raft.peers` lists no such node, so nothing dials it.
    #[error("raft.peers lists no node {node_id}: not dialing it")]
    Unaddressed { node_id: u64 },
    #[error("invalid raft gRPC endpoint {endpoint}")]
    InvalidEndpoint {
        endpoint: String,
        #[source]
        source: Arc<tonic::transport::Error>,
    },
    #[error("the raft gRPC channel pool for {endpoint} is poisoned")]
    PoolPoisoned { endpoint: String },
}

/// The owner-core channel a [`GrpcRaftNetwork`] sends on.
#[derive(Clone)]
struct PeerChannel {
    /// The cluster URL dialed.
    endpoint: String,
    client: RaftClient,
    /// The pooled channel's generation ([`shared_raft_client`]).
    generation: u64,
}

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    transport: Arc<CoreRaftTransport>,
    raft_group_id: RaftGroupId,
    target: u64,
    channel: Result<PeerChannel, RaftChannelError>,
    /// Streak of consecutive RPC failures on this channel. Reset to 0 on the
    /// next successful RPC. When it crosses `reconnect_threshold` we replace
    /// the owner-core HTTP/2 channel generation — tonic's `connect_lazy`
    /// keeps a stuck channel forever otherwise (the TCP socket stays open, the
    /// HTTP/2 streams stay borked, no auto-heal).
    consecutive_failures: u32,
    reconnect_threshold: u32,
}

impl Debug for GrpcRaftNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("GrpcRaftNetwork");
        debug
            .field("raft_group_id", &self.raft_group_id)
            .field("target", &self.target);
        match &self.channel {
            Ok(channel) => debug
                .field("endpoint", &channel.endpoint)
                .field("channel_generation", &channel.generation),
            Err(error) => debug.field("channel", error),
        };
        debug
            .field("consecutive_failures", &self.consecutive_failures)
            .field("reconnect_threshold", &self.reconnect_threshold)
            .finish()
    }
}

impl GrpcRaftNetwork {
    /// A client to `target` at the cluster URL `raft.peers` lists for it in
    /// `transport`'s peer cluster. A node it does not list gets a client
    /// that answers every RPC unreachable without dialing.
    pub fn new(transport: Arc<CoreRaftTransport>, raft_group_id: RaftGroupId, target: u64) -> Self {
        Self::resolve(transport, raft_group_id, target, 8)
    }

    fn resolve(
        transport: Arc<CoreRaftTransport>,
        raft_group_id: RaftGroupId,
        target: u64,
        reconnect_threshold: u32,
    ) -> Self {
        let channel = match transport.peer_cluster.cluster_url(target) {
            Some(address) => open_peer_channel(&transport, address),
            None => Err(RaftChannelError::Unaddressed { node_id: target }),
        };
        Self {
            transport,
            raft_group_id,
            target,
            channel,
            consecutive_failures: 0,
            reconnect_threshold,
        }
    }

    /// A client to `target` at `address`, whatever `raft.peers` lists.
    #[cfg(test)]
    fn with_transport(
        transport: Arc<CoreRaftTransport>,
        raft_group_id: RaftGroupId,
        target: u64,
        address: impl Into<String>,
        reconnect_threshold: u32,
    ) -> Self {
        let channel = open_peer_channel(&transport, address.into());
        Self {
            transport,
            raft_group_id,
            target,
            channel,
            consecutive_failures: 0,
            reconnect_threshold,
        }
    }

    pub(crate) fn client(&self) -> Result<RaftClient, RPCError<UrsulaRaftTypeConfig>> {
        self.channel
            .as_ref()
            .map(|channel| channel.client.clone())
            .map_err(|error| RPCError::Unreachable(Unreachable::new(error)))
    }

    /// The URL this client dials, or why it dials none, for log text.
    fn endpoint(&self) -> String {
        match &self.channel {
            Ok(channel) => channel.endpoint.clone(),
            Err(error) => error.to_string(),
        }
    }

    /// Increment the failure streak. If we cross the threshold, drop the
    /// stuck shared channel and build a fresh one — the next RPC call gets a
    /// new HTTP/2 connection. If another group already rebuilt this endpoint,
    /// adopt that newer generation instead of creating another connection.
    /// We also reset the counter so the replacement channel gets a full grace
    /// period before any further rebuild.
    fn note_failure(&mut self, route: &str) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures < self.reconnect_threshold {
            return;
        }
        self.consecutive_failures = 0;
        // A client without a channel sends nothing: there is no channel to
        // rebuild.
        let Ok(channel) = &self.channel else {
            return;
        };
        tracing::warn!(
            "raft-grpc: rebuilding channel to node {} ({}) after {} consecutive {} failures",
            self.target,
            channel.endpoint,
            self.reconnect_threshold,
            route,
        );
        let endpoint = channel.endpoint.clone();
        let generation = channel.generation;
        self.channel = shared_raft_client(&self.transport, &endpoint, Some(generation)).map(
            |(client, generation)| PeerChannel {
                endpoint,
                client,
                generation,
            },
        );
    }

    fn note_success(&mut self) {
        self.consecutive_failures = 0;
    }

    /// The Raft target this client's requests name.
    fn raft_target(&self) -> RaftTarget {
        RaftTarget::Data(self.raft_group_id)
    }

    /// `request` with this client's target and target node stamped on it.
    fn stamped<R: TargetedRequest>(&self, mut request: R) -> R {
        self.raft_target().stamp(self.target, &mut request);
        request
    }

    pub(crate) fn append_envelope(
        &self,
        request: &UrsulaAppendEntriesRequest,
    ) -> raft_internal_proto::RaftRpcEnvelopeV1 {
        self.stamped(raft_internal_proto::RaftRpcEnvelopeV1 {
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: encode_wire(request),
            ..Default::default()
        })
    }

    pub(crate) fn transfer_leader_envelope(
        &self,
        request: &TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> raft_internal_proto::RaftTransferLeaderRequestV1 {
        self.stamped(raft_internal_proto::RaftTransferLeaderRequestV1 {
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            request: encode_wire(request),
            ..Default::default()
        })
    }

    pub(crate) fn vote_envelope(
        &self,
        request: UrsulaVoteRequest,
    ) -> raft_internal_proto::RaftRpcEnvelopeV1 {
        self.stamped(raft_internal_proto::RaftRpcEnvelopeV1 {
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: encode_wire(&request),
            ..Default::default()
        })
    }

    pub(crate) fn apply_rpc_timeout<T>(&self, request: &mut tonic::Request<T>, option: RPCOption) {
        request.set_timeout(option.hard_ttl());
        // Note: trace context is intentionally NOT injected here. These are
        // OpenRaft consensus RPCs (append_entries/vote/snapshot) driven by the
        // replication loop, decoupled from any client request, so there is no
        // request span to propagate. Request-synchronous leader forwarding
        // injects its own context (see `crate::forward`).
    }

    /// The OpenRaft error for a failed call, and whether the peer refused it
    /// for good: for another cluster id, or as meant for another node (its
    /// address serves another node now).
    pub(crate) fn map_tonic_status(
        &self,
        route: &str,
        status: tonic::Status,
    ) -> (RPCError<UrsulaRaftTypeConfig>, bool) {
        observe_outbound_status(route, &status);
        let other_node = crate::target::refusing_node(&status).is_some();
        // A peer of another cluster refuses every RPC until an operator fixes
        // a configuration: to OpenRaft it is unreachable, so replication backs
        // off instead of retrying at once.
        let other_cluster = self.transport.peer_cluster.observe_refusal(
            self.target,
            &self.endpoint(),
            &status,
            crate::rt::time::Instant::now(),
        );
        let message = format!(
            "{route} to node {} at {} failed: {}",
            self.target,
            self.endpoint(),
            status
        );
        let error = match status.code() {
            tonic::Code::Unavailable | tonic::Code::Cancelled => {
                RPCError::Unreachable(Unreachable::from_string(message))
            }
            _ if other_cluster || other_node => {
                RPCError::Unreachable(Unreachable::from_string(message))
            }
            _ => raft_rpc_network_error(message),
        };
        (error, other_cluster || other_node)
    }

    /// Shared client-call path for every outbound raft RPC: build the tonic
    /// request, apply the RPC timeout, send via `send`, and track the
    /// success/failure streak for channel rebuilds.
    async fn call<Req, Resp, Fut>(
        &mut self,
        route: &'static str,
        request: Req,
        option: RPCOption,
        send: impl FnOnce(RaftClient, tonic::Request<Req>) -> Fut,
    ) -> Result<Resp, RPCError<UrsulaRaftTypeConfig>>
    where
        Req: Message,
        Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    {
        let use_zstd = request.encoded_len() >= RAFT_GRPC_ZSTD_MIN_MESSAGE_BYTES;
        let mut request = tonic::Request::new(request);
        self.apply_rpc_timeout(&mut request, option);
        let mut client = self.client()?;
        if use_zstd {
            client = client.send_compressed(CompressionEncoding::Zstd);
        }
        match send(client, request).await {
            Ok(response) => {
                self.note_success();
                Ok(response.into_inner())
            }
            Err(err) => {
                let (mapped, refused) = self.map_tonic_status(route, err);
                // A refusal for another cluster id or another node is a
                // final answer over a working channel: rebuilding the
                // channel would not change it.
                if !refused {
                    self.note_failure(route);
                }
                Err(mapped)
            }
        }
    }

    async fn try_append_stream(
        &self,
        envelope: raft_internal_proto::RaftRpcEnvelopeV1,
        option: RPCOption,
    ) -> Result<raft_internal_proto::RaftRpcAckV1, AppendCallError> {
        let channel = self
            .channel
            .as_ref()
            .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
        let session = shared_append_session(
            &self.transport,
            &channel.endpoint,
            self.target,
            self.raft_target(),
            channel.client.clone(),
        )
        .map_err(tonic::Status::unavailable)?;
        let charge = append_budget_charge(envelope.encoded_len(), self.transport.budget.bytes);
        // Fair admission: a call that does not fit waits in the semaphore's FIFO queue, which
        // hands freed budget to the oldest waiter first (and lets no later call overtake it), so
        // a multi-MiB catch-up append is not starved by a stream of small ones. The wait shares
        // the call's OpenRaft deadline; still not admitted by then is the true overflow case.
        let deadline = tokio::time::Instant::now()
            .checked_add(option.hard_ttl())
            .ok_or_else(|| {
                tonic::Status::invalid_argument("raft rpc hard TTL overflows the monotonic clock")
            })?;
        let admitted = match session.budget.clone().try_acquire_many_owned(charge) {
            Ok(permit) => Some(permit),
            Err(_) => {
                tokio::time::timeout_at(deadline, session.budget.clone().acquire_many_owned(charge))
                    .await
                    .ok()
                    .and_then(Result::ok)
            }
        };
        let Some(permit) = admitted else {
            GRPC_APPEND_STREAM_BACKPRESSURE_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            return Err(AppendCallError::BacklogFull {
                queued: self
                    .transport
                    .budget
                    .bytes
                    .saturating_sub(session.budget.available_permits()),
                budget: self.transport.budget.bytes,
            });
        };
        let queued = QueuedAppendBytes::new(
            permit,
            u64::from(charge),
            &GRPC_APPEND_STREAM_QUEUED_BYTES,
            &GRPC_APPEND_STREAM_QUEUED_BYTES_MAX,
        );
        let (response_sender, response_receiver) = oneshot::channel();
        session
            .sender
            .send(AppendStreamCall {
                envelope,
                response: response_sender,
                queued,
            })
            .map_err(|_closed| tonic::Status::unavailable("raft append stream is closed"))?;
        match tokio::time::timeout_at(deadline, response_receiver).await {
            Ok(Ok(result)) => Ok(result?),
            Ok(Err(_)) => Err(tonic::Status::unavailable(
                "raft append stream closed without a response",
            )
            .into()),
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "raft append stream exceeded the OpenRaft hard TTL",
            )
            .into()),
        }
    }

    async fn append_rpc(
        &mut self,
        envelope: raft_internal_proto::RaftRpcEnvelopeV1,
        option: RPCOption,
    ) -> Result<raft_internal_proto::RaftRpcAckV1, RPCError<UrsulaRaftTypeConfig>> {
        // A client without a channel answers unreachable before it queues
        // anything, and it has no channel to rebuild.
        if let Err(error) = &self.channel {
            return Err(RPCError::Unreachable(Unreachable::new(error)));
        }
        match self.try_append_stream(envelope, option).await {
            Ok(ack) => {
                self.note_success();
                Ok(ack)
            }
            Err(error) => {
                // A full queue is congestion on a live stream, not a broken channel: rebuilding
                // the channel would not drain it.
                let backpressure = matches!(error, AppendCallError::BacklogFull { .. });
                let (mapped, _) = self.map_tonic_status("AppendStream", error.into());
                // The append stream's calls only see a refused stream close; the
                // session recorded the refusal. Like a full queue, it is not a
                // broken channel.
                // A peer refused within the last minute keeps a failure from counting
                // toward a rebuild, a real transport failure just after an operator
                // fixed it included: tonic reconnects a broken connection on its own.
                let other_cluster = self
                    .transport
                    .peer_cluster
                    .refuses(self.target, crate::rt::time::Instant::now());
                if !backpressure && !other_cluster {
                    self.note_failure("AppendStream");
                }
                Err(mapped)
            }
        }
    }

    /// Decode the MessagePack payload of an envelope-style ack.
    fn decode_rpc_ack<T: DeserializeOwned>(
        &self,
        route: &'static str,
        payload: &[u8],
    ) -> Result<T, RPCError<UrsulaRaftTypeConfig>> {
        decode_wire(payload, route).map_err(|err| {
            raft_rpc_network_error(format!(
                "{route} response from node {} at {}: {}",
                self.target,
                self.endpoint(),
                ErrorChain(&err)
            ))
        })
    }
}

fn raft_client(channel: Channel, identity: &PeerIdentity) -> RaftClient {
    raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
        channel,
        identity.clone(),
    )
    .accept_compressed(CompressionEncoding::Zstd)
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

/// Return the owner-core HTTP/2 channel for a Raft peer.
///
/// OpenRaft constructs one network client per group and peer. Without this
/// pool, every group creates its own TCP connection even though tonic channels
/// can multiplex all of those RPCs over one HTTP/2 connection.
///
/// `observed_generation` is `None` for a new group, which always adopts the
/// current shared channel. A reconnect passes the generation it was using: if
/// another group has already replaced that generation, it adopts the newer
/// channel; otherwise it performs exactly one replacement within this core.
fn shared_raft_client(
    transport: &CoreRaftTransport,
    endpoint: &str,
    observed_generation: Option<u64>,
) -> Result<(RaftClient, u64), RaftChannelError> {
    let parsed = peer_endpoint(endpoint).map_err(|source| RaftChannelError::InvalidEndpoint {
        endpoint: endpoint.to_owned(),
        source: Arc::new(source),
    })?;
    let mut channels =
        transport
            .channels
            .lock()
            .map_err(|_poisoned| RaftChannelError::PoolPoisoned {
                endpoint: endpoint.to_owned(),
            })?;
    if let Some(shared) = channels.get(endpoint)
        && observed_generation.is_none_or(|generation| generation != shared.generation)
    {
        return Ok((
            raft_client(shared.channel.clone(), &transport.identity),
            shared.generation,
        ));
    }
    let generation = channels
        .get(endpoint)
        .map_or(1, |shared| shared.generation.saturating_add(1));
    let channel = parsed.connect_lazy();
    channels.insert(endpoint.to_owned(), SharedRaftChannel {
        generation,
        channel: channel.clone(),
    });
    Ok((raft_client(channel, &transport.identity), generation))
}

/// The pooled channel to the peer at `address`.
fn open_peer_channel(
    transport: &CoreRaftTransport,
    address: String,
) -> Result<PeerChannel, RaftChannelError> {
    let endpoint = normalize_grpc_endpoint(address);
    let (client, generation) = shared_raft_client(transport, &endpoint, None)?;
    Ok(PeerChannel {
        endpoint,
        client,
        generation,
    })
}

/// Return the one healthy Append stream for this peer endpoint.
///
/// The stream lifetime is intentionally independent of the unary channel
/// generation. OpenRaft keeps one network object per group, so after any
/// channel rebuild those objects temporarily observe different generations.
/// Keying the stream by each object's generation makes them replace one
/// another on nearly every Append call. A live stream is already proof that
/// its underlying channel works; replace it only after its sender closes.
/// A transport's sessions serve the plane of the groups it carries: the
/// stream names it in its `ursula-raft-plane` metadata, and the receiver
/// refuses an item of another plane. Sessions are keyed by endpoint alone,
/// so one plane per stream holds only because the meta group gets a
/// transport of its own: a transport that carried both planes would send
/// meta items on the data stream, which the receiver refuses.
fn shared_append_session(
    transport: &CoreRaftTransport,
    endpoint: &str,
    target: u64,
    raft_target: RaftTarget,
    client: RaftClient,
) -> Result<SharedAppendSession, String> {
    let sessions = &transport.sessions;
    let mut sessions = sessions
        .lock()
        .map_err(|err| format!("raft append session pool lock poisoned for {endpoint}: {err}"))?;
    if let Some(session) = sessions.get(endpoint)
        && !session.sender.is_closed()
    {
        return Ok(session.clone());
    }

    let (sender, receiver) = mpsc::unbounded_channel();
    let refusals = PeerRefusals {
        peer_cluster: transport.peer_cluster.clone(),
        target,
        address: endpoint.to_owned(),
    };
    let task = tokio::spawn(run_append_session(
        client,
        raft_target.plane_metadata(),
        receiver,
        refusals,
    ));
    let session = SharedAppendSession {
        _task: Arc::new(AppendSessionTask::Running(task)),
        sender,
        budget: transport.budget.semaphore.clone(),
    };
    sessions.insert(endpoint.to_owned(), session.clone());
    Ok(session)
}

/// Collect `first` and the calls already queued behind it (up to the frame's call and byte
/// limits) into one frame, dropping calls whose caller has already given up: their response would
/// go nowhere, and sending them anyway is what turned a lagging peer into an ever-growing backlog
/// (each OpenRaft retry re-queues the same entries).
fn collect_append_stream_frame(
    first: AppendStreamCall,
    calls: &mut mpsc::UnboundedReceiver<AppendStreamCall>,
) -> CollectedFrame {
    fn keep(call: AppendStreamCall, frame: &mut CollectedFrame) {
        if call.response.is_closed() {
            GRPC_APPEND_STREAM_EXPIRED_UNSENT.fetch_add(1, Ordering::Relaxed);
        } else {
            frame.bytes = frame.bytes.saturating_add(call.envelope.encoded_len());
            frame.calls.push(call);
        }
    }
    let mut frame = CollectedFrame {
        calls: Vec::new(),
        bytes: 0,
        receiver_open: true,
    };
    keep(first, &mut frame);
    while frame.calls.len() < RAFT_GRPC_APPEND_STREAM_MAX_BATCH_ITEMS
        && frame.bytes < RAFT_GRPC_APPEND_STREAM_MAX_FRAME_BYTES
    {
        match calls.try_recv() {
            Ok(call) => keep(call, &mut frame),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                frame.receiver_open = false;
                break;
            }
        }
    }
    frame
}

/// The calls of one request frame, from [`collect_append_stream_frame`].
struct CollectedFrame {
    calls: Vec<AppendStreamCall>,
    /// Encoded bytes of `calls`.
    bytes: usize,
    /// False once every sender is gone: no call follows these.
    receiver_open: bool,
}

type PendingAppend = oneshot::Sender<Result<raft_internal_proto::RaftRpcAckV1, tonic::Status>>;

#[derive(Default)]
struct PendingAppends(BTreeMap<u64, PendingAppend>);

impl std::ops::Deref for PendingAppends {
    type Target = BTreeMap<u64, PendingAppend>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for PendingAppends {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Drop for PendingAppends {
    fn drop(&mut self) {
        GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(self.len() as u64, Ordering::Relaxed);
    }
}

/// One request frame on its way to the encoder, with the queue budget its calls hold until the
/// encoder takes it.
type WireFrame = (
    raft_internal_proto::RaftAppendStreamRequest,
    Vec<QueuedAppendBytes>,
);

/// Where an append session records its peer's refusal to open it for
/// another cluster id: the stream's calls only see it close.
struct PeerRefusals {
    peer_cluster: PeerCluster,
    target: u64,
    address: String,
}

async fn run_append_session(
    mut client: RaftClient,
    plane: tonic::metadata::AsciiMetadataValue,
    mut calls: mpsc::UnboundedReceiver<AppendStreamCall>,
    refusals: PeerRefusals,
) {
    let (wire_sender, wire_receiver) =
        mpsc::channel::<WireFrame>(RAFT_GRPC_APPEND_STREAM_WIRE_FRAMES);
    // The encoder pulls the next frame only when HTTP/2 flow control has room; the queue budget is
    // released right there, so it bounds exactly the bytes waiting on this side of the wire.
    let outbound = ReceiverStream::new(wire_receiver).map(|(request, queued)| {
        drop(queued);
        request
    });
    client = client.send_compressed(CompressionEncoding::Zstd);
    let mut request = tonic::Request::new(outbound);
    request
        .metadata_mut()
        .insert(crate::target::RAFT_PLANE_METADATA, plane);
    let response = tokio::time::timeout(
        RAFT_GRPC_APPEND_STREAM_CONNECT_TIMEOUT,
        client.append_stream(request),
    )
    .await;
    let mut responses = match response {
        Ok(Ok(response)) => {
            GRPC_APPEND_STREAM_SESSIONS_OPENED.fetch_add(1, Ordering::Relaxed);
            response.into_inner()
        }
        Ok(Err(status)) => {
            GRPC_APPEND_STREAM_SESSION_FAILURES.fetch_add(1, Ordering::Relaxed);
            refusals.peer_cluster.observe_refusal(
                refusals.target,
                &refusals.address,
                &status,
                crate::rt::time::Instant::now(),
            );
            return;
        }
        Err(_) => {
            GRPC_APPEND_STREAM_SESSION_FAILURES.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    let mut wire_sender = Some(wire_sender);
    let mut pending = PendingAppends::default();
    let mut next_request_id = 1_u64;
    let mut accepting = true;
    let mut last_progress = tokio::time::Instant::now();

    loop {
        let pending_before_retain = pending.len();
        pending.retain(|_, response| !response.is_closed());
        let cancelled = pending_before_retain.saturating_sub(pending.len()) as u64;
        GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(cancelled, Ordering::Relaxed);
        if !accepting && pending.is_empty() {
            break;
        }
        if pending.is_empty() {
            last_progress = tokio::time::Instant::now();
        }
        let stall_wait = RAFT_GRPC_APPEND_STREAM_STALL_TIMEOUT
            .saturating_sub(tokio::time::Instant::now().saturating_duration_since(last_progress));
        // Take a call only once the wire has room for its frame, so this loop never blocks on
        // the wire while responses (which free the peer to read more) wait unread.
        let next_call = async {
            let slot = match wire_sender.as_ref() {
                Some(sender) => sender.clone().reserve_owned().await.ok(),
                None => None,
            };
            (slot, calls.recv().await)
        };
        tokio::select! {
            biased;
            response = responses.message() => {
                match response {
                    Ok(Some(response)) => {
                        last_progress = tokio::time::Instant::now();
                        GRPC_APPEND_STREAM_RESPONSE_FRAMES.fetch_add(1, Ordering::Relaxed);
                        GRPC_APPEND_STREAM_RESPONSE_BYTES.fetch_add(
                            response.encoded_len() as u64,
                            Ordering::Relaxed,
                        );
                        for item in response.items {
                            let Some(reply) = pending.remove(&item.request_id) else {
                                continue;
                            };
                            GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
                            GRPC_APPEND_STREAM_RESPONSES.fetch_add(1, Ordering::Relaxed);
                            let result = match item.result {
                                Some(raft_internal_proto::raft_append_stream_response_item::Result::Ack(ack)) => Ok(ack),
                                Some(raft_internal_proto::raft_append_stream_response_item::Result::Error(error)) => {
                                    Err(stream_item_status(error))
                                }
                                None => Err(tonic::Status::internal(
                                    "raft append stream response item is missing its result",
                                )),
                            };
                            reply_to(reply, result);
                        }
                    }
                    Ok(None) => {
                        GRPC_APPEND_STREAM_SESSION_FAILURES.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_status) => {
                        GRPC_APPEND_STREAM_SESSION_FAILURES.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
            () = tokio::time::sleep(stall_wait), if !pending.is_empty() => {
                GRPC_APPEND_STREAM_STALLS.fetch_add(1, Ordering::Relaxed);
                GRPC_APPEND_STREAM_SESSION_FAILURES.fetch_add(1, Ordering::Relaxed);
                break;
            }
            (slot, call) = next_call, if accepting => {
                let Some(call) = call else {
                    accepting = false;
                    wire_sender.take();
                    continue;
                };
                let CollectedFrame {
                    calls: frame_calls,
                    bytes: frame_bytes,
                    receiver_open,
                } = collect_append_stream_frame(call, &mut calls);
                if !receiver_open {
                    accepting = false;
                }
                if frame_calls.is_empty() {
                    continue;
                }
                let Some(slot) = slot else {
                    for call in frame_calls {
                        reply_to(
                            call.response,
                            Err(tonic::Status::unavailable(
                                "raft append stream request channel closed",
                            )),
                        );
                    }
                    accepting = false;
                    wire_sender.take();
                    continue;
                };
                let frame_items = frame_calls.len() as u64;
                let inflight = GRPC_APPEND_STREAM_INFLIGHT
                    .fetch_add(frame_items, Ordering::Relaxed)
                    .saturating_add(frame_items);
                GRPC_APPEND_STREAM_INFLIGHT_MAX.fetch_max(inflight, Ordering::Relaxed);
                GRPC_APPEND_STREAM_REQUESTS.fetch_add(frame_items, Ordering::Relaxed);
                GRPC_APPEND_STREAM_REQUEST_FRAMES.fetch_add(1, Ordering::Relaxed);
                if frame_items > 1 {
                    GRPC_APPEND_STREAM_BATCH_FRAMES.fetch_add(1, Ordering::Relaxed);
                }
                GRPC_APPEND_STREAM_BATCH_ITEMS_MAX.fetch_max(frame_items, Ordering::Relaxed);

                let mut items = Vec::with_capacity(frame_calls.len());
                let mut queued = Vec::with_capacity(frame_calls.len());
                for call in frame_calls {
                    let request_id = next_request_id;
                    next_request_id = next_request_id.saturating_add(1);
                    pending.insert(request_id, call.response);
                    queued.push(call.queued);
                    items.push(raft_internal_proto::RaftAppendStreamRequestItem {
                        request_id,
                        envelope: Some(call.envelope),
                    });
                }
                GRPC_APPEND_STREAM_REQUEST_BYTES.fetch_add(frame_bytes as u64, Ordering::Relaxed);
                GRPC_APPEND_STREAM_FRAME_BYTES_MAX.fetch_max(frame_bytes as u64, Ordering::Relaxed);
                // The slot was reserved above, so this cannot wait.
                let _sender =
                    slot.send((raft_internal_proto::RaftAppendStreamRequest { items }, queued));
            }
        }
    }

    let abandoned = pending.len() as u64;
    GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(abandoned, Ordering::Relaxed);
    for (_, response) in std::mem::take(&mut pending.0) {
        reply_to(
            response,
            Err(tonic::Status::unavailable(
                "raft append stream closed before the peer replied",
            )),
        );
    }
}

pub(crate) fn normalize_grpc_endpoint(address: String) -> String {
    let address = address.trim_end_matches('/').to_owned();
    if address.starts_with("http://") || address.starts_with("https://") {
        address
    } else {
        format!("http://{address}")
    }
}

pub(crate) fn raft_rpc_network_error(message: impl ToString) -> RPCError<UrsulaRaftTypeConfig> {
    RPCError::Network(NetworkError::from_string(message))
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for GrpcRaftNetwork {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: UrsulaAppendEntriesRequest,
        option: RPCOption,
    ) -> Result<UrsulaAppendEntriesResponse, RPCError<UrsulaRaftTypeConfig>> {
        let envelope = self.append_envelope(&rpc);
        record_append_logical_sample(append_logical_sample(&rpc, envelope.encoded_len()));
        let ack = self.append_rpc(envelope, option).await?;
        GRPC_APPEND_RESPONSE_BYTES.fetch_add(ack.encoded_len() as u64, Ordering::Relaxed);
        self.decode_rpc_ack("Append", &ack.payload)
    }

    async fn vote(
        &mut self,
        rpc: UrsulaVoteRequest,
        option: RPCOption,
    ) -> Result<UrsulaVoteResponse, RPCError<UrsulaRaftTypeConfig>> {
        let envelope = self.vote_envelope(rpc);
        GRPC_VOTE_REQUESTS.fetch_add(1, Ordering::Relaxed);
        GRPC_VOTE_REQUEST_BYTES.fetch_add(envelope.encoded_len() as u64, Ordering::Relaxed);
        let ack = self
            .call("Vote", envelope, option, |mut client, request| async move {
                client.vote(request).await
            })
            .await?;
        GRPC_VOTE_RESPONSE_BYTES.fetch_add(ack.encoded_len() as u64, Ordering::Relaxed);
        self.decode_rpc_ack("Vote", &ack.payload)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: UrsulaSnapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        // The lease keeps the object the pointer names until the RPC returns.
        let (snapshot_payload, _lease) = snapshot.snapshot.into_parts();
        let request = self.stamped(raft_internal_proto::RaftFullSnapshotRequestV1 {
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            vote: encode_wire(&vote),
            snapshot_meta: encode_wire(&snapshot.meta),
            snapshot_payload: snapshot_payload.into(),
            ..Default::default()
        });
        GRPC_SNAPSHOT_REQUESTS.fetch_add(1, Ordering::Relaxed);
        GRPC_SNAPSHOT_REQUEST_BYTES.fetch_add(request.encoded_len() as u64, Ordering::Relaxed);
        GRPC_SNAPSHOT_PAYLOAD_BYTES
            .fetch_add(request.snapshot_payload.len() as u64, Ordering::Relaxed);
        // RaftCore cancels a send it no longer wants, such as one to a
        // follower that caught up, and every send once it stops (#709).
        let sent = self.call(
            "FullSnapshot",
            request,
            option,
            |mut client, request| async move { client.full_snapshot(request).await },
        );
        let ack = crate::rt::select! {
            biased;
            closed = cancel => return Err(StreamingError::Closed(closed)),
            ack = sent => ack.map_err(StreamingError::from)?,
        };
        GRPC_SNAPSHOT_RESPONSE_BYTES.fetch_add(ack.encoded_len() as u64, Ordering::Relaxed);
        self.decode_rpc_ack("FullSnapshot", &ack.response)
            .map_err(StreamingError::from)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<UrsulaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig>,
        RPCError<UrsulaRaftTypeConfig>,
    > {
        let envelope = self.transfer_leader_envelope(&req);
        let ack = self
            .call(
                "TransferLeader",
                envelope,
                option,
                |mut client, request| async move { client.transfer_leader(request).await },
            )
            .await?;
        self.decode_rpc_ack("TransferLeader", &ack.response)
    }
}

#[cfg(test)]
mod reconnect_tests {
    #[test]
    fn node_budget_is_shared_across_core_pools_and_small_limits_are_valid() {
        let budget = super::AppendTransportBudget::new(512);
        let identity = crate::peer_cluster::test_identity(0);
        let a = super::CoreRaftTransport::with_budget(
            budget.clone(),
            identity.clone(),
            super::PeerCluster::default(),
        );
        let b =
            super::CoreRaftTransport::with_budget(budget, identity, super::PeerCluster::default());
        let charge = super::append_budget_charge(1, 512);
        assert_eq!(charge, 512);
        let permit = a
            .budget
            .semaphore
            .clone()
            .try_acquire_many_owned(charge)
            .unwrap();
        assert!(matches!(
            b.budget.semaphore.clone().try_acquire_owned(),
            Err(tokio::sync::TryAcquireError::NoPermits)
        ));
        drop(permit);
        assert_eq!(b.budget.semaphore.available_permits(), 512);
    }

    impl GrpcRaftNetwork {
        /// The pooled channel's generation this client sends on.
        fn generation(&self) -> u64 {
            self.channel
                .as_ref()
                .map(|channel| channel.generation)
                .expect("a client with a channel")
        }
    }

    fn test_network(
        transport: Arc<CoreRaftTransport>,
        group: RaftGroupId,
        target: u64,
        address: impl Into<String>,
    ) -> GrpcRaftNetwork {
        GrpcRaftNetwork::with_transport(transport, group, target, address, 8)
    }

    use std::collections::BTreeSet;
    use std::time::Duration;

    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::entry::RaftEntry;
    use openraft::vote::RaftLeaderId;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Endpoint;
    use ursula_stream::StreamCommand;

    use super::*;
    use crate::RaftCommand;

    /// An Append envelope for data group `raft_group_id` on node 1, the
    /// node every test server here runs as.
    fn test_envelope(
        raft_group_id: u32,
        payload: bytes::Bytes,
    ) -> raft_internal_proto::RaftRpcEnvelopeV1 {
        let mut envelope = raft_internal_proto::RaftRpcEnvelopeV1 {
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload,
            ..Default::default()
        };
        RaftTarget::Data(RaftGroupId(raft_group_id)).stamp(1, &mut envelope);
        envelope
    }

    #[tokio::test]
    async fn different_owner_pools_never_share_peer_sessions_or_channels() {
        let a = CoreRaftTransport::default();
        let b = CoreRaftTransport::default();
        let endpoint = "http://127.0.0.1:9";
        let (client_a, _) = shared_raft_client(&a, endpoint, None).unwrap();
        let (client_b, _) = shared_raft_client(&b, endpoint, None).unwrap();
        let data = RaftTarget::Data(RaftGroupId(1));
        let first = shared_append_session(&a, endpoint, 1, data, client_a.clone()).unwrap();
        let same = shared_append_session(&a, endpoint, 1, data, client_a).unwrap();
        let other = shared_append_session(&b, endpoint, 1, data, client_b).unwrap();
        assert!(first.sender.same_channel(&same.sender));
        assert!(!first.sender.same_channel(&other.sender));
        assert!(!Arc::ptr_eq(&first.budget, &other.budget));
        shared_raft_client(&a, endpoint, Some(1)).unwrap();
        assert_eq!(a.channels.lock().unwrap()[endpoint].generation, 2);
        assert_eq!(b.channels.lock().unwrap()[endpoint].generation, 1);
    }

    fn remove_shared_channel(transport: &CoreRaftTransport, endpoint: &str) {
        if let Ok(mut channels) = transport.channels.lock() {
            channels.remove(endpoint);
        }
    }

    fn fresh_network(threshold: u32) -> GrpcRaftNetwork {
        let transport = Arc::new(CoreRaftTransport::default());
        let mut net = test_network(
            transport.clone(),
            RaftGroupId(0),
            2,
            "http://127.0.0.1:9999",
        );
        // Override threshold so tests don't depend on the env var
        net.reconnect_threshold = threshold;
        net
    }

    #[test]
    fn append_logical_sample_separates_heartbeats_and_replicated_commands() {
        let heartbeat = UrsulaAppendEntriesRequest {
            vote: openraft::Vote::new_committed(1, 1),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        };
        assert_eq!(append_logical_sample(&heartbeat, 37), AppendLogicalSample {
            heartbeat: true,
            request_bytes: 37,
            entries: 0,
        });

        type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
        let command = RaftCommand::Stream(StreamCommand::CreateBucket {
            bucket_id: "network-accounting".to_owned(),
        });
        let replication = UrsulaAppendEntriesRequest {
            vote: openraft::Vote::new_committed(1, 1),
            prev_log_id: None,
            entries: vec![Entry::new(
                LogId {
                    leader_id: LeaderId::new(1, 1),
                    index: 1,
                },
                EntryPayload::Normal(command),
            )],
            leader_commit: None,
        };
        assert_eq!(
            append_logical_sample(&replication, 211),
            AppendLogicalSample {
                heartbeat: false,
                request_bytes: 211,
                entries: 1,
            }
        );
    }

    async fn spawn_append_stream_server()
    -> (String, RaftGroupHandleRegistry, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test raft grpc listener");
        let address = listener.local_addr().expect("read test listener address");
        let registry = RaftGroupHandleRegistry::default();
        registry.peer_cluster().bind(
            crate::peer_cluster::test_identity(1),
            crate::PeerDirectory::default(),
        );
        let service_registry = registry.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(raft_grpc_service(service_registry))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve test raft grpc");
        });
        (format!("http://{address}"), registry, task)
    }

    static TEST_QUEUED_BYTES: AtomicU64 = AtomicU64::new(0);
    static TEST_QUEUED_BYTES_MAX: AtomicU64 = AtomicU64::new(0);

    /// A queued call and the receiver its caller waits on (dropping it = the caller gave up).
    fn queued_call(
        raft_group_id: u32,
    ) -> (
        AppendStreamCall,
        oneshot::Receiver<Result<raft_internal_proto::RaftRpcAckV1, tonic::Status>>,
    ) {
        queued_call_of(raft_group_id, 0)
    }

    fn queued_call_of(
        raft_group_id: u32,
        payload_bytes: usize,
    ) -> (
        AppendStreamCall,
        oneshot::Receiver<Result<raft_internal_proto::RaftRpcAckV1, tonic::Status>>,
    ) {
        let (response, receiver) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("test permit");
        let call = AppendStreamCall {
            envelope: test_envelope(raft_group_id, vec![7_u8; payload_bytes].into()),
            response,
            queued: QueuedAppendBytes::new(permit, 0, &TEST_QUEUED_BYTES, &TEST_QUEUED_BYTES_MAX),
        };
        (call, receiver)
    }

    #[test]
    fn append_stream_frame_drains_only_already_queued_calls() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (first, _first) = queued_call(1);
        let (second, _second) = queued_call(2);
        let (third, _third) = queued_call(3);
        sender.send(second).expect("queue second call");
        sender.send(third).expect("queue third call");

        let CollectedFrame {
            calls: batch,
            receiver_open,
            ..
        } = collect_append_stream_frame(first, &mut receiver);
        assert!(receiver_open);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].envelope.raft_group_id, 1);
        assert_eq!(batch[1].envelope.raft_group_id, 2);
        assert_eq!(batch[2].envelope.raft_group_id, 3);
    }

    #[test]
    fn append_stream_frame_drops_calls_whose_caller_gave_up() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (first, first_waiter) = queued_call(1);
        let (second, _second_waiter) = queued_call(2);
        let (third, third_waiter) = queued_call(3);
        drop(first_waiter);
        drop(third_waiter);
        sender.send(second).expect("queue second call");
        sender.send(third).expect("queue third call");

        let CollectedFrame {
            calls: batch,
            receiver_open,
            ..
        } = collect_append_stream_frame(first, &mut receiver);
        assert!(receiver_open);
        assert_eq!(batch.len(), 1, "timed-out calls must not be sent");
        assert_eq!(batch[0].envelope.raft_group_id, 2);
    }

    /// #507: tonic keeps a stream's buffers at the largest frame it carried, so a frame stops
    /// taking calls at its byte budget; the calls behind it wait for the next frame.
    #[test]
    fn append_stream_frame_stops_at_its_byte_budget() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        // A call holds a little over half the budget, so a second one fills the frame.
        let call_bytes = RAFT_GRPC_APPEND_STREAM_MAX_FRAME_BYTES / 2 + 1024;
        let (first, _first) = queued_call_of(1, call_bytes);
        let mut waiters = Vec::new();
        for raft_group_id in 2..=4 {
            let (call, waiter) = queued_call_of(raft_group_id, call_bytes);
            sender.send(call).expect("queue call");
            waiters.push(waiter);
        }

        let frame = collect_append_stream_frame(first, &mut receiver);
        assert!(frame.receiver_open);
        let ids = frame
            .calls
            .iter()
            .map(|call| call.envelope.raft_group_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, [1, 2]);
        let encoded = frame
            .calls
            .iter()
            .map(|call| call.envelope.encoded_len())
            .sum::<usize>();
        assert_eq!(frame.bytes, encoded);
        assert!(frame.bytes >= RAFT_GRPC_APPEND_STREAM_MAX_FRAME_BYTES);
        let next = receiver.try_recv().expect("the rest stay queued");
        assert_eq!(next.envelope.raft_group_id, 3);
    }

    /// The EKS OOM: a peer that does not keep up must not let the leader queue an unbounded
    /// backlog of Append calls (each OpenRaft retry after a 250 ms timeout queued another copy of
    /// the same entries). Here the session never drains: the queue stops at the byte budget, later
    /// calls fail fast as backpressure, and backpressure does not count toward a channel rebuild.
    #[tokio::test]
    async fn stalled_peer_append_queue_is_bounded_by_its_byte_budget() {
        let transport = Arc::new(CoreRaftTransport::default());
        let endpoint = "http://127.0.0.1:9".to_owned();
        let (sender, _stalled_receiver) = mpsc::unbounded_channel();
        let budget = Arc::new(Semaphore::new(RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES));
        transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .insert(endpoint.clone(), SharedAppendSession {
                _task: Arc::new(AppendSessionTask::Paused),
                sender,
                budget: budget.clone(),
            });
        let mut network = test_network(transport.clone(), RaftGroupId(1), 2, endpoint.clone());
        let entry_bytes = 1024 * 1024;
        let envelope = || test_envelope(1, vec![7_u8; entry_bytes].into());
        let mut timed_out = 0;
        let mut rejected = 0;
        for _ in 0..(3 * RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES / entry_bytes) {
            let result = network
                .try_append_stream(envelope(), RPCOption::new(Duration::from_millis(1)))
                .await;
            match result.expect_err("a stalled peer never answers") {
                AppendCallError::Status(status)
                    if status.code() == tonic::Code::DeadlineExceeded =>
                {
                    timed_out += 1;
                }
                AppendCallError::BacklogFull { .. } => rejected += 1,
                other => panic!("unexpected error {other:?}"),
            }
        }
        // Only what fits the budget was queued (and is still held: nothing drained it).
        assert!(timed_out <= RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES / entry_bytes);
        assert!(budget.available_permits() < entry_bytes);
        assert!(rejected >= 2 * RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES / entry_bytes);

        // Backpressure is congestion on a live stream, not a broken channel.
        let failures_before = network.consecutive_failures;
        let error = network
            .append_rpc(envelope(), RPCOption::new(Duration::from_millis(1)))
            .await
            .expect_err("still backlogged");
        assert!(matches!(error, RPCError::Unreachable(_)));
        assert_eq!(network.consecutive_failures, failures_before);
        transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .remove(&endpoint);
    }

    /// Fair admission: a large catch-up append waiting for budget is served
    /// before later small calls, which must not overtake it.
    #[tokio::test]
    async fn large_append_is_not_starved_by_later_small_calls() {
        let transport = Arc::new(CoreRaftTransport::default());
        let endpoint = "http://127.0.0.1:10".to_owned();
        let (sender, _stalled_receiver) = mpsc::unbounded_channel();
        let budget = Arc::new(Semaphore::new(RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES));
        transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .insert(endpoint.clone(), SharedAppendSession {
                _task: Arc::new(AppendSessionTask::Paused),
                sender,
                budget: budget.clone(),
            });
        let mib = 1024 * 1024;
        let others = budget
            .clone()
            .try_acquire_many_owned(
                u32::try_from(RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES - mib).expect("fits"),
            )
            .expect("other groups' queued bytes");
        let envelope =
            |raft_group_id, bytes: usize| test_envelope(raft_group_id, vec![7_u8; bytes].into());
        let large = test_network(transport.clone(), RaftGroupId(1), 2, endpoint.clone());
        let large = tokio::spawn(async move {
            large
                .try_append_stream(envelope(1, 2 * mib), RPCOption::new(Duration::from_secs(2)))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Budget is free for a small call, but the large one is ahead of it.
        let small = test_network(transport.clone(), RaftGroupId(2), 2, endpoint.clone())
            .try_append_stream(envelope(2, 16), RPCOption::new(Duration::from_millis(50)))
            .await
            .expect_err("the small call waits behind the large one");
        assert!(matches!(small, AppendCallError::BacklogFull { .. }));

        drop(others);
        let large = large.await.expect("large call task");
        // Admitted and queued; the stalled peer never answers it.
        assert_eq!(
            tonic::Status::from(large.expect_err("stalled peer")).code(),
            tonic::Code::DeadlineExceeded
        );
        transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .remove(&endpoint);
    }

    #[tokio::test]
    async fn append_stream_accepts_a_batch_frame_with_independent_results() {
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        let channel = Endpoint::from_shared(endpoint)
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to test server");
        let mut client =
            raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
                channel,
                crate::peer_cluster::test_identity(2),
            );
        let (sender, receiver) = mpsc::channel(1);
        let item = |request_id, raft_group_id| raft_internal_proto::RaftAppendStreamRequestItem {
            request_id,
            envelope: Some(test_envelope(raft_group_id, Vec::new().into())),
        };
        sender
            .send(raft_internal_proto::RaftAppendStreamRequest {
                items: vec![item(11, 1), item(12, 2)],
            })
            .await
            .expect("send batch frame");
        let response = client
            .append_stream(tonic::Request::new(ReceiverStream::new(receiver)))
            .await
            .expect("open append stream");
        let frame = response
            .into_inner()
            .message()
            .await
            .expect("read batch response")
            .expect("batch response frame");
        assert_eq!(frame.items.len(), 2);
        let ids = frame
            .items
            .iter()
            .map(|item| item.request_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids, BTreeSet::from([11, 12]));
        assert!(frame.items.iter().all(|item| matches!(
            item.result,
            Some(raft_internal_proto::raft_append_stream_response_item::Result::Error(_))
        )));
        server.abort();
    }

    /// Opens an `AppendStream` to `endpoint` whose `ursula-raft-plane`
    /// metadata is `plane` (none for `None`), sends `envelopes` as one frame
    /// and returns each item's refusal, in the order sent.
    async fn refused_stream_items(
        endpoint: &str,
        plane: Option<&'static str>,
        envelopes: Vec<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Vec<raft_internal_proto::RaftAppendStreamError> {
        let channel = Endpoint::from_shared(endpoint.to_owned())
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to test server");
        let mut client =
            raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
                channel,
                crate::peer_cluster::test_identity(2),
            );
        let count = envelopes.len();
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(raft_internal_proto::RaftAppendStreamRequest {
                items: envelopes
                    .into_iter()
                    .zip(0_u64..)
                    .map(|(envelope, request_id)| {
                        raft_internal_proto::RaftAppendStreamRequestItem {
                            request_id,
                            envelope: Some(envelope),
                        }
                    })
                    .collect(),
            })
            .await
            .expect("send frame");
        let mut request = tonic::Request::new(ReceiverStream::new(receiver));
        if let Some(plane) = plane {
            request.metadata_mut().insert(
                crate::target::RAFT_PLANE_METADATA,
                tonic::metadata::AsciiMetadataValue::from_static(plane),
            );
        }
        let frame = client
            .append_stream(request)
            .await
            .expect("open append stream")
            .into_inner()
            .message()
            .await
            .expect("read response")
            .expect("response frame");
        let mut items = frame.items;
        assert_eq!(items.len(), count);
        items.sort_by_key(|item| item.request_id);
        items
            .into_iter()
            .map(|item| match item.result {
                Some(raft_internal_proto::raft_append_stream_response_item::Result::Error(
                    error,
                )) => error,
                other => panic!("item {} is not refused: {other:?}", item.request_id),
            })
            .collect()
    }

    /// An `AppendStream` carries the items of the one plane its
    /// `ursula-raft-plane` metadata names. A stream that names none, an
    /// unknown one or the meta plane has each data item refused
    /// (`INVALID_ARGUMENT`) before the group lookup, which answers a data
    /// stream's item for a group this node does not run `NOT_FOUND`, and a
    /// meta stream's meta item likewise, as no node runs the meta group yet.
    #[tokio::test]
    async fn an_append_stream_refuses_items_of_another_plane_than_its_own() {
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        let data = || test_envelope(1, Vec::new().into());
        let meta = || {
            let mut envelope = test_envelope(0, Vec::new().into());
            RaftTarget::Meta.stamp(1, &mut envelope);
            envelope
        };
        let code =
            |error: &raft_internal_proto::RaftAppendStreamError| tonic::Code::from_i32(error.code);
        for plane in [None, Some("other"), Some("meta")] {
            let errors = refused_stream_items(&endpoint, plane, vec![data(), data()]).await;
            for error in &errors {
                assert_eq!(
                    code(error),
                    tonic::Code::InvalidArgument,
                    "{plane:?}: {error:?}"
                );
                assert_eq!(error.refusing_node, 0);
            }
        }
        let errors = refused_stream_items(&endpoint, Some("data"), vec![data(), meta()]).await;
        assert_eq!(errors.iter().map(code).collect::<Vec<_>>(), [
            tonic::Code::NotFound,
            tonic::Code::InvalidArgument
        ]);
        let errors = refused_stream_items(&endpoint, Some("meta"), vec![meta()]).await;
        assert_eq!(code(&errors[0]), tonic::Code::NotFound);
        server.abort();
    }

    /// A stream item for another node is refused as misdelivered, and the
    /// refusal reaches the sender as that of a unary call does:
    /// `FAILED_PRECONDITION` with the refusing node's id, which the sender
    /// reads through `refusing_node`, and never a protocol mismatch.
    #[tokio::test]
    async fn a_misdelivered_stream_item_names_the_refusing_node() {
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        let mut misdelivered = test_envelope(1, Vec::new().into());
        RaftTarget::Data(RaftGroupId(1)).stamp(2, &mut misdelivered);

        let errors =
            refused_stream_items(&endpoint, Some("data"), vec![misdelivered.clone()]).await;
        assert_eq!(
            tonic::Code::from_i32(errors[0].code),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(errors[0].refusing_node, 1);

        let transport = Arc::new(CoreRaftTransport::default());
        remove_shared_channel(&transport, &endpoint);
        let status = tonic::Status::from(
            test_network(transport, RaftGroupId(1), 2, endpoint.clone())
                .try_append_stream(misdelivered, RPCOption::new(Duration::from_secs(2)))
                .await
                .expect_err("node 1 is not node 2"),
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(crate::target::refusing_node(&status), Some(1));
        assert!(!crate::format_epoch::is_protocol_mismatch(&status));
        server.abort();
    }

    #[tokio::test]
    async fn append_stream_client_coalesces_concurrent_group_calls() {
        let transport = Arc::new(CoreRaftTransport::default());
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        remove_shared_channel(&transport, &endpoint);
        let batch_frames_before = GRPC_APPEND_STREAM_BATCH_FRAMES.load(Ordering::Relaxed);
        let envelope = |raft_group_id| test_envelope(raft_group_id, Vec::new().into());
        let calls = (1..=16).map(|raft_group_id| {
            let network = test_network(
                transport.clone(),
                RaftGroupId(raft_group_id),
                2,
                endpoint.clone(),
            );
            async move {
                network
                    .try_append_stream(
                        envelope(raft_group_id),
                        RPCOption::new(Duration::from_secs(2)),
                    )
                    .await
            }
        });

        let results = futures_util::future::join_all(calls).await;

        assert!(results.into_iter().all(|result| {
            tonic::Status::from(result.expect_err("missing group should fail")).code()
                == tonic::Code::NotFound
        }));
        assert!(
            GRPC_APPEND_STREAM_BATCH_FRAMES.load(Ordering::Relaxed) > batch_frames_before,
            "concurrent groups should share at least one multi-item frame"
        );
        assert!(GRPC_APPEND_STREAM_BATCH_ITEMS_MAX.load(Ordering::Relaxed) > 1);
        server.abort();
    }

    /// #507: `raft_grpc_append_stream_frame_bytes_max` reports the largest request frame sent,
    /// as the encoded bytes of its calls before compression. The gauge is process-wide, and no
    /// other test sends a frame anywhere near 1 MiB through a live session, so it reads less than
    /// one such frame before this test sends any. After two of them it reads the size of one,
    /// where a sum would read two, and a smaller frame after them does not lower it. That no
    /// other test sends such a frame is a convention nothing enforces: one that does would break
    /// this test, and should then read the gauge in a process of its own.
    #[tokio::test]
    async fn append_stream_frame_bytes_max_reports_the_largest_frame_sent() {
        let gauge = || raft_grpc_metrics_snapshot().raft_grpc_append_stream_frame_bytes_max;
        let transport = Arc::new(CoreRaftTransport::default());
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        let network = test_network(transport, RaftGroupId(1), 2, endpoint);
        let envelope_of = |payload_bytes: usize| test_envelope(1, vec![7_u8; payload_bytes].into());
        let large = envelope_of(1024 * 1024);
        let frame_bytes = u64::try_from(large.encoded_len()).expect("fits in u64");
        let before = gauge();
        assert!(
            before < frame_bytes,
            "a {before} B frame went out before this test's"
        );
        // Each call waits for its answer, so each goes out in a frame of its own.
        for envelope in [large.clone(), large, envelope_of(1024)] {
            let status = tonic::Status::from(
                network
                    .try_append_stream(envelope, RPCOption::new(Duration::from_secs(10)))
                    .await
                    .expect_err("the peer hosts no group"),
            );
            // The peer answered the call, so its frame went out.
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
        let max = gauge();
        assert!(
            (frame_bytes..2 * frame_bytes).contains(&max),
            "the largest frame is {frame_bytes} B, and the gauge reads {max} B"
        );
        server.abort();
    }

    #[tokio::test]
    async fn groups_share_one_append_stream_and_receive_independent_errors() {
        let transport = Arc::new(CoreRaftTransport::default());
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        remove_shared_channel(&transport, &endpoint);

        let mut first = test_network(transport.clone(), RaftGroupId(1), 2, endpoint.clone());
        let second = test_network(transport.clone(), RaftGroupId(2), 2, endpoint.clone());
        let envelope = |raft_group_id| test_envelope(raft_group_id, Vec::new().into());
        let option = RPCOption::new(Duration::from_secs(2));
        let (first_result, second_result) = tokio::join!(
            first.try_append_stream(envelope(1), option.clone()),
            second.try_append_stream(envelope(2), option),
        );

        assert_eq!(
            tonic::Status::from(first_result.expect_err("missing group should fail")).code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            tonic::Status::from(second_result.expect_err("second missing group should fail"))
                .code(),
            tonic::Code::NotFound
        );
        let shared_session_open = transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .get(&endpoint)
            .is_some_and(|session| !session.sender.is_closed());
        assert!(shared_session_open);

        let original_sender = transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .get(&endpoint)
            .expect("shared append session")
            .sender
            .clone();
        let original_generation = first.generation();
        for _ in 0..first.reconnect_threshold {
            first.note_failure("test channel replacement");
        }
        assert_ne!(first.generation(), original_generation);
        let result = first
            .try_append_stream(envelope(1), RPCOption::new(Duration::from_secs(2)))
            .await;
        assert_eq!(
            tonic::Status::from(result.expect_err("missing group should still fail")).code(),
            tonic::Code::NotFound
        );
        let replacement_sender = transport
            .sessions
            .lock()
            .expect("append session pool lock")
            .get(&endpoint)
            .expect("shared append session")
            .sender
            .clone();
        assert!(
            original_sender.same_channel(&replacement_sender),
            "a unary channel generation change must not replace a healthy append stream"
        );

        server.abort();
    }

    /// A client dials the cluster URL `raft.peers` lists for its target.
    #[tokio::test]
    async fn the_factory_dials_the_address_raft_peers_lists() {
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        let transport = Arc::new(CoreRaftTransport::new(
            crate::peer_cluster::test_identity(2),
            crate::peer_cluster::test_peer_cluster_with(
                2,
                crate::PeerDirectory::new([(1, crate::PeerEndpoints::shared(endpoint.as_str()))]),
            ),
        ));
        let mut factory = GrpcRaftNetworkFactory::new(transport, RaftGroupId(1));
        let mut network = factory.new_client(1, &EmptyNode {}).await;
        assert_eq!(network.endpoint(), endpoint);
        // The server hosts no group 1: the vote reached it and was answered.
        let vote = network
            .vote(
                UrsulaVoteRequest::new(UrsulaVote::new(1, 2), None),
                RPCOption::new(Duration::from_secs(2)),
            )
            .await
            .expect_err("the server hosts no group 1");
        assert!(matches!(vote, RPCError::Network(_)), "{vote:?}");
        server.abort();
    }

    /// A member that `raft.peers` does not list is unreachable: its client
    /// answers every RPC without dialing, and has no channel to rebuild.
    #[tokio::test]
    async fn a_member_raft_peers_does_not_list_is_never_dialed() {
        let transport = Arc::new(CoreRaftTransport::new(
            crate::peer_cluster::test_identity(2),
            crate::peer_cluster::test_peer_cluster(2),
        ));
        let mut factory = GrpcRaftNetworkFactory::new(transport.clone(), RaftGroupId(1))
            .with_reconnect_threshold(1);
        let mut network = factory.new_client(7, &EmptyNode {}).await;
        assert!(matches!(
            network.channel,
            Err(RaftChannelError::Unaddressed { node_id: 7 })
        ));
        let option = RPCOption::new(Duration::from_secs(2));
        let vote = network
            .vote(
                UrsulaVoteRequest::new(UrsulaVote::new(1, 2), None),
                option.clone(),
            )
            .await
            .expect_err("nothing dials an unlisted member");
        assert!(matches!(vote, RPCError::Unreachable(_)), "{vote:?}");
        let append = network
            .append_entries(
                UrsulaAppendEntriesRequest {
                    vote: openraft::Vote::new_committed(1, 2),
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                },
                option,
            )
            .await
            .expect_err("nothing dials an unlisted member");
        assert!(matches!(append, RPCError::Unreachable(_)), "{append:?}");
        assert!(transport.channels.lock().unwrap().is_empty());
        assert!(transport.sessions.lock().unwrap().is_empty());
    }

    /// A peer of another cluster refuses this node's votes and appends
    /// before they run, and both sides record the other by node id: the
    /// sender from the refusals of a unary call and of an append stream's
    /// open, the peer from the requests it refused.
    #[tokio::test]
    async fn a_peer_of_another_cluster_is_recorded_from_its_refusals() {
        let (endpoint, registry, server) = spawn_append_stream_server().await;
        // The server lists node 3, at loopback, among its peers.
        registry.peer_cluster().bind(
            crate::peer_cluster::test_identity(1),
            crate::PeerDirectory::new([(3, crate::PeerEndpoints::shared("http://127.0.0.1:4437"))]),
        );
        let other = ursula_headers::ClusterId::new("other-cluster").unwrap();
        let peer_cluster = PeerCluster::default();
        let transport = Arc::new(CoreRaftTransport::new(
            PeerIdentity::new(3, other.clone()),
            peer_cluster.clone(),
        ));
        let mut network = test_network(transport, RaftGroupId(1), 1, endpoint);
        let option = RPCOption::new(Duration::from_secs(2));
        let vote = network
            .vote(
                UrsulaVoteRequest::new(UrsulaVote::new(1, 3), None),
                option.clone(),
            )
            .await
            .expect_err("a peer of another cluster refuses the vote");
        assert!(matches!(vote, RPCError::Unreachable(_)), "{vote:?}");
        assert_eq!(peer_cluster.refusals(), 1);
        let append = network
            .append_rpc(test_envelope(1, Vec::new().into()), option)
            .await;
        assert!(append.is_err(), "the append stream does not open");
        assert_eq!(peer_cluster.refusals(), 2);
        let test_cluster =
            ursula_headers::ClusterId::new(crate::peer_cluster::TEST_CLUSTER_ID).unwrap();
        assert_eq!(peer_cluster.mismatches(), vec![
            crate::peer_cluster::PeerClusterMismatch {
                node_id: 1,
                cluster_id: test_cluster,
            }
        ]);
        assert_eq!(registry.peer_cluster().mismatches(), vec![
            crate::peer_cluster::PeerClusterMismatch {
                node_id: 3,
                cluster_id: other,
            }
        ]);
        assert_eq!(registry.peer_cluster().refusals(), 2);
        server.abort();
    }

    #[tokio::test]
    async fn node_transport_shutdown_closes_append_stream() {
        let (endpoint, registry, server) = spawn_append_stream_server().await;
        let channel = Endpoint::from_shared(endpoint)
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to test server");
        let mut client =
            raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
                channel,
                crate::peer_cluster::test_identity(2),
            );
        let (_sender, receiver) = mpsc::channel(1);
        let response = client
            .append_stream(tonic::Request::new(ReceiverStream::new(receiver)))
            .await
            .expect("open append stream");
        let mut responses = response.into_inner();

        registry.shutdown_transport();

        let closed = tokio::time::timeout(Duration::from_secs(2), responses.message())
            .await
            .expect("append stream should close promptly")
            .expect("stream shutdown should not be an RPC error");
        assert!(closed.is_none());
        server.abort();
    }

    #[tokio::test]
    async fn note_failure_below_threshold_just_increments() {
        let mut net = fresh_network(5);
        for n in 1..=4 {
            net.note_failure("Append");
            assert_eq!(net.consecutive_failures, n);
        }
    }

    #[tokio::test]
    async fn crossing_threshold_rebuilds_and_resets_counter() {
        let mut net = fresh_network(3);
        net.note_failure("Append");
        net.note_failure("Append");
        assert_eq!(net.consecutive_failures, 2);
        net.note_failure("Append");
        // After crossing the threshold we should be back at 0 (the post-
        // rebuild grace period), and the client should still be valid.
        assert_eq!(net.consecutive_failures, 0);
        assert!(net.channel.is_ok(), "channel should be rebuilt cleanly");
    }

    #[tokio::test]
    async fn networks_share_one_channel_generation_per_endpoint() {
        let transport = Arc::new(CoreRaftTransport::default());
        let endpoint = "http://127.0.0.1:32197";
        remove_shared_channel(&transport, endpoint);
        let first = test_network(transport.clone(), RaftGroupId(1), 2, endpoint);
        let second = test_network(transport.clone(), RaftGroupId(2), 2, endpoint);

        assert_eq!(first.generation(), 1);
        assert_eq!(second.generation(), first.generation());
        let channel_count = transport
            .channels
            .lock()
            .ok()
            .map(|channels| usize::from(channels.contains_key(endpoint)));
        assert_eq!(channel_count, Some(1));
    }

    #[tokio::test]
    async fn stale_network_adopts_rebuilt_generation_without_replacing_it_again() {
        let transport = Arc::new(CoreRaftTransport::default());
        let endpoint = "http://127.0.0.1:32198";
        remove_shared_channel(&transport, endpoint);
        let mut first =
            GrpcRaftNetwork::with_transport(transport.clone(), RaftGroupId(1), 2, endpoint, 1);
        let mut stale =
            GrpcRaftNetwork::with_transport(transport.clone(), RaftGroupId(2), 2, endpoint, 1);
        let original_generation = first.generation();

        first.note_failure("Append");
        assert_eq!(first.generation(), original_generation.saturating_add(1));

        stale.note_failure("Append");
        assert_eq!(stale.generation(), first.generation());
        let pooled_generation = transport
            .channels
            .lock()
            .ok()
            .and_then(|channels| channels.get(endpoint).map(|shared| shared.generation));
        assert_eq!(pooled_generation, Some(first.generation()));
    }

    #[tokio::test]
    async fn success_clears_the_streak() {
        let mut net = fresh_network(5);
        net.note_failure("Append");
        net.note_failure("Append");
        assert_eq!(net.consecutive_failures, 2);
        net.note_success();
        assert_eq!(net.consecutive_failures, 0);
        // A subsequent failure starts the streak from 1, not 3 — the grace
        // period truly resets, so a flaky connection that periodically
        // succeeds doesn't accumulate toward a forced rebuild.
        net.note_failure("Append");
        assert_eq!(net.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn rebuild_path_does_not_panic_even_on_unparseable_endpoint() {
        let transport = Arc::new(CoreRaftTransport::default());
        // tonic accepts a lot of textually-weird endpoints (e.g. "not-a-url"
        // gets normalized to "http://not-a-url" and parses fine; it just
        // fails on connect). Force a real `from_shared` rejection with a
        // genuinely-invalid URI — the rebuild path must surface that as a
        // permanent Err on `client`, not panic, so openraft keeps retrying.
        let mut net = test_network(transport.clone(), RaftGroupId(0), 2, "http://");
        net.reconnect_threshold = 2;
        net.note_failure("Append");
        net.note_failure("Append");
        assert_eq!(net.consecutive_failures, 0);
        // Whether the post-rebuild client is Ok or Err is tonic's choice for
        // this endpoint string; the contract is just "no panic, counter reset".
    }
}

/// The barrier asker takes only a live leader's answer about the group it
/// asked: it refuses memberships that do not decode or name no voters, an
/// answer about another plane or group, and a misdelivery refusal, which is
/// no answer at all.
#[cfg(all(test, not(madsim)))]
mod barrier_asker_tests {
    use std::collections::BTreeSet;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::Stream;
    use openraft::type_config::alias::StoredMembershipOf;
    use openraft::vote::RaftLeaderId;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::Request;
    use tonic::Response;
    use tonic::Status;
    use ursula_shard::CoreId;
    use ursula_shard::RaftGroupId;
    use ursula_shard::ShardId;
    use ursula_shard::ShardPlacement;

    use super::CoreRaftTransport;
    use super::GrpcRpcError;
    use super::RecoveryProbeError;
    use super::probe_rejoin_vote_barrier;
    use crate::codec::encode_wire;
    use crate::memberships::BarrierAnswer;
    use crate::memberships::LeaderMemberships;
    use crate::memberships::MalformedMembership;
    use crate::memberships::MembershipKind;
    use crate::raft_internal_proto as pb;
    use crate::raft_internal_proto::raft_internal_server::RaftInternal;
    use crate::types::UrsulaRaftTypeConfig;
    use crate::types::UrsulaVote;

    type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
    type Answer = Result<pb::RejoinBarrierResponseV1, Status>;

    /// The group the asker asks about, and its leader.
    const GROUP: RaftGroupId = RaftGroupId(3);
    const LEADER: u64 = 2;

    /// A peer that answers every barrier with its script. Every other RPC
    /// is unimplemented.
    #[derive(Clone)]
    struct ScriptedBarrier {
        answer: Arc<dyn Fn() -> Answer + Send + Sync>,
    }

    #[tonic::async_trait]
    impl RaftInternal for ScriptedBarrier {
        type AppendStreamStream =
            Pin<Box<dyn Stream<Item = Result<pb::RaftAppendStreamResponse, Status>> + Send>>;

        async fn append(
            &self,
            _request: Request<pb::RaftRpcEnvelopeV1>,
        ) -> Result<Response<pb::RaftRpcAckV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn append_stream(
            &self,
            _request: Request<tonic::Streaming<pb::RaftAppendStreamRequest>>,
        ) -> Result<Response<Self::AppendStreamStream>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn vote(
            &self,
            _request: Request<pb::RaftRpcEnvelopeV1>,
        ) -> Result<Response<pb::RaftRpcAckV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn full_snapshot(
            &self,
            _request: Request<pb::RaftFullSnapshotRequestV1>,
        ) -> Result<Response<pb::RaftFullSnapshotAckV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn group_write(
            &self,
            _request: Request<pb::GroupWriteRequestV1>,
        ) -> Result<Response<pb::GroupWriteResponseV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn group_read(
            &self,
            _request: Request<pb::GroupReadRequestV1>,
        ) -> Result<Response<pb::GroupReadResponseV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn rejoin_barrier(
            &self,
            _request: Request<pb::RejoinBarrierRequestV1>,
        ) -> Result<Response<pb::RejoinBarrierResponseV1>, Status> {
            (self.answer)().map(Response::new)
        }

        async fn transfer_leader(
            &self,
            _request: Request<pb::RaftTransferLeaderRequestV1>,
        ) -> Result<Response<pb::RaftTransferLeaderAckV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }

        async fn genesis_probe(
            &self,
            _request: Request<pb::GenesisProbeRequestV1>,
        ) -> Result<Response<pb::GenesisProbeAckV1>, Status> {
            Err(Status::unimplemented("a barrier peer"))
        }
    }

    /// A membership of the leader's term 5 with `configs` as its voters.
    fn membership(configs: Vec<BTreeSet<u64>>) -> StoredMembershipOf<UrsulaRaftTypeConfig> {
        StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
            Some(openraft::LogId {
                leader_id: LeaderId::new(5, LEADER),
                index: 6,
            }),
            openraft::Membership::new_with_defaults(configs, std::iter::empty()),
        )
    }

    /// The answer of a live leader of [`GROUP`] at index 7.
    fn live_answer() -> pb::RejoinBarrierResponseV1 {
        let voters = membership(vec![BTreeSet::from([1, 2, 3])]);
        let vote: UrsulaVote = openraft::Vote::new_committed(5, LEADER);
        pb::RejoinBarrierResponseV1 {
            vote: encode_wire(&vote),
            index: 7,
            plane: pb::RaftPlaneV1::Data.into(),
            raft_group_id: GROUP.0,
            memberships: encode_wire(&LeaderMemberships {
                committed: voters.clone(),
                effective: voters,
            }),
        }
    }

    /// Asks a barrier peer that answers `answer` about [`GROUP`], as node 1.
    async fn ask(
        answer: impl Fn() -> Answer + Send + Sync + 'static,
    ) -> Result<BarrierAnswer, RecoveryProbeError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the barrier peer");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("barrier peer address")
        );
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(super::raft_grpc_server(
                    ScriptedBarrier {
                        answer: Arc::new(answer),
                    },
                    &crate::peer_cluster::test_peer_cluster(LEADER),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let transport = Arc::new(CoreRaftTransport::new(
            crate::peer_cluster::test_identity(1),
            crate::peer_cluster::test_peer_cluster_with(
                1,
                crate::PeerDirectory::new([(
                    LEADER,
                    crate::PeerEndpoints::shared(endpoint.as_str()),
                )]),
            ),
        ));
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: GROUP,
        };
        let asked =
            probe_rejoin_vote_barrier(transport, placement, LEADER, Duration::from_secs(5)).await;
        server.abort();
        asked
    }

    /// The control: a live leader's answer about the group asked is taken.
    #[tokio::test]
    async fn a_live_leaders_answer_is_taken() {
        let answer = ask(|| Ok(live_answer()))
            .await
            .expect("a live leader's answer");
        assert_eq!(answer.index, 7);
        assert_eq!(
            answer.memberships.committed,
            membership(vec![BTreeSet::from([1, 2, 3])])
        );
    }

    /// Empty memberships bytes do not decode: the field is required.
    #[tokio::test]
    async fn an_answer_with_empty_memberships_bytes_is_refused() {
        let error = ask(|| {
            Ok(pb::RejoinBarrierResponseV1 {
                memberships: Default::default(),
                ..live_answer()
            })
        })
        .await
        .expect_err("empty memberships are no answer");
        assert!(matches!(error, RecoveryProbeError::Payload(_)), "{error:?}");
    }

    /// A membership without voters is malformed, never an answer that the
    /// asking node is no member.
    #[tokio::test]
    async fn an_answer_whose_membership_has_no_voters_is_refused() {
        let error = ask(|| {
            Ok(pb::RejoinBarrierResponseV1 {
                memberships: encode_wire(&LeaderMemberships {
                    committed: membership(Vec::new()),
                    effective: membership(vec![BTreeSet::from([1, 2, 3])]),
                }),
                ..live_answer()
            })
        })
        .await
        .expect_err("a membership without voters is no answer");
        assert!(
            matches!(
                error,
                RecoveryProbeError::Memberships(MalformedMembership::EmptyVoterConfig {
                    membership: MembershipKind::Committed,
                })
            ),
            "{error:?}"
        );
    }

    /// An answer that echoes another plane or another group than the one
    /// asked about is refused.
    #[tokio::test]
    async fn an_answer_about_another_plane_or_group_is_refused() {
        let meta = i32::from(pb::RaftPlaneV1::Meta);
        let error = ask(move || {
            Ok(pb::RejoinBarrierResponseV1 {
                plane: meta,
                ..live_answer()
            })
        })
        .await
        .expect_err("an answer about the meta plane");
        assert!(
            matches!(
                error,
                RecoveryProbeError::OtherTarget { plane, raft_group_id }
                    if plane == meta && raft_group_id == GROUP.0
            ),
            "{error:?}"
        );
        let error = ask(|| {
            Ok(pb::RejoinBarrierResponseV1 {
                raft_group_id: 4,
                ..live_answer()
            })
        })
        .await
        .expect_err("an answer about group 4");
        assert!(
            matches!(
                error,
                RecoveryProbeError::OtherTarget {
                    plane,
                    raft_group_id: 4,
                } if plane == i32::from(pb::RaftPlaneV1::Data)
            ),
            "{error:?}"
        );
    }

    /// A misdelivery refusal names the node the address serves now, and is
    /// no answer.
    #[tokio::test]
    async fn a_misdelivered_barrier_names_the_node_that_refused_it() {
        let error = ask(|| Err(GrpcRpcError::misdelivered(9, LEADER).into()))
            .await
            .expect_err("node 9 answers nothing for node 2");
        assert!(
            matches!(error, RecoveryProbeError::OtherNode { node: 9 }),
            "{error:?}"
        );
    }
}

#[cfg(all(test, not(madsim)))]
mod transfer_leader_codec_tests {
    use std::sync::Arc;

    use openraft::error::RPCError;
    use openraft::vote::RaftLeaderId;
    use ursula_shard::RaftGroupId;

    use super::CoreRaftTransport;
    use super::GrpcRaftNetwork;
    use crate::codec::encode_wire;
    use crate::types::UrsulaRaftTypeConfig;

    type TransferLeaderResponse = openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig>;

    #[tokio::test]
    async fn transfer_leader_ack_preserves_typed_rejections_and_refuses_an_empty_response() {
        let network = GrpcRaftNetwork::with_transport(
            Arc::new(CoreRaftTransport::default()),
            RaftGroupId(1),
            2,
            "http://127.0.0.1:1",
            8,
        );
        // Every peer encodes its TransferLeaderResponse, so an empty ack is
        // a malformed response, not a success.
        assert!(matches!(
            network.decode_rpc_ack::<TransferLeaderResponse>("TransferLeader", &[]),
            Err(RPCError::Network(_))
        ));
        for response in [
            Ok(()),
            Err(openraft::raft::TransferLeaderError::VoteChanged {
                expected: openraft::Vote::new_committed(1, 1),
                actual: openraft::Vote::new_committed(2, 2),
            }),
            Err(openraft::raft::TransferLeaderError::LogNotFlushed {
                expected: Some(openraft::LogId::new(
                    <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId::new(1, 1),
                    4,
                )),
                actual: None,
            }),
        ] {
            assert_eq!(
                network
                    .decode_rpc_ack::<TransferLeaderResponse>(
                        "TransferLeader",
                        &encode_wire(&response)
                    )
                    .unwrap(),
                response
            );
        }
    }
}
#[cfg(all(test, not(madsim)))]
#[path = "../benches/support/grpc_counters.rs"]
mod counter_bench;

#[cfg(test)]
mod group_rpc_status_tests {
    use tonic::Code;
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::GroupInfraError;
    use ursula_runtime::GroupSnapshotError;

    use super::group_rpc_status;

    /// The gRPC code a peer gets for `error`: the table, an exhaustive match.
    fn expected(error: &GroupEngineError) -> Code {
        match error {
            GroupEngineError::Stream(_) | GroupEngineError::NotLeader(_) => Code::Internal,
            GroupEngineError::Infra(infra) => match infra {
                GroupInfraError::RaftGroupNotRegistered { .. } => Code::NotFound,
                GroupInfraError::RecoveryVoteFloor { .. }
                | GroupInfraError::RecoveryCutting { .. }
                | GroupInfraError::CommittedOverwrite { .. }
                | GroupInfraError::UnrelatedHistory { .. }
                | GroupInfraError::LogBehindCommit { .. }
                | GroupInfraError::RecoveryPlanFenced { .. }
                | GroupInfraError::OwnerStopped
                | GroupInfraError::ApplyStopped { .. }
                | GroupInfraError::EngineWorkOutlivedShutdown { .. }
                | GroupInfraError::PreviousEngineWorkRunning { .. }
                | GroupInfraError::OutcomeUnknown
                | GroupInfraError::Snapshot(
                    GroupSnapshotError::ForeignKey { .. }
                    | GroupSnapshotError::ObjectMissing { .. }
                    | GroupSnapshotError::ObjectCorrupt { .. }
                    | GroupSnapshotError::PointerUnreadable { .. }
                    | GroupSnapshotError::StoreUnavailable { .. },
                ) => Code::Unavailable,
                GroupInfraError::Snapshot(GroupSnapshotError::InvalidContent(_))
                | GroupInfraError::RecoveryNodeMismatch { .. }
                | GroupInfraError::RecoveryAlreadyBound { .. }
                | GroupInfraError::RecoveryGroupMismatch { .. }
                | GroupInfraError::InvalidRaftCommand { .. }
                | GroupInfraError::WalOpen { .. }
                | GroupInfraError::LogBelowSnapshot { .. }
                | GroupInfraError::RecoveryPending { .. }
                | GroupInfraError::ColdStore { .. }
                | GroupInfraError::ColdBackpressure { .. }
                | GroupInfraError::RaftUncommittedBackpressure { .. }
                | GroupInfraError::UnexpectedResponse { .. }
                | GroupInfraError::MissingStreamId { .. }
                | GroupInfraError::Unsupported { .. }
                | GroupInfraError::BootstrapWindowNotHot { .. }
                | GroupInfraError::ColdCompactionStale { .. }
                | GroupInfraError::ForwardFailed { .. }
                | GroupInfraError::ForwardedAnswerInvalid { .. }
                | GroupInfraError::InvalidRaftConfig { .. }
                | GroupInfraError::RecoveryGateOpen { .. }
                | GroupInfraError::MembershipInit { .. }
                | GroupInfraError::LeaderWaitTimedOut { .. } => Code::Internal,
            },
        }
    }

    #[test]
    fn every_sample_answers_its_table_code() {
        for error in ursula_runtime::group_engine_error_samples() {
            let code = expected(&error);
            assert_eq!(group_rpc_status(error.clone()).code(), code, "{error:?}");
        }
    }

    /// #539: a leadership transfer this node refused or could not take
    /// answers a precise code, and none reads as another format epoch's
    /// protocol mismatch.
    #[test]
    fn transfer_refusals_answer_precise_codes_and_never_a_protocol_mismatch() {
        use crate::IncomingTransferError;
        let group = ursula_shard::RaftGroupId(7);
        let cases = [
            (
                IncomingTransferError::Group(GroupEngineError::Infra(
                    GroupInfraError::RaftGroupNotRegistered {
                        raft_group_id: group,
                    },
                )),
                Code::NotFound,
            ),
            (
                IncomingTransferError::Group(GroupEngineError::Infra(
                    GroupInfraError::OwnerStopped,
                )),
                Code::Unavailable,
            ),
            (
                IncomingTransferError::OwnCommittedVote {
                    group,
                    vote: crate::UrsulaVote::new_committed(3, 1),
                },
                Code::FailedPrecondition,
            ),
            (
                IncomingTransferError::MayNotCampaign { group },
                Code::FailedPrecondition,
            ),
            (
                IncomingTransferError::Raft {
                    group,
                    source: openraft::error::Fatal::Stopped,
                },
                Code::Unavailable,
            ),
        ];
        for (error, code) in cases {
            let status = tonic::Status::from(error);
            assert_eq!(status.code(), code, "{status:?}");
            assert!(
                !crate::format_epoch::is_protocol_mismatch(&status),
                "{status:?}"
            );
        }
    }

    /// A recovery barrier this node could not confirm answers a precise
    /// code, and none reads as a protocol mismatch.
    #[test]
    fn barrier_refusals_answer_precise_codes_and_never_a_protocol_mismatch() {
        use crate::QuorumProofError;
        let group = ursula_shard::RaftGroupId(7);
        let cases = [
            (QuorumProofError::NotRegistered { group }, Code::NotFound),
            (
                QuorumProofError::Read {
                    group,
                    source: GroupEngineError::Infra(GroupInfraError::OwnerStopped),
                },
                Code::Unavailable,
            ),
            (
                QuorumProofError::NotLeader { group },
                Code::FailedPrecondition,
            ),
            (
                QuorumProofError::LeadershipChanged { group },
                Code::FailedPrecondition,
            ),
            (
                QuorumProofError::HistoryPending { group },
                Code::FailedPrecondition,
            ),
        ];
        for (error, code) in cases {
            let status = tonic::Status::from(error);
            assert_eq!(status.code(), code, "{status:?}");
            assert!(
                !crate::format_epoch::is_protocol_mismatch(&status),
                "{status:?}"
            );
        }
    }
}
