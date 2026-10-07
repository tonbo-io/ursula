use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::Stream;
use futures_util::StreamExt;
use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
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
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use ursula_runtime::ColdIndexPageCache;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::GroupEngine;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use crate::codec::decode_wire;
use crate::codec::encode_wire;
use crate::codec::placement_from_parts;
use crate::codec::required;
use crate::engine::RaftGroupEngine;
use crate::format_epoch::PROTOCOL_MISMATCH_TEXT;
use crate::format_epoch::observe_outbound_status;
use crate::format_epoch::record_format_epoch_mismatch;
use crate::forward::write_commands_on_raft;
use crate::raft_internal_proto;
use crate::rejoin::GroupRejoin;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
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

pub(crate) static GRPC_LEADER_CHANNELS: OnceLock<Mutex<BTreeMap<String, Channel>>> =
    OnceLock::new();
/// Shared only by groups constructed on one owner core. Connections and
/// encoders are created by that core, never by a process-wide first caller.
#[derive(Debug, Default)]
pub(crate) struct CoreRaftTransport {
    channels: Mutex<BTreeMap<String, SharedRaftChannel>>,
    sessions: Mutex<BTreeMap<String, SharedAppendSession>>,
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
static GRPC_APPEND_STREAM_INFLIGHT: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_INFLIGHT_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_QUEUED_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_QUEUED_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_BACKPRESSURE_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_EXPIRED_UNSENT: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_STALLS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_HEARTBEAT_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_HEARTBEAT_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_REPLICATION_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_REPLICATION_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_REPLICATION_ENTRIES: AtomicU64 = AtomicU64::new(0);
static GRPC_APPEND_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_VOTE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_VOTE_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_VOTE_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_PAYLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
static GRPC_SNAPSHOT_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
use crate::registry::RaftGroupHandleRegistry;

const APPEND_STREAM_BACKLOG_FULL: &str = "raft append stream backlog full";
pub(crate) const REJOIN_BARRIER_CAPABILITY: &str = "ursula-rejoin-barrier";

pub(crate) type RaftClient = raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>;

pub use ursula_proto::admin::QuorumPrefix;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecoveryProbeError {
    #[error("recovery transport: {0}")]
    Transport(#[from] RPCError<UrsulaRaftTypeConfig>),
    #[error("recovery RPC: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("recovery payload: {0}")]
    Payload(#[from] ursula_runtime::GroupEngineError),
    #[error("recovery peer does not report itself as committed leader")]
    NotLeader,
    #[error("recovery peer changed its vote across the ReadIndex proof")]
    LeadershipChanged,
    #[error("recovery peer has no log")]
    MissingLog,
    #[error("recovery HEAD unexpectedly found an empty-named stream")]
    UnexpectedHead,
    #[error("recovery HEAD did not confirm leadership: {0}")]
    HeadRejected(#[source] ursula_runtime::GroupEngineError),
}

/// Confirm a group's current quorum without issuing an application write.
/// This is a point-in-time observation, not a maintenance reservation or a
/// promise that another participant cannot disrupt a voter immediately after.
#[cfg(test)]
pub(crate) async fn confirm_quorum_prefix(
    placement: ursula_shard::ShardPlacement,
    leader_id: u64,
    address: &str,
    timeout: Duration,
) -> Result<QuorumPrefix, RecoveryProbeError> {
    let (vote, index) =
        probe_rejoin_vote_barrier(placement, leader_id, leader_id, address, timeout).await?;
    Ok(QuorumPrefix {
        raft_group_id: placement.raft_group_id.0,
        leader_id,
        leader_term: vote.leader_id().term(),
        required_applied_index: index,
    })
}

/// Fresh recovery evidence. Negotiate the explicit barrier RPC through the
/// existing Vote response before calling it: an unknown RPC on a 0.6.2
/// HTTP/gRPC mux falls through to the HTTP append route. For a 0.6.2
/// leader during rolling upgrade, a linearizable HEAD of an impossible HTTP
/// name confirms leadership before validating the name. A subsequent low-term
/// vote probe supplies a conservative catch-up bound (the leader's last log,
/// which includes its confirmed read index). Reject a peer reporting another
/// leader: its HEAD may have been forwarded and its own prefix may be behind.
///
/// ReadIndexBarrier only coalesces rounds whose confirmation has not started;
/// an inbound request never joins an already-started confirmation round.
pub(crate) async fn probe_rejoin_vote_barrier(
    placement: ursula_shard::ShardPlacement,
    node_id: u64,
    leader_id: u64,
    address: &str,
    timeout: Duration,
) -> Result<(UrsulaVote, u64), RecoveryProbeError> {
    let mut network = GrpcRaftNetwork::new(placement.raft_group_id, leader_id, address);
    let mut client = network.client()?;
    let envelope = network.vote_envelope(crate::rejoin::bootstrap_probe_vote(node_id));
    GRPC_VOTE_REQUESTS.fetch_add(1, Ordering::Relaxed);
    GRPC_VOTE_REQUEST_BYTES.fetch_add(envelope.encoded_len() as u64, Ordering::Relaxed);
    let mut capability_request = tonic::Request::new(envelope);
    capability_request.set_timeout(timeout);
    let capability_response = client.vote(capability_request).await?;
    let explicit_barrier = capability_response
        .metadata()
        .get(REJOIN_BARRIER_CAPABILITY)
        .is_some_and(|value| value == "1");
    let ack = capability_response.into_inner();
    GRPC_VOTE_RESPONSE_BYTES.fetch_add(ack.encoded_len() as u64, Ordering::Relaxed);
    let response: UrsulaVoteResponse = decode_wire(&ack.payload, "rejoin capability vote")?;
    if !response.vote.is_committed() || *response.vote.leader_id().node_id() != leader_id {
        return Err(RecoveryProbeError::NotLeader);
    }
    let observed_vote = response.vote;
    // Capability metadata is only a routing hint, never fresh quorum or
    // catch-up evidence. Do not cache it across peer replacements.
    if explicit_barrier {
        let mut barrier_request =
            tonic::Request::new(raft_internal_proto::RejoinBarrierRequestV1 {
                raft_group_id: placement.raft_group_id.0,
                protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            });
        barrier_request.set_timeout(timeout);
        match client.rejoin_barrier(barrier_request).await {
            Ok(response) => {
                let response = response.into_inner();
                let vote: UrsulaVote = decode_wire(&response.vote, "rejoin barrier vote")?;
                if !vote.is_committed() || *vote.leader_id().node_id() != leader_id {
                    return Err(RecoveryProbeError::NotLeader);
                }
                if vote != observed_vote {
                    return Err(RecoveryProbeError::LeadershipChanged);
                }
                return Ok((vote, response.index));
            }
            Err(status) if status.code() == tonic::Code::Unimplemented => {}
            Err(status) => return Err(status.into()),
        }
    }
    let mut request = tonic::Request::new(raft_internal_proto::GroupReadRequestV1 {
        raft_group_id: placement.raft_group_id.0,
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        // Neither empty name can be created through the HTTP API. This probe
        // performs no application write or TTL renewal.
        bucket_id: String::new(),
        stream_id: String::new(),
        now_ms: 0,
        read: Some(raft_internal_proto::group_read_request_v1::Read::Head(
            raft_internal_proto::HeadStreamReadV1 {
                applied_state_only: false,
            },
        )),
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
    });
    request.set_timeout(timeout);
    let response = client.group_read(request).await?.into_inner();
    if response.ok {
        return Err(RecoveryProbeError::UnexpectedHead);
    }
    let error: ursula_runtime::GroupEngineError =
        decode_wire(&response.payload, "rejoin HEAD error")?;
    if !matches!(
        error.code(),
        Some(ursula_stream::StreamErrorCode::InvalidBucketId)
    ) {
        return Err(RecoveryProbeError::HeadRejected(error));
    }
    let response = network
        .vote(
            crate::rejoin::bootstrap_probe_vote(node_id),
            RPCOption::new(timeout),
        )
        .await?;
    if !response.vote.is_committed() || *response.vote.leader_id().node_id() != leader_id {
        return Err(RecoveryProbeError::NotLeader);
    }
    if response.vote != observed_vote {
        return Err(RecoveryProbeError::LeadershipChanged);
    }
    Ok((
        response.vote,
        response
            .last_log_id
            .ok_or(RecoveryProbeError::MissingLog)?
            .index(),
    ))
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
    let charge = encoded_len.clamp(RAFT_GRPC_APPEND_STREAM_MIN_CHARGE_BYTES, budget);
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
        raft_grpc_append_heartbeat_requests: GRPC_APPEND_HEARTBEAT_REQUESTS.load(Ordering::Relaxed),
        raft_grpc_append_heartbeat_request_bytes: GRPC_APPEND_HEARTBEAT_REQUEST_BYTES
            .load(Ordering::Relaxed),
        raft_grpc_append_replication_requests: GRPC_APPEND_REPLICATION_REQUESTS
            .load(Ordering::Relaxed),
        raft_grpc_append_replication_request_bytes: GRPC_APPEND_REPLICATION_REQUEST_BYTES
            .load(Ordering::Relaxed),
        raft_grpc_append_replication_entries: GRPC_APPEND_REPLICATION_ENTRIES
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
        GRPC_APPEND_HEARTBEAT_REQUESTS.fetch_add(1, Ordering::Relaxed);
        GRPC_APPEND_HEARTBEAT_REQUEST_BYTES.fetch_add(sample.request_bytes, Ordering::Relaxed);
        return;
    }
    GRPC_APPEND_REPLICATION_REQUESTS.fetch_add(1, Ordering::Relaxed);
    GRPC_APPEND_REPLICATION_REQUEST_BYTES.fetch_add(sample.request_bytes, Ordering::Relaxed);
    GRPC_APPEND_REPLICATION_ENTRIES.fetch_add(sample.entries, Ordering::Relaxed);
}

pub const RAFT_GRPC_APPEND_PATH: &str = "/ursula.raft.v1.RaftInternal/Append";
pub const RAFT_GRPC_APPEND_STREAM_PATH: &str = "/ursula.raft.v1.RaftInternal/AppendStream";
pub const RAFT_GRPC_VOTE_PATH: &str = "/ursula.raft.v1.RaftInternal/Vote";
pub const RAFT_GRPC_FULL_SNAPSHOT_PATH: &str = "/ursula.raft.v1.RaftInternal/FullSnapshot";
pub const RAFT_GRPC_GROUP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupWrite";
pub const RAFT_GRPC_GROUP_READ_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupRead";
pub const RAFT_GRPC_REJOIN_BARRIER_PATH: &str = "/ursula.raft.v1.RaftInternal/RejoinBarrier";
pub const RAFT_GRPC_TRANSFER_LEADER_PATH: &str = "/ursula.raft.v1.RaftInternal/TransferLeader";
pub const RAFT_GRPC_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
/// The Raft gRPC protocol version is the format epoch
/// (`ursula_stream::RAFT_GRPC_PROTOCOL_VERSION`): Ursula 0.5.x speaks
/// protocol 1 and 0.6.x protocol 2, each refuses this one with its own check,
/// and this binary refuses theirs. Epoch 3 kept every 0.6 message and added
/// `RejoinBarrier`. The version keeps nodes of different epochs out of one
/// cluster.
pub(crate) const RAFT_GRPC_PROTOCOL_VERSION: u32 = ursula_stream::RAFT_GRPC_PROTOCOL_VERSION;
const RAFT_GRPC_APPEND_STREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const RAFT_GRPC_APPEND_STREAM_MAX_BATCH_ITEMS: usize = 32;
/// Leader side, per peer: bytes of Append calls queued (not yet taken by the HTTP/2 encoder).
/// Beyond it a call fails fast as `Unavailable`, which OpenRaft treats as an unreachable peer and
/// backs off on. Before this bound a lagging peer's queue grew without limit: every replication
/// retry after a 250 ms RPC timeout queued another copy of the same entries while the timed-out
/// copies were still sent (EKS, 128 SQLite-VFS owners: ~0.7 GB/min per node to OOM).
const RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;
/// Follower side, per inbound stream: decoded Append frames held before they are answered.
const RAFT_GRPC_APPEND_STREAM_SERVER_MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
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
}

impl GrpcRpcError {
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::InvalidArgument,
            message: message.into(),
        }
    }

    pub(crate) fn failed_precondition(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::FailedPrecondition,
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::NotFound,
            message: message.into(),
        }
    }
}

impl From<GrpcRpcError> for tonic::Status {
    fn from(error: GrpcRpcError) -> Self {
        tonic::Status::new(error.code, error.message)
    }
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

pub fn raft_grpc_service(
    registry: RaftGroupHandleRegistry,
) -> raft_internal_proto::raft_internal_server::RaftInternalServer<RaftGrpcService> {
    raft_internal_proto::raft_internal_server::RaftInternalServer::new(RaftGrpcService::new(
        registry,
    ))
    .accept_compressed(CompressionEncoding::Zstd)
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

async fn handle_append_envelope(
    registry: RaftGroupHandleRegistry,
    envelope: raft_internal_proto::RaftRpcEnvelopeV1,
) -> Result<raft_internal_proto::RaftRpcAckV1, tonic::Status> {
    let raft_group_id =
        validate_raft_rpc_preamble(&registry, envelope.protocol_version, envelope.raft_group_id)?;
    let request: UrsulaAppendEntriesRequest =
        decode_rpc_payload(&envelope.payload, "raft append request")?;
    let response = registry
        .append_entries(raft_group_id, request)
        .await
        .map_err(|err| tonic::Status::internal(err.to_string()))?;
    Ok(raft_internal_proto::RaftRpcAckV1 {
        payload: encode_wire(&response),
    })
}

async fn handle_append_stream_item(
    registry: RaftGroupHandleRegistry,
    concurrency: Arc<Semaphore>,
    item: raft_internal_proto::RaftAppendStreamRequestItem,
) -> raft_internal_proto::RaftAppendStreamResponseItem {
    let request_id = item.request_id;
    let Ok(_permit) = concurrency.acquire_owned().await else {
        return raft_internal_proto::RaftAppendStreamResponseItem {
            request_id,
            result: Some(
                raft_internal_proto::raft_append_stream_response_item::Result::Error(
                    raft_internal_proto::RaftAppendStreamError {
                        code: tonic::Code::Unavailable as i32,
                        message: "append stream concurrency guard is closed".to_owned(),
                    },
                ),
            ),
        };
    };
    let result = match item.envelope {
        Some(envelope) => match handle_append_envelope(registry, envelope).await {
            Ok(ack) => {
                Some(raft_internal_proto::raft_append_stream_response_item::Result::Ack(ack))
            }
            Err(status) => Some(
                raft_internal_proto::raft_append_stream_response_item::Result::Error(
                    raft_internal_proto::RaftAppendStreamError {
                        code: status.code() as i32,
                        message: status.message().to_owned(),
                    },
                ),
            ),
        },
        None => Some(
            raft_internal_proto::raft_append_stream_response_item::Result::Error(
                raft_internal_proto::RaftAppendStreamError {
                    code: tonic::Code::InvalidArgument as i32,
                    message: "append stream batch item is missing its envelope".to_owned(),
                },
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
        let ack = handle_append_envelope(self.registry.clone(), request.into_inner()).await?;
        Ok(tonic::Response::new(ack))
    }

    async fn append_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft_internal_proto::RaftAppendStreamRequest>>,
    ) -> Result<tonic::Response<Self::AppendStreamStream>, tonic::Status> {
        let registry = self.registry.clone();
        let concurrency = Arc::new(Semaphore::new(64));
        // Decoded frames waiting for (or in) processing hold a share of this budget; the next
        // frame is not read off the stream until it fits, so a slow follower pushes back through
        // HTTP/2 flow control instead of buffering the leader's backlog in memory.
        let budget = Arc::new(Semaphore::new(
            RAFT_GRPC_APPEND_STREAM_SERVER_MAX_BUFFERED_BYTES,
        ));
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
                        let charge = append_budget_charge(
                            frame.encoded_len(),
                            RAFT_GRPC_APPEND_STREAM_SERVER_MAX_BUFFERED_BYTES,
                        );
                        let permit = tokio::select! {
                            biased;
                            _ = shutdown.changed() => return None,
                            permit = budget.clone().acquire_many_owned(charge) => permit.ok()?,
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
                            handle_append_stream_item(registry.clone(), concurrency.clone(), item)
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
        let raft_group_id = validate_raft_rpc_preamble(
            &self.registry,
            envelope.protocol_version,
            envelope.raft_group_id,
        )?;
        let request: UrsulaVoteRequest =
            decode_rpc_payload(&envelope.payload, "raft vote request")?;
        let response = self
            .registry
            .vote(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        let mut response = tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: encode_wire(&response),
        });
        response.metadata_mut().insert(
            REJOIN_BARRIER_CAPABILITY,
            tonic::metadata::MetadataValue::from_static("1"),
        );
        Ok(response)
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<raft_internal_proto::RaftFullSnapshotRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftFullSnapshotAckV1>, tonic::Status> {
        let request = request.into_inner();
        let raft_group_id = validate_raft_rpc_preamble(
            &self.registry,
            request.protocol_version,
            request.raft_group_id,
        )?;
        let vote: VoteOf<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.vote, "full snapshot vote")?;
        let meta: SnapshotMetaOf<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.snapshot_meta, "full snapshot meta")?;
        let snapshot = SnapshotOf::<UrsulaRaftTypeConfig> {
            meta,
            snapshot: Cursor::new(request.snapshot_payload.to_vec()),
        };
        let response = self
            .registry
            .install_full_snapshot(raft_group_id, vote, snapshot)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
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
            let placement = placement_from_parts(
                request.core_id,
                request.shard_id,
                request.raft_group_id,
                "group_write_request",
            )
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
            let raft = self
                .registry
                .get(placement.raft_group_id)
                .ok_or_else(|| tonic::Status::not_found("raft group is not registered"))?;
            let commands = request
                .command_payloads
                .into_iter()
                .map(|payload| decode_wire(&payload, "group command"))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
            let results = raft
                .call(move |raft| async move { write_commands_on_raft(raft, commands).await })
                .await
                .map_err(|err| tonic::Status::unavailable(err.to_string()))?
                .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
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
        let group = validate_raft_rpc_preamble(
            &self.registry,
            request.protocol_version,
            request.raft_group_id,
        )?;
        let (vote, index) = self
            .registry
            .confirm_recovery_barrier(group)
            .await
            .map_err(|error| match error {
                crate::QuorumProofError::NotRegistered { .. } => {
                    tonic::Status::not_found(error.to_string())
                }
                crate::QuorumProofError::Read { .. } => {
                    tonic::Status::unavailable(error.to_string())
                }
                _ => tonic::Status::failed_precondition(error.to_string()),
            })?;
        Ok(tonic::Response::new(
            raft_internal_proto::RejoinBarrierResponseV1 {
                vote: encode_wire(&vote),
                index,
            },
        ))
    }

    async fn transfer_leader(
        &self,
        request: tonic::Request<raft_internal_proto::RaftTransferLeaderRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftTransferLeaderAckV1>, tonic::Status> {
        let request = request.into_inner();
        let raft_group_id = validate_raft_rpc_preamble(
            &self.registry,
            request.protocol_version,
            request.raft_group_id,
        )?;
        let openraft_request: TransferLeaderRequest<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.request, "transfer leader request")?;
        self.registry
            .handle_transfer_leader(raft_group_id, openraft_request)
            .await
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        Ok(tonic::Response::new(
            raft_internal_proto::RaftTransferLeaderAckV1 {},
        ))
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
            let placement = placement_from_parts(
                request.core_id,
                request.shard_id,
                request.raft_group_id,
                "group_read_request",
            )
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
            let raft = self
                .registry
                .get(placement.raft_group_id)
                .ok_or_else(|| tonic::Status::not_found("raft group is not registered"))?;
            // The group's barrier, so forwarded linearizable reads share
            // confirmation rounds with its local reads. The factories register
            // it before the raft handle.
            let read_barrier = self
                .registry
                .read_barrier(placement.raft_group_id)
                .ok_or_else(|| tonic::Status::not_found("raft group is not registered"))?;
            let cold_store = self.cold_store.clone();
            let cold_index_cache = self
                .registry
                .cold_index_cache(placement.raft_group_id)
                .or_else(|| {
                    cold_store.as_ref().map(|cold_store| {
                        Arc::new(ColdIndexPageCache::new(
                            Arc::new(ColdStoreColdIndexPageStore::new(cold_store.clone())),
                            1024,
                        ))
                    })
                });
            let result = raft
                .call(move |raft| async move {
                    let mut engine = RaftGroupEngine {
                        recovery_tasks: crate::rejoin::RecoveryGate::default(),
                        raft,
                        placement,
                        read_barrier,
                        cold_store,
                        // The group's shared page cache (bounded-state F13), which
                        // apply-time invalidation reaches; a request-scoped cache only
                        // when none was registered.
                        cold_index_cache,
                    };
                    let stream_id = BucketStreamId::new(request.bucket_id, request.stream_id);
                    let result = match required(request.read, "group_read.read")
                        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?
                    {
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
                .map_err(|err| tonic::Status::unavailable(err.to_string()))??;
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
/// linearized on the leader.
/// `Err` names the invalid field.
pub(crate) fn read_stream_request_from_v1(
    stream_id: BucketStreamId,
    now_ms: u64,
    read: raft_internal_proto::ReadStreamReadV1,
) -> Result<ReadStreamRequest, &'static str> {
    let max_len = usize::try_from(read.max_len)
        .map_err(|_overflow| "group_read.read_stream.max_len too large")?;
    Ok(ReadStreamRequest {
        stream_id,
        offset: read.offset,
        max_len,
        now_ms,
        leader_only: read.leader_only,
        read_index: None,
    })
}

/// Decode a MessagePack RPC payload carried inside a proto envelope, mapping
/// failures to `InvalidArgument`.
fn decode_rpc_payload<T: DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T, GrpcRpcError> {
    decode_wire(bytes, what).map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))
}

/// Validate the shared protocol-version + registered-group preamble carried by
/// every inbound raft RPC (envelope, snapshot, and transfer-leader requests).
pub(crate) fn validate_raft_rpc_preamble(
    registry: &RaftGroupHandleRegistry,
    protocol_version: u32,
    raft_group_id: u32,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_grpc_metadata(protocol_version)?;
    let raft_group_id = RaftGroupId(raft_group_id);
    if !registry.contains_group(raft_group_id) {
        return Err(GrpcRpcError::not_found(format!(
            "raft group {} is not registered on this node",
            raft_group_id.0
        )));
    }
    Ok(raft_group_id)
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
    rejoin: Option<Arc<GroupRejoin>>,
}

impl GrpcRaftNetworkFactory {
    pub fn new(raft_group_id: RaftGroupId) -> Self {
        Self {
            transport: Arc::default(),
            raft_group_id,
            reconnect_threshold: 8,
            rejoin: None,
        }
    }

    pub(crate) fn with_transport(mut self, transport: Arc<CoreRaftTransport>) -> Self {
        self.transport = transport;
        self
    }

    pub fn with_reconnect_threshold(mut self, threshold: u32) -> Self {
        self.reconnect_threshold = threshold;
        self
    }

    /// This node's recovery gate for the group: replication answers that
    /// show a follower lost its log go to it instead of to OpenRaft.
    pub fn with_rejoin(mut self, rejoin: Option<Arc<GroupRejoin>>) -> Self {
        self.rejoin = rejoin;
        self
    }
}

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let mut network = GrpcRaftNetwork::with_transport(
            self.transport.clone(),
            self.raft_group_id,
            target,
            node.addr.clone(),
            self.reconnect_threshold,
        );
        network.rejoin = self.rejoin.clone();
        network
    }
}

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    transport: Arc<CoreRaftTransport>,
    raft_group_id: RaftGroupId,
    target: u64,
    endpoint: String,
    client: Result<RaftClient, String>,
    channel_generation: u64,
    /// Streak of consecutive RPC failures on this channel. Reset to 0 on the
    /// next successful RPC. When it crosses `reconnect_threshold` we replace
    /// the owner-core HTTP/2 channel generation — tonic's `connect_lazy`
    /// keeps a stuck channel forever otherwise (the TCP socket stays open, the
    /// HTTP/2 streams stay borked, no auto-heal).
    consecutive_failures: u32,
    reconnect_threshold: u32,
    rejoin: Option<Arc<GroupRejoin>>,
}

impl Debug for GrpcRaftNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRaftNetwork")
            .field("raft_group_id", &self.raft_group_id)
            .field("target", &self.target)
            .field("endpoint", &self.endpoint)
            .field("channel_generation", &self.channel_generation)
            .field("consecutive_failures", &self.consecutive_failures)
            .field("reconnect_threshold", &self.reconnect_threshold)
            .finish()
    }
}

impl GrpcRaftNetwork {
    pub fn new(raft_group_id: RaftGroupId, target: u64, address: impl Into<String>) -> Self {
        Self::with_threshold(raft_group_id, target, address, 8)
    }

    pub fn with_threshold(
        raft_group_id: RaftGroupId,
        target: u64,
        address: impl Into<String>,
        reconnect_threshold: u32,
    ) -> Self {
        Self::with_transport(
            Arc::default(),
            raft_group_id,
            target,
            address,
            reconnect_threshold,
        )
    }

    fn with_transport(
        transport: Arc<CoreRaftTransport>,
        raft_group_id: RaftGroupId,
        target: u64,
        address: impl Into<String>,
        reconnect_threshold: u32,
    ) -> Self {
        let endpoint = normalize_grpc_endpoint(address.into());
        let (client, channel_generation) = shared_raft_client(&transport, &endpoint, None);
        Self {
            raft_group_id,
            target,
            endpoint,
            transport,
            client,
            channel_generation,
            consecutive_failures: 0,
            reconnect_threshold,
            rejoin: None,
        }
    }

    pub(crate) fn client(&self) -> Result<RaftClient, RPCError<UrsulaRaftTypeConfig>> {
        self.client
            .clone()
            .map_err(|err| RPCError::Unreachable(Unreachable::from_string(err)))
    }

    /// Increment the failure streak. If we cross the threshold, drop the
    /// stuck shared channel and build a fresh one — the next RPC call gets a
    /// new HTTP/2 connection. If another group already rebuilt this endpoint,
    /// adopt that newer generation instead of creating another connection.
    /// We also reset the counter so the replacement channel gets a full grace
    /// period before any further rebuild.
    fn note_failure(&mut self, route: &str) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= self.reconnect_threshold {
            tracing::warn!(
                "raft-grpc: rebuilding channel to node {} ({}) after {} consecutive {} failures",
                self.target,
                self.endpoint,
                self.consecutive_failures,
                route,
            );
            let (client, generation) = shared_raft_client(
                &self.transport,
                &self.endpoint,
                Some(self.channel_generation),
            );
            self.client = client;
            self.channel_generation = generation;
            self.consecutive_failures = 0;
        }
    }

    fn note_success(&mut self) {
        self.consecutive_failures = 0;
    }

    pub(crate) fn append_envelope(
        &self,
        request: &UrsulaAppendEntriesRequest,
    ) -> raft_internal_proto::RaftRpcEnvelopeV1 {
        raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: encode_wire(request),
        }
    }

    pub(crate) fn transfer_leader_envelope(
        &self,
        request: &TransferLeaderRequest<UrsulaRaftTypeConfig>,
    ) -> raft_internal_proto::RaftTransferLeaderRequestV1 {
        raft_internal_proto::RaftTransferLeaderRequestV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            request: encode_wire(request),
        }
    }

    pub(crate) fn vote_envelope(
        &self,
        request: UrsulaVoteRequest,
    ) -> raft_internal_proto::RaftRpcEnvelopeV1 {
        raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: encode_wire(&request),
        }
    }

    pub(crate) fn apply_rpc_timeout<T>(&self, request: &mut tonic::Request<T>, option: RPCOption) {
        request.set_timeout(option.hard_ttl());
        // Note: trace context is intentionally NOT injected here. These are
        // OpenRaft consensus RPCs (append_entries/vote/snapshot) driven by the
        // replication loop, decoupled from any client request, so there is no
        // request span to propagate. Request-synchronous leader forwarding
        // injects its own context (see `crate::forward`).
    }

    pub(crate) fn map_tonic_status(
        &self,
        route: &str,
        status: tonic::Status,
    ) -> RPCError<UrsulaRaftTypeConfig> {
        observe_outbound_status(route, &status);
        let message = format!(
            "{route} to node {} at {} failed: {}",
            self.target, self.endpoint, status
        );
        match status.code() {
            tonic::Code::Unavailable | tonic::Code::Cancelled => {
                RPCError::Unreachable(Unreachable::from_string(message))
            }
            _ => raft_rpc_network_error(message),
        }
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
                let mapped = self.map_tonic_status(route, err);
                self.note_failure(route);
                Err(mapped)
            }
        }
    }

    async fn try_append_stream(
        &self,
        envelope: raft_internal_proto::RaftRpcEnvelopeV1,
        option: RPCOption,
    ) -> Result<raft_internal_proto::RaftRpcAckV1, tonic::Status> {
        let client = self
            .client()
            .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
        let session = shared_append_session(&self.transport, &self.endpoint, client)
            .map_err(tonic::Status::unavailable)?;
        let charge = append_budget_charge(
            envelope.encoded_len(),
            RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES,
        );
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
            return Err(tonic::Status::unavailable(format!(
                "{APPEND_STREAM_BACKLOG_FULL}: {} of {} bytes queued",
                RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES
                    .saturating_sub(session.budget.available_permits()),
                RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES,
            )));
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
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(tonic::Status::unavailable(
                "raft append stream closed without a response",
            )),
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "raft append stream exceeded the OpenRaft hard TTL",
            )),
        }
    }

    async fn append_rpc(
        &mut self,
        envelope: raft_internal_proto::RaftRpcEnvelopeV1,
        option: RPCOption,
    ) -> Result<raft_internal_proto::RaftRpcAckV1, RPCError<UrsulaRaftTypeConfig>> {
        match self.try_append_stream(envelope, option).await {
            Ok(ack) => {
                self.note_success();
                Ok(ack)
            }
            Err(status) => {
                // A full queue is congestion on a live stream, not a broken channel: rebuilding
                // the channel would not drain it.
                let backpressure = status.code() == tonic::Code::Unavailable
                    && status.message().starts_with(APPEND_STREAM_BACKLOG_FULL);
                let mapped = self.map_tonic_status("AppendStream", status);
                if !backpressure {
                    self.note_failure("AppendStream");
                }
                Err(mapped)
            }
        }
    }

    /// Decode the MessagePack payload of an envelope-style ack.
    fn decode_rpc_ack<T: DeserializeOwned>(
        &self,
        route: &str,
        payload: &[u8],
    ) -> Result<T, RPCError<UrsulaRaftTypeConfig>> {
        decode_wire(payload, route).map_err(|err| {
            raft_rpc_network_error(format!(
                "decode {route} response from node {} at {}: {err}",
                self.target, self.endpoint
            ))
        })
    }
}

fn raft_client(channel: Channel) -> RaftClient {
    RaftClient::new(channel)
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
) -> (Result<RaftClient, String>, u64) {
    let parsed = match Endpoint::from_shared(endpoint.to_owned()) {
        Ok(parsed) => parsed,
        Err(err) => {
            return (
                Err(format!("invalid raft gRPC endpoint {endpoint}: {err}")),
                0,
            );
        }
    };
    let channels = &transport.channels;
    let mut channels = match channels.lock() {
        Ok(channels) => channels,
        Err(err) => {
            return (
                Err(format!(
                    "raft gRPC channel pool lock poisoned for {endpoint}: {err}"
                )),
                0,
            );
        }
    };
    if let Some(shared) = channels.get(endpoint)
        && observed_generation.is_none_or(|generation| generation != shared.generation)
    {
        return (Ok(raft_client(shared.channel.clone())), shared.generation);
    }
    let generation = channels
        .get(endpoint)
        .map_or(1, |shared| shared.generation.saturating_add(1));
    let channel = parsed.connect_lazy();
    channels.insert(endpoint.to_owned(), SharedRaftChannel {
        generation,
        channel: channel.clone(),
    });
    (Ok(raft_client(channel)), generation)
}

/// Return the one healthy Append stream for this peer endpoint.
///
/// The stream lifetime is intentionally independent of the unary channel
/// generation. OpenRaft keeps one network object per group, so after any
/// channel rebuild those objects temporarily observe different generations.
/// Keying the stream by each object's generation makes them replace one
/// another on nearly every Append call. A live stream is already proof that
/// its underlying channel works; replace it only after its sender closes.
fn shared_append_session(
    transport: &CoreRaftTransport,
    endpoint: &str,
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
    let task = tokio::spawn(run_append_session(client, receiver));
    let session = SharedAppendSession {
        _task: Arc::new(AppendSessionTask::Running(task)),
        sender,
        budget: Arc::new(Semaphore::new(RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES)),
    };
    sessions.insert(endpoint.to_owned(), session.clone());
    Ok(session)
}

/// Collect `first` and the calls already queued behind it (up to the frame limit) into one
/// frame, dropping calls whose caller has already given up: their response would go nowhere, and
/// sending them anyway is what turned a lagging peer into an ever-growing backlog (each OpenRaft
/// retry re-queues the same entries).
fn collect_append_stream_frame(
    first: AppendStreamCall,
    calls: &mut mpsc::UnboundedReceiver<AppendStreamCall>,
) -> (Vec<AppendStreamCall>, bool) {
    fn keep(call: AppendStreamCall, frame: &mut Vec<AppendStreamCall>) {
        if call.response.is_closed() {
            GRPC_APPEND_STREAM_EXPIRED_UNSENT.fetch_add(1, Ordering::Relaxed);
        } else {
            frame.push(call);
        }
    }
    let mut frame = Vec::new();
    keep(first, &mut frame);
    while frame.len() < RAFT_GRPC_APPEND_STREAM_MAX_BATCH_ITEMS {
        match calls.try_recv() {
            Ok(call) => keep(call, &mut frame),
            Err(mpsc::error::TryRecvError::Empty) => return (frame, true),
            Err(mpsc::error::TryRecvError::Disconnected) => return (frame, false),
        }
    }
    (frame, true)
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

async fn run_append_session(
    mut client: RaftClient,
    mut calls: mpsc::UnboundedReceiver<AppendStreamCall>,
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
    let response = tokio::time::timeout(
        RAFT_GRPC_APPEND_STREAM_CONNECT_TIMEOUT,
        client.append_stream(tonic::Request::new(outbound)),
    )
    .await;
    let mut responses = match response {
        Ok(Ok(response)) => {
            GRPC_APPEND_STREAM_SESSIONS_OPENED.fetch_add(1, Ordering::Relaxed);
            response.into_inner()
        }
        Ok(Err(_status)) => {
            GRPC_APPEND_STREAM_SESSION_FAILURES.fetch_add(1, Ordering::Relaxed);
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
                                    Err(tonic::Status::new(
                                        tonic::Code::from_i32(error.code),
                                        error.message,
                                    ))
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
                let (frame_calls, receiver_open) =
                    collect_append_stream_frame(call, &mut calls);
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
                    GRPC_APPEND_STREAM_REQUEST_BYTES.fetch_add(
                        call.envelope.encoded_len() as u64,
                        Ordering::Relaxed,
                    );
                    pending.insert(request_id, call.response);
                    queued.push(call.queued);
                    items.push(raft_internal_proto::RaftAppendStreamRequestItem {
                        request_id,
                        envelope: Some(call.envelope),
                    });
                }
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
    async fn append_entries(
        &mut self,
        rpc: UrsulaAppendEntriesRequest,
        option: RPCOption,
    ) -> Result<UrsulaAppendEntriesResponse, RPCError<UrsulaRaftTypeConfig>> {
        let envelope = self.append_envelope(&rpc);
        record_append_logical_sample(append_logical_sample(&rpc, envelope.encoded_len()));
        let ack = self.append_rpc(envelope, option).await?;
        GRPC_APPEND_RESPONSE_BYTES.fetch_add(ack.encoded_len() as u64, Ordering::Relaxed);
        let response: UrsulaAppendEntriesResponse = self.decode_rpc_ack("Append", &ack.payload)?;
        if let Some(rejoin) = &self.rejoin
            && rejoin.follower_lost_log(
                self.target,
                &rpc.vote,
                rpc.prev_log_id.as_ref(),
                rpc.entries.last().map(|entry| &entry.log_id),
                &response,
            )
        {
            return Err(raft_rpc_network_error(format!(
                "node {} at {} lost Raft log entries it had acknowledged",
                self.target, self.endpoint
            )));
        }
        Ok(response)
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
        snapshot: SnapshotOf<UrsulaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        let request = raft_internal_proto::RaftFullSnapshotRequestV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            vote: encode_wire(&vote),
            snapshot_meta: encode_wire(&snapshot.meta),
            snapshot_payload: snapshot.snapshot.into_inner().into(),
        };
        GRPC_SNAPSHOT_REQUESTS.fetch_add(1, Ordering::Relaxed);
        GRPC_SNAPSHOT_REQUEST_BYTES.fetch_add(request.encoded_len() as u64, Ordering::Relaxed);
        GRPC_SNAPSHOT_PAYLOAD_BYTES
            .fetch_add(request.snapshot_payload.len() as u64, Ordering::Relaxed);
        let ack = self
            .call(
                "FullSnapshot",
                request,
                option,
                |mut client, request| async move { client.full_snapshot(request).await },
            )
            .await
            .map_err(StreamingError::from)?;
        GRPC_SNAPSHOT_RESPONSE_BYTES.fetch_add(ack.encoded_len() as u64, Ordering::Relaxed);
        self.decode_rpc_ack("FullSnapshot", &ack.response)
            .map_err(StreamingError::from)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<UrsulaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<(), RPCError<UrsulaRaftTypeConfig>> {
        let envelope = self.transfer_leader_envelope(&req);
        self.call(
            "TransferLeader",
            envelope,
            option,
            |mut client, request| async move { client.transfer_leader(request).await },
        )
        .await
        .map(|_ack| ())
    }
}

#[cfg(test)]
mod reconnect_tests {
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
    use ursula_runtime::GroupWriteCommand;
    use ursula_stream::StreamCommand;

    use super::*;

    #[tokio::test]
    async fn different_owner_pools_never_share_peer_sessions_or_channels() {
        let a = CoreRaftTransport::default();
        let b = CoreRaftTransport::default();
        let endpoint = "http://127.0.0.1:9";
        let (client_a, _) = shared_raft_client(&a, endpoint, None);
        let (client_b, _) = shared_raft_client(&b, endpoint, None);
        let first = shared_append_session(&a, endpoint, client_a.clone().unwrap()).unwrap();
        let same = shared_append_session(&a, endpoint, client_a.unwrap()).unwrap();
        let other = shared_append_session(&b, endpoint, client_b.unwrap()).unwrap();
        assert!(first.sender.same_channel(&same.sender));
        assert!(!first.sender.same_channel(&other.sender));
        assert!(!Arc::ptr_eq(&first.budget, &other.budget));
        shared_raft_client(&a, endpoint, Some(1)).0.unwrap();
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
        let command = GroupWriteCommand::Stream(StreamCommand::CreateBucket {
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
        let (response, receiver) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("test permit");
        let call = AppendStreamCall {
            envelope: raft_internal_proto::RaftRpcEnvelopeV1 {
                raft_group_id,
                node_id: 2,
                protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
                payload: Vec::new().into(),
            },
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

        let (batch, receiver_open) = collect_append_stream_frame(first, &mut receiver);
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

        let (batch, receiver_open) = collect_append_stream_frame(first, &mut receiver);
        assert!(receiver_open);
        assert_eq!(batch.len(), 1, "timed-out calls must not be sent");
        assert_eq!(batch[0].envelope.raft_group_id, 2);
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
        let envelope = || raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id: 1,
            node_id: 2,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: vec![7_u8; entry_bytes].into(),
        };
        let mut timed_out = 0;
        let mut rejected = 0;
        for _ in 0..(3 * RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES / entry_bytes) {
            let result = network
                .try_append_stream(envelope(), RPCOption::new(Duration::from_millis(1)))
                .await;
            let status = result.expect_err("a stalled peer never answers");
            match status.code() {
                tonic::Code::DeadlineExceeded => timed_out += 1,
                tonic::Code::Unavailable => {
                    assert!(status.message().starts_with(APPEND_STREAM_BACKLOG_FULL));
                    rejected += 1;
                }
                code => panic!("unexpected status {code:?}"),
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
        let envelope = |raft_group_id, bytes: usize| raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id,
            node_id: 2,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: vec![7_u8; bytes].into(),
        };
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
        assert_eq!(small.code(), tonic::Code::Unavailable);
        assert!(small.message().starts_with(APPEND_STREAM_BACKLOG_FULL));

        drop(others);
        let large = large.await.expect("large call task");
        // Admitted and queued; the stalled peer never answers it.
        assert_eq!(
            large.expect_err("stalled peer").code(),
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
            raft_internal_proto::raft_internal_client::RaftInternalClient::new(channel);
        let (sender, receiver) = mpsc::channel(1);
        let item = |request_id, raft_group_id| raft_internal_proto::RaftAppendStreamRequestItem {
            request_id,
            envelope: Some(raft_internal_proto::RaftRpcEnvelopeV1 {
                raft_group_id,
                node_id: 2,
                protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
                payload: Vec::new().into(),
            }),
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

    #[tokio::test]
    async fn append_stream_client_coalesces_concurrent_group_calls() {
        let transport = Arc::new(CoreRaftTransport::default());
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        remove_shared_channel(&transport, &endpoint);
        let batch_frames_before = GRPC_APPEND_STREAM_BATCH_FRAMES.load(Ordering::Relaxed);
        let envelope = |raft_group_id| raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id,
            node_id: 2,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: Vec::new().into(),
        };
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

        assert!(results.iter().all(|result| {
            result
                .as_ref()
                .expect_err("missing group should fail")
                .code()
                == tonic::Code::NotFound
        }));
        assert!(
            GRPC_APPEND_STREAM_BATCH_FRAMES.load(Ordering::Relaxed) > batch_frames_before,
            "concurrent groups should share at least one multi-item frame"
        );
        assert!(GRPC_APPEND_STREAM_BATCH_ITEMS_MAX.load(Ordering::Relaxed) > 1);
        server.abort();
    }

    #[tokio::test]
    async fn groups_share_one_append_stream_and_receive_independent_errors() {
        let transport = Arc::new(CoreRaftTransport::default());
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        remove_shared_channel(&transport, &endpoint);

        let mut first = test_network(transport.clone(), RaftGroupId(1), 2, endpoint.clone());
        let second = test_network(transport.clone(), RaftGroupId(2), 2, endpoint.clone());
        let envelope = |raft_group_id| raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id,
            node_id: 2,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: Vec::new().into(),
        };
        let option = RPCOption::new(Duration::from_secs(2));
        let (first_result, second_result) = tokio::join!(
            first.try_append_stream(envelope(1), option.clone()),
            second.try_append_stream(envelope(2), option),
        );

        assert_eq!(
            first_result.expect_err("missing group should fail").code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            second_result
                .expect_err("second missing group should fail")
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
        let original_generation = first.channel_generation;
        for _ in 0..first.reconnect_threshold {
            first.note_failure("test channel replacement");
        }
        assert_ne!(first.channel_generation, original_generation);
        let result = first
            .try_append_stream(envelope(1), RPCOption::new(Duration::from_secs(2)))
            .await;
        assert_eq!(
            result.expect_err("missing group should still fail").code(),
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

    #[tokio::test]
    async fn node_transport_shutdown_closes_append_stream() {
        let (endpoint, registry, server) = spawn_append_stream_server().await;
        let channel = Endpoint::from_shared(endpoint)
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to test server");
        let mut client =
            raft_internal_proto::raft_internal_client::RaftInternalClient::new(channel);
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
        assert!(net.client.is_ok(), "channel should be rebuilt cleanly");
    }

    #[tokio::test]
    async fn networks_share_one_channel_generation_per_endpoint() {
        let transport = Arc::new(CoreRaftTransport::default());
        let endpoint = "http://127.0.0.1:32197";
        remove_shared_channel(&transport, endpoint);
        let first = test_network(transport.clone(), RaftGroupId(1), 2, endpoint);
        let second = test_network(transport.clone(), RaftGroupId(2), 2, endpoint);

        assert_eq!(first.channel_generation, 1);
        assert_eq!(second.channel_generation, first.channel_generation);
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
        let original_generation = first.channel_generation;

        first.note_failure("Append");
        assert_eq!(
            first.channel_generation,
            original_generation.saturating_add(1)
        );

        stale.note_failure("Append");
        assert_eq!(stale.channel_generation, first.channel_generation);
        let pooled_generation = transport
            .channels
            .lock()
            .ok()
            .and_then(|channels| channels.get(endpoint).map(|shared| shared.generation));
        assert_eq!(pooled_generation, Some(first.channel_generation));
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
