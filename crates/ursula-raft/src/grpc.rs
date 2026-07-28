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
use prost::Message;
use serde::de::DeserializeOwned;
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
use ursula_runtime::GetStreamAttrsRequest;
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
use crate::forward::write_commands_on_raft;
use crate::raft_internal_proto;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

pub(crate) static GRPC_LEADER_CHANNELS: OnceLock<Mutex<BTreeMap<String, Channel>>> =
    OnceLock::new();
static GRPC_RAFT_CHANNELS: OnceLock<Mutex<BTreeMap<String, SharedRaftChannel>>> = OnceLock::new();
static GRPC_APPEND_SESSIONS: OnceLock<Mutex<BTreeMap<String, SharedAppendSession>>> =
    OnceLock::new();
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
use crate::registry::LeadershipShedFlag;
use crate::registry::LeadershipShedState;
use crate::registry::RaftGroupHandleRegistry;

pub(crate) type RaftClient = raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>;

#[derive(Clone)]
struct SharedRaftChannel {
    generation: u64,
    channel: Channel,
}

#[derive(Clone)]
struct SharedAppendSession {
    sender: mpsc::UnboundedSender<AppendStreamCall>,
}

struct AppendStreamCall {
    envelope: raft_internal_proto::RaftRpcEnvelopeV1,
    response: oneshot::Sender<Result<raft_internal_proto::RaftRpcAckV1, tonic::Status>>,
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
pub const RAFT_GRPC_TRANSFER_LEADER_PATH: &str = "/ursula.raft.v1.RaftInternal/TransferLeader";
pub const RAFT_GRPC_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const RAFT_GRPC_PROTOCOL_VERSION: u32 = 1;
const RAFT_GRPC_APPEND_STREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const RAFT_GRPC_APPEND_STREAM_MAX_BATCH_ITEMS: usize = 32;
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
    leadership_shed: LeadershipShedFlag,
}

impl RaftGrpcService {
    pub fn new(registry: RaftGroupHandleRegistry) -> Self {
        let leadership_shed = registry.leadership_shed_flag();
        Self {
            registry,
            cold_store: None,
            leadership_shed,
        }
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }

    pub fn with_leadership_shed_flag(mut self, leadership_shed: LeadershipShedFlag) -> Self {
        self.leadership_shed = leadership_shed;
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
        let shutdown = registry.subscribe_transport_shutdown();
        let requests = futures_util::stream::unfold(
            (request.into_inner(), shutdown),
            |(mut requests, mut shutdown)| async move {
                if *shutdown.borrow() {
                    return None;
                }
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        let _ = changed;
                        None
                    }
                    request = requests.next() => {
                        request.map(|request| (request, (requests, shutdown)))
                    }
                }
            },
        );
        let responses = requests
            .map(move |request| {
                let registry = registry.clone();
                let concurrency = concurrency.clone();
                async move {
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
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: encode_wire(&response),
        }))
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
            let results = write_commands_on_raft(raft, placement, None, commands)
                .await
                .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?
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
        let shed_state = LeadershipShedState::load(&self.leadership_shed);
        if let Some(reason) = shed_state.transfer_rejection_reason() {
            return Err(GrpcRpcError::failed_precondition(format!(
                "node {reason} shed leadership; refusing TransferLeader for group {}",
                raft_group_id.0
            ))
            .into());
        }
        let openraft_request: TransferLeaderRequest<UrsulaRaftTypeConfig> =
            decode_rpc_payload(&request.request, "transfer leader request")?;
        self.registry
            .handle_transfer_leader(raft_group_id, openraft_request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
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
            let mut engine = RaftGroupEngine {
                raft,
                placement,
                metrics: None,
                cold_store: self.cold_store.clone(),
                cold_index_cache: self.cold_store.as_ref().map(|cold_store| {
                    Arc::new(ColdIndexPageCache::new(
                        Arc::new(ColdStoreColdIndexPageStore::new(cold_store.clone())),
                        1024,
                    ))
                }),
            };
            let stream_id = BucketStreamId::new(request.bucket_id, request.stream_id);
            let result = match required(request.read, "group_read.read")
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?
            {
                raft_internal_proto::group_read_request_v1::Read::Head(_) => engine
                    .head_stream(
                        HeadStreamRequest {
                            stream_id,
                            now_ms: request.now_ms,
                        },
                        placement,
                    )
                    .await
                    .map(|response| raft_internal_proto::GroupReadResponseV1 {
                        ok: true,
                        payload: encode_wire(&response),
                    }),
                raft_internal_proto::group_read_request_v1::Read::GetStreamAttrs(_) => engine
                    .get_stream_attrs(
                        GetStreamAttrsRequest {
                            stream_id,
                            now_ms: request.now_ms,
                        },
                        placement,
                    )
                    .await
                    .map(|response| raft_internal_proto::GroupReadResponseV1 {
                        ok: true,
                        payload: encode_wire(&response),
                    }),
                raft_internal_proto::group_read_request_v1::Read::ReadStream(read) => {
                    let max_len = usize::try_from(read.max_len).map_err(|_| {
                        tonic::Status::invalid_argument("group_read.read_stream.max_len too large")
                    })?;
                    engine
                        .read_stream(
                            ReadStreamRequest {
                                stream_id,
                                offset: read.offset,
                                max_len,
                                now_ms: request.now_ms,
                                record: read.record,
                                max_records: read.max_records,
                            },
                            placement,
                        )
                        .await
                        .map(|response| raft_internal_proto::GroupReadResponseV1 {
                            ok: true,
                            payload: encode_wire(&response),
                        })
                }
            };
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
        return Err(GrpcRpcError::failed_precondition(format!(
            "raft grpc protocol mismatch: local={}, remote={protocol_version}",
            RAFT_GRPC_PROTOCOL_VERSION
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct GrpcRaftNetworkFactory {
    raft_group_id: RaftGroupId,
    reconnect_threshold: u32,
}

impl GrpcRaftNetworkFactory {
    pub fn new(raft_group_id: RaftGroupId) -> Self {
        Self {
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

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        GrpcRaftNetwork::with_threshold(
            self.raft_group_id,
            target,
            node.addr.clone(),
            self.reconnect_threshold,
        )
    }
}

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    raft_group_id: RaftGroupId,
    target: u64,
    endpoint: String,
    client: Result<RaftClient, String>,
    channel_generation: u64,
    /// Streak of consecutive RPC failures on this channel. Reset to 0 on the
    /// next successful RPC. When it crosses `reconnect_threshold` we replace
    /// the process-wide HTTP/2 channel generation — tonic's `connect_lazy`
    /// keeps a stuck channel forever otherwise (the TCP socket stays open, the
    /// HTTP/2 streams stay borked, no auto-heal).
    consecutive_failures: u32,
    reconnect_threshold: u32,
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
        let endpoint = normalize_grpc_endpoint(address.into());
        let (client, channel_generation) = shared_raft_client(&endpoint, None);
        Self {
            raft_group_id,
            target,
            endpoint,
            client,
            channel_generation,
            consecutive_failures: 0,
            reconnect_threshold,
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
            let (client, generation) =
                shared_raft_client(&self.endpoint, Some(self.channel_generation));
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
        let sender =
            shared_append_session(&self.endpoint, client).map_err(tonic::Status::unavailable)?;
        let (response_sender, response_receiver) = oneshot::channel();
        sender
            .send(AppendStreamCall {
                envelope,
                response: response_sender,
            })
            .map_err(|_| tonic::Status::unavailable("raft append stream is closed"))?;
        match tokio::time::timeout(option.hard_ttl(), response_receiver).await {
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
                let mapped = self.map_tonic_status("AppendStream", status);
                self.note_failure("AppendStream");
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

/// Return the process-wide HTTP/2 channel for a Raft peer.
///
/// OpenRaft constructs one network client per group and peer. Without this
/// pool, every group creates its own TCP connection even though tonic channels
/// can multiplex all of those RPCs over one HTTP/2 connection.
///
/// `observed_generation` is `None` for a new group, which always adopts the
/// current shared channel. A reconnect passes the generation it was using: if
/// another group has already replaced that generation, it adopts the newer
/// channel; otherwise it performs exactly one process-wide replacement.
fn shared_raft_client(
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
    let channels = GRPC_RAFT_CHANNELS.get_or_init(|| Mutex::new(BTreeMap::new()));
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
    endpoint: &str,
    client: RaftClient,
) -> Result<mpsc::UnboundedSender<AppendStreamCall>, String> {
    let sessions = GRPC_APPEND_SESSIONS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut sessions = sessions
        .lock()
        .map_err(|err| format!("raft append session pool lock poisoned for {endpoint}: {err}"))?;
    if let Some(session) = sessions.get(endpoint)
        && !session.sender.is_closed()
    {
        return Ok(session.sender.clone());
    }

    let (sender, receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_append_session(client, receiver));
    sessions.insert(endpoint.to_owned(), SharedAppendSession {
        sender: sender.clone(),
    });
    Ok(sender)
}

fn collect_append_stream_frame(
    first: AppendStreamCall,
    calls: &mut mpsc::UnboundedReceiver<AppendStreamCall>,
) -> (Vec<AppendStreamCall>, bool) {
    let mut frame = vec![first];
    while frame.len() < RAFT_GRPC_APPEND_STREAM_MAX_BATCH_ITEMS {
        match calls.try_recv() {
            Ok(call) => frame.push(call),
            Err(mpsc::error::TryRecvError::Empty) => return (frame, true),
            Err(mpsc::error::TryRecvError::Disconnected) => return (frame, false),
        }
    }
    (frame, true)
}

async fn run_append_session(
    mut client: RaftClient,
    mut calls: mpsc::UnboundedReceiver<AppendStreamCall>,
) {
    let (wire_sender, wire_receiver) = mpsc::channel(1024);
    client = client.send_compressed(CompressionEncoding::Zstd);
    let response = tokio::time::timeout(
        RAFT_GRPC_APPEND_STREAM_CONNECT_TIMEOUT,
        client.append_stream(tonic::Request::new(ReceiverStream::new(wire_receiver))),
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
    let mut pending = BTreeMap::<
        u64,
        oneshot::Sender<Result<raft_internal_proto::RaftRpcAckV1, tonic::Status>>,
    >::new();
    let mut next_request_id = 1_u64;
    let mut accepting = true;

    loop {
        let pending_before_retain = pending.len();
        pending.retain(|_, response| !response.is_closed());
        let cancelled = pending_before_retain.saturating_sub(pending.len()) as u64;
        GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(cancelled, Ordering::Relaxed);
        if !accepting && pending.is_empty() {
            break;
        }
        tokio::select! {
            call = calls.recv(), if accepting => {
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
                for call in frame_calls {
                    let request_id = next_request_id;
                    next_request_id = next_request_id.saturating_add(1);
                    GRPC_APPEND_STREAM_REQUEST_BYTES.fetch_add(
                        call.envelope.encoded_len() as u64,
                        Ordering::Relaxed,
                    );
                    pending.insert(request_id, call.response);
                    items.push(raft_internal_proto::RaftAppendStreamRequestItem {
                        request_id,
                        envelope: Some(call.envelope),
                    });
                }
                let request_ids = items.iter().map(|item| item.request_id).collect::<Vec<_>>();
                let request = raft_internal_proto::RaftAppendStreamRequest {
                    items,
                };
                let sent = match wire_sender.as_ref() {
                    Some(sender) => sender.send(request).await.is_ok(),
                    None => false,
                };
                if !sent {
                    GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(frame_items, Ordering::Relaxed);
                    for request_id in request_ids {
                        if let Some(response) = pending.remove(&request_id) {
                            let _ = response.send(Err(tonic::Status::unavailable(
                                "raft append stream request channel closed",
                            )));
                        }
                    }
                    accepting = false;
                    wire_sender.take();
                }
            }
            response = responses.message() => {
                match response {
                    Ok(Some(response)) => {
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
                            let _ = reply.send(result);
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
        }
    }

    let abandoned = pending.len() as u64;
    GRPC_APPEND_STREAM_INFLIGHT.fetch_sub(abandoned, Ordering::Relaxed);
    for (_, response) in pending {
        let _ = response.send(Err(tonic::Status::unavailable(
            "raft append stream closed before the peer replied",
        )));
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

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyAppendStreamRequest {
        #[prost(uint64, tag = "1")]
        request_id: u64,
        #[prost(message, optional, tag = "2")]
        envelope: Option<raft_internal_proto::RaftRpcEnvelopeV1>,
        #[prost(message, repeated, tag = "3")]
        batch: Vec<raft_internal_proto::RaftAppendStreamRequestItem>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyAppendStreamResponse {
        #[prost(uint64, tag = "1")]
        request_id: u64,
        #[prost(message, repeated, tag = "4")]
        batch: Vec<raft_internal_proto::RaftAppendStreamResponseItem>,
    }

    fn remove_shared_channel(endpoint: &str) {
        if let Ok(mut channels) = GRPC_RAFT_CHANNELS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
        {
            channels.remove(endpoint);
        }
    }

    fn fresh_network(threshold: u32) -> GrpcRaftNetwork {
        let mut net = GrpcRaftNetwork::new(RaftGroupId(0), 2, "http://127.0.0.1:9999");
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

    #[test]
    fn batched_append_stream_preserves_032_wire_field_numbers() {
        let item = raft_internal_proto::RaftAppendStreamRequestItem {
            request_id: 7,
            envelope: Some(raft_internal_proto::RaftRpcEnvelopeV1 {
                raft_group_id: 3,
                node_id: 2,
                protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
                payload: vec![1, 2, 3].into(),
            }),
        };
        let current_request = raft_internal_proto::RaftAppendStreamRequest { items: vec![item] };
        let legacy_request =
            LegacyAppendStreamRequest::decode(current_request.encode_to_vec().as_slice())
                .expect("0.3.32 request shape decodes current batch");
        assert_eq!(legacy_request.batch.len(), 1);
        assert_eq!(legacy_request.batch[0].request_id, 7);

        let legacy_response = LegacyAppendStreamResponse {
            request_id: 0,
            batch: vec![raft_internal_proto::RaftAppendStreamResponseItem {
                request_id: 7,
                result: Some(
                    raft_internal_proto::raft_append_stream_response_item::Result::Ack(
                        raft_internal_proto::RaftRpcAckV1 {
                            payload: vec![4, 5, 6].into(),
                        },
                    ),
                ),
            }],
        };
        let current_response = raft_internal_proto::RaftAppendStreamResponse::decode(
            legacy_response.encode_to_vec().as_slice(),
        )
        .expect("current response shape decodes 0.3.32 batch");
        assert_eq!(current_response.items.len(), 1);
        assert_eq!(current_response.items[0].request_id, 7);
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

    #[test]
    fn append_stream_frame_drains_only_already_queued_calls() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let call = |raft_group_id| {
            let (response, _receiver) = oneshot::channel();
            AppendStreamCall {
                envelope: raft_internal_proto::RaftRpcEnvelopeV1 {
                    raft_group_id,
                    node_id: 2,
                    protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
                    payload: Vec::new().into(),
                },
                response,
            }
        };
        sender.send(call(2)).expect("queue second call");
        sender.send(call(3)).expect("queue third call");

        let (batch, receiver_open) = collect_append_stream_frame(call(1), &mut receiver);
        assert!(receiver_open);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].envelope.raft_group_id, 1);
        assert_eq!(batch[1].envelope.raft_group_id, 2);
        assert_eq!(batch[2].envelope.raft_group_id, 3);
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
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        remove_shared_channel(&endpoint);
        let batch_frames_before = GRPC_APPEND_STREAM_BATCH_FRAMES.load(Ordering::Relaxed);
        let envelope = |raft_group_id| raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id,
            node_id: 2,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: Vec::new().into(),
        };
        let calls = (1..=16).map(|raft_group_id| {
            let network = GrpcRaftNetwork::new(RaftGroupId(raft_group_id), 2, endpoint.clone());
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
        let (endpoint, _registry, server) = spawn_append_stream_server().await;
        remove_shared_channel(&endpoint);

        let mut first = GrpcRaftNetwork::new(RaftGroupId(1), 2, endpoint.clone());
        let second = GrpcRaftNetwork::new(RaftGroupId(2), 2, endpoint.clone());
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
        let shared_session_open = GRPC_APPEND_SESSIONS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .expect("append session pool lock")
            .get(&endpoint)
            .is_some_and(|session| !session.sender.is_closed());
        assert!(shared_session_open);

        let original_sender = GRPC_APPEND_SESSIONS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
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
        let replacement_sender = GRPC_APPEND_SESSIONS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
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
        let endpoint = "http://127.0.0.1:32197";
        remove_shared_channel(endpoint);
        let first = GrpcRaftNetwork::new(RaftGroupId(1), 2, endpoint);
        let second = GrpcRaftNetwork::new(RaftGroupId(2), 2, endpoint);

        assert_eq!(first.channel_generation, 1);
        assert_eq!(second.channel_generation, first.channel_generation);
        let channel_count = GRPC_RAFT_CHANNELS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .ok()
            .map(|channels| usize::from(channels.contains_key(endpoint)));
        assert_eq!(channel_count, Some(1));
    }

    #[tokio::test]
    async fn stale_network_adopts_rebuilt_generation_without_replacing_it_again() {
        let endpoint = "http://127.0.0.1:32198";
        remove_shared_channel(endpoint);
        let mut first = GrpcRaftNetwork::with_threshold(RaftGroupId(1), 2, endpoint, 1);
        let mut stale = GrpcRaftNetwork::with_threshold(RaftGroupId(2), 2, endpoint, 1);
        let original_generation = first.channel_generation;

        first.note_failure("Append");
        assert_eq!(
            first.channel_generation,
            original_generation.saturating_add(1)
        );

        stale.note_failure("Append");
        assert_eq!(stale.channel_generation, first.channel_generation);
        let pooled_generation = GRPC_RAFT_CHANNELS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
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
        // tonic accepts a lot of textually-weird endpoints (e.g. "not-a-url"
        // gets normalized to "http://not-a-url" and parses fine; it just
        // fails on connect). Force a real `from_shared` rejection with a
        // genuinely-invalid URI — the rebuild path must surface that as a
        // permanent Err on `client`, not panic, so openraft keeps retrying.
        let mut net = GrpcRaftNetwork::new(RaftGroupId(0), 2, "http://");
        net.reconnect_threshold = 2;
        net.note_failure("Append");
        net.note_failure("Append");
        assert_eq!(net.consecutive_failures, 0);
        // Whether the post-rebuild client is Ok or Err is tonic's choice for
        // this endpoint string; the contract is just "no panic, counter reset".
    }
}
