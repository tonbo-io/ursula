use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
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
use crate::log_store::append_entries_request_from_proto;
use crate::log_store::append_entries_request_to_proto;
use crate::log_store::append_entries_response_from_proto;
use crate::log_store::append_entries_response_to_proto;
use crate::log_store::log_id_from_required_proto;
use crate::log_store::log_id_to_proto;
use crate::log_store::snapshot_meta_from_required_proto;
use crate::log_store::snapshot_meta_to_proto;
use crate::log_store::snapshot_response_from_required_proto;
use crate::log_store::snapshot_response_to_proto;
use crate::log_store::vote_from_required_proto;
use crate::log_store::vote_request_from_proto;
use crate::log_store::vote_request_to_proto;
use crate::log_store::vote_response_from_proto;
use crate::log_store::vote_response_to_proto;
use crate::log_store::vote_to_proto;
use crate::raft_internal_proto;
use crate::raft_internal_proto::raft_rpc_ack_v1::Payload as AckPayload;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

pub(crate) static GRPC_LEADER_CHANNELS: OnceLock<Mutex<BTreeMap<String, Channel>>> =
    OnceLock::new();
use crate::registry::LeadershipShedFlag;
use crate::registry::LeadershipShedState;
use crate::registry::RaftGroupHandleRegistry;

pub(crate) type RaftClient = raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>;

pub const RAFT_GRPC_APPEND_PATH: &str = "/ursula.raft.v1.RaftInternal/Append";
pub const RAFT_GRPC_VOTE_PATH: &str = "/ursula.raft.v1.RaftInternal/Vote";
pub const RAFT_GRPC_FULL_SNAPSHOT_PATH: &str = "/ursula.raft.v1.RaftInternal/FullSnapshot";
pub const RAFT_GRPC_GROUP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupWrite";
pub const RAFT_GRPC_GROUP_READ_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupRead";
pub const RAFT_GRPC_TRANSFER_LEADER_PATH: &str = "/ursula.raft.v1.RaftInternal/TransferLeader";
pub const RAFT_GRPC_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const RAFT_GRPC_PROTOCOL_VERSION: u32 = 1;

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
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

#[tonic::async_trait]
impl raft_internal_proto::raft_internal_server::RaftInternal for RaftGrpcService {
    async fn append(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        let raft_group_id = validate_raft_rpc_preamble(
            &self.registry,
            envelope.protocol_version,
            envelope.raft_group_id,
        )?;
        let payload = required(envelope.payload, "raft_rpc_envelope.payload")
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?;
        let request = append_entries_request_from_proto(payload)?;
        let response = self
            .registry
            .append_entries(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: Some(AckPayload::AppendEntries(append_entries_response_to_proto(
                response,
            ))),
        }))
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
        let payload = required(envelope.payload, "raft_rpc_envelope.payload")
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?;
        let request = vote_request_from_proto(payload)?;
        let response = self
            .registry
            .vote(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: Some(AckPayload::Vote(vote_response_to_proto(response))),
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
        let vote = vote_from_required_proto(request.vote)?;
        let meta = snapshot_meta_from_required_proto(request.snapshot_meta)?;
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
                response: Some(snapshot_response_to_proto(response)),
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
        let from_leader = vote_from_required_proto(request.from_leader)
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?;
        let last_log_id = match request.last_log_id {
            Some(log_id) => Some(
                log_id_from_required_proto(Some(log_id), "transfer_leader.last_log_id")
                    .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?,
            ),
            None => None,
        };
        let openraft_request = openraft::raft::TransferLeaderRequest::<UrsulaRaftTypeConfig>::new(
            from_leader,
            request.to_node_id,
            last_log_id,
        );
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
    /// Streak of consecutive RPC failures on this channel. Reset to 0 on the
    /// next successful RPC. When it crosses `reconnect_threshold` we drop the
    /// underlying HTTP/2 channel and rebuild a fresh one — tonic's
    /// `connect_lazy` keeps a stuck channel forever otherwise (the TCP socket
    /// stays open, the HTTP/2 streams stay borked, no auto-heal).
    consecutive_failures: u32,
    reconnect_threshold: u32,
}

impl Debug for GrpcRaftNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRaftNetwork")
            .field("raft_group_id", &self.raft_group_id)
            .field("target", &self.target)
            .field("endpoint", &self.endpoint)
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
        let client = build_client(&endpoint);
        Self {
            raft_group_id,
            target,
            endpoint,
            client,
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
    /// stuck channel and build a fresh one — the next RPC call gets a new
    /// HTTP/2 connection. We also reset the counter so the freshly-built
    /// channel gets a full grace period before any further rebuild.
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
            self.client = build_client(&self.endpoint);
            self.consecutive_failures = 0;
        }
    }

    fn note_success(&mut self) {
        self.consecutive_failures = 0;
    }

    pub(crate) fn append_envelope(
        &self,
        request: UrsulaAppendEntriesRequest,
    ) -> raft_internal_proto::RaftRpcEnvelopeV1 {
        raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload: Some(
                raft_internal_proto::raft_rpc_envelope_v1::Payload::AppendEntries(
                    append_entries_request_to_proto(request),
                ),
            ),
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
            from_leader: Some(vote_to_proto(*request.from_leader())),
            to_node_id: *request.to_node_id(),
            last_log_id: request.last_log_id().cloned().map(log_id_to_proto),
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
            payload: Some(raft_internal_proto::raft_rpc_envelope_v1::Payload::Vote(
                vote_request_to_proto(request),
            )),
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
        Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    {
        let mut request = tonic::Request::new(request);
        self.apply_rpc_timeout(&mut request, option);
        match send(self.client()?, request).await {
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

    /// Decode an envelope-style ack: require the payload, select the expected
    /// payload variant via `take`, and convert it with `convert`.
    fn decode_rpc_ack<P, T, E>(
        &self,
        route: &str,
        ack_name: &str,
        ack: raft_internal_proto::RaftRpcAckV1,
        take: impl FnOnce(raft_internal_proto::raft_rpc_ack_v1::Payload) -> Option<P>,
        convert: impl FnOnce(P) -> Result<T, E>,
    ) -> Result<T, RPCError<UrsulaRaftTypeConfig>>
    where
        E: std::fmt::Display,
    {
        let payload = required(ack.payload, ack_name)
            .map_err(|err| raft_rpc_network_error(err.to_string()))?;
        let Some(payload) = take(payload) else {
            return Err(raft_rpc_network_error(format!(
                "{route} response from node {} at {} had wrong payload type",
                self.target, self.endpoint
            )));
        };
        convert(payload).map_err(|err| {
            raft_rpc_network_error(format!(
                "decode {route} response from node {} at {}: {err}",
                self.target, self.endpoint
            ))
        })
    }
}

/// Construct a fresh tonic client over a lazy channel. Called both during
/// initial `new` and during reconnect when the channel is detected stuck.
fn build_client(endpoint: &str) -> Result<RaftClient, String> {
    Endpoint::from_shared(endpoint.to_owned())
        .map(|ep| {
            RaftClient::new(ep.connect_lazy())
                .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
        })
        .map_err(|err| format!("invalid raft gRPC endpoint {endpoint}: {err}"))
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
        let envelope = self.append_envelope(rpc);
        let ack = self
            .call(
                "Append",
                envelope,
                option,
                |mut client, request| async move { client.append(request).await },
            )
            .await?;
        self.decode_rpc_ack(
            "Append",
            "raft append ack payload",
            ack,
            |payload| match payload {
                AckPayload::AppendEntries(response) => Some(response),
                _ => None,
            },
            append_entries_response_from_proto,
        )
    }

    async fn vote(
        &mut self,
        rpc: UrsulaVoteRequest,
        option: RPCOption,
    ) -> Result<UrsulaVoteResponse, RPCError<UrsulaRaftTypeConfig>> {
        let envelope = self.vote_envelope(rpc);
        let ack = self
            .call("Vote", envelope, option, |mut client, request| async move {
                client.vote(request).await
            })
            .await?;
        self.decode_rpc_ack(
            "Vote",
            "raft vote ack payload",
            ack,
            |payload| match payload {
                AckPayload::Vote(response) => Some(response),
                _ => None,
            },
            vote_response_from_proto,
        )
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
            vote: Some(vote_to_proto(vote)),
            snapshot_meta: Some(snapshot_meta_to_proto(snapshot.meta)),
            snapshot_payload: snapshot.snapshot.into_inner().into(),
        };
        let ack = self
            .call(
                "FullSnapshot",
                request,
                option,
                |mut client, request| async move { client.full_snapshot(request).await },
            )
            .await
            .map_err(StreamingError::from)?;
        snapshot_response_from_required_proto(ack.response).map_err(|err| {
            StreamingError::from(raft_rpc_network_error(format!(
                "decode FullSnapshot response from node {} at {}: {err}",
                self.target, self.endpoint
            )))
        })
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
    use super::*;

    fn fresh_network(threshold: u32) -> GrpcRaftNetwork {
        let mut net = GrpcRaftNetwork::new(RaftGroupId(0), 2, "http://127.0.0.1:9999");
        // Override threshold so tests don't depend on the env var
        net.reconnect_threshold = threshold;
        net
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
