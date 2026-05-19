use openraft::RaftNetworkV2;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::sync::Mutex;
use std::sync::OnceLock;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::alias::SnapshotOf;
use openraft::alias::VoteOf;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::SnapshotResponse;
use prost::Message;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use ursula_proto as raft_app_proto;
use ursula_runtime::{
    ColdStoreHandle, GroupEngine, GroupEngineError, HeadStreamRequest, ReadStreamRequest,
};
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use crate::codec::*;
use crate::engine::*;
use crate::forward::*;
use crate::log_store::*;
use crate::raft_internal_proto;
use crate::types::*;

pub(crate) static GRPC_LEADER_CHANNELS: OnceLock<Mutex<BTreeMap<String, Channel>>> =
    OnceLock::new();
use crate::registry::RaftGroupHandleRegistry;

pub const RAFT_GRPC_APPEND_PATH: &str = "/ursula.raft.v1.RaftInternal/Append";
pub const RAFT_GRPC_VOTE_PATH: &str = "/ursula.raft.v1.RaftInternal/Vote";
pub const RAFT_GRPC_FULL_SNAPSHOT_PATH: &str = "/ursula.raft.v1.RaftInternal/FullSnapshot";
pub const RAFT_GRPC_FORWARD_HTTP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/ForwardHttpWrite";
pub const RAFT_GRPC_GROUP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupWrite";
pub const RAFT_GRPC_GROUP_READ_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupRead";
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
        let raft_group_id = validate_raft_rpc_envelope(&self.registry, &envelope)?;
        let payload = required(envelope.payload, "raft_rpc_envelope.payload")
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?;
        let request = append_entries_request_from_proto(payload)?;
        let response = self
            .registry
            .append_entries(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: Some(
                raft_internal_proto::raft_rpc_ack_v1::Payload::AppendEntries(
                    append_entries_response_to_proto(response),
                ),
            ),
        }))
    }

    async fn vote(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        let raft_group_id = validate_raft_rpc_envelope(&self.registry, &envelope)?;
        let payload = required(envelope.payload, "raft_rpc_envelope.payload")
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?;
        let request = vote_request_from_proto(payload)?;
        let response = self
            .registry
            .vote(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: Some(raft_internal_proto::raft_rpc_ack_v1::Payload::Vote(
                vote_response_to_proto(response),
            )),
        }))
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<raft_internal_proto::RaftFullSnapshotRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftFullSnapshotAckV1>, tonic::Status> {
        let request = request.into_inner();
        let raft_group_id = validate_raft_snapshot_request(&self.registry, &request)?;
        let vote = vote_from_required_proto(request.vote)?;
        let meta = snapshot_meta_from_required_proto(request.snapshot_meta)?;
        let snapshot = SnapshotOf::<UrsulaRaftTypeConfig> {
            meta,
            snapshot: Cursor::new(request.snapshot_payload),
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

    async fn forward_http_write(
        &self,
        _request: tonic::Request<raft_internal_proto::HttpWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::HttpWriteResponseV1>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "HTTP write forwarding is provided by the HTTP adapter",
        ))
    }

    async fn group_write(
        &self,
        request: tonic::Request<raft_internal_proto::GroupWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupWriteResponseV1>, tonic::Status> {
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
            .map(|payload| {
                let command = raft_app_proto::RaftGroupCommandV1::decode(payload.as_slice())
                    .map_err(|err| GroupEngineError::new(format!("decode group command: {err}")))?;
                group_write_command_from_proto(RaftGroupCommand(command))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let results = write_commands_on_raft(raft, placement, None, commands)
            .await
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?
            .into_iter()
            .map(encode_group_write_result)
            .collect();
        Ok(tonic::Response::new(
            raft_internal_proto::GroupWriteResponseV1 { results },
        ))
    }

    async fn group_read(
        &self,
        request: tonic::Request<raft_internal_proto::GroupReadRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupReadResponseV1>, tonic::Status> {
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
                    payload: head_stream_response_to_proto(response).encode_to_vec(),
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
                        },
                        placement,
                    )
                    .await
                    .map(|response| raft_internal_proto::GroupReadResponseV1 {
                        ok: true,
                        payload: read_stream_response_to_proto(response).encode_to_vec(),
                    })
            }
        };
        let response = match result {
            Ok(response) => response,
            Err(err) => raft_internal_proto::GroupReadResponseV1 {
                ok: false,
                payload: group_engine_error_to_proto(err).encode_to_vec(),
            },
        };
        Ok(tonic::Response::new(response))
    }
}

pub(crate) fn validate_raft_rpc_envelope(
    registry: &RaftGroupHandleRegistry,
    envelope: &raft_internal_proto::RaftRpcEnvelopeV1,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_grpc_metadata(envelope.protocol_version)?;
    let raft_group_id = RaftGroupId(envelope.raft_group_id);
    if !registry.contains_group(raft_group_id) {
        return Err(GrpcRpcError::not_found(format!(
            "raft group {} is not registered on this node",
            raft_group_id.0
        )));
    }
    Ok(raft_group_id)
}

pub(crate) fn validate_raft_snapshot_request(
    registry: &RaftGroupHandleRegistry,
    request: &raft_internal_proto::RaftFullSnapshotRequestV1,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_grpc_metadata(request.protocol_version)?;
    let raft_group_id = RaftGroupId(request.raft_group_id);
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
}

impl GrpcRaftNetworkFactory {
    pub fn new(raft_group_id: RaftGroupId) -> Self {
        Self { raft_group_id }
    }
}

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        GrpcRaftNetwork::new(self.raft_group_id, target, node.addr.clone())
    }
}

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    raft_group_id: RaftGroupId,
    target: u64,
    endpoint: String,
    client: Result<raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>, String>,
}

impl Debug for GrpcRaftNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRaftNetwork")
            .field("raft_group_id", &self.raft_group_id)
            .field("target", &self.target)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl GrpcRaftNetwork {
    pub fn new(raft_group_id: RaftGroupId, target: u64, address: impl Into<String>) -> Self {
        let endpoint = normalize_grpc_endpoint(address.into());
        let client = Endpoint::from_shared(endpoint.clone())
            .map(|endpoint| {
                raft_internal_proto::raft_internal_client::RaftInternalClient::new(
                    endpoint.connect_lazy(),
                )
                .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
            })
            .map_err(|err| format!("invalid raft gRPC endpoint {endpoint}: {err}"));
        Self {
            raft_group_id,
            target,
            endpoint,
            client,
        }
    }

    pub(crate) fn client(
        &self,
    ) -> Result<
        raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>,
        RPCError<UrsulaRaftTypeConfig>,
    > {
        self.client
            .clone()
            .map_err(|err| RPCError::Unreachable(Unreachable::from_string(err)))
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
        let mut request = tonic::Request::new(self.append_envelope(rpc));
        self.apply_rpc_timeout(&mut request, option);
        let response = self
            .client()?
            .append(request)
            .await
            .map_err(|err| self.map_tonic_status("Append", err))?
            .into_inner();
        match required(response.payload, "raft append ack payload")
            .map_err(|err| raft_rpc_network_error(err.to_string()))?
        {
            raft_internal_proto::raft_rpc_ack_v1::Payload::AppendEntries(response) => {
                append_entries_response_from_proto(response).map_err(|err| {
                    raft_rpc_network_error(format!(
                        "decode Append response from node {} at {}: {err}",
                        self.target, self.endpoint
                    ))
                })
            }
            _ => Err(raft_rpc_network_error(format!(
                "Append response from node {} at {} had wrong payload type",
                self.target, self.endpoint
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: UrsulaVoteRequest,
        option: RPCOption,
    ) -> Result<UrsulaVoteResponse, RPCError<UrsulaRaftTypeConfig>> {
        let mut request = tonic::Request::new(self.vote_envelope(rpc));
        self.apply_rpc_timeout(&mut request, option);
        let response = self
            .client()?
            .vote(request)
            .await
            .map_err(|err| self.map_tonic_status("Vote", err))?
            .into_inner();
        match required(response.payload, "raft vote ack payload")
            .map_err(|err| raft_rpc_network_error(err.to_string()))?
        {
            raft_internal_proto::raft_rpc_ack_v1::Payload::Vote(response) => {
                vote_response_from_proto(response).map_err(|err| {
                    raft_rpc_network_error(format!(
                        "decode Vote response from node {} at {}: {err}",
                        self.target, self.endpoint
                    ))
                })
            }
            _ => Err(raft_rpc_network_error(format!(
                "Vote response from node {} at {} had wrong payload type",
                self.target, self.endpoint
            ))),
        }
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
            snapshot_payload: snapshot.snapshot.into_inner(),
        };
        let mut request = tonic::Request::new(request);
        self.apply_rpc_timeout(&mut request, option);
        let response = self
            .client()
            .map_err(StreamingError::from)?
            .full_snapshot(request)
            .await
            .map_err(|err| StreamingError::from(self.map_tonic_status("FullSnapshot", err)))?
            .into_inner();
        snapshot_response_from_required_proto(response.response).map_err(|err| {
            StreamingError::from(raft_rpc_network_error(format!(
                "decode FullSnapshot response from node {} at {}: {err}",
                self.target, self.endpoint
            )))
        })
    }
}
