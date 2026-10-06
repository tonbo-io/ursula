//! Concrete meta consensus transport, kept separate from the data-group RPCs.

use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;

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
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::codec::encode_wire;
use crate::grpc::GrpcRpcError;
use crate::grpc::normalize_grpc_endpoint;
use crate::meta::MetaRaft;
use crate::meta::MetaRaftError;
use crate::meta::MetaRaftHandle;
use crate::meta::MetaRaftTypeConfig;
use crate::raft_internal_proto::MetaRaftRpcEnvelopeV1;
use crate::raft_internal_proto::MetaRaftSnapshotRequestV1;
use crate::raft_internal_proto::RaftFullSnapshotAckV1;
use crate::raft_internal_proto::RaftRpcAckV1;
use crate::raft_internal_proto::RaftTransferLeaderAckV1;
use crate::raft_internal_proto::meta_raft_internal_client::MetaRaftInternalClient;
use crate::raft_internal_proto::meta_raft_internal_server::MetaRaftInternal;
use crate::raft_internal_proto::meta_raft_internal_server::MetaRaftInternalServer;

pub const META_RAFT_PROTOCOL_VERSION: u32 = 1;
pub const META_RAFT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const META_RAFT_APPEND_PATH: &str = "/ursula.raft.v1.MetaRaftInternal/Append";
pub const META_RAFT_VOTE_PATH: &str = "/ursula.raft.v1.MetaRaftInternal/Vote";
pub const META_RAFT_FULL_SNAPSHOT_PATH: &str = "/ursula.raft.v1.MetaRaftInternal/FullSnapshot";
pub const META_RAFT_TRANSFER_LEADER_PATH: &str = "/ursula.raft.v1.MetaRaftInternal/TransferLeader";

fn cluster_identity(value: impl Into<String>) -> Result<Arc<str>, MetaRaftError> {
    let value = value.into();
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(MetaRaftError::new(
            "validate meta cluster identity",
            "cluster_id must be 1..=128 ASCII letters, digits, '-', '_' or '.'",
        ));
    }
    Ok(value.into())
}

#[derive(Clone)]
pub struct MetaRaftGrpcService {
    cluster_id: Arc<str>,
    node_id: u64,
    raft: MetaRaft,
}

impl MetaRaftGrpcService {
    /// The caller must supply the replica's persisted bootstrap identity.
    /// This precondition prevents accidental cross-cluster routing; the private
    /// cluster listener still requires the deployment's network access controls.
    pub fn new(
        cluster_id: impl Into<String>,
        handle: &MetaRaftHandle,
    ) -> Result<Self, MetaRaftError> {
        let raft = handle.raft_handle();
        let node_id = raft.metrics().borrow_watched().id;
        if node_id == 0 {
            return Err(MetaRaftError::new(
                "create meta RPC service",
                "node_id must be non-zero",
            ));
        }
        Ok(Self {
            cluster_id: cluster_identity(cluster_id)?,
            node_id,
            raft,
        })
    }

    fn validate(&self, cluster_id: &str, target: u64, version: u32) -> Result<(), GrpcRpcError> {
        if version != META_RAFT_PROTOCOL_VERSION {
            return Err(GrpcRpcError::failed_precondition(
                "meta protocol version mismatch",
            ));
        }
        if cluster_id != self.cluster_id.as_ref() {
            return Err(GrpcRpcError::failed_precondition(
                "meta cluster identity mismatch",
            ));
        }
        if target != self.node_id {
            return Err(GrpcRpcError::failed_precondition(
                "meta recipient node identity mismatch",
            ));
        }
        Ok(())
    }
}

pub fn meta_raft_grpc_service(
    service: MetaRaftGrpcService,
) -> MetaRaftInternalServer<MetaRaftGrpcService> {
    MetaRaftInternalServer::new(service)
        .max_decoding_message_size(META_RAFT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(META_RAFT_MAX_MESSAGE_BYTES)
}

fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, GrpcRpcError> {
    rmp_serde::from_slice(payload).map_err(|error| {
        GrpcRpcError::invalid_argument(format!("invalid meta Raft payload: {error}"))
    })
}

#[tonic::async_trait]
impl MetaRaftInternal for MetaRaftGrpcService {
    async fn append(
        &self,
        request: tonic::Request<MetaRaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        self.validate(
            &envelope.cluster_id,
            envelope.target_node_id,
            envelope.protocol_version,
        )?;
        let rpc: AppendEntriesRequest<MetaRaftTypeConfig> = decode(&envelope.payload)?;
        let response = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(RaftRpcAckV1 {
            payload: encode_wire(&response),
        }))
    }

    async fn vote(
        &self,
        request: tonic::Request<MetaRaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        self.validate(
            &envelope.cluster_id,
            envelope.target_node_id,
            envelope.protocol_version,
        )?;
        let rpc: VoteRequest<MetaRaftTypeConfig> = decode(&envelope.payload)?;
        let response = self
            .raft
            .vote(rpc)
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(RaftRpcAckV1 {
            payload: encode_wire(&response),
        }))
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<MetaRaftSnapshotRequestV1>,
    ) -> Result<tonic::Response<RaftFullSnapshotAckV1>, tonic::Status> {
        let request = request.into_inner();
        self.validate(
            &request.cluster_id,
            request.target_node_id,
            request.protocol_version,
        )?;
        let vote: VoteOf<MetaRaftTypeConfig> = decode(&request.vote)?;
        let meta: SnapshotMetaOf<MetaRaftTypeConfig> = decode(&request.snapshot_meta)?;
        let snapshot = SnapshotOf::<MetaRaftTypeConfig> {
            meta,
            snapshot: Cursor::new(request.snapshot_payload.to_vec()),
        };
        let response = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(RaftFullSnapshotAckV1 {
            response: encode_wire(&response),
        }))
    }

    async fn transfer_leader(
        &self,
        request: tonic::Request<MetaRaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<RaftTransferLeaderAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        self.validate(
            &envelope.cluster_id,
            envelope.target_node_id,
            envelope.protocol_version,
        )?;
        let rpc: TransferLeaderRequest<MetaRaftTypeConfig> = decode(&envelope.payload)?;
        self.raft
            .handle_transfer_leader(rpc)
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(RaftTransferLeaderAckV1 {}))
    }
}

#[derive(Debug, Clone)]
pub struct MetaGrpcRaftNetworkFactory {
    cluster_id: Arc<str>,
}

impl MetaGrpcRaftNetworkFactory {
    pub fn new(cluster_id: impl Into<String>) -> Result<Self, MetaRaftError> {
        Ok(Self {
            cluster_id: cluster_identity(cluster_id)?,
        })
    }
}

impl RaftNetworkFactory<MetaRaftTypeConfig> for MetaGrpcRaftNetworkFactory {
    type Network = MetaGrpcRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        MetaGrpcRaftNetwork {
            cluster_id: self.cluster_id.clone(),
            target,
            endpoint: normalize_grpc_endpoint(node.addr.clone()),
            client: None,
        }
    }
}

#[derive(Debug)]
pub struct MetaGrpcRaftNetwork {
    cluster_id: Arc<str>,
    target: u64,
    endpoint: String,
    client: Option<MetaRaftInternalClient<Channel>>,
}

fn network_error(message: impl ToString) -> RPCError<MetaRaftTypeConfig> {
    RPCError::Network(NetworkError::from_string(message))
}

impl MetaGrpcRaftNetwork {
    fn envelope(&self, rpc: &impl Serialize) -> MetaRaftRpcEnvelopeV1 {
        MetaRaftRpcEnvelopeV1 {
            cluster_id: self.cluster_id.to_string(),
            target_node_id: self.target,
            protocol_version: META_RAFT_PROTOCOL_VERSION,
            payload: encode_wire(rpc),
        }
    }

    async fn call<Req, Resp, Fut>(
        &mut self,
        payload: Req,
        option: RPCOption,
        send: impl FnOnce(MetaRaftInternalClient<Channel>, tonic::Request<Req>) -> Fut,
    ) -> Result<Resp, RPCError<MetaRaftTypeConfig>>
    where
        Req: Message,
        Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    {
        if self.client.is_none() {
            let endpoint = Endpoint::from_shared(self.endpoint.clone()).map_err(network_error)?;
            self.client = Some(
                MetaRaftInternalClient::new(endpoint.connect_lazy())
                    .max_decoding_message_size(META_RAFT_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(META_RAFT_MAX_MESSAGE_BYTES),
            );
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| network_error("meta client unavailable"))?
            .clone();
        let mut request = tonic::Request::new(payload);
        request.set_timeout(option.hard_ttl());
        match send(client, request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => {
                // Drop the channel on failure so a dead/restarted peer can reconnect.
                self.client = None;
                let message = format!(
                    "meta RPC to node {} at {} failed: {status}",
                    self.target, self.endpoint
                );
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::Cancelled
                ) {
                    Err(RPCError::Unreachable(Unreachable::from_string(message)))
                } else {
                    Err(network_error(message))
                }
            }
        }
    }
}

impl RaftNetworkV2<MetaRaftTypeConfig> for MetaGrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<MetaRaftTypeConfig>, RPCError<MetaRaftTypeConfig>> {
        let response = self
            .call(
                self.envelope(&rpc),
                option,
                |mut client, request| async move { client.append(request).await },
            )
            .await?;
        rmp_serde::from_slice(&response.payload).map_err(network_error)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<MetaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<MetaRaftTypeConfig>, RPCError<MetaRaftTypeConfig>> {
        let response = self
            .call(
                self.envelope(&rpc),
                option,
                |mut client, request| async move { client.vote(request).await },
            )
            .await?;
        rmp_serde::from_slice(&response.payload).map_err(network_error)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<MetaRaftTypeConfig>,
        snapshot: SnapshotOf<MetaRaftTypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<MetaRaftTypeConfig>, StreamingError<MetaRaftTypeConfig>> {
        let request = MetaRaftSnapshotRequestV1 {
            cluster_id: self.cluster_id.to_string(),
            target_node_id: self.target,
            protocol_version: META_RAFT_PROTOCOL_VERSION,
            vote: encode_wire(&vote),
            snapshot_meta: encode_wire(&snapshot.meta),
            snapshot_payload: snapshot.snapshot.into_inner().into(),
        };
        let response = tokio::select! {
            biased;
            reason = cancel => return Err(StreamingError::Closed(reason)),
            response = self.call(request, option, |mut client, request| async move { client.full_snapshot(request).await }) => response.map_err(StreamingError::from)?,
        };
        rmp_serde::from_slice(&response.response)
            .map_err(network_error)
            .map_err(StreamingError::from)
    }

    async fn transfer_leader(
        &mut self,
        rpc: TransferLeaderRequest<MetaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<(), RPCError<MetaRaftTypeConfig>> {
        self.call(
            self.envelope(&rpc),
            option,
            |mut client, request| async move { client.transfer_leader(request).await },
        )
        .await
        .map(|_| ())
    }
}
