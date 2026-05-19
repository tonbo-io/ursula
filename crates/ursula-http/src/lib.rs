use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{DefaultBodyLimit, OriginalUri, Path, RawQuery, State};
use axum::http::header::{
    CACHE_CONTROL, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, HOST, IF_NONE_MATCH, LOCATION,
    TRANSFER_ENCODING,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::stream;
use openraft::BasicNode;
use openraft::Config;
use openraft::rt::WatchReceiver;
use tonic::transport::{Channel, Endpoint};
use tower::ServiceExt;
use ursula_raft::{
    ColdRaftGroupEngineFactory, DurableRaftGroupEngineFactory, DurableRaftLogStoreFactory,
    GrpcRaftNetworkFactory, RAFT_GRPC_APPEND_PATH, RAFT_GRPC_FORWARD_HTTP_WRITE_PATH,
    RAFT_GRPC_FULL_SNAPSHOT_PATH, RAFT_GRPC_GROUP_READ_PATH, RAFT_GRPC_GROUP_WRITE_PATH,
    RAFT_GRPC_MAX_MESSAGE_BYTES, RAFT_GRPC_VOTE_PATH, RaftGroupEngine, RaftGroupEngineFactory,
    RaftGroupHandleRegistry, RaftGroupLogStore, RaftGroupMetricsSnapshot, RaftGrpcService,
    RaftLogProgressSnapshot, raft_internal_proto,
};
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::{
    AppendBatchRequest, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, BootstrapStreamResponse, CloseStreamRequest, ColdStore,
    CreateStreamExternalRequest, CreateStreamRequest, CreateStreamResponse, DeleteSnapshotRequest,
    DeleteStreamRequest, ExternalPayloadRef, GroupEngine, GroupEngineCreateFuture,
    GroupEngineError, GroupEngineFactory, GroupEngineMetrics, HeadStreamRequest,
    InMemoryGroupEngineFactory, PlanColdFlushRequest, PlanGroupColdFlushRequest, ProducerRequest,
    PublishSnapshotRequest, ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest,
    ReadStreamResponse, RuntimeConfig, RuntimeError, RuntimeMailboxSnapshot,
    RuntimeMetricsSnapshot, ShardRuntime, WalGroupEngineFactory, new_external_payload_path,
};
use ursula_shard::{BucketStreamId, RaftGroupId};

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const HEADER_STREAM_CLOSED: &str = "stream-closed";
const HEADER_STREAM_CURSOR: &str = "stream-cursor";
const HEADER_STREAM_EXPIRES_AT: &str = "stream-expires-at";
const HEADER_STREAM_FORK_OFFSET: &str = "stream-fork-offset";
const HEADER_STREAM_FORKED_FROM: &str = "stream-forked-from";
const HEADER_STREAM_NEXT_OFFSET: &str = "stream-next-offset";
const HEADER_STREAM_SNAPSHOT_OFFSET: &str = "stream-snapshot-offset";
const HEADER_STREAM_SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";
const HEADER_STREAM_SEQ: &str = "stream-seq";
const HEADER_STREAM_TTL: &str = "stream-ttl";
const HEADER_STREAM_UP_TO_DATE: &str = "stream-up-to-date";
const HEADER_PRODUCER_ID: &str = "producer-id";
const HEADER_PRODUCER_EPOCH: &str = "producer-epoch";
const HEADER_PRODUCER_SEQ: &str = "producer-seq";
const HEADER_PREFER: &str = "prefer";
const HEADER_X_CONTENT_TYPE_OPTIONS: &str = "x-content-type-options";
const HEADER_CROSS_ORIGIN_RESOURCE_POLICY: &str = "cross-origin-resource-policy";
const HEADER_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";
const HEADER_URSULA_FORWARD_HOP: &str = "x-ursula-forward-hop";
const APPEND_BATCH_MAX_ITEMS: usize = 512;
const APPEND_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_LONG_POLL_TIMEOUT_MS: u64 = 1_000;
const MAX_LONG_POLL_TIMEOUT_MS: u64 = 60_000;
const V1_DEFAULT_BUCKET: &str = "_default";

type BoxResponse = Box<Response>;

struct CreateStreamHttpResponseInput<'a> {
    response: CreateStreamResponse,
    stream_id: &'a BucketStreamId,
    content_type: &'a str,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    producer: Option<&'a ProducerRequest>,
    public_path: Option<&'a str>,
    request_headers: &'a HeaderMap,
}

#[derive(Clone)]
pub struct HttpState {
    runtime: ShardRuntime,
    raft_registry: Option<RaftGroupHandleRegistry>,
    client_write_router: Option<ClientWriteLeaderRouter>,
    http_metrics: Arc<HttpMetrics>,
}

impl HttpState {
    pub fn new(runtime: ShardRuntime) -> Self {
        Self {
            runtime,
            raft_registry: None,
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
        }
    }

    pub fn with_raft_registry(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
    ) -> Self {
        Self {
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
        }
    }

    pub fn with_static_raft_cluster(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
        peers: impl IntoIterator<Item = (u64, String)>,
    ) -> Self {
        Self {
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: Some(ClientWriteLeaderRouter::new(peers)),
            http_metrics: Arc::new(HttpMetrics::default()),
        }
    }

    pub fn runtime(&self) -> &ShardRuntime {
        &self.runtime
    }

    pub fn raft_registry(&self) -> Option<&RaftGroupHandleRegistry> {
        self.raft_registry.as_ref()
    }

    pub fn client_write_router(&self) -> Option<&ClientWriteLeaderRouter> {
        self.client_write_router.as_ref()
    }
}

#[derive(Debug, Default)]
struct HttpMetrics {
    sse_streams_opened: AtomicU64,
    sse_read_iterations: AtomicU64,
    sse_data_events: AtomicU64,
    sse_control_events: AtomicU64,
    sse_error_events: AtomicU64,
}

impl HttpMetrics {
    fn snapshot(&self) -> HttpMetricsSnapshot {
        HttpMetricsSnapshot {
            sse_streams_opened: self.sse_streams_opened.load(Ordering::Relaxed),
            sse_read_iterations: self.sse_read_iterations.load(Ordering::Relaxed),
            sse_data_events: self.sse_data_events.load(Ordering::Relaxed),
            sse_control_events: self.sse_control_events.load(Ordering::Relaxed),
            sse_error_events: self.sse_error_events.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct HttpMetricsSnapshot {
    sse_streams_opened: u64,
    sse_read_iterations: u64,
    sse_data_events: u64,
    sse_control_events: u64,
    sse_error_events: u64,
}

#[derive(Clone, Debug)]
pub struct ClientWriteLeaderRouter {
    peers: Arc<BTreeMap<u64, String>>,
    channels: Arc<Mutex<BTreeMap<u64, Channel>>>,
}

impl ClientWriteLeaderRouter {
    pub fn new(peers: impl IntoIterator<Item = (u64, String)>) -> Self {
        Self {
            peers: Arc::new(
                peers
                    .into_iter()
                    .map(|(node_id, url)| (node_id, url.trim_end_matches('/').to_owned()))
                    .collect(),
            ),
            channels: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn leader_base(&self, err: &RuntimeError) -> Option<(u64, String)> {
        let RuntimeError::GroupEngine {
            leader_hint: Some(leader_hint),
            ..
        } = err
        else {
            return None;
        };
        let leader_id = leader_hint.node_id?;
        let leader_base = self
            .peers
            .get(&leader_id)
            .or(leader_hint.address.as_ref())?;
        Some((leader_id, leader_base.trim_end_matches('/').to_owned()))
    }

    async fn forward_http_write(
        &self,
        err: &RuntimeError,
        method: Method,
        request_target: &str,
        request_headers: &HeaderMap,
        body: Bytes,
    ) -> Option<Response> {
        let (leader_id, leader_base) = self.leader_base(err)?;
        let channel = match self.leader_channel(leader_id, &leader_base).await {
            Ok(channel) => channel,
            Err(err) => {
                return Some(internal_forward_error_response(
                    leader_id,
                    format!("connect to raft leader {leader_base}: {err}"),
                ));
            }
        };
        let mut client =
            raft_internal_proto::raft_internal_client::RaftInternalClient::new(channel)
                .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES);
        let next_hop = forward_hop(request_headers).saturating_add(1);
        let mut headers = headers_to_proto(request_headers);
        headers.retain(|header| !header.name.eq_ignore_ascii_case(HEADER_URSULA_FORWARD_HOP));
        headers.push(raft_internal_proto::HttpHeaderV1 {
            name: HEADER_URSULA_FORWARD_HOP.to_owned(),
            value: next_hop.to_string().into_bytes(),
        });
        let response = client
            .forward_http_write(raft_internal_proto::HttpWriteRequestV1 {
                method: method.as_str().to_owned(),
                target: request_target.to_owned(),
                headers,
                body: body.to_vec(),
            })
            .await;
        match response {
            Ok(response) => Some(http_response_from_proto(response.into_inner())),
            Err(err) => Some(internal_forward_error_response(
                leader_id,
                format!("forward HTTP write to raft leader {leader_base}: {err}"),
            )),
        }
    }

    async fn leader_channel(&self, leader_id: u64, leader_base: &str) -> Result<Channel, String> {
        if let Some(channel) = self
            .channels
            .lock()
            .map_err(|_| "raft leader channel cache mutex poisoned".to_owned())?
            .get(&leader_id)
            .cloned()
        {
            return Ok(channel);
        }
        let endpoint = Endpoint::from_shared(leader_base.to_owned())
            .map_err(|err| format!("invalid leader endpoint: {err}"))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|err| format!("connect: {err}"))?;
        self.channels
            .lock()
            .map_err(|_| "raft leader channel cache mutex poisoned".to_owned())?
            .insert(leader_id, channel.clone());
        Ok(channel)
    }

    fn redirect_response(&self, err: &RuntimeError, request_target: &str) -> Option<Response> {
        let (leader_id, leader_base) = self.leader_base(err)?;
        let mut headers = HeaderMap::new();
        insert_default_response_headers(&mut headers);
        let leader_url = format!("{}{}", leader_base.trim_end_matches('/'), request_target);
        if let Ok(value) = HeaderValue::from_str(&leader_url) {
            headers.insert(LOCATION, value);
        } else {
            return None;
        }
        insert_u64_header(&mut headers, HEADER_URSULA_RAFT_LEADER_ID, leader_id);
        Some((StatusCode::TEMPORARY_REDIRECT, headers, err.to_string()).into_response())
    }
}

#[derive(Clone)]
struct HttpRaftGrpcService {
    raft: RaftGrpcService,
    state: HttpState,
}

impl HttpRaftGrpcService {
    fn new(registry: RaftGroupHandleRegistry, state: HttpState) -> Self {
        let cold_store = state.runtime().cold_store();
        Self {
            raft: RaftGrpcService::new(registry).with_cold_store(cold_store),
            state,
        }
    }
}

#[tonic::async_trait]
impl raft_internal_proto::raft_internal_server::RaftInternal for HttpRaftGrpcService {
    async fn append(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::append(&self.raft, request).await
    }

    async fn vote(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::vote(&self.raft, request).await
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<raft_internal_proto::RaftFullSnapshotRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftFullSnapshotAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::full_snapshot(&self.raft, request)
            .await
    }

    async fn forward_http_write(
        &self,
        request: tonic::Request<raft_internal_proto::HttpWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::HttpWriteResponseV1>, tonic::Status> {
        let request = request.into_inner();
        let method: Method = request
            .method
            .parse()
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid method: {err}")))?;
        let uri: Uri = request
            .target
            .parse()
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid target: {err}")))?;
        let mut builder = Request::builder().method(method).uri(uri);
        for header in request.headers {
            let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|err| {
                tonic::Status::invalid_argument(format!("invalid header name: {err}"))
            })?;
            if !should_forward_request_header(&name) {
                continue;
            }
            let value = HeaderValue::from_bytes(&header.value).map_err(|err| {
                tonic::Status::invalid_argument(format!("invalid header value: {err}"))
            })?;
            builder = builder.header(name, value);
        }
        let request = builder
            .body(Body::from(request.body))
            .map_err(|err| tonic::Status::invalid_argument(format!("build request: {err}")))?;
        let response = router_from_state(self.state.clone())
            .oneshot(request)
            .await
            .map_err(|err| tonic::Status::internal(format!("dispatch forwarded write: {err}")))?;
        let (parts, body) = response.into_parts();
        let body = to_bytes(body, MAX_HTTP_BODY_BYTES)
            .await
            .map_err(|err| tonic::Status::internal(format!("read forwarded response: {err}")))?;
        Ok(tonic::Response::new(
            raft_internal_proto::HttpWriteResponseV1 {
                status: parts.status.as_u16().into(),
                headers: response_headers_to_proto(&parts.headers),
                body: body.to_vec(),
            },
        ))
    }

    async fn group_write(
        &self,
        request: tonic::Request<raft_internal_proto::GroupWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupWriteResponseV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::group_write(&self.raft, request)
            .await
    }

    async fn group_read(
        &self,
        request: tonic::Request<raft_internal_proto::GroupReadRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupReadResponseV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::group_read(&self.raft, request)
            .await
    }
}

fn raft_grpc_service(
    state: HttpState,
    registry: RaftGroupHandleRegistry,
) -> raft_internal_proto::raft_internal_server::RaftInternalServer<HttpRaftGrpcService> {
    raft_internal_proto::raft_internal_server::RaftInternalServer::new(HttpRaftGrpcService::new(
        registry, state,
    ))
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

fn should_forward_request_header(name: &HeaderName) -> bool {
    name != CONNECTION && name != CONTENT_LENGTH && name != TRANSFER_ENCODING
}

fn should_forward_response_header(name: &HeaderName) -> bool {
    name != HOST && name != CONNECTION && name != CONTENT_LENGTH && name != TRANSFER_ENCODING
}

fn headers_to_proto(headers: &HeaderMap) -> Vec<raft_internal_proto::HttpHeaderV1> {
    headers
        .iter()
        .filter(|(name, _)| should_forward_request_header(name))
        .map(|(name, value)| raft_internal_proto::HttpHeaderV1 {
            name: name.as_str().to_owned(),
            value: value.as_bytes().to_vec(),
        })
        .collect()
}

fn response_headers_to_proto(headers: &HeaderMap) -> Vec<raft_internal_proto::HttpHeaderV1> {
    headers
        .iter()
        .filter(|(name, _)| should_forward_response_header(name))
        .map(|(name, value)| raft_internal_proto::HttpHeaderV1 {
            name: name.as_str().to_owned(),
            value: value.as_bytes().to_vec(),
        })
        .collect()
}

fn http_response_from_proto(response: raft_internal_proto::HttpWriteResponseV1) -> Response {
    let status = u16::try_from(response.status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let mut headers = HeaderMap::new();
    for header in response.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(header.name.as_bytes()),
            HeaderValue::from_bytes(&header.value),
        ) && should_forward_response_header(&name)
        {
            headers.insert(name, value);
        }
    }
    (status, headers, Bytes::from(response.body)).into_response()
}

fn internal_forward_error_response(leader_id: u64, message: String) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_u64_header(&mut headers, HEADER_URSULA_RAFT_LEADER_ID, leader_id);
    (StatusCode::BAD_GATEWAY, headers, message).into_response()
}

pub fn router(runtime: ShardRuntime) -> Router {
    router_from_state(HttpState::new(runtime))
}

pub fn router_with_raft_registry(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
) -> Router {
    router_from_state(HttpState::with_raft_registry(runtime, raft_registry))
}

pub fn router_with_static_raft_cluster(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
    peers: impl IntoIterator<Item = (u64, String)>,
) -> Router {
    router_from_state(HttpState::with_static_raft_cluster(
        runtime,
        raft_registry,
        peers,
    ))
}

fn router_from_state(state: HttpState) -> Router {
    let raft_registry = state.raft_registry.clone().unwrap_or_default();
    Router::new()
        .route("/__ursula/metrics", get(metrics))
        .route_service(
            RAFT_GRPC_APPEND_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_VOTE_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_FULL_SNAPSHOT_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_FORWARD_HTTP_WRITE_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_GROUP_WRITE_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_GROUP_READ_PATH,
            raft_grpc_service(state.clone(), raft_registry),
        )
        .route(
            "/__ursula/flush-cold/{bucket}/{stream}",
            post(flush_cold_stream),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/snapshot",
            post(trigger_raft_snapshot),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/purge",
            post(trigger_raft_purge),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/learners/{node_id}",
            post(add_raft_learner),
        )
        .route(
            "/v1/stream/{*path}",
            put(create_stream_v1)
                .post(append_stream_v1)
                .get(read_stream_v1)
                .delete(delete_stream_v1)
                .head(head_stream_v1),
        )
        .route("/{bucket}", put(create_bucket))
        .route("/{bucket}/{stream}/snapshot", get(read_latest_snapshot))
        .route(
            "/{bucket}/{stream}/snapshot/{snapshot_offset}",
            put(publish_snapshot)
                .get(read_snapshot)
                .delete(delete_snapshot),
        )
        .route("/{bucket}/{stream}/bootstrap", get(bootstrap_stream))
        .route(
            "/{bucket}/{stream}",
            put(create_stream)
                .post(append_stream)
                .get(read_stream)
                .delete(delete_stream)
                .head(head_stream),
        )
        .route("/{bucket}/{stream}/append-batch", post(append_batch))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(state)
}

#[derive(Debug, Clone)]
pub struct StaticGrpcRaftGroupEngineFactory {
    node_id: u64,
    peers: BTreeMap<u64, String>,
    initialize_membership: bool,
    initialize_membership_per_group: bool,
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
    log_stores: Option<DurableRaftLogStoreFactory>,
}

impl StaticGrpcRaftGroupEngineFactory {
    pub fn new(
        node_id: u64,
        peers: impl IntoIterator<Item = (u64, String)>,
        initialize_membership: bool,
        registry: RaftGroupHandleRegistry,
    ) -> Self {
        Self {
            node_id,
            peers: peers.into_iter().collect(),
            initialize_membership,
            initialize_membership_per_group: false,
            registry,
            cold_store: None,
            log_stores: None,
        }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }

    pub fn with_raft_log_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.log_stores = Some(DurableRaftLogStoreFactory::new(root));
        self
    }

    pub fn with_per_group_membership_initializers(mut self, enabled: bool) -> Self {
        self.initialize_membership_per_group = enabled;
        self
    }

    fn peer_nodes(&self) -> BTreeMap<u64, BasicNode> {
        self.peers
            .iter()
            .map(|(node_id, address)| (*node_id, BasicNode::new(address.clone())))
            .collect()
    }

    fn should_initialize_membership(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.initialize_membership {
            return false;
        }
        if !self.initialize_membership_per_group {
            return true;
        }
        let peer_count = self.peers.len();
        if peer_count == 0 {
            return false;
        }
        let initializer_index =
            usize::try_from(raft_group_id.0).expect("raft group id fits usize") % peer_count;
        self.peers
            .keys()
            .nth(initializer_index)
            .is_some_and(|node_id| *node_id == self.node_id)
    }
}

impl GroupEngineFactory for StaticGrpcRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ursula_shard::ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            if !self.peers.contains_key(&self.node_id) {
                return Err(GroupEngineError::new(format!(
                    "raft node {} is not present in static peer config",
                    self.node_id
                )));
            }
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 100,
                    election_timeout_min: 300,
                    election_timeout_max: 600,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let engine = if let Some(log_stores) = &self.log_stores {
                RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    log_stores.open(placement, metrics.clone())?,
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?
            } else {
                RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?
            };
            self.registry.register(placement, engine.raft_handle());
            if self.should_initialize_membership(placement.raft_group_id) {
                engine.initialize_membership(self.peer_nodes()).await?;
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

pub fn spawn_default_runtime(
    core_count: usize,
    raft_group_count: usize,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        InMemoryGroupEngineFactory::with_cold_store(cold_store.clone()),
        cold_store,
    )?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_wal_runtime(
    core_count: usize,
    raft_group_count: usize,
    wal_dir: impl Into<PathBuf>,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        WalGroupEngineFactory::with_cold_store(wal_dir, cold_store.clone()),
        cold_store,
    )?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_raft_memory_runtime(
    core_count: usize,
    raft_group_count: usize,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = match cold_store {
        Some(cold_store) => ShardRuntime::spawn_with_engine_factory_and_cold_store(
            config,
            ColdRaftGroupEngineFactory::new(cold_store.clone()),
            Some(cold_store),
        ),
        None => ShardRuntime::spawn_with_engine_factory(config, RaftGroupEngineFactory),
    }?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_static_grpc_raft_memory_runtime(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_cold_store(cold_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_memory_runtime_with_per_group_initializers(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(true)
    .with_cold_store(cold_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_runtime(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_cold_store(cold_store.clone())
    .with_raft_log_dir(raft_log_dir);
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_runtime_with_per_group_initializers(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(true)
    .with_cold_store(cold_store.clone())
    .with_raft_log_dir(raft_log_dir);
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    Ok((runtime, registry))
}

pub fn spawn_raft_runtime(
    core_count: usize,
    raft_group_count: usize,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        DurableRaftGroupEngineFactory::with_cold_store(raft_log_dir, cold_store.clone()),
        cold_store,
    )?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

fn cold_store_from_env() -> Result<Option<ursula_runtime::ColdStoreHandle>, RuntimeError> {
    ColdStore::from_env().map_err(|err| RuntimeError::ColdStoreConfig {
        message: err.to_string(),
    })
}

fn runtime_config_from_env(
    core_count: usize,
    raft_group_count: usize,
    cold_store_configured: bool,
) -> RuntimeConfig {
    let mut config = RuntimeConfig::new(core_count, raft_group_count);
    let live_read_max_waiters = env_usize("URSULA_LIVE_READ_MAX_WAITERS_PER_CORE", 65_536);
    config = config.with_live_read_max_waiters_per_core(if live_read_max_waiters == 0 {
        None
    } else {
        Some(u64::try_from(live_read_max_waiters).unwrap_or(u64::MAX))
    });
    if cold_store_configured {
        let max_hot_bytes = env_usize("URSULA_COLD_MAX_HOT_BYTES_PER_GROUP", 64 * 1024 * 1024);
        if max_hot_bytes > 0 {
            config = config.with_cold_max_hot_bytes_per_group(Some(
                u64::try_from(max_hot_bytes).unwrap_or(u64::MAX),
            ));
        }
    }
    config
}

pub fn spawn_cold_flush_worker_if_configured(runtime: &ShardRuntime) {
    if !runtime.has_cold_store() {
        return;
    }
    let interval_ms = env_usize("URSULA_COLD_FLUSH_INTERVAL_MS", 1_000);
    if interval_ms == 0 {
        return;
    }
    let min_hot_bytes = env_usize("URSULA_COLD_FLUSH_MIN_HOT_BYTES", 8 * 1024 * 1024);
    let max_flush_bytes = env_usize("URSULA_COLD_FLUSH_MAX_BYTES", 8 * 1024 * 1024);
    let max_concurrency = env_usize("URSULA_COLD_FLUSH_MAX_CONCURRENCY", 4).max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        loop {
            if let Err(err) = runtime
                .flush_cold_all_groups_once_bounded(
                    PlanGroupColdFlushRequest {
                        min_hot_bytes,
                        max_flush_bytes,
                    },
                    max_concurrency,
                )
                .await
            {
                eprintln!("cold flush worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

fn should_externalize_payload(state: &HttpState, payload_len: usize, allowed: bool) -> bool {
    allowed
        && payload_len > 0
        && state.runtime.has_cold_store()
        && payload_len >= env_usize("URSULA_EXTERNAL_PAYLOAD_MIN_BYTES", 1024 * 1024)
}

async fn stage_external_payload(
    state: &HttpState,
    stream_id: &BucketStreamId,
    payload: &[u8],
) -> Result<ExternalPayloadRef, Response> {
    let Some(cold_store) = state.runtime.cold_store() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "URSULA_COLD_BACKEND must be configured before externalizing payloads",
        )
            .into_response());
    };
    let s3_path = new_external_payload_path(stream_id);
    let object_size = cold_store
        .write_chunk(&s3_path, payload)
        .await
        .map_err(|err| {
            (
                StatusCode::BAD_GATEWAY,
                format!("write external payload object: {err}"),
            )
                .into_response()
        })?;
    Ok(ExternalPayloadRef {
        s3_path,
        payload_len: u64::try_from(payload.len()).expect("payload len fits u64"),
        object_size,
    })
}

async fn cleanup_external_payload(state: &HttpState, s3_path: &str) {
    let Some(cold_store) = state.runtime.cold_store() else {
        return;
    };
    let _ = cold_store.delete_chunk(s3_path).await;
}

fn create_stream_http_response(input: CreateStreamHttpResponseInput<'_>) -> Response {
    let CreateStreamHttpResponseInput {
        response,
        stream_id,
        content_type,
        stream_ttl_seconds,
        stream_expires_at_ms,
        producer,
        public_path,
        request_headers,
    } = input;
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, content_type);
    insert_offset(&mut headers, response.next_offset);
    if let Some(public_path) = public_path {
        insert_public_location(&mut headers, request_headers, public_path);
    } else {
        insert_location(&mut headers, stream_id);
    }
    insert_lifetime_headers(&mut headers, stream_ttl_seconds, stream_expires_at_ms);
    insert_producer_ack(&mut headers, producer);
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    let status = if response.already_exists {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    (status, headers).into_response()
}

fn append_http_response(response: AppendResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_offset(&mut headers, response.next_offset);
    insert_producer_ack(&mut headers, response.producer.as_ref());
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    let status = if response.producer.is_some() && !response.deduplicated {
        StatusCode::OK
    } else {
        StatusCode::NO_CONTENT
    };
    (status, headers).into_response()
}

async fn create_bucket(Path(_bucket): Path<String>) -> Response {
    StatusCode::CREATED.into_response()
}

async fn metrics(State(state): State<HttpState>) -> Response {
    let mut headers = HeaderMap::new();
    insert_content_type(&mut headers, "application/json");
    let raft_groups = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::metrics_snapshot)
        .unwrap_or_default();
    (
        StatusCode::OK,
        headers,
        render_metrics(
            state.runtime.metrics().snapshot(),
            state.runtime.mailbox_snapshot(),
            state.http_metrics.snapshot(),
            &raft_groups,
        ),
    )
        .into_response()
}

async fn flush_cold_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let min_hot_bytes = query
        .get("min_hot_bytes")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(1);
    let max_flush_bytes = query
        .get("max_bytes")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id,
            min_hot_bytes,
            max_flush_bytes,
        })
        .await
    {
        Ok(Some(response)) => {
            let mut headers = HeaderMap::new();
            insert_content_type(&mut headers, "application/json");
            (
                StatusCode::OK,
                headers,
                format!(
                    "{{\"hot_start_offset\":{},\"group_commit_index\":{}}}",
                    response.hot_start_offset, response.group_commit_index
                ),
            )
                .into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::POST,
                &request_target(&uri),
                &headers,
                Bytes::new(),
            )
            .await
        }
    }
}

async fn trigger_raft_snapshot(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
) -> Response {
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    let snapshot_log_id = raft.metrics().borrow_watched().last_applied;
    if let Err(err) = raft.trigger().snapshot().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger raft snapshot: {err}"),
        )
            .into_response();
    }
    if let Some(snapshot_log_id) = snapshot_log_id
        && let Err(err) = raft
            .wait(Some(Duration::from_secs(10)))
            .snapshot(snapshot_log_id, "admin snapshot trigger")
            .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("wait for raft snapshot: {err}"),
        )
            .into_response();
    }

    let metrics = raft.metrics().borrow_watched().clone();
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        format!(
            "{{\"raft_group_id\":{},\"snapshot_index\":{}}}",
            raft_group_id.0,
            optional_u64_json(metrics.snapshot.map(|log_id| log_id.index))
        ),
    )
        .into_response()
}

async fn trigger_raft_purge(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let Some(upto) = query
        .get("upto")
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return (StatusCode::BAD_REQUEST, "upto query parameter is required").into_response();
    };
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    if let Err(err) = raft.trigger().purge_log(upto).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger raft purge: {err}"),
        )
            .into_response();
    }
    if let Err(err) = raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| metrics.purged.map(|log_id| log_id.index) >= Some(upto),
            format!("admin purge to index {upto}"),
        )
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("wait for raft purge: {err}"),
        )
            .into_response();
    }
    let metrics = raft.metrics().borrow_watched().clone();
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        format!(
            "{{\"raft_group_id\":{},\"purged_index\":{}}}",
            raft_group_id.0,
            optional_u64_json(metrics.purged.map(|log_id| log_id.index))
        ),
    )
        .into_response()
}

async fn add_raft_learner(
    State(state): State<HttpState>,
    Path((raft_group_id, node_id)): Path<(u64, u64)>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let Some(address) = query.get("addr").filter(|value| !value.trim().is_empty()) else {
        return (StatusCode::BAD_REQUEST, "addr query parameter is required").into_response();
    };
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    match raft
        .add_learner(node_id, BasicNode::new(address.clone()), true)
        .await
    {
        Ok(response) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            format!(
                "{{\"raft_group_id\":{},\"node_id\":{},\"log_index\":{}}}",
                raft_group_id.0,
                node_id,
                response.log_id.index()
            ),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("add raft learner: {err}"),
        )
            .into_response(),
    }
}

fn parse_raft_group_id(raw: u64) -> Result<RaftGroupId, std::num::TryFromIntError> {
    u32::try_from(raw).map(RaftGroupId)
}

fn optional_u64_json(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}

async fn create_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    create_stream_by_id(state, request_target(&uri), stream_id, None, headers, body).await
}

async fn create_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    create_stream_by_id(
        state,
        request_target(&uri),
        stream_id,
        Some(format!("/v1/stream/{path}")),
        headers,
        body,
    )
    .await
}

async fn create_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    public_path: Option<String>,
    request_headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type_explicit = has_content_type(&request_headers);
    let forked_from = match stream_forked_from(&request_headers) {
        Ok(forked_from) => forked_from,
        Err(response) => return *response,
    };
    let fork_offset = match stream_fork_offset(&request_headers) {
        Ok(fork_offset) => fork_offset,
        Err(response) => return *response,
    };
    let mut content_type = request_content_type(&request_headers);
    if let Some(source_id) = forked_from.as_ref()
        && !content_type_explicit
    {
        match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: source_id.clone(),
                now_ms: unix_time_ms(),
            })
            .await
        {
            Ok(source) => content_type = source.content_type,
            Err(err) => return runtime_error_response(err),
        }
    }
    let (stream_ttl_seconds, stream_expires_at_ms) = match stream_lifetime(&request_headers) {
        Ok(lifetime) => lifetime,
        Err(response) => return *response,
    };
    let mut request = CreateStreamRequest::new(stream_id.clone(), content_type.clone());
    request.content_type_explicit = content_type_explicit;
    request.now_ms = unix_time_ms();
    request.initial_payload = match normalize_http_write_payload(&content_type, body.clone(), true)
    {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.close_after = stream_closed(&request_headers);
    request.stream_seq = stream_seq(&request_headers);
    request.stream_ttl_seconds = stream_ttl_seconds;
    request.stream_expires_at_ms = stream_expires_at_ms;
    request.forked_from = forked_from;
    request.fork_offset = fork_offset;
    let producer = match producer_request(&request_headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.producer = producer.clone();

    if should_externalize_payload(
        &state,
        request.initial_payload.len(),
        request.forked_from.is_none(),
    ) {
        return create_stream_external_by_id(
            state,
            request_target,
            request,
            public_path,
            request_headers,
            body.clone(),
            producer,
        )
        .await;
    }

    match state.runtime.create_stream(request).await {
        Ok(response) => create_stream_http_response(CreateStreamHttpResponseInput {
            response,
            stream_id: &stream_id,
            content_type: &content_type,
            stream_ttl_seconds,
            stream_expires_at_ms,
            producer: producer.as_ref(),
            public_path: public_path.as_deref(),
            request_headers: &request_headers,
        }),
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::PUT,
                &request_target,
                &request_headers,
                body.clone(),
            )
            .await
        }
    }
}

async fn create_stream_external_by_id(
    state: HttpState,
    request_target: String,
    mut request: CreateStreamRequest,
    public_path: Option<String>,
    request_headers: HeaderMap,
    body: Bytes,
    producer: Option<ProducerRequest>,
) -> Response {
    let stream_id = request.stream_id.clone();
    let content_type = request.content_type.clone();
    let stream_ttl_seconds = request.stream_ttl_seconds;
    let stream_expires_at_ms = request.stream_expires_at_ms;
    let payload = std::mem::take(&mut request.initial_payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request =
        CreateStreamExternalRequest::from_create_request(request, external_payload);

    match state.runtime.create_stream_external(external_request).await {
        Ok(response) => create_stream_http_response(CreateStreamHttpResponseInput {
            response,
            stream_id: &stream_id,
            content_type: &content_type,
            stream_ttl_seconds,
            stream_expires_at_ms,
            producer: producer.as_ref(),
            public_path: public_path.as_deref(),
            request_headers: &request_headers,
        }),
        Err(err) => {
            cleanup_external_payload(&state, &external_path).await;
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::PUT,
                &request_target,
                &request_headers,
                body,
            )
            .await
        }
    }
}

async fn append_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    append_stream_by_id(state, request_target(&uri), stream_id, headers, body).await
}

async fn append_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    append_stream_by_id(state, request_target(&uri), stream_id, headers, body).await
}

async fn append_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let close_after = stream_closed(&headers);

    if body.is_empty() && close_after {
        let producer = match producer_request(&headers) {
            Ok(producer) => producer,
            Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
        };
        return match state
            .runtime
            .close_stream(CloseStreamRequest {
                stream_id,
                stream_seq: stream_seq(&headers),
                producer: producer.clone(),
                now_ms: unix_time_ms(),
            })
            .await
        {
            Ok(response) => {
                let mut headers = HeaderMap::new();
                insert_default_response_headers(&mut headers);
                insert_offset(&mut headers, response.next_offset);
                insert_producer_ack(&mut headers, producer.as_ref());
                insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
                (StatusCode::NO_CONTENT, headers).into_response()
            }
            Err(err) => {
                runtime_error_or_leader_redirect_async(
                    &state,
                    err,
                    Method::POST,
                    &request_target,
                    &headers,
                    Bytes::new(),
                )
                .await
            }
        };
    }
    if !body.is_empty() && !has_content_type(&headers) {
        return (
            StatusCode::BAD_REQUEST,
            "append with a body must include content type",
        )
            .into_response();
    }

    let content_type = request_content_type(&headers);
    let payload = match normalize_http_write_payload(&content_type, body.clone(), false) {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let mut request = AppendRequest::from_bytes(stream_id, payload);
    request.content_type = content_type;
    request.close_after = close_after;
    request.stream_seq = stream_seq(&headers);
    request.now_ms = unix_time_ms();
    let producer = match producer_request(&headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.producer = producer.clone();

    if should_externalize_payload(&state, request.payload.len(), true) {
        return append_stream_external_by_id(state, request_target, request, headers, body).await;
    }

    match state.runtime.append(request).await {
        Ok(response) => append_http_response(response),
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::POST,
                &request_target,
                &headers,
                body.clone(),
            )
            .await
        }
    }
}

async fn append_stream_external_by_id(
    state: HttpState,
    request_target: String,
    mut request: AppendRequest,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = request.stream_id.clone();
    let payload = std::mem::take(&mut request.payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request = AppendExternalRequest::from_append_request(request, external_payload);
    match state.runtime.append_external(external_request).await {
        Ok(response) => append_http_response(response),
        Err(err) => {
            cleanup_external_payload(&state, &external_path).await;
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::POST,
                &request_target,
                &headers,
                body,
            )
            .await
        }
    }
}

async fn append_batch(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > APPEND_BATCH_MAX_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "append batch is too large").into_response();
    }
    let producer = match producer_request(&headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let minimal_ack = prefers_minimal_response(&headers);
    let payloads = match parse_append_batch(&body) {
        Ok(payloads) => payloads,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    if payloads.len() > APPEND_BATCH_MAX_ITEMS {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "append batch contains too many items",
        )
            .into_response();
    }

    let stream_id = BucketStreamId::new(bucket, stream);
    let content_type = request_content_type(&headers);
    let mut request = AppendBatchRequest::new(stream_id, payloads);
    request.content_type = content_type;
    request.producer = producer.clone();
    request.now_ms = unix_time_ms();
    let response = match state.runtime.append_batch(request).await {
        Ok(response) => response,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::POST,
                &request_target(&uri),
                &headers,
                body.clone(),
            )
            .await;
        }
    };

    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_producer_ack(&mut headers, producer.as_ref());
    if minimal_ack && response.items.iter().all(Result::is_ok) {
        return (StatusCode::NO_CONTENT, headers).into_response();
    }

    insert_content_type(&mut headers, "application/json");
    let body = render_batch_results(&response.items);
    (StatusCode::OK, headers, body).into_response()
}

async fn delete_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    delete_stream_by_id(state, request_target(&uri), stream_id, headers).await
}

async fn delete_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    delete_stream_by_id(state, request_target(&uri), stream_id, headers).await
}

async fn delete_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
) -> Response {
    match state
        .runtime
        .delete_stream(DeleteStreamRequest { stream_id })
        .await
    {
        Ok(_) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::DELETE,
                &request_target,
                &headers,
                Bytes::new(),
            )
            .await
        }
    }
}

async fn head_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    head_stream_by_id(state, request_target(&uri), stream_id, headers).await
}

async fn head_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    head_stream_by_id(state, request_target(&uri), stream_id, headers).await
}

async fn head_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    request_headers: HeaderMap,
) -> Response {
    match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id,
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_content_type(&mut headers, &response.content_type);
            insert_offset(&mut headers, response.tail_offset);
            insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
            insert_cache_control(&mut headers, "no-store");
            insert_lifetime_headers(
                &mut headers,
                response.stream_ttl_seconds,
                response.stream_expires_at_ms,
            );
            if let Some(snapshot_offset) = response.snapshot_offset {
                insert_snapshot_offset(&mut headers, snapshot_offset);
            }
            if response.closed {
                insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
            }
            (StatusCode::OK, headers).into_response()
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::HEAD,
                &request_target,
                &request_headers,
                Bytes::new(),
            )
            .await
        }
    }
}

async fn read_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    read_stream_by_id(state, request_target(&uri), stream_id, headers, raw_query).await
}

async fn read_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    read_stream_by_id(state, request_target(&uri), stream_id, headers, raw_query).await
}

async fn read_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
    raw_query: Option<String>,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let live_mode = query.get("live").map(String::as_str);
    let offset_is_now = query.get("offset").is_some_and(|offset| offset == "now");
    if live_mode.is_some() && !query.contains_key("offset") {
        return (StatusCode::BAD_REQUEST, "live reads require offset").into_response();
    }
    if matches!(live_mode, Some("sse" | "long-poll"))
        && let Err(err) = state
            .runtime
            .require_local_live_read_owner(&stream_id)
            .await
    {
        return runtime_error_or_leader_redirect_async(
            &state,
            err,
            Method::GET,
            &request_target,
            &headers,
            Bytes::new(),
        )
        .await;
    }
    let offset = match read_offset(
        &state,
        &stream_id,
        query.get("offset").map(String::as_str),
        &request_target,
        &headers,
    )
    .await
    {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let max_len = query
        .get("max_bytes")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(usize::MAX);

    match live_mode {
        Some("sse") => {
            return sse_stream(
                state,
                request_target,
                stream_id,
                offset,
                max_len,
                &query,
                headers,
            )
            .await;
        }
        Some("long-poll") => {
            return long_poll_stream(
                state,
                request_target,
                stream_id,
                offset,
                max_len,
                &query,
                headers,
            )
            .await;
        }
        Some(_) => return (StatusCode::BAD_REQUEST, "invalid live mode").into_response(),
        None => {}
    }

    match state
        .runtime
        .read_stream(ReadStreamRequest {
            stream_id,
            offset,
            max_len,
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(response) if offset_is_now => offset_now_response(response),
        Ok(response) => read_response(response, &headers, None),
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::GET,
                &request_target,
                &headers,
                Bytes::new(),
            )
            .await
        }
    }
}

async fn publish_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream, snapshot_offset)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let stream_id = BucketStreamId::new(bucket, stream);
    let request = PublishSnapshotRequest {
        stream_id,
        snapshot_offset,
        content_type: request_content_type(&headers),
        payload: body.clone(),
        now_ms: unix_time_ms(),
    };
    match state.runtime.publish_snapshot(request).await {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_snapshot_offset(&mut headers, response.snapshot_offset);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::PUT,
                &request_target(&uri),
                &headers,
                body.clone(),
            )
            .await
        }
    }
}

async fn read_latest_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let stream_id = BucketStreamId::new(bucket.clone(), stream.clone());
    let head = match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id,
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(head) => head,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::GET,
                &request_target(&uri),
                &headers,
                Bytes::new(),
            )
            .await;
        }
    };
    let Some(snapshot_offset) = head.snapshot_offset else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response_headers = HeaderMap::new();
    insert_default_response_headers(&mut response_headers);
    insert_snapshot_offset(&mut response_headers, snapshot_offset);
    let path = format!("/{bucket}/{stream}/snapshot/{snapshot_offset:020}");
    insert_public_location(&mut response_headers, &headers, &path);
    (StatusCode::TEMPORARY_REDIRECT, response_headers).into_response()
}

async fn read_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream, snapshot_offset)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .read_snapshot(ReadSnapshotRequest {
            stream_id,
            snapshot_offset: Some(snapshot_offset),
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(response) => snapshot_response(response),
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::GET,
                &request_target(&uri),
                &headers,
                Bytes::new(),
            )
            .await
        }
    }
}

async fn delete_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream, snapshot_offset)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .delete_snapshot(DeleteSnapshotRequest {
            stream_id,
            snapshot_offset,
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(()) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::DELETE,
                &request_target(&uri),
                &headers,
                Bytes::new(),
            )
            .await
        }
    }
}

async fn bootstrap_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    if query.contains_key("live") {
        return (
            StatusCode::BAD_REQUEST,
            "bootstrap does not support live reads",
        )
            .into_response();
    }
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .bootstrap_stream(BootstrapStreamRequest {
            stream_id,
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(response) => bootstrap_response(response),
        Err(err) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::GET,
                &request_target(&uri),
                &headers,
                Bytes::new(),
            )
            .await
        }
    }
}

fn parse_snapshot_offset(raw: &str) -> Result<u64, BoxResponse> {
    if raw == "-1" {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "invalid snapshot offset").into_response(),
        ));
    }
    raw.parse::<u64>()
        .map_err(|_| Box::new((StatusCode::BAD_REQUEST, "invalid snapshot offset").into_response()))
}

async fn read_offset(
    state: &HttpState,
    stream_id: &BucketStreamId,
    raw: Option<&str>,
    request_target: &str,
    request_headers: &HeaderMap,
) -> Result<u64, BoxResponse> {
    match raw {
        Some("-1") => Ok(0),
        Some("now") => match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: unix_time_ms(),
            })
            .await
        {
            Ok(head) => Ok(head.tail_offset),
            Err(err) => {
                let response = runtime_error_or_leader_redirect_async(
                    state,
                    err,
                    Method::GET,
                    request_target,
                    request_headers,
                    Bytes::new(),
                )
                .await;
                Err(Box::new(response))
            }
        },
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| Box::new((StatusCode::BAD_REQUEST, "invalid offset").into_response())),
        None => Ok(0),
    }
}

async fn long_poll_stream(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    offset: u64,
    max_len: usize,
    query: &HashMap<String, String>,
    headers: HeaderMap,
) -> Response {
    let timeout_ms = long_poll_timeout_ms(query);
    let read = state.runtime.wait_read_stream(ReadStreamRequest {
        stream_id: stream_id.clone(),
        offset,
        max_len: max_len.max(1),
        now_ms: unix_time_ms(),
    });
    match tokio::time::timeout(Duration::from_millis(timeout_ms), read).await {
        Ok(Ok(response)) if response.payload.is_empty() && response.up_to_date => {
            long_poll_no_content_response(&response, query.get("cursor").map(String::as_str))
        }
        Ok(Ok(response)) => read_response(
            response,
            &headers,
            Some(query.get("cursor").map(String::as_str).unwrap_or("")),
        ),
        Ok(Err(err)) => {
            runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::GET,
                &request_target,
                &headers,
                Bytes::new(),
            )
            .await
        }
        Err(_) => match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: unix_time_ms(),
            })
            .await
        {
            Ok(head) => {
                let mut headers = HeaderMap::new();
                insert_default_response_headers(&mut headers);
                insert_offset(&mut headers, head.tail_offset);
                insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
                if head.closed {
                    insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
                } else {
                    insert_cursor(
                        &mut headers,
                        response_cursor(head.tail_offset, query.get("cursor").map(String::as_str)),
                    );
                }
                (StatusCode::NO_CONTENT, headers).into_response()
            }
            Err(err) => {
                runtime_error_or_leader_redirect_async(
                    &state,
                    err,
                    Method::GET,
                    &request_target,
                    &headers,
                    Bytes::new(),
                )
                .await
            }
        },
    }
}

#[derive(Debug, Clone)]
struct SseState {
    runtime: ShardRuntime,
    http_metrics: Arc<HttpMetrics>,
    stream_id: BucketStreamId,
    offset: u64,
    max_len: usize,
    encode_base64: bool,
    cursor: Option<String>,
    initial_read: bool,
}

async fn sse_stream(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    offset: u64,
    max_len: usize,
    query: &HashMap<String, String>,
    headers: HeaderMap,
) -> Response {
    let head = match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: unix_time_ms(),
        })
        .await
    {
        Ok(head) => head,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(
                &state,
                err,
                Method::GET,
                &request_target,
                &headers,
                Bytes::new(),
            )
            .await;
        }
    };

    let encode_base64 = should_base64_encode_sse_data(&head.content_type);
    state
        .http_metrics
        .sse_streams_opened
        .fetch_add(1, Ordering::Relaxed);
    let sse_state = SseState {
        runtime: state.runtime,
        http_metrics: state.http_metrics,
        stream_id,
        offset,
        max_len: max_len.max(1),
        encode_base64,
        cursor: query.get("cursor").cloned(),
        initial_read: true,
    };
    let body_stream = stream::unfold(Some(sse_state), |state| async move {
        let mut state = match state {
            Some(state) => state,
            None => return None,
        };
        state
            .http_metrics
            .sse_read_iterations
            .fetch_add(1, Ordering::Relaxed);
        let read_request = ReadStreamRequest {
            stream_id: state.stream_id.clone(),
            offset: state.offset,
            max_len: state.max_len,
            now_ms: unix_time_ms(),
        };
        let read = if state.initial_read {
            state.initial_read = false;
            state.runtime.read_stream(read_request).await
        } else {
            state.runtime.wait_read_stream(read_request).await
        };
        let read = match read {
            Ok(read) => read,
            Err(err) => {
                state
                    .http_metrics
                    .sse_error_events
                    .fetch_add(1, Ordering::Relaxed);
                let event = format!("event: error\ndata:{}\n\n", sse_safe_line(&err.to_string()));
                return Some((Ok::<Bytes, Infallible>(Bytes::from(event)), None));
            }
        };

        state.offset = read.next_offset;
        let done = read.closed && read.up_to_date;
        if !read.payload.is_empty() {
            state
                .http_metrics
                .sse_data_events
                .fetch_add(1, Ordering::Relaxed);
        }
        state
            .http_metrics
            .sse_control_events
            .fetch_add(1, Ordering::Relaxed);
        let event = render_sse_read(&read, state.encode_base64, state.cursor.as_deref());
        let next = if done { None } else { Some(state) };
        Some((Ok::<Bytes, Infallible>(Bytes::from(event)), next))
    });

    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, "text/event-stream");
    insert_cache_control(&mut headers, "no-cache");
    if encode_base64 {
        insert_static(&mut headers, HEADER_STREAM_SSE_DATA_ENCODING, "base64");
    }
    (StatusCode::OK, headers, Body::from_stream(body_stream)).into_response()
}

fn long_poll_timeout_ms(query: &HashMap<String, String>) -> u64 {
    query
        .get("timeout_ms")
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_LONG_POLL_TIMEOUT_MS)
        .clamp(1, MAX_LONG_POLL_TIMEOUT_MS)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn v1_stream_id(path: &str) -> Result<BucketStreamId, BoxResponse> {
    if path.is_empty() {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "stream path must not be empty").into_response(),
        ));
    }
    if path.contains('\0')
        || path
            .split('/')
            .any(|segment| segment == ".." || segment.is_empty())
    {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "stream path contains invalid characters",
            )
                .into_response(),
        ));
    }
    let (bucket, stream) = path.split_once('/').unwrap_or((V1_DEFAULT_BUCKET, path));
    Ok(BucketStreamId::new(bucket, stream))
}

fn parse_query(raw: Option<&str>) -> Result<HashMap<String, String>, BoxResponse> {
    let mut query = HashMap::new();
    let Some(raw) = raw else {
        return Ok(query);
    };
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "offset" && query.contains_key("offset") {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "multiple offset parameters").into_response(),
            ));
        }
        query.insert(key.to_owned(), value.to_owned());
    }
    Ok(query)
}

fn request_content_type(headers: &HeaderMap) -> String {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(normalize_content_type)
        .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned())
}

fn stream_forked_from(headers: &HeaderMap) -> Result<Option<BucketStreamId>, BoxResponse> {
    let Some(raw) = header_value(headers, HEADER_STREAM_FORKED_FROM) else {
        return Ok(None);
    };
    let path = raw
        .strip_prefix("/v1/stream/")
        .or_else(|| raw.strip_prefix("v1/stream/"))
        .unwrap_or(raw)
        .trim_start_matches('/');
    v1_stream_id(path).map(Some).map_err(|_| {
        Box::new((StatusCode::BAD_REQUEST, "invalid stream-forked-from").into_response())
    })
}

fn stream_fork_offset(headers: &HeaderMap) -> Result<Option<u64>, BoxResponse> {
    let Some(raw) = header_value(headers, HEADER_STREAM_FORK_OFFSET) else {
        return Ok(None);
    };
    let normalized = raw.replace('_', "");
    normalized.parse::<u64>().map(Some).map_err(|_| {
        Box::new((StatusCode::BAD_REQUEST, "invalid stream-fork-offset").into_response())
    })
}

fn has_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.trim().is_empty())
}

fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join("; ")
}

fn stream_lifetime(headers: &HeaderMap) -> Result<(Option<u64>, Option<u64>), BoxResponse> {
    let ttl = header_value(headers, HEADER_STREAM_TTL)
        .map(parse_stream_ttl)
        .transpose()
        .map_err(|message| Box::new((StatusCode::BAD_REQUEST, message).into_response()))?;
    let expires_at = header_value(headers, HEADER_STREAM_EXPIRES_AT)
        .map(parse_stream_expires_at)
        .transpose()
        .map_err(|message| Box::new((StatusCode::BAD_REQUEST, message).into_response()))?;
    if ttl.is_some() && expires_at.is_some() {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "stream-ttl and stream-expires-at cannot be provided together",
            )
                .into_response(),
        ));
    }
    Ok((ttl, expires_at))
}

fn parse_stream_ttl(raw: &str) -> Result<u64, String> {
    if raw.is_empty() {
        return Err("stream-ttl must not be empty".to_owned());
    }
    if raw.len() > 1 && raw.starts_with('0') {
        return Err("stream-ttl must not contain leading zeros".to_owned());
    }
    if !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("stream-ttl must be a non-negative decimal integer".to_owned());
    }
    raw.parse::<u64>()
        .map_err(|_| "stream-ttl is too large".to_owned())
}

fn parse_stream_expires_at(raw: &str) -> Result<u64, String> {
    let expires_at = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| "stream-expires-at must be an RFC3339 timestamp".to_owned())?;
    u64::try_from(expires_at.timestamp_millis())
        .map_err(|_| "stream-expires-at must not be before the Unix epoch".to_owned())
}

fn stream_closed(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn stream_seq(headers: &HeaderMap) -> Option<String> {
    headers
        .get(HEADER_STREAM_SEQ)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

fn producer_request(headers: &HeaderMap) -> Result<Option<ProducerRequest>, String> {
    let producer_id = header_value(headers, HEADER_PRODUCER_ID);
    let producer_epoch = header_value(headers, HEADER_PRODUCER_EPOCH);
    let producer_seq = header_value(headers, HEADER_PRODUCER_SEQ);
    let present = [
        producer_id.is_some(),
        producer_epoch.is_some(),
        producer_seq.is_some(),
    ];
    if present.iter().all(|value| !*value) {
        return Ok(None);
    }
    if !present.iter().all(|value| *value) {
        return Err(
            "producer-id, producer-epoch, and producer-seq must be provided together".to_owned(),
        );
    }

    let producer_id = producer_id.expect("checked present");
    if producer_id.trim().is_empty() {
        return Err("producer-id must not be empty".to_owned());
    }
    Ok(Some(ProducerRequest {
        producer_id: producer_id.to_owned(),
        producer_epoch: parse_producer_integer(
            HEADER_PRODUCER_EPOCH,
            producer_epoch.expect("checked present"),
        )?,
        producer_seq: parse_producer_integer(
            HEADER_PRODUCER_SEQ,
            producer_seq.expect("checked present"),
        )?,
    }))
}

fn prefers_minimal_response(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_PREFER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("return=minimal"))
        })
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

fn parse_producer_integer(name: &str, raw: &str) -> Result<u64, String> {
    const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    let value = raw
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a non-negative integer"))?;
    if value > MAX_JS_SAFE_INTEGER {
        return Err(format!("{name} must be <= {MAX_JS_SAFE_INTEGER}"));
    }
    Ok(value)
}

fn runtime_error_response(err: RuntimeError) -> Response {
    let status = runtime_error_status(&err);
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_producer_error_headers(&mut headers, &err);
    insert_stream_error_headers(&mut headers, &err);
    insert_stream_error_offset(&mut headers, &err);
    (status, headers, err.to_string()).into_response()
}

async fn runtime_error_or_leader_redirect_async(
    state: &HttpState,
    err: RuntimeError,
    method: Method,
    request_target: &str,
    request_headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let Some(router) = state.client_write_router() else {
        return runtime_error_response(err);
    };
    if method == Method::GET || method == Method::HEAD {
        return router
            .redirect_response(&err, request_target)
            .unwrap_or_else(|| runtime_error_response(err));
    }
    if forward_hop(request_headers) >= 4 {
        return runtime_error_response(err);
    }
    router
        .forward_http_write(&err, method, request_target, request_headers, body)
        .await
        .unwrap_or_else(|| runtime_error_response(err))
}

fn request_target(uri: &Uri) -> String {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned())
}

fn forward_hop(headers: &HeaderMap) -> u8 {
    headers
        .get(HEADER_URSULA_FORWARD_HOP)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(0)
}

fn runtime_error_status(err: &RuntimeError) -> StatusCode {
    match err {
        RuntimeError::EmptyAppend
        | RuntimeError::InvalidRaftGroup { .. }
        | RuntimeError::SnapshotPlacementMismatch { .. } => StatusCode::BAD_REQUEST,
        RuntimeError::InvalidConfig(_)
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::ColdStoreIo { .. }
        | RuntimeError::MailboxClosed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        RuntimeError::ResponseDropped { .. } | RuntimeError::SpawnCoreThread { .. } => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        RuntimeError::LiveReadBackpressure { .. } => StatusCode::SERVICE_UNAVAILABLE,
        RuntimeError::GroupEngine { message, .. } => stream_error_status(message),
    }
}

fn stream_error_status(message: &str) -> StatusCode {
    if message.contains("ColdBackpressure") {
        StatusCode::SERVICE_UNAVAILABLE
    } else if message.contains("StreamGone") {
        StatusCode::GONE
    } else if message.contains("NotFound") {
        StatusCode::NOT_FOUND
    } else if message.contains("ContentTypeMismatch")
        || message.contains("StreamAlreadyExistsConflict")
        || message.contains("StreamClosed")
        || message.contains("StreamSeqConflict")
        || message.contains("SnapshotConflict")
        || message.contains("ProducerSeqConflict")
    {
        StatusCode::CONFLICT
    } else if message.contains("ProducerEpochStale") {
        StatusCode::FORBIDDEN
    } else if message.contains("Invalid") || message.contains("EmptyAppend") {
        StatusCode::BAD_REQUEST
    } else if message.contains("OffsetOutOfRange") {
        StatusCode::RANGE_NOT_SATISFIABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn insert_offset(headers: &mut HeaderMap, next_offset: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{next_offset:020}")) {
        headers.insert(HEADER_STREAM_NEXT_OFFSET, value);
    }
}

fn insert_snapshot_offset(headers: &mut HeaderMap, snapshot_offset: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{snapshot_offset:020}")) {
        headers.insert(HEADER_STREAM_SNAPSHOT_OFFSET, value);
    }
}

fn insert_cursor(headers: &mut HeaderMap, cursor: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{cursor:020}")) {
        headers.insert(HEADER_STREAM_CURSOR, value);
    }
}

fn response_cursor(next_offset: u64, request_cursor: Option<&str>) -> u64 {
    let Some(request_cursor) = request_cursor else {
        return next_offset;
    };
    let Ok(request_cursor) = request_cursor.parse::<u64>() else {
        return next_offset;
    };
    if request_cursor >= next_offset {
        request_cursor.saturating_add(1)
    } else {
        next_offset
    }
}

fn insert_content_type(headers: &mut HeaderMap, content_type: &str) {
    if let Ok(value) = HeaderValue::from_str(content_type) {
        headers.insert(CONTENT_TYPE, value);
    }
}

fn insert_default_response_headers(headers: &mut HeaderMap) {
    insert_static(headers, HEADER_X_CONTENT_TYPE_OPTIONS, "nosniff");
    insert_static(headers, HEADER_CROSS_ORIGIN_RESOURCE_POLICY, "cross-origin");
}

fn insert_cache_control(headers: &mut HeaderMap, value: &'static str) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(value));
}

fn insert_lifetime_headers(
    headers: &mut HeaderMap,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
) {
    if let Some(ttl) = stream_ttl_seconds {
        insert_u64_header(headers, HEADER_STREAM_TTL, ttl);
    }
    if let Some(expires_at_ms) = stream_expires_at_ms
        && let Some(expires_at) = DateTime::<Utc>::from_timestamp_millis(
            i64::try_from(expires_at_ms).expect("expires_at_ms fits i64"),
        )
        && let Ok(value) =
            HeaderValue::from_str(&expires_at.to_rfc3339_opts(SecondsFormat::Millis, true))
    {
        headers.insert(HEADER_STREAM_EXPIRES_AT, value);
    }
}

fn insert_producer_ack(headers: &mut HeaderMap, producer: Option<&ProducerRequest>) {
    let Some(producer) = producer else {
        return;
    };
    insert_u64_header(headers, HEADER_PRODUCER_EPOCH, producer.producer_epoch);
    insert_u64_header(headers, HEADER_PRODUCER_SEQ, producer.producer_seq);
}

fn insert_producer_error_headers(headers: &mut HeaderMap, err: &RuntimeError) {
    let RuntimeError::GroupEngine { message, .. } = err else {
        return;
    };
    if let Some(current_epoch) = parse_u64_after(message, "current epoch is ") {
        insert_u64_header(headers, HEADER_PRODUCER_EPOCH, current_epoch);
    }
    if let Some(expected_seq) = parse_u64_after(message, "expected sequence ") {
        insert_u64_header(headers, "producer-expected-seq", expected_seq);
    }
    if let Some(received_seq) = parse_u64_after(message, "received ") {
        insert_u64_header(headers, "producer-received-seq", received_seq);
    }
}

fn insert_stream_error_headers(headers: &mut HeaderMap, err: &RuntimeError) {
    let RuntimeError::GroupEngine { message, .. } = err else {
        return;
    };
    if message.contains("StreamClosed") || message.contains(" is closed") {
        insert_static(headers, HEADER_STREAM_CLOSED, "true");
    }
}

fn insert_stream_error_offset(headers: &mut HeaderMap, err: &RuntimeError) {
    let RuntimeError::GroupEngine {
        next_offset: Some(next_offset),
        ..
    } = err
    else {
        return;
    };
    insert_offset(headers, *next_offset);
}

fn parse_u64_after(message: &str, marker: &str) -> Option<u64> {
    let start = message.find(marker)? + marker.len();
    let digits = message[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn insert_u64_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        headers.insert(name, value);
    }
}

fn insert_location(headers: &mut HeaderMap, stream_id: &BucketStreamId) {
    if let Ok(value) = HeaderValue::from_str(&format!("/{stream_id}")) {
        headers.insert(LOCATION, value);
    }
}

fn insert_public_location(headers: &mut HeaderMap, request_headers: &HeaderMap, path: &str) {
    let location = request_headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|host| !host.trim().is_empty())
        .map(|host| format!("http://{host}{path}"))
        .unwrap_or_else(|| path.to_owned());
    if let Ok(value) = HeaderValue::from_str(&location) {
        headers.insert(LOCATION, value);
    }
}

fn insert_static(headers: &mut HeaderMap, name: &'static str, value: &'static str) {
    headers.insert(name, HeaderValue::from_static(value));
}

fn parse_append_batch(body: &Bytes) -> Result<Vec<Bytes>, String> {
    let mut payloads = Vec::new();
    let mut cursor = 0usize;
    while cursor < body.len() {
        let Some(header_end) = cursor.checked_add(4) else {
            return Err("append batch frame offset overflow".to_owned());
        };
        if header_end > body.len() {
            return Err("append batch frame is missing length header".to_owned());
        }
        let len = u32::from_be_bytes(
            body[cursor..header_end]
                .try_into()
                .expect("slice length is exactly 4"),
        ) as usize;
        cursor = header_end;
        let Some(payload_end) = cursor.checked_add(len) else {
            return Err("append batch payload length overflow".to_owned());
        };
        if payload_end > body.len() {
            return Err("append batch frame payload is truncated".to_owned());
        }
        payloads.push(body.slice(cursor..payload_end));
        cursor = payload_end;
    }
    if payloads.is_empty() {
        return Err("append batch must contain at least one frame".to_owned());
    }
    Ok(payloads)
}

fn render_batch_results(results: &[Result<AppendResponse, RuntimeError>]) -> String {
    const OK_ACK: &str = "{\"status\":204}";
    if results.iter().all(Result::is_ok) {
        let mut body = String::with_capacity(2 + results.len().saturating_mul(OK_ACK.len() + 1));
        body.push('[');
        for index in 0..results.len() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(OK_ACK);
        }
        body.push(']');
        return body;
    }

    let mut body = String::with_capacity(2 + results.len().saturating_mul(OK_ACK.len() + 1));
    body.push('[');
    for (index, result) in results.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        let status = match result {
            Ok(_) => StatusCode::NO_CONTENT.as_u16(),
            Err(err) => runtime_error_status(err).as_u16(),
        };
        body.push_str("{\"status\":");
        body.push_str(&status.to_string());
        body.push('}');
    }
    body.push(']');
    body
}

fn render_metrics(
    snapshot: RuntimeMetricsSnapshot,
    mailbox: RuntimeMailboxSnapshot,
    http: HttpMetricsSnapshot,
    raft_groups: &[RaftGroupMetricsSnapshot],
) -> String {
    let active_cores = active_count(&snapshot.per_core_appends);
    let active_groups = active_count(&snapshot.per_group_appends);
    let mut body = String::from("{");
    body.push_str("\"accepted_appends\":");
    body.push_str(&snapshot.accepted_appends.to_string());
    body.push_str(",\"active_cores\":");
    body.push_str(&active_cores.to_string());
    body.push_str(",\"active_groups\":");
    body.push_str(&active_groups.to_string());
    body.push_str(",\"per_core_appends\":");
    body.push_str(&render_u64_array(&snapshot.per_core_appends));
    body.push_str(",\"per_group_appends\":");
    body.push_str(&render_u64_array(&snapshot.per_group_appends));
    body.push_str(",\"applied_mutations\":");
    body.push_str(&snapshot.applied_mutations.to_string());
    body.push_str(",\"per_core_applied_mutations\":");
    body.push_str(&render_u64_array(&snapshot.per_core_applied_mutations));
    body.push_str(",\"per_group_applied_mutations\":");
    body.push_str(&render_u64_array(&snapshot.per_group_applied_mutations));
    body.push_str(",\"mutation_apply_ns\":");
    body.push_str(&snapshot.mutation_apply_ns.to_string());
    body.push_str(",\"per_core_mutation_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_mutation_apply_ns));
    body.push_str(",\"per_group_mutation_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_mutation_apply_ns));
    body.push_str(",\"group_lock_wait_ns\":");
    body.push_str(&snapshot.group_lock_wait_ns.to_string());
    body.push_str(",\"per_core_group_lock_wait_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_group_lock_wait_ns));
    body.push_str(",\"per_group_group_lock_wait_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_group_lock_wait_ns));
    body.push_str(",\"group_engine_exec_ns\":");
    body.push_str(&snapshot.group_engine_exec_ns.to_string());
    body.push_str(",\"per_core_group_engine_exec_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_group_engine_exec_ns));
    body.push_str(",\"per_group_group_engine_exec_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_group_engine_exec_ns));
    body.push_str(",\"group_mailbox_depth\":");
    body.push_str(&snapshot.group_mailbox_depth.to_string());
    body.push_str(",\"per_group_group_mailbox_depth\":");
    body.push_str(&render_u64_array(&snapshot.per_group_group_mailbox_depth));
    body.push_str(",\"group_mailbox_max_depth\":");
    body.push_str(&snapshot.group_mailbox_max_depth.to_string());
    body.push_str(",\"per_group_group_mailbox_max_depth\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_group_mailbox_max_depth,
    ));
    body.push_str(",\"group_mailbox_full_events\":");
    body.push_str(&snapshot.group_mailbox_full_events.to_string());
    body.push_str(",\"per_group_group_mailbox_full_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_group_mailbox_full_events,
    ));
    body.push_str(",\"raft_write_many_batches\":");
    body.push_str(&snapshot.raft_write_many_batches.to_string());
    body.push_str(",\"per_core_raft_write_many_batches\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_batches,
    ));
    body.push_str(",\"per_group_raft_write_many_batches\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_batches,
    ));
    body.push_str(",\"raft_write_many_commands\":");
    body.push_str(&snapshot.raft_write_many_commands.to_string());
    body.push_str(",\"per_core_raft_write_many_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_commands,
    ));
    body.push_str(",\"per_group_raft_write_many_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_commands,
    ));
    body.push_str(",\"raft_write_many_logical_commands\":");
    body.push_str(&snapshot.raft_write_many_logical_commands.to_string());
    body.push_str(",\"per_core_raft_write_many_logical_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_logical_commands,
    ));
    body.push_str(",\"per_group_raft_write_many_logical_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_logical_commands,
    ));
    body.push_str(",\"raft_write_many_responses\":");
    body.push_str(&snapshot.raft_write_many_responses.to_string());
    body.push_str(",\"per_core_raft_write_many_responses\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_responses,
    ));
    body.push_str(",\"per_group_raft_write_many_responses\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_responses,
    ));
    body.push_str(",\"raft_write_many_submit_ns\":");
    body.push_str(&snapshot.raft_write_many_submit_ns.to_string());
    body.push_str(",\"per_core_raft_write_many_submit_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_submit_ns,
    ));
    body.push_str(",\"per_group_raft_write_many_submit_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_submit_ns,
    ));
    body.push_str(",\"raft_write_many_response_ns\":");
    body.push_str(&snapshot.raft_write_many_response_ns.to_string());
    body.push_str(",\"per_core_raft_write_many_response_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_response_ns,
    ));
    body.push_str(",\"per_group_raft_write_many_response_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_response_ns,
    ));
    body.push_str(",\"raft_apply_entries\":");
    body.push_str(&snapshot.raft_apply_entries.to_string());
    body.push_str(",\"per_core_raft_apply_entries\":");
    body.push_str(&render_u64_array(&snapshot.per_core_raft_apply_entries));
    body.push_str(",\"per_group_raft_apply_entries\":");
    body.push_str(&render_u64_array(&snapshot.per_group_raft_apply_entries));
    body.push_str(",\"raft_apply_ns\":");
    body.push_str(&snapshot.raft_apply_ns.to_string());
    body.push_str(",\"per_core_raft_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_raft_apply_ns));
    body.push_str(",\"per_group_raft_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_raft_apply_ns));
    body.push_str(",\"live_read_waiters\":");
    body.push_str(&snapshot.live_read_waiters.to_string());
    body.push_str(",\"per_core_live_read_waiters\":");
    body.push_str(&render_u64_array(&snapshot.per_core_live_read_waiters));
    body.push_str(",\"live_read_backpressure_events\":");
    body.push_str(&snapshot.live_read_backpressure_events.to_string());
    body.push_str(",\"per_core_live_read_backpressure_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_live_read_backpressure_events,
    ));
    body.push_str(",\"sse_streams_opened\":");
    body.push_str(&http.sse_streams_opened.to_string());
    body.push_str(",\"sse_read_iterations\":");
    body.push_str(&http.sse_read_iterations.to_string());
    body.push_str(",\"sse_data_events\":");
    body.push_str(&http.sse_data_events.to_string());
    body.push_str(",\"sse_control_events\":");
    body.push_str(&http.sse_control_events.to_string());
    body.push_str(",\"sse_error_events\":");
    body.push_str(&http.sse_error_events.to_string());
    body.push_str(",\"routed_requests\":");
    body.push_str(&snapshot.routed_requests.to_string());
    body.push_str(",\"per_core_routed_requests\":");
    body.push_str(&render_u64_array(&snapshot.per_core_routed_requests));
    body.push_str(",\"mailbox_send_wait_ns\":");
    body.push_str(&snapshot.mailbox_send_wait_ns.to_string());
    body.push_str(",\"per_core_mailbox_send_wait_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_mailbox_send_wait_ns));
    body.push_str(",\"mailbox_full_events\":");
    body.push_str(&snapshot.mailbox_full_events.to_string());
    body.push_str(",\"per_core_mailbox_full_events\":");
    body.push_str(&render_u64_array(&snapshot.per_core_mailbox_full_events));
    body.push_str(",\"wal_batches\":");
    body.push_str(&snapshot.wal_batches.to_string());
    body.push_str(",\"per_core_wal_batches\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_batches));
    body.push_str(",\"per_group_wal_batches\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_batches));
    body.push_str(",\"wal_records\":");
    body.push_str(&snapshot.wal_records.to_string());
    body.push_str(",\"per_core_wal_records\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_records));
    body.push_str(",\"per_group_wal_records\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_records));
    body.push_str(",\"wal_write_ns\":");
    body.push_str(&snapshot.wal_write_ns.to_string());
    body.push_str(",\"per_core_wal_write_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_write_ns));
    body.push_str(",\"per_group_wal_write_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_write_ns));
    body.push_str(",\"wal_sync_ns\":");
    body.push_str(&snapshot.wal_sync_ns.to_string());
    body.push_str(",\"per_core_wal_sync_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_sync_ns));
    body.push_str(",\"per_group_wal_sync_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_sync_ns));
    body.push_str(",\"cold_flush_uploads\":");
    body.push_str(&snapshot.cold_flush_uploads.to_string());
    body.push_str(",\"cold_flush_upload_bytes\":");
    body.push_str(&snapshot.cold_flush_upload_bytes.to_string());
    body.push_str(",\"cold_flush_upload_ns\":");
    body.push_str(&snapshot.cold_flush_upload_ns.to_string());
    body.push_str(",\"cold_flush_publishes\":");
    body.push_str(&snapshot.cold_flush_publishes.to_string());
    body.push_str(",\"cold_flush_publish_bytes\":");
    body.push_str(&snapshot.cold_flush_publish_bytes.to_string());
    body.push_str(",\"cold_flush_publish_ns\":");
    body.push_str(&snapshot.cold_flush_publish_ns.to_string());
    body.push_str(",\"cold_orphan_cleanup_attempts\":");
    body.push_str(&snapshot.cold_orphan_cleanup_attempts.to_string());
    body.push_str(",\"cold_orphan_cleanup_errors\":");
    body.push_str(&snapshot.cold_orphan_cleanup_errors.to_string());
    body.push_str(",\"cold_orphan_bytes\":");
    body.push_str(&snapshot.cold_orphan_bytes.to_string());
    body.push_str(",\"cold_hot_bytes\":");
    body.push_str(&snapshot.cold_hot_bytes.to_string());
    body.push_str(",\"per_group_cold_hot_bytes\":");
    body.push_str(&render_u64_array(&snapshot.per_group_cold_hot_bytes));
    body.push_str(",\"cold_hot_group_bytes_max\":");
    body.push_str(&snapshot.cold_hot_group_bytes_max.to_string());
    body.push_str(",\"per_group_cold_hot_bytes_max\":");
    body.push_str(&render_u64_array(&snapshot.per_group_cold_hot_bytes_max));
    body.push_str(",\"cold_hot_stream_bytes_max\":");
    body.push_str(&snapshot.cold_hot_stream_bytes_max.to_string());
    body.push_str(",\"cold_backpressure_events\":");
    body.push_str(&snapshot.cold_backpressure_events.to_string());
    body.push_str(",\"per_core_cold_backpressure_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_cold_backpressure_events,
    ));
    body.push_str(",\"per_group_cold_backpressure_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_cold_backpressure_events,
    ));
    body.push_str(",\"cold_backpressure_bytes\":");
    body.push_str(&snapshot.cold_backpressure_bytes.to_string());
    body.push_str(",\"mailbox_depths\":");
    body.push_str(&render_usize_array(&mailbox.depths));
    body.push_str(",\"mailbox_capacities\":");
    body.push_str(&render_usize_array(&mailbox.capacities));
    body.push_str(",\"raft_group_count\":");
    body.push_str(&raft_groups.len().to_string());
    body.push_str(",\"raft_groups\":");
    body.push_str(&render_raft_group_metrics_array(raft_groups));
    body.push('}');
    body
}

fn active_count(values: &[u64]) -> usize {
    values.iter().filter(|value| **value > 0).count()
}

fn render_u64_array(values: &[u64]) -> String {
    let mut body = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&value.to_string());
    }
    body.push(']');
    body
}

fn render_usize_array(values: &[usize]) -> String {
    let mut body = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&value.to_string());
    }
    body.push(']');
    body
}

fn render_raft_group_metrics_array(values: &[RaftGroupMetricsSnapshot]) -> String {
    let mut body = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push('{');
        body.push_str("\"raft_group_id\":");
        body.push_str(&value.raft_group_id.to_string());
        body.push_str(",\"node_id\":");
        body.push_str(&value.node_id.to_string());
        body.push_str(",\"current_term\":");
        body.push_str(&value.current_term.to_string());
        body.push_str(",\"current_leader\":");
        push_optional_u64(&mut body, value.current_leader);
        body.push_str(",\"last_log_index\":");
        push_optional_u64(&mut body, value.last_log_index);
        push_optional_log_progress(&mut body, "committed", value.committed);
        push_optional_log_progress(&mut body, "last_applied", value.last_applied);
        push_optional_log_progress(&mut body, "snapshot", value.snapshot);
        push_optional_log_progress(&mut body, "purged", value.purged);
        body.push_str(",\"voter_ids\":");
        body.push_str(&render_u64_array(&value.voter_ids));
        body.push_str(",\"learner_ids\":");
        body.push_str(&render_u64_array(&value.learner_ids));
        body.push('}');
    }
    body.push(']');
    body
}

fn push_optional_log_progress(
    body: &mut String,
    name: &str,
    progress: Option<RaftLogProgressSnapshot>,
) {
    body.push_str(",\"");
    body.push_str(name);
    body.push_str("_term\":");
    push_optional_u64(body, progress.map(|value| value.term));
    body.push_str(",\"");
    body.push_str(name);
    body.push_str("_index\":");
    push_optional_u64(body, progress.map(|value| value.index));
}

fn push_optional_u64(body: &mut String, value: Option<u64>) {
    match value {
        Some(value) => body.push_str(&value.to_string()),
        None => body.push_str("null"),
    }
}

fn should_base64_encode_sse_data(content_type: &str) -> bool {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    !(content_type.starts_with("text/") || content_type == "application/json")
}

fn read_etag(response: &ReadStreamResponse) -> String {
    let mut hasher = DefaultHasher::new();
    response.offset.hash(&mut hasher);
    response.next_offset.hash(&mut hasher);
    response.content_type.hash(&mut hasher);
    response.payload.hash(&mut hasher);
    response.up_to_date.hash(&mut hasher);
    response.closed.hash(&mut hasher);
    format!("\"{:016x}\"", hasher.finish())
}

fn read_response(
    response: ReadStreamResponse,
    request_headers: &HeaderMap,
    request_cursor: Option<&str>,
) -> Response {
    let payload = match project_http_read_payload(&response.content_type, &response.payload) {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response(),
    };
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, &response.content_type);
    insert_offset(&mut headers, response.next_offset);
    let etag = read_etag(&response);
    if let Ok(value) = HeaderValue::from_str(&etag) {
        headers.insert(ETAG, value);
    }
    if request_headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|part| part.trim() == etag))
    {
        return (StatusCode::NOT_MODIFIED, headers).into_response();
    }
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    let closed_at_tail = response.closed && response.up_to_date;
    if closed_at_tail {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    } else if request_cursor.is_some() {
        insert_cursor(
            &mut headers,
            response_cursor(response.next_offset, request_cursor),
        );
    }
    (StatusCode::OK, headers, payload).into_response()
}

fn snapshot_response(response: ReadSnapshotResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, &response.content_type);
    insert_snapshot_offset(&mut headers, response.snapshot_offset);
    insert_offset(&mut headers, response.next_offset);
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    (StatusCode::OK, headers, response.payload).into_response()
}

fn bootstrap_response(response: BootstrapStreamResponse) -> Response {
    let boundary = bootstrap_boundary(&response);
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(
        &mut headers,
        &format!("multipart/mixed; boundary={boundary}"),
    );
    match response.snapshot_offset {
        Some(snapshot_offset) => insert_snapshot_offset(&mut headers, snapshot_offset),
        None => insert_static(&mut headers, HEADER_STREAM_SNAPSHOT_OFFSET, "-1"),
    }
    insert_offset(&mut headers, response.next_offset);
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    insert_cache_control(&mut headers, "no-store");
    (
        StatusCode::OK,
        headers,
        render_bootstrap_multipart(&response, &boundary),
    )
        .into_response()
}

fn bootstrap_boundary(response: &BootstrapStreamResponse) -> String {
    let mut hasher = DefaultHasher::new();
    response.snapshot_offset.hash(&mut hasher);
    response.next_offset.hash(&mut hasher);
    response.updates.len().hash(&mut hasher);
    response.snapshot_payload.len().hash(&mut hasher);
    format!("ursula-bootstrap-{:016x}", hasher.finish())
}

fn render_bootstrap_multipart(response: &BootstrapStreamResponse, boundary: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_multipart_part(
        &mut body,
        boundary,
        &response.snapshot_content_type,
        &response.snapshot_payload,
    );
    for update in &response.updates {
        push_multipart_part(&mut body, boundary, &update.content_type, &update.payload);
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");
    body
}

fn push_multipart_part(body: &mut Vec<u8>, boundary: &str, content_type: &str, payload: &[u8]) {
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"\r\nContent-Type: ");
    body.extend_from_slice(content_type.as_bytes());
    body.extend_from_slice(b"\r\n\r\n");
    body.extend_from_slice(payload);
    body.extend_from_slice(b"\r\n");
}

fn offset_now_response(response: ReadStreamResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, &response.content_type);
    insert_offset(&mut headers, response.next_offset);
    insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    insert_cache_control(&mut headers, "no-store");
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    let body = if is_json_content_type(&response.content_type) {
        Bytes::from_static(b"[]")
    } else {
        Bytes::new()
    };
    (StatusCode::OK, headers, body).into_response()
}

fn long_poll_no_content_response(
    response: &ReadStreamResponse,
    request_cursor: Option<&str>,
) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_offset(&mut headers, response.next_offset);
    insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    } else {
        insert_cursor(
            &mut headers,
            response_cursor(response.next_offset, request_cursor),
        );
    }
    (StatusCode::NO_CONTENT, headers).into_response()
}

fn is_json_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .eq_ignore_ascii_case("application/json")
}

fn normalize_http_write_payload(
    content_type: &str,
    body: Bytes,
    allow_empty_array: bool,
) -> Result<Bytes, String> {
    if !is_json_content_type(content_type) || body.is_empty() {
        return Ok(body);
    }

    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|err| format!("invalid JSON payload: {err}"))?;
    let messages = match value {
        serde_json::Value::Array(items) => {
            if items.is_empty() && !allow_empty_array {
                return Err("JSON append array must not be empty".to_owned());
            }
            items
        }
        other => vec![other],
    };

    let mut out = Vec::new();
    for message in messages {
        serde_json::to_writer(&mut out, &message)
            .map_err(|err| format!("failed to encode JSON message: {err}"))?;
        out.push(b'\n');
    }
    Ok(Bytes::from(out))
}

fn project_http_read_payload(content_type: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
    if !is_json_content_type(content_type) {
        return Ok(payload.to_vec());
    }

    let mut out = Vec::with_capacity(payload.len().saturating_add(2));
    out.push(b'[');
    let mut first = true;
    let mut idx = 0usize;
    while idx < payload.len() {
        let Some(rel_end) = payload[idx..].iter().position(|byte| *byte == b'\n') else {
            return Err(format!("invalid JSON payload boundary at byte {idx}"));
        };
        let line_end = idx + rel_end;
        let line = &payload[idx..line_end];
        if !line.is_empty() {
            serde_json::from_slice::<serde_json::Value>(line)
                .map_err(|err| format!("invalid stored JSON message at byte {idx}: {err}"))?;
            if !first {
                out.push(b',');
            }
            out.extend_from_slice(line);
            first = false;
        }
        idx = line_end + 1;
    }
    out.push(b']');
    Ok(out)
}

fn render_sse_read(
    read: &ReadStreamResponse,
    encode_base64: bool,
    request_cursor: Option<&str>,
) -> String {
    let mut body = String::new();
    let closed_at_tail = read.closed && read.up_to_date;
    if !read.payload.is_empty() {
        body.push_str("event: data\n");
        let payload = if encode_base64 {
            BASE64_STANDARD.encode(&read.payload)
        } else if is_json_content_type(&read.content_type) {
            match project_http_read_payload(&read.content_type, &read.payload) {
                Ok(payload) => String::from_utf8_lossy(&payload).into_owned(),
                Err(message) => message,
            }
        } else {
            String::from_utf8_lossy(&read.payload).into_owned()
        };
        for line in payload.split('\n') {
            body.push_str("data:");
            body.push_str(&sse_safe_line(line));
            body.push('\n');
        }
        body.push('\n');
    }

    body.push_str("event: control\n");
    body.push_str("data:{\"streamNextOffset\":\"");
    body.push_str(&format!("{:020}", read.next_offset));
    body.push('"');
    if !closed_at_tail {
        body.push_str(",\"streamCursor\":\"");
        body.push_str(&format!(
            "{:020}",
            response_cursor(read.next_offset, request_cursor)
        ));
        body.push('"');
    }
    if read.up_to_date {
        body.push_str(",\"upToDate\":true");
    }
    if closed_at_tail {
        body.push_str(",\"streamClosed\":true");
    }
    body.push_str("}\n\n");
    body
}

fn sse_safe_line(line: &str) -> String {
    line.chars()
        .filter(|ch| *ch != '\r' && *ch != '\0')
        .collect()
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use openraft::RaftNetworkV2;
    use openraft::error::ReplicationClosed;
    use openraft::network::RPCOption;
    use openraft::raft::SnapshotResponse;
    use openraft::rt::WatchReceiver;
    use tower::ServiceExt;
    use ursula_raft::UrsulaRaftTypeConfig;
    use ursula_shard::RaftGroupId;

    use super::*;

    async fn wait_raft_state_machine_payload(
        registry: &RaftGroupHandleRegistry,
        placement: ursula_shard::ShardPlacement,
        stream_id: &BucketStreamId,
        expected: &[u8],
        context: &str,
    ) {
        let raft = registry
            .get(placement.raft_group_id)
            .expect("registered raft group");
        let mut last_observed = None;
        let max_len = expected.len().max(64);
        for _ in 0..100 {
            let read = raft
                .with_state_machine({
                    let stream_id = stream_id.clone();
                    move |state_machine| {
                        Box::pin(async move {
                            state_machine
                                .read_stream(
                                    ReadStreamRequest {
                                        stream_id,
                                        offset: 0,
                                        max_len,
                                        now_ms: 0,
                                    },
                                    placement,
                                )
                                .await
                        })
                    }
                })
                .await;
            match read {
                Ok(Ok(read)) if read.payload == expected => return,
                Ok(Ok(read)) => {
                    last_observed = Some(format!(
                        "payload={:?}",
                        String::from_utf8_lossy(&read.payload)
                    ));
                }
                Ok(Err(err)) => {
                    last_observed = Some(format!("err={err}"));
                }
                Err(err) => {
                    last_observed = Some(format!("state-machine err={err}"));
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{context}: timed out waiting for stream payload; latest={last_observed:?}");
    }

    struct StaticGrpcTestNode {
        runtime: ShardRuntime,
        registry: RaftGroupHandleRegistry,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        server: tokio::task::JoinHandle<()>,
    }

    impl StaticGrpcTestNode {
        async fn shutdown(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.server.abort();
            let _ = self.server.await;
        }
    }

    #[derive(Default)]
    struct StaticGrpcTestNodeStorage {
        raft_log_dir: Option<PathBuf>,
        cold_store: Option<ColdStoreHandle>,
        per_group_initializers: bool,
    }

    async fn spawn_static_grpc_test_node(
        node_id: u64,
        listener: tokio::net::TcpListener,
        factory_peers: Vec<(u64, String)>,
        router_peers: Vec<(u64, String)>,
        initialize_membership: bool,
        raft_group_count: usize,
    ) -> StaticGrpcTestNode {
        spawn_static_grpc_test_node_with_log_dir(
            node_id,
            listener,
            factory_peers,
            router_peers,
            initialize_membership,
            raft_group_count,
            None,
        )
        .await
    }

    async fn spawn_static_grpc_test_node_with_log_dir(
        node_id: u64,
        listener: tokio::net::TcpListener,
        factory_peers: Vec<(u64, String)>,
        router_peers: Vec<(u64, String)>,
        initialize_membership: bool,
        raft_group_count: usize,
        raft_log_dir: Option<PathBuf>,
    ) -> StaticGrpcTestNode {
        spawn_static_grpc_test_node_with_storage(
            node_id,
            listener,
            factory_peers,
            router_peers,
            initialize_membership,
            raft_group_count,
            StaticGrpcTestNodeStorage {
                raft_log_dir,
                cold_store: None,
                per_group_initializers: false,
            },
        )
        .await
    }

    async fn spawn_static_grpc_test_node_with_per_group_initializers(
        node_id: u64,
        listener: tokio::net::TcpListener,
        factory_peers: Vec<(u64, String)>,
        router_peers: Vec<(u64, String)>,
        initialize_membership: bool,
        raft_group_count: usize,
    ) -> StaticGrpcTestNode {
        spawn_static_grpc_test_node_with_storage(
            node_id,
            listener,
            factory_peers,
            router_peers,
            initialize_membership,
            raft_group_count,
            StaticGrpcTestNodeStorage {
                raft_log_dir: None,
                cold_store: None,
                per_group_initializers: true,
            },
        )
        .await
    }

    async fn spawn_static_grpc_test_node_with_storage(
        node_id: u64,
        listener: tokio::net::TcpListener,
        factory_peers: Vec<(u64, String)>,
        router_peers: Vec<(u64, String)>,
        initialize_membership: bool,
        raft_group_count: usize,
        storage: StaticGrpcTestNodeStorage,
    ) -> StaticGrpcTestNode {
        let registry = RaftGroupHandleRegistry::default();
        let mut config = RuntimeConfig::new(1, raft_group_count);
        config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
        let mut factory = StaticGrpcRaftGroupEngineFactory::new(
            node_id,
            factory_peers,
            initialize_membership,
            registry.clone(),
        );
        factory = factory.with_per_group_membership_initializers(storage.per_group_initializers);
        factory = factory.with_cold_store(storage.cold_store.clone());
        if let Some(raft_log_dir) = storage.raft_log_dir {
            factory = factory.with_raft_log_dir(raft_log_dir);
        }
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            config,
            factory,
            storage.cold_store,
        )
        .expect("runtime");
        let app = router_with_static_raft_cluster(runtime.clone(), registry.clone(), router_peers);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve static raft node");
        });
        StaticGrpcTestNode {
            runtime,
            registry,
            shutdown: Some(shutdown_tx),
            server,
        }
    }

    #[tokio::test]
    async fn create_append_read_and_head_match_perf_compare_subset() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/stream-1")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/stream-1")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("abcdefg"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000007"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/stream-1?offset=2&max_bytes=3")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000005"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"cde");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/benchcmp/stream-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000007"
        );
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");
    }

    #[tokio::test]
    async fn close_only_post_sets_closed_state_and_rejects_later_append() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/closing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            status,
            StatusCode::CREATED,
            "create body={}",
            std::str::from_utf8(&body).unwrap_or("<non-utf8>")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/closing")
                    .header(HEADER_STREAM_CLOSED, "true")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(HEADER_STREAM_CLOSED).unwrap(),
            "true"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/closing")
                    .header(CONTENT_TYPE, DEFAULT_CONTENT_TYPE)
                    .body(Body::from("x"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn append_conflict_precedence_reports_closed_header_before_mismatch_or_seq() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/closed-precedence")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/closed-precedence")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_STREAM_CLOSED, "true")
                    .body(Body::from("final"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000005"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/closed-precedence")
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(HEADER_STREAM_SEQ, "0001")
                    .body(Body::from("too-late"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response.headers().get(HEADER_STREAM_CLOSED).unwrap(),
            "true"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000005"
        );
    }

    #[tokio::test]
    async fn stream_seq_header_rejects_regressing_appends() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/seq-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/seq-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_STREAM_SEQ, "0002")
                    .body(Body::from("a"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/seq-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_STREAM_SEQ, "0002")
                    .body(Body::from("b"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/seq-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_STREAM_SEQ, "0003")
                    .body(Body::from("c"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/producer-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/producer-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "0")
                    .body(Body::from("a"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "0");
        assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/producer-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "0")
                    .body(Body::from("ignored"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000001"
        );
        assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "0");
        assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/producer-http?offset=0&max_bytes=16")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"a");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/producer-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "2")
                    .body(Body::from("gap"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response.headers().get("producer-expected-seq").unwrap(),
            "1"
        );
        assert_eq!(
            response.headers().get("producer-received-seq").unwrap(),
            "2"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/producer-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "1")
                    .header(HEADER_PRODUCER_SEQ, "0")
                    .body(Body::from("b"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "1");
        assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/producer-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "1")
                    .body(Body::from("stale"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "1");
    }

    #[tokio::test]
    async fn delete_stream_removes_http_visible_state() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/delete-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/delete-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("payload"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/benchcmp/delete-http")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/benchcmp/delete-http")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/delete-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("x"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn append_batch_matches_perf_compare_frame_format() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/batch-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-stream/append-batch")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from(batch_body(&[
                        b"abc".as_slice(),
                        b"de".as_slice(),
                    ])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"status":204},{"status":204}]"#);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-stream/append-batch")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from(batch_body(&[b"".as_slice(), b"f".as_slice()])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"status":400},{"status":204}]"#);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/batch-stream?offset=0&max_bytes=8")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000006"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"abcdef");
    }

    #[tokio::test]
    async fn append_batch_minimal_ack_skips_success_body_but_keeps_item_errors() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/batch-minimal")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-minimal/append-batch")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PREFER, "return=minimal")
                    .body(Body::from(batch_body(&[b"a".as_slice(), b"b".as_slice()])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert!(body.is_empty());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-minimal/append-batch")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_PREFER, "return=minimal")
                    .body(Body::from(batch_body(&[b"".as_slice(), b"c".as_slice()])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"status":400},{"status":204}]"#);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/batch-minimal?offset=0&max_bytes=16")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"abc");
    }

    #[tokio::test]
    async fn append_batch_producer_headers_deduplicate_retries() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/batch-producer")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-producer/append-batch")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "0")
                    .body(Body::from(batch_body(&[b"ab".as_slice(), b"c".as_slice()])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "0");
        assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"status":204},{"status":204}]"#);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-producer/append-batch")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "0")
                    .body(Body::from(batch_body(&[b"ignored".as_slice()])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"status":204},{"status":204}]"#);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/batch-producer/append-batch")
                    .header(HEADER_PRODUCER_ID, "writer-1")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "1")
                    .body(Body::from(batch_body(&[b"d".as_slice()])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"status":204}]"#);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/batch-producer?offset=0&max_bytes=16")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"abcd");
    }

    #[tokio::test]
    async fn json_mode_normalizes_appends_and_projects_reads() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/stream/json-mode")
                    .header(CONTENT_TYPE, "application/json; charset=utf-8")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/stream/json-mode")
                    .header(CONTENT_TYPE, "application/json; charset=utf-8")
                    .body(Body::from(r#"[[1,2,3]]"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/stream/json-mode")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[[1,2,3]]"#);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/stream/json-mode")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from("[]"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn fork_creation_copies_source_prefix_and_inherits_content_type() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/stream/fork-copy-source")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"[{"from":"source"}]"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/stream/fork-copy-child")
                    .header(HEADER_STREAM_FORKED_FROM, "/v1/stream/fork-copy-source")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).expect("content type"),
            "application/json"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/stream/fork-copy-child?offset=-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"[{"from":"source"}]"#);
    }

    #[test]
    fn append_batch_parser_returns_body_slices() {
        let body = Bytes::from(batch_body(&[b"abc".as_slice(), b"de".as_slice()]));
        let base = body.as_ptr();
        let payloads = parse_append_batch(&body).expect("batch");

        assert_eq!(payloads.len(), 2);
        assert_eq!(&payloads[0][..], b"abc");
        assert_eq!(&payloads[1][..], b"de");
        assert_eq!(payloads[0].as_ptr(), base.wrapping_add(4));
        assert_eq!(payloads[1].as_ptr(), base.wrapping_add(4 + 3 + 4));
    }

    #[tokio::test]
    async fn metrics_expose_per_core_and_group_append_distribution() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/metrics-stream")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/metrics-stream/append-batch")
                    .body(Body::from(batch_body(&[
                        b"abc".as_slice(),
                        b"de".as_slice(),
                    ])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"accepted_appends\":2"));
        assert!(body.contains("\"applied_mutations\":3"));
        assert!(body.contains("\"active_cores\":1"));
        assert!(body.contains("\"active_groups\":1"));
        assert!(body.contains("\"per_core_appends\":["));
        assert!(body.contains("\"per_group_appends\":["));
        assert!(body.contains("\"per_core_applied_mutations\":["));
        assert!(body.contains("\"per_group_applied_mutations\":["));
        assert!(body.contains("\"mutation_apply_ns\":"));
        assert!(body.contains("\"per_core_mutation_apply_ns\":["));
        assert!(body.contains("\"per_group_mutation_apply_ns\":["));
        assert!(body.contains("\"group_lock_wait_ns\":"));
        assert!(body.contains("\"per_core_group_lock_wait_ns\":["));
        assert!(body.contains("\"per_group_group_lock_wait_ns\":["));
        assert!(body.contains("\"group_engine_exec_ns\":"));
        assert!(body.contains("\"per_core_group_engine_exec_ns\":["));
        assert!(body.contains("\"per_group_group_engine_exec_ns\":["));
        assert!(body.contains("\"group_mailbox_depth\":0"));
        assert!(body.contains("\"per_group_group_mailbox_depth\":["));
        assert!(body.contains("\"group_mailbox_max_depth\":"));
        assert!(body.contains("\"per_group_group_mailbox_max_depth\":["));
        assert!(body.contains("\"group_mailbox_full_events\":0"));
        assert!(body.contains("\"per_group_group_mailbox_full_events\":["));
        assert!(body.contains("\"raft_write_many_batches\":0"));
        assert!(body.contains("\"per_core_raft_write_many_batches\":[0,0]"));
        assert!(body.contains("\"per_group_raft_write_many_batches\":["));
        assert!(body.contains("\"raft_write_many_commands\":0"));
        assert!(body.contains("\"per_core_raft_write_many_commands\":[0,0]"));
        assert!(body.contains("\"per_group_raft_write_many_commands\":["));
        assert!(body.contains("\"raft_write_many_logical_commands\":0"));
        assert!(body.contains("\"per_core_raft_write_many_logical_commands\":[0,0]"));
        assert!(body.contains("\"per_group_raft_write_many_logical_commands\":["));
        assert!(body.contains("\"raft_write_many_responses\":0"));
        assert!(body.contains("\"per_core_raft_write_many_responses\":[0,0]"));
        assert!(body.contains("\"per_group_raft_write_many_responses\":["));
        assert!(body.contains("\"raft_write_many_submit_ns\":0"));
        assert!(body.contains("\"per_core_raft_write_many_submit_ns\":[0,0]"));
        assert!(body.contains("\"per_group_raft_write_many_submit_ns\":["));
        assert!(body.contains("\"raft_write_many_response_ns\":0"));
        assert!(body.contains("\"per_core_raft_write_many_response_ns\":[0,0]"));
        assert!(body.contains("\"per_group_raft_write_many_response_ns\":["));
        assert!(body.contains("\"raft_apply_entries\":0"));
        assert!(body.contains("\"per_core_raft_apply_entries\":[0,0]"));
        assert!(body.contains("\"per_group_raft_apply_entries\":["));
        assert!(body.contains("\"raft_apply_ns\":0"));
        assert!(body.contains("\"per_core_raft_apply_ns\":[0,0]"));
        assert!(body.contains("\"per_group_raft_apply_ns\":["));
        assert!(body.contains("\"live_read_waiters\":0"));
        assert!(body.contains("\"per_core_live_read_waiters\":[0,0]"));
        assert!(body.contains("\"live_read_backpressure_events\":0"));
        assert!(body.contains("\"per_core_live_read_backpressure_events\":[0,0]"));
        assert!(body.contains("\"sse_streams_opened\":0"));
        assert!(body.contains("\"sse_read_iterations\":0"));
        assert!(body.contains("\"sse_data_events\":0"));
        assert!(body.contains("\"sse_control_events\":0"));
        assert!(body.contains("\"sse_error_events\":0"));
        assert!(body.contains("\"routed_requests\":2"));
        assert!(body.contains("\"per_core_routed_requests\":["));
        assert!(body.contains("\"mailbox_send_wait_ns\":"));
        assert!(body.contains("\"per_core_mailbox_send_wait_ns\":["));
        assert!(body.contains("\"mailbox_full_events\":0"));
        assert!(body.contains("\"per_core_mailbox_full_events\":[0,0]"));
        assert!(body.contains("\"wal_batches\":0"));
        assert!(body.contains("\"per_core_wal_batches\":[0,0]"));
        assert!(body.contains("\"per_group_wal_batches\":["));
        assert!(body.contains("\"wal_records\":0"));
        assert!(body.contains("\"per_core_wal_records\":[0,0]"));
        assert!(body.contains("\"per_group_wal_records\":["));
        assert!(body.contains("\"wal_write_ns\":0"));
        assert!(body.contains("\"per_core_wal_write_ns\":[0,0]"));
        assert!(body.contains("\"per_group_wal_write_ns\":["));
        assert!(body.contains("\"wal_sync_ns\":0"));
        assert!(body.contains("\"per_core_wal_sync_ns\":[0,0]"));
        assert!(body.contains("\"per_group_wal_sync_ns\":["));
        assert!(body.contains("\"cold_flush_uploads\":0"));
        assert!(body.contains("\"cold_flush_upload_bytes\":0"));
        assert!(body.contains("\"cold_flush_upload_ns\":0"));
        assert!(body.contains("\"cold_flush_publishes\":0"));
        assert!(body.contains("\"cold_flush_publish_bytes\":0"));
        assert!(body.contains("\"cold_flush_publish_ns\":0"));
        assert!(body.contains("\"cold_orphan_cleanup_attempts\":0"));
        assert!(body.contains("\"cold_orphan_cleanup_errors\":0"));
        assert!(body.contains("\"cold_orphan_bytes\":0"));
        assert!(body.contains("\"cold_hot_bytes\":0"));
        assert!(body.contains("\"per_group_cold_hot_bytes\":["));
        assert!(body.contains("\"cold_hot_group_bytes_max\":0"));
        assert!(body.contains("\"per_group_cold_hot_bytes_max\":["));
        assert!(body.contains("\"cold_hot_stream_bytes_max\":0"));
        assert!(body.contains("\"cold_backpressure_events\":0"));
        assert!(body.contains("\"per_core_cold_backpressure_events\":[0,0]"));
        assert!(body.contains("\"per_group_cold_backpressure_events\":["));
        assert!(body.contains("\"cold_backpressure_bytes\":0"));
        assert!(body.contains("\"mailbox_depths\":["));
        assert!(body.contains("\"mailbox_capacities\":[1024,1024]"));
        assert!(body.contains("\"raft_group_count\":0"));
        assert!(body.contains("\"raft_groups\":[]"));
    }

    #[tokio::test]
    async fn long_poll_times_out_with_no_content_and_cleans_waiter() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/long-poll-timeout")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/long-poll-timeout?offset=now&live=long-poll&timeout_ms=10")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000000"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_UP_TO_DATE).unwrap(),
            "true"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_CURSOR).unwrap(),
            "00000000000000000000"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"live_read_waiters\":0"));
    }

    #[tokio::test]
    async fn long_poll_returns_service_unavailable_when_live_waiters_are_full() {
        let runtime = ShardRuntime::spawn(
            RuntimeConfig::new(1, 1).with_live_read_max_waiters_per_core(Some(1)),
        )
        .expect("runtime");
        let app = router(runtime);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/long-poll-limit")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let first = {
            let app = app.clone();
            tokio::spawn(async move {
                app.oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/benchcmp/long-poll-limit?offset=now&live=long-poll&timeout_ms=1000")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response")
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/long-poll-limit?offset=now&live=long-poll&timeout_ms=1000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert!(
            std::str::from_utf8(&body)
                .expect("utf8 body")
                .contains("live read waiters")
        );

        first.abort();
        let _ = first.await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"live_read_backpressure_events\":1"));
        assert!(body.contains("\"per_core_live_read_backpressure_events\":[1]"));
    }

    #[tokio::test]
    async fn long_poll_returns_append_from_owner_waiter() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/long-poll-wake")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let read = {
            let app = app.clone();
            tokio::spawn(async move {
                app.oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/benchcmp/long-poll-wake?offset=now&live=long-poll&timeout_ms=1000")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response")
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/long-poll-wake")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("wake"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = tokio::time::timeout(std::time::Duration::from_secs(1), read)
            .await
            .expect("long poll completed")
            .expect("read task");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000004"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_CURSOR).unwrap(),
            "00000000000000000004"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"wake");
    }

    #[tokio::test]
    async fn sse_live_tail_delivers_appended_text_and_closed_control() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/sse-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/sse-stream?offset=now&live=sse")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );

        let body_task = tokio::spawn(async move {
            to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("sse body")
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/sse-stream")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(HEADER_STREAM_CLOSED, "true")
                    .body(Body::from("sse-token"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let body = tokio::time::timeout(std::time::Duration::from_secs(1), body_task)
            .await
            .expect("sse completed")
            .expect("body task");
        let body = std::str::from_utf8(&body).expect("utf8 sse body");
        assert!(body.contains("event: data"));
        assert!(body.contains("data:sse-token"));
        assert!(body.contains("\"streamNextOffset\":\"00000000000000000009\""));
        assert!(body.contains("\"streamClosed\":true"));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 metrics body");
        assert!(body.contains("\"sse_streams_opened\":1"));
        assert!(body.contains("\"sse_read_iterations\":"));
        assert!(body.contains("\"sse_data_events\":1"));
        assert!(body.contains("\"sse_control_events\":1"));
        assert!(body.contains("\"sse_error_events\":0"));
    }

    #[tokio::test]
    async fn wal_runtime_recovers_http_stream_after_restart() {
        let wal_root = std::env::temp_dir().join(format!(
            "ursula-http-wal-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&wal_root);

        {
            let app = router(spawn_wal_runtime(2, 8, &wal_root).expect("runtime"));
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/benchcmp/wal-http")
                        .header(CONTENT_TYPE, "text/plain")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::CREATED);

            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/benchcmp/wal-http/append-batch")
                        .header(CONTENT_TYPE, "text/plain")
                        .body(Body::from(batch_body(&[
                            b"persisted".as_slice(),
                            b"-batch".as_slice(),
                        ])))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);

            let response = app
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/__ursula/metrics")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("metrics body");
            let body = std::str::from_utf8(&body).expect("utf8 body");
            assert!(body.contains("\"wal_batches\":2"));
            assert!(body.contains("\"wal_records\":2"));
            assert!(body.contains("\"wal_write_ns\":"));
            assert!(body.contains("\"wal_sync_ns\":"));
        }

        let app = router(spawn_wal_runtime(2, 8, &wal_root).expect("recovered runtime"));
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/wal-http?offset=0&max_bytes=32")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"persisted-batch");

        std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
    }

    #[tokio::test]
    async fn raft_runtime_serves_http_subset_and_writes_core_journal() {
        let raft_root = std::env::temp_dir().join(format!(
            "ursula-http-raft-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&raft_root);

        let app = router(spawn_raft_runtime(1, 1, &raft_root).expect("runtime"));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/raft-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/raft-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("raft-payload"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/raft-http?offset=0&max_bytes=32")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"raft-payload");

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"wal_batches\":"));
        assert!(!body.contains("\"wal_batches\":0"));
        assert!(body.contains("\"wal_records\":"));
        assert!(!body.contains("\"wal_records\":0"));
        assert!(body.contains("\"wal_write_ns\":"));
        assert!(body.contains("\"wal_sync_ns\":"));

        let journal_path = raft_root.join("core-0").join("journal.bin");
        let journal_len = std::fs::metadata(&journal_path)
            .expect("raft core journal")
            .len();
        assert!(
            journal_len > 0,
            "expected raft journal records, got {journal_len} bytes"
        );

        std::fs::remove_dir_all(&raft_root).expect("remove raft root");
    }

    #[tokio::test]
    async fn static_grpc_raft_runtime_can_use_core_journal() {
        let raft_root = std::env::temp_dir().join(format!(
            "ursula-http-static-raft-log-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&raft_root);

        let (runtime, registry) = spawn_static_grpc_raft_runtime(
            1,
            1,
            1,
            [(1, "http://127.0.0.1:4477".to_owned())],
            true,
            &raft_root,
        )
        .expect("runtime");
        runtime.warm_all_groups().await.expect("warm group");
        let raft = registry
            .get(RaftGroupId(0))
            .expect("registered static raft group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static durable gRPC Raft group should elect node 1")
            .await
            .expect("wait for leader");
        let app = router_with_static_raft_cluster(
            runtime,
            registry,
            [(1, "http://127.0.0.1:4477".to_owned())],
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/static-raft-log")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            status,
            StatusCode::CREATED,
            "create body={}",
            std::str::from_utf8(&body).unwrap_or("<non-utf8>")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/static-raft-log")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("static-raft-payload"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/static-raft-log?offset=0&max_bytes=64")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"static-raft-payload");

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"wal_batches\":"));
        assert!(body.contains("\"wal_records\":"));

        let journal_path = raft_root.join("core-0").join("journal.bin");
        assert!(journal_path.exists(), "core journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("core journal metadata")
                .len()
                > 0,
            "core journal should contain records"
        );

        std::fs::remove_dir_all(&raft_root).expect("remove raft root");
    }

    #[tokio::test]
    async fn static_grpc_raft_runtime_recovers_from_core_journal_after_restart() {
        let raft_root = std::env::temp_dir().join(format!(
            "ursula-http-static-raft-log-restart-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&raft_root);
        let peers = [(1, "http://127.0.0.1:4477".to_owned())];

        {
            let (runtime, registry) =
                spawn_static_grpc_raft_runtime(1, 1, 1, peers.clone(), true, &raft_root)
                    .expect("runtime");
            runtime.warm_all_groups().await.expect("warm group");
            let raft = registry
                .get(RaftGroupId(0))
                .expect("registered static raft group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "static durable gRPC Raft group should elect node 1")
                .await
                .expect("wait for leader");
            let app = router_with_static_raft_cluster(runtime, registry, peers.clone());

            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/benchcmp/static-raft-log-restart")
                        .header(CONTENT_TYPE, "text/plain")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::CREATED);

            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/benchcmp/static-raft-log-restart")
                        .header(CONTENT_TYPE, "text/plain")
                        .body(Body::from("restart-payload"))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        let journal_path = raft_root.join("core-0").join("journal.bin");
        assert!(journal_path.exists(), "core journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("core journal metadata")
                .len()
                > 0,
            "core journal should contain records"
        );

        {
            let (runtime, registry) =
                spawn_static_grpc_raft_runtime(1, 1, 1, peers.clone(), false, &raft_root)
                    .expect("restarted runtime");
            runtime
                .warm_all_groups()
                .await
                .expect("warm restarted group");
            let raft = registry
                .get(RaftGroupId(0))
                .expect("registered restarted static raft group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "restarted durable gRPC Raft group should elect node 1")
                .await
                .expect("wait for restarted leader");
            let app = router_with_static_raft_cluster(runtime, registry, peers);

            let response = app
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/benchcmp/static-raft-log-restart?offset=0&max_bytes=64")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            assert_eq!(&body[..], b"restart-payload");
        }

        std::fs::remove_dir_all(&raft_root).expect("remove raft root");
    }

    #[tokio::test]
    async fn raft_memory_runtime_serves_http_subset_without_wal_metrics() {
        let app = router(spawn_raft_memory_runtime(1, 1).expect("runtime"));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/raft-memory-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/raft-memory-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("raft-memory-payload"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/raft-memory-http?offset=0&max_bytes=64")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"raft-memory-payload");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/raft-memory-http/snapshot/00000000000000000019")
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from("raft-state"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/raft-memory-http/bootstrap")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000019"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("bootstrap body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("raft-state"));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"accepted_appends\":1"));
        assert!(body.contains("\"wal_batches\":0"));
        assert!(body.contains("\"wal_records\":0"));
        assert!(body.contains("\"wal_write_ns\":0"));
        assert!(body.contains("\"wal_sync_ns\":0"));
        assert!(body.contains("\"raft_group_count\":0"));
        assert!(body.contains("\"raft_groups\":[]"));
    }

    #[tokio::test]
    async fn raft_grpc_network_dispatches_to_registered_runtime_owned_group() {
        let registry = RaftGroupHandleRegistry::default();
        let mut config = RuntimeConfig::new(1, 1);
        config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config,
            ursula_raft::RegisteredRaftGroupEngineFactory::new(registry.clone()),
        )
        .expect("runtime");
        runtime
            .warm_group(RaftGroupId(0))
            .await
            .expect("warm raft group");
        let app = router_with_raft_registry(runtime, registry);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve raft RPC router");
        });

        let metrics_body = reqwest::get(format!("http://{addr}/__ursula/metrics"))
            .await
            .expect("read registered raft metrics")
            .text()
            .await
            .expect("registered raft metrics body");
        assert!(metrics_body.contains("\"raft_group_count\":1"));
        assert!(metrics_body.contains("\"raft_group_id\":0"));
        assert!(metrics_body.contains("\"node_id\":1"));
        assert!(metrics_body.contains("\"voter_ids\":[1]"));

        let mut network =
            ursula_raft::GrpcRaftNetwork::new(RaftGroupId(0), 1, format!("http://{addr}"));
        let vote_request =
            ursula_raft::UrsulaVoteRequest::new(ursula_raft::UrsulaVote::new(2, 1), None);
        let _: ursula_raft::UrsulaVoteResponse = network
            .vote(vote_request, RPCOption::new(Duration::from_secs(1)))
            .await
            .expect("send vote over gRPC Raft network");

        let mut missing_group =
            ursula_raft::GrpcRaftNetwork::new(RaftGroupId(1), 1, format!("http://{addr}"));
        let err = missing_group
            .vote(
                ursula_raft::UrsulaVoteRequest::new(ursula_raft::UrsulaVote::new(3, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .expect_err("missing group should fail");
        assert!(err.to_string().contains("not registered"), "err={err}");

        let _ = shutdown_tx.send(());
        server.await.expect("server task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn static_grpc_per_group_membership_initializers_distribute_leaders() {
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for node_id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("local addr");
            peers.push((node_id, format!("http://{addr}")));
            listeners.push(listener);
        }

        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            nodes.push(
                spawn_static_grpc_test_node_with_per_group_initializers(
                    node_id,
                    listener,
                    peers.clone(),
                    peers.clone(),
                    true,
                    6,
                )
                .await,
            );
        }

        for (index, node) in nodes.iter().enumerate() {
            tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
                .await
                .unwrap_or_else(|_| panic!("warm node {} groups timed out", index + 1))
                .expect("warm node groups");
        }

        for raw_group_id in 0..6 {
            let expected_leader = u64::from(raw_group_id % 3) + 1;
            let raft_group_id = RaftGroupId(raw_group_id);
            for node in &nodes {
                let raft = node.registry.get(raft_group_id).expect("registered group");
                raft.wait(Some(Duration::from_secs(5)))
                    .current_leader(
                        expected_leader,
                        "per-group initializer should become group leader",
                    )
                    .await
                    .expect("wait for distributed leader");
            }
        }

        for node in nodes {
            node.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn static_grpc_raft_group_engine_replicates_between_routers() {
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for node_id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("local addr");
            peers.push((node_id, format!("http://{addr}")));
            listeners.push(listener);
        }

        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            nodes.push(
                spawn_static_grpc_test_node(
                    node_id,
                    listener,
                    peers.clone(),
                    peers.clone(),
                    node_id == 1,
                    4,
                )
                .await,
            );
        }

        for (index, node) in nodes.iter().enumerate().skip(1) {
            tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
                .await
                .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
                .expect("warm follower groups");
        }
        tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
            .await
            .expect("warm initializing leader groups timed out")
            .expect("warm initializing leader groups");

        for raw_group_id in 0..4 {
            let raft_group_id = RaftGroupId(raw_group_id);
            for node in &nodes {
                let raft = node.registry.get(raft_group_id).expect("registered group");
                raft.wait(Some(Duration::from_secs(5)))
                    .current_leader(1, "static gRPC Raft cluster should elect node 1")
                    .await
                    .expect("wait for shared leader");
            }
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build reqwest client");
        let forwarded_stream = BucketStreamId::new("benchcmp", "follower-forward");
        let follower_response = http_client
            .put(format!("{}/benchcmp/follower-forward", peers[1].1))
            .header(CONTENT_TYPE, "text/plain")
            .body("created-through-forward")
            .send()
            .await
            .expect("send follower write through internal gRPC forwarding");
        assert_eq!(follower_response.status(), StatusCode::CREATED);
        assert_eq!(
            follower_response
                .headers()
                .get(HEADER_STREAM_NEXT_OFFSET)
                .and_then(|value| value.to_str().ok()),
            Some("00000000000000000023")
        );
        let follower_read = http_client
            .get(format!("{}/benchcmp/follower-forward", peers[1].1))
            .send()
            .await
            .expect("send immediate follower read");
        assert_eq!(follower_read.status(), StatusCode::OK);
        assert_eq!(
            follower_read
                .headers()
                .get(HEADER_STREAM_NEXT_OFFSET)
                .and_then(|value| value.to_str().ok()),
            Some("00000000000000000023")
        );
        let follower_body = follower_read.bytes().await.expect("follower read body");
        assert_eq!(&follower_body[..], b"created-through-forward");

        let no_redirect_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build no-redirect reqwest client");
        let follower_sse = no_redirect_client
            .get(format!(
                "{}/benchcmp/follower-forward?offset=now&live=sse",
                peers[1].1
            ))
            .send()
            .await
            .expect("send follower live read");
        assert_eq!(follower_sse.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            follower_sse
                .headers()
                .get(HEADER_URSULA_RAFT_LEADER_ID)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        assert!(
            follower_sse
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|location| location.starts_with(&peers[0].1)
                    && location.ends_with("/benchcmp/follower-forward?offset=now&live=sse"))
        );

        let forwarded_placement = nodes[0].runtime.locate(&forwarded_stream);
        for node in &nodes {
            wait_raft_state_machine_payload(
                &node.registry,
                forwarded_placement,
                &forwarded_stream,
                b"created-through-forward",
                "follower forwarded write replicated",
            )
            .await;
        }

        let leader_sse = http_client
            .get(format!(
                "{}/benchcmp/follower-forward?offset=now&live=sse",
                peers[0].1
            ))
            .send()
            .await
            .expect("open leader SSE before follower append");
        assert_eq!(leader_sse.status(), StatusCode::OK);
        let sse_token = "wake-through-leader-runtime";
        let sse_task = tokio::spawn(async move {
            let mut response = leader_sse;
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.expect("SSE chunk") {
                body.extend_from_slice(&chunk);
                let body_text = String::from_utf8_lossy(&body);
                if body_text.contains(sse_token) {
                    return body_text.into_owned();
                }
            }
            panic!("SSE stream ended before follower append data");
        });
        let follower_append = http_client
            .post(format!("{}/benchcmp/follower-forward", peers[1].1))
            .header(CONTENT_TYPE, "text/plain")
            .body(sse_token)
            .send()
            .await
            .expect("append through follower HTTP");
        assert_eq!(follower_append.status(), StatusCode::NO_CONTENT);
        let sse_body = tokio::time::timeout(Duration::from_secs(5), sse_task)
            .await
            .expect("leader SSE woke after follower append")
            .expect("SSE task");
        assert!(sse_body.contains("event: data"));
        assert!(sse_body.contains(sse_token));

        let (stream_group, stream_id) = (0..10_000)
            .map(|index| {
                let stream_id =
                    BucketStreamId::new("benchcmp", format!("static-grpc-raft-{index}"));
                (nodes[0].runtime.locate(&stream_id).raft_group_id, stream_id)
            })
            .find(|(raft_group_id, _)| raft_group_id.0 == 0)
            .expect("find stream in raft group 0");
        nodes[0]
            .runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create through leader runtime");
        nodes[0]
            .runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                b"replicated-over-grpc".to_vec(),
            ))
            .await
            .expect("append through leader runtime");
        let stream_placement = nodes[0].runtime.locate(&stream_id);

        for node in &nodes {
            let raft = node.registry.get(stream_group).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "static gRPC Raft group should keep node 1 leader")
                .await
                .expect("wait for stable leader after replicated application");
        }
        for node in &nodes {
            wait_raft_state_machine_payload(
                &node.registry,
                stream_placement,
                &stream_id,
                b"replicated-over-grpc",
                "leader write replicated over gRPC transport",
            )
            .await;
        }

        let mut group_streams = Vec::new();
        let mut seen_groups = BTreeMap::new();
        for index in 0..10_000 {
            let stream_id = BucketStreamId::new("benchcmp", format!("multi-group-{index}"));
            let placement = nodes[0].runtime.locate(&stream_id);
            if seen_groups
                .insert(placement.raft_group_id.0, stream_id.clone())
                .is_none()
            {
                group_streams.push((placement, stream_id));
            }
            if group_streams.len() == 4 {
                break;
            }
        }
        assert_eq!(group_streams.len(), 4);

        for (placement, stream_id) in &group_streams {
            let payload = format!("payload-for-group-{}", placement.raft_group_id.0).into_bytes();
            nodes[0]
                .runtime
                .create_stream(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .await
                .expect("create multi-group stream through leader runtime");
            nodes[0]
                .runtime
                .append(AppendRequest::from_bytes(
                    stream_id.clone(),
                    payload.clone(),
                ))
                .await
                .expect("append multi-group stream through leader runtime");

            for node in &nodes {
                wait_raft_state_machine_payload(
                    &node.registry,
                    *placement,
                    stream_id,
                    &payload,
                    "multi-group stream replicated over gRPC transport",
                )
                .await;
            }
        }

        let snapshot = nodes[0]
            .registry
            .build_snapshot_for_transfer(RaftGroupId(0))
            .await
            .expect("build leader snapshot");
        let vote = ursula_raft::UrsulaVote::new(1, 1);
        let mut snapshot_network =
            ursula_raft::GrpcRaftNetwork::new(RaftGroupId(0), 2, peers[1].1.clone());
        let _: SnapshotResponse<UrsulaRaftTypeConfig> = snapshot_network
            .full_snapshot(
                vote,
                snapshot,
                std::future::pending::<ReplicationClosed>(),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .expect("send full snapshot over gRPC Raft network");

        for node in nodes {
            node.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn static_grpc_raft_group_engine_replicates_with_core_journals() {
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for node_id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("local addr");
            peers.push((node_id, format!("http://{addr}")));
            listeners.push(listener);
        }

        let raft_root = std::env::temp_dir().join(format!(
            "ursula-http-static-raft-multinode-log-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&raft_root);

        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            let node_root = raft_root.join(format!("node-{node_id}"));
            nodes.push(
                spawn_static_grpc_test_node_with_log_dir(
                    node_id,
                    listener,
                    peers.clone(),
                    peers.clone(),
                    node_id == 1,
                    2,
                    Some(node_root),
                )
                .await,
            );
        }

        for (index, node) in nodes.iter().enumerate().skip(1) {
            tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
                .await
                .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
                .expect("warm follower groups");
        }
        tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
            .await
            .expect("warm initializing leader groups timed out")
            .expect("warm initializing leader groups");

        for raw_group_id in 0..2 {
            let raft_group_id = RaftGroupId(raw_group_id);
            for node in &nodes {
                let raft = node.registry.get(raft_group_id).expect("registered group");
                raft.wait(Some(Duration::from_secs(5)))
                    .current_leader(1, "durable static gRPC Raft cluster should elect node 1")
                    .await
                    .expect("wait for shared leader");
            }
        }

        let mut group_streams = Vec::new();
        let mut seen_groups = BTreeMap::new();
        for index in 0..10_000 {
            let stream_id = BucketStreamId::new("benchcmp", format!("durable-multi-{index}"));
            let placement = nodes[0].runtime.locate(&stream_id);
            if seen_groups
                .insert(placement.raft_group_id.0, stream_id.clone())
                .is_none()
            {
                group_streams.push((placement, stream_id));
            }
            if group_streams.len() == 2 {
                break;
            }
        }
        assert_eq!(group_streams.len(), 2);

        for (placement, stream_id) in &group_streams {
            let payload = format!("durable-payload-for-group-{}", placement.raft_group_id.0);
            nodes[0]
                .runtime
                .create_stream(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .await
                .expect("create durable multi-group stream through leader runtime");
            nodes[0]
                .runtime
                .append(AppendRequest::from_bytes(
                    stream_id.clone(),
                    payload.as_bytes().to_vec(),
                ))
                .await
                .expect("append durable multi-group stream through leader runtime");

            for node in &nodes {
                wait_raft_state_machine_payload(
                    &node.registry,
                    *placement,
                    stream_id,
                    payload.as_bytes(),
                    "multi-node durable gRPC log replicated stream",
                )
                .await;
            }
        }

        for (node_index, (_, peer_url)) in peers.iter().enumerate() {
            let metrics_body = reqwest::get(format!("{peer_url}/__ursula/metrics"))
                .await
                .unwrap_or_else(|err| panic!("read node {} metrics: {err}", node_index + 1))
                .text()
                .await
                .expect("metrics body");
            assert!(
                !metrics_body.contains("\"wal_records\":0"),
                "node {} should record durable OpenRaft log writes: {metrics_body}",
                node_index + 1
            );

            let journal_path = raft_root
                .join(format!("node-{}", node_index + 1))
                .join("core-0")
                .join("journal.bin");
            assert!(journal_path.exists(), "node journal should exist");
            assert!(
                std::fs::metadata(&journal_path)
                    .expect("node journal metadata")
                    .len()
                    > 0,
                "node journal should contain records"
            );
        }

        for node in nodes {
            node.shutdown().await;
        }
        std::fs::remove_dir_all(&raft_root).expect("remove raft root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn static_grpc_raft_durable_cold_flush_replicates_manifest() {
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for node_id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("local addr");
            peers.push((node_id, format!("http://{addr}")));
            listeners.push(listener);
        }

        let raft_root = std::env::temp_dir().join(format!(
            "ursula-http-static-raft-durable-cold-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&raft_root);
        let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));

        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            let node_root = raft_root.join(format!("node-{node_id}"));
            nodes.push(
                spawn_static_grpc_test_node_with_storage(
                    node_id,
                    listener,
                    peers.clone(),
                    peers.clone(),
                    node_id == 1,
                    1,
                    StaticGrpcTestNodeStorage {
                        raft_log_dir: Some(node_root),
                        cold_store: Some(cold_store.clone()),
                        per_group_initializers: false,
                    },
                )
                .await,
            );
        }

        for (index, node) in nodes.iter().enumerate().skip(1) {
            tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
                .await
                .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
                .expect("warm follower groups");
        }
        tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
            .await
            .expect("warm leader groups timed out")
            .expect("warm leader groups");

        for node in &nodes {
            let raft = node.registry.get(RaftGroupId(0)).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "durable cold static gRPC cluster should elect node 1")
                .await
                .expect("wait for shared leader");
        }

        let stream_id = BucketStreamId::new("benchcmp", "durable-cold-manifest");
        let placement = nodes[0].runtime.locate(&stream_id);
        let payload = b"durable-cold-replicated-payload".to_vec();
        nodes[0]
            .runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create cold stream through leader runtime");
        nodes[0]
            .runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                payload.clone(),
            ))
            .await
            .expect("append cold stream through leader runtime");
        let flushed = nodes[0]
            .runtime
            .flush_cold_group_batch_once(
                placement.raft_group_id,
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 4,
                    max_flush_bytes: 4,
                },
                8,
            )
            .await
            .expect("flush replicated cold manifest");
        assert!(
            flushed.len() >= 2,
            "batch cold flush should publish multiple chunks"
        );

        for node in &nodes {
            wait_raft_state_machine_payload(
                &node.registry,
                placement,
                &stream_id,
                &payload,
                "multi-node durable gRPC cold manifest replicated stream",
            )
            .await;
            let raft = node
                .registry
                .get(placement.raft_group_id)
                .expect("registered raft group");
            let mut last_snapshot = None;
            for _ in 0..100 {
                let snapshot = raft
                    .with_state_machine(|state_machine| {
                        Box::pin(async move { state_machine.group_snapshot().await })
                    })
                    .await
                    .expect("snapshot node raft state machine")
                    .expect("group snapshot");
                let entry = snapshot
                    .stream_snapshot
                    .streams
                    .iter()
                    .find(|entry| entry.metadata.stream_id == stream_id)
                    .cloned()
                    .expect("stream snapshot entry");
                if !entry.cold_chunks.is_empty() && entry.payload.len() < payload.len() {
                    last_snapshot = Some(entry);
                    break;
                }
                last_snapshot = Some(entry);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let entry = last_snapshot.expect("stream snapshot entry");
            assert!(
                !entry.cold_chunks.is_empty(),
                "replicated stream should have cold manifest chunks"
            );
            assert!(
                entry.payload.len() < payload.len(),
                "replicated hot payload should shrink after cold flush"
            );
        }

        for (node_index, _) in peers.iter().enumerate() {
            let journal_path = raft_root
                .join(format!("node-{}", node_index + 1))
                .join("core-0")
                .join("journal.bin");
            assert!(journal_path.exists(), "node journal should exist");
            assert!(
                std::fs::metadata(&journal_path)
                    .expect("node journal metadata")
                    .len()
                    > 0,
                "node journal should contain records"
            );
        }

        let metrics_body = reqwest::get(format!("{}/__ursula/metrics", peers[0].1))
            .await
            .expect("read leader metrics")
            .text()
            .await
            .expect("metrics body");
        assert!(
            !metrics_body.contains("\"cold_flush_publishes\":0"),
            "leader should report cold metadata publishes: {metrics_body}"
        );
        assert!(
            !metrics_body.contains("\"wal_records\":0"),
            "leader should record durable OpenRaft log writes: {metrics_body}"
        );

        for node in nodes {
            node.shutdown().await;
        }
        std::fs::remove_dir_all(&raft_root).expect("remove raft root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn static_grpc_raft_installs_snapshot_for_late_learner_over_tcp() {
        run_static_grpc_late_learner_snapshot_over_tcp(None).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn static_grpc_raft_installs_snapshot_for_late_learner_with_core_journals() {
        let raft_root = std::env::temp_dir().join(format!(
            "ursula-http-static-raft-late-learner-log-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&raft_root);

        run_static_grpc_late_learner_snapshot_over_tcp(Some(raft_root.clone())).await;

        std::fs::remove_dir_all(&raft_root).expect("remove raft root");
    }

    async fn run_static_grpc_late_learner_snapshot_over_tcp(raft_root: Option<PathBuf>) {
        let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 1 listener");
        let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 2 listener");
        let listener3 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node 3 listener");
        let peers = vec![
            (
                1,
                format!("http://{}", listener1.local_addr().expect("node 1 addr")),
            ),
            (
                2,
                format!("http://{}", listener2.local_addr().expect("node 2 addr")),
            ),
            (
                3,
                format!("http://{}", listener3.local_addr().expect("node 3 addr")),
            ),
        ];
        let initial_peers = peers[..2].to_vec();
        let mut nodes = vec![
            spawn_static_grpc_test_node_with_log_dir(
                1,
                listener1,
                initial_peers.clone(),
                peers.clone(),
                true,
                1,
                raft_root.as_ref().map(|root| root.join("node-1")),
            )
            .await,
            spawn_static_grpc_test_node_with_log_dir(
                2,
                listener2,
                initial_peers.clone(),
                peers.clone(),
                false,
                1,
                raft_root.as_ref().map(|root| root.join("node-2")),
            )
            .await,
        ];

        nodes[1]
            .runtime
            .warm_all_groups()
            .await
            .expect("warm follower group");
        nodes[0]
            .runtime
            .warm_all_groups()
            .await
            .expect("warm leader group");

        for node in &nodes {
            let raft = node.registry.get(RaftGroupId(0)).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "two-node static gRPC cluster should elect node 1")
                .await
                .expect("wait for node 1 leadership");
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build reqwest client");
        let stream_id = BucketStreamId::new("benchcmp", "late-learner-snapshot");
        let leader_base = peers[0].1.as_str();
        let create = client
            .put(format!("{leader_base}/benchcmp/late-learner-snapshot"))
            .header(CONTENT_TYPE, "application/octet-stream")
            .send()
            .await
            .expect("create stream through leader http");
        assert_eq!(create.status(), StatusCode::CREATED);
        let payload = b"snapshot-over-grpc".to_vec();
        let append = client
            .post(format!("{leader_base}/benchcmp/late-learner-snapshot"))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(payload.clone())
            .send()
            .await
            .expect("append stream through leader http");
        assert_eq!(append.status(), StatusCode::NO_CONTENT);

        let placement = nodes[0].runtime.locate(&stream_id);
        for node in &nodes {
            wait_raft_state_machine_payload(
                &node.registry,
                placement,
                &stream_id,
                &payload,
                "initial two-node write replicated before snapshot",
            )
            .await;
        }

        let leader_raft = nodes[0].registry.get(RaftGroupId(0)).expect("leader group");
        let leader_metrics = leader_raft.metrics().borrow_watched().clone();
        let snapshot_log_id = leader_metrics
            .last_applied
            .expect("leader applied stream append");
        leader_raft
            .trigger()
            .snapshot()
            .await
            .expect("trigger leader snapshot");
        leader_raft
            .wait(Some(Duration::from_secs(5)))
            .snapshot(snapshot_log_id, "leader snapshot includes stream append")
            .await
            .expect("wait for leader snapshot");
        leader_raft
            .trigger()
            .purge_log(snapshot_log_id.index())
            .await
            .expect("trigger leader purge");
        leader_raft
            .wait(Some(Duration::from_secs(5)))
            .purged(Some(snapshot_log_id), "leader purged snapshotted logs")
            .await
            .expect("wait for leader purge");

        nodes.push(
            spawn_static_grpc_test_node_with_log_dir(
                3,
                listener3,
                peers.clone(),
                peers.clone(),
                false,
                1,
                raft_root.as_ref().map(|root| root.join("node-3")),
            )
            .await,
        );
        nodes[2]
            .runtime
            .warm_all_groups()
            .await
            .expect("warm late learner group");
        let late_learner = nodes[2]
            .registry
            .get(RaftGroupId(0))
            .expect("late learner group");
        let learner_added = leader_raft
            .add_learner(3, BasicNode::new(peers[2].1.clone()), true)
            .await
            .expect("add late learner over gRPC");
        late_learner
            .wait(Some(Duration::from_secs(10)))
            .snapshot(snapshot_log_id, "late learner installed gRPC snapshot")
            .await
            .expect("wait for late learner snapshot");
        late_learner
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(
                Some(learner_added.log_id.index()),
                "late learner applied learner membership",
            )
            .await
            .expect("wait for late learner catch-up");

        wait_raft_state_machine_payload(
            &nodes[2].registry,
            placement,
            &stream_id,
            &payload,
            "late learner restored stream from gRPC snapshot",
        )
        .await;

        let follower_read = client
            .get(format!(
                "{}/benchcmp/late-learner-snapshot?offset=0&max_bytes=64",
                peers[2].1
            ))
            .send()
            .await
            .expect("read stream from late learner http");
        assert_eq!(follower_read.status(), StatusCode::OK);
        let follower_body = follower_read.bytes().await.expect("late learner body");
        assert_eq!(&follower_body[..], &payload[..]);

        let leader_metrics_response = client
            .get(format!("{leader_base}/__ursula/metrics"))
            .send()
            .await
            .expect("read leader metrics");
        assert_eq!(leader_metrics_response.status(), StatusCode::OK);
        let leader_metrics_body = leader_metrics_response
            .text()
            .await
            .expect("leader metrics body");
        assert!(leader_metrics_body.contains("\"raft_group_count\":1"));
        assert!(leader_metrics_body.contains("\"raft_group_id\":0"));
        assert!(
            leader_metrics_body
                .contains(&format!("\"snapshot_index\":{}", snapshot_log_id.index()))
        );
        assert!(
            leader_metrics_body.contains(&format!("\"purged_index\":{}", snapshot_log_id.index()))
        );

        let late_metrics_response = client
            .get(format!("{}/__ursula/metrics", peers[2].1))
            .send()
            .await
            .expect("read late learner metrics");
        assert_eq!(late_metrics_response.status(), StatusCode::OK);
        let late_metrics_body = late_metrics_response
            .text()
            .await
            .expect("late learner metrics body");
        assert!(late_metrics_body.contains("\"raft_group_count\":1"));
        assert!(late_metrics_body.contains("\"raft_group_id\":0"));
        assert!(
            late_metrics_body.contains(&format!("\"snapshot_index\":{}", snapshot_log_id.index()))
        );
        assert!(late_metrics_body.contains("\"voter_ids\":[1,2]"));
        assert!(late_metrics_body.contains("\"learner_ids\":[3]"));

        if let Some(raft_root) = &raft_root {
            for node_id in 1..=3 {
                let journal_path = raft_root
                    .join(format!("node-{node_id}"))
                    .join("core-0")
                    .join("journal.bin");
                assert!(journal_path.exists(), "node journal should exist");
                assert!(
                    std::fs::metadata(&journal_path)
                        .expect("node journal metadata")
                        .len()
                        > 0,
                    "node journal should contain records"
                );
            }
        }

        for node in nodes {
            node.shutdown().await;
        }
    }

    #[tokio::test]
    async fn flush_cold_endpoint_uploads_and_reads_back_segments() {
        let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(1, 1),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("runtime");
        let app = router(runtime);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/http-cold")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/http-cold")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("abcdef"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/__ursula/flush-cold/benchcmp/http-cold?min_hot_bytes=4&max_bytes=4")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/http-cold?offset=0&max_bytes=6")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(&body[..], b"abcdef");
    }

    #[tokio::test]
    async fn cold_backpressure_returns_service_unavailable_and_metrics() {
        let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(4)),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("runtime");
        let app = router(runtime);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/http-cold-backpressure")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/http-cold-backpressure")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("abcd"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/http-cold-backpressure")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("e"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error body");
        assert!(
            std::str::from_utf8(&body)
                .expect("utf8 body")
                .contains("ColdBackpressure")
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"cold_hot_bytes\":4"));
        assert!(body.contains("\"cold_backpressure_events\":1"));
        assert!(body.contains("\"cold_backpressure_bytes\":1"));
    }

    #[tokio::test]
    async fn snapshot_and_bootstrap_routes_follow_extension_semantics() {
        let app = test_router();
        let stream_uri = "/benchcmp/snapshot-http";

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(stream_uri)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        for payload in ["abc", "de"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(stream_uri)
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-http/snapshot/00000000000000000003")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"state":"abc"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response
                .headers()
                .get(HEADER_STREAM_SNAPSHOT_OFFSET)
                .unwrap(),
            "00000000000000000003"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(stream_uri)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(HEADER_STREAM_SNAPSHOT_OFFSET)
                .unwrap(),
            "00000000000000000003"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-http/snapshot")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response.headers().get(LOCATION).unwrap(),
            "/benchcmp/snapshot-http/snapshot/00000000000000000003"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-http/snapshot/00000000000000000003")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000003"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"{"state":"abc"}"#);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-http?offset=0&max_bytes=1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::GONE);
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000003"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-http/bootstrap")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("multipart/mixed; boundary=ursula-bootstrap-")
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000005"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = std::str::from_utf8(&body).expect("multipart utf8");
        assert!(body.contains(r#"{"state":"abc"}"#));
        assert!(body.contains("de"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/benchcmp/snapshot-http/snapshot/00000000000000000003")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn bootstrap_without_snapshot_emits_empty_snapshot_part_and_rejects_live() {
        let app = test_router();
        let stream_uri = "/benchcmp/bootstrap-nosnapshot";

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(stream_uri)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(stream_uri)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from("one"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/bootstrap-nosnapshot/snapshot")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(stream_uri)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(HEADER_STREAM_SNAPSHOT_OFFSET)
                .is_none()
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/bootstrap-nosnapshot/bootstrap?live=sse")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/bootstrap-nosnapshot/bootstrap")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(HEADER_STREAM_SNAPSHOT_OFFSET)
                .unwrap(),
            "-1"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
            "00000000000000000003"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = std::str::from_utf8(&body).expect("multipart utf8");
        assert!(body.contains("Content-Type: application/octet-stream\r\n\r\n\r\n--"));
        assert!(body.contains("one"));
    }

    #[tokio::test]
    async fn snapshot_publish_errors_and_overwrite_follow_extension_statuses() {
        let app = test_router();
        let stream_uri = "/benchcmp/snapshot-errors";

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(stream_uri)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        for payload in ["abc", "de"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(stream_uri)
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-errors/snapshot/-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000002")
                    .body(Body::from("ab-state"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000006")
                    .body(Body::from("too-far"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000003")
                    .body(Body::from("abc-state"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000002")
                    .body(Body::from("old-state"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::GONE);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000005")
                    .body(Body::from("abcde-state"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000003")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000003")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-errors?offset=3&max_bytes=2")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::GONE);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/snapshot-errors/bootstrap")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(HEADER_STREAM_SNAPSHOT_OFFSET)
                .unwrap(),
            "00000000000000000005"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = std::str::from_utf8(&body).expect("multipart utf8");
        assert!(body.contains("abcde-state"));
        assert!(!body.contains("abc-state\r\n"));
    }

    fn test_router() -> Router {
        router(spawn_default_runtime(2, 8).expect("runtime"))
    }

    fn batch_body(payloads: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        for payload in payloads {
            body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            body.extend_from_slice(payload);
        }
        body
    }
}
