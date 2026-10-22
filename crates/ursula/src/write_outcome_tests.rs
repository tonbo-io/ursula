//! The write-outcome inventory, driven through a real node and a real
//! gateway (`ursula_headers::write_outcome`): every row that a node, the
//! gateway or an embedder sends is produced here by the production router,
//! directly and through `ursula_gateway::Gateway`, and `classify` must read
//! it as the row says. Faults a single in-memory node has no other way to
//! show (a lost leadership, a lost response, backpressure, a stopped group)
//! come from a scripted engine behind the real router. The rows from a
//! layer outside Ursula, and a transport failure, are covered by the unit
//! tests of `ursula-headers`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::response::Response;
use tower::ServiceExt;
use ursula_gateway::Gateway;
use ursula_gateway::GatewayConfig;
use ursula_gateway::admission::StaticQuotaProvider;
use ursula_gateway::auth::AccessControl;
use ursula_gateway::auth::AuthenticationError;
use ursula_gateway::auth::AuthorizationDecision;
use ursula_gateway::auth::AuthorizationError;
use ursula_gateway::auth::AuthorizationFuture;
use ursula_gateway::auth::AuthorizationRequest;
use ursula_gateway::auth::Authorizer;
use ursula_gateway::auth::PrincipalResolver;
use ursula_gateway::auth::PrincipalResolverFuture;
use ursula_gateway::auth::VerifiedPrincipal;
use ursula_gateway::refusal::EmbedderRefusal;
use ursula_gateway::uploads::UploadLimits;
use ursula_gateway::uploads::UploadLimitsSpec;
use ursula_headers::ErrorBody;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::WriteKind;
use ursula_headers::WriteOutcome;
use ursula_headers::classify;
use ursula_headers::names;
use ursula_headers::write_outcome::inventory::INVENTORY;
use ursula_headers::write_outcome::inventory::Row;
use ursula_headers::write_outcome::inventory::Source;
use ursula_headers::write_outcome::inventory::row;
use ursula_runtime::AdvanceRetentionRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdOperation;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::GroupAdvanceRetentionFuture;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupBucketUsageFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupPublishSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupRouteWriteFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::KnownLeader;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::Proposal;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ResponseKind;
use ursula_runtime::RouteWriteRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::WriteRoute;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::ClientWriteLeaderRouter;
use crate::HttpState;
use crate::IngressAdmission;
use crate::PublishTarget;
use crate::WallClock;
use crate::client_router_with_admission;
use crate::front_door::BodyState;
use crate::snapshot_parts::KeyCheck;
use crate::snapshot_parts::key_check;
use crate::snapshot_parts_tests::Gate;
use crate::snapshot_parts_tests::commit_request;
use crate::snapshot_parts_tests::digest_of;
use crate::wal_disk::WalDiskMonitor;

const TEXT: &str = "text/plain";
const OCTET: &str = "application/octet-stream";
const BEARER: &str = "Bearer good";
/// A producer idle this long is retired at its next write.
const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// A declared body this large asks the leader for an early answer, which
/// routes the write before its body is read.
const ROUTED_BODY_BYTES: usize = 64 * 1024;
/// The small node's in-flight body budget.
const SMALL_BUDGET_BYTES: u64 = 1024 * 1024;

/// An answer as a client reads it.
struct Answer {
    status: u16,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

impl std::fmt::Debug for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<String> = self
            .headers
            .iter()
            .map(|(name, value)| format!("{name}: {}", String::from_utf8_lossy(value)))
            .collect();
        write!(
            f,
            "{} {headers:?} {}",
            self.status,
            String::from_utf8_lossy(&self.body)
        )
    }
}

impl Answer {
    async fn read(response: Response) -> Self {
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
            .collect();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("answer body")
            .to_vec();
        Self {
            status,
            headers,
            body,
        }
    }

    fn pairs(&self) -> impl Iterator<Item = (&str, &[u8])> + Clone {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    }

    fn outcome(&self, kind: WriteKind) -> WriteOutcome {
        classify(kind, self.status, self.pairs(), &self.body)
    }

    /// The names of the protocol's typed headers the answer carries.
    fn typed_names(&self) -> BTreeSet<&'static str> {
        TYPED_HEADERS
            .iter()
            .copied()
            .filter(|name| self.header(name).is_some())
            .collect()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .and_then(|(_, value)| std::str::from_utf8(value).ok())
    }

    fn code(&self) -> Option<ursula_headers::ErrorCode> {
        ErrorBody::parse(&self.body).ok().map(|body| body.code)
    }
}

/// The protocol's typed headers that an inventory sample may name.
const TYPED_HEADERS: &[&str] = &[
    names::PRODUCER_EPOCH,
    names::PRODUCER_SEQ,
    names::PRODUCER_OUTCOME,
    names::PRODUCER_CLOSED_EPOCH,
    names::PRODUCER_CLOSED_SEQ,
    names::PRODUCER_EXPECTED_SEQ,
    names::PRODUCER_RECEIVED_SEQ,
    names::STREAM_OFFSET,
    names::STREAM_NEXT_OFFSET,
    names::STREAM_CLOSED,
    names::STREAM_INCARNATION,
    names::STREAM_SNAPSHOT_OFFSET,
    names::STREAM_SNAPSHOT_DIGEST,
    names::STREAM_SNAPSHOT_PART,
    names::STREAM_LAST_SEQ,
    names::X_URSULA_RAFT_LEADER_ID,
    names::X_URSULA_RAFT_GROUP,
    names::X_URSULA_RAFT_TERM,
    names::URSULA_MAX_REQUEST_BYTES,
    "retry-after",
    "location",
];

/// What a scripted engine answers instead of its in-memory engine.
#[derive(Default)]
struct Script {
    /// The next write fails with this.
    write_error: Option<GroupEngineError>,
    /// The next head fails with this.
    head_error: Option<GroupEngineError>,
    /// Every routing query answers this until it is cleared.
    route: Option<WriteRoute>,
}

type SharedScript = Arc<Mutex<Script>>;

fn next_error(script: &SharedScript) -> Option<GroupEngineError> {
    script
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .write_error
        .take()
}

/// An in-memory engine whose writes and routing answer from its script
/// when it holds one.
struct ScriptedEngine {
    inner: InMemoryGroupEngine,
    script: SharedScript,
}

macro_rules! scripted {
    ($self:ident, $call:expr) => {
        match next_error(&$self.script) {
            Some(error) => Box::pin(async move { Err(error) }),
            None => $call,
        }
    };
}

impl GroupEngine for ScriptedEngine {
    fn upload_fence(&self) -> Option<ursula_runtime::UploadFence> {
        self.inner.upload_fence()
    }

    fn route_write<'a>(
        &'a mut self,
        request: RouteWriteRequest,
        placement: ShardPlacement,
    ) -> GroupRouteWriteFuture<'a> {
        let route = self
            .script
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .route
            .clone();
        match route {
            Some(route) => Box::pin(async move { Ok(route) }),
            None => self.inner.route_write(request, placement),
        }
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        scripted!(
            self,
            self.inner.create_stream(request, placement, admission)
        )
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream_external(request, placement)
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        let error = self
            .script
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .head_error
            .take();
        match error {
            Some(error) => Box::pin(async move { Err(error) }),
            None => self.inner.head_stream(request, placement),
        }
    }

    fn bucket_usage<'a>(&'a mut self, placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        self.inner.bucket_usage(placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        self.inner.read_stream(request, placement)
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        scripted!(self, self.inner.close_stream(request, placement))
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        scripted!(self, self.inner.append(request, placement, admission))
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        scripted!(self, self.inner.append_external(request, placement))
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        scripted!(self, self.inner.publish_snapshot(request, placement))
    }

    fn advance_retention<'a>(
        &'a mut self,
        request: AdvanceRetentionRequest,
        placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        self.inner.advance_retention(request, placement)
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        self.inner.snapshot(placement)
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        self.inner.install_snapshot(snapshot)
    }
}

struct ScriptedEngineFactory {
    cold_store: Option<Arc<ColdStore>>,
    script: SharedScript,
}

impl GroupEngineFactory for ScriptedEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let mut inner = match &self.cold_store {
            Some(cold_store) => InMemoryGroupEngine::with_cold_store(cold_store.clone()),
            None => InMemoryGroupEngine::default(),
        };
        // An engine without Raft starts its group with its genesis.
        inner.start_genesis(placement, 0);
        let script = self.script.clone();
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(ScriptedEngine { inner, script });
            Ok(engine)
        })
    }
}

#[derive(Clone)]
struct Clock(Arc<AtomicU64>);

impl WallClock for Clock {
    fn unix_time_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Accepts the token `good`; `invalid` fails verification and `auth-down`
/// finds the verifier unavailable.
struct Resolver;

impl PrincipalResolver for Resolver {
    fn resolve<'a>(&'a self, bearer_token: &'a str) -> PrincipalResolverFuture<'a> {
        Box::pin(async move {
            match bearer_token {
                "invalid" => Err(AuthenticationError::InvalidCredential),
                "auth-down" => Err(AuthenticationError::Unavailable),
                _ => Ok(VerifiedPrincipal {
                    issuer: "https://issuer.invalid".to_owned(),
                    subject: "tenant".to_owned(),
                    client_id: "client".to_owned(),
                    scopes: BTreeSet::new(),
                    issued_at: 0,
                    expires_at: u64::from(u32::MAX),
                    token_id: "token".to_owned(),
                }),
            }
        })
    }
}

/// Allows every stream but `deny`, `conceal` and `authz-down`.
struct StreamAuthorizer;

impl Authorizer for StreamAuthorizer {
    fn authorize<'a>(&'a self, request: AuthorizationRequest) -> AuthorizationFuture<'a> {
        Box::pin(async move {
            match request.resource.stream_id.as_deref() {
                Some("deny") => Ok(AuthorizationDecision::Deny),
                Some("conceal") => Ok(AuthorizationDecision::ConcealAsNotFound),
                Some("authz-down") => Err(AuthorizationError::Unavailable),
                _ => Ok(AuthorizationDecision::Allow),
            }
        })
    }
}

const QUOTAS: &str = r#"
[[bucket]]
id = "small"
max_request_body_bytes = 1024

[[bucket]]
id = "limited"
requests_per_sec = 1

[[bucket]]
id = "upcount"
max_concurrent_uploads = 1

[[bucket]]
id = "upbytes"
max_upload_bytes = 3072
"#;

/// A gateway with access control, the quotas and small upload limits,
/// served on its own listener so its requests are read off a socket: a body
/// of 4 KiB at most, a budget of 8 KiB of which 4 KiB is for bodies of 1 KiB
/// at most, 50 ms to wait for the budget, and 300 ms for a body to stall.
struct UploadGateway {
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl UploadGateway {
    async fn spawn(upstream: &str) -> Self {
        let mut config = gateway_config(vec![upstream.to_owned()]);
        config.uploads = UploadLimits::try_from(UploadLimitsSpec {
            max_request_body_bytes: 4 << 10,
            budget_bytes: 8 << 10,
            small_body_headroom_bytes: 4 << 10,
            small_body_max_bytes: 1 << 10,
            budget_wait: Duration::from_millis(50),
            stall_timeout: Duration::from_millis(300),
            min_bytes_per_sec: 0,
        })
        .expect("upload limits");
        let gateway = Arc::new(
            Gateway::with_access_control(
                config,
                AccessControl::new(Arc::new(Resolver), Arc::new(StreamAuthorizer)),
            )
            .with_quota_provider(Arc::new(
                StaticQuotaProvider::from_toml_str(QUOTAS).expect("quotas"),
            )),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let addr = listener.local_addr().expect("gateway address");
        let app = Router::new().fallback(move |request: Request<Body>| {
            let gateway = Arc::clone(&gateway);
            async move { gateway.handle(request).await }
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve gateway");
        });
        Self { addr, server }
    }

    /// Opens a producer append to `path` that declares `declared` bytes and
    /// sends `sent` of them, over HTTP/1.1.
    async fn start(&self, path: &str, declared: usize, sent: usize) -> tokio::net::TcpStream {
        use tokio::io::AsyncWriteExt;
        let mut socket = tokio::net::TcpStream::connect(self.addr)
            .await
            .expect("connect");
        let head = format!(
            "POST {path} HTTP/1.1\r\nhost: gateway\r\nauthorization: {BEARER}\r\n\
             content-type: {TEXT}\r\nproducer-id: p\r\nproducer-epoch: 0\r\n\
             producer-seq: 0\r\nconnection: close\r\ncontent-length: {declared}\r\n\r\n"
        );
        socket.write_all(head.as_bytes()).await.expect("send head");
        socket
            .write_all(&vec![b'a'; sent])
            .await
            .expect("send body");
        socket
    }

    /// Reads the whole answer of `socket`, which the gateway closes after it.
    async fn answer(mut socket: tokio::net::TcpStream) -> Answer {
        use tokio::io::AsyncReadExt;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut raw))
            .await
            .expect("an answer in time")
            .expect("read the answer");
        let split = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("a head");
        let head = String::from_utf8(raw[..split].to_vec()).expect("an ASCII head");
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|status| status.parse().ok())
            .expect("a status");
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().as_bytes().to_vec()))
            .collect();
        Answer {
            status,
            headers,
            body: raw
                .get(split.saturating_add(4)..)
                .unwrap_or_default()
                .to_vec(),
        }
    }

    /// A large body of `path`'s bucket, held arriving, then another, whole.
    async fn second_of_two(&self, path: &str, held: usize) -> Answer {
        let held = self.start(path, held, 100).await;
        // The gateway reads the head of the held body and counts it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let answer = Self::answer(self.start(path, 2048, 2048).await).await;
        drop(held);
        answer
    }
}

impl Drop for UploadGateway {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn gateway_config(upstreams: Vec<String>) -> GatewayConfig {
    GatewayConfig {
        listen: "127.0.0.1:0".parse().expect("listen"),
        upstreams,
        response_header_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_secs(2),
        upstream_tcp_user_timeout_secs: std::num::NonZeroU64::new(30).expect("non-zero"),
        uploads: UploadLimits::default(),
        raft_group_count: None,
        cors_allowed_origins: Vec::new(),
        usage_chunk_bytes: None,
    }
}

/// A node behind its own TCP listener, with the gateway in front of it.
struct Node {
    app: Router,
    state: HttpState,
    url: String,
    script: SharedScript,
    cold_store: Option<Arc<ColdStore>>,
    clock: Arc<AtomicU64>,
    wal_disk: WalDiskMonitor,
    raft_log: ursula_raft::SnapshotBuildCoordinator,
    gateway: Gateway,
    /// Whether the gateway checks access, so a request through it needs the
    /// bearer.
    authenticated: bool,
    server: tokio::task::JoinHandle<()>,
}

impl Node {
    /// `cold`: a node with a memory cold store. `budget`: its in-flight body
    /// budget. `access`: a gateway with access control and quotas.
    async fn spawn(cold: bool, budget: Option<u64>, access: bool) -> Self {
        let cold_store = cold.then(|| Arc::new(ColdStore::memory().expect("memory cold store")));
        let script = SharedScript::default();
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(1, 1),
            ScriptedEngineFactory {
                cold_store: cold_store.clone(),
                script: script.clone(),
            },
            cold_store.clone(),
        )
        .expect("runtime");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind node");
        let url = format!("http://{}", listener.local_addr().expect("node address"));
        let clock = Arc::new(AtomicU64::new(
            u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("after the epoch")
                    .as_millis(),
            )
            .expect("ms fit"),
        ));
        let wal_disk = WalDiskMonitor::new(100, 200);
        let raft_log = ursula_raft::SnapshotBuildCoordinator::new(1);
        let mut state = HttpState::new(runtime)
            .with_wall_clock(Clock(clock.clone()))
            .with_wal_disk_monitor(wal_disk.clone());
        // Peer 2 is this node itself: a redirect to it is a redirect the
        // gateway can follow, and it answers each hop with a redirect again.
        state.fields_mut().client_write_router = Some(ClientWriteLeaderRouter::new(
            crate::tests::bound_peer_cluster(2, crate::shared_peers([(2, url.clone())])),
        ));
        // A stream holds two staged, unused parts at most, so the third is
        // refused for the bound.
        state.set_staged_parts(crate::staged_parts::StagedParts::for_test(2, u64::MAX));
        let admission = match budget {
            Some(bytes) => IngressAdmission::new(&ursula_config::ServerConfig {
                http_inflight_body_size: ursula_config::HumanSize::bytes(bytes),
                ..ursula_config::ServerConfig::default()
            }),
            None => IngressAdmission::default(),
        }
        .with_wal_disk_monitor(wal_disk.clone())
        .with_raft_log_pressure(Some(raft_log.clone()));
        let app = client_router_with_admission(state.clone(), admission);
        let served = app.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, served).await.expect("serve node");
        });
        let gateway = if access {
            Gateway::with_access_control(
                gateway_config(vec![url.clone()]),
                AccessControl::new(Arc::new(Resolver), Arc::new(StreamAuthorizer)),
            )
            .with_quota_provider(Arc::new(
                StaticQuotaProvider::from_toml_str(QUOTAS).expect("quotas"),
            ))
        } else {
            Gateway::new(gateway_config(vec![url.clone()]))
        };
        Self {
            app,
            state,
            url,
            script,
            cold_store,
            clock,
            wal_disk,
            raft_log,
            gateway,
            authenticated: access,
            server,
        }
    }

    fn fail_next_write(&self, error: GroupEngineError) {
        self.script
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write_error = Some(error);
    }

    fn route(&self, route: Option<WriteRoute>) {
        self.script
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .route = route;
    }

    async fn direct(&self, request: Request<Body>) -> Answer {
        Answer::read(self.app.clone().oneshot(request).await.expect("answer")).await
    }

    async fn send(&self, via: Via, mut request: Request<Body>) -> Answer {
        match via {
            Via::Node => self.direct(request).await,
            Via::Gateway => {
                if self.authenticated && !request.headers().contains_key("authorization") {
                    request
                        .headers_mut()
                        .insert("authorization", BEARER.parse().expect("bearer"));
                }
                Answer::read(self.gateway.handle(request).await).await
            }
        }
    }

    /// Creates `path` with `body`, and returns its incarnation.
    async fn create(&self, path: &str, content_type: &str, body: &[u8]) -> String {
        let answer = self
            .direct(request(
                "PUT",
                path,
                &[("content-type", content_type)],
                body,
            ))
            .await;
        assert_eq!(answer.status, 201, "create {path}: {answer:?}");
        answer
            .header(names::STREAM_INCARNATION)
            .expect("an incarnation")
            .to_owned()
    }

    async fn stage(&self, path: &str, body: &[u8]) -> ursula_headers::PartId {
        let answer = self
            .direct(request(
                "POST",
                &format!("{path}/snapshot/3/parts"),
                &[],
                body,
            ))
            .await;
        assert_eq!(answer.status, 201, "{answer:?}");
        PartStaged::parse(answer.pairs()).expect("a part").part
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Via {
    Node,
    Gateway,
}

fn request(method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .header("content-length", body.len())
        .body(Body::from(body.to_vec()))
        .expect("request")
}

/// A producer write of `body` with key `(p, epoch, seq)`.
fn produce(path: &str, epoch: u64, seq: u64, body: &[u8], extra: &[(&str, &str)]) -> Request<Body> {
    let (epoch, seq) = (epoch.to_string(), seq.to_string());
    let mut headers = vec![
        ("content-type", TEXT),
        (names::PRODUCER_ID, "p"),
        (names::PRODUCER_EPOCH, epoch.as_str()),
        (names::PRODUCER_SEQ, seq.as_str()),
    ];
    headers.extend_from_slice(extra);
    request("POST", path, &headers, body)
}

fn hex_digest(body: &[u8]) -> String {
    blake3::hash(body).to_hex().to_string()
}

struct Fresh(AtomicU64);

impl Fresh {
    fn path(&self) -> String {
        format!("/wout/s{}", self.0.fetch_add(1, Ordering::Relaxed))
    }
}

/// The nodes and gateways of the test.
struct World {
    /// A node with a cold store, behind a gateway with access control.
    main: Node,
    /// A node without a cold store and with a small body budget, behind a
    /// gateway without access control.
    small: Node,
    uploads: UploadGateway,
    fresh: Fresh,
}

/// Drives row `id` once, through `via`. Gateway and embedder rows go
/// through the gateway only.
async fn drive(world: &World, id: &str, via: Via) -> Answer {
    let node = &world.main;
    let path = world.fresh.path();
    let path = path.as_str();
    match id {
        "append-applied" => {
            node.create(path, TEXT, b"").await;
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "append-repeat" => {
            node.create(path, TEXT, b"").await;
            node.direct(produce(path, 0, 0, b"a", &[])).await;
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "append-receipt-evicted" => {
            node.create(path, TEXT, b"").await;
            node.direct(produce(path, 0, 0, b"a", &[])).await;
            node.direct(produce(path, 0, 1, b"b", &[])).await;
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "append-keyless" => {
            node.create(path, TEXT, b"").await;
            node.send(via, request("POST", path, &[("content-type", TEXT)], b"a"))
                .await
        }
        "close-keyless" => {
            node.create(path, TEXT, b"").await;
            node.send(
                via,
                request("POST", path, &[(names::STREAM_CLOSED, "true")], b""),
            )
            .await
        }
        "create-new" => {
            node.send(via, request("PUT", path, &[("content-type", TEXT)], b""))
                .await
        }
        "create-matching" => {
            node.create(path, TEXT, b"").await;
            node.send(via, request("PUT", path, &[("content-type", TEXT)], b""))
                .await
        }
        "snapshot-published" => {
            node.create(path, OCTET, b"abc").await;
            let target = format!("{path}/snapshot/3");
            node.send(
                via,
                request("PUT", &target, &[("content-type", OCTET)], b"snap"),
            )
            .await
        }
        "part-staged" => {
            node.create(path, OCTET, b"abc").await;
            let target = format!("{path}/snapshot/3/parts");
            node.send(via, request("POST", &target, &[], b"part")).await
        }
        "parts-discarded" => {
            node.create(path, OCTET, b"abc").await;
            node.stage(path, b"part").await;
            node.send(
                via,
                request("DELETE", &format!("{path}/snapshot/parts"), &[], b""),
            )
            .await
        }
        "retention-advanced" => {
            node.create(path, OCTET, b"abcdef").await;
            let snapshot = format!("{path}/snapshot/3");
            let answer = node
                .direct(request("PUT", &snapshot, &[("content-type", OCTET)], b"x"))
                .await;
            assert_eq!(answer.status, 204, "{answer:?}");
            node.send(
                via,
                request("PUT", &format!("{path}/retention/3"), &[], b""),
            )
            .await
        }
        "stream-deleted" => {
            node.create(path, TEXT, b"").await;
            node.send(via, request("DELETE", path, &[], b"")).await
        }
        "bucket-created" => node.send(via, request("PUT", "/wout", &[], b"")).await,
        "staged-parts-limit" => {
            node.create(path, OCTET, b"abc").await;
            for _ in 0..2 {
                node.stage(path, b"part").await;
            }
            let target = format!("{path}/snapshot/3/parts");
            node.send(via, request("POST", &target, &[], b"part")).await
        }
        "parts-commit-published" => {
            node.create(path, OCTET, b"abc").await;
            let part = node.stage(path, b"part").await;
            node.send(via, commit_request(path, &[part], digest_of(b"part")))
                .await
        }
        // The gateway follows the redirect back to the same node, which
        // redirects again, so through it the row ends as `gateway-no-leader`.
        "redirect" => {
            let token = node.create(path, TEXT, b"").await;
            node.route(Some(WriteRoute::Follower {
                leader_id: 2,
                term: 7,
            }));
            let answer = node
                .send(
                    via,
                    produce(path, 0, 0, &[b'a'; ROUTED_BODY_BYTES], &[(
                        names::STREAM_INCARNATION,
                        token.as_str(),
                    )]),
                )
                .await;
            node.route(None);
            answer
        }
        "producer-epoch-closed" => {
            node.create(path, TEXT, b"").await;
            node.direct(produce(path, 0, 0, b"a", &[])).await;
            node.direct(produce(path, 1, 0, b"b", &[])).await;
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "producer-stale-epoch" => {
            node.create(path, TEXT, b"").await;
            for epoch in 0..3 {
                node.direct(produce(path, epoch, 0, b"a", &[])).await;
            }
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "producer-key-conflict" => {
            node.create(path, TEXT, b"").await;
            node.direct(produce(path, 0, 0, b"a", &[])).await;
            let digest = hex_digest(b"b");
            node.send(
                via,
                produce(path, 0, 0, b"b", &[(
                    names::PRODUCER_DIGEST,
                    digest.as_str(),
                )]),
            )
            .await
        }
        "producer-expired" => {
            let token = node.create(path, TEXT, b"").await;
            let tokened = [(names::STREAM_INCARNATION, token.as_str())];
            node.direct(produce(path, 0, 0, b"a", &tokened)).await;
            node.clock
                .fetch_add(PRODUCER_IDLE_EXPIRY_MS + 1, Ordering::Relaxed);
            node.send(via, produce(path, 0, 1, b"b", &tokened)).await
        }
        "producer-seq-gap" => {
            node.create(path, TEXT, b"").await;
            node.direct(produce(path, 0, 0, b"a", &[])).await;
            node.send(via, produce(path, 0, 5, b"b", &[])).await
        }
        "producer-epoch-not-claimed" => {
            node.create(path, TEXT, b"").await;
            node.direct(produce(path, 0, 0, b"a", &[])).await;
            node.send(via, produce(path, 1, 1, b"b", &[])).await
        }
        "bad-request" => {
            node.create(path, TEXT, b"").await;
            node.send(via, request("POST", path, &[("content-type", TEXT)], b""))
                .await
        }
        "not-found" => node.send(via, produce(path, 0, 0, b"a", &[])).await,
        "stream-closed" => {
            node.create(path, TEXT, b"").await;
            node.direct(request(
                "POST",
                path,
                &[(names::STREAM_CLOSED, "true")],
                b"",
            ))
            .await;
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "stream-seq-conflict" => {
            node.create(path, TEXT, b"").await;
            node.direct(request(
                "POST",
                path,
                &[("content-type", TEXT), ("stream-seq", "5")],
                b"a",
            ))
            .await;
            node.send(
                via,
                request(
                    "POST",
                    path,
                    &[("content-type", TEXT), ("stream-seq", "3")],
                    b"b",
                ),
            )
            .await
        }
        "snapshot-conflict" => {
            node.create(path, OCTET, b"abc").await;
            let target = format!("{path}/snapshot/3");
            node.direct(request("PUT", &target, &[("content-type", OCTET)], b"x"))
                .await;
            node.send(
                via,
                request("PUT", &target, &[("content-type", OCTET)], b"y"),
            )
            .await
        }
        // A publish below the retained offset.
        "gone" => {
            node.create(path, OCTET, b"abcdef").await;
            let snapshot = format!("{path}/snapshot/3");
            let answer = node
                .direct(request("PUT", &snapshot, &[("content-type", OCTET)], b"x"))
                .await;
            assert_eq!(answer.status, 204, "{answer:?}");
            let answer = node
                .direct(request("PUT", &format!("{path}/retention/3"), &[], b""))
                .await;
            assert_eq!(answer.status, 204, "{answer:?}");
            let target = format!("{path}/snapshot/1");
            node.send(
                via,
                request("PUT", &target, &[("content-type", OCTET)], b"y"),
            )
            .await
        }
        "conflict" => {
            node.create(path, TEXT, b"").await;
            node.send(via, request("POST", path, &[("content-type", OCTET)], b"a"))
                .await
        }
        "incarnation-mismatch" => {
            let token = node.create(path, TEXT, b"").await;
            let other = if token == "4503599627370497" {
                "4503599627370498"
            } else {
                "4503599627370497"
            };
            node.send(
                via,
                produce(path, 0, 0, b"a", &[(names::STREAM_INCARNATION, other)]),
            )
            .await
        }
        "too-large" => {
            node.create(path, TEXT, b"").await;
            let body = vec![b'a'; crate::MAX_HTTP_BODY_BYTES + 1];
            node.send(via, request("POST", path, &[("content-type", TEXT)], &body))
                .await
        }
        "producer-limit" => {
            node.create(path, TEXT, b"").await;
            for index in 0..4_096 {
                let id = format!("p{index}");
                let answer = node
                    .direct(request(
                        "POST",
                        path,
                        &[
                            ("content-type", TEXT),
                            (names::PRODUCER_ID, id.as_str()),
                            (names::PRODUCER_EPOCH, "0"),
                            (names::PRODUCER_SEQ, "0"),
                        ],
                        b"x",
                    ))
                    .await;
                assert_eq!(answer.status, 200, "producer {id}");
            }
            node.send(via, produce(path, 0, 0, b"x", &[])).await
        }
        "no-cold-store" => {
            let small = &world.small;
            small.create(path, OCTET, b"abc").await;
            let target = format!("{path}/snapshot/3/parts");
            small
                .send(via, request("POST", &target, &[], b"part"))
                .await
        }
        "no-leader-before-body" => {
            let token = node.create(path, TEXT, b"").await;
            node.route(Some(WriteRoute::NoLeader));
            let answer = node
                .send(
                    via,
                    produce(path, 0, 0, &[b'a'; ROUTED_BODY_BYTES], &[(
                        names::STREAM_INCARNATION,
                        token.as_str(),
                    )]),
                )
                .await;
            node.route(None);
            answer
        }
        "no-leader-before-proposal" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::NotLeader(LeadershipRefusal {
                leader: None,
                proposal: Proposal::NotProposed,
                reason: LeadershipReason::Follower,
            }));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "leadership-lost-after-proposal" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::NotLeader(LeadershipRefusal {
                leader: None,
                proposal: Proposal::MayHaveCommitted,
                reason: LeadershipReason::SteppedDown,
            }));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "outcome-unknown" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::Infra(GroupInfraError::OutcomeUnknown));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "wal-disk-pressure" => {
            node.create(path, TEXT, b"").await;
            node.wal_disk.observe_available(99);
            let answer = node.send(via, produce(path, 0, 0, b"a", &[])).await;
            node.wal_disk.observe_available(300);
            answer
        }
        "raft-log-pressure" => {
            node.create(path, TEXT, b"").await;
            assert_eq!(node.raft_log.observe_log_bytes(300, 200, 100), Some(true));
            let answer = node.send(via, produce(path, 0, 0, b"a", &[])).await;
            assert_eq!(node.raft_log.observe_log_bytes(0, 200, 100), Some(false));
            answer
        }
        "ingress-body-budget" => {
            let small = &world.small;
            small.create(path, TEXT, b"").await;
            let body = vec![
                b'a';
                usize::try_from(SMALL_BUDGET_BYTES)
                    .expect("fits")
                    .saturating_add(1)
            ];
            small.send(via, produce(path, 0, 0, &body, &[])).await
        }
        "cold-backpressure" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::cold_backpressure(
                BucketStreamId::new("wout", "s"),
                10,
                20,
                15,
            ));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "raft-uncommitted-backpressure" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::raft_uncommitted_backpressure(10, 20, 15));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "history-pending" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::Infra(GroupInfraError::RecoveryPending {
                group: ursula_shard::RaftTarget::Data(ursula_shard::RaftGroupId(0)),
            }));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "apply-stopped" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::Infra(GroupInfraError::ApplyStopped {
                group: ursula_shard::RaftTarget::Data(ursula_shard::RaftGroupId(0)),
                term: 1,
                index: 2,
                kind: ursula_runtime::ApplyStopKind::Panic,
            }));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "group-restarting" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::Infra(
                GroupInfraError::PreviousEngineWorkRunning {
                    group: ursula_shard::RaftTarget::Data(ursula_shard::RaftGroupId(0)),
                },
            ));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "json-boundary-unverified" => {
            node.create(path, OCTET, b"abc").await;
            node.fail_next_write(GroupEngineError::stream(
                StreamErrorCode::JsonBoundaryUnverified,
                "the byte before the offset is not hot",
            ));
            let target = format!("{path}/snapshot/3");
            node.send(
                via,
                request("PUT", &target, &[("content-type", OCTET)], b"snap"),
            )
            .await
        }
        "snapshot-commits-busy" => {
            let cold_store = node.cold_store.as_ref().expect("a cold store");
            let mut commits = Vec::new();
            for _ in 0..3 {
                let stream = world.fresh.path();
                node.create(&stream, OCTET, b"abc").await;
                let part = node.stage(&stream, b"part").await;
                commits.push(commit_request(&stream, &[part], digest_of(b"part")));
            }
            let gate = Gate::on_part_reads(cold_store);
            let mut pending = Vec::new();
            let mut commits = commits.into_iter();
            for commit in commits.by_ref().take(2) {
                pending.push(tokio::spawn(node.app.clone().oneshot(commit)));
            }
            gate.reached(2).await;
            let answer = node.send(via, commits.next().expect("a third")).await;
            gate.open();
            for commit in pending {
                assert_eq!(commit.await.expect("task").expect("answer").status(), 204);
            }
            answer
        }
        "snapshot-commit-in-progress" => {
            let cold_store = node.cold_store.as_ref().expect("a cold store");
            node.create(path, OCTET, b"abc").await;
            let part = node.stage(path, b"part").await;
            let gate = Gate::on_part_reads(cold_store);
            let first = tokio::spawn(node.app.clone().oneshot(commit_request(
                path,
                std::slice::from_ref(&part),
                digest_of(b"part"),
            )));
            gate.reached(1).await;
            let answer = node
                .send(via, commit_request(path, &[part], digest_of(b"part")))
                .await;
            gate.open();
            assert_eq!(first.await.expect("task").expect("answer").status(), 204);
            answer
        }
        "internal" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::Infra(
                GroupInfraError::UnexpectedResponse {
                    expected: ResponseKind::Append,
                    actual: ResponseKind::Missing,
                },
            ));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        "cold-store-failed" => {
            node.create(path, TEXT, b"").await;
            node.fail_next_write(GroupEngineError::Infra(GroupInfraError::ColdStore {
                operation: ColdOperation::RepairIndex,
            }));
            node.send(via, produce(path, 0, 0, b"a", &[])).await
        }
        _ => gateway_row(world, id, path).await,
    }
}

/// The rows the gateway or an embedder answers itself.
async fn gateway_row(world: &World, id: &str, path: &str) -> Answer {
    let node = &world.main;
    let with_bearer = |token: &str, path: &str| {
        let mut request = produce(path, 0, 0, b"a", &[]);
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("bearer"),
        );
        request
    };
    match id {
        "gateway-invalid-credential" => node.send(Via::Gateway, with_bearer("invalid", path)).await,
        "gateway-authentication-unavailable" => {
            node.send(Via::Gateway, with_bearer("auth-down", path))
                .await
        }
        "gateway-forbidden" => {
            node.send(Via::Gateway, produce("/wout/deny", 0, 0, b"a", &[]))
                .await
        }
        "gateway-concealed" => {
            node.send(Via::Gateway, produce("/wout/conceal", 0, 0, b"a", &[]))
                .await
        }
        "gateway-authorization-unavailable" => {
            node.send(Via::Gateway, produce("/wout/authz-down", 0, 0, b"a", &[]))
                .await
        }
        "gateway-too-large" => {
            node.send(Via::Gateway, produce("/small/s", 0, 0, &[b'a'; 2048], &[]))
                .await
        }
        "gateway-rate-limited" => {
            let first = node
                .send(Via::Gateway, produce("/limited/s", 0, 0, b"a", &[]))
                .await;
            assert_ne!(first.status, 429, "{first:?}");
            node.send(Via::Gateway, produce("/limited/s", 0, 1, b"b", &[]))
                .await
        }
        "gateway-uploads-exhausted" => {
            world
                .uploads
                .second_of_two(&path.replace("/wout/", "/upcount/"), 2048)
                .await
        }
        "gateway-upload-bytes-exhausted" => {
            world
                .uploads
                .second_of_two(&path.replace("/wout/", "/upbytes/"), 2048)
                .await
        }
        "gateway-upload-budget-exhausted" => world.uploads.second_of_two(path, 4096).await,
        "gateway-body-too-slow" => {
            UploadGateway::answer(world.uploads.start(path, 2048, 100).await).await
        }
        "gateway-no-upstream" => {
            let gateway = Gateway::new(gateway_config(Vec::new()));
            Answer::read(gateway.handle(produce(path, 0, 0, b"a", &[])).await).await
        }
        "gateway-upstream-unreachable" => {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let url = format!("http://{}", listener.local_addr().expect("address"));
            drop(listener);
            let gateway = Gateway::new(gateway_config(vec![url]));
            Answer::read(gateway.handle(produce(path, 0, 0, b"a", &[])).await).await
        }
        "gateway-no-leader" => Box::pin(drive(world, "redirect", Via::Gateway)).await,
        "gateway-upstream-failed" | "gateway-upstream-timeout" => {
            // An upstream that reads the request, then closes the connection
            // or never answers.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let url = format!("http://{}", listener.local_addr().expect("address"));
            let close = id == "gateway-upstream-failed";
            let upstream = tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((mut socket, _)) = listener.accept().await {
                    let mut buffer = [0_u8; 4096];
                    let _read = tokio::io::AsyncReadExt::read(&mut socket, &mut buffer).await;
                    if !close {
                        held.push(socket);
                    }
                }
            });
            let gateway = Gateway::new(gateway_config(vec![url]));
            let answer = Answer::read(gateway.handle(produce(path, 0, 0, b"a", &[])).await).await;
            upstream.abort();
            answer
        }
        "embedder-payment-required" => embedder(node, path, EmbedderRefusal::PaymentRequired).await,
        "embedder-account-revoked" => embedder(node, path, EmbedderRefusal::AccountRevoked).await,
        "embedder-forbidden" => embedder(node, path, EmbedderRefusal::Forbidden).await,
        "embedder-rate-limited" => {
            embedder(node, path, EmbedderRefusal::RateLimited {
                retry_after_secs: 3,
            })
            .await
        }
        other => panic!("no scenario drives row {other}"),
    }
}

async fn embedder(node: &Node, path: &str, refusal: EmbedderRefusal) -> Answer {
    Answer::read(
        node.gateway
            .refuse(produce(path, 0, 0, b"a", &[]), refusal)
            .await,
    )
    .await
}

/// The row a node row becomes through the gateway.
fn through_gateway(id: &str) -> &str {
    match id {
        "redirect" => "gateway-no-leader",
        // The gateway's limit is the node's, so the gateway refuses first.
        "too-large" => "gateway-too-large",
        other => other,
    }
}

#[track_caller]
fn assert_row(row: &Row, answer: &Answer, via: Via) {
    let context = format!("{} via {via:?}: {answer:?}", row.id);
    assert_eq!(answer.status, row.status, "{context}");
    assert_eq!(answer.code(), row.code, "{context}");
    assert_eq!(answer.outcome(row.write.kind()), row.outcome, "{context}");
    // The live answer carries every typed header its sample claims, so the
    // samples the clients' cross-checks read are the answers they get, and
    // a producer answer is one exactly when the sample is.
    let sample = row
        .sample()
        .expect("a sample")
        .expect("a row with an answer");
    let claimed: BTreeSet<&'static str> = TYPED_HEADERS
        .iter()
        .copied()
        .filter(|name| sample.headers.iter().any(|(header, _)| header == name))
        .collect();
    let live = answer.typed_names();
    let missing: Vec<_> = claimed.difference(&live).collect();
    assert!(missing.is_empty(), "{context}: missing {missing:?}");
    for name in [names::PRODUCER_EPOCH, names::PRODUCER_OUTCOME] {
        assert_eq!(
            claimed.contains(name),
            live.contains(name),
            "{context}: {name}"
        );
    }
    if matches!(row.source, Source::Gateway | Source::Embedder) {
        // The gateway's own answer: no producer header, no server identity,
        // and never cached.
        for name in [
            names::PRODUCER_EPOCH,
            names::PRODUCER_OUTCOME,
            names::URSULA_PROTOCOL,
            names::URSULA_CLUSTER_ID,
        ] {
            assert_eq!(answer.header(name), None, "{context}");
        }
        assert_eq!(
            answer.header("cache-control"),
            Some("no-store"),
            "{context}"
        );
    }
}

/// Every row a node, the gateway or an embedder sends, driven for real and
/// read by `classify` as the inventory says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_inventory_row_classifies_as_driven() {
    let main = Node::spawn(true, None, true).await;
    let uploads = UploadGateway::spawn(&main.url).await;
    let world = World {
        main,
        small: Node::spawn(false, Some(SMALL_BUDGET_BYTES), false).await,
        uploads,
        fresh: Fresh(AtomicU64::new(0)),
    };
    let mut driven = BTreeSet::new();
    for row in INVENTORY {
        match row.source {
            Source::Foreign => continue,
            Source::Node => {
                let answer = drive(&world, row.id, Via::Node).await;
                assert_row(row, &answer, Via::Node);
                let through = self::row(through_gateway(row.id)).expect("a row");
                let answer = drive(&world, row.id, Via::Gateway).await;
                assert_row(through, &answer, Via::Gateway);
            }
            Source::Gateway | Source::Embedder => {
                let answer = drive(&world, row.id, Via::Gateway).await;
                assert_row(row, &answer, Via::Gateway);
            }
        }
        driven.insert(row.id);
    }
    let undriven: Vec<&str> = INVENTORY
        .iter()
        .filter(|row| row.source != Source::Foreign && !driven.contains(row.id))
        .map(|row| row.id)
        .collect();
    assert!(undriven.is_empty(), "{undriven:?}");
}

/// A parts commit checks its key twice: before its body, where a leadership
/// refusal redirects, and after its body, when a part went away during the
/// copy, where it answers `503 NO_LEADER` and never a redirect (extensions
/// Section 5.7).
#[tokio::test]
async fn a_parts_commit_never_redirects_after_its_body() {
    let node = Node::spawn(true, None, false).await;
    let path = "/wout/commit-key";
    node.create(path, OCTET, b"abc").await;
    let part = node.stage(path, b"part").await;
    let commit = PartsCommit::for_manifest(
        &PartsManifest::new(vec![part]).expect("a manifest"),
        digest_of(b"part"),
    );
    let target = PublishTarget {
        state: node.state.clone(),
        request_target: format!("{path}/snapshot/3"),
        stream_id: BucketStreamId::new("wout", "commit-key"),
        snapshot_offset: 3,
        content_type: OCTET.to_owned(),
        if_incarnation: None,
    };
    for (body, status, code) in [
        (BodyState::Unread, 307, None),
        (
            BodyState::Read,
            503,
            Some(ursula_headers::ErrorCode::NoLeader),
        ),
    ] {
        node.script
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .head_error = Some(GroupEngineError::NotLeader(LeadershipRefusal {
            leader: Some(KnownLeader {
                node_id: 2,
                term: 7,
            }),
            proposal: Proposal::NotProposed,
            reason: LeadershipReason::Follower,
        }));
        let KeyCheck::Answered(response) = key_check(&target, &commit, body).await else {
            panic!("{body:?}: the key check answers a leadership refusal");
        };
        let answer = Answer::read(response).await;
        assert_eq!(answer.status, status, "{body:?}: {answer:?}");
        assert_eq!(answer.code(), code, "{body:?}: {answer:?}");
    }
}

/// A read proposes nothing, so its leadership refusal redirects or answers
/// `NO_LEADER` whatever the forward says, and never `OUTCOME_UNKNOWN`. A
/// write's forward that may follow a proposal answers `OUTCOME_UNKNOWN` and
/// never redirects.
#[tokio::test]
async fn only_a_write_answers_outcome_unknown_for_a_forward() {
    let node = Node::spawn(false, None, false).await;
    let forward = |leader: Option<u64>| ursula_runtime::RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: ursula_shard::RaftGroupId(0),
        error: GroupEngineError::NotLeader(LeadershipRefusal {
            leader: leader.map(|node_id| KnownLeader { node_id, term: 7 }),
            proposal: Proposal::MayHaveCommitted,
            reason: LeadershipReason::SteppedDown,
        }),
    };
    let read = |leader| {
        crate::read_error_or_leader_redirect_async(&node.state, forward(leader), "/wout/s")
    };
    let write = |leader| {
        crate::write_error_or_leader_redirect_async(&node.state, forward(leader), "/wout/s")
    };
    assert_eq!(read(Some(2)).await.status(), 307);
    let answer = Answer::read(read(None).await).await;
    assert_eq!(
        (answer.status, answer.code()),
        (503, Some(ursula_headers::ErrorCode::NoLeader))
    );
    for leader in [Some(2), None] {
        let answer = Answer::read(write(leader).await).await;
        assert_eq!(
            (answer.status, answer.code()),
            (503, Some(ursula_headers::ErrorCode::OutcomeUnknown)),
            "{leader:?}"
        );
    }
}
