//! Ursula Gateway core.
//!
//! Routes client HTTP requests to upstream Ursula nodes. Redirect responses stay
//! visible to clients, but upstream node addresses are stripped from Location so
//! redirect-following clients keep talking to the gateway.
//!
//! Raft leadership redirects (marked by `x-ursula-raft-leader-id`) are followed
//! internally by the gateway after buffering the request body. This avoids
//! leaking internal node addresses to clients and prevents SSE live reads from
//! looping when the initial random upstream lands on a follower.
//!
//! Module map:
//!
//! - [`auth`]: opt-in, provider-neutral authentication and authorization hooks.
//! - [`admission`]: opt-in per-tenant rate, live-read, and body-size limits.
//! - [`usage`]: opt-in per-tenant request/byte accounting and batch export.
//! - [`service`]: command arguments and the long-running gateway service entrypoint.

use std::collections::HashMap;
use std::error::Error as _;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::Response;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::header::AUTHORIZATION;
use axum::http::header::CONNECTION;
use axum::http::header::LOCATION;
use axum::http::header::RETRY_AFTER;
use axum::http::header::WWW_AUTHENTICATE;
use axum::response::IntoResponse;
use axum::response::Response as AxumResponse;
use percent_encoding::percent_decode_str;
use rand::prelude::IndexedRandom;
use tokio::time::Instant;
use tracing::debug;
use tracing::error;
use ursula_shard::BucketStreamId;
use ursula_shard::StaticShardMap;

pub mod admission;
pub mod auth;
pub mod cors;
pub mod service;
pub mod usage;

use std::sync::Arc;

use crate::admission::Admission;
use crate::admission::AdmissionRejection;
use crate::admission::LiveReadGuard;
use crate::admission::QuotaProvider;
use crate::admission::TenantLimits;
use crate::auth::AccessControl;
use crate::auth::Action;
use crate::auth::AuthenticationError;
use crate::auth::AuthorizationDecision;
use crate::auth::AuthorizationRequest;
use crate::auth::Resource;
use crate::auth::VerifiedPrincipal;
use crate::usage::PrincipalRef;
use crate::usage::UsageClass;
use crate::usage::UsageCollector;
use crate::usage::UsageKey;

const HEADER_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";
const HEADER_STREAM_CLOSED: &str = "stream-closed";
const MAX_LONG_POLL_TIMEOUT_MS: u64 = 60_000;
const LONG_POLL_RESPONSE_HEADER_GRACE: Duration = Duration::from_secs(2);
const GATEWAY_METRICS_PATH: &str = "/__ursula/gateway/metrics";
const LEADER_AFFINITY_MAX_STREAMS: usize = 16_384;
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Default)]
struct GatewayMetrics {
    requests: AtomicU64,
    leader_cache_hits: AtomicU64,
    leader_cache_misses: AtomicU64,
    leader_cache_updates: AtomicU64,
    leader_cache_evictions: AtomicU64,
    leader_redirects: AtomicU64,
    leader_redirect_ns: AtomicU64,
}

#[derive(Debug, serde::Serialize)]
struct GatewayMetricsSnapshot {
    requests: u64,
    leader_cache_hits: u64,
    leader_cache_misses: u64,
    leader_cache_updates: u64,
    leader_cache_evictions: u64,
    leader_redirects: u64,
    leader_redirect_ns: u64,
    leader_cache_entries: usize,
}

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub listen: SocketAddr,
    pub upstreams: Vec<String>,
    /// Covers only response headers so SSE bodies stay open.
    pub response_header_timeout: Duration,
    pub connect_timeout: Duration,
    /// Use cleartext HTTP/2 for gateway-to-Ursula traffic so concurrent
    /// requests share multiplexed upstream connections.
    pub upstream_http2_prior_knowledge: bool,
    pub max_request_body_bytes: usize,
    /// Raft topology used to share one learned leader across every stream in
    /// the same group. `None` preserves per-stream affinity for standalone
    /// gateways that do not know the server topology.
    pub raft_group_count: Option<usize>,
    /// Origins allowed to read across origins, or a single `*`. Empty disables
    /// CORS entirely, leaving responses byte-identical to a deployment without
    /// it. Anonymous `public_read` buckets are unreachable from browser
    /// JavaScript until this is set.
    pub cors_allowed_origins: Vec<String>,
}

#[derive(Clone)]
pub struct Gateway {
    config: GatewayConfig,
    client: reqwest::Client,
    response_header_timeout: Duration,
    access_control: Option<AccessControl>,
    quota_provider: Option<Arc<dyn QuotaProvider>>,
    admission: Arc<Admission>,
    usage: Option<Arc<UsageCollector>>,
    shard_map: Option<StaticShardMap>,
    leader_affinity: Arc<Mutex<HashMap<String, String>>>,
    metrics: Arc<GatewayMetrics>,
    cors: Option<crate::cors::CorsPolicy>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("config", &self.config)
            .field("response_header_timeout", &self.response_header_timeout)
            .field("access_control", &self.access_control)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Self {
        let shard_map = config
            .raft_group_count
            .and_then(|group_count| StaticShardMap::new(1, group_count).ok());
        let mut client_builder = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none());
        if config.upstream_http2_prior_knowledge {
            client_builder = client_builder.http2_prior_knowledge();
        }
        let client = client_builder
            .build()
            .expect("static gateway reqwest client config should be valid");
        let response_header_timeout = config.response_header_timeout;
        let cors = crate::cors::CorsPolicy::new(config.cors_allowed_origins.clone());
        Self {
            config,
            client,
            response_header_timeout,
            access_control: None,
            quota_provider: None,
            admission: Arc::new(Admission::default()),
            usage: None,
            shard_map,
            leader_affinity: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(GatewayMetrics::default()),
            cors,
        }
    }

    /// Installs provider-neutral authentication and authorization hooks.
    ///
    /// [`Gateway::new`] intentionally leaves access control disabled so
    /// self-hosted deployments retain the original pass-through behavior.
    pub fn with_access_control(config: GatewayConfig, access_control: AccessControl) -> Self {
        Self {
            access_control: Some(access_control),
            ..Self::new(config)
        }
    }

    /// Installs per-tenant admission limits. Requires access control: limits
    /// are keyed by the classified bucket resource.
    pub fn with_quota_provider(mut self, provider: Arc<dyn QuotaProvider>) -> Self {
        self.quota_provider = Some(provider);
        self
    }

    /// Installs per-tenant usage accounting. Requires access control for the
    /// same reason as quotas. Pair with [`usage::run_exporter`] to drain the
    /// collector into a [`usage::UsageSink`].
    pub fn with_usage_collector(mut self, collector: Arc<UsageCollector>) -> Self {
        self.usage = Some(collector);
        self
    }

    pub async fn handle(&self, req: Request<Body>) -> AxumResponse {
        let Some(policy) = self.cors.clone() else {
            return self.handle_inner(req).await;
        };
        let (parts, body) = req.into_parts();
        // Answered before `gate`, and identically for every path: a preflight
        // carries no `Authorization`, so a per-resource reply would tell an
        // unauthenticated caller whether a private bucket exists.
        if crate::cors::CorsPolicy::is_preflight(&parts.method, &parts.headers)
            && let Some(response) = policy.preflight_response(&parts.headers)
        {
            return response;
        }
        let request_headers = parts.headers.clone();
        let mut response = self.handle_inner(Request::from_parts(parts, body)).await;
        policy.decorate(response.headers_mut(), &request_headers);
        response
    }

    async fn handle_inner(&self, req: Request<Body>) -> AxumResponse {
        let (mut parts, body) = req.into_parts();
        if parts.method == Method::GET && parts.uri.path() == GATEWAY_METRICS_PATH {
            return axum::Json(self.metrics_snapshot()).into_response();
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);

        let context = match self.gate(&parts).await {
            Ok(context) => context,
            Err(response) => return response,
        };
        if self.access_control.is_some() {
            // The credential terminates at the gateway. Internal Ursula nodes
            // do not need the user's bearer token.
            parts.headers.remove(AUTHORIZATION);
        }

        let mut live_guard = None;
        if let Some(context) = &context {
            match self.admit(context) {
                Ok(guard) => live_guard = guard,
                Err(response) => return response,
            }
        }

        let body_bytes = match axum::body::to_bytes(body, self.config.max_request_body_bytes).await
        {
            Ok(b) => b,
            Err(err) => {
                if err
                    .source()
                    .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
                {
                    return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                } else {
                    return StatusCode::BAD_REQUEST.into_response();
                }
            }
        };
        if let Some(max) = context
            .as_ref()
            .and_then(|ctx| ctx.limits.max_request_body_bytes)
            && body_bytes.len() as u64 > max
        {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }

        let upstream = match self.pick_upstream(&parts.uri) {
            Some(u) => u,
            None => {
                error!("no upstreams configured");
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        };

        let tail = ResponseTail {
            meter: context.as_ref().and_then(|ctx| {
                self.usage.as_ref().map(|collector| EgressMeter {
                    collector: Arc::clone(collector),
                    key: UsageKey {
                        bucket_id: ctx.bucket_id.clone(),
                        principal: ctx.principal.clone(),
                        class: ctx.class,
                    },
                    request_bytes: if matches!(ctx.class, UsageClass::Append) {
                        body_bytes.len() as u64
                    } else {
                        0
                    },
                    response_bytes: 0,
                })
            }),
            _live_guard: live_guard,
        };

        match self.forward(&upstream, &parts, body_bytes, tail).await {
            Ok(response) => response,
            Err(e) => {
                error!(error = %e, "gateway request failed");
                e.into_response()
            }
        }
    }

    /// Classifies and authorizes the request. `Ok(None)` means access control
    /// is not installed and the gateway keeps its pass-through behavior.
    async fn gate(
        &self,
        parts: &axum::http::request::Parts,
    ) -> Result<Option<RequestContext>, AxumResponse> {
        let Some(access_control) = self.access_control.as_ref() else {
            return Ok(None);
        };
        let Some(request) = classify_request(&parts.method, &parts.uri, &parts.headers) else {
            // An access-controlled gateway is a public resource server, not a
            // transparent escape hatch to internal or newly added routes.
            return Err(StatusCode::NOT_FOUND.into_response());
        };

        let principal = match bearer_token(&parts.headers) {
            Ok(Some(token)) => match access_control.principal_resolver().resolve(token).await {
                Ok(principal) => Some(principal),
                Err(error) => return Err(authentication_error_response(error)),
            },
            Ok(None) => None,
            Err(()) => {
                return Err(authentication_error_response(
                    AuthenticationError::InvalidCredential,
                ));
            }
        };

        let bucket_id = request.resource.bucket_id.clone();
        let action = request.action;
        let decision = access_control
            .authorizer()
            .authorize(AuthorizationRequest {
                principal: principal.clone(),
                resource: request.resource,
                action,
            })
            .await;
        match decision {
            Ok(AuthorizationDecision::Allow) => {}
            Ok(AuthorizationDecision::Deny) => return Err(StatusCode::FORBIDDEN.into_response()),
            Ok(AuthorizationDecision::ConcealAsNotFound) => {
                return Err(StatusCode::NOT_FOUND.into_response());
            }
            Err(error) => {
                error!(error = %error, "gateway authorization failed");
                return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
            }
        }

        let limits = match &self.quota_provider {
            Some(provider) => provider.limits_for(&bucket_id).await,
            None => TenantLimits::default(),
        };
        Ok(Some(RequestContext {
            bucket_id,
            class: usage_class(action),
            principal: principal.as_ref().map(principal_ref),
            limits,
        }))
    }

    /// Enforces the tenant's admission limits. `Retry-After` accompanies every
    /// retryable rejection; Ursula's own `503` backpressure semantics are
    /// untouched.
    #[expect(
        clippy::result_large_err,
        reason = "the error is the terminal HTTP response, produced at most once per request"
    )]
    fn admit(&self, context: &RequestContext) -> Result<Option<LiveReadGuard>, AxumResponse> {
        if self.quota_provider.is_none() {
            return Ok(None);
        }
        if let Err(rejection) = self
            .admission
            .admit_request(&context.bucket_id, context.limits)
        {
            return Err(admission_rejection_response(rejection));
        }
        if context.class == UsageClass::LiveRead {
            return self
                .admission
                .admit_live_read(&context.bucket_id, context.limits)
                .map_err(admission_rejection_response);
        }
        Ok(None)
    }

    fn pick_upstream(&self, uri: &Uri) -> Option<String> {
        if let Some(key) = stream_affinity_key(uri, self.shard_map.as_ref())
            && let Some(upstream) = self
                .leader_affinity
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&key)
                .cloned()
        {
            self.metrics
                .leader_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Some(upstream);
        }
        self.metrics
            .leader_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let mut rng = rand::rng();
        self.config.upstreams.choose(&mut rng).cloned()
    }

    async fn forward(
        &self,
        upstream: &str,
        parts: &axum::http::request::Parts,
        body: bytes::Bytes,
        tail: ResponseTail,
    ) -> Result<Response<Body>, GatewayError> {
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let target_url = format!("{}{}", upstream.trim_end_matches('/'), path_and_query);

        let redirect_started_at = Instant::now();
        let upstream_resp = self.send_request(&target_url, parts, body.clone()).await?;

        // Raft leadership redirect: follow internally for all methods because
        // the body has been buffered and the client cannot do better than
        // another random hop through the gateway.
        if upstream_resp.status() == StatusCode::TEMPORARY_REDIRECT
            && upstream_resp
                .headers()
                .contains_key(HEADER_URSULA_RAFT_LEADER_ID)
        {
            let response_headers = copy_forwarded_headers(upstream_resp.headers(), false);
            if let Some(leader_upstream) = self.resolve_leader_upstream(&response_headers) {
                self.metrics
                    .leader_redirects
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(key) = stream_affinity_key(&parts.uri, self.shard_map.as_ref()) {
                    self.remember_leader(key, leader_upstream.to_owned());
                }
                // Drop the follower response; it has no meaningful body.
                drop(upstream_resp);

                let leader_target = format!(
                    "{}{}",
                    leader_upstream.trim_end_matches('/'),
                    path_and_query
                );
                let leader_resp = self.send_request(&leader_target, parts, body).await?;
                self.metrics.leader_redirect_ns.fetch_add(
                    u64::try_from(redirect_started_at.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                return Self::build_response(leader_resp, tail);
            }
        }

        // A cached leader can become a follower after an election. When that
        // follower does not know the new leader yet, Ursula returns the
        // explicit retryable 503 form instead of a 307. Do not replay the
        // current request here: a generic write may not be idempotent. Evict
        // only the matching cached route so the caller's next retry gets a
        // fresh random/redirect lookup.
        if upstream_resp.status() == StatusCode::SERVICE_UNAVAILABLE
            && upstream_resp.headers().contains_key(RETRY_AFTER)
            && let Some(key) = stream_affinity_key(&parts.uri, self.shard_map.as_ref())
        {
            self.forget_leader_if_matches(&key, upstream);
        }

        Self::build_response(upstream_resp, tail)
    }

    /// Covers only response headers so SSE bodies stay open.
    async fn send_request(
        &self,
        url: &str,
        parts: &axum::http::request::Parts,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response, GatewayError> {
        debug!(method = %parts.method, url = %url, "sending upstream request");

        tokio::time::timeout(
            self.response_header_timeout_for_url(url),
            self.client
                .request(parts.method.clone(), url)
                .headers(copy_forwarded_headers(&parts.headers, true))
                .body(body)
                .send(),
        )
        .await
        .map_err(|e| GatewayError::Upstream(format!("upstream response header timeout: {e}")))?
        .map_err(|e| GatewayError::Upstream(e.to_string()))
    }

    fn response_header_timeout_for_url(&self, url: &str) -> Duration {
        let Ok(url) = reqwest::Url::parse(url) else {
            return self.response_header_timeout;
        };
        let mut is_long_poll = false;
        let mut requested_timeout_ms = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "live" if value == "long-poll" => is_long_poll = true,
                "timeout_ms" => requested_timeout_ms = value.parse::<u64>().ok(),
                _ => {}
            }
        }
        if !is_long_poll {
            return self.response_header_timeout;
        }
        let upstream_timeout = requested_timeout_ms
            .unwrap_or(1_000)
            .clamp(1, MAX_LONG_POLL_TIMEOUT_MS);
        self.response_header_timeout.max(
            Duration::from_millis(upstream_timeout).saturating_add(LONG_POLL_RESPONSE_HEADER_GRACE),
        )
    }

    fn build_response(
        upstream_resp: reqwest::Response,
        tail: ResponseTail,
    ) -> Result<Response<Body>, GatewayError> {
        let status = upstream_resp.status();
        let mut headers = copy_forwarded_headers(upstream_resp.headers(), false);

        if status == StatusCode::TEMPORARY_REDIRECT {
            // Keep redirects client-facing: strip internal host so the client
            // reconnects through the gateway, not directly to the Ursula node.
            headers.remove(HEADER_URSULA_RAFT_LEADER_ID);
            if let Some(location) = headers
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|location| location.parse::<Uri>().ok())
                .and_then(|uri| uri.path_and_query().map(|pq| pq.as_str().to_owned()))
                .and_then(|path| HeaderValue::from_str(&path).ok())
            {
                headers.insert(LOCATION, location);
            }
        }

        let mut response = Response::builder().status(status);
        if let Some(response_headers) = response.headers_mut() {
            *response_headers = headers;
        }
        response
            .body(Body::from_stream(CountedStream {
                inner: upstream_resp.bytes_stream(),
                tail,
            }))
            .map_err(|e| GatewayError::ResponseBuild(e.to_string()))
    }

    fn resolve_leader_upstream(&self, response_headers: &HeaderMap) -> Option<&str> {
        let location = response_headers.get(LOCATION)?.to_str().ok()?;
        self.config
            .upstreams
            .iter()
            .find(|u| location.starts_with(*u))
            .map(String::as_str)
    }

    fn remember_leader(&self, key: String, upstream: String) {
        let mut affinity = self
            .leader_affinity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if affinity.len() >= LEADER_AFFINITY_MAX_STREAMS && !affinity.contains_key(&key) {
            affinity.clear();
        }
        affinity.insert(key, upstream);
        self.metrics
            .leader_cache_updates
            .fetch_add(1, Ordering::Relaxed);
    }

    fn forget_leader_if_matches(&self, key: &str, upstream: &str) {
        let mut affinity = self
            .leader_affinity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if affinity.get(key).is_some_and(|cached| cached == upstream) {
            affinity.remove(key);
            self.metrics
                .leader_cache_evictions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        let leader_cache_entries = self
            .leader_affinity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        GatewayMetricsSnapshot {
            requests: self.metrics.requests.load(Ordering::Relaxed),
            leader_cache_hits: self.metrics.leader_cache_hits.load(Ordering::Relaxed),
            leader_cache_misses: self.metrics.leader_cache_misses.load(Ordering::Relaxed),
            leader_cache_updates: self.metrics.leader_cache_updates.load(Ordering::Relaxed),
            leader_cache_evictions: self.metrics.leader_cache_evictions.load(Ordering::Relaxed),
            leader_redirects: self.metrics.leader_redirects.load(Ordering::Relaxed),
            leader_redirect_ns: self.metrics.leader_redirect_ns.load(Ordering::Relaxed),
            leader_cache_entries,
        }
    }
}

fn stream_affinity_key(uri: &Uri, shard_map: Option<&StaticShardMap>) -> Option<String> {
    let mut segments = uri.path().split('/').filter(|segment| !segment.is_empty());
    let bucket = percent_decode_str(segments.next()?).decode_utf8().ok()?;
    let stream = percent_decode_str(segments.next()?).decode_utf8().ok()?;
    if bucket.starts_with("__ursula") {
        return None;
    }
    if let Some(shard_map) = shard_map {
        let placement = shard_map.locate(&BucketStreamId::new(bucket, stream));
        return Some(format!("group:{}", placement.raft_group_id.0));
    }
    Some(format!("/{bucket}/{stream}"))
}

/// Per-request context available once access control has classified and
/// authorized the request.
struct RequestContext {
    bucket_id: String,
    class: UsageClass,
    principal: Option<PrincipalRef>,
    limits: TenantLimits,
}

fn usage_class(action: Action) -> UsageClass {
    match action {
        Action::Append | Action::AppendAndClose | Action::Create | Action::CreateAndClose => {
            UsageClass::Append
        }
        Action::Read | Action::Head | Action::ReadSnapshot => UsageClass::Read,
        Action::Tail => UsageClass::LiveRead,
        Action::Update
        | Action::Delete
        | Action::PublishSnapshot
        | Action::DeleteSnapshot
        | Action::AdministerBucket => UsageClass::Admin,
    }
}

fn principal_ref(principal: &VerifiedPrincipal) -> PrincipalRef {
    PrincipalRef {
        issuer: principal.issuer.clone(),
        subject: principal.subject.clone(),
    }
}

fn admission_rejection_response(rejection: AdmissionRejection) -> AxumResponse {
    match rejection {
        AdmissionRejection::RateLimited { retry_after_secs } => {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                "tenant request rate limit exceeded",
            )
                .into_response();
            if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
                response.headers_mut().insert(RETRY_AFTER, value);
            }
            response
        }
        AdmissionRejection::LiveReadsExhausted => (
            StatusCode::TOO_MANY_REQUESTS,
            "tenant live-read connection limit exceeded",
        )
            .into_response(),
    }
}

/// Rides the response body: counts egress bytes and, on drop, records the
/// request's usage and releases any live-read slot. Dropping happens whether
/// the body completes, the client disconnects, or the response is discarded.
struct ResponseTail {
    meter: Option<EgressMeter>,
    _live_guard: Option<LiveReadGuard>,
}

struct EgressMeter {
    collector: Arc<UsageCollector>,
    key: UsageKey,
    request_bytes: u64,
    response_bytes: u64,
}

impl Drop for EgressMeter {
    fn drop(&mut self) {
        self.collector
            .record(self.key.clone(), 1, self.request_bytes, self.response_bytes);
    }
}

struct CountedStream<S> {
    inner: S,
    tail: ResponseTail,
}

impl<S> futures_core::Stream for CountedStream<S>
where S: futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin
{
    type Item = Result<bytes::Bytes, reqwest::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = std::pin::Pin::new(&mut this.inner).poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(chunk))) = &polled
            && let Some(meter) = &mut this.tail.meter
        {
            meter.response_bytes = meter.response_bytes.saturating_add(chunk.len() as u64);
        }
        polled
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClassifiedRequest {
    resource: Resource,
    action: Action,
}

fn classify_request(method: &Method, uri: &Uri, headers: &HeaderMap) -> Option<ClassifiedRequest> {
    let segments = uri
        .path()
        .strip_prefix('/')?
        .split('/')
        .map(decode_path_segment)
        .collect::<Option<Vec<_>>>()?;
    let bucket_id = segments.first()?.clone();
    if bucket_id.is_empty() || bucket_id == "__ursula" {
        return None;
    }

    let (stream_id, action) = match segments.as_slice() {
        [_bucket] if *method == Method::PUT => (None, Action::AdministerBucket),
        [_bucket, stream] if *method == Method::PUT => {
            let action = if closes_stream(headers) {
                Action::CreateAndClose
            } else {
                Action::Create
            };
            (Some(stream.clone()), action)
        }
        [_bucket, stream] if *method == Method::POST => {
            let action = if closes_stream(headers) {
                Action::AppendAndClose
            } else {
                Action::Append
            };
            (Some(stream.clone()), action)
        }
        [_bucket, stream] if *method == Method::GET => {
            // Any `live` mode (sse, long-poll) holds a server-side waiter, so
            // both classify as Tail for authorization and live-read quotas.
            let action = if query_has_live_mode(uri) {
                Action::Tail
            } else {
                Action::Read
            };
            (Some(stream.clone()), action)
        }
        [_bucket, stream] if *method == Method::HEAD => (Some(stream.clone()), Action::Head),
        [_bucket, stream] if *method == Method::DELETE => (Some(stream.clone()), Action::Delete),
        [_bucket, stream, suffix] if suffix == "attrs" && *method == Method::PUT => {
            (Some(stream.clone()), Action::Update)
        }
        [_bucket, stream, suffix] if suffix == "attrs" && *method == Method::GET => {
            (Some(stream.clone()), Action::Head)
        }
        [_bucket, stream, suffix] if suffix == "bootstrap" && *method == Method::GET => {
            (Some(stream.clone()), Action::Read)
        }
        [_bucket, stream, suffix] if suffix == "append-batch" && *method == Method::POST => {
            (Some(stream.clone()), Action::Append)
        }
        [_bucket, stream, suffix] if suffix == "snapshot" && *method == Method::GET => {
            (Some(stream.clone()), Action::ReadSnapshot)
        }
        [_bucket, stream, suffix, _offset] if suffix == "snapshot" && *method == Method::PUT => {
            (Some(stream.clone()), Action::PublishSnapshot)
        }
        [_bucket, stream, suffix, _offset] if suffix == "snapshot" && *method == Method::GET => {
            (Some(stream.clone()), Action::ReadSnapshot)
        }
        [_bucket, stream, suffix, _offset] if suffix == "snapshot" && *method == Method::DELETE => {
            (Some(stream.clone()), Action::DeleteSnapshot)
        }
        _ => return None,
    };

    Some(ClassifiedRequest {
        resource: Resource {
            bucket_id,
            stream_id,
        },
        action,
    })
}

fn closes_stream(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn decode_path_segment(segment: &str) -> Option<String> {
    percent_decode_str(segment)
        .decode_utf8()
        .ok()
        .map(|decoded| decoded.into_owned())
}

fn query_has_live_mode(uri: &Uri) -> bool {
    uri.query().is_some_and(|query| {
        query.split('&').any(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            decode_path_segment(name).as_deref() == Some("live")
                && decode_path_segment(value).is_some_and(|value| !value.is_empty())
        })
    })
}

fn bearer_token(headers: &HeaderMap) -> Result<Option<&str>, ()> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = value.to_str().map_err(|_invalid_header| ())?;
    let (scheme, token) = value.split_once(' ').ok_or(())?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(());
    }
    Ok(Some(token))
}

fn authentication_error_response(error: AuthenticationError) -> AxumResponse {
    if error == AuthenticationError::Unavailable {
        error!(error = %error, "gateway authentication failed");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static(r#"Bearer error="invalid_token""#),
    );
    response
}

// Copy end-to-end headers while removing hop-by-hop proxy headers. Hop-by-hop
// headers describe only the current TCP/HTTP connection, so the gateway must
// let its HTTP client/server stack recreate them for the next connection.
//
// Request forwarding also drops Host so the client can set it from the upstream
// URL. Use append, not insert, so repeated headers such as Set-Cookie survive.
fn copy_forwarded_headers(src: &HeaderMap, drop_host: bool) -> HeaderMap {
    let mut dst = HeaderMap::new();
    let connection_tokens = connection_header_tokens(src);
    for (key, value) in src {
        let key_str = key.as_str();
        if (drop_host && key_str == "host") || is_hop_by_hop_header(key_str, &connection_tokens) {
            continue;
        }
        dst.append(key.clone(), value.clone());
    }
    dst
}

// RFC 9110 lets Connection nominate additional hop-by-hop fields:
//
//     Connection: x-local
//     X-Local: value
//
// In that case X-Local is also scoped to this one connection and must not be
// forwarded. Normalize tokens once so later comparisons are cheap and
// case-insensitive.
fn connection_header_tokens(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn is_hop_by_hop_header(key: &str, connection_tokens: &[String]) -> bool {
    // The fixed names below are standard hop-by-hop headers. The dynamic
    // Connection tokens cover extension headers named by the sender.
    matches!(
        key,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    ) || connection_tokens
        .iter()
        .any(|token| token.eq_ignore_ascii_case(key))
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("failed to build response: {0}")]
    ResponseBuild(String),
}

impl GatewayError {
    fn into_response(self) -> AxumResponse {
        match self {
            Self::Upstream(_) | Self::ResponseBuild(_) => StatusCode::BAD_GATEWAY.into_response(),
        }
    }
}

#[cfg(test)]
mod tests;
