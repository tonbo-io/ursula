//! Ursula Gateway core.
//!
//! Routes client HTTP requests to upstream Ursula nodes.
//!
//! Ursula's routing redirects (a `307` that names the leader in
//! `x-ursula-raft-leader-id`, or the group alone in `x-ursula-raft-group` from
//! a node that hosts no replica of it) are followed internally by the gateway
//! after buffering the request body, so a client
//! never gets one: up to three hops after the first, none starting later than
//! 5 s after the first redirect. A redirect to an address that is not one of
//! the upstreams, or to an upstream the request already went to, sends the
//! request to an upstream it has not tried yet instead. The host a
//! non-hosting node's redirect names is a hint: the request goes there only
//! while that upstream is not ejected, and when it cannot connect there the
//! search goes on, as it does from a hop to an upstream no redirect named. A
//! leader redirect's target that cannot connect ends the request. When no
//! upstream answers without redirecting within those limits, the client gets a
//! retryable `503`. This keeps internal node addresses from clients, and SSE
//! live reads from looping when the first upstream is a follower. Other
//! redirects stay visible to clients, with the upstream's address stripped
//! from `Location` so that redirect-following clients keep talking to the
//! gateway.
//!
//! A refusal says that its request applied nothing by its status and error
//! code (extensions Section 11.1, `ursula_headers::classify`). Every refusal
//! the gateway sends itself, and every one of an embedder
//! ([`Gateway::refuse`]), is one. A client resolves any other outcome by
//! repeating a keyed write (#581 section 2).
//!
//! A refusal the gateway decides from a request's head alone (access control,
//! tenant admission) goes out once it has read the request's body, up to
//! `max_request_body_bytes`, for 30 s at most and until no data arrived for
//! 5 s, so that a client that sends its whole body before it reads the answer
//! gets the refusal instead of a closed connection (#540).
//!
//! The bodies the gateway buffers for the requests it forwards are bounded
//! across requests ([`uploads`]): each reserves its size from a node-wide
//! budget before any byte of it is read, with headroom only small bodies may
//! take. With a quota policy, a bucket's large bodies being read are bounded
//! by count and bytes too ([`admission::TenantLimits`]). A request past a
//! bucket's upload limits is refused at once (`429`), and one that finds the
//! budget spent after a short wait (`503`). A body that stops arriving, or
//! falls below a floor of bytes over one window of the stall timeout, is cut
//! and refused (`408`) before anything of it is forwarded. Each refusal
//! carries its own code in the JSON error body. The first two, and the `413`
//! for a body over a bucket's own limit, read the body first, at the pace a
//! forwarded body may keep, so that the client gets them on an intact
//! connection.
//!
//! Module map:
//!
//! - [`auth`]: opt-in, provider-neutral authentication and authorization hooks.
//! - [`admission`]: opt-in per-tenant rate, live-read, body-size and upload
//!   limits.
//! - [`usage`]: opt-in per-tenant request/byte accounting and batch export.
//! - [`refusal`]: [`refusal::EmbedderRefusal`], a refusal of an embedder
//!   before forwarding, answered by [`Gateway::refuse`] as the gateway
//!   answers its own.
//! - [`service`]: command arguments and the long-running gateway service entrypoint.
//! - [`uploads`]: the per-request limit and the node-wide budget for buffered
//!   request bodies, and the pace a body must keep.
//! - `ejection`: leaves upstreams that failed at the transport out of selection
//!   for a backoff window, then lets one request at a time probe them (#454).

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::body::HttpBody as _;
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
use http_body_util::BodyExt as _;
use percent_encoding::percent_decode_str;
use tokio::time::Instant;
use tracing::debug;
use tracing::error;
use ursula_headers::ErrorBody;
use ursula_headers::ErrorCode;
use ursula_headers::RoutingAnswer;
use ursula_headers::SizeLimit;
use ursula_headers::immutable;
use ursula_headers::lowered_request_limit;
use ursula_headers::names;
use ursula_observability::serve::CLIENT_IDLE_BOUND;
use ursula_shard::BucketStreamId;
use ursula_shard::StaticShardMap;

pub mod admission;
pub mod auth;
pub mod cors;
mod ejection;
pub mod refusal;
pub mod service;
pub mod uploads;
pub mod usage;

use std::sync::Arc;

use crate::admission::Admission;
use crate::admission::AdmissionRejection;
use crate::admission::LiveReadGuard;
use crate::admission::QuotaProvider;
use crate::admission::TenantLimits;
use crate::admission::UploadGuard;
use crate::auth::AccessControl;
use crate::auth::Action;
use crate::auth::AuthenticationError;
use crate::auth::AuthorizationDecision;
use crate::auth::AuthorizationError;
use crate::auth::AuthorizationRequest;
use crate::auth::Resource;
use crate::auth::VerifiedPrincipal;
use crate::ejection::Ejections;
use crate::ejection::Failed;
use crate::refusal::EmbedderRefusal;
use crate::uploads::BodyPermit;
use crate::uploads::BudgetExhausted;
use crate::uploads::UploadLimits;
use crate::uploads::Uploads;
use crate::usage::PrincipalRef;
use crate::usage::UsageClass;
use crate::usage::UsageCollector;
use crate::usage::UsageKey;

const HEADER_STREAM_CLOSED: &str = "stream-closed";
const MAX_LONG_POLL_TIMEOUT_MS: u64 = 60_000;
const LONG_POLL_RESPONSE_HEADER_GRACE: Duration = Duration::from_secs(2);
/// The gateway's own counters ([`GatewayMetricsSnapshot`]), which it answers
/// itself unless access control hides them (see [`Gateway::serves_metrics`]).
const GATEWAY_METRICS_PATH: &str = "/__ursula/gateway/metrics";
const LEADER_AFFINITY_MAX_STREAMS: usize = 16_384;
/// The most hops the gateway sends one request on after Raft leader
/// redirects, search hops included (see [`Gateway::forward`]). With three
/// voters, two reach every node.
const MAX_LEADER_REDIRECTS: usize = 3;
/// How long after a request's first redirect the gateway may still start a
/// hop: it chases a moving leader for this long at most, then answers `503`
/// ([`GatewayError::LeaderNotFound`]). It counts from the first redirect, not
/// the first hop, so a slow first hop, such as a follower that stages a large
/// body in object storage before its leadership check, does not use it up. A
/// hop under way runs to its answer: a long-poll or a large upload that
/// reached the leader is not cut short.
const LEADER_REDIRECT_DEADLINE: Duration = Duration::from_secs(5);
/// How long the gateway waits for the next chunk of the body of a request it
/// refused from the head alone before it sends the refusal anyway
/// ([`Gateway::refuse_unread`]): a body that stops arriving holds the
/// connection this long after its last chunk.
const REFUSED_BODY_STALL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the gateway reads such a body at most, however steadily it
/// arrives: a client that sends its body slowly holds the connection this long
/// at most. 32 MiB, the default `max_request_body_bytes`, takes this long at
/// about 9 Mbit/s.
const REFUSED_BODY_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// The pace of the reading of a request's body that the gateway refused from
/// its head alone, before it knew the request may be forwarded.
const REFUSED_PACE: Pace = Pace::Bounded {
    stall_timeout: REFUSED_BODY_STALL_TIMEOUT,
    read_timeout: REFUSED_BODY_DRAIN_TIMEOUT,
};
/// The protocol's request limit ([`ursula_headers::MAX_REQUEST_BYTES`]),
/// which is also the most the gateway may be configured to accept.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = ursula_headers::MAX_REQUEST_BYTES as usize;

#[derive(Debug, Default)]
struct GatewayMetrics {
    requests: AtomicU64,
    leader_cache_hits: AtomicU64,
    leader_cache_misses: AtomicU64,
    leader_cache_updates: AtomicU64,
    leader_cache_evictions: AtomicU64,
    leader_redirects: AtomicU64,
    leader_redirect_ns: AtomicU64,
    leader_searches: AtomicU64,
    leader_not_found: AtomicU64,
    upstream_ejections: AtomicU64,
    refusal_drains: AtomicU64,
    refusal_drains_cut: AtomicU64,
    refusal_drain_bytes: AtomicU64,
    body_bytes_buffered: AtomicU64,
    upload_refusals_tenant_bytes: AtomicU64,
    upload_refusals_tenant_uploads: AtomicU64,
    upload_refusals_budget: AtomicU64,
    upload_refusals_too_slow: AtomicU64,
    embedder_refusals: AtomicU64,
}

#[derive(Debug, serde::Serialize)]
struct GatewayMetricsSnapshot {
    requests: u64,
    leader_cache_hits: u64,
    leader_cache_misses: u64,
    leader_cache_updates: u64,
    leader_cache_evictions: u64,
    /// Raft leader redirects followed, each hop counted.
    leader_redirects: u64,
    leader_redirect_ns: u64,
    /// Search hops: hops to an upstream the request had not tried, which no
    /// redirect named. They follow a redirect whose `Location` names no
    /// upstream or one the request already went to, and a search hop that
    /// could not connect.
    leader_searches: u64,
    /// Requests answered `503` because no upstream answered without
    /// redirecting within the redirect limits.
    leader_not_found: u64,
    leader_cache_entries: usize,
    /// Times an upstream was left out of selection after a transport failure.
    upstream_ejections: u64,
    /// Upstreams left out of selection now: inside their window, or with a
    /// probe outstanding.
    upstream_ejected: usize,
    /// Requests refused from their head alone that carried a body, which the
    /// gateway reads before it answers ([`Gateway::refuse_unread`]), counted
    /// as the reading starts.
    refusal_drains: u64,
    /// Those whose body the gateway did not read to its end: one over
    /// `max_request_body_bytes` (by its declared length, then not read at
    /// all, or as read), one that stopped arriving for
    /// [`REFUSED_BODY_STALL_TIMEOUT`] or was still arriving after
    /// [`REFUSED_BODY_DRAIN_TIMEOUT`], or one that failed. The connection then
    /// closes after the refusal, and a client still sending the body may see
    /// that instead of the refusal.
    refusal_drains_cut: u64,
    /// Bytes of the bodies of refused requests that the gateway read and
    /// dropped ([`Gateway::refuse_unread`]), counted as each chunk arrives.
    refusal_drain_bytes: u64,
    /// Bytes of the bodies of requests the gateway may forward that it read
    /// to buffer them ([`Gateway::buffer`]), counted as each chunk arrives,
    /// also of a body cut or refused part way. The rest of a body refused
    /// part way counts in `refusal_drain_bytes`.
    body_bytes_buffered: u64,
    /// Requests refused because their bucket's large bodies being read would
    /// have gone past [`TenantLimits::max_upload_bytes`] with theirs.
    upload_refusals_tenant_bytes: u64,
    /// Requests refused because their bucket had
    /// [`TenantLimits::max_concurrent_uploads`] large bodies being read.
    upload_refusals_tenant_uploads: u64,
    /// Requests refused because the body budget stayed spent for
    /// [`UploadLimits::budget_wait`].
    upload_refusals_budget: u64,
    /// Bodies cut because no byte of them arrived for
    /// [`UploadLimits::stall_timeout`], or because fewer than
    /// [`UploadLimits::min_window_bytes`] of them arrived in one window of
    /// it.
    upload_refusals_too_slow: u64,
    /// Requests an embedder refused ([`Gateway::refuse`]). They count in
    /// `requests` too.
    embedder_refusals: u64,
    /// The body budget in effect, the small-body headroom included: an
    /// unset `--body-budget-bytes` grows with `--max-request-body-bytes`.
    upload_budget_bytes: u64,
    /// Bytes of the body budget reserved now, each body's share rounded up
    /// to whole KiB.
    upload_reserved_bytes: u64,
    /// Buckets with a large body being read under an upload limit now.
    upload_tenants: usize,
}

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub listen: SocketAddr,
    pub upstreams: Vec<String>,
    /// Covers only response headers so SSE bodies stay open.
    pub response_header_timeout: Duration,
    pub connect_timeout: Duration,
    /// Seconds data sent to an upstream may stay unacknowledged before the
    /// connection is closed and its request fails (Linux `TCP_USER_TIMEOUT`).
    /// An upstream that vanishes without closing its connections, as a
    /// force-deleted pod's address or a powered-off host does, otherwise
    /// holds a request on a pooled connection until the kernel gives up,
    /// after about 15 minutes. 0.7.0 used reqwest's default of 30 s. Idle
    /// connections are probed three times within it (see `UpstreamKeepalive`).
    pub upstream_tcp_user_timeout_secs: NonZeroU64,
    /// The most the gateway buffers of one request's body, the budget for the
    /// bodies it buffers across requests, and the pace those bodies must
    /// keep.
    pub uploads: UploadLimits,
    /// Raft topology used to share one learned leader across every stream in
    /// the same group. `None` preserves per-stream affinity for standalone
    /// gateways that do not know the server topology.
    pub raft_group_count: Option<usize>,
    /// Origins allowed to read across origins, or a single `*`. Empty disables
    /// CORS entirely, leaving responses byte-identical to a deployment without
    /// it. Anonymous `public_read` buckets are unreachable from browser
    /// JavaScript until this is set.
    pub cors_allowed_origins: Vec<String>,
    /// Unit size for [`UsageCounters::chunks`]. `None` leaves the counter at
    /// zero, which is right for any deployment not billing per unit.
    ///
    /// An append is counted as `ceil(bytes / unit)`, never below one, so a
    /// batched write costs proportionally less than the same records sent
    /// individually. That is not an accident of the accounting: batching is
    /// cheaper to serve, and a unit large enough to hold several records is
    /// how the price says so.
    pub usage_chunk_bytes: Option<u64>,
}

#[derive(Clone)]
pub struct Gateway {
    config: GatewayConfig,
    client: reqwest::Client,
    response_header_timeout: Duration,
    access_control: Option<AccessControl>,
    quota_provider: Option<Arc<dyn QuotaProvider>>,
    admission: Arc<Admission>,
    uploads: Arc<Uploads>,
    usage: Option<Arc<UsageCollector>>,
    shard_map: Option<StaticShardMap>,
    leader_affinity: Arc<Mutex<HashMap<String, String>>>,
    ejections: Arc<Ejections>,
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
        let client = upstream_client(&config);
        let response_header_timeout = config.response_header_timeout;
        // The longest a request to a dead upstream takes to fail at the
        // transport, so a probe that has neither answered nor failed by then
        // is not waiting on a dead node.
        let probe_deadline = config.connect_timeout.saturating_add(Duration::from_secs(
            config.upstream_tcp_user_timeout_secs.get(),
        ));
        let cors = crate::cors::CorsPolicy::new(config.cors_allowed_origins.clone());
        let uploads = Arc::new(Uploads::new(&config.uploads));
        Self {
            config,
            client,
            response_header_timeout,
            access_control: None,
            quota_provider: None,
            admission: Arc::new(Admission::default()),
            uploads,
            usage: None,
            shard_map,
            leader_affinity: Arc::new(Mutex::new(HashMap::new())),
            ejections: Arc::new(Ejections::new(probe_deadline)),
            metrics: Arc::new(GatewayMetrics::default()),
            cors,
        }
    }

    /// Installs provider-neutral authentication and authorization hooks.
    /// The gateway then answers its metrics path `404` unless
    /// [`AccessControl::with_public_metrics`] makes the metrics public.
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

    /// Answers one client request.
    ///
    /// A refusal decided from the request's head alone (access control,
    /// tenant admission) is returned once the request's body has been read
    /// and dropped: within `max_request_body_bytes`, 5 s without data and
    /// 30 s in all, so that a client that sends its whole body first gets the
    /// refusal (#540). The future can take that long, and the caller pays the
    /// body's ingress.
    pub async fn handle(&self, req: Request<Body>) -> AxumResponse {
        self.handle_with_cors(req).await
    }

    async fn handle_with_cors(&self, req: Request<Body>) -> AxumResponse {
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
        if self.serves_metrics(&parts) {
            return axum::Json(self.metrics_snapshot()).into_response();
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);

        let context = match self.gate(&parts).await {
            Ok(context) => context,
            Err(refusal) => return self.refuse_unread(refusal, body, REFUSED_PACE).await,
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
                Err(refusal) => return self.refuse_unread(refusal, body, REFUSED_PACE).await,
            }
        }

        let limit = self.request_limit(context.as_ref());
        // Held until the buffered body is dropped, after it was forwarded.
        let (body_bytes, _permit) = match self.buffer(body, context.as_ref(), limit).await {
            Ok(buffered) => buffered,
            Err(refused) => return refused,
        };

        let upstream = match self.pick_upstream(&parts.uri) {
            Some(u) => u,
            None => {
                error!("no upstreams configured");
                return Refusal::NoUpstream.into_response();
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
                    // Only appends carry a unit count. A read costs bytes out,
                    // which `response_bytes` already carries, and charging it a
                    // write unit would double-count the same request.
                    chunks: match (ctx.class, self.config.usage_chunk_bytes) {
                        (UsageClass::Append, Some(unit)) => {
                            crate::usage::chunks_for(body_bytes.len() as u64, unit)
                        }
                        _ => 0,
                    },
                    response_bytes: 0,
                })
            }),
            _live_guard: live_guard,
            credential_expires_at: context.as_ref().and_then(|ctx| ctx.credential_expires_at),
        };

        match self.forward(&upstream, &parts, body_bytes, tail).await {
            Ok(mut response) => {
                lower_request_limit(response.headers_mut(), limit);
                response
            }
            Err(e) => {
                error!(error = ?e, "gateway request failed");
                e.into_response()
            }
        }
    }

    /// The largest body the gateway forwards for a request: the smaller of
    /// `max_request_body_bytes` and the tenant's limit.
    fn request_limit(&self, context: Option<&RequestContext>) -> u64 {
        let global =
            u64::try_from(self.config.uploads.max_request_body_bytes()).unwrap_or(u64::MAX);
        context
            .and_then(|ctx| ctx.limits.max_request_body_bytes)
            .map_or(global, |tenant| tenant.min(global))
    }

    /// The pace of the reading of a body the gateway may forward
    /// ([`Pace::Live`]): [`UploadLimits::stall_timeout`] without a byte, and
    /// [`UploadLimits::min_bytes_per_sec`] over each window of the stall
    /// timeout.
    fn forwarded_pace(&self) -> Pace {
        Pace::Live {
            stall_timeout: self.config.uploads.stall_timeout(),
            min_window_bytes: self.config.uploads.min_window_bytes(),
        }
    }

    /// Reads the `body` of a request the gateway forwards whole, within
    /// `limit` ([`Gateway::request_limit`]), the body budget and the
    /// bucket's upload limits ([`uploads`]), at the pace
    /// [`Gateway::forwarded_pace`] sets. Returns the body and its share of
    /// the budget, which the caller holds until it has forwarded the body,
    /// or the answer to the request: a refusal.
    ///
    /// A body the gateway refuses for its size or for the budget is read to
    /// its end first, up to `max_request_body_bytes` and at the same pace, so
    /// that the client gets the refusal on an intact connection: one over the
    /// bucket's own limit, by its declared length or as it arrives, one that
    /// a bucket's upload limits refuse, and one that gets no share of the
    /// budget. Only a body past `max_request_body_bytes` gets its `413` with
    /// the rest unread, after which the connection closes, as before.
    async fn buffer(
        &self,
        body: Body,
        context: Option<&RequestContext>,
        limit: u64,
    ) -> Result<(bytes::Bytes, Option<BodyPermit>), AxumResponse> {
        if body.is_end_stream() {
            return Ok((bytes::Bytes::new(), None));
        }
        let pace = self.forwarded_pace();
        if body.size_hint().lower() > limit {
            return Err(self
                .refuse_unread(Refusal::BodyTooLarge { limit }, body, pace)
                .await);
        }
        let declared = body.size_hint().exact();
        // A body without a declared length starts small, and its reservation
        // grows as it arrives.
        let reserve = declared
            .unwrap_or_else(|| self.uploads.undeclared_start())
            .min(limit);
        let mut claim = None;
        if !self.uploads.is_small(reserve) {
            match self.claim_upload(context, reserve) {
                Ok(claimed) => claim = claimed,
                Err(refusal) => return Err(self.refuse_unread(refusal, body, pace).await),
            }
        }
        let mut permit = match self.uploads.reserve(reserve).await {
            Ok(permit) => permit,
            Err(BudgetExhausted) => {
                // The bucket's upload limits bound bodies the gateway may
                // forward, not the reading of a refused one.
                drop(claim);
                return Err(self
                    .refuse_unread(self.budget_exhausted(), body, pace)
                    .await);
            }
        };

        let mut reader = BodyReader::new(body, self.config.uploads.max_request_body_bytes(), pace);
        // Each chunk is copied as it arrives and dropped, so that the body
        // holds no more than its share of the budget: a chunk can be a
        // slice of a larger read buffer, which it would keep alive. A
        // declared body gets all of its share at once, and one without a
        // declared length grows within its share as it arrives.
        let mut buffered = BodyBuffer::new(declared.map_or(0, |_| permit.bytes()));
        loop {
            let chunk = match reader.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(end) => return Err(self.cut_short(end, buffered.len(), limit)),
            };
            self.metrics
                .body_bytes_buffered
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            let len = buffered.len().saturating_add(chunk.len() as u64);
            if len > limit {
                // Past the bucket's own limit, within the gateway's: the
                // rest is read and dropped, as for a refusal from the head.
                drop((chunk, buffered, permit, claim));
                return Err(self
                    .refuse_rest(Refusal::BodyTooLarge { limit }, reader)
                    .await);
            }
            if len > permit.bytes() {
                let grown = permit.bytes().saturating_mul(2).max(len).min(limit);
                if let Err(refusal) = self
                    .grow_upload(context, &mut claim, &mut permit, grown)
                    .await
                {
                    drop((chunk, buffered, permit, claim));
                    return Err(self.refuse_rest(refusal, reader).await);
                }
            }
            buffered.push(&chunk, permit.bytes());
        }
        // The bucket's upload limits bound bodies while they arrive: a
        // buffered body no longer counts, while it still holds its share of
        // the budget.
        drop(claim);
        let body_bytes = buffered.finish(&mut permit);
        Ok((body_bytes, Some(permit)))
    }

    /// Counts a large body of `bytes` against its bucket's upload limits
    /// ([`Admission::admit_upload`]). `Ok(None)` without access control or a
    /// quota policy, or when the bucket has no upload limit.
    fn claim_upload(
        &self,
        context: Option<&RequestContext>,
        bytes: u64,
    ) -> Result<Option<UploadGuard>, Refusal> {
        let Some(context) = context else {
            return Ok(None);
        };
        self.admission
            .admit_upload(&context.bucket_id, context.limits, bytes)
            .map_err(|rejection| self.upload_refused(rejection))
    }

    /// Grows the share of a body without a declared length that arrived past
    /// it to `bytes`: against its bucket's upload limits, claimed once it no
    /// longer counts as small, and from the budget.
    async fn grow_upload(
        &self,
        context: Option<&RequestContext>,
        claim: &mut Option<UploadGuard>,
        permit: &mut BodyPermit,
        bytes: u64,
    ) -> Result<(), Refusal> {
        if !self.uploads.is_small(bytes) {
            match claim {
                Some(claim) => claim
                    .grow(bytes)
                    .map_err(|rejection| self.upload_refused(rejection))?,
                None if self.uploads.is_small(permit.bytes()) => {
                    *claim = self.claim_upload(context, bytes)?;
                }
                None => {}
            }
        }
        self.uploads
            .grow(permit, bytes)
            .await
            .map_err(|BudgetExhausted| self.budget_exhausted())
    }

    /// Counts a bucket's upload refusal and returns it.
    fn upload_refused(&self, rejection: AdmissionRejection) -> Refusal {
        let counter = match rejection {
            AdmissionRejection::UploadsExhausted => {
                Some(&self.metrics.upload_refusals_tenant_uploads)
            }
            AdmissionRejection::UploadBytesExhausted => {
                Some(&self.metrics.upload_refusals_tenant_bytes)
            }
            AdmissionRejection::RateLimited { .. } | AdmissionRejection::LiveReadsExhausted => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        debug!(?rejection, "refused an upload");
        Refusal::Admission(rejection)
    }

    /// Counts a request that got no share of the body budget, and returns
    /// its refusal.
    fn budget_exhausted(&self) -> Refusal {
        tracing::warn!(
            reserved_bytes = self.uploads.reserved_bytes(),
            "the request body budget stayed spent; refusing an upload"
        );
        self.metrics
            .upload_refusals_budget
            .fetch_add(1, Ordering::Relaxed);
        Refusal::UploadBudgetExhausted
    }

    /// The answer to a forwarded request whose body ended before its end:
    /// past `max_request_body_bytes`, cut for its pace, or failed. `limit`
    /// is the request's limit ([`Gateway::request_limit`]), which a `413`
    /// names.
    fn cut_short(&self, end: ReadEnd, read_bytes: u64, limit: u64) -> AxumResponse {
        let refusal = match end {
            ReadEnd::OverLimit => Refusal::BodyTooLarge { limit },
            ReadEnd::Stalled | ReadEnd::TimedOut | ReadEnd::TooSlow => {
                self.metrics
                    .upload_refusals_too_slow
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(read_bytes, ?end, "cut a request body that stopped arriving");
                Refusal::BodyTooSlow
            }
            ReadEnd::Whole | ReadEnd::Failed => Refusal::BodyUnreadable,
        };
        refusal.into_response()
    }

    /// Classifies and authorizes the request. `Ok(None)` means access control
    /// is not installed and the gateway keeps its pass-through behavior.
    async fn gate(
        &self,
        parts: &axum::http::request::Parts,
    ) -> Result<Option<RequestContext>, Refusal> {
        let Some(access_control) = self.access_control.as_ref() else {
            return Ok(None);
        };
        let Some(request) = classify_request(&parts.method, &parts.uri, &parts.headers) else {
            // An access-controlled gateway is a public resource server, not a
            // transparent escape hatch to internal or newly added routes.
            return Err(Refusal::NotFound);
        };

        let principal = match bearer_token(&parts.headers) {
            Ok(Some(token)) => match access_control.principal_resolver().resolve(token).await {
                Ok(principal) => Some(principal),
                Err(error) => return Err(authentication_refusal(error)),
            },
            Ok(None) => None,
            Err(()) => {
                return Err(authentication_refusal(
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
            Ok(AuthorizationDecision::Deny) => return Err(Refusal::Forbidden),
            Ok(AuthorizationDecision::ConcealAsNotFound) => return Err(Refusal::Concealed),
            Err(error) => {
                error!(error = %error, "gateway authorization failed");
                return Err(Refusal::Authorization(error));
            }
        }

        let limits = match &self.quota_provider {
            Some(provider) => provider.limits_for(&bucket_id).await,
            None => TenantLimits::default(),
        };
        Ok(Some(RequestContext {
            bucket_id,
            class: usage_class(action),
            credential_expires_at: principal.as_ref().map(|principal| principal.expires_at),
            principal: principal.as_ref().map(principal_ref),
            limits,
        }))
    }

    /// Enforces the tenant's admission limits. `Retry-After` accompanies every
    /// retryable rejection; Ursula's own `503` backpressure semantics are
    /// untouched.
    fn admit(&self, context: &RequestContext) -> Result<Option<LiveReadGuard>, Refusal> {
        if self.quota_provider.is_none() {
            return Ok(None);
        }
        self.admission
            .admit_request(&context.bucket_id, context.limits)
            .map_err(Refusal::Admission)?;
        if context.class == UsageClass::LiveRead {
            return self
                .admission
                .admit_live_read(&context.bucket_id, context.limits)
                .map_err(Refusal::Admission);
        }
        Ok(None)
    }

    /// Answers with `refusal`, which the gateway decided before it read the
    /// request's body: from the request's head alone ([`Gateway::gate`],
    /// [`Gateway::admit`]), or before it buffered the body
    /// ([`Gateway::buffer`]). It first reads the request's `body` to its end
    /// at `pace`, dropping each chunk as it arrives (#540). Answered with its
    /// body unread, a request over HTTP/1.1 loses its connection: hyper reads
    /// once more, and closes the connection unless that read reached the end
    /// of the body. A client that sends its whole body before it reads the
    /// answer, as ureq (the SQLite extension's HTTP client) does, then meets
    /// a failed write or a reset instead of the refusal, which says nothing
    /// about whether the request was applied.
    ///
    /// Each request's reading is bounded ([`BodyReader`]):
    /// `max_request_body_bytes` at most, the most the gateway buffers of a
    /// request it forwards, and the two timeouts of `pace`. A refusal from
    /// the head is read at [`REFUSED_PACE`]: [`REFUSED_BODY_STALL_TIMEOUT`]
    /// without a chunk, and [`REFUSED_BODY_DRAIN_TIMEOUT`] in all. A refusal
    /// of a request the gateway would otherwise forward is read at the pace
    /// of a forwarded body ([`Gateway::forwarded_pace`]), so that a slow
    /// upload that meets a spent budget gets its refusal too. At any bound
    /// the refusal goes out at once and the connection closes, as after the
    /// `413` for a body over the limit. A body whose declared length is over
    /// the limit is not read at all, since the reading would stop at the
    /// limit before the body's end. Over HTTP/2 the body is read the same
    /// way, and a reading cut short resets only the request's stream. A
    /// reading keeps no byte of the body, holds no share of the body budget
    /// and adds no task: it runs in the request's own handler. So the
    /// requests under way bound the readings under way. A refused client
    /// holds its connection no longer than a forwarded body may take, and the
    /// gateway puts no time limit on a slow head or on an idle connection, so
    /// the reading gives a refused client no new way to hold a connection.
    ///
    /// Reading the body of a request that carries `Expect: 100-continue` has
    /// hyper send `100 Continue`, so such a client sends its body before it
    /// gets the refusal. Answering at once would spare it the upload only
    /// while it still waits for `100 Continue`. It waits 1 s by default
    /// (ureq 3, curl) and then sends the body anyway, and a decision that
    /// waits on the token issuer or the authorizer can take longer: the client
    /// would then meet the closed connection again.
    async fn refuse_unread(&self, refusal: Refusal, body: Body, pace: Pace) -> AxumResponse {
        if body.is_end_stream() {
            return refusal.into_response();
        }
        let limit = self.config.uploads.max_request_body_bytes();
        if body.size_hint().lower() > u64::try_from(limit).unwrap_or(u64::MAX) {
            // Counted as a reading cut short, without one.
            self.metrics.refusal_drains.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .refusal_drains_cut
                .fetch_add(1, Ordering::Relaxed);
            debug!("refused a request whose body is declared over the limit, unread");
            return refusal.into_response();
        }
        self.refuse_rest(refusal, BodyReader::new(body, limit, pace))
            .await
    }

    /// Answers with `refusal` once `reader` has read the rest of the body to
    /// its end, dropping each chunk ([`Gateway::refuse_unread`]).
    async fn refuse_rest(&self, refusal: Refusal, reader: BodyReader) -> AxumResponse {
        self.metrics.refusal_drains.fetch_add(1, Ordering::Relaxed);
        // Counted as cut unless the body was read to its end, also when the
        // request is dropped while its body is read.
        let mut cut = CutUnlessWhole {
            cut: &self.metrics.refusal_drains_cut,
            whole: false,
        };
        let drained = reader.discard(&self.metrics.refusal_drain_bytes).await;
        cut.whole = drained == ReadEnd::Whole;
        debug!(?drained, "read the body of a refused request");
        refusal.into_response()
    }

    /// The cached leader route of the request's stream or group, unless its
    /// upstream is ejected, else an upstream that is not ejected at random
    /// (see `ejection`).
    fn pick_upstream(&self, uri: &Uri) -> Option<String> {
        let key = upstream_pin_key(uri, self.shard_map.as_ref());
        // Its own statement: the cache's lock is released before the
        // ejection lock is taken and before the cache may be taken again.
        let cached = key.as_ref().and_then(|key| {
            self.leader_affinity
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .cloned()
        });
        let pick = self.ejections.pick(
            cached.as_deref(),
            &self.config.upstreams,
            Instant::now(),
            &mut rand::rng(),
        )?;
        if pick.cached {
            self.metrics
                .leader_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .leader_cache_misses
                .fetch_add(1, Ordering::Relaxed);
            // A route to an upstream that is ejected or being probed.
            if let (Some(key), Some(cached)) = (&key, &cached) {
                self.forget_leader_if_matches(key, cached);
            }
        }
        if pick.probe {
            debug!(
                upstream = pick.upstream,
                "probing an upstream whose ejection window expired"
            );
        }
        Some(pick.upstream.to_owned())
    }

    /// Forwards a request to `upstream` and follows the Raft leader redirects
    /// of its answers (see [`Chain`]), so that a client never gets one.
    ///
    /// - A leader redirect (`NotLeader`) sends the request to the upstream its
    ///   `Location` names, unless the request already went to that upstream in
    ///   this chain.
    /// - A redirect from a node that hosts no replica of the group
    ///   (`NotHosted`) names a host only as a hint, often the dead node the
    ///   group moves away from. The request goes there only when the upstream
    ///   is not ejected ([`Ejections::claim`]).
    /// - Otherwise, and when the `Location` names no upstream (`--upstream`
    ///   names the nodes otherwise than their client URLs), the request goes to
    ///   an upstream it has not tried yet, the next after the one that
    ///   redirected in the configured order.
    /// - A search hop or a hinted hop that cannot connect sent nothing, so the
    ///   search goes on to the next upstream. A leader hop that cannot connect
    ///   ends the request.
    /// - A hop that sends no response headers within the response header
    ///   timeout may have delivered the request to an upstream that accepted
    ///   it and never answers, such as a stopped process. A request whose
    ///   repeat is safe ([`Replay`]) goes on, once, to an upstream it has not
    ///   tried, which answers or redirects to the current leader. Any other
    ///   request ends with the timeout, whose outcome is unknown.
    /// - At most [`MAX_LEADER_REDIRECTS`] hops follow redirects, and none
    ///   starts later than [`LEADER_REDIRECT_DEADLINE`] after the first
    ///   redirect. When no upstream answers without redirecting within those
    ///   limits, the client gets a retryable `503`
    ///   ([`GatewayError::LeaderNotFound`]).
    ///
    /// Each hop sends the request again. A node redirects a write only when
    /// it refused the write before proposing it, so no hop after a redirect
    /// meets a write an earlier hop applied, and a `503 NO_LEADER` after the
    /// hops is a refusal too. Only a producer write goes on after a hop that
    /// answered nothing, and its key keeps the earlier hop's apply from
    /// applying it twice.
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
        let pin_key = upstream_pin_key(&parts.uri, self.shard_map.as_ref());
        let replay = Replay::of(parts);
        let started = Instant::now();
        let mut chain = Chain::new(upstream);
        loop {
            let target = chain.target.clone();
            let target_url = format!("{}{}", target.trim_end_matches('/'), path_and_query);
            let sent = self
                .send_request(chain.hop(), &target, &target_url, parts, body.clone())
                .await;
            let response = match sent {
                Ok(response) => response,
                Err(error) => {
                    self.upstream_failed(pin_key.as_deref(), &target, &error);
                    // A search or hinted hop that could not connect sent
                    // nothing: look on among the upstreams the request has not
                    // tried.
                    if chain.lead.searches_on_connect_failure()
                        && matches!(error, GatewayError::LeaderUnreachable { .. })
                        && let Some(next) = chain
                            .may_follow(Instant::now())
                            .then(|| self.untried_upstream(&chain))
                            .flatten()
                    {
                        self.followed(&target, &next, Lead::Search);
                        chain.follow(next, Lead::Search);
                        continue;
                    }
                    // A hop that may have delivered the request and answered
                    // nothing in time: a request whose repeat is safe goes
                    // on, once, to an upstream it has not tried.
                    if matches!(error, GatewayError::UpstreamTimeout { .. })
                        && replay != Replay::Never
                        && let Some(next) = chain
                            .may_replay()
                            .then(|| self.untried_upstream(&chain))
                            .flatten()
                    {
                        debug!(
                            upstream = target,
                            next, "no answer in time; sending the request to another upstream"
                        );
                        self.followed(&target, &next, Lead::Search);
                        chain.replay(next, error);
                        continue;
                    }
                    self.chain_ended(&chain, started);
                    return Err(chain.ending(replay, error));
                }
            };
            self.upstream_answered(&target);
            if !is_raft_redirect(&response) {
                self.chain_ended(&chain, started);
                if replay == Replay::Keyed && !response.status().is_success() {
                    // Only a receipt settles a write an earlier hop of the
                    // chain may still apply.
                    if let Some(unanswered) = chain.unanswered.take() {
                        return Err(unanswered);
                    }
                }
                return self.answer(response, tail, &chain, pin_key.as_deref());
            }

            // The node does not lead the request's group, so a route to it
            // is stale.
            if let Some(key) = &pin_key {
                self.forget_leader_if_matches(key, &target);
            }
            chain.redirected(Instant::now());
            let hinted = is_not_hosted(&response);
            let named = self
                .resolve_leader_upstream(response.headers())
                .map(str::to_owned);
            // Drop the redirect; it has no meaningful body.
            drop(response);
            let next = chain
                .may_follow(Instant::now())
                .then(|| {
                    if hinted {
                        self.next_hinted_hop(&chain, named)
                    } else {
                        self.next_hop(&chain, named)
                    }
                })
                .flatten();
            let Some((next, lead)) = next else {
                self.metrics
                    .leader_not_found
                    .fetch_add(1, Ordering::Relaxed);
                self.chain_ended(&chain, started);
                let followed = chain.redirects;
                return Err(chain.ending(replay, GatewayError::LeaderNotFound { followed }));
            };
            self.followed(&target, &next, lead);
            chain.follow(next, lead);
        }
    }

    /// Where a request goes after a redirect from the hop under way, and
    /// whether that is a search hop. `named` is the upstream the redirect's
    /// `Location` names. It goes there unless the request already went to it,
    /// else to an upstream it has not tried ([`Gateway::untried_upstream`]),
    /// else back to `named`. `None` when there is nowhere to go.
    fn next_hop(&self, chain: &Chain, named: Option<String>) -> Option<(String, Lead)> {
        match named {
            Some(leader) if !chain.tried.contains(&leader) => Some((leader, Lead::Leader)),
            named => self
                .untried_upstream(chain)
                .map(|upstream| (upstream, Lead::Search))
                .or_else(|| named.map(|leader| (leader, Lead::Leader))),
        }
    }

    /// Where a request goes after a `NotHosted` redirect, whose `Location`
    /// names `named`, a host as the redirecting node's view has it. It goes
    /// there only when the request has not tried it and [`Ejections::claim`]
    /// admits it, else to an upstream it has not tried
    /// ([`Gateway::untried_upstream`]), else to `named` all the same, as
    /// selection falls back to an ejected upstream rather than none. `None`
    /// when there is nowhere to go.
    fn next_hinted_hop(&self, chain: &Chain, named: Option<String>) -> Option<(String, Lead)> {
        if let Some(host) = &named
            && !chain.tried.contains(host)
            && self.ejections.claim(host, Instant::now())
        {
            return named.map(|host| (host, Lead::Hint));
        }
        self.untried_upstream(chain)
            .map(|upstream| (upstream, Lead::Search))
            .or_else(|| named.map(|host| (host, Lead::Hint)))
    }

    /// Counts a hop that follows a redirect, a search hop among them.
    fn followed(&self, from: &str, to: &str, lead: Lead) {
        debug!(from, to, ?lead, "following a leader redirect");
        self.metrics
            .leader_redirects
            .fetch_add(1, Ordering::Relaxed);
        if lead == Lead::Search {
            self.metrics.leader_searches.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The client's answer from the upstream that ended the chain, which did
    /// not redirect.
    fn answer(
        &self,
        response: reqwest::Response,
        tail: ResponseTail,
        chain: &Chain,
        pin_key: Option<&str>,
    ) -> Result<Response<Body>, GatewayError> {
        let status = response.status();
        if let Some(key) = pin_key {
            if status == StatusCode::SERVICE_UNAVAILABLE
                && response.headers().contains_key(RETRY_AFTER)
            {
                // A cached leader can become a follower after an election.
                // When that follower does not know the new leader yet, Ursula
                // returns the explicit retryable 503 form instead of a 307. Do
                // not replay the current request here: a generic write may not
                // be idempotent. Evict only the matching cached route so the
                // caller's next retry gets a fresh random/redirect lookup.
                self.forget_leader_if_matches(key, &chain.target);
            } else if chain.redirects > 0 && status.is_success() {
                // A node a redirect led to served the request: it leads the
                // group, as far as the gateway can tell. Any other answer,
                // which a node can give before its leadership check, teaches
                // nothing.
                self.remember_leader(key.to_owned(), chain.target.clone());
            }
        }
        Self::build_response(response, tail)
    }

    /// Counts the time a request spent on its hops, when it followed a
    /// redirect.
    fn chain_ended(&self, chain: &Chain, started: Instant) {
        if chain.redirects > 0 {
            self.metrics.leader_redirect_ns.fetch_add(
                u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
    }

    /// The next upstream after the hop under way, in the configured order and
    /// around, that the request has not tried yet and that selection does
    /// not skip (see `ejection`). One whose ejection window has expired becomes
    /// this request's probe. Where the gateway looks for the leader after a
    /// redirect it cannot follow. Starting after the redirecting upstream
    /// spreads the search over the upstreams.
    fn untried_upstream(&self, chain: &Chain) -> Option<String> {
        let upstreams = &self.config.upstreams;
        let after = upstreams
            .iter()
            .position(|upstream| *upstream == chain.target)
            .map_or(0, |at| at.saturating_add(1));
        let now = Instant::now();
        upstreams
            .iter()
            .cycle()
            .skip(after)
            .take(upstreams.len())
            .find(|upstream| !chain.tried.contains(upstream) && self.ejections.claim(upstream, now))
            .cloned()
    }

    /// Covers only response headers so SSE bodies stay open.
    async fn send_request(
        &self,
        hop: Hop,
        upstream: &str,
        url: &str,
        parts: &axum::http::request::Parts,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response, GatewayError> {
        debug!(method = %parts.method, url = %url, "sending upstream request");

        let timeout = self.response_header_timeout_for_url(url);
        tokio::time::timeout(
            timeout,
            self.client
                .request(parts.method.clone(), url)
                .headers(copy_forwarded_headers(&parts.headers, true))
                .body(body)
                .send(),
        )
        .await
        .map_err(|_elapsed| GatewayError::UpstreamTimeout {
            upstream: upstream.to_owned(),
            timeout,
        })?
        .map_err(|source| {
            let upstream = upstream.to_owned();
            // `is_connect` holds only for a failure before the request was
            // handed to a connection: in the connector (DNS, TCP or TLS
            // setup, or the connect timeout) or in the pool checkout. So this
            // attempt wrote nothing of the request. Any later failure, such
            // as an upstream that read the request and closed the connection
            // without answering, is another error. Inside one `send`, hyper
            // retries only a request that a pooled connection gave back
            // unwritten, and reqwest only one that an HTTP/2 peer refused
            // unprocessed (`REFUSED_STREAM`, or a graceful `GOAWAY`).
            match (source.is_connect(), hop) {
                (true, Hop::First) => GatewayError::UpstreamUnreachable { upstream, source },
                (true, Hop::Followed) => GatewayError::LeaderUnreachable { upstream, source },
                (false, _) => GatewayError::Upstream { upstream, source },
            }
        })
    }

    /// An upstream whose request failed loses its cached route, so the next
    /// request learns the current leader instead of failing on the same node.
    /// One that failed at the transport (down or gone) also leaves selection
    /// for a window, so that request does not pick it again at random (#454).
    fn upstream_failed(&self, pin_key: Option<&str>, upstream: &str, error: &GatewayError) {
        if let Some(key) = pin_key {
            self.forget_leader_if_matches(key, upstream);
        }
        if !error.ejects_upstream() {
            return;
        }
        if let Failed::Ejected { window, failures } =
            self.ejections.failed(upstream, Instant::now())
        {
            self.metrics
                .upstream_ejections
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                upstream,
                window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX),
                failures,
                "upstream failed at the transport; leaving it out of selection"
            );
        }
    }

    /// Any response proves the upstream reachable again.
    fn upstream_answered(&self, upstream: &str) {
        if self.ejections.answered(upstream) {
            tracing::info!(upstream, "upstream answers again; back in selection");
        }
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
            // A redirect other than a Raft leader redirect, which the gateway
            // follows itself, stays client-facing: strip the internal host so
            // the client reconnects through the gateway, not directly to the
            // Ursula node.
            for name in [
                names::X_URSULA_RAFT_LEADER_ID,
                names::X_URSULA_RAFT_GROUP,
                names::X_URSULA_RAFT_TERM,
            ] {
                headers.remove(name);
            }
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

        // Computed before `headers` moves into the response. Only responses that
        // stream indefinitely get a deadline: cutting a normal read short because
        // its credential lapsed mid-transfer would corrupt a body the client is
        // already committed to reading.
        let deadline = tail.credential_expires_at.and_then(|expires_at| {
            let streams_indefinitely = headers
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/event-stream"));
            if !streams_indefinitely {
                return None;
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_secs();
            Some(CredentialDeadline {
                sleep: Box::pin(tokio::time::sleep(Duration::from_secs(
                    expires_at.saturating_sub(now),
                ))),
                terminal_emitted: false,
            })
        });

        let mut response = Response::builder().status(status);
        if let Some(response_headers) = response.headers_mut() {
            *response_headers = headers;
        }
        response
            .body(Body::from_stream(CountedStream {
                inner: upstream_resp.bytes_stream(),
                tail,
                deadline,
            }))
            .map_err(|e| GatewayError::ResponseBuild(e.to_string()))
    }

    /// The upstream a redirect's `Location` names: the one it starts with,
    /// followed by a path, a query or nothing. `http://10.0.0.1:443` does not
    /// name `http://10.0.0.1:4437/...`.
    fn resolve_leader_upstream(&self, response_headers: &HeaderMap) -> Option<&str> {
        let location = response_headers.get(LOCATION)?.to_str().ok()?;
        self.config
            .upstreams
            .iter()
            .find(|upstream| {
                location
                    .strip_prefix(upstream.trim_end_matches('/'))
                    .is_some_and(|rest| {
                        rest.is_empty() || rest.starts_with('/') || rest.starts_with('?')
                    })
            })
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

    /// Whether the gateway answers the request with its own counters itself,
    /// before [`Gateway::gate`]: a `GET` on [`GATEWAY_METRICS_PATH`], without
    /// access control or with access control that makes the metrics public
    /// ([`AccessControl::with_public_metrics`]). Otherwise the request goes
    /// through `gate`. With access control, `gate` answers it `404`, as it
    /// answers every other `/__ursula/*` path: an access-controlled gateway
    /// can face the internet, and the counters show its traffic volume
    /// (#548). Without access control, the gateway forwards it like any other
    /// request.
    fn serves_metrics(&self, parts: &axum::http::request::Parts) -> bool {
        parts.method == Method::GET
            && parts.uri.path() == GATEWAY_METRICS_PATH
            && self
                .access_control
                .as_ref()
                .is_none_or(AccessControl::public_metrics)
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
            leader_searches: self.metrics.leader_searches.load(Ordering::Relaxed),
            leader_not_found: self.metrics.leader_not_found.load(Ordering::Relaxed),
            leader_cache_entries,
            upstream_ejections: self.metrics.upstream_ejections.load(Ordering::Relaxed),
            upstream_ejected: self.ejections.active(Instant::now()),
            refusal_drains: self.metrics.refusal_drains.load(Ordering::Relaxed),
            refusal_drains_cut: self.metrics.refusal_drains_cut.load(Ordering::Relaxed),
            refusal_drain_bytes: self.metrics.refusal_drain_bytes.load(Ordering::Relaxed),
            body_bytes_buffered: self.metrics.body_bytes_buffered.load(Ordering::Relaxed),
            upload_refusals_tenant_bytes: self
                .metrics
                .upload_refusals_tenant_bytes
                .load(Ordering::Relaxed),
            upload_refusals_tenant_uploads: self
                .metrics
                .upload_refusals_tenant_uploads
                .load(Ordering::Relaxed),
            upload_refusals_budget: self.metrics.upload_refusals_budget.load(Ordering::Relaxed),
            upload_refusals_too_slow: self
                .metrics
                .upload_refusals_too_slow
                .load(Ordering::Relaxed),
            embedder_refusals: self.metrics.embedder_refusals.load(Ordering::Relaxed),
            upload_budget_bytes: self.config.uploads.budget_bytes(),
            upload_reserved_bytes: self.uploads.reserved_bytes(),
            upload_tenants: self.admission.upload_buckets(),
        }
    }
}

/// Keepalive probing of an idle upstream connection, derived from the
/// upstream `TCP_USER_TIMEOUT` by [`UpstreamKeepalive::for_user_timeout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UpstreamKeepalive {
    /// Idle time before the first probe.
    idle: Duration,
    /// Time between unanswered probes.
    interval: Duration,
    /// Unanswered probes before the connection is closed where
    /// `TCP_USER_TIMEOUT` does not end probing first (not Linux).
    retries: u32,
}

impl UpstreamKeepalive {
    /// The shortest idle time and interval. Linux takes both in whole seconds,
    /// rounding down, and refuses 0.
    const MIN_STEP: Duration = Duration::from_secs(1);

    /// Probing that fits several probes inside a user timeout of
    /// `user_timeout_secs` (T): the first after 2T/5, then every T/5, each at
    /// least 1 s, three in all.
    ///
    /// On Linux the user timeout also ends keepalive probing: a connection
    /// that has received nothing for T while a probe is unanswered is closed,
    /// whatever the retry count. A probe's acknowledgement counts as received.
    /// With T = 5 s, probes go out after 2, 3 and 4 s of silence, and a
    /// connection to a vanished upstream closes at about 5 s. One lost probe
    /// or acknowledgement does not close a healthy idle connection, such as a
    /// long-poll or an SSE read waiting for data. An idle time of T or more
    /// would leave a single probe to decide. Elsewhere the three probes close
    /// a dead connection after `idle + 3 * interval`, which is T for T >= 5 s.
    fn for_user_timeout(user_timeout_secs: NonZeroU64) -> Self {
        let fifth = Duration::from_secs(user_timeout_secs.get())
            .checked_div(5)
            .unwrap_or_default();
        Self {
            idle: fifth.saturating_mul(2).max(Self::MIN_STEP),
            interval: fifth.max(Self::MIN_STEP),
            retries: 3,
        }
    }
}

/// The client for every upstream request. Redirects are the gateway's to
/// follow. A connection to an upstream that stopped answering fails within
/// `upstream_tcp_user_timeout_secs`, whether a request waits on it or it is
/// idle.
///
/// A pooled connection is reused only while it has been idle for at most
/// [`CLIENT_IDLE_BOUND`]. A node's graceful drain keeps a connection that was
/// idle when it began open longer than that, so it answers a request the
/// gateway sends on one in time, and closes the connection after the answer.
/// A request sent on a connection that then closes unanswered may have
/// reached the node, and is a `502`. The drain still closes two kinds of
/// connection that way: one the node had not accepted when its listener
/// closed, and one whose last answer started before the drain
/// ([`ursula_observability::serve::serve_until_shutdown`]).
fn upstream_client(config: &GatewayConfig) -> reqwest::Client {
    let keepalive = UpstreamKeepalive::for_user_timeout(config.upstream_tcp_user_timeout_secs);
    let builder = reqwest::Client::builder()
        .connect_timeout(config.connect_timeout)
        .pool_idle_timeout(CLIENT_IDLE_BOUND)
        .tcp_keepalive(keepalive.idle)
        .tcp_keepalive_interval(keepalive.interval)
        .tcp_keepalive_retries(keepalive.retries)
        // Straight to the nodes: a system proxy from `HTTP_PROXY` would sit
        // between the gateway and the nodes, which must not be (README,
        // Proxies Around the Gateway), and would make a connect failure
        // describe the proxy rather than the node.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none());
    #[cfg(target_os = "linux")]
    let builder = builder.tcp_user_timeout(Duration::from_secs(
        config.upstream_tcp_user_timeout_secs.get(),
    ));
    builder
        .build()
        .expect("static gateway reqwest client config should be valid")
}

/// The leader-cache key of a stream route: its Raft group when the topology
/// is known, else the `/{bucket}/{stream}` prefix. Subresource segments after
/// the stream share the stream's key.
fn upstream_pin_key(uri: &Uri, shard_map: Option<&StaticShardMap>) -> Option<String> {
    let mut segments = uri.path().split('/').filter(|segment| !segment.is_empty());
    let bucket = percent_decode_str(segments.next()?).decode_utf8().ok()?;
    let stream = percent_decode_str(segments.next()?).decode_utf8().ok()?;
    if bucket.starts_with("__ursula") {
        return None;
    }
    let stream_id = BucketStreamId::new(bucket.as_ref(), stream.as_ref());
    if let Some(shard_map) = shard_map {
        let placement = shard_map.locate(&stream_id);
        return Some(format!("group:{}", placement.raft_group_id.0));
    }
    Some(format!("/{stream_id}"))
}

/// Which exchange of a forwarded request a request to an upstream is.
#[derive(Clone, Copy, Debug)]
enum Hop {
    /// The upstream picked for the request. No node received the request
    /// before it.
    First,
    /// An upstream after a Raft redirect: the leader the redirect named, or
    /// an upstream the request has not tried yet.
    Followed,
}

/// What sent a forwarded request to the upstream of the hop under way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lead {
    /// Selection picked it for the first hop.
    Picked,
    /// A leader redirect (`NotLeader`) named it.
    Leader,
    /// A redirect from a node that hosts no replica of the group
    /// (`NotHosted`) named it, a hint that may name a node that is down.
    Hint,
    /// No redirect named it: an upstream the request had not tried.
    Search,
}

impl Lead {
    /// Whether a hop that cannot connect, and so sent nothing, goes on to an
    /// upstream the request has not tried instead of ending the request. A
    /// leader hop ends it: the leader is the only node that can serve it.
    fn searches_on_connect_failure(self) -> bool {
        match self {
            Self::Hint | Self::Search => true,
            Self::Picked | Self::Leader => false,
        }
    }
}

/// Whether the gateway may send a request again, to another upstream, after
/// a hop that may have delivered it answered nothing in time
/// ([`Gateway::forward`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Replay {
    /// A `GET` or a `HEAD`: a repeat changes nothing.
    Read,
    /// A producer write, keyed by `Producer-Id`, `Producer-Epoch` and
    /// `Producer-Seq`: the key applies it at most once however often it is
    /// sent, and a `2xx` receipt says that it holds. Any other answer to the
    /// repeat leaves the outcome of the earlier hop unknown.
    Keyed,
    /// Any other request: a repeat may apply a write twice.
    Never,
}

impl Replay {
    fn of(parts: &axum::http::request::Parts) -> Self {
        if matches!(parts.method, Method::GET | Method::HEAD) {
            return Self::Read;
        }
        let keyed = ursula_headers::ProducerRequestHeaders::parse(
            parts
                .headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .is_ok_and(|headers| headers.producer.is_some());
        if keyed { Self::Keyed } else { Self::Never }
    }
}

/// The hops of one forwarded request ([`Gateway::forward`]).
struct Chain {
    /// The upstream of the hop under way.
    target: String,
    /// Every upstream the request went to, `target` included.
    tried: Vec<String>,
    /// Hops that followed a redirect so far, search hops included.
    redirects: usize,
    /// What sent the request to `target`.
    lead: Lead,
    /// When the first redirect arrived: [`LEADER_REDIRECT_DEADLINE`] counts
    /// from it.
    first_redirect: Option<Instant>,
    /// The timeout of a hop that may have delivered the request, after which
    /// the request was sent again ([`Replay`]).
    unanswered: Option<GatewayError>,
}

impl Chain {
    fn new(first: &str) -> Self {
        Self {
            target: first.to_owned(),
            tried: vec![first.to_owned()],
            redirects: 0,
            lead: Lead::Picked,
            first_redirect: None,
            unanswered: None,
        }
    }

    fn hop(&self) -> Hop {
        if self.redirects == 0 {
            Hop::First
        } else {
            Hop::Followed
        }
    }

    /// Records a redirect that the hop under way got at `now`.
    fn redirected(&mut self, now: Instant) {
        self.first_redirect.get_or_insert(now);
    }

    /// Whether another hop may start at `now`: fewer than
    /// [`MAX_LEADER_REDIRECTS`] followed, and less than
    /// [`LEADER_REDIRECT_DEADLINE`] since the first redirect.
    fn may_follow(&self, now: Instant) -> bool {
        self.redirects < MAX_LEADER_REDIRECTS
            && self
                .first_redirect
                .is_none_or(|first| now.saturating_duration_since(first) < LEADER_REDIRECT_DEADLINE)
    }

    /// Whether the request may be sent again after a hop that answered
    /// nothing in time: it was not sent again before, and it has hops left.
    fn may_replay(&self) -> bool {
        self.unanswered.is_none() && self.redirects < MAX_LEADER_REDIRECTS
    }

    /// Sends the request again, to `next`, after the hop under way answered
    /// nothing in time (`unanswered`). The search for the leader from there
    /// starts its own [`LEADER_REDIRECT_DEADLINE`].
    fn replay(&mut self, next: String, unanswered: GatewayError) {
        self.unanswered = Some(unanswered);
        self.first_redirect = None;
        self.follow(next, Lead::Search);
    }

    /// The error a request of `replay` ends with when its last hop ended
    /// with `error`: a write sent again after a hop that answered nothing
    /// ends with that hop's timeout, since the earlier hop may still apply
    /// it. A read ends with `error`.
    fn ending(&mut self, replay: Replay, error: GatewayError) -> GatewayError {
        match (replay, self.unanswered.take()) {
            (Replay::Keyed, Some(unanswered)) => unanswered,
            (Replay::Read | Replay::Keyed | Replay::Never, _) => error,
        }
    }

    /// Sends the next hop to `next`, which `lead` chose.
    fn follow(&mut self, next: String, lead: Lead) {
        self.redirects = self.redirects.saturating_add(1);
        self.lead = lead;
        self.tried.push(next.clone());
        self.target = next;
    }
}

/// A Raft routing redirect, which the gateway follows itself (an upstream's
/// other redirects reach the client): a `307` that names the leader, or the
/// group alone, from a node that hosts no replica of it
/// ([`ursula_headers::RoutingAnswer::NotHosted`]). The gateway uses no term,
/// so a leader redirect whose term the node did not know counts too.
fn is_raft_redirect(response: &reqwest::Response) -> bool {
    response.status() == StatusCode::TEMPORARY_REDIRECT
        && [names::X_URSULA_RAFT_LEADER_ID, names::X_URSULA_RAFT_GROUP]
            .iter()
            .any(|name| response.headers().contains_key(*name))
}

/// Whether a Raft redirect is [`RoutingAnswer::NotHosted`], whose target is
/// a hint, rather than a leader redirect. A redirect whose routing headers do
/// not parse counts as a leader redirect, as before the hint rule.
fn is_not_hosted(response: &reqwest::Response) -> bool {
    matches!(
        RoutingAnswer::parse(
            response.status().as_u16(),
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        ),
        Ok(Some(RoutingAnswer::NotHosted { .. }))
    )
}

/// Per-request context available once access control has classified and
/// authorized the request.
struct RequestContext {
    bucket_id: String,
    class: UsageClass,
    principal: Option<PrincipalRef>,
    limits: TenantLimits,
    /// `exp` of the presented credential. `None` for anonymous public reads,
    /// which have no credential and therefore no deadline.
    credential_expires_at: Option<u64>,
}

fn usage_class(action: Action) -> UsageClass {
    match action {
        Action::Append | Action::AppendAndClose | Action::Create | Action::CreateAndClose => {
            UsageClass::Append
        }
        Action::Read | Action::Head | Action::ReadSnapshot => UsageClass::Read,
        Action::Tail => UsageClass::LiveRead,
        Action::Update | Action::Delete | Action::PublishSnapshot | Action::AdministerBucket => {
            UsageClass::Admin
        }
    }
}

fn principal_ref(principal: &VerifiedPrincipal) -> PrincipalRef {
    PrincipalRef {
        issuer: principal.issuer.clone(),
        subject: principal.subject.clone(),
    }
}

/// A request the gateway answered itself, before forwarding it to any
/// upstream.
#[derive(Debug)]
enum Refusal {
    /// Access control does not serve the route (`404`).
    NotFound,
    /// The authorizer concealed the resource: the status, headers and body
    /// a node answers for a bucket nothing created, on every route. Like
    /// every answer of the gateway's own, it carries no server identity
    /// (extensions Section 10), so a client never reads it as a deleted
    /// stream of a known deployment.
    Concealed,
    /// The bearer credential failed verification (`401`), or the verifier is
    /// unavailable (`503`).
    Authentication(AuthenticationError),
    /// The authorizer denied the request (`403`).
    Forbidden,
    /// The authorizer could not decide (`503`).
    Authorization(AuthorizationError),
    /// A tenant admission limit (`429`), the bucket's upload limits
    /// included.
    Admission(AdmissionRejection),
    /// The body is larger than the gateway's or the tenant's limit, the
    /// smaller of the two (`413` naming it).
    BodyTooLarge { limit: u64 },
    /// The body got no share of the body budget within
    /// [`UploadLimits::budget_wait`] (`503` with `Retry-After`), see
    /// [`uploads`].
    UploadBudgetExhausted,
    /// No byte of the body arrived for [`UploadLimits::stall_timeout`], or
    /// fewer than [`UploadLimits::min_window_bytes`] arrived in one window of
    /// it, and it was cut before anything of it was forwarded (`408`). Over
    /// HTTP/1.1 the connection then closes on the rest of the body: a client
    /// still sending sees that rather than the `408`.
    BodyTooSlow,
    /// The body could not be read (`400`).
    BodyUnreadable,
    /// No upstream is configured (`503`).
    NoUpstream,
    /// A refusal of the gateway's embedder ([`Gateway::refuse`]).
    Embedder(EmbedderRefusal),
}

impl IntoResponse for Refusal {
    fn into_response(self) -> AxumResponse {
        let mut headers = HeaderMap::new();
        let (status, code) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
            Self::Concealed => (StatusCode::NOT_FOUND, ErrorCode::BucketNotFound),
            Self::Authentication(AuthenticationError::Unavailable) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::AuthenticationUnavailable,
            ),
            Self::Authentication(
                error @ (AuthenticationError::InvalidCredential
                | AuthenticationError::Expired
                | AuthenticationError::WrongAudience),
            ) => {
                headers.insert(
                    WWW_AUTHENTICATE,
                    HeaderValue::from_static(r#"Bearer error="invalid_token""#),
                );
                let code = match error {
                    AuthenticationError::Expired => ErrorCode::CredentialExpired,
                    _ => ErrorCode::InvalidCredential,
                };
                (StatusCode::UNAUTHORIZED, code)
            }
            Self::Forbidden => (StatusCode::FORBIDDEN, ErrorCode::Forbidden),
            Self::Authorization(AuthorizationError::Unavailable) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::AuthorizationUnavailable,
            ),
            Self::Admission(AdmissionRejection::RateLimited { retry_after_secs }) => {
                headers.insert(RETRY_AFTER, HeaderValue::from(retry_after_secs));
                (StatusCode::TOO_MANY_REQUESTS, ErrorCode::RateLimited)
            }
            Self::Admission(AdmissionRejection::LiveReadsExhausted) => {
                (StatusCode::TOO_MANY_REQUESTS, ErrorCode::LiveReadsExhausted)
            }
            Self::Admission(AdmissionRejection::UploadsExhausted) => {
                headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
                (StatusCode::TOO_MANY_REQUESTS, ErrorCode::UploadsExhausted)
            }
            Self::Admission(AdmissionRejection::UploadBytesExhausted) => {
                headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    ErrorCode::UploadBytesExhausted,
                )
            }
            Self::UploadBudgetExhausted => {
                headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorCode::UploadBudgetExhausted,
                )
            }
            Self::BodyTooSlow => (StatusCode::REQUEST_TIMEOUT, ErrorCode::BodyTooSlow),
            Self::BodyTooLarge { limit } => {
                let typed = SizeLimit::Request(limit);
                typed.render(|name, value| {
                    if let Ok(value) = HeaderValue::from_str(value) {
                        headers.insert(name, value);
                    }
                });
                // The node's `413` names its limit the same way.
                let message =
                    format!("the request body is above the request limit of {limit} bytes");
                return error_answer(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    ErrorBody::new(typed.error_code(), message),
                    headers,
                );
            }
            Self::BodyUnreadable => (StatusCode::BAD_REQUEST, ErrorCode::BodyUnreadable),
            Self::NoUpstream => (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::NoUpstream),
            Self::Embedder(refusal) => return refusal.into_response(),
        };
        error_answer(status, ErrorBody::fixed(code), headers)
    }
}

/// The one renderer of the gateway's own error answers: `status`, the
/// headers every Ursula error answer carries, `headers`, and the JSON error
/// body (extensions Section 11). Its messages are the codes' fixed ones,
/// which name no internal address, request body or credential, and match
/// the node's for a code both send.
fn error_answer(status: StatusCode, body: ErrorBody, headers: HeaderMap) -> AxumResponse {
    let mut all = HeaderMap::new();
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("cross-origin-resource-policy", "cross-origin"),
        ("cache-control", "no-store"),
        ("content-type", ErrorBody::CONTENT_TYPE),
    ] {
        all.insert(name, HeaderValue::from_static(value));
    }
    all.extend(headers);
    (status, all, body.render()).into_response()
}

/// [`error_answer`] with `Retry-After: 1` and the code's fixed message.
fn retry_answer(status: StatusCode, code: ErrorCode) -> AxumResponse {
    let mut headers = HeaderMap::new();
    headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
    error_answer(status, ErrorBody::fixed(code), headers)
}

/// Replaces `Ursula-Max-Request-Bytes` on a forwarded response with the
/// smaller of the upstream's value and `limit`, the gateway's own limit for
/// the request (extensions Section 10). A response without the header gets
/// none. A malformed one gets `limit`, the most the gateway knows.
fn lower_request_limit(headers: &mut HeaderMap, limit: u64) {
    let lowered = lowered_request_limit(
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
        limit,
    );
    let lowered = match lowered {
        Ok(None) => return,
        Ok(Some(lowered)) => lowered,
        Err(error) => {
            debug!(%error, "an upstream sent a malformed request limit");
            limit
        }
    };
    headers.insert(names::URSULA_MAX_REQUEST_BYTES, HeaderValue::from(lowered));
}

/// How a [`BodyReader`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadEnd {
    /// The body ended: the connection can carry the next request.
    Whole,
    /// The body is longer than the limit: by its declared length, and then
    /// none of it was read ([`Gateway::refuse_unread`] only), or by the bytes
    /// read.
    OverLimit,
    /// No chunk of the body arrived for the pace's stall timeout.
    Stalled,
    /// The body was still arriving after the read timeout of a
    /// [`Pace::Bounded`] reading.
    TimedOut,
    /// Fewer bytes of the body than the floor of a [`Pace::Live`] reading
    /// arrived in one of its windows.
    TooSlow,
    /// The body failed, as when the client goes away in the middle of it.
    /// [`BodyReader::chunk`] logs the body's error.
    Failed,
}

/// Counts a refused request's reading in `cut` when it drops, unless the
/// reading read the body to its end.
struct CutUnlessWhole<'a> {
    cut: &'a AtomicU64,
    whole: bool,
}

impl Drop for CutUnlessWhole<'_> {
    fn drop(&mut self) {
        if !self.whole {
            self.cut.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// How long a [`BodyReader`] goes on reading a body.
#[derive(Clone, Copy, Debug)]
enum Pace {
    /// The body of a request refused from its head ([`REFUSED_PACE`]): until
    /// no chunk arrived for `stall_timeout`, and for `read_timeout` in all.
    Bounded {
        stall_timeout: Duration,
        read_timeout: Duration,
    },
    /// The body of a request the gateway may forward
    /// ([`Gateway::forwarded_pace`]): until no chunk arrived for
    /// `stall_timeout`, or until fewer than `min_window_bytes` arrived in one
    /// window of `stall_timeout`, the windows counted from the start of the
    /// reading. Neither depends on the body's size or on its average rate
    /// since the start, and nothing bounds the reading in all: a body that
    /// keeps arriving above the floor is never cut, however long it takes.
    Live {
        stall_timeout: Duration,
        min_window_bytes: u64,
    },
}

impl Pace {
    fn stall_timeout(self) -> Duration {
        match self {
            Self::Bounded { stall_timeout, .. } | Self::Live { stall_timeout, .. } => stall_timeout,
        }
    }
}

/// The timer that paces a body read: madsim's under `cfg(madsim)`, so that
/// a simulated server drains a refused body in virtual time.
mod clock {
    #[cfg(madsim)]
    pub(super) use madsim::time::timeout;
    #[cfg(not(madsim))]
    pub(super) use tokio::time::timeout;
}

/// `after` past `at`, or a year past it at most: a reading is cut by then
/// whatever the configuration.
fn later(at: Instant, after: Duration) -> Instant {
    const HORIZON: Duration = Duration::from_secs(31_536_000);
    at.checked_add(after.min(HORIZON)).unwrap_or(at)
}

/// Reads a request's body chunk by chunk: `limit` bytes at most, the limit
/// `max_request_body_bytes` sets, for as long as its [`Pace`] allows.
struct BodyReader {
    body: http_body_util::Limited<Body>,
    pace: Pace,
    started: Instant,
    last_chunk: Instant,
    /// The start of the [`Pace::Live`] window under way.
    window_start: Instant,
    /// The bytes that arrived in that window.
    window_bytes: u64,
}

impl BodyReader {
    fn new(body: Body, limit: usize, pace: Pace) -> Self {
        let started = Instant::now();
        Self {
            body: http_body_util::Limited::new(body, limit),
            pace,
            started,
            last_chunk: started,
            window_start: started,
            window_bytes: 0,
        }
    }

    /// Closes the [`Pace::Live`] windows that ended by `now`: each must have
    /// had its floor of bytes.
    fn close_windows(&mut self, now: Instant) -> Result<(), ReadEnd> {
        let Pace::Live {
            stall_timeout,
            min_window_bytes,
        } = self.pace
        else {
            return Ok(());
        };
        loop {
            let window_end = later(self.window_start, stall_timeout);
            if now < window_end {
                return Ok(());
            }
            if self.window_bytes < min_window_bytes {
                return Err(ReadEnd::TooSlow);
            }
            self.window_start = window_end;
            self.window_bytes = 0;
        }
    }

    /// When the reading is cut unless a chunk arrives first, and how it then
    /// ends.
    fn deadline(&self) -> (Instant, ReadEnd) {
        let stalled = (
            later(self.last_chunk, self.pace.stall_timeout()),
            ReadEnd::Stalled,
        );
        let other = match self.pace {
            Pace::Bounded { read_timeout, .. } => {
                (later(self.started, read_timeout), ReadEnd::TimedOut)
            }
            // A window that has its floor ends after the last chunk's stall
            // timeout: the next window starts with it, and is checked once a
            // chunk arrives in it, or the stall cuts the reading first.
            Pace::Live {
                stall_timeout,
                min_window_bytes,
            } if self.window_bytes < min_window_bytes => {
                (later(self.window_start, stall_timeout), ReadEnd::TooSlow)
            }
            Pace::Live { .. } => return stalled,
        };
        if stalled.0 <= other.0 { stalled } else { other }
    }

    /// The next data chunk, `Ok(None)` at the body's end, or how the reading
    /// ended short of it.
    async fn chunk(&mut self) -> Result<Option<bytes::Bytes>, ReadEnd> {
        loop {
            self.close_windows(Instant::now())?;
            let (deadline, cut) = self.deadline();
            let left = deadline.saturating_duration_since(Instant::now());
            match clock::timeout(left, self.body.frame()).await {
                Err(_elapsed) => return Err(cut),
                Ok(None) => return Ok(None),
                Ok(Some(Ok(frame))) => {
                    let now = Instant::now();
                    self.close_windows(now)?;
                    self.last_chunk = now;
                    if let Ok(chunk) = frame.into_data() {
                        self.window_bytes = self.window_bytes.saturating_add(chunk.len() as u64);
                        return Ok(Some(chunk));
                    }
                }
                Ok(Some(Err(error))) if error.is::<http_body_util::LengthLimitError>() => {
                    return Err(ReadEnd::OverLimit);
                }
                Ok(Some(Err(error))) => {
                    debug!(%error, "a request body failed");
                    return Err(ReadEnd::Failed);
                }
            }
        }
    }

    /// Reads the rest of the body to its end, dropping each chunk as it
    /// arrives and counting its bytes in `read`.
    async fn discard(mut self, read: &AtomicU64) -> ReadEnd {
        loop {
            match self.chunk().await {
                Ok(Some(dropped)) => {
                    read.fetch_add(dropped.len() as u64, Ordering::Relaxed);
                }
                Ok(None) => return ReadEnd::Whole,
                Err(end) => return end,
            }
        }
    }
}

/// Reads `body` to its end and drops each chunk as it arrives, as the
/// gateway reads the body of a request it refused from its head: `limit`
/// bytes at most, at [`REFUSED_PACE`]. Reads none of a body whose declared
/// length is over `limit`. A node uses it for the unread body of a write it
/// answered from the head alone on HTTP/1.1.
pub async fn drain(body: Body, limit: usize) -> ReadEnd {
    if body.size_hint().lower() > u64::try_from(limit).unwrap_or(u64::MAX) {
        return ReadEnd::OverLimit;
    }
    BodyReader::new(body, limit, REFUSED_PACE)
        .discard(&AtomicU64::new(0))
        .await
}

/// The body of a request the gateway forwards whole, as it arrives
/// ([`Gateway::buffer`]): one buffer that never holds more than the body's
/// share of the budget.
struct BodyBuffer {
    bytes: Vec<u8>,
}

impl BodyBuffer {
    /// A buffer of `capacity` bytes: a declared body's share of the budget,
    /// or nothing for a body without a declared length.
    fn new(capacity: u64) -> Self {
        Self {
            bytes: Vec::with_capacity(usize::try_from(capacity).unwrap_or(0)),
        }
    }

    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn capacity(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    /// Copies `chunk` into the buffer, growing it, when it must grow, to
    /// twice its size at most and never past `share`, the bytes the body's
    /// share of the budget covers, which holds the chunk.
    fn push(&mut self, chunk: &[u8], share: u64) {
        let needed = self.len().saturating_add(chunk.len() as u64);
        if needed > self.capacity() {
            let grown = self.capacity().saturating_mul(2).max(needed).min(share);
            let more = usize::try_from(grown.saturating_sub(self.len())).unwrap_or(usize::MAX);
            self.bytes.reserve_exact(more);
        }
        self.bytes.extend_from_slice(chunk);
    }

    /// The body, after `permit` has shrunk to the buffer's capacity, the
    /// memory the body holds until it is forwarded.
    fn finish(self, permit: &mut BodyPermit) -> bytes::Bytes {
        permit.settle(self.capacity());
        bytes::Bytes::from(self.bytes)
    }
}

/// Rides the response body: counts egress bytes and, on drop, records the
/// request's usage and releases any live-read slot. Dropping happens whether
/// the body completes, the client disconnects, or the response is discarded.
#[derive(Default)]
struct ResponseTail {
    meter: Option<EgressMeter>,
    _live_guard: Option<LiveReadGuard>,
    /// Unix seconds at which the credential that authorized this request stops
    /// being valid. Only consulted for responses that stream indefinitely.
    credential_expires_at: Option<u64>,
}

struct EgressMeter {
    collector: Arc<UsageCollector>,
    key: UsageKey,
    request_bytes: u64,
    response_bytes: u64,
    chunks: u64,
}

impl Drop for EgressMeter {
    fn drop(&mut self) {
        self.collector.record(
            self.key.clone(),
            1,
            self.request_bytes,
            self.response_bytes,
            self.chunks,
        );
    }
}

/// Terminal SSE frame for a subscription cut short by credential expiry.
///
/// Named rather than a bare stream end: a client cannot otherwise tell "the
/// stream is over" from "your credential ran out", and the correct response
/// differs — the second one means re-authenticate and resume from the last
/// `Stream-Next-Offset`.
const CREDENTIAL_EXPIRED_EVENT: &[u8] =
    b"event: credential-expired\ndata: {\"reason\":\"credential_expired\"}\n\n";

/// Deadline state for a stream that must not outlive its credential.
struct CredentialDeadline {
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
    terminal_emitted: bool,
}

struct CountedStream<S> {
    inner: S,
    tail: ResponseTail,
    /// Armed only for responses that stream indefinitely, so a normal read is
    /// never truncated by a credential that expires while it is in flight.
    deadline: Option<CredentialDeadline>,
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
        if let Some(deadline) = &mut this.deadline {
            if deadline.terminal_emitted {
                return std::task::Poll::Ready(None);
            }
            if deadline.sleep.as_mut().poll(cx).is_ready() {
                // Emit the terminal frame and stop reading upstream: forwarding
                // further data would be serving a request whose authorization
                // has lapsed.
                deadline.terminal_emitted = true;
                return std::task::Poll::Ready(Some(Ok(bytes::Bytes::from_static(
                    CREDENTIAL_EXPIRED_EVENT,
                ))));
            }
        }
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

    if segments.len() == 1 && *method == Method::PUT {
        return Some(ClassifiedRequest {
            resource: Resource {
                bucket_id,
                stream_id: None,
            },
            action: Action::AdministerBucket,
        });
    }

    let stream_id = segments.get(1)?.clone();
    let suffix = segments.get(2..).unwrap_or_default();

    let action = match suffix {
        [] if *method == Method::PUT => {
            if closes_stream(headers) {
                Action::CreateAndClose
            } else {
                Action::Create
            }
        }
        [] if *method == Method::POST => {
            if closes_stream(headers) {
                Action::AppendAndClose
            } else {
                Action::Append
            }
        }
        [] if *method == Method::GET => {
            // Any `live` mode (sse, long-poll) holds a server-side waiter, so
            // both classify as Tail for authorization and live-read quotas.
            if query_has_live_mode(uri) {
                Action::Tail
            } else {
                Action::Read
            }
        }
        [] if *method == Method::HEAD => Action::Head,
        [] if *method == Method::DELETE => Action::Delete,
        [suffix] if suffix == "bootstrap" && *method == Method::GET => Action::Read,
        // The bare path answers 405 upstream (the removed latest-snapshot
        // redirect), not a 404 that Loro's client reads as "no snapshot".
        [suffix] if suffix == "snapshot" && *method == Method::GET => Action::ReadSnapshot,
        [suffix, _offset] if suffix == "snapshot" && *method == Method::PUT => {
            Action::PublishSnapshot
        }
        [suffix, _offset] if suffix == "snapshot" && *method == Method::GET => Action::ReadSnapshot,
        // Discarding a stream's staged parts undoes part uploads (#622).
        [suffix, parts]
            if suffix == "snapshot" && parts == "parts" && *method == Method::DELETE =>
        {
            Action::PublishSnapshot
        }
        // A snapshot part stores bytes for a later publish (#581 section 3.1).
        [suffix, _offset, parts]
            if suffix == "snapshot" && parts == "parts" && *method == Method::POST =>
        {
            Action::PublishSnapshot
        }
        [suffix, _offset] if suffix == "retention" && *method == Method::PUT => Action::Update,
        // The immutable resources of one incarnation (extensions Section 4):
        // a chunk of the stream is a read, a snapshot of it a snapshot read,
        // by `GET` or `HEAD`.
        [incarnation, _token, kind, _offset]
            if incarnation == immutable::INCARNATION_SEGMENT
                && matches!(*method, Method::GET | Method::HEAD) =>
        {
            match kind.as_str() {
                immutable::CHUNK_SEGMENT => Action::Read,
                immutable::SNAPSHOT_SEGMENT => Action::ReadSnapshot,
                _ => return None,
            }
        }
        _ => return None,
    };

    Some(ClassifiedRequest {
        resource: Resource {
            bucket_id,
            stream_id: Some(stream_id),
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

fn authentication_refusal(error: AuthenticationError) -> Refusal {
    if error == AuthenticationError::Unavailable {
        error!(error = %error, "gateway authentication failed");
    }
    Refusal::Authentication(error)
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
    /// No connection to the upstream picked for the request: the request was
    /// never sent, so no node received it.
    #[error("upstream {upstream} is unreachable")]
    UpstreamUnreachable {
        upstream: String,
        #[source]
        source: reqwest::Error,
    },
    /// No connection to the upstream of a hop after a Raft redirect. The
    /// request was never sent there.
    #[error("upstream {upstream} is unreachable after a redirect")]
    LeaderUnreachable {
        upstream: String,
        #[source]
        source: reqwest::Error,
    },
    /// Every upstream that answered the request redirected it, and the
    /// gateway stopped following: after [`MAX_LEADER_REDIRECTS`] hops, after
    /// [`LEADER_REDIRECT_DEADLINE`], or with nowhere left to go. `followed`
    /// counts the hops after the first, search hops included.
    #[error("no leader found after {followed} further hops")]
    LeaderNotFound { followed: usize },
    /// No response headers in time. The request may have reached the
    /// upstream.
    #[error("upstream {upstream} sent no response headers within {timeout:?}")]
    UpstreamTimeout { upstream: String, timeout: Duration },
    /// The exchange failed after the request may have reached the upstream.
    #[error("upstream request to {upstream} failed")]
    Upstream {
        upstream: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to build response: {0}")]
    ResponseBuild(String),
}

impl GatewayError {
    /// Whether the upstream failed at the transport, or answered nothing
    /// within the response header timeout, so selection should leave it out
    /// for a while (#454). A node answers a write it cannot finish within its
    /// leader lease, so a missing answer marks a node that does not answer at
    /// all, such as a stopped process. A live node whose request was only
    /// slow comes back with its next answer, a redirect's included. A request
    /// that could not be built never left the gateway.
    fn ejects_upstream(&self) -> bool {
        match self {
            Self::UpstreamUnreachable { .. }
            | Self::LeaderUnreachable { .. }
            | Self::UpstreamTimeout { .. } => true,
            Self::Upstream { source, .. } => !source.is_builder(),
            Self::LeaderNotFound { .. } | Self::ResponseBuild(_) => false,
        }
    }

    /// The client's answer. An unreachable upstream is answered `503` with
    /// `Retry-After`, as Ursula answers while a group has no reachable
    /// leader: the request was never sent there. A request whose redirects
    /// the gateway stopped following is answered the same way. The other
    /// failures may follow a request the upstream received. The bodies name
    /// no internal address.
    fn into_response(self) -> AxumResponse {
        match self {
            Self::UpstreamUnreachable { .. } | Self::LeaderUnreachable { .. } => {
                unreachable_response()
            }
            Self::LeaderNotFound { .. } => {
                retry_answer(StatusCode::SERVICE_UNAVAILABLE, ErrorCode::NoLeader)
            }
            Self::UpstreamTimeout { .. } => error_answer(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorBody::fixed(ErrorCode::UpstreamTimeout),
                HeaderMap::new(),
            ),
            Self::Upstream { .. } | Self::ResponseBuild(_) => error_answer(
                StatusCode::BAD_GATEWAY,
                ErrorBody::fixed(ErrorCode::UpstreamFailed),
                HeaderMap::new(),
            ),
        }
    }
}

fn unreachable_response() -> AxumResponse {
    retry_answer(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::UpstreamUnreachable,
    )
}

#[cfg(test)]
mod tests;
