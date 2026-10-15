//! Client-plane ingress admission: refuses a write before it reaches a
//! handler, so nothing of a refused request is ever proposed. A response
//! never says so: a client resolves an unknown outcome by repeating a keyed
//! write (#581 section 2).

use std::sync::Arc;

use axum::body::Body;
use axum::body::HttpBody;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_headers::SizeLimit;

use crate::CLUSTER_PROBE_PATH;
use crate::MAX_HTTP_BODY_BYTES;
use crate::front_door;
use crate::limits;
use crate::render::insert_default_response_headers;
use crate::wal_disk::WalDiskMonitor;

const DEFAULT_HTTP_INFLIGHT_BODY_BYTES: usize = MAX_HTTP_BODY_BYTES * 8;

/// Client-plane ingress admission. The primary control is a process-wide
/// in-flight write-body byte budget: a request must reserve its body bytes
/// before axum drains the body into `Bytes`, and the reservation lives until
/// the response is produced. This is the layer that turns memory pressure into
/// backpressure at the HTTP edge.
///
/// Configurable via `ServerConfig.http_inflight_body_size`.
#[derive(Clone)]
pub struct IngressAdmission {
    body_bytes: Arc<tokio::sync::Semaphore>,
    wal_disk: WalDiskMonitor,
    /// The node's Raft log-pressure flag (set by the snapshot driver's
    /// monitor while the unsnapshotted log is over its hard limit).
    raft_log: Option<ursula_raft::SnapshotBuildCoordinator>,
    /// Whether the node stages snapshot bodies in a cold store, which sets
    /// the body limit of a snapshot publish.
    cold_snapshots: bool,
}

impl Default for IngressAdmission {
    fn default() -> Self {
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_HTTP_INFLIGHT_BODY_BYTES,
            )),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
            cold_snapshots: false,
        }
    }
}

impl IngressAdmission {
    pub fn new(cfg: &ursula_config::ServerConfig) -> Self {
        let body_budget = cfg.http_inflight_body_size.as_bytes() as usize;
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(body_budget)),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
            cold_snapshots: false,
        }
    }

    pub fn disabled() -> Self {
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(usize::MAX)),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
            cold_snapshots: false,
        }
    }

    /// Sets whether the node stages snapshot bodies in a cold store.
    pub(crate) fn with_cold_snapshots(mut self, cold_snapshots: bool) -> Self {
        self.cold_snapshots = cold_snapshots;
        self
    }

    pub(crate) fn with_wal_disk_monitor(mut self, monitor: WalDiskMonitor) -> Self {
        self.wal_disk = monitor;
        self
    }

    /// Refuse client writes while the node's Raft log is over its hard limit.
    pub fn with_raft_log_pressure(
        mut self,
        coordinator: Option<ursula_raft::SnapshotBuildCoordinator>,
    ) -> Self {
        self.raft_log = coordinator;
        self
    }
}

/// Why ingress admission refused a write. Admission answers before the request
/// reaches a handler, so nothing of a refused request was proposed or applied.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AdmissionRefusal {
    /// The body is larger than admission admits on the route (`413` naming
    /// the limit).
    BodyTooLarge(SizeLimit),
    /// The WAL filesystem is below its free-space floor, or its free-space
    /// check failed (`503`).
    WalDiskPressure,
    /// The node's unsnapshotted Raft log is over its hard limit (`503`).
    RaftLogPressure,
    /// The process-wide in-flight write-body budget is spent (`503`).
    IngressBodyBytesLimitReached,
}

impl AdmissionRefusal {
    /// The refusal's token: the JSON `error` of a `503`.
    fn code(self) -> &'static str {
        match self {
            Self::BodyTooLarge(_) => "RequestBodyTooLarge",
            Self::WalDiskPressure => "WalDiskPressure",
            Self::RaftLogPressure => "RaftLogPressure",
            Self::IngressBodyBytesLimitReached => "IngressBodyBytesLimitReached",
        }
    }
}

impl IntoResponse for AdmissionRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::BodyTooLarge(limit) => limits::size_refusal(limit),
            Self::WalDiskPressure | Self::RaftLogPressure | Self::IngressBodyBytesLimitReached => {
                retry_after_json(self.code())
            }
        }
    }
}

pub(crate) async fn ingress_admission_middleware(
    State(admission): State<IngressAdmission>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if request.uri().path() == CLUSTER_PROBE_PATH {
        return next.run(request).await;
    }
    let Some(body_bytes) = request_write_body_bytes(&request) else {
        return next.run(request).await;
    };
    // Size first: the head alone decides it (#581 section 6.1).
    let body_write = front_door::BodyWrite::of(
        request.method(),
        request.extensions().get::<axum::extract::MatchedPath>(),
    );
    let limit = body_write.map_or(limits::REQUEST_LIMIT, |write| {
        write.limit(admission.cold_snapshots)
    });
    if body_bytes > limit.bytes() {
        return AdmissionRefusal::BodyTooLarge(limit).into_response();
    }
    // A write routed before its body meets pressure after routing and its
    // early answer: a follower redirects rather than refusing for its own
    // pressure, and a repeat of an applied write adds nothing to any log.
    if body_write.is_some() {
        request.extensions_mut().insert(DeferredAdmission {
            admission,
            body_bytes,
        });
        return next.run(request).await;
    }
    let _permit = match admission.admit(body_bytes) {
        Ok(permit) => permit,
        Err(refusal) => return refusal.into_response(),
    };
    next.run(request).await
}

impl IngressAdmission {
    /// The pressure checks of a write whose body declares `body_bytes`,
    /// and its share of the in-flight body budget.
    fn admit(&self, body_bytes: u64) -> Result<AdmissionPermit, AdmissionRefusal> {
        if self.wal_disk.is_pressured() {
            return Err(AdmissionRefusal::WalDiskPressure);
        }
        // Every write that carries a body grows the Raft log; bodiless ones
        // (retention advances, deletes) only shrink state.
        if body_bytes > 0
            && self
                .raft_log
                .as_ref()
                .is_some_and(ursula_raft::SnapshotBuildCoordinator::log_pressured)
        {
            return Err(AdmissionRefusal::RaftLogPressure);
        }
        // A snapshot body above the inline cap streams to the cold store in
        // bounded parts (F16), so it holds at most the inline cap in memory.
        let body_bytes = body_bytes.min(limits::MAX_REQUEST_BYTES);
        if body_bytes == 0 {
            return Ok(AdmissionPermit { _held: None });
        }
        // At most the request limit, which fits.
        let permits = u32::try_from(body_bytes).unwrap_or(u32::MAX);
        match self.body_bytes.clone().try_acquire_many_owned(permits) {
            Ok(permit) => Ok(AdmissionPermit {
                _held: Some(permit),
            }),
            Err(_no_permits) => Err(AdmissionRefusal::IngressBodyBytesLimitReached),
        }
    }
}

/// A write's share of the in-flight body budget, held until its answer.
#[derive(Debug)]
pub(crate) struct AdmissionPermit {
    _held: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// The pressure checks of a write routed before its body, which its handler
/// runs once routing and early answers have passed (#581 section 6.1). The
/// middleware has checked its size already.
#[derive(Clone)]
pub(crate) struct DeferredAdmission {
    admission: IngressAdmission,
    body_bytes: u64,
}

/// The [`DeferredAdmission`] the middleware left for a handler, taken out of
/// the request rather than cloned. `None` when admission does not run.
pub(crate) struct PendingAdmission(pub(crate) Option<DeferredAdmission>);

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for PendingAdmission {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(parts.extensions.remove::<DeferredAdmission>()))
    }
}

impl DeferredAdmission {
    /// Runs the pressure checks: a refusal answers a retryable `503`,
    /// otherwise the permit is held until the answer.
    pub(crate) fn admit(self) -> Result<AdmissionPermit, AdmissionRefusal> {
        self.admission.admit(self.body_bytes)
    }
}

fn request_write_body_bytes(request: &Request<Body>) -> Option<u64> {
    if !is_write_method(request.method()) {
        return None;
    }
    if let Some(content_length) = request.headers().get(CONTENT_LENGTH)
        && let Ok(content_length) = content_length.to_str()
        && let Ok(parsed) = content_length.parse::<u64>()
    {
        return Some(parsed);
    }
    let size_hint = request.body().size_hint();
    size_hint.exact().or_else(|| size_hint.upper()).or(Some(
        u64::try_from(MAX_HTTP_BODY_BYTES).expect("max body bytes fits u64"),
    ))
}

fn is_write_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn retry_after_json(error: &'static str) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (
        StatusCode::SERVICE_UNAVAILABLE,
        headers,
        serde_json::json!({ "error": error }).to_string(),
    )
        .into_response()
}
