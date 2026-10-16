//! Client-plane ingress admission: refuses a write before it reaches a
//! handler, so nothing of a refused request is ever proposed. A response
//! never says so: a client resolves an unknown outcome by repeating a keyed
//! write (#581 section 2).

use std::convert::Infallible;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use axum::body::Body;
use axum::body::HttpBody;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::future::BoxFuture;
use tower::Layer;
use tower::Service;
use ursula_headers::ErrorCode;
use ursula_headers::SizeLimit;

use crate::MAX_HTTP_BODY_BYTES;
use crate::front_door;
use crate::limits;
use crate::render::fixed_error_answer;
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
    body_bytes: BodyBudget,
    wal_disk: WalDiskMonitor,
    /// The node's Raft log-pressure flag (set by the snapshot driver's
    /// monitor while the unsnapshotted log is over its hard limit).
    raft_log: Option<ursula_raft::SnapshotBuildCoordinator>,
}

impl Default for IngressAdmission {
    fn default() -> Self {
        Self {
            body_bytes: BodyBudget::new(DEFAULT_HTTP_INFLIGHT_BODY_BYTES),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
        }
    }
}

impl IngressAdmission {
    pub fn new(cfg: &ursula_config::ServerConfig) -> Self {
        let body_budget = cfg.http_inflight_body_size.as_bytes() as usize;
        Self {
            body_bytes: BodyBudget::new(body_budget),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
        }
    }

    pub fn disabled() -> Self {
        Self {
            body_bytes: BodyBudget::new(tokio::sync::Semaphore::MAX_PERMITS),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
        }
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
    /// The body is larger than the request limit (`413` naming it).
    BodyTooLarge(SizeLimit),
    /// The WAL filesystem is below its free-space floor, or its free-space
    /// check failed (`503`).
    WalDiskPressure,
    /// The node's unsnapshotted Raft log is over its hard limit (`503`).
    RaftLogPressure,
    /// The process-wide in-flight write-body budget is spent (`503`).
    IngressBodyBytesLimitReached,
}

impl IntoResponse for AdmissionRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::BodyTooLarge(limit) => limits::size_refusal(limit),
            Self::WalDiskPressure => retry_after_json(ErrorCode::WalDiskPressure),
            Self::RaftLogPressure => retry_after_json(ErrorCode::RaftLogPressure),
            Self::IngressBodyBytesLimitReached => {
                retry_after_json(ErrorCode::IngressBodyBytesLimitReached)
            }
        }
    }
}

/// Admission runs as a tower layer that calls the route it wraps directly:
/// a `from_fn` middleware would clone the route, and every layer below it,
/// for every request (#602).
impl<S> Layer<S> for IngressAdmission {
    type Service = Admitted<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Admitted {
            inner,
            admission: self.clone(),
        }
    }
}

/// The service of the [`IngressAdmission`] layer.
#[derive(Clone)]
pub struct Admitted<S> {
    inner: S,
    admission: IngressAdmission,
}

impl<S> Service<Request<Body>> for Admitted<S>
where
    S: Service<Request<Body>, Response = Response, Error = Infallible>,
    S::Future: Send + 'static,
{
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;
    type Response = Response;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let Some(body_bytes) = request_write_body_bytes(&request) else {
            return Box::pin(self.inner.call(request));
        };
        // Size first: the head alone decides it (#581 section 6.1). One
        // limit for every request (section 3.1).
        if body_bytes > limits::MAX_REQUEST_BYTES {
            let refusal = AdmissionRefusal::BodyTooLarge(limits::REQUEST_LIMIT).into_response();
            return Box::pin(std::future::ready(Ok(refusal)));
        }
        // A write routed before its body meets pressure after routing and
        // its early answer: a follower redirects rather than refusing for its
        // own pressure, and a repeat of an applied write adds nothing to any
        // log.
        if front_door::routed_before_body(
            request.method(),
            request.extensions().get::<axum::extract::MatchedPath>(),
        ) {
            request.extensions_mut().insert(DeferredAdmission {
                admission: self.admission.clone(),
                body_bytes,
            });
            return Box::pin(self.inner.call(request));
        }
        let permit = match self.admission.admit(body_bytes) {
            Ok(permit) => permit,
            Err(refusal) => return Box::pin(std::future::ready(Ok(refusal.into_response()))),
        };
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await;
            drop(permit);
            response
        })
    }
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
        self.reserve(body_bytes)
    }

    fn reserve(&self, bytes: u64) -> Result<AdmissionPermit, AdmissionRefusal> {
        self.body_bytes.reserve(bytes)
    }
}

/// The node's in-flight body budget (`server.http_inflight_body_size`):
/// the bytes of write bodies, and of a parts commit's copy buffers, that
/// the node holds at once.
#[derive(Clone)]
pub(crate) struct BodyBudget {
    permits: Arc<tokio::sync::Semaphore>,
    capacity: u64,
}

impl BodyBudget {
    fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity: u64::try_from(capacity).unwrap_or(u64::MAX),
        }
    }

    /// The whole budget.
    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    /// `bytes` of the budget, held until the permit drops, only while
    /// `free` more bytes stay free beside them: the check and the
    /// reservation are one acquisition.
    pub(crate) fn reserve_leaving(
        &self,
        bytes: u64,
        free: u64,
    ) -> Result<AdmissionPermit, AdmissionRefusal> {
        let mut held = self.reserve(bytes.saturating_add(free))?;
        let free = usize::try_from(free).unwrap_or(usize::MAX);
        if let Some(permit) = held._held.as_mut() {
            drop(permit.split(free));
        }
        Ok(held)
    }

    /// `bytes` of the budget, held until the permit drops.
    pub(crate) fn reserve(&self, bytes: u64) -> Result<AdmissionPermit, AdmissionRefusal> {
        if bytes == 0 {
            return Ok(AdmissionPermit { _held: None });
        }
        // A body of at most the request limit, or a commit's copy buffers,
        // fits.
        let permits = u32::try_from(bytes).unwrap_or(u32::MAX);
        match self.permits.clone().try_acquire_many_owned(permits) {
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
/// layer has checked its size already.
#[derive(Clone)]
pub(crate) struct DeferredAdmission {
    admission: IngressAdmission,
    body_bytes: u64,
}

/// The [`DeferredAdmission`] the layer left for a handler, taken out of
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

    /// Charges at most `bytes` for the body: a parts commit's manifest,
    /// which the handler reads no further, is never charged the request
    /// limit of a body without `Content-Length`.
    pub(crate) fn capped(mut self, bytes: u64) -> Self {
        self.body_bytes = self.body_bytes.min(bytes);
        self
    }

    /// The node's in-flight body budget, for a charge the handler makes
    /// later: a parts commit's copy buffers, once its copy slot is held.
    pub(crate) fn budget(&self) -> BodyBudget {
        self.admission.body_bytes.clone()
    }

    /// The in-flight budget alone, without the WAL and Raft-log pressure
    /// checks: a snapshot part touches neither until it is committed.
    pub(crate) fn admit_budget(self) -> Result<AdmissionPermit, AdmissionRefusal> {
        self.admission.reserve(self.body_bytes)
    }
}

fn request_write_body_bytes(request: &Request<Body>) -> Option<u64> {
    if !is_write_method(request.method()) {
        return None;
    }
    Some(declared_body_bytes(request.headers(), request.body()))
}

/// The bytes a request's body declares: its `Content-Length`, else its
/// body's size hint, else the request limit. The one reading of a body's
/// declared size, for admission and for a snapshot part's reservation.
pub(crate) fn declared_body_bytes(headers: &HeaderMap, body: &Body) -> u64 {
    if let Some(content_length) = headers.get(CONTENT_LENGTH)
        && let Ok(content_length) = content_length.to_str()
        && let Ok(parsed) = content_length.parse::<u64>()
    {
        return parsed;
    }
    let size_hint = body.size_hint();
    size_hint
        .exact()
        .or_else(|| size_hint.upper())
        .unwrap_or_else(|| u64::try_from(MAX_HTTP_BODY_BYTES).unwrap_or(u64::MAX))
}

fn is_write_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

/// A `503` with `Retry-After: 1` whose error body names `code`, with its
/// fixed message.
pub(crate) fn retry_after_json(code: ErrorCode) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    fixed_error_answer(StatusCode::SERVICE_UNAVAILABLE, code, headers)
}
