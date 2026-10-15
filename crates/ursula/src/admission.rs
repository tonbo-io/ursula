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

use crate::CLUSTER_PROBE_PATH;
use crate::MAX_HTTP_BODY_BYTES;
use crate::cold_snapshot;
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
}

impl Default for IngressAdmission {
    fn default() -> Self {
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_HTTP_INFLIGHT_BODY_BYTES,
            )),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
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
        }
    }

    pub fn disabled() -> Self {
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(usize::MAX)),
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
enum AdmissionRefusal {
    /// The body is larger than admission admits on the route (`413`).
    BodyTooLarge,
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
            Self::BodyTooLarge => "RequestBodyTooLarge",
            Self::WalDiskPressure => "WalDiskPressure",
            Self::RaftLogPressure => "RaftLogPressure",
            Self::IngressBodyBytesLimitReached => "IngressBodyBytesLimitReached",
        }
    }
}

impl IntoResponse for AdmissionRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::BodyTooLarge => {
                (StatusCode::PAYLOAD_TOO_LARGE, "request body is too large").into_response()
            }
            Self::WalDiskPressure | Self::RaftLogPressure | Self::IngressBodyBytesLimitReached => {
                retry_after_json(self.code())
            }
        }
    }
}

pub(crate) async fn ingress_admission_middleware(
    State(admission): State<IngressAdmission>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.uri().path() == CLUSTER_PROBE_PATH {
        return next.run(request).await;
    }
    let Some(body_bytes) = request_write_body_bytes(&request) else {
        return next.run(request).await;
    };
    if body_bytes > cold_snapshot::max_admitted_body_bytes(request.method(), request.uri()) {
        return AdmissionRefusal::BodyTooLarge.into_response();
    }
    if admission.wal_disk.is_pressured() {
        return AdmissionRefusal::WalDiskPressure.into_response();
    }
    // Every write that carries a body grows the Raft log; bodiless ones
    // (retention advances, deletes) only shrink state.
    if body_bytes > 0
        && admission
            .raft_log
            .as_ref()
            .is_some_and(ursula_raft::SnapshotBuildCoordinator::log_pressured)
    {
        return AdmissionRefusal::RaftLogPressure.into_response();
    }
    // A snapshot body above the inline cap streams to the cold store in
    // bounded parts (F16), so it holds at most the inline cap in memory.
    let body_bytes =
        body_bytes.min(u64::try_from(MAX_HTTP_BODY_BYTES).expect("max body bytes fits u64"));

    let _body_permits = if body_bytes > 0 {
        let Ok(permits) = u32::try_from(body_bytes) else {
            return AdmissionRefusal::BodyTooLarge.into_response();
        };
        match admission.body_bytes.clone().try_acquire_many_owned(permits) {
            Ok(permit) => Some(permit),
            Err(_no_permits) => {
                return AdmissionRefusal::IngressBodyBytesLimitReached.into_response();
            }
        }
    } else {
        None
    };

    next.run(request).await
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
