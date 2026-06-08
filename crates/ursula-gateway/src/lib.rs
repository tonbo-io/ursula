//! Ursula Gateway core.
//!
//! Routes client HTTP requests to upstream Ursula nodes. Redirect responses stay
//! visible to clients, but upstream node addresses are stripped from Location so
//! redirect-following clients keep talking to the gateway.

use std::net::SocketAddr;
use std::time::Duration;

use axum::body::Body;
use axum::http::header::{CONNECTION, LOCATION};
use axum::http::{HeaderMap, HeaderValue, Request, Response, StatusCode, Uri};
use axum::response::{IntoResponse, Response as AxumResponse};
use rand::prelude::IndexedRandom;
use tracing::{debug, error};

// Ursula nodes attach this header only to Raft leadership redirects. Plain
// protocol redirects, such as latest-snapshot redirects, intentionally do not
// carry it and must remain visible to clients.
const HEADER_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";

/// Configuration for the gateway.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Address the gateway server binds to.
    pub listen: SocketAddr,
    /// Base URLs for Ursula HTTP nodes, for example `http://10.0.0.12:4437`.
    pub upstreams: Vec<String>,
    /// Timeout for receiving upstream response headers.
    ///
    /// The returned response body is intentionally not covered, so SSE/live
    /// reads can stay open after headers arrive.
    pub response_header_timeout: Duration,
    /// TCP connect timeout for each upstream request attempt.
    pub connect_timeout: Duration,
}

/// Gateway service shared by each request handler.
#[derive(Clone)]
pub struct Gateway {
    config: GatewayConfig,
    client: reqwest::Client,
    response_header_timeout: Duration,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("config", &self.config)
            .field("response_header_timeout", &self.response_header_timeout)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    /// Create a new gateway with the given configuration.
    pub fn new(config: GatewayConfig) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("static gateway reqwest client config should be valid");
        let response_header_timeout = config.response_header_timeout;
        Self {
            config,
            client,
            response_header_timeout,
        }
    }

    /// Handle one client request.
    pub async fn handle(&self, req: Request<Body>) -> AxumResponse {
        let upstream = match self.pick_upstream() {
            Some(u) => u,
            None => {
                error!("no upstreams configured");
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        };

        match self.forward(upstream, req).await {
            Ok(response) => response,
            Err(e) => {
                error!(error = %e, "gateway request failed");
                e.into_response()
            }
        }
    }

    fn pick_upstream(&self) -> Option<&str> {
        let mut rng = rand::rng();
        self.config.upstreams.choose(&mut rng).map(String::as_str)
    }

    /// Forward a request to the given upstream.
    ///
    /// The request body is streamed to the upstream and is never read by the
    /// gateway. Redirects are returned to the client with gateway-relative
    /// Locations so client-side `curl -L` style follow keeps using the gateway.
    async fn forward(
        &self,
        upstream: &str,
        req: Request<Body>,
    ) -> Result<Response<Body>, GatewayError> {
        let (parts, body) = req.into_parts();

        // Treat configured upstreams as base URLs and preserve the client's
        // original path/query verbatim.
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let target_url = format!("{}{}", upstream.trim_end_matches('/'), path_and_query);

        debug!(
            method = %parts.method,
            url = %target_url,
            "forwarding request"
        );

        // Timeout covers only the upstream request and response headers. Once
        // headers arrive, the body is streamed back to the client.
        let upstream_resp = match tokio::time::timeout(
            self.response_header_timeout,
            self.client
                .request(parts.method.clone(), target_url)
                .headers(copy_forwarded_headers(&parts.headers, true))
                .body(reqwest::Body::wrap_stream(body.into_data_stream()))
                .send(),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(GatewayError::Upstream(e.to_string())),
            Err(e) => {
                return Err(GatewayError::Upstream(format!(
                    "upstream response header timeout: {e}"
                )));
            }
        };

        let status = upstream_resp.status();
        let mut headers = copy_forwarded_headers(upstream_resp.headers(), false);

        if status == StatusCode::TEMPORARY_REDIRECT {
            // The client should follow redirects through the gateway, not by
            // connecting to the internal Ursula node named by the upstream.
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
            .body(Body::from_stream(upstream_resp.bytes_stream()))
            .map_err(|e| GatewayError::ResponseBuild(e.to_string()))
    }
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

/// Errors that can occur while the gateway forwards a request.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// The upstream request failed.
    #[error("upstream request failed: {0}")]
    Upstream(String),
    /// Failed to build the response.
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
mod tests {
    use super::*;

    #[test]
    fn header_forwarding_applies_proxy_rules() {
        let mut request_headers = HeaderMap::new();
        request_headers.insert("content-type", "text/plain".parse().unwrap());
        request_headers.insert("host", "example.com".parse().unwrap());
        request_headers.insert("connection", "x-remove".parse().unwrap());
        request_headers.insert("x-remove", "drop-me".parse().unwrap());

        let copied_request = copy_forwarded_headers(&request_headers, true);
        assert_eq!(copied_request.get("content-type").unwrap(), "text/plain");
        assert!(copied_request.get("host").is_none());
        assert!(copied_request.get("connection").is_none());
        assert!(copied_request.get("x-remove").is_none());

        let mut response_headers = HeaderMap::new();
        response_headers.insert("content-type", "text/plain".parse().unwrap());
        response_headers.insert("transfer-encoding", "chunked".parse().unwrap());
        response_headers.insert("x-custom", "value".parse().unwrap());
        response_headers.append("set-cookie", "a=1".parse().unwrap());
        response_headers.append("set-cookie", "b=2".parse().unwrap());

        let copied_response = copy_forwarded_headers(&response_headers, false);
        assert_eq!(copied_response.get("content-type").unwrap(), "text/plain");
        assert!(copied_response.get("transfer-encoding").is_none());
        assert_eq!(copied_response.get("x-custom").unwrap(), "value");

        let cookies = copied_response
            .get_all("set-cookie")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(cookies, vec!["a=1", "b=2"]);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::Arc;

    use axum::http::{StatusCode, header::CONTENT_LENGTH};
    use axum::{
        Router,
        routing::{any, get, put},
    };
    use http_body_util::BodyExt;
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};

    // Owns a mock upstream server for one test. Dropping it aborts the server
    // task so tests do not need repeated cleanup code.
    struct TestUpstream {
        url: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestUpstream {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    // Some tests need the bound URL while constructing the app.
    async fn spawn_upstream_with_url(app_for_url: impl FnOnce(String) -> Router) -> TestUpstream {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let addr = listener.local_addr().expect("upstream local addr");
        let url = format!("http://{addr}");
        let app = app_for_url(url.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve upstream");
        });

        TestUpstream { url, task }
    }

    // Start a plain mock Ursula HTTP node and return its base URL.
    async fn spawn_upstream(app: Router) -> TestUpstream {
        spawn_upstream_with_url(|_| app).await
    }

    // Start a leader plus a follower. The follower returns Ursula's internal
    // Raft 307 redirect marker pointing at the leader.
    async fn spawn_raft_redirect_upstreams(leader_app: Router) -> (TestUpstream, TestUpstream) {
        let leader = spawn_upstream(leader_app).await;
        let leader_url = format!("{}/bucket/stream", leader.url);
        let follower_app = Router::new().route(
            "/bucket/stream",
            any(move || {
                let leader_url = leader_url.clone();
                async move {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [
                            ("location", leader_url),
                            ("x-ursula-raft-leader-id", "1".to_owned()),
                        ],
                        "redirecting",
                    )
                }
            }),
        );
        let follower = spawn_upstream(follower_app).await;

        (leader, follower)
    }

    fn test_config(upstreams: Vec<String>) -> GatewayConfig {
        GatewayConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            upstreams,
            response_header_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(1),
        }
    }

    fn gateway_for_url(upstream_url: impl Into<String>) -> Arc<Gateway> {
        Arc::new(Gateway::new(test_config(vec![upstream_url.into()])))
    }

    fn gateway_with_response_header_timeout(
        upstream_url: impl Into<String>,
        response_header_timeout: Duration,
    ) -> Arc<Gateway> {
        let mut config = test_config(vec![upstream_url.into()]);
        config.response_header_timeout = response_header_timeout;
        Arc::new(Gateway::new(config))
    }

    #[tokio::test]
    async fn gateway_handle_returns_service_unavailable_without_upstreams() {
        let gateway = Gateway::new(test_config(Vec::new()));
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream")
            .body(Body::empty())
            .unwrap();

        let resp = gateway.handle(req).await;

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn gateway_returns_leader_redirect_without_upstream_host() {
        let leader_app = Router::new().route("/bucket/stream", get(|| async { StatusCode::OK }));
        let (_leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
        let gateway = gateway_for_url(follower.url.clone());

        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream")
            .header("authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();

        let resp = gateway.forward(&follower.url, req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(resp.headers().get(LOCATION).unwrap(), "/bucket/stream");
        assert!(resp.headers().get(HEADER_URSULA_RAFT_LEADER_ID).is_none());
    }

    #[tokio::test]
    async fn gateway_preserves_path_and_query_with_trailing_upstream_slash() {
        let app = Router::new().route(
            "/bucket/stream",
            get(|uri: Uri| async move { (StatusCode::OK, uri.to_string()) }),
        );
        let upstream = spawn_upstream(app).await;
        let upstream_url = format!("{}/", upstream.url);
        let gateway = gateway_for_url(upstream_url.clone());
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream?offset=now&live=sse")
            .body(Body::empty())
            .unwrap();

        let resp = gateway.forward(&upstream_url, req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body_bytes, "/bucket/stream?offset=now&live=sse");
    }

    #[tokio::test]
    async fn gateway_accepts_https_upstream_scheme() {
        let gateway = gateway_for_url("https://127.0.0.1:1");
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream")
            .body(Body::empty())
            .unwrap();

        let err = gateway
            .forward("https://127.0.0.1:1", req)
            .await
            .unwrap_err();

        assert!(
            !err.to_string().contains("only http is supported"),
            "gateway rejected https before attempting upstream connection: {err}"
        );
    }

    #[tokio::test]
    async fn gateway_does_not_apply_response_header_timeout_to_sse_body() {
        let app = Router::new().route(
            "/bucket/stream",
            get(|| async {
                let (tx, rx) = tokio::sync::mpsc::channel(2);
                tokio::spawn(async move {
                    tx.send(bytes::Bytes::from_static(b"event: data\ndata: first\n\n"))
                        .await
                        .expect("send first SSE event");
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    tx.send(bytes::Bytes::from_static(b"event: data\ndata: second\n\n"))
                        .await
                        .expect("send second SSE event");
                });

                let body_stream = ReceiverStream::new(rx).map(Ok::<_, Infallible>);
                (
                    StatusCode::OK,
                    [("content-type", "text/event-stream")],
                    Body::from_stream(body_stream),
                )
            }),
        );
        let upstream = spawn_upstream(app).await;
        let gateway =
            gateway_with_response_header_timeout(upstream.url.clone(), Duration::from_millis(50));
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream")
            .body(Body::empty())
            .expect("build request");

        let mut resp = gateway
            .forward(&upstream.url, req)
            .await
            .expect("forward SSE request");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").expect("content type"),
            "text/event-stream"
        );

        let first = resp
            .body_mut()
            .frame()
            .await
            .expect("first frame")
            .expect("first frame ok")
            .into_data()
            .expect("first frame is data");
        assert_eq!(
            first,
            bytes::Bytes::from_static(b"event: data\ndata: first\n\n")
        );

        let second = tokio::time::timeout(Duration::from_secs(1), resp.body_mut().frame())
            .await
            .expect("second frame before test timeout")
            .expect("second frame")
            .expect("second frame ok")
            .into_data()
            .expect("second frame is data");
        assert_eq!(
            second,
            bytes::Bytes::from_static(b"event: data\ndata: second\n\n")
        );
    }

    #[tokio::test]
    async fn gateway_does_not_wait_for_get_request_body_before_opening_sse() {
        let app = Router::new().route(
            "/bucket/stream",
            get(|| async { (StatusCode::OK, "read-opened") }),
        );
        let upstream = spawn_upstream(app).await;
        let gateway = gateway_for_url(upstream.url.clone());
        let (_tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
        let body_stream = ReceiverStream::new(rx).map(Ok::<_, Infallible>);
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream?offset=now&live=sse")
            .body(Body::from_stream(body_stream))
            .unwrap();

        let resp = tokio::time::timeout(
            Duration::from_millis(100),
            gateway.forward(&upstream.url, req),
        )
        .await
        .expect("GET/SSE should not wait for request body EOF")
        .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body_bytes, "read-opened");
    }

    #[tokio::test]
    async fn gateway_does_not_wait_for_put_request_body_before_forwarding() {
        let app = Router::new().route(
            "/bucket/stream",
            put(|| async { (StatusCode::ACCEPTED, "write-opened") }),
        );
        let upstream = spawn_upstream(app).await;
        let gateway = gateway_for_url(upstream.url.clone());
        let (_tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
        let body_stream = ReceiverStream::new(rx).map(Ok::<_, Infallible>);
        let req = Request::builder()
            .method("PUT")
            .uri("/bucket/stream")
            .body(Body::from_stream(body_stream))
            .unwrap();

        let resp = tokio::time::timeout(
            Duration::from_millis(100),
            gateway.forward(&upstream.url, req),
        )
        .await
        .expect("PUT should be forwarded without waiting for request body EOF")
        .unwrap();

        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body_bytes, "write-opened");
    }

    #[tokio::test]
    async fn gateway_returns_leader_redirect_for_body_request() {
        let leader_app =
            Router::new().route("/bucket/stream", put(|| async { StatusCode::CREATED }));
        let (_leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
        let gateway = gateway_for_url(follower.url.clone());
        let req = Request::builder()
            .method("PUT")
            .uri("/bucket/stream")
            .header(CONTENT_LENGTH, "7")
            .body(Body::from("payload"))
            .unwrap();

        let resp = gateway.forward(&follower.url, req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(resp.headers().get(LOCATION).unwrap(), "/bucket/stream");
    }

    #[tokio::test]
    async fn gateway_preserves_public_snapshot_redirect_without_upstream_host() {
        let app = Router::new()
            .route(
                "/bucket/stream/snapshot",
                get(|| async {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [(
                            LOCATION,
                            "http://internal-node:4437/bucket/stream/snapshot/00000000000000000003",
                        )],
                        "redirecting",
                    )
                }),
            )
            .route(
                "/bucket/stream/snapshot/00000000000000000003",
                get(|| async { (StatusCode::OK, "snapshot-body") }),
            );
        let upstream = spawn_upstream(app).await;
        let gateway = gateway_for_url(upstream.url.clone());
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream/snapshot")
            .body(Body::empty())
            .unwrap();

        let resp = gateway.forward(&upstream.url, req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            resp.headers().get(LOCATION).unwrap(),
            "/bucket/stream/snapshot/00000000000000000003"
        );
    }
}
