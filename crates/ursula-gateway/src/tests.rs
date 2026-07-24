use std::convert::Infallible;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::http::header::LOCATION;
use axum::routing::any;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use http_body_util::BodyExt;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use super::*;
use crate::auth::AccessControl;
use crate::auth::AuthorizationError;
use crate::auth::AuthorizationFuture;
use crate::auth::Authorizer;
use crate::auth::PrincipalResolver;
use crate::auth::PrincipalResolverFuture;
use crate::auth::VerifiedPrincipal;

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
        max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
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

#[derive(Debug)]
struct FixedPrincipalResolver {
    calls: AtomicUsize,
    result: Result<VerifiedPrincipal, AuthenticationError>,
}

impl FixedPrincipalResolver {
    fn valid() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            result: Ok(VerifiedPrincipal {
                issuer: "https://issuer.example".to_owned(),
                subject: "user-1".to_owned(),
                client_id: "client-1".to_owned(),
                scopes: auth::parse_scope("streams:read streams:write"),
                issued_at: 1,
                expires_at: u64::MAX,
                token_id: "token-1".to_owned(),
            }),
        }
    }
}

impl PrincipalResolver for FixedPrincipalResolver {
    fn resolve<'a>(&'a self, bearer_token: &'a str) -> PrincipalResolverFuture<'a> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = if bearer_token == "valid-token" {
            self.result.clone()
        } else {
            Err(AuthenticationError::InvalidCredential)
        };
        Box::pin(async move { result })
    }
}

#[derive(Debug)]
struct RecordingAuthorizer {
    decision: Result<AuthorizationDecision, AuthorizationError>,
    requests: Mutex<Vec<AuthorizationRequest>>,
}

impl RecordingAuthorizer {
    fn new(decision: AuthorizationDecision) -> Self {
        Self {
            decision: Ok(decision),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl Authorizer for RecordingAuthorizer {
    fn authorize<'a>(&'a self, request: AuthorizationRequest) -> AuthorizationFuture<'a> {
        self.requests
            .lock()
            .expect("authorization request lock")
            .push(request);
        let decision = self.decision.clone();
        Box::pin(async move { decision })
    }
}

fn gateway_with_access_control(
    upstream_url: impl Into<String>,
    resolver: Arc<FixedPrincipalResolver>,
    authorizer: Arc<RecordingAuthorizer>,
) -> Gateway {
    Gateway::with_access_control(
        test_config(vec![upstream_url.into()]),
        AccessControl::new(resolver, authorizer),
    )
}

#[test]
fn request_classifier_maps_durable_stream_routes_to_bucket_resources() {
    let cases = [
        ("PUT", "/owner-a", Action::AdministerBucket, None),
        ("PUT", "/owner-a/orders", Action::Create, Some("orders")),
        ("POST", "/owner-a/orders", Action::Append, Some("orders")),
        ("GET", "/owner-a/orders", Action::Read, Some("orders")),
        (
            "GET",
            "/owner-a/orders?record=now&live=sse",
            Action::Tail,
            Some("orders"),
        ),
        ("HEAD", "/owner-a/orders", Action::Head, Some("orders")),
        ("DELETE", "/owner-a/orders", Action::Delete, Some("orders")),
        (
            "PUT",
            "/owner-a/orders/attrs",
            Action::Update,
            Some("orders"),
        ),
        (
            "GET",
            "/owner-a/orders/snapshot",
            Action::ReadSnapshot,
            Some("orders"),
        ),
        (
            "PUT",
            "/owner-a/orders/snapshot/42",
            Action::PublishSnapshot,
            Some("orders"),
        ),
    ];

    for (method, uri, expected_action, expected_stream) in cases {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let classified = classify_request(request.method(), request.uri(), request.headers())
            .expect("classified Durable Streams request");
        assert_eq!(classified.resource.bucket_id, "owner-a");
        assert_eq!(
            classified.resource.stream_id.as_deref(),
            expected_stream,
            "{method} {uri}"
        );
        assert_eq!(classified.action, expected_action, "{method} {uri}");
    }
}

#[test]
fn request_classifier_distinguishes_final_writes() {
    for (method, expected_action) in [
        ("PUT", Action::CreateAndClose),
        ("POST", Action::AppendAndClose),
    ] {
        let request = Request::builder()
            .method(method)
            .uri("/owner-a/orders")
            .header(HEADER_STREAM_CLOSED, "true")
            .body(Body::empty())
            .expect("request");

        let classified = classify_request(request.method(), request.uri(), request.headers())
            .expect("classified final write");

        assert_eq!(classified.action, expected_action);
    }
}

#[test]
fn request_classifier_decodes_resource_path_segments() {
    let request = Request::builder()
        .method("GET")
        .uri("/owner-a/hello%20world")
        .body(Body::empty())
        .expect("request");

    let classified = classify_request(request.method(), request.uri(), request.headers())
        .expect("classified request");

    assert_eq!(
        classified.resource.stream_id.as_deref(),
        Some("hello world")
    );
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
async fn gateway_without_access_control_preserves_existing_pass_through_behavior() {
    let upstream = spawn_upstream(Router::new().route(
        "/bucket/stream",
        get(|headers: HeaderMap| async move {
            headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_owned()
        }),
    ))
    .await;
    let gateway = Gateway::new(test_config(vec![upstream.url.clone()]));
    let request = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .header(AUTHORIZATION, "Bearer existing-client-token")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    assert_eq!(&body[..], b"Bearer existing-client-token");
}

#[tokio::test]
async fn access_control_allows_anonymous_public_resource_without_resolving_token() {
    let upstream =
        spawn_upstream(Router::new().route("/public/events", get(|| async { StatusCode::OK })))
            .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(
        upstream.url.clone(),
        Arc::clone(&resolver),
        Arc::clone(&authorizer),
    );
    let request = Request::builder()
        .method("GET")
        .uri("/public/events")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
    let requests = authorizer.requests.lock().expect("authorization requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].principal, None);
    assert_eq!(requests[0].resource.bucket_id, "public");
    assert_eq!(requests[0].resource.stream_id.as_deref(), Some("events"));
    assert_eq!(requests[0].action, Action::Read);
}

#[tokio::test]
async fn access_control_resolves_bearer_and_does_not_forward_it_upstream() {
    let upstream = spawn_upstream(Router::new().route(
        "/owner-a/events",
        get(|headers: HeaderMap| async move {
            if headers.contains_key(AUTHORIZATION) {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        }),
    ))
    .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(
        upstream.url.clone(),
        Arc::clone(&resolver),
        Arc::clone(&authorizer),
    );
    let request = Request::builder()
        .method("GET")
        .uri("/owner-a/events")
        .header(AUTHORIZATION, "Bearer valid-token")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 1);
    let requests = authorizer.requests.lock().expect("authorization requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .principal
            .as_ref()
            .map(|principal| principal.subject.as_str()),
        Some("user-1")
    );
}

#[tokio::test]
async fn access_control_conceals_private_resource_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().route(
        "/private/events",
        get(move || {
            let upstream_hits = Arc::clone(&upstream_hits);
            async move {
                upstream_hits.fetch_add(1, Ordering::Relaxed);
                StatusCode::OK
            }
        }),
    ))
    .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(
        AuthorizationDecision::ConcealAsNotFound,
    ));
    let gateway = gateway_with_access_control(upstream.url.clone(), resolver, authorizer);
    let request = Request::builder()
        .method("GET")
        .uri("/private/events")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn access_control_rejects_invalid_bearer_before_authorization() {
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway =
        gateway_with_access_control("http://127.0.0.1:1", resolver, Arc::clone(&authorizer));
    let request = Request::builder()
        .method("GET")
        .uri("/owner-a/events")
        .header(AUTHORIZATION, "Basic not-a-bearer")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(WWW_AUTHENTICATE)
            .expect("authenticate challenge"),
        r#"Bearer error="invalid_token""#
    );
    assert!(
        authorizer
            .requests
            .lock()
            .expect("authorization requests")
            .is_empty()
    );
}

#[tokio::test]
async fn access_control_fails_closed_for_unclassified_routes() {
    let hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().fallback(any(move || {
        let upstream_hits = Arc::clone(&upstream_hits);
        async move {
            upstream_hits.fetch_add(1, Ordering::Relaxed);
            StatusCode::OK
        }
    })))
    .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(upstream.url.clone(), resolver, authorizer);

    for uri in ["/__ursula/metrics", "/future-unclassified-route"] {
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let response = gateway.handle(request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
    }
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn gateway_rejects_body_larger_than_configured_limit_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let app_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().route(
        "/bucket/stream",
        post(move || {
            let app_hits = Arc::clone(&app_hits);
            async move {
                app_hits.fetch_add(1, Ordering::Relaxed);
                StatusCode::OK
            }
        }),
    ))
    .await;

    let mut config = test_config(vec![upstream.url.clone()]);
    config.max_request_body_bytes = 4;
    let gateway = Gateway::new(config);
    let req = Request::builder()
        .method("POST")
        .uri("/bucket/stream")
        .body(Body::from(bytes::Bytes::from_static(b"12345")))
        .unwrap();

    let resp = gateway.handle(req).await;

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn gateway_follows_leader_redirect_for_get_request() {
    let leader_app = Router::new().route("/bucket/stream", get(|| async { StatusCode::OK }));
    let (leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        follower.url.clone(),
        leader.url.clone(),
    ])));

    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .header("authorization", "Bearer secret")
        .body(Body::empty())
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&follower.url, &parts, body_bytes)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn gateway_follows_leader_redirect_for_put_request() {
    let leader_app = Router::new().route("/bucket/stream", put(|| async { StatusCode::OK }));
    let (leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        follower.url.clone(),
        leader.url.clone(),
    ])));

    let req = Request::builder()
        .method("PUT")
        .uri("/bucket/stream")
        .header("authorization", "Bearer secret")
        .body(Body::from("payload"))
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&follower.url, &parts, body_bytes)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn gateway_returns_raft_redirect_when_leader_not_in_upstreams() {
    let leader =
        spawn_upstream(Router::new().route("/bucket/stream", get(|| async { StatusCode::OK })))
            .await;
    let leader_url = format!("{}/bucket/stream", leader.url);
    let follower = spawn_upstream(Router::new().route(
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
    ))
    .await;

    // Gateway only knows about the follower, not the leader.
    let gateway = gateway_for_url(follower.url.clone());
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&follower.url, &parts, body_bytes)
        .await
        .unwrap();

    // Cannot resolve leader → return 307 to client with stripped host.
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(resp.headers().get(LOCATION).unwrap(), "/bucket/stream");
    assert!(resp.headers().get(HEADER_URSULA_RAFT_LEADER_ID).is_none());
}

#[tokio::test]
async fn gateway_handle_follows_leader_redirect_through_random_pick() {
    let leader_app = Router::new().route("/bucket/stream", get(|| async { StatusCode::OK }));
    let (leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        follower.url.clone(),
        leader.url.clone(),
    ])));

    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .unwrap();

    // Use handle() so the random pick is exercised.
    let resp = gateway.handle(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
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

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&upstream_url, &parts, body_bytes)
        .await
        .unwrap();

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

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let err = gateway
        .forward("https://127.0.0.1:1", &parts, body_bytes)
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

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let mut resp = gateway
        .forward(&upstream.url, &parts, body_bytes)
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

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&upstream.url, &parts, body_bytes)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers().get(LOCATION).unwrap(),
        "/bucket/stream/snapshot/00000000000000000003"
    );
}
