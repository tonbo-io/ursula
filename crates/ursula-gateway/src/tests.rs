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
        raft_group_count: None,
    }
}

fn gateway_for_url(upstream_url: impl Into<String>) -> Arc<Gateway> {
    Arc::new(Gateway::new(test_config(vec![upstream_url.into()])))
}

#[test]
fn gateway_reuses_learned_stream_leader() {
    let gateway = Gateway::new(test_config(vec![
        "http://follower.test".to_owned(),
        "http://leader.test".to_owned(),
    ]));
    gateway.remember_leader("/bucket/stream".to_owned(), "http://leader.test".to_owned());

    let uri: Uri = "/bucket/stream?live=long-poll".parse().expect("uri");
    assert_eq!(
        gateway.pick_upstream(&uri).as_deref(),
        Some("http://leader.test")
    );
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_cache_entries, 1);
}

#[tokio::test]
async fn gateway_evicts_cached_leader_on_retryable_leader_unknown_response() {
    let stale = spawn_upstream(Router::new().route(
        "/bucket/stream",
        get(|| async {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(RETRY_AFTER.as_str(), "1")],
                "leader unknown",
            )
        }),
    ))
    .await;
    let gateway = Gateway::new(test_config(vec![stale.url.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), stale.url.clone());

    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .expect("request");
    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("request body");
    let response = gateway
        .forward(&stale.url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
        .await
        .expect("gateway response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
}

#[test]
fn stream_affinity_key_ignores_subresource_and_internal_routes() {
    let append_batch: Uri = "/bucket/stream/append-batch".parse().expect("uri");
    let metrics: Uri = "/__ursula/gateway/metrics".parse().expect("uri");

    assert_eq!(
        stream_affinity_key(&append_batch, None).as_deref(),
        Some("/bucket/stream")
    );
    assert_eq!(stream_affinity_key(&metrics, None), None);
}

#[test]
fn group_affinity_key_is_shared_by_streams_in_the_same_group() {
    let shard_map = StaticShardMap::new(1, 16).expect("valid shard map");
    let first: Uri = "/bucket/stream-1".parse().expect("uri");
    let first_key = stream_affinity_key(&first, Some(&shard_map)).expect("first key");
    let second = (2..10_000)
        .map(|index| {
            format!("/bucket/stream-{index}")
                .parse::<Uri>()
                .expect("uri")
        })
        .find(|uri| stream_affinity_key(uri, Some(&shard_map)).as_ref() == Some(&first_key))
        .expect("stream in same group");

    assert_ne!(first.path(), second.path());
    assert_eq!(
        stream_affinity_key(&second, Some(&shard_map)).as_deref(),
        Some(first_key.as_str())
    );

    let mut config = test_config(vec![
        "http://follower.test".to_owned(),
        "http://leader.test".to_owned(),
    ]);
    config.raft_group_count = Some(16);
    let gateway = Gateway::new(config);
    gateway.remember_leader(first_key, "http://leader.test".to_owned());
    assert_eq!(
        gateway.pick_upstream(&second).as_deref(),
        Some("http://leader.test")
    );
    assert_eq!(gateway.metrics_snapshot().leader_cache_hits, 1);
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
        .forward(&follower.url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
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
        .forward(&follower.url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
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
        .forward(&follower.url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
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
        .forward(&upstream_url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
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
        .forward("https://127.0.0.1:1", &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
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
        .forward(&upstream.url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
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
async fn gateway_gives_long_poll_response_headers_timeout_headroom() {
    let upstream = spawn_upstream(Router::new().route(
        "/bucket/stream",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(80)).await;
            StatusCode::NO_CONTENT
        }),
    ))
    .await;
    let gateway =
        gateway_with_response_header_timeout(upstream.url.clone(), Duration::from_millis(50));
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream?live=long-poll&timeout_ms=75")
        .body(Body::empty())
        .expect("build request");

    let response = gateway.handle(req).await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[test]
fn gateway_caps_long_poll_timeout_headroom_at_server_limit() {
    let gateway =
        gateway_with_response_header_timeout("http://127.0.0.1:1", Duration::from_secs(30));

    assert_eq!(
        gateway.response_header_timeout_for_url(
            "http://127.0.0.1:1/bucket/stream?live=long-poll&timeout_ms=999999"
        ),
        Duration::from_secs(62)
    );
    assert_eq!(
        gateway.response_header_timeout_for_url(
            "http://127.0.0.1:1/bucket/stream?live=sse&timeout_ms=999999"
        ),
        Duration::from_secs(30)
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
        .forward(&upstream.url, &parts, body_bytes, ResponseTail {
            meter: None,
            _live_guard: None,
        })
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers().get(LOCATION).unwrap(),
        "/bucket/stream/snapshot/00000000000000000003"
    );
}

// End-to-end shape of the #132/#133 tenant boundary: one shared gateway, a
// static bucket policy, and two tenants whose credentials must not cross.
mod tenant_boundary {
    use super::*;
    use crate::auth::policy::StaticPolicyAuthorizer;

    const POLICY: &str = r#"
        [[bucket]]
        id = "tenant-a"
        owners = [{ issuer = "https://issuer.example", subject = "user-1" }]

        [[bucket]]
        id = "tenant-public"
        public_read = true
    "#;

    fn policy_gateway(upstream_url: &str) -> Gateway {
        Gateway::with_access_control(
            test_config(vec![upstream_url.to_owned()]),
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(StaticPolicyAuthorizer::from_toml_str(POLICY).expect("parse policy")),
            ),
        )
    }

    fn shared_upstream() -> Router {
        Router::new()
            .route("/tenant-a/orders", any(|| async { "tenant-a data" }))
            .route("/tenant-b/orders", any(|| async { "tenant-b data" }))
            .route("/tenant-public/orders", any(|| async { "public data" }))
    }

    async fn send(
        gateway: &Gateway,
        method: &str,
        uri: &str,
        bearer: Option<&str>,
    ) -> axum::response::Response {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(token) = bearer {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        gateway
            .handle(request.body(Body::empty()).expect("request"))
            .await
    }

    #[tokio::test]
    async fn owner_reaches_their_own_bucket() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        let response = send(&gateway, "GET", "/tenant-a/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = send(&gateway, "POST", "/tenant-a/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn credential_cannot_cross_into_another_tenant() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        // `valid-token` belongs to tenant-a's owner. tenant-b both exists
        // upstream and is absent from the policy: either way the caller
        // observes the same 404 surface.
        let response = send(&gateway, "GET", "/tenant-b/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = send(&gateway, "POST", "/tenant-b/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn anonymous_reads_reach_only_public_buckets() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        let response = send(&gateway, "GET", "/tenant-public/orders", None).await;
        assert_eq!(response.status(), StatusCode::OK);

        // A private bucket conceals its existence from anonymous probes.
        let response = send(&gateway, "GET", "/tenant-a/orders", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn public_visibility_grants_reads_but_never_writes() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        let response = send(&gateway, "POST", "/tenant-public/orders", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // A non-owner principal gets no more than the anonymous public grant.
        let response = send(
            &gateway,
            "POST",
            "/tenant-public/orders",
            Some("valid-token"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

// Gateway-half quota enforcement and usage accounting (#134/#135).
mod admission_and_usage {
    use super::*;
    use crate::admission::StaticQuotaProvider;
    use crate::auth::policy::StaticPolicyAuthorizer;
    use crate::usage::UsageBatch;
    use crate::usage::UsageClass;
    use crate::usage::UsageCollector;
    use crate::usage::UsageSink;
    use crate::usage::UsageSinkFuture;

    const POLICY: &str = r#"
        [[bucket]]
        id = "tenant-a"
        owners = [{ issuer = "https://issuer.example", subject = "user-1" }]
    "#;

    const QUOTAS: &str = r#"
        [[bucket]]
        id = "tenant-a"
        requests_per_sec = 2
        max_request_body_bytes = 8
    "#;

    struct CapturingSink {
        batches: Mutex<Vec<UsageBatch>>,
    }

    impl UsageSink for CapturingSink {
        fn export<'a>(&'a self, batch: &'a UsageBatch) -> UsageSinkFuture<'a> {
            self.batches
                .lock()
                .expect("batches lock")
                .push(batch.clone());
            Box::pin(async { Ok(()) })
        }
    }

    fn quota_gateway(upstream_url: &str, collector: Arc<UsageCollector>) -> Gateway {
        Gateway::with_access_control(
            test_config(vec![upstream_url.to_owned()]),
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(StaticPolicyAuthorizer::from_toml_str(POLICY).expect("parse policy")),
            ),
        )
        .with_quota_provider(Arc::new(
            StaticQuotaProvider::from_toml_str(QUOTAS).expect("parse quotas"),
        ))
        .with_usage_collector(collector)
    }

    async fn send(
        gateway: &Gateway,
        method: &str,
        uri: &str,
        body: &'static str,
    ) -> axum::response::Response {
        gateway
            .handle(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(AUTHORIZATION, "Bearer valid-token")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
    }

    #[tokio::test]
    async fn rate_limit_rejects_with_retry_after_and_never_reaches_upstream() {
        let upstream =
            spawn_upstream(Router::new().route("/tenant-a/orders", any(|| async { "ok" }))).await;
        let gateway = quota_gateway(&upstream.url, UsageCollector::new());

        assert_eq!(
            send(&gateway, "GET", "/tenant-a/orders", "").await.status(),
            StatusCode::OK
        );
        assert_eq!(
            send(&gateway, "GET", "/tenant-a/orders", "").await.status(),
            StatusCode::OK
        );
        let limited = send(&gateway, "GET", "/tenant-a/orders", "").await;
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after = limited
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("Retry-After header");
        assert!(retry_after >= 1);
    }

    #[tokio::test]
    async fn per_tenant_body_limit_rejects_oversized_appends() {
        let upstream =
            spawn_upstream(Router::new().route("/tenant-a/orders", any(|| async { "ok" }))).await;
        let gateway = quota_gateway(&upstream.url, UsageCollector::new());

        let oversized = send(&gateway, "POST", "/tenant-a/orders", "123456789").await;
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let allowed = send(&gateway, "POST", "/tenant-a/orders", "12345678").await;
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn usage_counts_append_ingress_and_read_egress_per_principal() {
        let upstream =
            spawn_upstream(Router::new().route("/tenant-a/orders", any(|| async { "0123456789" })))
                .await;
        let collector = UsageCollector::new();
        let gateway = quota_gateway(&upstream.url, Arc::clone(&collector));

        let append = send(&gateway, "POST", "/tenant-a/orders", "12345678").await;
        assert_eq!(append.status(), StatusCode::OK);
        // Drain the response body so the meter observes completion.
        let _ = append.into_body().collect().await.expect("append body");
        let read = send(&gateway, "GET", "/tenant-a/orders", "").await;
        let read_body = read.into_body().collect().await.expect("read body");
        assert_eq!(read_body.to_bytes().len(), 10);

        let sink = CapturingSink {
            batches: Mutex::new(Vec::new()),
        };
        crate::usage::flush(&collector, &sink).await;
        let batches = sink.batches.lock().expect("batches lock");
        assert_eq!(batches.len(), 1);
        let records = &batches[0].records;

        let append_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Append)
            .expect("append usage record");
        assert_eq!(append_record.key.bucket_id, "tenant-a");
        assert_eq!(append_record.counters.requests, 1);
        assert_eq!(append_record.counters.request_bytes, 8);
        let principal = append_record.key.principal.as_ref().expect("principal");
        assert_eq!(principal.subject, "user-1");

        let read_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Read)
            .expect("read usage record");
        assert_eq!(read_record.counters.response_bytes, 10);
    }
}
