use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::http::header::LOCATION;
use axum::routing::any;
use axum::routing::get;
use axum::routing::put;
use http_body_util::BodyExt;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

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
