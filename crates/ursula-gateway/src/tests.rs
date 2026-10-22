use std::convert::Infallible;
use std::num::NonZeroU64;
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
use crate::ejection::EJECTION_BASE;

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
        upstream_tcp_user_timeout_secs: NonZeroU64::new(5).unwrap(),
        uploads: crate::uploads::UploadLimits::default(),
        raft_group_count: None,
        cors_allowed_origins: Vec::new(),
        usage_chunk_bytes: None,
    }
}

/// Sets `config`'s per-request body limit to `bytes`, the rest of its upload
/// limits unchanged.
fn set_body_limit(config: &mut GatewayConfig, bytes: usize) {
    config.uploads = crate::uploads::UploadLimits::try_from(crate::uploads::UploadLimitsSpec {
        max_request_body_bytes: bytes,
        ..config.uploads.spec().clone()
    })
    .expect("valid upload limits");
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
        .forward(&stale.url, &parts, body_bytes, ResponseTail::default())
        .await
        .expect("gateway response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
}

fn get_stream() -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .expect("request")
}

/// A cached leader whose node is gone (a deleted pod, a host that lost
/// power) is dropped from the cache on the transport failure, and the client
/// gets a retryable 503 instead of an empty 502, so its retry reaches a live
/// node.
#[tokio::test]
async fn gateway_drops_an_unreachable_cached_leader_and_answers_retryable_503() {
    let (gone, _refusing) = refusing_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), gone.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(RETRY_AFTER)
            .map(HeaderValue::as_bytes),
        Some(&b"1"[..])
    );
    // The first hop failed to connect, so no node received the request.
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    assert!(!body.is_empty());
    assert!(!String::from_utf8_lossy(&body).contains(&gone));
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
}

/// Ports nothing listens on, `count` of them and all different: connects to
/// each are refused while the [`Refusing`] lives.
async fn refusing_upstreams(count: usize) -> (Vec<String>, Refusing) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let peer = listener.local_addr().expect("local addr");
    let mut urls = Vec::new();
    let mut connections = Vec::new();
    for _ in 0..count {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket
            .bind("127.0.0.1:0".parse().expect("loopback"))
            .expect("bind");
        let connection = socket.connect(peer).await.expect("connect");
        let (accepted, _peer) = listener.accept().await.expect("accept");
        urls.push(format!(
            "http://{}",
            connection.local_addr().expect("local addr")
        ));
        connections.push((connection, accepted));
    }
    (urls, Refusing {
        _connections: connections,
    })
}

/// A port nothing listens on: connects are refused while the [`Refusing`]
/// lives.
async fn refusing_upstream() -> (String, Refusing) {
    let (mut urls, refusing) = refusing_upstreams(1).await;
    (urls.pop().expect("one upstream"), refusing)
}

/// Holds the ports of [`refusing_upstreams`]: each is the local end of an
/// open loopback connection, so `bind(0)` never returns it and nothing
/// listens on it (a connect to it is refused). A socket that is
/// only bound would hold the port too, but macOS drops a connect to it
/// instead of refusing it, so the connect waits for its timeout.
struct Refusing {
    _connections: Vec<(tokio::net::TcpStream, tokio::net::TcpStream)>,
}

fn get_path(path: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .expect("request")
}

fn answering_router() -> Router {
    Router::new().route("/{bucket}/{stream}", any(|| async { "ok" }))
}

async fn answering_upstream() -> TestUpstream {
    spawn_upstream(answering_router()).await
}

/// Accepts requests and never answers them. Each request's arrival is sent
/// on the returned channel.
async fn silent_upstream() -> (TestUpstream, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let (arrived, arrivals) = tokio::sync::mpsc::unbounded_channel();
    let upstream = spawn_upstream(Router::new().route(
        "/{bucket}/{stream}",
        any(move || {
            arrived.send(()).expect("the test awaits arrivals");
            std::future::pending::<&'static str>()
        }),
    ))
    .await;
    (upstream, arrivals)
}

/// Holds a paused clock still while requests wait on real sockets, so time
/// moves only by `tokio::time::advance`. Tokio otherwise advances a paused
/// clock to the next timer whenever the runtime idles, as it does while a
/// request waits on the loopback, but not while a blocking task runs. Needs
/// `start_paused = true`.
struct FrozenClock {
    _release: std::sync::mpsc::Sender<()>,
    _holder: tokio::task::JoinHandle<()>,
}

impl FrozenClock {
    fn hold() -> Self {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            // Returns when `release` is dropped.
            let _released = released.recv();
        });
        Self {
            _release: release,
            _holder: holder,
        }
    }
}

/// Makes `gone` fail one request through `gateway`, along a cached route to
/// it, as a dead node's first failure does.
async fn fail_once(gateway: &Gateway, gone: &str) {
    gateway.remember_leader("/bucket/stream".to_owned(), gone.to_owned());
    let response = gateway.handle(get_stream()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// `count` requests to distinct streams, each answered 200.
async fn all_answered(gateway: &Gateway, prefix: &str, count: usize) {
    for i in 0..count {
        let response = gateway
            .handle(get_path(&format!("/bucket/{prefix}{i}")))
            .await;
        assert_eq!(response.status(), StatusCode::OK, "{prefix}{i}");
    }
}

/// After an upstream fails at the transport, requests stop picking it at
/// random (#454): before, each one that did paid a connect timeout or a
/// dead pooled connection.
#[tokio::test(start_paused = true)]
async fn gateway_leaves_an_upstream_that_failed_out_of_selection() {
    let _frozen = FrozenClock::hold();
    let (gone, _refusing) = refusing_upstream().await;
    let live = [answering_upstream().await, answering_upstream().await];
    let gateway = Gateway::new(test_config(vec![
        gone.clone(),
        live[0].url.clone(),
        live[1].url.clone(),
    ]));
    fail_once(&gateway, &gone).await;

    // 60 random picks among three would miss `gone` with probability (2/3)^60.
    all_answered(&gateway, "s", 60).await;
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 1);
    assert_eq!(metrics.upstream_ejected, 1);
}

/// When its window expires, the next request that selects the upstream
/// probes it. Another failure ejects it for a doubled window.
#[tokio::test(start_paused = true)]
async fn gateway_probes_an_ejected_upstream_after_its_window() {
    let _frozen = FrozenClock::hold();
    let (gone, _refusing) = refusing_upstream().await;
    let live = answering_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone(), live.url.clone()]));
    fail_once(&gateway, &gone).await;
    all_answered(&gateway, "s", 20).await;

    tokio::time::advance(EJECTION_BASE).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);
    // The probe: a request along a cached route to it.
    fail_once(&gateway, &gone).await;
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 2);
    assert_eq!(metrics.upstream_ejected, 1);

    // The second window is twice the first.
    let second_window = EJECTION_BASE.saturating_mul(2);
    tokio::time::advance(second_window.saturating_sub(Duration::from_secs(1))).await;
    all_answered(&gateway, "t", 20).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);
}

/// While one request probes an expired upstream, every other request skips
/// it, through a cached route or at random, until the probe deadline.
#[tokio::test(start_paused = true)]
async fn gateway_sends_one_probe_at_a_time() {
    let _frozen = FrozenClock::hold();
    let (silent, mut arrivals) = silent_upstream().await;
    let live = answering_upstream().await;
    // A header timeout past the probe deadline, as a long-poll's is.
    let mut config = test_config(vec![silent.url.clone(), live.url.clone()]);
    config.response_header_timeout = Duration::from_secs(60);
    let gateway = Arc::new(Gateway::new(config));
    gateway.ejections.failed(&silent.url, Instant::now());
    tokio::time::advance(EJECTION_BASE).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);

    // The probe: a request along a cached route to the silent upstream.
    gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());
    let probe = tokio::spawn({
        let gateway = Arc::clone(&gateway);
        async move { gateway.handle(get_stream()).await }
    });
    arrivals
        .recv()
        .await
        .expect("the probe reaches the upstream");
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 1);

    all_answered(&gateway, "s", 40).await;
    gateway.remember_leader("/bucket/other".to_owned(), silent.url.clone());
    let response = gateway.handle(get_path("/bucket/other")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 1);

    // The probe deadline is the 1 s connect timeout plus the 5 s user
    // timeout. Past it, the probe no longer holds the others back.
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);
    // At its header timeout the probe ejects the silent upstream again, and
    // the read goes on to the live one.
    tokio::time::advance(Duration::from_secs(54)).await;
    let probed = probe.await.expect("probe task");
    assert_eq!(probed.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(
        (metrics.upstream_ejections, metrics.upstream_ejected),
        (1, 1)
    );
    assert!(arrivals.try_recv().is_err(), "one probe only");
}

/// A response clears an ejection. Here the leader answers the request that
/// probes it after its window, along its cached route, which stays.
#[tokio::test(start_paused = true)]
async fn gateway_restores_an_upstream_that_answers_its_probe() {
    let _frozen = FrozenClock::hold();
    let (leader, follower) = spawn_raft_redirect_upstreams(answering_router()).await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), leader.url.clone()]));
    gateway.ejections.failed(&leader.url, Instant::now());
    tokio::time::advance(EJECTION_BASE).await;
    gateway.remember_leader("/bucket/stream".to_owned(), leader.url.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    // Unanswered, the probe would still count.
    assert_eq!(metrics.upstream_ejected, 0);
    assert_eq!(metrics.leader_cache_entries, 1);
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_redirects, 0);
}

/// Inside its window an ejected leader is skipped, so a request lands on a
/// follower. The follower's 307 re-learns the route to the leader, and the
/// request that follows it acts as the leader's probe: its response clears
/// the ejection.
#[tokio::test(start_paused = true)]
async fn gateway_follows_a_redirect_to_an_ejected_leader_as_its_probe() {
    let _frozen = FrozenClock::hold();
    let (leader, follower) = spawn_raft_redirect_upstreams(answering_router()).await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), leader.url.clone()]));
    gateway.ejections.failed(&leader.url, Instant::now());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_redirects, 1);
    assert_eq!(metrics.leader_cache_entries, 1);
    assert_eq!(metrics.upstream_ejected, 0);

    // The next request goes along the re-learned route.
    let response = gateway.handle(get_stream()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_redirects, 1);
}

/// The leader a follower redirects to is ejected when it fails, and the
/// follower, which answered, is not.
#[tokio::test(start_paused = true)]
async fn gateway_ejects_an_unreachable_redirect_leader() {
    let _frozen = FrozenClock::hold();
    let (gone, _refusing) = refusing_upstream().await;
    let leader_url = format!("{gone}/bucket/stream");
    let follower = spawn_upstream(Router::new().route(
        "/bucket/stream",
        any(move || {
            let leader_url = leader_url.clone();
            async move {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [
                        ("location", leader_url),
                        ("x-ursula-raft-leader-id", "2".to_owned()),
                    ],
                    "redirecting",
                )
            }
        }),
    ))
    .await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), gone.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), follower.url.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    // The follower may have proposed the request before it redirected.
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 1);
    assert_eq!(metrics.upstream_ejected, 1);
    // Every request now goes to the follower, which redirects again.
    for i in 0..20 {
        let response = gateway.handle(get_path(&format!("/bucket/r{i}"))).await;
        assert_ne!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "r{i}");
    }
}

/// With every upstream ejected, requests still go out rather than nowhere.
#[tokio::test(start_paused = true)]
async fn gateway_still_sends_when_every_upstream_is_ejected() {
    let _frozen = FrozenClock::hold();
    let (gone, _refusing) = refusing_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone()]));
    fail_once(&gateway, &gone).await;

    let response = gateway.handle(get_path("/bucket/again")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(gateway.metrics_snapshot().upstream_ejections, 1);
}

/// With every upstream ejected, a request keeps its cached route rather
/// than dropping it for a random pick.
#[test]
fn gateway_keeps_the_cached_route_when_every_upstream_is_ejected() {
    let gateway = Gateway::new(test_config(vec![
        "http://follower.test".to_owned(),
        "http://leader.test".to_owned(),
    ]));
    let now = Instant::now();
    gateway.ejections.failed("http://follower.test", now);
    gateway.ejections.failed("http://leader.test", now);
    gateway.remember_leader("/bucket/stream".to_owned(), "http://leader.test".to_owned());

    let uri: Uri = "/bucket/stream".parse().expect("uri");
    assert_eq!(
        gateway.pick_upstream(&uri).as_deref(),
        Some("http://leader.test")
    );
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_cache_evictions, 0);
    assert_eq!(metrics.leader_cache_entries, 1);
}

/// A cached route to an ejected upstream is not followed: the request picks
/// a live one.
#[tokio::test(start_paused = true)]
async fn gateway_drops_a_cached_route_to_an_ejected_upstream() {
    let _frozen = FrozenClock::hold();
    let (gone, _refusing) = refusing_upstream().await;
    let live = answering_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone(), live.url.clone()]));
    fail_once(&gateway, &gone).await;
    gateway.remember_leader("/bucket/stream".to_owned(), gone.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_entries, 0);
    assert_eq!(metrics.upstream_ejections, 1);
}

/// A request that could not be built never left the gateway, so its
/// upstream stays in selection.
#[tokio::test]
async fn gateway_does_not_eject_an_upstream_for_a_request_it_could_not_build() {
    let gateway = Gateway::new(test_config(vec!["not a url".to_owned()]));

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 0);
    assert_eq!(metrics.upstream_ejected, 0);
}

/// A cached leader that accepts the request and never answers is dropped
/// too, and leaves selection as a node that does not answer. The request
/// may have reached it, and no other upstream is left to send it to, so the
/// answer is a 504, not a retry invitation.
#[tokio::test]
async fn gateway_drops_a_silent_cached_leader_and_answers_gateway_timeout() {
    let silent = spawn_upstream(
        Router::new().route("/bucket/stream", get(std::future::pending::<&'static str>)),
    )
    .await;
    let gateway = gateway_with_response_header_timeout(&silent.url, Duration::from_millis(200));
    gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
    assert_eq!(metrics.upstream_ejections, 1);
    assert_eq!(metrics.upstream_ejected, 1);
}

/// `POST /bucket/stream` with a producer key.
fn producer_post() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/bucket/stream")
        .header(names::PRODUCER_ID, "p")
        .header(names::PRODUCER_EPOCH, "0")
        .header(names::PRODUCER_SEQ, "0")
        .body(Body::from("abc"))
        .expect("request")
}

/// Sends `request` through `gateway` and moves the paused clock past one
/// header timeout of `timeout` once the silent upstream got it.
async fn through_one_timeout(
    gateway: &Arc<Gateway>,
    request: Request<Body>,
    arrivals: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
    timeout: Duration,
) -> AxumResponse {
    let mut sent = send(gateway, request);
    until_reached(
        async {
            arrivals.recv().await.expect("the upstream is up");
        },
        &mut sent,
    )
    .await;
    tokio::time::advance(timeout).await;
    sent.await.expect("request task")
}

/// A cached leader that accepts a request and never answers, as a stopped
/// process does: a read and a producer write go on to an upstream they have
/// not tried, which answers them and becomes the cached route. The silent
/// one leaves selection. A write without a key may have reached it and
/// applies twice if sent again, so it gets the `504`, reaches no other
/// upstream and teaches no route.
#[tokio::test(start_paused = true)]
async fn a_read_or_a_keyed_write_that_gets_no_answer_goes_to_another_upstream() {
    let _frozen = FrozenClock::hold();
    let timeout = Duration::from_secs(5);
    let (silent, mut arrivals) = silent_upstream().await;
    let live = Scripted::spawn().await;
    let config = test_config(vec![silent.url.clone(), live.url()]);
    for (request, status, hits, route) in [
        (get_stream(), StatusCode::OK, 1, Some(live.url())),
        (producer_post(), StatusCode::OK, 1, Some(live.url())),
        (
            post_path("/bucket/stream"),
            StatusCode::GATEWAY_TIMEOUT,
            0,
            None,
        ),
        // A node reads a producer key on an append alone, so a keyed
        // `DELETE` is a keyless write: a repeat could delete a stream
        // created again meanwhile.
        (
            keyed(Method::DELETE, "/bucket/stream"),
            StatusCode::GATEWAY_TIMEOUT,
            0,
            None,
        ),
    ] {
        let gateway = Arc::new(Gateway::new(config.clone()));
        gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());
        let before = live.hits();
        let method = request.method().clone();

        let response = through_one_timeout(&gateway, request, &mut arrivals, timeout).await;

        assert_eq!(response.status(), status, "{method}");
        if status == StatusCode::GATEWAY_TIMEOUT {
            assert_eq!(
                error_code_of(response).await,
                ErrorCode::UpstreamTimeout,
                "{method}"
            );
        }
        assert_eq!(live.hits().saturating_sub(before), hits, "{method}");
        let metrics = gateway.metrics_snapshot();
        assert_eq!(metrics.upstream_ejections, 1, "{method}");
        assert_eq!(cached_route(&gateway), route, "{method}");
    }
}

/// A producer write is sent again once at most, and only a `2xx` receipt of
/// the repeat answers it: the first upstream may still apply it, so a
/// refusal of the repeat or a second silent upstream leave its outcome
/// unknown, a `504`.
#[tokio::test(start_paused = true)]
async fn a_keyed_write_sent_again_ends_unknown_unless_the_repeat_holds() {
    let _frozen = FrozenClock::hold();
    let timeout = Duration::from_secs(5);
    let (silent, mut arrivals) = silent_upstream().await;
    let refusing = Scripted::spawn().await;
    refusing.answers(refusal());
    let gateway = Arc::new(Gateway::new(test_config(vec![
        silent.url.clone(),
        refusing.url(),
    ])));
    gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());
    let response = through_one_timeout(&gateway, producer_post(), &mut arrivals, timeout).await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(error_code_of(response).await, ErrorCode::UpstreamTimeout);
    assert_eq!(refusing.hits(), 1);

    let (second, mut second_arrivals) = silent_upstream().await;
    let live = Scripted::spawn().await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        silent.url.clone(),
        second.url.clone(),
        live.url(),
    ])));
    gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());
    let mut sent = send(&gateway, producer_post());
    until_reached(
        async {
            arrivals.recv().await.expect("the upstream is up");
        },
        &mut sent,
    )
    .await;
    tokio::time::advance(timeout).await;
    until_reached(
        async {
            second_arrivals.recv().await.expect("the upstream is up");
        },
        &mut sent,
    )
    .await;
    tokio::time::advance(timeout).await;
    let response = sent.await.expect("request task");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(error_code_of(response).await, ErrorCode::UpstreamTimeout);
    assert_eq!(live.hits(), 0);
    assert_eq!(gateway.metrics_snapshot().upstream_ejections, 2);
}

/// A `method` request to `path` with a producer key.
fn keyed(method: Method, path: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(names::PRODUCER_ID, "p")
        .header(names::PRODUCER_EPOCH, "0")
        .header(names::PRODUCER_SEQ, "0")
        .body(Body::empty())
        .expect("request")
}

/// The code of the error answer `response`.
async fn error_code_of(response: AxumResponse) -> ErrorCode {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the answer's body");
    ErrorBody::parse(&body).expect("an error body").code
}

#[test]
fn upstream_pin_key_ignores_subresource_and_internal_routes() {
    let bootstrap: Uri = "/bucket/stream/bootstrap".parse().expect("uri");
    let metrics: Uri = "/__ursula/gateway/metrics".parse().expect("uri");

    assert_eq!(
        upstream_pin_key(&bootstrap, None).as_deref(),
        Some("/bucket/stream")
    );
    assert_eq!(upstream_pin_key(&metrics, None), None);
}

#[test]
fn group_pin_key_is_shared_by_streams_in_the_same_group() {
    let shard_map = StaticShardMap::new(1, 16).expect("valid shard map");
    let first: Uri = "/bucket/stream-1".parse().expect("uri");
    let first_key = upstream_pin_key(&first, Some(&shard_map)).expect("first key");
    let second = (2..10_000)
        .map(|index| {
            format!("/bucket/stream-{index}")
                .parse::<Uri>()
                .expect("uri")
        })
        .find(|uri| upstream_pin_key(uri, Some(&shard_map)).as_ref() == Some(&first_key))
        .expect("stream in same group");

    assert_ne!(first.path(), second.path());
    assert_eq!(
        upstream_pin_key(&second, Some(&shard_map)).as_deref(),
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
            "/owner-a/orders?offset=now&live=sse",
            Action::Tail,
            Some("orders"),
        ),
        ("HEAD", "/owner-a/orders", Action::Head, Some("orders")),
        ("DELETE", "/owner-a/orders", Action::Delete, Some("orders")),
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
        (
            "POST",
            "/owner-a/orders/snapshot/42/parts",
            Action::PublishSnapshot,
            Some("orders"),
        ),
        (
            "GET",
            "/owner-a/orders/incarnation/4503599627370497/chunk/00000000000004194304",
            Action::Read,
            Some("orders"),
        ),
        (
            "HEAD",
            "/owner-a/orders/incarnation/4503599627370497/chunk/00000000000004194304",
            Action::Read,
            Some("orders"),
        ),
        (
            "GET",
            "/owner-a/orders/incarnation/4503599627370497/snapshot/00000000000000000042",
            Action::ReadSnapshot,
            Some("orders"),
        ),
        (
            "DELETE",
            "/owner-a/orders/snapshot/parts",
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
fn request_classifier_refuses_other_immutable_paths() {
    for (method, uri) in [
        (
            "PUT",
            "/owner-a/orders/incarnation/4503599627370497/chunk/00000000000000000000",
        ),
        (
            "POST",
            "/owner-a/orders/incarnation/4503599627370497/snapshot/00000000000000000000",
        ),
        (
            "GET",
            "/owner-a/orders/incarnation/4503599627370497/bytes/00000000000000000000",
        ),
        ("GET", "/owner-a/orders/incarnation/4503599627370497/chunk"),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        assert_eq!(
            classify_request(request.method(), request.uri(), request.headers()),
            None,
            "{method} {uri}"
        );
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

/// A concealing `404` to a `HEAD` carries no server identity, as no answer
/// of the gateway's own does (extensions Section 10): a client reads it as
/// a refusal, never as a stream the deployment deleted.
#[tokio::test]
async fn a_concealed_head_carries_no_server_identity() {
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(
        AuthorizationDecision::ConcealAsNotFound,
    ));
    let gateway = gateway_with_access_control("http://127.0.0.1:1", resolver, authorizer);
    for method in ["HEAD", "GET"] {
        let request = Request::builder()
            .method(method)
            .uri("/private/events")
            .body(Body::empty())
            .expect("request");
        let response = gateway.handle(request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method}");
        for name in [names::URSULA_PROTOCOL, names::URSULA_CLUSTER_ID] {
            assert!(response.headers().get(name).is_none(), "{method} {name}");
        }
    }
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

    for uri in [
        "/__ursula/metrics",
        "/future-unclassified-route",
        // Three-segment paths that are not subresources (0.6.0 removed affinity).
        "/owner-a/run-42/queue",
    ] {
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

/// An upstream that answers every request `200` with an empty body, and
/// counts the requests.
async fn counting_fallback_upstream() -> (TestUpstream, Arc<AtomicUsize>) {
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
    (upstream, hits)
}

/// `method` on the gateway's metrics path, with `token` as a bearer
/// credential when set.
fn metrics_request(method: Method, token: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().method(method).uri(GATEWAY_METRICS_PATH);
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    request.body(Body::empty()).expect("request")
}

/// The gateway's own metrics in `response`: `200` with its counters as JSON.
/// A forwarded answer has no such body.
async fn served_metrics(response: AxumResponse) -> serde_json::Value {
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("metrics body")
        .to_bytes();
    serde_json::from_slice(&body).expect("the gateway's metrics as JSON")
}

/// Without access control the gateway answers its metrics path itself, to
/// any caller (#548).
#[tokio::test]
async fn gateway_without_access_control_serves_its_metrics() {
    let (upstream, hits) = counting_fallback_upstream().await;
    let gateway = Gateway::new(test_config(vec![upstream.url.clone()]));

    let metrics = served_metrics(gateway.handle(metrics_request(Method::GET, None)).await).await;

    assert_eq!(metrics["leader_searches"], 0);
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

/// An access-controlled gateway can face the internet, so by default it
/// answers its metrics path `404`, like every other `/__ursula/*` path, to
/// every caller: without a token, with a valid one, and with an invalid one.
/// It does not even check the token, so no `401` sets the path apart from a
/// missing one (#548).
#[tokio::test]
async fn access_control_conceals_the_gateway_metrics() {
    let (upstream, hits) = counting_fallback_upstream().await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(
        upstream.url.clone(),
        Arc::clone(&resolver),
        Arc::clone(&authorizer),
    );

    for token in [None, Some("valid-token"), Some("bogus-token")] {
        let response = gateway.handle(metrics_request(Method::GET, token)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{token:?}");
    }
    assert_eq!(hits.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
    assert!(
        authorizer
            .requests
            .lock()
            .expect("authorization requests")
            .is_empty()
    );
}

/// Access control that makes the metrics public lets any caller read them,
/// with a token or without, valid or not, as without access control, and
/// opens nothing else: other methods on the path, and other `/__ursula/*`
/// paths, still answer `404` (#548).
#[tokio::test]
async fn access_control_with_public_metrics_serves_them_to_any_caller() {
    let (upstream, hits) = counting_fallback_upstream().await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    // Conceals every resource, so a metrics request that reached it would
    // answer `404`.
    let authorizer = Arc::new(RecordingAuthorizer::new(
        AuthorizationDecision::ConcealAsNotFound,
    ));
    let gateway = Gateway::with_access_control(
        test_config(vec![upstream.url.clone()]),
        // `.clone()`: `Arc::clone(&resolver)` would take the trait object type
        // that `AccessControl::new` expects and refuse the concrete `Arc`.
        AccessControl::new(resolver.clone(), authorizer.clone()).with_public_metrics(true),
    );

    for token in [None, Some("valid-token"), Some("bogus-token")] {
        let metrics =
            served_metrics(gateway.handle(metrics_request(Method::GET, token)).await).await;
        assert_eq!(metrics["leader_searches"], 0, "{token:?}");
    }
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
    assert!(
        authorizer
            .requests
            .lock()
            .expect("authorization requests")
            .is_empty()
    );

    for (case, request) in [
        ("POST metrics", metrics_request(Method::POST, None)),
        ("node metrics", get_path("/__ursula/metrics")),
    ] {
        let response = gateway.handle(request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{case}");
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
    set_body_limit(&mut config, 4);
    let gateway = Gateway::new(config);
    let req = Request::builder()
        .method("POST")
        .uri("/bucket/stream")
        .body(Body::from(bytes::Bytes::from_static(b"12345")))
        .unwrap();

    let resp = gateway.handle(req).await;

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(named_limit(&resp), Some(SizeLimit::Request(4)));
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

/// The limit a `413` names, as clients parse it.
fn named_limit(response: &AxumResponse) -> Option<SizeLimit> {
    SizeLimit::parse(
        response.status().as_u16(),
        response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("well-formed limit")
}

/// The gateway replaces `Ursula-Max-Request-Bytes` on every forwarded answer
/// that carries it with the smaller of the node's value and its own limit,
/// a `413` of the node included, and adds it to no answer without it.
#[tokio::test]
async fn gateway_lowers_the_request_limit_a_node_advertises() {
    let upstream = spawn_upstream(
        Router::new()
            .route(
                "/bucket/head",
                any(|| async { ([("ursula-max-request-bytes", "33554432")], "") }),
            )
            .route(
                "/bucket/small",
                any(|| async { ([("ursula-max-request-bytes", "2")], "") }),
            )
            .route(
                "/bucket/refused",
                any(|| async {
                    (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        [("ursula-max-request-bytes", "33554432")],
                        "",
                    )
                }),
            )
            .route(
                "/bucket/malformed",
                any(|| async { ([("ursula-max-request-bytes", "lots")], "") }),
            )
            .route("/bucket/plain", any(|| async { "" })),
    )
    .await;
    let mut config = test_config(vec![upstream.url.clone()]);
    set_body_limit(&mut config, 1_048_576);
    let gateway = Gateway::new(config);
    for (path, expected) in [
        ("/bucket/head", Some("1048576")),
        ("/bucket/small", Some("2")),
        ("/bucket/refused", Some("1048576")),
        ("/bucket/malformed", Some("1048576")),
        ("/bucket/plain", None),
    ] {
        let response = gateway.handle(post_path(path)).await;
        assert_eq!(
            response
                .headers()
                .get("ursula-max-request-bytes")
                .map(|value| value.to_str().expect("ascii")),
            expected,
            "{path}"
        );
    }
    let refused = gateway.handle(post_path("/bucket/refused")).await;
    assert_eq!(named_limit(&refused), Some(SizeLimit::Request(1_048_576)));
}

/// A request body that fails while the gateway buffers it, as when the
/// client goes away mid-upload, is answered `400` before anything is
/// forwarded.
#[tokio::test]
async fn gateway_refuses_an_unreadable_body_before_forwarding() {
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
    let gateway = gateway_for_url(upstream.url.clone());
    let body = Body::from_stream(tokio_stream::iter([Err::<bytes::Bytes, _>(
        std::io::Error::other("client went away"),
    )]));
    let request = Request::builder()
        .method("POST")
        .uri("/bucket/stream")
        .body(body)
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

/// A token issuer or an authorizer that is down is answered `503` before
/// anything is forwarded, like a denial.
#[tokio::test]
async fn access_control_outages_and_denials_are_refused_before_forwarding() {
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
    let issuer_down = Arc::new(FixedPrincipalResolver {
        calls: AtomicUsize::new(0),
        result: Err(AuthenticationError::Unavailable),
    });
    let authorizer_down = Arc::new(RecordingAuthorizer {
        decision: Err(AuthorizationError::Unavailable),
        requests: Mutex::new(Vec::new()),
    });
    let allow = || Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let valid = || Arc::new(FixedPrincipalResolver::valid());
    let cases = [
        (
            "token issuer down",
            gateway_with_access_control(upstream.url.clone(), issuer_down, allow()),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            "authorizer down",
            gateway_with_access_control(upstream.url.clone(), valid(), authorizer_down),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            "denied",
            gateway_with_access_control(
                upstream.url.clone(),
                valid(),
                Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Deny)),
            ),
            StatusCode::FORBIDDEN,
        ),
    ];

    for (case, gateway, status) in cases {
        let request = Request::builder()
            .method("POST")
            .uri("/owner-a/events")
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(Body::from("abc"))
            .expect("request");
        let response = gateway.handle(request).await;
        assert_eq!(response.status(), status, "{case}");
    }
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

fn post_path(path: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .body(Body::from("abc"))
        .expect("request")
}

/// A Raft leader redirect to `location`, with `headers` added.
fn raft_redirect(location: &str, mut headers: HeaderMap) -> (StatusCode, HeaderMap) {
    headers.insert(
        LOCATION,
        HeaderValue::from_str(location).expect("redirect target"),
    );
    headers.insert(
        names::X_URSULA_RAFT_LEADER_ID,
        HeaderValue::from_static("1"),
    );
    (StatusCode::TEMPORARY_REDIRECT, headers)
}

/// An upstream that answers every request on `/bucket/stream` with `status`
/// and `headers`.
async fn spawn_answering(status: StatusCode, headers: HeaderMap) -> TestUpstream {
    spawn_upstream(Router::new().route(
        "/bucket/stream",
        any(move || {
            let headers = headers.clone();
            async move { (status, headers, "answer") }
        }),
    ))
    .await
}

/// A leader address that is not one of the gateway's upstreams.
const UNKNOWN_LEADER: &str = "http://192.0.2.1:4437/bucket/stream";

/// A redirect the gateway cannot follow, here because its leader is not an
/// upstream and no other upstream is left to try, never reaches the client.
/// It gets a retryable `503` instead, without a `Location`.
#[tokio::test]
async fn a_redirect_the_gateway_cannot_follow_ends_in_a_503() {
    let (status, headers) = raft_redirect(UNKNOWN_LEADER, HeaderMap::new());
    let node = spawn_answering(status, headers).await;
    let gateway = gateway_for_url(node.url.clone());

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(RETRY_AFTER).expect("retry-after"),
        "1"
    );
    assert!(response.headers().get(LOCATION).is_none());
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_redirects, 0);
    assert_eq!(metrics.leader_not_found, 1);
}

/// A connect failure is a `503`, on the first hop and on the hop after a
/// redirect.
#[tokio::test]
async fn a_connect_failure_answers_503_on_the_first_hop_and_after_a_redirect() {
    let (gone, _refusing) = refusing_upstream().await;

    let first_hop = gateway_for_url(gone.clone())
        .handle(post_path("/bucket/stream"))
        .await;

    assert_eq!(first_hop.status(), StatusCode::SERVICE_UNAVAILABLE);

    let (status, headers) = raft_redirect(&format!("{gone}/bucket/stream"), HeaderMap::new());
    let follower = spawn_answering(status, headers).await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), gone.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), follower.url.clone());

    let leader_hop = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(leader_hop.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(gateway.metrics_snapshot().leader_redirects, 1);
}

/// A node's answer on `/bucket/stream`, as a test scripts it: a status,
/// headers, and how long the node waits before it answers.
#[derive(Clone)]
struct Script {
    status: StatusCode,
    headers: HeaderMap,
    delay: Duration,
}

/// An upstream that answers every request on `/bucket/stream` as its script
/// says, and counts them. The script is set once every upstream of a test is
/// bound, so that redirects can name each other. Each request's arrival is
/// also signalled ([`Scripted::arrived`]), for tests that move a paused clock.
struct Scripted {
    upstream: TestUpstream,
    script: Arc<Mutex<Script>>,
    hits: Arc<AtomicUsize>,
    arrivals: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>,
}

impl Scripted {
    async fn spawn() -> Self {
        let script = Arc::new(Mutex::new(Script {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            delay: Duration::ZERO,
        }));
        let hits = Arc::new(AtomicUsize::new(0));
        let (arrived, arrivals) = tokio::sync::mpsc::unbounded_channel();
        let (answers, counts) = (Arc::clone(&script), Arc::clone(&hits));
        let upstream = spawn_upstream(Router::new().route(
            "/bucket/stream",
            any(move || {
                let (script, hits, arrived) =
                    (Arc::clone(&answers), Arc::clone(&counts), arrived.clone());
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    let Script {
                        status,
                        headers,
                        delay,
                    } = script.lock().unwrap().clone();
                    // The test may have stopped waiting for arrivals.
                    let _ignored = arrived.send(());
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    (status, headers, "scripted")
                }
            }),
        ))
        .await;
        Self {
            upstream,
            script,
            hits,
            arrivals: tokio::sync::Mutex::new(arrivals),
        }
    }

    /// Waits until a request reaches this upstream.
    async fn arrived(&self) {
        self.arrivals
            .lock()
            .await
            .recv()
            .await
            .expect("the upstream is up");
    }

    fn url(&self) -> String {
        self.upstream.url.clone()
    }

    fn answers(&self, (status, headers): (StatusCode, HeaderMap)) {
        self.answers_after(Duration::ZERO, (status, headers));
    }

    fn answers_after(&self, delay: Duration, (status, headers): (StatusCode, HeaderMap)) {
        *self.script.lock().unwrap() = Script {
            status,
            headers,
            delay,
        };
    }

    /// A Raft redirect to this upstream.
    fn redirect_here(&self) -> (StatusCode, HeaderMap) {
        raft_redirect(&format!("{}/bucket/stream", self.url()), HeaderMap::new())
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

/// A gateway over `upstreams` whose first hop goes to the first of them, the
/// cached route of `/bucket/stream`.
fn gateway_starting_at(upstreams: &[&Scripted]) -> Gateway {
    gateway_with(
        test_config(upstreams.iter().map(|upstream| upstream.url()).collect()),
        upstreams[0],
    )
}

/// A gateway with `config` whose first hop goes to `first`, the cached route
/// of `/bucket/stream`.
fn gateway_with(config: GatewayConfig, first: &Scripted) -> Gateway {
    let gateway = Gateway::new(config);
    gateway.remember_leader("/bucket/stream".to_owned(), first.url());
    gateway
}

/// Sends `request` through `gateway` on a task of its own, so that a test
/// can move a paused clock while it is under way.
fn send(gateway: &Arc<Gateway>, request: Request<Body>) -> tokio::task::JoinHandle<AxumResponse> {
    let gateway = Arc::clone(gateway);
    tokio::spawn(async move { gateway.handle(request).await })
}

/// Waits until `arrival` happens, an upstream getting the request under way.
/// The request ending first fails the test, instead of a wait on a paused
/// clock that never ends.
async fn until_reached(
    arrival: impl Future<Output = ()>,
    request: &mut tokio::task::JoinHandle<AxumResponse>,
) {
    tokio::select! {
        () = arrival => {}
        ended = request => panic!(
            "the request ended before it reached the upstream: {:?}",
            ended.map(|response| response.status())
        ),
    }
}

/// The route the gateway caches for `/bucket/stream`.
fn cached_route(gateway: &Gateway) -> Option<String> {
    gateway
        .leader_affinity
        .lock()
        .unwrap()
        .get("/bucket/stream")
        .cloned()
}

/// A node's `503` refusal.
fn refusal() -> (StatusCode, HeaderMap) {
    (StatusCode::SERVICE_UNAVAILABLE, HeaderMap::new())
}

/// The gateway follows a chain of redirects to the answer of the node that
/// does not redirect. The route it caches is that node, unless it answered
/// `503`. The redirecting nodes' routes are dropped.
#[tokio::test]
async fn a_chain_of_redirects_reaches_the_answering_node() {
    let cases = [
        ("503", refusal()),
        ("200", (StatusCode::OK, HeaderMap::new())),
        ("404", (StatusCode::NOT_FOUND, HeaderMap::new())),
    ];
    for (name, answer) in cases {
        let (a, b, leader) = (
            Scripted::spawn().await,
            Scripted::spawn().await,
            Scripted::spawn().await,
        );
        a.answers(b.redirect_here());
        b.answers(leader.redirect_here());
        let status = answer.0;
        leader.answers(answer);
        let gateway = gateway_starting_at(&[&a, &b, &leader]);

        let response = gateway.handle(post_path("/bucket/stream")).await;

        assert_eq!(response.status(), status, "{name}");
        let metrics = gateway.metrics_snapshot();
        assert_eq!(metrics.leader_redirects, 2, "{name}");
        assert_eq!(metrics.leader_searches, 0, "{name}");
        assert_eq!((a.hits(), b.hits(), leader.hits()), (1, 1, 1), "{name}");
        let learned = status.is_success().then(|| leader.url());
        assert_eq!(cached_route(&gateway), learned, "{name}");
    }
}

/// A redirect from a node that hosts no replica of the group names the
/// group alone (#581 section 5.7). The gateway follows the host its
/// `Location` names as a hint, and the client never sees the redirect.
#[tokio::test]
async fn the_gateway_follows_a_redirect_from_a_node_that_hosts_no_replica() {
    let (outsider, replica) = (Scripted::spawn().await, Scripted::spawn().await);
    let mut headers = HeaderMap::new();
    headers.insert(
        LOCATION,
        HeaderValue::from_str(&format!("{}/bucket/stream", replica.url())).unwrap(),
    );
    headers.insert(names::X_URSULA_RAFT_GROUP, HeaderValue::from_static("3"));
    outsider.answers((StatusCode::TEMPORARY_REDIRECT, headers));
    replica.answers((StatusCode::OK, HeaderMap::new()));
    let gateway = gateway_starting_at(&[&outsider, &replica]);

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!((outsider.hits(), replica.hits()), (1, 1));
    assert_eq!(gateway.metrics_snapshot().leader_redirects, 1);
}

/// A redirect whose `Location` is not one of the upstreams (`--upstream`
/// names the nodes otherwise than their peer URLs) sends the request to the
/// next upstream it has not tried, in the configured order, skipping
/// ejected ones. When none is left, the client gets a `503`.
#[tokio::test]
async fn a_redirect_to_an_unknown_address_searches_the_upstreams_not_yet_reached() {
    let unknown = || raft_redirect(UNKNOWN_LEADER, HeaderMap::new());

    // Found: the third upstream answers, and the route to it is learned.
    let (a, b, leader) = (
        Scripted::spawn().await,
        Scripted::spawn().await,
        Scripted::spawn().await,
    );
    a.answers(unknown());
    b.answers(unknown());
    leader.answers(refusal());
    let gateway = gateway_starting_at(&[&a, &b, &leader]);
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let metrics = gateway.metrics_snapshot();
    assert_eq!((metrics.leader_redirects, metrics.leader_searches), (2, 2));
    leader.answers((StatusCode::OK, HeaderMap::new()));
    gateway.remember_leader("/bucket/stream".to_owned(), a.url());
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(cached_route(&gateway), Some(leader.url()));

    // An ejected upstream is skipped.
    b.answers(unknown());
    leader.answers((StatusCode::OK, HeaderMap::new()));
    let gateway = gateway_starting_at(&[&a, &b, &leader]);
    gateway.ejections.failed(&b.url(), Instant::now());
    let before = b.hits();
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(b.hits(), before);
    assert_eq!(gateway.metrics_snapshot().leader_searches, 1);

    // Not found: every upstream redirects to an unknown address.
    let (a, b) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(unknown());
    b.answers(unknown());
    let gateway = gateway_starting_at(&[&a, &b]);
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get(LOCATION).is_none());
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_searches, 1);
    assert_eq!(metrics.leader_not_found, 1);
    assert_eq!(cached_route(&gateway), None);
}

/// The gateway follows three redirects at most. Two nodes that each name
/// the other as the leader get the request twice each, and the client a
/// `503`.
#[tokio::test]
async fn the_gateway_follows_three_redirects_at_most() {
    let (a, b) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(b.redirect_here());
    b.answers(a.redirect_here());
    let gateway = gateway_starting_at(&[&a, &b]);

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(RETRY_AFTER).expect("retry-after"),
        "1"
    );
    assert_eq!((a.hits(), b.hits()), (2, 2));
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_redirects, 3);
    assert_eq!(metrics.leader_not_found, 1);
    assert_eq!(cached_route(&gateway), None);
}

/// The deadline counts from the first redirect, on a paused clock. A slow
/// first hop does not use it up: its redirect is followed. Once the deadline
/// has passed since the first redirect, no further hop starts: after two hops
/// of more than half of it each, the second node's redirect is not followed,
/// and the client gets a `503`. A hop under way runs to its answer, which the
/// client gets.
#[tokio::test(start_paused = true)]
async fn the_redirect_deadline_counts_from_the_first_redirect() {
    let _frozen = FrozenClock::hold();
    let patient = |upstreams: &[&Scripted]| {
        let mut config = test_config(upstreams.iter().map(|upstream| upstream.url()).collect());
        config.response_header_timeout = Duration::from_secs(60);
        Arc::new(gateway_with(config, upstreams[0]))
    };
    let over = LEADER_REDIRECT_DEADLINE.saturating_add(Duration::from_secs(1));
    let more_than_half = LEADER_REDIRECT_DEADLINE
        .checked_div(2)
        .unwrap()
        .saturating_add(Duration::from_millis(500));

    // A slow first hop: its redirect is followed.
    let (a, leader) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers_after(over, leader.redirect_here());
    let gateway = patient(&[&a, &leader]);
    let mut request = send(&gateway, post_path("/bucket/stream"));
    until_reached(a.arrived(), &mut request).await;
    tokio::time::advance(over).await;
    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(leader.hits(), 1);
    assert_eq!(cached_route(&gateway), Some(leader.url()));

    // Two hops of more than half the deadline each: the third redirect is
    // not followed.
    let (a, b, c, d) = (
        Scripted::spawn().await,
        Scripted::spawn().await,
        Scripted::spawn().await,
        Scripted::spawn().await,
    );
    a.answers(b.redirect_here());
    b.answers_after(more_than_half, c.redirect_here());
    c.answers_after(more_than_half, d.redirect_here());
    let gateway = patient(&[&a, &b, &c, &d]);
    let mut request = send(&gateway, post_path("/bucket/stream"));
    until_reached(b.arrived(), &mut request).await;
    tokio::time::advance(more_than_half).await;
    until_reached(c.arrived(), &mut request).await;
    tokio::time::advance(more_than_half).await;
    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!((c.hits(), d.hits()), (1, 0));
    assert_eq!(gateway.metrics_snapshot().leader_redirects, 2);

    // A hop under way when the deadline passes answers.
    let (a, leader) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(leader.redirect_here());
    leader.answers_after(over, (StatusCode::OK, HeaderMap::new()));
    let gateway = patient(&[&a, &leader]);
    let mut request = send(&gateway, post_path("/bucket/stream"));
    until_reached(leader.arrived(), &mut request).await;
    tokio::time::advance(over).await;
    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(cached_route(&gateway), Some(leader.url()));
}

/// A connect failure after redirects is a `503`. The upstream is ejected,
/// and no route is learned.
#[tokio::test]
async fn a_connect_failure_after_redirects_ejects_the_upstream() {
    let (gone, _refusing) = refusing_upstream().await;
    let (a, b) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(b.redirect_here());
    b.answers(raft_redirect(
        &format!("{gone}/bucket/stream"),
        HeaderMap::new(),
    ));
    let gateway = Gateway::new(test_config(vec![a.url(), b.url(), gone.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), a.url());

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_redirects, 2);
    assert_eq!(metrics.upstream_ejections, 1);
    assert_eq!(cached_route(&gateway), None);
}

/// A leader hop that sends no response headers in time is a `504`. No route
/// is learned.
#[tokio::test(start_paused = true)]
async fn a_leader_hop_that_times_out_is_a_504_and_teaches_no_route() {
    let _frozen = FrozenClock::hold();
    let a = Scripted::spawn().await;
    let (silent, mut arrivals) = silent_upstream().await;
    a.answers(raft_redirect(
        &format!("{}/bucket/stream", silent.url),
        HeaderMap::new(),
    ));
    let gateway = Arc::new(gateway_with(
        test_config(vec![a.url(), silent.url.clone()]),
        &a,
    ));

    let mut request = send(&gateway, post_path("/bucket/stream"));
    until_reached(
        async {
            arrivals.recv().await.expect("the upstream is up");
        },
        &mut request,
    )
    .await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let response = request.await.unwrap();

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(gateway.metrics_snapshot().leader_redirects, 1);
    assert_eq!(cached_route(&gateway), None);
    assert!(arrivals.try_recv().is_err());
}

/// A search hop that cannot connect sent nothing, so the search goes on to
/// the next upstream the request has not tried, and the one it could not
/// connect to is ejected. With none left, the client gets the `503` of that
/// connect failure. A search hop whose upstream read the whole request and
/// closed the connection is a `502`: that node may have proposed the write.
#[tokio::test]
async fn a_search_hop_that_cannot_connect_searches_on_and_one_that_was_sent_is_a_502() {
    let unknown = || raft_redirect(UNKNOWN_LEADER, HeaderMap::new());
    let (gone, _refusing) = refusing_upstream().await;

    let (a, leader) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(unknown());
    let gateway = gateway_with(test_config(vec![a.url(), gone.clone(), leader.url()]), &a);
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(
        (metrics.leader_searches, metrics.upstream_ejections),
        (2, 1)
    );
    assert_eq!(cached_route(&gateway), Some(leader.url()));

    let a = Scripted::spawn().await;
    a.answers(unknown());
    let gateway = gateway_with(test_config(vec![a.url(), gone.clone()]), &a);
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(cached_route(&gateway), None);

    let (node, request) = hanging_up_upstream().await;
    let a = Scripted::spawn().await;
    a.answers(unknown());
    let gateway = gateway_with(test_config(vec![a.url(), node.url.clone()]), &a);
    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(gateway.metrics_snapshot().leader_searches, 1);
    let request = request.await.expect("the upstream read a request");
    let request = String::from_utf8(request).expect("an ASCII request");
    assert!(request.ends_with("\r\n\r\nabc"), "{request}");
}

/// A search hop whose upstream read the whole request and closed the
/// connection ends the request with a `502`, even with an upstream left that
/// the request has not tried: that node may have proposed the write. The
/// upstream after it never gets the request.
#[tokio::test]
async fn a_search_hop_that_was_sent_ends_the_request_with_upstreams_left() {
    let (node, request) = hanging_up_upstream().await;
    let (a, leader) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(raft_redirect(UNKNOWN_LEADER, HeaderMap::new()));
    leader.answers(refusal());
    let gateway = gateway_with(
        test_config(vec![a.url(), node.url.clone(), leader.url()]),
        &a,
    );

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(leader.hits(), 0);
    assert_eq!(gateway.metrics_snapshot().leader_searches, 1);
    assert_eq!(cached_route(&gateway), None);
    let request = request.await.expect("the upstream read a request");
    let request = String::from_utf8(request).expect("an ASCII request");
    assert!(request.ends_with("\r\n\r\nabc"), "{request}");
}

/// The same for a search hop that sends no response headers in time, on a
/// paused clock: a `504`, and the upstream after it never
/// gets the request. Its timeout ends well before the redirect deadline, so
/// the deadline does not stop the search instead.
#[tokio::test(start_paused = true)]
async fn a_search_hop_that_times_out_ends_the_request_with_upstreams_left() {
    let _frozen = FrozenClock::hold();
    let timeout = Duration::from_secs(2);
    assert!(timeout < LEADER_REDIRECT_DEADLINE);
    let (silent, mut arrivals) = silent_upstream().await;
    let (a, leader) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(raft_redirect(UNKNOWN_LEADER, HeaderMap::new()));
    leader.answers(refusal());
    let mut config = test_config(vec![a.url(), silent.url.clone(), leader.url()]);
    config.response_header_timeout = timeout;
    let gateway = Arc::new(gateway_with(config, &a));

    let mut request = send(&gateway, post_path("/bucket/stream"));
    until_reached(
        async {
            arrivals.recv().await.expect("the upstream is up");
        },
        &mut request,
    )
    .await;
    tokio::time::advance(timeout).await;
    let response = request.await.unwrap();

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(leader.hits(), 0);
    assert_eq!(gateway.metrics_snapshot().leader_searches, 1);
    assert_eq!(cached_route(&gateway), None);
}

/// Search hops that cannot connect count toward the three hops a request
/// follows: after three of them, the client gets the `503` of the last
/// connect failure, and the upstream after them never gets the request.
#[tokio::test]
async fn a_search_that_cannot_connect_stops_after_three_hops() {
    let (gone, _refusing) = refusing_upstreams(3).await;
    let (a, leader) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(raft_redirect(UNKNOWN_LEADER, HeaderMap::new()));
    let mut upstreams = vec![a.url()];
    upstreams.extend(gone.iter().cloned());
    upstreams.push(leader.url());
    let gateway = gateway_with(test_config(upstreams), &a);

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(leader.hits(), 0);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(
        (
            metrics.leader_redirects,
            metrics.leader_searches,
            metrics.upstream_ejections
        ),
        (3, 3, 3)
    );
}

/// An upstream a redirect named that cannot be connected to ends the request,
/// with the upstream after it left: only a search hop searches on. The client
/// gets the `503` of the connect failure.
#[tokio::test]
async fn a_named_upstream_that_cannot_connect_ends_the_request() {
    let (gone, _refusing) = refusing_upstream().await;
    let (a, b, c) = (
        Scripted::spawn().await,
        Scripted::spawn().await,
        Scripted::spawn().await,
    );
    a.answers(b.redirect_here());
    b.answers(raft_redirect(
        &format!("{gone}/bucket/stream"),
        HeaderMap::new(),
    ));
    let gateway = gateway_with(test_config(vec![a.url(), b.url(), gone, c.url()]), &a);

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(c.hits(), 0);
    let metrics = gateway.metrics_snapshot();
    assert_eq!((metrics.leader_redirects, metrics.leader_searches), (2, 0));
}

/// A redirect from a node that hosts no replica of the group, naming
/// `host` (`307` with `x-ursula-raft-group` alone).
fn not_hosted_redirect(host: &str) -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    headers.insert(
        LOCATION,
        HeaderValue::from_str(&format!("{host}/bucket/stream")).expect("redirect target"),
    );
    headers.insert(names::X_URSULA_RAFT_GROUP, HeaderValue::from_static("3"));
    (StatusCode::TEMPORARY_REDIRECT, headers)
}

/// A listener whose accept queue is full: it never accepts, and a connect
/// to it gets no answer, as one to a blackholed host does, until the
/// connector's timeout. Linux and macOS both drop a `SYN` to a full queue.
async fn blackholed_upstream() -> (String, Blackholed) {
    let socket = tokio::net::TcpSocket::new_v4().expect("socket");
    socket
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind");
    let addr = socket.local_addr().expect("local addr");
    let listener = socket.listen(1).expect("listen");
    let mut queued = Vec::new();
    loop {
        match tokio::time::timeout(
            Duration::from_millis(200),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        {
            Ok(connected) => queued.push(connected.expect("a queued connect")),
            Err(_elapsed) => break,
        }
        assert!(queued.len() < 64, "the accept queue never filled");
    }
    (format!("http://{addr}"), Blackholed {
        _listener: listener,
        _queued: queued,
    })
}

/// Holds [`blackholed_upstream`]'s listener and the connects that fill its
/// queue.
struct Blackholed {
    _listener: tokio::net::TcpListener,
    _queued: Vec<tokio::net::TcpStream>,
}

/// A `NotHosted` redirect names a host as a hint, often the dead node its
/// group moves away from. When the named upstream cannot be connected to,
/// stopped or blackholed, the request goes on to an upstream it has not
/// tried and reaches the hosting one within the hop budget, unlike a leader
/// redirect's target ([`a_named_upstream_that_cannot_connect_ends_the_request`]).
///
/// The connect timeout is lowered to 500 ms because the search after a
/// blackholed hint starts only when the failed connect ends inside
/// `LEADER_REDIRECT_DEADLINE` (5 s). With the default `--connect-timeout 5`
/// the request that pays the blackholed connect ends with `503`, and only
/// later requests pass the ejected host
/// ([`a_blackholed_not_hosted_hint_costs_one_connect_before_its_ejection`]).
#[tokio::test]
async fn a_not_hosted_hint_that_cannot_connect_searches_on_to_the_host() {
    let (stopped, _refusing) = refusing_upstream().await;
    let (blackholed, _blackholed) = blackholed_upstream().await;
    for (name, dead) in [("stopped", stopped), ("blackholed", blackholed)] {
        let (outsider, host) = (Scripted::spawn().await, Scripted::spawn().await);
        outsider.answers(not_hosted_redirect(&dead));
        let mut config = test_config(vec![outsider.url(), dead.clone(), host.url()]);
        config.connect_timeout = Duration::from_millis(500);
        let gateway = gateway_with(config, &outsider);

        let response = gateway.handle(post_path("/bucket/stream")).await;

        assert_eq!(response.status(), StatusCode::OK, "{name}");
        assert_eq!((outsider.hits(), host.hits()), (1, 1), "{name}");
        let metrics = gateway.metrics_snapshot();
        assert_eq!(
            (
                metrics.leader_redirects,
                metrics.leader_searches,
                metrics.upstream_ejections
            ),
            (2, 1, 1),
            "{name}"
        );
        assert_eq!(cached_route(&gateway), Some(host.url()), "{name}");
    }
}

/// A blackholed host that `NotHosted` redirects keep naming costs one
/// connect: it is ejected, and later hints to it go to an upstream not
/// ejected instead, with no connect to it, while its window lasts.
#[tokio::test]
async fn a_blackholed_not_hosted_hint_costs_one_connect_before_its_ejection() {
    let (blackholed, _blackholed) = blackholed_upstream().await;
    let (outsider, host) = (Scripted::spawn().await, Scripted::spawn().await);
    outsider.answers(not_hosted_redirect(&blackholed));
    let connect_timeout = Duration::from_millis(500);
    let mut config = test_config(vec![outsider.url(), blackholed.clone(), host.url()]);
    config.connect_timeout = connect_timeout;
    let gateway = gateway_with(config, &outsider);

    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(
        (metrics.leader_redirects, metrics.upstream_ejections),
        (2, 1)
    );

    for i in 1..=3 {
        gateway.remember_leader("/bucket/stream".to_owned(), outsider.url());
        let started = std::time::Instant::now();

        let response = gateway.handle(post_path("/bucket/stream")).await;

        assert_eq!(response.status(), StatusCode::OK, "request {i}");
        assert!(started.elapsed() < connect_timeout, "request {i}");
        let metrics = gateway.metrics_snapshot();
        // One hop each, a search hop past the ejected hint.
        assert_eq!(
            (
                metrics.leader_redirects,
                metrics.leader_searches,
                metrics.upstream_ejections
            ),
            (2 + i, 1 + i, 1),
            "request {i}"
        );
    }
    assert_eq!((outsider.hits(), host.hits()), (4, 4));
}

/// A search starts at the upstream after the one that redirected, in the
/// configured order and around, so that the first search hop of every miss
/// does not go to the same upstream.
#[tokio::test]
async fn a_search_starts_after_the_upstream_that_redirected() {
    let (x, a, leader) = (
        Scripted::spawn().await,
        Scripted::spawn().await,
        Scripted::spawn().await,
    );
    x.answers(raft_redirect(UNKNOWN_LEADER, HeaderMap::new()));
    a.answers(raft_redirect(UNKNOWN_LEADER, HeaderMap::new()));
    let gateway = gateway_with(test_config(vec![x.url(), a.url(), leader.url()]), &a);

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!((x.hits(), a.hits(), leader.hits()), (0, 1, 1));
}

/// A redirect that names an upstream the request already went to sends it
/// to one it has not tried instead, so that two nodes with stale views do
/// not use the hops up between them.
#[tokio::test]
async fn a_redirect_back_to_an_upstream_already_reached_searches_instead() {
    let (a, b, leader) = (
        Scripted::spawn().await,
        Scripted::spawn().await,
        Scripted::spawn().await,
    );
    a.answers(b.redirect_here());
    b.answers(a.redirect_here());
    let gateway = gateway_starting_at(&[&a, &b, &leader]);

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!((a.hits(), b.hits(), leader.hits()), (1, 1, 1));
    let metrics = gateway.metrics_snapshot();
    assert_eq!((metrics.leader_redirects, metrics.leader_searches), (2, 1));
}

/// A `Location` names an upstream only at a boundary: the upstream's address
/// followed by a path, a query or nothing, whether or not `--upstream` ends
/// in a slash.
#[test]
fn a_location_names_an_upstream_only_at_a_boundary() {
    let gateway = Gateway::new(test_config(vec![
        "http://127.0.0.1:1234".to_owned(),
        "http://node.test/".to_owned(),
    ]));
    let named = |location: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(LOCATION, HeaderValue::from_str(location).unwrap());
        gateway.resolve_leader_upstream(&headers).map(str::to_owned)
    };
    assert_eq!(named("http://127.0.0.1:12345/bucket/stream"), None);
    assert_eq!(named("http://127.0.0.1:1234.evil/bucket/stream"), None);
    assert_eq!(
        named("http://127.0.0.1:1234/bucket/stream").as_deref(),
        Some("http://127.0.0.1:1234")
    );
    assert_eq!(
        named("http://127.0.0.1:1234?x=1").as_deref(),
        Some("http://127.0.0.1:1234")
    );
    assert_eq!(
        named("http://127.0.0.1:1234").as_deref(),
        Some("http://127.0.0.1:1234")
    );
    assert_eq!(
        named("http://node.test/bucket/stream").as_deref(),
        Some("http://node.test/")
    );
    assert_eq!(named("http://node.testing/bucket/stream"), None);
}

/// Accepts one connection, reads one whole request from it, the head and the
/// `Content-Length` body, and closes it without answering, as a node that
/// went away after it received a write does. The bytes it read are sent on
/// the returned channel.
async fn hanging_up_upstream() -> (TestUpstream, tokio::sync::oneshot::Receiver<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let (read, request) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut connection, _peer) = listener.accept().await.expect("accept");
        let mut bytes = Vec::new();
        while !is_whole_request(&bytes) {
            let mut chunk = [0_u8; 1024];
            let len = tokio::io::AsyncReadExt::read(&mut connection, &mut chunk)
                .await
                .expect("read the request");
            assert_ne!(len, 0, "the gateway closed before it sent the request");
            bytes.extend_from_slice(&chunk[..len]);
        }
        drop(connection);
        read.send(bytes).expect("the test awaits the request");
    });
    (TestUpstream { url, task }, request)
}

/// Whether `bytes` hold a whole request: its head and as many body bytes as
/// its `Content-Length` names.
fn is_whole_request(bytes: &[u8]) -> bool {
    let Some(head_len) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let head = std::str::from_utf8(&bytes[..head_len]).expect("an ASCII request head");
    let body_len = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("a Content-Length"))
        })
        .unwrap_or(0);
    bytes.len() >= head_len.saturating_add(4).saturating_add(body_len)
}

/// A first-hop failure after the request was handed to a connection is a
/// `502`. Here the upstream reads the whole request, body included, and
/// closes the connection without answering.
#[tokio::test]
async fn a_first_hop_failure_after_the_request_was_sent_is_a_502() {
    let (node, request) = hanging_up_upstream().await;

    let response = gateway_for_url(node.url.clone())
        .handle(post_path("/bucket/stream"))
        .await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let request = request.await.expect("the upstream read a request");
    let request = String::from_utf8(request).expect("an ASCII request");
    assert!(request.starts_with("POST /bucket/stream "), "{request}");
    assert!(request.ends_with("\r\n\r\nabc"), "{request}");
}

/// The same after redirects: a hop whose upstream read the whole request and
/// closed the connection without answering is a `502`. No route is learned.
#[tokio::test]
async fn a_failure_after_the_request_was_sent_is_a_502_after_redirects() {
    let (node, request) = hanging_up_upstream().await;
    let (a, b) = (Scripted::spawn().await, Scripted::spawn().await);
    a.answers(b.redirect_here());
    b.answers(raft_redirect(
        &format!("{}/bucket/stream", node.url),
        HeaderMap::new(),
    ));
    let gateway = Gateway::new(test_config(vec![a.url(), b.url(), node.url.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), a.url());

    let response = gateway.handle(post_path("/bucket/stream")).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(gateway.metrics_snapshot().leader_redirects, 2);
    assert_eq!(cached_route(&gateway), None);
    let request = request.await.expect("the upstream read a request");
    let request = String::from_utf8(request).expect("an ASCII request");
    assert!(request.starts_with("POST /bucket/stream "), "{request}");
    assert!(request.ends_with("\r\n\r\nabc"), "{request}");
}

/// No response the gateway returns carries `x-ursula-not-applied` or
/// `x-ursula-not-applied-checked`, neither a forwarded answer nor one of its
/// own refusals.
#[tokio::test]
async fn no_gateway_response_carries_a_not_applied_header() {
    let node = spawn_upstream(Router::new().route("/bucket/ok", any(|| async { "ok" }))).await;
    let mut small_bodies = test_config(vec![node.url.clone()]);
    set_body_limit(&mut small_bodies, 2);

    let cases = [
        (
            "forwarded",
            gateway_for_url(node.url.clone())
                .handle(post_path("/bucket/ok"))
                .await,
            StatusCode::OK,
        ),
        (
            "own refusal",
            Gateway::new(small_bodies)
                .handle(post_path("/bucket/ok"))
                .await,
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "no upstream",
            Gateway::new(test_config(Vec::new()))
                .handle(post_path("/bucket/ok"))
                .await,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ];
    for (case, response, status) in cases {
        assert_eq!(response.status(), status, "{case}");
        for name in ["x-ursula-not-applied", "x-ursula-not-applied-checked"] {
            assert!(!response.headers().contains_key(name), "{case}: {name}");
        }
    }
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
        .forward(&follower.url, &parts, body_bytes, ResponseTail::default())
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
        .forward(&follower.url, &parts, body_bytes, ResponseTail::default())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

/// A redirect to a leader that is not an upstream, from the only upstream,
/// leaves nowhere to look: the client gets a retryable `503`, never the
/// redirect, so the leader's address does not leak.
#[tokio::test]
async fn gateway_answers_503_when_a_redirect_names_no_upstream_and_none_is_left() {
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
        .forward(&follower.url, &parts, body_bytes, ResponseTail::default())
        .await;

    assert!(matches!(
        resp,
        Err(GatewayError::LeaderNotFound { followed: 0 })
    ));
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
        get(|uri: Uri| async move {
            (
                StatusCode::OK,
                uri.path_and_query()
                    .map(axum::http::uri::PathAndQuery::as_str)
                    .unwrap_or(uri.path())
                    .to_owned(),
            )
        }),
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
        .forward(&upstream_url, &parts, body_bytes, ResponseTail::default())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body_bytes, "/bucket/stream?offset=now&live=sse");
}

/// A node's answers pass through with their own headers: the gateway adds
/// `Cache-Control` only to answers it sends itself (#640).
#[tokio::test]
async fn a_forwarded_answer_keeps_its_own_cache_control() {
    let app = Router::new()
        .route(
            "/bucket/stream",
            get(|| async { (StatusCode::NOT_FOUND, "missing") }),
        )
        .route(
            "/bucket/cached",
            get(|| async {
                (
                    StatusCode::OK,
                    [(axum::http::header::CACHE_CONTROL, "max-age=60")],
                    "ok",
                )
            }),
        );
    let upstream = spawn_upstream(app).await;
    let gateway = gateway_for_url(upstream.url.clone());
    for (uri, expected) in [
        ("/bucket/stream", None),
        ("/bucket/cached", Some("max-age=60")),
    ] {
        let response = gateway.handle(get_path(uri)).await;
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            expected,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn gateway_forwards_response_content_encoding_without_decoding() {
    const GZIP_HELLO: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 203, 72, 205, 201, 201, 7, 0, 134, 166, 16, 54, 5, 0, 0, 0,
    ];
    let app = Router::new().route(
        "/bucket/stream",
        get(|headers: HeaderMap| async move {
            let accept_encoding = headers
                .get("accept-encoding")
                .cloned()
                .unwrap_or_else(|| "missing".parse().expect("static header"));
            (
                StatusCode::OK,
                [
                    (
                        "content-type",
                        "application/x-ndjson".parse().expect("static header"),
                    ),
                    ("content-encoding", "gzip".parse().expect("static header")),
                    ("x-seen-accept-encoding", accept_encoding),
                ],
                bytes::Bytes::from_static(GZIP_HELLO),
            )
        }),
    );
    let upstream = spawn_upstream(app).await;
    let gateway = gateway_for_url(upstream.url.clone());
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .header("accept-encoding", "gzip")
        .body(Body::empty())
        .expect("build request");

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let response = gateway
        .forward(&upstream.url, &parts, body_bytes, ResponseTail::default())
        .await
        .expect("forward compressed response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-encoding").unwrap(), "gzip");
    assert_eq!(
        response.headers().get("x-seen-accept-encoding").unwrap(),
        "gzip"
    );
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("compressed body"),
        GZIP_HELLO
    );
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
        .forward(
            "https://127.0.0.1:1",
            &parts,
            body_bytes,
            ResponseTail::default(),
        )
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
        .forward(&upstream.url, &parts, body_bytes, ResponseTail::default())
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

// Gateway quota enforcement and usage accounting (#134/#135).
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
        chunked_quota_gateway(upstream_url, collector, None)
    }

    fn chunked_quota_gateway(
        upstream_url: &str,
        collector: Arc<UsageCollector>,
        usage_chunk_bytes: Option<u64>,
    ) -> Gateway {
        metered_gateway(upstream_url, collector, usage_chunk_bytes).with_quota_provider(Arc::new(
            StaticQuotaProvider::from_toml_str(QUOTAS).expect("parse quotas"),
        ))
    }

    /// Access control and usage, no quotas. Counting units needs several
    /// appends of differing sizes, which the shared quota policy's two
    /// requests per second and eight-byte body would reject before the meter
    /// ever saw them.
    fn metered_gateway(
        upstream_url: &str,
        collector: Arc<UsageCollector>,
        usage_chunk_bytes: Option<u64>,
    ) -> Gateway {
        let mut config = test_config(vec![upstream_url.to_owned()]);
        config.usage_chunk_bytes = usage_chunk_bytes;
        Gateway::with_access_control(
            config,
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(StaticPolicyAuthorizer::from_toml_str(POLICY).expect("parse policy")),
            ),
        )
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

    /// An upstream that answers `ok` on `/tenant-a/orders` and counts the
    /// requests that reach it.
    async fn counting_upstream() -> (TestUpstream, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let upstream_hits = Arc::clone(&hits);
        let upstream = spawn_upstream(Router::new().route(
            "/tenant-a/orders",
            any(move || {
                let upstream_hits = Arc::clone(&upstream_hits);
                async move {
                    upstream_hits.fetch_add(1, Ordering::Relaxed);
                    "ok"
                }
            }),
        ))
        .await;
        (upstream, hits)
    }

    #[tokio::test]
    async fn rate_limit_rejects_with_retry_after_and_never_reaches_upstream() {
        let (upstream, hits) = counting_upstream().await;
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
        assert_eq!(
            hits.load(Ordering::Relaxed),
            2,
            "the refusal was not forwarded"
        );
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
        let (upstream, hits) = counting_upstream().await;
        let gateway = quota_gateway(&upstream.url, UsageCollector::new());

        let oversized = send(&gateway, "POST", "/tenant-a/orders", "123456789").await;
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            super::named_limit(&oversized),
            Some(SizeLimit::Request(8)),
            "the tenant's limit, below the gateway's"
        );
        assert_eq!(
            hits.load(Ordering::Relaxed),
            0,
            "the refusal was not forwarded"
        );

        let allowed = send(&gateway, "POST", "/tenant-a/orders", "12345678").await;
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn live_read_limit_rejects_the_second_live_read() {
        let (upstream, hits) = counting_upstream().await;
        let gateway = metered_gateway(&upstream.url, UsageCollector::new(), None)
            .with_quota_provider(Arc::new(
                StaticQuotaProvider::from_toml_str(
                    r#"
                    [[bucket]]
                    id = "tenant-a"
                    max_concurrent_live_reads = 1
                    "#,
                )
                .expect("parse quotas"),
            ));
        let tail = "/tenant-a/orders?offset=now&live=long-poll";

        // The first response holds the tenant's only live-read slot until it
        // is dropped.
        let first = send(&gateway, "GET", tail, "").await;
        assert_eq!(first.status(), StatusCode::OK);
        let second = send(&gateway, "GET", tail, "").await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            hits.load(Ordering::Relaxed),
            1,
            "the refusal was not forwarded"
        );
        drop(first);
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
        // No unit size configured, so nothing to count.
        assert_eq!(append_record.counters.chunks, 0);
        assert_eq!(read_record.counters.chunks, 0);
    }

    #[tokio::test]
    async fn appends_count_in_units_and_reads_never_do() {
        let upstream =
            spawn_upstream(Router::new().route("/tenant-a/orders", any(|| async { "0123456789" })))
                .await;
        let collector = UsageCollector::new();
        // A four-byte unit keeps the arithmetic legible: 8 bytes is exactly
        // two units, 9 is three, and an empty append is still one.
        let gateway = metered_gateway(&upstream.url, Arc::clone(&collector), Some(4));

        for body in ["12345678", "123456789", ""] {
            let append = send(&gateway, "POST", "/tenant-a/orders", body).await;
            assert_eq!(append.status(), StatusCode::OK);
            let _ = append.into_body().collect().await.expect("append body");
        }
        let read = send(&gateway, "GET", "/tenant-a/orders", "").await;
        let _ = read.into_body().collect().await.expect("read body");

        let sink = CapturingSink {
            batches: Mutex::new(Vec::new()),
        };
        crate::usage::flush(&collector, &sink).await;
        let batches = sink.batches.lock().expect("batches lock");
        let records = &batches[0].records;

        let append_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Append)
            .expect("append usage record");
        assert_eq!(append_record.counters.requests, 3);
        assert_eq!(append_record.counters.request_bytes, 17);
        assert_eq!(append_record.counters.chunks, 2 + 3 + 1);

        // A read already pays through `response_bytes`; charging it a write
        // unit as well would bill the same request twice.
        let read_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Read)
            .expect("read usage record");
        assert_eq!(read_record.counters.chunks, 0);
    }
}

mod cors {
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;

    use super::test_config;
    use crate::Gateway;

    const ALLOW_ORIGIN: &str = "access-control-allow-origin";
    const ALLOW_CREDENTIALS: &str = "access-control-allow-credentials";
    const EXPOSE_HEADERS: &str = "access-control-expose-headers";

    fn gateway(origins: &[&str]) -> Gateway {
        let mut config = test_config(vec!["http://upstream.invalid".to_owned()]);
        config.cors_allowed_origins = origins.iter().map(|origin| (*origin).to_owned()).collect();
        Gateway::new(config)
    }

    fn preflight(uri: &str, origin: &str) -> Request<Body> {
        Request::builder()
            .method("OPTIONS")
            .uri(uri)
            .header("origin", origin)
            .header("access-control-request-method", "GET")
            .header("access-control-request-headers", "authorization")
            .body(Body::empty())
            .expect("request")
    }

    /// The property that keeps #133's concealment intact. A preflight arrives
    /// without `Authorization`, so if the reply differed between a real bucket
    /// and a missing one it would be an unauthenticated existence oracle. The
    /// policy is never given the path, but assert the observable behaviour so a
    /// future refactor cannot quietly make it path-dependent.
    #[tokio::test]
    async fn preflight_cannot_distinguish_an_existing_bucket_from_a_missing_one() {
        let gateway = gateway(&["https://app.example"]);

        let existing = gateway
            .handle(preflight("/ten_real/stream", "https://app.example"))
            .await;
        let missing = gateway
            .handle(preflight("/ten_absent/stream", "https://app.example"))
            .await;

        assert_eq!(existing.status(), StatusCode::NO_CONTENT);
        assert_eq!(existing.status(), missing.status());
        assert_eq!(existing.headers(), missing.headers());
    }

    #[tokio::test]
    async fn preflight_from_an_unlisted_origin_carries_no_cors_headers() {
        let gateway = gateway(&["https://app.example"]);

        let response = gateway
            .handle(preflight("/ten_real/stream", "https://evil.example"))
            .await;

        assert!(response.headers().get(ALLOW_ORIGIN).is_none());
    }

    /// `Expose-Headers: *` is what lets a browser read `stream-next-offset` and
    /// the rest of the protocol headers, so a client can paginate at all. It is
    /// only honoured while credentials stay disallowed.
    #[tokio::test]
    async fn credentials_are_never_allowed_so_the_expose_wildcard_holds() {
        let gateway = gateway(&["*"]);

        let response = gateway
            .handle(preflight("/ten_real/stream", "https://any.example"))
            .await;

        assert_eq!(
            response
                .headers()
                .get(ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*")
        );
        assert!(response.headers().get(ALLOW_CREDENTIALS).is_none());
    }

    #[tokio::test]
    async fn an_unconfigured_gateway_adds_nothing() {
        let gateway = gateway(&[]);

        let response = gateway
            .handle(preflight("/ten_real/stream", "https://app.example"))
            .await;

        assert!(response.headers().get(ALLOW_ORIGIN).is_none());
        assert!(response.headers().get(EXPOSE_HEADERS).is_none());
        // Without CORS the request falls through to normal routing rather than
        // being short-circuited as a preflight.
        assert_ne!(response.status(), StatusCode::NO_CONTENT);
    }
}

mod credential_deadline {
    use std::convert::Infallible;

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::routing::get;
    use http_body_util::BodyExt;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    use super::ResponseTail;
    use super::gateway_for_url;
    use super::spawn_upstream;

    /// An upstream that keeps sending SSE events forever, so the only thing that
    /// can end the response is the gateway.
    fn endless_sse() -> Router {
        Router::new().route(
            "/bucket/stream",
            get(|| async {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tokio::spawn(async move {
                    loop {
                        if tx
                            .send(bytes::Bytes::from_static(b"event: data\ndata: tick\n\n"))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                });
                (
                    StatusCode::OK,
                    [("content-type", "text/event-stream")],
                    Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, Infallible>)),
                )
            }),
        )
    }

    fn json_upstream() -> Router {
        Router::new().route(
            "/bucket/stream",
            get(|| async {
                (
                    StatusCode::OK,
                    [("content-type", "application/x-ndjson")],
                    Body::from("{\"record\":1}\n"),
                )
            }),
        )
    }

    fn unix_now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the unix epoch")
            .as_secs()
    }

    async fn forward_with(
        upstream_url: &str,
        tail: ResponseTail,
    ) -> axum::response::Response<Body> {
        let gateway = gateway_for_url(upstream_url.to_owned());
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream")
            .body(Body::empty())
            .expect("build request");
        let (parts, body) = req.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("request body");
        gateway
            .forward(upstream_url, &parts, body_bytes, tail)
            .await
            .expect("forward request")
    }

    /// The point of #203: a subscription must not outlive the credential that
    /// authorized it, and the client must be able to tell that is why it ended.
    #[tokio::test]
    async fn an_sse_subscription_ends_with_a_named_event_when_the_credential_expires() {
        let upstream = spawn_upstream(endless_sse()).await;
        let response = forward_with(&upstream.url, ResponseTail {
            credential_expires_at: Some(unix_now()),
            ..ResponseTail::default()
        })
        .await;

        // Bounded on purpose. The upstream never ends, so if the deadline is not
        // armed this collect would hang — and a hang in CI is a worse signal than
        // a failure, since it reads as flakiness rather than a missing feature.
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            axum::body::to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("the deadline must end the stream rather than leave it open")
        .expect("collect the terminated stream");
        let body = String::from_utf8_lossy(&body);

        assert!(
            body.contains("event: credential-expired"),
            "the stream must say why it ended, not just stop: {body:?}"
        );
        assert!(
            body.ends_with("\n\n"),
            "the terminal frame must be a complete SSE event: {body:?}"
        );
    }

    /// Anonymous public reads carry no credential, so they have no deadline. This
    /// is the regression that would silently break `public_read` tails.
    #[tokio::test]
    async fn an_anonymous_subscription_is_not_given_a_deadline() {
        let upstream = spawn_upstream(endless_sse()).await;
        let response = forward_with(&upstream.url, ResponseTail::default()).await;

        // One frame, not the whole body: the upstream never ends, and that it
        // keeps going is the property under test.
        let mut response = response;
        let first = response
            .body_mut()
            .frame()
            .await
            .expect("first frame")
            .expect("first frame ok")
            .into_data()
            .expect("first frame is data");
        let first = String::from_utf8_lossy(&first);

        assert!(
            first.contains("data: tick"),
            "an anonymous tail should receive upstream data, not a terminal frame: {first:?}"
        );
        assert!(
            !first.contains("credential-expired"),
            "no credential means no deadline: {first:?}"
        );
    }

    /// A credential lapsing mid-transfer must not truncate a body the client is
    /// already committed to reading; only indefinite streams get a deadline.
    #[tokio::test]
    async fn a_non_streaming_response_is_never_truncated() {
        let upstream = spawn_upstream(json_upstream()).await;
        let response = forward_with(&upstream.url, ResponseTail {
            credential_expires_at: Some(unix_now()),
            ..ResponseTail::default()
        })
        .await;

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("collect body");

        assert_eq!(
            String::from_utf8_lossy(&body),
            "{\"record\":1}\n",
            "an already-expired credential must not rewrite a finite response"
        );
    }
}

/// Keepalive derived from the upstream user timeout T (#495).
mod upstream_keepalive {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use super::UpstreamKeepalive;

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    fn for_user_timeout(secs: u64) -> UpstreamKeepalive {
        UpstreamKeepalive::for_user_timeout(NonZeroU64::new(secs).unwrap())
    }

    fn keepalive(idle: u64, interval: u64) -> UpstreamKeepalive {
        UpstreamKeepalive {
            idle: secs(idle),
            interval: secs(interval),
            retries: 3,
        }
    }

    // Below 5 s, both steps stay at the 1 s the kernel can express.
    #[test]
    fn one_second_floors_both_steps() {
        assert_eq!(for_user_timeout(1), keepalive(1, 1));
    }

    // The default: probes after 2, 3 and 4 s of silence, closed at about 5 s.
    #[test]
    fn five_seconds_probes_three_times_before_closing() {
        assert_eq!(for_user_timeout(5), keepalive(2, 1));
    }

    #[test]
    fn thirty_seconds_scales_both_steps() {
        assert_eq!(for_user_timeout(30), keepalive(12, 6));
    }

    // Linux closes an idle connection once T passes with a probe unanswered,
    // so all three probes go out only if the third is sent before T. With
    // steps of at least 1 s that needs T >= 4 s: at T = 3 s the third probe
    // would fall at T.
    #[test]
    fn three_probes_go_out_before_the_user_timeout() {
        for t in 4..=600 {
            let k = for_user_timeout(t);
            let third_probe = k.idle.saturating_add(k.interval.saturating_mul(2));
            assert!(third_probe < secs(t), "T = {t} s: {k:?}");
        }
    }
}

/// The gateway's upstream pool against a node's graceful drain
/// ([`ursula_observability::serve::CLIENT_IDLE_BOUND`]).
mod upstream_drain {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::Router;
    use axum::extract::ConnectInfo;
    use axum::http::StatusCode;
    use axum::routing::any;
    use http_body_util::BodyExt;
    use ursula_observability::serve::serve_until_shutdown;

    use super::gateway_for_url;
    use super::post_path;

    /// A write the gateway sends on its pooled connection to a node after the
    /// node's graceful drain began is answered over that connection, and the
    /// gateway returns the answer. A drain that closed the idle connection at
    /// once failed such a write with `502` when the gateway had just sent it,
    /// and with `503` a moment later, when the node's listener refused the
    /// gateway's new connection.
    #[tokio::test]
    async fn a_draining_node_answers_a_write_on_the_gateways_pooled_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the node");
        let addr = listener.local_addr().expect("the node's address");
        let (peers, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/{bucket}/{stream}",
            any(move |ConnectInfo(peer): ConnectInfo<SocketAddr>| {
                peers.send(peer).expect("the test reads the arrivals");
                async { "ok" }
            }),
        );
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let node = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            async move {
                drop(stopped.await);
            },
            Some(Duration::from_secs(30)),
        ));
        let gateway = gateway_for_url(format!("http://{addr}"));

        let first = gateway.handle(post_path("/bucket/stream")).await;
        assert_eq!(first.status(), StatusCode::OK);
        // Read to its end, the answer gives its connection back to the pool.
        first
            .into_body()
            .collect()
            .await
            .expect("the first answer's body");

        stop.send(()).expect("the node drains");
        // The drain began once the node refuses a new connection. The write
        // follows a moment later.
        while tokio::net::TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        let second = gateway.handle(post_path("/bucket/stream")).await;
        assert_eq!(second.status(), StatusCode::OK);
        let first_peer = arrivals.recv().await.expect("the first write arrived");
        let second_peer = arrivals.recv().await.expect("the second write arrived");
        assert_eq!(
            first_peer, second_peer,
            "both writes went over the pooled connection"
        );
        tokio::time::timeout(Duration::from_secs(10), node)
            .await
            .expect("the drain ends")
            .expect("the node's task")
            .expect("the node served");
    }
}

/// A refusal the gateway decides from a request's head alone reaches a client
/// that sends its whole body before it reads the answer: the gateway reads the
/// body first, up to `max_request_body_bytes`, for
/// [`REFUSED_BODY_DRAIN_TIMEOUT`] at most and until no chunk arrived for
/// [`REFUSED_BODY_STALL_TIMEOUT`] (#540). The clients here are raw
/// connections to the gateway, served as `ursula gateway` serves it.
mod refused_bodies {
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::HeaderMap;
    use axum::http::HeaderName;
    use axum::http::HeaderValue;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::http::header::AUTHORIZATION;
    use bytes::Bytes;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpSocket;
    use tokio::net::TcpStream;
    use tokio::time::Instant;
    use tokio_stream::wrappers::ReceiverStream;
    use ursula_headers::ErrorBody;
    use ursula_headers::ErrorCode;
    use ursula_observability::serve::serve_until_shutdown;

    use super::FixedPrincipalResolver;
    use super::FrozenClock;
    use super::RecordingAuthorizer;
    use super::TestUpstream;
    use super::answering_upstream;
    use super::set_body_limit;
    use super::test_config;
    use crate::Gateway;
    use crate::GatewayConfig;
    use crate::REFUSED_BODY_DRAIN_TIMEOUT;
    use crate::REFUSED_BODY_STALL_TIMEOUT;
    use crate::admission::StaticQuotaProvider;
    use crate::auth::AccessControl;
    use crate::auth::AuthenticationError;
    use crate::auth::AuthorizationDecision;
    use crate::auth::AuthorizationError;
    use crate::service::router;
    use crate::uploads::UploadLimits;
    use crate::uploads::UploadLimitsSpec;

    /// The body the clients announce: 8 MiB, far more than a connection holds
    /// while nothing reads it ([`BUFFER`]). Answered with this body unread, a
    /// client that sends all of it fails to write it before it reads anything.
    const LARGE_BODY: usize = 8 << 20;
    /// Bytes a client writes at a time.
    const CHUNK: usize = 64 << 10;
    /// The body limit of the gateways whose limit a test reaches.
    const LIMIT: usize = 64 << 10;
    /// The send buffer of a client's connection and the receive buffer of the
    /// gateway's. Linux doubles it and then stops tuning it, so how much of a
    /// body the connection holds while nothing reads it does not depend on
    /// the host's settings.
    const BUFFER: u32 = 64 << 10;

    /// How a client announces its body.
    #[derive(Clone, Copy)]
    enum Announce {
        /// `content-length` of this many bytes.
        Length(usize),
        /// `transfer-encoding: chunked`. The client never sends the last
        /// chunk, so the body does not end.
        Chunked,
        /// `transfer-encoding: chunked`, ended with the last chunk once the
        /// client has sent the bytes it sends.
        ChunkedWhole,
    }

    /// A gateway with access control and `config`: a bearer token other than
    /// `valid-token` is refused (`401`), and the authorizer answers
    /// `decision` to every other request.
    fn guarded(
        config: GatewayConfig,
        decision: Result<AuthorizationDecision, AuthorizationError>,
    ) -> Gateway {
        Gateway::with_access_control(
            config,
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(RecordingAuthorizer {
                    decision,
                    requests: Mutex::new(Vec::new()),
                }),
            ),
        )
    }

    /// Serves `gateway` on a loopback port as `ursula gateway` does, its
    /// router under `serve_until_shutdown`, from a listener whose connections
    /// receive into [`BUFFER`]. Dropping the returned server stops it.
    async fn serve(gateway: Arc<Gateway>) -> (SocketAddr, TestUpstream) {
        let socket = TcpSocket::new_v4().expect("a socket");
        socket
            .set_recv_buffer_size(BUFFER)
            .expect("a receive buffer");
        socket
            .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("bind the gateway");
        let listener = socket.listen(1024).expect("listen");
        let addr = listener.local_addr().expect("the gateway's address");
        let task = tokio::spawn(async move {
            serve_until_shutdown(
                listener,
                router(gateway),
                std::future::pending::<()>(),
                None,
            )
            .await
            .expect("serve the gateway");
        });
        let server = TestUpstream {
            url: format!("http://{addr}"),
            task,
        };
        (addr, server)
    }

    /// A client like ureq: on a new connection to `addr` that sends from
    /// [`BUFFER`], it sends a POST to `/owner-a/events` with `bearer`, its
    /// body announced as `announce`, and `sent` bytes of the body (all, or
    /// fewer to stop short), before it reads anything. The answer's status
    /// and headers, or the error that broke the exchange.
    async fn upload(
        addr: SocketAddr,
        bearer: &'static str,
        announce: Announce,
        sent: usize,
    ) -> std::io::Result<(StatusCode, HeaderMap)> {
        let socket = TcpSocket::new_v4()?;
        socket.set_send_buffer_size(BUFFER)?;
        let mut connection = socket.connect(addr).await?;
        write_request(&mut connection, bearer, announce, sent).await?;
        let mut answer = Vec::new();
        loop {
            if let Some(end) = answer.windows(4).position(|window| window == b"\r\n\r\n") {
                return Ok(parse_head(&answer[..end]));
            }
            let mut read = [0_u8; 1024];
            let got = connection.read(&mut read).await?;
            if got == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            answer.extend_from_slice(&read[..got]);
        }
    }

    /// Writes a POST to `/owner-a/events` with `bearer` on `connection`, its
    /// body announced as `announce`, and `sent` bytes of the body.
    async fn write_request(
        connection: &mut TcpStream,
        bearer: &str,
        announce: Announce,
        sent: usize,
    ) -> std::io::Result<()> {
        let framing = match announce {
            Announce::Length(len) => format!("content-length: {len}"),
            Announce::Chunked | Announce::ChunkedWhole => "transfer-encoding: chunked".to_owned(),
        };
        let head = format!(
            "POST /owner-a/events HTTP/1.1\r\nhost: gateway.test\r\nauthorization: Bearer \
             {bearer}\r\n{framing}\r\n\r\n"
        );
        connection.write_all(head.as_bytes()).await?;
        let chunked = !matches!(announce, Announce::Length(_));
        let chunk = vec![0_u8; CHUNK];
        let mut left = sent;
        while left > 0 {
            let now = left.min(CHUNK);
            if chunked {
                connection
                    .write_all(format!("{now:x}\r\n").as_bytes())
                    .await?;
            }
            connection.write_all(&chunk[..now]).await?;
            if chunked {
                connection.write_all(b"\r\n").await?;
            }
            left = left.saturating_sub(now);
        }
        if matches!(announce, Announce::ChunkedWhole) {
            connection.write_all(b"0\r\n\r\n").await?;
        }
        Ok(())
    }

    /// A new connection to `addr` that sends from [`BUFFER`].
    async fn connect(addr: SocketAddr) -> TcpStream {
        let socket = TcpSocket::new_v4().expect("a socket");
        socket.set_send_buffer_size(BUFFER).expect("a send buffer");
        socket.connect(addr).await.expect("connect")
    }

    /// A gateway with access control and `config` whose token issuer is
    /// unavailable, so that `valid-token` gets the issuer's `503`.
    fn issuer_down(config: GatewayConfig) -> Gateway {
        Gateway::with_access_control(
            config,
            AccessControl::new(
                Arc::new(FixedPrincipalResolver {
                    calls: AtomicUsize::new(0),
                    result: Err(AuthenticationError::Unavailable),
                }),
                Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow)),
            ),
        )
    }

    /// Reads one answer from `connection`, its body included, by its
    /// `content-length`, and returns its status and headers.
    async fn read_answer(connection: &mut TcpStream) -> (StatusCode, HeaderMap) {
        let (status, headers, _body) = read_whole_answer(connection).await;
        (status, headers)
    }

    /// [`read_answer`], with the answer's body.
    async fn read_whole_answer(connection: &mut TcpStream) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut answer = Vec::new();
        let end = loop {
            if let Some(end) = answer.windows(4).position(|window| window == b"\r\n\r\n") {
                break end;
            }
            let mut read = [0_u8; 1024];
            let got = connection.read(&mut read).await.expect("read the answer");
            assert_ne!(got, 0, "the connection closed before the answer");
            answer.extend_from_slice(&read[..got]);
        };
        let (status, headers) = parse_head(&answer[..end]);
        let length: usize = value(&headers, "content-length")
            .expect("a content-length")
            .parse()
            .expect("a length");
        let mut body = answer.split_off(end.saturating_add(4));
        while body.len() < length {
            let mut read = [0_u8; 1024];
            let got = connection.read(&mut read).await.expect("read the body");
            assert_ne!(got, 0, "the connection closed in the answer's body");
            body.extend_from_slice(&read[..got]);
        }
        (status, headers, body)
    }

    /// The code of an error answer's `body`.
    fn error_code(body: &[u8]) -> ErrorCode {
        ErrorBody::parse(body).expect("an error body").code
    }

    /// The status and headers of an answer's head, without its blank line.
    fn parse_head(head: &[u8]) -> (StatusCode, HeaderMap) {
        let head = std::str::from_utf8(head).expect("an ASCII head");
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|code| StatusCode::from_bytes(code.as_bytes()).ok())
            .expect("a status line");
        let mut headers = HeaderMap::new();
        for line in lines {
            let (name, value) = line.split_once(':').expect("a header line");
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).expect("a header name"),
                HeaderValue::from_str(value.trim()).expect("a header value"),
            );
        }
        (status, headers)
    }

    /// The value of `name` in `headers`.
    fn value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
        headers
            .get(name)
            .map(|value| value.to_str().expect("an ASCII value"))
    }

    /// The request [`upload`] sends, with `body`, as [`Gateway::handle`]
    /// takes it.
    fn request(bearer: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/owner-a/events")
            .header(AUTHORIZATION, format!("Bearer {bearer}"))
            .body(body)
            .expect("request")
    }

    /// Lets the runtime run, the loopback included, until `done` holds, and
    /// says whether it does. It gives up after many turns, so that a test
    /// whose gateway never answers fails instead of waiting on a frozen clock
    /// forever.
    async fn until(done: impl Fn() -> bool) -> bool {
        for _ in 0..100_000 {
            if done() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        done()
    }

    /// Each refusal decided from the head reaches a client that sends a body
    /// of 8 MiB before it reads the answer, with the status and headers that
    /// the same request without a body gets: the gateway read the whole body
    /// first. Unread, the body would fail the client's write.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_from_the_head_reaches_a_client_that_sends_its_whole_body_first() {
        // The bucket's rate does not refill, and no reading is cut short.
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let config = || test_config(vec![upstream.url.clone()]);
        let rate_limited =
            guarded(config(), Ok(AuthorizationDecision::Allow)).with_quota_provider(Arc::new(
                StaticQuotaProvider::from_toml_str(
                    "[[bucket]]\nid = \"owner-a\"\nrequests_per_sec = 1\n",
                )
                .expect("quotas"),
            ));
        // The bucket's one request a second goes through.
        let admitted = rate_limited
            .handle(request("valid-token", Body::empty()))
            .await;
        assert_eq!(admitted.status(), StatusCode::OK);
        let cases = [
            (
                "refused token",
                guarded(config(), Ok(AuthorizationDecision::Allow)),
                "not-a-token",
                StatusCode::UNAUTHORIZED,
            ),
            (
                "denied",
                guarded(config(), Ok(AuthorizationDecision::Deny)),
                "valid-token",
                StatusCode::FORBIDDEN,
            ),
            (
                "concealed",
                guarded(config(), Ok(AuthorizationDecision::ConcealAsNotFound)),
                "valid-token",
                StatusCode::NOT_FOUND,
            ),
            (
                "token issuer down",
                issuer_down(config()),
                "valid-token",
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "authorizer down",
                guarded(config(), Err(AuthorizationError::Unavailable)),
                "valid-token",
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "rate limited",
                rate_limited,
                "valid-token",
                StatusCode::TOO_MANY_REQUESTS,
            ),
        ];
        for (case, gateway, bearer, status) in cases {
            let gateway = Arc::new(gateway);
            let (addr, _server) = serve(Arc::clone(&gateway)).await;

            let client = tokio::spawn(upload(
                addr,
                bearer,
                Announce::Length(LARGE_BODY),
                LARGE_BODY,
            ));
            assert!(until(|| client.is_finished()).await, "{case}: no answer");
            let (answered, headers) = client.await.unwrap().unwrap_or_else(|error| {
                panic!("{case}: the connection broke before the answer: {error}")
            });

            assert_eq!(answered, status, "{case}");
            let unread = gateway.handle(request(bearer, Body::empty())).await;
            assert_eq!(answered, unread.status(), "{case}");
            for (name, expected) in unread.headers() {
                assert_eq!(headers.get(name), Some(expected), "{case}: {name}");
            }
            let metrics = gateway.metrics_snapshot();
            assert_eq!(
                (metrics.refusal_drains, metrics.refusal_drains_cut),
                (1, 0),
                "{case}"
            );
        }
    }

    /// A body that stops arriving is read until no chunk arrived for
    /// [`REFUSED_BODY_STALL_TIMEOUT`], to the timer's millisecond, and no
    /// longer: then its client gets the refusal, and the connection closes on
    /// the rest of the body.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_stops_arriving_is_read_until_it_stalls() {
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let gateway = Arc::new(guarded(
            test_config(vec![upstream.url.clone()]),
            Ok(AuthorizationDecision::Allow),
        ));
        let (addr, _server) = serve(Arc::clone(&gateway)).await;

        // The head and 64 KiB of an 8 MiB body, and then nothing.
        let client = tokio::spawn(upload(
            addr,
            "not-a-token",
            Announce::Length(LARGE_BODY),
            CHUNK,
        ));
        // Each read restarts the stall timeout: the clock moves only once the
        // gateway has read every byte sent.
        assert!(
            until(|| gateway.metrics_snapshot().refusal_drain_bytes == CHUNK as u64).await,
            "the gateway reads the body"
        );
        let almost = REFUSED_BODY_STALL_TIMEOUT.saturating_sub(Duration::from_millis(1));
        tokio::time::advance(almost).await;
        assert!(
            !until(|| client.is_finished()).await,
            "answered before the body stalled"
        );
        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(
            until(|| client.is_finished()).await,
            "no answer once the body stalled"
        );
        let (status, _headers) = client.await.unwrap().expect("the refusal");

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 1));
    }

    /// A body that keeps arriving, a chunk a second, is read for
    /// [`REFUSED_BODY_DRAIN_TIMEOUT`] and no longer: then the refusal goes
    /// out. The clock moves on its own here, from one timer to the next.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_keeps_arriving_is_read_until_the_timeout() {
        // Never reached: the gateway refuses the token.
        let gateway = guarded(
            test_config(vec!["http://127.0.0.1:9".to_owned()]),
            Ok(AuthorizationDecision::Allow),
        );
        let (chunks, body) = tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(1);
        let sender = tokio::spawn(async move {
            while chunks
                .send(Ok(Bytes::from_static(&[0; 1024])))
                .await
                .is_ok()
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let body = Body::from_stream(ReceiverStream::new(body));

        let started = Instant::now();
        let answered = tokio::time::timeout(
            REFUSED_BODY_DRAIN_TIMEOUT.saturating_mul(2),
            gateway.handle(request("not-a-token", body)),
        )
        .await
        .expect("an answer before twice the timeout");
        let took = started.elapsed();
        sender.abort();

        assert_eq!(answered.status(), StatusCode::UNAUTHORIZED);
        assert!(
            took >= REFUSED_BODY_DRAIN_TIMEOUT
                && took < REFUSED_BODY_DRAIN_TIMEOUT.saturating_add(Duration::from_secs(1)),
            "answered after {took:?}"
        );
        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 1));
    }

    /// A reading whose request is dropped before the body ended, as when the
    /// connection goes away, counts as cut.
    #[tokio::test(start_paused = true)]
    async fn a_reading_dropped_before_the_body_ended_counts_as_cut() {
        // Never reached: the gateway refuses the token.
        let gateway = guarded(
            test_config(vec!["http://127.0.0.1:9".to_owned()]),
            Ok(AuthorizationDecision::Allow),
        );
        let (chunks, body) = tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(1);
        chunks
            .send(Ok(Bytes::from_static(&[0; 1024])))
            .await
            .expect("the first chunk");
        let body = Body::from_stream(ReceiverStream::new(body));

        tokio::time::timeout(
            Duration::from_secs(1),
            gateway.handle(request("not-a-token", body)),
        )
        .await
        .expect_err("the reading waits for the rest of the body");
        drop(chunks);

        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 1));
    }

    /// A body that goes past `max_request_body_bytes` is read up to the limit
    /// and no further, whether the gateway would forward the request (its
    /// `413`, as before) or refused it from its head (with a chunked body,
    /// whose length the head does not declare): the client gets the answer as
    /// soon as the body passes the limit, and the connection closes on the
    /// rest of the body.
    #[tokio::test(start_paused = true)]
    async fn a_body_over_the_limit_is_read_up_to_the_limit() {
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let mut config = test_config(vec![upstream.url.clone()]);
        set_body_limit(&mut config, LIMIT);
        let cases = [
            (
                "forwarded",
                Gateway::new(config.clone()),
                Announce::Chunked,
                StatusCode::PAYLOAD_TOO_LARGE,
                (0_u64, 0_u64),
            ),
            (
                "refused token",
                guarded(config, Ok(AuthorizationDecision::Allow)),
                Announce::Chunked,
                StatusCode::UNAUTHORIZED,
                (1_u64, 1_u64),
            ),
        ];
        for (case, gateway, announce, status, drains) in cases {
            let gateway = Arc::new(gateway);
            let (addr, _server) = serve(Arc::clone(&gateway)).await;

            // One byte past the limit, and then nothing.
            let past = LIMIT.saturating_add(1);
            let client = tokio::spawn(upload(addr, "not-a-token", announce, past));
            assert!(
                until(|| client.is_finished()).await,
                "{case}: no answer once the body passed the limit"
            );
            let (answered, _headers) = client.await.unwrap().expect("the answer");

            assert_eq!(answered, status, "{case}");
            let metrics = gateway.metrics_snapshot();
            assert_eq!(
                (metrics.refusal_drains, metrics.refusal_drains_cut),
                drains,
                "{case}"
            );
        }
    }

    /// A refused request whose head declares a body over
    /// `max_request_body_bytes` gets the refusal before it sends any of the
    /// body: the gateway reads none of it, since the connection closes after
    /// the refusal either way.
    #[tokio::test(start_paused = true)]
    async fn a_body_declared_over_the_limit_is_not_read() {
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let mut config = test_config(vec![upstream.url.clone()]);
        set_body_limit(&mut config, LIMIT);
        let gateway = Arc::new(guarded(config, Ok(AuthorizationDecision::Allow)));
        let (addr, _server) = serve(Arc::clone(&gateway)).await;

        // The head alone, declaring one byte more than the limit.
        let declared = Announce::Length(LIMIT.saturating_add(1));
        let client = tokio::spawn(upload(addr, "not-a-token", declared, 0));
        assert!(
            until(|| client.is_finished()).await,
            "no answer to the head"
        );
        let (status, _headers) = client.await.unwrap().expect("the refusal");

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 1));
    }

    /// After a refused body was read to its end, the connection carries the
    /// client's next request.
    #[tokio::test(start_paused = true)]
    async fn the_connection_carries_the_next_request_after_a_whole_reading() {
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let gateway = Arc::new(guarded(
            test_config(vec![upstream.url.clone()]),
            Ok(AuthorizationDecision::Allow),
        ));
        let (addr, _server) = serve(Arc::clone(&gateway)).await;
        let mut connection = TcpStream::connect(addr).await.expect("connect");

        let head = format!(
            "POST /owner-a/events HTTP/1.1\r\nhost: gateway.test\r\nauthorization: Bearer \
             not-a-token\r\ncontent-length: {LARGE_BODY}\r\n\r\n"
        );
        connection
            .write_all(head.as_bytes())
            .await
            .expect("send the head");
        connection
            .write_all(&vec![0_u8; LARGE_BODY])
            .await
            .expect("send the whole body before reading");
        let (refused, _) = read_answer(&mut connection).await;
        assert_eq!(refused, StatusCode::UNAUTHORIZED);

        connection
            .write_all(
                b"POST /owner-a/events HTTP/1.1\r\nhost: gateway.test\r\nauthorization: Bearer \
                  valid-token\r\ncontent-length: 0\r\n\r\n",
            )
            .await
            .expect("send the next request on the same connection");
        let (next, _) = read_answer(&mut connection).await;
        assert_eq!(next, StatusCode::OK);
        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 0));
    }

    /// The body a holder declares: 2 MiB, past the default small-body
    /// threshold, so that it counts toward its bucket's upload limits.
    const HELD_BODY: usize = 2 << 20;

    /// Each refusal of a request the gateway would forward, but for its body
    /// or the bodies under way beside it, reaches a client that sends a body
    /// of 8 MiB before it reads the answer, with its code, after the gateway read
    /// the whole body: a body over its bucket's own limit (declared, or
    /// found as it arrives), past its bucket's upload count or bytes while
    /// another of its large bodies is under way, or without its share of the
    /// budget. The connection then carries the client's next request.
    #[tokio::test(start_paused = true)]
    async fn an_upload_refusal_reaches_a_client_that_sends_its_whole_body_first() {
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let config = || test_config(vec![upstream.url.clone()]);
        let quotas =
            |policy: &str| Arc::new(StaticQuotaProvider::from_toml_str(policy).expect("quotas"));
        let allowed = || guarded(config(), Ok(AuthorizationDecision::Allow));
        // 8 MiB per request and 9 MiB of budget, 1 MiB of it headroom: a
        // held body leaves no room for another at the limit. No wait.
        let mut spent = config();
        spent.uploads = UploadLimits::try_from(UploadLimitsSpec {
            max_request_body_bytes: 8 << 20,
            budget_bytes: 9 << 20,
            small_body_headroom_bytes: 1 << 20,
            budget_wait: Duration::ZERO,
            ..UploadLimitsSpec::default()
        })
        .expect("valid upload limits");
        let cases = [
            (
                "bucket limit, declared",
                allowed().with_quota_provider(quotas(
                    "[[bucket]]\nid = \"owner-a\"\nmax_request_body_bytes = 1048576\n",
                )),
                Announce::Length(LARGE_BODY),
                false,
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::RequestTooLarge,
            ),
            (
                "bucket limit, chunked",
                allowed().with_quota_provider(quotas(
                    "[[bucket]]\nid = \"owner-a\"\nmax_request_body_bytes = 1048576\n",
                )),
                Announce::ChunkedWhole,
                false,
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::RequestTooLarge,
            ),
            (
                "bucket upload count",
                allowed().with_quota_provider(quotas(
                    "[[bucket]]\nid = \"owner-a\"\nmax_concurrent_uploads = 1\n",
                )),
                Announce::Length(LARGE_BODY),
                true,
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::UploadsExhausted,
            ),
            (
                "bucket upload bytes",
                allowed().with_quota_provider(quotas(
                    "[[bucket]]\nid = \"owner-a\"\nmax_upload_bytes = 4194304\n",
                )),
                Announce::Length(LARGE_BODY),
                true,
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::UploadBytesExhausted,
            ),
            (
                "budget spent",
                guarded(spent, Ok(AuthorizationDecision::Allow)),
                Announce::Length(LARGE_BODY),
                true,
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::UploadBudgetExhausted,
            ),
        ];
        for (case, gateway, announce, held, status, code) in cases {
            let gateway = Arc::new(gateway);
            let (addr, _server) = serve(Arc::clone(&gateway)).await;
            // A large body of the bucket under way, which never ends.
            let mut holder = None;
            if held {
                let mut connection = connect(addr).await;
                write_request(
                    &mut connection,
                    "valid-token",
                    Announce::Length(HELD_BODY),
                    CHUNK,
                )
                .await
                .expect("send the held body's start");
                assert!(
                    until(|| gateway.metrics_snapshot().upload_reserved_bytes == HELD_BODY as u64)
                        .await,
                    "{case}: the held body is reserved"
                );
                holder = Some(connection);
            }

            let mut connection = connect(addr).await;
            let client = tokio::spawn(async move {
                let answered =
                    match write_request(&mut connection, "valid-token", announce, LARGE_BODY).await
                    {
                        Ok(()) => Ok(read_whole_answer(&mut connection).await),
                        Err(error) => Err(error),
                    };
                (answered, connection)
            });
            assert!(until(|| client.is_finished()).await, "{case}: no answer");
            let (answered, mut connection) = client.await.unwrap();
            let (answered, _headers, body) = answered.unwrap_or_else(|error| {
                panic!("{case}: the connection broke before the answer: {error}")
            });

            assert_eq!(answered, status, "{case}");
            assert_eq!(error_code(&body), code, "{case}");
            let metrics = gateway.metrics_snapshot();
            assert_eq!(
                (metrics.refusal_drains, metrics.refusal_drains_cut),
                (1, 0),
                "{case}"
            );

            let next = tokio::spawn(async move {
                connection
                    .write_all(
                        b"POST /owner-a/events HTTP/1.1\r\nhost: gateway.test\r\nauthorization: \
                          Bearer valid-token\r\ncontent-length: 0\r\n\r\n",
                    )
                    .await
                    .expect("send the next request on the same connection");
                read_answer(&mut connection).await
            });
            assert!(until(|| next.is_finished()).await, "{case}: no next answer");
            let (next, _) = next.await.unwrap();
            assert_eq!(next, StatusCode::OK, "{case}: the connection carries on");
            drop(holder);
        }
    }

    /// A forwarded body that stops arriving is cut once no byte of it
    /// arrived for the stall timeout, to the timer's millisecond: its client
    /// gets a `408`, and the connection closes on the rest of the
    /// body.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_upload_gets_a_408_and_its_connection_closes() {
        let _frozen = FrozenClock::hold();
        let upstream = answering_upstream().await;
        let gateway = Arc::new(guarded(
            test_config(vec![upstream.url.clone()]),
            Ok(AuthorizationDecision::Allow),
        ));
        let stall = gateway.config.uploads.stall_timeout();
        let (addr, _server) = serve(Arc::clone(&gateway)).await;

        let mut connection = connect(addr).await;
        write_request(
            &mut connection,
            "valid-token",
            Announce::Length(LARGE_BODY),
            CHUNK,
        )
        .await
        .expect("send the start of the body");
        // Each read restarts the stall timeout: the clock moves only once the
        // gateway has read every byte sent.
        assert!(
            until(|| gateway.metrics_snapshot().body_bytes_buffered == CHUNK as u64).await,
            "the gateway reads the body"
        );
        let client = tokio::spawn(async move {
            let answered = read_whole_answer(&mut connection).await;
            let mut rest = [0_u8; 16];
            let closed = connection.read(&mut rest).await.map(|read| read == 0);
            (answered, closed)
        });
        tokio::time::advance(stall.saturating_sub(Duration::from_millis(1))).await;
        assert!(
            !until(|| client.is_finished()).await,
            "cut before the stall"
        );
        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(
            until(|| client.is_finished()).await,
            "no answer once stalled"
        );
        let ((status, _headers, body), closed) = client.await.unwrap();

        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(error_code(&body), ErrorCode::BodyTooSlow);
        assert!(
            closed.unwrap_or(true),
            "the connection closes after the 408"
        );
        let metrics = gateway.metrics_snapshot();
        assert_eq!(metrics.upload_refusals_too_slow, 1);
        assert_eq!(metrics.upload_reserved_bytes, 0);
    }
}

/// The body budget, the buckets' upload limits and the pace of a forwarded
/// body ([`crate::uploads`]), with budgets of a few KiB, through
/// [`Gateway::handle`].
mod uploads {
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::HeaderMap;
    use axum::http::HeaderValue;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::http::header::AUTHORIZATION;
    use axum::http::header::CACHE_CONTROL;
    use axum::http::header::RETRY_AFTER;
    use axum::response::Response as AxumResponse;
    use bytes::Bytes;
    use http_body::Frame;
    use http_body::SizeHint;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio::time::Instant;
    use tokio_stream::wrappers::ReceiverStream;
    use ursula_headers::ErrorBody;
    use ursula_headers::ErrorCode;
    use ursula_headers::names::URSULA_MAX_REQUEST_BYTES;

    use super::FixedPrincipalResolver;
    use super::FrozenClock;
    use super::RecordingAuthorizer;
    use super::counting_fallback_upstream;
    use super::silent_upstream;
    use super::test_config;
    use crate::Gateway;
    use crate::GatewayConfig;
    use crate::admission::StaticQuotaProvider;
    use crate::auth::AccessControl;
    use crate::auth::AuthorizationDecision;
    use crate::uploads::UploadLimits;
    use crate::uploads::UploadLimitsSpec;

    /// A budget of 8 KiB, of which 4 KiB is headroom for bodies of at most
    /// 1 KiB, and a per-request limit of 4 KiB: one body at the limit spends
    /// the rest. A request waits 20 ms for the budget.
    fn tiny() -> UploadLimitsSpec {
        UploadLimitsSpec {
            max_request_body_bytes: 4 << 10,
            budget_bytes: 8 << 10,
            small_body_headroom_bytes: 4 << 10,
            small_body_max_bytes: 1 << 10,
            budget_wait: Duration::from_millis(20),
            ..UploadLimitsSpec::default()
        }
    }

    /// [`tiny`] with 64 KiB of budget, which no test here spends.
    fn roomy() -> UploadLimitsSpec {
        UploadLimitsSpec {
            budget_bytes: 64 << 10,
            ..tiny()
        }
    }

    fn config(upstream: &str, uploads: UploadLimitsSpec) -> GatewayConfig {
        let mut config = test_config(vec![upstream.to_owned()]);
        config.uploads = UploadLimits::try_from(uploads).expect("valid upload limits");
        config
    }

    /// A gateway with access control that allows every request with
    /// `valid-token`, and the quota policy `quotas`.
    fn guarded(config: GatewayConfig, quotas: &str) -> Gateway {
        Gateway::with_access_control(
            config,
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow)),
            ),
        )
        .with_quota_provider(Arc::new(
            StaticQuotaProvider::from_toml_str(quotas).expect("quotas"),
        ))
    }

    fn post(path: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(body)
            .expect("request")
    }

    /// A body without a declared length that sends `first` and then nothing
    /// until the returned sender sends more or is dropped, when it ends.
    fn open_body(first: &'static [u8]) -> (mpsc::Sender<Result<Bytes, Infallible>>, Body) {
        let (chunks, body) = mpsc::channel(1);
        chunks
            .try_send(Ok(Bytes::from_static(first)))
            .expect("room for the first chunk");
        (chunks, Body::from_stream(ReceiverStream::new(body)))
    }

    /// A body that declares `len` bytes, as a `content-length` does, and
    /// delivers what the returned sender sends, ending when it is dropped.
    struct Declared {
        len: u64,
        chunks: mpsc::Receiver<Bytes>,
    }

    impl http_body::Body for Declared {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.chunks
                .poll_recv(cx)
                .map(|chunk| chunk.map(|chunk| Ok(Frame::data(chunk))))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.len)
        }
    }

    /// A body declared as `len` bytes, of which none has arrived yet.
    fn declared(len: u64) -> (mpsc::Sender<Bytes>, Body) {
        let (chunks, receiver) = mpsc::channel(64);
        (
            chunks,
            Body::new(Declared {
                len,
                chunks: receiver,
            }),
        )
    }

    /// Sends `request` through `gateway` on a task of its own.
    fn send(gateway: &Arc<Gateway>, request: Request<Body>) -> JoinHandle<AxumResponse> {
        let gateway = Arc::clone(gateway);
        tokio::spawn(async move { gateway.handle(request).await })
    }

    /// Lets the runtime run until `done` holds of `gateway`'s metrics, and
    /// says whether it does. It gives up after many turns.
    async fn until(
        gateway: &Gateway,
        done: impl Fn(&crate::GatewayMetricsSnapshot) -> bool,
    ) -> bool {
        for _ in 0..100_000 {
            if done(&gateway.metrics_snapshot()) {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    }

    /// Lets the runtime run until `gateway` holds `bytes` of its budget.
    async fn until_reserved(gateway: &Gateway, bytes: u64) -> bool {
        until(gateway, |metrics| metrics.upload_reserved_bytes == bytes).await
    }

    /// The answer to `upload`, which must come within 5 s.
    async fn answer(upload: JoinHandle<AxumResponse>) -> AxumResponse {
        tokio::time::timeout(Duration::from_secs(5), upload)
            .await
            .expect("the upload is answered")
            .expect("the upload's task")
    }

    /// Ends a declared upload with the rest of its bytes and returns its
    /// answer.
    async fn finish_declared(
        chunks: mpsc::Sender<Bytes>,
        rest: usize,
        upload: JoinHandle<AxumResponse>,
    ) -> AxumResponse {
        chunks
            .send(Bytes::from(vec![0_u8; rest]))
            .await
            .expect("the gateway reads the body");
        drop(chunks);
        answer(upload).await
    }

    fn retry_after(headers: &HeaderMap) -> Option<&[u8]> {
        headers.get(RETRY_AFTER).map(HeaderValue::as_bytes)
    }

    /// Asserts that `response` is the gateway's own refusal `status` with
    /// `code` in its error body, and returns its headers.
    async fn assert_refused(
        response: AxumResponse,
        status: StatusCode,
        code: ErrorCode,
    ) -> HeaderMap {
        assert_eq!(response.status(), status);
        let (parts, body) = response.into_parts();
        assert_eq!(
            parts.headers.get(CACHE_CONTROL).map(HeaderValue::as_bytes),
            Some(&b"no-store"[..])
        );
        let body = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("the refusal's body");
        assert_eq!(ErrorBody::parse(&body).map(|body| body.code), Ok(code));
        parts.headers
    }

    /// A large body that would take its bucket's large bodies past
    /// `max_upload_bytes` is refused with a `429` and `Retry-After`,
    /// after the gateway read it, and nothing reaches the upstream. A small
    /// body of the same bucket goes through meanwhile: small bodies never
    /// count toward the bucket's upload limits.
    #[tokio::test]
    async fn a_body_past_the_bucket_upload_bytes_gets_a_429_while_small_ones_pass() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(guarded(
            config(&upstream.url, roomy()),
            "[[bucket]]\nid = \"tenant-a\"\nmax_upload_bytes = 3072\n",
        ));
        let (chunks, body) = declared(2 << 10);
        let first = send(&gateway, post("/tenant-a/orders", body));
        assert!(until(&gateway, |metrics| metrics.upload_tenants == 1).await);

        let refused = gateway
            .handle(post("/tenant-a/orders", Body::from(vec![0_u8; 2 << 10])))
            .await;
        let refused = assert_refused(
            refused,
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::UploadBytesExhausted,
        )
        .await;
        assert_eq!(retry_after(&refused), Some(&b"1"[..]));
        assert_eq!(hits.load(Ordering::Relaxed), 0);
        let metrics = gateway.metrics_snapshot();
        assert_eq!(metrics.upload_refusals_tenant_bytes, 1);
        assert_eq!(metrics.upload_tenants, 1, "the refusal holds nothing");
        // Read to its end before the answer.
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 0));

        for small in [512, 1 << 10] {
            let passed = gateway
                .handle(post("/tenant-a/orders", Body::from(vec![0_u8; small])))
                .await;
            assert_eq!(passed.status(), StatusCode::OK, "{small} bytes");
        }
        assert_eq!(hits.load(Ordering::Relaxed), 2);

        assert_eq!(
            finish_declared(chunks, 2 << 10, first).await.status(),
            StatusCode::OK
        );
        assert_eq!(gateway.metrics_snapshot().upload_tenants, 0);
    }

    /// A bucket with `max_concurrent_uploads` large bodies being read is
    /// refused another, with a `429`, while another bucket's large
    /// body goes through, and while more small bodies of the bucket than the
    /// count, sent at once and still arriving, go through too.
    #[tokio::test]
    async fn uploads_past_the_bucket_count_are_refused_while_small_bodies_and_other_buckets_proceed()
     {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(guarded(
            config(&upstream.url, roomy()),
            "[default]\nmax_concurrent_uploads = 1\n",
        ));
        let (chunks, body) = declared(2 << 10);
        let first = send(&gateway, post("/tenant-a/orders", body));
        assert!(until(&gateway, |metrics| metrics.upload_tenants == 1).await);

        let refused = gateway
            .handle(post("/tenant-a/other", Body::from(vec![0_u8; 2 << 10])))
            .await;
        let refused = assert_refused(
            refused,
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::UploadsExhausted,
        )
        .await;
        assert_eq!(retry_after(&refused), Some(&b"1"[..]));
        assert_eq!(hits.load(Ordering::Relaxed), 0);
        let metrics = gateway.metrics_snapshot();
        assert_eq!(metrics.upload_refusals_tenant_uploads, 1);
        assert_eq!(metrics.upload_tenants, 1, "the refusal holds nothing");

        // Sixteen small appends of the bucket at once, each still arriving.
        let small: Vec<_> = (0..16)
            .map(|_| {
                let (chunks, body) = open_body(b"commit");
                (chunks, send(&gateway, post("/tenant-a/orders", body)))
            })
            .collect();
        assert!(until_reserved(&gateway, (2 << 10) + (16 << 10)).await);
        for (chunks, upload) in small {
            drop(chunks);
            assert_eq!(answer(upload).await.status(), StatusCode::OK);
        }

        let other = gateway
            .handle(post("/tenant-b/orders", Body::from(vec![0_u8; 2 << 10])))
            .await;
        assert_eq!(other.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::Relaxed), 17);

        assert_eq!(
            finish_declared(chunks, 2 << 10, first).await.status(),
            StatusCode::OK
        );
        let metrics = gateway.metrics_snapshot();
        assert_eq!(
            (metrics.upload_tenants, metrics.upload_reserved_bytes),
            (0, 0)
        );
    }

    /// A large body stops counting toward its bucket's upload limits once
    /// the gateway has buffered it, while the upstream still works on it, and
    /// a body without a declared length counts once it grows past the small
    /// bodies.
    #[tokio::test]
    async fn the_upload_count_is_released_once_the_body_is_buffered() {
        let (upstream, mut arrivals) = silent_upstream().await;
        let gateway = Arc::new(guarded(
            config(&upstream.url, roomy()),
            "[default]\nmax_concurrent_uploads = 1\n",
        ));
        let (chunks, body) = open_body(b"abc");
        let first = send(&gateway, post("/tenant-a/orders", body));
        // Small so far: it reserves the small-body threshold and counts for
        // no bucket.
        assert!(until_reserved(&gateway, 1 << 10).await);
        assert_eq!(gateway.metrics_snapshot().upload_tenants, 0);
        chunks
            .send(Ok(Bytes::from(vec![0_u8; 2 << 10])))
            .await
            .expect("the gateway reads the body");
        assert!(until(&gateway, |metrics| metrics.upload_tenants == 1).await);
        assert!(until_reserved(&gateway, 3 << 10).await);

        drop(chunks);
        arrivals.recv().await.expect("the first body is forwarded");
        assert_eq!(gateway.metrics_snapshot().upload_tenants, 0);

        let second = send(
            &gateway,
            post("/tenant-a/orders", Body::from(vec![0_u8; 2 << 10])),
        );
        arrivals.recv().await.expect("the second body is forwarded");
        first.abort();
        second.abort();
        assert!(until_reserved(&gateway, 0).await);
    }

    /// Without a quota policy no bucket has upload limits: three large
    /// bodies of one bucket are read at once, within the budget.
    #[tokio::test]
    async fn upload_limits_apply_only_under_a_quota_policy() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(Gateway::new(config(&upstream.url, roomy())));
        let open: Vec<_> = (0..3)
            .map(|_| {
                let (chunks, body) = declared(2 << 10);
                (chunks, send(&gateway, post("/tenant-a/orders", body)))
            })
            .collect();
        assert!(until_reserved(&gateway, 6 << 10).await);
        assert_eq!(gateway.metrics_snapshot().upload_tenants, 0);
        for (chunks, upload) in open {
            assert_eq!(
                finish_declared(chunks, 2 << 10, upload).await.status(),
                StatusCode::OK
            );
        }
        assert_eq!(hits.load(Ordering::Relaxed), 3);
    }

    /// With the budget spent by one large upload, another large body waits
    /// `budget_wait` and gets a `503` with `Retry-After`, after the
    /// gateway read it, while a small body goes through on the headroom.
    #[tokio::test]
    async fn a_spent_budget_refuses_a_large_body_while_a_small_one_uses_the_headroom() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(Gateway::new(config(&upstream.url, tiny())));
        let (chunks, body) = declared(4 << 10);
        let large = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 4 << 10).await);

        let refused = gateway
            .handle(post("/tenant-b/orders", Body::from(vec![0_u8; 2 << 10])))
            .await;
        let refused = assert_refused(
            refused,
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::UploadBudgetExhausted,
        )
        .await;
        assert_eq!(retry_after(&refused), Some(&b"1"[..]));
        assert_eq!(hits.load(Ordering::Relaxed), 0);

        let small = gateway
            .handle(post("/tenant-c/orders", Body::from(vec![0_u8; 512])))
            .await;
        assert_eq!(small.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::Relaxed), 1);

        let metrics = gateway.metrics_snapshot();
        assert_eq!(metrics.upload_refusals_budget, 1);
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (1, 0));
        assert_eq!(metrics.upload_reserved_bytes, 4 << 10);
        assert_eq!(metrics.upload_budget_bytes, 8 << 10);
        assert_eq!(
            finish_declared(chunks, 4 << 10, large).await.status(),
            StatusCode::OK
        );
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 0);
    }

    /// A large body refused for the budget leaves its bucket's upload count
    /// while the gateway reads it to its end: once the budget frees up, the
    /// bucket's next large body goes through instead of getting a `429`.
    #[tokio::test]
    async fn a_body_refused_for_the_budget_gives_back_its_bucket_upload_while_it_is_read() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(guarded(
            config(&upstream.url, tiny()),
            "[[bucket]]\nid = \"tenant-b\"\nmax_concurrent_uploads = 1\n",
        ));
        let (holding, body) = declared(4 << 10);
        let holder = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 4 << 10).await);

        // Refused after the wait, and still arriving.
        let (arriving, body) = declared(2 << 10);
        let refused = send(&gateway, post("/tenant-b/orders", body));
        assert!(
            until(&gateway, |metrics| metrics.upload_refusals_budget == 1
                && metrics.upload_tenants == 0)
            .await
        );

        assert_eq!(
            finish_declared(holding, 4 << 10, holder).await.status(),
            StatusCode::OK
        );
        let next = gateway
            .handle(post("/tenant-b/orders", Body::from(vec![0_u8; 2 << 10])))
            .await;
        assert_eq!(next.status(), StatusCode::OK);
        assert_eq!(gateway.metrics_snapshot().upload_refusals_tenant_uploads, 0);

        assert_refused(
            finish_declared(arriving, 2 << 10, refused).await,
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::UploadBudgetExhausted,
        )
        .await;
        assert_eq!(hits.load(Ordering::Relaxed), 2);
    }

    /// A body declared as `parents.len()` bytes that sends the first byte of
    /// each parent in turn, as a slice of it, the way hyper hands out slices
    /// of a connection's read buffer. Each time it is polled, it counts in
    /// `retained` whether the gateway still holds the slice it sent last.
    struct Slices {
        parents: Vec<Bytes>,
        sent: usize,
        retained: Arc<AtomicUsize>,
    }

    impl http_body::Body for Slices {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            let sent = self.sent;
            if let Some(last) = sent.checked_sub(1).and_then(|last| self.parents.get(last))
                && !last.is_unique()
            {
                self.retained.fetch_add(1, Ordering::Relaxed);
            }
            let Some(parent) = self.parents.get(sent) else {
                return Poll::Ready(None);
            };
            let chunk = parent.slice(0..1);
            self.sent = sent.saturating_add(1);
            Poll::Ready(Some(Ok(Frame::data(chunk))))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.parents.len() as u64)
        }
    }

    /// A body that arrives as 1-byte slices of 8 KiB buffers holds none of
    /// them: the gateway copies each chunk and drops it before it reads the
    /// next, and the body it holds is exactly its share of the budget.
    #[tokio::test]
    async fn a_buffered_body_holds_no_chunk_and_only_its_share_of_the_budget() {
        let (upstream, mut arrivals) = silent_upstream().await;
        let gateway = Arc::new(Gateway::new(config(&upstream.url, roomy())));
        let retained = Arc::new(AtomicUsize::new(0));
        let body = Body::new(Slices {
            parents: (0..2 << 10)
                .map(|_| Bytes::from(vec![0_u8; 8 << 10]))
                .collect(),
            sent: 0,
            retained: Arc::clone(&retained),
        });
        let upload = send(&gateway, post("/tenant-a/orders", body));

        arrivals.recv().await.expect("the body is forwarded");
        assert_eq!(retained.load(Ordering::Relaxed), 0);
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 2 << 10);

        upload.abort();
        assert!(upload.await.unwrap_err().is_cancelled());
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 0);
    }

    /// A request that finds the budget spent gets it once the upload that
    /// holds it ends within `budget_wait`.
    #[tokio::test]
    async fn a_request_that_gets_the_budget_within_the_wait_goes_through() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(Gateway::new(config(&upstream.url, UploadLimitsSpec {
            budget_wait: Duration::from_secs(5),
            ..tiny()
        })));
        let (chunks, body) = declared(4 << 10);
        let holder = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 4 << 10).await);

        let waiting = send(
            &gateway,
            post("/tenant-b/orders", Body::from(vec![0_u8; 4 << 10])),
        );
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(!waiting.is_finished(), "the request waits for the budget");
        assert_eq!(
            finish_declared(chunks, 4 << 10, holder).await.status(),
            StatusCode::OK
        );
        assert_eq!(answer(waiting).await.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::Relaxed), 2);
        assert_eq!(gateway.metrics_snapshot().upload_refusals_budget, 0);
    }

    /// A request dropped while it waits for the budget gives back its count
    /// of its bucket's uploads and whatever of the budget it was assigned.
    #[tokio::test]
    async fn a_request_dropped_while_it_waits_releases_its_claim() {
        let (upstream, _hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(guarded(
            config(&upstream.url, UploadLimitsSpec {
                budget_wait: Duration::from_millis(300),
                ..tiny()
            }),
            "[default]\nmax_concurrent_uploads = 2\n",
        ));
        let (chunks, body) = declared(3 << 10);
        let holder = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 3 << 10).await);

        // Counted toward the bucket's two, then waits for the budget.
        let waiting = send(
            &gateway,
            post("/tenant-a/orders", Body::from(vec![0_u8; 4 << 10])),
        );
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(!waiting.is_finished(), "the request waits for the budget");
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());

        // Had the dropped request kept its count, this one would get the
        // bucket's `429`: it gets the budget's `503`.
        let refused = gateway
            .handle(post("/tenant-a/orders", Body::from(vec![0_u8; 4 << 10])))
            .await;
        assert_refused(
            refused,
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::UploadBudgetExhausted,
        )
        .await;
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 3 << 10);

        assert_eq!(
            finish_declared(chunks, 3 << 10, holder).await.status(),
            StatusCode::OK
        );
        let metrics = gateway.metrics_snapshot();
        assert_eq!(
            (metrics.upload_reserved_bytes, metrics.upload_tenants),
            (0, 0)
        );
    }

    /// A body declared over its bucket's own body limit is refused with a
    /// `413` before it reserves anything, once the gateway has read
    /// it to its end. A body without a declared length that passes the
    /// bucket's limit as it arrives is read to its end too, and gives back
    /// what it reserved.
    #[tokio::test]
    async fn a_body_over_the_bucket_limit_reserves_nothing_and_is_read_to_its_end() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(guarded(
            config(&upstream.url, roomy()),
            "[default]\nmax_request_body_bytes = 2048\n",
        ));
        let (chunks, body) = declared(3 << 10);
        let over = send(&gateway, post("/tenant-a/orders", body));
        assert!(until(&gateway, |metrics| metrics.refusal_drains == 1).await);
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 0);
        let refused = finish_declared(chunks, 3 << 10, over).await;
        let refused = assert_refused(
            refused,
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::RequestTooLarge,
        )
        .await;
        assert_eq!(
            refused
                .get(URSULA_MAX_REQUEST_BYTES)
                .map(HeaderValue::as_bytes),
            Some(&b"2048"[..])
        );

        let (chunks, body) = open_body(&[0; 1 << 10]);
        let growing = send(&gateway, post("/tenant-a/orders", body));
        chunks
            .send(Ok(Bytes::from(vec![0_u8; 2 << 10])))
            .await
            .expect("the gateway reads past the bucket's limit");
        assert!(until(&gateway, |metrics| metrics.refusal_drains == 2).await);
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 0);
        drop(chunks);
        assert_refused(
            answer(growing).await,
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::RequestTooLarge,
        )
        .await;

        assert_eq!(hits.load(Ordering::Relaxed), 0);
        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.refusal_drains, metrics.refusal_drains_cut), (2, 0));
    }

    /// A reservation ends with its request: after the upstream answered,
    /// after the body passed the per-request limit, and after the client
    /// went away. Each time a body that needs the whole budget but the
    /// headroom goes through next.
    #[tokio::test]
    async fn the_budget_is_released_after_success_refusal_and_disconnect() {
        let (upstream, hits) = counting_fallback_upstream().await;
        let gateway = Arc::new(Gateway::new(config(&upstream.url, tiny())));
        let whole_budget = || post("/tenant-a/orders", Body::from(vec![0_u8; 4 << 10]));
        let released = |gateway: &Gateway| gateway.metrics_snapshot().upload_reserved_bytes;

        let answered = gateway.handle(whole_budget()).await;
        assert_eq!(answered.status(), StatusCode::OK);
        assert_eq!(released(&gateway), 0, "after success");
        assert_eq!(
            gateway.handle(whole_budget()).await.status(),
            StatusCode::OK
        );

        // Undeclared, it grows past the limit.
        let over = Body::from_stream(tokio_stream::iter([
            Ok::<_, Infallible>(Bytes::from(vec![0_u8; 3 << 10])),
            Ok(Bytes::from(vec![0_u8; 3 << 10])),
        ]));
        let refused = gateway.handle(post("/tenant-a/orders", over)).await;
        let refused = assert_refused(
            refused,
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::RequestTooLarge,
        )
        .await;
        assert_eq!(
            refused
                .get(URSULA_MAX_REQUEST_BYTES)
                .map(HeaderValue::as_bytes),
            Some(&b"4096"[..])
        );
        assert_eq!(released(&gateway), 0, "after refusal");
        assert_eq!(
            gateway.handle(whole_budget()).await.status(),
            StatusCode::OK
        );

        let (chunks, body) = declared(4 << 10);
        let abandoned = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 4 << 10).await);
        abandoned.abort();
        assert!(abandoned.await.unwrap_err().is_cancelled());
        drop(chunks);
        assert_eq!(released(&gateway), 0, "after disconnect");
        assert_eq!(
            gateway.handle(whole_budget()).await.status(),
            StatusCode::OK
        );
        assert_eq!(hits.load(Ordering::Relaxed), 4);
    }

    /// A body without a declared length reserves the small-body threshold
    /// at first, grows its reservation as it arrives, and holds only its
    /// size, in whole KiB, once it is buffered and forwarded.
    #[tokio::test]
    async fn an_undeclared_reservation_grows_as_the_body_arrives_and_shrinks_once_buffered() {
        let (upstream, mut arrivals) = silent_upstream().await;
        let gateway = Arc::new(Gateway::new(config(&upstream.url, tiny())));
        let (chunks, body) = open_body(b"abc");
        let upload = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 1 << 10).await);
        chunks
            .send(Ok(Bytes::from(vec![0_u8; 1 << 10])))
            .await
            .expect("the gateway reads the body");
        assert!(until_reserved(&gateway, 2 << 10).await);
        chunks
            .send(Ok(Bytes::from(vec![0_u8; 1 << 10])))
            .await
            .expect("the gateway reads the body");
        assert!(until_reserved(&gateway, 4 << 10).await);

        drop(chunks);
        arrivals.recv().await.expect("the body is forwarded");
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 3 << 10);

        upload.abort();
        assert!(upload.await.unwrap_err().is_cancelled());
        assert_eq!(gateway.metrics_snapshot().upload_reserved_bytes, 0);
    }

    /// A body that keeps arriving is never cut, however long it takes: 8 MiB
    /// at 64 KiB/s takes 128 s, past the stall timeout and past the SQLite
    /// extension's 120 s for a snapshot transfer, and is forwarded.
    #[tokio::test(start_paused = true)]
    async fn a_slow_but_live_body_is_never_cut() {
        let _frozen = FrozenClock::hold();
        // Reads each body whole, and answers with its length.
        let upstream = super::spawn_upstream(axum::Router::new().fallback(axum::routing::any(
            |body: Body| async move {
                let read = http_body_util::BodyExt::collect(body)
                    .await
                    .expect("the forwarded body");
                read.to_bytes().len().to_string()
            },
        )))
        .await;
        let gateway = Arc::new(Gateway::new(config(
            &upstream.url,
            UploadLimitsSpec::default(),
        )));
        let (chunks, body) = declared(8 << 20);
        let upload = send(&gateway, post("/tenant-a/orders", body));
        assert!(until_reserved(&gateway, 8 << 20).await);
        for sent in 1..=128_u64 {
            chunks
                .send(Bytes::from(vec![0_u8; 64 << 10]))
                .await
                .expect("the gateway reads the body");
            // The chunk counts in its window once the gateway reads it.
            assert!(
                until(&gateway, |metrics| metrics.body_bytes_buffered
                    == sent << 16)
                .await
            );
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        drop(chunks);

        let answered = answer(upload).await;
        assert_eq!(answered.status(), StatusCode::OK);
        let forwarded = http_body_util::BodyExt::collect(answered.into_body())
            .await
            .expect("the upstream's answer")
            .to_bytes();
        assert_eq!(forwarded, (8_u32 << 20).to_string());
        assert_eq!(gateway.metrics_snapshot().upload_refusals_too_slow, 0);
    }

    /// A body is cut, and refused with a `408` before anything of it
    /// is forwarded, when no byte of it arrives for the stall timeout
    /// (30 s after its last chunk), and when one window of the stall timeout
    /// gets fewer bytes than the floor (1 KiB every 10 s against 1 KiB/s:
    /// cut when the first window ends, 30 s after the reading started, or,
    /// after a first chunk of 32 KiB that meets the first window's floor,
    /// when the second window ends, 60 s after it started).
    #[tokio::test(start_paused = true)]
    async fn an_abandoned_body_is_cut_before_it_is_forwarded() {
        let (upstream, hits) = counting_fallback_upstream().await;
        for (case, min_bytes_per_sec, first, every, chunks_sent, cut_after) in [
            (
                "stalled",
                0,
                1 << 10,
                Duration::from_secs(20),
                2,
                Duration::from_secs(50),
            ),
            (
                "below the floor",
                1 << 10,
                1 << 10,
                Duration::from_secs(10),
                u32::MAX,
                Duration::from_secs(30),
            ),
            (
                "below the floor in a later window",
                1 << 10,
                32 << 10,
                Duration::from_secs(10),
                u32::MAX,
                Duration::from_secs(60),
            ),
        ] {
            let gateway = Gateway::new(config(&upstream.url, UploadLimitsSpec {
                min_bytes_per_sec,
                ..UploadLimitsSpec::default()
            }));
            let (chunks, body) = declared(4 << 20);
            let sender = tokio::spawn(async move {
                for sent in 0..chunks_sent {
                    let size = if sent == 0 { first } else { 1 << 10 };
                    if chunks.send(Bytes::from(vec![0_u8; size])).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(every).await;
                }
                std::future::pending::<()>().await;
            });

            let started = Instant::now();
            let refused = gateway.handle(post("/tenant-a/orders", body)).await;
            let took = started.elapsed();
            sender.abort();

            assert_refused(refused, StatusCode::REQUEST_TIMEOUT, ErrorCode::BodyTooSlow).await;
            assert!(
                took >= cut_after && took < cut_after.saturating_add(Duration::from_millis(1)),
                "{case}: cut after {took:?}"
            );
            let metrics = gateway.metrics_snapshot();
            assert_eq!(metrics.upload_refusals_too_slow, 1, "{case}");
            assert_eq!(metrics.upload_reserved_bytes, 0, "{case}");
        }
        assert_eq!(hits.load(Ordering::Relaxed), 0);
    }
}

/// Every answer the gateway sends itself carries the error body Ursula
/// renders, `{"error":{"code":"<code>","message":"<message>"}}`, beside its
/// typed headers.
#[tokio::test]
async fn the_gateways_own_answers_carry_an_error_body() {
    let cases = [
        (Refusal::NotFound.into_response(), 404, ErrorCode::NotFound),
        (
            Refusal::Concealed.into_response(),
            404,
            ErrorCode::BucketNotFound,
        ),
        (
            Refusal::Authentication(AuthenticationError::InvalidCredential).into_response(),
            401,
            ErrorCode::InvalidCredential,
        ),
        (
            Refusal::Authentication(AuthenticationError::WrongAudience).into_response(),
            401,
            ErrorCode::InvalidCredential,
        ),
        (
            Refusal::Authentication(AuthenticationError::Expired).into_response(),
            401,
            ErrorCode::CredentialExpired,
        ),
        (
            Refusal::Authentication(AuthenticationError::Unavailable).into_response(),
            503,
            ErrorCode::AuthenticationUnavailable,
        ),
        (
            Refusal::Forbidden.into_response(),
            403,
            ErrorCode::Forbidden,
        ),
        (
            Refusal::Authorization(AuthorizationError::Unavailable).into_response(),
            503,
            ErrorCode::AuthorizationUnavailable,
        ),
        (
            Refusal::Admission(AdmissionRejection::RateLimited {
                retry_after_secs: 2,
            })
            .into_response(),
            429,
            ErrorCode::RateLimited,
        ),
        (
            Refusal::Admission(AdmissionRejection::LiveReadsExhausted).into_response(),
            429,
            ErrorCode::LiveReadsExhausted,
        ),
        (
            Refusal::BodyTooLarge { limit: 7 }.into_response(),
            413,
            ErrorCode::RequestTooLarge,
        ),
        (
            Refusal::BodyUnreadable.into_response(),
            400,
            ErrorCode::BodyUnreadable,
        ),
        (
            Refusal::NoUpstream.into_response(),
            503,
            ErrorCode::NoUpstream,
        ),
        (
            GatewayError::LeaderNotFound { followed: 3 }.into_response(),
            503,
            ErrorCode::NoLeader,
        ),
        (
            GatewayError::UpstreamTimeout {
                upstream: "http://node:4437".to_owned(),
                timeout: Duration::from_secs(1),
            }
            .into_response(),
            504,
            ErrorCode::UpstreamTimeout,
        ),
        (unreachable_response(), 503, ErrorCode::UpstreamUnreachable),
    ];
    for (response, status, code) in cases {
        assert_eq!(response.status().as_u16(), status, "{code}");
        // #640: no cache in front of the gateway may keep its own answer,
        // such as a concealing `404`.
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store"),
            "{code}"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(ErrorBody::CONTENT_TYPE),
            "{code}"
        );
        let typed_413 = SizeLimit::parse(
            status,
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        if status == 413 {
            assert_eq!(typed_413, Ok(Some(SizeLimit::Request(7))));
        }
        if code == ErrorCode::RateLimited {
            assert_eq!(
                response.headers().get(RETRY_AFTER),
                Some(&HeaderValue::from_static("2"))
            );
        }
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let parsed = ErrorBody::parse(&body).expect("an error body");
        assert_eq!(parsed.code, code);
        assert!(!parsed.message.is_empty(), "{code}");
        // No internal address leaks into a message.
        assert!(!parsed.message.contains("node:4437"), "{code}");
    }
}

/// A concealing `404` is the node's `404` for a bucket nothing created,
/// byte for byte: the same status, headers and body (the node side is
/// pinned by `ursula`'s `error_body_tests`).
#[tokio::test]
async fn a_concealing_404_is_the_nodes_missing_bucket_404() {
    let response = Refusal::Concealed.into_response();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let mut headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                value.to_str().expect("text").to_owned(),
            )
        })
        .collect();
    headers.sort();
    assert_eq!(headers, vec![
        ("cache-control".to_owned(), "no-store".to_owned()),
        ("content-type".to_owned(), "application/json".to_owned()),
        (
            "cross-origin-resource-policy".to_owned(),
            "cross-origin".to_owned()
        ),
        ("x-content-type-options".to_owned(), "nosniff".to_owned()),
    ]);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(
        body,
        ErrorBody::fixed(ErrorCode::BucketNotFound)
            .render()
            .as_bytes()
    );
}
