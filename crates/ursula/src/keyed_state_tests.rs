//! HTTP tests for `{stream_url}/keyed-state` (keyed-streams P3, U7) against
//! an in-process stub indexer.

use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::header::ACCEPT_ENCODING;
use axum::http::header::ALLOW;
use axum::http::header::CACHE_CONTROL;
use axum::http::header::CONTENT_ENCODING;
use axum::http::header::RETRY_AFTER;
use tower::ServiceExt;

use super::*;
use crate::keyed_state::HEADER_STREAM_KEYED_AFTER;
use crate::keyed_state::HEADER_STREAM_KEYED_THROUGH;
use crate::keyed_state::KEYED_ROWS_CONTENT_TYPE;
use crate::keyed_state::KEYED_STATE_EXTENSION;
use crate::keyed_state::KeyedStateUpstream;

const KEYED_CT: &str = "application/json; profile=keyed-batch-v1";
const CLOCK_MS: u64 = 1_700_000_000_000;

#[derive(Clone)]
struct StubReply {
    status: StatusCode,
    headers: Vec<(&'static str, String)>,
    body: String,
}

impl StubReply {
    fn rows(through: u64, body: &str) -> Self {
        Self {
            status: StatusCode::OK,
            headers: vec![
                (HEADER_STREAM_KEYED_THROUGH, through.to_string()),
                ("content-type", KEYED_ROWS_CONTENT_TYPE.to_owned()),
            ],
            body: body.to_owned(),
        }
    }
}

/// An in-process indexer: records each request target and answers with the
/// configured reply.
#[derive(Clone)]
struct StubIndexer {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    reply: Arc<Mutex<StubReply>>,
}

impl StubIndexer {
    async fn spawn() -> Self {
        Self::spawn_on("127.0.0.1:0").await
    }

    /// Serves on `addr` (a refused port that "comes back", for example).
    async fn spawn_on(addr: &str) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let reply = Arc::new(Mutex::new(StubReply::rows(0, "")));
        let app = Router::new().fallback({
            let requests = requests.clone();
            let reply = reply.clone();
            move |method: Method, uri: Uri| {
                let requests = requests.clone();
                let reply = reply.clone();
                async move {
                    requests.lock().expect("requests").push(format!(
                        "{method} {}",
                        uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("")
                    ));
                    let reply = reply.lock().expect("reply").clone();
                    let mut response = (reply.status, reply.body).into_response();
                    for (name, value) in reply.headers {
                        response
                            .headers_mut()
                            .insert(name, HeaderValue::from_str(&value).expect("header value"));
                    }
                    response
                }
            }
        });
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind stub indexer");
        let url = format!("http://{}", listener.local_addr().expect("stub addr"));
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve stub indexer");
        });
        Self {
            url,
            requests,
            reply,
        }
    }

    fn reply(&self, reply: StubReply) {
        *self.reply.lock().expect("reply") = reply;
    }

    fn take_requests(&self) -> Vec<String> {
        std::mem::take(&mut *self.requests.lock().expect("requests"))
    }

    /// Keyed reads received since the last call (prober `/readyz` excluded).
    fn take_reads(&self) -> usize {
        self.take_requests()
            .iter()
            .filter(|request| request.starts_with("GET /v1/keyed/"))
            .count()
    }
}

/// A loopback address that refuses connections (until a stub binds it).
fn refused_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    drop(listener);
    addr
}

const FAST_FAILOVER: crate::keyed_state::FailoverOptions = crate::keyed_state::FailoverOptions {
    connect_timeout: std::time::Duration::from_millis(500),
    header_timeout: std::time::Duration::from_millis(500),
    unhealthy_backoff: std::time::Duration::from_millis(300),
};

#[derive(Clone)]
struct FixedClock(Arc<AtomicU64>);

impl WallClock for FixedClock {
    fn unix_time_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn runtime() -> ShardRuntime {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 2;
    config.raft.group_count = 8;
    spawn_runtime(&config, Persistence::InMemory, Topology::SingleNode {
        raft_group_count: 8,
    })
    .expect("runtime")
    .runtime
}

async fn app_with_upstream(upstream: Option<&str>) -> (Router, Arc<AtomicU64>) {
    app_with(upstream.map(|url| KeyedStateUpstream::new(url).expect("upstream url"))).await
}

async fn app_with(upstream: Option<KeyedStateUpstream>) -> (Router, Arc<AtomicU64>) {
    let clock = Arc::new(AtomicU64::new(CLOCK_MS));
    let mut state = HttpState::new(runtime()).with_wall_clock(FixedClock(clock.clone()));
    if let Some(upstream) = upstream {
        state = state.with_keyed_state_upstream(upstream);
    }
    let app = router_with_http_state(state);
    let bucket = send(&app, "PUT", "/bkt1", &[], "").await;
    assert!(bucket.status().is_success(), "create bucket");
    // Keyed creates need feature level 1.
    let level = send(
        &app,
        "POST",
        "/__ursula/feature-level",
        &[("content-type", "application/json")],
        r#"{"level":1}"#,
    )
    .await;
    assert_eq!(level.status(), StatusCode::OK);
    (app, clock)
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(Body::from(body.to_owned())).expect("request"))
        .await
        .expect("response")
}

async fn get(app: &Router, uri: &str) -> Response {
    send(app, "GET", uri, &[], "").await
}

async fn body_text(response: Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    String::from_utf8(bytes.to_vec()).expect("utf-8 body")
}

fn header<'a>(response: &'a Response, name: &str) -> Option<&'a str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

fn advertises_keyed_state(response: &Response) -> bool {
    header(response, HEADER_STREAM_EXTENSIONS).is_some_and(|value| {
        value
            .split(',')
            .any(|token| token.trim() == KEYED_STATE_EXTENSION)
    })
}

/// Creates `uri` with `content_type` and appends `records` keyed batches.
async fn create(app: &Router, uri: &str, content_type: &str, records: usize) -> Response {
    let response = send(app, "PUT", uri, &[("content-type", content_type)], "").await;
    if !response.status().is_success() {
        let status = response.status();
        panic!("create {uri}: {status} {}", body_text(response).await);
    }
    for _ in 0..records {
        let append = send(
            app,
            "POST",
            uri,
            &[("content-type", content_type)],
            r#"{"ops":[["p","AQ",1]]}"#,
        )
        .await;
        assert!(append.status().is_success(), "append {uri}");
    }
    response
}

#[tokio::test]
async fn without_an_upstream_the_resource_is_404_and_never_advertised() {
    let (app, _) = app_with_upstream(None).await;
    let created = create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    assert!(!advertises_keyed_state(&created));
    let head = send(&app, "HEAD", "/bkt1/keyed", &[], "").await;
    assert_eq!(head.status(), StatusCode::OK);
    assert!(!advertises_keyed_state(&head));
    for method in ["GET", "HEAD", "PUT", "POST", "DELETE"] {
        let response = send(&app, method, "/bkt1/keyed/keyed-state", &[], "").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method}");
        assert!(!advertises_keyed_state(&response), "{method}");
    }
}

#[tokio::test]
async fn keyed_streams_advertise_keyed_state_on_create_and_head() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;

    let created = create(&app, "/bkt1/keyed", KEYED_CT, 0).await;
    assert_eq!(created.status(), StatusCode::CREATED);
    assert!(advertises_keyed_state(&created));
    // An idempotent re-create also advertises it.
    let again = create(&app, "/bkt1/keyed", KEYED_CT, 0).await;
    assert_eq!(again.status(), StatusCode::OK);
    assert!(advertises_keyed_state(&again));
    let head = send(&app, "HEAD", "/bkt1/keyed", &[], "").await;
    assert!(advertises_keyed_state(&head));
    let created = create(&app, "/bkt1/run/keyed", KEYED_CT, 0).await;
    assert!(advertises_keyed_state(&created));
    let head = send(&app, "HEAD", "/bkt1/run/keyed", &[], "").await;
    assert!(advertises_keyed_state(&head));

    let created = create(&app, "/bkt1/plain", "application/json", 0).await;
    assert!(!advertises_keyed_state(&created));
    let head = send(&app, "HEAD", "/bkt1/plain", &[], "").await;
    assert!(!advertises_keyed_state(&head));
}

#[tokio::test]
async fn absent_and_unkeyed_streams_answer_404_without_the_token() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/plain", "application/json", 0).await;
    create(&app, "/bkt1/text", "text/plain", 0).await;
    for uri in [
        "/bkt1/absent/keyed-state",
        "/bkt1/run/absent/keyed-state",
        "/bkt1/plain/keyed-state",
        "/bkt1/text/keyed-state?min_through_record=0",
    ] {
        let response = get(&app, uri).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        assert!(!advertises_keyed_state(&response), "{uri}");
    }
    assert!(indexer.take_requests().is_empty());
}

#[tokio::test]
async fn other_methods_answer_405_allow_get_without_running_the_read() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    create(&app, "/bkt1/run/keyed", KEYED_CT, 1).await;
    create(&app, "/bkt1/plain", "application/json", 0).await;
    for (uri, keyed) in [
        ("/bkt1/keyed/keyed-state", true),
        ("/bkt1/run/keyed/keyed-state", true),
        ("/bkt1/plain/keyed-state", false),
        ("/bkt1/absent/keyed-state", false),
    ] {
        for method in ["HEAD", "PUT", "POST", "DELETE", "PATCH", "OPTIONS"] {
            let response = send(
                &app,
                method,
                &format!("{uri}?min_through_record=1&timeout_ms=60000"),
                &[],
                "",
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {uri}"
            );
            assert_eq!(header(&response, ALLOW.as_str()), Some("GET"));
            assert_eq!(advertises_keyed_state(&response), keyed, "{method} {uri}");
        }
    }
    assert!(indexer.take_requests().is_empty());
}

#[tokio::test]
async fn invalid_parameters_answer_400_before_404() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    for query in [
        "key=AQ&key=AQ",
        "key=AQ&limit=2",
        "start=AQ&after=AQ",
        "key=AA%3D%3D",
        "limit=0",
        "limit=1001",
        "min_through_record=x",
    ] {
        let response = get(&app, &format!("/bkt1/keyed/keyed-state?{query}")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
        assert!(advertises_keyed_state(&response), "{query}");
        // Parameter 400 precedes 404.
        let response = get(&app, &format!("/bkt1/absent/keyed-state?{query}")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
        assert!(!advertises_keyed_state(&response), "{query}");
    }
    assert!(indexer.take_requests().is_empty());
}

#[tokio::test]
async fn a_wait_beyond_the_record_tail_is_400_with_stream_record_next() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 2).await;

    let response = get(&app, "/bkt1/keyed/keyed-state?min_through_record=3").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(header(&response, HEADER_STREAM_RECORD_NEXT), Some("2"));
    assert!(advertises_keyed_state(&response));
    assert!(indexer.take_requests().is_empty());

    indexer.reply(StubReply::rows(2, ""));
    let response = get(&app, "/bkt1/keyed/keyed-state?min_through_record=2").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(indexer.take_requests().len(), 1);
}

#[tokio::test]
async fn forwards_normalized_parameters_with_incarnation_and_source_next() {
    let indexer = StubIndexer::spawn().await;
    let (app, clock) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 3).await;
    // Distinct clocks keep the two incarnations predictable even when both
    // streams share a group (creates there are unique per group).
    clock.store(CLOCK_MS + 100, Ordering::Relaxed);
    create(&app, "/bkt1/run/keyed", KEYED_CT, 1).await;

    for (uri, expected) in [
        (
            "/bkt1/keyed/keyed-state",
            "GET /v1/keyed/bkt1/keyed?incarnation=1700000000000&source_next=3&limit=100",
        ),
        (
            "/bkt1/keyed/keyed-state?key=AQ",
            "GET /v1/keyed/bkt1/keyed?incarnation=1700000000000&source_next=3&key=AQ",
        ),
        (
            "/bkt1/keyed/keyed-state?after=AQ&end=Ag&limit=7&timeout_ms=5&other=x",
            "GET /v1/keyed/bkt1/keyed?incarnation=1700000000000&source_next=3&after=AQ&end=Ag&limit=7",
        ),
        (
            "/bkt1/keyed/keyed-state?start=AQ&min_through_record=3&timeout_ms=999999",
            "GET /v1/keyed/bkt1/keyed?incarnation=1700000000000&source_next=3&start=AQ&limit=100&min_through_record=3&timeout_ms=60000",
        ),
        (
            "/bkt1/keyed/keyed-state?min_through_record=0&timeout_ms=bad",
            "GET /v1/keyed/bkt1/keyed?incarnation=1700000000000&source_next=3&limit=100&min_through_record=0&timeout_ms=1000",
        ),
        // The affinity form's local name is one encoded segment.
        (
            "/bkt1/run/keyed/keyed-state?key=AQ",
            "GET /v1/keyed/bkt1/run%2Fkeyed?incarnation=1700000000100&source_next=1&key=AQ",
        ),
    ] {
        indexer.reply(StubReply::rows(1, ""));
        let response = get(&app, uri).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(indexer.take_requests(), vec![expected.to_owned()], "{uri}");
    }

    // A recreated stream is a new incarnation.
    let deleted = send(&app, "DELETE", "/bkt1/keyed", &[], "").await;
    assert!(deleted.status().is_success());
    clock.store(CLOCK_MS + 200, Ordering::Relaxed);
    create(&app, "/bkt1/keyed", KEYED_CT, 0).await;
    let response = get(&app, "/bkt1/keyed/keyed-state").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(indexer.take_requests(), vec![
        "GET /v1/keyed/bkt1/keyed?incarnation=1700000000200&source_next=0&limit=100".to_owned()
    ]);
}

#[tokio::test]
async fn maps_indexer_answers() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 3).await;
    let uri = "/bkt1/keyed/keyed-state?min_through_record=3&timeout_ms=10";

    // 200: rows, D, the continuation key, token and no-store.
    let rows = "{\"key\":\"AQ\",\"record\":2,\"value\":1}\n";
    let mut reply = StubReply::rows(3, rows);
    reply
        .headers
        .push((HEADER_STREAM_KEYED_AFTER, "AQ".to_owned()));
    indexer.reply(reply);
    let response = get(&app, uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header(&response, CONTENT_TYPE.as_str()),
        Some(KEYED_ROWS_CONTENT_TYPE)
    );
    assert_eq!(header(&response, HEADER_STREAM_KEYED_THROUGH), Some("3"));
    assert_eq!(header(&response, HEADER_STREAM_KEYED_AFTER), Some("AQ"));
    assert_eq!(header(&response, CACHE_CONTROL.as_str()), Some("no-store"));
    assert!(advertises_keyed_state(&response));
    assert_eq!(body_text(response).await, rows);

    // 204 after a timed-out wait.
    indexer.reply(StubReply {
        status: StatusCode::NO_CONTENT,
        headers: vec![(HEADER_STREAM_KEYED_THROUGH, "2".to_owned())],
        body: String::new(),
    });
    let response = get(&app, uri).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header(&response, HEADER_STREAM_KEYED_THROUGH), Some("2"));
    assert_eq!(header(&response, CACHE_CONTROL.as_str()), Some("no-store"));
    assert!(advertises_keyed_state(&response));

    // 500 keeps the plain-text reason.
    indexer.reply(StubReply {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        headers: Vec::new(),
        body: "record 1 cannot be applied".to_owned(),
    });
    let response = get(&app, uri).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(advertises_keyed_state(&response));
    assert_eq!(body_text(response).await, "record 1 cannot be applied");

    // 503 keeps the indexer's Retry-After, or supplies one.
    indexer.reply(StubReply {
        status: StatusCode::SERVICE_UNAVAILABLE,
        headers: vec![("retry-after", "7".to_owned())],
        body: "rebuilding".to_owned(),
    });
    let response = get(&app, uri).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(header(&response, RETRY_AFTER.as_str()), Some("7"));
    assert!(advertises_keyed_state(&response));
    for reply in [
        StubReply {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: Vec::new(),
            body: String::new(),
        },
        // A 200 without `D` cannot be served as state(D).
        StubReply {
            status: StatusCode::OK,
            headers: Vec::new(),
            body: rows.to_owned(),
        },
        // Statuses outside the contract are not passed through.
        StubReply {
            status: StatusCode::NOT_FOUND,
            headers: Vec::new(),
            body: String::new(),
        },
    ] {
        let status = reply.status;
        indexer.reply(reply);
        let response = get(&app, uri).await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "stub {status}"
        );
        assert_eq!(header(&response, RETRY_AFTER.as_str()), Some("1"));
        assert!(advertises_keyed_state(&response));
    }
}

#[tokio::test]
async fn an_unreachable_indexer_is_503() {
    // Bind then drop a listener so the port refuses connections.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    drop(listener);
    let (app, _) = app_with_upstream(Some(&url)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(header(&response, RETRY_AFTER.as_str()), Some("1"));
    assert!(advertises_keyed_state(&response));
}

#[tokio::test]
async fn rows_are_gzipped_when_accepted() {
    let indexer = StubIndexer::spawn().await;
    let (app, _) = app_with_upstream(Some(&indexer.url)).await;
    create(&app, "/bkt1/run/keyed", KEYED_CT, 1).await;
    let rows: String = (0..40)
        .map(|_| "{\"key\":\"AQ\",\"record\":0,\"value\":{\"text\":\"hello\"}}\n")
        .collect();
    indexer.reply(StubReply::rows(1, &rows));

    let response = send(
        &app,
        "GET",
        "/bkt1/run/keyed/keyed-state",
        &[(ACCEPT_ENCODING.as_str(), "gzip")],
        "",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, CONTENT_ENCODING.as_str()), Some("gzip"));
    assert!(advertises_keyed_state(&response));
    let compressed = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(&compressed[..])
        .read_to_string(&mut decoded)
        .expect("gunzip");
    assert_eq!(decoded, rows);
}

async fn upstream_metrics(app: &Router) -> serde_json::Value {
    let response = get(app, "/__ursula/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics: serde_json::Value =
        serde_json::from_str(&body_text(response).await).expect("metrics JSON");
    metrics["keyed_state_upstream"].clone()
}

/// Polls until the upstream metrics report `active_pod == pod`.
async fn until_active(app: &Router, pod: u64) -> serde_json::Value {
    for _ in 0..100 {
        let metrics = upstream_metrics(app).await;
        if metrics["active_pod"] == pod {
            return metrics;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!(
        "pod {pod} never became active: {}",
        upstream_metrics(app).await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_down_fails_over_to_the_standby_and_returns_when_it_recovers() {
    let primary_addr = refused_addr();
    let standby = StubIndexer::spawn().await;
    standby.reply(StubReply::rows(
        3,
        "{\"key\":\"AQ\",\"record\":2,\"value\":1}\n",
    ));
    let upstream = KeyedStateUpstream::with_pods(
        [format!("http://{primary_addr}"), standby.url.clone()],
        FAST_FAILOVER,
    )
    .expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 3).await;
    let uri = "/bkt1/keyed/keyed-state?min_through_record=3&timeout_ms=200";

    // The primary refuses connections: the standby answers within the
    // same request, and the primary leaves the order.
    let response = get(&app, uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, HEADER_STREAM_KEYED_THROUGH), Some("3"));
    assert_eq!(standby.take_reads(), 1);
    let metrics = upstream_metrics(&app).await;
    assert_eq!(metrics["active_pod"], 1, "{metrics}");
    assert_eq!(metrics["unhealthy_pods"], 1, "{metrics}");
    assert_eq!(metrics["failovers"], 1, "{metrics}");
    assert_eq!(metrics["pods"][0]["healthy"], false, "{metrics}");

    // While it is unhealthy, reads go straight to the standby.
    for _ in 0..3 {
        assert_eq!(get(&app, uri).await.status(), StatusCode::OK);
    }
    assert_eq!(standby.take_reads(), 3);
    assert_eq!(upstream_metrics(&app).await["failovers"], 1);

    // The primary comes back: the prober sees /readyz and traffic returns.
    let primary = StubIndexer::spawn_on(&primary_addr).await;
    primary.reply(StubReply::rows(4, ""));
    let metrics = until_active(&app, 0).await;
    assert_eq!(metrics["unhealthy_pods"], 0, "{metrics}");
    assert!(
        primary
            .take_requests()
            .iter()
            .any(|request| request == "GET /readyz"),
        "the prober probes /readyz"
    );
    let response = get(&app, uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, HEADER_STREAM_KEYED_THROUGH), Some("4"));
    assert_eq!(primary.take_reads(), 1);
    assert_eq!(standby.take_reads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_503_from_the_primary_fails_over_and_passes_the_remaining_wait_on() {
    let primary = StubIndexer::spawn().await;
    let standby = StubIndexer::spawn().await;
    primary.reply(StubReply {
        status: StatusCode::SERVICE_UNAVAILABLE,
        headers: vec![("retry-after", "7".to_owned())],
        body: "too many waiting keyed-state reads".to_owned(),
    });
    standby.reply(StubReply {
        status: StatusCode::NO_CONTENT,
        headers: vec![(HEADER_STREAM_KEYED_THROUGH, "2".to_owned())],
        body: String::new(),
    });
    let upstream =
        KeyedStateUpstream::with_pods([primary.url.clone(), standby.url.clone()], FAST_FAILOVER)
            .expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 3).await;

    let response = get(
        &app,
        "/bkt1/keyed/keyed-state?min_through_record=3&timeout_ms=5000",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header(&response, HEADER_STREAM_KEYED_THROUGH), Some("2"));
    assert_eq!(primary.take_reads(), 1);
    let standby_requests = standby.take_requests();
    assert_eq!(standby_requests.len(), 1, "{standby_requests:?}");
    // The standby gets what is left of the read's wait, never more.
    let timeout_ms = standby_requests[0]
        .rsplit_once("timeout_ms=")
        .and_then(|(_, ms)| ms.parse::<u64>().ok())
        .expect("timeout_ms");
    assert!((1..=5000).contains(&timeout_ms), "{timeout_ms}");

    // The primary answers again; after its backoff the prober restores it.
    primary.reply(StubReply::rows(3, ""));
    until_active(&app, 0).await;
    let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(primary.take_reads(), 1);
    assert_eq!(standby.take_reads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_pod_down_is_503_with_retry_after() {
    let upstream = KeyedStateUpstream::with_pods(
        [
            format!("http://{}", refused_addr()),
            format!("http://{}", refused_addr()),
        ],
        FAST_FAILOVER,
    )
    .expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    for _ in 0..2 {
        let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(header(&response, RETRY_AFTER.as_str()), Some("1"));
        assert!(advertises_keyed_state(&response));
    }
    let metrics = upstream_metrics(&app).await;
    assert_eq!(metrics["active_pod"], serde_json::Value::Null, "{metrics}");
    assert_eq!(metrics["unhealthy_pods"], 2, "{metrics}");

    // Both answering 503: the last answer's Retry-After is passed on.
    let primary = StubIndexer::spawn().await;
    let standby = StubIndexer::spawn().await;
    for (stub, retry_after) in [(&primary, "4"), (&standby, "9")] {
        stub.reply(StubReply {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: vec![("retry-after", retry_after.to_owned())],
            body: "keyed state is being rebuilt".to_owned(),
        });
    }
    let upstream =
        KeyedStateUpstream::with_pods([primary.url.clone(), standby.url.clone()], FAST_FAILOVER)
            .expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(header(&response, RETRY_AFTER.as_str()), Some("9"));
    assert_eq!(body_text(response).await, "keyed state is being rebuilt");
    assert_eq!(primary.take_reads(), 1);
    assert_eq!(standby.take_reads(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_primary_fails_over_after_the_header_timeout() {
    // Accepts connections but never answers.
    let hung = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let hung_url = format!("http://{}", hung.local_addr().expect("addr"));
    let _accepting = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = hung.accept().await {
            held.push(socket);
        }
    });
    let standby = StubIndexer::spawn().await;
    standby.reply(StubReply::rows(1, ""));
    let upstream = KeyedStateUpstream::with_pods([hung_url, standby.url.clone()], FAST_FAILOVER)
        .expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    let started = std::time::Instant::now();
    let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(standby.take_reads(), 1);
    assert_eq!(upstream_metrics(&app).await["pods"][0]["healthy"], false);
}

/// A pod that accepts connections and answers every request with `head`
/// (status line and headers), then never sends the body it announced.
async fn stalling_pod(head: &'static str) -> String {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = [0_u8; 4096];
                let _read = socket.read(&mut request).await;
                let _written = socket.write_all(head.as_bytes()).await;
                // Hold the connection open with the body unsent.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                drop(socket);
            });
        }
    });
    url
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_primary_that_stalls_after_its_headers_fails_over_within_the_request() {
    let primary =
        stalling_pod("HTTP/1.1 200 OK\r\nstream-keyed-through: 1\r\ncontent-length: 100\r\n\r\n")
            .await;
    let standby = StubIndexer::spawn().await;
    standby.reply(StubReply::rows(1, ""));
    let upstream = KeyedStateUpstream::with_pods([primary, standby.url.clone()], FAST_FAILOVER)
        .expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    let started = std::time::Instant::now();
    let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, HEADER_STREAM_KEYED_THROUGH), Some("1"));
    // Headers plus the body allowance (2 × 500 ms), not the 30 s budget.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(standby.take_reads(), 1);
    assert_eq!(upstream_metrics(&app).await["pods"][0]["healthy"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_503_with_a_slow_body_fails_over_right_after_its_headers() {
    let primary = stalling_pod(
        "HTTP/1.1 503 Service Unavailable\r\nretry-after: 3\r\ncontent-length: 100\r\n\r\n",
    )
    .await;
    let standby = StubIndexer::spawn().await;
    standby.reply(StubReply::rows(1, ""));
    let options = crate::keyed_state::FailoverOptions {
        header_timeout: std::time::Duration::from_secs(2),
        ..FAST_FAILOVER
    };
    let upstream =
        KeyedStateUpstream::with_pods([primary, standby.url.clone()], options).expect("upstream");
    let (app, _) = app_with(Some(upstream)).await;
    create(&app, "/bkt1/keyed", KEYED_CT, 1).await;
    let started = std::time::Instant::now();
    let response = get(&app, "/bkt1/keyed/keyed-state?key=AQ").await;
    assert_eq!(response.status(), StatusCode::OK);
    // Reading the body would hold the request for the 2 s header deadline
    // plus the 2 s body allowance.
    assert!(
        started.elapsed() < std::time::Duration::from_millis(1_500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(standby.take_reads(), 1);
    assert_eq!(upstream_metrics(&app).await["pods"][0]["healthy"], false);
}
