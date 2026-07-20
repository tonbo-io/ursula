//! HTTP protocol-surface scenarios extracted from `madsim_harness/mod.rs`
//! (DoD #3 modularity refactor — workloads axis, HTTP/axum in-process scenarios).

use axum::Router;
use axum::body::Bytes;
use axum::response::Response;

use super::Arc;
use super::AtomicU64;
use super::Body;
use super::Duration;
use super::HttpProtocolSurfacePlan;
use super::HttpRequest;
use super::HttpState;
use super::InMemoryGroupEngineFactory;
use super::Ordering;
use super::RuntimeConfig;
use super::RuntimeThreading;
use super::ServiceExt;
use super::ShardRuntime;
use super::SimEvent;
use super::SimHttpWallClock;
use super::SimTrace;
use super::StatusCode;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::assert_http_protocol_surface_randomized_final_read;
use super::assert_http_protocol_surface_randomized_live_backpressure;
use super::assert_http_protocol_surface_randomized_sse_next_offset;
use super::http_offset;
use super::parse_http_offset;
use super::router_with_http_state;
use super::to_bytes;

/// Spawns the hosted single-core runtime and wraps it in the protocol router
/// with a controllable wall clock starting at `1_000` ms.
fn http_surface_app(
    runtime_config: RuntimeConfig,
    spawn_expect: &'static str,
) -> (Router, Arc<AtomicU64>) {
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        InMemoryGroupEngineFactory::default(),
    )
    .expect(spawn_expect);
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(runtime).with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::clone(&now_ms),
    }));
    (router_with_http_state(state), now_ms)
}

/// Sends one HTTP request through a cloned `Router` and returns the response.
///
/// The status and header expectations stay at the call site; this only folds
/// the `HttpRequest::builder()` / `oneshot` / double-`expect` plumbing.
async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Response {
    let mut request = HttpRequest::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response")
}

/// Returns the named response header as `&str`, panicking when absent.
#[track_caller]
fn header_str<'a>(response: &'a Response, name: &str) -> &'a str {
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}"))
        .to_str()
        .expect("header value is valid utf-8")
}

async fn body_bytes(response: Response) -> Bytes {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
}

pub(super) async fn run_http_protocol_surface_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_snapshot_body_expectation: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let (app, now_ms) = http_surface_app(
        runtime_config,
        "spawn hosted runtime for http protocol surface",
    );
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = send(
        &app,
        "PUT",
        &path,
        &[("content-type", "text/plain"), ("stream-ttl", "1")],
        Body::empty(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let append = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "http-writer"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ],
        Body::from("a"),
    )
    .await;
    assert_eq!(append.status(), StatusCode::OK);

    let duplicate = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "http-writer"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ],
        Body::from("ignored"),
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&duplicate, "stream-next-offset"),
        "00000000000000000001"
    );

    let epoch_bump = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "http-writer"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        Body::from("b"),
    )
    .await;
    assert_eq!(epoch_bump.status(), StatusCode::OK);

    let stale = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "http-writer"),
            ("producer-epoch", "0"),
            ("producer-seq", "1"),
        ],
        Body::from("stale"),
    )
    .await;
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
    assert_eq!(header_str(&stale, "producer-epoch"), "1");

    let read_uri = format!("{path}?offset=0&max_bytes=16");
    let read = send(&app, "GET", &read_uri, &[], Body::empty()).await;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        header_str(&read, "stream-next-offset"),
        "00000000000000000002"
    );
    let body = body_bytes(read).await;
    assert_eq!(&body[..], b"ab");

    let snapshot_path = format!("{path}/snapshot/00000000000000000001");
    let publish_snapshot = send(
        &app,
        "PUT",
        &snapshot_path,
        &[("content-type", "application/json")],
        Body::from(r#"{"state":"a"}"#),
    )
    .await;
    assert_eq!(publish_snapshot.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&publish_snapshot, "stream-snapshot-offset"),
        "00000000000000000001"
    );

    let head_with_snapshot = send(&app, "HEAD", &path, &[], Body::empty()).await;
    assert_eq!(head_with_snapshot.status(), StatusCode::OK);
    assert_eq!(
        header_str(&head_with_snapshot, "stream-snapshot-offset"),
        "00000000000000000001"
    );

    let latest_snapshot = send(&app, "GET", &format!("{path}/snapshot"), &[], Body::empty()).await;
    assert_eq!(latest_snapshot.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        header_str(&latest_snapshot, "location"),
        snapshot_path.as_str()
    );

    let read_snapshot = send(&app, "GET", &snapshot_path, &[], Body::empty()).await;
    assert_eq!(read_snapshot.status(), StatusCode::OK);
    assert_eq!(
        header_str(&read_snapshot, "content-type"),
        "application/json"
    );
    assert_eq!(
        header_str(&read_snapshot, "stream-next-offset"),
        "00000000000000000001"
    );
    let snapshot_body = body_bytes(read_snapshot).await;
    let expected_snapshot_body = if corrupt_snapshot_body_expectation {
        br#"{"state":"corrupt"}"#.as_slice()
    } else {
        br#"{"state":"a"}"#.as_slice()
    };
    if &snapshot_body[..] != expected_snapshot_body {
        let message = format!(
            "snapshot read for stream {} returned {} bytes, expected {} bytes",
            config.stream,
            snapshot_body.len(),
            expected_snapshot_body.len()
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_snapshot_protocol_surface_read".to_owned(),
            after_event: "http_snapshot_protocol_surface_read".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_snapshot_protocol_surface_read` failed after `http_snapshot_protocol_surface_read`: {message}"
        );
    }

    let gone_read = send(
        &app,
        "GET",
        &format!("{path}?offset=0&max_bytes=1"),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(gone_read.status(), StatusCode::GONE);
    assert_eq!(
        header_str(&gone_read, "stream-next-offset"),
        "00000000000000000001"
    );

    let bootstrap = send(
        &app,
        "GET",
        &format!("{path}/bootstrap"),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(bootstrap.status(), StatusCode::OK);
    assert_eq!(
        header_str(&bootstrap, "stream-snapshot-offset"),
        "00000000000000000001"
    );
    assert_eq!(
        header_str(&bootstrap, "stream-next-offset"),
        "00000000000000000002"
    );
    let bootstrap_body = body_bytes(bootstrap).await;
    let bootstrap_body = std::str::from_utf8(&bootstrap_body).expect("utf8 bootstrap body");
    assert!(bootstrap_body.contains(r#"{"state":"a"}"#));
    assert!(bootstrap_body.contains("b"));

    let delete_snapshot = send(&app, "DELETE", &snapshot_path, &[], Body::empty()).await;
    assert_eq!(delete_snapshot.status(), StatusCode::CONFLICT);

    trace.push(SimEvent::HttpSnapshotProtocolSurfaceVerified {
        stream: config.stream.clone(),
        snapshot_offset: 1,
        next_offset: 2,
    });

    now_ms.store(1_999, Ordering::Relaxed);
    let head_before_expiry = send(&app, "HEAD", &path, &[], Body::empty()).await;
    assert_eq!(head_before_expiry.status(), StatusCode::OK);

    now_ms.store(2_000, Ordering::Relaxed);
    let head_after_expiry = send(&app, "HEAD", &path, &[], Body::empty()).await;
    assert_eq!(head_after_expiry.status(), StatusCode::NOT_FOUND);

    trace.push(SimEvent::HttpProtocolSurfaceVerified {
        stream: config.stream.clone(),
        next_offset: 2,
        expired_at_ms: 2_000,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

pub(super) async fn run_http_protocol_surface_randomized_inner(
    config: ThreeNodeRaftSimConfig,
    plan: HttpProtocolSurfacePlan,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    if plan.live_limit {
        runtime_config.live_read_max_waiters_per_core = Some(1);
    }
    let (app, now_ms) = http_surface_app(
        runtime_config,
        "spawn hosted runtime for randomized http protocol surface",
    );
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::HttpProtocolSurfaceRandomizedPlanSelected {
        stream: config.stream.clone(),
        ttl: plan.ttl,
        producer_sessions: plan.producer_sessions,
        producer_sequence_gap: plan.producer_sequence_gap,
        producer_epoch_bump: plan.producer_epoch_bump,
        concurrent_producers: plan.concurrent_producers,
        long_poll: plan.long_poll,
        sse_close: plan.sse_close,
        live_limit: plan.live_limit,
        live_timeout: plan.live_timeout,
        partial_reads: plan.partial_reads,
    });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let mut create_headers = vec![("content-type", "text/plain")];
    if plan.ttl {
        create_headers.push(("stream-ttl", "1"));
    }
    let create = send(&app, "PUT", &path, &create_headers, Body::empty()).await;
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let mut expected_payload = Vec::new();
    if plan.producer_sessions {
        let first = send(
            &app,
            "POST",
            &path,
            &[
                ("content-type", "text/plain"),
                ("producer-id", "random-writer"),
                ("producer-epoch", "0"),
                ("producer-seq", "0"),
            ],
            Body::from("aa"),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        expected_payload.extend_from_slice(b"aa");

        let duplicate = send(
            &app,
            "POST",
            &path,
            &[
                ("content-type", "text/plain"),
                ("producer-id", "random-writer"),
                ("producer-epoch", "0"),
                ("producer-seq", "0"),
            ],
            Body::from("ignored"),
        )
        .await;
        assert_eq!(duplicate.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            header_str(&duplicate, "stream-next-offset"),
            http_offset(expected_payload.len() as u64).as_str()
        );

        if plan.producer_sequence_gap {
            let gap = send(
                &app,
                "POST",
                &path,
                &[
                    ("content-type", "text/plain"),
                    ("producer-id", "random-writer"),
                    ("producer-epoch", "0"),
                    ("producer-seq", "2"),
                ],
                Body::from("gap"),
            )
            .await;
            assert_eq!(gap.status(), StatusCode::CONFLICT);
            assert_eq!(header_str(&gap, "producer-expected-seq"), "1");
            assert_eq!(header_str(&gap, "producer-received-seq"), "2");
            trace.push(SimEvent::HttpProtocolSurfaceRandomizedProducerGapRejected {
                stream: config.stream.clone(),
                expected_seq: 1,
                received_seq: 2,
            });
        }

        if plan.producer_epoch_bump {
            let epoch_bump = send(
                &app,
                "POST",
                &path,
                &[
                    ("content-type", "text/plain"),
                    ("producer-id", "random-writer"),
                    ("producer-epoch", "1"),
                    ("producer-seq", "0"),
                ],
                Body::from("cc"),
            )
            .await;
            assert_eq!(epoch_bump.status(), StatusCode::OK);
            expected_payload.extend_from_slice(b"cc");

            let stale = send(
                &app,
                "POST",
                &path,
                &[
                    ("content-type", "text/plain"),
                    ("producer-id", "random-writer"),
                    ("producer-epoch", "0"),
                    ("producer-seq", "1"),
                ],
                Body::from("stale"),
            )
            .await;
            assert_eq!(stale.status(), StatusCode::FORBIDDEN);
            assert_eq!(header_str(&stale, "producer-epoch"), "1");
        }

        if plan.concurrent_producers {
            let base_next_offset = expected_payload.len() as u64;
            let mut concurrent = Vec::new();
            for (producer_id, payload) in
                [("random-concurrent-a", "pa"), ("random-concurrent-b", "pb")]
            {
                let app = app.clone();
                let path = path.clone();
                concurrent.push(madsim::task::spawn(async move {
                    let response = send(
                        &app,
                        "POST",
                        &path,
                        &[
                            ("content-type", "text/plain"),
                            ("producer-id", producer_id),
                            ("producer-epoch", "0"),
                            ("producer-seq", "0"),
                        ],
                        Body::from(payload),
                    )
                    .await;
                    (producer_id, payload, response)
                }));
            }

            let mut committed = Vec::new();
            for task in concurrent {
                let (producer_id, payload, response) =
                    task.await.expect("randomized concurrent producer task");
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "producer {producer_id} concurrent append should commit"
                );
                let next_offset = parse_http_offset(
                    response
                        .headers()
                        .get("stream-next-offset")
                        .expect("randomized concurrent next offset"),
                );
                committed.push((next_offset, payload.as_bytes().to_vec()));
            }
            committed.sort_by_key(|(next_offset, _)| *next_offset);
            assert_eq!(committed.len(), 2);
            assert_eq!(committed[0].0, base_next_offset + 2);
            assert_eq!(committed[1].0, base_next_offset + 4);
            for (_, payload) in committed {
                expected_payload.extend_from_slice(&payload);
            }
            trace.push(
                SimEvent::HttpProtocolSurfaceRandomizedConcurrentProducersVerified {
                    stream: config.stream.clone(),
                    producer_count: 2,
                    next_offset: expected_payload.len() as u64,
                },
            );
        }
    } else {
        let append = send(
            &app,
            "POST",
            &path,
            &[("content-type", "text/plain")],
            Body::from("aa"),
        )
        .await;
        assert_eq!(append.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"aa");
    }

    if plan.live_timeout && !plan.live_limit {
        let timeout_ms = 25;
        let timeout_uri = format!("{path}?offset=now&live=long-poll&timeout_ms={timeout_ms}");
        let timeout = send(&app, "GET", &timeout_uri, &[], Body::empty()).await;
        assert_eq!(timeout.status(), StatusCode::NO_CONTENT);

        let metrics = send(&app, "GET", "/__ursula/metrics", &[], Body::empty()).await;
        assert_eq!(metrics.status(), StatusCode::OK);
        let metrics_body = body_bytes(metrics).await;
        let metrics_body =
            std::str::from_utf8(&metrics_body).expect("utf8 randomized live-timeout metrics");
        assert!(metrics_body.contains("\"live_read_waiters\":0"));
        trace.push(SimEvent::HttpProtocolSurfaceRandomizedLiveTimeoutVerified {
            stream: config.stream.clone(),
            timeout_ms,
        });
    }

    if plan.live_limit {
        let timeout_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=25");
        let timeout = send(&app, "GET", &timeout_uri, &[], Body::empty()).await;
        assert_eq!(timeout.status(), StatusCode::NO_CONTENT);

        let wait_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
        let wait_app = app.clone();
        let wait_uri_for_task = wait_uri.clone();
        let wait = madsim::task::spawn(async move {
            send(&wait_app, "GET", &wait_uri_for_task, &[], Body::empty()).await
        });
        madsim::time::sleep(Duration::from_millis(10)).await;

        let rejected = send(&app, "GET", &wait_uri, &[], Body::empty()).await;
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);

        let release = send(
            &app,
            "POST",
            &path,
            &[("content-type", "text/plain")],
            Body::from("lp"),
        )
        .await;
        assert_eq!(release.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"lp");

        let wait = wait.await.expect("randomized first live-limit task");
        assert_eq!(wait.status(), StatusCode::OK);
        assert_eq!(
            header_str(&wait, "stream-next-offset"),
            http_offset(expected_payload.len() as u64).as_str()
        );
        let wait_body = body_bytes(wait).await;
        assert_eq!(&wait_body[..], b"lp");
    } else if plan.long_poll {
        let wait_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
        let wait_app = app.clone();
        let wait_uri_for_task = wait_uri.clone();
        let wait = madsim::task::spawn(async move {
            send(&wait_app, "GET", &wait_uri_for_task, &[], Body::empty()).await
        });
        madsim::time::sleep(Duration::from_millis(10)).await;

        let wake = send(
            &app,
            "POST",
            &path,
            &[("content-type", "text/plain")],
            Body::from("lp"),
        )
        .await;
        assert_eq!(wake.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"lp");

        let wait = wait.await.expect("randomized long-poll task");
        assert_eq!(wait.status(), StatusCode::OK);
        assert_eq!(
            header_str(&wait, "stream-next-offset"),
            http_offset(expected_payload.len() as u64).as_str()
        );
    }

    if plan.sse_close {
        let sse_uri = format!("{path}?offset=now&live=sse");
        let sse = send(&app, "GET", &sse_uri, &[], Body::empty()).await;
        assert_eq!(sse.status(), StatusCode::OK);
        let sse_body = madsim::task::spawn(async move {
            to_bytes(sse.into_body(), usize::MAX)
                .await
                .expect("randomized http sse body")
        });
        madsim::time::sleep(Duration::from_millis(10)).await;

        let close = send(
            &app,
            "POST",
            &path,
            &[("content-type", "text/plain"), ("stream-closed", "true")],
            Body::from("sse"),
        )
        .await;
        assert_eq!(close.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"sse");

        let sse_body = sse_body.await.expect("randomized http sse body task");
        let sse_body = std::str::from_utf8(&sse_body).expect("utf8 randomized sse body");
        assert!(sse_body.contains("event: data"));
        assert!(sse_body.contains("data:sse"));
        assert_http_protocol_surface_randomized_sse_next_offset(
            &mut trace,
            &config.stream,
            sse_body,
            expected_payload.len() as u64 + u64::from(plan.corrupt_sse_next_offset_expectation),
        );
    }

    let read_uri = format!("{path}?offset=0&max_bytes=64");
    let read = send(&app, "GET", &read_uri, &[], Body::empty()).await;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        header_str(&read, "stream-next-offset"),
        http_offset(expected_payload.len() as u64).as_str()
    );
    let body = body_bytes(read).await;
    let mut expected_for_final_read = expected_payload.clone();
    if plan.corrupt_final_read_expectation {
        expected_for_final_read.push(b'!');
    }
    assert_http_protocol_surface_randomized_final_read(
        &mut trace,
        &config.stream,
        &body,
        &expected_for_final_read,
        expected_payload.len() as u64 + u64::from(plan.corrupt_final_read_expectation),
    );

    if plan.partial_reads && expected_payload.len() > 1 {
        let partial_offset = 1usize;
        let partial_max_bytes = (expected_payload.len() - partial_offset).min(3);
        let partial_uri = format!("{path}?offset={partial_offset}&max_bytes={partial_max_bytes}");
        let partial = send(&app, "GET", &partial_uri, &[], Body::empty()).await;
        assert_eq!(partial.status(), StatusCode::OK);
        assert_eq!(
            header_str(&partial, "stream-next-offset"),
            http_offset((partial_offset + partial_max_bytes) as u64).as_str()
        );
        let partial_body = body_bytes(partial).await;
        assert_eq!(
            &partial_body[..],
            &expected_payload[partial_offset..partial_offset + partial_max_bytes]
        );
        trace.push(SimEvent::HttpProtocolSurfaceRandomizedPartialReadVerified {
            stream: config.stream.clone(),
            offset: partial_offset as u64,
            max_bytes: partial_max_bytes as u64,
            next_offset: (partial_offset + partial_max_bytes) as u64,
        });
    }

    if plan.live_limit {
        let metrics = send(&app, "GET", "/__ursula/metrics", &[], Body::empty()).await;
        assert_eq!(metrics.status(), StatusCode::OK);
        let metrics_body = body_bytes(metrics).await;
        let metrics_body = std::str::from_utf8(&metrics_body).expect("utf8 randomized metrics");
        assert!(metrics_body.contains("\"live_read_waiters\":0"));
        assert_http_protocol_surface_randomized_live_backpressure(
            &mut trace,
            &config.stream,
            metrics_body,
            1 + u64::from(plan.corrupt_live_limit_backpressure_expectation),
        );
    }

    let ttl_checked = plan.ttl && !plan.sse_close;
    if ttl_checked {
        now_ms.store(1_999, Ordering::Relaxed);
        let head_before_expiry = send(&app, "HEAD", &path, &[], Body::empty()).await;
        assert_eq!(head_before_expiry.status(), StatusCode::OK);

        now_ms.store(2_000, Ordering::Relaxed);
        let head_after_expiry = send(&app, "HEAD", &path, &[], Body::empty()).await;
        assert_eq!(head_after_expiry.status(), StatusCode::NOT_FOUND);
    }

    trace.push(SimEvent::HttpProtocolSurfaceRandomizedVerified {
        stream: config.stream.clone(),
        final_next_offset: expected_payload.len() as u64,
        ttl_checked,
        producer_sessions: plan.producer_sessions,
        producer_sequence_gap: plan.producer_sequence_gap,
        concurrent_producers: plan.concurrent_producers,
        long_poll: plan.long_poll,
        sse_close: plan.sse_close,
        live_limit: plan.live_limit,
        live_timeout: plan.live_timeout,
        partial_reads: plan.partial_reads,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

pub(super) async fn run_http_live_protocol_surface_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_sse_next_offset_expectation: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let (app, _now_ms) = http_surface_app(
        runtime_config,
        "spawn hosted runtime for http live protocol surface",
    );
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = send(
        &app,
        "PUT",
        &path,
        &[("content-type", "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let long_poll_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
    let long_poll_app = app.clone();
    let long_poll_uri_for_task = long_poll_uri.clone();
    let long_poll = madsim::task::spawn(async move {
        send(
            &long_poll_app,
            "GET",
            &long_poll_uri_for_task,
            &[],
            Body::empty(),
        )
        .await
    });
    madsim::time::sleep(Duration::from_millis(10)).await;

    let wake = send(
        &app,
        "POST",
        &path,
        &[("content-type", "text/plain")],
        Body::from("wake"),
    )
    .await;
    assert_eq!(wake.status(), StatusCode::NO_CONTENT);

    let long_poll_response = long_poll.await.expect("http long-poll task");
    assert_eq!(long_poll_response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&long_poll_response, "stream-next-offset"),
        "00000000000000000004"
    );
    assert_eq!(
        header_str(&long_poll_response, "stream-cursor"),
        "00000000000000000004"
    );
    let long_poll_body = body_bytes(long_poll_response).await;
    assert_eq!(&long_poll_body[..], b"wake");

    let sse_uri = format!("{path}?offset=now&live=sse");
    let sse = send(&app, "GET", &sse_uri, &[], Body::empty()).await;
    assert_eq!(sse.status(), StatusCode::OK);
    assert_eq!(header_str(&sse, "content-type"), "text/event-stream");
    let sse_body = madsim::task::spawn(async move {
        to_bytes(sse.into_body(), usize::MAX)
            .await
            .expect("http sse body")
    });
    madsim::time::sleep(Duration::from_millis(10)).await;

    let close = send(
        &app,
        "POST",
        &path,
        &[("content-type", "text/plain"), ("stream-closed", "true")],
        Body::from("sse"),
    )
    .await;
    assert_eq!(close.status(), StatusCode::NO_CONTENT);

    let sse_body = sse_body.await.expect("http sse body task");
    let sse_body = std::str::from_utf8(&sse_body).expect("utf8 sse body");
    assert!(sse_body.contains("event: data"));
    assert!(sse_body.contains("data:sse"));
    let expected_sse_next_offset = if corrupt_sse_next_offset_expectation {
        "\"streamNextOffset\":\"00000000000000000008\""
    } else {
        "\"streamNextOffset\":\"00000000000000000007\""
    };
    if !sse_body.contains(expected_sse_next_offset) {
        let message = format!(
            "SSE body did not contain expected next offset {expected_sse_next_offset}: {sse_body}"
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_live_sse_delivery".to_owned(),
            after_event: "http_sse_body_received".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_live_sse_delivery` failed after `http_sse_body_received`: {message}"
        );
    }
    assert!(sse_body.contains("\"streamClosed\":true"));

    let metrics = send(&app, "GET", "/__ursula/metrics", &[], Body::empty()).await;
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_body = body_bytes(metrics).await;
    let metrics_body = std::str::from_utf8(&metrics_body).expect("utf8 metrics body");
    assert!(metrics_body.contains("\"sse_streams_opened\":1"));
    assert!(metrics_body.contains("\"sse_data_events\":1"));
    assert!(metrics_body.contains("\"sse_error_events\":0"));

    trace.push(SimEvent::HttpLiveProtocolSurfaceVerified {
        stream: config.stream.clone(),
        long_poll_next_offset: 4,
        sse_next_offset: 7,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

pub(super) async fn run_http_live_limit_protocol_surface_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_backpressure_expectation: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    runtime_config.live_read_max_waiters_per_core = Some(1);
    let (app, _now_ms) = http_surface_app(
        runtime_config,
        "spawn hosted runtime for http live limit protocol surface",
    );
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = send(
        &app,
        "PUT",
        &path,
        &[("content-type", "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let timeout_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=25");
    let timeout = send(&app, "GET", &timeout_uri, &[], Body::empty()).await;
    assert_eq!(timeout.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&timeout, "stream-next-offset"),
        "00000000000000000000"
    );
    assert_eq!(header_str(&timeout, "stream-up-to-date"), "true");
    assert_eq!(
        header_str(&timeout, "stream-cursor"),
        "00000000000000000000"
    );

    let first_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
    let first_app = app.clone();
    let first_uri_for_task = first_uri.clone();
    let first = madsim::task::spawn(async move {
        send(&first_app, "GET", &first_uri_for_task, &[], Body::empty()).await
    });
    madsim::time::sleep(Duration::from_millis(10)).await;

    let second = send(&app, "GET", &first_uri, &[], Body::empty()).await;
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    let second_body = body_bytes(second).await;
    assert!(
        std::str::from_utf8(&second_body)
            .expect("utf8 second backpressure response")
            .contains("live read waiters")
    );

    let release = send(
        &app,
        "POST",
        &path,
        &[("content-type", "text/plain")],
        Body::from("open"),
    )
    .await;
    assert_eq!(release.status(), StatusCode::NO_CONTENT);

    let first = first.await.expect("first long-poll task");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        header_str(&first, "stream-next-offset"),
        "00000000000000000004"
    );
    let first_body = body_bytes(first).await;
    assert_eq!(&first_body[..], b"open");

    let metrics = send(&app, "GET", "/__ursula/metrics", &[], Body::empty()).await;
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_body = body_bytes(metrics).await;
    let metrics_body = std::str::from_utf8(&metrics_body).expect("utf8 metrics body");
    assert!(metrics_body.contains("\"live_read_waiters\":0"));
    let expected_backpressure_events = if corrupt_backpressure_expectation {
        "\"live_read_backpressure_events\":2"
    } else {
        "\"live_read_backpressure_events\":1"
    };
    if !metrics_body.contains(expected_backpressure_events) {
        let message = format!(
            "live-read backpressure metrics did not contain expected event count {expected_backpressure_events}: {metrics_body}"
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_live_waiter_backpressure".to_owned(),
            after_event: "http_live_limit_metrics_received".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_live_waiter_backpressure` failed after `http_live_limit_metrics_received`: {message}"
        );
    }

    trace.push(SimEvent::HttpLiveLimitProtocolSurfaceVerified {
        stream: config.stream.clone(),
        timeout_next_offset: 0,
        backpressure_events: 1,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

pub(super) async fn run_http_producer_protocol_surface_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_duplicate_expectation: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let (app, _now_ms) = http_surface_app(
        runtime_config,
        "spawn hosted runtime for http producer protocol surface",
    );
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = send(
        &app,
        "PUT",
        &path,
        &[("content-type", "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let mut concurrent = Vec::new();
    for (producer_id, payload) in [("writer-a", "aa"), ("writer-b", "bb")] {
        let app = app.clone();
        let path = path.clone();
        concurrent.push(madsim::task::spawn(async move {
            let response = send(
                &app,
                "POST",
                &path,
                &[
                    ("content-type", "text/plain"),
                    ("producer-id", producer_id),
                    ("producer-epoch", "0"),
                    ("producer-seq", "0"),
                ],
                Body::from(payload),
            )
            .await;
            (producer_id, payload, response)
        }));
    }

    for task in concurrent {
        let (producer_id, _payload, response) = task.await.expect("http concurrent producer task");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "producer {producer_id} first append should commit"
        );
        assert_eq!(header_str(&response, "producer-epoch"), "0");
        assert_eq!(header_str(&response, "producer-seq"), "0");
    }

    let duplicate = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "writer-a"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ],
        Body::from("ignored"),
    )
    .await;
    let expected_duplicate_status = if corrupt_duplicate_expectation {
        StatusCode::OK
    } else {
        StatusCode::NO_CONTENT
    };
    if duplicate.status() != expected_duplicate_status {
        let message = format!(
            "duplicate producer retry returned {}, expected {}",
            duplicate.status(),
            expected_duplicate_status
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_producer_retry_idempotence".to_owned(),
            after_event: "http_producer_duplicate_retry".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_producer_retry_idempotence` failed after `http_producer_duplicate_retry`: {message}"
        );
    }
    assert_eq!(header_str(&duplicate, "producer-epoch"), "0");
    assert_eq!(header_str(&duplicate, "producer-seq"), "0");

    let gap = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "writer-a"),
            ("producer-epoch", "0"),
            ("producer-seq", "2"),
        ],
        Body::from("gap"),
    )
    .await;
    assert_eq!(gap.status(), StatusCode::CONFLICT);
    assert_eq!(header_str(&gap, "producer-expected-seq"), "1");
    assert_eq!(header_str(&gap, "producer-received-seq"), "2");

    let epoch_bump = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "writer-a"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        Body::from("cc"),
    )
    .await;
    assert_eq!(epoch_bump.status(), StatusCode::OK);
    assert_eq!(header_str(&epoch_bump, "producer-epoch"), "1");
    assert_eq!(header_str(&epoch_bump, "producer-seq"), "0");

    let stale = send(
        &app,
        "POST",
        &path,
        &[
            ("content-type", "text/plain"),
            ("producer-id", "writer-a"),
            ("producer-epoch", "0"),
            ("producer-seq", "1"),
        ],
        Body::from("stale"),
    )
    .await;
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
    assert_eq!(header_str(&stale, "producer-epoch"), "1");

    let read_uri = format!("{path}?offset=0&max_bytes=16");
    let read = send(&app, "GET", &read_uri, &[], Body::empty()).await;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        header_str(&read, "stream-next-offset"),
        "00000000000000000006"
    );
    let body = body_bytes(read).await;
    assert!(
        &body[..] == b"aabbcc" || &body[..] == b"bbaacc",
        "unexpected HTTP producer payload order: {:?}",
        body
    );

    let record_path = format!("{path}-records");
    let create_records = send(
        &app,
        "PUT",
        &record_path,
        &[("content-type", "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(create_records.status(), StatusCode::CREATED);
    assert_eq!(
        header_str(&create_records, "stream-extensions"),
        "json-record-coordinates-v1"
    );

    let append_records = send(
        &app,
        "POST",
        &record_path,
        &[
            ("content-type", "application/json"),
            ("producer-id", "record-writer"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ],
        Body::from(r#"[{"captured_at_ms":120},{"captured_at_ms":100}]"#),
    )
    .await;
    assert_eq!(append_records.status(), StatusCode::OK);
    assert_eq!(header_str(&append_records, "stream-record-start"), "0");
    assert_eq!(header_str(&append_records, "stream-record-next"), "2");

    let duplicate_records = send(
        &app,
        "POST",
        &record_path,
        &[
            ("content-type", "application/json"),
            ("producer-id", "record-writer"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ],
        Body::from(r#"{"ignored":true}"#),
    )
    .await;
    assert_eq!(duplicate_records.status(), StatusCode::NO_CONTENT);
    assert_eq!(header_str(&duplicate_records, "stream-record-next"), "2");

    let record_read = send(
        &app,
        "GET",
        &format!("{record_path}?record=1&max_records=1&record_view=envelope"),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(record_read.status(), StatusCode::OK);
    assert_eq!(header_str(&record_read, "stream-record-next"), "2");
    let record_body = body_bytes(record_read).await;
    assert_eq!(
        &record_body[..],
        br#"{"record":1,"value":{"captured_at_ms":100}}
"#
    );

    trace.push(SimEvent::HttpProducerProtocolSurfaceVerified {
        stream: config.stream.clone(),
        producer_count: 2,
        final_next_offset: 6,
        gap_expected_seq: 1,
        stale_epoch: 0,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}
