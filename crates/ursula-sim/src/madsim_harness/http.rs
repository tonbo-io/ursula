//! HTTP protocol-surface scenarios extracted from `madsim_harness/mod.rs`
//! (DoD #3 modularity refactor — workloads axis, HTTP/axum in-process scenarios).

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

pub(super) async fn run_http_protocol_surface_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_snapshot_body_expectation: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn hosted runtime for http protocol surface");
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(runtime).with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::clone(&now_ms),
    }));
    let app = router_with_http_state(state);
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("stream-ttl", "1")
                .body(Body::empty())
                .expect("http create request"),
        )
        .await
        .expect("http create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let append = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "http-writer")
                .header("producer-epoch", "0")
                .header("producer-seq", "0")
                .body(Body::from("a"))
                .expect("http append request"),
        )
        .await
        .expect("http append response");
    assert_eq!(append.status(), StatusCode::OK);

    let duplicate = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "http-writer")
                .header("producer-epoch", "0")
                .header("producer-seq", "0")
                .body(Body::from("ignored"))
                .expect("http duplicate producer request"),
        )
        .await
        .expect("http duplicate producer response");
    assert_eq!(duplicate.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        duplicate
            .headers()
            .get("stream-next-offset")
            .expect("duplicate next offset"),
        "00000000000000000001"
    );

    let epoch_bump = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "http-writer")
                .header("producer-epoch", "1")
                .header("producer-seq", "0")
                .body(Body::from("b"))
                .expect("http epoch bump request"),
        )
        .await
        .expect("http epoch bump response");
    assert_eq!(epoch_bump.status(), StatusCode::OK);

    let stale = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "http-writer")
                .header("producer-epoch", "0")
                .header("producer-seq", "1")
                .body(Body::from("stale"))
                .expect("http stale producer request"),
        )
        .await
        .expect("http stale producer response");
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        stale
            .headers()
            .get("producer-epoch")
            .expect("stale current epoch"),
        "1"
    );

    let read_uri = format!("{path}?offset=0&max_bytes=16");
    let read = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&read_uri)
                .body(Body::empty())
                .expect("http read request"),
        )
        .await
        .expect("http read response");
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        read.headers()
            .get("stream-next-offset")
            .expect("read next offset"),
        "00000000000000000002"
    );
    let body = to_bytes(read.into_body(), usize::MAX)
        .await
        .expect("http read body");
    assert_eq!(&body[..], b"ab");

    let snapshot_path = format!("{path}/snapshot/00000000000000000001");
    let publish_snapshot = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&snapshot_path)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"state":"a"}"#))
                .expect("http snapshot publish request"),
        )
        .await
        .expect("http snapshot publish response");
    assert_eq!(publish_snapshot.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        publish_snapshot
            .headers()
            .get("stream-snapshot-offset")
            .expect("published snapshot offset"),
        "00000000000000000001"
    );

    let head_with_snapshot = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("HEAD")
                .uri(&path)
                .body(Body::empty())
                .expect("http head with snapshot request"),
        )
        .await
        .expect("http head with snapshot response");
    assert_eq!(head_with_snapshot.status(), StatusCode::OK);
    assert_eq!(
        head_with_snapshot
            .headers()
            .get("stream-snapshot-offset")
            .expect("head snapshot offset"),
        "00000000000000000001"
    );

    let latest_snapshot = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!("{path}/snapshot"))
                .body(Body::empty())
                .expect("http latest snapshot request"),
        )
        .await
        .expect("http latest snapshot response");
    assert_eq!(latest_snapshot.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        latest_snapshot
            .headers()
            .get("location")
            .expect("latest snapshot location"),
        snapshot_path.as_str()
    );

    let read_snapshot = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&snapshot_path)
                .body(Body::empty())
                .expect("http snapshot read request"),
        )
        .await
        .expect("http snapshot read response");
    assert_eq!(read_snapshot.status(), StatusCode::OK);
    assert_eq!(
        read_snapshot
            .headers()
            .get("content-type")
            .expect("snapshot content type"),
        "application/json"
    );
    assert_eq!(
        read_snapshot
            .headers()
            .get("stream-next-offset")
            .expect("snapshot next offset"),
        "00000000000000000001"
    );
    let snapshot_body = to_bytes(read_snapshot.into_body(), usize::MAX)
        .await
        .expect("http snapshot read body");
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

    let gone_read = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!("{path}?offset=0&max_bytes=1"))
                .body(Body::empty())
                .expect("http retained prefix read request"),
        )
        .await
        .expect("http retained prefix read response");
    assert_eq!(gone_read.status(), StatusCode::GONE);
    assert_eq!(
        gone_read
            .headers()
            .get("stream-next-offset")
            .expect("retained prefix gone next offset"),
        "00000000000000000001"
    );

    let bootstrap = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!("{path}/bootstrap"))
                .body(Body::empty())
                .expect("http bootstrap request"),
        )
        .await
        .expect("http bootstrap response");
    assert_eq!(bootstrap.status(), StatusCode::OK);
    assert_eq!(
        bootstrap
            .headers()
            .get("stream-snapshot-offset")
            .expect("bootstrap snapshot offset"),
        "00000000000000000001"
    );
    assert_eq!(
        bootstrap
            .headers()
            .get("stream-next-offset")
            .expect("bootstrap next offset"),
        "00000000000000000002"
    );
    let bootstrap_body = to_bytes(bootstrap.into_body(), usize::MAX)
        .await
        .expect("http bootstrap body");
    let bootstrap_body = std::str::from_utf8(&bootstrap_body).expect("utf8 bootstrap body");
    assert!(bootstrap_body.contains(r#"{"state":"a"}"#));
    assert!(bootstrap_body.contains("b"));

    let delete_snapshot = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("DELETE")
                .uri(&snapshot_path)
                .body(Body::empty())
                .expect("http snapshot delete request"),
        )
        .await
        .expect("http snapshot delete response");
    assert_eq!(delete_snapshot.status(), StatusCode::CONFLICT);

    trace.push(SimEvent::HttpSnapshotProtocolSurfaceVerified {
        stream: config.stream.clone(),
        snapshot_offset: 1,
        next_offset: 2,
    });

    now_ms.store(1_999, Ordering::Relaxed);
    let head_before_expiry = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("HEAD")
                .uri(&path)
                .body(Body::empty())
                .expect("http head before expiry request"),
        )
        .await
        .expect("http head before expiry response");
    assert_eq!(head_before_expiry.status(), StatusCode::OK);

    now_ms.store(2_000, Ordering::Relaxed);
    let head_after_expiry = app
        .oneshot(
            HttpRequest::builder()
                .method("HEAD")
                .uri(&path)
                .body(Body::empty())
                .expect("http head after expiry request"),
        )
        .await
        .expect("http head after expiry response");
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
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn hosted runtime for randomized http protocol surface");
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(runtime).with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::clone(&now_ms),
    }));
    let app = router_with_http_state(state);
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
    let mut create_builder = HttpRequest::builder()
        .method("PUT")
        .uri(&path)
        .header("content-type", "text/plain");
    if plan.ttl {
        create_builder = create_builder.header("stream-ttl", "1");
    }
    let create = app
        .clone()
        .oneshot(
            create_builder
                .body(Body::empty())
                .expect("randomized http create request"),
        )
        .await
        .expect("randomized http create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let mut expected_payload = Vec::new();
    if plan.producer_sessions {
        let first = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "text/plain")
                    .header("producer-id", "random-writer")
                    .header("producer-epoch", "0")
                    .header("producer-seq", "0")
                    .body(Body::from("aa"))
                    .expect("randomized http producer append request"),
            )
            .await
            .expect("randomized http producer append response");
        assert_eq!(first.status(), StatusCode::OK);
        expected_payload.extend_from_slice(b"aa");

        let duplicate = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "text/plain")
                    .header("producer-id", "random-writer")
                    .header("producer-epoch", "0")
                    .header("producer-seq", "0")
                    .body(Body::from("ignored"))
                    .expect("randomized http duplicate producer request"),
            )
            .await
            .expect("randomized http duplicate producer response");
        assert_eq!(duplicate.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            duplicate
                .headers()
                .get("stream-next-offset")
                .expect("randomized duplicate next offset"),
            http_offset(expected_payload.len() as u64).as_str()
        );

        if plan.producer_sequence_gap {
            let gap = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(&path)
                        .header("content-type", "text/plain")
                        .header("producer-id", "random-writer")
                        .header("producer-epoch", "0")
                        .header("producer-seq", "2")
                        .body(Body::from("gap"))
                        .expect("randomized http producer gap request"),
                )
                .await
                .expect("randomized http producer gap response");
            assert_eq!(gap.status(), StatusCode::CONFLICT);
            assert_eq!(
                gap.headers()
                    .get("producer-expected-seq")
                    .expect("randomized gap expected seq"),
                "1"
            );
            assert_eq!(
                gap.headers()
                    .get("producer-received-seq")
                    .expect("randomized gap received seq"),
                "2"
            );
            trace.push(SimEvent::HttpProtocolSurfaceRandomizedProducerGapRejected {
                stream: config.stream.clone(),
                expected_seq: 1,
                received_seq: 2,
            });
        }

        if plan.producer_epoch_bump {
            let epoch_bump = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(&path)
                        .header("content-type", "text/plain")
                        .header("producer-id", "random-writer")
                        .header("producer-epoch", "1")
                        .header("producer-seq", "0")
                        .body(Body::from("cc"))
                        .expect("randomized http producer epoch bump request"),
                )
                .await
                .expect("randomized http producer epoch bump response");
            assert_eq!(epoch_bump.status(), StatusCode::OK);
            expected_payload.extend_from_slice(b"cc");

            let stale = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(&path)
                        .header("content-type", "text/plain")
                        .header("producer-id", "random-writer")
                        .header("producer-epoch", "0")
                        .header("producer-seq", "1")
                        .body(Body::from("stale"))
                        .expect("randomized http stale producer request"),
                )
                .await
                .expect("randomized http stale producer response");
            assert_eq!(stale.status(), StatusCode::FORBIDDEN);
            assert_eq!(
                stale
                    .headers()
                    .get("producer-epoch")
                    .expect("randomized stale current epoch"),
                "1"
            );
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
                    let response = app
                        .oneshot(
                            HttpRequest::builder()
                                .method("POST")
                                .uri(&path)
                                .header("content-type", "text/plain")
                                .header("producer-id", producer_id)
                                .header("producer-epoch", "0")
                                .header("producer-seq", "0")
                                .body(Body::from(payload))
                                .expect("randomized concurrent http producer request"),
                        )
                        .await
                        .expect("randomized concurrent http producer response");
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
        let append = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "text/plain")
                    .body(Body::from("aa"))
                    .expect("randomized http append request"),
            )
            .await
            .expect("randomized http append response");
        assert_eq!(append.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"aa");
    }

    if plan.live_timeout && !plan.live_limit {
        let timeout_ms = 25;
        let timeout_uri = format!("{path}?offset=now&live=long-poll&timeout_ms={timeout_ms}");
        let timeout = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&timeout_uri)
                    .body(Body::empty())
                    .expect("randomized http live-timeout request"),
            )
            .await
            .expect("randomized http live-timeout response");
        assert_eq!(timeout.status(), StatusCode::NO_CONTENT);

        let metrics = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("randomized http live-timeout metrics request"),
            )
            .await
            .expect("randomized http live-timeout metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);
        let metrics_body = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("randomized http live-timeout metrics body");
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
        let timeout = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&timeout_uri)
                    .body(Body::empty())
                    .expect("randomized http long-poll timeout request"),
            )
            .await
            .expect("randomized http long-poll timeout response");
        assert_eq!(timeout.status(), StatusCode::NO_CONTENT);

        let wait_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
        let wait_app = app.clone();
        let wait_uri_for_task = wait_uri.clone();
        let wait = madsim::task::spawn(async move {
            wait_app
                .oneshot(
                    HttpRequest::builder()
                        .method("GET")
                        .uri(&wait_uri_for_task)
                        .body(Body::empty())
                        .expect("randomized first live-limit request"),
                )
                .await
                .expect("randomized first live-limit response")
        });
        madsim::time::sleep(Duration::from_millis(10)).await;

        let rejected = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&wait_uri)
                    .body(Body::empty())
                    .expect("randomized second live-limit request"),
            )
            .await
            .expect("randomized second live-limit response");
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);

        let release = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "text/plain")
                    .body(Body::from("lp"))
                    .expect("randomized live-limit release append request"),
            )
            .await
            .expect("randomized live-limit release append response");
        assert_eq!(release.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"lp");

        let wait = wait.await.expect("randomized first live-limit task");
        assert_eq!(wait.status(), StatusCode::OK);
        assert_eq!(
            wait.headers()
                .get("stream-next-offset")
                .expect("randomized live-limit next offset"),
            http_offset(expected_payload.len() as u64).as_str()
        );
        let wait_body = to_bytes(wait.into_body(), usize::MAX)
            .await
            .expect("randomized live-limit body");
        assert_eq!(&wait_body[..], b"lp");
    } else if plan.long_poll {
        let wait_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
        let wait_app = app.clone();
        let wait_uri_for_task = wait_uri.clone();
        let wait = madsim::task::spawn(async move {
            wait_app
                .oneshot(
                    HttpRequest::builder()
                        .method("GET")
                        .uri(&wait_uri_for_task)
                        .body(Body::empty())
                        .expect("randomized long-poll request"),
                )
                .await
                .expect("randomized long-poll response")
        });
        madsim::time::sleep(Duration::from_millis(10)).await;

        let wake = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "text/plain")
                    .body(Body::from("lp"))
                    .expect("randomized long-poll wake append request"),
            )
            .await
            .expect("randomized long-poll wake append response");
        assert_eq!(wake.status(), StatusCode::NO_CONTENT);
        expected_payload.extend_from_slice(b"lp");

        let wait = wait.await.expect("randomized long-poll task");
        assert_eq!(wait.status(), StatusCode::OK);
        assert_eq!(
            wait.headers()
                .get("stream-next-offset")
                .expect("randomized long-poll next offset"),
            http_offset(expected_payload.len() as u64).as_str()
        );
    }

    if plan.sse_close {
        let sse_uri = format!("{path}?offset=now&live=sse");
        let sse = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&sse_uri)
                    .body(Body::empty())
                    .expect("randomized http sse request"),
            )
            .await
            .expect("randomized http sse response");
        assert_eq!(sse.status(), StatusCode::OK);
        let sse_body = madsim::task::spawn(async move {
            to_bytes(sse.into_body(), usize::MAX)
                .await
                .expect("randomized http sse body")
        });
        madsim::time::sleep(Duration::from_millis(10)).await;

        let close = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "text/plain")
                    .header("stream-closed", "true")
                    .body(Body::from("sse"))
                    .expect("randomized http sse close append request"),
            )
            .await
            .expect("randomized http sse close append response");
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
    let read = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&read_uri)
                .body(Body::empty())
                .expect("randomized http final read request"),
        )
        .await
        .expect("randomized http final read response");
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        read.headers()
            .get("stream-next-offset")
            .expect("randomized final read next offset"),
        http_offset(expected_payload.len() as u64).as_str()
    );
    let body = to_bytes(read.into_body(), usize::MAX)
        .await
        .expect("randomized http final read body");
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
        let partial = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&partial_uri)
                    .body(Body::empty())
                    .expect("randomized http partial read request"),
            )
            .await
            .expect("randomized http partial read response");
        assert_eq!(partial.status(), StatusCode::OK);
        assert_eq!(
            partial
                .headers()
                .get("stream-next-offset")
                .expect("randomized partial read next offset"),
            http_offset((partial_offset + partial_max_bytes) as u64).as_str()
        );
        let partial_body = to_bytes(partial.into_body(), usize::MAX)
            .await
            .expect("randomized http partial read body");
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
        let metrics = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("randomized http metrics request"),
            )
            .await
            .expect("randomized http metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);
        let metrics_body = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("randomized http metrics body");
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
        let head_before_expiry = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("HEAD")
                    .uri(&path)
                    .body(Body::empty())
                    .expect("randomized http head before expiry request"),
            )
            .await
            .expect("randomized http head before expiry response");
        assert_eq!(head_before_expiry.status(), StatusCode::OK);

        now_ms.store(2_000, Ordering::Relaxed);
        let head_after_expiry = app
            .oneshot(
                HttpRequest::builder()
                    .method("HEAD")
                    .uri(&path)
                    .body(Body::empty())
                    .expect("randomized http head after expiry request"),
            )
            .await
            .expect("randomized http head after expiry response");
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
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn hosted runtime for http live protocol surface");
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(runtime).with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::clone(&now_ms),
    }));
    let app = router_with_http_state(state);
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&path)
                .header("content-type", "text/plain")
                .body(Body::empty())
                .expect("http live create request"),
        )
        .await
        .expect("http live create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let long_poll_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
    let long_poll_app = app.clone();
    let long_poll_uri_for_task = long_poll_uri.clone();
    let long_poll = madsim::task::spawn(async move {
        long_poll_app
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&long_poll_uri_for_task)
                    .body(Body::empty())
                    .expect("http long-poll request"),
            )
            .await
            .expect("http long-poll response")
    });
    madsim::time::sleep(Duration::from_millis(10)).await;

    let wake = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .body(Body::from("wake"))
                .expect("http long-poll wake append request"),
        )
        .await
        .expect("http long-poll wake append response");
    assert_eq!(wake.status(), StatusCode::NO_CONTENT);

    let long_poll_response = long_poll.await.expect("http long-poll task");
    assert_eq!(long_poll_response.status(), StatusCode::OK);
    assert_eq!(
        long_poll_response
            .headers()
            .get("stream-next-offset")
            .expect("long-poll next offset"),
        "00000000000000000004"
    );
    assert_eq!(
        long_poll_response
            .headers()
            .get("stream-cursor")
            .expect("long-poll cursor"),
        "00000000000000000004"
    );
    let long_poll_body = to_bytes(long_poll_response.into_body(), usize::MAX)
        .await
        .expect("http long-poll body");
    assert_eq!(&long_poll_body[..], b"wake");

    let sse_uri = format!("{path}?offset=now&live=sse");
    let sse = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&sse_uri)
                .body(Body::empty())
                .expect("http sse request"),
        )
        .await
        .expect("http sse response");
    assert_eq!(sse.status(), StatusCode::OK);
    assert_eq!(
        sse.headers().get("content-type").expect("sse content type"),
        "text/event-stream"
    );
    let sse_body = madsim::task::spawn(async move {
        to_bytes(sse.into_body(), usize::MAX)
            .await
            .expect("http sse body")
    });
    madsim::time::sleep(Duration::from_millis(10)).await;

    let close = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("stream-closed", "true")
                .body(Body::from("sse"))
                .expect("http sse close append request"),
        )
        .await
        .expect("http sse close append response");
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

    let metrics = app
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("http metrics request"),
        )
        .await
        .expect("http metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_body = to_bytes(metrics.into_body(), usize::MAX)
        .await
        .expect("http metrics body");
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
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn hosted runtime for http live limit protocol surface");
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(runtime).with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::clone(&now_ms),
    }));
    let app = router_with_http_state(state);
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&path)
                .header("content-type", "text/plain")
                .body(Body::empty())
                .expect("http live limit create request"),
        )
        .await
        .expect("http live limit create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let timeout_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=25");
    let timeout = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&timeout_uri)
                .body(Body::empty())
                .expect("http long-poll timeout request"),
        )
        .await
        .expect("http long-poll timeout response");
    assert_eq!(timeout.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        timeout
            .headers()
            .get("stream-next-offset")
            .expect("timeout next offset"),
        "00000000000000000000"
    );
    assert_eq!(
        timeout
            .headers()
            .get("stream-up-to-date")
            .expect("timeout up-to-date"),
        "true"
    );
    assert_eq!(
        timeout
            .headers()
            .get("stream-cursor")
            .expect("timeout cursor"),
        "00000000000000000000"
    );

    let first_uri = format!("{path}?offset=now&live=long-poll&timeout_ms=1000");
    let first_app = app.clone();
    let first_uri_for_task = first_uri.clone();
    let first = madsim::task::spawn(async move {
        first_app
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(&first_uri_for_task)
                    .body(Body::empty())
                    .expect("first backpressure long-poll request"),
            )
            .await
            .expect("first backpressure long-poll response")
    });
    madsim::time::sleep(Duration::from_millis(10)).await;

    let second = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&first_uri)
                .body(Body::empty())
                .expect("second backpressure long-poll request"),
        )
        .await
        .expect("second backpressure long-poll response");
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    let second_body = to_bytes(second.into_body(), usize::MAX)
        .await
        .expect("second backpressure response body");
    assert!(
        std::str::from_utf8(&second_body)
            .expect("utf8 second backpressure response")
            .contains("live read waiters")
    );

    let release = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .body(Body::from("open"))
                .expect("release long-poll append request"),
        )
        .await
        .expect("release long-poll append response");
    assert_eq!(release.status(), StatusCode::NO_CONTENT);

    let first = first.await.expect("first long-poll task");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first
            .headers()
            .get("stream-next-offset")
            .expect("first next offset"),
        "00000000000000000004"
    );
    let first_body = to_bytes(first.into_body(), usize::MAX)
        .await
        .expect("first long-poll body");
    assert_eq!(&first_body[..], b"open");

    let metrics = app
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("http metrics request"),
        )
        .await
        .expect("http metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_body = to_bytes(metrics.into_body(), usize::MAX)
        .await
        .expect("http metrics body");
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
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn hosted runtime for http producer protocol surface");
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(runtime).with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::clone(&now_ms),
    }));
    let app = router_with_http_state(state);
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&path)
                .header("content-type", "text/plain")
                .body(Body::empty())
                .expect("http producer create request"),
        )
        .await
        .expect("http producer create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let mut concurrent = Vec::new();
    for (producer_id, payload) in [("writer-a", "aa"), ("writer-b", "bb")] {
        let app = app.clone();
        let path = path.clone();
        concurrent.push(madsim::task::spawn(async move {
            let response = app
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(&path)
                        .header("content-type", "text/plain")
                        .header("producer-id", producer_id)
                        .header("producer-epoch", "0")
                        .header("producer-seq", "0")
                        .body(Body::from(payload))
                        .expect("http concurrent producer request"),
                )
                .await
                .expect("http concurrent producer response");
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
        assert_eq!(
            response
                .headers()
                .get("producer-epoch")
                .expect("producer epoch ack"),
            "0"
        );
        assert_eq!(
            response
                .headers()
                .get("producer-seq")
                .expect("producer seq ack"),
            "0"
        );
    }

    let duplicate = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "writer-a")
                .header("producer-epoch", "0")
                .header("producer-seq", "0")
                .body(Body::from("ignored"))
                .expect("http duplicate producer request"),
        )
        .await
        .expect("http duplicate producer response");
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
    assert_eq!(
        duplicate
            .headers()
            .get("producer-epoch")
            .expect("duplicate producer epoch"),
        "0"
    );
    assert_eq!(
        duplicate
            .headers()
            .get("producer-seq")
            .expect("duplicate producer seq"),
        "0"
    );

    let gap = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "writer-a")
                .header("producer-epoch", "0")
                .header("producer-seq", "2")
                .body(Body::from("gap"))
                .expect("http producer gap request"),
        )
        .await
        .expect("http producer gap response");
    assert_eq!(gap.status(), StatusCode::CONFLICT);
    assert_eq!(
        gap.headers()
            .get("producer-expected-seq")
            .expect("gap expected seq"),
        "1"
    );
    assert_eq!(
        gap.headers()
            .get("producer-received-seq")
            .expect("gap received seq"),
        "2"
    );

    let epoch_bump = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "writer-a")
                .header("producer-epoch", "1")
                .header("producer-seq", "0")
                .body(Body::from("cc"))
                .expect("http producer epoch bump request"),
        )
        .await
        .expect("http producer epoch bump response");
    assert_eq!(epoch_bump.status(), StatusCode::OK);
    assert_eq!(
        epoch_bump
            .headers()
            .get("producer-epoch")
            .expect("epoch bump producer epoch"),
        "1"
    );
    assert_eq!(
        epoch_bump
            .headers()
            .get("producer-seq")
            .expect("epoch bump producer seq"),
        "0"
    );

    let stale = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "text/plain")
                .header("producer-id", "writer-a")
                .header("producer-epoch", "0")
                .header("producer-seq", "1")
                .body(Body::from("stale"))
                .expect("http producer stale epoch request"),
        )
        .await
        .expect("http producer stale epoch response");
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        stale
            .headers()
            .get("producer-epoch")
            .expect("stale current epoch"),
        "1"
    );

    let read_uri = format!("{path}?offset=0&max_bytes=16");
    let read = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(&read_uri)
                .body(Body::empty())
                .expect("http producer read request"),
        )
        .await
        .expect("http producer read response");
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        read.headers()
            .get("stream-next-offset")
            .expect("producer read next offset"),
        "00000000000000000006"
    );
    let body = to_bytes(read.into_body(), usize::MAX)
        .await
        .expect("http producer read body");
    assert!(
        &body[..] == b"aabbcc" || &body[..] == b"bbaacc",
        "unexpected HTTP producer payload order: {:?}",
        body
    );

    let record_path = format!("{path}-records");
    let create_records = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&record_path)
                .header("content-type", "application/json")
                .body(Body::empty())
                .expect("record stream create request"),
        )
        .await
        .expect("record stream create response");
    assert_eq!(create_records.status(), StatusCode::CREATED);
    assert_eq!(
        create_records
            .headers()
            .get("stream-extensions")
            .expect("record extension"),
        "json-record-coordinates-v1"
    );

    let append_records = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&record_path)
                .header("content-type", "application/json")
                .header("producer-id", "record-writer")
                .header("producer-epoch", "0")
                .header("producer-seq", "0")
                .body(Body::from(
                    r#"[{"captured_at_ms":120},{"captured_at_ms":100}]"#,
                ))
                .expect("record append request"),
        )
        .await
        .expect("record append response");
    assert_eq!(append_records.status(), StatusCode::OK);
    assert_eq!(
        append_records
            .headers()
            .get("stream-record-start")
            .expect("record append start"),
        "0"
    );
    assert_eq!(
        append_records
            .headers()
            .get("stream-record-next")
            .expect("record append next"),
        "2"
    );

    let duplicate_records = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(&record_path)
                .header("content-type", "application/json")
                .header("producer-id", "record-writer")
                .header("producer-epoch", "0")
                .header("producer-seq", "0")
                .body(Body::from(r#"{"ignored":true}"#))
                .expect("record duplicate request"),
        )
        .await
        .expect("record duplicate response");
    assert_eq!(duplicate_records.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        duplicate_records
            .headers()
            .get("stream-record-next")
            .expect("record duplicate next"),
        "2"
    );

    let record_read = app
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!(
                    "{record_path}?record=1&max_records=1&record_view=envelope"
                ))
                .body(Body::empty())
                .expect("record read request"),
        )
        .await
        .expect("record read response");
    assert_eq!(record_read.status(), StatusCode::OK);
    assert_eq!(
        record_read
            .headers()
            .get("stream-record-next")
            .expect("record read next"),
        "2"
    );
    let record_body = to_bytes(record_read.into_body(), usize::MAX)
        .await
        .expect("record read body");
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
