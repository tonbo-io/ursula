//! The bounded-state F0 tidy driver through the runtime on the in-memory
//! engine: rate-limited passes over every group converge streams with
//! normalization debt. The debt here is idle-producer expiry (F3). (One
//! receipt per producer is pinned by the state machine's producer-record
//! tests and the HTTP `receipt-evicted` regression.)

use ursula_shard::BucketStreamId;
use ursula_stream::ProducerRequest;

use crate::AppendRequest;
use crate::CreateStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::cold_store::DEFAULT_CONTENT_TYPE;

/// F3: a producer idle this long expires at the next tidy.
const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

fn spawn() -> ShardRuntime {
    ShardRuntime::spawn(RuntimeConfig::new(1, 2)).expect("spawn runtime")
}

fn producer_append(stream: &BucketStreamId, seq: u64) -> AppendRequest {
    let mut request = AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec());
    request.producer = Some(ProducerRequest {
        producer_id: "writer".to_owned(),
        producer_epoch: 1,
        producer_seq: seq,
        digest: None,
        ..Default::default()
    });
    request.now_ms = 1;
    request
}

#[tokio::test]
async fn tidy_pass_converges_streams_with_idle_producers() {
    let runtime = spawn();
    let streams = (0..3)
        .map(|index| BucketStreamId::new("window", format!("idle-{index}")))
        .collect::<Vec<_>>();
    for stream in &streams {
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                DEFAULT_CONTENT_TYPE,
            ))
            .await
            .expect("create");
        for seq in 0..3 {
            runtime
                .append(producer_append(stream, seq))
                .await
                .expect("append");
        }
    }
    // Before the expiry nothing has debt and nothing is proposed.
    let report = runtime
        .tidy_streams_all_groups_once(64, 0, 10)
        .await
        .expect("tidy pass before the expiry");
    assert_eq!(report.tidied, 0);

    let expired = 1 + PRODUCER_IDLE_EXPIRY_MS;
    // At most one stream per group per pass, so the pass is rate-limited.
    let report = runtime
        .tidy_streams_all_groups_once(1, 0, expired)
        .await
        .expect("bounded tidy pass");
    assert!(report.tidied >= 1 && report.tidied <= 2, "{report:?}");
    let mut passes = 1;
    loop {
        let report = runtime
            .tidy_streams_all_groups_once(1, 0, expired)
            .await
            .expect("tidy pass");
        if report.tidied == 0 {
            break;
        }
        passes += 1;
        assert!(passes <= 4, "tidy did not converge");
    }
    let gauges = runtime.state_gauges_all_groups().await;
    for (group, gauges) in gauges {
        let gauges = gauges.unwrap_or_else(|err| panic!("gauges {group:?}: {err}"));
        assert_eq!(gauges.producers, 0, "group {group:?}");
    }
}

/// Streams total across the runtime's groups.
async fn stream_count(runtime: &ShardRuntime) -> u64 {
    runtime
        .state_gauges_all_groups()
        .await
        .into_iter()
        .map(|(group, gauges)| {
            gauges
                .unwrap_or_else(|err| panic!("gauges {group:?}: {err}"))
                .streams
        })
        .sum()
}

/// #669: the leader's expiry sweep removes expired streams a group never
/// writes to again, at most its command cap per group and pass, reports the
/// groups it left due, and sums its commands over the groups. Plain appends
/// to a live stream never sweep.
#[tokio::test]
async fn expiry_sweep_removes_expired_streams_within_its_cap() {
    let runtime = spawn();
    let expired_at = 5_000;
    for index in 0..1_200 {
        let mut request = CreateStreamRequest::new(
            BucketStreamId::new("expiry", format!("s-{index:04}")),
            DEFAULT_CONTENT_TYPE,
        );
        request.stream_expires_at_ms = Some(expired_at);
        request.now_ms = 1;
        runtime.create_stream(request).await.expect("create");
    }
    let live = BucketStreamId::new("expiry", "live");
    let mut request = CreateStreamRequest::new(live.clone(), DEFAULT_CONTENT_TYPE);
    request.now_ms = 1;
    runtime
        .create_stream(request)
        .await
        .expect("create the live stream");
    let mut append = AppendRequest::from_bytes(live, b"after".to_vec());
    append.now_ms = expired_at.saturating_add(10_000);
    runtime.append(append).await.expect("append");
    assert_eq!(stream_count(&runtime).await, 1_201, "appends never sweep");

    // Within the clock grace nothing is swept.
    let report = runtime
        .tidy_streams_all_groups_once(0, 16, expired_at.saturating_add(999))
        .await
        .expect("a pass within the grace");
    assert_eq!(report.expiry_sweeps, 0);
    assert_eq!(stream_count(&runtime).await, 1_201);

    // One command per group removes at most 257 streams in each.
    let now = expired_at.saturating_add(10_000);
    let report = runtime
        .tidy_streams_all_groups_once(0, 1, now)
        .await
        .expect("a capped pass");
    assert_eq!(report.expiry_sweeps, 2, "{report:?}");
    assert_eq!(report.expiry_due_groups, 2, "{report:?}");
    let left = stream_count(&runtime).await;
    assert!(
        (1_201 - 2 * 257..1_201).contains(&left),
        "{left} streams left"
    );

    let report = runtime
        .tidy_streams_all_groups_once(0, 16, now)
        .await
        .expect("a full pass");
    assert!(report.expiry_sweeps >= 2, "{report:?}");
    assert_eq!(report.expiry_due_groups, 0, "{report:?}");
    assert_eq!(stream_count(&runtime).await, 1);
    let snapshot = runtime.metrics().snapshot();
    assert_eq!(
        snapshot.ttl_expiry_sweeps,
        report.expiry_sweeps.saturating_add(2)
    );
    assert_eq!(snapshot.ttl_expiry_sweep_due_groups, 0);

    // Nothing left: the next pass proposes nothing.
    let report = runtime
        .tidy_streams_all_groups_once(0, 16, now)
        .await
        .expect("an idle pass");
    assert_eq!(report.expiry_sweeps, 0);
}
