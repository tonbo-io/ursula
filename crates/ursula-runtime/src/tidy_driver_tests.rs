//! The bounded-state F0 tidy driver through the runtime on the in-memory
//! engine: rate-limited passes over every group converge streams with
//! normalization debt. The debt here is idle-producer expiry (F3). (One
//! receipt per producer is pinned by the state machine's producer-record
//! tests and the HTTP `receipt-evicted` regression.)

use ursula_shard::BucketStreamId;
use ursula_stream::ProducerRequest;

use crate::AppendRequest;
use crate::CloseStreamRequest;
use crate::CreateStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::TidyStreamsRequest;
use crate::cold_store::DEFAULT_CONTENT_TYPE;

/// F3: a producer idle this long expires at the next tidy.
const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// A tidy pass with the scheduled sweep's budget.
fn tidy(max_streams: usize, max_expiry_sweeps: usize, now_ms: u64) -> TidyStreamsRequest {
    TidyStreamsRequest {
        max_streams,
        max_expiry_sweeps,
        expiry_sweep_budget: crate::expiry_sweep::GROUP_BUDGET,
        now_ms,
    }
}

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
        .tidy_streams_all_groups_once(tidy(64, 0, 10))
        .await
        .into_result()
        .expect("tidy pass before the expiry");
    assert_eq!(report.tidied, 0);

    let expired = 1 + PRODUCER_IDLE_EXPIRY_MS;
    // At most one stream per group per pass, so the pass is rate-limited.
    let report = runtime
        .tidy_streams_all_groups_once(tidy(1, 0, expired))
        .await
        .into_result()
        .expect("bounded tidy pass");
    assert!(report.tidied >= 1 && report.tidied <= 2, "{report:?}");
    let mut passes = 1;
    loop {
        let report = runtime
            .tidy_streams_all_groups_once(tidy(1, 0, expired))
            .await
            .into_result()
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
/// writes to again with `ExpireStreams` entries of 256 streams, at most its
/// entry cap per group and pass, reports the groups it left capped, and
/// sums its entries and removals over the groups. Writes never remove an
/// expired stream they do not name: creates, appends and closes leave them
/// for the sweep.
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
    let later = expired_at.saturating_add(10_000);
    let mut append = AppendRequest::from_bytes(live.clone(), b"after".to_vec());
    append.now_ms = later;
    runtime.append(append).await.expect("append");
    let mut request = CreateStreamRequest::new(
        BucketStreamId::new("expiry", "created-later"),
        DEFAULT_CONTENT_TYPE,
    );
    request.now_ms = later;
    runtime.create_stream(request).await.expect("create later");
    runtime
        .close_stream(CloseStreamRequest {
            stream_id: live,
            stream_seq: None,
            producer: None,
            now_ms: later,
            if_incarnation: None,
        })
        .await
        .expect("close");
    assert_eq!(stream_count(&runtime).await, 1_202, "writes never sweep");

    // Within the clock grace nothing is swept.
    let report = runtime
        .tidy_streams_all_groups_once(tidy(0, 16, expired_at.saturating_add(999)))
        .await
        .into_result()
        .expect("a pass within the grace");
    assert_eq!(report.expiry_sweeps, 0);
    assert_eq!(stream_count(&runtime).await, 1_202);

    // One entry per group removes 256 streams in each, and leaves both
    // groups capped.
    let now = later;
    let report = runtime
        .tidy_streams_all_groups_once(tidy(0, 1, now))
        .await
        .into_result()
        .expect("a capped pass");
    assert_eq!(report.expiry_sweeps, 2, "{report:?}");
    assert_eq!(report.expiry_swept_streams, 512, "{report:?}");
    assert_eq!(report.expiry_due_groups, 2, "{report:?}");
    assert_eq!(report.expiry_capped_groups, 2, "{report:?}");
    assert_eq!(stream_count(&runtime).await, 1_202 - 512);

    let report = runtime
        .tidy_streams_all_groups_once(tidy(0, 16, now))
        .await
        .into_result()
        .expect("a full pass");
    assert!(report.expiry_sweeps >= 2, "{report:?}");
    assert_eq!(report.expiry_swept_streams, 1_200 - 512, "{report:?}");
    assert_eq!(report.expiry_due_groups, 0, "{report:?}");
    assert_eq!(report.expiry_capped_groups, 0, "{report:?}");
    assert_eq!(stream_count(&runtime).await, 2);
    let snapshot = runtime.metrics().snapshot();
    assert_eq!(
        snapshot.ttl_expiry_sweeps,
        report.expiry_sweeps.saturating_add(2)
    );
    assert_eq!(snapshot.ttl_expiry_swept_streams, 1_200);
    assert_eq!(snapshot.ttl_expiry_sweep_due_groups, 0);

    // Nothing left: the next pass proposes nothing.
    let report = runtime
        .tidy_streams_all_groups_once(tidy(0, 16, now))
        .await
        .into_result()
        .expect("an idle pass");
    assert_eq!(report.expiry_sweeps, 0);
}
