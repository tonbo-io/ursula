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
use crate::DeleteStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::TidyStreamsRequest;
use crate::cold_store::DEFAULT_CONTENT_TYPE;

/// F3: a producer idle this long expires at the next tidy.
const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// The default `raft.deleted_data_max_age`.
const DELETED_DATA_MAX_AGE: std::time::Duration =
    ursula_config::RaftConfig::DEFAULT_DELETED_DATA_MAX_AGE;

/// A tidy pass with the scheduled sweep's budget.
fn tidy(max_streams: usize, max_expiry_sweeps: usize, now_ms: u64) -> TidyStreamsRequest {
    TidyStreamsRequest {
        max_streams,
        max_expiry_sweeps,
        expiry_sweep_budget: crate::expiry_sweep::GROUP_BUDGET,
        deleted_data_max_age: DELETED_DATA_MAX_AGE,
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

/// #669: the leader's sweep removes expired streams a group never writes
/// to again with `Sweep` entries of 256 streams, at most its
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

/// The usage committed per bucket over every group, by bucket.
async fn bucket_committed(runtime: &ShardRuntime) -> Vec<(String, u64, u64)> {
    let mut buckets = std::collections::BTreeMap::<String, (u64, u64)>::new();
    for (_, usage) in runtime
        .bucket_usage_all_groups()
        .await
        .expect("bucket usage")
    {
        for row in usage.buckets {
            let entry = buckets.entry(row.bucket_id).or_default();
            entry.0 = entry.0.saturating_add(row.usage.committed_write_units);
            entry.1 = entry.1.saturating_add(row.usage.stream_count);
        }
    }
    buckets
        .into_iter()
        .map(|(bucket, (units, streams))| (bucket, units, streams))
        .collect()
}

/// F20: a deleted stream's end record stays until the sweep folds it into
/// its bucket's usage, once its retention passed. A group's first pass
/// seeds its retirement clock, which dates the end records the group held
/// then by their removal times: one removed half a retention before the
/// seed folds half a retention later by the pass's wall clock, and one
/// removed at least the retention before folds at once. An end record
/// removed after the seed is due only once this replica's samples are the
/// retention old, whatever the wall clock says. The bucket's totals never
/// move.
#[tokio::test]
async fn the_sweep_folds_end_records_past_their_retention() {
    // A retention of a minute.
    let bound = std::time::Duration::from_secs(120);
    let runtime = spawn();
    let delete = |index: usize, now_ms: u64| {
        let runtime = &runtime;
        async move {
            runtime
                .delete_stream(DeleteStreamRequest {
                    stream_id: BucketStreamId::new("retire", format!("s-{index:02}")),
                    now_ms,
                    if_incarnation: None,
                })
                .await
                .expect("delete");
        }
    };
    for index in 0..50 {
        let stream_id = BucketStreamId::new("retire", format!("s-{index:02}"));
        let mut request = CreateStreamRequest::new(stream_id.clone(), DEFAULT_CONTENT_TYPE);
        request.now_ms = 1;
        runtime.create_stream(request).await.expect("create");
    }
    // Twenty a whole retention before the first pass, twenty half a
    // retention before it.
    for index in 0..40 {
        delete(index, if index < 20 { 2 } else { 30_002 }).await;
    }
    let before = bucket_committed(&runtime).await;
    assert_eq!(before, vec![("retire".to_owned(), 50, 10)]);

    let pass = |now_ms: u64| {
        let runtime = &runtime;
        async move {
            let mut request = tidy(0, 16, now_ms);
            request.deleted_data_max_age = bound;
            runtime
                .tidy_streams_all_groups_once(request)
                .await
                .into_result()
                .expect("a sweep pass")
        }
    };
    let report = pass(60_002).await;
    assert_eq!(report.end_records_retired, 20, "{report:?}");
    assert_eq!(report.expiry_swept_streams, 0, "{report:?}");
    assert_eq!(report.expiry_due_groups, 0, "{report:?}");
    assert_eq!(bucket_committed(&runtime).await, before);

    // Ten removed after the seed, dated long before the pass's clock.
    for index in 40..50 {
        delete(index, 0).await;
    }
    // Half a retention before the next pass's clock is not yet a whole one.
    let report = pass(90_001).await;
    assert_eq!(report.end_records_retired, 0, "{report:?}");
    assert_eq!(report.expiry_sweeps, 0, "{report:?}");
    // Half a retention after the seed, the twenty held at the seed are
    // due, and the ten removed after it are not.
    let report = pass(90_002).await;
    assert_eq!(report.end_records_retired, 20, "{report:?}");
    let report = pass(10_000_000).await;
    assert_eq!(report.end_records_retired, 0, "{report:?}");
    assert_eq!(report.expiry_sweeps, 0, "{report:?}");
    assert_eq!(bucket_committed(&runtime).await, vec![(
        "retire".to_owned(),
        50,
        0
    )]);
    // `/__ursula/metrics` counts the folds this node's replicas applied.
    assert_eq!(runtime.metrics().snapshot().usage_end_records_retired, 40);
}
