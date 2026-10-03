//! The bounded-state F0 tidy driver through the runtime on the in-memory
//! engine: rate-limited passes over every group converge streams written
//! before the feature-level raise. (The F3 receipt window is pinned by the
//! state machine's producer-window tests and the HTTP 204 regression.)

use ursula_shard::BucketStreamId;
use ursula_stream::FEATURE_LEVEL_KEYED_STREAMS;
use ursula_stream::ProducerRequest;

use crate::AppendRequest;
use crate::CreateStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::cold_store::DEFAULT_CONTENT_TYPE;

fn spawn() -> ShardRuntime {
    ShardRuntime::spawn(RuntimeConfig::new(1, 2)).expect("spawn runtime")
}

async fn raise(runtime: &ShardRuntime) {
    for (group, result) in runtime
        .set_feature_level_all_groups(FEATURE_LEVEL_KEYED_STREAMS)
        .await
    {
        result.unwrap_or_else(|err| panic!("raise group {group:?}: {err}"));
    }
}

fn producer_append(stream: &BucketStreamId, seq: u64) -> AppendRequest {
    let mut request = AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec());
    request.producer = Some(ProducerRequest {
        producer_id: "writer".to_owned(),
        producer_epoch: 1,
        producer_seq: seq,
    });
    request.now_ms = 1;
    request
}

#[tokio::test]
async fn tidy_pass_converges_streams_written_before_the_raise() {
    let runtime = spawn();
    let streams = (0..3)
        .map(|index| BucketStreamId::new("window", format!("legacy-{index}")))
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
    // Below level 1 nothing has debt and nothing is proposed.
    let report = runtime
        .tidy_streams_all_groups_once(64, 10)
        .await
        .expect("tidy pass at level 0");
    assert_eq!(report.tidied, 0);

    raise(&runtime).await;
    // At most one stream per group per pass, so the pass is rate-limited.
    let report = runtime
        .tidy_streams_all_groups_once(1, 10)
        .await
        .expect("bounded tidy pass");
    assert!(report.tidied >= 1 && report.tidied <= 2, "{report:?}");
    let mut passes = 1;
    loop {
        let report = runtime
            .tidy_streams_all_groups_once(1, 10)
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
        assert_eq!(gauges.feature_level, FEATURE_LEVEL_KEYED_STREAMS);
    }
}
