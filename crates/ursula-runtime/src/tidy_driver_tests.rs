//! The bounded-state F0 tidy driver through the runtime on the in-memory
//! engine: rate-limited passes over every group converge streams with
//! normalization debt. The debt here is idle-producer expiry (F3). (The F3
//! receipt window is pinned by the state machine's producer-window tests and
//! the HTTP 204 regression.)

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
        .tidy_streams_all_groups_once(64, 10)
        .await
        .expect("tidy pass before the expiry");
    assert_eq!(report.tidied, 0);

    let expired = 1 + PRODUCER_IDLE_EXPIRY_MS;
    // At most one stream per group per pass, so the pass is rate-limited.
    let report = runtime
        .tidy_streams_all_groups_once(1, expired)
        .await
        .expect("bounded tidy pass");
    assert!(report.tidied >= 1 && report.tidied <= 2, "{report:?}");
    let mut passes = 1;
    loop {
        let report = runtime
            .tidy_streams_all_groups_once(1, expired)
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
