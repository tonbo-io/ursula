use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::StreamCommand;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;

const CONTENT_TYPE: &str = "application/octet-stream";
const STREAMS: usize = 128;
const CHUNKS_PER_STREAM: usize = 64;
const CHUNK_BYTES: usize = 1024;

fn hot_snapshot_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_snapshot");

    for scenario in [
        SnapshotScenario::HotOnly,
        SnapshotScenario::HalfCold,
        SnapshotScenario::ManyStreams,
    ] {
        let machine = build_machine(scenario);
        group.throughput(Throughput::Bytes(
            u64::try_from(scenario.hot_bytes()).expect("hot bytes fit u64"),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name()),
            &scenario,
            |b, _| {
                b.iter_batched(
                    || machine.clone(),
                    |machine| black_box(machine.snapshot()),
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

#[derive(Debug, Clone, Copy)]
enum SnapshotScenario {
    HotOnly,
    HalfCold,
    ManyStreams,
}

impl SnapshotScenario {
    fn name(self) -> &'static str {
        match self {
            Self::HotOnly => "one_stream_hot_chunks",
            Self::HalfCold => "one_stream_half_cold",
            Self::ManyStreams => "many_streams_hot_chunks",
        }
    }

    fn stream_count(self) -> usize {
        match self {
            Self::ManyStreams => STREAMS,
            Self::HotOnly | Self::HalfCold => 1,
        }
    }

    fn hot_chunks_per_stream(self) -> usize {
        match self {
            Self::HalfCold => CHUNKS_PER_STREAM / 2,
            Self::HotOnly | Self::ManyStreams => CHUNKS_PER_STREAM,
        }
    }

    fn hot_bytes(self) -> usize {
        self.stream_count() * self.hot_chunks_per_stream() * CHUNK_BYTES
    }
}

fn build_machine(scenario: SnapshotScenario) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));

    for stream_index in 0..scenario.stream_count() {
        let stream_id = BucketStreamId::new("benchcmp", format!("snapshot-{stream_index}"));
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: CONTENT_TYPE.to_owned(),
                initial_payload: bytes::Bytes::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                attrs: None,
                now_ms: 0,
            }),
            StreamResponse::Created { .. }
        ));

        let payload =
            vec![u8::try_from(stream_index % 251).expect("payload byte fits u8"); CHUNK_BYTES];
        for _ in 0..CHUNKS_PER_STREAM {
            assert!(matches!(
                machine.apply(StreamCommand::Append {
                    stream_id: stream_id.clone(),
                    content_type: Some(CONTENT_TYPE.to_owned()),
                    payload: bytes::Bytes::from(payload.clone()),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                    record_match: None,
                }),
                StreamResponse::Appended { .. }
            ));
        }

        if matches!(scenario, SnapshotScenario::HalfCold) {
            let cold_bytes =
                u64::try_from((CHUNKS_PER_STREAM / 2) * CHUNK_BYTES).expect("cold bytes fit u64");
            assert!(matches!(
                machine.apply(StreamCommand::FlushCold {
                    stream_id: stream_id.clone(),
                    chunk: ColdChunkRef {
                        start_offset: 0,
                        end_offset: cold_bytes,
                        s3_path: format!("{stream_id}/chunks/000000.bin"),
                        object_size: cold_bytes,
                    },
                }),
                StreamResponse::ColdFlushed { .. }
            ));
        }
    }

    machine
}

criterion_group!(benches, hot_snapshot_benches);
criterion_main!(benches);
