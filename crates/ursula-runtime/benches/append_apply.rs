use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use ursula_shard::BucketStreamId;
use ursula_stream::ProducerRequest;
use ursula_stream::StreamCommand;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;

const CONTENT_TYPE: &str = "application/octet-stream";
const APPENDS_PER_ITER: usize = 1024;
const PAYLOAD_BYTES: usize = 256;
const STREAM_COUNT: usize = 1024;
const RECORD_COUNT: usize = 100_000;
const JSON_RECORD: &[u8] = b"{\"value\":1}\n";

fn append_apply_benches(c: &mut Criterion) {
    let payload = vec![7; PAYLOAD_BYTES];
    let mut group = c.benchmark_group("append_apply");
    group.throughput(Throughput::Elements(
        u64::try_from(APPENDS_PER_ITER).expect("append count fits u64"),
    ));

    for scenario in [
        AppendScenario::SingleStream,
        AppendScenario::ManyStreams,
        AppendScenario::AppendBatch,
        AppendScenario::ProducerDedup,
        AppendScenario::SnapshotCompaction,
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name()),
            &scenario,
            |b, _| {
                b.iter_batched(
                    || setup_machine(&scenario),
                    |mut machine| {
                        let result = run_appends(&mut machine, &scenario, &payload);
                        black_box(result);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn record_coordinate_benches(c: &mut Criterion) {
    let (machine, stream_id) = setup_record_machine(RECORD_COUNT);
    let mut group = c.benchmark_group("record_coordinates");

    group.bench_function("seek_100k_records", |b| {
        b.iter(|| {
            black_box(
                machine
                    .offset_for_record(&stream_id, black_box(75_000))
                    .expect("record seek"),
            )
        });
    });

    group.bench_function("aligned_read_100_records", |b| {
        b.iter(|| {
            let start = machine
                .offset_for_record(&stream_id, black_box(50_000))
                .expect("record start")
                .expect("record index active");
            let end = machine
                .offset_for_record(&stream_id, 50_100)
                .expect("record end")
                .expect("record index active");
            black_box(
                machine
                    .read_plan(
                        &stream_id,
                        start,
                        usize::try_from(end - start).expect("read length fits usize"),
                    )
                    .expect("record-aligned read"),
            );
        });
    });

    group.throughput(Throughput::Elements(
        u64::try_from(APPENDS_PER_ITER).expect("append count fits u64"),
    ));
    group.bench_function("append_json_records", |b| {
        b.iter_batched(
            || setup_record_machine(0),
            |(mut machine, stream_id)| {
                for _ in 0..APPENDS_PER_ITER {
                    let response = machine.apply(StreamCommand::Append {
                        stream_id: stream_id.clone(),
                        content_type: Some("application/json".to_owned()),
                        payload: JSON_RECORD.to_vec(),
                        close_after: false,
                        stream_seq: None,
                        producer: None,
                        now_ms: 0,
                        record_match: None,
                    });
                    assert!(matches!(response, StreamResponse::Appended { .. }));
                }
                black_box(machine);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn setup_record_machine(record_count: usize) -> (StreamStateMachine, BucketStreamId) {
    let stream_id = BucketStreamId::new("benchcmp", "record-coordinates");
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/json".to_owned(),
            initial_payload: JSON_RECORD.repeat(record_count),
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
    (machine, stream_id)
}

#[derive(Debug, Clone, Copy)]
enum AppendScenario {
    SingleStream,
    ManyStreams,
    AppendBatch,
    ProducerDedup,
    SnapshotCompaction,
}

impl AppendScenario {
    fn name(self) -> &'static str {
        match self {
            Self::SingleStream => "single_stream_append",
            Self::ManyStreams => "many_streams_append",
            Self::AppendBatch => "single_stream_append_batch_16",
            Self::ProducerDedup => "producer_dedup_retry",
            Self::SnapshotCompaction => "snapshot_compaction_setsums",
        }
    }
}

fn setup_machine(scenario: &AppendScenario) -> StreamStateMachine {
    let stream_count = match scenario {
        AppendScenario::ManyStreams => STREAM_COUNT,
        AppendScenario::SingleStream
        | AppendScenario::AppendBatch
        | AppendScenario::ProducerDedup
        | AppendScenario::SnapshotCompaction => 1,
    };
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    for stream_index in 0..stream_count {
        let stream_id = stream_id(stream_index);
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id,
                content_type: CONTENT_TYPE.to_owned(),
                initial_payload: Vec::new(),
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
    }
    machine
}

fn run_appends(machine: &mut StreamStateMachine, scenario: &AppendScenario, payload: &[u8]) -> u64 {
    match scenario {
        AppendScenario::SingleStream => append_single_stream(machine, payload),
        AppendScenario::ManyStreams => append_many_streams(machine, payload),
        AppendScenario::AppendBatch => append_batch(machine, payload),
        AppendScenario::ProducerDedup => producer_dedup(machine, payload),
        AppendScenario::SnapshotCompaction => snapshot_compaction(machine, payload),
    }
}

fn append_single_stream(machine: &mut StreamStateMachine, payload: &[u8]) -> u64 {
    let stream_id = stream_id(0);
    let mut tail = 0u64;
    for _ in 0..APPENDS_PER_ITER {
        tail = append(machine, stream_id.clone(), payload.to_vec(), None);
    }
    tail
}

fn append_many_streams(machine: &mut StreamStateMachine, payload: &[u8]) -> u64 {
    let mut tail = 0u64;
    for index in 0..APPENDS_PER_ITER {
        tail = append(
            machine,
            stream_id(index % STREAM_COUNT),
            payload.to_vec(),
            None,
        );
    }
    tail
}

fn append_batch(machine: &mut StreamStateMachine, payload: &[u8]) -> u64 {
    let stream_id = stream_id(0);
    let payloads = vec![payload.to_vec(); 16];
    let mut tail = 0u64;
    for _ in 0..(APPENDS_PER_ITER / payloads.len()) {
        tail = match machine.apply(StreamCommand::AppendBatch {
            stream_id: stream_id.clone(),
            content_type: Some(CONTENT_TYPE.to_owned()),
            payloads: payloads.clone(),
            producer: None,
            now_ms: 0,
        }) {
            StreamResponse::Appended { next_offset, .. } => next_offset,
            response => panic!("append batch failed: {response:?}"),
        };
    }
    tail
}

fn producer_dedup(machine: &mut StreamStateMachine, payload: &[u8]) -> u64 {
    let stream_id = stream_id(0);
    let producer = ProducerRequest {
        producer_id: "producer-0".to_owned(),
        producer_epoch: 1,
        producer_seq: 0,
    };
    let first_tail = append(
        machine,
        stream_id.clone(),
        payload.to_vec(),
        Some(producer.clone()),
    );
    let mut tail = first_tail;
    for _ in 0..APPENDS_PER_ITER {
        tail = append(
            machine,
            stream_id.clone(),
            payload.to_vec(),
            Some(producer.clone()),
        );
    }
    tail
}

fn snapshot_compaction(machine: &mut StreamStateMachine, payload: &[u8]) -> u64 {
    let stream_id = stream_id(0);
    for _ in 0..APPENDS_PER_ITER {
        append(machine, stream_id.clone(), payload.to_vec(), None);
    }
    let snapshot_offset =
        u64::try_from(APPENDS_PER_ITER / 2 * payload.len()).expect("snapshot offset fits u64");
    match machine.apply(StreamCommand::PublishSnapshot {
        stream_id,
        snapshot_offset,
        content_type: "application/json".to_owned(),
        payload: b"{}".to_vec(),
        now_ms: 0,
    }) {
        StreamResponse::SnapshotPublished { snapshot_offset } => snapshot_offset,
        response => panic!("publish snapshot failed: {response:?}"),
    }
}

fn append(
    machine: &mut StreamStateMachine,
    stream_id: BucketStreamId,
    payload: Vec<u8>,
    producer: Option<ProducerRequest>,
) -> u64 {
    match machine.apply(StreamCommand::Append {
        stream_id,
        content_type: Some(CONTENT_TYPE.to_owned()),
        payload,
        close_after: false,
        stream_seq: None,
        producer,
        now_ms: 0,
        record_match: None,
    }) {
        StreamResponse::Appended { next_offset, .. } => next_offset,
        response => panic!("append failed: {response:?}"),
    }
}

fn stream_id(index: usize) -> BucketStreamId {
    BucketStreamId::new("benchcmp", format!("append-{index}"))
}

criterion_group!(benches, append_apply_benches, record_coordinate_benches);
criterion_main!(benches);
