use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use tempfile::TempDir;
use ursula_event_index::EventEntry;
use ursula_event_index::EventIndexConfig;
use ursula_event_index::LocalEventIndex as EventIndex;

const RECORDS: u64 = 100_000;

fn event_time_query(criterion: &mut Criterion) {
    let directory = TempDir::new().expect("create benchmark directory");
    let mut index = EventIndex::open(directory.path(), EventIndexConfig {
        source_id: "benchmark".to_owned(),
        flush_entries: 10_000,
        row_group_entries: 2_000,
        timestamp_field: "captured_at".to_owned(),
    })
    .expect("open benchmark index");
    for record in 0..RECORDS {
        let captured_at_ms = i64::try_from(record.wrapping_mul(7_919) % RECORDS)
            .expect("benchmark timestamp fits i64");
        index
            .ingest(EventEntry {
                captured_at_ms,
                record,
            })
            .expect("ingest benchmark event");
    }

    let mut group = criterion.benchmark_group("event_time_query");
    group.throughput(Throughput::Elements(100));
    group.bench_with_input(
        BenchmarkId::new("100_record_window", RECORDS),
        &index,
        |bencher, index| {
            bencher.iter(|| {
                black_box(
                    index
                        .query(50_000, 50_100, None, None, 1_000)
                        .expect("query benchmark index"),
                );
            });
        },
    );
    group.finish();
}

criterion_group!(benches, event_time_query);
criterion_main!(benches);
