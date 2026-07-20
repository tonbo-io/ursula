use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use tempfile::TempDir;
use ursula_index::EventEntry;
use ursula_index::EventIndex;
use ursula_index::EventIndexCache;
use ursula_index::EventIndexConfig;
use ursula_index::FsObjectStore;

const RECORDS: u64 = 100_000;
const COMPACTION_PARTS: usize = 8;
const COMPACTION_PART_ENTRIES: usize = 10_000;

fn event_time_query(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("create benchmark runtime");
    let objects = TempDir::new().expect("create object directory");
    let cache = TempDir::new().expect("create cache directory");
    let mut index = runtime
        .block_on(async {
            let mut index = EventIndex::open(
                FsObjectStore::new(objects.path())?,
                EventIndexCache::serving(cache.path(), 64 * 1024 * 1024)?,
                EventIndexConfig {
                    source_id: "benchmark".to_owned(),
                    flush_entries: 10_000,
                    row_group_entries: 2_000,
                    timestamp_field: "captured_at".to_owned(),
                },
            )
            .await?;
            for record in 0..RECORDS {
                let captured_at_ms = i64::try_from(record.wrapping_mul(7_919) % RECORDS)?;
                index
                    .ingest(EventEntry {
                        captured_at_ms,
                        record,
                    })
                    .await?;
            }
            Ok::<_, anyhow::Error>(index)
        })
        .expect("build query benchmark index");

    let mut group = criterion.benchmark_group("event_time_query");
    group.throughput(Throughput::Elements(100));
    group.bench_function(BenchmarkId::new("100_record_window", RECORDS), |bencher| {
        bencher.iter(|| {
            black_box(
                runtime
                    .block_on(index.query(50_000, 50_100, None, None, 1_000))
                    .expect("query benchmark index"),
            );
        });
    });
    group.finish();
}

fn bounded_partition_compaction(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("create benchmark runtime");
    let mut group = criterion.benchmark_group("event_time_compaction");
    group.throughput(Throughput::Elements(
        u64::try_from(COMPACTION_PARTS * COMPACTION_PART_ENTRIES)
            .expect("compaction benchmark size fits u64"),
    ));
    group.bench_function("one_day_8x10k_l0_parts", |bencher| {
        bencher.iter_batched(
            || {
                let objects = TempDir::new().expect("create object directory");
                let cache = TempDir::new().expect("create cache directory");
                let index = runtime
                    .block_on(async {
                        let mut index = EventIndex::open(
                            FsObjectStore::new(objects.path())?,
                            EventIndexCache::serving(cache.path(), 64 * 1024 * 1024)?,
                            EventIndexConfig {
                                source_id: "compaction-benchmark".to_owned(),
                                flush_entries: COMPACTION_PART_ENTRIES,
                                row_group_entries: 2_000,
                                timestamp_field: "captured_at".to_owned(),
                            },
                        )
                        .await?;
                        let record_count = COMPACTION_PARTS * COMPACTION_PART_ENTRIES;
                        for record in 0..record_count {
                            index
                                .ingest(EventEntry {
                                    captured_at_ms: i64::try_from(record)?,
                                    record: u64::try_from(record)?,
                                })
                                .await?;
                        }
                        Ok::<_, anyhow::Error>(index)
                    })
                    .expect("build compaction benchmark index");
                (objects, cache, index)
            },
            |(_objects, _cache, mut index)| {
                black_box(
                    runtime
                        .block_on(
                            index.compact_partition_once(
                                COMPACTION_PARTS,
                                u64::try_from(COMPACTION_PARTS * COMPACTION_PART_ENTRIES)
                                    .expect("compaction benchmark size fits u64"),
                            ),
                        )
                        .expect("compact benchmark partition"),
                );
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, event_time_query, bounded_partition_compaction);
criterion_main!(benches);
