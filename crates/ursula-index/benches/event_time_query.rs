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
use ursula_index::Extractor;
use ursula_index::FsObjectStore;
use ursula_index::IndexBase;
use ursula_index::QueryRequest;
use ursula_index::Segment;

const MESSAGES: u64 = 100_000;
const COMPACTION_PARTS: usize = 8;
const COMPACTION_PART_ENTRIES: usize = 10_000;
const MESSAGE_LEN: u64 = 64;

fn config(source: &str) -> EventIndexConfig {
    let mut config = EventIndexConfig::new(
        source,
        Extractor::timestamp_field("captured_at").expect("valid extractor"),
    );
    config.row_group_entries = 2_000;
    config
}

/// Commit `times` as consecutive messages from offset 0, `per_segment` at a
/// time, so each commit writes one part per event-time day.
async fn commit(index: &mut EventIndex, times: &[i64], per_segment: usize) -> anyhow::Result<()> {
    let mut offset = 0_u64;
    for chunk in times.chunks(per_segment) {
        let start = offset;
        let entries = chunk
            .iter()
            .map(|&t_ms| {
                let entry = EventEntry {
                    t_ms,
                    t_end_ms: t_ms,
                    offset,
                    len: MESSAGE_LEN,
                };
                offset = offset.saturating_add(MESSAGE_LEN);
                entry
            })
            .collect();
        index
            .commit_segment(Segment {
                start,
                end: offset,
                entries,
                skips: Vec::new(),
            })
            .await?;
    }
    Ok(())
}

fn event_time_query(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("create benchmark runtime");
    let objects = TempDir::new().expect("create object directory");
    let cache = TempDir::new().expect("create cache directory");
    let mut index = runtime
        .block_on(async {
            let mut index = EventIndex::open(
                FsObjectStore::new(objects.path())?,
                EventIndexCache::serving(cache.path(), 64 * 1024 * 1024)?,
                config("benchmark"),
                IndexBase::default(),
            )
            .await?;
            let times = (0..MESSAGES)
                .map(|message| i64::try_from(message.wrapping_mul(7_919) % MESSAGES))
                .collect::<Result<Vec<_>, _>>()?;
            commit(&mut index, &times, 10_000).await?;
            Ok::<_, anyhow::Error>(index)
        })
        .expect("build query benchmark index");

    let mut group = criterion.benchmark_group("event_time_query");
    group.throughput(Throughput::Elements(100));
    group.bench_function(BenchmarkId::new("100_entry_window", MESSAGES), |bencher| {
        bencher.iter(|| {
            black_box(
                runtime
                    .block_on(index.query(QueryRequest::window(50_000, 50_100, 1_000)))
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
                            config("compaction-benchmark"),
                            IndexBase::default(),
                        )
                        .await?;
                        let entry_count = COMPACTION_PARTS * COMPACTION_PART_ENTRIES;
                        let times = (0..entry_count)
                            .map(i64::try_from)
                            .collect::<Result<Vec<_>, _>>()?;
                        commit(&mut index, &times, COMPACTION_PART_ENTRIES).await?;
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
