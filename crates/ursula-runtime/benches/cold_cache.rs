use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use ursula_runtime::ColdCacheConfig;
use ursula_runtime::ColdStore;
use ursula_shard::BucketStreamId;
use ursula_stream::ObjectPayloadRef;

const OBJECT_BYTES: usize = 16 * 1024 * 1024;
const READ_BYTES: usize = 64 * 1024;
const CACHE_BYTES: usize = 64 * 1024 * 1024;
const CACHE_BLOCK_BYTES: usize = 1024 * 1024;
const CACHE_READAHEAD_BLOCKS: usize = 4;
const INTERLEAVED_STREAMS: usize = 4;
const RANDOM_OBJECT_BYTES: usize = 128 * 1024 * 1024;
const RANDOM_CACHE_BYTES: usize = 16 * 1024 * 1024;
const RANDOM_READS: usize = 512;

fn cold_cache_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    repeated_sequential_scan(c, &runtime);
    interleaved_sequential_scan(c, &runtime);
    random_working_set(c, &runtime);
    block_size_sweep(c, &runtime);
}

fn repeated_sequential_scan(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let stream_id = BucketStreamId::new("benchcmp", "cold-cache-sequential");
    let uncached = runtime.block_on(bench_store(false));
    let cached = runtime.block_on(bench_store(true));

    let mut group = c.benchmark_group("cold_cache_sequential_scan");
    group.throughput(Throughput::Bytes(
        u64::try_from(OBJECT_BYTES).expect("object size fits u64"),
    ));
    for (label, store, object) in [
        ("cache_off", &uncached.store, &uncached.object),
        ("cache_on", &cached.store, &cached.object),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(label), label, |b, _| {
            b.to_async(runtime).iter(|| async {
                black_box(scan_once(store, &stream_id, object).await);
            });
        });
    }
    group.finish();
}

fn interleaved_sequential_scan(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let uncached = runtime.block_on(bench_stores(false, INTERLEAVED_STREAMS, OBJECT_BYTES));
    let cached = runtime.block_on(bench_stores(true, INTERLEAVED_STREAMS, OBJECT_BYTES));

    let mut group = c.benchmark_group("cold_cache_interleaved_streams");
    group.throughput(Throughput::Bytes(
        u64::try_from(OBJECT_BYTES * INTERLEAVED_STREAMS).expect("object size fits u64"),
    ));
    for (label, stores) in [("cache_off", &uncached), ("cache_on", &cached)] {
        group.bench_with_input(BenchmarkId::from_parameter(label), label, |b, _| {
            b.to_async(runtime).iter(|| async {
                black_box(interleaved_scan_once(stores).await);
            });
        });
    }
    group.finish();
}

fn random_working_set(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let stream_id = BucketStreamId::new("benchcmp", "cold-cache-random");
    let uncached = runtime.block_on(bench_store_with_config(
        false,
        RANDOM_OBJECT_BYTES,
        RANDOM_CACHE_BYTES,
        CACHE_BLOCK_BYTES,
    ));
    let cached = runtime.block_on(bench_store_with_config(
        true,
        RANDOM_OBJECT_BYTES,
        RANDOM_CACHE_BYTES,
        CACHE_BLOCK_BYTES,
    ));
    let offsets = random_offsets(RANDOM_OBJECT_BYTES, READ_BYTES, RANDOM_READS);

    let mut group = c.benchmark_group("cold_cache_random_working_set");
    group.throughput(Throughput::Bytes(
        u64::try_from(READ_BYTES * RANDOM_READS).expect("read bytes fits u64"),
    ));
    for (label, store, object) in [
        ("cache_off", &uncached.store, &uncached.object),
        ("cache_on", &cached.store, &cached.object),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(label), label, |b, _| {
            b.to_async(runtime).iter(|| async {
                black_box(read_offsets(store, &stream_id, object, &offsets).await);
            });
        });
    }
    group.finish();
}

fn block_size_sweep(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let stream_id = BucketStreamId::new("benchcmp", "cold-cache-block-size");
    let stores = [256 * 1024, 1024 * 1024, 4 * 1024 * 1024]
        .into_iter()
        .map(|block_bytes| {
            (
                block_bytes,
                runtime.block_on(bench_store_with_config(
                    true,
                    OBJECT_BYTES,
                    CACHE_BYTES,
                    block_bytes,
                )),
            )
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("cold_cache_block_size");
    group.throughput(Throughput::Bytes(
        u64::try_from(OBJECT_BYTES).expect("object size fits u64"),
    ));
    for (block_bytes, bench_store) in &stores {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}KiB", block_bytes / 1024)),
            block_bytes,
            |b, _| {
                b.to_async(runtime).iter(|| async {
                    black_box(scan_once(&bench_store.store, &stream_id, &bench_store.object).await);
                });
            },
        );
    }
    group.finish();
}

struct BenchStore {
    store: ColdStore,
    object: ObjectPayloadRef,
}

async fn bench_store(cache_enabled: bool) -> BenchStore {
    bench_store_with_config(cache_enabled, OBJECT_BYTES, CACHE_BYTES, CACHE_BLOCK_BYTES).await
}

async fn bench_stores(
    cache_enabled: bool,
    stream_count: usize,
    object_bytes: usize,
) -> Vec<(BucketStreamId, BenchStore)> {
    let mut stores = Vec::new();
    for stream_index in 0..stream_count {
        stores.push((
            BucketStreamId::new("benchcmp", format!("cold-cache-{stream_index}")),
            bench_store_with_config(cache_enabled, object_bytes, CACHE_BYTES, CACHE_BLOCK_BYTES)
                .await,
        ));
    }
    stores
}

async fn bench_store_with_config(
    cache_enabled: bool,
    object_bytes: usize,
    cache_bytes: usize,
    cache_block_bytes: usize,
) -> BenchStore {
    let store = ColdStore::memory().expect("memory cold store");
    let store = if cache_enabled {
        store.with_read_cache(ColdCacheConfig {
            max_bytes: cache_bytes,
            block_bytes: cache_block_bytes,
            max_readahead_blocks: CACHE_READAHEAD_BLOCKS,
        })
    } else {
        store.without_read_cache()
    };

    let payload = (0..object_bytes)
        .map(|index| u8::try_from(index % 251).expect("pattern byte fits u8"))
        .collect::<Vec<_>>();
    let path = "benchcmp/cold-cache-bench/chunks/000000.bin";
    let object_size = store
        .write_chunk(path, &payload)
        .await
        .expect("write cold object");
    BenchStore {
        store,
        object: ObjectPayloadRef {
            start_offset: 0,
            end_offset: object_size,
            s3_path: path.to_owned(),
            object_size,
        },
    }
}

async fn scan_once(
    store: &ColdStore,
    stream_id: &BucketStreamId,
    object: &ObjectPayloadRef,
) -> u64 {
    let mut offset = 0u64;
    let mut checksum = 0u64;
    while offset < object.end_offset {
        let remaining = usize::try_from(object.end_offset - offset).expect("remaining fits usize");
        let len = remaining.min(READ_BYTES);
        let bytes = store
            .read_object_range_for_stream(stream_id, object, offset, len)
            .await
            .expect("read cold range");
        checksum = checksum.wrapping_add(bytes.iter().map(|byte| u64::from(*byte)).sum::<u64>());
        offset = offset.saturating_add(u64::try_from(len).expect("read len fits u64"));
    }
    checksum
}

async fn interleaved_scan_once(stores: &[(BucketStreamId, BenchStore)]) -> u64 {
    let mut checksum = 0u64;
    let mut offset = 0u64;
    let end_offset = stores
        .first()
        .map(|(_, store)| store.object.end_offset)
        .unwrap_or(0);
    while offset < end_offset {
        let remaining = usize::try_from(end_offset - offset).expect("remaining fits usize");
        let len = remaining.min(READ_BYTES);
        for (stream_id, bench_store) in stores {
            let bytes = bench_store
                .store
                .read_object_range_for_stream(stream_id, &bench_store.object, offset, len)
                .await
                .expect("read cold range");
            checksum =
                checksum.wrapping_add(bytes.iter().map(|byte| u64::from(*byte)).sum::<u64>());
        }
        offset = offset.saturating_add(u64::try_from(len).expect("read len fits u64"));
    }
    checksum
}

async fn read_offsets(
    store: &ColdStore,
    stream_id: &BucketStreamId,
    object: &ObjectPayloadRef,
    offsets: &[u64],
) -> u64 {
    let mut checksum = 0u64;
    for offset in offsets {
        let bytes = store
            .read_object_range_for_stream(stream_id, object, *offset, READ_BYTES)
            .await
            .expect("read cold range");
        checksum = checksum.wrapping_add(bytes.iter().map(|byte| u64::from(*byte)).sum::<u64>());
    }
    checksum
}

fn random_offsets(object_bytes: usize, read_bytes: usize, count: usize) -> Vec<u64> {
    let block_count = (object_bytes - read_bytes) / read_bytes;
    let mut state = 0x1234_5678_9abc_def0u64;
    (0..count)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let block_index =
                usize::try_from(state).expect("random state fits usize") % block_count;
            u64::try_from(block_index * read_bytes).expect("offset fits u64")
        })
        .collect()
}

criterion_group!(benches, cold_cache_benches);
criterion_main!(benches);
