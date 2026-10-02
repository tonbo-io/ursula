//! Keyed-state point-read latency against namespace size (design §9.3: a
//! point read should not grow with the namespace).
//!
//! Builds one namespace per size — a base run holding every key plus three
//! small newer runs, the shape size-tiered compaction leaves behind — and
//! reads random keys through the two openers the engine uses: the verified
//! object-store reader behind the serving range cache (a restarted pod) and
//! the in-memory parts the writer filled on write.

#![allow(clippy::arithmetic_side_effects, reason = "benchmark scaffolding")]

use std::sync::Arc;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use tempfile::TempDir;
use ursula_index::EventIndexCache;
use ursula_index::FsObjectStore;
use ursula_index::ObjectStore;
use ursula_index::keyed::KeyedNamespace;
use ursula_index::keyed::KeyedRunMeta;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::MemoryParts;
use ursula_index::keyed::PartOpener;
use ursula_index::keyed::PartOptions;
use ursula_index::keyed::RunBuilder;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::get;

const SIZES: [u64; 2] = [1_000, 100_000];
const VALUE_BYTES: usize = 700;
const NEWER_RUNS: u64 = 3;

fn key(index: u64) -> String {
    format!("m/{index:08}")
}

fn message(keys: impl Iterator<Item = u64>, filler: &str) -> String {
    let ops: Vec<String> = keys
        .map(|index| {
            format!(
                r#"["p","{}","{filler}"]"#,
                encode_key(key(index).as_bytes())
            )
        })
        .collect();
    format!(r#"{{"ops":[{}]}}"#, ops.join(","))
}

struct Fixture {
    runs: Vec<KeyedRunMeta>,
    memory: MemoryParts,
    namespace: KeyedNamespace,
    _objects: TempDir,
}

async fn fixture(size: u64, options: &PartOptions) -> anyhow::Result<Fixture> {
    let filler = "x".repeat(VALUE_BYTES);
    let objects = TempDir::new()?;
    let store = ObjectStore::from(FsObjectStore::new(objects.path())?);
    let namespace = KeyedNamespace::new(store, KeyedSource {
        bucket: "bench".to_owned(),
        key: format!("size-{size}"),
        incarnation: 1,
    });
    let mut memory = MemoryParts::new();
    let mut runs = Vec::new();
    let mut builder = RunBuilder::new(0, true);
    for record in 0..size {
        builder.apply_message(record, &message(std::iter::once(record), &filler))?;
    }
    let mut built = vec![builder.finish(options)?];
    for newer in 0..NEWER_RUNS {
        let mut builder = RunBuilder::new(size + newer, false);
        let keys = (0..size).step_by(97).map(|index| index + newer);
        builder.apply_message(size + newer, &message(keys, &filler))?;
        built.push(builder.finish(options)?);
    }
    for run in built.into_iter().rev() {
        for part in &run.parts {
            namespace.put_part(part).await?;
            memory.insert(part);
        }
        runs.push(run.meta);
    }
    Ok(Fixture {
        runs,
        memory,
        namespace,
        _objects: objects,
    })
}

fn keyed_point_read(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("create benchmark runtime");
    let options = PartOptions::default();
    let mut group = criterion.benchmark_group("keyed_point_read");
    for size in SIZES {
        let fixture = runtime
            .block_on(fixture(size, &options))
            .expect("build the namespace");
        let cache_dir = TempDir::new().expect("create cache directory");
        let cache = EventIndexCache::serving(cache_dir.path(), 256 * 1024 * 1024)
            .expect("create the serving cache");
        let store: Arc<dyn PartOpener> = Arc::new(
            fixture
                .namespace
                .opener()
                .with_cache(&cache)
                .expect("attach the range cache"),
        );
        let memory: Arc<dyn PartOpener> = Arc::new(fixture.memory.clone());
        for (name, opener) in [("store_cached", store), ("written", memory)] {
            let mut next = 0_u64;
            group.bench_function(BenchmarkId::new(name, size), |bencher| {
                bencher.iter(|| {
                    next = next.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    let wanted = key((next >> 33) % size);
                    let row = runtime
                        .block_on(get(&*opener, &fixture.runs, wanted.as_bytes(), &options))
                        .expect("point read");
                    black_box(row.expect("the key exists"));
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, keyed_point_read);
criterion_main!(benches);
