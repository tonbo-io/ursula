use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use crossbeam_utils::CachePadded;

const OPS_PER_THREAD: u64 = 100_000;

struct PackedCounters {
    left: AtomicU64,
    right: AtomicU64,
}

struct PaddedCounters {
    left: CachePadded<AtomicU64>,
    right: CachePadded<AtomicU64>,
}

fn run_packed(thread_count: usize) -> u64 {
    let counters = PackedCounters {
        left: AtomicU64::new(0),
        right: AtomicU64::new(0),
    };
    std::thread::scope(|scope| {
        for thread_index in 0..thread_count {
            let counter = if thread_index.is_multiple_of(2) {
                &counters.left
            } else {
                &counters.right
            };
            scope.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    counters
        .left
        .load(Ordering::Relaxed)
        .saturating_add(counters.right.load(Ordering::Relaxed))
}

fn run_padded(thread_count: usize) -> u64 {
    let counters = PaddedCounters {
        left: CachePadded::new(AtomicU64::new(0)),
        right: CachePadded::new(AtomicU64::new(0)),
    };
    std::thread::scope(|scope| {
        for thread_index in 0..thread_count {
            let counter = if thread_index.is_multiple_of(2) {
                &counters.left
            } else {
                &counters.right
            };
            scope.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    counters
        .left
        .load(Ordering::Relaxed)
        .saturating_add(counters.right.load(Ordering::Relaxed))
}

fn run_max(thread_count: usize, sharded: bool) -> u64 {
    let counters = (0..thread_count)
        .map(|_| CachePadded::new(AtomicU64::new(0)))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        for (index, local) in counters.iter().enumerate() {
            let counter = if sharded {
                local
            } else {
                counters.first().expect("nonempty")
            };
            scope.spawn(move || {
                for value in 0..OPS_PER_THREAD {
                    counter.fetch_max(value, Ordering::Relaxed);
                }
                black_box(index);
            });
        }
    });
    counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .max()
        .unwrap_or_default()
}

fn run_read_admission(thread_count: usize, sharded: bool) {
    let semaphores = (0..thread_count)
        .map(|_| std::sync::Arc::new(tokio::sync::Semaphore::new(thread_count)))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        for local in &semaphores {
            let semaphore = if sharded {
                local
            } else {
                semaphores.first().expect("nonempty")
            };
            scope.spawn(move || {
                for _ in 0..10_000 {
                    let permit = semaphore
                        .clone()
                        .try_acquire_owned()
                        .expect("enough permits");
                    black_box(&permit);
                    drop(permit);
                }
            });
        }
    });
}

fn atomic_padding_benches(c: &mut Criterion) {
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2);
    let mut thread_counts = [2, available.min(8)]
        .into_iter()
        .filter(|threads| *threads >= 2)
        .collect::<Vec<_>>();
    thread_counts.sort_unstable();
    thread_counts.dedup();
    let mut group = c.benchmark_group("atomic_padding_false_sharing");
    for thread_count in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("packed_adjacent", thread_count),
            &thread_count,
            |b, &threads| {
                b.iter(|| black_box(run_packed(threads)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cache_padded", thread_count),
            &thread_count,
            |b, &threads| {
                b.iter(|| black_box(run_padded(threads)));
            },
        );
    }
    group.finish();
    let mut maxima = c.benchmark_group("owner_max_counter");
    for threads in [1, 2, 4, 8] {
        maxima.bench_with_input(
            BenchmarkId::new("shared", threads),
            &threads,
            |b, &threads| b.iter(|| black_box(run_max(threads, false))),
        );
        maxima.bench_with_input(
            BenchmarkId::new("per_owner", threads),
            &threads,
            |b, &threads| b.iter(|| black_box(run_max(threads, true))),
        );
    }
    maxima.finish();
    let mut admission = c.benchmark_group("owner_read_admission");
    for threads in [1, 2, 4, 8] {
        admission.bench_with_input(
            BenchmarkId::new("shared", threads),
            &threads,
            |b, &threads| b.iter(|| run_read_admission(threads, false)),
        );
        admission.bench_with_input(
            BenchmarkId::new("per_owner", threads),
            &threads,
            |b, &threads| b.iter(|| run_read_admission(threads, true)),
        );
    }
    admission.finish();
}

fn owner_dispatch_benches(c: &mut Criterion) {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let mut group = c.benchmark_group("owner_dispatch");
    for threading in [
        ursula_runtime::RuntimeThreading::HostedTokio,
        ursula_runtime::RuntimeThreading::ThreadPerCore,
    ] {
        let mut config = ursula_runtime::RuntimeConfig::new(1, 1);
        config.threading = threading;
        let runtime = executor
            .block_on(async { ursula_runtime::ShardRuntime::spawn(config).expect("owner") });
        executor.block_on(runtime.warm_all_groups()).expect("warm");
        group.bench_function(format!("{threading:?}"), |b| {
            b.iter(|| {
                executor.block_on(async {
                    for _ in 0..100 {
                        black_box(
                            runtime
                                .state_gauges(ursula_shard::RaftGroupId(0))
                                .await
                                .expect("gauge"),
                        );
                    }
                });
            })
        });
        group.bench_function(format!("{threading:?}_on_owner"), |b| {
            b.iter(|| {
                let local = runtime.clone();
                let completed = runtime
                    .spawn_on_owner(ursula_shard::CoreId(0), async move {
                        for _ in 0..100 {
                            black_box(
                                local
                                    .state_gauges(ursula_shard::RaftGroupId(0))
                                    .await
                                    .expect("gauge"),
                            );
                        }
                    })
                    .expect("schedule");
                executor.block_on(completed).expect("completed");
            })
        });
        executor.block_on(runtime.shutdown_owners());
    }
    group.finish();
}

criterion_group!(benches, atomic_padding_benches, owner_dispatch_benches);
criterion_main!(benches);
