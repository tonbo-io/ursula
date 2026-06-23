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
            let counter = if thread_index % 2 == 0 {
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
    counters.left.load(Ordering::Relaxed) + counters.right.load(Ordering::Relaxed)
}

fn run_padded(thread_count: usize) -> u64 {
    let counters = PaddedCounters {
        left: CachePadded::new(AtomicU64::new(0)),
        right: CachePadded::new(AtomicU64::new(0)),
    };
    std::thread::scope(|scope| {
        for thread_index in 0..thread_count {
            let counter = if thread_index % 2 == 0 {
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
    counters.left.load(Ordering::Relaxed) + counters.right.load(Ordering::Relaxed)
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
}

criterion_group!(benches, atomic_padding_benches);
criterion_main!(benches);
