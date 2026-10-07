use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;

mod fixture;

use fixture::FlushScenario;
use fixture::MAX_CANDIDATES;
use fixture::MAX_FLUSH_BYTES;
use fixture::MIN_HOT_BYTES;
use fixture::build_state;

#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

fn snapshot() -> (u64, usize) {
    let stats = dhat::HeapStats::get();
    (stats.total_bytes, stats.curr_bytes)
}

fn plan_next_cold_flush_alloc_benches(c: &mut Criterion) {
    let _profiler = dhat::Profiler::builder().testing().build();
    let mut group = c.benchmark_group("plan_next_cold_flush_alloc");

    for scenario in [
        FlushScenario::HotOnly,
        FlushScenario::HalfCold,
        FlushScenario::ManyStreams,
    ] {
        // Snapshot before building the machine.
        let (_, baseline) = snapshot();
        let machine = build_state(scenario);
        let (_, after_build) = snapshot();
        let machine_bytes = after_build.saturating_sub(baseline);

        // Single-shot memory measurement printed to stderr.
        let m = machine.clone();
        let (alloc_before, current_before) = snapshot();
        let result = m.plan_next_cold_flush_batch(
            MIN_HOT_BYTES,
            MAX_FLUSH_BYTES,
            usize::MAX,
            MAX_CANDIDATES,
        );
        black_box(&result);
        let (alloc_after_plan, current_after_plan) = snapshot();
        drop(result);
        let (_, current_after_drop) = snapshot();

        let allocated = alloc_after_plan.saturating_sub(alloc_before);
        tracing::info!(
            "  [mem] {}:\n    machine: {} bytes\n    before plan: {} bytes\n    allocated during plan: {} bytes\n    after plan (with result): {} bytes\n    after drop result: {} bytes",
            scenario.name(),
            machine_bytes,
            current_before,
            allocated,
            current_after_plan,
            current_after_drop,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name()),
            &scenario,
            |b, _| {
                b.iter_batched(
                    || machine.clone(),
                    |machine| {
                        let (alloc_before, current_before) = snapshot();
                        let result = machine.plan_next_cold_flush_batch(
                            MIN_HOT_BYTES,
                            MAX_FLUSH_BYTES,
                            usize::MAX,
                            MAX_CANDIDATES,
                        );
                        black_box(&result);
                        let (alloc_after, current_after) = snapshot();
                        drop(result);
                        let (_, current_after_drop) = snapshot();
                        black_box((
                            alloc_after.saturating_sub(alloc_before),
                            current_before,
                            current_after,
                            current_after_drop,
                        ))
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, plan_next_cold_flush_alloc_benches);
criterion_main!(benches);
