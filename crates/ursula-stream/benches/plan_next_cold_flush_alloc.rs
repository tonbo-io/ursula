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

fn plan_next_cold_flush_alloc_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_next_cold_flush_alloc");

    for scenario in [
        FlushScenario::HotOnly,
        FlushScenario::HalfCold,
        FlushScenario::ManyStreams,
    ] {
        let machine = build_state(scenario);

        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name()),
            &scenario,
            |b, _| {
                b.iter_batched(
                    || machine.clone(),
                    |machine| {
                        black_box(machine.plan_next_cold_flush_batch(
                            MIN_HOT_BYTES,
                            MAX_FLUSH_BYTES,
                            usize::MAX,
                            MAX_CANDIDATES,
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
