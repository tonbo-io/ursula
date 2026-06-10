use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;

mod fixture;

use fixture::FlushScenario;
use fixture::MAX_CANDIDATES;
use fixture::MAX_FLUSH_BYTES;
use fixture::MIN_HOT_BYTES;
use fixture::build_state;

fn plan_next_cold_flush_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_next_cold_flush");

    for scenario in [
        FlushScenario::HotOnly,
        FlushScenario::HalfCold,
        FlushScenario::ManyStreams,
    ] {
        let machine = build_state(scenario);
        group.throughput(Throughput::Elements(
            u64::try_from(MAX_CANDIDATES).expect("max candidates fits u64"),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name()),
            &scenario,
            |b, _| {
                b.iter_batched(
                    || machine.clone(),
                    |machine| {
                        let result = machine.plan_next_cold_flush_batch(
                            MIN_HOT_BYTES,
                            MAX_FLUSH_BYTES,
                            MAX_CANDIDATES,
                        );
                        let _ = black_box(result);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, plan_next_cold_flush_benches);
criterion_main!(benches);
