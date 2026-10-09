//! Per-process allocation regression for bounded cold-flush planning.
//!
//! Run alone: this file deliberately contains one test because dhat profiling
//! is process-wide. Put additional allocation probes in separate test binaries.

#[path = "../benches/fixture.rs"]
mod fixture;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[test]
fn cold_flush_planning_allocations_are_bounded_and_released() {
    let _profiler = dhat::Profiler::builder().testing().build();
    for scenario in [
        fixture::FlushScenario::HotOnly,
        fixture::FlushScenario::HalfCold,
        fixture::FlushScenario::ManyStreams,
    ] {
        let machine = fixture::build_state(scenario);
        let before = dhat::HeapStats::get();
        let result = std::hint::black_box(machine.plan_next_cold_flush_batch(
            fixture::MIN_HOT_BYTES,
            fixture::MAX_FLUSH_BYTES,
            usize::MAX,
            fixture::MAX_CANDIDATES,
        ));
        let after = dhat::HeapStats::get();
        dhat::assert!(
            after.total_bytes.saturating_sub(before.total_bytes) < 64 * 1024,
            "{} planning copied unbounded state",
            scenario.name()
        );
        drop(result);
        let remaining = dhat::HeapStats::get().curr_bytes;
        dhat::assert_eq!(remaining, before.curr_bytes);
    }
}
