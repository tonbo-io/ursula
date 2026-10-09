//! Run this ignored measurement alone in an optimized test build. It compares
//! layouts of the five logical append counters under one harness:
//!
//! - `plain`: five adjacent `AtomicU64`s in one cache line, as before padding;
//! - `per_counter`: each counter on its own `CachePadded` line;
//! - `per_kind`: the heartbeat pair on one padded line, the replication triple
//!   on another;
//! - `production`: the production `record_append_logical_sample` and statics
//!   (per-kind padding), as a check on the `per_kind` copy.
//!
//! Even workers record heartbeats and odd workers replication batches, so two
//! workers isolate cross-kind false sharing and four or eight add same-kind
//! true sharing. Each worker pins to one core (on Linux), in the order given by
//! `URSULA_COUNTER_BENCH_CORES` (comma-separated CPU ids) or the OS order.
//! Rounds interleave the layouts so machine drift affects each one alike.

use std::sync::Barrier;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crossbeam_utils::CachePadded;

use super::AppendLogicalSample;
use super::GRPC_APPEND_HEARTBEAT;
use super::GRPC_APPEND_REPLICATION;
use super::record_append_logical_sample;

const WORKERS: [usize; 4] = [1, 2, 4, 8];
const WARMUP_OPERATIONS: u64 = 200_000;
const OPERATIONS: u64 = 2_000_000;
const ROUNDS: usize = 7;

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Layout {
    Plain,
    PerCounter,
    PerKind,
    Production,
}

const LAYOUTS: [Layout; 4] = [
    Layout::Plain,
    Layout::PerCounter,
    Layout::PerKind,
    Layout::Production,
];

#[derive(serde::Serialize)]
struct Measurement {
    layout: Layout,
    workers: usize,
    operations_per_worker: u64,
    cores: Vec<usize>,
    /// Whether every worker of every round was pinned to its core.
    pinned: bool,
    /// Slowest worker per round, starting after the common barrier; excludes
    /// thread spawn and join.
    elapsed_ns: Vec<u64>,
}

trait Counters: Sync {
    fn record(&self, sample: AppendLogicalSample);
    fn requests(&self) -> u64;
}

/// The same update sequence as `record_append_logical_sample`, so the local
/// layouts differ only in where the counters live.
#[inline(always)]
fn record_into(
    heartbeat: (&AtomicU64, &AtomicU64),
    replication: (&AtomicU64, &AtomicU64, &AtomicU64),
    sample: AppendLogicalSample,
) {
    if sample.heartbeat {
        heartbeat.0.fetch_add(1, Ordering::Relaxed);
        heartbeat
            .1
            .fetch_add(sample.request_bytes, Ordering::Relaxed);
        return;
    }
    replication.0.fetch_add(1, Ordering::Relaxed);
    replication
        .1
        .fetch_add(sample.request_bytes, Ordering::Relaxed);
    replication.2.fetch_add(sample.entries, Ordering::Relaxed);
}

/// Five adjacent counters, aligned so all of them share one cache line.
#[repr(C, align(128))]
#[derive(Default)]
struct Plain {
    heartbeat_requests: AtomicU64,
    heartbeat_request_bytes: AtomicU64,
    replication_requests: AtomicU64,
    replication_request_bytes: AtomicU64,
    replication_entries: AtomicU64,
}

impl Counters for Plain {
    #[inline(never)]
    fn record(&self, sample: AppendLogicalSample) {
        record_into(
            (&self.heartbeat_requests, &self.heartbeat_request_bytes),
            (
                &self.replication_requests,
                &self.replication_request_bytes,
                &self.replication_entries,
            ),
            sample,
        );
    }

    fn requests(&self) -> u64 {
        self.heartbeat_requests
            .load(Ordering::Relaxed)
            .saturating_add(self.replication_requests.load(Ordering::Relaxed))
    }
}

#[derive(Default)]
struct PerCounter {
    heartbeat_requests: CachePadded<AtomicU64>,
    heartbeat_request_bytes: CachePadded<AtomicU64>,
    replication_requests: CachePadded<AtomicU64>,
    replication_request_bytes: CachePadded<AtomicU64>,
    replication_entries: CachePadded<AtomicU64>,
}

impl Counters for PerCounter {
    #[inline(never)]
    fn record(&self, sample: AppendLogicalSample) {
        record_into(
            (&self.heartbeat_requests, &self.heartbeat_request_bytes),
            (
                &self.replication_requests,
                &self.replication_request_bytes,
                &self.replication_entries,
            ),
            sample,
        );
    }

    fn requests(&self) -> u64 {
        self.heartbeat_requests
            .load(Ordering::Relaxed)
            .saturating_add(self.replication_requests.load(Ordering::Relaxed))
    }
}

#[derive(Default)]
struct HeartbeatCounters {
    requests: AtomicU64,
    request_bytes: AtomicU64,
}

#[derive(Default)]
struct ReplicationCounters {
    requests: AtomicU64,
    request_bytes: AtomicU64,
    entries: AtomicU64,
}

#[derive(Default)]
struct PerKind {
    heartbeat: CachePadded<HeartbeatCounters>,
    replication: CachePadded<ReplicationCounters>,
}

impl Counters for PerKind {
    #[inline(never)]
    fn record(&self, sample: AppendLogicalSample) {
        record_into(
            (&self.heartbeat.requests, &self.heartbeat.request_bytes),
            (
                &self.replication.requests,
                &self.replication.request_bytes,
                &self.replication.entries,
            ),
            sample,
        );
    }

    fn requests(&self) -> u64 {
        self.heartbeat
            .requests
            .load(Ordering::Relaxed)
            .saturating_add(self.replication.requests.load(Ordering::Relaxed))
    }
}

/// The production function and statics.
struct Production;

impl Counters for Production {
    #[inline(never)]
    fn record(&self, sample: AppendLogicalSample) {
        record_append_logical_sample(sample);
    }

    fn requests(&self) -> u64 {
        GRPC_APPEND_HEARTBEAT
            .requests
            .load(Ordering::Relaxed)
            .saturating_add(GRPC_APPEND_REPLICATION.requests.load(Ordering::Relaxed))
    }
}

#[derive(Default)]
struct Instances {
    plain: Plain,
    per_counter: PerCounter,
    per_kind: PerKind,
}

/// Pins on Linux. Elsewhere `core_affinity` may only hint the scheduler.
fn pin_current(core: usize) -> bool {
    core_affinity::set_for_current(core_affinity::CoreId { id: core })
}

fn available_cores() -> Vec<usize> {
    if let Some(cores) = std::env::var_os("URSULA_COUNTER_BENCH_CORES") {
        return cores
            .to_str()
            .expect("URSULA_COUNTER_BENCH_CORES is UTF-8")
            .split(',')
            .map(str::trim)
            .filter(|core| !core.is_empty())
            .map(|core| {
                core.parse()
                    .expect("URSULA_COUNTER_BENCH_CORES lists CPU ids")
            })
            .collect();
    }
    let count = std::thread::available_parallelism()
        .expect("available parallelism")
        .get();
    (0..count).collect()
}

/// One round on `cores.len()` workers; returns the slowest worker's elapsed
/// nanoseconds and whether every worker was pinned.
fn trial<C: Counters>(counters: &C, cores: &[usize], operations: u64) -> (u64, bool) {
    let start = Barrier::new(cores.len());
    let before = counters.requests();
    let (elapsed, pinned) = std::thread::scope(|scope| {
        let workers = cores
            .iter()
            .enumerate()
            .map(|(worker, &core)| {
                let start = &start;
                scope.spawn(move || {
                    let pinned = pin_current(core);
                    let heartbeat = worker.is_multiple_of(2);
                    let sample = AppendLogicalSample {
                        heartbeat,
                        request_bytes: if heartbeat { 64 } else { 4096 },
                        entries: if heartbeat { 0 } else { 16 },
                    };
                    start.wait();
                    let began = Instant::now();
                    for _ in 0..operations {
                        counters.record(std::hint::black_box(sample));
                    }
                    let elapsed = u64::try_from(began.elapsed().as_nanos())
                        .expect("elapsed nanoseconds fit u64");
                    (elapsed, pinned)
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .fold((0, true), |(slowest, all_pinned), (elapsed, pinned)| {
                (slowest.max(elapsed), all_pinned && pinned)
            })
    });
    let workers = u64::try_from(cores.len()).expect("worker count fits u64");
    assert_eq!(
        counters.requests().saturating_sub(before),
        operations.saturating_mul(workers),
        "every recorded sample is counted once"
    );
    (elapsed, pinned)
}

fn run(layout: Layout, instances: &Instances, cores: &[usize], operations: u64) -> (u64, bool) {
    match layout {
        Layout::Plain => trial(&instances.plain, cores, operations),
        Layout::PerCounter => trial(&instances.per_counter, cores, operations),
        Layout::PerKind => trial(&instances.per_kind, cores, operations),
        Layout::Production => trial(&Production, cores, operations),
    }
}

#[test]
#[ignore = "optimized microbenchmark; run alone with URSULA_COUNTER_BENCH_OUTPUT set"]
fn measure_grpc_counter_contention() {
    let output = std::env::var_os("URSULA_COUNTER_BENCH_OUTPUT")
        .expect("set URSULA_COUNTER_BENCH_OUTPUT to the raw JSON output path");
    let available = available_cores();
    let instances = Instances::default();
    let mut measurements = Vec::new();
    for workers in WORKERS {
        let Some(cores) = available.get(..workers) else {
            continue;
        };
        let mut rounds = LAYOUTS.map(|layout| Measurement {
            layout,
            workers,
            operations_per_worker: OPERATIONS,
            cores: cores.to_vec(),
            pinned: true,
            elapsed_ns: Vec::with_capacity(ROUNDS),
        });
        for layout in LAYOUTS {
            run(layout, &instances, cores, WARMUP_OPERATIONS);
        }
        for _ in 0..ROUNDS {
            for measurement in &mut rounds {
                let (elapsed, pinned) = run(measurement.layout, &instances, cores, OPERATIONS);
                measurement.elapsed_ns.push(elapsed);
                measurement.pinned &= pinned;
            }
        }
        measurements.extend(rounds);
    }
    let file = std::fs::File::create(output).expect("create the output file");
    serde_json::to_writer_pretty(file, &measurements).expect("write the measurements");
}
