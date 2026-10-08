//! Run this ignored measurement alone in an optimized test build. It calls the
//! production accounting functions without network or profiler overhead.

use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::Semaphore;

use super::AppendLogicalSample;
use super::GRPC_APPEND_HEARTBEAT_REQUESTS;
use super::GRPC_APPEND_REPLICATION_REQUESTS;
use super::GRPC_APPEND_STREAM_QUEUED_BYTES;
use super::GRPC_APPEND_STREAM_QUEUED_BYTES_MAX;
use super::GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES;
use super::GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES_MAX;
use super::QueuedAppendBytes;
use super::record_append_logical_sample;

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Workload {
    LogicalMixed,
    QueuedMixed,
}

#[derive(serde::Serialize)]
struct Measurement {
    workload: Workload,
    threads: usize,
    operations_per_thread: u64,
    /// Slowest worker, starting after the common barrier; excludes thread spawn/join.
    elapsed_ns: Vec<u64>,
}

#[expect(
    clippy::unwrap_used,
    reason = "benchmark setup and worker failures invalidate the measurement"
)]
fn trial(workload: Workload, threads: usize, operations: u64) -> u64 {
    let start = Barrier::new(threads);
    let budget = Arc::new(Semaphore::new(1024));
    let requests_before = GRPC_APPEND_HEARTBEAT_REQUESTS
        .load(Ordering::Relaxed)
        .saturating_add(GRPC_APPEND_REPLICATION_REQUESTS.load(Ordering::Relaxed));
    let send_before = GRPC_APPEND_STREAM_QUEUED_BYTES.load(Ordering::Relaxed);
    let receive_before = GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES.load(Ordering::Relaxed);
    let elapsed = std::thread::scope(|scope| {
        let workers = (0..threads)
            .map(|worker| {
                let start = &start;
                let budget = &budget;
                scope.spawn(move || {
                    let sender = worker.is_multiple_of(2);
                    let sample = AppendLogicalSample {
                        heartbeat: sender,
                        request_bytes: if sender { 64 } else { 4096 },
                        entries: if sender { 0 } else { 16 },
                    };
                    start.wait();
                    let began = Instant::now();
                    for _ in 0..operations {
                        match workload {
                            Workload::LogicalMixed => {
                                record_append_logical_sample(std::hint::black_box(sample))
                            }
                            Workload::QueuedMixed => {
                                let permit = budget.clone().try_acquire_many_owned(64).unwrap();
                                let (gauge, max) = if sender {
                                    (
                                        &GRPC_APPEND_STREAM_QUEUED_BYTES,
                                        &GRPC_APPEND_STREAM_QUEUED_BYTES_MAX,
                                    )
                                } else {
                                    (
                                        &GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES,
                                        &GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES_MAX,
                                    )
                                };
                                let charge = QueuedAppendBytes::new(permit, 64, gauge, max);
                                std::hint::black_box(&charge);
                                drop(charge);
                            }
                        }
                    }
                    u64::try_from(began.elapsed().as_nanos()).unwrap()
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .max()
            .unwrap()
    });
    assert_eq!(
        GRPC_APPEND_STREAM_QUEUED_BYTES.load(Ordering::Relaxed),
        send_before
    );
    assert_eq!(
        GRPC_APPEND_STREAM_SERVER_BUFFERED_BYTES.load(Ordering::Relaxed),
        receive_before
    );
    assert_eq!(budget.available_permits(), 1024);
    if matches!(workload, Workload::LogicalMixed) {
        let requests_after = GRPC_APPEND_HEARTBEAT_REQUESTS
            .load(Ordering::Relaxed)
            .saturating_add(GRPC_APPEND_REPLICATION_REQUESTS.load(Ordering::Relaxed));
        assert_eq!(
            requests_after.saturating_sub(requests_before),
            operations.saturating_mul(u64::try_from(threads).unwrap())
        );
    }
    elapsed
}

#[test]
#[ignore = "optimized microbenchmark; run alone with URSULA_COUNTER_BENCH_OUTPUT set"]
fn measure_grpc_counter_contention() {
    let output = std::env::var_os("URSULA_COUNTER_BENCH_OUTPUT")
        .expect("set URSULA_COUNTER_BENCH_OUTPUT to the raw JSON output path");
    let available = std::thread::available_parallelism().unwrap().get();
    let mut measurements = Vec::new();
    for threads in [1, 2, 8].into_iter().filter(|count| *count <= available) {
        for workload in [Workload::LogicalMixed, Workload::QueuedMixed] {
            trial(workload, threads, 100_000);
            measurements.push(Measurement {
                workload,
                threads,
                operations_per_thread: 1_000_000,
                elapsed_ns: (0..9)
                    .map(|_| trial(workload, threads, 1_000_000))
                    .collect(),
            });
        }
    }
    let file = std::fs::File::create(output).unwrap();
    serde_json::to_writer_pretty(file, &measurements).unwrap();
}
