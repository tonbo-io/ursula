//! Export-time bridge from the runtime's per-core atomic metrics to OTLP.
//!
//! The hot path keeps incrementing the lock-free `PaddedAtomicU64` arrays
//! exactly as before. Here we register OpenTelemetry *observable* instruments
//! whose callbacks run only at the meter provider's export interval, read a
//! [`RuntimeMetricsSnapshot`], and report the aggregate counters/gauges.
//!
//! Nothing is added to the hot path, and no per-record OTel `add` call is made
//! (which would hash an attribute set and contend a shared aggregator). When no
//! OTLP meter provider is installed, the global meter is a no-op and the
//! callbacks never run.

use opentelemetry::global;
use ursula_runtime::RuntimeMetrics;

/// Register observable instruments backed by the runtime metrics snapshot.
///
/// Safe to call unconditionally: with no OTLP meter provider the instruments
/// are inert. The instruments are retained by the global meter for the life of
/// the process.
pub(crate) fn register(metrics: &RuntimeMetrics) {
    let meter = global::meter("ursula-runtime");

    // Monotonic counters.
    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.appends.accepted")
        .with_description("Appends accepted by the runtime")
        .with_callback(move |observer| observer.observe(m.snapshot().accepted_appends, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.mutations.applied")
        .with_description("Stream mutations applied on core")
        .with_callback(move |observer| observer.observe(m.snapshot().applied_mutations, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.mutation_apply.ns")
        .with_unit("ns")
        .with_description("Cumulative mutation apply time")
        .with_callback(move |observer| observer.observe(m.snapshot().mutation_apply_ns, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.group_engine_exec.ns")
        .with_unit("ns")
        .with_description("Cumulative group-engine execution time")
        .with_callback(move |observer| observer.observe(m.snapshot().group_engine_exec_ns, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.raft_apply.entries")
        .with_description("Raft entries applied")
        .with_callback(move |observer| observer.observe(m.snapshot().raft_apply_entries, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.raft_apply.ns")
        .with_unit("ns")
        .with_description("Cumulative raft apply time")
        .with_callback(move |observer| observer.observe(m.snapshot().raft_apply_ns, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.routed_requests")
        .with_description("Requests routed to a core mailbox")
        .with_callback(move |observer| observer.observe(m.snapshot().routed_requests, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.wal.batches")
        .with_description("WAL batches written")
        .with_callback(move |observer| observer.observe(m.snapshot().wal_batches, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.wal.records")
        .with_description("WAL records written")
        .with_callback(move |observer| observer.observe(m.snapshot().wal_records, &[]))
        .build();

    // Point-in-time gauges.
    let m = metrics.clone();
    let _ = meter
        .u64_observable_gauge("ursula.group_mailbox.depth")
        .with_description("Total group mailbox depth")
        .with_callback(move |observer| observer.observe(m.snapshot().group_mailbox_depth, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_gauge("ursula.live_read.waiters")
        .with_description("Live-read waiters parked across cores")
        .with_callback(move |observer| observer.observe(m.snapshot().live_read_waiters, &[]))
        .build();
}
