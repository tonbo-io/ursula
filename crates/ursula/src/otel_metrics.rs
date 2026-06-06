//! Export-time bridge from the runtime's per-core atomic metrics to OTLP.
//!
//! The hot path keeps incrementing the lock-free `PaddedAtomicU64` arrays
//! exactly as before. Here we register OpenTelemetry *observable* instruments
//! whose callbacks run only at the meter provider's export interval, read a
//! [`RuntimeMetricsSnapshot`], and report the counters/gauges.
//!
//! Nothing is added to the hot path, and no per-record OTel `add` call is made
//! (which would hash an attribute set and contend a shared aggregator). When no
//! OTLP meter provider is installed, the global meter is a no-op and the
//! callbacks never run.
//!
//! Per-group metrics carry a `group_id` label. That dimension is bounded by the
//! Raft group count, so series cardinality stays fixed. `stream_id`/`bucket_id`
//! are never used as labels (unbounded) — they live on spans only.

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Meter;
use ursula_runtime::RuntimeMetrics;
use ursula_runtime::RuntimeMetricsSnapshot;

/// Register observable instruments backed by the runtime metrics snapshot.
///
/// Safe to call unconditionally: with no OTLP meter provider the instruments
/// are inert. The instruments are retained by the global meter for the life of
/// the process.
pub(crate) fn register(metrics: &RuntimeMetrics) {
    let meter = global::meter("ursula-runtime");

    // Per-group monotonic counters (labeled by `group_id`). Totals are the
    // backend's `sum(...)` over the label, so we don't also emit an aggregate.
    per_group_counter(
        &meter,
        metrics,
        "ursula.appends.accepted",
        None,
        "Appends accepted by the runtime",
        |s| &s.per_group_appends,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.mutations.applied",
        None,
        "Stream mutations applied on core",
        |s| &s.per_group_applied_mutations,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.mutation_apply.ns",
        Some("ns"),
        "Cumulative mutation apply time",
        |s| &s.per_group_mutation_apply_ns,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.group_engine_exec.ns",
        Some("ns"),
        "Cumulative group-engine execution time",
        |s| &s.per_group_group_engine_exec_ns,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.raft_apply.entries",
        None,
        "Raft entries applied",
        |s| &s.per_group_raft_apply_entries,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.raft_apply.ns",
        Some("ns"),
        "Cumulative raft apply time",
        |s| &s.per_group_raft_apply_ns,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.wal.batches",
        None,
        "WAL batches written",
        |s| &s.per_group_wal_batches,
    );
    per_group_counter(
        &meter,
        metrics,
        "ursula.wal.records",
        None,
        "WAL records written",
        |s| &s.per_group_wal_records,
    );

    // Per-group point-in-time gauge.
    per_group_gauge(
        &meter,
        metrics,
        "ursula.group_mailbox.depth",
        "Group mailbox depth",
        |s| &s.per_group_group_mailbox_depth,
    );

    // Per-core only (no group dimension): keep these as process-level
    // aggregates with no label.
    let m = metrics.clone();
    let _ = meter
        .u64_observable_counter("ursula.routed_requests")
        .with_description("Requests routed to a core mailbox")
        .with_callback(move |observer| observer.observe(m.snapshot().routed_requests, &[]))
        .build();

    let m = metrics.clone();
    let _ = meter
        .u64_observable_gauge("ursula.live_read.waiters")
        .with_description("Live-read waiters parked across cores")
        .with_callback(move |observer| observer.observe(m.snapshot().live_read_waiters, &[]))
        .build();
}

/// Observe one value per Raft group, labeled with the bounded `group_id`.
fn per_group_counter(
    meter: &Meter,
    metrics: &RuntimeMetrics,
    name: &'static str,
    unit: Option<&'static str>,
    description: &'static str,
    field: fn(&RuntimeMetricsSnapshot) -> &[u64],
) {
    let metrics = metrics.clone();
    let mut builder = meter
        .u64_observable_counter(name)
        .with_description(description);
    if let Some(unit) = unit {
        builder = builder.with_unit(unit);
    }
    let _ = builder
        .with_callback(move |observer| {
            for (group, value) in field(&metrics.snapshot()).iter().enumerate() {
                observer.observe(*value, &[group_label(group)]);
            }
        })
        .build();
}

fn per_group_gauge(
    meter: &Meter,
    metrics: &RuntimeMetrics,
    name: &'static str,
    description: &'static str,
    field: fn(&RuntimeMetricsSnapshot) -> &[u64],
) {
    let metrics = metrics.clone();
    let _ = meter
        .u64_observable_gauge(name)
        .with_description(description)
        .with_callback(move |observer| {
            for (group, value) in field(&metrics.snapshot()).iter().enumerate() {
                observer.observe(*value, &[group_label(group)]);
            }
        })
        .build();
}

fn group_label(group: usize) -> KeyValue {
    KeyValue::new("group_id", i64::try_from(group).unwrap_or(-1))
}
