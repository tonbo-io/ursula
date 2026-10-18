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

/// Register `ursula.cluster.info`, 1 with this node's `cluster_id` as a
/// label, so dashboards and alerts tell deployments apart, and
/// `ursula.raft.peer_cluster_refusals`, the Raft RPCs refused since start
/// because the peer is in another cluster.
pub(crate) fn register_cluster(
    cluster_id: &ursula_headers::ClusterId,
    registry: Option<ursula_raft::RaftGroupHandleRegistry>,
) {
    let label = KeyValue::new("cluster_id", cluster_id.to_string());
    let _ = global::meter("ursula")
        .u64_observable_gauge("ursula.cluster.info")
        .with_description("1, labelled with this node's server.cluster_id")
        .with_callback(move |observer| observer.observe(1, std::slice::from_ref(&label)))
        .build();
    let Some(registry) = registry else {
        return;
    };
    let _ = global::meter("ursula-raft")
        .u64_observable_counter("ursula.raft.peer_cluster_refusals")
        .with_description(
            "Raft RPCs refused since start because the peer is in another cluster; check \
             server.cluster_id and raft.peers",
        )
        .with_callback({
            let registry = registry.clone();
            move |observer| observer.observe(registry.peer_cluster().refusals(), &[])
        })
        .build();
    let _ = global::meter("ursula-raft")
        .u64_observable_counter("ursula.raft.unidentified_peer_refusals")
        .with_description(
            "Raft RPCs refused since start because they named no cluster id: a node before \
             format epoch 4, or a stray client",
        )
        .with_callback(move |observer| {
            observer.observe(registry.peer_cluster().unidentified_refusals(), &[]);
        })
        .build();
}

/// Register `ursula.wal.recovering`: 1 while this node's Raft logs may be
/// missing entries it acknowledged (see `ursula_raft::RecoveryState`).
pub(crate) fn register_wal_recovery(recovery: ursula_raft::RecoveryState) {
    let (recovering, reason) = match recovery {
        ursula_raft::RecoveryState::Normal => (0, "none"),
        ursula_raft::RecoveryState::Recovering { reason } => (1, match reason {
            ursula_raft::wal::diagnostics::RecoveryReason::HostCrash => "host_crash",
            ursula_raft::wal::diagnostics::RecoveryReason::Poisoned => "poisoned",
            ursula_raft::wal::diagnostics::RecoveryReason::UnknownHistory => "unknown_history",
        }),
    };
    let _ = global::meter("ursula-raft")
        .u64_observable_gauge("ursula.wal.recovering")
        .with_description("1 while this node's Raft logs may be missing acknowledged entries")
        .with_callback(move |observer| {
            observer.observe(recovering, &[KeyValue::new("reason", reason)]);
        })
        .build();
}

/// Register `ursula.raft.recovery_gates`: the groups whose recovery gate on
/// this node is closed, by status. `stalled` groups see no leader that could
/// confirm a barrier and wait for an operator. Also register
/// `ursula.raft.follower_log_reversions`: by follower, how often one lost
/// entries it had acknowledged to a leader on this node.
pub(crate) fn register_recovery_gates(registry: ursula_raft::RaftGroupHandleRegistry) {
    let reversion_registry = registry.clone();
    let _ = global::meter("ursula-raft")
        .u64_observable_counter("ursula.raft.follower_log_reversions")
        .with_description(
            "Times a follower lost Raft log entries it had acknowledged to a leader on this node, \
             by follower; the leader rewinds it in place. Expected after a host crash under \
             raft.wal.fsync = never or a lost disk, otherwise check the follower's disk",
        )
        .with_callback(move |observer| {
            for (target, count) in reversion_registry.follower_log_reversions() {
                observer.observe(count, &[KeyValue::new(
                    "target",
                    i64::try_from(target).unwrap_or(i64::MAX),
                )]);
            }
        })
        .build();
    let apply_registry = registry.clone();
    let _ = global::meter("ursula-raft")
        .u64_observable_gauge("ursula.raft.apply_stopped")
        .with_description("1 for a group stopped by committed application failure; retain its WAL and repair the binary")
        .with_callback(move |observer| {
            for group in apply_registry.metrics_snapshot() {
                observer.observe(u64::from(group.apply_failure.is_some()), &[KeyValue::new("group_id", i64::from(group.raft_group_id))]);
            }
        }).build();

    let _ = global::meter("ursula-raft")
        .u64_observable_gauge("ursula.raft.recovery_gates")
        .with_description(
            "Raft groups whose recovery gate on this node is closed, by status; stalled groups \
             wait for an operator to accept the loss of the unsynced tail",
        )
        .with_callback(move |observer| {
            let gates = registry.recovery_gates();
            for (status, label) in [
                (
                    ursula_raft::RecoveryGateStatus::AwaitingBarrier,
                    "awaiting_barrier",
                ),
                (ursula_raft::RecoveryGateStatus::CatchingUp, "catching_up"),
                (ursula_raft::RecoveryGateStatus::Stalled, "stalled"),
            ] {
                let count = gates.values().filter(|gate| **gate == status).count();
                observer.observe(u64::try_from(count).unwrap_or(u64::MAX), &[KeyValue::new(
                    "status", label,
                )]);
            }
        })
        .build();
}

/// Register `ursula.snapshot_parts.staged_bytes`: the bytes of staged,
/// unused snapshot parts in the streams this node leads and counts. Sum it
/// across nodes for a cluster's total.
pub(crate) fn register_staged_snapshot_parts(state: crate::HttpState) {
    let _ = global::meter("ursula")
        .u64_observable_gauge("ursula.snapshot_parts.staged_bytes")
        .with_description(
            "Bytes of staged, unused snapshot parts in the streams this node leads and counts",
        )
        .with_callback(move |observer| {
            let bytes = state.staged_part_bytes_total();
            observer.observe(bytes, &[]);
        })
        .build();
}

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
