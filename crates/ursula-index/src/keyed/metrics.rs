//! Keyed engine metrics (design §6.1 U24, §9.4).
//!
//! Counters are lock-free atomics bumped on the engine's slow paths
//! (publication, compaction, GC); gauges are computed when a snapshot is
//! taken. Object-store requests are counted by S3 request class for the
//! whole pod and for each namespace held in memory, so the per-harness
//! budget of §9.4 can be checked from the per-namespace counters.
//!
//! The snapshot is served as JSON on `/__ursula/indexer/metrics`, next to
//! the node's `/__ursula/metrics` and the gateway's
//! `/__ursula/gateway/metrics`.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use serde::Serialize;

use super::admission::AdmissionMetrics;
use crate::object_store::ObjectRequestCounters;
use crate::object_store::ObjectRequestCounts;

/// Path of the indexer's metrics snapshot.
pub const INDEXER_METRICS_PATH: &str = "/__ursula/indexer/metrics";

/// The engine's counters.
#[derive(Debug, Default)]
pub(crate) struct KeyedMetrics {
    pub(crate) publishes: AtomicU64,
    pub(crate) compaction_publishes: AtomicU64,
    pub(crate) cas_conflicts: AtomicU64,
    pub(crate) compaction_input_bytes: AtomicU64,
    pub(crate) compaction_output_bytes: AtomicU64,
    pub(crate) gc_deleted: AtomicU64,
    pub(crate) reused_settled: AtomicU64,
    pub(crate) current_revalidations: AtomicU64,
    pub(crate) work_deadlines: AtomicU64,
    /// Every object-store request of the pod.
    pub(crate) requests: Arc<ObjectRequestCounters>,
}

pub(crate) fn bump(counter: &AtomicU64, by: u64) {
    counter.fetch_add(by, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// Object-store requests with their S3 request classes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct S3Requests {
    /// Requests by operation.
    #[serde(flatten)]
    pub counts: ObjectRequestCounts,
    /// PUT-class requests (PUT and LIST).
    pub put_class: u64,
    /// GET-class requests (GET and HEAD).
    pub get_class: u64,
}

impl From<ObjectRequestCounts> for S3Requests {
    fn from(counts: ObjectRequestCounts) -> Self {
        Self {
            put_class: counts.put_class(),
            get_class: counts.get_class(),
            counts,
        }
    }
}

/// One namespace held in memory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NamespaceMetrics {
    /// Bucket of the stream.
    pub bucket: String,
    /// The stream's local name.
    pub key: String,
    /// The stream incarnation.
    pub incarnation: u64,
    /// Published `D`.
    pub through_record: u64,
    /// The highest source tail `N` seen in a request.
    pub source_next: u64,
    /// `N − D`.
    pub lag_records: u64,
    /// Runs of the published manifest.
    pub runs: usize,
    /// Generation of the published manifest.
    pub generation: u64,
    /// Object-store requests issued for this namespace by this pod.
    pub s3_requests: S3Requests,
}

/// A snapshot of the engine's metrics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct KeyedMetricsSnapshot {
    /// Successful `CURRENT` publications (ingest, rebuild and compaction).
    pub publishes: u64,
    /// The compaction share of `publishes`.
    pub compaction_publishes: u64,
    /// Publications that lost the `CURRENT` compare-and-swap.
    pub cas_conflicts: u64,
    /// Bytes of input runs merged by compactions.
    pub compaction_input_bytes: u64,
    /// Bytes of parts written by compactions.
    pub compaction_output_bytes: u64,
    /// Objects deleted by garbage collection.
    pub gc_deleted: u64,
    /// Parts found already present (not created by this pod recently) and
    /// settled before being referenced: cross-pod reuse.
    pub reused_settled: u64,
    /// Objects waiting in the garbage-collection queue.
    pub gc_backlog: usize,
    /// `CURRENT` revalidations of reads without `min_through_record`.
    pub current_revalidations: u64,
    /// Ingest cycles and compactions dropped at the per-work deadline.
    pub work_deadlines: u64,
    /// Reads waiting for `min_through_record`.
    pub waiters: usize,
    /// Namespaces held in memory.
    pub namespaces: usize,
    /// Sum of `N − D` over the namespaces held.
    pub lag_records_total: u64,
    /// Largest `N − D` of a namespace held.
    pub lag_records_max: u64,
    /// Sum of published runs over the namespaces held.
    pub runs_total: usize,
    /// Largest run count of a namespace held.
    pub runs_max: usize,
    /// Every object-store request of the pod.
    pub s3_requests: S3Requests,
    /// The process-wide admission controller: budget in use, queue depth,
    /// rejections.
    pub admission: AdmissionMetrics,
    /// Per-namespace detail, largest lag first.
    pub namespace_detail: Vec<NamespaceMetrics>,
}

impl KeyedMetrics {
    pub(crate) fn snapshot(
        &self,
        waiters: usize,
        gc_backlog: usize,
        admission: AdmissionMetrics,
        mut detail: Vec<NamespaceMetrics>,
    ) -> KeyedMetricsSnapshot {
        detail.sort_by(|left, right| {
            right
                .lag_records
                .cmp(&left.lag_records)
                .then_with(|| left.bucket.cmp(&right.bucket))
                .then_with(|| left.key.cmp(&right.key))
                .then_with(|| left.incarnation.cmp(&right.incarnation))
        });
        KeyedMetricsSnapshot {
            publishes: load(&self.publishes),
            compaction_publishes: load(&self.compaction_publishes),
            cas_conflicts: load(&self.cas_conflicts),
            compaction_input_bytes: load(&self.compaction_input_bytes),
            compaction_output_bytes: load(&self.compaction_output_bytes),
            gc_deleted: load(&self.gc_deleted),
            reused_settled: load(&self.reused_settled),
            gc_backlog,
            current_revalidations: load(&self.current_revalidations),
            work_deadlines: load(&self.work_deadlines),
            waiters,
            namespaces: detail.len(),
            lag_records_total: detail
                .iter()
                .map(|namespace| namespace.lag_records)
                .fold(0, u64::saturating_add),
            lag_records_max: detail
                .iter()
                .map(|namespace| namespace.lag_records)
                .max()
                .unwrap_or(0),
            runs_total: detail
                .iter()
                .map(|namespace| namespace.runs)
                .fold(0, usize::saturating_add),
            runs_max: detail
                .iter()
                .map(|namespace| namespace.runs)
                .max()
                .unwrap_or(0),
            s3_requests: self.requests.snapshot().into(),
            admission,
            namespace_detail: detail,
        }
    }
}
