//! Gateway-side usage accounting (the metering half enforced at the edge).
//!
//! The gateway counts what only the gateway can see: admitted requests per
//! tenant and principal, request (ingress) bytes on accepted appends, and
//! actual response (egress) bytes including streamed bodies. Data-plane
//! truth — committed append bytes surviving retries, retained bytes, stream
//! counts — is accounted inside Ursula's replicated state and exported
//! separately; the two ledgers are complementary, not duplicates.
//!
//! Aggregation model: counters accumulate in memory keyed by
//! `(bucket, principal, action class)`. A background exporter drains them on
//! an interval into cursor-bearing [`UsageBatch`]es and hands them to a
//! [`UsageSink`]. A failing sink delays reporting — batches queue and merge —
//! but never blocks request handling and never drops acknowledged counts.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;

/// Highest number of pending batches kept while a sink is unavailable.
/// Beyond this the two oldest batches merge, so memory stays bounded while
/// counts are still never dropped.
const MAX_PENDING_BATCHES: usize = 64;

/// Coarse action classes for billing; finer distinctions stay in traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageClass {
    Append,
    Read,
    LiveRead,
    Admin,
}

/// Aggregation key. The optional principal is the verified `(issuer,
/// subject)` pair — present so a hosted consumer can classify service
/// traffic apart from tenant traffic; cardinality per bucket is bounded by
/// that bucket's principals.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UsageKey {
    pub bucket_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<PrincipalRef>,
    pub class: UsageClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct PrincipalRef {
    pub issuer: String,
    pub subject: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct UsageCounters {
    pub requests: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UsageRecord {
    #[serde(flatten)]
    pub key: UsageKey,
    #[serde(flatten)]
    pub counters: UsageCounters,
}

/// One export unit. `sequence` increases by one per drained window, so a
/// consumer can deduplicate replays and detect gaps; merged batches keep the
/// earliest sequence and widest window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UsageBatch {
    pub sequence: u64,
    pub window_start_unix_ms: u64,
    pub window_end_unix_ms: u64,
    pub records: Vec<UsageRecord>,
}

pub type UsageSinkFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), UsageSinkError>> + Send + 'a>>;

/// Receives drained batches. Implementations must be idempotent on
/// `sequence` because a crash between export and acknowledgement replays the
/// batch.
pub trait UsageSink: Send + Sync {
    fn export<'a>(&'a self, batch: &'a UsageBatch) -> UsageSinkFuture<'a>;
}

#[derive(Debug, thiserror::Error)]
#[error("usage sink unavailable: {0}")]
pub struct UsageSinkError(pub String);

/// Appends each batch as one JSON line. The self-hosted default: cheap,
/// greppable, and a workable ingestion point for an external biller.
pub struct JsonlUsageSink {
    file: tokio::sync::Mutex<std::fs::File>,
}

impl JsonlUsageSink {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: tokio::sync::Mutex::new(file),
        })
    }
}

impl UsageSink for JsonlUsageSink {
    fn export<'a>(&'a self, batch: &'a UsageBatch) -> UsageSinkFuture<'a> {
        Box::pin(async move {
            use std::io::Write;
            let line =
                serde_json::to_string(batch).map_err(|error| UsageSinkError(error.to_string()))?;
            let mut file = self.file.lock().await;
            writeln!(file, "{line}").map_err(|error| UsageSinkError(error.to_string()))?;
            file.flush()
                .map_err(|error| UsageSinkError(error.to_string()))
        })
    }
}

#[derive(Default)]
struct CollectorState {
    counters: HashMap<UsageKey, UsageCounters>,
    window_start_unix_ms: Option<u64>,
    next_sequence: u64,
    pending: VecDeque<UsageBatch>,
}

/// In-memory accumulation shared between the request path and the exporter.
#[derive(Default)]
pub struct UsageCollector {
    state: Mutex<CollectorState>,
}

impl UsageCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CollectorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn record(&self, key: UsageKey, requests: u64, request_bytes: u64, response_bytes: u64) {
        let now = unix_time_ms();
        let mut state = self.lock();
        state.window_start_unix_ms.get_or_insert(now);
        let counters = state.counters.entry(key).or_default();
        counters.requests = counters.requests.saturating_add(requests);
        counters.request_bytes = counters.request_bytes.saturating_add(request_bytes);
        counters.response_bytes = counters.response_bytes.saturating_add(response_bytes);
    }

    /// Moves the current window into the pending queue. Returns whether any
    /// batches are pending export.
    fn rotate(&self) -> bool {
        let now = unix_time_ms();
        let mut state = self.lock();
        if !state.counters.is_empty() {
            let records = state
                .counters
                .drain()
                .map(|(key, counters)| UsageRecord { key, counters })
                .collect::<Vec<_>>();
            let window_start = state.window_start_unix_ms.take().unwrap_or(now);
            let sequence = state.next_sequence;
            state.next_sequence = state.next_sequence.saturating_add(1);
            state.pending.push_back(UsageBatch {
                sequence,
                window_start_unix_ms: window_start,
                window_end_unix_ms: now,
                records,
            });
            while state.pending.len() > MAX_PENDING_BATCHES {
                merge_oldest(&mut state.pending);
            }
        }
        !state.pending.is_empty()
    }

    fn peek_pending(&self) -> Option<UsageBatch> {
        self.lock().pending.front().cloned()
    }

    fn acknowledge(&self, sequence: u64) {
        let mut state = self.lock();
        if state
            .pending
            .front()
            .is_some_and(|batch| batch.sequence == sequence)
        {
            state.pending.pop_front();
        }
    }
}

fn merge_oldest(pending: &mut VecDeque<UsageBatch>) {
    let Some(oldest) = pending.pop_front() else {
        return;
    };
    let Some(next) = pending.front_mut() else {
        pending.push_front(oldest);
        return;
    };
    next.sequence = oldest.sequence;
    next.window_start_unix_ms = oldest.window_start_unix_ms;
    let mut merged: HashMap<UsageKey, UsageCounters> = HashMap::new();
    for record in oldest.records.into_iter().chain(next.records.drain(..)) {
        let counters = merged.entry(record.key).or_default();
        counters.requests = counters.requests.saturating_add(record.counters.requests);
        counters.request_bytes = counters
            .request_bytes
            .saturating_add(record.counters.request_bytes);
        counters.response_bytes = counters
            .response_bytes
            .saturating_add(record.counters.response_bytes);
    }
    next.records = merged
        .into_iter()
        .map(|(key, counters)| UsageRecord { key, counters })
        .collect();
}

/// Drains the collector on an interval. A sink failure leaves the batch at
/// the head of the queue for the next tick.
pub async fn run_exporter(
    collector: Arc<UsageCollector>,
    sink: Arc<dyn UsageSink>,
    flush_interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut shutdown = shutdown;
    let mut ticker = tokio::time::interval(flush_interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }
        flush(&collector, sink.as_ref()).await;
    }
    // Final drain so a clean shutdown does not strand the last window.
    flush(&collector, sink.as_ref()).await;
}

pub(crate) async fn flush(collector: &UsageCollector, sink: &dyn UsageSink) {
    if !collector.rotate() {
        return;
    }
    while let Some(batch) = collector.peek_pending() {
        match sink.export(&batch).await {
            Ok(()) => collector.acknowledge(batch.sequence),
            Err(error) => {
                tracing::warn!(
                    sequence = batch.sequence,
                    error = %error,
                    "usage export failed; batch retained for retry"
                );
                break;
            }
        }
    }
}

fn unix_time_ms() -> u64 {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use super::*;

    fn key(bucket: &str, class: UsageClass) -> UsageKey {
        UsageKey {
            bucket_id: bucket.to_owned(),
            principal: None,
            class,
        }
    }

    struct FlakySink {
        healthy: AtomicBool,
        exported: Mutex<Vec<UsageBatch>>,
    }

    impl FlakySink {
        fn new(healthy: bool) -> Self {
            Self {
                healthy: AtomicBool::new(healthy),
                exported: Mutex::new(Vec::new()),
            }
        }
    }

    impl UsageSink for FlakySink {
        fn export<'a>(&'a self, batch: &'a UsageBatch) -> UsageSinkFuture<'a> {
            Box::pin(async move {
                if self.healthy.load(Ordering::Relaxed) {
                    self.exported
                        .lock()
                        .expect("exported lock")
                        .push(batch.clone());
                    Ok(())
                } else {
                    Err(UsageSinkError("down".to_owned()))
                }
            })
        }
    }

    #[tokio::test]
    async fn counters_aggregate_by_key_and_drain_into_sequenced_batches() {
        let collector = UsageCollector::new();
        collector.record(key("tenant-a", UsageClass::Append), 1, 100, 0);
        collector.record(key("tenant-a", UsageClass::Append), 1, 50, 0);
        collector.record(key("tenant-a", UsageClass::Read), 1, 0, 900);

        let sink = FlakySink::new(true);
        flush(&collector, &sink).await;

        let exported = sink.exported.lock().expect("exported lock");
        assert_eq!(exported.len(), 1);
        let batch = &exported[0];
        assert_eq!(batch.sequence, 0);
        assert_eq!(batch.records.len(), 2);
        let append = batch
            .records
            .iter()
            .find(|record| record.key.class == UsageClass::Append)
            .expect("append record");
        assert_eq!(append.counters.requests, 2);
        assert_eq!(append.counters.request_bytes, 150);
    }

    #[tokio::test]
    async fn failed_export_retains_counts_until_the_sink_recovers() {
        let collector = UsageCollector::new();
        let sink = FlakySink::new(false);

        collector.record(key("tenant-a", UsageClass::Append), 1, 10, 0);
        flush(&collector, &sink).await;
        assert!(sink.exported.lock().expect("lock").is_empty());

        collector.record(key("tenant-a", UsageClass::Append), 1, 20, 0);
        sink.healthy.store(true, Ordering::Relaxed);
        flush(&collector, &sink).await;

        let exported = sink.exported.lock().expect("lock");
        assert_eq!(exported.len(), 2, "both windows export after recovery");
        assert_eq!(exported[0].sequence, 0);
        assert_eq!(exported[1].sequence, 1);
        let total: u64 = exported
            .iter()
            .flat_map(|batch| &batch.records)
            .map(|record| record.counters.request_bytes)
            .sum();
        assert_eq!(total, 30, "no count is dropped across the outage");
    }

    #[tokio::test]
    async fn overflow_merges_batches_without_losing_counts() {
        let collector = UsageCollector::new();
        for _ in 0..(MAX_PENDING_BATCHES + 8) {
            collector.record(key("tenant-a", UsageClass::Append), 1, 1, 0);
            collector.rotate();
        }
        let pending_len = collector.lock().pending.len();
        assert!(pending_len <= MAX_PENDING_BATCHES);

        let sink = FlakySink::new(true);
        flush(&collector, &sink).await;
        let exported = sink.exported.lock().expect("lock");
        let total: u64 = exported
            .iter()
            .flat_map(|batch| &batch.records)
            .map(|record| record.counters.requests)
            .sum();
        assert_eq!(total, (MAX_PENDING_BATCHES + 8) as u64);
    }

    #[tokio::test]
    async fn jsonl_sink_appends_one_line_per_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("usage.jsonl");
        let sink = JsonlUsageSink::create(&path).expect("create sink");
        let collector = UsageCollector::new();
        collector.record(key("tenant-a", UsageClass::Read), 1, 0, 42);
        flush(&collector, &sink).await;

        let contents = std::fs::read_to_string(&path).expect("read usage log");
        assert_eq!(contents.lines().count(), 1);
        assert!(contents.contains("\"tenant-a\""));
        assert!(contents.contains("\"response_bytes\":42"));
    }
}
