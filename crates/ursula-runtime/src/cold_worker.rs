//! Cold-tier background workers.
//!
//! Started by the bootstrap layer after the runtime is constructed.

use std::time::Duration;

use crate::PlanGroupColdFlushRequest;
use crate::ShardRuntime;
use crate::cold_config::ColdWorkerConfig;

/// Start the periodic cold-flush worker if the configured interval is non-zero.
pub fn spawn_cold_flush_worker_if_configured(runtime: &ShardRuntime, config: &ColdWorkerConfig) {
    if config.flush_interval_ms == 0 {
        return;
    }
    let interval_ms = config.flush_interval_ms;
    let min_hot_bytes = config.flush_min_hot_bytes;
    let max_flush_bytes = config.flush_max_bytes;
    let max_concurrency = config.flush_max_concurrency.max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        loop {
            if let Err(err) = runtime
                .flush_cold_all_groups_once_bounded(
                    PlanGroupColdFlushRequest {
                        min_hot_bytes,
                        max_flush_bytes,
                    },
                    max_concurrency,
                )
                .await
            {
                tracing::error!("cold flush worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Start the periodic cold-gc worker if the configured interval is non-zero.
pub fn spawn_cold_gc_worker_if_configured(runtime: &ShardRuntime, config: &ColdWorkerConfig) {
    if config.gc_interval_ms == 0 {
        return;
    }
    let interval_ms = config.gc_interval_ms;
    let max_entries = config.gc_max_entries.max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        loop {
            if let Err(err) = runtime.run_cold_gc_all_groups_once(max_entries).await {
                tracing::error!("cold gc worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}
