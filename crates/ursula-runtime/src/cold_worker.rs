//! Cold-tier background workers.
//!
//! Started by the bootstrap layer after the runtime is constructed.

use crate::PlanGroupColdFlushRequest;
use crate::ShardRuntime;

/// Start the periodic cold-flush worker if the configured interval is non-zero.
pub fn spawn_cold_flush_worker_if_configured(
    runtime: &ShardRuntime,
    config: &ursula_config::ColdConfig,
) {
    let interval = config.flush_interval.as_duration();
    if interval.is_zero() {
        return;
    }
    let min_hot_bytes = usize::try_from(config.flush_min_hot_size().as_bytes())
        .expect("config validation ensures flush sizes fit usize");
    let max_flush_bytes = usize::try_from(config.flush_max_size().as_bytes())
        .expect("config validation ensures flush sizes fit usize");
    let max_concurrency = config.flush_max_concurrency.max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
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
pub fn spawn_cold_gc_worker_if_configured(
    runtime: &ShardRuntime,
    config: &ursula_config::ColdConfig,
) {
    let interval = config.gc_interval.as_duration();
    if interval.is_zero() {
        return;
    }
    let max_entries = config.gc_max_entries.max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        loop {
            if let Err(err) = runtime.run_cold_gc_all_groups_once(max_entries).await {
                tracing::error!("cold gc worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}
