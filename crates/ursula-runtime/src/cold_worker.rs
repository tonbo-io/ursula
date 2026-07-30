//! Cold-tier background workers.
//!
//! Started by the bootstrap layer after the runtime is constructed.

use crate::PlanGroupColdFlushRequest;
use crate::ShardRuntime;

fn effective_min_hot_bytes(
    normal_min_hot_bytes: usize,
    observed_hot_bytes: u64,
    pressure_hot_bytes: u64,
) -> (usize, bool) {
    let pressure_active = pressure_hot_bytes > 0 && observed_hot_bytes >= pressure_hot_bytes;
    (
        if pressure_active {
            1
        } else {
            normal_min_hot_bytes
        },
        pressure_active,
    )
}

/// Start the periodic same-stream cold chunk compactor when explicitly enabled.
pub fn spawn_cold_compaction_worker_if_configured(
    runtime: &ShardRuntime,
    config: &ursula_config::ColdConfig,
) {
    if !config.compaction_enabled {
        return;
    }
    let interval = config.compaction_interval.as_duration();
    let target_bytes = config.compaction_target_size.as_bytes();
    let max_bytes = config.compaction_max_size.as_bytes();
    let max_streams = config.compaction_max_streams_per_pass.max(1);
    let gc_grace_ms =
        u64::try_from(config.compaction_gc_grace.as_duration().as_millis()).unwrap_or(u64::MAX);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        loop {
            match runtime
                .compact_cold_once(target_bytes, max_bytes, max_streams, gc_grace_ms)
                .await
            {
                Ok(compacted) if compacted > 0 => {
                    tracing::info!(compacted, "cold chunk compaction pass completed");
                }
                Ok(_) => {}
                Err(err) => tracing::error!("cold compaction worker error: {err}"),
            }
            tokio::time::sleep(interval).await;
        }
    });
}

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
    let pressure_hot_bytes = config.flush_pressure_hot_size.as_bytes();
    let max_concurrency = config.flush_max_concurrency.max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        loop {
            let metrics = runtime.metrics();
            let observed_hot_bytes = metrics.inner.cold_hot_bytes();
            let (pass_min_hot_bytes, pressure_active) =
                effective_min_hot_bytes(min_hot_bytes, observed_hot_bytes, pressure_hot_bytes);
            match runtime
                .flush_cold_all_groups_once_bounded(
                    PlanGroupColdFlushRequest {
                        min_hot_bytes: pass_min_hot_bytes,
                        max_flush_bytes,
                        max_batch_bytes: max_flush_bytes,
                    },
                    max_concurrency,
                )
                .await
            {
                Ok(flushed) if pressure_active => {
                    metrics.inner.record_cold_pressure_flush(flushed);
                    if flushed > 0 {
                        tracing::info!(
                            observed_hot_bytes,
                            pressure_hot_bytes,
                            flushed,
                            "cold pressure flush pass completed"
                        );
                    }
                }
                Ok(_) => {}
                Err(err) => tracing::error!("cold flush worker error: {err}"),
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

#[cfg(test)]
mod tests {
    use super::effective_min_hot_bytes;

    #[test]
    fn pressure_flush_activates_at_the_aggregate_watermark() {
        assert_eq!(effective_min_hot_bytes(8, 127, 128), (8, false));
        assert_eq!(effective_min_hot_bytes(8, 128, 128), (1, true));
        assert_eq!(effective_min_hot_bytes(8, 129, 128), (1, true));
    }

    #[test]
    fn zero_pressure_watermark_disables_the_fallback() {
        assert_eq!(effective_min_hot_bytes(8, u64::MAX, 0), (8, false));
    }
}
