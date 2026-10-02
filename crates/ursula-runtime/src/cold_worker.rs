//! Cold-tier background workers.
//!
//! Started by the bootstrap layer after the runtime is constructed.

use ursula_stream::ColdFlushPressure;

use crate::PlanGroupColdFlushRequest;
use crate::ShardRuntime;

/// Node-level flush pressure for one pass (bounded-stream-state F10): once
/// hot bytes across the locally led groups reach the watermark, every group
/// drains, largest streams first, its share of the excess over three quarters
/// of the watermark. Lowering a per-group threshold instead would leave
/// pressure a no-op for groups far below `flush_size`, the common case.
fn pass_pressure(observed_hot_bytes: u64, pressure_hot_bytes: u64) -> Option<ColdFlushPressure> {
    (pressure_hot_bytes > 0 && observed_hot_bytes >= pressure_hot_bytes).then(|| {
        ColdFlushPressure {
            node_hot_bytes: observed_hot_bytes,
            node_target_bytes: pressure_hot_bytes / 4 * 3,
        }
    })
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
            let pressure = pass_pressure(observed_hot_bytes, pressure_hot_bytes);
            let pressure_active = pressure.is_some();
            match runtime
                .flush_cold_all_groups_once_bounded(
                    PlanGroupColdFlushRequest {
                        min_hot_bytes,
                        max_flush_bytes,
                        max_batch_bytes: max_flush_bytes,
                        pressure,
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
    use ursula_stream::ColdFlushPressure;

    use super::pass_pressure;

    #[test]
    fn pressure_flush_activates_at_the_aggregate_watermark() {
        assert_eq!(pass_pressure(127, 128), None);
        assert_eq!(
            pass_pressure(128, 128),
            Some(ColdFlushPressure {
                node_hot_bytes: 128,
                node_target_bytes: 96,
            })
        );
        assert_eq!(
            pass_pressure(129, 128).map(|pressure| pressure.node_target_bytes),
            Some(96)
        );
    }

    #[test]
    fn zero_pressure_watermark_disables_the_fallback() {
        assert_eq!(pass_pressure(u64::MAX, 0), None);
    }
}
