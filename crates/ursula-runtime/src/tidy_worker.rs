//! Leader-side `TidyStream` driver (bounded-stream-state F0).
//!
//! Streams that are idle after a feature-level raise keep their legacy
//! debt (uncollapsed message records, receipts beyond the window, unstamped
//! or idle producers) until something normalizes them. Every
//! [`TIDY_INTERVAL`], each group this node leads proposes `TidyStream` for
//! at most [`TIDY_MAX_STREAMS_PER_GROUP`] streams with debt, and repeats on
//! later passes until none remains. Each command does bounded work, so the
//! driver never stalls a group's apply. Below feature level 1 no stream has
//! debt and the driver proposes nothing.

use std::time::Duration;

use crate::ShardRuntime;

/// Pause between tidy passes.
pub const TIDY_INTERVAL: Duration = Duration::from_secs(60);
/// `TidyStream` commands one pass proposes per group.
pub const TIDY_MAX_STREAMS_PER_GROUP: usize = 64;

/// Start the periodic tidy driver.
pub fn spawn_tidy_worker(runtime: &ShardRuntime) {
    let runtime = runtime.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(TIDY_INTERVAL).await;
            match runtime
                .tidy_streams_all_groups_once(
                    TIDY_MAX_STREAMS_PER_GROUP,
                    crate::runtime::unix_time_ms(),
                )
                .await
            {
                Ok(report) if report.tidied > 0 => tracing::info!(
                    tidied = report.tidied,
                    debt_remaining = report.debt_remaining,
                    "tidy pass completed"
                ),
                Ok(_) => {}
                Err(err) => tracing::debug!("tidy pass incomplete: {err}"),
            }
        }
    });
}
