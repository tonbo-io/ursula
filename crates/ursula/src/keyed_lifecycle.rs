//! Node side of the keyed-state lifecycle (keyed-streams §3.8, U23, U24):
//! the bucket-purge drain fan-out to every keyed-state indexer pod, and the
//! keyed-state request counters exported in `/__ursula/metrics`.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::http::StatusCode;
use futures_util::future::join_all;

/// Path of the indexer's drain endpoint below each configured base URL.
pub(crate) const KEYED_DRAIN_PATH: &str = "/v1/keyed/drain";

/// Sends `drain(bucket)` to every configured keyed-state indexer pod.
#[derive(Clone, Debug)]
pub(crate) struct KeyedStateDrain {
    client: reqwest::Client,
    indexer_urls: Arc<[String]>,
    timeout: Duration,
}

impl Default for KeyedStateDrain {
    fn default() -> Self {
        Self::new(Vec::new(), Duration::from_secs(60))
    }
}

impl KeyedStateDrain {
    pub(crate) fn new(indexer_urls: Vec<String>, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            indexer_urls: indexer_urls.into(),
            timeout,
        }
    }

    pub(crate) fn from_config(config: &ursula_config::KeyedStateConfig) -> Self {
        Self::new(
            config.indexer_urls.clone(),
            config.drain_timeout.as_duration(),
        )
    }

    pub(crate) fn indexer_count(&self) -> usize {
        self.indexer_urls.len()
    }

    /// `POST {url}/v1/keyed/drain` with `{"bucket": bucket}` to every pod
    /// concurrently. Succeeds only when every pod answers 200: each pod has
    /// then blocked new work for the bucket and finished its in-flight
    /// operations, so erasing `.keyed/{bucket}/` cannot race a publish.
    /// With no pod configured there is nothing to drain.
    pub(crate) async fn drain_bucket(&self, bucket_id: &str) -> Result<(), String> {
        let body = serde_json::json!({ "bucket": bucket_id }).to_string();
        let requests = self.indexer_urls.iter().map(|base| {
            let url = format!("{}{KEYED_DRAIN_PATH}", base.trim_end_matches('/'));
            let request = self
                .client
                .post(&url)
                .timeout(self.timeout)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send();
            async move {
                match request.await {
                    Ok(response) if response.status() == reqwest::StatusCode::OK => Ok(()),
                    Ok(response) => Err(format!(
                        "keyed-state indexer {url} answered drain with {}",
                        response.status()
                    )),
                    Err(err) => Err(format!("keyed-state indexer {url} drain failed: {err}")),
                }
            }
        });
        let failures = join_all(requests)
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

/// Keyed-state proxy responses by status (U24), counted by every
/// `{stream}/keyed-state` handler.
#[derive(Debug, Default)]
pub(crate) struct KeyedStateRequestMetrics {
    ok: AtomicU64,
    no_content: AtomicU64,
    bad_request: AtomicU64,
    not_found: AtomicU64,
    internal_error: AtomicU64,
    unavailable: AtomicU64,
    other: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct KeyedStateRequestMetricsSnapshot {
    pub(crate) status_200: u64,
    pub(crate) status_204: u64,
    pub(crate) status_400: u64,
    pub(crate) status_404: u64,
    pub(crate) status_500: u64,
    pub(crate) status_503: u64,
    pub(crate) status_other: u64,
}

impl KeyedStateRequestMetrics {
    pub(crate) fn record(&self, status: StatusCode) {
        let counter = match status.as_u16() {
            200 => &self.ok,
            204 => &self.no_content,
            400 => &self.bad_request,
            404 => &self.not_found,
            500 => &self.internal_error,
            503 => &self.unavailable,
            _ => &self.other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> KeyedStateRequestMetricsSnapshot {
        KeyedStateRequestMetricsSnapshot {
            status_200: self.ok.load(Ordering::Relaxed),
            status_204: self.no_content.load(Ordering::Relaxed),
            status_400: self.bad_request.load(Ordering::Relaxed),
            status_404: self.not_found.load(Ordering::Relaxed),
            status_500: self.internal_error.load(Ordering::Relaxed),
            status_503: self.unavailable.load(Ordering::Relaxed),
            status_other: self.other.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_metrics_count_by_status() {
        let metrics = KeyedStateRequestMetrics::default();
        for status in [
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::NO_CONTENT,
            StatusCode::BAD_REQUEST,
            StatusCode::NOT_FOUND,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            metrics.record(status);
        }
        assert_eq!(metrics.snapshot(), KeyedStateRequestMetricsSnapshot {
            status_200: 2,
            status_204: 1,
            status_400: 1,
            status_404: 1,
            status_500: 1,
            status_503: 1,
            status_other: 1,
        });
    }

    #[tokio::test]
    async fn drain_without_indexers_is_a_no_op() {
        let drain = KeyedStateDrain::default();
        assert_eq!(drain.indexer_count(), 0);
        assert_eq!(drain.drain_bucket("bucket").await, Ok(()));
    }
}
