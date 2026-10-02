//! Active/standby failover over the keyed-state indexer pods.
//!
//! The node's keyed-state proxy ([`crate::keyed_state`]) sends every read to
//! the first healthy pod of an ordered list: the primary, then the standbys
//! (`keyed_state.indexer_urls`, or the single `server.keyed_state_upstream`).
//! This is availability, not load sharding: one pod takes all the traffic
//! while it is healthy.
//!
//! An attempt fails on a connection error, on no response headers within
//! the read's wait plus `failover_header_timeout`, on a body that breaks off
//! or does not finish within another `failover_header_timeout`, or on a
//! 502/503/504 answer (a non-final attempt fails over right after such
//! headers, without reading the body). Only the last pod tried gets the rest
//! of the budget, so a pod that stalls after its headers cannot use up the
//! time of the pods after it. The request then moves on to the next pod
//! within its own budget (the read's remaining `timeout_ms` is passed on),
//! and the failed pod is marked unhealthy for `unhealthy_backoff`. Healthy
//! pods are tried first, in order, then unhealthy ones in order, so a
//! request never fails while some pod can answer. A background prober (one
//! per upstream, started by the first request) sends `GET /readyz` to every
//! unhealthy pod whose backoff has elapsed; success makes it healthy again,
//! which returns traffic to the primary.
//!
//! Correctness does not depend on routing: indexer publications are CAS
//! guarded, and a pod serves only published state, revalidating `CURRENT`
//! when its view is older than its revalidation bound (1 s), which the
//! minimum backoff exceeds.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use futures_util::future::join_all;
use tokio::time::Instant;

/// How often the prober looks for unhealthy pods whose backoff elapsed.
const PROBE_TICK: Duration = Duration::from_millis(200);
/// Upper bound of one `/readyz` probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Failover timing (`[keyed_state]` in the node config).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailoverOptions {
    /// TCP connect timeout of one attempt.
    pub connect_timeout: Duration,
    /// Response-header deadline of a non-final attempt, beyond the wait.
    pub header_timeout: Duration,
    /// How long a failed pod stays out of the order before it is probed.
    pub unhealthy_backoff: Duration,
}

impl Default for FailoverOptions {
    fn default() -> Self {
        Self::from_config(&ursula_config::KeyedStateConfig::default())
    }
}

impl FailoverOptions {
    pub fn from_config(config: &ursula_config::KeyedStateConfig) -> Self {
        Self {
            connect_timeout: config.upstream_connect_timeout.as_duration(),
            header_timeout: config.failover_header_timeout.as_duration(),
            unhealthy_backoff: config.unhealthy_backoff.as_duration(),
        }
    }
}

#[derive(Debug)]
struct Pod {
    base: url::Url,
    /// `None` while healthy; otherwise when the pod may be probed again.
    retry_at: Mutex<Option<Instant>>,
    requests: AtomicU64,
    failures: AtomicU64,
}

impl Pod {
    fn retry_at(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        self.retry_at.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn healthy(&self) -> bool {
        self.retry_at().is_none()
    }
}

/// One upstream answer, body read in full.
#[derive(Debug)]
pub(crate) struct UpstreamAnswer {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

/// The configured keyed-state indexer pods, in failover order.
#[derive(Debug)]
pub struct KeyedStateUpstream {
    pods: Vec<Pod>,
    client: reqwest::Client,
    options: FailoverOptions,
    failovers: AtomicU64,
    prober_started: AtomicBool,
}

/// `keyed_state_upstream` in `/__ursula/metrics`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct UpstreamSnapshot {
    /// Index of the pod that takes new reads: the first healthy one.
    pub(crate) active_pod: Option<usize>,
    pub(crate) active_url: Option<String>,
    /// Attempts that moved on to the next pod after a failure.
    pub(crate) failovers: u64,
    pub(crate) unhealthy_pods: u64,
    pub(crate) pods: Vec<PodSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PodSnapshot {
    pub(crate) url: String,
    pub(crate) healthy: bool,
    /// Attempts sent to this pod.
    pub(crate) requests: u64,
    /// Attempts that failed over away from this pod.
    pub(crate) failures: u64,
}

impl KeyedStateUpstream {
    /// One pod with the default failover timing.
    pub fn new(base_url: &str) -> Result<Self, String> {
        Self::with_pods([base_url], FailoverOptions::default())
    }

    /// Pods in failover order: the primary first. Each is an absolute
    /// `http` or `https` base URL; a path prefix is kept.
    pub fn with_pods<I, S>(urls: I, options: FailoverOptions) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut pods = Vec::new();
        for raw in urls {
            let raw = raw.as_ref();
            let base = url::Url::parse(raw)
                .map_err(|err| format!("invalid keyed-state upstream {raw:?}: {err}"))?;
            if !matches!(base.scheme(), "http" | "https") || base.cannot_be_a_base() {
                return Err(format!(
                    "keyed-state upstream {raw:?} must be an http or https base URL"
                ));
            }
            pods.push(Pod {
                base,
                retry_at: Mutex::new(None),
                requests: AtomicU64::new(0),
                failures: AtomicU64::new(0),
            });
        }
        if pods.is_empty() {
            return Err("keyed-state upstream needs at least one indexer URL".to_owned());
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(options.connect_timeout)
            .build()
            .map_err(|err| format!("build keyed-state upstream client: {err}"))?;
        Ok(Self {
            pods,
            client,
            options,
            failovers: AtomicU64::new(0),
            prober_started: AtomicBool::new(false),
        })
    }

    /// The configured base URLs, in failover order.
    pub fn pod_urls(&self) -> impl Iterator<Item = &url::Url> {
        self.pods.iter().map(|pod| &pod.base)
    }

    pub(crate) fn snapshot(&self) -> UpstreamSnapshot {
        let pods = self
            .pods
            .iter()
            .map(|pod| PodSnapshot {
                url: pod.base.to_string(),
                healthy: pod.healthy(),
                requests: pod.requests.load(Ordering::Relaxed),
                failures: pod.failures.load(Ordering::Relaxed),
            })
            .collect::<Vec<_>>();
        let active_pod = pods.iter().position(|pod| pod.healthy);
        UpstreamSnapshot {
            active_pod,
            active_url: active_pod
                .and_then(|index| pods.get(index))
                .map(|pod| pod.url.clone()),
            failovers: self.failovers.load(Ordering::Relaxed),
            unhealthy_pods: pods.iter().filter(|pod| !pod.healthy).count() as u64,
            pods,
        }
    }

    /// Pod indices in attempt order: healthy pods in configured order, then
    /// unhealthy ones in configured order (a last resort, never skipped).
    fn attempt_order(&self) -> Vec<usize> {
        let healthy = self.pods.iter().map(Pod::healthy).collect::<Vec<_>>();
        let first = healthy
            .iter()
            .enumerate()
            .filter(|(_, healthy)| **healthy)
            .map(|(index, _)| index);
        let rest = healthy
            .iter()
            .enumerate()
            .filter(|(_, healthy)| !**healthy)
            .map(|(index, _)| index);
        first.chain(rest).collect()
    }

    fn mark_unhealthy(&self, index: usize, reason: &str) {
        let Some(pod) = self.pods.get(index) else {
            return;
        };
        pod.failures.fetch_add(1, Ordering::Relaxed);
        let was_healthy = {
            let mut retry_at = pod.retry_at();
            let was_healthy = retry_at.is_none();
            *retry_at = Some(Instant::now() + self.options.unhealthy_backoff);
            was_healthy
        };
        if was_healthy {
            tracing::warn!(
                pod = %pod.base,
                reason,
                backoff_ms = self.options.unhealthy_backoff.as_millis() as u64,
                "keyed-state indexer pod marked unhealthy"
            );
        }
    }

    fn mark_healthy(&self, index: usize) {
        let Some(pod) = self.pods.get(index) else {
            return;
        };
        if pod.retry_at().take().is_some() {
            tracing::info!(pod = %pod.base, "keyed-state indexer pod is healthy again");
        }
    }

    /// Sends one GET through the failover order. `build` makes the request
    /// URL for a pod base and the read's remaining wait in milliseconds
    /// (`None` for a read that does not wait). Returns the first answer that
    /// is not a failover status, else the last 502/503/504 answer seen, else
    /// `None` (no pod answered).
    pub(crate) async fn get<F>(
        self: &Arc<Self>,
        wait_ms: Option<u64>,
        base_budget: Duration,
        build: F,
    ) -> Option<UpstreamAnswer>
    where
        F: Fn(&url::Url, Option<u64>) -> Option<url::Url>,
    {
        self.ensure_prober();
        let started = Instant::now();
        let wait = Duration::from_millis(wait_ms.unwrap_or_default());
        let budget = base_budget.saturating_add(wait);
        let order = self.attempt_order();
        let mut last_answer = None;
        for (attempt, index) in order.iter().copied().enumerate() {
            let Some(pod) = self.pods.get(index) else {
                continue;
            };
            let elapsed = started.elapsed();
            let remaining = budget.saturating_sub(elapsed);
            if remaining.is_zero() {
                break;
            }
            if attempt > 0 {
                self.failovers.fetch_add(1, Ordering::Relaxed);
            }
            // The read's own wait shrinks by what earlier attempts used.
            let attempt_wait_ms = wait_ms.map(|ms| {
                ms.saturating_sub(elapsed.as_millis().try_into().unwrap_or(u64::MAX))
                    .max(1)
            });
            let Some(url) = build(&pod.base, attempt_wait_ms) else {
                tracing::warn!(pod = %pod.base, "keyed-state upstream URL cannot carry a path");
                continue;
            };
            let final_attempt = attempt + 1 == order.len();
            let (header_timeout, total_timeout) = if final_attempt {
                (remaining, remaining)
            } else {
                let header_timeout = Duration::from_millis(attempt_wait_ms.unwrap_or_default())
                    .saturating_add(self.options.header_timeout)
                    .min(remaining);
                // Headers plus a body allowance: a pod that stalls after
                // its headers leaves the rest of the budget to later pods.
                let total_timeout = header_timeout
                    .saturating_add(self.options.header_timeout)
                    .min(remaining);
                (header_timeout, total_timeout)
            };
            pod.requests.fetch_add(1, Ordering::Relaxed);
            match self
                .attempt(url, header_timeout, total_timeout, !final_attempt)
                .await
            {
                Ok(answer) if is_failover_status(answer.status) => {
                    self.mark_unhealthy(index, answer.status.as_str());
                    last_answer = Some(answer);
                }
                Ok(answer) => {
                    self.mark_healthy(index);
                    return Some(answer);
                }
                Err(reason) => {
                    tracing::warn!(pod = %pod.base, %reason, "keyed-state upstream request failed");
                    self.mark_unhealthy(index, &reason);
                }
            }
        }
        last_answer
    }

    /// One request to one pod: headers within `header_timeout`, the whole
    /// answer within `total_timeout`. With `skip_failover_body`, a
    /// 502/503/504 answer returns right after its headers with an empty body
    /// (the request fails over; a slow error body must not delay it).
    async fn attempt(
        &self,
        url: url::Url,
        header_timeout: Duration,
        total_timeout: Duration,
        skip_failover_body: bool,
    ) -> Result<UpstreamAnswer, String> {
        let started = Instant::now();
        let send = self.client.get(url).timeout(total_timeout).send();
        let response = match tokio::time::timeout(header_timeout, send).await {
            Err(_) => return Err("no response headers in time".to_owned()),
            Ok(Err(err)) => return Err(err.to_string()),
            Ok(Ok(response)) => response,
        };
        let status = response.status();
        let headers = response.headers().clone();
        if skip_failover_body && is_failover_status(status) {
            return Ok(UpstreamAnswer {
                status,
                headers,
                body: Bytes::new(),
            });
        }
        let body_timeout = total_timeout.saturating_sub(started.elapsed());
        let body = match tokio::time::timeout(body_timeout, response.bytes()).await {
            Err(_) => return Err("response body not complete in time".to_owned()),
            Ok(Err(err)) => return Err(format!("response body failed: {err}")),
            Ok(Ok(body)) => body,
        };
        Ok(UpstreamAnswer {
            status,
            headers,
            body,
        })
    }

    /// Starts the prober on the current tokio runtime once; it ends when the
    /// upstream is dropped.
    fn ensure_prober(self: &Arc<Self>) {
        if self.prober_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.prober_started.store(false, Ordering::Release);
            return;
        };
        let weak = Arc::downgrade(self);
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(PROBE_TICK).await;
                let Some(upstream) = weak.upgrade() else {
                    break;
                };
                upstream.probe_due().await;
            }
        });
    }

    /// Probes `GET {base}/readyz` of every unhealthy pod whose backoff has
    /// elapsed; a 200 makes it healthy, anything else restarts its backoff.
    async fn probe_due(&self) {
        let now = Instant::now();
        let due = self
            .pods
            .iter()
            .enumerate()
            .filter(|(_, pod)| pod.retry_at().is_some_and(|retry_at| retry_at <= now))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let probes = due.into_iter().filter_map(|index| {
            let pod = self.pods.get(index)?;
            let mut url = pod.base.clone();
            url.path_segments_mut().ok()?.pop_if_empty().push("readyz");
            let request = self
                .client
                .get(url)
                .timeout(PROBE_TIMEOUT.max(self.options.connect_timeout))
                .send();
            Some(async move {
                let ready =
                    matches!(request.await, Ok(response) if response.status() == StatusCode::OK);
                (index, ready)
            })
        });
        for (index, ready) in join_all(probes).await {
            if ready {
                self.mark_healthy(index);
            } else if let Some(pod) = self.pods.get(index) {
                *pod.retry_at() = Some(Instant::now() + self.options.unhealthy_backoff);
            }
        }
    }
}

/// Answers that move a request on to the next pod.
fn is_failover_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(urls: &[&str]) -> KeyedStateUpstream {
        KeyedStateUpstream::with_pods(urls.iter().copied(), FailoverOptions::default())
            .expect("upstream")
    }

    #[test]
    fn rejects_bad_or_missing_urls() {
        assert!(KeyedStateUpstream::new("ftp://indexer").is_err());
        assert!(KeyedStateUpstream::new("not a url").is_err());
        assert!(
            KeyedStateUpstream::with_pods(Vec::<String>::new(), FailoverOptions::default())
                .is_err()
        );
    }

    #[test]
    fn healthy_pods_come_first_in_configured_order() {
        let upstream = upstream(&["http://a", "http://b", "http://c"]);
        assert_eq!(upstream.attempt_order(), vec![0, 1, 2]);
        upstream.mark_unhealthy(0, "test");
        assert_eq!(upstream.attempt_order(), vec![1, 2, 0]);
        upstream.mark_unhealthy(2, "test");
        assert_eq!(upstream.attempt_order(), vec![1, 0, 2]);
        let snapshot = upstream.snapshot();
        assert_eq!(snapshot.active_pod, Some(1));
        assert_eq!(snapshot.active_url.as_deref(), Some("http://b/"));
        assert_eq!(snapshot.unhealthy_pods, 2);
        upstream.mark_healthy(0);
        assert_eq!(upstream.attempt_order(), vec![0, 1, 2]);
        assert_eq!(upstream.snapshot().active_pod, Some(0));
    }
}
