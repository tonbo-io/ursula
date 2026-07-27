//! Per-tenant admission control (the gateway half of quota enforcement).
//!
//! A [`QuotaProvider`] supplies [`TenantLimits`] for a bucket; the
//! [`Admission`] engine enforces them before a request is forwarded:
//!
//! - request rate, via a per-bucket token bucket (`429` + `Retry-After`);
//! - concurrent live reads (SSE / long-poll), released when the response
//!   body finishes streaming;
//! - per-tenant request body size (`413`), tightening the gateway-wide cap.
//!
//! Every limit is optional; an absent limit means unlimited, and a gateway
//! without an installed provider behaves exactly as before. Data-plane
//! quotas (stream count, retained bytes) are enforced inside Ursula, not
//! here — see the tracking issue for the split.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::time::Duration;
use tokio::time::Instant;

pub type QuotaFuture<'a> = Pin<Box<dyn Future<Output = TenantLimits> + Send + 'a>>;

/// Limits for one tenant bucket. `None` means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantLimits {
    /// Sustained request rate; burst capacity equals one second of rate.
    pub requests_per_sec: Option<u32>,
    /// Concurrent SSE / long-poll connections.
    pub max_concurrent_live_reads: Option<u32>,
    /// Per-request body bytes, applied under the gateway-wide cap.
    pub max_request_body_bytes: Option<u64>,
}

/// Supplies per-bucket limits. Implementations may read static
/// configuration or an external control plane; resolution must be cheap
/// because it sits on every admitted request.
pub trait QuotaProvider: Send + Sync {
    fn limits_for<'a>(&'a self, bucket_id: &'a str) -> QuotaFuture<'a>;
}

/// The self-hosted default: everything unlimited.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnlimitedQuotaProvider;

impl QuotaProvider for UnlimitedQuotaProvider {
    fn limits_for<'a>(&'a self, _bucket_id: &'a str) -> QuotaFuture<'a> {
        Box::pin(async { TenantLimits::default() })
    }
}

/// Static per-bucket limits loaded from TOML:
///
/// ```toml
/// [default]
/// requests_per_sec = 100
///
/// [[bucket]]
/// id = "tenant-a"
/// requests_per_sec = 10
/// max_concurrent_live_reads = 4
/// ```
#[derive(Debug, Default)]
pub struct StaticQuotaProvider {
    default: TenantLimits,
    buckets: HashMap<String, TenantLimits>,
}

impl StaticQuotaProvider {
    pub fn from_toml_str(source: &str) -> Result<Self, QuotaPolicyError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Limits {
            requests_per_sec: Option<u32>,
            max_concurrent_live_reads: Option<u32>,
            max_request_body_bytes: Option<u64>,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct BucketLimits {
            id: String,
            #[serde(flatten)]
            limits: Limits,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            default: Option<Limits>,
            #[serde(default, rename = "bucket")]
            buckets: Vec<BucketLimits>,
        }
        fn lower(limits: Limits) -> TenantLimits {
            TenantLimits {
                requests_per_sec: limits.requests_per_sec,
                max_concurrent_live_reads: limits.max_concurrent_live_reads,
                max_request_body_bytes: limits.max_request_body_bytes,
            }
        }

        let file: File =
            toml::from_str(source).map_err(|error| QuotaPolicyError::Parse(error.to_string()))?;
        let mut buckets = HashMap::new();
        for bucket in file.buckets {
            if buckets
                .insert(bucket.id.clone(), lower(bucket.limits))
                .is_some()
            {
                return Err(QuotaPolicyError::DuplicateBucket(bucket.id));
            }
        }
        Ok(Self {
            default: file.default.map(lower).unwrap_or_default(),
            buckets,
        })
    }

    pub fn from_file(path: &std::path::Path) -> Result<Self, QuotaPolicyError> {
        let source = std::fs::read_to_string(path).map_err(|error| {
            QuotaPolicyError::Read(path.display().to_string(), error.to_string())
        })?;
        Self::from_toml_str(&source)
    }
}

impl QuotaProvider for StaticQuotaProvider {
    fn limits_for<'a>(&'a self, bucket_id: &'a str) -> QuotaFuture<'a> {
        let limits = self.buckets.get(bucket_id).copied().unwrap_or(self.default);
        Box::pin(async move { limits })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QuotaPolicyError {
    #[error("failed to read quota policy {0}: {1}")]
    Read(String, String),
    #[error("failed to parse quota policy: {0}")]
    Parse(String),
    #[error("quota policy declares bucket {0:?} more than once")]
    DuplicateBucket(String),
}

/// Why a request was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Request rate exhausted; retry after the given whole seconds.
    RateLimited { retry_after_secs: u64 },
    /// The tenant's live-read connection budget is fully in use.
    LiveReadsExhausted,
}

struct RateBucket {
    tokens: f64,
    refilled_at: Instant,
}

/// Prune once the tracked set grows past this. A trigger, not a hard cap:
/// dropping *active* state would reset a tenant's limiter mid-flight, which is a
/// rate-limit bypass rather than a memory fix. If a deployment genuinely serves
/// more concurrently active Buckets than this, the set stays larger.
const PRUNE_ABOVE_TRACKED_BUCKETS: usize = 10_000;

/// Idle threshold for reclaiming a rate bucket.
///
/// Safe by construction, not by tuning. A bucket refills at `rate` per second
/// and is capped at `rate`, so any entry untouched for a full second is already
/// back at maximum tokens. Recreating it therefore grants exactly what refill
/// would have produced — eviction beyond one second of idleness is
/// indistinguishable from retention, and this threshold is far beyond that.
const RATE_BUCKET_IDLE_TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct AdmissionState {
    rate: HashMap<String, RateBucket>,
    live_reads: HashMap<String, u32>,
}

impl AdmissionState {
    /// Reclaim rate buckets no request has touched recently.
    ///
    /// Without this the map holds one entry per Bucket the process has ever
    /// served, so resident memory tracks distinct Buckets ever seen rather than
    /// Buckets currently active — unbounded on a service where accounts create
    /// their own Buckets.
    ///
    /// Only runs once the set is large, so the `retain` scan is amortised across
    /// the insertions that grew it.
    fn prune_idle_rate_buckets(&mut self, now: Instant) {
        if self.rate.len() <= PRUNE_ABOVE_TRACKED_BUCKETS {
            return;
        }
        self.rate.retain(|_bucket_id, bucket| {
            now.saturating_duration_since(bucket.refilled_at) < RATE_BUCKET_IDLE_TTL
        });
    }
}

/// Enforces [`TenantLimits`] with in-memory state. State is per gateway
/// process: a horizontally scaled deployment multiplies effective limits by
/// the replica count, which is the standard first-order trade-off for
/// gateway-local admission.
#[derive(Default)]
pub struct Admission {
    state: Mutex<AdmissionState>,
}

impl Admission {
    fn lock(&self) -> std::sync::MutexGuard<'_, AdmissionState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Takes one token from the bucket's rate limiter.
    pub fn admit_request(
        &self,
        bucket_id: &str,
        limits: TenantLimits,
    ) -> Result<(), AdmissionRejection> {
        let Some(rate) = limits.requests_per_sec else {
            return Ok(());
        };
        let rate = f64::from(rate.max(1));
        let now = Instant::now();
        let mut state = self.lock();
        let bucket = state
            .rate
            .entry(bucket_id.to_owned())
            .or_insert_with(|| RateBucket {
                tokens: rate,
                refilled_at: now,
            });
        let elapsed = now.saturating_duration_since(bucket.refilled_at);
        bucket.tokens = (bucket.tokens + elapsed.as_secs_f64() * rate).min(rate);
        bucket.refilled_at = now;
        let outcome = if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let deficit = (1.0 - bucket.tokens).max(0.0);
            #[expect(
                clippy::cast_sign_loss,
                reason = "deficit and rate are both clamped non-negative above"
            )]
            let retry_after_secs = (deficit / rate).ceil().max(1.0) as u64;
            Err(AdmissionRejection::RateLimited { retry_after_secs })
        };
        // After deciding, so the decision never depends on whether this call
        // happened to be the one that triggered a scan.
        state.prune_idle_rate_buckets(now);
        outcome
    }

    /// Reserves one live-read slot; the returned guard releases it when the
    /// response body is dropped.
    pub fn admit_live_read(
        self: &Arc<Self>,
        bucket_id: &str,
        limits: TenantLimits,
    ) -> Result<Option<LiveReadGuard>, AdmissionRejection> {
        let Some(max) = limits.max_concurrent_live_reads else {
            return Ok(None);
        };
        let mut state = self.lock();
        let active = state.live_reads.entry(bucket_id.to_owned()).or_insert(0);
        if *active >= max {
            return Err(AdmissionRejection::LiveReadsExhausted);
        }
        *active = active.saturating_add(1);
        Ok(Some(LiveReadGuard {
            admission: Arc::clone(self),
            bucket_id: bucket_id.to_owned(),
        }))
    }
}

/// RAII release of a live-read slot.
pub struct LiveReadGuard {
    admission: Arc<Admission>,
    bucket_id: String,
}

impl Drop for LiveReadGuard {
    fn drop(&mut self) {
        let mut state = self.admission.lock();
        if let Some(active) = state.live_reads.get_mut(&self.bucket_id) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.live_reads.remove(&self.bucket_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(rate: Option<u32>, live: Option<u32>) -> TenantLimits {
        TenantLimits {
            requests_per_sec: rate,
            max_concurrent_live_reads: live,
            max_request_body_bytes: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limit_denies_after_burst_and_reports_retry_after() {
        let admission = Admission::default();
        let two_per_sec = limits(Some(2), None);

        admission.admit_request("tenant-a", two_per_sec).unwrap();
        admission.admit_request("tenant-a", two_per_sec).unwrap();
        let rejection = admission
            .admit_request("tenant-a", two_per_sec)
            .expect_err("burst exhausted");
        assert!(matches!(
            rejection,
            AdmissionRejection::RateLimited { retry_after_secs } if retry_after_secs >= 1
        ));

        // Another tenant is unaffected.
        admission.admit_request("tenant-b", two_per_sec).unwrap();

        // Advancing time refills the bucket.
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        admission.admit_request("tenant-a", two_per_sec).unwrap();
    }

    #[tokio::test]
    async fn live_read_slots_release_on_guard_drop() {
        let admission = Arc::new(Admission::default());
        let one_live = limits(None, Some(1));

        let guard = admission
            .admit_live_read("tenant-a", one_live)
            .expect("first live read admitted")
            .expect("guard for limited tenant");
        assert!(matches!(
            admission.admit_live_read("tenant-a", one_live),
            Err(AdmissionRejection::LiveReadsExhausted)
        ));

        drop(guard);
        admission.admit_live_read("tenant-a", one_live).unwrap();
    }

    #[tokio::test]
    async fn unlimited_limits_admit_without_state() {
        let admission = Arc::new(Admission::default());
        admission
            .admit_request("t", TenantLimits::default())
            .unwrap();
        assert!(
            admission
                .admit_live_read("t", TenantLimits::default())
                .expect("admitted")
                .is_none()
        );
    }

    #[test]
    fn static_provider_parses_defaults_and_overrides() {
        let provider = StaticQuotaProvider::from_toml_str(
            r#"
            [default]
            requests_per_sec = 100

            [[bucket]]
            id = "tenant-a"
            requests_per_sec = 10
            max_concurrent_live_reads = 4
            "#,
        )
        .expect("parse policy");

        let tenant_a = futures_executor(provider.limits_for("tenant-a"));
        assert_eq!(tenant_a.requests_per_sec, Some(10));
        assert_eq!(tenant_a.max_concurrent_live_reads, Some(4));
        let other = futures_executor(provider.limits_for("tenant-b"));
        assert_eq!(other.requests_per_sec, Some(100));
        assert_eq!(other.max_concurrent_live_reads, None);
    }

    fn futures_executor<T>(future: impl Future<Output = T>) -> T {
        let mut future = Box::pin(future);
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(value) => value,
            std::task::Poll::Pending => unreachable!("static provider resolves immediately"),
        }
    }
}

#[cfg(test)]
impl Admission {
    /// Tracked rate buckets. Exists for the eviction tests: the point of the
    /// change is a number that stops growing, which is not observable from the
    /// admission decision itself.
    pub(crate) fn tracked_rate_buckets(&self) -> usize {
        self.lock().rate.len()
    }
}

#[cfg(test)]
mod eviction_tests {
    use super::Admission;
    use super::PRUNE_ABOVE_TRACKED_BUCKETS;
    use super::TenantLimits;

    fn limits(rate: u32) -> TenantLimits {
        TenantLimits {
            requests_per_sec: Some(rate),
            ..TenantLimits::default()
        }
    }

    /// Fills the tracked set past the prune trigger. Every entry is touched at
    /// the current instant, so none is prunable yet.
    fn fill_past_trigger(admission: &Admission) {
        for index in 0..=PRUNE_ABOVE_TRACKED_BUCKETS {
            assert!(
                admission
                    .admit_request(&format!("bucket-{index}"), limits(10))
                    .is_ok(),
                "a first request against a fresh bucket must be admitted"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_rate_buckets_are_reclaimed() {
        let admission = Admission::default();
        fill_past_trigger(&admission);
        assert_eq!(
            admission.tracked_rate_buckets(),
            PRUNE_ABOVE_TRACKED_BUCKETS + 1,
            "nothing should be prunable while every entry was just touched"
        );

        tokio::time::advance(super::RATE_BUCKET_IDLE_TTL + std::time::Duration::from_secs(1)).await;
        admission
            .admit_request("late-arrival", limits(10))
            .expect("a fresh bucket is admitted");

        assert_eq!(
            admission.tracked_rate_buckets(),
            1,
            "every idle entry should be gone, leaving only the request that triggered the scan"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_active_bucket_survives_the_scan() {
        let admission = Admission::default();
        fill_past_trigger(&admission);

        tokio::time::advance(super::RATE_BUCKET_IDLE_TTL + std::time::Duration::from_secs(1)).await;
        // Touched after the idle window, so this one is still in use.
        admission
            .admit_request("bucket-0", limits(10))
            .expect("an active bucket stays within its rate");
        admission
            .admit_request("late-arrival", limits(10))
            .expect("a fresh bucket is admitted");

        assert_eq!(
            admission.tracked_rate_buckets(),
            2,
            "pruning must not drop state for a bucket that is still sending traffic"
        );
    }

    /// The property that makes eviction safe rather than a rate-limit bypass.
    ///
    /// A bucket refills at `rate` per second capped at `rate`, so an entry idle
    /// for longer than a second is already back at full tokens. Recreating it
    /// must therefore grant exactly what retention would have granted — assert
    /// the two are indistinguishable rather than trusting the reasoning.
    #[tokio::test(start_paused = true)]
    async fn a_returning_bucket_gets_no_more_than_refill_would_have_given() {
        const RATE: u32 = 5;

        fn burst_after_idling(admission: &Admission, bucket_id: &str) -> u32 {
            let mut admitted = 0;
            while admission.admit_request(bucket_id, limits(RATE)).is_ok() {
                admitted += 1;
                assert!(admitted <= RATE * 2, "burst did not converge");
            }
            admitted
        }

        // Retained: the tracked set stays below the trigger, so nothing is pruned.
        let retained = Admission::default();
        let initial_burst = burst_after_idling(&retained, "kept");
        assert_eq!(initial_burst, RATE);
        tokio::time::advance(super::RATE_BUCKET_IDLE_TTL + std::time::Duration::from_secs(1)).await;
        let after_retention = burst_after_idling(&retained, "kept");

        // Evicted: the same bucket, but pruned out while it was idle.
        let evicted = Admission::default();
        assert_eq!(burst_after_idling(&evicted, "kept"), RATE);
        fill_past_trigger(&evicted);
        tokio::time::advance(super::RATE_BUCKET_IDLE_TTL + std::time::Duration::from_secs(1)).await;
        evicted
            .admit_request("prune-trigger", limits(10))
            .expect("a fresh bucket is admitted");
        let after_eviction = burst_after_idling(&evicted, "kept");

        assert_eq!(after_retention, RATE);
        assert_eq!(
            after_eviction, after_retention,
            "eviction must not hand back a larger burst than refill would have"
        );
    }
}
