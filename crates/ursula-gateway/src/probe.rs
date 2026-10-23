//! Probes of every upstream, which eject an upstream that stops answering
//! whether or not requests reach it, and readmit it once it answers again
//! (see `ejection`).
//!
//! [`Gateway::probe_upstreams`] sends `GET` [`LIVENESS_PATH`] to each
//! upstream every [`PROBE_INTERVAL`], one probe at a time per upstream, with
//! the client that forwards requests. A probe that fails at the transport or
//! gets no response headers within [`PROBE_TIMEOUT`] ejects the upstream as
//! a failed request does, and any answer readmits it. An upstream that stops
//! answering, as a stopped process or a frozen host does, leaves selection
//! within [`PROBE_INTERVAL`] plus [`PROBE_TIMEOUT`] of its last answer.
//!
//! The probe timeout is sized so that a uniform delay ejects nothing: under
//! a 500 ms round trip a probe takes one round trip on a pooled connection
//! and two on a new one, 1 s, which stays within 1.5 s with jitter to spare.
//! A probe every 250 ms keeps its connection within the pool's idle bound,
//! so probes mostly take one round trip. When every upstream is ejected none
//! is skipped, so a delay past the timeout on every path at once ejects
//! nothing either.

use std::time::Duration;

use tokio::time::Instant;
use ursula_headers::routing::LIVENESS_PATH;

use crate::Gateway;
use crate::GatewayError;
use crate::Hop;

/// How often each upstream is probed: the time from one probe's start to
/// the next one's, or longer while a probe waits.
pub(crate) const PROBE_INTERVAL: Duration = Duration::from_millis(250);

/// How long a probe waits for its answer's headers before it ejects the
/// upstream.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

impl Gateway {
    /// Probes every upstream until the future is dropped (see `probe`). A
    /// gateway that answers requests runs this beside them: without it an
    /// ejected upstream comes back only when a request reaches it, which
    /// happens only once every upstream is ejected.
    ///
    /// `ursula gateway` runs it for as long as it serves. An embedder of
    /// [`Gateway`] spawns it once, next to the server that calls
    /// [`Gateway::handle`].
    pub async fn probe_upstreams(&self) {
        let mut upstreams = self.config.upstreams.clone();
        upstreams.sort_unstable();
        upstreams.dedup();
        let mut probes = tokio::task::JoinSet::new();
        for upstream in upstreams {
            let gateway = self.clone();
            probes.spawn(async move { gateway.probe_upstream(&upstream).await });
        }
        while let Some(ended) = probes.join_next().await {
            tracing::error!(?ended, "an upstream's probes ended");
        }
    }

    /// Probes `upstream` every [`PROBE_INTERVAL`], forever.
    async fn probe_upstream(&self, upstream: &str) {
        let url = format!("{}{LIVENESS_PATH}", upstream.trim_end_matches('/'));
        loop {
            let started = Instant::now();
            let probed = tokio::time::timeout(PROBE_TIMEOUT, self.client.get(&url).send()).await;
            match probed {
                // A `204` has no body, so dropping the answer returns its
                // connection to the pool.
                Ok(Ok(_answer)) => self.upstream_answered(upstream),
                Ok(Err(source)) => self.upstream_failed(
                    None,
                    upstream,
                    &GatewayError::of_transport(upstream, Hop::First, source),
                ),
                Err(_elapsed) => {
                    self.upstream_failed(None, upstream, &GatewayError::UpstreamTimeout {
                        upstream: upstream.to_owned(),
                        timeout: PROBE_TIMEOUT,
                    })
                }
            }
            tokio::time::sleep_until(started.checked_add(PROBE_INTERVAL).unwrap_or(started)).await;
        }
    }
}
