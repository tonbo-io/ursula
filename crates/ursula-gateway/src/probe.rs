//! Probes of every upstream, which eject an upstream that stops answering
//! whether or not requests reach it, and readmit it once it answers again
//! (see `ejection`).
//!
//! Every [`Gateway`](crate::Gateway) starts its [`Probes`] when it is built
//! and stops them when its last clone is dropped, so a gateway that answers
//! requests always probes. Each probe sends `GET` [`LIVENESS_PATH`] to one
//! upstream with the client that forwards requests, every
//! [`PROBE_INTERVAL`], one probe at a time per upstream. A probe that fails
//! at the transport, or gets no response headers within [`PROBE_TIMEOUT`],
//! ejects the upstream as a failed request does, and any answer readmits it.
//! An upstream that stops answering, as a stopped process or a frozen host
//! does, leaves selection within [`PROBE_INTERVAL`] plus [`PROBE_TIMEOUT`]
//! of its last answer.
//!
//! A probe past its timeout keeps waiting for its answer, up to the
//! response header timeout, and readmits the upstream if it comes. Dropping
//! it would drop the connection it opens with it, so that under a delay at
//! which a new connection takes longer than the probe timeout every later
//! probe would start cold and time out too: requests skip the ejected
//! upstream, so nothing else would open a connection to it. Waiting lets
//! the connection complete and go into the pool, and the next probe, on it,
//! takes one round trip. The bound is needed because a stopped process's
//! kernel acknowledges everything sent to it, so no transport timeout ends
//! the wait.
//!
//! The probe timeout is sized so that a uniform delay ejects nothing once a
//! connection is pooled: under a 500 ms round trip a probe on a pooled
//! connection takes 500 ms, and a probe every 250 ms keeps its connection
//! within the pool's idle bound. A probe on a new connection takes two
//! round trips, or more over TLS, which past the timeout ejects the
//! upstream until that probe answers. When every upstream is ejected none
//! is skipped, so a delay past the timeout on every path at once ejects
//! nothing either.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::Instant;
use ursula_headers::routing::LIVENESS_PATH;

use crate::GatewayError;
use crate::Hop;
use crate::ejection::Ejections;

/// How often each upstream is probed: the time from one probe's start to
/// the next one's, or longer while a probe waits.
pub(crate) const PROBE_INTERVAL: Duration = Duration::from_millis(250);

/// How long a probe waits for its answer's headers before it ejects the
/// upstream.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// The probes of a gateway's upstreams, one task per upstream, running until
/// this is dropped.
#[derive(Debug, Default)]
pub(crate) struct Probes {
    /// Aborts every probe when dropped.
    _tasks: JoinSet<()>,
}

impl Probes {
    /// Starts probing each distinct upstream of `upstreams` with `client`,
    /// ejecting and readmitting it in `ejections`. A probe waits for its
    /// answer for `answer_bound` at most, the response header timeout of
    /// requests.
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime, as [`tokio::spawn`] does.
    pub(crate) fn start(
        client: &reqwest::Client,
        upstreams: &[String],
        ejections: &Arc<Ejections>,
        answer_bound: Duration,
    ) -> Self {
        let mut upstreams = upstreams.to_vec();
        upstreams.sort_unstable();
        upstreams.dedup();
        let mut tasks = JoinSet::new();
        for upstream in upstreams {
            let probe = Probe {
                url: format!("{}{LIVENESS_PATH}", upstream.trim_end_matches('/')),
                upstream,
                client: client.clone(),
                ejections: Arc::clone(ejections),
                answer_bound: answer_bound.max(PROBE_TIMEOUT),
            };
            tasks.spawn(probe.run());
        }
        Self { _tasks: tasks }
    }
}

/// The probes of one upstream.
struct Probe {
    upstream: String,
    url: String,
    client: reqwest::Client,
    ejections: Arc<Ejections>,
    /// How long a probe waits for its answer in all (see the module doc).
    answer_bound: Duration,
}

impl Probe {
    /// Probes the upstream every [`PROBE_INTERVAL`], forever.
    async fn run(self) {
        loop {
            let started = Instant::now();
            let send = self.client.get(&self.url).send();
            tokio::pin!(send);
            let sent = match tokio::time::timeout_at(after(started, PROBE_TIMEOUT), &mut send).await
            {
                Ok(sent) => Some(sent),
                Err(_elapsed) => {
                    self.ejections
                        .eject(&self.upstream, &GatewayError::UpstreamTimeout {
                            upstream: self.upstream.clone(),
                            timeout: PROBE_TIMEOUT,
                        });
                    tokio::time::timeout_at(after(started, self.answer_bound), send)
                        .await
                        .ok()
                }
            };
            match sent {
                // A `204` has no body, so dropping the answer returns its
                // connection to the pool.
                Some(Ok(_answer)) => self.ejections.readmit(&self.upstream),
                Some(Err(source)) => self.ejections.eject(
                    &self.upstream,
                    &GatewayError::of_transport(&self.upstream, Hop::First, source),
                ),
                // Ejected at the probe timeout already.
                None => {}
            }
            tokio::time::sleep_until(after(started, PROBE_INTERVAL)).await;
        }
    }
}

/// `wait` after `start`, or `start` in the impossible case that the clock
/// cannot represent it.
fn after(start: Instant, wait: Duration) -> Instant {
    start.checked_add(wait).unwrap_or(start)
}
