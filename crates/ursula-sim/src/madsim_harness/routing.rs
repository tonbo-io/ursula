//! Routing before the body under leader churn (#603, #581 section 6.1):
//! producers write through HTTP to a runtime hosting one replica of a
//! three-node OpenRaft group, and the front door routes each write from the
//! production leadership view of that replica
//! ([`ursula_raft::RaftGroupHandleRegistry::leadership`]), registered by the
//! runtime's engine factory.
//!
//! Each episode cuts the leader off while writes are in flight: the leader
//! still leads in its own view, so it admits them and polls their bodies,
//! which take a while to arrive. The others elect a new leader and the
//! partition heals, so the old leader steps down and its view follows the
//! new one. Writes whose head arrives now are redirected before their body;
//! the writes in flight are refused after theirs. Then the runtime swaps to
//! the new leader's engine and the old leader restarts as a follower.
//!
//! Every request body records whether the server polled it. Writes go as
//! HTTP/2 or with `Expect: 100-continue`, where a server that answers before
//! the body never reads it. The client treats a `307` as an unknown outcome
//! and resends the same key until it gets a producer answer. Invariants:
//!
//! - `no_redirect_after_the_body_is_polled`: no `307` answers a write whose
//!   body the server polled;
//! - `every_redirect_is_a_typed_routing_answer`: every `307` parses as a
//!   [`RoutingAnswer`] that names a leader, with a `Location`;
//! - `routed_writes_apply_exactly_once`: no key applies twice, and the stream
//!   holds each key's body once, where its receipt says.
//!
//! [`Controls`] break one defence, for the test that shows the invariants
//! catch its loss.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::atomic::AtomicBool;

use axum::Router;
use axum::body::Bytes;
use axum::http::Response;
use axum::http::Version;
use axum::http::header::LOCATION;
use axum::http::header::RETRY_AFTER;
use ursula_headers::Delivery;
use ursula_headers::ProducerAnswer;
use ursula_headers::RoutingAnswer;

use super::Arc;
use super::Body;
use super::Duration;
use super::HttpRequest;
use super::Ordering;
use super::ServiceExt;
use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::StatusCode;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::write_keys::BODY_LEN;
use super::write_keys::CONTENT_TYPE;
use super::write_keys::Cluster;
use super::write_keys::Label;

pub(super) const NO_REDIRECT_INVARIANT: &str = "no_redirect_after_the_body_is_polled";
pub(super) const TYPED_INVARIANT: &str = "every_redirect_is_a_typed_routing_answer";
pub(super) const ONCE_INVARIANT: &str = "routed_writes_apply_exactly_once";
/// Attempts of one key before the client gives up.
const MAX_ATTEMPTS: usize = 40;
/// How long the client waits before it resends after an unknown outcome.
const RESEND_PAUSE: Duration = Duration::from_millis(20);
/// How long the old leader may take to follow its successor after the heal.
const STEP_DOWN_WAIT: Duration = Duration::from_secs(5);

/// Defences a negative control turns off. Production runs use the default,
/// with every defence on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// A leadership refusal after the body redirects, as one before it does
    /// ([`ursula_stream::sim_controls::Defence::NoRedirectAfterBody`]).
    redirect_after_body: bool,
}

#[cfg(test)]
impl Controls {
    pub(super) const REDIRECT_AFTER_BODY: Self = Self {
        redirect_after_body: true,
    };
}

/// The defences [`Controls`] turned off for this simulation, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        ursula_stream::sim_controls::set_off(
            ursula_stream::sim_controls::Defence::NoRedirectAfterBody,
            controls.redirect_after_body,
        );
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        ursula_stream::sim_controls::set_off(
            ursula_stream::sim_controls::Defence::NoRedirectAfterBody,
            false,
        );
    }
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// How a write's head asks the server not to read the body it answers
/// early: both mean a server that answers before the body never polls it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// An HTTP/2 request, which the server answers and resets.
    Http2,
    /// An HTTP/1.1 request with `Expect: 100-continue`, which also asks the
    /// leader for an early answer from the write's key.
    ExpectContinue,
}

/// One write: its producer's key and how it is sent.
#[derive(Debug, Clone, Copy)]
struct Write {
    producer: u8,
    seq: u64,
    framing: Framing,
    /// How long the body takes to arrive once the server polls it, in
    /// milliseconds.
    body_delay_ms: u64,
}

impl Write {
    fn label(self) -> Label {
        Label {
            producer: self.producer,
            epoch: 0,
            seq: self.seq,
            clone: false,
        }
    }
}

/// One leader change.
#[derive(Debug)]
struct Episode {
    /// Writes of each steady producer before the leader is cut off.
    steady: u64,
    /// Writes sent to the cut-off leader, whose bodies arrive late.
    in_flight: Vec<Write>,
    /// How long the in-flight writes travel before the others elect, in
    /// milliseconds.
    travel_ms: u64,
    /// Writes whose head reaches the old leader after it stepped down.
    late: Vec<Write>,
}

/// The plan of one run. The steady producers write one key at a time; each
/// write caught in a leader change is the first of its own producer, so
/// writes resolved in any order never leave a gap in a producer's seqs.
#[derive(Debug)]
struct RoutingPlan {
    episodes: Vec<Episode>,
}

/// Producers that write one key at a time between leader changes.
const STEADY_PRODUCERS: u8 = 3;

impl RoutingPlan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x726f_7574_6562_6f64);
        let mut next_producer = STEADY_PRODUCERS;
        let mut write = |rng: &mut SplitMix64, max_delay_ms: u64| {
            let producer = next_producer;
            next_producer = next_producer.saturating_add(1);
            Write {
                producer,
                seq: 0,
                framing: if rng.next_bounded(2) == 0 {
                    Framing::Http2
                } else {
                    Framing::ExpectContinue
                },
                body_delay_ms: rng.next_bounded(max_delay_ms),
            }
        };
        let episode_count = 2_u64.saturating_add(rng.next_bounded(2));
        let mut episodes = Vec::new();
        for _ in 0..episode_count {
            let steady = 1_u64.saturating_add(rng.next_bounded(2));
            let in_flight_count = 2_u64.saturating_add(rng.next_bounded(2));
            let in_flight = (0..in_flight_count).map(|_| write(&mut rng, 40)).collect();
            let travel_ms = 1_u64.saturating_add(rng.next_bounded(5));
            let late_count = 2_u64.saturating_add(rng.next_bounded(2));
            let late = (0..late_count).map(|_| write(&mut rng, 10)).collect();
            episodes.push(Episode {
                steady,
                in_flight,
                travel_ms,
                late,
            });
        }
        Self { episodes }
    }
}

fn take_seq(next_seq: &mut BTreeMap<u8, u64>, producer: u8) -> u64 {
    let next = next_seq.entry(producer).or_default();
    let seq = *next;
    *next = next.saturating_add(1);
    seq
}

/// What one request saw: its answer's status and headers, and whether the
/// server polled its body before answering.
struct Exchange {
    write: Write,
    response: Response<Body>,
    body_polled: bool,
}

/// Sends `write` once, with a body that records whether it was polled.
async fn send_once(app: Router, path: String, incarnation: String, write: Write) -> Exchange {
    let label = write.label();
    let payload = label.body();
    let polled = Arc::new(AtomicBool::new(false));
    let body = {
        let polled = Arc::clone(&polled);
        let delay = Duration::from_millis(write.body_delay_ms);
        Body::from_stream(futures_util::stream::once(async move {
            polled.store(true, Ordering::SeqCst);
            if !delay.is_zero() {
                madsim::time::sleep(delay).await;
            }
            Ok::<_, Infallible>(Bytes::from(payload))
        }))
    };
    let mut request = HttpRequest::builder()
        .method("POST")
        .uri(path)
        .header("content-type", CONTENT_TYPE)
        .header("content-length", BODY_LEN.to_string())
        .header("producer-id", label.producer_id())
        .header("producer-epoch", label.epoch.to_string())
        .header("producer-seq", label.seq.to_string())
        .header("stream-incarnation", incarnation)
        .header(
            "producer-digest",
            blake3::hash(&label.body()).to_hex().to_string(),
        );
    request = match write.framing {
        Framing::Http2 => request.version(Version::HTTP_2),
        Framing::ExpectContinue => request
            .version(Version::HTTP_11)
            .header("expect", "100-continue"),
    };
    let response = app
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response");
    // The answer is decided: what the server polled until now is what it
    // read before answering.
    let body_polled = polled.load(Ordering::SeqCst);
    Exchange {
        write,
        response,
        body_polled,
    }
}

fn header_pairs<B>(response: &Response<B>) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

/// What the answers told, checked as they come.
#[derive(Default)]
struct Ledger {
    /// The receipt range of each key that got one, by `(producer, seq)`.
    receipts: BTreeMap<(u8, u64), Option<(u64, u64)>>,
    /// How often each key got an applied receipt.
    applied: BTreeMap<(u8, u64), u64>,
    redirects: u64,
    refused_after_body: u64,
}

impl Ledger {
    /// Checks one exchange against the routing invariants and returns its
    /// producer answer, or `None` when the outcome is unknown.
    fn check(&mut self, trace: &mut SimTrace, exchange: &Exchange) -> Option<ProducerAnswer> {
        let Exchange {
            write,
            response,
            body_polled,
        } = exchange;
        let status = response.status();
        if status == StatusCode::TEMPORARY_REDIRECT {
            if *body_polled {
                fail(
                    trace,
                    NO_REDIRECT_INVARIANT,
                    "write_answered",
                    format!("{write:?} was redirected after the server polled its body"),
                );
            }
            match RoutingAnswer::parse(status.as_u16(), header_pairs(response)) {
                Ok(Some(RoutingAnswer::NotLeader { .. } | RoutingAnswer::NotHosted { .. }))
                    if response.headers().contains_key(LOCATION) => {}
                other => fail(
                    trace,
                    TYPED_INVARIANT,
                    "write_answered",
                    format!("{write:?} got a 307 that is no typed redirect: {other:?}"),
                ),
            }
            self.redirects = self.redirects.saturating_add(1);
            return None;
        }
        let parsed = ProducerAnswer::parse(status.as_u16(), header_pairs(response));
        let answer = match parsed {
            Ok(Some(answer)) => answer,
            Ok(None) | Err(_) => {
                // Only the refusal the rule prescribes after the body counts
                // toward the run's coverage: a `503` with `Retry-After`. Any
                // other unknown outcome, a `500` included, is resent but
                // shows nothing about the rule.
                if *body_polled
                    && status == StatusCode::SERVICE_UNAVAILABLE
                    && response.headers().contains_key(RETRY_AFTER)
                {
                    self.refused_after_body = self.refused_after_body.saturating_add(1);
                }
                return None;
            }
        };
        let slot = (write.producer, write.seq);
        match &answer {
            ProducerAnswer::Receipt {
                delivery, receipt, ..
            } => {
                let range = (receipt.start().get(), receipt.next().get());
                if let Some(Some(held)) = self.receipts.get(&slot)
                    && *held != range
                {
                    fail(
                        trace,
                        ONCE_INVARIANT,
                        "receipt",
                        format!("{write:?} got receipt {range:?} after {held:?}"),
                    );
                }
                self.receipts.insert(slot, Some(range));
                if *delivery == Delivery::Applied {
                    let count = self.applied.entry(slot).or_default();
                    *count = count.saturating_add(1);
                    if *count > 1 {
                        fail(
                            trace,
                            ONCE_INVARIANT,
                            "receipt",
                            format!("{write:?} applied {count} times"),
                        );
                    }
                }
            }
            // A repeat whose range the record no longer keeps.
            ProducerAnswer::ReceiptEvicted { .. } => {
                self.receipts.entry(slot).or_insert(None);
            }
            other => fail(
                trace,
                ONCE_INVARIANT,
                "answer",
                format!("{write:?} got {other:?}"),
            ),
        }
        Some(answer)
    }
}

/// Sends `write` until it gets a producer answer, checking every exchange.
async fn deliver(cluster: &Cluster, trace: &mut SimTrace, ledger: &mut Ledger, write: Write) {
    for _ in 0..MAX_ATTEMPTS {
        let exchange = send_once(
            cluster.app.clone(),
            cluster.path.clone(),
            cluster.incarnation.clone(),
            write,
        )
        .await;
        if ledger.check(trace, &exchange).is_some() {
            return;
        }
        madsim::time::sleep(RESEND_PAUSE).await;
    }
    panic!("{write:?} got no producer answer in {MAX_ATTEMPTS} attempts");
}

pub(super) async fn run_routing_before_body_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = RoutingPlan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let mut cluster = Cluster::start(&config, &mut trace).await;
    let group = cluster.placement.raft_group_id;
    let mut ledger = Ledger::default();
    let mut next_seq = BTreeMap::<u8, u64>::new();
    let mut unresolved = Vec::new();
    for (index, episode) in plan.episodes.iter().enumerate() {
        for producer in 0..STEADY_PRODUCERS {
            for _ in 0..episode.steady {
                let write = Write {
                    producer,
                    seq: take_seq(&mut next_seq, producer),
                    framing: if index % 2 == 0 {
                        Framing::Http2
                    } else {
                        Framing::ExpectContinue
                    },
                    body_delay_ms: 0,
                };
                deliver(&cluster, &mut trace, &mut ledger, write).await;
            }
        }

        // The leader is cut off and still leads in its own view: it admits
        // the writes in flight and polls their bodies, which arrive late.
        let old_leader = cluster
            .factory
            .leader_id(group)
            .expect("runtime raft leader id");
        cluster.isolate_leader();
        trace.push(SimEvent::FaultApplied {
            phase: "leader_isolated_with_bodies_in_flight".to_owned(),
        });
        let in_flight = episode
            .in_flight
            .iter()
            .map(|write| {
                madsim::task::spawn(send_once(
                    cluster.app.clone(),
                    cluster.path.clone(),
                    cluster.incarnation.clone(),
                    *write,
                ))
            })
            .collect::<Vec<_>>();
        madsim::time::sleep(Duration::from_millis(episode.travel_ms)).await;
        let new_leader = cluster
            .factory
            .wait_successor(group)
            .await
            .expect("the others elect a new leader");
        trace.push(SimEvent::LeaderElected {
            leader_id: new_leader,
        });

        // The partition heals: the old leader steps down, and its view, the
        // one the front door routes by, follows the new leader.
        cluster.heal();
        cluster
            .factory
            .raft_handle(old_leader)
            .expect("the old leader's replica")
            .wait(Some(STEP_DOWN_WAIT))
            .metrics(
                |metrics| {
                    metrics.state != openraft::ServerState::Leader
                        && metrics.current_leader == Some(new_leader)
                },
                "the deposed leader follows its successor",
            )
            .await
            .expect("the deposed leader follows its successor");
        trace.push(SimEvent::FaultApplied {
            phase: "deposed_leader_follows".to_owned(),
        });

        // Writes whose head arrives now are routed by the deposed view.
        for write in &episode.late {
            let exchange = send_once(
                cluster.app.clone(),
                cluster.path.clone(),
                cluster.incarnation.clone(),
                *write,
            )
            .await;
            if ledger.check(&mut trace, &exchange).is_none() {
                unresolved.push(*write);
            }
        }
        // The writes in flight resolve: their bodies were polled.
        for handle in in_flight {
            let exchange = handle.await.expect("in-flight write task");
            if ledger.check(&mut trace, &exchange).is_none() {
                unresolved.push(exchange.write);
            }
        }

        // The runtime drops the deposed engine and hosts the new leader's;
        // the old leader restarts as a follower.
        let stopped = cluster
            .kill_leader(&mut trace, "deposed_leader_replaced")
            .await;
        cluster.restart(&mut trace, stopped).await;

        // The client resends every write whose outcome is unknown.
        for write in unresolved.drain(..) {
            deliver(&cluster, &mut trace, &mut ledger, write).await;
        }
    }

    // The stream holds each key's body once, where its receipt says.
    let bytes = cluster.read_all().await;
    let mut seen = BTreeMap::<(u8, u64), u64>::new();
    for (index, chunk) in bytes.chunks(BODY_LEN).enumerate() {
        let start = u64::try_from(index.saturating_mul(BODY_LEN)).expect("offset fits u64");
        let Some(label) = Label::parse(chunk) else {
            fail(
                &mut trace,
                ONCE_INVARIANT,
                "final_read",
                format!("unreadable body at {start}"),
            );
        };
        let slot = (label.producer, label.seq);
        if let Some(first) = seen.insert(slot, start) {
            fail(
                &mut trace,
                ONCE_INVARIANT,
                "final_read",
                format!("{label:?} is in the stream at {first} and at {start}"),
            );
        }
        let end = start.saturating_add(u64::try_from(BODY_LEN).expect("small"));
        match ledger.receipts.get(&slot) {
            Some(Some(range)) if *range != (start, end) => fail(
                &mut trace,
                ONCE_INVARIANT,
                "final_read",
                format!("{label:?} is at {start}, its receipt said {range:?}"),
            ),
            Some(_) => {}
            None => fail(
                &mut trace,
                ONCE_INVARIANT,
                "final_read",
                format!("{label:?} is in the stream, but no answer named it"),
            ),
        }
    }
    for slot in ledger.receipts.keys() {
        if !seen.contains_key(slot) {
            fail(
                &mut trace,
                ONCE_INVARIANT,
                "final_read",
                format!("key {slot:?} was answered, but the stream does not hold it"),
            );
        }
    }
    // The run exercised both sides of the rule.
    assert!(
        ledger.redirects > 0,
        "seed {} redirected no write before its body",
        config.seed
    );
    assert!(
        ledger.refused_after_body > 0,
        "seed {} refused no write after its body",
        config.seed
    );
    trace.push(SimEvent::RoutingBeforeBodyVerified {
        stream: config.stream.clone(),
        keys: u64::try_from(ledger.receipts.len()).expect("count fits u64"),
        redirects: ledger.redirects,
        refused_after_body: ledger.refused_after_body,
        leader_changes: cluster.leader_kills,
    });
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: cluster.factory.leader_id(group).unwrap_or_default(),
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::NO_REDIRECT_INVARIANT;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::RoutingBeforeBodyUnderLeaderChurn;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug()))
    }

    #[test]
    fn routing_before_the_body_under_leader_churn_replays() {
        let _guard = sim_test_guard();
        for seed in [500, 501] {
            let first = ThreeNodeRaftSim::run_routing_with_controls_report(
                config(seed),
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_routing_with_controls_report(
                config(seed),
                Controls::default(),
            );
            assert_eq!(first, second);
        }
    }

    #[test]
    fn negative_control_a_redirect_after_the_body_is_caught() {
        let _guard = sim_test_guard();
        SimTrace::clear_recorded();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_routing_with_controls_report(
                config(500),
                Controls::REDIRECT_AFTER_BODY,
            )
        });
        std::panic::set_hook(previous_hook);
        assert!(
            result.is_err(),
            "a redirect after the body left the run passing"
        );
        assert!(
            invariant_failed(&SimTrace::last_recorded(), NO_REDIRECT_INVARIANT),
            "a redirect after the body did not fail `{NO_REDIRECT_INVARIANT}`"
        );
    }
}
