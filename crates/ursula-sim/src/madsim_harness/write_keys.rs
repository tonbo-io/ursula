//! Write keys under faults (#581 section 10): producers that write through
//! HTTP against a runtime hosting one replica of a three-node OpenRaft group.
//!
//! Invariant `producer_repeats_are_exactly_once`: every key applies at most
//! one body, once. The leader is killed while writes are in flight, so a
//! write's outcome is unknown; the client resends the same key until it gets
//! an answer. A resender duplicates writes at once and replays older ones
//! later, and clones send other bodies under applied keys with their own
//! `Producer-Digest`. Every receipt names the body its key holds, every
//! clone is refused with `key-conflict`, and the stream holds each key's
//! body exactly once.
//!
//! Invariant `idle_expiry_cannot_reopen_an_epoch`: a producer idle for 7 days
//! retires, at its own next write or at `TidyStream`, and the stream's epoch
//! floor rises to its epoch. A stale writer of that producer, with
//! `Stream-Incarnation`, never applies a write of the retired epoch again,
//! across a leader kill, while a claim above the floor applies.
//!
//! [`Controls`] break one defence each, for the tests that show the
//! invariants catch its loss.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use axum::Router;
use axum::http::Response;
use ursula_headers::Delivery;
use ursula_headers::ProducerAnswer;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_shard::ShardPlacement;

use super::Arc;
use super::AtomicU64;
use super::Body;
use super::Duration;
use super::HttpState;
use super::MadsimRuntimeRaftNetworkFactory;
use super::Ordering;
use super::RuntimeConfig;
use super::RuntimeThreading;
use super::ShardRuntime;
use super::SimEvent;
use super::SimHttpWallClock;
use super::SimTrace;
use super::SplitMix64;
use super::StatusCode;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::http::body_bytes;
use super::http::send;
use super::http_offset;
use super::parse_http_offset;
use super::router_with_http_state;
use super::sim_network_policy;

pub(super) const REPEATS_INVARIANT: &str = "producer_repeats_are_exactly_once";
pub(super) const EXPIRY_INVARIANT: &str = "idle_expiry_cannot_reopen_an_epoch";
/// The epoch the expiry scenario's producer holds when it retires.
const EXPIRY_EPOCH: u64 = 3;
pub(super) const CONTENT_TYPE: &str = "application/octet-stream";
/// Every body is this long, so the stream splits back into bodies.
pub(super) const BODY_LEN: usize = 16;
const IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// How long the network holds each Raft RPC back while a write is caught
/// in flight.
const REPLICATION_HOLD: Duration = Duration::from_millis(30);
/// How long a client waits before it gives up on a write.
const CLIENT_TIMEOUT: Duration = Duration::from_millis(5);
/// Attempts of one key before the client gives up.
const MAX_ATTEMPTS: usize = 40;

/// Defences a negative control turns off. Production runs use the default,
/// with every defence on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// Writers send no `Producer-Digest`, so a clone's body cannot be told
    /// from the original's.
    digest_off: bool,
    /// The client gives a resent write a new seq instead of its key.
    renumbering: bool,
    /// The stale writer sends no `Stream-Incarnation`, which the epoch floor
    /// does not bind: the stream treats it as a server without a floor
    /// treats every writer.
    floor_off: bool,
}

#[cfg(test)]
impl Controls {
    pub(super) const DIGEST_OFF: Self = Self {
        digest_off: true,
        renumbering: false,
        floor_off: false,
    };
    pub(super) const RENUMBERING: Self = Self {
        digest_off: false,
        renumbering: true,
        floor_off: false,
    };
    pub(super) const FLOOR_OFF: Self = Self {
        digest_off: false,
        renumbering: false,
        floor_off: true,
    };
}

/// A body: its producer, the seq it was written for, and whether a clone
/// wrote it. Padded to [`BODY_LEN`] bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Label {
    pub(super) producer: u8,
    pub(super) epoch: u64,
    pub(super) seq: u64,
    pub(super) clone: bool,
}

impl Label {
    pub(super) fn body(self) -> Vec<u8> {
        let mut body = format!(
            "{}e{}s{:02}{}",
            char::from(b'a'.saturating_add(self.producer)),
            self.epoch,
            self.seq,
            if self.clone { 'c' } else { 'o' }
        )
        .into_bytes();
        body.resize(BODY_LEN, b'.');
        body
    }

    pub(super) fn parse(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?.trim_end_matches('.');
        let producer = text.bytes().next()?.checked_sub(b'a')?;
        let rest = text.get(1..)?.strip_prefix('e')?;
        let (epoch, rest) = rest.split_once('s')?;
        let (seq, kind) = rest.split_at_checked(rest.len().checked_sub(1)?)?;
        Some(Self {
            producer,
            epoch: epoch.parse().ok()?,
            seq: seq.parse().ok()?,
            clone: match kind {
                "c" => true,
                "o" => false,
                _ => return None,
            },
        })
    }

    pub(super) fn producer_id(self) -> String {
        format!("writer-{}", self.producer)
    }
}

/// One producer write.
#[derive(Debug, Clone, Copy)]
struct Key {
    label: Label,
    /// The seq the request carries: the label's, unless a renumbering
    /// client gave it another.
    seq: u64,
}

/// What came back for one request.
enum Answer {
    Producer(ProducerAnswer),
    /// No producer answer: the write's outcome is unknown.
    Unknown(StatusCode),
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// A runtime over a three-node group, its HTTP router and its clock. The
/// router's `HttpState` reads leadership from the factory's front-door
/// registry, so writes are routed before their body by the production
/// leadership view.
pub(super) struct Cluster {
    pub(super) app: Router,
    policy: InProcessRaftNetworkPolicy,
    pub(super) runtime: ShardRuntime,
    pub(super) factory: MadsimRuntimeRaftNetworkFactory,
    pub(super) placement: ShardPlacement,
    now_ms: Arc<AtomicU64>,
    pub(super) path: String,
    pub(super) incarnation: String,
    pub(super) leader_kills: u64,
}

impl Cluster {
    pub(super) async fn start(config: &ThreeNodeRaftSimConfig, trace: &mut SimTrace) -> Self {
        let mut cluster = Self::start_empty(config, trace).await;
        let create = send(
            &cluster.app,
            "PUT",
            &cluster.path,
            &[("content-type", CONTENT_TYPE)],
            Body::empty(),
        )
        .await;
        assert_eq!(create.status(), StatusCode::CREATED);
        cluster.incarnation = create
            .headers()
            .get("stream-incarnation")
            .and_then(|value| value.to_str().ok())
            .expect("a created stream names its incarnation")
            .to_owned();
        trace.push(SimEvent::StreamCreated {
            stream: config.stream.clone(),
        });
        cluster
    }

    /// A cluster whose group holds nothing yet: no stream, only the group's
    /// genesis once its first leader proposed it.
    pub(super) async fn start_empty(config: &ThreeNodeRaftSimConfig, trace: &mut SimTrace) -> Self {
        let policy = sim_network_policy();
        let factory = MadsimRuntimeRaftNetworkFactory::new(config.seed, policy.clone());
        let mut runtime_config = RuntimeConfig::new(1, 1);
        runtime_config.threading = RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(runtime_config, factory.clone())
            .expect("spawn hosted runtime over a three-node raft group");
        let peers = (1..=3).map(|node_id| (node_id, format!("http://runtime-raft-node-{node_id}")));
        let now_ms = Arc::new(AtomicU64::new(1_000));
        // The front door routes writes through the leadership view of the
        // replica the runtime hosts, as a node's own registry does.
        let state =
            HttpState::with_static_raft_cluster(runtime.clone(), factory.front_door(), peers)
                .with_wall_clock_handle(Arc::new(SimHttpWallClock {
                    now_ms: Arc::clone(&now_ms),
                }));
        let app = router_with_http_state(state);
        trace.push(SimEvent::ClusterBuilt { seed: config.seed });
        let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
        // Building the group elects its first leader.
        runtime
            .warm_all_groups()
            .await
            .expect("start the group's replicas");
        let placement = runtime.locate(&config.stream);
        let leader_id = factory
            .leader_id(placement.raft_group_id)
            .expect("runtime raft leader id");
        trace.push(SimEvent::LeaderElected { leader_id });
        Self {
            app,
            policy,
            runtime,
            factory,
            placement,
            now_ms,
            path,
            incarnation: String::new(),
            leader_kills: 0,
        }
    }

    /// Kills the leader: the runtime drops its engine and the others elect
    /// a new leader. Returns the old leader, for [`Self::restart`] once no
    /// request holds its engine.
    pub(super) async fn kill_leader(&mut self, trace: &mut SimTrace, phase: &str) -> u64 {
        let old_leader_id = self
            .factory
            .unregister_current_leader(self.placement.raft_group_id)
            .expect("unregister the leader");
        self.runtime
            .shutdown_group_engine_for_simulation(self.placement)
            .await
            .expect("shut down the leader's engine");
        trace.push(SimEvent::FaultApplied {
            phase: phase.to_owned(),
        });
        trace.push(SimEvent::NodeStopped {
            node_id: old_leader_id,
        });
        let (new_leader_id, engine) = self
            .factory
            .take_current_leader_engine(self.placement.raft_group_id)
            .await
            .expect("the others elect a new leader");
        self.runtime
            .install_group_engine_for_simulation(self.placement, engine)
            .await
            .expect("install the new leader's engine");
        trace.push(SimEvent::LeaderElected {
            leader_id: new_leader_id,
        });
        self.leader_kills = self.leader_kills.saturating_add(1);
        old_leader_id
    }

    /// Deposes a leader that [`Self::isolate_leader`] cut off while
    /// `in_flight` was proposed to it: the others elect a new leader, the
    /// partition heals so the old one steps down and fails the write, and
    /// the runtime swaps engines. Returns the old leader, for
    /// [`Self::restart`], and the write's first answer.
    pub(super) async fn depose_isolated_leader<T>(
        &mut self,
        trace: &mut SimTrace,
        in_flight: madsim::task::JoinHandle<T>,
    ) -> (u64, T) {
        let old_leader_id = self
            .factory
            .leader_id(self.placement.raft_group_id)
            .expect("runtime raft leader id");
        trace.push(SimEvent::FaultApplied {
            phase: "leader_isolated_with_write_in_flight".to_owned(),
        });
        let (new_leader_id, engine) = self
            .factory
            .take_current_leader_engine(self.placement.raft_group_id)
            .await
            .expect("the others elect a new leader");
        trace.push(SimEvent::LeaderElected {
            leader_id: new_leader_id,
        });
        self.heal();
        let first = in_flight.await.expect("in-flight write task");
        self.runtime
            .shutdown_group_engine_for_simulation(self.placement)
            .await
            .expect("shut down the deposed leader's engine");
        self.runtime
            .install_group_engine_for_simulation(self.placement, engine)
            .await
            .expect("install the new leader's engine");
        trace.push(SimEvent::NodeStopped {
            node_id: old_leader_id,
        });
        self.leader_kills = self.leader_kills.saturating_add(1);
        (old_leader_id, first)
    }

    /// Cuts the leader off from the followers: what it proposes now cannot
    /// commit.
    pub(super) fn isolate_leader(&self) {
        let leader = self
            .factory
            .leader_id(self.placement.raft_group_id)
            .expect("runtime raft leader id");
        for node_id in (1..=3).filter(|node_id| *node_id != leader) {
            self.policy.partition_bidirectional(leader, node_id);
        }
    }

    /// Heals every partition.
    pub(super) fn heal(&self) {
        for a in 1..=3 {
            for b in 1..=3 {
                self.policy.heal_bidirectional(a, b);
            }
        }
    }

    /// Restarts a killed node from its log as a follower.
    pub(super) async fn restart(&self, trace: &mut SimTrace, node_id: u64) {
        self.factory
            .restart_follower(node_id)
            .await
            .expect("restart the old leader as a follower");
        trace.push(SimEvent::NodeRestarted { node_id });
    }

    fn request(&self, key: Key, body: &[u8], tokened: bool, digest: bool, early: bool) -> Request {
        let mut headers = vec![
            ("content-type".to_owned(), CONTENT_TYPE.to_owned()),
            ("producer-id".to_owned(), key.label.producer_id()),
            ("producer-epoch".to_owned(), key.label.epoch.to_string()),
            ("producer-seq".to_owned(), key.seq.to_string()),
        ];
        if tokened {
            headers.push(("stream-incarnation".to_owned(), self.incarnation.clone()));
        }
        if digest {
            headers.push((
                "producer-digest".to_owned(),
                blake3::hash(body).to_hex().to_string(),
            ));
        }
        if early {
            headers.push(("expect".to_owned(), "100-continue".to_owned()));
        }
        Request {
            app: self.app.clone(),
            path: self.path.clone(),
            headers,
            body: body.to_vec(),
        }
    }

    /// The whole stream, read from offset 0.
    pub(super) async fn read_all(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let offset = u64::try_from(bytes.len()).expect("offset fits u64");
            let read = send(
                &self.app,
                "GET",
                &format!("{}?offset={}", self.path, http_offset(offset)),
                &[],
                Body::empty(),
            )
            .await;
            assert_eq!(read.status(), StatusCode::OK, "read at {offset}");
            let up_to_date = read.headers().contains_key("stream-up-to-date");
            let next = read
                .headers()
                .get("stream-next-offset")
                .map(parse_http_offset)
                .expect("a read names its next offset");
            let body = body_bytes(read).await;
            bytes.extend_from_slice(&body);
            assert_eq!(
                u64::try_from(bytes.len()).expect("offset fits u64"),
                next,
                "a read ends at its next offset"
            );
            if up_to_date || body.is_empty() {
                return bytes;
            }
        }
    }
}

/// One HTTP request, owned, so a task can send it while the leader dies.
struct Request {
    app: Router,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    async fn send(self) -> Answer {
        let headers = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let response = send(
            &self.app,
            "POST",
            &self.path,
            &headers,
            Body::from(self.body),
        )
        .await;
        answer(&response)
    }
}

fn answer<B>(response: &Response<B>) -> Answer {
    let parsed = ProducerAnswer::parse(
        response.status().as_u16(),
        response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    );
    match parsed {
        Ok(Some(answer)) => Answer::Producer(answer),
        Ok(None) | Err(_) => Answer::Unknown(response.status()),
    }
}

/// The plan of one `producer_repeats_are_exactly_once` run.
#[derive(Debug)]
struct RepeatsPlan {
    producers: u8,
    rounds: u64,
    /// `(round, producer)` whose write is in flight when the leader dies,
    /// with how long it travels first, in milliseconds. The leader is cut
    /// off from the followers meanwhile, so the write dies with it.
    kills: BTreeMap<(u64, u8), u64>,
    /// Writes whose client gives up while replication is held back: the
    /// write commits after the client stopped waiting.
    timeouts: BTreeSet<(u64, u8)>,
    /// Writes resent at once, early.
    duplicates: BTreeSet<(u64, u8)>,
    /// Writes resent two rounds later.
    delayed: BTreeSet<(u64, u8)>,
    /// Writes a clone sends another body for after the original applied.
    clones_after: BTreeSet<(u64, u8)>,
    /// Writes a clone sends another body for before the original.
    clones_before: BTreeSet<(u64, u8)>,
}

impl RepeatsPlan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x7772_6974_656b_6579);
        let producers = 3_u8;
        let rounds = 6_u64;
        let mut plan = Self {
            producers,
            rounds,
            kills: BTreeMap::new(),
            timeouts: BTreeSet::new(),
            duplicates: BTreeSet::new(),
            delayed: BTreeSet::new(),
            clones_after: BTreeSet::new(),
            clones_before: BTreeSet::new(),
        };
        while plan.kills.len() < 2 {
            let round = rng.next_bounded(rounds);
            let producer = u8::try_from(rng.next_bounded(u64::from(producers))).expect("small");
            plan.kills
                .insert((round, producer), 1_u64.saturating_add(rng.next_bounded(3)));
        }
        for round in 0..rounds {
            for producer in 0..producers {
                let write = (round, producer);
                match rng.next_bounded(8) {
                    0 | 1 => {
                        plan.duplicates.insert(write);
                    }
                    2 => {
                        plan.delayed.insert(write);
                    }
                    3 => {
                        plan.clones_after.insert(write);
                    }
                    4 if !plan.kills.contains_key(&write) => {
                        plan.clones_before.insert(write);
                    }
                    5 if !plan.kills.contains_key(&write) => {
                        plan.timeouts.insert(write);
                    }
                    _ => {}
                }
            }
        }
        // Every run exercises each path at least once.
        plan.duplicates.insert((0, 0));
        plan.delayed.insert((0, 1));
        plan.clones_after.insert((1, 0));
        if !plan.kills.contains_key(&(1, 1)) {
            plan.clones_before.insert((1, 1));
        }
        if !plan.kills.contains_key(&(2, 2)) {
            plan.clones_before.remove(&(2, 2));
            plan.timeouts.insert((2, 2));
        }
        plan
    }
}

/// A key's holder: its body and the receipt range, once one was given.
type Holder = (Label, Option<(u64, u64)>);

/// What the answers told about each key, checked as they come.
#[derive(Default)]
struct Book {
    /// The body each key holds, by its label without the clone bit, with
    /// its receipt range when one was given.
    holders: BTreeMap<(u8, u64), Holder>,
    /// Bodies refused with `key-conflict`.
    refused: BTreeSet<Label>,
    /// Bodies that got an applied receipt, by label.
    applied: BTreeMap<Label, u64>,
    repeats: u64,
}

impl Book {
    /// Records a receipt for `label`'s body under `key`, or fails.
    fn receipt(
        &mut self,
        trace: &mut SimTrace,
        label: Label,
        delivery: Delivery,
        range: Option<(u64, u64)>,
    ) {
        let slot = (label.producer, label.seq);
        if let Some((holder, held)) = self.holders.get(&slot) {
            if *holder != label {
                fail(
                    trace,
                    REPEATS_INVARIANT,
                    "receipt",
                    format!("a receipt for {label:?} under a key that holds {holder:?}"),
                );
            }
            if let (Some(held), Some(range)) = (held, range)
                && *held != range
            {
                fail(
                    trace,
                    REPEATS_INVARIANT,
                    "receipt",
                    format!("{label:?} got receipt {range:?} after {held:?}"),
                );
            }
        }
        if delivery == Delivery::Applied {
            let count = self.applied.entry(label).or_default();
            *count = count.saturating_add(1);
            if *count > 1 {
                fail(
                    trace,
                    REPEATS_INVARIANT,
                    "receipt",
                    format!("{label:?} applied {count} times"),
                );
            }
        } else {
            self.repeats = self.repeats.saturating_add(1);
        }
        let entry = self.holders.entry(slot).or_insert((label, range));
        if entry.1.is_none() {
            entry.1 = range;
        }
    }
}

/// Sends `key` until it gets a producer answer, giving a renumbering client's
/// retries new seqs.
async fn deliver(
    cluster: &Cluster,
    controls: Controls,
    next_seq: &mut BTreeMap<u8, u64>,
    mut key: Key,
    early: bool,
) -> (Key, ProducerAnswer) {
    for _ in 0..MAX_ATTEMPTS {
        let body = key.label.body();
        let request = cluster.request(key, &body, true, !controls.digest_off, early);
        match request.send().await {
            Answer::Producer(answer) => return (key, answer),
            Answer::Unknown(_) => {
                madsim::time::sleep(Duration::from_millis(20)).await;
                if controls.renumbering {
                    key.seq = take_seq(next_seq, key.label.producer);
                }
            }
        }
    }
    panic!("{key:?} got no producer answer in {MAX_ATTEMPTS} attempts");
}

fn take_seq(next_seq: &mut BTreeMap<u8, u64>, producer: u8) -> u64 {
    let next = next_seq.entry(producer).or_default();
    let seq = *next;
    *next = next.saturating_add(1);
    seq
}

/// Checks and records a producer's answer to `key`'s `label` body.
fn record(trace: &mut SimTrace, book: &mut Book, label: Label, answer: &ProducerAnswer) {
    match answer {
        ProducerAnswer::Receipt {
            delivery, receipt, ..
        } => book.receipt(
            trace,
            label,
            *delivery,
            Some((receipt.start().get(), receipt.next().get())),
        ),
        // An older write's repeat: the record keeps no range and no digest
        // for it (#581 section 2.7), so the answer says only that some body
        // applied under the key.
        ProducerAnswer::ReceiptEvicted { .. } => {
            if !book.holders.contains_key(&(label.producer, label.seq)) {
                fail(
                    trace,
                    REPEATS_INVARIANT,
                    "receipt_evicted",
                    format!("{label:?} got receipt-evicted under a key that never applied"),
                );
            }
            book.repeats = book.repeats.saturating_add(1);
        }
        ProducerAnswer::KeyConflict { .. } => {
            let held = book.holders.get(&(label.producer, label.seq));
            if held.is_some_and(|(holder, _)| *holder == label) {
                fail(
                    trace,
                    REPEATS_INVARIANT,
                    "key_conflict",
                    format!("{label:?} conflicts with its own write"),
                );
            }
            book.refused.insert(label);
        }
        other => fail(
            trace,
            REPEATS_INVARIANT,
            "answer",
            format!("{label:?} got {other:?}"),
        ),
    }
}

pub(super) async fn run_producer_repeats_are_exactly_once_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let plan = RepeatsPlan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let mut cluster = Cluster::start(&config, &mut trace).await;
    let mut book = Book::default();
    let mut next_seq = BTreeMap::new();
    let mut waiting: Vec<(u64, Key)> = Vec::new();
    let original = |producer, seq| Label {
        producer,
        epoch: 0,
        seq,
        clone: false,
    };
    for round in 0..plan.rounds {
        // The resender replays the writes it held back.
        let (due, later): (Vec<_>, Vec<_>) = waiting.into_iter().partition(|(at, _)| *at <= round);
        waiting = later;
        for (_, key) in due {
            let (key, answer) = deliver(&cluster, controls, &mut next_seq, key, false).await;
            if matches!(answer, ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            }) && !controls.renumbering
            {
                fail(
                    &mut trace,
                    REPEATS_INVARIANT,
                    "delayed_resend",
                    format!("a delayed resend of {key:?} applied again"),
                );
            }
            record(&mut trace, &mut book, key.label, &answer);
        }
        for producer in 0..plan.producers {
            let write = (round, producer);
            let seq = take_seq(&mut next_seq, producer);
            let label = original(producer, seq);
            let key = Key { label, seq };
            let clone = Key {
                label: Label {
                    clone: true,
                    ..label
                },
                seq,
            };
            if plan.clones_before.contains(&write) {
                let (_, answer) = deliver(&cluster, controls, &mut next_seq, clone, false).await;
                record(&mut trace, &mut book, clone.label, &answer);
            }
            let key = if let Some(travel_ms) = plan.kills.get(&write) {
                // The write is in flight when the leader dies: it reached the
                // leader, and may or may not have committed. Its outcome is
                // unknown, and the client resends the key.
                let body = label.body();
                cluster.isolate_leader();
                let in_flight = madsim::task::spawn(
                    cluster
                        .request(key, &body, true, !controls.digest_off, false)
                        .send(),
                );
                madsim::time::sleep(Duration::from_millis(*travel_ms)).await;
                let (old_leader, first) =
                    cluster.depose_isolated_leader(&mut trace, in_flight).await;
                let key = match first {
                    Answer::Producer(answer) => {
                        record(&mut trace, &mut book, label, &answer);
                        key
                    }
                    Answer::Unknown(_) if controls.renumbering => Key {
                        label,
                        seq: take_seq(&mut next_seq, producer),
                    },
                    Answer::Unknown(_) => key,
                };
                let (key, answer) = deliver(&cluster, controls, &mut next_seq, key, false).await;
                record(&mut trace, &mut book, label, &answer);
                cluster.restart(&mut trace, old_leader).await;
                key
            } else if plan.timeouts.contains(&write) {
                // The client stops waiting while replication is held back;
                // the write commits after, and the resend must not apply it
                // again.
                let body = label.body();
                cluster.policy.set_delay(Some(REPLICATION_HOLD));
                let first = madsim::time::timeout(
                    CLIENT_TIMEOUT,
                    cluster
                        .request(key, &body, true, !controls.digest_off, false)
                        .send(),
                )
                .await;
                cluster.policy.set_delay(None);
                trace.push(SimEvent::FaultApplied {
                    phase: "client_timeout".to_owned(),
                });
                let key = match first {
                    Ok(Answer::Producer(answer)) => {
                        record(&mut trace, &mut book, label, &answer);
                        key
                    }
                    Ok(Answer::Unknown(_)) | Err(_) if controls.renumbering => Key {
                        label,
                        seq: take_seq(&mut next_seq, producer),
                    },
                    Ok(Answer::Unknown(_)) | Err(_) => key,
                };
                let (key, answer) = deliver(&cluster, controls, &mut next_seq, key, false).await;
                record(&mut trace, &mut book, label, &answer);
                key
            } else {
                let (key, answer) = deliver(&cluster, controls, &mut next_seq, key, false).await;
                record(&mut trace, &mut book, label, &answer);
                key
            };
            if plan.duplicates.contains(&write) {
                let key = if controls.renumbering {
                    Key {
                        label,
                        seq: take_seq(&mut next_seq, producer),
                    }
                } else {
                    key
                };
                let (_, answer) = deliver(&cluster, controls, &mut next_seq, key, true).await;
                record(&mut trace, &mut book, label, &answer);
            }
            if plan.delayed.contains(&write) {
                waiting.push((round.saturating_add(2), key));
            }
            if plan.clones_after.contains(&write) {
                let (_, answer) = deliver(&cluster, controls, &mut next_seq, clone, false).await;
                record(&mut trace, &mut book, clone.label, &answer);
            }
        }
    }
    for (_, key) in waiting {
        let (key, answer) = deliver(&cluster, controls, &mut next_seq, key, false).await;
        record(&mut trace, &mut book, key.label, &answer);
    }

    // The stream holds each key's body exactly once, where its receipt
    // says, and no refused body.
    let bytes = cluster.read_all().await;
    let mut seen = BTreeMap::<Label, u64>::new();
    for (index, chunk) in bytes.chunks(BODY_LEN).enumerate() {
        let start = u64::try_from(index.saturating_mul(BODY_LEN)).expect("offset fits u64");
        let Some(label) = Label::parse(chunk) else {
            fail(
                &mut trace,
                REPEATS_INVARIANT,
                "final_read",
                format!("unreadable body at {start}"),
            );
        };
        if let Some(first) = seen.insert(label, start) {
            fail(
                &mut trace,
                REPEATS_INVARIANT,
                "final_read",
                format!("{label:?} is in the stream at {first} and at {start}"),
            );
        }
        if book.refused.contains(&label) {
            fail(
                &mut trace,
                REPEATS_INVARIANT,
                "final_read",
                format!("{label:?} was refused with key-conflict but is in the stream"),
            );
        }
        let held = book.holders.get(&(label.producer, label.seq));
        match held {
            Some((holder, range)) if *holder == label => {
                let end = start.saturating_add(u64::try_from(BODY_LEN).expect("small"));
                if range.is_some_and(|range| range != (start, end)) {
                    fail(
                        &mut trace,
                        REPEATS_INVARIANT,
                        "final_read",
                        format!("{label:?} is at {start}, its receipt said {range:?}"),
                    );
                }
            }
            other => fail(
                &mut trace,
                REPEATS_INVARIANT,
                "final_read",
                format!("{label:?} is in the stream, its key's answers said {other:?}"),
            ),
        }
    }
    for (slot, (holder, _)) in &book.holders {
        if !seen.contains_key(holder) {
            fail(
                &mut trace,
                REPEATS_INVARIANT,
                "final_read",
                format!("key {slot:?} answered for {holder:?}, which the stream does not hold"),
            );
        }
    }
    trace.push(SimEvent::ProducerRepeatsVerified {
        stream: config.stream.clone(),
        keys: u64::try_from(book.holders.len()).expect("count fits u64"),
        repeats: book.repeats,
        conflicts: u64::try_from(book.refused.len()).expect("count fits u64"),
        leader_kills: cluster.leader_kills,
    });
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: cluster
            .factory
            .leader_id(cluster.placement.raft_group_id)
            .unwrap_or_default(),
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

/// The epoch at or below the retired one that an answer reports as closed:
/// a claim's closure, or an `epoch-closed` refusal's.
fn reported_closure(answer: &ProducerAnswer) -> Option<u64> {
    let closed = match answer {
        ProducerAnswer::Receipt { epoch, .. } | ProducerAnswer::ReceiptEvicted { epoch, .. } => {
            epoch.closed_epoch()?
        }
        ProducerAnswer::EpochClosed(closure) => closure.closed_epoch(),
        _ => return None,
    };
    let epoch = closed.epoch.get();
    (epoch <= EXPIRY_EPOCH).then_some(epoch)
}

/// The plan of one `idle_expiry_cannot_reopen_an_epoch` run.
#[derive(Debug)]
struct ExpiryPlan {
    /// Writes of the stale producer before it goes idle.
    writes: u64,
    /// Whether `TidyStream` retires it, or its own next write.
    tidy: bool,
    /// Whether the leader dies after the retirement.
    kill: bool,
    /// Whether a writer without the token re-claims the retired epoch
    /// before the stale writer returns: the base protocol treats it as new,
    /// and its record opens below the floor.
    reclaim: bool,
}

impl ExpiryPlan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6964_6c65_6578_7069);
        Self {
            writes: 2_u64.saturating_add(rng.next_bounded(3)),
            tidy: rng.next_bounded(2) == 0,
            kill: rng.next_bounded(4) != 0,
            reclaim: rng.next_bounded(2) == 0,
        }
    }
}

pub(super) async fn run_idle_expiry_cannot_reopen_an_epoch_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    const STALE: u8 = 25;
    const EPOCH: u64 = EXPIRY_EPOCH;
    let plan = ExpiryPlan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let mut cluster = Cluster::start(&config, &mut trace).await;
    let label = |epoch, seq| Label {
        producer: STALE,
        epoch,
        seq,
        clone: false,
    };
    // The producer claims epoch 3 and writes, then goes idle.
    for seq in 0..plan.writes {
        let key = Key {
            label: label(EPOCH, seq),
            seq,
        };
        let body = key.label.body();
        match cluster.request(key, &body, true, true, false).send().await {
            Answer::Producer(ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            }) => {}
            _ => panic!("the producer's write {seq} applies"),
        }
    }
    let retired_at = u64::try_from(cluster.read_all().await.len()).expect("offset fits u64");
    let idle_now = cluster
        .now_ms
        .load(Ordering::Relaxed)
        .saturating_add(IDLE_EXPIRY_MS);
    cluster.now_ms.store(idle_now, Ordering::Relaxed);
    trace.push(SimEvent::FaultApplied {
        phase: "producer_idle".to_owned(),
    });
    if plan.tidy {
        cluster
            .runtime
            .tidy_streams_all_groups_once(16, idle_now)
            .await
            .expect("tidy pass");
        trace.push(SimEvent::FaultApplied {
            phase: "tidy_stream".to_owned(),
        });
    }
    if plan.reclaim {
        // Another writer under the same id, without the token, writes the
        // retired epoch again. Its bodies are clones, told apart from the
        // stale writer's.
        for seq in 0..2 {
            let key = Key {
                label: Label {
                    clone: true,
                    ..label(EPOCH, seq)
                },
                seq,
            };
            let body = key.label.body();
            match cluster.request(key, &body, false, true, false).send().await {
                Answer::Producer(ProducerAnswer::Receipt {
                    delivery: Delivery::Applied,
                    ..
                }) => {}
                _ => panic!("an untokened re-claim of a forgotten producer applies as new"),
            }
        }
        trace.push(SimEvent::FaultApplied {
            phase: "untokened_reclaim".to_owned(),
        });
    }
    if plan.kill {
        // The leader dies, so the record and its marker come back from the
        // log on the new leader and on the restarted node.
        let old_leader = cluster.kill_leader(&mut trace, "after_idle").await;
        cluster.restart(&mut trace, old_leader).await;
    }

    // A stale writer of the producer resends, re-claims and continues the
    // retired epoch, and writes an older one. None of it applies.
    let attempts = [
        (EPOCH, plan.writes),
        (EPOCH, plan.writes.saturating_sub(1)),
        (EPOCH, 0),
        (EPOCH.saturating_sub(1), 0),
        (EPOCH, plan.writes),
    ];
    let mut refused = 0_u64;
    for (epoch, seq) in attempts {
        let key = Key {
            label: label(epoch, seq),
            seq,
        };
        let body = key.label.body();
        let answer = cluster
            .request(key, &body, !controls.floor_off, true, false)
            .send()
            .await;
        if let Answer::Producer(answer) = &answer
            && let Some(closed) = reported_closure(answer)
        {
            fail(
                &mut trace,
                EXPIRY_INVARIANT,
                "stale_write",
                format!(
                    "epoch {epoch} seq {seq} was told epoch {closed} closed, which the stream forgot"
                ),
            );
        }
        match answer {
            Answer::Producer(ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            }) => fail(
                &mut trace,
                EXPIRY_INVARIANT,
                "stale_write",
                format!("epoch {epoch} seq {seq} of a retired producer applied"),
            ),
            Answer::Producer(_) => refused = refused.saturating_add(1),
            Answer::Unknown(status) => panic!("stale write answered {status}"),
        }
    }
    // A claim above the floor applies.
    let claim = Key {
        label: label(EPOCH.saturating_add(1), 0),
        seq: 0,
    };
    let body = claim.label.body();
    match cluster
        .request(claim, &body, true, true, false)
        .send()
        .await
    {
        Answer::Producer(
            answer @ ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            },
        ) => {
            if let Some(closed) = reported_closure(&answer) {
                fail(
                    &mut trace,
                    EXPIRY_INVARIANT,
                    "claim_above_floor",
                    format!("the claim above the floor reported forgotten epoch {closed} closed"),
                );
            }
        }
        Answer::Producer(other) => fail(
            &mut trace,
            EXPIRY_INVARIANT,
            "claim_above_floor",
            format!("a claim above the floor answered {other:?}"),
        ),
        Answer::Unknown(status) => panic!("claim above the floor answered {status}"),
    }
    // The stale writer resends its retired epoch once more, after the claim:
    // still no answer reports that epoch closed.
    let resend = Key {
        label: label(EPOCH, plan.writes.saturating_sub(1)),
        seq: plan.writes.saturating_sub(1),
    };
    let body = resend.label.body();
    match cluster
        .request(resend, &body, true, true, false)
        .send()
        .await
    {
        Answer::Producer(answer) => {
            if let Some(closed) = reported_closure(&answer) {
                fail(
                    &mut trace,
                    EXPIRY_INVARIANT,
                    "resend_after_claim",
                    format!("a resend after the claim was told epoch {closed} closed"),
                );
            }
            if matches!(answer, ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            }) {
                fail(
                    &mut trace,
                    EXPIRY_INVARIANT,
                    "resend_after_claim",
                    "a resend of the retired epoch applied after the claim".to_owned(),
                );
            }
        }
        Answer::Unknown(status) => panic!("resend after the claim answered {status}"),
    }
    // The stream holds no write of the retired epoch past its retirement,
    // but the untokened re-claim's own.
    let bytes = cluster.read_all().await;
    let tail = bytes
        .get(usize::try_from(retired_at).expect("offset fits usize")..)
        .unwrap_or_default();
    for chunk in tail.chunks(BODY_LEN) {
        if Label::parse(chunk).is_some_and(|written| written.epoch <= EPOCH && !written.clone) {
            fail(
                &mut trace,
                EXPIRY_INVARIANT,
                "final_read",
                format!(
                    "{:?} was written after its epoch retired",
                    Label::parse(chunk)
                ),
            );
        }
    }
    trace.push(SimEvent::IdleExpiryVerified {
        stream: config.stream.clone(),
        retired_epoch: EPOCH,
        refused,
        tidied: plan.tidy,
        leader_kills: cluster.leader_kills,
    });
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: cluster
            .factory
            .leader_id(cluster.placement.raft_group_id)
            .unwrap_or_default(),
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::EXPIRY_INVARIANT;
    use super::Label;
    use super::REPEATS_INVARIANT;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    #[test]
    fn labels_round_trip() {
        let label = Label {
            producer: 2,
            epoch: 3,
            seq: 14,
            clone: true,
        };
        assert_eq!(Label::parse(&label.body()), Some(label));
    }

    fn config(seed: u64, scenario: SimScenario) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", scenario.slug()))
    }

    /// Runs `scenario` with `controls` and asserts `invariant` failed.
    fn assert_fails(scenario: SimScenario, controls: Controls, invariant: &str) {
        let _guard = sim_test_guard();
        SimTrace::clear_recorded();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_write_keys_with_controls_report(
                config(365, scenario),
                scenario,
                controls,
            )
        });
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{controls:?} left {scenario:?} passing");
        assert!(
            invariant_failed(&SimTrace::last_recorded(), invariant),
            "{controls:?} did not fail `{invariant}`"
        );
    }

    #[test]
    fn producer_repeats_are_exactly_once_replays() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::ProducerRepeatsExactlyOnce;
        let first = ThreeNodeRaftSim::run_write_keys_with_controls_report(
            config(365, scenario),
            scenario,
            Controls::default(),
        );
        let second = ThreeNodeRaftSim::run_write_keys_with_controls_report(
            config(365, scenario),
            scenario,
            Controls::default(),
        );
        assert_eq!(first, second);
    }

    #[test]
    fn idle_expiry_cannot_reopen_an_epoch_replays() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::IdleExpiryCannotReopenAnEpoch;
        for seed in [370, 371] {
            let first = ThreeNodeRaftSim::run_write_keys_with_controls_report(
                config(seed, scenario),
                scenario,
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_write_keys_with_controls_report(
                config(seed, scenario),
                scenario,
                Controls::default(),
            );
            assert_eq!(first, second);
        }
    }

    #[test]
    fn without_digests_a_clone_gets_the_originals_receipt() {
        assert_fails(
            SimScenario::ProducerRepeatsExactlyOnce,
            Controls::DIGEST_OFF,
            REPEATS_INVARIANT,
        );
    }

    #[test]
    fn a_renumbering_client_applies_a_write_twice() {
        assert_fails(
            SimScenario::ProducerRepeatsExactlyOnce,
            Controls::RENUMBERING,
            REPEATS_INVARIANT,
        );
    }

    #[test]
    fn without_the_floor_a_stale_writer_reopens_its_epoch() {
        assert_fails(
            SimScenario::IdleExpiryCannotReopenAnEpoch,
            Controls::FLOOR_OFF,
            EXPIRY_INVARIANT,
        );
    }
}
