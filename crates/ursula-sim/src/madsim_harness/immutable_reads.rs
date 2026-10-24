//! Immutable reads under faults (#581 section 10, DST items 2 and 4): chunk
//! reads of one stream through the leader's runtime, through each
//! follower's engine and through the HTTP chunk route, and immutable
//! snapshot reads through the HTTP route, against a three-node OpenRaft
//! group, while the leader dies mid-read, followers lag behind partitions,
//! the stream is closed, deleted and recreated, snapshots are published and
//! retention advances, and the group is restored from a backup. A last step
//! ([`super::immutable_reads_loss`]) reads through every replica of a
//! journal-backed group across an accepted loss and its recovery plan.
//!
//! Invariant `marked_chunks_never_contradict`: a cache-like observer keeps
//! the first marked answer of every chunk URL, `(token, offset)`, as a
//! cache in front of the stream would, across every replica, the restore
//! and the plan. Every later answer for that URL carries the same bytes,
//! `Stream-Next-Offset` and closure, or fails as the protocol allows: another
//! token (`412`), a forward to the leader (`3xx`, `503`), or, once the URL's
//! token is retired, a `404`, `410` or `416`. Under the URL's own token a
//! `404` or `416` contradicts the marked answer, and so does a `410` until
//! retention passed the offset. Every later `200` of an immutable snapshot
//! URL holds the digest and body of its first. Engine reads mark with
//! [`ursula_runtime::ReadStreamResponse::is_immutable`], the predicate the
//! chunk route renders `Stream-Immutable` from, and the HTTP reads take the
//! marker the route rendered. A follower read takes the engine's own route,
//! so a forward is never answered locally.
//!
//! Every run starts with a boundary probe: a chunk that ends exactly at the
//! open tail is read through the leader and a follower, and the stream is
//! then closed at that offset. Marking either answer would contradict the
//! closed tail's `Stream-Closed`.
//!
//! A kill of the leader with a chunk read in flight is followed by a read
//! through the new leader, which may not have applied what the old one
//! marked: the leader's barrier for what may hold only for a lagging state
//! keeps it from answering short, `404`, `410` or `416`. A leader deposed
//! without knowing it may lag too. Its barrier round finds no quorum, and
//! once no quorum acknowledged it within its lease it no longer leads
//! (`LeaderRole::Lapsed`), so it forwards or refuses what it cannot mark.
//! Either defence alone keeps it from answering short.
//!
//! [`Controls`] turn off one server defence each, through
//! [`ursula_stream::sim_controls`], for the tests that show the invariant
//! catches its loss.

use std::collections::BTreeMap;

use axum::http::Response;
use ursula_headers::ChunkMark;
use ursula_headers::ChunkRead;
use ursula_headers::ImmutableResource;
use ursula_headers::ImmutableSnapshotRead;
use ursula_headers::ResourceKind;
use ursula_headers::StreamHead;
use ursula_headers::value::Incarnation;
use ursula_headers::value::Offset;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupReadRoute;
use ursula_runtime::ReadEnd;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::StreamErrorCode;
use ursula_shard::ShardPlacement;

use super::Body;
use super::Duration;
use super::MadsimRuntimeRaftNetworkFactory;
use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::StatusCode;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::history::ImportAnswer;
use super::history::export;
use super::history::import;
use super::history::wait_followers_joined;
use super::http::body_bytes;
use super::http::send;
use super::write_keys::Cluster;

pub(super) const INVARIANT: &str = "marked_chunks_never_contradict";
const CONTENT_TYPE: &str = "application/octet-stream";
/// The simulation's chunk grid. Marking does not depend on its size, so a
/// small grid puts many chunk boundaries inside a short stream.
const GRID: u64 = 16;
const NOW_MS: u64 = 1_000;
/// The server's chunk grid, which the HTTP chunk route reads on.
const HTTP_GRID: u64 = 4 * 1024 * 1024;

/// Server defences a negative control turns off. Production runs use the
/// default, with every defence on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// Every answer at an open tail is marked.
    open_tail_marked: bool,
    /// A follower answers every chunk from its own state.
    follower_mark_check_off: bool,
    /// A leader answers a chunk it cannot mark, and a `416`, from its own
    /// applied state, which after a leader change may lag.
    leader_barrier_off: bool,
    /// The leader predicate reads OpenRaft's server state alone, without
    /// the node fence and the leader lease, so a leader cut off from its
    /// quorum keeps leading after its lease ends.
    fenced_leadership_off: bool,
}

#[cfg(test)]
impl Controls {
    pub(super) const OPEN_TAIL_MARKED: Self = Self {
        open_tail_marked: true,
        follower_mark_check_off: false,
        leader_barrier_off: false,
        fenced_leadership_off: false,
    };
    pub(super) const FOLLOWER_MARK_CHECK_OFF: Self = Self {
        open_tail_marked: false,
        follower_mark_check_off: true,
        leader_barrier_off: false,
        fenced_leadership_off: false,
    };
    pub(super) const LEADER_BARRIER_OFF: Self = Self {
        open_tail_marked: false,
        follower_mark_check_off: false,
        leader_barrier_off: true,
        fenced_leadership_off: false,
    };
    pub(super) const FENCED_LEADERSHIP_OFF: Self = Self {
        open_tail_marked: false,
        follower_mark_check_off: false,
        leader_barrier_off: false,
        fenced_leadership_off: true,
    };
    /// Both defences of a leader deposed without knowing it.
    pub(super) const DEPOSED_LEADER_UNDEFENDED: Self = Self {
        open_tail_marked: false,
        follower_mark_check_off: false,
        leader_barrier_off: true,
        fenced_leadership_off: true,
    };
}

/// The defences [`Controls`] turned off for this simulation, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_defences(controls);
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::default());
    }
}

fn set_defences(controls: Controls) {
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::set_off;
    set_off(Defence::ImmutableOpenTail, controls.open_tail_marked);
    set_off(Defence::FollowerMarkCheck, controls.follower_mark_check_off);
    set_off(Defence::ImmutableLeaderBarrier, controls.leader_barrier_off);
    set_off(Defence::FencedLeadership, controls.fenced_leadership_off);
}

pub(super) fn fail(trace: &mut SimTrace, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: INVARIANT.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{INVARIANT}` failed after `{after_event}`: {message}");
}

/// The length of a chunk read from `offset`: to the end of its chunk.
pub(super) fn chunk_len(offset: u64) -> usize {
    usize::try_from(GRID.saturating_sub(offset % GRID)).expect("the grid fits usize")
}

pub(super) fn chunk_request(
    stream: &ursula_shard::BucketStreamId,
    offset: u64,
) -> ReadStreamRequest {
    ReadStreamRequest {
        stream_id: stream.clone(),
        offset,
        max_len: chunk_len(offset),
        now_ms: NOW_MS,
        leader_only: false,
        read_index: None,
        follower: FollowerRead::Immutable,
    }
}

/// What an answer for a chunk URL says, compared across answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Answer {
    next_offset: u64,
    closed: bool,
    bytes: Vec<u8>,
}

impl Answer {
    fn of(response: &ReadStreamResponse) -> Self {
        Self {
            next_offset: response.next_offset,
            closed: response.end() == ReadEnd::ClosedTail,
            bytes: response.payload.clone(),
        }
    }
}

/// How one read ended.
pub(super) enum Read {
    Answered(ReadStreamResponse),
    /// A `404` from the state that served it.
    NotFound,
    /// A `410` from the state that served it.
    Gone,
    /// A `416` from the state that served it.
    BeyondTail,
    /// Any other failure the protocol allows: another stream state, no
    /// leader, a forward to the leader.
    Refused,
}

impl Read {
    /// A read that failed with `error`, by the protocol's answer.
    pub(super) fn failed(code: Option<StreamErrorCode>) -> Self {
        match code {
            Some(StreamErrorCode::StreamNotFound) => Self::NotFound,
            Some(StreamErrorCode::StreamGone) => Self::Gone,
            Some(StreamErrorCode::OffsetOutOfRange) => Self::BeyondTail,
            _ => Self::Refused,
        }
    }
}

/// One answer to a chunk URL, as the observer judges it.
enum Outcome {
    /// An answer under the URL's token, and whether it was marked.
    Answered {
        answer: Answer,
        marked: bool,
    },
    NotFound,
    Gone,
    BeyondTail,
    /// Another token (a `412`), or a refusal the protocol allows anyway.
    Refused,
}

/// The cache-like observer: the first marked answer of every chunk URL, and
/// how far retention moved under each token.
#[derive(Default)]
pub(super) struct Observer {
    marked: BTreeMap<(u64, u64), Answer>,
    /// The marked URLs in the order they were marked. Token values differ
    /// between runs in one process (creation times are unique across it),
    /// so sweeps follow this order, never the map's.
    order: Vec<(u64, u64)>,
    retained: BTreeMap<u64, u64>,
    pub(super) answers: u64,
    pub(super) checked: u64,
}

impl Observer {
    pub(super) fn marked(&self) -> Vec<(u64, u64)> {
        self.order.clone()
    }

    pub(super) fn marked_count(&self) -> u64 {
        u64::try_from(self.marked.len()).expect("few URLs")
    }

    /// Records that retention moved to `offset` under `token`.
    fn retain(&mut self, token: u64, offset: u64) {
        let retained = self.retained.entry(token).or_default();
        *retained = (*retained).max(offset);
    }

    /// Checks an engine read of the chunk URL `(token, offset)` against the
    /// marked answer the observer keeps for it, and keeps it if it is the
    /// first marked one. `current` is the stream's token when the read was
    /// sent.
    pub(super) fn observe(
        &mut self,
        trace: &mut SimTrace,
        phase: &str,
        (token, offset): (u64, u64),
        current: u64,
        read: Read,
    ) {
        let outcome = match read {
            Read::Answered(response) => {
                // Another token is the `412` of the URL's precondition.
                if response.incarnation != token {
                    return;
                }
                if response.offset != offset {
                    fail(
                        trace,
                        phase,
                        format!(
                            "chunk ({token}, {offset}) answered from offset {}",
                            response.offset
                        ),
                    );
                }
                Outcome::Answered {
                    marked: response.is_immutable(chunk_len(offset)),
                    answer: Answer::of(&response),
                }
            }
            Read::NotFound => Outcome::NotFound,
            Read::Gone => Outcome::Gone,
            Read::BeyondTail => Outcome::BeyondTail,
            Read::Refused => Outcome::Refused,
        };
        self.judge(trace, phase, (token, offset), current, outcome);
    }

    fn judge(
        &mut self,
        trace: &mut SimTrace,
        phase: &str,
        (token, offset): (u64, u64),
        current: u64,
        outcome: Outcome,
    ) {
        let kept = self.marked.get(&(token, offset));
        // A failure the URL's own token decided contradicts a marked answer:
        // the stream exists, holds the offset, and retained it.
        let contradiction = match &outcome {
            Outcome::NotFound | Outcome::BeyondTail => token == current && kept.is_some(),
            Outcome::Gone => {
                token == current
                    && kept.is_some()
                    && self.retained.get(&token).copied().unwrap_or(0) <= offset
            }
            Outcome::Answered { .. } | Outcome::Refused => false,
        };
        if contradiction {
            let status = match outcome {
                Outcome::NotFound => "404",
                Outcome::Gone => "410",
                _ => "416",
            };
            fail(
                trace,
                phase,
                format!(
                    "chunk ({token}, {offset}) was marked, and a later read under the same \
                     token answered {status}"
                ),
            );
        }
        let Outcome::Answered { answer, marked } = outcome else {
            return;
        };
        self.answers = self.answers.saturating_add(1);
        match kept {
            Some(kept) => {
                self.checked = self.checked.saturating_add(1);
                if *kept != answer {
                    fail(
                        trace,
                        phase,
                        format!(
                            "chunk ({token}, {offset}) was marked as {kept:?}, and a later \
                             answer for the same URL is {answer:?}"
                        ),
                    );
                }
            }
            None if marked => {
                self.marked.insert((token, offset), answer);
                self.order.push((token, offset));
            }
            None => {}
        }
    }
}

/// The first answer of every immutable snapshot URL, `(token, offset)`:
/// every later `200` holds the same digest and body.
#[derive(Default)]
struct SnapshotObserver {
    kept: BTreeMap<(u64, u64), (String, Vec<u8>)>,
    /// The kept URLs in the order they were kept (see [`Observer`]).
    order: Vec<(u64, u64)>,
    checked: u64,
}

impl MadsimRuntimeRaftNetworkFactory {
    /// The follower replicas of the group, by node id.
    fn follower_ids(&self) -> Vec<u64> {
        self.follower_histories()
            .into_iter()
            .map(|(node_id, _)| node_id)
            .collect()
    }

    /// `request` routed by follower `node_id`'s own engine, as the runtime
    /// on that node would route it. A read the follower forwards to the
    /// leader is not answered here: the leader's answer is observed through
    /// the leader's runtime.
    async fn follower_read(
        &self,
        node_id: u64,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Read {
        // Taken out for the read, so no lock is held across it, and put
        // back. Only this task changes the followers while it runs.
        let taken = {
            let mut followers = self
                .followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            followers
                .iter()
                .position(|(candidate, _)| *candidate == node_id)
                .map(|index| followers.remove(index))
        };
        let Some((node_id, mut engine)) = taken else {
            return Read::Refused;
        };
        let read = match engine.route_read_stream(request, placement).await {
            Ok(GroupReadRoute::Local(parts)) => match parts.into_response().await {
                Ok(response) => Read::Answered(response),
                Err(_unreadable) => Read::Refused,
            },
            Ok(GroupReadRoute::Leader(_forwarded)) => Read::Refused,
            Err(error) => Read::failed(error.code()),
        };
        self.followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((node_id, engine));
        read
    }
}

fn leader_answer(result: Result<ReadStreamResponse, ursula_runtime::RuntimeError>) -> Read {
    match result {
        Ok(response) => Read::Answered(response),
        Err(error) => Read::failed(error.stream_error_code()),
    }
}

fn header_u64<B>(response: &Response<B>, name: &str) -> Option<u64> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

/// HEAD of the stream, repeated while a leader change refuses it.
async fn head(cluster: &Cluster) -> StreamHead {
    for _ in 0..500 {
        let response = send(&cluster.app, "HEAD", &cluster.path, &[], Body::empty()).await;
        if response.status() == StatusCode::OK {
            return StreamHead::parse(
                response
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_bytes())),
            )
            .expect("a well-formed stream head");
        }
        madsim::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("HEAD of the stream never answered 200");
}

/// The stream the run reads, as the client knows it.
struct Stream {
    token: u64,
    tail: u64,
    closed: bool,
    /// Every token the stream has had, for reads of retired URLs.
    tokens: Vec<u64>,
}

impl Stream {
    async fn of(cluster: &Cluster) -> Self {
        let head = head(cluster).await;
        Self {
            token: head.incarnation().get(),
            tail: head.next_offset().get(),
            closed: head.closed(),
            tokens: vec![head.incarnation().get()],
        }
    }

    async fn refresh(&mut self, cluster: &Cluster) {
        let head = head(cluster).await;
        self.token = head.incarnation().get();
        self.tail = head.next_offset().get();
        self.closed = head.closed();
        if !self.tokens.contains(&self.token) {
            self.tokens.push(self.token);
        }
    }
}

/// One run's driver: the cluster, the stream, the observer and the trace.
struct Run {
    cluster: Cluster,
    stream_id: ursula_shard::BucketStreamId,
    stream: Stream,
    observer: Observer,
    /// Chunks read through the HTTP route, on the server's own grid.
    http: Observer,
    snapshots: SnapshotObserver,
    /// Every snapshot published, by token and offset.
    published: Vec<(u64, u64)>,
    trace: SimTrace,
    rng: SplitMix64,
    /// A follower cut off from the leader, so it lags.
    lagging: Option<u64>,
}

impl Run {
    async fn append(&mut self, len: u64) {
        let body: Vec<u8> = (0..len)
            .map(|_| u8::try_from(self.rng.next_bounded(256)).expect("a byte"))
            .collect();
        let response = send(
            &self.cluster.app,
            "POST",
            &self.cluster.path,
            &[("content-type", CONTENT_TYPE)],
            Body::from(body),
        )
        .await;
        if response.status().is_success()
            && let Some(next) = header_u64(&response, "stream-next-offset")
        {
            self.stream.tail = next;
        }
    }

    async fn close(&mut self) {
        let response = send(
            &self.cluster.app,
            "POST",
            &self.cluster.path,
            &[("stream-closed", "true")],
            Body::empty(),
        )
        .await;
        if response.status().is_success() {
            self.stream.closed = true;
        }
    }

    async fn recreate(&mut self) {
        // What a cache kept is checked once more while its token is current.
        self.sweep("before_recreate").await;
        self.until_answered("DELETE", &[], &[
            StatusCode::NO_CONTENT,
            StatusCode::NOT_FOUND,
        ])
        .await;
        self.until_answered("PUT", &[("content-type", CONTENT_TYPE)], &[
            StatusCode::CREATED,
            StatusCode::OK,
        ])
        .await;
        self.stream.refresh(&self.cluster).await;
        self.trace.push(SimEvent::StreamCreated {
            stream: self.stream_id.clone(),
        });
    }

    /// Sends a bodyless `method` to the stream until it answers one of
    /// `answers`: a leader change may refuse it with a retryable status, and
    /// a repeat of one that applied answers as if it had.
    async fn until_answered(
        &mut self,
        method: &str,
        headers: &[(&str, &str)],
        answers: &[StatusCode],
    ) {
        let mut last = None;
        for _ in 0..100 {
            let response = send(
                &self.cluster.app,
                method,
                &self.cluster.path,
                headers,
                Body::empty(),
            )
            .await;
            if answers.contains(&response.status()) {
                return;
            }
            last = Some(response.status());
            if response.status() == StatusCode::TEMPORARY_REDIRECT {
                self.follow_the_leader().await;
            }
            madsim::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("{method} of the stream never answered {answers:?}, last {last:?}");
    }

    /// The runtime hosts one replica's engine. When leadership moved off it,
    /// as when a healed follower's campaign deposed the leader, the runtime
    /// redirects every write: swap in the leader's engine, as a client's
    /// redirect would reach it.
    async fn follow_the_leader(&mut self) {
        self.heal();
        let old = self
            .cluster
            .kill_leader(&mut self.trace, "leadership_moved_off_the_runtime")
            .await;
        self.cluster.restart(&mut self.trace, old).await;
    }

    /// Publishes a snapshot at the tail, and advances retention to it when
    /// `retain`.
    async fn publish(&mut self, retain: bool) {
        let token = self.stream.token;
        let offset = self.stream.tail;
        let body: Vec<u8> = (0..8)
            .map(|_| u8::try_from(self.rng.next_bounded(256)).expect("a byte"))
            .collect();
        let response = send(
            &self.cluster.app,
            "PUT",
            &format!(
                "{}/snapshot/{}",
                self.cluster.path,
                Offset::from_u64(offset)
            ),
            &[("content-type", CONTENT_TYPE)],
            Body::from(body),
        )
        .await;
        if response.status() != StatusCode::NO_CONTENT {
            return;
        }
        self.published.push((token, offset));
        if !retain {
            return;
        }
        let response = send(
            &self.cluster.app,
            "PUT",
            &format!(
                "{}/retention/{}",
                self.cluster.path,
                Offset::from_u64(offset)
            ),
            &[],
            Body::empty(),
        )
        .await;
        if response.status() == StatusCode::NO_CONTENT {
            self.observer.retain(token, offset);
            self.http.retain(token, offset);
            self.trace.push(SimEvent::FaultApplied {
                phase: "retention_advanced".to_owned(),
            });
        }
    }

    /// A chunk read of `(token, offset)` through the HTTP chunk route, on
    /// the server's 4 MiB grid: the precondition, the token check against
    /// the leader and the marker as the route renders them.
    async fn read_http_chunk(&mut self, phase: &str, token: u64, offset: u64) {
        let resource = ImmutableResource {
            kind: ResourceKind::Chunk,
            incarnation: Incarnation::try_from(token).expect("a token"),
            offset: Offset::from_u64(offset),
        };
        let current = self.stream.token;
        let response = send(
            &self.cluster.app,
            "GET",
            &format!("{}{}", self.cluster.path, resource.path()),
            &[],
            Body::empty(),
        )
        .await;
        let outcome = match response.status() {
            StatusCode::OK => {
                let read = ChunkRead::parse(
                    response
                        .headers()
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_bytes())),
                )
                .unwrap_or_else(|err| fail(&mut self.trace, phase, format!("{err}")));
                if !read.answers(&resource) {
                    fail(
                        &mut self.trace,
                        phase,
                        format!("chunk {resource:?} was answered as {read:?}"),
                    );
                }
                let bytes = body_bytes(response).await.to_vec();
                if u64::try_from(bytes.len()).ok() != Some(read.len()) {
                    fail(
                        &mut self.trace,
                        phase,
                        format!("chunk {resource:?}: a body of {} bytes", bytes.len()),
                    );
                }
                Outcome::Answered {
                    answer: Answer {
                        next_offset: read.next().get(),
                        closed: read.mark() == ChunkMark::ClosedTail,
                        bytes,
                    },
                    marked: read.mark() != ChunkMark::Unmarked,
                }
            }
            StatusCode::NOT_FOUND => Outcome::NotFound,
            StatusCode::GONE => Outcome::Gone,
            StatusCode::RANGE_NOT_SATISFIABLE => Outcome::BeyondTail,
            _ => Outcome::Refused,
        };
        self.http
            .judge(&mut self.trace, phase, (token, offset), current, outcome);
    }

    /// An immutable snapshot read of `(token, offset)` through the HTTP
    /// route: every `200` for the URL holds the same digest and body.
    async fn read_http_snapshot(&mut self, phase: &str, token: u64, offset: u64) {
        let resource = ImmutableResource {
            kind: ResourceKind::Snapshot,
            incarnation: Incarnation::try_from(token).expect("a token"),
            offset: Offset::from_u64(offset),
        };
        let response = send(
            &self.cluster.app,
            "GET",
            &format!("{}{}", self.cluster.path, resource.path()),
            &[],
            Body::empty(),
        )
        .await;
        if response.status() != StatusCode::OK {
            // `404` once superseded (a snapshot read never answers `410`:
            // an offset that is not the latest snapshot's is a `404`), `412`
            // once retired, or a refusal.
            return;
        }
        let read = ImmutableSnapshotRead::parse(
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .unwrap_or_else(|err| fail(&mut self.trace, phase, format!("{err}")));
        if !read.answers(&resource) {
            fail(
                &mut self.trace,
                phase,
                format!("snapshot {resource:?} was answered as {read:?}"),
            );
        }
        let answer = (
            read.snapshot.digest.to_string(),
            body_bytes(response).await.to_vec(),
        );
        match self.snapshots.kept.get(&(token, offset)) {
            Some(kept) => {
                self.snapshots.checked = self.snapshots.checked.saturating_add(1);
                if *kept != answer {
                    fail(
                        &mut self.trace,
                        phase,
                        format!(
                            "snapshot ({token}, {offset}) answered digest {} and later {}",
                            kept.0, answer.0
                        ),
                    );
                }
            }
            None => {
                self.snapshots.kept.insert((token, offset), answer);
                self.snapshots.order.push((token, offset));
            }
        }
    }

    fn pick_snapshot(&mut self) -> Option<(u64, u64)> {
        let count = u64::try_from(self.published.len()).expect("few snapshots");
        if count == 0 {
            return None;
        }
        // Mostly the latest, which a reader would ask for.
        let index = if self.rng.next_bounded(3) == 0 {
            self.rng.next_bounded(count)
        } else {
            count.saturating_sub(1)
        };
        self.published
            .get(usize::try_from(index).expect("an index"))
            .copied()
    }

    /// A chunk read of `(token, offset)` through the leader's runtime.
    async fn read_leader(&mut self, phase: &str, token: u64, offset: u64) {
        let request = chunk_request(&self.stream_id, offset);
        let current = self.stream.token;
        let read = leader_answer(self.cluster.runtime.read_stream(request).await);
        self.observer
            .observe(&mut self.trace, phase, (token, offset), current, read);
    }

    /// A chunk read of `(token, offset)` through follower `node_id`.
    async fn read_follower(&mut self, phase: &str, node_id: u64, token: u64, offset: u64) {
        let request = chunk_request(&self.stream_id, offset);
        let current = self.stream.token;
        let read = self
            .cluster
            .factory
            .follower_read(node_id, request, self.cluster.placement)
            .await;
        self.observer
            .observe(&mut self.trace, phase, (token, offset), current, read);
    }

    /// A chunk read through the leader while the leader dies.
    async fn read_across_a_leader_kill(&mut self, token: u64, offset: u64) {
        let request = chunk_request(&self.stream_id, offset);
        let current = self.stream.token;
        let runtime = self.cluster.runtime.clone();
        let in_flight = madsim::task::spawn(async move { runtime.read_stream(request).await });
        madsim::time::sleep(Duration::from_micros(self.rng.next_bounded(2_000))).await;
        self.heal();
        let old = self
            .cluster
            .kill_leader(&mut self.trace, "leader_killed_during_chunk_read")
            .await;
        let read = leader_answer(in_flight.await.expect("the read task"));
        self.observer.observe(
            &mut self.trace,
            "leader_killed_during_chunk_read",
            (token, offset),
            current,
            read,
        );
        // The new leader may not have applied what the old one served.
        self.read_leader("read_after_leader_change", token, offset)
            .await;
        self.cluster.restart(&mut self.trace, old).await;
    }

    fn lag(&mut self, node_id: u64) {
        if let Some(leader) = self
            .cluster
            .factory
            .leader_id(self.cluster.placement.raft_group_id)
        {
            self.cluster.policy.partition_bidirectional(leader, node_id);
            self.lagging = Some(node_id);
            self.trace.push(SimEvent::FaultApplied {
                phase: "follower_lagging".to_owned(),
            });
        }
    }

    fn heal(&mut self) {
        if self.lagging.take().is_some() {
            self.cluster.heal();
        }
    }

    /// Waits until follower `node_id` serves the stream up to `tail`, and
    /// closed there when `closed`.
    async fn wait_applied(&mut self, node_id: u64, tail: u64, closed: bool) {
        let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut request = chunk_request(&self.stream_id, tail.saturating_sub(1));
            request.follower = FollowerRead::Prefix;
            request.max_len = 1;
            if let Read::Answered(response) = self
                .cluster
                .factory
                .follower_read(node_id, request, self.cluster.placement)
                .await
                && response.next_offset == tail
                && response.incarnation == self.stream.token
                && (response.end() == ReadEnd::ClosedTail) == closed
            {
                return;
            }
            assert!(
                madsim::time::Instant::now() < deadline,
                "follower {node_id} never applied the stream to {tail}"
            );
            madsim::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A chunk that ends exactly at the open tail, read through the leader
    /// and a follower that holds the same tail, and the stream then closed
    /// at that offset: neither answer may be marked.
    async fn boundary_probe(&mut self) {
        let boundary = GRID.saturating_mul(2);
        self.append(boundary.saturating_sub(self.stream.tail)).await;
        let follower = *self
            .cluster
            .factory
            .follower_ids()
            .first()
            .expect("a follower");
        self.wait_applied(follower, boundary, false).await;
        let token = self.stream.token;
        self.read_leader("boundary_probe_leader", token, GRID).await;
        self.read_follower("boundary_probe_follower", follower, token, GRID)
            .await;
        self.close().await;
        self.read_leader("boundary_probe_closed", token, GRID).await;
        self.wait_applied(follower, boundary, true).await;
        self.read_follower("boundary_probe_closed_follower", follower, token, GRID)
            .await;
        self.recreate().await;
        // One chunk of the server's own grid, and a little more, for the
        // HTTP route's reads.
        self.append(HTTP_GRID.saturating_add(37)).await;
        let token = self.stream.token;
        for offset in [0, 5, HTTP_GRID] {
            self.read_http_chunk("http_chunk_probe", token, offset)
                .await;
        }
        // A snapshot of the stream, read twice through the immutable route.
        self.publish(false).await;
        if let Some((token, offset)) = self.published.last().copied() {
            for _ in 0..2 {
                self.read_http_snapshot("http_snapshot_probe", token, offset)
                    .await;
            }
        }
    }

    /// Restores the group from a backup into a new deployment behind the
    /// same URLs: a history event. The observer keeps what it marked.
    async fn restore(&mut self, config: &ThreeNodeRaftSimConfig) {
        self.heal();
        let backup = export(&self.cluster).await;
        let target = Cluster::start_empty(config, &mut self.trace).await;
        wait_followers_joined(&target).await;
        match import(&target, &backup).await {
            ImportAnswer::Answered(_) => {}
            ImportAnswer::Conflict | ImportAnswer::Unknown => {
                panic!("the restore of a fresh group answers")
            }
        }
        self.cluster = target;
        self.stream.refresh(&self.cluster).await;
        self.trace.push(SimEvent::FaultApplied {
            phase: "restored_from_backup".to_owned(),
        });
    }

    fn pick_token(&mut self) -> u64 {
        let tokens = &self.stream.tokens;
        if tokens.len() > 1 && self.rng.next_bounded(5) == 0 {
            let index = usize::try_from(
                self.rng
                    .next_bounded(u64::try_from(tokens.len()).expect("few tokens")),
            )
            .expect("an index");
            tokens.get(index).copied().unwrap_or(self.stream.token)
        } else {
            self.stream.token
        }
    }

    fn pick_offset(&mut self) -> u64 {
        self.rng.next_bounded(self.stream.tail.saturating_add(2))
    }

    fn pick_follower(&mut self) -> Option<u64> {
        let followers = self.cluster.factory.follower_ids();
        let count = u64::try_from(followers.len()).expect("few followers");
        if count == 0 {
            return None;
        }
        let index = usize::try_from(self.rng.next_bounded(count)).expect("an index");
        followers.get(index).copied()
    }

    /// Reads every marked URL again, through the leader and every follower.
    async fn sweep(&mut self, phase: &str) {
        self.heal();
        for (token, offset) in self.observer.marked() {
            self.read_leader(phase, token, offset).await;
            for follower in self.cluster.factory.follower_ids() {
                self.read_follower(phase, follower, token, offset).await;
            }
        }
        for (token, offset) in self.http.marked() {
            self.read_http_chunk(phase, token, offset).await;
        }
        let snapshots = self.snapshots.order.clone();
        for (token, offset) in snapshots {
            self.read_http_snapshot(phase, token, offset).await;
        }
    }
}

/// The plan of one run.
#[derive(Debug)]
struct Plan {
    steps: u64,
    /// The step before which the group is restored, if any.
    restore_at: Option<u64>,
}

impl Plan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x696d_6d75_7461_626c);
        let steps = 80_u64.saturating_add(rng.next_bounded(40));
        Self {
            steps,
            restore_at: (rng.next_bounded(3) != 0).then(|| rng.next_bounded(steps)),
        }
    }
}

pub(super) async fn run_marked_chunks_never_contradict_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = Plan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let cluster = Cluster::start(&config, &mut trace).await;
    wait_followers_joined(&cluster).await;
    let stream = Stream::of(&cluster).await;
    let mut run = Run {
        cluster,
        stream_id: config.stream.clone(),
        stream,
        observer: Observer::default(),
        http: Observer::default(),
        snapshots: SnapshotObserver::default(),
        published: Vec::new(),
        trace,
        rng: SplitMix64::new(config.seed ^ 0x6368_756e_6b73_2121),
        lagging: None,
    };
    run.boundary_probe().await;
    for step in 0..plan.steps {
        if plan.restore_at == Some(step) {
            run.sweep("before_restore").await;
            run.restore(&config).await;
        }
        let roll = run.rng.next_bounded(100);
        match roll {
            0..=29 if !run.stream.closed => {
                let len = 1_u64.saturating_add(run.rng.next_bounded(GRID.saturating_add(8)));
                run.append(len).await;
            }
            0..=29 => run.recreate().await,
            30..=44 => {
                let (token, offset) = (run.pick_token(), run.pick_offset());
                run.read_leader("leader_chunk_read", token, offset).await;
            }
            45..=59 => {
                let (token, offset) = (run.pick_token(), run.pick_offset());
                if let Some(follower) = run.pick_follower() {
                    run.read_follower("follower_chunk_read", follower, token, offset)
                        .await;
                }
            }
            60..=64 => {
                let (token, offset) = (run.pick_token(), run.pick_offset());
                run.read_across_a_leader_kill(token, offset).await;
            }
            65..=71 => match run.lagging {
                Some(_) => run.heal(),
                None => {
                    if let Some(follower) = run.pick_follower() {
                        run.lag(follower);
                    }
                }
            },
            72..=74 if !run.stream.closed => run.close().await,
            75..=80 => {
                let (token, offset) = (run.pick_token(), run.pick_offset());
                run.read_http_chunk("http_chunk_read", token, offset).await;
            }
            81..=85 => {
                if let Some((token, offset)) = run.pick_snapshot() {
                    run.read_http_snapshot("http_snapshot_read", token, offset)
                        .await;
                }
            }
            86..=90 => {
                let retain = run.rng.next_bounded(2) == 0;
                run.publish(retain).await;
            }
            _ => run.recreate().await,
        }
    }
    run.sweep("final_sweep").await;
    let Run {
        cluster,
        mut observer,
        http,
        snapshots,
        mut trace,
        ..
    } = run;
    // A recovery plan after an accepted loss, on journal-backed replicas.
    let loss =
        super::immutable_reads_loss::accepted_loss_phase(config.seed, &mut observer, &mut trace)
            .await;
    trace.push(SimEvent::MarkedChunksVerified {
        stream: config.stream.clone(),
        marked: observer.marked_count(),
        answers: observer.answers,
        checked: observer.checked,
        http_marked: http.marked_count(),
        http_checked: http.checked,
        snapshots_checked: snapshots.checked,
        loss_checked: loss,
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
    use super::INVARIANT;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::MarkedChunksNeverContradict;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug()))
    }

    /// Runs the scenario at `seed` with `controls` and asserts the
    /// invariant failed after `step`, the read the lost defence decided.
    fn assert_fails(seed: u64, controls: Controls, step: &str) {
        let _guard = sim_test_guard();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_immutable_reads_with_controls_report(config(seed), controls)
        });
        std::panic::set_hook(previous_hook);
        let (failed, recorded) = (result.is_err(), SimTrace::last_recorded());
        assert!(failed, "{controls:?} left the scenario passing");
        assert!(
            invariant_failed(&recorded, INVARIANT),
            "{controls:?} did not fail `{INVARIANT}`"
        );
        let failed_after = recorded.events.iter().find_map(|event| match event {
            SimEvent::InvariantFailed {
                invariant,
                after_event,
                ..
            } if invariant == INVARIANT => Some(after_event.clone()),
            _ => None,
        });
        assert_eq!(
            failed_after.as_deref(),
            Some(step),
            "{controls:?} failed `{INVARIANT}` at another step"
        );
    }

    #[test]
    fn negative_control_marking_an_open_tail_contradicts_a_later_answer() {
        assert_fails(395, Controls::OPEN_TAIL_MARKED, "boundary_probe_closed");
    }

    #[test]
    fn negative_control_a_follower_that_marks_without_forwarding_contradicts_the_leader() {
        assert_fails(
            395,
            Controls::FOLLOWER_MARK_CHECK_OFF,
            "boundary_probe_closed",
        );
    }

    /// With the leader's barrier and the lease of the leader predicate both
    /// off, seed 920 fails the invariant at `before_power_loss`, in the
    /// accepted-loss step: the leader cut off from the others, deposed
    /// without knowing it, keeps leading past its lease and answers short of
    /// a chunk the new leader marked. With every defence on the seed passes
    /// (`marked_chunks_never_contradict_replays` runs it), and with either
    /// one on it passes too
    /// (`either_defence_against_a_deposed_leader_alone_suffices`).
    #[test]
    fn negative_control_a_deposed_leader_without_barrier_or_lease_contradicts_a_marked_chunk() {
        assert_fails(
            920,
            Controls::DEPOSED_LEADER_UNDEFENDED,
            "before_power_loss",
        );
    }

    /// Either defence of a leader deposed without knowing it alone keeps
    /// seed 920 passing, the run its negative control above fails. With the
    /// barrier off, the cut-off leader's lease has ended by the time the
    /// others elected a leader and wrote, so it no longer leads, and it
    /// forwards or refuses what it cannot mark. With the lease off, it still
    /// leads, and its barrier round finds no quorum. The loss step reads
    /// through every replica, the cut-off leader included, and each run must
    /// judge some of its answers: one that judged none would pass vacuously.
    #[test]
    fn either_defence_against_a_deposed_leader_alone_suffices() {
        let _guard = sim_test_guard();
        for controls in [
            Controls::LEADER_BARRIER_OFF,
            Controls::FENCED_LEADERSHIP_OFF,
        ] {
            // A run that fails the invariant panics.
            let report =
                ThreeNodeRaftSim::run_immutable_reads_with_controls_report(config(920), controls);
            let loss_checked = report
                .outcome
                .trace
                .events
                .iter()
                .find_map(|event| match event {
                    SimEvent::MarkedChunksVerified { loss_checked, .. } => Some(*loss_checked),
                    _ => None,
                })
                .expect("the run verified its chunks");
            assert!(
                loss_checked > 0,
                "{controls:?}: the loss step checked no answer"
            );
        }
    }

    #[test]
    fn marked_chunks_never_contradict_replays() {
        let _guard = sim_test_guard();
        // 920 is the seed of the barrier's negative control: with every
        // defence on it passes.
        for seed in (395..=399).chain([920]) {
            // Every run starts on a fresh thread (`run_with_madsim`, #638).
            let run = || ThreeNodeRaftSim::run_report(SCENARIO, config(seed));
            let first = run();
            let second = run();
            assert_eq!(first, second, "seed {seed}");
            // Every part of the run exercised its observer, so no coverage
            // goes vacuous.
            let verified = first
                .outcome
                .trace
                .events
                .iter()
                .find_map(|event| match event {
                    SimEvent::MarkedChunksVerified {
                        marked,
                        checked,
                        http_marked,
                        http_checked,
                        snapshots_checked,
                        loss_checked,
                        ..
                    } => Some([
                        *marked,
                        *checked,
                        *http_marked,
                        *http_checked,
                        *snapshots_checked,
                        *loss_checked,
                    ]),
                    _ => None,
                })
                .expect("the run verified its chunks");
            assert!(
                verified.iter().all(|count| *count > 0),
                "seed {seed}: {verified:?}"
            );
        }
    }
}
