//! A model of the Ursula 0.8 server, written from the text of #581 revision 2 and its addenda: an
//! oracle for the client, independent of the server's code. It shares only `ursula_headers`, so
//! header syntax cannot drift between the two, while the evaluation of every row is this model's
//! own reading of the contract.
//!
//! It models one Raft group: one history id, one clock, any number of streams. In scope: the
//! producer rows of §2.3.1 with the epoch floor and idle retirement, write identity by
//! `Producer-Digest`, contiguous receipts, tokens in [2^52, 2^53) that change on history events,
//! what a stream inherited after one (W5, §4.8), HEAD metadata on every answer (A1), the typed and
//! untyped `413`, create keyed by configuration, reads with the incarnation precondition (`412`,
//! `410`), snapshots keyed by offset and digest, snapshot reads in one byte range (`206`, `416`),
//! snapshots uploaded in parts (#618: minted part ids, the commit's key checked first, the parts
//! a final answer consumes, the one-day sweep on the model's clock), monotonic retention, and
//! scripted faults. Out of scope: immutable reads, the timing of answers before the body, the
//! producer cap, SSE and JSON streams.
//!
//! Requests come in through [`ModelServer::exchange`] (with faults) or [`ModelServer::handle`]
//! (without), in process ([`crate::transport::model::ModelTransport`]) or over HTTP/1.1
//! ([`http`]).
//!
//! Module map:
//!
//! - [`producer`]: the producer record and the evaluation of a producer write, row by row.
//! - [`stream`]: one stream's state.
//! - [`faults`]: scripted faults.
//! - [`http`]: the model over HTTP/1.1 on `std::net`, with `/__model/` control routes.

pub(crate) mod faults;
pub mod http;
pub(crate) mod producer;
pub(crate) mod stream;

#[cfg(test)]
pub(crate) mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

pub use http::ServeError;
pub use http::serve_forever;
use ursula_headers::ACCEPT_RANGES_BYTES;
use ursula_headers::ByteRange;
use ursula_headers::ClusterId;
use ursula_headers::ContentRange;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationMismatch;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::PartId;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::ProducerAnswer;
use ursula_headers::ProducerId;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::ProducerWrite;
use ursula_headers::Receipt;
use ursula_headers::Seq;
use ursula_headers::ServerIdentity;
use ursula_headers::SizeLimit;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRead;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamClosedConflict;
use ursula_headers::StreamHead;
use ursula_headers::names;

use self::faults::CommitRefusal;
use self::faults::Fault;
use self::faults::Faults;
use self::faults::Scripted;
use self::producer::Decision;
use self::producer::ProducerRecord;
use self::producer::evaluate;
use self::stream::Snapshot;
use self::stream::Stream;
use crate::codec::digest::SnapshotDigester;
use crate::codec::digest::producer_digest;
use crate::wire::CONTENT_TYPE;
use crate::wire::LOCATION;
use crate::wire::Method;
use crate::wire::PRODUCER_ID;
use crate::wire::RETRY_AFTER;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Target;

/// A node's one request limit (#581 §3.1).
pub(crate) const MAX_REQUEST_BYTES: u64 = 32 << 20;

/// The largest snapshot accepted in parts, with a cold store (#581 §6.3).
pub(crate) const MAX_SNAPSHOT_BYTES: u64 = 1 << 30;

/// How long a staged part waits for a commit before the sweep reclaims it (extensions §2.2).
const PART_GRACE_MS: u64 = 24 * 60 * 60 * 1000;

/// The base protocol's content type of a stream created without one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// The model's clock when it starts, in milliseconds since the Unix epoch.
const START_MS: u64 = 1_760_000_000_000;

const TOKEN_TAG: u64 = 1 << 52;
const TOKEN_MASK: u64 = (1 << 52) - 1;

/// The key a history id mixes into every token of its history (#581 §4.2).
fn history_key(history: u64) -> u64 {
    let digest = blake3::Hasher::new()
        .update(b"ursula incarnation key v1")
        .update(&history.to_le_bytes())
        .finalize();
    let mut first = [0u8; 8];
    first.copy_from_slice(digest.as_bytes().first_chunk::<8>().unwrap_or(&[0; 8]));
    u64::from_le_bytes(first) & TOKEN_MASK
}

/// The `Stream-Incarnation` token of a stream created at `created_at_ms`, in history `history`
/// (#581 §4.2): never 0, below 2^53, a bijection of `created_at_ms` within one history.
pub(crate) fn incarnation_token(created_at_ms: u64, history: u64) -> Incarnation {
    Incarnation::try_from(TOKEN_TAG | ((created_at_ms ^ history_key(history)) & TOKEN_MASK))
        .expect("a token is non-zero and below 2^53")
}

/// The limits a node advertises and applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limits {
    /// `Ursula-Max-Request-Bytes`, for every request body.
    pub(crate) max_request_bytes: u64,
    /// `Ursula-Max-Snapshot-Bytes`: `Some` with a cold store.
    pub(crate) max_snapshot_bytes: Option<u64>,
    /// The most bytes one read returns.
    pub(crate) read_chunk: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_bytes: MAX_REQUEST_BYTES,
            max_snapshot_bytes: None,
            read_chunk: 1 << 20,
        }
    }
}

/// A saved state of every stream, to restore as a history event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CheckpointId(pub(crate) usize);

/// Every stream as it stood, each with the history it was last written in.
#[derive(Debug, Clone)]
struct Checkpoint {
    streams: BTreeMap<String, Stream>,
}

/// A history event (#581 §4.3): every token of the group changes and every producer record is
/// history-fenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryEvent {
    /// No byte changes: an accepted loss that lost nothing.
    NewHistory,
    /// Every stream goes back to a checkpoint: an accepted loss of what was written since, or a
    /// restore of a backup taken there.
    Restore(CheckpointId),
}

/// A control operation the model cannot do.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ModelError {
    #[error(transparent)]
    Header(#[from] ursula_headers::HeaderError),
    #[error("no stream at {0}")]
    NoStream(String),
    #[error("the stream at {0} is closed")]
    Closed(String),
    #[error("no checkpoint {0}")]
    NoCheckpoint(usize),
}

/// What became of a request that went through the faults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Delivered {
    /// The answer, to send after `delay_ms`.
    Answered { answer: RawAnswer, delay_ms: u64 },
    /// No connection was made.
    Unreachable,
    /// Lost on the way: nothing applied.
    DroppedBeforeApply,
    /// Applied, and the answer lost.
    DroppedAfterApply,
}

/// A part staged for a parts commit: the stream and token it belongs to, its bytes, and when the
/// model's clock staged it.
#[derive(Debug, Clone)]
struct StagedPart {
    path: String,
    token: Incarnation,
    body: Arc<[u8]>,
    staged_ms: u64,
}

/// The model of one server group.
#[derive(Debug)]
pub(crate) struct ModelServer {
    identity: ServerIdentity,
    /// The group's history id, random and non-zero.
    history: u64,
    /// The state of the generator that draws history ids.
    seed: u64,
    /// C7: every create takes a later `created_at_ms`.
    last_created_at_ms: u64,
    clock_ms: u64,
    limits: Limits,
    streams: BTreeMap<String, Stream>,
    checkpoints: Vec<Checkpoint>,
    faults: Faults,
    /// Staged snapshot parts, by id.
    parts: BTreeMap<String, StagedPart>,
    /// The parts staged so far: every id is minted once.
    minted: u64,
}

/// Whether a snapshot read honors `Range`: always, but under the scripted `ignore-range` fault,
/// which answers as a layer that strips `Range` makes the server answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ranges {
    Honored,
    Ignored,
}

impl ModelServer {
    /// A group of the deployment `cluster_id`, after its genesis history event. `seed` draws the
    /// history ids, so a run is reproducible.
    pub(crate) fn new(cluster_id: ClusterId, seed: u64) -> Self {
        let mut server = Self {
            identity: ServerIdentity::current(cluster_id),
            history: 0,
            seed,
            last_created_at_ms: 0,
            clock_ms: START_MS,
            limits: Limits::default(),
            streams: BTreeMap::new(),
            checkpoints: Vec::new(),
            faults: Faults::default(),
            parts: BTreeMap::new(),
            minted: 0,
        };
        server.history = server.draw_history();
        server
    }

    /// A non-zero history id (splitmix64).
    fn draw_history(&mut self) -> u64 {
        loop {
            self.seed = self.seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^= z >> 31;
            if z != 0 {
                return z;
            }
        }
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.clock_ms
    }

    /// Moves the clock forward.
    pub(crate) fn advance(&mut self, ms: u64) {
        self.clock_ms = self.clock_ms.saturating_add(ms);
    }

    pub(crate) fn limits(&self) -> Limits {
        self.limits
    }

    pub(crate) fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    pub(crate) fn inject(&mut self, scripted: Scripted) {
        self.faults.push(scripted);
    }

    /// The stream's current token.
    #[cfg(test)]
    pub(crate) fn token(&self, path: &str) -> Option<Incarnation> {
        Some(self.streams.get(path)?.token_in(self.history))
    }

    /// How many parts are staged, swept or not.
    #[cfg(test)]
    pub(crate) fn staged_parts(&self) -> usize {
        self.parts.len()
    }

    /// Every byte of the stream, from offset 0.
    pub(crate) fn frames(&self, path: &str) -> Option<&[u8]> {
        Some(&self.streams.get(path)?.bytes)
    }

    /// Saves every stream's state, to restore later as a history event.
    pub(crate) fn checkpoint(&mut self) -> CheckpointId {
        self.checkpoints.push(Checkpoint {
            streams: self.streams.clone(),
        });
        CheckpointId(self.checkpoints.len().saturating_sub(1))
    }

    /// Applies a history event (#581 §4.3): a fresh history id, `last_created_at_ms` kept at least
    /// the clock, and nothing else. Tokens, fences and inheritance all derive from the id. A
    /// restore puts every stream back as the source held it, in the history it was last written
    /// in, so HEAD reports the token under which the stream was last written with the restored
    /// tail (§4.8): after events with no write between them, the token before all of them.
    pub(crate) fn history_event(&mut self, event: HistoryEvent) -> Result<(), ModelError> {
        if let HistoryEvent::Restore(CheckpointId(id)) = event {
            let source = self
                .checkpoints
                .get(id)
                .ok_or(ModelError::NoCheckpoint(id))?;
            self.streams = source.streams.clone();
        }
        self.history = self.draw_history();
        self.last_created_at_ms = self.last_created_at_ms.max(self.clock_ms);
        Ok(())
    }

    /// An append without producer headers by another writer, which holds the stream's content
    /// type. Returns the stream's new tail.
    pub(crate) fn foreign_append(
        &mut self,
        path: &str,
        bytes: &[u8],
    ) -> Result<Offset, ModelError> {
        let history = self.history;
        let stream = self
            .streams
            .get_mut(path)
            .ok_or_else(|| ModelError::NoStream(path.to_owned()))?;
        if stream.closed {
            return Err(ModelError::Closed(path.to_owned()));
        }
        Ok(stream.append(bytes, history).1)
    }

    /// A claim of `epoch` by another claimant of this format's producer id, with `body`, tokened
    /// and with its digest, as a takeover by another owner sends it.
    pub(crate) fn foreign_claim(
        &mut self,
        path: &str,
        epoch: Epoch,
        body: &[u8],
    ) -> Result<RawAnswer, ModelError> {
        let stream = self
            .streams
            .get(path)
            .ok_or_else(|| ModelError::NoStream(path.to_owned()))?;
        let request = Request {
            keys: ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: ProducerId::new(PRODUCER_ID)?,
                    epoch,
                    seq: Seq::ZERO,
                    digest: Some(producer_digest(body)),
                }),
                precondition: Some(IncarnationPrecondition {
                    incarnation: stream.token_in(self.history),
                }),
            },
            content_type: Some(stream.content_type.clone()),
            body: Arc::from(body),
            ..Request::new(Method::Post, Target::Stream)
        };
        Ok(self.handle(path, &request))
    }

    /// Closes the stream without producer headers.
    pub(crate) fn close(&mut self, path: &str) -> Result<(), ModelError> {
        let history = self.history;
        self.streams
            .get_mut(path)
            .ok_or_else(|| ModelError::NoStream(path.to_owned()))?
            .close(history);
        Ok(())
    }

    /// Deletes the stream. A create at its path later starts a new incarnation.
    pub(crate) fn delete(&mut self, path: &str) -> Result<(), ModelError> {
        self.streams
            .remove(path)
            .map(drop)
            .ok_or_else(|| ModelError::NoStream(path.to_owned()))
    }

    /// A request through the scripted faults, then the server.
    pub(crate) fn exchange(&mut self, path: &str, request: &Request) -> Delivered {
        self.deliver(request.method, |server, ranges| {
            server.answer(path, request, ranges)
        })
    }

    /// A stream request whose `Content-Length` exceeds the request limit, as the HTTP front meets
    /// it before reading the body: through the same faults as [`ModelServer::exchange`], so both
    /// fronts consume a scripted fault alike, then the typed `413`.
    pub(crate) fn exchange_too_large(&mut self, method: Method) -> Delivered {
        self.deliver(method, |server, _ranges| server.too_large())
    }

    /// The typed `413` of the request limit (#581 §6.4).
    fn too_large(&mut self) -> RawAnswer {
        let mut answer = RawAnswer::new(SizeLimit::STATUS);
        SizeLimit::Request(self.limits.max_request_bytes).render(answer.emit());
        answer
    }

    /// The next scripted fault for `method` around the server's answer, `handle`.
    fn deliver(
        &mut self,
        method: Method,
        handle: impl FnOnce(&mut Self, Ranges) -> RawAnswer,
    ) -> Delivered {
        let answered = |answer| Delivered::Answered {
            answer,
            delay_ms: 0,
        };
        match self.faults.take(method) {
            None => answered(handle(self, Ranges::Honored)),
            Some(Fault::Unreachable) => Delivered::Unreachable,
            Some(Fault::DropRequest) => Delivered::DroppedBeforeApply,
            Some(Fault::DropAnswer) => {
                drop(handle(self, Ranges::Honored));
                Delivered::DroppedAfterApply
            }
            Some(Fault::Status {
                status,
                retry_after,
            }) => {
                let mut answer = RawAnswer::new(status);
                if let Some(seconds) = retry_after {
                    answer.header(RETRY_AFTER, seconds.to_string());
                }
                answered(answer)
            }
            Some(Fault::Redirect { location }) => {
                let mut answer = RawAnswer::new(307);
                answer.header(LOCATION, location);
                answered(answer)
            }
            Some(Fault::UntypedTooLarge) => answered(RawAnswer::new(SizeLimit::STATUS)),
            Some(Fault::Delay { ms }) => Delivered::Answered {
                answer: handle(self, Ranges::Honored),
                delay_ms: ms,
            },
            Some(Fault::IgnoreRange) => answered(handle(self, Ranges::Ignored)),
            Some(Fault::CommitRefused(refusal)) => answered(commit_refused(refusal)),
        }
    }

    /// The server's answer to `request` for the stream at `path`, decided in #581 §6.1's order:
    /// size, then the route.
    pub(crate) fn handle(&mut self, path: &str, request: &Request) -> RawAnswer {
        self.answer(path, request, Ranges::Honored)
    }

    /// [`ModelServer::handle`], a snapshot read honoring `Range` or not.
    fn answer(&mut self, path: &str, request: &Request, ranges: Ranges) -> RawAnswer {
        let len = u64::try_from(request.body.len()).unwrap_or(u64::MAX);
        if request.method.has_body() && len > self.limits.max_request_bytes {
            return self.too_large();
        }
        match (request.method, request.target) {
            (Method::Head, Target::Stream) => self.head(path),
            (Method::Put, Target::Stream) => self.create(path, request),
            (Method::Post, Target::Stream) => self.append(path, request),
            (Method::Get, Target::Read { from, .. }) => self.read(path, request, from),
            (Method::Put, Target::Snapshot { offset }) => match request.parts.as_deref() {
                Some(commit) => self.commit_parts(path, request, offset, *commit),
                None => self.publish(path, request, offset),
            },
            (Method::Post, Target::SnapshotParts { .. }) => self.stage_part(path, request),
            (Method::Get, Target::Snapshot { offset }) => {
                self.snapshot_read(path, request, offset, ranges)
            }
            (Method::Put, Target::Retention { offset }) => self.retain(path, request, offset),
            _ => RawAnswer::new(405),
        }
    }

    /// The stream at `path`, after the incarnation precondition: `404` without one, `412` naming
    /// the current token when the request names another.
    fn stream(&mut self, path: &str, request: &Request) -> Result<&mut Stream, RawAnswer> {
        let history = self.history;
        let stream = self
            .streams
            .get_mut(path)
            .ok_or_else(|| RawAnswer::new(404))?;
        precondition(stream, request, history)?;
        Ok(stream)
    }

    /// `HEAD`: the server's identity on every answer, `404` included (A1), and the stream's
    /// metadata (#581 §6.3, §4.8).
    fn head(&mut self, path: &str) -> RawAnswer {
        let mut answer = RawAnswer::new(404);
        self.identity.render(answer.emit());
        let Some(stream) = self.streams.get(path) else {
            return answer;
        };
        answer.status = 200;
        let head = StreamHead::new(
            stream.token_in(self.history),
            stream.tail(),
            stream.retained,
            self.limits.max_request_bytes,
        )
        .expect("retention never passes the tail")
        .with_closed(stream.closed)
        .with_snapshot(stream.snapshot.as_ref().map(|s| s.snapshot))
        .expect("a published snapshot is at most the tail")
        .with_max_snapshot_bytes(self.limits.max_snapshot_bytes)
        .with_inherited(stream.inherited(self.history))
        .expect("the inherited offset is at most the tail, under another token");
        head.render(answer.emit());
        answer.header(CONTENT_TYPE, stream.content_type.clone());
        answer
    }

    /// `PUT {stream}`, keyed by its configuration, content type and closure (#581 §3): `201` when
    /// this request created the stream, `200` when one with this configuration exists, `409`
    /// otherwise, `400` with producer headers.
    fn create(&mut self, path: &str, request: &Request) -> RawAnswer {
        if request.keys.producer.is_some() {
            return RawAnswer::new(400);
        }
        let content_type = request
            .content_type
            .clone()
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned());
        let history = self.history;
        if let Some(stream) = self.streams.get(path) {
            if stream.content_type != content_type || stream.closed != request.closes {
                return RawAnswer::new(409);
            }
            return created(200, stream, history);
        }
        let created_at_ms = self.clock_ms.max(self.last_created_at_ms.saturating_add(1));
        self.last_created_at_ms = created_at_ms;
        let mut stream = Stream::new(created_at_ms, content_type, history);
        if !request.body.is_empty() {
            stream.append(&request.body, history);
        }
        if request.closes {
            stream.close(history);
        }
        let answer = created(201, &stream, history);
        self.streams.insert(path.to_owned(), stream);
        answer
    }

    /// `POST {stream}`, in #581 addendum 4's order, the same on every path:
    ///
    /// 1. Header shape (`400`): [`Request`] is typed, so a malformed header never reaches here.
    /// 2. The stream (`404`), the precondition (`412`), then idle retirement of the producer.
    /// 3. The rows of §2.3.1, keyed by the `Producer-Digest` header, never by the body. Every
    ///    refusal and every repeat is answered here.
    /// 4. Only a write that would apply meets the closed stream (`409 Stream-Closed`).
    /// 5. Only a write that would apply meets the content checks: a body unless the write closes
    ///    the stream (`400`), `Content-Type` present (`400`) and the stream's (`409`), and the body
    ///    matching `Producer-Digest` (`400`).
    ///
    /// A close-only write applies, keyed or not, and an unkeyed one is idempotent on a closed
    /// stream (base protocol §5.2). An append that also closes is not modeled
    /// (`501`): the VFS never closes its stream.
    fn append(&mut self, path: &str, request: &Request) -> RawAnswer {
        let (history, now_ms) = (self.history, self.clock_ms);
        let stream = match self.stream(path, request) {
            Ok(stream) => stream,
            Err(answer) => return answer,
        };
        let token = stream.token_in(history);
        let applying = match &request.keys.producer {
            None => None,
            Some(write) => {
                let id = write.id.as_str();
                stream.retire_idle(id, now_ms);
                let evaluation = evaluate(
                    stream.producers.get(id),
                    stream.watermark,
                    write,
                    request.keys.precondition.is_some(),
                    history,
                    token,
                );
                match evaluation.decision {
                    Decision::Answer(answer) => return producer_answer(&answer),
                    Decision::Apply {
                        closes,
                        below_floor,
                    } => Some((write, closes, below_floor)),
                }
            }
        };
        let close_only = request.closes && request.body.is_empty();
        if stream.closed {
            // Base protocol §5.2: an unkeyed close-only write is idempotent.
            if close_only && applying.is_none() {
                return closed_append(stream.tail());
            }
            let mut answer = RawAnswer::new(StreamClosedConflict::STATUS);
            StreamClosedConflict {
                final_offset: stream.tail(),
            }
            .render(answer.emit());
            return answer;
        }
        if request.closes && !close_only {
            return RawAnswer::new(501);
        }
        if let Some(refusal) = content_refusal(stream, request, close_only) {
            return refusal;
        }
        let (start, next) = if close_only {
            stream.close(history);
            (stream.tail(), stream.tail())
        } else {
            stream.append(&request.body, history)
        };
        let Some((write, closes, below_floor)) = applying else {
            if close_only {
                return closed_append(next);
            }
            let mut answer = RawAnswer::new(204);
            answer.header(names::STREAM_NEXT_OFFSET, next.to_string());
            return answer;
        };
        let id = write.id.as_str();
        let receipt =
            Receipt::new(start, next, close_only).expect("a write ends at or after its start");
        let epoch = if write.is_claim() {
            let record =
                ProducerRecord::claimed(write, closes, below_floor, receipt, history, now_ms);
            let epoch = record.epoch;
            stream.producers.insert(id.to_owned(), record);
            epoch
        } else {
            if let Some(record) = stream.producers.get_mut(id) {
                record.advanced(write, receipt, now_ms);
            }
            CurrentEpoch::new(write.epoch)
        };
        producer_answer(&ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            epoch,
            seq: write.seq,
            receipt,
            incarnation: token,
        })
    }

    /// `GET {stream}?offset=`: up to a chunk of bytes from `from`, its refusals in #581's order:
    /// `404`, `410` below retention, `416` past the tail, then the precondition (`412`).
    fn read(&mut self, path: &str, request: &Request, from: Option<Offset>) -> RawAnswer {
        let chunk = self.limits.read_chunk;
        let Some(stream) = self.streams.get(path) else {
            return RawAnswer::new(404);
        };
        let from = from.unwrap_or(Offset::ZERO);
        let tail = stream.tail();
        if from < stream.retained {
            return RawAnswer::new(410);
        }
        if from > tail {
            return RawAnswer::new(416);
        }
        if let Err(mismatch) = precondition(stream, request, self.history) {
            return mismatch;
        }
        let next = Offset::from_u64(from.get().saturating_add(chunk).min(tail.get()));
        let bytes = usize::try_from(from.get())
            .ok()
            .zip(usize::try_from(next.get()).ok())
            .and_then(|(start, end)| stream.bytes.get(start..end))
            .unwrap_or_default();
        let mut answer = RawAnswer::new(200);
        answer.header(names::STREAM_NEXT_OFFSET, next.to_string());
        if next == tail {
            answer.header(names::STREAM_UP_TO_DATE, "true");
            if stream.closed {
                answer.header(names::STREAM_CLOSED, "true");
            }
        }
        answer.header(CONTENT_TYPE, stream.content_type.clone());
        answer.body = bytes.to_vec();
        answer
    }

    /// `PUT {stream}/snapshot/{offset}`, keyed by `(offset, digest)` (#581 §3): `204` with the
    /// digest while it is the latest, `409` naming the latest otherwise.
    fn publish(&mut self, path: &str, request: &Request, offset: Offset) -> RawAnswer {
        let history = self.history;
        let stream = match self.stream(path, request) {
            Ok(stream) => stream,
            Err(answer) => return answer,
        };
        let (Some(content_type), Some(digest)) = (&request.content_type, request.snapshot_digest)
        else {
            return RawAnswer::new(400);
        };
        let mut digester = SnapshotDigester::new(content_type);
        digester.update(&request.body);
        if digester.finalize() != digest || offset > stream.tail() {
            return RawAnswer::new(400);
        }
        let published = SnapshotRef { offset, digest };
        let answer = match &stream.snapshot {
            Some(latest)
                if latest.snapshot.offset > offset
                    || (latest.snapshot.offset == offset && latest.snapshot.digest != digest) =>
            {
                SnapshotPublishAnswer::Conflict {
                    latest: latest.snapshot,
                }
            }
            _ => {
                if stream.snapshot.as_ref().map(|s| s.snapshot) != Some(published) {
                    stream.snapshot = Some(Snapshot {
                        snapshot: published,
                        content_type: content_type.clone(),
                        body: Arc::clone(&request.body),
                    });
                }
                SnapshotPublishAnswer::Published {
                    snapshot: published,
                    incarnation: stream.token_in(history),
                }
            }
        };
        let mut raw = RawAnswer::new(answer.status());
        answer.render(raw.emit());
        raw
    }

    /// `POST {stream}/snapshot/{offset}/parts` (extensions §2.2): `201` naming a part id minted
    /// for this request alone, bound to the stream and its token, never to the offset. Not
    /// idempotent: a repeat stages another part. `501` without a cold store.
    fn stage_part(&mut self, path: &str, request: &Request) -> RawAnswer {
        if self.limits.max_snapshot_bytes.is_none() {
            return RawAnswer::new(501);
        }
        self.sweep_parts();
        let (history, now_ms) = (self.history, self.clock_ms);
        let token = match self.stream(path, request) {
            Ok(stream) => stream.token_in(history),
            Err(answer) => return answer,
        };
        self.minted = self.minted.saturating_add(1);
        let part = PartId::new(format!("{now_ms:016x}-{:016x}.part", self.minted))
            .expect("a minted id is visible ASCII within the length");
        self.parts.insert(part.as_str().to_owned(), StagedPart {
            path: path.to_owned(),
            token,
            body: Arc::clone(&request.body),
            staged_ms: now_ms,
        });
        let mut answer = RawAnswer::new(201);
        PartStaged { part }.render(answer.emit());
        answer
    }

    /// Reclaims every part older than its grace, used or not.
    fn sweep_parts(&mut self) {
        let now_ms = self.clock_ms;
        self.parts
            .retain(|_, part| now_ms.saturating_sub(part.staged_ms) < PART_GRACE_MS);
    }

    /// `PUT {stream}/snapshot/{offset}` with `Stream-Snapshot-Parts` (extensions §2.2), in the
    /// server's order: `501` without a cold store; the stream (`404`, `412`); the key, before the
    /// body, so a repeat is answered even after its parts are gone: `204` for the latest
    /// snapshot, `409` naming the latest when another one is at or after the offset, `410` below
    /// retention, `409` beyond the tail. Then the manifest and its parts: `400` for a malformed
    /// manifest or a part not staged for this incarnation of this stream (the parts stay), `413`
    /// naming the snapshot limit (they stay), and `400` for a digest that differs (the copy used
    /// them up). A publish deletes them.
    fn commit_parts(
        &mut self,
        path: &str,
        request: &Request,
        offset: Offset,
        commit: PartsCommit,
    ) -> RawAnswer {
        let Some(max_snapshot) = self.limits.max_snapshot_bytes else {
            return RawAnswer::new(501);
        };
        self.sweep_parts();
        let history = self.history;
        let ours = SnapshotRef {
            offset,
            digest: commit.digest,
        };
        let (token, latest, retained, tail) = match self.stream(path, request) {
            Ok(stream) => (
                stream.token_in(history),
                stream.snapshot.as_ref().map(|s| s.snapshot),
                stream.retained,
                stream.tail(),
            ),
            Err(answer) => return answer,
        };
        match latest {
            Some(latest) if latest == ours => {
                return publish_answer(&SnapshotPublishAnswer::Published {
                    snapshot: ours,
                    incarnation: token,
                });
            }
            Some(latest) if latest.offset >= offset => {
                return publish_answer(&SnapshotPublishAnswer::Conflict { latest });
            }
            Some(_) | None => {}
        }
        if offset < retained {
            return RawAnswer::new(410);
        }
        if offset > tail {
            let mut answer = RawAnswer::new(409);
            answer.header(names::STREAM_NEXT_OFFSET, tail.to_string());
            return answer;
        }
        let Some(content_type) = request.content_type.clone() else {
            return RawAnswer::new(400);
        };
        let Ok(manifest) = PartsManifest::parse(commit.parts, &request.body) else {
            return RawAnswer::new(400);
        };
        let mut bodies = Vec::with_capacity(manifest.parts().len());
        for id in manifest.parts() {
            match self.parts.get(id.as_str()) {
                Some(part) if part.path == path && part.token == token => {
                    bodies.push(Arc::clone(&part.body));
                }
                Some(_) | None => return RawAnswer::new(400),
            }
        }
        let total = bodies.iter().try_fold(0_u64, |total, body| {
            total.checked_add(u64::try_from(body.len()).ok()?)
        });
        if total.is_none_or(|total| total > max_snapshot) {
            let mut answer = RawAnswer::new(SizeLimit::STATUS);
            SizeLimit::Snapshot(max_snapshot).render(answer.emit());
            return answer;
        }
        let assembled: Arc<[u8]> = Arc::from(bodies.concat());
        // The copy consumes the parts, whatever its digest.
        for id in manifest.parts() {
            self.parts.remove(id.as_str());
        }
        let mut digester = SnapshotDigester::new(&content_type);
        digester.update(&assembled);
        if digester.finalize() != commit.digest {
            return RawAnswer::new(400);
        }
        let Some(stream) = self.streams.get_mut(path) else {
            return RawAnswer::new(404);
        };
        stream.snapshot = Some(Snapshot {
            snapshot: ours,
            content_type,
            body: assembled,
        });
        publish_answer(&SnapshotPublishAnswer::Published {
            snapshot: ours,
            incarnation: token,
        })
    }

    /// `GET {stream}/snapshot/{offset}`: the latest snapshot when it is at `offset`. A single
    /// `Range` is answered `206` with its bytes, or `416` when it selects none (extensions §2.5).
    fn snapshot_read(
        &mut self,
        path: &str,
        request: &Request,
        offset: Offset,
        ranges: Ranges,
    ) -> RawAnswer {
        let history = self.history;
        let stream = match self.stream(path, request) {
            Ok(stream) => stream,
            Err(answer) => return answer,
        };
        let Some(snapshot) = stream
            .snapshot
            .as_ref()
            .filter(|s| s.snapshot.offset == offset)
        else {
            return RawAnswer::new(404);
        };
        let total = u64::try_from(snapshot.body.len()).unwrap_or(u64::MAX);
        let range = match ranges {
            Ranges::Honored => request
                .range
                .and_then(|range: ByteRange| range.resolve(total)),
            Ranges::Ignored => None,
        };
        let (status, body) = match range {
            None => (200, snapshot.body.as_ref()),
            Some(ContentRange::Unsatisfied { .. }) => (416, &[][..]),
            Some(ContentRange::Partial(span)) => {
                let first = usize::try_from(span.first()).unwrap_or(usize::MAX);
                let last = usize::try_from(span.last()).unwrap_or(usize::MAX);
                (206, snapshot.body.get(first..=last).unwrap_or_default())
            }
        };
        let mut answer = RawAnswer::new(status);
        if let Some(range) = range {
            range.render(answer.emit());
        }
        if let Some(ContentRange::Unsatisfied { .. }) = range {
            return answer;
        }
        answer.header(names::ACCEPT_RANGES, ACCEPT_RANGES_BYTES);
        SnapshotRead {
            snapshot: snapshot.snapshot,
            incarnation: stream.token_in(history),
            up_to_date: offset == stream.tail(),
        }
        .render(answer.emit());
        answer.header(CONTENT_TYPE, snapshot.content_type.clone());
        answer.body = body.to_vec();
        answer
    }

    /// `PUT {stream}/retention/{offset}`, monotonic (#581 §3): `204` with the retained offset, at
    /// or above `offset`, and `409` for a boundary past the latest snapshot.
    fn retain(&mut self, path: &str, request: &Request, offset: Offset) -> RawAnswer {
        let history = self.history;
        let stream = match self.stream(path, request) {
            Ok(stream) => stream,
            Err(answer) => return answer,
        };
        // Extensions §2.2: a boundary newer than the latest published snapshot is refused, so is
        // any boundary of a stream without one.
        if stream
            .snapshot
            .as_ref()
            .is_none_or(|latest| offset > latest.snapshot.offset)
        {
            return RawAnswer::new(409);
        }
        stream.retained = stream.retained.max(offset);
        let mut answer = RawAnswer::new(204);
        answer.header(names::STREAM_RETAINED_OFFSET, stream.retained.to_string());
        answer.header(
            names::STREAM_INCARNATION,
            stream.token_in(history).to_string(),
        );
        answer
    }
}

/// The content checks of a write that would apply (#581 addendum 4, step 5): a body unless the
/// write only closes the stream (`400`), then `Content-Type` present (`400`) and the stream's
/// (`409`, the base protocol's mismatch, with the tail), then the body matching its
/// `Producer-Digest` (`400`). A write without a body is never refused for its content type (base
/// protocol §5.2).
fn content_refusal(stream: &Stream, request: &Request, close_only: bool) -> Option<RawAnswer> {
    if close_only {
        // Its body is empty, so its digest is checked alike below.
    } else if request.body.is_empty() {
        return Some(RawAnswer::new(400));
    } else {
        match request.content_type.as_deref() {
            None => return Some(RawAnswer::new(400)),
            Some(content_type) if content_type != stream.content_type => {
                let mut answer = RawAnswer::new(409);
                answer.header(names::STREAM_NEXT_OFFSET, stream.tail().to_string());
                return Some(answer);
            }
            Some(_) => {}
        }
    }
    let digest_ok = request
        .keys
        .producer
        .as_ref()
        .and_then(|write| write.digest)
        .is_none_or(|digest| digest == producer_digest(&request.body));
    (!digest_ok).then(|| RawAnswer::new(400))
}

/// The answer to an unkeyed close-only write: `204`, the tail and `Stream-Closed: true`.
fn closed_append(tail: Offset) -> RawAnswer {
    let mut answer = RawAnswer::new(204);
    answer.header(names::STREAM_NEXT_OFFSET, tail.to_string());
    answer.header(names::STREAM_CLOSED, "true");
    answer
}

/// The incarnation precondition: `412` naming the stream's current token when the request names
/// another.
fn precondition(stream: &Stream, request: &Request, history: u64) -> Result<(), RawAnswer> {
    let current = stream.token_in(history);
    match request.keys.precondition {
        Some(precondition) if precondition.incarnation != current => {
            let mut answer = RawAnswer::new(IncarnationMismatch::STATUS);
            IncarnationMismatch {
                current: Some(current),
            }
            .render(answer.emit());
            Err(answer)
        }
        Some(_) | None => Ok(()),
    }
}

/// A snapshot publish's answer, rendered.
fn publish_answer(answer: &SnapshotPublishAnswer) -> RawAnswer {
    let mut raw = RawAnswer::new(answer.status());
    answer.render(raw.emit());
    raw
}

/// The `503` of a node that refuses a parts commit for now, as the server sends it:
/// `Retry-After: 1` and the JSON `error` code.
fn commit_refused(refusal: CommitRefusal) -> RawAnswer {
    let mut answer = RawAnswer::new(503);
    answer.header(RETRY_AFTER, "1");
    answer.header(CONTENT_TYPE, "application/json");
    answer.body = format!("{{\"error\":\"{}\"}}", refusal.code()).into_bytes();
    answer
}

/// A producer answer, rendered.
fn producer_answer(answer: &ProducerAnswer) -> RawAnswer {
    let mut raw = RawAnswer::new(answer.status());
    answer.render(raw.emit());
    raw
}

/// The answer to a create that found or made `stream`.
fn created(status: u16, stream: &Stream, history: u64) -> RawAnswer {
    let mut answer = RawAnswer::new(status);
    answer.header(
        names::STREAM_INCARNATION,
        stream.token_in(history).to_string(),
    );
    answer.header(names::STREAM_NEXT_OFFSET, stream.tail().to_string());
    answer
}
