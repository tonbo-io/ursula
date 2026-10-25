//! The protocol's requests and answers as values (base protocol and extensions), shared by the
//! transports and the model server, so both sides of the protocol build and read the same structs.
//!
//! Every header the extensions type goes through `ursula_headers`, never a local parser: producer
//! keys and answers, HEAD metadata, snapshot answers, the typed `413`, the `412` of the incarnation
//! precondition, the `409` of a closed stream and routing answers. Four answers the crate does
//! not type yet (a read's `Stream-Next-Offset`, a create's `Stream-Incarnation`, the
//! `Stream-Incarnation` of a read's refusal, retention's `Stream-Retained-Offset`) are picked out
//! here and parsed with the crate's value types.
//! `Retry-After`, `Location` and `Content-Type` are plain HTTP and read here.
//!
//! [`parse_answer`] reads an answer in the light of the request it answers: a `409` to a keyed
//! append is the producer's or a closed stream's, a `204` to a snapshot publish is a published
//! snapshot. An answer to a write also carries what `ursula_headers::classify` reads from it
//! ([`Answer::outcome`]): the replicator (`replicator/answer.rs`) and the snapshotter
//! (`snapshotter::transient`) read a write's answer by that outcome, never by a status table of
//! their own.

use std::sync::Arc;
use std::time::Duration;

use ursula_headers::ByteRange;
use ursula_headers::ByteSpan;
use ursula_headers::ContentRange;
use ursula_headers::HeaderError;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationMismatch;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::ProducerAnswer;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::RoutingAnswer;
use ursula_headers::ServerIdentity;
use ursula_headers::SizeLimit;
use ursula_headers::SnapshotDigest;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRead;
use ursula_headers::StreamClosedConflict;
use ursula_headers::StreamHead;
use ursula_headers::WriteKind;
use ursula_headers::WriteOutcome;
use ursula_headers::classify;
use ursula_headers::names;

/// `Producer-Id` of every keyed write: constant for the `USQ2` format (design: Appends).
pub(crate) const PRODUCER_ID: &str = "sqlite-ursula-vfs/2";

/// `Content-Type` header name.
pub(crate) const CONTENT_TYPE: &str = "content-type";

/// `Retry-After` header name.
pub(crate) const RETRY_AFTER: &str = "retry-after";

/// `Location` header name.
pub(crate) const LOCATION: &str = "location";

/// The headers of a `429` to a part past the stream's limit of staged parts (extensions: Publish
/// Snapshot), naming the limit it met: its unused parts, or their bytes.
pub(crate) const MAX_STAGED_PARTS: &str = "ursula-max-staged-parts";
pub(crate) const MAX_STAGED_BYTES: &str = "ursula-max-staged-bytes";

/// A `USS3` header's length, which a header read asks for.
const SNAPSHOT_HEADER: u64 = crate::codec::snapshot::HEADER as u64;

/// The `Range` of a header read: the `USS3` header alone.
const HEADER_RANGE: ByteRange = ByteRange::Bounded {
    first: 0,
    last: SNAPSHOT_HEADER - 1,
};

/// The base protocol's read offset for the start of a stream.
const START: &str = "-1";

/// The path suffix, after the snapshot's, of a part staged for it.
const PARTS: &str = "/parts";

/// The path suffix of the stream's staged parts, discarded together.
const STAGED_PARTS: &str = "/snapshot/parts";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Method {
    Head,
    Get,
    Put,
    Post,
    Delete,
}

#[cfg_attr(
    not(any(test, feature = "model")),
    expect(dead_code, reason = "the model server's side of the protocol")
)]
impl Method {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text {
            "HEAD" => Some(Self::Head),
            "GET" => Some(Self::Get),
            "PUT" => Some(Self::Put),
            "POST" => Some(Self::Post),
            "DELETE" => Some(Self::Delete),
            _ => None,
        }
    }

    /// Whether the request carries a body (and so `Content-Length`).
    pub(crate) fn has_body(self) -> bool {
        matches!(self, Self::Put | Self::Post)
    }
}

/// What a request addresses, relative to the stream URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    /// The stream: `HEAD`, create (`PUT`), append (`POST`).
    Stream,
    /// A read from `from` (`None`: the start of the stream), of the leader's applied state when
    /// `leader`.
    Read { from: Option<Offset>, leader: bool },
    /// The snapshot at `offset`: a publish or a parts commit (`PUT`), or a read (`GET`).
    Snapshot { offset: Offset },
    /// A part of the snapshot at `offset`, staged (`POST`) for a parts commit.
    SnapshotParts { offset: Offset },
    /// Every part staged under the stream's current token, discarded (`DELETE`).
    StagedParts,
    /// The retention boundary (`PUT`), monotonic (extensions: Publish Snapshot).
    Retention { offset: Offset },
}

impl Target {
    /// The path and query that follow the stream URL.
    pub(crate) fn suffix(&self) -> String {
        match self {
            Self::Stream => String::new(),
            Self::Read { from, leader } => {
                let from = from.map_or_else(|| START.to_owned(), |from| from.to_string());
                let consistency = if *leader { "&consistency=leader" } else { "" };
                format!("?offset={from}{consistency}")
            }
            Self::Snapshot { offset } => format!("/snapshot/{offset}"),
            Self::SnapshotParts { offset } => format!("/snapshot/{offset}/parts"),
            Self::StagedParts => STAGED_PARTS.to_owned(),
            Self::Retention { offset } => format!("/retention/{offset}"),
        }
    }

    /// Splits a request target (path and query) into the stream's path and the [`Target`]: the
    /// inverse of [`Target::suffix`]. A `GET` of the stream's path is a read.
    #[cfg_attr(
        not(any(test, feature = "model")),
        expect(dead_code, reason = "the model server's side of the protocol")
    )]
    pub(crate) fn split(method: Method, target: &str) -> Result<(&str, Self), WireError> {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        if let Some(stream) = path.strip_suffix(STAGED_PARTS) {
            return Ok((stream, Self::StagedParts));
        }
        if let Some((stream, offset)) = path
            .strip_suffix(PARTS)
            .and_then(|path| path.rsplit_once("/snapshot/"))
        {
            let offset = Offset::parse(names::STREAM_OFFSET, offset)?;
            return Ok((stream, Self::SnapshotParts { offset }));
        }
        for (marker, build) in [
            ("/snapshot/", Self::snapshot as fn(Offset) -> Self),
            ("/retention/", Self::retention),
        ] {
            if let Some((stream, offset)) = path.rsplit_once(marker) {
                let offset = Offset::parse(names::STREAM_OFFSET, offset)?;
                return Ok((stream, build(offset)));
            }
        }
        if method != Method::Get {
            return Ok((path, Self::Stream));
        }
        let mut from = None;
        let mut leader = false;
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            match pair.split_once('=') {
                Some(("offset", START)) => from = None,
                Some(("offset", offset)) => {
                    from = Some(Offset::parse(names::STREAM_OFFSET, offset)?);
                }
                Some(("consistency", "leader")) => leader = true,
                _ => return Err(WireError::Query(pair.to_owned())),
            }
        }
        Ok((path, Self::Read { from, leader }))
    }

    #[cfg_attr(
        not(any(test, feature = "model")),
        expect(dead_code, reason = "the model server's side of the protocol")
    )]
    fn snapshot(offset: Offset) -> Self {
        Self::Snapshot { offset }
    }

    #[cfg_attr(
        not(any(test, feature = "model")),
        expect(dead_code, reason = "the model server's side of the protocol")
    )]
    fn retention(offset: Offset) -> Self {
        Self::Retention { offset }
    }
}

/// A request that a transport cannot carry, or that the model server cannot read.
#[cfg_attr(
    not(any(test, feature = "model")),
    expect(dead_code, reason = "the model server's side of the protocol")
)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WireError {
    #[error("malformed header: {0}")]
    Header(#[from] HeaderError),
    #[error("unknown query parameter `{0}`")]
    Query(String),
}

/// One request to a stream. The transport adds the stream URL, `Content-Length`, the bearer token
/// and `User-Agent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Request {
    pub(crate) method: Method,
    pub(crate) target: Target,
    /// The producer key and the `Stream-Incarnation` precondition.
    pub(crate) keys: ProducerRequestHeaders,
    pub(crate) content_type: Option<String>,
    /// `Stream-Snapshot-Digest` of a snapshot publish.
    pub(crate) snapshot_digest: Option<SnapshotDigest>,
    /// `Stream-Snapshot-Parts` and `Stream-Snapshot-Digest` of a parts commit, whose body is the
    /// `PartsManifest`.
    pub(crate) parts: Option<Box<PartsCommit>>,
    /// `Stream-Closed: true`: a create of a closed stream, or a closing append.
    pub(crate) closes: bool,
    pub(crate) body: Arc<[u8]>,
    /// The largest answer body the caller expects (a read's chunk, a snapshot): it scales the
    /// time the answer's body may take. Not sent.
    pub(crate) answer_bytes: u64,
    /// `Range` of a snapshot read: a server answers these bytes (`206`), or the whole body
    /// (`200`) when it ignores `Range`.
    pub(crate) range: Option<ByteRange>,
}

/// Which part of a snapshot a read asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotPart {
    /// The `USS3` header alone, which verifies without the body (design: Snapshots).
    Header,
    /// The bytes `first..=last` of the body (`first <= last`). A server that ignores `Range`
    /// answers the whole body instead, in the time these bytes take.
    Chunk { first: u64, last: u64 },
}

impl Request {
    /// The write this request makes, which `ursula_headers::classify` reads its answer as: `None`
    /// for a read or a HEAD, which change nothing.
    pub(crate) fn write_kind(&self) -> Option<WriteKind> {
        match (self.method, self.target) {
            (Method::Post, Target::Stream) if self.keys.producer.is_some() => {
                Some(WriteKind::Producer)
            }
            // An append or a close without producer headers.
            (Method::Post, Target::Stream) => Some(WriteKind::Append),
            (Method::Put, Target::Stream) => Some(WriteKind::Create),
            // A snapshot publish or a parts commit.
            (Method::Put, Target::Snapshot { .. }) => Some(WriteKind::Publish),
            (Method::Post, Target::SnapshotParts { .. }) => Some(WriteKind::PartStage),
            (Method::Delete, Target::StagedParts) => Some(WriteKind::PartsDiscard),
            (Method::Put, Target::Retention { .. }) => Some(WriteKind::Retention),
            (Method::Delete, Target::Stream) => Some(WriteKind::DeleteStream),
            (Method::Head | Method::Get, _) => None,
            // No request the VFS sends: nothing a write's answer is read as.
            (Method::Put | Method::Post | Method::Delete, _) => None,
        }
    }

    /// A request without headers or body, expecting no answer body. A `GET` goes through
    /// [`Request::read`] or [`Request::snapshot_read`], which size its answer.
    pub(crate) fn new(method: Method, target: Target) -> Self {
        Self {
            method,
            target,
            keys: ProducerRequestHeaders::default(),
            content_type: None,
            snapshot_digest: None,
            parts: None,
            closes: false,
            body: Arc::from(Vec::new()),
            answer_bytes: 0,
            range: None,
        }
    }

    /// How long the answer's head may take beyond the base wait while the server stores or
    /// copies the request's bytes: the body, or the parts a commit copies, at the pace every
    /// waiter in front of a node assumes ([`ursula_headers::answer_allowance`]). A gateway adds
    /// the same allowance to its own base, which stays below the VFS's, so the VFS waits longer
    /// than a gateway in front of the node.
    pub(crate) fn answer_wait(&self) -> Duration {
        ursula_headers::answer_allowance(
            u64::try_from(self.body.len()).unwrap_or(u64::MAX),
            self.parts.as_ref().map(|commit| commit.parts),
        )
    }

    /// A read from `from`, expecting up to `answer_bytes` (the read's chunk) in the answer.
    pub(crate) fn read(from: Option<Offset>, leader: bool, answer_bytes: u64) -> Self {
        Self {
            answer_bytes,
            ..Self::new(Method::Get, Target::Read { from, leader })
        }
    }

    /// A read of `part` of the snapshot at `offset`, under the precondition `incarnation`: the
    /// one request every snapshot read sends.
    pub(crate) fn snapshot_read(
        offset: Offset,
        incarnation: Incarnation,
        part: SnapshotPart,
    ) -> Self {
        let (answer_bytes, range) = match part {
            SnapshotPart::Header => (SNAPSHOT_HEADER, HEADER_RANGE),
            SnapshotPart::Chunk { first, last } => (
                last.saturating_sub(first).saturating_add(1),
                ByteRange::Bounded { first, last },
            ),
        };
        Self {
            keys: ProducerRequestHeaders {
                producer: None,
                precondition: Some(IncarnationPrecondition { incarnation }),
            },
            answer_bytes,
            range: Some(range),
            ..Self::new(Method::Get, Target::Snapshot { offset })
        }
    }

    /// How much of a `200` body the client keeps: a header read keeps the `USS3` header of a
    /// whole body that a server ignoring `Range` sends, every other request the whole body.
    pub(crate) fn whole_answer_cut(&self) -> Option<u64> {
        (self.range == Some(HEADER_RANGE)).then_some(SNAPSHOT_HEADER)
    }

    /// Emits every header of the request this module owns, as `(lowercase name, value)`.
    pub(crate) fn render_headers(&self, mut emit: impl FnMut(&'static str, &str)) {
        if let Some(content_type) = &self.content_type {
            emit(CONTENT_TYPE, content_type);
        }
        self.keys.render(&mut emit);
        if let Some(digest) = &self.snapshot_digest {
            emit(names::STREAM_SNAPSHOT_DIGEST, &digest.to_string());
        }
        if let Some(parts) = &self.parts {
            parts.render(&mut emit);
        }
        if self.closes {
            emit(names::STREAM_CLOSED, "true");
        }
        if let Some(range) = &self.range {
            range.render(&mut emit);
        }
    }

    /// The request a server reads from `method`, `target`, the `(name, value)` header pairs and
    /// the body: the inverse of [`Request::render_headers`].
    #[cfg_attr(
        not(any(test, feature = "model")),
        expect(dead_code, reason = "the model server's side of the protocol")
    )]
    pub(crate) fn parse<'a, I>(
        method: Method,
        target: Target,
        headers: I,
        body: Vec<u8>,
    ) -> Result<Self, WireError>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])> + Clone,
    {
        let keys = ProducerRequestHeaders::parse(headers.clone())?;
        let content_type = text(headers.clone(), CONTENT_TYPE)?.map(str::to_owned);
        let closes = flag(headers.clone(), names::STREAM_CLOSED)?;
        let range = ByteRange::parse(headers.clone());
        // A commit's digest travels in its `PartsCommit`.
        let parts = PartsCommit::parse(headers.clone())?.map(Box::new);
        let snapshot_digest = match parts {
            Some(_) => None,
            None => text(headers, names::STREAM_SNAPSHOT_DIGEST)?
                .map(|digest| SnapshotDigest::parse(names::STREAM_SNAPSHOT_DIGEST, digest))
                .transpose()?,
        };
        Ok(Self {
            method,
            target,
            keys,
            content_type,
            snapshot_digest,
            parts,
            closes,
            body: Arc::from(body),
            answer_bytes: 0,
            range,
        })
    }
}

/// What an answer says in the headers `ursula_headers` types, read in the light of the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Typed {
    /// No typed header applies: the status alone speaks.
    Untyped,
    /// An answer to a keyed write (extensions: Producer Write Answers).
    Producer(ProducerAnswer),
    /// `409 Stream-Closed`: the stream is closed and the append did not apply.
    StreamClosed(StreamClosedConflict),
    /// `412`: the `Stream-Incarnation` precondition failed.
    IncarnationMismatch(IncarnationMismatch),
    /// `413`, with the limit it applied when a layer that implements the size refusals
    /// (extensions: Server Metadata) sent it.
    TooLarge(Option<SizeLimit>),
    /// `307` or `503` of a node that does not lead the stream's group.
    Routing(RoutingAnswer),
    /// A `HEAD`: the server's identity on every answer (extensions: Server Metadata), and the
    /// stream's metadata on a `200`.
    /// A server that sends no identity speaks protocol 1, whose metadata is not read.
    Head {
        identity: Option<ServerIdentity>,
        stream: Option<StreamHead>,
    },
    /// `201` or `200` to a create: the stream exists with this token.
    Created { incarnation: Incarnation },
    /// `204` or `409` to a snapshot publish or a parts commit.
    Snapshot(SnapshotPublishAnswer),
    /// `201` to a part staged: its id.
    PartStaged(PartStaged),
    /// `429` to a part, naming a limit of staged parts (`Ursula-Max-Staged-Parts` or
    /// `Ursula-Max-Staged-Bytes`): no repeat of the part lifts it. A `429` without either is a
    /// rate limit like any other.
    StagedPartsFull,
    /// `200` or `206` to a snapshot read: `range` is the `Content-Range` of a `206`, `None` for
    /// the whole body of a `200`.
    SnapshotRead {
        read: SnapshotRead,
        range: Option<ByteSpan>,
    },
    /// A read's `404`, `410` or `416` (a catch-up read or a snapshot read). `incarnation` is the
    /// token of the stream that refused it: the server names it on every `410` and `416` of a
    /// read and on the `404` of a snapshot that is not the latest, and none when no stream exists
    /// (extensions: Snapshot Metadata on HEAD and Stream Incarnation). These come before the
    /// `412`, so a token other than the request's means the read went to another history.
    ReadRefused { incarnation: Option<Incarnation> },
    /// `200` to a read: the bytes in the body end at `next`.
    Read {
        next: Offset,
        up_to_date: bool,
        closed: bool,
    },
    /// `204` to a retention request: the stream retains from `offset` on.
    Retained { offset: Offset },
}

/// An answer, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Answer {
    pub(crate) status: u16,
    /// `Err` when a header the answer requires is malformed or missing: a protocol violation.
    pub(crate) typed: Result<Typed, HeaderError>,
    /// What `ursula_headers::classify` reads from an answer to a write ([`Request::write_kind`]):
    /// applied, a repeat, refused, redirected, or unknown. `None` for an answer to a read or a
    /// HEAD.
    pub(crate) outcome: Option<WriteOutcome>,
    /// `Retry-After` in seconds; an HTTP date is not read.
    pub(crate) retry_after: Option<Duration>,
    pub(crate) location: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) body: Vec<u8>,
}

/// What came of one attempt of a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Exchange {
    Answered(Box<Answer>),
    NoAnswer(NoAnswer),
}

/// An attempt that got no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NoAnswer {
    /// Nothing of the request went out: the host did not resolve, or no connection was made.
    NotSent { reason: String },
    /// The request may have gone out, in part or whole, and may have applied.
    Lost { reason: String },
}

/// An answer as it travels: what the model server renders and the transports parse.
#[cfg_attr(
    not(any(test, feature = "model")),
    expect(dead_code, reason = "the model server's side of the protocol")
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawAnswer {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(&'static str, String)>,
    pub(crate) body: Vec<u8>,
}

#[cfg_attr(
    not(any(test, feature = "model")),
    expect(dead_code, reason = "the model server's side of the protocol")
)]
impl RawAnswer {
    pub(crate) fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Adds a header.
    pub(crate) fn header(&mut self, name: &'static str, value: impl Into<String>) {
        self.headers.push((name, value.into()));
    }

    /// The callback the crate's `render` methods emit into.
    pub(crate) fn emit(&mut self) -> impl FnMut(&'static str, &str) + '_ {
        |name, value| self.headers.push((name, value.to_owned()))
    }

    /// The answer as the client reads it.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "tests read the model's answers as the client does"
        )
    )]
    pub(crate) fn parse(&self, request: &Request) -> Answer {
        // A header read keeps the header alone of a whole body (`200`), as the HTTP transport
        // does.
        let cut = request.whole_answer_cut().filter(|_| self.status == 200);
        let body = match cut {
            Some(n) => self
                .body
                .get(..usize::try_from(n).unwrap_or(usize::MAX))
                .unwrap_or(&self.body)
                .to_vec(),
            None => self.body.clone(),
        };
        parse_answer(
            request,
            self.status,
            self.headers
                .iter()
                .map(|(name, value)| (*name, value.as_bytes())),
            body,
        )
    }
}

/// Reads an answer to `request` from its status, `(name, value)` header pairs and body.
pub(crate) fn parse_answer<'a, I>(
    request: &Request,
    status: u16,
    headers: I,
    body: Vec<u8>,
) -> Answer
where
    I: IntoIterator<Item = (&'a str, &'a [u8])> + Clone,
{
    let plain = |name| text(headers.clone(), name).ok().flatten();
    Answer {
        status,
        typed: typed(request, status, headers.clone()),
        outcome: request
            .write_kind()
            .map(|kind| classify(kind, status, headers.clone(), &body)),
        retry_after: plain(RETRY_AFTER)
            .and_then(|seconds| seconds.parse().ok())
            .map(Duration::from_secs),
        location: plain(LOCATION).map(str::to_owned),
        content_type: plain(CONTENT_TYPE).map(str::to_owned),
        body,
    }
}

fn typed<'a, I>(request: &Request, status: u16, headers: I) -> Result<Typed, HeaderError>
where I: IntoIterator<Item = (&'a str, &'a [u8])> + Clone {
    if let Some(routing) = RoutingAnswer::parse(status, headers.clone())? {
        return Ok(Typed::Routing(routing));
    }
    if status == SizeLimit::STATUS {
        return Ok(Typed::TooLarge(SizeLimit::parse(status, headers)?));
    }
    if let Some(mismatch) = IncarnationMismatch::parse(status, headers.clone())? {
        return Ok(Typed::IncarnationMismatch(mismatch));
    }
    let typed = match (request.method, request.target) {
        (Method::Head, _) => match ServerIdentity::parse(headers.clone())? {
            None => Typed::Head {
                identity: None,
                stream: None,
            },
            Some(identity) => Typed::Head {
                identity: Some(identity),
                stream: (status == 200)
                    .then(|| StreamHead::parse(headers))
                    .transpose()?,
            },
        },
        (Method::Post, Target::Stream) => {
            let producer = match request.keys.producer {
                Some(_) => ProducerAnswer::parse(status, headers.clone())?,
                None => None,
            };
            match producer {
                Some(answer) => Typed::Producer(answer),
                // A success to a keyed write is always a typed producer answer (extensions:
                // Producer Write Answers).
                None if request.keys.producer.is_some() && (200..300).contains(&status) => {
                    return Err(HeaderError::Missing {
                        name: names::PRODUCER_EPOCH,
                    });
                }
                None => StreamClosedConflict::parse(status, headers)?
                    .map_or(Typed::Untyped, Typed::StreamClosed),
            }
        }
        (Method::Put, Target::Stream) if matches!(status, 200 | 201) => Typed::Created {
            incarnation: required(names::STREAM_INCARNATION, headers, Incarnation::parse)?,
        },
        (Method::Put, Target::Snapshot { .. }) => {
            SnapshotPublishAnswer::parse(status, headers)?.map_or(Typed::Untyped, Typed::Snapshot)
        }
        (Method::Post, Target::SnapshotParts { .. }) if status == 201 => {
            Typed::PartStaged(PartStaged::parse(headers)?)
        }
        (Method::Post, Target::SnapshotParts { .. })
            if status == 429
                && (text(headers.clone(), MAX_STAGED_PARTS)?.is_some()
                    || text(headers.clone(), MAX_STAGED_BYTES)?.is_some()) =>
        {
            Typed::StagedPartsFull
        }
        (Method::Get, Target::Snapshot { .. }) if matches!(status, 200 | 206) => {
            let range = match ContentRange::parse(status, headers.clone())? {
                None => None,
                Some(ContentRange::Partial(span)) => Some(span),
                // `parse` takes `bytes */total` only with a `416`.
                Some(ContentRange::Unsatisfied { .. }) => {
                    return Err(HeaderError::InvalidContentRange);
                }
            };
            Typed::SnapshotRead {
                read: SnapshotRead::parse(headers)?,
                range,
            }
        }
        (Method::Get, Target::Read { .. } | Target::Snapshot { .. })
            if matches!(status, 404 | 410 | 416) =>
        {
            Typed::ReadRefused {
                incarnation: text(headers, names::STREAM_INCARNATION)?
                    .map(|token| Incarnation::parse(names::STREAM_INCARNATION, token))
                    .transpose()?,
            }
        }
        (Method::Get, Target::Read { .. }) if status == 200 => Typed::Read {
            next: required(names::STREAM_NEXT_OFFSET, headers.clone(), Offset::parse)?,
            up_to_date: flag(headers.clone(), names::STREAM_UP_TO_DATE)?,
            closed: flag(headers, names::STREAM_CLOSED)?,
        },
        (Method::Put, Target::Retention { .. }) if status == 204 => Typed::Retained {
            offset: required(names::STREAM_RETAINED_OFFSET, headers, Offset::parse)?,
        },
        _ => Typed::Untyped,
    };
    Ok(typed)
}

/// The value of header `name` as text, trimmed of field whitespace: `None` when absent, an error
/// when repeated or not visible ASCII. The rules of `ursula_headers`' own field reader, which the
/// crate does not export.
fn text<'a, I>(headers: I, name: &'static str) -> Result<Option<&'a str>, HeaderError>
where I: IntoIterator<Item = (&'a str, &'a [u8])> {
    let mut found = None;
    for (key, value) in headers {
        if key.eq_ignore_ascii_case(name) {
            if found.is_some() {
                return Err(HeaderError::Duplicate { name });
            }
            found = Some(value);
        }
    }
    let Some(value) = found else {
        return Ok(None);
    };
    if !value
        .iter()
        .all(|byte| *byte == b'\t' || (0x20..=0x7e).contains(byte))
    {
        return Err(HeaderError::NotText { name });
    }
    let text = std::str::from_utf8(value).map_err(|_not_utf8| HeaderError::NotText { name })?;
    Ok(Some(text.trim_matches([' ', '\t'])))
}

/// Header `name`, which the answer requires, parsed with the crate's value type.
fn required<'a, I, T>(
    name: &'static str,
    headers: I,
    parse: fn(&'static str, &str) -> Result<T, HeaderError>,
) -> Result<T, HeaderError>
where
    I: IntoIterator<Item = (&'a str, &'a [u8])>,
{
    let value = text(headers, name)?.ok_or(HeaderError::Missing { name })?;
    parse(name, value)
}

/// A flag header: absent, or exactly `true`.
fn flag<'a, I>(headers: I, name: &'static str) -> Result<bool, HeaderError>
where I: IntoIterator<Item = (&'a str, &'a [u8])> {
    match text(headers, name)? {
        None => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(HeaderError::InvalidFlag { name }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ursula_headers::BodyDigest;
    use ursula_headers::ByteRange;
    use ursula_headers::Epoch;
    use ursula_headers::HeaderError;
    use ursula_headers::Incarnation;
    use ursula_headers::IncarnationPrecondition;
    use ursula_headers::Offset;
    use ursula_headers::PartId;
    use ursula_headers::PartStaged;
    use ursula_headers::PartsCommit;
    use ursula_headers::PartsManifest;
    use ursula_headers::ProducerId;
    use ursula_headers::ProducerRequestHeaders;
    use ursula_headers::ProducerWrite;
    use ursula_headers::Seq;
    use ursula_headers::SnapshotDigest;
    use ursula_headers::WriteKind;

    use super::Method;
    use super::PRODUCER_ID;
    use super::RawAnswer;
    use super::Request;
    use super::SnapshotPart;
    use super::Target;
    use super::Typed;
    use super::WireError;

    /// `Request::write_kind` for each request the VFS sends, built as its sender builds it: the
    /// kind `ursula_headers::classify` reads the answer as, and none for a read or a HEAD.
    #[test]
    fn every_request_the_vfs_sends_has_its_write_kind() {
        let offset = Offset::from_u64(64);
        let incarnation = Incarnation::try_from(1 << 52).unwrap();
        let precondition = || ProducerRequestHeaders {
            producer: None,
            precondition: Some(IncarnationPrecondition { incarnation }),
        };
        // An append or a claim (`Producer-Seq: 0`): the replicator's and the import's.
        let keyed = |seq: u64| Request {
            keys: ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: ProducerId::new(PRODUCER_ID).unwrap(),
                    epoch: Epoch::try_from(3).unwrap(),
                    seq: Seq::try_from(seq).unwrap(),
                    digest: Some(BodyDigest::from_bytes([7; 32])),
                }),
                precondition: Some(IncarnationPrecondition { incarnation }),
            },
            content_type: Some("application/vnd.ursula.sqlite-vfs.v2".to_owned()),
            body: Arc::from(b"frame".to_vec()),
            ..Request::new(Method::Post, Target::Stream)
        };
        let manifest = PartsManifest::new(vec![PartId::new("a.part").unwrap()]).unwrap();
        let digest = SnapshotDigest::from_bytes([9; 32]);
        let cases = [
            ("append", keyed(4), Some(WriteKind::Producer)),
            ("claim", keyed(0), Some(WriteKind::Producer)),
            (
                "create",
                Request {
                    content_type: Some("application/vnd.ursula.sqlite-vfs.v2".to_owned()),
                    ..Request::new(Method::Put, Target::Stream)
                },
                Some(WriteKind::Create),
            ),
            (
                "snapshot publish",
                Request {
                    keys: precondition(),
                    snapshot_digest: Some(digest),
                    body: Arc::from(b"USS3".to_vec()),
                    ..Request::new(Method::Put, Target::Snapshot { offset })
                },
                Some(WriteKind::Publish),
            ),
            (
                "parts commit",
                Request {
                    keys: precondition(),
                    parts: Some(Box::new(PartsCommit::for_manifest(&manifest, digest))),
                    body: Arc::from(manifest.render().into_bytes()),
                    ..Request::new(Method::Put, Target::Snapshot { offset })
                },
                Some(WriteKind::Publish),
            ),
            (
                "part",
                Request {
                    keys: precondition(),
                    body: Arc::from(b"part".to_vec()),
                    ..Request::new(Method::Post, Target::SnapshotParts { offset })
                },
                Some(WriteKind::PartStage),
            ),
            (
                "parts discard",
                Request {
                    keys: precondition(),
                    ..Request::new(Method::Delete, Target::StagedParts)
                },
                Some(WriteKind::PartsDiscard),
            ),
            (
                "retention move",
                Request {
                    keys: precondition(),
                    ..Request::new(Method::Put, Target::Retention { offset })
                },
                Some(WriteKind::Retention),
            ),
            ("HEAD", Request::new(Method::Head, Target::Stream), None),
            ("read", Request::read(Some(offset), false, 1 << 20), None),
            ("leader read", Request::read(None, true, 1 << 20), None),
            (
                "snapshot header read",
                Request::snapshot_read(offset, incarnation, SnapshotPart::Header),
                None,
            ),
            (
                "snapshot chunk read",
                Request::snapshot_read(offset, incarnation, SnapshotPart::Chunk {
                    first: 0,
                    last: 7,
                }),
                None,
            ),
        ];
        for (what, request, kind) in cases {
            assert_eq!(request.write_kind(), kind, "{what}");
            // The answer carries an outcome exactly when the request is a write.
            let answer = RawAnswer::new(503).parse(&request);
            assert_eq!(answer.outcome.is_some(), kind.is_some(), "{what}");
        }
    }

    fn rendered(request: &Request) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        request.render_headers(|name, value| out.push((name, value.to_owned())));
        out
    }

    #[test]
    fn targets_split_back_from_their_suffix() {
        let offset = Offset::from_u64(4096);
        for (method, target) in [
            (Method::Head, Target::Stream),
            (Method::Post, Target::Stream),
            (Method::Get, Target::Read {
                from: None,
                leader: false,
            }),
            (Method::Get, Target::Read {
                from: Some(offset),
                leader: true,
            }),
            (Method::Put, Target::Snapshot { offset }),
            (Method::Get, Target::Snapshot { offset }),
            (Method::Post, Target::SnapshotParts { offset }),
            (Method::Delete, Target::StagedParts),
            (Method::Put, Target::Retention { offset }),
        ] {
            let path = format!("/v1/stream/b/s{}", target.suffix());
            assert_eq!(
                Target::split(method, &path),
                Ok(("/v1/stream/b/s", target)),
                "{path}"
            );
        }
        assert!(matches!(
            Target::split(Method::Get, "/s?offset=12"),
            Err(WireError::Header(HeaderError::InvalidOffset { .. }))
        ));
        assert_eq!(
            Target::split(Method::Get, "/s?live=sse"),
            Err(WireError::Query("live=sse".to_owned()))
        );
    }

    #[test]
    fn requests_parse_back_from_their_headers() {
        let request = Request {
            keys: ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: ProducerId::new(PRODUCER_ID).unwrap(),
                    epoch: Epoch::try_from(3).unwrap(),
                    seq: Seq::try_from(7).unwrap(),
                    digest: Some(BodyDigest::from_bytes([7; 32])),
                }),
                precondition: Some(IncarnationPrecondition {
                    incarnation: Incarnation::try_from(1 << 52).unwrap(),
                }),
            },
            content_type: Some("application/vnd.ursula.sqlite-vfs.v2".to_owned()),
            snapshot_digest: Some(SnapshotDigest::from_bytes([9; 32])),
            closes: true,
            body: Arc::from(b"frame".to_vec()),
            range: Some(ByteRange::Bounded {
                first: 65_536,
                last: 131_071,
            }),
            ..Request::new(Method::Post, Target::Stream)
        };
        let headers = rendered(&request);
        let parsed = Request::parse(
            request.method,
            request.target,
            headers
                .iter()
                .map(|(name, value)| (*name, value.as_bytes())),
            request.body.to_vec(),
        );
        assert_eq!(parsed, Ok(request));
    }

    // A parts commit renders its count and digest through `PartsCommit`, and its manifest is
    // the body; a staged part's `201` names its id.
    #[test]
    fn a_parts_commit_and_a_staged_part_travel_typed() {
        let manifest = PartsManifest::new(vec![
            PartId::new("a.part").unwrap(),
            PartId::new("b.part").unwrap(),
        ])
        .unwrap();
        let digest = SnapshotDigest::from_bytes([9; 32]);
        let commit = Request {
            parts: Some(Box::new(PartsCommit::for_manifest(&manifest, digest))),
            content_type: Some("application/vnd.ursula.sqlite-vfs.v2".to_owned()),
            body: Arc::from(manifest.render().into_bytes()),
            ..Request::new(Method::Put, Target::Snapshot {
                offset: Offset::from_u64(64),
            })
        };
        let headers = rendered(&commit);
        assert!(headers.contains(&("stream-snapshot-parts", "2".to_owned())));
        assert_eq!(commit.body.as_ref(), b"a.part\nb.part\n");
        let parsed = Request::parse(
            commit.method,
            commit.target,
            headers
                .iter()
                .map(|(name, value)| (*name, value.as_bytes())),
            commit.body.to_vec(),
        );
        assert_eq!(parsed, Ok(commit));
        let stage = Request::new(Method::Post, Target::SnapshotParts {
            offset: Offset::from_u64(64),
        });
        let mut raw = RawAnswer::new(201);
        raw.header("stream-snapshot-part", "a.part");
        assert_eq!(
            raw.parse(&stage).typed,
            Ok(Typed::PartStaged(PartStaged {
                part: PartId::new("a.part").unwrap()
            }))
        );
        assert_eq!(
            RawAnswer::new(201).parse(&stage).typed,
            Err(HeaderError::Missing {
                name: "stream-snapshot-part"
            })
        );
    }

    // An answer may wait beyond the base for the server to store the body, or for a commit to
    // copy its parts, each at the 32 MiB request limit: 8 MiB/s, as a gateway waits.
    #[test]
    fn an_answer_waits_for_the_bytes_the_server_stores() {
        let manifest = PartsManifest::new(vec![
            PartId::new("a.part").unwrap(),
            PartId::new("b.part").unwrap(),
        ])
        .unwrap();
        let commit = Request {
            parts: Some(Box::new(PartsCommit::for_manifest(
                &manifest,
                SnapshotDigest::from_bytes([9; 32]),
            ))),
            body: Arc::from(manifest.render().into_bytes()),
            ..Request::new(Method::Put, Target::Snapshot {
                offset: Offset::from_u64(64),
            })
        };
        assert_eq!(commit.answer_wait(), std::time::Duration::from_secs(8));
        let part = Request {
            body: Arc::from(vec![0; 4 << 20]),
            ..Request::new(Method::Post, Target::SnapshotParts {
                offset: Offset::from_u64(64),
            })
        };
        assert_eq!(part.answer_wait(), std::time::Duration::from_millis(500));
        let read = Request::read(None, true, 1 << 20);
        assert_eq!(read.answer_wait(), std::time::Duration::ZERO);
    }

    #[test]
    fn a_repeated_header_is_malformed() {
        let mut raw = RawAnswer::new(200);
        raw.header("stream-next-offset", Offset::ZERO.to_string());
        raw.header("stream-next-offset", Offset::ZERO.to_string());
        let read = Request::read(None, true, 1 << 20);
        assert_eq!(
            raw.parse(&read).typed,
            Err(HeaderError::Duplicate {
                name: "stream-next-offset"
            })
        );
    }

    // A 429 to a part is the staged-parts limit only when it names one; otherwise a rate limit.
    #[test]
    fn a_429_to_a_part_is_the_staged_limit_only_when_it_names_one() {
        let part = Request::new(Method::Post, Target::SnapshotParts {
            offset: Offset::from_u64(4096),
        });
        assert_eq!(RawAnswer::new(429).parse(&part).typed, Ok(Typed::Untyped));
        for name in [super::MAX_STAGED_PARTS, super::MAX_STAGED_BYTES] {
            let mut raw = RawAnswer::new(429);
            raw.header(name, "8192");
            assert_eq!(raw.parse(&part).typed, Ok(Typed::StagedPartsFull));
        }
    }

    #[test]
    fn a_head_without_identity_reads_no_metadata() {
        let mut raw = RawAnswer::new(200);
        raw.header("stream-next-offset", "12");
        let head = Request::new(Method::Head, Target::Stream);
        assert_eq!(
            raw.parse(&head).typed,
            Ok(Typed::Head {
                identity: None,
                stream: None
            })
        );
    }

    #[test]
    fn a_success_to_a_keyed_write_without_producer_headers_is_malformed() {
        let keyed = Request {
            keys: ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: ProducerId::new(PRODUCER_ID).unwrap(),
                    epoch: Epoch::ZERO,
                    seq: Seq::ZERO,
                    digest: None,
                }),
                precondition: None,
            },
            ..Request::new(Method::Post, Target::Stream)
        };
        for status in [200, 204] {
            assert_eq!(
                RawAnswer::new(status).parse(&keyed).typed,
                Err(HeaderError::Missing {
                    name: "producer-epoch"
                })
            );
        }
        let unkeyed = Request::new(Method::Post, Target::Stream);
        assert_eq!(
            RawAnswer::new(204).parse(&unkeyed).typed,
            Ok(Typed::Untyped)
        );
    }

    #[test]
    fn a_ranged_snapshot_answer_names_its_bytes() {
        let read = Request::snapshot_read(
            Offset::from_u64(64),
            Incarnation::try_from(1 << 52).unwrap(),
            super::SnapshotPart::Chunk { first: 4, last: 7 },
        );
        assert_eq!(read.answer_bytes, 4);
        assert_eq!(read.whole_answer_cut(), None);
        let mut raw = RawAnswer::new(206);
        raw.header("stream-snapshot-offset", Offset::from_u64(64).to_string());
        raw.header(
            "stream-snapshot-digest",
            SnapshotDigest::from_bytes([9; 32]).to_string(),
        );
        raw.header("stream-next-offset", Offset::from_u64(64).to_string());
        raw.header("stream-incarnation", (1u64 << 52).to_string());
        raw.body = b"abcd".to_vec();
        // A `206` names its bytes; without `Content-Range` it is malformed.
        assert_eq!(
            raw.parse(&read).typed,
            Err(HeaderError::Missing {
                name: "content-range"
            })
        );
        raw.header("content-range", "bytes 4-7/12");
        let answer = raw.parse(&read);
        let Ok(Typed::SnapshotRead {
            range: Some(span), ..
        }) = answer.typed
        else {
            panic!("{answer:?}");
        };
        assert_eq!((span.first(), span.last(), span.total()), (4, 7, 12));
        assert_eq!(answer.body, b"abcd");
        // A header read keeps the header alone of a whole body, and all of a `206`.
        let header = Request::snapshot_read(
            Offset::from_u64(64),
            Incarnation::try_from(1 << 52).unwrap(),
            super::SnapshotPart::Header,
        );
        assert_eq!(header.whole_answer_cut(), Some(56));
    }

    #[test]
    fn plain_http_headers_are_read() {
        let mut raw = RawAnswer::new(503);
        raw.header("retry-after", "7");
        raw.header("location", "http://elsewhere/s");
        let answer = raw.parse(&Request::new(Method::Post, Target::Stream));
        assert_eq!(answer.typed, Ok(Typed::Untyped));
        assert_eq!(answer.retry_after, Some(std::time::Duration::from_secs(7)));
        assert_eq!(answer.location.as_deref(), Some("http://elsewhere/s"));
    }
}
