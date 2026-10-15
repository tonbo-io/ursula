//! The protocol's requests and answers as values (#581 revision 2), shared by the transports and
//! the model server, so both sides of the protocol build and read the same structs.
//!
//! Every header #581 types goes through `ursula_headers`, never a local parser: producer keys and
//! answers, HEAD metadata, snapshot answers, the typed `413`, the `412` of the incarnation
//! precondition, the `409` of a closed stream and routing answers. Three answers the crate does
//! not type yet (a read's `Stream-Next-Offset`, a create's `Stream-Incarnation`, retention's
//! `Stream-Retained-Offset`) are picked out here and parsed with the crate's value types.
//! `Retry-After`, `Location` and `Content-Type` are plain HTTP and read here.
//!
//! [`parse_answer`] reads an answer in the light of the request it answers: a `409` to a keyed
//! append is the producer's or a closed stream's, a `204` to a snapshot publish is a published
//! snapshot.

use std::sync::Arc;
use std::time::Duration;

use ursula_headers::HeaderError;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationMismatch;
use ursula_headers::Offset;
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
use ursula_headers::names;

/// `Producer-Id` of every keyed write: constant for the `USQ2` format (design §4.6).
pub(crate) const PRODUCER_ID: &str = "sqlite-ursula-vfs/2";

/// `Content-Type` header name.
pub(crate) const CONTENT_TYPE: &str = "content-type";

/// `Retry-After` header name.
pub(crate) const RETRY_AFTER: &str = "retry-after";

/// `Location` header name.
pub(crate) const LOCATION: &str = "location";

/// The base protocol's read offset for the start of a stream.
const START: &str = "-1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Method {
    Head,
    Get,
    Put,
    Post,
}

impl Method {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text {
            "HEAD" => Some(Self::Head),
            "GET" => Some(Self::Get),
            "PUT" => Some(Self::Put),
            "POST" => Some(Self::Post),
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
    /// The snapshot at `offset`: a publish (`PUT`) or a read (`GET`).
    Snapshot { offset: Offset },
    /// The retention boundary (`PUT`), monotonic (#581 §3).
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
            Self::Retention { offset } => format!("/retention/{offset}"),
        }
    }

    /// Splits a request target (path and query) into the stream's path and the [`Target`]: the
    /// inverse of [`Target::suffix`]. A `GET` of the stream's path is a read.
    pub(crate) fn split(method: Method, target: &str) -> Result<(&str, Self), WireError> {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
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

    fn snapshot(offset: Offset) -> Self {
        Self::Snapshot { offset }
    }

    fn retention(offset: Offset) -> Self {
        Self::Retention { offset }
    }
}

/// A request that a transport cannot carry, or that the model server cannot read.
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
    pub(crate) body: Arc<[u8]>,
}

impl Request {
    /// A request without headers or body.
    pub(crate) fn new(method: Method, target: Target) -> Self {
        Self {
            method,
            target,
            keys: ProducerRequestHeaders::default(),
            content_type: None,
            snapshot_digest: None,
            body: Arc::from(Vec::new()),
        }
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
    }

    /// The request a server reads from `method`, `target`, the `(name, value)` header pairs and
    /// the body: the inverse of [`Request::render_headers`].
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
        let snapshot_digest = text(headers, names::STREAM_SNAPSHOT_DIGEST)?
            .map(|digest| SnapshotDigest::parse(names::STREAM_SNAPSHOT_DIGEST, digest))
            .transpose()?;
        Ok(Self {
            method,
            target,
            keys,
            content_type,
            snapshot_digest,
            body: Arc::from(body),
        })
    }
}

/// What an answer says in the headers `ursula_headers` types, read in the light of the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Typed {
    /// No typed header applies: the status alone speaks.
    Untyped,
    /// An answer to a keyed write (#581 §2.3.1, §2.4).
    Producer(ProducerAnswer),
    /// `409 Stream-Closed`: the stream is closed and the append did not apply.
    StreamClosed(StreamClosedConflict),
    /// `412`: the `Stream-Incarnation` precondition failed.
    IncarnationMismatch(IncarnationMismatch),
    /// `413`, with the limit it applied when a layer of #581 §6.4 sent it.
    TooLarge(Option<SizeLimit>),
    /// `307` or `503` of a node that does not lead the stream's group.
    Routing(RoutingAnswer),
    /// A `HEAD`: the server's identity on every answer (A1), and the stream's metadata on a `200`.
    /// A server that sends no identity speaks protocol 1, whose metadata is not read.
    Head {
        identity: Option<ServerIdentity>,
        stream: Option<StreamHead>,
    },
    /// `201` or `200` to a create: the stream exists with this token.
    Created { incarnation: Incarnation },
    /// `204` or `409` to a snapshot publish.
    Snapshot(SnapshotPublishAnswer),
    /// `200` to a snapshot read.
    SnapshotRead(SnapshotRead),
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawAnswer {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(&'static str, String)>,
    pub(crate) body: Vec<u8>,
}

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
    pub(crate) fn parse(&self, request: &Request) -> Answer {
        parse_answer(
            request,
            self.status,
            self.headers
                .iter()
                .map(|(name, value)| (*name, value.as_bytes())),
            self.body.clone(),
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
        return Ok(Typed::TooLarge(SizeLimit::parse(headers)?));
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
        (Method::Get, Target::Snapshot { .. }) if status == 200 => {
            Typed::SnapshotRead(SnapshotRead::parse(headers)?)
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
    use ursula_headers::Epoch;
    use ursula_headers::HeaderError;
    use ursula_headers::Incarnation;
    use ursula_headers::IncarnationPrecondition;
    use ursula_headers::Offset;
    use ursula_headers::ProducerId;
    use ursula_headers::ProducerRequestHeaders;
    use ursula_headers::ProducerWrite;
    use ursula_headers::Seq;
    use ursula_headers::SnapshotDigest;

    use super::Method;
    use super::PRODUCER_ID;
    use super::RawAnswer;
    use super::Request;
    use super::Target;
    use super::Typed;
    use super::WireError;

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
            body: Arc::from(b"frame".to_vec()),
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

    #[test]
    fn a_repeated_header_is_malformed() {
        let mut raw = RawAnswer::new(200);
        raw.header("stream-next-offset", Offset::ZERO.to_string());
        raw.header("stream-next-offset", Offset::ZERO.to_string());
        let read = Request::new(Method::Get, Target::Read {
            from: None,
            leader: true,
        });
        assert_eq!(
            raw.parse(&read).typed,
            Err(HeaderError::Duplicate {
                name: "stream-next-offset"
            })
        );
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
