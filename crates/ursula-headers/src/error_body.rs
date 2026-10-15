//! The one body of every error answer (extensions Section 11): a JSON object
//! whose `error` member is an object with two string members, `code`, a
//! stable [`ErrorCode`], and `message`, a human-readable detail. It is the
//! shape the Durable Streams protocol uses for its subscription API errors.
//!
//! ```text
//! {"error":{"code":"INVALID_HEADER","message":"stream-ttl must not contain leading zeros"}}
//! ```
//!
//! The body adds the code to what the status and the typed headers say: a
//! `413` names its limit in a header and its code here, a `503` keeps its
//! `Retry-After`. The server and the gateway render every error body with
//! [`ErrorBody::render`], so a client matches the code instead of parsing
//! text. The message is informative only: its text may change in any
//! release, and a client never matches it.
//!
//! The body is extensible inside a minor version: a later version may add
//! members, which [`ErrorBody::parse`] skips, and codes, which it keeps as
//! [`ErrorCode::Unknown`].

use crate::error::HeaderError;

/// Defines [`ErrorCode`]: each variant with its wire name and its default
/// message.
macro_rules! error_codes {
    ($($(#[doc = $doc:literal])+ $code:ident = $name:literal, $message:literal;)+) => {
        /// The stable code of an error answer. The wire names are fixed
        /// strings, so renaming a variant changes no code.
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ErrorCode {
            $($(#[doc = $doc])+ $code,)+
            /// A code this version does not know, as the body named it. A
            /// client acts on the status alone.
            Unknown(String),
        }

        impl ErrorCode {
            /// Every known code, in declaration order.
            pub const ALL: &[Self] = &[$(Self::$code,)+];

            /// The code as it appears in a body.
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$code => $name,)+
                    Self::Unknown(name) => name,
                }
            }

            /// The fixed message of the code, for an answer whose detail
            /// would name the server's internals.
            pub fn default_message(&self) -> &'static str {
                match self {
                    $(Self::$code => $message,)+
                    Self::Unknown(_) => "the request was refused",
                }
            }

            /// The code `name` names: [`ErrorCode::Unknown`] for a name this
            /// version does not know.
            pub fn from_name(name: &str) -> Self {
                match name {
                    $($name => Self::$code,)+
                    other => Self::Unknown(other.to_owned()),
                }
            }
        }
    };
}

error_codes! {
    // Stream state (one code per refusal of the stream state machine).
    /// The bucket id is not a valid bucket id (`400`).
    InvalidBucketId = "INVALID_BUCKET_ID", "the bucket id is invalid";
    /// The stream id is not a valid stream id (`400`).
    InvalidStreamId = "INVALID_STREAM_ID", "the stream id is invalid";
    /// No such bucket (`404`).
    BucketNotFound = "BUCKET_NOT_FOUND", "the bucket does not exist";
    /// The bucket was purged and its name is fenced from reuse (`404`).
    BucketErased = "BUCKET_ERASED", "the bucket was erased";
    /// No such stream (`404`).
    StreamNotFound = "STREAM_NOT_FOUND", "the stream does not exist";
    /// The offset is below the stream's retained offset, or the stream is
    /// gone (`410`).
    StreamGone = "STREAM_GONE", "the offset is below the stream's retained history";
    /// A create of a stream that exists with other settings (`409`).
    StreamAlreadyExistsConflict = "STREAM_ALREADY_EXISTS_CONFLICT", "the stream exists with other settings";
    /// A write with a body names no `Content-Type` (`400`).
    MissingContentType = "MISSING_CONTENT_TYPE", "a write with a body must include a content type";
    /// The write's `Content-Type` is not the stream's (`409`).
    ContentTypeMismatch = "CONTENT_TYPE_MISMATCH", "the content type does not match the stream's";
    /// An append with no body that does not close the stream (`400`).
    EmptyAppend = "EMPTY_APPEND", "an append must carry a body unless it closes the stream";
    /// The stream is closed (`409`, with `Stream-Closed`).
    StreamClosed = "STREAM_CLOSED", "the stream is closed";
    /// `Stream-Seq` is not above the stream's last one (`409`).
    StreamSeqConflict = "STREAM_SEQ_CONFLICT", "Stream-Seq is not above the stream's last one";
    /// The producer headers are invalid, or a producer write's epoch was
    /// never claimed (`400`).
    InvalidProducer = "INVALID_PRODUCER", "the producer headers are invalid";
    /// A claim closed the producer's epoch (`403 epoch-closed`).
    ProducerEpochClosed = "PRODUCER_EPOCH_CLOSED", "the producer's epoch is closed";
    /// The stream forgot the producer's epoch (`403 producer-expired`).
    ProducerExpired = "PRODUCER_EXPIRED", "the producer's epoch is not remembered";
    /// The producer's epoch is below its current one (`403 stale-epoch`).
    ProducerEpochStale = "PRODUCER_EPOCH_STALE", "the producer's epoch is stale";
    /// The write's key holds another write (`403 key-conflict`).
    ProducerKeyConflict = "PRODUCER_KEY_CONFLICT", "the write's key holds a write with another identity";
    /// The body does not match `Producer-Digest` (`400`).
    ProducerDigestMismatch = "PRODUCER_DIGEST_MISMATCH", "Producer-Digest does not match the request body";
    /// `Producer-Seq` is not the next one (`409 seq-gap`).
    ProducerSeqConflict = "PRODUCER_SEQ_CONFLICT", "Producer-Seq is out of order";
    /// The retention offset is invalid (`400`).
    InvalidRetention = "INVALID_RETENTION", "the retention offset is invalid";
    /// The offset is beyond the stream's tail (`416`).
    OffsetOutOfRange = "OFFSET_OUT_OF_RANGE", "the offset is beyond the stream's tail";
    /// A snapshot read's `Range` lies outside the snapshot (`416`, with
    /// `Content-Range: bytes */<size>`).
    RangeNotSatisfiable = "RANGE_NOT_SATISFIABLE", "the range lies outside the snapshot";
    /// A cold flush the stream cannot take (`400`).
    InvalidColdFlush = "INVALID_COLD_FLUSH", "the cold flush is invalid";
    /// The snapshot is invalid (`400`).
    InvalidSnapshot = "INVALID_SNAPSHOT", "the snapshot is invalid";
    /// No snapshot at that offset (`404`).
    SnapshotNotFound = "SNAPSHOT_NOT_FOUND", "no snapshot at that offset";
    /// Another snapshot is the latest at or after the offset, or the
    /// offset is beyond the tail (`409`).
    SnapshotConflict = "SNAPSHOT_CONFLICT", "another snapshot is the latest, or the offset is beyond the tail";
    /// A JSON body is not valid JSON message text (`400`).
    InvalidRecordBoundaries = "INVALID_RECORD_BOUNDARIES", "the body is not valid JSON message text";
    /// Ursula could not check that the offset of a JSON snapshot or
    /// retention request is a message boundary (`503`).
    JsonBoundaryUnverified = "JSON_BOUNDARY_UNVERIFIED", "the JSON message boundary could not be verified; retry";
    /// A state import targeted a group that holds data (`409`).
    ImportConflict = "IMPORT_CONFLICT", "the import target holds data";
    /// A state import is invalid (`400`).
    ImportInvalid = "IMPORT_INVALID", "the import is invalid";
    /// The stream holds as many producers as it may (`429`).
    ProducerLimit = "PRODUCER_LIMIT", "the stream holds as many producers as it may";
    /// The server's clock is out of range (`500`).
    CreatedAtOutOfRange = "CREATED_AT_OUT_OF_RANGE", "the server's clock is out of range";
    /// `Stream-Incarnation` does not match the stream (`412`).
    IncarnationMismatch = "INCARNATION_MISMATCH", "Stream-Incarnation does not match the stream";
    /// The stream's group has not applied its history yet (`503`).
    HistoryPending = "HISTORY_PENDING", "the stream's group is not ready yet; retry";

    // The request.
    /// A `4xx` that no other code names: a malformed request, or a refusal
    /// of a layer in front of the handlers.
    InvalidRequest = "INVALID_REQUEST", "the request is malformed";
    /// A header value is malformed (`400`).
    InvalidHeader = "INVALID_HEADER", "a header value is malformed";
    /// A query parameter is malformed or does not fit the request (`400`).
    InvalidQuery = "INVALID_QUERY", "a query parameter is malformed";
    /// An offset in the path or query is malformed (`400`).
    InvalidOffset = "INVALID_OFFSET", "the offset is malformed";
    /// An immutable resource (extensions Section 4.1) carries a query
    /// component (`400`).
    QueryNotAccepted = "QUERY_NOT_ACCEPTED", "an immutable resource takes no query";
    /// The token or the offset in an immutable resource's path is not in the
    /// form the server issues (`400`).
    InvalidResource = "INVALID_RESOURCE", "the token or offset in the path is malformed";
    /// The request uses a parameter, header or route that was removed
    /// (`400`, or `405` on the bare snapshot path).
    Unsupported = "UNSUPPORTED", "the request uses a removed feature";
    /// The request body could not be read (`400`).
    BodyUnreadable = "BODY_UNREADABLE", "the request body could not be read";
    /// The offset of a JSON snapshot or retention request is not a message
    /// boundary (`400`).
    NotJsonBoundary = "NOT_JSON_BOUNDARY", "the offset is not a JSON message boundary";
    /// No route serves the path (`404`).
    NotFound = "NOT_FOUND", "no route serves this path";
    /// The route does not serve the method (`405`).
    MethodNotAllowed = "METHOD_NOT_ALLOWED", "the route does not serve this method";
    /// The body is above the request limit, `Ursula-Max-Request-Bytes`
    /// (`413`).
    RequestTooLarge = "REQUEST_TOO_LARGE", "the request body is above the request limit";
    /// The assembled snapshot is above the snapshot limit,
    /// `Ursula-Max-Snapshot-Bytes` (`413`).
    SnapshotTooLarge = "SNAPSHOT_TOO_LARGE", "the snapshot is above the snapshot limit";

    // Snapshot parts.
    /// The node has no cold store, which the request needs (`501`).
    NoColdStore = "NO_COLD_STORE", "this node has no cold store";
    /// A parts commit's headers or manifest are malformed (`400`).
    InvalidPartsCommit = "INVALID_PARTS_COMMIT", "the parts commit is malformed";
    /// A manifest names a part that is not staged for the stream (`400`).
    UnknownPart = "UNKNOWN_PART", "a part is not staged for this stream";
    /// The assembled snapshot's digest is not `Stream-Snapshot-Digest`
    /// (`400`).
    SnapshotDigestMismatch = "SNAPSHOT_DIGEST_MISMATCH", "the assembled snapshot's digest differs from Stream-Snapshot-Digest";
    /// The node copies as many snapshot commits as it may (`503`).
    SnapshotCommitsBusy = "SNAPSHOT_COMMITS_BUSY", "this node copies as many snapshot commits as it may; retry";
    /// The node runs this commit now (`503`): repeat it.
    SnapshotCommitInProgress = "SNAPSHOT_COMMIT_IN_PROGRESS", "this commit is running; repeat it";

    // Node pressure and routing. Each answers `503` with `Retry-After`.
    /// No leader of the stream's group is known.
    NoLeader = "NO_LEADER", "no leader of the stream's group is known; retry";
    /// The WAL filesystem is below its free-space floor.
    WalDiskPressure = "WAL_DISK_PRESSURE", "the node is low on WAL disk space; retry";
    /// The node's unsnapshotted Raft log is over its hard limit.
    RaftLogPressure = "RAFT_LOG_PRESSURE", "the node's Raft log is over its limit; retry";
    /// The node's in-flight request bodies fill its budget.
    IngressBodyBytesLimitReached = "INGRESS_BODY_BYTES_LIMIT_REACHED", "the node's in-flight request budget is full; retry";
    /// The group's hot bytes wait for the cold tier.
    ColdBackpressure = "COLD_BACKPRESSURE", "the stream's group waits for the cold tier; retry";
    /// The group's uncommitted Raft bytes are over their limit.
    RaftUncommittedBackpressure = "RAFT_UNCOMMITTED_BACKPRESSURE", "the stream's group has too many uncommitted bytes; retry";
    /// The node serves as many live reads as it may.
    LiveReadBackpressure = "LIVE_READ_BACKPRESSURE", "the node serves as many live reads as it may; retry";
    /// The group stopped applying after a committed apply failure.
    ApplyStopped = "APPLY_STOPPED", "the stream's group stopped applying";
    /// The write may or may not have applied: resolve it by its key.
    OutcomeUnknown = "OUTCOME_UNKNOWN", "the write's outcome is unknown; repeat it with its key";

    // Server faults.
    /// The cold store failed (`502`).
    ColdStoreFailed = "COLD_STORE_FAILED", "the cold store failed";
    /// A `5xx` that no other code names: a server fault.
    Internal = "INTERNAL", "the server failed";

    // The gateway's own answers.
    /// The bearer credential is not valid for this gateway (`401`).
    InvalidCredential = "INVALID_CREDENTIAL", "the bearer credential is invalid";
    /// The bearer credential expired (`401`).
    CredentialExpired = "CREDENTIAL_EXPIRED", "the bearer credential is expired";
    /// The credential verifier is unavailable (`503`).
    AuthenticationUnavailable = "AUTHENTICATION_UNAVAILABLE", "authentication is unavailable";
    /// The authorizer denied the request (`403`).
    Forbidden = "FORBIDDEN", "the request is not allowed";
    /// The authorizer could not decide (`503`).
    AuthorizationUnavailable = "AUTHORIZATION_UNAVAILABLE", "authorization is unavailable";
    /// The tenant's request rate limit is spent (`429`, with
    /// `Retry-After`).
    RateLimited = "RATE_LIMITED", "the request rate limit is spent";
    /// The tenant holds as many live reads as it may (`429`).
    LiveReadsExhausted = "LIVE_READS_EXHAUSTED", "the live-read limit is spent";
    /// The bucket has as many large request bodies arriving as it may
    /// (`429`, with `Retry-After`).
    UploadsExhausted = "UPLOADS_EXHAUSTED", "the bucket's concurrent upload limit is spent; retry";
    /// The bucket's large request bodies arriving hold as many bytes as
    /// they may (`429`, with `Retry-After`).
    UploadBytesExhausted = "UPLOAD_BYTES_EXHAUSTED", "the bucket's upload bytes limit is spent; retry";
    /// The gateway's budget for buffered request bodies stayed spent
    /// (`503`, with `Retry-After`).
    UploadBudgetExhausted = "UPLOAD_BUDGET_EXHAUSTED", "the gateway's request body budget is spent; retry";
    /// The request body stopped arriving, or arrived below the gateway's
    /// floor, and was cut before anything of it was forwarded (`408`).
    BodyTooSlow = "BODY_TOO_SLOW", "the request body arrived too slowly";
    /// The gateway has no upstream (`503`).
    NoUpstream = "NO_UPSTREAM", "no upstream is configured";
    /// No upstream could be reached (`503`, with `Retry-After`).
    UpstreamUnreachable = "UPSTREAM_UNREACHABLE", "the upstream is unreachable; retry";
    /// The upstream sent no response in time (`504`).
    UpstreamTimeout = "UPSTREAM_TIMEOUT", "the upstream sent no response in time";
    /// The upstream request failed (`502`).
    UpstreamFailed = "UPSTREAM_FAILED", "the upstream request failed";
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The deepest JSON nesting [`ErrorBody::parse`] skips in a member it does
/// not know.
const MAX_SKIPPED_DEPTH: usize = 32;

/// The body of an error answer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ErrorBody {
    /// What refused the request: the member a client matches.
    pub code: ErrorCode,
    /// Why, for a person reading it. Never matched.
    pub message: String,
}

impl ErrorBody {
    /// The `Content-Type` of every error body.
    pub const CONTENT_TYPE: &'static str = "application/json";

    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// `code` with its [default message](ErrorCode::default_message).
    pub fn fixed(code: ErrorCode) -> Self {
        let message = code.default_message();
        Self::new(code, message)
    }

    /// The body: `{"error":{"code":"<code>","message":"<message>"}}`, with
    /// both strings escaped as JSON strings.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(self.message.len().saturating_add(64));
        out.push_str(r#"{"error":{"code":"#);
        push_json_string(&mut out, self.code.as_str());
        out.push_str(r#","message":"#);
        push_json_string(&mut out, &self.message);
        out.push_str("}}");
        out
    }

    /// Parses an error body: a JSON object whose `error` member is an object
    /// with the string member `code` and, optionally, `message` (empty when
    /// absent, as in the subscription errors of the Durable Streams
    /// protocol: `{"error":{"code":"FENCED"}}`). Ursula always sends a
    /// message. Members either object
    /// does not know are skipped, in any order, and a code this version
    /// does not know parses as [`ErrorCode::Unknown`] with its message. JSON
    /// whitespace may surround each token. `Err` for any other body, such as
    /// the plain text of a proxy in front of Ursula, a repeated member, or
    /// an unknown member nested deeper than 32 levels.
    ///
    /// The parse takes time and memory in proportion to the body, so a
    /// caller bounds the body it reads.
    pub fn parse(body: &[u8]) -> Result<Self, HeaderError> {
        let mut cursor = Cursor(body);
        let mut parsed = None;
        cursor.object(|cursor, key| match key {
            "error" => {
                let error = cursor.error_member()?;
                once(&mut parsed, error)
            }
            _ => cursor.skip_value(MAX_SKIPPED_DEPTH),
        })?;
        if !cursor.rest().is_empty() {
            return Err(HeaderError::InvalidErrorBody);
        }
        parsed.ok_or(HeaderError::InvalidErrorBody)
    }
}

/// Sets `slot` to `value`, unless a repeated member set it already.
fn once<T>(slot: &mut Option<T>, value: T) -> Result<(), HeaderError> {
    match slot.replace(value) {
        None => Ok(()),
        Some(_) => Err(HeaderError::InvalidErrorBody),
    }
}

/// Appends `text` to `out` as a JSON string.
fn push_json_string(out: &mut String, text: &str) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The unread rest of an error body.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    /// The rest, without its leading JSON whitespace.
    fn rest(&mut self) -> &'a [u8] {
        let start = self
            .0
            .iter()
            .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
            .unwrap_or(self.0.len());
        self.0 = self.0.get(start..).unwrap_or_default();
        self.0
    }

    /// The next byte after whitespace, without taking it.
    fn peek(&mut self) -> Result<u8, HeaderError> {
        self.rest()
            .first()
            .copied()
            .ok_or(HeaderError::InvalidErrorBody)
    }

    /// The next byte after whitespace.
    fn next(&mut self) -> Result<u8, HeaderError> {
        let (&byte, rest) = self
            .rest()
            .split_first()
            .ok_or(HeaderError::InvalidErrorBody)?;
        self.0 = rest;
        Ok(byte)
    }

    fn expect(&mut self, byte: u8) -> Result<(), HeaderError> {
        if self.next()? == byte {
            Ok(())
        } else {
            Err(HeaderError::InvalidErrorBody)
        }
    }

    /// An object, handing each member's key to `member`, which reads the
    /// member's value.
    fn object(
        &mut self,
        mut member: impl FnMut(&mut Self, &str) -> Result<(), HeaderError>,
    ) -> Result<(), HeaderError> {
        self.expect(b'{')?;
        if self.peek()? == b'}' {
            self.0 = self.0.get(1..).unwrap_or_default();
            return Ok(());
        }
        loop {
            let key = self.string()?;
            self.expect(b':')?;
            member(self, &key)?;
            match self.next()? {
                b',' => continue,
                b'}' => return Ok(()),
                _ => return Err(HeaderError::InvalidErrorBody),
            }
        }
    }

    /// The `error` member's value: an object with `code` and `message`.
    fn error_member(&mut self) -> Result<ErrorBody, HeaderError> {
        let mut code = None;
        let mut message = None;
        self.object(|cursor, key| match key {
            "code" => {
                let value = cursor.string()?;
                once(&mut code, value)
            }
            "message" => {
                let value = cursor.string()?;
                once(&mut message, value)
            }
            _ => cursor.skip_value(MAX_SKIPPED_DEPTH),
        })?;
        let code = code.ok_or(HeaderError::InvalidErrorBody)?;
        Ok(ErrorBody::new(
            ErrorCode::from_name(&code),
            message.unwrap_or_default(),
        ))
    }

    /// Skips one JSON value of at most `depth` levels of nesting.
    fn skip_value(&mut self, depth: usize) -> Result<(), HeaderError> {
        let Some(inner) = depth.checked_sub(1) else {
            return Err(HeaderError::InvalidErrorBody);
        };
        match self.peek()? {
            b'"' => self.string().map(drop),
            b'{' => self.object(|cursor, _key| cursor.skip_value(inner)),
            b'[' => {
                self.expect(b'[')?;
                if self.peek()? == b']' {
                    self.0 = self.0.get(1..).unwrap_or_default();
                    return Ok(());
                }
                loop {
                    self.skip_value(inner)?;
                    match self.next()? {
                        b',' => continue,
                        b']' => return Ok(()),
                        _ => return Err(HeaderError::InvalidErrorBody),
                    }
                }
            }
            _ => self.scalar(),
        }
    }

    /// Skips `true`, `false`, `null` or a number.
    fn scalar(&mut self) -> Result<(), HeaderError> {
        for word in [b"true".as_slice(), b"false", b"null"] {
            if let Some(rest) = self.0.strip_prefix(word) {
                self.0 = rest;
                return Ok(());
            }
        }
        let len = self
            .0
            .iter()
            .position(|byte| !matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
            .unwrap_or(self.0.len());
        let number = self.0.get(..len).unwrap_or_default();
        let valid = std::str::from_utf8(number)
            .ok()
            .is_some_and(|text| !text.is_empty() && text.parse::<f64>().is_ok());
        if !valid {
            return Err(HeaderError::InvalidErrorBody);
        }
        self.0 = self.0.get(len..).unwrap_or_default();
        Ok(())
    }

    /// A JSON string after whitespace, decoded.
    fn string(&mut self) -> Result<String, HeaderError> {
        self.expect(b'"')?;
        let mut out = Vec::new();
        loop {
            let (&byte, rest) = self.0.split_first().ok_or(HeaderError::InvalidErrorBody)?;
            self.0 = rest;
            match byte {
                b'"' => break,
                b'\\' => {
                    let (&escape, rest) =
                        self.0.split_first().ok_or(HeaderError::InvalidErrorBody)?;
                    self.0 = rest;
                    let c = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.escaped_char()?,
                        _ => return Err(HeaderError::InvalidErrorBody),
                    };
                    let mut buf = [0; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                }
                0..=0x1f => return Err(HeaderError::InvalidErrorBody),
                byte => out.push(byte),
            }
        }
        String::from_utf8(out).map_err(|_not_utf8| HeaderError::InvalidErrorBody)
    }

    /// The character of a `\uXXXX` escape, whose `\u` was read, with the
    /// low half of a surrogate pair when it is the high half.
    fn escaped_char(&mut self) -> Result<char, HeaderError> {
        let high = self.hex4()?;
        if !(0xd800..=0xdbff).contains(&high) {
            return char::from_u32(u32::from(high)).ok_or(HeaderError::InvalidErrorBody);
        }
        self.0 = self
            .0
            .strip_prefix(b"\\u")
            .ok_or(HeaderError::InvalidErrorBody)?;
        let low = self.hex4()?;
        char::decode_utf16([high, low])
            .next()
            .and_then(Result::ok)
            .ok_or(HeaderError::InvalidErrorBody)
    }

    fn hex4(&mut self) -> Result<u16, HeaderError> {
        let digits = self.0.get(..4).ok_or(HeaderError::InvalidErrorBody)?;
        let text =
            std::str::from_utf8(digits).map_err(|_not_utf8| HeaderError::InvalidErrorBody)?;
        if !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(HeaderError::InvalidErrorBody);
        }
        let value =
            u16::from_str_radix(text, 16).map_err(|_not_hex| HeaderError::InvalidErrorBody)?;
        self.0 = self.0.get(4..).unwrap_or_default();
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorBody;
    use super::ErrorCode;
    use crate::error::HeaderError;

    /// Every code's wire name, pinned: a rename that changes one fails here.
    #[test]
    fn every_code_keeps_its_wire_name() {
        let golden = [
            (ErrorCode::InvalidBucketId, "INVALID_BUCKET_ID"),
            (ErrorCode::InvalidStreamId, "INVALID_STREAM_ID"),
            (ErrorCode::BucketNotFound, "BUCKET_NOT_FOUND"),
            (ErrorCode::BucketErased, "BUCKET_ERASED"),
            (ErrorCode::StreamNotFound, "STREAM_NOT_FOUND"),
            (ErrorCode::StreamGone, "STREAM_GONE"),
            (
                ErrorCode::StreamAlreadyExistsConflict,
                "STREAM_ALREADY_EXISTS_CONFLICT",
            ),
            (ErrorCode::MissingContentType, "MISSING_CONTENT_TYPE"),
            (ErrorCode::ContentTypeMismatch, "CONTENT_TYPE_MISMATCH"),
            (ErrorCode::EmptyAppend, "EMPTY_APPEND"),
            (ErrorCode::StreamClosed, "STREAM_CLOSED"),
            (ErrorCode::StreamSeqConflict, "STREAM_SEQ_CONFLICT"),
            (ErrorCode::InvalidProducer, "INVALID_PRODUCER"),
            (ErrorCode::ProducerEpochClosed, "PRODUCER_EPOCH_CLOSED"),
            (ErrorCode::ProducerExpired, "PRODUCER_EXPIRED"),
            (ErrorCode::ProducerEpochStale, "PRODUCER_EPOCH_STALE"),
            (ErrorCode::ProducerKeyConflict, "PRODUCER_KEY_CONFLICT"),
            (
                ErrorCode::ProducerDigestMismatch,
                "PRODUCER_DIGEST_MISMATCH",
            ),
            (ErrorCode::ProducerSeqConflict, "PRODUCER_SEQ_CONFLICT"),
            (ErrorCode::InvalidRetention, "INVALID_RETENTION"),
            (ErrorCode::OffsetOutOfRange, "OFFSET_OUT_OF_RANGE"),
            (ErrorCode::RangeNotSatisfiable, "RANGE_NOT_SATISFIABLE"),
            (ErrorCode::InvalidColdFlush, "INVALID_COLD_FLUSH"),
            (ErrorCode::InvalidSnapshot, "INVALID_SNAPSHOT"),
            (ErrorCode::SnapshotNotFound, "SNAPSHOT_NOT_FOUND"),
            (ErrorCode::SnapshotConflict, "SNAPSHOT_CONFLICT"),
            (
                ErrorCode::InvalidRecordBoundaries,
                "INVALID_RECORD_BOUNDARIES",
            ),
            (
                ErrorCode::JsonBoundaryUnverified,
                "JSON_BOUNDARY_UNVERIFIED",
            ),
            (ErrorCode::ImportConflict, "IMPORT_CONFLICT"),
            (ErrorCode::ImportInvalid, "IMPORT_INVALID"),
            (ErrorCode::ProducerLimit, "PRODUCER_LIMIT"),
            (ErrorCode::CreatedAtOutOfRange, "CREATED_AT_OUT_OF_RANGE"),
            (ErrorCode::IncarnationMismatch, "INCARNATION_MISMATCH"),
            (ErrorCode::HistoryPending, "HISTORY_PENDING"),
            (ErrorCode::InvalidRequest, "INVALID_REQUEST"),
            (ErrorCode::InvalidHeader, "INVALID_HEADER"),
            (ErrorCode::InvalidQuery, "INVALID_QUERY"),
            (ErrorCode::InvalidOffset, "INVALID_OFFSET"),
            (ErrorCode::QueryNotAccepted, "QUERY_NOT_ACCEPTED"),
            (ErrorCode::InvalidResource, "INVALID_RESOURCE"),
            (ErrorCode::Unsupported, "UNSUPPORTED"),
            (ErrorCode::BodyUnreadable, "BODY_UNREADABLE"),
            (ErrorCode::NotJsonBoundary, "NOT_JSON_BOUNDARY"),
            (ErrorCode::NotFound, "NOT_FOUND"),
            (ErrorCode::MethodNotAllowed, "METHOD_NOT_ALLOWED"),
            (ErrorCode::RequestTooLarge, "REQUEST_TOO_LARGE"),
            (ErrorCode::SnapshotTooLarge, "SNAPSHOT_TOO_LARGE"),
            (ErrorCode::NoColdStore, "NO_COLD_STORE"),
            (ErrorCode::InvalidPartsCommit, "INVALID_PARTS_COMMIT"),
            (ErrorCode::UnknownPart, "UNKNOWN_PART"),
            (
                ErrorCode::SnapshotDigestMismatch,
                "SNAPSHOT_DIGEST_MISMATCH",
            ),
            (ErrorCode::SnapshotCommitsBusy, "SNAPSHOT_COMMITS_BUSY"),
            (
                ErrorCode::SnapshotCommitInProgress,
                "SNAPSHOT_COMMIT_IN_PROGRESS",
            ),
            (ErrorCode::NoLeader, "NO_LEADER"),
            (ErrorCode::WalDiskPressure, "WAL_DISK_PRESSURE"),
            (ErrorCode::RaftLogPressure, "RAFT_LOG_PRESSURE"),
            (
                ErrorCode::IngressBodyBytesLimitReached,
                "INGRESS_BODY_BYTES_LIMIT_REACHED",
            ),
            (ErrorCode::ColdBackpressure, "COLD_BACKPRESSURE"),
            (
                ErrorCode::RaftUncommittedBackpressure,
                "RAFT_UNCOMMITTED_BACKPRESSURE",
            ),
            (ErrorCode::LiveReadBackpressure, "LIVE_READ_BACKPRESSURE"),
            (ErrorCode::ApplyStopped, "APPLY_STOPPED"),
            (ErrorCode::OutcomeUnknown, "OUTCOME_UNKNOWN"),
            (ErrorCode::ColdStoreFailed, "COLD_STORE_FAILED"),
            (ErrorCode::Internal, "INTERNAL"),
            (ErrorCode::InvalidCredential, "INVALID_CREDENTIAL"),
            (ErrorCode::CredentialExpired, "CREDENTIAL_EXPIRED"),
            (
                ErrorCode::AuthenticationUnavailable,
                "AUTHENTICATION_UNAVAILABLE",
            ),
            (ErrorCode::Forbidden, "FORBIDDEN"),
            (
                ErrorCode::AuthorizationUnavailable,
                "AUTHORIZATION_UNAVAILABLE",
            ),
            (ErrorCode::RateLimited, "RATE_LIMITED"),
            (ErrorCode::LiveReadsExhausted, "LIVE_READS_EXHAUSTED"),
            (ErrorCode::UploadsExhausted, "UPLOADS_EXHAUSTED"),
            (ErrorCode::UploadBytesExhausted, "UPLOAD_BYTES_EXHAUSTED"),
            (ErrorCode::UploadBudgetExhausted, "UPLOAD_BUDGET_EXHAUSTED"),
            (ErrorCode::BodyTooSlow, "BODY_TOO_SLOW"),
            (ErrorCode::NoUpstream, "NO_UPSTREAM"),
            (ErrorCode::UpstreamUnreachable, "UPSTREAM_UNREACHABLE"),
            (ErrorCode::UpstreamTimeout, "UPSTREAM_TIMEOUT"),
            (ErrorCode::UpstreamFailed, "UPSTREAM_FAILED"),
        ];
        assert_eq!(golden.len(), ErrorCode::ALL.len());
        for (code, name) in golden {
            assert_eq!(code.as_str(), name);
            assert_eq!(ErrorCode::from_name(name), code);
        }
    }

    #[test]
    fn every_code_round_trips_with_a_message() {
        for code in ErrorCode::ALL {
            assert!(!code.default_message().is_empty(), "{code}");
            let body = ErrorBody::new(code.clone(), "why");
            assert_eq!(
                body.render(),
                format!(r#"{{"error":{{"code":"{code}","message":"why"}}}}"#)
            );
            assert_eq!(ErrorBody::parse(body.render().as_bytes()), Ok(body));
        }
    }

    #[test]
    fn codes_are_upper_snake_case() {
        for code in ErrorCode::ALL {
            let name = code.as_str();
            assert!(
                name.bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte == b'_'),
                "{name}"
            );
        }
    }

    #[test]
    fn messages_are_escaped_and_round_trip() {
        for message in [
            "",
            "a \"quoted\" \\ path",
            "line\nbreak\ttab\r\u{1}",
            "ünïcødé 🦀",
        ] {
            let body = ErrorBody::new(ErrorCode::InvalidHeader, message);
            let rendered = body.render();
            assert!(!rendered.bytes().any(|byte| byte < 0x20), "{rendered}");
            assert_eq!(ErrorBody::parse(rendered.as_bytes()), Ok(body));
        }
    }

    #[test]
    fn unknown_members_and_codes_are_kept_out_of_the_way() {
        assert_eq!(
            ErrorBody::parse(
                br#"{"v":2,"error":{"current_holder":"w-2","message":"m","generation":7,"code":"ALREADY_CLAIMED","extra":{"a":[1,true,null,{"b":"c"}],"n":-1.5e3}},"trace":[]}"#
            ),
            Ok(ErrorBody::new(
                ErrorCode::Unknown("ALREADY_CLAIMED".to_owned()),
                "m"
            ))
        );
        assert_eq!(
            ErrorBody::parse(
                b" {\n  \"error\" : { \"message\":\"a\\u00e9\\ud83e\\udd80\\/\" , \"code\" : \"NO_LEADER\" }\n}\n"
            ),
            Ok(ErrorBody::new(ErrorCode::NoLeader, "aé🦀/"))
        );
    }

    #[test]
    fn a_body_without_a_message_parses() {
        assert_eq!(
            ErrorBody::parse(br#"{"error":{"code":"FENCED"}}"#),
            Ok(ErrorBody::new(ErrorCode::Unknown("FENCED".to_owned()), ""))
        );
        assert_eq!(
            ErrorBody::parse(br#"{"error":{"code":"NO_LEADER"}}"#),
            Ok(ErrorBody::new(ErrorCode::NoLeader, ""))
        );
    }

    #[test]
    fn other_bodies_are_refused() {
        let deep = format!(
            r#"{{"x":{}1{},"error":{{"code":"NO_LEADER","message":"m"}}}}"#,
            "[".repeat(40),
            "]".repeat(40)
        );
        for body in [
            b"".as_slice(),
            b"request body is too large",
            b"{}",
            br#"{"error":"NO_LEADER"}"#,
            br#"{"error":{"message":"m"}}"#,
            br#"{"error":{"code":"NO_LEADER","message":"m"}"#,
            br#"{"error":{"code":"NO_LEADER","code":"NO_LEADER","message":"m"}}"#,
            br#"{"error":{"code":"NO_LEADER","message":"m"},"error":{"code":"NO_LEADER","message":"m"}}"#,
            br#"{"error":{"code":NO_LEADER,"message":"m"}}"#,
            br#"{"error":{"code":"NO_LEADER","message":1}}"#,
            br#"{"error":{"code":"NO_LEADER","message":"m"}} trailing"#,
            br#"{"error":{"code":"NO_LEADER","message":"\x"}}"#,
            br#"{"error":{"code":"NO_LEADER","message":"\ud800"}}"#,
            br#"{"x":tru,"error":{"code":"NO_LEADER","message":"m"}}"#,
            b"{\"error\":{\"code\":\"NO_LEADER\",\"message\":\"a\nb\"}}",
            deep.as_bytes(),
        ] {
            assert_eq!(
                ErrorBody::parse(body),
                Err(HeaderError::InvalidErrorBody),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }
}
