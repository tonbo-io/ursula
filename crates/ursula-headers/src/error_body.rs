//! The one body of every error answer (extensions Section 11): a JSON
//! object with one member, `error`, whose value is a stable [`ErrorCode`].
//!
//! ```text
//! {"error":"SnapshotCommitsBusy"}
//! ```
//!
//! The body adds the code to what the status and the typed headers say: a
//! `413` names its limit in a header and its code here, a `503` keeps its
//! `Retry-After`. The server and the gateway render every error body with
//! [`ErrorBody::render`], so a client matches the code instead of parsing
//! text. A code is ASCII letters and digits, so the body needs no escaping.

use crate::error::HeaderError;

/// Defines [`ErrorCode`]: each variant's code is its name.
macro_rules! error_codes {
    ($($(#[doc = $doc:literal])+ $code:ident,)+) => {
        /// The stable code of an error answer. Each code is its variant's
        /// name, so `ErrorCode::StreamNotFound` is `"StreamNotFound"`.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $($(#[doc = $doc])+ $code,)+
        }

        impl ErrorCode {
            /// Every code, in declaration order.
            pub const ALL: &[Self] = &[$(Self::$code,)+];

            /// The code as it appears in a body.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$code => stringify!($code),)+
                }
            }
        }
    };
}

error_codes! {
    // Stream state (one code per refusal of the stream state machine).
    /// The bucket id is not a valid bucket id (`400`).
    InvalidBucketId,
    /// The stream id is not a valid stream id (`400`).
    InvalidStreamId,
    /// No such bucket (`404`).
    BucketNotFound,
    /// The bucket was purged and its name is fenced from reuse (`404`).
    BucketErased,
    /// No such stream (`404`).
    StreamNotFound,
    /// The offset is below the stream's retained offset, or the stream is
    /// gone (`410`).
    StreamGone,
    /// A create of a stream that exists with other settings (`409`).
    StreamAlreadyExistsConflict,
    /// A write with a body names no `Content-Type` (`400`).
    MissingContentType,
    /// The write's `Content-Type` is not the stream's (`409`).
    ContentTypeMismatch,
    /// An append with no body that does not close the stream (`400`).
    EmptyAppend,
    /// The stream is closed (`409`, with `Stream-Closed`).
    StreamClosed,
    /// `Stream-Seq` is not above the stream's last one (`409`).
    StreamSeqConflict,
    /// The producer headers are invalid, or a producer write's epoch was
    /// never claimed (`400`).
    InvalidProducer,
    /// A claim closed the producer's epoch (`403 epoch-closed`).
    ProducerEpochClosed,
    /// The stream forgot the producer's epoch (`403 producer-expired`).
    ProducerExpired,
    /// The producer's epoch is below its current one (`403 stale-epoch`).
    ProducerEpochStale,
    /// The write's key holds another write (`403 key-conflict`).
    ProducerKeyConflict,
    /// The body does not match `Producer-Digest` (`400`).
    ProducerDigestMismatch,
    /// `Producer-Seq` is not the next one (`409 seq-gap`).
    ProducerSeqConflict,
    /// The retention offset is invalid (`400`).
    InvalidRetention,
    /// The offset is beyond the stream's tail (`416`).
    OffsetOutOfRange,
    /// A snapshot read's `Range` lies outside the snapshot (`416`, with
    /// `Content-Range: bytes */<size>`).
    RangeNotSatisfiable,
    /// A cold flush the stream cannot take (`400`).
    InvalidColdFlush,
    /// The snapshot is invalid (`400`).
    InvalidSnapshot,
    /// No snapshot at that offset (`404`).
    SnapshotNotFound,
    /// Another snapshot is the latest at or after the offset, or the
    /// offset is beyond the tail (`409`).
    SnapshotConflict,
    /// A JSON body is not valid JSON message text (`400`).
    InvalidRecordBoundaries,
    /// Ursula could not check that the offset of a JSON snapshot or
    /// retention request is a message boundary (`503`).
    JsonBoundaryUnverified,
    /// A state import targeted a group that holds data (`409`).
    ImportConflict,
    /// A state import is invalid (`400`).
    ImportInvalid,
    /// The stream holds as many producers as it may (`429`).
    ProducerLimit,
    /// The server's clock is out of range (`500`).
    CreatedAtOutOfRange,
    /// `Stream-Incarnation` does not match the stream (`412`).
    IncarnationMismatch,
    /// The stream's group has not applied its history yet (`503`).
    HistoryPending,

    // The request.
    /// The request is malformed in a way no other code names (`400`).
    InvalidRequest,
    /// A header value is malformed (`400`).
    InvalidHeader,
    /// A query parameter is malformed or does not fit the request (`400`).
    InvalidQuery,
    /// An offset in the path or query is malformed (`400`).
    InvalidOffset,
    /// The request uses a parameter, header or route that was removed
    /// (`400`, or `405` on the bare snapshot path).
    Unsupported,
    /// The request body could not be read (`400`).
    BodyUnreadable,
    /// The offset of a JSON snapshot or retention request is not a message
    /// boundary (`400`).
    NotJsonBoundary,
    /// No route serves the path (`404`).
    NotFound,
    /// The route does not serve the method (`405`).
    MethodNotAllowed,
    /// The body is above the request limit, `Ursula-Max-Request-Bytes`
    /// (`413`).
    RequestTooLarge,
    /// The assembled snapshot is above the snapshot limit,
    /// `Ursula-Max-Snapshot-Bytes` (`413`).
    SnapshotTooLarge,

    // Snapshot parts.
    /// The node has no cold store, so it takes no snapshot parts (`501`).
    NoColdStore,
    /// A parts commit's headers or manifest are malformed (`400`).
    InvalidPartsCommit,
    /// A manifest names a part that is not staged for the stream (`400`).
    UnknownPart,
    /// The assembled snapshot's digest is not `Stream-Snapshot-Digest`
    /// (`400`).
    SnapshotDigestMismatch,
    /// The node copies as many snapshot commits as it may (`503`).
    SnapshotCommitsBusy,
    /// The node runs this commit now (`503`): repeat it.
    SnapshotCommitInProgress,

    // Node pressure and routing. Each answers `503` with `Retry-After`.
    /// No leader of the stream's group is known.
    NoLeader,
    /// The WAL filesystem is below its free-space floor.
    WalDiskPressure,
    /// The node's unsnapshotted Raft log is over its hard limit.
    RaftLogPressure,
    /// The node's in-flight request bodies fill its budget.
    IngressBodyBytesLimitReached,
    /// The group's hot bytes wait for the cold tier.
    ColdBackpressure,
    /// The group's uncommitted Raft bytes are over their limit.
    RaftUncommittedBackpressure,
    /// The node serves as many live reads as it may.
    LiveReadBackpressure,
    /// The group stopped applying after a committed apply failure.
    ApplyStopped,
    /// The write may or may not have applied: resolve it by its key.
    OutcomeUnknown,

    // Server faults.
    /// The cold store failed (`500` or `502`), or is missing where the
    /// request needs it (`503`).
    ColdStoreFailed,
    /// The server failed (`500`).
    Internal,

    // The gateway's own answers.
    /// The bearer credential is not valid for this gateway (`401`).
    InvalidCredential,
    /// The bearer credential expired (`401`).
    CredentialExpired,
    /// The credential verifier is unavailable (`503`).
    AuthenticationUnavailable,
    /// The authorizer denied the request (`403`).
    Forbidden,
    /// The authorizer could not decide (`503`).
    AuthorizationUnavailable,
    /// The tenant's request rate limit is spent (`429`, with
    /// `Retry-After`).
    RateLimited,
    /// The tenant holds as many live reads as it may (`429`).
    LiveReadsExhausted,
    /// The gateway has no upstream (`503`).
    NoUpstream,
    /// No upstream could be reached (`503`, with `Retry-After`).
    UpstreamUnreachable,
    /// The upstream sent no response in time (`504`).
    UpstreamTimeout,
    /// The upstream request failed (`502`).
    UpstreamFailed,
}

impl ErrorCode {
    /// The code `text` names, if any.
    pub fn from_name(text: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|code| code.as_str() == text)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The body of an error answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorBody {
    pub code: ErrorCode,
}

impl ErrorBody {
    /// The `Content-Type` of every error body.
    pub const CONTENT_TYPE: &'static str = "application/json";

    pub const fn new(code: ErrorCode) -> Self {
        Self { code }
    }

    /// The body: `{"error":"<code>"}`.
    pub fn render(self) -> String {
        format!(r#"{{"error":"{}"}}"#, self.code.as_str())
    }

    /// Parses an error body: one JSON object whose only member is `error`,
    /// a string naming a known code. JSON whitespace may surround each
    /// token. `Err` for any other body, such as the plain text of a proxy
    /// in front of Ursula, or a code this version does not know.
    pub fn parse(body: &[u8]) -> Result<Self, HeaderError> {
        let mut rest = body;
        for expected in [b"{".as_slice(), b"\"error\"", b":", b"\""] {
            rest = skip_whitespace(rest)
                .strip_prefix(expected)
                .ok_or(HeaderError::InvalidErrorBody)?;
        }
        let end = rest
            .iter()
            .position(|byte| *byte == b'"')
            .ok_or(HeaderError::InvalidErrorBody)?;
        let (name, rest) = rest.split_at(end);
        let rest = skip_whitespace(rest.get(1..).unwrap_or_default());
        if skip_whitespace(
            rest.strip_prefix(b"}")
                .ok_or(HeaderError::InvalidErrorBody)?,
        )
        .is_empty()
        {
            let name =
                std::str::from_utf8(name).map_err(|_not_utf8| HeaderError::InvalidErrorBody)?;
            ErrorCode::from_name(name)
                .map(Self::new)
                .ok_or(HeaderError::UnknownErrorCode)
        } else {
            Err(HeaderError::InvalidErrorBody)
        }
    }
}

/// `bytes` without its leading JSON whitespace.
fn skip_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        .unwrap_or(bytes.len());
    bytes.get(start..).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::ErrorBody;
    use super::ErrorCode;
    use crate::error::HeaderError;

    #[test]
    fn every_code_round_trips() {
        for &code in ErrorCode::ALL {
            let body = ErrorBody::new(code).render();
            assert_eq!(body, format!(r#"{{"error":"{code}"}}"#));
            assert_eq!(ErrorBody::parse(body.as_bytes()), Ok(ErrorBody::new(code)));
        }
    }

    #[test]
    fn codes_are_letters_and_digits() {
        for code in ErrorCode::ALL {
            let name = code.as_str();
            assert!(
                name.bytes().all(|byte| byte.is_ascii_alphanumeric()),
                "{name}"
            );
            assert!(name.starts_with(|c: char| c.is_ascii_uppercase()), "{name}");
        }
    }

    #[test]
    fn the_existing_503_bodies_parse() {
        assert_eq!(
            ErrorBody::parse(br#"{"error":"SnapshotCommitsBusy"}"#),
            Ok(ErrorBody::new(ErrorCode::SnapshotCommitsBusy))
        );
        assert_eq!(
            ErrorBody::parse(b" {\n  \"error\" : \"NoLeader\"\n}\n"),
            Ok(ErrorBody::new(ErrorCode::NoLeader))
        );
    }

    #[test]
    fn other_bodies_are_refused() {
        for body in [
            b"".as_slice(),
            b"request body is too large",
            b"{}",
            br#"{"error":"NoLeader""#,
            br#"{"error":"NoLeader","message":"x"}"#,
            br#"{"error":NoLeader}"#,
            br#"{"code":"NoLeader"}"#,
            br#"{"error":"NoLeader"} trailing"#,
        ] {
            assert_eq!(
                ErrorBody::parse(body),
                Err(HeaderError::InvalidErrorBody),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        assert_eq!(
            ErrorBody::parse(br#"{"error":"NoSuchCode"}"#),
            Err(HeaderError::UnknownErrorCode)
        );
    }
}
