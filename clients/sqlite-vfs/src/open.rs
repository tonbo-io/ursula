//! Opening a stream (design §5.5): from the URL to a takeover ready to claim. The driver's core
//! runs it as a step machine: every request and every disk job goes out through the core, and
//! their outcomes come back to [`Opening::answered`] and [`Opening::disk_done`].
//!
//! The first disk job ([`prepare`]) takes the stream's root lock (`in_use` when another process,
//! or another binding or open of the stream in this one, holds it), or shares the root that only
//! reset bindings' connections still hold, finishes what a crash left, and materializes the
//! current generation when there is one (a gap under valid checksums is `internal`). A generation
//! the connections of a reset binding still hold is never materialized under them: the open copies
//! it, publishes the copy in its place and materializes the copy ([`Root::copy`], design §5.6). A
//! generation whose db file is not its own, or whose log's first segment header is unreadable, is
//! left as found and salvaged as `replaced` by the build that replaces it. Then
//! a HEAD without a precondition decides, before any claim, by the table of design §5.5
//! ([`table::decide`]): it creates the stream, builds a generation from the latest snapshot and the
//! tail, takes over with the generation's token, re-keys a generation whose history the stream
//! inherits and takes over, or refuses. A build that replaces a generation holding data salvages
//! it ([`crate::salvage`]) in the same publish: a generation of another token the history does not
//! inherit, or does while its log holds commits after its latest ACK (`history_changed`), one over
//! whose missing stream the open created another (`recreate`, `recreated`), or a replaced one.
//!
//! Every refusal keeps the local files and sends nothing to the stream. A request that gets no
//! answer is sent again under the replicator's retry [`Policy`] until the open's deadline, which
//! then fails with the last condition (`unreachable` when nothing ever went out). A fresh build
//! whose stream changes history while it reads starts over under the new token (design D8),
//! bounded like its restarts after a `410`.
//!
//! When the first HEAD gets no answer with nothing sent (the name lookup or the connect failed,
//! or took longer than its 5 s limit) and the trusted generation holds a CLAIM record, the open
//! resolves at once, provisionally ([`table::decide_unreachable`], [`OpenStep::Provisional`]):
//! the binding takes commits and serves reads from its files, and its replicator defers its claim
//! behind a HEAD and a pre-check of the stream from the log's acknowledged offset (design §7).
//! Without a trusted generation, or without a claim to continue, the open retries until its
//! deadline.
//!
//! A read-only open ([`Access::Reader`]) runs the same steps in a reader directory of its own
//! ([`Root::reader`]) and decides by the reader rows ([`table::decide_reader`], [`reader`]): it
//! builds a reader generation, catches a trusted one of the stream's token up in place, and binds
//! with no claim ([`OpenStep::Reader`]).
//!
//! Module map:
//!
//! - [`table`]: the open table of design §5.5, one pure function, and its reader rows.
//! - [`reader`]: a reader's open after its HEAD: its catch-up in place and its unreachable row.

pub(crate) mod reader;
pub(crate) mod table;

use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamHead;

use self::reader::Applying;
use self::reader::CatchUp;
use self::reader::ReaderReady;
use self::table::Asked;
use self::table::HeadSeen;
use self::table::Local;
use self::table::LocalSummary;
use self::table::OpenRow;
use self::table::Superseded;
use self::table::UnreachableRow;
use crate::applier::stamp_page_one;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::frame::Frame;
use crate::codec::frame::FrameError;
use crate::codec::marker;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentHeader;
use crate::codec::snapshot::SnapshotError;
use crate::codec::snapshot::check_body;
use crate::codec::snapshot::read_image;
use crate::codec::stream::FrameStream;
use crate::codec::stream::StreamError;
use crate::driver::Attach;
use crate::driver::Kept;
use crate::driver::disk::DiskDone;
use crate::driver::disk::DiskJob;
use crate::driver::disk::NewBuild;
use crate::generation::Building;
use crate::generation::CanonicalPath;
use crate::generation::Fate;
use crate::generation::GenerationError;
use crate::generation::PathError;
use crate::generation::Root;
use crate::generation::canonical_path;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::log;
use crate::log::Level;
use crate::materialize;
use crate::materialize::ForeignFrame;
use crate::materialize::LocalError;
use crate::materialize::Materialized;
use crate::materialize::holds_data;
use crate::options::Access;
use crate::replicator::BlockedReason;
use crate::replicator::Condition;
use crate::replicator::LocalState;
use crate::replicator::MAX_BATCH_BYTES;
use crate::replicator::Pending;
use crate::replicator::READ_ANSWER_BYTES;
use crate::replicator::ResetKind;
use crate::replicator::answer::Common;
use crate::replicator::answer::ReadDecision;
use crate::replicator::answer::Unacked;
use crate::replicator::answer::decide_read;
use crate::replicator::answer::decide_status;
use crate::replicator::backoff::Next;
use crate::replicator::backoff::Policy;
use crate::replog::LogError;
use crate::salvage::Acknowledged;
use crate::salvage::Manifest;
use crate::salvage::Reason;
use crate::salvage::State;
use crate::salvage::utc_now;
use crate::transport::Attempt;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::Request;
use crate::wire::SnapshotPart;
use crate::wire::Target;
use crate::wire::Typed;
use crate::wire::snapshot_limit;

/// What `open()` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenSpec {
    /// The stream URL.
    pub(crate) url: String,
    /// The cache root: the stream's files go in `<dir>/<label>-<hash32>/`.
    pub(crate) dir: PathBuf,
    /// The host activation, carried by CLAIM frames.
    pub(crate) activation: Activation,
    /// Compressed frame bytes logged and not acknowledged past which writes are refused.
    pub(crate) max_pending_bytes: u64,
    /// `URSULA_VFS_MAX_REQUEST_BYTES`.
    pub(crate) max_request_bytes: Option<u64>,
    /// How long the open may take, from its start.
    pub(crate) timeout_ms: u64,
    /// An owner's open (with `recreate` or not) or a reader's.
    pub(crate) access: Access,
}

/// The condition an open that ran out of time reports (design §8.4, `UrsulaUnavailableError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unavailable {
    /// Nothing went out: no connection was made.
    Unreachable,
    /// The stream answered, or may have, without a definite answer: retried.
    Retrying,
    RateLimited,
    Unauthorized,
    PaymentRequired,
    Forbidden,
    WrongCluster,
    Redirected {
        location: Option<String>,
    },
    Rejected {
        status: u16,
    },
    RequestTooLarge,
}

impl Unavailable {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Unreachable => "unreachable",
            Self::RateLimited => "rate_limited",
            Self::Unauthorized => "unauthorized",
            Self::PaymentRequired => "payment_required",
            Self::Forbidden => "forbidden",
            Self::WrongCluster => "wrong_cluster",
            Self::Retrying
            | Self::Redirected { .. }
            | Self::Rejected { .. }
            | Self::RequestTooLarge => "unavailable",
        }
    }

    fn blocked(reason: BlockedReason) -> Self {
        match reason {
            BlockedReason::RequestTooLarge => Self::RequestTooLarge,
            BlockedReason::Unauthorized => Self::Unauthorized,
            BlockedReason::PaymentRequired => Self::PaymentRequired,
            BlockedReason::Forbidden => Self::Forbidden,
            BlockedReason::Redirected { location } => Self::Redirected { location },
            BlockedReason::WrongCluster => Self::WrongCluster,
            BlockedReason::Rejected { status } => Self::Rejected { status },
        }
    }
}

impl From<&Condition> for Unavailable {
    fn from(condition: &Condition) -> Self {
        match condition {
            Condition::Retrying => Self::Retrying,
            Condition::Blocked(reason) => Self::blocked(reason.clone()),
        }
    }
}

/// Why the stream is not one the open can take over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamState {
    Closed,
    /// Bytes that are not `USQ2` frames.
    Undecodable,
    /// A read answered a range other than the one it names.
    Contradiction,
    /// Retention kept passing what a build installed, over every restart it makes: the frames
    /// from `from` are below retention.
    BelowRetention {
        from: Offset,
    },
    /// A fresh build started over after a history change more times than it restarts.
    HistoryChanged,
    /// A commit or snapshot whose page 1 is not a SQLite database's.
    NotSqlite,
}

/// Why the stream's content is not this format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamFormat {
    ContentType {
        found: Option<String>,
    },
    /// `USQ1` frames: a 0.7 stream.
    Usq1,
    /// A stream at the path exists with another configuration.
    Configuration,
}

/// Local files the open cannot use.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalSetup {
    #[error(transparent)]
    Generation(#[from] GenerationError),
    #[error(transparent)]
    Local(#[from] LocalError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("the generation is of stream {found:?}, not {expected:?}")]
    OtherStream { found: String, expected: String },
}

/// Why an open failed (design §8.4: `UrsulaSetupError` and `UrsulaUnavailableError`).
#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenError {
    #[error("invalid_url: {0}")]
    InvalidUrl(#[from] PathError),
    #[error("in_use: {}: another process, or another binding or open of the stream in this one, holds the stream's local files", .root.display())]
    InUse { root: PathBuf },
    /// The stream is bound in this process under the cache root `bound`, and the host still holds
    /// that binding: an open under `requested` would replicate the stream from two caches.
    #[error("{}: the stream is open in this process under {}, not {}: {kept}", .kept.error_kind().as_str(), .bound.display(), .requested.display())]
    BoundElsewhere {
        bound: PathBuf,
        requested: PathBuf,
        kept: Kept,
    },
    #[error("local_setup: {0}")]
    LocalSetup(#[source] LocalSetup),
    #[error("internal: {0}")]
    Internal(#[source] LocalSetup),
    #[error("internal: the open panicked")]
    Panicked,
    #[error("stream_format: {0:?}")]
    StreamFormat(StreamFormat),
    #[error("server_too_old: protocol {protocol:?}, this extension needs 2")]
    ServerTooOld { protocol: Option<u32> },
    #[error(
        "wrong_cluster: the generation is of cluster {local:?}, the stream answers as {stream}"
    )]
    WrongCluster { local: String, stream: ClusterId },
    #[error("stream_state: {0:?}")]
    StreamState(StreamState),
    /// The snapshot the stream names is not one this extension can install.
    #[error("stream_state: the stream's latest snapshot: {0}")]
    Snapshot(#[source] SnapshotError),
    /// The stream is missing, and the open does not create it: an owner's over a local generation
    /// holding data without `recreate`, or a reader's.
    #[error("not_found: the stream is missing")]
    NotFound,
    #[error("claim_races: every claim lost to another claimant")]
    ClaimRaces,
    #[error("{}: the open ran out of time", .0.kind())]
    Unavailable(Unavailable),
    /// The open gave up before its deadline: the stream kept contradicting it (`unavailable`).
    #[error("unavailable: {0}")]
    GaveUp(GaveUp),
    #[error("{}: the takeover reset", .0.kind())]
    Reset(ResetKind),
    #[error("cancelled")]
    Cancelled,
}

/// Why an open gave up before its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum GaveUp {
    /// A build's reads met a `412` or `404` and its HEADs said otherwise, more than
    /// [`MAX_BUILD_RECHECKS`] times.
    #[error("the build gave up after {MAX_BUILD_RECHECKS} HEADs that disagreed with its reads")]
    Rechecks,
    /// The stream went missing under a build more than [`MAX_BUILD_RESTARTS`] times.
    #[error("the stream went missing under the build {MAX_BUILD_RESTARTS} times")]
    Missing,
}

/// A local failure: a gap or malformed record under valid checksums is the extension's own bug
/// (`internal`), a commit or snapshot from the stream whose page 1 is not a SQLite database's is
/// the stream's (`stream_state`), anything else `local_setup`.
pub(crate) fn local(e: impl Into<LocalSetup>) -> OpenError {
    let e = e.into();
    if let LocalSetup::Local(l) = &e
        && l.is_stream_content()
    {
        return OpenError::StreamState(StreamState::NotSqlite);
    }
    let bug = |log: &LogError| matches!(log, LogError::Gap { .. } | LogError::Malformed { .. });
    let internal = match &e {
        LocalSetup::Local(LocalError::Log(log))
        | LocalSetup::Log(log)
        | LocalSetup::Generation(GenerationError::Log(log)) => bug(log),
        LocalSetup::Local(_) | LocalSetup::Generation(_) | LocalSetup::OtherStream { .. } => false,
    };
    if internal {
        OpenError::Internal(e)
    } else {
        OpenError::LocalSetup(e)
    }
}

/// What a failed request of the open leads to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Failure {
    /// Send it again at a time in `at_ms ..= at_ms + jitter_ms`.
    Again {
        at_ms: u64,
        jitter_ms: u64,
        attempt: Attempt,
    },
    /// A `412` or `404`: HEAD again.
    Restart,
    /// The stream is closed.
    Closed,
}

/// The retries of the open's requests: the replicator's [`Policy`], and the last condition, kept
/// for the deadline's error.
#[derive(Debug, Clone, Default)]
pub(crate) struct Retry {
    policy: Policy,
    condition: Option<Unavailable>,
}

impl Retry {
    /// The condition the deadline reports.
    pub(crate) fn condition(&self) -> Unavailable {
        self.condition.clone().unwrap_or(Unavailable::Unreachable)
    }

    /// The last condition met, if any.
    pub(crate) fn seen(&self) -> Option<Unavailable> {
        self.condition.clone()
    }

    fn succeeded(&mut self) {
        self.policy = Policy::default();
    }

    /// What to do after `exchange`, which decided nothing about the request itself.
    pub(crate) fn failed(&mut self, exchange: &Exchange, now_ms: u64) -> Failure {
        let no_answer = Common::Resend {
            retry_after: None,
            fresh: true,
        };
        let (common, answer) = match exchange {
            Exchange::NoAnswer(_) => (no_answer, None),
            Exchange::Answered(answer) => (decide_status(answer), Some(answer)),
        };
        let retry_after = answer.and_then(|a| a.retry_after);
        match self.policy.decide(common, retry_after, now_ms) {
            Next::Wait {
                at_ms,
                jitter_ms,
                attempt,
                condition,
            } => {
                let condition = match (condition, exchange) {
                    (Condition::Blocked(reason), _) => Unavailable::blocked(reason),
                    // Unreachable only while nothing ever answered.
                    (Condition::Retrying, Exchange::NoAnswer(NoAnswer::NotSent { .. }))
                        if self
                            .condition
                            .as_ref()
                            .is_none_or(|c| *c == Unavailable::Unreachable) =>
                    {
                        Unavailable::Unreachable
                    }
                    (Condition::Retrying, _) if answer.is_some_and(|a| a.status == 429) => {
                        Unavailable::RateLimited
                    }
                    (Condition::Retrying, _) => Unavailable::Retrying,
                };
                self.condition = Some(condition);
                Failure::Again {
                    at_ms,
                    jitter_ms,
                    attempt,
                }
            }
            Next::Resend(attempt) => {
                self.condition = Some(Unavailable::Unauthorized);
                Failure::Again {
                    at_ms: now_ms,
                    jitter_ms: 0,
                    attempt,
                }
            }
            Next::Head(_) => Failure::Restart,
            Next::Reset(_) => Failure::Closed,
        }
    }
}

/// Reads the stream from `from` (its start, or an installed snapshot's offset) up to `end`, a
/// frame boundary, through the reconcile's [`FrameStream`].
#[derive(Debug, Clone)]
pub(crate) struct Fetch {
    incarnation: Incarnation,
    stream: FrameStream,
    highest_epoch: Option<Epoch>,
}

impl Fetch {
    pub(crate) fn new(
        incarnation: Incarnation,
        from: Offset,
        end: Offset,
        highest_epoch: Option<Epoch>,
    ) -> Self {
        Self {
            incarnation,
            stream: FrameStream::new(from, end, MAX_BATCH_BYTES),
            highest_epoch,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.stream.done()
    }

    /// Reads up to `end` instead, before anything was read.
    fn extend_to(&mut self, end: Offset) {
        self.stream = FrameStream::new(self.stream.at(), end, MAX_BATCH_BYTES);
    }

    /// The next read, under the precondition.
    pub(crate) fn request(&self) -> Request {
        let at = self.stream.at();
        let from = (at != Offset::ZERO).then_some(at);
        let mut request = Request::read(from, false, READ_ANSWER_BYTES);
        request.keys.precondition = Some(IncarnationPrecondition {
            incarnation: self.incarnation,
        });
        request
    }

    /// A read's bytes up to `next`.
    pub(crate) fn feed(
        &mut self,
        body: &[u8],
        next: Offset,
    ) -> Result<Vec<ForeignFrame>, OpenError> {
        let (frames, error) = self.stream.feed(body, next);
        match error {
            None => {}
            Some(StreamError::Range { .. }) => {
                return Err(OpenError::StreamState(StreamState::Contradiction));
            }
            Some(StreamError::Undecodable(FrameError::Magic { found }))
                if found.as_slice() == b"USQ1" =>
            {
                return Err(OpenError::StreamFormat(StreamFormat::Usq1));
            }
            Some(StreamError::Undecodable(_) | StreamError::PastEnd { .. }) => {
                return Err(OpenError::StreamState(StreamState::Undecodable));
            }
        }
        Ok(frames
            .into_iter()
            .map(|read| {
                if let Frame::Claim { epoch, .. } = &read.frame {
                    self.highest_epoch = Some(self.highest_epoch.map_or(*epoch, |e| e.max(*epoch)));
                }
                ForeignFrame {
                    frame: read.frame,
                    end_offset: read.end,
                }
            })
            .collect())
    }
}

/// A generation built from the stream in `gen-<id32>.tmp` (design §5.6): the open's build, or
/// a takeover's rebuild. It is published by one rename only once complete ([`Build::publish`]), so
/// the generation it supersedes stays whole until then, and a failed build leaves it in place. A
/// crash midway leaves only the `.tmp`, which the next open deletes (`finish_leftovers`).
///
/// A HEAD names the latest snapshot. Its body is read, checked (the digest the stream names, the
/// `USS3` header's checksum and offset) and installed into the db file, whose image's checksum
/// [`read_image`] checks; an ACK record then names the snapshot's offset as acknowledged at the
/// position before the log's first (the base the header names). The tail is read from there,
/// or from the stream's start without a snapshot. Each read's frames are logged as COMMIT records
/// with an ACK and fsynced before they are applied ([`materialize::catch_up_files`]), so memory
/// stays bounded by one read. A `410` while the tail is read (retention passed the installed
/// snapshot) deletes the `.tmp` and starts over from the HEAD ([`Build::restart`]), and so does a
/// history change while a fresh build reads (design D8), under the new token.
#[derive(Debug)]
pub(crate) struct Build {
    building: Building,
    /// Holds the root's lock while the build lives, in a disk job or not.
    root: Arc<Root>,
    /// The log's first segment header, for a restart.
    header: SegmentHeader,
    /// The position before the log's first: the snapshot's place in the new log.
    base: Position,
    stage: BuildStage,
    fetch: Fetch,
    retry: Retry,
    /// The next attempt's flags, after a failure.
    attempt: Attempt,
    /// Restarts after a `410` or a history change so far, at most [`MAX_BUILD_RESTARTS`].
    restarts: u32,
    /// HEADs a `412` or `404` on a read sent so far, at most [`MAX_BUILD_RECHECKS`].
    rechecks: u32,
    /// Their backoff: it grows across the HEADs that answer between them, until a read succeeds.
    recheck: Policy,
    /// The offset of the snapshot this build installed, if any.
    installed: Option<Offset>,
    /// What becomes of the generation the publish supersedes.
    fate: Fate,
    /// The latest HEAD the build read: a fresh build that started over after a history change
    /// takes over with its token.
    head: Option<StreamHead>,
}

/// A build published: the generation materialized, the highest epoch a CLAIM read named, where the
/// open got the database, and the latest HEAD the build read.
#[derive(Debug)]
pub(crate) struct Published {
    pub(crate) m: Box<Materialized>,
    pub(crate) highest: Option<Epoch>,
    pub(crate) attach: Attach,
    pub(crate) head: Option<StreamHead>,
}

/// The restarts a build makes after a `410` or a history change before it gives up.
const MAX_BUILD_RESTARTS: u32 = 8;

/// The HEADs a build sends after a `412` or `404` on a read before it gives up (`unavailable`):
/// each after the open's backoff, two per restart at most.
const MAX_BUILD_RECHECKS: u32 = 2 * MAX_BUILD_RESTARTS;

/// What a build reads next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildStage {
    /// The HEAD that names the latest snapshot.
    Head,
    /// The latest snapshot's body, at most `limit` bytes.
    Snapshot { snapshot: SnapshotRef, limit: u64 },
    /// The frames after the snapshot (or from the start), up to the end.
    Tail,
    /// A `410` met the tail: after the backoff, start over ([`Build::restart`]).
    Restarting,
}

/// What a build does next.
#[derive(Debug)]
pub(crate) enum BuildStep {
    Send {
        request: Request,
        attempt: Attempt,
    },
    Timer {
        at_ms: u64,
        jitter_ms: u64,
    },
    /// Install this snapshot body (a disk job: [`Build::install`]).
    Install(Vec<u8>),
    /// Apply these frames (a disk job: [`Build::apply`]).
    Apply(Vec<ForeignFrame>),
    /// Retention passed what the build installed: start over (a disk job: [`Build::restart`]).
    Restart,
    /// Everything is read and applied: publish (a disk job: [`Build::publish`]).
    Done,
    /// A fresh build found the stream missing: it is abandoned and the open goes back through
    /// the open table (a disk job: [`Build::abandon`], then [`prepare_locked`] under the lock it
    /// held).
    Missing,
    Fail(OpenError),
}

impl Build {
    pub(crate) fn new(
        building: Building,
        root: Arc<Root>,
        header: SegmentHeader,
        end: Offset,
        fate: Fate,
    ) -> Self {
        Self {
            building,
            root,
            base: Position(header.first_position.0.saturating_sub(1)),
            fetch: Fetch::new(header.incarnation, Offset::ZERO, end, None),
            header,
            stage: BuildStage::Head,
            retry: Retry::default(),
            attempt: Attempt::default(),
            restarts: 0,
            rechecks: 0,
            recheck: Policy::default(),
            installed: None,
            fate,
            head: None,
        }
    }

    /// The next request, or done.
    pub(crate) fn next(&self) -> BuildStep {
        let request = match self.stage {
            BuildStage::Head => head_request(),
            BuildStage::Snapshot { snapshot, limit } => Request::snapshot_read(
                snapshot.offset,
                self.fetch.incarnation,
                SnapshotPart::Body { limit },
            ),
            BuildStage::Tail if self.fetch.done() => return BuildStep::Done,
            BuildStage::Tail => self.fetch.request(),
            BuildStage::Restarting => return BuildStep::Restart,
        };
        BuildStep::Send {
            request,
            attempt: self.attempt,
        }
    }

    /// The condition the deadline reports.
    pub(crate) fn condition(&self) -> Unavailable {
        self.retry.condition()
    }

    /// The last condition met, if any.
    pub(crate) fn seen(&self) -> Option<Unavailable> {
        self.retry.seen()
    }

    /// The answer to the last request.
    pub(crate) fn answered(&mut self, exchange: &Exchange, now_ms: u64) -> BuildStep {
        let answer = match exchange {
            Exchange::Answered(answer) => Some(answer),
            Exchange::NoAnswer(_) => None,
        };
        match (self.stage, answer) {
            (BuildStage::Head, Some(answer)) => {
                // A HEAD of another deployment (a route that flipped) or of an older server decides
                // nothing about this build's stream: refused, as the open table refuses it.
                if let Ok(Typed::Head { identity, .. }) = &answer.typed
                    && matches!(answer.status, 200 | 404)
                    && let Err(refused) =
                        table::check_identity(identity.as_ref(), Some(&self.header.cluster_id))
                {
                    return BuildStep::Fail(refused.into());
                }
                let head = match &answer.typed {
                    Ok(Typed::Head {
                        stream: Some(head), ..
                    }) if answer.status == 200 => *head,
                    // The stream is gone: a fresh build goes back through the open table, and a
                    // rebuild's claim went with it.
                    Ok(Typed::Head { stream: None, .. }) if answer.status == 404 => {
                        self.succeeded();
                        return match self.header.claim {
                            None => BuildStep::Missing,
                            Some(_) => BuildStep::Fail(OpenError::Reset(ResetKind::Deleted)),
                        };
                    }
                    _ => return self.failed(exchange, now_ms),
                };
                self.succeeded();
                if head.incarnation() != self.fetch.incarnation {
                    return self.history_changed(head.incarnation());
                }
                self.head = Some(head);
                // A fresh open's build reads up to the tail this HEAD names. A takeover's rebuild
                // reads up to its claim: a snapshot past the claim means another owner wrote
                // after it.
                match self.header.claim {
                    None => self.fetch.extend_to(head.next_offset()),
                    Some(_) => {
                        if head
                            .snapshot()
                            .is_some_and(|s| s.offset > self.fetch.stream.end())
                        {
                            return BuildStep::Fail(OpenError::Reset(ResetKind::Fenced));
                        }
                    }
                }
                self.stage = match head.snapshot() {
                    Some(snapshot) if snapshot.offset > Offset::ZERO => BuildStage::Snapshot {
                        snapshot,
                        limit: snapshot_limit(&head),
                    },
                    _ => BuildStage::Tail,
                };
                self.next()
            }
            (BuildStage::Snapshot { snapshot, .. }, Some(answer)) => match &answer.typed {
                Ok(Typed::SnapshotRead(read))
                    if answer.status == 200 && read.snapshot == snapshot =>
                {
                    self.succeeded();
                    BuildStep::Install(answer.body.clone())
                }
                // Superseded since the HEAD: the HEAD again names the latest.
                Ok(Typed::Untyped) if answer.status == 404 => {
                    self.succeeded();
                    self.stage = BuildStage::Head;
                    self.next()
                }
                _ => self.failed(exchange, now_ms),
            },
            (BuildStage::Head | BuildStage::Snapshot { .. }, None) => self.failed(exchange, now_ms),
            (BuildStage::Tail, _) => self.tail_answered(exchange, now_ms),
            // No request is out.
            (BuildStage::Restarting, _) => BuildStep::Fail(OpenError::Panicked),
        }
    }

    fn succeeded(&mut self) {
        self.retry.succeeded();
        self.attempt = Attempt::default();
    }

    /// An answer that decided nothing about the request.
    fn failed(&mut self, exchange: &Exchange, now_ms: u64) -> BuildStep {
        match self.retry.failed(exchange, now_ms) {
            Failure::Again {
                at_ms,
                jitter_ms,
                attempt,
            } => {
                self.attempt = attempt;
                BuildStep::Timer { at_ms, jitter_ms }
            }
            // A `412` or a `404`: after the open's backoff, the HEAD tells whether the history
            // changed or the stream went. A changed history counts as a restart, and these checks
            // count apart, so reads and HEADs that disagree (two nodes) do not loop.
            Failure::Restart => {
                self.rechecks = self.rechecks.saturating_add(1);
                if self.rechecks > MAX_BUILD_RECHECKS {
                    return BuildStep::Fail(OpenError::GaveUp(GaveUp::Rechecks));
                }
                self.stage = BuildStage::Head;
                match self.recheck.backoff(None, Attempt::default(), now_ms) {
                    Next::Wait {
                        at_ms, jitter_ms, ..
                    } => BuildStep::Timer { at_ms, jitter_ms },
                    Next::Resend(_) | Next::Head(_) | Next::Reset(_) => self.next(),
                }
            }
            Failure::Closed => BuildStep::Fail(OpenError::StreamState(StreamState::Closed)),
        }
    }

    /// The stream changed history while the build read it. A fresh build starts over in a fresh
    /// `.tmp`, under the token the HEAD named (design D8), at most [`MAX_BUILD_RESTARTS`] times. A
    /// takeover's rebuild reads up to its claim, which the new history may not hold: the takeover
    /// resets `rewound`, and the reopen goes through the open table.
    fn history_changed(&mut self, incarnation: Incarnation) -> BuildStep {
        if self.header.claim.is_some() {
            return BuildStep::Fail(OpenError::Reset(ResetKind::Rewound));
        }
        self.restarts = self.restarts.saturating_add(1);
        if self.restarts > MAX_BUILD_RESTARTS {
            return BuildStep::Fail(OpenError::StreamState(StreamState::HistoryChanged));
        }
        self.header.incarnation = incarnation;
        self.fetch.incarnation = incarnation;
        self.stage = BuildStage::Restarting;
        BuildStep::Restart
    }

    /// Disk job: checks the snapshot body and installs its image into the db file, then names
    /// its offset acknowledged in an ACK record, so the tail is read from there.
    pub(crate) fn install(&mut self, body: &[u8]) -> Result<(), OpenError> {
        let BuildStage::Snapshot { snapshot, .. } = self.stage else {
            return Err(OpenError::Panicked);
        };
        let (header, at) = check_body(CONTENT_TYPE, body, snapshot.offset, &snapshot.digest)
            .map_err(OpenError::Snapshot)?;
        let mut compressed = body.get(at..).unwrap_or_default();
        let db_path = self.building.db_path();
        let mut out = BufWriter::new(&self.building.db);
        read_image(&header, &mut compressed, &mut out).map_err(OpenError::Snapshot)?;
        out.flush().map_err(|source| {
            local(LocalError::Io {
                op: "write",
                path: db_path.clone(),
                source,
            })
        })?;
        drop(out);
        stamp_page_one(&self.building.db, &db_path, self.header.lineage)
            .map_err(|e| local(LocalError::from(e)))?;
        self.building
            .log
            .append(&RecordBody::Ack {
                acked: self.base,
                acked_offset: snapshot.offset,
                log_bytes: 0,
            })
            .map_err(local)?;
        let highest = Some(header.fields.epoch);
        self.fetch = Fetch::new(
            self.fetch.incarnation,
            snapshot.offset,
            self.fetch.stream.end(),
            highest,
        );
        self.stage = BuildStage::Tail;
        self.installed = Some(snapshot.offset);
        Ok(())
    }

    /// The answer to a read of the tail.
    fn tail_answered(&mut self, exchange: &Exchange, now_ms: u64) -> BuildStep {
        match decide_read(exchange) {
            ReadDecision::Bytes { next } => {
                self.retry.succeeded();
                self.recheck = Policy::default();
                self.attempt = Attempt::default();
                let body = match exchange {
                    Exchange::Answered(answer) => answer.body.as_slice(),
                    Exchange::NoAnswer(_) => &[],
                };
                match self.fetch.feed(body, next) {
                    Ok(frames) if frames.is_empty() => self.next(),
                    Ok(frames) => BuildStep::Apply(frames),
                    Err(e) => BuildStep::Fail(e),
                }
            }
            // Retention passed what this build installed: it starts over in a fresh `.tmp`, from
            // the snapshot the HEAD names then, after the open's backoff, a few times at most.
            ReadDecision::Gone => {
                self.restarts = self.restarts.saturating_add(1);
                if self.restarts > MAX_BUILD_RESTARTS {
                    return BuildStep::Fail(OpenError::StreamState(StreamState::BelowRetention {
                        from: self.fetch.stream.at(),
                    }));
                }
                self.stage = BuildStage::Restarting;
                match self.retry.policy.backoff(None, Attempt::default(), now_ms) {
                    Next::Wait {
                        at_ms, jitter_ms, ..
                    } => BuildStep::Timer { at_ms, jitter_ms },
                    Next::Resend(_) | Next::Head(_) | Next::Reset(_) => BuildStep::Restart,
                }
            }
            ReadDecision::Common(_) => self.failed(exchange, now_ms),
        }
    }

    /// Disk job: deletes what the build wrote and starts it again in a fresh `.tmp`, from its
    /// HEAD.
    pub(crate) fn restart(self) -> Result<Self, OpenError> {
        let Self {
            building,
            root,
            header,
            fetch,
            retry,
            restarts,
            rechecks,
            fate,
            ..
        } = self;
        building.discard().map_err(local)?;
        let mut build = start_build(root, header, fetch.stream.end(), fate)?;
        build.retry = retry;
        build.restarts = restarts;
        build.rechecks = rechecks;
        Ok(build)
    }

    /// Disk job: deletes what the build wrote, its `.tmp`, and hands back the root, still locked.
    pub(crate) fn abandon(self) -> Result<Arc<Root>, OpenError> {
        let Self { building, root, .. } = self;
        building.discard().map_err(local)?;
        Ok(root)
    }

    /// Disk job: logs, acknowledges, fsyncs and applies one read's frames.
    pub(crate) fn apply(&mut self, frames: &[ForeignFrame]) -> Result<(), OpenError> {
        let db_path = self.building.db_path();
        materialize::catch_up_files(&mut self.building.log, &self.building.db, &db_path, frames)
            .map_err(local)
    }

    /// Disk job: publishes the complete generation by one rename (the one it supersedes is
    /// trashed or salvaged then) and materializes it. Its log is trimmed only then, once the
    /// generation it superseded is gone ([`materialize::trim_published`]).
    pub(crate) fn publish(self) -> Result<Published, OpenError> {
        let highest = self.fetch.highest_epoch;
        let salvaged = match &self.fate {
            Fate::Trash => None,
            Fate::Salvage(manifest) => Some(manifest.reason),
        };
        let attach = Attach::Built {
            snapshot: self.installed,
            salvaged,
        };
        let generation = self.building.publish(self.fate).map_err(local)?;
        let mut m = materialize::materialize(&generation).map_err(local)?;
        materialize::trim_published(&mut m).map_err(local)?;
        drop(self.root);
        Ok(Published {
            m: Box::new(m),
            highest,
            attach,
            head: self.head,
        })
    }
}

/// Disk job: starts building a generation of a fresh lineage in `gen-<id32>.tmp`, whose log
/// starts at `first`. A rebuild's claim is logged as a CLAIM record, fsynced, so the generation
/// knows its claim frame as its own.
pub(crate) fn new_build(job: NewBuild) -> Result<Build, OpenError> {
    let NewBuild {
        root,
        name,
        lineage,
        cluster_id,
        incarnation,
        first,
        claim,
        end,
        fate,
    } = job;
    let before = Position(first.0.saturating_sub(1));
    let header = SegmentHeader {
        lineage,
        first_position: first,
        supersedes: None,
        cluster_id: cluster_id.as_str().to_owned(),
        stream_path: name.as_str().to_owned(),
        incarnation,
        base: before,
        acked: before,
        acked_offset: Offset::ZERO,
        log_bytes: 0,
        claim,
    };
    start_build(root, header, end, fate)
}

/// Starts building the generation `header` names, its claim (a rebuild's) logged and fsynced.
fn start_build(
    root: Arc<Root>,
    header: SegmentHeader,
    end: Offset,
    fate: Fate,
) -> Result<Build, OpenError> {
    let mut building = root.build(header.clone()).map_err(local)?;
    if let Some((epoch, activation)) = header.claim {
        building
            .log
            .append(&RecordBody::Claim { epoch, activation })
            .and_then(|_position| building.log.sync())
            .map_err(local)?;
    }
    Ok(Build::new(building, root, header, end, fate))
}

/// A generation whose db file is not its own, or whose log's first segment header is unreadable:
/// left as found, and salvaged as `replaced` by the build that replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Replaced {
    lineage: Option<Lineage>,
    cluster_id: Option<String>,
    /// Its db file holds SQLite's own magic.
    plain: bool,
}

/// The local generation the open found.
type Found = Local<Box<Materialized>, Replaced>;

/// What the local steps of an open found.
#[derive(Debug)]
pub(crate) struct Prepared {
    root: Arc<Root>,
    found: Found,
}

impl Prepared {
    /// The stream's root, locked.
    pub(crate) fn root(&self) -> &Arc<Root> {
        &self.root
    }
}

/// Whether the db file in `dir` starts with SQLite's magic.
fn plain_db(dir: &Path) -> bool {
    let mut head = [0u8; marker::LEN];
    File::open(dir.join("db"))
        .and_then(|f| f.read_exact_at(&mut head, 0))
        .is_ok_and(|()| head == marker::SQLITE_MAGIC)
}

/// Disk job: locks the root (unless `root`, the one a reset binding of this process still holds,
/// is given), or a reader directory for a reader's open, finishes leftovers and materializes the
/// current generation, unless it is replaced (its db file not its own, its log's first segment
/// header unreadable), which is left as found.
pub(crate) fn prepare(spec: &OpenSpec, root: Option<Arc<Root>>) -> Result<Prepared, OpenError> {
    let root = match root {
        Some(root) => root,
        None => {
            let name = canonical_path(&spec.url)?;
            let locked = match spec.access {
                Access::Owner { .. } => Root::lock(&spec.dir, &name),
                Access::Reader => Root::reader(&spec.dir, &name),
            };
            let root = locked.map_err(|e| match e {
                GenerationError::InUse { root } => OpenError::InUse { root },
                other => local(other),
            })?;
            Arc::new(root)
        }
    };
    prepare_locked(root, spec)
}

/// [`prepare`] in a root this open holds locked already. A current generation that a reset
/// binding's connections still hold is copied first, and the copy published in its place.
pub(crate) fn prepare_locked(root: Arc<Root>, spec: &OpenSpec) -> Result<Prepared, OpenError> {
    let name = canonical_path(&spec.url)?;
    root.finish_leftovers().map_err(local)?;
    let current = match root.current_or_unreadable().map_err(local)? {
        Some(Ok(generation)) if root.held(&generation) => {
            let copy = root
                .copy(&generation)
                .and_then(|building| building.publish(Fate::Trash))
                .map_err(local)?;
            log::emit(Level::Info, "generation_copied", &[
                ("from", &generation.dir.display()),
                ("to", &copy.dir.display()),
            ]);
            Some(Ok(copy))
        }
        current => current,
    };
    let found = match current {
        None => Local::None,
        Some(Err(unreadable)) => {
            log::emit(Level::Warn, "generation_replaced", &[
                ("dir", &unreadable.dir.display()),
                ("error", &unreadable.error),
            ]);
            Local::Replaced(Replaced {
                lineage: None,
                cluster_id: None,
                plain: plain_db(&unreadable.dir),
            })
        }
        Some(Ok(generation)) => match materialize::materialize(&generation) {
            Ok(m) => {
                let found = &m.recovered.header.stream_path;
                if found != name.as_str() {
                    return Err(OpenError::LocalSetup(LocalSetup::OtherStream {
                        found: found.clone(),
                        expected: name.as_str().to_owned(),
                    }));
                }
                Local::Trusted(summary(&m), Box::new(m))
            }
            // Refused before anything was written: the generation stays as it was found.
            Err(e) if e.is_replaced() => {
                log::emit(Level::Warn, "generation_replaced", &[
                    ("dir", &generation.dir.display()),
                    ("error", &e),
                ]);
                Local::Replaced(Replaced {
                    lineage: Some(generation.lineage),
                    cluster_id: generation.first_header().ok().map(|h| h.cluster_id),
                    plain: matches!(e, LocalError::Unmarked { .. }),
                })
            }
            Err(e) => return Err(local(e)),
        },
    };
    Ok(Prepared { root, found })
}

/// What the open table reads of a trusted local generation.
fn summary(m: &Materialized) -> LocalSummary {
    let r = &m.recovered;
    LocalSummary {
        cluster_id: r.header.cluster_id.clone(),
        incarnation: r.header.incarnation,
        acked_offset: r.acked_offset,
        pending: unacked(m),
        holds_data: holds_data(m),
        claim: m.recovered.claim,
    }
}

/// Every claim the generation logged: its CLAIM records, and its segment header's.
fn claims(m: &Materialized) -> Vec<(Epoch, Activation)> {
    let r = &m.recovered;
    let mut claims = r.claims.clone();
    if let Some(claim) = r.claim
        && !claims.contains(&claim)
    {
        claims.push(claim);
    }
    claims
}

/// Whether the log holds any COMMIT record after its latest ACK, own or caught up. The log's open
/// cuts caught-up ones no ACK follows, so this is the generation's pending commits, checked whole.
fn unacked(m: &Materialized) -> Unacked {
    if m.recovered.commit_after_ack() {
        Unacked::Commits
    } else {
        Unacked::Nothing
    }
}

/// The manifest of a salvage of the trusted generation `m` for `reason`, decided now against
/// a stream of token `stream` (design D3).
pub(crate) fn salvage_manifest(
    m: &Materialized,
    reason: Reason,
    stream: Option<Incarnation>,
) -> Manifest {
    let r = &m.recovered;
    Manifest {
        reason,
        state: State::Moving,
        time: utc_now(),
        path: r.header.stream_path.clone(),
        cluster_id: Some(r.header.cluster_id.clone()),
        lineage: Some(r.header.lineage),
        incarnation: Some(r.header.incarnation),
        stream_incarnation: stream,
        acknowledged: Some(Acknowledged {
            position: r.acked,
            offset: r.acked_offset,
            unacknowledged: u64::try_from(r.pending().len()).unwrap_or(u64::MAX),
        }),
        plain: true,
    }
}

/// What the replicator starts from: a trusted or freshly built generation, as its log says, the
/// token and request limit of the open's HEAD (the log's token and the largest batch for a
/// provisional open, which got none), and the token it re-keys from when the stream's history
/// inherits it.
fn local_state(
    spec: &OpenSpec,
    m: &Materialized,
    cluster_id: ClusterId,
    (incarnation, max_request_bytes): (Incarnation, u64),
    highest_epoch: Option<Epoch>,
    rekey_from: Option<Incarnation>,
) -> LocalState {
    let r = &m.recovered;
    let lineage = r.header.lineage;
    LocalState {
        lineage,
        activation: spec.activation,
        cluster_id,
        incarnation,
        acked: r.acked,
        acked_offset: r.acked_offset,
        log_bytes: r.log_bytes,
        logged: Position(m.log.next_position().0.saturating_sub(1)),
        pending: r
            .pending()
            .into_iter()
            .map(|(position, frame)| Pending {
                id: CommitId { lineage, position },
                frame,
            })
            .collect(),
        claims: claims(m),
        highest_epoch,
        max_request_bytes,
        configured_max_request_bytes: spec.max_request_bytes,
        // A catch-up in place larger than the database rebuilds instead (design §5.6).
        db_bytes: m.db.metadata().map_or(u64::MAX, |meta| meta.len()),
        rekey_from,
        unacked: unacked(m),
    }
}

/// An open ready to bind: the root locked, the generation materialized, and what its replicator
/// starts from.
#[derive(Debug)]
pub(crate) struct Ready {
    pub(crate) spec: OpenSpec,
    pub(crate) name: CanonicalPath,
    pub(crate) root: Arc<Root>,
    pub(crate) m: Box<Materialized>,
    pub(crate) local: LocalState,
    pub(crate) deadline_ms: u64,
    pub(crate) attach: Attach,
}

/// What the open waits for.
#[derive(Debug)]
enum Phase {
    /// The [`prepare`] job.
    Preparing,
    Head,
    Create,
    /// A build's disk job (its start, a read's apply, or the publish).
    Building {
        cluster_id: ClusterId,
        head: StreamHead,
    },
    /// A build's read.
    Reading {
        build: Box<Build>,
        cluster_id: ClusterId,
        head: StreamHead,
    },
    /// A reader's catch-up in place: a read of it, or the timer before one.
    CatchingUp {
        m: Box<Materialized>,
        catch_up: CatchUp,
    },
    /// A reader's catch-up in place: a disk job applies a read's frames.
    CatchUpApplying(Applying),
}

/// An open before its takeover.
#[derive(Debug)]
pub(crate) struct Opening {
    spec: OpenSpec,
    name: CanonicalPath,
    deadline_ms: u64,
    prepared: Option<Prepared>,
    phase: Phase,
    retry: Retry,
    /// The request to send when the timer fires.
    resend: Option<(Request, Attempt)>,
    /// This open created the stream: a generation it salvages after is `recreated`.
    created: bool,
    /// Builds that found the stream missing and went back through the open table.
    missing: u32,
}

/// What an open does next.
#[derive(Debug)]
pub(crate) enum OpenStep {
    Send {
        opening: Box<Opening>,
        request: Request,
        attempt: Attempt,
    },
    Timer {
        opening: Box<Opening>,
        at_ms: u64,
        jitter_ms: u64,
    },
    Disk {
        opening: Box<Opening>,
        job: DiskJob,
    },
    Takeover(Box<Ready>),
    /// The stream is unreachable and the trusted generation holds a CLAIM record: bind now, and
    /// send the log's latest claim `claim` again once the stream answered and the pre-check found
    /// nothing foreign (design §5.5, §7).
    Provisional {
        ready: Box<Ready>,
        claim: (Epoch, Activation),
    },
    /// A reader's open is done: bind it, with no claim.
    Reader(Box<ReaderReady>),
    Fail(OpenError),
}

fn head_request() -> Request {
    Request::new(Method::Head, Target::Stream)
}

/// The create of the stream: an empty body, the VFS content type, no producer headers.
fn create_request() -> Request {
    Request {
        content_type: Some(CONTENT_TYPE.to_owned()),
        body: Arc::from(Vec::new()),
        ..Request::new(Method::Put, Target::Stream)
    }
}

impl Opening {
    /// Starts with the local steps, a disk job, in `root` when a reset binding of this process
    /// still holds the stream's root.
    pub(crate) fn start(spec: OpenSpec, root: Option<Arc<Root>>, now_ms: u64) -> OpenStep {
        let name = match canonical_path(&spec.url) {
            Ok(name) => name,
            Err(e) => return OpenStep::Fail(e.into()),
        };
        let job = DiskJob::Prepare {
            spec: spec.clone(),
            root,
        };
        OpenStep::Disk {
            opening: Box::new(Self {
                deadline_ms: now_ms.saturating_add(spec.timeout_ms),
                spec,
                name,
                prepared: None,
                phase: Phase::Preparing,
                retry: Retry::default(),
                resend: None,
                created: false,
                missing: 0,
            }),
            job,
        }
    }

    pub(crate) fn url(&self) -> &str {
        &self.spec.url
    }

    pub(crate) fn name(&self) -> &CanonicalPath {
        &self.name
    }

    /// An owner's open or a reader's.
    pub(crate) fn access(&self) -> Access {
        self.spec.access
    }

    pub(crate) fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    /// The error of an open whose deadline passed.
    pub(crate) fn expired(&self) -> OpenError {
        let condition = match &self.phase {
            Phase::Reading { build, .. } => build.condition(),
            Phase::Preparing
            | Phase::Head
            | Phase::Create
            | Phase::Building { .. }
            | Phase::CatchingUp { .. }
            | Phase::CatchUpApplying(_) => self.retry.condition(),
        };
        OpenError::Unavailable(condition)
    }

    /// The error of an open given up (`ursula_cancel`): the last condition it met, `cancelled`
    /// when it met none.
    pub(crate) fn given_up(&self) -> OpenError {
        let seen = match &self.phase {
            Phase::Reading { build, .. } => build.seen(),
            Phase::Preparing
            | Phase::Head
            | Phase::Create
            | Phase::Building { .. }
            | Phase::CatchingUp { .. }
            | Phase::CatchUpApplying(_) => self.retry.seen(),
        };
        seen.map_or(OpenError::Cancelled, OpenError::Unavailable)
    }

    /// The timer fired: send again what failed.
    pub(crate) fn tick(mut self: Box<Self>) -> OpenStep {
        let resend = self.resend.take();
        if resend.is_none() {
            let phase = std::mem::replace(&mut self.phase, Phase::Head);
            if let Phase::Reading {
                build,
                cluster_id,
                head,
            } = phase
            {
                // The build's next step: a read, or a restart after its backoff.
                let step = build.next();
                return self.build_step(build, step, cluster_id, head);
            }
            self.phase = phase;
        }
        let (request, attempt) = match (resend, &self.phase) {
            (Some(resend), _) => resend,
            (None, Phase::Create) => (create_request(), Attempt::default()),
            (None, _) => (head_request(), Attempt::default()),
        };
        OpenStep::Send {
            opening: self,
            request,
            attempt,
        }
    }

    /// A disk job of the open finished.
    pub(crate) fn disk_done(mut self: Box<Self>, done: DiskDone) -> OpenStep {
        let phase = std::mem::replace(&mut self.phase, Phase::Head);
        match (phase, done) {
            (Phase::Preparing, DiskDone::Prepared(Ok(prepared))) => {
                self.prepared = Some(prepared);
                OpenStep::Send {
                    opening: self,
                    request: head_request(),
                    attempt: Attempt::default(),
                }
            }
            (
                Phase::Building { cluster_id, head },
                DiskDone::Built(Ok(build))
                | DiskDone::Chunk {
                    build,
                    result: Ok(()),
                },
            ) => {
                let step = build.next();
                self.build_step(build, step, cluster_id, head)
            }
            (Phase::Building { cluster_id, head }, DiskDone::Published(Ok(published))) => {
                let Published {
                    m,
                    highest,
                    attach,
                    head: read,
                } = published;
                // A build that started over after a history change takes over with its token.
                let head = read.unwrap_or(head);
                match self.spec.access {
                    Access::Owner { .. } => {
                        self.takeover(m, cluster_id, &head, highest, attach, None)
                    }
                    Access::Reader => {
                        let (incarnation, closed) = (head.incarnation(), head.closed());
                        self.reader_ready(m, cluster_id.as_str(), incarnation, closed, attach)
                    }
                }
            }
            (Phase::CatchUpApplying(applying), DiskDone::CaughtUp { m, outcome }) => {
                self.caught_up(m, applying, outcome)
            }
            (
                _,
                DiskDone::Prepared(Err(e)) | DiskDone::Built(Err(e)) | DiskDone::Published(Err(e)),
            )
            | (_, DiskDone::Chunk { result: Err(e), .. }) => OpenStep::Fail(e),
            _ => OpenStep::Fail(OpenError::Panicked),
        }
    }

    /// Carries out a build's next step.
    fn build_step(
        mut self: Box<Self>,
        build: Box<Build>,
        step: BuildStep,
        cluster_id: ClusterId,
        head: StreamHead,
    ) -> OpenStep {
        match step {
            BuildStep::Send { request, attempt } => {
                self.phase = Phase::Reading {
                    build,
                    cluster_id,
                    head,
                };
                OpenStep::Send {
                    opening: self,
                    request,
                    attempt,
                }
            }
            BuildStep::Timer { at_ms, jitter_ms } => {
                self.phase = Phase::Reading {
                    build,
                    cluster_id,
                    head,
                };
                OpenStep::Timer {
                    opening: self,
                    at_ms,
                    jitter_ms,
                }
            }
            BuildStep::Install(body) => {
                self.phase = Phase::Building { cluster_id, head };
                OpenStep::Disk {
                    opening: self,
                    job: DiskJob::Install { build, body },
                }
            }
            BuildStep::Restart => {
                self.phase = Phase::Building { cluster_id, head };
                OpenStep::Disk {
                    opening: self,
                    job: DiskJob::Restart(build),
                }
            }
            BuildStep::Apply(frames) => {
                self.phase = Phase::Building { cluster_id, head };
                OpenStep::Disk {
                    opening: self,
                    job: DiskJob::Chunk { build, frames },
                }
            }
            BuildStep::Done => {
                self.phase = Phase::Building { cluster_id, head };
                OpenStep::Disk {
                    opening: self,
                    job: DiskJob::Publish(build),
                }
            }
            // The open starts again from its local steps, under the lock the build holds, a few
            // times at most: a stream deleted again and again is not this open's to chase.
            BuildStep::Missing => {
                self.missing = self.missing.saturating_add(1);
                if self.missing > MAX_BUILD_RESTARTS {
                    return OpenStep::Fail(OpenError::GaveUp(GaveUp::Missing));
                }
                self.prepared = None;
                self.phase = Phase::Preparing;
                let spec = self.spec.clone();
                OpenStep::Disk {
                    opening: self,
                    job: DiskJob::Abandon { build, spec },
                }
            }
            BuildStep::Fail(e) => OpenStep::Fail(e),
        }
    }

    /// The answer to the last request.
    pub(crate) fn answered(
        mut self: Box<Self>,
        exchange: &Exchange,
        lineage: Lineage,
        now_ms: u64,
    ) -> OpenStep {
        let phase = std::mem::replace(&mut self.phase, Phase::Head);
        match phase {
            Phase::Head => match self.spec.access {
                Access::Owner { .. } => self.head_answered(exchange, lineage, now_ms),
                Access::Reader => self.reader_head_answered(exchange, lineage, now_ms),
            },
            Phase::Create => self.create_answered(exchange, now_ms),
            Phase::CatchingUp { m, catch_up } => {
                self.catch_up_answered(m, catch_up, exchange, lineage, now_ms)
            }
            Phase::Reading {
                mut build,
                cluster_id,
                head,
            } => {
                let step = build.answered(exchange, now_ms);
                self.build_step(build, step, cluster_id, head)
            }
            // No request is out in these.
            phase @ (Phase::Preparing | Phase::Building { .. } | Phase::CatchUpApplying(_)) => {
                self.phase = phase;
                OpenStep::Fail(OpenError::Panicked)
            }
        }
    }

    fn failed(mut self: Box<Self>, exchange: &Exchange, now_ms: u64) -> OpenStep {
        match self.retry.failed(exchange, now_ms) {
            Failure::Again {
                at_ms,
                jitter_ms,
                attempt,
            } => {
                let request = match self.phase {
                    Phase::Create => create_request(),
                    _ => head_request(),
                };
                self.resend = Some((request, attempt));
                OpenStep::Timer {
                    opening: self,
                    at_ms,
                    jitter_ms,
                }
            }
            Failure::Restart => {
                self.phase = Phase::Head;
                OpenStep::Send {
                    opening: self,
                    request: head_request(),
                    attempt: Attempt::default(),
                }
            }
            Failure::Closed => OpenStep::Fail(OpenError::StreamState(StreamState::Closed)),
        }
    }

    fn create_answered(mut self: Box<Self>, exchange: &Exchange, now_ms: u64) -> OpenStep {
        self.phase = Phase::Create;
        let Exchange::Answered(answer) = exchange else {
            return self.failed(exchange, now_ms);
        };
        match &answer.typed {
            Ok(Typed::Created { .. }) => {
                self.retry.succeeded();
                // A `200` names a stream that existed: only a `201` is ours.
                self.created |= answer.status == 201;
                self.phase = Phase::Head;
                OpenStep::Send {
                    opening: self,
                    request: head_request(),
                    attempt: Attempt::default(),
                }
            }
            _ if answer.status == 409 => {
                OpenStep::Fail(OpenError::StreamFormat(StreamFormat::Configuration))
            }
            _ => self.failed(exchange, now_ms),
        }
    }

    fn head_answered(
        mut self: Box<Self>,
        exchange: &Exchange,
        lineage: Lineage,
        now_ms: u64,
    ) -> OpenStep {
        let answer = match exchange {
            Exchange::Answered(answer) => answer,
            // The first request got no answer and nothing went out: the stream is unreachable.
            Exchange::NoAnswer(NoAnswer::NotSent { .. }) if self.retry.seen().is_none() => {
                return self.unreachable(exchange, now_ms);
            }
            Exchange::NoAnswer(_) => return self.failed(exchange, now_ms),
        };
        let (identity, stream) = match &answer.typed {
            Ok(Typed::Head { identity, stream }) if matches!(answer.status, 200 | 404) => {
                (identity.clone(), *stream)
            }
            _ => return self.failed(exchange, now_ms),
        };
        self.retry.succeeded();
        let Some(prepared) = &mut self.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        let local = std::mem::replace(&mut prepared.found, Local::None);
        let seen = HeadSeen {
            identity: identity.as_ref(),
            stream: stream.as_ref(),
            content_type: answer.content_type.as_deref(),
        };
        let asked = Asked {
            recreate: matches!(self.spec.access, Access::Owner { recreate: true }),
            created: self.created,
        };
        match table::decide(local, asked, seen) {
            OpenRow::Refuse { refused, local } => {
                prepared.found = local;
                OpenStep::Fail(refused.into())
            }
            // What the open found stays for the HEAD after the create.
            OpenRow::Create { local } => {
                prepared.found = local;
                self.phase = Phase::Create;
                OpenStep::Send {
                    opening: self,
                    request: create_request(),
                    attempt: Attempt::default(),
                }
            }
            OpenRow::Resume {
                local,
                cluster_id,
                head,
            } => self.takeover(local, cluster_id, &head, None, Attach::Resumed, None),
            OpenRow::ReKey {
                local,
                from,
                cluster_id,
                head,
            } => self.takeover(local, cluster_id, &head, None, Attach::ReKeyed, Some(from)),
            OpenRow::Build {
                superseded,
                cluster_id,
                head,
            } => {
                let stream = Some(head.incarnation());
                let fate = match superseded {
                    Superseded::Nothing | Superseded::Trash(_) => Fate::Trash,
                    Superseded::Salvage { why, local } => {
                        Fate::Salvage(salvage_manifest(&local, why.into(), stream))
                    }
                    Superseded::Replaced(replaced) => Fate::Salvage(Manifest {
                        reason: Reason::Replaced,
                        state: State::Moving,
                        time: utc_now(),
                        path: self.name.as_str().to_owned(),
                        cluster_id: replaced.cluster_id,
                        lineage: replaced.lineage,
                        incarnation: None,
                        stream_incarnation: stream,
                        acknowledged: None,
                        plain: replaced.plain,
                    }),
                };
                self.build(lineage, cluster_id, head, fate)
            }
        }
    }

    /// The first HEAD got no answer with nothing sent: the unreachable rows of design §5.5.
    fn unreachable(mut self: Box<Self>, exchange: &Exchange, now_ms: u64) -> OpenStep {
        let Some(prepared) = &mut self.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        let local = std::mem::replace(&mut prepared.found, Local::None);
        let back = match table::decide_unreachable(local) {
            // A cluster id the header holds is one the open wrote: one that does not parse leaves
            // nothing a claim could be checked against, so the open waits.
            UnreachableRow::Provisional { local: m, claim } => {
                match ClusterId::new(&m.recovered.header.cluster_id) {
                    Ok(cluster_id) => return self.provisional(m, cluster_id, claim),
                    Err(_unparsed) => Local::Trusted(summary(&m), m),
                }
            }
            UnreachableRow::Wait { local } => local,
        };
        if let Some(prepared) = &mut self.prepared {
            prepared.found = back;
        }
        self.failed(exchange, now_ms)
    }

    /// Binds the trusted generation `m` now, under the log's token, its claim `claim` deferred
    /// behind the pre-check.
    fn provisional(
        self: Box<Self>,
        m: Box<Materialized>,
        cluster_id: ClusterId,
        claim: (Epoch, Activation),
    ) -> OpenStep {
        let this = *self;
        let Some(prepared) = this.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        // No HEAD named a limit yet: the pre-check's HEAD names it before anything is sealed.
        let start = (m.recovered.header.incarnation, MAX_BATCH_BYTES);
        let local = local_state(&this.spec, &m, cluster_id, start, None, None);
        OpenStep::Provisional {
            ready: Box::new(Ready {
                spec: this.spec,
                name: this.name,
                root: prepared.root,
                m,
                local,
                deadline_ms: this.deadline_ms,
                attach: Attach::Resumed,
            }),
            claim,
        }
    }

    /// Builds a generation of a fresh `lineage` from the stream `head` names (a disk job), the one
    /// it supersedes meeting `fate` at its publish.
    fn build(
        mut self: Box<Self>,
        lineage: Lineage,
        cluster_id: ClusterId,
        head: StreamHead,
        fate: Fate,
    ) -> OpenStep {
        let Some(prepared) = &self.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        let job = DiskJob::NewBuild(NewBuild {
            root: Arc::clone(&prepared.root),
            name: self.name.clone(),
            lineage,
            cluster_id: cluster_id.clone(),
            incarnation: head.incarnation(),
            first: Position(1),
            claim: None,
            end: head.next_offset(),
            fate,
        });
        self.phase = Phase::Building { cluster_id, head };
        OpenStep::Disk { opening: self, job }
    }

    fn takeover(
        self: Box<Self>,
        m: Box<Materialized>,
        cluster_id: ClusterId,
        head: &StreamHead,
        highest_epoch: Option<Epoch>,
        attach: Attach,
        rekey_from: Option<Incarnation>,
    ) -> OpenStep {
        let this = *self;
        let Some(prepared) = this.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        let start = (head.incarnation(), head.max_request_bytes());
        let local = local_state(&this.spec, &m, cluster_id, start, highest_epoch, rekey_from);
        OpenStep::Takeover(Box::new(Ready {
            spec: this.spec,
            name: this.name,
            root: prepared.root,
            m,
            local,
            deadline_ms: this.deadline_ms,
            attach,
        }))
    }
}
