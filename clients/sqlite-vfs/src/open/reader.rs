//! A reader's open after its HEAD (design: Readers): the reader rows ([`table::decide_reader`])
//! carried out, a catch-up in place of a trusted reader generation, and the row of an unreachable
//! stream.
//!
//! A reader holds no commit of its own, so its generation is the stream as of its open and nothing
//! else. A trusted one of the stream's token is caught up in place: the stream is read from the
//! log's acknowledged offset to the tail the HEAD named, under the token's precondition, and each
//! read's frames are logged, acknowledged and fsynced, then applied (a disk job,
//! [`crate::materialize::catch_up_files`]), so a crash midway replays. A `410` (retention passed
//! the acknowledged offset) builds anew, the old generation trashed, and a `412` or `404` sends the
//! HEAD again, which decides by the reader rows once more. Nothing is ever claimed or appended.
//!
//! When the first HEAD got no answer with nothing sent, a trusted reader generation opens as it
//! is, the stream as of its last open. Without one the open retries until its deadline.

use std::sync::Arc;

use ursula_headers::ClusterId;
use ursula_headers::Incarnation;
use ursula_headers::StreamHead;

use super::Failure;
use super::Fetch;
use super::OpenError;
use super::OpenStep;
use super::Opening;
use super::Phase;
use super::StreamState;
use super::head_request;
use super::local;
use super::summary;
use super::table;
use super::table::HeadSeen;
use super::table::Local;
use super::table::ReaderRow;
use super::table::ReaderUnreachableRow;
use crate::driver::Attach;
use crate::driver::disk::DiskJob;
use crate::generation::CanonicalPath;
use crate::generation::Fate;
use crate::generation::Root;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::materialize::ForeignFrame;
use crate::materialize::LocalError;
use crate::materialize::Materialized;
use crate::replicator::answer::ReadDecision;
use crate::replicator::answer::decide_read;
use crate::transport::Attempt;
use crate::wire::Exchange;
use crate::wire::NoAnswer;
use crate::wire::Typed;

/// A reader's open, done: what its binding starts from.
#[derive(Debug)]
pub(crate) struct ReaderReady {
    pub(crate) name: CanonicalPath,
    /// The cache root the open was asked for, as its spec named it.
    pub(crate) dir: std::path::PathBuf,
    /// The reader directory, locked.
    pub(crate) root: Arc<Root>,
    pub(crate) m: Box<Materialized>,
    /// The cluster id of the HEAD the open read, or of the generation's log when the stream was
    /// unreachable.
    pub(crate) cluster_id: String,
    /// The stream's token, the same way.
    pub(crate) incarnation: Incarnation,
    /// The HEAD the open read named the stream closed: a reader reads a closed stream.
    pub(crate) closed: bool,
    pub(crate) attach: Attach,
}

/// A catch-up in place: the reads from the reader generation's acknowledged offset to the tail.
#[derive(Debug)]
pub(crate) struct CatchUp {
    fetch: Fetch,
    cluster_id: ClusterId,
    head: StreamHead,
}

/// A catch-up in place whose generation a disk job holds, applying one read's frames.
#[derive(Debug)]
pub(crate) struct Applying(CatchUp);

impl Opening {
    /// The reader's HEAD answered, or got no answer.
    pub(super) fn reader_head_answered(
        mut self: Box<Self>,
        exchange: &Exchange,
        lineage: Lineage,
        now_ms: u64,
    ) -> OpenStep {
        let answer = match exchange {
            Exchange::Answered(answer) => answer,
            // The first request got no answer and nothing went out: the stream is unreachable.
            Exchange::NoAnswer(NoAnswer::NotSent { .. }) if self.retry.seen().is_none() => {
                return self.reader_unreachable(exchange, now_ms);
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
        let found = std::mem::replace(&mut prepared.found, Local::None);
        let seen = HeadSeen {
            identity: identity.as_ref(),
            stream: stream.as_ref(),
            content_type: answer.content_type.as_deref(),
        };
        match table::decide_reader(found, seen) {
            ReaderRow::Refuse(refused) => OpenStep::Fail(refused.into()),
            ReaderRow::Build { cluster_id, head } => {
                self.build(lineage, cluster_id, head, Fate::Trash)
            }
            ReaderRow::CatchUp {
                local: m,
                cluster_id,
                head,
            } => {
                let fetch = Fetch::new(
                    head.incarnation(),
                    m.recovered.acked_offset,
                    head.next_offset(),
                    None,
                );
                let catch_up = CatchUp {
                    fetch,
                    cluster_id,
                    head,
                };
                self.catch_up_next(m, catch_up)
            }
        }
    }

    /// The reader's first HEAD got no answer with nothing sent: a trusted reader generation opens
    /// as it is, and otherwise the open retries until its deadline.
    fn reader_unreachable(mut self: Box<Self>, exchange: &Exchange, now_ms: u64) -> OpenStep {
        let Some(prepared) = &mut self.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        let found = std::mem::replace(&mut prepared.found, Local::None);
        match table::decide_reader_unreachable(found) {
            ReaderUnreachableRow::Open { local: m } => {
                let header = &m.recovered.header;
                let (cluster_id, incarnation) = (header.cluster_id.clone(), header.incarnation);
                // Whether the stream closed since is unknown: reported open.
                self.reader_ready(m, &cluster_id, incarnation, false, Attach::Resumed)
            }
            ReaderUnreachableRow::Wait { local: found } => {
                prepared.found = found;
                self.failed(exchange, now_ms)
            }
        }
    }

    /// The catch-up's next read, or the reader done once the tail is read.
    fn catch_up_next(mut self: Box<Self>, m: Box<Materialized>, catch_up: CatchUp) -> OpenStep {
        if catch_up.fetch.done() {
            let cluster_id = catch_up.cluster_id.as_str().to_owned();
            let incarnation = catch_up.head.incarnation();
            let closed = catch_up.head.closed();
            return self.reader_ready(m, &cluster_id, incarnation, closed, Attach::CaughtUp);
        }
        let request = catch_up.fetch.request();
        self.phase = Phase::CatchingUp { m, catch_up };
        OpenStep::Send {
            opening: self,
            request,
            attempt: Attempt::default(),
        }
    }

    /// The answer to a read of the catch-up.
    pub(super) fn catch_up_answered(
        mut self: Box<Self>,
        m: Box<Materialized>,
        mut catch_up: CatchUp,
        exchange: &Exchange,
        lineage: Lineage,
        now_ms: u64,
    ) -> OpenStep {
        match decide_read(exchange) {
            ReadDecision::Bytes { next } => {
                self.retry.succeeded();
                let body = match exchange {
                    Exchange::Answered(answer) => answer.body.as_slice(),
                    Exchange::NoAnswer(_) => &[],
                };
                match catch_up.fetch.feed(body, next) {
                    Ok(frames) if frames.is_empty() => self.catch_up_next(m, catch_up),
                    Ok(frames) => self.catch_up_apply(m, catch_up, frames),
                    Err(e) => OpenStep::Fail(e),
                }
            }
            // Retention passed what the reader generation acknowledged: a build replaces it.
            ReadDecision::Gone => {
                self.retry.succeeded();
                drop(m);
                let CatchUp {
                    cluster_id, head, ..
                } = catch_up;
                self.build(lineage, cluster_id, head, Fate::Trash)
            }
            ReadDecision::Common(_) => match self.retry.failed(exchange, now_ms) {
                Failure::Again {
                    at_ms,
                    jitter_ms,
                    attempt,
                } => {
                    self.resend = Some((catch_up.fetch.request(), attempt));
                    self.phase = Phase::CatchingUp { m, catch_up };
                    OpenStep::Timer {
                        opening: self,
                        at_ms,
                        jitter_ms,
                    }
                }
                // A `412` or `404`: the HEAD decides again, by the reader rows.
                Failure::Restart => {
                    let Some(prepared) = &mut self.prepared else {
                        return OpenStep::Fail(OpenError::Panicked);
                    };
                    prepared.found = Local::Trusted(summary(&m), m);
                    self.phase = Phase::Head;
                    OpenStep::Send {
                        opening: self,
                        request: head_request(),
                        attempt: Attempt::default(),
                    }
                }
                Failure::Closed => OpenStep::Fail(OpenError::StreamState(StreamState::Closed)),
            },
        }
    }

    /// One read's frames into the reader generation, a disk job.
    fn catch_up_apply(
        mut self: Box<Self>,
        m: Box<Materialized>,
        catch_up: CatchUp,
        frames: Vec<ForeignFrame>,
    ) -> OpenStep {
        let Some(prepared) = &self.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        let job = DiskJob::CatchUp {
            m,
            frames,
            root: Arc::clone(&prepared.root),
        };
        self.phase = Phase::CatchUpApplying(Applying(catch_up));
        OpenStep::Disk { opening: self, job }
    }

    /// A read's frames are applied: the next read.
    pub(super) fn caught_up(
        self: Box<Self>,
        m: Box<Materialized>,
        Applying(catch_up): Applying,
        outcome: Result<(Position, u64), LocalError>,
    ) -> OpenStep {
        match outcome {
            Ok(_) => self.catch_up_next(m, catch_up),
            Err(e) => OpenStep::Fail(local(e)),
        }
    }

    /// The reader done with `m`, bound under `cluster_id` and `incarnation` (of a stream the open
    /// found `closed` or not) in the reader directory the open prepared.
    pub(super) fn reader_ready(
        self: Box<Self>,
        m: Box<Materialized>,
        cluster_id: &str,
        incarnation: Incarnation,
        closed: bool,
        attach: Attach,
    ) -> OpenStep {
        let this = *self;
        let Some(prepared) = this.prepared else {
            return OpenStep::Fail(OpenError::Panicked);
        };
        OpenStep::Reader(Box::new(ReaderReady {
            name: this.name,
            dir: this.spec.dir,
            root: prepared.root,
            m,
            cluster_id: cluster_id.to_owned(),
            incarnation,
            closed,
            attach,
        }))
    }
}
