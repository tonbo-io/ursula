//! The inventory of the module doc as data, for conformance tests of a
//! client or an intermediary: every answer a write can get, with a sample
//! of it rendered as the node or the gateway renders it, and what
//! [`classify`] reads. Built with the `test-support` feature only.

use super::WriteKind;
use super::WriteOutcome;
use super::classify;
use crate::answer::ClosedEpoch;
use crate::answer::CurrentEpoch;
use crate::answer::Delivery;
use crate::answer::EpochClosure;
use crate::answer::ProducerAnswer;
use crate::answer::Receipt;
use crate::error::HeaderError;
use crate::error_body::ErrorBody;
use crate::error_body::ErrorCode;
use crate::limit::SizeLimit;
use crate::limit::StagedPartsLimit;
use crate::names;
use crate::refusal::IncarnationMismatch;
use crate::refusal::StreamClosedConflict;
use crate::refusal::StreamSeqConflict;
use crate::routing::NodeId;
use crate::routing::RaftGroup;
use crate::routing::RaftTerm;
use crate::routing::RoutingAnswer;
use crate::snapshot::PartId;
use crate::snapshot::PartStaged;
use crate::snapshot::SnapshotPublishAnswer;
use crate::snapshot::SnapshotRef;
use crate::value::Epoch;
use crate::value::Incarnation;
use crate::value::Offset;
use crate::value::Seq;
use crate::value::SnapshotDigest;
use crate::value::StreamSeq;

/// Who sends an inventory row's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// An Ursula node. A gateway passes its answer through, but for a
    /// redirect, which it follows.
    Node,
    /// The Ursula gateway, on its own.
    Gateway,
    /// An embedder of the gateway, through its refusal API.
    Embedder,
    /// A layer outside Ursula, such as a proxy, or no answer at all.
    Foreign,
}

/// The write a row's representative request makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// An append with producer headers, a claim included.
    ProducerAppend,
    /// An append without producer headers.
    Append,
    /// A close without producer headers and without a body.
    Close,
    Create,
    SnapshotPublish,
    PartStage,
    PartsCommit,
    /// `DELETE {stream}/snapshot/parts`.
    PartsDiscard,
    /// `PUT {stream}/retention/{offset}`.
    Retention,
    /// `DELETE {stream}`.
    DeleteStream,
    /// `PUT /{bucket}`.
    CreateBucket,
}

/// The typed headers a row's answer carries beside its error body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// None: the status and the error body speak.
    Plain,
    /// A producer answer.
    Producer(ProducerShape),
    /// `Stream-Next-Offset` of a keyless write, with `Stream-Closed` when
    /// it closed the stream.
    Keyless { closes: bool },
    /// `Stream-Incarnation` of a create.
    Created,
    /// The snapshot a publish or a commit published.
    SnapshotPublished,
    /// The latest snapshot of a refused publish.
    SnapshotConflict,
    /// `Stream-Snapshot-Part`.
    PartStaged,
    /// The leader a `307` names.
    Redirect,
    /// `x-ursula-raft-group` of a routing `503`.
    NoLeader,
    /// `Stream-Closed: true` and the final offset.
    StreamClosed,
    /// `Stream-Last-Seq`.
    StreamSeqConflict,
    /// The current `Stream-Incarnation` of a `412`.
    IncarnationMismatch,
    /// The limit a `413` applied.
    SizeLimit,
    /// The staged-parts limit a `429` applied, and `Stream-Incarnation`.
    StagedPartsLimit,
    /// `Stream-Retained-Offset` and `Stream-Incarnation` of a retention
    /// advance.
    Retained,
}

/// The producer answer of a [`Shape::Producer`] row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerShape {
    Applied,
    Repeat,
    ReceiptEvicted,
    EpochClosed,
    StaleEpoch,
    KeyConflict,
    Expired,
    SeqGap,
    EpochNotClaimed,
}

/// One row of the inventory (module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The row's id in the module doc's table.
    pub id: &'static str,
    pub source: Source,
    /// The representative request.
    pub write: Write,
    /// `0` for no answer.
    pub status: u16,
    pub shape: Shape,
    /// The error body's code, `None` for no error body.
    pub code: Option<ErrorCode>,
    /// What [`classify`] reads.
    pub outcome: WriteOutcome,
}

impl Write {
    /// What [`classify`] is told the write is.
    pub fn kind(self) -> WriteKind {
        match self {
            Self::ProducerAppend => WriteKind::Producer,
            Self::Append | Self::Close => WriteKind::Append,
            Self::Create => WriteKind::Create,
            Self::SnapshotPublish | Self::PartsCommit => WriteKind::Publish,
            Self::PartStage => WriteKind::PartStage,
            Self::PartsDiscard => WriteKind::PartsDiscard,
            Self::Retention => WriteKind::Retention,
            Self::DeleteStream => WriteKind::DeleteStream,
            Self::CreateBucket => WriteKind::CreateBucket,
        }
    }
}

/// An answer as it travels: a status, `(name, value)` headers and a body,
/// to a write of `kind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    pub kind: WriteKind,
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

impl Sample {
    /// The headers as [`classify`] and the parsers take them.
    pub fn pairs(&self) -> impl Iterator<Item = (&str, &[u8])> + Clone {
        self.headers
            .iter()
            .map(|(name, value)| (*name, value.as_bytes()))
    }

    #[must_use]
    pub fn classify(&self) -> WriteOutcome {
        classify(self.kind, self.status, self.pairs(), &self.body)
    }
}

impl Row {
    /// The row's answer with fixed values, as the node or the gateway
    /// renders it. `Ok(None)` for `transport-failure`, which has no
    /// answer. `Err` only if a sample value were out of its range.
    pub fn sample(&self) -> Result<Option<Sample>, HeaderError> {
        if self.status == 0 {
            return Ok(None);
        }
        let mut headers = Vec::new();
        render_shape(self.shape, &mut |name, value| {
            headers.push((name, value.to_owned()))
        })?;
        let body = match &self.code {
            Some(code) => {
                headers.push(("content-type", ErrorBody::CONTENT_TYPE.to_owned()));
                ErrorBody::fixed(code.clone()).render().into_bytes()
            }
            None => Vec::new(),
        };
        Ok(Some(Sample {
            kind: self.write.kind(),
            status: self.status,
            headers,
            body,
        }))
    }
}

/// The sample values.
const SAMPLE_INCARNATION: u64 = 4_503_599_627_370_497;
const SAMPLE_TERM: u64 = 7;

fn sample_snapshot() -> SnapshotRef {
    SnapshotRef {
        offset: Offset::from_u64(3),
        digest: SnapshotDigest::from_bytes([7; 32]),
    }
}

fn render_shape(shape: Shape, emit: &mut dyn FnMut(&'static str, &str)) -> Result<(), HeaderError> {
    let incarnation = Incarnation::try_from(SAMPLE_INCARNATION)?;
    match shape {
        Shape::Plain => {}
        Shape::Producer(producer) => producer_answer(producer, incarnation)?.render(emit),
        Shape::Keyless { closes } => {
            emit(names::STREAM_NEXT_OFFSET, &Offset::from_u64(3).to_string());
            if closes {
                emit(names::STREAM_CLOSED, "true");
            }
        }
        Shape::Created => emit(names::STREAM_INCARNATION, &incarnation.to_string()),
        Shape::SnapshotPublished => SnapshotPublishAnswer::Published {
            snapshot: sample_snapshot(),
            incarnation,
        }
        .render(emit),
        Shape::SnapshotConflict => SnapshotPublishAnswer::Conflict {
            latest: sample_snapshot(),
        }
        .render(emit),
        Shape::PartStaged => PartStaged {
            part: PartId::new("part-1")?,
        }
        .render(emit),
        Shape::Redirect => {
            emit("location", "http://leader.invalid/bucket/stream");
            RoutingAnswer::NotLeader {
                leader: NodeId::from_u64(2),
                group: RaftGroup::from_u32(0),
                term: RaftTerm::from_u64(SAMPLE_TERM),
            }
            .render(emit);
        }
        Shape::NoLeader => {
            emit("retry-after", "1");
            RoutingAnswer::NoLeader {
                group: RaftGroup::from_u32(0),
            }
            .render(emit);
        }
        Shape::StreamClosed => StreamClosedConflict {
            final_offset: Offset::from_u64(3),
        }
        .render(emit),
        Shape::StreamSeqConflict => StreamSeqConflict {
            last: StreamSeq::new("7")?,
        }
        .render(emit),
        Shape::IncarnationMismatch => IncarnationMismatch {
            current: Some(incarnation),
        }
        .render(emit),
        Shape::SizeLimit => SizeLimit::Request(1024).render(emit),
        Shape::Retained => {
            emit(
                names::STREAM_RETAINED_OFFSET,
                &Offset::from_u64(3).to_string(),
            );
            emit(names::STREAM_INCARNATION, &incarnation.to_string());
        }
        Shape::StagedPartsLimit => {
            StagedPartsLimit::Parts(8192).render(&mut *emit);
            emit(names::STREAM_INCARNATION, &incarnation.to_string());
        }
    }
    Ok(())
}

fn producer_answer(
    shape: ProducerShape,
    incarnation: Incarnation,
) -> Result<ProducerAnswer, HeaderError> {
    let epoch = Epoch::try_from;
    let seq = Seq::try_from;
    let receipt = |delivery| -> Result<ProducerAnswer, HeaderError> {
        Ok(ProducerAnswer::Receipt {
            delivery,
            epoch: CurrentEpoch::new(epoch(1)?),
            seq: seq(2)?,
            receipt: Receipt::new(Offset::from_u64(1), Offset::from_u64(3), false)?,
            incarnation,
        })
    };
    Ok(match shape {
        ProducerShape::Applied => receipt(Delivery::Applied)?,
        ProducerShape::Repeat => receipt(Delivery::Repeat)?,
        ProducerShape::ReceiptEvicted => ProducerAnswer::ReceiptEvicted {
            epoch: CurrentEpoch::new(epoch(1)?),
            seq: seq(2)?,
            incarnation,
        },
        ProducerShape::EpochClosed => {
            ProducerAnswer::EpochClosed(EpochClosure::by_claim(epoch(2)?, ClosedEpoch {
                epoch: epoch(1)?,
                seq: seq(1)?,
            })?)
        }
        ProducerShape::StaleEpoch => ProducerAnswer::StaleEpoch { epoch: epoch(3)? },
        ProducerShape::KeyConflict => ProducerAnswer::KeyConflict { epoch: epoch(1)? },
        ProducerShape::Expired => ProducerAnswer::ProducerExpired { epoch: epoch(1)? },
        ProducerShape::SeqGap => ProducerAnswer::SeqGap {
            expected: seq(2)?,
            received: seq(5)?,
        },
        ProducerShape::EpochNotClaimed => ProducerAnswer::EpochNotClaimed,
    })
}

macro_rules! rows {
    ($($id:literal, $source:ident, $write:ident, $status:literal, $shape:expr, $code:expr, $outcome:expr;)+) => {
        &[$(Row {
            id: $id,
            source: Source::$source,
            write: Write::$write,
            status: $status,
            shape: $shape,
            code: $code,
            outcome: $outcome,
        },)+]
    };
}

/// The inventory of the module doc, in its order.
pub const INVENTORY: &[Row] = {
    use ErrorCode as C;
    use ProducerShape as P;
    use Shape as S;
    use WriteOutcome::Applied;
    use WriteOutcome::Redirected;
    use WriteOutcome::Refused;
    use WriteOutcome::Repeat;
    use WriteOutcome::Unknown;
    rows! {
        "append-applied", Node, ProducerAppend, 200, S::Producer(P::Applied), None, Applied;
        "append-repeat", Node, ProducerAppend, 204, S::Producer(P::Repeat), None, Repeat;
        "append-receipt-evicted", Node, ProducerAppend, 204, S::Producer(P::ReceiptEvicted), None, Repeat;
        "append-keyless", Node, Append, 204, S::Keyless { closes: false }, None, Applied;
        "close-keyless", Node, Close, 204, S::Keyless { closes: true }, None, Applied;
        "create-new", Node, Create, 201, S::Created, None, Applied;
        "create-matching", Node, Create, 200, S::Created, None, Applied;
        "snapshot-published", Node, SnapshotPublish, 204, S::SnapshotPublished, None, Applied;
        "part-staged", Node, PartStage, 201, S::PartStaged, None, Applied;
        "parts-discarded", Node, PartsDiscard, 204, S::Created, None, Applied;
        "retention-advanced", Node, Retention, 204, S::Retained, None, Applied;
        "stream-deleted", Node, DeleteStream, 204, S::Plain, None, Applied;
        "bucket-created", Node, CreateBucket, 201, S::Plain, None, Applied;
        "parts-commit-published", Node, PartsCommit, 204, S::SnapshotPublished, None, Applied;
        "redirect", Node, ProducerAppend, 307, S::Redirect, None, Redirected;
        "producer-epoch-closed", Node, ProducerAppend, 403, S::Producer(P::EpochClosed), Some(C::ProducerEpochClosed), Refused(C::ProducerEpochClosed);
        "producer-stale-epoch", Node, ProducerAppend, 403, S::Producer(P::StaleEpoch), Some(C::ProducerEpochStale), Refused(C::ProducerEpochStale);
        "producer-key-conflict", Node, ProducerAppend, 403, S::Producer(P::KeyConflict), Some(C::ProducerKeyConflict), Refused(C::ProducerKeyConflict);
        "producer-expired", Node, ProducerAppend, 403, S::Producer(P::Expired), Some(C::ProducerExpired), Refused(C::ProducerExpired);
        "producer-seq-gap", Node, ProducerAppend, 409, S::Producer(P::SeqGap), Some(C::ProducerSeqConflict), Refused(C::ProducerSeqConflict);
        "producer-epoch-not-claimed", Node, ProducerAppend, 400, S::Producer(P::EpochNotClaimed), Some(C::InvalidProducer), Refused(C::InvalidProducer);
        "bad-request", Node, Append, 400, S::Plain, Some(C::EmptyAppend), Refused(C::EmptyAppend);
        "not-found", Node, ProducerAppend, 404, S::Plain, Some(C::StreamNotFound), Refused(C::StreamNotFound);
        "stream-closed", Node, ProducerAppend, 409, S::StreamClosed, Some(C::StreamClosed), Refused(C::StreamClosed);
        "stream-seq-conflict", Node, Append, 409, S::StreamSeqConflict, Some(C::StreamSeqConflict), Refused(C::StreamSeqConflict);
        "snapshot-conflict", Node, SnapshotPublish, 409, S::SnapshotConflict, Some(C::SnapshotConflict), Refused(C::SnapshotConflict);
        "conflict", Node, Append, 409, S::Plain, Some(C::ContentTypeMismatch), Refused(C::ContentTypeMismatch);
        "gone", Node, SnapshotPublish, 410, S::Plain, Some(C::StreamGone), Refused(C::StreamGone);
        "incarnation-mismatch", Node, ProducerAppend, 412, S::IncarnationMismatch, Some(C::IncarnationMismatch), Refused(C::IncarnationMismatch);
        "too-large", Node, Append, 413, S::SizeLimit, Some(C::RequestTooLarge), Refused(C::RequestTooLarge);
        "producer-limit", Node, ProducerAppend, 429, S::Plain, Some(C::ProducerLimit), Refused(C::ProducerLimit);
        "staged-parts-limit", Node, PartStage, 429, S::StagedPartsLimit, Some(C::StagedPartsLimit), Refused(C::StagedPartsLimit);
        "no-cold-store", Node, PartStage, 501, S::Plain, Some(C::NoColdStore), Refused(C::NoColdStore);
        "no-leader-before-body", Node, ProducerAppend, 503, S::NoLeader, Some(C::NoLeader), Refused(C::NoLeader);
        "no-leader-before-proposal", Node, ProducerAppend, 503, S::NoLeader, Some(C::NoLeader), Refused(C::NoLeader);
        "leadership-lost-after-proposal", Node, ProducerAppend, 503, S::Plain, Some(C::OutcomeUnknown), Unknown;
        "outcome-unknown", Node, ProducerAppend, 503, S::Plain, Some(C::OutcomeUnknown), Unknown;
        "wal-disk-pressure", Node, ProducerAppend, 503, S::Plain, Some(C::WalDiskPressure), Refused(C::WalDiskPressure);
        "raft-log-pressure", Node, ProducerAppend, 503, S::Plain, Some(C::RaftLogPressure), Refused(C::RaftLogPressure);
        "ingress-body-budget", Node, ProducerAppend, 503, S::Plain, Some(C::IngressBodyBytesLimitReached), Refused(C::IngressBodyBytesLimitReached);
        "cold-backpressure", Node, ProducerAppend, 503, S::Plain, Some(C::ColdBackpressure), Refused(C::ColdBackpressure);
        "raft-uncommitted-backpressure", Node, ProducerAppend, 503, S::Plain, Some(C::RaftUncommittedBackpressure), Refused(C::RaftUncommittedBackpressure);
        "history-pending", Node, ProducerAppend, 503, S::Plain, Some(C::HistoryPending), Refused(C::HistoryPending);
        "apply-stopped", Node, ProducerAppend, 503, S::Plain, Some(C::ApplyStopped), Refused(C::ApplyStopped);
        "group-restarting", Node, ProducerAppend, 503, S::Plain, Some(C::GroupRestarting), Refused(C::GroupRestarting);
        "json-boundary-unverified", Node, SnapshotPublish, 503, S::Plain, Some(C::JsonBoundaryUnverified), Refused(C::JsonBoundaryUnverified);
        "snapshot-commits-busy", Node, PartsCommit, 503, S::Plain, Some(C::SnapshotCommitsBusy), Refused(C::SnapshotCommitsBusy);
        "snapshot-commit-in-progress", Node, PartsCommit, 503, S::Plain, Some(C::SnapshotCommitInProgress), Unknown;
        "internal", Node, ProducerAppend, 500, S::Plain, Some(C::Internal), Unknown;
        "cold-store-failed", Node, ProducerAppend, 502, S::Plain, Some(C::ColdStoreFailed), Unknown;
        "gateway-invalid-credential", Gateway, ProducerAppend, 401, S::Plain, Some(C::InvalidCredential), Refused(C::InvalidCredential);
        "gateway-forbidden", Gateway, ProducerAppend, 403, S::Plain, Some(C::Forbidden), Refused(C::Forbidden);
        "gateway-concealed", Gateway, ProducerAppend, 404, S::Plain, Some(C::BucketNotFound), Refused(C::BucketNotFound);
        "gateway-too-large", Gateway, ProducerAppend, 413, S::SizeLimit, Some(C::RequestTooLarge), Refused(C::RequestTooLarge);
        "gateway-rate-limited", Gateway, ProducerAppend, 429, S::Plain, Some(C::RateLimited), Refused(C::RateLimited);
        "gateway-uploads-exhausted", Gateway, ProducerAppend, 429, S::Plain, Some(C::UploadsExhausted), Refused(C::UploadsExhausted);
        "gateway-upload-bytes-exhausted", Gateway, ProducerAppend, 429, S::Plain, Some(C::UploadBytesExhausted), Refused(C::UploadBytesExhausted);
        "gateway-upload-budget-exhausted", Gateway, ProducerAppend, 503, S::Plain, Some(C::UploadBudgetExhausted), Refused(C::UploadBudgetExhausted);
        "gateway-body-too-slow", Gateway, ProducerAppend, 408, S::Plain, Some(C::BodyTooSlow), Refused(C::BodyTooSlow);
        "gateway-authentication-unavailable", Gateway, ProducerAppend, 503, S::Plain, Some(C::AuthenticationUnavailable), Refused(C::AuthenticationUnavailable);
        "gateway-authorization-unavailable", Gateway, ProducerAppend, 503, S::Plain, Some(C::AuthorizationUnavailable), Refused(C::AuthorizationUnavailable);
        "gateway-no-upstream", Gateway, ProducerAppend, 503, S::Plain, Some(C::NoUpstream), Refused(C::NoUpstream);
        "gateway-upstream-unreachable", Gateway, ProducerAppend, 503, S::Plain, Some(C::UpstreamUnreachable), Refused(C::UpstreamUnreachable);
        "gateway-no-leader", Gateway, ProducerAppend, 503, S::Plain, Some(C::NoLeader), Refused(C::NoLeader);
        "gateway-upstream-failed", Gateway, ProducerAppend, 502, S::Plain, Some(C::UpstreamFailed), Unknown;
        "gateway-upstream-timeout", Gateway, ProducerAppend, 504, S::Plain, Some(C::UpstreamTimeout), Unknown;
        "embedder-payment-required", Embedder, ProducerAppend, 402, S::Plain, Some(C::PaymentRequired), Refused(C::PaymentRequired);
        "embedder-account-revoked", Embedder, ProducerAppend, 403, S::Plain, Some(C::AccountRevoked), Refused(C::AccountRevoked);
        "embedder-forbidden", Embedder, ProducerAppend, 403, S::Plain, Some(C::Forbidden), Refused(C::Forbidden);
        "embedder-rate-limited", Embedder, ProducerAppend, 429, S::Plain, Some(C::RateLimited), Refused(C::RateLimited);
        "foreign-4xx", Foreign, ProducerAppend, 403, S::Plain, None, Refused(C::InvalidRequest);
        "foreign-408", Foreign, ProducerAppend, 408, S::Plain, None, Unknown;
        "foreign-5xx", Foreign, ProducerAppend, 503, S::Plain, None, Unknown;
        "keyed-2xx-without-receipt", Foreign, ProducerAppend, 200, S::Plain, None, Unknown;
        "keyless-2xx-untyped", Foreign, Create, 201, S::Plain, None, Unknown;
        "transport-failure", Foreign, ProducerAppend, 0, S::Plain, None, Unknown;
    }
};

/// The row `id` names, if the inventory holds one.
pub fn row(id: &str) -> Option<&'static Row> {
    INVENTORY.iter().find(|row| row.id == id)
}
