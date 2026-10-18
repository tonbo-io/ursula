//! The VFS's action for every row of the write-outcome inventory
//! (`ursula_headers::write_outcome::inventory`). The VFS classifies a write's answer once, with
//! `ursula_headers::classify` ([`crate::wire::Answer::outcome`]), and maps the outcome to its
//! action in `replicator/answer.rs`. This test pins that mapping row by row.
//!
//! Each row's answer goes through every reader of write answers the VFS has, by its production
//! entry points:
//!
//! - [`decide_append`], for the in-flight keyed append, which decides a producer answer by its own
//!   rows and hands every other answer to [`decide_status`];
//! - [`decide_claim`], for the in-flight claim (`Producer-Seq: 0`), for the rows of a keyed write;
//! - the snapshotter's [`transient`] for a publish, a parts commit, a discard or a retention
//!   advance, and [`transient_part`] for a part, which tell an answer to send again from one the
//!   snapshotter decides on.
//!
//! [`EXPECTED`] names what each reader does with each row, and whether the replicator's action is
//! the one the outcome asks of a client ([`Mapping::Follows`]) or a deliberate difference that
//! `replicator/answer.rs` maps explicitly ([`Mapping::Deliberate`], with why it is safe). Every
//! reader sends again whatever `classify` reads as `Unknown`. A new row, a changed VFS action or a
//! changed classification fails the test.

use std::sync::Arc;

use ursula_headers::WriteOutcome;
use ursula_headers::write_outcome::inventory::INVENTORY;
use ursula_headers::write_outcome::inventory::Row;
use ursula_headers::write_outcome::inventory::Write;

use super::epoch;
use super::local;
use super::off;
use super::seq;
use crate::replicator::BlockedReason;
use crate::replicator::Ctx;
use crate::replicator::ResetKind;
use crate::replicator::answer::AppendDecision;
use crate::replicator::answer::ClaimDecision;
use crate::replicator::answer::Common;
use crate::replicator::answer::decide_append;
use crate::replicator::answer::decide_claim;
use crate::replicator::answer::decide_status;
use crate::replicator::batch::Sealed;
use crate::snapshotter::transient;
use crate::snapshotter::transient_part;
use crate::wire::Answer;
use crate::wire::Exchange;
use crate::wire::LOCATION;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::parse_answer;

/// What the replicator does with an answer, coarsely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Vfs {
    /// The write landed (an append), or the claim holds its epoch.
    Acknowledge,
    /// The identical request goes again.
    Resend,
    /// A claim of the next epoch, after `producer-expired`.
    ContinuationClaim,
    /// The claim lost its epoch to another claimant or a newer epoch.
    Lost,
    /// A HEAD decides.
    Head,
    Reauthorize,
    Redirect,
    LowerLimit,
    /// The request waits for its cause to go and is sent again every 30 s.
    Blocked,
    /// A reset of the binding: the write did not land as sent.
    Reset,
}

/// What the snapshotter does with an answer to a publish, a commit or a part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Snap {
    /// [`transient`]: the request goes again.
    Resend,
    /// The snapshotter decides on the answer: published, superseded, or failed.
    Decides,
}

/// Whether the replicator's action for a row is the one its outcome asks of a client.
#[derive(Debug, Clone, Copy)]
enum Mapping {
    /// Acknowledge what holds, resend what may have applied, follow a redirect, and stop or
    /// repair on a refusal.
    Follows,
    /// A difference `replicator/answer.rs` maps explicitly, and why it is safe.
    Deliberate(&'static str),
}

impl Mapping {
    /// Why the action differs from the outcome's, for a deliberate difference.
    fn reason(self) -> Option<&'static str> {
        match self {
            Self::Follows => None,
            Self::Deliberate(reason) => Some(reason),
        }
    }
}

/// One row of what the VFS does: its append and claim actions (`None` for a row that is no keyed
/// write's answer), its snapshotter's, and how the replicator's relate to the outcome.
struct Expected {
    id: &'static str,
    append: Vfs,
    claim: Option<Vfs>,
    snapshot: Snap,
    mapping: Mapping,
}

const FOLLOWS: Mapping = Mapping::Follows;
/// answer.rs: `Refused`, a `408`, `429` or `5xx`.
const SAFE_RESEND: Mapping = Mapping::Deliberate(
    "refused, and a refusal of load or routing passes: a keyed write's repeat applies once or \
     gets its receipt",
);
/// answer.rs: `Applied` or `Repeat` without a producer answer.
const ELSEWHERE: Mapping = Mapping::Deliberate(
    "the VFS reads this write's success in its own table, never through decide_status",
);
const KEYED_ONLY: Mapping = Mapping::Deliberate(
    "the VFS writes keyed only: a success without a receipt is not one of its answers",
);
const NO_BUCKETS: Mapping =
    Mapping::Deliberate("the VFS never creates a bucket: an answer to one is none of its answers");
/// answer.rs: `Unknown`, a `1xx` or `2xx` (the keyed one through its malformed receipt).
const NOT_URSULA: Mapping = Mapping::Deliberate(
    "a keyed success without its receipt is no answer of Ursula's: the VFS blocks, which sends \
     the identical request again every 30 s",
);
const UNTYPED: Mapping = Mapping::Deliberate(
    "a success without the typed answer Ursula sends for the write is no answer of Ursula's: the \
     VFS rejects it and blocks, which sends the identical request again every 30 s",
);
/// answer.rs's producer rows: `receipt-evicted` reads `Repeat`.
const EVICTED: Mapping = Mapping::Deliberate(
    "the in-flight write is always the producer's newest, so an evicted receipt names another \
     writer's write under our key: the VFS fences, and a claim gives up its epoch",
);

macro_rules! expected {
    ($($id:literal => $append:ident, $claim:tt, $snapshot:ident, $mapping:expr;)+) => {
        &[$(Expected {
            id: $id,
            append: Vfs::$append,
            claim: expected!(@claim $claim),
            snapshot: Snap::$snapshot,
            mapping: $mapping,
        },)+]
    };
    (@claim -) => { None };
    (@claim $claim:ident) => { Some(Vfs::$claim) };
}

/// What the VFS does for each row, and how the replicator's action relates to the outcome.
const EXPECTED: &[Expected] = expected! {
    "append-applied" => Acknowledge, Acknowledge, Decides, FOLLOWS;
    "append-repeat" => Acknowledge, Acknowledge, Decides, FOLLOWS;
    "append-receipt-evicted" => Reset, Lost, Decides, EVICTED;
    "append-keyless" => Blocked, -, Decides, KEYED_ONLY;
    "close-keyless" => Blocked, -, Decides, KEYED_ONLY;
    "create-new" => Blocked, -, Decides, ELSEWHERE;
    "create-matching" => Blocked, -, Decides, ELSEWHERE;
    "snapshot-published" => Blocked, -, Decides, ELSEWHERE;
    "part-staged" => Blocked, -, Decides, ELSEWHERE;
    "parts-discarded" => Blocked, -, Decides, ELSEWHERE;
    "retention-advanced" => Blocked, -, Decides, ELSEWHERE;
    "stream-deleted" => Blocked, -, Decides, ELSEWHERE;
    "bucket-created" => Blocked, -, Decides, NO_BUCKETS;
    "parts-commit-published" => Blocked, -, Decides, ELSEWHERE;
    "redirect" => Redirect, Redirect, Decides, FOLLOWS;
    "producer-epoch-closed" => Reset, Lost, Decides, FOLLOWS;
    "producer-stale-epoch" => Reset, Lost, Decides, FOLLOWS;
    "producer-key-conflict" => Reset, Lost, Decides, FOLLOWS;
    "producer-expired" => ContinuationClaim, Lost, Decides, FOLLOWS;
    "producer-seq-gap" => Reset, Reset, Decides, FOLLOWS;
    "producer-epoch-not-claimed" => Reset, Reset, Decides, FOLLOWS;
    "bad-request" => Blocked, -, Decides, FOLLOWS;
    "not-found" => Head, Head, Decides, FOLLOWS;
    "stream-closed" => Reset, Reset, Decides, FOLLOWS;
    "stream-seq-conflict" => Blocked, -, Decides, FOLLOWS;
    "snapshot-conflict" => Blocked, -, Decides, FOLLOWS;
    "conflict" => Blocked, -, Decides, FOLLOWS;
    "gone" => Blocked, -, Decides, FOLLOWS;
    "incarnation-mismatch" => Head, Head, Decides, FOLLOWS;
    "too-large" => LowerLimit, -, Decides, FOLLOWS;
    "producer-limit" => Resend, Resend, Resend, SAFE_RESEND;
    // Unlike every other `429`, the snapshotter decides on this one
    // (`transient_part`): no repeat of the part lifts the stream's bound.
    "staged-parts-limit" => Resend, -, Decides, SAFE_RESEND;
    "no-cold-store" => Resend, -, Decides, SAFE_RESEND;
    "no-leader-before-body" => Resend, Resend, Resend, SAFE_RESEND;
    "no-leader-before-proposal" => Resend, Resend, Resend, SAFE_RESEND;
    "leadership-lost-after-proposal" => Resend, Resend, Resend, FOLLOWS;
    "outcome-unknown" => Resend, Resend, Resend, FOLLOWS;
    "wal-disk-pressure" => Resend, Resend, Resend, SAFE_RESEND;
    "raft-log-pressure" => Resend, Resend, Resend, SAFE_RESEND;
    "ingress-body-budget" => Resend, Resend, Resend, SAFE_RESEND;
    "cold-backpressure" => Resend, Resend, Resend, SAFE_RESEND;
    "raft-uncommitted-backpressure" => Resend, Resend, Resend, SAFE_RESEND;
    "history-pending" => Resend, Resend, Resend, SAFE_RESEND;
    "apply-stopped" => Resend, Resend, Resend, SAFE_RESEND;
    "group-restarting" => Resend, Resend, Resend, SAFE_RESEND;
    "json-boundary-unverified" => Resend, -, Resend, SAFE_RESEND;
    "snapshot-commits-busy" => Resend, -, Resend, SAFE_RESEND;
    "snapshot-commit-in-progress" => Resend, -, Resend, FOLLOWS;
    "internal" => Resend, Resend, Resend, FOLLOWS;
    "cold-store-failed" => Resend, Resend, Resend, FOLLOWS;
    "gateway-invalid-credential" => Reauthorize, Reauthorize, Decides, FOLLOWS;
    "gateway-forbidden" => Blocked, Blocked, Decides, FOLLOWS;
    "gateway-concealed" => Head, Head, Decides, FOLLOWS;
    "gateway-too-large" => LowerLimit, LowerLimit, Decides, FOLLOWS;
    "gateway-rate-limited" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-uploads-exhausted" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-upload-bytes-exhausted" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-upload-budget-exhausted" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-body-too-slow" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-authentication-unavailable" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-authorization-unavailable" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-no-upstream" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-upstream-unreachable" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-no-leader" => Resend, Resend, Resend, SAFE_RESEND;
    "gateway-upstream-failed" => Resend, Resend, Resend, FOLLOWS;
    "gateway-upstream-timeout" => Resend, Resend, Resend, FOLLOWS;
    "embedder-payment-required" => Blocked, Blocked, Decides, FOLLOWS;
    "embedder-account-revoked" => Blocked, Blocked, Decides, FOLLOWS;
    "embedder-forbidden" => Blocked, Blocked, Decides, FOLLOWS;
    "embedder-rate-limited" => Resend, Resend, Resend, SAFE_RESEND;
    "foreign-4xx" => Blocked, Blocked, Decides, FOLLOWS;
    "foreign-408" => Resend, Resend, Resend, FOLLOWS;
    "foreign-5xx" => Resend, Resend, Resend, FOLLOWS;
    "keyed-2xx-without-receipt" => Blocked, Blocked, Decides, NOT_URSULA;
    "keyless-2xx-untyped" => Blocked, -, Decides, UNTYPED;
    "transport-failure" => Resend, Resend, Resend, FOLLOWS;
};

/// Whether the replicator does what `outcome` asks of a client: acknowledge what holds, resend
/// what may have applied, follow a redirect, and stop or repair on a refusal.
fn follows(outcome: &WriteOutcome, vfs: Vfs) -> bool {
    match outcome {
        WriteOutcome::Applied | WriteOutcome::Repeat => vfs == Vfs::Acknowledge,
        WriteOutcome::Unknown => vfs == Vfs::Resend,
        WriteOutcome::Redirected => vfs == Vfs::Redirect,
        WriteOutcome::Refused(_) => !matches!(vfs, Vfs::Acknowledge | Vfs::Resend | Vfs::Redirect),
    }
}

/// Whether a replicator decision is safe for `outcome` at all, by design or not: it never treats
/// an answer that may have applied as one that did not, and never acknowledges what did not
/// apply. After an `Unknown` it sends the identical request again: at once (`Resend`), every 30 s
/// while blocked (`Blocked`), or, for a `3xx` but `307` or `308`, as after a redirect it never
/// follows (`Redirect`): again to the same URL a few times, then blocked.
fn safe(outcome: &WriteOutcome, vfs: Vfs) -> bool {
    match outcome {
        WriteOutcome::Unknown => matches!(vfs, Vfs::Resend | Vfs::Blocked | Vfs::Redirect),
        WriteOutcome::Refused(_) | WriteOutcome::Redirected => vfs != Vfs::Acknowledge,
        WriteOutcome::Applied | WriteOutcome::Repeat => true,
    }
}

fn of_common(common: &Common) -> Vfs {
    match common {
        Common::Resend { .. } => Vfs::Resend,
        Common::Head(_) => Vfs::Head,
        Common::Reauthorize => Vfs::Reauthorize,
        Common::Redirect { .. } => Vfs::Redirect,
        Common::LowerLimit { .. } => Vfs::LowerLimit,
        Common::Blocked(_) => Vfs::Blocked,
        Common::Reset(_) => Vfs::Reset,
    }
}

/// The request a row's answer answers, as the VFS would send it, a keyed one at `seq`.
fn request_of(write: Write, keyed_seq: u64) -> Request {
    let with = |method, target, content_type: Option<&str>, closes| {
        let mut request = Request::new(method, target);
        request.content_type = content_type.map(str::to_owned);
        request.closes = closes;
        request
    };
    let snapshot = Target::Snapshot { offset: off(3) };
    match write {
        Write::ProducerAppend => Ctx::new(local(Vec::new(), 0))
            .expect("a context")
            .write_request(epoch(3), seq(keyed_seq), Arc::from(b"batch".to_vec())),
        Write::Append => with(Method::Post, Target::Stream, Some("text/plain"), false),
        Write::Close => with(Method::Post, Target::Stream, None, true),
        Write::Create => with(Method::Put, Target::Stream, Some("text/plain"), false),
        Write::SnapshotPublish | Write::PartsCommit => with(
            Method::Put,
            snapshot,
            Some("application/octet-stream"),
            false,
        ),
        Write::PartStage => with(
            Method::Post,
            Target::SnapshotParts { offset: off(3) },
            None,
            false,
        ),
        Write::PartsDiscard => with(Method::Delete, Target::StagedParts, None, false),
        Write::Retention => with(
            Method::Put,
            Target::Retention { offset: off(3) },
            None,
            false,
        ),
        Write::DeleteStream => with(Method::Delete, Target::Stream, None, false),
        // The VFS never creates a bucket: its stream's create stands in.
        Write::CreateBucket => with(Method::Put, Target::Stream, None, false),
    }
}

/// `row`'s answer to `request`, or no answer.
fn exchange(row: &Row, request: &Request) -> Exchange {
    match row.sample().expect("a sample") {
        // No answer: the request may have gone out.
        None => Exchange::NoAnswer(NoAnswer::Lost {
            reason: "connection reset".to_owned(),
        }),
        Some(sample) => Exchange::Answered(Box::new(parse_answer(
            request,
            sample.status,
            sample.pairs(),
            sample.body.clone(),
        ))),
    }
}

/// The in-flight append `(3, 4)`, expected where the samples' receipts start.
fn in_flight() -> Sealed {
    Sealed {
        epoch: epoch(3),
        seq: seq(4),
        first: crate::ids::Position(5),
        last: crate::ids::Position(6),
        body: Arc::from(b"batch".to_vec()),
        expected_start: off(1),
        part: None,
    }
}

/// What the replicator does with `row`'s answer to the in-flight append, or, for a row of
/// another write, through [`decide_status`].
fn append_decision(row: &Row) -> Vfs {
    let exchange = exchange(row, &request_of(row.write, 4));
    if row.write != Write::ProducerAppend {
        return match &exchange {
            Exchange::Answered(answer) => of_common(&decide_status(answer)),
            Exchange::NoAnswer(_) => Vfs::Resend,
        };
    }
    match decide_append(&in_flight(), &exchange) {
        AppendDecision::Acknowledge { .. } => Vfs::Acknowledge,
        AppendDecision::ContinuationClaim => Vfs::ContinuationClaim,
        AppendDecision::Common(common) => of_common(&common),
    }
}

/// What the replicator does with `row`'s answer to the in-flight claim, for a keyed write's row.
fn claim_decision(row: &Row) -> Option<Vfs> {
    if row.write != Write::ProducerAppend {
        return None;
    }
    Some(
        match decide_claim(&exchange(row, &request_of(row.write, 0))) {
            ClaimDecision::Won { .. } => Vfs::Acknowledge,
            ClaimDecision::Lost { .. } => Vfs::Lost,
            ClaimDecision::Common(common) => of_common(&common),
        },
    )
}

/// What the snapshotter does with `row`'s answer to a part, for a part's row, else to a publish
/// or a commit, and the outcome that answer carries.
fn snapshot_decision(row: &Row) -> (Snap, WriteOutcome) {
    let (request, reader): (Request, fn(&Answer) -> bool) = match row.write {
        Write::PartStage => (request_of(row.write, 0), transient_part),
        // The snapshotter's own writes, read with their own request.
        Write::Retention | Write::PartsDiscard => (request_of(row.write, 0), transient),
        _ => (request_of(Write::SnapshotPublish, 0), transient),
    };
    match exchange(row, &request) {
        Exchange::NoAnswer(_) => (Snap::Resend, WriteOutcome::NO_ANSWER),
        Exchange::Answered(answer) => {
            let snap = if reader(&answer) {
                Snap::Resend
            } else {
                Snap::Decides
            };
            (snap, answer.outcome.expect("an answer to a write"))
        }
    }
}

/// The outcome the VFS's own answer to `row`'s write carries ([`Answer::outcome`]), the one
/// classification of the answer it reads.
fn vfs_outcome(row: &Row) -> WriteOutcome {
    match exchange(row, &request_of(row.write, 4)) {
        Exchange::NoAnswer(_) => WriteOutcome::NO_ANSWER,
        Exchange::Answered(answer) => answer.outcome.expect("an answer to a write"),
    }
}

#[test]
fn the_vfs_maps_every_inventory_row_to_its_action() {
    let ids: Vec<&str> = INVENTORY.iter().map(|row| row.id).collect();
    let expected: Vec<&str> = EXPECTED.iter().map(|row| row.id).collect();
    assert_eq!(
        ids, expected,
        "one expectation per inventory row, in its order"
    );
    for (row, expected) in INVENTORY.iter().zip(EXPECTED) {
        let id = row.id;
        let outcome = row
            .sample()
            .expect("a sample")
            .map_or(WriteOutcome::NO_ANSWER, |sample| sample.classify());
        assert_eq!(outcome, row.outcome, "{id}");
        // The VFS reads the answer with `classify` itself. It never creates a bucket, and the
        // stream's create that stands in reads a success without `Stream-Incarnation` as unknown.
        let read = vfs_outcome(row);
        if row.write == Write::CreateBucket {
            assert_eq!(read, WriteOutcome::Unknown, "{id}");
        } else {
            assert_eq!(read, outcome, "{id}: the VFS read the answer otherwise");
        }

        let (append, claim, (snapshot, as_snapshot)) = (
            append_decision(row),
            claim_decision(row),
            snapshot_decision(row),
        );
        assert_eq!(
            append, expected.append,
            "{id}: the append reader decided otherwise"
        );
        assert_eq!(
            claim, expected.claim,
            "{id}: the claim reader decided otherwise"
        );
        assert_eq!(
            snapshot, expected.snapshot,
            "{id}: the snapshotter decided otherwise"
        );

        let decisions = [Some(append), claim];
        for vfs in decisions.into_iter().flatten() {
            assert!(
                safe(&outcome, vfs),
                "{id}: {vfs:?} is unsafe for {outcome:?}"
            );
        }
        // The snapshotter sends again every answer that may have applied. A success it decides
        // on counts as published only with the typed snapshot, so an untyped one never passes
        // for a publish.
        let success = row
            .sample()
            .expect("a sample")
            .is_some_and(|sample| (200..300).contains(&sample.status));
        if as_snapshot == WriteOutcome::Unknown && !success {
            assert_eq!(
                snapshot,
                Snap::Resend,
                "{id}: an unknown outcome is sent again"
            );
        }
        let all_follow = decisions
            .into_iter()
            .flatten()
            .all(|vfs| follows(&outcome, vfs));
        let reason = expected.mapping.reason();
        assert_eq!(
            all_follow,
            reason.is_none(),
            "{id}: {decisions:?} for {outcome:?} ({reason:?}): a difference is mapped \
             deliberately, with its reason, and an action that follows the outcome is not"
        );
    }
}

/// Two answers no inventory row stands for, which `classify` reads as `Unknown` and
/// `replicator/answer.rs` maps deliberately: a `3xx` but `307` or `308` is a redirect, never
/// followed, and a `1xx` is no answer of Ursula's, blocked `rejected`. Both send the identical
/// request again.
#[test]
fn a_3xx_but_307_or_308_redirects_and_a_1xx_is_rejected() {
    let location = "http://elsewhere/s";
    let redirect = Common::Redirect {
        location: Some(location.to_owned()),
    };
    for (status, expected) in [
        (300, redirect.clone()),
        (301, redirect.clone()),
        (302, redirect.clone()),
        (303, redirect.clone()),
        (304, redirect),
        (
            100,
            Common::Blocked(BlockedReason::Rejected { status: 100 }),
        ),
        (
            103,
            Common::Blocked(BlockedReason::Rejected { status: 103 }),
        ),
    ] {
        for keyed_seq in [4, 0] {
            let answer = parse_answer(
                &request_of(Write::ProducerAppend, keyed_seq),
                status,
                [(LOCATION, location.as_bytes())],
                Vec::new(),
            );
            assert_eq!(answer.outcome, Some(WriteOutcome::Unknown), "{status}");
            let exchange = Exchange::Answered(Box::new(answer));
            let common = if keyed_seq == 0 {
                match decide_claim(&exchange) {
                    ClaimDecision::Common(common) => common,
                    other => panic!("{status}: the claim decided {other:?}"),
                }
            } else {
                match decide_append(&in_flight(), &exchange) {
                    AppendDecision::Common(common) => common,
                    other => panic!("{status}: the append decided {other:?}"),
                }
            };
            assert_eq!(common, expected, "{status} at seq {keyed_seq}");
            assert!(
                safe(&WriteOutcome::Unknown, of_common(&common)),
                "{status}: {common:?} is unsafe for an unknown outcome"
            );
        }
    }
}

/// An untyped `429`, an intermediary's rate limit without an error body, is sent again by every
/// snapshot write: each repeats safely by its offset and digest, whether `classify` reads the
/// `429` as `Refused` (as it does) or `Unknown`.
#[test]
fn the_snapshotter_resends_an_untyped_429() {
    for write in [
        Write::SnapshotPublish,
        Write::PartsCommit,
        Write::PartStage,
        Write::PartsDiscard,
        Write::Retention,
    ] {
        let answer = parse_answer(&request_of(write, 4), 429, [], Vec::new());
        assert!(
            matches!(answer.outcome, Some(WriteOutcome::Refused(_))),
            "{write:?}: {:?}",
            answer.outcome
        );
        assert!(transient(&answer), "{write:?}");
        assert!(transient_part(&answer), "{write:?}");
        let unknown = Answer {
            outcome: Some(WriteOutcome::Unknown),
            ..answer
        };
        assert!(transient(&unknown), "{write:?} read as unknown");
    }
}

/// The VFS's blocked reasons for the gateway's and an embedder's refusals that it shows its host.
#[test]
fn the_vfs_names_the_authorizer_refusals() {
    for (id, reason) in [
        ("embedder-payment-required", BlockedReason::PaymentRequired),
        ("embedder-account-revoked", BlockedReason::Forbidden),
        ("embedder-forbidden", BlockedReason::Forbidden),
        ("gateway-forbidden", BlockedReason::Forbidden),
    ] {
        let row = ursula_headers::write_outcome::inventory::row(id).expect("a row");
        let sample = row.sample().expect("a sample").expect("an answer");
        let answer = parse_answer(
            &request_of(row.write, 4),
            sample.status,
            sample.pairs(),
            sample.body.clone(),
        );
        assert_eq!(decide_status(&answer), Common::Blocked(reason), "{id}");
        // A refusal never resets: nothing applied, so nothing fences.
        assert_ne!(
            decide_status(&answer),
            Common::Reset(ResetKind::Fenced),
            "{id}"
        );
    }
}
