//! The VFS's answer tables against `ursula_headers::classify`, row by row of the write-outcome
//! inventory (`ursula_headers::write_outcome::inventory`). Until the VFS reads its answers with
//! `classify` (#652), several readers of the same answers exist: this test keeps them from
//! drifting.
//!
//! Each row's answer goes through every reader of write answers the VFS has:
//!
//! - [`decide_append`], for the in-flight keyed append, which reads a producer answer and hands
//!   every other answer to [`decide_status`];
//! - [`decide_claim`], for the in-flight claim (`Producer-Seq: 0`), for the rows of a keyed write;
//! - the snapshotter's [`transient`] for a publish or a parts commit, and [`transient_part`] for
//!   a part, which tell an answer to send again from one the snapshotter decides on.
//!
//! [`EXPECTED`] names what each reader does with each row. Where the append and claim readers do
//! what `classify` says, the row's reason is empty. Where either differs by design, the reason
//! says why that is safe. Every reader must send again whatever `classify` reads as `Unknown`. A
//! new row, a changed VFS decision or a changed classification fails the test.

use std::sync::Arc;

use ursula_headers::WriteKind;
use ursula_headers::WriteOutcome;
use ursula_headers::classify;
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

/// One row of what the VFS does: its append and claim decisions (`None` for a row that is no
/// keyed write's answer), its snapshotter's, and why the replicator's differ from `classify`'s.
struct Expected {
    id: &'static str,
    append: Vfs,
    claim: Option<Vfs>,
    snapshot: Snap,
    reason: &'static str,
}

const SAFE_RESEND: &str =
    "refused, and a keyed write's repeat is safe: it applies once or gets its receipt";
const ELSEWHERE: &str =
    "the VFS reads this write's success in its own table, never through decide_status";
const KEYED_ONLY: &str =
    "the VFS writes keyed only: a success without a receipt is not one of its answers";
const NOT_URSULA: &str = "a keyed success without its receipt is no answer of Ursula's: the VFS \
                          blocks, which sends the identical request again every 30 s";

const UNTYPED: &str = "a success without the typed answer Ursula sends for the write is no \
                       answer of Ursula's: the VFS rejects it and blocks, which sends the \
                       identical request again every 30 s";

macro_rules! expected {
    ($($id:literal => $append:ident, $claim:tt, $snapshot:ident, $reason:expr;)+) => {
        &[$(Expected {
            id: $id,
            append: Vfs::$append,
            claim: expected!(@claim $claim),
            snapshot: Snap::$snapshot,
            reason: $reason,
        },)+]
    };
    (@claim -) => { None };
    (@claim $claim:ident) => { Some(Vfs::$claim) };
}

/// What the VFS does for each row, and, where the replicator does not do what `classify` reads,
/// why that is safe.
const EXPECTED: &[Expected] = expected! {
    "append-applied" => Acknowledge, Acknowledge, Decides, "";
    "append-repeat" => Acknowledge, Acknowledge, Decides, "";
    "append-receipt-evicted" => Reset, Lost, Decides,
        "the in-flight write is always the producer's newest, so an evicted receipt names \
         another writer's write under our key: the VFS fences, and a claim gives up its epoch";
    "append-keyless" => Blocked, -, Decides, KEYED_ONLY;
    "close-keyless" => Blocked, -, Decides, KEYED_ONLY;
    "create-new" => Blocked, -, Decides, ELSEWHERE;
    "create-matching" => Blocked, -, Decides, ELSEWHERE;
    "snapshot-published" => Blocked, -, Decides, ELSEWHERE;
    "part-staged" => Blocked, -, Decides, ELSEWHERE;
    "parts-discarded" => Blocked, -, Decides, ELSEWHERE;
    "parts-commit-published" => Blocked, -, Decides, ELSEWHERE;
    "redirect" => Redirect, Redirect, Decides, "";
    "producer-epoch-closed" => Reset, Lost, Decides, "";
    "producer-stale-epoch" => Reset, Lost, Decides, "";
    "producer-key-conflict" => Reset, Lost, Decides, "";
    "producer-expired" => ContinuationClaim, Lost, Decides, "";
    "producer-seq-gap" => Reset, Reset, Decides, "";
    "producer-epoch-not-claimed" => Reset, Reset, Decides, "";
    "bad-request" => Blocked, -, Decides, "";
    "not-found" => Head, Head, Decides, "";
    "stream-closed" => Reset, Reset, Decides, "";
    "stream-seq-conflict" => Blocked, -, Decides, "";
    "snapshot-conflict" => Blocked, -, Decides, "";
    "conflict" => Blocked, -, Decides, "";
    "gone" => Blocked, -, Decides, "";
    "incarnation-mismatch" => Head, Head, Decides, "";
    "too-large" => LowerLimit, -, Decides, "";
    "producer-limit" => Resend, Resend, Resend, SAFE_RESEND;
    // Unlike every other `429`, the snapshotter decides on this one
    // (`transient_part`): no repeat of the part lifts the stream's bound.
    "staged-parts-limit" => Resend, -, Decides, SAFE_RESEND;
    "no-cold-store" => Resend, -, Decides, SAFE_RESEND;
    "no-leader-before-body" => Resend, Resend, Resend, SAFE_RESEND;
    "no-leader-before-proposal" => Resend, Resend, Resend, SAFE_RESEND;
    "leadership-lost-after-proposal" => Resend, Resend, Resend, "";
    "outcome-unknown" => Resend, Resend, Resend, "";
    "wal-disk-pressure" => Resend, Resend, Resend, SAFE_RESEND;
    "raft-log-pressure" => Resend, Resend, Resend, SAFE_RESEND;
    "ingress-body-budget" => Resend, Resend, Resend, SAFE_RESEND;
    "cold-backpressure" => Resend, Resend, Resend, SAFE_RESEND;
    "raft-uncommitted-backpressure" => Resend, Resend, Resend, SAFE_RESEND;
    "history-pending" => Resend, Resend, Resend, SAFE_RESEND;
    "apply-stopped" => Resend, Resend, Resend, SAFE_RESEND;
    "json-boundary-unverified" => Resend, -, Resend, SAFE_RESEND;
    "snapshot-commits-busy" => Resend, -, Resend, SAFE_RESEND;
    "snapshot-commit-in-progress" => Resend, -, Resend, "";
    "internal" => Resend, Resend, Resend, "";
    "cold-store-failed" => Resend, Resend, Resend, "";
    "gateway-invalid-credential" => Reauthorize, Reauthorize, Decides, "";
    "gateway-forbidden" => Blocked, Blocked, Decides, "";
    "gateway-concealed" => Head, Head, Decides, "";
    "gateway-too-large" => LowerLimit, LowerLimit, Decides, "";
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
    "gateway-upstream-failed" => Resend, Resend, Resend, "";
    "gateway-upstream-timeout" => Resend, Resend, Resend, "";
    "embedder-payment-required" => Blocked, Blocked, Decides, "";
    "embedder-account-revoked" => Blocked, Blocked, Decides, "";
    "embedder-forbidden" => Blocked, Blocked, Decides, "";
    "embedder-rate-limited" => Resend, Resend, Resend, SAFE_RESEND;
    "foreign-4xx" => Blocked, Blocked, Decides, "";
    "foreign-408" => Resend, Resend, Resend, "";
    "foreign-5xx" => Resend, Resend, Resend, "";
    "keyed-2xx-without-receipt" => Blocked, Blocked, Decides, NOT_URSULA;
    "keyless-2xx-untyped" => Blocked, -, Decides, UNTYPED;
    "transport-failure" => Resend, Resend, Resend, "";
};

/// Whether the replicator does what `outcome` asks of a client: acknowledge what holds, resend
/// what may have applied, follow a redirect, and stop or repair on a refusal.
fn agrees(outcome: &WriteOutcome, vfs: Vfs) -> bool {
    match outcome {
        WriteOutcome::Applied | WriteOutcome::Repeat => vfs == Vfs::Acknowledge,
        WriteOutcome::Unknown => vfs == Vfs::Resend,
        WriteOutcome::Redirected => vfs == Vfs::Redirect,
        WriteOutcome::Refused(_) => !matches!(vfs, Vfs::Acknowledge | Vfs::Resend | Vfs::Redirect),
    }
}

/// Whether a replicator decision is safe for `outcome` at all, by design or not: it never treats
/// an answer that may have applied as one that did not (after an `Unknown` it sends the identical
/// request again, at once or every 30 s while blocked), and never acknowledges what did not apply.
fn safe(outcome: &WriteOutcome, vfs: Vfs) -> bool {
    match outcome {
        WriteOutcome::Unknown => matches!(vfs, Vfs::Resend | Vfs::Blocked),
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
    // The in-flight append `(3, 4)`, expected where the samples' receipts start.
    let sealed = Sealed {
        epoch: epoch(3),
        seq: seq(4),
        first: crate::ids::Position(5),
        last: crate::ids::Position(6),
        body: Arc::from(b"batch".to_vec()),
        expected_start: off(1),
        part: None,
    };
    match decide_append(&sealed, &exchange) {
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
/// or a commit.
fn snapshot_decision(row: &Row) -> Snap {
    let (request, reader): (Request, fn(&Answer) -> bool) = match row.write {
        Write::PartStage => (request_of(row.write, 0), transient_part),
        _ => (request_of(Write::SnapshotPublish, 0), transient),
    };
    match exchange(row, &request) {
        Exchange::NoAnswer(_) => Snap::Resend,
        Exchange::Answered(answer) if reader(&answer) => Snap::Resend,
        Exchange::Answered(_) => Snap::Decides,
    }
}

#[test]
fn the_vfs_and_classify_agree_on_every_inventory_row() {
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

        let (append, claim, snapshot) = (
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
        // The snapshotter sends again every answer that may have applied. A
        // success it decides on (`decided`) counts as published only with the
        // typed snapshot, so an untyped one never passes for a publish.
        let as_snapshot =
            row.sample()
                .expect("a sample")
                .map_or(WriteOutcome::NO_ANSWER, |sample| {
                    let kind = match row.write {
                        Write::PartStage => WriteKind::PartStage,
                        _ => WriteKind::Publish,
                    };
                    if (200..300).contains(&sample.status) {
                        return WriteOutcome::Applied;
                    }
                    classify(kind, sample.status, sample.pairs(), &sample.body)
                });
        if as_snapshot == WriteOutcome::Unknown {
            assert_eq!(
                snapshot,
                Snap::Resend,
                "{id}: an unknown outcome is sent again"
            );
        }
        let all_agree = decisions
            .into_iter()
            .flatten()
            .all(|vfs| agrees(&outcome, vfs));
        assert_eq!(
            all_agree,
            expected.reason.is_empty(),
            "{id}: {decisions:?} for {outcome:?}: a difference needs its reason, and an \
             agreement none"
        );
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
